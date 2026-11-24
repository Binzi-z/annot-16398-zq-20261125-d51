"""Tenant-scoped batch store with a commit-pointer (approval) protocol.

On-disk layout::

    root/
      tenants/
        <tenant-dir>/
          <batch-dir>/
            segment.bin       # all pages, framed, in seq order
            COMMIT            # the approval pointer (written last)

A *batch* is approved iff ``COMMIT`` exists, parses, is bound to this
tenant/batch, and the byte range it names in ``segment.bin`` re-validates:
the segment prefix length and SHA-256 match, every page's length and digest
match, the last page is the summary page, and the recomputed content digest
equals the committed digest.

The pointer is the sole source of approval.  Anything else is invisible:

* no ``COMMIT`` (temp written, or only the segment) -> unapproved,
* ``COMMIT`` present but segment shorter than ``segment_len`` -> unapproved,
* bytes appended to the segment *past* ``segment_len`` -> unapproved tail,
  never read, truncated on recovery,
* ``COMMIT`` bound to a different tenant/batch -> rejected.

Commit sequence (each arrow is a durability boundary)::

    write segment.bin entirely, fsync it
    fsync batch dir                         (segment name durable)
    --- crash here: segment durable, no pointer => unapproved ---
    write COMMIT.tmp, fsync it
    --- crash here: pointer content durable, pointer name absent => unapproved ---
    rename COMMIT.tmp -> COMMIT
    --- crash here: rename may not be durable; recovery sees either old
        (no COMMIT => unapproved) or new (COMMIT => approved). Both consistent ---
    fsync batch dir                         (COMMIT name durable => APPROVED)

Only after the final directory fsync is the batch approved.  This is the
"directory flushed but the pointer not flushed" fault: a crash between the
first and last directory fsync leaves a durable segment with no pointer, which
recovery must discard.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import struct
from dataclasses import dataclass, field
from pathlib import Path

from .durability import fsync_dir, open_binary, write_file_durably

PAGE_MAGIC = b"RPG1"
PAGE_HEADER = struct.Struct(">4sBIII")  # magic, flags, seq, index, payload_len
FLAG_SUMMARY = 0x01
DIGEST_LEN = 32

COMMIT_VERSION = 1
SEGMENT_NAME = "segment.bin"
COMMIT_NAME = "COMMIT"
COMMIT_TMP = ".COMMIT.tmp"

# base64url expands 4/3; cap raw ids so one path component stays < 255 chars.
MAX_ID_LEN = 160


class StoreCorrupt(Exception):
    """On-disk state failed validation; the affected batch is not approved."""


class CrashInjected(Exception):
    """Raised by an injected fault to emulate an abrupt process stop.

    The receive loop lets this propagate (a crash is not a per-batch error);
    the caller then runs recovery, which is exactly the real-world sequence.
    """


class TransientIOError(Exception):
    """Injected retryable I/O error (batch can still succeed on a retry)."""


def content_digest(pages: list[tuple[int, bytes]]) -> bytes:
    """SHA-256 over ``seq || len || payload`` for each page in seq order.

    Equal sequence numbers with equal payloads hash identically, so duplicate
    submissions that were merged do not perturb the digest.
    """
    h = hashlib.sha256()
    for seq, payload in pages:
        h.update(struct.pack(">II", seq, len(payload)))
        h.update(payload)
    return h.digest()


def safe_component(name: str) -> str:
    """Encode an arbitrary tenant/batch id into one reversible path component.

    Base64url of the UTF-8 bytes: filesystem-safe on Windows (no reserved
    characters), case-insensitive-safe, and reversible so recovery reports and
    the read port can address a tenant by its true id.
    """
    raw = name.encode("utf-8")
    if len(raw) > MAX_ID_LEN:
        raise ValueError(f"id longer than {MAX_ID_LEN} bytes")
    return "b" + base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_component(comp: str) -> str:
    if not comp.startswith("b"):
        raise StoreCorrupt(f"unrecognised path component {comp!r}")
    body = comp[1:]
    pad = "=" * (-len(body) % 4)
    try:
        return base64.urlsafe_b64decode(body + pad).decode("utf-8")
    except (ValueError, UnicodeDecodeError) as e:
        raise StoreCorrupt(f"cannot decode path component {comp!r}: {e}") from e


def encode_page(seq: int, index: int, payload, *, summary: bool) -> bytes:
    flags = FLAG_SUMMARY if summary else 0
    header = PAGE_HEADER.pack(PAGE_MAGIC, flags, seq, index, len(payload))
    body = bytearray(header)
    body += payload
    body += hashlib.sha256(body).digest()
    return bytes(body)


def decode_page(data: bytes, *, expect_index: int) -> tuple[int, bytes, bool]:
    """Validate one framed page; return (seq, payload, is_summary)."""
    if len(data) < PAGE_HEADER.size + DIGEST_LEN:
        raise StoreCorrupt("page shorter than header+digest")
    body, digest = data[:-DIGEST_LEN], data[-DIGEST_LEN:]
    if hashlib.sha256(body).digest() != digest:
        raise StoreCorrupt("page digest mismatch")
    magic, flags, seq, index, plen = PAGE_HEADER.unpack(body[: PAGE_HEADER.size])
    if magic != PAGE_MAGIC:
        raise StoreCorrupt(f"bad page magic {magic!r}")
    if index != expect_index:
        raise StoreCorrupt(f"page index {index} != expected {expect_index}")
    if len(body) - PAGE_HEADER.size != plen:
        raise StoreCorrupt("page length field mismatch")
    return seq, body[PAGE_HEADER.size :], bool(flags & FLAG_SUMMARY)


@dataclass
class CommitRecord:
    """O(1)-sized approval pointer.

    Pages are self-framing (each carries its own length and digest), so the
    pointer need not store a per-page offset table -- that would be 18,000
    dicts of unbudgeted memory for a large batch.  The reader walks the
    committed prefix and re-derives every page boundary, checking each page's
    index and the strictly-increasing seq as it goes; the content digest then
    covers the whole set.
    """

    tenant: str
    batch: str
    count: int
    summary_seq: int
    segment_len: int
    segment_sha256: str
    content_digest: str

    def to_bytes(self) -> bytes:
        core = {
            "version": COMMIT_VERSION,
            "tenant": self.tenant,
            "batch": self.batch,
            "count": self.count,
            "summary_seq": self.summary_seq,
            "segment_len": self.segment_len,
            "segment_sha256": self.segment_sha256,
            "content_digest": self.content_digest,
        }
        payload = json.dumps(core, sort_keys=True, separators=(",", ":")).encode()
        return json.dumps(
            {**core, "commit_sha256": hashlib.sha256(payload).hexdigest()},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()

    @classmethod
    def from_bytes(cls, data: bytes, *, tenant: str, batch: str) -> "CommitRecord":
        try:
            obj = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise StoreCorrupt(f"commit record unparseable: {e}") from e
        claimed = obj.pop("commit_sha256", None)
        if claimed is None:
            raise StoreCorrupt("commit record missing checksum")
        payload = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()
        if hashlib.sha256(payload).hexdigest() != claimed:
            raise StoreCorrupt("commit record checksum mismatch")
        if obj.get("version") != COMMIT_VERSION:
            raise StoreCorrupt("commit record version mismatch")
        if obj.get("tenant") != tenant or obj.get("batch") != batch:
            raise StoreCorrupt("commit record bound to a different tenant/batch")
        try:
            rec = cls(
                tenant=tenant,
                batch=batch,
                count=int(obj["count"]),
                summary_seq=int(obj["summary_seq"]),
                segment_len=int(obj["segment_len"]),
                segment_sha256=str(obj["segment_sha256"]),
                content_digest=str(obj["content_digest"]),
            )
        except (KeyError, TypeError, ValueError) as e:
            raise StoreCorrupt(f"commit record malformed: {e}") from e
        if rec.count <= 0 or rec.segment_len <= 0:
            raise StoreCorrupt("commit record has non-positive count/length")
        return rec


@dataclass
class RecoveryReport:
    approved: list[tuple[str, str]] = field(default_factory=list)
    rejected: list[tuple[str, str, str]] = field(default_factory=list)
    truncated_tails: list[tuple[str, str, int]] = field(default_factory=list)


class CommitHooks:
    """Named fault-injection points for the commit protocol.

    A hook raising any exception aborts the commit.  Because the protocol is
    ordered, whatever was durable before the raise defines the post-crash
    state, and recovery must sort it out.
    """

    def __init__(self, events: dict[str, object] | None = None) -> None:
        self._events = dict(events or {})

    def at(self, name: str, tenant: str, batch: str) -> None:
        fn = self._events.get(name)
        if fn is not None:
            fn(tenant, batch)


class Store:
    """Filesystem-backed batch store.  Approval == a valid COMMIT pointer."""

    def __init__(self, root: os.PathLike | str) -> None:
        self.root = Path(root)
        self.tenants = self.root / "tenants"
        self.tenants.mkdir(parents=True, exist_ok=True)

    # -- paths ---------------------------------------------------------
    def tenant_dir(self, tenant: str) -> Path:
        return self.tenants / safe_component(tenant)

    def batch_dir(self, tenant: str, batch: str) -> Path:
        return self.tenant_dir(tenant) / safe_component(batch)

    # -- commit --------------------------------------------------------
    def commit_batch(
        self,
        tenant: str,
        batch: str,
        pages: list[tuple[int, bytes]],
        *,
        summary_seq: int,
        hooks: CommitHooks | None = None,
    ) -> CommitRecord:
        """Durably commit an ordered page iterable; returns the commit record.

        ``pages`` may be any iterable of ``(seq, payload)`` in ascending seq
        order; it is consumed exactly once, so the caller need not materialise
        a list of every page.  The content digest is accumulated inline as each
        page is written, in the same order ``content_digest`` would use.
        """
        hooks = hooks or CommitHooks()
        tdir = self.tenant_dir(tenant)
        bdir = self.batch_dir(tenant, batch)
        tdir.mkdir(parents=True, exist_ok=True)
        bdir.mkdir(parents=True, exist_ok=True)
        # A pre-existing unapproved leftover from an earlier failed attempt is
        # never trusted: overwrite the segment from scratch.
        seg_path = bdir / SEGMENT_NAME
        (bdir / COMMIT_NAME).unlink(missing_ok=True)
        (bdir / COMMIT_TMP).unlink(missing_ok=True)

        seg_hasher = hashlib.sha256()
        content_hasher = hashlib.sha256()
        offset = 0
        count = 0
        fd = open_binary(seg_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
        try:
            for index, (seq, payload) in enumerate(pages):
                blob = encode_page(seq, index, payload, summary=(seq == summary_seq))
                _write_all(fd, blob)
                seg_hasher.update(blob)
                content_hasher.update(struct.pack(">II", seq, len(payload)))
                content_hasher.update(payload)
                offset += len(blob)
                count += 1
            os.fsync(fd)
        finally:
            os.close(fd)
        if count == 0:
            seg_path.unlink(missing_ok=True)
            raise ValueError("cannot commit an empty batch")
        segment_len = offset
        segment_sha = seg_hasher.hexdigest()

        hooks.at("after_segment_fsync", tenant, batch)
        fsync_dir(bdir)  # segment name durable
        hooks.at("after_segment_dir_fsync", tenant, batch)

        rec = CommitRecord(
            tenant=tenant,
            batch=batch,
            count=count,
            summary_seq=summary_seq,
            segment_len=segment_len,
            segment_sha256=segment_sha,
            content_digest=content_hasher.hexdigest(),
        )
        blob = rec.to_bytes()
        tmp = bdir / COMMIT_TMP
        # Write the pointer content durably in its temp name, then rename.
        write_file_durably(tmp, blob, flush_dir=False)  # fsync temp file
        hooks.at("after_commit_tmp_fsync", tenant, batch)
        os.replace(tmp, bdir / COMMIT_NAME)
        hooks.at("after_commit_rename", tenant, batch)
        fsync_dir(bdir)  # COMMIT name durable => approved
        hooks.at("after_commit_dir_fsync", tenant, batch)
        return rec

    # -- read side -----------------------------------------------------
    def load_commit(self, tenant: str, batch: str) -> CommitRecord:
        path = self.batch_dir(tenant, batch) / COMMIT_NAME
        try:
            data = path.read_bytes()
        except FileNotFoundError as e:
            raise StoreCorrupt(f"no commit pointer for {tenant}/{batch}") from e
        return CommitRecord.from_bytes(data, tenant=tenant, batch=batch)

    def _read_segment_prefix(self, bdir: Path, rec: CommitRecord) -> bytes:
        seg = bdir / SEGMENT_NAME
        try:
            with open(seg, "rb") as f:
                data = f.read(rec.segment_len)
        except FileNotFoundError as e:
            raise StoreCorrupt("segment missing for an approved commit") from e
        if len(data) != rec.segment_len:
            raise StoreCorrupt(
                f"segment shorter than committed ({len(data)} < {rec.segment_len})"
            )
        return data

    def _iter_segment_pages(self, bdir: Path, rec: CommitRecord):
        """Yield ``(index, seq, payload, is_summary)`` by walking the committed
        segment prefix frame-by-frame, without materialising the whole prefix.

        Each page is self-framing (magic, flags, seq, index, len, digest), so
        boundaries are re-derived and every page is checked.  Raises
        :class:`StoreCorrupt` on any mismatch.
        """
        full = self._read_segment_prefix(bdir, rec)  # enforces segment_sha
        if hashlib.sha256(full).hexdigest() != rec.segment_sha256:
            raise StoreCorrupt("segment prefix digest changed")
        off = 0
        index = 0
        while off < rec.segment_len:
            if off + PAGE_HEADER.size > rec.segment_len:
                raise StoreCorrupt(f"page {index} header runs past committed end")
            magic, flags, seq, pindex, plen = PAGE_HEADER.unpack_from(full, off)
            total = PAGE_HEADER.size + plen + DIGEST_LEN
            if off + total > rec.segment_len:
                raise StoreCorrupt(f"page {index} body runs past committed end")
            chunk = full[off : off + total]
            seq2, payload, is_summary = decode_page(chunk, expect_index=index)
            if seq2 != seq or pindex != index:
                raise StoreCorrupt(f"page {index} framing mismatch")
            yield index, seq, payload, is_summary
            off += total
            index += 1
        if index != rec.count:
            raise StoreCorrupt(f"segment holds {index} pages, pointer says {rec.count}")

    def read_batch_pages(
        self, tenant: str, batch: str, rec: CommitRecord | None = None
    ) -> list[tuple[int, bytes]]:
        """Return validated pages for an approved batch, or raise StoreCorrupt."""
        bdir = self.batch_dir(tenant, batch)
        if rec is None:
            rec = self.load_commit(tenant, batch)
        pages: list[tuple[int, bytes]] = []
        for _index, seq, payload, _summary in self._iter_segment_pages(bdir, rec):
            pages.append((seq, payload))
        if content_digest(pages).hex() != rec.content_digest:
            raise StoreCorrupt("content digest mismatch")
        return pages

    def is_approved(self, tenant: str, batch: str) -> bool:
        try:
            rec = self.load_commit(tenant, batch)
            self.read_batch_pages(tenant, batch, rec)
            return True
        except (StoreCorrupt, OSError):
            return False

    def validate_full(self, tenant: str, batch: str) -> CommitRecord:
        """Full re-validation used by recovery: pointer + pages + summary."""
        bdir = self.batch_dir(tenant, batch)
        rec = self.load_commit(tenant, batch)
        pages: list[tuple[int, bytes]] = []
        last = None
        for _index, seq, payload, is_summary in self._iter_segment_pages(bdir, rec):
            pages.append((seq, payload))
            last = (seq, is_summary)
        if len(pages) != rec.count:
            raise StoreCorrupt("page count mismatch")
        if content_digest(pages).hex() != rec.content_digest:
            raise StoreCorrupt("content digest mismatch")
        if last is None or not last[1] or last[0] != rec.summary_seq:
            raise StoreCorrupt("last page is not the expected summary")
        seqs = [s for s, _ in pages]
        if seqs != sorted(seqs) or len(set(seqs)) != len(seqs):
            raise StoreCorrupt("page seqs not strictly increasing")
        return rec

    # -- recovery ------------------------------------------------------
    def recover(self) -> RecoveryReport:
        """Rebuild approval from disk; only fully validated COMMITs count.

        * No ``COMMIT`` -> the batch directory (segment included) is removed.
        * ``COMMIT`` present but invalid / short segment -> removed.
        * Bytes appended past a valid ``segment_len`` -> truncated away.

        Returns a report that a caller folds into the in-memory index.
        """
        report = RecoveryReport()
        if not self.tenants.exists():
            return report
        for tdir in sorted(self.tenants.iterdir()):
            if not tdir.is_dir():
                continue
            try:
                tenant = decode_component(tdir.name)
            except StoreCorrupt:
                shutil.rmtree(tdir, ignore_errors=True)
                report.rejected.append((tdir.name, "", "undecodable tenant dir"))
                continue
            tenant_touched = False
            for bdir in sorted(tdir.iterdir()):
                if not bdir.is_dir():
                    continue
                if bdir.name.startswith("."):
                    shutil.rmtree(bdir, ignore_errors=True)
                    tenant_touched = True
                    continue
                try:
                    batch = decode_component(bdir.name)
                except StoreCorrupt:
                    shutil.rmtree(bdir, ignore_errors=True)
                    report.rejected.append((tenant, bdir.name, "undecodable batch dir"))
                    tenant_touched = True
                    continue
                tenant_touched = True
                self._recover_one(tenant, batch, bdir, report)
            if tenant_touched:
                fsync_dir(tdir)
        return report

    def _recover_one(
        self, tenant: str, batch: str, bdir: Path, report: RecoveryReport
    ) -> None:
        commit_path = bdir / COMMIT_NAME
        if not commit_path.exists():
            shutil.rmtree(bdir, ignore_errors=True)
            report.rejected.append((tenant, batch, "no commit pointer"))
            return
        try:
            rec = self.validate_full(tenant, batch)
        except (StoreCorrupt, OSError) as e:
            shutil.rmtree(bdir, ignore_errors=True)
            report.rejected.append((tenant, batch, str(e)))
            return

        # Approved.  Drop any unapproved tail appended past the pointer.
        seg = bdir / SEGMENT_NAME
        try:
            actual = seg.stat().st_size
        except OSError:
            actual = rec.segment_len
        if actual > rec.segment_len:
            with open(seg, "r+b") as f:
                f.truncate(rec.segment_len)
                os.fsync(f.fileno())
            report.truncated_tails.append((tenant, batch, actual - rec.segment_len))
        # Stray temp files are not approved state.
        (bdir / COMMIT_TMP).unlink(missing_ok=True)
        report.approved.append((tenant, batch))


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        view = view[written:]
