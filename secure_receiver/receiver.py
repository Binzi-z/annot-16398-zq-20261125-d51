"""Two-producer receipt ingest with dedupe, validation, and approval.

Model
-----
A *batch* arrives as a stream of numbered pages from up to two producer
threads.  The pages themselves may be zlib-compressed; the receiver expands
them into a bounded decompression arena.  Two producers may submit the *same*
sequence number with *different* payloads; identical duplicates are merged and
divergent duplicates are a hard batch failure (the batch cannot be approved
with an ambiguous page).

A batch becomes *approved* only when its **last page summary** validates:
that page carries a digest over every earlier page, plus the page count.  If
the summary is wrong the whole batch is failed *after* all the earlier pages
were read -- so no partial batch can leak.  On failure the batch is deleted:
it is invisible to the index, to notifications, and to the read port.

Every buffer the pipeline holds is charged to exactly one :class:`Pool`, so
the resident working set is the sum of the pools and stays under the budget.
"""

from __future__ import annotations

import struct
import threading
import zlib
from dataclasses import dataclass, field
from typing import Callable

from .budget import Budget, BudgetExhausted, Pool
from .store import (
    CrashInjected,
    Store,
    TransientIOError,
    content_digest,
)

SUMMARY_MAGIC = b"SUM1"
SUMMARY_STRUCT = struct.Struct(">4sII32s")  # magic, page_count, digest, zero pad
# page_count (u32), content digest (32), plus magic; exact size fixed.
SUMMARY_LEN = SUMMARY_STRUCT.size


class BatchRejected(Exception):
    """The batch failed validation and must not be published."""


class ThreadAborted(Exception):
    """A producer thread was aborted mid-batch (fault injection)."""


class BudgetStall(Exception):
    """A batch could not get working memory within its deadline."""


@dataclass
class Page:
    seq: int
    payload: bytes


@dataclass
class IngestStats:
    batches_ok: int = 0
    batches_failed: int = 0
    duplicates_merged: int = 0
    retries: int = 0
    thread_errors: list[str] = field(default_factory=list)


def make_summary(pages: list[tuple[int, bytes]]) -> bytes:
    """Build the last-page summary for an ordered page list (excluding itself)."""
    count = len(pages) + 1  # include the summary page
    digest = content_digest(pages)
    return SUMMARY_STRUCT.pack(SUMMARY_MAGIC, count, 0, digest)


def verify_summary(summary_payload: bytes, pages: list[tuple[int, bytes]]) -> None:
    if len(summary_payload) != SUMMARY_LEN:
        raise BatchRejected("summary page has wrong length")
    magic, count, _pad, digest = SUMMARY_STRUCT.unpack(summary_payload)
    if magic != SUMMARY_MAGIC:
        raise BatchRejected("summary page magic mismatch")
    if count != len(pages) + 1:
        raise BatchRejected(f"summary page count {count} != {len(pages) + 1}")
    if content_digest(pages) != digest:
        raise BatchRejected("summary digest does not cover the earlier pages")


class ReadPort:
    """The only read surface.  Yields pages for *approved* batches only."""

    def __init__(self, store: Store, index: "Index") -> None:
        self._store = store
        self._index = index

    def list_batches(self, tenant: str) -> list[str]:
        return sorted(self._index.approved_for(tenant))

    def read(self, tenant: str, batch: str) -> list[tuple[int, bytes]]:
        if not self._index.is_approved(tenant, batch):
            raise KeyError(f"batch {tenant}/{batch} is not approved")
        return self._store.read_batch_pages(tenant, batch)


class Index:
    """In-memory approved set + persisted index snapshot, per tenant.

    A batch appears here only after its COMMIT pointer is durable; a failed
    batch is never added, and recovery rebuilds this set from validated
    COMMITs alone.
    """

    def __init__(self, store: Store, path=None) -> None:
        self._store = store
        self._lock = threading.Lock()
        self._approved: dict[str, set[str]] = {}
        self._path = path

    def _key(self, tenant: str, batch: str) -> tuple[str, str]:
        return tenant, batch

    def publish(self, tenant: str, batch: str) -> None:
        with self._lock:
            self._approved.setdefault(tenant, set()).add(batch)

    def unpublish(self, tenant: str, batch: str) -> None:
        with self._lock:
            s = self._approved.get(tenant)
            if s:
                s.discard(batch)

    def is_approved(self, tenant: str, batch: str) -> bool:
        with self._lock:
            return batch in self._approved.get(tenant, ())

    def approved_for(self, tenant: str) -> set[str]:
        with self._lock:
            return set(self._approved.get(tenant, ()))

    def rebuild_from_store(self) -> None:
        report = self._store.recover()
        with self._lock:
            self._approved.clear()
            for tenant, batch in report.approved:
                self._approved.setdefault(tenant, set()).add(batch)


class _BatchBuffer:
    """Accumulates one batch's pages, charging them to a single named pool.

    The pool changes as the batch advances: pages arrive via ARENA and land in
    PENDING_PAGES; after validation the batch is moved to INDEX_QUEUE while it
    is indexed/committed; a transient failure parks it in RETRY_HELD.  The
    byte count is constant across those moves -- ``transfer`` never changes the
    global total -- so the invariant "resident == bytes_held" holds in whichever
    pool is current.

    Each page is charged ``len(payload) + page_overhead``: the payload bytes
    plus a per-page allowance for the Python object that holds them (the dict
    slot, the ``bytearray`` header, the int key).  Measured on CPython 3.12
    that overhead is ~120 bytes; we charge 160 to keep the ledger an upper
    bound on real resident memory rather than a lower bound.
    """

    def __init__(
        self,
        budget: Budget,
        batch: str,
        reserve_timeout: float | None = None,
        page_overhead: int = 160,
    ) -> None:
        self.budget = budget
        self.batch = batch
        self.pages: dict[int, bytearray] = {}
        self.bytes_held = 0
        self.pool = Pool.PENDING_PAGES
        self.reserve_timeout = reserve_timeout
        self.page_overhead = page_overhead
        self._lock = threading.Lock()

    def add(self, seq: int, arena_payload, timeout: float | None = None) -> bool:
        """Add a decompressed page.  Returns False if it was an exact duplicate.

        Divergent duplicate raises :class:`BatchRejected`.  The payload's bytes
        are already charged to ARENA; on acceptance we transfer that charge to
        the buffer pool and additionally reserve the per-page object overhead.
        """
        with self._lock:
            n = len(arena_payload)
            existing = self.pages.get(seq)
            if existing is not None:
                if existing == arena_payload:
                    return False
                raise BatchRejected(
                    f"seq {seq} submitted with divergent content by two producers"
                )
            # Charge the object overhead first; if it does not fit, the page is
            # not accepted and ARENA keeps its bytes (the caller releases them).
            self.budget.reserve({self.pool: self.page_overhead}, timeout=timeout)
            try:
                self.budget.transfer(Pool.ARENA, self.pool, n)
            except BudgetExhausted:
                self.budget.release({self.pool: self.page_overhead})
                raise
            self.pages[seq] = arena_payload
            self.bytes_held += n + self.page_overhead
            return True

    def move_to(self, pool: Pool) -> None:
        with self._lock:
            if self.bytes_held and pool != self.pool:
                self.budget.transfer(self.pool, pool, self.bytes_held)
                self.pool = pool

    def ordered(self):
        """Yield (seq, payload) in ascending seq order without copying the set.

        ``sorted`` builds a list of the int keys (O(n) pointers) and frees it
        as the generator is exhausted; it does not copy payloads.
        """
        for seq in sorted(self.pages):
            yield seq, self.pages[seq]

    def drop(self) -> None:
        with self._lock:
            if self.bytes_held:
                self.budget.release({self.pool: self.bytes_held})
                self.bytes_held = 0
            self.pages.clear()


class Receiver:
    """Coordinates producers, the budget, the store, and the read port."""

    def __init__(
        self,
        store: Store,
        budget: Budget | None = None,
        *,
        hooks: "ReceiverHooks | None" = None,
        max_page_uncompressed: int = 256 * 1024,
        arena_chunk: int = 16 * 1024,
    ) -> None:
        self.store = store
        self.budget = budget or Budget()
        self.hooks = hooks or ReceiverHooks()
        self.index = Index(store)
        self.port = ReadPort(store, self.index)
        self.stats = IngestStats()
        self._notify = _Notifier()
        self.max_page_uncompressed = max_page_uncompressed
        self.arena_chunk = arena_chunk

    # -- notifications -------------------------------------------------
    @property
    def notifications(self):
        return self._notify

    # -- ingest --------------------------------------------------------
    def ingest_batch(
        self,
        tenant: str,
        batch: str,
        *,
        summary_seq: int,
        max_retries: int = 2,
        retry_backoff: float = 0.0,
        reserve_timeout: float | None = 5.0,
    ) -> bool:
        """Ingest and approve one batch from its two producer threads.

        Returns True if approved, False if failed (and made invisible).
        A crash injection propagates as :class:`CrashInjected` after the
        in-memory buffers are freed (the process "died"); the caller is
        expected to run :meth:`recover` to show recovery is correct.
        """
        buf = _BatchBuffer(self.budget, batch, reserve_timeout=reserve_timeout)
        try:
            self._run_producers(tenant, batch, buf, summary_seq)
            self.hooks.at("before_validate")
            self._validate(buf, summary_seq)
            # Validated: the pages now queue for indexing/commit.
            buf.move_to(Pool.INDEX_QUEUE)

            attempt = 0
            while True:
                try:
                    self.hooks.at("before_commit")
                    self.store.commit_batch(
                        tenant,
                        batch,
                        buf.ordered(),
                        summary_seq=summary_seq,
                        hooks=self.hooks.commit_hooks,
                    )
                    break
                except TransientIOError:
                    if attempt >= max_retries:
                        raise
                    attempt += 1
                    self.stats.retries += 1
                    # Keep bytes resident and *independently counted* in
                    # RETRY_HELD across the backoff, then back to INDEX_QUEUE.
                    buf.move_to(Pool.RETRY_HELD)
                    try:
                        self.hooks.at("during_retry_backoff")
                        if retry_backoff:
                            import time

                            time.sleep(retry_backoff)
                    finally:
                        buf.move_to(Pool.INDEX_QUEUE)

            self.index.publish(tenant, batch)
            self._notify.emit(tenant, batch)
            self.stats.batches_ok += 1
            return True
        except CrashInjected:
            # Emulated abrupt stop: reclaim this process's working memory.
            buf.drop()
            raise
        except (BatchRejected, BudgetStall, TransientIOError, ThreadAborted, OSError) as e:
            self._fail(tenant, batch, buf, e)
            return False
        finally:
            buf.drop()

    def _run_producers(self, tenant, batch, buf, summary_seq) -> None:
        stream = self.hooks.streams.get((tenant, batch))
        if stream is None:
            raise BatchRejected("no input stream configured")
        errors: list[BaseException] = []
        threads = []
        for i, src in enumerate(stream.split_producers()):
            t = threading.Thread(
                target=self._producer_worker,
                args=(buf, src, errors),
                name=f"producer-{i}",
                daemon=True,
            )
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        if errors:
            raise errors[0]

    def _producer_worker(
        self, buf: _BatchBuffer, source, errors: list[BaseException]
    ) -> None:
        try:
            for raw_page in source.pages():
                self.hooks.at("in_producer")
                self._ingest_one(buf, raw_page)
        except BaseException as e:  # surface every failure on the main thread
            errors.append(e)

    def _ingest_one(self, buf: _BatchBuffer, raw_page) -> None:
        seq, wire = raw_page.seq, raw_page.payload
        try:
            # 1. compressed input is charged while held.
            self.budget.reserve(
                {Pool.COMPRESSED: max(1, len(wire))}, timeout=buf.reserve_timeout
            )
        except BudgetExhausted as e:
            raise BudgetStall(f"page {seq} input: {e}") from e
        compressed_held = max(1, len(wire))
        arena: bytes | None = None
        arena_charge = 0
        transferred = False
        try:
            # 2. Expand under an incremental arena charge: the arena is never
            #    charged for more than the bytes actually produced plus one
            #    small chunk, so a page that lies about its size cannot blow
            #    the budget.  Oversize pages are rejected, not buffered.
            arena, arena_charge = self._decompress_bounded(seq, wire, buf.reserve_timeout)
            self.hooks.at("in_arena")
            # Compressed input is no longer needed once expanded.
            self.budget.release({Pool.COMPRESSED: compressed_held})
            compressed_held = 0

            added = buf.add(seq, arena, timeout=buf.reserve_timeout)
            if added:
                # buf.add transferred len(arena) from ARENA to the batch pool.
                transferred = True
                arena_charge -= len(arena)  # normally 0 (exact charge)
                if arena_charge:
                    self.budget.release({Pool.ARENA: arena_charge})
                    arena_charge = 0
            else:
                self.stats.duplicates_merged += 1
        except zlib.error as e:
            raise BatchRejected(f"page {seq} did not decompress: {e}") from e
        except BudgetExhausted as e:
            # buf.add could not move this page into the batch pool: the batch
            # (as a whole) does not fit the budget.  Fail it cleanly.
            raise BudgetStall(f"page {seq} batch pool: {e}") from e
        finally:
            if compressed_held:
                self.budget.release({Pool.COMPRESSED: compressed_held})
            # If the page was not transferred into the batch, ARENA still owns
            # its bytes and must return them (duplicate, reject, or error).
            if arena_charge and not transferred:
                self.budget.release({Pool.ARENA: arena_charge})

    def _decompress_bounded(
        self, seq: int, wire: bytes, timeout: float | None
    ) -> tuple[bytearray, int]:
        """Decompress ``wire`` charging ARENA in step with output growth.

        Returns ``(data, arena_charge)`` where ``arena_charge == len(data)``.
        We grow one :class:`bytearray` in place -- returned without a copy, so
        the bytes we charge for are exactly the bytes we hold -- and reserve a
        single chunk at a time, releasing any unused part immediately.  The
        only slack is one transient chunk-sized piece from ``decompress``.
        Pages that expand past ``max_page_uncompressed`` are rejected before
        any large buffer exists.  The returned bytes stay charged to ARENA; the
        caller transfers that charge into the batch or releases it.
        """
        d = zlib.decompressobj()
        buf = bytearray()
        produced = 0
        feed = wire
        while not d.eof:
            remaining = self.max_page_uncompressed - produced
            if remaining <= 0:
                self.budget.release({Pool.ARENA: produced})
                raise BatchRejected(
                    f"page {seq} exceeds max decompressed size "
                    f"{self.max_page_uncompressed}"
                )
            step = min(self.arena_chunk, remaining)
            try:
                self.budget.reserve({Pool.ARENA: step}, timeout=timeout)
            except BudgetExhausted as e:
                self.budget.release({Pool.ARENA: produced})
                raise BudgetStall(f"page {seq} arena: {e}") from e
            try:
                piece = d.decompress(feed, step)
            except zlib.error:
                self.budget.release({Pool.ARENA: step})
                self.budget.release({Pool.ARENA: produced})
                raise
            buf.extend(piece)
            produced += len(piece)
            feed = d.unconsumed_tail
            if len(piece) < step:
                self.budget.release({Pool.ARENA: step - len(piece)})
            if not piece and not feed and not d.eof:
                self.budget.release({Pool.ARENA: produced})
                raise BatchRejected(f"page {seq} compressed stream is truncated")
        return buf, produced

    def _validate(self, buf: _BatchBuffer, summary_seq: int) -> None:
        pages = buf.pages
        if not pages:
            raise BatchRejected("empty batch")
        seqs = sorted(pages)
        if seqs != list(range(seqs[0], seqs[0] + len(seqs))):
            raise BatchRejected(f"page sequence has gaps: {seqs[:4]}...")
        if seqs[-1] != summary_seq:
            raise BatchRejected("last page is not the declared summary page")
        summary_payload = pages[summary_seq]
        earlier = [(s, pages[s]) for s in seqs if s != summary_seq]
        verify_summary(summary_payload, earlier)

    def _fail(self, tenant, batch, buf: _BatchBuffer, exc: BaseException) -> None:
        self.stats.batches_failed += 1
        self.stats.thread_errors.append(f"{tenant}/{batch}: {type(exc).__name__}: {exc}")
        # Discard everything durable/unapproved for this batch before freeing
        # the buffer, so no reader can observe unapproved bytes even briefly.
        try:
            self._purge(tenant, batch)
        finally:
            self.index.unpublish(tenant, batch)
            buf.drop()

    def _purge(self, tenant: str, batch: str) -> None:
        bdir = self.store.batch_dir(tenant, batch)
        if bdir.exists():
            import shutil

            shutil.rmtree(bdir, ignore_errors=True)
            try:
                from .durability import fsync_dir

                fsync_dir(bdir.parent)
            except OSError:
                pass


class _Notifier:
    """Records approved-batch notifications; a failed batch never emits."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.events: list[tuple[str, str]] = []

    def emit(self, tenant: str, batch: str) -> None:
        with self._lock:
            self.events.append((tenant, batch))

    def seen(self, tenant: str, batch: str) -> bool:
        with self._lock:
            return (tenant, batch) in self.events

    def all(self) -> list[tuple[str, str]]:
        with self._lock:
            return list(self.events)


class ReceiverHooks:
    """Fault-injection points spanning producers, validation, and commit."""

    def __init__(self) -> None:
        self.streams: dict = {}
        self.commit_hooks = None
        self._events: dict[str, Callable] = {}

    def on(self, name: str, fn: Callable) -> "ReceiverHooks":
        self._events[name] = fn
        return self

    def at(self, name: str) -> None:
        fn = self._events.get(name)
        if fn is not None:
            fn()
