"""Synthetic receipt streams for the two-producer ingest path.

A :class:`BatchStream` holds an ordered list of logical pages and splits them
across two producer iterators.  The split has a deliberate *overlap*: the last
``overlap`` pages handed out by producer 0 are also handed out by producer 1,
so both threads carry the same sequence numbers.  Identical overlap pages must
merge; a test may inject a divergent payload on producer 1 for a chosen seq to
force a conflict.

Wire format per page is zlib-compressed payload bytes; the sequence number
travels alongside as :class:`RawPage`.  The stream can inject a producer
abort/crash at a precise (producer, position).
"""

from __future__ import annotations

import hashlib
import struct
import zlib
from dataclasses import dataclass
from typing import Iterator

from .receiver import SUMMARY_MAGIC, SUMMARY_STRUCT, Page, make_summary
from .store import CrashInjected


@dataclass
class RawPage:
    seq: int
    payload: bytes  # zlib-compressed


def _compress(payload: bytes, level: int = 6) -> bytes:
    return zlib.compress(payload, level)


def build_pages(
    *,
    count: int,
    payload_size: int = 64,
    prefix: bytes = b"",
    compress_level: int = 6,
) -> list[Page]:
    """Build ``count - 1`` data pages plus a valid summary page at the end.

    The summary is page ``count - 1`` (0-based); it is computed over the data
    pages so the batch validates end to end.
    """
    if count < 2:
        raise ValueError("count must be at least 2 (data + summary)")
    data: list[Page] = []
    for i in range(count - 1):
        body = prefix + (f"page-{i:08d}-".encode() + b"." * payload_size)
        data.append(Page(seq=i, payload=body))
    summary = Page(
        seq=count - 1,
        payload=make_summary([(p.seq, p.payload) for p in data]),
    )
    return data + [summary]


class BatchStream:
    """A batch's logical pages, split across two overlapping producers."""

    def __init__(
        self,
        pages: list[Page],
        *,
        overlap: int = 0,
        compress_level: int = 6,
        divergent: dict[int, bytes] | None = None,
        abort_at: tuple[int, int] | None = None,
        abort_kind: str = "abort",
        summary_corrupt: bool = False,
    ) -> None:
        """
        overlap       trailing pages of producer 0 re-sent by producer 1
                      (exercises duplicate merge on the same seq).
        divergent     {seq: alternative_payload} injected on producer 1 for
                      that seq (exercises conflict rejection).  The seq must
                      be inside the overlap window to be effective.
        abort_at      (producer_index, position) at which to raise a fault.
        abort_kind    "abort" -> ThreadAborted (one producer dies, the batch
                      fails but the process lives on);
                      "crash" -> CrashInjected (emulated abrupt process stop,
                      so the caller must recover).
        summary_corrupt  flip a byte in the last (summary) page.
        """
        self.pages = pages
        self.overlap = overlap
        self.compress_level = compress_level
        self.divergent = dict(divergent or {})
        self.abort_at = abort_at
        self.abort_kind = abort_kind
        self.summary_corrupt = summary_corrupt

    def split_producers(self) -> list["Producer"]:
        n = len(self.pages)
        half = (n + 1) // 2
        overlap = min(self.overlap, half)
        # Producer 0 owns pages [0, half).  Producer 1 starts `overlap` pages
        # before the midpoint (re-sending those) and runs to the end.  Union is
        # every page exactly once except the overlap window, which is twice.
        p0 = list(range(0, half))
        p1 = list(range(half - overlap, n))
        return [Producer(self, 0, p0), Producer(self, 1, p1)]

    def wire_for(self, producer_index: int, page_index: int) -> RawPage:
        page = self.pages[page_index]
        payload = page.payload
        if self.summary_corrupt and page_index == len(self.pages) - 1:
            payload = _flip(payload)
        if producer_index == 1 and page.seq in self.divergent:
            payload = self.divergent[page.seq]
        return RawPage(seq=page.seq, payload=_compress(payload, self.compress_level))


class Producer:
    def __init__(self, stream: BatchStream, index: int, order: list[int]) -> None:
        self.stream = stream
        self.index = index
        self.order = order

    def pages(self) -> Iterator[RawPage]:
        for pos, page_index in enumerate(self.order):
            if self.stream.abort_at == (self.index, pos):
                kind = "crashed" if self.stream.abort_kind == "crash" else "aborted"
                msg = f"producer {self.index} {kind} at position {pos}"
                if self.stream.abort_kind == "crash":
                    raise CrashInjected(msg)
                from .receiver import ThreadAborted

                raise ThreadAborted(msg)
            yield self.stream.wire_for(self.index, page_index)


def _flip(data: bytes) -> bytes:
    if not data:
        return b"\x00"
    b = bytearray(data)
    b[0] ^= 0xFF
    return bytes(b)


class LazyBatchStream:
    """A batch whose pages are generated on demand, never held all at once.

    This models a real wire source: the sender streams pages and the receiver
    is the only component that retains a full batch (inside the budget).  The
    summary page is computed over the data pages, which are regenerated
    deterministically, so no list of ``Page`` objects is kept resident -- the
    benchmark's process footprint then reflects the receiver, not the harness.
    """

    def __init__(
        self,
        *,
        count: int,
        payload_size: int,
        prefix: bytes = b"",
        overlap: int = 0,
        compress_level: int = 6,
    ) -> None:
        if count < 2:
            raise ValueError("count must be at least 2")
        self.count = count
        self.payload_size = payload_size
        self.prefix = prefix
        self.overlap = overlap
        self.compress_level = compress_level
        self._data_cache: dict[int, bytes] = {}

    def data_payload(self, i: int) -> bytes:
        p = self._data_cache.get(i)
        if p is None:
            p = self.prefix + (f"page-{i:08d}-".encode() + b"." * self.payload_size)
            # Keep at most a handful cached; regeneration is deterministic.
            if len(self._data_cache) < 64:
                self._data_cache[i] = p
        return p

    def summary_payload(self) -> bytes:
        digest = hashlib.sha256()
        for i in range(self.count - 1):
            p = self.data_payload(i)
            digest.update(struct.pack(">II", i, len(p)))
            digest.update(p)
        return SUMMARY_STRUCT.pack(SUMMARY_MAGIC, self.count, 0, digest.digest())

    def payload_for(self, index: int) -> bytes:
        if index == self.count - 1:
            return self.summary_payload()
        return self.data_payload(index)

    def split_producers(self):
        n = self.count
        half = (n + 1) // 2
        overlap = min(self.overlap, half)
        p0 = list(range(0, half))
        p1 = list(range(half - overlap, n))
        return [LazyProducer(self, 0, p0), LazyProducer(self, 1, p1)]


class LazyProducer:
    def __init__(self, stream: LazyBatchStream, index: int, order: list[int]) -> None:
        self.stream = stream
        self.index = index
        self.order = order

    def pages(self) -> Iterator[RawPage]:
        for page_index in self.order:
            wire = zlib.compress(
                self.stream.payload_for(page_index), self.stream.compress_level
            )
            yield RawPage(seq=page_index, payload=wire)
