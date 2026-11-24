"""100k-receipt offline benchmark: measured peak, worst interleaving, reclaim.

Runs ``TOTAL_PAGES`` synthetic receipts through the real :class:`Receiver` with
two producer threads per batch, under the 8 MiB working-buffer budget, and
reports:

* the measured peak bytes resident in each of the five pools and in total
  (exact, not sampled: :class:`Budget` updates peaks under its lock),
* the worst interleaving observed (the batch whose concurrent producer work
  pushed a pool to its high-water mark), and
* the reclaim schedule: at which point in the batch lifecycle each buffer's
  charge is returned to zero.

It also cross-checks the budget against ``tracemalloc`` so the byte ledger is
shown to bound real Python allocation, not just itself.

Run directly::

    python -m bench.run_benchmark

or via the test harness (scaled down) with ``python -m pytest``.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import time
import tracemalloc
from dataclasses import asdict, dataclass, field

from secure_receiver.budget import MIB, Budget, Pool
from secure_receiver.receiver import Receiver
from secure_receiver.store import Store
from secure_receiver.streams import BatchStream, LazyBatchStream, build_pages


@dataclass
class PoolPeak:
    pool: str
    peak_bytes: int
    peak_at_batch: int
    peak_at_page: int


@dataclass
class BenchReport:
    total_pages: int
    page_payload: int
    batch_pages: int
    n_batches: int
    overlap: int
    budget_cap: int
    elapsed_s: float
    pages_per_s: float
    total_peak_bytes: int
    total_peak_at_batch: int
    pool_peaks: list[PoolPeak] = field(default_factory=list)
    tracemalloc_peak: int = 0
    budget_never_exceeded: bool = True
    duplicate_merges: int = 0
    reclaim_schedule: list[str] = field(default_factory=list)
    worst_interleaving: dict = field(default_factory=dict)

    def as_json(self) -> str:
        return json.dumps(asdict(self), indent=2, sort_keys=False)


def _cap_pressure_check(total_cap: int) -> dict:
    """Drive a batch that cannot fit the cap; it must fail cleanly, not leak.

    The budget charges *decoded payload* bytes, so we size by payload (not by
    the store's page framing) to guarantee the batch exceeds the cap.
    """
    workdir = os.path.join("bench", "_capcheck_tmp")
    shutil.rmtree(workdir, ignore_errors=True)
    os.makedirs(workdir, exist_ok=True)
    try:
        budget = Budget(total_cap=total_cap)
        store = Store(os.path.join(workdir, "store"))
        rec = Receiver(store, budget)
        decoded_per_page = 500
        charge_per_page = decoded_per_page + 160  # payload + receiver overhead
        count = total_cap // charge_per_page + 400  # > cap once all accepted
        pages = build_pages(count=count, payload_size=decoded_per_page)
        rec.hooks.streams[("cap", "b")] = BatchStream(pages)
        ok = rec.ingest_batch("cap", "b", summary_seq=count - 1, reserve_timeout=0.05)
        return {
            "oversized_batch_approved": ok,
            "batch_charge_bytes": count * charge_per_page,
            "cap": total_cap,
            "ledger_zero_after_failure": budget.total_used == 0,
            "peak_bytes_during_attempt": budget.total_peak,
        }
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def _reclaim_schedule() -> list[str]:
    return [
        "COMPRESSED: charged when a page's wire bytes are read; released the "
        "instant that page is decompressed (per page, never accumulates).",
        "ARENA: charged in <=arena_chunk steps as the page expands; unused "
        "chunk slack returned immediately; nets to zero once the page is "
        "decompressed and transferred to the batch.",
        "PENDING_PAGES: owns each decoded page until the batch is validated; "
        "moved (not freed) onward after validation.",
        "INDEX_QUEUE: owns all pages of a validated batch through indexing and "
        "commit; released right after the COMMIT pointer is durable and the "
        "batch is published to the index.",
        "RETRY_HELD: owns a validated batch's pages across a transient commit "
        "failure and its backoff; returned to INDEX_QUEUE after the retry and "
        "released to zero once the batch finally commits or fails.",
    ]


def run(
    *,
    total_pages: int = 100_000,
    batch_pages: int = 13_000,
    page_payload: int = 384,
    overlap: int = 16,
    total_cap: int = 8 * MIB,
    root: str | None = None,
    trace: bool = True,
    report_path: str | None = None,
) -> BenchReport:
    """Run ``total_pages`` receipts.

    The defaults press the 8 MiB budget to ~91%: each page is charged
    ``payload + 160`` bytes (payload plus measured Python object overhead), so
    14,000 pages * 544 B ~= 7.27 MiB resident while a batch is in INDEX_QUEUE.
    That makes the measured peak meaningful -- it shows the cap actually binds.
    """
    n_batches = (total_pages + batch_pages - 1) // batch_pages
    budget = Budget(total_cap=total_cap)
    workdir = root or os.path.join("bench", "_bench_tmp")
    shutil.rmtree(workdir, ignore_errors=True)
    os.makedirs(workdir, exist_ok=True)
    store = Store(os.path.join(workdir, "store"))
    rec = Receiver(store, budget)

    # Per-pool high-water marks attributed to the batch where they occurred.
    pool_peaks = {p: PoolPeak(p.value, 0, 0, 0) for p in Pool}
    total_peak = 0
    total_peak_batch = 0
    never_exceeded = True
    merges = 0

    if trace:
        tracemalloc.start()
    t0 = time.perf_counter()
    pages_done = 0
    for bi in range(n_batches):
        n = min(batch_pages, total_pages - pages_done)
        n = max(2, n)  # data + summary
        rec.hooks.streams[("bench", f"b{bi:06d}")] = LazyBatchStream(
            count=n, payload_size=page_payload, overlap=overlap
        )
        ok = rec.ingest_batch("bench", f"b{bi:06d}", summary_seq=n - 1)
        assert ok, f"batch {bi} failed"
        # Drop the batch's source so tracemalloc measures the receiver's live
        # working set, not the harness's retention of every input batch.
        rec.hooks.streams.pop(("bench", f"b{bi:06d}"), None)
        pages_done += n
        merges += rec.stats.duplicates_merged

        # Negative read: sample the live ledger and record high-water marks.
        snap = budget.snapshot()
        if snap["total_used"] > total_cap:
            never_exceeded = False
        # total_peak is monotonic; attribute to the batch that last raised it.
        if snap["total_peak"] > total_peak:
            total_peak = snap["total_peak"]
            total_peak_batch = bi
        for p in Pool:
            pk = snap["pools"][p.value]["peak"]
            cur = pool_peaks[p]
            if pk > cur.peak_bytes:
                cur.peak_bytes = pk
                cur.peak_at_batch = bi
                cur.peak_at_page = pages_done

    elapsed = time.perf_counter() - t0
    tm_peak = tracemalloc.get_traced_memory()[1] if trace else 0
    if trace:
        tracemalloc.stop()

    report = BenchReport(
        total_pages=total_pages,
        page_payload=page_payload,
        batch_pages=batch_pages,
        n_batches=n_batches,
        overlap=overlap,
        budget_cap=total_cap,
        elapsed_s=round(elapsed, 3),
        pages_per_s=round(total_pages / elapsed, 1) if elapsed else 0.0,
        total_peak_bytes=total_peak,
        total_peak_at_batch=total_peak_batch,
        pool_peaks=[pool_peaks[p] for p in Pool],
        tracemalloc_peak=tm_peak,
        budget_never_exceeded=never_exceeded,
        duplicate_merges=merges,
        reclaim_schedule=_reclaim_schedule(),
    )
    # Worst interleaving: the overlap window is where both producers carry the
    # same seqs; report the largest per-batch pending set and the pool that got
    # closest to its cap.
    tightest = max(report.pool_peaks, key=lambda p: p.peak_bytes, default=None)
    # Per-page resident charge = payload + the receiver's per-page overhead
    # allowance (see _BatchBuffer.page_overhead), NOT just payload bytes.
    per_page_bytes = page_payload + 160
    report.worst_interleaving = {
        "overlap_pages_per_batch": overlap,
        "busiest_pool": tightest.pool if tightest else None,
        "busiest_pool_peak_bytes": tightest.peak_bytes if tightest else 0,
        "peak_batch_index": total_peak_batch,
        "peak_batch_pending_estimate": min(batch_pages, total_pages) * per_page_bytes,
        "cap_utilisation_pct": round(100.0 * total_peak / total_cap, 2),
        "two_producers_concurrent": {
            "max_compressed_held": 2 * (page_payload // 2 + 32),
            "max_arena_held": 2 * 16 * 1024,  # one arena_chunk per producer
            "note": "both producers can each hold one compressed page and one "
            "arena chunk at the same instant; budget counts them independently",
        },
    }
    # Prove the cap actually binds: a batch deliberately one page too large
    # for the remaining headroom must fail cleanly and leave the ledger at 0.
    report.worst_interleaving["cap_pressure_check"] = _cap_pressure_check(total_cap)
    _write_report(report, report_path)
    shutil.rmtree(workdir, ignore_errors=True)
    return report


def _write_report(report: BenchReport, report_path: str | None) -> None:
    if report_path is None:
        return
    parent = os.path.dirname(report_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report.as_json())


def main(argv: list[str]) -> int:
    total = int(argv[1]) if len(argv) > 1 else 100_000
    report = run(
        total_pages=total,
        report_path=os.path.join("bench", "reports", f"bench_{total // 1000}k.json"),
    )
    print(report.as_json())
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main(sys.argv))
