"""Benchmark assertions.

The full 100k run is heavy for CI, so the default test runs a scaled-down but
representative version that still exercises two producers, overlap, real
fsync commits, and the budget.  The full 100k run is available via
``python -m bench.run_benchmark 100000`` and its numbers are recorded in
``bench/reports/bench_100k.json``.
"""

import os

import pytest

from bench import run_benchmark
from secure_receiver.budget import MIB, Pool


@pytest.mark.parametrize("total", [3000])
def test_benchmark_reports_peak_and_holds_cap(total):
    report = run_benchmark.run(
        total_pages=total,
        batch_pages=500,
        page_payload=96,
        overlap=16,
        total_cap=8 * MIB,
        trace=False,
    )
    assert report.budget_never_exceeded
    assert report.total_peak_bytes <= report.budget_cap
    assert report.pages_per_s > 0
    # Duplicate merge really happened (overlap pages were resubmitted).
    assert report.duplicate_merges > 0
    # Every pool reports a peak attribution.
    assert {p.pool for p in report.pool_peaks} == {p.value for p in Pool}
    # INDEX_QUEUE holds the whole validated batch, so it dominates the
    # per-page pools.  ARENA/COMPRESSED are bounded by a chunk/per-page, never
    # by batch size.
    peaks = {p.pool: p.peak_bytes for p in report.pool_peaks}
    assert peaks[Pool.INDEX_QUEUE.value] >= peaks[Pool.COMPRESSED.value]
    assert peaks[Pool.INDEX_QUEUE.value] >= peaks[Pool.RETRY_HELD.value]
    # Reclaim schedule is present for every pool.
    assert len(report.reclaim_schedule) == 5


def test_arena_and_compressed_are_small_even_with_big_batches(tmp_path):
    """A batch far larger than one chunk must not make ARENA or COMPRESSED
    scale with batch size: they are per-page, not per-batch."""
    report = run_benchmark.run(
        total_pages=2000,
        batch_pages=1000,
        page_payload=256,
        overlap=8,
        total_cap=8 * MIB,
        trace=False,
    )
    peaks = {p.pool: p.peak_bytes for p in report.pool_peaks}
    # ARENA is bounded by one chunk per concurrent producer (2 * 16 KiB).
    assert peaks[Pool.ARENA.value] <= 2 * 16 * 1024 + 4096
    # COMPRESSED is bounded by the compressed size of one page per producer.
    assert peaks[Pool.COMPRESSED.value] <= 2 * 1024
