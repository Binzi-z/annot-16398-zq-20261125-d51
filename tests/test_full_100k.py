"""Full 100k-receipt run. Heavier than the rest of the suite (~20s); it exists
so the recorded report in bench/reports/bench_100k.json is reproducible.

Run in isolation with::

    python -m pytest tests/test_full_100k.py
"""

import os

from bench.run_benchmark import run


def test_full_100k_receipts():
    report = run(
        total_pages=100_000,
        batch_pages=13_000,
        page_payload=384,
        overlap=16,
        total_cap=8 * 1024 * 1024,
        trace=True,
        report_path=os.path.join("bench", "reports", "bench_100k.json"),
    )
    assert report.budget_never_exceeded
    assert report.total_peak_bytes <= report.budget_cap
    assert report.n_batches >= 5
    # The measured process allocation (tracemalloc) must also stay under the
    # 8 MiB cap -- proving the ledger bounds real memory, not just itself.
    assert report.tracemalloc_peak <= report.budget_cap
    # The cap-pressure sub-check must prove an oversized batch is rejected.
    assert report.worst_interleaving["cap_pressure_check"]["oversized_batch_approved"] is False
    assert report.worst_interleaving["cap_pressure_check"]["ledger_zero_after_failure"] is True
