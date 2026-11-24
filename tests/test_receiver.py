"""End-to-end receiver pipeline: two producers, dedupe, summary faults,
thread abort, crash+recovery, tenant isolation, and invisibility of failures.
"""

import threading

import pytest

from secure_receiver.budget import MIB, Budget, Pool
from secure_receiver.receiver import (
    BatchRejected,
    Receiver,
    ReceiverHooks,
    ThreadAborted,
)
from secure_receiver.store import CrashInjected, Store, TransientIOError
from secure_receiver.streams import BatchStream, build_pages


def make_receiver(tmp_path, budget=None, hooks=None):
    store = Store(tmp_path / "store")
    return Receiver(store, budget or Budget(total_cap=8 * MIB), hooks=hooks or ReceiverHooks())


def wire(receiver, tenant, batch, stream):
    receiver.hooks.streams[(tenant, batch)] = stream


def test_every_exit_path_nets_arena_to_zero(tmp_path):
    """A batch mixing a good page, a zlib-invalid page, and a duplicate must
    fail cleanly and leave every pool at exactly zero afterward."""
    import zlib

    from secure_receiver.streams import RawPage

    r = make_receiver(tmp_path)
    good = zlib.compress(b"a" * 50)

    class MixedProducer:
        def __init__(self, pages):
            self._pages = pages

        def pages(self):
            yield from self._pages

    class MixedStream:
        def split_producers(self):
            pages = [
                RawPage(0, good),
                RawPage(1, b"not zlib at all"),  # zlib.error mid-batch
                RawPage(2, zlib.compress(b"b" * 50)),
                RawPage(2, zlib.compress(b"b" * 50)),  # exact duplicate
            ]
            return [MixedProducer(pages)]

    wire(r, "t", "mixed", MixedStream())
    assert r.ingest_batch("t", "mixed", summary_seq=3) is False
    assert r.budget.total_used == 0
    for p in Pool:
        assert r.budget.used(p) == 0


def test_single_batch_approves_and_reads_back(tmp_path):
    r = make_receiver(tmp_path)
    pages = build_pages(count=12, payload_size=32)
    wire(r, "t", "b1", BatchStream(pages, overlap=0))
    assert r.ingest_batch("t", "b1", summary_seq=11)
    assert r.port.list_batches("t") == ["b1"]
    assert r.port.read("t", "b1") == [(p.seq, p.payload) for p in pages]
    assert r.notifications.seen("t", "b1")


def test_identical_duplicates_are_merged(tmp_path):
    r = make_receiver(tmp_path)
    pages = build_pages(count=10, payload_size=32)
    # Overlap=4 means 4 trailing pages are delivered by BOTH producers.
    wire(r, "t", "b1", BatchStream(pages, overlap=4))
    assert r.ingest_batch("t", "b1", summary_seq=9)
    assert r.stats.duplicates_merged == 4
    # The merged result is byte-identical to a single-producer ingest.
    assert r.port.read("t", "b1") == [(p.seq, p.payload) for p in pages]


def test_divergent_duplicate_fails_batch(tmp_path):
    r = make_receiver(tmp_path)
    pages = build_pages(count=8, payload_size=32)
    # Producer 1 sends seq 2 with different content; batch must fail.
    stream = BatchStream(pages, overlap=3, divergent={2: b"totally different"})
    wire(r, "t", "bad", stream)
    assert r.ingest_batch("t", "bad", summary_seq=7) is False
    assert not r.notifications.seen("t", "bad")
    assert r.port.list_batches("t") == []
    assert not r.store.batch_dir("t", "bad").exists()


def test_last_page_summary_error_rejects_entire_batch(tmp_path):
    """All earlier pages read fine; only the final summary is wrong."""
    r = make_receiver(tmp_path)
    pages = build_pages(count=20, payload_size=24)
    stream = BatchStream(pages, summary_corrupt=True)
    wire(r, "acme", "bad", stream)
    assert r.ingest_batch("acme", "bad", summary_seq=19) is False
    # Invisible from every surface.
    assert not r.notifications.seen("acme", "bad")
    assert r.port.list_batches("acme") == []
    with pytest.raises(KeyError):
        r.port.read("acme", "bad")
    assert not r.store.batch_dir("acme", "bad").exists()
    # Buffers fully reclaimed.
    assert r.budget.total_used == 0


def test_summary_over_wrong_page_set_rejects(tmp_path):
    """Summary is well-formed but computed over a different page set."""
    from secure_receiver.receiver import make_summary

    r = make_receiver(tmp_path)
    pages = build_pages(count=6, payload_size=16)
    # Replace the summary with one over a different (wrong) set of pages.
    wrong = make_summary([(i, b"wrong") for i in range(5)])
    from secure_receiver.receiver import Page

    tampered = pages[:-1] + [Page(seq=5, payload=wrong)]
    wire(r, "t", "b", BatchStream(tampered))
    assert r.ingest_batch("t", "b", summary_seq=5) is False
    assert r.port.list_batches("t") == []


def test_thread_abort_fails_batch_but_tenant_survives(tmp_path):
    r = make_receiver(tmp_path)
    good = build_pages(count=10, payload_size=24)
    wire(r, "t", "good", BatchStream(good))
    assert r.ingest_batch("t", "good", summary_seq=9)

    bad = build_pages(count=10, payload_size=24)
    wire(r, "t", "aborted", BatchStream(bad, abort_at=(1, 3), abort_kind="abort"))
    assert r.ingest_batch("t", "aborted", summary_seq=9) is False

    # The aborted batch is invisible; the earlier good batch still readable.
    assert r.port.list_batches("t") == ["good"]
    assert r.port.read("t", "good") == [(p.seq, p.payload) for p in good]
    assert r.budget.total_used == 0


def test_crash_then_recovery_keeps_approved_only(tmp_path):
    r = make_receiver(tmp_path)
    good = build_pages(count=10, payload_size=24)
    wire(r, "t", "good", BatchStream(good))
    assert r.ingest_batch("t", "good", summary_seq=9)

    crashp = build_pages(count=10, payload_size=24)
    wire(r, "t", "crash", BatchStream(crashp, abort_at=(0, 3), abort_kind="crash"))
    with pytest.raises(CrashInjected):
        r.ingest_batch("t", "crash", summary_seq=9)

    # Simulate restart: a fresh Receiver over the same store.
    r2 = make_receiver(tmp_path)
    r2.index.rebuild_from_store()
    assert r2.port.list_batches("t") == ["good"]
    assert r2.port.read("t", "good") == [(p.seq, p.payload) for p in good]
    with pytest.raises(KeyError):
        r2.port.read("t", "crash")


def test_dir_flushed_pointer_not_flushed_across_restart(tmp_path):
    """Commit faults at the storage layer leave the batch unapproved after a
    restart, while a sibling approved batch remains readable."""
    r = make_receiver(tmp_path)
    good = build_pages(count=8, payload_size=24)
    wire(r, "acme", "good", BatchStream(good))
    assert r.ingest_batch("acme", "good", summary_seq=7)

    ok = build_pages(count=8, payload_size=24)
    wire(r, "acme", "pending", BatchStream(ok))

    class Crash(Exception):
        pass

    def crash_after_dir_fsync(tenant, batch):
        raise Crash("dir flushed, pointer not flushed")

    from secure_receiver.store import CommitHooks

    r.hooks.commit_hooks = CommitHooks({"after_segment_dir_fsync": crash_after_dir_fsync})
    with pytest.raises(Crash):
        r.ingest_batch("acme", "pending", summary_seq=7)

    # Restart and recover: only 'good' is approved and readable.
    r2 = make_receiver(tmp_path)
    r2.index.rebuild_from_store()
    assert r2.port.list_batches("acme") == ["good"]
    with pytest.raises(KeyError):
        r2.port.read("acme", "pending")
    assert not r2.store.batch_dir("acme", "pending").exists()


def test_transient_io_error_retries_and_succeeds(tmp_path):
    """A transient commit error is retried once and the batch then approves."""
    r = make_receiver(tmp_path)
    pages = build_pages(count=10, payload_size=24)
    wire(r, "t", "b", BatchStream(pages))

    from secure_receiver.store import CommitHooks

    calls = {"n": 0}

    def transient_once(tenant, batch):
        calls["n"] += 1
        if calls["n"] == 1:
            raise TransientIOError("disk busy")

    r.hooks.commit_hooks = CommitHooks({"after_segment_fsync": transient_once})
    assert r.ingest_batch("t", "b", summary_seq=9, max_retries=2) is True
    assert r.stats.retries == 1
    assert r.port.list_batches("t") == ["b"]


def test_retry_exhaustion_holds_then_releases_retry_pool(tmp_path):
    """When every commit attempt hits a transient error, the bytes are counted
    in RETRY_HELD during backoff and fully released once the batch fails."""
    r = make_receiver(tmp_path)
    pages = build_pages(count=10, payload_size=24)
    wire(r, "t", "b", BatchStream(pages))

    seen_retry_peak = {}

    def during_backoff():
        seen_retry_peak["peak"] = r.budget.used(Pool.RETRY_HELD)

    r.hooks.on("during_retry_backoff", during_backoff)

    from secure_receiver.store import CommitHooks

    def always_transient(tenant, batch):
        raise TransientIOError("disk busy")

    r.hooks.commit_hooks = CommitHooks({"after_segment_fsync": always_transient})

    assert r.ingest_batch("t", "b", summary_seq=9, max_retries=2) is False
    assert r.stats.retries == 2
    assert seen_retry_peak["peak"] > 0  # bytes were independently counted
    assert r.budget.used(Pool.RETRY_HELD) == 0
    assert r.budget.total_used == 0
    assert r.port.list_batches("t") == []


def test_tenant_isolation_end_to_end(tmp_path):
    r = make_receiver(tmp_path)
    for tenant in ("alpha", "beta", "gamma"):
        pages = build_pages(count=8, payload_size=20, prefix=tenant.encode())
        wire(r, tenant, "b", BatchStream(pages))
        assert r.ingest_batch(tenant, "b", summary_seq=7)
    # Corrupt one tenant's batch by failing a later one; others unaffected.
    bad = build_pages(count=8, payload_size=20)
    wire(r, "beta", "bad", BatchStream(bad, summary_corrupt=True))
    assert r.ingest_batch("beta", "bad", summary_seq=7) is False
    assert r.port.list_batches("beta") == ["b"]
    assert r.port.list_batches("alpha") == ["b"]
    assert r.port.list_batches("gamma") == ["b"]
    # Each tenant reads only its own bytes.
    a = r.port.read("alpha", "b")
    c = r.port.read("gamma", "b")
    assert a[0][1].startswith(b"alpha")
    assert c[0][1].startswith(b"gamma")
    assert a != c


def test_failed_batch_never_notifies_and_index_stays_clean(tmp_path):
    r = make_receiver(tmp_path)
    for i in range(5):
        pages = build_pages(count=6, payload_size=16)
        good = i % 2 == 0
        stream = BatchStream(pages, summary_corrupt=not good)
        wire(r, "t", f"b{i}", stream)
        r.ingest_batch("t", f"b{i}", summary_seq=5)
    approved = set(r.port.list_batches("t"))
    notified = {b for (t, b) in r.notifications.all()}
    assert approved == notified == {"b0", "b2", "b4"}
    for b in ("b1", "b3"):
        assert not r.notifications.seen("t", b)
        with pytest.raises(KeyError):
            r.port.read("t", b)


def test_batch_larger_than_budget_fails_cleanly(tmp_path):
    """A batch whose pages cannot all fit the budget must fail as a batch --
    not deadlock, not leak, and not partially publish."""
    tiny = Budget(total_cap=256 * 1024)  # 256 KiB working budget
    r = make_receiver(tmp_path, budget=tiny)
    # 400 pages * ~200 bytes decoded ~= 80 KiB... make it clearly too big.
    pages = build_pages(count=400, payload_size=1024)  # ~400 KiB decoded
    wire(r, "t", "huge", BatchStream(pages))
    assert r.ingest_batch("t", "huge", summary_seq=399, reserve_timeout=0.2) is False
    assert r.port.list_batches("t") == []
    assert tiny.total_used == 0  # every charge returned


def test_reserve_timeout_does_not_hang(tmp_path):
    """An all-pages-too-big batch returns within the reserve budget."""
    import time

    tiny = Budget(total_cap=64 * 1024)
    r = make_receiver(tmp_path, budget=tiny)
    pages = build_pages(count=200, payload_size=2048)
    wire(r, "t", "b", BatchStream(pages))
    t0 = time.monotonic()
    ok = r.ingest_batch("t", "b", summary_seq=199, reserve_timeout=0.2)
    elapsed = time.monotonic() - t0
    assert ok is False
    assert elapsed < 10.0
    assert tiny.total_used == 0
