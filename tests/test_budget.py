"""Budget ledger invariants: independent counting, atomicity, peaks."""

import threading

import pytest

from secure_receiver.budget import (
    MIB,
    Budget,
    BudgetExhausted,
    Pool,
)


def test_independent_pools_sum_to_total():
    b = Budget(total_cap=8 * MIB)
    # Charge all five pools independently.
    reqs = {
        Pool.COMPRESSED: 1 * MIB,
        Pool.ARENA: 1 * MIB,
        Pool.INDEX_QUEUE: 1 * MIB,
        Pool.PENDING_PAGES: 1 * MIB,
        Pool.RETRY_HELD: 1 * MIB,
    }
    b.reserve(reqs)
    assert b.total_used == 5 * MIB
    for p, n in reqs.items():
        assert b.used(p) == n
    b.release(reqs)
    assert b.total_used == 0


def test_cap_is_hard_and_request_is_atomic():
    b = Budget(total_cap=8 * MIB)
    # Two independent reservations that individually fit but together exceed.
    b.reserve({Pool.PENDING_PAGES: 5 * MIB})
    # try_reserve is the fail-fast probe; reserve() would instead wait.
    with pytest.raises(BudgetExhausted):
        b.try_reserve({Pool.PENDING_PAGES: 4 * MIB})
    # The failed request charged nothing.
    assert b.used(Pool.PENDING_PAGES) == 5 * MIB
    # An atomic multi-pool request that cannot fully fit charges nothing.
    with pytest.raises(BudgetExhausted):
        b.try_reserve({Pool.COMPRESSED: 2 * MIB, Pool.ARENA: 2 * MIB})
    assert b.used(Pool.COMPRESSED) == 0 and b.used(Pool.ARENA) == 0


def test_reserve_times_out_when_never_freed():
    b = Budget(total_cap=1 * MIB)
    b.reserve({Pool.ARENA: 1 * MIB})
    with pytest.raises(BudgetExhausted):
        b.reserve({Pool.ARENA: 1 * MIB}, timeout=0.05)


def test_oversized_request_fails_fast_not_deadlock():
    b = Budget(total_cap=1 * MIB)
    with pytest.raises(BudgetExhausted):
        b.reserve({Pool.ARENA: 2 * MIB}, timeout=None)


def test_per_pool_cap():
    b = Budget(total_cap=8 * MIB, per_pool={Pool.ARENA: 1 * MIB})
    with pytest.raises(BudgetExhausted):
        b.reserve({Pool.ARENA: 2 * MIB})
    # Other pools unaffected.
    b.reserve({Pool.PENDING_PAGES: 2 * MIB})
    assert b.used(Pool.PENDING_PAGES) == 2 * MIB


def test_peak_tracks_high_water_mark():
    b = Budget(total_cap=8 * MIB)
    with b.hold({Pool.PENDING_PAGES: 3 * MIB}):
        pass
    with b.hold({Pool.PENDING_PAGES: 2 * MIB}):
        pass
    assert b.peak(Pool.PENDING_PAGES) == 3 * MIB
    assert b.total_peak == 3 * MIB
    assert b.total_used == 0


def test_transfer_preserves_total_and_moves_pool():
    b = Budget(total_cap=8 * MIB)
    b.reserve({Pool.ARENA: 1 * MIB})
    b.transfer(Pool.ARENA, Pool.PENDING_PAGES, 1 * MIB)
    assert b.used(Pool.ARENA) == 0
    assert b.used(Pool.PENDING_PAGES) == 1 * MIB
    assert b.total_used == 1 * MIB
    assert b.peak(Pool.PENDING_PAGES) == 1 * MIB


def test_two_threads_never_exceed_cap():
    b = Budget(total_cap=8 * MIB)
    stop = threading.Event()
    violations = []

    def worker():
        while not stop.is_set():
            with b.hold({Pool.COMPRESSED: 256 * 1024, Pool.ARENA: 256 * 1024}):
                # Sum of all pools is never above the cap while both hold.
                if b.total_used > 8 * MIB:
                    violations.append(b.total_used)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    import time

    time.sleep(0.3)
    stop.set()
    for t in threads:
        t.join()
    assert not violations
    assert b.total_used == 0


def test_blocking_reserve_wakes_on_release():
    b = Budget(total_cap=1 * MIB)
    b.reserve({Pool.ARENA: 1 * MIB})
    done = threading.Event()

    def release_soon():
        import time

        time.sleep(0.05)
        b.release({Pool.ARENA: 1 * MIB})
        done.set()

    threading.Thread(target=release_soon).start()
    b.reserve({Pool.ARENA: 1 * MIB}, timeout=2.0)  # blocks until released
    assert done.wait(1.0)
    b.release({Pool.ARENA: 1 * MIB})
