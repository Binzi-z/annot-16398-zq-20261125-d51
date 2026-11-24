"""Allocation ledger with independently counted pools under one hard cap.

Every byte of working memory that the receiver holds is charged to exactly
one :class:`Pool`.  The five pools are intentionally *independent*: a byte
counted as compressed input is not also counted as decompression arena, so a
single ``compressed -> arena -> index -> pending -> retry`` pipeline charges
each stage separately and the sum is the true resident working set.

The ledger enforces a single global cap (8 MiB by default) *in addition* to
per-pool caps, and it can acquire several pools atomically so a caller never
gets a partial reservation it cannot use.

Thread safety: all mutation happens under one ``threading.Condition``.  Two
producer threads contend on the same ledger; release() notifies waiters.
"""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from enum import Enum

MIB = 1024 * 1024


class Pool(str, Enum):
    """The five independently counted working-buffer pools."""

    COMPRESSED = "compressed_input"
    ARENA = "decompression_arena"
    INDEX_QUEUE = "index_queue"
    PENDING_PAGES = "pending_pages"
    RETRY_HELD = "retry_held"


ALL_POOLS = tuple(Pool)


class BudgetExhausted(Exception):
    """Raised by :meth:`Budget.try_reserve` / non-blocking paths."""


class Budget:
    """A byte ledger with one global cap and per-pool accounting.

    ``per_pool`` caps are optional; the default is the global cap.  Use a
    zero cap to forbid a pool entirely (useful in tests).
    """

    def __init__(
        self,
        total_cap: int = 8 * MIB,
        per_pool: dict[Pool, int] | None = None,
    ) -> None:
        if total_cap <= 0:
            raise ValueError("total_cap must be positive")
        self._total_cap = total_cap
        self._per_pool_cap = {p: total_cap for p in ALL_POOLS}
        if per_pool:
            for p, cap in per_pool.items():
                if not isinstance(p, Pool):
                    p = Pool(p)
                if cap <= 0:
                    raise ValueError(f"cap for {p.value} must be positive")
                self._per_pool_cap[p] = min(cap, total_cap)

        self._used = {p: 0 for p in ALL_POOLS}
        self._peak = {p: 0 for p in ALL_POOLS}
        self._total_used = 0
        self._total_peak = 0

        # Observability for the benchmark / report.
        self._waits = 0
        self._wait_seconds = 0.0
        self._denied = 0

        self._cond = threading.Condition(threading.Lock())

    # -- introspection -------------------------------------------------
    @property
    def total_cap(self) -> int:
        return self._total_cap

    @property
    def total_used(self) -> int:
        with self._cond:
            return self._total_used

    @property
    def total_peak(self) -> int:
        with self._cond:
            return self._total_peak

    def used(self, pool: Pool) -> int:
        with self._cond:
            return self._used[Pool(pool)]

    def peak(self, pool: Pool) -> int:
        with self._cond:
            return self._peak[Pool(pool)]

    def snapshot(self) -> dict:
        with self._cond:
            return {
                "total_cap": self._total_cap,
                "total_used": self._total_used,
                "total_peak": self._total_peak,
                "pools": {
                    p.value: {
                        "used": self._used[p],
                        "peak": self._peak[p],
                        "cap": self._per_pool_cap[p],
                    }
                    for p in ALL_POOLS
                },
                "waits": self._waits,
                "wait_seconds": round(self._wait_seconds, 6),
                "denied": self._denied,
            }

    # -- internal helpers ---------------------------------------------
    def _fits(self, req: dict[Pool, int]) -> bool:
        add = sum(req.values())
        if self._total_used + add > self._total_cap:
            return False
        for p, n in req.items():
            if self._used[p] + n > self._per_pool_cap[p]:
                return False
        return True

    def _apply(self, req: dict[Pool, int]) -> None:
        add = 0
        for p, n in req.items():
            self._used[p] += n
            if self._used[p] > self._peak[p]:
                self._peak[p] = self._used[p]
            add += n
        self._total_used += add
        if self._total_used > self._total_peak:
            self._total_peak = self._total_used

    @staticmethod
    def _normalize(req: dict) -> dict[Pool, int]:
        out: dict[Pool, int] = {}
        for p, n in req.items():
            if n <= 0:
                continue
            out[Pool(p)] = out.get(Pool(p), 0) + n
        return out

    # -- acquisition ---------------------------------------------------
    def _impossible(self, r: dict[Pool, int]) -> str | None:
        """Return a reason if no amount of waiting could ever satisfy ``r``."""
        if sum(r.values()) > self._total_cap:
            return (
                f"request {sum(r.values())} exceeds total cap {self._total_cap}"
            )
        for p, n in r.items():
            if n > self._per_pool_cap[p]:
                return f"request {n} exceeds {p.value} cap {self._per_pool_cap[p]}"
        return None

    def try_reserve(self, req: dict) -> None:
        """Reserve all pools atomically or raise :class:`BudgetExhausted`."""
        r = self._normalize(req)
        if not r:
            return
        with self._cond:
            bad = self._impossible(r)
            if bad is not None:
                self._denied += 1
                raise BudgetExhausted(bad)
            if not self._fits(r):
                self._denied += 1
                raise BudgetExhausted(
                    f"cannot reserve {sum(r.values())} bytes: "
                    f"used={self._total_used} cap={self._total_cap}"
                )
            self._apply(r)

    def reserve(self, req: dict, timeout: float | None = None) -> None:
        """Block until all pools in ``req`` can be reserved atomically.

        A request larger than the cap fails immediately rather than blocking
        forever, which is what a single oversized page must do.
        """
        r = self._normalize(req)
        if not r:
            return
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            bad = self._impossible(r)
            if bad is not None:
                self._denied += 1
                raise BudgetExhausted(bad)
            while not self._fits(r):
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        self._denied += 1
                        raise BudgetExhausted(
                            f"timed out reserving {sum(r.values())} bytes"
                        )
                else:
                    remaining = None
                self._waits += 1
                t0 = time.monotonic()
                self._cond.wait(remaining)
                self._wait_seconds += time.monotonic() - t0
            self._apply(r)

    def transfer(self, src: Pool, dst: Pool, n: int) -> None:
        """Move ``n`` reserved bytes from one pool to another, total unchanged.

        Used when the *same* bytes advance a pipeline stage (index -> pending,
        pending -> retry).  It never changes the global total, so a stage move
        cannot fail from global exhaustion; it still updates per-pool peaks.
        """
        if n <= 0:
            return
        src, dst = Pool(src), Pool(dst)
        with self._cond:
            if self._used[src] < n:
                raise ValueError(
                    f"transfer of {n} from {src.value} exceeds used {self._used[src]}"
                )
            if src == dst:
                return
            if self._used[dst] + n > self._per_pool_cap[dst]:
                raise BudgetExhausted(
                    f"transfer into {dst.value} would exceed its cap "
                    f"({self._used[dst]} + {n} > {self._per_pool_cap[dst]})"
                )
            self._used[src] -= n
            self._used[dst] += n
            if self._used[dst] > self._peak[dst]:
                self._peak[dst] = self._used[dst]

    def release(self, req: dict) -> None:
        r = self._normalize(req)
        if not r:
            return
        with self._cond:
            for p, n in r.items():
                if n > self._used[p]:
                    raise ValueError(
                        f"release of {n} from {p.value} exceeds used {self._used[p]}"
                    )
            self._apply_release(r)

    def _apply_release(self, r: dict[Pool, int]) -> None:
        sub = 0
        for p, n in r.items():
            self._used[p] -= n
            sub += n
        self._total_used -= sub
        if sub:
            self._cond.notify_all()

    @contextmanager
    def hold(self, req: dict, timeout: float | None = None):
        """Reserve on enter, release on exit (exception-safe)."""
        r = self._normalize(req)
        self.reserve(r, timeout=timeout)
        try:
            yield self
        finally:
            self.release(r)

    def charge(self, pool: Pool, n: int) -> "_Charge":
        """Return an object usable as a context manager for one pool."""
        return _Charge(self, {Pool(pool): n})


class _Charge:
    __slots__ = ("_budget", "_req", "_held")

    def __init__(self, budget: Budget, req: dict[Pool, int]) -> None:
        self._budget = budget
        self._req = req
        self._held = False

    def __enter__(self) -> Budget:
        self._budget.reserve(self._req)
        self._held = True
        return self._budget

    def __exit__(self, *exc) -> None:
        if self._held:
            self._budget.release(self._req)
            self._held = False
