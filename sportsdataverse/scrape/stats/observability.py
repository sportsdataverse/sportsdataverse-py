"""Sweep observability: heartbeat, miss categorisation, proxy health, degradation alerts.

A multi-hour sweep that only logs at season boundaries is silent for 30-60 minutes
at a stretch, reports one opaque "misses" number, and gives no view of the proxy
pool at all. That makes three different situations look identical: working
normally, blocked by a throttled pool, and hung.

Four pieces, all additive:

:class:`Progress`
    Periodic per-game heartbeat with rate and ETA, so a stall is visible within a
    minute instead of at the next season boundary.

:func:`classify`
    Buckets a failed fetch into ``endpoint_absent`` / ``timeout`` / ``throttled`` /
    ``error``. The first is the important one: most early-season "misses" are
    endpoints that did not exist yet, which is expected and permanent -- not a
    failure, and not worth retrying on the next run.

:class:`ProxyHealth`
    Per-proxy outcome counters with quarantine. The scraper picks the proxy for
    every call, so this needs nothing from sdv-py: the caller already knows which
    IP it used.

:class:`Degradation`
    Escalates to a loud warning when the real-error rate spikes or the healthy pool
    shrinks past a floor, so a multi-hour run against a dead pool announces itself
    rather than quietly burning the budget.

Everything here is pure bookkeeping over outcomes the caller reports -- no HTTP, no
globals -- so it is all testable offline.
"""

from __future__ import annotations

import time
from collections import Counter, defaultdict
from collections.abc import Callable

# -- outcome vocabulary --------------------------------------------------------

OK = "ok"
#: Upstream has no data for this (endpoint, season) -- expected, permanent.
ENDPOINT_ABSENT = "endpoint_absent"
TIMEOUT = "timeout"
#: Rate-limited / blocked: the pool's problem, not the data's.
THROTTLED = "throttled"
ERROR = "error"

#: Categories that mean "do not bother trying this again".
PERMANENT = frozenset({ENDPOINT_ABSENT})

_TIMEOUT_HINTS = ("timeout", "timed out", "read timeout", "connecttimeout")
_THROTTLE_HINTS = (
    "429",
    "too many requests",
    "rate limit",
    "forbidden",
    "403",
    "blocked",
)
_ABSENT_HINTS = ("no data", "empty", "not found", "404")


def classify(exc: BaseException | None, payload: object = None) -> str:
    """Bucket one fetch outcome.

    An empty payload is treated as ``endpoint_absent`` rather than an error:
    stats.com answers 200-with-nothing for an endpoint that has no data in a given
    season, which is the single largest source of "misses" in early seasons.
    """
    if exc is None:
        if payload is None or (isinstance(payload, (dict, list)) and not payload):
            return ENDPOINT_ABSENT
        return OK
    text = f"{type(exc).__name__}: {exc}".lower()
    if any(h in text for h in _TIMEOUT_HINTS):
        return TIMEOUT
    if any(h in text for h in _THROTTLE_HINTS):
        return THROTTLED
    if any(h in text for h in _ABSENT_HINTS):
        return ENDPOINT_ABSENT
    return ERROR


class Progress:
    """Heartbeat for a per-game pass."""

    def __init__(
        self,
        total: int,
        label: str,
        log: Callable[[str], None],
        interval_s: float = 60.0,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        self.total = total
        self.label = label
        self._log = log
        self.interval_s = interval_s
        self._now = now
        self._start = now()
        self._last = self._start
        self.done = 0
        self.payloads = 0

    def advance(self, payloads: int = 0) -> None:
        """Record one finished game, emitting a heartbeat when due."""
        self.done += 1
        self.payloads += payloads
        now = self._now()
        if now - self._last < self.interval_s and self.done < self.total:
            return
        self._last = now
        self._log(self.render(now))

    def render(self, now: float | None = None) -> str:
        now = self._now() if now is None else now
        elapsed = max(now - self._start, 1e-9)
        rate = self.done / elapsed
        remaining = self.total - self.done
        eta = remaining / rate if rate > 0 else 0.0
        return (
            f"{self.label}: {self.done}/{self.total} games · {self.payloads} payloads"
            f" · {rate * 60:.0f} games/min · ETA {_human(eta)}"
        )


def _human(seconds: float) -> str:
    seconds = int(max(seconds, 0))
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    return f"{seconds // 3600}h{(seconds % 3600) // 60:02d}m"


class MissLedger:
    """Counts fetch outcomes, overall and per endpoint."""

    def __init__(self) -> None:
        self.totals: Counter[str] = Counter()
        self.by_endpoint: dict[str, Counter[str]] = defaultdict(Counter)

    def record(self, endpoint: str, outcome: str) -> None:
        self.totals[outcome] += 1
        self.by_endpoint[endpoint][outcome] += 1

    @property
    def real_failures(self) -> int:
        """Misses that are actually wrong -- absent endpoints are not."""
        return sum(v for k, v in self.totals.items() if k not in (OK, ENDPOINT_ABSENT))

    def snapshot(self) -> Counter:
        """Copy of the current totals, for diffing one season against the run."""
        return Counter(self.totals)

    def since(self, mark: Counter) -> str:
        """Outcome breakdown accumulated since ``mark`` -- i.e. this season only.

        The ledger spans the whole run, so reporting its totals under a per-season
        heading silently invites mis-attribution: a season's numbers look like the
        run's. Callers mark at season start and report the delta.
        """
        delta = {k: self.totals[k] - mark.get(k, 0) for k in self.totals}
        parts = [f"{k}={v}" for k, v in sorted(delta.items()) if v]
        return " ".join(parts) if parts else "no fetches"

    def real_failures_since(self, mark: Counter) -> int:
        return sum(self.totals[k] - mark.get(k, 0) for k in self.totals if k not in (OK, ENDPOINT_ABSENT))

    def summary(self) -> str:
        if not self.totals:
            return "no fetches"
        parts = [f"{k}={v}" for k, v in sorted(self.totals.items()) if v]
        return " ".join(parts)

    def worst_endpoints(self, limit: int = 3) -> str:
        """Endpoints with the most real failures, for pointing at a culprit."""
        ranked = sorted(
            (
                (sum(v for k, v in c.items() if k not in (OK, ENDPOINT_ABSENT)), ep)
                for ep, c in self.by_endpoint.items()
            ),
            reverse=True,
        )
        hits = [f"{ep}={n}" for n, ep in ranked[:limit] if n]
        return ", ".join(hits) if hits else "none"


class ProxyHealth:
    """Per-proxy outcome counters with quarantine of consistently-failing IPs."""

    def __init__(self, pool_size: int, consecutive_fail_limit: int = 20) -> None:
        self.pool_size = pool_size
        self.limit = consecutive_fail_limit
        self.ok: Counter[str] = Counter()
        self.bad: Counter[str] = Counter()
        self.consecutive: Counter[str] = Counter()
        self.quarantined: set[str] = set()

    def record(self, proxy: str | None, outcome: str) -> None:
        if not proxy:
            return
        # An absent endpoint says nothing about the proxy -- counting it would
        # quarantine healthy IPs during early seasons, which are mostly absences.
        if outcome == ENDPOINT_ABSENT:
            return
        if outcome == OK:
            self.ok[proxy] += 1
            self.consecutive[proxy] = 0
            return
        self.bad[proxy] += 1
        self.consecutive[proxy] += 1
        if self.consecutive[proxy] >= self.limit:
            self.quarantined.add(proxy)

    def healthy(self) -> int:
        return self.pool_size - len(self.quarantined)

    def is_quarantined(self, proxy: str | None) -> bool:
        return bool(proxy) and proxy in self.quarantined

    def summary(self) -> str:
        degraded = sum(1 for p, n in self.bad.items() if n and p not in self.quarantined)
        return (
            f"proxies: {self.healthy()}/{self.pool_size} healthy"
            f" · {degraded} degraded · {len(self.quarantined)} quarantined"
        )


class Degradation:
    """Escalates when the sweep looks like it is fighting the pool, not the data."""

    def __init__(
        self,
        proxies: ProxyHealth,
        ledger: MissLedger,
        log: Callable[[str], None],
        min_healthy_fraction: float = 0.5,
        max_failure_rate: float = 0.25,
        min_sample: int = 50,
    ) -> None:
        self.proxies = proxies
        self.ledger = ledger
        self._log = log
        self.min_healthy_fraction = min_healthy_fraction
        self.max_failure_rate = max_failure_rate
        self.min_sample = min_sample
        self._warned: set[str] = set()

    def failure_rate(self) -> float:
        attempted = sum(v for k, v in self.ledger.totals.items() if k != ENDPOINT_ABSENT)
        return self.ledger.real_failures / attempted if attempted else 0.0

    def check(self) -> None:
        """Warn once per condition, so a bad run is loud but not spammy."""
        if self.proxies.pool_size:
            fraction = self.proxies.healthy() / self.proxies.pool_size
            if fraction < self.min_healthy_fraction and "pool" not in self._warned:
                self._warned.add("pool")
                self._log(
                    f"WARNING: proxy pool degraded -- {self.proxies.healthy()}"
                    f"/{self.proxies.pool_size} healthy. The sweep will keep running but"
                    " is likely burning budget against blocked IPs."
                )
        attempted = sum(v for k, v in self.ledger.totals.items() if k != ENDPOINT_ABSENT)
        if attempted >= self.min_sample:
            rate = self.failure_rate()
            if rate > self.max_failure_rate and "rate" not in self._warned:
                self._warned.add("rate")
                self._log(
                    f"WARNING: {rate:.0%} of attempted fetches are failing"
                    f" ({self.ledger.summary()}); worst: {self.ledger.worst_endpoints()}."
                    " Expected-absent endpoints are excluded, so this is real."
                )
