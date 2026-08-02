"""Tests for sweep observability. All pure bookkeeping -- no HTTP, no sleeping."""

from __future__ import annotations

from sportsdataverse.scrape.stats.observability import (
    ENDPOINT_ABSENT,
    ERROR,
    OK,
    THROTTLED,
    TIMEOUT,
    Degradation,
    MissLedger,
    Progress,
    ProxyHealth,
    classify,
)


class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


# -- classify ------------------------------------------------------------------


def test_empty_payload_is_an_absent_endpoint_not_an_error() -> None:
    """The largest source of early-season misses: 200 with nothing in it."""
    assert classify(None, {}) == ENDPOINT_ABSENT
    assert classify(None, []) == ENDPOINT_ABSENT
    assert classify(None, None) == ENDPOINT_ABSENT
    assert classify(None, {"resultSets": [1]}) == OK


def test_timeouts_and_throttles_are_distinguished() -> None:
    assert classify(TimeoutError("read timeout")) == TIMEOUT
    assert classify(RuntimeError("HTTP 429 Too Many Requests")) == THROTTLED
    assert classify(RuntimeError("403 Forbidden")) == THROTTLED
    assert classify(ValueError("something else entirely")) == ERROR


def test_absent_hints_in_an_exception_still_read_as_absent() -> None:
    assert classify(RuntimeError("404 not found")) == ENDPOINT_ABSENT


# -- progress ------------------------------------------------------------------


def test_heartbeat_fires_on_interval_not_every_game() -> None:
    lines: list[str] = []
    clock = Clock()
    p = Progress(100, "1997", lines.append, interval_s=60, now=clock)
    for _ in range(10):
        clock.t += 1
        p.advance(3)
    assert lines == [], "should not log before the interval elapses"
    clock.t += 60
    p.advance(3)
    assert len(lines) == 1
    assert "11/100 games" in lines[0] and "payloads" in lines[0]


def test_final_game_always_reports() -> None:
    lines: list[str] = []
    clock = Clock()
    p = Progress(2, "x", lines.append, interval_s=9999, now=clock)
    p.advance()
    p.advance()
    assert len(lines) == 1 and "2/2" in lines[0]


def test_render_includes_rate_and_eta() -> None:
    clock = Clock()
    p = Progress(120, "s", lambda _m: None, interval_s=9999, now=clock)
    for _ in range(60):
        clock.t += 1
        p.done += 1
    out = p.render()
    assert "games/min" in out and "ETA" in out


# -- ledger --------------------------------------------------------------------


def test_absent_endpoints_are_not_real_failures() -> None:
    led = MissLedger()
    for _ in range(9):
        led.record("boxscoreadvancedv3", ENDPOINT_ABSENT)
    led.record("playbyplayv3", TIMEOUT)
    assert led.real_failures == 1
    assert "endpoint_absent=9" in led.summary()


def test_worst_endpoints_points_at_the_culprit() -> None:
    led = MissLedger()
    for _ in range(5):
        led.record("gamerotation", ERROR)
    led.record("playbyplayv3", ERROR)
    led.record("boxscoremiscv3", ENDPOINT_ABSENT)
    worst = led.worst_endpoints()
    assert worst.startswith("gamerotation=5")
    assert "boxscoremiscv3" not in worst, "absent endpoints are not culprits"


# -- proxy health --------------------------------------------------------------


def test_absent_endpoints_never_count_against_a_proxy() -> None:
    """Early seasons are mostly absences; counting them would quarantine the pool."""
    ph = ProxyHealth(pool_size=5, consecutive_fail_limit=3)
    for _ in range(50):
        ph.record("p1", ENDPOINT_ABSENT)
    assert ph.healthy() == 5 and not ph.quarantined


def test_consecutive_failures_quarantine_and_success_resets() -> None:
    ph = ProxyHealth(pool_size=5, consecutive_fail_limit=3)
    for _ in range(2):
        ph.record("p1", TIMEOUT)
    ph.record("p1", OK)  # resets the streak
    for _ in range(2):
        ph.record("p1", TIMEOUT)
    assert not ph.is_quarantined("p1")
    ph.record("p1", TIMEOUT)
    assert ph.is_quarantined("p1") and ph.healthy() == 4


def test_summary_counts_degraded_separately_from_quarantined() -> None:
    ph = ProxyHealth(pool_size=3, consecutive_fail_limit=2)
    ph.record("p1", TIMEOUT)  # degraded, not out
    ph.record("p2", TIMEOUT)
    ph.record("p2", TIMEOUT)  # quarantined
    s = ph.summary()
    assert "2/3 healthy" in s and "1 degraded" in s and "1 quarantined" in s


# -- degradation ---------------------------------------------------------------


def test_warns_once_when_the_pool_collapses() -> None:
    lines: list[str] = []
    ph = ProxyHealth(pool_size=4, consecutive_fail_limit=1)
    deg = Degradation(ph, MissLedger(), lines.append)
    for p in ("a", "b", "c"):
        ph.record(p, TIMEOUT)
    deg.check()
    deg.check()
    assert len(lines) == 1 and "pool degraded" in lines[0]


def test_failure_rate_excludes_absent_endpoints() -> None:
    """Otherwise every early season would trip the alarm."""
    led = MissLedger()
    for _ in range(500):
        led.record("boxscoreusagev3", ENDPOINT_ABSENT)
    for _ in range(60):
        led.record("playbyplayv3", OK)
    deg = Degradation(ProxyHealth(10), led, lambda _m: None)
    assert deg.failure_rate() == 0.0


def test_warns_when_real_failures_spike() -> None:
    lines: list[str] = []
    led = MissLedger()
    for _ in range(40):
        led.record("playbyplayv3", OK)
    for _ in range(30):
        led.record("playbyplayv3", TIMEOUT)
    deg = Degradation(ProxyHealth(10), led, lines.append)
    deg.check()
    assert len(lines) == 1 and "failing" in lines[0]


def test_no_warning_below_the_sample_floor() -> None:
    lines: list[str] = []
    led = MissLedger()
    led.record("x", ERROR)
    Degradation(ProxyHealth(10), led, lines.append).check()
    assert lines == [], "one bad call must not raise an alarm"


def test_since_reports_one_season_not_the_whole_run() -> None:
    """The ledger spans the run; a per-season line must not print run totals."""
    led = MissLedger()
    for _ in range(81):
        led.record("gamerotation", TIMEOUT)
    mark = led.snapshot()
    for _ in range(209):
        led.record("gamerotation", TIMEOUT)
    assert "timeout=209" in led.since(mark), "must report the season, not 290"
    assert led.real_failures_since(mark) == 209
    assert led.real_failures == 290, "the run total is still available"


def test_since_on_an_unchanged_ledger_is_empty() -> None:
    led = MissLedger()
    led.record("x", OK)
    assert led.since(led.snapshot()) == "no fetches"
