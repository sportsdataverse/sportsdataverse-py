"""Behaviour of the NCAA capture loop ``capture_contests`` (B-M36) and the stage entry points (B-L30).

The clean pages are the three real pages of the committed 2015 WBB producer bundle; the page
that did not clear is the real Akamai "Access Denied" answer stats.ncaa.org gives a client that
has not solved the challenge (``tests/fixtures/ncaa/capture/``). No network: the fetcher is a
stub serving those bodies.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

import pytest

from sportsdataverse.errors import AssetFetchError, NoDataError, NotAvailableYetError
from sportsdataverse.scrape.ncaa import capture, discover, rosters
from sportsdataverse.scrape.ncaa.bundle import bundle_path, read_bundle, write_bundle

FIX = Path(__file__).resolve().parents[2] / "fixtures" / "ncaa"
CLEAN: Dict[str, str] = read_bundle(FIX / "wbb_raw" / "wbb" / "raw" / "2015" / "385627.json.gz")["pages"]
DENIED = (FIX / "capture" / "access_denied_box_score_385627.html").read_text(encoding="utf-8")


def _pages(clean: bool) -> Dict[str, str]:
    return dict(CLEAN) if clean else {**CLEAN, "box_score": DENIED}


class _Script:
    """A fetch_pages_fn answering contest i with plan[i]: True (clean), False (denied) or an exception."""

    def __init__(self, plan: List[object]):
        self.plan = plan
        self.fetched: List[str] = []

    def __call__(self, fetcher, contest_id: str) -> Dict[str, str]:
        step = self.plan[len(self.fetched)]
        self.fetched.append(contest_id)
        if isinstance(step, Exception):
            raise step
        return _pages(bool(step))


def _written(root: Path) -> List[str]:
    return sorted(p.name for p in root.rglob("*.json.gz"))


def test_clean_pages_write_the_bundle(tmp_path):
    fetch = _Script([True])
    counts = capture.capture_contests(["385627"], 2015, league="wbb", root=tmp_path, fetch_pages_fn=fetch)
    assert counts == {"captured": 1, "skipped": 0, "failed": 0, "not_ready": 0}
    written = read_bundle(bundle_path(tmp_path, "wbb", "2015", "385627"))
    assert written["pages"] == CLEAN
    assert written["urls"]["box_score"] == "https://stats.ncaa.org/contests/385627/box_score"
    # a second run skips the captured contest without fetching it
    again = _Script([])
    assert capture.capture_contests(["385627"], 2015, league="wbb", root=tmp_path, fetch_pages_fn=again) == {
        "captured": 0,
        "skipped": 1,
        "failed": 0,
        "not_ready": 0,
    }
    assert again.fetched == []


def test_a_page_that_did_not_clear_writes_nothing(tmp_path):
    counts = capture.capture_contests(["385627"], 2015, league="wbb", root=tmp_path, fetch_pages_fn=_Script([False]))
    assert counts == {"captured": 0, "skipped": 0, "failed": 1, "not_ready": 0}
    assert _written(tmp_path) == []


def test_soft_ban_stops_after_consecutive_failures(tmp_path):
    fetch = _Script([False] * 5)
    with pytest.raises(SystemExit, match="SOFT-BAN: capture halted after 3 consecutive challenge failures"):
        capture.capture_contests(
            ["1", "2", "3", "4", "5"],
            2015,
            league="wbb",
            root=tmp_path,
            fetch_pages_fn=fetch,
            max_consecutive_failures=3,
        )
    assert fetch.fetched == ["1", "2", "3"]  # stops at the third, never digs the ban deeper
    assert _written(tmp_path) == []


def test_a_clean_page_resets_the_soft_ban_counter(tmp_path):
    fetch = _Script([False, True, False])
    counts = capture.capture_contests(
        ["1", "2", "3"], 2015, league="wbb", root=tmp_path, fetch_pages_fn=fetch, max_consecutive_failures=2
    )
    assert counts == {"captured": 1, "skipped": 0, "failed": 2, "not_ready": 0}
    assert _written(tmp_path) == ["2.json.gz"]


def test_ban_suspect_is_a_hard_stop(tmp_path):
    fetch = _Script([True, AssetFetchError("proxy pool exhausted"), True])
    with pytest.raises(SystemExit, match="BAN-SUSPECT: capture halted at contest_id=2"):
        capture.capture_contests(["1", "2", "3"], 2015, league="wbb", root=tmp_path, fetch_pages_fn=fetch)
    assert fetch.fetched == ["1", "2"]
    assert _written(tmp_path) == ["1.json.gz"]


def test_vocabulary_a_404_contest_is_counted_failed_and_the_run_goes_on(tmp_path):
    fetch = _Script([NoDataError("stats.ncaa.org has no page (HTTP 404)"), True])
    counts = capture.capture_contests(["1", "2"], 2015, league="wbb", root=tmp_path, fetch_pages_fn=fetch)
    assert counts == {"captured": 1, "skipped": 0, "failed": 1, "not_ready": 0}
    assert _written(tmp_path) == ["2.json.gz"]


def test_vocabulary_a_bug_is_not_reported_as_a_ban(tmp_path):
    fetch = _Script([RuntimeError("a parser bug")])
    with pytest.raises(RuntimeError, match="a parser bug"):
        capture.capture_contests(["1"], 2015, league="wbb", root=tmp_path, fetch_pages_fn=fetch)


@pytest.mark.parametrize(
    ("error", "calls"),
    [(AssetFetchError("pool exhausted"), 3), (NoDataError("HTTP 404"), 1)],
    ids=["failed-fetch-retried", "404-not-retried"],
)
def test_vocabulary_team_sweeps_skip_a_failed_team(tmp_path, error, calls):
    seen: List[str] = []

    def fetch(arg):
        seen.append(str(arg))
        raise error

    df = discover.discover_season(2015, league="wbb", fetch_fn=fetch, team_ids=[7], root=None)
    assert df.height == 0 and seen == ["7"] * calls
    seen.clear()
    assert rosters.capture_rosters(2015, league="wbb", root=tmp_path, fetch_fn=fetch, team_ids=[7]) == (0, 0, 1)
    assert seen == ["teams/7/roster"] * calls


@pytest.mark.parametrize(
    ("error", "aborts"),
    [(NoDataError("HTTP 404"), False), (AssetFetchError("pool exhausted"), True)],
    ids=["404s-are-not-ban-suspect", "failed-fetches-are"],
)
def test_vocabulary_404_teams_never_trip_the_ban_suspect_abort(tmp_path, error, aborts):
    """R4.5-1: six 404s in a row are six missing pages, not a ban (the capture stage's rule)."""

    def fetch(arg):
        raise error

    teams = list(range(1, 7))  # one more than _MAX_CONSECUTIVE_TEAM_FAILURES
    if aborts:
        with pytest.raises(RuntimeError, match="consecutive teams failed"):
            discover.discover_season(2015, league="wbb", fetch_fn=fetch, team_ids=teams, root=None)
        with pytest.raises(RuntimeError, match="consecutive team failures"):
            rosters.capture_rosters(2015, league="wbb", root=tmp_path, fetch_fn=fetch, team_ids=teams)
    else:
        assert discover.discover_season(2015, league="wbb", fetch_fn=fetch, team_ids=teams, root=None).height == 0
        assert rosters.capture_rosters(2015, league="wbb", root=tmp_path, fetch_fn=fetch, team_ids=teams) == (0, 0, 6)


def test_vocabulary_a_404_team_does_not_reset_the_ban_suspect_count(tmp_path):
    """R4.5-3: four failed teams, a 404, then one more failure is still five failures in a row."""

    def fetch(arg):
        team = int(str(arg).split("/")[1]) if "/" in str(arg) else int(arg)
        if team == 5:
            raise NoDataError("HTTP 404")
        raise AssetFetchError("pool exhausted")

    teams = [1, 2, 3, 4, 5, 6]
    with pytest.raises(RuntimeError, match="5 consecutive teams failed"):
        discover.discover_season(2015, league="wbb", fetch_fn=fetch, team_ids=teams, root=None)
    with pytest.raises(RuntimeError, match="5 consecutive team failures"):
        rosters.capture_rosters(2015, league="wbb", root=tmp_path, fetch_fn=fetch, team_ids=teams)


def test_vocabulary_team_sweeps_do_not_swallow_a_bug(tmp_path):
    def fetch(arg):
        raise RuntimeError("a parser bug")

    with pytest.raises(RuntimeError, match="a parser bug"):
        discover.discover_season(2015, league="wbb", fetch_fn=fetch, team_ids=[7], root=None)
    with pytest.raises(RuntimeError, match="a parser bug"):
        rosters.capture_rosters(2015, league="wbb", root=tmp_path, fetch_fn=fetch, team_ids=[7])


def test_shards_are_disjoint_and_cover_every_contest():
    ids = [str(i) for i in range(10, 0, -1)]
    parts = [capture.shard(ids, i, 3) for i in range(3)]
    assert parts == [["1", "3", "6", "9"], ["10", "4", "7"], ["2", "5", "8"]]
    assert sorted(sum(parts, [])) == sorted(ids)


def test_shard_rotation_starts_each_worker_on_its_own_sticky_proxy(tmp_path, monkeypatch):
    from sportsdataverse.scrape.ncaa import canary

    proxies = [f"http://user-session-old{i}:pw@gw.proxy.invalid:{7000 + i}" for i in range(4)]
    (tmp_path / "canary_vendors.toml").write_text(
        '[[vendor]]\nname = "sticky"\ntype = "proxy_patchright"\nproxies = ['
        + ", ".join(f'"{p}"' for p in proxies)
        + "]\n",
        encoding="utf-8",
    )
    built: List[List[str]] = []
    monkeypatch.setattr(canary, "build_fetcher", lambda vendor, cache_dir: built.append(vendor["proxies"]) or object())
    for i in range(2):
        capture._vendor_fetcher("sticky", tmp_path, shard_i=i, shard_n=2)
    first_ports = [p[0].rsplit(":", 1)[1] for p in built]
    assert first_ports == ["7000", "7002"]  # disjoint starting offsets, so no two workers share an IP
    for rotated in built:
        assert sorted(p.rsplit(":", 1)[1] for p in rotated) == ["7000", "7001", "7002", "7003"]
        assert all("-session-old" not in p and "-session-cap" in p for p in rotated)  # re-minted per run


# --------------------------------------------------------------------------------------
# B-L30: each stage's `_main(league, root)` binds the producer's league and root
# --------------------------------------------------------------------------------------


def _record(store: Dict[str, object], name: str, result: object = None):
    def fn(*args, **kwargs):
        store[name] = (args, kwargs)
        return result

    return fn


def test_capture_main_binds_league_and_root(tmp_path, monkeypatch, capsys):
    seen: Dict[str, object] = {}
    monkeypatch.setattr(capture, "_select_pending", lambda master, season: seen.update(master=master) or ["9", "8"])
    monkeypatch.setattr(
        capture,
        "capture_contests",
        _record(seen, "capture", {"captured": 2, "skipped": 0, "failed": 0, "not_ready": 0}),
    )
    monkeypatch.setattr(sys, "argv", ["capture", "--season", "2015"])
    capture._main("wbb", str(tmp_path))
    assert seen["master"] == tmp_path / "wbb" / "schedule_master.parquet"
    args, kwargs = seen["capture"]
    assert args == (["8", "9"], 2015) and kwargs["league"] == "wbb" and kwargs["root"] == str(tmp_path)
    assert "captured=2 skipped=0 failed=0" in capsys.readouterr().out


def test_discover_main_binds_league_and_root(tmp_path, monkeypatch):
    import polars as pl

    from sportsdataverse.scrape.ncaa import discover

    seen: Dict[str, object] = {}
    monkeypatch.setattr(discover, "discover_season", _record(seen, "discover", pl.DataFrame({"contest_id": ["1"]})))
    monkeypatch.setattr(sys, "argv", ["discover", "--season", "2015"])
    discover._main("wbb", str(tmp_path))
    args, kwargs = seen["discover"]
    assert args == (2015,) and kwargs["league"] == "wbb" and kwargs["root"] == str(tmp_path)
    assert kwargs["write_master"] is True


def test_discover_main_scoreboard_needs_no_season_and_refuses_shards(tmp_path, monkeypatch, capsys):
    import polars as pl

    from sportsdataverse.scrape.ncaa import discover

    seen: Dict[str, object] = {}
    found = pl.DataFrame({"contest_id": ["1", "2"], "season": ["2027", "2027"]})
    monkeypatch.setattr(discover, "discover_dates", _record(seen, "dates", found))
    monkeypatch.setenv("NCAA_SCOREBOARD_LOOKBACK_DAYS", "x")  # a bad value falls back to the default
    monkeypatch.setattr(sys, "argv", ["discover", "--scoreboard", "--dates", "2026-11-02"])
    discover._main("mbb", str(tmp_path))
    args, kwargs = seen["dates"]
    assert args == (["2026-11-02"],) and kwargs["lookback_days"] == 3
    assert "{'2027': 2}" in capsys.readouterr().out

    monkeypatch.setattr(sys, "argv", ["discover", "--scoreboard", "--shard", "0/4"])
    with pytest.raises(SystemExit):
        discover._main("mbb", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["discover"])  # the team sweep still needs its season
    with pytest.raises(SystemExit):
        discover._main("mbb", str(tmp_path))


def test_rosters_main_binds_league_and_root(tmp_path, monkeypatch):
    from sportsdataverse.scrape.ncaa import rosters

    seen: Dict[str, object] = {}
    monkeypatch.setattr(rosters, "capture_rosters", _record(seen, "rosters", (1, 0, 0)))
    monkeypatch.setattr(sys, "argv", ["rosters", "--season", "2015"])
    rosters._main("wbb", str(tmp_path))
    args, kwargs = seen["rosters"]
    assert args == (2015,) and kwargs["league"] == "wbb" and kwargs["root"] == tmp_path


def test_datasets_main_binds_league_and_root(tmp_path, monkeypatch):
    import polars as pl

    from sportsdataverse.scrape.ncaa import datasets

    seen: Dict[str, object] = {}
    monkeypatch.setattr(datasets, "rebuild_missing", _record(seen, "rebuild"))
    monkeypatch.setattr(datasets, "build_season_aggregate", _record(seen, "aggregate", pl.DataFrame({"x": [1]})))
    monkeypatch.setattr(datasets, "build_teams", _record(seen, "teams", pl.DataFrame({"x": [1, 2]})))
    monkeypatch.setattr(sys, "argv", ["datasets", "--season", "2015"])
    datasets._main("wbb", str(tmp_path))
    for name in ("rebuild", "aggregate", "teams"):
        _, kwargs = seen[name]
        assert kwargs["league"] == "wbb" and kwargs["root"] == str(tmp_path), name


def test_xwalk_main_binds_league_and_root(tmp_path, monkeypatch):
    from sportsdataverse.scrape.ncaa import espn_game_xwalk as xwalk

    seen: Dict[str, object] = {}
    monkeypatch.setattr(xwalk, "build_season_xwalk", _record(seen, "build", "frame"))
    monkeypatch.setattr(
        xwalk, "summarize", lambda frame: {"contests": 0, "unmatched": 0, **{m: 0 for m in xwalk.MATCH_METHODS}}
    )
    monkeypatch.setattr(sys, "argv", ["xwalk", "--seasons", "2015"])
    xwalk._main("wbb", str(tmp_path))
    assert seen["build"] == ((str(tmp_path), "wbb", 2015), {})


# --- not-ready contests, optional tabs, officials refresh (2026-11-03) ----------------------------

_BB = Path(__file__).resolve().parents[2] / "fixtures" / "mbb_ncaa"
OFFICIALS = (_BB / "bkb_officials_5722355.html").read_text(encoding="utf-8")
OFFICIALS_BLANK = (_BB / "bkb_officials_blank_6635843.html").read_text(encoding="utf-8")
TEAM_STATS = (_BB / "bkb_team_stats_5722355.html").read_text(encoding="utf-8")


def test_not_ready_contest_is_counted_apart_and_never_trips_the_soft_ban_guard(tmp_path):
    """A scheduled-but-unplayed contest answers "Box score not available": skip it, ask again next run."""
    plan = [NotAvailableYetError("not available")] * 3 + [True]
    counts = capture.capture_contests(
        ["1", "2", "3", "4"],
        2015,
        league="wbb",
        root=tmp_path,
        fetch_pages_fn=_Script(plan),
        max_consecutive_failures=2,
    )
    assert counts == {"captured": 1, "skipped": 0, "failed": 0, "not_ready": 3}
    assert _written(tmp_path) == ["4.json.gz"]


class _TabFetcher:
    """Serves the three core pages plus whatever optional tabs it is given; records every path."""

    def __init__(self, tabs: Dict[str, object]):
        self.tabs = tabs
        self.paths: List[str] = []

    def fetch_game_pbp(self, cid, force=False):
        return CLEAN["play_by_play"]

    def fetch_game_box(self, cid, force=False):
        return CLEAN["box_score"]

    def fetch_game_individual_stats(self, cid, force=False):
        return CLEAN["individual_stats"]

    def fetch_html(self, path, force=False):
        self.paths.append(path)
        body = self.tabs[path.rsplit("/", 1)[1]]
        if isinstance(body, Exception):
            raise body
        return body


def test_optional_tabs_are_captured_with_the_bundle(tmp_path):
    f = _TabFetcher({"team_stats": TEAM_STATS, "officials": OFFICIALS})
    capture.capture_contests(["385627"], 2015, league="wbb", root=tmp_path, fetcher=f)
    b = read_bundle(bundle_path(tmp_path, "wbb", "2015", "385627"))
    assert set(b["pages"]) == {"play_by_play", "box_score", "individual_stats", "team_stats", "officials"}
    assert b["urls"]["officials"] == "https://stats.ncaa.org/contests/385627/officials"
    assert f.paths == ["contests/385627/team_stats", "contests/385627/officials"]


def test_a_missing_optional_tab_does_not_block_the_bundle(tmp_path):
    f = _TabFetcher({"team_stats": TEAM_STATS, "officials": NoDataError("404")})
    counts = capture.capture_contests(["385627"], 2015, league="wbb", root=tmp_path, fetcher=f)
    assert counts["captured"] == 1
    b = read_bundle(bundle_path(tmp_path, "wbb", "2015", "385627"))
    assert "officials" not in b["pages"] and "officials" not in b["urls"]


def _bundle_with_officials(root: Path, cid: str, officials: str) -> Path:
    return write_bundle(root, "mbb", "2027", cid, pages={**CLEAN, "officials": officials},
                        urls={"officials": "u"}, captured_at="2026-11-03T08:00:00+00:00")  # fmt: skip


def test_refresh_officials_fills_a_blank_crew(tmp_path):
    _bundle_with_officials(tmp_path, "6635843", OFFICIALS_BLANK)
    _bundle_with_officials(tmp_path, "5722355", OFFICIALS)  # already has its crew: never re-fetched
    f = _TabFetcher({"officials": OFFICIALS})
    counts = capture.refresh_officials(2027, league="mbb", root=tmp_path, fetcher_factory=lambda: f)
    assert counts == {"checked": 2, "refreshed": 1, "still_blank": 0}
    assert f.paths == ["contests/6635843/officials"]
    b = read_bundle(bundle_path(tmp_path, "mbb", "2027", "6635843"))
    assert b["pages"]["officials"] == OFFICIALS
    assert b["captured_at"] == "2026-11-03T08:00:00+00:00"  # the capture time is the original one


def test_refresh_officials_leaves_a_still_blank_crew_and_builds_no_fetcher_when_nothing_qualifies(tmp_path):
    path = _bundle_with_officials(tmp_path, "6635843", OFFICIALS_BLANK)
    f = _TabFetcher({"officials": OFFICIALS_BLANK})
    assert capture.refresh_officials(2027, league="mbb", root=tmp_path, fetcher_factory=lambda: f) == {
        "checked": 1, "refreshed": 0, "still_blank": 1}  # fmt: skip
    # past the window the bundle is not even opened, and no browser is launched
    old = (datetime.now(timezone.utc) - timedelta(days=30)).timestamp()
    os.utime(path, (old, old))

    def _never():
        raise AssertionError("fetcher built with nothing to refresh")

    assert capture.refresh_officials(2027, league="mbb", root=tmp_path, fetcher_factory=_never) == {
        "checked": 0, "refreshed": 0, "still_blank": 0}  # fmt: skip


def test_team_stats_not_posted_yet_leaves_the_contest_for_the_next_run(tmp_path):
    """A bundled contest is never fetched again, so a team_stats tab not posted yet must not be bundled away."""
    f = _TabFetcher({"team_stats": NotAvailableYetError("Team stats not available"), "officials": OFFICIALS})
    counts = capture.capture_contests(["385627"], 2015, league="wbb", root=tmp_path, fetcher=f)
    assert counts == {"captured": 0, "skipped": 0, "failed": 0, "not_ready": 1}
    assert _written(tmp_path) == []


def test_officials_not_posted_yet_is_bundled_without_them(tmp_path):
    """The crew posts days later and refresh_officials re-asks, so the bundle need not wait for it."""
    f = _TabFetcher({"team_stats": TEAM_STATS, "officials": NotAvailableYetError("Officials not available")})
    counts = capture.capture_contests(["385627"], 2015, league="wbb", root=tmp_path, fetcher=f)
    assert counts["captured"] == 1
    assert "officials" not in read_bundle(bundle_path(tmp_path, "wbb", "2015", "385627"))["pages"]


def test_an_optional_tab_that_is_not_a_contest_page_writes_nothing(tmp_path):
    f = _TabFetcher({"team_stats": "<html><body>Service unavailable</body></html>", "officials": OFFICIALS})
    counts = capture.capture_contests(["385627"], 2015, league="wbb", root=tmp_path, fetcher=f)
    assert counts["failed"] == 1 and counts["captured"] == 0
    assert _written(tmp_path) == []


def test_capture_main_refresh_days_env_falls_back_on_a_bad_value(tmp_path, monkeypatch):
    seen: Dict[str, object] = {}
    monkeypatch.setattr(capture, "_select_pending", lambda master, season: [])
    monkeypatch.setattr(
        capture, "capture_contests", lambda *a, **k: {"captured": 0, "skipped": 0, "failed": 0, "not_ready": 0}
    )

    def _refresh(season, **kwargs):
        seen.update(kwargs)
        return {"checked": 0, "refreshed": 0, "still_blank": 0}

    monkeypatch.setattr(capture, "refresh_officials", _refresh)
    monkeypatch.setattr(capture, "_vendor_fetcher", lambda vendor, root, **kw: ("vendor", vendor))
    monkeypatch.setenv("NCAA_REFRESH_OFFICIALS_DAYS", "two weeks")
    monkeypatch.setattr(sys, "argv", ["capture", "--season", "2027", "--vendor", "decodo"])
    capture._main("mbb", str(tmp_path))
    assert seen["days"] == 14
    assert seen["fetcher_factory"]() == ("vendor", "decodo")
