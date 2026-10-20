"""Behaviour of the NCAA capture loop ``capture_contests`` (B-M36) and the stage entry points (B-L30).

The clean pages are the three real pages of the committed 2015 WBB producer bundle; the page
that did not clear is the real Akamai "Access Denied" answer stats.ncaa.org gives a client that
has not solved the challenge (``tests/fixtures/ncaa/capture/``). No network: the fetcher is a
stub serving those bodies.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List

import pytest

from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.scrape.ncaa import capture, discover, rosters
from sportsdataverse.scrape.ncaa.bundle import bundle_path, read_bundle

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
    assert counts == {"captured": 1, "skipped": 0, "failed": 0}
    written = read_bundle(bundle_path(tmp_path, "wbb", "2015", "385627"))
    assert written["pages"] == CLEAN
    assert written["urls"]["box_score"] == "https://stats.ncaa.org/contests/385627/box_score"
    # a second run skips the captured contest without fetching it
    again = _Script([])
    assert capture.capture_contests(["385627"], 2015, league="wbb", root=tmp_path, fetch_pages_fn=again) == {
        "captured": 0,
        "skipped": 1,
        "failed": 0,
    }
    assert again.fetched == []


def test_a_page_that_did_not_clear_writes_nothing(tmp_path):
    counts = capture.capture_contests(["385627"], 2015, league="wbb", root=tmp_path, fetch_pages_fn=_Script([False]))
    assert counts == {"captured": 0, "skipped": 0, "failed": 1}
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
    assert counts == {"captured": 1, "skipped": 0, "failed": 2}
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
    assert counts == {"captured": 1, "skipped": 0, "failed": 1}
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
        capture, "capture_contests", _record(seen, "capture", {"captured": 2, "skipped": 0, "failed": 0})
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
