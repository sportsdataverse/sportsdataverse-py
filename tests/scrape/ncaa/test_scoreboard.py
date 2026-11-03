"""Offline tests for sport-neutral scoreboard discovery (sportsdataverse.scrape.ncaa.scoreboard)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.scrape.ncaa import scoreboard as SB
from sportsdataverse.scrape.ncaa import seasons as S

FIX = Path(__file__).resolve().parents[2] / "fixtures"


def _page(name: str) -> str:
    return (FIX / "ncaa_scoreboard" / name).read_text(encoding="utf-8")


@pytest.mark.parametrize(
    ("name", "d", "n", "first"),
    [
        ("mbb_d1_2026-11-02.html", "2026-11-02", 184, "6624354"),
        ("mfb_d3_2026-10-31.html", "10/31/2026", 117, None),
        ("mba_d1_2026-05-16.html", date(2026, 5, 16), 138, None),
    ],
)
def test_parse_every_sport_the_same_way(name, d, n, first):
    df = SB.parse_ncaa_scoreboard_contests(_page(name), d, sport_code="X", season_division_id=1)
    assert df.schema == pl.Schema(SB.SCOREBOARD_CONTESTS_SCHEMA)
    assert df.height == n == df["contest_id"].n_unique()
    assert df["contest_id"].str.contains(r"^\d+$").all()
    if first:
        assert df["contest_id"][0] == first


def test_page_showing_another_date_yields_nothing():
    """stats.ncaa.org answers an out-of-season date with another day's board: never file it under the asked date."""
    df = SB.parse_ncaa_scoreboard_contests(_page("mbb_d1_2026-11-02.html"), "2026-11-03")
    assert df.height == 0
    assert df.schema == pl.Schema(SB.SCOREBOARD_CONTESTS_SCHEMA)


def test_empty_page_is_zero_rows():
    assert SB.parse_ncaa_scoreboard_contests("", "2026-11-02").height == 0


def test_bad_date_raises():
    with pytest.raises(ValueError, match="unrecognised date"):
        SB.parse_ncaa_scoreboard_contests("", "Nov 2")


@pytest.fixture
def _mbb_table(tmp_path, monkeypatch):
    monkeypatch.setenv("SDV_PY_CACHE_DIR", str(tmp_path))
    rows = S._parse_scoreboard_page((FIX / "ncaa_season_divisions" / "mbb_2026_27_d1.html").read_text(), "MBB")
    monkeypatch.setattr(S, "_bundled", lambda: S._with_derived(pl.DataFrame(rows, schema=S._CSV_SCHEMA)))


def test_contests_resolves_the_season_and_fetches_one_page_per_date(_mbb_table):
    calls: "list[str]" = []

    def _fetch(path: str) -> str:
        calls.append(path)
        return _page("mbb_d1_2026-11-02.html")

    df = SB.ncaa_scoreboard_contests("mbb", "2026-11-02", fetch_html=_fetch)
    assert calls == [
        "season_divisions/18983/scoreboards?utf8=%E2%9C%93&season_division_id=&game_date=11%2F02%2F2026"
        "&conference_id=0&tournament_id=&commit=Submit"
    ]
    assert df.height == 184
    assert df["sport_code"].unique().to_list() == ["MBB"]
    assert df["season_division_id"].unique().to_list() == [18983]


def test_lookback_adds_earlier_dates_and_dedups(_mbb_table):
    seen: "list[str]" = []

    def _fetch(path: str) -> str:
        seen.append(path.split("game_date=")[1][:14])
        return _page("mbb_d1_2026-11-02.html")

    df = SB.ncaa_scoreboard_contests("mbb", ["2026-11-02"], lookback_days=2, fetch_html=_fetch)
    assert seen == ["10%2F31%2F2026", "11%2F01%2F2026", "11%2F02%2F2026"]
    # the fake answers every date with the 11/02 board: only 11/02 keeps rows (the shown-date guard)
    assert df["game_date"].unique().to_list() == [date(2026, 11, 2)]
    assert df.height == 184


def test_division_label_is_passed_through(_mbb_table):
    paths: "list[str]" = []
    SB.ncaa_scoreboard_contests("mbb", "2026-11-02", division="D-II", fetch_html=lambda p: paths.append(p) or "")
    assert paths[0].startswith("season_divisions/18984/")
