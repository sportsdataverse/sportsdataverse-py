"""Shot coordinates in the hoopR / wehoop convention on the committed ESPN captures (task 2.16 PR2: C-M20).

The processors emit ``coordinate.{x,y}`` transformed the way the published hoopR / wehoop
files are (free throws pinned to (25, 13.75), ``x = y - 41.75`` and ``y = x - 25``, both
negated on home plays) with ESPN's values beside them as ``coordinate.{x,y}.raw``. The
oracles are R's ``case_when`` written row by row from the raw capture, independent of the
polars code under test, and the published release rows of the captured games
(``basketball_pbp/published_coordinates.parquet``). One deliberate divergence (owner ruling
R844-3): a free throw is ``(?i)free ?throw``, so MBB / WBB's "MadeFreeThrow" rows, which R's
case-sensitive "Free Throw" never pinned, sit on the line too.
"""

from __future__ import annotations

import re

import polars as pl
import pytest

from sportsdataverse.dl_utils import underscore
from sportsdataverse.mbb.mbb_play_by_play import helper_mbb_play_by_play
from sportsdataverse.nba.nba_game_page import COORD_SENTINEL, shot_coordinates
from sportsdataverse.nba.nba_play_by_play import helper_nba_play_by_play
from sportsdataverse.wbb.wbb_play_by_play import helper_wbb_play_by_play
from sportsdataverse.wnba.wnba_play_by_play import helper_wnba_play_by_play
from tests.validation.test_basketball_clock_seconds import CAPTURES, FIX, _process, _summary

HALVES = "basketball_pbp/wbb_400509734.json.gz"  # 2013-14: ESPN ships no coordinates
COORD_CAPTURES = [(lg, n) for lg, n in CAPTURES if n != HALVES]
CONVERTERS = {
    "mbb": helper_mbb_play_by_play,
    "nba": helper_nba_play_by_play,
    "wbb": helper_wbb_play_by_play,
    "wnba": helper_wnba_play_by_play,
}


def _hoopr(summary: dict) -> list[tuple[float, float, float, float]]:
    """(x, y, x_raw, y_raw) per play: hoopR / wehoop ``helper_espn_<lg>_pbp``, the R844-3 match on the transformed pair."""
    competitors = summary["header"]["competitions"][0]["competitors"]
    home = next(int(c["team"]["id"]) for c in competitors if c["homeAway"] == "home")
    out = []
    for p in summary["plays"]:
        x, y = float(p["coordinate"]["x"]), float(p["coordinate"]["y"])
        text = (p.get("type") or {}).get("text") or ""
        # R pins the raw pair on its case-sensitive match; R844-3 moves only the transformed pair
        x_raw, y_raw = (25.0, 13.75) if "Free Throw" in text else (x, y)
        if re.search(r"(?i)free ?throw", text):
            x, y = 25.0, 13.75
        team = (p.get("team") or {}).get("id")
        sign = -1 if team is not None and int(team) == home else 1
        out.append((sign * (y - 41.75), sign * (x - 25), x_raw, y_raw))
    return out


def _snake(plays: pl.DataFrame) -> pl.DataFrame:
    return plays.rename({c: underscore(c.replace(".", "_")) for c in plays.columns})


@pytest.mark.parametrize(("lg", "name"), COORD_CAPTURES)
def test_coordinates_follow_the_hoopr_convention(monkeypatch, lg: str, name: str) -> None:
    summary = _summary(name)
    plays, _ = _process(monkeypatch, lg, summary)
    got = plays.select("coordinate.x", "coordinate.y", "coordinate.x.raw", "coordinate.y.raw").rows()
    assert got == _hoopr(summary)
    assert plays.schema["coordinate.x"] == plays.schema["coordinate.x.raw"] == pl.Float64


@pytest.mark.parametrize(("lg", "name"), [("nba", "summary_nba.json"), ("mbb", "summary_mbb.json")])
def test_free_throws_pinned_and_home_plays_flipped(monkeypatch, lg: str, name: str) -> None:
    summary = _summary(name)
    plays, _ = _process(monkeypatch, lg, summary)
    ft = plays.filter(pl.col("type.text").str.contains(r"(?i)free ?throw"))
    assert ft.height > 0
    # the raw pair is pinned only where R's case-sensitive "Free Throw" matches: every NBA free
    # throw; MBB's "MadeFreeThrow" keeps ESPN's rim value there (R844-3 moves the transformed pair)
    r_ft = ft.filter(pl.col("type.text").str.contains("Free Throw", literal=True))
    assert set(r_ft.select("coordinate.x.raw", "coordinate.y.raw").rows()) <= {(25.0, 13.75)}
    assert (r_ft.height == ft.height) == (lg == "nba")
    if lg == "mbb":
        assert set(ft.select("coordinate.x.raw", "coordinate.y.raw").rows()) == {(25.0, 0.0)}
    # a free throw sits 28 feet from half court on its own team's end: -28 at home, +28 away
    home = ft.filter(pl.col("team.id").cast(pl.Int64) == pl.col("homeTeamId"))
    away = ft.filter(pl.col("team.id").cast(pl.Int64) != pl.col("homeTeamId"))
    assert home.height and away.height
    assert set(home.select("coordinate.x", "coordinate.y").rows()) == {(28.0, 0.0)}
    assert set(away.select("coordinate.x", "coordinate.y").rows()) == {(-28.0, 0.0)}


# R844-3 partition: rows where the library departs from the published file, per game. Only
# MBB / WBB free throws move (2014-2026 ESPN types made and missed ones alike "MadeFreeThrow");
# every other row is equal.
PUBLISHED_FT_DIFFS = {
    401830342: 39,
    401856600: 44,
    401638645: 26,
    401637613: 37,
    260312029: 0,
    401585607: 0,
    400927398: 0,
    401726992: 0,
}


@pytest.mark.parametrize(("lg", "name"), COORD_CAPTURES)
def test_published_rows_equal_except_ncaa_free_throws(monkeypatch, lg: str, name: str) -> None:
    summary = _summary(name)
    plays, _ = _process(monkeypatch, lg, summary)
    gid = int(summary["header"]["id"])
    published = pl.read_parquet(FIX / "basketball_pbp" / "published_coordinates.parquet").filter(
        pl.col("game_id") == gid
    )
    lib = plays.select(
        pl.col("id").cast(pl.Int64),
        pl.col("type.text").alias("type_text"),
        *[pl.col(f"coordinate.{a}{r}").alias(f"lib_{a}{r}") for a in "xy" for r in ("", ".raw")],
    )
    j = lib.join(published, on="id", how="full", coalesce=True)
    assert j.height == plays.height == published.height
    cols = [
        ("lib_x", "coordinate_x"),
        ("lib_y", "coordinate_y"),
        ("lib_x.raw", "coordinate_x_raw"),
        ("lib_y.raw", "coordinate_y_raw"),
    ]
    same = pl.all_horizontal(pl.col(a).eq_missing(pl.col(b)) for a, b in cols)
    diff = j.filter(~same)
    # every departure is an NCAA free throw on the line; R left those at ESPN's coordinates
    ft = pl.col("type_text").str.contains(r"(?i)free ?throw") & ~pl.col("type_text").str.contains("Free Throw")
    assert diff.filter(~ft).height == 0
    # ... in the transformed pair only: the raw pair equals the published file on every row
    assert diff.filter(~pl.col("lib_x.raw").eq_missing(pl.col("coordinate_x_raw"))).height == 0
    assert diff.filter(~pl.col("lib_y.raw").eq_missing(pl.col("coordinate_y_raw"))).height == 0
    assert diff.height == PUBLISHED_FT_DIFFS[gid]
    assert (lg in ("mbb", "wbb")) == (diff.height > 0)


def test_a_season_without_coordinates_passes_through(monkeypatch) -> None:
    plays, _ = _process(monkeypatch, "wbb", _summary(HALVES))
    assert not [c for c in plays.columns if c.startswith("coordinate")]
    rel = helper_wbb_play_by_play({**_summary(HALVES), "plays": plays.to_dicts()})
    # the release converter appends the four columns all-null, the creation script's fallback
    assert rel.select(pl.col("^coordinate_.*$").null_count()).row(0) == (rel.height,) * 4


@pytest.mark.parametrize(("lg", "name"), COORD_CAPTURES)
def test_release_converter_does_not_transform_twice(monkeypatch, lg: str, name: str) -> None:
    # the producers feed the processor's output to helper_<lg>_play_by_play; a payload
    # that already carries the raw pair converts exactly as one holding ESPN's values
    summary = _summary(name)
    plays, _ = _process(monkeypatch, lg, summary)
    new = plays.to_dicts()
    # the pre-PR payload: ESPN's values under coordinate.{x,y}, where the raw pair now sits
    back = {"coordinate.x.raw": "coordinate.x", "coordinate.y.raw": "coordinate.y"}
    old = [{back.get(k, k): v for k, v in p.items() if k not in ("coordinate.x", "coordinate.y")} for p in new]
    convert = CONVERTERS[lg]
    a, b = convert({**summary, "plays": new}), convert({**summary, "plays": old})
    assert a.columns == b.columns
    assert a.equals(b)
    assert a["coordinate_x"].to_list() == plays["coordinate.x"].to_list()


@pytest.mark.parametrize("name", ["summary_nba.json", "basketball_pbp/nba_260312029.json.gz"])
def test_game_page_shots_read_the_library_frame(monkeypatch, name: str) -> None:
    # nba_game_page.shot_coordinates reads coordinate_{x,y}_raw (#749); the processor's own
    # frame now carries them, so the page locates shots without the release converter
    summary = _summary(name)
    plays, _ = _process(monkeypatch, "nba", summary)
    shots = shot_coordinates(_snake(plays)).filter(pl.col("x_legacy").is_not_null())
    espn = {
        int(p["id"]): p["coordinate"]
        for p in summary["plays"]
        if p.get("shootingPlay")
        and "free throw" not in p["type"]["text"].lower()
        and min(float(p["coordinate"]["x"]), float(p["coordinate"]["y"])) > COORD_SENTINEL
    }
    assert shots.height == len(espn) > 0
    for pid, x, y in shots.select("id", "x_legacy", "y_legacy").iter_rows():
        assert (x, y) == ((float(espn[pid]["x"]) - 25) * 10, float(espn[pid]["y"]) * 10)
