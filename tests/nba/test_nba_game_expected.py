"""BB-D40: nba_game_expected on real data (fixtures: tests/fixtures/bb_game_expected/README.md).

* The per-shot expectation reproduces BB-D9's production SQL on the real WNBA 2025 season.
* On real ESPN WNBA 2025 games its expected points track BB-D9's total for the same game.
* On the BB-D31 captures of all four leagues the zones and situations add up to the totals.
"""

from __future__ import annotations

import copy
import datetime as dt
import gzip
import importlib
import json
import math
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nba import nba_game_expected as ge
from sportsdataverse.nba import nba_game_page as gp

FIX = Path(__file__).resolve().parents[1] / "fixtures"
EXP = FIX / "bb_game_expected"
BB = FIX / "espn" / "basketball_pbp"
#: (ESPN game, stats.wnba.com game) of the three ESPN captures.
WNBA_2025 = [(401736238, "1022500127"), (401736239, "1022500128"), (401736240, "1022500129")]
#: ESPN-located over BB-D9 feed-located game expected points: 0.978-1.006 on the 38 games of
#: espn_vs_d9_wnba_2025.csv (largest gap 0.022), so a game more than 2.5% off fails.
ESPN_TOLERANCE = 0.025
#: The BB-D31 captures (tests/nba/test_nba_game_splits.py's GAMES).
GAMES = [
    ("nba", BB / "nba_401766458.json.gz"),
    ("nba", BB / "nba_401704638.json.gz"),
    ("wnba", BB / "wnba_400927398.json.gz"),
    ("mbb", BB / "mbb_401812161.json.gz"),
    ("mbb", BB / "mbb_401856600.json.gz"),
    ("wbb", BB / "wbb_401364640.json.gz"),
    ("wbb", BB / "wbb_400509734.json.gz"),
    *[("wnba", EXP / f"wnba_{g}.json.gz") for g, _ in WNBA_2025],
]


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._p = payload

    def json(self) -> dict:
        return self._p


def _frames(monkeypatch: pytest.MonkeyPatch, lg: str, path: Path, cells: pl.DataFrame | None = None) -> dict:
    """``game_page_frames`` on a capture, offline (the capture is served as the HTTP body)."""
    with gzip.open(path, "rt", encoding="utf-8") as f:
        raw = json.load(f)
    mod = importlib.import_module(f"sportsdataverse.{lg}.{lg}_pbp")
    monkeypatch.setattr(mod, "download", lambda url, **kwargs: _Resp(copy.deepcopy(raw)))
    final = getattr(mod, f"espn_{lg}_pbp")(game_id=int(raw["header"]["id"]), return_parsed=False)
    return gp.game_page_frames(final, league=lg, shot_cells=cells)


@pytest.fixture(scope="module")
def history() -> pl.DataFrame:
    dates = (
        pl.read_parquet(EXP / "wnba_game_logs_2024_2025.parquet")
        .with_columns(pl.col("game_date").str.slice(0, 10).str.to_date())
        .group_by("season", "game_id")
        .agg(pl.col("game_date").min())
    )
    shots = pl.read_parquet(EXP / "wnba_shots_2024_2025.parquet")
    return shots.join(dates, on=["season", "game_id"]).with_columns(made=pl.col("shot_result") == "Made")


def _on_court(df: pl.DataFrame) -> pl.DataFrame:
    """BB-D9's scored rows: located on the court, a two or a three, with a distance."""
    return df.filter(
        pl.col("x_legacy").abs() <= 250,
        pl.col("y_legacy") >= -52.5,
        pl.col("shot_distance").is_not_null(),
        pl.col("shot_value").is_in([2, 3]),
    )


def _score_by_date(shots: pl.DataFrame, history: pl.DataFrame) -> pl.DataFrame:
    return pl.concat(
        [ge.shot_xfg(g, ge.shot_cells(history, day), league="wnba") for (day,), g in shots.group_by("game_date")]
    )


def test_port_reproduces_d9_on_the_2025_season(history: pl.DataFrame) -> None:
    """Every scored 2025 shot, scored day by day: fit_wnba_stats.json's 2025 numbers, from BB-D9's SQL."""
    scored = _score_by_date(_on_court(history.filter(pl.col("season") == 2025)), history)
    pts = pl.when(pl.col("made")).then(pl.col("shot_value")).otherwise(0)
    two, three = pl.col("shot_value") == 2, pl.col("shot_value") == 3
    got = scored.select(
        n=pl.len(),
        brier=((pl.col("made").cast(pl.Float64) - pl.col("xfg")) ** 2).mean(),
        calibration=(pl.col("xfg") * pl.col("shot_value")).sum() / pts.sum(),
        calibration_2=pl.col("xfg").filter(two).sum() / (pl.col("made") & two).sum(),
        calibration_3=pl.col("xfg").filter(three).sum() / (pl.col("made") & three).sum(),
    ).row(0, named=True)
    want = json.loads((EXP / "fit_wnba_stats.json").read_text(encoding="utf-8"))["holdout_scores"]["2025"]
    assert got["n"] == want["n"] == 38_535
    for k in ("brier", "calibration", "calibration_2", "calibration_3"):
        assert math.isclose(got[k], want[k], rel_tol=1e-9), (k, got[k], want[k])


def test_cells_never_see_the_game_day(history: pl.DataFrame) -> None:
    """Flipping every outcome of the scored day and later leaves that day's cells as they were."""
    day = dt.date(2025, 7, 10)
    flipped = history.with_columns(
        pl.when(pl.col("game_date") >= day).then(~pl.col("made")).otherwise("made").alias("made")
    )
    keys = ["kind", "shot_value", "shot_distance", "i", "j"]
    assert ge.shot_cells(flipped, day).sort(keys).equals(ge.shot_cells(history, day).sort(keys))
    later = day + dt.timedelta(days=1)
    assert not ge.shot_cells(flipped, later).sort(keys).equals(ge.shot_cells(history, later).sort(keys))
    # and nothing older than the window: the 2024 season opened 2024-05-14, more than 365 days before
    assert ge.shot_cells(history.filter(pl.col("season") == 2024), dt.date(2025, 6, 1)).height > 0
    assert ge.shot_cells(history.filter(pl.col("season") == 2024), dt.date(2025, 10, 1)).height == 0


@pytest.mark.parametrize(("espn_id", "stats_id"), WNBA_2025)
def test_espn_game_tracks_the_d9_game_total(
    monkeypatch: pytest.MonkeyPatch, history: pl.DataFrame, espn_id: int, stats_id: str
) -> None:
    game = history.filter(pl.col("game_id") == stats_id)
    day = game["game_date"][0]
    cells = ge.shot_cells(history, day)
    team = _frames(monkeypatch, "wnba", EXP / f"wnba_{espn_id}.json.gz", cells)["team_game_expected"]
    d9 = _on_court(game)
    d9_xpts = float((ge.shot_xfg(d9, cells, league="wnba")["xfg"] * d9["shot_value"]).sum())
    assert abs(team["xfg_pts"].sum() / d9_xpts - 1) <= ESPN_TOLERANCE
    # the same attempts and made field-goal points per team as the feed
    pts = pl.when(pl.col("made")).then(pl.col("shot_value")).otherwise(0)
    feed = game.group_by("team_id").agg(pl.len().alias("fga"), pts.sum().alias("fg_pts"))
    assert sorted(team.select("fga", "fg_pts").rows()) == sorted(feed.select("fga", "fg_pts").rows())
    assert team["unlocated_fga"].sum() == 0


def test_the_espn_tolerance_comes_from_the_measured_games() -> None:
    m = pl.read_csv(EXP / "espn_vs_d9_wnba_2025.csv").with_columns(
        (pl.col("espn_xpts") / pl.col("d9_xpts") - 1).abs().alias("gap")
    )
    assert m.height == 38
    assert m["gap"].max() < ESPN_TOLERANCE <= m["gap"].max() + 0.005


@pytest.mark.parametrize(("lg", "path"), GAMES, ids=[p.name for _, p in GAMES])
def test_zones_and_situations_add_up(monkeypatch: pytest.MonkeyPatch, lg: str, path: Path) -> None:
    f = _frames(monkeypatch, lg, path)
    e, s = f["team_game_expected"], f["team_game_splits"]
    assert e.height == 2
    assert e.schema == pl.Schema(ge.TEAM_EXPECTED_SCHEMA)
    for c in ("fga", "fgm"):
        assert (
            e.select(pl.sum_horizontal([*[f"{z}_{c}" for z in ge.ZONES], f"unlocated_{c}"])).to_series() == e[c]
        ).all()
    joined = e.join(s, on="team_id", suffix="_d31")
    for a, b in (("fga", "fga_d31"), ("points", "points_d31"), ("possessions", "possessions_d31")):
        assert joined[a].to_list() == joined[b].to_list(), a
    assert joined["fast_break_poss"].to_list() == joined["transition_poss"].to_list()
    assert joined["second_chance_poss"].to_list() == joined["second_chance_poss_d31"].to_list()
    assert joined["second_chance_pts"].to_list() == joined["second_chance_pts_d31"].to_list()
    assert ((e["off_turnover_pts"] - s["pts_off_live_tov"] - s["pts_off_dead_tov"]) == 0).all()
    for sit in ge.SITUATIONS:
        assert (e[f"{sit}_poss"] <= e["possessions"]).all()
    # without the league's cells every expected column is null, the counts are not
    for c in ge._EXPECTED_COLS:
        assert e[c].is_null().all(), c
    assert e["efg_pct"].is_not_null().all() and e["ortg"].is_not_null().all()
    if (e["unlocated_fga"] < e["fga"]).any():  # captures with play coordinates
        assert e["rim_fga"].sum() > 0 and e["above_break3_fga"].sum() > 0 and e["midrange_fga"].sum() > 0


def test_expected_columns_with_cells(monkeypatch: pytest.MonkeyPatch, history: pl.DataFrame) -> None:
    cells = ge.shot_cells(history, dt.date(2025, 7, 10))
    f = _frames(monkeypatch, "wnba", EXP / "wnba_401736239.json.gz", cells)
    e = f["team_game_expected"]
    # paint possessions: the possessions holding an attempt in the restricted area or the rest of the
    # paint, recounted from the play text's distance (else the coordinates') (add_shot_zones' rule)
    fga = f["pbp"].filter(
        pl.col("shooting_play")
        & ~pl.col("type_text").str.contains("(?i)free ?throw")
        & pl.col("x_legacy").is_not_null()
    )
    hypot = ((fga["x_legacy"] ** 2 + fga["y_legacy"] ** 2).sqrt() / 10 + 0.5).floor().cast(pl.Int64)
    feet = fga["text"].str.extract(r"(\d+)-foot", 1).cast(pl.Int64).fill_null(hypot)
    two = ~fga["text"].str.contains("(?i)three point")
    in_paint = two & ((feet < 4) | ((feet < 8) & (fga["x_legacy"].abs() <= 80)))
    assert int(in_paint.sum()) == int(e["rim_fga"].sum() + e["short_mid_fga"].sum())
    assert (e["paint_poss"] >= 1).all() and e["paint_poss"].sum() <= in_paint.sum()
    assert e["paint_poss"].sum() > e["rim_fga"].sum()
    # the zones' and the totals' points over expected agree
    zones = e.select(pl.sum_horizontal([f"{z}_poe" for z in ge.ZONES])).to_series()
    assert ((zones - e["poe"]).abs() < 1e-9).all()
    # each team's expected defence is the other's expected offence
    assert e["xdrtg"].to_list() == e["xortg"].reverse().to_list()
    assert e["drtg"].to_list() == e["ortg"].reverse().to_list()
    assert ((e["xortg"] - 100 * (e["points"] - e["poe"]) / e["possessions"]).abs() < 1e-9).all()
    xts = (e["xfg_pts"] + e["ftm"]) / (2 * (e["fga"] + 0.44 * e["fta"]))
    assert ((e["xts_pct"] - xts).abs() < 1e-12).all()
    for z in ge.ZONES:
        assert e[f"{z}_xppp"].is_between(0.0, 3.0).all(), z
    for sit in ge.SITUATIONS:
        assert e[f"{sit}_poe"].is_not_null().all(), sit


def test_empty_before_tip_off() -> None:
    out = ge.team_game_expected(pl.DataFrame(), league="wnba")
    assert out.height == 0 and out.schema == pl.Schema(ge.TEAM_EXPECTED_SCHEMA)
    assert (
        ge.shot_cells(
            pl.DataFrame(
                schema={
                    "game_date": pl.Date,
                    "shot_value": pl.Int64,
                    "shot_distance": pl.Int64,
                    "x_legacy": pl.Int64,
                    "y_legacy": pl.Int64,
                    "made": pl.Boolean,
                }
            ),
            dt.date(2025, 7, 1),
        ).height
        == 0
    )


def test_unknown_league_raises() -> None:
    with pytest.raises(ValueError, match="league must be one of"):
        ge.shot_xfg(pl.DataFrame(), pl.DataFrame(schema=ge.CELLS_SCHEMA), league="g-league")
