"""BB-D27: college game flow (mbb / wbb), on committed real captures.

Fixtures (``tests/fixtures/bb_game_flow``, README there; none synthetic): 42 contests each of the
2025-26 men's and women's seasons from the ``ncaa_{mbb,wbb}_pbp`` and ``ncaa_{mbb,wbb}_possessions``
releases, the ESPN schedule's ``neutral_site`` and ESPN's team box score (``lead_changes``,
``largest_lead``, ``lead_percentage``), the oracle.

The known misses are listed and recomputed, so a new one fails the test; they are ESPN-side (a
``0`` written for a wire-to-wire game) or a contest whose NCAA home/away ESPN ids are swapped
against ESPN's box (its points disagree, so it is left out of the comparison). The gates are the
values observed on these contests; never lower one to make a change pass.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.game_flow import GAME_FLOW_SCHEMA, attach_home_wp, game_flow, team_season_flow
from sportsdataverse.mbb import mbb_loaders
from sportsdataverse.mbb.mbb_game_flow import mbb_game_flow, ncaa_score_rows
from sportsdataverse.mbb.mbb_team_season_rates import ncaa_possession_rate_rows
from sportsdataverse.wbb import wbb_loaders
from sportsdataverse.wbb.wbb_game_flow import wbb_game_flow
from tests.nba.test_nba_game_flow import flow_kills, flow_runs, recount_kills, recount_runs

FLOW = Path(__file__).resolve().parents[1] / "fixtures" / "bb_game_flow"
LEAGUES = {"mbb": "mens", "wbb": "womens"}
#: ESPN writes lead_changes 0 / lead_percentage 0.0 for a team that led 96% of 6459088.
ESPN_ZERO_GAMES = {"mbb": {"6459088"}, "wbb": set()}
#: The NCAA release's home/away ESPN ids are swapped against ESPN's box for 6407871.
SWAPPED_GAMES = {"mbb": {"6407871"}, "wbb": set()}
#: The contest with a non-D1 side (no ESPN team id): only the D1 team gets a row.
NON_D1 = {"mbb": ("6398358", 2619), "wbb": ("6387146", 45)}


def _read(lg: str, kind: str) -> pl.DataFrame:
    name = {
        "pbp": f"ncaa_{lg}_pbp",
        "poss": f"ncaa_{lg}_possessions",
        "sched": f"{lg}_schedule",
        "box": f"{lg}_team_box",
    }
    return pl.read_parquet(FLOW / f"{name[kind]}_2026.parquet")


def _flow(lg: str) -> pl.DataFrame:
    sched = _read(lg, "sched")
    rows = attach_home_wp(ncaa_score_rows(_read(lg, "pbp"), sched, league=LEAGUES[lg]), league=lg)
    return game_flow(rows, ncaa_possession_rate_rows(_read(lg, "poss"), sched, league=LEAGUES[lg]))


@pytest.fixture(scope="module", params=list(LEAGUES))
def league(request: pytest.FixtureRequest) -> tuple[str, pl.DataFrame]:
    return request.param, _flow(request.param)


def _vs_espn(lg: str, flow: pl.DataFrame) -> pl.DataFrame:
    xw = (
        _read(lg, "pbp")
        .select(pl.col("contest_id").cast(pl.Utf8).alias("game_id"), pl.col("espn_game_id").cast(pl.Int64))
        .unique()
    )
    box = _read(lg, "box").select(
        pl.col("game_id").cast(pl.Int64).alias("espn_game_id"),
        pl.col("team_id").cast(pl.Int64),
        pl.col("team_score").cast(pl.Int64).alias("espn_pts"),
        pl.col("largest_lead").cast(pl.Int64).alias("espn_largest_lead"),
        pl.col("lead_changes").cast(pl.Int64).alias("espn_lead_changes"),
        pl.col("lead_percentage").cast(pl.Float64).alias("espn_lead_pct"),
    )
    assert xw.schema["espn_game_id"] == box.schema["espn_game_id"]
    assert flow.schema["game_id"] == xw.schema["game_id"] and flow.schema["team_id"] == box.schema["team_id"]
    return flow.join(xw, on="game_id").join(box, on=["espn_game_id", "team_id"], how="inner")


def test_points_match_espn_except_the_swapped_contest(league: tuple[str, pl.DataFrame]) -> None:
    lg, flow = league
    j = _vs_espn(lg, flow)
    assert j.height == flow.height  # every D1 row has an ESPN box row
    assert set(j.filter(pl.col("pts") != pl.col("espn_pts"))["game_id"]) == SWAPPED_GAMES[lg]


def test_lead_changes_largest_lead_and_lead_pct_match_espn(league: tuple[str, pl.DataFrame]) -> None:
    lg, flow = league
    stats = ("lead_changes", "largest_lead", "largest_deficit", "lead_pct", "avg_wp", "max_run")
    assert flow.select(pl.col(*stats).null_count()).row(0) == (0,) * len(stats)  # a null never slips past the filters
    j = _vs_espn(lg, flow).filter(pl.col("pts") == pl.col("espn_pts"))
    lc_miss = set(j.filter(pl.col("lead_changes") != pl.col("espn_lead_changes"))["game_id"])
    assert lc_miss == ESPN_ZERO_GAMES[lg]
    assert j.filter(pl.col("largest_lead") != pl.col("espn_largest_lead")).height == 0
    # ESPN rounds lead_percentage to a tenth of a point; observed max 0.5 points.
    ok = j.filter(~pl.col("game_id").is_in(list(ESPN_ZERO_GAMES[lg])))
    assert ok.select((100 * pl.col("lead_pct") - pl.col("espn_lead_pct")).abs().max()).item() <= 0.5


def test_the_two_rows_of_a_game_mirror(league: tuple[str, pl.DataFrame]) -> None:
    _, flow = league
    both = flow.join(flow, left_on=["game_id", "opp_team_id"], right_on=["game_id", "team_id"], suffix="_o")
    assert both.height == flow.height - 1  # the non-D1 contest has one row
    assert (both["lead_sec"] == both["trail_sec_o"]).all()
    assert (both["largest_lead"] == both["largest_deficit_o"]).all()
    assert ((both["avg_margin"] + both["avg_margin_o"]).abs() < 1e-9).all()
    assert ((both["avg_wp"] + both["avg_wp_o"] - 1).abs() < 1e-9).all()
    assert ((flow["lead_sec"] + flow["trail_sec"] + flow["tied_sec"] - flow["game_seconds"]).abs() < 1e-6).all()


def test_a_non_d1_side_gets_no_row(league: tuple[str, pl.DataFrame]) -> None:
    lg, flow = league
    game, team = NON_D1[lg]
    rows = flow.filter(pl.col("game_id") == game)
    assert rows["team_id"].to_list() == [team]
    assert rows["opp_team_id"].null_count() == 1
    # the non-D1 side's runs still count against the D1 team
    want, _ = recount_runs(
        ncaa_score_rows(_read(lg, "pbp"), _read(lg, "sched"), league=LEAGUES[lg]).filter(pl.col("game_id") == game)
    )
    opp_home = not rows["is_home"].item()
    assert rows["opp_runs_10"].item() == want[(game, opp_home)][1]


def test_stops_and_kills_recount(league: tuple[str, pl.DataFrame]) -> None:
    """An independent loop over each defense's possessions in order gives the same counts."""
    lg, flow = league
    poss = ncaa_possession_rate_rows(_read(lg, "poss"), _read(lg, "sched"), league=LEAGUES[lg])
    assert flow_kills(flow) == recount_kills(poss)


def test_runs_droughts_and_margin_recount(league: tuple[str, pl.DataFrame]) -> None:
    lg, flow = league
    rows = ncaa_score_rows(_read(lg, "pbp"), _read(lg, "sched"), league=LEAGUES[lg])
    assert rows.sort("game_id", "order").select((pl.col("elapsed").diff().over("game_id") < 0).sum()).item() == 0
    want, _ = recount_runs(rows)
    have = flow_runs(flow)
    assert {k: want[k] for k in have} == have  # the non-D1 side has no row


def test_game_control_tracks_the_margin(league: tuple[str, pl.DataFrame]) -> None:
    _, flow = league
    # observed on the fixture: corr 0.93 (MBB) / 0.94 (WBB); winners average a 0.73 / 0.75 WP
    assert flow.select(pl.corr("avg_wp", "avg_margin")).item() >= 0.9
    assert flow.filter("won")["avg_wp"].mean() >= 0.7
    assert flow["avg_wp"].is_between(0, 1).all()


def test_season_table_ranks_and_rollups(league: tuple[str, pl.DataFrame]) -> None:
    _, flow = league
    season = team_season_flow(flow)
    # every team of the fixture plays once or twice, so every team qualifies
    assert season.height == flow["team_id"].n_unique()
    totals = flow.group_by("team_id").agg(pl.col("kills").sum(), pl.col("max_run").max())
    assert season.select("team_id", "kills", "max_run").sort("team_id").equals(totals.sort("team_id"))
    for metric, best in (("avg_longest_drought_sec", "min"), ("kills_pg", "max")):  # the drought ranks low first
        top = season.filter(pl.col(f"{metric}_rank") == pl.col(f"{metric}_rank").min())
        assert top[metric].unique().to_list() == [getattr(season[metric], best)()]


def test_neutral_site_gets_the_even_prior() -> None:
    rows = ncaa_score_rows(_read("mbb", "pbp"), _read("mbb", "sched"), league="mens")
    assert rows.filter(pl.col("neutral_site"))["game_id"].n_unique() == 7
    tips = rows.sort("game_id", "order").group_by("game_id", maintain_order=True).first()
    home = attach_home_wp(tips.with_columns(pl.lit(False).alias("neutral_site")), league="mbb")["home_wp"]
    even = attach_home_wp(tips.with_columns(pl.lit(True).alias("neutral_site")), league="mbb")["home_wp"]
    assert (home > even).all()  # the home-court prior lifts the home side at every tip


def test_empty_input_returns_the_schema() -> None:
    empty = ncaa_score_rows(_read("mbb", "pbp").head(0), _read("mbb", "sched"), league="mens")
    assert game_flow(attach_home_wp(empty, league="mbb"), pl.DataFrame()).schema == pl.Schema(GAME_FLOW_SCHEMA)
    with pytest.raises(ValueError, match="league"):
        ncaa_score_rows(_read("mbb", "pbp"), _read("mbb", "sched"), league="girls")


def test_public_functions_read_the_releases(monkeypatch: pytest.MonkeyPatch) -> None:
    mbb_team_season_rates = importlib.import_module("sportsdataverse.mbb.mbb_team_season_rates")

    # the men's possessions and schedule loaders are bound in mbb_team_season_rates; the rest are
    # looked up on their loader module at call time
    targets = {
        "mbb": {
            "pbp": (mbb_loaders, "load_ncaa_mbb_pbp"),
            "poss": (mbb_team_season_rates, "load_ncaa_mbb_possessions"),
            "sched": (mbb_team_season_rates, "load_mbb_schedule"),
        },
        "wbb": {
            "pbp": (wbb_loaders, "load_ncaa_wbb_pbp"),
            "poss": (wbb_loaders, "load_ncaa_wbb_possessions"),
            "sched": (wbb_loaders, "load_wbb_schedule"),
        },
    }
    for lg, fn in (("mbb", mbb_game_flow), ("wbb", wbb_game_flow)):
        for kind, (module, name) in targets[lg].items():
            monkeypatch.setattr(module, name, lambda _seasons, _lg=lg, _k=kind: _read(_lg, _k))
        assert fn(2026).equals(_flow(lg))
