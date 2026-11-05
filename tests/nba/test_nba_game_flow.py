"""BB-D27: NBA / WNBA game flow, on committed real captures.

Fixtures (``tests/fixtures/bb_game_flow``, README there; none synthetic): 103 games of the 2025-26
NBA season and 62 of the 2025 WNBA season (regular season and playoffs) from the
``*_stats_pbp``, ``*_stats_possessions`` and ``*_stats_schedules`` releases, plus each game's
official cdn.nba.com / cdn.wnba.com live box score (``leadChanges``, ``timesTied``,
``biggestLead``, ``timeLeading``, ``biggestScoringRun``), the oracle; and two 2002 WNBA games,
played in halves.

The gates are the values observed on these games; never lower one to make a change pass. The
``bb-game-flow`` rows of ``tests/mutants/registry.yaml`` go red here.
"""

from __future__ import annotations

from collections import Counter
from itertools import pairwise
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.game_flow import GAME_FLOW_SCHEMA, RUN_POINTS, attach_home_wp, game_flow, team_season_flow
from sportsdataverse.nba import nba_loaders
from sportsdataverse.nba.nba_game_flow import nba_game_flow, nba_score_rows, nba_team_season_flow
from sportsdataverse.nba.nba_team_season_rates import nba_possession_rate_rows
from sportsdataverse.wnba import wnba_loaders
from sportsdataverse.wnba.wnba_game_flow import wnba_game_flow

FLOW = Path(__file__).resolve().parents[1] / "fixtures" / "bb_game_flow"
#: league -> (season the files are named by, league_id, the model league)
LEAGUES = {"nba": ("2026", "00"), "wnba": ("2025", "10")}
#: The largest |time leading - official timeLeading| observed, in seconds (the WNBA feed's
#: clock is rounded differently below a minute).
TIME_LEADING_TOL = {"nba": 0.01, "wnba": 1.0}
SEASON_TYPES = ("regular-season", "playoffs")
#: Observed on the fixture: score corrections and rows where both scores move (the rare run branches).
OBSERVED_BRANCHES = {"nba": {"correction": 5, "both": 3}, "wnba": {"correction": 2, "both": 2}}
#: Observed: team-games whose max_run equals the official biggestScoringRun.
OBSERVED_RUN_EQUAL = {"nba": 146, "wnba": 84}
#: Observed: the teams under half the most games (13), so unranked in the fixture's season table.
OBSERVED_UNRANKED = {"nba": 15, "wnba": 1}


def _read(lg: str, kind: str, season: str | None = None) -> pl.DataFrame:
    name = {"pbp": "pbp", "poss": "possessions", "sched": "schedule", "box": "live_boxscore_flow"}[kind]
    return pl.read_parquet(FLOW / f"{lg}_{name}_{season or LEAGUES[lg][0]}.parquet")


def _flow(lg: str, season: str | None = None) -> pl.DataFrame:
    lid = LEAGUES[lg][1]
    pbp, poss, sched = _read(lg, "pbp", season), _read(lg, "poss", season), _read(lg, "sched", season)
    parts = []
    for st in SEASON_TYPES:
        rows = nba_score_rows(pbp, sched, league_id=lid, season_type=st)
        if rows.height:
            stops = nba_possession_rate_rows(poss, sched, league_id=lid, season_type=st)
            parts.append(game_flow(attach_home_wp(rows, league=lg), stops))
    return pl.concat(parts)


@pytest.fixture(scope="module", params=list(LEAGUES))
def league(request: pytest.FixtureRequest) -> tuple[str, pl.DataFrame]:
    return request.param, _flow(request.param)


def _vs_box(lg: str, flow: pl.DataFrame) -> pl.DataFrame:
    box = _read(lg, "box").with_columns(
        (
            pl.col("time_leading").str.extract(r"PT(\d+)M", 1).cast(pl.Float64) * 60
            + pl.col("time_leading").str.extract(r"M([\d.]+)S", 1).cast(pl.Float64)
        ).alias("box_lead_sec")
    )
    assert box.schema["game_id"] == flow.schema["game_id"] and box.schema["team_id"] == flow.schema["team_id"]
    return flow.join(box, on=["game_id", "team_id"], how="inner", validate="1:1")


def test_every_fixture_game_is_built(league: tuple[str, pl.DataFrame]) -> None:
    lg, flow = league
    sched = _read(lg, "sched")
    assert flow.height == 2 * sched.height
    finals = pl.concat(
        [
            sched.select("game_id", pl.col(f"{s}_team_id").alias("team_id"), pl.col(f"{s}_pts").alias("final"))
            for s in ("home", "away")
        ]
    )
    j = flow.join(finals, on=["game_id", "team_id"], how="inner")
    assert j.height == flow.height and (j["pts"] == j["final"]).all()


def test_lead_changes_ties_largest_lead_and_time_leading_match_the_official_box(
    league: tuple[str, pl.DataFrame],
) -> None:
    lg, flow = league
    j = _vs_box(lg, flow)
    assert j.height == flow.height  # every game has its box
    assert (j["pts"] == j["score"]).all()
    for ours, theirs in (("lead_changes", "lead_changes_right"), ("times_tied", "times_tied_right")):
        assert (j[ours] == j[theirs]).all(), ours
    assert (j["largest_lead"] == j["biggest_lead"]).all()
    assert (j["lead_sec"] - j["box_lead_sec"]).abs().max() <= TIME_LEADING_TOL[lg]


def test_max_run_is_an_n0_run_inside_the_official_biggest_run(league: tuple[str, pl.DataFrame]) -> None:
    """The official ``biggestScoringRun`` lets the opponent score a little (a 12-2 run counts 12)."""
    lg, flow = league
    j = _vs_box(lg, flow)
    assert (j["max_run"] <= j["biggest_scoring_run"]).all()
    assert (j["max_run"] == j["biggest_scoring_run"]).sum() == OBSERVED_RUN_EQUAL[lg]
    if lg == "nba":
        # 0022500048: CLE's official 12 is 91-79 to 103-81; the unanswered part is 95-81 to 103-81.
        one = j.filter(pl.col("game_id") == "0022500048").sort("team_id")
        assert one.select("team_id", "max_run", "biggest_scoring_run").rows() == [
            (1610612739, 8, 12),
            (1610612754, 14, 14),
        ]
    else:
        # 1022500008: 75-79, a review takes 3 off (76), then 82, 84, 86: the official 11, not 4+6+2+2
        one = j.filter((pl.col("game_id") == "1022500008") & (pl.col("team_id") == 1611661324))
        assert one.select("max_run", "biggest_scoring_run").rows() == [(11, 11)]


def test_stale_scores_on_period_and_replay_rows_are_not_read() -> None:
    flow = _vs_box("nba", _flow("nba"))
    # a replay row reads 13-15 at 16-15: reading it adds two lead changes (9, not 7)
    replay = flow.filter(pl.col("game_id") == "0022500109")
    assert replay["lead_changes"].to_list() == [7, 7] == replay["lead_changes_right"].to_list()
    # the end of the 3rd reads 60-55 at 86-87: reading it gives a 5-point lead that never was
    period = flow.filter(pl.col("game_id") == "0022500232")
    assert (period["largest_lead"] == period["biggest_lead"]).all()


def test_the_two_rows_of_a_game_mirror(league: tuple[str, pl.DataFrame]) -> None:
    _, flow = league
    both = flow.join(flow, left_on=["game_id", "opp_team_id"], right_on=["game_id", "team_id"], suffix="_o")
    assert both.height == flow.height
    assert (both["lead_sec"] == both["trail_sec_o"]).all()
    assert (both["largest_lead"] == both["largest_deficit_o"]).all()
    assert ((both["avg_margin"] + both["avg_margin_o"]).abs() < 1e-9).all()
    assert ((both["avg_wp"] + both["avg_wp_o"] - 1).abs() < 1e-9).all()
    assert (both["won"] != both["won_o"]).all()
    assert ((flow["lead_sec"] + flow["trail_sec"] + flow["tied_sec"] - flow["game_seconds"]).abs() < 1e-6).all()
    assert (flow.filter("won")["comeback"] == flow.filter("won")["largest_deficit"]).all()
    assert (flow.filter(~pl.col("won"))["comeback"] == 0).all()


def recount_runs(rows: pl.DataFrame) -> tuple[dict[tuple[str, bool], tuple[int, int, float, float]], Counter[str]]:
    """An independent loop over score rows: per (game, is_home) max_run, runs_10, longest drought, avg margin.

    Also counts the branches it took (``correction``: a score going down; ``both``: both scores
    moving on one row), so a test can show the rare branches fired.
    """
    want: dict[tuple[str, bool], tuple[int, int, float, float]] = {}
    branches: Counter[str] = Counter()
    for (gid,), g in rows.sort("game_id", "order").group_by("game_id", maintain_order=True):
        seq = list(zip(g["elapsed"], g["home_score"], g["away_score"], strict=True))
        length = g["game_seconds"][0]
        ends = [t for t, _, _ in seq[1:]] + [length]
        area = sum((h - a) * max(end - t, 0.0) for (t, h, a), end in zip(seq, ends, strict=True))
        runs: list[tuple[str, int]] = []
        scored: dict[str, list[float]] = {"home": [0.0], "away": [0.0]}
        prev = (0, 0)
        for t, h, a in seq:
            dh, da = h - prev[0], a - prev[1]
            prev = (h, a)
            for who, delta in (("home", dh), ("away", da)):
                if delta > 0:  # any score ends a drought, even one the other side matched
                    scored[who].append(t)
            side = "home" if dh > 0 and da <= 0 else "away" if da > 0 and dh <= 0 else "both" if dh > 0 else None
            if side is None and (dh < 0 or da < 0):  # a correction comes off the run of the side it lowers
                branches["correction"] += 1
                if runs and runs[-1][0] != "both":
                    runs[-1] = (runs[-1][0], runs[-1][1] + (dh if runs[-1][0] == "home" else da))
            elif side == "both":
                branches["both"] += 1
                runs.append(("both", 0))
            elif side and (not runs or runs[-1][0] != side):
                runs.append((side, dh if side == "home" else da))
            elif side:
                runs[-1] = (side, runs[-1][1] + (dh if side == "home" else da))
        for side in ("home", "away"):
            mine = [n for s, n in runs if s == side]
            drought = max(b - a for a, b in pairwise([*scored[side], length]))
            margin = round((area if side == "home" else -area) / length, 9)
            want[(str(gid), side == "home")] = (
                max(mine, default=0),
                sum(n >= RUN_POINTS for n in mine),
                drought,
                margin,
            )
    return want, branches


def flow_runs(flow: pl.DataFrame) -> dict[tuple[str, bool], tuple[int, int, float, float]]:
    """The same four numbers from the game-flow table."""
    cols = ("game_id", "is_home", "max_run", "runs_10", "longest_drought_sec", "avg_margin")
    return {(g, h): (r, n, d, round(m, 9)) for g, h, r, n, d, m in flow.select(cols).iter_rows()}


def recount_kills(poss: pl.DataFrame) -> dict[tuple[str, int], tuple[int, int, int, int]]:
    """An independent loop over each defense's possessions: def_poss, stops, kills, double kills."""
    want: dict[tuple[str, int], tuple[int, int, int, int]] = {}
    for (gid, team), part in poss.sort("game_id", "period", "possession_number").group_by(
        "game_id", "defense_team_id", maintain_order=True
    ):
        if team is None:
            continue
        stops = kills = doubles = streak = 0
        for pts in part["points"]:
            streak = streak + 1 if pts == 0 else 0
            stops += pts == 0
            kills += streak > 0 and streak % 3 == 0
            doubles += streak > 0 and streak % 6 == 0
        want[(str(gid), int(team))] = (part.height, stops, kills, doubles)
    return want


def flow_kills(flow: pl.DataFrame) -> dict[tuple[str, int], tuple[int, int, int, int]]:
    """The same four numbers from the game-flow table (rows with possessions only)."""
    cols = ("game_id", "team_id", "def_poss", "stops", "kills", "double_kills")
    return {
        (g, t): (p, s, k, d)
        for g, t, p, s, k, d in flow.filter(pl.col("def_poss").is_not_null()).select(cols).iter_rows()
    }


def _rows(lg: str) -> pl.DataFrame:
    return pl.concat(
        [
            nba_score_rows(_read(lg, "pbp"), _read(lg, "sched"), league_id=LEAGUES[lg][1], season_type=st)
            for st in SEASON_TYPES
        ]
    )


def test_runs_and_droughts_recount(league: tuple[str, pl.DataFrame]) -> None:
    """An independent loop over the score sequence gives the same runs, droughts and average margin."""
    lg, flow = league
    want, branches = recount_runs(_rows(lg))
    assert flow_runs(flow) == want
    assert dict(branches) == OBSERVED_BRANCHES[lg]  # both rare branches fire on the fixture


def test_stops_and_kills_recount(league: tuple[str, pl.DataFrame]) -> None:
    lg, flow = league
    sched = _read(lg, "sched")
    lid = LEAGUES[lg][1]
    poss = pl.concat(
        [nba_possession_rate_rows(_read(lg, "poss"), sched, league_id=lid, season_type=st) for st in SEASON_TYPES]
    )
    assert flow_kills(flow) == recount_kills(poss)


def test_game_control_tracks_the_margin(league: tuple[str, pl.DataFrame]) -> None:
    _, flow = league
    # observed on the fixture: corr 0.94 (NBA) / 0.96 (WNBA); winners average a 0.68 / 0.72 WP
    assert flow.select(pl.corr("avg_wp", "avg_margin")).item() >= 0.9
    assert flow.filter("won")["avg_wp"].mean() >= 0.65
    assert flow["avg_wp"].is_between(0, 1).all()


def test_season_table(league: tuple[str, pl.DataFrame]) -> None:
    lg, flow = league
    season = team_season_flow(flow)
    assert season.height == flow["team_id"].n_unique()
    secs = pl.col("game_seconds")
    totals = flow.group_by("team_id").agg(
        pl.len().alias("games"),
        pl.col("kills").sum(),
        (pl.col("lead_sec").sum() / secs.sum()).alias("lead_pct"),
        ((pl.col("avg_margin") * secs).sum() / secs.sum()).alias("avg_margin"),
        ((pl.col("avg_wp") * secs).sum() / secs.sum()).alias("avg_wp"),
    )
    j = season.join(totals, on="team_id", suffix="_g")
    assert (j["games"] == j["games_g"]).all() and (j["kills"] == j["kills_g"]).all()
    for m in ("lead_pct", "avg_margin", "avg_wp"):  # weighted by game clock, not a mean of games
        assert ((j[m] - j[f"{m}_g"]).abs() < 1e-12).all(), m
    unranked = season.filter(pl.col("kills_pg_rank").is_null())
    assert unranked.height == OBSERVED_UNRANKED[lg]
    assert (unranked["games"] < 0.5 * season["games"].max()).all()  # the half-the-most-games gate
    ranked = season.filter(pl.col("stop_pct_rank").is_not_null())
    assert ranked.height == season.height - OBSERVED_UNRANKED[lg]
    assert (ranked["stop_pct_n"] == ranked["def_poss"]).all()
    low = season.filter(pl.col("opp_runs_10_pg_rank") == pl.col("opp_runs_10_pg_rank").min())
    assert low["opp_runs_10_pg"].unique().to_list() == [ranked["opp_runs_10_pg"].min()]


def test_wnba_halves_games() -> None:
    flow = _flow("wnba", "2002")
    assert sorted(flow.select("game_id", "game_seconds").unique().rows()) == [
        ("1020200001", 2700.0),
        ("1020200002", 2400.0),
    ]
    sched = _read("wnba", "sched", "2002")
    assert sorted(flow.filter("is_home")["pts"].to_list()) == sorted(sched["home_pts"].to_list())


def test_errors_and_empty_input() -> None:
    with pytest.raises(ValueError, match="league_id"):
        nba_score_rows(_read("nba", "pbp"), _read("nba", "sched"), league_id="20")
    with pytest.raises(ValueError, match="season_type"):
        nba_game_flow(2026, season_type="regular")
    with pytest.raises(KeyError, match="home_wp"):
        game_flow(nba_score_rows(_read("nba", "pbp"), _read("nba", "sched")), pl.DataFrame())
    with pytest.raises(ValueError, match="league"):
        attach_home_wp(pl.DataFrame(), league="nhl")
    empty = nba_score_rows(_read("nba", "pbp").head(0), _read("nba", "sched"))
    assert game_flow(attach_home_wp(empty, league="nba"), pl.DataFrame()).schema == pl.Schema(GAME_FLOW_SCHEMA)


def test_public_functions_read_the_releases(monkeypatch: pytest.MonkeyPatch) -> None:
    for module, lg, prefix in ((nba_loaders, "nba", "load_nba_stats"), (wnba_loaders, "wnba", "load_wnba_stats")):
        for name, kind in (("pbp", "pbp"), ("possessions", "poss"), ("schedules", "sched")):
            monkeypatch.setattr(module, f"{prefix}_{name}", lambda _seasons, _lg=lg, _k=kind: _read(_lg, _k))
    want = _flow("nba").filter(pl.col("game_id").str.starts_with("002"))
    assert nba_game_flow(2026).equals(want)
    assert nba_team_season_flow(2026).equals(team_season_flow(want))
    assert wnba_game_flow(2025).equals(_flow("wnba").filter(pl.col("game_id").str.starts_with("102")))
