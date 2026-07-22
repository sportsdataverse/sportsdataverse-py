"""Cross-league basketball sim gates — one engine, five leagues, real games.

Every league's classifier passes the conservation oracle (classified event
points reconstruct the real final exactly), and every league's sim emits a
full pbp log with the right clock structure and event distributions within
reason of the real game's (v1 bands: single-game PMFs; multi-game shelves
tighten them).
"""

from __future__ import annotations

import json
import pathlib

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nba.nba_possession_sim import (
    OUTCOMES,
    RULES_BY_LEAGUE,
    build_shelf,
    possessions_from_pbp,
    simulate_ensemble,
    simulate_game_pbp,
)
from sportsdataverse.nba.nba_possession_sim.espn_adapter import (
    espn_final_total,
    espn_summary_to_events,
)

V3_GAMES = {
    "nba": ("tests/fixtures/nba_engine", ("0022100001", "0022200001", "0022300001")),
    "nbagl": ("tests/fixtures/nbagl_engine", ("2022400003", "2022400009")),
}
ESPN_LEAGUES = ("wnba", "mbb", "wbb")


def _events_for(league: str) -> "tuple[pl.DataFrame, int]":
    """(events, real_final_total) for a league from its committed real games."""
    if league in V3_GAMES:
        root, gids = V3_GAMES[league]
        frames = []
        total = 0
        for gid in gids:
            payload = json.loads(pathlib.Path(f"{root}/{gid}/playbyplayv3.json").read_text(encoding="utf-8"))
            acts = payload.get("game", {}).get("actions") or payload["actions"]
            frames.append(pl.DataFrame(acts, infer_schema_length=None).with_columns(pl.lit(gid).alias("game_id")))
            scored = [
                (int(a.get("scoreHome") or 0) + int(a.get("scoreAway") or 0))
                for a in acts
                if str(a.get("scoreHome") or "") != ""
            ]
            total += max(scored)
        return possessions_from_pbp(pl.concat(frames, how="diagonal_relaxed")), total
    summary = json.loads(pathlib.Path(f"tests/fixtures/espn/summary_{league}.json").read_text(encoding="utf-8"))
    return espn_summary_to_events(summary), espn_final_total(summary)


ALL_LEAGUES = [*V3_GAMES, *ESPN_LEAGUES]


@pytest.mark.parametrize("league", ALL_LEAGUES)
def test_conservation_every_league(league: str) -> None:
    """Classified event points reconstruct the league's real finals exactly."""
    events, real_total = _events_for(league)
    assert int(events["points"].sum()) == real_total


@pytest.mark.parametrize("league", ALL_LEAGUES)
def test_full_pbp_sim_structure_and_distribution(league: str) -> None:
    events, real_total = _events_for(league)
    n_games = events["game_id"].n_unique()
    shelf = build_shelf(events)
    rules = RULES_BY_LEAGUE[league]

    # full pbp emission with the league's clock structure
    final, pbp = simulate_game_pbp(shelf, np.random.default_rng(11), rules=rules)
    assert len(pbp) > 50
    assert final.score_home != final.score_away or final.period > rules.periods
    assert all(0 <= row["clock_seconds"] <= rules.period_seconds for row in pbp)
    reg_rows = [row for row in pbp if row["period"] <= rules.periods]
    assert {row["period"] for row in reg_rows} == set(range(1, rules.periods + 1))
    # running score in the log lands in a plausible range for ONE game
    assert pbp[-1]["score_home"] + pbp[-1]["score_away"] >= (real_total / n_games) * 0.5

    # distribution gates vs the real game(s)
    ens = simulate_ensemble(shelf, n_sim=120, seed=7, rules=rules, collect_event_counts=True)
    real_outcomes = events.filter(pl.col("kind") == "outcome")
    real_per_game = real_outcomes.height / n_games
    real_total_per_game = real_total / n_games

    # scoring within reason of the real per-game total (v1 single-game PMFs)
    assert ens["mean_total"] == pytest.approx(real_total_per_game, rel=0.25)

    counts = ens["event_counts"]
    assert counts is not None
    sim_events_per_game = float(sum(counts[o].mean() for o in OUTCOMES))
    # pace: simulated event volume within reason of the real game's
    assert sim_events_per_game == pytest.approx(real_per_game, rel=0.30)

    # event-type mix: sim shares track the real shares
    real_shares = {
        row["outcome"]: row["n"] / real_outcomes.height
        for row in real_outcomes.group_by("outcome").agg(pl.len().alias("n")).to_dicts()
    }
    for outcome in OUTCOMES:
        sim_share = float(counts[outcome].mean()) / sim_events_per_game
        assert abs(sim_share - real_shares.get(outcome, 0.0)) < 0.08, outcome


@pytest.mark.parametrize(
    "league, module, prefix",
    [
        ("wnba", "sportsdataverse.wnba.wnba_possession_sim", "wnba"),
        ("mbb", "sportsdataverse.mbb.mbb_possession_sim", "mbb"),
        ("wbb", "sportsdataverse.wbb.wbb_possession_sim", "wbb"),
    ],
)
def test_league_shims_run_end_to_end(league: str, module: str, prefix: str) -> None:
    import importlib

    mod = importlib.import_module(module)
    summary = json.loads(pathlib.Path(f"tests/fixtures/espn/summary_{league}.json").read_text(encoding="utf-8"))
    shelf = getattr(mod, f"{prefix}_shelf_from_espn_summary")(summary)
    ens = getattr(mod, f"{prefix}_simulate_ensemble")(shelf, n_sim=60, seed=3)
    assert 0.0 <= ens["win_prob_home"] <= 1.0
    final, pbp = getattr(mod, f"{prefix}_simulate_game_pbp")(shelf, np.random.default_rng(5))
    assert len(pbp) > 50
    assert mod.RULES.league == league
