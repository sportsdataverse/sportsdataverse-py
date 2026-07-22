"""Gates for the fully extended possession node tree, on real fixtures.

Every stochastic node is now fitted and state-conditional through the
models2shelf path: per-key pace, period x clock aux rates, shooter-
conditional free throws (opt-in), and the learned-keyer-composed grid —
with the empirical default path byte-identical (the rendered goldens and
calibration artifact remain the backstop). Thresholds pinned from
observed fixture values — never lower them.
"""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nba.nba_possession_sim import (
    PlayerAttribution,
    build_shelf,
    fit_learned_gamestate_keyer,
    models_to_shelf,
    player_game_logs_from_pbp,
    possessions_from_pbp,
    shelf_from_parquet,
    shelf_to_parquet,
    simulate_ensemble,
)
from sportsdataverse.nba.nba_possession_sim.attribution import simulate_player_boxscores
from sportsdataverse.nba.nba_possession_sim.nodes import FreeThrowNode
from tools.calibration import build as calibration_build


@pytest.fixture(scope="module")
def data():
    raw = calibration_build.fixture_raw()
    events = possessions_from_pbp(raw)
    logs = player_game_logs_from_pbp(raw)
    return raw, events, logs


def test_logs_carry_made_free_throws(data) -> None:
    _raw, _events, logs = data
    assert "ftm" in logs.columns
    assert logs.filter(pl.col("ftm") > pl.col("fta")).height == 0
    made, att = int(logs["ftm"].sum()), int(logs["fta"].sum())
    assert made == 109 and att == 146  # deterministic from the committed captures


def test_shooter_ft_rates_shrink_toward_league(data) -> None:
    _raw, _events, logs = data
    game = logs.filter(pl.col("game_id") == "0022300001")
    teams = sorted(int(t) for t in game["team_id"].drop_nulls().unique().to_list())
    att = PlayerAttribution.from_logs(logs, home_team_id=teams[0], away_team_id=teams[1], with_ft_pct=True)
    assert att.ft_pct and len(att.ft_pct) >= 50
    league = float(logs["ftm"].sum() / logs["fta"].sum())
    raw_rates = {
        int(r["player_id"]): (int(r["ftm"]), int(r["fta"]))
        for r in logs.group_by("player_id").agg(pl.col("ftm").sum(), pl.col("fta").sum()).iter_rows(named=True)
    }
    for pid, shrunk in att.ft_pct.items():
        made, attempts = raw_rates[pid]
        raw_rate = made / attempts if attempts else league
        low, high = sorted((raw_rate, league))
        assert low - 1e-9 <= shrunk <= high + 1e-9
        assert 0.0 < shrunk < 1.0
    # availability masks preserve the fitted rates
    masked = att.without(home_unavailable=[next(iter(att.home.player_ids))])
    assert masked.ft_pct == att.ft_pct
    with pytest.raises(ValueError, match="ftm"):
        PlayerAttribution.from_logs(logs.drop("ftm"), home_team_id=teams[0], away_team_id=teams[1], with_ft_pct=True)


def test_free_throw_node_shooter_override() -> None:
    class _StubShelf:
        ft_pct = 0.5

    rng = np.random.default_rng(7)
    node = FreeThrowNode()
    assert node.sample(_StubShelf(), 10, rng, p_make=1.0) == 10
    assert node.sample(_StubShelf(), 10, rng, p_make=0.0) == 0
    # default path: the shelf rate still drives the binomial
    draws = [node.sample(_StubShelf(), 100, np.random.default_rng(s)) for s in range(5)]
    assert all(30 <= d <= 70 for d in draws)


def test_fitted_pace_and_aux_nodes(data) -> None:
    raw, events, _logs = data
    shelf = models_to_shelf(events, actions=raw)
    assert len(shelf.outcome_pmfs) == 144
    assert shelf.pace_rates and len(shelf.pace_rates) >= 100  # observed 120
    assert shelf.aux_rates and len(shelf.aux_rates) == 144
    pooled = float(np.mean(list(shelf.pace_rates.values())))
    assert pooled == pytest.approx(shelf.mean_possession_seconds, abs=0.5)  # observed gap 0.01
    assert all(rate > 0 for rate in shelf.pace_rates.values())
    # the real end-of-period effect: clutch timeout rates exceed early ones
    clutch = np.mean([v["timeout_rate"] for k, v in shelf.aux_rates.items() if k.endswith("clutch")])
    early = np.mean([v["timeout_rate"] for k, v in shelf.aux_rates.items() if k.endswith("early")])
    assert clutch > early  # observed .0503 vs .0383
    for rates in shelf.aux_rates.values():
        for name, value in rates.items():
            assert 0.0 <= value <= 1.0, (name, value)
    # fallback chain: unknown key serves the global scalars
    assert shelf.pace_for("nope") == shelf.mean_possession_seconds
    ens1 = simulate_ensemble(shelf, n_sim=40, seed=7)
    ens2 = simulate_ensemble(shelf, n_sim=40, seed=7)
    assert np.array_equal(ens1["score_home"], ens2["score_home"])


def test_extended_shelf_parquet_round_trip(data) -> None:
    raw, events, _logs = data
    shelf = models_to_shelf(events, actions=raw)
    import pathlib
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        path = pathlib.Path(tmp) / "tree_shelf.parquet"
        shelf_to_parquet(shelf, path)
        back = shelf_from_parquet(path)
        assert back.pace_rates == shelf.pace_rates
        assert back.aux_rates == shelf.aux_rates
        assert back.outcome_pmfs == shelf.outcome_pmfs


def test_keyer_composed_models_shelf(data) -> None:
    raw, events, _logs = data
    keyer = fit_learned_gamestate_keyer(events)
    shelf = models_to_shelf(events, keyer=keyer, actions=raw)
    assert shelf.keyer is keyer
    assert len(shelf.outcome_pmfs) == shelf.keyer.leaf_table.height
    assert shelf.pace_rates and set(shelf.pace_rates) <= set(shelf.outcome_pmfs)
    assert shelf.aux_rates and set(shelf.aux_rates) == set(shelf.outcome_pmfs)
    simulate_ensemble(shelf, n_sim=40, seed=7)
    assert shelf.fallback_rate() == 0.0  # every learned key is baked


def test_boxscore_with_shooter_ft_conserves_exactly(data) -> None:
    raw, events, logs = data
    shelf = models_to_shelf(events, actions=raw)
    game = logs.filter(pl.col("game_id") == "0022300001")
    teams = sorted(int(t) for t in game["team_id"].drop_nulls().unique().to_list())
    att = PlayerAttribution.from_logs(logs, home_team_id=teams[0], away_team_id=teams[1], with_ft_pct=True)
    box = simulate_player_boxscores(shelf, att, n_sim=80, seed=7)
    total = box["score_home"] + box["score_away"]
    player_sum = np.sum([vec for vec in box["pts"].values()], axis=0)
    assert np.array_equal(player_sum, total)  # shooter-conditional FT keeps exact conservation
    again = simulate_player_boxscores(shelf, att, n_sim=80, seed=7)
    assert all(np.array_equal(box["pts"][p], again["pts"][p]) for p in box["pts"])


def test_empirical_default_path_untouched(data) -> None:
    """The byte-identity backstop: empirical shelves carry no fitted nodes
    and every fallback resolves to the exact global scalars."""
    _raw, events, _logs = data
    shelf = build_shelf(events)
    assert shelf.pace_rates is None and shelf.aux_rates is None
    shelf.aux = {"timeout_rate": 0.04}
    assert shelf.aux_for("d0|p1|early") == {"timeout_rate": 0.04}
    assert shelf.pace_for("d0|p1|early") == shelf.mean_possession_seconds
