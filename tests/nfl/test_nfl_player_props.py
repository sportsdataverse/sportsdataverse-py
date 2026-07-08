"""Unit tests for the NFL empirical-Bayes player-prop projections (Phase 3)."""

import polars as pl

from sportsdataverse.nfl.nfl_player_props import player_usage_efficiency
from sportsdataverse.nfl.nfl_prediction_constants import get_prop_constants


def _player_stats():
    rows = []
    # Veteran QB: 5 games at 40 att / 300 yds / 2 td.
    for w in range(1, 6):
        rows.append(
            {
                "player_id": "vet",
                "player_display_name": "Vet QB",
                "position": "QB",
                "recent_team": "KC",
                "opponent_team": "LV",
                "season": 2023,
                "week": w,
                "attempts": 40.0,
                "completions": 28.0,
                "passing_yards": 300.0,
                "passing_tds": 2.0,
                "carries": 3.0,
                "rushing_yards": 12.0,
                "rushing_tds": 0.0,
                "targets": 0.0,
                "receptions": 0.0,
                "receiving_yards": 0.0,
                "receiving_tds": 0.0,
            }
        )
    # Rookie QB: 1 game at 20 att.
    rows.append(
        {
            "player_id": "rook",
            "player_display_name": "Rook QB",
            "position": "QB",
            "recent_team": "CHI",
            "opponent_team": "GB",
            "season": 2023,
            "week": 5,
            "attempts": 20.0,
            "completions": 12.0,
            "passing_yards": 150.0,
            "passing_tds": 1.0,
            "carries": 2.0,
            "rushing_yards": 8.0,
            "rushing_tds": 0.0,
            "targets": 0.0,
            "receptions": 0.0,
            "receiving_yards": 0.0,
            "receiving_tds": 0.0,
        }
    )
    # A week-6 game that MUST be excluded by as_of_week=6.
    rows.append(
        {
            "player_id": "vet",
            "player_display_name": "Vet QB",
            "position": "QB",
            "recent_team": "KC",
            "opponent_team": "DEN",
            "season": 2023,
            "week": 6,
            "attempts": 99.0,
            "completions": 70.0,
            "passing_yards": 999.0,
            "passing_tds": 9.0,
            "carries": 0.0,
            "rushing_yards": 0.0,
            "rushing_tds": 0.0,
            "targets": 0.0,
            "receptions": 0.0,
            "receiving_yards": 0.0,
            "receiving_tds": 0.0,
        }
    )
    return pl.DataFrame(rows)


def test_usage_eb_shrinkage_and_as_of_filter():
    cfg = get_prop_constants("modern")
    kappa = cfg.shrink_pass
    prior_att = cfg.pos_priors["QB"]["attempts"]

    out = player_usage_efficiency(_player_stats(), as_of_week=6)
    assert out.schema["player_id"] == pl.Utf8
    assert out.schema["team_id"] == pl.Utf8
    vet = out.filter(pl.col("player_id") == "vet").row(0, named=True)
    rook = out.filter(pl.col("player_id") == "rook").row(0, named=True)

    # Hand-check the EB values: (n*mean + kappa*prior) / (n + kappa).
    assert abs(vet["exp_attempts"] - (5 * 40.0 + kappa * prior_att) / (5 + kappa)) < 1e-9
    assert abs(rook["exp_attempts"] - (1 * 20.0 + kappa * prior_att) / (1 + kappa)) < 1e-9

    # Veteran stays near its raw mean; the 1-game rookie is pulled to the prior.
    assert abs(vet["exp_attempts"] - 40.0) < abs(40.0 - prior_att)
    rook_pull = abs(rook["exp_attempts"] - 20.0)
    vet_pull = abs(vet["exp_attempts"] - 40.0)
    assert rook_pull > vet_pull

    # The 99-attempt week-6 line was excluded (as-of) -- vet mean stayed 40.
    assert vet["exp_attempts"] < 45.0


def test_usage_empty_input_returns_typed_zero_row():
    out = player_usage_efficiency(_player_stats().head(0), as_of_week=6)
    assert out.height == 0
    assert out.schema["ypa"] == pl.Float64
