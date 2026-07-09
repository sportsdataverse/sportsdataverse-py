"""Unit tests for the NFL empirical-Bayes player-prop projections (Phase 3)."""

import datetime as dt
import importlib

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


PROPS_COLS = [
    "season",
    "week",
    "game_id",
    "player_id",
    "position",
    "team_id",
    "opp_team_id",
    "stat",
    "proj_mean",
    "proj_sd",
    "line",
    "p_over",
]


def _assembly_fixtures(monkeypatch, *, weak_def_margin=0.0):
    """Monkeypatch loaders + ratings + predictions to tiny deterministic frames.

    Teams: KC (WR1's team) plays LV (weak pass defense, adj_def +0.10) in g1;
    CHI (WR2's team) plays SF (strong defense, adj_def -0.10) in g2. RB1 on a
    favored team (KC), RB2 on an underdog (CHI).
    """
    mod = importlib.import_module("sportsdataverse.nfl.nfl_player_props")

    stats_rows = []
    for w in range(1, 6):
        for pid, pos, team, tgt, rec_yds, car, rush_yds in (
            ("wr1", "WR", "KC", 8.0, 80.0, 0.0, 0.0),
            ("wr2", "WR", "CHI", 8.0, 80.0, 0.0, 0.0),
            ("rb1", "RB", "KC", 2.0, 10.0, 15.0, 60.0),
            ("rb2", "RB", "CHI", 2.0, 10.0, 15.0, 60.0),
        ):
            stats_rows.append(
                {
                    "player_id": pid,
                    "player_display_name": pid,
                    "position": pos,
                    "recent_team": team,
                    "opponent_team": "X",
                    "season": 2023,
                    "week": w,
                    "attempts": 0.0,
                    "completions": 0.0,
                    "passing_yards": 0.0,
                    "passing_tds": 0.0,
                    "carries": car,
                    "rushing_yards": rush_yds,
                    "rushing_tds": 0.0,
                    "targets": tgt,
                    "receptions": tgt * 0.7,
                    "receiving_yards": rec_yds,
                    "receiving_tds": 0.0,
                }
            )
    stats = pl.DataFrame(stats_rows)
    schedule = pl.DataFrame(
        {
            "game_id": ["g1", "g2"],
            "season": [2023, 2023],
            "week": [6, 6],
            "gameday": [dt.date(2023, 10, 15), dt.date(2023, 10, 15)],
            "home_team": ["KC", "CHI"],
            "away_team": ["LV", "SF"],
            "location": ["Home", "Home"],
        }
    )
    ratings = pl.DataFrame(
        {
            "team_id": ["KC", "LV", "CHI", "SF"],
            "adj_off_epa": [0.10, -0.05, 0.00, 0.05],
            "adj_def_epa": [0.00, 0.10, 0.00, -0.10],  # LV weak, SF strong
            "adj_net": [0.10, -0.15, 0.00, 0.15],
        }
    )
    monkeypatch.setattr(mod, "load_nfl_player_stats", lambda **kw: stats)
    monkeypatch.setattr(mod, "load_nfl_schedule", lambda seasons, **kw: schedule)
    monkeypatch.setattr(mod, "nfl_ratings", lambda seasons, **kw: ratings)
    return mod


def test_props_matchup_and_game_script(monkeypatch):
    mod = _assembly_fixtures(monkeypatch)
    out = mod.nfl_player_props(2023)
    assert out.columns == PROPS_COLS
    assert out.schema["player_id"] == pl.Utf8
    assert out.schema["team_id"] == pl.Utf8

    rec = {r["player_id"]: r for r in out.filter(pl.col("stat") == "receiving_yards").to_dicts()}
    # Identical WR usage: the one facing the WEAK pass defense (LV, adj_def
    # +0.10) projects higher than the one facing the strong one (SF, -0.10).
    assert rec["wr1"]["proj_mean"] > rec["wr2"]["proj_mean"]

    rush = {r["player_id"]: r for r in out.filter(pl.col("stat") == "rushing_yards").to_dicts()}
    # Game script: identical RB usage, but RB1's team is favored (KC net 0.10
    # vs LV -0.15) while RB2's is an underdog (CHI 0.00 vs SF 0.15) -- and
    # RB1 also faces the weaker defense, both effects push the same way.
    assert rush["rb1"]["proj_mean"] > rush["rb2"]["proj_mean"]

    # No line supplied -> line/p_over are null.
    assert out["line"].null_count() == out.height
    assert out["p_over"].null_count() == out.height


def test_props_line_join_and_pandas(monkeypatch):
    mod = _assembly_fixtures(monkeypatch)
    lines = pl.DataFrame({"game_id": ["g1"], "player_id": ["wr1"], "stat": ["receiving_yards"], "line": [60.5]})
    out = mod.nfl_player_props(2023, lines=lines)
    wr1 = out.filter((pl.col("player_id") == "wr1") & (pl.col("stat") == "receiving_yards")).row(0, named=True)
    assert wr1["line"] == 60.5
    assert 0.0 < wr1["p_over"] < 1.0
    pdf = mod.nfl_player_props(2023, return_as_pandas=True)
    assert not isinstance(pdf, pl.DataFrame)
    assert list(pdf.columns) == PROPS_COLS


def test_props_null_projection_yields_null_p_over_not_nan(monkeypatch):
    import math

    mod = _assembly_fixtures(monkeypatch)
    # drop CHI/SF from ratings: wr2/rb2's game loses its script -> proj_mean null;
    # a supplied line must then yield a NULL p_over, never NaN through norm.cdf
    ratings = pl.DataFrame(
        {
            "team_id": ["KC", "LV"],
            "adj_off_epa": [0.10, -0.05],
            "adj_def_epa": [0.00, 0.10],
            "adj_net": [0.10, -0.15],
        }
    )
    monkeypatch.setattr(mod, "nfl_ratings", lambda seasons, **kw: ratings)
    lines = pl.DataFrame({"game_id": ["g2"], "player_id": ["wr2"], "stat": ["receiving_yards"], "line": [60.5]})
    out = mod.nfl_player_props(2023, lines=lines)
    wr2 = out.filter((pl.col("player_id") == "wr2") & (pl.col("stat") == "receiving_yards")).row(0, named=True)
    assert wr2["line"] == 60.5
    assert wr2["p_over"] is None or not math.isnan(wr2["p_over"])
    assert wr2["p_over"] is None  # null, not NaN
