"""Tests for BPM2_COEFFICIENTS constant and position/role estimators."""

from __future__ import annotations

import numpy as np
import polars as pl
from sportsdataverse.nba.nba_bpm import (
    BPM2_COEFFICIENTS,
    NbaBpmModel,
    _estimate_position,
    _estimate_role,
    _interp,
    _raw_bpm,
    _recursive_team_center,
    nba_bpm,
)


def test_position_regression_team_sums_to_three_and_clamps() -> None:
    # 5 players, one team, arbitrary team-stat shares summing to 100% each
    shares = pl.DataFrame(
        {
            "player_id": [1, 2, 3, 4, 5],
            "team_id": [10] * 5,
            "min": [200.0] * 5,
            "pct_trb": [0.10, 0.15, 0.20, 0.25, 0.30],
            "pct_stl": [0.2] * 5,
            "pct_pf": [0.2] * 5,
            "pct_ast": [0.4, 0.3, 0.15, 0.1, 0.05],
            "pct_blk": [0.2] * 5,
        }
    )
    listed = pl.DataFrame({"player_id": [1, 2, 3, 4, 5], "position_num": [1.0, 2.0, 3.0, 4.0, 5.0]})
    pos = _estimate_position(shares, listed)
    # minute-weighted team mean position == 3.0 (the recursive constraint)
    m = (
        pos.join(shares.select(["player_id", "min"]), on="player_id")
        .select((pl.col("position_num") * pl.col("min")).sum() / pl.col("min").sum())
        .item()
    )
    assert abs(m - 3.0) < 1e-6
    assert pos["position_num"].min() >= 1.0 and pos["position_num"].max() <= 5.0


def test_role_regression_clamps_1_5() -> None:
    shares = pl.DataFrame(
        {
            "player_id": [1, 2],
            "team_id": [10, 10],
            "min": [200.0, 200.0],
            "pct_ast": [0.05, 0.40],
            "pct_threshold_pts": [0.30, 0.02],
        }
    )
    role = _estimate_role(shares)
    assert role["role_num"].min() >= 1.0 and role["role_num"].max() <= 5.0


def test_coefficients_constant_has_lebron_anchor_values() -> None:
    base = BPM2_COEFFICIENTS["base"]
    assert base["pts"] == (0.860, 0.860)  # (pos1, pos5)
    assert base["ast"] == (0.580, 1.034)
    assert base["fga_role"] == (-0.560, -0.780)  # (role1 creator, role5 receiver)
    off = BPM2_COEFFICIENTS["offense"]
    assert off["pts"] == (0.605, 0.605)
    assert off["blk"] == (0.725, 0.097)


def test_recursive_team_center_converges_with_clamping() -> None:
    # raw positions span outside [1,5] so clamping engages, but a mean-3 solution exists
    df = pl.DataFrame(
        {
            "player_id": [1, 2, 3, 4, 5],
            "team_id": [7] * 5,
            "min": [240.0] * 5,
            "raw": [0.2, 1.0, 3.0, 5.0, 6.5],  # 0.2 and 6.5 will clamp
        }
    )
    out = _recursive_team_center(df, "raw", "position_num", target=3.0)
    m = (
        out.join(df.select(["player_id", "min"]), on="player_id")
        .select((pl.col("position_num") * pl.col("min")).sum() / pl.col("min").sum())
        .item()
    )
    assert abs(m - 3.0) < 1e-6
    assert out["position_num"].min() >= 1.0 and out["position_num"].max() <= 5.0


def test_interp_endpoints_and_midpoint() -> None:
    # at scale=1 should return lo; at scale=5 should return hi; at scale=3 midpoint
    assert _interp((0.860, 0.860), 1.0) == 0.860
    assert _interp((0.860, 0.860), 5.0) == 0.860
    assert abs(_interp((0.580, 1.034), 1.0) - 0.580) < 1e-9
    assert abs(_interp((0.580, 1.034), 5.0) - 1.034) < 1e-9
    assert abs(_interp((0.580, 1.034), 3.0) - (0.580 + 1.034) / 2) < 1e-9


def test_raw_bpm_reproduces_bref_lebron_2017() -> None:
    # B-Ref worked example, per-100 (pts already shooting-context-adjusted 34.9 -> 30.4)
    feats = pl.DataFrame(
        {
            "player_id": [23],
            "pts": [30.4],
            "fg3m": [2.2],
            "ast": [11.5],
            "tov": [5.4],
            "orb": [1.7],
            "drb": [9.7],
            "stl": [1.6],
            "blk": [0.8],
            "pf": [2.4],
            "fga": [24.0],
            "fta": [9.5],
        }
    )
    positions = pl.DataFrame({"player_id": [23], "position_num": [2.30]})
    roles = pl.DataFrame({"player_id": [23], "role_num": [1.0]})
    out = _raw_bpm(feats, positions, roles)
    assert out.schema["raw_bpm"] == pl.Float64
    assert out.schema["raw_obpm"] == pl.Float64
    assert abs(out["raw_bpm"][0] - 18.7) < 0.3  # published raw total 18.7


def test_raw_bpm_missing_position_defaults_neutral_not_dropped():
    feats = pl.DataFrame(
        {
            "player_id": [1, 2],
            "pts": [20.0, 15.0],
            "fg3m": [1.0, 1.0],
            "ast": [4.0, 3.0],
            "tov": [2.0, 2.0],
            "orb": [1.0, 1.0],
            "drb": [4.0, 4.0],
            "stl": [1.0, 1.0],
            "blk": [0.5, 0.5],
            "pf": [2.0, 2.0],
            "fga": [12.0, 10.0],
            "fta": [3.0, 2.0],
        }
    )
    # player 2 is missing from positions AND roles -> must still appear (neutral 3.0), not dropped
    positions = pl.DataFrame({"player_id": [1], "position_num": [2.0]})
    roles = pl.DataFrame({"player_id": [1], "role_num": [1.0]})
    out = _raw_bpm(feats, positions, roles)
    assert set(out["player_id"].to_list()) == {1, 2}  # player 2 NOT dropped
    assert out.filter(pl.col("player_id") == 2)["raw_bpm"].item() is not None


# ---------------------------------------------------------------------------
# Task 4: team adjustment + shooting-context + nba_bpm public function
# ---------------------------------------------------------------------------


def _synth_logs(seed: int = 0) -> tuple[pl.DataFrame, pl.DataFrame]:
    rng = np.random.default_rng(seed)
    rows_p, rows_t = [], []
    for gi in range(20):
        for team, pids in ((1, range(1, 9)), (2, range(9, 17))):
            for p in pids:
                rows_p.append(
                    {
                        "game_id": f"G{gi}",
                        "team_id": team,
                        "player_id": p,
                        "min": 30.0,
                        "pts": float(rng.integers(0, 30)),
                        "fg3m": 1.0,
                        "fga": 12.0,
                        "fta": 3.0,
                        "ast": 4.0,
                        "oreb": 1.0,
                        "dreb": 4.0,
                        "reb": 5.0,
                        "stl": 1.0,
                        "blk": 0.5,
                        "tov": 2.0,
                        "pf": 2.0,
                    }
                )
        for team, pm in ((1, 5.0), (2, -5.0)):
            rows_t.append(
                {
                    "game_id": f"G{gi}",
                    "team_id": team,
                    "min": 240.0,
                    "fga": 88.0,
                    "oreb": 10.0,
                    "dreb": 34.0,
                    "reb": 44.0,
                    "tov": 13.0,
                    "fta": 22.0,
                    "ast": 24.0,
                    "stl": 7.0,
                    "blk": 5.0,
                    "pf": 20.0,
                    "pts": 112.0,
                    "plus_minus": pm,
                }
            )
    return pl.DataFrame(rows_p), pl.DataFrame(rows_t)


def test_team_adjustment_invariant() -> None:
    player_logs, team_logs = _synth_logs()
    positions = pl.DataFrame({"player_id": list(range(1, 17)), "position_num": [3.0] * 16})
    out = nba_bpm(player_logs, team_logs, positions, team_adjust=True)
    # minute-weighted team BPM == team efficiency margin (plus_minus per 100), for ANY coeffs
    # team 1 margin = 5 pm/game over its possessions; compute the same way nba_bpm does and compare
    merged = out.join(
        player_logs.group_by("player_id").agg(pl.col("team_id").first(), pl.col("min").sum().alias("mins")),
        on="player_id",
    )
    for team in (1, 2):
        t = merged.filter(pl.col("team_id") == team)
        wbpm = (t["bpm"] * t["mins"]).sum() / t["mins"].sum()
        # expected margin recomputed from team_logs
        tl = team_logs.filter(pl.col("team_id") == team)
        poss = (tl["fga"] - tl["oreb"] + tl["tov"] + 0.44 * tl["fta"]).sum()
        margin = tl["plus_minus"].sum() / poss * 100
        assert abs(wbpm - margin) < 1e-6


def test_team_adjust_false_is_raw() -> None:
    player_logs, team_logs = _synth_logs()
    positions = pl.DataFrame({"player_id": list(range(1, 17)), "position_num": [3.0] * 16})
    adj = nba_bpm(player_logs, team_logs, positions, team_adjust=True)
    raw = nba_bpm(player_logs, team_logs, positions, team_adjust=False)
    # the two differ by a per-team constant (raw is not centered on the margin)
    assert not np.allclose(adj["bpm"].to_numpy(), raw["bpm"].to_numpy())


# ---------------------------------------------------------------------------
# Task 5: NbaBpmModel adapter + RAPM/SPM/BPM 3-way head-to-head
# ---------------------------------------------------------------------------

from sportsdataverse.nba.nba_model_validation import validate_model, _synthetic_possessions


def _bpm_setup() -> tuple:
    """Build aligned synthetic possessions + box logs for NbaBpmModel tests.

    Player IDs 1-16 (teams A=100: 1-8, B=200: 9-16) are used in possessions.
    Box logs use the same player ids and the same game_ids as the possession frame.
    """
    ids = list(range(1, 17))
    o = {p: 0.02 for p in ids}
    d = {p: 0.01 for p in ids}
    poss = _synthetic_possessions(o, d, n_games=20, poss_per_game=40, noise_sd=0.3, seed=1)
    game_ids = poss["game_id"].unique().to_list()
    # build player/team box logs aligned to poss game_ids; two teams, players 1-8 / 9-16
    rows_p, rows_t = [], []
    rng = np.random.default_rng(0)
    for gi in game_ids:
        for team, pids in ((1, range(1, 9)), (2, range(9, 17))):
            for p in pids:
                rows_p.append(
                    {
                        "game_id": gi,
                        "team_id": team,
                        "player_id": p,
                        "min": 30.0,
                        "pts": float(rng.integers(0, 30)),
                        "fg3m": 1.0,
                        "fga": 12.0,
                        "fta": 3.0,
                        "ast": 4.0,
                        "oreb": 1.0,
                        "dreb": 4.0,
                        "reb": 5.0,
                        "stl": 1.0,
                        "blk": 0.5,
                        "tov": 2.0,
                        "pf": 2.0,
                    }
                )
        for team, pm in ((1, 5.0), (2, -5.0)):
            rows_t.append(
                {
                    "game_id": gi,
                    "team_id": team,
                    "min": 240.0,
                    "fga": 88.0,
                    "oreb": 10.0,
                    "dreb": 34.0,
                    "reb": 44.0,
                    "tov": 13.0,
                    "fta": 22.0,
                    "ast": 24.0,
                    "stl": 7.0,
                    "blk": 5.0,
                    "pf": 20.0,
                    "pts": 112.0,
                    "plus_minus": pm,
                }
            )
    player_logs = pl.DataFrame(rows_p)
    team_logs = pl.DataFrame(rows_t)
    return ids, poss, game_ids, player_logs, team_logs


def test_nba_bpm_model_head_to_head() -> None:
    ids, poss, game_ids, player_logs, team_logs = _bpm_setup()
    positions = pl.DataFrame({"player_id": ids, "position_num": [3.0] * len(ids)})
    model = NbaBpmModel(player_logs, team_logs, positions)
    # fold restriction: fit_ratings on the full poss and on a 2-game subset both yield ratings
    rf_all = model.fit_ratings(poss)
    rf_two = model.fit_ratings(poss.filter(pl.col("game_id").is_in(game_ids[:2])))
    assert set(rf_all.o_ratings)  # produces ratings on full set
    assert set(rf_two.o_ratings)  # produces ratings on 2-game fold
    assert set(rf_two.o_ratings) == set(rf_all.o_ratings)  # same players
    assert rf_two.o_ratings != rf_all.o_ratings  # but different values -> fold actually restricted the input
    # head-to-head: BPM runs through the SAME validate_model as RAPM
    rep = validate_model(model, [poss], model_name="bpm", oracles=("retrodiction",))
    assert rep.retrodiction is not None
    assert rep.retrodiction.n_test_games > 0


# ---------------------------------------------------------------------------
# Task 6: gated live smoke test
# ---------------------------------------------------------------------------

from tests.conftest import skip_if_no_nba_stats_live


@skip_if_no_nba_stats_live
def test_nba_bpm_live_season_smoke() -> None:
    from sportsdataverse.nba import nba_bpm, nba_box_logs, nba_player_positions

    logs = nba_box_logs("2023-24")
    pos = nba_player_positions("2023-24")
    out = nba_bpm(logs["player"], logs["team"], pos)
    assert out.height > 0 and set(out.columns) == {"player_id", "obpm", "dbpm", "bpm", "min", "gp"}
