"""Tests for BPM2_COEFFICIENTS constant and position/role estimators."""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest
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
    # BPM 2.0: the five %Min-weighted slots sum to the team's net rating per 100, i.e. the
    # minute-weighted team MEAN is margin / 5, for ANY coeffs (it was the full margin, 5x too large)
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
        assert abs(wbpm - margin / 5) < 1e-6


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
# Task 7 (WP4): granularity="game" — single-game BPM 2.0
# ---------------------------------------------------------------------------


def test_nba_bpm_game_granularity_one_row_per_game_player():
    player_logs, team_logs = _synth_logs()  # 20 games, 2 teams x 8 players
    positions = pl.DataFrame({"player_id": list(range(1, 17)), "position_num": [3.0] * 16})
    out = nba_bpm(player_logs, team_logs, positions, granularity="game")
    assert set(out.columns) == {"game_id", "player_id", "obpm", "dbpm", "bpm", "min", "gp"}
    assert out.height == 20 * 16
    assert (out["gp"] == 1).all()
    assert set(out["game_id"].unique().to_list()) == {f"G{gi}" for gi in range(20)}


def test_nba_bpm_game_granularity_per_game_team_adjustment_invariant():
    """Per game: minute-weighted team BPM == that game's own efficiency margin / 5
    (same invariant test_team_adjustment_invariant checks at season grain)."""
    player_logs, team_logs = _synth_logs()
    positions = pl.DataFrame({"player_id": list(range(1, 17)), "position_num": [3.0] * 16})
    out = nba_bpm(player_logs, team_logs, positions, granularity="game")
    for gi in range(20):
        gid = f"G{gi}"
        og = out.filter(pl.col("game_id") == gid).join(
            player_logs.filter(pl.col("game_id") == gid).select("player_id", "team_id"), on="player_id"
        )
        tlg = team_logs.filter(pl.col("game_id") == gid)
        for team in (1, 2):
            t = og.filter(pl.col("team_id") == team)
            wbpm = (t["bpm"] * t["min"]).sum() / t["min"].sum()
            tl = tlg.filter(pl.col("team_id") == team)
            poss = (tl["fga"] - tl["oreb"] + tl["tov"] + 0.44 * tl["fta"]).sum()
            margin = tl["plus_minus"].sum() / poss * 100
            assert abs(wbpm - margin / 5) < 1e-6, (gid, team)


def test_nba_bpm_season_mode_unchanged_by_refactor():
    """Regression guard for the _nba_bpm_one extraction: default (season) mode
    must produce the exact same columns/shape as before the granularity dispatcher."""
    player_logs, team_logs = _synth_logs()
    positions = pl.DataFrame({"player_id": list(range(1, 17)), "position_num": [3.0] * 16})
    out = nba_bpm(player_logs, team_logs, positions)  # default granularity="season"
    assert set(out.columns) == {"player_id", "obpm", "dbpm", "bpm", "min", "gp"}
    assert out.height == 16
    assert out["gp"].to_list() == [20] * 16


def test_nba_bpm_invalid_granularity_raises():
    player_logs, team_logs = _synth_logs()
    positions = pl.DataFrame({"player_id": list(range(1, 17)), "position_num": [3.0] * 16})
    with pytest.raises(ValueError, match="granularity"):
        nba_bpm(player_logs, team_logs, positions, granularity="quarter")


def test_nba_bpm_game_granularity_requires_game_id():
    player_logs, team_logs = _synth_logs()
    positions = pl.DataFrame({"player_id": list(range(1, 17)), "position_num": [3.0] * 16})
    with pytest.raises(ValueError, match="game_id"):
        nba_bpm(player_logs.drop("game_id"), team_logs, positions, granularity="game")


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


# ---------------------------------------------------------------------------
# Real season: the BPM 2.0 offense / defense split (tests/fixtures/nba_bpm/, README there)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def real_2025():
    import pathlib

    fx = pathlib.Path(__file__).resolve().parents[1] / "fixtures" / "nba_bpm"
    player = pl.read_parquet(fx / "player_logs_2025.parquet")
    team = pl.read_parquet(fx / "team_logs_2025.parquet")
    pos = pl.read_parquet(fx / "positions_2025.parquet")
    return player, team, pos, nba_bpm(player, team, pos)


def _wmean(df: pl.DataFrame, col: str, by: str | None = None):
    if by is None:
        return float((df[col] * df["min"]).sum() / df["min"].sum())
    return df.group_by(by).agg(((pl.col(col) * pl.col("min")).sum() / pl.col("min").sum()).alias(col))


def test_real_season_obpm_and_dbpm_each_centre_on_the_league(real_2025) -> None:
    """2024-25 NBA: minute-weighted obpm / dbpm / bpm read -0.00x each. The old adjustment put the whole
    team constant into obpm: -2.94 / +2.90 there (2025-26), 97% of players with a positive dbpm."""
    *_, out = real_2025
    assert abs(_wmean(out, "obpm")) < 0.05 and abs(_wmean(out, "dbpm")) < 0.05 and abs(_wmean(out, "bpm")) < 0.05
    assert (out["obpm"] + out["dbpm"] - out["bpm"]).abs().max() <= 1e-9
    assert 0.3 < float((out["dbpm"] > 0).mean()) < 0.6


def test_real_season_team_sums_hit_net_and_offensive_ratings(real_2025) -> None:
    """Per team, minute-weighted bpm = net rating / 5 and obpm = (ORtg - league ORtg) / 5 (Myers' %Min sums)."""
    player, team, _, out = real_2025
    poss = pl.col("fga") - pl.col("oreb") + pl.col("tov") + 0.44 * pl.col("fta")
    t = team.group_by("team_id").agg(poss.sum().alias("poss"), pl.col("pts").sum(), pl.col("plus_minus").sum())
    lg = float(t["pts"].sum() / t["poss"].sum() * 100)
    t = t.select("team_id", (pl.col("plus_minus") / pl.col("poss") * 20).alias("net5"),
                 ((pl.col("pts") / pl.col("poss") * 100 - lg) / 5).alias("o5"))  # fmt: skip
    first_team = player.group_by("player_id").agg(pl.col("team_id").first())
    j = out.join(first_team, on="player_id")
    w = _wmean(j, "bpm", "team_id").join(_wmean(j, "obpm", "team_id"), on="team_id").join(t, on="team_id")
    assert w.height == 30
    assert (w["bpm"] - w["net5"]).abs().max() < 1e-9
    assert (w["obpm"] - w["o5"]).abs().max() < 1e-9


def test_real_season_tracks_basketball_reference(real_2025) -> None:
    """Against Basketball-Reference's 2024-25 BPM (tests/fixtures/bb_player_war, 500+ minutes, matched by
    name): corr 0.945 and MAE 0.649 observed 2026-11-09; the old adjustment read 0.708 / 3.83."""
    import pathlib
    import unicodedata

    player, _, _, out = real_2025
    bref = pl.read_parquet(
        pathlib.Path(__file__).resolve().parents[1] / "fixtures" / "bb_player_war" / "bref_nba_advanced_2025.parquet"
    )
    names = pl.read_parquet(pathlib.Path(__file__).resolve().parents[1] / "fixtures" / "nba_bpm" / "names_2025.parquet")

    def norm(s: str) -> str:
        s = s.encode("latin-1", "ignore").decode("utf-8", "ignore")  # the capture's mojibake, as the README says
        return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower().replace(".", "").strip()

    bref = bref.with_columns(pl.col("player").map_elements(norm, return_dtype=pl.Utf8).alias("k"))
    bref = bref.sort("mp", descending=True).unique("k", keep="first")
    ours = out.join(names, on="player_id").with_columns(
        pl.col("player_name").map_elements(norm, return_dtype=pl.Utf8).alias("k")
    )
    j = ours.join(bref.select("k", pl.col("bpm").cast(pl.Float64).alias("bref")), on="k").filter(pl.col("min") >= 500)
    assert j.height >= 350
    assert float(np.corrcoef(j["bpm"], j["bref"])[0, 1]) >= 0.93
    assert float((j["bpm"] - j["bref"]).abs().mean()) <= 0.8


def test_real_season_star_splits(real_2025) -> None:
    """2024-25 (obpm / dbpm / bpm). Basketball-Reference has Jokic 9.0 / 4.3 / 13.3."""
    *_, out = real_2025
    got = {r["player_id"]: (r["obpm"], r["dbpm"], r["bpm"]) for r in out.iter_rows(named=True)}
    want = {203999: (8.97, 4.40, 13.38), 2544: (4.52, 1.81, 6.34), 1629008: (1.14, -2.08, -0.94)}
    for pid, vals in want.items():
        assert np.allclose(got[pid], vals, atol=0.01), (pid, got[pid])


def test_player_war_repair_is_a_no_op_on_the_fixed_bpm(real_2025) -> None:
    """player_war's #1114 repair (minute-weighted team bpm -> net / 5) leaves the fixed bpm where it is,
    given the same net rating, so the WAR built on a rebuilt release does not move under it."""
    from sportsdataverse._common.player_war import repair_bpm_team_constant

    player, team, _, out = real_2025
    poss = pl.col("fga") - pl.col("oreb") + pl.col("tov") + 0.44 * pl.col("fta")
    net = team.group_by("team_id").agg((pl.col("plus_minus").sum() / poss.sum() * 100).alias("net"))
    first_team = player.group_by("player_id").agg(pl.col("team_id").first())
    ratings = out.join(first_team, on="player_id").with_columns(pl.lit(2025, dtype=pl.Int64).alias("season"))
    net = net.with_columns(pl.lit(2025, dtype=pl.Int64).alias("season"))
    repaired = repair_bpm_team_constant(ratings, net)
    j = ratings.select("player_id", "bpm").join(repaired.select("player_id", pl.col("bpm").alias("r")), on="player_id")
    assert (j["bpm"] - j["r"]).abs().max() < 1e-9


def test_game_mode_takes_each_seasons_own_league_rating() -> None:
    """Scoring two seasons' games in one call gives each game the obpm it gets from its own season."""
    p1, t1 = _synth_logs(0)
    p2, t2 = _synth_logs(1)
    t2 = t2.with_columns(pl.col("pts") + 10.0)  # a higher-scoring league

    def tag(df: pl.DataFrame, season: str, prefix: str) -> pl.DataFrame:
        return df.with_columns(pl.lit(season).alias("season_id"), (pl.lit(prefix) + pl.col("game_id")).alias("game_id"))

    p1, t1, p2, t2 = tag(p1, "22024", "A"), tag(t1, "22024", "A"), tag(p2, "22025", "B"), tag(t2, "22025", "B")
    positions = pl.DataFrame({"player_id": list(range(1, 17)), "position_num": [3.0] * 16})
    both = nba_bpm(pl.concat([p1, p2]), pl.concat([t1, t2]), positions, granularity="game")
    alone = nba_bpm(p1, t1, positions, granularity="game")
    j = alone.join(both, on=["game_id", "player_id"], suffix="_both")
    assert j.height == alone.height and (j["obpm"] - j["obpm_both"]).abs().max() < 1e-9


def test_league_rating_refuses_incomplete_team_totals() -> None:
    player_logs, team_logs = _synth_logs()
    positions = pl.DataFrame({"player_id": list(range(1, 17)), "position_num": [3.0] * 16})
    broken = team_logs.with_columns(
        pl.when(pl.int_range(pl.len()) == 0).then(None).otherwise(pl.col("pts")).alias("pts")
    )
    with pytest.raises(ValueError, match="null"):
        nba_bpm(player_logs, broken, positions)


def test_league_rating_refuses_non_finite_team_totals() -> None:
    player_logs, team_logs = _synth_logs()
    positions = pl.DataFrame({"player_id": list(range(1, 17)), "position_num": [3.0] * 16})
    broken = team_logs.with_columns(
        pl.when(pl.int_range(pl.len()) == 0).then(float("nan")).otherwise(pl.col("pts")).alias("pts")
    )
    with pytest.raises(ValueError, match="non-finite"):
        nba_bpm(player_logs, broken, positions)


def test_one_game_scored_alone_matches_the_season_with_the_league_rating(real_2025) -> None:
    """A single game's logs carry no league: passing the season's league_ortg reproduces its game-mode values."""
    from sportsdataverse.nba.nba_bpm import _league_ortg

    player, team, pos, _ = real_2025
    gid = team["game_id"].sort()[0]
    season = nba_bpm(player, team, pos, granularity="game").filter(pl.col("game_id") == gid)
    lg = _league_ortg(team)
    one = nba_bpm(
        player.filter(pl.col("game_id") == gid),
        team.filter(pl.col("game_id") == gid),
        pos,
        granularity="game",
        league_ortg=lg,
    )
    j = season.join(one, on=["game_id", "player_id"], suffix="_one")
    assert j.height == season.height > 0 and (j["obpm"] - j["obpm_one"]).abs().max() < 1e-9
