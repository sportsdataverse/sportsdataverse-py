"""Tests for nba_rapm_variants (WP2: LA / four-factor / decay RAPM)."""

from __future__ import annotations

import datetime

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_model_validation import _synthetic_possessions
from sportsdataverse.nba.nba_rapm import nba_rapm
from sportsdataverse.nba.nba_rapm_variants import (
    DECAY_RAPM_SCHEMA,
    FOUR_FACTOR_SCHEMA,
    LA_RAPM_SCHEMA,
    ORACLE_RAPM_CV,
    ORACLE_RAPM_LAMBDAS,
    _fit_weighted,
    _prepare,
    _shrunk_shooter_rates,
    decay_weights,
    luck_adjusted_response,
    nba_decay_rapm,
    nba_four_factor_rapm,
    nba_la_rapm,
    oracle_rapm_alphas,
)


def _same_schedule_reference(poss: pl.DataFrame, response_col: str) -> pl.DataFrame:
    """Internal oracle-schedule reference fit: ``_fit_weighted`` at ``(oracle_rapm_alphas, ORACLE_RAPM_CV)``.

    Used by the WP2 variants' reduces-to-plain gates instead of the public
    ``nba_rapm`` (which fits at the plain ``DEFAULT_RAPM_ALPHAS``/``cv=None``
    schedule) -- comparing against ``nba_rapm`` directly would be a
    cross-schedule comparison per the controller ruling extending decision #8.
    """
    X, y, _w, pids = _prepare(poss, response_col, weight_col=None)
    o, d, _off_poss, _def_poss = _fit_weighted(X, y, alphas=oracle_rapm_alphas(X.shape[0]), cv=ORACLE_RAPM_CV)
    return pl.DataFrame({"player_id": pl.Series(pids, dtype=pl.Int64), "rapm": pl.Series(o + d, dtype=pl.Float64)})


def _with_possession_number(poss: pl.DataFrame) -> pl.DataFrame:
    """Assign a within-game ``possession_number`` (resets per ``game_id``, like real data).

    ``_synthetic_possessions`` doesn't emit this column; luck-adjusted-response
    tests need it as a join key alongside ``game_id`` -- using a per-game index
    (values repeat across games) keeps the join a real two-column join instead
    of accidentally being unique on ``possession_number`` alone.
    """
    return poss.with_columns(pl.int_range(pl.len()).over("game_id").cast(pl.Int64).alias("possession_number"))


def _shooting_for(poss: pl.DataFrame) -> pl.DataFrame:
    """Minimal per-shooter frame consistent with a possessions frame's team fg2m/fg3m/ftm."""
    rows = []
    for r in poss.iter_rows(named=True):
        # attribute all of the possession's makes to its first offense player (synthetic)
        pid = r["off_player_1"]
        rows.append(
            {
                "game_id": r["game_id"],
                "possession_number": r["possession_number"],
                "player_id": pid,
                "team_id": r["offense_team_id"],
                "fg2a": r["fg2m"],
                "fg2m": r["fg2m"],
                "fg3a": r["fg3m"],
                "fg3m": r["fg3m"],
                "fta": r["ftm"],
                "ftm": r["ftm"],
            }
        )
    return pl.DataFrame(rows)


def test_shrinkage_zero_attempts_gets_league_mean():
    sh = pl.DataFrame(
        {
            "game_id": ["g"],
            "possession_number": [1],
            "player_id": [7],
            "team_id": [100],
            "fg2a": [0],
            "fg2m": [0],
            "fg3a": [0],
            "fg3m": [0],
            "fta": [0],
            "ftm": [0],
        }
    )
    rates = _shrunk_shooter_rates(sh)
    # no attempts anywhere -> league mean is 0/0 guarded to 0.0, rate == that mean
    assert rates.filter(pl.col("player_id") == 7)["p3"][0] == 0.0


def test_shrinkage_high_volume_approaches_raw_rate():
    # Two shooters: player 7's huge volume dominates the pooled totals, player 9's
    # smaller-volume, different rate pulls the pooled league mean away from 0.5 --
    # with only one shooter the league mean equals the player's own raw rate
    # exactly, so the shrinkage pseudo-count k does no real work and the
    # assertion would pass even with a broken formula. With two shooters the
    # league mean genuinely diverges from 0.5, so k must actually pull the
    # estimate toward it; the assertion still holds because player 7's volume
    # (10000 attempts) swamps the pseudo-count (k=100).
    sh = pl.DataFrame(
        {
            "game_id": ["g", "g"],
            "possession_number": [1, 1],
            "player_id": [7, 9],
            "team_id": [100, 100],
            "fg2a": [0, 0],
            "fg2m": [0, 0],
            "fg3a": [10000, 100],
            "fg3m": [5000, 0],
            "fta": [0, 0],
            "ftm": [0, 0],
        }
    )
    lg3 = 5000 / 10100
    assert lg3 != 0.5  # pooled league mean now genuinely differs from player 7's raw rate
    rates = _shrunk_shooter_rates(sh, fg3_k=100.0)
    assert abs(rates.filter(pl.col("player_id") == 7)["p3"][0] - 0.5) < 0.02


def test_la_response_reduces_to_points_when_rates_are_realized():
    poss = _with_possession_number(_synth())
    # give every possession a couple of made 3s/FTs so the terms are non-trivial
    poss = (
        poss.with_columns(
            (pl.col("points") // 3).alias("fg3m"),
            pl.lit(1).alias("ftm"),
        )
        .with_columns(((pl.col("points") - 3 * pl.col("fg3m") - pl.col("ftm")).clip(0) // 2).alias("fg2m"))
        .with_columns((2 * pl.col("fg2m") + 3 * pl.col("fg3m") + pl.col("ftm")).alias("points"))
    )
    sh = _shooting_for(poss)
    # realized per-shooter rates => la_points == points exactly (invariant, formula-independent)
    realized = {
        int(r["player_id"]): (
            (r["fg3m"] / r["fg3a"]) if r["fg3a"] else 0.0,
            (r["ftm"] / r["fta"]) if r["fta"] else 0.0,
        )
        for r in sh.group_by("player_id").agg(pl.col(["fg3a", "fg3m", "fta", "ftm"]).sum()).iter_rows(named=True)
    }
    out = luck_adjusted_response(poss, sh, realized)
    assert np.allclose(out["la_points"].to_numpy(), out["points"].to_numpy(), atol=1e-6)


def test_la_response_empty_shooting_is_two_point_only():
    poss = _with_possession_number(_synth()).with_columns(
        pl.lit(0).alias("fg3m"), pl.lit(0).alias("ftm"), (pl.col("points") // 2).alias("fg2m")
    )
    empty_sh = pl.DataFrame(
        schema={
            "game_id": pl.Utf8,
            "possession_number": pl.Int64,
            "player_id": pl.Int64,
            "team_id": pl.Int64,
            "fg2a": pl.Int64,
            "fg2m": pl.Int64,
            "fg3a": pl.Int64,
            "fg3m": pl.Int64,
            "fta": pl.Int64,
            "ftm": pl.Int64,
        }
    )
    out = luck_adjusted_response(poss, empty_sh)
    assert np.allclose(out["la_points"].to_numpy(), (2 * poss["fg2m"]).to_numpy())


def test_la_response_excludes_defense_team_shooters():
    # Reviewer-reproduced bug: build_possession_shooting legitimately keeps a
    # DEFENSE-team shooter row (e.g. a defensive technical FT -- its own
    # team_id != offense_team_id, which is exactly why the frame carries a
    # team_id column). la_points is offense-only by definition (DECISION 2), so
    # a single offense 2-pt make plus one defense tech-FT shooter must produce
    # la_points == 2.0. Before the fix the defense shooter's fta * p_hat_ft term
    # leaked into the aggregation (0 + 1 * 0.9 = 0.9), inflating la_points to 2.9.
    poss = pl.DataFrame(
        {
            "game_id": ["g"],
            "possession_number": [1],
            "offense_team_id": [100],
            "fg2m": [1],
        }
    )
    sh = pl.DataFrame(
        {
            "game_id": ["g", "g"],
            "possession_number": [1, 1],
            "player_id": [7, 8],
            "team_id": [100, 200],  # 7 = offense shooter, 8 = defense tech-FT shooter
            "fg2a": [1, 0],
            "fg2m": [1, 0],
            "fg3a": [0, 0],
            "fg3m": [0, 0],
            "fta": [0, 1],
            "ftm": [0, 1],
        }
    )
    player_rates = {7: (0.0, 0.0), 8: (0.0, 0.9)}
    out = luck_adjusted_response(poss, sh, player_rates)
    assert out["la_points"][0] == 2.0


def test_la_response_sums_multiple_offense_shooters_in_one_possession():
    # Exercise the n>1 summation path: two offense shooters each attempt a 3 in
    # the SAME possession -- exp_extra must sum both shooters' contributions,
    # not just the first / last.
    poss = pl.DataFrame(
        {
            "game_id": ["g"],
            "possession_number": [1],
            "offense_team_id": [100],
            "fg2m": [0],
        }
    )
    sh = pl.DataFrame(
        {
            "game_id": ["g", "g"],
            "possession_number": [1, 1],
            "player_id": [7, 9],
            "team_id": [100, 100],
            "fg2a": [0, 0],
            "fg2m": [0, 0],
            "fg3a": [2, 4],
            "fg3m": [0, 0],
            "fta": [0, 0],
            "ftm": [0, 0],
        }
    )
    player_rates = {7: (0.25, 0.0), 9: (0.5, 0.0)}
    out = luck_adjusted_response(poss, sh, player_rates)
    # la_points = 2*fg2m(0) + 3*(2*0.25 + 4*0.5) = 3*(0.5 + 2.0) = 7.5
    assert out["la_points"][0] == 7.5


def _synth(seed: int = 1, n_games: int = 20) -> pl.DataFrame:
    o = {p: 0.03 for p in list(range(100, 108)) + list(range(200, 208))}
    d = {p: 0.01 for p in o}
    return _synthetic_possessions(o, d, n_games=n_games, poss_per_game=40, noise_sd=0.3, seed=seed)


def test_decay_weights_halflife_math():
    dates = pl.Series("game_date", [datetime.date(2023, 1, 1), datetime.date(2023, 1, 31)])
    w = decay_weights(dates, datetime.date(2023, 1, 31), half_life_days=30.0)
    # 30 days ago -> 0.5 ; 0 days ago -> 1.0
    assert np.isclose(w[0], 0.5, atol=1e-9)
    assert np.isclose(w[1], 1.0, atol=1e-9)


def test_decay_weights_asof_none_all_ones():
    dates = pl.Series("game_date", [datetime.date(2023, 1, 1), datetime.date(2023, 6, 1)])
    w = decay_weights(dates, None, half_life_days=30.0)
    assert np.allclose(w, 1.0)


def test_decay_weights_future_games_clamped_not_amplified():
    # a game AFTER asof must not receive weight > 1
    dates = pl.Series("game_date", [datetime.date(2023, 12, 31)])
    w = decay_weights(dates, datetime.date(2023, 1, 1), half_life_days=30.0)
    assert w[0] <= 1.0 + 1e-9


def test_prepare_row_alignment_matches_design():
    poss = _synth()
    X, y, w, pids = _prepare(poss, "points", weight_col=None)
    assert X.shape[0] == len(y)
    assert w is None
    assert len(pids) > 0


def test_fit_weighted_equals_plain_rapm_on_points():
    poss = _synth()
    X, y, _w, pids = _prepare(poss, "points")
    o, d, off_poss, def_poss = _fit_weighted(X, y)
    ref = nba_rapm(poss).sort("player_id")
    got = pl.DataFrame({"player_id": pids, "o": o, "d": d}).sort("player_id")
    # same design + same RidgeCV grid + unit weights => byte-close to nba_rapm
    assert np.allclose(got["o"].to_numpy(), ref["o_rapm"].to_numpy(), atol=1e-6)
    assert np.allclose(got["d"].to_numpy(), ref["d_rapm"].to_numpy(), atol=1e-6)


def test_fit_weighted_honors_weights():
    # planted: down-weighting half the games to ~0 must change the fit
    poss = _synth()
    X, y, _w, _pids = _prepare(poss, "points")
    o_unw, _d, _o, _dp = _fit_weighted(X, y)
    w = np.ones(len(y))
    w[: len(y) // 2] = 1e-6
    o_w, _d2, _o2, _dp2 = _fit_weighted(X, y, weights=w)
    assert not np.allclose(o_unw, o_w, atol=1e-3)


def test_oracle_rapm_alphas_scales_by_sample_count_not_player_count():
    # Regression pin: Ryan Davis's oracle (NBA_Tutorials_Ryan_Davis/rapm/rapm.py:112-125)
    # scales lambda by `train_x.shape[0]` (possessions / regression samples), NOT the
    # player count. lambda_to_alpha(l, samples) = (l * samples) / 2.0.
    alphas = oracle_rapm_alphas(50_000, ORACLE_RAPM_LAMBDAS)
    assert np.allclose(alphas, [250.0, 1250.0, 2500.0])


def _synth_with_dates(seed: int = 1, n_games: int = 20) -> pl.DataFrame:
    poss = _synth(seed=seed, n_games=n_games)
    # assign each game a distinct date, oldest game first
    gids = poss["game_id"].unique(maintain_order=True).to_list()
    base = datetime.date(2023, 1, 1)
    dmap = {g: base + datetime.timedelta(days=i) for i, g in enumerate(gids)}
    return poss.with_columns(pl.col("game_id").replace_strict(dmap, return_dtype=pl.Date).alias("game_date"))


def test_decay_rapm_empty_input():
    out = nba_decay_rapm(pl.DataFrame())
    assert out.height == 0
    assert dict(out.schema) == DECAY_RAPM_SCHEMA


def test_decay_rapm_asof_none_equals_plain_rapm():
    poss = _synth_with_dates()
    dec = nba_decay_rapm(poss, asof=None).sort("player_id")
    ref = nba_rapm(poss.drop("game_date")).sort("player_id")
    assert np.allclose(dec["decay_rapm"].to_numpy(), ref["rapm"].to_numpy(), atol=1e-6)


def test_decay_rapm_weighting_changes_fit():
    # Isolate the decay-WEIGHT effect from the ridge-schedule switch: both calls
    # pass `asof` (so both take the oracle-alphas / cv=ORACLE_RAPM_CV branch and,
    # since asof == the max game_date, neither filters any possessions --
    # `oracle_rapm_alphas(X.shape[0])` is evaluated at an identical sample count
    # on both sides). Only `half_life_days` differs: a huge half-life makes every
    # weight ~1.0 (decay-neutral), vs a short one that decays hard.
    #
    # A prior version compared asof=None (DEFAULT_RAPM_ALPHAS, cv=None) against
    # asof=<date> (oracle alphas, cv=5): that discriminates on the schedule
    # switch ALONE (empirically: max diff ~2.55 even with weights forced to 1),
    # so it would still "pass" if the `_w` decay-weight wiring were silently
    # broken -- it proved "two configs differ," not "recency weighting changes
    # the fit." This version holds the schedule fixed and isolates the
    # decay-only effect (empirically: max diff ~2.57 at atol=1e-3).
    poss = _synth_with_dates()
    asof = poss["game_date"].max()
    neutral = nba_decay_rapm(poss, asof=asof, half_life_days=1e9).sort("player_id")
    decayed = nba_decay_rapm(poss, asof=asof, half_life_days=5.0).sort("player_id")
    assert not np.allclose(neutral["decay_rapm"].to_numpy(), decayed["decay_rapm"].to_numpy(), atol=1e-3)


def test_decay_rapm_schema_and_dtypes():
    out = nba_decay_rapm(_synth_with_dates())
    assert dict(out.schema) == DECAY_RAPM_SCHEMA


def test_la_rapm_empty_input():
    empty_sh = pl.DataFrame(
        schema={
            "game_id": pl.Utf8,
            "possession_number": pl.Int64,
            "player_id": pl.Int64,
            "team_id": pl.Int64,
            "fg2a": pl.Int64,
            "fg2m": pl.Int64,
            "fg3a": pl.Int64,
            "fg3m": pl.Int64,
            "fta": pl.Int64,
            "ftm": pl.Int64,
        }
    )
    out = nba_la_rapm(pl.DataFrame(), empty_sh)
    assert out.height == 0 and dict(out.schema) == LA_RAPM_SCHEMA


def test_la_rapm_equals_same_schedule_reference_when_rates_realized():
    # Controller ruling: nba_la_rapm's operative fit is the ORACLE schedule
    # (oracle_rapm_alphas + cv=ORACLE_RAPM_CV), same as every other WP2 variant --
    # so this reduces-to-plain-response gate must compare against a SAME-SCHEDULE
    # internal reference (_same_schedule_reference), never the public nba_rapm
    # (which fits at the plain DEFAULT_RAPM_ALPHAS/cv=None schedule -- a
    # cross-schedule comparison would conflate "response recipe is correct" with
    # "ridge schedule happens to match", which it no longer does).
    poss = _with_possession_number(_synth()).with_columns((pl.col("points") // 3).alias("fg3m"), pl.lit(1).alias("ftm"))
    poss = poss.with_columns(
        ((pl.col("points") - 3 * pl.col("fg3m") - pl.col("ftm")).clip(0) // 2).alias("fg2m")
    ).with_columns((2 * pl.col("fg2m") + 3 * pl.col("fg3m") + pl.col("ftm")).alias("points"))
    sh = _shooting_for(poss)
    realized = {
        int(r["player_id"]): (
            (r["fg3m"] / r["fg3a"]) if r["fg3a"] else 0.0,
            (r["ftm"] / r["fta"]) if r["fta"] else 0.0,
        )
        for r in sh.group_by("player_id").agg(pl.col(["fg3a", "fg3m", "fta", "ftm"]).sum()).iter_rows(named=True)
    }
    la = nba_la_rapm(poss, sh, realized).sort("player_id")
    ref = _same_schedule_reference(poss, "points").sort("player_id")
    # la_points == points => LA-RAPM equals the same-schedule internal reference
    # (formula-independent invariant, isolated from the ridge-schedule choice)
    assert np.allclose(la["la_rapm"].to_numpy(), ref["rapm"].to_numpy(), atol=1e-6)


def _synth_four_factor(seed: int = 3) -> pl.DataFrame:
    poss = _synth(seed=seed)
    # plant per-possession four-factor counts consistent with points
    return poss.with_columns(
        (pl.col("points") // 3).alias("fg3m"),
        pl.lit(1).alias("ftm"),
        pl.lit(1).alias("oreb"),
        pl.lit(0).alias("tov"),
    ).with_columns(((pl.col("points") - 3 * pl.col("fg3m") - pl.col("ftm")).clip(0) // 2).alias("fg2m"))


def test_four_factor_empty_input():
    out = nba_four_factor_rapm(pl.DataFrame())
    assert out.height == 0 and dict(out.schema) == FOUR_FACTOR_SCHEMA


def test_four_factor_schema_and_dtypes():
    out = nba_four_factor_rapm(_synth_four_factor())
    assert dict(out.schema) == FOUR_FACTOR_SCHEMA
    assert out.height > 0


def test_four_factor_planted_orbd_signal():
    # a team whose offense always grabs an oreb should have positive orbd__off for its players
    poss = _synth_four_factor()
    out = nba_four_factor_rapm(poss)
    # every possession has oreb=1 (constant) => ridge shrinks player effects toward ~0 but
    # the fit must run and produce finite values for all four factors
    for c in ["efg__off", "ftr__off", "orbd__off", "tov__off"]:
        assert out[c].is_finite().all()


def test_four_factor_uses_oracle_schedule_matches_same_schedule_reference():
    # Binding WP2 ruling: every variant's operative fit is the ORACLE schedule
    # (oracle_rapm_alphas + cv=ORACLE_RAPM_CV); pin that nba_four_factor_rapm's
    # default (alphas=None) reproduces the same-schedule internal reference on
    # each factor's own response, so this variant can't silently drift back to
    # the plain nba_rapm schedule.
    poss = _synth_four_factor()
    out = nba_four_factor_rapm(poss).sort("player_id")
    for factor, response_expr in (
        ("efg", (2 * pl.col("fg2m") + 3 * pl.col("fg3m")).cast(pl.Float64)),
        ("ftr", pl.col("ftm").cast(pl.Float64)),
        ("orbd", pl.col("oreb").cast(pl.Float64)),
        ("tov", (-pl.col("tov")).cast(pl.Float64)),  # RA_TOV fits on -tov (polarity fix)
    ):
        ref_poss = poss.with_columns(response_expr.alias("_resp"))
        ref = _same_schedule_reference(ref_poss, "_resp").sort("player_id")
        got = out[f"{factor}__off"].to_numpy() + out[f"{factor}__def"].to_numpy()
        assert np.allclose(got, ref["rapm"].to_numpy(), atol=1e-6)


def test_four_factor_tov_directionality_turnover_prone_offense_is_lower():
    # Bugfix regression gate: RA_TOV must follow the module-wide "higher = better"
    # convention on BOTH sides. Plant a turnover-prone offensive player (player 100:
    # every possession he's on offense for has tov=1, all others tov=0) alongside a
    # clean teammate (player 101, never elevated). A correctly-signed fit must give
    # the turnover-prone player a LOWER tov__off than the clean one -- fitting on the
    # raw (un-negated) response would get this backwards (more turnovers reading as
    # a HIGHER, i.e. "better", tov__off).
    poss = _synth_four_factor(seed=11)
    prone_on_offense = pl.any_horizontal([pl.col(f"off_player_{i}") == 100 for i in range(1, 6)])
    poss = poss.with_columns(pl.when(prone_on_offense).then(1).otherwise(0).alias("tov"))
    out = nba_four_factor_rapm(poss)
    prone = out.filter(pl.col("player_id") == 100)["tov__off"][0]
    clean = out.filter(pl.col("player_id") == 101)["tov__off"][0]
    assert prone < clean


def test_la_rapm_schema_and_dtypes():
    poss = _with_possession_number(_synth()).with_columns(
        pl.lit(0).alias("fg3m"), pl.lit(0).alias("ftm"), (pl.col("points") // 2).alias("fg2m")
    )
    out = nba_la_rapm(poss, _shooting_for(poss))
    assert dict(out.schema) == LA_RAPM_SCHEMA
    assert out.height > 0


# ---------------------------------------------------------------------------
# Task 6 — gated concurrent-validity live tests (vs Ryan Davis oracle CSVs)
# ---------------------------------------------------------------------------
#
# Both gates must be set to run these: SDV_PY_NBA_STATS_LIVE=1 (stats.nba.com
# hangs on datacenter/cloud IPs -- residential only, see conftest.py) AND
# SDV_PY_NBA_ORACLE_DIR=<path to the Ryan Davis oracle CSVs> (rapm_ryan_davis.csv
# / rapm_multi_ryan_davis.csv, `playerId` Int64 + `LA_RAPM`/`RAPM`/`RA_*`
# columns). These compile FULL NBA seasons over the live stats API, so a run
# takes a long time -- they're meant for occasional local verification, not CI.

import os  # noqa: E402
import pathlib  # noqa: E402

from tests.conftest import skip_if_no_nba_oracle, skip_if_no_nba_stats_live  # noqa: E402


def _oracle(name: str) -> pl.DataFrame:
    root = pathlib.Path(os.environ["SDV_PY_NBA_ORACLE_DIR"])
    return pl.read_csv(root / name, infer_schema_length=10000).with_columns(pl.col("playerId").cast(pl.Int64))


@skip_if_no_nba_stats_live
@skip_if_no_nba_oracle
def test_la_rapm_concurrent_validity_vs_ryan_davis():
    """LA-RAPM must track the oracle's LA_RAPM at least as well as plain RAPM does."""
    from sportsdataverse.nba import build_possession_shooting, compile_nba_season

    # NOTE: import the FUNCTION from its own submodule, not the package level --
    # `sportsdataverse.nba` also has a `nba_enhanced_pbp` SUBMODULE of the same
    # name, and Python auto-registers imported submodules as attributes of their
    # parent package, so `from sportsdataverse.nba import nba_enhanced_pbp` binds
    # the module object (not callable), not the function.
    from sportsdataverse.nba.nba_enhanced_pbp import nba_enhanced_pbp

    poss = compile_nba_season(2022)  # 2022-23 regular season -- oracle coverage ends here
    gids = poss["game_id"].unique().to_list()
    shoot_frames = [build_possession_shooting(nba_enhanced_pbp(gid)) for gid in gids]
    shooting = pl.concat(shoot_frames, how="diagonal_relaxed")

    la = nba_la_rapm(poss, shooting).filter(pl.col("off_poss") + pl.col("def_poss") >= 500)
    plain = nba_rapm(poss).filter(pl.col("off_poss") + pl.col("def_poss") >= 500)
    orc = (
        _oracle("rapm_ryan_davis.csv")
        .filter(pl.col("season") == "2022-23")
        .select(pl.col("playerId").alias("player_id"), pl.col("LA_RAPM"), pl.col("RAPM"))
    )
    assert la.schema["player_id"] == orc.schema["player_id"]  # Int64 both sides
    j_la = la.join(orc, on="player_id", how="inner")
    j_pl = plain.join(orc, on="player_id", how="inner")
    cov = j_la.height / max(orc.height, 1)
    print(f"LA-RAPM oracle join coverage: {cov:.1%} ({j_la.height}/{orc.height})")
    la_corr = float(np.corrcoef(j_la["la_rapm"], j_la["LA_RAPM"])[0, 1])
    plain_corr = float(np.corrcoef(j_pl["rapm"], j_pl["LA_RAPM"])[0, 1])
    print(f"LA vs LA_RAPM corr={la_corr:.3f} ; plain-RAPM vs LA_RAPM corr={plain_corr:.3f}")
    # FLOOR set empirically on first real run, then ratcheted (harness convention).
    # The spec's teeth: the LA variant must track LA_RAPM at least as well as plain RAPM.
    assert la_corr >= plain_corr - 0.02  # non-regression guard; tighten to a real floor after first run


@skip_if_no_nba_stats_live
@skip_if_no_nba_oracle
def test_decay_rapm_concurrent_validity_vs_ryan_davis_multi():
    """decay_rapm (best of half_life_days in [60, 120, 240]) vs a multi-season oracle window.

    Compiles 2 real seasons (2018-19, 2019-20). ``rapm_multi_ryan_davis.csv``'s
    windows are irregular (mostly 3- or 5-season spans anchored to varying
    start years -- see the CSV's ``season`` column), so there is no exact
    2-season match; ``"2015-20"`` (the only window ENDING at 2019-20) is used
    as the nearest available comparison and the mapping is a printed
    diagnostic only, never a hard-asserted equivalence (DECISION 9).
    """

    from sportsdataverse.nba import compile_nba_season

    seasons = [2018, 2019]
    frames = [compile_nba_season(s) for s in seasons]
    poss = pl.concat(frames, how="diagonal_relaxed")
    asof = poss["game_date"].max()
    print(
        f"decay_rapm oracle diagnostic: compiled seasons {seasons} (asof={asof}); "
        "nearest oracle multi-window = '2015-20' (only window ending in 2019-20's season)"
    )

    oracle_window = "2015-20"
    orc = (
        _oracle("rapm_multi_ryan_davis.csv")
        .filter(pl.col("season") == oracle_window)
        .select(pl.col("playerId").alias("player_id"), pl.col("LA_RAPM"), pl.col("RAPM"))
    )

    plain = nba_rapm(poss).filter(pl.col("off_poss") + pl.col("def_poss") >= 500)
    j_pl = plain.join(orc, on="player_id", how="inner")
    plain_corr = float(np.corrcoef(j_pl["rapm"], j_pl["RAPM"])[0, 1])

    best_corr, best_hl, best_cov = float("-inf"), None, 0.0
    for hl in (60.0, 120.0, 240.0):
        decay = nba_decay_rapm(poss, asof=asof, half_life_days=hl).filter(
            pl.col("off_poss") + pl.col("def_poss") >= 500
        )
        j = decay.join(orc, on="player_id", how="inner")
        cov = j.height / max(orc.height, 1)
        corr = float(np.corrcoef(j["decay_rapm"], j["RAPM"])[0, 1]) if j.height > 1 else float("-inf")
        print(
            f"half_life_days={hl:.0f}: decay_rapm vs oracle RAPM corr={corr:.3f} coverage={cov:.1%} ({j.height}/{orc.height})"
        )
        if corr > best_corr:
            best_corr, best_hl, best_cov = corr, hl, cov

    print(
        f"best half_life_days={best_hl}: corr={best_corr:.3f} coverage={best_cov:.1%} ; "
        f"plain-RAPM vs oracle RAPM corr={plain_corr:.3f}"
    )
    # non-regression guard; tighten to a real floor once a first real run establishes one.
    assert best_corr >= plain_corr - 0.02
