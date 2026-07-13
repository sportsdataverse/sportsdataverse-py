"""Internal-oracle gates for the T5.3/T5.3b PWHL xG models (quality proxy + coord xG).

No external PWHL oracle exists (first-of-its-kind, best-effort): the honest
internal oracle is a HELD-OUT realized-outcome backtest, run for BOTH xG
methods over the identical as-of walk. The fixtures are built with a strict
train/holdout split (see ``tests/fixtures/pwhl_prediction/README.md`` +
``dev/pwhl_prediction/build_pwhl_xg_fixture.py``):

- the xG model (tier weights OR coordinate logistic) is fit on strictly-prior
  complete seasons (2024+2025 for the held-out 2026 walk) -- no game sees a
  model fit on its own or later data;
- ``margin_sd`` is fit PER METHOD on 2025 only (quality 1.21, coords 1.19),
  with 2026 kept entirely out of the fit.

**Honest result** (held-out 2026, n=107, 2026-07-12 rebuild): coords Brier
0.2444 (delta -0.0056 vs naive, SE 0.0055) vs quality Brier 0.2449 (delta
-0.0051, SE 0.0053) vs naive 0.2500; paired coords-minus-quality per-game
Brier diff -0.0005 (paired SE 0.0006). The coordinate xG gates best on every
measured axis and is the DEFAULT ``xg_method``, but neither method's edge
over naive reaches 2 SE, so per the honesty rule the gates remain held-out
CALIBRATION + no-worse-than-naive-within-noise for BOTH methods -- a
beats-naive magnitude assertion stays deliberately absent (needs more PWHL
seasons). Floors are set from observed values, conservative; never lower a
floor to pass -- debug the model.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, roc_auc_score

from sportsdataverse.nhl.nhl_prediction_constants import brier_score, calibration_table, get_constants
from sportsdataverse.pwhl.pwhl_xg_proxy import (
    PwhlCoordXGModel,
    fit_pwhl_coord_xg,
    fit_shot_quality_xg,
    pwhl_team_game_xg_rates,
)
from tests.conftest import skip_if_no_live

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "pwhl_prediction"


@pytest.fixture(scope="module")
def train_shots() -> pl.DataFrame:
    return pl.read_parquet(FIXTURES_DIR / "shots_train_2024_2025.parquet")


@pytest.fixture(scope="module")
def game_rates() -> pl.DataFrame:
    return pl.read_parquet(FIXTURES_DIR / "game_rates_heldout_2026.parquet")


@pytest.fixture(scope="module")
def backtest() -> pl.DataFrame:
    return pl.read_parquet(FIXTURES_DIR / "backtest_heldout_2026.parquet")


# ---------------------------------------------------------------------------
# Shot-quality tier weight: directional sanity (the internal oracle for the
# proxy fit itself -- "Quality" shots must score at a higher empirical goal
# rate than "Non quality" shots, or the tier labels are meaningless/inverted).
# Fit on the training pool (2024+2025); this checks the fit MECHANISM, not an
# out-of-sample prediction, so it is not a leakage surface.
# ---------------------------------------------------------------------------
QUALITY_WEIGHT_MARGIN = 0.03  # observed diff ~0.070 (0.120 vs 0.050); floor conservative below


def test_shot_quality_weights_directionally_sane(train_shots: pl.DataFrame) -> None:
    model = fit_shot_quality_xg(train_shots)
    assert "quality" in model.weights and "non_quality" in model.weights
    diff = model.weights["quality"] - model.weights["non_quality"]
    assert diff >= QUALITY_WEIGHT_MARGIN, (
        f"'quality' tier goal-rate {model.weights['quality']:.4f} exceeds 'non_quality' "
        f"{model.weights['non_quality']:.4f} by only {diff:.4f} (floor {QUALITY_WEIGHT_MARGIN}) "
        "-- the shot_quality tier collapse or fit may be inverted"
    )
    for w in model.weights.values():
        assert 0.0 < w < 1.0


# ---------------------------------------------------------------------------
# Held-out game-rates shape sanity.
# ---------------------------------------------------------------------------
def test_game_rates_shape_and_dtypes(game_rates: pl.DataFrame) -> None:
    assert game_rates.height > 150, "held-out PWHL game-rates corpus unexpectedly small"
    assert game_rates.schema["game_id"] == pl.Utf8
    assert game_rates.schema["team"] == pl.Utf8
    assert game_rates.schema["date"] == pl.Date
    assert game_rates.filter(pl.col("date").is_null()).height == 0, "every regular-season game should resolve a date"
    counts = game_rates.group_by("game_id").agg(pl.len().alias("n"))
    assert (counts["n"] == 2).all(), "every game should contribute exactly one home + one away row"


# ---------------------------------------------------------------------------
# Held-out realized-outcome backtest -- the honest internal oracle.
# NOT a beats-naive magnitude gate (the -0.0051 edge is within noise): instead
# (a) no-worse-than-naive within sampling noise, and (b) calibration.
# ---------------------------------------------------------------------------
NAIVE_BRIER = 0.25
# No-harm tolerance ~2 SE (SE ~0.0053 on 107 games) so a genuine degradation
# (anti-predictive model -> Brier well above naive) fails, while noise on a
# fixture refresh does not spuriously fail. Observed held-out Brier 0.2449.
BRIER_NO_HARM_TOL = 0.011
MIN_EVALUATED_GAMES = 90
# Held-out predictions cluster near 0.5 (exp_margin is heavily shrink-
# compressed), so calibration_table collapses to a single adequately-sampled
# base-rate bucket: observed mean_pred 0.5505 vs mean_actual 0.5607, dev
# ~0.0102. This is effectively a base-rate calibration check; a multi-bucket
# calibration gate needs more predicted spread (more seasons). Floor
# conservative above observed.
CALIBRATION_FLOOR = 0.06
MIN_CALIBRATION_BUCKET_N = 30


def test_pwhl_heldout_no_worse_than_naive(backtest: pl.DataFrame) -> None:
    assert backtest.height >= MIN_EVALUATED_GAMES, (
        f"held-out PWHL backtest only {backtest.height} games (floor {MIN_EVALUATED_GAMES})"
    )
    y = backtest["home_win"].to_numpy()
    p = backtest["home_win_prob"].to_numpy()
    model_brier = brier_score(y, p)
    naive_brier = brier_score(y, np.full(len(y), 0.5))
    assert abs(naive_brier - NAIVE_BRIER) < 0.01, "sanity-check the documented naive baseline"
    # Honest claim: the de-leaked model is NOT meaningfully WORSE than naive
    # out-of-sample (it is very slightly better, within noise). A real
    # degradation (bug making it anti-predictive) pushes Brier above this bar.
    assert model_brier <= naive_brier + BRIER_NO_HARM_TOL, (
        f"held-out PWHL model Brier {model_brier:.4f} exceeds naive {naive_brier:.4f} by more than the "
        f"no-harm tolerance {BRIER_NO_HARM_TOL} -- the xG proxy or margin_sd fit regressed; debug before "
        "touching this tolerance"
    )


def test_pwhl_heldout_calibration(backtest: pl.DataFrame) -> None:
    cal = calibration_table(backtest["home_win"].to_numpy(), backtest["home_win_prob"].to_numpy(), n_bins=5)
    cal = cal.filter(pl.col("n") >= MIN_CALIBRATION_BUCKET_N)
    assert cal.height >= 1, "no adequately-sampled calibration bucket -- corpus may have shrunk"
    dev = (cal["mean_pred"] - cal["mean_actual"]).abs()
    assert dev.max() <= CALIBRATION_FLOOR, (
        f"held-out PWHL calibration max per-bucket deviation {dev.max():.4f} above floor {CALIBRATION_FLOOR}"
    )


def test_pwhl_margin_sd_was_fit_out_of_sample() -> None:
    # The old seeded value (2.35) was a real-world-goal-margin-scale guess; a
    # fitted, much-smaller sigma (1.19 coords-paired / 1.21 quality-paired, fit
    # on 2025 with 2026 held out) must be in place. See
    # nhl_prediction_constants.py's PWHL margin_sd comment.
    const = get_constants("pwhl")
    assert 0.9 <= const.margin_sd <= 1.6, (
        f"PWHL margin_sd {const.margin_sd} outside the fitted-scale range -- reverted to the old seed?"
    )


# ---------------------------------------------------------------------------
# T5.3b coordinate-based xG -- mechanism tests (synthetic mini-frames).
# ---------------------------------------------------------------------------
def _mini_pbp() -> pl.DataFrame:
    """One game, 5 shot rows; one is a GOAL with null coords (the coverage-gap case)."""
    return pl.DataFrame(
        {
            "game_id": [1, 1, 1, 1, 1],
            "event": ["shot"] * 5,
            "team_id": [10, 10, 10, 20, 20],
            "shot_quality": [
                "Quality on net",
                "Non quality on net",
                "Quality goal",
                "Non quality on net",
                "Quality on net",
            ],
            "goal": [False, False, True, False, False],
            "x_coord": [85.0, 30.0, None, -85.0, -40.0],
            "y_coord": [0.0, -10.0, None, 2.0, 20.0],
            "power_play": pl.Series([None] * 5, dtype=pl.Int32),
        }
    )


def _mini_sched() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "game_id": [1],
            "season": [2026],
            "game_type": ["regular"],
            "home_team_id": [10],
            "home_team": ["Toronto"],
            "away_team_id": [20],
            "away_team": ["Boston"],
        }
    )


def _distance_only_model(fallback_rate: float = 0.08) -> PwhlCoordXGModel:
    """A deterministic real-logistic PwhlCoordXGModel: close-range -> high xG, long-range -> low."""
    clf = LogisticRegression()
    x = np.zeros((4, 2))
    x[:, 0] = [4.0, 10.0, 60.0, 85.0]  # shot_distance (shot_angle stays 0)
    clf.fit(x, [1, 1, 0, 0])
    return PwhlCoordXGModel(model=clf, fallback_rate=fallback_rate)


def test_team_game_xg_rates_coords_wiring_and_null_coord_fallback() -> None:
    model = _distance_only_model(fallback_rate=0.08)
    rates = pwhl_team_game_xg_rates(_mini_pbp(), _mini_sched(), xg_method="coords", xg_model=model)
    assert rates.height == 2
    home = rates.filter(pl.col("team") == "Toronto")
    # Realized goals are NOT model-dependent: the null-coord goal still counts.
    assert home["gf"][0] == 1
    # The null-coord shot contributes the model's fallback rate -- NOT a
    # fill_null(0)->distance=0 point-blank prediction (which would be >0.5
    # for this model). Home xgf = xg(85,0) + xg(30,-10) + fallback.
    scored = model.predict(pl.DataFrame({"x_coord": [85.0, 30.0], "y_coord": [0.0, -10.0]}))
    assert home["xgf"][0] == pytest.approx(scored.sum() + 0.08, abs=1e-9)
    # Close-range shot must out-xG the long-range one under the fitted curve.
    assert scored[0] > scored[1]


def test_team_game_xg_rates_quality_path_unchanged_when_requested() -> None:
    # xg_method="quality" must keep the T5.3 categorical-proxy behavior
    # working (API stability): tier weights fit internally, all 5 shots
    # contribute. (The DEFAULT method is coords since T5.3b.)
    rates = pwhl_team_game_xg_rates(_mini_pbp(), _mini_sched(), xg_method="quality")
    assert rates.height == 2
    assert rates["xgf"].sum() == pytest.approx(1.0)  # empirical rates sum to total goals


def test_team_game_xg_rates_explicit_quality_model_default_method_coordless_frame() -> None:
    # T5.3-era calling pattern regression: an explicit ShotQualityXGModel with
    # xg_method omitted (now defaulting to coords) on a frame WITHOUT
    # coordinate columns must keep working -- the method only selects the
    # internal fit, and no coords-path code may reference x_coord/y_coord.
    pbp = _mini_pbp().drop("x_coord", "y_coord")
    model = fit_shot_quality_xg(pbp)
    rates = pwhl_team_game_xg_rates(pbp, _mini_sched(), xg_model=model)
    assert rates.height == 2
    assert rates["xgf"].sum() == pytest.approx(1.0)


def test_fit_pwhl_coord_xg_coordless_frame_falls_back_to_goal_rate() -> None:
    # A frame with no coordinate columns fits nothing but must NOT emit a
    # silent all-zero xG: constant-rate model at the observed goal rate.
    pbp = _mini_pbp().drop("x_coord", "y_coord")
    model = fit_pwhl_coord_xg(pbp)
    assert model.model is None
    assert model.fallback_rate == pytest.approx(0.2)  # 1 goal / 5 shots
    with pytest.raises(ValueError, match="goal"):
        fit_pwhl_coord_xg(pbp.drop("goal"))


def test_team_game_xg_rates_rejects_unknown_method() -> None:
    with pytest.raises(ValueError, match="xg_method"):
        pwhl_team_game_xg_rates(_mini_pbp(), _mini_sched(), xg_method="nope")


def test_fit_pwhl_coord_xg_small_sample_falls_back() -> None:
    model = fit_pwhl_coord_xg(_mini_pbp())
    assert model.model is None  # < 200 qualifying shots -> constant-rate fallback
    # Fallback rate = goal rate among coord-complete shots (1 goal row lacks coords -> 0/4).
    assert model.fallback_rate == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# T5.3b coordinate-based xG -- the model's own internal-oracle gates, fit on
# the committed real train pool (2024+2025). In-sample internal-consistency
# checks of the fit mechanism (not an out-of-sample claim; the out-of-sample
# claim is the held-out 2026 backtest below).
# ---------------------------------------------------------------------------
# Observed on the 2026-07-12 rebuild (2024+2025 pool): 10,593 coord-complete
# shots. Floor conservative below; a big drop means the fixture regressed.
COORD_FIT_SHOTS_FLOOR = 10_000
# Observed in-sample AUC 0.6745, logloss 0.2733 vs base-rate 0.2871 on the
# train pool (dev/pwhl_prediction/build_pwhl_xg_fixture.py's model report).
# Floors conservative below/above observed; never lower to pass.
COORD_AUC_FLOOR = 0.60
COORD_LOGLOSS_MARGIN = 0.005  # observed base-rate - model logloss ~ 0.0138


@pytest.fixture(scope="module")
def coord_model(train_shots: pl.DataFrame) -> PwhlCoordXGModel:
    return fit_pwhl_coord_xg(train_shots)


def test_coord_xg_fit_is_real_and_sample_floored(train_shots: pl.DataFrame, coord_model: PwhlCoordXGModel) -> None:
    assert coord_model.model is not None, "coord xG fell back to constant-rate on the full train pool"
    qualifying = train_shots.filter(
        (pl.col("event") == "shot") & pl.col("x_coord").is_not_null() & pl.col("y_coord").is_not_null()
    )
    assert qualifying.height >= COORD_FIT_SHOTS_FLOOR, (
        f"coord-xG fit sample {qualifying.height} below floor {COORD_FIT_SHOTS_FLOOR} -- "
        "the shots_train fixture lost coordinate coverage"
    )


def test_coord_xg_monotone_in_distance(coord_model: PwhlCoordXGModel) -> None:
    # Hard assert on the fitted curve: point-blank > slot > long-range, all
    # straight-on (y=0). Distances 4 / 30 / 64 ft; observed on the 2026-07-12
    # rebuild: [0.2482, 0.1152, 0.0372] (point-blank ~6.7x long-range).
    grid = pl.DataFrame({"x_coord": [85.0, 59.0, 25.0], "y_coord": [0.0, 0.0, 0.0]})
    xg = coord_model.predict(grid)
    assert xg[0] > xg[1] > xg[2], f"fitted xG not decreasing in distance: {xg.to_list()}"
    assert xg[0] >= 2.0 * xg[2], (
        f"point-blank xG {xg[0]:.4f} not clearly above long-range {xg[2]:.4f} -- distance signal collapsed"
    )


def test_coord_xg_beats_base_rate_in_sample(train_shots: pl.DataFrame, coord_model: PwhlCoordXGModel) -> None:
    shots = train_shots.filter(
        (pl.col("event") == "shot") & pl.col("x_coord").is_not_null() & pl.col("y_coord").is_not_null()
    )
    y = shots["goal"].cast(pl.Int64).to_numpy()
    p = coord_model.predict(shots).to_numpy()
    auc = roc_auc_score(y, p)
    model_ll = log_loss(y, p)
    base_ll = log_loss(y, np.full(len(y), y.mean()))
    assert auc >= COORD_AUC_FLOOR, f"coord-xG in-sample AUC {auc:.4f} below floor {COORD_AUC_FLOOR}"
    assert model_ll <= base_ll - COORD_LOGLOSS_MARGIN, (
        f"coord-xG logloss {model_ll:.4f} does not beat base-rate {base_ll:.4f} by {COORD_LOGLOSS_MARGIN}"
    )


# ---------------------------------------------------------------------------
# T5.3b held-out 2026 backtest for the COORDS method -- same as-of walk, same
# leakage discipline as the quality-proxy gate above (model fit 2024+2025,
# margin_sd fit 2025 only). Gates mirror the quality path: no-worse-than-naive
# + calibration; floors from the observed rebuild, documented in
# tests/fixtures/pwhl_prediction/README.md.
# ---------------------------------------------------------------------------
def test_pwhl_heldout_coords_no_worse_than_naive(backtest: pl.DataFrame) -> None:
    assert "home_win_prob_coords" in backtest.columns, "backtest fixture predates the T5.3b dual-method rebuild"
    assert backtest.height >= MIN_EVALUATED_GAMES
    y = backtest["home_win"].to_numpy()
    p = backtest["home_win_prob_coords"].to_numpy()
    model_brier = brier_score(y, p)
    naive_brier = brier_score(y, np.full(len(y), 0.5))
    assert model_brier <= naive_brier + BRIER_NO_HARM_TOL, (
        f"held-out coords Brier {model_brier:.4f} exceeds naive {naive_brier:.4f} by more than "
        f"{BRIER_NO_HARM_TOL} -- the coord xG or its margin_sd fit regressed"
    )


def test_pwhl_heldout_coords_calibration(backtest: pl.DataFrame) -> None:
    cal = calibration_table(backtest["home_win"].to_numpy(), backtest["home_win_prob_coords"].to_numpy(), n_bins=5)
    cal = cal.filter(pl.col("n") >= MIN_CALIBRATION_BUCKET_N)
    assert cal.height >= 1
    dev = (cal["mean_pred"] - cal["mean_actual"]).abs()
    assert dev.max() <= CALIBRATION_FLOOR, (
        f"held-out coords calibration max per-bucket deviation {dev.max():.4f} above floor {CALIBRATION_FLOOR}"
    )


# ---------------------------------------------------------------------------
# T5 Phase 3: feature expansion (rebound + strength dummies) is additive and
# backward-compatible. The 6-feature model activates only when the pbp carries
# the R4 strength/clock columns; a legacy frame falls back to distance/angle.
# ---------------------------------------------------------------------------

_FULL_FEATURES = (
    "shot_distance",
    "shot_angle",
    "rebound",
    "is_home",
    "is_pp",
    "is_sh",
    "empty_net_for",
    "is_wrist",
    "is_snap",
    "is_slap",
    "is_backhand",
    "is_tip",
)


def _synth_strength_pbp(n: int = 300) -> pl.DataFrame:
    """Deterministic shot log carrying the R4 strength + clock + shot-type columns."""
    return pl.DataFrame(
        {
            "event": ["shot"] * n,
            "event_type": [["Wrist", "Snap", "Slap", "Backhand", "Tip", "Default"][i % 6] for i in range(n)],
            "goal": [1 if i % 7 == 0 else 0 for i in range(n)],  # ~14%, two classes
            "x_coord": [40.0 + (i % 50) for i in range(n)],  # varying distance 40..89 ft
            "y_coord": [0.0] * n,
            "game_id": [str(1000 + i // 30) for i in range(n)],  # ~10 games
            "sec_from_start": [float((i % 30) * 40) for i in range(n)],
            "team_id": ["5" if i % 2 else "6" for i in range(n)],
            "home_team_id": ["5"] * n,
            "away_team_id": ["6"] * n,
            "skaters_home": [5] * n,
            "skaters_away": [5 if i % 3 else 4 for i in range(n)],  # some home power plays
        }
    )


_MOVE_FEATURES = (
    "last_shot",
    "last_faceoff",
    "last_hit",
    "last_blocked",
    "last_penalty",
    "time_since_last",
    "distance_from_last",
    "last_x",
    "last_y",
)


def _synth_full_pbp(n: int = 300) -> pl.DataFrame:
    """Full-pbp shape: a real PLAY event before each shot, plus PWHL's coordless
    DUPLICATE ``goal`` row -- emitted immediately BEFORE the scoring shot, so an
    unguarded ``shift(1)`` would pick it up as that shot's "prior event" (the leak).
    """
    rows: list[dict[str, object]] = []
    for i in range(n):
        common: dict[str, object] = {
            "game_id": str(1000 + i // 30),  # ~10 games
            "home_team_id": "5",
            "away_team_id": "6",
            "skaters_home": 5,
            "skaters_away": 5 if i % 3 else 4,  # some home power plays
        }
        t = float((i % 30) * 40)
        scored = i % 7 == 0  # ~14%, two classes
        rows.append(
            {
                **common,
                "event": "faceoff" if i % 2 else "hit",
                "event_type": "Default",
                "goal": 0,
                "x_coord": 20.0,
                "y_coord": 5.0,
                "sec_from_start": t - 5.0,
                "team_id": "5",
            }
        )
        if scored:
            rows.append(
                {
                    **common,
                    "event": "goal",
                    "event_type": "Default",
                    "goal": 1,
                    "x_coord": None,
                    "y_coord": None,
                    "sec_from_start": t,
                    "team_id": "5",
                }
            )
        rows.append(
            {
                **common,
                "event": "shot",
                "event_type": ["Wrist", "Snap", "Slap", "Backhand", "Tip", "Default"][i % 6],
                "goal": 1 if scored else 0,
                "x_coord": 40.0 + (i % 50),  # varying distance 40..89 ft
                "y_coord": 0.0,
                "sec_from_start": t,
                "team_id": "5" if i % 2 else "6",
            }
        )
    return pl.DataFrame(rows)


def test_coord_xg_adds_movement_features_on_full_pbp() -> None:
    from sportsdataverse.pwhl.pwhl_xg_proxy import fit_pwhl_coord_xg

    m = fit_pwhl_coord_xg(_synth_full_pbp())
    assert m.model is not None
    assert m.features == _FULL_FEATURES + _MOVE_FEATURES, f"expected movement features, got {m.features}"


def test_preshot_context_excludes_duplicate_goal_rows() -> None:
    """The leak guard: a scoring shot's "prior event" must be the real prior PLAY,
    not PWHL's coordless duplicate ``goal`` row (a perfect goal tell -- it inflated
    held-out AUC 0.707 -> 0.755 before the ``_PLAY_EVENTS`` filter).
    """
    from sportsdataverse.pwhl.pwhl_xg_proxy import _add_preshot_context

    ctx = _add_preshot_context(_synth_full_pbp()).filter(pl.col("event") == "shot")
    assert ctx.filter(pl.col("goal") == 1).height > 0, "synth frame must contain goals to test the leak"
    # Every shot is preceded by a faceoff or a hit, 5s and 20ft-x away -- including
    # the scoring ones. If the `goal` dupe leaked in, goal shots would show neither
    # (and inherit its null coords as last_x=0).
    assert ((ctx["last_faceoff"] + ctx["last_hit"]) == 1).all()
    assert (ctx["time_since_last"] == 5.0).all()
    assert (ctx["last_x"] == 20.0).all()
    assert (ctx["last_shot"] == 0).all()


def test_preshot_context_noop_on_shots_only_frame_and_idempotent() -> None:
    from sportsdataverse.pwhl.pwhl_xg_proxy import _add_preshot_context

    # A shots-only frame has no prior-event context (every "prior" would be a shot):
    # no-op, so the caller degrades to the shot-local features.
    shots = _synth_strength_pbp()
    assert _add_preshot_context(shots).columns == shots.columns
    # Idempotent -- fit() and pwhl_team_game_xg_rates() may both call it.
    once = _add_preshot_context(_synth_full_pbp())
    assert _add_preshot_context(once).columns == once.columns


def _mini_strength_pbp() -> pl.DataFrame:
    """One game: a 5v5 shot, a valid 5v4 power-play shot, and a null-strength shot."""
    return pl.DataFrame(
        {
            "game_id": [1, 1, 1],
            "event": ["shot"] * 3,
            "team_id": [10, 10, 10],
            "goal": [False, False, False],
            "x_coord": [85.0, 30.0, 60.0],
            "y_coord": [0.0, -10.0, 5.0],
            "strength_state": ["5v5", "5v4", None],
            "strength_state_valid": [True, True, None],
        }
    )


def test_even_strength_filter_prefers_shift_derived_strength_state() -> None:
    """R4 `strength_state` governs when present: drop the CONFIDENTLY non-even shot,
    keep the null-strength one (a shift-tracking gap must not delete a shot).
    """
    model = _distance_only_model()
    pbp, sched = _mini_strength_pbp(), _mini_sched()
    ev = pwhl_team_game_xg_rates(pbp, sched, xg_model=model, even_strength_only=True)
    everything = pwhl_team_game_xg_rates(pbp, sched, xg_model=model, even_strength_only=False)
    tor_ev = ev.filter(pl.col("team") == "Toronto")["xgf"][0]
    tor_all = everything.filter(pl.col("team") == "Toronto")["xgf"][0]
    # Exactly the 5v4 shot is dropped -- so the null-strength shot was KEPT.
    dropped = model.predict(pl.DataFrame({"x_coord": [30.0], "y_coord": [-10.0]}))[0]
    assert tor_all - tor_ev == pytest.approx(dropped, abs=1e-9)


def test_even_strength_filter_handles_string_power_play_on_prer4_frame() -> None:
    """Regression: load_pwhl_pbp ships `power_play` as Utf8 ("0"/"1"/null), not int.
    The old numeric filter (`fill_null(0) != 1`) raised ComputeError on every real
    frame; it only passed tests because `_mini_pbp` hand-builds it as Int32.
    """
    model = _distance_only_model()
    # Pre-R4 shape (no strength_state) with the feed's real String power_play.
    pbp = (
        _mini_strength_pbp()
        .drop("strength_state", "strength_state_valid")
        .with_columns(power_play=pl.Series(["0", "1", None], dtype=pl.Utf8))
    )
    ev = pwhl_team_game_xg_rates(pbp, _mini_sched(), xg_model=model, even_strength_only=True)
    everything = pwhl_team_game_xg_rates(pbp, _mini_sched(), xg_model=model, even_strength_only=False)
    tor_ev = ev.filter(pl.col("team") == "Toronto")["xgf"][0]
    tor_all = everything.filter(pl.col("team") == "Toronto")["xgf"][0]
    # Only the "1" shot is dropped -- the null-tagged one is KEPT (sparse tagging).
    dropped = model.predict(pl.DataFrame({"x_coord": [30.0], "y_coord": [-10.0]}))[0]
    assert tor_all - tor_ev == pytest.approx(dropped, abs=1e-9)


def test_coord_xg_uses_strength_features_when_present() -> None:
    from sportsdataverse.pwhl.pwhl_xg_proxy import fit_pwhl_coord_xg

    m = fit_pwhl_coord_xg(_synth_strength_pbp())
    assert m.model is not None
    assert m.features == _FULL_FEATURES, f"expected full-feature model, got {m.features}"
    p = m.predict(_synth_strength_pbp())
    assert len(p) == 300
    assert p.min() >= 0.0 and p.max() <= 1.0


def test_coord_xg_falls_back_to_two_features_without_strength_cols() -> None:
    from sportsdataverse.pwhl.pwhl_xg_proxy import fit_pwhl_coord_xg

    n = 300
    legacy = pl.DataFrame(
        {
            "event": ["shot"] * n,
            "goal": [1 if i % 7 == 0 else 0 for i in range(n)],
            "x_coord": [40.0 + (i % 50) for i in range(n)],
            "y_coord": [0.0] * n,
        }
    )
    m = fit_pwhl_coord_xg(legacy)
    assert m.features == ("shot_distance", "shot_angle"), f"legacy frame must stay 2-feature, got {m.features}"


def test_shot_geometry_rejects_raw_scale_coords() -> None:
    """R7-(1): a RAW-scale (0-600) enrich frame must fail loud, never be scored with goal_x=89.

    The dual-frame footgun: pwhl_pbp() enrich-path x_coord is RAW feed scale, where only
    shot_distance/shot_angle are trustworthy. Deriving geometry from those coords with the
    default goal_x=89 is silent garbage -- _shot_geometry range-checks x_coord instead.
    """
    from sportsdataverse.pwhl.pwhl_xg_proxy import _assert_rink_feet, _shot_geometry

    raw = pl.DataFrame({"event": ["shot", "shot"], "x_coord": [120.0, 540.0], "y_coord": [150.0, 160.0]})
    with pytest.raises(ValueError, match="RAW HockeyTech feed scale"):
        _shot_geometry(raw)
    with pytest.raises(ValueError, match="RAW HockeyTech feed scale"):
        _assert_rink_feet(raw)

    # rink-feet coords (|x| <= ~100) pass, and a frame already carrying the geometry cols
    # is reused verbatim (no range check, no recompute).
    feet = pl.DataFrame({"event": ["shot"], "x_coord": [80.0], "y_coord": [5.0]})
    assert "shot_distance" in _shot_geometry(feet).columns
    precomputed = feet.with_columns(shot_distance=pl.lit(9.0), shot_angle=pl.lit(0.0))
    assert _shot_geometry(precomputed)["shot_distance"][0] == 9.0  # reused, not recomputed


# ---------------------------------------------------------------------------
# Per-strength Platt recalibration (T5 follow-up). Mechanism proven offline on a
# seeded synthetic frame where each strength bucket has a DIFFERENT true distance
# slope -- something the pooled model's single distance coefficient + is_pp/is_sh
# intercepts cannot fit, leaving residual per-bucket bias for Platt to correct. The
# real-data ECE/ordering improvement is gated live below.
# ---------------------------------------------------------------------------
def _synthetic_strength_pbp(n_per_bucket: int = 400, sh_n: int | None = None) -> pl.DataFrame:
    rng = np.random.default_rng(0)
    rows = []
    # (skaters_home, skaters_away, intercept, slope) per bucket -- distinct slopes.
    specs = {
        "EV": (5, 5, 0.0, 0.10, n_per_bucket),
        "PP": (5, 4, 0.6, 0.03, n_per_bucket),
        "SH": (4, 5, -0.6, 0.22, sh_n if sh_n is not None else n_per_bucket),
    }
    for _b, (skh, ska, inter, slope, nb) in specs.items():
        xc = rng.uniform(55.0, 89.0, nb)
        dist = 89.0 - xc
        p = 1.0 / (1.0 + np.exp(-(inter - slope * dist)))
        goal = (rng.random(nb) < p).astype(int)
        for xi, gi in zip(xc, goal):
            rows.append(
                {
                    "event": "shot",
                    "goal": int(gi),
                    "x_coord": float(xi),
                    "y_coord": 0.0,
                    "team_id": "1",
                    "home_team_id": "1",
                    "skaters_home": skh,
                    "skaters_away": ska,
                }
            )
    return pl.DataFrame(rows)


def _bucket_of(pbp: pl.DataFrame) -> np.ndarray:
    return np.where(
        pbp["skaters_home"].to_numpy() > pbp["skaters_away"].to_numpy(),
        "PP",
        np.where(pbp["skaters_home"].to_numpy() < pbp["skaters_away"].to_numpy(), "SH", "EV"),
    )


def test_strength_calibration_fits_and_reduces_per_bucket_bias() -> None:
    pbp = _synthetic_strength_pbp()
    cal_model = fit_pwhl_coord_xg(pbp, calibrate_strength=True)
    raw_model = fit_pwhl_coord_xg(pbp, calibrate_strength=False)

    # (1) calibrators are fit per well-sampled bucket; the off switch yields none.
    assert cal_model.strength_calibrators is not None
    assert set(cal_model.strength_calibrators) == {"EV", "PP", "SH"}
    assert raw_model.strength_calibrators is None

    # (2) calibration reduces the aggregate per-bucket calibration error (sum over
    # EV/PP/SH of |mean_pred - mean_goal|). Platt per bucket is the MLE recal, so this
    # is a principled, deterministic improvement.
    buckets = _bucket_of(pbp)
    y = pbp["goal"].to_numpy()
    p_cal = cal_model.predict(pbp).to_numpy()
    p_raw = raw_model.predict(pbp).to_numpy()
    err_cal = sum(abs(p_cal[buckets == b].mean() - y[buckets == b].mean()) for b in ("EV", "PP", "SH"))
    err_raw = sum(abs(p_raw[buckets == b].mean() - y[buckets == b].mean()) for b in ("EV", "PP", "SH"))
    assert err_cal < err_raw, f"calibration did not reduce per-bucket bias: {err_cal:.4f} vs {err_raw:.4f}"

    # (3) AUC preserved (Platt is monotone in the base logit within a bucket).
    assert roc_auc_score(y, p_cal) >= roc_auc_score(y, p_raw) - 0.005


def test_strength_calibration_skips_thin_bucket() -> None:
    # SH bucket below _MIN_CALIB_SHOTS earns no calibrator; the well-sampled ones do.
    pbp = _synthetic_strength_pbp(n_per_bucket=400, sh_n=50)
    model = fit_pwhl_coord_xg(pbp, calibrate_strength=True)
    assert model.strength_calibrators is not None
    assert "SH" not in model.strength_calibrators
    assert {"EV", "PP"}.issubset(model.strength_calibrators)


def _bucket_ece(y: np.ndarray, p: np.ndarray, nbins: int = 10) -> float:
    edges = np.linspace(0, 1, nbins + 1)
    e = 0.0
    for i in range(nbins):
        m = (p >= edges[i]) & (p < edges[i + 1])
        if m.sum():
            e += abs(p[m].mean() - y[m].mean()) * m.sum()
    return e / len(y)


@skip_if_no_live
def test_strength_calibration_improves_real_pwhl_per_strength_ece() -> None:
    """Live gate: per-strength Platt cuts HELD-OUT (LOSO) SH calibration error on real
    PWHL pbp, at no AUC cost -- the productionization proof for the T5 follow-up.

    Measured per-bucket **10-bin ECE** (not mean bias: the ``is_pp``/``is_sh`` dummies
    already zero each shooter-relative bucket's *mean*, so the win is in the within-bucket
    tails, which ECE captures and a mean check misses), on the model's own SHOOTER-relative
    partition (``strength_state`` is home-relative and mislabels away-team special teams).
    NOT asserting a PP>EV>SH ordering: shooter-relative SH includes empty-net shots (high
    conversion), so SH is legitimately the highest-rate bucket. Observed 2026-07-13:
    SH_ECE 0.0130->0.0091, PP_ECE 0.0047->0.0036, AUC 0.6962 both."""
    from sklearn.metrics import roc_auc_score

    from sportsdataverse.pwhl import load_pwhl_pbp
    from sportsdataverse.pwhl.pwhl_xg_proxy import (
        _add_preshot_context,
        _build_xg_features,
        _strength_bucket_from_onehots,
    )

    seasons = [2024, 2025, 2026]
    pbp = pl.concat(
        [load_pwhl_pbp([s]).with_columns(pl.lit(s).alias("season")) for s in seasons], how="diagonal_relaxed"
    ).with_columns(pl.col("game_id").cast(pl.Utf8))
    pbp = _add_preshot_context(pbp)

    def _loso(calibrate: bool) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        ys, ps, bs = [], [], []
        for s in seasons:
            tr = pbp.filter(pl.col("season") != s)
            te = pbp.filter(pl.col("season") == s).filter(
                (pl.col("event") == "shot") & pl.col("x_coord").is_not_null() & pl.col("y_coord").is_not_null()
            )
            m = fit_pwhl_coord_xg(tr, calibrate_strength=calibrate)
            xb, names, _ = _build_xg_features(te, want=m.features)
            ys.append(te["goal"].cast(pl.Int64).to_numpy())
            ps.append(m.predict(te).to_numpy())
            bs.append(_strength_bucket_from_onehots(xb, names))
        return np.concatenate(ys), np.concatenate(ps), np.concatenate(bs)

    y_r, p_r, b_r = _loso(calibrate=False)
    y_c, p_c, b_c = _loso(calibrate=True)  # same folds/order -> same y, b

    # SH (the structural weak spot) held-out ECE must strictly improve; overall AUC held.
    sh_r = _bucket_ece(y_r[b_r == "SH"], p_r[b_r == "SH"])
    sh_c = _bucket_ece(y_c[b_c == "SH"], p_c[b_c == "SH"])
    assert sh_c < sh_r, f"held-out SH ECE not improved by calibration: {sh_c:.4f} vs {sh_r:.4f}"
    assert _bucket_ece(y_c, p_c) <= _bucket_ece(y_r, p_r) + 1e-4, "overall held-out ECE regressed"
    assert roc_auc_score(y_c, p_c) >= roc_auc_score(y_r, p_r) - 0.002, "calibration cost AUC"
