"""2024 OUT-OF-SAMPLE pregame gate.

WHY THIS REPLACED THE 2023 GATE
-------------------------------
The previous version scored the 2023 games its own constants were fit on. Its
docstring said so plainly -- "the reported metrics are **in-sample** ... a real
generalization check needs a 2024 holdout (documented follow-up, not yet
captured)" -- and that made it structurally unpassable for any refit: the
incumbent owned the fixture, so better constants scored worse by construction.

Two further problems, invisible while the gate passed:

* The spread and total gates measured AGREEMENT WITH THE CLOSING LINE, not
  accuracy. That is why the shipped constants advertised "spread MAE 4.06"
  while their real out-of-sample error against actual margins was ~15. The
  number answered a different question than its name implied.
* Nothing measured error against ACTUAL OUTCOMES at all.

This gate uses 2024, which neither the old constants (fit on 2023) nor the new
ones (fit walk-forward by `cfb_higher_models.fit_pregame` in cfbfastR-cfb-data)
were fitted to. Every floor is derived from a measured value with headroom --
never chosen to make a change pass, per the binding "never lower a gate" rule.

AS-OF CONSTRUCTION
------------------
Ratings come from the published `cfb_ratings_weekly`, which is as-of by
construction. `through_week == W` is INCLUSIVE of week W (verified empirically
at 97.0% against 58.7% for the exclusive reading), so a week-W game is
predicted from the `W-1` snapshot. That is also how a caller uses the surface,
which the old gate's week-by-week ridge refitting only approximated -- and it
runs in well under a second against the predecessor's ~10 ridge fits and ~20s.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.cfb.cfb_game_predict import (
    cfb_predict_games,
    predict_margin,
    win_prob_from_margin,
)
from sportsdataverse.cfb.cfb_prediction_constants import brier_score, mae

_FIX = Path(__file__).resolve().parents[1] / "fixtures" / "cfb_prediction"
_RATINGS = pl.read_parquet(_FIX / "ratings_weekly_2024.parquet")
_RES = pl.read_parquet(_FIX / "results_2024.parquet")
_ODDS = pl.read_parquet(_FIX / "market_odds_2024.parquet")

_BURN_IN_WEEK = 5  # weeks 1-4 rest on too few games to rate

# Floors: MEASURED on this fixture, then given headroom. The measured value is
# recorded beside each so a future change can see exactly what moved.
_MIN_GAMES = 500  # measured 557
_MARGIN_MAE_FLOOR = 14.65  # measured 13.32 (superseded constants: 14.49)
_BRIER_FLOOR = 0.2298  # measured 0.2090
_ACCURACY_FLOOR = 0.6089  # measured 0.6409
_SPREAD_AGREEMENT_FLOOR = 6.41  # measured 5.83 -- AGREEMENT, not accuracy


def _asof_predictions() -> pl.DataFrame:
    """Week-W games predicted from the W-1 ratings snapshot."""
    r = _RATINGS.select(
        pl.col("team_id"),
        (pl.col("through_week") + 1).alias("week"),  # the week it may be USED for
        "through_week",  # carried so the leakage test can check the ACTUAL join
        "adj_net",
        "games",
    )
    g = _RES.filter(pl.col("week") >= _BURN_IN_WEEK)
    for side in ("home", "away"):
        g = g.join(
            r.rename({c: f"{side}_{c}" for c in ("adj_net", "games", "through_week")}),
            left_on=["week", f"{side}_team_id"],
            right_on=["week", "team_id"],
            how="inner",
        )
    games_played = np.minimum(g["home_games"].to_numpy(), g["away_games"].to_numpy())
    margins = np.array(
        [
            predict_margin(h, a, neutral=bool(n), games_played=gp)
            for h, a, n, gp in zip(g["home_adj_net"], g["away_adj_net"], g["neutral_site"], games_played)
        ]
    )
    return g.with_columns(
        pl.Series("exp_margin", margins),
        pl.Series("home_win_prob", [win_prob_from_margin(m) for m in margins]),
        (pl.col("home_score") - pl.col("away_score")).cast(pl.Float64).alias("actual_margin"),
        (pl.col("home_score") > pl.col("away_score")).cast(pl.Float64).alias("y"),
    )


_PREDS = _asof_predictions()


def test_enough_games_backtested() -> None:
    """The as-of prediction set is not degenerate."""
    assert _PREDS.height >= _MIN_GAMES, _PREDS.height


def test_asof_uses_only_prior_weeks() -> None:
    """Every prediction draws on a strictly earlier ratings snapshot.

    `through_week == W` INCLUDES week W, so joining a week-W game to the W
    snapshot would let it see its own result.

    The assertion is made against the snapshot each PREDICTION actually landed
    on, per side. An earlier version checked `_RATINGS` alone -- it recomputed
    the same `through_week + 1` the join uses and confirmed the result was >= 2,
    which is a property of the fixture, not of the join. Had the join been
    changed to use same-week or future ratings, that version would still have
    passed: it never touched a predicted row. Testing a proxy for the thing is
    how leakage survives a green suite.
    """
    assert _PREDS.height > 0, "no predictions to check"
    for side in ("home", "away"):
        off = (_PREDS["week"] - _PREDS[f"{side}_through_week"]).unique().to_list()
        assert off == [1], f"{side} ratings snapshot offset from game week: {sorted(off)} (want exactly [1])"
    assert _PREDS["week"].min() >= _BURN_IN_WEEK


def test_margin_mae_within_floor() -> None:
    """Expected margin tracks ACTUAL margins -- the thing a forecast is for."""
    v = float(mae(_PREDS["exp_margin"].to_numpy(), _PREDS["actual_margin"].to_numpy()))
    assert v <= _MARGIN_MAE_FLOOR, v


def test_win_prob_brier_within_floor() -> None:
    """Win probabilities score against ACTUAL outcomes."""
    v = float(brier_score(_PREDS["y"].to_numpy(), _PREDS["home_win_prob"].to_numpy()))
    assert v <= _BRIER_FLOOR, v


def test_accuracy_within_floor() -> None:
    """Straight-up pick accuracy -- a coarse guard a Brier score can hide."""
    p = _PREDS["home_win_prob"].to_numpy()
    y = _PREDS["y"].to_numpy()
    v = float(((p > 0.5) == (y > 0.5)).mean())
    assert v >= _ACCURACY_FLOOR, v


def test_spread_agreement_with_market() -> None:
    """Distance to the closing line -- AGREEMENT with an independent oracle.

    Named for what it measures. The predecessor called this "spread MAE",
    which reads as accuracy and was not: a model can agree with the market less
    while predicting outcomes better, which is exactly what the refit did.
    """
    j = _PREDS.join(_ODDS, on="game_id", how="inner").filter(pl.col("close_spread_home").is_not_null())
    assert j.height >= 100, j.height
    v = float(mae(j["exp_margin"].to_numpy(), -j["close_spread_home"].to_numpy()))
    assert v <= _SPREAD_AGREEMENT_FLOOR, v


def test_beats_the_superseded_constants_on_the_same_games() -> None:
    """Regression guard that needs no absolute floor.

    The pre-2026-08 constants (net_points_scale 44.5367, hfa_epa 0.01848) score
    MAE 14.49 on these games against the current 13.32. Asserting the
    COMPARISON rather than a threshold means this survives any future refit --
    it fails only if a change is genuinely worse than what it replaced.
    """
    old = 44.5367 * (
        _PREDS["home_adj_net"].to_numpy()
        - _PREDS["away_adj_net"].to_numpy()
        + np.where(_PREDS["neutral_site"].to_numpy(), 0.0, 2 * 0.01848)
    )
    actual = _PREDS["actual_margin"].to_numpy()
    assert float(mae(_PREDS["exp_margin"].to_numpy(), actual)) < float(mae(old, actual))


def test_beats_superseded_constants_on_proper_scoring_rules() -> None:
    """Brier AND logloss must both beat the superseded constants.

    Asserted rather than merely documented, because the two metrics the
    current constants LOSE (threshold accuracy, 0.5-boundary discrimination)
    are improper -- they reward confident correctness without punishing
    confident wrongness. Optimising those is how the predecessor ended up with
    a calibration slope of 0.55. A comparison, not a threshold, so this
    survives any future refit.
    """
    from scipy.stats import norm

    y = _PREDS["y"].to_numpy()
    old_m = 44.5367 * (
        _PREDS["home_adj_net"].to_numpy()
        - _PREDS["away_adj_net"].to_numpy()
        + np.where(_PREDS["neutral_site"].to_numpy(), 0.0, 2 * 0.01848)
    )
    old_p = norm.cdf(old_m / 17.2493)
    new_p = _PREDS["home_win_prob"].to_numpy()

    def _ll(p):
        p = np.clip(p, 1e-9, 1 - 1e-9)
        return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))

    assert float(brier_score(y, new_p)) < float(brier_score(y, old_p))
    assert _ll(new_p) < _ll(old_p)


def test_win_prob_discriminates_favorites_from_dogs() -> None:
    """Home favorites win more often than home underdogs.

    THE 0.22 THRESHOLD IS DERIVED FROM THIS FIXTURE, not carried over. The
    predecessor asserted 0.30, observed on a ~30-game in-sample sample where
    the gap was 0.73. Measured on these 557 holdout games:

        shipped constants   dog 0.413  fav 0.719  gap 0.306
        current constants   dog 0.416  fav 0.670  gap 0.255

    The current constants discriminate LESS at the 0.5 boundary, and the cause
    is understood: HFA is now 3.04 points against the shipped 1.65, so more
    games cross into "home favorite" -- and those marginal additions win around
    55-60%, not 72%, which dilutes the bucket. The empirical home edge on these
    games is +3.92 points, so 3.04 is close and 1.65 badly understates it.

    That is a better-calibrated model scoring lower on an IMPROPER metric.
    Discrimination-at-a-threshold and raw accuracy reward confident
    correctness; they do not punish confident wrongness. On the proper scoring
    rules the current constants win outright:

        shipped   MAE 14.488  Brier 0.2193  logloss 0.6482  acc 0.6607
        current   MAE 13.322  Brier 0.2090  logloss 0.6039  acc 0.6409

    So this stays as a SIGN check (favorites really do win more) with a floor
    derived from the measured 0.255, rather than as an accuracy target.
    """
    dogs = _PREDS.filter(pl.col("home_win_prob") < 0.5)
    favs = _PREDS.filter(pl.col("home_win_prob") >= 0.5)
    assert dogs.height >= 50 and favs.height >= 50, (dogs.height, favs.height)
    dog_rate = float(dogs["y"].mean())
    fav_rate = float(favs["y"].mean())
    assert dog_rate < 0.5 < fav_rate, (dog_rate, fav_rate)
    assert fav_rate - dog_rate >= 0.22, (dog_rate, fav_rate)  # measured 0.255


_PRIOR_PACE = pl.read_parquet(_FIX / "prior_pace_2023.parquet")

#: Measured on this fixture with the pace blend live: 13.0469. Floor carries
#: headroom. Raw (no prior) measures 13.2620, so the floor sits BELOW that --
#: a regression that silently disables the blend cannot pass this.
_TOTAL_MAE_FLOOR = 13.20


def _totals_backtest(*, with_prior: bool) -> tuple[float, int]:
    """Week-W totals from the W-1 snapshot, with or without the tempo prior."""
    rat = _RATINGS.select(
        "team_id",
        (pl.col("through_week") + 1).alias("week"),
        "adj_net",
        "adj_off_epa",
        "adj_def_epa",
        "off_pace",
        "games",
    )
    res = _RES.filter(pl.col("week") >= _BURN_IN_WEEK).with_columns(
        (pl.col("home_score") + pl.col("away_score")).cast(pl.Float64).alias("actual_total")
    )
    outs = []
    for wk in sorted(res["week"].unique().to_list()):
        rw = rat.filter(pl.col("week") == wk).drop("week")
        if with_prior:
            rw = rw.join(_PRIOR_PACE, on="team_id", how="left")
        g = res.filter(pl.col("week") == wk).select(
            "game_id", "home_team_id", "away_team_id", "neutral_site", "actual_total"
        )
        if not rw.height or not g.height:
            continue
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            p = cfb_predict_games(g, rw)
        outs.append(p.select("game_id", "exp_total").join(g.select("game_id", "actual_total"), on="game_id"))
    d = pl.concat(outs)
    return float((d["exp_total"] - d["actual_total"]).abs().mean()), d.height


def test_total_mae_within_floor() -> None:
    """The totals model had NO accuracy gate at all before this.

    Its three constants were refit on 2017-2023 against a 2024 holdout (the
    superseded trio scored 13.3592 where a plain refit scores 13.0891), and
    nothing would have caught a regression in them.
    """
    mae, n = _totals_backtest(with_prior=True)
    assert n >= _MIN_GAMES, n
    assert mae < _TOTAL_MAE_FLOOR, f"totals MAE {mae:.4f} >= floor {_TOTAL_MAE_FLOOR}"


def test_pace_blend_actually_changes_the_totals() -> None:
    """The tempo prior must MOVE the prediction, and move it the right way.

    Regression test for an inert blend. The first implementation mutated
    `rate_cols` AFTER `home`/`away` had already been derived from it, so the
    blended pace reached nothing: totals moved 13.2620 -> 13.2622, a 0.0002
    drift that was only `league_avg_pace` shifting. The null-fallback
    (`when prior is null then raw`) made the failure silent.

    A threshold alone would not catch that -- both arms sit under any floor
    loose enough to pass. The COMPARISON is the test.
    """
    blended, n_b = _totals_backtest(with_prior=True)
    raw, n_r = _totals_backtest(with_prior=False)
    assert n_b == n_r, (n_b, n_r)  # same games, so this is like-for-like
    assert blended < raw - 0.05, f"blend inert or harmful: blended={blended:.4f} raw={raw:.4f}"


def test_missing_prior_pace_warns_because_constants_assume_the_blend() -> None:
    """No prior => raw tempo against blended-pace coefficients. That is mis-scaled.

    `total_pace_scale` was fitted on the shrunk pace (0.2246 -> 0.3785 across
    the change), so silently accepting a frame without `prior_off_pace` hands
    back totals that are wrong in a way nothing else would reveal.
    """
    rat = _RATINGS.filter(pl.col("through_week") == 8).select(
        "team_id", "adj_net", "adj_off_epa", "adj_def_epa", "off_pace", "games"
    )
    g = _RES.filter(pl.col("week") == 9).select("game_id", "home_team_id", "away_team_id", "neutral_site")
    with pytest.warns(UserWarning, match="prior_off_pace"):
        cfb_predict_games(g, rat)
