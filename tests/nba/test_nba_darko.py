"""Tests for AgingCurve + fit_aging_curve (Task 2), Kalman filter (Task 3), MLE fit + projection (Task 4), and forecast validator (Task 5)."""

from __future__ import annotations

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_darko import (
    AgingCurve,
    _fit_noise_params,
    _forecast,
    _kalman_filter,
    darko_forecast_accuracy,
    fit_aging_curve,
    nba_darko,
)


def _panel(seed: int = 0) -> tuple[pl.DataFrame, pl.DataFrame]:
    rng = np.random.default_rng(seed)
    rows, arows = [], []
    for pid in range(60):
        skill = float(rng.normal(0, 3))
        start = int(rng.integers(22, 30))
        for k, season in enumerate(range(2018, 2023)):
            rows.append({"player_id": pid, "season": season, "rating": skill + rng.normal(0, 0.8), "weight": 1.0})
            arows.append({"player_id": pid, "season": season, "age": float(start + k)})
            skill += rng.normal(0, 0.3)
    return pl.DataFrame(rows), pl.DataFrame(arows)


def test_fit_aging_curve_recovers_planted_deltas() -> None:
    """Aging-curve meta-oracle: planted ±1.0/yr drift should be recovered within 0.4."""
    # planted true aging delta: +1.0/yr before age 27, -1.0/yr after
    rng = np.random.default_rng(0)
    rows, arows = [], []
    for pid in range(200):
        start_age = int(rng.integers(20, 34))
        skill = float(rng.normal(0, 3))
        for k, season in enumerate(range(2016, 2022)):
            age = start_age + k
            rows.append({"player_id": pid, "season": season, "rating": skill + rng.normal(0, 0.5)})
            arows.append({"player_id": pid, "season": season, "age": float(age)})
            skill += 1.0 if age < 27 else -1.0  # planted drift applied going forward
    curve = fit_aging_curve(pl.DataFrame(rows), pl.DataFrame(arows), smooth=1)
    assert curve.delta(23) > 0.5 and curve.delta(30) < -0.5  # recovers sign + rough magnitude
    assert abs(curve.delta(23) - 1.0) < 0.4 and abs(curve.delta(30) + 1.0) < 0.4


def test_aging_curve_delta_outside_range_is_zero() -> None:
    """delta() returns 0.0 for ages not in delta_by_age."""
    c = AgingCurve(delta_by_age={25: 0.5, 26: 0.3})
    assert c.delta(99) == 0.0 and c.delta(10) == 0.0


def test_kalman_recovers_latent_and_forecasts() -> None:
    """Kalman filter denoises a random-walk latent skill and forecasts sanely."""
    rng = np.random.default_rng(1)
    curve = AgingCurve(delta_by_age={a: 0.0 for a in range(20, 40)})  # no aging for this test
    true_skill = 5.0
    ratings, ages, weights, latents = [], [], [], []
    s = true_skill
    for k in range(8):
        ratings.append(s + rng.normal(0, 1.0))
        ages.append(25.0 + k)
        weights.append(1.0)
        latents.append(s)
        s += rng.normal(0, 0.3)  # small random-walk skill drift
    s_final, P_final, s_preds, innov_vars = _kalman_filter(
        np.array(ratings), np.array(ages), np.array(weights), curve, q=0.09, obs_base=1.0
    )
    # filtered final skill is closer to the true latent than the last noisy observation
    assert abs(s_final - latents[-1]) < abs(ratings[-1] - latents[-1]) + 0.5
    proj, sd = _forecast(s_final, P_final, ages[-1], curve, q=0.09)
    assert sd > 0 and abs(proj - latents[-1]) < 2.0


def test_fit_noise_params_positive_deterministic() -> None:
    """MLE fit returns positive params and is deterministic (no RNG)."""
    panel, ages = _panel()
    curve = fit_aging_curve(panel, ages, smooth=1)
    q1, o1 = _fit_noise_params(panel, ages, curve)
    q2, o2 = _fit_noise_params(panel, ages, curve)
    assert q1 > 0 and o1 > 0 and q1 == q2 and o1 == o2


def test_nba_darko_projection_frame() -> None:
    """nba_darko returns the 6-col schema with correct dtypes, forecast_season, and row count."""
    panel, ages = _panel()
    out = nba_darko(panel, ages)
    assert set(out.columns) == {
        "player_id",
        "last_season",
        "forecast_season",
        "filtered_skill",
        "projected_rating",
        "projected_sd",
    }
    assert out.schema["player_id"] == pl.Int64 and out.schema["projected_sd"] == pl.Float64
    assert (out["forecast_season"] == out["last_season"] + 1).all()
    assert (out["projected_sd"] > 0).all() and out.height == 60


# ---------------------------------------------------------------------------
# Task 5 — meta-oracle helpers
# ---------------------------------------------------------------------------


def _skill_panel(seed: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """80 players × 6 seasons with persistent latent skill + small drift + obs noise."""
    rng = np.random.default_rng(seed)
    rows, arows = [], []
    for pid in range(80):
        skill = float(rng.normal(0, 4))
        start = int(rng.integers(23, 29))
        for k, season in enumerate(range(2017, 2023)):
            rows.append({"player_id": pid, "season": season, "rating": skill + rng.normal(0, 0.7), "weight": 1.0})
            arows.append({"player_id": pid, "season": season, "age": float(start + k)})
            skill += rng.normal(0, 0.2)
    return pl.DataFrame(rows), pl.DataFrame(arows)


def _noise_panel(seed: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """80 players × 6 seasons of pure i.i.d. noise — no persistent skill."""
    rng = np.random.default_rng(seed)
    rows, arows = [], []
    for pid in range(80):
        start = int(rng.integers(23, 29))
        for k, season in enumerate(range(2017, 2023)):
            rows.append({"player_id": pid, "season": season, "rating": float(rng.normal(0, 4)), "weight": 1.0})
            arows.append({"player_id": pid, "season": season, "age": float(start + k)})
    return pl.DataFrame(rows), pl.DataFrame(arows)


def test_forecast_beats_baseline_on_skill_panel() -> None:
    """Meta-oracle (skill panel): Kalman projection beats carry-forward and achieves corr > 0.5."""
    panel, ages = _skill_panel(3)
    res = darko_forecast_accuracy(panel, ages)
    assert res.n_forecasts > 0
    assert res.forecast_rmse < res.baseline_rmse  # projection beats carry-forward
    assert res.forecast_corr > 0.5


def test_forecast_does_not_beat_baseline_on_noise_panel() -> None:
    """Meta-oracle (noise panel): pure noise ratings -> projection cannot meaningfully beat flat baseline."""
    panel, ages = _noise_panel(3)
    res = darko_forecast_accuracy(panel, ages)
    # no persistent skill -> projection must NOT beat the carry-forward by more than 0.15
    assert res.forecast_rmse >= res.baseline_rmse - 0.15


# ---------------------------------------------------------------------------
# Task 6 — gated live smoke test
# ---------------------------------------------------------------------------

from tests.conftest import skip_if_no_nba_stats_live  # noqa: E402


def test_nba_darko_is_bit_deterministic() -> None:
    """fit_aging_curve mean rounding makes nba_darko bit-for-bit reproducible across calls."""
    import numpy as np
    import polars as pl
    from sportsdataverse.nba.nba_darko import nba_darko

    rng = np.random.default_rng(0)
    rows, arows = [], []
    for pid in range(50):
        skill = float(rng.normal(0, 3))
        start = int(rng.integers(23, 30))
        for k, season in enumerate(range(2018, 2023)):
            rows.append({"player_id": pid, "season": season, "rating": skill + rng.normal(0, 0.8), "weight": 1.0})
            arows.append({"player_id": pid, "season": season, "age": float(start + k)})
            skill += rng.normal(0, 0.3)
    panel, ages = pl.DataFrame(rows), pl.DataFrame(arows)
    assert nba_darko(panel, ages).equals(nba_darko(panel, ages))  # bit-for-bit reproducible


@skip_if_no_nba_stats_live
def test_darko_live_smoke() -> None:
    """Live smoke: build a 2-season panel from nba_rapm + nba_player_ages and run nba_darko."""
    import polars as pl

    from sportsdataverse.nba import compile_nba_season, nba_darko, nba_player_ages
    from sportsdataverse.nba.nba_rapm import nba_rapm

    frames = []
    for season, yr in (("2022-23", 2022), ("2023-24", 2023)):
        r = (
            nba_rapm(compile_nba_season(yr))
            .select(
                pl.col("player_id"),
                pl.col("rapm").alias("rating"),
                (pl.col("off_poss") + pl.col("def_poss")).alias("weight"),
            )
            .with_columns(pl.lit(yr).alias("season"))
        )
        frames.append(r)
    panel = pl.concat(frames)
    ages = pl.concat(
        [
            nba_player_ages(s).with_columns(pl.lit(yr).alias("season"))
            for s, yr in (("2022-23", 2022), ("2023-24", 2023))
        ]
    )
    out = nba_darko(panel, ages)
    assert out.height > 0 and set(out.columns) == {
        "player_id",
        "last_season",
        "forecast_season",
        "filtered_skill",
        "projected_rating",
        "projected_sd",
    }
