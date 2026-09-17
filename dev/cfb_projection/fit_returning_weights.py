"""Fit returning-production unit weights (T2.2 Task 2.3; refit 2026-09-17).

Regresses realized YoY scoring-margin change on standardized off/def returning
production (FBS, >=6 games both seasons) and sets ``returning_prod_weights``
from the non-negative standardized coefficients. The shipped FBS weights are the
pooled 2018-2025 fit on ``returning_2005_2025.parquet`` (defense from play
participants from 2015; 2005-2014 is the pbp-splash era, kept out of the fit and
used as a held-out check). ``tests/cfb/test_cfb_projection_backtest.py`` imports
this module, so the gates and the fit share one definition of the target, the join
and the fit. ``__main__`` prints the fits, the gate values and the team-cluster
bootstrap intervals the gate docstrings quote.
Run from the repo root:  uv run python dev/cfb_projection/fit_returning_weights.py
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl

from sportsdataverse.cfb.cfb_projection_constants import spearman_corr
from sportsdataverse.cfb.cfb_returning_production import _combine_units

FIX = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "cfb_projection"


def gate_frame(returning: pl.DataFrame, results: pl.DataFrame) -> pl.DataFrame:
    """FBS team-seasons with returning production joined to YoY scoring-margin change."""
    home = results.select(
        "season", pl.col("home_team_id").alias("team_id"), (pl.col("home_score") - pl.col("away_score")).alias("m")
    )
    away = results.select(
        "season", pl.col("away_team_id").alias("team_id"), (pl.col("away_score") - pl.col("home_score")).alias("m")
    )
    margins = (
        pl.concat([home, away])
        .group_by("season", "team_id")
        .agg(pl.col("m").cast(pl.Float64).mean().alias("avg_margin"), pl.len().alias("g"))
    )
    prior = margins.with_columns((pl.col("season") + 1).alias("season")).rename(
        {"avg_margin": "prior_margin", "g": "prior_g"}
    )
    delta = (
        margins.join(prior, on=["season", "team_id"], how="inner")
        .filter((pl.col("g") >= 6) & (pl.col("prior_g") >= 6))
        .with_columns((pl.col("avg_margin") - pl.col("prior_margin")).alias("margin_delta"))
    )
    fbs = returning.filter(pl.col("is_fbs") == True).drop_nulls(["team_id", "off_returning"])
    assert fbs.schema["team_id"] == delta.schema["team_id"] == pl.Utf8
    return fbs.join(delta, on=["season", "team_id"], how="inner")


def fit_weights(frame: pl.DataFrame, first: int, last: int) -> tuple[dict[str, float], int]:
    """Non-negative standardized OLS coefficients, normalized to sum to 1, over ``first..last``."""
    k = frame.filter(pl.col("season").is_between(first, last)).drop_nulls(["def_returning"])
    x = np.column_stack([k["off_returning"].to_numpy(), k["def_returning"].to_numpy()])
    x = (x - x.mean(axis=0)) / x.std(axis=0)
    coef, *_ = np.linalg.lstsq(np.column_stack([np.ones(k.height), x]), k["margin_delta"].to_numpy(), rcond=None)
    w = np.maximum(coef[1:], 0.0)
    w = w / w.sum()
    return {"offense": float(w[0]), "defense": float(w[1])}, k.height


def paired_bootstrap(
    frame: pl.DataFrame,
    weights: dict[str, float],
    first: int,
    last: int,
    *,
    n_boot: int = 2000,
    seed: int = 20260917,
) -> tuple[float, float, float]:
    """Gain in rho of ``weights`` over offense-only, with a 95% team-cluster bootstrap interval.

    Resamples PROGRAMS, not team-seasons: one program's seasons are not
    independent, and a row-level bootstrap would be too narrow.
    """
    overall, _ = _combine_units(weights)
    k = frame.filter(pl.col("season").is_between(first, last)).with_columns(overall.alias("overall_w"))
    teams = k["team_id"].to_numpy()
    rows = {t: np.flatnonzero(teams == t) for t in np.unique(teams)}
    ids = np.array(list(rows))
    new, base, y = k["overall_w"].to_numpy(), k["off_returning"].to_numpy(), k["margin_delta"].to_numpy()
    rng = np.random.default_rng(seed)
    gains = []
    for _ in range(n_boot):
        idx = np.concatenate([rows[t] for t in rng.choice(ids, len(ids))])
        gains.append(spearman_corr(new[idx], y[idx]) - spearman_corr(base[idx], y[idx]))
    lo, hi = np.percentile(gains, [2.5, 97.5])
    return spearman_corr(new, y) - spearman_corr(base, y), float(lo), float(hi)


def overall_rho(frame: pl.DataFrame, weights: dict[str, float], first: int, last: int) -> tuple[float, int]:
    """spearman(overall_returning under ``weights``, margin_delta) over ``first..last``, every FBS team."""
    overall, _ = _combine_units(weights)
    k = frame.filter(pl.col("season").is_between(first, last)).with_columns(overall.alias("overall_w"))
    return spearman_corr(k["overall_w"].to_numpy(), k["margin_delta"].to_numpy()), k.height


if __name__ == "__main__":
    j = gate_frame(
        pl.read_parquet(FIX / "returning_2005_2025.parquet"), pl.read_parquet(FIX / "results_2004_2025.parquet")
    )
    print(
        j.group_by("season")
        .agg(
            pl.len().alias("fbs"),
            pl.col("def_returning").is_not_null().mean().round(3).alias("def_share"),
            pl.col("def_basis").drop_nulls().unique().implode().alias("def_basis"),
            pl.col("off_returning").mean().round(3).alias("off_mean"),
            pl.col("def_returning").mean().round(3).alias("def_mean"),
        )
        .sort("season")
    )
    offense = {"offense": 1.0, "defense": 0.0}
    fits = {}
    for first, last in ((2018, 2023), (2018, 2025)):
        fits[first, last], n = fit_weights(j, first, last)
        w = fits[first, last]
        print(f"fit {first}-{last}: n={n}  offense={w['offense']:.3f} defense={w['defense']:.3f}")
    shipped = {k: round(v, 2) for k, v in fits[2018, 2025].items()}
    for label, w, first, last in (
        ("shipped, 2018-2025 (in-sample)", shipped, 2018, 2025),
        ("shipped, 2018-2023 (in-sample)", shipped, 2018, 2023),
        ("2018-2023 fit, held-out 2024-2025", fits[2018, 2023], 2024, 2025),
        ("shipped, held-out splash era 2005-2014", shipped, 2005, 2014),
    ):
        rho, n = overall_rho(j, w, first, last)
        base, _ = overall_rho(j, offense, first, last)
        gain, lo, hi = paired_bootstrap(j, w, first, last)
        print(f"{label}: n={n} rho={rho:.4f} offense-only={base:.4f} gain={gain:+.4f} [{lo:+.4f}, {hi:+.4f}]")
