"""Post-game win expectancy (deserved-win probability) from drive-EP deltas.

Given a completed game's per-drive EP deltas for both offenses, estimate
the probability that the home team wins a COUNTERFACTUAL replay in which
each side's drive outcomes are redrawn from its own observed drives — a
"deserved win" probability that strips single-drive luck while keeping
each team's actual performance distribution. Two estimators per the
plan's post-game track:

- G3 ``analytic``: normal approximation — the resampled margin is
  ``N(sum_h - sum_a, n_h * var_h + n_a * var_a)``; ``pg_we`` is the
  probability it exceeds zero.
- G2 ``resample``: drive bootstrap — redraw each side's ``n`` drives with
  replacement ``n_boot`` times; ``pg_we`` is the fraction of resampled
  margins above zero plus half the ties.

G1 (play-level turnover-opportunity bootstrap) needs play-level state and
lands with the play-capture arm. Validation (luck_delta -> future
regression) lives in the dev harness and its numbers in the plan ledger.
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import polars as pl

__all__ = ["postgame_we"]


def postgame_we(
    drives: pl.DataFrame,
    games: pl.DataFrame,
    *,
    method: str = "analytic",
    n_boot: int = 2000,
    seed: Optional[int] = 0,
) -> pl.DataFrame:
    """Deserved-win probability per game from per-drive EP deltas.

    Args:
        drives: Per-drive frame from
            :func:`~sportsdataverse.wexp.engines.cfb_drive_deltas`
            (``game_id``, ``off``, ``delta``; raw Int64 ids).
        games: Frame with ``game_id`` (Utf8) and ``home_team_id`` (Utf8)
            — the oracle frame works directly.
        method: ``"analytic"`` (G3 normal) or ``"resample"`` (G2 drive
            bootstrap).
        n_boot: Bootstrap draws per game (resample method).
        seed: RNG seed for the bootstrap (fixed default: reproducible).

    Returns:
        One row per game with both sides observed: ``game_id`` (Utf8),
        ``pg_we`` (home-perspective deserved-win probability),
        ``perf_margin`` (sum of home drive deltas minus away's),
        ``n_drives_home`` / ``n_drives_away``. Games missing a side are
        dropped, never imputed.

    Raises:
        ValueError: On an unknown ``method``.

    Example:
        Quick start::

            from sportsdataverse.wexp.engines import cfb_drive_deltas
            from sportsdataverse.wexp.postgame import postgame_we
            we = postgame_we(cfb_drive_deltas(pbp), oracle)

        Drive bootstrap instead of the normal approximation::

            we_boot = postgame_we(cfb_drive_deltas(pbp), oracle, method="resample")
    """
    if method not in ("analytic", "resample"):
        raise ValueError(f"unknown method {method!r}; one of ('analytic', 'resample')")
    d = drives.select(
        game_id=pl.col("game_id").cast(pl.Int64).cast(pl.Utf8),
        off=pl.col("off").cast(pl.Int64).cast(pl.Utf8),
        delta=pl.col("delta"),
    ).join(games.select("game_id", "home_team_id"), on="game_id", how="inner")
    d = d.with_columns(is_home=pl.col("off") == pl.col("home_team_id"))

    if method == "analytic":
        from scipy.stats import norm

        sides = d.group_by("game_id", "is_home").agg(s=pl.col("delta").sum(), v=pl.col("delta").var(ddof=1), n=pl.len())
        home = sides.filter(pl.col("is_home") == True)  # noqa: E712
        away = sides.filter(pl.col("is_home") == False)  # noqa: E712
        out = home.join(away, on="game_id", how="inner", suffix="_a")
        mean = (out["s"] - out["s_a"]).to_numpy()
        var = (out["n"] * out["v"].fill_null(0.0) + out["n_a"] * out["v_a"].fill_null(0.0)).to_numpy()
        with np.errstate(divide="ignore", invalid="ignore"):
            z = np.where(var > 0, mean / np.sqrt(var), np.inf * np.sign(mean))
        we = norm.cdf(z)
        we = np.where(np.isnan(we), 0.5, we)  # zero variance AND zero mean
        return pl.DataFrame(
            {
                "game_id": out["game_id"],
                "pg_we": we,
                "perf_margin": mean,
                "n_drives_home": out["n"].cast(pl.Int64),
                "n_drives_away": out["n_a"].cast(pl.Int64),
            }
        ).sort("game_id")

    rng = np.random.default_rng(seed)
    records: list[tuple[str, float, float, int, int]] = []
    for (game_id,), group in d.group_by(["game_id"], maintain_order=True):
        h = group.filter(pl.col("is_home") == True)["delta"].to_numpy()  # noqa: E712
        a = group.filter(pl.col("is_home") == False)["delta"].to_numpy()  # noqa: E712
        if len(h) == 0 or len(a) == 0:
            continue
        hs = rng.choice(h, size=(n_boot, len(h)), replace=True).sum(axis=1)
        as_ = rng.choice(a, size=(n_boot, len(a)), replace=True).sum(axis=1)
        margins = hs - as_
        we = float(((margins > 0).sum() + 0.5 * (margins == 0).sum()) / n_boot)
        records.append((str(game_id), we, float(h.sum() - a.sum()), len(h), len(a)))
    return pl.DataFrame(
        records,
        schema={
            "game_id": pl.Utf8,
            "pg_we": pl.Float64,
            "perf_margin": pl.Float64,
            "n_drives_home": pl.Int64,
            "n_drives_away": pl.Int64,
        },
        orient="row",
    ).sort("game_id")
