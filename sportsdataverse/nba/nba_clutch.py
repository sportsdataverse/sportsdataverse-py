"""Clutch-performance model (model ⑤) for the NBA prediction stack.

Computes a team's clutch net-rating delta (clutch-window net minus full-game
net) and applies empirical-Bayes / James-Stein shrinkage toward zero, so the
output is a defensible "clutch skill" estimate rather than raw small-sample
noise. The clutch endpoint *is* the raw clutch data, so any in-sample fit is
circular -- the honest gate is out-of-sample (does season-N shrunk skill
forecast season-(N+1) clutch net rating; see ``tests/nba/test_nba_clutch.py``).

Baseline choice (ponytail): the full-game baseline is the stats.nba.com
full-game ``net_rating`` (``nba_stats_leaguedashteamstats``), NOT the ESPN
``nba_team_ratings`` AdjNet. stats.nba.com and ESPN use unrelated team-id
systems (10-digit franchise id vs small int), and the clutch feed is
stats.nba.com-keyed; using the stats full-game net keeps the delta a
single-id, like-with-like (both un-opponent-adjusted) subtraction and avoids a
two-id crosswalk. `# ponytail: stats full-game net is the natural baseline for
a stats-keyed clutch feed; the ESPN AdjNet engine would only add a crosswalk +
an opp-adjustment mismatch.`

Observed out-of-sample result (2026-07-08, 2022-23 shrunk skill vs 2023-24
clutch net, 30-team intersection): **this is a documented NULL result.**
``var(clutch_delta) ≈ 49.6`` equals the direct net-rating sampling variance at
~315 clutch possessions, so ``τ² ≈ 0`` -- clutch-over-baseline skill is
statistically indistinguishable from noise at this sample. The model therefore
shrinks hard toward zero (``sum|shrunk| ≈ 1.4`` vs ``sum|raw| ≈ 161``) and the
cross-season Spearman is only ``ρ ≈ 0.10`` (in the null band). Per Decision 7 /
Task 4.3 we keep the shrinkage and report the null -- we do NOT invent a signal.
"""

from __future__ import annotations

from typing import Literal, Union, overload

import numpy as np
import pandas as pd
import polars as pl

__all__ = ["clutch_delta", "nba_team_clutch", "shrink_clutch"]

# Per-team clutch-net sampling-variance scale (points²·possessions), FITTED
# (dev/nba_prediction/fit_clutch_shrinkage.py, 2026-07-08). The direct net-rating
# SE over n possessions is σ² = (100·√2·sd_ppp)² / n; with the NBA points-per-poss
# SD sd_ppp≈0.9 this gives scale = (100·√2·0.9)² ≈ 16200, i.e. σ²≈49 at ~315 clutch
# poss. Crucially, the observed between-team variance of clutch_delta is ALSO ≈49.6 --
# so τ² = var(delta) − mean(σ²) ≈ 0: at a few-hundred clutch possessions, the spread
# in clutch-over-baseline performance is statistically indistinguishable from sampling
# noise. This is the honest empirical-Bayes NULL (Decision 7 / Task 4.3): the shrinkage
# collapses clutch_skill_shrunk toward ~0 for every team -- we do NOT invent a signal.
# The season-to-season reliability (r=0.36) corroborates it. See the module docstring.
_CLUTCH_SIGMA2_SCALE = 16200.0

_DELTA_SCHEMA = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "clutch_net_rating": pl.Float64,
    "adj_net_rtg": pl.Float64,
    "clutch_delta": pl.Float64,
    "clutch_poss": pl.Float64,
}


def clutch_delta(clutch: pl.DataFrame, ratings: pl.DataFrame) -> pl.DataFrame:
    """Clutch net-rating delta vs a full-game baseline, per (season, team_id).

    ``clutch_delta = clutch_net_rating - adj_net_rtg``. Joins ``clutch`` to the
    baseline ``ratings`` frame on ``(season, team_id)`` (asserting dtype
    agreement first).

    Args:
        clutch: Frame with ``season, team_id, clutch_net_rating, clutch_poss``.
        ratings: Full-game baseline with ``season, team_id, adj_net_rtg`` (the
            stats full-game net, or any per-team baseline).

    Returns:
        One row per matched (season, team_id): ``season, team_id,
        clutch_net_rating, adj_net_rtg, clutch_delta, clutch_poss``. Empty input
        returns that schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_clutch import clutch_delta
            d = clutch_delta(clutch_frame, baseline_frame)
    """
    if clutch.height == 0 or ratings.height == 0:
        return pl.DataFrame(schema=_DELTA_SCHEMA)
    for key in ("season", "team_id"):
        if clutch.schema[key] != ratings.schema[key]:
            raise ValueError(
                f"join-key dtype mismatch on {key!r}: clutch is {clutch.schema[key]} "
                f"but ratings is {ratings.schema[key]}"
            )
    return (
        clutch.join(ratings.select("season", "team_id", "adj_net_rtg"), on=["season", "team_id"], how="inner")
        .with_columns((pl.col("clutch_net_rating") - pl.col("adj_net_rtg")).alias("clutch_delta"))
        .select("season", "team_id", "clutch_net_rating", "adj_net_rtg", "clutch_delta", "clutch_poss")
        .sort("team_id")
    )


def shrink_clutch(delta: pl.DataFrame, *, league_id: str = "00") -> pl.DataFrame:
    """Empirical-Bayes / James-Stein shrinkage of ``clutch_delta`` toward zero.

    Per-team sampling variance is ``σ²_i = scale / clutch_poss`` (small samples
    shrink harder); the between-team signal variance ``τ²`` is the observed
    variance of ``clutch_delta`` net of mean sampling variance; the shrink
    factor ``k_i = τ² / (τ² + σ²_i)`` and ``clutch_skill_shrunk = k_i · delta_i``.

    Args:
        delta: Output of :func:`clutch_delta` (needs ``clutch_delta`` +
            ``clutch_poss``).
        league_id: ``"00"``/``"10"``/``"20"`` (accepted for parity; the scale is
            currently league-shared).

    Returns:
        ``delta`` with an added ``clutch_skill_shrunk`` column. Empty input
        returns the input schema plus that column.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_clutch import clutch_delta, shrink_clutch
            skill = shrink_clutch(clutch_delta(clutch_frame, baseline_frame))
    """
    if delta.height == 0:
        return delta.with_columns(pl.lit(None, dtype=pl.Float64).alias("clutch_skill_shrunk"))
    d = delta["clutch_delta"].to_numpy().astype(float)
    poss = np.clip(delta["clutch_poss"].to_numpy().astype(float), 1.0, None)
    sig2 = _CLUTCH_SIGMA2_SCALE / poss
    tau2 = max(float(np.var(d) - np.mean(sig2)), 1e-6)
    k = tau2 / (tau2 + sig2)
    return delta.with_columns(pl.Series("clutch_skill_shrunk", k * d).cast(pl.Float64))


def _load_clutch(season: int, league_id: str) -> pl.DataFrame:  # pragma: no cover - live network
    """Live clutch net rating (monkeypatched to fixtures in tests)."""
    from sportsdataverse.nba.nba_stats import nba_stats_leaguedashteamclutch  # noqa: PLC0415

    season_str = f"{season - 1}-{str(season)[2:]}"
    raw = nba_stats_leaguedashteamclutch(
        season=season_str, measure_type_detailed_defense="Advanced", league_id=league_id
    )
    if isinstance(raw, dict):
        raw = next(iter(raw.values()))
    return raw.select(
        pl.lit(season, dtype=pl.Int64).alias("season"),
        pl.col("team_id").cast(pl.Int64, strict=False).cast(pl.Utf8),
        pl.col("net_rating").cast(pl.Float64).alias("clutch_net_rating"),
        pl.col("poss").cast(pl.Float64).alias("clutch_poss"),
    )


def _load_full_game_net(season: int, league_id: str) -> pl.DataFrame:  # pragma: no cover - live network
    """Live full-game net rating baseline (monkeypatched to fixtures in tests)."""
    from sportsdataverse.nba.nba_stats import nba_stats_leaguedashteamstats  # noqa: PLC0415

    season_str = f"{season - 1}-{str(season)[2:]}"
    raw = nba_stats_leaguedashteamstats(
        season=season_str, measure_type_detailed_defense="Advanced", league_id=league_id
    )
    if isinstance(raw, dict):
        raw = next(iter(raw.values()))
    return raw.select(
        pl.lit(season, dtype=pl.Int64).alias("season"),
        pl.col("team_id").cast(pl.Int64, strict=False).cast(pl.Utf8),
        pl.col("net_rating").cast(pl.Float64).alias("adj_net_rtg"),
    )


@overload
def nba_team_clutch(
    season: int, *, league_id: str = "00", return_as_pandas: Literal[False] = False
) -> pl.DataFrame: ...


@overload
def nba_team_clutch(season: int, *, league_id: str = "00", return_as_pandas: Literal[True]) -> pd.DataFrame: ...


def nba_team_clutch(
    season: int, *, league_id: str = "00", return_as_pandas: bool = False
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Opponent-agnostic clutch skill (shrunk clutch net-rating delta) per team.

    Loads the season's clutch net rating (``nba_stats_leaguedashteamclutch``)
    and full-game net baseline (``nba_stats_leaguedashteamstats``), computes
    :func:`clutch_delta`, and applies :func:`shrink_clutch`.

    Args:
        season: End year of the season (e.g. ``2024`` for 2023-24).
        league_id: ``"00"`` NBA / ``"10"`` WNBA / ``"20"`` G-League.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per team: ``season, team_id, clutch_net_rating, adj_net_rtg,
        clutch_delta, clutch_skill_shrunk, clutch_poss``. Empty input returns
        that schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_clutch import nba_team_clutch
            skill = nba_team_clutch(2024)
            skill.sort("clutch_skill_shrunk", descending=True).head()
    """
    clutch = _load_clutch(season, league_id)
    net = _load_full_game_net(season, league_id)
    delta = clutch_delta(clutch, net)
    out = shrink_clutch(delta, league_id=league_id).select(
        "season", "team_id", "clutch_net_rating", "adj_net_rtg", "clutch_delta", "clutch_skill_shrunk", "clutch_poss"
    )
    return out.to_pandas() if return_as_pandas else out
