"""Four-factor RAPM split (BB-D10): the shared core behind ``{lg}_player_factor_rapm``.

Plain RAPM (:func:`sportsdataverse.nba.nba_rapm.nba_rapm`) regresses each possession's points on
the ten players on the floor. This module splits those points into Dean Oliver's four factors and
fits the same ridge to each part, offense and defense, so every player gets eight components:

* ``o_efg_rapm`` / ``d_efg_rapm`` -- shooting: field-goal points over the league's points per
  field-goal attempt ``v`` times the attempts (``2*fg2m + 3*fg3m - v*fga``), the eFG% factor;
* ``o_tov_rapm`` / ``d_tov_rapm`` -- turnovers: ``-v`` per turnover, a lost shot;
* ``o_orb_rapm`` / ``d_drb_rapm`` -- offensive rebounding (on defense, defensive rebounding):
  ``+v`` per offensive rebound, an extra shot;
* ``o_ftr_rapm`` / ``d_ftr_rapm`` -- free throws: the possession's other points (free throws)
  less ``v`` for each chance that ended in neither a shot nor a turnover
  (``1 + oreb - tov - fga``), the FT-rate factor (it carries FT% too). It also takes the
  possession's bookkeeping remainder: an empty possession (a period running out before a shot)
  costs ``-v`` here, and one with an unrecorded offensive rebound (more shots and turnovers than
  chances) gains ``+v``: 1.5% of the 2024 and of the 2025 WNBA regular-season possessions.

``v`` is field-goal points per field-goal attempt over the season's possessions: a turnover is
valued as the shot it lost, not as a possession's worth of points (Oliver's weights would value it
nearer points per possession). Any ``v`` keeps the identity below; it moves value between factors.

The four parts add up to ``points - v`` on every possession, whatever ``v`` is, and a ridge fit is
linear in its response: fitting all five responses (points and the four parts) at the one penalty
picked for points makes ``o_efg + o_tov + o_orb + o_ftr`` equal ``o_rapm`` exactly (and the same on
defense), up to floating point. That penalty is the best one for points, not for each factor, so a
noisy factor (opponent shooting, free throws) is shrunk less than its own cross-validation would. ``o_rapm`` / ``d_rapm`` are the plain-RAPM fit of
:func:`~sportsdataverse.nba.nba_rapm.nba_rapm` on the same possessions: the penalty is its
:class:`~sklearn.linear_model.RidgeCV` pick over the same grid.

Every component is per 100 possessions, sign-flipped on defense (``d_* = -100 * coef``) so higher
is better throughout: a positive ``d_tov_rapm`` forces turnovers, a positive ``d_drb_rapm`` keeps
the opponent off the offensive glass. Shrinkage is toward zero (no box prior), so the split does
not carry the prior offset of the adjusted RAPM (``o_adj_rapm`` / ``d_adj_rapm``).

Each value carries ``<m>_se``, a standard error clustered by game: the ridge sandwich
``(G + aI)^-1 M (G + aI)^-1`` with ``G`` the centred Gram matrix and ``M`` the sum over games of the
outer product of each game's residual score. It measures sampling noise around the shrunk estimate,
not the shrinkage bias, so a component whose ``_se`` is near its spread across players is noise
(year over year, WNBA 2024 to 2025, the weakest component, ``d_efg_rapm``, repeats at Spearman 0.13).

The "six-factor" name of roadmap item BB-D10 comes from the three-factor-per-side split (free
throws folded into shooting): ``o_efg_rapm + o_ftr_rapm`` is that shooting factor.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

import numpy as np
import polars as pl
from scipy.sparse import csr_matrix
from sklearn.linear_model import RidgeCV

from sportsdataverse._common.league_context import MIN_COHORT_PLAYERS, attach_ranks, attach_sample_sizes
from sportsdataverse.nba.nba_rapm import DEFAULT_RAPM_ALPHAS, build_rapm_design

__all__ = ["COUNTS", "FACTORS", "factor_rapm", "factor_rapm_schema", "factor_targets"]

#: The possession counts the split reads (the ``nba_stats_possessions`` columns).
COUNTS = ("points", "fg2a", "fg2m", "fg3a", "fg3m", "oreb", "tov")
#: The cluster key of the standard errors.
GAME = "game_id"
#: The four factors, in output order.
FACTORS = ("efg", "tov", "orb", "ftr")
_SLOTS = [f"{side}_player_{i}" for side in ("off", "def") for i in range(1, 6)]
#: Factor -> its defensive column (the defensive side of offensive rebounding is defensive rebounding).
_DEF_NAME = {"efg": "d_efg_rapm", "tov": "d_tov_rapm", "orb": "d_drb_rapm", "ftr": "d_ftr_rapm"}
_OFF_COLS = ["o_rapm", *[f"o_{f}_rapm" for f in FACTORS]]
_DEF_COLS = ["d_rapm", *[_DEF_NAME[f] for f in FACTORS]]
#: Every ranked metric (all higher-is-better) -> its sample size.
_N = {
    **{c: pl.col("off_poss") for c in _OFF_COLS},
    **{c: pl.col("def_poss") for c in _DEF_COLS},
    "rapm": pl.col("off_poss") + pl.col("def_poss"),
}
_METRICS = ["rapm", "o_rapm", "d_rapm", *_OFF_COLS[1:], *_DEF_COLS[1:]]


def factor_rapm_schema(player_dtype: pl.DataType) -> dict[str, pl.DataType]:
    """Columns of :func:`factor_rapm` for a league whose player ids are ``player_dtype``."""
    return {
        "season": pl.Int64(),
        "player_id": player_dtype,
        "off_poss": pl.Int64(),
        "def_poss": pl.Int64(),
        **dict.fromkeys(_METRICS, pl.Float64()),
        **{f"{m}_{s}": pl.Float64() for m in _METRICS for s in ("rank", "pct")},
        **{f"{m}_se": pl.Float64() for m in _METRICS},
        **{f"{m}_n": pl.Int64() for m in _METRICS},
        "pts_per_fga": pl.Float64(),
        "ridge_alpha": pl.Float64(),
    }


def factor_targets(possessions: pl.DataFrame, pts_per_fga: float) -> pl.DataFrame:
    """Split each possession's points into the four factor parts.

    Args:
        possessions: Rows carrying :data:`COUNTS`.
        pts_per_fga: ``v``, the value of one field-goal attempt (the sample's points per attempt).

    Returns:
        ``points`` and one Float64 column per factor (:data:`FACTORS`), whose sum is
        ``points - pts_per_fga`` on every row.
    """
    v = pts_per_fga
    fga = pl.col("fg2a") + pl.col("fg3a")
    fg_pts = 2 * pl.col("fg2m") + 3 * pl.col("fg3m")
    return possessions.select(
        pl.col("points").cast(pl.Float64),
        (fg_pts - v * fga).cast(pl.Float64).alias("efg"),
        (-v * pl.col("tov")).cast(pl.Float64).alias("tov"),
        (v * pl.col("oreb")).cast(pl.Float64).alias("orb"),
        ((pl.col("points") - fg_pts) - v * (1 + pl.col("oreb") - pl.col("tov") - fga)).cast(pl.Float64).alias("ftr"),
    )


def _ridge(X: csr_matrix, Y: np.ndarray, alpha: float, games: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """The intercept-fitted ridge of every column of ``Y`` at one ``alpha``, solved exactly, with game-clustered SEs.

    Centring the sparse design is done on its Gram matrix (``X'X - n m m'``), so the solve is one
    dense ``2P x 2P`` system for all responses: no iterative solver, whose tolerance would break the
    sum of the parts.

    Args:
        X: The ``(n, 2P)`` offense-then-defense design.
        Y: The ``(n, k)`` responses.
        alpha: The ridge penalty.
        games: The ``(n,)`` integer game code of each row (the clusters).

    Returns:
        ``(coef, var, cov)``: the ``(2P, k)`` coefficients, their ``(2P, k)`` sandwich variances and
        the ``(P, k)`` covariance of each player's offensive and defensive coefficient.
    """
    n = X.shape[0]
    P = X.shape[1] // 2
    mean = np.asarray(X.mean(axis=0)).ravel()
    gram = (X.T @ X).toarray() - n * np.outer(mean, mean)
    gram[np.diag_indices_from(gram)] += alpha
    inv = np.linalg.inv(gram)
    coef = inv @ (X.T @ Y - n * np.outer(mean, Y.mean(axis=0)))
    resid = (Y - Y.mean(axis=0)) - (X @ coef - mean @ coef)
    by_game = csr_matrix((np.ones(n), (games, np.arange(n))))
    var = np.empty_like(coef)
    cov = np.empty((P, Y.shape[1]))
    for k in range(Y.shape[1]):
        # each game's score of the centred design, (x_i - m) * e_i summed over its rows
        score = np.asarray(by_game @ X.multiply(resid[:, [k]]).tocsr().toarray()) - np.outer(
            by_game @ resid[:, k], mean
        )
        lever = inv @ score.T  # (2P, games)
        var[:, k] = (lever**2).sum(axis=1)
        cov[:, k] = (lever[:P] * lever[P:]).sum(axis=1)
    return coef, var, cov


def _fit_season(possessions: pl.DataFrame, alphas: np.ndarray) -> pl.DataFrame | None:
    """One season's split: totals and the eight components per player, or ``None`` with no possessions."""
    poss = possessions.drop_nulls([*_SLOTS, *COUNTS, GAME])
    X, points, player_ids = build_rapm_design(poss)
    if not player_ids:
        return None
    fga = poss.select((pl.col("fg2a") + pl.col("fg3a")).sum()).item()
    fg_pts = poss.select((2 * pl.col("fg2m") + 3 * pl.col("fg3m")).sum()).item()
    v = fg_pts / fga if fga else 0.0
    Y = factor_targets(poss, v).to_numpy()
    # The penalty nba_rapm would pick (RidgeCV's leave-one-out over the same grid; one possession
    # cannot be cross-validated, so it takes the strongest), then every response at that penalty.
    alpha = float(RidgeCV(alphas=alphas, fit_intercept=True).fit(X, points).alpha_) if X.shape[0] > 1 else max(alphas)
    games = poss[GAME].cast(pl.Utf8).rank("dense").cast(pl.Int64).to_numpy() - 1
    coef, var, cov = _ridge(X, Y, alpha, games)
    P = len(player_ids)
    poss_counts = np.asarray(X.sum(axis=0)).ravel()
    cols: dict[str, np.ndarray] = {}
    for k, (o, d) in enumerate(zip(_OFF_COLS, _DEF_COLS)):
        cols[o] = 100 * coef[:P, k]
        cols[d] = -100 * coef[P:, k]
        cols[f"{o}_se"] = 100 * np.sqrt(var[:P, k])
        cols[f"{d}_se"] = 100 * np.sqrt(var[P:, k])
    # rapm = 100 * (b_off - b_def): its variance takes the covariance of the two
    cols["rapm_se"] = 100 * np.sqrt(np.maximum(var[:P, 0] + var[P:, 0] - 2 * cov[:, 0], 0.0))
    return pl.DataFrame(
        {
            "player_id": player_ids,
            "off_poss": poss_counts[:P].astype(np.int64),
            "def_poss": poss_counts[P:].astype(np.int64),
            **cols,
        }
    ).with_columns(
        (pl.col("o_rapm") + pl.col("d_rapm")).alias("rapm"),
        pl.lit(v).alias("pts_per_fga"),
        pl.lit(alpha).alias("ridge_alpha"),
    )


def factor_rapm(
    possessions: pl.DataFrame,
    *,
    player_dtype: pl.DataType,
    min_poss: int,
    alphas: np.ndarray = DEFAULT_RAPM_ALPHAS,
) -> pl.DataFrame:
    """Per player and season: plain RAPM and its four-factor split, offense and defense.

    Each season is fit on its own. A possession with a null lineup slot or count is left out.

    Args:
        possessions: ``nba_stats_possessions``-shaped rows: ``season``, ``game_id``,
            ``off_player_1..5``, ``def_player_1..5`` and :data:`COUNTS`, one season type of games.
        player_dtype: The league's player id dtype.
        min_poss: The rank gate: a player is ranked when ``off_poss + def_poss`` is at least this.
        alphas: The ridge penalty grid (default :data:`~sportsdataverse.nba.nba_rapm.DEFAULT_RAPM_ALPHAS`).

    Returns:
        One row per ``(season, player_id)`` in :func:`factor_rapm_schema`: possessions, ``rapm`` /
        ``o_rapm`` / ``d_rapm`` and the eight components (per 100 possessions, higher is better),
        ``<m>_rank`` / ``<m>_pct`` within the season among players with at least ``min_poss``
        possessions, ``<m>_se`` (clustered by game), ``<m>_n``, and the ``pts_per_fga`` and ``ridge_alpha`` the season was fit
        with. Sorted by season and ``rapm`` (best first). Empty input returns that schema with zero rows.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse._common.factor_rapm import factor_rapm
            from sportsdataverse.nba.nba_loaders import load_nba_stats_possessions
            factor_rapm(load_nba_stats_possessions(2024), player_dtype=pl.Int64(), min_poss=400)
    """
    schema = factor_rapm_schema(player_dtype)
    if possessions.height == 0:
        return pl.DataFrame(schema=schema)
    fits = []
    for (season,), part in possessions.partition_by("season", as_dict=True).items():
        fit = _fit_season(part, alphas)
        if fit is not None:
            fits.append(fit.with_columns(pl.lit(season).cast(pl.Int64).alias("season")))
    if not fits:
        return pl.DataFrame(schema=schema)
    out = pl.concat(fits, how="vertical_relaxed").with_columns(pl.col("player_id").cast(player_dtype))
    return (
        out.pipe(
            attach_ranks,
            _METRICS,
            min_cohort=MIN_COHORT_PLAYERS,
            by=("season",),
            qualifier=(pl.col("off_poss") + pl.col("def_poss")) >= min_poss,
        )
        .pipe(attach_sample_sizes, _N)
        .select(list(schema))
        .cast(schema)
        .sort(["season", "rapm", "player_id"], descending=[False, True, False])
    )
