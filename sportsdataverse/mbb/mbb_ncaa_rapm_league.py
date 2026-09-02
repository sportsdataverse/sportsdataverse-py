"""League-wide NCAA RAPM: one joint O/D ridge per (league, season).

This is "Path B" of the NCAA RAPM program. The published
``ncaa_{lg}_rapm_within_team`` datasets estimate each player RELATIVE TO
TEAMMATES (the hoop-explorer engine solves one team at a time); this module
estimates every Division-I player on a COMMON league scale by regressing all
possessions jointly, which is what makes a team-aggregate external gate
(Torvik AdjEM Spearman) meaningful.

Like :mod:`sportsdataverse.mbb.mbb_ncaa_rapm_input`, this module is
league-blind -- frames in, frames out. WBB passes its own frames; there is
deliberately no ``wbb_ncaa_rapm_league`` twin.

The pipeline:

1. :func:`mbb_ncaa_rapm_input.resolve_possessions` attaches a ``player_id``
   to each of the ten on-floor slots (identity is the hard part and lives
   there, not here).
2. :func:`aggregate_stints` collapses those possessions to matchup stints --
   one row per unique (offense five, defense five, home/away offense). A
   possession-count-weighted ridge on stints is mathematically identical to
   the per-possession ridge (same ``X'WX`` / ``X'Wy``) while keeping the
   design matrix ~300k sparse rows instead of ~870k.
3. :func:`solve_rapm_league` runs the weighted sparse joint O/D ridge.

The model, on the per-100-possession scale::

    pts_per_100 = mu + sum(orapm_j, offense five) - sum(drapm_k, defense five)
                  + hca * side          (side = +1 home offense, -1 away)

``mu`` is the possession-weighted league mean, removed by centering (so it is
unpenalized); the player coefficients and ``hca`` are L2-penalized. With the
minus sign on the defense block, POSITIVE ``drapm`` means good defense. The
``hca`` coefficient is half the home-minus-away offensive gap; its shrinkage
under the shared penalty is negligible because every possession loads it.

A null slot id (an unresolved player, or the ``TEAM`` pseudo-slot) marks the
possession unusable and it is DROPPED -- "not a rated player" must never be
imputed. Callers should report the usable fraction.

**Uncertainty.** :func:`solve_rapm_league` also reports per-player standard
errors from the ridge POSTERIOR: with the Gaussian prior
``beta ~ N(0, sigma^2 / lambda)`` that the penalty encodes and
possession-weighted Gaussian errors, the posterior covariance is
``sigma^2 (X'WX + lambda I)^-1``, where ``sigma^2`` is the weighted residual
variance on ``n_stints - df_eff`` degrees of freedom (``df_eff`` = trace of
the ridge hat matrix). This is the Bayesian (credible-interval) SE and is
what the ``*_se`` columns carry: an interval for the TRUE impact under the
prior, which widens exactly where the prior is doing the work (a
low-possession player sits at ~0 +/- the prior SD ``sigma/sqrt(lambda)``).
The frequentist sandwich ``sigma^2 M X'WX M = sigma^2 (M - lambda M^2)``,
``M = (X'WX + lambda I)^-1``, is the repeatability of the SHRUNK estimate
and is exposed as ``*_se_sampling``; it is NOT an interval for the truth
(it collapses to ~0 for a player the ridge pins at zero) but it is the
quantity a refit can check. On real MBB 2024 the posterior SE is ~2.3x the
sampling SE even at 4,000 possessions (lambda = 1000 is prior-dominated), so
a split-half test covers ~100% under the posterior SE and ~95% under the
sampling SE -- :func:`split_half_se_check` reports both, and the producer
gates both. The intercept is treated as fixed (its SE is ~0.14 pts/100 on
~740k usable possessions, negligible beside player SEs of 3-5).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import numpy as np
import polars as pl

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "DEFAULT_RIDGE_LAMBDA",
    "STINT_SCHEMA",
    "aggregate_stints",
    "possession_deciles",
    "solve_rapm_league",
    "split_half_se_check",
    "team_aggregate",
]

#: Ridge penalty on the possession-weighted per-100 scale. Fitted by
#: game-grouped 5-fold CV (``dev/ncaa_rapm/fit_lambda_league.py``) on the
#: real 2024 seasons: BOTH leagues minimize weighted OOS MSE at 1000
#: (mbb 5145.33 over a 50..5000 grid; wbb 4910.74). Refit with that script;
#: do not tune ad hoc.
DEFAULT_RIDGE_LAMBDA = 1000.0

#: The ten id slots emitted by ``resolve_possessions`` (``"<slot>_id"``).
_SLOT_IDS = [f"{side}_{i}_id" for side in ("home", "away") for i in range(1, 6)]
_HOME_IDS = _SLOT_IDS[:5]
_AWAY_IDS = _SLOT_IDS[5:]

#: Documented output schema of :func:`aggregate_stints`.
STINT_SCHEMA: dict[str, pl.DataType] = {
    "off_ids": pl.List(pl.Utf8),
    "def_ids": pl.List(pl.Utf8),
    "off_team": pl.Utf8,
    "def_team": pl.Utf8,
    "is_home_offense": pl.Boolean,
    "n_poss": pl.Int64,
    "pts": pl.Int64,
}

_PLAYERS_SCHEMA: dict[str, pl.DataType] = {
    "player_id": pl.Utf8,
    "orapm": pl.Float64,
    "drapm": pl.Float64,
    "rapm_net": pl.Float64,
    "off_poss": pl.Int64,
    "def_poss": pl.Int64,
    "orapm_se": pl.Float64,
    "drapm_se": pl.Float64,
    "rapm_net_se": pl.Float64,
    "orapm_se_sampling": pl.Float64,
    "drapm_se_sampling": pl.Float64,
    "rapm_net_se_sampling": pl.Float64,
}

#: SE columns of :func:`solve_rapm_league`, posterior first, sampling (sandwich) second.
_SE_COLS = tuple(c for c in _PLAYERS_SCHEMA if c.endswith("_se") or c.endswith("_se_sampling"))


def aggregate_stints(resolved: pl.DataFrame) -> pl.DataFrame:
    """Collapse id-resolved possessions into matchup stints.

    Args:
        resolved: Output of ``resolve_possessions`` -- needs ``home``,
            ``away``, ``poss_team``, ``pts`` and the ten ``{slot}_id``
            columns. Extra columns are ignored.

    Returns:
        One row per unique ``(offense five, defense five, home/away
        offense)`` with ``n_poss`` and total ``pts`` (schema
        :data:`STINT_SCHEMA`). Possessions with ANY null slot id are dropped
        (unresolved player or ``TEAM`` pseudo-slot -- never imputed), as are
        possessions whose ``poss_team`` matches neither side.

    Raises:
        ValueError: A required column is missing.

    Example:
        Season pipeline::

            from sportsdataverse.mbb.mbb_ncaa_rapm_input import (
                build_player_xwalk, resolve_possessions,
            )
            from sportsdataverse.mbb.mbb_ncaa_rapm_league import aggregate_stints

            resolved = resolve_possessions(possessions, build_player_xwalk(rosters))
            stints = aggregate_stints(resolved)
    """
    required = ["home", "away", "poss_team", "pts", *_SLOT_IDS]
    missing = [c for c in required if c not in resolved.columns]
    if missing:
        raise ValueError(f"aggregate_stints: missing required columns {missing}")

    usable = resolved.filter(
        pl.all_horizontal([pl.col(c).is_not_null() for c in _SLOT_IDS])
        & ((pl.col("poss_team") == pl.col("home")) | (pl.col("poss_team") == pl.col("away")))
    )
    if usable.height == 0:
        return pl.DataFrame(schema=STINT_SCHEMA)

    is_home = pl.col("poss_team") == pl.col("home")
    home_ids = pl.concat_list([pl.col(c) for c in _HOME_IDS]).list.sort()
    away_ids = pl.concat_list([pl.col(c) for c in _AWAY_IDS]).list.sort()
    shaped = usable.with_columns(
        pl.when(is_home).then(home_ids).otherwise(away_ids).alias("off_ids"),
        pl.when(is_home).then(away_ids).otherwise(home_ids).alias("def_ids"),
        pl.when(is_home).then(pl.col("away")).otherwise(pl.col("home")).alias("def_team"),
        pl.col("poss_team").alias("off_team"),
        is_home.alias("is_home_offense"),
    )
    return (
        shaped.with_columns(
            pl.col("off_ids").list.join("|").alias("_ok"),
            pl.col("def_ids").list.join("|").alias("_dk"),
        )
        .group_by(["_ok", "_dk", "is_home_offense"], maintain_order=True)
        .agg(
            pl.col("off_ids").first(),
            pl.col("def_ids").first(),
            pl.col("off_team").first(),
            pl.col("def_team").first(),
            pl.len().cast(pl.Int64).alias("n_poss"),
            pl.col("pts").cast(pl.Int64).sum().alias("pts"),
        )
        .select(list(STINT_SCHEMA))
    )


_TEAM_AGG_SCHEMA: dict[str, pl.DataType] = {
    "team": pl.Utf8,
    "team_orapm": pl.Float64,
    "team_drapm": pl.Float64,
    "team_net": pl.Float64,
    "off_poss": pl.Int64,
    "def_poss": pl.Int64,
}


def _side_aggregate(
    stints: pl.DataFrame, players: pl.DataFrame, ids: str, team: str, coef: str, out: str
) -> pl.DataFrame:
    per_stint = (
        stints.with_row_index("_r")
        .select("_r", pl.col(team).alias("team"), "n_poss", pl.col(ids).alias("player_id"))
        .explode("player_id", empty_as_null=False)
        .join(players.select("player_id", coef), on="player_id", how="left")
        .group_by("_r", "team", "n_poss")
        .agg(pl.col(coef).fill_null(0.0).sum().alias("_sum"))
    )
    poss_col = "off_poss" if ids == "off_ids" else "def_poss"
    return per_stint.group_by("team").agg(
        ((pl.col("_sum") * pl.col("n_poss")).sum() / pl.col("n_poss").sum()).alias(out),
        pl.col("n_poss").sum().cast(pl.Int64).alias(poss_col),
    )


def team_aggregate(stints: pl.DataFrame, players: pl.DataFrame) -> pl.DataFrame:
    """Model-implied team ratings from player coefficients.

    For each team: ``team_orapm`` is the possession-weighted mean over its
    offensive stints of the on-floor five's ``orapm`` sum; ``team_drapm``
    likewise with ``drapm`` over its defensive stints; ``team_net`` is their
    sum. This is exactly what the fitted model predicts for the team's
    average possession (excluding opponent terms and home-court), so it is
    the faithful aggregate to hold against an external team rating (Torvik
    AdjEM) in the oracle gate.

    Args:
        stints: Output of :func:`aggregate_stints`.
        players: Players frame from :func:`solve_rapm_league` (needs
            ``player_id``, ``orapm``, ``drapm``). A player absent from it
            contributes 0 -- the model's own convention for an unrated
            column.

    Returns:
        One row per team: ``team``, ``team_orapm``, ``team_drapm``,
        ``team_net``, ``off_poss``, ``def_poss``.

    Example:
        Gate a season against Torvik::

            teams = team_aggregate(stints, players)
            joined = teams.join(torvik, on="team", how="inner")
    """
    if stints.height == 0:
        return pl.DataFrame(schema=_TEAM_AGG_SCHEMA)
    if players.schema["player_id"] != pl.Utf8:
        # A wrong-dtype key would left-join to all-null, fill_null(0.0) every
        # coefficient, and emit all-zero team ratings with no error.
        raise TypeError(f"players.player_id must be Utf8, got {players.schema['player_id']}")
    on_floor = set(stints["off_ids"].explode().to_list()) | set(stints["def_ids"].explode().to_list())
    if players.height and not (set(players["player_id"].to_list()) & on_floor):
        raise ValueError(
            "zero player_id overlap between stints and players -- wrong frames "
            "or an upstream id mismatch; refusing to emit all-zero team ratings"
        )
    off = _side_aggregate(stints, players, "off_ids", "off_team", "orapm", "team_orapm")
    dfn = _side_aggregate(stints, players, "def_ids", "def_team", "drapm", "team_drapm")
    return (
        off.join(dfn, on="team", how="full", coalesce=True)
        .with_columns(
            pl.col("team_orapm").fill_null(0.0),
            pl.col("team_drapm").fill_null(0.0),
            pl.col("off_poss").fill_null(0).cast(pl.Int64),
            pl.col("def_poss").fill_null(0).cast(pl.Int64),
        )
        .with_columns((pl.col("team_orapm") + pl.col("team_drapm")).alias("team_net"))
        .select(list(_TEAM_AGG_SCHEMA))
        .sort("team")
    )


def solve_rapm_league(
    stints: pl.DataFrame,
    *,
    ridge_lambda: float = DEFAULT_RIDGE_LAMBDA,
    compute_se: bool = True,
    return_as_pandas: bool = False,
) -> "tuple[pl.DataFrame | pd.DataFrame, dict[str, float]]":
    """Weighted sparse joint O/D ridge over matchup stints.

    Args:
        stints: Output of :func:`aggregate_stints`.
        ridge_lambda: L2 penalty on the possession-weighted normal equations.
            Because rows are weighted by possessions, the same ``lambda`` is
            exactly equivalent to a per-possession ridge with that penalty.
        compute_se: Also return the posterior standard errors (module
            docstring, "Uncertainty"). Costs one dense Cholesky inverse of the
            ``(2P+1)``-square penalised Gram matrix. One such matrix is
            ~0.8 GB for a D-I season (P ~ 5,000, dim ~ 10,001); the
            symmetrise step holds two of them at once, so the peak is
            ~1.6 GB and a few seconds. Switch off for pooled designs past
            ~20k columns.
        return_as_pandas: Return the players frame as pandas.

    Returns:
        ``(players, info)``. ``players`` has one row per rated player:
        ``player_id``, ``orapm``, ``drapm`` (positive = good defense),
        ``rapm_net = orapm + drapm`` (all per 100 possessions), ``off_poss``,
        ``def_poss``, the posterior standard errors ``orapm_se``,
        ``drapm_se``, ``rapm_net_se`` and the sampling (sandwich) standard
        errors ``*_se_sampling`` (both net SEs include the O/D covariance; all
        six are null when ``compute_se=False``; see the module docstring for
        which to publish). ``info``
        carries ``intercept`` (possession-weighted league mean pts/100),
        ``hca`` (half the home-minus-away offensive gap), ``ridge_lambda``,
        ``n_stints``, ``n_poss``, the lsqr diagnostics and, with SEs,
        ``sigma2`` (weighted residual variance on the per-100 scale = ``1e4 x``
        the per-possession points variance, ~1.3e4 in D-I), ``df_eff`` (trace
        of the ridge hat matrix), ``hca_se`` and ``solve_max_abs_dev`` (max
        |lsqr - exact| coefficient deviation -- the exact solve comes free
        with the factorisation and doubles as the solver-tolerance check).

    Raises:
        RuntimeError: lsqr stopped without converging, or the penalised Gram
            matrix is not positive definite (``ridge_lambda`` must be > 0 for
            the SEs).

    Example:
        Solve a season::

            from sportsdataverse.mbb.mbb_ncaa_rapm_league import (
                aggregate_stints, solve_rapm_league,
            )

            players, info = solve_rapm_league(aggregate_stints(resolved))
            players.sort("rapm_net", descending=True).head()

        A 95% interval per player::

            players.with_columns(
                (pl.col("rapm_net") - 2 * pl.col("rapm_net_se")).alias("lo95"),
                (pl.col("rapm_net") + 2 * pl.col("rapm_net_se")).alias("hi95"),
            )

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's college
          basketball companion (R)
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's college
          basketball companion (R)
    """
    from scipy.sparse import coo_matrix
    from scipy.sparse.linalg import lsqr

    empty = pl.DataFrame(schema=_PLAYERS_SCHEMA)
    if stints.height == 0:
        info = {
            "intercept": 0.0,
            "hca": 0.0,
            "ridge_lambda": float(ridge_lambda),
            "n_stints": 0,
            "n_poss": 0,
        }
        return (empty.to_pandas() if return_as_pandas else empty), info

    s = stints.with_row_index("_row")
    # empty_as_null=False is the polars-2.0 default; the lists are always
    # exactly five ids so the choice is behavior-neutral here.
    off = s.select("_row", "n_poss", pl.col("off_ids").alias("pid")).explode("pid", empty_as_null=False)
    dfn = s.select("_row", "n_poss", pl.col("def_ids").alias("pid")).explode("pid", empty_as_null=False)
    players = sorted(set(off["pid"].to_list()) | set(dfn["pid"].to_list()))
    idx = {p: i for i, p in enumerate(players)}
    n_players = len(players)
    n_stints = s.height

    w = s["n_poss"].to_numpy().astype(np.float64)
    sw = np.sqrt(w)
    y = 100.0 * s["pts"].to_numpy().astype(np.float64) / w
    mu = float(np.average(y, weights=w))

    # Sparse design: offense block [0, P), defense block [P, 2P) with -1
    # entries (so positive drapm = fewer points allowed), hca column at 2P.
    off_rows = off["_row"].to_numpy().astype(np.int64)
    dfn_rows = dfn["_row"].to_numpy().astype(np.int64)
    off_cols: np.ndarray = np.fromiter((idx[p] for p in off["pid"].to_list()), np.int64, len(off_rows))
    dfn_cols: np.ndarray = np.fromiter((n_players + idx[p] for p in dfn["pid"].to_list()), np.int64, len(dfn_rows))
    side = np.where(s["is_home_offense"].to_numpy(), 1.0, -1.0)

    rows = np.concatenate([off_rows, dfn_rows, np.arange(n_stints)])
    cols = np.concatenate([off_cols, dfn_cols, np.full(n_stints, 2 * n_players)])
    vals = np.concatenate([sw[off_rows], -sw[dfn_rows], side * sw])
    x = coo_matrix((vals, (rows, cols)), shape=(n_stints, 2 * n_players + 1)).tocsr()

    beta, istop, itn = lsqr(x, (y - mu) * sw, damp=float(np.sqrt(ridge_lambda)))[:3]
    if istop not in (0, 1, 2):
        # istop=7 is the iteration limit: lsqr hands back a PARTIAL iterate
        # with no error, which must never flow silently into the gate.
        raise RuntimeError(f"lsqr did not converge (istop={istop}, itn={itn}) -- refusing to return a partial solve")
    orapm = beta[:n_players]
    drapm = beta[n_players : 2 * n_players]
    hca = float(beta[2 * n_players])

    se_info: dict[str, float] = {}
    if compute_se:
        se, se_info = _posterior_se(x, (y - mu) * sw, beta, n_players, float(ridge_lambda))
    else:
        se = {c: np.full(n_players, np.nan) for c in _SE_COLS}  # -> null below, never NaN

    exposure = (
        pl.concat(
            [
                off.select("pid", pl.col("n_poss").alias("off_poss")),
                dfn.select("pid", pl.col("n_poss").alias("def_poss")),
            ],
            how="diagonal",
        )
        .group_by("pid")
        .agg(
            pl.col("off_poss").sum().fill_null(0).cast(pl.Int64),
            pl.col("def_poss").sum().fill_null(0).cast(pl.Int64),
        )
    )
    out = (
        pl.DataFrame(
            {
                "player_id": pl.Series(players, dtype=pl.Utf8),
                "orapm": orapm.astype(np.float64),
                "drapm": drapm.astype(np.float64),
                **{c: pl.Series(se[c], dtype=pl.Float64).fill_nan(None) for c in _SE_COLS},
            }
        )
        .with_columns((pl.col("orapm") + pl.col("drapm")).alias("rapm_net"))
        .join(exposure.rename({"pid": "player_id"}), on="player_id", how="left")
        .with_columns(
            pl.col("off_poss").fill_null(0).cast(pl.Int64),
            pl.col("def_poss").fill_null(0).cast(pl.Int64),
        )
        .select(list(_PLAYERS_SCHEMA))
        .sort("player_id")
    )
    info = {
        "intercept": mu,
        "hca": hca,
        "ridge_lambda": float(ridge_lambda),
        "n_stints": n_stints,
        "n_poss": int(w.sum()),
        "lsqr_istop": int(istop),
        "lsqr_itn": int(itn),
        **se_info,
    }
    return (out.to_pandas() if return_as_pandas else out), info


def _posterior_se(
    x: Any, yw: np.ndarray, beta: np.ndarray, n_players: int, ridge_lambda: float
) -> "tuple[dict[str, np.ndarray], dict[str, float]]":
    """Posterior and sampling SEs of the ridge coefficients from ONE dense Cholesky inverse.

    ``x`` is the sqrt-weight-scaled sparse design, so ``X'X`` IS the
    possession-weighted Gram matrix ``G = X_raw' W X_raw`` and ``yw`` the
    scaled centred target. With ``M = (G + lambda I)^-1``:

    * posterior covariance ``sigma2 * M`` -> ``*_se`` (interval for the truth);
    * sampling covariance ``sigma2 * M G M = sigma2 * (M - lambda M^2)`` ->
      ``*_se_sampling`` (repeatability of the shrunk estimate; what
      :func:`split_half_se_check` can verify). Both are O(dim^2) once ``M``
      is dense, since ``diag(M^2)`` is the squared row norms.

    The net SEs fold in the O/D covariance ``M[i, P+i]`` (resp.
    ``(M - lambda M^2)[i, P+i]``). Also returns ``sigma2`` (weighted residual
    variance on ``n - df_eff`` dof), ``df_eff``, ``hca_se`` and the max
    |lsqr - exact| coefficient deviation.
    """
    from scipy.linalg import lapack

    dim = x.shape[1]
    # ponytail: dense Cholesky inverse, O(dim^3) -- ~10k dims for a D-I season, 0.8 GB per
    # dense array. `low + low.T` below holds two of them live, so peak is ~1.6 GB; blocked
    # in-place symmetrisation would halve it if that ever binds. A pooled multi-season
    # design past ~20k dims needs a sparse (CHOLMOD) factor.
    a = (x.T @ x).toarray(order="F")
    a[np.diag_indices(dim)] += ridge_lambda
    c, info_f = lapack.dpotrf(a, lower=1, clean=1, overwrite_a=1)
    if info_f != 0:
        raise RuntimeError(
            f"penalised Gram matrix is not positive definite (dpotrf info={info_f}); ridge_lambda must be > 0"
        )
    low, info_i = lapack.dpotri(c, lower=1, overwrite_c=1)
    if info_i != 0:
        raise RuntimeError(f"Cholesky inverse failed (dpotri info={info_i})")
    # dpotri fills only the lower triangle (clean=1 zeroed the upper): symmetrise.
    diag = np.diagonal(low).copy()
    m = low + low.T
    m[np.diag_indices(dim)] = diag
    del a, c, low
    beta_exact = m @ (x.T @ yw)
    resid = yw - x @ beta
    df_eff = float(dim - ridge_lambda * diag.sum())
    sigma2 = float(resid @ resid) / max(x.shape[0] - df_eff, 1.0)
    p = np.arange(n_players)
    o, d = slice(0, n_players), slice(n_players, 2 * n_players)
    m2_diag = np.einsum("ij,ij->i", m, m)  # diag(M^2)
    m2_od = np.einsum("ij,ij->i", m[o], m[d])  # (M^2)[i, P+i]
    post_o, post_d, post_od = diag[o], diag[d], m[p, n_players + p]
    samp_o = post_o - ridge_lambda * m2_diag[o]
    samp_d = post_d - ridge_lambda * m2_diag[d]
    samp_od = post_od - ridge_lambda * m2_od

    def _se(var: np.ndarray) -> np.ndarray:
        return np.sqrt(np.maximum(sigma2 * var, 0.0))

    se = {
        "orapm_se": _se(post_o),
        "drapm_se": _se(post_d),
        "rapm_net_se": _se(post_o + post_d + 2.0 * post_od),
        "orapm_se_sampling": _se(samp_o),
        "drapm_se_sampling": _se(samp_d),
        "rapm_net_se_sampling": _se(samp_o + samp_d + 2.0 * samp_od),
    }
    info = {
        "sigma2": sigma2,
        "df_eff": df_eff,
        "hca_se": float(np.sqrt(sigma2 * diag[2 * n_players])),
        "solve_max_abs_dev": float(np.max(np.abs(beta - beta_exact))),
    }
    return se, info


def split_half_se_check(
    resolved: pl.DataFrame,
    *,
    ridge_lambda: float = DEFAULT_RIDGE_LAMBDA,
) -> "tuple[pl.DataFrame, dict[str, float]]":
    """Odd-vs-even-game refit: are the standard errors on the right scale?

    Splits ``resolved`` by the parity of ``contest_id`` (deterministic and
    roster-neutral -- the halves differ by sampling noise, not by the
    mid-season development or transfer drift a date split would add), fits
    :func:`solve_rapm_league` on each half, and asks per player rated in both
    whether the two estimates agree to within ``2 * sqrt(se_A^2 + se_B^2)``.

    Two readings, both reported. Under the SAMPLING SE this is the textbook
    calibration test -- ~95% coverage means ``sigma2`` and the inverse are
    right. Under the POSTERIOR SE (the published one) coverage is expected
    ABOVE nominal (~100% on real seasons): a credible interval for the truth
    is wider than the estimator's repeatability, so its coverage here is a
    one-sided guard (a bug that shrinks the SEs drops it), not a two-sided
    calibration.

    Args:
        resolved: Output of ``resolve_possessions`` WITH its ``contest_id``
            column (integer-like).
        ridge_lambda: Passed to both half fits.

    Returns:
        ``(per_player, summary)``. ``per_player`` has one row per player
        rated in both halves: ``player_id``, ``poss`` (both halves' exposure)
        and, for each of ``orapm`` / ``drapm`` / ``rapm_net``, ``<c>_a``,
        ``<c>_b``, ``<c>_z`` / ``<c>_covered`` (posterior SE units, |z| <= 2)
        and ``<c>_z_sampling`` / ``<c>_covered_sampling`` (sampling SE units).
        ``summary`` carries ``n_players``, ``n_games_a``, ``n_games_b`` and,
        per ``<c>``, ``coverage_<c>``, ``coverage_sampling_<c>``, ``z_sd_<c>``
        (a posterior/sampling ratio proxy) and ``z_sd_sampling_<c>`` (~1 when
        calibrated). Feed ``per_player`` to :func:`possession_deciles` for
        the coverage by playing time.

    Raises:
        ValueError: ``contest_id`` is missing or not integer-like.

    Example:
        Season check::

            per_player, summary = split_half_se_check(resolved)
            print(summary["coverage_sampling_rapm_net"])   # ~0.95
            possession_deciles(per_player)
    """
    if "contest_id" not in resolved.columns:
        raise ValueError("split_half_se_check: resolved needs its contest_id column")
    cid = resolved.get_column("contest_id").cast(pl.Int64, strict=False)
    if cid.null_count():
        raise ValueError("split_half_se_check: contest_id must be integer-like")
    halves = resolved.with_columns((cid % 2).alias("_half"))
    fits = []
    for h in (0, 1):
        part = halves.filter(pl.col("_half") == h)
        players, _ = solve_rapm_league(aggregate_stints(part), ridge_lambda=ridge_lambda)
        fits.append((players, part.get_column("contest_id").n_unique()))
    (pa, n_a), (pb, n_b) = fits
    assert isinstance(pa, pl.DataFrame) and isinstance(pb, pl.DataFrame)
    j = pa.join(pb, on="player_id", how="inner", suffix="_b")
    cols = ("orapm", "drapm", "rapm_net")
    exprs = [pl.col(f"{c}_b") for c in cols]
    for c in cols:
        exprs.append(pl.col(c).alias(f"{c}_a"))
        for kind, sfx in (("", ""), ("_sampling", "_sampling")):
            se_a, se_b = pl.col(f"{c}_se{kind}"), pl.col(f"{c}_se{kind}_b")
            z = (pl.col(c) - pl.col(f"{c}_b")) / (se_a**2 + se_b**2).sqrt()
            exprs += [
                z.alias(f"{c}_z{sfx}"),
                (z.abs() <= 2.0).alias(f"{c}_covered{sfx}"),
            ]
    per_player = j.select(
        "player_id",
        (pl.col("off_poss") + pl.col("def_poss") + pl.col("off_poss_b") + pl.col("def_poss_b")).alias("poss"),
        *exprs,
    ).sort("player_id")
    summary: dict[str, float] = {
        "n_players": per_player.height,
        "n_games_a": n_a,
        "n_games_b": n_b,
    }
    for c in cols:
        for sfx in ("", "_sampling"):
            key = sfx.lstrip("_") + "_" if sfx else ""
            cov = per_player[f"{c}_covered{sfx}"]
            summary[f"coverage_{key}{c}"] = float(cov.mean()) if per_player.height else float("nan")
            z_sd = per_player[f"{c}_z{sfx}"].std()
            summary[f"z_sd_{key}{c}"] = float(z_sd) if z_sd is not None else float("nan")
    return per_player, summary


def possession_deciles(players: pl.DataFrame, *, n_bins: int = 10) -> pl.DataFrame:
    """Equal-count possession bins: median of every SE column, mean of every coverage flag.

    One table serves both SE validations: on :func:`solve_rapm_league`
    output the median posterior SE must fall with playing time (Spearman
    strongly negative; the top deciles flatten at a collinearity floor --
    heavy-minute starters share the floor with the same four teammates); on
    :func:`split_half_se_check` output the sampling coverage should sit
    near 0.95 in every bin. Uses ``poss`` when present, else
    ``off_poss + def_poss``. Bins are rank-based (always ``n_bins``
    equal-count bins), so heavy ties at low possessions cannot collapse a
    decile.

    Args:
        players: Output of :func:`solve_rapm_league` or the ``per_player``
            frame of :func:`split_half_se_check`.
        n_bins: Number of equal-count bins (10 = deciles).

    Returns:
        One row per bin, sorted: ``decile`` (0 = fewest possessions), ``n``,
        ``poss_min``, ``poss_max``, ``median_<se col>`` ... and
        ``coverage_<c>[_sampling]`` ... (only for the columns present).

    Example:
        Shrinkage check::

            d = possession_deciles(players)
            assert d["median_rapm_net_se"][0] > d["median_rapm_net_se"][-1]
    """
    poss = pl.col("poss") if "poss" in players.columns else (pl.col("off_poss") + pl.col("def_poss"))
    se_cols = [c for c in players.columns if c.endswith("_se") or c.endswith("_se_sampling")]
    cov_cols = [c for c in players.columns if "_covered" in c]
    binned = players.with_columns(poss.alias("_poss")).with_columns(
        ((pl.col("_poss").rank(method="ordinal") - 1) * n_bins // pl.len()).cast(pl.Int32).alias("decile")
    )
    return (
        binned.group_by("decile")
        .agg(
            pl.len().alias("n"),
            pl.col("_poss").min().alias("poss_min"),
            pl.col("_poss").max().alias("poss_max"),
            *[pl.col(c).median().alias(f"median_{c}") for c in se_cols],
            *[pl.col(c).mean().alias(f"coverage_{c.replace('_covered', '')}") for c in cov_cols],
        )
        .sort("decile")
    )
