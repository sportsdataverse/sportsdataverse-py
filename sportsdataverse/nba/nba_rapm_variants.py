"""RAPM variants over the shared possession design: luck-adjusted, four-factor, time-decay.

All three variants reuse :func:`sportsdataverse.nba.nba_rapm.build_rapm_design`
unchanged and differ only in the response vector and/or per-possession sample
weights. :mod:`sportsdataverse.nba.nba_rapm` is intentionally left untouched.

**Ridge-schedule divergence from** ``nba_rapm`` **(binding, 2026-07-03 WP2 plan
ruling, decision #8):** the RAPM *variants* (luck-adjusted / four-factor /
time-decay, added in later tasks of this module) fit against the **oracle**
regularization schedule — Ryan Davis's reference RAPM implementation's 3-point
lambda grid ``[0.01, 0.05, 0.1]`` converted to sklearn ``alpha`` via
``alpha = lambda * n_players / 2`` (see :func:`oracle_rapm_alphas`), combined
with explicit 5-fold cross-validation (``cv=5``, see :data:`ORACLE_RAPM_CV`) —
**not** :data:`sportsdataverse.nba.nba_rapm.DEFAULT_RAPM_ALPHAS`'s 8-point
``logspace(2, 5)`` grid with sklearn's default efficient LOOCV (``cv=None``).
Plain ``nba_rapm`` keeps its own settled convention unchanged.

The shared :func:`_fit_weighted` engine below stays a generic ``(alphas, cv)``
knob so a **single** fitting routine serves both conventions: its defaults
(``alphas=DEFAULT_RAPM_ALPHAS``, ``cv=None``) intentionally reproduce
``nba_rapm`` byte-for-byte (see ``test_fit_weighted_equals_plain_rapm_on_points``
in the test module — a scaffold-only invariant, since plain RAPM has no
weights or alternate response to justify diverging from it); the variant
functions added on top of this scaffold pass ``alphas=oracle_rapm_alphas(P)``
and ``cv=ORACLE_RAPM_CV`` explicitly, per the ruling above.
"""

from __future__ import annotations

import datetime
from typing import Optional, Sequence

import numpy as np
import polars as pl
from scipy.sparse import csr_matrix
from sklearn.linear_model import RidgeCV

from sportsdataverse.nba.nba_rapm import DEFAULT_RAPM_ALPHAS, build_rapm_design

_OFF: list[str] = [f"off_player_{i}" for i in range(1, 6)]
_DEF: list[str] = [f"def_player_{i}" for i in range(1, 6)]

#: Ryan Davis's oracle RAPM lambda grid (``rapm/rapm.py``), 3 points.
#: Converted to sklearn's ``alpha`` scale per possession count via
#: :func:`oracle_rapm_alphas` — the oracle's ``lambda_to_alpha(l, n) = l * n / 2``.
ORACLE_RAPM_LAMBDAS: tuple[float, ...] = (0.01, 0.05, 0.1)

#: Oracle RidgeCV fold count (explicit 5-fold, NOT sklearn's default LOOCV).
ORACLE_RAPM_CV: int = 5


def oracle_rapm_alphas(
    n_players: int,
    lambdas: Sequence[float] = ORACLE_RAPM_LAMBDAS,
) -> np.ndarray:
    """Convert the oracle's lambda grid to sklearn ``alpha`` values for *n_players*.

    Ryan Davis's reference RAPM scales the ridge penalty by the number of
    distinct players in the design (``lambda_to_alpha(l, n) = l * n / 2``), so
    unlike :data:`~sportsdataverse.nba.nba_rapm.DEFAULT_RAPM_ALPHAS` this grid
    is **not** a fixed array — it must be recomputed per design.

    Args:
        n_players: Number of distinct players ``P`` in the design (i.e.
            ``len(pids)`` from :func:`_prepare` / :func:`build_rapm_design`).
        lambdas: Lambda grid to convert. Defaults to :data:`ORACLE_RAPM_LAMBDAS`.

    Returns:
        Float64 array of RidgeCV ``alphas``, same length as *lambdas*.

    Example:
        Oracle-schedule alphas for a 250-player design::

            from sportsdataverse.nba.nba_rapm_variants import oracle_rapm_alphas
            alphas = oracle_rapm_alphas(250)
            print(alphas)  # array([1.25, 6.25, 12.5])
    """
    return np.asarray([lam * n_players / 2.0 for lam in lambdas], dtype=np.float64)


def decay_weights(
    game_date: pl.Series,
    asof: Optional[datetime.date],
    half_life_days: float,
) -> np.ndarray:
    """Exponential time-decay sample weights ``w = 0.5 ** (days_ago / half_life)``.

    Args:
        game_date: Per-possession game dates (``pl.Date`` Series), aligned
            row-for-row with the design the weights will be applied to.
        asof: Reference "today". ``None`` disables decay (all weights ``1.0``).
            Games dated after ``asof`` are clamped to ``days_ago = 0`` (weight
            ``1.0``); callers that want a strict as-of cutoff must filter first.
        half_life_days: Days at which a possession's weight halves. Must be > 0.

    Returns:
        Float64 array of weights, one per row of ``game_date``.

    Example:
        Down-weight month-old possessions by half::

            import datetime
            import polars as pl
            from sportsdataverse.nba.nba_rapm_variants import decay_weights

            dates = pl.Series("game_date", [datetime.date(2023, 1, 1)])
            w = decay_weights(dates, datetime.date(2023, 1, 31), half_life_days=30.0)
            print(round(float(w[0]), 3))  # 0.5
    """
    n = game_date.len()
    if asof is None or half_life_days <= 0:
        return np.ones(n, dtype=np.float64)
    days = game_date.to_numpy()
    asof_np = np.datetime64(asof)
    days_ago = (asof_np - days).astype("timedelta64[D]").astype(np.float64)
    days_ago = np.clip(days_ago, 0.0, None)
    return np.power(0.5, days_ago / float(half_life_days)).astype(np.float64)


def _prepare(
    possessions: pl.DataFrame,
    response_col: str,
    *,
    weight_col: Optional[str] = None,
) -> tuple[csr_matrix, np.ndarray, Optional[np.ndarray], list[int]]:
    """Build the shared design ``X`` with an externally-supplied, row-aligned response.

    Drops null-lineup rows ONCE (identical subset to ``build_rapm_design``) so the
    response/weight columns stay aligned to the surviving design rows, then builds
    ``X`` from the dropped frame (its internal re-drop is then a no-op that
    preserves row order).

    Args:
        possessions: Possession+lineup frame carrying ``response_col`` (and
            ``weight_col`` when given) as extra columns.
        response_col: Column name of the per-possession regression target.
        weight_col: Optional per-possession sample-weight column.

    Returns:
        ``(X, y, w, pids)`` where ``w`` is ``None`` when ``weight_col`` is ``None``.
        Empty / all-null-lineup input -> ``(csr_matrix((0, 0)), empty, None, [])``.
    """
    if possessions.is_empty():
        return csr_matrix((0, 0)), np.empty(0, dtype=np.float64), None, []
    kept = possessions.drop_nulls(subset=_OFF + _DEF)
    if kept.is_empty():
        return csr_matrix((0, 0)), np.empty(0, dtype=np.float64), None, []
    X, _y_points, pids = build_rapm_design(kept)
    y = kept[response_col].to_numpy().astype(np.float64)
    w = kept[weight_col].to_numpy().astype(np.float64) if weight_col is not None else None
    assert X.shape[0] == len(y), (X.shape, len(y))  # alignment contract
    return X, y, w, pids


def _fit_weighted(
    X: csr_matrix,
    y: np.ndarray,
    *,
    weights: Optional[np.ndarray] = None,
    alphas: np.ndarray = DEFAULT_RAPM_ALPHAS,
    cv: Optional[int] = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Fit a (optionally weighted) RidgeCV and return per-100 offense/defense + poss counts.

    Sign convention matches :func:`~sportsdataverse.nba.nba_rapm.nba_rapm`:
    ``d`` is the NEGATED raw defense coefficient x 100 (positive = good defender).

    This is the **shared** fitting engine for every RAPM variant in this module.
    Its defaults (``alphas=DEFAULT_RAPM_ALPHAS``, ``cv=None`` i.e. sklearn's
    efficient LOOCV) intentionally reproduce plain ``nba_rapm``'s own fit when
    called unweighted with no overrides. RAPM variants call this with
    ``alphas=oracle_rapm_alphas(len(pids))`` and ``cv=ORACLE_RAPM_CV`` per the
    binding oracle-ridge-schedule ruling documented in the module docstring.

    Args:
        X: Sparse design ``(n, 2P)`` from :func:`_prepare`.
        y: Row-aligned response ``(n,)``.
        weights: Optional per-possession sample weights ``(n,)``.
        alphas: RidgeCV alpha grid.
        cv: Cross-validation fold count forwarded to ``RidgeCV(cv=...)``.
            ``None`` (default) uses sklearn's efficient LOOCV -- the plain-
            ``nba_rapm`` convention. Pass :data:`ORACLE_RAPM_CV` (``5``) for
            the oracle schedule used by the RAPM variants.

    Returns:
        ``(o_per100, d_per100, off_poss, def_poss)`` each shape ``(P,)``.
    """
    P = X.shape[1] // 2
    model = RidgeCV(alphas=alphas, fit_intercept=True, cv=cv)
    # RidgeCV.fit accepts sample_weight with a sparse csr design under the default
    # ("auto") solver; verified by test_fit_weighted_honors_weights. If a future
    # sklearn drops sparse+weight support, fall back to Ridge + an explicit KFold
    # alpha loop (see spec Sec5 caveat) -- the public surface is unchanged.
    model.fit(X, y, sample_weight=weights)
    coef = np.asarray(model.coef_, dtype=np.float64)
    o = coef[:P] * 100.0
    d = -coef[P:] * 100.0
    col_sums = np.asarray(X.sum(axis=0), dtype=np.float64).ravel()
    return o, d, col_sums[:P].astype(np.int64), col_sums[P:].astype(np.int64)
