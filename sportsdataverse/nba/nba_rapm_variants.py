"""RAPM variants over the shared possession design: luck-adjusted, four-factor, time-decay.

All three variants reuse :func:`sportsdataverse.nba.nba_rapm.build_rapm_design`
unchanged and differ only in the response vector and/or per-possession sample
weights. :mod:`sportsdataverse.nba.nba_rapm` is intentionally left untouched.

**Ridge-schedule divergence from** ``nba_rapm`` **(binding, 2026-07-03 WP2 plan
ruling, decision #8):** the RAPM *variants* (luck-adjusted / four-factor /
time-decay, added in later tasks of this module) fit against the **oracle**
regularization schedule — Ryan Davis's reference RAPM implementation's 3-point
lambda grid ``[0.01, 0.05, 0.1]`` converted to sklearn ``alpha`` via
``alpha = lambda * n_samples / 2`` (see :func:`oracle_rapm_alphas`), combined
with explicit 5-fold cross-validation (``cv=5``, see :data:`ORACLE_RAPM_CV`) —
**not** :data:`sportsdataverse.nba.nba_rapm.DEFAULT_RAPM_ALPHAS`'s 8-point
``logspace(2, 5)`` grid with sklearn's default efficient LOOCV (``cv=None``).
Plain ``nba_rapm`` keeps its own settled convention unchanged.

**``n_samples`` is the design-matrix row count (possessions), NOT the player
count** — verified against the oracle source
(``NBA_Tutorials_Ryan_Davis/rapm/rapm.py:112-125``):
``lambda_to_alpha(lambda_value, samples) = (lambda_value * samples) / 2.0``,
called as ``lambda_to_alpha(l, train_x.shape[0])`` where ``train_x`` is the
possession-level design matrix, so ``samples`` is ``X.shape[0]`` (possessions),
never ``len(player_ids)``.

The shared :func:`_fit_weighted` engine below stays a generic ``(alphas, cv)``
knob so a **single** fitting routine serves both conventions: its defaults
(``alphas=DEFAULT_RAPM_ALPHAS``, ``cv=None``) intentionally reproduce
``nba_rapm`` byte-for-byte (see ``test_fit_weighted_equals_plain_rapm_on_points``
in the test module — a scaffold-only invariant, since plain RAPM has no
weights or alternate response to justify diverging from it); the variant
functions added on top of this scaffold pass ``alphas=oracle_rapm_alphas(n)``
(``n`` = possession count) and ``cv=ORACLE_RAPM_CV`` explicitly, per the
ruling above.
"""

from __future__ import annotations

import datetime
from typing import Optional, Sequence, Union

import numpy as np
import pandas as pd
import polars as pl
from scipy.sparse import csr_matrix
from sklearn.linear_model import RidgeCV

from sportsdataverse.nba.nba_rapm import DEFAULT_RAPM_ALPHAS, build_rapm_design

_OFF: list[str] = [f"off_player_{i}" for i in range(1, 6)]
_DEF: list[str] = [f"def_player_{i}" for i in range(1, 6)]

#: Ryan Davis's oracle RAPM lambda grid (``rapm/rapm.py``), 3 points.
#: Converted to sklearn's ``alpha`` scale per possession (sample) count via
#: :func:`oracle_rapm_alphas` — the oracle's ``lambda_to_alpha(l, n) = l * n / 2``
#: where ``n`` is the number of possessions (design-matrix rows), NOT players.
ORACLE_RAPM_LAMBDAS: tuple[float, ...] = (0.01, 0.05, 0.1)

#: Oracle RidgeCV fold count (explicit 5-fold, NOT sklearn's default LOOCV).
ORACLE_RAPM_CV: int = 5


def oracle_rapm_alphas(
    n_samples: int,
    lambdas: Sequence[float] = ORACLE_RAPM_LAMBDAS,
) -> np.ndarray:
    """Convert the oracle's lambda grid to sklearn ``alpha`` values for *n_samples*.

    Ryan Davis's reference RAPM scales the ridge penalty by the number of
    **possessions** (design-matrix rows, i.e. regression samples) —
    ``NBA_Tutorials_Ryan_Davis/rapm/rapm.py:112-125``:
    ``lambda_to_alpha(lambda_value, samples) = (lambda_value * samples) / 2.0``,
    invoked as ``lambda_to_alpha(l, train_x.shape[0])`` where ``train_x`` is the
    possession-level design matrix. So ``samples`` is ``X.shape[0]``
    (possessions) — **not** the player count ``P``. Unlike
    :data:`~sportsdataverse.nba.nba_rapm.DEFAULT_RAPM_ALPHAS` this grid is
    **not** a fixed array — it must be recomputed per design.

    Args:
        n_samples: Number of possessions (design-matrix rows) in the design,
            i.e. ``X.shape[0]`` / ``len(y)`` from :func:`_prepare` /
            :func:`build_rapm_design`.
        lambdas: Lambda grid to convert. Defaults to :data:`ORACLE_RAPM_LAMBDAS`.

    Returns:
        Float64 array of RidgeCV ``alphas``, same length as *lambdas*.

    Example:
        Oracle-schedule alphas for a 50 000-possession design::

            from sportsdataverse.nba.nba_rapm_variants import oracle_rapm_alphas
            alphas = oracle_rapm_alphas(50_000)
            print(alphas)  # array([ 250., 1250., 2500.])
    """
    return np.asarray([lam * n_samples / 2.0 for lam in lambdas], dtype=np.float64)


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
    ``alphas=oracle_rapm_alphas(n_samples)`` (``n_samples`` = possession /
    design-row count, NOT player count) and ``cv=ORACLE_RAPM_CV`` per the
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


#: Output schema for :func:`nba_decay_rapm`.
DECAY_RAPM_SCHEMA: dict[str, pl.DataType] = {
    "player_id": pl.Int64,
    "o_decay_rapm": pl.Float64,
    "d_decay_rapm": pl.Float64,
    "decay_rapm": pl.Float64,
    "off_poss": pl.Int64,
    "def_poss": pl.Int64,
}


def _empty(schema: dict[str, pl.DataType]) -> pl.DataFrame:
    """Zero-row frame with exactly ``schema``."""
    return pl.DataFrame({c: pl.Series([], dtype=t) for c, t in schema.items()})


def nba_decay_rapm(
    possessions: pl.DataFrame,
    *,
    asof: Optional[datetime.date] = None,
    half_life_days: float = 180.0,
    alphas: Optional[np.ndarray] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Time-decay RAPM: ridge weighted by ``0.5 ** (days_ago / half_life_days)``.

    ``asof=None`` disables decay: every possession is weighted ``1.0`` and the
    fit uses **exactly** plain :func:`~sportsdataverse.nba.nba_rapm.nba_rapm`'s
    own schedule (``alphas=DEFAULT_RAPM_ALPHAS``, sklearn's efficient default
    LOOCV) so the two agree byte-for-byte (see
    ``test_decay_rapm_asof_none_equals_plain_rapm``). When ``asof`` is set,
    possessions dated after ``asof`` are dropped, the remainder is
    exponentially down-weighted by age, and the fit switches to the
    **oracle** regularization schedule (:func:`oracle_rapm_alphas` evaluated
    at the post-filter possession count, ``cv=`` :data:`ORACLE_RAPM_CV`) per
    the binding WP2 ridge-schedule ruling documented in the module docstring.

    .. note::
        **Deviation from the task interface sketch**: the brief's draft
        signature defaulted ``alphas=DEFAULT_RAPM_ALPHAS`` unconditionally,
        which is exactly the schedule that :func:`_fit_weighted` already uses
        when nothing is overridden -- fine for the ``asof=None`` branch, but
        it would silently skip the oracle schedule the binding ruling
        requires for the decay-weighted branch. This function instead
        defaults ``alphas=None`` and auto-selects the schedule per branch
        (described above); passing an explicit ``alphas`` array overrides
        the auto-selection in either branch.

    Args:
        possessions: Multi-season possession+lineup frame. Must carry a
            ``game_date`` (``pl.Date``) column when ``asof`` is not ``None``.
        asof: Reference date; ``None`` -> unweighted, plain-RAPM-equivalent fit.
        half_life_days: Weight half-life in days (default 180).
        alphas: Optional RidgeCV alpha grid override. ``None`` (default)
            auto-selects :data:`~sportsdataverse.nba.nba_rapm.DEFAULT_RAPM_ALPHAS`
            when ``asof is None`` or :func:`oracle_rapm_alphas` (evaluated at
            the possession count) when ``asof`` is set.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        Frame with :data:`DECAY_RAPM_SCHEMA`. Empty input, or an ``asof`` that
        drops every possession, -> zero-row frame.

    Raises:
        ValueError: ``asof`` is not ``None`` but *possessions* lacks a
            ``game_date`` column.

    Example:
        Recency-weighted ratings as of a date::

            import datetime
            from sportsdataverse.nba.nba_rapm_variants import nba_decay_rapm

            df = nba_decay_rapm(season_poss, asof=datetime.date(2024, 3, 1), half_life_days=120.0)
            print(df.sort("decay_rapm", descending=True).head())

        Plain-RAPM-equivalent (no decay)::

            df = nba_decay_rapm(season_poss)  # asof=None
    """
    if possessions.is_empty():
        out = _empty(DECAY_RAPM_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    frame = possessions
    weight_col: Optional[str] = None
    if asof is not None:
        if "game_date" not in frame.columns:
            raise ValueError("nba_decay_rapm(asof=...) requires a 'game_date' column")
        frame = frame.filter(pl.col("game_date") <= asof)
        if frame.is_empty():
            out = _empty(DECAY_RAPM_SCHEMA)
            return out.to_pandas() if return_as_pandas else out
        w = decay_weights(frame["game_date"], asof, half_life_days)
        frame = frame.with_columns(pl.Series("_w", w))
        weight_col = "_w"

    X, y, wv, pids = _prepare(frame, "points", weight_col=weight_col)
    if not pids:
        out = _empty(DECAY_RAPM_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    if asof is None:
        # Plain-RAPM-equivalent branch: reproduce nba_rapm's own schedule exactly.
        fit_alphas = alphas if alphas is not None else DEFAULT_RAPM_ALPHAS
        fit_cv: Optional[int] = None
    else:
        # Decay-weighted branch: oracle regularization schedule (binding WP2 ruling),
        # evaluated at the post-filter possession count (X.shape[0]), NOT player count.
        fit_alphas = alphas if alphas is not None else oracle_rapm_alphas(X.shape[0])
        fit_cv = ORACLE_RAPM_CV

    o, d, off_poss, def_poss = _fit_weighted(X, y, weights=wv, alphas=fit_alphas, cv=fit_cv)
    out = pl.DataFrame(
        {
            "player_id": pl.Series(pids, dtype=pl.Int64),
            "o_decay_rapm": pl.Series(o, dtype=pl.Float64),
            "d_decay_rapm": pl.Series(d, dtype=pl.Float64),
            "decay_rapm": pl.Series(o + d, dtype=pl.Float64),
            "off_poss": pl.Series(off_poss, dtype=pl.Int64),
            "def_poss": pl.Series(def_poss, dtype=pl.Int64),
        }
    ).sort("player_id")
    return out.to_pandas() if return_as_pandas else out
