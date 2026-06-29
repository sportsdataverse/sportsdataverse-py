"""Regularized Adjusted Plus-Minus (RAPM) pilot over the possession stint matrix.

This module builds the sparse design matrix consumed by the Ridge regression
solver and fits a plain-RAPM via :class:`sklearn.linear_model.RidgeCV` from
possession-level lineup data produced by ``attach_possession_lineups``.
"""

from __future__ import annotations

import numpy as np
import polars as pl
from scipy.sparse import csr_matrix
from sklearn.linear_model import RidgeCV

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

#: Alpha grid for RidgeCV (logspace 100 … 100 000, 8 points).
DEFAULT_RAPM_ALPHAS: np.ndarray = np.logspace(2, 5, 8)

#: Canonical output schema for :func:`nba_rapm`.
#: Use this mapping both to build the zero-row empty frame and to enforce
#: final column dtypes on non-empty results.
RAPM_SCHEMA: dict[str, type[pl.DataType]] = {
    "player_id": pl.Int64,
    "o_rapm": pl.Float64,
    "d_rapm": pl.Float64,
    "rapm": pl.Float64,
    "off_poss": pl.Int64,
    "def_poss": pl.Int64,
}

_OFF: list[str] = [f"off_player_{i}" for i in range(1, 6)]
_DEF: list[str] = [f"def_player_{i}" for i in range(1, 6)]


def build_rapm_design(
    possessions: pl.DataFrame,
) -> tuple[csr_matrix, np.ndarray, list[int]]:
    """Build a sparse RAPM design matrix from a possession stint DataFrame.

    Each possession maps to one row in the output matrix.  The column layout
    is offense-first then defense:

    * Columns ``0 .. P-1``   — offense indicators: column ``i`` is 1 when
      ``player_ids[i]`` was on offense that possession, 0 otherwise.
    * Columns ``P .. 2P-1``  — defense indicators: column ``P+i`` is 1 when
      ``player_ids[i]`` was on defense that possession, 0 otherwise.

    Args:
        possessions: A :class:`polars.DataFrame` with columns
            ``off_player_1`` … ``off_player_5`` (Int64 player IDs on offense),
            ``def_player_1`` … ``def_player_5`` (Int64 player IDs on defense),
            and ``points`` (Int64 points scored on the possession).
            Produced by ``sportsdataverse.nba.nba_possessions.attach_possession_lineups``.
            Possessions with any null lineup cell are dropped (a partial lineup
            is unreliable for RAPM); if that leaves no rows, an empty design is
            returned.

    Returns:
        A 3-tuple ``(X, y, player_ids)`` where:

        * **X** — :class:`scipy.sparse.csr_matrix` of shape ``(n_poss, 2P)``
          with dtype ``float64``.  Binary offense/defense indicators as
          described above.  ``n_poss`` counts only possessions with all 10
          lineup slots populated (null-lineup rows are dropped).
        * **y** — :class:`numpy.ndarray` of shape ``(n_poss,)`` with dtype
          ``float64``.  Points scored on each possession.
        * **player_ids** — ``list[int]`` of length ``P``: the sorted distinct
          player IDs appearing in any lineup cell across the input.

        When *possessions* is empty (or every row is dropped for a null lineup
        cell), returns ``(csr_matrix((0, 0)), np.empty(0), [])``.

    Example:
        Basic two-possession example::

            import polars as pl
            from sportsdataverse.nba.nba_rapm import build_rapm_design

            rows = {
                "off_player_1": [1, 6], "off_player_2": [2, 7],
                "off_player_3": [3, 8], "off_player_4": [4, 9],
                "off_player_5": [5, 10],
                "def_player_1": [6, 1], "def_player_2": [7, 2],
                "def_player_3": [8, 3], "def_player_4": [9, 4],
                "def_player_5": [10, 5],
                "points": [2, 0],
            }
            poss = pl.DataFrame(rows)
            X, y, pids = build_rapm_design(poss)
            print(X.shape)   # (2, 20)  — 2 possessions, 10 players × 2 sides
            print(pids[:3])  # [1, 2, 3]

        Downstream Ridge fit::

            from sportsdataverse.nba.nba_rapm import nba_rapm
            df = nba_rapm(poss)
            print(df.head())
    """
    if possessions.is_empty():
        return csr_matrix((0, 0)), np.empty(0), []

    # A possession missing any of its 10 lineup slots is unreliable for RAPM
    # (a partial <5-man lineup would otherwise inject a phantom player id via the
    # NaN->int64 sentinel). Drop those rows; never raise. (never-raise discipline)
    possessions = possessions.drop_nulls(subset=_OFF + _DEF)
    if possessions.is_empty():
        return csr_matrix((0, 0)), np.empty(0), []

    off = possessions.select(_OFF).to_numpy().astype(np.int64)
    deff = possessions.select(_DEF).to_numpy().astype(np.int64)

    pids = sorted(set(int(v) for v in np.concatenate([off.ravel(), deff.ravel()])))
    idx: dict[int, int] = {p: k for k, p in enumerate(pids)}
    P = len(pids)
    n = possessions.height

    rows: list[int] = []
    cols: list[int] = []
    for r in range(n):
        # off[r]/deff[r] each hold 5 DISTINCT player ids per the upstream contract;
        # csr_matrix sums duplicate (row,col) entries, so duplicates would corrupt the matrix.
        for p in off[r]:
            rows.append(r)
            cols.append(idx[int(p)])
        for p in deff[r]:
            rows.append(r)
            cols.append(P + idx[int(p)])

    data: np.ndarray = np.ones(len(rows), dtype=np.float64)
    X = csr_matrix((data, (rows, cols)), shape=(n, 2 * P))
    y = possessions["points"].to_numpy().astype(np.float64)
    return X, y, pids


def _empty_rapm_frame() -> pl.DataFrame:
    """Return a zero-row :class:`polars.DataFrame` with exactly :data:`RAPM_SCHEMA`."""
    return pl.DataFrame({col: pl.Series([], dtype=dtype) for col, dtype in RAPM_SCHEMA.items()})


def nba_rapm(
    possessions: pl.DataFrame,
    *,
    alphas: np.ndarray = DEFAULT_RAPM_ALPHAS,
) -> pl.DataFrame:
    """Fit plain Regularized Adjusted Plus-Minus (RAPM) via ridge regression.

    Builds the sparse offense/defense indicator design matrix from *possessions*
    (via :func:`build_rapm_design`) then fits a
    :class:`~sklearn.linear_model.RidgeCV` model with cross-validated alpha
    selection.  Coefficients are converted to a **per-100-possessions** scale.

    **Sign convention**

    * ``o_rapm`` = offensive coefficient × 100.  Positive means the player
      helped his team score more points per 100 possessions when on offense.
    * ``d_rapm`` = **negative** defensive coefficient × 100.  The regression
      models points *allowed*, so a good defender has a negative raw coefficient
      (he suppresses opponent scoring).  Negating produces a positive ``d_rapm``
      for good defenders — consistent with the convention that higher is better.
    * ``rapm`` = ``o_rapm + d_rapm``.

    .. note::
        This is a **plain-RAPM pilot** (single-season ridge, no Bayesian prior,
        no prior-year regression).  A full season of ~90 000 possessions is
        needed for meaningful individual estimates.  With fewer data the ridge
        penalty dominates and estimates shrink toward zero.

    Args:
        possessions: A :class:`polars.DataFrame` with columns
            ``off_player_1`` … ``off_player_5`` (Int64 player IDs on offense),
            ``def_player_1`` … ``def_player_5`` (Int64 player IDs on defense),
            and ``points`` (Int64 points scored on the possession).
            Rows with any null lineup cell are silently dropped.
            An empty or fully-null-lineup frame returns a zero-row result.
        alphas: 1-D array of ridge penalty values to evaluate via cross-
            validation.  Defaults to :data:`DEFAULT_RAPM_ALPHAS`
            (``np.logspace(2, 5, 8)``, i.e. 100 … 100 000).

    Returns:
        A :class:`polars.DataFrame` with exactly the columns defined in
        :data:`RAPM_SCHEMA`:

        * **player_id** (Int64) — player identifier.
        * **o_rapm** (Float64) — offensive RAPM per 100 possessions.
        * **d_rapm** (Float64) — defensive RAPM per 100 possessions
          (positive = good defender).
        * **rapm** (Float64) — total RAPM (``o_rapm + d_rapm``).
        * **off_poss** (Int64) — number of possessions the player appeared
          on offense.
        * **def_poss** (Int64) — number of possessions the player appeared
          on defense.

        Sorted ascending by ``player_id``.  Returns a zero-row frame (same
        schema) when *possessions* is empty or all lineups are incomplete.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_rapm import nba_rapm

            poss = pl.read_parquet("season_possessions.parquet")
            df = nba_rapm(poss)
            print(df.sort("rapm", descending=True).head(10))

        Custom alpha grid::

            import numpy as np
            df = nba_rapm(poss, alphas=np.logspace(1, 6, 12))

        Downstream filter::

            df.filter(pl.col("off_poss") >= 500).sort("rapm", descending=True)

        See Also:
            * `nflverse`_ — nflfastR EPA/WPA (parallel metric philosophy)
            * `nba_api`_ — upstream play-by-play source

        .. _nflverse: https://nflverse.nflverse.com
        .. _nba_api: https://github.com/swar/nba_api
    """
    X, y, player_ids = build_rapm_design(possessions)

    if not player_ids:
        return _empty_rapm_frame()

    P = len(player_ids)

    # Fit RidgeCV — accepts sparse csr_matrix with default solver="auto"
    model = RidgeCV(alphas=alphas, fit_intercept=True)
    model.fit(X, y)

    coef: np.ndarray = model.coef_  # shape (2P,)

    # Per-100-possessions scale; sign-flip defense so positive = good defender
    o_rapm: np.ndarray = coef[:P] * 100.0
    d_rapm: np.ndarray = -coef[P:] * 100.0
    rapm: np.ndarray = o_rapm + d_rapm

    # Possession counts: sum each design column (sparse column sums)
    col_sums: np.ndarray = np.asarray(X.sum(axis=0), dtype=np.float64).ravel()  # shape (2P,)
    off_poss: np.ndarray = col_sums[:P]
    def_poss: np.ndarray = col_sums[P:]

    df = pl.DataFrame(
        {
            "player_id": pl.Series(player_ids, dtype=pl.Int64),
            "o_rapm": pl.Series(o_rapm.astype(np.float64), dtype=pl.Float64),
            "d_rapm": pl.Series(d_rapm.astype(np.float64), dtype=pl.Float64),
            "rapm": pl.Series(rapm.astype(np.float64), dtype=pl.Float64),
            "off_poss": pl.Series(off_poss.astype(np.int64), dtype=pl.Int64),
            "def_poss": pl.Series(def_poss.astype(np.int64), dtype=pl.Int64),
        }
    ).sort("player_id")

    return df
