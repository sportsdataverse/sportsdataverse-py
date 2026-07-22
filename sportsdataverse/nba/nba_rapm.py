"""Regularized Adjusted Plus-Minus (RAPM) pilot over the possession stint matrix.

This module builds the sparse design matrix consumed by the Ridge regression
solver and fits a plain-RAPM via :class:`sklearn.linear_model.RidgeCV` from
possession-level lineup data produced by ``attach_possession_lineups``.
"""

from __future__ import annotations

from typing import Sequence, Union

import numpy as np
import pandas as pd
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


#: Sentinel id the low-observation replacement pool collapses into. Negative so
#: it can never collide with a real NBA person id.
REPLACEMENT_PLAYER_ID = -9999


def lambda_to_alpha(prior_possessions: float, mean_row_weight: float = 1.0) -> float:
    """Convert a possession-denominated ridge prior to a sklearn ``alpha``.

    The interpretability convention (WS6 fold-in): with one design row
    per possession and binary on-court indicators, the ridge penalty acts
    like ``prior_possessions`` of league-average evidence per player — so
    ``alpha`` IS the prior strength in possessions. When rows carry weights
    (aggregated stints), scale by the mean row weight to keep the same
    interpretation.

    Args:
        prior_possessions: Prior strength expressed in possessions of
            league-average evidence (e.g. 3000).
        mean_row_weight: Mean design-row weight (1.0 for per-possession rows).

    Returns:
        The equivalent sklearn ``Ridge``/``RidgeCV`` alpha.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nba.nba_rapm import lambda_to_alpha, nba_rapm
            df = nba_rapm(poss, alphas=np.array([lambda_to_alpha(3000.0)]))
    """
    return float(prior_possessions) * float(mean_row_weight)


def build_rapm_design(
    possessions: pl.DataFrame,
    *,
    replacement_min_obs: int | None = None,
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
        replacement_min_obs: When set, players below this combined
            possession count collapse into the :data:`REPLACEMENT_PLAYER_ID`
            column.  Two pooled players sharing the floor intentionally sum
            to 2 in that column — the count of replacement-level players on
            court is the correct design encoding (the distinct-ids duplicate
            warning below applies to REAL player ids only).

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

    if replacement_min_obs is not None:
        # The reference replacement-pool recipe: players below the observation floor
        # collapse into ONE replacement column instead of being dropped — their
        # possessions still inform everyone else's coefficients.
        all_ids = np.concatenate([off.ravel(), deff.ravel()])
        ids, counts = np.unique(all_ids, return_counts=True)
        low = ids[counts < replacement_min_obs]
        if low.size:
            off = np.where(np.isin(off, low), REPLACEMENT_PLAYER_ID, off)
            deff = np.where(np.isin(deff, low), REPLACEMENT_PLAYER_ID, deff)

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
    replacement_min_obs: int | None = None,
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
            (``np.logspace(2, 5, 8)``, i.e. 100 … 100 000).  Use
            :func:`lambda_to_alpha` to express the grid in possessions of
            prior evidence.
        replacement_min_obs: When set, players appearing on fewer than this
            many possessions (offense + defense combined) collapse into a
            single replacement pool (``player_id`` =
            :data:`REPLACEMENT_PLAYER_ID`) instead of receiving their own
            noisy coefficient — their possessions still inform everyone
            else's estimates.  ``None`` (default) keeps every player.

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
    X, y, player_ids = build_rapm_design(possessions, replacement_min_obs=replacement_min_obs)

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


def _fetch_possessions(game_id: str, league_id: str) -> pl.DataFrame:
    """Fetch the possession stint frame for *game_id* via :func:`~sportsdataverse.nba.nba_possessions.nba_possessions`.

    Module-level so tests can monkeypatch it without touching the public API.
    """
    # Local import: no cycle today, kept defensive against a future nba_possessions<->nba_rapm refactor.
    from .nba_possessions import nba_possessions

    return nba_possessions(game_id, league_id)


def nba_rapm_from_games(
    game_ids: Sequence[str],
    league_id: str = "00",
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Fit plain RAPM over one or more games by fetching possessions and combining them.

    For each game identifier in *game_ids*, the module-level
    :func:`_fetch_possessions` helper is called to retrieve the possession
    stint frame (monkeypatchable in tests).  Non-empty frames are concatenated
    with :func:`polars.concat` (``how="diagonal_relaxed"`` for schema
    robustness), then passed to :func:`nba_rapm`.

    **Important**: a single game typically has ~100 possessions — far too few
    for meaningful RAPM estimates.  A full regular season (~90 000 possessions
    for ~500 players) is the intended input.  With few possessions the ridge
    penalty dominates and all estimates shrink toward zero; the function never
    raises, but the estimates are not informative.

    **Sign convention** (same as :func:`nba_rapm`):

    * ``o_rapm`` — positive = player helped his team score more per 100
      possessions on offense.
    * ``d_rapm`` — positive = player suppressed opponent scoring (good
      defender).  The raw defensive regression coefficient is negated.
    * ``rapm`` = ``o_rapm + d_rapm``.

    **Per-100-possessions scale**: all three columns are multiplied by 100
    relative to the raw per-possession regression coefficients.

    Args:
        game_ids: Sequence of ten-character NBA game identifiers
            (e.g. ``["0022200001", "0022200002"]``).  An empty sequence
            returns a zero-row frame immediately without any network calls.
        league_id: League identifier forwarded to
            :func:`~sportsdataverse.nba.nba_possessions.nba_possessions`
            (default ``"00"`` for NBA).
        return_as_pandas: If ``True``, return a :class:`pandas.DataFrame`
            instead of :class:`polars.DataFrame`.

    Returns:
        A :class:`polars.DataFrame` (or :class:`pandas.DataFrame` when
        *return_as_pandas* is ``True``) with exactly the columns defined in
        :data:`RAPM_SCHEMA`:

        * **player_id** (Int64)
        * **o_rapm** (Float64) — offensive RAPM per 100 possessions
        * **d_rapm** (Float64) — defensive RAPM per 100 possessions
        * **rapm** (Float64)
        * **off_poss** (Int64)
        * **def_poss** (Int64)

        Returns a zero-row frame with :data:`RAPM_SCHEMA` when *game_ids*
        is empty or every game returns no possessions.  Never raises.

    Example:
        Quick start (single game — illustrative, not statistically meaningful)::

            from sportsdataverse.nba.nba_rapm import nba_rapm_from_games
            df = nba_rapm_from_games(["0022200001"])
            print(df.sort("rapm", descending=True).head(10))

        Full-season batch (meaningful estimates)::

            import polars as pl
            from sportsdataverse.nba.nba_schedule import load_nba_schedule
            schedule = load_nba_schedule(seasons=[2023])
            game_ids = schedule["game_id"].to_list()
            df = nba_rapm_from_games(game_ids)
            df.filter(pl.col("off_poss") >= 500).sort("rapm", descending=True)

        Pandas output::

            df_pd = nba_rapm_from_games(["0022200001"], return_as_pandas=True)
            print(type(df_pd))

        See Also:
            * `08-nba-rapm`_ — Evan Zamir's plain-RAPM reference implementation
            * `nba_api`_ — upstream play-by-play source used by sdv-py

        .. _08-nba-rapm: https://github.com/EvanZ/nba-rapm
        .. _nba_api: https://github.com/swar/nba_api
    """
    if not game_ids:
        result: pl.DataFrame = _empty_rapm_frame()
        if return_as_pandas:
            return result.to_pandas()
        return result

    frames: list[pl.DataFrame] = []
    for gid in game_ids:
        poss = _fetch_possessions(gid, league_id)
        if not poss.is_empty():
            frames.append(poss)

    if not frames:
        result = _empty_rapm_frame()
        if return_as_pandas:
            return result.to_pandas()
        return result

    combined = pl.concat(frames, how="diagonal_relaxed")
    result = nba_rapm(combined)
    if return_as_pandas:
        return result.to_pandas()
    return result
