"""Regularized Adjusted Plus-Minus (RAPM) pilot over the possession stint matrix.

This module builds the sparse design matrix consumed by the ridge solver and fits a plain RAPM
from possession-level lineup data produced by ``attach_possession_lineups``, at one fixed ridge
penalty per league (:data:`RAPM_PENALTY`).
"""

from __future__ import annotations

from typing import Sequence, Union

import numpy as np
import pandas as pd
import polars as pl
from scipy.sparse import csr_matrix
from sklearn.linear_model import RidgeCV

from sportsdataverse._deprecation import warn_deprecated

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

#: Alpha grid for RidgeCV (logspace 100 … 100 000, 8 points). Plain RAPM no longer picks its penalty
#: from it (see :data:`RAPM_PENALTY`); the RAPM variants, the adjusted RAPM and the validation harness
#: still cross-validate over it.
DEFAULT_RAPM_ALPHAS: np.ndarray = np.logspace(2, 5, 8)

#: The plain-RAPM ridge penalty per league id, one value for every season and season type. Fitted by
#: ``tools/fit_rapm_penalty.py`` (run 2026-11-06): the point of its grid (steps of x1.18) that
#: minimizes the game-grouped 10-fold CV error summed over the complete regular seasons from 1997
#: (NBA through 2025-26, WNBA through 2026; 30 each) of the ``{nba,wnba}_stats_possessions`` releases.
#: NBA 2,683: its neighbours lose 0.05 / 0.09 basis points of MSE (SE 0.03 across seasons) and 5,179
#: loses 1.07 (all 30 seasons prefer 2,683 to it). WNBA 1,931: 2,276 is within noise (0.05 basis
#: points, SE 0.06), 1,638 loses 0.14 and 5,179 loses 2.6. The penalty is the per-possession noise
#: variance over the prior variance of a player's coefficient, which does not depend on a season's
#: length, so one value keeps the same prior, and the RAPM scale, in every season (a player's own
#: shrinkage still depends on his possessions: the NBA regular-season spread is 1.53 per 100 in the
#: 1999 lockout, 1.65 to 1.99 in full seasons). Until this was frozen each fit took RidgeCV's
#: leave-one-possession-out pick over :data:`DEFAULT_RAPM_ALPHAS`: one season's error moves by under
#: 0.5% over that whole grid, so the pick flipped between grid points (NBA 5,179 in 2023-24 and 2024-25,
#: 1,931 in 2025-26; WNBA up to 13,895 in 2004) and the spread of RAPM followed (NBA 1.15 to 2.34 per
#: 100). RAPM fit before the change is not comparable across seasons, nor with RAPM fit after it.
#: Two caveats. Hindsight: the value is pooled over every complete season, so a season's fit uses a
#: scalar its later seasons informed; pooled over only that season and the ones before it, the pick is
#: the frozen value for every NBA season from 2018-19 (2,276 before) and every WNBA season but 1997,
#: 1999-2007 and 2016 (2,276 there). Year-over-year rank stability: it rises with the penalty (NBA
#: Spearman 0.35 at 1,931, 0.37 at 2,683, 0.40 at 5,179), so the old rule's heavy picks bought some;
#: against it the frozen value repeats at WNBA -0.008 Spearman (SE 0.004 over 29 season pairs) and NBA
#: +0.001 (0.004), while next-season R^2 of a fitted line improves (NBA +0.027, SE 0.009). The G League
#: (``"20"``) is seeded with the NBA value, not fitted (it has no possessions release).
RAPM_PENALTY: dict[str, float] = {"00": 2682.6957952797247, "10": 1930.6977288832495}
RAPM_PENALTY["20"] = RAPM_PENALTY["00"]

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


def rapm_penalty(possessions: pl.DataFrame | None = None, league_id: str | None = None) -> float:
    """The ridge penalty plain RAPM is fit at: :data:`RAPM_PENALTY` of the possessions' league.

    Args:
        possessions: Possession rows. When ``league_id`` is ``None`` the league is read from the
            ``game_id``: the first two digits of the ten-digit id (``"00"`` NBA, ``"10"`` WNBA, ``"20"``
            G League; an integer id is zero-padded to ten digits first). A frame without a ``game_id``
            is taken as NBA.
        league_id: ``"00"``, ``"10"`` or ``"20"``; overrides the inference.

    Returns:
        The penalty.

    Raises:
        ValueError: If ``league_id`` is not a key of :data:`RAPM_PENALTY`, the ``game_id`` prefixes name
            more than one league, or they name another league than ``league_id``.

    Example:
        The penalty a WNBA season is fit at::

            from sportsdataverse.nba.nba_rapm import rapm_penalty
            rapm_penalty(league_id="10")  # 1930.70, the same in every season
    """
    return RAPM_PENALTY[_possessions_league(possessions, league_id)]


def _possessions_league(possessions: pl.DataFrame | None = None, league_id: str | None = None) -> str:
    """The league id of a possession frame: ``league_id``, else the ``game_id`` prefix, else ``"00"``.

    Args:
        possessions: Possession rows; the first two digits of the ten-digit ``game_id`` name the
            league (an integer id is zero-padded first). A frame without a ``game_id`` is taken as NBA.
        league_id: ``"00"``, ``"10"`` or ``"20"``; overrides the inference.

    Returns:
        ``"00"``, ``"10"`` or ``"20"``.

    Raises:
        ValueError: If ``league_id`` is not one of them, the ``game_id`` prefixes name more than one
            league, or they name another league than ``league_id``.
    """
    found: set[str] = set()
    if possessions is not None and "game_id" in possessions.columns:
        ids = possessions["game_id"].cast(pl.Utf8).str.zfill(10)
        found = set(ids.str.slice(0, 2).drop_nulls().unique()) & set(RAPM_PENALTY)
    if len(found) > 1 or (league_id is not None and found and found != {league_id}):
        raise ValueError(f"possessions' game_id prefixes name leagues {sorted(found)}, league_id is {league_id!r}")
    league = league_id or (found.pop() if found else "00")
    if league not in RAPM_PENALTY:
        raise ValueError(f"league_id must be one of {sorted(RAPM_PENALTY)}, got {league!r}")
    return league


def ridge_rapm_coef(X: csr_matrix, y: np.ndarray, alpha: float) -> np.ndarray:
    """The intercept-fitted ridge coefficients of ``y`` on ``X`` at ``alpha``, solved exactly.

    The intercept is not penalized: the design is centred through its Gram matrix (``X'X - n m m'``),
    so the solve is one dense ``2P x 2P`` system and no iterative tolerance enters the result.

    Args:
        X: The ``(n, 2P)`` design of :func:`build_rapm_design`.
        y: The ``(n,)`` responses, or ``(n, k)`` for several at once.
        alpha: The ridge penalty.

    Returns:
        The ``(2P,)`` (or ``(2P, k)``) coefficients.

    Raises:
        ValueError: If ``alpha`` is not positive (the centred Gram matrix is singular without it).

    Example:
        The coefficients :func:`nba_rapm` reports, before the per-100 scaling::

            from sportsdataverse.nba.nba_rapm import RAPM_PENALTY, build_rapm_design, ridge_rapm_coef
            X, y, player_ids = build_rapm_design(possessions)
            coef = ridge_rapm_coef(X, y, RAPM_PENALTY["00"])
    """
    if not alpha > 0:
        raise ValueError(f"the ridge penalty must be positive, got {alpha!r}")
    n = X.shape[0]
    mean = np.asarray(X.mean(axis=0)).ravel()
    gram = (X.T @ X).toarray() - n * np.outer(mean, mean)
    gram[np.diag_indices_from(gram)] += alpha
    rhs = np.asarray(X.T @ y) - n * np.multiply.outer(mean, y.mean(axis=0))
    return np.linalg.solve(gram, rhs)


def nba_rapm(
    possessions: pl.DataFrame,
    *,
    league_id: str | None = None,
    alpha: float | None = None,
    alphas: np.ndarray | None = None,
) -> pl.DataFrame:
    """Fit plain Regularized Adjusted Plus-Minus (RAPM) via ridge regression.

    Builds the sparse offense/defense indicator design matrix from *possessions*
    (via :func:`build_rapm_design`) then fits an intercept-fitted ridge at the league's
    fixed penalty (:data:`RAPM_PENALTY`, see :func:`rapm_penalty`), the same in every
    season and season type so the RAPM scale is comparable across them.  Coefficients
    are converted to a **per-100-possessions** scale.

    .. warning::
        Until the penalty was frozen (November 2026) each call picked it by
        :class:`~sklearn.linear_model.RidgeCV` leave-one-possession-out over
        :data:`DEFAULT_RAPM_ALPHAS`, which jumped between grid points from season to
        season (NBA 5,179 in 2023-24 and 2024-25, 1,931 in 2025-26). RAPM fit that way,
        including ``nba_player_impact`` / ``wnba_player_impact`` releases published before
        the change, is on a different and season-varying scale: do not compare it across
        seasons or with RAPM fit now.

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
        league_id: ``"00"`` NBA, ``"10"`` WNBA or ``"20"`` G League, which selects the
            penalty. ``None`` (default) reads it from the ``game_id`` prefix and
            otherwise takes the NBA (see :func:`rapm_penalty`).
        alpha: A ridge penalty to fit at instead of the league's (positive).
        alphas: **Deprecated** (removed in 0.4.0): the old rule, RidgeCV's leave-one-out pick over
            this grid for this fit alone, whose scale is not comparable across seasons. Warns
            ``DeprecationWarning``; pass ``alpha=`` instead.

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

        WNBA possessions (the penalty is the WNBA's; the game ids say so too)::

            wnba = nba_rapm(wnba_poss, league_id="10")

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

    if alphas is not None:
        warn_deprecated(
            "nba_rapm(alphas=...)",
            replacement="nba_rapm(alpha=...) or the league penalty (league_id=...)",
            since="0.2.0",
            removed_in="0.4.0",
            extra="It picks the penalty per fit, so its RAPM scale is not comparable across seasons.",
        )
        penalty = float(RidgeCV(alphas=alphas).fit(X, y).alpha_) if X.shape[0] > 1 else float(max(alphas))
    else:
        penalty = rapm_penalty(possessions, league_id) if alpha is None else alpha
    coef: np.ndarray = ridge_rapm_coef(X, y, penalty)  # shape (2P,)

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
    result = nba_rapm(combined, league_id=league_id)
    if return_as_pandas:
        return result.to_pandas()
    return result
