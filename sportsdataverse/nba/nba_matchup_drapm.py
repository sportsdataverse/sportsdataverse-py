"""(2) Matchup defensive RAPM -- a two-way-FE ridge on partial-possession matchups.

Reuses the shipped :mod:`sportsdataverse.nba.nba_rapm` CSR-construction idiom
and :class:`~sklearn.linear_model.RidgeCV` fit path verbatim (a new
:func:`build_matchup_drapm_design` sibling to
:func:`~sportsdataverse.nba.nba_rapm.build_rapm_design`), so the ridge/scaling
machinery is shared, not re-implemented. See the design spec
(``2026-07-07-nba-playtype-impact-design.md`` §3.4) for the methodology.
"""

from __future__ import annotations

from typing import Optional, Union

import numpy as np
import pandas as pd
import polars as pl
from scipy.sparse import csr_matrix
from sklearn.linear_model import RidgeCV

from sportsdataverse.nba.nba_playtype_constants import PlaytypeConfig

DRAPM_SCHEMA: dict[str, type[pl.DataType]] = {
    "player_id": pl.Int64,
    "matchup_drapm": pl.Float64,
    "matchup_poss": pl.Float64,
}


def _empty_drapm_frame() -> pl.DataFrame:
    return pl.DataFrame({c: pl.Series([], dtype=d) for c, d in DRAPM_SCHEMA.items()})


def build_matchup_drapm_design(
    matchups: pl.DataFrame,
    *,
    min_poss: float = 25.0,
) -> tuple[csr_matrix, np.ndarray, np.ndarray, list[int], list[int]]:
    """Build the two-way fixed-effects design matrix for matchup DRAPM.

    Mirrors :func:`~sportsdataverse.nba.nba_rapm.build_rapm_design`'s CSR
    idiom. Column layout is **defender-first then offense**: columns
    ``0 .. D-1`` are defender one-hots, ``D .. D+O-1`` are offense one-hots
    (offense columns are controls, absorbing "this defender guarded an
    easy/hard assignment").

    Args:
        matchups: One row per (offensive player, defender) pair for the
            season: ``off_player_id``/``def_player_id`` (Int64), ``partial_poss``
            (Float64, matchup possessions), ``player_pts`` (Float64, points the
            offensive player scored in that matchup).
        min_poss: Drop rows with ``partial_poss`` below this floor (a single
            near-zero-possession matchup row is unreliable for the ridge).

    Returns:
        5-tuple ``(X, y, w, def_ids, off_ids)``:

        * **X** -- :class:`scipy.sparse.csr_matrix`, shape ``(n_rows, D+O)``.
        * **y** -- points allowed per 100 matchup possessions,
          ``100 * player_pts / partial_poss``.
        * **w** -- sample weights, ``partial_poss``.
        * **def_ids** / **off_ids** -- sorted distinct Int64 ids, column order.

        Empty input (or nothing survives the ``min_poss`` floor) returns
        ``(csr_matrix((0, 0)), empty, empty, [], [])``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_matchup_drapm import build_matchup_drapm_design
            X, y, w, def_ids, off_ids = build_matchup_drapm_design(matchups_df)
            print(X.shape)
    """
    if matchups.is_empty():
        return csr_matrix((0, 0)), np.empty(0), np.empty(0), [], []

    m = matchups.filter(pl.col("partial_poss") >= min_poss)
    if m.is_empty():
        return csr_matrix((0, 0)), np.empty(0), np.empty(0), [], []

    def_ids = sorted(m["def_player_id"].cast(pl.Int64).unique().to_list())
    off_ids = sorted(m["off_player_id"].cast(pl.Int64).unique().to_list())
    d_idx = {p: i for i, p in enumerate(def_ids)}
    o_idx = {p: i for i, p in enumerate(off_ids)}
    n_def, n_off = len(def_ids), len(off_ids)
    n = m.height

    def_col = m["def_player_id"].cast(pl.Int64).to_list()
    off_col = m["off_player_id"].cast(pl.Int64).to_list()
    rows = list(range(n)) + list(range(n))
    cols = [d_idx[p] for p in def_col] + [n_def + o_idx[p] for p in off_col]
    data = np.ones(2 * n, dtype=np.float64)
    X = csr_matrix((data, (rows, cols)), shape=(n, n_def + n_off))

    partial_poss = m["partial_poss"].cast(pl.Float64).to_numpy()
    y = 100.0 * m["player_pts"].cast(pl.Float64).to_numpy() / partial_poss
    w = partial_poss

    return X, y, w, def_ids, off_ids


def nba_matchup_drapm(
    season: str,
    *,
    league_id: str = "00",
    matchups: Optional[pl.DataFrame] = None,
    config: Optional[PlaytypeConfig] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Matchup-based defensive RAPM (offense-quality-controlled).

    Fits ``points_allowed_per_100 ~ defender_FE + offense_FE`` via
    :class:`~sklearn.linear_model.RidgeCV` (weighted by matchup possessions),
    reusing the shipped RAPM ridge machinery on the
    :func:`build_matchup_drapm_design` two-way-FE design.

    **Sign + scale:** the design target ``y`` is already points-allowed *per 100*
    matchup possessions (``100 * player_pts / partial_poss``), so the defender
    coefficient is already on the per-100 scale -- ``matchup_drapm =
    -(beta_defender - mean_beta_defender)`` (centered, NO extra ×100, unlike
    :func:`~sportsdataverse.nba.nba_rapm.nba_rapm` whose ``y`` is per-*possession*
    and needs the ×100). Sign is negated so higher = better defense (fewer points
    allowed), matching the ``d_rapm`` convention. Typical magnitudes are a few to
    low-double-digit points per 100 vs the league defender average.

    Args:
        season: Season string, e.g. ``"2023-24"``.
        league_id: ``"00"`` NBA (default), ``"10"`` WNBA, ``"20"`` G-League.
        matchups: Injected ``nba_stats_leagueseasonmatchups``-shaped frame
            (bypasses the live fetch -- used for tests / oracle fixtures).
        config: :class:`~sportsdataverse.nba.nba_playtype_constants.PlaytypeConfig`;
            defaults to a fresh instance (``ridge_alphas`` = the shared RAPM grid,
            ``min_matchup_poss`` = 25.0 inclusion floor).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per defender: ``player_id`` (Int64), ``matchup_drapm`` (Float64,
        points-allowed-per-100 estimate, higher = better defense),
        ``matchup_poss`` (Float64, total matchup possessions guarded). Returns
        a zero-row frame with this schema when the upstream fetch/injection is
        empty or no row survives the ``min_matchup_poss`` floor (sparse-coverage
        leagues never raise).

    Example:
        Quick start::

            from sportsdataverse.nba import nba_matchup_drapm
            d = nba_matchup_drapm("2023-24")
            print(d.sort("matchup_drapm", descending=True).head(10))

        Injected offline (oracle / test) path::

            d = nba_matchup_drapm("2023-24", matchups=matchups_df)

        Pipeline next step::

            d.filter(pl.col("matchup_poss") >= 200).sort("matchup_drapm", descending=True)

        See Also:
            * `nba_api`_ -- upstream ``leagueseasonmatchups`` source

        .. _nba_api: https://github.com/swar/nba_api
    """
    cfg = config if config is not None else PlaytypeConfig()

    if matchups is None:
        from sportsdataverse.nba.nba_stats import nba_stats_leagueseasonmatchups

        matchups = nba_stats_leagueseasonmatchups(league_id=league_id, season=season, per_mode_simple="Totals")

    X, y, w, def_ids, off_ids = build_matchup_drapm_design(matchups, min_poss=cfg.min_matchup_poss)

    if X.shape[0] == 0 or not def_ids:
        out = _empty_drapm_frame()
        return out.to_pandas() if return_as_pandas else out

    model = RidgeCV(alphas=cfg.ridge_alphas, fit_intercept=True)
    model.fit(X, y, sample_weight=w)
    beta_def = model.coef_[: len(def_ids)]
    # y is already per-100 (100*player_pts/partial_poss), so beta_def is per-100 too --
    # center and negate (higher = better D), NO extra ×100 (that was a double-scale bug).
    drapm = -(beta_def - beta_def.mean())

    # matchup_poss per defender: sum of partial_poss over rows involving that defender
    poss_by_def = np.asarray(X[:, : len(def_ids)].T.dot(w)).ravel()

    out = pl.DataFrame(
        {
            "player_id": pl.Series(def_ids, dtype=pl.Int64),
            "matchup_drapm": pl.Series(drapm.astype(np.float64), dtype=pl.Float64),
            "matchup_poss": pl.Series(poss_by_def.astype(np.float64), dtype=pl.Float64),
        }
    ).sort("player_id")

    return out.to_pandas() if return_as_pandas else out
