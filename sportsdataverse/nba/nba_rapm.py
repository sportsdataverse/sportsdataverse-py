"""Regularized Adjusted Plus-Minus (RAPM) pilot over the possession stint matrix.

This module builds the sparse design matrix consumed by the Ridge regression
solver (Task 2) from possession-level lineup data produced by
``attach_possession_lineups``.
"""

from __future__ import annotations

import numpy as np
import polars as pl
from scipy.sparse import csr_matrix

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

    Returns:
        A 3-tuple ``(X, y, player_ids)`` where:

        * **X** — :class:`scipy.sparse.csr_matrix` of shape ``(n_poss, 2P)``
          with dtype ``float64``.  Binary offense/defense indicators as
          described above.
        * **y** — :class:`numpy.ndarray` of shape ``(n_poss,)`` with dtype
          ``float64``.  Points scored on each possession.
        * **player_ids** — ``list[int]`` of length ``P``: the sorted distinct
          player IDs appearing in any lineup cell across the input.

        When *possessions* is empty, returns
        ``(csr_matrix((0, 0)), np.empty(0), [])``.

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

        Downstream Ridge fit (Task 2)::

            from sklearn.linear_model import RidgeCV
            model = RidgeCV(alphas=[1e2, 1e3, 1e4]).fit(X, y)
    """
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
