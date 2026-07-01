"""Out-of-sample validation harness for the NBA model zoo.

A model is a design-matrix estimator (``fit(X, y) -> FitResult``); the harness
owns the player-id column map (from ``build_rapm_design``) and scores fitted
coefficients against held-out games. See the spec for the four oracles and the
synthetic meta-oracle that proves the harness itself correct.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Protocol, Tuple

import numpy as np
import polars as pl
from scipy.sparse import csr_matrix
from sklearn.linear_model import RidgeCV

from .nba_rapm import DEFAULT_RAPM_ALPHAS

_OFF: List[str] = [f"off_player_{i}" for i in range(1, 6)]
_DEF: List[str] = [f"def_player_{i}" for i in range(1, 6)]


@dataclass(frozen=True)
class FitResult:
    """A fitted RAPM-family model's coefficients on the raw per-possession scale.

    Attributes:
        coef: Shape ``(2P,)``; offense columns ``0..P-1`` then defense ``P..2P-1``,
            index-aligned to the ``player_ids`` the harness built the design with.
        intercept: Scalar regression intercept.
        posterior: Optional ``(S, 2P)`` posterior samples; only models that emit
            uncertainty set it (enables interval calibration). ``None`` for point models.
    """

    coef: np.ndarray
    intercept: float
    posterior: Optional[np.ndarray] = None


class RapmModel(Protocol):
    """Design-matrix estimator: fit a sparse design ``X`` against targets ``y``."""

    def fit(self, X: csr_matrix, y: np.ndarray) -> FitResult: ...


class RidgeRapmModel:
    """Reference model: the merged plain-RAPM RidgeCV fit, adapted to ``RapmModel``.

    Args:
        alphas: Ridge penalty grid for cross-validation. Defaults to the merged
            ``DEFAULT_RAPM_ALPHAS``.
    """

    def __init__(self, alphas: np.ndarray = DEFAULT_RAPM_ALPHAS) -> None:
        self._alphas = alphas

    def fit(self, X: csr_matrix, y: np.ndarray) -> FitResult:
        """Fit RidgeCV and return coefficients + intercept (no posterior).

        Args:
            X: Sparse design matrix of shape ``(n_possessions, 2P)``.
            y: Target points per possession, shape ``(n_possessions,)``.

        Returns:
            FitResult with ``coef`` shape ``(2P,)``, scalar ``intercept``,
            and ``posterior=None``.
        """
        model = RidgeCV(alphas=self._alphas, fit_intercept=True)
        model.fit(X, y)
        return FitResult(
            coef=np.asarray(model.coef_, dtype=np.float64),
            intercept=float(model.intercept_),
            posterior=None,
        )


def _design_with_ids(
    possessions: pl.DataFrame,
    player_ids: List[int],
    *,
    unknown_player_rating: float = 0.0,  # v1: unknown players contribute 0 (league-average); reserved for a future non-zero prior
) -> Tuple[csr_matrix, np.ndarray]:
    """Build a design matrix against a FIXED ``player_ids`` column map.

    Used to score a held-out split with the training fit: a player absent from
    ``player_ids`` (unseen in training) has no column and contributes nothing to
    the prediction (the ``unknown_player_rating=0.0`` neutral prior). Rows with a
    null lineup cell are dropped (mirrors ``build_rapm_design``).

    Args:
        possessions: Held-out possession frame (same lineup + ``points`` columns).
        player_ids: The training design's sorted player ids (defines the columns).
        unknown_player_rating: Reserved; only ``0.0`` (skip unknown players) is
            implemented in v1.

    Returns:
        ``(X, y)`` with ``X`` shape ``(n_rows, 2 * len(player_ids))`` float64 and
        ``y`` the possession points. Empty input → ``(csr_matrix((0, 2P)), empty)``.
    """
    P = len(player_ids)
    if possessions.is_empty() or P == 0:
        return csr_matrix((0, 2 * P)), np.empty(0, dtype=np.float64)
    possessions = possessions.drop_nulls(subset=_OFF + _DEF)
    if possessions.is_empty():
        return csr_matrix((0, 2 * P)), np.empty(0, dtype=np.float64)

    idx = {p: k for k, p in enumerate(player_ids)}
    off = possessions.select(_OFF).to_numpy().astype(np.int64)
    deff = possessions.select(_DEF).to_numpy().astype(np.int64)
    n = possessions.height
    rows: List[int] = []
    cols: List[int] = []
    for r in range(n):
        for p in off[r]:
            c = idx.get(int(p))
            if c is not None:
                rows.append(r)
                cols.append(c)
        for p in deff[r]:
            c = idx.get(int(p))
            if c is not None:
                rows.append(r)
                cols.append(P + c)
    data: np.ndarray = np.ones(len(rows), dtype=np.float64)
    X = csr_matrix((data, (rows, cols)), shape=(n, 2 * P))
    y = possessions["points"].to_numpy().astype(np.float64)
    return X, y


def predict_points(X: csr_matrix, fit: FitResult) -> np.ndarray:
    """Predicted offense points per possession: ``X @ coef + intercept``.

    Args:
        X: Sparse design matrix of shape ``(n_possessions, 2P)``.
        fit: Fitted model result from ``RapmModel.fit``.

    Returns:
        Float64 array of shape ``(n_possessions,)`` with predicted points.
    """
    return np.asarray(X @ fit.coef, dtype=np.float64) + fit.intercept
