"""Expected Threat (xT) on a 12 x 16 grid (port of socceraction's xthreat module).

Ported to polars from socceraction ``xthreat.py``
(https://github.com/ML-KULeuven/socceraction, commit 93a1242).

MIT License. Copyright (c) 2019 KU Leuven Machine Learning Research Group.
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
associated documentation files (the "Software"), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute,
sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions: The above copyright notice and this
permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES
OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Departures from the original: convergence is tested on the absolute change with a ``max_iter`` cap;
rows with null coordinates are excluded from every count including the transition matrix; there is no
interpolated ``rate`` (SciPy's ``interp2d`` no longer exists).

Reference: Singh, Karun. "Introducing Expected Threat (xT)", 2019, https://karun.in/blog/expected-threat.html.
"""

from __future__ import annotations

import numpy as np
import polars as pl

from sportsdataverse.soccer.spadl import FIELD_LENGTH, FIELD_WIDTH

__all__ = ["NotFittedError"]

GRID_W: int = 12  # cells across the width (y)
GRID_L: int = 16  # cells along the length (x)
MOVE_TYPES: tuple[str, ...] = ("pass", "dribble", "cross")


class NotFittedError(RuntimeError):
    """The grid is all zeros: call ``fit`` or load a model first."""


def _cell_indexes(x: np.ndarray, y: np.ndarray, l: int, w: int) -> tuple[np.ndarray, np.ndarray]:  # noqa: E741
    """Convert continuous coordinates to cell indexes.

    Parameters
    ----------
    x : np.ndarray
        x-coordinates (0 to FIELD_LENGTH).
    y : np.ndarray
        y-coordinates (0 to FIELD_WIDTH).
    l : int
        Number of cells along the length (x).
    w : int
        Number of cells along the width (y).

    Returns
    -------
    tuple[np.ndarray, np.ndarray]
        (xi, yj) cell indexes, clipped to valid range.
    """
    xi = np.clip((x / FIELD_LENGTH * l).astype(np.int64), 0, l - 1)
    yj = np.clip((y / FIELD_WIDTH * w).astype(np.int64), 0, w - 1)
    return xi, yj


def _flat_indexes(x: np.ndarray, y: np.ndarray, l: int, w: int) -> np.ndarray:  # noqa: E741
    """Convert continuous coordinates to flat indexes in row-major order.

    The grid origin (top-left) is at (0, 0) in the visual representation,
    but flat indexes are numbered with row 0 at the top (y = FIELD_WIDTH).

    Parameters
    ----------
    x : np.ndarray
        x-coordinates (0 to FIELD_LENGTH).
    y : np.ndarray
        y-coordinates (0 to FIELD_WIDTH).
    l : int
        Number of cells along the length (x).
    w : int
        Number of cells along the width (y).

    Returns
    -------
    np.ndarray
        Flat indexes into a (w * l,) shaped array.
    """
    xi, yj = _cell_indexes(x, y, l, w)
    return (w - 1 - yj) * l + xi


def _finite(x: np.ndarray, y: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Filter out NaN coordinates."""
    keep = ~(np.isnan(x) | np.isnan(y))
    return x[keep], y[keep]


def _count(x: np.ndarray, y: np.ndarray, l: int, w: int) -> np.ndarray:  # noqa: E741
    """Count occurrences in each grid cell.

    Parameters
    ----------
    x : np.ndarray
        x-coordinates.
    y : np.ndarray
        y-coordinates.
    l : int
        Number of cells along length.
    w : int
        Number of cells along width.

    Returns
    -------
    np.ndarray
        Shape (w, l) count matrix, with origin at top-left.
    """
    x, y = _finite(x, y)
    counts = np.bincount(_flat_indexes(x, y, l, w), minlength=w * l)
    return counts.reshape((w, l))


def _safe_divide(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Divide arrays, returning 0 where denominator is 0."""
    return np.divide(a, b, out=np.zeros_like(a, dtype=np.float64), where=b != 0)


def _xy(df: pl.DataFrame, prefix: str) -> tuple[np.ndarray, np.ndarray]:
    """Extract x, y columns by prefix as float64 numpy arrays."""
    return df[f"{prefix}_x"].to_numpy().astype(np.float64), df[f"{prefix}_y"].to_numpy().astype(np.float64)


def _scoring_prob(actions: pl.DataFrame, l: int, w: int) -> np.ndarray:  # noqa: E741
    """Compute shot success probability per grid cell.

    Parameters
    ----------
    actions : pl.DataFrame
        SPADL actions with type_name, result_name, start_x, start_y columns.
    l : int
        Number of cells along length.
    w : int
        Number of cells along width.

    Returns
    -------
    np.ndarray
        Shape (w, l) probability matrix (0 to 1).
    """
    shots = actions.filter(pl.col("type_name") == "shot")
    goals = shots.filter(pl.col("result_name") == "success")
    return _safe_divide(_count(*_xy(goals, "start"), l, w), _count(*_xy(shots, "start"), l, w))


def _moves(actions: pl.DataFrame) -> pl.DataFrame:
    """Filter to move-type actions (pass, dribble, cross)."""
    return actions.filter(pl.col("type_name").is_in(list(MOVE_TYPES)))


def _action_prob(actions: pl.DataFrame, l: int, w: int) -> tuple[np.ndarray, np.ndarray]:  # noqa: E741
    """Compute probability of shooting vs moving per grid cell.

    Parameters
    ----------
    actions : pl.DataFrame
        SPADL actions.
    l : int
        Number of cells along length.
    w : int
        Number of cells along width.

    Returns
    -------
    tuple[np.ndarray, np.ndarray]
        (shot_prob, move_prob) each shape (w, l).
    """
    move = _count(*_xy(_moves(actions), "start"), l, w)
    shot = _count(*_xy(actions.filter(pl.col("type_name") == "shot"), "start"), l, w)
    total = move + shot
    return _safe_divide(shot, total), _safe_divide(move, total)


def _move_transition_matrix(actions: pl.DataFrame, l: int, w: int) -> np.ndarray:  # noqa: E741
    """Compute successful move transition matrix.

    Parameters
    ----------
    actions : pl.DataFrame
        SPADL actions with type_name, result_name, start_x, start_y, end_x, end_y columns.
    l : int
        Number of cells along length.
    w : int
        Number of cells along width.

    Returns
    -------
    np.ndarray
        Shape (w*l, w*l) transition matrix. Row i sums to 1 if cell i had moves.
    """
    m = _moves(actions).drop_nulls(["start_x", "start_y", "end_x", "end_y"])
    start = _flat_indexes(*_xy(m, "start"), l, w)
    end = _flat_indexes(*_xy(m, "end"), l, w)
    ok = (m["result_name"] == "success").to_numpy()
    n = w * l
    start_counts: np.ndarray = np.bincount(start, minlength=n).astype(np.float64)
    T = np.zeros((n, n))
    np.add.at(T, (start[ok], end[ok]), 1.0)
    return _safe_divide(T, start_counts[:, None])
