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

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

import numpy as np
import polars as pl

from sportsdataverse.soccer.spadl import FIELD_LENGTH, FIELD_WIDTH

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["XThreat", "NotFittedError", "load_xthreat_model", "soccer_xthreat_rate"]

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

    Args:
        actions: SPADL actions with type_name, result_name, start_x, start_y, end_x, end_y columns.
        l: Number of cells along length.
        w: Number of cells along width.

    Returns:
        Shape (w*l, w*l) transition matrix. Rows sum to at most 1 (failed moves stay in denominator).
    """
    m = _moves(actions).drop_nulls(["start_x", "start_y", "end_x", "end_y"])
    start = _flat_indexes(*_xy(m, "start"), l, w)
    end = _flat_indexes(*_xy(m, "end"), l, w)
    ok = (m["result_name"] == "success").fill_null(False).to_numpy()
    n = w * l
    start_counts: np.ndarray = np.bincount(start, minlength=n).astype(np.float64)
    T = np.zeros((n, n))
    np.add.at(T, (start[ok], end[ok]), 1.0)
    return _safe_divide(T, start_counts[:, None])


class XThreat:
    """A fitted Expected Threat grid.

    Args:
        grid: An existing ``(w, l)`` array (row 0 = the top of the pitch); ``None`` for an unfitted model.
        l: Cells along the pitch length.
        w: Cells across the pitch width.
        eps: Convergence tolerance on the absolute change of every cell.
        max_iter: Iteration cap; exceeding it raises ``RuntimeError``.
        meta: Free-form provenance stored in the JSON.

    Example:
        Fit on SPADL actions and value the moves::

            from sportsdataverse.soccer import XThreat, soccer_open_dataset, soccer_spadl
            actions = soccer_spadl(soccer_open_dataset("statsbomb", 8658))
            model = XThreat().fit(actions)
            actions = actions.with_columns(model.rate(actions))

    See Also:
        * `socceraction`_ -- the original implementation (MIT)
        * `Expected Threat`_ -- Karun Singh's description of the model

    .. _socceraction: https://github.com/ML-KULeuven/socceraction
    .. _Expected Threat: https://karun.in/blog/expected-threat.html
    """

    def __init__(
        self,
        grid: Optional[np.ndarray] = None,
        *,
        l: int = GRID_L,  # noqa: E741
        w: int = GRID_W,
        eps: float = 1e-5,
        max_iter: int = 1000,
        meta: Optional[dict[str, Any]] = None,
    ) -> None:
        self.l, self.w, self.eps, self.max_iter = l, w, eps, max_iter
        self.xT: np.ndarray = np.zeros((w, l)) if grid is None else np.asarray(grid, dtype=np.float64)
        if self.xT.shape != (w, l):
            raise ValueError(f"grid must have shape ({w}, {l}); got {self.xT.shape}")
        self.iterations = 0
        self.meta: dict[str, Any] = dict(meta or {})

    def fit(self, actions: pl.DataFrame) -> XThreat:
        """Fit the grid on SPADL actions by value iteration.

        Args:
            actions: SPADL actions with ``type_name``, ``result_name`` and start/end coordinates.

        Returns:
            This model, fitted in place.

        Raises:
            RuntimeError: If the grid has not converged within ``max_iter`` iterations.
        """
        p_scoring = _scoring_prob(actions, self.l, self.w)
        p_shot, p_move = _action_prob(actions, self.l, self.w)
        T = _move_transition_matrix(actions, self.l, self.w)
        gs = p_scoring * p_shot
        xT = np.zeros((self.w, self.l))
        for it in range(1, self.max_iter + 1):
            new = gs + p_move * (T @ xT.ravel()).reshape(self.w, self.l)
            done = np.abs(new - xT).max() <= self.eps
            xT = new
            if done:
                self.iterations = it
                break
        else:
            raise RuntimeError(f"xT did not converge in {self.max_iter} iterations (eps={self.eps})")
        self.xT = xT
        return self

    def rate(self, actions: pl.DataFrame) -> pl.Series:
        """Rate each action: end-cell minus start-cell value for successful passes, dribbles and crosses.

        Args:
            actions: SPADL actions with ``type_name``, ``result_name`` and start/end coordinates.

        Returns:
            A ``Float64`` series named ``xt_value``; null for actions xT does not value.

        Raises:
            NotFittedError: If the grid is all zeros.
        """
        if not np.any(self.xT):
            raise NotFittedError("the xT grid is all zeros; fit() or load_xthreat_model() first")
        values = np.full(actions.height, np.nan)
        ok = (
            (actions["type_name"].is_in(list(MOVE_TYPES)) & (actions["result_name"] == "success"))
            .fill_null(False)
            .to_numpy()
        )
        if ok.any():
            sx, sy = _xy(actions.filter(pl.Series(ok)), "start")
            ex, ey = _xy(actions.filter(pl.Series(ok)), "end")
            good = ~(np.isnan(sx) | np.isnan(sy) | np.isnan(ex) | np.isnan(ey))
            sxi, syj = _cell_indexes(sx[good], sy[good], self.l, self.w)
            exi, eyj = _cell_indexes(ex[good], ey[good], self.l, self.w)
            out = np.full(ok.sum(), np.nan)
            out[good] = self.xT[self.w - 1 - eyj, exi] - self.xT[self.w - 1 - syj, sxi]
            values[ok] = out
        return pl.Series("xt_value", values, dtype=pl.Float64).fill_nan(None)

    def to_json(self, path: Union[str, Path]) -> None:
        """Write ``{"xT": grid, "w": .., "l": .., "meta": {..}}`` (readable by ``from_json``)."""
        Path(path).write_text(
            json.dumps({"xT": self.xT.tolist(), "w": self.w, "l": self.l, "meta": self.meta}, indent=2),
            encoding="utf-8",
        )

    @classmethod
    def from_json(cls, path: Union[str, Path]) -> XThreat:
        """Read this module's format or socceraction's bare nested list."""
        data: Any = json.loads(Path(path).read_text(encoding="utf-8"))
        if isinstance(data, list):
            grid: np.ndarray = np.asarray(data, dtype=np.float64)
            return cls(grid, l=grid.shape[1], w=grid.shape[0])
        grid = np.asarray(data["xT"], dtype=np.float64)
        return cls(
            grid, l=int(data.get("l", grid.shape[1])), w=int(data.get("w", grid.shape[0])), meta=data.get("meta")
        )


def load_xthreat_model() -> XThreat:
    """The bundled grid fit on StatsBomb open data (see ``meta`` for competitions, counts and license).

    Returns:
        The fitted :class:`XThreat` shipped with the package.

    Example:
        Quick start::

            from sportsdataverse.soccer import load_xthreat_model
            model = load_xthreat_model()
            print(model.xT.shape, model.meta["matches"])

    See Also:
        * `socceraction`_ -- the original xT implementation (MIT)

    .. _socceraction: https://github.com/ML-KULeuven/socceraction
    """
    from importlib.resources import files

    text = (files("sportsdataverse.soccer") / "models" / "xthreat_statsbomb_open.json").read_text(encoding="utf-8")
    data = json.loads(text)
    return XThreat(np.asarray(data["xT"], dtype=np.float64), l=int(data["l"]), w=int(data["w"]), meta=data.get("meta"))


def soccer_xthreat_rate(
    actions: pl.DataFrame, model: Optional[XThreat] = None, *, return_as_pandas: bool = False
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Append ``xt_value`` (Expected Threat added by each successful pass, dribble or cross) to a SPADL frame.

    Args:
        actions: SPADL actions from :func:`soccer_spadl` (needs ``type_name``, ``result_name``, start/end coordinates).
        model: A fitted :class:`XThreat`; ``None`` uses the bundled grid.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        ``actions`` with a ``Float64`` ``xt_value`` column (null for actions xT does not value).

    Raises:
        NotFittedError: If ``model`` is an unfitted grid.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.soccer import soccer_open_dataset, soccer_spadl, soccer_xthreat_rate
            actions = soccer_xthreat_rate(soccer_spadl(soccer_open_dataset("statsbomb", 8658)))
            print(actions.group_by("player_id").agg(pl.col("xt_value").sum()).sort("xt_value", descending=True).head())

    See Also:
        * `socceraction`_ -- the original xT implementation (MIT)

    .. _socceraction: https://github.com/ML-KULeuven/socceraction
    """
    out = actions.with_columns((model if model is not None else load_xthreat_model()).rate(actions))
    return out.to_pandas() if return_as_pandas else out
