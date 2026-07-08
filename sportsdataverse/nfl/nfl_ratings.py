"""Native opponent-adjusted EPA power ratings for the NFL (model 1 of T4.2).

Fits an opponent-adjusted ridge on the **already-computed** ``epa`` column
from :func:`sportsdataverse.nfl.nfl_loaders.load_nfl_pbp` (owned by
``ep_wp.py`` -- this module never re-scores plays), producing per-team
offense / defense / special-teams components and ``adj_net``.

The solver, :func:`opponent_adjusted_ridge`, is a self-contained pure
function parameterized on column names so it is league-agnostic -- the
designated T7.2 extraction target for a shared ``_common_ratings`` module
backing both CFB and NFL.

Non-market discipline (binding): nothing in this module reads
``spread_line`` / ``total_line`` / ``vegas_wp``; the competitive-play filter
uses the naive ``wp``.
"""

from __future__ import annotations

import numpy as np
import polars as pl

_RIDGE_OUTPUT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "team_id": pl.Utf8,
    "off_coef": pl.Float64,
    "def_coef": pl.Float64,
}


def opponent_adjusted_ridge(
    plays: pl.DataFrame,
    *,
    off_col: str,
    def_col: str,
    home_col: str,
    resp_col: str,
    lam: float,
    penalize_home: bool = False,
) -> tuple[pl.DataFrame, float, float]:
    """Ridge-regress ``resp_col`` on offense + defense team indicators + HFA.

    League-agnostic (column names are arguments) so this is the single solver
    a T7.2 refactor can lift into ``_common_ratings`` to back both CFB and
    NFL. Builds the offense/defense-indicator + intercept + home design and
    solves the ridge normal equations ``beta = (X'X + lam*R)^-1 X'y``. Only
    team coefficients are penalised; the intercept (and, unless
    ``penalize_home``, the home term) is free.

    Args:
        plays: One row per play. Rows with a null ``off_col`` / ``def_col`` /
            ``resp_col`` must be filtered by the caller.
        off_col: Column naming the offense (possession) team.
        def_col: Column naming the defense team.
        home_col: Column naming the home team (HFA indicator is
            ``off_col == home_col``).
        resp_col: Numeric response column (e.g. ``epa``).
        lam: Ridge penalty applied to the team coefficients.
        penalize_home: Also penalise the home-field coefficient
            (default False).

    Returns:
        A ``(frame, intercept, home_coef)`` tuple: ``frame`` has one row per
        team (``team_id`` Utf8, ``off_coef`` / ``def_coef`` Float64);
        ``intercept`` is the league baseline; ``home_coef`` the fitted HFA in
        response units. Zero-row frame + ``(0.0, 0.0)`` on empty input.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_ratings import opponent_adjusted_ridge
            frame, intercept, hfa = opponent_adjusted_ridge(
                plays, off_col="posteam", def_col="defteam",
                home_col="home_team", resp_col="epa", lam=200.0,
            )
            frame.sort("off_coef", descending=True).head()
    """
    if plays.height == 0:
        return pl.DataFrame(schema=_RIDGE_OUTPUT_SCHEMA), 0.0, 0.0
    off = plays[off_col].cast(pl.Utf8)
    dff = plays[def_col].cast(pl.Utf8)
    teams = sorted(set(off.to_list()) | set(dff.to_list()))
    idx = {t: i for i, t in enumerate(teams)}
    n_t = len(teams)
    n = plays.height
    # columns: [off_0..off_{T-1}, def_0..def_{T-1}, intercept, home]
    p = 2 * n_t + 2
    X = np.zeros((n, p), dtype=float)
    oi = np.array([idx[t] for t in off.to_list()])
    di = np.array([idx[t] for t in dff.to_list()])
    X[np.arange(n), oi] = 1.0
    X[np.arange(n), n_t + di] = 1.0
    X[:, 2 * n_t] = 1.0  # intercept
    is_home = (off == plays[home_col].cast(pl.Utf8)).to_numpy().astype(float)
    X[:, 2 * n_t + 1] = is_home  # HFA (offense is home)
    y = plays[resp_col].cast(pl.Float64).to_numpy()
    R = np.eye(p)
    R[2 * n_t, 2 * n_t] = 0.0  # don't penalise intercept
    if not penalize_home:
        R[2 * n_t + 1, 2 * n_t + 1] = 0.0  # don't penalise HFA
    beta = np.linalg.solve(X.T @ X + lam * R, X.T @ y)
    frame = pl.DataFrame(
        {
            "team_id": teams,
            "off_coef": beta[:n_t].astype(np.float64),
            "def_coef": beta[n_t : 2 * n_t].astype(np.float64),
        }
    )
    return frame, float(beta[2 * n_t]), float(beta[2 * n_t + 1])
