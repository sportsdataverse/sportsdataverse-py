"""Shared opponent-adjusted power-rating engines (T7.2).

Three league-agnostic solvers, moved **verbatim** (byte-for-byte, same op
order) out of their originating per-sport modules so ``sportsdataverse``
hosts one copy instead of N divergent ones:

* :func:`opponent_adjusted_ridge` -- NFL's dense-design-matrix ridge
  (``sportsdataverse.nfl.nfl_ratings``): full offense/defense dummy encoding
  + intercept + home indicator, solved via the normal equations.
* :func:`dropped_level_ridge` -- CFB's ``model.matrix``-style ridge
  (``sportsdataverse.cfb.cfb_adjusted_epa``): drops one reference level per
  side and fits a standardized :class:`sklearn.linear_model.Ridge`. This is
  a **genuinely different algorithm** from :func:`opponent_adjusted_ridge`
  (different design encoding, different library, different regularization
  convention) despite both solving "opponent-adjusted ridge" -- it is kept
  as its own function rather than forced into one solver, per the T7.2
  no-force-unification discipline.
* :func:`iterative_opponent_adjust` -- the MBB/NBA (and, via those modules,
  WBB/WNBA) KenPom-style fixed point: alternately re-estimates each team's
  offense/defense from its opponents' *current* adjusted ratings and a
  home-court term until convergence. Byte-identical across MBB and NBA
  today; the only difference is whether the baseline (``avg``) is the
  data's own mean (MBB) or a fitted external constant (NBA), which this
  function takes as an optional parameter so both behaviors are preserved
  exactly.

Every per-sport caller retains its own public signature, its own league
constants, and its own output column names -- only the pure numeric core
moved. See each per-sport module for the byte-for-byte migration.
"""

from __future__ import annotations

import warnings

import numpy as np
import polars as pl

__all__ = ["dropped_level_ridge", "iterative_opponent_adjust", "opponent_adjusted_ridge"]


_RIDGE_OUTPUT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "team_id": pl.Utf8,
    "off_coef": pl.Float64,
    "def_coef": pl.Float64,
}

_ITER_SCHEMA: dict[str, pl.PolarsDataType] = {
    "team_id": pl.Utf8,
    "adj_off": pl.Float64,
    "adj_def": pl.Float64,
    "adj_net": pl.Float64,
    "raw_off": pl.Float64,
    "raw_def": pl.Float64,
    "games": pl.Int64,
}


def drop_unusable_possession_rows(paired: pl.DataFrame) -> pl.DataFrame:
    """Drop game rows whose possession estimate is not usable (``poss <= 0`` / null).

    ESPN ships the occasional boxscore shell -- a final score with every
    shooting counter zeroed (MBB game ``310573129``, 2011-02-26; WBB game
    ``400768032``, 2015-03-10) -- which makes ``poss`` exactly ``0`` and the
    efficiency ``100 * pts / 0`` an infinity. One such row is enough to take a
    whole season down: :func:`~sportsdataverse._common.ratings.iterative_opponent_adjust`
    centres on the data's own mean, and ``mean([..., inf])`` is ``inf``, so
    every team's fixed point (not just the two teams in that game) converges to
    a non-finite rating. ``raw_o``/``raw_d`` are per-team sums that never touch
    the mean, which is why they stay finite and the failure looks selective.

    The row carries no information -- there is no possession estimate to be had
    from an all-zero box -- so it is dropped from both the efficiency and the
    tempo path, with a warning naming the games.
    """
    if paired.height == 0:
        return paired
    usable = paired.filter(pl.col("poss").is_not_null() & pl.col("poss").is_finite() & (pl.col("poss") > 0.0))
    dropped = paired.height - usable.height
    if dropped:
        games = paired.join(usable.select("game_id", "team_id"), on=["game_id", "team_id"], how="anti")[
            "game_id"
        ].unique(maintain_order=True)
        warnings.warn(
            f"raw_game_efficiency: dropped {dropped} team-game row(s) with a non-positive possession "
            f"estimate (empty boxscore) from {games.len()} game(s): {games.head(10).to_list()}",
            UserWarning,
            stacklevel=3,
        )
    return usable


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

    League-agnostic (column names are arguments): builds the full
    offense/defense-indicator + intercept + home design and solves the
    ridge normal equations ``beta = (X'X + lam*R)^-1 X'y``. Only team
    coefficients are penalised; the intercept (and, unless
    ``penalize_home``, the home term) is free. Moved verbatim (T7.2) from
    ``sportsdataverse.nfl.nfl_ratings`` -- NFL is currently the sole
    adopter of this exact dense-design encoding (CFB's ridge is the
    genuinely different :func:`dropped_level_ridge`).

    Args:
        plays: One row per play. Rows with a null ``off_col`` / ``def_col``
            / ``resp_col`` must be filtered by the caller.
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
        ``intercept`` is the league baseline; ``home_coef`` the fitted HFA
        in response units. Zero-row frame + ``(0.0, 0.0)`` on empty input.

    Example:
        Quick start (via the NFL public re-export -- its sole adopter today)::

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


def dropped_level_ridge(clean: pl.DataFrame, ridge_lambda: float) -> tuple[pl.DataFrame, pl.DataFrame, float]:
    """Fit the offense/defense ridge on competitive plays -> (offense, defense, intercept).

    ``model.matrix``-style encoding: drops the first factor level per side
    (offense, defense) and fits a standardized :class:`sklearn.linear_model.Ridge`,
    then un-standardizes the coefficients. ``offense``/``defense`` carry one
    row per team **including the reference level**, whose effect is 0 by
    construction so its strength is the intercept; ``intercept`` is the league baseline used
    as the fallback strength for not-yet-seen teams in CFB's walk-forward
    variant. Moved verbatim (T7.2) from
    ``sportsdataverse.cfb.cfb_adjusted_epa._fit_opponent_ridge`` -- a
    genuinely different algorithm from :func:`opponent_adjusted_ridge`
    (dropped reference level + standardized sklearn fit vs. full-dummy
    manual normal equations), kept as its own function rather than forced
    into a false unification.

    Args:
        clean: Competitive-play frame with ``pos_team_id``, ``def_pos_team_id``,
            ``hfa``, ``EPA`` columns (see ``cfb_adjusted_epa._prepare``).
        ridge_lambda: Ridge penalty, PER OBSERVATION -- the sklearn penalty is
            formed as ``alpha = ridge_lambda * n_plays``. Not a glmnet lambda;
            see ``cfb_adjusted_epa._RIDGE_LAMBDA`` for why that mapping matters.

    Returns:
        ``(offense, defense, intercept)``: ``offense`` has ``team_id`` +
        ``adjmodelOff``; ``defense`` has ``team_id`` + ``adjmodelDef``;
        ``intercept`` is the fitted league baseline (float).

    Raises:
        ImportError: If ``scikit-learn`` is not installed.

    Example:
        Quick start::

            from sportsdataverse._common.ratings import dropped_level_ridge
            offense, defense, intercept = dropped_level_ridge(clean, 0.02)
    """
    try:
        from sklearn.linear_model import Ridge
    except ImportError as exc:  # pragma: no cover - optional dep guidance
        raise ImportError(
            "dropped_level_ridge requires scikit-learn. Install it with "
            "`pip install sportsdataverse[models]` (or `pip install scikit-learn`)."
        ) from exc

    off_ids = sorted(clean["pos_team_id"].drop_nulls().unique().to_list())
    def_ids = sorted(clean["def_pos_team_id"].drop_nulls().unique().to_list())
    off_dummy, def_dummy = off_ids[1:], def_ids[1:]

    pos = clean["pos_team_id"].to_numpy()
    dfn = clean["def_pos_team_id"].to_numpy()
    feats = [clean["hfa"].cast(pl.Float64).to_numpy().reshape(-1, 1)]
    feats += [(pos == t).astype(float).reshape(-1, 1) for t in off_dummy]
    feats += [(dfn == t).astype(float).reshape(-1, 1) for t in def_dummy]
    x_mat = np.hstack(feats)
    y = clean["EPA"].cast(pl.Float64).to_numpy()

    # glmnet (1/2n)RSS + lambda/2||b||^2 with internal standardization vs sklearn
    # RSS + alpha||b||^2: standardize X and scale alpha by n. Coefficients won't
    # byte-match glmnet, but the relative team strengths correlate closely.
    mu, sd = x_mat.mean(axis=0), x_mat.std(axis=0)
    sd[sd == 0] = 1.0
    model = Ridge(alpha=ridge_lambda * len(y), fit_intercept=True)
    model.fit((x_mat - mu) / sd, y)
    coef_std = model.coef_ / sd
    intercept = float(model.intercept_ - (coef_std * mu).sum())
    names = ["hfa"] + [f"pos_team_id{t}" for t in off_dummy] + [f"def_pos_team_id{t}" for t in def_dummy]
    coef = dict(zip(names, coef_std))
    # The reference level belongs in the OUTPUT even though it has no column in
    # the design matrix: under model.matrix encoding its effect is 0 and lives in
    # the intercept, so its strength is `intercept + 0`. Emitting only the dummy
    # columns silently dropped one team per side from every fit -- and because the
    # season path joins opponent strength with fill_strength=None, that team's
    # opponents got a null adjustment and were filtered out downstream too. Which
    # team it hit was arbitrary: `sorted()` runs on the STRING id, so it is
    # whichever id sorts first lexicographically ("100" < "1005" < "101").
    offense = pl.DataFrame(
        {
            "team_id": off_ids,
            "adjmodelOff": [intercept + (coef[f"pos_team_id{t}"] if t in set(off_dummy) else 0.0) for t in off_ids],
        }
    )
    defense = pl.DataFrame(
        {
            "team_id": def_ids,
            "adjmodelDef": [intercept + (coef[f"def_pos_team_id{t}"] if t in set(def_dummy) else 0.0) for t in def_ids],
        }
    )
    return offense, defense, intercept


def iterative_opponent_adjust(
    game_eff: pl.DataFrame,
    *,
    team_col: str,
    opp_col: str,
    off_col: str,
    def_col: str,
    home_col: str,
    neutral_col: str,
    hfa: float,
    baseline: float | None = None,
    max_iter: int = 100,
    tol: float = 1e-4,
) -> pl.DataFrame:
    """KenPom-style opponent-adjustment fixed point for one group of game rows.

    Initialises ``adj_off = raw_off`` / ``adj_def = raw_def``, then
    repeatedly recomputes each team's rating from its games with the
    opponent's *current* adjusted rating and a home-court adjustment
    removed, until the largest change is below ``tol``. Moved verbatim
    (T7.2) from the identical inner ``_adjust_one_season`` loops in
    ``sportsdataverse.mbb.mbb_team_ratings`` and
    ``sportsdataverse.nba.nba_team_ratings`` (and, via those modules,
    WBB/WNBA) -- the two were byte-identical except for column names and
    where the baseline average comes from, which is why ``baseline`` is
    optional here: pass ``None`` to compute it as the data's own mean
    (MBB's behavior) or an explicit float to use a fitted external
    constant (NBA's behavior). Callers group by season (or whatever the
    per-league unit is) before calling this once per group.

    Args:
        game_eff: One row per (game, team) with ``team_col``, ``opp_col``,
            ``off_col``, ``def_col``, ``home_col`` (boolean), ``neutral_col``
            (boolean). Already restricted to one rating group (e.g. season).
        team_col: Column naming the team.
        opp_col: Column naming the opponent.
        off_col: Column with the team's own-side per-game rate/efficiency.
        def_col: Column with the team's against-side per-game rate/efficiency.
        home_col: Boolean column, True when ``team_col`` is the home side.
        neutral_col: Boolean column, True for a neutral-site game.
        hfa: Home-court/field edge, split ``+-hfa/2``.
        baseline: League-average baseline to adjust and converge toward. If
            ``None`` (default), computed as the data's own ``off_col`` mean.
        max_iter: Maximum fixed-point iterations.
        tol: Convergence tolerance on the largest rating change.

    Returns:
        One row per team: ``team_id, adj_off, adj_def, adj_net, raw_off,
        raw_def, games``. Empty input returns that schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse._common.ratings import iterative_opponent_adjust
            out = iterative_opponent_adjust(
                game_eff, team_col="team_id", opp_col="opp_team_id",
                off_col="off_eff", def_col="def_eff", home_col="is_home",
                neutral_col="neutral_site", hfa=3.0,
            )
    """
    if game_eff.height == 0:
        return pl.DataFrame(schema=_ITER_SCHEMA)
    teams = game_eff[team_col].unique(maintain_order=True).to_list()
    index = {t: i for i, t in enumerate(teams)}
    n = len(teams)
    ti = np.array([index[t] for t in game_eff[team_col].to_list()], dtype=np.int64)
    oi = np.array([index[t] for t in game_eff[opp_col].to_list()], dtype=np.int64)
    off = game_eff[off_col].to_numpy().astype(float)
    dfn = game_eff[def_col].to_numpy().astype(float)
    is_home = game_eff[home_col].to_numpy()
    neutral = game_eff[neutral_col].to_numpy()

    half = hfa / 2.0
    loc_o = np.where(neutral, 0.0, np.where(is_home, half, -half))
    loc_d = np.where(neutral, 0.0, np.where(is_home, -half, half))
    avg = float(off.mean()) if baseline is None else float(baseline)

    counts: np.ndarray = np.bincount(ti, minlength=n).astype(float)
    raw_o = np.bincount(ti, weights=off, minlength=n) / counts
    raw_d = np.bincount(ti, weights=dfn, minlength=n) / counts

    adj_o, adj_d = raw_o.copy(), raw_d.copy()
    for _ in range(max_iter):
        contrib_o = off - (adj_d[oi] - avg) - loc_o
        contrib_d = dfn - (adj_o[oi] - avg) - loc_d
        new_o = np.bincount(ti, weights=contrib_o, minlength=n) / counts
        new_d = np.bincount(ti, weights=contrib_d, minlength=n) / counts
        delta = max(float(np.abs(new_o - adj_o).max()), float(np.abs(new_d - adj_d).max()))
        adj_o, adj_d = new_o, new_d
        if delta < tol:
            break

    return pl.DataFrame(
        {
            "team_id": teams,
            "adj_off": adj_o,
            "adj_def": adj_d,
            "adj_net": adj_o - adj_d,
            "raw_off": raw_o,
            "raw_def": raw_d,
            "games": counts.astype(np.int64),
        },
        schema=_ITER_SCHEMA,
    )
