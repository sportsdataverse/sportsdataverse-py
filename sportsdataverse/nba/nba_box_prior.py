"""Box prior v2: a cross-fitted box-score prior trained on an age-adjusted, time-weighted career RAPM.

Three pieces, chained by ``tools/fit_box_prior_v2.py`` and consumed by
:func:`~sportsdataverse.nba.nba_reml_rapm.nba_reml_rapm`:

1. :func:`career_rapm_target` -- the long-horizon training target: one RAPM over many seasons with a
   single career coefficient per player (offense and defense), a league aging curve, and a points-per-
   possession level per season, the possessions of each season weighted by how close it is to the
   season the prior is for. A player-season's target is his career coefficient plus the aging curve at
   his age that season.
2. :func:`box_prior_features` -- one fixed feature set for every season: league-relative per-100 box
   rates padded toward league average at low possessions, plus age, height, listed position, and the
   on-court teammate / opponent context.
3. :func:`train_box_prior` / :func:`apply_box_prior` -- a possession-weighted ridge of the target on the
   features, cross-fitted in :data:`BOX_PRIOR_FOLDS` player folds so a player's prior never trained on
   him; :func:`box_prior_training_seasons` keeps it walk-forward by season.
"""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Iterable, Mapping, Sequence

import numpy as np
import pandas as pd
import polars as pl
from scipy.linalg import cho_factor, cho_solve

from sportsdataverse.nba.nba_box_logs import box_features
from sportsdataverse.nba.nba_rapm import RAPM_PENALTY, build_rapm_design

#: Possessions of padding toward league average: a per-100 rate enters as
#: ``(x - league) * poss / (poss + BOX_PRIOR_PAD_POSS)``, so a player with 1,000 on-court possessions keeps
#: half his deviation and a starter's 4,000 keep 80%. Chosen a priori (a quarter season of a rotation
#: player), not tuned on the gate.
BOX_PRIOR_PAD_POSS: float = 1000.0
#: Half-life in seasons of the time weight ``0.5 ** (|season - t| / H)``, both on a season's possessions
#: in the career target and on its rows in the prior regression. Chosen a priori, not tuned on the gate.
BOX_PRIOR_HALF_LIFE: float = 4.0
#: Player folds of the cross-fit (``player_id % BOX_PRIOR_FOLDS``).
BOX_PRIOR_FOLDS: int = 5
#: Seasons (end year) up to which the prior learns from every OTHER season (leave-one-season-out)
#: instead of the earlier ones: 1997 has no earlier season and 1998-2005 too few.
BOX_PRIOR_LEAVE_SEASON_OUT_THROUGH: int = 2005
#: Ridge penalty of the prior regression on standardized features. Against possession weights summing
#: to millions it is a numerical guard, not a shrinkage choice.
BOX_PRIOR_RIDGE_ALPHA: float = 1.0
#: Age the aging curve is centred at.
BOX_PRIOR_AGE_CENTER: float = 27.0

_RATES: list[str] = ["pts", "fg3m", "fga", "fta", "ast", "oreb", "dreb", "stl", "blk", "tov", "pf"]
_CONTEXT_STATS: list[str] = ["pts", "ast", "reb"]
#: The prior's feature columns, in order.
BOX_PRIOR_FEATURES: list[str] = (
    [f"{s}_rel" for s in _RATES]
    + ["mpg_rel", "age_c", "age_c2", "height_rel", "position_c"]
    + [f"tm_{s}" for s in _CONTEXT_STATS]
    + [f"opp_{s}" for s in _CONTEXT_STATS]
)
_OFF = [f"off_player_{i}" for i in range(1, 6)]
_DEF = [f"def_player_{i}" for i in range(1, 6)]


def player_fold(player_id: int | pl.Expr) -> int | pl.Expr:
    """The cross-fit fold of a player id: ``player_id % BOX_PRIOR_FOLDS`` (an int or a polars expression)."""
    return player_id % BOX_PRIOR_FOLDS


def box_prior_training_seasons(season: int, available: Iterable[int]) -> list[int]:
    """The seasons a prior for ``season`` may learn from: earlier ones, or every other one through 2005.

    Args:
        season: The season (end year) the prior is for.
        available: Seasons with a complete career-target fit (complete regular seasons).

    Returns:
        Sorted season list, never containing ``season``.
    """
    avail = sorted({int(s) for s in available})
    if season <= BOX_PRIOR_LEAVE_SEASON_OUT_THROUGH:
        return [s for s in avail if s != season]
    return [s for s in avail if s < season]


def _time_weight(season: int | np.ndarray, center: int, half_life: float) -> float | np.ndarray:
    return 0.5 ** (np.abs(np.asarray(season, dtype=np.float64) - center) / half_life)


@dataclass(frozen=True)
class SeasonGram:
    """One season's RAPM sufficient statistics (the design of ``build_rapm_design``).

    Attributes:
        pids: The season's player ids, in design order.
        gram: ``X'X`` dense ``(2P, 2P)``.
        xty: ``X'y`` ``(2P,)``.
        counts: ``X'1`` ``(2P,)``: offensive then defensive possessions per player.
        n: Possessions.
        ysum: Total points.
    """

    pids: np.ndarray
    gram: np.ndarray
    xty: np.ndarray
    counts: np.ndarray
    n: int
    ysum: float


def season_gram(possessions: pl.DataFrame) -> SeasonGram:
    """The :class:`SeasonGram` of a possession frame (rows with a full lineup).

    Args:
        possessions: Possession+lineup rows (``points``, ``off_player_1..5``, ``def_player_1..5``).

    Returns:
        The season's :class:`SeasonGram`.
    """
    X, y, pids = build_rapm_design(possessions)
    return SeasonGram(
        pids=np.asarray(pids, dtype=np.int64),
        gram=(X.T @ X).toarray(),
        xty=np.asarray(X.T @ y, dtype=np.float64).ravel(),
        counts=np.asarray(X.sum(axis=0), dtype=np.float64).ravel(),
        n=int(X.shape[0]),
        ysum=float(y.sum()),
    )


def _age_lookup(ages: pl.DataFrame, season: int, pids: np.ndarray) -> np.ndarray:
    """Centred, /10 ages of ``pids`` in ``season`` (a missing age is the centre: no aging term)."""
    a = ages.filter(pl.col("season") == season)
    look = dict(zip(a["player_id"].cast(pl.Int64).to_list(), a["age"].to_list(), strict=True))
    vals = [look.get(int(p)) for p in pids]
    return np.array([0.0 if v is None else (float(v) - BOX_PRIOR_AGE_CENTER) / 10.0 for v in vals])


def career_rapm_target(
    grams: Mapping[int, SeasonGram],
    ages: pl.DataFrame,
    *,
    seasons: Sequence[int],
    center: int,
    half_life: float = BOX_PRIOR_HALF_LIFE,
    penalty: float | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Age-adjusted, time-weighted career RAPM over ``seasons``: the box prior's training target.

    One ridge fit over every possession of ``seasons``. Each player has one offensive and one defensive
    career coefficient (penalty ``penalty``, the plain-RAPM per-possession penalty by default); the
    on-court players' ages enter through a quadratic aging curve per side, and each season has its own
    points-per-possession level (both unpenalized). A season's possessions are weighted
    ``0.5 ** (|t - center| / half_life)``, so the career rating leans toward the seasons near
    ``center``. The fit is assembled from the per-season :class:`SeasonGram` blocks (no possession is
    re-read) and solved by one Cholesky factorization.

    Args:
        grams: ``{season: SeasonGram}``; must hold every season of ``seasons``.
        ages: ``player_id`` (Int64), ``season`` (Int64), ``age`` (Float64) rows. A missing age is
            taken as :data:`BOX_PRIOR_AGE_CENTER`.
        seasons: The seasons to fit over.
        center: The season the time weight is centred on (the season the prior is for).
        half_life: Half-life of the time weight, in seasons.
        penalty: Ridge penalty on the career coefficients; default the NBA plain-RAPM penalty.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        One row per player-season of ``seasons``: ``player_id`` (Int64), ``season`` (Int64),
        ``o_target`` / ``d_target`` (Float64, per 100, ``d`` positive = good defense), ``off_poss`` /
        ``def_poss`` (Float64, that season's possessions).

    Raises:
        ValueError: If ``seasons`` is empty or a season has no gram.

    Example:
        The target a 2015 prior trains on::

            from sportsdataverse.nba.nba_box_prior import box_prior_training_seasons, career_rapm_target
            train = box_prior_training_seasons(2015, grams)
            target = career_rapm_target(grams, ages, seasons=train, center=2015)
    """
    seasons = sorted(int(s) for s in seasons)
    if not seasons or any(s not in grams for s in seasons):
        raise ValueError(f"need a gram for every season, got seasons {seasons} and grams {sorted(grams)}")
    lam = RAPM_PENALTY["00"] if penalty is None else float(penalty)
    all_p = np.unique(np.concatenate([grams[s].pids for s in seasons]))
    N, T = all_p.size, len(seasons)
    q = 2 * N + 4 + T
    A = np.zeros((q, q))
    b = np.zeros(q)
    age_cols = np.arange(2 * N, 2 * N + 4)
    basis: dict[int, np.ndarray] = {}
    for ti, t in enumerate(seasons):
        g = grams[t]
        w = float(_time_weight(t, center, half_life))
        P = g.pids.size
        loc = np.searchsorted(all_p, g.pids)
        idx = np.concatenate([loc, N + loc])
        a = _age_lookup(ages, t, g.pids)
        B = np.zeros((2 * P, 4))
        B[:P, 0], B[:P, 1], B[P:, 2], B[P:, 3] = a, a**2, a, a**2
        basis[t] = B
        sc = 2 * N + 4 + ti
        GB = g.gram @ B
        A[np.ix_(idx, idx)] += w * g.gram
        A[np.ix_(idx, age_cols)] += w * GB
        A[np.ix_(age_cols, idx)] += w * GB.T
        A[np.ix_(age_cols, age_cols)] += w * (B.T @ GB)
        A[idx, sc] += w * g.counts
        A[sc, idx] += w * g.counts
        A[age_cols, sc] += w * (B.T @ g.counts)
        A[sc, age_cols] += w * (B.T @ g.counts)
        A[sc, sc] += w * g.n
        b[idx] += w * g.xty
        b[age_cols] += w * (B.T @ g.xty)
        b[sc] += w * g.ysum
    diag = np.full(q, 1e-6)
    diag[: 2 * N] = lam
    A[np.diag_indices_from(A)] += diag
    coef = cho_solve(cho_factor(A), b)
    out = pl.concat([_target_rows(grams[t], t, all_p, coef, basis[t] @ coef[age_cols]) for t in seasons])
    return out.to_pandas() if return_as_pandas else out


def _target_rows(g: SeasonGram, t: int, all_p: np.ndarray, coef: np.ndarray, aging: np.ndarray) -> pl.DataFrame:
    """Season ``t``'s target rows: career coefficient plus the aging term (``aging``, per local column)."""
    P, N = g.pids.size, all_p.size
    loc = np.searchsorted(all_p, g.pids)
    return pl.DataFrame(
        {
            "player_id": pl.Series(g.pids, dtype=pl.Int64),
            "season": pl.Series(np.full(P, t), dtype=pl.Int64),
            "o_target": 100.0 * (coef[loc] + aging[:P]),
            "d_target": -100.0 * (coef[N + loc] + aging[P:]),
            "off_poss": g.counts[:P],
            "def_poss": g.counts[P:],
        }
    )


def _on_court_poss(possessions: pl.DataFrame) -> pl.DataFrame:
    full = possessions.drop_nulls(_OFF + _DEF)
    off = full.select(pl.concat_list(_OFF).alias("player_id")).explode("player_id").group_by("player_id").len("poss")
    de = full.select(pl.concat_list(_DEF).alias("player_id")).explode("player_id").group_by("player_id").len("def_poss")
    return off.join(de, on="player_id", how="full", coalesce=True).with_columns(
        pl.col("player_id").cast(pl.Int64), pl.col("poss").fill_null(0).cast(pl.Float64),
        pl.col("def_poss").fill_null(0).cast(pl.Float64),
    )  # fmt: skip


def _context(possessions: pl.DataFrame, padded: pl.DataFrame) -> pl.DataFrame:
    """Mean over a player's offensive possessions of his 4 teammates' and 5 opponents' padded stats."""
    full = possessions.drop_nulls(_OFF + _DEF).with_row_index("_r")
    stats = padded.select("player_id", *[pl.col(f"{s}_pad").alias(s) for s in _CONTEXT_STATS])

    def side(cols: list[str]) -> pl.DataFrame:
        long = full.select("_r", pl.concat_list(cols).alias("player_id")).explode("player_id")
        long = long.with_columns(pl.col("player_id").cast(pl.Int64)).join(stats, on="player_id", how="left")
        return long.with_columns(pl.col(_CONTEXT_STATS).fill_null(0.0))

    off, de = side(_OFF), side(_DEF)
    off_sum = off.group_by("_r").agg(*[pl.col(s).sum().alias(f"{s}_osum") for s in _CONTEXT_STATS])
    de_mean = de.group_by("_r").agg(*[pl.col(s).mean().alias(f"{s}_dmean") for s in _CONTEXT_STATS])
    j = off.join(off_sum, on="_r").join(de_mean, on="_r")
    return j.group_by("player_id").agg(
        *[((pl.col(f"{s}_osum") - pl.col(s)) / 4.0).mean().alias(f"tm_{s}") for s in _CONTEXT_STATS],
        *[pl.col(f"{s}_dmean").mean().alias(f"opp_{s}") for s in _CONTEXT_STATS],
    )


def box_prior_features(
    player_logs: pl.DataFrame,
    team_logs: pl.DataFrame,
    possessions: pl.DataFrame,
    bio: pl.DataFrame,
    *,
    game_ids: Sequence[str] | None = None,
    pad_poss: float = BOX_PRIOR_PAD_POSS,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """One season's box-prior features: league-relative, padded box rates plus player and lineup context.

    Per-100 rates come from :func:`~sportsdataverse.nba.nba_box_logs.box_features`. Each rate (and
    minutes per game) is taken relative to the season's on-court-possession-weighted league mean and
    padded toward it: ``(x - league) * poss / (poss + pad_poss)``, ``poss`` the player's offensive
    on-court possessions. Context: age (centred at 27, and squared), height relative to the league mean,
    listed position (1-5, centred at 3), and the mean over his offensive possessions of his four
    teammates' and five opponents' padded points, assists and rebounds per 100.

    Args:
        player_logs: Per-player-game box lines (``leaguegamelog`` ``P``).
        team_logs: Per-team-game lines (``leaguegamelog`` ``T``).
        possessions: The season's possession+lineup rows.
        bio: ``player_id`` (Int64), ``age``, ``height_in``, ``position_num`` (Float64; nulls allowed).
        game_ids: Restrict logs and possessions to these games (a fold's training games: the leakage
            guard of the held-out and chronological tests).
        pad_poss: Padding possessions.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        One row per player on court in ``possessions``: ``player_id`` (Int64), ``poss`` / ``def_poss``
        (Float64, on-court possessions), and :data:`BOX_PRIOR_FEATURES` (Float64, no nulls).

    Example:
        A season's features::

            from sportsdataverse.nba.nba_box_prior import box_prior_features
            feats = box_prior_features(logs["player"], logs["team"], possessions, bio)
    """
    if game_ids is not None:
        gids = list(game_ids)
        possessions = possessions.filter(pl.col("game_id").is_in(gids))
    box = box_features(player_logs, team_logs, game_ids=None if game_ids is None else gids)
    poss = _on_court_poss(possessions)
    f = poss.join(box, on="player_id", how="left").join(
        bio.select("player_id", "age", "height_in", "position_num"), on="player_id", how="left"
    )
    f = f.with_columns(
        *[pl.col(s).fill_null(0.0).cast(pl.Float64) for s in _RATES],
        pl.when(pl.col("gp") > 0).then(pl.col("min") / pl.col("gp")).otherwise(0.0).fill_null(0.0).alias("mpg"),
        (pl.col("oreb") + pl.col("dreb")).fill_null(0.0).alias("reb"),
    )
    w = pl.col("poss")
    shrink = w / (w + pad_poss)

    def rel(c: str) -> pl.Expr:
        return pl.col(c) - (pl.col(c) * w).sum() / w.sum()

    f = f.with_columns(
        *[(rel(s) * shrink).alias(f"{s}_rel") for s in _RATES],
        (rel("mpg") * shrink).alias("mpg_rel"),
        (rel("reb") * shrink).alias("reb_pad"),
        ((pl.col("age").fill_null(BOX_PRIOR_AGE_CENTER) - BOX_PRIOR_AGE_CENTER) / 10.0).alias("age_c"),
        (pl.col("height_in").fill_null((pl.col("height_in") * w).sum() / w.filter(pl.col("height_in").is_not_null()).sum())).alias("_h"),
        (pl.col("position_num").fill_null(3.0) - 3.0).alias("position_c"),
    )  # fmt: skip
    f = f.with_columns(
        (pl.col("age_c") ** 2).alias("age_c2"),
        (pl.col("_h") - (pl.col("_h") * w).sum() / w.sum()).alias("height_rel"),
        pl.col("pts_rel").alias("pts_pad"),
        pl.col("ast_rel").alias("ast_pad"),
    )
    ctx = _context(possessions, f)
    out = f.join(ctx, on="player_id", how="left").select(
        "player_id", "poss", "def_poss", *[pl.col(c).fill_null(0.0).cast(pl.Float64) for c in BOX_PRIOR_FEATURES]
    )
    return out.to_pandas() if return_as_pandas else out


@dataclass(frozen=True)
class BoxPriorModel:
    """A cross-fitted box prior: one offense and one defense linear rule per player fold.

    Attributes:
        feature_names: Ordered feature columns (:data:`BOX_PRIOR_FEATURES`).
        o_coef: ``(folds, F)`` offense coefficients on the raw features.
        o_intercept: ``(folds,)``.
        d_coef: ``(folds, F)`` defense coefficients (``d`` positive = good defense).
        d_intercept: ``(folds,)``.
        season: The season the prior is for.
        train_seasons: The seasons it learned from.
    """

    feature_names: list[str]
    o_coef: np.ndarray
    o_intercept: np.ndarray
    d_coef: np.ndarray
    d_intercept: np.ndarray
    season: int
    train_seasons: list[int]


def train_box_prior(
    panel: pl.DataFrame,
    *,
    season: int,
    half_life: float = BOX_PRIOR_HALF_LIFE,
    alpha: float = BOX_PRIOR_RIDGE_ALPHA,
    folds: int = BOX_PRIOR_FOLDS,
) -> BoxPriorModel:
    """Possession-weighted ridge of the career target on the box-prior features, cross-fit by player.

    Fold ``k``'s rule is fit on every row whose player is NOT in fold ``k`` (``player_id % folds``) and
    is applied only to fold ``k``'s players, so no player's prior saw his own target. Each row is
    weighted by its possessions times the time weight of its season relative to ``season``.

    Args:
        panel: Training rows: ``player_id``, ``season``, ``poss``, ``def_poss``, ``o_target``,
            ``d_target`` and :data:`BOX_PRIOR_FEATURES`; seasons from :func:`box_prior_training_seasons`
            (a row of ``season`` itself raises).
        season: The season the prior is for.
        half_life: Half-life of the row time weight, in seasons.
        alpha: Ridge penalty on the standardized features.
        folds: Player folds.

    Returns:
        A :class:`BoxPriorModel`.

    Raises:
        ValueError: If ``panel`` holds a row of ``season`` or a fold has no training rows.

    Example:
        Train and apply a 2015 prior::

            from sportsdataverse.nba.nba_box_prior import apply_box_prior, train_box_prior
            model = train_box_prior(panel, season=2015)
            prior = apply_box_prior(features_2015, model)
    """
    if panel.filter(pl.col("season") == season).height:
        raise ValueError(f"the training panel holds rows of the prior's own season {season}")
    p = panel.filter((pl.col("poss") + pl.col("def_poss")) > 0)
    X = p.select(BOX_PRIOR_FEATURES).to_numpy().astype(np.float64)
    bad = [c for c, ok in zip(BOX_PRIOR_FEATURES, np.isfinite(X).all(0), strict=True) if not ok]
    bad += [c for c in ("o_target", "d_target") if not np.isfinite(p[c].to_numpy()).all()]
    if bad:  # one NaN row would turn every fold's rule into NaN without an error
        raise ValueError(f"non-finite values in the training panel columns {bad}")
    w = ((p["poss"] + p["def_poss"]).to_numpy() / 2.0) * _time_weight(p["season"].to_numpy(), season, half_life)
    fold = (p["player_id"].to_numpy().astype(np.int64)) % folds
    F = len(BOX_PRIOR_FEATURES)
    oc, dc = np.zeros((folds, F)), np.zeros((folds, F))
    oi, di = np.zeros(folds), np.zeros(folds)
    for k in range(folds):
        tr = fold != k
        if not tr.any():
            raise ValueError(f"fold {k} has no training rows")
        Xt, wt = X[tr], w[tr]
        mu = (wt[:, None] * Xt).sum(0) / wt.sum()
        sd = np.sqrt((wt[:, None] * (Xt - mu) ** 2).sum(0) / wt.sum())
        sd[sd == 0] = 1.0
        Z = (Xt - mu) / sd
        G = Z.T @ (wt[:, None] * Z) + alpha * np.eye(F)
        for tgt, cf, ic in (("o_target", oc, oi), ("d_target", dc, di)):
            y = p[tgt].to_numpy()[tr]
            ybar = (wt * y).sum() / wt.sum()
            beta = np.linalg.solve(G, Z.T @ (wt * (y - ybar)))
            cf[k] = beta / sd
            ic[k] = ybar - (beta / sd) @ mu
    return BoxPriorModel(
        feature_names=list(BOX_PRIOR_FEATURES), o_coef=oc, o_intercept=oi, d_coef=dc, d_intercept=di,
        season=int(season), train_seasons=sorted(int(s) for s in p["season"].unique()),
    )  # fmt: skip


def apply_box_prior(
    features: pl.DataFrame, model: BoxPriorModel, *, return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Score box-prior features with each player's out-of-fold rule.

    Args:
        features: Output of :func:`box_prior_features` (``player_id`` and :data:`BOX_PRIOR_FEATURES`).
        model: A :class:`BoxPriorModel` from :func:`train_box_prior`.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        ``player_id`` (Int64), ``o_prior``, ``d_prior``, ``box_prior`` (Float64, per 100; ``d`` positive
        = good defense; ``box_prior = o_prior + d_prior``).

    Example:
        The prior as the dict :func:`~sportsdataverse.nba.nba_reml_rapm.nba_reml_rapm` takes::

            prior = apply_box_prior(feats, model)
            prior_dict = {r[0]: (r[1], r[2]) for r in prior.select("player_id", "o_prior", "d_prior").iter_rows()}
    """
    X = features.select(model.feature_names).to_numpy().astype(np.float64)
    k = features["player_id"].to_numpy().astype(np.int64) % model.o_coef.shape[0]
    o = (X * model.o_coef[k]).sum(1) + model.o_intercept[k]
    d = (X * model.d_coef[k]).sum(1) + model.d_intercept[k]
    out = pl.DataFrame(
        {"player_id": features["player_id"].cast(pl.Int64), "o_prior": o, "d_prior": d, "box_prior": o + d}
    )
    return out.to_pandas() if return_as_pandas else out
