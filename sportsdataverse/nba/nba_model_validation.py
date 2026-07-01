"""Out-of-sample validation harness for the NBA model zoo.

A model is a design-matrix estimator (``fit(X, y) -> FitResult``); the harness
owns the player-id column map (from ``build_rapm_design``) and scores fitted
coefficients against held-out games. See the spec for the four oracles and the
synthetic meta-oracle that proves the harness itself correct.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Protocol, Tuple

import numpy as np
import polars as pl
from scipy.sparse import csr_matrix
from sklearn.linear_model import RidgeCV

from .nba_rapm import DEFAULT_RAPM_ALPHAS, build_rapm_design

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


@dataclass(frozen=True)
class RetrodictionResult:
    """Out-of-sample retrodiction metrics from k-fold-over-games cross-validation.

    Attributes:
        game_margin_rmse: RMSE of predicted vs actual per-(game, team) margins on
            held-out games, pooled across all folds.
        game_margin_corr: Pearson correlation of predicted vs actual margins,
            pooled across all folds. ``nan`` / 0 when fewer than 2 test margins.
        baseline_rmse: RMSE of a zero-margin (tossup) baseline on the same held-out
            margins — predicts 0 margin for every game; the floor any model must beat.
        poss_rmse: RMSE of per-possession predicted vs actual points, pooled across
            all folds (granular sanity check).
        n_test_games: Total number of distinct game_ids actually evaluated across all
            non-degenerate folds (folds skipped for empty train/test or no players
            are excluded).
    """

    game_margin_rmse: float
    game_margin_corr: float
    baseline_rmse: float
    poss_rmse: float
    n_test_games: int


def _team_game_margins(
    possessions: pl.DataFrame,
    pred_points: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray]:
    """Aggregate per-possession predicted vs actual points to per-(game, team) margins.

    A team's margin = (points it scored on its offensive possessions) minus
    (points it allowed on its defensive possessions = the opponent's offensive
    possessions). Returned as aligned (predicted, actual) arrays over each
    (game_id, team) present.

    Args:
        possessions: Possession frame with ``game_id``, ``offense_team_id``, and
            ``points`` columns. Must not be empty.
        pred_points: Per-possession predicted points, shape ``(n_rows,)``, aligned
            row-for-row with ``possessions``.

    Returns:
        ``(predicted_margins, actual_margins)`` as float64 numpy arrays of equal
        length — one entry per (game_id, offense_team_id) pair.
    """
    df = possessions.select(["game_id", "offense_team_id", "points"]).with_columns(pl.Series("pred", pred_points))
    scored = df.group_by(["game_id", "offense_team_id"]).agg(
        pl.col("points").sum().alias("scored"),
        pl.col("pred").sum().alias("scored_pred"),
    )
    game_tot = df.group_by("game_id").agg(
        pl.col("points").sum().alias("g_pts"),
        pl.col("pred").sum().alias("g_pred"),
    )
    m = scored.join(game_tot, on="game_id", how="left")
    m = m.with_columns(
        (pl.col("scored") - (pl.col("g_pts") - pl.col("scored"))).alias("actual_margin"),
        (pl.col("scored_pred") - (pl.col("g_pred") - pl.col("scored_pred"))).alias("pred_margin"),
    )
    return (
        m["pred_margin"].to_numpy().astype(np.float64),
        m["actual_margin"].to_numpy().astype(np.float64),
    )


def _rmse(a: np.ndarray, b: np.ndarray) -> float:
    """Root mean squared error between arrays ``a`` and ``b``.

    Args:
        a: Predicted values.
        b: Actual values.

    Returns:
        ``float`` RMSE, or ``nan`` when the arrays are empty.
    """
    return float(np.sqrt(np.mean((a - b) ** 2))) if a.size else float("nan")


def retrodiction(
    model: RapmModel,
    possessions: pl.DataFrame,
    *,
    k_folds: int = 5,
    seed: int = 0,
) -> RetrodictionResult:
    """Oracle ①: k-fold-over-games out-of-sample game-margin RMSE + correlation.

    Games are partitioned into ``k_folds`` disjoint folds (never split a game
    across train/test). For each fold: fit on the other games, predict the held-out
    games' possessions, aggregate to per-(game, team) margins. Scores pool all
    held-out margins. Baseline = zero-margin (tossup) predictor: predicts 0 margin
    for every held-out game.

    Args:
        model: A ``RapmModel`` instance.
        possessions: A season (or multi-game) possession+lineup frame with
            ``game_id``, ``offense_team_id``, ``points``, and the ten lineup
            columns (``off_player_1..5``, ``def_player_1..5``).
        k_folds: Number of game folds (default 5).
        seed: RNG seed for the game shuffle (default 0 for determinism).

    Returns:
        ``RetrodictionResult``. All metrics are ``nan`` and ``n_test_games=0``
        on empty input or when every fold is degenerate (no train or test rows).
    """
    if possessions.is_empty():
        return RetrodictionResult(float("nan"), float("nan"), float("nan"), float("nan"), 0)

    games = possessions["game_id"].unique().to_list()
    rng = np.random.default_rng(seed)
    rng.shuffle(games)
    folds = np.array_split(np.array(games, dtype=object), min(k_folds, len(games)))

    pred_m: List[np.ndarray] = []
    act_m: List[np.ndarray] = []
    base_m: List[np.ndarray] = []
    poss_pred: List[np.ndarray] = []
    poss_act: List[np.ndarray] = []
    evaluated_games: set = set()

    for fold in folds:
        test_ids = set(fold.tolist())
        train = possessions.filter(~pl.col("game_id").is_in(list(test_ids)))
        test = possessions.filter(pl.col("game_id").is_in(list(test_ids)))
        if train.is_empty() or test.is_empty():
            continue
        X_tr, y_tr, pids = build_rapm_design(train)
        if not pids:
            continue
        fit = model.fit(X_tr, y_tr)
        X_te, y_te = _design_with_ids(test, pids)
        # rebuild aligned test frame (rows with null lineups are dropped by _design_with_ids)
        test_valid = test.drop_nulls(subset=_OFF + _DEF)
        pp = predict_points(X_te, fit)
        p_pred, p_act = _team_game_margins(test_valid, pp)
        pred_m.append(p_pred)
        act_m.append(p_act)
        base_m.append(np.zeros_like(p_act))  # zero-margin (tossup) baseline
        poss_pred.append(pp)
        poss_act.append(y_te)
        evaluated_games |= test_ids

    if not pred_m:
        return RetrodictionResult(float("nan"), float("nan"), float("nan"), float("nan"), 0)

    P = np.concatenate(pred_m)
    A = np.concatenate(act_m)
    B = np.concatenate(base_m)
    corr = float(np.corrcoef(P, A)[0, 1]) if P.size > 1 and np.std(P) > 0 else 0.0
    return RetrodictionResult(
        game_margin_rmse=_rmse(P, A),
        game_margin_corr=corr,
        baseline_rmse=_rmse(B, A),
        poss_rmse=_rmse(np.concatenate(poss_pred), np.concatenate(poss_act)),
        n_test_games=len(evaluated_games),
    )


@dataclass(frozen=True)
class ReliabilityResult:
    """Split-half reliability of per-player ratings with Spearman-Brown adjustment.

    Attributes:
        split_half_corr: Pearson correlation of per-player total ratings fitted on
            the two random game halves. ``nan`` when fewer than 3 shared players.
        spearman_brown: Spearman-Brown prophecy formula correction for the full-season
            test length: ``2r / (1 + r)``. ``nan`` when ``split_half_corr`` is ``nan``
            or exactly -1.
        n_shared_players: Number of players present in both halves (the basis for
            the correlation).
    """

    split_half_corr: float
    spearman_brown: float
    n_shared_players: int


def _fit_ratings(model: RapmModel, possessions: pl.DataFrame) -> Dict[int, float]:
    """Fit the model on ``possessions`` and return player_id -> total rating (o - d_raw).

    Args:
        model: A ``RapmModel`` instance.
        possessions: A possession+lineup frame.

    Returns:
        Dict mapping player_id to total impact rating (offense coef minus defense coef).
        Empty dict when ``possessions`` has no players.
    """
    X, y, pids = build_rapm_design(possessions)
    if not pids:
        return {}
    fit = model.fit(X, y)
    P = len(pids)
    total = fit.coef[:P] - fit.coef[P:]  # offense minus (raw) defense coef = total impact
    return {int(p): float(total[k]) for k, p in enumerate(pids)}


def reliability(model: RapmModel, possessions: pl.DataFrame, *, seed: int = 0) -> ReliabilityResult:
    """Oracle ②: split-half reliability of the per-player rating across two game halves.

    Randomly halves the games, fits each half independently, and correlates the
    per-player total ratings over players present in both. Reports the raw split-
    half correlation and the Spearman-Brown-adjusted full-season reliability
    ``2r / (1 + r)``.

    Args:
        model: A ``RapmModel`` instance.
        possessions: A season (or multi-game) possession+lineup frame with
            ``game_id``, ``offense_team_id``, ``points``, and the ten lineup
            columns (``off_player_1..5``, ``def_player_1..5``).
        seed: RNG seed for the game shuffle (default 0 for determinism).

    Returns:
        ``ReliabilityResult``. ``split_half_corr`` and ``spearman_brown`` are ``nan``
        and ``n_shared_players=0`` on empty input or when fewer than 3 players are
        shared between the two halves.
    """
    if possessions.is_empty():
        return ReliabilityResult(float("nan"), float("nan"), 0)
    games = possessions["game_id"].unique().to_list()
    rng = np.random.default_rng(seed)
    rng.shuffle(games)
    mid = len(games) // 2
    a = possessions.filter(pl.col("game_id").is_in(games[:mid]))
    b = possessions.filter(pl.col("game_id").is_in(games[mid:]))
    ra, rb = _fit_ratings(model, a), _fit_ratings(model, b)
    shared = sorted(set(ra) & set(rb))
    if len(shared) < 3:
        return ReliabilityResult(float("nan"), float("nan"), len(shared))
    va = np.array([ra[p] for p in shared])
    vb = np.array([rb[p] for p in shared])
    r = float(np.corrcoef(va, vb)[0, 1]) if np.std(va) > 0 and np.std(vb) > 0 else 0.0
    sb = (2 * r / (1 + r)) if r > -1 else float("nan")
    return ReliabilityResult(split_half_corr=r, spearman_brown=sb, n_shared_players=len(shared))


@dataclass(frozen=True)
class CrossSeasonResult:
    """Cross-season predictivity metrics from season-N ratings applied to season N+1.

    Attributes:
        outcome_corr: Pearson correlation of predicted vs actual per-(game, team)
            margins in season N+1 when predicted with season-N coefficients.
            ``nan`` when fewer than 2 adjacent season pairs are available.
        outcome_rmse: RMSE of the same margin predictions. ``nan`` on insufficient data.
        rating_corr: Pearson correlation of season-N per-player ratings with
            season N+1 ratings over players shared across both seasons. Tests
            whether skill persists year-to-year. ``nan`` when fewer than 3 shared
            players exist.
        coverage_pct: Percentage of season N+1 lineup slots (across all valid
            possessions) whose player was seen in season N. Measures how much of
            the N+1 population is covered by the N model.
        n_shared_players: Total shared-player count summed across all adjacent pairs.
    """

    outcome_corr: float
    outcome_rmse: float
    rating_corr: float
    coverage_pct: float
    n_shared_players: int


def cross_season(
    model: RapmModel,
    season_frames: List[pl.DataFrame],
    *,
    unknown_player_rating: float = 0.0,
) -> CrossSeasonResult:
    """Oracle ③: do season-N ratings predict season N+1 outcomes and ratings?

    For each adjacent (N, N+1) pair: fit N -> coef_N; predict N+1's possessions
    with coef_N (players unseen in N contribute ``unknown_player_rating``, v1=0);
    aggregate to N+1 game margins for the outcome scores. Also correlate the N
    ratings with the N+1 ratings over shared players. Multiple pairs are averaged.
    ``coverage_pct`` = share of N+1 lineup slots whose player was seen in N.

    Args:
        model: A ``RapmModel`` instance.
        season_frames: Ordered list of per-season possession frames (season N,
            season N+1, ...). Must contain at least 2 frames to produce non-nan
            results.
        unknown_player_rating: Reserved; only ``0.0`` (skip unknown players) is
            implemented in v1.

    Returns:
        ``CrossSeasonResult``. All metrics are ``nan`` and ``n_shared_players=0``
        when fewer than 2 season frames are provided.
    """
    if len(season_frames) < 2:
        return CrossSeasonResult(float("nan"), float("nan"), float("nan"), float("nan"), 0)
    out_corrs: List[float] = []
    out_rmses: List[float] = []
    rate_corrs: List[float] = []
    covs: List[float] = []
    shared_counts: List[int] = []
    for n, np1 in zip(season_frames, season_frames[1:]):
        X_n, y_n, pids_n = build_rapm_design(n)
        if not pids_n:
            continue
        fit_n = model.fit(X_n, y_n)
        # outcome: predict N+1 with coef_N
        X_np1, _ = _design_with_ids(np1, pids_n, unknown_player_rating=unknown_player_rating)
        np1_valid = np1.drop_nulls(subset=_OFF + _DEF)
        pp = predict_points(X_np1, fit_n)
        p_pred, p_act = _team_game_margins(np1_valid, pp)
        if p_act.size > 1 and np.std(p_pred) > 0:
            out_corrs.append(float(np.corrcoef(p_pred, p_act)[0, 1]))
            out_rmses.append(_rmse(p_pred, p_act))
        # rating persistence
        ratings_n = _fit_ratings(model, n)
        ratings_np1 = _fit_ratings(model, np1)
        shared = sorted(set(ratings_n) & set(ratings_np1))
        if len(shared) >= 3:
            va = np.array([ratings_n[p] for p in shared])
            vb = np.array([ratings_np1[p] for p in shared])
            if np.std(va) > 0 and np.std(vb) > 0:
                rate_corrs.append(float(np.corrcoef(va, vb)[0, 1]))
            shared_counts.append(len(shared))
        # coverage: fraction of N+1 lineup ids seen in N
        seen = set(pids_n)
        np1_ids = np1_valid.select(_OFF + _DEF).to_numpy().ravel()
        covs.append(100.0 * np.mean([int(x) in seen for x in np1_ids]) if np1_ids.size else 0.0)

    def _mean(xs: List[float]) -> float:
        return float(np.mean(xs)) if xs else float("nan")

    return CrossSeasonResult(
        outcome_corr=_mean(out_corrs),
        outcome_rmse=_mean(out_rmses),
        rating_corr=_mean(rate_corrs),
        coverage_pct=_mean(covs),
        n_shared_players=int(np.sum(shared_counts)) if shared_counts else 0,
    )


@dataclass(frozen=True)
class CalibrationResult:
    """Empirical coverage of a posterior model's credible intervals.

    Attributes:
        levels: The nominal coverage levels requested (e.g. ``[0.5, 0.9]``).
        coverage: Empirical fraction of held-out player ratings falling inside
            the credible interval at each corresponding level in ``levels``.
        n_players: Number of players shared across both game halves (the basis
            for coverage computation).
    """

    levels: List[float]
    coverage: List[float]
    n_players: int


def calibration(
    model: RapmModel,
    possessions: pl.DataFrame,
    *,
    levels: Tuple[float, ...] = (0.5, 0.8, 0.9, 0.95),
    seed: int = 0,
) -> Optional[CalibrationResult]:
    """Oracle ④ (forward-looking hook): empirical coverage of the model's intervals.

    Fits the full frame; if the fit has no ``posterior`` (point model) returns
    ``None`` (n/a). Otherwise splits games in half, treats each player's half-B
    rating as the held-out "truth", forms central credible intervals from the
    half-A posterior at each level, and reports the fraction of players whose
    truth falls inside — the calibration curve.

    Args:
        model: A ``RapmModel`` instance; must emit ``FitResult.posterior`` (an
            ``(S, 2P)`` sample array) to produce a non-``None`` result.
        possessions: A season (or multi-game) possession+lineup frame with
            ``game_id``, ``offense_team_id``, ``points``, and the ten lineup
            columns (``off_player_1..5``, ``def_player_1..5``).
        levels: Nominal coverage levels to evaluate (default
            ``(0.5, 0.8, 0.9, 0.95)``).
        seed: RNG seed for the game shuffle (default 0 for determinism).

    Returns:
        ``CalibrationResult`` with the empirical coverage curve, or ``None``
        when the model is a point estimator (``posterior`` is ``None``) or when
        fewer than 3 players are shared between the two game halves.
    """
    if possessions.is_empty():
        return None
    games = possessions["game_id"].unique().to_list()
    rng = np.random.default_rng(seed)
    rng.shuffle(games)
    mid = len(games) // 2
    a = possessions.filter(pl.col("game_id").is_in(games[:mid]))
    b = possessions.filter(pl.col("game_id").is_in(games[mid:]))
    Xa, ya, pids_a = build_rapm_design(a)
    if not pids_a:
        return None
    fit_a = model.fit(Xa, ya)
    if fit_a.posterior is None:
        return None
    P = len(pids_a)
    post_total = fit_a.posterior[:, :P] - fit_a.posterior[:, P:]  # (S, P) total-impact posterior
    truth_b = _fit_ratings(model, b)
    shared = [(k, p) for k, p in enumerate(pids_a) if p in truth_b]
    if len(shared) < 3:
        return None
    cover: List[float] = []
    for lvl in levels:
        lo_q, hi_q = (1 - lvl) / 2, 1 - (1 - lvl) / 2
        hits = 0
        for k, p in shared:
            lo, hi = np.quantile(post_total[:, k], [lo_q, hi_q])
            if lo <= truth_b[p] <= hi:
                hits += 1
        cover.append(hits / len(shared))
    return CalibrationResult(levels=list(levels), coverage=cover, n_players=len(shared))


_TEAM_A, _TEAM_B = 100, 200


def _synthetic_possessions(
    o_ratings: Dict[int, float],
    d_ratings: Dict[int, float],
    *,
    n_games: int,
    poss_per_game: int,
    noise_sd: float,
    seed: int,
    base_points: float = 1.0,
) -> pl.DataFrame:
    """Generate possessions from KNOWN player ratings (the meta-oracle ground truth).

    Two teams (A=100 players are the first half of the id set, B=200 the second
    half). Each possession draws 5 offense from the possessing team and 5 defense
    from the opponent; expected points = ``base_points + sum(o) - sum(d)`` on the
    per-possession scale, observed = expected + Gaussian(0, ``noise_sd``), clamped
    at 0. Each game, both teams take ``poss_per_game`` offensive possessions.

    Args:
        o_ratings: player_id -> per-possession offensive rating.
        d_ratings: player_id -> per-possession defensive rating.
        n_games: Number of games to simulate.
        poss_per_game: Offensive possessions per team per game.
        noise_sd: Standard deviation of Gaussian observation noise.
        seed: Integer seed for the ``np.random.default_rng`` generator.
        base_points: League-average points per possession baseline.

    Returns:
        A possession+lineup frame (``game_id``/``offense_team_id``/``points``/
        ``off_player_1..5``/``def_player_1..5``), ``2 * n_games * poss_per_game`` rows.
    """
    rng = np.random.default_rng(seed)
    ids = sorted(o_ratings)
    half = len(ids) // 2
    team_players: Dict[int, List[int]] = {_TEAM_A: ids[:half], _TEAM_B: ids[half:]}
    rows: List[dict] = []
    for g in range(n_games):
        gid = f"SYN{g:05d}"
        for off_team, def_team in ((_TEAM_A, _TEAM_B), (_TEAM_B, _TEAM_A)):
            for _ in range(poss_per_game):
                off5 = rng.choice(team_players[off_team], size=5, replace=False)
                def5 = rng.choice(team_players[def_team], size=5, replace=False)
                mu = base_points + sum(o_ratings[int(p)] for p in off5) - sum(d_ratings[int(p)] for p in def5)
                pts = max(0.0, mu + rng.normal(0.0, noise_sd))
                row: dict = {"game_id": gid, "offense_team_id": off_team, "points": int(round(pts))}
                for i in range(5):
                    row[f"off_player_{i + 1}"] = int(off5[i])
                    row[f"def_player_{i + 1}"] = int(def5[i])
                rows.append(row)
    schema: Dict[str, pl.DataType] = {
        "game_id": pl.Utf8,
        "offense_team_id": pl.Int64,
        "points": pl.Int64,
        **{c: pl.Int64 for c in _OFF + _DEF},
    }
    return pl.DataFrame(rows, schema=schema)
