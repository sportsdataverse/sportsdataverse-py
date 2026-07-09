"""NHL/PWHL player-impact shared constants, metrics, and the ridge solver (league-agnostic).

Single home for the ``LEAGUE_CONSTANTS`` table (danger-zone geometry, ``goals_per_win``,
replacement levels, RAPM lambda grid, GSAx baseline, PP/PK league rates, faceoff/penalty
goal weights) plus the metric/solver helpers shared by every player-impact model
(``nhl_xg``, ``nhl_gsax``, ``nhl_rapm``, ``nhl_unit_ratings``, ``nhl_special_teams``,
``nhl_war``) and their PWHL by-reference shims.

The design boundary this module encodes: the *math* (weighted-ridge RAPM, xG feature
recipe, GAR->WAR conversion) is league-agnostic; only the *fitted constants* differ
between the NHL and the PWHL. Every engine function takes a ``league`` argument backed
by ``LEAGUE_CONSTANTS[league]`` -- see the CFB/NFL and MBB/WBB model splits for the same
pattern.

Example:
    Quick start::

        from sportsdataverse.nhl.nhl_player_impact_constants import get_constants
        cfg = get_constants("nhl")
        print(cfg.goals_per_win)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import scipy.sparse as sp
from scipy.sparse.linalg import cg
from scipy.stats import rankdata

__all__ = [
    "ImpactConfig",
    "LEAGUE_CONSTANTS",
    "get_constants",
    "booster_cache_dir",
    "NHL_TEAM_FULLNAME_TO_ABBR",
    "team_fullname_to_abbr",
    "spearman_corr",
    "calibration_table",
    "weighted_ridge",
]


@dataclass(frozen=True)
class ImpactConfig:
    """League-specific constants consumed by every player-impact engine function.

    Attributes:
        goals_per_win: goals-per-win denominator for GAR->WAR (Task 6.2 fits the NHL
            value from team wins vs goal differential; seeded here until fit).
        replacement_ev_off: EV offense replacement-level rate (xG/60), subtracted before
            summing GAR.
        replacement_ev_def: EV defense replacement-level rate (xGA/60 suppressed).
        league_xg_rate_ev: league-average even-strength xG rate (per 60), used as the
            RAPM intercept sanity check.
        league_xg_rate_pp: league-average power-play xGF rate (per 60).
        league_xg_rate_pk: league-average penalty-kill xGA rate (per 60).
        rapm_lambda_grid: candidate ridge penalties for the skater RAPM CV.
        penalty_goal_weight: goals-per-(penalty drawn - taken) conversion.
        faceoff_goal_weight: goals-per-(faceoff win - 0.5) conversion.
        rink_x_goal_line: absolute rink x-coordinate of the goal line (feet), used by the
            shot-geometry expansion.
        danger_high: ``{"max_distance": float, "max_angle": float}`` band for "high" danger.
        danger_medium: same shape, wider band for "medium" danger; outside both -> "low".
        xg_booster_league: which league's published boosters back this league's ``nhl_xg``
            scoring (the PWHL borrows the NHL boosters -- a documented approximation).
    """

    goals_per_win: float
    replacement_ev_off: float
    replacement_ev_def: float
    league_xg_rate_ev: float
    league_xg_rate_pp: float
    league_xg_rate_pk: float
    rapm_lambda_grid: list[float] = field(default_factory=lambda: [10.0, 100.0, 1000.0, 5000.0])
    penalty_goal_weight: float = 0.18
    faceoff_goal_weight: float = 0.02
    rink_x_goal_line: float = 89.0
    danger_high: dict = field(default_factory=lambda: {"max_distance": 20.0, "max_angle": 30.0})
    danger_medium: dict = field(default_factory=lambda: {"max_distance": 40.0, "max_angle": 55.0})
    xg_booster_league: str = "nhl"


# `goals_per_win` for the NHL is the fitted output of
# `dev/nhl_player_impact/fit_goals_per_win.py` (Task 6.2): OLS `wins = a + b*goal_diff`
# over every team's full 2024-25 regular season (31 teams, `load_nhl_schedule(2025)`),
# `goals_per_win = 1/b = 6.596` -- run 2026-07-08, consistent with the widely-published
# NHL rule-of-thumb range (~6-6.5, Perry/EvolvingHockey, MoneyPuck). Every other NHL
# constant below remains a documented seed pending its own fitting task.
LEAGUE_CONSTANTS: dict[str, ImpactConfig] = {
    "nhl": ImpactConfig(
        goals_per_win=6.596,
        replacement_ev_off=1.9,
        replacement_ev_def=2.6,
        league_xg_rate_ev=2.5,
        league_xg_rate_pp=6.2,
        league_xg_rate_pk=6.2,
        xg_booster_league="nhl",
    ),
    "pwhl": ImpactConfig(
        # PWHL constants are approximate seeds (published PWHL analytics are sparse -- ~2
        # seasons of history as of 2026); revisit as PWHL-native research matures.
        goals_per_win=6.0,
        replacement_ev_off=1.6,
        replacement_ev_def=2.2,
        league_xg_rate_ev=2.1,
        league_xg_rate_pp=5.4,
        league_xg_rate_pk=5.4,
        xg_booster_league="nhl",  # PWHL has no PWHL-trained xG model -- borrows NHL boosters.
    ),
}


def get_constants(league: str) -> ImpactConfig:
    """Return the ``ImpactConfig`` for ``league``.

    Args:
        league: ``"nhl"`` or ``"pwhl"``.

    Returns:
        The league's ``ImpactConfig``.

    Raises:
        ValueError: if ``league`` is not a registered key of ``LEAGUE_CONSTANTS``.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_player_impact_constants import get_constants
            cfg = get_constants("pwhl")
            print(cfg.xg_booster_league)  # "nhl" -- the borrowed-booster caveat
    """
    try:
        return LEAGUE_CONSTANTS[league]
    except KeyError as exc:
        raise ValueError(f"Unknown league {league!r}; expected one of {sorted(LEAGUE_CONSTANTS)}") from exc


def booster_cache_dir(override: str | Path | None = None) -> Path:
    """Resolve the local cache directory for the downloaded ``nhl_xg_models`` boosters.

    Precedence: explicit ``override`` argument > ``NHL_XG_MODEL_DIR`` env var >
    ``~/.cache/nhl_xg_models``.

    Args:
        override: an explicit directory (e.g. a committed test-fixture dir); wins over
            the env var when given.

    Returns:
        The resolved ``pathlib.Path`` (not created here -- ``ensure_xg_models`` creates
        it on first download).

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_player_impact_constants import booster_cache_dir
            d = booster_cache_dir()
    """
    if override is not None:
        return Path(override)
    env = os.environ.get("NHL_XG_MODEL_DIR")
    if env:
        return Path(env)
    return Path.home() / ".cache" / "nhl_xg_models"


# ---------------------------------------------------------------------------
# Team full-name <-> abbreviation crosswalk.
#
# `load_nhl_pbp_full` carries team identity as `event_team_abbr` / `home_abbr` /
# `away_abbr`; `load_nhl_shifts` carries team identity as the full display name
# (`event_team`, e.g. "Buffalo Sabres"). The stint builder must join xG events (keyed
# by abbr) against shift stints (keyed by full name), so this static crosswalk is the
# join-key bridge. Static (not network-fetched) by design -- same pattern as
# `nfl/datasets.py::team_abbr_mapping`.
# ---------------------------------------------------------------------------
NHL_TEAM_FULLNAME_TO_ABBR: dict[str, str] = {
    "Anaheim Ducks": "ANA",
    "Arizona Coyotes": "ARI",
    "Boston Bruins": "BOS",
    "Buffalo Sabres": "BUF",
    "Calgary Flames": "CGY",
    "Carolina Hurricanes": "CAR",
    "Chicago Blackhawks": "CHI",
    "Colorado Avalanche": "COL",
    "Columbus Blue Jackets": "CBJ",
    "Dallas Stars": "DAL",
    "Detroit Red Wings": "DET",
    "Edmonton Oilers": "EDM",
    "Florida Panthers": "FLA",
    "Los Angeles Kings": "LAK",
    "Minnesota Wild": "MIN",
    "Montreal Canadiens": "MTL",
    "Montréal Canadiens": "MTL",
    "Nashville Predators": "NSH",
    "New Jersey Devils": "NJD",
    "New York Islanders": "NYI",
    "New York Rangers": "NYR",
    "Ottawa Senators": "OTT",
    "Philadelphia Flyers": "PHI",
    "Pittsburgh Penguins": "PIT",
    "San Jose Sharks": "SJS",
    "Seattle Kraken": "SEA",
    "St. Louis Blues": "STL",
    "St Louis Blues": "STL",
    "Tampa Bay Lightning": "TBL",
    "Toronto Maple Leafs": "TOR",
    "Utah Hockey Club": "UTA",
    "Utah Mammoth": "UTA",
    "Vancouver Canucks": "VAN",
    "Vegas Golden Knights": "VGK",
    "Washington Capitals": "WSH",
    "Winnipeg Jets": "WPG",
    # Historical / relocated identities that may appear in older shift captures.
    "Atlanta Thrashers": "ATL",
    "Phoenix Coyotes": "PHX",
}


def team_fullname_to_abbr(name: str) -> str | None:
    """Map an NHL full team display name to its abbreviation, or ``None`` if unknown.

    Args:
        name: a full team display name as it appears in ``load_nhl_shifts``'s
            ``event_team`` column (e.g. ``"Buffalo Sabres"``).

    Returns:
        The team abbreviation matching ``load_nhl_pbp_full``'s ``event_team_abbr`` /
        ``home_abbr`` / ``away_abbr`` convention, or ``None`` for an unmapped name.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_player_impact_constants import team_fullname_to_abbr
            team_fullname_to_abbr("Buffalo Sabres")  # "BUF"
    """
    return NHL_TEAM_FULLNAME_TO_ABBR.get(name)


def spearman_corr(a: np.ndarray, b: np.ndarray) -> float:
    """Spearman rank correlation between two 1-D arrays (no scipy.stats.spearmanr dep).

    Args:
        a: first sample.
        b: second sample, same length as ``a``.

    Returns:
        The Pearson correlation of the rank-transformed samples.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nhl.nhl_player_impact_constants import spearman_corr
            spearman_corr(np.array([1, 2, 3]), np.array([3, 6, 9]))  # 1.0
    """
    ra, rb = rankdata(a), rankdata(b)
    return float(np.corrcoef(ra, rb)[0, 1])


def calibration_table(y_true: np.ndarray, p_pred: np.ndarray, n_bins: int = 10) -> pl.DataFrame:
    """Bucket predicted probabilities into ``n_bins`` and compare to the realized rate.

    Args:
        y_true: binary outcomes (0/1), e.g. ``event_type == "GOAL"``.
        p_pred: predicted probabilities, e.g. per-shot ``xg``.
        n_bins: number of equal-width probability bins.

    Returns:
        polars.DataFrame: ``bin_mid:Float64, mean_pred:Float64, mean_actual:Float64,
        n:Int64`` -- reliability tracks the diagonal (``mean_actual`` monotone in
        ``bin_mid``) when the model is well-calibrated.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nhl.nhl_player_impact_constants import calibration_table
            tbl = calibration_table(np.array([0, 1, 1, 0]), np.array([0.1, 0.8, 0.6, 0.2]))
    """
    df = pl.DataFrame({"y": np.asarray(y_true, float), "p": np.asarray(p_pred, float)})
    df = df.with_columns((pl.col("p").clip(0.0, 0.9999) * n_bins).floor().cast(pl.Int64).alias("bin"))
    return (
        df.group_by("bin")
        .agg(pl.col("p").mean().alias("mean_pred"), pl.col("y").mean().alias("mean_actual"), pl.len().alias("n"))
        .sort("bin")
        .with_columns(((pl.col("bin") + 0.5) / n_bins).alias("bin_mid"))
        .select("bin_mid", "mean_pred", "mean_actual", "n")
    )


def weighted_ridge(X: Any, y: np.ndarray, w: np.ndarray, lam: float) -> np.ndarray:
    """Solve the weighted ridge normal equations ``(X'WX + lam*I)^-1 X'Wy``.

    Dense path (``numpy.linalg.solve``) for small/dense ``X``; conjugate-gradient
    (``scipy.sparse.linalg.cg``) for ``scipy.sparse`` ``X`` (the skater-RAPM design
    matrix, ~thousands of columns).

    Args:
        X: design matrix, dense ``numpy.ndarray`` or any ``scipy.sparse`` matrix.
        y: response vector.
        w: nonnegative observation weights (e.g. stint duration in seconds).
        lam: ridge penalty.

    Returns:
        The fitted coefficient vector.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nhl.nhl_player_impact_constants import weighted_ridge
            X = np.array([[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]])
            y = np.array([2.0, -1.0, 1.0])
            beta = weighted_ridge(X, y, np.ones(3), lam=1e-6)
    """
    y = np.asarray(y, float)
    w = np.asarray(w, float)
    if sp.issparse(X):
        W = sp.diags(w)
        A = (X.T @ W @ X).tocsc()
        A = A + lam * sp.identity(A.shape[0], format="csc")
        b = X.T @ (w * y)
        beta, _ = cg(A, b, atol=1e-8, maxiter=10000)
        return beta
    X = np.asarray(X, float)
    A = X.T @ (w[:, None] * X) + lam * np.eye(X.shape[1])
    b = X.T @ (w * y)
    return np.linalg.solve(A, b)
