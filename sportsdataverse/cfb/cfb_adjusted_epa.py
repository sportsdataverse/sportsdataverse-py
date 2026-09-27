"""Opponent-adjusted EPA (ridge / RAPM-style) for college football play-by-play.

Reusable estimation primitives that separate a team's per-play EPA from its
schedule with a ridge regression on offense/defense team indicators (plus
home-field) -- the "Binion Box Score" opponent adjustment, lifted out of the
data-build layer so any caller can run it on a supplied frame.

Two entry points:

* :func:`cfb_adjusted_epa` -- one row per team for a whole season (the season
  figure used by the team-summary tables). The ridge is fit on the full season,
  so per-team values are in-sample/descriptive.
* :func:`cfb_adjusted_epa_by_game` -- one row per team-game, **walk-forward /
  point-in-time**: each week is adjusted using opponent strengths fit only on
  *prior* weeks. Leak-free, so the values are valid as in-season power-rating or
  model inputs (week 1 has no prior, so its adjustments are null; not-yet-seen
  opponents fall back to the league baseline, i.e. an average team).

Neither is a bundled model artifact (unlike the EP/WP/QBR ``.ubj`` files): the
ridge is fit *in-sample on team dummies*, so the coefficients *are* that window's
team strengths -- nothing to persist or apply to a different season. Faithful
port of cfbfastR's ``adjust_epa`` / ``cfbfastR-cfb-data`` ``espn_cfb_15``.

Required input columns (a cfbfastR-schema pbp frame): ``game_id``, ``pos_team``,
``pos_team_id``, ``def_pos_team_id``, ``home``, ``neutral_site``, ``EPA``,
``pass``, ``rush``, ``wp_before_naive`` (plus ``week`` for the by-game variant).

Which plays fit the opponent strengths: ``0.05 <= wp_before_naive <= 0.95``, the
score-and-clock win probability with no pregame spread. The spread-aware
``wp_before`` put 171 of 807 2025 FBS games outside a 10-90% band at 0-0 and kept
~11% of the plays in games with a 21+ point spread, so 72 games (the lopsided
cross-conference ones that link the conferences) gave the fit nothing. A team's
own per-game EPA still counts every play; only the strength fit is filtered.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, overload

import polars as pl

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["cfb_adjusted_epa", "cfb_adjusted_epa_by_game"]

# Ridge penalty, PER OBSERVATION -- `dropped_level_ridge` forms the sklearn
# penalty as `alpha = ridge_lambda * n_plays`.
#
# This was 325.0, ported from cfbfastR's glmnet call at `cv$lambda[[1]]`. Two
# things were wrong with carrying that number across:
#   1. `cv$lambda[[1]]` is the LARGEST lambda in glmnet's grid, which by
#      construction is the value at which the null model wins -- every
#      coefficient shrunk to zero.
#   2. glmnet's lambda and sklearn's alpha are not the same parameter, and the
#      solve multiplies by n on top, so 325 became alpha = 2.1e7 on a 64k-play
#      season.
# The result was a no-op: fitted team strengths spanned 0.0008 EPA/play across
# all of FBS, and `spearman(raw_off_epa, adjusted_off_epa)` was 0.999982 -- the
# "adjustment" did not reorder a single team, so G5 teams kept full credit for
# weak schedules (Toledo 8th, James Madison 5th) while P5 teams were buried
# (Florida 114th).
#
# 0.02 is tuned, not guessed: swept over five seasons (2021-2025) against ESPN
# FPI joined on team_id (no name matching), scoring each lambda per season and
# ranking by the MEAN, so a value that only wins on one year cannot take it.
# Mean Spearman across those seasons:
#
#     lambda   10     2.0    0.5    0.2    0.1    0.05   0.035  0.03   0.02   1e-4
#     rho      .792   .809   .848   .893   .917   .929   .9303  .930   .9307  .927
#
# The curve is flat across [0.001, 0.05] (spread 0.004), so the precise value
# matters far less than not being anywhere near 325. 0.035 is chosen over the
# nominal 0.02 peak because the two are statistically tied on the mean (.9303 vs
# .9307) while 0.035 has the better WORST season (.9015 vs .9013) AND clears the
# PFF-grade oracle gate, which 0.02 fails (0.7437 vs a 0.75 floor). PFF is a
# player-grade aggregation rather than play-level EPA, so it prefers slightly
# more shrinkage; 0.035 is the value that satisfies FPI, SP+ and PFF at once
# rather than optimizing one oracle into another's red.
#
# Cross-checked for 2025 against four independent oracles (SP+, FEI, F+, FPI),
# which agree with each other at 0.967-0.990: the old default scored rho 0.794
# with a mean rank error of 20 places; the tuned value scores ~0.96 with ~8.
# See tests/cfb/test_ridge_lambda_calibration.py.
#
# That tuning ran on the standardized fit (`dropped_level_ridge`), where a team
# with n_t plays is penalized by ~lambda * n_t: its own data weight scales the
# same way, so EVERY team shrank by the same 1/(1+lambda) ~ 3%, whether it
# rested on 500 plays or 1. In-season that is the whole board: 2026 week 4 rated
# Washington's offense +2.2 EPA/play from 9 competitive plays and put Utah State
# and Washington State (raw 124th/123rd, both had played Washington) 1st and
# 2nd. `_fit_team_strengths` keeps lambda but charges it in PLAYS: the penalty
# is `lambda * _TEAM_SEASON_PLAYS` for every team, so a team shrinks toward the
# league average by n_t / (n_t + ~20) -- the calibrated ~3% at a full season,
# 94% at 1 play.
_RIDGE_LAMBDA = 0.035

# Median fit-band (`_FIT_WP`) offensive pass/rush plays per FBS team in a full
# FBS-vs-FBS season: 595.5 / 571 / 564 in 2019 / 2021 / 2022 (fixed from those
# seasons; 2023-2025 were held out to evaluate the change). Anchors the penalty
# to a full team season. Was 440 under the old 0.1 <= wp_before <= 0.9 band.
_TEAM_SEASON_PLAYS = 577.0

# Plays that fit the opponent strengths: naive (score + clock, no spread) win
# probability inside 5-95%. Keeps 70-74% of FBS-vs-FBS pass/rush plays in every
# season 2014-2026 and no game gives the fit zero plays; `wp_before_naive` has no
# nulls in 2014-2026 (a null row would be left out of the fit only).
_FIT_WP = ("wp_before_naive", 0.05, 0.95)

_REQUIRED_COLUMNS = (
    "game_id",
    "pos_team",
    "pos_team_id",
    "def_pos_team_id",
    "home",
    "neutral_site",
    "EPA",
    "pass",
    "rush",
    "wp_before",
)
_BY_GAME_REQUIRED = (*_REQUIRED_COLUMNS, "week")
# `_REQUIRED_COLUMNS` / `_prepare` defaults stay on `wp_before` for `cfb_ratings`.
_ADJ_REQUIRED = (*_REQUIRED_COLUMNS[:-1], _FIT_WP[0])
_ADJ_BY_GAME_REQUIRED = (*_ADJ_REQUIRED, "week")

_EMPTY_OFFENSE = pl.DataFrame(schema={"team_id": pl.Utf8, "adjmodelOff": pl.Float64})
_EMPTY_DEFENSE = pl.DataFrame(schema={"team_id": pl.Utf8, "adjmodelDef": pl.Float64})


def _rank(col: str, *, descending: bool) -> pl.Expr:
    """R ``rank()`` -- average ties, ``na.last=TRUE`` (NA rows get trailing ranks)."""
    c = pl.col(col)
    base = c.rank(method="average", descending=descending)
    n_nonnull = c.is_not_null().sum()
    null_trail = (n_nonnull + c.is_null().cum_sum()).cast(pl.Float64)
    return pl.when(c.is_null()).then(null_trail).otherwise(base)


def _prepare(
    plays: pl.DataFrame | pd.DataFrame,
    required: tuple[str, ...],
    fit_wp: tuple[str, float, float] = ("wp_before", 0.1, 0.9),
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Validate + build the ``base`` (EPA pass/rush) and ``clean`` (``fit_wp`` band + hfa) frames."""
    df = pl.from_pandas(plays) if not isinstance(plays, pl.DataFrame) else plays
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"cfb_adjusted_epa: plays is missing required columns {missing}")
    base = df.filter(pl.col("EPA").is_not_null() & ((pl.col("pass") == 1) | (pl.col("rush") == 1))).with_columns(
        pos_team_id=pl.col("pos_team_id").cast(pl.Utf8),
        def_pos_team_id=pl.col("def_pos_team_id").cast(pl.Utf8),
        game_id=pl.col("game_id").cast(pl.Utf8),
    )
    wp_col, lo, hi = fit_wp
    clean = base.filter(pl.col(wp_col).is_between(lo, hi)).with_columns(
        hfa=pl.when(pl.col("neutral_site") == True)  # noqa: E712
        .then(pl.lit(0))
        .when(pl.col("pos_team") == pl.col("home"))
        .then(pl.lit(1))
        .otherwise(pl.lit(-1))
    )
    return base, clean


def _fit_opponent_ridge(clean: pl.DataFrame, ridge_lambda: float) -> tuple[pl.DataFrame, pl.DataFrame, float]:
    """Fit the offense/defense ridge on competitive plays -> (offense, defense, intercept).

    ``offense``/``defense`` carry one row per team. ``model.matrix`` drops the first
    factor level from the DESIGN, but that team's effect is 0 by construction, so it
    is emitted at the intercept rather than omitted; ``intercept`` is the league
    baseline used as the fallback strength for not-yet-seen teams in the
    walk-forward variant.

    Thin wrapper (T7.2): the pure ridge solve moved verbatim to
    :func:`sportsdataverse._common.ratings.dropped_level_ridge`; this name
    stays so existing internal imports (``cfb_ratings.py``,
    ``cfb_adjusted_epa`` call sites) keep working unchanged.
    """
    from sportsdataverse._common.ratings import dropped_level_ridge

    return dropped_level_ridge(clean, ridge_lambda)


def _fit_team_strengths(clean: pl.DataFrame, ridge_lambda: float) -> tuple[pl.DataFrame, pl.DataFrame, float]:
    """Offense/defense strengths shrunk toward the league average by each team's own sample.

    Every team gets its own offense and defense indicator (no reference level) and
    the same penalty ``ridge_lambda * _TEAM_SEASON_PLAYS`` in plays, so a team with
    ``n`` fit-band plays keeps about ``n / (n + penalty)`` of its signal and the
    intercept is the average team. Replaces :func:`_fit_opponent_ridge` for
    adjusted EPA, where the standardized fit shrank every team by the same ~3% and
    pinned the first team id (as a string) to the intercept on both sides.
    ``cfb_ratings`` still uses :func:`_fit_opponent_ridge`. Same return shape.
    """
    from sportsdataverse._common.ratings import opponent_adjusted_ridge

    if ridge_lambda <= 0:
        raise ValueError(f"ridge_lambda must be > 0, got {ridge_lambda}")
    fit, intercept, _hfa = opponent_adjusted_ridge(
        clean.filter(pl.col("pos_team_id").is_not_null() & pl.col("def_pos_team_id").is_not_null()),
        off_col="pos_team_id",
        def_col="def_pos_team_id",
        home_col="home",
        resp_col="EPA",
        lam=ridge_lambda * _TEAM_SEASON_PLAYS,
        hfa_col="hfa",
    )
    offense = fit.select("team_id", adjmodelOff=pl.col("off_coef") + intercept)
    defense = fit.select("team_id", adjmodelDef=pl.col("def_coef") + intercept)
    return offense, defense, intercept


def _adjust_games(
    base: pl.DataFrame,
    offense: pl.DataFrame,
    defense: pl.DataFrame,
    *,
    fill_strength: float | None,
) -> pl.DataFrame:
    """Per-(game, pos_team) raw + opponent-adjusted EPA from given strength tables.

    ``fill_strength`` (the league baseline) replaces missing opponent strengths
    when set -- used by the walk-forward variant so not-yet-seen opponents shrink
    to average; the season variant passes ``None`` (reference-team opponents stay
    null and are excluded by the valid-games filter).
    """
    off_aggs = [pl.col("pos_team").last().alias("pos_team"), pl.col("EPA").mean().alias("raw_off_epa")]
    if "week" in base.columns:
        off_aggs.append(pl.col("week").first().alias("week"))
    off_game = (
        base.group_by(["game_id", "pos_team_id", "def_pos_team_id"])
        .agg(off_aggs)
        .join(defense, left_on="def_pos_team_id", right_on="team_id", how="left")
    )
    def_game = (
        base.group_by(["game_id", "def_pos_team_id", "pos_team_id"])
        .agg(raw_def_epa=pl.col("EPA").mean())
        .join(offense, left_on="pos_team_id", right_on="team_id", how="left")
        .select("game_id", "def_pos_team_id", "raw_def_epa", "adjmodelOff")
    )
    opp = off_game.join(
        def_game,
        left_on=["game_id", "pos_team_id"],
        right_on=["game_id", "def_pos_team_id"],
        how="left",
    )
    if fill_strength is not None:
        opp = opp.with_columns(
            pl.col("adjmodelDef").fill_null(fill_strength),
            pl.col("adjmodelOff").fill_null(fill_strength),
        )
    return opp.with_columns(
        adj_off_epa=pl.col("raw_off_epa") - pl.col("adjmodelDef"),
        adj_def_epa=pl.col("raw_def_epa") - pl.col("adjmodelOff"),
    )


@overload
def cfb_adjusted_epa(
    plays: pl.DataFrame | pd.DataFrame, *, ridge_lambda: float = ..., return_as_pandas: Literal[False] = ...
) -> pl.DataFrame: ...


@overload
def cfb_adjusted_epa(
    plays: pl.DataFrame | pd.DataFrame, *, ridge_lambda: float = ..., return_as_pandas: Literal[True]
) -> pd.DataFrame: ...


def cfb_adjusted_epa(
    plays: pl.DataFrame | pd.DataFrame,
    *,
    ridge_lambda: float = _RIDGE_LAMBDA,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Season opponent-adjusted per-team EPA from a season's play-by-play.

    Fits one ridge of per-play ``EPA`` on offense-team, defense-team, and
    home-field indicators (every team shrunk toward the league average by its own
    play count) over the ``0.05 <= wp_before_naive <= 0.95`` pass
    and rush plays, nets each team's per-game raw EPA against the opponent's
    fitted strength, and averages to a season figure. In-sample/descriptive (the
    fit uses the whole season); for leak-free per-game values use
    :func:`cfb_adjusted_epa_by_game`.

    Args:
        plays: A cfbfastR-schema play-by-play frame (polars or pandas) with the
            columns listed in the module docstring. One season at a time.
        ridge_lambda: Ridge penalty per play of a full team season: each team
            is shrunk toward the league average by ``n / (n + ridge_lambda *
            577)`` for its ``n`` fit plays (~3% at a full season, most
            of the way on a handful). Must be > 0. Default 0.035, tuned across
            2021-2025 vs ESPN FPI.
        return_as_pandas: Return a pandas ``DataFrame`` instead of polars.

    Returns:
        One row per team (>= 2 valid games): ``team_id``, ``pos_team``,
        ``valid_games``, ``adj_off_epa``, ``adj_def_epa``, ``off_strength_faced``,
        ``def_strength_faced``, ``net_adj_epa`` and their ``*_rank`` columns.

    Raises:
        KeyError: If ``plays`` is missing a required column.
        ValueError: If ``ridge_lambda`` is not positive.

    Example:
        Quick start::

            import sportsdataverse.cfb as cfb
            pbp = cfb.load_cfb_pbp(seasons=[2023])
            cfb.cfb_adjusted_epa(pbp).sort("net_adj_epa_rank").head()

    See Also:
        * `cfbfastR`_ -- the R implementation this ports (``adjust_epa``).

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    base, clean = _prepare(plays, _ADJ_REQUIRED, _FIT_WP)
    offense, defense, _ = _fit_team_strengths(clean, ridge_lambda)
    opp = _adjust_games(base, offense, defense, fill_strength=None)
    team = (
        opp.group_by("pos_team_id")
        .agg(
            pos_team=pl.col("pos_team").last(),
            valid_games=(pl.col("adj_off_epa").is_not_null() & pl.col("adj_def_epa").is_not_null()).sum(),
            adj_off_epa=pl.col("adj_off_epa").mean(),
            adj_def_epa=pl.col("adj_def_epa").mean(),
            off_strength_faced=pl.col("adjmodelOff").mean(),
            def_strength_faced=pl.col("adjmodelDef").mean(),
        )
        .filter(
            pl.col("adj_off_epa").is_not_null() & pl.col("adj_def_epa").is_not_null() & (pl.col("valid_games") >= 2)
        )
        .with_columns(net_adj_epa=pl.col("adj_off_epa") - pl.col("adj_def_epa"))
        .sort("pos_team_id")
    )
    out = team.with_columns(
        adj_off_epa_rank=_rank("adj_off_epa", descending=True),
        adj_def_epa_rank=_rank("adj_def_epa", descending=False),
        net_adj_epa_rank=_rank("net_adj_epa", descending=True),
    ).rename({"pos_team_id": "team_id"})
    return out.to_pandas() if return_as_pandas else out


@overload
def cfb_adjusted_epa_by_game(
    plays: pl.DataFrame | pd.DataFrame, *, ridge_lambda: float = ..., return_as_pandas: Literal[False] = ...
) -> pl.DataFrame: ...


@overload
def cfb_adjusted_epa_by_game(
    plays: pl.DataFrame | pd.DataFrame, *, ridge_lambda: float = ..., return_as_pandas: Literal[True]
) -> pd.DataFrame: ...


def cfb_adjusted_epa_by_game(
    plays: pl.DataFrame | pd.DataFrame,
    *,
    ridge_lambda: float = _RIDGE_LAMBDA,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Walk-forward (point-in-time) opponent-adjusted EPA, one row per team-game.

    For each week ``w`` the opponent-strength ridge is fit on ``_FIT_WP``-band plays
    from **weeks before ``w`` only**, then that week's games are adjusted with
    those as-of strengths -- so the value uses no future information and is valid
    as an in-season power-rating / model feature. Week 1 (no prior) yields null
    adjustments; not-yet-seen opponents fall back to the league baseline (an
    average team), and teams seen on few plays are shrunk most of the way there.

    Args:
        plays: A cfbfastR-schema play-by-play frame (polars or pandas) with the
            module-docstring columns **plus** ``week``. One season at a time.
        ridge_lambda: Ridge penalty per play of a full team season: each team
            is shrunk toward the league average by ``n / (n + ridge_lambda *
            577)`` for its ``n`` fit plays (~3% at a full season, most
            of the way on a handful). Must be > 0. Default 0.035, tuned across
            2021-2025 vs ESPN FPI.
        return_as_pandas: Return a pandas ``DataFrame`` instead of polars.

    Returns:
        One row per (game, team), sorted by ``week`` then ``team_id``:
        ``game_id``, ``week``, ``team_id``, ``opponent_id``, ``pos_team``,
        ``raw_off_epa``, ``adj_off_epa``, ``raw_def_epa``, ``adj_def_epa``,
        ``off_strength_faced`` (opponent offense), ``def_strength_faced``
        (opponent defense), ``net_adj_epa``. The ``adj_*`` / ``net`` columns are
        null for week 1 (and any week with no prior fit).

    Raises:
        KeyError: If ``plays`` is missing a required column (incl. ``week``).
        ValueError: If ``ridge_lambda`` is not positive.

    Example:
        Quick start::

            import sportsdataverse.cfb as cfb
            pbp = cfb.load_cfb_pbp(seasons=[2023])
            tg = cfb.cfb_adjusted_epa_by_game(pbp)
            tg.filter(pl.col("week") >= 5).sort("net_adj_epa", descending=True).head()

    See Also:
        * `cfbfastR`_ -- the season implementation this extends (``adjust_epa``).

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    base, clean = _prepare(plays, _ADJ_BY_GAME_REQUIRED, _FIT_WP)
    weeks = sorted(base.filter(pl.col("week").is_not_null())["week"].unique().to_list())

    parts: list[pl.DataFrame] = []
    for week in weeks:
        prior = clean.filter(pl.col("week") < week)
        if prior.height > 0 and prior["pos_team_id"].n_unique() >= 2 and prior["def_pos_team_id"].n_unique() >= 2:
            offense, defense, intercept = _fit_team_strengths(prior, ridge_lambda)
        else:
            offense, defense, intercept = _EMPTY_OFFENSE, _EMPTY_DEFENSE, None
        wk = base.filter(pl.col("week") == week)
        parts.append(_adjust_games(wk, offense, defense, fill_strength=intercept))

    if parts:
        out = pl.concat(parts, how="vertical_relaxed")
    else:
        out = _adjust_games(base.head(0), _EMPTY_OFFENSE, _EMPTY_DEFENSE, fill_strength=None)

    out = (
        out.with_columns(net_adj_epa=pl.col("adj_off_epa") - pl.col("adj_def_epa"))
        .rename(
            {
                "pos_team_id": "team_id",
                "def_pos_team_id": "opponent_id",
                "adjmodelDef": "def_strength_faced",
                "adjmodelOff": "off_strength_faced",
            }
        )
        .select(
            "game_id",
            "week",
            "team_id",
            "opponent_id",
            "pos_team",
            "raw_off_epa",
            "adj_off_epa",
            "raw_def_epa",
            "adj_def_epa",
            "off_strength_faced",
            "def_strength_faced",
            "net_adj_epa",
        )
        .sort(["week", "team_id"])
    )
    return out.to_pandas() if return_as_pandas else out
