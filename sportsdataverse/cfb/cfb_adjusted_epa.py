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
  opponents fall back to the league baseline, which with the heavy ridge penalty
  is the intended early-season shrinkage toward average).

Neither is a bundled model artifact (unlike the EP/WP/QBR ``.ubj`` files): the
ridge is fit *in-sample on team dummies*, so the coefficients *are* that window's
team strengths -- nothing to persist or apply to a different season. Faithful
port of cfbfastR's ``adjust_epa`` / ``cfbfastR-cfb-data`` ``espn_cfb_15``.

Required input columns (a cfbfastR-schema pbp frame): ``game_id``, ``pos_team``,
``pos_team_id``, ``def_pos_team_id``, ``home``, ``neutral_site``, ``EPA``,
``pass``, ``rush``, ``wp_before`` (plus ``week`` for the by-game variant).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, overload

import numpy as np
import polars as pl

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["cfb_adjusted_epa", "cfb_adjusted_epa_by_game"]

# Ridge penalty: cfbfastR uses glmnet at the grid's largest lambda (cv$lambda[[1]]).
_RIDGE_LAMBDA = 325.0

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

_EMPTY_OFFENSE = pl.DataFrame(schema={"team_id": pl.Utf8, "adjmodelOff": pl.Float64})
_EMPTY_DEFENSE = pl.DataFrame(schema={"team_id": pl.Utf8, "adjmodelDef": pl.Float64})


def _rank(col: str, *, descending: bool) -> pl.Expr:
    """R ``rank()`` -- average ties, ``na.last=TRUE`` (NA rows get trailing ranks)."""
    c = pl.col(col)
    base = c.rank(method="average", descending=descending)
    n_nonnull = c.is_not_null().sum()
    null_trail = (n_nonnull + c.is_null().cum_sum()).cast(pl.Float64)
    return pl.when(c.is_null()).then(null_trail).otherwise(base)


def _prepare(plays: pl.DataFrame | pd.DataFrame, required: tuple[str, ...]) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Validate + build the ``base`` (EPA pass/rush) and ``clean`` (competitive + hfa) frames."""
    df = pl.from_pandas(plays) if not isinstance(plays, pl.DataFrame) else plays
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"cfb_adjusted_epa: plays is missing required columns {missing}")
    base = df.filter(pl.col("EPA").is_not_null() & ((pl.col("pass") == 1) | (pl.col("rush") == 1))).with_columns(
        pos_team_id=pl.col("pos_team_id").cast(pl.Utf8),
        def_pos_team_id=pl.col("def_pos_team_id").cast(pl.Utf8),
        game_id=pl.col("game_id").cast(pl.Utf8),
    )
    clean = base.filter((pl.col("wp_before") >= 0.1) & (pl.col("wp_before") <= 0.9)).with_columns(
        hfa=pl.when(pl.col("neutral_site") == True)  # noqa: E712
        .then(pl.lit(0))
        .when(pl.col("pos_team") == pl.col("home"))
        .then(pl.lit(1))
        .otherwise(pl.lit(-1))
    )
    return base, clean


def _fit_opponent_ridge(clean: pl.DataFrame, ridge_lambda: float) -> tuple[pl.DataFrame, pl.DataFrame, float]:
    """Fit the offense/defense ridge on competitive plays -> (offense, defense, intercept).

    ``offense``/``defense`` carry one row per *non-reference* team (``model.matrix``
    drops the first factor level); ``intercept`` is the league baseline used as the
    fallback strength for not-yet-seen teams in the walk-forward variant.
    """
    try:
        from sklearn.linear_model import Ridge
    except ImportError as exc:  # pragma: no cover - optional dep guidance
        raise ImportError(
            "cfb_adjusted_epa requires scikit-learn. Install it with "
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
    offense = pl.DataFrame(
        {"team_id": off_dummy, "adjmodelOff": [coef[f"pos_team_id{t}"] + intercept for t in off_dummy]}
    )
    defense = pl.DataFrame(
        {"team_id": def_dummy, "adjmodelDef": [coef[f"def_pos_team_id{t}"] + intercept for t in def_dummy]}
    )
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
    home-field indicators over the competitive (``0.1 <= wp_before <= 0.9``) pass
    and rush plays, nets each team's per-game raw EPA against the opponent's
    fitted strength, and averages to a season figure. In-sample/descriptive (the
    fit uses the whole season); for leak-free per-game values use
    :func:`cfb_adjusted_epa_by_game`.

    Args:
        plays: A cfbfastR-schema play-by-play frame (polars or pandas) with the
            columns listed in the module docstring. One season at a time.
        ridge_lambda: Ridge penalty (glmnet-scale; default 325).
        return_as_pandas: Return a pandas ``DataFrame`` instead of polars.

    Returns:
        One row per team (>= 2 valid games): ``team_id``, ``pos_team``,
        ``valid_games``, ``adj_off_epa``, ``adj_def_epa``, ``off_strength_faced``,
        ``def_strength_faced``, ``net_adj_epa`` and their ``*_rank`` columns.

    Raises:
        ImportError: If ``scikit-learn`` is not installed.
        KeyError: If ``plays`` is missing a required column.

    Example:
        Quick start::

            import sportsdataverse.cfb as cfb
            pbp = cfb.load_cfb_pbp(seasons=[2023])
            cfb.cfb_adjusted_epa(pbp).sort("net_adj_epa_rank").head()

    See Also:
        * `cfbfastR`_ -- the R implementation this ports (``adjust_epa``).

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    base, clean = _prepare(plays, _REQUIRED_COLUMNS)
    offense, defense, _ = _fit_opponent_ridge(clean, ridge_lambda)
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

    For each week ``w`` the opponent-strength ridge is fit on competitive plays
    from **weeks before ``w`` only**, then that week's games are adjusted with
    those as-of strengths -- so the value uses no future information and is valid
    as an in-season power-rating / model feature. Week 1 (no prior) yields null
    adjustments; not-yet-seen opponents fall back to the league baseline (with the
    heavy ridge penalty this is the intended early-season shrinkage to average).

    Args:
        plays: A cfbfastR-schema play-by-play frame (polars or pandas) with the
            module-docstring columns **plus** ``week``. One season at a time.
        ridge_lambda: Ridge penalty (glmnet-scale; default 325).
        return_as_pandas: Return a pandas ``DataFrame`` instead of polars.

    Returns:
        One row per (game, team), sorted by ``week`` then ``team_id``:
        ``game_id``, ``week``, ``team_id``, ``opponent_id``, ``pos_team``,
        ``raw_off_epa``, ``adj_off_epa``, ``raw_def_epa``, ``adj_def_epa``,
        ``off_strength_faced`` (opponent offense), ``def_strength_faced``
        (opponent defense), ``net_adj_epa``. The ``adj_*`` / ``net`` columns are
        null for week 1 (and any week with no prior fit).

    Raises:
        ImportError: If ``scikit-learn`` is not installed.
        KeyError: If ``plays`` is missing a required column (incl. ``week``).

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
    base, clean = _prepare(plays, _BY_GAME_REQUIRED)
    weeks = sorted(base.filter(pl.col("week").is_not_null())["week"].unique().to_list())

    parts: list[pl.DataFrame] = []
    for week in weeks:
        prior = clean.filter(pl.col("week") < week)
        if prior.height > 0 and prior["pos_team_id"].n_unique() >= 2 and prior["def_pos_team_id"].n_unique() >= 2:
            offense, defense, intercept = _fit_opponent_ridge(prior, ridge_lambda)
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
