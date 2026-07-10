"""MLB team-runs + strikeout prop projections (T6.4, model ⑤).

Closed-form log5-style run-rate blend plus a Poisson tail for
over/under probabilities. Built on the same as-of-date team rate
aggregates :func:`sportsdataverse.mlb.mlb_team_projection.mlb_pythagenpat_table`
produces.

**Deferred piece:** the strikeout projection needs a team K/9 +
opponent K-rate collector against statsapi team pitching stats, which
does not exist yet in this spine (only game-level runs are captured).
:func:`mlb_prop_strikeouts` and :func:`prop_over_prob` are fully
implemented closed forms; :func:`mlb_props` only populates the
strikeout columns when the caller's ``ratings`` frame carries ``k9``
and ``k_rate`` (documented capture contract below) -- otherwise they
are null. The runs projection is fully real/production today: its
inputs (``off_rpg``/``def_rpg``) come straight from
:func:`mlb_pythagenpat_table`.

See Also:
    * `baseballr`_ -- R sibling package for MLB sabermetrics.

    .. _baseballr: https://baseballr.sportsdataverse.org
"""

from __future__ import annotations

import math
from typing import Union

import pandas as pd
import polars as pl
from scipy.stats import poisson

_PROPS_SCHEMA = {
    "game_id": pl.Utf8,
    "home_team_id": pl.Utf8,
    "away_team_id": pl.Utf8,
    "exp_runs_home": pl.Float64,
    "exp_runs_away": pl.Float64,
    "exp_strikeouts_home": pl.Float64,
    "exp_strikeouts_away": pl.Float64,
}


def mlb_prop_team_runs(home_off: float, away_def: float, lg_rpg: float, *, park_factor: float = 1.0) -> float:
    """Expected team runs via a log5-style rate blend.

    ``lg_rpg * (home_off / lg_rpg) * (away_def / lg_rpg) * park_factor``.

    Args:
        home_off: Team's own runs-scored-per-game rate.
        away_def: Opponent's runs-allowed-per-game rate.
        lg_rpg: League-average runs-per-game rate.
        park_factor: Park run-scoring multiplier (default neutral 1.0;
            a real park-factor table is a documented follow-on).

    Returns:
        float: expected runs for the team in this matchup.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_prop_projection import mlb_prop_team_runs
            mlb_prop_team_runs(5.5, 5.0, 4.5)
    """
    if lg_rpg == 0:
        return 0.0
    return lg_rpg * (home_off / lg_rpg) * (away_def / lg_rpg) * park_factor


def mlb_prop_strikeouts(team_k9: float, opp_k_rate: float, lg_k_rate: float, *, innings: float = 9.0) -> float:
    """Expected pitcher/team strikeouts via a K/9-and-opponent-K-rate blend.

    ``team_k9 / 9 * innings * (opp_k_rate / lg_k_rate)``.

    Args:
        team_k9: Team/pitcher strikeouts per 9 innings pitched.
        opp_k_rate: Opponent's own strikeout rate (K per PA).
        lg_k_rate: League-average strikeout rate.
        innings: Innings pitched in this outing (default 9.0).

    Returns:
        float: expected strikeouts.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_prop_projection import mlb_prop_strikeouts
            mlb_prop_strikeouts(9.0, 0.22, 0.22)
    """
    if lg_k_rate == 0:
        return 0.0
    return team_k9 / 9 * innings * (opp_k_rate / lg_k_rate)


def prop_over_prob(line: float, expected: float) -> float:
    """P(realized count > line) under a Poisson(expected) model.

    ``1 - poisson.cdf(floor(line), expected)``.

    Args:
        line: The prop betting line (e.g. 8.5 runs).
        expected: The Poisson mean (expected runs/strikeouts/etc.).

    Returns:
        float: P(over), in ``[0, 1]``.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_prop_projection import prop_over_prob
            prop_over_prob(3.5, 4.5)
    """
    return float(1.0 - poisson.cdf(math.floor(line), expected))


def mlb_props(
    matchups: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Expected team runs + strikeouts for a slate of matchups.

    Args:
        matchups: One row per game: ``game_id``, ``home_team_id``, ``away_team_id``.
        ratings: Per-team as-of-date rate table: ``team_id``, ``off_rpg``
            (runs scored/game), ``def_rpg`` (runs allowed/game), and
            optionally ``k9`` + ``k_rate`` (see the module docstring --
            strikeout columns are null without them). ``team_id`` must
            share a dtype with ``matchups``' team-id columns.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: one row per matchup.

        | Column | Type | Description |
        |---|---|---|
        | game_id | Utf8 | Game identifier |
        | home_team_id | Utf8 | Home team identifier |
        | away_team_id | Utf8 | Away team identifier |
        | exp_runs_home | Float64 | Expected home-team runs |
        | exp_runs_away | Float64 | Expected away-team runs |
        | exp_strikeouts_home | Float64 | Expected home-pitcher strikeouts (null if ``ratings`` lacks k9/k_rate) |
        | exp_strikeouts_away | Float64 | Expected away-pitcher strikeouts (null if ``ratings`` lacks k9/k_rate) |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_prop_projection import mlb_props
            props = mlb_props(matchups, ratings)
    """
    if matchups is None or matchups.height == 0 or ratings is None or ratings.height == 0:
        out = pl.DataFrame(schema=_PROPS_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    assert matchups.schema["home_team_id"] == ratings.schema["team_id"], (
        f"team_id dtype mismatch: matchups.home_team_id={matchups.schema['home_team_id']} "
        f"ratings.team_id={ratings.schema['team_id']}"
    )
    has_k = "k9" in ratings.columns and "k_rate" in ratings.columns
    lg_rpg = float((ratings["off_rpg"].mean() + ratings["def_rpg"].mean()) / 2.0)
    lg_k_rate = float(ratings["k_rate"].mean()) if has_k else None

    home_r = ratings.rename({c: f"home_{c}" for c in ratings.columns if c != "team_id"}).rename(
        {"team_id": "home_team_id"}
    )
    away_r = ratings.rename({c: f"away_{c}" for c in ratings.columns if c != "team_id"}).rename(
        {"team_id": "away_team_id"}
    )
    joined = matchups.join(home_r, on="home_team_id", how="left").join(away_r, on="away_team_id", how="left")
    assert joined.height >= matchups.height, f"props join dropped rows: {joined.height} < {matchups.height}"

    joined = joined.with_columns(
        pl.struct(["home_off_rpg", "away_def_rpg"])
        .map_elements(
            lambda s: mlb_prop_team_runs(s["home_off_rpg"], s["away_def_rpg"], lg_rpg), return_dtype=pl.Float64
        )
        .alias("exp_runs_home"),
        pl.struct(["away_off_rpg", "home_def_rpg"])
        .map_elements(
            lambda s: mlb_prop_team_runs(s["away_off_rpg"], s["home_def_rpg"], lg_rpg), return_dtype=pl.Float64
        )
        .alias("exp_runs_away"),
    )
    if has_k:
        assert lg_k_rate is not None
        joined = joined.with_columns(
            pl.struct(["home_k9", "away_k_rate"])
            .map_elements(
                lambda s: mlb_prop_strikeouts(s["home_k9"], s["away_k_rate"], lg_k_rate), return_dtype=pl.Float64
            )
            .alias("exp_strikeouts_home"),
            pl.struct(["away_k9", "home_k_rate"])
            .map_elements(
                lambda s: mlb_prop_strikeouts(s["away_k9"], s["home_k_rate"], lg_k_rate), return_dtype=pl.Float64
            )
            .alias("exp_strikeouts_away"),
        )
    else:
        joined = joined.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("exp_strikeouts_home"),
            pl.lit(None, dtype=pl.Float64).alias("exp_strikeouts_away"),
        )
    out = joined.select(
        "game_id",
        "home_team_id",
        "away_team_id",
        "exp_runs_home",
        "exp_runs_away",
        "exp_strikeouts_home",
        "exp_strikeouts_away",
    )
    return out.to_pandas() if return_as_pandas else out
