"""MLB team projection: pythagenpat + Elo (T6.4, model ④).

Closed-form pythagenpat expected win% (Smyth-Patriot, run-environment
adaptive exponent) plus an as-of-date iterative Elo run-differential
rating (538 MLB-Elo-style seeds, refit against a real backtest -- see
``dev/mlb_game_state/fit_elo.py``).

See Also:
    * `baseballr`_ -- R sibling package for MLB sabermetrics.
    * FiveThirtyEight's MLB Elo methodology -- source of the Elo seed
      constants this module refits.

    .. _baseballr: https://baseballr.sportsdataverse.org
"""

from __future__ import annotations

from typing import List, Optional, Union

import pandas as pd
import polars as pl

from sportsdataverse.mlb.mlb_game_state_constants import ELO_HFA, ELO_INIT, ELO_K, PYTHAGENPAT_EXPONENT

_PYTHAG_TABLE_SCHEMA = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "runs_scored": pl.Int64,
    "runs_allowed": pl.Int64,
    "games": pl.Int64,
    "win_pct": pl.Float64,
    "pythag_win_pct": pl.Float64,
}
_ELO_SCHEMA = {
    "game_id": pl.Utf8,
    "date": pl.Date,
    "home_team_id": pl.Utf8,
    "away_team_id": pl.Utf8,
    "home_rating": pl.Float64,
    "away_rating": pl.Float64,
    "home_win_prob_elo": pl.Float64,
    "home_rating_post": pl.Float64,
    "away_rating_post": pl.Float64,
}
_PROJECTION_SCHEMA = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "win_pct": pl.Float64,
    "pythag_win_pct": pl.Float64,
    "rating": pl.Float64,
    "exp_margin": pl.Float64,
}
#: Elo-to-runs scale for exp_margin: a 400-point Elo gap ~ 10 runs of talent
#: over a season is the common informal 538 heuristic; kept as a documented,
#: freely revisitable constant (not fit against a run-differential target --
#: no such oracle exists in this spine's fixtures).
_ELO_RUNS_SCALE = 40.0


def mlb_pythagenpat(
    runs_scored: float,
    runs_allowed: float,
    games: int,
    *,
    exponent: float = PYTHAGENPAT_EXPONENT,
) -> float:
    """Pythagenpat expected win percentage (Smyth-Patriot, run-environment adaptive exponent).

    ``x = ((runs_scored + runs_allowed) / games) ** exponent``;
    ``win_pct = runs_scored**x / (runs_scored**x + runs_allowed**x)``.

    Args:
        runs_scored: Total runs scored.
        runs_allowed: Total runs allowed.
        games: Games played.
        exponent: Run-environment exponent (default the published 0.287).

    Returns:
        float: expected win percentage in ``[0, 1]``. Returns ``0.5`` when
        ``games == 0`` or ``runs_scored + runs_allowed == 0`` (guard against
        a zero-division/degenerate input).

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_team_projection import mlb_pythagenpat
            mlb_pythagenpat(800, 600, 162)
    """
    if games == 0 or (runs_scored + runs_allowed) == 0:
        return 0.5
    x = ((runs_scored + runs_allowed) / games) ** exponent
    rs_x, ra_x = runs_scored**x, runs_allowed**x
    if rs_x + ra_x == 0:
        return 0.5
    return float(rs_x / (rs_x + ra_x))


def mlb_pythagenpat_table(
    results: pl.DataFrame,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Per-(season, team) pythagenpat table from game-level results.

    This is a **same-window estimator**, not a forward-looking prediction:
    pythagenpat smooths a team's *already-known* run differential into an
    implied "true-talent" win rate over that same window (the classic
    Bill James validation is exactly "does the formula's win% track the
    actual win% over the same season"). To use it predictively for a
    future game, pre-filter ``results`` to games strictly before that date
    with :func:`sportsdataverse.mlb.mlb_game_state_constants.as_of_split`
    first -- this function does not do that filtering itself.

    Args:
        results: Game-level results (``season``, ``home_team_id``,
            ``away_team_id``, ``home_score``, ``away_score``).
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: one row per (season, team).

        | Column | Type | Description |
        |---|---|---|
        | season | Int64 | Season |
        | team_id | Utf8 | Team identifier |
        | runs_scored | Int64 | Total runs scored |
        | runs_allowed | Int64 | Total runs allowed |
        | games | Int64 | Games played |
        | win_pct | Float64 | Realized win percentage |
        | pythag_win_pct | Float64 | Pythagenpat expected win percentage |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_team_projection import mlb_pythagenpat_table
            table = mlb_pythagenpat_table(results)
    """
    if results is None or results.height == 0:
        out = pl.DataFrame(schema=_PYTHAG_TABLE_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    home_side = results.select(
        "season",
        pl.col("home_team_id").alias("team_id"),
        pl.col("home_score").alias("runs_scored"),
        pl.col("away_score").alias("runs_allowed"),
        (pl.col("home_score") > pl.col("away_score")).cast(pl.Int64).alias("won"),
    )
    away_side = results.select(
        "season",
        pl.col("away_team_id").alias("team_id"),
        pl.col("away_score").alias("runs_scored"),
        pl.col("home_score").alias("runs_allowed"),
        (pl.col("away_score") > pl.col("home_score")).cast(pl.Int64).alias("won"),
    )
    long = pl.concat([home_side, away_side], how="vertical")
    agg = long.group_by("season", "team_id").agg(
        pl.col("runs_scored").sum(),
        pl.col("runs_allowed").sum(),
        pl.len().alias("games"),
        pl.col("won").sum().alias("wins"),
    )
    agg = agg.with_columns(
        (pl.col("wins") / pl.col("games")).alias("win_pct"),
        pl.struct(["runs_scored", "runs_allowed", "games"])
        .map_elements(
            lambda s: mlb_pythagenpat(s["runs_scored"], s["runs_allowed"], s["games"]),
            return_dtype=pl.Float64,
        )
        .alias("pythag_win_pct"),
    )
    out = agg.select("season", "team_id", "runs_scored", "runs_allowed", "games", "win_pct", "pythag_win_pct").sort(
        "season", "team_id"
    )
    return out.to_pandas() if return_as_pandas else out


def mlb_team_elo(
    results: pl.DataFrame,
    *,
    k: float = ELO_K,
    hfa: float = ELO_HFA,
    init: float = ELO_INIT,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """As-of-date iterative Elo run-differential rating.

    Games are folded **in date order** (ties broken by ``game_id``); each
    team's rating updates only *after* its game is scored, so the
    ``home_rating``/``away_rating`` columns are strictly as-of-date (no
    leakage from later games). ``home_win_prob_elo`` uses the standard
    logistic Elo formula with a home-field-advantage offset.

    Args:
        results: Game-level results (``game_id``, ``date``,
            ``home_team_id``, ``away_team_id``, ``home_score``, ``away_score``).
        k: Elo K-factor (rating-update step size).
        hfa: Home-field-advantage Elo-point offset.
        init: Initial rating for a team with no prior games.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: one row per game, in date order.

        | Column | Type | Description |
        |---|---|---|
        | game_id | Utf8 | Game identifier |
        | date | Date | Game date |
        | home_team_id | Utf8 | Home team identifier |
        | away_team_id | Utf8 | Away team identifier |
        | home_rating | Float64 | Home team's rating **before** this game |
        | away_rating | Float64 | Away team's rating **before** this game |
        | home_win_prob_elo | Float64 | Elo-implied P(home wins) before this game |
        | home_rating_post | Float64 | Home team's rating **after** this game |
        | away_rating_post | Float64 | Away team's rating **after** this game |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_team_projection import mlb_team_elo
            elo = mlb_team_elo(results)

        Pipeline next step (one line)::

            elo.group_by("home_team_id").agg(pl.col("home_rating_post").last())
    """
    if results is None or results.height == 0:
        out = pl.DataFrame(schema=_ELO_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    ordered = results.sort(["date", "game_id"])
    ratings: dict = {}
    rows: List[dict] = []
    for row in ordered.to_dicts():
        home, away = row["home_team_id"], row["away_team_id"]
        r_home = ratings.get(home, init)
        r_away = ratings.get(away, init)
        expected_home = 1.0 / (1.0 + 10.0 ** (-((r_home + hfa) - r_away) / 400.0))
        actual_home = 1.0 if row["home_score"] > row["away_score"] else 0.0
        r_home_post = r_home + k * (actual_home - expected_home)
        r_away_post = r_away + k * ((1.0 - actual_home) - (1.0 - expected_home))
        ratings[home] = r_home_post
        ratings[away] = r_away_post
        rows.append(
            {
                "game_id": row["game_id"],
                "date": row["date"],
                "home_team_id": home,
                "away_team_id": away,
                "home_rating": r_home,
                "away_rating": r_away,
                "home_win_prob_elo": expected_home,
                "home_rating_post": r_home_post,
                "away_rating_post": r_away_post,
            }
        )
    out = pl.DataFrame(rows, schema=_ELO_SCHEMA)
    return out.to_pandas() if return_as_pandas else out


def mlb_team_projection(
    seasons: Union[int, List[int], None] = None,
    *,
    results: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Combined pythagenpat + Elo team projection.

    Args:
        seasons: Reserved for a future network-collector path (currently
            unused -- pass ``results`` directly; see
            :func:`sportsdataverse.mlb.mlb_run_expectancy.mlb_run_expectancy_matrix`
            for the collector pattern this will follow once wired).
        results: Game-level results (see :func:`mlb_pythagenpat_table` and
            :func:`mlb_team_elo` for the required columns).
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: one row per (season, team).

        | Column | Type | Description |
        |---|---|---|
        | season | Int64 | Season |
        | team_id | Utf8 | Team identifier |
        | win_pct | Float64 | Realized win percentage |
        | pythag_win_pct | Float64 | Pythagenpat expected win percentage |
        | rating | Float64 | Final (as of the last observed game) Elo rating |
        | exp_margin | Float64 | Elo-implied expected run margin vs a league-average opponent |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_team_projection import mlb_team_projection
            projection = mlb_team_projection(results=results)
    """
    del seasons  # reserved; see docstring
    if results is None or results.height == 0:
        out = pl.DataFrame(schema=_PROJECTION_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    pythag = mlb_pythagenpat_table(results)
    elo = mlb_team_elo(results)

    home_final = elo.group_by("home_team_id").agg(
        pl.col("date").last().alias("last_date"), pl.col("home_rating_post").last().alias("rating")
    )
    away_final = elo.group_by("away_team_id").agg(
        pl.col("date").last().alias("last_date"), pl.col("away_rating_post").last().alias("rating")
    )
    both = pl.concat(
        [
            home_final.rename({"home_team_id": "team_id"}),
            away_final.rename({"away_team_id": "team_id"}),
        ],
        how="vertical",
    )
    final_rating = both.sort("last_date").group_by("team_id").agg(pl.col("rating").last().alias("rating"))

    assert pythag.schema["team_id"] == final_rating.schema["team_id"], (
        f"team_id dtype mismatch: pythag={pythag.schema['team_id']} elo={final_rating.schema['team_id']}"
    )
    out = pythag.join(final_rating, on="team_id", how="left").with_columns(
        pl.col("rating").fill_null(ELO_INIT).alias("rating"),
    )
    out = out.with_columns(((pl.col("rating") - ELO_INIT) / _ELO_RUNS_SCALE).alias("exp_margin"))
    out = out.select("season", "team_id", "win_pct", "pythag_win_pct", "rating", "exp_margin")
    return out.to_pandas() if return_as_pandas else out
