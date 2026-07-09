"""NHL/PWHL player-prop projections (shots on goal, points) + game total re-export.

Model (3) of the T5.3 prediction spine: empirical-Bayes usage x matchup x
game-script projections, per player-game, **as-of** (only prior games feed a
projection -- the leakage boundary). Usage and efficiency are combined into a
single EB-shrunk per-game rate per stat family (a documented simplification:
the shipped skater-boxscore surface doesn't cleanly separate shot-attempt
opportunity from per-shot conversion the way a full possession-tracking feed
would, so one EB-shrunk rate captures both). The matchup multiplier comes
from the opponent's model-① `adj_xga` (as-of); the team-volume/game-script
tilt comes from model-②'s **native** `exp_margin` (favored teams protect
leads -> fewer late shots for; trailing teams push -> more), never the
market line -- keeping model ③ market-free like ①②.

``nhl_game_total`` is a thin re-export of ②'s expected-goals helper (DRY --
see :func:`sportsdataverse.nhl.nhl_market.predict_total`), satisfying the
brief's "props + total" grouping under one model without a second
implementation of the goals math.

Example:
    Quick start::

        from sportsdataverse.nhl.nhl_player_props import nhl_player_props

        props = nhl_player_props(2024, stats=("shots", "points"))
        print(props.filter(pl.col("stat") == "shots").head())

See Also:
    * `nhl-api-py`_ -- companion NHL Python client.

.. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
"""

from __future__ import annotations

import datetime as _dt
from typing import Literal, Union, overload

import numpy as np
import pandas as pd
import polars as pl
from scipy.stats import norm

from sportsdataverse.nhl.nhl_loaders import load_nhl_skater_boxscores
from sportsdataverse.nhl.nhl_prediction_constants import get_constants
from sportsdataverse.nhl.nhl_team_ratings import nhl_team_ratings

_PROPS_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "game_id": pl.Utf8,
    "player_id": pl.Utf8,
    "team": pl.Utf8,
    "opp_team": pl.Utf8,
    "stat": pl.Utf8,
    "proj_mean": pl.Float64,
    "proj_sd": pl.Float64,
    "p_over": pl.Float64,
    "line": pl.Float64,
}

_STAT_COLUMN = {"shots": "shots_on_goal", "points": "points"}

# Fitted per-stat residual SD (Task 4.2 overwrites via dev/nhl_prediction/fit_props.py).
_DEFAULT_PROJ_SD = {"shots": 1.6, "points": 0.9}

_TEAM_VOLUME_SLOPE = 0.04  # small documented tilt: favored teams protect leads (fewer shots).


def _eb_shrink(n: np.ndarray, rate: np.ndarray, prior: float, kappa: float) -> np.ndarray:
    """Empirical-Bayes shrinkage: ``(n*rate + kappa*prior) / (n + kappa)``.

    Args:
        n: games-played (or other sample-size) array.
        rate: the observed per-game rate array.
        prior: the position/league prior rate to shrink toward.
        kappa: shrinkage strength (games-equivalent prior weight).

    Returns:
        The shrunk rate array.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nhl.nhl_player_props import _eb_shrink
            _eb_shrink(np.array([1.0, 50.0]), np.array([5.0, 2.0]), prior=2.2, kappa=6.0)
    """
    return (n * rate + kappa * prior) / (n + kappa)


def _p_over(mean: float, line: float, sd: float) -> float:
    """``1 - Phi((line - mean) / sd)`` -- probability the realized stat exceeds ``line``."""
    if sd <= 0:
        return 0.5
    return float(1.0 - norm.cdf((line - mean) / sd))


def _position_bucket(position: str) -> str:
    return "D" if position == "D" else "F"


@overload
def nhl_player_props(
    seasons: Union[int, list[int]],
    *,
    league: str = ...,
    as_of_date: _dt.date | None = ...,
    stats: tuple[str, ...] = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def nhl_player_props(
    seasons: Union[int, list[int]],
    *,
    league: str = ...,
    as_of_date: _dt.date | None = ...,
    stats: tuple[str, ...] = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def nhl_player_props(
    seasons: Union[int, list[int]],
    *,
    league: str = "nhl",
    as_of_date: _dt.date | None = None,
    stats: tuple[str, ...] = ("shots", "points"),
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Empirical-Bayes shots/points player-prop projections, as-of each game.

    For every player-game in ``load_nhl_skater_boxscores``, projects that
    game's shots-on-goal / points using only the player's **strictly prior**
    games in the same season(s) (the leakage boundary), EB-shrunk toward a
    position prior, adjusted by the opponent's as-of matchup (model ①
    ``adj_xga``) and the team's own as-of game-script (model ② native
    ``exp_margin`` -- never the market line).

    Args:
        seasons: an int or iterable of seasons (``load_nhl_skater_boxscores``
            only publishes seasons >= 2024).
        league: resolves ``prop_kappa``/``pos_priors`` via :func:`get_constants`.
        as_of_date: if given, only games strictly before this date are
            projected (in addition to the per-player as-of-prior-games rule).
        stats: which stat families to project (``"shots"``, ``"points"``).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame, one row per (player, game, stat).
        Empty/malformed input returns a zero-row frame with the documented schema.

        |col_name  |type   |
        |:---------|:------|
        |season    |Int64  |
        |game_id   |String |
        |player_id |String |
        |team      |String |
        |opp_team  |String |
        |stat      |String |
        |proj_mean |Float64|
        |proj_sd   |Float64|
        |p_over    |Float64|
        |line      |Float64|

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_player_props import nhl_player_props

            props = nhl_player_props(2024, stats=("shots",))
            print(props.sort("proj_mean", descending=True).head())

        Pipeline next step (one line)::

            props.filter(pl.col("player_id") == "8478402")
    """
    box = load_nhl_skater_boxscores(seasons)
    if box.is_empty():
        return _empty_props(return_as_pandas)

    const = get_constants(league)
    box = box.with_columns(
        pl.col("game_id").cast(pl.Int64).cast(pl.Utf8),
        pl.col("player_id").cast(pl.Int64).cast(pl.Utf8),
        pl.col("game_date").str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("date"),
    ).filter(pl.col("position") != "G")

    if as_of_date is not None:
        box = box.filter(pl.col("date") < as_of_date)
    if box.is_empty():
        return _empty_props(return_as_pandas)

    # Opponent per game_id: the other team_abbrev present in the same game.
    teams_per_game = box.select("game_id", "team_abbrev").unique()
    opp = (
        teams_per_game.join(teams_per_game, on="game_id", suffix="_opp")
        .filter(pl.col("team_abbrev") != pl.col("team_abbrev_opp"))
        .rename({"team_abbrev": "team", "team_abbrev_opp": "opp_team"})
    )
    box = box.join(opp, left_on=["game_id", "team_abbrev"], right_on=["game_id", "team"], how="left")
    box = box.rename({"team_abbrev": "team"})

    ratings = nhl_team_ratings(seasons, league=league)
    avg_xga = float(ratings["adj_xga"].mean()) if ratings.height else const.avg_xgf

    rows = []
    for stat in stats:
        col = _STAT_COLUMN[stat]
        kappa = const.prop_kappa[stat]
        per_player = box.select("player_id", "team", "position", "date", "game_id", "opp_team", "season", col).sort(
            "player_id", "date"
        )
        for pid, sub in per_player.group_by("player_id", maintain_order=True):
            player_id = pid[0] if isinstance(pid, tuple) else pid
            sub = sub.sort("date")
            values = sub[col].to_numpy().astype(float)
            position = _position_bucket(sub["position"][0])
            prior = const.pos_priors[stat][position]
            n_prior = np.arange(len(values))  # games strictly before this row
            cumsum_prior = np.concatenate(([0.0], np.cumsum(values)[:-1])) if len(values) > 1 else np.array([0.0])
            avg_prior_rate = np.where(n_prior > 0, cumsum_prior / np.maximum(n_prior, 1), prior)
            shrunk = _eb_shrink(n_prior.astype(float), avg_prior_rate, prior, kappa)

            teams = sub["team"].to_list()
            opp_teams = sub["opp_team"].to_list()
            seasons_list = sub["season"].to_list()
            for i in range(len(values)):
                opp_row = ratings.filter((pl.col("team") == opp_teams[i]))
                team_row = ratings.filter((pl.col("team") == teams[i]))
                opp_adj_xga = float(opp_row["adj_xga"][0]) if opp_row.height else avg_xga
                matchup_multiplier = opp_adj_xga / avg_xga if avg_xga else 1.0

                team_volume_factor = 1.0
                if team_row.height and opp_row.height:
                    home_xgf, home_xga = float(team_row["adj_xgf"][0]), float(team_row["adj_xga"][0])
                    away_xgf, away_xga = float(opp_row["adj_xgf"][0]), float(opp_row["adj_xga"][0])
                    exp_margin = 0.5 * (home_xgf + away_xga) - 0.5 * (away_xgf + home_xga)
                    team_volume_factor = 1.0 - _TEAM_VOLUME_SLOPE * exp_margin

                proj_mean = shrunk[i] * matchup_multiplier * team_volume_factor
                rows.append(
                    {
                        "season": int(seasons_list[i]),
                        "game_id": sub["game_id"][i],
                        "player_id": player_id,
                        "team": teams[i],
                        "opp_team": opp_teams[i],
                        "stat": stat,
                        "proj_mean": float(proj_mean),
                        "proj_sd": _DEFAULT_PROJ_SD[stat],
                        "p_over": None,
                        "line": None,
                    }
                )

    if not rows:
        return _empty_props(return_as_pandas)
    out = pl.DataFrame(rows, schema=_PROPS_SCHEMA)
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out


@overload
def nhl_game_total(
    games: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    league: str = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def nhl_game_total(
    games: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    league: str = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def nhl_game_total(
    games: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    league: str = "nhl",
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Per-game expected total goals -- a thin re-export of model ②'s expected-goals helper.

    Satisfies the "props + total" grouping of model ③ (the brief's player-prop
    surface) without a second implementation of the goals math: this calls
    the exact same :func:`sportsdataverse.nhl.nhl_market.predict_total` that
    :func:`sportsdataverse.nhl.nhl_market.nhl_predict_games` uses internally.

    Args:
        games: a schedule-shaped frame with ``game_id``, ``home_team``,
            ``away_team``, ``neutral_site``.
        ratings: the output of :func:`sportsdataverse.nhl.nhl_team_ratings.nhl_team_ratings`.
        league: resolves HFA/sigma/total_scale via :func:`get_constants`.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame with ``game_id``, ``exp_total``.

        |col_name  |type   |
        |:---------|:------|
        |game_id   |String |
        |exp_total |Float64|

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_player_props import nhl_game_total
            nhl_game_total(games, ratings)
    """
    from sportsdataverse.nhl.nhl_market import nhl_predict_games

    preds = nhl_predict_games(games, ratings, league=league)
    out = preds.select("game_id", "exp_total")
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out


def _empty_props(return_as_pandas: bool) -> Union[pl.DataFrame, pd.DataFrame]:
    out = pl.DataFrame(schema=_PROPS_SCHEMA)
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out
