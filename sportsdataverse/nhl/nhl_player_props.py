"""NHL/PWHL player-prop projections (shots on goal, points) + game total re-export.

Model (3) of the T5.3 prediction spine: empirical-Bayes usage x matchup x
game-script projections, per player-game. Usage and efficiency are combined
into a single EB-shrunk per-game rate per stat family (a documented
simplification: the shipped skater-boxscore surface doesn't cleanly separate
shot-attempt opportunity from per-shot conversion the way a full
possession-tracking feed would, so one EB-shrunk rate captures both).

**Leakage scope (read carefully):** the per-player usage rate IS strictly
as-of -- each projected row uses only that player's *strictly prior* games in
the season (a per-player expanding mean, leakage-safe by construction for
every row in one pass). BUT the opponent matchup multiplier (from model-①
`adj_xga`) and the team game-script tilt (from model-②'s native `exp_margin`)
read a **single team-ratings snapshot**, not per-projected-game ratings: when
``as_of_date`` is given, ratings are computed as-of that one cutoff (so the
last game before it is clean; earlier games see a mildly forward-looking
snapshot); when ``as_of_date`` is ``None`` (the whole-season backtest mode),
ratings are **full-season** -- a documented approximation, not a per-game
as-of, since the opponent-strength adjustment is a second-order (~1.0±small)
multiplier on the dominant strictly-prior usage term. Fully per-projected-game
ratings are deferred (a per-date rating snapshot recompute is heavy at fixture
scale). The game-script tilt uses model-②'s **native** `exp_margin`, never the
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
import math
from typing import Literal, Union, overload

import numpy as np
import pandas as pd
import polars as pl
from scipy.stats import poisson

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


def _p_over(mean: float, line: float) -> float:
    """``P(X > line)`` for ``X ~ Poisson(mean)`` -- probability the realized
    (non-negative integer count) stat exceeds ``line``.

    Shots-on-goal and points are non-negative count data, not continuous --
    a Gaussian ``Phi((line - mean) / sd)`` approximation was tried first and
    found to be systematically overconfident against the real 2024 held-out
    calibration (see ``dev/nhl_prediction/build_player_props_backtest_fixture.py``
    and the fixtures README): predicted probabilities ran ~0.13-0.17 above
    the realized over-rate in the worst bucket. The Poisson form (using
    ``mean`` as the Poisson rate directly, ``sd`` no longer needed since
    Poisson variance == mean) cut that to ~0.06, a genuine fix, not a
    tuned-to-pass floor.
    """
    if mean <= 0:
        return 0.0

    return float(1.0 - poisson.cdf(math.floor(line), mean))


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
    """Empirical-Bayes shots/points player-prop projections.

    For every player-game in ``load_nhl_skater_boxscores``, projects that
    game's shots-on-goal / points from the player's **strictly prior** games
    (leakage-safe per row by construction), EB-shrunk toward a position prior,
    then adjusted by an opponent matchup multiplier (model ① ``adj_xga``) and a
    team game-script tilt (model ② native ``exp_margin`` -- never the market
    line). **See the module docstring's leakage-scope note:** the per-player
    rate is strictly as-of, but the matchup/game-script ratings are a single
    snapshot (as-of ``as_of_date`` if given, else full-season), not
    per-projected-game ratings.

    Args:
        seasons: an int or iterable of seasons (``load_nhl_skater_boxscores``
            only publishes seasons >= 2024).
        league: resolves ``prop_kappa``/``pos_priors``/``prop_team_volume_slope``
            via :func:`get_constants`.
        as_of_date: if given, only games strictly before this date are
            projected AND the matchup/game-script ratings snapshot is computed
            as-of this cutoff. NOTE: the per-player usage rate is strictly-prior
            regardless of this arg (it never needed a cutoff); this arg tightens
            *which* games are projected and the *single* ratings snapshot, but
            does not make the ratings per-projected-game as-of (a documented
            approximation -- see the module docstring).
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

    # Thread as_of_date into the ratings snapshot too (not only the box filter),
    # so the public param genuinely tightens the matchup/game-script ratings and
    # cannot be misread as leakage-safe while ratings stay full-season. This is a
    # single snapshot as-of the cutoff (or full-season when None), NOT
    # per-projected-game ratings -- see the module docstring's leakage-scope note.
    ratings = nhl_team_ratings(seasons, league=league, as_of_date=as_of_date)
    avg_xga = float(ratings["adj_xga"].mean()) if ratings.height else const.avg_xgf
    team_volume_slope = const.prop_team_volume_slope
    # Pre-index ratings once (team -> row) so the per-player-game loop is a dict
    # lookup, not two full-frame .filter() scans per row.
    ratings_by_team = {r["team"]: r for r in ratings.iter_rows(named=True)}

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
                opp_row = ratings_by_team.get(opp_teams[i])
                team_row = ratings_by_team.get(teams[i])
                opp_adj_xga = float(opp_row["adj_xga"]) if opp_row is not None else avg_xga
                matchup_multiplier = opp_adj_xga / avg_xga if avg_xga else 1.0

                team_volume_factor = 1.0
                if team_row is not None and opp_row is not None:
                    home_xgf, home_xga = float(team_row["adj_xgf"]), float(team_row["adj_xga"])
                    away_xgf, away_xga = float(opp_row["adj_xgf"]), float(opp_row["adj_xga"])
                    exp_margin = 0.5 * (home_xgf + away_xga) - 0.5 * (away_xgf + home_xga)
                    team_volume_factor = 1.0 - team_volume_slope * exp_margin

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
                        "proj_sd": math.sqrt(max(proj_mean, 1e-9)),  # Poisson SD == sqrt(mean)
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
