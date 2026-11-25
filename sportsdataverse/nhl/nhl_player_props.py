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
from collections.abc import Callable
from typing import Any, Literal, Union, overload

import numpy as np
import pandas as pd
import polars as pl
from scipy.stats import nbinom, poisson

from sportsdataverse._temporal import as_date
from sportsdataverse.nhl.nhl_loaders import load_nhl_goalie_boxscores, load_nhl_skater_boxscores
from sportsdataverse.nhl.nhl_prediction_constants import LeagueConstants, get_constants
from sportsdataverse.nhl.nhl_team_ratings import nhl_team_ratings

_PROPS_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "game_id": pl.Int64,
    "player_id": pl.Int64,
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
    calibration (see ``tools/models/nhl_prediction/build_player_props_backtest_fixture.py``
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
    load_skater_boxscores: Callable[[Any], pl.DataFrame] | None = ...,
    team_ratings: Callable[..., pl.DataFrame] | None = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def nhl_player_props(
    seasons: Union[int, list[int]],
    *,
    league: str = ...,
    as_of_date: _dt.date | None = ...,
    stats: tuple[str, ...] = ...,
    load_skater_boxscores: Callable[[Any], pl.DataFrame] | None = ...,
    team_ratings: Callable[..., pl.DataFrame] | None = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def nhl_player_props(
    seasons: Union[int, list[int]],
    *,
    league: str = "nhl",
    as_of_date: _dt.date | None = None,
    stats: tuple[str, ...] = ("shots", "points"),
    load_skater_boxscores: Callable[[Any], pl.DataFrame] | None = None,
    team_ratings: Callable[..., pl.DataFrame] | None = None,
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
        load_skater_boxscores: the per-player-game box loader, called as ``f(seasons)``;
            ``None`` reads ``load_nhl_skater_boxscores``. Another league binds its own loader
            here (columns ``game_id``, ``player_id``, ``game_date``, ``season``, ``position``,
            ``team_abbrev``, ``shots_on_goal``, ``points``).
        team_ratings: the matchup/game-script ratings builder, called as
            ``f(seasons, as_of_date=as_of_date)``; ``None`` runs :func:`nhl_team_ratings`
            with ``league``. Its ``team`` values must match the box's ``team_abbrev``.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame, one row per (player, game, stat).
        Empty/malformed input returns a zero-row frame with the documented schema.

        |col_name  |type   |
        |:---------|:------|
        |season    |Int64  |
        |game_id   |Int64  |
        |player_id |Int64  |
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

            props.filter(pl.col("player_id") == 8478402)
    """
    box = (load_skater_boxscores or load_nhl_skater_boxscores)(seasons)
    if box.is_empty():
        return _empty_props(return_as_pandas)

    const = get_constants(league)
    box = box.with_columns(
        # Int64 like every NHL parser id (task 5.2, R986-3), so props join parse_nhl_web_* / EDGE frames
        pl.col("game_id").cast(pl.Int64),
        pl.col("player_id").cast(pl.Int64),
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
    if team_ratings is None:
        ratings = nhl_team_ratings(seasons, league=league, as_of_date=as_of_date)
    else:
        ratings = team_ratings(seasons, as_of_date=as_of_date)
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


_SAVES_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "game_id": pl.Int64,
    "game_date": pl.Date,
    "player_id": pl.Int64,
    "team": pl.Utf8,
    "opp_team": pl.Utf8,
    "is_home": pl.Boolean,
    "exp_shots_against": pl.Float64,
    "exp_save_pct": pl.Float64,
    "exp_saves": pl.Float64,
    "saves_lo": pl.Int64,
    "saves_hi": pl.Int64,
    "coverage": pl.Float64,
}


def _goalie_rows(box: pl.DataFrame, starters: pl.DataFrame | None) -> pl.DataFrame:
    """Goalie-game rows (history, plus ``starters`` as games still to play), with ``target`` marking the projected ones.

    ``season`` comes from the NHL game id (``2025020001`` -> ``20252026``), so a future game needs no season column.
    A ``starters`` game already in ``box`` is dropped from the history, so its own result never reaches it.

    Raises:
        ValueError: if a ``starters`` game does not list exactly two teams.
    """
    if starters is not None:
        teams = starters.group_by("game_id").agg(pl.col("team").n_unique().alias("n"))
        if teams.filter(pl.col("n") != 2).height:
            raise ValueError("starters must list both teams of every game (one starter per team)")
        box = box.join(starters.select(pl.col("game_id").cast(box.schema["game_id"])), on="game_id", how="anti")
    rows = box.select(
        pl.col("game_id").cast(pl.Int64),
        as_date(pl.col("game_date")).alias("date"),
        pl.col("team_abbrev").alias("team"),
        (pl.col("home_away") == "home").alias("is_home"),
        pl.col("player_id").cast(pl.Int64),
        pl.col("shots_against").cast(pl.Float64),
        pl.col("saves").cast(pl.Float64),
        (pl.col("starter") == True).alias("target") if starters is None else pl.lit(False).alias("target"),  # noqa: E712
    )
    if starters is not None:
        pending = starters.select(
            pl.col("game_id").cast(pl.Int64),
            as_date(pl.col("game_date")).alias("date"),
            pl.col("team").cast(pl.Utf8),
            pl.col("is_home").cast(pl.Boolean),
            pl.col("player_id").cast(pl.Int64),
            pl.lit(None, dtype=pl.Float64).alias("shots_against"),
            pl.lit(None, dtype=pl.Float64).alias("saves"),
            pl.lit(True).alias("target"),
        )
        rows = pl.concat([rows, pending])
    return rows.with_columns(((pl.col("game_id") // 1_000_000) * 10001 + 1).alias("season"))


def _prior_sum(col: str, *by: str) -> pl.Expr:
    """Running total of ``col`` over rows strictly before this one (nulls count as 0)."""
    x = pl.col(col).fill_null(0.0)
    return (x.cum_sum() - x).over(*by)


def _team_shot_rates(rows: pl.DataFrame, const: LeagueConstants) -> pl.DataFrame:
    """Per team-game, strictly-prior in-season shots against / for per game, shrunk toward the league's in-season mean.

    Returns ``game_id, team, opp_team, sa_rate, opp_sf_rate`` for every team-game in ``rows``.
    """
    team = rows.group_by("season", "game_id", "date", "team").agg(
        pl.when(pl.col("shots_against").is_not_null().any()).then(pl.col("shots_against").sum()).alias("sa")
    )
    opp = team.select("game_id", pl.col("team").alias("opp_team"), pl.col("sa").alias("sf"))
    team = team.join(opp, on="game_id").filter(pl.col("team") != pl.col("opp_team"))
    team = team.with_columns(pl.col("sa").is_not_null().cast(pl.Float64).alias("played"))
    # League shots per team-game over the season's earlier days (an unplayed game adds nothing).
    daily = (
        team.group_by("season", "date")
        .agg(pl.col("sa").fill_null(0.0).sum().alias("day_sa"), pl.col("played").sum().alias("day_n"))
        .sort("date")
        .with_columns(_prior_sum("day_sa", "season").alias("lg_sa"), _prior_sum("day_n", "season").alias("lg_n"))
    )
    k_lg, k_team = const.league_shots_k, const.shrink_k
    lg = (pl.col("lg_sa") + k_lg * const.avg_shots_against) / (pl.col("lg_n") + k_lg)
    daily = daily.select("season", "date", lg.alias("lg"))
    team = team.join(daily, on=["season", "date"]).sort("team", "date")
    team = team.with_columns(
        _prior_sum("played", "season", "team").alias("n"),
        _prior_sum("sa", "season", "team").alias("sa_cum"),
        _prior_sum("sf", "season", "team").alias("sf_cum"),
    ).with_columns(
        ((pl.col("sa_cum") + k_team * pl.col("lg")) / (pl.col("n") + k_team)).alias("sa_rate"),
        ((pl.col("sf_cum") + k_team * pl.col("lg")) / (pl.col("n") + k_team)).alias("sf_rate"),
    )
    opp_rates = team.select("game_id", pl.col("team").alias("opp_team"), pl.col("sf_rate").alias("opp_sf_rate"))
    return team.join(opp_rates, on=["game_id", "opp_team"]).select(
        "game_id", "team", "opp_team", "sa_rate", "opp_sf_rate"
    )


def nhl_goalie_saves(
    seasons: int | list[int],
    *,
    starters: pl.DataFrame | None = None,
    coverage: float = 0.8,
    load_goalie_boxscores: Callable[[Any], pl.DataFrame] | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Starting goalie saves projection with a central interval, per goalie-game.

    Expected shots against blend the goalie's team's shots-against rate with the opponent's shots-for rate
    (strictly prior games of the season, shrunk toward the league's in-season mean by ``shrink_k`` games),
    move by ``shots_hfa / 2`` for home ice and scale by ``starter_shot_share``. The save percentage is the
    goalie's own record over every earlier game in ``seasons``, shrunk toward ``save_pct_prior`` by
    ``save_pct_kappa`` shots. Saves then follow a negative binomial (size ``shots_nb_size``, mean
    ``exp_shots_against * exp_save_pct``), and ``saves_lo`` / ``saves_hi`` are its ``(1 - coverage) / 2`` and
    ``(1 + coverage) / 2`` quantiles. Every row uses only games before its date.

    Args:
        seasons: the seasons whose goalie boxscores make the history (``load_nhl_goalie_boxscores``
            publishes 2024 onward). Without ``starters``, every starter-game in them is projected.
        starters: games still to play, one row per projected starter: ``game_id``, ``game_date``
            (``Date`` or ``"YYYY-MM-DD"``), ``team``, ``is_home``, ``player_id``. Both teams of a game
            must be listed. A game already in the boxscores is projected without its own result. When
            given, only these rows are returned.
        coverage: central probability of the interval, in (0, 1).
        load_goalie_boxscores: the goalie-box loader, called as ``f(seasons)``; ``None`` reads
            :func:`~sportsdataverse.nhl.nhl_loaders.load_nhl_goalie_boxscores`.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame, one row per projected goalie-game. Because saves are whole numbers,
        the interval holds at least ``coverage`` of the predicted distribution. Empty input returns a
        zero-row frame with this schema.

        |col_name          |type   |
        |:-----------------|:------|
        |season            |Int64  |
        |game_id           |Int64  |
        |game_date         |Date   |
        |player_id         |Int64  |
        |team              |String |
        |opp_team          |String |
        |is_home           |Boolean|
        |exp_shots_against |Float64|
        |exp_save_pct      |Float64|
        |exp_saves         |Float64|
        |saves_lo          |Int64  |
        |saves_hi          |Int64  |
        |coverage          |Float64|

    Raises:
        ValueError: if ``coverage`` is not strictly between 0 and 1, or a ``starters`` game lacks a team.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_player_props import nhl_goalie_saves
            saves = nhl_goalie_saves(2026)
            saves.select("game_id", "player_id", "exp_saves", "saves_lo", "saves_hi").head()
    """
    if not 0.0 < coverage < 1.0:
        raise ValueError(f"coverage must be in (0, 1), got {coverage}")
    const = get_constants("nhl")
    box = (load_goalie_boxscores or load_nhl_goalie_boxscores)(seasons)
    if box.is_empty() or (starters is not None and starters.is_empty()):
        out = pl.DataFrame(schema=_SAVES_SCHEMA)
        return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out

    rows = _goalie_rows(box, starters).sort("player_id", "date")
    rows = rows.with_columns(
        _prior_sum("shots_against", "player_id").alias("shots_cum"),
        _prior_sum("saves", "player_id").alias("saves_cum"),
    )
    proj = rows.filter(pl.col("target") == True).join(  # noqa: E712
        _team_shot_rates(rows, const), on=["game_id", "team"], how="left"
    )
    side = pl.when(pl.col("is_home") == True).then(-0.5).otherwise(0.5) * const.shots_hfa  # noqa: E712
    proj = proj.with_columns(
        (const.starter_shot_share * (0.5 * (pl.col("sa_rate") + pl.col("opp_sf_rate")) + side)).alias(
            "exp_shots_against"
        ),
        (
            (pl.col("saves_cum") + const.save_pct_kappa * const.save_pct_prior)
            / (pl.col("shots_cum") + const.save_pct_kappa)
        ).alias("exp_save_pct"),
    ).with_columns((pl.col("exp_shots_against") * pl.col("exp_save_pct")).alias("exp_saves"))

    mean = proj["exp_saves"].fill_null(float("nan")).to_numpy()
    size = const.shots_nb_size
    tail = (1.0 - coverage) / 2.0
    lo, hi = nbinom.ppf(tail, size, size / (size + mean)), nbinom.ppf(1.0 - tail, size, size / (size + mean))
    out = proj.with_columns(
        pl.Series("saves_lo", lo).fill_nan(None).cast(pl.Int64),
        pl.Series("saves_hi", hi).fill_nan(None).cast(pl.Int64),
        pl.lit(coverage).alias("coverage"),
        pl.col("date").alias("game_date"),
    ).select(list(_SAVES_SCHEMA))
    out = out.sort("game_id", "is_home")
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out
