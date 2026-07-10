"""NHL/PWHL native power ratings -- opponent-adjusted even-strength xG, shrunk.

Model (1) of the T5.3 prediction spine: reuses the shipped ``xg`` column on
``load_nhl_pbp_full``/``load_nhl_pbp_lite`` (never re-scores shots), restricts
to even strength, opponent-adjusts by an iterative fixed-point solve, and
regresses each team's rating toward the league mean by games played (a
fitted games-played prior, ``shrink_k``) -- the hockey-specific answer to a
low-event-count, high-variance sport. See the design spec
(``2026-07-07-nhl-prediction-market-design.md`` Sec 3.3) for the full
methodology writeup and Sec 3.2(c) for why the solver differs from the
NFL/CFB per-play ridge.

``adjust_rate_opponent`` is intentionally a pure, column-name-parameterized
function with every constant passed in -- it is the flagged T7.2 extraction
candidate (the "rate-iterative + shrinkage" member of a future shared
``_common_ratings.py`` solver family, alongside the ridge member NFL/CFB use).
This module ships a working, self-contained NHL implementation now; the
cross-league factoring is a separate follow-on spine.

Example:
    Quick start::

        from sportsdataverse.nhl.nhl_team_ratings import nhl_team_ratings

        ratings = nhl_team_ratings(2023)
        print(ratings.sort("net_rank").head())

    Pipeline next step (one line)::

        ratings.filter(pl.col("team") == "TOR")

See Also:
    * `nhl-api-py`_ -- companion NHL Python client.
    * `nflfastR`_ -- the shared Phi-margin power-rating core this spine's Phase 2 mirrors.

.. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
.. _nflfastR: https://www.nflfastr.com
"""

from __future__ import annotations

import datetime as _dt
from typing import Literal, Union, overload

import numpy as np
import pandas as pd
import polars as pl

from sportsdataverse.nhl.nhl_prediction_constants import as_of_ratings_split, get_constants

_GAME_RATES_SCHEMA: dict[str, pl.PolarsDataType] = {
    "game_id": pl.Utf8,
    "season": pl.Int64,
    "date": pl.Date,
    "team": pl.Utf8,
    "opp_team": pl.Utf8,
    "is_home": pl.Boolean,
    "neutral_site": pl.Boolean,
    "xgf": pl.Float64,
    "xga": pl.Float64,
    "gf": pl.Int64,
    "ga": pl.Int64,
}

_ADJUST_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "team": pl.Utf8,
    "adj_for": pl.Float64,
    "adj_against": pl.Float64,
    "adj_net": pl.Float64,
    "raw_for": pl.Float64,
    "raw_against": pl.Float64,
    "games": pl.Int64,
}

_RATINGS_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "team": pl.Utf8,
    "adj_xgf": pl.Float64,
    "adj_xga": pl.Float64,
    "adj_xg_net": pl.Float64,
    "adj_gf": pl.Float64,
    "adj_ga": pl.Float64,
    "games": pl.Int64,
    "off_rank": pl.Int64,
    "def_rank": pl.Int64,
    "net_rank": pl.Int64,
    "net_z": pl.Float64,
}


def team_game_xg_rates(pbp: pl.DataFrame, schedule: pl.DataFrame, *, even_strength_only: bool = True) -> pl.DataFrame:
    """Per-(game, team) even-strength xG-for/against + realized goals.

    Args:
        pbp: a play-by-play frame shaped like ``load_nhl_pbp_full``/``load_nhl_pbp_lite``
            (needs ``game_id``, ``event_team_abbr``, ``home_abbr``, ``away_abbr``,
            ``home_skaters``, ``away_skaters``, ``home_goalie_in``, ``away_goalie_in``, ``xg``).
        schedule: a schedule frame with ``game_id``, ``season``, ``date``,
            ``home_abbr``, ``away_abbr``, ``neutral_site`` (``home_goals``/``away_goals``
            are accepted but ignored -- realized ``gf``/``ga`` are derived from the
            pbp's own GOAL events, never from schedule scores; see the module note
            on the ``load_nhl_schedule(s)`` placeholder-score bug for seasons <= 2023).
        even_strength_only: restrict to ``home_skaters == away_skaters == 5``
            with both goalies in net (filters out PP/PK/empty-net distortion).

    Returns:
        A polars DataFrame, one row per (game_id, team), both home and away.

        |col_name     |type   |
        |:------------|:------|
        |game_id      |String |
        |season       |Int64  |
        |date         |Date   |
        |team         |String |
        |opp_team     |String |
        |is_home      |Boolean|
        |neutral_site |Boolean|
        |xgf          |Float64|
        |xga          |Float64|
        |gf           |Int64  |
        |ga           |Int64  |

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_team_ratings import team_game_xg_rates
            from sportsdataverse.nhl import load_nhl_pbp_full, load_nhl_schedules

            pbp = load_nhl_pbp_full([2023])
            sched = load_nhl_schedules([2023])
            rates = team_game_xg_rates(pbp, sched)
            print(rates.filter(pl.col("team") == "TOR").head())
    """
    if pbp.is_empty() or schedule.is_empty():
        return pl.DataFrame(schema=_GAME_RATES_SCHEMA)

    shots = pbp.filter(pl.col("game_id").is_not_null() & pl.col("xg").is_not_null())
    if even_strength_only:
        shots = shots.filter(
            (pl.col("home_skaters") == 5)
            & (pl.col("away_skaters") == 5)
            & (pl.col("home_goalie_in") == 1)
            & (pl.col("away_goalie_in") == 1)
        )

    per_team_xg = (
        shots.group_by(["game_id", "event_team_abbr"])
        .agg(pl.col("xg").sum().alias("xgf"))
        .rename({"event_team_abbr": "team"})
    )

    goals = shots.filter(pl.col("event_type") == "GOAL")
    per_team_goals = (
        goals.group_by(["game_id", "event_team_abbr"]).agg(pl.len().alias("gf")).rename({"event_team_abbr": "team"})
    )

    sched = schedule.select(
        pl.col("game_id").cast(pl.Int64).cast(pl.Utf8),
        pl.col("season").cast(pl.Int64),
        pl.col("date"),
        pl.col("home_abbr"),
        pl.col("away_abbr"),
        pl.col("neutral_site").cast(pl.Boolean),
    )

    per_team_xg = per_team_xg.with_columns(pl.col("game_id").cast(pl.Int64).cast(pl.Utf8))
    per_team_goals = per_team_goals.with_columns(pl.col("game_id").cast(pl.Int64).cast(pl.Utf8))

    # NOTE: realized goals (gf/ga) are derived from the pbp's own GOAL events,
    # never from the schedule loader's home_score/away_score -- those columns
    # were found at grounding to be a placeholder constant (e.g. every 2022-23
    # game reporting the same "2-3" score) for load_nhl_schedule(s) seasons
    # <= 2023 (fixed from 2024 onward). Deriving from pbp sidesteps that
    # upstream data bug entirely and is also what the ``xgf``/``xga`` sum
    # already does, so both stats share one ground-truth source.
    rows = []
    for is_home, team_col, opp_col in (
        (True, "home_abbr", "away_abbr"),
        (False, "away_abbr", "home_abbr"),
    ):
        side = sched.select(
            "game_id",
            "season",
            "date",
            "neutral_site",
            pl.col(team_col).alias("team"),
            pl.col(opp_col).alias("opp_team"),
            pl.lit(is_home).alias("is_home"),
        )
        side = side.join(per_team_xg, on=["game_id", "team"], how="left")
        opp_xg = per_team_xg.rename({"team": "opp_team", "xgf": "xga"})
        side = side.join(opp_xg, on=["game_id", "opp_team"], how="left")
        side = side.join(per_team_goals, on=["game_id", "team"], how="left")
        opp_goals = per_team_goals.rename({"team": "opp_team", "gf": "ga"})
        side = side.join(opp_goals, on=["game_id", "opp_team"], how="left")
        rows.append(side)

    out = pl.concat(rows, how="vertical_relaxed").with_columns(
        pl.col("xgf").fill_null(0.0),
        pl.col("xga").fill_null(0.0),
        pl.col("gf").fill_null(0).cast(pl.Int64),
        pl.col("ga").fill_null(0).cast(pl.Int64),
    )
    return out.select(
        "game_id", "season", "date", "team", "opp_team", "is_home", "neutral_site", "xgf", "xga", "gf", "ga"
    )


def adjust_rate_opponent(
    game_rates: pl.DataFrame,
    *,
    for_col: str,
    against_col: str,
    hfa: float,
    avg: float,
    shrink_k: float,
    max_iter: int = 100,
    tol: float = 1e-4,
) -> pl.DataFrame:
    """Opponent-adjust a per-game for/against rate by iterative fixed-point, then shrink.

    League-agnostic: every constant (``hfa``, ``avg``, ``shrink_k``) is passed
    in -- no NHL/PWHL number is hard-coded here. This is the flagged T7.2
    "rate-iterative + shrinkage" shared-solver candidate (the hockey
    counterpart of the NFL/CFB per-play ridge); ``for_col``/``against_col``
    are symmetric (offense sees opponent defense).

    Args:
        game_rates: one row per (team, opponent, game) with columns
            ``season``, ``team``, ``opp_team``, ``is_home``, ``neutral_site``,
            and the two numeric rate columns named by ``for_col``/``against_col``.
        for_col: name of the team's own-side rate column (e.g. ``"xgf"``).
        against_col: name of the team's against-side rate column (e.g. ``"xga"``).
        hfa: home-ice edge added to the home side / subtracted from the away side.
        avg: league mean rate to adjust and shrink toward.
        shrink_k: games-played prior strength for the post-convergence shrink.
        max_iter: maximum fixed-point iterations.
        tol: convergence tolerance on the max absolute update.

    Returns:
        A polars DataFrame, one row per (season, team).

        |col_name     |type   |
        |:------------|:------|
        |season       |Int64  |
        |team         |String |
        |adj_for      |Float64|
        |adj_against  |Float64|
        |adj_net      |Float64|
        |raw_for      |Float64|
        |raw_against  |Float64|
        |games        |Int64  |

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_team_ratings import adjust_rate_opponent
            adjust_rate_opponent(
                game_rates, for_col="xgf", against_col="xga",
                hfa=0.2, avg=2.55, shrink_k=15.0,
            )
    """
    if game_rates.is_empty():
        return pl.DataFrame(schema=_ADJUST_SCHEMA)

    season = int(game_rates["season"][0])
    teams = game_rates["team"].unique().sort().to_list()
    idx = {t: i for i, t in enumerate(teams)}
    n = len(teams)
    ti = game_rates["team"].replace_strict(idx, return_dtype=pl.Int64).to_numpy()
    oi = game_rates["opp_team"].replace_strict(idx, return_dtype=pl.Int64).to_numpy()
    vf = game_rates[for_col].to_numpy().astype(float)
    va = game_rates[against_col].to_numpy().astype(float)
    is_home = game_rates["is_home"].to_numpy()
    neutral = game_rates["neutral_site"].to_numpy()
    side = np.where(neutral, 0.0, np.where(is_home, hfa / 2.0, -hfa / 2.0))
    games: np.ndarray = np.bincount(ti, minlength=n).astype(float)

    adj_o = np.full(n, avg)
    adj_d = np.full(n, avg)
    for _ in range(max_iter):
        contrib_o = vf - (adj_d[oi] - avg) - side
        contrib_d = va - (adj_o[oi] - avg) + side
        new_o = np.zeros(n)
        new_d = np.zeros(n)
        np.add.at(new_o, ti, contrib_o)
        np.add.at(new_d, ti, contrib_d)
        with np.errstate(invalid="ignore"):
            new_o = np.where(games > 0, new_o / np.maximum(games, 1), avg)
            new_d = np.where(games > 0, new_d / np.maximum(games, 1), avg)
        delta = max(np.max(np.abs(new_o - adj_o)), np.max(np.abs(new_d - adj_d)))
        adj_o, adj_d = new_o, new_d
        if delta < tol:
            break

    shrunk_o = (games * adj_o + shrink_k * avg) / (games + shrink_k)
    shrunk_d = (games * adj_d + shrink_k * avg) / (games + shrink_k)

    raw_o = np.zeros(n)
    raw_d = np.zeros(n)
    np.add.at(raw_o, ti, vf)
    np.add.at(raw_d, ti, va)
    raw_o = np.where(games > 0, raw_o / np.maximum(games, 1), avg)
    raw_d = np.where(games > 0, raw_d / np.maximum(games, 1), avg)

    return pl.DataFrame(
        {
            "season": [season] * n,
            "team": teams,
            "adj_for": shrunk_o,
            "adj_against": shrunk_d,
            "adj_net": shrunk_o - shrunk_d,
            "raw_for": raw_o,
            "raw_against": raw_d,
            "games": games.astype(np.int64),
        }
    )


@overload
def nhl_team_ratings(
    seasons: Union[int, list[int]],
    *,
    league: str = ...,
    as_of_date: _dt.date | None = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def nhl_team_ratings(
    seasons: Union[int, list[int]],
    *,
    league: str = ...,
    as_of_date: _dt.date | None = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def nhl_team_ratings(
    seasons: Union[int, list[int]],
    *,
    league: str = "nhl",
    as_of_date: _dt.date | None = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Opponent-adjusted, shrunk even-strength xG (+ goal) team ratings.

    Loads pbp + schedule for ``seasons``, restricts to even strength, applies
    the as-of-date leakage split if requested, opponent-adjusts + shrinks both
    the xG rate (primary) and the realized-goal rate (concurrent sanity
    rating) via :func:`adjust_rate_opponent`, and derives off/def/net ranks.

    Args:
        seasons: an int or iterable of seasons.
        league: ``"nhl"`` or ``"pwhl"`` -- resolves HFA/avg/shrink_k via
            :func:`sportsdataverse.nhl.nhl_prediction_constants.get_constants`.
        as_of_date: if given, only games strictly before this date are used
            (the leakage boundary for a predictive backtest).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame, one row per (season, team). Empty
        input seasons return a zero-row frame with the documented schema.

        |col_name   |type   |
        |:----------|:------|
        |season     |Int64  |
        |team       |String |
        |adj_xgf    |Float64|
        |adj_xga    |Float64|
        |adj_xg_net |Float64|
        |adj_gf     |Float64|
        |adj_ga     |Float64|
        |games      |Int64  |
        |off_rank   |Int64  |
        |def_rank   |Int64  |
        |net_rank   |Int64  |
        |net_z      |Float64|

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_team_ratings import nhl_team_ratings

            ratings = nhl_team_ratings(2023)
            print(ratings.sort("net_rank").head())

        As-of-date leakage-safe rating::

            import datetime as dt
            ratings = nhl_team_ratings(2023, as_of_date=dt.date(2023, 1, 1))

        Pipeline next step (one line)::

            ratings.filter(pl.col("team") == "TOR")

    See Also:
        * `nhl-api-py`_ -- companion NHL Python client.

    .. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
    """
    from sportsdataverse.nhl.nhl_loaders import load_nhl_pbp_full
    from sportsdataverse.nhl.nhl_loaders import load_nhl_schedules as _load_schedules

    const = get_constants(league)
    pbp = load_nhl_pbp_full(seasons)
    schedule = _load_schedules(seasons)

    if pbp.is_empty() or schedule.is_empty():
        return _empty_ratings(return_as_pandas)

    sched = schedule.filter(pl.col("game_type") == "R").select(
        pl.col("game_id"),
        pl.col("season"),
        pl.col("game_date").str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("date"),
        pl.col("home_team_abbr").alias("home_abbr"),
        pl.col("away_team_abbr").alias("away_abbr"),
        pl.lit(False).alias("neutral_site"),
        pl.col("home_score").cast(pl.Int64).alias("home_goals"),
        pl.col("away_score").cast(pl.Int64).alias("away_goals"),
    )

    game_rates = team_game_xg_rates(pbp, sched)
    if as_of_date is not None:
        game_rates = as_of_ratings_split(game_rates, as_of_date)
    if game_rates.is_empty():
        return _empty_ratings(return_as_pandas)

    xg_adj = adjust_rate_opponent(
        game_rates, for_col="xgf", against_col="xga", hfa=const.hfa, avg=const.avg_xgf, shrink_k=const.shrink_k
    )
    avg_goals = const.avg_total_goals / 2.0
    goal_adj = adjust_rate_opponent(
        game_rates, for_col="gf", against_col="ga", hfa=const.hfa, avg=avg_goals, shrink_k=const.shrink_k
    )

    assert xg_adj.schema["team"] == goal_adj.schema["team"]
    out = xg_adj.join(
        goal_adj.select("team", pl.col("adj_for").alias("adj_gf"), pl.col("adj_against").alias("adj_ga")),
        on="team",
        how="left",
    ).rename({"adj_for": "adj_xgf", "adj_against": "adj_xga", "adj_net": "adj_xg_net"})

    net_mean = out["adj_xg_net"].mean()
    net_std = out["adj_xg_net"].std()
    out = out.with_columns(
        pl.col("adj_xgf").rank(method="ordinal", descending=True).cast(pl.Int64).alias("off_rank"),
        pl.col("adj_xga").rank(method="ordinal", descending=False).cast(pl.Int64).alias("def_rank"),
        pl.col("adj_xg_net").rank(method="ordinal", descending=True).cast(pl.Int64).alias("net_rank"),
        (((pl.col("adj_xg_net") - net_mean) / net_std) if net_std else pl.lit(0.0)).alias("net_z"),
    ).select(
        "season",
        "team",
        "adj_xgf",
        "adj_xga",
        "adj_xg_net",
        "adj_gf",
        "adj_ga",
        "games",
        "off_rank",
        "def_rank",
        "net_rank",
        "net_z",
    )

    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out


def _empty_ratings(return_as_pandas: bool) -> Union[pl.DataFrame, pd.DataFrame]:
    out = pl.DataFrame(schema=_RATINGS_SCHEMA)
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out
