"""College basketball season products: RPI, a NET-style estimate, bracketology and the season Monte Carlo.

BB-D17. The league-agnostic core behind three daily tables (``wbb`` shims pass
``league="womens"``):

* :func:`season_resume` -- the ``strength_of_schedule`` table: records, SoS, the quad
  résumé, WAB, :func:`rating_percentage_index` and :func:`net_estimate`.
* :func:`season_bracketology` -- the ``bracketology`` table: the as-of projected field and
  seeds, auto-bids, at-large probability and the bubble.
* :func:`simulate_season` -- the ``season_sim`` table: projected W-L, conference standings,
  regular-season and conference-tournament title %, auto-bid / at-large / bid %, and the
  seed distribution, from a seeded Monte Carlo.

Everything reads one normalized ESPN schedule (:func:`normalize_college_schedule`) plus the
team box the ratings engine needs. Division I membership and conferences come from the
schedule itself (:func:`college_teams`), because the women's standings release has gaps.

Sources for the fixed rules (no fitted numbers here):

* **RPI.** ``RPI = 0.25 * WP + 0.50 * OWP + 0.25 * OOWP`` over Division I games only. WP
  weights a home win 0.6, a road win 1.4, a home loss 1.4, a road loss 0.6 and a neutral game
  1.0. OWP is each opponent's unweighted winning percentage with its games against the team
  removed, averaged over the team's games; OOWP is the opponents' OWP, averaged the same way.
  NCAA News, "Misunderstood metric" (2006-12-18), and "Division I women's basketball announces
  RPI revision" (2010-09):
  https://s3.amazonaws.com/ncaanewsarchive/2006/Association-wide/misunderstood-metric---12-18-06-ncaa-news.html
  https://ncaanewsarchive.s3.amazonaws.com/2010-news-stories/september-latest-news/division-i-womens-basketball-announces-rpi-revision.html
  The men's location weights start in 2004-05 (season 2005); the women's in 2011-12 (season
  2012). Earlier seasons weight every game 1.0.
* **Quads** are in ``LEAGUE_CONSTANTS[league].quad_thresholds``
  (:mod:`sportsdataverse.mbb.mbb_prediction_constants`), from the NCAA's selection-criteria
  explainer (2025-03-05). The men's and women's ranges differ.
* **Field.** 68 teams: every conference-tournament champion (the auto-bid) plus the best
  at-large résumés. The First Four is the last four at-large teams and the four lowest auto-bid
  teams, two games each, so 64 seed slots fill four per seed line.
"""

from __future__ import annotations

import datetime
from collections.abc import Iterable
from typing import NamedTuple, Optional

import numpy as np
import polars as pl
from scipy.stats import norm

from sportsdataverse._common.league_context import MIN_COHORT_TEAMS, attach_ranks, attach_sample_sizes
from sportsdataverse.mbb.mbb_prediction_constants import get_constants

__all__ = [
    "FIELD_SIZE",
    "NET_ESTIMATE_WEIGHTS",
    "RPI_LOCATION_WEIGHTS",
    "RPI_WEIGHTS",
    "college_teams",
    "conference_auto_bids",
    "net_estimate",
    "normalize_college_schedule",
    "SeasonInputs",
    "load_season_inputs",
    "rating_percentage_index",
    "season_bracketology",
    "season_inputs",
    "season_resume",
    "simulate_season",
    "split_season",
    "with_conf_games",
]

#: RPI component weights: (WP, OWP, OOWP).
RPI_WEIGHTS = (0.25, 0.50, 0.25)
#: WP location weights (NCAA, men's 2004-05+, women's 2011-12+).
RPI_LOCATION_WEIGHTS = {"home_win": 0.6, "road_win": 1.4, "home_loss": 1.4, "road_loss": 0.6, "neutral": 1.0}
_RPI_WEIGHTED_FROM = {"mens": 2005, "womens": 2012}

#: NET-style estimate: the NET's two published halves, an efficiency score and a results
#: score, stood in for by AdjEM and WAB (each z-scored over Division I). The NCAA does not
#: publish its blend. 0.8 / 0.2 maximizes the rank correlation with the men's 2024 final NET
#: (Spearman 0.996, vs 0.991 at 0.5 / 0.5 and 0.994 for AdjEM alone); the women's 2024 NET,
#: not used in the fit, agrees (0.998 at 0.8, vs 0.989 at 0.5). Grid of 0.1 steps over the
#: committed WarrenNolan mirror in tests/fixtures/bb_college_season.
NET_ESTIMATE_WEIGHTS = {"adj_em": 0.8, "wab": 0.2}

FIELD_SIZE = 68
_SEED_LINES = 16

#: The normalized schedule every function here reads.
SCHEDULE_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Utf8(),
    "season": pl.Int64(),
    "season_type": pl.Int64(),
    "date": pl.Date(),
    "home_team_id": pl.Utf8(),
    "away_team_id": pl.Utf8(),
    "home_team": pl.Utf8(),
    "away_team": pl.Utf8(),
    "home_conference_id": pl.Utf8(),
    "away_conference_id": pl.Utf8(),
    "home_score": pl.Int64(),
    "away_score": pl.Int64(),
    "neutral_site": pl.Boolean(),
    "conference_competition": pl.Boolean(),
    "type_abbreviation": pl.Utf8(),
    "groups_id": pl.Utf8(),
    "groups_short_name": pl.Utf8(),
    "notes_headline": pl.Utf8(),
    "completed": pl.Boolean(),
}

_MIN_CONF_OPP_GAMES = 10

_TEAMS_SCHEMA = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "team": pl.Utf8,
    "conference_id": pl.Utf8,
    "conference": pl.Utf8,
    "eligible": pl.Boolean,
}


def _sid(c: str) -> pl.Expr:
    return pl.col(c).cast(pl.Int64).cast(pl.Utf8)


def normalize_college_schedule(raw: pl.DataFrame) -> pl.DataFrame:
    """Project an ESPN ``load_mbb_schedule`` / ``load_wbb_schedule`` frame to :data:`SCHEDULE_SCHEMA`.

    Ids become Utf8 (``Int -> Int64 -> Utf8``, so never ``"123.0"``) and a game repeated in
    the release keeps its first row.

    Args:
        raw: The schedule release for one or more seasons.

    Returns:
        One row per game with the :data:`SCHEDULE_SCHEMA` columns, sorted by date.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_loaders import load_mbb_schedule
            from sportsdataverse.mbb.mbb_college_season import normalize_college_schedule
            sched = normalize_college_schedule(load_mbb_schedule([2024]))
    """
    return (
        raw.select(
            _sid("game_id").alias("game_id"),
            pl.col("season").cast(pl.Int64),
            pl.col("season_type").cast(pl.Int64),
            pl.col("game_date").cast(pl.Date).alias("date"),
            _sid("home_id").alias("home_team_id"),
            _sid("away_id").alias("away_team_id"),
            pl.col("home_location").cast(pl.Utf8).alias("home_team"),
            pl.col("away_location").cast(pl.Utf8).alias("away_team"),
            _sid("home_conference_id").alias("home_conference_id"),
            _sid("away_conference_id").alias("away_conference_id"),
            pl.col("home_score").cast(pl.Int64),
            pl.col("away_score").cast(pl.Int64),
            pl.col("neutral_site").cast(pl.Boolean),
            pl.col("conference_competition").cast(pl.Boolean),
            pl.col("type_abbreviation").cast(pl.Utf8),
            _sid("groups_id").alias("groups_id"),
            pl.col("groups_short_name").cast(pl.Utf8),
            pl.col("notes_headline").cast(pl.Utf8),
            pl.col("status_type_completed").cast(pl.Boolean).alias("completed"),
        )
        .unique("game_id", keep="first", maintain_order=True)
        .sort("date", "game_id")
    )


def _is_conf_tourney() -> pl.Expr:
    return (pl.col("type_abbreviation") == "TRNMNT") & (pl.col("conference_competition") == True)  # noqa: E712


def college_teams(schedule: pl.DataFrame, ineligible: Iterable[str] = ()) -> pl.DataFrame:
    """Division I teams and their conferences, read off the schedule.

    A team is Division I when ESPN gives it a conference id. The id is the team's own most
    common ``*_conference_id``; else the most common ``groups_id`` of its conference games;
    else, when ESPN strips every conference field (Dayton in the 2024 women's release), the
    conference it played 10 or more games against. Independents keep their id with a null
    ``conference`` name and get no auto-bid.

    Args:
        schedule: A :func:`normalize_college_schedule` frame.
        ineligible: Team ids barred from the NCAA tournament that season (programs
            reclassifying to Division I, postseason bans). They count in every rating and
            résumé but get no bid; ESPN does not flag them, so the caller lists them.

    Returns:
        One row per Division I ``(season, team_id)``: ``season, team_id, team,
        conference_id, conference, eligible``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_college_season import college_teams
            teams = college_teams(sched)
    """
    sides = []
    for s in ("home", "away"):
        sides.append(
            schedule.select(
                "season",
                pl.col(f"{s}_team_id").alias("team_id"),
                pl.col(f"{s}_team").alias("team"),
                pl.col(f"{s}_conference_id").alias("own"),
                pl.when(pl.col("conference_competition") == True)  # noqa: E712
                .then(pl.col("groups_id"))
                .alias("grp"),
                pl.col(f"{'away' if s == 'home' else 'home'}_conference_id").alias("opp"),
            )
        )
    first_mode = lambda c: pl.col(c).drop_nulls().mode().sort().first()  # noqa: E731
    # last resort: a team ESPN strips of every conference field (Dayton, 2024 women) but that
    # played a conference schedule -- _MIN_CONF_OPP_GAMES or more games against one conference
    opp = (
        pl.concat(sides)
        .drop_nulls("opp")
        .group_by("season", "team_id", "opp")
        .len()
        .filter(pl.col("len") >= _MIN_CONF_OPP_GAMES)
        .sort("len", "opp", descending=[True, False])
        .group_by("season", "team_id", maintain_order=True)
        .agg(pl.col("opp").first())
    )
    per = (
        pl.concat(sides)
        .group_by("season", "team_id")
        .agg(first_mode("team").alias("team"), first_mode("own").alias("own"), first_mode("grp").alias("grp"))
        .join(opp, on=["season", "team_id"], how="left", validate="1:1")
        .with_columns(pl.coalesce("own", "grp", "opp").alias("conference_id"))
        .filter(pl.col("conference_id").is_not_null())
    )
    names = (
        schedule.filter(pl.col("conference_competition") == True)  # noqa: E712
        .group_by("season", pl.col("groups_id").alias("conference_id"))
        .agg(first_mode("groups_short_name").alias("conference"))
    )
    assert per.schema["conference_id"] == names.schema["conference_id"]
    return (
        per.join(names, on=["season", "conference_id"], how="left", validate="m:1")
        .with_columns(~pl.col("team_id").is_in(sorted(set(ineligible))).alias("eligible"))
        .select(list(_TEAMS_SCHEMA))
        .cast(_TEAMS_SCHEMA)
        .sort("season", "team_id")
    )


def split_season(
    schedule: pl.DataFrame, as_of_date: Optional[datetime.date] = None
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Split a season into the games played before ``as_of_date`` and the ones left to simulate.

    Args:
        schedule: A :func:`normalize_college_schedule` frame.
        as_of_date: Games strictly before this date count as played. ``None`` takes every
            completed game.

    Returns:
        ``(played, remaining)``. ``played`` is every completed game with both scores.
        ``remaining`` is the regular-season games (``season_type == 2``) not yet played,
        without the conference tournaments, which :func:`simulate_season` draws itself, and
        without the postseason.

    Example:
        Quick start::

            import datetime
            from sportsdataverse.mbb.mbb_college_season import split_season
            played, remaining = split_season(sched, datetime.date(2024, 2, 1))
    """
    done = (
        (pl.col("completed") == True)  # noqa: E712
        & pl.col("home_score").is_not_null()
        & pl.col("away_score").is_not_null()
    )
    if as_of_date is not None:
        done = done & (pl.col("date") < as_of_date)
    played = schedule.filter(done)
    remaining = schedule.filter(~done.fill_null(False) & (pl.col("season_type") == 2) & ~_is_conf_tourney())
    if as_of_date is not None:
        remaining = remaining.filter(pl.col("date") >= as_of_date)
    return played, remaining


def _long(results: pl.DataFrame) -> pl.DataFrame:
    """Two rows per game: the team, its opponent, the venue and the result."""
    out = []
    for s, o in (("home", "away"), ("away", "home")):
        out.append(
            results.select(
                "game_id",
                "season",
                pl.col(f"{s}_team_id").alias("team_id"),
                pl.col(f"{o}_team_id").alias("opp_team_id"),
                pl.when(pl.col("neutral_site") == True)  # noqa: E712
                .then(pl.lit("neutral"))
                .otherwise(pl.lit(s if s == "home" else "away"))
                .alias("venue"),
                (pl.col(f"{s}_score") > pl.col(f"{o}_score")).alias("won"),
                pl.col("conf_game") if "conf_game" in results.columns else pl.lit(False).alias("conf_game"),
            )
        )
    return pl.concat(out)


def with_conf_games(games: pl.DataFrame, teams: pl.DataFrame) -> pl.DataFrame:
    """Add ``conf_game``: a regular-season conference game, the standings' denominator.

    Both teams are in the same conference (as :func:`college_teams` assigns them), the game is
    not a tournament (conference tournaments and multi-team events are ``TRNMNT``), and ESPN
    flags it a conference game -- or cannot, because it left a side's conference id null.

    Args:
        games: Games with the :data:`SCHEDULE_SCHEMA` columns.
        teams: The Division I teams (:func:`college_teams`).

    Returns:
        ``games`` with a Boolean ``conf_game``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_college_season import with_conf_games
            games = with_conf_games(played, teams)
    """
    assert games.schema["home_team_id"] == teams.schema["team_id"]
    conf = teams.filter(pl.col("conference").is_not_null()).select("season", "team_id", "conference_id")
    out = games.join(
        conf.rename({"team_id": "home_team_id", "conference_id": "_hc"}), on=["season", "home_team_id"], how="left"
    ).join(conf.rename({"team_id": "away_team_id", "conference_id": "_ac"}), on=["season", "away_team_id"], how="left")
    flagged = (
        (pl.col("conference_competition") == True)
        | pl.col("home_conference_id").is_null()
        | pl.col(  # noqa: E712
            "away_conference_id"
        ).is_null()
    )
    return out.with_columns(
        ((pl.col("_hc") == pl.col("_ac")) & (pl.col("type_abbreviation") != "TRNMNT") & flagged)
        .fill_null(False)
        .alias("conf_game")
    ).drop("_hc", "_ac")


def _d1_games(results: pl.DataFrame, teams: pl.DataFrame) -> pl.DataFrame:
    assert results.schema["home_team_id"] == teams.schema["team_id"] == pl.Utf8
    d1 = teams.get_column("team_id").implode()
    return results.filter(pl.col("home_team_id").is_in(d1) & pl.col("away_team_id").is_in(d1))


_RPI_SCHEMA = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "rpi_games": pl.Int64,
    "rpi_wp": pl.Float64,
    "rpi_owp": pl.Float64,
    "rpi_oowp": pl.Float64,
    "rpi": pl.Float64,
}


def rating_percentage_index(results: pl.DataFrame, teams: pl.DataFrame, *, league: str = "mens") -> pl.DataFrame:
    """The NCAA Rating Percentage Index over Division I games.

    ``RPI = 0.25 * WP + 0.50 * OWP + 0.25 * OOWP`` (NCAA, see the module docstring for the
    sources). WP is location weighted from the league's first weighted season (men's 2005,
    women's 2012): a home win counts 0.6, a road win 1.4, a home loss 1.4, a road loss 0.6, a
    neutral game 1.0. OWP is each opponent's unweighted winning percentage without its games
    against the team, averaged over the team's games (an opponent met twice counts twice);
    OOWP averages the opponents' OWP the same way. Games against non-Division I teams are left
    out of every term.

    Args:
        results: Played games (:func:`split_season`).
        teams: The Division I teams (:func:`college_teams`).
        league: ``"mens"`` or ``"womens"`` (sets the first location-weighted season).

    Returns:
        One row per Division I team with a Division I game: ``season, team_id, rpi_games,
        rpi_wp, rpi_owp, rpi_oowp, rpi``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_college_season import rating_percentage_index
            rpi = rating_percentage_index(played, teams)
    """
    games = _long(_d1_games(results, teams))
    if games.height == 0:
        return pl.DataFrame(schema=_RPI_SCHEMA)
    lw = RPI_LOCATION_WEIGHTS
    weighted = pl.col("season") >= _RPI_WEIGHTED_FROM[league]
    w = (
        pl.when(~weighted | (pl.col("venue") == "neutral"))
        .then(lw["neutral"])
        .when(pl.col("won") & (pl.col("venue") == "home"))
        .then(lw["home_win"])
        .when(pl.col("won"))
        .then(lw["road_win"])
        .when(pl.col("venue") == "home")
        .then(lw["home_loss"])
        .otherwise(lw["road_loss"])
    )
    games = games.with_columns(w.alias("w"), pl.col("won").cast(pl.Float64).alias("win"))
    keys = ["season", "team_id"]
    tot = games.group_by(keys).agg(pl.col("win").sum().alias("tw"), pl.len().cast(pl.Float64).alias("tg"))
    h2h = games.group_by("season", "team_id", "opp_team_id").agg(
        pl.col("win").sum().alias("hw"), pl.len().cast(pl.Float64).alias("hg")
    )
    # each opponent's record without its games against this team
    opp = (
        games.join(tot.rename({"team_id": "opp_team_id"}), on=["season", "opp_team_id"], how="left", validate="m:1")
        .join(
            h2h.rename({"team_id": "opp_team_id", "opp_team_id": "team_id"}),
            on=["season", "team_id", "opp_team_id"],
            how="left",
            validate="m:1",
        )
        .with_columns(
            pl.when(pl.col("tg") > pl.col("hg"))
            .then((pl.col("tw") - pl.col("hw")) / (pl.col("tg") - pl.col("hg")))
            .alias("opp_wp_ex")
        )
    )
    base = opp.group_by(keys).agg(
        pl.len().cast(pl.Int64).alias("rpi_games"),
        ((pl.col("w") * pl.col("win")).sum() / pl.col("w").sum()).alias("rpi_wp"),
        pl.col("opp_wp_ex").mean().alias("rpi_owp"),
    )
    oowp = (
        games.select("season", "team_id", "opp_team_id")
        .join(
            base.select("season", pl.col("team_id").alias("opp_team_id"), pl.col("rpi_owp").alias("opp_owp")),
            on=["season", "opp_team_id"],
            how="left",
            validate="m:1",
        )
        .group_by(keys)
        .agg(pl.col("opp_owp").mean().alias("rpi_oowp"))
    )
    a, b, c = RPI_WEIGHTS
    return (
        base.join(oowp, on=keys, how="left", validate="1:1")
        .with_columns((a * pl.col("rpi_wp") + b * pl.col("rpi_owp") + c * pl.col("rpi_oowp")).alias("rpi"))
        .select(list(_RPI_SCHEMA))
        .sort("season", "rpi", descending=[False, True], nulls_last=True)
    )


def _z(c: str) -> pl.Expr:
    return (pl.col(c) - pl.col(c).mean().over("season")) / pl.col(c).std().over("season")


def net_estimate() -> pl.Expr:
    """The NET-style score: ``0.8 * z(adj_em) + 0.2 * z(wab)`` within the season.

    The NCAA's NET blends a results score (the Team Value Index) with adjusted net
    efficiency and does not publish the blend; WAB stands in for the results half and the
    ratings engine's AdjEM for the efficiency half, weighted as :data:`NET_ESTIMATE_WEIGHTS`
    (fitted to the men's 2024 NET, see there). Apply it to
    a Division I frame with ``adj_em`` and ``wab``.

    Returns:
        A Float64 expression named ``net_estimate`` (higher is better).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_college_season import net_estimate
            resume.with_columns(net_estimate())
    """
    return sum(wt * _z(c) for c, wt in NET_ESTIMATE_WEIGHTS.items()).cast(pl.Float64).alias("net_estimate")


def _records(results: pl.DataFrame, teams: pl.DataFrame) -> pl.DataFrame:
    """W-L overall (every opponent) and in conference games, per Division I team."""
    long = _long(with_conf_games(results, teams)).join(
        teams.select("season", "team_id"), on=["season", "team_id"], how="semi"
    )
    return long.group_by("season", "team_id").agg(
        pl.col("won").sum().cast(pl.Int64).alias("wins"),
        (~pl.col("won")).sum().cast(pl.Int64).alias("losses"),
        (pl.col("won") & pl.col("conf_game")).sum().cast(pl.Int64).alias("conf_wins"),
        (~pl.col("won") & pl.col("conf_game")).sum().cast(pl.Int64).alias("conf_losses"),
    )


RESUME_RANKED = ("adj_em", "sos", "wab", "rpi", "net_estimate")


def season_resume(
    results: pl.DataFrame,
    team_box: pl.DataFrame,
    teams: pl.DataFrame,
    *,
    league: str = "mens",
    as_of_date: Optional[datetime.date] = None,
) -> pl.DataFrame:
    """The ``strength_of_schedule`` table: records, ratings, SoS, quads, WAB, RPI and the NET estimate.

    The ratings come from :mod:`sportsdataverse.mbb.mbb_team_ratings` (every played game).
    SoS, WAB and the quads come from
    :func:`~sportsdataverse.mbb.mbb_strength_of_schedule.strength_of_schedule` over Division I
    games, with the opponent ranked by :func:`net_estimate` as the NCAA ranks by NET.

    Args:
        results: Played games (:func:`split_season`).
        team_box: The ESPN team box (``load_mbb_team_boxscore``) for the season.
        teams: The Division I teams (:func:`college_teams`).
        league: ``"mens"`` or ``"womens"``.
        as_of_date: Stamped on every row (null when ``None``).

    Returns:
        One row per Division I team, ordered by ``net_estimate_rank``. The ``_rank`` /
        ``_pct`` columns follow BB-D2's grammar (1 is best, average ties, within the season);
        ``_n`` is the Division I games behind the number (all games for ``adj_em``).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_college_season import season_resume
            resume = season_resume(played, team_box, teams)
    """
    from sportsdataverse.mbb.mbb_strength_of_schedule import strength_of_schedule
    from sportsdataverse.mbb.mbb_team_ratings import adjust_efficiency, raw_game_efficiency

    ratings = (
        adjust_efficiency(raw_game_efficiency(results, team_box), league=league)
        .join(teams.select("season", "team_id"), on=["season", "team_id"], how="semi")
        .with_columns(_z("adj_em").alias("adj_em_z"))
    )
    d1 = _d1_games(results, teams)
    pre = strength_of_schedule(
        d1,
        ratings.with_columns(pl.col("adj_em").rank("ordinal", descending=True).over("season").alias("rank")),
        league=league,
    )
    net = (
        ratings.select("season", "team_id", "adj_em")
        .join(pre.select("season", "team_id", "wab"), on=["season", "team_id"], how="inner", validate="1:1")
        .with_columns(net_estimate())
    )
    net = attach_ranks(net, ["net_estimate"], min_cohort=MIN_COHORT_TEAMS)
    sos = strength_of_schedule(
        d1,
        ratings.join(
            net.select("season", "team_id", pl.col("net_estimate_rank").alias("rank")), on=["season", "team_id"]
        ),
        league=league,
    ).drop("sos_rank")
    rpi = rating_percentage_index(results, teams, league=league)
    keys = ["season", "team_id"]
    out = (
        teams.join(_records(results, teams), on=keys, how="left", validate="1:1")
        .join(
            ratings.select(*keys, "adj_o", "adj_d", "adj_em", "adj_em_z", "games"), on=keys, how="left", validate="1:1"
        )
        .join(sos, on=keys, how="left", validate="1:1")
        .join(rpi, on=keys, how="left", validate="1:1")
        .join(net.select(*keys, "net_estimate"), on=keys, how="left", validate="1:1")
        .with_columns(pl.col("wins", "losses", "conf_wins", "conf_losses", "rpi_games").fill_null(0))
    )
    out = attach_ranks(out, list(RESUME_RANKED), min_cohort=MIN_COHORT_TEAMS)
    out = attach_sample_sizes(
        out,
        {
            "adj_em": pl.col("games"),
            "sos": pl.col("rpi_games"),
            "wab": pl.col("rpi_games"),
            "rpi": pl.col("rpi_games"),
            "net_estimate": pl.col("rpi_games"),
        },
    )
    return (
        out.with_columns(pl.lit(as_of_date, dtype=pl.Date).alias("as_of_date"))
        .select(list(RESUME_SCHEMA))
        .sort("season", "net_estimate_rank", nulls_last=True)
    )


RESUME_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "as_of_date": pl.Date(),
    "team_id": pl.Utf8(),
    "team": pl.Utf8(),
    "conference_id": pl.Utf8(),
    "conference": pl.Utf8(),
    "eligible": pl.Boolean(),
    "wins": pl.Int64(),
    "losses": pl.Int64(),
    "conf_wins": pl.Int64(),
    "conf_losses": pl.Int64(),
    "games": pl.Int64(),
    "adj_o": pl.Float64(),
    "adj_d": pl.Float64(),
    "adj_em": pl.Float64(),
    "adj_em_z": pl.Float64(),
    "sos": pl.Float64(),
    "wab": pl.Float64(),
    **{f"quad{q}_{r}": pl.Int64() for q in (1, 2, 3, 4) for r in ("w", "l")},
    "quality_wins": pl.Int64(),
    "rpi_games": pl.Int64(),
    "rpi_wp": pl.Float64(),
    "rpi_owp": pl.Float64(),
    "rpi_oowp": pl.Float64(),
    "rpi": pl.Float64(),
    "net_estimate": pl.Float64(),
    **{f"{m}_{s}": (pl.Int64() if s == "n" else pl.Float64()) for m in RESUME_RANKED for s in ("rank", "pct", "n")},
}


def _final_played() -> pl.Expr:
    return pl.col("notes_headline").str.contains(r"(?i)-\s*final\s*$")


def conference_auto_bids(results: pl.DataFrame, teams: pl.DataFrame, resume: pl.DataFrame) -> pl.DataFrame:
    """One auto-bid team per conference, as of the played games.

    The auto-bid goes to the conference-tournament champion: the winner of the conference's
    last tournament game, once that game's ESPN note ends in "Final" -- or the runner-up when
    the champion is ineligible (Southern Indiana won the 2024 women's OVC final; UT Martin took
    the bid). Before the final the projected auto-bid is the conference leader among eligible
    teams still alive in the tournament (no tournament loss): best conference winning
    percentage, ties to the higher ``net_estimate``. Independents get none.

    Args:
        results: Played games (:func:`split_season`).
        teams: The Division I teams (:func:`college_teams`, with ``eligible``).
        resume: :func:`season_resume` (``conf_wins``, ``conf_losses``, ``net_estimate``).

    Returns:
        ``season, conference_id, team_id, champion`` -- ``champion`` is True when the final has
        been played.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_college_season import conference_auto_bids
            bids = conference_auto_bids(played, teams, resume)
    """
    status = _conference_status(results, teams)
    lead = (
        resume.join(status.select("season", "conference_id", "alive"), on=["season", "conference_id"], how="inner")
        .filter(pl.col("alive").list.contains(pl.col("team_id")) & (pl.col("eligible") == True))  # noqa: E712
        .with_columns(
            (pl.col("conf_wins") / pl.max_horizontal(pl.col("conf_wins") + pl.col("conf_losses"), pl.lit(1))).alias(
                "_pct"
            )
        )
        .sort("_pct", "net_estimate", descending=True, nulls_last=True)
        .group_by("season", "conference_id", maintain_order=True)
        .agg(pl.col("team_id").first().alias("leader"))
    )
    return (
        status.join(lead, on=["season", "conference_id"], how="left", validate="1:1")
        .select(
            "season",
            "conference_id",
            pl.coalesce("champion_id", "leader").alias("team_id"),
            pl.col("champion_id").is_not_null().alias("champion"),
        )
        .filter(pl.col("team_id").is_not_null())
        .sort("season", "conference_id")
    )


def _conference_status(results: pl.DataFrame, teams: pl.DataFrame) -> pl.DataFrame:
    """Per conference: once the final is played, its winner and the auto-bid holder; the members still alive.

    Ineligible members play the tournament, so they are members and can be alive or win it; the
    auto-bid then goes to the final's loser.
    """
    barred = teams.filter(pl.col("eligible") == False).get_column("team_id").implode()  # noqa: E712
    members = (
        teams.filter(pl.col("conference").is_not_null())
        .group_by("season", "conference_id")
        .agg(pl.col("team_id").alias("members"))
    )
    ct = results.filter(_is_conf_tourney()).with_columns(pl.col("groups_id").alias("conference_id"))
    final = (
        ct.sort("date", "game_id")
        .group_by("season", "conference_id")
        .agg(pl.all().last())
        .filter(_final_played())
        .select(
            "season",
            "conference_id",
            pl.when(pl.col("home_score") > pl.col("away_score"))
            .then(pl.col("home_team_id"))
            .otherwise(pl.col("away_team_id"))
            .alias("_w"),
            pl.when(pl.col("home_score") > pl.col("away_score"))
            .then(pl.col("away_team_id"))
            .otherwise(pl.col("home_team_id"))
            .alias("_l"),
        )
        .with_columns(
            pl.when(pl.col("_w").is_in(barred)).then(pl.col("_l")).otherwise(pl.col("_w")).alias("champion_id")
        )
        .rename({"_w": "winner_id"})
        .drop("_l")
    )
    losers = (
        ct.select(
            "season",
            "conference_id",
            pl.when(pl.col("home_score") > pl.col("away_score"))
            .then(pl.col("away_team_id"))
            .otherwise(pl.col("home_team_id"))
            .alias("loser"),
        )
        .group_by("season", "conference_id")
        .agg(pl.col("loser"))
    )
    return (
        members.join(final, on=["season", "conference_id"], how="left", validate="1:1")
        .join(losers, on=["season", "conference_id"], how="left", validate="1:1")
        .with_columns(
            pl.when(pl.col("loser").is_null())
            .then(pl.col("members"))
            .otherwise(pl.col("members").list.set_difference(pl.col("loser")))
            .alias("alive")
        )
        .select("season", "conference_id", "members", "alive", "winner_id", "champion_id")
    )


# --- field selection and seeding -----------------------------------------------------------


def _seed_field(score: np.ndarray, bid: np.ndarray, auto: np.ndarray) -> np.ndarray:
    """Seed lines (1-16, 0 = out) for one field, with the First Four.

    The four worst at-large teams and the four worst auto-bid teams in the field play the
    First Four in pairs (1st-2nd, 3rd-4th of each group); a pair takes one seed slot at its
    mean score. The 64 slots then fill four per line in score order. A field that is not 68
    teams with at least four of each kind falls back to four per line, the overflow on 16.
    """
    seeds = np.zeros(score.shape[0], dtype=np.int64)
    field = np.flatnonzero(bid)
    field = field[np.argsort(-score[field], kind="stable")]
    al = [i for i in field if not auto[i]]
    ab = [i for i in field if auto[i]]
    if len(field) == FIELD_SIZE and len(al) >= 4 and len(ab) >= 4:
        ff_al, ff_ab = al[-4:], ab[-4:]
        ff = set(ff_al) | set(ff_ab)
        units: list[tuple[float, list[int]]] = [(float(score[i]), [int(i)]) for i in field if i not in ff]
        for grp in (ff_al, ff_ab):
            for pair in (grp[0:2], grp[2:4]):
                units.append((float(np.mean(score[pair])), [int(i) for i in pair]))
        units.sort(key=lambda u: -u[0])
        for pos, (_s, members) in enumerate(units):
            seeds[members] = pos // 4 + 1
    else:
        seeds[field] = np.minimum(np.arange(len(field)) // 4 + 1, _SEED_LINES)
    return seeds


def _bubble_labels(at_large_rank: pl.Expr, slots: int) -> pl.Expr:
    """Bracketology's bubble: the last four byes / in and the first / next four out."""
    return (
        pl.when(at_large_rank.is_between(slots - 7, slots - 4))
        .then(pl.lit("last_four_byes"))
        .when(at_large_rank.is_between(slots - 3, slots))
        .then(pl.lit("last_four_in"))
        .when(at_large_rank.is_between(slots + 1, slots + 4))
        .then(pl.lit("first_four_out"))
        .when(at_large_rank.is_between(slots + 5, slots + 8))
        .then(pl.lit("next_four_out"))
    )


BRACKETOLOGY_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "as_of_date": pl.Date(),
    "team_id": pl.Utf8(),
    "team": pl.Utf8(),
    "conference": pl.Utf8(),
    "eligible": pl.Boolean(),
    "wins": pl.Int64(),
    "losses": pl.Int64(),
    "net_estimate_rank": pl.Float64(),
    "rpi_rank": pl.Float64(),
    "wab": pl.Float64(),
    "quad1_w": pl.Int64(),
    "quad1_l": pl.Int64(),
    "resume_score": pl.Float64(),
    "auto_bid": pl.Boolean(),
    "conf_champion": pl.Boolean(),
    "at_large_rank": pl.Int64(),
    "bid": pl.Boolean(),
    "projected_seed": pl.Int64(),
    "at_large_prob": pl.Float64(),
    "bubble": pl.Utf8(),
}


def season_bracketology(
    resume: pl.DataFrame, auto_bids: pl.DataFrame, *, league: str = "mens", field_size: int = FIELD_SIZE
) -> pl.DataFrame:
    """The ``bracketology`` table: the projected field, seeds and bubble from one résumé.

    Args:
        resume: :func:`season_resume` for one season.
        auto_bids: :func:`conference_auto_bids`.
        league: ``"mens"`` or ``"womens"``.
        field_size: The field (68).

    Returns:
        One row per Division I team, best ``resume_score`` first. ``at_large_rank`` orders the
        teams without an auto-bid; ``bubble`` labels at-large ranks around the cut
        (``last_four_byes``, ``last_four_in``, ``first_four_out``, ``next_four_out``).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_college_season import season_bracketology
            field = season_bracketology(resume, bids)
    """
    from sportsdataverse.mbb.mbb_bracketology import project_bracket

    auto_ids = set(auto_bids.get_column("team_id").to_list())
    champs = set(auto_bids.filter(pl.col("champion") == True).get_column("team_id").to_list())  # noqa: E712
    proj = project_bracket(
        resume.filter(pl.col("eligible") == True)  # noqa: E712
        .select("season", "team_id", "adj_em_z", "sos", "wab", "quad1_w")
        .drop_nulls(),
        auto_ids,
        league=league,
        field_size=field_size,
    )
    slots = field_size - len(auto_ids)
    return (
        resume.join(proj, on=["season", "team_id"], how="left", validate="1:1")
        .with_columns(
            pl.col("auto_bid", "bid").fill_null(False),
            pl.col("team_id").is_in(sorted(champs)).alias("conf_champion"),
            _bubble_labels(pl.col("at_large_rank"), slots).alias("bubble"),
        )
        .select(list(BRACKETOLOGY_SCHEMA))
        .sort("season", "resume_score", descending=[False, True], nulls_last=True)
    )


# --- season Monte Carlo --------------------------------------------------------------------


def _bracket_order(size: int) -> np.ndarray:
    """1-based seeds in bracket slot order (1 v size, ...), size a power of two."""
    order = [1]
    while len(order) < size:
        n = 2 * len(order)
        order = [x for s in order for x in (s, n + 1 - s)]
    return np.array(order)


SEASON_SIM_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "as_of_date": pl.Date(),
    "team_id": pl.Utf8(),
    "team": pl.Utf8(),
    "conference_id": pl.Utf8(),
    "conference": pl.Utf8(),
    "wins": pl.Int64(),
    "losses": pl.Int64(),
    "conf_wins": pl.Int64(),
    "conf_losses": pl.Int64(),
    "proj_wins": pl.Float64(),
    "proj_losses": pl.Float64(),
    "proj_conf_wins": pl.Float64(),
    "proj_conf_losses": pl.Float64(),
    "proj_conf_win_pct": pl.Float64(),
    "proj_conf_win_pct_rank": pl.Float64(),
    "proj_conf_win_pct_pct": pl.Float64(),
    "reg_season_title_prob": pl.Float64(),
    "conf_tourney_title_prob": pl.Float64(),
    "auto_bid_prob": pl.Float64(),
    "at_large_prob": pl.Float64(),
    "bid_prob": pl.Float64(),
    "seed_mean": pl.Float64(),
    **{f"seed_{k}_prob": pl.Float64() for k in range(1, _SEED_LINES + 1)},
    "n_sims": pl.Int64(),
}


def simulate_season(
    played: pl.DataFrame,
    remaining: pl.DataFrame,
    teams: pl.DataFrame,
    resume: pl.DataFrame,
    *,
    league: str = "mens",
    n_sims: int = 10000,
    seed: int = 0,
    batch: int = 1000,
) -> pl.DataFrame:
    """The ``season_sim`` table: a seeded Monte Carlo of the rest of one season and its field.

    Each simulation draws the remaining regular-season games (``Phi(margin / sigma)`` from the
    as-of AdjEM, as :mod:`~sportsdataverse.mbb.mbb_season_sim`), crowns the regular-season
    champions (best conference winning percentage, co-champions all count), plays every
    unfinished conference tournament as a neutral-site single elimination of the members still
    alive seeded by conference record (byes to the top seeds; an ineligible champion passes the
    auto-bid to the runner-up), then rebuilds each team's
    résumé (WAB and Quad-1 wins move with the simulated results; ratings, SoS and opponent
    ranks stay at their as-of values) and selects and seeds the 68-team field as
    :func:`season_bracketology` does.

    ``numpy.random.default_rng(seed)`` drives every draw, in batches of ``batch`` sims, so a
    seed reproduces the table exactly.

    Args:
        played: Played games (:func:`split_season`).
        remaining: Games left (:func:`split_season`).
        teams: The Division I teams (:func:`college_teams`).
        resume: :func:`season_resume` over ``played``.
        league: ``"mens"`` or ``"womens"``.
        n_sims: Simulated seasons.
        seed: RNG seed.
        batch: Sims per batch, to bound memory. The draw order depends on it, so keep it
            fixed when comparing runs.

    Returns:
        One row per Division I team: current and projected W-L, the projected conference
        standing (``proj_conf_win_pct_rank`` within the conference, 1 best), title, bid and
        seed probabilities (``seed_k_prob`` is unconditional; ``seed_mean`` is over the sims
        with a bid).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_college_season import simulate_season
            sim = simulate_season(played, remaining, teams, resume, n_sims=2000, seed=7)
    """
    from sportsdataverse.mbb.mbb_bracketology import _RESUME_WEIGHTS

    c = get_constants(league)
    res = resume.sort("team_id")
    ids = res.get_column("team_id").to_list()
    idx = {t: i for i, t in enumerate(ids)}
    n = len(ids)
    # a team with no rating yet (preseason, or no box score) plays as the weakest rated team
    em_min = res.get_column("adj_em").min()
    em = res.get_column("adj_em").fill_null(em_min if em_min is not None else 0.0).to_numpy().astype(np.float64)
    net_rank = res.get_column("net_estimate_rank").fill_null(float(n)).to_numpy()
    conf = res.get_column("conference_id").to_list()
    has_conf = res.get_column("conference").is_not_null().to_numpy()
    eligible = res.get_column("eligible").to_numpy()

    # remaining games: D1 sides only matter for D1 tables, but a non-D1 opponent still adds a
    # result to the D1 team's W-L (rated at the D1 minimum when it has no rating).
    rem = with_conf_games(remaining, teams).filter(
        pl.col("home_team_id").is_in(ids) | pl.col("away_team_id").is_in(ids)
    )
    h = np.array([idx.get(t, -1) for t in rem.get_column("home_team_id").to_list()], dtype=np.int64)
    a = np.array([idx.get(t, -1) for t in rem.get_column("away_team_id").to_list()], dtype=np.int64)
    emin = float(em.min())
    em_h = np.where(h >= 0, em[np.maximum(h, 0)], emin)
    em_a = np.where(a >= 0, em[np.maximum(a, 0)], emin)
    neutral = rem.get_column("neutral_site").fill_null(False).to_numpy()
    p_home = norm.cdf((c.em_scale * (em_h - em_a) + c.hfa * (~neutral)) / c.margin_sd)
    conf_game = rem.get_column("conf_game").to_numpy()
    # the bubble team's win chance in each remaining game, and the Quad-1 flag, per side
    thr = c.quad_thresholds
    bub = lambda opp_em, hfa_sign: norm.cdf(  # noqa: E731
        (c.em_scale * (c.bubble_adj_em - opp_em) + hfa_sign * c.hfa) / c.margin_sd
    )
    sign_h = np.where(neutral, 0.0, 1.0)
    pb_h, pb_a = bub(em_a, sign_h), bub(em_h, -sign_h)
    venue_h = np.where(neutral, "neutral", "home")
    venue_a = np.where(neutral, "neutral", "away")
    q1_h = np.array([a_ >= 0 and net_rank[a_] <= thr[v]["q1"] for a_, v in zip(a, venue_h)], dtype=bool)
    q1_a = np.array([h_ >= 0 and net_rank[h_] <= thr[v]["q1"] for h_, v in zip(h, venue_a)], dtype=bool)
    both_d1 = (h >= 0) & (a >= 0)

    g = len(p_home)

    # incidence columns: [wins | conf wins | wab wins | q1 wins] for home and away sides
    def _inc(side: np.ndarray, mask: np.ndarray) -> np.ndarray:
        m: np.ndarray = np.zeros((g, n), dtype=np.float32)
        ok = (side >= 0) & mask
        m[np.flatnonzero(ok), side[ok]] = 1.0
        return m

    true_: np.ndarray = np.ones(g, dtype=bool)
    inc_h = np.concatenate([_inc(h, true_), _inc(h, conf_game), _inc(h, both_d1), _inc(h, both_d1 & q1_h)], axis=1)
    inc_a = np.concatenate([_inc(a, true_), _inc(a, conf_game), _inc(a, both_d1), _inc(a, both_d1 & q1_a)], axis=1)
    games_left = inc_h[:, :n].sum(axis=0) + inc_a[:, :n].sum(axis=0)
    conf_left = inc_h[:, n : 2 * n].sum(axis=0) + inc_a[:, n : 2 * n].sum(axis=0)
    pb_left = (pb_h[:, None] * inc_h[:, 2 * n : 3 * n]).sum(axis=0) + (pb_a[:, None] * inc_a[:, 2 * n : 3 * n]).sum(
        axis=0
    )

    col = lambda name: res.get_column(name).fill_null(0).to_numpy().astype(np.float64)  # noqa: E731
    wins0, losses0 = col("wins"), col("losses")
    cw0, cl0 = col("conf_wins"), col("conf_losses")
    wab0, q10 = res.get_column("wab").fill_null(0.0).to_numpy(), col("quad1_w")
    # SoS over every scheduled D1 game, not only the played ones
    opp_em_left = (em_a[:, None] * inc_h[:, 2 * n : 3 * n]).sum(axis=0) + (em_h[:, None] * inc_a[:, 2 * n : 3 * n]).sum(
        axis=0
    )
    d1_left = inc_h[:, 2 * n : 3 * n].sum(axis=0) + inc_a[:, 2 * n : 3 * n].sum(axis=0)
    g0 = col("rpi_games")
    sos0 = res.get_column("sos").fill_null(0.0).to_numpy()
    sos = np.where(g0 + d1_left > 0, (sos0 * g0 + opp_em_left) / np.maximum(g0 + d1_left, 1), 0.0)
    em_z = res.get_column("adj_em_z").fill_null(0.0).to_numpy()

    def zs(x: np.ndarray) -> np.ndarray:
        sd = x.std(axis=-1, ddof=1, keepdims=True)
        return (x - x.mean(axis=-1, keepdims=True)) / (sd + 1e-9)

    w = _RESUME_WEIGHTS
    status = _conference_status(played, teams).filter(pl.col("conference_id").is_in(conf))
    conf_ids = sorted({x for x, hc in zip(conf, has_conf) if hc})
    members = {k: np.array([i for i, x in enumerate(conf) if x == k]) for k in conf_ids}
    champ_of = {r["conference_id"]: r["champion_id"] for r in status.iter_rows(named=True)}
    winner_of = {r["conference_id"]: r["winner_id"] for r in status.iter_rows(named=True)}
    alive_of = {r["conference_id"]: [idx[t] for t in r["alive"] if t in idx] for r in status.iter_rows(named=True)}
    pb_neutral = norm.cdf((c.em_scale * (c.bubble_adj_em - em)) / c.margin_sd)  # vs each opponent, neutral
    q1_neutral = net_rank <= thr["neutral"]["q1"]

    rng = np.random.default_rng(seed)
    acc = {k: np.zeros(n) for k in ("wins", "cwins", "reg", "title", "auto", "al", "bid")}
    seed_counts = np.zeros((n, _SEED_LINES + 1))
    done = 0
    while done < n_sims:
        b = min(batch, n_sims - done)
        hw = (rng.random((b, g)) < p_home).astype(np.float32)
        got = hw @ inc_h + (1.0 - hw) @ inc_a  # (b, 4n)
        wins_rem, cw_rem, d1w_rem, q1_rem = got[:, :n], got[:, n : 2 * n], got[:, 2 * n : 3 * n], got[:, 3 * n :]
        cw = cw0 + cw_rem
        cl = cl0 + (conf_left - cw_rem)
        cpct = cw / np.maximum(cw + cl, 1.0)
        wab = wab0 + d1w_rem - pb_left
        q1 = q10 + q1_rem
        acc["wins"] += wins_rem.sum(axis=0)
        acc["cwins"] += cw_rem.sum(axis=0)
        auto: np.ndarray = np.zeros((b, n), dtype=bool)
        rows = np.arange(b)
        for k in conf_ids:
            mem = members[k]
            best = cpct[:, mem].max(axis=1, keepdims=True)
            acc["reg"][mem] += (cpct[:, mem] >= best - 1e-12).sum(axis=0)
            champ = champ_of.get(k)
            if champ is not None:
                if champ in idx:
                    auto[:, idx[champ]] = True
                if winner_of.get(k) in idx:
                    acc["title"][idx[winner_of[k]]] += b
                continue
            alive = np.array(alive_of.get(k) or [], dtype=np.int64)
            if alive.size == 0:
                continue
            # seed the alive members by simulated conference record, AdjEM breaking ties
            key = cpct[:, alive] + 1e-6 * (em[alive] - emin)[None, :]
            ranked = alive[np.argsort(-key, axis=1, kind="stable")]  # (b, m) seed 1 first
            m = alive.size
            size = 1 << max(0, int(np.ceil(np.log2(m))))
            order: np.ndarray = _bracket_order(size) - 1
            slot = np.where(order[None, :] < m, ranked[:, np.minimum(order, m - 1)], -1)
            runner_up: np.ndarray = np.full(b, -1, dtype=np.int64)
            while slot.shape[1] > 1:
                x, y = slot[:, 0::2], slot[:, 1::2]
                both = (x >= 0) & (y >= 0)
                px = norm.cdf(c.em_scale * (em[np.maximum(x, 0)] - em[np.maximum(y, 0)]) / c.margin_sd)
                xw = rng.random(x.shape) < px
                win = np.where(y < 0, x, np.where(x < 0, y, np.where(xw, x, y)))
                lose = np.where(win == x, y, x)
                r, cix = np.nonzero(both)
                wt, lt = win[r, cix], lose[r, cix]
                np.add.at(wab, (r, wt), 1.0 - pb_neutral[lt])
                np.add.at(wab, (r, lt), -pb_neutral[wt])
                np.add.at(q1, (r, wt), q1_neutral[lt].astype(np.float64))
                if win.shape[1] == 1:
                    runner_up = lose[:, 0]
                slot = win
            champ_sim = slot[:, 0]
            np.add.at(acc["title"], champ_sim, 1.0)
            # an ineligible champion passes the auto-bid to the runner-up
            holder = np.where(eligible[champ_sim] | (runner_up < 0), champ_sim, runner_up)
            ok = eligible[holder]
            auto[rows[ok], holder[ok]] = True
        score = w["adj_em_z"] * em_z[None, :] + w["sos"] * zs(sos)[None, :] + w["wab"] * zs(wab) + w["quad1_w"] * zs(q1)
        n_auto = auto.sum(axis=1)
        al_score = np.where(auto | ~eligible[None, :], -np.inf, score)
        order_al = np.argsort(-al_score, axis=1, kind="stable")
        al_rank = np.empty_like(order_al)
        al_rank[rows[:, None], order_al] = np.arange(n)[None, :]
        at_large = (~auto) & eligible[None, :] & (al_rank < (FIELD_SIZE - n_auto)[:, None])
        bid = auto | at_large
        for s in range(b):
            sd = _seed_field(score[s], bid[s], auto[s])
            seed_counts[np.arange(n), sd] += 1
        acc["auto"] += auto.sum(axis=0)
        acc["al"] += at_large.sum(axis=0)
        acc["bid"] += bid.sum(axis=0)
        done += b

    probs = seed_counts[:, 1:] / n_sims
    in_field = seed_counts[:, 1:].sum(axis=1)
    seed_mean = np.where(
        in_field > 0, (seed_counts[:, 1:] * np.arange(1, _SEED_LINES + 1)).sum(axis=1) / np.maximum(in_field, 1), np.nan
    )
    proj_w = wins0 + acc["wins"] / n_sims
    proj_cw = cw0 + acc["cwins"] / n_sims
    out = (
        res.select(
            "season",
            "as_of_date",
            "team_id",
            "team",
            "conference_id",
            "conference",
            "wins",
            "losses",
            "conf_wins",
            "conf_losses",
        )
        .with_columns(
            pl.Series("proj_wins", proj_w),
            pl.Series("proj_losses", losses0 + games_left - (proj_w - wins0)),
            pl.Series("proj_conf_wins", proj_cw),
            pl.Series("proj_conf_losses", cl0 + conf_left - (proj_cw - cw0)),
            pl.Series("reg_season_title_prob", np.where(has_conf, acc["reg"] / n_sims, np.nan)),
            pl.Series("conf_tourney_title_prob", acc["title"] / n_sims),
            pl.Series("auto_bid_prob", acc["auto"] / n_sims),
            pl.Series("at_large_prob", acc["al"] / n_sims),
            pl.Series("bid_prob", acc["bid"] / n_sims),
            pl.Series("seed_mean", seed_mean),
            *[pl.Series(f"seed_{k}_prob", probs[:, k - 1]) for k in range(1, _SEED_LINES + 1)],
            pl.lit(n_sims, dtype=pl.Int64).alias("n_sims"),
        )
        .with_columns(
            pl.col("reg_season_title_prob", "seed_mean").fill_nan(None),
            (pl.col("proj_conf_wins") / (pl.col("proj_conf_wins") + pl.col("proj_conf_losses")))
            .fill_nan(None)
            .alias("proj_conf_win_pct"),
        )
    )
    out = attach_ranks(
        out.with_columns(pl.when(pl.col("conference").is_not_null()).then(pl.col("conference_id")).alias("_cohort")),
        ["proj_conf_win_pct"],
        min_cohort=MIN_COHORT_TEAMS,
        by=("season", "_cohort"),
    )
    return out.select(list(SEASON_SIM_SCHEMA)).sort(
        "season", "conference", "proj_conf_win_pct_rank", "team_id", nulls_last=True
    )


class SeasonInputs(NamedTuple):
    """Everything the three daily tables share for one season and as-of date."""

    schedule: pl.DataFrame
    teams: pl.DataFrame
    played: pl.DataFrame
    remaining: pl.DataFrame
    resume: pl.DataFrame
    auto_bids: pl.DataFrame


def season_inputs(
    schedule: pl.DataFrame,
    team_box: pl.DataFrame,
    *,
    as_of_date: Optional[datetime.date] = None,
    league: str = "mens",
    ineligible: Iterable[str] = (),
) -> SeasonInputs:
    """Split, rate and résumé one season from a normalized schedule and its team box.

    Args:
        schedule: :func:`normalize_college_schedule` for one season.
        team_box: The ESPN team box for that season.
        as_of_date: Games strictly before this date count as played; ``None`` takes every
            completed game.
        league: ``"mens"`` or ``"womens"``.
        ineligible: Team ids barred from the tournament (:func:`college_teams`).

    Returns:
        A :class:`SeasonInputs`: the schedule, the Division I teams, the played and remaining
        games, the :func:`season_resume` table and the :func:`conference_auto_bids`.

    Raises:
        ValueError: If ``schedule`` holds more than one season.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_college_season import season_inputs
            inp = season_inputs(sched, team_box)
    """
    if schedule.get_column("season").n_unique() > 1:
        raise ValueError("season_inputs takes one season at a time")
    teams = college_teams(schedule, ineligible)
    played, remaining = split_season(schedule, as_of_date)
    resume = season_resume(played, team_box, teams, league=league, as_of_date=as_of_date)
    return SeasonInputs(schedule, teams, played, remaining, resume, conference_auto_bids(played, teams, resume))


def load_season_inputs(
    season: int,
    *,
    as_of_date: Optional[datetime.date] = None,
    league: str = "mens",
    ineligible: Iterable[str] = (),
) -> SeasonInputs:
    """:func:`season_inputs` from the released ESPN schedule and team box.

    Args:
        season: The season (e.g. ``2024``).
        as_of_date: Games strictly before this date count as played.
        league: ``"mens"`` or ``"womens"``.
        ineligible: Team ids barred from the tournament (:func:`college_teams`).

    Returns:
        A :class:`SeasonInputs`.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_college_season import load_season_inputs
            inp = load_season_inputs(2024)
    """
    from sportsdataverse.mbb.mbb_team_ratings import _league_loaders

    load_schedule, load_team_box = _league_loaders(league)
    sched = normalize_college_schedule(load_schedule([season]))
    return season_inputs(sched, load_team_box([season]), as_of_date=as_of_date, league=league, ineligible=ineligible)
