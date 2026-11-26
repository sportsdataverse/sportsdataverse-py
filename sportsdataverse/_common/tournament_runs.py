"""College tournament runs from the schedule, and their team and player totals (BB-D43 for MBB / WBB).

A **run** is one team's games in one tournament: the NCAA tournament (First Four onward), a
conference tournament, the NIT / WNIT, the CBI, the Crown, the WBIT and the other postseason
events. The league-agnostic cores are here; the entry points are in
:mod:`sportsdataverse.mbb.mbb_tournament_runs`. The team and player totals reuse the playoff
series' (:func:`sportsdataverse._common.playoff_series.team_box_totals` /
:func:`~sportsdataverse._common.playoff_series.player_box_totals`).

**Which games, and which tournament.** ESPN's schedule tags a tournament game with a
``tournament_id``. A game is in a run when it is a postseason game (``season_type`` 3) or a
regular-season game with a ``tournament_id`` whose ``groups`` is a conference (a conference
tournament played from mid-February, :data:`CONFERENCE_TOURNAMENT_FROM`). Early-season events
carry a ``tournament_id`` too (some with a conference group), in November and December, so they are
not runs. A postseason game with no ``tournament_id`` (older
seasons) is keyed by its headline's event name instead. A conference tournament ESPN did not tag
(most of them before the mid-2010s, and some women's tournaments since) is not a run: the schedule
does not say which games were in it.

**Keys.** ``tournament_key`` is the ``tournament_id`` as text, or the headline's event name as a
slug where the id is missing; ``run_id`` is ``"{season}-{tournament_key}-{team_id}"`` (ESPN team
id). A game is in two runs, one per side.

**Result.** A team is its tournament's ``champion`` when it won the tournament's last listed game
(by tip-off), that game is final and it is the only game on the tournament's last date;
``eliminated`` when it lost its last listed game; ``alive`` otherwise. A final still to play is
the last listed game, so nobody is champion before it, and a release whose last date holds two
semifinals (the final not listed yet) names no champion. ``run_result`` and ``run_games`` are the
run's outcome as of the release, on every row: never a feature for an earlier game.

A bracket slot whose team ESPN has not named yet (a null team id) is in no run. A postseason game
with neither a ``tournament_id`` nor a headline is in no run either.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse._common.playoff_series import (
    SERIES_PLAYERS_SCHEMA,
    SERIES_TEAMS_SCHEMA,
    player_box_totals,
    team_box_totals,
)

__all__ = [
    "CONFERENCE_TOURNAMENT_FROM",
    "NCAA_TOURNAMENT_ID",
    "RUN_GAMES_SCHEMA",
    "RUN_PLAYERS_SCHEMA",
    "RUN_TEAMS_SCHEMA",
    "tournament_run_games",
    "tournament_run_players",
    "tournament_run_teams",
]

#: ESPN's ``tournament_id`` for the NCAA tournament (men's and women's).
NCAA_TOURNAMENT_ID = 22
#: A tagged regular-season tournament game on or after February this day is a conference tournament game.
#: Measured on 2025: the earliest conference tournament game was 2025-02-26 (women) and 2025-03-02
#: (men); the latest early-season event game with a conference group was in December.
CONFERENCE_TOURNAMENT_FROM = 15

RUN_GAMES_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "run_id": pl.Utf8(),
    "tournament_key": pl.Utf8(),
    "tournament_id": pl.Int64(),
    "tournament_name": pl.Utf8(),
    "tournament_kind": pl.Utf8(),
    "conference_id": pl.Int64(),
    "round_name": pl.Utf8(),
    "game_number": pl.Int64(),
    "game_id": pl.Int64(),
    "game_date": pl.Date(),
    "completed": pl.Boolean(),
    "home_away": pl.Utf8(),
    "team_id": pl.Int64(),
    "team_abbreviation": pl.Utf8(),
    "team_display_name": pl.Utf8(),
    "team_score": pl.Int64(),
    "opponent_team_id": pl.Int64(),
    "opponent_abbreviation": pl.Utf8(),
    "opponent_display_name": pl.Utf8(),
    "opponent_score": pl.Int64(),
    "winner_team_id": pl.Int64(),
    "wins": pl.Int64(),
    "losses": pl.Int64(),
    "run_games": pl.Int64(),
    "run_result": pl.Utf8(),
}

_RUN_TEAM_TOTALS_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "run_id": pl.Utf8(),
    **{k: v for k, v in SERIES_TEAMS_SCHEMA.items() if k not in ("season", "series_id", "opponent_team_id")},
}
RUN_TEAMS_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "run_id": pl.Utf8(),
    "tournament_name": pl.Utf8(),
    "tournament_kind": pl.Utf8(),
    **{k: v for k, v in _RUN_TEAM_TOTALS_SCHEMA.items() if k not in ("season", "run_id")},
}

RUN_PLAYERS_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "run_id": pl.Utf8(),
    **{k: v for k, v in SERIES_PLAYERS_SCHEMA.items() if k not in ("season", "series_id")},
}

_SIDES = (("home", "away"), ("away", "home"))


def _event_name(headline: pl.Expr) -> pl.Expr:
    """The event in a game headline: the text before the first " - ", without a trailing " AT <site>"."""
    return headline.str.split(" - ").list.first().str.replace(r"\s+(?:AT|at)\s+.*$", "").str.strip_chars()


def _event_slug(event: pl.Expr) -> pl.Expr:
    """An event name as a key: lowercase, without "'s" and the generic words a headline varies on
    ("tournament", "championship", "series", "game", "playoffs"), non-alphanumerics as "-"."""
    words = r"\b(?:tournament|championships?|series|game|playoffs)\b"
    return (
        event.str.to_lowercase()
        .str.replace_all("'s", "s", literal=True)
        .str.replace_all(chr(0x2019) + "s", "s", literal=True)  # a typographic apostrophe
        .str.replace_all(words, " ")
        .str.replace_all(r"[^a-z0-9]+", "-")
        .str.strip_chars("-")
    )


def _key_tournaments(games: pl.DataFrame) -> pl.DataFrame:
    """``tournament_id`` filled from the season's tagged tournament of the same event name, ``tournament_key`` and
    ``tournament_kind``; a game with neither an id nor an event name is dropped."""
    # an untagged game (older seasons) joins the tagged tournament of its season with the same event name,
    # else its event name is its key; a game with neither (a blank headline) is not in a run
    tagged = (
        games.filter(pl.col("tournament_id").is_not_null() & (pl.col("slug") != ""))
        .group_by("season", "slug")
        .agg(pl.col("tournament_id").mode().sort().first().alias("tagged_id"))
    )
    return (
        games.join(tagged, on=["season", "slug"], how="left")
        .with_columns(
            pl.coalesce(pl.col("tournament_id"), pl.col("tagged_id")).alias("tournament_id"),
        )
        .with_columns(
            pl.coalesce(pl.col("tournament_id").cast(pl.Utf8), pl.col("slug")).alias("tournament_key"),
            pl.when(pl.col("tournament_id") == NCAA_TOURNAMENT_ID)
            .then(pl.lit("ncaa"))
            .when(pl.col("season_type") == 2)
            .then(pl.lit("conference"))
            .otherwise(pl.lit("postseason"))
            .alias("tournament_kind"),
        )
        .filter(pl.col("tournament_key") != "")
    )


def _sides(games: pl.DataFrame) -> pl.DataFrame:
    """Each tournament game twice, once from each side (``team_*`` / ``opponent_*``), with ``home_away``."""
    return pl.concat(
        [
            games.select(
                "season",
                "tournament_key",
                "tournament_id",
                "tournament_name",
                "tournament_kind",
                "conference_id",
                "round_name",
                "game_id",
                "game_date",
                "game_date_time",
                "completed",
                pl.when(pl.col("neutral_site").fill_null(False) == True)
                .then(pl.lit("neutral"))
                .otherwise(pl.lit(us))
                .alias("home_away"),
                pl.col(f"{us}_id").cast(pl.Int64).alias("team_id"),
                pl.col(f"{us}_abbreviation").alias("team_abbreviation"),
                pl.col(f"{us}_display_name").alias("team_display_name"),
                pl.col(f"{us}_score").cast(pl.Int64).alias("team_score"),
                pl.col(f"{them}_id").cast(pl.Int64).alias("opponent_team_id"),
                pl.col(f"{them}_abbreviation").alias("opponent_abbreviation"),
                pl.col(f"{them}_display_name").alias("opponent_display_name"),
                pl.col(f"{them}_score").cast(pl.Int64).alias("opponent_score"),
                "winner_team_id",
            )
            for us, them in _SIDES
        ]
    )


def tournament_run_games(schedule: pl.DataFrame) -> pl.DataFrame:
    """One row per team per tournament game, keyed to the team's run, with the run's record after it.

    Args:
        schedule: ESPN college schedule rows (``load_mbb_schedule`` / ``load_wbb_schedule``):
            ``game_id``, ``season``, ``season_type``, ``game_date``, ``game_date_time``,
            ``tournament_id``, ``groups_id``, ``groups_is_conference``, ``notes_headline``,
            ``neutral_site``, ``status_type_completed`` and the ``home_`` / ``away_`` ``id``,
            ``abbreviation``, ``display_name`` and ``score`` columns.

    Returns:
        :data:`RUN_GAMES_SCHEMA` rows, two per tournament game (one per side), ordered by season,
        run and game number. ``tournament_kind`` is ``ncaa`` (ESPN id 22), ``conference`` (a
        conference tournament; ``conference_id`` is its ESPN group id) or ``postseason``
        (the other postseason events). ``tournament_name`` is the event name most of its game
        headlines carry; ``round_name`` the rest of the game's headline. ``home_away`` is
        ``neutral`` at a neutral site. ``wins`` / ``losses`` are the run's record after the game,
        ``run_games`` the games listed in the run (played and scheduled), and ``run_result``
        (``champion``, ``eliminated`` or ``alive``) repeats on every row of the run: the run's
        outcome as of the release, not as of that game. A side with a null team id is in no run.

    Raises:
        polars.exceptions.ColumnNotFoundError: If ``schedule`` lacks a column named above.

    See Also:
        :func:`tournament_run_teams`, :func:`tournament_run_players`, and
        :func:`sportsdataverse.mbb.mbb_tournament_runs.mbb_tournament_runs` (the release reader).

    Example:
        From a loaded schedule::

            from sportsdataverse.mbb import load_mbb_schedule
            from sportsdataverse._common.tournament_runs import tournament_run_games
            runs = tournament_run_games(load_mbb_schedule([2025]))
    """
    conference = (pl.col("season_type") == 2) & pl.col("tournament_id").is_not_null()
    conference = conference & (pl.col("groups_is_conference").fill_null(False) == True)
    # early-season events can carry a conference group too; conference tournaments start in late February
    conference = conference & (
        pl.col("game_date") >= pl.date(pl.col("season").cast(pl.Int32), 2, CONFERENCE_TOURNAMENT_FROM)
    )
    games = schedule.filter((pl.col("season_type") == 3) | conference).unique(subset=["game_id"], keep="first")
    if games.height == 0:
        return pl.DataFrame(schema=RUN_GAMES_SCHEMA)
    event = _event_name(pl.col("notes_headline").fill_null(""))
    games = games.with_columns(
        pl.col("season").cast(pl.Int64),
        pl.col("tournament_id").cast(pl.Int64),
        _event_slug(event).alias("slug"),
        event.alias("event"),
        pl.col("notes_headline")
        .fill_null("")
        .str.split(" - ")
        .list.slice(1)
        .list.join(" - ")
        .str.replace(r"\s+(?:AT|at)\s+.*$", "")
        .str.strip_chars()
        .alias("round_name"),
        pl.when(pl.col("season_type") == 2).then(pl.col("groups_id")).cast(pl.Int64).alias("conference_id"),
        pl.col("status_type_completed").fill_null(False).alias("completed"),
    )
    games = _key_tournaments(games)
    games = games.with_columns(
        pl.col("event")
        .filter(pl.col("event") != "")
        .mode()
        .sort()
        .first()
        .over("season", "tournament_key")
        .alias("tournament_name"),
        pl.when(pl.col("completed") == True)
        .then(
            pl.when(pl.col("home_score") > pl.col("away_score"))
            .then(pl.col("home_id"))
            .when(pl.col("away_score") > pl.col("home_score"))
            .then(pl.col("away_id"))
        )
        .cast(pl.Int64)
        .alias("winner_team_id"),
    )
    rows = _sides(games)
    rows = (
        # a bracket slot whose team is not known yet is in no run (it would pool into one null run)
        rows.filter(pl.col("team_id").is_not_null())
        .with_columns(pl.format("{}-{}-{}", "season", "tournament_key", "team_id").alias("run_id"))
        .sort("season", "run_id", "game_date_time", "game_id", nulls_last=True)
        .with_columns(
            pl.int_range(1, pl.len() + 1).over("run_id").alias("game_number"),
            (pl.col("winner_team_id") == pl.col("team_id")).fill_null(False).cum_sum().over("run_id").alias("wins"),
            ((pl.col("winner_team_id") != pl.col("team_id")) & (pl.col("completed") == True))
            .fill_null(False)
            .cum_sum()
            .over("run_id")
            .alias("losses"),
            pl.len().over("run_id").alias("run_games"),
        )
    )
    return rows.join(_run_results(rows), on="run_id", how="left").select(list(RUN_GAMES_SCHEMA)).cast(RUN_GAMES_SCHEMA)


def _run_results(rows: pl.DataFrame) -> pl.DataFrame:
    """``(run_id, run_result)``: the champion won the tournament's last listed game, when that game is the only one
    on the tournament's last date (a game to play has no winner; two semifinals on the last date mean the final is
    not in the release yet)."""
    tournament = ["season", "tournament_key"]
    last_game = (
        rows.sort("game_date_time", "game_id", nulls_last=True)
        .group_by(tournament)
        .agg(
            pl.col("winner_team_id").last().alias("last_winner"),
            pl.col("game_id").filter(pl.col("game_date") == pl.col("game_date").max()).n_unique().alias("last_day"),
        )
    )
    champion = last_game.filter(pl.col("last_winner").is_not_null() & (pl.col("last_day") == 1)).select(
        *tournament, pl.col("last_winner").alias("team_id"), pl.lit(True).alias("champion")
    )
    per = rows.group_by("run_id").agg(
        pl.col("season").first(),
        pl.col("tournament_key").first(),
        pl.col("team_id").first(),
        pl.col("completed").last().alias("last_done"),
        (pl.col("winner_team_id").last() != pl.col("team_id").last()).alias("last_lost"),
    )
    per = per.join(champion, on=[*tournament, "team_id"], how="left")
    return per.select(
        "run_id",
        pl.when(pl.col("champion").fill_null(False) == True)
        .then(pl.lit("champion"))
        .when((pl.col("last_done") == True) & (pl.col("last_lost").fill_null(False) == True))
        .then(pl.lit("eliminated"))
        .otherwise(pl.lit("alive"))
        .alias("run_result"),
    )


def _finals(games: pl.DataFrame) -> pl.DataFrame:
    return games.filter(pl.col("completed") == True).select("season", "run_id", "game_id", "team_id", "winner_team_id")


def tournament_run_teams(games: pl.DataFrame, team_box: pl.DataFrame) -> pl.DataFrame:
    """Each run's record and box totals: the sum of the team's final game boxes in the tournament.

    Args:
        games: :func:`tournament_run_games` output.
        team_box: ESPN team box rows (``load_mbb_team_boxscore``): ``game_id``, ``team_id``,
            ``team_abbreviation``, ``team_score``, ``opponent_team_id``, ``opponent_team_score``,
            ``total_rebounds`` and the other box counts.

    Returns:
        :data:`RUN_TEAMS_SCHEMA` rows, one per run with a final game: the run's
        ``tournament_name`` and ``tournament_kind`` (as on its :func:`tournament_run_games` rows),
        ``games``, ``wins``, ``losses``, ``pts``, ``opp_pts`` and the summed counts (as
        :func:`~sportsdataverse._common.playoff_series.playoff_series_teams`). A box row listed
        twice counts once.

    Raises:
        polars.exceptions.ColumnNotFoundError: If ``team_box`` lacks a column named above.

    See Also:
        :func:`tournament_run_games`, :func:`tournament_run_players`.

    Example:
        From the loaded releases::

            from sportsdataverse.mbb import load_mbb_schedule, load_mbb_team_boxscore
            from sportsdataverse._common.tournament_runs import tournament_run_games, tournament_run_teams
            runs = tournament_run_games(load_mbb_schedule([2025]))
            teams = tournament_run_teams(runs, load_mbb_team_boxscore([2025]))
    """
    totals = team_box_totals(
        _finals(games), team_box, key="run_id", on=["game_id", "team_id"], schema=_RUN_TEAM_TOTALS_SCHEMA
    )
    # one tournament per run, so its name and kind are the same on every one of the run's game rows
    names = games.select("run_id", "tournament_name", "tournament_kind").unique("run_id", keep="first")
    return (
        totals.join(names, on="run_id", how="left", maintain_order="left")
        .select(list(RUN_TEAMS_SCHEMA))
        .cast(RUN_TEAMS_SCHEMA)
    )


def tournament_run_players(games: pl.DataFrame, player_box: pl.DataFrame) -> pl.DataFrame:
    """Each player's totals and per-game line over a run, against the player's regular season.

    Args:
        games: :func:`tournament_run_games` output.
        player_box: ESPN player box rows for the season (``load_mbb_player_boxscore``), regular
            season and the tournament games: ``game_id``, ``season``, ``season_type``, ``athlete_id``,
            ``athlete_display_name``, ``team_id``, ``minutes``, ``did_not_play`` and the
            :data:`~sportsdataverse._common.playoff_series.SERIES_BOX_STATS` columns.

    Returns:
        :data:`RUN_PLAYERS_SCHEMA` rows, one per player who played in a final game of the run,
        with the same columns and rules as
        :func:`~sportsdataverse._common.playoff_series.playoff_series_players` (a row counts when
        not marked did-not-play and with minutes or any box count). The regular-season line is
        ``season_type`` 2 without the conference-tournament games (ESPN types those as regular
        season), so a run is never compared with itself.

    Raises:
        polars.exceptions.ColumnNotFoundError: If ``player_box`` lacks a column named above.

    See Also:
        :func:`tournament_run_games`, :func:`tournament_run_teams`.

    Example:
        From the loaded releases::

            from sportsdataverse.mbb import load_mbb_player_boxscore, load_mbb_schedule
            from sportsdataverse._common.tournament_runs import tournament_run_games, tournament_run_players
            runs = tournament_run_games(load_mbb_schedule([2025]))
            players = tournament_run_players(runs, load_mbb_player_boxscore([2025]))
    """
    finals = _finals(games).drop("winner_team_id")
    conference = games.filter(pl.col("tournament_kind") == "conference")["game_id"].unique()
    box = player_box.with_columns(
        pl.when(pl.col("game_id").cast(pl.Int64).is_in(conference.implode()))
        .then(pl.lit(3))
        .otherwise(pl.col("season_type"))
        .cast(pl.Int64)
        .alias("season_type")
    )
    return player_box_totals(finals, box, key="run_id", on=["game_id", "team_id"], schema=RUN_PLAYERS_SCHEMA)
