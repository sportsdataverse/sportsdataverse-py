"""Playoff series from the schedule, and their team and player totals (BB-D43).

League-agnostic cores over the ESPN schedule and box score releases; the NBA / WNBA entry points
are in :mod:`sportsdataverse.nba.nba_playoff_series`.

A **series** is every postseason game (ESPN ``season_type`` 3) between one pair of teams in one
season: in these leagues a pair meets at most once per postseason, so the pair is the key.
Play-in games are ESPN ``season_type`` 5 and are not a series. The series id is
``"{season}-{lower team id}-{higher team id}"`` (ESPN team ids), so it is stable whatever order
the games arrive in. Games are numbered by tip-off within the series.

``team_a`` is the home team of game 1 (the side with home court), ``team_b`` the other. A series
is ``decided`` once its leader has the wins to clinch (:func:`wins_to_clinch`) or has a later
series that postseason; the round comes from ESPN's ``type_abbreviation`` where it has one.
"""

from __future__ import annotations

import polars as pl

__all__ = [
    "PLAYER_SHIFT_STATS",
    "ROUND_CODES",
    "SERIES_BOX_STATS",
    "SERIES_GAMES_SCHEMA",
    "playoff_series_games",
    "playoff_series_players",
    "playoff_series_teams",
    "wins_to_clinch",
]

#: ESPN ``type_abbreviation`` -> round number (1 = first round). Older seasons carry ``STD`` and
#: no round; their ``round`` is null.
ROUND_CODES: dict[str, int] = {"RD16": 1, "QTR": 2, "SEMI": 3, "FINAL": 4}

#: Box columns summed per team and per player: output name -> ESPN box column. The player box
#: names its total rebounds ``rebounds``, the team box ``total_rebounds``.
SERIES_BOX_STATS: dict[str, str] = {
    "pts": "points",
    "fgm": "field_goals_made",
    "fga": "field_goals_attempted",
    "fg3m": "three_point_field_goals_made",
    "fg3a": "three_point_field_goals_attempted",
    "ftm": "free_throws_made",
    "fta": "free_throws_attempted",
    "oreb": "offensive_rebounds",
    "dreb": "defensive_rebounds",
    "reb": "rebounds",
    "ast": "assists",
    "stl": "steals",
    "blk": "blocks",
    "tov": "turnovers",
    "pf": "fouls",
}

#: The per-game stats compared between the regular season and the series (``<stat>_pg``,
#: ``reg_<stat>_pg``, ``<stat>_pg_diff``); ``ts_pct`` is compared the same way.
PLAYER_SHIFT_STATS: tuple[str, ...] = ("min", "pts", "reb", "ast", "stl", "blk", "tov", "fg3m")

SERIES_GAMES_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "series_id": pl.Utf8(),
    "round": pl.Int64(),
    "round_code": pl.Utf8(),
    "round_name": pl.Utf8(),
    "game_number": pl.Int64(),
    "game_id": pl.Int64(),
    "game_date": pl.Date(),
    "completed": pl.Boolean(),
    "home_team_id": pl.Int64(),
    "home_abbreviation": pl.Utf8(),
    "home_display_name": pl.Utf8(),
    "home_score": pl.Int64(),
    "away_team_id": pl.Int64(),
    "away_abbreviation": pl.Utf8(),
    "away_display_name": pl.Utf8(),
    "away_score": pl.Int64(),
    "winner_team_id": pl.Int64(),
    "team_a_id": pl.Int64(),
    "team_b_id": pl.Int64(),
    "team_a_wins": pl.Int64(),
    "team_b_wins": pl.Int64(),
    "series_games": pl.Int64(),
    "decided": pl.Boolean(),
    "series_winner_team_id": pl.Int64(),
}

_TEAM_KEYS = {"season": pl.Int64(), "series_id": pl.Utf8(), "team_id": pl.Int64(), "opponent_team_id": pl.Int64()}
_COUNTS = {"games": pl.Int64(), "wins": pl.Int64(), "losses": pl.Int64(), "opp_pts": pl.Int64()}
SERIES_TEAMS_SCHEMA: dict[str, pl.DataType] = {
    **_TEAM_KEYS,
    "team_abbreviation": pl.Utf8(),
    **_COUNTS,
    **{k: pl.Int64() for k in SERIES_BOX_STATS},
}

SERIES_PLAYERS_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "series_id": pl.Utf8(),
    "team_id": pl.Int64(),
    "athlete_id": pl.Int64(),
    "athlete_display_name": pl.Utf8(),
    "games": pl.Int64(),
    "min": pl.Float64(),
    **{k: pl.Int64() for k in SERIES_BOX_STATS},
    **{f"{s}_pg": pl.Float64() for s in PLAYER_SHIFT_STATS},
    "ts_pct": pl.Float64(),
    "reg_games": pl.Int64(),
    **{f"reg_{s}_pg": pl.Float64() for s in PLAYER_SHIFT_STATS},
    "reg_ts_pct": pl.Float64(),
    **{f"{s}_pg_diff": pl.Float64() for s in PLAYER_SHIFT_STATS},
    "ts_pct_diff": pl.Float64(),
}


def wins_to_clinch(league: str, season: int, *, final: bool) -> int | None:
    """Wins that settle a series, where the format is fixed for the round; None where it varies.

    NBA: every round has been best-of-seven since 2003; before that the first round was
    best-of-five, and only the Finals are taken as fixed. WNBA: the early rounds have changed
    format often (single games, best-of-three, best-of-five), so only the Finals are fixed:
    best-of-three through 2004, best-of-five 2005-2024, best-of-seven from 2025. A series without
    a fixed format is decided when its leader plays a later series.

    Args:
        league: ``"nba"`` or ``"wnba"``.
        season: The season, named by the year it ends.
        final: Whether the series is the Finals.

    Returns:
        The wins that clinch the series, or None where the round's format is not fixed.

    Example:
        Quick start::

            from sportsdataverse._common.playoff_series import wins_to_clinch
            wins_to_clinch("wnba", 2024, final=True)  # 3: the Finals were best-of-five
    """
    if league == "nba":
        return 4 if (season >= 2003 or final) else None
    if not final:
        return None
    return 2 if season <= 2004 else 3 if season <= 2024 else 4


def playoff_series_games(schedule: pl.DataFrame, *, league: str) -> pl.DataFrame:
    """One row per postseason game, keyed to its series, with the series score after it.

    Args:
        schedule: ESPN schedule rows (``load_nba_schedule`` / ``load_wnba_schedule``): ``game_id``,
            ``season``, ``season_type``, ``game_date``, ``game_date_time``, ``type_abbreviation``,
            ``notes_headline``, ``status_type_completed`` and the ``home_`` / ``away_`` ``id``,
            ``abbreviation``, ``display_name``, ``score`` and ``winner`` columns.
        league: ``"nba"`` or ``"wnba"`` (the clinch rule, :func:`wins_to_clinch`).

    Returns:
        :data:`SERIES_GAMES_SCHEMA` rows, one per ``season_type`` 3 game, ordered by season,
        series and game number. ``winner_team_id`` is null until the game is final;
        ``team_a_wins`` / ``team_b_wins`` count the finals up to and including this game, and
        ``series_games`` / ``decided`` / ``series_winner_team_id`` repeat on every row of the
        series. ``series_games`` counts the games listed in the series, played and scheduled.
        ``round_name`` is the game headline without its "Game N".

    Raises:
        polars.exceptions.ColumnNotFoundError: If ``schedule`` lacks a column named above.

    See Also:
        :func:`playoff_series_teams`, :func:`playoff_series_players`, and
        :func:`sportsdataverse.nba.nba_playoff_series.nba_playoff_series` (the release reader).

    Example:
        From a loaded schedule::

            from sportsdataverse.nba import load_nba_schedule
            from sportsdataverse._common.playoff_series import playoff_series_games
            games = playoff_series_games(load_nba_schedule([2025]), league="nba")
    """
    po = schedule.filter(pl.col("season_type") == 3)
    if po.height == 0:
        return pl.DataFrame(schema=SERIES_GAMES_SCHEMA)
    lo = pl.min_horizontal("home_id", "away_id").cast(pl.Int64)
    hi = pl.max_horizontal("home_id", "away_id").cast(pl.Int64)
    completed = pl.col("status_type_completed").fill_null(False)
    winner = (
        pl.when(completed == False)
        .then(None)
        .when(pl.col("home_score") > pl.col("away_score"))
        .then(pl.col("home_id"))
        .when(pl.col("away_score") > pl.col("home_score"))
        .then(pl.col("away_id"))
        .cast(pl.Int64)
    )
    games = (
        po.unique(subset=["game_id"], keep="first")
        .with_columns(
            pl.col("season").cast(pl.Int64),
            pl.format("{}-{}-{}", pl.col("season"), lo, hi).alias("series_id"),
            completed.alias("completed"),
            winner.alias("winner_team_id"),
            pl.col("notes_headline")
            .str.replace(r"(?i)\s*-\s*game\s+\d+\s*$", "")
            .str.strip_chars()
            .alias("round_name"),
        )
        .sort("season", "series_id", "game_date_time", "game_id", nulls_last=True)
        .with_columns(
            pl.int_range(1, pl.len() + 1).over("series_id").alias("game_number"),
            pl.col("home_id").first().over("series_id").cast(pl.Int64).alias("team_a_id"),
            pl.col("away_id").first().over("series_id").cast(pl.Int64).alias("team_b_id"),
            # the latest headline names the round best (game 1 is sometimes blank)
            pl.col("round_name").replace("", None).drop_nulls().last().over("series_id").alias("round_name"),
            pl.col("type_abbreviation").replace_strict(ROUND_CODES, default=None, return_dtype=pl.Int64).alias("round"),
            pl.when(pl.col("type_abbreviation").is_in(list(ROUND_CODES)))
            .then(pl.col("type_abbreviation"))
            .alias("round_code"),
        )
        .with_columns(
            (pl.col("winner_team_id") == pl.col("team_a_id"))
            .fill_null(False)
            .cum_sum()
            .over("series_id")
            .alias("team_a_wins"),
            (pl.col("winner_team_id") == pl.col("team_b_id"))
            .fill_null(False)
            .cum_sum()
            .over("series_id")
            .alias("team_b_wins"),
            pl.len().over("series_id").alias("series_games"),
        )
    )
    games = games.join(_series_outcomes(games, league), on="series_id", how="left")
    return (
        games.rename({"home_id": "home_team_id", "away_id": "away_team_id"})
        .select(list(SERIES_GAMES_SCHEMA))
        .cast(SERIES_GAMES_SCHEMA)
    )


def _series_outcomes(games: pl.DataFrame, league: str) -> pl.DataFrame:
    """``(series_id, decided, series_winner_team_id)``: the leader has clinched, or plays on."""
    per = games.group_by("series_id").agg(
        pl.col("season").first(),
        pl.col("round_code").first(),
        pl.col("team_a_id").first(),
        pl.col("team_b_id").first(),
        pl.col("team_a_wins").max(),
        pl.col("team_b_wins").max(),
        pl.col("game_date").min().alias("first_date"),
        pl.col("game_date").filter(pl.col("completed") == True).max().alias("last_date"),
    )
    # the season's latest-starting series is its final where ESPN gives no round (older seasons)
    latest = pl.col("first_date") == pl.col("first_date").max().over("season")
    per = per.with_columns(
        pl.when(pl.col("team_a_wins") > pl.col("team_b_wins"))
        .then(pl.col("team_a_id"))
        .when(pl.col("team_b_wins") > pl.col("team_a_wins"))
        .then(pl.col("team_b_id"))
        .alias("leader"),
        pl.max_horizontal("team_a_wins", "team_b_wins").alias("lead_wins"),
        ((pl.col("round_code") == "FINAL") | (pl.col("round_code").is_null() & latest)).fill_null(False).alias("final"),
    )
    later = (
        per.select("season", "series_id", "last_date", "leader")
        .join(
            pl.concat(
                [
                    per.select("season", pl.col("team_a_id").alias("leader"), pl.col("first_date").alias("next_first")),
                    per.select("season", pl.col("team_b_id").alias("leader"), pl.col("first_date").alias("next_first")),
                ]
            ),
            on=["season", "leader"],
        )
        .filter(pl.col("next_first") > pl.col("last_date"))
        .select("series_id")
        .unique()
        .with_columns(pl.lit(True).alias("plays_on"))
    )
    clinch = [wins_to_clinch(league, int(s), final=bool(f)) for s, f in zip(per["season"], per["final"], strict=True)]
    per = (
        per.with_columns(pl.Series("clinch", clinch, dtype=pl.Int64))
        .join(later, on="series_id", how="left")
        .with_columns(
            (
                pl.col("leader").is_not_null()
                & (pl.col("plays_on").fill_null(False) | (pl.col("lead_wins") >= pl.col("clinch")).fill_null(False))
            ).alias("decided")
        )
    )
    return per.select(
        "series_id",
        "decided",
        pl.when(pl.col("decided") == True).then(pl.col("leader")).cast(pl.Int64).alias("series_winner_team_id"),
    )


def playoff_series_teams(games: pl.DataFrame, team_box: pl.DataFrame) -> pl.DataFrame:
    """Each side's series record and box totals: the sum of its final game boxes in the series.

    Args:
        games: :func:`playoff_series_games` output.
        team_box: ESPN team box rows (``load_nba_team_boxscore``): ``game_id``, ``team_id``,
            ``team_abbreviation``, ``team_score``, ``opponent_team_id``, ``opponent_team_score``,
            ``total_rebounds`` and the other :data:`SERIES_BOX_STATS` columns.

    Returns:
        :data:`SERIES_TEAMS_SCHEMA` rows, two per series with a final game (a series whose games
        are all still to play has none): ``games``, ``wins``, ``losses``, ``pts`` and ``opp_pts``
        over its final games, and the summed box counts. A box row listed twice in the release
        is counted once (one row per ``(game_id, team_id)``).

    Raises:
        polars.exceptions.ColumnNotFoundError: If ``team_box`` lacks a column named above.

    See Also:
        :func:`playoff_series_games`, :func:`playoff_series_players`.

    Example:
        From the loaded releases::

            from sportsdataverse.nba import load_nba_schedule, load_nba_team_boxscore
            from sportsdataverse._common.playoff_series import playoff_series_games, playoff_series_teams
            games = playoff_series_games(load_nba_schedule([2025]), league="nba")
            teams = playoff_series_teams(games, load_nba_team_boxscore([2025]))
    """
    finals = games.filter(pl.col("completed") == True)
    finals = finals.select("season", "series_id", "game_id", "winner_team_id")
    if finals.height == 0:
        return pl.DataFrame(schema=SERIES_TEAMS_SCHEMA)
    box = (
        team_box.unique(subset=["game_id", "team_id"], keep="first", maintain_order=True)
        .rename({"total_rebounds": "rebounds", "team_score": "points"})
        .with_columns(pl.col("game_id", "team_id", "opponent_team_id").cast(pl.Int64))
    )
    rows = finals.join(box, on="game_id", how="inner")
    out = rows.group_by("season", "series_id", "team_id").agg(
        pl.col("opponent_team_id").first(),
        pl.col("team_abbreviation").sort_by("game_id").last(),
        pl.len().alias("games"),
        (pl.col("winner_team_id") == pl.col("team_id")).sum().alias("wins"),
        (pl.col("winner_team_id") != pl.col("team_id")).sum().alias("losses"),
        pl.col("opponent_team_score").sum().alias("opp_pts"),
        *[pl.col(src).sum().alias(k) for k, src in SERIES_BOX_STATS.items()],
    )
    return out.select(list(SERIES_TEAMS_SCHEMA)).cast(SERIES_TEAMS_SCHEMA).sort("season", "series_id", "team_id")


def _played(box: pl.DataFrame) -> pl.DataFrame:
    """Box rows the player took the floor in: not marked did-not-play, with minutes or any box count.

    ESPN lists some cameos at 0.0 minutes with a stat (a free throw, a rebound, a foul); those
    count. A row listed twice in the release counts once (one row per ``(game_id, athlete_id)``).
    """
    counted = pl.sum_horizontal([pl.col(c).fill_null(0) for c in SERIES_BOX_STATS.values()]) > 0
    return box.unique(subset=["game_id", "athlete_id"], keep="first", maintain_order=True).filter(
        (pl.col("did_not_play").fill_null(False) == False) & ((pl.col("minutes").fill_null(0) > 0) | counted)
    )


def _ts(pts: pl.Expr, fga: pl.Expr, fta: pl.Expr) -> pl.Expr:
    """True shooting: points over two shots per (FGA + 0.44 FTA); null with no attempts."""
    att = fga + 0.44 * fta
    return pl.when(att > 0).then(pts / (2 * att))


def playoff_series_players(games: pl.DataFrame, player_box: pl.DataFrame) -> pl.DataFrame:
    """Each player's series totals and per-game line, against the player's regular season per game.

    Args:
        games: :func:`playoff_series_games` output.
        player_box: ESPN player box rows for the season, regular season (``season_type`` 2) and
            postseason (``load_nba_player_boxscore``): ``game_id``, ``season``, ``season_type``,
            ``athlete_id``, ``athlete_display_name``, ``team_id``, ``minutes``, ``did_not_play``
            and the :data:`SERIES_BOX_STATS` columns.

    Returns:
        :data:`SERIES_PLAYERS_SCHEMA` rows, one per player who played in a final game of the
        series. ``games`` and the totals count the games played (not marked did-not-play, with
        minutes or any box count; a 0.0-minute cameo with a stat counts); ``<stat>_pg`` divides
        by those games, ``reg_<stat>_pg`` is the same over the player's regular season (every
        team played for; null with no regular-season game) and ``<stat>_pg_diff`` is series
        minus regular season. ``ts_pct`` is true shooting, ``PTS / (2 * (FGA + 0.44 * FTA))``.

    Raises:
        polars.exceptions.ColumnNotFoundError: If ``player_box`` lacks a column named above.

    See Also:
        :func:`playoff_series_games`, :func:`playoff_series_teams`.

    Example:
        From the loaded releases::

            from sportsdataverse.nba import load_nba_player_boxscore, load_nba_schedule
            from sportsdataverse._common.playoff_series import playoff_series_games, playoff_series_players
            games = playoff_series_games(load_nba_schedule([2025]), league="nba")
            players = playoff_series_players(games, load_nba_player_boxscore([2025]))
    """
    finals = games.filter(pl.col("completed") == True)
    finals = finals.select("season", "series_id", "game_id")
    if finals.height == 0:
        return pl.DataFrame(schema=SERIES_PLAYERS_SCHEMA)
    box = (
        _played(player_box)
        .rename({"minutes": "min_"})
        .with_columns(pl.col("game_id", "team_id", "athlete_id").cast(pl.Int64), pl.col("season").cast(pl.Int64))
    )
    sums = [pl.col("min_").sum().alias("min"), *[pl.col(src).sum().alias(k) for k, src in SERIES_BOX_STATS.items()]]
    po = (
        finals.join(box.drop("season"), on="game_id", how="inner")
        .group_by("season", "series_id", "team_id", "athlete_id")
        .agg(pl.col("athlete_display_name").sort_by("game_id").last(), pl.len().alias("games"), *sums)
    )
    reg = (
        box.filter(pl.col("season_type") == 2)
        .group_by("season", "athlete_id")
        .agg(pl.len().alias("reg_games"), *sums)
        .select(
            "season",
            "athlete_id",
            "reg_games",
            *[(pl.col(s) / pl.col("reg_games")).alias(f"reg_{s}_pg") for s in PLAYER_SHIFT_STATS],
            _ts(pl.col("pts"), pl.col("fga"), pl.col("fta")).alias("reg_ts_pct"),
        )
    )
    out = (
        po.with_columns(
            *[(pl.col(s) / pl.col("games")).alias(f"{s}_pg") for s in PLAYER_SHIFT_STATS],
            _ts(pl.col("pts"), pl.col("fga"), pl.col("fta")).alias("ts_pct"),
        )
        .join(reg, on=["season", "athlete_id"], how="left")
        .with_columns(
            *[(pl.col(f"{s}_pg") - pl.col(f"reg_{s}_pg")).alias(f"{s}_pg_diff") for s in PLAYER_SHIFT_STATS],
            (pl.col("ts_pct") - pl.col("reg_ts_pct")).alias("ts_pct_diff"),
        )
    )
    return (
        out.select(list(SERIES_PLAYERS_SCHEMA))
        .cast(SERIES_PLAYERS_SCHEMA)
        .sort("season", "series_id", "team_id", "pts", "athlete_id", descending=[False, False, False, True, False])
    )
