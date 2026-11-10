"""Official season profiles (BB-D34): the shared core behind ``{lg}_official_seasons``.

One row per official and season, over the completed games of the season type that the official
worked (ESPN's ``gameInfo.officials`` release, ``load_{lg}_officials``):

* ``games`` -- the games the officials table lists the official on;
* ``fouls_per_game`` / ``fta_per_game`` -- both teams' fouls and free-throw attempts per game,
  from ESPN's team box (``fouls`` and ``free_throws_attempted``);
* ``pace`` -- possessions per regulation length (48 minutes NBA, 40 the other three):
  ``regulation * sum(possessions) / sum(game minutes)``, possessions being Dean Oliver's box
  estimate FGA - ORB + TOV + 0.44 FTA averaged over the two teams (the turnover repairs of
  :func:`sportsdataverse.nba.nba_game_page.team_game_rates`), the minutes the schedule's last
  period (regulation plus five per overtime);
* ``home_win_pct`` and ``home_minus_away_fouls`` (the home team's fouls less the away team's,
  per game) -- over the games not at a neutral site (the schedule's ``neutral_site``).

Every rate carries ``<m>_n``, the games it is computed over (BB-D2): a game without a two-team
box drops out of the box rates, a game without a usable last period out of ``pace``, a neutral
site game out of the two home rates. They are not ranked.

An official is keyed by ESPN's ``official_id`` where the release carries one (WNBA, WBB) and by
name elsewhere (NBA, MBB carry names only); the name is whitespace-normalised.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

import polars as pl

__all__ = [
    "GAME_ROWS_SCHEMA",
    "OFFICIALS_SCHEMA",
    "OFFICIAL_SEASONS_SCHEMA",
    "league_official_seasons",
    "official_game_rows",
    "official_seasons",
    "officials_rows",
]

#: One completed game: the box sums and the schedule facts the profile reads.
GAME_ROWS_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "game_id": pl.Utf8(),
    "neutral": pl.Boolean(),
    "home_win": pl.Boolean(),
    "home_fouls": pl.Int64(),
    "away_fouls": pl.Int64(),
    "fta": pl.Int64(),
    "possessions": pl.Float64(),
    "game_minutes": pl.Int64(),
}

#: One official on one game.
OFFICIALS_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Utf8(),
    "official_id": pl.Int64(),
    "official_name": pl.Utf8(),
}

#: The returned profile.
OFFICIAL_SEASONS_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "official_id": pl.Int64(),
    "official_name": pl.Utf8(),
    "games": pl.Int64(),
    "fouls_per_game": pl.Float64(),
    "fouls_per_game_n": pl.Int64(),
    "fta_per_game": pl.Float64(),
    "fta_per_game_n": pl.Int64(),
    "pace": pl.Float64(),
    "pace_n": pl.Int64(),
    "home_win_pct": pl.Float64(),
    "home_win_pct_n": pl.Int64(),
    "home_minus_away_fouls": pl.Float64(),
    "home_minus_away_fouls_n": pl.Int64(),
}

_OT_MINUTES = 5
#: First WBB season whose box records turnovers (see ``nba_game_page._tov``).
_WBB_FIRST_TURNOVER_SEASON = 2009
_WBB_MIN_RECORDED_GAME_TURNOVERS = 5


def _empty(schema: dict[str, pl.DataType]) -> pl.DataFrame:
    return pl.DataFrame(schema=schema)


def _box_turnovers(team_box: pl.DataFrame, league: str) -> pl.Expr:
    """Team turnovers for the possession estimate: the game page's rule, vectorised over games."""
    from sportsdataverse.nba.nba_game_page import _team_tov

    if league in ("nba", "wnba"):
        return _team_tov(team_box.columns).cast(pl.Float64)
    from sportsdataverse.mbb.mbb_team_ratings import _turnovers

    tov = _turnovers(team_box)
    if league != "wbb":
        return tov
    # WBB: a 0 before 2009, or a game whose two teams show under 5, is "not recorded".
    unrecorded = ((pl.col("season") < _WBB_FIRST_TURNOVER_SEASON) & (tov.fill_null(0.0) == 0)) | (
        tov.fill_null(0.0).sum().over("game_id") < _WBB_MIN_RECORDED_GAME_TURNOVERS
    )
    return pl.when(unrecorded).then(pl.lit(None, dtype=pl.Float64)).otherwise(tov)


def official_game_rows(
    team_box: pl.DataFrame,
    schedule: pl.DataFrame,
    *,
    league: str,
    season_type: int | None = 2,
) -> pl.DataFrame:
    """One row per completed game of the season type, with the box sums the profile reads.

    Args:
        team_box: ``load_{lg}_team_boxscore`` rows (two per game).
        schedule: ``load_{lg}_schedule`` rows.
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"``.
        season_type: ESPN's season type (2 regular season, 3 postseason, 5 NBA play-in); ``None``
            keeps every type.

    Returns:
        :data:`GAME_ROWS_SCHEMA` rows. A game without a box of one home and one away team keeps
        null box columns; ``game_minutes`` is null when the last period is below regulation.
    """
    from sportsdataverse.nba.nba_game_page import _REGULATION_MINUTES, FT_WEIGHT, game_regulation_periods

    if schedule.is_empty():
        return _empty(GAME_ROWS_SCHEMA)
    sched = schedule.filter(pl.col("status_type_completed") == True)  # noqa: E712
    if season_type is not None:
        sched = sched.filter(pl.col("season_type") == season_type)
    sched = sched.select(
        pl.col("season").cast(pl.Int64),
        pl.col("game_id").cast(pl.Int64).cast(pl.Utf8),
        pl.col("neutral_site").fill_null(False).alias("neutral"),
        pl.col("status_period").cast(pl.Float64).alias("_periods"),
        pl.col("format_regulation_periods").cast(pl.Float64).fill_null(0.0).alias("_frp"),  # 0: unknown
    ).unique("game_id", keep="first")
    regs = sched.select("season", "_frp").unique()
    regs = regs.with_columns(
        pl.Series(
            "_reg",
            [game_regulation_periods(league, season=s, format_regulation_periods=f) for s, f in regs.iter_rows()],
            dtype=pl.Float64,
        )
    )
    sched = sched.join(regs, on=["season", "_frp"], how="left").with_columns(
        pl.when(pl.col("_periods") >= pl.col("_reg"))
        .then(_REGULATION_MINUTES[league] + _OT_MINUTES * (pl.col("_periods") - pl.col("_reg")))
        .cast(pl.Int64)
        .alias("game_minutes")
    )

    box = team_box.with_columns(pl.col("game_id").cast(pl.Int64).cast(pl.Utf8), pl.col("season").cast(pl.Int64))
    home = pl.col("team_home_away") == "home"
    away = pl.col("team_home_away") == "away"
    poss = (
        pl.col("field_goals_attempted")
        - pl.col("offensive_rebounds")
        + _box_turnovers(box, league)
        + FT_WEIGHT * pl.col("free_throws_attempted")
    ).cast(pl.Float64)
    games = (
        box.with_columns(poss.alias("_poss"))
        .group_by("game_id")
        .agg(
            pl.len().alias("_rows"),
            home.sum().alias("_homes"),
            away.sum().alias("_aways"),
            pl.col("team_winner").filter(home).first().alias("home_win"),
            pl.col("fouls").filter(home).first().cast(pl.Int64).alias("home_fouls"),
            pl.col("fouls").filter(away).first().cast(pl.Int64).alias("away_fouls"),
            pl.when(pl.col("free_throws_attempted").null_count() == 0)
            .then(pl.col("free_throws_attempted").sum())
            .cast(pl.Int64)
            .alias("fta"),
            pl.when(pl.col("_poss").null_count() == 0).then(pl.col("_poss").mean()).alias("possessions"),
        )
        .filter((pl.col("_rows") == 2) & (pl.col("_homes") == 1) & (pl.col("_aways") == 1))
        .drop("_rows", "_homes", "_aways")
    )
    out = sched.join(games, on="game_id", how="left")
    return out.select(list(GAME_ROWS_SCHEMA)).cast(GAME_ROWS_SCHEMA)


def officials_rows(officials: pl.DataFrame) -> pl.DataFrame:
    """``load_{lg}_officials`` rows as :data:`OFFICIALS_SCHEMA`, one per official and game.

    Args:
        officials: The officials release (``official_id`` where the league carries one, and
            ``official_full_name`` or ``official_display_name``).

    Returns:
        :data:`OFFICIALS_SCHEMA` rows; ``official_id`` null where the release has none, the name
        with runs of whitespace collapsed. A row with neither id nor name is dropped.
    """
    if officials.is_empty():
        return _empty(OFFICIALS_SCHEMA)
    name_col = "official_full_name" if "official_full_name" in officials.columns else "official_display_name"
    ident = pl.col("official_id").cast(pl.Int64) if "official_id" in officials.columns else pl.lit(None, dtype=pl.Int64)
    rows = officials.select(
        pl.col("game_id").cast(pl.Int64).cast(pl.Utf8),
        ident.alias("official_id"),
        pl.col(name_col).str.replace_all(r"\s+", " ").str.strip_chars().alias("official_name"),
    ).with_columns(pl.when(pl.col("official_name") != "").then(pl.col("official_name")).alias("official_name"))
    rows = rows.filter(pl.col("official_id").is_not_null() | pl.col("official_name").is_not_null())
    key = pl.coalesce(pl.col("official_id").cast(pl.Utf8), pl.col("official_name"))
    return rows.with_columns(key.alias("_key")).unique(["game_id", "_key"], keep="first").drop("_key")


def official_seasons(officials: pl.DataFrame, games: pl.DataFrame, *, regulation_minutes: int) -> pl.DataFrame:
    """The per-official season profile from :func:`officials_rows` and :func:`official_game_rows`.

    Args:
        officials: :data:`OFFICIALS_SCHEMA` rows.
        games: :data:`GAME_ROWS_SCHEMA` rows (the season's completed games of one season type).
        regulation_minutes: 48 (NBA) or 40, the length ``pace`` is expressed per.

    Returns:
        :data:`OFFICIAL_SEASONS_SCHEMA` rows, sorted by season, most games, then name.
    """
    if officials.is_empty() or games.is_empty():
        return _empty(OFFICIAL_SEASONS_SCHEMA)
    rows = (
        officials.join(games, on="game_id", how="inner")
        .with_columns(
            pl.coalesce(pl.col("official_id").cast(pl.Utf8), pl.col("official_name")).alias("_key"),
            (pl.col("home_fouls") + pl.col("away_fouls")).alias("_fouls"),
        )
        .sort("_key", "game_id")  # a fixed summation order: the pace floats are reproducible
    )
    boxed = pl.col("_fouls").is_not_null()
    paced = pl.col("possessions").is_not_null() & pl.col("game_minutes").is_not_null()
    home_won = ~pl.col("neutral") & pl.col("home_win").is_not_null()
    home_diff = ~pl.col("neutral") & boxed
    agg = rows.group_by("season", "_key").agg(
        pl.col("official_id").drop_nulls().first(),
        pl.col("official_name").drop_nulls().mode().sort().first(),
        pl.len().alias("games"),
        pl.col("_fouls").filter(boxed).mean().alias("fouls_per_game"),
        boxed.sum().alias("fouls_per_game_n"),
        pl.col("fta").drop_nulls().mean().alias("fta_per_game"),
        pl.col("fta").is_not_null().sum().alias("fta_per_game_n"),
        (
            regulation_minutes * pl.col("possessions").filter(paced).sum() / pl.col("game_minutes").filter(paced).sum()
        ).alias("pace"),
        paced.sum().alias("pace_n"),
        pl.col("home_win").filter(home_won).cast(pl.Float64).mean().alias("home_win_pct"),
        home_won.sum().alias("home_win_pct_n"),
        (pl.col("home_fouls") - pl.col("away_fouls"))
        .filter(home_diff)
        .cast(pl.Float64)
        .mean()
        .alias("home_minus_away_fouls"),
        home_diff.sum().alias("home_minus_away_fouls_n"),
    )
    # a rate over zero games is null, not NaN
    rates = ("fouls_per_game", "fta_per_game", "pace", "home_win_pct", "home_minus_away_fouls")
    agg = agg.with_columns(pl.when(pl.col(f"{m}_n") > 0).then(pl.col(m)).alias(m) for m in rates)
    return (
        agg.select(list(OFFICIAL_SEASONS_SCHEMA))
        .cast(OFFICIAL_SEASONS_SCHEMA)
        .sort(
            ["season", "games", "official_name", "official_id"], descending=[False, True, False, False], nulls_last=True
        )
    )


def league_official_seasons(league: str, seasons: int | list[int], *, season_type: int | None = 2) -> pl.DataFrame:
    """Load the league's officials, team box and schedule releases and build the profile.

    Args:
        league: ``"nba"``, ``"wnba"``, ``"mbb"`` or ``"wbb"``.
        seasons: A season or list of seasons, named by the year it ends.
        season_type: ESPN's season type (2 regular season, 3 postseason); ``None`` keeps all.

    Returns:
        :func:`official_seasons` rows.
    """
    import importlib

    from sportsdataverse.nba.nba_game_page import _REGULATION_MINUTES

    if league not in _REGULATION_MINUTES:
        raise ValueError(f"league {league!r} is not one of {tuple(_REGULATION_MINUTES)}")
    wanted = [seasons] if isinstance(seasons, int) else list(seasons)
    mod = importlib.import_module(f"sportsdataverse.{league}.{league}_loaders")
    officials = mod.__dict__[f"load_{league}_officials"](wanted)
    if officials.is_empty():
        return _empty(OFFICIAL_SEASONS_SCHEMA)
    games = official_game_rows(
        mod.__dict__[f"load_{league}_team_boxscore"](wanted),
        mod.__dict__[f"load_{league}_schedule"](wanted),
        league=league,
        season_type=season_type,
    )
    return official_seasons(officials_rows(officials), games, regulation_minutes=_REGULATION_MINUTES[league])
