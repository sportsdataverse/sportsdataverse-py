"""Rolling event-count windows: a player's or team's form over its last N events.

One league-agnostic core, :func:`rolling_windows`, over a long *events* frame
(one row per entity x unit x metric x event), and the adapters that build it:
:func:`football_events` from released ``espn_{cfb,nfl}_pbp`` plays and
:func:`shot_events` from released ``{nba,wnba}_stats_shots``.

Windows count EVENTS (dropbacks, targets, carries, plays, field-goal attempts),
not games, so a backup's last 100 dropbacks compare fairly with a starter's.
Windows cross season boundaries; a row is labelled with the season its data
runs through.

Example:
    Quick start::

        import polars as pl
        from sportsdataverse.cfb import load_cfb_pbp, load_cfb_schedule
        from sportsdataverse.rolling_windows import FOOTBALL_PBP_COLUMNS, football_events

        pbp = load_cfb_pbp(2024).select(FOOTBALL_PBP_COLUMNS)
        sched = load_cfb_schedule(2024)
        game_dates = sched.select(
            pl.col("game_id").cast(pl.Int64),
            game_date=pl.col("start_date").str.slice(0, 10).str.to_date(),
        )
        ev = football_events(pbp, game_dates)
        ev.filter(pl.col("window_unit") == "dropback").head()
"""

from __future__ import annotations

import polars as pl

__all__ = [
    "EVENT_SCHEMA",
    "FOOTBALL_PBP_COLUMNS",
    "WINDOWS",
    "football_events",
]

#: window sizes per unit; a producer may pass its own to :func:`rolling_windows`
WINDOWS: dict[str, tuple[int, ...]] = {
    "dropback": (50, 100, 300),
    "target": (30, 60),
    "carry": (50, 100),
    "play": (150, 300),
    "fga": (50, 200),
    "fg3a": (50, 200),
}

EVENT_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64,
    "entity_type": pl.Utf8,
    "entity_id": pl.Utf8,
    "entity_name": pl.Utf8,
    "team_id": pl.Utf8,
    "window_unit": pl.Utf8,
    "metric": pl.Utf8,
    "game_id": pl.Utf8,
    "event_date": pl.Date,
    "seq": pl.Int64,
    "value": pl.Float64,
}

#: the released pbp columns :func:`football_events` reads (project to these)
FOOTBALL_PBP_COLUMNS: tuple[str, ...] = (
    "season",
    "seasonType",
    "game_id",
    "game_play_number",
    "down",
    "EPA",
    "EPA_success",
    "EPA_scrimmage",
    "pass",
    "rush",
    "target",
    "passer_player_id",
    "passer_player_name",
    "receiver_player_id",
    "receiver_player_name",
    "rusher_player_id",
    "rusher_player_name",
    "pos_team_id",
    "pos_team",
)

#: (unit, entity type, flag column or None for every play, id column, name column)
_FOOTBALL_UNITS = (
    ("dropback", "player", "pass", "passer_player_id", "passer_player_name"),
    ("target", "player", "target", "receiver_player_id", "receiver_player_name"),
    ("carry", "player", "rush", "rusher_player_id", "rusher_player_name"),
    ("play", "team", None, "pos_team_id", "pos_team"),
)
#: ESPN season types that count: regular season (2) and postseason (3)
COUNTED_SEASON_TYPES = (2, 3)
_ID_DTYPES = (pl.Int64, pl.Int32, pl.UInt32, pl.UInt64, pl.Utf8)


def _as_id(df: pl.DataFrame, col: str) -> pl.Expr:
    """An id column as text; a float id is refused (it would stringify as ``"123.0"``)."""
    if df.schema[col] not in _ID_DTYPES:
        raise TypeError(f"{col} is {df.schema[col]}; ids must be integer or string, never float")
    return pl.col(col).cast(pl.Utf8)


def _require_dates(df: pl.DataFrame) -> None:
    missing = df.filter(pl.col("game_date").is_null())["game_id"].unique()
    if missing.len():
        raise ValueError(f"{missing.len()} game(s) have no game_date: {missing.head(5).to_list()}")


def football_events(pbp: pl.DataFrame, game_dates: pl.DataFrame) -> pl.DataFrame:
    """Dropback / target / carry / team-play events from released ``espn_{cfb,nfl}_pbp`` plays.

    Population: plays from scrimmage on a numbered down (``EPA_scrimmage`` not null,
    ``down`` 1-4) in the regular season or postseason -- the population sdv-db's
    player routes aggregate. ``pass`` includes sacks (a sack is a dropback); CFB has no
    scramble flag, so a scramble is a carry.

    Args:
        pbp: released pbp plays, any number of seasons (project to ``FOOTBALL_PBP_COLUMNS``).
        game_dates: ``game_id`` (int) and ``game_date`` (date) for every game in ``pbp``.

    Returns:
        pl.DataFrame: one row per event x metric (``epa``, ``success_rate``), ``EVENT_SCHEMA``.

    Raises:
        TypeError: an id column is float.
        ValueError: a play's game has no ``game_date``.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.rolling_windows import football_events

            ev = football_events(pbp, game_dates)
            ev.filter(pl.col("window_unit") == "dropback").head()

        Pipeline next step (one line)::

            ev.group_by("entity_id", "season").agg(pl.col("value").mean())

        See Also:
            * `nflfastR`_ -- source EPA/success convention this population mirrors.
            * `cfbfastR`_ -- CFB pbp this adapter reads.

        .. _nflfastR: https://www.nflfastr.com
        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    if pbp.height == 0:
        return pl.DataFrame(schema=EVENT_SCHEMA)
    dates = game_dates.select(pl.col("game_id").cast(pl.Int64), pl.col("game_date").cast(pl.Date))
    keep = pl.col("EPA_scrimmage").is_not_null() & pl.col("down").is_between(1, 4) & pl.col("EPA").is_not_null()
    if "seasonType" in pbp.columns:
        keep = keep & pl.col("seasonType").cast(pl.Int64, strict=False).is_in(COUNTED_SEASON_TYPES)
    plays = pbp.filter(keep).with_columns(pl.col("game_id").cast(pl.Int64))
    assert plays.schema["game_id"] == dates.schema["game_id"]
    plays = plays.join(dates, on="game_id", how="left")
    _require_dates(plays)
    index = [
        "season",
        "entity_type",
        "entity_id",
        "entity_name",
        "team_id",
        "window_unit",
        "game_id",
        "event_date",
        "seq",
    ]
    frames = []
    for unit, etype, flag, id_col, name_col in _FOOTBALL_UNITS:
        sel = plays if flag is None else plays.filter(pl.col(flag) == True)  # noqa: E712
        sel = sel.filter(pl.col(id_col).is_not_null())
        if etype == "player":
            # ESPN's play text sometimes can't be attributed to an individual; the
            # parser records that as name "TEAM" with a synthetic negative id -- not
            # a real player, so it doesn't belong in a player's event window.
            sel = sel.filter(pl.col(name_col) != "TEAM")
        frames.append(
            sel.select(
                pl.col("season").cast(pl.Int64),
                pl.lit(etype).alias("entity_type"),
                _as_id(sel, id_col).alias("entity_id"),
                pl.col(name_col).cast(pl.Utf8).alias("entity_name"),
                _as_id(sel, "pos_team_id").alias("team_id"),
                pl.lit(unit).alias("window_unit"),
                pl.col("game_id").cast(pl.Utf8),
                pl.col("game_date").alias("event_date"),
                pl.col("game_play_number").cast(pl.Int64).alias("seq"),
                pl.col("EPA").cast(pl.Float64).alias("epa"),
                pl.col("EPA_success").cast(pl.Float64).alias("success_rate"),
            ).unpivot(on=["epa", "success_rate"], index=index, variable_name="metric", value_name="value")
        )
    return pl.concat(frames).select(list(EVENT_SCHEMA)).cast(EVENT_SCHEMA)
