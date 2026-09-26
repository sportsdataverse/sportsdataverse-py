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
    "OUTPUT_SCHEMA",
    "WINDOWS",
    "football_events",
    "rolling_windows",
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


def _require_unique_game_ids(game_dates: pl.DataFrame) -> None:
    dup = game_dates.filter(pl.col("game_id").is_duplicated())["game_id"].unique()
    if dup.len():
        raise ValueError(f"{dup.len()} game_id(s) appear more than once in game_dates: {dup.head(5).to_list()}")


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
        ValueError: a play's game has no ``game_date``, or ``game_dates`` has a
            duplicate ``game_id``.

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
    _require_unique_game_ids(game_dates)
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


OUTPUT_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64,
    "entity_type": pl.Utf8,
    "entity_id": pl.Utf8,
    "entity_name": pl.Utf8,
    "team_id": pl.Utf8,
    "metric": pl.Utf8,
    "window_unit": pl.Utf8,
    "window_n": pl.Int64,
    "cur": pl.Float64,
    "prev": pl.Float64,
    "season_start": pl.Float64,
    "career_baseline": pl.Float64,
    "delta_prev": pl.Float64,
    "delta_season": pl.Float64,
    "delta_career": pl.Float64,
    "delta_prev_rank": pl.Int64,
    "n": pl.Int64,
    "qualified": pl.Boolean,
    "last_event_date": pl.Date,
    "as_of_date": pl.Date,
}
_KEY = ["entity_type", "entity_id", "window_unit", "metric"]
_RANK_GROUP = ["entity_type", "window_unit", "window_n", "metric"]


def _window(ev: pl.DataFrame, size: int) -> pl.DataFrame:
    """One window size over events already indexed from the end (``_r``) and season start (``_r0``)."""
    r, r0, v = pl.col("_r"), pl.col("_r0"), pl.col("value")
    in_cur = r < size
    in_prev = (r >= size) & (r < 2 * size)
    in_start = (r0 >= 0) & (r0 < size)
    before = r >= size
    return (
        ev.group_by(_KEY)
        .agg(
            entity_name=pl.col("entity_name").filter(r == 0).first(),
            team_id=pl.col("team_id").filter(r == 0).first(),
            last_event_date=pl.col("event_date").filter(r == 0).first(),
            cur=v.filter(in_cur).mean(),
            n=in_cur.sum(),
            prev=pl.when(in_prev.sum() == size).then(v.filter(in_prev).mean()),
            season_start=pl.when(in_start.sum() == size).then(v.filter(in_start).mean()),
            career_baseline=pl.when(before.sum() >= size).then(v.filter(before).mean()),
        )
        .with_columns(window_n=pl.lit(size, dtype=pl.Int64))
    )


def rolling_windows(
    events: pl.DataFrame, season: int, windows: dict[str, tuple[int, ...]] | None = None
) -> pl.DataFrame:
    """Rolling-window form for every entity with an event in ``season``.

    Args:
        events: an ``EVENT_SCHEMA`` frame covering every season up to ``season``
            (the career history the baselines read).
        season: the season the rows describe; later seasons in ``events`` are ignored.
        windows: ``{window_unit: (sizes...)}``; defaults to :data:`WINDOWS`.

    Returns:
        pl.DataFrame: one row per (entity, unit, metric, window size), ``OUTPUT_SCHEMA``.
        ``prev`` / ``season_start`` need a FULL window and ``career_baseline`` at least
        one window of history, else null; ``qualified`` is ``True`` iff ``n == window_n``;
        ``delta_prev_rank`` (1 = biggest riser, ties share the lowest rank) is null unless
        ``qualified`` and ``prev`` exists.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.rolling_windows import football_events, rolling_windows

            ev = football_events(pbp, game_dates)
            rw = rolling_windows(ev, 2024)
            rw.filter(pl.col("window_unit") == "dropback").head()

        Pipeline next step (one line)::

            rw.filter(pl.col("delta_prev_rank") == 1).select("entity_name", "window_unit", "window_n")
    """
    windows = WINDOWS if windows is None else windows
    ev = events.filter((pl.col("season") <= season) & pl.col("value").is_not_null() & pl.col("value").is_not_nan())
    current = ev.filter(pl.col("season") == season)
    if current.height == 0:
        return pl.DataFrame(schema=OUTPUT_SCHEMA)
    as_of = current["event_date"].max()
    # ponytail: whole history in memory (~2 GB for CFB 2004-2026); pre-filter to the
    # season's active entities if a runner OOMs
    ev = (
        ev.sort([*_KEY, "event_date", "game_id", "seq"], maintain_order=True)
        .with_columns(
            _i=pl.int_range(pl.len()).over(_KEY),
            _n=pl.len().over(_KEY),
            _prior=(pl.col("season") < season).sum().over(_KEY),
            _in=(pl.col("season") == season).any().over(_KEY),
        )
        .filter(pl.col("_in"))
        .with_columns(
            _r=pl.col("_n") - 1 - pl.col("_i"),  # 0 = the latest event
            _r0=pl.col("_prior") - 1 - pl.col("_i"),  # 0 = the last event before the season
        )
    )
    parts = [
        _window(ev.filter(pl.col("window_unit") == unit), size)
        for unit, sizes in windows.items()
        for size in sizes
        if ev.filter(pl.col("window_unit") == unit).height
    ]
    if not parts:
        return pl.DataFrame(schema=OUTPUT_SCHEMA)
    # ranking (and rank-eligibility) reads a fully-qualified window with a full
    # prior window to diff against; `qualified` alone (see OUTPUT_SCHEMA) is a
    # weaker, output-facing condition that doesn't require `prev`.
    ranked = (pl.col("n") == pl.col("window_n")) & pl.col("prev").is_not_null()
    out = (
        pl.concat(parts)
        .with_columns(
            season=pl.lit(season, dtype=pl.Int64),
            # round before ranking: two entities whose deltas are "really" equal can
            # still differ at ~1e-16 from IEEE 754 subtraction (e.g. 0.6 - 0.4 vs.
            # 0.5 - 0.3), which would otherwise split a tie into adjacent ranks.
            delta_prev=(pl.col("cur") - pl.col("prev")).round(12),
            delta_season=(pl.col("cur") - pl.col("season_start")).round(12),
            delta_career=(pl.col("cur") - pl.col("career_baseline")).round(12),
            qualified=pl.col("n") == pl.col("window_n"),
            as_of_date=pl.lit(as_of, dtype=pl.Date),
        )
        .with_columns(
            delta_prev_rank=pl.when(ranked).then(
                pl.when(ranked).then(pl.col("delta_prev")).rank(method="min", descending=True).over(_RANK_GROUP)
            )
        )
    )
    return (
        out.select(list(OUTPUT_SCHEMA))
        .cast(OUTPUT_SCHEMA)
        .sort(["window_unit", "window_n", "metric", "entity_type", "entity_id"])
    )
