from __future__ import annotations

import datetime

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import download
from sportsdataverse.errors import SeasonNotFoundError


def espn_wbb_schedule(
    dates=None,
    groups=50,
    season_type=None,
    limit=500,
    return_as_pandas=False,
    **kwargs,
) -> pl.DataFrame:
    """espn_wbb_schedule - look up the women's college basketball schedule for a given season

    Args:
        dates (int): Used to define different seasons. 2002 is the earliest available season.
        groups (int): Used to define different divisions. 50 is Division I, 51 is Division II/Division III.
        season_type (int): 2 for regular season, 3 for post-season, 4 for off-season.
        limit (int): number of records to return, default: 500.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing schedule dates for the requested season. Returns None if no games

    Example:
        Single date (April 7, 2024 - 2024 NCAA W championship day)::

            from sportsdataverse.wbb import espn_wbb_schedule
            day = espn_wbb_schedule(dates=20240407)
            print(day.shape)

        Season-level pull (2024 season)::

            season = espn_wbb_schedule(dates=2024, limit=1500)
            print(season.shape)

        Filter to a specific team (UConn ``team_id=2509``)::

            import polars as pl
            uconn = season.filter(
                (pl.col("home_id") == "2509") | (pl.col("away_id") == "2509")
            )

        Pandas round-trip::

            season_pd = espn_wbb_schedule(dates=2024, return_as_pandas=True)
            season_pd.head()

        See Also:
            * `wehoop`_ - R sister package
            * `cfbfastR`_ - companion R package for college football
            * `ESPN`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
        .. _ESPN: https://www.espn.com
    """
    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/scoreboard"
    params = {
        "dates": dates,
        "seasonType": season_type,
        "groups": groups if groups is not None else "50",
        "limit": limit,
    }
    resp = download(url=url, params=params, **kwargs)

    ev = pd.DataFrame()
    events_txt = resp.json()
    events = events_txt.get("events")
    if events is None:
        return pd.DataFrame() if return_as_pandas else pl.DataFrame()
    if len(events) == 0:
        return pd.DataFrame() if return_as_pandas else pl.DataFrame()

    for event in events:
        event = scoreboard_event_parsing(event)
        x = pl.from_pandas(pd.json_normalize(event.get("competitions")[0], sep="_"))
        x = x.with_columns(
            game_id=(pl.col("id").cast(pl.Int32)),
            season=(event.get("season").get("year")),
            season_type=(event.get("season").get("type")),
            home_linescores=pl.when(pl.col("status_type_description") == "Postponed")
            .then(None)
            .otherwise(pl.col("home_linescores")),
            away_linescores=pl.when(pl.col("status_type_description") == "Postponed")
            .then(None)
            .otherwise(pl.col("away_linescores")),
        ).with_columns(
            season=pl.col("season").cast(pl.Int32),
            season_type=pl.col("season_type").cast(pl.Int32),
        )
        x = x[[s.name for s in x if s.null_count() != x.height]]
        ev = pd.concat([ev, x.to_pandas()], axis=0, ignore_index=True)
    ev = pl.from_pandas(ev)
    ev = ev.janitor.clean_names()

    return ev.to_pandas() if return_as_pandas else ev


def scoreboard_event_parsing(event):
    event.get("competitions")[0].get("competitors")[0].get("team").pop("links", None)
    event.get("competitions")[0].get("competitors")[1].get("team").pop("links", None)
    if event.get("competitions")[0].get("competitors")[0].get("homeAway") == "home":
        event = __extract_home_away(event, 0, "home")
        event = __extract_home_away(event, 1, "away")
    else:
        event = __extract_home_away(event, 0, "away")
        event = __extract_home_away(event, 1, "home")
    del_keys = ["geoBroadcasts", "headlines", "series", "situation", "tickets", "odds", "leaders"]
    for k in del_keys:
        event.get("competitions")[0].pop(k, None)
    event.get("competitions")[0]["notes_type"] = (
        event.get("competitions")[0]["notes"][0].get("type") if len(event.get("competitions")[0]["notes"]) > 0 else ""
    )
    event.get("competitions")[0]["notes_headline"] = (
        event.get("competitions")[0]["notes"][0].get("headline").replace('"', "")
        if len(event.get("competitions")[0]["notes"]) > 0
        else ""
    )
    event.get("competitions")[0]["broadcast_market"] = (
        event.get("competitions")[0].get("broadcasts", [])[0].get("market", "")
        if len(event.get("competitions")[0].get("broadcasts")) > 0
        else ""
    )
    event.get("competitions")[0]["broadcast_name"] = (
        event.get("competitions")[0].get("broadcasts", [])[0].get("names", [])[0]
        if len(event.get("competitions")[0].get("broadcasts")) > 0
        else ""
    )
    event.get("competitions")[0].pop("broadcasts", None)
    event.get("competitions")[0].pop("notes", None)
    event.get("competitions")[0].pop("competitors", None)
    return event


def __extract_home_away(event, arg1, arg2):
    event["competitions"][0][arg2] = event.get("competitions")[0].get("competitors")[arg1].get("team")
    event["competitions"][0][arg2]["score"] = event.get("competitions")[0].get("competitors")[arg1].get("score")
    event["competitions"][0][arg2]["winner"] = event.get("competitions")[0].get("competitors")[arg1].get("winner")
    # add winner back to main competitors if does not exist
    event["competitions"][0]["competitors"][arg1]["winner"] = (
        event.get("competitions")[0].get("competitors")[arg1].get("winner", False)
    )
    event["competitions"][0][arg2]["currentRank"] = (
        event.get("competitions")[0].get("competitors")[arg1].get("curatedRank", {}).get("current", 99)
    )
    event["competitions"][0][arg2]["linescores"] = (
        event.get("competitions")[0]
        .get("competitors")[arg1]
        .get("linescores", [{"value": 0}, {"value": 0}, {"value": 0}, {"value": 0}])
    )
    # add linescores back to main competitors if does not exist
    event["competitions"][0]["competitors"][arg1]["linescores"] = (
        event.get("competitions")[0]
        .get("competitors")[arg1]
        .get("linescores", [{"value": 0}, {"value": 0}, {"value": 0}, {"value": 0}])
    )
    event["competitions"][0][arg2]["records"] = (
        event.get("competitions")[0]
        .get("competitors")[arg1]
        .get(
            "records",
            [
                {"abbreviation": "Game", "name": "overall", "summary": "0-0", "type": "total"},
                {"abbreviation": "null", "name": "Home", "summary": "0-0", "type": "home"},
                {"abbreviation": "null", "name": "Road", "summary": "0-0", "type": "road"},
                {"abbreviation": "null", "name": "vs. Conf.", "summary": "0-0", "type": "vsconf"},
            ],
        )
    )
    return event


def espn_wbb_calendar(season=None, ondays=None, return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """espn_wbb_calendar - look up the women's college basketball calendar for a given season

    Args:
        season (int): Used to define different seasons. 2002 is the earliest available season.
        ondays (boolean): Used to return dates for calendar ondays
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing calendar dates for the requested season.

    Raises:
        ValueError: If `season` is less than 2002.

    Example:
        Calendar dates for a single season::

            from sportsdataverse.wbb import espn_wbb_calendar
            cal = espn_wbb_calendar(season=2024)
            cal.head()

        On-days only (dates with games on the schedule)::

            ondays = espn_wbb_calendar(season=2024, ondays=True)
            ondays.head()

        Pandas round-trip::

            cal_pd = espn_wbb_calendar(season=2024, return_as_pandas=True)
            cal_pd.head()

        See Also:
            * `wehoop`_ - R sister package
            * `cfbfastR`_ - companion R package for college football
            * `ESPN`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
        .. _ESPN: https://www.espn.com
    """
    if int(season) < 2002:
        raise SeasonNotFoundError("season cannot be less than 2002")
    if ondays is not None:
        full_schedule = __ondays_wbb_calendar(season, **kwargs)
    else:
        url = f"http://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/scoreboard?groups=50&dates={season}"
        resp = download(url=url, **kwargs)
        txt = resp.json().get("leagues")[0].get("calendar")
        datenum = list(map(lambda x: x[:10].replace("-", ""), txt))
        date = list(map(lambda x: x[:10], txt))
        year = list(map(lambda x: x[:4], txt))
        month = list(map(lambda x: x[5:7], txt))
        day = list(map(lambda x: x[8:10], txt))
        data = {
            "season": season,
            "datetime": txt,
            "date": date,
            "year": year,
            "month": month,
            "day": day,
            "dateURL": datenum,
        }
        full_schedule = pl.DataFrame(data)
        full_schedule = full_schedule.with_columns(
            url="http://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/scoreboard?groups=50&dates="
            + pl.col("dateURL"),
        )
    return full_schedule.to_pandas() if return_as_pandas else full_schedule


def __ondays_wbb_calendar(season, **kwargs):
    url = f"https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/seasons/{season}/types/2/calendar/ondays?groups=50"
    resp = download(url=url, **kwargs)
    txt = resp.json().get("eventDate").get("dates")
    result = pl.DataFrame(txt, schema=["dates"])
    result = result.with_columns(dateURL=pl.col("dates").str.slice(0, 10))
    result = result.with_columns(
        url="http://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/scoreboard?groups=50&dates="
        + pl.col("dateURL"),
    )

    return result


def most_recent_wbb_season():
    """Return the most recent women's college basketball season year.

    The women's college basketball season spans late October through early
    April; for any month October-December the "current season" is the
    following calendar year (e.g. October 2025 returns ``2026``).

    Returns:
        int: The most recent / current season year.

    Example:
        Use as a default season argument::

            from sportsdataverse.wbb import most_recent_wbb_season, espn_wbb_schedule
            season = most_recent_wbb_season()
            sched = espn_wbb_schedule(dates=season)

        See Also:
            * `wehoop`_ - R sister package
            * `cfbfastR`_ - companion R package for college football

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    if datetime.datetime.now().month >= 10:
        return datetime.datetime.now().year + 1
    else:
        return datetime.datetime.now().year


# R: dplyr::across(any_of(...), as.integer) in espn_wbb_0{1,2,3}_*_creation.R.
_SCHED_INT32_COLS = (
    "id",
    "game_id",
    "type_id",
    "status_type_id",
    "home_id",
    "home_venue_id",
    "home_conference_id",
    "home_score",
    "away_id",
    "away_venue_id",
    "away_conference_id",
    "away_score",
    "season",
    "season_type",
    "groups_id",
    "tournament_id",
    "venue_id",
)

# Left numeric by R (not in the as.integer list) but Int64 in the raw parquet.
_SCHED_FLOAT64_COLS = (
    "attendance",
    "status_period",
    "format_regulation_periods",
    "home_current_rank",
    "away_current_rank",
)


def helper_wbb_schedule(
    sched: pl.DataFrame,
    *,
    pbp_game_ids: list[int],
    team_box_game_ids: list[int],
    player_box_game_ids: list[int],
) -> pl.DataFrame:
    """Reshape the raw WBB season schedule into the released schedule frame.

    Faithful polars port of the schedule blocks in the wehoop-wbb-data
    creation scripts (``espn_wbb_01_pbp_creation.R`` adds casts +
    ``game_date_time``/``game_date`` + the ``PBP`` flag; ``02`` stamps
    ``team_box``; ``03`` stamps ``player_box`` and uploads). Column order and
    dtypes mirror the released ``wbb_schedule_{season}.parquet``.

    Args:
        sched: The raw ``wehoop-wbb-raw`` season schedule frame
            (``wbb/schedules/parquet/wbb_schedule_{season}.parquet``).
        pbp_game_ids: Game ids present in the compiled play_by_play dataset.
        team_box_game_ids: Game ids present in the compiled team_box dataset.
        player_box_game_ids: Game ids present in the compiled player_box dataset.

    Returns:
        pl.DataFrame: One row per game, deduped, sorted by ``date`` descending.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.wbb import helper_wbb_schedule
            raw = pl.read_parquet("wbb_schedule_2025.parquet")
            df = helper_wbb_schedule(
                raw, pbp_game_ids=[], team_box_game_ids=[], player_box_game_ids=[]
            )
            print(df.shape)

    See Also:
        * `wehoop`_ -- the R producer this ports; retained as the parity oracle.

    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    df = sched.drop([c for c in ("__index_level_0__",) if c in sched.columns])
    # Float64 intermediate keeps R as.integer semantics ("59.0" -> 59, not null).
    df = df.with_columns(
        [pl.col(c).cast(pl.Float64, strict=False).cast(pl.Int32) for c in _SCHED_INT32_COLS if c in df.columns]
    )
    if "status_display_clock" in df.columns:
        df = df.with_columns(pl.col("status_display_clock").cast(pl.Utf8))
    # R read these as numeric from the scraper's rds (the raw parquet has
    # Int64); released dtype is Float64.
    df = df.with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in _SCHED_FLOAT64_COLS if c in df.columns])
    # The raw scraper's rds writer stringifies nested list columns as numpy
    # object-array reprs (dict-per-line, Python None/float literals); the raw
    # PARQUET keeps them nested. Released dtype is String -- mirror the repr.
    list_cols = [c for c, dt in df.schema.items() if isinstance(dt, pl.List)]
    if list_cols:
        import numpy as np

        def _np_repr(v: object) -> str | None:
            if v is None:
                return None
            # Explicit 1-D object array (nested equal-length lists would
            # otherwise build 2-D). array2string's kwargs govern only the OUTER
            # array -- an ndarray nested inside a struct field is rendered by
            # its own repr(), which reads the process-global print options --
            # so pin them for the whole call, not just the outer one. These are
            # numpy's defaults, so this is a no-op on today's output; it stops a
            # numpy upgrade or a caller's set_printoptions from silently
            # rewriting the released strings.
            arr = np.empty(len(v), dtype=object)  # type: ignore[arg-type]
            arr[:] = v  # type: ignore[call-overload]
            with np.printoptions(linewidth=75, threshold=1000, edgeitems=3):
                return np.array2string(arr, max_line_width=75, threshold=1000, edgeitems=3)

        def _entries(col: str) -> list:
            # List(Struct) MUST go through to_pandas(): it surfaces each entry
            # as a dict (keeping the field names) AND renders a list nested
            # inside a struct field as a numpy object array -- which is how the
            # scraper's writer wrote it ("devices": array([...], dtype=object)).
            # to_list() would emit plain Python lists there and miss the
            # released string.
            #
            # Everything else stays on to_list(), because to_pandas() UPCASTS a
            # numeric list with nulls ([1, None, 3] -> [1.0 nan 3.0]) and would
            # silently rewrite the repr of any numeric list column.
            dtype = df.schema[col]
            inner = getattr(dtype, "inner", None)
            if isinstance(inner, pl.Struct):
                return list(df.get_column(col).to_pandas())
            return df.get_column(col).to_list()

        df = df.with_columns([pl.Series(c, [_np_repr(v) for v in _entries(c)], dtype=pl.Utf8) for c in list_cols])
    # R: ymd_hm(substr(date, 1, nchar - 1)) parsed UTC -> America/New_York;
    # game_date is the New York date of the kickoff instant.
    df = df.with_columns(
        pl.col("date")
        .str.replace(r"Z$", "")
        .str.strptime(pl.Datetime("us"), "%Y-%m-%dT%H:%M", strict=False)
        .dt.replace_time_zone("UTC")
        .dt.convert_time_zone("America/New_York")
        .alias("game_date_time")
    )
    df = df.with_columns(pl.col("game_date_time").dt.date().alias("game_date"))
    # R: ifelse(game_id %in% ids, TRUE, FALSE) -- NA game_id folds to FALSE.
    df = df.with_columns(
        pl.col("game_id").is_in(pbp_game_ids).fill_null(False).alias("PBP"),
        pl.col("game_id").is_in(team_box_game_ids).fill_null(False).alias("team_box"),
        pl.col("game_id").is_in(player_box_game_ids).fill_null(False).alias("player_box"),
    )
    # R: distinct() then arrange(desc(date)) -- both stable, NA last.
    return df.unique(maintain_order=True, keep="first").sort(
        "date", descending=True, nulls_last=True, maintain_order=True
    )
