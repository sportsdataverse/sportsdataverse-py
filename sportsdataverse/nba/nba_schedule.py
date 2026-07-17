from __future__ import annotations

import datetime

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import download


def espn_nba_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """espn_nba_schedule - look up the NBA schedule for a given date from ESPN

    Args:
        dates (int): Used to define different seasons. 2002 is the earliest available season.
        season_type (int): season type, 1 for pre-season, 2 for regular season, 3 for post-season,
        4 for all-star, 5 for off-season
        limit (int): number of records to return, default: 500.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing schedule dates for the requested season. Returns None if no games

    Example:
        Quick start (today's slate)::

            from sportsdataverse.nba import espn_nba_schedule
            slate = espn_nba_schedule()
            print(slate.shape)

        Pull a specific date::

            jan2 = espn_nba_schedule(dates=20230102, season_type=2)

        Pipeline next step (extract finals only)::

            import polars as pl
            finals = espn_nba_schedule(dates=20230102).filter(
                pl.col("status_type_completed") == True
            )

        See Also:
            * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R sister package for NBA data
            * `nba_api <https://github.com/swar/nba_api>`_ -- Python alternative to the NBA Stats API
    """
    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
    params = {"dates": dates, "seasonType": season_type, "limit": limit}
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
    """Internal helper that flattens an ESPN NBA scoreboard event dict into a
    shape suitable for ``pd.json_normalize``.

    Args:
        event (dict): A single scoreboard ``events[*]`` entry from the ESPN
            NBA scoreboard API.

    Returns:
        dict: The same event dict, mutated in place with ``home``/``away``
        copies of the competitors and trimmed of unused link/odds keys.

    Example:
        Used internally by :func:`espn_nba_schedule`::

            from sportsdataverse.nba import espn_nba_schedule
            sched = espn_nba_schedule(dates=20230102)
    """
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
            ],
        )
    )
    return event


def espn_nba_calendar(season=None, ondays=None, return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """espn_nba_calendar - look up the NBA calendar for a given season from ESPN

    Args:
        season (int): Used to define different seasons. 2002 is the earliest available season.
        ondays (boolean): Used to return dates for calendar ondays
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing calendar dates for the requested season.

    Raises:
        ValueError: If `season` is less than 2002.

    Example:
        Quick start::

            from sportsdataverse.nba import espn_nba_calendar
            cal = espn_nba_calendar(season=2023)
            print(cal.shape)

        Use ondays to get every scheduled date for the season::

            ondays = espn_nba_calendar(season=2023, ondays=True)

        Pipeline next step (loop the URLs to scrape day-by-day)::

            cal = espn_nba_calendar(season=2023, ondays=True)
            urls = cal["url"].to_list()  # feed each into espn_nba_schedule

        See Also:
            * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R sister package for NBA data
    """
    if ondays is not None:
        full_schedule = __ondays_nba_calendar(season, **kwargs)
    else:
        url = f"http://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={season}"
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
            url="http://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates=" + pl.col("dateURL"),
        )
    return full_schedule.to_pandas() if return_as_pandas else full_schedule


def __ondays_nba_calendar(season, **kwargs):
    url = f"https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/seasons/{season}/types/2/calendar/ondays"
    resp = download(url=url, **kwargs)
    txt = resp.json().get("eventDate").get("dates")
    result = pl.DataFrame(txt, schema=["dates"])
    result = result.with_columns(dateURL=pl.col("dates").str.slice(0, 10))
    result = result.with_columns(
        url="http://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates=" + pl.col("dateURL"),
    )

    return result


def most_recent_nba_season():
    """Return the most recent NBA season year based on today's date.

    The NBA season crosses calendar years -- a season started in October of
    year Y is reported as season Y+1. If today is in October or later, this
    returns next calendar year; otherwise it returns the current calendar year.

    Returns:
        int: The most recent NBA season year (e.g. 2024 for the 2023-24 season).

    Example:
        Quick start::

            from sportsdataverse.nba import most_recent_nba_season
            year = most_recent_nba_season()
            print(year)

        Combine with the loaders for a "current season" pull::

            from sportsdataverse.nba import load_nba_schedule, most_recent_nba_season
            sched = load_nba_schedule(seasons=[most_recent_nba_season()])
    """
    if int(str(datetime.date.today())[5:7]) >= 10:
        return int(str(datetime.date.today())[:4]) + 1
    else:
        return int(str(datetime.date.today())[:4])


def year_to_season(year):
    """Convert a season START year (e.g. 2023) to the NBA's hyphenated label
    (e.g. ``"2023-24"``).

    Callers working in the end-year convention pass ``end_year - 1`` (e.g.
    ``year_to_season(most_recent_nba_season() - 1)``).

    Handles century rollover (1999 -> ``"1999-00"``) and zero-pads the
    second half of the label.

    Args:
        year (int): The starting calendar year of the season (e.g. 2023 for
            the 2023-24 season).

    Returns:
        str: NBA-style season label.

    Example:
        Quick start::

            from sportsdataverse.nba import year_to_season
            label = year_to_season(2023)
            print(label)  # "2023-24"

        Century rollover::

            print(year_to_season(1999))  # "1999-00"
    """
    first_year = str(year)[2:4]
    next_year = int(first_year) + 1
    if int(next_year) < 10 and int(first_year) >= 0:
        next_year_formatted = f"0{next_year}"
    elif int(first_year) == 99:
        next_year_formatted = "00"
    else:
        next_year_formatted = str(next_year)
    return f"{year}-{next_year_formatted}"


def helper_nba_schedule(
    sched: pl.DataFrame,
    *,
    pbp_game_ids: list[int],
    team_box_game_ids: list[int],
    player_box_game_ids: list[int],
) -> pl.DataFrame:
    """Reshape the raw NBA season schedule into the released schedule frame.

    Faithful polars port of the schedule blocks in the hoopR-nba-data
    creation scripts (``espn_nba_01_pbp_creation.R`` adds casts +
    ``game_date_time``/``game_date`` + the ``PBP`` flag; ``02`` stamps
    ``team_box``; ``03`` stamps ``player_box`` and uploads) -- byte-identical
    to the WBB blocks after league normalization, so this delegates to the
    shared implementation. College-only columns (conference ids, ranks,
    tournament/groups) are simply absent from NBA raw schedules; every cast
    in the shared helper is presence-guarded. Column order and dtypes mirror
    the released ``nba_schedule_{season}.parquet``.

    Args:
        sched: The raw ``hoopR-nba-raw`` season schedule frame
            (``nba/schedules/parquet/nba_schedule_{season}.parquet``).
        pbp_game_ids: Game ids present in the compiled play_by_play dataset.
        team_box_game_ids: Game ids present in the compiled team_box dataset.
        player_box_game_ids: Game ids present in the compiled player_box dataset.

    Returns:
        pl.DataFrame: One row per game, deduped, sorted by ``date`` descending.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba import helper_nba_schedule
            raw = pl.read_parquet("nba_schedule_2025.parquet")
            df = helper_nba_schedule(
                raw, pbp_game_ids=[], team_box_game_ids=[], player_box_game_ids=[]
            )
            print(df.shape)

    See Also:
        * `hoopR`_ -- the R producer this ports; retained as the parity oracle.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    from sportsdataverse.wbb.wbb_schedule import helper_wbb_schedule

    return helper_wbb_schedule(
        sched,
        pbp_game_ids=pbp_game_ids,
        team_box_game_ids=team_box_game_ids,
        player_box_game_ids=player_box_game_ids,
    )
