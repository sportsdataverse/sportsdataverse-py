from __future__ import annotations

import datetime

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import download


def espn_nhl_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """espn_nhl_schedule - look up the NHL schedule for a given date

    Args:
        dates (int): Used to define different seasons. 2002 is the earliest available season.
        season_type (int): season type, 1 for pre-season, 2 for regular season, 3 for post-season, 4 for all-star, 5 for off-season
        limit (int): number of records to return, default: 500.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing schedule dates for the requested season. Returns None if no games

    Example:
        Pull a single date's slate (YYYYMMDD)::

            from sportsdataverse.nhl import espn_nhl_schedule
            sched = espn_nhl_schedule(dates=20230613)  # 2023 Stanley Cup Final game date
            print(sched.shape)
            sched.select(["game_id", "home_name", "away_name", "status_type_description"]).head()

        Pull a regular-season slate from a season-year::

            reg = espn_nhl_schedule(dates=2023, season_type=2, limit=500)
            reg.group_by("status_type_description").len().sort("len", descending=True)

        Pandas round-trip for one date::

            espn_nhl_schedule(dates=20230613, return_as_pandas=True).head()

        See Also:
            * `fastRhockey`_ — R companion package; mirrors this surface
            * `nhl-api-py`_ — alternative Python source for the NHL stats API

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
        .. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
    """

    url = "http://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard"
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
        .get("linescores", [{"value": 0}, {"value": 0}, {"value": 0}])
    )
    # add linescores back to main competitors if does not exist
    event["competitions"][0]["competitors"][arg1]["linescores"] = (
        event.get("competitions")[0]
        .get("competitors")[arg1]
        .get("linescores", [{"value": 0}, {"value": 0}, {"value": 0}])
    )
    event["competitions"][0][arg2]["records"] = (
        event.get("competitions")[0]
        .get("competitors")[arg1]
        .get(
            "records",
            [
                {"abbreviation": "Game", "name": "YTD", "summary": "0-0", "type": "ytd"},
                {"abbreviation": "HOME", "name": "Home", "summary": "0-0", "type": "home"},
                {"abbreviation": "AWAY", "name": "Road", "summary": "0-0", "type": "road"},
            ],
        )
    )


def espn_nhl_calendar(season=None, ondays=None, return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """espn_nhl_calendar - look up the NHL calendar for a given season

    Args:
        season (int): Used to define different seasons. 2002 is the earliest available season.
        ondays (boolean): Used to return dates for calendar ondays
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing calendar dates for the requested season.

    Raises:
        ValueError: If `season` is less than 2002.

    Example:
        Calendar dates for a season::

            from sportsdataverse.nhl import espn_nhl_calendar
            cal = espn_nhl_calendar(season=2023)
            print(cal.shape)
            cal.head()

        Just the on-days (game-played dates), useful for batch loops::

            ondays = espn_nhl_calendar(season=2023, ondays=True)
            for url in ondays["url"].head(3).to_list():
                print(url)

        See Also:
            * `fastRhockey`_ — R companion package; mirrors this surface
            * `nhl-api-py`_ — alternative Python source for the NHL stats API

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
        .. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
    """
    if ondays is not None:
        full_schedule = __ondays_nhl_calendar(season, **kwargs)
    else:
        url = f"http://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates={season}"
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
            url="http://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates=" + pl.col("dateURL"),
        )
    return full_schedule.to_pandas() if return_as_pandas else full_schedule


def __ondays_nhl_calendar(season, **kwargs):
    url = f"https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{season}/types/2/calendar/ondays"
    resp = download(url=url, **kwargs)
    txt = resp.json().get("eventDate").get("dates")
    result = pl.DataFrame(txt, schema=["dates"])
    result = result.with_columns(dateURL=pl.col("dates").str.slice(0, 10))
    result = result.with_columns(
        url="http://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates=" + pl.col("dateURL"),
    )

    return result


def most_recent_nhl_season():
    """most_recent_nhl_season - return the season year for "today".

    NHL seasons are labeled by the year they end in. October flips the
    label to next calendar year (the new season just started), otherwise
    the current calendar year is returned.

    Returns:
        int: A season year suitable for season-aware loaders / schedule helpers.

    Example:
        Use as a default season for downstream calls::

            from sportsdataverse.nhl import most_recent_nhl_season, espn_nhl_calendar
            season = most_recent_nhl_season()
            cal = espn_nhl_calendar(season=season)
            print(season, cal.height)

        See Also:
            * `fastRhockey`_ — R companion package; mirrors this surface
            * `nhl-api-py`_ — alternative Python source for the NHL stats API

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
        .. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
    """
    if int(str(datetime.date.today())[5:7]) >= 10:
        return int(str(datetime.date.today())[:4]) + 1
    else:
        return int(str(datetime.date.today())[:4])


def year_to_season(year):
    """year_to_season - format a starting year as the canonical ``YYYY-YY`` season string.

    NHL season strings (used by ``statsapi`` / ``api-web.nhle.com``) are of the form
    ``"2023-24"``. This helper converts a starting year (``2023``) into that string.

    Args:
        year: Starting calendar year of the season (e.g. ``2023``).

    Returns:
        str: Season string formatted as ``"YYYY-YY"``.

    Example:
        Convert a starting year::

            from sportsdataverse.nhl import year_to_season
            year_to_season(2023)  # '2023-24'
            year_to_season(2009)  # '2009-10'
            year_to_season(1999)  # '1999-00'

        See Also:
            * `fastRhockey`_ — R companion package; mirrors this surface
            * `nhl-api-py`_ — alternative Python source for the NHL stats API

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
        .. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
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
