from __future__ import annotations

import datetime

import polars as pl

from sportsdataverse._codegen_runtime import _download_json
from sportsdataverse._espn_schedule import _schedule_frame
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError as _AssetFetchError
from sportsdataverse.errors import SeasonNotFoundError


def espn_mbb_schedule(
    dates=None,
    groups=50,
    season_type=None,
    limit=500,
    return_as_pandas=False,
    **kwargs,
) -> pl.DataFrame:
    """espn_mbb_schedule - look up the men's college basketball scheduler for a given season

    Args:
        dates (int): A date (``YYYYMMDD``) or a season (``YYYY``, 2002 or later). A season reads that
            season's game days from ESPN's scoreboard calendar and requests each day in turn
            (one calendar request plus ~150 day requests), keeping the games ESPN files under the
            season, sorted by date. A day that fails raises ``AssetFetchError``.
        groups (int): Used to define different divisions. 50 is Division I, 51 is Division II/Division III.
        season_type (int): 2 for regular season, 3 for post-season, 4 for off-season.
        limit (int): number of records to return, default: 500. A season pull sends it per day, capped
            at 1000 (ESPN answers 25 events above that), and raises ``AssetFetchError`` for a day that
            fills it.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.
    Returns:
        pl.DataFrame: Polars dataframe containing schedule dates for the requested season. A day or season
            with no games returns a zero-row frame carrying the documented columns.

    Example:
        Single date (April 8, 2024 - 2024 NCAA M championship day)::

            from sportsdataverse.mbb import espn_mbb_schedule
            day = espn_mbb_schedule(dates=20240408)
            print(day.shape)

        Season-level pull (2024 season)::

            season = espn_mbb_schedule(dates=2024)
            print(season.shape)

        Filter to a specific team (Duke ``team_id=150``)::

            import polars as pl
            duke = season.filter(
                (pl.col("home_id") == 150) | (pl.col("away_id") == 150)
            )

        Pandas round-trip::

            season_pd = espn_mbb_schedule(dates=2024, return_as_pandas=True)
            season_pd.head()

        See Also:
            * `hoopR`_ - R sister package
            * `cfbfastR`_ - companion R package for college football
            * `ESPN`_ - data origin

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
        .. _ESPN: https://www.espn.com
    """
    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"
    params = {
        "dates": dates,
        "seasontype": season_type,
        "groups": groups if groups is not None else "50",
        "limit": limit,
    }
    if dates is None or len(str(dates)) != 4:
        events = _download_json(download, url, params=params, **kwargs).get("events")
    else:
        events = _season_events(int(dates), url, params, **kwargs)
    return _schedule_frame(events, scoreboard_event_parsing, "mbb", return_as_pandas=return_as_pandas)


_DAY_LIMIT = 1000


def _season_events(season: int, url: str, params: dict, **kwargs) -> list:
    """Every event ESPN assigns to ``season``, one request per game day (B-M13, R855-1).

    ESPN reads a bare ``dates=YYYY`` as the calendar year and caps ``limit`` (500 events reach only
    January 13), so a whole-year request returns a fraction of the season. The day list comes from
    the season's own scoreboard calendar (:func:`espn_mbb_calendar`, ~150 days from opening night
    to the title game); the days are fetched one at a time, in order. Events ESPN files under
    another season are dropped, a game listed on two days is kept once, and the result is sorted
    by date. A day that fails raises (``AssetFetchError``), so a gap is never silent.
    """
    days = espn_mbb_calendar(season, **kwargs)["dateURL"].unique(maintain_order=True).to_list()
    # R855-8: above ~1000 ESPN silently answers 25 events, so a day never asks for more than 1000 (the
    # busiest 2023-24 day had 184). A day that fills its limit may hold more games, so it raises.
    limit = min(int(params.get("limit") or _DAY_LIMIT), _DAY_LIMIT)
    events: list = []
    for day in days:  # ponytail: sequential on purpose; ESPN 403s under parallel load
        payload = _download_json(download, url, params={**params, "dates": day, "limit": limit}, **kwargs)
        if not isinstance(payload, dict) or not isinstance(payload.get("events"), list):
            raise _AssetFetchError(f"ESPN scoreboard for {day} answered without an events list")
        if len(payload["events"]) >= limit:
            raise _AssetFetchError(f"ESPN scoreboard for {day} filled limit={limit}; games may be missing")
        events += payload["events"]
    seen: set = set()
    keep = []
    for e in events:
        s = e.get("season") if isinstance(e, dict) else None
        if isinstance(s, dict) and s.get("year") == season and e.get("id") not in seen:
            seen.add(e.get("id"))
            keep.append(e)
    return sorted(keep, key=lambda e: e.get("date") or "")


def scoreboard_event_parsing(event):
    """Flatten one ESPN scoreboard event for the schedule frame, in place.

    Args:
        event: One element of the scoreboard payload's ``events`` list.

    Returns:
        dict: The same event, modified: ``competitions[0]`` gains ``home`` / ``away`` team
            dicts (with ``score``, ``winner``, ``currentRank``, ``linescores``, ``records``),
            ``notes_type`` / ``notes_headline`` and ``broadcast_market`` / ``broadcast_name``,
            and loses ``competitors``, ``broadcasts``, ``notes``, ``odds``, ``leaders`` and the
            other nested blocks the schedule frame does not use.
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
    event["competitions"][0][arg2]["currentRank"] = (
        event.get("competitions")[0].get("competitors")[arg1].get("curatedRank", {}).get("current", 99)
    )
    event["competitions"][0][arg2]["linescores"] = (
        event.get("competitions")[0].get("competitors")[arg1].get("linescores", [{"value": 0}, {"value": 0}])
    )
    # add linescores back to main competitors if does not exist
    event["competitions"][0]["competitors"][arg1]["linescores"] = (
        event.get("competitions")[0].get("competitors")[arg1].get("linescores", [{"value": 0}, {"value": 0}])
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


def espn_mbb_calendar(season=None, ondays=None, return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """espn_mbb_calendar - look up the men's college basketball calendar for a given season

    Args:
        season (int): Used to define different seasons. 2002 is the earliest available season;
            ``None`` means the current season.
        ondays (boolean): Used to return dates for calendar ondays
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing calendar dates for the requested season.

    Raises:
        SeasonNotFoundError: If `season` is less than 2002.

    Example:
        Calendar dates for a single season::

            from sportsdataverse.mbb import espn_mbb_calendar
            cal = espn_mbb_calendar(season=2024)
            cal.head()

        On-days only (dates with games on the schedule)::

            ondays = espn_mbb_calendar(season=2024, ondays=True)
            ondays.head()

        Pandas round-trip::

            cal_pd = espn_mbb_calendar(season=2024, return_as_pandas=True)
            cal_pd.head()

        See Also:
            * `hoopR`_ - R sister package
            * `cfbfastR`_ - companion R package for college football
            * `ESPN`_ - data origin

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
        .. _ESPN: https://www.espn.com
    """
    season = most_recent_mbb_season() if season is None else int(season)  # B-L9: never `2024.0` in a URL
    if season < 2002:
        raise SeasonNotFoundError("season cannot be less than 2002")
    if ondays is not None:
        full_schedule = __ondays_mbb_calendar(season, **kwargs)
    else:
        url = f"http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?groups=50&dates={season}"
        txt = _download_json(download, url, **kwargs).get("leagues")[0].get("calendar")
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
            url="http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?groups=50&dates="
            + pl.col("dateURL"),
        )
    return full_schedule.to_pandas() if return_as_pandas else full_schedule


def __ondays_mbb_calendar(season, **kwargs):
    url = f"https://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball/seasons/{season}/types/2/calendar/ondays?groups=50"
    txt = _download_json(download, url, **kwargs).get("eventDate").get("dates")
    result = pl.DataFrame(txt, schema=["dates"])
    result = result.with_columns(dateURL=pl.col("dates").str.slice(0, 10))
    result = result.with_columns(
        url="http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?groups=50&dates="
        + pl.col("dateURL"),
    )

    return result


def most_recent_mbb_season():
    """Return the most recent men's college basketball season year.

    The men's college basketball season spans early November through early
    April; for any month October-December the "current season" is the
    following calendar year (e.g. October 2025 returns ``2026``).

    Returns:
        int: The most recent / current season year.

    Example:
        Use as a default season argument::

            from sportsdataverse.mbb import most_recent_mbb_season, espn_mbb_schedule
            season = most_recent_mbb_season()
            sched = espn_mbb_schedule(dates=season)

        See Also:
            * `hoopR`_ - R sister package
            * `cfbfastR`_ - companion R package for college football

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    if datetime.datetime.now().month >= 10:
        return datetime.datetime.now().year + 1
    else:
        return datetime.datetime.now().year


def helper_mbb_schedule(
    sched: pl.DataFrame,
    *,
    pbp_game_ids: list[int],
    team_box_game_ids: list[int],
    player_box_game_ids: list[int],
) -> pl.DataFrame:
    """Reshape the raw MBB season schedule into the released schedule frame.

    Faithful polars port of the schedule blocks in the hoopR-mbb-data
    creation scripts (``espn_mbb_01_pbp_creation.R`` adds casts +
    ``game_date_time``/``game_date`` + the ``PBP`` flag; ``02`` stamps
    ``team_box``; ``03`` stamps ``player_box`` and uploads) -- byte-identical
    to the WBB blocks after league normalization, so this delegates to the
    shared implementation. Column order and dtypes mirror the released
    ``mbb_schedule_{season}.parquet``.

    Args:
        sched: The raw ``hoopR-mbb-raw`` season schedule frame
            (``mbb/schedules/parquet/mbb_schedule_{season}.parquet``).
        pbp_game_ids: Game ids present in the compiled play_by_play dataset.
        team_box_game_ids: Game ids present in the compiled team_box dataset.
        player_box_game_ids: Game ids present in the compiled player_box dataset.

    Returns:
        pl.DataFrame: One row per game, deduped, sorted by ``date`` descending.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.mbb import helper_mbb_schedule
            raw = pl.read_parquet("mbb_schedule_2025.parquet")
            df = helper_mbb_schedule(
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
