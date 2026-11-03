"""Discover stats.ncaa.org contests from the per-date scoreboard, for any sport.

Every sport's scoreboard (``season_divisions/{id}/scoreboards?game_date=...``) links each game as
``/contests/{contest_id}/box_score``, so one page per date and division lists every contest that
day: verified on men's basketball, baseball and football (FBS and D-III). That replaces sweeping
every team's schedule page (hundreds of fetches per division) for nightly discovery. The day's
page lists a game before it is played, so a capture of the same day meets contests whose pages
answer "not available" yet; treat those as not ready, not as failures.

* :func:`ncaa_scoreboard_contests` -- contest ids for one or more dates.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Callable, Iterable, Optional, Union

import polars as pl

from sportsdataverse.scrape.ncaa.seasons import ncaa_season_division_id, ncaa_sport_code

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["SCOREBOARD_CONTESTS_SCHEMA", "ncaa_scoreboard_contests", "parse_ncaa_scoreboard_contests"]

logger = logging.getLogger(__name__)

SCOREBOARD_CONTESTS_SCHEMA: "dict[str, pl.DataType]" = {
    "contest_id": pl.Utf8,
    "game_date": pl.Date,
    "sport_code": pl.Utf8,
    "season_division_id": pl.Int64,
}

_CONTEST_RE = re.compile(r"/contests/(\d+)/")
_SHOWN_DATE_RE = re.compile(r"""#game_date"\)\.val\('(\d{2}/\d{2}/\d{4})'\)""")

DateLike = Union[date, str]


def _as_date(value: DateLike) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    try:
        return datetime.strptime(text, "%Y-%m-%d" if "-" in text else "%m/%d/%Y").date()
    except ValueError:
        raise ValueError(f"unrecognised date {value!r}; expected YYYY-MM-DD, MM/DD/YYYY or a date") from None


def parse_ncaa_scoreboard_contests(
    html: str,
    game_date: DateLike,
    *,
    sport_code: Optional[str] = None,
    season_division_id: Optional[int] = None,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Parse a scoreboard page into one row per contest it links.

    Returns zero rows when the page shows a different date than *game_date* (the site falls back
    to another day for a date outside the season), so a fallback page is never filed under the
    date that was asked for.

    Args:
        html: A ``season_divisions/{id}/scoreboards`` page.
        game_date: The date the page was requested for.
        sport_code: Stamped on every row.
        season_division_id: Stamped on every row.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per contest, in page order:

        | col_name | type | description |
        |---|---|---|
        | contest_id | String | stats.ncaa.org contest id (``/contests/{id}/...``) |
        | game_date | Date | The scoreboard date |
        | sport_code | String | stats.ncaa.org sport code |
        | season_division_id | Int64 | The scoreboard's season and division id |

    Example:
        Parse a saved page::

            from sportsdataverse.scrape.ncaa.scoreboard import parse_ncaa_scoreboard_contests
            df = parse_ncaa_scoreboard_contests(open("scoreboard.html").read(), "2026-11-02")
    """
    d = _as_date(game_date)
    shown = _SHOWN_DATE_RE.search(html or "")
    ids: "list[str]" = []
    if shown is None or datetime.strptime(shown.group(1), "%m/%d/%Y").date() == d:
        ids = list(dict.fromkeys(_CONTEST_RE.findall(html or "")))
    else:
        logger.warning("scoreboard for %s shows %s instead; no contests taken from it", d, shown.group(1))
    df = pl.DataFrame(
        {
            "contest_id": ids,
            "game_date": [d] * len(ids),
            "sport_code": [sport_code] * len(ids),
            "season_division_id": [season_division_id] * len(ids),
        },
        schema=SCOREBOARD_CONTESTS_SCHEMA,
    )
    return df.to_pandas() if return_as_pandas else df


def _scoreboard_path(season_division_id: int, d: date) -> str:
    return (
        f"season_divisions/{season_division_id}/scoreboards?utf8=%E2%9C%93&season_division_id="
        f"&game_date={d.strftime('%m%%2F%d%%2F%Y')}&conference_id=0&tournament_id=&commit=Submit"
    )


def ncaa_scoreboard_contests(
    sport: str,
    dates: "Union[DateLike, Iterable[DateLike], None]" = None,
    division: "Union[int, str, None]" = None,
    *,
    lookback_days: int = 0,
    fetch_html: "Optional[Callable[[str], str]]" = None,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Contest ids for a sport on one or more dates, one scoreboard page per date.

    The season and division ids are resolved for you (see
    :func:`~sportsdataverse.scrape.ncaa.seasons.ncaa_season_division_id`), per date, so a date
    range may cross seasons.

    Args:
        sport: Sport name, alias or code (``"mbb"``, ``"football"``, ``"baseball"``).
        dates: A date, an iterable of dates (``date`` or ``"YYYY-MM-DD"``/``"MM/DD/YYYY"``), or
            ``None`` for yesterday and today.
        division: The division (see ``ncaa_season_division_id``); ``None`` -> the top division.
        lookback_days: Also fetch this many days before the earliest date, to pick up results
            posted late.
        fetch_html: ``path -> html`` (a vendor transport or an offline fake). Default: one
            :class:`~sportsdataverse.mbb.mbb_ncaa_fetch.NcaaFetcher` for the whole call.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        The :data:`SCOREBOARD_CONTESTS_SCHEMA` frame, one row per contest and date, deduplicated.

    Raises:
        ValueError: Unknown sport, division or date format.
        SeasonNotFoundError: A date falls in a season the site does not list for the division.

    Example:
        Last night's men's basketball contests::

            from sportsdataverse.scrape.ncaa.scoreboard import ncaa_scoreboard_contests
            df = ncaa_scoreboard_contests("mbb", "2026-11-02")

        A football Saturday at FCS::

            df = ncaa_scoreboard_contests("football", "2026-10-31", division="FCS")
    """
    code = ncaa_sport_code(sport)
    if dates is None:
        today = date.today()
        wanted = [today - timedelta(days=1), today]
    elif isinstance(dates, (str, date)):
        wanted = [_as_date(dates)]
    else:
        wanted = [_as_date(x) for x in dates]
    if wanted and lookback_days > 0:
        first = min(wanted)
        wanted += [first - timedelta(days=n) for n in range(1, lookback_days + 1)]
    wanted = sorted(set(wanted))

    def _run(fetch: Callable[[str], str]) -> pl.DataFrame:
        frames = []
        for d in wanted:
            academic_year = d.year + 1 if d.month >= 7 else d.year
            sid = ncaa_season_division_id(code, academic_year=academic_year, division=division, fetch_html=fetch)
            page = fetch(_scoreboard_path(sid, d))
            frames.append(parse_ncaa_scoreboard_contests(page, d, sport_code=code, season_division_id=sid))
        if not frames:
            return pl.DataFrame(schema=SCOREBOARD_CONTESTS_SCHEMA)
        return pl.concat(frames).unique(["contest_id", "game_date"], keep="first", maintain_order=True)

    if fetch_html is not None:
        df = _run(fetch_html)
    else:
        from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher

        with NcaaFetcher.with_browser() as fetcher:
            df = _run(lambda path: fetcher.fetch_html(path, force=True))
    return df.to_pandas() if return_as_pandas else df
