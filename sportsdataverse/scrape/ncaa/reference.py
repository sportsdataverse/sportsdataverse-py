"""Sport-generic stats.ncaa.org reference-page parsers (team list / team
schedule / roster).

These pages share one platform layout across sports (verified on football
``MFB`` and baseball ``MBA`` captures), so the parsers are sport-neutral:
graduated from ``ncaa-mfb-football-raw``'s stage-05 builders and validated
against real baseball fixtures (``tests/fixtures/ncaa_reference/``). The
producer repos feed them the persisted HTML their capture stages write.

* :func:`parse_ncaa_team_list` -- ``team/inst_team_list`` page -> one row per team.
* :func:`parse_ncaa_team_schedule` -- a team page's schedule table -> one row per
  game. Baseball doubleheaders print the date as ``MM/DD/YYYY(N)``; the raw
  ``date`` is preserved and ``game_number`` carries the ``(N)`` (null when absent).
* :func:`parse_ncaa_team_roster` -- a team roster page -> one row per player,
  header-keyed (teams vary in which columns they publish).
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Optional, Union

import polars as pl

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "TEAM_LIST_SCHEMA",
    "TEAM_ROSTER_SCHEMA",
    "TEAM_SCHEDULE_SCHEMA",
    "parse_ncaa_team_list",
    "parse_ncaa_team_roster",
    "parse_ncaa_team_schedule",
]

_RESULT_RE = re.compile(r"^([WLT])\s+(\d+)-(\d+)")
_TEAM_HREF_RE = re.compile(r"/teams/(\d+)")
_CONTEST_HREF_RE = re.compile(r"/contests/(\d+)/")
_PLAYER_HREF_RE = re.compile(r"/players/(\d+)")
_GAME_NUM_RE = re.compile(r"\((\d+)\)\s*$")

TEAM_LIST_SCHEMA: "dict[str, pl.DataType]" = {
    "team_id": pl.Utf8,
    "team_name": pl.Utf8,
}

TEAM_SCHEDULE_SCHEMA: "dict[str, pl.DataType]" = {
    "team_id": pl.Utf8,
    "team_name": pl.Utf8,
    "date": pl.Utf8,
    "game_number": pl.Int64,
    "opponent_id": pl.Utf8,
    "opponent": pl.Utf8,
    "result": pl.Utf8,
    "outcome": pl.Utf8,
    "team_score": pl.Int64,
    "opponent_score": pl.Int64,
    "contest_id": pl.Utf8,
    "attendance": pl.Int64,
}

TEAM_ROSTER_SCHEMA: "dict[str, pl.DataType]" = {
    "team_id": pl.Utf8,
    "team_name": pl.Utf8,
    "player_id": pl.Utf8,
    "player_name": pl.Utf8,
    "jersey": pl.Utf8,
    "statcrew_jersey": pl.Utf8,
    "player_class": pl.Utf8,
    "position": pl.Utf8,
    "height": pl.Utf8,
    "weight": pl.Int64,
    "hometown": pl.Utf8,
    "high_school": pl.Utf8,
    "games_played": pl.Int64,
    "games_started": pl.Int64,
}

_ROSTER_COLMAP = {
    "GP": "games_played",
    "GS": "games_started",
    "#": "jersey",
    "StatCrew #": "statcrew_jersey",
    "Name": "player_name",
    "Class": "player_class",
    "Position": "position",
    "Height": "height",
    "Weight": "weight",
    "Hometown": "hometown",
    "High School": "high_school",
}


def _finish(
    rows: "list[dict]", schema: "dict[str, pl.DataType]", return_as_pandas: bool
) -> "Union[pl.DataFrame, pd.DataFrame]":
    df = pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)
    return df.to_pandas() if return_as_pandas else df


def parse_ncaa_team_list(html: str, *, return_as_pandas: bool = False) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Parse a ``team/inst_team_list`` page -> one row per team.

    Args:
        html: Raw HTML of the team-list page (any sport_code / division).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of ``polars``.

    Returns:
        ``team_id`` (Utf8) + ``team_name``, order preserved, de-duplicated.
        Empty/unparseable input returns a zero-row frame with the schema.

    Example:
        Quick start::

            from sportsdataverse.scrape.ncaa.reference import parse_ncaa_team_list
            teams = parse_ncaa_team_list(open("inst_team_list.html").read())
            print(teams.height)

        See Also:
            * `baseballr`_ -- NCAA baseball via R

        .. _baseballr: https://billpetti.github.io/baseballr/
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html or "", "html.parser")
    rows: "list[dict]" = []
    seen: "set[str]" = set()
    for a in soup.select('a[href*="/teams/"]'):
        m = _TEAM_HREF_RE.search(a.get("href") or "")
        name = a.get_text(" ", strip=True)
        if m and name and m.group(1) not in seen:
            seen.add(m.group(1))
            rows.append({"team_id": m.group(1), "team_name": name})
    return _finish(rows, TEAM_LIST_SCHEMA, return_as_pandas)


def parse_ncaa_team_schedule(
    html: str,
    team_id: "Optional[str]" = None,
    *,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Parse a team page's schedule table -> one row per game.

    Args:
        html: Raw HTML of the ``/teams/{id}`` page.
        team_id: Stamped on every row (the page doesn't repeat its own id).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of ``polars``.

    Returns:
        One row per scheduled game: ``date`` exactly as printed (baseball
        doubleheaders keep their ``(1)``/``(2)`` suffix, lifted into
        ``game_number``), ``opponent_id``/``opponent``, the raw ``result``
        (e.g. ``"W 11-1"``), ``outcome`` (W/L/T), scores, ``contest_id``,
        ``attendance``. Unplayed/cancelled games keep null scores.

    Example:
        Quick start::

            from sportsdataverse.scrape.ncaa.reference import parse_ncaa_team_schedule
            games = parse_ncaa_team_schedule(open("team_page.html").read(), team_id="614839")
            print(games.select("date", "opponent", "result").head())

        See Also:
            * `baseballr`_ -- NCAA baseball via R

        .. _baseballr: https://billpetti.github.io/baseballr/
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html or "", "html.parser")
    header = soup.select_one("div.card-header")
    team_name = None
    if header:
        # strip from the W-L record on -- the header can continue past it
        # ("A&M-Corpus Christi (23-28) RPI Ranking - 202"), so an end-anchored
        # sub keeps the junk. Names with letter parentheticals (St. Thomas (MN))
        # survive because the record marker requires digits.
        team_name = re.sub(r"\s*\([\d\-]+\).*$", "", header.get_text(" ", strip=True)) or None
    table = soup.find("table")
    rows: "list[dict]" = []
    if table is not None:
        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all(["th", "td"])
            if len(cells) < 3 or not cells[0].get_text(strip=True):
                continue
            opp_a = cells[1].find("a")
            res_a = cells[2].find("a")
            opp_m = _TEAM_HREF_RE.search(opp_a.get("href") or "") if opp_a else None
            con_m = _CONTEST_HREF_RE.search(res_a.get("href") or "") if res_a else None
            result = cells[2].get_text(" ", strip=True) or None
            rm = _RESULT_RE.match(result or "")
            date = cells[0].get_text(" ", strip=True)
            gm = _GAME_NUM_RE.search(date)
            att = (cells[3].get_text(strip=True) if len(cells) > 3 else "").replace(",", "")
            rows.append(
                {
                    "team_id": team_id,
                    "team_name": team_name,
                    "date": date,
                    "game_number": int(gm.group(1)) if gm else None,
                    "opponent_id": opp_m.group(1) if opp_m else None,
                    "opponent": cells[1].get_text(" ", strip=True) or None,
                    "result": result,
                    "outcome": rm.group(1) if rm else None,
                    "team_score": int(rm.group(2)) if rm else None,
                    "opponent_score": int(rm.group(3)) if rm else None,
                    "contest_id": con_m.group(1) if con_m else None,
                    "attendance": int(att) if att.isdigit() else None,
                }
            )
    return _finish(rows, TEAM_SCHEDULE_SCHEMA, return_as_pandas)


def parse_ncaa_team_roster(
    html: str,
    team_id: "Optional[str]" = None,
    *,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Parse a team roster page -> one row per player.

    Header-keyed (teams vary in which columns they publish); ``player_id``
    comes from the ``/players/{id}`` link and ``team_name`` from the card
    header.

    Args:
        html: Raw HTML of the ``/teams/{id}/roster`` page.
        team_id: Stamped on every row.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of ``polars``.

    Returns:
        One row per player with the documented schema; columns a team doesn't
        publish are null. Empty input returns a zero-row frame with the schema.

    Example:
        Quick start::

            from sportsdataverse.scrape.ncaa.reference import parse_ncaa_team_roster
            roster = parse_ncaa_team_roster(open("roster.html").read(), team_id="614839")
            print(roster.select("player_name", "position", "player_class").head())

        See Also:
            * `baseballr`_ -- NCAA baseball via R

        .. _baseballr: https://billpetti.github.io/baseballr/
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html or "", "html.parser")
    header = soup.select_one("div.card-header")
    team_name = None
    if header:
        team_name = re.sub(r"\s*\([\d\-]+\).*$", "", header.get_text(" ", strip=True)) or None
    table = soup.find("table", id=re.compile(r"^rosters_form_players_.*_data_table$"))
    rows: "list[dict]" = []
    if table is not None:
        trs = table.find_all("tr")
        if trs:
            head = [c.get_text(" ", strip=True) for c in trs[0].find_all(["th", "td"])]
            for tr in trs[1:]:
                cells = tr.find_all(["th", "td"])
                if len(cells) != len(head):
                    continue
                rec: dict = {"team_id": team_id, "team_name": team_name}
                for h, c in zip(head, cells):
                    key = _ROSTER_COLMAP.get(h)
                    if key:
                        rec[key] = c.get_text(" ", strip=True) or None
                a = tr.find("a", href=_PLAYER_HREF_RE)
                pm = _PLAYER_HREF_RE.search(a["href"]) if a else None
                rec["player_id"] = pm.group(1) if pm else None
                for k in ("weight", "games_played", "games_started"):
                    v = rec.get(k)
                    rec[k] = int(v) if isinstance(v, str) and v.isdigit() else None
                if rec.get("player_name"):
                    rows.append(rec)
    return _finish(rows, TEAM_ROSTER_SCHEMA, return_as_pandas)
