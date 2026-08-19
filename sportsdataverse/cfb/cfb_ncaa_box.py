"""Parse stats.ncaa.org college-football (NCAA sport code ``MFB``) game-detail
pages other than play-by-play into tidy polars frames.

One parser per stats.ncaa.org tab of a contest (all bm-verify-gated, fetched via
the shared browser transport in :mod:`sportsdataverse.mbb.mbb_ncaa_fetch`):

* :func:`parse_cfb_ncaa_drives` -- ``/contests/{id}/drives`` (drive-level box).
* :func:`parse_cfb_ncaa_scoring_summary` -- the ``/contests/{id}/box_score``
  scoring-summary table (one row per score, running score, OT as period 5+).
* :func:`parse_cfb_ncaa_team_stats` -- ``/contests/{id}/team_stats`` (team box,
  **with per-quarter breakdown** -- one row per category/stat/period).
* :func:`parse_cfb_ncaa_player_stats` -- ``/contests/{id}/individual_stats``
  (individual player box; a ``dict`` of one frame per stat category).
* :func:`parse_cfb_ncaa_officials` -- ``/contests/{id}/officials`` (officiating crew).
* :func:`parse_cfb_ncaa_linescore` -- the linescore + game info (date/venue/
  attendance) block shared by every tab.

**Provenance.** Original sdv-py code, built against a real capture (contest
5362283, California @ Auburn 2024-09-07). Empty/unparseable input returns a
zero-row frame (or empty dict) with the documented schema. The play-by-play tab
is parsed separately by :func:`sportsdataverse.cfb.cfb_ncaa_pbp.parse_cfb_ncaa_pbp`.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Union

import polars as pl
from bs4 import BeautifulSoup

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "DRIVES_SCHEMA",
    "LINESCORE_SCHEMA",
    "OFFICIALS_SCHEMA",
    "SCORING_SUMMARY_SCHEMA",
    "TEAM_STATS_SCHEMA",
    "parse_cfb_ncaa_drives",
    "parse_cfb_ncaa_linescore",
    "parse_cfb_ncaa_officials",
    "parse_cfb_ncaa_player_stats",
    "parse_cfb_ncaa_scoring_summary",
    "parse_cfb_ncaa_team_stats",
]

# Period-row labels: "1st Quarter", and every OT spelling stats.ncaa.org emits
# ("OT", "OT2", "1OT", "2OT", any case). _QUARTER_RE classifies period rows
# (team stats / linescore); _OT_PERIOD_RE numbers them -- they MUST accept the
# same OT forms or a "1OT" row gets classified as a team/stat name.
_OT_LABEL = r"(?:\d*\s*OT\s*\d*)"
_QUARTER_RE = re.compile(rf"^(?:(\d+)(?:st|nd|rd|th)\s+Quarter|{_OT_LABEL})$", re.I)
_OT_PERIOD_RE = re.compile(r"^(?:(\d+)\s*OT|OT\s*(\d*))$", re.I)
_COMPETITOR_ID_RE = re.compile(r"competitor_(\d+)_year_stat_category_(\d+)_data_table")


def _cells(tr: "Any") -> "list[str]":
    return [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]


def _rows(table: "Any") -> "list[list[str]]":
    return [_cells(tr) for tr in table.find_all("tr")]


def _cid(contest_id: "str | int | None") -> "str | None":
    return str(contest_id) if contest_id is not None else None


def _finish(rows: "list[dict]", schema: dict, return_as_pandas: bool) -> "Union[pl.DataFrame, pd.DataFrame]":
    df = pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)
    return df.to_pandas() if return_as_pandas else df


def _linescore_table(soup: "BeautifulSoup") -> "Any":
    """The 6-row linescore table (header ``['', '1', '2', ...,'S']`` + team rows)."""
    for t in soup.find_all("table"):
        trs = t.find_all("tr")
        if not trs:
            continue
        hdr = _cells(trs[0])
        if len(hdr) >= 3 and hdr[0] == "" and hdr[-1] == "S" and hdr[1] == "1":
            return t
    return None


def _teams(soup: "BeautifulSoup") -> "tuple[str | None, str | None]":
    """(away, home) team names -- the first two team rows of the linescore."""
    t = _linescore_table(soup)
    if t is None:
        return None, None
    names = [r[0] for r in _rows(t)[1:] if r and r[0] and not _QUARTER_RE.match(r[0])]
    names = [n for n in names if not re.match(r"^\d", n) and "Stadium" not in n and "Attendance" not in n]
    away = names[0] if len(names) > 0 else None
    home = names[1] if len(names) > 1 else None
    return away, home


# --- drives ---------------------------------------------------------------

DRIVES_SCHEMA: "dict[str, pl.DataType]" = {
    "contest_id": pl.Utf8,
    "drive_number": pl.Int64,
    "quarter": pl.Int64,
    "period": pl.Int64,
    "team": pl.Utf8,
    "start_period": pl.Int64,
    "start_how": pl.Utf8,
    "start_clock": pl.Utf8,
    "start_yard_line": pl.Utf8,
    "end_period": pl.Int64,
    "end_how": pl.Utf8,
    "end_clock": pl.Utf8,
    "end_yard_line": pl.Utf8,
}


def _int(v: str) -> "int | None":
    return int(v) if v and v.lstrip("-").isdigit() else None


def _period_num(v: "str | None") -> "int | None":
    """``"3"`` -> 3, ``"1OT"``/``"OT"`` -> 5, ``"2OT"``/``"OT2"`` -> 6 (OT periods continue
    after the 4th; case-insensitive, an unnumbered ``OT`` is the first one)."""
    if not v:
        return None
    v = v.strip()
    m = _OT_PERIOD_RE.match(v)
    if m:
        n = m.group(1) or m.group(2) or "1"
        return 4 + int(n)
    return int(v) if v.isdigit() else None


def parse_cfb_ncaa_drives(
    html: str, contest_id: "str | int | None" = None, *, return_as_pandas: bool = False
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Parse the ``/contests/{id}/drives`` page -> one row per drive.

    Columns: ``drive_number``, ``quarter``, ``period``, ``team``,
    ``start_period``/``start_how`` (KO/PUNT/DOWNS/…)/``start_clock``/
    ``start_yard_line``, and the ``end_*`` equivalents (``end_how`` =
    TD/FGA/PUNT/DOWNS/…).

    ``quarter`` is the page's Quarter cell as an int and is **null for overtime
    drives** (the cell reads ``"1OT"``/``"2OT"``); ``period`` preserves those as
    5, 6, … so OT drives stay addressable.
    """
    soup = BeautifulSoup(html or "", "html.parser")
    cid = _cid(contest_id)
    table = soup.find("table", id="public_game_drives_data_table")
    rows: "list[dict]" = []
    if table is not None:
        for r in _rows(table)[1:]:
            if len(r) < 11 or not (r[0] or "").isdigit():
                continue
            rows.append(
                {
                    "contest_id": cid,
                    "drive_number": _int(r[0]),
                    "quarter": _int(r[1]),
                    "period": _period_num(r[1]),
                    "team": r[2] or None,
                    "start_period": _int(r[3]),
                    "start_how": r[4] or None,
                    "start_clock": r[5] or None,
                    "start_yard_line": r[6] or None,
                    "end_period": _int(r[7]),
                    "end_how": r[8] or None,
                    "end_clock": r[9] or None,
                    "end_yard_line": r[10] or None,
                }
            )
    return _finish(rows, DRIVES_SCHEMA, return_as_pandas)


# --- scoring summary ------------------------------------------------------

SCORING_SUMMARY_SCHEMA: "dict[str, pl.DataType]" = {
    "contest_id": pl.Utf8,
    "period": pl.Int64,
    "clock": pl.Utf8,
    "team": pl.Utf8,
    "play_text": pl.Utf8,
    "n_plays": pl.Int64,
    "yards": pl.Int64,
    "top": pl.Utf8,
    "score_away": pl.Int64,
    "score_home": pl.Int64,
}


def parse_cfb_ncaa_scoring_summary(
    html: str, contest_id: "str | int | None" = None, *, return_as_pandas: bool = False
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Parse the ``/contests/{id}/box_score`` scoring-summary table -> one row per score.

    The ``scoring_summary_table`` ``tr`` elements concatenate logical rows, so the
    cells are flattened and re-chunked by the 9-column header (Period, Time,
    Has Ball, Play, Plays, Yards, TOP, away, home) and de-duplicated preserving
    order. ``period`` keeps overtime as 5, 6, … (``"1OT"`` -> 5); ``team`` ("Has
    Ball") and ``play_text`` are null when the page leaves them blank (common on
    OT rows); ``score_away`` / ``score_home`` are the running score after the play.

    Args:
        html: Raw HTML of the ``/contests/{id}/box_score`` page.
        contest_id: Optional stats.ncaa.org contest id, written to every row's
            ``contest_id`` column. Coerced to ``str``.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of ``polars``.

    Returns:
        A ``polars.DataFrame`` (or ``pandas.DataFrame`` when ``return_as_pandas``)
        with one row per scoring play. Empty/unparseable input returns a
        **zero-row frame carrying the documented schema**.

    Example:
        Quick start::

            from sportsdataverse.cfb import parse_cfb_ncaa_scoring_summary
            df = parse_cfb_ncaa_scoring_summary(open("box_score_6386512.html").read(), contest_id=6386512)
            print(df.shape)

        Overtime scores only::

            df.filter(pl.col("period") > 4).select("period", "play_text", "score_away", "score_home")

        See Also:
            * `cfbfastR`_ -- ESPN-sourced college-football scoring plays (R)

        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    soup = BeautifulSoup(html or "", "html.parser")
    cid = _cid(contest_id)
    table = soup.find("table", id="scoring_summary_table")
    rows: "list[dict]" = []
    if table is not None:
        cells = [c.get_text(" ", strip=True) for c in table.find_all(["th", "td"])]
        flat = [c for c in cells if c != "Scoring Summary"]  # drop the title cell
        if len(flat) >= 9 and flat[0] == "Period":  # drop the 9-cell header
            flat = flat[9:]
        for i in range(0, len(flat) - 8, 9):
            chunk = flat[i : i + 9]
            period = _period_num(chunk[0])
            if period is None:
                continue
            rows.append(
                {
                    "contest_id": cid,
                    "period": period,
                    "clock": chunk[1] or None,
                    "team": chunk[2] or None,
                    "play_text": chunk[3] or None,
                    "n_plays": _int(chunk[4]),
                    "yards": _int(chunk[5]),
                    "top": chunk[6] or None,
                    "score_away": _int(chunk[7]),
                    "score_home": _int(chunk[8]),
                }
            )
    df = pl.DataFrame(rows, schema=SCORING_SUMMARY_SCHEMA) if rows else pl.DataFrame(schema=SCORING_SUMMARY_SCHEMA)
    df = df.unique(maintain_order=True)  # the tr-concatenation duplicates rows
    return df.to_pandas() if return_as_pandas else df


# --- officials ------------------------------------------------------------

OFFICIALS_SCHEMA: "dict[str, pl.DataType]" = {
    "contest_id": pl.Utf8,
    "role": pl.Utf8,
    "official": pl.Utf8,
}


def parse_cfb_ncaa_officials(
    html: str, contest_id: "str | int | None" = None, *, return_as_pandas: bool = False
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Parse the ``/contests/{id}/officials`` page -> one row per official.

    ``role`` is populated when the crew is listed with positions (Referee, Umpire,
    …); when the page lists only names it is null.
    """
    soup = BeautifulSoup(html or "", "html.parser")
    cid = _cid(contest_id)
    rows: "list[dict]" = []
    for t in soup.find_all("table"):
        trs = t.find_all("tr")
        if not trs:
            continue
        hdr = _cells(trs[0])
        # the officials data table has a short header ("Official" / "Role","Official");
        # the tab-nav row also contains "Officials" but has many cells -- exclude it.
        if len(hdr) > 3 or not any("official" in h.lower() for h in hdr):
            continue
        two_col = len(hdr) >= 2
        for r in _rows(t)[1:]:
            if not r or not any(r):
                continue
            role = r[0] if two_col and len(r) >= 2 else None
            name = r[1] if two_col and len(r) >= 2 else r[0]
            if name:
                rows.append({"contest_id": cid, "role": role or None, "official": name})
        break  # only the first officials table
    return _finish(rows, OFFICIALS_SCHEMA, return_as_pandas)


# --- team stats (by period) ----------------------------------------------

TEAM_STATS_SCHEMA: "dict[str, pl.DataType]" = {
    "contest_id": pl.Utf8,
    "category": pl.Utf8,
    "stat": pl.Utf8,
    "period": pl.Utf8,
    "away_team": pl.Utf8,
    "away_value": pl.Utf8,
    "home_team": pl.Utf8,
    "home_value": pl.Utf8,
}


def parse_cfb_ncaa_team_stats(
    html: str, contest_id: "str | int | None" = None, *, return_as_pandas: bool = False
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Parse the ``/contests/{id}/team_stats`` page -> one row per category/stat/period.

    ``period`` is ``"total"`` for the game total and ``"1st Quarter"`` / … / ``"OT"``
    for the per-quarter breakdown that stats.ncaa.org nests under each stat.
    """
    soup = BeautifulSoup(html or "", "html.parser")
    cid = _cid(contest_id)
    table = soup.find("table", id="rankings_table")
    rows: "list[dict]" = []
    if table is not None:
        away_team, home_team = None, None
        category, stat = None, None
        for r in _rows(table):
            ne = [c for c in r if c]
            if not ne:
                continue
            if len(ne) >= 3 and ne[0] in ("Period Stats", "Team Stats"):
                away_team, home_team = ne[1], ne[2]
                continue
            if len(ne) == 1:  # a category section header (Rushing, Passing, ...)
                category = ne[0]
                stat = None
                continue
            if len(r) >= 3:
                label, away_v, home_v = r[0], r[1], r[2]
                if _QUARTER_RE.match(label or ""):
                    period = label  # per-quarter row for the current stat
                else:
                    stat = label
                    period = "total"
                rows.append(
                    {
                        "contest_id": cid,
                        "category": category,
                        "stat": stat,
                        "period": period,
                        "away_team": away_team,
                        "away_value": away_v or None,
                        "home_team": home_team,
                        "home_value": home_v or None,
                    }
                )
    return _finish(rows, TEAM_STATS_SCHEMA, return_as_pandas)


# --- individual player stats ---------------------------------------------

_CATEGORY_HINTS = [
    ("rushing", ("Rush Attempts", "Yds/Rush")),
    ("passing", ("Pass Attempts", "Completions")),
    ("receiving", ("Rec", "Receiving Yards")),
    ("defense", ("Tackles", "Solo", "Sacks")),
    ("kicking", ("FG Made", "FGM", "XP Made")),
    ("punting", ("Punts", "Punt Yds")),
    ("kick_returns", ("Kick Ret", "KR Yds")),
    ("punt_returns", ("Punt Ret", "PR Yds")),
]


def _category_of(header: "list[str]") -> str:
    hset = set(header)
    for name, hints in _CATEGORY_HINTS:
        if any(h in hset for h in hints):
            return name
    return "other"


def parse_cfb_ncaa_player_stats(
    html: str, contest_id: "str | int | None" = None, *, return_as_pandas: bool = False
) -> "dict[str, Union[pl.DataFrame, pd.DataFrame]]":
    """Parse the ``/contests/{id}/individual_stats`` page.

    Returns a ``dict`` keyed by stat category (``"rushing"``, ``"passing"``,
    ``"receiving"``, …) -> a frame of that category's player rows across both
    teams. Each frame carries ``contest_id``, ``team_id``, ``number``, ``name``,
    ``position`` + the category's stat columns (snake-cased). Empty input -> ``{}``.
    """
    soup = BeautifulSoup(html or "", "html.parser")
    cid = _cid(contest_id)
    buckets: "dict[str, list[dict]]" = {}
    for t in soup.find_all("table"):
        m = _COMPETITOR_ID_RE.match(t.get("id") or "")
        if not m:
            continue
        team_id = m.group(1)
        trs = t.find_all("tr")
        if len(trs) < 2:
            continue
        header = _cells(trs[0])
        cat = _category_of(header)
        # stat columns = everything past the fixed #/Name/P triple
        stat_cols = [re.sub(r"[^\w]+", "_", c).strip("_").lower() for c in header[3:]]
        for r in _rows(t)[1:]:
            if len(r) < 3 or not any(r):
                continue
            row = {
                "contest_id": cid,
                "team_id": team_id,
                "number": r[0] or None,
                "name": r[1] or None,
                "position": r[2] or None,
            }
            for col, val in zip(stat_cols, r[3:]):
                row[col] = val or None
            buckets.setdefault(cat, []).append(row)
    out: "dict" = {}
    for cat, rows in buckets.items():
        df = pl.DataFrame(rows)
        out[cat] = df.to_pandas() if return_as_pandas else df
    return out


# --- linescore + game info ------------------------------------------------

LINESCORE_SCHEMA: "dict[str, pl.DataType]" = {
    "contest_id": pl.Utf8,
    "team": pl.Utf8,
    "home_away": pl.Utf8,
    "period": pl.Utf8,
    "points": pl.Int64,
    "final": pl.Int64,
    "game_date": pl.Utf8,
    "venue": pl.Utf8,
    "attendance": pl.Int64,
}


def parse_cfb_ncaa_linescore(
    html: str, contest_id: "str | int | None" = None, *, return_as_pandas: bool = False
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Parse the shared linescore + game-info block -> one row per team/period.

    ``period`` is the quarter label (``"1"``..``"4"``, plus ``"OT"`` variants when
    present); ``points`` the score that period; ``final`` the team's final; and
    ``game_date`` / ``venue`` / ``attendance`` the game-info line repeated per row.
    """
    soup = BeautifulSoup(html or "", "html.parser")
    cid = _cid(contest_id)
    table = _linescore_table(soup)
    rows: "list[dict]" = []
    if table is not None:
        all_rows = _rows(table)
        header = all_rows[0]  # ['', '1', '2', '3', '4', 'S']
        periods = header[1:-1]  # quarter labels
        # game-info lines (date / venue / attendance) are single-cell rows below
        game_date, venue, attendance = None, None, None
        team_rows = []
        for r in all_rows[1:]:
            ne = [c for c in r if c]
            if len(r) >= len(header) and r[0] and not _QUARTER_RE.match(r[0]):
                team_rows.append(r)
            elif len(ne) == 1:
                v = ne[0]
                if re.search(r"\d{1,2}/\d{1,2}/\d{2,4}", v):
                    game_date = v
                elif "Attendance" in v:
                    mnum = re.search(r"([\d,]+)", v)
                    attendance = int(mnum.group(1).replace(",", "")) if mnum else None
                elif "Stadium" in v or "(" in v:
                    venue = v
        for idx, tr in enumerate(team_rows):
            team = tr[0]
            final = _int(tr[-1])
            home_away = "away" if idx == 0 else "home"
            for pi, period in enumerate(periods):
                rows.append(
                    {
                        "contest_id": cid,
                        "team": team,
                        "home_away": home_away,
                        "period": period,
                        "points": _int(tr[1 + pi]),
                        "final": final,
                        "game_date": game_date,
                        "venue": venue,
                        "attendance": attendance,
                    }
                )
    return _finish(rows, LINESCORE_SCHEMA, return_as_pandas)
