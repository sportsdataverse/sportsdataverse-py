"""Parse the remaining stats.ncaa.org basketball contest tabs into tidy frames:
**officials**, **team stats (by period)**, and the **linescore**.

These fill the gaps left by the existing bigballR-port parsers, which already
cover ``/individual_stats`` (:func:`sportsdataverse.mbb.parse_ncaa_bb_box`),
``/play_by_play`` (:mod:`sportsdataverse.mbb.mbb_ncaa_game_pbp`) and the shot
chart. A basketball contest page has five tabs -- Box Score, Team Stats,
Individual Stats, Play By Play, Officials -- so with these three parsers every
tab is mapped.

League-agnostic (the ``parse_ncaa_bb_*`` convention): the men's and women's
pages share identical markup -- WBB runs four ``Nth Period`` quarters, MBB two
halves -- so :mod:`sportsdataverse.wbb.wbb_ncaa_box_tabs` re-exports these under
``parse_ncaa_wbb_*`` names. Built against a real capture (WBB contest 5722355,
Coppin St. @ South Carolina 2024-11-14). Empty/unparseable input returns a
zero-row frame with the documented schema.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Union

import polars as pl
from bs4 import BeautifulSoup

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "LINESCORE_SCHEMA",
    "OFFICIALS_SCHEMA",
    "TEAM_STATS_SCHEMA",
    "parse_ncaa_bb_linescore",
    "parse_ncaa_bb_officials",
    "parse_ncaa_bb_team_stats",
]

# a period sub-row inside team_stats -- basketball labels these "Nth Period"
# (WBB quarters, MBB halves); accept Quarter/Half/Inning too for cross-sport reuse.
_PERIOD_RE = re.compile(r"^(?:\d+(?:st|nd|rd|th)\s+(?:Period|Quarter|Half|Inning)|OT\s*\d*|Extra\s+Innings)$", re.I)


def _cells(tr: "Any") -> "list[str]":
    return [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]


def _rows(table: "Any") -> "list[list[str]]":
    return [_cells(tr) for tr in table.find_all("tr")]


def _cid(contest_id: "str | int | None") -> "str | None":
    return str(contest_id) if contest_id is not None else None


def _int(v: str) -> "int | None":
    return int(v) if v and v.lstrip("-").isdigit() else None


def _finish(rows: "list[dict]", schema: dict, return_as_pandas: bool) -> "Union[pl.DataFrame, pd.DataFrame]":
    df = pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)
    return df.to_pandas() if return_as_pandas else df


# --- officials ------------------------------------------------------------

OFFICIALS_SCHEMA: "dict[str, pl.DataType]" = {
    "contest_id": pl.Utf8,
    "role": pl.Utf8,
    "official": pl.Utf8,
}


def parse_ncaa_bb_officials(
    html: str, contest_id: "str | int | None" = None, *, return_as_pandas: bool = False
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Parse the ``/officials`` tab -> one row per official (the game's crew)."""
    soup = BeautifulSoup(html or "", "html.parser")
    cid = _cid(contest_id)
    rows: "list[dict]" = []
    for t in soup.find_all("table"):
        trs = t.find_all("tr")
        if not trs:
            continue
        hdr = _cells(trs[0])
        # short "Official" header -- exclude the tab-nav row (which also says "Officials").
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
        break
    return _finish(rows, OFFICIALS_SCHEMA, return_as_pandas)


# --- team stats (by period) ----------------------------------------------

TEAM_STATS_SCHEMA: "dict[str, pl.DataType]" = {
    "contest_id": pl.Utf8,
    "stat": pl.Utf8,
    "period": pl.Utf8,
    "away_team": pl.Utf8,
    "away_value": pl.Utf8,
    "home_team": pl.Utf8,
    "home_value": pl.Utf8,
}


def parse_ncaa_bb_team_stats(
    html: str, contest_id: "str | int | None" = None, *, return_as_pandas: bool = False
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Parse the ``/team_stats`` tab -> one row per stat/period.

    ``period`` is ``"total"`` for the game total and ``"1st Period"`` / … for the
    per-quarter/half breakdown stats.ncaa.org nests under each stat.
    """
    soup = BeautifulSoup(html or "", "html.parser")
    cid = _cid(contest_id)
    table = soup.find("table", id="rankings_table")
    rows: "list[dict]" = []
    if table is not None:
        away_team, home_team = None, None
        stat = None
        for r in _rows(table):
            ne = [c for c in r if c]
            if not ne:
                continue
            if len(ne) >= 3 and ne[0] in ("Period Stats", "Team Stats"):
                away_team, home_team = ne[1], ne[2]
                continue
            if len(r) >= 3:
                label = r[0]
                if _PERIOD_RE.match(label or ""):
                    period = label
                else:
                    stat = label
                    period = "total"
                rows.append(
                    {
                        "contest_id": cid,
                        "stat": stat,
                        "period": period,
                        "away_team": away_team,
                        "away_value": r[1] or None,
                        "home_team": home_team,
                        "home_value": r[2] or None,
                    }
                )
    return _finish(rows, TEAM_STATS_SCHEMA, return_as_pandas)


# --- linescore ------------------------------------------------------------

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


def _linescore_table(soup: "BeautifulSoup") -> "Any":
    for t in soup.find_all("table"):
        trs = t.find_all("tr")
        if not trs:
            continue
        hdr = _cells(trs[0])
        if len(hdr) >= 3 and hdr[0] == "" and hdr[1] == "1" and hdr[-1] == "S":
            return t
    return None


def parse_ncaa_bb_linescore(
    html: str, contest_id: "str | int | None" = None, *, return_as_pandas: bool = False
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Parse the ``/box_score`` linescore + game info -> one row per team/period.

    ``period`` is the quarter/half label (``"1"``..), ``points`` that period's
    score, ``final`` the team total, and ``game_date`` / ``venue`` / ``attendance``
    the game-info line repeated per row.
    """
    soup = BeautifulSoup(html or "", "html.parser")
    cid = _cid(contest_id)
    table = _linescore_table(soup)
    rows: "list[dict]" = []
    if table is not None:
        all_rows = _rows(table)
        header = all_rows[0]  # ['', '1', '2', ..., 'S']
        periods = header[1:-1]
        game_date, venue, attendance = None, None, None
        team_rows = []
        for r in all_rows[1:]:
            ne = [c for c in r if c]
            if len(r) >= len(header) and r[0] and not r[0].isdigit():
                team_rows.append(r)
            elif len(ne) == 1:
                v = ne[0]
                if re.search(r"\d{1,2}/\d{1,2}/\d{2,4}", v):
                    game_date = v
                elif "Attendance" in v:
                    mnum = re.search(r"([\d,]+)", v)
                    attendance = int(mnum.group(1).replace(",", "")) if mnum else None
                elif "Arena" in v or "Stadium" in v or "Center" in v or "(" in v:
                    venue = v
        for idx, tr in enumerate(team_rows):
            team = tr[0]
            final = _int(tr[-1])
            for pi, period in enumerate(periods):
                rows.append(
                    {
                        "contest_id": cid,
                        "team": team,
                        "home_away": "away" if idx == 0 else "home",
                        "period": period,
                        "points": _int(tr[1 + pi]) if 1 + pi < len(tr) else None,
                        "final": final,
                        "game_date": game_date,
                        "venue": venue,
                        "attendance": attendance,
                    }
                )
    return _finish(rows, LINESCORE_SCHEMA, return_as_pandas)
