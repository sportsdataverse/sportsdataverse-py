"""Parse stats.ncaa.org college-baseball (NCAA sport code ``MBA``) game-detail
pages other than play-by-play into tidy polars frames.

One parser per stats.ncaa.org tab of a contest (all bm-verify-gated, fetched via
the shared browser transport in :mod:`sportsdataverse.mbb.mbb_ncaa_fetch`):

* :func:`parse_college_baseball_ncaa_linescore` -- ``/box_score`` linescore
  (per-inning runs + R/H/E) and game info (date/venue/attendance).
* :func:`parse_college_baseball_ncaa_team_stats` -- ``/team_stats`` team box,
  **with per-inning breakdown** (one row per category/stat/period).
* :func:`parse_college_baseball_ncaa_player_stats` -- ``/individual_stats``
  (batting / pitching / fielding; a ``dict`` of one frame per category).
* :func:`parse_college_baseball_ncaa_situational_stats` -- ``/situational_stats``
  (per-player situational splits; ``dict`` of ``batting`` / ``pitching`` frames).

**Provenance.** Original sdv-py code, built against a real capture (contest
6357953, Kansas @ A&M-Corpus Christi 2025-02-14). Empty/unparseable input returns
a zero-row frame (or empty dict) with the documented schema. Play-by-play is parsed
by :func:`sportsdataverse.baseball.college_baseball.parse_college_baseball_ncaa_pbp`.
The softball twin re-exports these (identical page layout) -- see
:mod:`sportsdataverse.baseball.college_softball.college_softball_ncaa_box`.
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
    "TEAM_STATS_SCHEMA",
    "parse_college_baseball_ncaa_linescore",
    "parse_college_baseball_ncaa_player_stats",
    "parse_college_baseball_ncaa_situational_stats",
    "parse_college_baseball_ncaa_team_stats",
]

# a period sub-row inside team_stats: "1st Inning" / "Extra Innings" / "App" is a
# stat, so only genuine inning/quarter labels count as period rows.
_PERIOD_RE = re.compile(r"^(?:\d+(?:st|nd|rd|th)\s+(?:Inning|Quarter)|OT\s*\d*|Extra\s+Innings)$", re.I)
_COMPETITOR_ID_RE = re.compile(r"competitor_(\d+)_year_stat_category_(\d+)_data_table")


def _cells(tr: "Any") -> "list[str]":
    return [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]


def _rows(table: "Any") -> "list[list[str]]":
    return [_cells(tr) for tr in table.find_all("tr")]


def _cid(contest_id: "str | int | None") -> "str | None":
    return str(contest_id) if contest_id is not None else None


def _int(v: str) -> "int | None":
    return int(v) if v and v.lstrip("-").isdigit() else None


def _snake(c: str) -> str:
    return re.sub(r"[^\w]+", "_", c).strip("_").lower()


def _finish(rows: "list[dict]", schema: dict, return_as_pandas: bool) -> "Union[pl.DataFrame, pd.DataFrame]":
    df = pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)
    return df.to_pandas() if return_as_pandas else df


# --- linescore ------------------------------------------------------------

LINESCORE_SCHEMA: "dict[str, pl.DataType]" = {
    "contest_id": pl.Utf8,
    "team": pl.Utf8,
    "home_away": pl.Utf8,
    "inning": pl.Utf8,
    "runs": pl.Int64,
    "runs_total": pl.Int64,
    "hits": pl.Int64,
    "errors": pl.Int64,
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
        if len(hdr) >= 4 and hdr[0] == "" and hdr[1] == "1" and ("R" in hdr or "S" in hdr):
            return t
    return None


def parse_college_baseball_ncaa_linescore(
    html: str, contest_id: "str | int | None" = None, *, return_as_pandas: bool = False
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Parse the ``/box_score`` linescore + game info -> one row per team/inning.

    ``inning`` is the inning label (``"1"``..``"9"`` and beyond for extras);
    ``runs`` the runs that inning; ``runs_total`` / ``hits`` / ``errors`` the R/H/E
    totals; and ``game_date`` / ``venue`` / ``attendance`` repeated per row.
    """
    soup = BeautifulSoup(html or "", "html.parser")
    cid = _cid(contest_id)
    table = _linescore_table(soup)
    rows: "list[dict]" = []
    if table is not None:
        all_rows = _rows(table)
        header = all_rows[0]  # ['', '1', ... , 'R', 'H', 'E']
        # trailing R/H/E (or S) totals; the numeric labels between are innings.
        rhe = [i for i, h in enumerate(header) if h in ("R", "H", "E", "S")]
        innings_idx = [i for i, h in enumerate(header) if h.isdigit()]
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
                elif "Stadium" in v or "Field" in v or "(" in v:
                    venue = v
        for idx, tr in enumerate(team_rows):
            team = tr[0]
            r_tot = _int(tr[rhe[0]]) if rhe else None
            h_tot = _int(tr[rhe[1]]) if len(rhe) > 1 else None
            e_tot = _int(tr[rhe[2]]) if len(rhe) > 2 else None
            for ii in innings_idx:
                rows.append(
                    {
                        "contest_id": cid,
                        "team": team,
                        "home_away": "away" if idx == 0 else "home",
                        "inning": header[ii],
                        "runs": _int(tr[ii]) if ii < len(tr) else None,
                        "runs_total": r_tot,
                        "hits": h_tot,
                        "errors": e_tot,
                        "game_date": game_date,
                        "venue": venue,
                        "attendance": attendance,
                    }
                )
    return _finish(rows, LINESCORE_SCHEMA, return_as_pandas)


# --- team stats (by inning) ----------------------------------------------

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


def parse_college_baseball_ncaa_team_stats(
    html: str, contest_id: "str | int | None" = None, *, return_as_pandas: bool = False
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Parse the ``/team_stats`` page -> one row per category/stat/period.

    ``period`` is ``"total"`` for the game total and ``"1st Inning"`` / … for the
    per-inning breakdown that stats.ncaa.org nests under each stat.
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
            if len(ne) == 1:
                category = ne[0]
                stat = None
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
                        "category": category,
                        "stat": stat,
                        "period": period,
                        "away_team": away_team,
                        "away_value": r[1] or None,
                        "home_team": home_team,
                        "home_value": r[2] or None,
                    }
                )
    return _finish(rows, TEAM_STATS_SCHEMA, return_as_pandas)


# --- individual player stats (batting / pitching / fielding) --------------

_PLAYER_CATEGORY_HINTS = [
    ("batting", ("AB", "RBI")),
    ("pitching", ("IP", "ER")),
    ("fielding", ("PO", "TC")),
]


def _player_category(header: "list[str]") -> str:
    hset = set(header)
    for name, hints in _PLAYER_CATEGORY_HINTS:
        if all(h in hset for h in hints):
            return name
    return "other"


def parse_college_baseball_ncaa_player_stats(
    html: str, contest_id: "str | int | None" = None, *, return_as_pandas: bool = False
) -> "dict[str, Union[pl.DataFrame, pd.DataFrame]]":
    """Parse the ``/individual_stats`` page.

    Returns a ``dict`` keyed by ``"batting"`` / ``"pitching"`` / ``"fielding"`` ->
    a frame of that category's player rows across both teams. Each frame carries
    ``contest_id``, ``team_id``, ``number``, ``name``, ``position`` + the category's
    stat columns (snake-cased). Empty input -> ``{}``.
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
        cat = _player_category(header)
        stat_cols = [_snake(c) for c in header[3:]]
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


# --- situational stats (batting / pitching splits) ------------------------


def parse_college_baseball_ncaa_situational_stats(
    html: str, contest_id: "str | int | None" = None, *, return_as_pandas: bool = False
) -> "dict[str, Union[pl.DataFrame, pd.DataFrame]]":
    """Parse the ``/situational_stats`` page.

    Returns a ``dict`` keyed ``"batting"`` / ``"pitching"`` -> a frame of per-player
    situational splits (``vs LHP``/``vs RHP``, RISP, leadoff, bases-loaded, … as
    ``H-AB`` strings). ``team_seq`` (0 = first team's table, 1 = second) distinguishes
    the two teams. Empty input -> ``{}``.
    """
    soup = BeautifulSoup(html or "", "html.parser")
    cid = _cid(contest_id)
    buckets: "dict[str, list[dict]]" = {}
    seq: "dict[str, int]" = {}
    for t in soup.find_all("table"):
        trs = t.find_all("tr")
        if not trs:
            continue
        header = _cells(trs[0])
        if not header or header[0] != "Player":
            continue
        hset = set(header)
        if "vs LHP" in hset or "vs RHP" in hset:
            kind = "batting"
        elif "vs LHB" in hset or "vs RHB" in hset:
            kind = "pitching"
        elif "Field Pct" in hset:
            kind = "fielding"
        else:
            kind = "other"
        team_seq = seq.get(kind, 0)
        seq[kind] = team_seq + 1
        split_cols = [_snake(c) for c in header[2:]]
        for r in _rows(t)[1:]:
            if len(r) < 2 or not any(r):
                continue
            row = {
                "contest_id": cid,
                "team_seq": team_seq,
                "player": r[0] or None,
                "position": r[1] or None,
            }
            for col, val in zip(split_cols, r[2:]):
                row[col] = val or None
            buckets.setdefault(kind, []).append(row)
    out: "dict" = {}
    for kind, rows in buckets.items():
        df = pl.DataFrame(rows)
        out[kind] = df.to_pandas() if return_as_pandas else df
    return out
