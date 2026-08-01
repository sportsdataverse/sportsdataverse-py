"""NCAA team schedule + roster scrapers (bigballR port, stats.ncaa.org).

Faithful polars port of bigballR ``get_team_schedule`` (all_functions.R:1456-
1680) and ``get_team_roster`` (:1709-1845), shared by MBB and WBB through the
``league`` knob (spec_wbigballr_divergence.md section 3: one core, per-league
data tables).

Documented divergences from the R sources:

- **Per-league team-name resolution** (deliberate fix of the wbigballR bug):
  wbigballR resolves both the name->id lookup and the self-team reverse lookup
  against ``bigballR::teamids`` -- the MEN'S crosswalk -- so a women's team id
  finds no name and the schedule's ``Home``/``Away`` self cells come out NA.
  Here both lookups use the *league*'s own crosswalk
  (:mod:`sportsdataverse.mbb.mbb_ncaa_team_ids`).
- **Ppd + Canceled exclusion for both leagues**: the positional game-id fill
  skips ``Ppd`` and ``Canceled`` rows (bigballR :1649); wbigballR only skips
  ``Canceled`` (W:1513) and mis-aligns ids around postponements. The later
  bigballR fix is adopted for both.
- **Lookaround regexes rewritten as capture groups** (game ids:
  ``contests/(\\d+)/`` for R's ``(?<=contests/)\\d+(?=[/])``; rank prefix:
  ``[#\\[0-9]+\\] (.*)`` for the lookbehind form).
- **Row order is raw page order.** The R oracle for MBB was captured through
  chromote, whose rendered DataTable re-sorts roster rows alphabetically; the
  static-HTML path (and this port) keeps the server's row order.
- Scores and attendance are typed columns (``Int64``); ids are ``Utf8``
  (contest ids are opaque). R keeps everything character.
- Improper requests raise :class:`ValueError` instead of R's
  ``message() + NULL``.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Union

import polars as pl

from sportsdataverse.dl_utils import underscore
from sportsdataverse.mbb.mbb_ncaa_game_pbp import _normalize_v2_name
from sportsdataverse.mbb.mbb_ncaa_html import parse_html
from sportsdataverse.mbb.mbb_ncaa_team_ids import _ncaa_bb_team_ids, resolve_ncaa_team_id

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd
    from bs4 import Tag

    from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher

# all_functions.R:1543 -- strips MTE/event suffixes like
# "Campbell 2022-23 MBB App State MTE". ponytail: hardcoded decade, verbatim
# from R; breaks for "203x-xx" event names in 2030 -- widen then.
_EVENT_SUFFIX_RE = re.compile(r" 202.*$")
# :1547 -- strips neutral-site venue ("UC Santa Barbara @Phoenix, AZ ...").
_VENUE_RE = re.compile(r" @[A-Z].*$")
# :1551 -- R lookaround "(?<=contests/)\d+(?=[/])" -> capture group.
_CONTEST_ID_RE = re.compile(r"contests/(\d+)/")
# :1558 -- R lookbehind "(?<=[\#[0-9]+] ).*" (rank-prefix strip) -> capture.
# stringr/ICU treats "[" inside a class as a NESTED set, so the R class is
# the single-char set {#, 0-9, +}: one such char + a space precede the kept
# text ("#9 Indiana" -> "Indiana").
_RANK_RE = re.compile(r"[#0-9+] (.*)$")
_WS_RE = re.compile(r"\s+")

_SCHEDULE_SCHEMA: Dict[str, type] = {
    "game_date": pl.Utf8,
    "home": pl.Utf8,
    "home_score": pl.Int64,
    "away": pl.Utf8,
    "away_score": pl.Int64,
    "box_id": pl.Utf8,
    "game_id": pl.Utf8,
    "is_neutral": pl.Boolean,
    "detail": pl.Utf8,
    "attendance": pl.Int64,
}

#: site header -> snake_case for the roster's first 9 columns (2024-era site).
_ROSTER_HEADER_SNAKE = {
    "GP": "gp",
    "GS": "gs",
    "#": "jersey",
    "Name": "name",
    "Class": "class",
    "Position": "position",
    "Height": "height",
    "Hometown": "hometown",
    "High School": "high_school",
}
_ROSTER_SCHEMA: Dict[str, type] = {
    **{c: pl.Utf8 for c in _ROSTER_HEADER_SNAKE.values()},
    "player": pl.Utf8,
    "clean_name": pl.Utf8,
    "ht_inches": pl.Int64,
    # Python-only additive vs the bigballR oracle: the stats.ncaa.org player id
    # from each row's ``/players/{id}`` link (Utf8 -- NCAA ids stay strings).
    "player_id": pl.Utf8,
}

_PLAYER_HREF_RE = re.compile(r"/players/(\d+)")


def _cell(el: "Tag") -> str:
    """Cell text, whitespace-collapsed + trimmed (rvest/readHTMLTable parity)."""
    return _WS_RE.sub(" ", el.get_text()).strip()


def _find_table(html: str, required: Tuple[str, ...]) -> Optional["Tag"]:
    """First ``<table>`` whose ``<th>`` headers include all of *required*.

    R takes ``readHTMLTable(html)[[1]]`` (first table); header-matching is the
    order-robust equivalent -- on the fixture pages the schedule/roster table
    is also the first table.
    """
    soup = parse_html(html)
    for table in soup.find_all("table"):
        headers = {_cell(th) for th in table.find_all("th")}
        if all(col in headers for col in required):
            return table
    return None


def _r_split_at(s: str) -> List[str]:
    """``strsplit(s, "@")`` with R semantics (trailing empty pieces dropped)."""
    parts = s.split("@")
    while len(parts) > 1 and parts[-1] == "":
        parts.pop()
    return parts


def _clean_pieces(pieces: List[str], team_set: "set[str]") -> List[str]:
    """Rank-prefix strip + incremental-prefix team matching (R :1555-1571).

    Per piece, take the text after a ``"...] "`` rank prefix (fallback: the
    raw piece). If NO trimmed piece is a known team name, grow each piece's
    prefix one character at a time until it hits a known team name (handles
    names glued to venue text); an exhausted search keeps the whole piece.
    """
    t: List[str] = []
    for x in pieces:
        m = _RANK_RE.search(x)
        t.append(m.group(1) if m is not None else x)
    if not any(p.strip() in team_set for p in t):
        out: List[str] = []
        for p in t:
            i = 1
            while p[:i] not in team_set and i <= len(p):
                i += 1
            out.append(p[:i])
        t = out
    return t


def _parse_result(result: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """R :1627-1644 -> ``(selected_score, opponent_score, detail)``.

    ``selected_score`` is the scraped team's score ("W"/"L" stripped),
    ``opponent_score`` the other side, ``detail`` the parenthetical OT note
    (or ``"Canceled"``/``"Ppd"``, in which case the score moves to detail).
    """
    if result is None or result == "":
        # R strsplit("") -> character(0): both scores NA.
        return None, None, None
    parts = result.split("-")
    selected: Optional[str] = parts[0].replace("L", "").replace("W", "").strip()
    opponent: Optional[str] = None
    detail: Optional[str] = None
    if len(parts) > 1:
        opp_raw = parts[1].strip()
        paren = opp_raw.split("(")
        detail = paren[1].replace(")", "") if len(paren) > 1 else None
        opponent = opp_raw.split(" (")[0]
    if selected in ("Canceled", "Ppd"):
        detail = selected
        selected = None
    return selected, opponent, detail


def _dash_na(value: Optional[str]) -> Optional[str]:
    """R :1667 ``team_data[team_data == "-"] <- NA`` (site's blank marker)."""
    return None if value == "-" else value


def parse_ncaa_bb_team_schedule(html: str, team_id: int, *, league: str = "mbb") -> pl.DataFrame:
    """Parse a ``stats.ncaa.org/teams/{team_id}`` page into a schedule frame.

    Pure core of bigballR ``get_team_schedule`` (all_functions.R:1456-1680):
    de-doubles the schedule table's rows, cleans opponent strings (MTE-suffix
    + venue strips, rank-prefix strip, incremental-prefix team matching
    against the *league* crosswalk), derives home/away/neutral, splits the
    Result column into scores + OT detail, and positionally aligns the page's
    ``contests/{id}/`` links onto non-Ppd/non-Canceled rows.

    Args:
        html: Raw team-page HTML.
        team_id: The season-specific stats.ncaa.org team id (used to resolve
            the self team name from the crosswalk).
        league: ``"mbb"`` or ``"wbb"`` -- selects the crosswalk used for the
            self-name lookup and opponent matching. (wbigballR always used
            the men's table -- fixed here; see module docstring.)

    Returns:
        One row per game: ``game_date``, ``home``, ``home_score``, ``away``,
        ``away_score``, ``box_id``, ``game_id``, ``is_neutral``, ``detail``,
        ``attendance``. Zero-row frame with the same schema when the page has
        no schedule table.

    Example:
        Parse an on-disk capture::

            from pathlib import Path
            from sportsdataverse.mbb.mbb_ncaa_schedule import parse_ncaa_bb_team_schedule
            df = parse_ncaa_bb_team_schedule(Path("team_609554.html").read_text(), 609554)
            print(df.shape)
    """
    table = _find_table(html, ("Date", "Opponent", "Result"))
    if table is None:
        return pl.DataFrame(schema=_SCHEDULE_SCHEMA)

    header = [_cell(th) for th in table.find_all("th")]
    idx = {name: header.index(name) for name in ("Date", "Opponent", "Result", "Attendance") if name in header}
    data_rows = [tr for tr in table.find_all("tr") if tr.find("td") is not None]
    # R :1538 -- the table ships doubled rows; keep the odd (1-based) ones.
    data_rows = data_rows[::2]

    def _at(cells: List["Tag"], name: str) -> Optional[str]:
        i = idx.get(name)
        if i is None or i >= len(cells):
            return None
        return _cell(cells[i])

    rows: List[Dict[str, Optional[str]]] = []
    for tr in data_rows:
        cells = tr.find_all("td")
        opponent = _at(cells, "Opponent")
        if opponent is None:  # R :1539 -- drop is.na(Opponent) rows
            continue
        rows.append(
            {
                "date": _at(cells, "Date"),
                "opponent": opponent,
                "result": _at(cells, "Result"),
                "attendance": _at(cells, "Attendance"),
            }
        )

    crosswalk = _ncaa_bb_team_ids(league)
    team_set = set(crosswalk.get_column("team").to_list())
    self_names = crosswalk.filter(pl.col("id") == team_id).get_column("team")
    team_name: Optional[str] = self_names[0] if len(self_names) > 0 else None

    game_ids = _CONTEST_ID_RE.findall(html)

    dates: List[Optional[str]] = []
    homes: List[Optional[str]] = []
    home_scores: List[Optional[str]] = []
    aways: List[Optional[str]] = []
    away_scores: List[Optional[str]] = []
    neutrals: List[bool] = []
    details: List[Optional[str]] = []
    attendances: List[Optional[int]] = []

    for row in rows:
        opp = row["opponent"] or ""
        with_detail = _EVENT_SUFFIX_RE.sub("", opp)
        opp_clean = _VENUE_RE.sub("", with_detail)
        pieces = _clean_pieces(_r_split_at(opp_clean), team_set)
        pieces_detail = _clean_pieces(_r_split_at(with_detail), team_set)

        # R :1594-1622 -- leading "@" => opponent is home; else opponent away.
        opp_is_home = len(pieces) > 1 and pieces[0] == ""
        home_team = pieces[1].strip() if opp_is_home else None
        away_team = None if opp_is_home else pieces[0].strip()
        is_neutral = len(pieces_detail) == 2 and pieces_detail[0] != ""

        selected, opponent_score, detail = _parse_result(row["result"])

        dates.append(_dash_na(row["date"]))
        homes.append(_dash_na(home_team if home_team is not None else team_name))
        home_scores.append(_dash_na(opponent_score if home_team is not None else selected))
        aways.append(_dash_na(away_team if away_team is not None else team_name))
        away_scores.append(_dash_na(opponent_score if away_team is not None else selected))
        neutrals.append(is_neutral)
        details.append(_dash_na(detail))
        att = (row["attendance"] or "").replace(",", "")
        attendances.append(int(att) if att.isdigit() else None)

    # R :1647-1650 -- positional id fill over non-Ppd/non-Canceled rows.
    # ponytail: zip truncates when the page has more links than eligible rows
    # (R would error); never observed on real pages.
    game_id_col: List[Optional[str]] = [None] * len(rows)
    eligible = [i for i, d in enumerate(details) if d is None or d not in ("Ppd", "Canceled")]
    for pos, gid in zip(eligible, game_ids):
        game_id_col[pos] = gid

    return (
        pl.DataFrame(
            {
                "game_date": dates,
                "home": homes,
                "home_score": home_scores,
                "away": aways,
                "away_score": away_scores,
                "box_id": game_id_col,
                "game_id": game_id_col,
                "is_neutral": neutrals,
                "detail": details,
                "attendance": attendances,
            },
            schema={**_SCHEDULE_SCHEMA, "home_score": pl.Utf8, "away_score": pl.Utf8},
        )
        .with_columns(
            pl.col("home_score").cast(pl.Int64, strict=False),
            pl.col("away_score").cast(pl.Int64, strict=False),
        )
        .select(list(_SCHEDULE_SCHEMA))
    )


def _ht_inches(height: Optional[str]) -> Optional[int]:
    """R :1836-1839 -- ``"6-2"`` -> 74; NA on malformed heights.

    ponytail: Int64 -- NCAA heights are whole inches; go Float64 if a
    half-inch ever ships.
    """
    parts = (height or "").split("-")
    if len(parts) < 2:
        return None
    try:
        return 12 * int(parts[0]) + int(parts[1])
    except ValueError:
        return None


def parse_ncaa_bb_team_roster(html: str, team_id: int) -> pl.DataFrame:
    """Parse a ``stats.ncaa.org/teams/{team_id}/roster`` page.

    Pure core of bigballR ``get_team_roster`` (all_functions.R:1709-1845):
    the site's first 9 roster columns (snake_cased) plus the four derived
    columns -- ``player`` (the normalized ``FIRST.LAST`` join key, byte-
    matching the play-by-play name normalization), ``clean_name`` (the raw
    display name), ``ht_inches``, and ``player_id`` (from the row's
    ``/players/{id}`` link).

    Rows are emitted in raw page order; the R oracle's chromote-rendered
    DataTable re-sorts alphabetically (see module docstring).

    Args:
        html: Raw roster-page HTML.
        team_id: The season-specific stats.ncaa.org team id (provenance only;
            the page is self-contained).

    Returns:
        One row per player: ``gp``, ``gs``, ``jersey``, ``name``, ``class``,
        ``position``, ``height``, ``hometown``, ``high_school``, ``player``,
        ``clean_name``, ``ht_inches``, ``player_id`` (the stats.ncaa.org id
        from the row's ``/players/{id}`` link -- Utf8, Python-only additive
        vs the bigballR oracle). Zero-row frame with the same schema when
        the page has no roster table.

    Example:
        Parse an on-disk capture::

            from pathlib import Path
            from sportsdataverse.mbb.mbb_ncaa_schedule import parse_ncaa_bb_team_roster
            df = parse_ncaa_bb_team_roster(Path("roster_609554.html").read_text(), 609554)
            print(df.get_column("player").to_list()[:3])
    """
    table = _find_table(html, ("Name", "Height"))
    if table is None:
        return pl.DataFrame(schema=_ROSTER_SCHEMA)

    header = [_cell(th) for th in table.find_all("th")][:9]  # R [, 1:9]
    snake = [_ROSTER_HEADER_SNAKE.get(h, underscore(h.replace(" ", "_"))) for h in header]
    records: List[List[Optional[str]]] = []
    player_ids: List[Optional[str]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all("td")
        if not cells:
            continue
        values: List[Optional[str]] = [_cell(td) for td in cells[:9]]
        values.extend([None] * (len(header) - len(values)))
        records.append(values)
        pid: Optional[str] = None
        for a in tr.find_all("a", href=True):
            m = _PLAYER_HREF_RE.search(a["href"])
            if m:
                pid = m.group(1)
                break
        player_ids.append(pid)

    data: Dict[str, List[Optional[str]]] = {col: [rec[i] for rec in records] for i, col in enumerate(snake)}
    names = data.get("name", [None] * len(records))
    heights = data.get("height", [None] * len(records))
    data["player"] = [_normalize_v2_name(n) if n is not None else None for n in names]
    data["clean_name"] = list(names)
    data["player_id"] = player_ids
    df = pl.DataFrame(data, schema={c: pl.Utf8 for c in [*snake, "player", "clean_name", "player_id"]})
    return df.with_columns(pl.Series("ht_inches", [_ht_inches(h) for h in heights], dtype=pl.Int64)).select(
        [c for c in _ROSTER_SCHEMA if c in [*snake, "player", "clean_name", "ht_inches", "player_id"]]
    )


def _resolve_id(team_id: Optional[int], team: Optional[str], season: Optional[str], league: str) -> int:
    """bigballR's NA-resolution ladder, with ValueError instead of NULL."""
    if team_id is not None:
        return int(team_id)
    if team is None or season is None:
        raise ValueError("Improper request: pass team_id, or both team= and season=.")
    resolved = resolve_ncaa_team_id(team, season, league=league)
    if resolved is None:
        raise ValueError(f"No {league} team id for team={team!r}, season={season!r}.")
    return resolved


def _fetch_html(fetcher: Optional["NcaaFetcher"], path: str) -> str:
    if fetcher is not None:
        return fetcher.fetch_html(path)
    from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher

    with NcaaFetcher.with_browser() as browser_fetcher:
        return browser_fetcher.fetch_html(path)


def ncaa_mbb_team_schedule(
    team_id: Optional[int] = None,
    *,
    team: Optional[str] = None,
    season: Optional[str] = None,
    fetcher: Optional["NcaaFetcher"] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Scrape a men's team's season schedule from stats.ncaa.org.

    Port of bigballR ``get_team_schedule``. Give either the season-specific
    ``team_id`` or a ``team`` + ``season`` pair (resolved through the bundled
    men's crosswalk).

    Args:
        team_id: stats.ncaa.org team id (changes every season) -- the number
            in the team page URL.
        team: School name, e.g. ``"Illinois"`` (not the mascot form).
        season: Season string, e.g. ``"2025-26"``; required with ``team``.
        fetcher: Injectable :class:`~sportsdataverse.mbb.mbb_ncaa_fetch.
            NcaaFetcher`; defaults to a fresh browser-transport fetcher
            (stats.ncaa.org blocks plain HTTP clients).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per scheduled game -- see
        :func:`parse_ncaa_bb_team_schedule` for the column contract.

    Raises:
        ValueError: Neither ``team_id`` nor a resolvable ``team``/``season``
            pair was given.

    Example:
        Quick start::

            from sportsdataverse.mbb import ncaa_mbb_team_schedule
            df = ncaa_mbb_team_schedule(team="Illinois", season="2025-26")
            print(df.shape)

        Pipeline next step (one line)::

            df.filter(pl.col("is_neutral") == True).head()

    See Also:
        * `hoopR`_ -- men's college basketball in R

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    resolved = _resolve_id(team_id, team, season, "mbb")
    html = _fetch_html(fetcher, f"teams/{resolved}")
    df = parse_ncaa_bb_team_schedule(html, resolved, league="mbb")
    return df.to_pandas() if return_as_pandas else df


def ncaa_mbb_team_roster(
    team_id: Optional[int] = None,
    *,
    team: Optional[str] = None,
    season: Optional[str] = None,
    fetcher: Optional["NcaaFetcher"] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Scrape a men's team roster from stats.ncaa.org.

    Port of bigballR ``get_team_roster``. The ``player`` column is the
    normalized ``FIRST.LAST`` key that byte-matches the play-by-play name
    normalization, so roster<->pbp joins line up.

    Args:
        team_id: stats.ncaa.org team id (changes every season).
        team: School name, e.g. ``"Illinois"``.
        season: Season string, e.g. ``"2025-26"``; required with ``team``.
        fetcher: Injectable :class:`~sportsdataverse.mbb.mbb_ncaa_fetch.
            NcaaFetcher`; defaults to a fresh browser-transport fetcher.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per player -- see :func:`parse_ncaa_bb_team_roster` for the
        column contract.

    Raises:
        ValueError: Neither ``team_id`` nor a resolvable ``team``/``season``
            pair was given.

    Example:
        Quick start::

            from sportsdataverse.mbb import ncaa_mbb_team_roster
            df = ncaa_mbb_team_roster(team="Illinois", season="2025-26")
            print(df.select("jersey", "player", "ht_inches").head())

    See Also:
        * `hoopR`_ -- men's college basketball in R

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    resolved = _resolve_id(team_id, team, season, "mbb")
    html = _fetch_html(fetcher, f"teams/{resolved}/roster")
    df = parse_ncaa_bb_team_roster(html, resolved)
    return df.to_pandas() if return_as_pandas else df


__all__ = [
    "parse_ncaa_bb_team_schedule",
    "parse_ncaa_bb_team_roster",
    "ncaa_mbb_team_schedule",
    "ncaa_mbb_team_roster",
]
