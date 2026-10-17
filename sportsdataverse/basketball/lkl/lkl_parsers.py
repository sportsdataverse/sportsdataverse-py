"""Parsers for the generated ``lkl`` wrappers (LKL, Lietuvos krepsinio lyga, on lkl.lt).

Two kinds of body feed this family. The seven ``/api/livestream/{route}/{gameId}`` routes answer JSON. The
game page ``/rungtynes/{gameId}``, the results list ``/loadResults`` and the standings ``/loadStandings/{id}``
answer server-rendered HTML, which this module reduces to the records the refs captures bank
(``sdv-internal-refs`` ``lkl/tools/capture.py``): the game page's ``<game-info-component>`` props, the paged
results merged into one record, the four standings tables. The HTML readers are stdlib regex readers that refuse
layout drift (:class:`_NotParsed`) instead of guessing.

Each public ``parse_lkl_*`` takes the dict the wrapper returns with ``return_parsed=False`` (the JSON body, or
the page record, with the request stamped under ``_request`` by ``lkl_runtime``) and follows the package parser
contract: a polars frame by default, pandas with ``return_as_pandas=True``, snake_case columns, and a zero-row
frame with the documented columns and dtypes, never an exception, for an empty or unrecognised input.

**Dtypes** are pinned by column name (:data:`SPECS`), never sampled. The site sends percentages as strings
(``"51.47"``, ``"63.6"``, ``"71.4%"``), ``x`` / ``y`` as decimal strings and some counts as strings; the parsers
cast at the boundary. Ids are ``Int64``; shirt numbers and slugs stay strings.

**Id key spaces.** ``game_id``, ``team_id`` and ``player_id`` are lkl.lt's own and join nothing outside it.

* ``game_id``: lkl.lt games, the ``{gameId}`` of every route. Not sequential by season before 2018-19.
* ``team_id``: lkl.lt clubs, stable across seasons and renamings (6 = Rytas). Not the Genius team id.
* ``player_id``: lkl.lt persons, stable across seasons. Only the game page roster and the shot chart carry it;
  the box score, leaders and play-by-play carry the player SLUG, so ``lkl_runtime`` stamps a slug -> id map under
  ``_request`` and the parsers add ``player_id`` through it (null on a bare body, or a slug the roster lacks).
* team slug (results, standings): the club's ``/komandos/{slug}``; the game page maps slug <-> ``team_id``.
* The season ids (results id, standings id) are opaque and never appear in a frame's key columns: the public
  ``season`` is the END year (2026 = 2025-26), which ``lkl_runtime`` maps through the site's season selects.
  From 2016-17 the results season id is also the Genius LiveStats competition id.

**Sides.** ``team_side`` is ``home`` / ``away`` in every frame. ``gameStatistics`` is keyed by team id with no
side marker, so the runtime stamps ``home_team_id`` / ``away_team_id`` from the game page.
"""

from __future__ import annotations

import html as _html
import json
import re
from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd
import polars as pl

__all__ = [
    "parse_lkl_game_info",
    "parse_lkl_game_players",
    "parse_lkl_game_score",
    "parse_lkl_game_stats",
    "parse_lkl_leaders",
    "parse_lkl_lead_chart",
    "parse_lkl_pbp",
    "parse_lkl_period_scores",
    "parse_lkl_player_box",
    "parse_lkl_results",
    "parse_lkl_shots",
    "parse_lkl_standings",
    "parse_lkl_summary",
    "parse_lkl_team_box",
]

Frame = Union[pl.DataFrame, pd.DataFrame]
Spec = Dict[str, pl.DataType]

#: Where the runtime stamps the request (``lkl_runtime.REQUEST_KEY``).
_REQUEST = "_request"

INT, F, S, B, D = pl.Int64, pl.Float64, pl.Utf8, pl.Boolean, pl.Date

#: ``game_status`` of a finished game ("ended").
FINISHED = "Pasibaigė"
PREPARING = "Informacija ruošiama"  # "information is being prepared": the site's empty-table text
PAGE_SIZE = 10  # games per /loadResults page


class _NotParsed(Exception):
    """The page is not the data asked for: a missing selector or component, header drift, a row of the
    wrong width, or another scope (game, season).

    ``empty`` marks a page that is the right one but holds no games; the runtime raises it as ``NoDataError``.
    Every other refusal is an ``AssetFetchError``.
    """

    def __init__(self, msg: str, *, empty: bool = False) -> None:
        super().__init__(msg)
        self.empty = empty


# ------------------------------------------------------------------------------------------ casts
def _same(a: Any, b: Any) -> bool:
    """Type-strict equality (``1 != "1"``, ``True != 1``)."""
    return type(a) is type(b) and a == b


def _i(v: Any) -> Optional[int]:
    """An id / count from an integer or an integer string, else ``None``. Never goes through float."""
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str) and re.fullmatch(r"\s*[+-]?\d+\s*", v):
        return int(v)
    return None


def _f(v: Any) -> Optional[float]:
    """A number from a number or a decimal string (``"46.67"``, ``"71.4%"``, ``"-"`` is ``None``)."""
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str) and re.fullmatch(r"\s*[+-]?\d+(?:\.\d+)?%?\s*", v):
        return float(v.strip().rstrip("%"))
    return None


def _s(v: Any) -> Optional[str]:
    """A stripped non-empty string, else ``None`` (numbers are stringified)."""
    if v is None or isinstance(v, (dict, list, bool)):
        return None
    t = str(v).strip()
    return t or None


def _b(v: Any) -> Optional[bool]:
    return v if isinstance(v, bool) else None


def _mmss(v: Any) -> Optional[int]:
    """``"32:54"`` -> 1974 seconds (a team row sums its players: ``"225:00"`` -> 13500); else ``None``."""
    m = re.fullmatch(r"\s*(\d+):(\d{2})\s*", v) if isinstance(v, str) else None
    return int(m.group(1)) * 60 + int(m.group(2)) if m else None


def _made_att(v: Any) -> Tuple[Optional[int], Optional[int]]:
    """``"4/9"`` -> ``(4, 9)``."""
    m = re.fullmatch(r"\s*(\d+)\s*/\s*(\d+)\s*", v) if isinstance(v, str) else None
    return (int(m.group(1)), int(m.group(2))) if m else (None, None)


def _date(v: Any) -> Optional[date]:
    try:
        return date.fromisoformat(v) if isinstance(v, str) else None
    except ValueError:
        return None


def _get(d: Any, *path: str) -> Any:
    for k in path:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


def _dict(v: Any) -> Dict[str, Any]:
    """``v`` when it is a dict, else an empty one."""
    return v if isinstance(v, dict) else {}


def _frame(rows: List[Dict[str, Any]], spec: Spec, columns: Optional[Sequence[str]], as_pandas: bool) -> Frame:
    """``rows`` as a frame with exactly ``spec``'s columns and dtypes (a zero-row frame when there are none).

    ``columns`` is the wrapper's documented column list; a name the spec lacks is added as a null string column.
    """
    full: Dict[str, pl.DataType] = dict(spec)
    for c in columns or ():
        full.setdefault(c, S)
    df = pl.DataFrame({c: [r.get(c) for r in rows] for c in full}, schema=full)
    return df.to_pandas() if as_pandas else df


def _req(raw: Any) -> Dict[str, Any]:
    r = raw.get(_REQUEST) if isinstance(raw, dict) else None
    return r if isinstance(r, dict) else {}


def _team_ids(req: Dict[str, Any]) -> Dict[str, Optional[int]]:
    return {"home": _i(req.get("home_team_id")), "away": _i(req.get("away_team_id"))}


def _slug_ids(req: Dict[str, Any]) -> Dict[str, int]:
    players = req.get("players")
    if not isinstance(players, dict):
        return {}
    return {k: v for k, v in ((k, _i(v)) for k, v in players.items()) if isinstance(k, str) and v is not None}


# ------------------------------------------------------------------------- HTML: game page
_COMPONENT = re.compile(r"<game-info-component\b(.*?)>", re.S)
_ATTR = re.compile(r'(:?[a-z][a-z-]*)="([^"]*)"')
#: UI config props (column / tab labels), not data.
_DROP_PROPS = frozenset({":players-stats-columns", ":main-tabs", ":leaders-tabs"})

_MONTHS = {
    "sausio": 1,
    "vasario": 2,
    "kovo": 3,
    "balandžio": 4,
    "gegužės": 5,
    "birželio": 6,
    "liepos": 7,
    "rugpjūčio": 8,
    "rugsėjo": 9,
    "spalio": 10,
    "lapkričio": 11,
    "gruodžio": 12,
}
_WEEKDAYS = {
    "pirmadienis": "Monday",
    "antradienis": "Tuesday",
    "trečiadienis": "Wednesday",
    "ketvirtadienis": "Thursday",
    "penktadienis": "Friday",
    "šeštadienis": "Saturday",
    "sekmadienis": "Sunday",
}


def _text(fragment: str) -> str:
    return re.sub(r"\s+", " ", _html.unescape(re.sub(r"<[^>]+>", " ", fragment or ""))).strip()


def _lt_date(text: str) -> Optional[str]:
    """``'2026  m. Spalio 3 d.'`` (Lithuanian, month in the genitive) -> ``'2026-10-03'``."""
    m = re.search(r"(\d{4})\s+m\.\s+(\S+)\s+(\d{1,2})\s+d\.", text or "")
    if not m or m.group(2).lower() not in _MONTHS:
        return None
    try:
        return date(int(m.group(1)), _MONTHS[m.group(2).lower()], int(m.group(3))).isoformat()
    except ValueError:
        return None


def _game_record(text: str, game_id: int) -> Dict[str, Any]:
    """The ``<game-info-component>`` props of the game page as one record (``:``-props are JSON).

    Raises:
        _NotParsed: no component (an unplayed game's preview page, or layout drift), a prop that is not JSON,
            or a ``game-id`` other than ``game_id``.
    """
    m = _COMPONENT.search(text or "")
    if not m:
        raise _NotParsed("no <game-info-component> (an unplayed game's preview page, or layout drift)")
    props: Dict[str, Any] = {}
    for a in _ATTR.finditer(m.group(1)):
        name, raw = a.group(1), _html.unescape(a.group(2))
        if name in _DROP_PROPS:
            continue
        if name.startswith(":"):
            try:
                raw = json.loads(raw)
            except ValueError:
                raise _NotParsed(f"prop {name} is not JSON") from None
        props[name.lstrip(":").replace("-", "_")] = raw
    if not _same(props.get("game_id"), str(game_id)):
        raise _NotParsed(f"out of scope: game-id {props.get('game_id')!r}, asked for {game_id}")
    info = _dict(props.get("game_info"))
    tm = re.search(r",\s*(\d{1,2}:\d{2})\s*$", info.get("time") or "")
    att = re.match(r"\s*(\d+)", info.get("attendance") or "")
    return {
        "game_id": int(props["game_id"]),
        **{k: v for k, v in props.items() if k != "game_id"},
        "start_date": _lt_date(info.get("time") or ""),
        "start_time": tm.group(1) if tm else None,
        "attendance_count": int(att.group(1)) if att else None,
    }


# ----------------------------------------------------------------- HTML: results and standings
def _options(text: str, select_id: str) -> List[Dict[str, Any]]:
    """``<option value=id>label</option>`` of ``select#select_id`` as ``[{season_id, label}]``."""
    m = re.search(rf'<select\b[^>]*\bid="{re.escape(select_id)}".*?</select>', text or "", re.S)
    if not m:
        raise _NotParsed(f"no select#{select_id}")
    return [
        {"season_id": int(v), "label": _text(lbl)}
        for v, lbl in re.findall(r'<option value="([^"]*)"[^>]*>(.*?)</option>', m.group(0), re.S)
        if re.fullmatch(r"\d+", v)
    ]


def _season_window(label: Optional[str]) -> Optional[Tuple[str, str]]:
    """``'2025-2026 m. sezonas'`` -> ``('2025-08-01', '2026-07-31')``: the window every game of the season is in."""
    m = re.fullmatch(r"(\d{4})-(\d{4}) m\. sezonas", (label or "").strip())
    if not m or int(m.group(2)) != int(m.group(1)) + 1:
        return None
    return f"{m.group(1)}-08-01", f"{m.group(2)}-07-31"


def _label_end_year(label: Optional[str]) -> Optional[int]:
    w = _season_window(label)
    return int(w[1][:4]) if w else None


_ITEM_SPLIT = re.compile(r'(<div class="font-semibold text-2xl[^"]*">.*?</div>)|<div class="result-item">', re.S)
_TEAM_LINK = re.compile(r'<a href="https://lkl\.lt/komandos/([^"]+)"[^>]*>(.*?)</a>', re.S)


def _team_block(block: Tuple[str, str]) -> Dict[str, Optional[str]]:
    slug, inner = block
    code = re.search(r"<strong[^>]*>(.*?)</strong>", inner, re.S)
    alt = re.search(r'<img[^>]*alt="([^"]*)"', inner)
    return {
        "slug": slug,
        "code": _text(code.group(1)) if code else None,
        "name": _html.unescape(alt.group(1)) if alt else None,
    }


def _results_page(text: str) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """``(games, last page number)`` of one ``/loadResults`` fragment; the number is ``None`` without a paginator."""
    games: List[Dict[str, Any]] = []
    day = weekday = None
    parts = _ITEM_SPLIT.split(text or "")
    i = 1  # split() yields: pre, header|None, chunk, header|None, chunk ...
    while i < len(parts):
        header, chunk = parts[i], parts[i + 1] if i + 1 < len(parts) else ""
        i += 2
        if header:
            h = _text(header)
            day = _lt_date(h)
            wd = re.search(r"\(([^)]+)\)", h)
            weekday = _WEEKDAYS.get(wd.group(1).lower()) if wd else None
            if day is None:
                raise _NotParsed(f"date header drift: {h!r}")
            continue
        if day is None:
            raise _NotParsed("result item before any date header")
        teams = _TEAM_LINK.findall(chunk)
        score = re.search(r'<a href="https://lkl\.lt/rungtynes/(\d+)">(.*?)</a>', chunk, re.S)
        loc = re.search(r'<div class="location[^"]*">\s*<div[^>]*>(.*?)</div>\s*<div>(.*?)</div>', chunk, re.S)
        if len(teams) != 2 or not score or not loc:
            raise _NotParsed("result item drift (teams / score link / location)")
        pts = re.fullmatch(r"(\d+)\s*-\s*(\d+)", _text(score.group(2)))
        if not pts:
            raise _NotParsed(f"score drift: {_text(score.group(2))!r}")
        left, _dash, right = score.group(2).partition("-")
        winner = "home" if "<strong" in left else "away" if "<strong" in right else None
        venue = [v.strip() for v in _text(loc.group(2)).split(",")]
        home, away = _team_block(teams[0]), _team_block(teams[1])
        games.append(
            {
                "game_id": int(score.group(1)),
                "date": day,
                "weekday": weekday,
                "time": _text(loc.group(1)) or None,
                "arena": venue[0] or None,
                "city": venue[1] if len(venue) > 1 and venue[1] else None,
                **{f"home_{k}": v for k, v in home.items()},
                "home_score": int(pts.group(1)),
                **{f"away_{k}": v for k, v in away.items()},
                "away_score": int(pts.group(2)),
                "winner_highlighted": winner,
            }
        )
    pages = [int(p) for p in re.findall(r"loadResults\?page=(\d+)", text or "")]
    return games, max(pages) if pages else None


def _results_record(season_id: int, index_html: str, pages: List[str], after: Optional[str] = None) -> Dict[str, Any]:
    """Merge every page of one season's results; refuse a short, duplicated, unconfirmed or out-of-season list.

    ``index_html`` is ``/rezultatai`` (the season select that names the season), ``pages`` are the
    ``/loadResults`` fragments 1..last, ``after`` is page last + 1, which the walk must have fetched whenever
    the last page is full: it has to hold no games, or the paginator understated the list.

    Raises:
        _NotParsed: ``empty`` for a season with no played games; otherwise a refusal.
    """
    lkl = _options(index_html, "season-lkl")
    label = next((o["label"] for o in lkl if o["season_id"] == season_id), None)
    window = _season_window(label)
    if window is None:
        raise _NotParsed(f"season {season_id} not in the /rezultatai season select (or label drift: {label!r})")
    games: List[Dict[str, Any]] = []
    last: Optional[int] = None
    for n, page in enumerate(pages, start=1):
        rows, page_last = _results_page(page)
        if n == 1:
            if not rows and PREPARING in _text(page):
                raise _NotParsed(f"season {season_id} ({label}) has no played games yet", empty=True)
            if page_last is None and len(rows) >= PAGE_SIZE:
                raise _NotParsed(f"page 1 holds {len(rows)} games but has no paginator (pagination drift)")
            last = page_last or 1
        if (page_last or 1) != last:
            raise _NotParsed(f"page {n} says {page_last} pages, page 1 said {last}")
        if not rows or (n < (last or 1) and len(rows) != PAGE_SIZE):
            raise _NotParsed(f"page {n} has {len(rows)} games (expected {PAGE_SIZE} on all but the last page)")
        games += rows
    if last is None or len(pages) != last:
        raise _NotParsed(f"walked {len(pages)} pages, the list has {last}")
    if len(_results_page(pages[-1])[0]) == PAGE_SIZE:
        if after is None:
            raise _NotParsed(f"the last page ({last}) is full and page {last + 1} was not checked")
        extra = _results_page(after)[0]
        if extra:
            raise _NotParsed(f"page {last + 1}, after the paginator's last page, holds {len(extra)} games")
    ids = [g["game_id"] for g in games]
    if len(set(ids)) != len(ids):
        raise _NotParsed("duplicate games across pages (the list moved while walking it)")
    out = [g for g in games if not window[0] <= g["date"] <= window[1]]
    if out:
        raise _NotParsed(f"out of scope: game {out[0]['game_id']} on {out[0]['date']} outside {label} {window}")
    return {
        "season_id": season_id,
        "season_label": label,
        "season_end_year": int(window[1][:4]),
        "pages": last,
        "game_count": len(games),
        "games": games,
    }


_STANDINGS_HEADERS = {
    "full": ["Poz.", "Komanda", "Rng.", "Per.", "Pr.", "Laim.%", "Serija", "Tšk. vid.", "Paskutinės 5"],
    "home": ["Poz.", "Komanda", "Rng.", "Per.", "Pr.", "Tšk. vid.", "Paskutinės 5"],
    "away": ["Poz.", "Komanda", "Rng.", "Per.", "Pr.", "Tšk. vid.", "Paskutinės 5"],
    "final": ["Poz.", "Komanda"],
}
_LAST5 = {"green": "W", "red": "L"}


def _last5(cell: str) -> List[str]:
    """The 'Paskutinės 5' squares in rendered order: bg-green = W(in), bg-red = L(oss); anything else refuses."""
    colors = re.findall(r"\bblock\s+bg-(\w+)", cell)
    if any(c not in _LAST5 for c in colors):
        raise _NotParsed(f"last-5 colour drift: {colors}")
    return [_LAST5[c] for c in colors]


def _standings_record(text: str, season_id: int, index_html: str) -> Dict[str, Any]:
    """The four standings tables of ``/loadStandings/{season_id}`` as one record.

    ``index_html`` is ``/turnyrine-lentele`` (the season select that names the season). The fragment itself
    carries no echo of its season, so the select's label is the only scope check.

    Raises:
        _NotParsed: season not in the select, tab / header / row drift, or every table empty (``empty``).
    """
    labels = _options(index_html, "season")
    label = next((o["label"] for o in labels if o["season_id"] == season_id), None)
    if label is None:
        raise _NotParsed(f"standings season {season_id} not in the /turnyrine-lentele season select")
    text = re.sub(r"<svg\b.*?</svg>", "", text or "", flags=re.S)
    tabs = re.findall(r'<div class="tab-c-ins[^"]*" data-id="([a-z]+)">(.*?)(?=<div class="tab-c-ins|\Z)', text, re.S)
    if [t for t, _ in tabs] != list(_STANDINGS_HEADERS):
        raise _NotParsed(f"standings tabs drift: {[t for t, _ in tabs]}")
    rec: Dict[str, Any] = {
        "season_id": season_id,
        "season_label": label,
        "season_end_year": _label_end_year(label),
    }
    for tab, body in tabs:
        head = [_text(h) for h in re.findall(r"<th\b[^>]*>(.*?)</th>", body, re.S)]
        if head != _STANDINGS_HEADERS[tab]:
            raise _NotParsed(f"{tab} header drift: {head}")
        rows = []
        for tr in re.findall(r"<tr>(.*?)</tr>", body, re.S)[1:]:
            if _text(tr) == PREPARING:  # placeholder row of a table the site has no data for
                continue
            c = re.findall(r"<td\b[^>]*>(.*?)</td>", tr, re.S)
            if len(c) != len(head):
                raise _NotParsed(f"{tab} row width {len(c)} != {len(head)}")
            team = _TEAM_LINK.findall(c[1])
            if not team:
                raise _NotParsed(f"{tab} row without a team link")
            row: Dict[str, Any] = {
                "position": _i(_text(c[0])),
                "team_slug": team[0][0],
                "team_name": _text(team[-1][1]) or None,
            }
            if tab != "final":
                row |= {"played": _i(_text(c[2])), "won": _i(_text(c[3])), "lost": _i(_text(c[4]))}
                rest = c[5:]
                if tab == "full":
                    row |= {"win_pct": _f(_text(rest[0])), "streak": _i(_text(rest[1]))}
                    rest = rest[2:]
                avg = re.findall(r'<span class="num base bordered[^"]*">\s*([^<]*?)\s*</span>', rest[0])
                row |= {
                    "points_for_avg": _f(avg[0]) if len(avg) == 2 else None,
                    "points_against_avg": _f(avg[1]) if len(avg) == 2 else None,
                    "last5": _last5(rest[1]),
                }
            rows.append(row)
        rec[tab] = rows
    if not any(rec[t] for t in _STANDINGS_HEADERS):
        raise _NotParsed(f"standings of {label}: every table is empty", empty=True)
    return rec


def _page_record(kind: str, *args: Any) -> Dict[str, Any]:
    """Build a page record (``game``, ``results`` or ``standings``); an odd page is a refusal, never a crash."""
    build: Any = {"game": _game_record, "results": _results_record, "standings": _standings_record}[kind]
    try:
        return build(*args)
    except _NotParsed:
        raise
    except Exception as exc:  # RecursionError is a RuntimeError
        raise _NotParsed(f"parser error: {type(exc).__name__}") from None


# -------------------------------------------------------------------------------- game page
def _side_rows(rec: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
    info = rec.get("teams_info") if isinstance(rec, dict) else None
    if not isinstance(info, dict):
        return []
    return [(side, info[side]) for side in ("home", "away") if isinstance(info.get(side), dict)]


_INFO_SPEC: Spec = {
    "game_id": INT,
    "game_status": S,
    "start_date": D,
    "start_time": S,
    "arena": S,
    "attendance": S,
    "attendance_count": INT,
    "referees": S,
    "has_actions": B,
    "is_streaming": B,
    **{
        f"{side}_{c}": t
        for side in ("home", "away")
        for c, t in (
            ("team_id", INT),
            ("team", S),
            ("short_name", S),
            ("slug", S),
            ("winner", B),
            ("coach", S),
            ("assistant_coach", S),
        )
    },
}


def parse_lkl_game_info(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """One row per game from the game page (``/rungtynes/{gameId}``): status, tip-off, venue, officials, both teams.

    Args:
        raw: the page record ``lkl_game_info(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row. ``start_date`` / ``start_time`` are the Lithuanian local date and ``HH:MM`` (the page carries no
        offset; the zone is Europe/Vilnius by the site's own wording). ``referees`` is the page's name list as one
        string. ``coach`` / ``assistant_coach`` are the team's CURRENT staff on an old game; team name and slug are
        historical. The page carries no officials' contact or birth data; any such key would be dropped by the
        runtime. Videos, logos and the UI config props are dropped. An empty or unrecognised input gives a zero-row
        frame with the documented columns.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.lkl import lkl_game_info

            info = lkl_game_info(game_id=11446)
    """
    if not isinstance(raw, dict) or not isinstance(raw.get("teams_info"), dict):
        return _frame([], _INFO_SPEC, columns, return_as_pandas)
    row: Dict[str, Any] = {
        "game_id": _i(raw.get("game_id")),
        "game_status": _s(raw.get("game_status")),
        "start_date": _date(raw.get("start_date")),
        "start_time": _s(raw.get("start_time")),
        "arena": _s(_get(raw, "game_info", "arena")),
        "attendance": _s(_get(raw, "game_info", "attendance")),
        "attendance_count": _i(raw.get("attendance_count")),
        "referees": _s(raw.get("referees")),
        "has_actions": _b(raw.get("has_actions")),
        "is_streaming": _b(raw.get("is_streaming")),
    }
    for side, t in _side_rows(raw):
        row |= {
            f"{side}_team_id": _i(t.get("id")),
            f"{side}_team": _s(t.get("name")),
            f"{side}_short_name": _s(t.get("short_name")),
            f"{side}_slug": _s(t.get("slug")),
            f"{side}_winner": _b(t.get("winner")),
            f"{side}_coach": _s(t.get("coach")),
            f"{side}_assistant_coach": _s(t.get("assistant_coach")),
        }
    return _frame([row], _INFO_SPEC, columns, return_as_pandas)


_PLAYERS_SPEC: Spec = {
    "game_id": INT,
    "team_side": S,
    "team_id": INT,
    "player_id": INT,
    "player": S,
    "jersey": S,
    "slug": S,
}


def parse_lkl_game_players(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The game roster from the game page: one row per player on either team sheet, with the lkl.lt player id.

    Args:
        raw: the page record ``lkl_game_players(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per rostered player. ``player_id`` (lkl.lt persons, stable across seasons) and ``slug`` are the
        two keys: this frame is the slug -> id map for the box score, leaders and play-by-play, which carry the
        slug only. ``jersey`` is a string. An empty or unrecognised input gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.lkl import lkl_game_players

            roster = lkl_game_players(game_id=11446)
    """
    if not isinstance(raw, dict) or not isinstance(raw.get("players"), dict):
        return _frame([], _PLAYERS_SPEC, columns, return_as_pandas)
    gid = _i(raw.get("game_id"))
    ids = {side: _i(_get(t, "id")) for side, t in _side_rows(raw)}
    rows = []
    for side in ("home", "away"):
        for p in raw["players"].get(side) or []:
            if isinstance(p, dict):
                rows.append(
                    {
                        "game_id": gid,
                        "team_side": side,
                        "team_id": ids.get(side),
                        "player_id": _i(p.get("id")),
                        "player": _s(p.get("name")),
                        "jersey": _s(p.get("jersey")),
                        "slug": _s(p.get("slug")),
                    }
                )
    return _frame(rows, _PLAYERS_SPEC, columns, return_as_pandas)


# ------------------------------------------------------------------------------- box score
_SIDES = ("home", "away")
_PLAYER_SPEC: Spec = {
    "game_id": INT,
    "team_side": S,
    "team_id": INT,
    "player_slug": S,
    "player_id": INT,
    "player": S,
    "jersey": S,
    "is_starter": B,
    "is_captain": B,
    "is_active": B,
    "seconds_played": INT,
    "points": INT,
    "fg_made": INT,
    "fg_attempted": INT,
    "fg_pct": F,
    "fg2_made": INT,
    "fg2_attempted": INT,
    "fg2_pct": F,
    "fg3_made": INT,
    "fg3_attempted": INT,
    "fg3_pct": F,
    "ft_made": INT,
    "ft_attempted": INT,
    "ft_pct": F,
    "total_rebounds": INT,
    "offensive_rebounds": INT,
    "defensive_rebounds": INT,
    "assists": INT,
    "steals": INT,
    "turnovers": INT,
    "blocks": INT,
    "blocks_received": INT,
    "fouls": INT,
    "fouls_on": INT,
    "plus_minus": INT,
    "efficiency": INT,
}
_CELL_INTS = (
    "points",
    "total_rebounds",
    "offensive_rebounds",
    "defensive_rebounds",
    "assists",
    "steals",
    "turnovers",
    "blocks",
    "blocks_received",
    "fouls",
    "fouls_on",
    "plus_minus",
    "efficiency",
)
_SHOOTING = (("fg", "fg"), ("fg2", "fg2"), ("fg3", "fg3"), ("ft", "ft"))


def parse_lkl_player_box(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Per-player box score of one LKL game (``/api/livestream/boxscore/{gameId}``).

    Args:
        raw: the body ``lkl_player_box(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per player on the team sheet (9-17 a side), starters first as the site lists them. Every box cell
        is unwrapped from its ``{value[, percentage]}`` object; shooting cells ``"made/attempted"`` become
        ``*_made`` / ``*_attempted`` integers and the one-decimal percentage string a 0-100 float. ``player`` is the
        short name ``I. Surname``; ``player_slug`` is the key the site uses, and ``player_id`` is resolved through
        the game roster the runtime stamps (null on a bare body). ``seconds_played`` is the ``mm:ss`` minutes in
        seconds. Team total rows are in ``lkl_team_box``. A body without players gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.lkl import lkl_player_box

            box = lkl_player_box(game_id=11446)
    """
    box = raw.get("boxscore") if isinstance(raw, dict) else None
    if not isinstance(box, dict):
        return _frame([], _PLAYER_SPEC, columns, return_as_pandas)
    req = _req(raw)
    teams, ids = _team_ids(req), _slug_ids(req)
    rows: List[Dict[str, Any]] = []
    for side in _SIDES:
        for p in _get(box, side, "players") or []:
            if not isinstance(p, dict):
                continue
            cell = lambda k: _get(p, k, "value")  # noqa: E731
            row: Dict[str, Any] = {
                "game_id": _i(req.get("game_id")),
                "team_side": side,
                "team_id": teams[side],
                "player_slug": _s(p.get("slug")),
                "player_id": ids.get(p["slug"]) if isinstance(p.get("slug"), str) else None,
                "player": _s(cell("name")),
                "jersey": _s(cell("jersey")),
                "is_starter": _b(p.get("is_starter")),
                "is_captain": _b(p.get("is_captain")),
                "is_active": _b(p.get("is_active")),
                "seconds_played": _mmss(cell("time")),
            }
            for c in _CELL_INTS:
                row[c] = _i(cell(c))
            for key, name in _SHOOTING:
                row[f"{name}_made"], row[f"{name}_attempted"] = _made_att(cell(key))
                row[f"{name}_pct"] = _f(_get(p, key, "percentage"))
            rows.append(row)
    return _frame(rows, _PLAYER_SPEC, columns, return_as_pandas)


_TEAM_INTS = (
    "points",
    "seconds_played",
    "field_goals_made_total",
    "field_goals_attempted_total",
    "field_goals_made_2pt",
    "field_goals_attempted_2pt",
    "field_goals_made_3pt",
    "field_goals_attempted_3pt",
    "free_throws_made",
    "free_throws_attempted",
    "offensive_rebounds",
    "defensive_rebounds",
    "total_rebounds",
    "assists",
    "steals",
    "blocks",
    "blocks_received",
    "turnovers",
    "fouls",
    "fouls_on",
    "efficiency",
    "points_off_turnovers",
    "fast_break_points",
    "second_chance_points",
    "points_in_paint",
    "bench_points",
    "biggest_lead",
    "biggest_scoring_run",
    "lead_changes",
    "tie_results",
    "time_leading",
)
_TEAM_PCTS = (
    "field_goals_percentage",
    "field_goals_2pt_percentage",
    "field_goals_3pt_percentage",
    "free_throws_percentage",
)
_TEAM_SPEC: Spec = {
    "game_id": INT,
    "team_id": INT,
    "team_side": S,
    **{c: INT for c in _TEAM_INTS},
    **{c: F for c in _TEAM_PCTS},
}


def _stats_rows(raw: Any) -> List[Tuple[Optional[str], str, Dict[str, Any]]]:
    """``(side, team id key, row)`` of ``gameStatistics`` (keyed by team id), home first when the sides are known."""
    gs = raw.get("gameStatistics") if isinstance(raw, dict) else None
    if not isinstance(gs, dict):
        return []
    teams = _team_ids(_req(raw))
    out = []
    for key, row in gs.items():
        if isinstance(row, dict):
            tid = _i(row.get("team_id"))
            side = next((s for s in _SIDES if teams[s] is not None and teams[s] == tid), None)
            out.append((side, key, row))
    return sorted(out, key=lambda t: (t[0] != "home", t[0] != "away"))


def parse_lkl_team_box(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Team game totals of one LKL game: one row per team (``gameStatistics`` of the box score or summary).

    Args:
        raw: the body ``lkl_team_box(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        Two rows (home first). The body keys the totals by team id with no side marker, so ``team_side`` comes
        from the game page the runtime stamps (null on a bare body). Percentages are 0-100 floats (the site sends
        two-decimal strings); ``time_leading`` is seconds (the site sends a string). ``points_off_turnovers``,
        ``fast_break_points``, ``second_chance_points`` and ``points_in_paint`` are 0 before 2024-25 (not
        tracked). Per-period points are in ``lkl_period_scores``. A body without ``gameStatistics`` gives a
        zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.lkl import lkl_team_box

            teams = lkl_team_box(game_id=11446)
    """
    rows = []
    for side, _key, r in _stats_rows(raw):
        row: Dict[str, Any] = {"game_id": _i(r.get("game_id")), "team_id": _i(r.get("team_id")), "team_side": side}
        row |= {c: _i(r.get(c)) for c in _TEAM_INTS}
        row |= {c: _f(r.get(c)) for c in _TEAM_PCTS}
        rows.append(row)
    return _frame(rows, _TEAM_SPEC, columns, return_as_pandas)


_PERIOD_SPEC: Spec = {
    "game_id": INT,
    "team_id": INT,
    "team_side": S,
    "period": INT,
    "period_type": S,
    "points": INT,
}


def parse_lkl_period_scores(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Points each team scored in each period of one LKL game (``gameStatistics.*.quarter_scores``).

    Args:
        raw: the body ``lkl_period_scores(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per team and period (home first, periods ascending). ``points`` is the points scored IN the
        period, not cumulative. ``period`` 1-4 is regulation (``period_type`` ``regular``); 5 is the first
        overtime, 6 the second (``overtime``). A body without period scores gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.lkl import lkl_period_scores

            periods = lkl_period_scores(game_id=11446)
    """
    rows = []
    for side, _key, r in _stats_rows(raw):
        qs = r.get("quarter_scores")
        if not isinstance(qs, dict):
            continue
        for k in sorted((k for k in qs if _i(k) is not None), key=lambda k: int(k)):
            rows.append(
                {
                    "game_id": _i(r.get("game_id")),
                    "team_id": _i(r.get("team_id")),
                    "team_side": side,
                    "period": int(k),
                    "period_type": "regular" if int(k) <= 4 else "overtime",
                    "points": _i(qs[k]),
                }
            )
    return _frame(rows, _PERIOD_SPEC, columns, return_as_pandas)


# ---------------------------------------------------------------- summary / game-stats tabs
#: The comparison rows' Lithuanian labels (translated in lkl-returns.md) -> a stable English code.
STAT_CODES = {
    "Metimai iš žaidimo": "field_goals",
    "Dvitaškiai metimai": "two_pointers",
    "Tritaškiai metimai": "three_pointers",
    "Baudų metimai": "free_throws",
    "Atkovoti kamuoliai": "rebounds",
    "Atkovoti kamuoliai gynyboje": "defensive_rebounds",
    "Atkovoti kamuoliai puolime": "offensive_rebounds",
    "Rezultatyvūs perdavimai": "assists",
    "Perimti kamuoliai": "steals",
    "Blokai": "blocks",
    "Klaidos": "turnovers",
    "Pražangos": "fouls",
    "Taškai": "points",
    "Taškai po klaidų": "points_off_turnovers",
    "Taškai greitose atakose": "fast_break_points",
    "Taškai antru bandymu": "second_chance_points",
    "Atsarginių žaidėjų taškai": "bench_points",
}
_COMPARISON_SPEC: Spec = {
    "game_id": INT,
    "group": S,
    "stat": S,
    "label": S,
    **{
        f"{side}_{c}": t
        for side in ("home", "away")
        for c, t in (("value", S), ("percentage", F), ("graph_length", F), ("winning", B))
    },
}


def _comparison_rows(
    rows: Any, gid: Optional[int], group_of: Dict[str, str], group: Optional[str] = None
) -> List[Dict[str, Any]]:
    out = []
    for r in rows if isinstance(rows, list) else []:
        if not isinstance(r, dict):
            continue
        label = _s(r.get("label"))
        row: Dict[str, Any] = {
            "game_id": gid,
            "group": group or group_of.get(label or ""),
            "stat": STAT_CODES.get(label or ""),
            "label": label,
        }
        for side in ("home", "away"):
            c = _dict(r.get(side))
            row |= {
                f"{side}_value": _s(c.get("value")),
                f"{side}_percentage": _f(c.get("percentage")),
                f"{side}_graph_length": _f(c.get("graph_length")),
                f"{side}_winning": _b(c.get("winning")),
            }
        out.append(row)
    return out


def parse_lkl_summary(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The summary tab's team comparison of one LKL game (``/api/livestream/summary/{gameId}``).

    Args:
        raw: the body ``lkl_summary(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per comparison line (up to 14), home and away side by side. ``label`` is the site's Lithuanian
        text and ``stat`` its English code (null for a label this package does not know). Shooting lines have
        a ``"made/attempted"`` ``*_value`` string and a ``*_percentage`` (0-100 float, the site's ``"46.7%"``);
        counting lines have an integer-valued string and no percentage. ``*_graph_length`` is the bar length for
        the chart and ``*_winning`` marks the side that leads the line. ``group`` is null here (the flat list
        has no groups; ``lkl_game_stats`` labels them). The summary also repeats ``gameStatistics`` (use
        ``lkl_team_box``), four leader categories (use ``lkl_leaders``) and a broadcasts HTML string, which is
        not parsed. A body without a comparison gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.lkl import lkl_summary

            comparison = lkl_summary(game_id=11446)
    """
    if not isinstance(raw, dict):
        return _frame([], _COMPARISON_SPEC, columns, return_as_pandas)
    rows = _comparison_rows(raw.get("comparison"), _i(_req(raw).get("game_id")), {})
    return _frame(rows, _COMPARISON_SPEC, columns, return_as_pandas)


def parse_lkl_game_stats(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The statistics tab's team comparison of one LKL game (``/api/livestream/game-stats/{gameId}``).

    Args:
        raw: the body ``lkl_game_stats(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per line of ``comparison.all``, with the same columns as ``lkl_summary``. ``group`` marks the
        lines the tab also shows in its ``shooting`` (2PT, 3PT, free throws) and ``main`` (rebounds, assists,
        steals) bar groups; it is null for the rest. A 2007-08 game has only a few lines and a flat lead chart
        (no event data then). A body without a comparison gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.lkl import lkl_game_stats

            stats = lkl_game_stats(game_id=11446)
    """
    comp = raw.get("comparison") if isinstance(raw, dict) else None
    if not isinstance(comp, dict):
        return _frame([], _COMPARISON_SPEC, columns, return_as_pandas)
    group_of: Dict[str, str] = {}
    for g in ("shooting", "main"):
        for r in comp.get(g) or []:
            if isinstance(r, dict) and isinstance(r.get("label"), str):
                group_of.setdefault(r["label"], g)
    rows = _comparison_rows(comp.get("all"), _i(_req(raw).get("game_id")), group_of)
    return _frame(rows, _COMPARISON_SPEC, columns, return_as_pandas)


_LEAD_SPEC: Spec = {
    "game_id": INT,
    "period": INT,
    "diff": INT,
    "clock": S,
    "score_home": INT,
    "score_away": INT,
    "column_height": F,
}


def parse_lkl_lead_chart(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The lead chart of one LKL game: the score margin at each chart point of each period (``leads``).

    Args:
        raw: the body ``lkl_lead_chart(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per chart point, periods ascending and points in clock order. ``diff`` is home minus away
        (signed); ``score_home`` / ``score_away`` are the cumulative scores (the site sends strings);
        ``clock`` is the clock REMAINING as the site's ``mm:ss:00`` text (a trailing ``:00``; ``10:00:00`` is the
        period start); ``column_height`` is the bar height (``|diff| / maxDiff x 50``). A 2007-08 game has a flat
        chart (no event data then). A body without ``leads`` gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.lkl import lkl_lead_chart

            leads = lkl_lead_chart(game_id=11446)
    """
    leads = raw.get("leads") if isinstance(raw, dict) else None
    if not isinstance(leads, dict):
        return _frame([], _LEAD_SPEC, columns, return_as_pandas)
    gid = _i(_req(raw).get("game_id"))
    rows = []
    for k in sorted((k for k in leads if _i(k) is not None), key=lambda k: int(k)):
        for pt in leads[k] if isinstance(leads[k], list) else []:
            if isinstance(pt, dict):
                rows.append(
                    {
                        "game_id": gid,
                        "period": int(k),
                        "diff": _i(pt.get("diff")),
                        "clock": _s(pt.get("clock")),
                        "score_home": _i(pt.get("score_1")),
                        "score_away": _i(pt.get("score_2")),
                        "column_height": _f(pt.get("column_height")),
                    }
                )
    return _frame(rows, _LEAD_SPEC, columns, return_as_pandas)


_SCORE_SPEC: Spec = {"game_id": INT, "home_score": INT, "away_score": INT, "clock": S}


def parse_lkl_game_score(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The score line of one LKL game (``/api/livestream/game-score/{gameId}``).

    Args:
        raw: the body ``lkl_game_score(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row: the two scores and the clock text. The clock is NOT a state flag: an old game keeps
        ``'1 KELINYS - 10:00'`` after the final, and a finished overtime game reads ``'1 pratesimas - 00:00'``.
        A body without scores gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.lkl import lkl_game_score

            score = lkl_game_score(game_id=11446)
    """
    if not isinstance(raw, dict) or ("home" not in raw and "away" not in raw):
        return _frame([], _SCORE_SPEC, columns, return_as_pandas)
    row = {
        "game_id": _i(_req(raw).get("game_id")),
        "home_score": _i(raw.get("home")),
        "away_score": _i(raw.get("away")),
        "clock": _s(raw.get("clock")),
    }
    return _frame([row], _SCORE_SPEC, columns, return_as_pandas)


# ---------------------------------------------------------------------------------- leaders
_LEADER_SPEC: Spec = {
    "game_id": INT,
    "category": S,
    "category_label": S,
    "team_side": S,
    "team_id": INT,
    "rank": INT,
    "player": S,
    "player_slug": S,
    "player_id": INT,
    "value": INT,
}


def parse_lkl_leaders(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Each team's top players of one LKL game in six categories (``/api/livestream/leaders/{gameId}``).

    Args:
        raw: the body ``lkl_leaders(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per team, category and ranked player. ``category`` is points, total_rebounds, assists,
        efficiency, steals or blocks; ``category_label`` the site's Lithuanian label. ``rank`` 1 is the
        category leader and 2 and up are the runners-up, in the order the site lists them (up to 4 a side).
        ``player_slug`` is the site's key and ``player_id`` is resolved through the game roster the runtime
        stamps (null on a bare body). A body without leaders gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.lkl import lkl_leaders

            leaders = lkl_leaders(game_id=11446)
    """
    cats = raw.get("leaders") if isinstance(raw, dict) else None
    if not isinstance(cats, dict):
        return _frame([], _LEADER_SPEC, columns, return_as_pandas)
    req = _req(raw)
    teams, ids = _team_ids(req), _slug_ids(req)
    rows = []
    for cat, body in cats.items():
        if not isinstance(body, dict):
            continue
        for side in _SIDES:
            lead = body.get(side)
            if not isinstance(lead, dict):
                continue
            ranked = [lead] + [o for o in lead.get("others") or [] if isinstance(o, dict)]
            for n, p in enumerate(ranked, start=1):
                slug = _s(p.get("slug"))
                rows.append(
                    {
                        "game_id": _i(req.get("game_id")),
                        "category": str(cat),
                        "category_label": _s(body.get("label")),
                        "team_side": side,
                        "team_id": teams[side],
                        "rank": n,
                        "player": _s(p.get("name")),
                        "player_slug": slug,
                        "player_id": ids.get(slug) if slug else None,
                        "value": _i(p.get("value")),
                    }
                )
    return _frame(rows, _LEADER_SPEC, columns, return_as_pandas)


# -------------------------------------------------------------------- play-by-play and shots
_PBP_SPEC: Spec = {
    "game_id": INT,
    "event_num": INT,
    "period": INT,
    "period_type": S,
    "clock": S,
    "clock_seconds": INT,
    "team_side": S,
    "team_id": INT,
    "player": S,
    "player_slug": S,
    "player_id": INT,
    "type": S,
    "subtype": S,
    "description": S,
    "success": B,
    "is_points_info": B,
    "score_home": INT,
    "score_away": INT,
}


def _chronological(quarters: Any) -> List[Tuple[int, str, Dict[str, Any]]]:
    """``(period, period_type, line)`` oldest first from the newest-first ``quarters`` body.

    Regulation lines come under their period key (``{"4": [...], "3": [...], ...}``). Overtime lines are ONE
    merged newest-first list with no period field, so the number is derived: the oldest overtime is period 5 and
    each ``period`` / ``end`` line (read oldest first) closes one, so the lines after it, including the next
    overtime's substitutions that precede its ``period`` / ``start`` line, belong to the next period. The
    ``game`` / ``end`` line keeps the period of the line before it. The list order is kept as the site gives it
    (one line of the one-overtime game in the fixtures sits out of clock order).
    """
    if not isinstance(quarters, dict):
        return []
    out: List[Tuple[int, str, Dict[str, Any]]] = []
    reg = quarters.get("regular")
    if isinstance(reg, dict):
        for k in sorted((k for k in reg if _i(k) is not None), key=lambda k: int(k)):
            lines = [ln for ln in reg[k] if isinstance(ln, dict)] if isinstance(reg[k], list) else []
            out += [(int(k), "regular", ln) for ln in reversed(lines)]
    ot = [ln for ln in quarters.get("overtime") or [] if isinstance(ln, dict)]
    done, period = 0, 5  # periods finished so far; the period of the line before (the game-end line keeps it)
    for ln in reversed(ot):  # oldest first
        if ln.get("type") != "game":
            period = 5 + done
        out.append((period, "overtime", ln))
        if ln.get("type") == "period" and ln.get("subtype") == "end":
            done += 1
    return out


def parse_lkl_pbp(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Play-by-play of one LKL game, oldest first (``/api/livestream/play-by-play/{gameId}``).

    Args:
        raw: the body ``lkl_pbp(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per play, chronological (the site lists them newest first, per period). ``event_num`` is a
        1-based sequence the parser assigns. ``period`` of a regulation line is its quarter; the site gives
        overtime lines as one merged list without a period, so the parser DERIVES the overtime number (the oldest
        overtime is 5; each ``period`` / ``end`` line closes one): checked on a one-overtime game only, so a
        multi-overtime game's numbering is unverified. ``clock`` is the time REMAINING (``mm:ss``) and ``clock_seconds`` the same
        in seconds. ``type`` / ``subtype`` are English codes, ``description`` the site's Lithuanian phrase.
        ``score_home`` / ``score_away`` are the running score AFTER the play on every line. Team, period and game
        lines have no player. ``player_id`` is resolved through the game roster the runtime stamps. The site
        charts play-by-play from 2024-25 on. A body without plays gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.lkl import lkl_pbp

            plays = lkl_pbp(game_id=11446)
    """
    req = _req(raw)
    teams, ids = _team_ids(req), _slug_ids(req)
    rows = []
    for n, (period, ptype, ln) in enumerate(_chronological(raw.get("quarters") if isinstance(raw, dict) else None), 1):
        is_home = ln.get("is_home")
        side = "home" if is_home is True else "away" if is_home is False else None
        player = _dict(ln.get("player"))
        slug = _s(player.get("slug"))
        rows.append(
            {
                "game_id": _i(req.get("game_id")),
                "event_num": n,
                "period": period,
                "period_type": ptype,
                "clock": _s(ln.get("clock")),
                "clock_seconds": _mmss(ln.get("clock")),
                "team_side": side,
                "team_id": teams.get(side) if side else None,
                "player": _s(player.get("name")),
                "player_slug": slug,
                "player_id": ids.get(slug) if slug else None,
                "type": _s(ln.get("type")),
                "subtype": _s(ln.get("subtype")),
                "description": _s(ln.get("description")),
                "success": _b(ln.get("success")),
                "is_points_info": _b(ln.get("is_points_info")),
                "score_home": _i(_get(ln, "score", "home")),
                "score_away": _i(_get(ln, "score", "away")),
            }
        )
    return _frame(rows, _PBP_SPEC, columns, return_as_pandas)


_SHOT_SPEC: Spec = {
    "game_id": INT,
    "shot_id": INT,
    "period": INT,
    "period_type": S,
    "team_side": S,
    "team_id": INT,
    "player_id": INT,
    "player_slug": S,
    "type": S,
    "made": B,
    "x": F,
    "y": F,
    "side": S,
    "area": S,
}


def parse_lkl_shots(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Every field-goal attempt of one LKL game with its court position (``/api/livestream/shot-chart/{gameId}``).

    Args:
        raw: the body ``lkl_shots(..., return_parsed=False)`` returns (with ``_request`` stamped).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per attempt (free throws are not charted), periods ascending. The data is Genius LiveStats'. ``x``
        is the percent (0-100) of the court LENGTH from the left baseline of a horizontal full court (baskets
        near 5 and 95) and ``y`` the percent of the WIDTH (baskets at 50); metres are ``x / 100 x 28`` and
        ``y / 100 x 15``, an inference. Which sideline is ``y`` 0 is unverified. ``side`` is the basket half
        (``LEFT`` = x < 50) except backcourt heaves, and teams switch baskets at half-time, so normalise to one
        basket before plotting. ``area`` is a zone code, ``left`` / ``right`` from the shooter's view. The shooter
        is named by ``player_id`` (lkl.lt persons); ``player_slug`` is the reverse lookup through the runtime's
        roster (null on a bare body). The site charts shots from 2024-25 on. A body without shots gives a zero-row
        frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.lkl import lkl_shots

            shots = lkl_shots(game_id=11446)
    """
    q = raw.get("quarters") if isinstance(raw, dict) else None
    if not isinstance(q, dict):
        return _frame([], _SHOT_SPEC, columns, return_as_pandas)
    req = _req(raw)
    teams = _team_ids(req)
    slugs = {v: k for k, v in _slug_ids(req).items()}
    reg = q.get("regular")
    lines: List[Dict[str, Any]] = []
    if isinstance(reg, dict):
        for k in sorted((k for k in reg if _i(k) is not None), key=lambda k: int(k)):
            lines += [ln for ln in reg[k] if isinstance(ln, dict)] if isinstance(reg[k], list) else []
    lines += [ln for ln in q.get("overtime") or [] if isinstance(ln, dict)]
    rows = []
    for ln in lines:
        is_home = ln.get("is_home")
        side = "home" if is_home is True else "away" if is_home is False else None
        pid = _i(ln.get("player_id"))
        rows.append(
            {
                "game_id": _i(req.get("game_id")),
                "shot_id": _i(ln.get("id")),
                "period": _i(ln.get("period")),
                "period_type": (_s(ln.get("period_type")) or "").lower() or None,
                "team_side": side,
                "team_id": teams.get(side) if side else None,
                "player_id": pid,
                "player_slug": slugs.get(pid) if pid is not None else None,
                "type": _s(ln.get("type")),
                "made": _b(ln.get("success")),
                "x": _f(ln.get("x")),
                "y": _f(ln.get("y")),
                "side": _s(ln.get("side")),
                "area": _s(ln.get("area")),
            }
        )
    return _frame(rows, _SHOT_SPEC, columns, return_as_pandas)


# ------------------------------------------------------------------ results and standings
_RESULTS_SPEC: Spec = {
    "season_id": INT,
    "season_end_year": INT,
    "game_id": INT,
    "date": D,
    "weekday": S,
    "time": S,
    "arena": S,
    "city": S,
    **{
        f"{side}_{c}": t
        for side in ("home", "away")
        for c, t in (("slug", S), ("code", S), ("name", S), ("score", INT))
    },
    "winner_highlighted": S,
}


def parse_lkl_results(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Every played game of one LKL season (``/loadResults``, all pages merged).

    Args:
        raw: the record ``lkl_results(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per played game, in the site's order (newest first). ``season_end_year`` is the END year of the
        season (2026 = 2025-26) and ``season_id`` the site's opaque results-season id (from 2016-17 it is the
        Genius LiveStats competition id). Teams are given by slug, code and name only (no team id); map a slug to
        ``team_id`` with ``lkl_game_info``. ``time`` is the Lithuanian local tip-off ``HH:MM``. ``winner_highlighted``
        is the side whose score the page shows in bold. Played games only; the unplayed schedule is not
        wrapped. A record without games gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.lkl import lkl_results

            games = lkl_results(season=2026)
    """
    games = raw.get("games") if isinstance(raw, dict) else None
    if not isinstance(games, list):
        return _frame([], _RESULTS_SPEC, columns, return_as_pandas)
    rows = []
    for g in games:
        if isinstance(g, dict):
            rows.append(
                {
                    "season_id": _i(raw.get("season_id")),
                    "season_end_year": _i(raw.get("season_end_year")),
                    "game_id": _i(g.get("game_id")),
                    "date": _date(g.get("date")),
                    **{c: _s(g.get(c)) for c in ("weekday", "time", "arena", "city")},
                    **{f"{side}_{c}": _s(g.get(f"{side}_{c}")) for side in _SIDES for c in ("slug", "code", "name")},
                    **{f"{side}_score": _i(g.get(f"{side}_score")) for side in _SIDES},
                    "winner_highlighted": _s(g.get("winner_highlighted")),
                }
            )
    return _frame(rows, _RESULTS_SPEC, columns, return_as_pandas)


_STANDINGS_SPEC: Spec = {
    "season_id": INT,
    "season_end_year": INT,
    "table": S,
    "position": INT,
    "team_slug": S,
    "team_name": S,
    "played": INT,
    "won": INT,
    "lost": INT,
    "win_pct": F,
    "streak": INT,
    "points_for_avg": F,
    "points_against_avg": F,
    "last5": S,
}


def parse_lkl_standings(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The four standings tables of one LKL season (``/loadStandings/{seasonId}``) in one long frame.

    Args:
        raw: the record ``lkl_standings(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per team and table. ``table`` is ``full`` (overall: with ``win_pct`` and ``streak``), ``home``
        and ``away`` (the same record split by venue) or ``final`` (the final ranking: position and team only,
        empty until the season is over and for old seasons). ``win_pct`` is 0-100, null when the site shows
        ``-`` (2007-08). ``streak`` is a signed integer as the site prints it; its sign convention is
        unverified. ``points_for_avg`` / ``points_against_avg`` are per-game averages. ``last5`` is the last
        results as ``W`` / ``L`` letters in the order rendered (oldest first, unverified). ``season_id`` is the
        site's standings-season id, a different key space from the results id. Team slugs join
        ``lkl_results``. A record without tables gives a zero-row frame.

    Raises:
        None: bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.lkl import lkl_standings

            table = lkl_standings(season=2026)
    """
    if not isinstance(raw, dict):
        return _frame([], _STANDINGS_SPEC, columns, return_as_pandas)
    rows = []
    for table in ("full", "home", "away", "final"):
        for r in raw.get(table) or []:
            if isinstance(r, dict):
                l5 = r.get("last5")
                rows.append(
                    {
                        "season_id": _i(raw.get("season_id")),
                        "season_end_year": _i(raw.get("season_end_year")),
                        "table": table,
                        "position": _i(r.get("position")),
                        "team_slug": _s(r.get("team_slug")),
                        "team_name": _s(r.get("team_name")),
                        "played": _i(r.get("played")),
                        "won": _i(r.get("won")),
                        "lost": _i(r.get("lost")),
                        "win_pct": _f(r.get("win_pct")),
                        "streak": _i(r.get("streak")),
                        "points_for_avg": _f(r.get("points_for_avg")),
                        "points_against_avg": _f(r.get("points_against_avg")),
                        "last5": "".join(x for x in l5 if isinstance(x, str)) if isinstance(l5, list) else None,
                    }
                )
    return _frame(rows, _STANDINGS_SPEC, columns, return_as_pandas)


#: Each parser's column dtypes, for the generator and the tests.
SPECS: Dict[str, Spec] = {
    "game_info": _INFO_SPEC,
    "game_players": _PLAYERS_SPEC,
    "player_box": _PLAYER_SPEC,
    "team_box": _TEAM_SPEC,
    "period_scores": _PERIOD_SPEC,
    "summary": _COMPARISON_SPEC,
    "game_stats": _COMPARISON_SPEC,
    "lead_chart": _LEAD_SPEC,
    "game_score": _SCORE_SPEC,
    "leaders": _LEADER_SPEC,
    "pbp": _PBP_SPEC,
    "shots": _SHOT_SPEC,
    "results": _RESULTS_SPEC,
    "standings": _STANDINGS_SPEC,
}
