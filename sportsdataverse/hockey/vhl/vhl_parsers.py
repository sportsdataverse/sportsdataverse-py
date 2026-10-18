"""Parsers for the generated ``vhl`` wrappers (VHL: ``www.vhlru.ru`` + ``online.vhlru.ru``).

The VHL has no JSON API: every route is a server-rendered ``text/html`` page. This module
holds the whole HTML side, ported from ``sdv-internal-refs`` ``vhl/tools/capture.py``:

* a minimal DOM on the stdlib :mod:`html.parser` (optional end tags closed the way browsers
  close them, void elements, iterative walks, so a deeply nested page never raises
  ``RecursionError``);
* table reading: the header is the LAST ``<thead>`` row and the body the ``<tbody>`` rows;
  cells styled ``display:none`` are dropped from both; header colspans cover several body
  columns; every body row must span exactly the header's width (a row shortened by a rowspan
  or a dropped cell is refused, never shifted); the headers must equal the column spec (drift
  is refused, never guessed);
* one record builder per page (calendar, report, broadcast, standings, player stats, player,
  team) producing the same records the refs captures bank (``tests/fixtures/vhl/captures/``).

Each public ``parse_vhl_*`` takes either the record the runtime returns (a dict, as
``return_parsed=False`` hands it back) or the page's HTML text, and follows the package
parser contract: a polars frame by default, pandas with ``return_as_pandas=True``,
snake_case columns, and a zero-row frame with the documented columns, never an exception,
for an empty, malformed or unrecognised input. Given ``columns`` (the generated wrappers pass
the returns-schema), a missing documented column is added as nulls.

**Dtypes** are pinned by column name, never sampled: ``*_pct``, ``gaa`` and ``*_per_game``
are ``Float64`` (the page prints ``50`` for 50.0 %); counts are ``Int64``; on-ice jerseys and
broadcast player ids are ``List(Int64)``; ``toi`` / ``toi_per_game`` stay ``mm:ss`` strings
(minutes can exceed 59). A dash or blank cell is null.

**Id key spaces** (khl/ terminology; none joins another directly). Every id is ``Int64``:

* ``home_club_id`` / ``away_club_id``: the club id, stable across seasons (the img.khl.ru
  logo path). Calendar only.
* ``team_season_id``: one per team per tournament (calendar club filter, standings and
  player career team links). Never equal to a club id.
* ``team_page_id`` / ``home_team_page_id`` / ``away_team_page_id``: the vhlru.ru team page id
  (``/en/teams/{team_page_id}/``), stable across seasons; the report header and the roster.
* ``tn``: the tournament id, one season STAGE (2025/26 regular season = 1374, playoffs =
  1375). The public key is ``season`` (END year) + ``stage`` (``regular`` / ``playoff``).
* ``game_id``: the calendar / report / broadcast game id (``idgame``). The report and the
  broadcast pages do not print it: it comes from the request the runtime stamps under
  ``_request``, so a frame parsed from bare HTML has a null ``game_id``.
* ``player_id`` and ``scorer_id`` / ``assist1_id`` / ``assist2_id``: one player id space,
  stable across seasons (the ``/en/players/{player_id}/`` id and the img.khl.ru photo id).
  Whether it equals the khl.ru id of the same person is unverified.

Bridges: a game's calendar row (club ids) and its report (team page ids) name the same two
teams in the same home/away order; a roster carries the team page's current
``calendar_tn`` + ``calendar_team_season_id``; each career row carries ``tn`` +
``team_season_id``.

**Season.** ``season`` is the END year (2026 = 2025/26), read from the page where it says
(calendar, report, standings, stats, career labels ``25/26``) and from the request stamp
where it does not (broadcast). ``stage`` is ``regular`` or ``playoff``.

**Language.** ``/en/`` pages are English, but team rosters are Cyrillic even there, the
report's shootout period is ``РБ`` and its date line is Russian; the broadcast
(``online.vhlru.ru``, no ``/en/`` mirror) is Russian throughout.
"""

from __future__ import annotations

import re
from html.parser import HTMLParser
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple, Union

import pandas as pd
import polars as pl

__all__ = [
    "parse_vhl_game_info",
    "parse_vhl_goalie_box",
    "parse_vhl_goalie_stats",
    "parse_vhl_goals",
    "parse_vhl_pbp",
    "parse_vhl_penalties",
    "parse_vhl_player",
    "parse_vhl_player_box",
    "parse_vhl_player_career",
    "parse_vhl_roster",
    "parse_vhl_schedule",
    "parse_vhl_skater_stats",
    "parse_vhl_standings",
]

Frame = Union[pl.DataFrame, pd.DataFrame]

#: Where the runtime stamps the request (``vhl_runtime.REQUEST_KEY``).
_REQUEST = "_request"


class _NotParsed(Exception):
    """The page is not the data asked for: a missing table or selector, header drift, zero rows,
    a row of the wrong width, or another scope (season, tournament, game, player).

    ``empty`` marks the provider's "nothing here" shells (an unknown game's report, an unknown
    team id's page), which the runtime raises as ``NoDataError``; every other refusal is an
    ``AssetFetchError``.
    """

    def __init__(self, msg: str, *, empty: bool = False) -> None:
        super().__init__(msg)
        self.empty = empty


# --------------------------------------------------------------------------------------- DOM
_VOID = frozenset(
    {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "source", "track", "wbr"}
)
#: An open <key> is implicitly closed by a new start tag in the set (HTML optional end tags).
_CLOSES = {
    "td": {"td", "th", "tr", "tbody", "thead", "tfoot"},
    "th": {"td", "th", "tr", "tbody", "thead", "tfoot"},
    "tr": {"tr", "tbody", "thead", "tfoot"},
    "li": {"li"},
    "option": {"option"},
    "p": {"p", "div", "table", "ul", "ol", "section", "h1", "h2", "h3", "h4", "h5"},
}
_BLOCK = frozenset({"br", "p", "div", "li", "td", "th", "tr"})


class _Node:
    """One element: tag, attributes, children (``_Node`` or text) and parent."""

    __slots__ = ("attrs", "children", "parent", "tag")

    def __init__(self, tag: str, attrs: Sequence[Tuple[str, str]] = (), parent: Optional["_Node"] = None) -> None:
        self.tag = tag
        self.attrs: Dict[str, str] = dict(attrs)
        self.children: List[Union["_Node", str]] = []
        self.parent = parent

    def iter(self, tag: Optional[str] = None) -> Iterator["_Node"]:
        """Descendants in document order, through an explicit stack (no recursion)."""
        stack = [iter(self.children)]
        while stack:
            c = next(stack[-1], None)
            if c is None:
                stack.pop()
            elif isinstance(c, _Node):
                if tag is None or c.tag == tag:
                    yield c
                stack.append(iter(c.children))

    def classes(self) -> List[str]:
        return (self.attrs.get("class") or "").split()

    def find_all(self, tag: Optional[str] = None, cls: Optional[str] = None) -> List["_Node"]:
        return [n for n in self.iter(tag) if cls is None or cls in n.classes()]

    def find(self, tag: Optional[str] = None, cls: Optional[str] = None) -> Optional["_Node"]:
        return next((n for n in self.iter(tag) if cls is None or cls in n.classes()), None)

    def text(self) -> str:
        """Whitespace-collapsed text; block elements are padded with spaces. Iterative."""
        out: List[str] = []
        stack: List[Iterator[Any]] = [iter(self.children)]
        while stack:
            c = next(stack[-1], None)
            if c is None:
                stack.pop()
            elif isinstance(c, str):
                out.append(c)
            elif c.tag not in ("script", "style"):
                if c.tag in _BLOCK:
                    out.append(" ")
                    stack.append(iter([" "]))  # trailing pad, emitted after the children
                stack.append(iter(c.children))
        return re.sub(r"\s+", " ", "".join(out)).strip()

    def links(self) -> List[str]:
        return [a.attrs.get("href") or "" for a in self.iter("a")]


class _Builder(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.root = self.cur = _Node("#root")

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        while self.cur.parent is not None and tag in _CLOSES.get(self.cur.tag, ()):
            self.cur = self.cur.parent
        node = _Node(tag, [(k, v or "") for k, v in attrs], self.cur)
        self.cur.children.append(node)
        if tag not in _VOID:
            self.cur = node

    def handle_startendtag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        self.cur.children.append(_Node(tag, [(k, v or "") for k, v in attrs], self.cur))

    def handle_endtag(self, tag: str) -> None:
        n: Optional[_Node] = self.cur
        while n is not None and n.tag != tag:
            n = n.parent
        if n is not None and n.parent is not None:  # a stray end tag with no open match is ignored
            self.cur = n.parent

    def handle_data(self, data: str) -> None:
        self.cur.children.append(data)


def _parse_html(page: str) -> _Node:
    """The DOM root of an HTML page."""
    b = _Builder()
    b.feed(page or "")
    b.close()
    return b.root


# ------------------------------------------------------------------- cells, specs and tables
def _int(v: Any) -> Optional[int]:
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return None


def _num(s: str) -> Union[int, float, None]:
    """``'12'`` -> 12, ``'94.6'`` -> 94.6, ``'-1'`` -> -1; ``''``, ``'-'`` and anything else -> None."""
    s = (s or "").strip()
    if re.fullmatch(r"[-+]?\d+", s):
        return int(s)
    if re.fullmatch(r"[-+]?\d+\.\d+", s):
        return float(s)
    return None


def _str(s: Optional[str]) -> Optional[str]:
    s = (s or "").strip()
    return s or None


def _same(a: Any, b: Any) -> bool:
    """Type-strict equality: ``True`` never equals 1, ``'1374'`` never equals 1374."""
    return type(a) is type(b) and a == b


def _id(href: str, kind: str) -> Optional[int]:
    """The numeric id of a ``/players/<id>/`` or ``/teams/<id>/`` link (any host), else None."""
    m = re.search(rf"/{kind}/(\d+)/", href or "")
    return int(m.group(1)) if m else None


def _need(cond: Any, msg: str, *, empty: bool = False) -> None:
    if not cond:
        raise _NotParsed(msg, empty=empty)


Cell = Optional[_Node]
Conv = Callable[[Cell, str], Dict[str, Any]]


def _N(cell: Cell, key: str) -> Dict[str, Any]:  # number
    return {key: _num(cell.text()) if cell is not None else None}


def _N0(cell: Cell, key: str) -> Dict[str, Any]:  # broadcast count: a blank cell is zero
    txt = cell.text().strip() if cell is not None else ""
    return {key: 0 if txt == "" else _num(txt)}


def _S(cell: Cell, key: str) -> Dict[str, Any]:  # string
    return {key: _str(cell.text()) if cell is not None else None}


def _T(cell: Cell, key: str) -> Dict[str, Any]:  # mm:ss time; '-' (no time on ice) -> None
    v = _str(cell.text()) if cell is not None else None
    return {key: None if v == "-" else v}


_PERSON = re.compile(r"^(?:(\d+)\s*\.\s*)?(.*?)\s*(?:\((\d+|C|A)\))?$")


def _person(cell: Cell) -> Tuple[Optional[int], Optional[str], Optional[int], Optional[str]]:
    m = _PERSON.match(cell.text() if cell is not None else "")
    ids = [i for i in (_id(h, "players") for h in (cell.links() if cell is not None else [])) if i is not None]
    jersey, name, paren = (m.group(1), m.group(2), m.group(3)) if m else (None, "", None)
    return (ids[0] if ids else None), (name or None), _int(jersey), paren


def _P(cell: Cell, key: str) -> Dict[str, Any]:  # player link: id + name
    pid, name, _j, _p = _person(cell)
    return {f"{key}_id": pid, key: name}


def _PR(cell: Cell, key: str) -> Dict[str, Any]:  # player + captaincy suffix "(C)" / "(A)"
    pid, name, _j, paren = _person(cell)
    return {f"{key}_id": pid, key: name, "captaincy": paren if paren in ("C", "A") else None}


def _PJ(cell: Cell, key: str) -> Dict[str, Any]:  # "71.Polshakov Zakhar(12)": id, name, jersey, season count
    pid, name, jersey, paren = _person(cell)
    return {f"{key}_id": pid, key: name, f"{key}_jersey": jersey, f"{key}_season_total": _int(paren)}


def _PN(cell: Cell, key: str) -> Dict[str, Any]:  # "29. Krasnov Ivan V.": id, name, jersey
    pid, name, jersey, _p = _person(cell)
    return {f"{key}_id": pid, key: name, f"{key}_jersey": jersey}


def _JL(cell: Cell, key: str) -> Dict[str, Any]:  # "30,5,51,71,90,92" -> [30, 5, 51, 71, 90, 92]
    return {key: [int(x) for x in re.findall(r"\d+", cell.text() if cell is not None else "")]}


def _GFA(cell: Cell, key: str) -> Dict[str, Any]:  # "226-131" -> goals_for, goals_against
    m = re.fullmatch(r"(\d+)\s*-\s*(\d+)", cell.text() if cell is not None else "")
    return {"goals_for": int(m.group(1)) if m else None, "goals_against": int(m.group(2)) if m else None}


def _TOIS(cell: Cell, key: str) -> Dict[str, Any]:  # "16:45(22)" -> toi "16:45", shifts 22
    m = re.fullmatch(r"(\d+:\d\d)\s*(?:\((\d+)\))?", cell.text() if cell is not None else "")
    return {"toi": m.group(1) if m else None, "shifts": _int(m.group(2)) if m else None}


def _RUP(cell: Cell, key: str) -> Dict[str, Any]:  # "Жиляков Богдан(з)" -> name + position letter
    txt = cell.text() if cell is not None else ""
    m = re.fullmatch(r"(.*?)\s*\((\w)\)", txt)
    return {key: m.group(1) if m else _str(txt), "position_code": m.group(2) if m else None}


def _STD_TEAM(cell: Cell, key: str) -> Dict[str, Any]:  # standings: name + /stats/players/<tn>/all/<id>/
    href = next(iter(cell.links()), "") if cell is not None else ""
    m = re.search(r"/stats/players/(\d+)/all/(\d+)/", href)
    return {
        key: _str(cell.text() if cell is not None else ""),
        "team_season_id": int(m.group(2)) if m else None,
        "_tn": int(m.group(1)) if m else None,
    }


Spec = List[Tuple[Optional[str], Optional[str], Conv]]


def _cells(tr: _Node) -> List[_Node]:
    return [c for c in tr.children if isinstance(c, _Node) and c.tag in ("td", "th")]


def _hidden(cell: _Node) -> bool:
    return "display:none" in (cell.attrs.get("style") or "").replace(" ", "").lower()


def _span(cell: _Node) -> int:
    return max(_int(cell.attrs.get("colspan")) or 1, 1)


def _width(cells: List[_Node]) -> int:
    """Rendered column count of a row: colspans summed."""
    return sum(_span(c) for c in cells)


def _table_parts(table: _Node) -> Tuple[List[_Node], List[List[_Node]]]:
    """``(header cells, body rows)`` of a table (see the module docstring)."""
    head: List[_Node] = []
    body: List[List[_Node]] = []
    has_body = any(c.tag == "tbody" for c in table.iter())
    for tr in table.iter("tr"):
        sect = tr.parent.tag if tr.parent is not None else ""
        cells = [c for c in _cells(tr) if not _hidden(c)]
        if sect == "thead":
            head = cells
        elif sect == "tbody" or (not has_body and sect != "tfoot"):
            if not head and cells and all(c.tag == "th" for c in cells):
                head = cells
            else:
                body.append(cells)
    return head, body


def _read_table(
    table: Optional[_Node], spec: Spec, what: str, skip: Optional[Callable[[List[_Node]], bool]] = None
) -> List[Dict[str, Any]]:
    """Rows of a table as dicts per ``spec``; refuses a missing table, header drift, a row of the
    wrong width and zero rows."""
    _need(table is not None, f"{what}: table missing")
    assert table is not None
    head, body = _table_parts(table)
    got = [c.text() for c in head]
    want = [h for h, _k, _c in spec]
    _need(
        len(got) == len(want) and all(w is None or g == w for g, w in zip(got, want)),
        f"{what}: headers {got} != {want}",
    )
    offsets, w = [], 0  # column offset of each header cell (a header colspan covers several body columns)
    for c in head:
        offsets.append(w)
        w += _span(c)
    rows = []
    for n, cells in enumerate(body):
        if not cells or (skip and skip(cells)):
            continue
        _need(_width(cells) == w, f"{what}: row {n} spans {_width(cells)} columns, header {w}")
        grid: List[_Node] = []
        for c in cells:
            grid += [c] * _span(c)
        row: Dict[str, Any] = {}
        for i, (_h, key, conv) in enumerate(spec):
            if key:
                row.update(conv(grid[offsets[i]], key))
        rows.append(row)
    _need(rows, f"{what}: zero rows")
    return rows


STAT = {  # English header -> key, shared by every stats table (the pages' own legends)
    "GP": "games_played",
    "G": "goals",
    "A": "assists",
    "PTS": "points",
    "+/-": "plus_minus",
    "+": "plus",
    "-": "minus",
    "PIM": "penalty_minutes",
    "ESG": "even_strength_goals",
    "PPG": "power_play_goals",
    "SHG": "shorthanded_goals",
    "OTG": "overtime_goals",
    "GWG": "game_winning_goals",
    "SDS": "shootout_deciding_goals",
    "SOG": "shots_on_goal",
    "%SOG": "shot_pct",
    "S/G": "shots_per_game",
    "FO": "faceoffs",
    "FOW": "faceoffs_won",
    "%FO": "faceoff_pct",
    "TOI/G": "toi_per_game",
    "Sft/G": "shifts_per_game",
    "Hits": "hits",
    "BLS": "blocked_shots",
    "W": "wins",
    "L": "losses",
    "SOP": "shootouts_played",
    "GA": "goals_against",
    "Sv": "saves",
    "%Sv": "save_pct",
    "GAA": "gaa",
    "SO": "shutouts",
    "TOI": "toi",
}
GOALIE_STAT = {"SOG": "shots_against"}
STR_STAT = frozenset({"TOI/G", "TOI"})  # mm:ss (minutes may exceed 59: '2314:48')
SKATER_COLS = ("GP", "G", "A", "PTS", "+/-", "PIM", "ESG", "PPG", "SHG", "OTG", "GWG", "SDS", "SOG", "%SOG")
SKATER_TAIL = ("FO", "FOW", "%FO", "TOI/G", "Sft/G", "Hits", "BLS")
GOALIE_COLS = ("GP", "W", "L", "SOP", "SOG", "GA", "Sv", "%Sv", "GAA")


def _stat(*headers: str, goalie: bool = False) -> Spec:
    m = {**STAT, **(GOALIE_STAT if goalie else {})}
    return [(h, m[h], _T if h in STR_STAT else _N) for h in headers]


REPORT_GOALS: Spec = [
    ("G", "goal_number", _N),
    ("Per", "period", _S),
    ("Time", "time", _S),
    ("Score", "score", _S),
    ("PMG", "strength", _S),
    ("Goal Scorer", "scorer", _PJ),
    ("Assist", "assist1", _PJ),
    ("Assist", "assist2", _PJ),
    (None, "home_on_ice", _JL),
    (None, "away_on_ice", _JL),  # header = the two short team names (home first)
]
REPORT_PENALTIES: Spec = [
    ("Time", "time", _S),
    ("Player", "player", _PN),
    ("", "minutes", _N),
    ("Penalty", "infraction", _S),
]
REPORT_GOALIES: Spec = [
    ("", "jersey", _N),
    ("Player", "player", _PR),
    *_stat(*GOALIE_COLS, "A", "SO", "PIM", "TOI", goalie=True),
]
REPORT_SKATERS: Spec = [("", "jersey", _N), ("Player", "player", _PR), *_stat(*SKATER_COLS, *SKATER_TAIL)]
REPORT_POWERPLAY: Spec = [
    ("Team", "team", _S),
    ("GP", "games_played", _N),
    ("PP", "power_plays", _N),
    ("GF", "power_play_goals", _N),
    ("%PP", "power_play_pct", _N),
    ("SHGA", "shorthanded_goals_against", _N),
    ("TS", "times_shorthanded", _N),
    ("PPGA", "power_play_goals_against", _N),
    ("%PK", "penalty_kill_pct", _N),
    ("SHG", "shorthanded_goals", _N),
]
STRENGTHS = ("5 x 5", "5 x 4", "5 x 3", "4 x 4", "4 x 3", "3 x 3", "3 x 4", "3 x 5", "4 x 5")
REPORT_BY_STRENGTH: Spec = [
    ("Team", "team", _S),
    ("GP", "games_played", _N),
    *[(s, "g_" + s.replace(" x ", "v"), _N) for s in STRENGTHS],
    ("EN", "empty_net", _N),
    ("PS", "penalty_shot", _N),
    ("", "total", _N),
]
REPORT_SHOTS: Spec = [
    ("Period", "period", _S),
    ("G", "home_goals", _N),
    ("SOG", "home_shots_on_goal", _N),
    ("%SOG", "home_shot_pct", _N),
    ("", None, _S),
    ("G", "away_goals", _N),
    ("SOG", "away_shots_on_goal", _N),
    ("%SOG", "away_shot_pct", _N),
]
STATS_GOALIES: Spec = [
    ("Player", "player", _P),
    ("#", "jersey", _N),
    ("Team", "team", _S),
    *_stat(*GOALIE_COLS, "G", "A", "SO", "PIM", "TOI", goalie=True),
]
STATS_SKATERS: Spec = [
    ("Player", "player", _P),
    ("#", "jersey", _N),
    ("Team", "team", _S),
    *_stat(
        "GP",
        "G",
        "A",
        "PTS",
        "+/-",
        "+",
        "-",
        "PIM",
        "ESG",
        "PPG",
        "SHG",
        "OTG",
        "GWG",
        "SDS",
        "SOG",
        "%SOG",
        "S/G",
        *SKATER_TAIL,
    ),
]
STANDINGS: Spec = [
    ("", "rank", _N),
    ("Team", "team", _STD_TEAM),
    ("GP", "games_played", _N),
    ("W", "wins", _N),
    ("OTW", "ot_wins", _N),
    ("SOW", "so_wins", _N),
    ("SOL", "so_losses", _N),
    ("OTL", "ot_losses", _N),
    ("L", "losses", _N),
    ("G", "goals", _GFA),
    ("PTS", "points", _N),
]
CAREER_SKATER = _stat(
    "GP", "G", "A", "PTS", "+/-", "PIM", "ESG", "PPG", "SHG", "OTG", "GWG", "SDS", "SOG", "%SOG", "S/G", *SKATER_TAIL
)
CAREER_GOALIE = _stat(*GOALIE_COLS, "G", "A", "SO", "PIM", "TOI", goalie=True)
ROSTER: Spec = [
    ("#", "jersey", _N),
    ("Position", "position", _S),
    ("Name", "player", _P),
    ("Born", "born", _S),
    ("Country", "country", _S),
]
ONLINE_PLAYERS: Spec = [  # online.vhlru.ru has no /en/ mirror: Russian headers
    ("#", "jersey", _N),
    ("Игрок", "player", _RUP),
    ("Ш", "goals", _N0),
    ("А", "assists", _N0),
    ("О", "points", _N0),
    ("БВ", "shots_on_goal", _N0),
    ("Штр", "penalty_minutes", _N0),
    ("Вбр", "faceoffs", _N0),
    ("ВВбр", "faceoffs_won", _N0),
    ("%ВВбр", "faceoff_pct", _N),
    ("БлБ", "blocked_shots", _N0),
    ("СПр", "hits", _N0),
    ("ВП(См)", "toi", _TOIS),
]
ONLINE_TEAMS: Spec = [
    ("", None, _S),
    ("Команда", "team", _S),
    ("Ш", "goals", _N0),
    ("БВ", "shots_on_goal", _N0),
    ("Штр", "penalty_minutes", _N0),
    ("ВВбр", "faceoffs_won", _N0),
    ("%ВВбр", "faceoff_pct", _N),
    ("БлБ", "blocked_shots", _N0),
    ("СПр", "hits", _N0),
]
EVENT_KINDS = {
    "ga": "game_state",
    "co": "comment",
    "go": "goal",
    "pn": "penalty",
    "gc": "goalie_change",
    "vr": "video_review",
    "bu": "shootout_attempt",
}
STRENGTH = {"РАВ": "even", "БОЛ": "power_play", "МЕН": "shorthanded", "БУЛ": "penalty_shot_or_shootout"}
RU_MONTHS = {
    "янв": 1,
    "фев": 2,
    "мар": 3,
    "апр": 4,
    "май": 5,
    "мая": 5,
    "июн": 6,
    "июл": 7,
    "авг": 8,
    "сен": 9,
    "окт": 10,
    "ноя": 11,
    "дек": 12,
}
EN_MONTHS = {
    m: i + 1
    for i, m in enumerate(
        (
            "january",
            "february",
            "march",
            "april",
            "may",
            "june",
            "july",
            "august",
            "september",
            "october",
            "november",
            "december",
        )
    )
}


# ------------------------------------------------------------------------ record builders
def _selected(root: _Node, name: str) -> Tuple[Optional[int], Optional[str]]:
    """``(tn, label)`` of the selected <option> of <select name=name>; the tn is the first number in its value."""
    sel = next((s for s in root.iter("select") if s.attrs.get("name") == name), None)
    opt = next((o for o in (sel.iter("option") if sel else []) if "selected" in o.attrs), None)
    if opt is None:
        return None, None
    m = re.search(r"/(\d+)/", opt.attrs.get("value") or "")
    return (int(m.group(1)) if m else None), _str(opt.text())


def _tournament(root: _Node, name: str, tn: Optional[int] = None) -> Dict[str, Any]:
    """The page's own tournament (the selected option); when ``tn`` is given it must be that one
    (a bad or ignored tn silently serves another season)."""
    got, label = _selected(root, name)
    _need(isinstance(got, int), "no selected tournament")
    if tn is not None:
        _need(_same(got, tn), f"page is tournament {got}, asked {tn}")
    season, _, stage = (label or "").partition(" | ")
    return {"tn": got, "label": label, "season": _str(season), "stage": _str(stage)}


def _tournament_table(page: Union[str, _Node]) -> List[Dict[str, Any]]:
    """Every tournament the calendar's ``<select name="tournament">`` offers, newest first.

    Args:
        page: a calendar page (HTML text or its DOM root).

    Returns:
        ``[{"tn", "label", "season" (END year), "stage" ("regular" / "playoff")}]``; empty when
        the page has no such select. Options whose label does not parse are skipped.
    """
    root = _parse_html(page) if isinstance(page, str) else page
    sel = next((s for s in root.iter("select") if s.attrs.get("name") == "tournament"), None)
    out = []
    for o in sel.iter("option") if sel else []:
        m = re.search(r"/calendar/(\d+)/", o.attrs.get("value") or "")
        label = _str(o.text())
        season, _, stage = (label or "").partition(" | ")
        end, st = _end_year(season), _stage(stage)
        if m and end is not None and st is not None:
            out.append({"tn": int(m.group(1)), "label": label, "season": end, "stage": st})
    return out


def _calendar(root: _Node, want: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    rec: Dict[str, Any] = {"tournament": _tournament(root, "tournament", (want or {}).get("tn"))}
    tn = rec["tournament"]["tn"]
    sel = next((s for s in root.iter("select") if s.attrs.get("name") == "tournament"), None)
    rec["tournaments"] = [
        {"tn": int(m.group(1)), "label": _str(o.text())}
        for o in (sel.iter("option") if sel else [])
        if (m := re.search(r"/calendar/(\d+)/", o.attrs.get("value") or ""))
    ]
    club = next((s for s in root.iter("select") if s.attrs.get("name") == "club"), None)
    rec["team_seasons"] = [
        {"team_season_id": int(m.group(1)), "name": _str(o.text())}
        for o in (club.iter("option") if club else [])
        if (m := re.search(r"/calendar/\d+/0/(\d+)/", o.attrs.get("value") or ""))
    ]
    games = []
    for day in root.find_all(cls="calendar-page__day"):
        d, w = day.find(cls="calendar-page__day-date"), day.find(cls="calendar-page__day-week")
        for card in day.find_all(cls="calendar-page__match"):
            link = card.find(cls="calendar-page__match-detail-link")
            hm = re.search(r"/report/(\d+)/\?idgame=(\d+)", (link.attrs.get("href") if link else "") or "")
            _need(hm and _same(int(hm.group(1)), tn), f"game link {hm and hm.group(0)} outside tn {tn}")
            assert hm is not None
            teams = card.find_all(cls="calendar-page__match-team")
            _need(len(teams) == 2, "game without two teams")
            score = card.find(cls="calendar-page__match-score_total")
            sm = re.fullmatch(r"(\d+):(\d+)", score.text() if score else "")
            kind = card.find(cls="calendar-page__match-score_type")
            status = card.find(cls="calendar-page__match-status")
            num = card.find(cls="calendar-page__match-num")
            city = card.find(cls="calendar-page__match-city")
            online = next((h for h in card.links() if "online.vhlru.ru/online/" in h), None)
            row: Dict[str, Any] = {
                "game_id": int(hm.group(2)),
                "game_number": _int((num.text() if num else "").lstrip("№")),
                "date": _str(d.text() if d else ""),
                "weekday": _str(w.text() if w else ""),
            }
            for side, t in zip(("home", "away"), teams):
                img = t.find("img")
                im = re.search(r"/teams/\w+/\d+/(\d+)/", (img.attrs.get("src") if img else "") or "")
                nm = t.find(cls="calendar-page__match-team-name")
                row[f"{side}_team"] = _str(nm.text() if nm else "")
                row[f"{side}_club_id"] = int(im.group(1)) if im else None  # stable, logo-keyed
            row.update(
                {
                    "city": _str(city.text() if city else ""),
                    "score": _str(score.text() if score else ""),
                    "home_score": int(sm.group(1)) if sm else None,
                    "away_score": int(sm.group(2)) if sm else None,
                    "decision": _str(kind.text() if kind else ""),
                    "winner": "home"
                    if "calendar-page__match--win-home" in card.classes()
                    else "away"
                    if "calendar-page__match--win-visitor" in card.classes()
                    else None,
                    "status": _str(status.text() if status else ""),
                    "online_url": online
                    if online is None
                    else ("https:" + online if online.startswith("//") else online),
                }
            )
            games.append(row)
    _need(games, "zero games")
    rec["games"] = games
    return rec


def _ru_date(txt: str) -> Tuple[Optional[str], Optional[str]]:
    """``'18 Март 2026, Среда, 17:00'`` -> ``('2026-03-18', '17:00')``."""
    m = re.match(r"(\d{1,2})\s+(\w+)\s+(\d{4})(?:.*?(\d{1,2}:\d\d))?", txt or "")
    mon = RU_MONTHS.get(m.group(2).lower()[:3]) if m else None
    if not m or not mon:
        return None, None
    return f"{m.group(3)}-{mon:02d}-{int(m.group(1)):02d}", m.group(4)


def _officials(p: Optional[_Node]) -> List[Dict[str, Any]]:
    out = []
    for part in re.split(r"\s+and\s+", p.text() if p else ""):
        m = re.fullmatch(r"(\d+)\.\s*(.+)", part.strip())
        if m:
            out.append({"number": int(m.group(1)), "name": m.group(2)})
    return out


def _walk(node: _Node) -> List[Tuple[Optional[str], Optional[str], Optional[str], _Node]]:
    """Pre-order walk of the protocol through an explicit stack: track the current h3 / h4 / h5
    headings and emit ``(title, team, desc, table)`` for every table, in document order."""
    ctx: Dict[str, Optional[str]] = {}
    out = []
    stack: List[Iterator[Any]] = [iter(node.children)]
    while stack:
        c = next(stack[-1], None)
        if c is None:
            stack.pop()
            continue
        if not isinstance(c, _Node):
            continue
        cl = c.classes()
        if c.tag == "table":
            out.append((ctx.get("title"), ctx.get("team"), ctx.get("desc"), c))
            continue  # the shootout table carries team <h4>s in its thead: never context
        if "match-centr-protocol__title" in cl:
            ctx.update(title=c.text(), team=None, desc=None)
        elif "table-wrap__team-name" in cl:
            ctx.update(team=c.text(), desc=None)
        elif "table-wrap__table-desc" in cl:
            ctx["desc"] = c.text()
        stack.append(iter(c.children))
    return out


def _shootout(table: _Node) -> List[Dict[str, Any]]:
    """Rows come in pairs per round: A = [score, home shooter, ..., away goalie]; B = [home goalie,
    away shooter]. The score cell (rowspan 2) is the score AFTER the round."""
    head, body = _table_parts(table)
    _need(len(body) >= 2 and len(body) % 2 == 0, f"shootout: {len(body)} rows, expected pairs")
    w = _width(head)
    rounds = []
    for i in range(0, len(body), 2):
        a, b = body[i], body[i + 1]
        # row A spans the full width and opens the rowspan-2 score cell; row B is one column short because of it
        _need(
            _width(a) == w and bool(a) and a[0].attrs.get("rowspan") == "2" and _width(b) == w - 1,
            f"shootout: round {i // 2 + 1} spans {_width(a)}/{_width(b)} columns, header {w}",
        )
        la = [c for c in a if c.links()]
        lb = [c for c in b if c.links()]
        hs, ag = (_PN(la[0], "x"), _PN(la[-1], "x")) if len(la) == 2 else ({}, {})
        hg, as_ = (_PN(lb[0], "x"), _PN(lb[-1], "x")) if len(lb) == 2 else ({}, {})
        rounds.append(
            {
                "round": i // 2 + 1,
                "score_after": _str(a[0].text()),
                "home_shooter_id": hs.get("x_id"),
                "home_shooter": hs.get("x"),
                "home_shooter_jersey": hs.get("x_jersey"),
                "away_goalie_id": ag.get("x_id"),
                "away_goalie": ag.get("x"),
                "away_shooter_id": as_.get("x_id"),
                "away_shooter": as_.get("x"),
                "away_shooter_jersey": as_.get("x_jersey"),
                "home_goalie_id": hg.get("x_id"),
                "home_goalie": hg.get("x"),
            }
        )
    return rounds


def _report(root: _Node, want: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    card = root.find(cls="match-card")
    _need(card is not None, "no match-card (unknown game, or a tn that does not hold this game)", empty=True)
    assert card is not None

    def t(cls: str, node: _Node = card) -> Optional[str]:
        n = node.find(cls=cls)
        return _str(n.text()) if n else None

    season = re.sub(r"^Season\s+", "", t("match-card__season") or "")
    number = _int((t("match-card__number") or "").lstrip("№"))
    if want is not None:
        _need(_same(season, want["season"]), f"season {season!r} != {want['season']!r}")
        _need(
            _same(t("match-card__league-name"), want["stage"]),
            f"stage {t('match-card__league-name')!r} != {want['stage']!r}",
        )
        _need(_same(number, want["game_number"]), f"game number {number} != {want['game_number']}")
    date_text = t("match-card__date")
    date, local_time = _ru_date(date_text or "")
    sb = card.find(cls="match-card__scoreboard")
    score = t("match-card__score")
    sm = re.match(r"(\d+):(\d+)\s*(OT|SO)?", score or "")
    _need(sm, f"no final score ({score!r})")
    assert sm is not None
    game: Dict[str, Any] = {
        "stage": t("match-card__league-name"),
        "season": season,
        "game_number": number,
        "date_text": date_text,
        "date": date,
        "local_time": local_time,
        "attendance": _int(re.sub(r"\D", "", t("match-card__spectators-count") or "") or None),
        "score": f"{sm.group(1)}:{sm.group(2)}",
        "home_score": int(sm.group(1)),
        "away_score": int(sm.group(2)),
        "decision": sm.group(3),
        "period_scores": [s.text() for s in sb.find_all(cls="match-card__period-score")] if sb else [],
    }
    teams = card.find_all(cls="match-card__team")
    _need(len(teams) == 2, "match-card without two teams")
    for sd, tm in zip(("home", "away"), teams):
        a = tm.find("a")
        coach = tm.find(cls="match-card__coach")
        game[f"{sd}_team_page_id"] = _id((a.attrs.get("href") if a else "") or "", "teams")
        game[f"{sd}_team"] = t("match-card__team-name", tm)
        game[f"{sd}_city"] = t("match-card__team-city", tm)
        game[f"{sd}_coach"] = _str(re.sub(r"^Coach:\s*", "", coach.text())) if coach else None
    refs = card.find_all(cls="match-card__referee")
    game["referees"] = _officials(refs[0] if refs else None)
    game["linesmen"] = _officials(refs[1] if len(refs) > 1 else None)

    prot = root.find(cls="match-centr-protocol")
    _need(prot is not None, "no match-centr-protocol")
    assert prot is not None
    tables = _walk(prot)
    rec: Dict[str, Any] = {"game": game, "goals": [], "shootout": [], "penalties": [], "goalies": [], "skaters": []}
    goals_t = next((tb for ti, _tm, _d, tb in tables if (ti or "").lower() == "scoring summary"), None)
    rec["goals"] = _read_table(goals_t, REPORT_GOALS, "goals")
    assert goals_t is not None
    short = [c.text() for c in _table_parts(goals_t)[0][-2:]]  # short team names, home first
    game["home_short"], game["away_short"] = short

    def side(team: Optional[str]) -> str:  # section headings use the full name, the goals header the short one
        home, away = {game["home_team"], short[0]}, {game["away_team"], short[1]}
        _need(not home & away and team in home | away, f"team heading {team!r} not in {sorted(home)} / {sorted(away)}")
        return "home" if team in home else "away"

    seen_players = []
    for title, team, desc, tb in tables:
        ti = (title or "").lower()
        if ti == "shootouts summary":
            rec["shootout"] = _shootout(tb)
        elif ti == "penalty summary":
            rows = _read_table(tb, REPORT_PENALTIES, "penalties", skip=lambda c: c[0].attrs.get("colspan") == "2")
            rec["penalties"] += [{"team_side": side(team), **r} for r in rows]
        elif ti == "players summary":
            s = side(team)
            seen_players.append((s, desc))
            if desc == "Goaltenders":
                rec["goalies"] += [{"team_side": s, **r} for r in _read_table(tb, REPORT_GOALIES, "goalies")]
            elif desc in ("Defensmen", "Forwards"):  # sic: the site spells it Defensmen
                pos = "defense" if desc == "Defensmen" else "forward"
                rec["skaters"] += [
                    {"team_side": s, "position_group": pos, **r} for r in _read_table(tb, REPORT_SKATERS, desc or "")
                ]
            else:
                raise _NotParsed(f"players summary table {desc!r}")
        elif ti == "teams summary":
            key = {
                "Powerplays": "team_powerplay",
                "GOALS FOR": "team_goals_for",
                "GOALS AGAINST": "team_goals_against",
            }.get(desc or "")
            _need(key, f"teams summary table {desc!r}")
            assert key is not None
            rec[key] = _read_table(tb, REPORT_POWERPLAY if key == "team_powerplay" else REPORT_BY_STRENGTH, desc or "")
        elif ti == "shots":
            rec["shots_by_period"] = _read_table(tb, REPORT_SHOTS, "shots")
    for s in ("home", "away"):
        _need((s, "Goaltenders") in seen_players, f"no {s} goalie table")
        _need(any(x == s and d != "Goaltenders" for x, d in seen_players), f"no {s} skater table")
    for k in ("team_powerplay", "team_goals_for", "team_goals_against", "shots_by_period"):
        _need(k in rec, f"no {k} table")
    _need(bool(rec["shootout"]) == (game["decision"] == "SO"), "shootout table vs decision mismatch")
    return rec


def _online(root: _Node, want: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    title_node = root.find("title")
    title = title_node.text() if title_node else ""
    m = re.search(r"ВХЛ\.\s*(.*?)\.\s*Игра номер (\d+)\s+(.*?):\s*(.*?)\s*\(онлайн", title)
    _need(m, f"not a VHL broadcast page ({title[:60]!r})")
    assert m is not None
    if want is not None:
        _need(_same(int(m.group(2)), want["game_number"]), f"game number {m.group(2)} != {want['game_number']}")
    names = [n.text() for n in root.find_all(cls="game__team-name")][:2]
    cities = [n.text() for n in root.find_all(cls="game__team-city")][:2]
    _need(len(names) == 2, "no two team names")
    game = {
        "title": title,
        "stage_ru": m.group(1),
        "game_number": int(m.group(2)),
        "date_text": m.group(3),
        "home_team": names[0],
        "away_team": names[1],
        "home_city": cities[0] if len(cities) == 2 else None,
        "away_city": cities[1] if len(cities) == 2 else None,
    }
    events = []
    for e in root.find_all(cls="game-event-item"):
        code = next((c.split("--", 1)[1] for c in e.classes() if c.startswith("game-event-item--")), None)
        tm = e.find(cls="game-event-item__time")
        sc = e.find(cls="game-event-item__score-re__num")
        st = e.find(cls="game-event-item__score-status")
        right = e.find(cls="game-event-item__right")
        tt = _str(tm.text()) if tm else None
        events.append(
            {
                "kind": EVENT_KINDS.get(code or ""),
                "kind_code": code,
                "time": tt,
                "clock": None if tt is None else "wall" if code == "ga" else "minute" if tt.endswith("мин") else "game",
                "score": _str(sc.text().replace(" ", "")) if sc else None,
                "strength_code": _str(st.text()) if st else None,
                "strength": STRENGTH.get(st.text()) if st else None,
                "text": _str(right.text()) if right else None,
                "player_ids": [
                    i for i in (_id(h, "players") for h in (right.links() if right else [])) if i is not None
                ],
            }
        )
    _need(events, "zero events")
    rec: Dict[str, Any] = {"game": game, "events": events, "team_stats": [], "player_stats": []}
    for tab, sidek in (("stat-teams", None), ("stat-teamA", "home"), ("stat-teamB", "away")):
        box = next((n for n in root.find_all(cls="game__content_tabs__item") if n.attrs.get("data-tab") == tab), None)
        _need(box is not None, f"no {tab} tab")
        assert box is not None
        periods: List[Any] = []
        for per in box.iter():
            pt = per.attrs.get("data-tab") or ""
            pm = re.fullmatch(r"stats-teams-(match|period-(\d+))", pt)
            tb = per.find("table") if pm else None
            if tb is None or pm is None:
                continue
            period: Any = "match" if pm.group(1) == "match" else int(pm.group(2))
            periods.append(period)
            if sidek is None:
                rec["team_stats"] += [{"period": period, **r} for r in _read_table(tb, ONLINE_TEAMS, f"{tab} {pt}")]
            else:
                rec["player_stats"] += [
                    {"team_side": sidek, "period": period, **r} for r in _read_table(tb, ONLINE_PLAYERS, f"{tab} {pt}")
                ]
        # every tab must hold its whole-game table: a tab with only period tables is not a box score
        _need("match" in periods, f"{tab}: no stats-teams-match table (periods {periods})")
    return rec


def _standings(root: _Node, want: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    rec: Dict[str, Any] = {"tournament": _tournament(root, "team", (want or {}).get("tn"))}
    tn = rec["tournament"]["tn"]
    rows = _read_table(root.find("table"), STANDINGS, "standings")
    for r in rows:  # every team link must point into the same tournament (fail closed on a missing link)
        _need(_same(r.pop("_tn"), tn), f"standings row {r.get('team')!r} links outside tn {tn}")
    rec["rows"] = rows
    return rec


def _stats_players(root: _Node, want: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    rec: Dict[str, Any] = {"tournament": _tournament(root, "tournament", (want or {}).get("tn"))}
    tables = {}
    for wrap in root.find_all(cls="table__wrapper"):
        h2, tb = wrap.find("h2"), wrap.find("table")
        if h2 and tb:
            tables[h2.text()] = tb
    rec["goalies"] = _read_table(tables.get("Goaltenders"), STATS_GOALIES, "Goaltenders")
    rec["defensemen"] = _read_table(tables.get("Defensemen"), STATS_SKATERS, "Defensemen")
    rec["forwards"] = _read_table(tables.get("Forwards"), STATS_SKATERS, "Forwards")
    for k in ("goalies", "defensemen", "forwards"):
        _need(all(isinstance(r.get("player_id"), int) for r in rec[k]), f"{k}: a row without a player link")
    return rec


def _career(table: Optional[_Node], what: str) -> List[Dict[str, Any]]:
    """Season-by-season rows: a label row (``'25/26 | Regular Season'``, no stats) precedes that season's team rows."""
    head = [c.text() for c in _table_parts(table)[0]] if table is not None else []
    spec: Spec = [
        ("Season / Team", "team", _S),
        ("#", "jersey", _N),
        *(CAREER_GOALIE if "SOP" in head else CAREER_SKATER),
    ]
    rows = _read_table(table, spec, what)
    assert table is not None
    out, label = [], None
    for r, cells in zip(rows, [c for c in _table_parts(table)[1] if c]):
        if all(v is None for k, v in r.items() if k != "team"):
            label = r["team"]
            continue
        href = next(iter(cells[0].links()), "")
        m = re.search(r"/stats/players/(\d+)/all/(\d+)/", href)
        season, _, stage = (label or "").partition(" | ")
        out.append(
            {
                "season": _str(season),
                "stage": _str(stage),
                "tn": int(m.group(1)) if m else None,
                "team_season_id": int(m.group(2)) if m else None,
                **r,
            }
        )
    _need(out, f"{what}: zero season rows")
    return out


def _player(root: _Node, want: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    games = next((h for h in root.links() if re.search(r"/players/\d+/games/", h)), "")
    pid = _id(games.replace("/games/", "/"), "players")
    _need(isinstance(pid, int), "no /players/<id>/games/ link: not a player page")
    if want is not None:
        _need(_same(pid, want["player_id"]), f"page is player {pid}, asked {want['player_id']}")
    h1 = root.find("h1")
    num = h1.find(cls="player-card__number") if h1 else None
    name = h1.text().replace(num.text(), "").strip() if (h1 and num) else (h1.text() if h1 else "")
    _need(name, "no player name")
    props: Dict[str, Optional[str]] = {}
    for item in root.find_all(cls="player-card__item"):
        k = item.find(cls="player-card__property")
        if k:
            props[k.text().rstrip(":")] = _str(item.text()[len(k.text()) :])
    img = root.find("img", cls="player-card__img")
    pos = root.find(cls="player-card__subtitle")
    tables = root.find_all("table")
    rec: Dict[str, Any] = {
        "player_id": pid,
        "name": name,
        "jersey": _int((num.text() if num else "").lstrip("#")),
        "position": _str(pos.text()) if pos else None,
        "height_cm": _int(props.get("Height")),
        "weight_kg": _int(props.get("Weight")),
        "born": props.get("Born"),
        "age": _int(re.sub(r"\D", "", props.get("Age") or "") or None),
        "country": props.get("Country"),
        "photo_url": img.attrs.get("src") if img else None,
        "seasons": _career(tables[0] if tables else None, "career by season"),
    }
    totals = tables[1] if len(tables) > 1 else None
    head = [c.text() for c in _table_parts(totals)[0]] if totals is not None else []
    spec: Spec = [("Season / Team", "stage", _S), ("", None, _S), *(CAREER_GOALIE if "SOP" in head else CAREER_SKATER)]
    rec["career_totals"] = _read_table(totals, spec, "career totals")
    return rec


def _team(root: _Node, want: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    h1 = root.find("h1", cls="title")
    name = _str(h1.text()) if h1 else None
    content = root.find(cls="team__content")
    # an unknown team id answers a 158 KB shell without the team block
    _need(name and content is not None, "no h1.title / team__content: not a team page", empty=True)
    assert content is not None
    if want is not None and want.get("team_name") is not None:
        _need(_same(name, want["team_name"]), f"page is team {name!r}, asked {want['team_name']!r}")
    h2 = content.find("h2")
    hm = re.match(r"(.*?)\s*,\s*(.*?)\s*\|\s*Year of foundation:\s*(\d+)", h2.text() if h2 else "")
    officials = []  # every '<p>Role:<b>Name</b></p>' line, whatever the role
    for p in content.iter("p"):
        b = p.find("b")
        if b:
            role = p.text()[: -len(b.text())].strip().rstrip(":").strip()
            officials.append({"role": _str(role), "name": _str(b.text())})
    contacts: Dict[str, Optional[str]] = {}
    for li in content.find_all(cls="team__contacts-item"):
        k, _, v = li.text().partition(":")
        contacts[k.strip()] = _str(v)
    cal = next((h for h in root.links() if re.search(r"/calendar/\d+/0/\d+/", h)), "")
    cm = re.search(r"/calendar/(\d+)/0/(\d+)/", cal)
    asof = root.find(cls="right_team")
    cap = next((p.text() for p in root.iter("p") if p.text().startswith("Capacity:")), None)
    addr = content.find("address")
    rec: Dict[str, Any] = {
        "name": name,
        "full_name": hm.group(1) if hm else None,
        "city": hm.group(2) if hm else None,
        "founded": _int(hm.group(3)) if hm else None,
        "phone": contacts.get("Phone"),
        "site": contacts.get("Site"),
        "officials": officials,
        "address": _str(addr.text()) if addr else None,
        "calendar_tn": int(cm.group(1)) if cm else None,
        "calendar_team_season_id": int(cm.group(2)) if cm else None,
        "roster_as_of": _str(re.sub(r"^As of\s*", "", asof.text())) if asof else None,
        "arena_capacity": _int(re.sub(r"\D", "", cap or "") or None),
        "roster": _read_table(root.find("table"), ROSTER, "roster"),
    }
    rec["honours"] = [
        txt
        for h3 in root.iter("h3")
        if (txt := h3.text()) and txt != "Players" and h3.find_all() == [] and not h3.classes()
    ]
    return rec


_BUILDERS: Dict[str, Callable[[_Node, Optional[Dict[str, Any]]], Dict[str, Any]]] = {
    "calendar": _calendar,
    "report": _report,
    "online": _online,
    "standings": _standings,
    "stats_players": _stats_players,
    "player": _player,
    "team": _team,
}


def _page_record(kind: str, page: str, want: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """The parsed record of one page, or :class:`_NotParsed`.

    Args:
        kind: ``calendar``, ``report``, ``online``, ``standings``, ``stats_players``, ``player``
            or ``team``.
        page: the page's HTML text.
        want: what the page must say about itself (the runtime's identity check): ``tn``
            (calendar, standings, stats), ``season`` / ``stage`` / ``game_number`` (report),
            ``game_number`` (broadcast), ``player_id`` (player). ``None`` skips the identity
            check but keeps every structural one.

    Raises:
        _NotParsed: the page is not that kind of page, or not the one ``want`` names. Any
            parser exception on an odd page (and a ``RecursionError``) becomes ``_NotParsed``.
    """
    try:
        return _BUILDERS[kind](_parse_html(page), want)
    except _NotParsed:
        raise
    except RecursionError:
        raise _NotParsed("page nesting too deep") from None
    except (
        AttributeError,
        TypeError,
        KeyError,
        IndexError,
        ValueError,
    ) as e:  # a page that breaks the parser is not data
        raise _NotParsed(f"unexpected page shape: {type(e).__name__}: {e}") from None


# ------------------------------------------------------------------------------- frames
_ID_COL = re.compile(r"(^|_)id$")
_FLOAT_COL = re.compile(r"(_pct|_per_game)$|^gaa$")
_STR_COL = frozenset({"toi", "toi_per_game"})
_LIST_INT_COL = frozenset({"home_on_ice", "away_on_ice", "player_ids"})
_LIST_STR_COL = frozenset({"period_scores"})


def _spec_cols(spec: Spec) -> List[str]:
    """The record keys a spec produces, in order (its converters run on an empty cell)."""
    return [k for _h, key, conv in spec if key for k in conv(None, key) if not k.startswith("_")]


def _spec_ints(*specs: Spec) -> set:
    return {k for spec in specs for _h, key, conv in spec if key and conv in (_N, _N0) for k in conv(None, key)}


_INT_COL = _spec_ints(
    REPORT_GOALS,
    REPORT_PENALTIES,
    REPORT_GOALIES,
    REPORT_SKATERS,
    STATS_GOALIES,
    STATS_SKATERS,
    STANDINGS,
    CAREER_GOALIE,
    ROSTER,
) | {
    "tn",
    "season",
    "game_number",
    "home_score",
    "away_score",
    "attendance",
    "event_index",
    "goals_for",
    "goals_against",
    "height_cm",
    "weight_kg",
    "age",
    "founded",
    "arena_capacity",
    "calendar_tn",
    "referee1_number",
    "referee2_number",
    "linesman1_number",
    "linesman2_number",
}
_INT_SUFFIX = re.compile(r"_(jersey|season_total)$")


def _dtype_for(col: str) -> Any:
    """The dtype a column has by name alone (see the module docstring)."""
    if col in _LIST_INT_COL:
        return pl.List(pl.Int64)
    if col in _LIST_STR_COL:
        return pl.List(pl.String)
    if col in _STR_COL:
        return pl.String
    if _ID_COL.search(col):
        return pl.Int64
    if _FLOAT_COL.search(col):
        return pl.Float64
    if col in _INT_COL or _INT_SUFFIX.search(col):
        return pl.Int64
    return pl.String


def _cell(value: Any, dtype: Any) -> Any:
    if value is None or isinstance(value, bool):
        return None
    if dtype == pl.Float64:
        return float(value) if isinstance(value, (int, float)) else None
    if dtype == pl.Int64:
        return value if isinstance(value, int) else None
    if dtype == pl.String:
        return value if isinstance(value, str) else str(value)
    if isinstance(value, list):  # List(Int64) / List(String)
        inner = int if dtype == pl.List(pl.Int64) else str
        return [v for v in value if isinstance(v, inner) and not isinstance(v, bool)]
    return None


def _frame(
    rows: List[Dict[str, Any]], cols: Sequence[str], columns: Optional[Sequence[str]], return_as_pandas: bool
) -> Frame:
    """A frame with exactly ``cols`` (then any documented ``columns`` it lacks), dtypes by name."""
    names = list(dict.fromkeys([*cols, *(columns or [])]))
    schema = {c: _dtype_for(c) for c in names}
    data = {c: [_cell(r.get(c), schema[c]) for r in rows] for c in names}
    df = pl.DataFrame(data, schema=schema)
    return df.to_pandas() if return_as_pandas else df


def _record(raw: Any, kind: str, key: str) -> Optional[Dict[str, Any]]:
    """The page record: ``raw`` itself when it is one (it has ``key``), else ``raw`` parsed as HTML."""
    if isinstance(raw, dict):
        return raw if key in raw else None
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if isinstance(raw, str) and raw.strip():
        try:
            return _page_record(kind, raw)
        except _NotParsed:
            return None
    return None


def _req(rec: Dict[str, Any]) -> Dict[str, Any]:
    r = rec.get(_REQUEST)
    return dict(r) if isinstance(r, dict) else {}


def _end_year(label: Any) -> Optional[int]:
    """``'25/26'`` -> 2026, ``'2025/2026'`` -> 2026; anything else -> None."""
    m = re.fullmatch(r"\s*(\d{2}|\d{4})\s*/\s*(\d{2}|\d{4})\s*", label or "") if isinstance(label, str) else None
    if not m:
        return None
    end = int(m.group(2))
    return end if end >= 1000 else 2000 + end


def _stage(label: Any) -> Optional[str]:
    """``'Regular Season'`` -> ``'regular'``, ``'Playoffs'`` -> ``'playoff'``; else None."""
    low = (label or "").strip().lower() if isinstance(label, str) else ""
    return "regular" if low.startswith("regular") else "playoff" if low.startswith("playoff") else None


def _obj(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> List[Dict[str, Any]]:
    return [v for v in value if isinstance(v, dict)] if isinstance(value, list) else []


def _scope(rec: Dict[str, Any]) -> Dict[str, Any]:
    """``season`` / ``stage`` / ``tn`` of a tournament page: the page's own, the request's as fallback."""
    t = _obj(rec.get("tournament"))
    req = _req(rec)
    return {
        "season": _end_year(t.get("season")) or req.get("season"),
        "stage": _stage(t.get("stage")) or req.get("stage"),
        "tn": t.get("tn") if isinstance(t.get("tn"), int) else req.get("tn"),
    }


def _iso_day(text: Any, season: Optional[int]) -> Optional[str]:
    """Calendar ``'18 March'`` -> ``'2026-03-18'`` (August to December fall in the START year)."""
    m = re.fullmatch(r"\s*(\d{1,2})\s+([A-Za-z]+)\s*", text) if isinstance(text, str) else None
    mon = EN_MONTHS.get(m.group(2).lower()) if m else None
    if not m or not mon or season is None:
        return None
    return f"{season - 1 if mon >= 8 else season}-{mon:02d}-{int(m.group(1)):02d}"


# ------------------------------------------------------------------------------- shapers
_SCHEDULE_COLS = [
    "season",
    "stage",
    "tn",
    "game_id",
    "game_number",
    "game_date",
    "date",
    "weekday",
    "home_team",
    "home_club_id",
    "away_team",
    "away_club_id",
    "city",
    "score",
    "home_score",
    "away_score",
    "decision",
    "winner",
    "status",
    "online_url",
]


def parse_vhl_schedule(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Games of one tournament (``/en/calendar/{tn}/0/``), one row per game.

    Args:
        raw: the calendar record (``vhl_schedule(..., return_parsed=False)``) or the page's HTML.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per game: ``season`` / ``stage`` / ``tn``, ``game_id``, ``game_number``,
        ``game_date`` (ISO, from the day heading and the season), the two teams with their
        club ids, score, ``decision`` (OT / SO), ``winner`` and the broadcast URL.

    Example:
        Quick start::

            from sportsdataverse.hockey.vhl import vhl_schedule, parse_vhl_schedule

            raw = vhl_schedule(season=2026, return_parsed=False)
            games = parse_vhl_schedule(raw)

        A saved calendar page (the parsers also take HTML text)::

            with open("calendar_1374.html", encoding="utf-8") as fh:
                games = parse_vhl_schedule(fh.read())

    See Also:
        * :func:`sportsdataverse.hockey.vhl.vhl_schedule` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rec = _record(raw, "calendar", "games")
    if rec is None:
        return _frame([], _SCHEDULE_COLS, columns, return_as_pandas)
    scope = _scope(rec)
    rows = [{**g, **scope, "game_date": _iso_day(g.get("date"), scope["season"])} for g in _list(rec.get("games"))]
    return _frame(rows, _SCHEDULE_COLS, columns, return_as_pandas)


_GAME_KEY = ["game_id", "season", "stage", "game_number"]
_GAME_INFO_COLS = [
    *_GAME_KEY,
    "tn",
    "date",
    "local_time",
    "date_text",
    "attendance",
    "score",
    "home_score",
    "away_score",
    "decision",
    "period_scores",
    "home_team_page_id",
    "home_team",
    "home_short",
    "home_city",
    "home_coach",
    "away_team_page_id",
    "away_team",
    "away_short",
    "away_city",
    "away_coach",
    "referee1_number",
    "referee1",
    "referee2_number",
    "referee2",
    "linesman1_number",
    "linesman1",
    "linesman2_number",
    "linesman2",
]


def _report_key(rec: Dict[str, Any]) -> Dict[str, Any]:
    """The game key of a report record: page fields first, the request stamp for what the page lacks."""
    g = _obj(rec.get("game"))
    req = _req(rec)
    return {
        "game_id": req.get("game_id"),
        "season": _end_year(g.get("season")) or req.get("season"),
        "stage": _stage(g.get("stage")) or req.get("stage"),
        "game_number": g.get("game_number"),
        "tn": req.get("tn"),
    }


def parse_vhl_game_info(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The game header of a report (``/en/report/{tn}/?idgame=``) as one row.

    Args:
        raw: the report record (``vhl_game_info(..., return_parsed=False)``) or the page's HTML.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row: the game key, date and local time, attendance, final and per-period score,
        both teams (page id, full and short name, city, coach) and the officials (the first two
        referees and linesmen, with sweater numbers).

    Example:
        Quick start::

            from sportsdataverse.hockey.vhl import vhl_game_info, parse_vhl_game_info

            raw = vhl_game_info(season=2026, game_id=899223, return_parsed=False)
            info = parse_vhl_game_info(raw)

    See Also:
        * :func:`sportsdataverse.hockey.vhl.vhl_game_info` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rec = _record(raw, "report", "game")
    g = rec.get("game") if rec is not None else None
    if not isinstance(g, dict):
        return _frame([], _GAME_INFO_COLS, columns, return_as_pandas)
    assert rec is not None
    row = {**{k: v for k, v in g.items() if k not in ("season", "stage")}, **_report_key(rec)}
    for kind, key in (("referee", "referees"), ("linesman", "linesmen")):
        for i, o in enumerate(_list(g.get(key))[:2], 1):
            row[f"{kind}{i}_number"], row[f"{kind}{i}"] = o.get("number"), o.get("name")
    return _frame([row], _GAME_INFO_COLS, columns, return_as_pandas)


_GOALS_COLS = [*_GAME_KEY, "team_side", "team_page_id", *_spec_cols(REPORT_GOALS)]


def _score(s: Any) -> Optional[Tuple[int, int]]:
    m = re.fullmatch(r"\s*(\d+)\s*:\s*(\d+)\s*", s) if isinstance(s, str) else None
    return (int(m.group(1)), int(m.group(2))) if m else None


def _side_page(rec: Dict[str, Any], side: Any) -> Dict[str, Any]:
    """``team_page_id`` / ``team`` (short name) of a report row's ``team_side``."""
    g = _obj(rec.get("game"))
    if side not in ("home", "away"):
        return {"team_page_id": None, "team": None}
    return {"team_page_id": g.get(f"{side}_team_page_id"), "team": g.get(f"{side}_short")}


def parse_vhl_goals(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The scoring summary of a report, one row per goal (the shootout winner is the last row).

    Args:
        raw: the report record (``vhl_goals(..., return_parsed=False)``) or the page's HTML.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per goal: number, period, time, score after the goal, ``team_side`` (the side
        whose score went up), strength code, scorer and assists (id, name, jersey, season count)
        and ``home_on_ice`` / ``away_on_ice``, the JERSEY numbers on ice (``List(Int64)``).

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.hockey.vhl import vhl_goals, parse_vhl_goals

            raw = vhl_goals(season=2026, game_id=899223, return_parsed=False)
            goals = parse_vhl_goals(raw)

        Power-play goals::

            goals.filter(pl.col("strength") == "ppg")

    See Also:
        * :func:`sportsdataverse.hockey.vhl.vhl_goals` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rec = _record(raw, "report", "goals")
    if rec is None:
        return _frame([], _GOALS_COLS, columns, return_as_pandas)
    key = _report_key(rec)
    rows, prev = [], (0, 0)
    for gl in _list(rec.get("goals")):
        now = _score(gl.get("score"))
        side = None
        if now is not None:
            up = (now[0] > prev[0], now[1] > prev[1])
            side = "home" if up == (True, False) else "away" if up == (False, True) else None
            prev = now
        rows.append({**gl, **key, "team_side": side, "team_page_id": _side_page(rec, side)["team_page_id"]})
    return _frame(rows, _GOALS_COLS, columns, return_as_pandas)


_TEAM_COLS = ["team_side", "team_page_id", "team"]
_PENALTY_COLS = [*_GAME_KEY, *_TEAM_COLS, *_spec_cols(REPORT_PENALTIES)]


def _side_rows(rec: Dict[str, Any], key: str) -> List[Dict[str, Any]]:
    k = _report_key(rec)
    return [{**r, **k, **_side_page(rec, r.get("team_side"))} for r in _list(rec.get(key))]


def parse_vhl_penalties(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The penalty summary of a report, one row per penalty (per-period total rows dropped).

    Args:
        raw: the report record (``vhl_penalties(..., return_parsed=False)``) or the page's HTML.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per penalty: team, time, player (null for a bench penalty), minutes and the
        infraction (null when the site leaves it blank).

    Example:
        Quick start::

            from sportsdataverse.hockey.vhl import vhl_penalties, parse_vhl_penalties

            raw = vhl_penalties(season=2026, game_id=899223, return_parsed=False)
            penalties = parse_vhl_penalties(raw)

    See Also:
        * :func:`sportsdataverse.hockey.vhl.vhl_penalties` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rec = _record(raw, "report", "penalties")
    rows = _side_rows(rec, "penalties") if rec is not None else []
    return _frame(rows, _PENALTY_COLS, columns, return_as_pandas)


_PLAYER_BOX_COLS = [*_GAME_KEY, *_TEAM_COLS, "position_group", *_spec_cols(REPORT_SKATERS)]
_GOALIE_BOX_COLS = [*_GAME_KEY, *_TEAM_COLS, *_spec_cols(REPORT_GOALIES)]


def parse_vhl_player_box(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Skater lines of a report (Players summary > Defensmen / Forwards), one row per skater.

    Args:
        raw: the report record (``vhl_player_box(..., return_parsed=False)``) or the page's HTML.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per skater: team, ``position_group`` (defense / forward), jersey, player,
        captaincy and the game's scoring, shooting, faceoff, time-on-ice, hit and block stats.

    Example:
        Quick start::

            from sportsdataverse.hockey.vhl import vhl_player_box, parse_vhl_goalie_box, parse_vhl_player_box

            raw = vhl_player_box(season=2026, game_id=899223, return_parsed=False)
            skaters = parse_vhl_player_box(raw)

        The same report, more frames (one fetch)::

            goalies = parse_vhl_goalie_box(raw)

    See Also:
        * :func:`sportsdataverse.hockey.vhl.vhl_player_box` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rec = _record(raw, "report", "skaters")
    rows = _side_rows(rec, "skaters") if rec is not None else []
    return _frame(rows, _PLAYER_BOX_COLS, columns, return_as_pandas)


def parse_vhl_goalie_box(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Goalie lines of a report (Players summary > Goaltenders); GP 0 = dressed, did not play.

    Args:
        raw: the report record (``vhl_goalie_box(..., return_parsed=False)``) or the page's HTML.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per goalie: team, jersey, player, decision counts, shots against, goals
        against, saves, ``save_pct`` (0-100) and ``gaa`` (``Float64``), and time on ice.

    Example:
        Quick start::

            from sportsdataverse.hockey.vhl import vhl_goalie_box, parse_vhl_goalie_box

            raw = vhl_goalie_box(season=2026, game_id=899223, return_parsed=False)
            goalies = parse_vhl_goalie_box(raw)

    See Also:
        * :func:`sportsdataverse.hockey.vhl.vhl_goalie_box` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rec = _record(raw, "report", "goalies")
    rows = _side_rows(rec, "goalies") if rec is not None else []
    return _frame(rows, _GOALIE_BOX_COLS, columns, return_as_pandas)


_PBP_COLS = [
    "game_id",
    "season",
    "stage",
    "game_number",
    "event_index",
    "kind",
    "kind_code",
    "time",
    "clock",
    "score",
    "strength_code",
    "strength",
    "text",
    "player_ids",
    "home_team",
    "away_team",
]


def parse_vhl_pbp(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The text broadcast (``online.vhlru.ru/online/{game_id}.html``), one row per event.

    Args:
        raw: the broadcast record (``vhl_pbp(..., return_parsed=False)``) or the page's HTML.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per event in page order (``event_index`` 0 = the first event the page lists, the
        latest one): ``kind`` (game_state, goal, penalty, shootout_attempt, goalie_change,
        video_review, comment), the clock (``time`` with ``clock`` = game / minute / wall),
        score, strength, the RUSSIAN text and the linked ``player_ids``. No shot coordinates.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.hockey.vhl import vhl_pbp, parse_vhl_pbp

            raw = vhl_pbp(game_id=899223, season=2026, return_parsed=False)
            events = parse_vhl_pbp(raw)

        Goals only::

            events.filter(pl.col("kind") == "goal")

    See Also:
        * :func:`sportsdataverse.hockey.vhl.vhl_pbp` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rec = _record(raw, "online", "events")
    if rec is None:
        return _frame([], _PBP_COLS, columns, return_as_pandas)
    g = _obj(rec.get("game"))
    req = _req(rec)
    key = {
        "game_id": req.get("game_id"),
        "season": req.get("season"),
        "stage": req.get("stage"),
        "game_number": g.get("game_number"),
        "home_team": g.get("home_team"),
        "away_team": g.get("away_team"),
    }
    rows = [{**e, **key, "event_index": i} for i, e in enumerate(_list(rec.get("events")))]
    return _frame(rows, _PBP_COLS, columns, return_as_pandas)


_SCOPE_COLS = ["season", "stage", "tn"]
_STANDINGS_COLS = [*_SCOPE_COLS, *_spec_cols(STANDINGS)]


def parse_vhl_standings(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """League standings of one regular season (``/en/standings/league/{tn}/``), one row per team.

    Args:
        raw: the standings record (``vhl_standings(..., return_parsed=False)``) or the page's HTML.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per team: rank, team (short name) and its ``team_season_id``, games, wins and
        losses by decision type, goals for / against and points.

    Example:
        Quick start::

            from sportsdataverse.hockey.vhl import vhl_standings, parse_vhl_standings

            raw = vhl_standings(season=2026, return_parsed=False)
            table = parse_vhl_standings(raw)

    See Also:
        * :func:`sportsdataverse.hockey.vhl.vhl_standings` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rec = _record(raw, "standings", "rows")
    if rec is None:
        return _frame([], _STANDINGS_COLS, columns, return_as_pandas)
    scope = _scope(rec)
    rows = [{**r, **scope} for r in _list(rec.get("rows"))]
    return _frame(rows, _STANDINGS_COLS, columns, return_as_pandas)


_SKATER_STATS_COLS = [*_SCOPE_COLS, "position_group", *_spec_cols(STATS_SKATERS)]
_GOALIE_STATS_COLS = [*_SCOPE_COLS, *_spec_cols(STATS_GOALIES)]


def parse_vhl_skater_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Season stats of every skater of a tournament (``/en/stats/players/{tn}/``).

    Args:
        raw: the stats record (``vhl_skater_stats(..., return_parsed=False)``) or the page's HTML.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per skater and team (defensemen first, then forwards; ``position_group``),
        with scoring, shooting, faceoff, time-on-ice, hit and block stats.

    Example:
        Quick start::

            from sportsdataverse.hockey.vhl import vhl_skater_stats, parse_vhl_goalie_stats, parse_vhl_skater_stats

            raw = vhl_skater_stats(season=2026, return_parsed=False)
            skaters = parse_vhl_skater_stats(raw)

        The same page holds the goalies::

            goalies = parse_vhl_goalie_stats(raw)

    See Also:
        * :func:`sportsdataverse.hockey.vhl.vhl_skater_stats` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rec = _record(raw, "stats_players", "forwards")
    if rec is None:
        return _frame([], _SKATER_STATS_COLS, columns, return_as_pandas)
    scope = _scope(rec)
    rows = [
        {**r, **scope, "position_group": group}
        for key, group in (("defensemen", "defense"), ("forwards", "forward"))
        for r in _list(rec.get(key))
    ]
    return _frame(rows, _SKATER_STATS_COLS, columns, return_as_pandas)


def parse_vhl_goalie_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Season stats of every goalie of a tournament (``/en/stats/players/{tn}/``).

    Args:
        raw: the stats record (``vhl_goalie_stats(..., return_parsed=False)``) or the page's HTML.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per goalie and team: decisions, shots against, goals against, saves,
        ``save_pct`` (0-100), ``gaa``, shutouts and time on ice.

    Example:
        Quick start::

            from sportsdataverse.hockey.vhl import vhl_goalie_stats, parse_vhl_goalie_stats

            raw = vhl_goalie_stats(season=2026, stage="playoff", return_parsed=False)
            goalies = parse_vhl_goalie_stats(raw, return_as_pandas=True)

    See Also:
        * :func:`sportsdataverse.hockey.vhl.vhl_goalie_stats` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rec = _record(raw, "stats_players", "goalies")
    if rec is None:
        return _frame([], _GOALIE_STATS_COLS, columns, return_as_pandas)
    scope = _scope(rec)
    rows = [{**r, **scope} for r in _list(rec.get("goalies"))]
    return _frame(rows, _GOALIE_STATS_COLS, columns, return_as_pandas)


_PLAYER_COLS = [
    "player_id",
    "name",
    "jersey",
    "position",
    "height_cm",
    "weight_kg",
    "born",
    "age",
    "country",
    "photo_url",
]


def parse_vhl_player(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """A player's card (``/en/players/{player_id}/``) as one row.

    Args:
        raw: the player record (``vhl_player(..., return_parsed=False)``) or the page's HTML.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row: id, name, jersey, position, height (cm), weight (kg), birth date
        (``DD.MM.YYYY``, public bio), age, country and photo URL.

    Example:
        Quick start::

            from sportsdataverse.hockey.vhl import vhl_player, parse_vhl_player

            raw = vhl_player(player_id=34526, return_parsed=False)
            card = parse_vhl_player(raw)

    See Also:
        * :func:`sportsdataverse.hockey.vhl.vhl_player` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rec = _record(raw, "player", "player_id")
    rows = [rec] if rec is not None else []
    return _frame(rows, _PLAYER_COLS, columns, return_as_pandas)


_CAREER_STATS = list(dict.fromkeys([*_spec_cols(CAREER_SKATER), *_spec_cols(CAREER_GOALIE)]))
_CAREER_COLS = ["player_id", "season", "stage", "tn", "team_season_id", "team", "jersey", *_CAREER_STATS]


def parse_vhl_player_career(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """A player's career by season, stage and team (``/en/players/{player_id}/`` table 1).

    Args:
        raw: the player record (``vhl_player_career(..., return_parsed=False)``) or the page's HTML.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per season stage and team, with its ``tn`` and ``team_season_id``. Skater and
        goalie pages share one column set: the goalie columns are null for a skater and the
        other way round.

    Example:
        Quick start::

            from sportsdataverse.hockey.vhl import vhl_player_career, parse_vhl_player_career

            raw = vhl_player_career(player_id=34526, return_parsed=False)
            career = parse_vhl_player_career(raw)

    See Also:
        * :func:`sportsdataverse.hockey.vhl.vhl_player_career` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rec = _record(raw, "player", "seasons")
    if rec is None:
        return _frame([], _CAREER_COLS, columns, return_as_pandas)
    rows = [
        {**r, "player_id": rec.get("player_id"), "season": _end_year(r.get("season")), "stage": _stage(r.get("stage"))}
        for r in _list(rec.get("seasons"))
    ]
    return _frame(rows, _CAREER_COLS, columns, return_as_pandas)


_ROSTER_COLS = [
    "team_page_id",
    "team",
    "calendar_tn",
    "calendar_team_season_id",
    "roster_as_of",
    *_spec_cols(ROSTER),
]


def parse_vhl_roster(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The CURRENT roster of a team page (``/en/teams/{team_page_id}/``), one row per player.

    Args:
        raw: the team record (``vhl_roster(..., return_parsed=False)``) or the page's HTML.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per player: the team (page id from the request, name, the current tournament's
        ``calendar_tn`` + ``calendar_team_season_id``, ``roster_as_of``), jersey, position,
        player id and name (Cyrillic even on /en/), birth date and country.

    Example:
        Quick start::

            from sportsdataverse.hockey.vhl import vhl_roster, parse_vhl_roster

            raw = vhl_roster(team_page_id=390, return_parsed=False)
            roster = parse_vhl_roster(raw)

    See Also:
        * :func:`sportsdataverse.hockey.vhl.vhl_roster` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rec = _record(raw, "team", "roster")
    if rec is None:
        return _frame([], _ROSTER_COLS, columns, return_as_pandas)
    team = {
        "team_page_id": _req(rec).get("team_page_id"),
        "team": rec.get("name"),
        "calendar_tn": rec.get("calendar_tn"),
        "calendar_team_season_id": rec.get("calendar_team_season_id"),
        "roster_as_of": rec.get("roster_as_of"),
    }
    rows = [{**r, **team} for r in _list(rec.get("roster"))]
    return _frame(rows, _ROSTER_COLS, columns, return_as_pandas)
