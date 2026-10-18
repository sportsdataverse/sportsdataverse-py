"""Parsers for the generated ``feb`` wrappers (Spanish FEB: Primera FEB, Liga Femenina Endesa, Segunda FEB).

Two hosts feed this family. The JSON side (``intrafeb.feb.es/LiveStats.API``) answers one fixed
10-block envelope (``HEADER, SCOREBOARD, TEAMSTATS, TICKER, OVERVIEW, PLAYBYPLAY, BANNER,
BOXSCORE, SHOTCHART, RANKING``) of which each route fills only its own blocks. The schedule side
(``baloncestoenvivo.feb.es``) is server-rendered ASP.NET HTML, parsed here with the stdlib
:mod:`html.parser` into the records the refs captures bank (``sdv-internal-refs`` ``feb/tools/capture.py``):
a minimal DOM, iterative walks (a deep page never raises ``RecursionError``), and header / width
checks that refuse layout drift instead of guessing.

Each public ``parse_feb_*`` takes the dict the wrapper returns with ``return_parsed=False`` (the
envelope, or the page record, with the request stamped under ``_request`` by ``feb_runtime``) and
follows the package parser contract: a polars frame by default, pandas with
``return_as_pandas=True``, snake_case columns, and a zero-row frame with the documented columns and
dtypes, never an exception, for an empty or unrecognised input.

**Dtypes** are pinned by column name, never sampled. The API sends ids, counts and seconds as
STRINGS and percentages as strings with a DECIMAL COMMA (``"83,3"``); the parsers cast at the
boundary: ids and counts ``Int64``, percentages ``Float64`` (0-100), shot ``x`` / ``y``
``Float64``. Team codes (``team_code`` ``"02014401"``, ``club_code`` ``"020144"``) and shirt numbers
(``"00"`` differs from ``"0"``) stay strings.

**Id key spaces.** Every id is ``Int64``; none joins another directly except where noted.

* ``game_id``: FEB games, one global sequence. The ``{gameId}`` of every API route, the ``p=`` of
  ``Partido.aspx`` and the schedule pages' ``game_id``; the API bodies do not print it, so a frame
  parsed from a bare envelope has a null ``game_id`` (the runtime stamps it under ``_request``).
* ``player_id``: FEB PERSONS, stable across seasons and clubs. The box ``id``, the play-by-play
  ``idPlayer`` and the leaders' ``id`` are this key. ``license_id`` (play-by-play only) is the
  player LICENCE, one per season registration: it changes when the player re-registers.
* ``team_id``: a TEAM-SEASON registration, NEW every season (HLA Alicante = 979989 in 2025-26 and
  1008687 in 2026-27). It joins the schedule's ``home_team_id`` / ``away_team_id`` and the overview.
  ``club_code`` / ``team_code`` are the federation's club and club+team codes (strings).
* ``comp_id``: the competition (Primera FEB 115, Segunda FEB 118, Liga Femenina Endesa 210),
  constant across seasons. ``group`` is the site's menu group (``g=1`` Primera FEB), which is
  NOT a competition id; ``comp_id`` ties the two on the results page.
* Shots carry the shirt number, not a player id: ``feb_shots`` resolves ``player_id`` through the
  game's roster (side + shirt number) when that pair is unique.

**Sides.** ``team_side`` is ``home`` / ``away`` in every frame. The raw API uses three codings
for the same sides (TEAM[0] / TEAM[1]; play-by-play ``1`` / ``2``; shot chart ``0`` / ``1``); the
parsers normalise them.

**Season** is the END year (2027 = 2026-27). The schedule site keys a season by its START year
(``t``); ``feb_runtime`` maps it, and the page's own season label is checked against the request.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from html.parser import HTMLParser
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union

import pandas as pd
import polars as pl

__all__ = [
    "parse_feb_game_info",
    "parse_feb_leaders",
    "parse_feb_overview",
    "parse_feb_pbp",
    "parse_feb_period_scores",
    "parse_feb_player_box",
    "parse_feb_results",
    "parse_feb_schedule",
    "parse_feb_shots",
    "parse_feb_standings",
    "parse_feb_team_stats",
]

Frame = Union[pl.DataFrame, pd.DataFrame]

#: Where the runtime stamps the request (``feb_runtime.REQUEST_KEY``).
_REQUEST = "_request"

INT, F, S, B, D = pl.Int64, pl.Float64, pl.Utf8, pl.Boolean, pl.Date
_DT = pl.Datetime("us")


class _NotParsed(Exception):
    """The page is not the data asked for: a missing selector or table, header drift, a row of the
    wrong width, or another scope (group, season).

    ``empty`` marks a page that is the right one but holds no games or results; the runtime raises
    it as ``NoDataError``. Every other refusal is an ``AssetFetchError``.
    """

    def __init__(self, msg: str, *, empty: bool = False) -> None:
        super().__init__(msg)
        self.empty = empty


# --------------------------------------------------------------------------------------- DOM
_VOID = frozenset(
    {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}
)


class _Node:
    """One element: tag, attributes, children (``_Node`` or text) and parent."""

    __slots__ = ("attrs", "children", "parent", "tag")

    def __init__(self, tag: str, attrs: Sequence[Tuple[str, Optional[str]]], parent: Optional["_Node"]) -> None:
        self.tag = tag
        self.attrs: Dict[str, Optional[str]] = dict(attrs)
        self.children: List[Union["_Node", str]] = []
        self.parent = parent

    def iter(self) -> Iterator["_Node"]:
        """Descendants in document order through an explicit stack (deep pages cannot recurse)."""
        stack: List[Union["_Node", str]] = list(reversed(self.children))
        while stack:
            node = stack.pop()
            if isinstance(node, _Node):
                yield node
                stack.extend(reversed(node.children))

    def text(self) -> str:
        """Collapsed text of the subtree; ``<br>`` separates words (``SABADO<br/>17/10/2026``)."""
        parts: List[str] = []
        stack: List[Union["_Node", str]] = list(reversed(self.children))
        while stack:
            node = stack.pop()
            if isinstance(node, _Node):
                if node.tag == "br":
                    parts.append(" ")
                stack.extend(reversed(node.children))
            else:
                parts.append(node)
        return re.sub(r"\s+", " ", "".join(parts)).strip()

    def classes(self) -> set:
        """The element's CSS classes."""
        return set((self.attrs.get("class") or "").split())

    def find(
        self,
        tag: Optional[str] = None,
        cls: Optional[str] = None,
        id_end: Optional[str] = None,
        name_end: Optional[str] = None,
    ) -> Optional["_Node"]:
        """First descendant matching every given criterion, else ``None``."""
        return next(self.findall(tag, cls, id_end, name_end), None)

    def findall(
        self,
        tag: Optional[str] = None,
        cls: Optional[str] = None,
        id_end: Optional[str] = None,
        name_end: Optional[str] = None,
    ) -> Iterator["_Node"]:
        """Descendants matching every given criterion (tag, class, id suffix, name suffix)."""
        for n in self.iter():
            if tag and n.tag != tag:
                continue
            if cls and cls not in n.classes():
                continue
            if id_end and not (n.attrs.get("id") or "").endswith(id_end):
                continue
            if name_end and not (n.attrs.get("name") or "").endswith(name_end):
                continue
            yield n


class _Builder(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.root = self.cur = _Node("#root", (), None)

    def handle_starttag(self, tag: str, attrs: Any) -> None:
        node = _Node(tag, attrs, self.cur)
        self.cur.children.append(node)
        if tag not in _VOID:
            self.cur = node

    def handle_startendtag(self, tag: str, attrs: Any) -> None:
        self.cur.children.append(_Node(tag, attrs, self.cur))

    def handle_endtag(self, tag: str) -> None:
        n: Optional[_Node] = self.cur
        while n is not None and n.tag != tag:
            n = n.parent
        if n is not None and n.parent is not None:  # unmatched end tags are ignored
            self.cur = n.parent

    def handle_data(self, data: str) -> None:
        self.cur.children.append(data)


def _dom(html: str) -> _Node:
    b = _Builder()
    b.feed(html)
    b.close()
    return b.root


def _same(a: Any, b: Any) -> bool:
    """Type-strict equality: ``1`` is not ``True`` and ``"1"`` is not ``1``; tuples compare element-wise."""
    if isinstance(a, (tuple, list)) and isinstance(b, (tuple, list)):
        return type(a) is type(b) and len(a) == len(b) and all(_same(x, y) for x, y in zip(a, b))
    return type(a) is type(b) and a == b


def _int_text(s: Optional[str]) -> Optional[int]:
    s = (s or "").strip().replace(".", "")
    return int(s) if re.fullmatch(r"[+-]?\d+", s) else None


def _qint(href: Optional[str], key: str) -> Optional[int]:
    m = re.search(rf"[?&]{key}=(-?\d+)", href or "")
    return int(m.group(1)) if m else None


def _iso(dmy: Optional[str]) -> Optional[str]:
    m = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", (dmy or "").strip())
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1))).isoformat()
    except ValueError:
        return None


def _options(select: Optional[_Node]) -> List[Dict[str, Any]]:
    if select is None:
        return []
    return [
        {"value": _int_text(o.attrs.get("value")), "label": o.text(), "selected": "selected" in o.attrs}
        for o in select.findall("option")
    ]


def _cells(tr: _Node) -> List[_Node]:
    return [c for c in tr.children if isinstance(c, _Node) and c.tag in ("td", "th")]


def _rows(table: _Node, header: List[str], what: str) -> List[List[_Node]]:
    trs = list(table.findall("tr"))
    if not trs:
        raise _NotParsed(f"{what}: no rows")
    got = [c.text() for c in _cells(trs[0])]
    if got != header:
        raise _NotParsed(f"{what}: header drift {got} != {header}")
    body = [_cells(tr) for tr in trs[1:]]
    for cells in body:
        if len(cells) != len(header):
            raise _NotParsed(f"{what}: row width {len(cells)} != {len(header)}")
    return body


def _team(cell: _Node, which: int = 0) -> Tuple[Optional[int], str]:
    links = [a for a in cell.findall("a") if "Equipo.aspx" in (a.attrs.get("href") or "")]
    if len(links) <= which:
        raise _NotParsed("team link missing")
    a = links[which]
    return _qint(a.attrs.get("href"), "i"), a.text()


def _game(cell: _Node) -> Tuple[Optional[int], str]:
    a = next((a for a in cell.findall("a") if "Partido.aspx" in (a.attrs.get("href") or "")), None)
    if a is None:
        raise _NotParsed("game link missing")
    return _qint(a.attrs.get("href"), "p"), a.text()


_WEEKDAYS = {
    "LUNES": "Monday",
    "MARTES": "Tuesday",
    "MIÉRCOLES": "Wednesday",
    "JUEVES": "Thursday",
    "VIERNES": "Friday",
    "SÁBADO": "Saturday",
    "DOMINGO": "Sunday",
}


def _when(text: str) -> Dict[str, Optional[str]]:
    """A scheduled calendar cell (``SÁBADO 17/10/2026 19:30``) -> English weekday, ISO date, time."""
    m = re.fullmatch(r"(\S+)\s+(\d{2}/\d{2}/\d{4})(?:\s+(\d{1,2}:\d{2}))?", text.strip())
    if not m or m.group(1).upper() not in _WEEKDAYS:
        return {"weekday": None, "date": None, "time": None}
    return {"weekday": _WEEKDAYS[m.group(1).upper()], "date": _iso(m.group(2)), "time": m.group(3)}


def _score(text: str) -> Tuple[Optional[int], Optional[int], str]:
    m = re.fullmatch(r"(\d+)-(\d+)", text.strip())
    return (int(m.group(1)), int(m.group(2)), "played") if m else (None, None, "scheduled")


def _header(root: _Node, g: int, t: int) -> Dict[str, Any]:
    """Page title block + season select, checked against the request (type-strict, fails closed)."""
    logo = root.find("img", id_end="logoCategoriaImage")
    src = (logo.attrs.get("src") or "") if logo else ""
    wrap = root.find("div", cls="wrapper-text")
    liga = wrap.find("span", cls="liga") if wrap else None
    temporada = wrap.find("span", cls="temporada") if wrap else None
    seasons = _options(root.find("select", name_end="temporadasDropDownList"))
    selected = [o["value"] for o in seasons if o["selected"]]
    if not (logo and liga and temporada and seasons):
        raise _NotParsed("page header (logo, liga, temporada, season select) missing")
    if not _same((_qint(src, "g"), _qint(src, "t")), (g, t)):
        raise _NotParsed(f"out of scope: logo g/t {(_qint(src, 'g'), _qint(src, 't'))}, asked for {(g, t)}")
    if not _same(selected, [t]) or not _same(temporada.text(), f"{t}/{t + 1}"):
        raise _NotParsed(f"out of scope: season {selected} / {temporada.text()!r}, asked for {t}")
    return {
        "group": g,
        "season_start": t,
        "season_label": temporada.text(),
        "competition": liga.text(),
        "seasons": [{"season_start": o["value"], "label": o["label"]} for o in seasons],
        "phases": [
            {"phase_id": o["value"], "label": o["label"], "selected": o["selected"]}
            for o in _options(root.find("select", name_end="gruposDropDownList"))
        ],
    }


def _calendar_record(html: str, g: int, t: int) -> Dict[str, Any]:
    root = _dom(html)
    rec = _header(root, g, t)
    page = root.find("div", id_end="page-calendario") or root
    rec["playoff_series_ids"] = sorted(
        {_qint(a.attrs.get("href"), "f") for a in page.findall("a") if "Series.aspx?f=" in (a.attrs.get("href") or "")}
    )
    games: List[Dict[str, Any]] = []
    rnd: Optional[Dict[str, Any]] = None
    for n in page.iter():
        if n.tag == "h1" and "titulo-modulo" in n.classes():
            m = re.fullmatch(r"Jornada\s+(\d+)\s*(.*)", n.text())
            if not m:
                raise _NotParsed(f"round header drift: {n.text()!r}")
            rnd = {"round": int(m.group(1)), "round_label": n.text(), "round_date": _iso(m.group(2))}
        elif n.tag == "table" and (n.attrs.get("id") or "").endswith("DataGrid3"):
            if rnd is None:
                raise _NotParsed("game table before any round header")
            for cells in _rows(n, ["Local", "Resultado", "Visitante"], "calendar"):
                hid, hname = _team(cells[0])
                aid, aname = _team(cells[2])
                gid, result = _game(cells[1])
                hs, as_, status = _score(result)
                games.append(
                    {
                        **rnd,
                        "game_id": gid,
                        "home_team_id": hid,
                        "home_team": hname,
                        "away_team_id": aid,
                        "away_team": aname,
                        "result_text": result,
                        "home_score": hs,
                        "away_score": as_,
                        "status": status,
                        **_when(result),
                    }
                )
    if not games:
        raise _NotParsed("calendar has zero games", empty=True)
    rec["games"] = games
    return rec


_RESULT_HEADER = ["Equipos", "Resultado", "Fecha", "Hora"]
_STANDINGS_HEADER = ["Po", "Equipo", "PJ", "PG", "PP", "PF", "PC", "DIF", "%", "R"]
_FINAL_HEADER = ["Po", "Equipo"]


def _round_games(table: Optional[_Node], what: str) -> List[Dict[str, Any]]:
    if table is None:
        return []
    out: List[Dict[str, Any]] = []
    for cells in _rows(table, _RESULT_HEADER, what):
        hid, hname = _team(cells[0], 0)
        aid, aname = _team(cells[0], 1)
        gid, result = _game(cells[1])
        hs, as_, status = _score(result)
        out.append(
            {
                "game_id": gid,
                "home_team_id": hid,
                "home_team": hname,
                "away_team_id": aid,
                "away_team": aname,
                "result_text": result,
                "home_score": hs,
                "away_score": as_,
                "status": status,
                "date": _iso(cells[2].text()),
                "time": cells[3].text() or None,
            }
        )
    return out


def _pct_text(s: Optional[str]) -> Optional[float]:
    m = re.fullmatch(r"(\d+(?:,\d+)?)%", (s or "").strip())
    return float(m.group(1).replace(",", ".")) if m else None


def _results_record(html: str, g: int, t: int) -> Dict[str, Any]:
    root = _dom(html)
    rec = _header(root, g, t)
    comp = root.find("input", id_end="CodCategoriaHiddenField")
    rec["comp_id"] = comp.attrs.get("value") if comp else None
    if not (isinstance(rec["comp_id"], str) and rec["comp_id"].isdigit()):
        raise _NotParsed("CodCategoriaHiddenField (CompID) missing")
    rec["rounds"] = [
        {"round_id": o["value"], "label": o["label"], "selected": o["selected"]}
        for o in _options(root.find("select", name_end="jornadasDropDownList"))
    ]
    titles = [h.text() for h in root.findall("h1", cls="titulo-modulo")]
    rec["results_title"] = next((x for x in titles if x.startswith("Resultados")), None)
    rec["next_round_title"] = next((x for x in titles if x.startswith("Próxima")), None)
    rec["results"] = _round_games(root.find("table", id_end="_jornadaDataGrid"), "results")
    rec["next_round"] = _round_games(root.find("table", id_end="_proximaJornadaDataGrid"), "next round")
    st = root.find("table", id_end="_clasificacionDataGrid")
    rec["standings"] = []
    if st is not None:
        for c in _rows(st, _STANDINGS_HEADER, "standings"):
            tid, name = _team(c[1])
            streak = c[9].text()
            rec["standings"].append(
                {
                    "position": _int_text(c[0].text()),
                    "team_id": tid,
                    "team": name,
                    "played": _int_text(c[2].text()),
                    "won": _int_text(c[3].text()),
                    "lost": _int_text(c[4].text()),
                    "points_for": _int_text(c[5].text()),
                    "points_against": _int_text(c[6].text()),
                    "point_diff": _int_text(c[7].text()),
                    "win_pct": _pct_text(c[8].text()),
                    "streak": _int_text(streak),
                    "streak_text": streak or None,
                }
            )
    fin = root.find("table", id_end="_clasificacionFinalDataGrid")
    rec["final_standings"] = []
    if fin is not None:
        for c in _rows(fin, _FINAL_HEADER, "final standings"):
            tid, name = _team(c[1])
            rec["final_standings"].append({"position": _int_text(c[0].text()), "team_id": tid, "team": name})
    if not rec["results"]:
        raise _NotParsed("results page has zero result rows", empty=True)
    return rec


def _page_record(kind: str, html: str, g: int, t: int) -> Dict[str, Any]:
    """Parse a schedule page (``kind`` ``calendar`` or ``results``) for group ``g`` and season START year ``t``.

    Raises:
        _NotParsed: not the page asked for (see the class); ``empty`` when it holds no games.
    """
    build = {"calendar": _calendar_record, "results": _results_record}[kind]
    try:
        return build(html, g, t)
    except _NotParsed:
        raise
    except Exception as exc:  # an odd page is a refusal, never a crash (RecursionError is a RuntimeError)
        raise _NotParsed(f"parser error: {type(exc).__name__}") from None


# ------------------------------------------------------------------------------------- casts
def _i(v: Any) -> Optional[int]:
    """An id / count / seconds from the API's string or integer, else ``None``. Never goes through float."""
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str) and re.fullmatch(r"\s*[+-]?\d+\s*", v):
        return int(v)
    return None


def _f(v: Any) -> Optional[float]:
    """A number from the API's string (``"83,3"`` decimal comma, ``"63.47"`` point) or number, else ``None``."""
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str) and re.fullmatch(r"\s*[+-]?\d+(?:[.,]\d+)?\s*", v):
        return float(v.strip().replace(",", "."))
    return None


def _s(v: Any) -> Optional[str]:
    """A stripped non-empty string, else ``None`` (numbers are stringified)."""
    if v is None or isinstance(v, (dict, list)):
        return None
    t = str(v).strip()
    return t or None


def _flag(v: Any) -> Optional[bool]:
    """The API's ``"1"`` / ``"0"`` flags as booleans."""
    n = _i(v)
    return None if n is None else n == 1


def _clock_seconds(v: Any) -> Optional[int]:
    """``"08:34"`` -> 514; ``None`` for anything that is not ``mm:ss``."""
    m = re.fullmatch(r"\s*(\d+):(\d{2})\s*", v) if isinstance(v, str) else None
    return int(m.group(1)) * 60 + int(m.group(2)) if m else None


def _local_dt(v: Any) -> Optional[datetime]:
    """``"26-09-2026 - 19:00"`` (header) or ISO ``2026-10-12T11:00:00`` -> a naive local datetime."""
    if not isinstance(v, str):
        return None
    for fmt in ("%d-%m-%Y - %H:%M", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(v.strip(), fmt)
        except ValueError:
            continue
    return None


def _utc_dt(v: Any) -> Optional[datetime]:
    """``"2026-10-12T09:00:00Z"`` -> a naive UTC datetime."""
    if not isinstance(v, str):
        return None
    try:
        return datetime.strptime(v.strip(), "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None


def _date(v: Any) -> Optional[date]:
    try:
        return date.fromisoformat(v) if isinstance(v, str) else None
    except ValueError:
        return None


def _log_dt(v: Any) -> Optional[datetime]:
    if not isinstance(v, str):
        return None
    try:
        return datetime.strptime(v.strip(), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


# ----------------------------------------------------------------------------------- frames
Spec = Dict[str, pl.DataType]


def _frame(rows: List[Dict[str, Any]], spec: Spec, columns: Optional[Sequence[str]], as_pandas: bool) -> Frame:
    """``rows`` as a frame with exactly ``spec``'s columns and dtypes (a zero-row frame when there are none).

    ``columns`` is the wrapper's documented column list; a name the spec lacks is added as a null string column.
    """
    full: Dict[str, pl.DataType] = dict(spec)
    for c in columns or ():
        full.setdefault(c, S)
    df = pl.DataFrame({c: [r.get(c) for r in rows] for c in full}, schema=full)
    return df.to_pandas() if as_pandas else df


def _env(raw: Any, *blocks: str) -> Optional[Dict[str, Any]]:
    """``raw`` when it is a dict holding a non-empty dict under one of ``blocks``, else ``None``."""
    if not isinstance(raw, dict):
        return None
    return raw if any(isinstance(raw.get(b), dict) and raw[b] for b in blocks) else None


def _game_id(raw: Dict[str, Any]) -> Optional[int]:
    req = raw.get(_REQUEST)
    return _i(req.get("game_id")) if isinstance(req, dict) else None


_SIDES = ("home", "away")


def _teams(raw: Dict[str, Any], block: str) -> List[Tuple[str, Dict[str, Any]]]:
    """``(side, team dict)`` of a block's ``TEAM`` list: index 0 is the home team, 1 the away team."""
    b = raw.get(block)
    teams = b.get("TEAM") if isinstance(b, dict) else None
    if not isinstance(teams, list):
        return []
    return [(_SIDES[i], t) for i, t in enumerate(teams[:2]) if isinstance(t, dict)]


def _team_ctx(team: Dict[str, Any], side: str, game_id: Optional[int]) -> Dict[str, Any]:
    return {
        "game_id": game_id,
        "team_side": side,
        "team_id": _i(team.get("id")),
        "team": _s(team.get("name")),
    }


_CTX_SPEC: Spec = {"game_id": INT, "team_side": S, "team_id": INT, "team": S}

# ---- game_info / period_scores -----------------------------------------------------------------
_INFO_SPEC: Spec = {
    "game_id": INT,
    "comp_id": INT,
    "competition": S,
    "group_label": S,
    "period": INT,
    "clock": S,
    "start_time": _DT,
    "venue": S,
    "address": S,
    "referee1": S,
    "referee2": S,
    "referee3": S,
    "status": INT,
    "status_text": S,
    "home_team_id": INT,
    "home_team": S,
    "home_team_code": S,
    "home_club_code": S,
    "home_score": INT,
    "away_team_id": INT,
    "away_team": S,
    "away_team_code": S,
    "away_club_code": S,
    "away_score": INT,
}


def _info_row(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    h = raw.get("HEADER")
    if not isinstance(h, dict) or not h:
        return None
    teams = dict(_teams(raw, "HEADER"))
    row: Dict[str, Any] = {
        "game_id": _game_id(raw),
        "comp_id": _i(h.get("CompID")),
        "competition": _s(h.get("competition")),
        "group_label": _s(h.get("round")),
        "period": _i(h.get("quarter")),
        "clock": _s(h.get("time")),
        "start_time": _local_dt(h.get("starttime")),
        "venue": _s(h.get("field")),
        "address": _s(h.get("place")),
        "referee1": _s(h.get("referee1")),
        "referee2": _s(h.get("referee2")),
        "referee3": _s(h.get("referee3")),
        "status": _i(h.get("status")),
        "status_text": _s(h.get("statusText")),
    }
    for side in _SIDES:
        t = teams.get(side) or {}
        row.update(
            {
                f"{side}_team_id": _i(t.get("id")),
                f"{side}_team": _s(t.get("name")),
                f"{side}_team_code": _s(t.get("teamCode")),
                f"{side}_club_code": _s(t.get("clubCode")),
                f"{side}_score": _i(t.get("pts")),
            }
        )
    return row


def parse_feb_game_info(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Game header of one FEB game: competition, tip-off, venue, officials, status, teams and score.

    Every game route carries the same ``HEADER`` block, so any of them feeds this parser.

    Args:
        raw: the envelope a game wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row. ``start_time`` is the tip-off in Spain local time (Europe/Madrid, no zone attached);
        ``clock`` is ``FINAL`` once the game is over. ``status`` / ``status_text`` are null on 2020-21 games.
        Scores are the final score when ``clock`` is ``FINAL``, else the live score. A body without a
        ``HEADER`` gives a zero-row frame with the documented columns.

    Raises:
        None: does not raise; an unusable body (not a dict, or without the block this parser reads) gives a zero-row
            frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.feb import feb_team_stats, parse_feb_game_info

            raw = feb_team_stats(game_id=2535725, return_parsed=False)
            info = parse_feb_game_info(raw)
    """
    row = _info_row(raw) if isinstance(raw, dict) else None
    return _frame([row] if row else [], _INFO_SPEC, columns, return_as_pandas)


_PERIOD_SPEC: Spec = {"game_id": INT, "period": INT, "home_points": INT, "away_points": INT}


def parse_feb_period_scores(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Points each team scored in each period of one FEB game (``HEADER.QUARTERS``).

    Args:
        raw: the envelope a game wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per period played. The points are scored IN the period, not cumulative. Overtime
        numbering is unverified (no overtime game is in the committed samples). A body without period
        scores gives a zero-row frame with the documented columns.

    Raises:
        None: does not raise; an unusable body (not a dict, or without the block this parser reads) gives a zero-row
            frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.feb import feb_team_stats, parse_feb_period_scores

            raw = feb_team_stats(game_id=2535725, return_parsed=False)
            periods = parse_feb_period_scores(raw)
    """
    rows: List[Dict[str, Any]] = []
    h = raw.get("HEADER") if isinstance(raw, dict) else None
    q = (
        (h.get("QUARTERS") or {}).get("QUARTER")
        if isinstance(h, dict) and isinstance(h.get("QUARTERS"), dict)
        else None
    )
    for p in q if isinstance(q, list) else []:
        if isinstance(p, dict):
            rows.append(
                {
                    "game_id": _game_id(raw),
                    "period": _i(p.get("n")),
                    "home_points": _i(p.get("scoreA")),
                    "away_points": _i(p.get("scoreB")),
                }
            )
    return _frame(rows, _PERIOD_SPEC, columns, return_as_pandas)


# ---- box score / team stats ----------------------------------------------------------------------
_SHOOT_INT = ("p1m", "p1a", "p2m", "p2a", "p3m", "p3a", "fgm", "fga")
_SHOOT_PCT = ("p1p", "p2p", "p3p", "fgp")
_COUNT_INT = ("bs", "tc", "mt", "ro", "rd", "rt", "rf", "to", "st", "assist", "pf", "pts", "val")

_PLAYER_SPEC: Spec = {
    **_CTX_SPEC,
    "player_id": INT,
    "shirt_number": S,
    "player": S,
    "starter": B,
    "captain": B,
    "seconds_played": INT,
    "min_formatted": S,
    "pts": INT,
    **{k: INT for k in ("p1m", "p1a")},
    "p1p": F,
    **{k: INT for k in ("p2m", "p2a")},
    "p2p": F,
    **{k: INT for k in ("p3m", "p3a")},
    "p3p": F,
    "fgm": INT,
    "fga": INT,
    "fgp": F,
    **{k: INT for k in ("ro", "rd", "rt", "reb", "assist", "st", "to", "bs", "tc", "mt", "pf", "rf", "pllss", "val")},
}


def _stat_cells(d: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {k: _i(d.get(k)) for k in (*_SHOOT_INT, *_COUNT_INT)}
    out.update({k: _f(d.get(k)) for k in _SHOOT_PCT})
    out["seconds_played"] = _i(d.get("min"))
    out["min_formatted"] = _s(d.get("minFormatted"))
    return out


def parse_feb_player_box(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Per-player box score of one FEB game (``BOXSCORE.TEAM[].PLAYER[]``).

    Args:
        raw: the envelope ``feb_player_box(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per player and team. Column names follow the provider: ``p1`` / ``p2`` / ``p3`` are free
        throws, two- and three-point field goals (``m`` made, ``a`` attempted, ``p`` percentage 0-100),
        ``ro`` / ``rd`` / ``rt`` offensive, defensive and total rebounds, ``pllss`` plus/minus, ``val``
        the FEB efficiency rating. ``seconds_played`` is the provider's ``min`` (seconds, not minutes).
        ``player_id`` is the FEB person id (stable across seasons); ``shirt_number`` stays a string.
        ``captain`` is null on 2020-21 games. A body without a box score gives a zero-row frame with the
        documented columns.

    Raises:
        None: does not raise; an unusable body (not a dict, or without the block this parser reads) gives a zero-row
            frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.feb import feb_player_box

            players = feb_player_box(game_id=2535725)
    """
    if _env(raw, "BOXSCORE") is None:
        return _frame([], _PLAYER_SPEC, columns, return_as_pandas)
    gid = _game_id(raw)
    rows: List[Dict[str, Any]] = []
    for side, team in _teams(raw, "BOXSCORE"):
        for p in team.get("PLAYER") or []:
            if isinstance(p, dict):
                rows.append(
                    {
                        **_team_ctx(team, side, gid),
                        "player_id": _i(p.get("id")),
                        "shirt_number": _s(p.get("no")),
                        "player": _s(p.get("name")),
                        "starter": _flag(p.get("inn")),
                        "captain": _flag(p.get("captain")),
                        **_stat_cells(p),
                        "reb": _i(p.get("reb")),
                    }
                )
    return _frame(rows, _PLAYER_SPEC, columns, return_as_pandas)


_TEAM_SPEC: Spec = {
    **_CTX_SPEC,
    "team_code": S,
    "club_code": S,
    "seconds_played": INT,
    "min_formatted": S,
    "pts": INT,
    "p1m": INT,
    "p1a": INT,
    "p1p": F,
    "p2m": INT,
    "p2a": INT,
    "p2p": F,
    "p3m": INT,
    "p3a": INT,
    "p3p": F,
    "fgm": INT,
    "fga": INT,
    "fgp": F,
    **{k: INT for k in ("ro", "rd", "rt", "assist", "st", "to", "bs", "tc", "mt", "pf", "rf", "fouls", "val")},
    **{k: INT for k in ("fastb", "ptsto", "secpts", "bench", "paint")},
}


def parse_feb_team_stats(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Team totals of both teams in one FEB game (``TEAMSTATS.TEAM[]``, or ``BOXSCORE.TEAM[].TOTAL``).

    Args:
        raw: the envelope ``feb_team_stats(..., return_parsed=False)`` returns. A box-score or
            play-by-play envelope also works: the totals come from ``TEAMSTATS`` when filled, else from
            the box score's per-team ``TOTAL`` block.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per team (home first). Shooting columns are as in :func:`parse_feb_player_box`.
        ``fastb``, ``ptsto``, ``secpts``, ``bench`` and ``paint`` (fast-break, points-off-turnover,
        second-chance, bench and paint points) are null on every sampled game: the federation's stat
        crews do not fill them. ``seconds_played`` and ``val`` are null on ``TEAMSTATS`` rows (filled on
        box-score totals). A body with neither block gives a zero-row frame with the documented columns.

    Raises:
        None: does not raise; an unusable body (not a dict, or without the block this parser reads) gives a zero-row
            frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.feb import feb_team_stats

            totals = feb_team_stats(game_id=2535725)
    """
    if not isinstance(raw, dict):
        return _frame([], _TEAM_SPEC, columns, return_as_pandas)
    gid = _game_id(raw)
    teams = _teams(raw, "TEAMSTATS")
    from_box = not teams
    if from_box:
        teams = [(side, {**t, **(t.get("TOTAL") or {})}) for side, t in _teams(raw, "BOXSCORE")]
    rows: List[Dict[str, Any]] = []
    for side, t in teams:
        rows.append(
            {
                **_team_ctx(t, side, gid),
                "team_code": _s(t.get("teamCode")),
                "club_code": _s(t.get("clubCode")),
                **_stat_cells(t),
                "fouls": _i(t.get("fouls")),
                **{k: _i(t.get(k)) for k in ("fastb", "ptsto", "secpts", "bench", "paint")},
            }
        )
    return _frame(rows, _TEAM_SPEC, columns, return_as_pandas)


# ---- play by play --------------------------------------------------------------------------------
_PBP_SPEC: Spec = {
    "game_id": INT,
    "play_num": INT,
    "period": INT,
    "clock": S,
    "clock_seconds": INT,
    "team_side": S,
    "team_id": INT,
    "player_id": INT,
    "license_id": INT,
    "action": S,
    "text": S,
    "score_home": INT,
    "score_away": INT,
    "x": F,
    "y": F,
    **{f"log_param{n}": S for n in range(1, 11)},
    "log_time": _DT,
    "log_time_utc": _DT,
}
_PBP_SIDE = {"1": "home", "2": "away"}


def _position(v: Any) -> Tuple[Optional[float], Optional[float]]:
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\|(\d+(?:\.\d+)?)\s*", v) if isinstance(v, str) else None
    return (float(m.group(1)), float(m.group(2))) if m else (None, None)


def parse_feb_pbp(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Play-by-play of one FEB game (``KeyFacts`` ``PLAYBYPLAY.LINES``), oldest play first.

    Args:
        raw: the envelope ``feb_pbp(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per play, sorted by ``play_num`` (the API lists them newest first). ``text`` is Spanish
        free text. ``action`` is one of ``shoot``, ``fthrow``, ``rebound``, ``assist``, ``recovery``,
        ``lose``, ``blockshot``, ``foul``, ``subst``, ``timeout``, ``period``. ``clock`` /
        ``clock_seconds`` count down the time REMAINING in the period. ``score_home`` / ``score_away``
        are filled on scoring plays only. ``x`` / ``y`` are percent of a horizontal full-court image
        (origin top-left) on shoot lines; teams change ends at half-time. ``log_param1`` ...
        ``log_param10`` are the provider's per-action parameters, only partly decoded (kept as strings).
        2020-21 games are text only: ``action``, the ids, the parameters, the position and the log times
        are null there. ``player_id`` is the FEB person id; ``license_id`` is the per-season licence.
        A body without play-by-play gives a zero-row frame with the documented columns.

    Raises:
        None: does not raise; an unusable body (not a dict, or without the block this parser reads) gives a zero-row
            frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.feb import feb_pbp

            plays = feb_pbp(game_id=2535725)
    """
    pbp = raw.get("PLAYBYPLAY") if isinstance(raw, dict) else None
    lines = pbp.get("LINES") if isinstance(pbp, dict) else None
    rows: List[Dict[str, Any]] = []
    gid = _game_id(raw) if isinstance(raw, dict) else None
    for ln in lines if isinstance(lines, list) else []:
        if not isinstance(ln, dict):
            continue
        x, y = _position(ln.get("Position"))
        rows.append(
            {
                "game_id": gid,
                "play_num": _i(ln.get("num")),
                "period": _i(ln.get("quarter")),
                "clock": _s(ln.get("time")),
                "clock_seconds": _clock_seconds(ln.get("time")),
                "team_side": _PBP_SIDE.get(str(ln.get("team"))),
                "team_id": _i(ln.get("idTeam")),
                "player_id": _i(ln.get("idPlayer")),
                "license_id": _i(ln.get("idLicense")),
                "action": _s(ln.get("action")),
                "text": _s(ln.get("text")),
                "score_home": _i(ln.get("scoreA")),
                "score_away": _i(ln.get("scoreB")),
                "x": x,
                "y": y,
                **{f"log_param{n}": _s(ln.get(f"logParam{n}")) for n in range(1, 11)},
                "log_time": _log_dt(ln.get("LogTime")),
                "log_time_utc": _log_dt(ln.get("LogTimeUtc")),
            }
        )
    rows.sort(key=lambda r: (r["play_num"] is None, r["play_num"] or 0))
    return _frame(rows, _PBP_SPEC, columns, return_as_pandas)


# ---- shots ---------------------------------------------------------------------------------------
_SHOT_SPEC: Spec = {
    "game_id": INT,
    "period": INT,
    "clock": S,
    "clock_seconds": INT,
    "team_side": S,
    "team_id": INT,
    "shirt_number": S,
    "player_id": INT,
    "made": B,
    "x": F,
    "y": F,
}
_SHOT_SIDE = {"0": "home", "1": "away"}


def parse_feb_shots(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Every field-goal attempt of one FEB game with court position (``SHOTCHART.SHOTS``).

    Args:
        raw: the envelope ``feb_shots(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per attempt (free throws are not in the feed). ``x`` / ``y`` are percent (0-100) of a
        horizontal full-court image, origin top-left, x along the length; teams change ends at
        half-time, so normalise to one basket before plotting. Metres are an inference (x / 100 x 28,
        y / 100 x 15) and unverified. ``clock`` is the time remaining in the period. The feed names the
        shooter by SHIRT NUMBER; ``player_id`` is resolved through the game's roster (the same
        side and shirt number in ``SHOTCHART.TEAM[].PLAYER[]``) and is null when that pair is not
        unique. A body without shots gives a zero-row frame with the documented columns.

    Raises:
        None: does not raise; an unusable body (not a dict, or without the block this parser reads) gives a zero-row
            frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.feb import feb_shots

            shots = feb_shots(game_id=2535725)
    """
    block = raw.get("SHOTCHART") if isinstance(raw, dict) else None
    shots = block.get("SHOTS") if isinstance(block, dict) else None
    if not isinstance(shots, list) or not isinstance(raw, dict):
        return _frame([], _SHOT_SPEC, columns, return_as_pandas)
    gid = _game_id(raw)
    teams = dict(_teams(raw, "SHOTCHART"))
    roster: Dict[Tuple[str, str], Optional[int]] = {}
    for team_side, team in teams.items():
        for p in team.get("PLAYER") or []:
            no = _s(p.get("no")) if isinstance(p, dict) else None
            if no is not None:  # a pair seen twice cannot name one player
                roster[(team_side, no)] = None if (team_side, no) in roster else _i(p.get("id"))
    rows: List[Dict[str, Any]] = []
    for sh in shots:
        if not isinstance(sh, dict):
            continue
        side = _SHOT_SIDE.get(str(sh.get("team")))
        no = _s(sh.get("player"))
        rows.append(
            {
                "game_id": gid,
                "period": _i(sh.get("quarter")),
                "clock": _s(sh.get("t")),
                "clock_seconds": _clock_seconds(sh.get("t")),
                "team_side": side,
                "team_id": _i((teams.get(side) or {}).get("id")) if side else None,
                "shirt_number": no,
                "player_id": roster.get((side, no)) if side and no else None,
                "made": _flag(sh.get("m")),
                "x": _f(sh.get("x")),
                "y": _f(sh.get("y")),
            }
        )
    return _frame(rows, _SHOT_SPEC, columns, return_as_pandas)


# ---- leaders -------------------------------------------------------------------------------------
_LEADER_SPEC: Spec = {
    **_CTX_SPEC,
    "category": S,
    "rank": INT,
    "player_id": INT,
    "shirt_number": S,
    "player": S,
    "starter": B,
    "captain": B,
    "value": INT,
}
_LEADER_CAT = {
    "Puntos": ("points", "pts"),
    "Rebotes": ("rebounds", "reb"),
    "Asistencias": ("assists", "assist"),
    "Faltas": ("fouls", "pf"),
}


def parse_feb_leaders(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Each team's top players of one FEB game by points, rebounds, assists and fouls (``RANKING``).

    Args:
        raw: the envelope ``feb_leaders(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per team, category and ranked player (``rank`` 1 = the leader, in the order the feed
        lists them). ``category`` is ``points``, ``rebounds``, ``assists`` or ``fouls`` (the feed's
        Spanish keys, translated); ``value`` is that statistic's count. A body without a ranking gives a
        zero-row frame with the documented columns.

    Raises:
        None: does not raise; an unusable body (not a dict, or without the block this parser reads) gives a zero-row
            frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.feb import feb_leaders

            leaders = feb_leaders(game_id=2535725)
    """
    block = raw.get("RANKING") if isinstance(raw, dict) else None
    teams = block.get("TEAMS") if isinstance(block, dict) else None
    rows: List[Dict[str, Any]] = []
    gid = _game_id(raw) if isinstance(raw, dict) else None
    for i, team in enumerate(teams[:2] if isinstance(teams, list) else []):
        if not isinstance(team, dict):
            continue
        for key, (cat, field) in _LEADER_CAT.items():
            for rank, p in enumerate(team.get(key) or [], start=1):
                if isinstance(p, dict):
                    rows.append(
                        {
                            **_team_ctx(team, _SIDES[i], gid),
                            "category": cat,
                            "rank": rank,
                            "player_id": _i(p.get("id")),
                            "shirt_number": _s(p.get("no")),
                            "player": _s(p.get("name")),
                            "starter": _flag(p.get("inn")),
                            "captain": _flag(p.get("captain")),
                            "value": _i(p.get(field)),
                        }
                    )
    return _frame(rows, _LEADER_SPEC, columns, return_as_pandas)


# ---- overview ------------------------------------------------------------------------------------
_OVERVIEW_SPEC: Spec = {
    "comp_id": INT,
    "comp_code": INT,
    "competition": S,
    "game_id": INT,
    "round_id": INT,
    "stage": S,
    "venue": S,
    "start_time": _DT,
    "start_time_utc": _DT,
    "period": INT,
    "clock": S,
    "status": INT,
    "home_team_id": INT,
    "home_team": S,
    "home_team_code": S,
    "home_club_code": S,
    "home_abbreviation": S,
    "home_score": INT,
    "away_team_id": INT,
    "away_team": S,
    "away_team_code": S,
    "away_club_code": S,
    "away_abbreviation": S,
    "away_score": INT,
    "period_scores": S,
}


def parse_feb_overview(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Today's (or the latest) games of every live FEB competition (``/overview``).

    Args:
        raw: the envelope ``feb_overview(return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per game across the competitions with games in the window (the latest or current game
        day). ``start_time`` is Spain local time (no zone), ``start_time_utc`` UTC. ``status``: 3 =
        finished, 1 = scheduled (live codes unverified). Ids are integers here, as everywhere in these
        frames. ``period_scores`` is ``"20-25;14-27;..."`` (home-away points per period). An empty window
        gives a zero-row frame with the documented columns.

    Raises:
        None: does not raise; an unusable body (not a dict, or without the block this parser reads) gives a zero-row
            frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.feb import feb_overview

            today = feb_overview()
    """
    block = raw.get("OVERVIEW") if isinstance(raw, dict) else None
    comps = block.get("COMPETITIONS") if isinstance(block, dict) else None
    rows: List[Dict[str, Any]] = []
    for c in comps if isinstance(comps, list) else []:
        for g in (c.get("GAMES") or []) if isinstance(c, dict) else []:
            if not isinstance(g, dict):
                continue
            qs = [q for q in g.get("Quarters") or [] if isinstance(q, dict)]
            rows.append(
                {
                    "comp_id": _i(g.get("CompID")),
                    "comp_code": _i(g.get("CompCode")),
                    "competition": _s(g.get("Competition")),
                    "game_id": _i(g.get("ID")),
                    "round_id": _i(g.get("RoundID")),
                    "stage": _s(g.get("Stage")),
                    "venue": _s(g.get("Place")),
                    "start_time": _local_dt(g.get("StartTime")),
                    "start_time_utc": _utc_dt(g.get("StartTimeUTC")),
                    "period": _i(g.get("Quarter")),
                    "clock": _s(g.get("Time")),
                    "status": _i(g.get("Status")),
                    "home_team_id": _i(g.get("TeamAID")),
                    "home_team": _s(g.get("TeamA")),
                    "home_team_code": _s(g.get("TeamACode")),
                    "home_club_code": _s(g.get("TeamAClubCode")),
                    "home_abbreviation": _s(g.get("TeamASlug")),
                    "home_score": _i(g.get("ScoreA")),
                    "away_team_id": _i(g.get("TeamBID")),
                    "away_team": _s(g.get("TeamB")),
                    "away_team_code": _s(g.get("TeamBCode")),
                    "away_club_code": _s(g.get("TeamBClubCode")),
                    "away_abbreviation": _s(g.get("TeamBSlug")),
                    "away_score": _i(g.get("ScoreB")),
                    "period_scores": ";".join(f"{q.get('scoreA')}-{q.get('scoreB')}" for q in qs) or None,
                }
            )
    return _frame(rows, _OVERVIEW_SPEC, columns, return_as_pandas)


# ---- schedule pages ------------------------------------------------------------------------------
_PAGE_SPEC: Spec = {"group": INT, "season": INT, "competition": S}
_GAME_SPEC: Spec = {
    "game_id": INT,
    "home_team_id": INT,
    "home_team": S,
    "away_team_id": INT,
    "away_team": S,
    "result_text": S,
    "home_score": INT,
    "away_score": INT,
    "status": S,
}
_SCHEDULE_SPEC: Spec = {
    **_PAGE_SPEC,
    "round": INT,
    "round_label": S,
    "round_date": D,
    **_GAME_SPEC,
    "weekday": S,
    "date": D,
    "time": S,
}


def _record(raw: Any, key: str) -> Optional[Dict[str, Any]]:
    """The page record when ``raw`` is a dict holding a non-empty list under ``key``."""
    return raw if isinstance(raw, dict) and isinstance(raw.get(key), list) and raw[key] else None


def _page_ctx(rec: Dict[str, Any]) -> Dict[str, Any]:
    start = _i(rec.get("season_start"))
    return {
        "group": _i(rec.get("group")),
        "season": start + 1 if start is not None else None,
        "competition": _s(rec.get("competition")),
    }


def _game_cells(g: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "game_id": _i(g.get("game_id")),
        "home_team_id": _i(g.get("home_team_id")),
        "home_team": _s(g.get("home_team")),
        "away_team_id": _i(g.get("away_team_id")),
        "away_team": _s(g.get("away_team")),
        "result_text": _s(g.get("result_text")),
        "home_score": _i(g.get("home_score")),
        "away_score": _i(g.get("away_score")),
        "status": _s(g.get("status")),
    }


def parse_feb_schedule(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Full schedule of one group (phase) of a FEB competition season, from ``calendario.aspx``.

    Args:
        raw: the record ``feb_schedule(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per game, every round. ``season`` is the END year (2027 = 2026-27). Played games carry
        no date or time on this page: use ``round_date`` (the round's date) or ``feb_game_info``'s
        ``start_time``. Scheduled games carry an English ``weekday``, ISO ``date`` and ``time`` (Spain
        local). The page renders only the FIRST phase of the group's select (Segunda FEB shows group A);
        other phases and the playoffs need an ASP.NET postback and are not modelled. ``team_id`` values
        are team-season registrations. A record without games gives a zero-row frame with the documented
        columns.

    Raises:
        None: does not raise; an unusable body (not a dict, or without the block this parser reads) gives a zero-row
            frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.feb import feb_schedule

            games = feb_schedule(group=1, season=2027)
    """
    rec = _record(raw, "games")
    rows: List[Dict[str, Any]] = []
    if rec is not None:
        ctx = _page_ctx(rec)
        for g in rec["games"]:
            if isinstance(g, dict):
                rows.append(
                    {
                        **ctx,
                        "round": _i(g.get("round")),
                        "round_label": _s(g.get("round_label")),
                        "round_date": _date(g.get("round_date")),
                        **_game_cells(g),
                        "weekday": _s(g.get("weekday")),
                        "date": _date(g.get("date")),
                        "time": _s(g.get("time")),
                    }
                )
    return _frame(rows, _SCHEDULE_SPEC, columns, return_as_pandas)


_RESULTS_SPEC: Spec = {
    **_PAGE_SPEC,
    "comp_id": INT,
    "section": S,
    "round_title": S,
    **_GAME_SPEC,
    "date": D,
    "time": S,
}


def parse_feb_results(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Latest round's results and the next round's fixtures of a FEB competition season (``resultados.aspx``).

    Args:
        raw: the record ``feb_results(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per game. ``section`` is ``results`` (the LATEST round of the shown phase; for a
        finished season the last playoff round) or ``next_round`` (empty once the season is over).
        ``round_title`` is the page's table title (``Resultados Jornada 3(09/10/2026)``). ``comp_id``
        ties the site group to the API's competition id (group 1 -> 115, 2 -> 118, 4 -> 210). Scores are
        null on ``next_round`` rows. A record without results gives a zero-row frame with the documented
        columns.

    Raises:
        None: does not raise; an unusable body (not a dict, or without the block this parser reads) gives a zero-row
            frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.feb import feb_results

            latest = feb_results(group=1, season=2027)
    """
    rec = _record(raw, "results")
    rows: List[Dict[str, Any]] = []
    if rec is not None:
        ctx = {**_page_ctx(rec), "comp_id": _i(rec.get("comp_id"))}
        for section, title in (("results", "results_title"), ("next_round", "next_round_title")):
            for g in rec.get(section) or []:
                if isinstance(g, dict):
                    rows.append(
                        {
                            **ctx,
                            "section": section,
                            "round_title": _s(rec.get(title)),
                            **_game_cells(g),
                            "date": _date(g.get("date")),
                            "time": _s(g.get("time")),
                        }
                    )
    return _frame(rows, _RESULTS_SPEC, columns, return_as_pandas)


_STANDINGS_SPEC: Spec = {
    **_PAGE_SPEC,
    "comp_id": INT,
    "table": S,
    "position": INT,
    "team_id": INT,
    "team": S,
    "played": INT,
    "won": INT,
    "lost": INT,
    "points_for": INT,
    "points_against": INT,
    "point_diff": INT,
    "win_pct": F,
    "streak": INT,
    "streak_text": S,
}


def parse_feb_standings(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """League table of one FEB competition season phase (``resultados.aspx``).

    Args:
        raw: the record ``feb_standings(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per team. ``table`` is ``live`` (the in-season league table, with games, points and
        streak) or ``final`` (the final classification once the season is over: only ``position`` and
        the team, every other statistic is null); the page shows one of the two. ``win_pct`` is 0-100
        (the page's ``100,0%``), ``streak`` a signed integer (+3 = three wins in a row). Only the first
        phase of the group is shown. A record without a table gives a zero-row frame with the
        documented columns.

    Raises:
        None: does not raise; an unusable body (not a dict, or without the block this parser reads) gives a zero-row
            frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.feb import feb_standings

            table = feb_standings(group=1, season=2027)
    """
    rec = raw if isinstance(raw, dict) else None
    rows: List[Dict[str, Any]] = []
    if rec is not None:
        ctx = {**_page_ctx(rec), "comp_id": _i(rec.get("comp_id"))}
        for table, key in (("live", "standings"), ("final", "final_standings")):
            for r in rec.get(key) or []:
                if isinstance(r, dict):
                    rows.append(
                        {
                            **ctx,
                            "table": table,
                            "position": _i(r.get("position")),
                            "team_id": _i(r.get("team_id")),
                            "team": _s(r.get("team")),
                            "played": _i(r.get("played")),
                            "won": _i(r.get("won")),
                            "lost": _i(r.get("lost")),
                            "points_for": _i(r.get("points_for")),
                            "points_against": _i(r.get("points_against")),
                            "point_diff": _i(r.get("point_diff")),
                            "win_pct": _f(r.get("win_pct")),
                            "streak": _i(r.get("streak")),
                            "streak_text": _s(r.get("streak_text")),
                        }
                    )
    return _frame(rows, _STANDINGS_SPEC, columns, return_as_pandas)


#: Each parser's column dtypes, for the generator and the tests.
SPECS: Dict[str, Spec] = {
    "game_info": _INFO_SPEC,
    "period_scores": _PERIOD_SPEC,
    "player_box": _PLAYER_SPEC,
    "team_stats": _TEAM_SPEC,
    "pbp": _PBP_SPEC,
    "shots": _SHOT_SPEC,
    "leaders": _LEADER_SPEC,
    "overview": _OVERVIEW_SPEC,
    "schedule": _SCHEDULE_SPEC,
    "results": _RESULTS_SPEC,
    "standings": _STANDINGS_SPEC,
}
