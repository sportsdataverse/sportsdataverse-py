"""Parsers for the generated ``aba`` wrappers (ABA League and ABA League 2, the Adriatic leagues).

The data comes from the leagues' own PHP sites (``www.aba-liga.com`` and ``druga.aba-liga.com``; one
database behind both, ``lea`` picks the league). Two shapes feed this family:

* **HTML pages** (match, calendar, standings, player), parsed here with the stdlib :mod:`html.parser` into
  the records the refs captures bank (``sdv-internal-refs`` ``aba-liga/tools/capture.py``): a minimal DOM,
  iterative walks (a deep page never raises ``RecursionError``), and header / width / scope checks that
  refuse layout drift or another game, season or league instead of guessing. :func:`record_for` is the
  one entry point the runtime uses; a refusal is :class:`_NotParsed`.
* **JSON served as text/html** (shot chart and the two minute-by-minute score helpers), wrapped by the runtime
  as ``{"rows": [...], "_request": {...}}`` because the bodies echo nothing about the game.

Each public ``parse_aba_*`` takes the dict a wrapper returns with ``return_parsed=False`` (the page record, or
the wrapped helper rows, with the request stamped under ``_request`` by ``aba_runtime``) and follows the
package parser contract: a polars frame by default, pandas with ``return_as_pandas=True``, snake_case
columns, and a zero-row frame with the documented columns and dtypes, never an exception, for an empty or
unrecognised input.

**Season** is the END year (2026 = 2025-26). The site's ``sez`` is the START year minus 2000 (25 = 2025-26),
so ``season = 2001 + sez``; ``aba_runtime`` maps it and the page's own echo is checked against the request.

**League.** ``league_id`` is the site's ``lea`` (1 = ABA League, 2 = ABA League 2) and ``league`` its label
(``aba`` / ``aba2``, the wrapper argument).

**Id key spaces.** Every id is ``Int64``.

* ``game_no``: the game number, **per season AND league**. It restarts at 1 for every (``season``,
  ``league_id``), so a game's key is (``game_no``, ``season``, ``league_id``); every match-page frame carries
  all three. Unplayed playoff games leave gaps.
* ``team_id``: the club, **global**: one id per club in every season and in both leagues (Partizan = 22 in
  2008-09 and 2025-26). The calendar has no team ids (names and three-letter codes only); the box score,
  play-by-play, standings and player frames do. Club names are the club's CURRENT name on every season: join on
  ``team_id``, never on the name.
* ``player_id``: the person, **global** across seasons and both leagues (Carlik Jones = 4807). An integer in
  every frame; the shot chart sends it as the STRING ``"4807"`` and the parser casts it at the boundary
  (``"0"`` = shooter not recorded = null). Join the shot chart to the box score and play-by-play on
  ``player_id``, never on a name: the box prints ``Jones C.``, the play-by-play ``JONES C`` and the shot chart
  ``Carlik Jones``.

**Dtypes** are pinned by column name, never sampled. Percentages are ``Float64`` (0-100, one decimal), minutes
are ``mm:ss`` text, shot ``x`` / ``y`` are ``Float64`` (the site sends x as a string in one half of a game and a
number in the other).
"""

from __future__ import annotations

import re
from datetime import date
from html.parser import HTMLParser
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union

import pandas as pd
import polars as pl

__all__ = [
    "parse_aba_box_players",
    "parse_aba_box_teams",
    "parse_aba_calendar",
    "parse_aba_game",
    "parse_aba_pbp",
    "parse_aba_periods",
    "parse_aba_player",
    "parse_aba_player_games",
    "parse_aba_score_difference",
    "parse_aba_score_flow",
    "parse_aba_shots",
    "parse_aba_standings",
]

Frame = Union[pl.DataFrame, pd.DataFrame]

#: Where the runtime stamps the request (``aba_runtime.REQUEST_KEY``).
_REQUEST = "_request"

INT, F, S, B, D = pl.Int64, pl.Float64, pl.Utf8, pl.Boolean, pl.Date

#: The site's league codes this family serves.
LEAGUES = {1: "aba", 2: "aba2"}


class _NotParsed(Exception):
    """The page is not the data asked for: a missing selector or table, header drift, a row of the wrong
    width, another scope (game, season, league) or an unfinished game.

    ``empty`` marks a page that is the right one but holds nothing (an unknown season); the runtime raises it as
    ``NoDataError``. Every other refusal is an ``AssetFetchError``.
    """

    def __init__(self, msg: str, *, empty: bool = False) -> None:
        super().__init__(msg)
        self.empty = empty


# --------------------------------------------------------------------------------------- DOM
_VOID = frozenset(
    {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}
)


class _Node:
    __slots__ = ("tag", "attrs", "children", "parent")

    def __init__(self, tag: str, attrs: Any, parent: Optional["_Node"]) -> None:
        self.tag, self.attrs, self.parent = tag, dict(attrs), parent
        self.children: List[Any] = []

    def iter(self) -> Iterator["_Node"]:
        """Descendants in document order (explicit stack: a deep page cannot recurse)."""
        stack = list(reversed(self.children))
        while stack:
            node = stack.pop()
            if isinstance(node, _Node):
                yield node
                stack.extend(reversed(node.children))

    def text(self, br: str = " ") -> str:
        parts: List[str] = []
        stack = list(reversed(self.children))
        while stack:
            node = stack.pop()
            if isinstance(node, _Node):
                if node.tag == "br":
                    parts.append(br)
                stack.extend(reversed(node.children))
            else:
                parts.append(node)
        out = "".join(parts)
        if br == " ":
            return re.sub(r"\s+", " ", out).strip()
        return "\n".join(re.sub(r"\s+", " ", seg).strip() for seg in out.split(br))

    def classes(self) -> set:
        return set((self.attrs.get("class") or "").split())

    def find(self, tag: Optional[str] = None, cls: Optional[str] = None, id: Optional[str] = None) -> Optional["_Node"]:
        return next(self.findall(tag, cls, id), None)

    def findall(
        self, tag: Optional[str] = None, cls: Optional[str] = None, id: Optional[str] = None
    ) -> Iterator["_Node"]:
        for n in self.iter():
            if tag and n.tag != tag:
                continue
            if cls and cls not in n.classes():
                continue
            if id and n.attrs.get("id") != id:
                continue
            yield n

    def kids(self, *tags: str) -> List["_Node"]:
        return [c for c in self.children if isinstance(c, _Node) and (not tags or c.tag in tags)]


class _Builder(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.root = self.cur = _Node("#root", {}, None)

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


# ------------------------------------------------------------------------------- cell readers
def _need(cond: Any, msg: str) -> None:
    if not cond:
        raise _NotParsed(msg)


def _same(a: Any, b: Any) -> bool:
    """Type-strict equality (``1 == True`` and ``"1" == 1`` are both refused)."""
    return type(a) is type(b) and a == b


def _int_text(s: Optional[str]) -> Optional[int]:
    s = (s or "").strip()
    return int(s) if re.fullmatch(r"[+-]?\d+", s) else None


def _num_text(s: Optional[str]) -> Optional[float]:
    """``'57.1'`` -> 57.1, ``'50'`` -> 50.0 (percentages are always numbers), ``''`` -> None."""
    s = (s or "").strip()
    return float(s) if re.fullmatch(r"[+-]?\d+(?:\.\d+)?", s) else None


def _path_ids(href: Optional[str], kind: str) -> Optional[Tuple[int, int, int]]:
    """``'/team/22/25/1/0/slug/'`` -> (22, 25, 1); ``'/player/4807/25/1/x/'`` -> (4807, 25, 1); else None."""
    m = re.search(rf"/{kind}/(\d+)/(\d+)/(\d+)(?:/|$)", href or "")
    return (int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else None


def _iso(dmy: Optional[str]) -> Optional[str]:
    m = re.fullmatch(r"(\d{2})\.(\d{2})\.(\d{4})", (dmy or "").strip())
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1))).isoformat()
    except ValueError:
        return None


_WHEN = re.compile(
    r"^(?:[A-Za-z]+, )?(\d{2}\.\d{2}\.\d{4})"
    r"(?: - (?:[A-Za-z]+, )?(\d{2}\.\d{2}\.\d{4}))?"
    r"(?: (\d{2}:\d{2}) (CET|Local time))?"
    r"(?: \((\d{2}:\d{2}) (.+?) local time\))?$"
)


def _when(text: str) -> Dict[str, Optional[str]]:
    """``'Monday, 06.10.2025 17:00 CET (19:00 Dubai local time)'`` -> date / time fields. ``'TBA'`` -> all null."""
    text = re.sub(r"\s+", " ", text or "").strip()
    out: Dict[str, Optional[str]] = {
        "when": text,
        "date": None,
        "date_to": None,
        "time": None,
        "time_zone": None,
        "local_time": None,
        "local_place": None,
    }
    if text == "TBA":
        return out
    m = _WHEN.match(text)
    _need(m, f"unreadable date/time {text!r}")
    assert m is not None
    out.update(
        date=_iso(m.group(1)),
        date_to=_iso(m.group(2)) if m.group(2) else None,
        time=m.group(3),
        time_zone={"CET": "CET", "Local time": "local"}.get(m.group(4)),
        local_time=m.group(5),
        local_place=m.group(6),
    )
    _need(out["date"], f"bad date in {text!r}")
    return out


def _cells(tr: _Node) -> List[_Node]:
    return tr.kids("td", "th")


def _season_select(root: _Node) -> Tuple[Optional[str], List[Dict[str, Any]]]:
    """The ``select#season`` options: (selected value, ``[{sez, lea, label}]``)."""
    sel = root.find("select", id="season")
    _need(sel is not None, "no season select")
    assert sel is not None
    chosen, opts = None, []
    for o in sel.findall("option"):
        v = o.attrs.get("value") or ""
        m = re.fullmatch(r"(\d+)/(\d+)/", v)
        if m:
            opts.append({"sez": int(m.group(1)), "lea": int(m.group(2)), "label": o.text()})
        if "selected" in o.attrs:
            chosen = v
    return chosen, opts


# ------------------------------------------------------------------------------------ box score
# The two header rows (group row + label row) read together as "group label" tokens. The AdvStats trio (Points
# from Paint, 2ndCh, FstBr) is absent before about 2014 (absent on the 2008-09 sample), so the box has two layouts.
_BOX = [  # (group label, key, kind)  kind: i = int, f = number (percent), t = mm:ss text
    ("Min", "minutes", "t"),
    ("overall Pts", "points", "i"),
    ("overall %", "fg_pct", "f"),
    ("FG2 M", "fg2_made", "i"),
    ("FG2 A", "fg2_att", "i"),
    ("FG2 %", "fg2_pct", "f"),
    ("FG3 M", "fg3_made", "i"),
    ("FG3 A", "fg3_att", "i"),
    ("FG3 %", "fg3_pct", "f"),
    ("FT M", "ft_made", "i"),
    ("FT A", "ft_att", "i"),
    ("FT %", "ft_pct", "f"),
    ("Rebs D", "reb_def", "i"),
    ("Rebs O", "reb_off", "i"),
    ("Rebs T", "reb_tot", "i"),
    ("Ass", "assists", "i"),
    ("St", "steals", "i"),
    ("To", "turnovers", "i"),
    ("Blck Fv", "blocks", "i"),
    ("Blck Ag", "blocks_against", "i"),
    ("Foul Cm", "fouls", "i"),
    ("Foul Rv", "fouls_drawn", "i"),
    ("Points from Paint", "pts_paint", "i"),
    ("Points from 2ndCh", "pts_second_chance", "i"),
    ("Points from FstBr", "pts_fast_break", "i"),
    ("+/-", "plus_minus", "i"),
    ("Val", "valuation", "i"),
]
_ADV = ("Points from Paint", "Points from 2ndCh", "Points from FstBr")
_BOX_LAYOUTS = (tuple(c[0] for c in _BOX), tuple(c[0] for c in _BOX if c[0] not in _ADV))
_KIND = {"i": INT, "f": F, "t": S}
#: stat column -> dtype, in box order (shared by the box score, team totals and the player game log).
_STAT_SPEC: Dict[str, pl.DataType] = {key: _KIND[kind] for _, key, kind in _BOX}


def _box_header(table: _Node, what: str) -> List[Tuple[str, str, str]]:
    """The stat columns after the two lead cells, as ``_BOX`` entries; layout drift is refused."""
    thead = table.find("thead")
    _need(thead is not None, f"{what}: no thead")
    assert thead is not None
    rows = thead.kids("tr")
    _need(len(rows) == 2, f"{what}: {len(rows)} header rows")
    groups: List[str] = []
    for c in _cells(rows[0]):
        groups += [c.text()] * int(c.attrs.get("colspan") or 1)
    labels: List[str] = []
    for c in _cells(rows[1]):
        labels += [c.text()] * int(c.attrs.get("colspan") or 1)
    _need(len(groups) == len(labels), f"{what}: header widths {len(groups)} != {len(labels)}")
    tokens = tuple(f"{g} {lab}".strip() for g, lab in zip(groups[2:], labels[2:]))
    _need(tokens in _BOX_LAYOUTS, f"{what}: header drift {tokens}")
    return [c for c in _BOX if c[0] in tokens]


def _stat(cell: _Node, kind: str, what: str) -> Any:
    s = cell.text()
    if kind == "t":
        _need(re.fullmatch(r"\d{1,3}:\d{2}", s), f"{what}: minutes {s!r}")
        return s
    v = _int_text(s) if kind == "i" else _num_text(s)
    _need(v is not None, f"{what}: not a number {s!r}")
    return v


def _box_players(table: _Node, side: str, team_id: int, want: Dict[str, int]) -> List[Dict[str, Any]]:
    what = f"{side} box"
    cols = _box_header(table, what)
    tbody = table.find("tbody")
    _need(tbody is not None, f"{what}: no tbody")
    assert tbody is not None
    out = []
    for tr in tbody.kids("tr"):
        cells = _cells(tr)
        # a did-not-play row is the minutes cell '00:00' plus one empty cell spanning the other stat columns
        dnp = len(cells) == 4 and cells[3].attrs.get("colspan") == str(len(cols) - 1) and not cells[3].text()
        _need(dnp or len(cells) == 2 + len(cols), f"{what}: row width {len(cells)} != {2 + len(cols)}")
        a = cells[1].find("a")
        _need(a is not None, f"{what}: no player link")
        assert a is not None
        ids = _path_ids(a.attrs.get("href"), "player")
        _need(
            ids is not None and ids[1:] == (want["sez"], want["lea"]),
            f"{what}: player link {a.attrs.get('href')!r} outside sez/lea",
        )
        assert ids is not None
        row: Dict[str, Any] = {
            "side": side,
            "team_id": team_id,
            "jersey": _int_text(cells[0].text()),
            "player_id": ids[0],
            "player_name": a.text(),
            "starter": "*" in cells[1].text(),
            # bold name = on court at the final buzzer (checked against the substitutions in the play-by-play)
            "on_court_at_end": cells[1].find("strong") is not None,
            "played": not dnp,
        }
        for i, (_, key, kind) in enumerate(cols):
            row[key] = _stat(cells[2 + i], kind, what) if not dnp or i == 0 else None
        _need(not dnp or row["minutes"] == "00:00", f"{what}: did-not-play row with minutes {row['minutes']!r}")
        out.append(row)
    _need(out, f"{what}: no player rows")
    return out


def _box_teams(table: _Node, want: Dict[str, int]) -> List[Dict[str, Any]]:
    cols = _box_header(table, "team compare")
    out = []
    body = table.find("tbody")
    for side, tr in zip(("home", "away"), body.kids("tr") if body is not None else []):
        cells = _cells(tr)
        _need(len(cells) == 1 + len(cols) and cells[0].attrs.get("colspan") == "2", f"team compare: row {len(cells)}")
        a = cells[0].find("a")
        ids = _path_ids(a.attrs.get("href") if a else None, "team")
        _need(ids is not None and ids[1:] == (want["sez"], want["lea"]), "team compare: team link outside sez/lea")
        assert ids is not None and a is not None
        row: Dict[str, Any] = {"side": side, "team_id": ids[0], "team_name": a.text()}
        for (_, key, kind), cell in zip(cols, cells[1:]):
            row[key] = _stat(cell, kind, "team compare")
        out.append(row)
    _need(len(out) == 2, f"team compare: {len(out)} rows")
    return out


# --------------------------------------------------------------------------------- play-by-play
# Each period tab (#q1..#q4, #q5 = overtime(s)) holds two tables: the desktop ``.match_table_play_by_play``
# (home | minute+score | away) and the mobile ``#playbyplay-res`` copy that drops the home/away split. The desktop
# table is parsed. Rows are listed newest first; the record reverses them into game order.
def _team_img(cell: _Node) -> Optional[int]:
    img = cell.find("img")
    m = re.search(r"/images/club/50x50/(\d+)\.png", img.attrs.get("src") or "") if img else None
    return int(m.group(1)) if m else None


def _split_event(text: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """``'missed 3 points (jump shot)'`` -> (``'missed 3 points'``, ``'jump shot'``); ``'(offensive)'`` -> (None, ...)."""
    m = re.fullmatch(r"(.*?)\s*\(([^()]*)\)", text or "")
    if not m:
        return (text or None), None
    return (m.group(1) or None), m.group(2)


def _side_event(cell: _Node, side: str, what: str) -> Dict[str, Any]:
    a = cell.find("a")
    pid = name = jersey = None
    if a is not None:
        ids = _path_ids(a.attrs.get("href"), "player")
        _need(ids is not None, f"{what}: player link {a.attrs.get('href')!r}")
        assert ids is not None
        pid, name = ids[0], a.text()
        before: List[str] = []
        for c in cell.children:  # text between the badge and the link is the jersey number
            if c is a:
                break
            if isinstance(c, str):
                before.append(c)
        jersey = _int_text("".join(before))
        after = "".join(c if isinstance(c, str) else c.text() for c in cell.children[cell.children.index(a) + 1 :])
        text = re.sub(r"^\.\s*", "", re.sub(r"\s+", " ", after).strip())
    else:
        text = cell.text()
    action, qualifier = _split_event(text)
    return {
        "side": side,
        "team_id": _team_img(cell),
        "jersey": jersey,
        "player_id": pid,
        "player_name": name,
        "text": text,
        "action": action,
        "qualifier": qualifier,
    }


def _pbp(root: _Node, home_id: int, away_id: int) -> List[Dict[str, Any]]:
    tabs = root.find("div", id="tabs_for_quaters")
    if tabs is None:
        return []  # box-only seasons (before 2017-18) have no Play-by-play tab
    out: List[Dict[str, Any]] = []
    for pane in tabs.findall("div", cls="tab-pane"):
        m = re.fullmatch(r"q(\d+)", pane.attrs.get("id") or "")
        _need(m, f"pbp: tab {pane.attrs.get('id')!r}")
        assert m is not None
        period = int(m.group(1))
        table = pane.find("table", cls="match_table_play_by_play")
        _need(table is not None, f"pbp q{period}: no desktop table")
        assert table is not None
        thead = table.find("thead")
        head_tr = thead.find("tr") if thead is not None else None
        head = [c.text() for c in _cells(head_tr)] if head_tr is not None else []
        _need(len(head) == 3 and head[1] == "Minute | Score", f"pbp q{period}: header {head}")
        rows: List[Dict[str, Any]] = []
        tbody = table.find("tbody")
        for tr in tbody.kids("tr") if tbody is not None else []:
            cells = _cells(tr)
            _need(len(cells) == 3, f"pbp q{period}: row width {len(cells)}")
            mid = [s for s in cells[1].text(br="\n").split("\n") if s]
            ev: Dict[str, Any] = {"period": period, "clock": None, "home_score": None, "away_score": None}
            clock = next((s for s in mid if re.fullmatch(r"\d{2}:\d{2}", s)), None)
            ev["clock"] = clock
            score = cells[1].find("span", cls="td-rezultat")
            if score is not None:
                sm = re.fullmatch(r"(\d+) : (\d+)", score.text())
                _need(sm, f"pbp q{period}: score {score.text()!r}")
                assert sm is not None
                ev["home_score"], ev["away_score"] = int(sm.group(1)), int(sm.group(2))
            home_txt, away_txt = cells[0].text(), cells[2].text()
            if home_txt or away_txt:
                _need(not (home_txt and away_txt), f"pbp q{period}: both sides filled")
                side, cell = ("home", cells[0]) if home_txt else ("away", cells[2])
                ev.update(_side_event(cell, side, f"pbp q{period}"))
                _need(
                    ev["team_id"] in (None, home_id if side == "home" else away_id),
                    f"pbp q{period}: {side} badge {ev['team_id']}",
                )
            else:  # a game / period / timeout marker in the middle column
                label = [s for s in mid if s != clock and not re.fullmatch(r"\d+ : \d+", s)]
                text = " ".join(label)
                tid = _team_img(cells[1])
                mark = None if tid is None else ("home" if tid == home_id else "away" if tid == away_id else "?")
                _need(mark != "?", f"pbp q{period}: timeout badge {tid}")
                _need(text or clock, f"pbp q{period}: empty row")
                ptext: Optional[str] = text or None  # a bare-clock row ('10:00' and nothing else) opens some periods
                action, qualifier = _split_event(ptext) if ptext else (None, None)
                ev.update(
                    {
                        "side": mark,
                        "team_id": tid,
                        "jersey": None,
                        "player_id": None,
                        "player_name": None,
                        "text": ptext,
                        "action": action,
                        "qualifier": qualifier,
                    }
                )
            rows.append(ev)
        rows.reverse()  # newest first on the page -> game order
        out.extend(rows)
    for i, ev in enumerate(out, 1):
        ev["seq"] = i
    return out


# --------------------------------------------------------------------------------- match record
_ROUND = re.compile(r"^(?P<head>.+), Season (?P<season>\d{4}/\d{2})(?: - (?P<group>.+))?$")


def _hidden(root: _Node, ident: str) -> Optional[str]:
    n = root.find("input", id=ident)
    return n.attrs.get("value") if n is not None else None


def _officials(root: _Node) -> Dict[str, Any]:
    out: Dict[str, Any] = {"commissioner": None, "referees": []}
    for span in root.findall("span", cls="smallCaps"):
        label = span.text().rstrip(":").strip()
        val: List[str] = []
        parent = span.parent
        assert parent is not None
        for sib in parent.children[parent.children.index(span) + 1 :]:
            if isinstance(sib, _Node) and sib.tag in ("br", "span"):
                break
            val.append(sib if isinstance(sib, str) else sib.text())
        v = re.sub(r"\s+", " ", "".join(val)).strip()
        if label == "Commissioner":
            out["commissioner"] = v or None
        elif label == "Referees":
            out["referees"] = [r.strip() for r in v.split(",") if r.strip()]
        else:
            raise _NotParsed(f"unknown official label {label!r}")
    return out


def _match_record(html: str, want: Dict[str, int]) -> Dict[str, Any]:
    root = _dom(html)
    echo = {k: _int_text(_hidden(root, f"match_data_{k}")) for k in ("id", "sez", "lea")}
    for k, w in (("id", want["game_no"]), ("sez", want["sez"]), ("lea", want["lea"])):
        _need(_same(echo[k], w), f"out of scope: match_data_{k}={echo[k]!r}, asked for {w!r}")
    info = root.find("div", id="basic_match_info")
    _need(info is not None, "no #basic_match_info")
    assert info is not None
    rn = info.find("span", cls="roundNumber")
    rm = _ROUND.match(rn.text() if rn else "")
    _need(rm, f"round line {rn.text() if rn else None!r}")
    assert rm is not None and rn is not None
    round_label, _, competition = rm.group("head").rpartition(", ")
    _need(round_label and competition, f"round line {rn.text()!r}")
    dv = info.find("div", cls="dateAndVenue_container")
    lines = [s for s in dv.text(br="\n").split("\n") if s] if dv else []
    _need(lines, "no date line")
    game: Dict[str, Any] = {
        "game_no": echo["id"],
        "sez": echo["sez"],
        "lea": echo["lea"],
        "round": round_label,
        "competition": competition,
        "season": rm.group("season"),
    }
    if rm.group("group"):
        gm = re.fullmatch(r"Group (\w+)", rm.group("group"))
        _need(gm, f"group {rm.group('group')!r}")
        assert gm is not None
        game["group"] = gm.group(1)  # 'A', as in the calendar's Group column
    game.update(_when(lines[0]))
    game["attendance"] = None
    for s in lines[1:]:
        if re.fullmatch(r"\d+", s):
            game["attendance"] = int(s)
        elif s.startswith("Venue:"):
            game["venue"] = s[len("Venue:") :].strip()
        else:
            raise _NotParsed(f"unknown header line {s!r}")
    clubs = root.find("div", id="match_clubs_and_results_info_table")
    _need(clubs is not None, "no clubs table")
    assert clubs is not None
    teams: List[Tuple[int, str, Optional[str]]] = []
    for td in clubs.findall("td", cls="hidden-xs"):
        club = td.find("span", cls="club")
        a = club.find("a") if club else None
        ids = _path_ids(a.attrs.get("href") if a else None, "team")
        _need(ids is not None and ids[1:] == (want["sez"], want["lea"]), "team link outside sez/lea")
        assert ids is not None and a is not None
        city = td.find("span", cls="city")
        teams.append((ids[0], a.text(), city.text() if city else None))
    _need(len(teams) == 2, f"{len(teams)} teams in the header")
    score_cell = clubs.find("td", cls="gameScore")
    sc = re.fullmatch(r"(\d+) : (\d+)", score_cell.text() if score_cell else "")
    _need(sc, "no final score: the game is not finished")  # an unplayed game shows ' : '
    assert sc is not None
    for side, (tid, name, city_text), pts in zip(("home", "away"), teams, sc.groups()):
        game[f"{side}_team_id"], game[f"{side}_team"] = tid, name
        game[f"{side}_city"], game[f"{side}_score"] = city_text, int(pts)
    _need(
        _same(_hidden(root, "match_data_home"), game["home_team"])
        and _same(_hidden(root, "match_data_guest"), game["away_team"]),
        "match_data_home/guest differ from the header teams",
    )
    game.update(_officials(root))
    q = root.find("table", id="match_cetrtine_rezultat")
    _need(q is not None, "no quarter table")
    assert q is not None
    qbody = q.find("tbody")
    qrows = q.kids("tr") or (qbody.kids("tr") if qbody is not None else [])
    _need(len(qrows) == 2, "quarter table shape")
    labels, scores = [c.text() for c in _cells(qrows[0])], [c.text() for c in _cells(qrows[1])]
    _need(len(labels) == len(scores) and labels[:4] == ["Q1", "Q2", "Q3", "Q4"], f"quarter labels {labels}")
    periods: List[Dict[str, Any]] = []
    for lab, s in zip(labels, scores):
        m = re.fullmatch(r"(\d+):\s*(\d+)", s)
        _need(m and re.fullmatch(r"Q[1-4]|OT\d", lab), f"period {lab} {s!r}")
        assert m is not None
        periods.append({"period": lab, "home": int(m.group(1)), "away": int(m.group(2))})
    _need(
        sum(p["home"] for p in periods) == game["home_score"] and sum(p["away"] for p in periods) == game["away_score"],
        "period scores do not add up to the final score: the game is not final",
    )
    box = root.find("div", id="Boxscore")
    _need(box is not None, "no #Boxscore")
    assert box is not None
    ptables = list(box.findall("table", cls="match_boxscore_team_table"))
    ttable = box.find("table", cls="match_boxscore_teams_compare_table")
    _need(len(ptables) == 2 and ttable is not None, f"{len(ptables)} box tables")
    assert ttable is not None
    heads = [h.find("a") for h in box.findall("h4") if h.find("a", cls="team")]
    _need(
        [(_path_ids(a.attrs.get("href"), "team") or (None,))[0] for a in heads if a is not None]
        == [game["home_team_id"], game["away_team_id"]],
        "box tables are not home then away",
    )
    players = _box_players(ptables[0], "home", game["home_team_id"], want) + _box_players(
        ptables[1], "away", game["away_team_id"], want
    )
    tteams = _box_teams(ttable, want)
    _need(
        [t["team_id"] for t in tteams] == [game["home_team_id"], game["away_team_id"]],
        "team compare is not home then away",
    )
    _need(
        [t["points"] for t in tteams] == [game["home_score"], game["away_score"]],
        "team totals differ from the final score",
    )
    for flag in ("starter", "on_court_at_end"):  # '*' and bold only exist with play-by-play data (2017-18 on)
        if not any(p[flag] for p in players):
            for p in players:
                p[flag] = None
    pbp = _pbp(root, game["home_team_id"], game["away_team_id"])
    if pbp:
        _need(any(e["text"] == "game finished" for e in pbp), "play-by-play without 'game finished': not final")
        scored = [e for e in pbp if e["home_score"] is not None]
        _need(
            scored and (scored[-1]["home_score"], scored[-1]["away_score"]) == (game["home_score"], game["away_score"]),
            "last play-by-play score differs from the final score",
        )
        _need(max(e["period"] for e in pbp) == 4 + (len(periods) - 4), "play-by-play periods differ from the quarters")
    return {"game": game, "periods": periods, "players": players, "teams": tteams, "pbp": pbp}


# --------------------------------------------------------------------- calendar / standings / player
_CAL_HEAD = (["Game", "Result", "Date and Time", "Info"], ["Game", "Result", "Date and Time", "Info", "Group"])


def _pair(a: _Node, what: str) -> List[Optional[str]]:
    """``'<a>Home <span>:</span> Away</a>'`` -> [home, away]; an unset playoff pairing (``' : '``) -> [None, None]."""
    spans = [c for c in a.children if isinstance(c, _Node) and c.tag == "span"]
    _need(len(spans) == 1 and spans[0].text() == ":", f"calendar: {what} {a.text()!r}")
    i = a.children.index(spans[0])

    def side(parts: List[Any]) -> Optional[str]:
        return re.sub(r"\s+", " ", "".join(c if isinstance(c, str) else c.text() for c in parts)).strip() or None

    return [side(a.children[:i]), side(a.children[i + 1 :])]


def _season_label(chosen: Optional[str], seasons: List[Dict[str, Any]]) -> str:
    return next(o["label"] for o in seasons if f"{o['sez']}/{o['lea']}/" == chosen)


def _scoped_season(root: _Node, want: Dict[str, int], has_games: bool) -> Tuple[str, List[Dict[str, Any]]]:
    chosen, seasons = _season_select(root)
    if chosen is None and not has_games:  # an unknown sez: no selected option and no games
        raise _NotParsed(f"the site has no season sez={want['sez']} lea={want['lea']}", empty=True)
    _need(_same(chosen, f"{want['sez']}/{want['lea']}/"), f"out of scope: season {chosen!r}, asked for {want}")
    assert chosen is not None
    return chosen, seasons


def _calendar_record(html: str, want: Dict[str, int]) -> Dict[str, Any]:
    root = _dom(html)
    games: List[Dict[str, Any]] = []
    for panel in root.findall("div", cls="panel"):
        title = panel.find("h4", cls="panel-title")
        table = panel.find("table", cls="league_calendar_table")
        if title is None or table is None:
            continue
        rnd = re.sub(r"[-]", "", title.text()).strip()  # drop the icon-font glyph of the <i> toggle
        thead, tbody = table.find("thead"), table.find("tbody")
        _need(thead is not None and tbody is not None and thead.find("tr") is not None, "calendar: table shape")
        assert thead is not None and tbody is not None
        head = [c.text() for c in _cells(thead.find("tr"))]  # type: ignore[arg-type]
        _need(head in _CAL_HEAD, f"calendar header {head}")
        for tr in tbody.kids("tr"):
            cells = _cells(tr)
            _need(len(cells) == len(head), f"calendar row width {len(cells)}")
            full = cells[0].find("p", cls="hidden-xs")
            short = cells[0].find("p", cls="visible-xs")
            a = full.find("a") if full else None
            m = re.search(r"/match/(\d+)/(\d+)/(\d+)/Overview", a.attrs.get("href") or "") if a else None
            _need(m, "calendar: no match link")
            assert m is not None and a is not None
            gno, sez, lea = (int(x) for x in m.groups())
            _need((sez, lea) == (want["sez"], want["lea"]), f"calendar: game link {sez}/{lea} outside the season")
            names = _pair(a, "teams")  # a name can hold ':' ('Igokea m:tel'): split on the <span>:</span>
            sa = short.find("a") if short else None
            _need(sa is not None, "calendar: no abbreviations")
            assert sa is not None
            abbr = _pair(sa, "abbreviations")
            res = cells[1].text()
            rm = re.fullmatch(r"(\d+) : (\d+)", res)
            _need(rm or res == "", f"calendar: result {res!r}")
            g: Dict[str, Any] = {
                "round": rnd,
                "game_no": gno,
                "sez": sez,
                "lea": lea,
                "home_team": names[0],
                "away_team": names[1],
                "home_abbr": abbr[0],
                "away_abbr": abbr[1],
                "home_score": int(rm.group(1)) if rm else None,
                "away_score": int(rm.group(2)) if rm else None,
            }
            g.update(_when(cells[2].text()))
            if len(head) == 5:
                g["group"] = cells[4].text() or None
            games.append(g)
    chosen, seasons = _scoped_season(root, want, bool(games))
    _need(games, "calendar: no games")
    _need(len({g["game_no"] for g in games}) == len(games), "calendar: duplicate game numbers")
    return {
        "season": {"sez": want["sez"], "lea": want["lea"], "label": _season_label(chosen, seasons)},
        "seasons": seasons,
        "games": games,
    }


_STAND_HEAD = ["#", "Club", "GP", "W", "L", "Overall", "Diff", "Points"]


def _standings_record(html: str, want: Dict[str, int]) -> Dict[str, Any]:
    root = _dom(html)
    main = root.find("section", id="main_content")
    _need(main is not None, "no #main_content")
    assert main is not None
    rows: List[Dict[str, Any]] = []
    title: Optional[str] = None
    stage: Optional[str] = None
    for n in main.iter():
        if n.tag == "h2" and "main_title" in n.classes():
            t = n.text()
            if re.fullmatch(r"Group \w+", t):
                title = t
            else:
                stage, title = t, None
        elif n.tag == "table" and "league_standings_table" in n.classes():
            thead, tbody = n.find("thead"), n.find("tbody")
            _need(thead is not None and tbody is not None and thead.find("tr") is not None, "standings: table shape")
            assert thead is not None and tbody is not None
            head = [c.text() for c in _cells(thead.find("tr"))]  # type: ignore[arg-type]
            _need(head == _STAND_HEAD, f"standings header {head}")
            for tr in tbody.kids("tr"):
                cells = _cells(tr)
                _need(len(cells) == len(head), f"standings row width {len(cells)}")
                a = cells[1].find("a")
                ids = _path_ids(a.attrs.get("href") if a else None, "team")
                _need(ids is not None and ids[1:] == (want["sez"], want["lea"]), "standings: team link outside season")
                pm = re.fullmatch(r"(\d+):(\d+)", cells[5].text())
                _need(pm, f"standings: overall {cells[5].text()!r}")
                assert ids is not None and a is not None and pm is not None
                r: Dict[str, Any] = {
                    "table": stage,
                    "group": title,
                    "rank": _int_text(cells[0].text()),
                    "team_id": ids[0],
                    "team_name": a.text(),
                    "games": _int_text(cells[2].text()),
                    "wins": _int_text(cells[3].text()),
                    "losses": _int_text(cells[4].text()),
                    "points_for": int(pm.group(1)),
                    "points_against": int(pm.group(2)),
                    "point_diff": _int_text(cells[6].text()),
                    "standing_points": _int_text(cells[7].text()),
                }
                keys = ("rank", "games", "wins", "losses", "point_diff", "standing_points")
                _need(all(r[k] is not None for k in keys), "standings: non-numeric cell")
                rows.append(r)
    chosen, seasons = _scoped_season(root, want, bool(rows))
    _need(rows, "standings: no rows")
    for r in rows:
        _need(r["games"] == r["wins"] + r["losses"], f"standings: {r['team_name']} games != wins + losses")
        _need(
            r["points_for"] - r["points_against"] == r["point_diff"],
            f"standings: {r['team_name']} diff != for - against",
        )
    for key in dict.fromkeys((r["table"], r["group"]) for r in rows):
        grp = [r for r in rows if (r["table"], r["group"]) == key]
        if (key[0] or "").startswith("Regular Season"):  # a closed round robin; Top 8 / Play-out carry points over
            _need(
                sum(r["wins"] for r in grp) == sum(r["losses"] for r in grp)
                and sum(r["points_for"] for r in grp) == sum(r["points_against"] for r in grp),
                f"standings: {key} wins/losses or points for/against do not balance (a row is missing)",
            )
    return {
        "season": {"sez": want["sez"], "lea": want["lea"], "label": _season_label(chosen, seasons)},
        "rows": rows,
    }


_PLAYER_INFO = {
    "Club:": "club",
    "Position:": "position",
    "Height:": "height_cm",
    "Date of Birth:": "birth_date",
    "Place of Birth:": "birth_place",
    "Nationality:": "nationality",
}
#: the game log's last row: "Average" is the verified label, "Total" the expected twin (unverified)
_LOG_TOTAL_LABELS = {"Average", "Total"}


def _player_record(html: str, want: Dict[str, int]) -> Dict[str, Any]:
    root = _dom(html)
    h1 = root.find("h1", cls="main_title")
    _need(h1 is not None and h1.text(), "no player name")
    assert h1 is not None
    # select#season options are '{playerId}/{sez}/{lea}/{slug}/': the selected one echoes all three request ids
    sel = root.find("select", id="season")
    _need(sel is not None, "no season select")
    assert sel is not None
    seasons: List[Dict[str, Any]] = []
    chosen: Optional[Tuple[int, int, int]] = None
    for o in sel.findall("option"):
        m = re.fullmatch(r"(\d+)/(\d+)/(\d+)/[\w-]+/", o.attrs.get("value") or "")
        _need(m, f"player season option {o.attrs.get('value')!r}")
        assert m is not None
        pid, sez, lea = (int(x) for x in m.groups())
        _need(pid == want["player_id"], f"out of scope: season option of player {pid}")
        seasons.append({"sez": sez, "lea": lea, "label": o.text()})
        if "selected" in o.attrs:
            chosen = (pid, sez, lea)
    ask = (want["player_id"], want["sez"], want["lea"])
    _need(chosen == ask, f"out of scope: selected {chosen}, asked for {ask}")
    info = root.find("table", id="player_basic_info_table")
    _need(info is not None, "no player info table")
    assert info is not None
    p: Dict[str, Any] = {
        "player_id": want["player_id"],
        "sez": want["sez"],
        "lea": want["lea"],
        "name": h1.text(),
        "seasons": seasons,
    }
    ibody = info.find("tbody")
    _need(ibody is not None, "player info: no tbody")
    assert ibody is not None
    for tr in ibody.kids("tr"):
        cells = _cells(tr)
        _need(len(cells) == 2, "player info row width")
        label = cells[0].text()
        _need(label in _PLAYER_INFO, f"unknown player info {label!r}")
        key, val = _PLAYER_INFO[label], cells[1].text()
        if key == "club":
            a = cells[1].find("a")
            ids = _path_ids(a.attrs.get("href") if a else None, "team")
            _need(ids is not None and ids[1:] == (want["sez"], want["lea"]), "player club link outside sez/lea")
            assert ids is not None and a is not None
            p["team_id"], p["club"] = ids[0], a.text()
        elif key == "height_cm":
            hm = re.fullmatch(r"(\d+) cm", val)
            p[key] = int(hm.group(1)) if hm else None
        elif key == "birth_date":
            p[key] = _iso(val)
        else:
            p[key] = val or None
    clubs = root.find("div", id="players_clubs_container")
    _need(clubs is not None and clubs.find("h3") is not None, "no Clubs box")
    assert clubs is not None
    clubs.children.remove(clubs.find("h3"))  # the 'Clubs' heading is not a career line
    p["career"] = [s for s in clubs.text(br="\n").split("\n") if s]
    table = root.find("table", id="player_game_stats_table")
    _need(table is not None, "no game log")
    assert table is not None
    cols = _box_header(table, "game log")
    gbody = table.find("tbody")
    _need(gbody is not None, "game log: no tbody")
    assert gbody is not None
    games = []
    for tr in gbody.kids("tr"):
        cells = _cells(tr)
        _need(len(cells) == 2 + len(cols), f"game log row width {len(cells)}")
        a = cells[1].find("a")
        m = re.search(r"/match/(\d+)/(\d+)/(\d+)/", a.attrs.get("href") or "") if a else None
        if not m:  # only the season totals / averages row has no game link; any other linkless row is drift
            _need(cells[1].text() in _LOG_TOTAL_LABELS, f"game log row without a game link: {cells[1].text()!r}")
            continue
        assert a is not None
        gno, sez, lea = (int(x) for x in m.groups())
        _need((sez, lea) == (want["sez"], want["lea"]), "game log: game outside sez/lea")
        g: Dict[str, Any] = {"round_no": _int_text(cells[0].text()), "game_no": gno, "game": a.text()}
        for (_, key, kind), cell in zip(cols, cells[2:]):
            g[key] = _stat(cell, kind, "game log")
        games.append(g)
    _need(games, "player: empty game log")
    p["games"] = games
    return p


_RECORDS = {
    "match": _match_record,
    "calendar": _calendar_record,
    "standings": _standings_record,
    "player": _player_record,
}


def record_for(kind: str, html: str, want: Dict[str, int]) -> Dict[str, Any]:
    """The page record of one ABA page, scoped to the request.

    Args:
        kind: ``match``, ``calendar``, ``standings`` or ``player``.
        html: the page text.
        want: the request as the site keys it: ``game_no`` (match), ``player_id`` (player), ``sez`` and ``lea``.

    Returns:
        The refs capture shape of the page (see ``sdv-internal-refs`` ``aba-liga/aba-liga-returns.md``).

    Raises:
        _NotParsed: the page is not the one asked for, is not final (a match) or has drifted. ``empty`` is set
            for a season the site does not hold. Any other exception inside the DOM walk is also a refusal.

    Example:
        Parse a final match page::

            from sportsdataverse.basketball.aba.aba_parsers import record_for

            record = record_for("match", html, {"game_no": 1, "sez": 2025, "lea": 1})
    """
    try:
        return _RECORDS[kind](html, want)
    except _NotParsed:
        raise
    except Exception as exc:  # an odd page is a refusal, never a crash (RecursionError is a RuntimeError)
        raise _NotParsed(f"parser error: {type(exc).__name__}") from None


# --------------------------------------------------------------------------- JSON helper checks
_SHOT_PLAYER_SLACK = 1
_SHOT_SUM_ATT_MAX = 8
_SHOT_SUM_MADE_MAX = 4
_SHOT_UNRECORDED_MAX = 3


def _shot_scope_error(body: List[Dict[str, Any]], match: Dict[str, Any]) -> Optional[str]:
    """Tally the chart per shooter and compare it with that shooter's box row (the chart echoes no game id).

    Rows must equal field-goal attempts and made shots within one per shooter; the summed absolute differences and
    the unrecorded-shooter rows (``player_id`` ``"0"``) are capped. Measured on the four refs samples: a shooter
    differs by 0 or +1, the sums are at most 4 attempts / 1 made / 1 unrecorded; a rematch of the same rosters
    differs by several per player.
    """
    box = {}
    for p in match["players"]:
        played = p["played"]
        box[(p["side"], p["player_id"])] = (
            p["fg2_att"] + p["fg3_att"] if played else 0,
            p["fg2_made"] + p["fg3_made"] if played else 0,
        )
    tally: Dict[Tuple[str, int], List[int]] = {}
    unrecorded = 0
    for r in body:
        side = {"1": "home", "2": "away"}.get(r.get("ekipa"))  # type: ignore[arg-type]
        if side is None:
            return f"shot with ekipa {r.get('ekipa')!r}"
        pid = r.get("player_id")
        if not isinstance(pid, str) or r.get("koordinata_uspeh") not in ("0", "1"):
            return f"shot with player_id {pid!r}, koordinata_uspeh {r.get('koordinata_uspeh')!r}"
        if pid == "0":  # shooter not recorded (player_name ' ')
            unrecorded += 1
            continue
        pid_n = _int_text(pid)
        if pid_n is None or (side, pid_n) not in box:
            return f"out of scope: shot by player {pid!r}, not in the {side} box score"
        t = tally.setdefault((side, pid_n), [0, 0])
        t[0] += 1
        t[1] += r["koordinata_uspeh"] == "1"
    if unrecorded > _SHOT_UNRECORDED_MAX:
        return f"out of scope: {unrecorded} shots without a recorded shooter (max {_SHOT_UNRECORDED_MAX})"
    d_att = d_made = 0
    for key, (att, made) in box.items():
        got = tally.get(key, [0, 0])
        if abs(got[0] - att) > _SHOT_PLAYER_SLACK or abs(got[1] - made) > _SHOT_PLAYER_SLACK:
            return f"out of scope: player {key[1]} has {got[0]} shots / {got[1]} made on the chart, {att} / {made} in the box"
        d_att += abs(got[0] - att)
        d_made += abs(got[1] - made)
    if d_att > _SHOT_SUM_ATT_MAX or d_made > _SHOT_SUM_MADE_MAX:
        return f"out of scope: per-player differences add up to {d_att} attempts / {d_made} made against the box score"
    return None


def helper_scope_error(kind: str, body: Any, match: Dict[str, Any]) -> Optional[str]:
    """Why a live-match helper body is not this game's (``None`` when it is).

    Args:
        kind: ``shots``, ``flow`` or ``scorediff``.
        body: the decoded JSON.
        match: the same game's match record (:func:`record_for`).

    Returns:
        A reason string, or ``None``. The helpers echo nothing about the game, so each is checked against the match
        page: minutes must run exactly 0..40 (+5 per overtime) and the last row must equal the final score; the shot
        chart is tallied per shooter against the box score.

    Example:
        Check a score-flow helper against the match page::

            from sportsdataverse.basketball.aba.aba_parsers import helper_scope_error

            reason = helper_scope_error("flow", flow_rows, match_record)  # None when the rows are this game's
    """
    if not isinstance(body, list) or not all(isinstance(r, dict) for r in body):
        return f"not a list of objects: {type(body).__name__}"
    g = match["game"]
    if kind == "shots":
        return _shot_scope_error(body, match)
    minutes = [r.get("minuta") for r in body]
    want_last = 40 + 5 * (len(match["periods"]) - 4)
    if minutes != list(range(want_last + 1)):
        return f"out of scope: minutes 0..{minutes[-1] if minutes else None}, the game ran 0..{want_last}"
    last = body[-1]
    if not (_same(last.get("domaci_rez"), str(g["home_score"])) and _same(last.get("tuji_rez"), str(g["away_score"]))):
        return f"out of scope: last minute {last.get('domaci_rez')!r}:{last.get('tuji_rez')!r} != final {g['home_score']}:{g['away_score']}"
    return None


# ------------------------------------------------------------------------------------- frames
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


def _date_or_none(v: Any) -> Optional[date]:
    try:
        return date.fromisoformat(v) if isinstance(v, str) else None
    except ValueError:
        return None


def _seconds(v: Any) -> Optional[int]:
    """``"08:34"`` -> 514; None for anything that is not ``mm:ss``."""
    m = re.fullmatch(r"\s*(\d+):(\d{2})\s*", v) if isinstance(v, str) else None
    return int(m.group(1)) * 60 + int(m.group(2)) if m else None


def _joined(v: Any) -> Optional[str]:
    return "; ".join(str(x) for x in v) if isinstance(v, list) and v else None


def _record(raw: Any, key: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """``raw`` (or ``raw[key]``) when it is a non-empty dict, else ``None``."""
    if not isinstance(raw, dict):
        return None
    got = raw.get(key) if key else raw
    return got if isinstance(got, dict) and got else None


def _lists(raw: Any, key: str) -> List[Dict[str, Any]]:
    got = raw.get(key) if isinstance(raw, dict) else None
    return [r for r in got if isinstance(r, dict)] if isinstance(got, list) else []


def _ctx_row(sez: Any, lea: Any, **keys: Any) -> Dict[str, Any]:
    """Season / league context columns for a record whose ``sez`` / ``lea`` are known."""
    sez_n = sez if isinstance(sez, int) and not isinstance(sez, bool) else None
    lea_n = lea if isinstance(lea, int) and not isinstance(lea, bool) else None
    return {
        **keys,
        "season": None if sez_n is None else 2001 + sez_n,
        "league_id": lea_n,
        "league": LEAGUES.get(lea_n) if lea_n is not None else None,
    }


_CTX_SPEC: Spec = {"game_no": INT, "season": INT, "league_id": INT, "league": S}
_SEASON_SPEC: Spec = {"season": INT, "league_id": INT, "league": S}


def _match_ctx(raw: Any) -> Dict[str, Any]:
    g = _record(raw, "game") or {}
    return _ctx_row(g.get("sez"), g.get("lea"), game_no=g.get("game_no"))


# -------------------------------------------------------------------------------------- game
_GAME_SPEC: Spec = {
    **_CTX_SPEC,
    "season_label": S,
    "round": S,
    "competition": S,
    "group": S,
    "when": S,
    "date": D,
    "date_to": D,
    "time": S,
    "time_zone": S,
    "local_time": S,
    "local_place": S,
    "attendance": INT,
    "venue": S,
    "home_team_id": INT,
    "home_team": S,
    "home_city": S,
    "home_score": INT,
    "away_team_id": INT,
    "away_team": S,
    "away_city": S,
    "away_score": INT,
    "commissioner": S,
    "referees": S,
}


def parse_aba_game(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The header of one finished ABA League game, one row (the match page's header, clubs table and officials line).

    Args:
        raw: the page record a match wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row keyed by (``game_no``, ``season``, ``league_id``): round and competition labels, the date line
        split into ``date`` / ``time`` / ``time_zone`` (``CET`` is wall-clock Central European time including
        summer time; ABA League 2 prints ``local``), attendance, venue (null before 2017-18), the two clubs with
        their global ``team_id`` and final score, the commissioner and the referees (``"; "``-joined, names only).
        Zero rows for an empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.aba import aba_game, parse_aba_game

            raw = aba_game(game_no=6, season=2026, return_parsed=False)
            header = parse_aba_game(raw)
    """
    g = _record(raw, "game")
    rows = []
    if g:
        rows.append(
            {
                **_match_ctx(raw),
                "season_label": g.get("season"),
                "round": g.get("round"),
                "competition": g.get("competition"),
                "group": g.get("group"),
                "when": g.get("when"),
                "date": _date_or_none(g.get("date")),
                "date_to": _date_or_none(g.get("date_to")),
                **{k: g.get(k) for k in ("time", "time_zone", "local_time", "local_place", "attendance", "venue")},
                **{
                    f"{s}_{k}": g.get(f"{s}_{k}")
                    for s in ("home", "away")
                    for k in ("team_id", "team", "city", "score")
                },
                "commissioner": g.get("commissioner"),
                "referees": _joined(g.get("referees")),
            }
        )
    return _frame(rows, _GAME_SPEC, columns, return_as_pandas)


# ------------------------------------------------------------------------------------ periods
_PERIOD_SPEC: Spec = {**_CTX_SPEC, "period": S, "period_no": INT, "home_points": INT, "away_points": INT}


def parse_aba_periods(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Points each team scored in each period of one finished ABA League game.

    Args:
        raw: the page record a match wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per period played: ``period`` (``Q1``..``Q4``, then ``OT1``, ``OT2``...), its 1-based ``period_no``
        and the points scored IN the period (not cumulative). Zero rows for an empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.aba import aba_periods, parse_aba_periods

            raw = aba_periods(game_no=6, season=2026, return_parsed=False)
            periods = parse_aba_periods(raw)
    """
    ctx = _match_ctx(raw)
    rows = []
    for i, p in enumerate(_lists(raw, "periods"), 1):
        rows.append(
            {
                **ctx,
                "period": p.get("period"),
                "period_no": i,
                "home_points": p.get("home"),
                "away_points": p.get("away"),
            }
        )
    return _frame(rows, _PERIOD_SPEC, columns, return_as_pandas)


# ------------------------------------------------------------------------------- box score
_BOX_PLAYER_SPEC: Spec = {
    **_CTX_SPEC,
    "side": S,
    "team_id": INT,
    "jersey": INT,
    "player_id": INT,
    "player_name": S,
    "starter": B,
    "on_court_at_end": B,
    "played": B,
    **_STAT_SPEC,
}
_BOX_TEAM_SPEC: Spec = {**_CTX_SPEC, "side": S, "team_id": INT, "team_name": S, **_STAT_SPEC}


def parse_aba_box_players(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The player box score of one finished ABA League game (both teams, home first).

    Args:
        raw: the page record a match wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per player listed, in page order. ``played`` is false on a did-not-play row (minutes ``00:00``, every
        stat null). ``starter`` and ``on_court_at_end`` exist from 2017-18 and are null for the whole game before.
        The advanced ``pts_paint`` / ``pts_second_chance`` / ``pts_fast_break`` trio is null where the season has
        none (2008-09). ``plus_minus`` is 0 on every row of the 2008-09 sample (not tracked then). Percentages are
        0-100. Join on ``player_id`` (global), not on the printed name. Zero rows for an empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.aba import aba_box_players, parse_aba_box_players

            raw = aba_box_players(game_no=6, season=2026, return_parsed=False)
            box = parse_aba_box_players(raw)
    """
    ctx = _match_ctx(raw)
    rows = [{**ctx, **p} for p in _lists(raw, "players")]
    return _frame(rows, _BOX_PLAYER_SPEC, columns, return_as_pandas)


def parse_aba_box_teams(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The team totals ("Team Compare" table) of one finished ABA League game.

    Args:
        raw: the page record a match wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        Two rows, home then away. ``minutes`` is the five on court summed (``200:00``, ``225:00`` with one
        overtime); ``points`` equals the final score; ``reb_tot`` equals the players' sum (team rebounds are not
        added). Zero rows for an empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.aba import aba_box_teams, parse_aba_box_teams

            raw = aba_box_teams(game_no=6, season=2026, return_parsed=False)
            totals = parse_aba_box_teams(raw)
    """
    ctx = _match_ctx(raw)
    rows = [{**ctx, **t} for t in _lists(raw, "teams")]
    return _frame(rows, _BOX_TEAM_SPEC, columns, return_as_pandas)


# --------------------------------------------------------------------------------------- pbp
_PBP_SPEC: Spec = {
    **_CTX_SPEC,
    "seq": INT,
    "period": INT,
    "clock": S,
    "seconds_left": INT,
    "home_score": INT,
    "away_score": INT,
    "side": S,
    "team_id": INT,
    "jersey": INT,
    "player_id": INT,
    "player_name": S,
    "text": S,
    "action": S,
    "qualifier": S,
}


def parse_aba_pbp(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The play-by-play of one finished ABA League game (2017-18 on), in game order.

    Args:
        raw: the page record a match wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per event, ``seq`` 1-based in game order (the page lists each period newest first). ``period`` is
        1-4 for the quarters and 5 for every overtime together; ``clock`` is ``mm:ss`` REMAINING (10:00 per quarter,
        05:00 in overtime) and null on starting-lineup and period markers. The score is printed only on scoring
        events, so ``home_score`` / ``away_score`` are null elsewhere (carry them forward for a running score).
        ``text`` is the English event; ``action`` is the text without its parenthesised ``qualifier`` (shot type,
        rebound side, ``n of m`` free throw, foul or turnover type). Team-only rows print just the qualifier, so
        ``action`` is null there. Empty before 2017-18 (the page has no play-by-play tab). Zero rows for an empty or
        malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.aba import aba_pbp, parse_aba_pbp

            raw = aba_pbp(game_no=6, season=2026, return_parsed=False)
            plays = parse_aba_pbp(raw)
    """
    ctx = _match_ctx(raw)
    rows = [{**ctx, **e, "seconds_left": _seconds(e.get("clock"))} for e in _lists(raw, "pbp")]
    return _frame(rows, _PBP_SPEC, columns, return_as_pandas)


# ------------------------------------------------------------------------------ JSON helpers
def _helper_ctx(raw: Any) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    req = raw.get(_REQUEST) if isinstance(raw, dict) else None
    req = req if isinstance(req, dict) else {}
    lea = req.get("league_id")
    ctx = {
        "game_no": req.get("game_no"),
        "season": req.get("season"),
        "league_id": lea,
        "league": LEAGUES.get(lea) if isinstance(lea, int) else None,
    }
    return ctx, _lists(raw, "rows")


_SHOT_SPEC: Spec = {
    **_CTX_SPEC,
    "side": S,
    "x": F,
    "y": F,
    "made": B,
    "player_id": INT,
    "player_name": S,
}


def _float_any(v: Any) -> Optional[float]:
    """A coordinate from the site's string or number, else None."""
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    return _num_text(v) if isinstance(v, str) else None


def parse_aba_shots(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The shot chart of one finished ABA League game (2017-18 on): every field-goal attempt with x / y.

    Args:
        raw: the wrapped helper body a shot-chart wrapper returns with ``return_parsed=False``
            (``{"rows": [...], "_request": {...}}``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per field-goal attempt (no free throws), in the site's order. ``x`` and ``y`` are PERCENTAGES of the
        full court on the site's 1000x535 px image (court 28 x 15 m), cast to float (the site sends x as a string in
        one half of a game and a number in the other). x runs from the LEFT baseline (home shoots at x < 50, away at
        x > 50). y is drawn upward from the bottom edge, 50 = the line through both baskets; whether "up" is the
        scorer's table side is unverified. ``side`` is ``home`` / ``away`` (the site's ``ekipa`` 1 / 2). ``player_id``
        is cast from the site's string (null when the shooter was not recorded, id 0); join on it, not on the name.
        The chart can differ from the box score by one attempt per shooter. Zero rows for an empty or malformed
        input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.aba import aba_shots, parse_aba_shots

            raw = aba_shots(game_no=6, season=2026, return_parsed=False)
            shots = parse_aba_shots(raw)
    """
    ctx, items = _helper_ctx(raw)
    rows = []
    for r in items:
        pid = _int_text(r.get("player_id")) if isinstance(r.get("player_id"), str) else None
        name = r.get("player_name")
        rows.append(
            {
                **ctx,
                "side": {"1": "home", "2": "away"}.get(r.get("ekipa")),  # type: ignore[arg-type]
                "x": _float_any(r.get("koordinata_x")),
                "y": _float_any(r.get("koordinata_y")),
                "made": {"1": True, "0": False}.get(r.get("koordinata_uspeh")),  # type: ignore[arg-type]
                "player_id": pid or None,
                "player_name": name.strip() or None if isinstance(name, str) else None,
            }
        )
    return _frame(rows, _SHOT_SPEC, columns, return_as_pandas)


_FLOW_SPEC: Spec = {**_CTX_SPEC, "minute": INT, "home_score": INT, "away_score": INT}


def _score_rows(raw: Any) -> List[Dict[str, Any]]:
    ctx, items = _helper_ctx(raw)
    out = []
    for r in items:

        def cum(v: Any) -> Optional[int]:
            return v if isinstance(v, int) and not isinstance(v, bool) else _int_text(v) if isinstance(v, str) else None

        minute = r.get("minuta")
        out.append(
            {
                **ctx,
                "minute": minute if isinstance(minute, int) and not isinstance(minute, bool) else None,
                "home_score": cum(r.get("domaci_rez")),
                "away_score": cum(r.get("tuji_rez")),
            }
        )
    return out


def parse_aba_score_flow(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The score after every whole minute of one finished ABA League game (the Result Graph; 2017-18 on).

    Args:
        raw: the wrapped helper body a ``aba_score_flow`` call returns with ``return_parsed=False``
            (``{"rows": [...], "_request": {...}}``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per minute: ``minute`` 0 (tip-off) to 40, then 41-45 per overtime, and the CUMULATIVE ``home_score`` /
        ``away_score`` at the end of that minute (the site sends them as numeric strings; cast here). The last row
        equals the final score. Zero rows for an empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.aba import aba_score_flow, parse_aba_score_flow

            raw = aba_score_flow(game_no=6, season=2026, return_parsed=False)
            flow = parse_aba_score_flow(raw)
    """
    return _frame(_score_rows(raw), _FLOW_SPEC, columns, return_as_pandas)


def parse_aba_score_difference(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The minute-by-minute score of the site's "Score difference" chart (same rows as ``aba_score_flow``).

    Args:
        raw: the wrapped helper body a ``aba_score_difference`` call returns with ``return_parsed=False``
            (``{"rows": [...], "_request": {...}}``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per minute with the cumulative ``home_score`` / ``away_score``, as :func:`parse_aba_score_flow`;
        for a played game the two routes carry the same rows. Zero rows for an empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.aba import aba_score_difference, parse_aba_score_difference

            raw = aba_score_difference(game_no=6, season=2026, return_parsed=False)
            flow = parse_aba_score_difference(raw)
    """
    return _frame(_score_rows(raw), _FLOW_SPEC, columns, return_as_pandas)


# ---------------------------------------------------------------------------------- calendar
_CALENDAR_SPEC: Spec = {
    "game_no": INT,
    **_SEASON_SPEC,
    "season_label": S,
    "round": S,
    "group": S,
    "home_team": S,
    "away_team": S,
    "home_abbr": S,
    "away_abbr": S,
    "home_score": INT,
    "away_score": INT,
    "when": S,
    "date": D,
    "date_to": D,
    "time": S,
    "time_zone": S,
    "local_time": S,
    "local_place": S,
}


def parse_aba_calendar(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The full schedule and results of one ABA League or ABA League 2 season, one row per game.

    Args:
        raw: the page record ``aba_calendar`` returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per game in page order (one accordion panel per ``round``), keyed by (``game_no``, ``season``,
        ``league_id``). Scores are null for a game not played yet. The calendar carries club NAMES and
        three-letter codes only (no team ids): join to the box score on ``game_no``. ``when`` is the cell verbatim;
        ``date`` / ``date_to`` / ``time`` are split from it (null for ``TBA`` or a date window). ``time_zone`` is
        ``CET`` (Central European wall-clock time including summer time) or ``local`` (ABA League 2). ``group`` is
        the group letter, null after the groups and absent on seasons without a Group column. Zero rows for an empty
        or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.aba import aba_calendar, parse_aba_calendar

            raw = aba_calendar(season=2026, return_parsed=False)
            games = parse_aba_calendar(raw)
    """
    s = _record(raw, "season") or {}
    rows = []
    for g in _lists(raw, "games"):
        rows.append(
            {
                **_ctx_row(g.get("sez"), g.get("lea"), game_no=g.get("game_no")),
                "season_label": s.get("label"),
                **{
                    k: g.get(k)
                    for k in _CALENDAR_SPEC
                    if k not in ("game_no", "season", "league_id", "league", "season_label", "date", "date_to")
                },
                "date": _date_or_none(g.get("date")),
                "date_to": _date_or_none(g.get("date_to")),
            }
        )
    return _frame(rows, _CALENDAR_SPEC, columns, return_as_pandas)


# --------------------------------------------------------------------------------- standings
_STANDINGS_SPEC: Spec = {
    **_SEASON_SPEC,
    "season_label": S,
    "table": S,
    "group": S,
    "rank": INT,
    "team_id": INT,
    "team_name": S,
    "games": INT,
    "wins": INT,
    "losses": INT,
    "points_for": INT,
    "points_against": INT,
    "point_diff": INT,
    "standing_points": INT,
}


def parse_aba_standings(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The standings tables of one ABA League or ABA League 2 season, one row per team per table.

    Args:
        raw: the page record ``aba_standings`` returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per team per table, in page order. A season has several tables: ``table`` is the phase heading
        (``Regular Season 2025/26``, ``Top 8 Season 2025/26``, ``Play-out Season 2025/26``) and ``group`` the group
        under it (``Group A``, null without one). ``standing_points`` are table points, 2 per win and 1 per loss.
        ``points_for`` / ``points_against`` are the two sides of the page's ``Overall`` column; Top 8 and Play-out
        tables carry points over from the regular season. Zero rows for an empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.aba import aba_standings, parse_aba_standings

            raw = aba_standings(season=2026, return_parsed=False)
            table = parse_aba_standings(raw)
    """
    s = _record(raw, "season") or {}
    ctx = _ctx_row(s.get("sez"), s.get("lea"))
    rows = [{**ctx, "season_label": s.get("label"), **r} for r in _lists(raw, "rows")]
    return _frame(rows, _STANDINGS_SPEC, columns, return_as_pandas)


# ------------------------------------------------------------------------------------ player
_PLAYER_SPEC: Spec = {
    "player_id": INT,
    **_SEASON_SPEC,
    "name": S,
    "team_id": INT,
    "club": S,
    "position": S,
    "height_cm": INT,
    "birth_date": D,
    "birth_place": S,
    "nationality": S,
    "career": S,
}
_PLAYER_GAME_SPEC: Spec = {
    "player_id": INT,
    **_SEASON_SPEC,
    "round_no": INT,
    "game_no": INT,
    "game": S,
    **_STAT_SPEC,
}


def parse_aba_player(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """One player's bio for one season and league (the player page's info box and Clubs box), one row.

    Args:
        raw: the page record ``aba_player`` returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row keyed by (``player_id``, ``season``, ``league_id``) with the club (``team_id`` is the global club
        id), position, height in cm, birth date and place, nationality (a three-letter code) and ``career``, the
        free-text Clubs box joined with ``"; "`` (oldest first). These bio fields are the public player profile.
        Zero rows for an empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.aba import aba_player, parse_aba_player

            raw = aba_player(player_id=4807, season=2026, return_parsed=False)
            bio = parse_aba_player(raw)
    """
    p = _record(raw)
    rows = []
    if p and "name" in p:
        rows.append(
            {
                **_ctx_row(p.get("sez"), p.get("lea"), player_id=p.get("player_id")),
                **{
                    k: p.get(k)
                    for k in ("name", "team_id", "club", "position", "height_cm", "birth_place", "nationality")
                },
                "birth_date": _date_or_none(p.get("birth_date")),
                "career": _joined(p.get("career")),
            }
        )
    return _frame(rows, _PLAYER_SPEC, columns, return_as_pandas)


def parse_aba_player_games(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """One player's game log for one season and league, one row per game (the season Average row is dropped).

    Args:
        raw: the page record ``aba_player`` returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per game the player was listed in, keyed by (``player_id``, ``game_no``, ``season``,
        ``league_id``). ``round_no`` is the season-wide round index (regular season 1-18, later phases continue the
        count) and ``game`` the short ``Home-Away`` pairing label. The stat columns are the box-score stats
        (``minutes`` is ``mm:ss``); the three ``pts_*`` columns are null here (the game log has no such columns).
        Zero rows for an empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.aba import aba_player, parse_aba_player_games

            raw = aba_player(player_id=4807, season=2026, return_parsed=False)
            log = parse_aba_player_games(raw)
    """
    p = _record(raw) or {}
    ctx = _ctx_row(p.get("sez"), p.get("lea"), player_id=p.get("player_id"))
    rows = [{**ctx, **g} for g in _lists(p, "games")]
    return _frame(rows, _PLAYER_GAME_SPEC, columns, return_as_pandas)


#: Each parser's column dtypes, keyed by wrapper short, for the generator and the tests.
SPECS: Dict[str, Spec] = {
    "game": _GAME_SPEC,
    "periods": _PERIOD_SPEC,
    "box_players": _BOX_PLAYER_SPEC,
    "box_teams": _BOX_TEAM_SPEC,
    "pbp": _PBP_SPEC,
    "shots": _SHOT_SPEC,
    "score_flow": _FLOW_SPEC,
    "score_difference": _FLOW_SPEC,
    "calendar": _CALENDAR_SPEC,
    "standings": _STANDINGS_SPEC,
    "player": _PLAYER_SPEC,
    "player_games": _PLAYER_GAME_SPEC,
}
