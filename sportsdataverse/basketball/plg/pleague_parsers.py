"""Parsers for the generated ``pleague`` wrappers (P. LEAGUE+, Taiwan's men's league, ``pleagueofficial.com``).

Two shapes feed this family:

* **Server-rendered HTML pages** (schedule per stage, game page, standings, the player season table, the player page)
  and the play-by-play fragment (JSON whose ``data`` is an HTML table). They are parsed here with the stdlib
  :mod:`html.parser` into the records the refs captures bank (``sdv-internal-refs`` ``pleague/tools/capture.py``):
  a minimal DOM, iterative walks, and header / scope / vocabulary checks that refuse layout drift, another game,
  season or stage, an unfinished game and an unknown play-by-play phrase instead of guessing. :func:`record_for`
  and :func:`pbp_record` are what the runtime uses; a refusal is :class:`_NotParsed`.
* **JSON**: the box score (``/api/boxscore.preciser.php``, served as ``text/html``), kept as the site sends it.

Each public ``parse_pleague_*`` takes the dict a wrapper returns with ``return_parsed=False`` (the record, with the
request stamped under ``_request`` by ``pleague_runtime``) and follows the package parser contract: a polars frame
by default, pandas with ``return_as_pandas=True``, snake_case columns, and a zero-row frame with the documented
columns and dtypes, never an exception, for an empty or unrecognised input.

**Season** is the END year (2025 = the site's slug ``"2024-25"``); ``season_slug`` keeps the slug. The site has no
season parameter outside the page paths: games are keyed by ``gid``.

**Id key spaces** (every id is ``Int64`` unless it is a uuid or a page id, which are ``Utf8``):

* ``gid``: ONE global integer sequence over every stage and season (1-12 = 2020-21 pre-season, 197 = the 2022-23
  opener, 662 = the 2025-26 opener), with gaps. It keys the game page, the box and the play-by-play; the box and the
  play-by-play echo no id, so the runtime checks identity by content (scores, quarters, points).
* ``team_id``: the club on the site (1-8), stable across seasons and renames. The schedule and standings carry it;
  the game page, box and play-by-play do not (they carry ``side`` and printed names). The Preciser team uuid is
  the file name of the club logo (``team_uuid`` on the standings).
* ``player_id``: the player-team membership (the box's digit string, cast to ``Int64``; the player page's
  ``member_id`` is the same space). ``person_id``: the Preciser person uuid (box rows from 2022-23; the refs notes
  report some 2025-26 finals boxes without it): the join to the Preciser shot list. ``random_id`` / ``player_page_id``: the 8-character page id
  of ``/player/{id}``. **``random_id`` is unreliable on 2020-21 and 2021-22 box rows** (gid 13 gives one player another
  player's page id): join box rows on ``player_id`` and box to shots on ``person_id``, never on ``random_id``.
* ``uuid``: the Preciser game uuid (empty before 2022-23, so no play-by-play or vendor data there).

**Dtypes** are pinned by column name, never sampled: the box mixes numbers, digit strings and ``''`` (a did-not-play
row) in the same column across eras, so every numeric box cell is cast at this boundary (``''`` -> null).

**Era A / era B box rows.** 2020-21 and 2021-22 (gid 13-188) rows carry made/missed pairs, ``seconds``, the
``efgp`` / ``tsp`` / ``ugp`` percentages and ``position``; 2022-23 on (gid 191+) rows carry ``person_id``, the
ratings (``ortg``, ``drtg``, ``netrtg``, ``pace``), the ratios and the possession columns. A column of the other era
is null.
"""

from __future__ import annotations

import base64
import binascii
import json
import re
from datetime import date
from html.parser import HTMLParser
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union

import pandas as pd
import polars as pl

__all__ = [
    "parse_pleague_box_players",
    "parse_pleague_box_teams",
    "parse_pleague_game",
    "parse_pleague_officials",
    "parse_pleague_pbp",
    "parse_pleague_periods",
    "parse_pleague_player",
    "parse_pleague_player_seasons",
    "parse_pleague_schedule",
    "parse_pleague_standings",
    "parse_pleague_stat_player",
]

Frame = Union[pl.DataFrame, pd.DataFrame]

#: Where the runtime stamps the request (``pleague_runtime.REQUEST_KEY``).
_REQUEST = "_request"

INT, F, S, B, D = pl.Int64, pl.Float64, pl.Utf8, pl.Boolean, pl.Date

#: The game page's status badge for a finished game (the only status the runtime serves).
FINAL = "已完賽"
STAGES = {  # URL slug -> (title word, English)
    "pre-season": ("熱身賽", "pre-season (exhibition)"),
    "regular-season": ("例行賽", "regular season"),
    "playoffs": ("季後賽", "playoffs"),
    "finals": ("總冠軍賽", "finals"),
}
STAT_STAGES = {2: "regular-season", 3: "playoffs", 4: "finals"}  # /stat-player/{season}/{stage_sn} select values


class _NotParsed(Exception):
    """The page is not the data asked for: a missing selector or header, a row of the wrong width, another scope
    (game, season, stage, player), an unfinished game or an unknown play-by-play phrase.

    ``empty`` marks a page that is the right one but holds nothing (a stage the season does not have); the runtime
    raises it as ``NoDataError``. Every other refusal is an ``AssetFetchError``.
    """

    def __init__(self, msg: str, *, empty: bool = False) -> None:
        super().__init__(msg)
        self.empty = empty


# ---------------------------------------------------------------------------------- play-by-play vocabulary
# Phrase -> (code, English). Codes are this table's own. The video clip's stat_category names the PLAY the clip
# shows, not the row it hangs on (a rebound row carries the missed shot's clip), so it is never used as the action.
_SHOT_STYLES = [  # (phrase stem, code stem, English)
    ("上籃", "layup", "layup"),
    ("突破上籃", "driving_layup", "driving layup"),
    ("跳投", "jump_shot", "jump shot"),
    ("拋投", "floater", "floater"),
    ("補籃", "putback", "putback / tip-in"),
    ("灌籃", "dunk", "dunk"),
    ("轉身跳投", "turnaround_jumper", "turnaround jumper"),
    ("急停跳投", "pull_up_jumper", "pull-up jumper"),
    ("後仰跳投", "fadeaway_jumper", "fadeaway jumper"),
    ("勾射", "hook_shot", "hook shot"),
    ("後撤步投籃", "step_back_shot", "step-back shot"),
    ("alleyOop", "alley_oop", "alley-oop"),
]
_THREE_STYLES = [
    ("三分外線" + z, c, e)
    for z, c, e in _SHOT_STYLES
    if c in ("step_back_shot", "pull_up_jumper", "turnaround_jumper", "fadeaway_jumper")
]
_OUTCOMES = [("得分", "made", "made"), ("不進", "missed", "missed")]
_FIXED = [  # phrase, code, English (a player or a team did it)
    ("三分球得分", "fg3_made", "3-point shot, style not recorded, made"),
    ("三分球不進", "fg3_missed", "3-point shot, style not recorded, missed"),
    ("得分", "fg_made", "field goal made, shot type and distance not recorded"),
    ("不進", "fg_missed", "field goal missed, shot type and distance not recorded"),
    ("搶到進攻籃板", "reb_off", "offensive rebound (a team rebound has no player)"),
    ("搶到防守籃板", "reb_def", "defensive rebound (a team rebound has no player)"),
    ("助攻", "ast", "assist"),
    ("抄截", "stl", "steal"),
    ("阻攻", "blk", "block"),
    ("傳球失誤", "tov_bad_pass", "turnover: bad pass"),
    ("控球失誤", "tov_ball_handling", "turnover: ball-handling"),
    ("進攻失誤", "tov_offensive", "turnover: offensive (kind not recorded)"),
    ("帶球走步", "tov_travel", "turnover: travelling"),
    ("球出界", "tov_out_of_bounds", "turnover: ball out of bounds"),
    ("3Sec 失誤", "tov_3sec", "turnover: 3-second violation"),
    ("5Sec 失誤", "tov_5sec", "turnover: 5-second violation"),
    ("8Sec 失誤", "tov_8sec", "turnover: 8-second violation"),
    ("shotClock 失誤", "tov_shot_clock", "turnover: shot-clock violation"),
    ("backCourt 失誤", "tov_backcourt", "turnover: backcourt violation"),
    ("doubleDribble 失誤", "tov_double_dribble", "turnover: double dribble"),
    ("carrying 失誤", "tov_carrying", "turnover: carrying the ball"),
    ("offensiveGoalTending 失誤", "tov_offensive_goaltending", "turnover: offensive goaltending"),
    ("其他失誤", "tov_other", "turnover: other"),
    ("個人犯規", "foul_personal", "personal foul committed"),
    ("進攻犯規", "foul_offensive", "offensive foul committed"),
    ("造成對手犯規", "foul_drawn", "foul drawn (the opponent's foul on this player)"),
    ("技術犯規", "foul_technical", "technical foul"),
    ("違反運動道德犯規", "foul_unsportsmanlike", "unsportsmanlike foul"),
    ("犯規disqualifying", "foul_disqualifying", "disqualifying foul"),
    ("犯規coachTechnical", "foul_coach_technical", "technical foul on the coach"),
    ("犯規benchTechnical", "foul_bench_technical", "technical foul on the bench"),
    ("犯規", "foul", "foul, kind not recorded (team row)"),
    ("暫停", "timeout", "timeout (a team row)"),
    (
        "videoReview, coachChallenge,",
        "review_coach_challenge",
        "video review: coach's challenge (2025-26 onward; the printed text ends with a bare comma)",
    ),
    (
        "videoReview, instantReplaySystem,",
        "review_instant_replay",
        "video review: instant-replay system, started by the officials (2025-26 onward)",
    ),
    (
        "videoReview, instantReplaySystem, 1",
        "review_instant_replay_1",
        "video review: instant-replay system, variant printed with a trailing '1' (meaning unexplained; the row "
        "also prints the running score and the points tag '+videoReview')",
    ),
    ("替換上場", "sub_in", "substitution: enters the game"),
    ("下場休息", "sub_out", "substitution: leaves the game"),
    ("跳球 贏", "jump_ball_won", "jump ball won"),
    ("跳球 輸", "jump_ball_lost", "jump ball lost"),
]
_FT_COUNT = {"": "", "得到一次罰球 ": "1", "得到兩次罰球 ": "2", "得到三次罰球 ": "3"}
_FT_NTH = {"第一罰": 1, "第二罰": 2, "第三罰": 3}
_FT_OUT = {"罰進": ("ft_made", "made"), "不進": ("ft_missed", "missed")}

#: phrase -> events over the 404 finished 2022-23..2025-26 games (258,183 events); '' = a row with no action text.
OBSERVED_COUNTS: Dict[str, int] = {
    "搶到防守籃板": 29537,
    "下場休息": 25532,
    "替換上場": 25505,
    "三分球不進": 17046,
    "造成對手犯規": 17035,
    "助攻": 17020,
    "個人犯規": 15745,
    "搶到進攻籃板": 12379,
    "三分球得分": 7817,
    "抄截": 7361,
    "傳球失誤": 6325,
    "第二罰 罰進": 5798,
    "得到兩次罰球 第一罰 罰進": 5187,
    "上籃得分": 4428,
    "突破上籃得分": 4281,
    "突破上籃不進": 3961,
    "跳投不進": 3606,
    "控球失誤": 3547,
    "上籃不進": 3403,
    "阻攻": 3181,
    "拋投不進": 3052,
    "暫停": 2854,
    "得到兩次罰球 第一罰 不進": 2736,
    "第二罰 不進": 2319,
    "補籃得分": 2138,
    "拋投得分": 1920,
    "跳投得分": 1657,
    "急停跳投不進": 1564,
    "補籃不進": 1539,
    "得到一次罰球 第一罰 罰進": 1326,
    "灌籃得分": 1316,
    "轉身跳投不進": 1158,
    "後仰跳投不進": 1121,
    "進攻犯規": 1024,
    "進攻失誤": 1007,
    "急停跳投得分": 1004,
    "三分外線急停跳投不進": 922,
    "勾射不進": 773,
    "帶球走步": 718,
    "跳球 贏": 708,
    "轉身跳投得分": 672,
    "得到一次罰球 第一罰 不進": 669,
    "勾射得分": 610,
    "後仰跳投得分": 583,
    "三分外線後撤步投籃不進": 559,
    "shotClock 失誤": 489,
    "三分外線急停跳投得分": 480,
    "球出界": 441,
    "後撤步投籃不進": 410,
    "跳球 輸": 404,
    "違反運動道德犯規": 261,
    "技術犯規": 210,
    "3Sec 失誤": 249,
    "三分外線後撤步投籃得分": 245,
    "後撤步投籃得分": 243,
    "alleyOop得分": 205,
    "": 169,
    "不進": 168,
    "灌籃不進": 162,
    "得分": 158,
    "第三罰 罰進": 149,
    "得到三次罰球 第一罰 罰進": 128,
    "backCourt 失誤": 96,
    "三分外線後仰跳投不進": 92,
    "videoReview, coachChallenge,": 82,
    "alleyOop不進": 81,
    "犯規coachTechnical": 75,
    "videoReview, instantReplaySystem,": 67,
    "得到三次罰球 第一罰 不進": 67,
    "doubleDribble 失誤": 53,
    "其他失誤": 51,
    "三分外線後仰跳投得分": 48,
    "carrying 失誤": 47,
    "第三罰 不進": 46,
    "5Sec 失誤": 40,
    "犯規benchTechnical": 35,
    "8Sec 失誤": 26,
    "三分外線轉身跳投不進": 23,
    "犯規": 18,
    "三分外線轉身跳投得分": 10,
    "videoReview, instantReplaySystem, 1": 6,
    "offensiveGoalTending 失誤": 4,
    "犯規disqualifying": 2,
}


def _actions() -> Dict[str, Tuple[str, str]]:
    out: Dict[str, Tuple[str, str]] = {}
    for stems, tag, label in ((_SHOT_STYLES, "fg2", "2-point "), (_THREE_STYLES, "fg3", "3-point ")):
        for z, c, e in stems:
            for oz, oc, oe in _OUTCOMES:
                out[z + oz] = (f"{tag}_{c}_{oc}", f"{label}{e}, {oe}")
    out.update({z: (c, e) for z, c, e in _FIXED})
    for pre, n in _FT_COUNT.items():
        for nth, k in _FT_NTH.items():
            for oz, (oc, oe) in _FT_OUT.items():
                out[f"{pre}{nth} {oz}"] = (oc, f"free throw {k}" + (f" of {n} awarded" if n else "") + f", {oe}")
    # Only phrases seen in the corpus: the generators above describe the SHAPE of the vocabulary, but a cross-product
    # holds impossible rows (3 free throws 'of 1'). An unseen phrase is refused, so vocabulary drift stays loud.
    return {z: v for z, v in out.items() if z in OBSERVED_COUNTS}


#: The observed vocabulary: Traditional Chinese phrase -> (action code, English meaning).
ACTIONS: Dict[str, Tuple[str, str]] = _actions()
UNLABELLED = (
    "unlabelled",
    "row has a player but no action text; in the corpus a shot with x/y followed by a rebound, i.e. a missed shot "
    "(inferred)",
)
_FT_PHRASE = re.compile(r"(?:得到(.)次罰球 )?第(.)罰 (罰進|不進)")
_FT_AWARDED = {"一": 1, "兩": 2, "三": 3}  # 得到N次罰球
_FT_ORDINAL = {"一": 1, "二": 2, "三": 3}  # 第N罰


def action_table() -> List[Tuple[str, str, str, int]]:
    """The observed play-by-play phrase vocabulary, for the docs table.

    Returns:
        ``(action_zh, action, meaning, events)`` for every observed phrase, ordered by code; the row with an
        empty ``action_zh`` is the unlabelled action.
    """
    rows = [(z, c, e, OBSERVED_COUNTS[z]) for z, (c, e) in ACTIONS.items()]
    rows.append(("", UNLABELLED[0], UNLABELLED[1], OBSERVED_COUNTS[""]))
    return sorted(rows, key=lambda r: (r[1], r[0]))


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

    def text(self) -> str:
        parts: List[str] = []
        stack = list(reversed(self.children))
        while stack:
            node = stack.pop()
            if isinstance(node, _Node):
                if node.tag in ("script", "style"):
                    continue
                if node.tag == "br":
                    parts.append(" ")
                stack.extend(reversed(node.children))
            else:
                parts.append(node)
        return re.sub(r"\s+", " ", "".join(parts).replace("\xa0", " ")).strip()

    def classes(self) -> set:
        return set((self.attrs.get("class") or "").split())

    def find(self, tag: Optional[str] = None, cls: Optional[str] = None, **attrs: str) -> Optional["_Node"]:
        return next(self.findall(tag, cls, **attrs), None)

    def findall(self, tag: Optional[str] = None, cls: Optional[str] = None, **attrs: str) -> Iterator["_Node"]:
        want = set(cls.split()) if cls else set()
        for n in self.iter():
            if tag and n.tag != tag:
                continue
            if want and not want <= n.classes():
                continue
            if any(n.attrs.get(k) != v for k, v in attrs.items()):
                continue
            yield n

    def kids(self, tag: Optional[str] = None) -> List["_Node"]:
        return [c for c in self.children if isinstance(c, _Node) and (tag is None or c.tag == tag)]


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


# --------------------------------------------------------------------------------------- small helpers
def _int_text(s: Optional[str]) -> Optional[int]:
    s = (s or "").strip().replace(",", "")
    return int(s) if re.fullmatch(r"[+-]?\d+", s) else None


def _num_text(s: Optional[str]) -> Optional[float]:
    s = (s or "").strip().replace(",", "").rstrip("%")
    return float(s) if re.fullmatch(r"[+-]?\d+(\.\d+)?", s) else None


def _flt_text(s: Optional[str]) -> Optional[float]:
    s = (s or "").strip()
    return float(s) if re.fullmatch(r"-?\d+(\.\d+)?", s) else None


def _same(a: Any, b: Any) -> bool:
    """Equality that also requires equal types: ``True != 1``, ``"5" != 5``, ``1 != 1.0``."""
    return type(a) is type(b) and a == b


def _check(what: str, got: Any, want: Any) -> None:
    if not _same(got, want):
        raise _NotParsed(f"out of scope: {what} {got!r}, asked for {want!r}")


def _need(node: Any, what: str) -> Any:
    if node is None:
        raise _NotParsed(f"layout drift: {what} not found")
    return node


def season_end_year(season: str) -> int:
    """The sdv END year of a season label.

    Args:
        season: the site's label such as ``"2024-25"``.

    Returns:
        The END year (``"2024-25"`` -> 2025).

    Raises:
        ValueError: ``season`` does not start with a four-digit year.
    """
    return int(season[:4]) + 1


# --------------------------------------------------------------------------------------- schedule
_TITLE = re.compile(r"^(\d{4}-\d{2})-(熱身賽|例行賽|季後賽|總冠軍賽)賽程")
_SEATS = re.compile(r"([\d,]+)\s*/\s*([\d,]+)")
_PHASES = ("before-today", "is-today", "is-future")


def _schedule_record(html: str, stage: str, season: str) -> Dict[str, Any]:
    root = _dom(html.replace("<br />", " ").replace("<br>", " "))
    title = _need(root.find("title"), "title").text()
    m = _TITLE.match(title)
    if not m:
        raise _NotParsed(f"layout drift: schedule title {title!r}")
    _check("season", m.group(1), season)
    _check("stage", m.group(2), STAGES[stage][0])
    rows = list(root.findall("div", "match_row justify-content-center"))
    if not rows:
        raise _NotParsed("zero games on the schedule page (a stage this season does not have)", empty=True)
    games: List[Dict[str, Any]] = []
    months: set = set()
    for row in rows:
        cls = row.classes()
        month = next((c[2:] for c in cls if re.fullmatch(r"d-\d{4}-\d{2}", c)), None)
        phase = next((c for c in _PHASES if c in cls), None)
        ids = [int(c[5:]) for c in cls if re.fullmatch(r"team-\d+", c)]
        home_ids = [int(c[10:]) for c in cls if re.fullmatch(r"team-home-\d+", c)]
        if month is None or phase is None or len(ids) != 2 or len(home_ids) != 1 or home_ids[0] not in ids:
            raise _NotParsed(f"layout drift: match row classes {sorted(cls)}")
        months.add(month)
        home_id = home_ids[0]
        away_id = next(i for i in ids if i != home_id)
        dt = _need(row.find("div", "match_row_datetime"), "datetime")
        parts = [h.text() for h in dt.iter() if h.tag in ("h5", "h6") and h.text()]
        star = dt.find("i", "czi-star-filled") is not None
        if len(parts) != 3:
            raise _NotParsed(f"layout drift: datetime block {parts}")
        md, wd, tm = parts
        mm = re.fullmatch(r"(\d\d)/(\d\d)", md)
        wm = re.fullmatch(r"\((.)\)", wd)
        if not (mm and wm and re.fullmatch(r"\d\d:\d\d", tm)):
            raise _NotParsed(f"layout drift: datetime {md!r} {wd!r} {tm!r}")
        if int(month[5:7]) != int(mm.group(1)):
            raise _NotParsed(f"layout drift: month class {month} vs date {md}")
        cols = [c for c in row.findall("div") if {"col-lg-3"} <= c.classes()]
        if len(cols) != 2:
            raise _NotParsed("layout drift: team columns")
        teams = []
        for col, label in zip(cols, ("客隊", "主隊")):
            if label not in col.text():
                raise _NotParsed(f"layout drift: team column lacks {label}")
            zh = _need(col.find("span", "PC_only fs14"), "team name").text()
            en = _need(col.find("span", "fs12 PC_only"), "team English name").text()
            logo = _need(col.find("img"), "team logo").attrs.get("src")
            teams.append((zh, en, logo))
        info = _need(row.find("div", "col-lg-4"), "score column")
        scores = [h.text() for h in info.findall("h6") if "PC_only" in h.classes() and "ff8bit" in h.classes()]
        if len(scores) != 2 or _int_text(scores[0]) is None or _int_text(scores[1]) is None:
            raise _NotParsed(f"layout drift: scores {scores}")
        label = _need(info.find("h5", "fs14"), "game label").text()
        venue = _need(info.find("h5", "fs12 mb-0"), "venue").text()
        seats = _SEATS.search(info.text())
        link = [a.attrs["href"] for a in row.findall("a") if re.fullmatch(r"/game/\d+", a.attrs.get("href", ""))]
        track = re.search(r"trackGame\('(\d+)'", json.dumps([a.attrs.get("onclick") for a in row.findall("a")]))
        if len(link) != 1 or not track or int(link[0][6:]) != int(track.group(1)):
            raise _NotParsed(f"layout drift: game link {link} vs trackGame")
        games.append(
            {
                "gid": int(link[0][6:]),
                "date": f"{int(month[:4])}-{mm.group(1)}-{mm.group(2)}",
                "weekday_zh": wm.group(1),
                "time": tm,
                "starred": star,
                "game_label": label,
                "venue_zh": venue,
                "attendance": _int_text(seats.group(1)) if seats else None,
                "capacity": _int_text(seats.group(2)) if seats else None,
                "phase": phase.replace("-", "_"),
                "away_team_id": away_id,
                "away_name_zh": teams[0][0],
                "away_name_en": teams[0][1],
                "away_logo": teams[0][2],
                "away_score": int(scores[0]),
                "home_team_id": home_id,
                "home_name_zh": teams[1][0],
                "home_name_en": teams[1][1],
                "home_logo": teams[1][2],
                "home_score": int(scores[1]),
            }
        )
    gids = [g["gid"] for g in games]
    if len(set(gids)) != len(gids):
        raise _NotParsed("duplicate game ids on the schedule page")
    shown = {b.attrs.get("data-val") for b in root.findall("button", "month-btn")} - {"all", None}
    shown_sorted = sorted(str(x) for x in shown)
    if shown != months:  # every month the page offers has its rows: the list is whole
        raise _NotParsed(f"month buttons {shown_sorted} vs months with games {sorted(months)}")
    games.sort(key=lambda g: g["gid"])
    return {
        "season": season,
        "season_sdv": season_end_year(season),
        "stage": stage,
        "stage_zh": STAGES[stage][0],
        "game_count": len(games),
        "games": games,
    }


# --------------------------------------------------------------------------------------- game page
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_QUARTER_ID = re.compile(r"(q[1-4]|ot[1-9])_(away|home)")


def _game_record(html: str, gid: int) -> Dict[str, Any]:
    m = re.search(r'window\.gid\s*=\s*"(\d+)"', html)
    if not m:
        raise _NotParsed("layout drift: window.gid not found")
    _check("gid", int(m.group(1)), gid)
    u = re.findall(r"getPlayByPlay\('([^']*)'\)", html)
    if len(u) != 1:
        raise _NotParsed(f"layout drift: getPlayByPlay calls {len(u)}")
    for other in re.findall(r"(?:fetchChartData|fetchData)\('([^']*)'\)", html):
        if other != u[0]:
            raise _NotParsed("layout drift: the page's uuids disagree")
    if u[0] and not _UUID.fullmatch(u[0]):
        raise _NotParsed(f"uuid not a uuid: {u[0]!r}")
    root = _dom(html)
    head = _need(root.find("div", "card-header game_header"), "game header")
    htext = head.text()
    dm = re.search(r"(\d{4}-\d{2}-\d{2}) (\d\d:\d\d)", htext)
    venue = next((a.text() for a in head.findall("a") if "google.com/maps" in a.attrs.get("href", "")), None)
    label = next((s.text() for s in head.findall("span") if "fs16" in s.classes()), None)
    if not (dm and venue and label):
        raise _NotParsed("layout drift: game header date / venue / label")
    badge = _need(root.find("span", "badge badge-secondary"), "status badge").text()
    sides: Dict[str, Tuple[str, str, Any, int]] = {}
    for side, zh in (("away", "客隊"), ("home", "主隊")):
        tag = next((s for s in root.findall("span", "fs10") if s.text() == zh), None)
        col = _need(tag, f"{zh} label").parent
        score = _need(col.find("h6", f"score_{side}"), f"{side} score").text()
        name = _need(
            next(
                (
                    h
                    for h in col.findall("h6")
                    if "fs14" in h.classes() and "mt-2" in h.classes() and "pb-0" in h.classes()
                ),
                None,
            ),
            f"{side} name",
        ).text()
        en = _need(col.find("small"), f"{side} English name").text()
        logo = _need(col.parent.find("img"), f"{side} logo").attrs.get("src")
        if _int_text(score) is None:
            raise _NotParsed(f"layout drift: {side} score {score!r}")
        sides[side] = (name, en, logo, int(score))
    quarters: Dict[str, int] = {}
    for td in root.findall("td"):
        i = td.attrs.get("id") or ""
        if _QUARTER_ID.fullmatch(i):
            if _int_text(td.text()) is None:
                raise _NotParsed(f"layout drift: {i} {td.text()!r}")
            quarters[i] = int(td.text())
    if not all(f"q{q}_{s}" in quarters for q in (1, 2, 3, 4) for s in ("away", "home")):
        raise _NotParsed("layout drift: quarter score cells")
    officials: List[Dict[str, str]] = []
    box = root.find("div", "container text-center fs12 text-light my-3")
    if box is not None:
        role = None
        for c in box.children:
            if isinstance(c, _Node) and c.tag == "b":
                role = c.text()
            elif isinstance(c, str) and c.strip() and role:
                officials.append({"role_zh": role, "name_zh": c.strip()})
                role = None
    return {
        "gid": gid,
        "uuid": u[0] or None,
        "status_zh": badge,
        "date": dm.group(1),
        "time": dm.group(2),
        "venue_zh": venue,
        "game_label_zh": label,
        "away_name_zh": sides["away"][0],
        "away_name_en": sides["away"][1] or None,
        "away_logo": sides["away"][2],
        "away_score": sides["away"][3],
        "home_name_zh": sides["home"][0],
        "home_name_en": sides["home"][1] or None,
        "home_logo": sides["home"][2],
        "home_score": sides["home"][3],
        "quarters": quarters,
        "officials": officials,
    }


# --------------------------------------------------------------------------------------- play-by-play fragment
_PERIOD = re.compile(r"(Q[1-4]|OT[1-9])-tr")
_SCORE = re.compile(r"(\d+) - (\d+)")


def _split_text(text: str, has_xy: bool) -> Tuple[Optional[str], Optional[str], str]:
    """``'#35 辛巴 灌籃得分'`` -> ``('35', '辛巴', '灌籃得分')``; the action phrase is the longest known suffix."""
    t = text.strip()
    best = max((a for a in ACTIONS if t.endswith(a)), key=len, default=None)
    if best is None:
        if has_xy and re.fullmatch(r"#\S+ \S+|\S+", t):  # a player and coordinates but no action text
            if t.startswith("#"):
                num, _, name = t[1:].partition(" ")
                return num, name, ""
            return None, t, ""
        raise _NotParsed(f"unknown play-by-play phrase: {text!r}")
    who = t[: len(t) - len(best)].strip()
    jersey: Optional[str] = None
    player: Optional[str] = None
    if who:
        mm = re.fullmatch(r"#(\S+)\s+(.+)", who)
        jersey, player = (mm.group(1), mm.group(2)) if mm else (None, who)
    return jersey, player, best


def _video(raw: str) -> Tuple[Optional[Dict[str, Any]], bool]:
    if not raw:
        return None, False
    try:
        d = json.loads(base64.b64decode(raw, validate=True))
    except (binascii.Error, ValueError):
        raise _NotParsed("layout drift: data-tryspot is not base64 JSON") from None
    if not isinstance(d, dict):
        raise _NotParsed("layout drift: data-tryspot is not an object")
    return (
        {  # the clip URLs carry a vendor access key and are never kept
            "video_uuid": d.get("uuid"),
            "synergy_play_id": d.get("synergy_play_id"),
            "stat_category": d.get("stat_category"),
            "tryspot_player_id": d.get("player_id"),
            "created_at": d.get("created_at"),
        },
        bool(d.get("starred")),
    )


def pbp_record(body: Any) -> Dict[str, Any]:
    """The play-by-play JSON body ``{error, data: '<table ...>'}`` -> ``{periods, final_score_*, events: [...]}``.

    Args:
        body: the decoded play-by-play body.

    Returns:
        ``{"periods": [...], "final_score_away": int, "final_score_home": int, "events": [...]}``.

    Raises:
        _NotParsed: the body is not a play-by-play table, the layout drifted, a phrase is not in the observed
            vocabulary, or the list is empty or truncated (no period row or no closing row).
    """
    html = body.get("data") if isinstance(body, dict) else None
    if not isinstance(html, str) or not html.strip():
        raise _NotParsed("play-by-play data is not an HTML string")
    root = _dom(html)
    table = _need(root.find("table", "pbplay"), "pbplay table")
    events: List[Dict[str, Any]] = []
    periods: List[str] = []
    final: Optional[Tuple[int, int]] = None
    seq = 0
    for tr in table.findall("tr"):
        cls = tr.classes()
        if "quater-tr" in cls:
            label = _need(tr.find("span", "quater"), "period label").text()
            if "all-tr" in cls:
                sc = _SCORE.fullmatch(_need(tr.find("span", "match_score"), "final score").text())
                if label != "Final" or not sc:
                    raise _NotParsed(f"layout drift: closing row {label!r}")
                final = (int(sc.group(1)), int(sc.group(2)))
            else:
                if {c[:-3] for c in cls if _PERIOD.fullmatch(c)} != {label}:
                    raise _NotParsed(f"layout drift: period row {label!r} {sorted(cls)}")
                periods.append(label)
            continue
        per = [c[:-3] for c in cls if _PERIOD.fullmatch(c)]
        if len(per) != 1 or per[0] not in periods:
            raise _NotParsed(f"layout drift: event row in period {per} (headers {periods})")
        tds = tr.kids("td")
        if len(tds) != 3:
            raise _NotParsed("layout drift: event row cells")
        side_td = [
            (td, "away" if "bar_away" in td.classes() else "home")
            for td in tds
            if {"bar_away"} & td.classes() or {"bar_home"} & td.classes()
        ]
        if len(side_td) != 1:
            raise _NotParsed("layout drift: event row has no single side cell")
        td, side = side_td[0]
        clock = _need(tr.find("span", "countdown"), "countdown").text()
        if not re.fullmatch(r"\d\d:\d\d", clock):
            raise _NotParsed(f"layout drift: clock {clock!r}")
        sc = _SCORE.fullmatch(_need(tr.find("span", "match_score"), "match score").text())
        txt = _need(td.find("b", "pbp_detail_text"), "event text")
        act = _need(td.find("b", "pbp_detail_action"), "event points").text()
        jersey, player, phrase = _split_text(txt.text(), bool(txt.attrs.get("data-x")))
        video, starred = _video(td.attrs.get("data-tryspot", ""))
        code = ACTIONS[phrase][0] if phrase else UNLABELLED[0]
        ftm = _FT_PHRASE.fullmatch(phrase)
        seq += 1
        events.append(
            {
                "seq": seq,
                "period": per[0],
                "clock": clock,
                "side": side,
                "jersey": jersey,
                "player_zh": player,
                "action": code,
                "action_zh": phrase,
                "text_zh": txt.text(),
                "ft_number": _FT_ORDINAL[ftm.group(2)] if ftm else None,
                "ft_awarded": _FT_AWARDED.get(ftm.group(1)) if ftm and ftm.group(1) else None,
                "points": _int_text(act.lstrip("+")) if act.startswith("+") and act != "+" else None,
                "score_away": int(sc.group(1)) if sc else None,
                "score_home": int(sc.group(2)) if sc else None,
                "x": _flt_text(txt.attrs.get("data-x")),
                "y": _flt_text(txt.attrs.get("data-y")),
                "nice_play": bool(starred or any(c.startswith("nice_play") for c in cls)),
                "video": video,
            }
        )
    if not events or final is None or not periods:
        raise _NotParsed("play-by-play has no events, no period rows or no closing row (truncated or empty)")
    return {
        "period_count": len(periods),
        "periods": periods,
        "final_score_away": final[0],
        "final_score_home": final[1],
        "event_count": len(events),
        "events": events,
    }


# --------------------------------------------------------------------------------------- standings
_STANDINGS_HEADER = [
    "排行",
    "球隊",
    "已賽 GP",
    "勝 W",
    "敗 L",
    "勝率 PCT",
    "勝差",
    "平均得分",
    "平均失分",
    "平均分差",
    "主場",
    "客場",
    "連勝連敗",
    "近10場比賽",
]
_STANDINGS_TITLE = re.compile(r"^(\d{4}-\d{2}) 戰績")
_LAST_N = re.compile(r"近(\d+)場比賽")


def _standings_record(html: str, season: str) -> Dict[str, Any]:
    root = _dom(html)
    m = _STANDINGS_TITLE.match(_need(root.find("title"), "title").text())
    if not m:
        raise _NotParsed("layout drift: standings title")
    _check("season", m.group(1), season)
    table = _need(root.find("table", "team_ranking"), "standings table")
    head = [th.text() for th in _need(table.find("thead"), "thead").findall("th")]
    if head != _STANDINGS_HEADER:
        raise _NotParsed(f"layout drift: standings header {head}")
    rows = []
    for tr in _need(table.find("tbody"), "tbody").findall("tr"):
        team, tds = tr.kids("th"), tr.kids("td")
        cells = [c.text() for c in tds]
        a = _need(tr.find("a"), "team link")
        tid = re.fullmatch(r"/team/(\d+)", a.attrs.get("href", ""))
        if not tid or len(team) != 2 or len(cells) != 12 or _int_text(team[0].text()) is None:
            raise _NotParsed("layout drift: standings row")
        gp, w, ls, pct, gb, pf, pa, diff, home, away, streak, last = cells
        img = a.find("img")
        rows.append(
            {
                "rank": int(team[0].text()),
                "team_id": int(tid.group(1)),
                "team_zh": a.text(),
                "team_logo": img.attrs.get("src") if img is not None else None,
                "games_played": _int_text(gp),
                "wins": _int_text(w),
                "losses": _int_text(ls),
                "win_pct": _num_text(pct),
                "games_behind": _num_text(gb),
                "points_for_avg": _num_text(pf),
                "points_against_avg": _num_text(pa),
                "point_diff_avg": _num_text(diff),
                "home_record": home,
                "away_record": away,
                "streak": streak,
                "last_n_record": last,
            }
        )
    if not rows:
        raise _NotParsed("zero standings rows", empty=True)
    last_n = _LAST_N.search(head[-1])
    return {
        "season": season,
        "season_sdv": season_end_year(season),
        "last_n": int(last_n.group(1)) if last_n else None,
        "team_count": len(rows),
        "teams": rows,
    }


# --------------------------------------------------------------------------------------- player season table
_STAT_HEADER = [
    "球員",
    "背號",
    "球隊",
    "出賽",
    "上場時間",
    "兩分命中",
    "兩分出手",
    "兩分命中率",
    "三分命中",
    "三分出手",
    "三分命中率",
    "罰球命中",
    "罰球出手",
    "罰球命中率",
    "得分",
    "攻板",
    "防板",
    "籃板",
    "助攻",
    "抄截",
    "阻攻",
    "失誤",
    "犯規",
]
_STAT_KEYS = [
    "jersey",
    "team_zh",
    "games",
    "minutes",
    "fg2_made",
    "fg2_att",
    "fg2_pct",
    "fg3_made",
    "fg3_att",
    "fg3_pct",
    "ft_made",
    "ft_att",
    "ft_pct",
    "points",
    "reb_off",
    "reb_def",
    "reb",
    "ast",
    "stl",
    "blk",
    "tov",
    "pf",
]
_SINGLE = frozenset({"games", "fg2_pct", "fg3_pct", "ft_pct"})
_STAT_TITLE = re.compile(r"^(\d{4}-\d{2}) 球員數據")


def _stat_player_record(html: str, season: str, stage_sn: int) -> Dict[str, Any]:
    root = _dom(html)
    m = _STAT_TITLE.match(_need(root.find("title"), "title").text())
    if not m:
        raise _NotParsed("layout drift: stat-player title")
    _check("season", m.group(1), season)
    sel = {
        s.attrs.get("id"): [o.attrs.get("value") for o in s.findall("option") if "selected" in o.attrs]
        for s in root.findall("select")
    }
    _check("season select", sel.get("season_name"), [season])
    _check("stage select", sel.get("stage_sn"), [str(stage_sn)])
    table = _need(root.find("table", id="main-table"), "main-table")
    head = [re.sub(r"\s+", "", th.text()) for th in _need(table.find("thead"), "thead").findall("th")]
    if head != _STAT_HEADER:
        raise _NotParsed(f"layout drift: stat-player header {head}")
    players = []
    for tr in _need(table.find("tbody"), "tbody").findall("tr"):
        th = _need(tr.find("th"), "player cell")
        a = _need(th.find("a"), "player link")
        pid = re.fullmatch(r"/player/([A-Za-z0-9]+)", a.attrs.get("href", ""))
        tds = tr.kids("td")
        if not pid or len(tds) != len(_STAT_KEYS):
            raise _NotParsed("layout drift: stat-player row")
        rec: Dict[str, Any] = {"name_zh": a.text(), "player_page_id": pid.group(1)}
        for key, td in zip(_STAT_KEYS, tds):
            avg, tot = td.attrs.get("data-avg"), td.attrs.get("data-total")
            if key in ("jersey", "team_zh"):
                rec[key] = td.text()
            elif key in _SINGLE:  # a count of games or a percentage: avg and total are the same cell
                rec[key] = _int_text(avg) if key == "games" else _num_text(avg)
                if avg != tot:
                    raise _NotParsed(f"layout drift: {key} avg {avg!r} != total {tot!r}")
            else:
                rec[key + "_avg"] = avg if key == "minutes" else _num_text(avg)
                rec[key + "_total"] = (
                    tot if key == "minutes" else _int_text(tot) if re.fullmatch(r"\d+", tot or "") else _num_text(tot)
                )
        players.append(rec)
    if not players:
        raise _NotParsed("zero player rows", empty=True)
    ids = [p["player_page_id"] for p in players]
    if len(set(ids)) != len(ids):
        raise _NotParsed("duplicate player rows")
    return {
        "season": season,
        "season_sdv": season_end_year(season),
        "stage_sn": stage_sn,
        "stage": STAT_STAGES[stage_sn],
        "player_count": len(players),
        "players": players,
    }


# --------------------------------------------------------------------------------------- player page
_SEASON_LINK = re.compile(r"/player/([A-Za-z0-9]+)/(\d{4}-\d{2})")
_PLAYER_COLS = ["games", "minutes", "points", "reb", "ast", "stl", "blk", "fg2", "fg2_pct", "fg3", "fg3_pct"]
_PLAYER_COLS += ["ft", "ft_pct", "tov", "pf"]
_PLAYER_HEAD = ["賽季", "出賽", "上場時間", "得分", "籃板", "助攻", "抄截", "阻攻", "二分", "二分%", "三分", "三分%"]
_PLAYER_HEAD += ["罰球", "罰球%", "失誤", "犯規"]
_BIO = {
    "綽號": "nickname",
    "年資": "tenure",
    "身高": "height",
    "體重": "weight",
    "生日": "birthday",
    "出生地": "birthplace",
    "身份": "status_zh",
}


def _season_rows(table: _Node, pid: str) -> List[Dict[str, Any]]:
    head = [th.text() for th in _need(table.find("thead"), "thead").findall("th")]
    if head != _PLAYER_HEAD:
        raise _NotParsed(f"layout drift: player table header {head}")
    out = []
    for tr in _need(table.find("tbody"), "tbody").findall("tr"):
        tds = tr.kids("td")
        if len(tds) != len(_PLAYER_HEAD):
            raise _NotParsed("layout drift: player season row")
        a = tds[0].find("a")
        if a is not None:
            lm = _SEASON_LINK.fullmatch(a.attrs.get("href", ""))
            if not lm:
                raise _NotParsed("layout drift: season link")
            _check("player page id", lm.group(1), pid)
        c = dict(zip(["season"] + _PLAYER_COLS, [t.text() for t in tds]))
        row: Dict[str, Any] = {"season": c["season"], "games": _int_text(c["games"]), "minutes": c["minutes"]}
        for k in ("points", "reb", "ast", "stl", "blk", "tov", "pf"):
            row[k] = _num_text(c[k])
        for k in ("fg2", "fg3", "ft"):
            pm = re.fullmatch(r"([\d.]+) - ([\d.]+)", c[k])
            if not pm:
                raise _NotParsed(f"layout drift: player {k} cell {c[k]!r}")
            row[k + "_made"], row[k + "_att"], row[k + "_pct"] = (
                float(pm.group(1)),
                float(pm.group(2)),
                _num_text(c[k + "_pct"]),
            )
        out.append(row)
    return out


def _player_record(html: str, pid: str) -> Dict[str, Any]:
    root = _dom(html)
    sec = _need(root.find("section", id="section_player"), "player section")
    track = re.search(r"trackPlayer\('(\d+)'", html)
    name = _need(sec.find("h1", "player_name_ch"), "player name")
    en = _need(sec.find("h3", "player_name_en"), "player English name").text()
    head = _need(sec.find("h5", "text-light mt-2 mb-5"), "team line").text()
    tm = re.fullmatch(r"(.+?) (.+?) \| # (\S+) \| (\S+)", head)
    if not (track and tm):
        raise _NotParsed(f"layout drift: player header {head!r}")
    bio: Dict[str, Optional[str]] = {}
    for sp in _need(sec.find("p", "text-light opacity-9"), "bio").findall("span"):
        k, _, v = sp.text().partition("：")
        if k in _BIO:
            bio[_BIO[k]] = v.strip() or None
    if set(bio) != set(_BIO.values()):
        raise _NotParsed(f"layout drift: bio fields {sorted(bio)}")
    exp = sec.find("div", id="player_experience")
    edu = [s.text() for s in exp.findall("span", "d-block")] if exp else []
    sections: Dict[str, List[_Node]] = {"plg": [], "easl": []}  # the league's tables, then the EASL tables
    cur = None
    for n in root.iter():
        if n.tag == "tbody" and n.attrs.get("id") in ("match-tbody", "easl-match-tbody"):
            cur = "easl" if n.attrs["id"].startswith("easl") else "plg"
        elif (
            n.tag == "table"
            and n.find("thead") is not None
            and [th.text() for th in _need(n.find("thead"), "thead").findall("th")] == _PLAYER_HEAD
        ):
            if cur is None:
                raise _NotParsed("layout drift: a season table before any game-log block")
            sections[cur].append(n)
    links = {m.group(1) for m in _SEASON_LINK.finditer(html)}
    if links != {pid}:
        raise _NotParsed(f"out of scope: season links name {sorted(links)}, asked for {pid!r}")
    if len(sections["plg"]) != 2 or len(sections["easl"]) not in (0, 2):
        raise _NotParsed(f"layout drift: season tables {len(sections['plg'])} + {len(sections['easl'])}")
    return {
        "player_page_id": pid,
        "member_id": int(track.group(1)),
        "name_zh": name.text(),
        "name_en": en,
        "team_zh": tm.group(1),
        "team_en": tm.group(2),
        "jersey": tm.group(3),
        "position": tm.group(4),
        "nickname": bio["nickname"],
        "tenure": bio["tenure"],
        "height_cm": _num_text((bio["height"] or "").replace("cm", "")),
        "weight_kg": _num_text((bio["weight"] or "").replace("kg", "")),
        "birthday": bio["birthday"],
        "birthplace_zh": bio["birthplace"],
        "status_zh": bio["status_zh"],
        "education": [e for e in edu if e],
        "season_totals": _season_rows(sections["plg"][0], pid),
        "season_averages": _season_rows(sections["plg"][1], pid),
        "easl_season_totals": _season_rows(sections["easl"][0], pid) if sections["easl"] else [],
        "easl_season_averages": _season_rows(sections["easl"][1], pid) if sections["easl"] else [],
    }


def record_for(kind: str, html: str, **want: Any) -> Dict[str, Any]:
    """Parse one HTML page into its record and check it against the request.

    Args:
        kind: ``schedule`` (``stage``, ``season`` slug), ``game`` (``gid``), ``standings`` (``season`` slug),
            ``stat_player`` (``season`` slug, ``stage_sn``) or ``player`` (``pid``).
        html: the page text.
        **want: the request's scope, per ``kind``.

    Returns:
        The record (the refs capture shape).

    Raises:
        _NotParsed: layout drift, another scope than asked, or an empty page (``empty=True``).
    """
    try:
        if kind == "schedule":
            return _schedule_record(html, want["stage"], want["season"])
        if kind == "game":
            return _game_record(html, want["gid"])
        if kind == "standings":
            return _standings_record(html, want["season"])
        if kind == "stat_player":
            return _stat_player_record(html, want["season"], want["stage_sn"])
        if kind == "player":
            return _player_record(html, want["pid"])
    except _NotParsed:
        raise
    except (RecursionError, Exception) as exc:  # an odd page is a refusal, never a crash
        raise _NotParsed(f"parser error: {type(exc).__name__}: {exc}") from None
    raise _NotParsed(f"unknown page kind {kind!r}")


# --------------------------------------------------------------------------------------- scope checks
_BOX_KEYS = {
    "q1_home",
    "q1_away",
    "q2_home",
    "q2_away",
    "q3_home",
    "q3_away",
    "q4_home",
    "q4_away",
    "ot1_home",
    "ot1_away",
    "ot2_home",
    "ot2_away",
    "score_home",
    "score_away",
    "home",
    "away",
    "home_total",
    "away_total",
}
#: Points a box may leave off its player rows (gid 213 is 2 short).
MAX_UNCREDITED = 3


def _score(v: Any) -> Optional[int]:
    """Scores are ints or digit strings (``'86'`` on 2020-21); anything else is None."""
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    return int(v) if isinstance(v, str) and re.fullmatch(r"\d+", v) else None


def game_error(game: Dict[str, Any], gid: int) -> Optional[str]:
    """Why the game page record is not a finished game ``gid``.

    Args:
        game: the parsed game-page record.
        gid: the game id the caller asked for.

    Returns:
        A reason string, or ``None`` when the page is that game and it is final.
    """
    if not _same(game.get("gid"), gid):
        return f"game page gid {game.get('gid')!r}, asked for {gid!r}"
    if game.get("status_zh") != FINAL:
        return f"game {gid} is not final (status {game.get('status_zh')!r}): only finished games are served"
    return None


def box_error(body: Any, game: Dict[str, Any]) -> Optional[str]:
    """Whether ``body`` is the final box of the game whose page record is ``game``.

    The body echoes no game id, so identity is the scores: both finals, every quarter, and the player points
    summing to the team total (within :data:`MAX_UNCREDITED`).

    Args:
        body: the decoded box-score body.
        game: the parsed game-page record of the game asked for.

    Returns:
        A reason string, or ``None`` when the body is that game's final box.
    """
    if not isinstance(body, dict) or body.get("error") not in ("", None):
        return f"box error field {body.get('error') if isinstance(body, dict) else body!r}"
    d = body.get("data")
    if not isinstance(d, dict):
        return "box data is not an object"
    missing = _BOX_KEYS - set(d)
    if missing:
        return f"box body lacks {sorted(missing)}"
    if not d["home"] or not d["away"]:
        return "box has no player rows (unknown or unplayed game id)"
    for side in ("home", "away"):
        final = _score(d[f"score_{side}"])
        if final is None or final != game[f"{side}_score"]:
            return f"box {side} score {d[f'score_{side}']!r} != game page {game[f'{side}_score']}"
        for q in (1, 2, 3, 4):
            if _score(d[f"q{q}_{side}"]) != game["quarters"][f"q{q}_{side}"]:
                return f"box q{q}_{side} {d[f'q{q}_{side}']!r} != game page {game['quarters'][f'q{q}_{side}']}"
        for ot in ("ot1", "ot2"):
            page, got = game["quarters"].get(f"{ot}_{side}"), d[f"{ot}_{side}"]
            if (_score(got) or 0) != (page or 0) or (page is None and got not in ("-", 0, "0", None)):
                return f"box {ot}_{side} {got!r} != game page {page!r}"
        pts = sum(_score(p.get("points")) or 0 for p in d[side])
        tot = _score((d.get(f"{side}_total") or {}).get("points"))
        # The total row is the team's final. The player rows may fall a little short (gid 213: 80 of 82): points the
        # site does not credit to a player. A body whose rows are blank (an unknown tab or id) sums to 0, far below.
        if tot != final or not final - MAX_UNCREDITED <= pts <= final:
            return f"box {side} player points {pts} / total row {tot} != final {final}"
    return None


def expected_periods(game: Dict[str, Any]) -> List[str]:
    """The period labels a game page implies.

    Args:
        game: the parsed game-page record (its ``quarters`` cells).

    Returns:
        ``["Q1", "Q2", "Q3", "Q4"]`` plus one ``OT`` label per overtime cell the page shows with points.
    """
    ot = sorted({k[:3].upper() for k, v in game["quarters"].items() if k.startswith("ot") and v})
    return ["Q1", "Q2", "Q3", "Q4"] + ot


def pbp_error(rec: Dict[str, Any], game: Dict[str, Any]) -> Optional[str]:
    """Whether the parsed play-by-play closes on the page's final score with the page's periods.

    Args:
        rec: the record :func:`pbp_record` returned.
        game: the parsed game-page record.

    Returns:
        A reason string, or ``None`` when the final score and the periods agree with the page.
    """
    if (rec["final_score_away"], rec["final_score_home"]) != (game["away_score"], game["home_score"]):
        return (
            f"play-by-play final {rec['final_score_away']}-{rec['final_score_home']} != game page "
            f"{game['away_score']}-{game['home_score']}"
        )
    if rec["periods"] != expected_periods(game):
        return f"play-by-play periods {rec['periods']} != game page {expected_periods(game)}"
    last = next((e for e in reversed(rec["events"]) if e["score_away"] is not None), None)
    if last is None or (last["score_away"], last["score_home"]) != (game["away_score"], game["home_score"]):
        return "the last scoring event does not reach the final score"
    return None


# --------------------------------------------------------------------------------------- frames
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


def _i(v: Any) -> Optional[int]:
    """An int from an int or a signed digit string; ``''``, bools and anything else are null."""
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v) if v == int(v) else None
    if isinstance(v, str) and re.fullmatch(r"\s*[+-]?\d+\s*", v):
        return int(v)
    return None


def _f(v: Any) -> Optional[float]:
    """A float from a number or a numeric string; ``''``, bools and anything else are null."""
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str) and re.fullmatch(r"\s*[+-]?\d+(\.\d+)?\s*", v):
        return float(v)
    return None


def _s(v: Any) -> Optional[str]:
    """A non-blank string (stripped), else null."""
    return v.strip() or None if isinstance(v, str) else None


def _date(v: Any) -> Optional[date]:
    try:
        return date.fromisoformat(v) if isinstance(v, str) else None
    except ValueError:
        return None


def _seconds(v: Any) -> Optional[int]:
    """``"08:34"`` -> 514 (also ``"240:00"``); None for anything that is not ``mm:ss``."""
    m = re.fullmatch(r"\s*(\d+):(\d{2})\s*", v) if isinstance(v, str) else None
    return int(m.group(1)) * 60 + int(m.group(2)) if m else None


def _obj(v: Any) -> Dict[str, Any]:
    """``v`` when it is a dict, else an empty dict (so chained ``.get`` never meets ``None``)."""
    return v if isinstance(v, dict) else {}


def _dicts(v: Any) -> List[Dict[str, Any]]:
    return [r for r in v if isinstance(r, dict)] if isinstance(v, list) else []


def _rec(raw: Any) -> Dict[str, Any]:
    return raw if isinstance(raw, dict) else {}


def _gid(raw: Any) -> Optional[int]:
    """The game id: the record's own ``gid`` (game page) or the request stamp (box, play-by-play)."""
    r = _rec(raw)
    got = r.get("gid")
    if _i(got) is None:
        got = _rec(r.get(_REQUEST)).get("gid")
    return _i(got)


# -------------------------------------------------------------------------------------- schedule
_SCHEDULE_SPEC: Spec = {
    "season": INT,
    "season_slug": S,
    "stage": S,
    "gid": INT,
    "date": D,
    "weekday_zh": S,
    "time": S,
    "starred": B,
    "game_label": S,
    "venue_zh": S,
    "attendance": INT,
    "capacity": INT,
    "phase": S,
    "away_team_id": INT,
    "away_name_zh": S,
    "away_name_en": S,
    "away_logo": S,
    "away_score": INT,
    "home_team_id": INT,
    "home_name_zh": S,
    "home_name_en": S,
    "home_logo": S,
    "home_score": INT,
}


def parse_pleague_schedule(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The schedule and results of one season and stage of the P. LEAGUE+: one row per game.

    Args:
        raw: the record a ``pleague_schedule`` wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per game in ascending ``gid``. ``season`` is the END year (2025 = ``"2024-25"``, kept in
        ``season_slug``). The away team comes first on the page (``away_*`` before ``home_*``). A future game
        prints 0-0 with ``phase`` = ``is_future``; a past game with 0-0 may be a series game that was never played
        (check the game page's status). Join teams on ``away_team_id`` / ``home_team_id``, never on the printed
        names (renames change them). Zero rows for an empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.plg import pleague_schedule, parse_pleague_schedule

            raw = pleague_schedule(season=2025, return_parsed=False)
            games = parse_pleague_schedule(raw)
    """
    r = _rec(raw)
    rows = []
    for g in _dicts(r.get("games")):
        rows.append(
            {
                "season": _i(r.get("season_sdv")),
                "season_slug": _s(r.get("season")),
                "stage": _s(r.get("stage")),
                "gid": _i(g.get("gid")),
                "date": _date(g.get("date")),
                "weekday_zh": _s(g.get("weekday_zh")),
                "time": _s(g.get("time")),
                "starred": g.get("starred") if isinstance(g.get("starred"), bool) else None,
                "game_label": _s(g.get("game_label")),
                "venue_zh": _s(g.get("venue_zh")),
                "attendance": _i(g.get("attendance")),
                "capacity": _i(g.get("capacity")),
                "phase": _s(g.get("phase")),
                **{
                    f"{side}_{k}": (_i if k in ("team_id", "score") else _s)(g.get(f"{side}_{k}"))
                    for side in ("away", "home")
                    for k in ("team_id", "name_zh", "name_en", "logo", "score")
                },
            }
        )
    return _frame(rows, _SCHEDULE_SPEC, columns, return_as_pandas)


# -------------------------------------------------------------------------------------- game / periods / officials
_GAME_SPEC: Spec = {
    "gid": INT,
    "uuid": S,
    "status_zh": S,
    "finished": B,
    "date": D,
    "time": S,
    "venue_zh": S,
    "game_label_zh": S,
    "away_name_zh": S,
    "away_name_en": S,
    "away_logo": S,
    "away_score": INT,
    "home_name_zh": S,
    "home_name_en": S,
    "home_logo": S,
    "home_score": INT,
    "overtimes": INT,
}


def parse_pleague_game(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The header of one finished P. LEAGUE+ game: date, venue, both clubs with their score and the Preciser uuid.

    Args:
        raw: the record a ``pleague_game`` wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row. ``uuid`` is the Preciser game uuid (the key of every ``pleague_preciser`` wrapper), null before
        2022-23. The page prints no team ids: take them from ``pleague_schedule`` on ``gid``. Date and time are
        Asia/Taipei (UTC+8). Zero rows for an empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.plg import pleague_game, parse_pleague_game

            raw = pleague_game(gid=197, return_parsed=False)
            game = parse_pleague_game(raw)
    """
    r = _rec(raw)
    gid = _gid(r)
    rows = []
    if gid is not None and r.get("status_zh") is not None:
        q = _rec(r.get("quarters"))
        rows.append(
            {
                "gid": gid,
                "uuid": _s(r.get("uuid")),
                "status_zh": _s(r.get("status_zh")),
                "finished": r.get("status_zh") == FINAL,
                "date": _date(r.get("date")),
                "time": _s(r.get("time")),
                "venue_zh": _s(r.get("venue_zh")),
                "game_label_zh": _s(r.get("game_label_zh")),
                "away_name_zh": _s(r.get("away_name_zh")),
                "away_name_en": _s(r.get("away_name_en")),
                "away_logo": _s(r.get("away_logo")),
                "away_score": _i(r.get("away_score")),
                "home_name_zh": _s(r.get("home_name_zh")),
                "home_name_en": _s(r.get("home_name_en")),
                "home_logo": _s(r.get("home_logo")),
                "home_score": _i(r.get("home_score")),
                "overtimes": sum(1 for k, v in q.items() if str(k).startswith("ot") and str(k).endswith("_home") and v),
            }
        )
    return _frame(rows, _GAME_SPEC, columns, return_as_pandas)


_PERIOD_SPEC: Spec = {"gid": INT, "period": S, "period_no": INT, "home_points": INT, "away_points": INT}


def parse_pleague_periods(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Points each team scored in each quarter and overtime of one finished P. LEAGUE+ game.

    Args:
        raw: the record a ``pleague_game`` wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per period played: Q1-Q4, then OT1 / OT2 when the page shows overtime points. The points are
        scored IN the period (not cumulative). Zero rows for an empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.plg import pleague_game, parse_pleague_periods

            raw = pleague_game(gid=206, return_parsed=False)
            periods = parse_pleague_periods(raw)
    """
    r = _rec(raw)
    gid = _gid(r)
    q = _rec(r.get("quarters"))
    rows = []
    if gid is not None:
        for key in ("q1", "q2", "q3", "q4", "ot1", "ot2", "ot3", "ot4", "ot5"):
            h, a = q.get(f"{key}_home"), q.get(f"{key}_away")
            if _i(h) is None or _i(a) is None or (key.startswith("ot") and not (_i(h) or _i(a))):
                continue
            n = int(key[-1])
            rows.append(
                {
                    "gid": gid,
                    "period": key.upper(),
                    "period_no": n if key.startswith("q") else 4 + n,
                    "home_points": _i(h),
                    "away_points": _i(a),
                }
            )
    return _frame(rows, _PERIOD_SPEC, columns, return_as_pandas)


_ROLES = {"主裁判": "referee", "第一檢查員": "first_umpire", "第二檢查員": "second_umpire"}
_OFFICIAL_SPEC: Spec = {"gid": INT, "official_no": INT, "role": S, "role_zh": S, "name_zh": S}


def parse_pleague_officials(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The officials printed under the header of one finished P. LEAGUE+ game (names only).

    Args:
        raw: the record a ``pleague_game`` wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per official in page order, with the role translated (``referee``, ``first_umpire``,
        ``second_umpire``; ``role`` is null for a role this table does not know). Names only: the page carries no
        ids or contact data. Zero rows for an empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.plg import pleague_game, parse_pleague_officials

            raw = pleague_game(gid=197, return_parsed=False)
            officials = parse_pleague_officials(raw)
    """
    r = _rec(raw)
    gid = _gid(r)
    rows = []
    if gid is not None:
        for n, o in enumerate(_dicts(r.get("officials")), 1):
            role = _s(o.get("role_zh"))
            rows.append(
                {
                    "gid": gid,
                    "official_no": n,
                    "role": _ROLES.get(role or ""),
                    "role_zh": role,
                    "name_zh": _s(o.get("name_zh")),
                }
            )
    return _frame(rows, _OFFICIAL_SPEC, columns, return_as_pandas)


# -------------------------------------------------------------------------------------- box score
_FG_PAIR = re.compile(r"\s*(\d+)\s*-\s*(\d+)\s*")

_BOX_PLAYER_SPEC: Spec = {
    "gid": INT,
    "side": S,
    "player_id": INT,
    "person_id": S,
    "random_id": S,
    "name_zh": S,
    "jersey": S,
    "starter": B,
    "played": B,
    "minutes": S,
    "seconds": INT,
    "points": INT,
    "reb_off": INT,
    "reb_def": INT,
    "reb": INT,
    "ast": INT,
    "stl": INT,
    "blk": INT,
    "tov": INT,
    "pf": INT,
    "plus_minus": INT,
    "eff": INT,
    "fg2_made": INT,
    "fg2_att": INT,
    "fg2_pct": F,
    "fg3_made": INT,
    "fg3_att": INT,
    "fg3_pct": F,
    "ft_made": INT,
    "ft_att": INT,
    "ft_pct": F,
    "efg_pct": F,
    "ts_pct": F,
    "usage_pct": F,
    "position": S,
    "ortg": F,
    "drtg": F,
    "netrtg": F,
    "pace": F,
    "ast_ratio": F,
    "to_ratio": F,
    "off_possessions": INT,
    "opp_off_possessions": INT,
    "opp_pts_second_chance": INT,
    "opp_pts_fast_break": INT,
    "opp_pts_off_turnover": INT,
    "opp_pts_paint": INT,
}


def _pair(v: Any) -> Tuple[Optional[int], Optional[int]]:
    m = _FG_PAIR.fullmatch(v) if isinstance(v, str) else None
    return (int(m.group(1)), int(m.group(2))) if m else (None, None)


def _box_data(raw: Any) -> Dict[str, Any]:
    d = _rec(raw).get("data")
    return d if isinstance(d, dict) else {}


def parse_pleague_box_players(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The player box score of one finished P. LEAGUE+ game: one row per rostered player, did-not-play rows included.

    Args:
        raw: the body a ``pleague_box`` wrapper returns with ``return_parsed=False`` (the site's JSON, the
            request stamped under ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per rostered player, ``side`` = ``home`` / ``away`` (the site lists home first; the play-by-play and
        the schedule list away first). Every numeric cell is cast here: the site sends numbers, digit strings and
        ``''`` (a did-not-play row, ``played`` = false, all stats null) in one column. Made field goals and free
        throws come from the site's ``made-attempted`` strings, so they exist in every era. Era A rows (2020-21,
        2021-22) fill ``seconds``, ``efg_pct``, ``ts_pct``, ``usage_pct`` and ``position``; era B rows (2022-23 on) fill
        ``person_id`` (the join to the Preciser shot list; the refs notes report some 2025-26 finals boxes without it), the ratings, the
        ratios and the possession columns. ``random_id`` is unreliable in era A; join on ``player_id``. Zero rows for
        an empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.plg import pleague_box, parse_pleague_box_players

            raw = pleague_box(gid=197, return_parsed=False)
            players = parse_pleague_box_players(raw)
    """
    gid = _gid(raw)
    d = _box_data(raw)
    rows = []
    for side in ("home", "away"):
        for p in _dicts(d.get(side)):
            played = isinstance(p.get("mins"), str) and p["mins"].strip() not in ("", "DNP")
            fg2 = _pair(p.get("two_m_two"))
            fg3 = _pair(p.get("trey_m_trey"))
            ft = _pair(p.get("ft_m_ft"))
            secs = _i(p.get("seconds"))
            rows.append(
                {
                    "gid": gid,
                    "side": side,
                    "player_id": _i(p.get("player_id")),
                    "person_id": _s(p.get("personId")),
                    "random_id": _s(p.get("random_id")),
                    "name_zh": _s(p.get("name_alt")) or _s(p.get("name")),
                    "jersey": _s(p.get("jersey")),
                    "starter": (p.get("starter") == "〇") if played else None,
                    "played": played,
                    "minutes": p["mins"].strip() if played else None,
                    "seconds": secs if secs is not None else (_seconds(p.get("mins")) if played else None),
                    "points": _i(p.get("points")),
                    "reb_off": _i(p.get("reb_o")),
                    "reb_def": _i(p.get("reb_d")),
                    "reb": _i(p.get("reb")),
                    "ast": _i(p.get("ast")),
                    "stl": _i(p.get("stl")),
                    "blk": _i(p.get("blk")),
                    "tov": _i(p.get("turnover")),
                    "pf": _i(p.get("pfoul")),
                    "plus_minus": _i(p.get("positive")),
                    "eff": _i(p.get("eff")),
                    "fg2_made": fg2[0],
                    "fg2_att": fg2[1],
                    "fg2_pct": _f(p.get("twop")),
                    "fg3_made": fg3[0],
                    "fg3_att": fg3[1],
                    "fg3_pct": _f(p.get("treyp")),
                    "ft_made": ft[0],
                    "ft_att": ft[1],
                    "ft_pct": _f(p.get("ftp")),
                    "efg_pct": _f(p.get("efgp")),
                    "ts_pct": _f(p.get("tsp")),
                    "usage_pct": _f(p.get("ugp")),
                    "position": _s(p.get("position")),
                    "ortg": _f(p.get("ORTG")),
                    "drtg": _f(p.get("DRTG")),
                    "netrtg": _f(p.get("NETRTG")),
                    "pace": _f(p.get("pace")),
                    "ast_ratio": _f(p.get("AST_RATIO")),
                    "to_ratio": _f(p.get("TO_RATIO")),
                    "off_possessions": _i(p.get("offensive_possession")),
                    "opp_off_possessions": _i(p.get("opp_offensive_possession")),
                    "opp_pts_second_chance": _i(p.get("opp_points_from_2nd_chance")),
                    "opp_pts_fast_break": _i(p.get("opp_points_from_fastbreak")),
                    "opp_pts_off_turnover": _i(p.get("opp_points_from_turnover")),
                    "opp_pts_paint": _i(p.get("opp_points_in_the_paint")),
                }
            )
    return _frame(rows, _BOX_PLAYER_SPEC, columns, return_as_pandas)


_BOX_TEAM_SPEC: Spec = {
    "gid": INT,
    "side": S,
    "minutes": S,
    "seconds": INT,
    "points": INT,
    "fg2_made": INT,
    "fg2_att": INT,
    "fg2_pct": F,
    "fg3_made": INT,
    "fg3_att": INT,
    "fg3_pct": F,
    "ft_made": INT,
    "ft_att": INT,
    "ft_pct": F,
    "reb": INT,
    "reb_off": INT,
    "reb_def": INT,
    "ast": INT,
    "stl": INT,
    "blk": INT,
    "tov": INT,
    "pf": INT,
    "plus_minus": INT,
}


def parse_pleague_box_teams(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The team totals of one finished P. LEAGUE+ game: the box score's total row, home then away.

    Args:
        raw: the body a ``pleague_box`` wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        Two rows (``home``, ``away``). ``points`` equals the final score (the runtime checks it). Team ``reb``
        includes team rebounds no player row carries, so it can exceed the sum of the player rows. ``seconds`` and
        ``plus_minus`` exist in era A (2020-21, 2021-22) only. Zero rows for an empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.plg import pleague_box, parse_pleague_box_teams

            raw = pleague_box(gid=197, return_parsed=False)
            teams = parse_pleague_box_teams(raw)
    """
    gid = _gid(raw)
    d = _box_data(raw)
    rows = []
    for side in ("home", "away"):
        t = d.get(f"{side}_total")
        if not isinstance(t, dict):
            continue
        rows.append(
            {
                "gid": gid,
                "side": side,
                "minutes": _s(t.get("mins")),
                "seconds": _i(t.get("seconds")) if _i(t.get("seconds")) is not None else _seconds(t.get("mins")),
                "points": _i(t.get("points")),
                "fg2_made": _i(t.get("two_m")),
                "fg2_att": _i(t.get("two")),
                "fg2_pct": _f(t.get("twop")),
                "fg3_made": _i(t.get("trey_m")),
                "fg3_att": _i(t.get("trey")),
                "fg3_pct": _f(t.get("treyp")),
                "ft_made": _i(t.get("ft_m")),
                "ft_att": _i(t.get("ft")),
                "ft_pct": _f(t.get("ftp")),
                "reb": _i(t.get("reb")),
                "reb_off": _i(t.get("reb_o")),
                "reb_def": _i(t.get("reb_d")),
                "ast": _i(t.get("ast")),
                "stl": _i(t.get("stl")),
                "blk": _i(t.get("blk")),
                "tov": _i(t.get("turnover")),
                "pf": _i(t.get("pfoul")),
                "plus_minus": _i(t.get("positive")),
            }
        )
    return _frame(rows, _BOX_TEAM_SPEC, columns, return_as_pandas)


# -------------------------------------------------------------------------------------- play-by-play
_PBP_SPEC: Spec = {
    "gid": INT,
    "seq": INT,
    "period": S,
    "period_no": INT,
    "clock": S,
    "seconds_left": INT,
    "side": S,
    "jersey": S,
    "player_zh": S,
    "action": S,
    "action_zh": S,
    "action_en": S,
    "text_zh": S,
    "ft_number": INT,
    "ft_awarded": INT,
    "points": INT,
    "score_away": INT,
    "score_home": INT,
    "x": F,
    "y": F,
    "nice_play": B,
    "video_uuid": S,
    "synergy_play_id": S,
    "video_stat_category": S,
    "tryspot_player_id": INT,
    "video_created_at": S,
}
_ACTION_EN = {code: en for _z, (code, en) in ACTIONS.items()}
_ACTION_EN[UNLABELLED[0]] = UNLABELLED[1]


def _action_en(code: Optional[str], phrase: Any) -> Optional[str]:
    """The meaning of the row's own phrase (codes like ``ft_made`` cover many phrases); the code's as a fallback."""
    if code is None:
        return None
    hit = ACTIONS.get(phrase) if isinstance(phrase, str) else None
    return hit[1] if hit else _ACTION_EN.get(code)


def _period_no(label: Any) -> Optional[int]:
    m = re.fullmatch(r"(Q|OT)([1-9])", label) if isinstance(label, str) else None
    return None if not m else int(m.group(2)) if m.group(1) == "Q" else 4 + int(m.group(2))


def parse_pleague_pbp(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The play-by-play of one finished P. LEAGUE+ game (2022-23 on), in game order, with shot x / y.

    Args:
        raw: the record a ``pleague_pbp`` wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per play. The text is Traditional Chinese; ``action`` is a code from the observed vocabulary
        (``action_zh`` the printed phrase, ``action_en`` its meaning), so an unseen phrase never reaches a frame:
        the runtime refuses it. ``score_away`` / ``score_home`` print AWAY first (the box lists home first) and are
        null on rows that print no score (substitutions, rebounds, jump balls). The page prints player NAMES and
        jersey numbers, no player ids: join to the box on ``side``, ``jersey`` and ``player_zh`` (= the box
        ``name_zh``). ``x`` / ``y`` are in the PHYSICAL court frame (teams swap ends at half-time); the Preciser shot
        list uses a home-always-attacks-right frame: ``x_vendor = 100 - x`` and ``y_vendor = 100 - y`` in the
        halves where the home team attacks x < 50. Clip urls are never kept (they carry a vendor key). Zero rows for
        an empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.plg import pleague_pbp, parse_pleague_pbp

            raw = pleague_pbp(gid=197, return_parsed=False)
            plays = parse_pleague_pbp(raw)
    """
    r = _rec(raw)
    gid = _gid(r)
    rows = []
    for e in _dicts(r.get("events")):
        v = _rec(e.get("video"))
        code = _s(e.get("action"))
        rows.append(
            {
                "gid": gid,
                "seq": _i(e.get("seq")),
                "period": _s(e.get("period")),
                "period_no": _period_no(e.get("period")),
                "clock": _s(e.get("clock")),
                "seconds_left": _seconds(e.get("clock")),
                "side": _s(e.get("side")),
                "jersey": _s(e.get("jersey")),
                "player_zh": _s(e.get("player_zh")),
                "action": code,
                "action_zh": e.get("action_zh") if isinstance(e.get("action_zh"), str) else None,
                "action_en": _action_en(code, e.get("action_zh")),
                "text_zh": _s(e.get("text_zh")),
                "ft_number": _i(e.get("ft_number")),
                "ft_awarded": _i(e.get("ft_awarded")),
                "points": _i(e.get("points")),
                "score_away": _i(e.get("score_away")),
                "score_home": _i(e.get("score_home")),
                "x": _f(e.get("x")),
                "y": _f(e.get("y")),
                "nice_play": e.get("nice_play") if isinstance(e.get("nice_play"), bool) else None,
                "video_uuid": _s(v.get("video_uuid")),
                "synergy_play_id": _s(v.get("synergy_play_id")),
                "video_stat_category": _s(v.get("stat_category")),
                "tryspot_player_id": _i(v.get("tryspot_player_id")),
                "video_created_at": _s(v.get("created_at")),
            }
        )
    return _frame(rows, _PBP_SPEC, columns, return_as_pandas)


# -------------------------------------------------------------------------------------- standings
_TEAM_LOGO_UUID = re.compile(r"/plg/teams/([0-9a-f-]{36})\.png")
_STANDINGS_SPEC: Spec = {
    "season": INT,
    "season_slug": S,
    "rank": INT,
    "team_id": INT,
    "team_zh": S,
    "team_uuid": S,
    "team_logo": S,
    "games_played": INT,
    "wins": INT,
    "losses": INT,
    "win_pct": F,
    "games_behind": F,
    "points_for_avg": F,
    "points_against_avg": F,
    "point_diff_avg": F,
    "home_record": S,
    "away_record": S,
    "streak": S,
    "last_n": INT,
    "last_n_record": S,
}


def parse_pleague_standings(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The regular-season standings of one P. LEAGUE+ season.

    Args:
        raw: the record a ``pleague_standings`` wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per club in the page's rank order (ties are not shared). ``team_uuid`` is cut from the logo url
        (the file name is the Preciser team uuid): it is the join between the site ``team_id`` and the Preciser
        ``home_id`` / ``away_id`` / ``team_id``. ``last_n`` is the width of ``last_n_record`` (10). Only the
        regular-season table is parsed; the playoffs and finals tables are not wired on the page. Zero rows for an
        empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.plg import pleague_standings, parse_pleague_standings

            raw = pleague_standings(season=2023, return_parsed=False)
            table = parse_pleague_standings(raw)
    """
    r = _rec(raw)
    rows = []
    for t in _dicts(r.get("teams")):
        logo = _s(t.get("team_logo"))
        uu = _TEAM_LOGO_UUID.search(logo or "")
        rows.append(
            {
                "season": _i(r.get("season_sdv")),
                "season_slug": _s(r.get("season")),
                "rank": _i(t.get("rank")),
                "team_id": _i(t.get("team_id")),
                "team_zh": _s(t.get("team_zh")),
                "team_uuid": uu.group(1) if uu else None,
                "team_logo": logo,
                "games_played": _i(t.get("games_played")),
                "wins": _i(t.get("wins")),
                "losses": _i(t.get("losses")),
                "win_pct": _f(t.get("win_pct")),
                "games_behind": _f(t.get("games_behind")),
                "points_for_avg": _f(t.get("points_for_avg")),
                "points_against_avg": _f(t.get("points_against_avg")),
                "point_diff_avg": _f(t.get("point_diff_avg")),
                "home_record": _s(t.get("home_record")),
                "away_record": _s(t.get("away_record")),
                "streak": _s(t.get("streak")),
                "last_n": _i(r.get("last_n")),
                "last_n_record": _s(t.get("last_n_record")),
            }
        )
    return _frame(rows, _STANDINGS_SPEC, columns, return_as_pandas)


# -------------------------------------------------------------------------------------- player season table
_STAT_PLAYER_SPEC: Spec = {
    "season": INT,
    "season_slug": S,
    "stage_sn": INT,
    "stage": S,
    "player_page_id": S,
    "name_zh": S,
    "jersey": S,
    "team_zh": S,
    "games": INT,
    "minutes_avg": S,
    "minutes_total": S,
    **{
        f"{k}_{agg}": (INT if agg == "total" else F)
        for k in (
            "fg2_made",
            "fg2_att",
            "fg3_made",
            "fg3_att",
            "ft_made",
            "ft_att",
            "points",
            "reb_off",
            "reb_def",
            "reb",
            "ast",
            "stl",
            "blk",
            "tov",
            "pf",
        )
        for agg in ("avg", "total")
    },
    "fg2_pct": F,
    "fg3_pct": F,
    "ft_pct": F,
}


def parse_pleague_stat_player(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The player season averages and totals of one P. LEAGUE+ season and stage.

    Args:
        raw: the record a ``pleague_stat_player`` wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per player of the season and stage (``stage_sn`` 2 = regular season, 3 = playoffs, 4 = finals).
        Each count has an ``_avg`` (per game) and a ``_total`` column; the percentages are single columns. The page
        shows a traded player on the current team. ``player_page_id`` is the 8-character page id (the box
        ``random_id`` from 2022-23), NOT the box ``player_id``. Basic columns only (no ratings). Zero rows for an
        empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.plg import pleague_stat_player, parse_pleague_stat_player

            raw = pleague_stat_player(season=2023, stage_sn=2, return_parsed=False)
            players = parse_pleague_stat_player(raw)
    """
    r = _rec(raw)
    rows = []
    for p in _dicts(r.get("players")):
        row: Dict[str, Any] = {
            "season": _i(r.get("season_sdv")),
            "season_slug": _s(r.get("season")),
            "stage_sn": _i(r.get("stage_sn")),
            "stage": _s(r.get("stage")),
        }
        for c, dt in _STAT_PLAYER_SPEC.items():
            if c in row:
                continue
            v = p.get(c)
            row[c] = _s(v) if dt == S else _i(v) if dt == INT else _f(v)
        rows.append(row)
    return _frame(rows, _STAT_PLAYER_SPEC, columns, return_as_pandas)


# -------------------------------------------------------------------------------------- player page
_PLAYER_SPEC: Spec = {
    "player_page_id": S,
    "player_id": INT,
    "name_zh": S,
    "name_en": S,
    "team_zh": S,
    "team_en": S,
    "jersey": S,
    "position": S,
    "nickname": S,
    "tenure": S,
    "height_cm": F,
    "weight_kg": F,
    "birthplace_zh": S,
    "status_zh": S,
    "education": S,
}


def parse_pleague_player(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The profile of one P. LEAGUE+ player: names, club, position, size and birthplace.

    Args:
        raw: the record a ``pleague_player`` wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row. ``player_id`` is the site's membership id (the box ``player_id``), read from the page's
        ``trackPlayer()`` call; ``player_page_id`` is the page id. The date of birth the page prints is dropped on
        purpose (personal data); ``education`` joins the printed lines with ``'; '``. The per-season tables are in
        ``pleague_player_seasons``. Zero rows for an empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.plg import pleague_player, parse_pleague_player

            raw = pleague_player(player_page_id="1JbPozLB", return_parsed=False)
            profile = parse_pleague_player(raw)
    """
    r = _rec(raw)
    rows = []
    if _s(r.get("player_page_id")):
        edu = [e for e in r.get("education") or [] if isinstance(e, str) and e.strip()]
        rows.append(
            {
                "player_page_id": _s(r.get("player_page_id")),
                "player_id": _i(r.get("member_id")),
                "name_zh": _s(r.get("name_zh")),
                "name_en": _s(r.get("name_en")),
                "team_zh": _s(r.get("team_zh")),
                "team_en": _s(r.get("team_en")),
                "jersey": _s(r.get("jersey")),
                "position": _s(r.get("position")),
                "nickname": _s(r.get("nickname")),
                "tenure": _s(r.get("tenure")),
                "height_cm": _f(r.get("height_cm")),
                "weight_kg": _f(r.get("weight_kg")),
                "birthplace_zh": _s(r.get("birthplace_zh")),
                "status_zh": _s(r.get("status_zh")),
                "education": "; ".join(e.strip() for e in edu) or None,
            }
        )
    return _frame(rows, _PLAYER_SPEC, columns, return_as_pandas)


_SEASON_SLUG = re.compile(r"(\d{4})-(\d{2})")
_PLAYER_SEASON_SPEC: Spec = {
    "player_page_id": S,
    "competition": S,
    "table": S,
    "season": INT,
    "season_slug": S,
    "games": INT,
    "minutes": S,
    "points": F,
    "reb": F,
    "ast": F,
    "stl": F,
    "blk": F,
    "tov": F,
    "pf": F,
    "fg2_made": F,
    "fg2_att": F,
    "fg2_pct": F,
    "fg3_made": F,
    "fg3_att": F,
    "fg3_pct": F,
    "ft_made": F,
    "ft_att": F,
    "ft_pct": F,
}


def parse_pleague_player_seasons(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """One P. LEAGUE+ player's season totals and per-game averages, league and East Asia Super League.

    Args:
        raw: the record a ``pleague_player`` wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per season per table: ``competition`` is ``plg`` (the league) or ``easl`` (the East Asia Super
        League block, present only for players with games there), ``table`` is ``totals`` (season totals) or
        ``averages`` (per game). A ``career`` row ends each table (``season`` and ``season_slug`` are then null /
        ``'career'``). ``season`` is the END year of the slug. Zero rows for an empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.plg import pleague_player, parse_pleague_player_seasons

            raw = pleague_player(player_page_id="1JbPozLB", return_parsed=False)
            seasons = parse_pleague_player_seasons(raw)
    """
    r = _rec(raw)
    pid = _s(r.get("player_page_id"))
    rows = []
    for comp, table, key in (
        ("plg", "totals", "season_totals"),
        ("plg", "averages", "season_averages"),
        ("easl", "totals", "easl_season_totals"),
        ("easl", "averages", "easl_season_averages"),
    ):
        for t in _dicts(r.get(key)):
            slug = _s(t.get("season"))
            m = _SEASON_SLUG.fullmatch(slug or "")
            row: Dict[str, Any] = {
                "player_page_id": pid,
                "competition": comp,
                "table": table,
                "season": int(m.group(1)) + 1 if m else None,
                "season_slug": slug,
            }
            for c, dt in _PLAYER_SEASON_SPEC.items():
                if c not in row:
                    row[c] = _s(t.get(c)) if dt == S else _i(t.get(c)) if dt == INT else _f(t.get(c))
            rows.append(row)
    return _frame(rows, _PLAYER_SEASON_SPEC, columns, return_as_pandas)


#: Each parser's column dtypes, keyed by wrapper short, for the generator and the tests.
SPECS: Dict[str, Spec] = {
    "schedule": _SCHEDULE_SPEC,
    "game": _GAME_SPEC,
    "periods": _PERIOD_SPEC,
    "officials": _OFFICIAL_SPEC,
    "box_players": _BOX_PLAYER_SPEC,
    "box_teams": _BOX_TEAM_SPEC,
    "pbp": _PBP_SPEC,
    "standings": _STANDINGS_SPEC,
    "stat_player": _STAT_PLAYER_SPEC,
    "player": _PLAYER_SPEC,
    "player_seasons": _PLAYER_SEASON_SPEC,
}
