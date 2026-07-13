"""NCAA stats.ncaa.org play-by-play scraper — bigballR ``scrape_game`` port.

Faithful polars port of bigballR's keystone scraper + lineup engine:

* ``scrape_game``      — ``bigballR/R/all_functions.R:51-1090``
* ``convert_events``   — ``bigballR/R/all_functions.R:3179-3238``
* ``get_play_by_play`` — ``bigballR/R/all_functions.R:1857-1897``

The core is generalized over a ``period_model`` knob —
``(n_regulation_periods, regulation_period_seconds, overtime_period_seconds)``
— so the same engine serves MBB halves ``(2, 1200, 300)`` and WBB quarters
``(4, 600, 300)`` (bigballR/wbigballR hardcode the MBB halves math; see
``dev/bigballr_port/spec_wbigballr_divergence.md`` §3).

Deliberate R-faithful quirks are marked with ``ponytail:`` comments citing the
R line they reproduce; the 11 fork-skew fixes from the divergence spec are
adopted (the bigballR side already carries all that apply here).

**Technical / flagrant fouls (``fix_technicals``, default True).** The R chain
has no technical or flagrant rules, so a made technical free throw flips
``poss_team`` — a possession change that never happened
(``dev/bigballr_port/possession_engine_reconciliation.md`` BUG-3). The fix
ports the semantics of hoop-explorer's ``bad_fouls`` /
``offsetting_tech_or_flagrant`` (``mbb_ncaa_possessions.calculate_stats``):
free throws sharing a clock reading with a technical / flagrant foul are inert
— no switch, no corrective flip, no and-1 — which also nets offsetting double
technicals to zero. Pass ``fix_technicals=False`` for faithful R output (the
parity tests do).

**This rule is UN-ADJUDICATED.** Two committed fixtures do exercise it (1613299
carries a coach technical, 6479639 a flagrant — 2 and 1 phantom possessions
respectively), but their R oracle encodes the BUGGY chain, so no oracle can
score the fixed output: there is no ground truth for the correct behavior. The
rule is a code-read port of hoop-explorer's, pinned by constructed sequences in
``tests/mbb/test_mbb_ncaa_technical_possessions.py``. Re-validate it against a
refereed capture when one exists.

Dropping a phantom possession also drops a possession BOUNDARY, so the lineup
walk-forward can re-home a substitution and ``sub_deviate`` can move — expected
downstream of the fix, not a second change.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Literal, Optional, Protocol, Sequence, Union, overload

import polars as pl

from .mbb_ncaa_html import parse_html

logger = logging.getLogger(__name__)

__all__ = [
    "PBP_SCHEMA",
    "parse_ncaa_bb_game_pbp",
    "ncaa_mbb_game_pbp",
    "ncaa_mbb_play_by_play",
]

#: Output contract — snake_case rename of bigballR's 35-column ``scrape_game``
#: frame (``Half_Status`` → ``period``); mirrored by the parity oracle helper
#: ``tests/mbb/_bigballr_oracle.py``.
PBP_SCHEMA: "dict[str, type[pl.DataType]]" = {
    "game_id": pl.Utf8,
    "game_date": pl.Utf8,
    "home": pl.Utf8,
    "away": pl.Utf8,
    "period": pl.Int64,
    "clock": pl.Utf8,
    "game_time": pl.Utf8,
    "game_seconds": pl.Int64,
    "home_score": pl.Int64,
    "away_score": pl.Int64,
    "event_team": pl.Utf8,
    "event_description": pl.Utf8,
    "player_1": pl.Utf8,
    "player_2": pl.Utf8,
    "event_type": pl.Utf8,
    "event_result": pl.Utf8,
    "shot_value": pl.Int64,
    "event_length": pl.Int64,
    "poss_num": pl.Int64,
    "poss_team": pl.Utf8,
    "poss_length": pl.Int64,
    "is_transition": pl.Boolean,
    "home_1": pl.Utf8,
    "home_2": pl.Utf8,
    "home_3": pl.Utf8,
    "home_4": pl.Utf8,
    "home_5": pl.Utf8,
    "away_1": pl.Utf8,
    "away_2": pl.Utf8,
    "away_3": pl.Utf8,
    "away_4": pl.Utf8,
    "away_5": pl.Utf8,
    "status": pl.Utf8,
    "is_garbage_time": pl.Boolean,
    "sub_deviate": pl.Int64,
}

_MBB_PERIOD_MODEL: "tuple[int, int, int]" = (2, 1200, 300)

#: all_functions.R:886 — empty sub-queue sentinel, kept verbatim.
_SENTINEL = "HOPEFULLY THIS IS NOBODY'S NAME"

#: all_functions.R:648-655 — Player_1 values excluded as team-event noise.
_TEAM_PLAYER_SENTINELS = frozenset({"TEAM.TEAM", "TEAM.TEAM 30", " TEAM.TEAM", "TEAM", "TEAM.TEAM ", "TEAM.TEAM 20"})

_SUB_TYPES = ("Enters Game", "Leaves Game")
_FG_TYPES = frozenset({"Two Point Jumper", "Three Point Jumper", "Layup", "Dunk", "Tip In", "Hook"})
_TERMINAL_TYPES = frozenset(
    {
        "Two Point Jumper",
        "Three Point Jumper",
        "Free Throw",
        "Dunk",
        "Layup",
        "Hook",
        "Tip In",
        "Steal",
        "Defensive Rebound",
    }
)
_TRANSITION_FIRST_TYPES = frozenset(
    {"Steal", "Dunk", "Layup", "Hook", "Tip In", "Two Point Jumper", "Three Point Jumper"}
)
_SHOT_VALUES: "dict[str, int]" = {
    "Two Point Jumper": 2,
    "Layup": 2,
    "Three Point Jumper": 3,
    "Dunk": 2,
    "Free Throw": 1,
    "Tip In": 2,
    "Hook": 2,
}

# Name normalization (all_functions.R:228-232 V2 / :262-276 V1).
# R: gsub("[^[:alnum:] ]", "", x) under a UTF-8 locale keeps unicode letters;
# \w (unicode) minus underscore is the Python equivalent (same as mbb_ncaa_box_stats).
_NON_ALNUM_SPACE_RE = re.compile(r"[^\w ]|_")
_WS_RE = re.compile(r"\s+")
_SUFFIX_RE = re.compile(r"(\.JR\.|\.SR\.|\.J\.R\.|\.JR\.|JR\.|SR\.|\.SR|\.JR|\.SR|\.III|\.II|\.IV)$")
# invalid-substitution detection (all_functions.R:479-481).
_SUB_NUMBER_RE = re.compile(r"\.[0-9]+")
_SUB_TEAM_RE = re.compile(r"\.TEAM$")

#: convert_events (all_functions.R:3179-3238) — V2 → V1 event vocabulary.
#: First match wins; the order is load-bearing (deadball before defensive /
#: offensive rebound, specific jumpballs before the generic ``"rebound "``,
#: ``"foulon"`` before ``"foul "``).
_V2_EVENT_MAP: "tuple[tuple[tuple[str, ...], str], ...]" = (
    (("2pt", "jumpshot", "missed"), "missed Two Point Jumper"),
    (("3pt", "jumpshot", "missed"), "missed Three Point Jumper"),
    (("2pt", "jumpshot", "made"), "made Two Point Jumper"),
    (("3pt", "jumpshot", "made"), "made Three Point Jumper"),
    (("rebound defensivedeadball",), "Deadball Rebound"),
    (("rebound offensivedeadball",), "Deadball Rebound"),
    (("rebound defensive",), "Defensive Rebound"),
    (("layup", "missed"), "missed Layup"),
    (("layup", "made"), "made Layup"),
    (("steal",), "Steal"),
    # ponytail: R maps "foulon" -> "Draw Foul" (all_functions.R:3202) while the
    # isTransition rule tests "Draws Foul" (:458) — V2 games can never satisfy
    # the foul-drawn transition branch. Latent upstream bug, kept for parity.
    (("foulon",), "Draw Foul"),
    (("assist",), "Assist"),
    (("foul ",), "Commits Foul"),
    (("substitution out",), "Leaves Game"),
    (("substitution in",), "Enters Game"),
    (("hookshot", "made"), "made Hook"),
    (("hookshot", "missed"), "missed Hook"),
    (("freethrow", "made"), "made Free Throw"),
    (("freethrow", "missed"), "missed Free Throw"),
    (("timeout",), "Timeout"),
    (("dunk", "made"), "made Dunk"),
    (("dunk", "missed"), "missed Dunk"),
    (("alleyoop", "made"), "made Dunk"),
    (("alleyoop", "missed"), "missed Dunk"),
    (("block",), "Blocked Shot"),
    (("rebound offensive",), "Offensive Rebound"),
    (("rebound ",), "Deadball Rebound"),
    ((" jumpball won",), "won Jumpball"),
    ((" jumpball lost",), "lost Jumpball"),
    ((" jumpball heldball",), "Jumpball (held ball)"),
    ((" jumpball outofbounds",), "Jumpball (out of bounds)"),
    ((" jumpball lodgedball",), "Jumpball (lodged ball)"),
    (("Team, foul",), "Team Foul"),
    (("turnover",), "Turnover"),
    (("wrongbasket",), "Wrong Basket (2pt Opp. Team)"),
    (("coachchallenge outofbounds",), "Challenge (Out of Bounds)"),
    (("coachchallenge goal",), "Challenge (Interference)"),
)

#: Technical / flagrant foul detector, run over the RAW ``event_description``.
#: bigballR's event vocabulary has no technical or flagrant type — the V2 map
#: folds ``"foul technical ..."`` / ``"foul personal flagrant ..."`` into the
#: generic ``"Commits Foul"`` (:3204) and V1 spells them out in the description
#: — so the raw text is the only signal either format preserves.
_BAD_FOUL_RE = re.compile(r"(?i)technical|flagrant")


def _is_bad_foul(row: "dict[str, Any]") -> bool:
    """True when the event is a technical / flagrant foul (BUG-3 detector)."""
    desc = row.get("event_description")
    return desc is not None and _BAD_FOUL_RE.search(desc) is not None


def _is_bad_ft(row: "dict[str, Any]", bad_secs: "set[int]") -> bool:
    """True for a free throw awarded by a technical / flagrant foul.

    A bad-foul FT is one sharing a clock reading with a technical / flagrant
    foul — the same "concurrent clump" grouping hoop-explorer uses
    (``mbb_ncaa_possessions.calculate_stats``). Such an FT neither ends nor
    transfers possession, so the chain must treat it as inert.
    """
    return row["event_type"] == "Free Throw" and row["game_seconds"] in bad_secs


def _stamp_possessions(
    rows: "list[dict[str, Any]]",
    home_team: Optional[str],
    away_team: Optional[str],
    *,
    fix_technicals: bool = True,
) -> None:
    """Stamp ``poss_num`` / ``poss_team`` onto arranged pbp rows, in place.

    bigballR's sequential possession chain (``all_functions.R:380-436``),
    reset per period: a defensive rebound, a turnover, a made FG (unless it's
    an and-1) or a made FT with nothing following it arms a switch, which
    takes effect at the next distinct clock reading; an event by the wrong
    team corrects the chain immediately.

    Args:
        rows: Arranged event rows (post assist-merge), each carrying
            ``period``, ``game_seconds``, ``event_team``, ``event_type``,
            ``event_result`` and ``event_description``. Mutated in place.
        home_team: Home team name, as it appears in ``event_team``.
        away_team: Away team name, as it appears in ``event_team``.
        fix_technicals: See :func:`parse_ncaa_bb_game_pbp`. True (default)
            makes technical / flagrant free throws inert; False reproduces
            bigballR, which has no technical / flagrant rules at all.
    """
    poss_num = 0
    max_period = rows[-1]["period"] if rows else 0
    for i in range(1, max_period + 1):
        prs = [r for r in rows if r["period"] == i]
        if not prs:
            # ponytail: R's 1:max loop would error on an empty period table;
            # real pages carry rows in every period.
            continue
        # BUG-3 fix: clock readings carrying a technical/flagrant foul. Every
        # Free Throw at one of these readings is a bad-foul FT and is INERT for
        # the chain (see _BAD_FOUL_RE + the `fix_technicals` docs).
        bad_secs: "set[int]" = {r["game_seconds"] for r in prs if _is_bad_foul(r)} if fix_technicals else set()
        first_non_sub = next((r for r in prs if r["event_type"] not in _SUB_TYPES), None)
        poss_team = first_non_sub["event_team"] if first_non_sub is not None else None
        other_team = away_team if poss_team == home_team else home_team
        poss_switch = False
        poss_num += 1
        for j, r in enumerate(prs):
            team = r["event_team"]
            typ = r["event_type"]
            result = r["event_result"]
            seconds = r["game_seconds"]
            bad_ft = _is_bad_ft(r, bad_secs)
            swap = poss_switch and seconds != prs[max(j - 1, 0)]["game_seconds"]
            if swap:
                poss_num += 1
                poss_team, other_team = other_team, poss_team
                poss_switch = False
            # R precedence: (shot & wrong team) | (turnover & wrong team) (:410).
            # A bad-foul FT is shot by whoever was fouled, not by whoever has
            # the ball, so it must NOT trigger the corrective re-sync flip.
            if not bad_ft and ((result is not None and team != poss_team) or (typ == "Turnover" and poss_team != team)):
                poss_num += 1 - (1 if swap else 0)
                poss_team, other_team = other_team, poss_team
                poss_switch = False
            and_one = any(
                x["event_type"] == "Free Throw" and not _is_bad_ft(x, bad_secs)
                for x in prs
                if x["game_seconds"] == seconds
            )
            nxt = prs[j + 1] if j + 1 < len(prs) else None
            next_reb = nxt is not None and (
                nxt["event_type"] == "Defensive Rebound"
                or (nxt["event_type"] == "Free Throw" and not _is_bad_ft(nxt, bad_secs))
            )
            if (
                typ in ("Defensive Rebound", "Turnover")
                or (typ in _FG_TYPES and result == "made" and not and_one)
                # A made bad-foul FT does not end the possession (the ball goes
                # back to whoever had it) — hoop-explorer's `bad_fouls` term.
                or (typ == "Free Throw" and result == "made" and not next_reb and not bad_ft)
            ):
                poss_switch = True
            r["poss_num"] = poss_num
            r["poss_team"] = poss_team


_Row = "dict[str, Any]"


class _SupportsFetchGamePbp(Protocol):
    """Structural type for the injected fetcher (``NcaaFetcher`` satisfies it)."""

    def fetch_game_pbp(self, contest_id: object) -> str: ...  # pragma: no cover


def _empty_pbp() -> pl.DataFrame:
    return pl.DataFrame(schema=PBP_SCHEMA)


def _convert_v2_event(event: Optional[str]) -> Optional[str]:
    """convert_events (all_functions.R:3179-3238): V2 grammar → V1 vocabulary."""
    if event is None:
        return None
    for needles, out in _V2_EVENT_MAP:
        if all(needle in event for needle in needles):
            return out
    return "ERROR CHECK THE EVENT"


def _extract_tables(html: str) -> "list[list[list[str]]]":
    """All ``<table>`` elements as rows of end-trimmed cell text.

    Mirrors ``rvest::html_table(html, header=TRUE)`` closely enough for this
    page family: cell text is the concatenated descendant text with leading /
    trailing whitespace trimmed (internal whitespace preserved), and each
    table's first row is its header.
    """
    soup = parse_html(html)
    tables: "list[list[list[str]]]" = []
    for tbl in soup.find_all("table"):
        rows: "list[list[str]]" = []
        for tr in tbl.find_all("tr"):
            cells = [c.get_text().strip() for c in tr.find_all(["th", "td"])]
            if cells:
                rows.append(cells)
        tables.append(rows)
    return tables


def _pad_row(cells: "Sequence[str]", width: int) -> "list[Optional[str]]":
    """Pad a short ``<tr>`` (colspan rows) with ``None`` — rvest's NA fill."""
    row: "list[Optional[str]]" = list(cells[:width])
    while len(row) < width:
        row.append(None)
    return row


def _time_in_seconds(clock: str) -> Optional[int]:
    """all_functions.R:157-166 — ``MM:SS`` → seconds; ``End`` rows → 0."""
    parts = clock.split(":")
    if "End" in parts[0]:
        return 0
    if len(parts[0]) == 2:
        try:
            return int(parts[0]) * 60 + int(parts[1])
        except (IndexError, ValueError):
            return None
    return None


def _mmss(total: int) -> str:
    return f"{total // 60:02d}:{total % 60:02d}"


def _int_or_none(x: Optional[str]) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x.strip())
    except ValueError:
        return None


def _normalize_v2_name(raw: str) -> str:
    """all_functions.R:228-232 — V2 name → ``FIRST.LAST`` uppercase."""
    name = _NON_ALNUM_SPACE_RE.sub("", raw)
    name = _WS_RE.sub(".", name.upper())
    name = _SUFFIX_RE.sub("", name)
    return name.strip()


def _normalize_v1_name(players: str) -> str:
    """all_functions.R:262-276 — V1 ``LAST,FIRST`` → ``FIRST.LAST``."""
    parts = players.split(",")
    last = _NON_ALNUM_SPACE_RE.sub("", parts[0]) if parts else ""
    first = _NON_ALNUM_SPACE_RE.sub("", parts[1]) if len(parts) > 1 else ""
    name = f"{first}.{last}"
    if name[:2] == ".T":
        name = "TEAM"
    name = _WS_RE.sub(".", name)
    return _SUFFIX_RE.sub("", name)


def _event_priority(event_type: Optional[str]) -> int:
    """all_functions.R:324-341 — within-second ordering priority."""
    if event_type == "won Jumpball":
        return 1
    if event_type == "lost Jumpball":
        return 2
    if event_type == "Offensive Rebound":
        return 3
    if event_type in _FG_TYPES:
        return 4
    if event_type == "Assist":
        return 5
    if event_type == "Turnover":
        return 6
    if event_type == "Steal":
        return 7
    if event_type not in _SUB_TYPES:
        return 6
    return 7


def _unique(seq: "Sequence[Any]") -> "list[Any]":
    """R ``unique()`` — first occurrence, order preserved."""
    seen: "set[Any]" = set()
    out: "list[Any]" = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _pop_queue(queue: "list[str]") -> "list[str]":
    """ponytail: R pops with ``x[2:length(x)]`` then drops NAs
    (all_functions.R:908-911, 961-964) — for a length-1 queue that is
    ``x[2:1] = c(NA, x[1])`` → NA-drop → the queue survives its own pop.
    """
    return queue[1:] if len(queue) > 1 else queue


def _infer_starters(
    period_rows: "list[dict[str, Any]]",
    all_rows: "list[dict[str, Any]]",
    side_team: str,
    leaving: "list[str]",
    entering: "list[str]",
    prev_starters: "list[Optional[str]]",
    start_clock: str,
    period: int,
    n_ot: int,
    *,
    dedup_leaving: bool,
) -> "list[Optional[str]]":
    """Per-period starter inference (all_functions.R:604-747 home / :750-872 away).

    ``dedup_leaving`` reproduces the home/away asymmetry: the home loop also
    rejects a leaver already seen in ``leaving`` (:617); the away loop does not
    (:761).
    """
    # ponytail: when nobody left this period, R never reassigns *_starters and
    # silently reuses the previous period's final five (stale-variable
    # carryover, load-bearing in low-sub OT periods) — modeled explicitly.
    starters: "list[Optional[str]]" = list(prev_starters)
    if leaving:
        starters = []
        for j, name in enumerate(leaving):
            # ponytail: R's `entering[1:(j-1)]` at j=1 indexes element 1 (the
            # c(1,0) quirk); deliberate correct emptiness here — the oracle
            # games do not exercise the divergent case.
            if name in entering[:j]:
                continue
            if dedup_leaving and name in leaving[:j]:
                continue
            starters.append(name)

    true_starters = [
        r["player_1"]
        for r in period_rows
        if r["event_team"] == side_team
        and r["clock"] == start_clock
        and r["clock"] != "00:00"
        and r["event_type"] == "Enters Game"
    ]
    true_nonstarters = [
        r["player_1"]
        for r in period_rows
        if r["event_team"] == side_team and r["clock"] == start_clock and r["event_type"] == "Leaves Game"
    ]
    # Self-sub at the period start cancels out (:631-633).
    swap = list(true_starters)
    true_starters = [x for x in true_starters if x not in true_nonstarters]
    true_nonstarters = [x for x in true_nonstarters if x not in swap]
    starters = [x for x in starters if x not in true_nonstarters]
    true_starters = [x for x in true_starters if x not in starters]
    starters = starters + true_starters

    if len(starters) < 5:
        split_rows = [
            r for r in period_rows if r["event_team"] == side_team and r["player_1"] not in _TEAM_PLAYER_SENTINELS
        ]
        # Self-subbed starters (:660-671) — ons that are also offs at the same
        # second, excluding the final 60 seconds of the period.
        error_catch: "list[str]" = []
        if split_rows:
            max_gs = max(r["game_seconds"] for r in split_rows)
            for t in _unique([r["game_seconds"] for r in split_rows]):
                if t >= max_gs - 60:
                    continue
                at_t = [r for r in split_rows if r["game_seconds"] == t]
                ons = [r["player_1"] for r in at_t if r["event_type"] == "Enters Game"]
                offs = [r["player_1"] for r in at_t if r["event_type"] == "Leaves Game"]
                error_catch.extend(p for p in ons if p in offs)
        error_catch = [p for p in error_catch if p not in starters]

        # Players with events who never sub (:674-675).
        excluded = set(leaving) | set(entering) | set(starters) | set(true_nonstarters)
        non_subs = _unique([r["player_1"] for r in split_rows if r["player_1"] not in excluded])

        # Players who record an event before the team's first sub-out (:678-683).
        play_before_sub: "list[str]" = []
        leave_secs = [r["game_seconds"] for r in split_rows if r["event_type"] == "Leaves Game"]
        if leave_secs:
            first_leave = leave_secs[0]
            first_gs: "dict[str, int]" = {}
            order: "list[str]" = []
            for r in split_rows:
                if r["player_1"] not in first_gs:
                    first_gs[r["player_1"]] = r["game_seconds"]
                    order.append(r["player_1"])
            play_before_sub = [p for p in order if first_gs[p] < first_leave and p not in starters]

        all_starters = _unique(list(starters) + non_subs + play_before_sub + error_catch)
        if len(all_starters) >= 5:
            starters = all_starters[:5]
        else:
            # Pad with the first distinct event-recording players (:708-724);
            # R's `[1:(5-length(...))]` NA-pads when the pool runs short.
            pool = _unique(
                [
                    r["player_1"]
                    for r in period_rows
                    if r["event_team"] == side_team
                    and r["event_type"] not in _SUB_TYPES
                    and r["player_1"] != "TEAM"
                    and r["player_1"] not in all_starters
                ]
            )
            need = 5 - len(all_starters)
            pad: "list[Optional[str]]" = list(pool[:need])
            while len(pad) < need:
                pad.append(None)
            starters = list(all_starters) + pad
    else:
        starters = starters[:5]
    starters = (list(starters) + [None] * 5)[:5]

    # NA backfill from other periods (:732-747 home / :857-872 away). The home
    # branch's `home_enter_players` reference is an R bug (used before defined
    # on period 1); the spec-directed fix is `entering`, matching the away arm.
    if any(s is None for s in starters):
        numb = sum(1 for s in starters if s is None)
        if period == 1:
            half_using = [2, 1] if n_ot == 0 else list(range(2, n_ot + 2))
        else:
            half_using = list(range(1, period + 1))
        banned = {s for s in starters if s is not None} | set(entering) | set(leaving) | {"TEAM"}
        prior = [
            r
            for r in all_rows
            if r["period"] in half_using
            and r["event_team"] == side_team
            and r["player_1"] not in banned
            and r["event_type"] != "Enters Game"
        ]
        # R `unique(x, fromLast=T)` — dedup keeping the LAST occurrence,
        # ordered by that occurrence's position.
        last_pos: "dict[str, int]" = {}
        for idx, r in enumerate(prior):
            last_pos[r["player_1"]] = idx
        ordered = sorted(last_pos, key=lambda p: last_pos[p])
        fill: "list[Optional[str]]" = []
        fill.extend(ordered[:numb] if period == 1 else list(reversed(ordered))[:numb])
        while len(fill) < numb:
            fill.append(None)
        fill_iter = iter(fill)
        starters = [s if s is not None else next(fill_iter) for s in starters]
        logger.info("5 starters not found for period %s; using estimate", period)
    return starters


def _assemble(
    rows: "list[dict[str, Any]]",
    game_id: str,
    game_date: str,
    home_team: str,
    away_team: str,
    status: str,
    sub_deviate: int,
    *,
    no_player: bool,
) -> pl.DataFrame:
    """Build the 35-column contract frame from the processed row dicts."""
    n = len(rows)
    data: "dict[str, list[Any]]" = {
        "game_id": [game_id] * n,
        "game_date": [game_date] * n,
        "home": [home_team] * n,
        "away": [away_team] * n,
        "period": [r["period"] for r in rows],
        "clock": [r["clock"] for r in rows],
        "game_time": [r["game_time"] for r in rows],
        "game_seconds": [r["game_seconds"] for r in rows],
        "home_score": [r["home_score"] for r in rows],
        "away_score": [r["away_score"] for r in rows],
        "event_team": [r["event_team"] for r in rows],
        "event_description": [r["event_description"] for r in rows],
        "player_1": [r["player_1"] for r in rows],
        "player_2": [r["player_2"] for r in rows],
        "event_type": [r["event_type"] for r in rows],
        "event_result": [r["event_result"] for r in rows],
        "shot_value": [r["shot_value"] for r in rows],
        "event_length": [r["event_length"] for r in rows],
        "poss_num": [r["poss_num"] for r in rows],
        "poss_team": [r["poss_team"] for r in rows],
        "poss_length": [r["poss_length"] for r in rows],
    }
    if no_player:
        # ponytail: R's NO_PLAYER select (all_functions.R:502-519) drops
        # isTransition/isGarbageTime and ships all-NA lineups; bind_rows then
        # unions them back as NA — the uniform 35-column contract does the
        # same with nulls.
        data["is_transition"] = [None] * n
        for k in range(5):
            data[f"home_{k + 1}"] = [None] * n
            data[f"away_{k + 1}"] = [None] * n
        data["status"] = [status] * n
        data["is_garbage_time"] = [None] * n
    else:
        data["is_transition"] = [r["is_transition"] for r in rows]
        for k in range(5):
            data[f"home_{k + 1}"] = [r["home_lineup"][k] for r in rows]
            data[f"away_{k + 1}"] = [r["away_lineup"][k] for r in rows]
        data["status"] = [status] * n
        data["is_garbage_time"] = [r["is_garbage_time"] for r in rows]
    data["sub_deviate"] = [sub_deviate] * n
    return pl.DataFrame(data, schema=PBP_SCHEMA)


def parse_ncaa_bb_game_pbp(
    html: str,
    game_id: str,
    *,
    period_model: "tuple[int, int, int]" = _MBB_PERIOD_MODEL,
    fix_technicals: bool = True,
) -> pl.DataFrame:
    """Parse one stats.ncaa.org play-by-play page (bigballR ``scrape_game``).

    Pure parser — HTML in, frame out, no network. Ports the full
    ``scrape_game`` algorithm (``bigballR/R/all_functions.R:51-1090``): table
    walk, V1/V2 format detection, clock math, event/player extraction,
    Event_Priority ordering, assist merge, sequential possession counting,
    invalid-substitution bailout, per-period starter inference, lineup
    walk-forward, garbage-time flag, and the deviation count.

    Args:
        html: Raw HTML of the ``/contests/{game_id}/play_by_play`` page.
        game_id: NCAA contest id (kept as Utf8 in the output — opaque id).
        period_model: ``(n_regulation_periods, regulation_period_seconds,
            overtime_period_seconds)``. MBB ``(2, 1200, 300)`` (the bigballR
            hardcoded model), WBB quarters ``(4, 600, 300)``.
        fix_technicals: When True (default, and the CORRECT behavior), free
            throws awarded by a technical or flagrant foul are inert in the
            possession chain: they neither end nor transfer possession, and
            they cannot pose as an and-1. When False, reproduce bigballR's
            faithful behavior (``all_functions.R:380-436`` has no technical /
            flagrant rules at all, so a made technical FT sets
            ``poss_switch`` and flips ``poss_team``, inventing a possession
            change that never happened). Parity tests pass False. Same
            spirit as ``mbb_ncaa_lineups.fix_tip_in``.

    Returns:
        A 35-column polars frame, one row per event in bigballR's arranged
        order; a zero-row frame with the documented schema when the page has
        no usable play-by-play.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_game_pbp import parse_ncaa_bb_game_pbp
            df = parse_ncaa_bb_game_pbp(html, "6470186")
            print(df.shape)

        WBB quarters::

            df = parse_ncaa_bb_game_pbp(html, "5722355", period_model=(4, 600, 300))

    See Also:
        * `hoopR`_ -- men's college basketball companion (R)
        * `wehoop`_ -- women's college basketball companion (R)

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    n_reg, reg_len, ot_len = period_model
    tables = _extract_tables(html)
    if len(tables) < 3 + n_reg:
        return _empty_pbp()

    # -- line score / metadata (R :94, :141-147) -----------------------------
    line_score = tables[1]
    if not line_score:
        return _empty_pbp()
    # R: ncol(half_scores) == 4 -> 0 OTs, else length - 4 (:106-110);
    # generalized: columns = Team + n_reg + n_OT + Total.
    n_ot = max(0, len(line_score[0]) - (2 + n_reg))
    meta_rows = line_score[1:]
    datetime_str = meta_rows[2][0] if len(meta_rows) > 2 and meta_rows[2] else ""
    game_date = datetime_str[:10]

    # -- period tables: R table[[4]], [[5]], [[5+i]] (:97-117) ---------------
    n_periods = n_reg + n_ot
    if len(tables) < 3 + n_periods:
        return _empty_pbp()
    period_tables = [tables[2 + p] for p in range(1, n_periods + 1)]
    first_pt = period_tables[0]
    if len(first_pt) < 2 or len(first_pt[0]) < 4:
        return _empty_pbp()
    away_team = first_pt[0][1]
    home_team = first_pt[0][3]

    raw_rows: "list[tuple[list[Optional[str]], int]]" = []
    for pnum, pt in enumerate(period_tables, start=1):
        for cells in pt[1:]:
            raw_rows.append((_pad_row(cells, 4), pnum))

    # -- V1/V2 format detection (R :124-136), on the PRE-filter rows ---------
    fh_first: "list[Optional[str]]" = _pad_row(first_pt[1], 4) if len(first_pt) > 1 else [None] * 4
    # ponytail: R hardcodes "20:00:00" (:125); derived from period_model so
    # quarter-format pages can also detect via the first-row branch.
    reg_start_hms = f"{reg_len // 60:02d}:00:00"

    def _any_contains(cells: "Sequence[Optional[str]]", needle: str) -> bool:
        return any(needle in c for c in cells if c is not None)

    is_v2 = (
        (fh_first[0] == reg_start_hms and fh_first[1] in ("game start", "period start", "jumpball startperiod"))
        or _any_contains([c[1] for c, _ in raw_rows], "commercial")
        or _any_contains(fh_first, "Technical")
        or _any_contains(fh_first, "jumpball lost")
        or _any_contains(fh_first, "jumpball won")
    )

    # -- drop rows without a Score cell (R :139) ------------------------------
    kept = [(cells, pnum) for cells, pnum in raw_rows if cells[2] is not None]
    if not kept:
        return _empty_pbp()

    # -- per-row derivation (R :153-343) --------------------------------------
    rows: "list[dict[str, Any]]" = []
    for row_cells, pnum in kept:
        clock = (row_cells[0] or "")[:5]
        tis = _time_in_seconds(clock)
        # elapsed = min(p-1, n_reg)*reg + max(0, p-1-n_reg)*ot + (len - t_remaining)
        end_elapsed = reg_len * pnum if pnum <= n_reg else reg_len * n_reg + ot_len * (pnum - n_reg)
        game_seconds = 0 if tis is None else end_elapsed - tis

        score_parts = (row_cells[2] or "").split("-")
        away_score = _int_or_none(score_parts[0] if score_parts else None)
        home_score = _int_or_none(score_parts[1] if len(score_parts) > 1 else None)

        # merge home/away event columns (R :192-200)
        p1_home = row_cells[3]
        desc = p1_home if p1_home not in (None, "") else row_cells[1]
        event_team = home_team if p1_home == desc else away_team

        events: Optional[str]
        if is_v2:
            # V2: "PLAYER, event" (R :204-241); a 3-token split means the name
            # itself contained a comma and is re-joined (:210-220).
            parts = (desc or "").split(",")
            if len(parts) == 3:
                parts = [parts[0] + parts[1], parts[2]]
            player_name = _normalize_v2_name(parts[0])
            events = _convert_v2_event(parts[1] if len(parts) > 1 else None)
        else:
            # V1: "LAST,FIRST event description" (R :243-277) — player tokens
            # are the all-uppercase words (or literal "Team,").
            toks = (desc or "").split(" ")
            players = " ".join(t for t in toks if t == t.upper() or t == "Team,")
            events = " ".join(t for t in toks if not (t == t.upper() or t == "Team,"))
            player_name = _normalize_v1_name(players)

        if events is None:
            event_type: Optional[str] = None
            event_result: Optional[str] = None
        else:
            words = events.split(" ")
            if words[0] in ("made", "missed"):
                event_type = " ".join(words[1:])
                event_result = words[0]
            else:
                event_type = events
                event_result = None

        rows.append(
            {
                "period": pnum,
                "clock": clock,
                "game_time": _mmss(game_seconds),
                "game_seconds": game_seconds,
                "home_score": home_score,
                "away_score": away_score,
                "event_team": event_team,
                "event_description": desc,
                "player_1": player_name,
                "player_2": None,
                "event_type": event_type,
                "event_result": event_result,
                "priority": _event_priority(event_type),
            }
        )

    # -- arrange (R :348): stable sort, NA scores last within ties ------------
    def _sort_key(r: "dict[str, Any]") -> "tuple[Any, ...]":
        hs, aw = r["home_score"], r["away_score"]
        return (
            r["period"],
            r["game_seconds"],
            hs is None,
            0 if hs is None else hs,
            aw is None,
            0 if aw is None else aw,
            r["priority"],
        )

    rows.sort(key=_sort_key)

    # -- assist merge (R :353-362) --------------------------------------------
    for idx in range(len(rows)):
        nxt = rows[idx + 1] if idx + 1 < len(rows) else None
        if nxt is not None and nxt["event_type"] == "Assist":
            rows[idx]["player_2"] = nxt["player_1"]
            rows[idx]["event_description"] = f"{rows[idx]['event_description']} - {nxt['event_description']}"
    # R filter(Event_Type != "Assist") also NA-drops null event types.
    rows = [r for r in rows if r["event_type"] is not None and r["event_type"] != "Assist"]
    if not rows:
        return _empty_pbp()

    # -- Event_Length + Shot_Value (R :364-378) --------------------------------
    prev_gs: Optional[int] = None
    for r in rows:
        r["event_length"] = r["game_seconds"] if prev_gs is None else r["game_seconds"] - prev_gs
        prev_gs = r["game_seconds"]
        r["shot_value"] = _SHOT_VALUES.get(r["event_type"])

    # -- possession counting (R :380-436) --------------------------------------
    _stamp_possessions(rows, home_team, away_team, fix_technicals=fix_technicals)

    # -- possession validity + Poss_Length + isTransition (R :438-462) ---------
    last_type: "dict[int, Optional[str]]" = {}
    for r in rows:
        last_type[r["poss_num"]] = r["event_type"]
    poss_ids = sorted(last_type)
    end_flag = {p: last_type[p] in _TERMINAL_TYPES for p in poss_ids}
    valid_flag = {p: (end_flag[poss_ids[k - 1]] if k > 0 else False) for k, p in enumerate(poss_ids)}
    first_row_of_poss: "dict[int, dict[str, Any]]" = {}
    cum_len: "dict[int, int]" = {}
    for r in rows:
        p = r["poss_num"]
        cum_len[p] = cum_len.get(p, 0) + r["event_length"]
        r["poss_length"] = cum_len[p]
        if p not in first_row_of_poss:
            first_row_of_poss[p] = r
    for r in rows:
        first = first_row_of_poss[r["poss_num"]]
        first_et = first["event_type"]
        head = (
            first_et in _TRANSITION_FIRST_TYPES
            or (first_et == "Draws Foul" and r["event_team"] == r["poss_team"])
            or (first_et == "Commits Foul" and r["event_team"] != r["poss_team"])
        )
        r["is_transition"] = bool(first["poss_length"] <= 10 and head and valid_flag[r["poss_num"]])

    # -- invalid-substitution bailout (R :470-534) ------------------------------
    # ponytail: "Exits Game" verbatim from R (:472) — the emitted type is
    # "Leaves Game", so player_subs is entries-only; "fixing" it would change
    # the bailout behavior.
    player_subs = [r["player_1"] for r in rows if r["event_type"] in ("Enters Game", "Exits Game")]
    # Team names interpolated unescaped, matching R's paste0-into-regex (:481).
    team_pattern = re.compile(rf"^\.?({home_team.upper()}|{away_team.upper()})$")
    invalid_sub = any(
        _SUB_NUMBER_RE.search(s) is not None or _SUB_TEAM_RE.search(s) is not None or team_pattern.search(s) is not None
        for s in player_subs
        if s is not None
    )
    if len({r["player_1"] for r in rows}) == 1 or invalid_sub or not player_subs:
        return _assemble(
            rows,
            game_id,
            game_date,
            home_team,
            away_team,
            "NO_PLAYER",
            len(rows),
            no_player=True,
        )

    # -- substitution-parity check (R :539-558) ---------------------------------
    status = "CLEAN"
    # R: Game_Seconds != 1200 (:541) — generalized to the interior regulation
    # period boundaries ({1200} for MBB halves; {600, 1200, 1800} for quarters).
    boundaries = {reg_len * k for k in range(1, n_reg)}
    sec_counts: "dict[int, int]" = {}
    for r in rows:
        if r["event_type"] in _SUB_TYPES and r["game_seconds"] not in boundaries:
            sec_counts[r["game_seconds"]] = sec_counts.get(r["game_seconds"], 0) + 1
    if any(c % 2 != 0 for c in sec_counts.values()):
        status = "SUB_MISTAKE"

    # -- starter inference + lineup walk-forward per period (R :560-972) --------
    reg_start = _mmss(reg_len)
    ot_start = _mmss(ot_len)
    home_prev: "list[Optional[str]]" = []
    away_prev: "list[Optional[str]]" = []
    for i in range(1, n_reg + n_ot + 1):
        prs = [r for r in rows if r["period"] == i]
        if not prs:
            continue
        start_clock = reg_start if i <= n_reg else ot_start
        home_leaving = [
            r["player_1"]
            for r in prs
            if r["event_team"] == home_team
            and r["event_type"] == "Leaves Game"
            and r["clock"] != "00:00"
            and r["clock"] != start_clock
        ]
        home_entering = [
            r["player_1"]
            for r in prs
            if r["event_team"] == home_team
            and r["event_type"] == "Enters Game"
            and r["clock"] != "00:00"
            and r["clock"] != start_clock
        ]
        away_leaving = [
            r["player_1"]
            for r in prs
            if r["event_team"] == away_team
            and r["event_type"] == "Leaves Game"
            and r["clock"] != "00:00"
            and r["clock"] != start_clock
        ]
        # ponytail: away entering omits the 00:00 guard — verbatim R asymmetry
        # (all_functions.R:596-602).
        away_entering = [
            r["player_1"]
            for r in prs
            if r["event_team"] == away_team and r["event_type"] == "Enters Game" and r["clock"] != start_clock
        ]

        home_starters = _infer_starters(
            prs,
            rows,
            home_team,
            home_leaving,
            home_entering,
            home_prev,
            start_clock,
            i,
            n_ot,
            dedup_leaving=True,
        )
        away_starters = _infer_starters(
            prs,
            rows,
            away_team,
            away_leaving,
            away_entering,
            away_prev,
            start_clock,
            i,
            n_ot,
            dedup_leaving=False,
        )
        home_prev, away_prev = home_starters, away_starters

        # walk-forward (R :874-965)
        home_exit = list(home_leaving) if home_leaving else [_SENTINEL]
        home_enter = list(home_entering) if home_entering else [_SENTINEL]
        away_exit = list(away_leaving) if away_leaving else [_SENTINEL]
        away_enter = list(away_entering) if away_entering else [_SENTINEL]
        cur_home: "list[Optional[str]]" = list(home_starters)
        cur_away: "list[Optional[str]]" = list(away_starters)
        for r in prs:
            et = r["event_type"]
            tm = r["event_team"]
            p1 = r["player_1"]
            not_zero = r["clock"] != "00:00"
            if (
                et == "Leaves Game"
                and tm == home_team
                and p1 == home_exit[0]
                and home_enter[0] not in cur_home
                and not_zero
            ):
                cur_home = [home_enter[0] if x == home_exit[0] else x for x in cur_home]
                home_enter = _pop_queue(home_enter)
                home_exit = _pop_queue(home_exit)
            elif (
                et == "Leaves Game"
                and tm == away_team
                and p1 == away_exit[0]
                and away_enter[0] not in cur_away
                and not_zero
            ):
                cur_away = [away_enter[0] if x == away_exit[0] else x for x in cur_away]
                away_enter = _pop_queue(away_enter)
                away_exit = _pop_queue(away_exit)
            elif (
                et == "Leaves Game"
                and tm == away_team
                and p1 == away_exit[0]
                and away_enter[0] in cur_away
                and not_zero
            ):
                # entering player already on court — ignore the sub (R :934-944)
                away_enter = _pop_queue(away_enter)
                away_exit = _pop_queue(away_exit)
            elif (
                et == "Leaves Game"
                and tm == home_team
                and p1 == home_exit[0]
                and home_enter[0] in cur_home
                and not_zero
            ):
                home_enter = _pop_queue(home_enter)
                home_exit = _pop_queue(home_exit)
            r["home_lineup"] = cur_home
            r["away_lineup"] = cur_away

    # -- garbage time (R :1016-1034), on the pre-freeze lineups -----------------
    game_home_starters = rows[0]["home_lineup"]
    game_away_starters = rows[0]["away_lineup"]
    total_reg = n_reg * reg_len
    garbage_thresholds = (
        (25, total_reg - 600),
        (20, total_reg - 300),
        (15, total_reg - 120),
    )
    cum_garbage = 0
    for r in rows:
        hs, aw = r["home_score"], r["away_score"]
        if hs is None or aw is None:
            margin_ok = False
        else:
            margin_ok = any(
                abs(hs - aw) >= margin and r["game_seconds"] >= floor for margin, floor in garbage_thresholds
            )
        n_starters_on = sum(1 for x in r["home_lineup"] if x in game_home_starters) + sum(
            1 for x in r["away_lineup"] if x in game_away_starters
        )
        if margin_ok and n_starters_on <= 3:
            cum_garbage += 1
        r["is_garbage_time"] = cum_garbage >= 1

    # -- possession-lineup freeze (R :1037-1049) ---------------------------------
    poss_lineups: "dict[int, tuple[list[Optional[str]], list[Optional[str]]]]" = {}
    for r in rows:
        p = r["poss_num"]
        if p not in poss_lineups:
            poss_lineups[p] = (r["home_lineup"], r["away_lineup"])
        r["home_lineup"], r["away_lineup"] = poss_lineups[p]

    # -- deviations (R :1055-1076) ------------------------------------------------
    n_mistakes = 0
    for r in rows:
        on_court = list(r["home_lineup"]) + list(r["away_lineup"])
        n_match = sum(1 for p in (r["player_1"], r["player_2"]) if p in on_court)
        if (
            n_match == 0
            and r["event_type"] not in ("Leaves Game", "Enters Game", "Free Throw")
            and r["player_1"] is not None
            and r["player_1"] != "TEAM"
        ):
            n_mistakes += 1

    return _assemble(
        rows,
        game_id,
        game_date,
        home_team,
        away_team,
        status,
        n_mistakes,
        no_player=False,
    )


def _fetch_and_parse(
    fetcher: _SupportsFetchGamePbp,
    game_id: object,
    period_model: "tuple[int, int, int]",
) -> pl.DataFrame:
    try:
        return parse_ncaa_bb_game_pbp(fetcher.fetch_game_pbp(game_id), str(game_id), period_model=period_model)
    except Exception:  # noqa: BLE001 — R tryCatch(-> NULL) parity (:1870-1872)
        logger.exception("scrape failed for game id %s", game_id)
        return _empty_pbp()


def _ncaa_bb_game_pbp(
    game_id: object,
    *,
    fetcher: Optional[_SupportsFetchGamePbp] = None,
    period_model: "tuple[int, int, int]" = _MBB_PERIOD_MODEL,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, Any]":
    """League-parameterized single-game scrape (wbb wrapper binds this later)."""
    if fetcher is None:
        from .mbb_ncaa_fetch import NcaaFetcher

        with NcaaFetcher.with_browser() as browser_fetcher:
            html = browser_fetcher.fetch_game_pbp(game_id)
    else:
        html = fetcher.fetch_game_pbp(game_id)
    df = parse_ncaa_bb_game_pbp(html, str(game_id), period_model=period_model)
    return df.to_pandas() if return_as_pandas else df


def _ncaa_bb_play_by_play(
    game_ids: "Sequence[object]",
    *,
    fetcher: Optional[_SupportsFetchGamePbp] = None,
    period_model: "tuple[int, int, int]" = _MBB_PERIOD_MODEL,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, Any]":
    """League-parameterized multi-game driver (bigballR ``get_play_by_play``)."""
    # drop NA ids (R :1859); `x == x` filters float NaN
    ids = [g for g in game_ids if g is not None and g == g]

    def _run(f: _SupportsFetchGamePbp) -> "tuple[list[pl.DataFrame], list[str]]":
        frames: "list[pl.DataFrame]" = []
        removed: "list[str]" = []
        for gid in ids:
            df = _fetch_and_parse(f, gid, period_model)
            if df.height == 0:
                # retry a NULL/empty scrape once (R :1873-1878)
                df = _fetch_and_parse(f, gid, period_model)
            if df.height == 0:
                removed.append(str(gid))
            else:
                frames.append(df)
        return frames, removed

    if fetcher is None:
        from .mbb_ncaa_fetch import NcaaFetcher

        with NcaaFetcher.with_browser() as browser_fetcher:
            frames, removed = _run(browser_fetcher)
    else:
        frames, removed = _run(fetcher)

    if removed:
        logger.warning("%s removed", ",".join(removed))
    out = pl.concat(frames, how="diagonal_relaxed") if frames else _empty_pbp()
    return out.to_pandas() if return_as_pandas else out


@overload
def ncaa_mbb_game_pbp(
    game_id: object,
    *,
    fetcher: Optional[_SupportsFetchGamePbp] = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def ncaa_mbb_game_pbp(
    game_id: object,
    *,
    fetcher: Optional[_SupportsFetchGamePbp] = ...,
    return_as_pandas: Literal[True],
) -> Any: ...


def ncaa_mbb_game_pbp(
    game_id: object,
    *,
    fetcher: Optional[_SupportsFetchGamePbp] = None,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, Any]":
    """Scrape one MBB game's play-by-play (bigballR ``scrape_game``).

    Fetches ``stats.ncaa.org/contests/{game_id}/play_by_play`` and parses it
    through :func:`parse_ncaa_bb_game_pbp` with the MBB period model
    ``(2, 1200, 300)``.

    Args:
        game_id: NCAA contest id (e.g. ``"6470186"``).
        fetcher: Optional injected fetcher exposing ``fetch_game_pbp`` (for
            tests/offline use). Defaults to a fresh
            ``NcaaFetcher.with_browser()`` context per call.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The 35-column play-by-play frame (zero rows when the game is not
        found).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_game_pbp import ncaa_mbb_game_pbp
            df = ncaa_mbb_game_pbp("6470186")
            print(df.shape)

        Offline with an injected fetcher::

            df = ncaa_mbb_game_pbp("6470186", fetcher=my_fetcher)

        Pipeline next step (one line)::

            df.filter(pl.col("event_type") == "Three Point Jumper").head()

    See Also:
        * `hoopR`_ -- men's college basketball companion (R)

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return _ncaa_bb_game_pbp(
        game_id,
        fetcher=fetcher,
        period_model=_MBB_PERIOD_MODEL,
        return_as_pandas=return_as_pandas,
    )


@overload
def ncaa_mbb_play_by_play(
    game_ids: "Sequence[object]",
    *,
    fetcher: Optional[_SupportsFetchGamePbp] = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def ncaa_mbb_play_by_play(
    game_ids: "Sequence[object]",
    *,
    fetcher: Optional[_SupportsFetchGamePbp] = ...,
    return_as_pandas: Literal[True],
) -> Any: ...


def ncaa_mbb_play_by_play(
    game_ids: "Sequence[object]",
    *,
    fetcher: Optional[_SupportsFetchGamePbp] = None,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, Any]":
    """Scrape many MBB games' play-by-play (bigballR ``get_play_by_play``).

    Ports ``bigballR/R/all_functions.R:1857-1897``: drops missing ids, shares
    one fetcher session across games, retries each empty/failed game once,
    row-binds the survivors, and logs the removed ids.

    Args:
        game_ids: NCAA contest ids; ``None``/NaN entries are dropped.
        fetcher: Optional injected fetcher exposing ``fetch_game_pbp``.
            Defaults to one shared ``NcaaFetcher.with_browser()`` context.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        Row-bound play-by-play for every game that scraped successfully
        (zero-row contract frame when none did).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_game_pbp import ncaa_mbb_play_by_play
            df = ncaa_mbb_play_by_play(["6470186", "6479639"])
            print(df.shape)

        Pipeline next step (one line)::

            df.group_by("game_id").len()

    See Also:
        * `hoopR`_ -- men's college basketball companion (R)

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return _ncaa_bb_play_by_play(
        game_ids,
        fetcher=fetcher,
        period_model=_MBB_PERIOD_MODEL,
        return_as_pandas=return_as_pandas,
    )
