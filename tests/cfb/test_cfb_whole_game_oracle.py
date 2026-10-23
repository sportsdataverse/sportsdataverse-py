"""Oracle-anchored whole-game characterization of the CFB processor (quality program task 1.2).

Every committed ESPN capture under ``tests/cfb/fixtures/summary_*.json[.gz]`` (provenance:
``tests/cfb/fixtures/README.md``) runs offline through ``CFBPlayProcess(...).run_processing_pipeline()``
once per process, and each column below is checked against an oracle that is NOT this processor's
output. Nothing here reads a stored copy of a processed frame: a snapshot of today's output would pin
the open defects as correct. A known-wrong column is ``xfail(strict=True, raises=AssertionError)`` with
the audit finding id in its reason (``skills_outputs/CONSOLIDATED-TIER-C.md`` /
``CONSOLIDATED-ADDENDUM-WAVES-4-6.md``), so the fix turns it into a strict XPASS (red) and must flip it
to a plain assert in the same PR. Every comparison is null-aware: a null where the oracle has a value
is a mismatch, never a pass. The module is marked ``whole_game`` (about 6 minutes serially, 2 at
``-n 4 --dist loadgroup``); CI runs it in its own job and ``-m "not whole_game"`` skips it locally.

Oracle per column
=================

``firstHalfKickoffTeamId``
    The ESPN feed: the kicking team of the play the feed files first in period 1, when that play is a
    kickoff -- cfbfastR's rule (``cfbfastR/R/cfbd_pbp_data.R:1288``, ``receives_2H_kickoff`` from the
    defense of game play 1, i.e. the kicker). A game-wide constant. ESPN's ``start.team.id`` names the
    kicker, except on 400559176 where it names the receiver; ``_opening_kicker`` settles that from the
    snap order. Cross-checked against cfbfastR's R output
    (``tests/cfb/fixtures/cfbfastr_r_opening_kickoff.csv``): it agrees on all 64 captures where both
    define the kicker. Asserted on the 85 captures whose feed opens period 1 with a kickoff (43 kicked
    by the home team). On the other 41 (32 feeds from 2004-2012 that file a scrimmage play first, and 9
    later feeds that lack the opening kickoff or number a later play ahead of it) nothing is asserted.
    Green since task 2.6 (C-NF-2 / C-M13: it was a per-row expression naming the receiving team on row 1
    and the away team on every later row). A second oracle reads period 3 alone: the opening kicker
    receives the second-half kickoff, so it takes the first period-3 snap. Asserted on 118 captures,
    34 of them among the 41 above (``_opening_kicker``'s first-snap-defense fallback); 6 are
    undetermined (the half's start missing or misnumbered, an onside or return-touchdown kickoff) and 2
    contradict their own period 1 (``_P3_CONTRADICTS_P1``).
``start/end.pos_team_receives_2H_kickoff``
    First-half rows: cfbfastR ``cfbd_pbp_data.R:1329-1332``, true exactly when the possession team is
    the opening kicker (the 85 captures above). Rows after the first half: false on every capture,
    nflfastR's ``receive_2h_ko`` gate (cfbfastR leaves the flag ungated there; task 2.6 chose the
    nflfastR rule, as the NFL processor does). Green since task 2.6 (C-NF-2).
``start.yardsToEndzone.touchback`` (kickoff rows)
    The NCAA rule, sdv-internal-refs ``rules/cfb.yaml#cfb-2012-kickoff-35-touchback-25``: 80 through
    2011, 75 from 2012. Owner ruling: through 2011 the RULE is the oracle, not cfbfastR -- cfbfastR's
    ``create_epa.R:233`` sets 75 for every season, which is the R bug (a separate cfbfastR PR fixes
    it). Green on every capture: main's #778 (2b73fa5a5e) moved the 2012-13 spot to 75 and closed C-H4.
``end_of_half`` and ``EPA`` on the last real play of each half
    cfbfastR ``cfbd_pbp_data.R:1366`` (the half's last play ends it), ``:1261`` (a Timeout row is not
    a play) and ``create_epa.R:361`` (a non-scoring half-ending play's EPA is ``-ep_before``). The
    first half is the run of rows before the first period-3 row (chronology, not the period label: an
    overtime touchdown ESPN labels period 2 must not stand in for the Q2 closer); the game end is
    checked on games without overtime. Green since task 2.2 (C-NF-12) on the Timeout-closed halves too.
``end.down`` / ``end.yardsToEndzone`` where ESPN's end block is ``{down 0, yardLine 0, yardsToEndzone 0}``
    The ESPN feed: when the same team snaps next, the play ended where that snap starts. Asserted on
    feeds that carry end blocks (a feed whose end blocks are mostly degenerate, e.g. 252532751 from
    2005, has no end state to trust and is skipped), on the plays the processor emits. Green on fumble
    plays since task 3.4(a), and on the 7 captures whose degenerate end is on a rush, a pass or a
    penalty since EX-3.4-c (a declined flag, an enforced flag and a gain each end where the next snap
    starts; the full text of 401761622's "Roughing The Passer declined ... Pass Interference 15 yards
    from GASO21 to GASO36, 1ST DOWN" names that spot).
``EPA`` of 401856682's Q4 Fumble Recovery (Own)
    EP at the next snap's start minus ``EP_start``, within 0.1 (the addendum's tolerance; the Yahoo and
    CBS adapters score -0.39, the next-snap state gives -0.41). The state comes from the feed; the EP
    values are the processor's own model evaluated at that state, so this pins the end STATE, not the
    model. Green since task 3.4(a) (supp-002; its 23-candidate count is
    ``tests/cfb/test_cfb_supp002_degenerate_end.py``).
``line_yards``
    The standard per-yard adjusted-line-yards band (the ruling for plan task 2.1 in
    ``audit/EXECUTION-FINDINGS.md``; Action Network's ALY definition, Football Study Hall): a loss
    counts 120%, yards 0-4 100%, yards 5-10 50%, yards beyond 10 nothing, so a gain is capped at 7. Not
    the summary producers' formula; ``adj_rush_yardage`` is the carry capped at 10 and ``highlight_yards``
    the carry less ``line_yards`` on a gain. Green on every carry since task 2.1 (C-NF-3: the processor credited
    100% through 3, 50% of 4-8, cap 5.5).
``yds_rushed``
    The feed's text states every rush's yardage (a number, "no gain", "loss of N"); a rush row without
    it is a parse gap. XFAIL EX-1.2-a (a finding surfaced by this suite's null-aware review) on the 6
    captures where a rush's text states no carry (#768 closed 27 of the original 42, task 3.4(f) 9 more:
    "for 1 yard", "loss of N" without "a", a name ending "yard" before "rush", directional gains on
    fumbled or flagged rushes, a "1 Yd Run" with a two-point try); the line-yards checks below read only
    the rows that carry a yardage.
``line_yards + second_level_yards + open_field_yards`` on gains
    The decomposition identity: the three pieces sum to ``yds_rushed``. Green (C-H1's CFB twin).
``period``
    The feed's chronology: never decreases along the emitted rows. XFAIL C-M18 on 401628439 only (the
    period label is wrong; ``errors.PlayOrderWarning`` names it).
``game_play_number``, ``lag_half``
    Recomputed after the final sort: ``game_play_number`` strictly increases along the rows and
    ``lag_half`` is the previous row's ``half`` (null on row 1). Green (C-L1, fixed in 3.4(b2)).
``id``
    The feed: every emitted row is a feed play, emitted once (C-L11's row-count diagnostic). Green.
feed ``text`` literals
    The case-sensitive literals the rush/pass flags match (``cfb_pbp.py`` "sacked", "pass complete",
    ...) meet no other casing in any capture, so cfbfastR's ``ignore_case`` matching would agree
    (C-L16, latent). Green.
schema
    One dtype per column across the captures, except the rows of ``tests/cfb/schema_allowlist.yaml``
    (``scope: across_captures``, each listing exactly the captures that deviate from the reference
    401628455; C-M8's CFB half, C-L5, C-L10); the columns CFB shares with NFL agree in dtype except that
    file's ``scope: vs_nfl`` rows (CFB 401628455 against NFL 401872922; C-M8, C-M7). Both halves are
    exact both ways, so the allowlist only shrinks.
odds round trip (401301042)
    The offline-reprocess contract (CLAUDE.md, 0.0.52): the payload's resolved odds persist as JSON
    and, fed back as ``odds_override``, rebuild the same frame; the persisted values are ESPN's
    pickcenter numbers. Green since task 3.4(b1) (C-M19).

Not oracles, and why. ``cfbfastR-cfb-data`` publishes this processor's own output (comparing to it is
the snapshot ban in another form). ``cfbfastR-data`` holds three play-by-play trees: ``pbp/`` (2002-21)
was written by pandas from the 2021-22 Python processor, and ``cfb/pbp/`` (2004-22) is that processor's
final JSON re-serialized by ``R/espn_cfb_01_pbp_creation.R`` -- both are this processor's ancestor
(they carry its 2012-13 touchback at 80), so no EP/WP tolerance against them would be independent. The
third, ``data/{rds,parquet}/pbp_players_pos_{2014..2026}``, IS cfbfastR's R output
(``cfbd_pbp_data(..., epa_wpa = TRUE)`` on CFBD data); its game-level ``receives_2H_kickoff`` is the
second kicker oracle above. Its play-level values are not used: CFBD rows do not join ESPN rows one to
one.
"""

from __future__ import annotations

import copy
import functools
import gzip
import json
import re
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pytest
import yaml

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess
from sportsdataverse.errors import PlayOrderWarning

FIX = Path(__file__).parent / "fixtures"
_CAPTURE = re.compile(r"^summary_(\d+)(?:_trimmed)?\.json(?:\.gz)?$")
_KICKOFF_TYPE = re.compile(r"(?i)kickoff|onside")
_NOT_A_PLAY = re.compile(r"(?i)timeout|end of|end period")
_NOT_A_SNAP = re.compile(r"(?i)kickoff|timeout|end of|end period|extra point|two.point|2pt|coin toss")
#: a kickoff the kicking team may have recovered: the next snap says nothing about who kicked
_KICK_TURNOVER = re.compile(r"(?i)fumble|recover|muff|onside")
_TRY_OR_KICK_OR_SCORE = re.compile(
    r"(?i)touchdown|field goal good|safety|extra point|two.point|2pt|conversion|kickoff",
)
#: the case-sensitive text literals of the rush/pass flags (cfb_pbp.py, ``pass`` alias); "run for"
#: is case-sensitive on purpose ("(X Run for Two-Point Conversion)" is a try) and stays out
_FLAG_LITERALS = ("sacked", "pass complete", "pass incomplete", "pass intercepted")

#: |EPA + EP_start| on a non-scoring half-ending play: 0.0 on every green capture (both Float32,
#: the same value negated), so the bound is one Float32 ulp at EP ~ 7
HALF_END_EPA_ABS_TOL = 1e-6
#: line_yards, second_level_yards and open_field_yards are exact halves (Float64); observed 0.0
RUSH_YARDS_ABS_TOL = 1e-9
#: supp-002's bound (addendum W-M6): the fixed EPA within 0.1 of the next-snap value
SUPP_002_EPA_ABS_TOL = 0.1
#: committed row count of tests/cfb/schema_allowlist.yaml: it may only go down
SCHEMA_ALLOWLIST_MAX_ROWS = 17  # task 5.2 removed the 24 vs_nfl id rows

# Processing a default-odds capture warns DefaultOddsWarning incidentally;
# tests/cfb/test_cfb_pbp_offline.py::test_default_odds_warn asserts it.
# 401628439 warns PlayOrderWarning; test_period_monotonic_or_the_step_back_warns asserts it.
pytestmark = [
    pytest.mark.whole_game,
    pytest.mark.filterwarnings("ignore::sportsdataverse.errors.DefaultOddsWarning"),
    pytest.mark.filterwarnings("ignore::sportsdataverse.errors.PlayOrderWarning"),
]


def _load(path: Path) -> dict:
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            return json.load(fh)
    return json.loads(path.read_text(encoding="utf-8"))


def _paths() -> dict[int, Path]:
    out: dict[int, Path] = {}
    for path in sorted(FIX.glob("summary_*.json*")):
        hit = _CAPTURE.match(path.name)
        if hit:
            out[int(hit.group(1))] = path
    return out


PATHS = _paths()
CAPTURES = tuple(sorted(PATHS))


@dataclass(frozen=True)
class Feed:
    """What the raw ESPN capture says, read without the processor."""

    game_id: int
    season: int
    home_id: int
    away_id: int
    opening_kicker: int | None
    #: the team that received the second-half kickoff, read from period 3 alone; ``None`` when undetermined
    second_half_receiver: int | None
    play_ids: frozenset[int]
    #: (play id, next snap's start.down, next snap's start.yardsToEndzone)
    degenerate_ends: tuple[tuple[int, int, int], ...]
    texts: tuple[str, ...]


def _type(play: dict) -> str:
    return (play.get("type") or {}).get("text") or ""


def _opening_kicker(period1: list[dict], home: int, away: int) -> int | None:
    """The kicking team of the play the feed files first in period 1, or ``None`` if that is no kickoff.

    ESPN's kickoff ``start.team.id`` is the kicking team, except on a few opening kickoffs where it
    holds the receiver (400559176: "N/A kickoff for 55 yds, Daryl Collins return ... to the WAlab 33",
    filed under West Alabama, who then snaps). The snap order settles it: with no turnover on the kick
    and no second kickoff before it (a return touchdown), the first scrimmage snap belongs to the
    receiver, so a snap by the labelled team means the label is the receiver.
    """
    if not period1 or not _KICKOFF_TYPE.search(_type(period1[0])):
        return None
    kickoff = period1[0]
    labelled = int(kickoff["start"]["team"]["id"])
    if _KICK_TURNOVER.search(kickoff.get("text") or ""):
        return labelled
    for play in period1[1:]:
        kind = _type(play)
        if _KICKOFF_TYPE.search(kind):
            return labelled
        if _NOT_A_SNAP.search(kind) or "penalty" in kind.lower() or not (play.get("start") or {}).get("down"):
            continue
        if int(play["start"]["team"]["id"]) == labelled:
            return away if labelled == home else home
        return labelled
    return labelled


def _clock_secs(play: dict) -> int | None:
    hit = re.match(r"^(\d{1,2}):(\d{2})$", (play.get("clock") or {}).get("displayValue") or "")
    return int(hit.group(1)) * 60 + int(hit.group(2)) if hit else None


def _second_half_receiver(period3: list[dict]) -> int | None:
    """The team that received the second-half kickoff, read from period 3 alone, or ``None``.

    The receiver snaps first: the first scrimmage snap of period 3 belongs to it, whether or not the
    feed files the kickoff (a 2025 feed types it "Pass Completion"; the text still says kickoff).
    ``None`` (6 captures) when the feed's first period-3 play comes after the opening minute (the
    half's start is missing, 242972641 and 252740026, or a later play is numbered first, 322430041 and
    401425418), or the kickoff is onside, ends in a turnover or a return touchdown, or a second kickoff
    comes before any snap: then the first snap says nothing about who received (400548250, 272650194).
    """
    rows = [p for p in period3 if not re.search(r"(?i)timeout|end of|end period|coin toss|penalty", _type(p))]
    if not rows or (_clock_secs(rows[0]) or 0) < 14 * 60:
        return None
    kicked = False
    for play in rows:
        kickoff = _KICKOFF_TYPE.search(_type(play)) or re.search(r"(?i)\bkickoff\b", play.get("text") or "")
        if kickoff:
            if kicked or re.search(r"(?i)fumble|recover|muff|onside|touchdown", play.get("text") or ""):
                return None
            kicked = True
            continue
        if _NOT_A_SNAP.search(_type(play)) or not (play.get("start") or {}).get("down"):
            continue
        return int(play["start"]["team"]["id"])
    return None


def _feed(game_id: int, summary: dict) -> Feed:
    plays = [p for d in (summary.get("drives") or {}).get("previous", []) for p in d.get("plays", [])]

    def period(n: int) -> list[dict]:
        return sorted(
            (p for p in plays if (p.get("period") or {}).get("number") == n),
            key=lambda p: int(p.get("sequenceNumber") or 0),
        )

    period1 = period(1)
    sides = {c["homeAway"]: int(c["team"]["id"]) for c in summary["header"]["competitions"][0]["competitors"]}
    kicker = _opening_kicker(period1, sides["home"], sides["away"])

    def degenerate(p: dict) -> bool:
        end = p.get("end") or {}
        return end.get("down") == 0 and end.get("yardLine") == 0 and end.get("yardsToEndzone") == 0

    ends: list[tuple[int, int, int]] = []
    # a feed whose end blocks are mostly {0, 0, 0} carries no end state to check against (2005)
    if plays and sum(degenerate(p) for p in plays) < len(plays) / 2:
        for i, play in enumerate(plays):
            if not degenerate(play) or _NOT_A_SNAP.search(_type(play)):
                continue
            nxt = next((q for q in plays[i + 1 :] if not _NOT_A_PLAY.search(_type(q))), None)
            if nxt is None or _NOT_A_SNAP.search(_type(nxt)):
                continue
            start, nstart = play.get("start") or {}, nxt.get("start") or {}
            if (nstart.get("team") or {}).get("id") != (start.get("team") or {}).get("id"):
                continue
            if not nstart.get("down"):
                continue
            ends.append((int(play["id"]), int(nstart["down"]), int(nstart["yardsToEndzone"])))
    return Feed(
        game_id=game_id,
        season=int(summary["header"]["season"]["year"]),
        home_id=sides["home"],
        away_id=sides["away"],
        opening_kicker=kicker,
        second_half_receiver=_second_half_receiver(period(3)),
        play_ids=frozenset(int(p["id"]) for p in plays),
        degenerate_ends=tuple(ends),
        texts=tuple(p.get("text") or "" for p in plays),
    )


FEEDS = {gid: _feed(gid, _load(path)) for gid, path in PATHS.items()}

_COLUMNS = (
    "id",
    "season",
    "period",
    "half",
    "lag_half",
    "game_play_number",
    "type.text",
    "text",
    "end_of_half",
    "scoring_play",
    "EPA",
    "EP_start",
    "EP_end",
    "start.pos_team.id",
    "end.pos_team.id",
    "firstHalfKickoffTeamId",
    "start.pos_team_receives_2H_kickoff",
    "end.pos_team_receives_2H_kickoff",
    "kickoff_play",
    "start.yardsToEndzone.touchback",
    "rush",
    "yds_rushed",
    "statYardage",
    "fumble_vec",
    "penalty_flag",
    "penalty_detail",
    "line_yards",
    "second_level_yards",
    "open_field_yards",
    "adj_rush_yardage",
    "highlight_yards",
    "opp_highlight_yards",
    "end.down",
    "end.yardsToEndzone",
    "wp_before",
    "wp_after",
)


@dataclass(frozen=True)
class Run:
    frame: pl.DataFrame
    schema: dict[str, str]


#: real captures outside the ``summary_*`` set (so not whole-game oracle captures), read by one test each;
#: provenance in tests/cfb/fixtures/README.md
_FORM_FIXTURES = {282782655: FIX / "rush_forms" / "282782655_trimmed.json.gz"}


@functools.lru_cache(maxsize=None)
def _run(game_id: int) -> Run:
    """Process one capture offline, once per process; keep the columns the oracles read."""
    proc = CFBPlayProcess(gameId=game_id, join_participants=False)
    proc.espn_cfb_pbp(summary=_load(PATHS.get(game_id) or _FORM_FIXTURES[game_id]))
    proc.run_processing_pipeline()
    plays = proc.plays_frame
    return Run(frame=plays.select(_COLUMNS), schema={c: str(t) for c, t in plays.schema.items()})


def _frame(game_id: int) -> pl.DataFrame:
    return _run(game_id).frame


def _params(game_ids, red: dict[int, str] | None = None) -> list:
    """One param per capture, grouped per game for ``--dist loadgroup`` so a game is processed once."""
    red = red or {}
    out = []
    for gid in game_ids:
        marks = [pytest.mark.xdist_group(f"cfb-oracle-{gid}")]
        if gid in red:
            marks.append(pytest.mark.xfail(strict=True, raises=AssertionError, reason=red[gid]))
        out.append(pytest.param(gid, marks=marks, id=str(gid)))
    return out


# --- C-NF-2 / C-M13: the opening-kickoff team and the 2H-kickoff flag -------------------------------

_KICKOFF_DEFINED = tuple(gid for gid in CAPTURES if FEEDS[gid].opening_kicker is not None)


@pytest.mark.parametrize("game_id", _params(_KICKOFF_DEFINED))
def test_first_half_kickoff_team_constant_is_the_opening_kicker(game_id):
    kicker = FEEDS[game_id].opening_kicker
    df = _frame(game_id)
    named = df["firstHalfKickoffTeamId"].cast(pl.Int64)
    assert named.null_count() == 0, f"{named.null_count()} of {df.height} rows name no team"
    assert named.n_unique() == 1, f"not a game-wide constant: {sorted(named.unique().to_list())}"
    wrong = int(named.ne_missing(kicker).sum())
    assert wrong == 0, f"{wrong} of {df.height} rows name a team other than the opening kicker {kicker}"


@pytest.mark.parametrize("game_id", _params(_KICKOFF_DEFINED))
def test_receives_2h_kickoff_on_first_half_rows_is_the_opening_kicker(game_id):
    kicker = FEEDS[game_id].opening_kicker
    first_half = _frame(game_id).filter(pl.col("period") <= 2)
    for side in ("start", "end"):
        # a row with no possession team does not belong to the kicker (the oracle's side only)
        expected = (first_half[f"{side}.pos_team.id"].cast(pl.Int64) == kicker).fill_null(False)
        got = first_half[f"{side}.pos_team_receives_2H_kickoff"]
        wrong = int(expected.ne_missing(got).sum())
        assert wrong == 0, f"{side}: {wrong} of {first_half.height} first-half rows carry the inverted flag"


@pytest.mark.parametrize("game_id", _params(CAPTURES))
def test_receives_2h_kickoff_is_false_after_the_first_half(game_id):
    """Nobody is still waiting for the second-half kickoff once it has happened (nflfastR's gate)."""
    later = _frame(game_id).filter(pl.col("period") > 2)
    # every committed capture, trimmed or not, keeps rows after the first half; without them this passes vacuously
    assert later.height > 0, "no rows after the first half to check"
    for side in ("start", "end"):
        flagged = int(later[f"{side}.pos_team_receives_2H_kickoff"].ne_missing(False).sum())
        assert flagged == 0, f"{side}: {flagged} of {later.height} rows after the first half carry the flag"


#: Feeds whose period 3 contradicts their period 1: the same team takes the first snap of both halves
_P3_CONTRADICTS_P1 = {
    262522509: "2006: Miami (OH) snaps first in both halves; period 3 opens with Purdue's kickoff",
    322590228: "2012: Furman snaps first in both halves; the feed files no kickoff before either snap",
}
_RECEIVER_DEFINED = tuple(
    gid for gid in CAPTURES if FEEDS[gid].second_half_receiver is not None and gid not in _P3_CONTRADICTS_P1
)


@pytest.mark.parametrize("game_id", _params(_RECEIVER_DEFINED))
def test_first_half_kickoff_team_received_the_second_half_kickoff(game_id):
    """The opening kicker receives the second-half kickoff, read from period 3 alone (R781-3).

    Independent of period 1, so it also covers the captures with no opening kickoff, where
    ``_opening_kicker`` falls back to the first snap's defense.
    """
    receiver = FEEDS[game_id].second_half_receiver
    named = _frame(game_id)["firstHalfKickoffTeamId"].cast(pl.Int64)
    wrong = int(named.ne_missing(receiver).sum())
    assert wrong == 0, f"{wrong} of {named.len()} rows name a team other than the second-half receiver {receiver}"


def test_the_second_half_oracle_covers_the_fallback_captures():
    """Guard the guard: 123 captures checked, 36 of them without an opening kickoff in period 1."""
    assert len(_RECEIVER_DEFINED) == 123
    fallback = [FEEDS[g] for g in _RECEIVER_DEFINED if g not in _KICKOFF_DEFINED]
    assert len(fallback) == 36
    assert {f.second_half_receiver == f.home_id for f in fallback} == {True, False}


def test_the_kickoff_oracle_covers_the_captures_it_should():
    """Guard the guard: the feed names the opening kicker on 89 of the 132 captures, 44 of them home."""
    kicked = [FEEDS[g] for g in _KICKOFF_DEFINED]
    assert len(kicked) == 89
    assert sum(1 for f in kicked if f.opening_kicker == f.home_id) == 44
    # 400547866 (Kansas @ West Virginia, 2014): the away team kicks, the home team receives
    assert FEEDS[400547866].opening_kicker == 2305
    # 400559176 (West Alabama @ Jacksonville State, 2014): ESPN files the kickoff under the receiver,
    # West Alabama (2695), who snaps next; Jacksonville State (55, home) kicked
    assert FEEDS[400559176].opening_kicker == 55


def test_the_kickoff_oracle_agrees_with_cfbfastr_r():
    """The second, independent kicker oracle: cfbfastR's R output (CFBD data, ``receives_2H_kickoff``).

    Compared where cfbfastR's game play 1 is the opening kickoff (otherwise its rule falls back to the
    first scrimmage play's defense, which CFBD's play order makes unreliable) and the feed names the
    kicker. cfbfastR names teams by school; ``team.location`` in the capture header maps them.
    """
    rows = pl.read_csv(
        FIX / "cfbfastr_r_opening_kickoff.csv",
        schema_overrides={"game_id": pl.Int64, "r_receives_2h_kickoff": pl.Int64},
        null_values="NA",
    )
    compared = 0
    for row in rows.iter_rows(named=True):
        feed = FEEDS.get(row["game_id"])
        if feed is None or feed.opening_kicker is None or "Kickoff" not in row["r_play1_type"]:
            continue
        school = row["r_home"] if row["r_receives_2h_kickoff"] == 1 else row["r_away"]
        teams = _load(PATHS[row["game_id"]])["header"]["competitions"][0]["competitors"]
        by_school = {c["team"]["location"]: int(c["team"]["id"]) for c in teams}
        assert by_school.get(school) == feed.opening_kicker, (row["game_id"], school, by_school, feed.opening_kicker)
        compared += 1
    assert compared == 64


# --- C-H4: the kickoff touchback spot per season --------------------------------------------------

# C-H4 (80 through 2013) was a strict xfail on the 2012-13 captures until main's #778 (2b73fa5a5e,
# "2012+ kickoff touchbacks at the 25") reached this branch; with #778's touchback hunk reverted those
# 7 captures go red again.


def _touchback_rule(season: int) -> int:
    return 80 if season <= 2011 else 75


@pytest.mark.parametrize(
    "game_id",
    _params(CAPTURES),
)
def test_kickoff_touchback_spot_follows_the_rule(game_id):
    season = FEEDS[game_id].season
    kickoffs = _frame(game_id).filter(pl.col("kickoff_play") == True)
    assert kickoffs.height > 0
    spots = set(kickoffs["start.yardsToEndzone.touchback"].to_list())
    assert spots == {_touchback_rule(season)}, f"{season}: kickoff touchback spot {spots}"


# --- C-NF-12: end_of_half on the last real play ---------------------------------------------------


def _half_closers(df: pl.DataFrame) -> list[tuple[str, dict]]:
    """The last non-Timeout row of the first half and, without overtime, of the game.

    The first half is the run of rows from the first period-1/2 row up to the first period-3+ row
    after it: chronology, not a ``period <= 2`` filter, which on 401628439 picks an overtime touchdown
    ESPN labels period 2 (and on 401425418 a third-quarter kickoff sorts first).
    """
    rows = df.with_row_index("row")
    period = rows["period"].to_list()
    start = next(i for i, p in enumerate(period) if p is not None and p <= 2)
    end = next((i for i in range(start, len(period)) if period[i] is None or period[i] >= 3), len(period))
    real = pl.col("type.text").fill_null("") != "Timeout"
    out = [("first half", rows.slice(start, end - start).filter(real).tail(1).row(0, named=True))]
    if df["period"].max() <= 4:  # overtime games end on an overtime possession, a separate rule set
        out.append(("game", rows.filter(real).tail(1).row(0, named=True)))
    return out


@pytest.mark.parametrize("game_id", _params(CAPTURES))
def test_end_of_half_timeout_ending_lands_on_the_last_real_play(game_id):
    """C-NF-12: named for gates/cfb.md's ``-k timeout_ending``; checks every capture's half-closing rows."""
    for which, row in _half_closers(_frame(game_id)):
        assert row["end_of_half"] is True, f"{which}: row {row['row']} ({row['type.text']}) is not end_of_half"
        scoring = row["scoring_play"] is True or bool(_TRY_OR_KICK_OR_SCORE.search(row["type.text"] or ""))
        if not scoring:
            epa, ep_start = row["EPA"], row["EP_start"]
            assert epa is not None and ep_start is not None, f"{which}: row {row['row']} EPA {epa}, EP_start {ep_start}"
            assert abs(epa + ep_start) <= HALF_END_EPA_ABS_TOL, (
                f"{which}: row {row['row']} EPA {epa} != -EP_start {-ep_start}"
            )


# Task 2.2 (C-NF-12) flipped 292810142, 401752753, 401761656, 401858213 (the game ends on a Timeout row)
# and 401762858 (the first half does) in the test above. The Timeout-ending boundaries of the captures,
# overtime games included: the Timeout row took end_of_half (EPA 0.0) and the play before it kept a
# normal EP_end - EP_start.
_TIMEOUT_ENDINGS = (
    (292810142, "game"),
    (401752753, "game"),
    (401754585, "game"),
    (401761656, "game"),
    (401858213, "game"),
    (401858224, "game"),
    (401762858, "first half"),
    (401761665, "regulation"),
)


def _boundary_rows(game_id: int, which: str) -> pl.DataFrame:
    rows = _frame(game_id).with_row_index("row")
    if which == "first half":
        period = rows["period"].to_list()
        start = next(i for i, p in enumerate(period) if p is not None and p <= 2)
        end = next((i for i in range(start, len(period)) if period[i] is None or period[i] >= 3), len(period))
        rows = rows.slice(start, end - start)
    elif which == "regulation":
        rows = rows.filter(pl.col("period") <= 4)
    return rows


@pytest.mark.parametrize(("game_id", "which"), _TIMEOUT_ENDINGS)
def test_timeout_ending_boundary_ends_on_the_last_real_play(game_id, which):
    rows = _boundary_rows(game_id, which)
    timeout = rows.tail(1).row(0, named=True)
    if timeout["type.text"] != "Timeout":
        pytest.fail(f"{game_id} {which} no longer ends on a Timeout row ({timeout['type.text']})")
    last = rows.filter(pl.col("type.text").fill_null("") != "Timeout").tail(1).row(0, named=True)
    assert timeout["end_of_half"] is False, f"Timeout row {timeout['row']} took end_of_half"
    assert last["end_of_half"] is True, f"row {last['row']} ({last['type.text']}) is not end_of_half"
    if last["scoring_play"] is not True:
        assert abs(last["EPA"] + last["EP_start"]) <= HALF_END_EPA_ABS_TOL, (last["row"], last["EPA"])


def test_timeout_ending_401752753_last_scrimmage_play_scores_minus_ep_start():
    # The kneel before 'Timeout Arkansas' kept EPA -1.195 (EP_end - EP_start); the play that ends the
    # game without scoring scores -EP_start (the audit's measured -4.986, gaps-C p20/p24).
    rows = _frame(401752753)
    last = rows.filter(pl.col("type.text").fill_null("") != "Timeout").tail(1).row(0, named=True)
    assert abs(last["EPA"] - -4.986) < 5e-4, last["EPA"]


@pytest.mark.parametrize("game_id", [g for g, which in _TIMEOUT_ENDINGS if which == "game"])
def test_timeout_ending_game_last_play_wp_after_is_the_result(game_id):
    # The play that ends the game is followed by Timeout rows only; its wp_after is the result for
    # the team that had the ball (ESPN's header names the winner). 401761656's game-ending sack read
    # 0.064 for the team that won.
    competitors = _load(PATHS[game_id])["header"]["competitions"][0]["competitors"]
    winner = next(str(c["id"]) for c in competitors if c.get("winner"))
    last = _frame(game_id).filter(pl.col("type.text") != "Timeout").tail(1).row(0, named=True)
    expected = 1.0 if str(last["start.pos_team.id"]) == winner else 0.0
    assert last["wp_after"] == expected, (last["id"], last["type.text"], last["wp_after"], expected)


def test_timeout_ending_regulation_wp_after_is_overtime_wp_before():
    # 401761665 ties at the end of regulation; ESPN files the Timeout row after the last play to the
    # other team (start.pos_team 309), so borrowing the next ROW's wp_before read it as a possession
    # change. The last real play's wp_after is the next real play's (overtime's first snap, the same
    # team's) wp_before: WP does not move between two snaps nothing happened between.
    f = _frame(401761665).with_row_index("row")
    last = _boundary_rows(401761665, "regulation").filter(pl.col("type.text") != "Timeout").tail(1)
    i = last["row"].item()
    nxt = f.filter((pl.col("row") > i) & (pl.col("type.text") != "Timeout")).head(1).row(0, named=True)
    last = last.row(0, named=True)
    if nxt["start.pos_team.id"] != last["start.pos_team.id"]:
        pytest.fail("401761665's first overtime snap is no longer the same team's")
    assert abs(last["wp_after"] - nxt["wp_before"]) < 1e-9, (last["wp_after"], nxt["wp_before"])


def test_timeout_ending_half_end_possession_change_wp_after_is_one_minus_next_plays():
    # R832-4, an edited capture: 401234617's first half ends on a punt (Georgia Tech 59 -> Pitt 103)
    # and ESPN's "End of 2nd Quarter" marker row; the edit retypes that marker row as a Timeout row
    # (the game's own first Timeout's type, text re-clocked to 00:00), so the half ends on a Timeout.
    # The punt changed possession and the next real play (Pitt's 2H kickoff return) is the other
    # team's: the punt's wp_after is 1 - that play's wp_before, not 1 - the Timeout row's.
    summary = copy.deepcopy(_load(PATHS[401234617]))
    plays = [p for d in summary["drives"]["previous"] for p in d["plays"]]
    timeout = next(p for p in plays if (p.get("type") or {}).get("text") == "Timeout")
    marker = next(p for p in plays if p["id"] == "401234617102997305")
    if (marker.get("type") or {}).get("text") != "End of Half":
        pytest.fail("401234617's first half no longer ends on an End of Half marker")
    marker["type"] = copy.deepcopy(timeout["type"])
    marker["text"] = "Timeout GEORGIA TECH, clock 00:00"
    proc = CFBPlayProcess(gameId=401234617, join_participants=False)
    proc.espn_cfb_pbp(summary=summary)
    proc.run_processing_pipeline()
    f = proc.plays_frame.with_row_index("row")
    i = f.filter(pl.col("id") == 401234617102997304)["row"].item()
    punt, edited, nxt = (f.row(i + k, named=True) for k in range(3))
    if edited["type.text"] != "Timeout" or nxt["type.text"] != "Kickoff":
        pytest.fail(f"unexpected rows after the punt: {edited['type.text']}, {nxt['type.text']}")
    if not (punt["start.pos_team.id"] != punt["end.pos_team.id"] == nxt["start.pos_team.id"]):
        pytest.fail("the punt no longer hands the ball to the 2H kickoff's team")
    assert punt["end_of_half"] is True and edited["end_of_half"] is False
    assert abs(punt["wp_after"] - (1 - nxt["wp_before"])) < 1e-9, (punt["wp_after"], nxt["wp_before"])


# --- supp-002: ESPN's degenerate end block --------------------------------------------------------

_DEGENERATE = tuple(gid for gid in CAPTURES if FEEDS[gid].degenerate_ends)


@pytest.mark.parametrize("game_id", _params(_DEGENERATE))
def test_degenerate_end_block_takes_the_next_snap_state(game_id):
    oracle = pl.DataFrame(
        FEEDS[game_id].degenerate_ends,
        schema={"id": pl.Int64, "next_down": pl.Int64, "next_ytez": pl.Int64},
        orient="row",
    )
    df = _frame(game_id).with_columns(pl.col("id").cast(pl.Int64))
    assert df.schema["id"] == oracle.schema["id"]
    joined = oracle.join(df.select("id", "end.down", "end.yardsToEndzone"), on="id", how="inner")
    if joined.height == 0:
        # 401752854: all 35 such plays are ESPN duplicate records (C-L11) the processor drops
        pytest.skip(f"none of the {oracle.height} degenerate-end plays is emitted: ESPN duplicate records")
    bad = joined.filter(
        pl.col("end.down").cast(pl.Int64).ne_missing(pl.col("next_down"))
        | pl.col("end.yardsToEndzone").cast(pl.Int64).ne_missing(pl.col("next_ytez")),
    )
    dropped = oracle.height - joined.height
    assert bad.height == 0, f"{bad.height} of {joined.height} emitted rows ({dropped} dropped): {bad.rows()}"


@pytest.mark.xdist_group("cfb-oracle-401856682")
def test_fumble_recovery_own_epa_from_the_next_snap_401856682():
    df = _frame(401856682).filter(pl.col("type.text").fill_null("") != "Timeout").with_row_index("row")
    hit = df.filter(pl.col("id").cast(pl.Int64) == 401856682824)
    assert hit.height == 1
    row = hit.row(0, named=True)
    nxt = df.row(row["row"] + 1, named=True)
    assert nxt["start.pos_team.id"] == row["start.pos_team.id"]
    assert None not in (row["EPA"], row["EP_start"], nxt["EP_start"]), (row["EPA"], row["EP_start"], nxt["EP_start"])
    oracle = nxt["EP_start"] - row["EP_start"]
    assert abs(row["EPA"] - oracle) <= SUPP_002_EPA_ABS_TOL, f"EPA {row['EPA']} vs next-snap {oracle}"


# --- C-NF-3: the line-yards band -------------------------------------------------------------------


def _line_yards_oracle(yards: pl.Expr) -> pl.Expr:
    """The standard per-yard band: losses 120%, yards 0-4 100%, yards 5-10 50%, beyond 10 nothing."""
    return (
        pl.when(yards < 0)
        .then(1.2 * yards)
        .when(yards <= 4)
        .then(yards)
        .when(yards <= 10)
        .then(4 + 0.5 * (yards - 4))
        .otherwise(7.0)
    )


def _off(column: str, expected: pl.Expr) -> pl.Expr:
    """Null-aware mismatch: a null on either side, or a value off by more than the tolerance."""
    diff = (pl.col(column) - expected).abs()
    return diff.is_null() | (diff > RUSH_YARDS_ABS_TOL)


def _rushes(game_id: int) -> pl.DataFrame:
    """Rush rows that carry their yardage; the rows that lose it are EX-1.2-a's test, not these."""
    return (
        _frame(game_id)
        .filter((pl.col("rush") == True) & pl.col("yds_rushed").is_not_null())
        .with_columns(
            pl.col("yds_rushed").cast(pl.Float64),
            pl.col("line_yards").cast(pl.Float64),
            pl.col("second_level_yards").cast(pl.Float64),
            pl.col("open_field_yards").cast(pl.Float64),
        )
        .with_columns(oracle=_line_yards_oracle(pl.col("yds_rushed")))
    )


# 27 captures left this table when main's #768 (6c5a48e44f, "kneel-down and yardless vendor rush yardage")
# reached this branch, 9 more with task 3.4(f) (the stated forms in test_stated_rush_yardage_is_read). The
# 7 rows left are rushes whose text states no carry: a fumble named with no yardage clause (only where the
# ball was RECOVERED, which the NCAA rule does not credit; statYardage disagrees with that spot on 2 of 4)
# and a vendor rush whose only number is the penalty. Null is the honest value there, as for a sack that
# states no loss; the xfails stay until a source states the carry.
#: rush rows per capture whose text states no carry; pinned so a new null on these captures still fails
_EX_1_2_A_COUNTS = {
    252532751: 1,
    282430151: 1,
    282570328: 1,
    313090025: 1,
    322802005: 1,
    401644749: 2,
}
_EX_1_2_A = {
    gid: f"EX-1.2-a: {n} rush row(s) state no carry ('X rush, fumbled, recovered by ... at the Y 28', "
    "'X rush (tackler), PENALTY ...'), so yds_rushed stays null (cfb_pbp.py yds_rushed chain)"
    for gid, n in _EX_1_2_A_COUNTS.items()
}

#: (game, play id, yards, oracle): one row per text form task 3.4(f) reads; each was null before it
_STATED_RUSH_FORMS = [
    (242972641, 2429726410102, 1, "'rushed up the middle for 1 yard'; statYardage 1"),
    (252532751, 252532751133, -8, "'rush for loss of 8 yards'; next snap 55 -> 63 yards to go (statYardage 0)"),
    (303102638, 303102638101, 7, "statYardage stand-in (fall-through): 'Joseph Banyard rush' is no 'N Yard Rush'"),
    (303102638, 303102638009, 3, "'rush over left tackle for 3 yards', SMU penalty after; statYardage 3"),
    (313090025, 313090025162, 11, "'rush over right guard for 11 yards, fumbled', own recovery; statYardage 11"),
    (400756970, 400756970104999301, 1, "'1 Yd Run (X Run for Two-Point Conversion)'; statYardage 1"),
    # rush_forms/ fixture: a directional loss on a flagged rush (penalty declined)
    (
        282782655,
        282782655087,
        -5,
        "'rush over right end for a loss of 5 yards', penalty declined; statYardage -5, next snap 52 -> 57 yards to go",
    ),
]


@pytest.mark.parametrize(
    ("game_id", "play_id", "yards", "oracle"),
    [
        pytest.param(*row, marks=pytest.mark.xdist_group(f"cfb-oracle-{row[0]}"), id=f"{row[0]}-{row[1]}")
        for row in _STATED_RUSH_FORMS
    ],
)
def test_stated_rush_yardage_is_read(game_id, play_id, yards, oracle):
    """EX-1.2-a: each text form that states a carry yields it (the feed's statYardage or next snap)."""
    row = _frame(game_id).filter(pl.col("id").cast(pl.Int64) == play_id)
    assert row.height == 1
    assert row["yds_rushed"].item() == yards, oracle


@pytest.mark.parametrize("game_id", _params(CAPTURES, _EX_1_2_A))
def test_every_rush_carries_its_yardage(game_id):
    """The feed states every rush's yardage (a number, "no gain" or "loss of N"); none may be null."""
    rushes = _frame(game_id).filter(pl.col("rush") == True)
    lost = rushes.filter(pl.col("yds_rushed").is_null())
    assert lost.height == 0, f"{lost.height} of {rushes.height} rushes: {lost['text'].head(3).to_list()}"


@pytest.mark.parametrize("game_id", _params(sorted(_EX_1_2_A_COUNTS)))
def test_rushes_without_a_stated_carry_stay_counted(game_id):
    """EX-1.2-a: the xfailed captures keep exactly their counted no-carry rows (growth fails here)."""
    lost = _frame(game_id).filter((pl.col("rush") == True) & pl.col("yds_rushed").is_null())
    assert lost.height == _EX_1_2_A_COUNTS[game_id], lost["text"].to_list()


#: a feed whose three yardages disagree: no reader can match statYardage without dropping the text
_PLAIN_RUSH_PROVIDER = {
    401012682: "provider defect: play 401012682104884901 reads 'run for 4 yds to the OrgSt 29' (yds_rushed 4) "
    "with statYardage 8 and a 41 -> 29 spot (12 yards); two runners' texts merged in one record",
}


@pytest.mark.parametrize("game_id", _params(CAPTURES, _PLAIN_RUSH_PROVIDER))
def test_plain_rush_yardage_equals_stat_yardage(game_id):
    """A rush with no fumble and no flag gains what ESPN's statYardage says (where it is not the 0 blank)."""
    plain = _frame(game_id).filter(
        (pl.col("rush") == True)
        & (pl.col("fumble_vec").fill_null(False) == False)
        & (pl.col("penalty_flag").fill_null(False) == False)
        & pl.col("penalty_detail").is_null()
        & (pl.col("statYardage").cast(pl.Float64) != 0)
    )
    bad = plain.filter(~pl.col("yds_rushed").cast(pl.Float64).eq_missing(pl.col("statYardage").cast(pl.Float64)))
    assert bad.height == 0, bad.select("id", "statYardage", "yds_rushed", "text").rows()[:5]


@pytest.mark.parametrize("game_id", _params(CAPTURES))
def test_line_yards_band_on_losses_and_gains_through_four(game_id):
    short = _rushes(game_id).filter(pl.col("yds_rushed") <= 4)
    bad = short.filter(_off("line_yards", pl.col("oracle")))
    assert bad.height == 0, bad.select("yds_rushed", "line_yards", "oracle").rows()


@pytest.mark.parametrize("game_id", _params(CAPTURES))
def test_line_yards_band_on_gains_of_four_or_more(game_id):
    long = _rushes(game_id).filter(pl.col("yds_rushed") >= 4)
    assert long.height > 0
    bad = long.filter(_off("line_yards", pl.col("oracle")))
    assert bad.height == 0, f"{bad.height} of {long.height} carries of 4+ yards"


@pytest.mark.parametrize("game_id", _params(CAPTURES))
def test_rush_decomposition_sums_to_the_gain(game_id):
    gains = _rushes(game_id).filter(pl.col("yds_rushed") >= 0)
    total = pl.col("line_yards") + pl.col("second_level_yards") + pl.col("open_field_yards")
    bad = gains.filter(_off("yds_rushed", total))
    assert bad.height == 0, bad.select("yds_rushed", "line_yards", "second_level_yards", "open_field_yards").rows()


@pytest.mark.parametrize("game_id", _params(CAPTURES))
def test_rush_adj_and_highlight_yards_follow_the_band(game_id):
    """adj_rush_yardage is the carry capped at 10; highlight yards are the carry less the line's credit."""
    r = _rushes(game_id).with_columns(
        pl.col("adj_rush_yardage").cast(pl.Float64),
        pl.col("highlight_yards").cast(pl.Float64),
        pl.col("opp_highlight_yards").cast(pl.Float64),
    )
    y = pl.col("yds_rushed")
    bad = r.filter(_off("adj_rush_yardage", pl.min_horizontal(y, pl.lit(10.0))))
    assert bad.height == 0, bad.select("yds_rushed", "adj_rush_yardage").rows()
    gains = r.filter(y >= 0)
    bad = gains.filter(_off("highlight_yards", y - pl.col("line_yards")))
    assert bad.height == 0, bad.select("yds_rushed", "line_yards", "highlight_yards").rows()
    bad = r.filter((y < 0) & _off("highlight_yards", pl.lit(0.0)))  # a loss is all the line's
    assert bad.height == 0, bad.select("yds_rushed", "highlight_yards").rows()
    # opportunity_run is a carry of 4+ (cfbfastR-cfb-data espn_cfb_15 :610): highlight yards there, 0 below
    bad = r.filter(_off("opp_highlight_yards", pl.when(y >= 4).then(pl.col("highlight_yards")).otherwise(0.0)))
    assert bad.height == 0, bad.select("yds_rushed", "highlight_yards", "opp_highlight_yards").rows()


# --- C-M18 / C-L1: row order ------------------------------------------------------------------------

# 401425418's third-quarter kickoff under an out-of-scheme id is placed by its clock since 3.4(b2).
# 401628439's is a provider label defect, not a sort one: ESPN files an overtime touchdown (id period
# digit 6) as period 2; PlayOrderWarning names it (test_period_monotonic_or_the_step_back_warns).
_M18 = {
    401628439: "C-M18: an overtime touchdown ESPN labels period 2 sorts to the end of regulation",
}


@pytest.mark.parametrize("game_id", _params(CAPTURES, _M18))
def test_period_never_decreases_along_the_rows(game_id):
    """C-M18: kept as the deliberate strict-xfail marker for ESPN's 401628439 period-label error.

    test_period_monotonic_or_the_step_back_warns implies it (its exact-pair assert); this one keeps the
    provider defect visible as an open xfail.
    """
    steps_back = _period_steps_back(_frame(game_id))
    assert steps_back == [], f"period decreases at rows {steps_back}"


def _period_steps_back(df: pl.DataFrame) -> list[int]:
    period = df["period"].cast(pl.Int64)
    assert period.null_count() == 0
    return [i for i, d in enumerate(period.diff().to_list()) if d is not None and d < 0]


#: the (before, after) play ids PlayOrderWarning names where a capture's period label steps back
_M18_WARNED = {401628439: (401628439104999901, 401628439106260230)}


@pytest.mark.parametrize("game_id", _params(CAPTURES))
def test_period_monotonic_or_the_step_back_warns(game_id):
    """C-M18: the period never decreases along the rows, or the processor warns naming the step."""
    if game_id not in _M18_WARNED:
        assert _period_steps_back(_frame(game_id)) == []
        return
    before, after = _M18_WARNED[game_id]
    # a fresh run: the cached one warned once, under the module's ignore filter
    with pytest.warns(PlayOrderWarning, match=rf"{game_id}.*\({before}, {after}\)"):
        frame = _run.__wrapped__(game_id).frame
    ids = frame["id"].cast(pl.Int64).to_list()
    assert [(ids[i - 1], ids[i]) for i in _period_steps_back(frame)] == [(before, after)]


@pytest.mark.parametrize("game_id", _params(CAPTURES))
def test_game_play_number_and_lag_half_after_sort_follow_the_rows(game_id):
    df = _frame(game_id)
    gpn = df["game_play_number"].cast(pl.Int64)
    assert gpn.null_count() == 0
    back = [i for i, d in enumerate(gpn.diff().to_list()) if d is not None and d <= 0]
    assert back == [], f"game_play_number does not increase at rows {back}"
    previous = df["half"].cast(pl.Int64).shift(1)
    lag = df["lag_half"].cast(pl.Int64)
    stale = [i for i, (a, b) in enumerate(zip(lag.to_list(), previous.to_list())) if a != b]
    assert stale == [], f"lag_half is not the previous row's half at rows {stale}"


# --- C-L11 / C-L16: the rows are the feed's plays ---------------------------------------------------


@pytest.mark.parametrize("game_id", _params(CAPTURES))
def test_every_row_is_a_feed_play_once(game_id):
    ids = _frame(game_id)["id"].cast(pl.Int64)
    assert ids.null_count() == 0
    assert ids.n_unique() == ids.len(), "a play id is emitted twice"
    invented = set(ids.to_list()) - FEEDS[game_id].play_ids
    assert invented == set(), f"rows that are not feed plays: {sorted(invented)}"


_PRE_2005 = tuple(gid for gid in CAPTURES if FEEDS[gid].season < 2005)
#: the feed rows that are not plays: period/game markers and the 2004 feed's drive banners
_ADMIN_TEXT = re.compile(r"(?i)^(?:end of|start of|begin drive)")


@pytest.mark.parametrize("game_id", _params(_PRE_2005))
def test_pre_2005_rows_dropped_from_the_feed_are_admin_rows(game_id):
    """C-L11: before 2005 ``sequenceNumber`` restarts per drive (nearly every value repeats), so it is
    no key; the copy drops key on ``id``. Every feed row the output lacks is a marker, never a play."""
    summary = _load(PATHS[game_id])
    plays = [p for d in summary["drives"]["previous"] for p in d["plays"]]
    sequences = [p.get("sequenceNumber") for p in plays]
    assert len(set(sequences)) < len(sequences) / 2, "the capture no longer repeats sequenceNumber"
    kept = set(_frame(game_id)["id"].cast(pl.Int64).to_list())
    dropped = [p.get("text") or "" for p in plays if int(p["id"]) not in kept]
    assert dropped and all(_ADMIN_TEXT.search(text) for text in dropped), dropped


def test_flag_literals_meet_no_other_casing():
    """C-L16 (latent): R matches these with ignore_case; the captures never make the two disagree."""
    differ = [
        (gid, literal, text)
        for gid in CAPTURES
        for text in FEEDS[gid].texts
        for literal in _FLAG_LITERALS
        if (literal in text) != (literal in text.lower())
    ]
    assert sum(len(FEEDS[g].texts) for g in CAPTURES) > 20000
    assert differ == []


# --- C-M8 (CFB half), C-L5, C-L10, C-M7: dtypes ----------------------------------------------------

_SCHEMA_REFERENCE = 401628455
_NFL_REFERENCE = 401872922


@functools.lru_cache(maxsize=None)
def _schema_allowlist() -> dict[str, list[dict]]:
    rows = yaml.safe_load((Path(__file__).parent / "schema_allowlist.yaml").read_text(encoding="utf-8"))
    out: dict[str, list[dict]] = {"across_captures": [], "vs_nfl": []}
    for row in rows:
        out[row["scope"]].append(row)
    return out


def test_schema_allowlist_rows_name_a_finding_and_only_shrink():
    rows = yaml.safe_load((Path(__file__).parent / "schema_allowlist.yaml").read_text(encoding="utf-8"))
    assert all(r.get("finding") and r.get("reason") and r.get("column") for r in rows)
    assert {r["scope"] for r in rows} <= {"across_captures", "vs_nfl"}
    across = [r for r in rows if r["scope"] == "across_captures"]
    assert all(r.get("games") and set(r["games"]) <= set(CAPTURES) for r in across)
    assert len({r["column"] for r in across}) == len(across), "one across_captures row per column"
    assert len(rows) <= SCHEMA_ALLOWLIST_MAX_ROWS


@pytest.mark.parametrize("game_id", _params(CAPTURES))
def test_schema_is_identical_across_captures(game_id):
    """Exact both ways: the columns that differ from the reference are exactly the rows naming this capture."""
    allowed = {r["column"] for r in _schema_allowlist()["across_captures"] if game_id in r["games"]}
    ref, got = _run(_SCHEMA_REFERENCE).schema, _run(game_id).schema
    differ = {c for c in set(ref) | set(got) if ref.get(c) != got.get(c)}
    assert differ == allowed, {
        "new": sorted((c, ref.get(c), got.get(c)) for c in differ - allowed),
        "fixed, drop the capture from the row": sorted(allowed - differ),
    }


@pytest.mark.xdist_group(f"cfb-oracle-{_SCHEMA_REFERENCE}")
def test_shared_column_dtypes_match_nfl():
    from sportsdataverse.nfl import NFLPlayProcess

    nfl_summary = json.loads(
        (Path(__file__).parents[1] / "nfl" / "fixtures" / f"summary_{_NFL_REFERENCE}.json").read_text(encoding="utf-8"),
    )
    proc = NFLPlayProcess(gameId=_NFL_REFERENCE)
    proc.espn_nfl_pbp(summary=nfl_summary)
    proc.run_processing_pipeline()
    nfl = {c: str(t) for c, t in proc.plays_frame.schema.items()}
    cfb = _run(_SCHEMA_REFERENCE).schema
    differ = {c for c in cfb if c in nfl and cfb[c] != nfl[c]}
    allowed = {r["column"] for r in _schema_allowlist()["vs_nfl"]}
    assert differ == allowed, {"new": sorted(differ - allowed), "fixed, drop the row": sorted(allowed - differ)}


def _canon_box(v):
    """advBoxScore with every list sorted (its group_by row order is not deterministic) and NaN named."""
    if isinstance(v, list):
        return sorted((_canon_box(x) for x in v), key=lambda x: json.dumps(x, sort_keys=True, default=str))
    if isinstance(v, dict):
        return {k: _canon_box(x) for k, x in v.items()}
    return "nan" if isinstance(v, float) and v != v else v


@pytest.mark.parametrize("game_id", _params((CAPTURES[0], _SCHEMA_REFERENCE, CAPTURES[-1])))
def test_box_score_rerun_on_plays_frame_reproduces_adv_box_score(game_id):
    """R797-2 (task 2.17): Game on Paper's ?span= boxes re-run create_box_score on plays_frame, which
    publishes the EP/WP floats Float64; the box must still aggregate and round as advBoxScore did."""
    proc = CFBPlayProcess(gameId=game_id, join_participants=False)
    proc.espn_cfb_pbp(summary=_load(PATHS[game_id]))
    adv = proc.run_processing_pipeline()["advBoxScore"]
    assert _canon_box(proc.create_box_score(proc.plays_frame)) == _canon_box(adv)


# --- C-M19: the pickcenter odds round trip ----------------------------------------------------------

# the five keys the payload carries; a four-key override rebuilds with a null overUnderAvailable (unknown, R895-1)
_ODDS_KEYS = ("gameSpread", "overUnder", "homeFavorite", "gameSpreadAvailable", "overUnderAvailable")


@pytest.mark.xdist_group("cfb-oracle-401301042")
def test_pickcenter_odds_round_trip_rebuilds_the_frame_401301042():
    first = CFBPlayProcess(gameId=401301042, join_participants=False)
    first.espn_cfb_pbp(summary=_load(PATHS[401301042]))
    payload = first.run_processing_pipeline()
    assert payload["odds_source"] == "summary_pickcenter"
    persisted = json.loads(json.dumps({k: payload[k] for k in _ODDS_KEYS}))
    # the oracle: ESPN's pickcenter in the capture (both providers carry -4.0 / 58.5 / home favored)
    espn = _load(PATHS[401301042])["pickcenter"]
    assert {(p["spread"], p["overUnder"], p["homeTeamOdds"]["favorite"]) for p in espn} == {(-4.0, 58.5, True)}
    assert persisted == {
        "gameSpread": -4.0,
        "overUnder": 58.5,
        "homeFavorite": True,
        "gameSpreadAvailable": True,
        "overUnderAvailable": True,
    }
    again = CFBPlayProcess(gameId=401301042, join_participants=False, odds_override=persisted)
    again.espn_cfb_pbp(summary=_load(PATHS[401301042]))
    again.run_processing_pipeline()
    assert again.plays_frame.equals(first.plays_frame)
