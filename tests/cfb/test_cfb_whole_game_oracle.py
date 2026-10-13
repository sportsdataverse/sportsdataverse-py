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
    XFAIL C-NF-2 / C-M13 on every one of the 85 (row 1 names the receiving team; later rows the away
    team).
``start/end.pos_team_receives_2H_kickoff`` (first-half rows)
    cfbfastR ``cfbd_pbp_data.R:1329-1332``: true exactly when the possession team is the opening
    kicker. Second-half rows are not asserted: cfbfastR leaves the flag ungated there while nflfastR
    gates it to the first half, and the phase-2 port (plan task 2.6) makes that choice. XFAIL C-NF-2.
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
    2005, has no end state to trust and is skipped), on the plays the processor emits. XFAIL supp-002.
``EPA`` of 401856682's Q4 Fumble Recovery (Own)
    EP at the next snap's start minus ``EP_start``, within 0.1 (the addendum's tolerance; the Yahoo and
    CBS adapters score -0.39, the next-snap state gives -0.41). The state comes from the feed; the EP
    values are the processor's own model evaluated at that state, so this pins the end STATE, not the
    model. XFAIL supp-002.
``line_yards``
    The standard per-yard adjusted-line-yards band (the ruling for plan task 2.1 in
    ``audit/EXECUTION-FINDINGS.md``; Action Network's ALY definition, Football Study Hall): a loss
    counts 120%, yards 0-4 100%, yards 5-10 50%, yards beyond 10 nothing, so a gain is capped at 7. Not
    the summary producers' formula; ``adj_rush_yardage`` is the carry capped at 10 and ``highlight_yards``
    the carry less ``line_yards`` on a gain. Green on every carry since task 2.1 (C-NF-3: the processor credited
    100% through 3, 50% of 4-8, cap 5.5).
``yds_rushed``
    The feed's text states every rush's yardage (a number, "no gain", "loss of N"); a rush row without
    it is a parse gap. XFAIL EX-1.2-a (a finding surfaced by this suite's null-aware review) on the 15
    captures where it still happens (#768 closed 27 of the original 42); the line-yards checks below read
    only the rows that carry a yardage.
``line_yards + second_level_yards + open_field_yards`` on gains
    The decomposition identity: the three pieces sum to ``yds_rushed``. Green (C-H1's CFB twin).
``period``
    The feed's chronology: never decreases along the emitted rows. XFAIL C-M18.
``game_play_number``, ``lag_half``
    Recomputed after the final sort: ``game_play_number`` strictly increases along the rows and
    ``lag_half`` is the previous row's ``half`` (null on row 1). XFAIL C-L1.
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
    and, fed back as ``odds_override``, rebuild the same frame. XFAIL C-M19.

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
SCHEMA_ALLOWLIST_MAX_ROWS = 49

pytestmark = pytest.mark.whole_game


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


def _feed(game_id: int, summary: dict) -> Feed:
    plays = [p for d in (summary.get("drives") or {}).get("previous", []) for p in d.get("plays", [])]
    period1 = sorted(
        (p for p in plays if (p.get("period") or {}).get("number") == 1),
        key=lambda p: int(p.get("sequenceNumber") or 0),
    )
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


@functools.lru_cache(maxsize=None)
def _run(game_id: int) -> Run:
    """Process one capture offline, once per process; keep the columns the oracles read."""
    proc = CFBPlayProcess(gameId=game_id, join_participants=False)
    proc.espn_cfb_pbp(summary=_load(PATHS[game_id]))
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
_NF2 = (
    "C-NF-2 / C-M13: firstHalfKickoffTeamId is a per-row expression (cfb_pbp.py:2865-2873): row 1 names the "
    "receiving team and every later row the away team, so it is the opening kicker on no row-1 and on "
    "no row at all when the home team kicked"
)
_NF2_FLAG = (
    "C-NF-2: pos_team_receives_2H_kickoff reads the per-row firstHalfKickoffTeamId (cfb_pbp.py:4323-4330): "
    "row 1 always, and every first-half row when the home team kicked, carry the inverted flag"
)


@pytest.mark.parametrize("game_id", _params(_KICKOFF_DEFINED, dict.fromkeys(_KICKOFF_DEFINED, _NF2)))
def test_first_half_kickoff_team_is_the_opening_kicker_on_every_row(game_id):
    kicker = FEEDS[game_id].opening_kicker
    df = _frame(game_id)
    named = df["firstHalfKickoffTeamId"].cast(pl.Int64)
    assert named.null_count() == 0, f"{named.null_count()} of {df.height} rows name no team"
    assert named.n_unique() == 1, f"not a game-wide constant: {sorted(named.unique().to_list())}"
    wrong = int(named.ne_missing(kicker).sum())
    assert wrong == 0, f"{wrong} of {df.height} rows name a team other than the opening kicker {kicker}"


@pytest.mark.parametrize("game_id", _params(_KICKOFF_DEFINED, dict.fromkeys(_KICKOFF_DEFINED, _NF2_FLAG)))
def test_receives_2h_kickoff_on_first_half_rows_is_the_opening_kicker(game_id):
    kicker = FEEDS[game_id].opening_kicker
    first_half = _frame(game_id).filter(pl.col("period") <= 2)
    for side in ("start", "end"):
        # a row with no possession team does not belong to the kicker (the oracle's side only)
        expected = (first_half[f"{side}.pos_team.id"].cast(pl.Int64) == kicker).fill_null(False)
        got = first_half[f"{side}.pos_team_receives_2H_kickoff"]
        wrong = int(expected.ne_missing(got).sum())
        assert wrong == 0, f"{side}: {wrong} of {first_half.height} first-half rows carry the inverted flag"


def test_the_kickoff_oracle_covers_the_captures_it_should():
    """Guard the guard: the feed names the opening kicker on 85 of the 126 captures, 43 of them home."""
    kicked = [FEEDS[g] for g in _KICKOFF_DEFINED]
    assert len(kicked) == 85
    assert sum(1 for f in kicked if f.opening_kicker == f.home_id) == 43
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
def test_end_of_half_lands_on_the_last_real_play(game_id):
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
_SUPP_002 = {
    gid: f"supp-002: ESPN's {{down 0, yardLine 0, yardsToEndzone 0}} end block is kept ({note}) instead of the next snap's start"
    for gid, note in {
        322590228: "end.down 0",
        400869270: "end.down 0 on a declined penalty",
        401752744: "end.down 0 on a declined penalty",
        401754585: "end.down 0, end.yardsToEndzone 99",
        401754598: "end.down 0, end.yardsToEndzone 99",
        401761622: "end.down 0, end.yardsToEndzone 79 where the next snap starts at 64",
        401856682: "end.down 0, end.yardsToEndzone 99 on the Q4 Fumble Recovery (Own)",
        401858435: "end.down 0, end.yardsToEndzone 99",
    }.items()
}


@pytest.mark.parametrize("game_id", _params(_DEGENERATE, _SUPP_002))
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
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="supp-002: 401856682's Q4 'Fumble Recovery (Own)' scores EPA -5.91 from end.yardsToEndzone 99; "
    "the next snap (3rd & 1 at the 1) gives -0.41",
)
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
# reached this branch: they now assert plainly. With #768's cfb_pbp.py hunk reverted every count below and
# every removed capture returns to its pre-#768 value (303102638 48, 313090025 47, 401644749 7).
_EX_1_2_A = {
    gid: f"EX-1.2-a: {n} rush row(s) lose yds_rushed (and so line_yards) because the text pattern is unmatched "
    "('rushed up the middle for 1 yard', fumbled rushes with no yardage clause, a '1 Yd Run' with a "
    "two-point try; cfb_pbp.py yds_rushed chain)"
    for gid, n in {
        242972641: 14,
        243040130: 9,
        243040265: 5,
        243042579: 5,
        243110264: 6,
        243392572: 5,
        252532751: 6,
        282430151: 1,
        282570328: 1,
        303102638: 3,
        313090025: 2,
        322802005: 1,
        400756970: 1,
        400953391: 1,
        401644749: 2,
    }.items()
}


@pytest.mark.parametrize("game_id", _params(CAPTURES, _EX_1_2_A))
def test_every_rush_carries_its_yardage(game_id):
    """The feed states every rush's yardage (a number, "no gain" or "loss of N"); none may be null."""
    rushes = _frame(game_id).filter(pl.col("rush") == True)
    lost = rushes.filter(pl.col("yds_rushed").is_null())
    assert lost.height == 0, f"{lost.height} of {rushes.height} rushes: {lost['text'].head(3).to_list()}"


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

_M18 = {
    401425418: "C-M18: a third-quarter kickoff sorts first (game_play_number 1), so period steps back from 3 to 1",
    401628439: "C-M18: an overtime touchdown ESPN labels period 2 sorts after period 5 rows",
}


@pytest.mark.parametrize("game_id", _params(CAPTURES, _M18))
def test_period_never_decreases_along_the_rows(game_id):
    period = _frame(game_id)["period"].cast(pl.Int64)
    assert period.null_count() == 0
    steps_back = [i for i, d in enumerate(period.diff().to_list()) if d is not None and d < 0]
    assert steps_back == [], f"period decreases at rows {steps_back}"


_L1 = {
    gid: f"C-L1: game_play_number / lag_half are set before the final sort (cfb_pbp.py:2536, :2619); {note}"
    for gid, note in {
        282430151: "lag_half stale on rows 0, 2",
        282570328: "lag_half stale on rows 0, 2",
        293250145: "lag_half stale on rows 0, 1",
        302540025: "lag_half stale on rows 0, 1",
        332990030: "lag_half stale on rows 0, 1",
        401282146: "lag_half stale on rows 0, 201",
        401301042: "lag_half stale on rows 0, 183",
        401425418: "lag_half stale on rows 0, 1, 10",
        401525860: "lag_half stale on rows 0, 98",
        401525903: "game_play_number steps back at row 117",
        401628439: "lag_half stale on row 173",
        401752817: "lag_half stale on row 81",
        401752854: "lag_half stale on row 84",
        401754585: "lag_half stale on rows 40, 51, 191",
        401858224: "lag_half stale on rows 35, 83, 174",
    }.items()
}


@pytest.mark.parametrize("game_id", _params(CAPTURES, _L1))
def test_game_play_number_and_lag_half_follow_the_rows(game_id):
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

_ODDS_KEYS = ("gameSpread", "overUnder", "homeFavorite", "gameSpreadAvailable")


@pytest.mark.xdist_group("cfb-oracle-401301042")
@pytest.mark.xfail(
    strict=True,
    raises=(KeyError, TypeError),
    reason="C-M19: on the summary_pickcenter path gameSpread/overUnder are shape-(1,) ndarrays and "
    "homeFavorite is missing from the payload (cfb_pbp.py:3655-3667), so the persisted odds cannot round trip",
)
def test_pickcenter_odds_round_trip_rebuilds_the_frame_401301042():
    first = CFBPlayProcess(gameId=401301042, join_participants=False)
    first.espn_cfb_pbp(summary=_load(PATHS[401301042]))
    payload = first.run_processing_pipeline()
    assert payload["odds_source"] == "summary_pickcenter"
    persisted = json.loads(json.dumps({k: payload[k] for k in _ODDS_KEYS}))
    again = CFBPlayProcess(gameId=401301042, join_participants=False, odds_override=persisted)
    again.espn_cfb_pbp(summary=_load(PATHS[401301042]))
    again.run_processing_pipeline()
    assert again.plays_frame.equals(first.plays_frame)
