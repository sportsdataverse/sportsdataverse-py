"""Oracle-anchored whole-game characterization of ``NFLPlayProcess`` (quality program task 1.1).

Every committed ESPN capture (``tests/nfl/fixtures/README.md``: 37 games, 2002-2026) is run offline
through the full pipeline and each column that has an oracle OUTSIDE this processor is asserted
against it:

* nflfastR's R source (``nflverse-dev/nflfastR/R/helper_add_ep_wp.R:710-724``: a non-scoring play
  that ends a half, regulation or the game has ``epa == -ep``);
* the team-summary producers' ``opportunity_run``
  (``cfbfastR-cfb-data/R/espn_cfb_15_team_summaries_creation.R:610``, ``nfl-data`` ``build.py:244``);
* the standard adjusted-line-yards band (losses 120%, yards 0-4 at 100%, 5-10 at 50%, beyond 10 at
  0%: the program's ruling for task 2.1; the producers' own ``line_yards`` reading is not it);
* the CFB processor's fixed copy of the same stage (``cfb_pbp.py``: the kickoff-return ``end.down``
  rewrite);
* ESPN's own feed (``scoringPlay``, the play text, the summary's ``boxscore``);
* the committed nflverse slices of five of the games (``fixtures/nflverse/``), joined play by play:
  ESPN's play id is the game id followed by the GSIS ``play_id``.

Snapshot ban: no expected value is a stored copy of this processor's output. A check that is red
today carries ``xfail(strict=True, raises=AssertionError, reason="<finding id>: ...")`` on exactly
the games where it is red, so the fix turns it XPASS (a failure) and must drop the marker in the
same PR, and any other exception (a renamed column, a crash) still fails; every other
game asserts the oracle plainly. Tolerances on model columns are the agreement measured on HEAD
36f7a02f1b (2026-10-11), rounded one step toward failing; raise them as fixes land, never lower.
"""

from __future__ import annotations

import copy
import functools
import gzip
import json
import math

import polars as pl
import pytest

from sportsdataverse.nfl.ep_wp import _derive_wpa, calculate_completion_probability, calculate_epa, calculate_wpa
from sportsdataverse.nfl.model_vars import (
    clock_stoppage_vec,
    kickoff_vec,
    normalplay,
    offense_score_vec,
    touchback_yardline_expr,
)
import sportsdataverse.nfl.nfl_fourth_down as nfl_fourth_down
from sportsdataverse.nfl import NFLPlayProcess
from sportsdataverse.nfl.nfl_pbp import _touchdown_did_not_stand
from tests.nfl._whole_game import CAPTURES, FIX, load_summary, run

pytestmark = [pytest.mark.whole_game, pytest.mark.xdist_group("nfl_whole_game")]


def _t(name: str) -> pl.Expr:
    return (pl.col(name) == True).fill_null(False)  # noqa: E712


def _xfail(reason: str) -> pytest.MarkDecorator:
    """Red today on the oracle; only a failed assertion counts as the expected failure."""
    return pytest.mark.xfail(strict=True, raises=AssertionError, reason=reason)


def _cases(known: dict[int, str] | None = None, games=CAPTURES) -> list:
    """One param per game; the games in ``known`` (game -> "<finding>: <why>") are strict xfails."""
    known = known or {}
    return [pytest.param(g, marks=_xfail(known[g])) if g in known else g for g in games]


# ESPN admin row of the 2002-05 feed, typed "Unknown" (C-M2): not a play.
_ADMIN_END = pl.col("text").str.contains(r"(?i)^\s*end of (the )?game").fill_null(False)
_PLAY = ~pl.col("type.text").is_in(clock_stoppage_vec).fill_null(False) & ~_ADMIN_END
_RUSH = _t("rush")
_YDS = pl.col("yds_rushed")


def _require(ok: bool, why: str) -> None:
    """A broken precondition fails outright, never as an xfail's expected AssertionError."""
    if not ok:
        pytest.fail(why)


def _bad(frame: pl.DataFrame, ok: pl.Expr, cols: list[str]) -> list[tuple]:
    return frame.filter(~ok.fill_null(False)).select("id", *cols).rows()


# ---------------------------------------------------------------------------------------------
# C-C1 (critical, FIXED on HEAD by #739): the regression gate
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("game_id", _cases())
def test_opportunity_run_is_a_rush_of_four_or_more(game_id):
    # Oracle: cfbfastR-cfb-data espn_cfb_15_team_summaries_creation.R:610
    # `opportunity_run = (rush == 1) & (yds_rushed >= 4)` (nfl-data build.py:244 the same).
    f = run(game_id).plays
    oracle = (_RUSH & (_YDS >= 4)).fill_null(False)
    assert _bad(f, _t("opportunity_run") == oracle, ["opportunity_run", "rush", "yds_rushed"]) == []
    assert f.filter(pl.col("opp_highlight_yards") > 0).height > 0  # it read 0 on every row (C-C1)


# ---------------------------------------------------------------------------------------------
# C-H1 / C-NF-3: the rushing decomposition
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("game_id", _cases())
def test_rushing_decomposition_sums_to_the_carry(game_id):
    # Oracle: the yards themselves -- line + second level + open field is the carry on every rush of
    # 4+ yards (task 2.1 flipped all 31 C-H1 xfails: the old band summed to the carry minus 0.5).
    f = run(game_id).plays.filter(_RUSH & (_YDS >= 4))
    parts = pl.col("line_yards") + pl.col("second_level_yards") + pl.col("open_field_yards")
    _require(f.height > 0, "no 4+ yard rush in the capture")
    assert _bad(f, (parts - _YDS).abs() < 1e-9, ["yds_rushed", "line_yards", "second_level_yards"]) == []


@pytest.mark.parametrize("game_id", _cases())
def test_every_rush_has_all_three_parts(game_id):
    # #739 moved the gates to `rush`: a "Rushing Touchdown" or fumbled carry has a full decomposition.
    f = run(game_id).plays.filter(_RUSH)
    full = pl.all_horizontal(pl.col(c).is_not_null() for c in ("line_yards", "second_level_yards", "open_field_yards"))
    assert _bad(f, full, ["type.text", "yds_rushed"]) == []


@pytest.mark.parametrize("game_id", _cases())
def test_line_yards_band_is_the_standard(game_id):
    # Task 2.1 flipped all 31 C-NF-3 xfails (the processor read 0-3 / 4-8 / cap 5.5).
    # Oracle: standard adjusted line yards (Football Outsiders ALY, Connelly's college version, the
    # plan's "cap 7"), per yard: losses 120%, yards 0-4 at 100%, yards 5-10 at 50%, beyond 10 at 0%.
    # The program's ruling for task 2.1 (ClaudeCowork plan audit/EXECUTION-FINDINGS.md). The three
    # summary producers' reading (0.5 * the whole carry on 5-10, 0 from 11) is not the standard: it
    # credits a 5-yard run less than a 4-yard run.
    band = (
        pl.when(_YDS < 0).then(1.2 * _YDS).when(_YDS <= 4).then(_YDS).when(_YDS <= 10).then(4 + 0.5 * (_YDS - 4))
    ).otherwise(7.0)
    f = run(game_id).plays.filter(_RUSH & _YDS.is_not_null())
    assert _bad(f, (pl.col("line_yards") - band).abs() < 1e-9, ["yds_rushed", "line_yards"]) == []


# ---------------------------------------------------------------------------------------------
# C-H2 (and the C-NF-12 timeout twin): the play that ends a half, regulation or the game
# ---------------------------------------------------------------------------------------------

_OT_GAMES = (260917021, 271209010, 401547449, 401671835, 401772636)  # a period 5 in the capture
# Task 2.2 (C-H2, C-NF-12) flipped every (game, boundary) here: the game's last play and the
# regulation-ending play were null / False, and 400791590's half ended on a Timeout row.
_BOUNDARIES = [(g, "half") for g in CAPTURES] + [(g, "regulation") for g in _OT_GAMES] + [(g, "game") for g in CAPTURES]


def _half_ending_play(game_id: int, boundary: str) -> dict:
    # Oracle: nflfastR helper_add_ep_wp.R:710-724 -- the last play of Q2, of Q4 when the game goes to
    # overtime, and of the game ends a half; when it did not score, epa = 0 - ep.
    plays = run(game_id).plays.filter(_PLAY)
    if boundary == "half":
        plays = plays.filter(pl.col("period.number") == 2)
    elif boundary == "regulation":
        plays = plays.filter(pl.col("period.number") == 4)
    return plays.tail(1).row(0, named=True)


@pytest.mark.parametrize(("game_id", "boundary"), _BOUNDARIES)
def test_end_of_half_on_the_half_ending_play(game_id, boundary):
    last = _half_ending_play(game_id, boundary)
    assert last["end_of_half"] is True, (last["id"], last["type.text"], last["end_of_half"])


@pytest.mark.parametrize(("game_id", "boundary"), _BOUNDARIES)
def test_final_play_epa_is_minus_ep_start(game_id, boundary):
    last = _half_ending_play(game_id, boundary)
    if last["scoringPlay"] is not True:
        assert last["EPA"] is not None and abs(last["EPA"] + last["EP_start"]) < 1e-9, (last["id"], last["EPA"])


@pytest.mark.parametrize("game_id", CAPTURES)
def test_end_of_half_rows_two_per_game_three_with_overtime(game_id):
    # R832-5: end_of_half fires on the first half's last play, regulation's last play when the game
    # goes to overtime, and the game's last play, never on a clock-stoppage row (C-NF-12: 400791590's
    # first half ends on a Timeout row, which must not take it from the play before it). 261022011
    # flipped with task 2.19 (R832-10): ESPN files its Q2 kickoff at 0:08 as period 3 between two
    # period-2 rows, which _repair_espn_period puts back in period 2.
    f = run(game_id).plays
    _require(f["status_type_completed"].all(), "the capture is not a completed game")
    stops = f.filter(pl.col("type.text").is_in(clock_stoppage_vec) & _t("end_of_half"))
    assert stops.select("id", "type.text").rows() == []
    expected = 3 if f["period.number"].max() >= 5 else 2
    assert f.filter(_t("end_of_half")).height == expected, f.filter(_t("end_of_half")).select("id", "type.text").rows()


@pytest.mark.parametrize("game_id", CAPTURES)
def test_half_end_home_wp_is_continuous_into_the_next_real_play(game_id):
    # 2.26 (the NFL analog of R832-6): nflfastR has no half-end branch, home_wpa = lead(home_wp) -
    # home_wp with the marker rows filled up from the next play (helper_add_ep_wp.R:1073-1181), so the
    # play that ends a half hands the home WP to the next real play, past a Timeout or an End Period
    # row, whichever team has the ball there. 18 of the 37 captures broke it before 2.26: a half-ending
    # kneel kept the ball and fell through to the model's own end state.
    f = run(game_id).plays
    real = ~pl.col("type.text").is_in(clock_stoppage_vec)
    rows = f.with_columns(_next_home_wp=pl.when(real).then(pl.col("home_wp_before")).shift(-1).backward_fill()).filter(
        _t("end_of_half") & real & pl.col("_next_home_wp").is_not_null()
    )
    assert rows.height >= 1  # the first half's end always has a next play
    gap = rows.filter((pl.col("home_wp_after") - pl.col("_next_home_wp")).abs() > 1e-6)
    assert gap.select("id", "type.text", "home_wp_after", "_next_home_wp").rows() == []


# Games whose last real play is a scored try (fixtures/try_endings/, from nfl-raw; not whole-game captures)
_TRY_ENDINGS = {291122008: "Extra Point Good", 271021004: "Two Point Pass"}


@pytest.mark.parametrize("game_id", list(_TRY_ENDINGS))
def test_try_ending_game_keeps_the_trys_own_epa(game_id):
    # R832-3. Oracle: nflfastR helper_add_ep_wp.R:710-724 rewrites epa to 0 - ep only when sp == 0, and
    # a good try is a scoring play (sp == 1), so the try that ends the game keeps its own EP_end -
    # EP_start (+0.08 for a kick, +1.08 for two) instead of -EP_start (-0.92).
    with gzip.open(FIX / "try_endings" / f"summary_{game_id}.json.gz", "rt", encoding="utf-8") as fh:
        summary = json.load(fh)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(nfl_fourth_down, "_try_load", lambda name: None)
        proc = NFLPlayProcess(gameId=game_id)
        proc.espn_nfl_pbp(summary=summary)
        proc.run_processing_pipeline()
    last = proc.plays_frame.filter(~pl.col("type.text").is_in(clock_stoppage_vec)).tail(1).row(0, named=True)
    _require(last["type.text"] == _TRY_ENDINGS[game_id] and last["scoringPlay"] is True, str(last["type.text"]))
    assert last["end_of_half"] is True
    assert abs(last["EPA"] - (last["EP_end"] - last["EP_start"])) < 1e-9, (
        last["EPA"],
        last["EP_end"],
        last["EP_start"],
    )
    assert last["EPA"] > 0, last["EPA"]


# ---------------------------------------------------------------------------------------------
# C-H5, C-M1, C-NF-6, C-M2: rows the CFB copy or ESPN's feed already gets right
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("game_id", _cases())
def test_end_down_minus_one_kickoff_return_ends_on_a_real_down(game_id):
    # Oracle: the CFB copy (cfb_pbp.py, __add_downs_data): a non-scoring kickoff ends at a down 1-4.
    # C-H5 (task 2.3): 21 returns in 7 games ended at ESPN's -1.
    f = run(game_id).plays.filter(pl.col("type.text").is_in(kickoff_vec) & ~_t("scoringPlay"))
    assert _bad(f, pl.col("end.down").is_in([1, 2, 3, 4]), ["type.text", "end.down"]) == []


@pytest.mark.parametrize("game_id", _cases())
def test_end_down_minus_one_missed_field_goal_ends_on_a_real_down(game_id):
    # Oracle: the opponent takes over at a first down (the 2015+ feeds ship down 1 or 3); C-H5's
    # mechanism on a second play type. EX-1.1-c (task 2.3): 8 pre-2014 rows in 7 games.
    f = run(game_id).plays.filter(pl.col("type.text").is_in(["Field Goal Missed", "Blocked Field Goal"]))
    assert _bad(f.filter(~_t("scoringPlay")), pl.col("end.down").is_in([1, 2, 3, 4]), ["type.text", "end.down"]) == []


@pytest.mark.parametrize("game_id", _cases())
def test_end_down_minus_one_only_on_a_score(game_id):
    # Oracle: ESPN's own feed. Only a score is followed by no snap of the same possession (a try or
    # a kickoff), so only a score may end at down -1; on HEAD before task 2.3, 29 non-scoring plays
    # did (21 kickoff returns, 8 missed field goals). Timeouts are not plays: nflverse carries no
    # media timeout and gives a team timeout no down, so their -1 stands.
    f = run(game_id).plays.filter(~_t("scoringPlay") & ~pl.col("type.text").is_in(clock_stoppage_vec))
    assert _bad(f, pl.col("end.down") != -1, ["type.text", "end.down"]) == []


# nflverse's epa on the plays task 2.3 moves (fixtures/nflverse/end_down_minus_one_epa.csv; the README
# has the provenance): 21 kickoff returns and every unblocked missed field goal in the captures.
# Ceilings are the measured MAE rounded up to the next hundredth:
# - kickoff returns (21 rows): 0.952 on devel-nfl -> 0.0723 under the owner's kickoff model -> 0.08;
# - missed field goals from 2014 on (16 rows): 0.3763 on devel-nfl -> 0.2446 under the owner's
#   missed field goal model -> 0.25. The 8 pre-2014 misses stay in the CSV but are out of the test:
#   the FG row's own start line is unreliable in those feeds (a separate finding), and the model
#   reads it (0.162 with the receiver's next snap, 0.178 with the formula).
_END_DOWN_EPA_MAE = {"field_goal": 0.25, "kickoff": 0.08}


@pytest.mark.parametrize("play_type", list(_END_DOWN_EPA_MAE))
def test_end_down_minus_one_epa_agrees_with_nflverse(play_type):
    nv = pl.read_csv(FIX / "nflverse" / "end_down_minus_one_epa.csv").filter(
        (pl.col("play_type") == play_type) & (pl.col("nflverse_game_id").str.slice(0, 4).cast(pl.Int32) >= 2014)
    )
    ours = pl.concat(
        run(g)
        .plays.select(pl.col("id").cast(pl.Utf8), pl.col("EPA").cast(pl.Float64))
        .with_columns(espn_game_id=pl.lit(g, dtype=pl.Int64))
        for g in nv["espn_game_id"].unique().sort()
    ).with_columns(play_id=pl.col("id").str.slice(9).cast(pl.Int64))
    j = nv.join(ours, on=["espn_game_id", "play_id"], how="inner")
    _require(j.height == nv.height, f"{j.height} of {nv.height} nflverse plays matched")
    assert (j["EPA"] - j["epa"]).abs().mean() <= _END_DOWN_EPA_MAE[play_type]


def _nflverse_next_snap(game_id: int) -> pl.DataFrame:
    """Each nflverse play with the down / ydstogo / yardline of the next row that has a down."""
    nv = _nflverse(game_id).with_columns(pl.col("play_id").cast(pl.Int64)).sort("play_id")
    nxt = pl.when(pl.col("down").is_not_null())
    return nv.with_columns(
        next_down=nxt.then(pl.col("down")).shift(-1).backward_fill(),
        next_ydstogo=nxt.then(pl.col("ydstogo")).shift(-1).backward_fill(),
    )


def _run_edited(game_id: int, play_id: str, edit) -> pl.DataFrame:
    """Run a capture with ``edit(plays, i)`` applied to its drive plays, ``i`` the index of ``play_id``."""
    summary = copy.deepcopy(load_summary(game_id))
    plays = [p for d in summary["drives"]["previous"] for p in d["plays"]]
    edit(plays, next(k for k, p in enumerate(plays) if p["id"] == play_id))
    proc = NFLPlayProcess(gameId=game_id)
    proc.espn_nfl_pbp(summary=summary)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(nfl_fourth_down, "_try_load", lambda name: None)
        proc.run_processing_pipeline()
    return proc.plays_frame


def _next_out_of_order(plays: list, i: int) -> None:
    plays[i + 1]["sequenceNumber"] = str(int(plays[i]["sequenceNumber"]) - 1)


def _next_is_a_kickoff(plays: list, i: int) -> None:
    # A re-kick by the same kicker, so the next row's possession is still the receiver's and only the
    # not-a-kickoff guard can reject it (the possession check, #831, would catch any other kickoff).
    plays[i + 1]["type"]["text"] = "Kickoff"
    plays[i + 1]["start"]["team"] = copy.deepcopy(plays[i]["start"]["team"])


@pytest.mark.parametrize("edit", [_next_out_of_order, _next_is_a_kickoff], ids=["out_of_order", "next_is_kickoff"])
def test_end_down_minus_one_kickoff_net_spot_falls_back_to_espn(edit):
    # A kickoff ends at the receiver's next snap, but only when the next row by id is also the next
    # snap by sequenceNumber (pre-2014 ids are not always in game order, R785-4) and is not another
    # kickoff (a half-ending play ESPN files under the next period is followed by its kickoff).
    # Otherwise ESPN's end spot stands. 400874512's return (play 326): ESPN ends it at the 85, a
    # flipped spot (R810-5); the receiver's next snap starts at the 53.
    game_id, ko = 400874512, "400874512326"
    clean = run(game_id).plays.filter(pl.col("id").cast(pl.Utf8) == ko).row(0, named=True)
    row = _run_edited(game_id, ko, edit).filter(pl.col("id").cast(pl.Utf8) == ko).row(0, named=True)
    assert (clean["end.down"], clean["end.distance"], clean["end.yardsToEndzone"]) == (1, 10, 53)
    assert (row["end.down"], row["end.distance"], row["end.yardsToEndzone"]) == (1, 10, 85)


def _espn_ships_second_and_seven(plays: list, i: int) -> None:
    plays[i]["end"]["down"], plays[i]["end"]["distance"] = 2, 7
    _next_is_a_kickoff(plays, i)


def test_end_down_minus_one_kickoff_ignores_espn_down_and_distance():
    # The owner's kickoff model reads 1st & 10 at the net spot whatever down / distance ESPN ships.
    # Every capture kickoff already reaches 1st & 10 some other way (ESPN's -1 or the next snap), so
    # the return 400874512/326 is given ESPN's "2nd & 7" with no next snap to read (a kickoff
    # follows): it still ends at 1st & 10, at ESPN's spot.
    game_id, ko = 400874512, "400874512326"
    row = _run_edited(game_id, ko, _espn_ships_second_and_seven).filter(pl.col("id").cast(pl.Utf8) == ko)
    assert row.select("end.down", "end.distance", "end.yardsToEndzone").row(0) == (1, 10, 85)


@pytest.mark.parametrize("game_id", _cases())
def test_end_down_minus_one_every_kickoff_ends_at_a_first_down(game_id):
    # The owner's kickoff model: EP(1st & 10 at the net spot, for the team with the ball after the
    # kick) - EP(1st & 10 at the touchback spot). Every unscored kickoff (returned, out of bounds,
    # penalty spot, onside) ends at 1st & 10, or 1st & goal inside the 10, whatever ESPN ships.
    f = run(game_id).plays.filter(pl.col("type.text").is_in(kickoff_vec) & ~_t("scoringPlay"))
    goal = pl.when(pl.col("end.yardsToEndzone").is_between(1, 9)).then(pl.col("end.yardsToEndzone")).otherwise(10)
    ok = (pl.col("end.down") == 1) & (pl.col("end.distance") == goal)
    assert _bad(f, ok, ["type.text", "end.down", "end.distance", "end.yardsToEndzone"]) == []


@pytest.mark.parametrize("game_id", [400554340, 401030962, 401326467, 401547449, 401671740])  # the nflverse slices
def test_end_down_minus_one_kickoff_ends_at_the_next_snap_in_nflverse(game_id):
    # Oracle: nflverse's next snap after every unscored kickoff the slice and the capture share.
    ours = run(game_id).plays.filter(pl.col("type.text").is_in(kickoff_vec) & ~_t("scoringPlay"))
    ours = ours.with_columns(play_id=pl.col("id").cast(pl.Utf8).str.slice(len(str(game_id))).cast(pl.Int64))
    j = ours.join(_nflverse_next_snap(game_id), on="play_id")
    _require(j.height > 0, "no kickoff joined")
    ok = (pl.col("end.down") == pl.col("next_down")) & (pl.col("end.distance") == pl.col("next_ydstogo"))
    assert _bad(j, ok, ["type.text", "end.down", "end.distance", "next_down", "next_ydstogo"]) == []


@pytest.mark.parametrize(
    ("game_id", "play", "start", "spot"),
    [(271209010, "2712090101602", 11, 80), (401547449, "4015474491782", 29, 64)],
    ids=["capped_at_the_20", "spot_of_the_kick"],
)
def test_end_down_minus_one_missed_field_goal_spot_on_real_rows(game_id, play, start, spot):
    # The owner's formula on two capture rows: a kick from the 11 is spotted at the 18, inside the
    # 20, so the ball goes to the 20 (nflverse's next snap: the 80); a kick from the 29 gives the 36
    # (100 - 29 - 7 = 64; nflverse's next snap: the 63).
    row = run(game_id).plays.filter(pl.col("id").cast(pl.Utf8) == play).row(0, named=True)
    assert row["start.yardsToEndzone"] == start
    assert (row["end.down"], row["end.distance"], row["end.yardsToEndzone"]) == (1, 10, spot)
    assert row["end.pos_team.id"] != row["start.pos_team.id"]


@pytest.mark.parametrize("game_id", _cases())
def test_end_down_minus_one_score_keeps_espns_minus_one(game_id):
    # Oracle: ESPN's own feed. A score is followed by a try or a kickoff, not a snap of the next
    # possession, and the series / first-down / touchdown logic reads its -1 (rewriting the 321 scored
    # -1 rows in the captures moves first_down_created on 160, series on 18, series_result on 5 and
    # wpa on 76; review PR-810, R810-8). Every scored row ESPN ships at -1 keeps it.
    summary = load_summary(game_id)
    shipped = {
        p["id"]: (p.get("end") or {}).get("down")
        for d in (summary.get("drives") or {}).get("previous", [])
        for p in d.get("plays", [])
    }
    minus_one = [int(k) for k, v in shipped.items() if v == -1]
    f = run(game_id).plays.filter(_t("scoringPlay") & pl.col("id").cast(pl.Int64).is_in(minus_one))
    assert _bad(f, pl.col("end.down") == -1, ["type.text", "end.down"]) == []


def _next_snap_is_a_kickoff(plays: list, i: int) -> None:
    j = next(k for k in range(i + 1, len(plays)) if plays[k]["type"]["text"] not in clock_stoppage_vec)
    plays[j]["type"]["text"] = "Kickoff"


def _fg_blocked_text(plays: list, i: int) -> None:
    plays[i]["text"] = plays[i]["text"].replace("is No Good, Wide Right", "is BLOCKED")


def _fg_ships_second_and_five(plays: list, i: int) -> None:
    plays[i]["end"]["down"], plays[i]["end"]["distance"] = 2, 5
    _next_snap_is_a_kickoff(plays, i)


def _fg_scored(plays: list, i: int) -> None:
    plays[i]["scoringPlay"] = True


@pytest.mark.parametrize(
    ("edit", "expected"),
    [
        (_fg_blocked_text, (1, 10, 63)),  # ESPN's own end stands (a live ball), not the formula's 64
        (_fg_ships_second_and_five, (1, 10, 64)),  # the model's 1st & 10 at the formula spot
        (_fg_scored, (1, 10, 63)),  # a scored row is left to the scoring logic: ESPN's 63 stands
    ],
    ids=["blocked_text_is_left_alone", "espn_second_and_five_becomes_first_and_ten", "scored_miss_is_left_alone"],
)
def test_end_down_minus_one_missed_field_goal_model_branches(edit, expected):
    # R810-9, on a copy of a real row: 401547449/1782 (M.Gay 47 yards, No Good, from the 29). ESPN ends
    # it at the receiver's 1st & 10 at the 63; the owner's formula gives 100 - 29 - 7 = 64. Each edit
    # is the only change to the capture:
    # - blocked_text_is_left_alone: the text says "is BLOCKED" (168 "Field Goal Missed" rows in
    #   nfl-raw say blocked; a blocked kick is a live ball, so the model does not touch it);
    # - espn_second_and_five_becomes_first_and_ten: ESPN ships "2nd & 5" and the next snap is a
    #   kickoff, so nothing but the model's `| missed_fg` first-down clause sets the end (about 100
    #   2014+ misses in nfl-raw do not end at ESPN's 1st & 10);
    # - scored_miss_is_left_alone: scoringPlay is true (3 scored "Field Goal Missed" rows in nfl-raw).
    game_id, fg = 401547449, "4015474491782"
    row = _run_edited(game_id, fg, edit).filter(pl.col("id").cast(pl.Utf8) == fg)
    assert row.select("end.down", "end.distance", "end.yardsToEndzone").row(0) == expected


@pytest.mark.parametrize("game_id", _cases())
def test_end_down_minus_one_missed_field_goal_ends_at_the_kick_spot(game_id):
    # The owner's missed field goal model: EP(the other team, 1st & 10 at the spot of the kick) -
    # EP_start, every season; the kick is 7 yards behind the line, and since 1994 a kick from inside
    # the 20 gives the ball at the 20. A blocked kick (a live ball) keeps its own result.
    f = run(game_id).plays.filter(
        (pl.col("type.text") == "Field Goal Missed")
        & ~_t("scoringPlay")
        & ~pl.col("text").str.contains("(?i)block").fill_null(False)
        & (pl.col("end.TimeSecsRem") > 0)  # a half-ending miss is scored 0 - EP_start (nflfastR)
    )
    spot = pl.min_horizontal(100 - pl.col("start.yardsToEndzone") - 7, pl.lit(80))
    ok = (
        (pl.col("end.down") == 1)
        & (pl.col("end.distance") == 10)
        & (pl.col("end.yardsToEndzone") == spot)
        & (pl.col("end.pos_team.id") != pl.col("start.pos_team.id"))
    )
    assert _bad(f, ok, ["start.yardsToEndzone", "end.down", "end.distance", "end.yardsToEndzone"]) == []


@pytest.mark.parametrize("game_id", _cases())
def test_td_flags_nullified_touchdown_is_not_a_touchdown(game_id):
    f = run(game_id).plays.filter(pl.col("text").str.contains(r"(?i)TOUCHDOWN NULLIFIED").fill_null(False))
    assert _bad(f, ~_t("td_play"), ["td_play", "scoringPlay"]) == []


@pytest.mark.parametrize("game_id", _cases())
def test_td_flags_reversed_touchdown_is_not_a_touchdown(game_id):
    # Oracle: ESPN's own scoringPlay is False on these rows ("... and the play was REVERSED").
    f = run(game_id).plays.filter(pl.col("text").str.contains("(?i)REVERSED").fill_null(False) & ~_t("scoringPlay"))
    assert _bad(f, ~(_t("td_play") | _t("pass_td") | _t("rush_td")), ["type.text", "td_play"]) == []


def test_td_flags_scored_row_keeps_a_touchdown_its_no_play_does_not_touch():
    # Oracle: ESPN scored both rows (published nfl/espn/pbp 2008, 2014; no capture has the shape). The
    # "No Play" is the overturned ruling's (a reversal INTO the touchdown) and the try's false start.
    texts = [
        "L.Johnson pass incomplete short right to T.Gonzalez.PENALTY on SD-E.Weddle, Defensive Pass "
        "Interference, 3 yards, enforced at SD 4 - No Play.Kansas City challenged the pass completion ruling, "
        "and the play was REVERSED.L.Johnson pass short right to T.Gonzalez for 4 yards, TOUCHDOWN.",
        "(2:00) A.Smith pass deep left to A.Fasano for 19 yards, TOUCHDOWN. The Replay Official challenged the "
        "runner broke the plane ruling, and the play was Upheld. (Kick formation) C.Santos extra point is GOOD. "
        "PENALTY on KC-E.Fisher, False Start, 5 yards, enforced at OAK 2 - No Play.",
    ]
    f = pl.DataFrame(
        {
            "text": texts,
            "scoringPlay": [True, True],
            "type.text": ["Passing Touchdown", "Passing Touchdown"],
            "game_id": [281214012, 400554293],
            "season": [2008, 2014],
        }
    )
    assert f.select(_touchdown_did_not_stand()).to_series().to_list() == [False, False]


# Untrimmed nfl-raw summaries holding the three task 2.20 shapes (fixtures/td_flag_residuals/; not
# whole-game captures). Oracle: nflverse touchdown / rush_touchdown on the play (local pbp_<season>.parquet,
# values in the fixtures README) -> the (td_play, rush_td) the row must carry; pass_td is False on all four.
_TD_RESIDUALS = {
    # R808-2: "TOUCHDOWN NULLIFIED by Penalty", but the excessive-celebration foul was enforced on the
    # kickoff and the touchdown stood: the next row is the try, "Extra Point Good" (nflverse 1/1).
    (311218002, 3112180024175): (True, True),
    # R840-2: a scored "TOUCHDOWN NULLIFIED" row with no try after it (holding; nflverse 0). ESPN's raw score
    # glitches on the row before (27 -> 24), which a score-movement signal would have read as a touchdown.
    (291129016, 2911290162862): (False, False),
    # R808-3: a fumble-return touchdown reversed on challenge into a safety, which ESPN scores (nflverse 0).
    (291126008, 2911260083341): (False, False),
    # R808-4: a 2024 short-of-landing-zone kick and an onside kick, unscored, typed "Kickoff Return
    # Touchdown" with no touchdown in the text (nflverse 0).
    (401671715, 4016717152262): (False, False),
    (401671715, 4016717154728): (False, False),
    # R840-1: the floor. A real 2007 kickoff return touchdown the bare-text feed writes as "Kickoff", unscored,
    # with ESPN type "Kickoff Return Touchdown" (nflverse touchdown 1, return_touchdown 1); before 2009 it stands.
    (271209034, 2712090341856): (True, False),
}


@functools.cache
def _residual_plays(game_id: int) -> pl.DataFrame:
    with gzip.open(FIX / "td_flag_residuals" / f"summary_{game_id}.json.gz", "rt", encoding="utf-8") as fh:
        summary = json.load(fh)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(nfl_fourth_down, "_try_load", lambda name: None)
        proc = NFLPlayProcess(gameId=game_id)
        proc.espn_nfl_pbp(summary=summary)
        proc.run_processing_pipeline()
    return proc.plays_frame


@pytest.mark.parametrize(("game_id", "play_id"), list(_TD_RESIDUALS))
def test_td_flags_residual_row_matches_nflverse(game_id, play_id):
    row = _residual_plays(game_id).filter(pl.col("id") == play_id).row(0, named=True)
    assert (row["td_play"], row["rush_td"], row["pass_td"]) == (*_TD_RESIDUALS[(game_id, play_id)], False), (
        row["type.text"],
        row["text"],
    )


@pytest.mark.parametrize("game_id", _cases())
def test_no_end_of_game_admin_row_is_a_play(game_id):
    # Oracle: nflfastR helper_add_ep_wp.R:706-708, 728-731 -- the END GAME row is no play (epa, ep NA).
    # Task 2.2 (C-M2) flipped 221013007, 221013028, 231221029 and 241010028.
    assert run(game_id).plays.filter(_ADMIN_END).select("id", "type.text", "EPA").rows() == []


def test_admin_row_2002_end_of_game_dropped():
    # C-M2 on 2002 MIA@DEN: ESPN's feed carries the "End of the game." row with no type (the
    # processor labels it Unknown), which the type.text marker filter kept as a play with an EPA; the
    # processed game has none.
    raw = [p for d in load_summary(221013007)["drives"]["previous"] for p in d["plays"]]
    _require(
        any((p.get("text") or "").startswith("End of the game") and not p.get("type") for p in raw),
        "221013007's feed no longer carries the untyped end-of-game row",
    )
    assert run(221013007).plays.filter(_ADMIN_END).height == 0


# ---------------------------------------------------------------------------------------------
# Team box score vs ESPN's own box (C-L17's live-gate-free box-score test, the NFL half)
# ---------------------------------------------------------------------------------------------

_FEED_GAP = (
    "EX-1.1-b: ESPN's play feed disagrees with its own box on passing TDs (251113015 MIA: a null-text "
    "TD typed Fumble; 260917021 NYG: a 'Pass Completion' stub typed Passing Touchdown; 400951676 SEA: no TD row)"
)


@pytest.mark.parametrize(
    "game_id",
    _cases(dict.fromkeys((251113015, 260917021, 400951676), _FEED_GAP)),
)
def test_box_td_parity_team_box_touchdowns_match_espn_box(game_id):
    r = run(game_id)
    espn = {}
    for team in r.summary["boxscore"]["players"]:
        for cat in team["statistics"]:
            for name, key in (("passing", "passingTouchdowns"), ("rushing", "rushingTouchdowns")):
                if cat["name"] == name:
                    espn[(int(team["team"]["id"]), name)] = int(cat["totals"][cat["keys"].index(key)])
    ours = {}
    for name, table, col in (("passing", "pass", "Pass_TD"), ("rushing", "rush", "Rush_TD")):
        for team_id, n in pl.DataFrame(r.game["advBoxScore"][table]).group_by("pos_team").agg(pl.col(col).sum()).rows():
            ours[(int(team_id), name)] = int(n)
    _require(len(espn) == 4, f"ESPN box lacks a team passing/rushing total: {espn}")
    assert {k: (v, ours.get(k, 0)) for k, v in espn.items() if ours.get(k, 0) != v} == {}


# ---------------------------------------------------------------------------------------------
# nflverse slices: 401547449 = 2023_04_LA_IND, 401671740 = 2024_04_PHI_TB (C-H3, C-M3, C-M5)
# ---------------------------------------------------------------------------------------------

SLICES = {401547449: "2023_04_LA_IND", 401671740: "2024_04_PHI_TB"}
# EX-1.5-d: one capture from each season ESPN's play `end` state is corrupt in (2014-2021)
END_STATE_SLICES = {400554340: "2014_11_MIN_CHI", 401030962: "2018_13_IND_JAX", 401326467: "2021_10_JAX_IND"}
# ESPN rows carrying a GSIS play_id nflverse has
_MATCHED = {401547449: 171, 401671740: 159, 400554340: 152, 401030962: 164, 401326467: 171}


def _nflverse(game_id: int) -> pl.DataFrame:
    return pl.read_parquet(FIX / "nflverse" / f"{(SLICES | END_STATE_SLICES)[game_id]}.parquet")


def _joined(game_id: int) -> pl.DataFrame:
    """ESPN rows joined to nflverse on play_id (ESPN id = game id + GSIS play_id); nflverse cols ``*_nv``."""
    nv = _nflverse(game_id).with_columns(pl.col("play_id").cast(pl.Int64))
    espn = run(game_id).plays.with_columns(
        play_id=pl.col("id").cast(pl.Utf8).str.slice(len(str(game_id))).cast(pl.Int64)
    )
    joined = espn.join(nv.rename({c: f"{c}_nv" for c in nv.columns if c != "play_id"}), on="play_id", how="inner")
    _require(
        joined.height == _MATCHED[game_id], f"{joined.height} ESPN rows match nflverse, expected {_MATCHED[game_id]}"
    )
    return joined


# Series and drives are compared on the plays both feeds carry: nflverse also numbers rows ESPN's
# feed has no row for (its "END GAME" / "END QUARTER 2" rows open a series of their own: 401547449's
# 62nd series and 401030962's 27th), so a whole-frame count would score the feed, not the processor.
_ALL_SLICES = (*SLICES, *END_STATE_SLICES)


@pytest.mark.parametrize("game_id", _ALL_SLICES)
def test_nflverse_series_count(game_id):
    # task 2.4 (C-H3, C-M3): base 59/61, 57/59, 49/50, 55/51, 49/51 in _ALL_SLICES order
    j = _joined(game_id)
    assert j["series"].n_unique() == j["series_nv"].n_unique()


@pytest.mark.parametrize("game_id", _ALL_SLICES)
def test_nflverse_series_starts_on_the_same_plays(game_id):
    # helper_add_series_data.R:35-47: a series opens on the same joined play in both frames
    j = _joined(game_id).sort("game_play_number")
    opens = pl.col("series").diff().fill_null(1) != 0, pl.col("series_nv").diff().fill_null(1) != 0
    assert j.filter(opens[0] != opens[1]).select("id", "series", "series_nv").rows() == []


@pytest.mark.parametrize("game_id", _ALL_SLICES)
def test_nflverse_series_fixed_drive(game_id):
    # helper_add_fixed_drives.R:32-137. Base: 401671740 20 vs 21 drives (EX-1.1-a, the muffed punt the
    # kicking team recovers), 401030962 a drive at every 2018 timeout row (ESPN's team there is often
    # the defense's; nflverse leaves a timeout's posteam NA), 401326467 no drive after the blocked-punt
    # touchdown (ESPN folds the try, whose posteam opens nflverse's drive, into the scoring row).
    j = _joined(game_id)
    assert (
        j.filter(pl.col("fixed_drive") != pl.col("fixed_drive_nv")).select("id", "fixed_drive", "fixed_drive_nv").rows()
        == []
    )


@pytest.mark.parametrize("game_id", _ALL_SLICES)
def test_nflverse_series_first_down_penalty(game_id):
    # base 2 vs 6, 1 vs 3, 0 vs 1, 1 vs 5, 1 vs 3: the CFB phrase "1st down" never occurs in NFL text
    j = _joined(game_id)
    ours, theirs = pl.col("first_down_penalty").cast(pl.Int64), pl.col("first_down_penalty_nv").cast(pl.Int64)
    assert (
        run(game_id).plays["first_down_penalty"].cast(pl.Int64).sum() == _nflverse(game_id)["first_down_penalty"].sum()
    )
    assert j.filter(ours != theirs).select("id", "first_down_penalty", "text").rows() == []


@pytest.mark.parametrize("game_id", _ALL_SLICES)
def test_nflverse_series_first_down_penalty_cleaning_pipeline(game_id):
    # review R806-1: run_cleaning_pipeline() skipped the penalty step and returned 0 on every slice
    proc = NFLPlayProcess(gameId=game_id)
    proc.espn_nfl_pbp(summary=load_summary(game_id))  # a supplied summary never touches the network
    plays = proc.run_cleaning_pipeline()["plays"]
    assert sum(bool(p["first_down_penalty"]) for p in plays) == _nflverse(game_id)["first_down_penalty"].sum()


@pytest.mark.parametrize("game_id", tuple(SLICES))
def test_fixed_drive_result_has_nflverse_null_count(game_id):
    # before task 2.2 (C-H2): 1 and 7 nulls, the final drive never reached "End of half"; nflverse
    # has 0 in both games
    assert (
        run(game_id).plays["fixed_drive_result"].null_count() == _nflverse(game_id)["fixed_drive_result"].null_count()
    )


_SNAP = pl.col("play_type_nv").is_in(["pass", "run", "punt", "field_goal", "qb_kneel", "qb_spike"])
# ESPN states a kickoff from the receiving side (distance 10, 65 to go) where nflverse writes
# ydstogo 0 / yardline_100 35, and ESPN's touchdown row carries the try's point in its score:
# those rows are outside the domain of the columns below, by the feed's convention. Rushing yards
# are compared on ESPN rushes (nflverse also books an aborted snap as a 0-yard run; the `rush`
# definitions differ, the yardage does not).
_EXACT = [
    ("period", "qtr", _SNAP),
    ("start.down", "down", _SNAP),
    ("start.distance", "ydstogo", _SNAP),
    ("start.yardsToEndzone", "yardline_100", _SNAP),
    ("yds_rushed", "rushing_yards", _RUSH & pl.col("rushing_yards_nv").is_not_null()),
    ("yds_receiving", "receiving_yards", pl.col("receiving_yards_nv").is_not_null()),
    ("end.homeScore", "total_home_score", pl.col("touchdown_nv") == 0),
    ("end.awayScore", "total_away_score", pl.col("touchdown_nv") == 0),
    ("td_play", "touchdown", pl.lit(True)),
    ("pass_td", "pass_touchdown", pl.lit(True)),
    ("rush_td", "rush_touchdown", pl.lit(True)),
    ("fixed_drive", "fixed_drive", pl.lit(True)),
]
_EXACT_KNOWN: dict[tuple[int, str], str] = {}


@pytest.mark.parametrize(
    ("game_id", "ours", "theirs", "domain"),
    [
        pytest.param(g, a, b, d, id=f"{g}-{a}", marks=_xfail(_EXACT_KNOWN[(g, a)]))
        if (g, a) in _EXACT_KNOWN
        else pytest.param(g, a, b, d, id=f"{g}-{a}")
        for g in SLICES
        for a, b, d in _EXACT
    ],
)
def test_play_column_equals_nflverse(game_id, ours, theirs, domain):
    j = _joined(game_id).filter(domain)
    _require(j.height > 0, f"no {ours} rows in the comparison domain")
    left, right = pl.col(ours).cast(pl.Float64), pl.col(f"{theirs}_nv").cast(pl.Float64)
    assert j.filter(left.ne_missing(right)).select("id", ours, f"{theirs}_nv").rows() == []


# (corr floor, MAE ceiling, rows) per game and column: measured on HEAD, see the module docstring.
# 2.2 (C-H2) dropped the half-ending exclusion and re-measured on every joined row (raise only):
# EP_start on both games and EPA on 401547449 hold or raise their floors with the half-ending rows in.
# Three (game, column) pairs would loosen a floor with a row, for a reason that is not C-H2, so that
# row stays out of that pair only (_HELD_OUT) and gets its own bound in
# test_held_out_row_sits_in_the_band (the #810 ruling: an excluded row is bounded, never dropped);
# test_half_ending_epa_gap_is_the_ep_gap also holds the half-ending EPA to nflverse directly. 2.5 (C-M1/C-NF-6) dropped the negated-score exclusion (+2 rows per
# game); 2.3 (C-H5) dropped the kickoff exclusion and checks the two returns it fixed separately
# (_H5_RETURNS below).
_MODEL = {
    # measured on the 2.2 head, on the rows asserted: 401547449 171 (170 for wp), 401671740 157 (156 EPA)
    (401547449, "EP_start", "ep"): (0.996, 0.117, 171),  # 0.99614, 0.11698
    (401547449, "EPA", "epa"): (0.990, 0.111, 171),  # 0.99049, 0.11083 (2.2; was 0.99018, 0.11118 on 169)
    (401547449, "wp", "wp"): (0.993, 0.024, 170),  # 0.99338, 0.02335 (2.2 R832-1: +the regulation kneel)
    (401547449, "vegas_wp", "vegas_wp"): (0.987, 0.063, 170),  # 0.98766, 0.06204 (the same)
    (401671740, "EP_start", "ep"): (0.996, 0.119, 157),  # 0.99617, 0.11820 (EX-1.5-d; was 0.99278, 0.12545)
    (401671740, "EPA", "epa"): (0.972, 0.132, 156),  # 0.97255, 0.13188 (EX-1.5-d; was 0.92781, 0.32713)
    (401671740, "wp", "wp"): (0.999, 0.007, 157),  # 0.99980, 0.00643
    (401671740, "vegas_wp", "vegas_wp"): (0.991, 0.062, 157),  # 0.99180, 0.06039
}
# ESPN play ids the floors above leave out, per (game, column), with the value the pair reads with them:
# * 401671740 EPA: the final kneel (EPA -3.998 == -EP_start, nflverse -4.205 == -ep): its 0.207 gap is
#   the EP model's (EP_start 3.998 vs ep 4.205); 0.97429, 0.13236.
# * 401547449 wp / vegas_wp: the overtime touchdown. Start-of-play wp does not read end_of_half;
#   sdv-py's wp sits below nflverse's on every overtime row of that game (0.48-0.63 vs 0.56-0.89, the
#   other rows already counted in the floors; EX-2.2-a). With it: 0.99159, 0.02490 and 0.98691, 0.06300.
_HELD_OUT = {
    (401671740, "EPA"): (4016717404375,),
    (401547449, "wp"): (4015474494565,),
    (401547449, "vegas_wp"): (4015474494565,),
}
# Each held-out row's bound: (band pair's ours, theirs, the asserted rows its error belongs with). The
# kneel's EPA gap is its EP_start gap, so the EP_start pair's asserted rows bound it (max 0.960; the
# kneel 0.207). The overtime touchdown is bounded by its pair's asserted overtime rows (wp max 0.2611,
# vegas_wp 0.2301; the touchdown 0.2880 and 0.2262).
_HELD_OUT_BAND = {
    (401671740, "EPA", "epa", 4016717404375): ("EP_start", "ep", pl.lit(True)),
    (401547449, "wp", "wp", 4015474494565): ("wp", "wp", pl.col("period") >= 5),
    (401547449, "vegas_wp", "vegas_wp", 4015474494565): ("vegas_wp", "vegas_wp", pl.col("period") >= 5),
}
_HELD_OUT_BAND_KNOWN = {
    (401547449, "wp", "wp", 4015474494565): (
        "EX-2.2-a: sdv-py's overtime wp drifts below nflverse's down 401547449's overtime drive; the "
        "touchdown misses by 0.288 against the overtime rows' 0.261"
    ),
}
# The two kickoff returns 2.3 (C-H5) fixed in 401671740 (GSIS play_id). The floors above stay on the
# population they were measured on (the returns left out); test_end_down_minus_one_returns_sit_in_the_
# kickoff_band covers these two against the game's other kickoffs instead (with them, 158 rows, the
# 401671740 values read 0.99607/0.12035, 0.97196/0.13546, 0.99979/0.00654, 0.99154/0.06112).
_H5_RETURNS = {401671740: (39, 2997)}


@pytest.mark.parametrize(("game_id", "ours", "theirs"), list(_MODEL), ids=lambda v: str(v))
def test_model_column_agrees_with_nflverse(game_id, ours, theirs):
    corr_floor, mae_ceiling, rows = _MODEL[(game_id, ours, theirs)]
    j = _asserted(game_id, ours, theirs)
    a, b = pl.col(ours).cast(pl.Float64), pl.col(f"{theirs}_nv").cast(pl.Float64)
    corr, mae = j.select(pl.corr(a, b).alias("corr"), (a - b).abs().mean().alias("mae")).row(0)
    assert j.height >= rows
    assert corr >= corr_floor, corr
    assert mae <= mae_ceiling, mae


def _asserted(game_id: int, ours: str, theirs: str) -> pl.DataFrame:
    """The rows test_model_column_agrees_with_nflverse asserts for one pair."""
    return _joined(game_id).filter(
        ~pl.col("id").is_in(_HELD_OUT.get((game_id, ours), ()))
        & ~pl.col("play_id").is_in(_H5_RETURNS.get(game_id, ()))
        & pl.col(ours).is_not_null()
        & pl.col(f"{theirs}_nv").is_not_null()
    )


def _err(ours: str, theirs: str) -> pl.Expr:
    return (pl.col(ours).cast(pl.Float64) - pl.col(f"{theirs}_nv").cast(pl.Float64)).abs()


@pytest.mark.parametrize(
    "key",
    [
        pytest.param(k, marks=_xfail(_HELD_OUT_BAND_KNOWN[k])) if k in _HELD_OUT_BAND_KNOWN else k
        for k in _HELD_OUT_BAND
    ],
    ids=lambda k: f"{k[0]}-{k[1]}-{k[3]}",
)
def test_held_out_row_sits_in_the_band(key):
    game_id, ours, theirs, play = key
    band_ours, band_theirs, domain = _HELD_OUT_BAND[key]
    assert {(g, c) for g, c, _, _ in _HELD_OUT_BAND} == set(_HELD_OUT), "every held-out pair has a band"
    assert play in _HELD_OUT[(game_id, ours)]
    row = _joined(game_id).filter(pl.col("id") == play)
    _require(row.height == 1, f"{play} joins {row.height} nflverse rows")
    band_rows = _asserted(game_id, band_ours, band_theirs).filter(domain)
    _require(band_rows.height > 0, "no asserted rows to band against")
    band = band_rows.select(_err(band_ours, band_theirs).max()).item()
    assert row.select(_err(ours, theirs)).item() <= band


@pytest.mark.parametrize("game_id", tuple(SLICES))
def test_half_ending_epa_gap_is_the_ep_gap(game_id):
    # 2.2 (C-H2): on a play that ends a half, regulation or the game without scoring, nflverse's epa
    # is -ep, so sdv-py's EPA gap to it is its EP_start gap exactly (no residue from the derivation).
    boundaries = ("half", "game", *(("regulation",) if game_id in _OT_GAMES else ()))
    ids = [_half_ending_play(game_id, b)["id"] for b in boundaries]
    j = _joined(game_id).filter(pl.col("id").is_in(ids) & ~_t("scoringPlay"))
    _require(j.height >= 2, f"{j.height} non-scoring half-ending plays joined")
    gap = (pl.col("EPA") - pl.col("epa_nv")) + (pl.col("EP_start") - pl.col("ep_nv"))
    assert _bad(j, gap.abs() < 1e-9, ["EPA", "epa_nv", "EP_start", "ep_nv"]) == []


@pytest.mark.parametrize("game_id", list(_H5_RETURNS))
def test_end_down_minus_one_returns_sit_in_the_kickoff_band(game_id):
    # 2.3 (C-H5): a fixed return's EPA agrees with nflverse no worse than the game's other kickoffs.
    # Measured on 401671740: returns 0.245/0.326 (0.224/0.605 before R810-1 and the owner's kickoff
    # model); other kickoffs max 0.630 (before 2.3 the returns were off 1.182/1.499). No EP_start
    # case: start-of-play EP does not read the end state, so it could not fail on a 2.3 regression
    # (R810-4).
    err = (pl.col("EPA").cast(pl.Float64) - pl.col("epa_nv").cast(pl.Float64)).abs()
    j = _joined(game_id).filter(pl.col("type.text").is_in(kickoff_vec))
    returns = j.filter(pl.col("play_id").is_in(_H5_RETURNS[game_id]))
    _require(returns.height == len(_H5_RETURNS[game_id]), f"{returns.height} returns joined")
    band = j.filter(~pl.col("play_id").is_in(_H5_RETURNS[game_id])).select(err.max()).item()
    assert returns.select(err.max()).item() <= band


# C-L20: the CP booster scored on nflverse's own inputs must reproduce nflverse's cp (a constant
# cp = 0.5 passed 121 tests). Measured: corr 0.99492 / 0.99396, MAE 0.01552 / 0.01551, 63 / 76 rows.
_CP = {401547449: (0.994, 0.016, 63), 401671740: (0.993, 0.016, 76)}
_CP_INPUTS = [
    "play_id", "air_yards", "season", "ydstogo", "down", "posteam", "home_team", "roof",
    "pass_location", "qb_hit", "complete_pass", "yardline_100",
]  # fmt: skip


@pytest.mark.parametrize("game_id", list(_CP))
def test_completion_probability_reproduces_nflverse(game_id):
    corr_floor, mae_ceiling, rows = _CP[game_id]
    nv = _nflverse(game_id)
    out = calculate_completion_probability(nv.select(_CP_INPUTS)).join(
        nv.select("play_id", "cp"), on="play_id", suffix="_nv"
    )
    out = out.filter(pl.col("cp").is_not_null() & pl.col("cp_nv").is_not_null())
    corr, mae = out.select(
        pl.corr("cp", "cp_nv").alias("corr"), (pl.col("cp") - pl.col("cp_nv")).abs().mean().alias("mae")
    ).row(0)
    assert out.height >= rows
    assert not math.isnan(corr) and corr >= corr_floor, corr
    assert mae <= mae_ceiling, mae


# ---------------------------------------------------------------------------------------------
# EX-1.5-d (task 2.18): a play ends where the next play starts, not where ESPN's `end` says
# ---------------------------------------------------------------------------------------------
# From 2014 to 2021 ESPN's `end` object carries the defense's team id and a down/distance that does
# not follow from the play on a growing share of plays (2021: 30% of plain scrimmage plays; the yard
# line stays right). The oracle is nflverse on the same plays: a possession change is a posteam
# that differs from the next play's, a first down is nflverse's `first_down`. The domain is a
# pass or run that is not a touchdown and whose next play is in the same half.


def _snaps(game_id: int) -> pl.DataFrame:
    half = pl.when(pl.col("qtr") <= 2).then(1).when(pl.col("qtr") <= 4).then(2).otherwise(pl.col("qtr"))
    nxt = (
        _nflverse(game_id)
        .filter(pl.col("play_type").is_not_null())
        .sort("play_id")
        .select(
            pl.col("play_id").cast(pl.Int64),
            next_posteam=pl.col("posteam").shift(-1),
            same_half=(half == half.shift(-1)).fill_null(False),
        )
    )
    joined = _joined(game_id)
    _require(joined.schema["play_id"] == nxt.schema["play_id"], "play_id dtypes differ")
    return joined.join(nxt, on="play_id").filter(
        pl.col("play_type_nv").is_in(["pass", "run"]) & (pl.col("touchdown_nv") == 0) & pl.col("same_half")
    )


# (snaps, change_of_poss disagreements, first-down agreement floor, EPA corr floor, EPA MAE ceiling)
# Measured after the fix (before it: 17 / 23 / 67 wrong possession changes, first downs 0.886 /
# 0.869 / 0.661, EPA corr 0.756 / 0.771 / 0.426), rounded one step toward failing.
_END_STATE = {
    400554340: (114, 0, 1.0, 0.761, 0.430),  # 1.0, 0.76179, 0.42959
    401030962: (122, 0, 1.0, 0.951, 0.197),  # 1.0, 0.95171, 0.19693
    401326467: (118, 0, 0.991, 0.932, 0.139),  # 0.99153, 0.93294, 0.13892
}


@pytest.mark.parametrize("game_id", list(END_STATE_SLICES))
def test_EX_1_5_d_change_of_poss_is_nflverse_possession_change(game_id):
    s = _snaps(game_id)
    _require(s.height >= _END_STATE[game_id][0], f"{s.height} snaps")
    ours = pl.col("change_of_poss").cast(pl.Boolean).fill_null(False)
    wrong = s.filter(ours != (pl.col("posteam_nv") != pl.col("next_posteam")))
    assert wrong.height <= _END_STATE[game_id][1], wrong.select("id", "text", "change_of_poss").rows()


@pytest.mark.parametrize("game_id", list(END_STATE_SLICES))
def test_EX_1_5_d_first_down_created_is_nflverse_first_down(game_id):
    s = _snaps(game_id)
    agree = s.select((pl.col("first_down_created").cast(pl.Int64) == pl.col("first_down_nv")).mean()).item()
    assert agree >= _END_STATE[game_id][2], agree


@pytest.mark.parametrize("game_id", list(END_STATE_SLICES))
def test_EX_1_5_d_epa_agrees_with_nflverse(game_id):
    s = _snaps(game_id).filter(pl.col("EPA").is_not_null() & pl.col("epa_nv").is_not_null())
    corr, mae = s.select(
        pl.corr("EPA", "epa_nv").alias("corr"), (pl.col("EPA") - pl.col("epa_nv")).abs().mean().alias("mae")
    ).row(0)
    assert corr >= _END_STATE[game_id][3], corr
    assert mae <= _END_STATE[game_id][4], mae


# No next play speaks for a half's last play or a play followed by a kickoff (whose start is the
# receiving team's). nflfastR keeps the snapping team's possession through a play that ends the
# half (helper_add_ep_wp.R:710-728 scores it 0 - ep; drives and series group by game_half,
# helper_add_fixed_drives.R:29): a plain snap that is not a 4th-down failure cannot hand the ball
# over. 401326132 (2021_02_DEN_JAX) play 2030, "(:02) T.Bridgewater pass ... to JAX 23 for 30
# yards" on 4th & 1, ends the half; ESPN states it for JAX, nflverse has posteam DEN, first_down 1
# (review R785-2). The same rule covers 400951676's Q4 incompletion before a kickoff (R785-3).
# ESPN's own type (before the processor re-types fumbles), no turnover in the text, downs 1-3 (the
# converted 4th down is pinned row by row below).
_HELD = (
    pl.col("orig_play_type").is_in([*normalplay, "Penalty"])
    & ~_t("scoringPlay")
    & ~pl.col("text").str.contains(r"(?i)fumble|intercept")
    & pl.col("start.down").is_between(1, 3)
).fill_null(False)


@pytest.mark.parametrize("game_id", CAPTURES)
def test_EX_1_5_d_plain_snap_without_a_next_play_keeps_possession(game_id):
    half = pl.when(pl.col("period.number") <= 2).then(1).otherwise(2)
    f = (
        run(game_id)
        .plays.filter(_PLAY)
        .with_columns(_h=half, _nh=half.shift(-1), _nk=pl.col("type.text").shift(-1).is_in(kickoff_vec))
    )
    ends = f.filter(_HELD & (pl.col("_nh").is_null() | (pl.col("_nh") != pl.col("_h")) | _t("_nk")))
    assert _bad(ends, ~_t("change_of_poss"), ["type.text", "start.down", "change_of_poss"]) == []


# The half's last snap, ESPN-stated for the other team on "1st & 10": its first down follows from its
# own gain. nflverse: 2021_02_DEN_JAX play 2030 posteam DEN, 4th & 1, first_down 1; 2021_04_CAR_DAL
# play 2167 posteam CAR, 2nd & 17, 6 yards, first_down 0.
_HALF_END_ROWS = {(401326132, 4013261322030): True, (401326383, 4013263832167): False}


@pytest.mark.parametrize(("game_id", "play_id"), list(_HALF_END_ROWS))
def test_EX_1_5_d_half_ending_snap_keeps_the_ball_and_its_own_first_down(game_id, play_id):
    row = run(game_id).plays.filter(pl.col("id") == play_id).row(0, named=True)
    assert (row["change_of_poss"], row["first_down_created"]) == (False, _HALF_END_ROWS[(game_id, play_id)])


# ESPN types a lateral fumbled away on the last snap "Pass Reception"; a lost fumble is still a possession
# change (review R785-9). nflverse 2021_17_LA_BAL play 3846: posteam BAL, fumble_lost 1, LA recovers.
_HALF_END_TURNOVERS = [(401326581, 4013265813846)]


@pytest.mark.parametrize(("game_id", "play_id"), _HALF_END_TURNOVERS)
def test_EX_1_5_d_half_ending_lateral_fumble_lost_changes_possession(game_id, play_id):
    row = run(game_id).plays.filter(pl.col("id") == play_id).row(0, named=True)
    assert row["change_of_poss"] is True


# A score keeps ESPN's end (stated for the scorer; the try follows). An offensive score does not
# hand the ball over on the play itself: the scorer kicks off next (R785-3).
@pytest.mark.parametrize("game_id", CAPTURES)
def test_EX_1_5_d_offensive_score_keeps_possession(game_id):
    f = run(game_id).plays.filter(
        _t("scoringPlay") & pl.col("type.text").is_in([*offense_score_vec, "Extra Point Good"])
    )
    assert _bad(f, ~_t("change_of_poss"), ["type.text", "change_of_poss"]) == []


# ---------------------------------------------------------------------------------------------
# C-L12: two real games concatenated give the per-game answer, column by column
# ---------------------------------------------------------------------------------------------


def _same_per_game(fn, frames: list[pl.DataFrame], key: str) -> list[str]:
    """Columns where ``fn(concat)`` differs from ``concat(fn(each game))`` (row order kept)."""
    flat = [f.select(c for c, t in f.schema.items() if not t.is_nested()) for f in frames]
    both = pl.concat(flat, how="diagonal_relaxed")
    joint = fn(both)
    per_game = pl.concat([fn(both.filter(pl.col(key) == g)) for g in both[key].unique(maintain_order=True)])
    assert joint.columns == per_game.columns and joint.height == per_game.height == both.height
    return [c for c in joint.columns if not joint[c].equals(per_game[c], null_equal=True)]


@pytest.mark.parametrize("step", ["calculate_epa", "calculate_wpa"])
def test_real_concat_equals_per_game_runs(step):
    # The pipeline's own inputs to the step, recorded for the two slice games and stacked.
    fn = {"calculate_epa": calculate_epa, "calculate_wpa": calculate_wpa}[step]
    frames = [getattr(run(g), f"{step.split('_')[1]}_input") for g in SLICES]
    assert _same_per_game(fn, frames, "game_id") == []


def test_real_concat_reaches_the_post_score_clock_stoppage_guard():
    """Reordered real rows: game 1 ends on a score, game 2 opens on its own first official timeout.

    No capture opens on a clock-stoppage row, so the stacked captures above never reach the
    ``scoring_play.shift(1).over("game_id")`` guard of ``calculate_epa``. Here every row is the
    pipeline's own input for 401547449 / 401671740; only their order is changed (game 1 truncated
    after its last scoring play, game 2's first clock-stoppage row moved to its head), and the
    oracle is still the per-game run of the same rows.
    """
    first, second = run(401547449).epa_input, run(401671740).epa_input
    last_score = first.with_row_index("_i").filter(_t("scoring_play"))["_i"].max()
    first = first.head(last_score + 1)
    stop = second.with_row_index("_i").filter(pl.col("type.text").is_in(clock_stoppage_vec))["_i"].min()
    second = pl.concat([second.slice(stop, 1), second.slice(0, stop), second.slice(stop + 1)])
    _require(first["scoring_play"][-1] is True, "game 1 must end on a scoring play")
    assert _same_per_game(calculate_epa, [first, second], "game_id") == []


def test_real_concat_equals_per_game_runs_nflverse_wpa():
    # The nflverse-path WPA derivation (enrich_nfl_pbp step 4) on the two committed slices.
    frames = [
        _nflverse(g).select(
            "game_id", "posteam", "home_team", "down", "wp", "vegas_wp", "desc", "home_score", "away_score"
        )
        for g in SLICES
    ]
    assert _same_per_game(_derive_wpa, frames, "game_id") == []


# ---------------------------------------------------------------------------------------------
# EX-2.3-a (task 2.21): a touchback kickoff ends at the season's touchback spot
# ---------------------------------------------------------------------------------------------

# The owner's kickoff model: EPA = EP(1st & 10 at the net spot) - EP(1st & 10 at the touchback spot),
# so a touchback kickoff is worth 0. EP_start_touchback starts it at the season's rule spot
# (model_vars.touchback_yardline: 80 before 2016, 75 for 2016-23, 70 in 2024, 65 from 2025); the end
# state was a literal 75, right only in 2016-23. One or more captures per era; every touchback kickoff in
# them ends at that spot with |EPA| < 1e-3, except the 2024+ landing-zone touchback below. Elsewhere in
# the captures 25 kickoff_tb rows were not 0 at the season spot, every one with ESPN's end clock moving
# (R831-3, routed to task 2.19): 22 are 2006-07 bare "KickOff" rows nflverse has as returns (R831-2,
# task 2.19), which now keep their next snap's spot; 3 are real touchbacks in the CSV below, of which
# 400791590/1855 (no next snap to read) now takes its start's clock and is 0. Owner ruling (2026-10-17,
# R856-2): every touchback kickoff's EP_end is scored at its START clock, so ESPN's moving end clock no
# longer moves its EPA; WPA keeps ESPN's end clock, because ESPN's start clock is wrong on some
# touchbacks too (400791567/36 starts at 3:47 of Q1). Unflagging the bare "KickOff" rows (R831-2) stays a
# follow-up: it moves a half-ending kickoff with no next snap off its season spot (260917021/2190).
_TB_ERAS = {
    "pre-2016": (331103020, 400791508),
    "2016-23": (401547449,),
    "2024": (401671740, 401671835),
    "2025": (401772636, 401772726, 401872922),
}
# From 2024 a kick that lands in the landing zone and rolls into the end zone is a touchback at the
# receiving 20, not the 30 / 35 (R831-1): its net spot is the next snap, as on any other kickoff.
# 401671740 play 3563, "Touchback to the PHI 20": nflverse's next snap is at yardline_100 80.
_TB_LANDING_ZONE = {401671740: 3563}


@pytest.mark.parametrize("game_id", [g for gs in _TB_ERAS.values() for g in gs])
def test_kickoff_touchback_epa_is_zero(game_id):
    f = run(game_id).plays.filter(_t("kickoff_tb") & ~_t("scoringPlay"))
    f = f.filter(pl.col("id").cast(pl.Utf8) != f"{game_id}{_TB_LANDING_ZONE.get(game_id, '')}")
    _require(f.height > 0, "no touchback kickoff in the capture")
    ok = (pl.col("end.yardsToEndzone") == touchback_yardline_expr(pl.col("season"))) & (pl.col("EPA").abs() < 1e-3)
    assert _bad(f, ok, ["season", "end.yardsToEndzone", "EP_start", "EP_end", "EPA"]) == []


@pytest.mark.parametrize(("game_id", "play_id"), list(_TB_LANDING_ZONE.items()))
def test_kickoff_touchback_landing_zone_ends_at_the_next_snap(game_id, play_id):
    # Oracle: nflverse's next snap (the 80), and the owner's model there: EP(1st & 10 at the 80) -
    # EP(1st & 10 at the 2024 touchback 70) = -0.600 (measured; -0.233 with the literal 75, 0 at the 70).
    row = run(game_id).plays.filter(pl.col("id").cast(pl.Utf8) == f"{game_id}{play_id}").row(0, named=True)
    nxt = _nflverse_next_snap(game_id).filter(pl.col("play_id") > play_id).filter(pl.col("down").is_not_null())
    assert row["kickoff_tb"] is True
    assert row["end.yardsToEndzone"] == nxt["yardline_100"][0] == 80
    assert abs(row["EPA"] - (-0.600)) < 0.01, row["EPA"]


@pytest.mark.parametrize(
    ("game_id", "play", "spot"),
    [(401671740, "401671740343", 70), (401772636, "401772636646", 65)],
    ids=["2024", "2025"],
)
def test_kickoff_touchback_without_a_next_snap_ends_at_the_season_spot(game_id, play, spot):
    # The season-spot re-spot is the fallback for a touchback whose next snap cannot be read (pre-2014
    # ids out of game order, R785-4); every 2024+ capture touchback has its next snap, so a real one is
    # given an out-of-order next row here. It must still end 1st & 10 at the season spot, EPA 0.
    row = _run_edited(game_id, play, _next_out_of_order).filter(pl.col("id").cast(pl.Utf8) == play).row(0, named=True)
    assert row["kickoff_tb"] is True
    assert (row["end.down"], row["end.distance"], row["end.yardsToEndzone"]) == (1, 10, spot)
    assert abs(row["EPA"]) < 1e-3, row["EPA"]


def _run_kickoff_fixture(name: str, game_id: int) -> pl.DataFrame:
    """Run one of the kickoff fixtures that are not whole-game captures (README "Other files")."""
    with gzip.open(FIX / name, "rt", encoding="utf-8") as fh:
        summary = json.load(fh)
    proc = NFLPlayProcess(gameId=game_id)
    proc.espn_nfl_pbp(summary=summary)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(nfl_fourth_down, "_try_load", lambda name: None)
        proc.run_processing_pipeline()
    return proc.plays_frame.sort("game_play_number")


def test_kickoff_net_spot_is_the_receivers_next_snap():
    # The owner's model reads the net spot at the RECEIVER's next snap. ESPN files 401326517's touchback
    # 1442 (J.Elliott, PHI, 2021) between two PHI snaps of one drive, so the next row is PHI's 1st & 10
    # at the NYJ 30; without the possession check that was read as NYJ's net spot (EPA +2.508). The
    # check fails, so the touchback falls back to the season spot: EPA 0 (nflverse 0.000).
    # fixtures/kickoff_misfiled_401326517_trimmed.json.gz (README "Other files").
    f = _run_kickoff_fixture("kickoff_misfiled_401326517_trimmed.json.gz", 401326517)
    i = f["id"].cast(pl.Utf8).to_list().index("4013265171442")
    row, nxt = f.row(i, named=True), f.row(i + 1, named=True)
    assert row["kickoff_tb"] is True and nxt["start.pos_team.id"] != row["start.pos_team.id"]
    assert (row["end.down"], row["end.distance"], row["end.yardsToEndzone"]) == (1, 10, 75)
    assert abs(row["EPA"]) < 1e-3, row["EPA"]


# The kicking team recovers (R810-3): the team with the ball after the kick is the kicker, its next
# snap is the net spot and EP_end is the kicker's (calculate_epa flips it). nflverse epa and the error
# before -> after (previous head 8a1cd9e57e, which read only the receiver's snap):
# - 301003002/297 (2010, capture): "C.Ellis MUFFS catch, RECOVERED by NYJ", nflverse -3.890, ours
#   0.625 -> -3.823 (error 4.52 -> 0.067); the kicker's snap is at the BUF 39.
# - 400554238/568 (2014, fixtures/kickoff_onside_400554238_trimmed.json.gz): "kicks onside ... RECOVERED
#   by IND", nflverse -3.798, ours -2.855 -> -3.498 (error 0.943 -> 0.300); IND's snap at the TEN 44.
_KICKER_RECOVERS = [
    (301003002, None, "3010030020297", 39, -3.890, 0.07),
    (400554238, "kickoff_onside_400554238_trimmed.json.gz", "400554238568", 44, -3.798, 0.31),
]


@pytest.mark.parametrize(
    ("game_id", "fixture", "play", "spot", "nflverse_epa", "ceiling"), _KICKER_RECOVERS, ids=["muff", "onside"]
)
def test_kickoff_kicking_team_recovery_ends_at_the_kickers_snap(game_id, fixture, play, spot, nflverse_epa, ceiling):
    f = run(game_id).plays if fixture is None else _run_kickoff_fixture(fixture, game_id)
    row = f.filter(pl.col("id").cast(pl.Utf8) == play).row(0, named=True)
    kicker = row["start.team.id"]  # ESPN states a kickoff from the kicking team
    assert row["end.pos_team.id"] == kicker != row["start.pos_team.id"]
    assert (row["end.down"], row["end.distance"], row["end.yardsToEndzone"]) == (1, 10, spot)
    assert abs(row["EPA"] - nflverse_epa) <= ceiling, row["EPA"]


def test_kickoff_reversed_recovery_stays_the_receivers_ball():
    # 301114003/3734 (2010): "... FUMBLES (C.Graham), RECOVERED by CHI ... the play was REVERSED. R.Gould
    # kicks ... P.Harvin to MIN 28": only the ruling after REVERSED counts, so MIN keeps the ball and the
    # net spot is MIN's next snap. Reading the reversed recovery gave EPA -1.289 against nflverse 0.399
    # (0.418 when only the receiver's snap is read). fixtures/kickoff_reversed_recovery_301114003_trimmed.json.gz.
    f = _run_kickoff_fixture("kickoff_reversed_recovery_301114003_trimmed.json.gz", 301114003)
    row = f.filter(pl.col("id").cast(pl.Utf8) == "3011140033734").row(0, named=True)
    assert row["end.pos_team.id"] == row["start.pos_team.id"] != row["start.team.id"]
    assert abs(row["EPA"] - 0.399) <= 0.02, row["EPA"]


def test_kickoff_touchback_cleaning_pipeline_has_no_kick_spot_column():
    # _kick_spot is carried from the C-H5 block to the EP block, which drops it; the cleaning pipeline
    # stops before the EP block and must drop it itself (review thread on #831).
    proc = NFLPlayProcess(gameId=401671740)
    proc.espn_nfl_pbp(summary=load_summary(401671740))
    plays = proc.run_cleaning_pipeline()["plays"]
    _require(len(plays) > 0, "the cleaning pipeline returned no plays")
    assert [c for c in plays[0] if c in ("_kick_spot", "_kicker_recovers")] == []
    assert {"_kick_spot", "_kicker_recovers"}.isdisjoint(run(401671740).plays.columns)


# nflverse's epa on every touchback kickoff of the captures that nflverse also flags as one
# (fixtures/nflverse/kickoff_touchback_epa.csv, 124 rows, 2006-2025; the README has the provenance).
# MAE before -> after the fix: pre-2016 0.328 -> 0.006, 2016-23 0.0005 -> 0.0005, 2024 0.469 -> 0.255,
# 2025 1.096 -> 0.536. Ceilings are the measured MAE rounded up. From 2024 nflfastR still scores a
# kickoff at its stale touchback 75 (helper_add_ep_wp.R: `ifelse(season < 2016, 80, 75)`) while the
# next snap is at the 30 / 35, so nflverse's own touchback epa is not 0 there: +0.258 (2024, 1,880
# plays) and +0.347 (2025, 610) over each full season, +0.211 / +0.536 on these capture rows. Ours is
# the owner's model (tests above), and each row may sit no further from nflverse than 0 does.
_TB_NFLVERSE_MAE = {"pre-2016": 0.01, "2016-23": 0.002}


@pytest.mark.parametrize("era", list(_TB_ERAS))
def test_kickoff_touchback_epa_agrees_with_nflverse(era):
    nv = pl.read_csv(FIX / "nflverse" / "kickoff_touchback_epa.csv")
    nv = nv.filter(
        {
            "pre-2016": pl.col("season") < 2016,
            "2016-23": pl.col("season").is_between(2016, 2023),
            "2024": pl.col("season") == 2024,
            "2025": pl.col("season") >= 2025,
        }[era]
    )
    ours = pl.concat(
        run(g)
        .plays.select(pl.col("id").cast(pl.Utf8), pl.col("EPA").cast(pl.Float64))
        .with_columns(espn_game_id=pl.lit(g, dtype=pl.Int64))
        for g in nv["espn_game_id"].unique().sort()
    ).with_columns(play_id=pl.col("id").str.slice(9).cast(pl.Int64))
    j = nv.join(ours, on=["espn_game_id", "play_id"], how="inner")
    _require(j.height == nv.height > 0, f"{j.height} of {nv.height} nflverse plays matched")
    err = (pl.col("EPA") - pl.col("epa")).abs()
    if era in _TB_NFLVERSE_MAE:
        assert j.select(err.mean()).item() <= _TB_NFLVERSE_MAE[era]
    else:
        assert _bad(j, err <= pl.col("epa").abs() + 0.01, ["play_id", "EPA", "epa"]) == []
