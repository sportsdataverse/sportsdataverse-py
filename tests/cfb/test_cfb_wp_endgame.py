"""End of regulation and overtime: the regulation WP boosters never saw a game that went to OT.

Real games -- trimmed ESPN summaries copied from ``cfbfastR-cfb-raw/cfb/json/raw/{game_id}.json``
and reduced to ``boxscore``, ``drives``, ``gameInfo``, ``header``, ``pickcenter`` and
``scoringPlays`` -- with the published line injected, so nothing touches the network:

* ``401754554`` Virginia @ Louisville, 2025 (OT). Louisville's tying 50-yard field goal, 1:08 left.
* ``401754623`` Clemson @ Georgia Tech, 2025. Georgia Tech's 55-yard walk-off field goal at 0:00, tied.
* ``401762856`` Kent State @ Akron, 2025 (OT). Akron, tied, 4th and 15 at midfield with 0:05 left.
* ``401754585`` California @ Louisville, 2025 (OT). Louisville, tied, 4th and 12 at 0:01; in overtime
  Cal, down 3, scores the winning touchdown on 4th and 3 from the 3.
* ``401761656`` Marshall @ Appalachian State, 2025. App State, up 2, 4th and 34 at its own 15 with 0:02
  left: the sack ended the game.
* ``401777353`` Indiana vs Ohio State, 2025 Big Ten Championship. Ohio State, down 3, 4th and 1 at the
  Indiana 10 with 2:48 left: the bot said go.

The published values these tests replace are quoted in each test.
"""

from __future__ import annotations

import gzip
import json
from functools import lru_cache
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess
from sportsdataverse.cfb.cfb_wp_overtime import tie_value

FIX = Path(__file__).parent / "fixtures"
#: cfbfastR-cfb-data play_by_play_2025: homeTeamSpread (sign = home favoured), overUnder
ODDS = {
    401754554: (6.5, 60.0),
    401754623: (-3.0, 51.0),
    401762856: (7.5, 49.5),
    401754585: (18.5, 49.0),
    401777353: (3.5, 45.75),
    401761656: (-4.0, 56.5),
}


@lru_cache(maxsize=None)
def _plays(game_id: int) -> pl.DataFrame:
    with gzip.open(FIX / f"summary_{game_id}_trimmed.json.gz", "rt", encoding="utf-8") as fh:
        summary = json.load(fh)
    spread, total = ODDS[game_id]
    proc = CFBPlayProcess(
        gameId=game_id,
        join_participants=False,
        odds_override={
            "gameSpread": abs(spread),
            "overUnder": total,
            "homeFavorite": spread > 0,
            "gameSpreadAvailable": True,
        },
    )
    proc.espn_cfb_pbp(summary=summary)
    proc.run_processing_pipeline()
    return proc.plays_frame.sort("game_play_number")


def _row(game_id: int, text: str) -> dict:
    hit = _plays(game_id).filter(pl.col("text").str.contains(text, literal=True))
    assert hit.height == 1, (game_id, text, hit.height)
    return hit.row(0, named=True)


def test_a_made_field_goal_hands_its_wp_to_the_kickoff():
    # Louisville ties it 24-24 at 1:08 and kicks off. Published: WP 32.0% -> 62.9%, WPA +30.9%
    # -- the kicker scored as a team WITH the ball, tied, 73 seconds left. The kickoff's own
    # board gives Louisville 35.4%.
    plays = _plays(401754554)
    kick = _row(401754554, "Cooper Ranvier 50 yd FG GOOD")
    nxt = plays.filter(pl.col("game_play_number") > kick["game_play_number"]).row(0, named=True)
    assert nxt["type.text"] == "Kickoff"
    assert abs(kick["home_wp_after"] - nxt["home_wp_before"]) < 0.01
    assert 0.0 < kick["wpa"] < 0.10


@pytest.mark.parametrize("game_id", sorted(ODDS))
def test_every_made_field_goal_before_a_kickoff_hands_over(game_id):
    plays = _plays(game_id).with_columns(
        next_type=pl.col("type.text").shift(-1), next_home_wp=pl.col("home_wp_before").shift(-1)
    )
    made = plays.filter((pl.col("type.text") == "Field Goal Good") & (pl.col("next_type") == "Kickoff"))
    gap = (made["home_wp_after"] - made["next_home_wp"]).abs()
    assert gap.max() is None or gap.max() < 0.01, made.select("game_play_number", "home_wp_after", "next_home_wp")


def test_a_walk_off_field_goal_is_not_worse_than_a_punt():
    # Tied, 0:00, 4th and 2 at the Clemson 38. Published: punt 91.7%, go 57.0%, FG 43.8%, rec punt.
    # With no time left a punt is overtime, and a make wins.
    r = _row(401754623, "Aidan Birr 55 Yd Field Goal")
    assert abs(r["punt_wp"] - float(tie_value([r["start.pos_team_spread"]])[0])) < 0.02
    assert r["fg_wp"] > r["punt_wp"]
    assert r["fourth_down_recommendation"] != "punt"


@pytest.mark.parametrize(
    ("game_id", "text"),
    [
        (
            401762856,
            "B.Finley pass incomplete deep left thrown to KENT18",
        ),  # Akron, 0:05, 4th & 15; published punt 91.2%
        (401754585, "thrown to CAL30 QB hurried"),  # Louisville, 0:01, 4th & 12; published punt 92.8%
    ],
)
def test_a_punt_in_a_tie_with_no_time_left_is_overtime(game_id, text):
    r = _row(game_id, text)
    assert r["pos_score_diff_start"] == 0 and r["period"] == 4
    # the punt ends regulation: overtime, worth the tie value of the pregame line
    assert abs(r["punt_wp"] - float(tie_value([r["start.pos_team_spread"]])[0])) < 0.02


def test_a_leader_with_no_time_left_wins_whatever_the_fourth_down_call():
    # App State up 2, 0:02 left. Published: punt 93.5%, go 46.3% -- a failed go handed Marshall
    # the ball at the 15 "with time to score". The clock runs out on the play either way.
    r = _row(401761656, "J.Kohl sacked for loss of 10 yards to the APP05")
    assert r["pos_score_diff_start"] == 2 and r["period"] == 4
    assert r["go_wp"] > 0.99
    assert r["wp_fail"] > 0.99


@pytest.mark.parametrize("game_id", [401754554, 401762856, 401754585])
def test_the_first_snap_of_a_tied_overtime_is_near_a_coin_flip(game_id):
    # Published: 0.957, 0.935, 0.966 -- the booster read overtime as the last snap of a
    # regulation game somebody won. 2004-25 overtime first possessions won 46%.
    first = _plays(game_id).filter((pl.col("period") == 5) & (pl.col("start.down") == 1)).row(0, named=True)
    assert first["pos_score_diff_start"] == 0
    assert 0.3 < first["wp_before"] < 0.7


def test_overtime_fourth_down_counts_what_the_other_team_did():
    # Cal, second possession, down 3: 4th and 3 at the 3. Published: FG 85.2%, go 36.3%, rec FG.
    # A make only ties it and starts another overtime; a touchdown wins it. Cal scored.
    r = _row(401754585, "Jacob De Jesus 3 Yd pass from")
    assert r["period"] == 5 and r["pos_score_diff_start"] == -3
    assert r["fg_wp"] < 0.5
    assert r["go_wp"] > r["fg_wp"]
    assert r["fourth_down_recommendation"] == "go"
    # and the touchdown itself ends it: the winning score is worth the rest of the game
    assert r["wp_after"] > 0.99


def test_overtime_first_possession_field_goal_leaves_the_other_team_its_turn():
    # Louisville, first possession, tied: 4th and 2 at the 7. Published: FG 96.5%, go 95.9%.
    # A made kick puts Virginia on the field from the 25 knowing a field goal ties it and a
    # touchdown wins; 2004-25, the second team trailing by 3 won 65% of the time.
    r = _row(401754554, "Cooper Ranvier 24 yd FG GOOD")
    assert r["period"] == 5 and r["pos_score_diff_start"] == 0
    assert 0.3 < r["wp_before"] < 0.7
    assert r["fg_wp"] < 0.5
    assert r["go_wp"] < 0.6


def test_the_big_ten_championship_hook_keeps_its_call():
    # Ohio State, down 3, 4th and 1 at the Indiana 10, 2:48 left, on the captured line (3.5 / 45.75).
    # Published: go 55.91%, FG 39.87%, go_boost +16.04. A make ties it with 2:42 left, a state
    # that reaches overtime about a third of the time; the old booster, trained without those
    # games, gave Indiana the regulation-only edge of the team with the ball. The make branch
    # rises 41.8% -> 46.5%, go_boost to +10.0, and the call holds. Values pinned as observed.
    r = _row(401777353, "J.Fielding field goal attempt from 27 yards")
    assert r["fourth_down_recommendation"] == "go"
    assert abs(r["go_wp"] - 0.5438) < 0.005
    assert abs(r["fg_wp"] - 0.4439) < 0.005
    assert r["go_boost"] > 9.5


def test_the_overtime_correction_beats_the_booster_on_the_holdout():
    # Gates on the committed card: fitted 2004-2021 (early stopping inside that span), scored on
    # 2022-2025. Thresholds are the values the fit observed, so a refit may only hold or improve.
    from sportsdataverse.cfb.model_cards import load_model_card

    card = load_model_card("wp_ot_reach")
    assert card["training_seasons"] == [2004, 2021] and card["holdout_seasons"] == [2022, 2025]
    m = card["holdout_metrics"]
    for block in ("regulation", "q4", "tied_last_2m", "overtime"):
        assert m[block]["after"]["brier"] < m[block]["before"]["brier"], block
        assert m[block]["after"]["logloss"] < m[block]["before"]["logloss"], block
    assert m["regulation"]["naive_after"]["brier"] < m["regulation"]["naive_before"]["brier"]
    # tied, two minutes or less: 0.771 predicted against 0.648 won before the fix
    assert abs(m["tied_last_2m"]["mean_after"] - m["tied_last_2m"]["won"]) <= 0.005
    assert m["q4"]["after"]["brier"] <= 0.07289
    assert m["overtime"]["after"]["brier"] <= 0.23626
    assert m["overtime"]["after"]["brier"] < m["overtime"]["flat_half"]["brier"]


def test_possession_order_is_inferred_from_the_margin_only_when_unknown():
    from sportsdataverse.cfb.cfb_wp_overtime import infer_second_possession

    got = infer_second_possession([True, None, False, None], [0, -3, 0, 0])
    assert got.tolist() == [True, True, False, False]
    assert infer_second_possession(None, [0, 7]).tolist() == [False, True]
