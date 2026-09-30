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
* ``401761665`` UL Monroe @ Louisiana, 2025 (OT). ULM, first, is intercepted; Louisiana, second and still
  tied, kicks a 19-yard field goal on fourth down to win.
* ``401762461`` North Texas @ Western Michigan, 2025 (OT). The period opens with a row ESPN files as
  "Timeout Western Michigan" under North Texas; Western Michigan has the first possession.
* ``401777353`` Indiana vs Ohio State, 2025 Big Ten Championship. Ohio State, down 3, 4th and 1 at the
  Indiana 10 with 2:48 left: the bot said go.

The published values these tests replace are quoted in each test.
"""

from __future__ import annotations

import gzip
import json
from functools import lru_cache
from pathlib import Path

import numpy as np
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
    401761665: (11.0, 47.5),
    401762461: (-12.25, 56.25),
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


def test_the_second_team_tied_in_overtime_needs_only_a_score():
    # Louisiana, second, still tied after ULM's interception. Any score wins, so its first snap is
    # worth ~0.9 and the chip-shot field goal is the call. Read as the FIRST possession (what the
    # margin alone implies) a make would leave ULM its turn and be worth ~0.35.
    plays = _plays(401761665)
    first_snap = _row(401761665, "W.Howard rush right for 20 yards gain to the ULM05")
    assert first_snap["pos_score_diff_start"] == 0 and first_snap["period"] == 5
    assert first_snap["wp_before"] > 0.8
    kick = _row(401761665, "T.Sterner field goal attempt from 19 yards GOOD")
    assert kick["start.down"] == 4 and kick["pos_score_diff_start"] == 0
    assert kick["fourth_down_recommendation"] == "field_goal"
    assert kick["fg_wp"] > 0.9
    assert plays.filter(pl.col("period") >= 5)["wp_before"].is_not_null().all()


def test_a_timeout_row_does_not_decide_who_had_the_ball_first():
    # The first overtime row is "Timeout Western Michigan", filed under North Texas. Western
    # Michigan has the first possession, tied: a coin flip, not the second team's ~0.9.
    r = _row(401762461, "Jalen Buckley run for a loss of 5 yards to the UNT 30")
    assert r["period"] == 5 and r["pos_score_diff_start"] == 0
    assert 0.3 < r["wp_before"] < 0.7


def test_an_overtime_touchdown_with_its_try_on_the_row_hands_over():
    # Since 2014 ESPN files the try on the touchdown row. Akron's first-snap touchdown and kick
    # (0 -> +7) ends its possession at +7; Kent State's first snap is the other side of it.
    plays = _plays(401762856)
    td = _row(401762856, "D.DeShields pass complete deep right to #13 A.Banks caught at AKRON02")
    nxt = plays.filter(pl.col("game_play_number") > td["game_play_number"]).row(0, named=True)
    assert td["end.pos_score_diff"] - td["pos_score_diff_start"] == 7
    assert nxt["start.pos_team.id"] != td["start.pos_team.id"]
    assert abs(td["wp_after"] - (1 - nxt["wp_before"])) < 0.005


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


HOLDOUT = FIX / "wp_overtime"


def _logloss(p, y):
    p = np.clip(np.asarray(p, dtype=float), 1e-6, 1 - 1e-6)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def _brier(p, y):
    return float(np.mean((np.asarray(p, dtype=float) - y) ** 2))


def test_tied_late_holdout_rows_are_calibrated_through_the_runtime():
    # 2022-25 holdout (see fixtures/wp_overtime/README.md): Q4 scrimmage snaps, tied, two minutes
    # or less. The booster gave the team with the ball 0.771; it won 0.648.
    from sportsdataverse.cfb.cfb_wp_overtime import adjust_wp
    from sportsdataverse.cfb.model_vars import wp_final_names, wp_start_columns

    f = pl.read_parquet(HOLDOUT / "tied_last_2m_2022_2025.parquet")
    assert f.height >= 1300
    X = f.select([pl.col(src).alias(name) for src, name in zip(wp_start_columns, wp_final_names)])
    y = f["won"].cast(pl.Float64).to_numpy()
    before = f["wp_before"].to_numpy()
    after = adjust_wp(before, X)
    assert abs(after.mean() - y.mean()) < 0.01
    assert before.mean() - y.mean() > 0.10  # the fixture still shows the failure it guards
    assert _brier(after, y) < _brier(before, y) - 0.02


def test_overtime_holdout_rows_beat_a_coin_flip_through_the_runtime():
    # 2022-25 overtime scrimmage snaps with the possession order the pbp computes.
    from sportsdataverse.cfb.cfb_wp_overtime import ot_live_wp, tie_value as tv

    f = pl.read_parquet(HOLDOUT / "overtime_2022_2025.parquet")
    assert f.height >= 1300
    y = f["won"].cast(pl.Float64).to_numpy()
    p = ot_live_wp(
        f["pos_score_diff_start"].to_numpy(),
        f["start.down"].to_numpy(),
        f["start.distance"].to_numpy(),
        f["start.yardsToEndzone"].to_numpy(),
        tv(f["start.pos_team_spread"].to_numpy()),
        f["ot_second"].to_numpy(),
    )
    flat = np.full(len(y), 0.5)
    assert _brier(p, y) < _brier(flat, y) - 0.01
    assert _logloss(p, y) < _logloss(flat, y)
    assert _brier(f["wp_before"].to_numpy(), y) > 0.35  # the booster's overtime, for scale
    # the level of the commonest state: a tied snap
    tied = f["pos_score_diff_start"].to_numpy() == 0
    assert abs(p[tied].mean() - y[tied].mean()) < 0.06


def test_the_card_pins_the_boosters_it_corrects():
    # q is P(reach OT) layered on boosters that never saw an OT game. A retrained booster --
    # above all one trained WITH overtime games -- must come with a refit, or this goes red.
    import hashlib

    from sportsdataverse.cfb.model_cards import _MODEL_DIR, load_model_card

    pins = load_model_card("wp_ot_reach")["corrects"]
    assert set(pins) == {"wp_spread.ubj", "wp_naive.ubj"}
    for name, sha in pins.items():
        assert hashlib.sha256((_MODEL_DIR / name).read_bytes()).hexdigest() == sha, name


def test_the_card_records_a_disjoint_holdout_that_the_fix_improves():
    from sportsdataverse.cfb.model_cards import load_model_card

    card = load_model_card("wp_ot_reach")
    assert card["training_seasons"] == [2004, 2021] and card["holdout_seasons"] == [2022, 2025]
    assert card["early_stopping"]["valid_seasons"][1] <= card["training_seasons"][1]
    m = card["holdout_metrics"]
    for block in ("regulation", "q4"):
        # game-clustered bootstrap: the Brier change is an improvement, not noise
        assert m[block]["brier_delta"]["ci95"][1] < 0, block
    assert m["overtime"]["after"]["logloss"] < m["overtime"]["flat_half"]["logloss"]


def test_possession_order_is_inferred_from_the_margin_only_when_unknown():
    from sportsdataverse.cfb.cfb_wp_overtime import infer_second_possession

    got = infer_second_possession([True, None, False, None], [0, -3, 0, 0])
    assert got.tolist() == [True, True, False, False]
    assert infer_second_possession(None, [0, 7]).tolist() == [False, True]
