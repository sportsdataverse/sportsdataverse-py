from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess
import pandas as pd
import pytest
import numpy as np
import logging
import re
from sportsdataverse.cfb.model_vars import *

LOGGER = logging.getLogger(__name__)
logging.basicConfig()

@pytest.fixture()
def generated_data():
    test = CFBPlayProcess(gameId = 401301025)
    test.espn_cfb_pbp()
    test.run_processing_pipeline()
    yield test

@pytest.fixture()
def box_score(generated_data):
    box = generated_data.create_box_score()
    yield box

def test_basic_pbp(generated_data):
    assert generated_data.json != None

    generated_data.run_processing_pipeline()
    assert len(generated_data.plays_json) > 0
    assert generated_data.ran_pipeline == True
    assert isinstance(generated_data.plays_json, pd.DataFrame)

def test_adv_box_score(box_score):
    assert box_score != None
    assert len(set(box_score.keys()).difference({"win_pct","pass","team","situational","receiver","rush","receiver","defensive","turnover","drives"})) == 0

def test_havoc_rate(generated_data):
    generated_data.run_processing_pipeline()
    box_score = generated_data.create_box_score()


    defense_home = box_score["defensive"][0]
    # print(defense_home)
    pd = defense_home.get("pass_breakups", 0)
    home_int = defense_home.get("def_int", 0)
    tfl = defense_home.get("TFL", 0)
    fum = defense_home.get("fumbles", 0)
    plays = defense_home.get("scrimmage_plays", 0)

    # mask = (generated_data.plays_json.statYardage < 0) & (generated_data.plays_json.penalty_flag == False) & (generated_data.plays_json["start.team.id"] != 2567)
    # LOGGER.info(generated_data.plays_json[mask][["id", "text", "statYardage", "havoc", "start.down", "start.yardsToEndzone", "end.down", "end.yardsToEndzone", "int", "forced_fumble"]].to_json(orient = "records", indent = 2))
    LOGGER.info(generated_data.plays_json[(generated_data.plays_json.havoc == True) & (generated_data.plays_json.penalty_flag == False) & (generated_data.plays_json["start.team.id"] != 2567)][["id", "text", "statYardage", "havoc", "start.down", "start.yardsToEndzone", "end.down", "end.yardsToEndzone", "int", "forced_fumble", "TFL", "TFL_pass", "TFL_rush"]].to_json(orient = "records", indent = 2))
    LOGGER.info({
        "pd": pd,
        "home_int": home_int,
        "tfl": tfl,
        "fum": fum
    })

    assert plays > 0
    assert defense_home["havoc_total"] == (pd + home_int + tfl + fum)
    assert round(defense_home["havoc_total_rate"], 4) == round(((pd + home_int + tfl + fum) / plays), 4)

@pytest.fixture()
def dupe_fsu_play_base():
    test = CFBPlayProcess(gameId = 401411109)
    test.espn_cfb_pbp()
    test.run_processing_pipeline()
    yield test.plays_json

# def test_fsu_play_dedupe(dupe_fsu_play_base):
#     target_strings = [
#         {
#             "text": "Jordan Travis pass intercepted Rance Conner return for no gain to the FlaSt 45",
#             "down": 3,
#             "distance": 9,
#             "yardsToEndzone": 74
#         },
#         {
#             "down" : 4,
#             "text": "Malik Cunningham pass incomplete to Tyler Hudson",
#             "distance": 2,
#             "yardsToEndzone": 45
#         }
#     ]

#     regression_cases = [
#         {
#             "text" : "Alex Mastromanno punt for 52 yds , Braden Smith returns for no gain to the Lvile 37",
#             "down" : 4,
#             "distance" : 9,
#             "yardsToEndzone" : 89
#         }
#     ]

#     for item in target_strings:
#         print(f"Checking known test cases for dupes for play_text '{item}'")
#         assert len(dupe_fsu_play_base[
#             (dupe_fsu_play_base["text"] == item["text"])
#             & (dupe_fsu_play_base["start.down"] == item["down"])
#             & (dupe_fsu_play_base["start.distance"] == item["distance"])
#             & (dupe_fsu_play_base["start.yardsToEndzone"] == item["yardsToEndzone"])
#         ]) == 1
#         print(f"No dupes for play_text '{item}'")


#     for item in regression_cases:
#         print(f"Checking non-dupe base cases for dupes for play_text '{item}'")
#         assert len(dupe_fsu_play_base[
#             (dupe_fsu_play_base["text"] == item["text"])
#             & (dupe_fsu_play_base["start.down"] == item["down"])
#             & (dupe_fsu_play_base["start.distance"] == item["distance"])
#             & (dupe_fsu_play_base["start.yardsToEndzone"] == item["yardsToEndzone"])
#         ]) == 1
#         print(f"confirmed no dupes for regression case of play_text '{item}'")

@pytest.fixture()
def iu_play_base():
    test = CFBPlayProcess(gameId = 401426563)
    test.espn_cfb_pbp()
    test.run_processing_pipeline()
    yield test

@pytest.fixture()
def dupe_iu_play_base(iu_play_base):
    yield iu_play_base.plays_json

def test_iu_play_dedupe(dupe_iu_play_base):
    target_strings = [
        {
            "text": "A. Reed pass,to J. Beljan for 26 yds for a TD, (B. Narveson KICK)",
            "down": 2,
            "distance": 9,
            "yardsToEndzone": 26
        }
    ]

    elimination_strings = [
        {
            "text" : "Austin Reed pass complete to Joey Beljan for 26 yds for a TD",
            "down": 2,
            "distance": 9,
            "yardsToEndzone": 26
        }
    ]

    for item in target_strings:
        print(f"Checking known test cases for dupes for play_text '{item}'")
        assert len(dupe_iu_play_base[
            (dupe_iu_play_base["text"] == item["text"])
            & (dupe_iu_play_base["start.down"] == item["down"])
            & (dupe_iu_play_base["start.distance"] == item["distance"])
            & (dupe_iu_play_base["start.yardsToEndzone"] == item["yardsToEndzone"])
        ]) == 1
        print(f"No dupes for play_text '{item}'")

    for item in elimination_strings:
        print(f"Checking for strings that should have been removed by dupe check for play_text '{item}'")
        assert len(dupe_iu_play_base[
            (dupe_iu_play_base["text"] == item["text"])
            & (dupe_iu_play_base["start.down"] == item["down"])
            & (dupe_iu_play_base["start.distance"] == item["distance"])
            & (dupe_iu_play_base["start.yardsToEndzone"] == item["yardsToEndzone"])
        ]) == 0
        print(f"Confirmed no values for play_text '{item}'")

@pytest.fixture()
def iu_play_base_box(iu_play_base):
    box = iu_play_base.create_box_score()
    yield box

def test_expected_turnovers(iu_play_base_box):
    defense_home = iu_play_base_box["defensive"][1]
    def_home_team = defense_home.get('def_pos_team', 'NA')
    away_pd = iu_play_base_box['turnover'][0].get("pass_breakups", 0)
    away_off_int = iu_play_base_box['turnover'][0].get("Int", 0)
    away_fum = iu_play_base_box['turnover'][0].get("total_fumbles", 0)

    away_exp_xTO = (0.22 * (away_pd + away_off_int)) + (0.5 * away_fum)
    away_actual_xTO = iu_play_base_box['turnover'][0].get('expected_turnovers')
    away_team = iu_play_base_box['turnover'][0].get('pos_team', "NA")

    defense_away = iu_play_base_box["defensive"][0]
    def_away_team = defense_away.get('def_pos_team', 'NA')
    home_pd = iu_play_base_box['turnover'][1].get("pass_breakups", 0)
    home_off_int = iu_play_base_box['turnover'][1].get("Int", 0)
    home_fum = iu_play_base_box['turnover'][1].get("total_fumbles", 0)

    home_exp_xTO = (0.22 * (home_pd + home_off_int)) + (0.5 * home_fum)
    home_actual_xTO = iu_play_base_box['turnover'][1].get('expected_turnovers')
    home_team = iu_play_base_box['turnover'][1].get('pos_team', "NA")

    print(f"home team: {home_team} vs def {def_away_team} - fum: {home_fum}, int: {home_off_int}, pd: {home_pd} -> xTO: {home_exp_xTO}")
    print(f"away off {away_team} vs def {def_home_team} - fum: {away_fum}, int: {away_off_int}, pd: {away_pd} -> xTO: {away_exp_xTO}")
    assert round(away_exp_xTO, 4) == round(away_actual_xTO, 4)
    assert round(home_exp_xTO, 4) == round(home_actual_xTO, 4)


def test_onside_kickoff_recovery():
    test_fsu_23 = CFBPlayProcess(gameId = 401525493)
    test_fsu_23.espn_cfb_pbp()
    test_fsu_23.run_processing_pipeline()

    target_plays_fsu_23 = test_fsu_23.plays_json[
        (test_fsu_23.plays_json["text"] == "Ryan Fitzgerald on-side kick recovered by Florida State at the FSU 49")
    ]

    # winning team kicks onside
    LOGGER.info("---- ONSIDE KICK (FSU/SOMISS 2023) ----")
    LOGGER.info(target_plays_fsu_23.iloc[0]["type.text"])
    LOGGER.info(target_plays_fsu_23.iloc[0]["kickoff_onside"])
    LOGGER.info(target_plays_fsu_23.iloc[0]["change_of_pos_team"])
    LOGGER.info(target_plays_fsu_23.iloc[0]["change_of_poss"])
    LOGGER.info(target_plays_fsu_23.iloc[0]["wp_after"])
    LOGGER.info(target_plays_fsu_23.iloc[0]["wpa"])
    LOGGER.info(target_plays_fsu_23.iloc[0]["pos_score_diff_end"])
    assert float(target_plays_fsu_23.iloc[0]["wp_after"]) < 0.1
    assert float(target_plays_fsu_23.iloc[0]["wpa"]) < 0.9

    test_gatech_15 = CFBPlayProcess(gameId = 400756922)
    test_gatech_15.espn_cfb_pbp()
    test_gatech_15.run_processing_pipeline()

    target_plays_gatech_15 = test_gatech_15.plays_json[
        (test_gatech_15.plays_json["text"] == "Harrison Butker on-side kick recovered by GEORGIA TECH at the NDame 43")
    ]

    # losing team kicks onside
    LOGGER.info("---- ONSIDE KICK (GT/ND 2015) ----")
    LOGGER.info(target_plays_gatech_15.iloc[0]["type.text"])
    LOGGER.info(target_plays_gatech_15.iloc[0]["kickoff_onside"])
    LOGGER.info(target_plays_gatech_15.iloc[0]["change_of_pos_team"])
    LOGGER.info(target_plays_gatech_15.iloc[0]["change_of_poss"])
    LOGGER.info(target_plays_gatech_15.iloc[0]["wp_after"])
    LOGGER.info(target_plays_gatech_15.iloc[0]["wpa"])
    LOGGER.info(target_plays_gatech_15.iloc[0]["pos_score_diff_end"])
    assert float(target_plays_gatech_15.iloc[0]["wp_after"]) > 0.9
    assert float(target_plays_gatech_15.iloc[0]["wpa"]) < 0.1

def test_play_order():
    test = CFBPlayProcess(gameId = 401525825)
    test.espn_cfb_pbp()
    test.run_processing_pipeline()
    
    should_be_first = test.plays_json[
        (test.plays_json["text"] == "Tahj Brooks run for 1 yd to the WYO 6")
        & (test.plays_json["start.down"] == 2)
        & (test.plays_json["start.distance"] == 3)
        & (test.plays_json["start.yardsToEndzone"] == 7)
    ]

    should_be_next = test.plays_json[
        (test.plays_json["text"] == "Tahj Brooks 6 Yd Run (Gino Garcia Kick)")
    ]

    pbp_ot = test.plays_json[
        (test.plays_json["period.number"] == 5)
    ]
    LOGGER.info(pbp_ot[["id", "sequenceNumber", "period", "start.down", "start.distance", "text"]])

    assert int(should_be_first.iloc[0]["sequenceNumber"]) + 1 == int(should_be_next.iloc[0]["sequenceNumber"])
    assert int(should_be_first.iloc[0]["game_play_number"]) + 1 == int(should_be_next.iloc[0]["game_play_number"])

def test_explosive_play_count():
    test = CFBPlayProcess(gameId = 401525500)
    test.espn_cfb_pbp()
    test.run_processing_pipeline()

    box = test.create_box_score()
    
    fsu_expl_total = box['team'][0]['EPA_explosive']
    LOGGER.info(fsu_expl_total)

    fsu_expl_plays = test.plays_json[
        (test.plays_json["pos_team"] == 52)
        & ((test.plays_json["EPA"] >= 1.8))
    ]
    LOGGER.info(fsu_expl_plays[["id", "text", "statYardage", "pass", "rush", "EPA", "EPA_explosive"]])

    fsu_naive_expl_plays = test.plays_json[
        (test.plays_json["pos_team"] == 52)
        & (test.plays_json["statYardage"] >= 15)
        # & (test.plays_json["scrimmage_play"] == True)
    ]
    LOGGER.info(fsu_naive_expl_plays[["id", "text", "statYardage", "pass", "rush", "EPA", "EPA_explosive"]])
    LOGGER.info(len(fsu_naive_expl_plays))

    bc_naive_expl_plays = test.plays_json[
        (test.plays_json["pos_team"] != 52)
        & (test.plays_json["statYardage"] >= 15)
        # & (test.plays_json["scrimmage_play"] == True)
    ]
    LOGGER.info(bc_naive_expl_plays[["id", "text", "statYardage", "pass", "rush", "EPA", "EPA_explosive"]])
    LOGGER.info(len(bc_naive_expl_plays))

    # assert fsu_expl_total == len(fsu_expl_plays)

# def test_spread_available():
#     test = CFBPlayProcess(gameId = 401525519)
#     test.espn_cfb_pbp()
#     json_dict_stuff = test.run_processing_pipeline()

#     # assert that pickcenter is dead for all games
#     assert len(json_dict_stuff["pickcenter"]) == 0
#     assert test.plays_json.loc[0, "gameSpreadAvailable"] == True

def test_def_fumbles_lost():
    test = CFBPlayProcess(gameId = 401525530)
    test.espn_cfb_pbp()
    json_dict_stuff = test.run_processing_pipeline()

    box_score = test.create_box_score()
    LOGGER.info(box_score['turnover'][0])

    fsu_fumbles_lost = box_score['turnover'][0]['fumbles_lost']
    fsu_fumbles_recovered = box_score['turnover'][0]['fumbles_recovered']
    fsu_fumbles_total = box_score['turnover'][0]['total_fumbles']

    fsu_fum_plays = test.plays_json[
        (test.plays_json["pos_team"] == 52)
        & (test.plays_json["fumble_lost"] == True)
    ]
    LOGGER.info(fsu_fum_plays[["pos_team", "text"]]) #, "fumble_lost", "fumble_vec", "fumble_recovered"]])

    assert fsu_fumbles_total == 1
    assert fsu_fumbles_lost == 0
    assert fsu_fumbles_recovered == 1

def test_ou_tul_bad_spread():
    test = CFBPlayProcess(gameId = 401287894)
    test.espn_cfb_pbp()
    json_dict_stuff = test.run_processing_pipeline()

    # LOGGER.info(json_dict_stuff["pickcenter"])

    # assert len(json_dict_stuff["pickcenter"]) == 0
    assert test.plays_json.loc[0, "gameSpreadAvailable"] == True
    assert test.plays_json.loc[0, "homeTeamSpread"] >= 31.0
    assert test.plays_json.loc[0, "homeTeamId"] == 201


def test_bad_wp_after_situations():
    test = CFBPlayProcess(gameId = 401551786) # Ohio St/Mich: 401520434 vs BC/SMU: 401551750
    test.espn_cfb_pbp()
    json_dict_stuff = test.run_processing_pipeline()

    plays = test.plays_json

    plays["lead_play_text"] = plays["text"].shift(-1)

    bad_wpa_play = plays[
        plays["text"].isin([
            "Michigan Penalty, Unsportsmanlike Conduct (Jaylen Harrell) to the MICH 11 for a 1ST down",
            "[NHSG] Kneel down by MCCARTHY, J.J. at MIC9 (team loss of 2), clock 00:00.",
            "Jesse Mirco punt for 43 yds, downed at the MICH 36",
            "Tommy Doman punt for 49 yds, downed at the OSU 2",
            "Ryan Bujcevski punt blocked by TEAM blocked by TEAM Bujcevski, Ryan punt 29 yards to the SMU44, recovered by BOSTONCOLL # at SMU44 (blocked by TEAM).",
            "Kevin Jennings pass incomplete, broken up by #",
            "Jalen Milroe run for 1 yd to the MICH 2"
        ])
    ]

    bad_wpa_play["proper_time_set"] = bad_wpa_play["start.adj_TimeSecsRem"] >= bad_wpa_play["end.adj_TimeSecsRem"]

    search_cols = sorted(list(set(wp_start_columns + wp_end_columns + ["end.ExpScoreDiff", "start.ExpScoreDiff"])))
    LOGGER.info(bad_wpa_play[["id", "text", "lead_play_text", "change_of_poss", "change_of_pos_team", "wp_after_case", "wp_before", "wp_after", "proper_time_set", "game_play_number"] + search_cols].to_json(orient = "records", indent = 2))

    assert bad_wpa_play.proper_time_set.all()

def test_available_yards():
    test = CFBPlayProcess(gameId = 401677179) # Ohio St/Mich: 401520434 vs BC/SMU: 401551750, IU/ND: 401677179
    test.espn_cfb_pbp()
    json_dict_stuff = test.run_processing_pipeline()
    # box = test.create_box_score()

    plays = test.plays_json
    tb_play = plays[
        plays['text'].isin([
            "Riley Leonard run for 1 yd to the ND 21"
        ])
    ]
    assert tb_play.loc[tb_play.index[0], 'drive_start'] == 65
    # LOGGER.info(tb_play.loc[tb_play.index[1], 'drive_st


def test_bugged_pass_yards():
    test = CFBPlayProcess(gameId = 401628456)     # known bugged game - 2024 W1: Idaho vs Oregon
    test.espn_cfb_pbp()
    json_dict_stuff = test.run_processing_pipeline()
    
    plays = test.plays_json
    bad_yards_play = plays[
        ((plays['text'].str.contains(" pass complete ")) & (plays['start.team.id'] == 70)) # Idaho passing yards
        |  ((plays['text'].str.contains(" sacked ")) & (plays['start.team.id'] == 70))
    ]
    LOGGER.info(bad_yards_play[["id", "text", "yds_receiving", "statYardage", "start.yardsToEndzone", "end.yardsToEndzone", "yds_sacked"]].to_json(orient = "records", indent = 2))
    
    box = test.create_box_score()
    LOGGER.info(box['pass'][0])
    LOGGER.info(box['rush'][0])

    assert box['pass'][0]['Yds'] == 357 # make sure a bugged game matches the right total
    assert box['rush'][0]['Yds'] == 95 # rush totals should not have been changed

    # make sure sack yardage is accounted for
    assert list(filter(lambda x: x['passer_player_name'] == "Dillon Gabriel", box['pass']))[0]['Yds'] == (380 - 23) # make sure a bugged game matches the right total


    # known good game - 2024 W1: GAST vs Georgia Tech
    good = CFBPlayProcess(gameId = 401634302)     # known bugged game - 2024 W1: Idaho vs Oregon
    good.espn_cfb_pbp()
    good_json = good.run_processing_pipeline()
    
    good_plays = good.plays_json
    good_yards_play = good_plays[
        (good_plays['text'].str.contains(" pass complete ")) & (good_plays['start.team.id'] == 59) # GT passing yards
    ]
    LOGGER.info(good_yards_play[["id", "text", "yds_receiving", "statYardage", "start.yardsToEndzone", "end.yardsToEndzone"]].to_json(orient = "records", indent = 2))

    good_box = good.create_box_score()
    LOGGER.info(good_box['pass'][1])
    LOGGER.info(good_box['rush'][1])

    assert good_box['pass'][1]['Yds'] == 275 # make sure a non-bugged game matches the right total
    assert good_box['rush'][1]['Yds'] == 61 # rush totals should not have been changed

    # edge case: completed pass, fumble, recovery
    edge = CFBPlayProcess(gameId = 401634169)

    edge.espn_cfb_pbp()
    edge_json = edge.run_processing_pipeline()
    
    edge_plays = edge.plays_json
    edge_yards_play = edge_plays[
        (edge_plays['text'].str.contains("Hudson Card pass complete to Drew Biber for 2 yds fumbled, forced by Maddix Blackwell, recovered by INST Garret Ollendieck G. Ollendieck return for 0 yds"))
    ]
    LOGGER.info(edge_yards_play[["id", "text", "yds_receiving", "statYardage", "start.yardsToEndzone", "end.yardsToEndzone"]].to_json(orient = "records", indent = 2))
    assert edge_yards_play.loc[edge_yards_play.index[0], 'yds_receiving'] == 2


def test_neb_24wk1():
    test = CFBPlayProcess(gameId = 401628454)
    test.espn_cfb_pbp()
    json_dict_stuff = test.run_processing_pipeline()

    box = test.create_box_score()
    LOGGER.info(box['pass'][0])
    assert box['pass'][0]['Yds'] == (238.0 - 6.0)
    # LOGGER.info(box['rush'][0])
    
    # plays = test.plays_json
    # bad_yards_play = plays[
    #     (plays['text'].str.contains(" pass complete ")) & (plays['text'].str.contains('Raiola '))
    # ]
    # LOGGER.info(bad_yards_play[["id", "text", "yds_passing", "yds_receiving", "statYardage", "start.yardsToEndzone", "end.yardsToEndzone", "yds_sacked"]].to_json(orient = "records", indent = 2))
    

# def test_okst_24wk1():
#     test = CFBPlayProcess(gameId = 401634212)
#     test.espn_cfb_pbp()
#     json_dict_stuff = test.run_processing_pipeline()

#     plays = test.plays_json

#     LOGGER.info(
#         plays[
#             (plays["drive.id"] == "4016342128")
#         ]["text"].to_json(orient = "records", indent = 2)
#     )
#     bad_yards_play = plays[
#         (plays['text'].str.contains('Bowman ')) & (plays['pass'] == True)
#     ]
#     # LOGGER.info(bad_yards_play[["start.down", "start.distance", "text", "yds_passing", "yds_receiving", "statYardage", "start.yardsToEndzone", "start.team.id", "end.yardsToEndzone", "end.team.id", "yds_sacked", "downs_turnover", "dropback"]].to_json(orient = "records", indent = 2))
#     # LOGGER.info(f"Bowman dropbacks: {len(bad_yards_play[(bad_yards_play.dropback == True)])}")
#     # LOGGER.info(f"Bowman Q1 dropbacks: {len(bad_yards_play[(bad_yards_play.period == 1)])}")
#     # LOGGER.info(f"Bowman Q2 dropbacks: {len(bad_yards_play[(bad_yards_play.period == 2)])}")
#     # LOGGER.info(f"Bowman Q3 dropbacks: {len(bad_yards_play[(bad_yards_play.period == 3)])}")
#     # LOGGER.info(f"Bowman Q4 dropbacks: {len(bad_yards_play[(bad_yards_play.period == 4)])}")

#     # LOGGER.info(f"Bowman non-dropbacks:")
#     # LOGGER.info(bad_yards_play[(bad_yards_play.dropback == False)][["start.down", "start.distance", "text", "yds_passing", "yds_receiving", "statYardage", "start.yardsToEndzone", "start.team.id", "end.yardsToEndzone", "end.team.id", "yds_sacked", "downs_turnover", "dropback"]].to_json(orient = "records", indent = 2))


#     drive_agg = bad_yards_play.sort_values(by="game_play_number").groupby(by = ['drive.id'], as_index=False, group_keys = False).agg(dropback = ('dropback', sum)).to_json(orient = "records", indent = 2)
#     LOGGER.info(f"Bowman dropbacks by drive: {drive_agg}")

#     box = test.create_box_score()
#     LOGGER.info(box['pass'][0])
#     assert box['pass'][0]['Yds'] == 267.0 # PBP seems to be missing a 6-yd completion? - 01-Sept-2024


def test_lsu_24wk1():
    test = CFBPlayProcess(gameId = 401628334)
    test.espn_cfb_pbp()
    json_dict_stuff = test.run_processing_pipeline()

    plays = test.plays_json
    bad_yards_play = plays[
        plays['text'].isin([
            "LSU Penalty, Unsportsmanlike Conduct (Kyren Lacy) to the LSU 20",
            # "USC Penalty, Unsportsmanlike Conduct (Anthony Lucas) to the 50 yard line",
        ])
    ]
    bad_yards_play.id = bad_yards_play.id.astype(str)
    LOGGER.info("BEFORE:")
    LOGGER.info(bad_yards_play[["penalty_assessed_on_kickoff", "text", "penalty_flag", "wp_before", "EP_start"] + wp_start_columns].to_json(orient = "records", indent = 2))
    LOGGER.info("AFTER:")
    LOGGER.info(bad_yards_play[["penalty_assessed_on_kickoff", "text", "penalty_flag", "wp_after_case", "wp_after", "wpa", "end.ExpScoreDiff_case", "EP_end", "EPA"] + wp_end_columns].to_json(orient = "records", indent = 2))
    assert bad_yards_play.loc[bad_yards_play.index[0], 'end.pos_score_diff'] == 0
    assert bad_yards_play.loc[bad_yards_play.index[0], 'end.yardsToEndzone'] == 75 
    assert bad_yards_play.loc[bad_yards_play.index[0], 'end.down'] == 1
    assert bad_yards_play.loc[bad_yards_play.index[0], 'end.distance'] == 10


def test_kickoff_tb():
    test = CFBPlayProcess(gameId = 401677179)
    test.espn_cfb_pbp()
    json_dict_stuff = test.run_processing_pipeline()

    plays = test.plays_json
    tb_play = plays[
        plays['text'].isin([
            "Eric Goins kickoff for 40 yds fair catch by Ke'Shawn Williams at the IU 7",
        ])
    ]
    assert tb_play.loc[tb_play.index[0], 'kickoff_tb'] == True
    assert tb_play.loc[tb_play.index[0], 'end.yardsToEndzone'] == 75

# def test_last_play_live_game():
#     test = CFBPlayProcess(gameId = 401628334)
#     test.espn_cfb_pbp()
#     json_dict_stuff = test.run_processing_pipeline()

#     plays = test.plays_json
#     bad_yards_play = plays[
#         plays['game_play_number'] == plays['game_play_number'].max()
#     ]
#     bad_yards_play.id = bad_yards_play.id.astype(str)
#     LOGGER.info("BEFORE:")
#     LOGGER.info(bad_yards_play[["text", "penalty_flag", "wp_before", "EP_start"] + wp_start_columns].to_json(orient = "records", indent = 2))
#     LOGGER.info("AFTER:")
#     LOGGER.info(bad_yards_play[["text", "penalty_flag", "wp_after_case", "wp_after", "wpa", "end.ExpScoreDiff_case", "EP_end", "EPA"] + wp_end_columns].to_json(orient = "records", indent = 2))

def test_yards_per_drive():
    test = CFBPlayProcess(gameId = 401757219)
    test.espn_cfb_pbp()
    json_dict_stuff = test.run_processing_pipeline()

    box = test.create_box_score()
    LOGGER.info(box['drives'])
    assert round(box['drives'][1]['plays_per_drive'] * box['drives'][1]['drives']) == 62
    assert round(box['drives'][0]['plays_per_drive'] * box['drives'][0]['drives']) == 70

# # ESPN pulled the PBP for this game, so can't test anything 
# @pytest.mark.parametrize(
#     "play_text,field_name,expected_yards",
#     [
#         ("Aaron Philo pass to Dean Patterson for 6 yds to the GWEB 48", 'yds_receiving', 6),
#         ("Cole Pennington sacked  by Christian Garrett for a loss of 2 yards to the GWEB 37", 'yds_sacked', 2),
#         ("to Anthony Lowe for 1 yd to the GT 0, for a TD, Nate Hampton pass (Charlie Viorel PAT MISSED)", 'yds_receiving', 1),
#         ("to Anthony Lowe for 6 yds to the GWEB 35, Nate Hampton pass", 'yds_receiving', 6),
#         ("to Brett Seither for 21 yds to the GWEB 32 for a 1ST down", 'yds_receiving', 31)
#     ]
# )
# def test_25_pass_receipt_parsing(play_text, field_name, expected_yards):
#     test = CFBPlayProcess(gameId = 401754622)
#     test.espn_cfb_pbp()
#     json_dict_stuff = test.run_processing_pipeline()
#     plays = test.plays_json

#     target_plays = plays[
#         plays['text'].isin([play_text])
#     ]

#     assert target_plays.loc[target_plays.index[0], field_name] == expected_yards

@pytest.mark.parametrize(
    "game_id,play_text,yards_field,expected_yards",
    [
        (401754571, "(14:46) Shotgun #10 H.King pass complete short right to #1 J.Haynes caught at GT27, for 15 yards to the GT40 (#13 G.Bryant III), 1ST DOWN", "yds_passing", 15),
        (401754571, "(14:46) Shotgun #10 H.King pass complete short right to #1 J.Haynes caught at GT27, for 15 yards to the GT40 (#13 G.Bryant III), 1ST DOWN", "yds_receiving", 15),
        (401754571, "(14:17) No Huddle-Shotgun #10 H.King pass complete short right to #4 I.Canion caught at GT46, for 2 yards to the GT42 fumbled by #4 I.Canion at GT46 forced by #16 C.Peal recovered by SU #8 D.Reese at GT42, End Of Play", "yds_receiving", 2),
        (401754571, "(06:15) Shotgun #10 H.King pass incomplete short left to #17 J.Beetham thrown to SU01", "yds_receiving", 0),
        (401754571, "(13:31) Shotgun #10 H.King rush right for 7 yards gain to the SU30, out of bounds at SU30, 1ST DOWN", "yds_rushed", 7),
        (401754571, "(07:16) No Huddle-Shotgun #1 J.Haynes rush left for 4 yards loss to the SU35 (#6 J.Heard Jr.; #3 K.Singleton)", "yds_rushed", -4),
        (401754571, "(15:00) No Huddle-Shotgun #10 R.Collins pass complete deep right to #2 J.Cook II caught at GT37, for 41 yards to the GT34 (#6 R.Shelley), 1ST DOWN", "yds_passing", 41),
        (401754571, "(15:00) No Huddle-Shotgun #10 R.Collins pass complete deep right to #2 J.Cook II caught at GT37, for 41 yards to the GT34 (#6 R.Shelley), 1ST DOWN", "yds_receiving", 41),
        (401754571, "(09:25) No Huddle-Shotgun #10 R.Collins pass complete short left to #2 J.Cook II caught at SU31, for 4 yards to the SU34 (#2 E.Lightsey)", "yds_passing", 4),
        (401754571, "(05:49) Shotgun #10 H.King pass complete short middle to #85 J.Allen caught at SU33, for 19 yards to the SU09 (#0 B.Long Jr.)", "yds_passing", 19),
        (401777353, "(07:37) Shotgun #10 J.Sayin pass complete short left to #4 J.Smith caught at OSU29, for 5 yards loss to the OSU32 (#12 D.Boykin)", "yds_receiving", -5),
        (401778302, "Shotgun #14 M.Cutforth pass complete deep middle to #3 L.Caples caught at WAS06, for 22 yards to the WAS06 (#18 R.Dillard-Allen), 1ST DOWN", "yds_receiving", 22)
    ]
)
def test_25_yardage_detection(game_id: int, play_text: str, yards_field: str, expected_yards: int):
    test = CFBPlayProcess(gameId = game_id)
    test.espn_cfb_pbp()
    test.run_processing_pipeline()

    plays = test.plays_json
    target_plays = plays[
        plays['text'].isin([play_text])
    ]
    LOGGER.info(target_plays.loc[target_plays.index[0], "cleaned_text"])
    LOGGER.info(target_plays.loc[target_plays.index[0], "yds_receiving_case"])
    assert len(target_plays) == 1
    assert target_plays.loc[target_plays.index[0], yards_field] == expected_yards


@pytest.mark.parametrize(
    "game_id, play_text, yds_punted, yds_punt_return, fumble_vec, change_of_poss, end_yardsToEndzone, end_pos_team_id",
    [
        # error case
        (401754571, "(13:37) #47 M.Nichols punt 57 yards to the SU22 #10 D.Kerr return for loss of 11 yards to the SU20 fumbled by #10 D.Kerr at SU20 forced by #17 J.Hamilton recovered by SU #26 T.Haile at SU11, End Of Play", 57, -11, True, True, 89, 183),
        (401754572, "(02:21) #94 D.Joyce punt 47 yards to the STAN15 fair catch by #13 L.Thorpe at STAN15", 47, 0, False, True, 85, 24),
        (401757292, "(08:05) #32 A.Logan punt 39 yards to the JSU10, out of bounds at JSU10", 39, 0, False, True, 90, 55),
        # (401757292, "(05:26) #32 A.Logan punt 45 yards to the JSU19 #1 M.Pettway return 3 yards to the JSU22 (#9 P.Hughes) PENALTY JSU Holding (#80 C.Williams) 10 yards from JSU22 to JSU12", 45, 3, False, True, 88, 55),
        (401754592, "(11:26) #47 M.Nichols punt 37 yards to the BCE01", 37, None, False, True, 99, 103),
        (401754592, "(08:35) #28 S.Florio punt 21 yards to the GT 17 fair catch by #3 E.Rivers at GT 17", 21, 0, False, True, 83, 59),

        # base case
        (401752748, "Grant Chadwick punt for 48 yds , KC Concepcion returns for 14 yds to the TA&M 32", 48, 14, False, True, 68, 245),
    ]
)
def test_errored_punt_yardlines(game_id, play_text, yds_punted, yds_punt_return, fumble_vec, change_of_poss, end_yardsToEndzone, end_pos_team_id):
    test = CFBPlayProcess(gameId = game_id)
    test.espn_cfb_pbp()
    test.run_processing_pipeline()

    plays = test.plays_json
    target_plays = plays[
        plays['text'].isin([play_text])
    ]

    assert len(target_plays) == 1
    assert target_plays.loc[target_plays.index[0], "yds_punted"] == yds_punted
    assert target_plays.loc[target_plays.index[0], "yds_punt_return"] == yds_punt_return
    assert target_plays.loc[target_plays.index[0], "fumble_vec"] == fumble_vec
    assert target_plays.loc[target_plays.index[0], "change_of_poss"] == change_of_poss
    assert target_plays.loc[target_plays.index[0], "end.yardsToEndzone"] == end_yardsToEndzone
    assert target_plays.loc[target_plays.index[0], "end.pos_team.id"] == end_pos_team_id

@pytest.mark.parametrize(
    "game_id, box_type, field_name, player_name",
    [
        # error case
        (401754571, "pass", "passer_player_name", "Haynes King"),
        (401754571, "rush", "rusher_player_name", "Aaron Philo"),
        (401754571, "receiver", "receiver_player_name", "Bailey Stockton"),
        # base case
        (401752748, "pass", "passer_player_name", "Garrett Nussmeier"),
        (401752748, "rush", "rusher_player_name", "Caden Durham"),
        (401752748, "receiver", "receiver_player_name", "Aaron Anderson"),
        (401752765, "pass", "passer_player_name", "John Mateer")
    ]
)
def test_25_weird_format_box_score_names(game_id, box_type, field_name, player_name):
    test = CFBPlayProcess(gameId = game_id)
    test.espn_cfb_pbp()
    test.run_processing_pipeline()

    box = test.create_box_score()

    players = list(map(lambda x: x[field_name], box[box_type]))
    # LOGGER.info(players)

    assert player_name in players
    assert all(["hurried" not in p for p in players]) == True
    assert all(["caught at" not in p for p in players]) == True
    assert all(["thrown" not in p for p in players]) == True
    assert all(["Shotgun" not in p for p in players]) == True
    assert all(["Huddle" not in p for p in players]) == True
    assert all(["#" not in p for p in players]) == True


@pytest.mark.parametrize(
    "game_id, expected_rows, play_id, athlete_id, participant_type",
    [
        (401754594, 151, "40175459415", "4678010", "passer"),
    ]
)
def test_play_participants(game_id, expected_rows, play_id, athlete_id, participant_type):
    test = CFBPlayProcess(gameId = game_id)
    df = test.espn_cfb_play_participants()

    assert len(df) == expected_rows
    assert len(df) == df.play_id.nunique()
    assert "passDefender" not in df.columns
    assert "pass_defender" not in df.columns
    assert "pass_defender_player_id" in df.columns

    target_plays = df[
        df['play_id'].isin([play_id])
    ]

    assert len(target_plays) == 1
    assert target_plays.loc[target_plays.index[0], f"{participant_type}_player_id"] == athlete_id
    assert target_plays.loc[target_plays.index[0], f"{participant_type}_player_name"] == "Kyron Drones"



@pytest.mark.parametrize(
    "game_id, expected_rows",
    [
        (401754594, 69),
    ]
)
def test_game_athletes(game_id, expected_rows):
    test = CFBPlayProcess(gameId = game_id)
    df = test.espn_cfb_athletes()

    assert len(df) == expected_rows
    assert "homeAway" not in df.columns
    assert "headshot" not in df.columns
    assert "headshot.href" not in df.columns
    assert "team_id" in df.columns


@pytest.mark.parametrize(
    "game_id, play_text, end_yardsToEndzone, end_pos_team_id, penalty_declined, penalty_no_play, penalty_1st_conv, change_of_poss",
    [
        # error case
        (401754591, "(11:29) #2 C.Klubnik rush middle for 3 yards loss to the LOU04 fumbled by #2 C.Klubnik at LOU04 recovered by LOU #99 J.Guerad at LOU04, End Of Play PENALTY LOU UNS: Unsportsmanlike Conduct (#21 D.Hutchinson) 2 yards from LOU04 to LOU02", 2, 228, False, False, True, False),

        # base case
        (401754591, "(11:24) Shotgun #22 K.Brown rush right for 18 yards gain to the LOU20 (#6 R.Jones), out of bounds PENALTY LOU Holding (#85 N.Kurisky) 1 yard from LOU02 to LOU01. NO PLAY", 99, 97, False, True, False, False),
    ]
)
def test_25_weird_format_penalty(game_id, play_text, end_yardsToEndzone, end_pos_team_id, penalty_declined, penalty_no_play, penalty_1st_conv, change_of_poss):
    test = CFBPlayProcess(gameId = game_id)
    test.espn_cfb_pbp()
    test.run_processing_pipeline()

    plays = test.plays_json
    target_plays = plays[
        plays['text'].isin([play_text])
    ]

    assert len(target_plays) == 1
    assert target_plays.loc[target_plays.index[0], "start.pos_team.id"] == end_pos_team_id
    assert target_plays.loc[target_plays.index[0], "lead_start_team"] == end_pos_team_id
    assert target_plays.loc[target_plays.index[0], "change_of_poss"] == change_of_poss
    assert target_plays.loc[target_plays.index[0], "change_of_pos_team"] == change_of_poss

    assert target_plays.loc[target_plays.index[0], "end.yardsToEndzone"] == end_yardsToEndzone
    assert target_plays.loc[target_plays.index[0], "penalty_1st_conv"] == penalty_1st_conv
    assert target_plays.loc[target_plays.index[0], "penalty_declined"] == penalty_declined
    assert target_plays.loc[target_plays.index[0], "penalty_no_play"] == penalty_no_play
    assert target_plays.loc[target_plays.index[0], "end.pos_team.id"] == end_pos_team_id


@pytest.mark.parametrize(
    "game_id, play_text, end_yardsToEndzone, end_pos_team_id",
    [
        # error case
        (401754579, "(10:26) No Huddle-Shotgun #1 J.Haynes rush middle for 0 yards to the NCSU02 (#52 C.Wallace)", 2, 59),
        (401754579, "(07:42) Shotgun #10 H.King rush middle for 0 yards to the NCSU02 (#44 B.Cleveland; #1 C.Fordham)", 2, 59),
        (401754579, "(03:32) Shotgun #10 H.King pass incomplete short middle to #85 J.Allen thrown to GT34 QB hurried by #4 T.Thomas PENALTY NCSU Targeting (#4 T.Thomas) 15 yards from GT28 to GT43, 1ST DOWN. NO PLAY", 57, 59),
        (401757292, "(05:21) No Huddle-Shotgun #29 D.Taylor rush middle for 0 yards to the JSU16 (#91 G.Stansbury)", 16, 2393),
        (401677184, "Gunner Stockton pass complete to Dillon Bell for no gain to the ND 42", 58, 87),
        (401762521, "(09:03) No Huddle #22 E.Heidenreich rush right for 9 yards gain to the Army13 (#5 J.Weaver; #14 G.Shields), 1ST DOWN", 13, 2426),

        # base case
        (401754579, "(07:03) Shotgun #0 M.Hosley rush middle for 0 yards to the NCSU02 (#1 C.Fordham; #33 K.Soares, Jr.)", 2, 59),
        (401752748, "Grant Chadwick punt for 48 yds , KC Concepcion returns for 14 yds to the TA&M 32", 68, 245),
        (401752748, "TEAM run for a loss of 11 yards to the TA&M 16 TEAM fumbled, recovered by TA&M Rueben Owens II", 84, 245),
        (401754579, "PENALTY NCSU False Start (#44 C.Hardy) 5 yards from GT06 to GT11. NO PLAY", 11, 152),
        (401778317, "No Huddle-Shotgun #17 E.Grunkemeyer pass complete short left to #87 A.Rappleyea caught at CLE46, for 1 yard to the CLE46, End Of Play, TURNOVER ON DOWNS", 54, 228)
    ]
)
def test_25_weird_format_end_of_play(game_id, play_text, end_yardsToEndzone, end_pos_team_id):
    test = CFBPlayProcess(gameId = game_id)
    test.espn_cfb_pbp()
    test.run_processing_pipeline()

    plays = test.plays_json
    target_plays = plays[
        plays['text'].isin([play_text])
    ]
    # LOGGER.info(target_plays.loc[target_plays.index[0], "cleaned_text"])
    assert len(target_plays) == 1
    assert target_plays.loc[target_plays.index[0], "end.yardsToEndzone"] == end_yardsToEndzone
    assert target_plays.loc[target_plays.index[0], "end.pos_team.id"] == end_pos_team_id


@pytest.mark.parametrize(
    "game_id, play_text, start_pos_team_id",
    [
        # error case
        (401754591, "Timeout Louisville, clock 01:26", 228),

        # base case
        (401754591, "Timeout Louisville, clock 05:36", 97),
        (401752748, "Timeout LSU, clock 13:06", 99),
        (401752748, "Timeout Texas A&M, clock 04:26", 99),
        (401754579, "(07:03) Shotgun #0 M.Hosley rush middle for 0 yards to the NCSU02 (#1 C.Fordham; #33 K.Soares, Jr.)", 59),
    ]
)
def test_25_weird_format_timeouts(game_id, play_text, start_pos_team_id):
    test = CFBPlayProcess(gameId = game_id)
    test.espn_cfb_pbp()
    test.run_processing_pipeline()

    plays = test.plays_json
    target_plays = plays[
        plays['text'].isin([play_text])
    ]
    # LOGGER.info(target_plays.loc[target_plays.index[0], "cleaned_text"])
    assert len(target_plays) == 1
    assert target_plays.loc[target_plays.index[0], "start.pos_team.id"] == start_pos_team_id

@pytest.mark.parametrize(
    "game_id, play_text, clock_minutes, clock_seconds",
    [
        # error case
        (401754579, "(10:26) No Huddle-Shotgun #1 J.Haynes rush middle for 0 yards to the NCSU02 (#52 C.Wallace)", '10', '26'),
        (401754579, "(07:42) Shotgun #10 H.King rush middle for 0 yards to the NCSU02 (#44 B.Cleveland; #1 C.Fordham)", '07', '42'),
        (401778328, "(08:57) Shotgun #11 M.Gronowski pass complete deep right to #81 D.Vonnahme caught at Vandy00, for 21 yards to the Vandy00 TOUCHDOWN, clock 08:50, 1ST DOWN #18 D.Stevens kick attempt good (H: #99 T.Nissen, LS: #45 B.Worrell)", '08', '57'),
        # base case
        (401752748, "Grant Chadwick punt for 48 yds , KC Concepcion returns for 14 yds to the TA&M 32", '12', '47'),
    ]
)
def test_25_weird_format_play_timestamp(game_id, play_text, clock_minutes, clock_seconds):
    test = CFBPlayProcess(gameId = game_id)
    test.espn_cfb_pbp()
    test.run_processing_pipeline()

    plays = test.plays_json
    target_plays = plays[
        plays['text'].isin([play_text])
    ]

    assert len(target_plays) == 1
    assert target_plays.loc[target_plays.index[0], "clock.minutes"] == clock_minutes
    assert target_plays.loc[target_plays.index[0], "clock.seconds"] == clock_seconds


@pytest.mark.parametrize(
    "game_id,play_text,expected_air_yardsToEndzone, expected_air_yards, expected_yards_after_catch",
    [
        ## different other cases
        (401754571, "(14:46) Shotgun #10 H.King pass complete short right to #1 J.Haynes caught at GT27, for 15 yards to the GT40 (#13 G.Bryant III), 1ST DOWN", 73, 2, 13),
        (401754571, "(14:17) No Huddle-Shotgun #10 H.King pass complete short right to #4 I.Canion caught at GT46, for 2 yards to the GT42 fumbled by #4 I.Canion at GT46 forced by #16 C.Peal recovered by SU #8 D.Reese at GT42, End Of Play", 54, 6, -4),
        (401754571, "(06:15) Shotgun #10 H.King pass incomplete short left to #17 J.Beetham thrown to SU01", 1, 2, None),
        (401754571, "(15:00) No Huddle-Shotgun #10 R.Collins pass complete deep right to #2 J.Cook II caught at GT37, for 41 yards to the GT34 (#6 R.Shelley), 1ST DOWN", 37, 38, 3),
        (401754571, "(09:25) No Huddle-Shotgun #10 R.Collins pass complete short left to #2 J.Cook II caught at SU31, for 4 yards to the SU34 (#2 E.Lightsey)", 69, 1, 3),
        (401777353, "(07:37) Shotgun #10 J.Sayin pass complete short left to #4 J.Smith caught at OSU29, for 5 yards loss to the OSU32 (#12 D.Boykin)", 71, -8, 3),

        ## cosine similarity
        (401777353, "(00:46) Shotgun #15 F.Mendoza pass incomplete deep middle to #80 C.Becker thrown to OSU23 PENALTY OSU Pass Interference (#3 L.Styles Jr.) 15 yards from IND25 to IND40, 1ST DOWN. NO PLAY", 23, 52, None),
        (401777353, "(12:54) Shotgun #10 J.Sayin pass complete deep middle to #4 J.Smith caught at IND32, for 52 yards to the IND22 (#1 A.Ferrell), 1ST DOWN, PENALTY IND Personal Foul (#6 M.Kamara) 11 yard from IND22 to IND11, 1ST DOWN", 32, 42, 10),
        
        ## no air yards on rush plays
        (401777353, "(09:50) Shotgun #25 B.Jackson rush middle for 4 yards gain to the OSU29 (#7 L.Moore; #5 D.Ponds)", None, None, None),

        ## extreme cases, cosine similarity should kick in here -- TOO similar
        (401833989, "No Huddle-Shotgun #8 J.Lamson pass complete short middle to #1 C.Long caught at UMT16, for 21 yards to the UMT04 (#5 K.Loud), 1ST DOWN", None, None, None), # 16, 9),

        ## old PBP, no air yards
        (400756962, "Justin Thomas pass complete to Ricky Jeune for 33 yds to the GTech 36 for a 1ST down", None, None, None),
    ]
)
def test_25_air_yards_detection(game_id: int, play_text: str, expected_air_yardsToEndzone: int, expected_air_yards: int, expected_yards_after_catch: int):
    test = CFBPlayProcess(gameId = game_id)
    test.espn_cfb_pbp()
    test.run_processing_pipeline()

    plays = test.plays_json
    target_plays = plays[
        plays['text'].isin([play_text])
    ]
    assert len(target_plays) == 1

    x_play = target_plays.loc[target_plays.index[0], :]
        
    assert x_play["air_yardsToEndzone"] == expected_air_yardsToEndzone
    assert x_play["air_yards"] == expected_air_yards
    assert x_play["yards_after_catch"] == expected_yards_after_catch

