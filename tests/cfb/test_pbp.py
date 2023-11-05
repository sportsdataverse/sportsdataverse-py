from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess
import pandas as pd
import pytest
import logging

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

def test_havoc_rate(box_score):
    defense_home = box_score["defensive"][0]
    # print(defense_home)
    pd = defense_home.get("pass_breakups", 0)
    home_int = defense_home.get("Int", 0)
    tfl = defense_home.get("TFL", 0)
    fum = defense_home.get("fumbles", 0)
    plays = defense_home.get("scrimmage_plays", 0)

    assert plays > 0
    assert defense_home["havoc_total"] == (pd + home_int + tfl + fum)
    assert round(defense_home["havoc_total_rate"], 4) == round(((pd + home_int + tfl + fum) / plays), 4)

@pytest.fixture()
def dupe_fsu_play_base():
    test = CFBPlayProcess(gameId = 401411109)
    test.espn_cfb_pbp()
    test.run_processing_pipeline()
    yield test.plays_json

def test_fsu_play_dedupe(dupe_fsu_play_base):
    target_strings = [
        {
            "text": "Jordan Travis pass intercepted Rance Conner return for no gain to the FlaSt 45",
            "down": 3,
            "distance": 9,
            "yardsToEndzone": 74
        },
        {
            "down" : 4,
            "text": "Malik Cunningham pass incomplete to Tyler Hudson",
            "distance": 2,
            "yardsToEndzone": 45
        }
    ]

    regression_cases = [
        {
            "text" : "Alex Mastromanno punt for 52 yds , Braden Smith returns for no gain to the Lvile 37",
            "down" : 4,
            "distance" : 9,
            "yardsToEndzone" : 89
        }
    ]

    for item in target_strings:
        print(f"Checking known test cases for dupes for play_text '{item}'")
        assert len(dupe_fsu_play_base[
            (dupe_fsu_play_base["text"] == item["text"])
            & (dupe_fsu_play_base["start.down"] == item["down"])
            & (dupe_fsu_play_base["start.distance"] == item["distance"])
            & (dupe_fsu_play_base["start.yardsToEndzone"] == item["yardsToEndzone"])
        ]) == 1
        print(f"No dupes for play_text '{item}'")


    for item in regression_cases:
        print(f"Checking non-dupe base cases for dupes for play_text '{item}'")
        assert len(dupe_fsu_play_base[
            (dupe_fsu_play_base["text"] == item["text"])
            & (dupe_fsu_play_base["start.down"] == item["down"])
            & (dupe_fsu_play_base["start.distance"] == item["distance"])
            & (dupe_fsu_play_base["start.yardsToEndzone"] == item["yardsToEndzone"])
        ]) == 1
        print(f"confirmed no dupes for regression case of play_text '{item}'")

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

def test_spread_available():
    test = CFBPlayProcess(gameId = 401525519)
    test.espn_cfb_pbp()
    json_dict_stuff = test.run_processing_pipeline()

    LOGGER.info(json_dict_stuff["pickcenter"])

    assert test.plays_json.loc[0, "gameSpreadAvailable"] == True

    test2 = CFBPlayProcess(gameId = 401520260)
    test2.espn_cfb_pbp()
    json_dict_stuff2 = test2.run_processing_pipeline()

    LOGGER.info(json_dict_stuff2["pickcenter"])

    assert test2.plays_json.loc[0, "gameSpreadAvailable"] == False

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