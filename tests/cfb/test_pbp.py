from sportsdataverse.cfb import CFBPlayProcess
import pandas as pd
import pytest

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
    print(defense_home)
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
def dupe_iu_play_base():
    test = CFBPlayProcess(gameId = 401426563)
    test.espn_cfb_pbp()
    test.run_processing_pipeline()
    yield test.plays_json

def test_iu_play_dedupe(dupe_iu_play_base):
    target_strings = [
        {
            "text": "Austin Reed pass complete to Joey Beljan for 26 yds for a TD (Brayden Narveson KICK)",
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