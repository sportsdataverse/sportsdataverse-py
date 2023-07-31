import polars as pl
import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess
from sportsdataverse.cfb.cfb_schedule import (
    espn_cfb_calendar,
    espn_cfb_schedule,
    most_recent_cfb_season,
)


@pytest.fixture()
def generated_data():
    test = CFBPlayProcess(gameId=401301025)
    test.espn_cfb_pbp()
    test.run_processing_pipeline()
    yield test


@pytest.fixture()
def box_score(generated_data):
    yield generated_data.create_box_score(pl.DataFrame(generated_data.plays_json, infer_schema_length=400))


def test_basic_pbp(generated_data):
    assert generated_data.json is not None

    generated_data.run_processing_pipeline()
    assert len(generated_data.plays_json) > 0
    assert generated_data.ran_pipeline == True
    assert isinstance(pl.DataFrame(generated_data.plays_json, infer_schema_length=400), pl.DataFrame)


def test_adv_box_score(box_score):
    assert box_score is not None
    assert not set(box_score.keys()).difference(
        {
            "win_pct",
            "pass",
            "team",
            "situational",
            "rush",
            "receiver",
            "defensive",
            "turnover",
            "drives",
        }
    )


def test_havoc_rate(box_score):
    defense_home = box_score["defensive"][0]
    # print(defense_home)
    passes_defended = defense_home.get("pass_breakups", 0)
    home_int = defense_home.get("Int", 0)
    tfl = defense_home.get("TFL", 0)
    fum = defense_home.get("fumbles", 0)
    plays = defense_home.get("scrimmage_plays", 0)

    assert plays > 0
    assert defense_home["havoc_total"] == (passes_defended + home_int + tfl + fum)
    assert round(defense_home["havoc_total_rate"], 4) == round(((passes_defended + home_int + tfl + fum) / plays), 4)


@pytest.fixture()
def dupe_fsu_play_base():
    test = CFBPlayProcess(gameId=401411109)
    test.espn_cfb_pbp()
    test.run_processing_pipeline()
    yield pl.DataFrame(test.plays_json, infer_schema_length=400)


def test_fsu_play_dedupe(dupe_fsu_play_base):
    target_strings = [
        {
            "text": "Jordan Travis pass intercepted Rance Conner return for no gain to the FlaSt 45",
            "down": 3,
            "distance": 9,
            "yardsToEndzone": 74,
        },
        {"down": 4, "text": "Malik Cunningham pass incomplete to Tyler Hudson", "distance": 2, "yardsToEndzone": 45},
    ]

    regression_cases = [
        {
            "text": "Alex Mastromanno punt for 52 yds , Braden Smith returns for no gain to the Lvile 37",
            "down": 4,
            "distance": 9,
            "yardsToEndzone": 89,
        }
    ]

    for item in target_strings:
        print(f"Checking known test cases for dupes for play_text '{item}'")
        assert (
            len(
                dupe_fsu_play_base.filter(
                    (pl.col("text") == item["text"])
                    & (pl.col("start.down") == item["down"])
                    & (pl.col("start.distance") == item["distance"])
                    & (pl.col("start.yardsToEndzone") == item["yardsToEndzone"])
                )
            )
            == 1
        )
        print(f"No dupes for play_text '{item}'")

    for item in regression_cases:
        print(f"Checking non-dupe base cases for dupes for play_text '{item}'")
        assert (
            len(
                dupe_fsu_play_base.filter(
                    (pl.col("text") == item["text"])
                    & (pl.col("start.down") == item["down"])
                    & (pl.col("start.distance") == item["distance"])
                    & (pl.col("start.yardsToEndzone") == item["yardsToEndzone"])
                )
            )
            == 1
        )
        print(f"confirmed no dupes for regression case of play_text '{item}'")


@pytest.fixture()
def iu_play_base():
    test = CFBPlayProcess(gameId=401426563)
    test.espn_cfb_pbp()
    test.run_processing_pipeline()
    yield test


@pytest.fixture()
def dupe_iu_play_base(iu_play_base):
    yield pl.DataFrame(iu_play_base.plays_json, infer_schema_length=400)


def test_iu_play_dedupe(dupe_iu_play_base):
    target_strings = [
        {
            "text": "A. Reed pass,to J. Beljan for 26 yds for a TD, (B. Narveson KICK)",
            "down": 2,
            "distance": 9,
            "yardsToEndzone": 26,
        }
    ]

    elimination_strings = [
        {
            "text": "Austin Reed pass complete to Joey Beljan for 26 yds for a TD",
            "down": 2,
            "distance": 9,
            "yardsToEndzone": 26,
        }
    ]

    for item in target_strings:
        print(f"Checking known test cases for dupes for play_text '{item}'")
        assert (
            len(
                dupe_iu_play_base.filter(
                    (pl.col("text") == item["text"])
                    & (pl.col("start.down") == item["down"])
                    & (pl.col("start.distance") == item["distance"])
                    & (pl.col("start.yardsToEndzone") == item["yardsToEndzone"])
                )
            )
            == 1
        )
        print(f"No dupes for play_text '{item}'")

    for item in elimination_strings:
        print(f"Checking for strings that should have been removed by dupe check for play_text '{item}'")
        assert (
            len(
                dupe_iu_play_base.filter(
                    (pl.col("text") == item["text"])
                    & (pl.col("start.down") == item["down"])
                    & (pl.col("start.distance") == item["distance"])
                    & (pl.col("start.yardsToEndzone") == item["yardsToEndzone"])
                )
            )
            == 0
        )
        print(f"Confirmed no values for play_text '{item}'")


@pytest.fixture()
def iu_play_base_box(iu_play_base):
    yield iu_play_base.create_box_score(pl.DataFrame(iu_play_base.plays_json, infer_schema_length=400))


def test_expected_turnovers(iu_play_base_box):
    defense_home = iu_play_base_box["defensive"][1]
    def_home_team = defense_home.get("def_pos_team", "NA")
    away_pd = iu_play_base_box["turnover"][0].get("pass_breakups", 0)
    away_off_int = iu_play_base_box["turnover"][0].get("Int", 0)
    away_fum = iu_play_base_box["turnover"][0].get("total_fumbles", 0)

    away_exp_xTO = (0.22 * (away_pd + away_off_int)) + (0.5 * away_fum)
    away_actual_xTO = iu_play_base_box["turnover"][0].get("expected_turnovers")
    away_team = iu_play_base_box["turnover"][0].get("pos_team", "NA")

    defense_away = iu_play_base_box["defensive"][0]
    def_away_team = defense_away.get("def_pos_team", "NA")
    home_pd = iu_play_base_box["turnover"][1].get("pass_breakups", 0)
    home_off_int = iu_play_base_box["turnover"][1].get("Int", 0)
    home_fum = iu_play_base_box["turnover"][1].get("total_fumbles", 0)

    home_exp_xTO = (0.22 * (home_pd + home_off_int)) + (0.5 * home_fum)
    home_actual_xTO = iu_play_base_box["turnover"][1].get("expected_turnovers")
    home_team = iu_play_base_box["turnover"][1].get("pos_team", "NA")

    print(
        f"home team: {home_team} vs def {def_away_team} - fum: {home_fum}, int: {home_off_int}, pd: {home_pd} -> xTO: {home_exp_xTO}"
    )
    print(
        f"away off {away_team} vs def {def_home_team} - fum: {away_fum}, int: {away_off_int}, pd: {away_pd} -> xTO: {away_exp_xTO}"
    )
    assert round(away_exp_xTO, 4) == round(away_actual_xTO, 4)
    assert round(home_exp_xTO, 4) == round(home_actual_xTO, 4)


@pytest.fixture()
def calendar_data():
    yield espn_cfb_calendar(season=most_recent_cfb_season(), return_as_pandas=False)


def calendar_data_check(calendar_data):
    assert isinstance(calendar_data, pl.DataFrame)
    assert len(calendar_data) > 0


@pytest.fixture()
def calendar_ondays_data():
    yield espn_cfb_calendar(season=2021, ondays=True, return_as_pandas=False)


def calendar_ondays_data_check(calendar_ondays_data):
    assert isinstance(calendar_ondays_data, pl.DataFrame)
    assert len(calendar_ondays_data) > 0


@pytest.fixture()
def schedule_data():
    yield espn_cfb_schedule(return_as_pandas=False)


def schedule_data_check(schedule_data):
    assert isinstance(schedule_data, pl.DataFrame)
    assert len(schedule_data) > 0


@pytest.fixture()
def schedule_data2():
    yield espn_cfb_schedule(dates=20220901, return_as_pandas=False)


def schedule_data_check2(schedule_data2):
    assert isinstance(schedule_data, pl.DataFrame)
    assert len(schedule_data) > 0


@pytest.fixture()
def week_1_cfb_schedule():
    yield espn_cfb_schedule(dates=2022, week=1, return_as_pandas=False)


def week_1_schedule_check(week_1_cfb_schedule):
    assert isinstance(week_1_cfb_schedule, pl.DataFrame)
    assert len(week_1_cfb_schedule) > 0
