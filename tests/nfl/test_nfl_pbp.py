import polars as pl
import pytest

from sportsdataverse.nfl.nfl_pbp import NFLPlayProcess


@pytest.fixture()
def generated_nfl_data():
    test = NFLPlayProcess(gameId=401220403)
    test.espn_nfl_pbp()
    test.run_processing_pipeline()
    yield test


@pytest.fixture()
def nfl_box_score(generated_nfl_data):
    yield generated_nfl_data.create_box_score(pl.DataFrame(generated_nfl_data.plays_json, infer_schema_length=400))


def test_basic_nfl_pbp(generated_nfl_data):
    assert generated_nfl_data.json is not None

    generated_nfl_data.run_processing_pipeline()
    assert len(generated_nfl_data.plays_json) > 0
    assert generated_nfl_data.ran_pipeline == True
    assert isinstance(pl.DataFrame(generated_nfl_data.plays_json, infer_schema_length=400), pl.DataFrame)


def test_nfl_adv_box_score(nfl_box_score):
    assert nfl_box_score is not None
    assert not set(nfl_box_score.keys()).difference(
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


def test_havoc_rate(nfl_box_score):
    defense_home = nfl_box_score["defensive"][0]
    # print(defense_home)
    passes_defended = defense_home.get("pass_breakups", 0)
    home_int = defense_home.get("Int", 0)
    tfl = defense_home.get("TFL", 0)
    fum = defense_home.get("fumbles", 0)
    plays = defense_home.get("scrimmage_plays", 0)

    assert plays > 0
    assert defense_home["havoc_total"] == (passes_defended + home_int + tfl + fum)
    assert round(defense_home["havoc_total_rate"], 4) == round(((passes_defended + home_int + tfl + fum) / plays), 4)
