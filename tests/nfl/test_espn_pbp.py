from sportsdataverse.nfl.nfl_pbp import NFLPlayProcess
import pandas as pd
import pytest

@pytest.fixture()
def generated_data():
    test = NFLPlayProcess(gameId = 401437777)
    test.espn_nfl_pbp()
    test.run_processing_pipeline()
    yield test

def test_basic_pbp(generated_data):
    assert generated_data.json != None

    generated_data.run_processing_pipeline()
    assert len(generated_data.plays_json) > 0
    assert generated_data.ran_pipeline == True
    assert isinstance(generated_data.plays_json, pd.DataFrame)