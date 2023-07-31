import polars as pl
import pytest

from sportsdataverse.nfl.nfl_pbp import NFLPlayProcess


@pytest.fixture()
def generated_nfl_data():
    test = NFLPlayProcess(gameId=401437777)
    test.espn_nfl_pbp()
    test.run_processing_pipeline()
    yield test


def test_basic_nfl_pbp(generated_nfl_data):
    assert generated_nfl_data.json is not None

    generated_nfl_data.run_processing_pipeline()
    assert len(generated_nfl_data.plays_json) > 0
    assert generated_nfl_data.ran_pipeline == True
    assert isinstance(generated_nfl_data.plays_json, pl.DataFrame)
