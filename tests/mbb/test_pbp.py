import pytest

from sportsdataverse.mbb import espn_mbb_pbp


@pytest.fixture()
def generated_data():
    test = espn_mbb_pbp(game_id=401265031)
    yield test


def test_basic_pbp(generated_data):
    assert generated_data != None
    assert len(generated_data.get("plays")) > 0
