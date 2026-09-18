import pytest

from sportsdataverse.mbb import espn_mbb_pbp
from tests.conftest import skip_if_no_live

# The only test here fetches a live ESPN summary; gate the module.
pytestmark = skip_if_no_live


@pytest.fixture()
def generated_mbb_data():
    yield espn_mbb_pbp(game_id=401265031)


def test_basic_mbb_pbp(generated_mbb_data):
    assert generated_mbb_data is not None
    assert len(generated_mbb_data.get("plays")) > 0
