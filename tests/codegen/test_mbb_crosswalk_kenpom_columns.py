"""The KenPom id columns on the published MBB crosswalks, as SHIPPED.

hoopR-mbb-data's ``kenpom.add_kp_player_id`` inserts ``kp_player_id`` (Utf8)
after ``yahoo_player_name`` on ``mbb_player_crosswalk`` (17 -> 18 columns), and
``add_kp_game_id`` fills ``kp_game_id`` on ``mbb_schedule_crosswalk``. Asserts
against the committed ``loader_schemas.yaml`` and
``manual_column_descriptions.yaml``, not the generator.

The live builder ``mbb_player_crosswalk()`` stays at 17 columns on purpose: the
data repo inserts ``kp_player_id`` by name into its output, so emitting it here
too would duplicate the column there.
"""

from pathlib import Path

import pytest
import yaml

_CODEGEN = Path(__file__).resolve().parents[2] / "tools" / "codegen"


@pytest.fixture(scope="module")
def schemas() -> dict:
    return yaml.safe_load((_CODEGEN / "schemas" / "loader_schemas.yaml").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def manual() -> dict:
    return yaml.safe_load((_CODEGEN / "manual_column_descriptions.yaml").read_text(encoding="utf-8"))


def test_player_crosswalk_declares_kp_player_id_after_yahoo(schemas):
    cols = [(c["name"], c["type"]) for c in schemas["load_mbb_player_crosswalk"]]
    names = [n for n, _ in cols]
    assert len(cols) == 18
    assert names[names.index("yahoo_player_name") + 1] == "kp_player_id"
    assert dict(cols)["kp_player_id"] == "String"


def test_schedule_crosswalk_declares_kp_game_id(schemas):
    assert {c["name"]: c["type"] for c in schemas["load_mbb_schedule_crosswalk"]}["kp_game_id"] == "String"


@pytest.mark.parametrize(
    "loader,col",
    [("load_mbb_player_crosswalk", "kp_player_id"), ("load_mbb_schedule_crosswalk", "kp_game_id")],
)
def test_kenpom_ids_are_described(manual, loader, col):
    assert "KenPom" in (manual.get(loader) or {}).get(col, "")


def test_published_is_the_builder_plus_kp_player_id(schemas):
    from sportsdataverse._common_crosswalk_basketball import PLAYER_ESPN_FOX_COLUMNS

    expected = list(PLAYER_ESPN_FOX_COLUMNS)
    assert len(expected) == 17 and "kp_player_id" not in expected
    expected.insert(expected.index("yahoo_player_name") + 1, "kp_player_id")
    assert [c["name"] for c in schemas["load_mbb_player_crosswalk"]] == expected
