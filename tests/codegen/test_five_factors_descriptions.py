"""The Five Factors columns on the CFB team summaries, as SHIPPED.

cfbfastR-cfb-data#103 adds 18 whole-team columns to cfb_team_summaries and
cfb_team_summaries_weekly. Asserts against the committed artifacts --
``manual_column_descriptions.yaml`` and ``schemas/loader_schemas.yaml`` -- not
the generator, so a missing or wrong description fails here. Definitions come
from cfbfastR-cfb-data ``team_summaries.py`` (``_drives``, ``_drive_owners``,
``_summarize_team``) and ``summaries_input.game_giveaways``.
"""

from pathlib import Path

import pytest
import yaml

_CODEGEN = Path(__file__).resolve().parents[2] / "tools" / "codegen"
_LOADERS = ("load_cfb_team_summaries", "load_cfb_team_summaries_weekly")
_VALUES = (
    "explosive_margin",
    "pts_per_opp_off",
    "pts_per_opp_def",
    "pts_per_opp_margin",
    "turnovers_off",
    "turnovers_def",
    "turnover_margin",
)
_COUNTS = ("pts_per_opp_off_n", "pts_per_opp_def_n", "turnovers_off_n", "turnovers_def_n")
FIVE_FACTORS = {
    **{c: "Float64" for c in _VALUES},
    **{f"{c}_rank": "Float64" for c in _VALUES},
    **{c: "Int64" for c in _COUNTS},
}


@pytest.fixture(scope="module")
def manual() -> dict:
    return yaml.safe_load((_CODEGEN / "manual_column_descriptions.yaml").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def schemas() -> dict:
    return yaml.safe_load((_CODEGEN / "schemas" / "loader_schemas.yaml").read_text(encoding="utf-8"))


@pytest.mark.parametrize("loader", _LOADERS)
def test_declared_with_their_dtypes(schemas, loader):
    declared = {c["name"]: c["type"] for c in schemas[loader]}
    assert len(FIVE_FACTORS) == 18
    assert {c: declared.get(c) for c in FIVE_FACTORS} == FIVE_FACTORS


def test_every_column_is_described_the_same_in_both_loaders(manual):
    missing = [f"{t}.{c}" for t in _LOADERS for c in FIVE_FACTORS if not (manual.get(t) or {}).get(c)]
    assert not missing, missing
    season, week = (manual[t] for t in _LOADERS)
    assert {c: week[c] for c in FIVE_FACTORS} == {c: season[c] for c in FIVE_FACTORS}


@pytest.mark.parametrize(
    "col,text",
    [
        (
            "pts_per_opp_off",
            "Points per scoring opportunity. A scoring opportunity is a drive with a run or pass snap at or "
            "inside the opponent 40, charged only to the drive's owner (ESPN's drive team); it scores its ESPN "
            "drive result, 7 for a touchdown, 3 for a field goal and 0 otherwise. Counted on the team's own "
            "drives. Null when the team had no scoring opportunity.",
        ),
        (
            "pts_per_opp_def_rank",
            "National rank of pts_per_opp_def, where 1 is best (fewest points allowed per opportunity). "
            "Null when pts_per_opp_def is null: unranked, not last.",
        ),
        (
            "turnover_margin",
            "Turnover margin per game: turnovers_def minus turnovers_off (takeaways minus giveaways). Higher is "
            "better. Spelled singular; there is no turnovers_margin column.",
        ),
        (
            "turnovers_off",
            "Giveaways per game: interceptions and lost fumbles on every play, special teams included (a muffed "
            "punt counts against the return team). Lower is better.",
        ),
        (
            "pts_per_opp_off_n",
            "Sample size behind pts_per_opp_off: the number of scoring opportunities on the team's own drives. "
            "0 when there were none, and pts_per_opp_off is then null.",
        ),
        ("turnovers_def_n", "Sample size behind turnovers_def: the number of games it is computed over."),
    ],
)
@pytest.mark.parametrize("loader", _LOADERS)
def test_shipped_text(manual, loader, col, text):
    assert manual[loader][col] == text
