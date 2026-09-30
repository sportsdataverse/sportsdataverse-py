"""The drive-efficiency and strength-faced columns on the CFB team summaries, as SHIPPED.

CFBE-1d adds ten whole-team columns to cfb_team_summaries and
cfb_team_summaries_weekly: points per drive (off / def / margin) with a
``_rank`` each, the two ``_n`` drive counts, and ranks for the two
strength-of-schedule columns. It also corrects the strength-faced text, which
had swapped the unit that faced each set of opponents and called both columns
"higher means a tougher slate".

Asserts against the committed artifacts -- ``manual_column_descriptions.yaml``
and ``schemas/loader_schemas.yaml`` -- and that the generator composes the same
text, so fixing only one of them fails here. Definitions come from
cfbfastR-cfb-data ``team_summaries.py`` (``_drives``, ``_drive_owners``,
``_strength_faced_ranks``) and sdv-py ``cfb_adjusted_epa`` (``adjmodelOff`` is
the opposing offense, ``adjmodelDef`` the EPA per play the opposing defense allows).
"""

from pathlib import Path

import pytest
import yaml

from tools.codegen.gen_cfb_summary_descriptions import describe

_CODEGEN = Path(__file__).resolve().parents[2] / "tools" / "codegen"
_LOADERS = ("load_cfb_team_summaries", "load_cfb_team_summaries_weekly")
_VALUES = ("pts_per_drive_off", "pts_per_drive_def", "pts_per_drive_margin")
DRIVE_EFFICIENCY = {
    **{c: "Float64" for c in _VALUES},
    **{f"{c}_rank": "Float64" for c in _VALUES},
    "pts_per_drive_off_n": "Int64",
    "pts_per_drive_def_n": "Int64",
    "off_strength_faced_rank": "Float64",
    "def_strength_faced_rank": "Float64",
}
_DESCRIBED = (*DRIVE_EFFICIENCY, "off_strength_faced", "def_strength_faced")


@pytest.fixture(scope="module")
def manual() -> dict:
    return yaml.safe_load((_CODEGEN / "manual_column_descriptions.yaml").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def schemas() -> dict:
    return yaml.safe_load((_CODEGEN / "schemas" / "loader_schemas.yaml").read_text(encoding="utf-8"))


@pytest.mark.parametrize("loader", _LOADERS)
def test_declared_with_their_dtypes(schemas, loader):
    declared = {c["name"]: c["type"] for c in schemas[loader]}
    assert len(DRIVE_EFFICIENCY) == 10
    assert {c: declared.get(c) for c in DRIVE_EFFICIENCY} == DRIVE_EFFICIENCY


def test_every_column_is_described_the_same_in_both_loaders(manual):
    missing = [f"{t}.{c}" for t in _LOADERS for c in _DESCRIBED if not (manual.get(t) or {}).get(c)]
    assert not missing, missing
    season, week = (manual[t] for t in _LOADERS)
    assert {c: week[c] for c in _DESCRIBED} == {c: season[c] for c in _DESCRIBED}


@pytest.mark.parametrize("col", _DESCRIBED)
def test_generator_composes_the_shipped_text(manual, col):
    """merge_column_descriptions.py never overwrites, so the yaml is edited by hand too."""
    assert describe(col) == manual["load_cfb_team_summaries"][col]


@pytest.mark.parametrize(
    "col,phrases",
    [
        (
            "off_strength_faced",
            ("Average strength of the opposing offenses the team's defense faced", "Higher means a tougher slate."),
        ),
        (
            "def_strength_faced",
            ("Average strength of the opposing defenses the team's offense faced", "Lower means a tougher slate."),
        ),
        ("off_strength_faced_rank", ("1 = toughest slate", "unranked, not last")),
        ("def_strength_faced_rank", ("1 = toughest slate", "unranked, not last")),
        ("pts_per_drive_off", ("Higher is better.",)),
        ("pts_per_drive_def", ("Lower is better.",)),
        ("pts_per_drive_margin", ("pts_per_drive_off minus pts_per_drive_def", "Higher is better.")),
        ("pts_per_drive_off_rank", ("1 is best (most points per drive)", "unranked, not last")),
        ("pts_per_drive_def_rank", ("1 is best (fewest points allowed per drive)", "unranked, not last")),
        ("pts_per_drive_margin_rank", ("1 = largest margin", "unranked, not last")),
    ],
)
@pytest.mark.parametrize("loader", _LOADERS)
def test_direction_phrases(manual, loader, col, phrases):
    text = manual[loader][col]
    assert [p for p in phrases if p not in text] == [], text


@pytest.mark.parametrize("loader", _LOADERS)
def test_strength_faced_is_not_swapped(manual, loader):
    off, dfn = manual[loader]["off_strength_faced"], manual[loader]["def_strength_faced"]
    assert "opposing offenses" in off and "opposing defenses" not in off
    assert "opposing defenses" in dfn and "opposing offenses" not in dfn
    assert "Higher means a tougher slate" not in dfn


@pytest.mark.parametrize("col", ["pts_per_drive_off", "pts_per_drive_def"])
@pytest.mark.parametrize("loader", _LOADERS)
def test_return_touchdown_caveat(manual, loader, col):
    """A return TD on a drive ESPN labels plain "TD" is credited to the drive's owner (a known ceiling)."""
    text = manual[loader][col]
    assert "return touchdowns are the other team's" not in text
    assert 'labels plain "TD" is credited to the drive\'s owner' in text
    assert "FBS-vs-FBS" in text


@pytest.mark.parametrize(
    "col,text",
    [
        (
            "off_strength_faced",
            "Average strength of the opposing offenses the team's defense faced: the mean over its games of each "
            "opponent's ridge-fitted offensive EPA per play. Higher means a tougher slate. Null when the team has "
            "fewer than two valid games.",
        ),
        (
            "def_strength_faced",
            "Average strength of the opposing defenses the team's offense faced: the mean over its games of the "
            "EPA per play each opponent's defense is fitted to allow. Lower means a tougher slate. Null when the "
            "team has fewer than two valid games.",
        ),
        (
            "pts_per_drive_off_n",
            "Sample size behind pts_per_drive_off: the number of the team's own drives it is computed over. 0 when "
            "there were none, and pts_per_drive_off is then null. Can sit slightly below drives_off, which also "
            "counts drive ids holding only a stray snap.",
        ),
    ],
)
@pytest.mark.parametrize("loader", _LOADERS)
def test_shipped_text(manual, loader, col, text):
    assert manual[loader][col] == text
