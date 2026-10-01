"""The F5 cohort-percentile columns on the CFB summaries loaders, as SHIPPED.

cfbfastR-cfb-data #126 adds ``<m>_pos_pct`` (cohort ``position_group``, a new
column) beside every player ``_pct``, and ``<m>_conf_pct`` (cohort ``conference``,
FBS Independents excluded) beside every team ``_rank``. Asserts against the
committed ``manual_column_descriptions.yaml`` and ``schemas/loader_schemas.yaml``,
and that the generators compose the same text, so fixing only one fails here.
"""

from pathlib import Path

import pytest
import yaml

from tools.codegen.gen_cfb_player_percentile_descriptions import POSITION_GROUP_DESC, pct_desc
from tools.codegen.gen_cfb_summary_descriptions import describe

_CODEGEN = Path(__file__).resolve().parents[2] / "tools" / "codegen"
_PLAYERS = ("load_cfb_passing", "load_cfb_rushing", "load_cfb_receiving")
_TEAMS = ("load_cfb_team_summaries", "load_cfb_team_summaries_weekly")

_HEAD = (
    "where 100 is best. Direction is already encoded in the matching rank, so a "
    "lower-is-better metric still scores 100 at its best. Null when the metric is null, "
)
_TAIL = " Rows with a null metric are excluded from the denominator."
EPAPLAY_POS_PCT = (
    "Percentile (0-100) of EPA generated per play among rushers clearing the leaderboard "
    "minimum of 6.25 plays per team game at the same position group (position_group), "
    f"{_HEAD}when position_group is null, or when fewer than 10 qualifiers in the group "
    f"have the metric.{_TAIL}"
)
EPAPLAY_OFF_CONF_PCT = (
    "Percentile (0-100) of EPA per play with the team on offense among teams in the same "
    f"conference, {_HEAD}for an FBS Independent (not a conference), or when fewer than 5 "
    f"teams in the conference have the metric.{_TAIL}"
)


@pytest.fixture(scope="module")
def manual() -> dict:
    return yaml.safe_load((_CODEGEN / "manual_column_descriptions.yaml").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def schemas() -> dict:
    raw = yaml.safe_load((_CODEGEN / "schemas" / "loader_schemas.yaml").read_text(encoding="utf-8"))
    return {t: {c["name"]: c["type"] for c in raw[t]} for t in (*_PLAYERS, *_TEAMS)}


@pytest.mark.parametrize("loader", _PLAYERS)
def test_every_player_pct_has_a_declared_pos_pct(schemas, loader):
    declared = schemas[loader]
    pcts = [c for c in declared if c.endswith("_pct") and not c.endswith("_pos_pct")]
    assert pcts
    assert {c: declared.get(c[: -len("_pct")] + "_pos_pct") for c in pcts} == dict.fromkeys(pcts, "Float64")
    assert declared["position_group"] == "String"


@pytest.mark.parametrize("loader", _TEAMS)
def test_every_team_rank_has_a_declared_conf_pct(schemas, loader):
    declared = schemas[loader]
    ranks = [c for c in declared if c.endswith("_rank")]
    assert len(ranks) == 204
    assert {c: declared.get(c[: -len("_rank")] + "_conf_pct") for c in ranks} == dict.fromkeys(ranks, "Float64")


def test_every_cohort_column_is_described(manual, schemas):
    missing = [
        f"{t}.{c}"
        for t, cols in schemas.items()
        for c in cols
        if (c.endswith(("_pos_pct", "_conf_pct")) or c == "position_group") and not (manual.get(t) or {}).get(c)
    ]
    assert not missing, missing


def test_shipped_cohort_text(manual):
    assert manual["load_cfb_rushing"]["EPAplay_pos_pct"] == EPAPLAY_POS_PCT
    assert manual["load_cfb_team_summaries"]["EPAplay_off_conf_pct"] == EPAPLAY_OFF_CONF_PCT
    assert manual["load_cfb_team_summaries_weekly"]["EPAplay_off_conf_pct"] == EPAPLAY_OFF_CONF_PCT


def test_generators_compose_the_shipped_text():
    pop = "rushers clearing the leaderboard minimum of 6.25 plays per team game"
    assert pct_desc("EPA generated per play", pop, cohort="position_group") == EPAPLAY_POS_PCT
    assert describe("EPAplay_off_conf_pct") == EPAPLAY_OFF_CONF_PCT


def test_the_plain_pct_text_is_unchanged():
    assert pct_desc("EPA per play", "passers").startswith("Percentile (0-100) of EPA per play among passers, where")
    assert pct_desc("EPA per play", "passers").endswith(
        "Null when the metric is null, and null rows are excluded from the denominator."
    )


@pytest.mark.parametrize("loader", _PLAYERS)
def test_position_group_text(manual, loader):
    assert manual[loader]["position_group"] == POSITION_GROUP_DESC
    for group in ("QB;", "RB (RB and FB);", "WR;", "TE;", "other"):
        assert group in POSITION_GROUP_DESC
