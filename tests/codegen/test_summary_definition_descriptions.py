"""The CFB team-summary definitions republished by cfbfastR-cfb-data#139/#140, as SHIPPED.

play_stuffed and opportunity_rate are over rushes (null on the pass split), an
interception counts 0 yards, and a null or constant metric gets no rank. Asserts
the committed ``manual_column_descriptions.yaml`` against the generator for every
column it composes, so fixing only one of them fails here.
"""

from pathlib import Path

import pytest
import yaml

from tools.codegen.gen_cfb_summary_descriptions import describe

_CODEGEN = Path(__file__).resolve().parents[2] / "tools" / "codegen"
_LOADERS = ("load_cfb_team_summaries", "load_cfb_team_summaries_weekly")


@pytest.fixture(scope="module")
def manual() -> dict:
    return yaml.safe_load((_CODEGEN / "manual_column_descriptions.yaml").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def schemas() -> dict:
    return yaml.safe_load((_CODEGEN / "schemas" / "loader_schemas.yaml").read_text(encoding="utf-8"))


@pytest.mark.parametrize("loader", _LOADERS)
def test_generator_composes_every_shipped_text(manual, schemas, loader):
    cols = [c["name"] for c in schemas[loader] if describe(c["name"]) is not None]
    assert len(cols) > 700
    assert {c: manual[loader].get(c) for c in cols} == {c: describe(c) for c in cols}


@pytest.mark.parametrize(
    "col,phrase",
    [
        ("play_stuffed_off", "the share of rushes stopped for 0 or fewer yards"),
        ("opportunity_rate_def", "the share of rushes gaining 4 or more yards"),
        ("play_stuffed_off_pass", "always null"),
        ("opportunity_rate_def_pass_n", "Always 0 here"),
        ("yardsplay_off", "an interception counts as 0 yards"),
        ("available_yards_pct_off", "Null when no available yards are on record"),
        ("expected_turnovers_def", "at least 95% of team-games"),
    ],
)
def test_definition_text(col, phrase):
    assert phrase in describe(col)


def test_every_team_rank_says_when_it_is_unranked(schemas):
    ranks = [c["name"] for c in schemas["load_cfb_team_summaries"] if c["name"].endswith("_rank")]
    assert [c for c in ranks if "unranked" not in (describe(c) or "unranked")] == []
