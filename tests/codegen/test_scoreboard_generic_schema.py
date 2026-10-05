"""The generic scoreboard returns-schema (leagues without a per-league file: soccer
clubs, CFL, MCH/WCH, UFL/XFL, college baseball/softball) must list every column
``parse_scoreboard`` emits on the committed real captures, not a 3-column stub."""

import glob
import json
from pathlib import Path

import pytest
import yaml

from sportsdataverse._common_espn_parsers import parse_scoreboard

ROOT = Path(__file__).resolve().parents[2]
_CAPTURES = sorted(
    glob.glob(str(ROOT / "tests/fixtures/espn/scoreboard_*.json"))
    + glob.glob(str(ROOT / "tests/fixtures/espn/soccer/*/site-v2/scoreboard.json"))
)


def test_captures_found():
    assert len(_CAPTURES) >= 12


@pytest.mark.parametrize("capture", _CAPTURES, ids=lambda p: Path(p).relative_to(ROOT).as_posix())
def test_generic_scoreboard_schema_matches_parser(capture):
    doc = yaml.safe_load((ROOT / "tools/codegen/schemas/scoreboard.yaml").read_text(encoding="utf-8"))
    df = parse_scoreboard(json.loads(Path(capture).read_text(encoding="utf-8")))
    assert [c["name"] for c in doc["columns"]] == df.columns
