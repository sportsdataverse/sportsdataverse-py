"""The nba_stats / wnba_stats / on3 returns-schemas must name, order and type exactly
the columns their parser emits on every committed real capture of the endpoint.

Regenerate with tools/codegen/gen_nba_stats.py and tools/codegen/gen_on3.py; the
captures under tests/fixtures/{nba_stats,wnba_stats}/endpoints/ and the On3 ones
come from tools/codegen/vendor_captures.py.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from sportsdataverse.cfb.on3_parsers import parse_on3_rdb
from sportsdataverse.nba.nba_stats_parsers import parse_nba_stats_result_sets
from tools.codegen import gen_nba_stats
from tools.codegen.vendor_captures import trim

ROOT = Path(__file__).resolve().parents[2]
_R = {"Int64": "integer", "Float64": "numeric", "Boolean": "logical"}


def _endpoints(stem: str) -> list[str]:
    doc = yaml.safe_load((ROOT / f"tools/codegen/endpoints/{stem}.yaml").read_text(encoding="utf-8"))
    return [e["short"] for e in doc["endpoints"]]


def _schema(stem: str, short: str) -> dict:
    return yaml.safe_load((ROOT / f"tools/codegen/schemas/native/{stem}/{short}.yaml").read_text(encoding="utf-8"))


def _frames(doc: dict) -> dict:
    if doc["kind"] == "frames":
        return {b["section"]: b["columns"] for b in doc["frames"]}
    return {None: doc["columns"]}


def _frames_of(parsed) -> dict:
    return parsed if isinstance(parsed, dict) else {None: parsed}


def _assert_matches(parsed, doc: dict, label: str) -> None:
    got = _frames_of(parsed)
    want = _frames(doc)
    assert list(got) == list(want), f"{label}: result sets {list(got)} != schema {list(want)}"
    for name, df in got.items():
        cols = {c["name"]: c["type"] for c in want[name]}
        assert list(df.columns) == list(cols), f"{label}/{name}: columns differ from schema"
        bad = {
            col: (cols[col], _R.get(str(dt), "character"))
            for col, dt in df.schema.items()
            if df[col].null_count() < df.height and cols[col] != _R.get(str(dt), "character")
        }
        assert not bad, f"{label}/{name}: (schema, parser) {bad}"


# Other committed captures of the same endpoints (pilot + earlier fixtures), checked
# too: a schema must hold for every real capture the repo ships, not just its source.
# Not listed: tests/fixtures/nba_stats/tracking/ (leaguedashptstats measure types
# other than the default -- different columns by design).
_EXTRA = [
    ("nba_stats", "commonteamroster", "tests/fixtures/nba_stats/commonteamroster_1610612747_2023_24.json"),
    ("nba_stats", "leaguegamelog", "tests/fixtures/nba_stats/leaguegamelog_team_2023_24.json"),
    ("nba_stats", "leaguestandingsv3", "tests/fixtures/nba_stats/leaguestandingsv3_2023_24.json"),
    ("nba_stats", "scheduleleaguev2", "tests/fixtures/nba_stats/scheduleleaguev2_2025_26.json"),
    ("nba_stats", "leaguedashplayerstats", "tests/nba/fixtures/cap_leaguedashplayerstats_nba.json"),
    ("nba_stats", "leaguedashplayerstats", "tests/nba/fixtures/cap_leaguedashplayerstats_gleague.json"),
    ("nba_stats", "leaguedashplayerstats", "tests/nba/fixtures/cap_leaguedashplayerstats_summer.json"),
    ("nba_stats", "playercareerstats", "tests/nba/fixtures/cap_playercareerstats_nba.json"),
    ("nba_stats", "scoreboardv2", "tests/nba/fixtures/cap_scoreboardv2_nba.json"),
    ("nba_stats", "scoreboardv3", "tests/nba/fixtures/cap_scoreboardv3_nba.json"),
    ("nba_stats", "leaguedashplayershotlocations", "tests/nba/fixtures/cap_shotlocations_nba.json"),
    ("wnba_stats", "commonteamroster", "tests/fixtures/wnba_stats/commonteamroster_1611661319_2023.json"),
    ("wnba_stats", "scheduleleaguev2", "tests/fixtures/wnba_stats/scheduleleaguev2_2026.json"),
    ("wnba_stats", "leaguedashplayerstats", "tests/nba/fixtures/cap_leaguedashplayerstats_wnba.json"),
    ("wnba_stats", "boxscoresummaryv3", "tests/nba/fixtures/cap_boxscoresummaryv3_wnba.json"),
    ("wnba_stats", "boxscoretraditionalv3", "tests/nba/fixtures/cap_boxscoretraditionalv3_wnba.json"),
    ("wnba_stats", "playbyplayv2", "tests/nba/fixtures/cap_playbyplayv2_wnba.json"),
    ("wnba_stats", "playercareerbycollegerollup", "tests/nba/fixtures/cap_playercareerbycollegerollup_wnba.json"),
]
_STATS_CASES = [
    (stem, short, gen_nba_stats.capture_path(stem, short).relative_to(ROOT).as_posix())
    for stem in gen_nba_stats.STEMS
    for short in _endpoints(stem)
] + _EXTRA


@pytest.mark.parametrize(("stem", "short", "capture"), _STATS_CASES)
def test_stats_schema_matches_parser(stem, short, capture):
    doc = _schema(stem, short)
    parsed = parse_nba_stats_result_sets(json.loads((ROOT / capture).read_text(encoding="utf-8")))
    if "unverified" in doc:  # the parser emits nothing for this capture, so nothing is published
        assert doc["unverified"], short  # the template renders only a non-empty reason
        assert doc["columns"] == [] and not any(df.width for df in _frames_of(parsed).values())
        return
    _assert_matches(parsed, doc, f"{stem}/{short} on {capture}")


@pytest.mark.parametrize("short", _endpoints("on3"))
def test_on3_schema_matches_parser(short):
    doc = _schema("on3", short)
    capture = ROOT / "tests/fixtures/on3" / f"{short}.json"
    df = parse_on3_rdb(json.loads(capture.read_text(encoding="utf-8"))) if capture.exists() else None
    if df is None or df.width == 0:
        # no capture with rows: no table is published, and the schema says why
        assert doc["columns"] == [] and doc.get("unverified"), short
        return
    assert "unverified" not in doc, short
    _assert_matches(df, doc, f"on3/{short}")


def test_trim_keeps_records_that_change_a_dtype():
    rows = [[1, None], [2, None], [3, None], [4, 1.5], [5, None]]
    payload = {"resultSets": [{"name": "S", "headers": ["A", "B"], "rowSet": rows}]}
    small = trim(payload)
    assert small["resultSets"][0]["rowSet"] == [[1, None], [2, None], [4, 1.5]]
    assert parse_nba_stats_result_sets(small).schema == parse_nba_stats_result_sets(payload).schema
