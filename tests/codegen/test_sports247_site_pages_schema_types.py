"""The sports247_site_pages returns-schemas must label each column with the dtype the
parser actually emits (it widens numeric strings to Int64/Float64), checked on every
real capture. Regenerate with tools/codegen/gen_sports247_site_pages.py."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
from tools.codegen import gen_sports247_site_pages as gen  # noqa: E402

from sportsdataverse.cfb.sports247_site_pages_parsers import parse_sports247_site_page  # noqa: E402

_R = {"Int64": "integer", "Float64": "numeric", "Boolean": "logical"}
_ENDPOINTS = {
    e["short"]: e
    for e in yaml.safe_load((ROOT / "tools/codegen/endpoints/sports247_site_pages.yaml").read_text("utf-8"))[
        "endpoints"
    ]
}
_CASES = [(short, stem) for short, stems in gen.FIXTURE_FOR_SHORT.items() for stem in stems]


def test_every_endpoint_has_a_fixture():
    assert set(gen.FIXTURE_FOR_SHORT) == set(_ENDPOINTS)


@pytest.mark.parametrize(("short", "stem"), _CASES)
def test_parser_types_match_schema(short, stem):
    schema_path = ROOT / "tools/codegen/schemas" / f"{_ENDPOINTS[short]['returns_schema']}.yaml"
    schema = {c["name"]: c["type"] for c in yaml.safe_load(schema_path.read_text("utf-8"))["columns"]}
    df = parse_sports247_site_page(json.loads((gen.FIXTURE_DIR / f"{stem}.json").read_text("utf-8")))
    bad = {}
    for col, dt in df.schema.items():
        if df[col].null_count() == df.height:
            continue  # all-null: no type signal
        got = _R.get(str(dt), "character")
        # Whole-number floats parse as Int64 on some captures, so a "numeric" column
        # legitimately shows up as integer; anything else must match exactly.
        if schema.get(col) != got and not (schema.get(col) == "numeric" and got == "integer"):
            bad[col] = (schema.get(col), got)
    assert not bad, f"{short}/{stem}: (schema, parser) {bad}"
