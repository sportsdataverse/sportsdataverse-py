"""The one ``_query_params`` every flat-API generator shares (A-M23 generator half).

Each generator names its policy in keywords; these cases pin the policies the ten callers use,
so a caller's YAML cannot drift by an edit to the shared reader.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_CODEGEN = Path(__file__).resolve().parents[2] / "tools" / "codegen"


def _common():
    if str(_CODEGEN) not in sys.path:
        sys.path.insert(0, str(_CODEGEN))
    spec = importlib.util.spec_from_file_location("gen_soccer_common", _CODEGEN / "gen_soccer_common.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_OP = {
    "parameters": [
        {"$ref": "#/components/parameters/Lang"},
        {"in": "path", "name": "teamId", "required": True},
        {"in": "query", "name": "teamId", "description": "Filter by team."},
        {
            "in": "query",
            "name": "seasonId",
            "required": True,
            "description": " The season. ",
            "schema": {"type": "integer"},
        },
        {"in": "query", "name": "lang", "schema": {"type": "string"}},
        {"in": "query", "name": "limit", "schema": {"type": "number"}},
    ]
}


def test_required_policies():
    qp = _common()._query_params
    by = {p["query_key"]: p for p in qp(_OP, required="both")}
    assert by["seasonId"]["required"] is True
    assert by["seasonId"]["description"] == "Required. The season."
    assert "required" not in by["lang"]
    flag = {p["query_key"]: p for p in qp(_OP, required="flag")}["seasonId"]
    assert flag["required"] is True and flag["description"] == "The season."
    note = {p["query_key"]: p for p in qp(_OP, required="note")}["seasonId"]
    assert "required" not in note and note["description"] == "Required. The season."
    ignored = {p["query_key"]: p for p in qp(_OP)}["seasonId"]
    assert "required" not in ignored and ignored["description"] == "The season."


def test_names_types_skip_and_clash():
    qp = _common()._query_params
    out = qp(_OP, path_names={"team_id"}, pytypes={"integer": "int", "number": "float"}, skip={"lang"})
    assert [p["name"] for p in out] == ["season_id", "limit"]  # path token wins; lang skipped; $ref ignored
    assert [p["type"] for p in out] == ["int", "float"]
    prefixed = qp(_OP, path_names={"team_id"}, on_clash="prefix", names={"limit": "page_size"})
    assert [p["name"] for p in prefixed] == ["filter_team_id", "season_id", "lang", "page_size"]
    assert {p["type"] for p in prefixed} == {"str"}  # no pytypes map types every param str


def test_description_policies():
    qp = _common()._query_params
    bare = {p["query_key"]: p for p in qp(_OP)}
    assert "description" not in bare["lang"]  # empty descriptions are omitted by default
    kept = {p["query_key"]: p for p in qp(_OP, keep_empty_description=True)}
    assert kept["lang"]["description"] == ""
    assert all("description" not in p for p in qp(_OP, description=None))
    custom = {p["query_key"]: p for p in qp(_OP, description=lambda prm: prm["name"].upper())}
    assert custom["limit"]["description"] == "LIMIT"


@pytest.mark.parametrize("kw", [{"on_clash": "rename"}, {"required": "yes"}])
def test_unknown_policy_raises(kw):
    with pytest.raises(ValueError, match="must be"):
        _common()._query_params(_OP, **kw)
