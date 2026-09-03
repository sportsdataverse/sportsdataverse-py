"""Generate ``endpoints/cfbd.yaml`` from the committed CFBD OpenAPI spec.

The College Football Data API (``api.collegefootballdata.com``) is the largest
single source the Python side never wrapped -- cfbfastR reaches it through 78 R
functions, sdv-py through none. This mints the codegen stem from the spec that
already lives in ``sdv-swagger``, so the wrapper surface tracks the published
contract instead of a hand-typed transcription of it.

Run::

    uv run python tools/codegen/gen_cfbd.py

then the usual ``uv run python tools/codegen/generate.py``.

Two things about the source file are worth knowing before editing this:

* ``cfbd-swagger.json`` is a **swagger-UI config dump**, not a bare OpenAPI
  document -- the spec is nested under ``swaggerDoc``, and the file ends with a
  stray ``;`` that makes ``json.load`` fail outright. Both are handled below.
* Auth is a plain HTTP **bearer** token (``CFBD_API_KEY``), which is why the stem
  sets ``auth: true`` and points at a runtime ``_get`` rather than the shared
  no-auth getter.
"""

from __future__ import annotations

import json
import pathlib
import re
from typing import Any, Dict, List

HERE = pathlib.Path(__file__).resolve().parent
OUT = HERE / "endpoints" / "cfbd.yaml"


def _find_spec() -> pathlib.Path:
    """Locate ``sdv-swagger/cfbd-swagger.json`` by walking up from this file.

    A fixed ``parents[N]`` hop breaks inside a git worktree, where the checkout
    sits one level deeper than a normal clone. Searching upward works from both.
    """
    for base in HERE.parents:
        candidate = base / "sdv-swagger" / "cfbd-swagger.json"
        if candidate.is_file():
            return candidate
    raise FileNotFoundError("cfbd-swagger.json not found - expected a sibling sdv-swagger checkout")


SPEC = _find_spec()

HOST = "https://api.collegefootballdata.com"

#: OpenAPI scalar -> the type names the codegen spec understands.
_TYPES = {"integer": "int", "number": "float", "boolean": "bool", "string": "str"}


def load_spec(path: pathlib.Path = SPEC) -> Dict[str, Any]:
    """The OpenAPI document, unwrapped from the swagger-UI envelope.

    ``json.load`` cannot read this file: it carries a trailing ``;`` and nests the
    real document under ``swaggerDoc``. ``raw_decode`` stops cleanly at the end of
    the first value and ignores what follows.
    """
    raw = path.read_text(encoding="utf-8")
    doc, _ = json.JSONDecoder().raw_decode(raw, 0)
    return doc["swaggerDoc"] if "swaggerDoc" in doc else doc


def short_name(route: str) -> str:
    """``/games/teams`` -> ``games_teams``; the wrapper becomes ``cfbd_games_teams``."""
    return re.sub(r"[^a-z0-9]+", "_", route.strip("/").lower()).strip("_") or "root"


def snake(name: str) -> str:
    """``seasonType`` -> ``season_type`` (the Python-side argument name)."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower().replace("__", "_")


def _param(p: Dict[str, Any]) -> Dict[str, Any]:
    schema = p.get("schema") or {}
    kind = schema.get("type") or "string"
    if kind == "array":  # repeated query params arrive as a comma list
        kind = (schema.get("items") or {}).get("type", "string")
    out: Dict[str, Any] = {
        "name": snake(p["name"]),
        "query_key": p["name"],
        "type": _TYPES.get(kind, "str"),
    }
    desc = (p.get("description") or "").strip().replace("\n", " ")
    # Required params get no default, so the generated signature demands them --
    # the same mechanism kenpom uses. An optional one keeps default: null.
    if p.get("required"):
        out["required"] = True
    else:
        out["default"] = None
    out["description"] = desc or f"``{p['name']}`` query parameter."
    return out


def build() -> Dict[str, Any]:
    spec = load_spec()
    endpoints: List[Dict[str, Any]] = []
    for route, methods in sorted(spec.get("paths", {}).items()):
        op = methods.get("get")
        if op is None:
            continue
        short = short_name(route)
        summary = (op.get("summary") or op.get("description") or route).strip().replace("\n", " ")
        params = [_param(p) for p in op.get("parameters", []) if p.get("in") == "query"]
        endpoints.append(
            {
                "short": short,
                "path": route,
                "summary": f"GET {route} — {summary}",
                "parser": "parse_cfbd_records",
                "returns_schema": f"native/cfbd/{short}",
                "extra_params": params,
            }
        )
    return {
        "api": "cfbd",
        "auth": True,
        "host": HOST,
        "module": "cfbd",
        "name_pattern": "cfbd_{short}",
        "getter_module": "sportsdataverse.cfb.cfbd_runtime",
        "parser_module": "cfb.cfbd_parsers",
        "runtime_imports": ["_get"],
        "endpoints": endpoints,
    }


def main() -> int:
    import yaml

    doc = build()
    OUT.write_text(yaml.safe_dump(doc, sort_keys=True, width=100), encoding="utf-8")
    n_params = sum(len(e["extra_params"]) for e in doc["endpoints"])
    print(f"wrote {OUT.relative_to(HERE.parent.parent)}: {len(doc['endpoints'])} endpoints, {n_params} params")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI
    raise SystemExit(main())
