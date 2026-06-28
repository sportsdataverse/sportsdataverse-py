"""Generate nba_stats/wnba_stats endpoint YAML + returns-schemas from the enriched
canonical catalog (Plans 1-2). Idempotent: same catalog -> byte-identical output.

Run: python tools/codegen/gen_nba_stats.py
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import yaml

ROOT = Path(__file__).resolve().parents[2]
CAT = json.loads((ROOT / "tools/codegen/inputs/nba_canonical_catalog.json").read_text(encoding="utf-8"))

STEMS = {
    "nba_stats": {"host": "https://stats.nba.com", "league": "nba", "league_id": "00", "default_league": "00"},
    "wnba_stats": {"host": "https://stats.wnba.com", "league": "wnba", "league_id": "10", "default_league": "10"},
}
# catalog column type -> R-style returns-schema type
_DTYPE = {
    "integer": "integer",
    "number": "numeric",
    "string": "character",
    "boolean": "logical",
    "unknown": "character",
}
# only endpoints that ANSWER for a league are "applicable" (drop "dead"/absent)
_APPLICABLE = ("live", "untested")


def _stats_eps(league_id: str) -> List[dict]:
    return sorted(
        (
            e
            for e in CAT["endpoints"]
            if e["family"] == "stats" and e["league_applicability"].get(league_id) in _APPLICABLE
        ),
        key=lambda e: e["slug"],
    )


def _endpoint_entry(ep: dict, stem: str, default_league: str, parser_name: str) -> Dict[str, Any]:
    extra: List[Dict[str, Any]] = []
    has_league = False
    for p in ep["params"]:
        if p["query_key"] == "LeagueID":
            # normalize the routing param to a clean, uniform python name + pin the stem default
            extra.append({"name": "league_id", "query_key": "LeagueID", "type": "str", "default": default_league})
            has_league = True
        else:
            extra.append({"name": p["name"], "query_key": p["query_key"], "type": "str", "default": p.get("default")})
    return {
        "short": ep["slug"],
        "summary": f"GET /stats/{ep['slug']}",
        "path": f"/stats/{ep['slug']}",
        "extra_params": extra,
        "parser": parser_name,
        "returns_schema": f"native/{stem}/{ep['slug']}",
        "example_args": {"league_id": default_league} if has_league else {},
    }


def _write_yaml(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(obj, fh, sort_keys=True, default_flow_style=False, allow_unicode=True)


def main() -> None:
    for stem, cfg in STEMS.items():
        eps = _stats_eps(cfg["league_id"])
        parser_name = f"parse_{cfg['league']}_stats_result_sets"
        doc = {
            "api": stem,
            "host": cfg["host"],
            "name_pattern": f"{stem}_{{short}}",
            "module": stem,
            "parser_module": f"{cfg['league']}.{stem}_parsers",
            "getter_module": f"sportsdataverse.{cfg['league']}.{stem}_runtime",
            "endpoints": [_endpoint_entry(e, stem, cfg["default_league"], parser_name) for e in eps],
        }
        _write_yaml(ROOT / f"tools/codegen/endpoints/{stem}.yaml", doc)
        for e in eps:
            primary = sorted(e["result_sets"])[0] if e["result_sets"] else None
            cols = e["result_sets"].get(primary, []) if primary else []
            schema = {
                "schema": e["slug"],
                "kind": "dataframe",
                "columns": [
                    {
                        "name": c["name"],
                        "type": _DTYPE.get(c["type"], "character"),
                        "description": c.get("description", ""),
                    }
                    for c in cols
                ],
            }
            _write_yaml(ROOT / f"tools/codegen/schemas/native/{stem}/{e['slug']}.yaml", schema)
        print(f"{stem}: {len(eps)} endpoints")


if __name__ == "__main__":
    main()
