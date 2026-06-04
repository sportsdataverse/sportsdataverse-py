"""Convert OpenAPI specs (fastRhockey NHL, MLB stats) -> flat endpoint YAML.

The output matches the ``FlatApi`` shape consumed by ``spec.load_flat_api`` /
``generate.render_flat_module``. Handles:

* ``operationId`` or path -> clean ``short`` (via ``name_map``, else a path-segment
  fallback),
* ``/now`` + ``/{season}/...`` sibling pairs folded into one endpoint with a
  ``now_variant``,
* ``$ref`` parameters resolved against ``components.parameters`` (MLB uses these),
* path vs query param classification + integer/string typing,
* EDGE path-prefix partitioning (``/v1/edge/`` + ``/v1/cat/edge/``).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional

import yaml

_TYPE_MAP = {"integer": "int", "number": "int", "string": "str", "boolean": "bool"}


def _short_from(op_id: str, path: str, name_map: Dict[str, str]) -> str:
    if op_id and op_id in name_map:
        return name_map[op_id]
    if path in name_map:
        return name_map[path]
    # fallback: last 1-2 non-templated path segments, snake_cased
    segs = [s for s in path.split("/") if s and not s.startswith("{")]
    return re.sub(r"[^a-z0-9]+", "_", "_".join(segs[-2:]).lower()).strip("_")


def partition_edge(paths):
    """Split api-web paths into (web, edge) by the EDGE path prefixes."""
    web, edge = {}, {}
    for p, v in paths.items():
        is_edge = p.startswith("/v1/edge/") or p.startswith("/v1/cat/edge/")
        (edge if is_edge else web)[p] = v
    return web, edge


def _resolve_param(prm: dict, spec: dict) -> Optional[dict]:
    """Resolve a (possibly ``$ref``) parameter object; None if unresolvable."""
    if "$ref" in prm:
        ref = prm["$ref"]
        node = spec
        for part in ref.lstrip("#/").split("/"):
            node = (node or {}).get(part, {})
        prm = node or {}
    return prm if prm.get("name") else None


def convert(spec: dict, name_map: Dict[str, str] | None = None) -> List[dict]:
    name_map = name_map or {}
    paths = spec.get("paths", {})
    # collapse `/now` + `/{...}` sibling pairs into one endpoint with now_variant
    now_pairs = {}
    for p in list(paths):
        if p.endswith("/now"):
            stem = p[: -len("/now")]
            for q in paths:
                if q != p and q.startswith(stem + "/{"):
                    now_pairs[q] = p
    folded_now = set(now_pairs.values())

    out = []
    for path, item in paths.items():
        if path in folded_now:
            continue  # this /now path is folded into its parameterized sibling
        if "get" not in item or path.startswith("/model/") or path in ("/ping", "/api"):
            continue
        op = item["get"]
        short = _short_from(op.get("operationId", ""), path, name_map)
        if not short:
            continue
        path_params, query_params = [], []
        for raw in op.get("parameters", []):
            prm = _resolve_param(raw, spec)
            if prm is None:
                continue
            t = _TYPE_MAP.get((prm.get("schema") or {}).get("type"), "str")
            in_ = prm.get("in")
            entry = {"name": prm["name"], "type": t, "required": prm.get("required", in_ == "path")}
            (path_params if in_ == "path" else query_params).append(entry)
        ep = {"short": short, "summary": op.get("summary", ""), "path": path}
        if path in now_pairs:
            ep["now_variant"] = now_pairs[path]
        if path_params:
            ep["path_params"] = path_params
        if query_params:
            ep["extra_params"] = [{"name": q["name"], "type": q["type"]} for q in query_params]
        out.append(ep)
    return out


def write_api_yaml(
    spec_path: Path,
    out_path: Path,
    *,
    api: str,
    host: str,
    name_pattern: str,
    module: str,
    parser_module: str | None = None,
    name_map: Dict[str, str] | None = None,
    allow_list=None,
    edge: Optional[bool] = None,
    runtime_imports=("_get",),
) -> List[dict]:
    """Convert ``spec_path`` -> ``out_path`` flat-API YAML; return the endpoint list.

    ``edge``: None -> all paths; True -> only EDGE paths; False -> only non-EDGE.
    ``allow_list``: keep only these shorts (records curation).
    """
    spec = yaml.safe_load(Path(spec_path).read_text(encoding="utf-8"))
    if edge is not None:
        web, edge_paths = partition_edge(spec.get("paths", {}))
        spec = {**spec, "paths": edge_paths if edge else web}
    eps = convert(spec, name_map)
    if allow_list is not None:
        allow = set(allow_list)
        eps = [e for e in eps if e["short"] in allow]
    doc = {
        "api": api,
        "host": host,
        "name_pattern": name_pattern,
        "module": module,
        "parser_module": parser_module,
        "runtime_imports": list(runtime_imports),
        "endpoints": eps,
    }
    Path(out_path).write_text(yaml.safe_dump(doc, sort_keys=False, width=100, allow_unicode=True), encoding="utf-8")
    return eps
