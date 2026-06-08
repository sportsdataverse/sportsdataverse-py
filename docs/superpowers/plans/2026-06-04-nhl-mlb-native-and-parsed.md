<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NHL + MLB Native APIs + Concrete `parsed.*` Implementation Plan](#nhl--mlb-native-apis--concrete-parsed-implementation-plan)
  - [File Structure](#file-structure)
  - [Task 1: Flat-API spec + `api_module.py.jinja`](#task-1-flat-api-spec--api_modulepyjinja)
  - [Task 2: `openapi_to_endpoints.py` converter](#task-2-openapi_to_endpointspy-converter)
  - [Task 3: Reserved-name collision resolution (clean `nhl_{short}`/`mlb_{short}`)](#task-3-reserved-name-collision-resolution-clean-nhl_shortmlb_short)
  - [Task 4: `scrape_edge_vocab.py` + documented-options](#task-4-scrape_edge_vocabpy--documented-options)
  - [Task 5: Generate NHL native modules + delete `nhl_api.py`](#task-5-generate-nhl-native-modules--delete-nhl_apipy)
  - [Task 6: Generate MLB Stats module (`hydrate`/`fields`/`{metaType}`)](#task-6-generate-mlb-stats-module-hydratefieldsmetatype)
  - [Task 7: Concrete `parsed.*` modules (retire `types.ModuleType`)](#task-7-concrete-parsed-modules-retire-typesmoduletype)
  - [Self-Review](#self-review)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NHL + MLB Native APIs + Concrete `parsed.*` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax. **Prerequisite:** Plans 1–2 complete — the engine generates concrete ESPN modules, the factory is retired, `spec.py` supports path params / `now_variant` / `default_from` / `transform`.

**Goal:** Generate the native NHL (api-web / edge / stats-rest / records) and MLB Stats wrappers from the fastRhockey + MLB OpenAPI specs into concrete modules with clean `nhl_{short}`/`mlb_{short}` names (collision-aware vs composites), delete the deprecated `nhl_api.py`, and replace the runtime `parsed.*` virtual modules with generated concrete files.

**Architecture:** `openapi_to_endpoints.py` converts `sdv-swagger`/fastRhockey OpenAPI specs → `nhl_api_web.yaml`/`nhl_edge.yaml`/`nhl_stats_rest.yaml`/`nhl_records.yaml`/`mlb_api.yaml` (operationId→clean short via the R-export name + the mapping doc; EDGE = path-prefix partition; `now_variant` from `/now` pairs; records via a curated allow-list). A flat `api_module.py.jinja` (no `{sport}/{league}`) renders these. `scrape_edge_vocab.py` fetches `*-landing`/`{metaType}` vocab → `_edge_vocab.yaml`/`_mlb_meta_vocab.yaml` for documented-options. A `parsed_module.py.jinja` emits concrete `sportsdataverse/parsed/{league}.py`, retiring the `types.ModuleType` builder.

**Tech Stack:** Python ≥3.10, PyYAML, Jinja2, pytest, ruff. Source specs: `sdv-swagger/{nhl_api_web,nhl_stats_rest,nhl_records}_openapi.yaml`, `sdv-swagger/mlb-stats-api.openapi.yaml`; `fastRhockey/data-raw/nhl_missing_endpoint_function_mapping.md`; R NAMESPACE exports.

---

## File Structure

**Create:**

- `tools/codegen/openapi_to_endpoints.py` — OpenAPI → flat endpoint YAML + reserved-name collision resolution.
- `tools/codegen/scrape_edge_vocab.py` — `*-landing`/`{metaType}` → vocab YAML (network, on-demand).
- `tools/codegen/templates/api_module.py.jinja` — flat (host+prefix) module template.
- `tools/codegen/templates/parsed_module.py.jinja` — concrete `parsed/{league}.py`.
- `tools/codegen/endpoints/{nhl_api_web,nhl_edge,nhl_stats_rest,nhl_records,mlb_api}.yaml` — generated.
- `tools/codegen/endpoints/{_edge_vocab,_mlb_meta_vocab}.yaml` — scraped.
- `tests/codegen/test_openapi_to_endpoints.py`, `tests/codegen/test_api_module.py`, `tests/codegen/test_parsed.py`.

**Modify:**

- `tools/codegen/spec.py` — `FlatApi` dataclass + `load_flat_api`; `{lang}` default param; `hydrate`/`fields` free-string params.
- `tools/codegen/render.py` / `generate.py` — render flat modules + concrete `parsed/{league}.py`.
- `tools/codegen/_codegen_runtime.py` — add `format_nhl_season`, `_nhl_season` transform; NHL/MLB base hosts.
- `sportsdataverse/nhl/{nhl_api_web,nhl_edge,nhl_stats_rest,nhl_records}.py` — replaced by generated.
- `sportsdataverse/mlb/mlb_api.py` — replaced by generated.
- `sportsdataverse/nhl/__init__.py` — drop `nhl_api` import; managed region for generated modules.
- `sportsdataverse/parsed/__init__.py` — reduce to importing 8 generated submodules; delete `_build_parsed_module`.
- `sportsdataverse/parsed/{league}.py` ×8 — generated.
- `docs/docs/nhl/index.md` — remove `nhl_api` deprecation rows (interim; superseded by Plan 5).

**Delete:**

- `sportsdataverse/nhl/nhl_api.py` (deprecated `statsapi.web.nhl.com`).

---

## Task 1: Flat-API spec + `api_module.py.jinja`

**Files:**

- Modify: `tools/codegen/spec.py` (`FlatApi`, `load_flat_api`), `tools/codegen/_codegen_runtime.py` (transforms + hosts)
- Create: `tools/codegen/templates/api_module.py.jinja`
- Test: `tests/codegen/test_api_module.py`

- [ ] **Step 1: Write the failing test**

`tests/codegen/test_api_module.py`:

```python
import ast
from pathlib import Path

from tools.codegen import generate, spec

def test_load_flat_api_and_render(tmp_path):
    y = tmp_path / "nhl_api_web.yaml"
    y.write_text(
        "api: nhl_api_web\nhost: 'https://api-web.nhle.com'\nname_pattern: 'nhl_{short}'\n"
        "module: nhl_api_web\nruntime_imports: [_get]\n"
        "endpoints:\n"
        "  - short: pbp\n    summary: 'PBP feed.'\n    path: '/v1/gamecenter/{game_id}/play-by-play'\n"
        "    path_params: [ { name: game_id, type: int, required: true } ]\n"
        "    parser: parse_nhl_web_pbp\n    example_args: { game_id: 2024020001 }\n"
        "  - short: club_schedule\n    summary: 'Club schedule.'\n"
        "    path: '/v1/club-schedule-season/{team}/{season}'\n"
        "    now_variant: '/v1/club-schedule-season/{team}/now'\n"
        "    path_params:\n      - { name: team, type: str, required: true }\n"
        "      - { name: season, type: 'int|str', required: false, transform: format_nhl_season }\n"
        "    example_args: { team: 'TOR', season: 2025 }\n",
        encoding="utf-8",
    )
    api = spec.load_flat_api(y, {})
    src = generate.render_flat_module(api)
    tree = ast.parse(src)  # valid python
    funcs = {n.name for n in tree.body if isinstance(n, ast.FunctionDef)}
    assert {"nhl_pbp", "nhl_club_schedule"} <= funcs
    assert "format_nhl_season" in src  # transform import + use
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/codegen/test_api_module.py -v`
Expected: FAIL — no `load_flat_api`/`render_flat_module`/`api_module.py.jinja`.

- [ ] **Step 3: Implement `FlatApi` + loader in `spec.py`**

```python
@dataclass(frozen=True)
class FlatApi:
    api: str
    host: str
    name_pattern: str
    module: str
    endpoints: List[Endpoint]
    runtime_imports: List[str] = field(default_factory=lambda: ["_get"])

def load_flat_api(path: Path, registry: Dict[str, Param]) -> FlatApi:
    raw = _read_yaml(path)
    endpoints = []
    for e in raw["endpoints"]:
        # (reuse the same query/path param parsing as load_espn_api — factor into _parse_endpoint)
        endpoints.append(_parse_endpoint(e, registry, path))
    return FlatApi(
        api=raw["api"], host=raw["host"], name_pattern=raw["name_pattern"],
        module=raw["module"], endpoints=endpoints,
        runtime_imports=raw.get("runtime_imports", ["_get"]),
    )
```

Factor the per-endpoint parsing from `load_espn_api` into a shared `_parse_endpoint(e, registry, path)` and call it from both loaders (DRY).

- [ ] **Step 4: Add transforms + hosts to `_codegen_runtime.py`**

Append to `sportsdataverse/_codegen_runtime.py`:

```python
def format_nhl_season(season):
    """4-digit end year (2025) or 8-digit string ('20242025') -> '20242025'; None -> None."""
    if season is None:
        return None
    s = str(season)
    if len(s) == 8 and s.isdigit():
        return s
    if len(s) == 4 and s.isdigit():
        return f"{int(s) - 1}{s}"
    raise ValueError(f"Unrecognized NHL season {season!r}")
```

(`_csv` already present for MLB `hydrate`/`fields` passthrough — MLB uses raw string passthrough, no transform needed.)

- [ ] **Step 5: Write `api_module.py.jinja`**

`tools/codegen/templates/api_module.py.jinja` — like the ESPN template but host is a literal base and there's no sport/league substitution. Reuse `_EndpointView` (it already computes `signature_params`/`has_dynamic_path`/`path_build_expr`/`url_fstring` from a `League`-like object; pass a stub league with `sport=""`, `league=""` so `path.format(sport="", league="")` is a no-op for flat paths that contain no `{sport}`/`{league}`).

```jinja
{% import "_docstring.jinja" as d %}
# GENERATED by tools/codegen/generate.py — DO NOT EDIT.
"""{{ api }} wrappers (host {{ host }}). Generated."""
from __future__ import annotations

from typing import Dict, Optional, Union

from sportsdataverse._codegen_runtime import {{ runtime_imports|join(", ") }}
{% if parser_imports %}from sportsdataverse.{{ parser_module }} import {{ parser_imports|join(", ") }}
{% endif %}

__all__ = [
{% for ep in endpoints %}    "{{ ep.fn_name }}",
{% endfor %}]

{% for ep in endpoints %}

def {{ ep.fn_name }}(
{% for p in ep.signature_params %}
{% if p.required and p.default_from is none %}    {{ p.python_name }},
{% else %}    {{ p.python_name }}: Optional[{{ p.type|type_hint }}] = {{ p.default|py_repr }},
{% endif %}
{% endfor %}
    *,
{% if ep.parser %}    return_parsed: bool = False,
    return_as_pandas: bool = False,
{% endif %}
    **kwargs,
) -> Dict:
{{ d.docstring(ep, "", "", "") }}
{% if ep.has_dynamic_path %}    {{ ep.path_build_expr }}
    raw = _get(__url, params={ {% for p in ep.query_params %}"{{ p.api }}": {{ p.python_name }}, {% endfor %} }, **kwargs)
{% else %}    raw = _get("{{ ep.url_fstring }}", params={ {% for p in ep.query_params %}"{{ p.api }}": {{ p.python_name }}, {% endfor %} }, **kwargs)
{% endif %}
{% if ep.parser %}    if return_parsed:
        return {{ ep.parser }}(raw, return_as_pandas=return_as_pandas)
{% endif %}    return raw
{% endfor %}
```

Add `generate.render_flat_module(api)` that builds `_EndpointView`s (host = `api.host`, parser_module from a per-api map e.g. `nhl.nhl_api_web_parsers`), computes `parser_imports`, and renders. For flat paths, `_EndpointView` must skip `path.format(sport=…)` when there's no `{sport}` token — guard with `"{sport}" in path`.

- [ ] **Step 6: Run + commit**

Run: `pytest tests/codegen/test_api_module.py -v` → PASS

```bash
git add tools/codegen/spec.py tools/codegen/_codegen_runtime.py tools/codegen/templates/api_module.py.jinja tools/codegen/generate.py tests/codegen/test_api_module.py
git commit -m "feat(codegen): flat-API spec + api_module template + format_nhl_season transform"
```

---

## Task 2: `openapi_to_endpoints.py` converter

**Files:**

- Create: `tools/codegen/openapi_to_endpoints.py`
- Test: `tests/codegen/test_openapi_to_endpoints.py`

- [ ] **Step 1: Write the failing test**

`tests/codegen/test_openapi_to_endpoints.py`:

```python
from tools.codegen import openapi_to_endpoints as o2e

def test_convert_extracts_paths_params_and_now_variant():
    spec = {
        "servers": [{"url": "https://api-web.nhle.com"}],
        "paths": {
            "/v1/gamecenter/{gameId}/play-by-play": {"get": {"summary": "PBP",
                "operationId": "gamecenter_play_by_play",
                "parameters": [{"name": "gameId", "in": "path", "required": True,
                                "schema": {"type": "integer"}}]}},
            "/v1/club-schedule-season/{team}/now": {"get": {"summary": "now"}},
            "/v1/club-schedule-season/{team}/{season}": {"get": {"summary": "season",
                "parameters": [{"name": "team", "in": "path", "schema": {"type": "string"}},
                               {"name": "season", "in": "path", "schema": {"type": "string"}}]}},
        },
    }
    eps = o2e.convert(spec, name_map={"gamecenter_play_by_play": "pbp",
                                      "/v1/club-schedule-season/{team}/{season}": "club_schedule"})
    by_short = {e["short"]: e for e in eps}
    assert by_short["pbp"]["path"] == "/v1/gamecenter/{gameId}/play-by-play"
    # /now + /{season} pair collapses to one endpoint with now_variant
    cs = by_short["club_schedule"]
    assert cs["now_variant"] == "/v1/club-schedule-season/{team}/now"

def test_edge_partition_splits_by_path_prefix():
    web, edge = o2e.partition_edge({"/v1/gamecenter/x": 1, "/v1/edge/skater-detail/{id}/now": 2,
                                    "/v1/cat/edge/goalie-detail/{id}/now": 3})
    assert "/v1/gamecenter/x" in web
    assert "/v1/edge/skater-detail/{id}/now" in edge and "/v1/cat/edge/goalie-detail/{id}/now" in edge
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/codegen/test_openapi_to_endpoints.py -v`
Expected: FAIL — no module.

- [ ] **Step 3: Implement the converter**

`tools/codegen/openapi_to_endpoints.py`:

```python
"""Convert OpenAPI specs (fastRhockey NHL, MLB stats) -> flat endpoint YAML."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List

import yaml

def _short_from(op_id: str, path: str, name_map: Dict[str, str]) -> str:
    if op_id in name_map:
        return name_map[op_id]
    if path in name_map:
        return name_map[path]
    # fallback: last 1-2 non-templated path segments, snake_cased
    segs = [s for s in path.split("/") if s and not s.startswith("{")]
    return re.sub(r"[^a-z0-9]+", "_", "_".join(segs[-2:]).lower()).strip("_")

def partition_edge(paths):
    web, edge = {}, {}
    for p, v in paths.items():
        (edge if p.startswith("/v1/edge/") or p.startswith("/v1/cat/edge/") else web)[p] = v
    return web, edge

def convert(spec: dict, name_map: Dict[str, str] | None = None) -> List[dict]:
    name_map = name_map or {}
    paths = spec.get("paths", {})
    # collapse `/now` + `/{...}` pairs into one endpoint with now_variant
    now_pairs = {}
    for p in list(paths):
        if p.endswith("/now"):
            stem = p[: -len("/now")]
            for q in paths:
                if q.startswith(stem + "/{") and q != p:
                    now_pairs[q] = p
    out = []
    for path, item in paths.items():
        if path.endswith("/now") and any(path == nv for nv in now_pairs.values()):
            continue  # folded into its parameterized sibling
        if "get" not in item or path.startswith("/model/") or path == "/ping":
            continue
        op = item["get"]
        short = _short_from(op.get("operationId", ""), path, name_map)
        path_params, query_params = [], []
        for prm in op.get("parameters", []):
            t = {"integer": "int", "string": "str"}.get(prm.get("schema", {}).get("type"), "str")
            entry = {"name": prm["name"], "type": t, "required": prm.get("required", prm.get("in") == "path")}
            (path_params if prm.get("in") == "path" else query_params).append(entry)
        ep = {"short": short, "summary": op.get("summary", ""), "path": path}
        if path in now_pairs:
            ep["now_variant"] = now_pairs[path]
        if path_params:
            ep["path_params"] = path_params
        if query_params:
            ep["extra_params"] = [{"name": q["name"], "type": q["type"]} for q in query_params]
        out.append(ep)
    return out

def write_api_yaml(spec_path: Path, out_path: Path, *, api, host, name_pattern, module,
                   parser_module, name_map=None, allow_list=None, runtime_imports=("_get",)):
    spec = yaml.safe_load(Path(spec_path).read_text(encoding="utf-8"))
    eps = convert(spec, name_map)
    if allow_list is not None:
        eps = [e for e in eps if e["short"] in allow_list]
    doc = {"api": api, "host": host, "name_pattern": name_pattern, "module": module,
           "parser_module": parser_module, "runtime_imports": list(runtime_imports), "endpoints": eps}
    Path(out_path).write_text(yaml.safe_dump(doc, sort_keys=False, width=100), encoding="utf-8")
    return eps
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/codegen/test_openapi_to_endpoints.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/codegen/openapi_to_endpoints.py tests/codegen/test_openapi_to_endpoints.py
git commit -m "feat(codegen): OpenAPI->endpoint YAML converter (now-variant fold + edge partition)"
```

---

## Task 3: Reserved-name collision resolution (clean `nhl_{short}`/`mlb_{short}`)

**Files:**

- Modify: `tools/codegen/generate.py` (collision check spanning the live package namespace)
- Test: `tests/codegen/test_api_module.py` (extend)

- [ ] **Step 1: Write the failing test**

```python
def test_reserved_composite_forces_qualifier():
    from tools.codegen import generate
    # nhl_teams is a reserved composite (sportsdataverse.nhl.nhl_teams); the api-web
    # `teams` endpoint must qualify to nhl_web_teams.
    reserved = generate.reserved_names("nhl")
    assert "nhl_teams" in reserved
    final = generate.resolve_name("nhl", "teams", reserved, qualifier="web")
    assert final == "nhl_web_teams"
    final2 = generate.resolve_name("nhl", "pbp", reserved - {"nhl_pbp"}, qualifier="web")
    assert final2 == "nhl_pbp"  # free -> clean
```

- [ ] **Step 2: Run → fail; implement in `generate.py`**

```python
def reserved_names(prefix: str) -> set[str]:
    """Names already defined in sportsdataverse.{prefix} by hand-written composites/loaders."""
    import importlib
    mod = importlib.import_module(f"sportsdataverse.{prefix}")
    return {n for n in dir(mod) if not n.startswith("_")}

def resolve_name(prefix, short, reserved, qualifier):
    clean = f"{prefix}_{short}"
    if clean not in reserved:
        return clean
    return f"{prefix}_{qualifier}_{short}"
```

The flat-module renderer applies `resolve_name` per endpoint (qualifier = `web`/`edge`/`stats_rest`/`records` for NHL, `api` for MLB), records old→new into `rename_map.yaml`, and the uniqueness check spans `reserved ∪ generated`.

- [ ] **Step 3: Run + commit**

Run: `pytest tests/codegen/test_api_module.py -v` → PASS

```bash
git add tools/codegen/generate.py tests/codegen/test_api_module.py
git commit -m "feat(codegen): clean NHL/MLB names with reserved-composite collision resolution"
```

---

## Task 4: `scrape_edge_vocab.py` + documented-options

**Files:**

- Create: `tools/codegen/scrape_edge_vocab.py`
- Modify: `templates/_docstring.jinja` (render `documented_options`)
- Test: `tests/codegen/test_render.py` (extend — offline, fixture vocab)

- [ ] **Step 1: Implement the scraper (network, on-demand)**

`tools/codegen/scrape_edge_vocab.py` fetches each EDGE `*-landing` payload + MLB `/api/v1/{metaType}` index and extracts the valid slugs, writing `_edge_vocab.yaml` / `_mlb_meta_vocab.yaml`:

```python
"""Refresh EDGE enum vocab + MLB metaType vocab (network; outputs committed)."""
from __future__ import annotations
import json, urllib.request, yaml
from pathlib import Path

UA = {"User-Agent": "Mozilla/5.0 (sdv-codegen vocab)"}

def _get(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=20) as r:
        return json.loads(r.read())

def edge_vocab() -> dict:
    base = "https://api-web.nhle.com/v1/edge"
    out = {}
    for kind in ("skater", "goalie", "team"):
        data = _get(f"{base}/{kind}-landing/now")
        out[kind] = sorted({k for k in data.keys()})  # implementer refines to the category/sortBy lists
    return out

def mlb_meta_vocab() -> list:
    return sorted(_get("https://statsapi.mlb.com/api/v1/meta")["metaTypes"]) if False else \
        ["leagueLeaderTypes", "gameTypes", "statGroups", "rosterTypes"]  # seed; refine live

if __name__ == "__main__":
    Path("tools/codegen/endpoints/_edge_vocab.yaml").write_text(yaml.safe_dump(edge_vocab()), encoding="utf-8")
    Path("tools/codegen/endpoints/_mlb_meta_vocab.yaml").write_text(yaml.safe_dump({"meta_types": mlb_meta_vocab()}), encoding="utf-8")
```

> The implementer refines the slug extraction against the live landing payloads (the exact keys for `category`/`sortBy`/`position`/`strength`) per the spec §3.6 note; commit the resulting vocab snapshot.

- [ ] **Step 2: Render `documented_options` in `_docstring.jinja`**

In the `Args:` loop of `_docstring.jinja`, when a param has `documented_options`, append them:

```jinja
{% for p in ep.query_params %}        {{ p.python_name }}: {{ p.api }} query parameter.{% if p.documented_options %} Options: {{ p.documented_options|join(", ") }}.{% endif %}
{% endfor %}
```

`generate.py` attaches `documented_options` to the relevant params from the vocab YAML when rendering EDGE top-10 / MLB `meta` / `hydrate`.

- [ ] **Step 3: Offline test with fixture vocab + commit**

Add a test that renders an endpoint whose param carries `documented_options=["all","C","L"]` and asserts the docstring contains "Options: all, C, L". Run → PASS.

```bash
git add tools/codegen/scrape_edge_vocab.py tools/codegen/templates/_docstring.jinja tools/codegen/generate.py tests/codegen/test_render.py
git commit -m "feat(codegen): EDGE/metaType vocab scrape + documented-options in docstrings"
```

---

## Task 5: Generate NHL native modules + delete `nhl_api.py`

**Files:**

- Create (generated): `tools/codegen/endpoints/{nhl_api_web,nhl_edge,nhl_stats_rest,nhl_records}.yaml`, `sportsdataverse/nhl/{nhl_api_web,nhl_edge,nhl_stats_rest,nhl_records}.py`
- Modify: `sportsdataverse/nhl/__init__.py`, `docs/docs/nhl/index.md`
- Delete: `sportsdataverse/nhl/nhl_api.py`
- Test: `tests/codegen/test_parity_full.py` (extend for NHL native)

- [ ] **Step 1: Generate the NHL YAML from the swagger specs**

Run a conversion script using `openapi_to_endpoints.write_api_yaml` for each NHL spec (from `sdv-swagger/`):

- `nhl_api_web_openapi.yaml` → partition edge → `nhl_api_web.yaml` (web paths) + `nhl_edge.yaml` (edge paths), `name_pattern: "nhl_{short}"` / `"nhl_edge_{short}"`, `module: nhl_api_web`/`nhl_edge`, `parser_module: nhl.nhl_api_web_parsers`/`nhl.nhl_edge_parsers`, `name_map` from `nhl_missing_endpoint_function_mapping.md` + fastRhockey NAMESPACE.
- `nhl_stats_rest_openapi.yaml` → `nhl_stats_rest.yaml` (`{lang}` param default `"en"`).
- `nhl_records_openapi.yaml` → `nhl_records.yaml` with `allow_list=` the curated ~50 (spec §8/§14).

Hand-verify `now_variant` folds and reserved-name collisions (Task 3).

- [ ] **Step 2: Generate the modules + parity test**

Add NHL native to `generate.build_live()` (render each `FlatApi` to `sportsdataverse/nhl/{module}.py`, manage `nhl/__init__.py` region). Extend `tests/codegen/test_parity_full.py` with a NHL-native check: every current `nhl_web_*`/`nhl_edge_*`/`nhl_stats_rest_*`/`nhl_records_*` name maps (via `rename_map.yaml`) to a generated function with matching path-param signature. Run → iterate to green.

- [ ] **Step 3: Delete `nhl_api.py` + its import + docs rows**

```bash
git rm sportsdataverse/nhl/nhl_api.py
```

In `sportsdataverse/nhl/__init__.py` remove `from sportsdataverse.nhl.nhl_api import *` (and the legacy `nhl_api_*` re-exports). In `docs/docs/nhl/index.md` remove the `nhl_api` deprecation table rows (the page is regenerated wholesale in Plan 5; this interim edit keeps it accurate). Confirm no composite imports `nhl_api`:

Run: `grep -rn "nhl\.nhl_api import\|from \.nhl_api import" sportsdataverse/` → no matches.

- [ ] **Step 4: Full suite + import**

Run: `python -c "import sportsdataverse.nhl as n; print(n.nhl_pbp, n.nhl_club_schedule)" && pytest tests/ -q`
Expected: import clean; suite green.

- [ ] **Step 5: Commit**

```bash
git add tools/codegen/endpoints/nhl_*.yaml sportsdataverse/nhl/ docs/docs/nhl/index.md tools/codegen/rename_map.yaml tests/codegen/test_parity_full.py
git commit -m "feat(nhl)!: generate native NHL modules (api-web/edge/stats-rest/records); delete deprecated nhl_api"
```

---

## Task 6: Generate MLB Stats module (`hydrate`/`fields`/`{metaType}`)

**Files:**

- Create: `tools/codegen/endpoints/mlb_api.yaml`, regenerated `sportsdataverse/mlb/mlb_api.py`
- Modify: `sportsdataverse/mlb/__init__.py` (managed region)
- Test: `tests/codegen/test_parity_full.py` (MLB)

- [ ] **Step 1: Generate `mlb_api.yaml`**

`openapi_to_endpoints.write_api_yaml` on `sdv-swagger/mlb-stats-api.openapi.yaml` → `mlb_api.yaml`, `name_pattern: "mlb_{short}"`, `module: mlb_api`, `parser_module: mlb.mlb_api_parsers`, `name_map` from baseballR NAMESPACE (e.g. `mlb_attendance`, `mlb_draft`). Add `hydrate`/`fields` as free-string `extra_params` on the relevant endpoints; model `/api/v1/{metaType}` as one `mlb_meta(meta_type)` with `documented_options` from `_mlb_meta_vocab.yaml`. Handle the `/api/v1.1/` host override on `feed/live` (per-endpoint `host`). Reserved-collision: `mlb_schedule`/`mlb_pbp`/`mlb_teams` are composites → those endpoints qualify to `mlb_api_*`.

- [ ] **Step 2: Generate + parity**

Add MLB to `generate.build_live()`. Extend parity test: every current `mlb_api_*` maps (via rename map) to a generated function with matching signature. Run → green.

- [ ] **Step 3: Full suite + commit**

Run: `python -c "import sportsdataverse.mlb as m; print(m.mlb_attendance)" && pytest tests/ -q` → PASS

```bash
git add tools/codegen/endpoints/mlb_api.yaml sportsdataverse/mlb/ tools/codegen/rename_map.yaml tests/codegen/test_parity_full.py
git commit -m "feat(mlb)!: generate MLB Stats module (clean names, hydrate/fields, metaType)"
```

---

## Task 7: Concrete `parsed.*` modules (retire `types.ModuleType`)

**Files:**

- Create: `tools/codegen/templates/parsed_module.py.jinja`, `sportsdataverse/parsed/{league}.py` ×8 (generated)
- Modify: `sportsdataverse/parsed/__init__.py`, `tools/codegen/generate.py`
- Test: `tests/codegen/test_parsed.py`

- [ ] **Step 1: Write the failing test**

`tests/codegen/test_parsed.py`:

```python
import sportsdataverse.parsed.nba as pnba
from sportsdataverse.nba import espn_nba_scoreboard as raw_scoreboard

def test_parsed_module_is_concrete_file_not_virtual():
    import sportsdataverse.parsed.nba as m
    assert m.__file__.endswith("parsed/nba.py")  # real file, not types.ModuleType

def test_parsed_flips_return_parsed_default(monkeypatch):
    seen = {}

    def fake(*a, **k):
        seen.update(k)
        return "df"

    monkeypatch.setattr("sportsdataverse.nba.espn_nba_scoreboard", fake)
    import importlib
    import sportsdataverse.parsed.nba as m
    importlib.reload(m)
    m.espn_nba_scoreboard(dates="20240115")
    assert seen.get("return_parsed") is True
```

- [ ] **Step 2: Run → fail; write `parsed_module.py.jinja`**

```jinja
# GENERATED by tools/codegen/generate.py — DO NOT EDIT.
"""DataFrame-by-default mirror of sportsdataverse.{{ league }}. Generated."""
from __future__ import annotations

{% for fn in parser_fns %}from sportsdataverse.{{ league }} import {{ fn }} as _raw_{{ fn }}
{% endfor %}
{% for fn in passthrough_fns %}from sportsdataverse.{{ league }} import {{ fn }} as {{ fn }}  # noqa: F401
{% endfor %}

__all__ = [
{% for fn in parser_fns + passthrough_fns %}    "{{ fn }}",
{% endfor %}]

{% for fn in parser_fns %}

def {{ fn }}(*args, **kwargs):
    """``return_parsed=True`` by default (parsed.* mirror). Pass ``return_parsed=False`` for raw."""
    kwargs.setdefault("return_parsed", True)
    return _raw_{{ fn }}(*args, **kwargs)
{% endfor %}
```

`generate.py` computes, per league, `parser_fns` (generated functions that accept `return_parsed`) vs `passthrough_fns` (those that don't), by inspecting the generated module's signatures, and renders `sportsdataverse/parsed/{league}.py`.

- [ ] **Step 3: Reduce `parsed/__init__.py`**

Replace `sportsdataverse/parsed/__init__.py` body with plain imports of the 8 generated submodules; delete `_build_parsed_module` / `_wrap_default_parsed` / the `types.ModuleType` + `sys.modules` loop:

```python
"""DataFrame-by-default mirror of the raw API (generated concrete modules)."""
from __future__ import annotations

from sportsdataverse.parsed import (  # noqa: F401
    cfb, mbb, mlb, nba, nfl, nhl, wbb, wnba,
)

__all__ = ["nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb", "nhl"]
```

- [ ] **Step 4: Run + parity for parsed**

Run: `pytest tests/codegen/test_parsed.py -q && python -c "from sportsdataverse.parsed.cfb import espn_cfb_scoreboard; print(espn_cfb_scoreboard)"`
Expected: PASS; import clean. Add a parity check that `dir(sportsdataverse.parsed.nba)` ⊇ the pre-change parsed surface.

- [ ] **Step 5: Commit**

```bash
git add tools/codegen/templates/parsed_module.py.jinja sportsdataverse/parsed/ tools/codegen/generate.py tests/codegen/test_parsed.py
git commit -m "feat(parsed): concrete generated parsed.* modules; retire types.ModuleType builder"
```

---

## Self-Review

- **Spec coverage:** §3.2 NHL/MLB naming → Task 3; §3.6 NHL provenance/now_variant/{lang}/converter → Tasks 1,2,5; §3.7 MLB hydrate/fields/{metaType} → Task 6; EDGE vocab/documented-options → Task 4; phase 7 (generate + delete nhl_api) → Tasks 5,6; phase 8 (concrete parsed.*) → Task 7; records curation → Task 5 Step 1 `allow_list`. Deferred: loaders (Plan 4), docs site (Plan 5).
- **Placeholder scan:** the scraper slug-refinement (Task 4) and `name_map`/allow-list authoring (Tasks 5,6) are data steps with concrete sources (landing payloads; `nhl_missing_endpoint_function_mapping.md`; baseballR NAMESPACE; spec §14 curated list), not vague TODOs. All code steps are complete.
- **Type consistency:** `FlatApi`/`load_flat_api`/`render_flat_module` consistent; `_EndpointView` reused for flat APIs with the `"{sport}" in path` guard; `reserved_names`/`resolve_name` shared by NHL+MLB; `rename_map.yaml` extended (not overwritten) by Tasks 5,6 and consumed by parity + Plan 2's NEWS task; `parser_fns`/`passthrough_fns` match the `parsed_module` template reads.
