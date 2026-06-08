<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [ESPN Full Surface + Factory Retirement Implementation Plan](#espn-full-surface--factory-retirement-implementation-plan)
  - [File Structure](#file-structure)
  - [Task 1: Extend `spec.py` for path params + irregular constructs](#task-1-extend-specpy-for-path-params--irregular-constructs)
  - [Task 2: Render path-param signatures + URL construction](#task-2-render-path-param-signatures--url-construction)
  - [Task 3: `extract.py` — seed full ESPN YAML + rename map from the factory](#task-3-extractpy--seed-full-espn-yaml--rename-map-from-the-factory)
  - [Task 4: Author high-traffic `@return` schemas](#task-4-author-high-traffic-return-schemas)
  - [Task 5: Full inventory-parity test (generated ⊇ factory via rename map)](#task-5-full-inventory-parity-test-generated-%E2%8A%87-factory-via-rename-map)
  - [Task 6: Repoint the runtime `_get` (pure refactor)](#task-6-repoint-the-runtime-_get-pure-refactor)
  - [Task 7: Swap generated modules into the live package + manage `__init__`](#task-7-swap-generated-modules-into-the-live-package--manage-__init__)
  - [Task 8: Retire the runtime factory](#task-8-retire-the-runtime-factory)
  - [Task 9: Update tests to canonical names + publish rename map](#task-9-update-tests-to-canonical-names--publish-rename-map)
  - [Self-Review](#self-review)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# ESPN Full Surface + Factory Retirement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. **Prerequisite:** Plan 1 (`2026-06-04-codegen-engine-foundation.md`) is complete — the engine (`tools/codegen/{spec,render,generate}.py`, templates, `_codegen_runtime.py`) exists and the site-v2 slice generates + parity-tests green.

**Goal:** Extend the engine to the entire ESPN surface (Site v2, Site v2 alt, Web v3, Core v2) across all 8 leagues with scopes + irregular-path constructs, swap the generated modules into the live package, delete the runtime factory, and publish the rename map — with full inventory parity guaranteeing no public function is lost or silently renamed.

**Architecture:** `extract.py` introspects the live `_common_espn._UNIVERSAL_WRAPPERS`/`_NCAA_/_FOOTBALL_/_MLB_WRAPPERS` + core-fn source + `ENDPOINT_PARSERS` to seed `espn_site_v2.yaml`/`espn_web_v3.yaml`/`espn_core_v2.yaml` + a `rename_map.yaml`. `spec.py`/`render.py` grow path-params, optional segments, `default_from`, and `now_variant`. `generate.py` writes real `sportsdataverse/{league}/{league}_espn_ext.py` and manages each `__init__.py` marker region. The factory (`make_league_module`, `_bind`, the `_*_WRAPPERS` tables, the 127 core fns) is deleted; `ENDPOINT_PARSERS`/`parser_for` stay.

**Tech Stack:** Python ≥3.10, PyYAML, Jinja2, pytest, ruff, `inspect`/`ast` for extraction.

---

## File Structure

**Create:**

- `tools/codegen/extract.py` — introspect the factory → emit YAML + `rename_map.yaml` + schema stubs.
- `tools/codegen/endpoints/espn_web_v3.yaml`, `tools/codegen/endpoints/espn_core_v2.yaml` — seeded by extract.
- `tools/codegen/rename_map.yaml` — old→new public-name map.
- `tools/codegen/schemas/{teams,standings,team_roster,leaders,summary}.yaml` — authored high-traffic schemas.
- `tests/codegen/test_extract.py`, `tests/codegen/test_render_paths.py`, `tests/codegen/test_parity_full.py`.

**Modify:**

- `tools/codegen/spec.py` — `Endpoint.path_params`, `now_variant`, `default_from`/`optional_segment` on `Param`; `load_espn_api` parses them.
- `tools/codegen/render.py` / `templates/espn_league_module.py.jinja` — path-param signature + URL construction, optional segments, `now_variant`.
- `tools/codegen/endpoints/espn_site_v2.yaml` — expanded from the slice to the full site-v2 set.
- `tools/codegen/generate.py` — `ESPN_APIS = ["espn_site_v2", "espn_web_v3", "espn_core_v2"]`; `--target live` writes to `sportsdataverse/{league}/`; `__init__.py` managed-region rewrite.
- `sportsdataverse/{nba,wnba,mbb,wbb,cfb,nfl,mlb,nhl}/{prefix}_espn_ext.py` — replaced by generated output.
- `sportsdataverse/{league}/__init__.py` — managed import region.
- `sportsdataverse/_common_espn.py` — delete factory + core fns (keep host constants only if still referenced).
- `sportsdataverse/nhl/__init__.py` — add `from .nhl_espn_ext import *` (new module).
- `tests/test_cli.py`, `tests/test_espn_live.py`, `tests/test_qol.py` — new canonical names.
- `NEWS.md` — rename map.

---

## Task 1: Extend `spec.py` for path params + irregular constructs

**Files:**

- Modify: `tools/codegen/spec.py`
- Test: `tests/codegen/test_spec.py` (extend)

- [ ] **Step 1: Write the failing test**

Append to `tests/codegen/test_spec.py`:

```python
def test_endpoint_parses_path_params_optional_segment_default_from_now_variant(tmp_path):
    from tools.codegen import spec
    y = tmp_path / "api.yaml"
    y.write_text(
        "api: espn_core_v2\nhost: core_v2\nname_pattern: 'espn_{prefix}_{short}'\n"
        "endpoints:\n"
        "  - short: athlete_career_stats\n"
        "    path: '/{sport}/leagues/{league}/athletes/{athlete_id}/statistics[/{stat_type}]'\n"
        "    path_params:\n"
        "      - { name: athlete_id, type: 'int|str', required: true }\n"
        "      - { name: stat_type, type: int, required: false, optional_segment: true }\n"
        "  - short: event_competition\n"
        "    path: '/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}'\n"
        "    path_params:\n"
        "      - { name: event_id, type: 'int|str', required: true }\n"
        "      - { name: cid, type: 'int|str', required: false, default_from: event_id }\n"
        "  - short: club_schedule\n"
        "    path: '/x/{team}/{season}'\n"
        "    now_variant: '/x/{team}/now'\n"
        "    path_params:\n"
        "      - { name: team, type: str, required: true }\n"
        "      - { name: season, type: 'int|str', required: false }\n",
        encoding="utf-8",
    )
    api = spec.load_espn_api(y, {})
    a = api.endpoints[0]
    assert [p.python_name for p in a.path_params] == ["athlete_id", "stat_type"]
    assert a.path_params[1].optional_segment is True
    b = api.endpoints[1]
    assert b.path_params[1].default_from == "event_id"
    c = api.endpoints[2]
    assert c.now_variant == "/x/{team}/now"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/codegen/test_spec.py::test_endpoint_parses_path_params_optional_segment_default_from_now_variant -v`
Expected: FAIL — `Param` has no `optional_segment`/`default_from`; `Endpoint` has no `now_variant`/`path_params` parsing.

- [ ] **Step 3: Implement**

In `tools/codegen/spec.py`, extend `Param` and `Endpoint` and the loader:

```python
@dataclass(frozen=True)
class Param:
    python_name: str
    api: str
    type: str = "str"
    required: bool = False
    default: object = None
    pattern: Optional[str] = None
    is_query: bool = True
    optional_segment: bool = False     # pairs with [/{token}] in path
    default_from: Optional[str] = None  # use another arg's value when None
    transform: Optional[str] = None     # named runtime transform (e.g. format_nhl_season, _csv)
```

Add `now_variant` to `Endpoint`:

```python
@dataclass(frozen=True)
class Endpoint:
    short: str
    path: str
    summary: str = ""
    scope: str = "universal"
    host: Optional[str] = None
    parser: Optional[str] = None
    returns_schema: Optional[str] = None
    query_params: List[Param] = field(default_factory=list)
    path_params: List[Param] = field(default_factory=list)
    example_args: Dict[str, object] = field(default_factory=dict)
    now_variant: Optional[str] = None
    exclude_leagues: List[str] = field(default_factory=list)
```

In `load_espn_api`, parse `path_params` (inline) and pass through the new fields:

```python
        pps = []
        for pp in e.get("path_params", []):
            pps.append(
                Param(
                    python_name=pp["name"],
                    api=pp["name"],
                    type=pp.get("type", "str"),
                    required=pp.get("required", True),
                    default=pp.get("default"),
                    is_query=False,
                    optional_segment=pp.get("optional_segment", False),
                    default_from=pp.get("default_from"),
                    transform=pp.get("transform"),
                )
            )
        ep = Endpoint(
            short=e["short"],
            path=e["path"],
            summary=e.get("summary", ""),
            scope=e.get("scope", "universal"),
            host=e.get("host"),
            parser=e.get("parser"),
            returns_schema=e.get("returns_schema"),
            query_params=qps,
            path_params=pps,
            example_args=e.get("example_args", {}) or {},
            now_variant=e.get("now_variant"),
            exclude_leagues=list(e.get("exclude_leagues", [])),
        )
```

Update the path-token validation to strip optional-segment brackets first:

```python
        bare_path = ep.path.replace("[", "").replace("]", "")
        tokens = set(_PATH_TOKEN.findall(bare_path)) - {"sport", "league"}
        known = {p.python_name for p in ep.path_params} | set(registry)
        missing = tokens - known
        if missing:
            raise SpecError(f"{path}: endpoint {ep.short!r} path token(s) {missing} have no param")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/codegen/test_spec.py -v`
Expected: PASS (all spec tests).

- [ ] **Step 5: Commit**

```bash
git add tools/codegen/spec.py tests/codegen/test_spec.py
git commit -m "feat(codegen): spec support for path params, optional segments, default_from, now_variant"
```

---

## Task 2: Render path-param signatures + URL construction

**Files:**

- Modify: `tools/codegen/generate.py` (`_EndpointView`), `tools/codegen/templates/espn_league_module.py.jinja`
- Test: `tests/codegen/test_render_paths.py`

- [ ] **Step 1: Write the failing test**

`tests/codegen/test_render_paths.py`:

```python
import ast

from tools.codegen import spec
from tools.codegen.generate import _EndpointView

def _view(ep, league):
    return _EndpointView(ep, f"espn_x_{ep.short}", "https://h", league)

def test_path_param_function_renders_positional_args_and_fstring():
    lg = spec.League("x", "basketball", "nba", ["universal"])
    ep = spec.Endpoint(
        short="team",
        path="/{sport}/{league}/teams/{team_id}",
        path_params=[spec.Param("team_id", "team_id", "int|str", required=True, is_query=False)],
    )
    v = _view(ep, lg)
    # full_url is an f-string body with {team_id} retained for runtime substitution
    assert "{team_id}" in v.url_fstring
    assert v.signature_params[0].python_name == "team_id"

def test_optional_segment_renders_conditional_path():
    lg = spec.League("x", "hockey", "nhl", ["universal"])
    ep = spec.Endpoint(
        short="athlete_career_stats",
        path="/{sport}/leagues/{league}/athletes/{athlete_id}/statistics[/{stat_type}]",
        path_params=[
            spec.Param("athlete_id", "athlete_id", "int|str", required=True, is_query=False),
            spec.Param("stat_type", "stat_type", "int", required=False, is_query=False, optional_segment=True),
        ],
    )
    v = _view(ep, lg)
    assert v.has_dynamic_path is True
    # the generated body references a path-building helper
    assert "stat_type" in v.path_build_expr
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/codegen/test_render_paths.py -v`
Expected: FAIL — `_EndpointView` lacks `url_fstring`/`signature_params`/`has_dynamic_path`/`path_build_expr`.

- [ ] **Step 3: Implement `_EndpointView` path handling**

Rewrite `tools/codegen/generate.py` `_EndpointView` to compute, from the endpoint:

- `signature_params`: ordered list = required path params (no default) → required query → defaulted path → defaulted query → `now_variant` makes its trailing path param optional.
- `has_dynamic_path`: True if any path token beyond sport/league, or `now_variant`, or `optional_segment`.
- For the **simple** case (no dynamic path), `url_fstring` = the literal `{host}{path.format(sport,league)}`.
- For the **dynamic** case, `path_build_expr` = a Python expression string the template drops into the body that builds the path, handling `default_from`, `optional_segment`, `now_variant`, and `transform`.

```python
class _EndpointView:
    def __init__(self, ep, fn_name, ep_host, league):
        self.short = ep.short
        self.summary = ep.summary
        self.parser = ep.parser
        self.fn_name = fn_name
        self.example_args = ep.example_args
        self.query_params = ep.query_params
        self.path_params = ep.path_params
        self._host = ep_host
        self._sport = league.sport
        self._league = league.league
        self._path = ep.path
        self._now_variant = ep.now_variant
        self.example_call = _example_call(ep, fn_name)
        self.example_url = _example_url(ep_host, ep, league.sport, league.league)

        # signature order: required (no default) first, then optional
        req_path = [p for p in ep.path_params if p.required and p.default_from is None]
        opt_path = [p for p in ep.path_params if not p.required or p.default_from is not None]
        req_q = [p for p in ep.query_params if p.required]
        opt_q = [p for p in ep.query_params if not p.required]
        self.signature_params = req_path + req_q + opt_path + opt_q

        bare = ep.path.replace("[", "").replace("]", "")
        import re as _re
        dyn_tokens = set(_re.findall(r"\{(\w+)\}", bare)) - {"sport", "league"}
        self.has_dynamic_path = bool(dyn_tokens) or ep.now_variant is not None
        if not self.has_dynamic_path:
            self.url_fstring = f"{ep_host}{ep.path.format(sport=league.sport, league=league.league)}"
            self.path_build_expr = ""
        else:
            self.url_fstring = ""
            self.path_build_expr = self._build_path_expr(ep, ep_host, league)

    def _build_path_expr(self, ep, ep_host, league):
        # Emit Python statements (joined by ';') assigning `__url`.
        lines = []
        # transforms + default_from applied to locals first
        for p in ep.path_params:
            if p.default_from:
                lines.append(f"{p.python_name} = {p.python_name} if {p.python_name} is not None else {p.default_from}")
            if p.transform:
                lines.append(f"{p.python_name} = {p.transform}({p.python_name})")
        base = ep.path.format(sport=league.sport, league=league.league) if "{sport}" in ep.path else ep.path
        # optional segment: split on the bracket
        if "[" in ep.path:
            head, tail = ep.path.split("[", 1)
            tail = tail.rstrip("]")
            seg_param = _PATH_TOKEN_FIRST(tail)
            head_f = head.format(sport=league.sport, league=league.league)
            tail_f = tail  # like "/{stat_type}"
            lines.append(f'__suffix = f"{tail_f}" if {seg_param} is not None else ""')
            lines.append(f'__url = f"{ep_host}{head_f}" + __suffix')
        elif ep.now_variant:
            # last optional path param toggles now-variant
            toggle = ep.path_params[-1].python_name
            now_f = ep.now_variant.format(sport=league.sport, league=league.league)
            full_f = ep.path.format(sport=league.sport, league=league.league)
            lines.append(f'__path = f"{now_f}" if {toggle} is None else f"{full_f}"')
            lines.append(f'__url = f"{ep_host}" + __path')
        else:
            full_f = ep.path.format(sport=league.sport, league=league.league)
            lines.append(f'__url = f"{ep_host}{full_f}"')
        return "\n    ".join(lines)
```

Add the helper at module scope:

```python
import re as _re_mod

def _PATH_TOKEN_FIRST(s: str) -> str:
    m = _re_mod.search(r"\{(\w+)\}", s)
    return m.group(1) if m else ""
```

- [ ] **Step 4: Update the template to use `signature_params` / dynamic path**

In `templates/espn_league_module.py.jinja`, render the signature from `ep.signature_params` (path params required-first, no default; query/optional with defaults), and the body from `has_dynamic_path`:

```jinja
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
{{ d.docstring(ep, sport, league, "") }}
{% if ep.has_dynamic_path %}    {{ ep.path_build_expr }}
    raw = _get(
        __url,
{% else %}    raw = _get(
        "{{ ep.url_fstring }}",
{% endif %}
        params={
{% for p in ep.query_params %}            "{{ p.api }}": {{ p.python_name }},
{% endfor %}        },
        **kwargs,
    )
{% if ep.parser %}    if return_parsed:
        return {{ ep.parser }}(raw, return_as_pandas=return_as_pandas)
{% endif %}    return raw
```

Add `from sportsdataverse._codegen_runtime import _get, _csv` and any `transform` imports (e.g. `format_nhl_season`) to the module-header import block — the generator passes a `runtime_imports` list computed from endpoints' `transform` fields.

- [ ] **Step 5: Run tests + a build smoke**

Run: `pytest tests/codegen/test_render_paths.py tests/codegen/test_generate.py -v`
Expected: PASS. Then `python tools/codegen/generate.py && ruff check tools/codegen/_generated/` → clean.

- [ ] **Step 6: Commit**

```bash
git add tools/codegen/generate.py tools/codegen/templates/espn_league_module.py.jinja tests/codegen/test_render_paths.py
git commit -m "feat(codegen): path params, optional segments, default_from, now_variant rendering"
```

---

## Task 3: `extract.py` — seed full ESPN YAML + rename map from the factory

**Files:**

- Create: `tools/codegen/extract.py`
- Test: `tests/codegen/test_extract.py`

- [ ] **Step 1: Write the failing test**

`tests/codegen/test_extract.py`:

```python
from tools.codegen import extract

def test_extract_core_fn_url_and_params():
    # _site_v2_scoreboard builds an f-string with /scoreboard + known query params
    info = extract.describe_core_fn("scoreboard")
    assert info["short"] == "scoreboard"
    assert info["path"].endswith("/scoreboard")
    assert "dates" in info["query_params"]
    assert info["parser"] == "parse_scoreboard"  # from ENDPOINT_PARSERS

def test_extract_emits_rename_map_for_suffixed_shorts():
    # teams_site -> teams (clean); teams_core keeps qualifier
    rm = extract.build_rename_map()
    assert rm.get("espn_nba_teams_site") == "espn_nba_teams"
    assert "espn_nba_teams_core" in rm.values() or "espn_nba_teams_core" in rm  # core retains
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/codegen/test_extract.py -v`
Expected: FAIL — no `tools.codegen.extract`.

- [ ] **Step 3: Implement `extract.py`**

`tools/codegen/extract.py` — introspect the live factory tables + core-fn source:

```python
"""One-time/refresh extraction: live ESPN factory -> codegen YAML + rename_map.

Reads sportsdataverse._common_espn._UNIVERSAL_WRAPPERS (+ NCAA/football/MLB tables)
and each core fn's source (f-string URL + signature) and
_common_espn_parsers.ENDPOINT_PARSERS, then emits espn_*.yaml + rename_map.yaml.
"""

from __future__ import annotations

import inspect
import re
from pathlib import Path
from typing import Dict

import yaml

from sportsdataverse import _common_espn as ce
from sportsdataverse._common_espn_parsers import ENDPOINT_PARSERS

_HOST_CONST = {
    ce._SITE_V2: "site_v2",
    ce._SITE_V2_ALT: "site_v2_alt",
    ce._WEB_V3: "web_v3",
    ce._CORE_V2: "core_v2",
}
_URL_RE = re.compile(r'f"\{(_[A-Z_0-9]+)\}([^"]*)"')  # f"{_CORE_V2}/...."

# clean-name policy: drop these suffixes when the un-suffixed name is unique
_DROP_SUFFIX = {"_site": "", "_alt": ""}  # _core kept (collides with site)

def describe_core_fn(short: str) -> Dict:
    table = dict(ce._UNIVERSAL_WRAPPERS + ce._NCAA_WRAPPERS + ce._FOOTBALL_WRAPPERS + ce._MLB_WRAPPERS)
    core_fn = table[short]
    src = inspect.getsource(core_fn)
    m = _URL_RE.search(src)
    host_key = _HOST_CONST.get(getattr(ce, m.group(1)), "core_v2") if m else "core_v2"
    path = m.group(2) if m else ""
    sig = inspect.signature(core_fn)
    skip = {"sport", "league", "kwargs"}
    query_params, path_params = [], []
    for name, p in sig.parameters.items():
        if name in skip or p.kind == p.VAR_KEYWORD:
            continue
        if "{" + name + "}" in path:
            path_params.append(name)
        else:
            query_params.append(name)
    return {
        "short": short,
        "host": host_key,
        "path": path,
        "query_params": query_params,
        "path_params": path_params,
        "parser": ENDPOINT_PARSERS.get(short),
    }

def _clean_name(short: str, all_shorts: set) -> str:
    for suf in _DROP_SUFFIX:
        if short.endswith(suf):
            base = short[: -len(suf)]
            if base not in all_shorts:  # un-suffixed name is free
                return base
    return short

def build_rename_map(prefixes=("nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb", "nhl")) -> Dict[str, str]:
    shorts = {s for s, _ in ce._UNIVERSAL_WRAPPERS}
    rename: Dict[str, str] = {}
    for short, _ in ce._UNIVERSAL_WRAPPERS:
        new = _clean_name(short, shorts)
        if new != short:
            for pfx in prefixes:
                rename[f"espn_{pfx}_{short}"] = f"espn_{pfx}_{new}"
    return rename

def write_yaml(obj, path: Path) -> None:
    Path(path).write_text(yaml.safe_dump(obj, sort_keys=False, width=100), encoding="utf-8")
```

> The implementer extends `describe_core_fn` to also detect `optional_segment`/`default_from` patterns (functions whose body has `if x is not None` path branches — `_core_v2_season_powerindex`, `_core_v2_athlete_statistics`, `_core_v2_event_competition`, `_core_v2_season_qbr`) and emit the corresponding YAML. There are ~6 such functions (enumerated in spec §3.4); hand-verify each after extraction.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/codegen/test_extract.py -v`
Expected: PASS.

- [ ] **Step 5: Generate the YAML + rename map**

Run a one-shot script (or `python -c`) that calls `describe_core_fn` for every short in each table, groups by host into `espn_site_v2.yaml`/`espn_web_v3.yaml`/`espn_core_v2.yaml` (scope from which table it came: universal/ncaa/football/mlb), writes `rename_map.yaml`, and seeds `example_args` for each (use a small per-param default map: `dates="20240115"`, `team_id`/`event_id`/`athlete_id` real ids from the liveness/probe data, `season=2024`). **Hand-verify** the ~6 irregular endpoints and `nhl` `exclude_leagues: [nhl]` on `athlete_gamelog`.

- [ ] **Step 6: Commit**

```bash
git add tools/codegen/extract.py tests/codegen/test_extract.py tools/codegen/endpoints/ tools/codegen/rename_map.yaml
git commit -m "feat(codegen): extract full ESPN YAML + rename map from the live factory"
```

---

## Task 4: Author high-traffic `@return` schemas

**Files:**

- Create: `tools/codegen/schemas/{teams,standings,team_roster,leaders,summary}.yaml`
- Modify: `tools/codegen/extract.py` (`--schemas`)
- Test: `tests/codegen/test_generate.py` (extend)

- [ ] **Step 1: Add `extract.py --schemas` introspection**

Add a function that, given an endpoint short + a captured fixture payload (committed under `tests/codegen/fixtures/`), runs the registered parser and emits a schema stub `{name, type}` per column:

```python
def schema_from_parser(parser_name: str, payload: dict) -> dict:
    import sportsdataverse._common_espn_parsers as P
    df = getattr(P, parser_name)(payload)  # polars DataFrame
    cols = [{"name": c, "type": str(df.schema[c]), "description": ""} for c in df.columns]
    return {"schema": parser_name, "kind": "dataframe", "description": "", "columns": cols}
```

- [ ] **Step 2: Capture fixtures + emit stubs + author descriptions**

For `parse_teams`, `parse_standings`, `parse_team_roster`, `parse_leaders`, and the `parse_summary` sections: capture one live payload each into `tests/codegen/fixtures/`, run `schema_from_parser`, write the stub YAML, then **author the `description` fields** (mine the wehoop/hoopR `@return` roxygen where a 1:1 column exists). For `summary` use `kind: frames` with one block per `SUMMARY_SECTION_PARSERS` key.

- [ ] **Step 3: Add a schema-resolution test**

Append to `tests/codegen/test_generate.py`:

```python
def test_returns_schema_resolves_for_authored_endpoints():
    from tools.codegen import spec
    from pathlib import Path
    for name in ("scoreboard", "teams", "standings"):
        d = spec._read_yaml(Path(f"tools/codegen/schemas/{name}.yaml"))
        assert d["columns"], f"{name} schema has no columns"
        assert all(c["description"] for c in d["columns"]), f"{name} has blank descriptions"
```

- [ ] **Step 4: Run + commit**

Run: `pytest tests/codegen/test_generate.py -v` → PASS

```bash
git add tools/codegen/schemas/ tools/codegen/extract.py tests/codegen/fixtures/ tests/codegen/test_generate.py
git commit -m "feat(codegen): author high-traffic @return schemas (teams/standings/roster/leaders/summary)"
```

---

## Task 5: Full inventory-parity test (generated ⊇ factory via rename map)

**Files:**

- Create: `tests/codegen/test_parity_full.py`

- [ ] **Step 1: Write the parity test**

`tests/codegen/test_parity_full.py` — for every league, assert every live factory `espn_{prefix}_*` name maps (through `rename_map.yaml`, identity if absent) to exactly one generated function with the same non-(`kwargs`/`return_parsed`/`return_as_pandas`) param names:

```python
import importlib.util
import inspect
from pathlib import Path

import yaml

from tools.codegen import generate

PREFIXES = ["nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb"]  # nhl is additive (new)
RENAME = yaml.safe_load(Path("tools/codegen/rename_map.yaml").read_text()) or {}
SKIP = {"kwargs", "return_parsed", "return_as_pandas"}

def _load(p, n):
    s = importlib.util.spec_from_file_location(n, p)
    m = importlib.util.module_from_spec(s)
    s.loader.exec_module(m)
    return m

def _params(fn):
    return [p for p in inspect.signature(fn).parameters if p not in SKIP]

def test_every_factory_name_maps_to_a_generated_function():
    generate.build()
    for pfx in PREFIXES:
        live = __import__(f"sportsdataverse.{pfx}", fromlist=["x"])
        gen = _load(generate.OUT / f"{pfx}_espn_ext.py", f"_gen_{pfx}")
        live_names = [n for n in dir(live) if n.startswith(f"espn_{pfx}_")]
        for name in live_names:
            new = RENAME.get(name, name)
            assert hasattr(gen, new), f"{name} -> {new} missing in generated {pfx}"
            assert _params(getattr(gen, new)) == _params(getattr(live, name)), f"signature drift: {name}"
```

- [ ] **Step 2: Run + iterate**

Run: `pytest tests/codegen/test_parity_full.py -v`
Expected: PASS. Fix any mismatch by adjusting the YAML (`python_name`, `optional_segment`, `default_from`) or `rename_map.yaml` until green. This is the gate that proves the generated surface reproduces the factory.

- [ ] **Step 3: Commit**

```bash
git add tests/codegen/test_parity_full.py tools/codegen/
git commit -m "test(codegen): full inventory parity — generated ESPN surface matches factory"
```

---

## Task 6: Repoint the runtime `_get` (pure refactor)

**Files:**

- Modify: `sportsdataverse/_common_espn.py`
- Test: existing ESPN tests (`ESPN_TESTS=1`)

- [ ] **Step 1: Replace the body of `_common_espn._get`/`_csv` with re-exports**

In `sportsdataverse/_common_espn.py`, replace the local `_get`/`_csv` definitions (lines ~55-73) with:

```python
from sportsdataverse._codegen_runtime import _csv, _get  # noqa: F401  (single source of truth)
```

- [ ] **Step 2: Run the ESPN parser tests to confirm no behavior change**

Run: `pytest tests/ -k espn -q` (and `ESPN_TESTS=1 pytest tests/test_espn_live.py -q` if running live)
Expected: PASS / unchanged.

- [ ] **Step 3: Commit**

```bash
git add sportsdataverse/_common_espn.py
git commit -m "refactor(espn): source _get/_csv from _codegen_runtime (single impl)"
```

---

## Task 7: Swap generated modules into the live package + manage `__init__`

**Files:**

- Modify: `tools/codegen/generate.py` (`--target live`, `__init__` region), `sportsdataverse/{league}/{prefix}_espn_ext.py` (regenerated), `sportsdataverse/{league}/__init__.py`, `sportsdataverse/nhl/__init__.py`

- [ ] **Step 1: Add `--target live` + `__init__` managed region to `generate.py`**

```python
LIVE = ROOT / "sportsdataverse"
_BEGIN = "# >>> codegen: generated espn wrappers (managed) >>>"
_END = "# <<< codegen <<<"

def _rewrite_init(prefix: str, modules: list[str]) -> None:
    init = LIVE / prefix / "__init__.py"
    text = init.read_text(encoding="utf-8")
    block = _BEGIN + "\n" + "".join(f"from sportsdataverse.{prefix}.{m} import *\n" for m in modules) + _END
    if _BEGIN in text:
        import re
        text = re.sub(_BEGIN + r".*?" + _END, block, text, flags=re.S)
    else:
        text = text.rstrip() + "\n" + block + "\n"
    init.write_text(text, encoding="utf-8")

def build_live() -> None:
    cfg = spec.load_leagues(ENDPOINTS / "leagues.yaml")
    params = spec.load_parameters(ENDPOINTS / "parameters.yaml")
    apis = [spec.load_espn_api(ENDPOINTS / f"{a}.yaml", params) for a in ESPN_APIS]
    for league in cfg.leagues:
        src = _league_module_source(league, apis, cfg.hosts)
        (LIVE / league.prefix / f"{league.prefix}_espn_ext.py").write_text(src, encoding="utf-8")
        _rewrite_init(league.prefix, [f"{league.prefix}_espn_ext"])
    import subprocess
    subprocess.run(["ruff", "format", str(LIVE)], check=False)
```

Wire `--target live` into `main()`.

- [ ] **Step 2: Generate into the live tree**

Run: `python tools/codegen/generate.py --target live`
Expected: rewrites the 7 existing `{prefix}_espn_ext.py` + creates `sportsdataverse/nhl/nhl_espn_ext.py`; updates each `__init__.py` managed region.

- [ ] **Step 3: Convert each league `__init__.py` from the factory to the import**

The 7 existing `{prefix}_espn_ext.py` were 2-line `make_league_module(...)` files — now overwritten with concrete code. The `__init__.py` files already `from .{prefix}_espn_ext import *` (verified). For `nhl/__init__.py`, the managed region now adds `from .nhl_espn_ext import *`.

- [ ] **Step 4: Run the full suite + import check**

Run: `python -c "import sportsdataverse; import sportsdataverse.nhl; print(sportsdataverse.nhl.espn_nhl_scoreboard)"` then `pytest tests/ -q`
Expected: import succeeds; `espn_nhl_*` now exists; suite passes (live ESPN tests gated as before).

- [ ] **Step 5: Commit**

```bash
git add tools/codegen/generate.py sportsdataverse/
git commit -m "feat(espn): generate concrete {league}_espn_ext modules (incl. new espn_nhl_*)"
```

---

## Task 8: Retire the runtime factory

**Files:**

- Modify: `sportsdataverse/_common_espn.py`
- Test: full suite

- [ ] **Step 1: Delete the factory + core fns**

From `sportsdataverse/_common_espn.py` delete: `make_league_module`, `_bind`, `_UNIVERSAL_WRAPPERS`, `_NCAA_WRAPPERS`, `_FOOTBALL_WRAPPERS`, `_MLB_WRAPPERS`, and the 127 `_site_v2_*` / `_espn_*` (web v3) / `_core_v2_*` core functions. **Keep** the host constants (`_SITE_V2` etc.) only if still imported elsewhere (e.g. `extract.py`) — otherwise move them into `extract.py`. Keep nothing else.

- [ ] **Step 2: Confirm nothing imports the deleted names**

Run: `grep -rn "make_league_module\|_UNIVERSAL_WRAPPERS\|_bind\b" sportsdataverse/ tests/`
Expected: no matches (extract.py reads the tables, but it runs against a *pre-retirement* git ref — extraction is one-time; document that extract.py is only runnable before this task, or guard its imports). Re-run `extract.py` is no longer needed post-retirement; mark it as requiring the factory git history.

- [ ] **Step 3: Run the suite**

Run: `pytest tests/ -q && python -c "import sportsdataverse"`
Expected: PASS; import clean.

- [ ] **Step 4: Commit**

```bash
git add sportsdataverse/_common_espn.py
git commit -m "refactor!: retire ESPN runtime factory (make_league_module/_bind/_*_WRAPPERS/core fns)"
```

---

## Task 9: Update tests to canonical names + publish rename map

**Files:**

- Modify: `tests/test_cli.py`, `tests/test_espn_live.py`, `tests/test_qol.py`, `NEWS.md`

- [ ] **Step 1: Replace old names with canonical names in tests**

For each occurrence of a renamed name (e.g. `espn_*_teams_site` → `espn_*_teams`) per `rename_map.yaml`, update the test. Run a guarded sweep:

```bash
python - <<'PY'
import re, yaml, pathlib
rm = yaml.safe_load(open("tools/codegen/rename_map.yaml")) or {}
for f in ["tests/test_cli.py", "tests/test_espn_live.py", "tests/test_qol.py"]:
    t = pathlib.Path(f).read_text()
    for old, new in rm.items():
        t = t.replace(old, new)
    pathlib.Path(f).write_text(t)
print("rewrote", len(rm), "names")
PY
```

- [ ] **Step 2: Run those tests**

Run: `pytest tests/test_qol.py -q` (and `test_cli`/`test_espn_live` with their gates)
Expected: PASS with canonical names.

- [ ] **Step 3: Add the rename map to `NEWS.md`**

Under the current dev version heading in `NEWS.md`, add a `### Breaking — function renames` subsection listing the old→new map (render it from `rename_map.yaml`), noting "no aliases; update call sites once."

- [ ] **Step 4: Full suite + check**

Run: `pytest tests/ -q && python tools/codegen/generate.py --target live --check` (add a `--check` variant of `build_live`)
Expected: PASS; check clean.

- [ ] **Step 5: Commit**

```bash
git add tests/ NEWS.md
git commit -m "test(espn)!: canonical names in tests; publish rename map in NEWS"
```

---

## Self-Review

- **Spec coverage:** phase 2 (runtime repoint) → Task 6; phase 3 (seed ESPN YAML + rename map + parameters) → Task 3; §3.4 irregular constructs → Tasks 1–2; phase 4 (schemas) → Task 4; phase 5 (generate 8 modules incl. espn_nhl + parity + `__init__`) → Tasks 5,7; phase 6 (retire factory) → Task 8; renamed-tests + NEWS → Task 9; §3.1 scopes/exclude_leagues → Task 3 Step 5. Deferred: `parsed.*` (Plan 3), NHL/MLB native (Plan 3), loaders (Plan 4), docs (Plan 5).
- **Placeholder scan:** the two "implementer extends/hand-verifies" notes (Task 3 irregular endpoints; Task 4 description authoring) are inherent data-authoring steps with concrete enumerations (the ~6 functions; the 5 schemas), not vague placeholders. All code steps have complete code.
- **Type consistency:** `_EndpointView` fields (`signature_params`, `has_dynamic_path`, `url_fstring`, `path_build_expr`, `query_params`, `parser`, `fn_name`) match the template reads; `generate.build`/`build_live`/`OUT`/`LIVE`/`ESPN_APIS` consistent across tasks; `rename_map.yaml` produced (Task 3) and consumed (Tasks 5,9) with the same `old→new` shape.
