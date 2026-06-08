<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Codegen Engine Foundation Implementation Plan](#codegen-engine-foundation-implementation-plan)
  - [File Structure](#file-structure)
  - [Task 1: Scaffold package + dev dependencies](#task-1-scaffold-package--dev-dependencies)
  - [Task 2: Runtime helper `_codegen_runtime.py`](#task-2-runtime-helper-_codegen_runtimepy)
  - [Task 3: `spec.py` — data model + YAML loaders](#task-3-specpy--data-model--yaml-loaders)
  - [Task 4: `render.py` — Jinja env + type filters](#task-4-renderpy--jinja-env--type-filters)
  - [Task 5: Templates (`_docstring.jinja` + `espn_league_module.py.jinja`)](#task-5-templates-_docstringjinja--espn_league_modulepyjinja)
  - [Task 6: Seed the vertical-slice YAML + schema](#task-6-seed-the-vertical-slice-yaml--schema)
  - [Task 7: `generate.py` — build the generated modules](#task-7-generatepy--build-the-generated-modules)
  - [Task 8: Golden render test (template stability)](#task-8-golden-render-test-template-stability)
  - [Task 9: Inventory-parity test (generated vs live factory)](#task-9-inventory-parity-test-generated-vs-live-factory)
  - [Task 10: Wire `--check` into pre-commit (drift guard, scoped)](#task-10-wire---check-into-pre-commit-drift-guard-scoped)
  - [Self-Review](#self-review)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Codegen Engine Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the metadata→code generator core and prove it end-to-end on a vertical slice — ESPN Site-v2 (`scoreboard`, `teams`, `standings`) for all 8 leagues — emitting concrete, documented function modules from YAML via Jinja, byte-stable under `--check` and signature-equivalent to the existing runtime factory.

**Architecture:** A new `tools/codegen/` package: `spec.py` (YAML → dataclasses), `render.py` (Jinja env + filters), `templates/` (`_docstring.jinja`, `espn_league_module.py.jinja`), `generate.py` (CLI: build / `--check`). Generated code calls a small hand-written runtime helper `sportsdataverse/_codegen_runtime.py` (`_get`/`_csv`, extracted verbatim from `_common_espn.py`). **Plan 1 writes generated modules to a parallel `tools/codegen/_generated/` directory — the live package is NOT modified** (the swap + factory retirement is Plan 2). A parity test imports both the generated module and the live factory module and asserts the public function signatures match, so the engine is proven correct before anything is swapped.

**Tech Stack:** Python ≥3.10, PyYAML, Jinja2, pytest, ruff. Reuses existing `sportsdataverse.dl_utils.download` and `sportsdataverse._common_espn_parsers`.

---

## File Structure

**Create:**

- `tools/codegen/__init__.py` — empty package marker.
- `tools/codegen/spec.py` — dataclasses (`Param`, `Endpoint`, `EspnApi`, `League`, `Hosts`) + YAML loaders + validation.
- `tools/codegen/render.py` — Jinja `Environment` + filters (`type_hint`, `py_repr`) + `render_espn_league_module()`.
- `tools/codegen/generate.py` — CLI entrypoint (`build` default, `--check`, `--only`).
- `tools/codegen/templates/_docstring.jinja` — shared docstring macro.
- `tools/codegen/templates/espn_league_module.py.jinja` — ESPN league module template.
- `tools/codegen/endpoints/leagues.yaml` — hosts + league slug map.
- `tools/codegen/endpoints/parameters.yaml` — shared parameter registry.
- `tools/codegen/endpoints/espn_site_v2.yaml` — the 3 slice endpoints.
- `tools/codegen/schemas/scoreboard.yaml` — column table for `parse_scoreboard`.
- `sportsdataverse/_codegen_runtime.py` — `_get` / `_csv` runtime helper (generated code's call target).
- `tests/codegen/__init__.py`
- `tests/codegen/test_spec.py` — loader/validation unit tests.
- `tests/codegen/test_render.py` — filter + golden render tests.
- `tests/codegen/test_generate.py` — build + `--check` tests.
- `tests/codegen/test_parity.py` — generated-vs-factory signature parity.

**Modify:**

- `pyproject.toml` — add `pyyaml`, `jinja2` to `[dependency-groups] dev`.

Each file has one responsibility: `spec.py` = data model, `render.py` = templating, `generate.py` = orchestration/CLI, `_codegen_runtime.py` = HTTP. Generated output lives only under `tools/codegen/_generated/` in this plan.

---

## Task 1: Scaffold package + dev dependencies

**Files:**

- Create: `tools/codegen/__init__.py`, `tools/codegen/templates/` (dir), `tools/codegen/endpoints/` (dir), `tools/codegen/schemas/` (dir), `tests/codegen/__init__.py`
- Modify: `pyproject.toml` (`[dependency-groups] dev`)

- [ ] **Step 1: Create the package + test dirs**

```bash
mkdir -p tools/codegen/templates tools/codegen/endpoints tools/codegen/schemas tests/codegen
printf '"""sportsdataverse API codegen (build-time only; not shipped)."""\n' > tools/codegen/__init__.py
printf '' > tests/codegen/__init__.py
```

- [ ] **Step 2: Add dev deps to pyproject.toml**

In `pyproject.toml`, locate `[dependency-groups]` → `dev = [` (around line 177) and add these two entries to the list (keep alphabetical with the existing entries):

```toml
    "jinja2>=3.1",
    "pyyaml>=6.0",
```

- [ ] **Step 3: Sync the dev environment**

Run: `uv sync --group dev`
Expected: resolves and installs `jinja2` and `pyyaml` (no errors).

- [ ] **Step 4: Verify imports**

Run: `python -c "import jinja2, yaml; print(jinja2.__version__, yaml.__version__)"`
Expected: prints two version strings.

- [ ] **Step 5: Commit**

```bash
git add tools/codegen/__init__.py tests/codegen/__init__.py pyproject.toml uv.lock
git commit -m "chore(codegen): scaffold tools/codegen package + jinja2/pyyaml dev deps"
```

---

## Task 2: Runtime helper `_codegen_runtime.py`

**Files:**

- Create: `sportsdataverse/_codegen_runtime.py`
- Test: `tests/codegen/test_runtime.py`

- [ ] **Step 1: Write the failing test**

`tests/codegen/test_runtime.py`:

```python
from unittest.mock import patch

from sportsdataverse._codegen_runtime import _csv, _get

def test_csv_joins_iterables_and_passes_scalars():
    assert _csv([1, 2, 3]) == "1,2,3"
    assert _csv(("a", "b")) == "a,b"
    assert _csv("x") == "x"
    assert _csv(None) is None

def test_get_strips_none_params_and_returns_json():
    class FakeResp:
        def json(self):
            return {"ok": True}

    with patch("sportsdataverse._codegen_runtime.download", return_value=FakeResp()) as dl:
        out = _get("https://example.test/x", params={"a": 1, "b": None})
    assert out == {"ok": True}
    # None-valued params are stripped before the request
    assert dl.call_args.kwargs["params"] == {"a": 1}

def test_get_returns_empty_dict_on_download_none():
    with patch("sportsdataverse._codegen_runtime.download", return_value=None):
        assert _get("https://example.test/x") == {}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/codegen/test_runtime.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'sportsdataverse._codegen_runtime'`

- [ ] **Step 3: Write the implementation**

`sportsdataverse/_codegen_runtime.py` (extracted verbatim from `_common_espn.py:55-73`, the canonical `_get`/`_csv`):

```python
"""Runtime helpers for codegen-emitted wrappers (HTTP + value coercion).

Hand-written and stable; generated modules import ``_get`` / ``_csv`` from here so
the ~1,000 generated functions share one tested HTTP path instead of inlining it.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from sportsdataverse.dl_utils import download

def _get(url: str, params: Optional[dict] = None, **kwargs) -> Dict:
    """GET ``url`` as JSON. Returns ``{}`` on failure. Strips ``None`` params."""
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    resp = download(url=url, params=clean, **kwargs)
    if resp is None:
        return {}
    try:
        return resp.json()
    except Exception:
        return {}

def _csv(values: Any) -> Optional[str]:
    """Join an iterable into a comma-separated string; pass scalar / None through."""
    if values is None:
        return None
    if isinstance(values, (list, tuple, set)):
        return ",".join(str(v) for v in values)
    return str(values)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/codegen/test_runtime.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add sportsdataverse/_codegen_runtime.py tests/codegen/test_runtime.py
git commit -m "feat(codegen): add _codegen_runtime (_get/_csv) for generated wrappers"
```

---

## Task 3: `spec.py` — data model + YAML loaders

**Files:**

- Create: `tools/codegen/spec.py`
- Test: `tests/codegen/test_spec.py`

- [ ] **Step 1: Write the failing test**

`tests/codegen/test_spec.py`:

```python
from pathlib import Path

from tools.codegen import spec

ENDPOINTS = Path("tools/codegen/endpoints")

def test_load_parameters_registry():
    params = spec.load_parameters(ENDPOINTS / "parameters.yaml")
    p = params["limit"]
    assert p.python_name == "limit"
    assert p.api == "limit"
    assert p.default == 500

def test_load_leagues_resolves_hosts_and_scopes():
    cfg = spec.load_leagues(ENDPOINTS / "leagues.yaml")
    assert cfg.hosts["site_v2"].startswith("https://site.api.espn.com")
    nba = next(lg for lg in cfg.leagues if lg.prefix == "nba")
    assert nba.sport == "basketball" and nba.league == "nba"
    assert "universal" in nba.scopes

def test_load_espn_api_resolves_param_keys_and_validates_path_tokens():
    cfg = spec.load_leagues(ENDPOINTS / "leagues.yaml")
    params = spec.load_parameters(ENDPOINTS / "parameters.yaml")
    api = spec.load_espn_api(ENDPOINTS / "espn_site_v2.yaml", params)
    sb = next(e for e in api.endpoints if e.short == "scoreboard")
    assert sb.scope == "universal"
    assert sb.parser == "parse_scoreboard"
    # query params resolved from the registry to Param objects
    names = {qp.python_name for qp in sb.query_params}
    assert {"dates", "limit", "season_type"} <= names
    # season_type carries its wire key override
    st = next(qp for qp in sb.query_params if qp.python_name == "season_type")
    assert st.api == "seasontype"

def test_validate_rejects_unknown_param_key(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        "api: x\nhost: site_v2\nname_pattern: 'espn_{prefix}_{short}'\n"
        "endpoints:\n  - short: foo\n    path: '/{sport}/{league}/foo'\n"
        "    params: [does_not_exist]\n",
        encoding="utf-8",
    )
    params = spec.load_parameters(ENDPOINTS / "parameters.yaml")
    try:
        spec.load_espn_api(bad, params)
        assert False, "expected SpecError"
    except spec.SpecError as e:
        assert "does_not_exist" in str(e)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/codegen/test_spec.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'tools.codegen.spec'` (and the YAML files don't exist yet — Task 6 creates them; this task creates `spec.py`, Task 6 the data, and the test passes after Task 6. To keep the task self-contained, the assertions that read real YAML are exercised in Task 6 Step 4.)

> Note: `spec.py` is implemented here; the registry/leagues/api YAML it loads are created in Task 6. Run the unit-only assertions now with inline fixtures; the file-backed assertions go green at the end of Task 6.

- [ ] **Step 3: Write the implementation**

`tools/codegen/spec.py`:

```python
"""YAML endpoint specs → typed dataclasses (the codegen data model)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import yaml

class SpecError(ValueError):
    """Raised when a YAML spec is malformed or internally inconsistent."""

@dataclass(frozen=True)
class Param:
    python_name: str
    api: str                      # wire/query key
    type: str = "str"             # e.g. "int", "int|str"
    required: bool = False
    default: object = None
    pattern: Optional[str] = None  # regex for docs/validation
    is_query: bool = True          # query vs path

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

@dataclass(frozen=True)
class EspnApi:
    api: str
    host: str
    name_pattern: str
    endpoints: List[Endpoint]

@dataclass(frozen=True)
class League:
    prefix: str
    sport: str
    league: str
    scopes: List[str]

@dataclass(frozen=True)
class LeaguesConfig:
    hosts: Dict[str, str]
    leagues: List[League]

_PATH_TOKEN = re.compile(r"\{(\w+)\}")

def _read_yaml(path: Path) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))

def load_parameters(path: Path) -> Dict[str, Param]:
    raw = _read_yaml(path)["params"]
    out: Dict[str, Param] = {}
    for key, v in raw.items():
        out[key] = Param(
            python_name=key,
            api=v.get("api", key),
            type=v.get("type", "str"),
            required=v.get("required", False),
            default=v.get("default"),
            pattern=v.get("pattern"),
            is_query=v.get("is_query", True),
        )
    return out

def load_leagues(path: Path) -> LeaguesConfig:
    raw = _read_yaml(path)
    leagues = [
        League(prefix=l["prefix"], sport=l["sport"], league=l["league"], scopes=list(l["scopes"]))
        for l in raw["leagues"]
    ]
    return LeaguesConfig(hosts=dict(raw["hosts"]), leagues=leagues)

def _resolve_param(name: str, registry: Dict[str, Param], src: Path) -> Param:
    if name not in registry:
        raise SpecError(f"{src}: endpoint references unknown parameter key {name!r}")
    return registry[name]

def load_espn_api(path: Path, registry: Dict[str, Param]) -> EspnApi:
    raw = _read_yaml(path)
    endpoints: List[Endpoint] = []
    for e in raw["endpoints"]:
        qps = [_resolve_param(k, registry, path) for k in e.get("params", [])]
        for extra in e.get("extra_params", []):
            qps.append(
                Param(
                    python_name=extra["name"],
                    api=extra.get("query_key", extra["name"]),
                    type=extra.get("type", "str"),
                    required=extra.get("required", False),
                    default=extra.get("default"),
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
            example_args=e.get("example_args", {}) or {},
        )
        # validate path tokens (excluding the {sport}/{league} slugs) have a known param
        tokens = set(_PATH_TOKEN.findall(ep.path)) - {"sport", "league"}
        known = {p.python_name for p in ep.path_params} | set(registry)
        missing = tokens - known
        if missing:
            raise SpecError(f"{path}: endpoint {ep.short!r} path token(s) {missing} have no param")
        endpoints.append(ep)
    return EspnApi(api=raw["api"], host=raw["host"], name_pattern=raw["name_pattern"], endpoints=endpoints)
```

- [ ] **Step 4: Run the unit-only test**

Run: `pytest tests/codegen/test_spec.py::test_validate_rejects_unknown_param_key -v`
Expected: PASS (this test uses an inline tmp YAML + the real `parameters.yaml`; if `parameters.yaml` doesn't exist yet, this step is deferred to Task 6 Step 4).

- [ ] **Step 5: Commit**

```bash
git add tools/codegen/spec.py tests/codegen/test_spec.py
git commit -m "feat(codegen): spec.py data model + YAML loaders with validation"
```

---

## Task 4: `render.py` — Jinja env + type filters

**Files:**

- Create: `tools/codegen/render.py`
- Test: `tests/codegen/test_render.py`

- [ ] **Step 1: Write the failing test**

`tests/codegen/test_render.py`:

```python
from tools.codegen import render

def test_type_hint_maps_union_and_scalars():
    assert render.type_hint("int") == "int"
    assert render.type_hint("str") == "str"
    assert render.type_hint("int|str") == "Union[int, str]"

def test_py_repr_quotes_strings_and_passes_numbers():
    assert render.py_repr("a") == "'a'"
    assert render.py_repr(500) == "500"
    assert render.py_repr(None) == "None"

def test_env_renders_a_trivial_template():
    out = render.ENV.from_string("hi {{ name }}").render(name="x")
    assert out == "hi x"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/codegen/test_render.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'tools.codegen.render'`

- [ ] **Step 3: Write the implementation**

`tools/codegen/render.py`:

```python
"""Jinja environment + filters for code generation."""

from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined

_TEMPLATES = Path(__file__).parent / "templates"

def type_hint(t: str) -> str:
    """Map a spec type string to a Python annotation. 'int|str' -> 'Union[int, str]'."""
    parts = [p.strip() for p in t.split("|")]
    if len(parts) == 1:
        return parts[0]
    return f"Union[{', '.join(parts)}]"

def py_repr(value) -> str:
    """Render a default value as a Python literal."""
    return repr(value)

ENV = Environment(
    loader=FileSystemLoader(str(_TEMPLATES)),
    trim_blocks=True,
    lstrip_blocks=True,
    keep_trailing_newline=True,
    undefined=StrictUndefined,
)
ENV.filters["type_hint"] = type_hint
ENV.filters["py_repr"] = py_repr
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/codegen/test_render.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add tools/codegen/render.py tests/codegen/test_render.py
git commit -m "feat(codegen): render.py Jinja env + type_hint/py_repr filters"
```

---

## Task 5: Templates (`_docstring.jinja` + `espn_league_module.py.jinja`)

**Files:**

- Create: `tools/codegen/templates/_docstring.jinja`, `tools/codegen/templates/espn_league_module.py.jinja`

- [ ] **Step 1: Write `_docstring.jinja`**

`tools/codegen/templates/_docstring.jinja`:

```jinja
{% macro docstring(ep, sport, league, host_url) -%}
    """{{ ep.summary }}

    Bound to sport={{ sport!r }}, league={{ league!r }}.

    Endpoint: ``GET {{ host_url }}{{ ep.path }}``
{% if ep.example_url %}    Example URL: {{ ep.example_url }}
{% endif %}
    Args:
{% for p in ep.query_params %}        {{ p.python_name }}: {{ p.api }} query parameter.
{% endfor %}
{% if ep.parser %}        return_parsed: dispatch through {{ ep.parser }} -> polars DataFrame.
        return_as_pandas: with return_parsed, return pandas instead of polars.
{% endif %}
    Returns:
        {% if ep.parser %}polars/pandas DataFrame when return_parsed=True, else raw Dict.{% else %}Raw JSON Dict.{% endif %}

    Example:
        >>> {{ ep.example_call }}
    """
{%- endmacro %}
```

- [ ] **Step 2: Write `espn_league_module.py.jinja`**

`tools/codegen/templates/espn_league_module.py.jinja`:

```jinja
{% import "_docstring.jinja" as d %}
# GENERATED by tools/codegen/generate.py — DO NOT EDIT.
# Source: tools/codegen/endpoints/*.yaml
"""ESPN endpoint wrappers for {{ prefix|upper }} ({{ sport }}/{{ league }}). Generated."""
from __future__ import annotations

from typing import Dict, Optional, Union

from sportsdataverse._codegen_runtime import _get
{% if parser_imports %}from sportsdataverse._common_espn_parsers import {{ parser_imports|join(", ") }}
{% endif %}

_HOST = "{{ host_url }}"

__all__ = [
{% for ep in endpoints %}    "{{ ep.fn_name }}",
{% endfor %}]

{% for ep in endpoints %}

def {{ ep.fn_name }}(
{% for p in ep.query_params %}    {{ p.python_name }}: Optional[{{ p.type|type_hint }}] = {{ p.default|py_repr }},
{% endfor %}
    *,
{% if ep.parser %}    return_parsed: bool = False,
    return_as_pandas: bool = False,
{% endif %}
    **kwargs,
) -> Dict:
{{ d.docstring(ep, sport, league, host_url) }}
    raw = _get(
        f"{_HOST}{{ ep.resolved_path }}",
        params={
{% for p in ep.query_params %}            "{{ p.api }}": {{ p.python_name }},
{% endfor %}        },
        **kwargs,
    )
{% if ep.parser %}    if return_parsed:
        return {{ ep.parser }}(raw, return_as_pandas=return_as_pandas)
{% endif %}    return raw
{% endfor %}
```

- [ ] **Step 3: Sanity-render (no separate test file; covered by Task 8 golden test)**

Run: `python -c "from tools.codegen import render; print(render.ENV.get_template('espn_league_module.py.jinja') is not None)"`
Expected: prints `True` (template parses/loads).

- [ ] **Step 4: Commit**

```bash
git add tools/codegen/templates/_docstring.jinja tools/codegen/templates/espn_league_module.py.jinja
git commit -m "feat(codegen): shared _docstring macro + espn_league_module template"
```

---

## Task 6: Seed the vertical-slice YAML + schema

**Files:**

- Create: `tools/codegen/endpoints/leagues.yaml`, `tools/codegen/endpoints/parameters.yaml`, `tools/codegen/endpoints/espn_site_v2.yaml`, `tools/codegen/schemas/scoreboard.yaml`

- [ ] **Step 1: `leagues.yaml`**

```yaml
hosts:
  site_v2:     "https://site.api.espn.com/apis/site/v2/sports"
  site_v2_alt: "https://site.api.espn.com/apis/v2/sports"

leagues:
  - { prefix: nba,  sport: basketball, league: nba,                        scopes: [universal] }
  - { prefix: wnba, sport: basketball, league: wnba,                       scopes: [universal] }
  - { prefix: mbb,  sport: basketball, league: mens-college-basketball,    scopes: [universal, ncaa] }
  - { prefix: wbb,  sport: basketball, league: womens-college-basketball,  scopes: [universal, ncaa] }
  - { prefix: cfb,  sport: football,   league: college-football,           scopes: [universal, ncaa, football] }
  - { prefix: nfl,  sport: football,   league: nfl,                        scopes: [universal, football] }
  - { prefix: mlb,  sport: baseball,   league: mlb,                        scopes: [universal, mlb] }
  - { prefix: nhl,  sport: hockey,     league: nhl,                        scopes: [universal] }
```

- [ ] **Step 2: `parameters.yaml`**

```yaml
params:
  dates:       { api: dates,      type: "int|str" }
  week:        { api: week,       type: int }
  season_type: { api: seasontype, type: int }
  groups:      { api: groups,     type: "int|str" }
  limit:       { api: limit,      type: int, default: 500 }
  season:      { api: season,     type: "int|str" }
  group:       { api: group,      type: "int|str" }
  standings_type: { api: type,    type: str }
```

- [ ] **Step 3: `espn_site_v2.yaml`** (the 3-endpoint slice)

```yaml
api: espn_site_v2
host: site_v2
name_pattern: "espn_{prefix}_{short}"

endpoints:
  - short: scoreboard
    scope: universal
    summary: "GET /scoreboard. `dates`: YYYYMMDD, YYYYMMDD-YYYYMMDD, or a season year."
    path: "/{sport}/{league}/scoreboard"
    params: [dates, week, season_type, groups, limit]
    parser: parse_scoreboard
    returns_schema: scoreboard
    example_args: { dates: "20240115" }

  - short: teams
    scope: universal
    summary: "GET /teams — all teams."
    path: "/{sport}/{league}/teams"
    params: [limit]
    parser: parse_teams
    example_args: {}

  - short: standings
    scope: universal
    host: site_v2_alt
    summary: "GET /standings — full standings (not the site-v2 stub)."
    path: "/{sport}/{league}/standings"
    params: [season, group, standings_type]
    parser: parse_standings
    example_args: { season: 2024 }
```

- [ ] **Step 4: `schemas/scoreboard.yaml`** (stub — descriptions filled later)

```yaml
schema: scoreboard
kind: dataframe
description: "One row per event on the scoreboard for the requested date(s)."
columns:
  - { name: game_id,   type: integer,   description: "ESPN event id." }
  - { name: season,    type: integer,   description: "Four-digit season year." }
  - { name: game_date, type: character, description: "ISO 8601 kickoff timestamp (UTC)." }
```

- [ ] **Step 5: Run the spec tests against the real YAML**

Run: `pytest tests/codegen/test_spec.py -v`
Expected: PASS (4 passed — the file-backed loaders now resolve).

- [ ] **Step 6: Commit**

```bash
git add tools/codegen/endpoints/ tools/codegen/schemas/
git commit -m "feat(codegen): seed site_v2 vertical-slice YAML + scoreboard schema"
```

---

## Task 7: `generate.py` — build the generated modules

**Files:**

- Create: `tools/codegen/generate.py`
- Test: `tests/codegen/test_generate.py`

- [ ] **Step 1: Write the failing test**

`tests/codegen/test_generate.py`:

```python
import importlib.util
from pathlib import Path
from unittest.mock import patch

from tools.codegen import generate

OUT = Path("tools/codegen/_generated")

def _load(mod_path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, mod_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def test_build_emits_one_module_per_league():
    generate.build()
    for prefix in ("nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb", "nhl"):
        assert (OUT / f"{prefix}_espn_ext.py").exists()

def test_generated_nba_module_imports_and_exposes_functions():
    generate.build()
    mod = _load(OUT / "nba_espn_ext.py", "_gen_nba")
    assert hasattr(mod, "espn_nba_scoreboard")
    assert hasattr(mod, "espn_nba_teams")
    assert hasattr(mod, "espn_nba_standings")
    assert "espn_nba_scoreboard" in mod.__all__

def test_generated_function_builds_correct_url_and_strips_none():
    generate.build()
    mod = _load(OUT / "nba_espn_ext.py", "_gen_nba2")

    class FakeResp:
        def json(self):
            return {"events": []}

    with patch("sportsdataverse._codegen_runtime.download", return_value=FakeResp()) as dl:
        out = mod.espn_nba_scoreboard(dates="20240115")
    assert out == {"events": []}
    called = dl.call_args.kwargs
    assert called["url"] == "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
    assert called["params"] == {"dates": "20240115", "limit": 500}  # None week/seasontype/groups stripped

def test_standings_uses_alt_host():
    generate.build()
    mod = _load(OUT / "nba_espn_ext.py", "_gen_nba3")

    class FakeResp:
        def json(self):
            return {}

    with patch("sportsdataverse._codegen_runtime.download", return_value=FakeResp()) as dl:
        mod.espn_nba_standings(season=2024)
    assert dl.call_args.kwargs["url"] == "https://site.api.espn.com/apis/v2/sports/basketball/nba/standings"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/codegen/test_generate.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'tools.codegen.generate'`

- [ ] **Step 3: Write the implementation**

`tools/codegen/generate.py`:

```python
"""Codegen CLI: render concrete ESPN league modules from YAML (build / --check)."""

from __future__ import annotations

import argparse
import sys
from dataclasses import replace
from pathlib import Path
from typing import List
from urllib.parse import urlencode

from tools.codegen import render, spec

ROOT = Path(__file__).resolve().parents[2]
ENDPOINTS = ROOT / "tools" / "codegen" / "endpoints"
OUT = ROOT / "tools" / "codegen" / "_generated"

ESPN_APIS = ["espn_site_v2"]  # extended in Plan 2

def _example_url(host_url: str, ep: spec.Endpoint, sport: str, league: str) -> str:
    path = ep.path.format(sport=sport, league=league)
    qs = {}
    for p in ep.query_params:
        if p.python_name in ep.example_args:
            qs[p.api] = ep.example_args[p.python_name]
    return f"{host_url}{path}" + (f"?{urlencode(qs)}" if qs else "")

def _example_call(ep: spec.Endpoint, fn_name: str) -> str:
    args = ", ".join(f"{k}={v!r}" for k, v in ep.example_args.items())
    return f"{fn_name}({args})"

def _league_module_source(league: spec.League, apis, hosts) -> str:
    endpoints = []
    parser_imports = set()
    for api in apis:
        host_url = hosts[api.host]
        for ep in api.endpoints:
            if ep.scope not in league.scopes:
                continue
            ep_host = hosts[ep.host] if ep.host else host_url
            fn_name = api.name_pattern.format(prefix=league.prefix, short=ep.short)
            if ep.parser:
                parser_imports.add(ep.parser)
            endpoints.append(
                replace(
                    ep,
                    # attach render-only attributes via a dict the template reads
                ).__class__(  # keep dataclass; stash computed fields on a SimpleNamespace-like dict
                    **{**ep.__dict__}
                )
                if False
                else _EndpointView(ep, fn_name, ep_host, league)
            )
    template = render.ENV.get_template("espn_league_module.py.jinja")
    # all endpoints in this slice share one host for the _HOST constant; alt-host
    # endpoints embed their absolute host in resolved_path's f-string prefix instead.
    base_host = hosts[apis[0].host]
    src = template.render(
        prefix=league.prefix,
        sport=league.sport,
        league=league.league,
        host_url=base_host,
        endpoints=endpoints,
        parser_imports=sorted(parser_imports),
    )
    return src

class _EndpointView:
    """Template-facing view of an Endpoint with computed render fields."""

    def __init__(self, ep: spec.Endpoint, fn_name: str, ep_host: str, league: spec.League):
        self.short = ep.short
        self.summary = ep.summary
        self.query_params = ep.query_params
        self.parser = ep.parser
        self.fn_name = fn_name
        self.example_args = ep.example_args
        # resolved_path is an absolute-url suffix so alt-host endpoints render correctly:
        # template does f"{_HOST}{resolved_path}" — for alt host we override _HOST per call by
        # embedding the full path. Simpler: resolved_path carries the full path; if ep_host
        # differs from the module _HOST, we prepend the difference via an absolute literal.
        path = ep.path.format(sport=league.sport, league=league.league)
        self._ep_host = ep_host
        self.resolved_path = path
        self.host_url = ep_host
        self.path = ep.path
        self.example_url = _example_url(ep_host, ep, league.sport, league.league)
        self.example_call = _example_call(ep, fn_name)
```

> **Design note for the implementer:** the alt-host (`standings`) case means a single `_HOST`
> constant per module is insufficient. Resolve this cleanly in Step 3a below before running tests.

- [ ] **Step 3a: Fix the alt-host rendering (make `_HOST` per-endpoint)**

The simplest correct approach: the template emits the **full absolute URL literal per function** (no shared `_HOST`). Replace the URL line in `espn_league_module.py.jinja` Step-2 template:

Change:

```jinja
    raw = _get(
        f"{_HOST}{{ ep.resolved_path }}",
```

to:

```jinja
    raw = _get(
        "{{ ep.full_url }}",
```

and remove the `_HOST = "..."` line from the template. Then in `_EndpointView.__init__`, set:

```python
        self.full_url = f"{ep_host}{path}"
```

This makes each function embed its own absolute URL (correct for both site_v2 and site_v2_alt). Re-render is covered by the tests below.

- [ ] **Step 3b: Add `build()` / `check()` / `main()` to `generate.py`**

Append to `tools/codegen/generate.py`:

```python
def _render_all() -> dict[str, str]:
    cfg = spec.load_leagues(ENDPOINTS / "leagues.yaml")
    params = spec.load_parameters(ENDPOINTS / "parameters.yaml")
    apis = [spec.load_espn_api(ENDPOINTS / f"{a}.yaml", params) for a in ESPN_APIS]
    out: dict[str, str] = {}
    for league in cfg.leagues:
        out[f"{league.prefix}_espn_ext.py"] = _league_module_source(league, apis, cfg.hosts)
    return out

def build() -> List[Path]:
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "__init__.py").write_text("", encoding="utf-8")
    written = []
    for name, src in _render_all().items():
        path = OUT / name
        path.write_text(src, encoding="utf-8")
        written.append(path)
    return written

def check() -> int:
    stale = []
    for name, src in _render_all().items():
        path = OUT / name
        if not path.exists() or path.read_text(encoding="utf-8") != src:
            stale.append(name)
    if stale:
        print("codegen --check: stale/missing generated files:", ", ".join(sorted(stale)), file=sys.stderr)
        return 1
    print("codegen --check: all generated files current")
    return 0

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="generate.py")
    ap.add_argument("--check", action="store_true", help="fail if any generated file is stale")
    args = ap.parse_args(argv)
    if args.check:
        return check()
    build()
    print(f"codegen: wrote {len(list(OUT.glob('*_espn_ext.py')))} modules to {OUT}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 3c: Simplify `_league_module_source`**

Replace the convoluted endpoints-building loop in `_league_module_source` with the clean version (remove the dead `if False` branch):

```python
def _league_module_source(league: spec.League, apis, hosts) -> str:
    endpoints = []
    parser_imports = set()
    for api in apis:
        host_url = hosts[api.host]
        for ep in api.endpoints:
            if ep.scope not in league.scopes:
                continue
            ep_host = hosts[ep.host] if ep.host else host_url
            fn_name = api.name_pattern.format(prefix=league.prefix, short=ep.short)
            if ep.parser:
                parser_imports.add(ep.parser)
            endpoints.append(_EndpointView(ep, fn_name, ep_host, league))
    template = render.ENV.get_template("espn_league_module.py.jinja")
    return template.render(
        prefix=league.prefix,
        sport=league.sport,
        league=league.league,
        endpoints=endpoints,
        parser_imports=sorted(parser_imports),
    )
```

And update the template's module docstring/header to not reference `_HOST` (removed in 3a). The `__all__` + per-function `full_url` are all the template needs.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/codegen/test_generate.py -v`
Expected: PASS (5 passed)

- [ ] **Step 5: Verify generated code is ruff-clean**

Run: `ruff format tools/codegen/_generated/ && ruff check tools/codegen/_generated/`
Expected: format makes no changes after a second run; check passes. (If `ruff format` changes files, make `build()` run `ruff format` on `OUT` at the end so `--check` and the formatter agree — add `subprocess.run(["ruff", "format", str(OUT)], check=False)` to `build()`.)

- [ ] **Step 6: Commit**

```bash
git add tools/codegen/generate.py tests/codegen/test_generate.py tools/codegen/templates/espn_league_module.py.jinja tools/codegen/_generated/
git commit -m "feat(codegen): generate.py build/--check rendering ESPN league modules"
```

---

## Task 8: Golden render test (template stability)

**Files:**

- Test: `tests/codegen/test_render.py` (extend)

- [ ] **Step 1: Add the golden test**

Append to `tests/codegen/test_render.py`:

```python
import ast
from pathlib import Path

from tools.codegen import generate

def test_generated_modules_are_valid_python_with_all_and_defs():
    for name, src in generate._render_all().items():
        tree = ast.parse(src)  # raises SyntaxError if malformed
        funcs = {n.name for n in tree.body if isinstance(n, ast.FunctionDef)}
        assert funcs, f"{name} has no functions"
        assert "__all__" in src
        # every function name is exported
        for fn in funcs:
            assert f'"{fn}"' in src, f"{fn} missing from __all__ in {name}"
```

- [ ] **Step 2: Run test to verify it passes**

Run: `pytest tests/codegen/test_render.py -v`
Expected: PASS (4 passed)

- [ ] **Step 3: Commit**

```bash
git add tests/codegen/test_render.py
git commit -m "test(codegen): golden test — generated modules parse + export all defs"
```

---

## Task 9: Inventory-parity test (generated vs live factory)

**Files:**

- Test: `tests/codegen/test_parity.py`

- [ ] **Step 1: Write the parity test**

`tests/codegen/test_parity.py` — asserts the generated functions match the live factory's public signature for the slice endpoints (proving the engine reproduces current behavior before any swap):

```python
import importlib.util
import inspect
from pathlib import Path

import sportsdataverse.nba as live_nba
from tools.codegen import generate

OUT = Path("tools/codegen/_generated")
SLICE = ["espn_nba_scoreboard", "espn_nba_teams", "espn_nba_standings"]

def _load(mod_path, name):
    spec = importlib.util.spec_from_file_location(name, mod_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def test_generated_nba_param_names_match_live_factory():
    generate.build()
    gen = _load(OUT / "nba_espn_ext.py", "_gen_parity")
    for fn_name in SLICE:
        assert hasattr(live_nba, fn_name), f"live factory missing {fn_name}"
        live_params = [
            p for p in inspect.signature(getattr(live_nba, fn_name)).parameters
            if p not in ("kwargs", "return_parsed", "return_as_pandas")
        ]
        gen_params = [
            p for p in inspect.signature(getattr(gen, fn_name)).parameters
            if p not in ("kwargs", "return_parsed", "return_as_pandas")
        ]
        assert gen_params == live_params, f"{fn_name}: {gen_params} != {live_params}"
```

- [ ] **Step 2: Run test to verify it passes**

Run: `pytest tests/codegen/test_parity.py -v`
Expected: PASS. (If a param-name mismatch surfaces — e.g. the live factory names a kwarg differently — fix the corresponding `parameters.yaml` `python_name`/`api` so the generated signature matches, then re-run. This is the intended feedback loop.)

- [ ] **Step 3: Commit**

```bash
git add tests/codegen/test_parity.py
git commit -m "test(codegen): parity — generated nba signatures match live factory"
```

---

## Task 10: Wire `--check` into pre-commit (drift guard, scoped)

**Files:**

- Modify: `.pre-commit-config.yaml`

- [ ] **Step 1: Add the local codegen hook**

In `.pre-commit-config.yaml`, append a `local` repo block:

```yaml
  - repo: local
    hooks:
      - id: sdv-codegen
        name: regenerate API wrappers from endpoint YAML
        entry: python tools/codegen/generate.py
        language: system
        pass_filenames: false
        files: ^tools/codegen/(endpoints|schemas|templates)/.*$
```

- [ ] **Step 2: Verify the hook regenerates on input change**

Run: `pre-commit run sdv-codegen --files tools/codegen/endpoints/espn_site_v2.yaml`
Expected: hook runs `generate.py`; if `_generated/` is current, exits clean (no changes). If you edit a YAML and re-run, it rewrites `_generated/` and the hook reports modified files (commit fails until re-added) — the intended drift guard.

- [ ] **Step 3: Verify `--check` passes on a clean tree**

Run: `python tools/codegen/generate.py --check`
Expected: prints `codegen --check: all generated files current`, exit 0.

- [ ] **Step 4: Full codegen test suite green**

Run: `pytest tests/codegen/ -v`
Expected: PASS (all tasks' tests).

- [ ] **Step 5: Commit**

```bash
git add .pre-commit-config.yaml
git commit -m "ci(codegen): pre-commit hook regenerates wrappers on YAML/template change"
```

---

## Self-Review

- **Spec coverage (Plan 1 scope = engine + slice):** spec §3 (metadata: leagues/parameters/endpoints/schemas YAML) → Tasks 3, 6; §3.8 parameter registry → Task 6 `parameters.yaml` + `spec.load_parameters`; §4 generator (spec→render→build/--check) → Tasks 3,4,7; §5.1 generated ESPN module shape → Task 5 template + Task 7; §5.2 runtime helper → Task 2; §7 golden + parity + import tests → Tasks 8,9; §12 pre-commit drift → Task 10. Out of Plan-1 scope (deferred to Plans 2–5, by design): web_v3/core_v2/scopes fan-out, factory retirement, `parsed.*`, NHL/MLB, loaders, docs site, notebooks, the full `@return`/Valid-URL/param-table docs rendering (the docstring macro here is the minimal version; the full 8-section macro lands with the docs plan).
- **Placeholder scan:** no TBD/TODO; every code step has complete code. The one design subtlety (alt-host `_HOST`) is resolved concretely in Steps 3a/3c, not left vague.
- **Type consistency:** `Param.python_name`/`Param.api` used consistently across `spec.py`, the template, and tests; `generate.build()`/`generate.check()`/`generate._render_all()` names match across Tasks 7–10; `_EndpointView` attributes (`fn_name`, `full_url`, `query_params`, `parser`, `example_url`, `example_call`) match the template's field reads after Steps 3a/3c.
