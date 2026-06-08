<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [404-Safe Dataset Loaders Implementation Plan](#404-safe-dataset-loaders-implementation-plan)
  - [File Structure](#file-structure)
  - [Task 1: `_read_release_parquet` 404-safe helper](#task-1-_read_release_parquet-404-safe-helper)
  - [Task 2: `Loader` spec + `load_module.py.jinja`](#task-2-loader-spec--load_modulepyjinja)
  - [Task 3: Seed `releases.yaml` (`extract.py --releases`)](#task-3-seed-releasesyaml-extractpy---releases)
  - [Task 4: Loader `@return` schemas (WNBA/PWHL/NHL priority)](#task-4-loader-return-schemas-wnbapwhlnhl-priority)
  - [Task 5: Generate loaders into the live package](#task-5-generate-loaders-into-the-live-package)
  - [Task 6: Network audit — manifest vs live release list (CI job, not `--check`)](#task-6-network-audit--manifest-vs-live-release-list-ci-job-not---check)
  - [Self-Review](#self-review)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 404-Safe Dataset Loaders Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax. **Prerequisite:** Plans 1–3 complete — the engine renders flat modules, `_codegen_runtime` exists, `spec.py` supports the param/transform model.

**Goal:** Generate the ~90 missing `load_{league}_{dataset}(seasons)` functions from a `releases.yaml` manifest — every sportsdataverse-data release tag gets a **404-safe** loader (skips + warns missing seasons instead of crashing) — closing the gap in spec §14, with WNBA and PWHL prioritized; MLB stays a documented stub (no releases).

**Architecture:** A new `_read_release_parquet` runtime helper returns `None` on HTTP 404 / missing asset. `releases.yaml` (one entry per dataset: `fn`, `base`, `url`, `tag`, `min_season`, `returns_schema`, `automation`, `example_args`) is seeded by `extract.py --releases` from the 69 `config.py` URL constants + `gh release list`. A `load_module.py.jinja` renders 404-safe season-looping loaders into `sportsdataverse/{league}/{league}_loaders.py`. A network CI check audits manifest tags vs the live release list.

**Tech Stack:** Python ≥3.10, polars, PyYAML, Jinja2, pytest, ruff; `gh` CLI for the manifest seed.

---

## File Structure

**Create:**

- `tools/codegen/endpoints/releases.yaml` — the loader manifest (seeded).
- `tools/codegen/templates/load_module.py.jinja` — 404-safe loader template.
- `tools/codegen/schemas/{loader datasets}.yaml` — `@return` tables (introspected + R-sourced).
- `tests/codegen/test_runtime_release.py`, `tests/codegen/test_load_module.py`, `tests/codegen/test_loaders_parity.py`.

**Modify:**

- `sportsdataverse/_codegen_runtime.py` — `_read_release_parquet`, `_as_season_list`, `cli_warn`, `_SDV_RELEASES`/`_RAW_DATA` bases, `SeasonNotFoundError` re-export.
- `tools/codegen/extract.py` — `--releases` (config.py + `gh release list` → `releases.yaml`).
- `tools/codegen/spec.py` — `Loader` dataclass + `load_releases`.
- `tools/codegen/generate.py` — render `{league}_loaders.py`; manage `__init__` region; manifest-vs-release audit.
- `sportsdataverse/{league}/{league}_loaders.py` — regenerated/expanded.
- `sportsdataverse/{league}/__init__.py` — managed region includes loaders.
- `sportsdataverse/mlb/mlb_loaders.py` — keep as documented stub (raise clear message).

---

## Task 1: `_read_release_parquet` 404-safe helper

**Files:**

- Modify: `sportsdataverse/_codegen_runtime.py`
- Test: `tests/codegen/test_runtime_release.py`

- [ ] **Step 1: Write the failing test**

`tests/codegen/test_runtime_release.py`:

```python
from unittest.mock import patch

import polars as pl

from sportsdataverse import _codegen_runtime as rt

def test_as_season_list_normalizes():
    assert rt._as_season_list(2024) == [2024]
    assert rt._as_season_list(range(2022, 2024)) == [2022, 2023]
    assert rt._as_season_list([2021, 2022]) == [2021, 2022]

def test_read_release_parquet_returns_df_on_success():
    df = pl.DataFrame({"a": [1]})
    with patch("sportsdataverse._codegen_runtime.pl.read_parquet", return_value=df):
        out = rt._read_release_parquet("https://x/ok.parquet")
    assert out is not None and out.shape == (1, 1)

def test_read_release_parquet_returns_none_on_404():
    def boom(*a, **k):
        raise FileNotFoundError("404 Not Found")
    with patch("sportsdataverse._codegen_runtime.pl.read_parquet", side_effect=boom):
        assert rt._read_release_parquet("https://x/missing.parquet") is None
```

- [ ] **Step 2: Run → fail**

Run: `pytest tests/codegen/test_runtime_release.py -v`
Expected: FAIL — helpers don't exist.

- [ ] **Step 3: Implement in `_codegen_runtime.py`**

```python
import polars as pl

from sportsdataverse.errors import SeasonNotFoundError  # noqa: F401  (re-export for generated loaders)

_SDV_RELEASES = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
_RAW_DATA = "https://raw.githubusercontent.com/sportsdataverse/"

def _as_season_list(seasons):
    if isinstance(seasons, int):
        return [seasons]
    return [int(s) for s in seasons]

def cli_warn(msg: str) -> None:
    import warnings
    warnings.warn(msg, stacklevel=2)

def _read_release_parquet(url: str):
    """Read a release parquet; return None on 404 / missing asset (404-safe loaders)."""
    try:
        return pl.read_parquet(url, use_pyarrow=True)
    except Exception as e:  # noqa: BLE001  — any fetch/parse failure => treat as missing
        msg = str(e).lower()
        if "404" in msg or "not found" in msg or "no such" in msg or "forbidden" in msg:
            return None
        raise
```

- [ ] **Step 4: Run → pass; commit**

Run: `pytest tests/codegen/test_runtime_release.py -v` → PASS

```bash
git add sportsdataverse/_codegen_runtime.py tests/codegen/test_runtime_release.py
git commit -m "feat(codegen): 404-safe _read_release_parquet + season-list helpers"
```

---

## Task 2: `Loader` spec + `load_module.py.jinja`

**Files:**

- Modify: `tools/codegen/spec.py`
- Create: `tools/codegen/templates/load_module.py.jinja`
- Modify: `tools/codegen/generate.py` (`render_loader_module`)
- Test: `tests/codegen/test_load_module.py`

- [ ] **Step 1: Write the failing test**

`tests/codegen/test_load_module.py`:

```python
import ast
from pathlib import Path
from unittest.mock import patch

import polars as pl

from tools.codegen import generate, spec

def test_load_releases_and_render(tmp_path):
    y = tmp_path / "releases.yaml"
    y.write_text(
        "bases:\n  sdv_releases: 'https://github.com/sportsdataverse/sportsdataverse-data/releases/download/'\n"
        "loaders:\n"
        "  - fn: load_wnba_shots\n    base: sdv_releases\n    url: 'espn_wnba_shots/shot_locations_{season}.parquet'\n"
        "    tag: espn_wnba_shots\n    min_season: 2002\n    league: wnba\n    example_args: { seasons: 2024 }\n",
        encoding="utf-8",
    )
    rel = spec.load_releases(y)
    src = generate.render_loader_module("wnba", [l for l in rel.loaders if l.league == "wnba"], rel.bases)
    ast.parse(src)
    assert "def load_wnba_shots(" in src
    assert "_read_release_parquet" in src

def test_generated_loader_is_404_safe(tmp_path):
    # render + exec a one-loader module; first season ok, second 404 -> skipped + warned
    y = tmp_path / "releases.yaml"
    y.write_text(
        "bases:\n  sdv_releases: 'https://x/'\n"
        "loaders:\n  - fn: load_wnba_shots\n    base: sdv_releases\n    url: 'espn_wnba_shots/s_{season}.parquet'\n"
        "    tag: espn_wnba_shots\n    min_season: 2002\n    league: wnba\n    example_args: { seasons: 2024 }\n",
        encoding="utf-8",
    )
    rel = spec.load_releases(y)
    src = generate.render_loader_module("wnba", rel.loaders, rel.bases)
    ns = {}
    exec(compile(src, "gen_wnba_loaders", "exec"), ns)  # noqa: S102 (test of generated code)

    calls = {"n": 0}

    def fake_read(url):
        calls["n"] += 1
        return pl.DataFrame({"x": [1]}) if "2023" in url else None  # 2024 -> None (missing)

    with patch("sportsdataverse._codegen_runtime._read_release_parquet", side_effect=fake_read):
        out = ns["load_wnba_shots"](seasons=[2023, 2024])
    assert out.shape[0] == 1  # 2024 skipped, not crashed
```

- [ ] **Step 2: Run → fail**

Run: `pytest tests/codegen/test_load_module.py -v`
Expected: FAIL — `load_releases`/`render_loader_module`/`load_module.py.jinja` missing.

- [ ] **Step 3: Implement `Loader` + `load_releases` in `spec.py`**

```python
@dataclass(frozen=True)
class Loader:
    fn: str
    league: str
    base: str
    url: str
    tag: str
    min_season: Optional[int] = None
    returns_schema: Optional[str] = None
    example_args: Dict[str, object] = field(default_factory=dict)
    automation: Dict[str, str] = field(default_factory=dict)
    notebook: Optional[str] = None
    stub: bool = False
    stub_message: Optional[str] = None

@dataclass(frozen=True)
class ReleasesConfig:
    bases: Dict[str, str]
    loaders: List[Loader]

def load_releases(path: Path) -> ReleasesConfig:
    raw = _read_yaml(path)
    loaders = [
        Loader(
            fn=l["fn"], league=l["league"], base=l["base"], url=l["url"], tag=l["tag"],
            min_season=l.get("min_season"), returns_schema=l.get("returns_schema"),
            example_args=l.get("example_args", {}) or {}, automation=l.get("automation", {}) or {},
            notebook=l.get("notebook"), stub=l.get("stub", False), stub_message=l.get("stub_message"),
        )
        for l in raw["loaders"]
    ]
    return ReleasesConfig(bases=dict(raw["bases"]), loaders=loaders)
```

- [ ] **Step 4: Write `load_module.py.jinja`**

```jinja
# GENERATED by tools/codegen/generate.py — DO NOT EDIT.
"""sportsdataverse.{{ league }} dataset loaders (sportsdataverse-data releases). Generated."""
from __future__ import annotations

import polars as pl

from sportsdataverse._codegen_runtime import (
    SeasonNotFoundError,
    _as_season_list,
    _read_release_parquet,
    cli_warn,
)

_BASE = "{{ base_url }}"

__all__ = [
{% for ld in loaders %}    "{{ ld.fn }}",
{% endfor %}]

{% for ld in loaders %}

def {{ ld.fn }}(seasons, return_as_pandas: bool = False):
    """Load {{ ld.tag }} (sportsdataverse-data release).

    Source: https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/{{ ld.tag }}
    Args:
        seasons: int or iterable of seasons{% if ld.min_season %} (>= {{ ld.min_season }}){% endif %}.
        return_as_pandas: return pandas instead of polars.
    Returns: polars (or pandas) DataFrame; missing seasons are skipped with a warning.
    Example:
        >>> {{ ld.fn }}(seasons={{ ld.example_args.get('seasons', 2024) }})
    """
{% if ld.stub %}    raise NotImplementedError("{{ ld.stub_message }}")
{% else %}    frames, missing = [], []
    for s in _as_season_list(seasons):
{% if ld.min_season %}        if int(s) < {{ ld.min_season }}:
            raise SeasonNotFoundError("season cannot be less than {{ ld.min_season }}")
{% endif %}        df = _read_release_parquet(f"{_BASE}{{ ld.url }}")
        if df is None:
            missing.append(s)
            continue
        frames.append(df)
    if missing:
        cli_warn(f"{{ ld.fn }}: no data for season(s) {missing} (skipped)")
    out = pl.concat(frames, how="vertical_relaxed") if frames else pl.DataFrame()
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out
{% endif %}
{% endfor %}
```

Add `generate.render_loader_module(league, loaders, bases)` resolving `base_url = bases[loaders[0].base]` (all of a league's loaders share `sdv_releases` or `raw_data`; if mixed, group by base into separate `_BASE` constants — keep simple: emit per-loader absolute url by inlining `bases[ld.base]` into the f-string at render time instead of a shared `_BASE`).

> **Refinement (do this in Step 4):** to support mixed bases per league cleanly, inline the full URL: render `f"{{ bases[ld.base] }}{{ ld.url }}"` so each loader carries its absolute base; drop the shared `_BASE`. This mirrors the ESPN alt-host decision from Plan 2.

- [ ] **Step 5: Run → pass; commit**

Run: `pytest tests/codegen/test_load_module.py -v` → PASS

```bash
git add tools/codegen/spec.py tools/codegen/templates/load_module.py.jinja tools/codegen/generate.py tests/codegen/test_load_module.py
git commit -m "feat(codegen): Loader spec + 404-safe load_module template"
```

---

## Task 3: Seed `releases.yaml` (`extract.py --releases`)

**Files:**

- Modify: `tools/codegen/extract.py`
- Create: `tools/codegen/endpoints/releases.yaml`
- Test: `tests/codegen/test_extract.py` (extend)

- [ ] **Step 1: Add `--releases` to `extract.py`**

Parse `sportsdataverse/config.py` URL constants (regex `NAME_URL = ... "{season}" ...`) into manifest entries, map each to a `(league, dataset, base, url, tag)`, **and** enumerate `gh release list -R sportsdataverse/sportsdataverse-data --limit 300` to add the tags lacking a config constant (the gap). Emit `releases.yaml`. Add the gap-flagging:

```python
def gh_release_tags():
    import subprocess
    out = subprocess.run(
        ["gh", "release", "list", "-R", "sportsdataverse/sportsdataverse-data", "--limit", "300"],
        capture_output=True, text=True, check=True).stdout
    return sorted({line.split("\t")[0] for line in out.splitlines() if line.strip()})
```

For each release tag with no existing loader, synthesize a `Loader` entry: `league` from the tag prefix (`espn_wnba_*`/`wnba_stats_*`→wnba, `pwhl_*`→pwhl, `nhl_*`→nhl, …), `fn = load_{league}_{dataset}`, `base: sdv_releases`, `url: "{tag}/{stem}_{season}.parquet"` (stem inferred per family; hand-verify), `min_season` from the family. **PWHL** gets `min_season: 2024`; **MLB** entries are emitted with `stub: true` + `stub_message` (no releases). **CFB** existing loaders cut over from `cfbfastR-data` to `espn_cfb_*`.

- [ ] **Step 2: Generate + hand-verify `releases.yaml`**

Run `python tools/codegen/extract.py --releases`, then hand-verify stems/min-seasons against the actual release assets (spot-check WNBA/PWHL/NHL with `gh release view <tag>`), and add `automation:` blocks (repo+workflow) and `returns_schema:` refs.

- [ ] **Step 3: Test the seed has WNBA/PWHL coverage**

Append to `tests/codegen/test_extract.py`:

```python
def test_releases_manifest_covers_wnba_and_pwhl():
    from tools.codegen import spec
    from pathlib import Path
    rel = spec.load_releases(Path("tools/codegen/endpoints/releases.yaml"))
    fns = {l.fn for l in rel.loaders}
    assert "load_wnba_shots" in fns and "load_wnba_standings" in fns
    assert any(l.league == "pwhl" for l in rel.loaders)
    # MLB present but stubbed
    assert any(l.league == "mlb" and l.stub for l in rel.loaders)
```

- [ ] **Step 4: Run → pass; commit**

Run: `pytest tests/codegen/test_extract.py -v` → PASS

```bash
git add tools/codegen/extract.py tools/codegen/endpoints/releases.yaml tests/codegen/test_extract.py
git commit -m "feat(codegen): seed releases.yaml loader manifest (config.py + gh release list)"
```

---

## Task 4: Loader `@return` schemas (WNBA/PWHL/NHL priority)

**Files:**

- Create: `tools/codegen/schemas/{loader datasets}.yaml`
- Modify: `tools/codegen/extract.py` (`--loader-schemas`)
- Test: `tests/codegen/test_load_module.py` (extend)

- [ ] **Step 1: Introspect one season per dataset → stub schema**

Add `extract.py --loader-schemas` that, for each loader, reads `https://.../{stem}_{min_season}.parquet` (or a known-good season) with polars and emits `{name, type}` columns. Author descriptions for the WNBA/PWHL/NHL high-value datasets (mine wehoop/fastRhockey `@return` roxygen).

- [ ] **Step 2: Wire `returns_schema` into the loader docstring**

Extend `load_module.py.jinja` Returns block to render the `@return` table when the loader has a resolved `returns_schema` (reuse the same column-table rendering as the API reference). Add a test asserting a known loader's docstring contains a column name.

- [ ] **Step 3: Run → pass; commit**

```bash
git add tools/codegen/schemas/ tools/codegen/extract.py tools/codegen/templates/load_module.py.jinja tests/codegen/test_load_module.py
git commit -m "feat(codegen): loader @return schemas (WNBA/PWHL/NHL); render in docstrings"
```

---

## Task 5: Generate loaders into the live package

**Files:**

- Modify: `tools/codegen/generate.py` (`build_live` includes loaders), `sportsdataverse/{league}/{league}_loaders.py`, `sportsdataverse/{league}/__init__.py`, `sportsdataverse/mlb/mlb_loaders.py`
- Test: `tests/codegen/test_loaders_parity.py`

- [ ] **Step 1: Add loaders to `build_live()`**

For each league with loaders in `releases.yaml`, render `sportsdataverse/{league}/{league}_loaders.py` and add it to the `__init__` managed region. **MLB**: render the stub module (its `load_mlb_*` raise `NotImplementedError` with the "no release yet — use live wrappers" message). **PWHL**: new `sportsdataverse/pwhl/` package — create `pwhl/__init__.py` if absent (the spec adds PWHL as a league; create the package dir + `__init__.py` importing `pwhl_loaders`).

- [ ] **Step 2: Parity test — every release tag has a loader**

`tests/codegen/test_loaders_parity.py`:

```python
from pathlib import Path

from tools.codegen import spec

LEGACY = {"ESPN", "cfbfastR_cfb_pbp"}  # excluded

def test_existing_load_functions_are_preserved_or_renamed():
    rel = spec.load_releases(Path("tools/codegen/endpoints/releases.yaml"))
    fns = {l.fn for l in rel.loaders}
    # the 4 historical WNBA loaders still exist (names preserved)
    for f in ("load_wnba_pbp", "load_wnba_schedule", "load_wnba_player_boxscore", "load_wnba_team_boxscore"):
        assert f in fns, f"regressed historical loader {f}"

def test_no_release_tag_without_a_loader(monkeypatch):
    # offline: assert manifest tags cover the (committed) release-tag snapshot fixture
    snap = Path("tests/codegen/fixtures/release_tags.txt").read_text().split()
    rel = spec.load_releases(Path("tools/codegen/endpoints/releases.yaml"))
    tags = {l.tag for l in rel.loaders}
    missing = [t for t in snap if t not in tags and t not in LEGACY and not t.startswith("ESPN")]
    assert not missing, f"release tags without loaders: {missing}"
```

(Commit a `tests/codegen/fixtures/release_tags.txt` snapshot from `gh release list` so the test is offline; the live audit is a separate network CI job.)

- [ ] **Step 3: Generate + full suite**

Run: `python tools/codegen/generate.py --target live && python -c "import sportsdataverse.wnba as w; print(w.load_wnba_shots)" && pytest tests/ -q`
Expected: import clean; suite green; `load_wnba_shots`/`load_pwhl_pbp` exist; `load_mlb_pbp` raises the stub message.

- [ ] **Step 4: Commit**

```bash
git add tools/codegen/generate.py sportsdataverse/ tests/codegen/test_loaders_parity.py tests/codegen/fixtures/release_tags.txt
git commit -m "feat(loaders): generate ~90 404-safe load_* (WNBA/PWHL/NHL/WBB/NBA/CFB); MLB stubbed"
```

---

## Task 6: Network audit — manifest vs live release list (CI job, not `--check`)

**Files:**

- Modify: `tools/codegen/generate.py` (`audit_releases()`), `.github/workflows/` (a network job — wired fully in Plan 5)
- Test: covered by the offline snapshot test (Task 5); this adds the live audit entrypoint.

- [ ] **Step 1: Add `generate.py --audit-releases`**

```python
def audit_releases() -> int:
    from tools.codegen.extract import gh_release_tags
    rel = spec.load_releases(ENDPOINTS / "releases.yaml")
    manifest = {l.tag for l in rel.loaders}
    live = set(gh_release_tags())
    legacy = {"ESPN", "cfbfastR_cfb_pbp"}
    missing = sorted(t for t in live - manifest if t not in legacy and not t.startswith("ESPN"))
    orphan = sorted(t for t in manifest - live)
    if missing or orphan:
        print("release manifest drift — missing loaders:", missing, "| orphan tags:", orphan, file=sys.stderr)
        return 1
    print("release manifest matches live release list")
    return 0
```

Wire `--audit-releases` into `main()`.

- [ ] **Step 2: Run it live (manual verify)**

Run: `python tools/codegen/generate.py --audit-releases`
Expected: prints match, or lists any newly-published tag needing a one-line manifest add.

- [ ] **Step 3: Commit**

```bash
git add tools/codegen/generate.py
git commit -m "feat(codegen): --audit-releases (manifest vs live sportsdataverse-data release list)"
```

---

## Self-Review

- **Spec coverage:** §3.9 manifest + 404-safe `_read_release_parquet` → Tasks 1,2; `extract.py --releases` seed from config + `gh release list` → Task 3; §14 gap (WNBA/PWHL priority, ~90 loaders) → Tasks 3,5; PWHL new package, CFB cutover, MLB stub → Task 5; loader `@return` schemas → Task 4; automation block → Task 3 (consumed by Plan 5 docs); manifest-drift audit → Task 6. Deferred: loaders **docs page** with Mermaid/badges → Plan 5; loaders **notebook** → Plan 5.
- **Placeholder scan:** stem/min-season/automation authoring (Task 3 Step 2) and description authoring (Task 4) are data steps with concrete verification (`gh release view`, R roxygen), not vague TODOs. All code complete.
- **Type consistency:** `Loader`/`ReleasesConfig`/`load_releases` consistent; `render_loader_module(league, loaders, bases)` signature matches its tests and `build_live` call; `_read_release_parquet`/`_as_season_list`/`cli_warn`/`SeasonNotFoundError` names match the template's imports; `stub`/`stub_message` fields match the template's `{% if ld.stub %}` branch.
