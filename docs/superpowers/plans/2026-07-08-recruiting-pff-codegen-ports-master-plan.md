<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Recruiting + PFF Codegen Ports — Master Implementation Plan](#recruiting--pff-codegen-ports--master-implementation-plan)
  - [Global Constraints](#global-constraints)
  - [Wave 0 — Foundation (serial; lands on the base branch first)](#wave-0--foundation-serial-lands-on-the-base-branch-first)
    - [Task F1: pff + 247 live-test gates](#task-f1-pff--247-live-test-gates)
    - [Task F2: pff auth spike (determines how much of `pff_login` ships)](#task-f2-pff-auth-spike-determines-how-much-of-pff_login-ships)
  - [Wave 1 — Four parallel tracks (each in its own worktree off the Wave-0 base)](#wave-1--four-parallel-tracks-each-in-its-own-worktree-off-the-wave-0-base)
  - [Wave 2 — Integration (serial; on the base branch)](#wave-2--integration-serial-on-the-base-branch)
    - [Task I1: merge sources + reconcile generated drift](#task-i1-merge-sources--reconcile-generated-drift)
    - [Task I2: full gate sweep](#task-i2-full-gate-sweep)
    - [Task I3: PRs](#task-i3-prs)
  - [Self-Review (run before execution)](#self-review-run-before-execution)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Recruiting + PFF Codegen Ports — Master Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement the per-track plans task-by-task. This master plan orchestrates four parallel track plans; execute Wave 0 and Wave 2 from here, and each Wave-1 track from its own plan file. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Port four reverse-engineered OpenAPI specs (pff, on3 RDB, sports247 RDB, sports247 site-pages) into sdv-py as codegen-generated flat-API stems — wrappers + parsers + returns-schemas + returns-table descriptions + docs + tests + the sdv-swagger mirror seam — executed as four parallel tracks.

**Architecture:** Three waves. **Wave 0** (serial, small) lands the only shared prerequisites: the `@skip_if_no_pff_live` test gate and the pff auth spike. **Wave 1** runs the four tracks in parallel git worktrees, each self-contained (its own generator, parser, schemas, descriptions, tests, and `FLAT_APIS` line). **Wave 2** (serial) merges the four branches, runs `generate.py` **once** on the merged tree to reconcile all generated-file drift, then a full gate sweep and PRs. Design spec: [`../specs/2026-07-08-recruiting-pff-codegen-ports-design.md`](../specs/2026-07-08-recruiting-pff-codegen-ports-design.md).

**Tech Stack:** Python 3.9–3.14, polars 1.x, uv, the sdv-py `tools/codegen/` flat-API pipeline (`FLAT_APIS`, `endpoints/<stem>.yaml`, `schemas/native/<stem>/`, jinja `api_module.py.jinja`), curl_cffi (optional, for 247 hosts), requests.

## Global Constraints

Every task in every track plan implicitly includes these (verbatim from `CLAUDE.md`):

- **polars `>=1.0,<2.0`, modern API only** — `group_by`, `with_row_index`, `map_elements(…, return_dtype=)`, `pl.len()`, `how="full", coalesce=True`, `cum_sum`, `str.strip_chars`; bool masks explicit `pl.col("c") == True`; **no regex lookaround** — use `(?i)prefix(?-i: NAMES)`.
- **Returns-table descriptions live ONLY in `tools/codegen/schemas/manual_column_descriptions.yaml`** (schema-keyed), NEVER in `schemas/**.yaml` (clobbered on recapture).
- **uv for everything** (`uv run pytest|ruff|mypy`); PEP 621 `pyproject.toml`; no `setup.py`/`requirements*.txt`.
- **New modules fully typed**, appended to the `[tool.mypy] files` ratchet in `pyproject.toml` once clean.
- **ID / join-key dtype discipline** — pin one dtype per id at the boundary; assert `left.schema[k] == right.schema[k]` before a join; 247 site-pages serializes numerics as strings (cast at the boundary); pff `player_id` is snake_case + int.
- **Regenerate generated files after touching sources, then `uv run python tools/codegen/generate.py --check`** before every commit (CI + pre-commit drift gate).
- **Conventional Commits; NEVER add an AI co-author trailer.**
- `uv run mypy`/`pytest` can silently re-lock `uv.lock` — `git checkout uv.lock` unless the bump is deliberate.
- The **doctoc** pre-commit hook rewrites staged markdown and aborts the commit — re-add + re-commit; verify the commit landed (`git log -1`).
- **Live-test gates (reconciled — single policy):** pff → `@skip_if_no_pff_live` (env `SDV_PY_PFF_LIVE=1`), paywalled + residential (like `skip_if_no_nba_stats_live`). **on3** → standard `@skip_if_no_live` (`SDV_PY_LIVE_TESTS`), auth-free `api.on3.com`, no Fastly-hang evidence. **The 247 family (T3 RDB *and* T4 site-pages) shares ONE dedicated `@skip_if_no_247_live` gate (env `SDV_PY_247_LIVE=1`), CI-off** — `ipa/www.247sports.com` sit behind Fastly and may *hang* (not fail-fast) on datacenter/CI IPs the way `stats.nba.com` does, so the generic gate (which CI sets) must never run them. This overrides T4's per-track "standard gate" note: **both 247 tracks import the same gate**, defined once in Wave 0 F1. If a live run later proves 247 is CI-safe from datacenter IPs, relaxing both to `SDV_PY_LIVE_TESTS` is a trivial follow-up; guessing CI-safe wrong hangs CI, so default conservative.

---

## Wave 0 — Foundation (serial; lands on the base branch first)

Base branch `feat/recruiting-pff-codegen-ports` already exists and carries the design spec + these master/track plans. Wave 1 worktrees branch off it *after* Wave 0.

### Task F1: pff + 247 live-test gates

**Files:**
- Modify: `tests/conftest.py` (add `skip_if_no_pff_live` and `skip_if_no_247_live` beside `skip_if_no_nba_stats_live`)

**Interfaces:**
- Produces: `skip_if_no_pff_live` (env `SDV_PY_PFF_LIVE`) importable by T1 live tests, and `skip_if_no_247_live` (env `SDV_PY_247_LIVE`) importable by **both** T3 and T4 live tests. Defined once here so the two 247 tracks don't redefine it.

- [ ] **Step 1: Write the failing test** — `tests/test_conftest_gates.py`:
```python
def test_pff_live_gate_skips_without_env(monkeypatch):
    monkeypatch.delenv("SDV_PY_PFF_LIVE", raising=False)
    from tests.conftest import skip_if_no_pff_live
    marker = skip_if_no_pff_live
    assert marker.args and "SDV_PY_PFF_LIVE" in marker.kwargs.get("reason", marker.args[-1])
```
- [ ] **Step 2: Run it, verify it fails** — `uv run pytest tests/test_conftest_gates.py -q` → FAIL (ImportError: cannot import name `skip_if_no_pff_live`).
- [ ] **Step 3: Implement** — copy the `skip_if_no_nba_stats_live` definition in `tests/conftest.py` twice: (a) `skip_if_no_pff_live`, env `SDV_PY_PFF_LIVE`, reason `"Set SDV_PY_PFF_LIVE=1 to run PFF Premium live tests (paywalled; residential IP)"`; (b) `skip_if_no_247_live`, env `SDV_PY_247_LIVE`, reason `"ipa/www.247sports.com Fastly-block datacenter IPs; set SDV_PY_247_LIVE=1 to run from a residential IP"`. Extend the F1 test to assert both exist.
- [ ] **Step 4: Run it, verify it passes** — `uv run pytest tests/test_conftest_gates.py -q` → PASS.
- [ ] **Step 5: Commit** — `git add tests/conftest.py tests/test_conftest_gates.py && git commit -m "test(pff): add skip_if_no_pff_live gate"` (verify it landed after doctoc/hooks).

### Task F2: pff auth spike (determines how much of `pff_login` ships)

**Files:**
- Create: `dev/pff_auth_spike.md` (findings; `dev/` is gitignored working notes — the *finding* is copied into the T1 plan/PR description, not committed here)

**Interfaces:**
- Produces: a documented decision — *"`_premium_key` alone authorizes `/api/v1/*` for its TTL"* (→ ship cookie-supply only, `pff_login` optional/deferred) **or** *"`__session` (60s Clerk JWT) must be present/refreshed per call"* (→ `pff_login` Clerk helper is required in T1).

- [ ] **Step 1:** With a logged-in PFF+ browser session, capture the current `_premium_key` and `__session` cookie values (DevTools → Application → Cookies).
- [ ] **Step 2:** From a plain `requests`/`curl_cffi` call (no browser), hit `https://premium.pff.com/api/v1/facet/passing/summary?league=nfl&season=2025&week=1,2,3` with **only `_premium_key`** in the cookie jar. Record status + whether rows return.
- [ ] **Step 3:** Repeat with **only `__session`**, then with **both**. Wait >60s and retry the `_premium_key`-only call to test TTL.
- [ ] **Step 4:** Write the finding to `dev/pff_auth_spike.md`: minimal sufficient cookie set + TTL behaviour + the ship/defer decision for `pff_login`. **No commit** (dev/ gitignored); paste the decision into the T1 branch's PR body.

> If a live PFF session is unavailable at execution time, default to the **cookie-supply-only** path (T1 ships `pff_login` as an experimental, best-effort helper behind a clear docstring) and note the spike as deferred.

---

## Wave 1 — Four parallel tracks (each in its own worktree off the Wave-0 base)

Create one worktree per track with the `superpowers:using-git-worktrees` skill; branch names below. Each track is fully specified in its own plan file and ends with green local gates (`pytest`, `mypy` ratchet, `ruff`, `generate.py --check`). Local `generate.py` runs inside a track are **self-verification only** — the canonical regeneration is Wave 2.

| Track | Branch | Plan file | Touches (source-of-truth files) |
|---|---|---|---|
| **T1 pff** (NEW; core + 4 league shims) | `feat/pff-port` | [`2026-07-08-pff-port-plan.md`](2026-07-08-pff-port-plan.md) | `tools/codegen/gen_pff.py`, `endpoints/pff.yaml`, `schemas/native/pff/`, `sportsdataverse/nfl/pff_core.py`+`pff_parsers.py`+`pff_runtime.py`, `sportsdataverse/{nfl,cfb,football/aaf,football/ufl}/pff.py`, a self-contained `make_pff_league_module` helper (`functools.partial`; the ESPN factory it echoes is retired), `FLAT_APIS` (+`pff_core`/nfl), `manual_column_descriptions.yaml` (pff keys) |
| **T2 on3 retarget** | `feat/on3-rdb-retarget` | [`2026-07-08-on3-retarget-plan.md`](2026-07-08-on3-retarget-plan.md) | `tools/codegen/gen_on3.py`, `endpoints/on3.yaml`, `schemas/native/on3/`, `sportsdataverse/cfb/on3*.py`, `manual_column_descriptions.yaml` (on3 keys) |
| **T3 sports247 expand** | `feat/sports247-rdb-expand` | [`2026-07-08-sports247-rdb-expand-plan.md`](2026-07-08-sports247-rdb-expand-plan.md) | `tools/codegen/gen_sports247.py`, `endpoints/sports247.yaml`, `schemas/native/sports247/`, `sportsdataverse/cfb/sports247*.py`, `manual_column_descriptions.yaml` (sports247 keys) |
| **T4 sports247 site-pages** (NEW) | `feat/sports247-site-pages` | [`2026-07-08-sports247-site-pages-plan.md`](2026-07-08-sports247-site-pages-plan.md) | `tools/codegen/gen_sports247_site_pages.py`, `endpoints/sports247_site_pages.yaml`, `schemas/native/sports247_site_pages/`, `sportsdataverse/cfb/sports247_site_pages*.py`, `FLAT_APIS` (+`sports247_site_pages`/cfb), `manual_column_descriptions.yaml` (site-pages keys) |

**Contention register (why Wave 2 exists):**

- **`FLAT_APIS` + `_FLAT_API_DOC`** — only T1 (`("pff_core","nfl")`) and T4 (`("sports247_site_pages","cfb")`) append; T2/T3 leave them unchanged (on3/sports247 already registered). Two disjoint lines → trivial merge.
- **`manual_column_descriptions.yaml`** — all four append **disjoint top-level schema keys** → merge-friendly.
- **Generated tree** (`sportsdataverse/**/<stem>.py` wrappers, `docs/docs/**` reference, and any regenerated `schemas/native/**`) — every track regenerates the *whole* tree, so branch merges **will** conflict here. **Do not hand-resolve generated-file conflicts** — Wave 2 re-runs `generate.py` to author them.

---

## Wave 2 — Integration (serial; on the base branch)

### Task I1: merge sources + reconcile generated drift

**Files:** all four branches → base; then regenerated `sportsdataverse/**`, `docs/docs/**`.

- [ ] **Step 1: Merge the four branches into the base** in low-risk order — `feat/sports247-rdb-expand` → `feat/sports247-site-pages` → `feat/on3-rdb-retarget` → `feat/pff-port`. For each merge, if git conflicts land in **generated** files (wrappers under `sportsdataverse/`, `docs/docs/**`), take either side — they're rebuilt next. Resolve conflicts only in **source-of-truth** files (`endpoints/*.yaml`, `gen_*.py`, `*_parsers.py`, `*_runtime.py`, hand-written modules, `FLAT_APIS`, `manual_column_descriptions.yaml`) — these should be disjoint.
- [ ] **Step 2: Re-run every generator** — `uv run python tools/codegen/gen_pff.py && uv run python tools/codegen/gen_on3.py && uv run python tools/codegen/gen_sports247.py && uv run python tools/codegen/gen_sports247_site_pages.py`.
- [ ] **Step 3: Regenerate the whole tree** — `uv run python tools/codegen/generate.py` (rebuilds wrappers, parsed modules, docs). Then `uv run python tools/codegen/generate.py --check` → expect **no drift**.
- [ ] **Step 4: Commit the reconciled generated tree** — `git add sportsdataverse docs/docs tools/codegen && git commit -m "chore(codegen): regenerate after pff + recruiting flat-API ports"`.

### Task I2: full gate sweep

- [ ] **Step 1:** `uv run pytest -q` (offline suite) → PASS; then `SDV_PY_LIVE_TESTS=1 uv run pytest -q tests/cfb` (on3 + sports247 + site-pages live) from a residential IP → PASS/known-flaky-tolerant.
- [ ] **Step 2:** `uv run mypy` (files-ratchet includes the new typed modules) → clean.
- [ ] **Step 3:** `uv run ruff check sportsdataverse/ tools/` → clean; run the toolkit `polars-1x-reviewer` over the new/changed parsers.
- [ ] **Step 4:** `git checkout uv.lock` if `pytest`/`mypy` re-locked it and the bump was unintended.
- [ ] **Step 5:** Confirm no `sdv-internal-refs`/`sdv-swagger` spec changed (these ports are read-only consumers) → the statcast + pff mirror-sync tests in `sdv-internal-refs/tests` are unaffected; no mirror push needed.

### Task I3: PRs

- [ ] **Step 1:** Open the PR(s). Preferred: **four stacked PRs** in the merge order above, each self-contained; or one combined `feat(codegen): pff + on3/247 recruiting flat-API ports` PR if simpler to review. Body: link the design spec + per-track plans; note the pff auth-spike finding; NO AI co-author.

---

## Self-Review (run before execution)

1. **Spec coverage:** every design-spec track (T1–T4) + cross-cutting mechanics maps to a Wave-1 track plan + Wave-2 integration. Auth spike = F2. Live gate = F1. ✔ (Re-verify once the four track plans land.)
2. **Placeholder scan:** F2 spike is a real investigation task with concrete steps, not a placeholder; the per-track plans must contain real code (verify on assembly).
3. **Type consistency:** the pff shim helper is `make_pff_league_module(namespace, league_slug)` everywhere; core module is `pff_core` (prefix `nfl`) everywhere; new stem is `sports247_site_pages` everywhere. Confirm the four track plans use these exact names.
4. **Scope:** four PR-sized tracks + a thin foundation + an integration — appropriately decomposed.
