<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [On3 Recruit-Database Retarget — Implementation Plan (Track 2)](#on3-recruit-database-retarget--implementation-plan-track-2)
  - [Goal](#goal)
  - [Architecture](#architecture)
  - [Tech Stack](#tech-stack)
  - [Global Constraints](#global-constraints)
  - [Migration Risk (call it out up front)](#migration-risk-call-it-out-up-front)
  - [File Structure](#file-structure)
  - [Task 1 — `gen_on3.py` generator (reads the OpenAPI spec, emits endpoint YAML + schemas)](#task-1--gen_on3py-generator-reads-the-openapi-spec-emits-endpoint-yaml--schemas)
  - [Task 2 — Run `gen_on3.py`; commit regenerated `endpoints/on3.yaml` + `schemas/native/on3/`](#task-2--run-gen_on3py-commit-regenerated-endpointson3yaml--schemasnativeon3)
  - [Task 3 — Retarget `on3_runtime._get` to the RDB host; demote the scrape to `_scrape_get`](#task-3--retarget-on3_runtime_get-to-the-rdb-host-demote-the-scrape-to-_scrape_get)
  - [Task 4 — Rankings continuity: 4 legacy shim wrappers with `DeprecationWarning`](#task-4--rankings-continuity-4-legacy-shim-wrappers-with-deprecationwarning)
  - [Task 5 — `parse_on3_rdb` for the three RDB envelope shapes + real-capture tests](#task-5--parse_on3_rdb-for-the-three-rdb-envelope-shapes--real-capture-tests)
  - [Task 6 — Regenerate wrappers + docs; verify `FLAT_APIS`/`_FLAT_API_DOC`; `--check`](#task-6--regenerate-wrappers--docs-verify-flat_apis_flat_api_doc---check)
  - [Task 7 — Returns-table descriptions (pilots) + defer the `native/on3` bucket](#task-7--returns-table-descriptions-pilots--defer-the-nativeon3-bucket)
  - [Task 8 — Live-gated smoke tests (standard `SDV_PY_LIVE_TESTS` gate)](#task-8--live-gated-smoke-tests-standard-sdv_py_live_tests-gate)
  - [Task 9 — Gate sweep (polars-1x, mypy, ruff, drift) + close-out](#task-9--gate-sweep-polars-1x-mypy-ruff-drift--close-out)
  - [Parallel-contention notes (for the orchestrator running T1-T4 together)](#parallel-contention-notes-for-the-orchestrator-running-t1-t4-together)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# On3 Recruit-Database Retarget — Implementation Plan (Track 2)

> Written for agentic workers executing TDD. Every task is bite-sized: write a failing
> test, run it (see it fail), implement, run it (see it pass), commit. No task depends on
> an unstated earlier step. Real file paths, real signatures, real commands only.

- **Date:** 2026-07-08
- **Track:** T2 of the `2026-07-08-recruiting-pff-codegen-ports-design.md` program (PR #3 of 4).
- **Design spec:** `docs/superpowers/specs/2026-07-08-recruiting-pff-codegen-ports-design.md`
  (§"Track 2 — `on3` retarget → RDB").

## Goal

Retarget the existing `on3` codegen stem from the `www.on3.com/_next/data` Next.js scrape
(4 rankings endpoints) to the **On3 Public Recruit Database** (`api.on3.com/public/rdb/v1`,
82 endpoints, **auth-free**), while preserving rankings continuity: the 4 existing public
wrapper names keep working. Regenerate `endpoints/on3.yaml` + `schemas/native/on3/` from the
frozen OpenAPI spec via a new idempotent `tools/codegen/gen_on3.py` (modeled on
`tools/codegen/gen_nba_stats.py`).

## Architecture

- **Host swap.** `on3_runtime._get(url, params)` becomes a plain `requests`-via-`download`
  GET against `https://api.on3.com/public/rdb/v1/...` with a browser UA. No buildId discovery
  on the hot path (the RDB `/public/` gateway is read-only and open).
- **Scrape demoted to a fallback.** The current buildId-discovery machinery
  (`_extract_build_id`, `_discover_build_id`, `_BUILD_ID_RE`, `_RANKINGS_PATH_RE`) moves under
  a `_scrape_get()` used **only** by the 4 legacy rankings shim wrappers, which continue to hit
  `_next/data`. No RDB dependency for those; no public name dropped.
- **Three RDB envelope shapes**, all handled by one new generic parser `parse_on3_rdb`:
  paged (`{relatedModel, pagination, list:[...]}` → rows = `list`), single object
  (`{...}` → rows = `[obj]`), bare array (`[...]` → rows = the list). Confirmed against
  `sdv-internal-refs/on3/captures/_sample/*.json`.
- **Legacy scrape parsers stay.** `parse_on3_rankings` / `parse_on3_team_rankings` (which parse
  the `pageProps` envelope) are retained for the demoted shim so their existing tests still pass.
- **Codegen surface.** `gen_on3.py` reads the OpenAPI YAML, emits one endpoint per usable GET op
  (skip the 6 `x-live-status` non-200 removals), preferring the `On3*Live` response schema where
  the spec stamps one. `FLAT_APIS`/`_FLAT_API_DOC` already list `on3` — verify, don't re-add.

## Tech Stack

polars 1.x (modern API), PyYAML for codegen, `pandas.json_normalize` for flattening,
`sportsdataverse.dl_utils.{download,underscore}`, pytest (offline fixtures + `SDV_PY_LIVE_TESTS`
gated live), uv for all tooling.

## Global Constraints

- **polars `>=1.0,<2.0`, modern API only.** `group_by` / `with_row_index` /
  `map_elements(..., return_dtype=)` / `pl.len()` / `how="full", coalesce=True` / `cum_sum` /
  `str.strip_chars`. Bool masks explicit: `pl.col("c") == True`. **No regex lookaround**
  (Rust engine) — use the inline case toggle `(?i)prefix(?-i: NAMES)` if needed.
- **Returns-table column descriptions live ONLY in
  `tools/codegen/manual_column_descriptions.yaml`** (schema-keyed), NEVER in
  `tools/codegen/schemas/**.yaml` (those are clobbered on re-capture / re-gen).
- **ID / join-key dtype discipline.** On3 `personKey`/`organizationKey`/`recruitmentKey`/`key`
  are integers in the RDB JSON; keep one canonical dtype at the parser boundary. Never a
  float→Utf8 paper-over cast (`"123.0"`). Columns snake_case via `underscore`; empty frames
  carry the documented schema.
- **mypy files-ratchet.** New typed modules (`gen_on3.py` is a tool, not shipped; but
  `on3_runtime.py` / `on3_parsers.py` / a new `on3_rankings.py` shim) must type cleanly and be
  in the `[tool.mypy] files` list (the two runtime/parser modules already are — verify + add
  `on3_rankings.py`).
- **uv for everything:** `uv run pytest`, `uv run ruff check`, `uv run mypy`. After any
  `uv run mypy`/`pytest`, check `git status` for a silent `uv.lock` re-lock and
  `git checkout uv.lock` if it drifted.
- **Regenerate + `--check` before every commit that touched codegen sources.**
  `uv run python tools/codegen/generate.py` then
  `uv run python tools/codegen/generate.py --check` (drift gate; runs in CI + pre-commit).
- **Conventional Commits, scope `on3` or `cfb`; NO AI co-author trailer.**
- **Live tests on the standard `SDV_PY_LIVE_TESTS=1` gate** (`@skip_if_no_live` from
  `tests/conftest.py`) — the RDB is auth-free and does not JA3-block, so it is NOT on the
  separate `SDV_PY_NBA_STATS_LIVE` gate.

## Migration Risk (call it out up front)

The **existing offline tests in `tests/test_on3_parsers.py` MUST keep passing after the
retarget.** That file has three groups:
1. Parser tests (`test_parse_on3_rankings_*`, `test_parse_on3_team_rankings_*`,
   `test_parsers_return_zero_row_frame_on_empty`, `test_parsers_return_pandas_when_asked`) —
   these keep passing because `parse_on3_rankings` / `parse_on3_team_rankings` are **retained**.
2. Runtime tests (`test_extract_build_id`, `test_get_discovers_build_id_and_hits_data_route`,
   `test_get_refreshes_build_id_after_deploy_rotation`,
   `test_get_treats_unchanged_build_id_404_as_no_data`,
   `test_get_merges_caller_headers_and_derived_params_win`,
   `test_get_returns_empty_dict_on_unknown_path`) — these exercise the buildId scrape via
   `on3_runtime._get`. They must be **re-pointed to `on3_runtime._scrape_get`** (the demoted
   function keeps identical behavior). This is a test edit, done in Task 3.
3. Wrapper-wiring tests (`test_wrapper_routes_fixture_through_parser`,
   `test_all_four_wrappers_exported`) — the 4 legacy names now live in the hand-written
   `on3_rankings.py` shim; these are re-pointed in Task 4.

Do the retarget so groups (1) survives untouched and (2)/(3) are edited deliberately with the
test change committed alongside the code change.

---

## File Structure

```
tools/codegen/
  gen_on3.py                         # NEW — spec → endpoints/on3.yaml + schemas/native/on3/*.yaml (Task 1)
  endpoints/on3.yaml                 # REGENERATED — RDB host + N endpoints (Task 2, generated)
  schemas/native/on3/                # REGENERATED — one <short>.yaml per endpoint (Task 2, generated)
    on3_player_rankings.yaml         #   PRESERVED (legacy scrape schema, not clobbered)
    on3_team_rankings.yaml           #   PRESERVED (legacy scrape schema, not clobbered)
    team_ranking_team_rankings.yaml  #   NEW RDB schema … (+ ~30 more)
  generate.py                        # VERIFY only — FLAT_APIS + _FLAT_API_DOC already list on3 (Task 6)
  manual_column_descriptions.yaml    # APPEND — pilot RDB schema descriptions (Task 7)
  extract_residual_columns.py        # MODIFY — add "native/on3" to _DEFERRED_BUCKETS (Task 7)

sportsdataverse/cfb/
  on3.py                             # REGENERATED — RDB wrappers on3_<short> (Task 6, generated)
  on3_runtime.py                     # REWRITE — _get → RDB; scrape → _scrape_get fallback (Task 3)
  on3_parsers.py                     # EXTEND — add parse_on3_rdb; keep the two scrape parsers (Task 5)
  on3_rankings.py                    # NEW — 4 legacy rankings shim wrappers (DeprecationWarning) (Task 4)
  __init__.py                        # MODIFY — export on3_rankings shim wrappers (Task 4)

tests/
  test_on3_parsers.py                # EDIT — re-point runtime tests to _scrape_get (Task 3)
  test_on3_rdb.py                    # NEW — RDB runtime + parse_on3_rdb + wrapper wiring (Tasks 3/5/6)
  test_on3_rankings_continuity.py    # NEW — 4 legacy names still return frames (Task 4)
  fixtures/on3/                       # ADD real captures: team_ranking_team_rankings.json,
                                     #   player_profile.json, player_all_rankings.json, etc. (Task 5)

docs/docs/cfb/reference/on3.md       # REGENERATED (Task 6, generated)
```

Reference inputs (read-only, do NOT copy into the repo except the fixtures):
`C:/Users/saiem/Documents/sdv-internal-refs/on3/on3-recruit-database.openapi.yaml`,
`.../ENDPOINTS.md`, `.../on3-recruit-database-returns.md`, `.../captures/_sample/*.json`,
`.../captures/manifest.csv`.

---

## Task 1 — `gen_on3.py` generator (reads the OpenAPI spec, emits endpoint YAML + schemas)

**Files**
- Create: `tools/codegen/gen_on3.py`
- Test: `tests/codegen/test_gen_on3.py` (NEW)

**Interfaces**
- Consumes: `on3-recruit-database.openapi.yaml` (path via
  `SDV_INTERNAL_REFS_REPO` env, default
  `C:/Users/saiem/Documents/sdv-internal-refs` → `on3/on3-recruit-database.openapi.yaml`).
- Produces: `tools/codegen/endpoints/on3.yaml` (flat-API doc: `api`, `host`, `name_pattern`,
  `module`, `parser_module`, `getter_module`, `qualifier: ''`, `passthrough_query: true`,
  `runtime_imports: [_get]`, `endpoints: [...]`) and one
  `tools/codegen/schemas/native/on3/<short>.yaml` per endpoint.
- Public entry: `def main() -> None`; helpers `_load_spec() -> dict`,
  `_short_from_path(path: str) -> str`, `_usable(op: dict) -> bool`,
  `_response_columns(op: dict, spec: dict) -> list[dict]`.

**Design decisions baked in**
- **Host** = `https://api.on3.com/public/rdb/v1`. The OpenAPI `paths` keys are `/rdb/v1/...`
  (and one `/rdb/v2/...`); strip the leading `/rdb/v1` (keep `/rdb/v2` explicit) so the emitted
  `path` concatenates cleanly under the host. i.e. `host + path` == the real URL.
- **`short`** = snake_case of the path minus the version prefix and `{...}` params, joined by
  `_`. Examples: `/rdb/v1/team-ranking/{sport}-{year}/team-rankings` → `team_ranking_team_rankings`;
  `/rdb/v1/players/industry-comparision` → `players_industry_comparision`;
  `/rdb/v1/person-sport-rankings` → `person_sport_rankings`;
  `/rdb/v1/player/{personKey}/profile` → `player_profile`;
  `/rdb/v2/nil-100` → `nil_100_v2` (v2 suffix disambiguates the `/rdb/v1/nil-100` sibling).
- **`_usable`** drops the 6 non-200 removals (spec stamp `x-live-status` with 404/400 —
  `filters/interest`, `predictions`, `player/look-up-mapping`, `nil-compliances`, the two
  `primary-recruitment-evaluation`). Keep the `⚠204` recruitment-eval routes? No — keep them
  (204 = empty body, tolerable zero-row frame), only drop 404/400. Encode as:
  `x-live-status` present AND status in `{400, 404}` → skip.
- **Response schema pick:** resolve `op.responses["200"].content["application/json"].schema`.
  If it `$ref`s an `On3*Live` component, use that (live-validated superset). If it is an array
  (`type: array, items: $ref`), unwrap `items`. If it is `*PagedData` (has a `list` property
  whose items `$ref`), unwrap `list.items`. Emit the resolved object's `properties` as the
  returns-schema columns (name + JSON→R type via `_DTYPE` mirroring `gen_nba_stats._DTYPE`:
  `integer→integer`, `number→numeric`, `string→character`, `boolean→logical`,
  `object|array|unknown→character`). Empty description (filled later in Task 7 / the R backfill).
- **Preserve the two legacy scrape schemas.** `_clean_generated_schema_dir` must NOT delete
  `on3_player_rankings.yaml` / `on3_team_rankings.yaml` (they back the demoted shim). Encode a
  `_PRESERVE = {"on3_player_rankings", "on3_team_rankings"}` skip-set in the cleaner.
- **Idempotent:** `yaml.safe_dump(..., sort_keys=True, default_flow_style=False)` like
  `gen_nba_stats._write_yaml`; same spec → byte-identical output.

Steps:
- [ ] Write `tests/codegen/test_gen_on3.py::test_short_from_path` asserting
  `gen_on3._short_from_path("/rdb/v1/team-ranking/{sport}-{year}/team-rankings") ==
  "team_ranking_team_rankings"` and
  `gen_on3._short_from_path("/rdb/v2/nil-100") == "nil_100_v2"`.
- [ ] Run `uv run pytest tests/codegen/test_gen_on3.py -q` → fails (module does not exist).
  Expected: `ModuleNotFoundError: No module named ... gen_on3` / collection error.
- [ ] Implement `gen_on3.py` with `_load_spec`, `_short_from_path`, `_usable`,
  `_response_columns`, `_endpoint_entry`, `_write_yaml`, `_clean_generated_schema_dir` (with
  `_PRESERVE`), and `main()`. Mirror the structure of `tools/codegen/gen_nba_stats.py`
  (STEMS→single stem `on3`; `parser_module: "cfb.on3_parsers"`,
  `getter_module: "sportsdataverse.cfb.on3_runtime"`, `parser: "parse_on3_rdb"` on every
  endpoint).
- [ ] Add `tests/codegen/test_gen_on3.py::test_usable_drops_non200` asserting `_usable`
  returns `False` for an op dict `{"x-live-status": {"status": 404}}` and `True` for one with
  `x-live-validated: True`.
- [ ] Run `uv run pytest tests/codegen/test_gen_on3.py -q` → **passes** (`2 passed`).
- [ ] `git add tools/codegen/gen_on3.py tests/codegen/test_gen_on3.py &&
  git commit -m "feat(on3): add gen_on3.py codegen generator for the RDB spec"`

---

## Task 2 — Run `gen_on3.py`; commit regenerated `endpoints/on3.yaml` + `schemas/native/on3/`

**Files**
- Modify (generated): `tools/codegen/endpoints/on3.yaml`
- Modify (generated): `tools/codegen/schemas/native/on3/*.yaml` (new RDB schemas;
  `on3_player_rankings.yaml` + `on3_team_rankings.yaml` preserved)
- Test: reuse `tests/codegen/test_gen_on3.py` (add an idempotence assertion)

**Interfaces**
- Consumes: the OpenAPI spec. Produces: the committed endpoint YAML + schema files consumed by
  `tools/codegen/generate.py` at wrapper-render time (Task 6).

Steps:
- [ ] Add `tests/codegen/test_gen_on3.py::test_idempotent` that runs `gen_on3.main()` twice into
  a `tmp_path` copy and asserts byte-identical `on3.yaml` on the second run.
- [ ] Run `uv run pytest tests/codegen/test_gen_on3.py::test_idempotent -q` → fails until wired
  (`main()` may need a param or env to target tmp; if `main()` is fixed-path, assert on the
  real regenerated file's stability across two `main()` calls instead).
- [ ] Run the generator for real:
  `uv run python tools/codegen/gen_on3.py` → prints e.g. `on3: 34 endpoints`.
- [ ] Verify the host + a known endpoint landed:
  `uv run python -c "import yaml;d=yaml.safe_load(open('tools/codegen/endpoints/on3.yaml'));print(d['host']);print([e['short'] for e in d['endpoints']][:6])"`
  → expect `https://api.on3.com/public/rdb/v1` and shorts including
  `team_ranking_team_rankings`, `players_industry_comparision`, `person_sport_rankings`.
- [ ] Confirm the two legacy schemas survived:
  `ls tools/codegen/schemas/native/on3/on3_player_rankings.yaml tools/codegen/schemas/native/on3/on3_team_rankings.yaml` → both present.
- [ ] Run `uv run pytest tests/codegen/test_gen_on3.py -q` → passes.
- [ ] `git add tools/codegen/endpoints/on3.yaml tools/codegen/schemas/native/on3/ tests/codegen/test_gen_on3.py &&
  git commit -m "feat(on3): regenerate on3 endpoints+schemas from the RDB spec"`
- [ ] `git status` → confirm `uv.lock` did NOT drift; if it did, `git checkout uv.lock`.

---

## Task 3 — Retarget `on3_runtime._get` to the RDB host; demote the scrape to `_scrape_get`

**Files**
- Modify: `sportsdataverse/cfb/on3_runtime.py`
- Modify: `tests/test_on3_parsers.py` (re-point runtime tests to `_scrape_get`)
- Test: `tests/test_on3_rdb.py` (NEW — RDB `_get` behavior)

**Interfaces**
- Produces: `on3_runtime._get(url: str, params: Optional[Dict[str, Any]] = None, **kwargs) -> Dict | list`
  — plain GET to `url` (already the full `https://api.on3.com/public/rdb/v1/...` from the
  wrapper), browser UA, `None`-valued params dropped, returns parsed JSON (`dict` OR `list` —
  RDB returns both), `{}`/`[]`-safe on non-JSON.
- Retains (demoted): `_scrape_get(url, params=None, **kwargs) -> Dict` = the current buildId
  logic verbatim (rename of today's `_get`); plus `_extract_build_id`, `_discover_build_id`,
  `_BUILD_ID_RE`, `_RANKINGS_PATH_RE`, `_build_id`, `_headers`.
- Transport injectable: both `_get` and `_scrape_get` call `download(...)` from
  `sportsdataverse.dl_utils`, so tests monkeypatch `on3_runtime.download`.

Steps:
- [ ] In `tests/test_on3_rdb.py` write `test_get_hits_rdb_host_and_returns_json`: monkeypatch
  `on3_runtime.download` to a fake returning `_Resp(body={"list": [{"key": 1}], "pagination": {}})`,
  call `on3_runtime._get("https://api.on3.com/public/rdb/v1/commits/latest", params={"sportKey": 1, "page": None})`,
  assert the fake saw `url == "https://api.on3.com/public/rdb/v1/commits/latest"`,
  `params == {"sportKey": 1}` (None dropped), and the return is `{"list": [{"key": 1}], "pagination": {}}`.
- [ ] Add `test_get_returns_bare_list` (fake returns `_Resp(body=[{"a": 1}])`, assert `_get(...) == [{"a": 1}]`)
  and `test_get_empty_on_non_json` (fake `_Resp(text="<html>")` → `_get(...) == {}`).
- [ ] Run `uv run pytest tests/test_on3_rdb.py -q` → fails (`_get` still does buildId scrape /
  path regex → returns `{}` for the RDB URL because `_RANKINGS_PATH_RE` won't match).
- [ ] Rewrite `on3_runtime.py`: rename the existing `_get` body to `_scrape_get` (unchanged
  logic + docstring), then add a new small `_get`:

  ```python
  def _get(url: str, params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Any:
      """GET an api.on3.com RDB route and return its parsed JSON (dict or list).

      The RDB `/public/` gateway is read-only and auth-free — no buildId, no JWT.
      ``url`` is already the full ``https://api.on3.com/public/rdb/v1/...`` route
      built by the generated wrapper; ``params`` are query args (None-valued dropped).
      """
      headers = {**_headers(), **kwargs.pop("headers", {})}
      query = {k: v for k, v in (params or {}).items() if v is not None}
      try:
          resp = download(url=url, params=query, headers=headers, **kwargs)
      except NoESPNDataError:
          return {}
      try:
          body = resp.json()
      except ValueError:
          return {}
      return body if isinstance(body, (dict, list)) else {}
  ```

  Update the module docstring to describe the RDB primary path + the demoted scrape fallback.
- [ ] Run `uv run pytest tests/test_on3_rdb.py -q` → passes (`3 passed`).
- [ ] **Migration:** in `tests/test_on3_parsers.py`, change the 6 runtime tests to call
  `on3_runtime._scrape_get(...)` instead of `on3_runtime._get(...)` (the `on3_runtime` fixture
  and `_Resp` helper stay). Leave the parser tests untouched.
- [ ] Run `uv run pytest tests/test_on3_parsers.py -q` → passes (all groups green:
  parser tests unchanged, runtime tests now green against `_scrape_get`).
- [ ] `uv run mypy sportsdataverse/cfb/on3_runtime.py` → clean.
- [ ] `git add sportsdataverse/cfb/on3_runtime.py tests/test_on3_rdb.py tests/test_on3_parsers.py &&
  git commit -m "refactor(on3): retarget _get to the RDB host; demote buildId scrape to _scrape_get"`

---

## Task 4 — Rankings continuity: 4 legacy shim wrappers with `DeprecationWarning`

**Files**
- Create: `sportsdataverse/cfb/on3_rankings.py`
- Modify: `sportsdataverse/cfb/__init__.py`
- Modify: `pyproject.toml` (`[tool.mypy] files` — add `on3_rankings.py`)
- Test: `tests/test_on3_rankings_continuity.py` (NEW)
- Modify: `tests/test_on3_parsers.py` (re-point the two wrapper-wiring tests)

**Interfaces**
- Produces four hand-written shim wrappers preserving the exact public names + signatures the
  generated `on3.py` used to own (which the regenerated `on3.py` no longer defines — it now
  defines RDB `on3_<short>` names):
  - `on3_player_rankings(year, sport_slug="football", page=None, *, return_parsed=True, return_as_pandas=False, **kwargs)`
  - `on3_industry_player_rankings(...)`, `on3_team_rankings(...)`, `on3_industry_team_rankings(...)`
- Each: emits `DeprecationWarning` (points at the RDB natives
  `on3_person_sport_rankings` / `on3_players_industry_comparision` /
  `on3_team_ranking_team_rankings` / `on3_team_ranking_consensus_team_rankings`), then calls
  `on3_runtime._scrape_get(...)` and routes through `parse_on3_rankings` /
  `parse_on3_team_rankings` (unchanged behavior — legacy `_next/data` shape). No public name
  silently dropped; the RDB natives are the forward path.

Steps:
- [ ] Write `tests/test_on3_rankings_continuity.py::test_legacy_names_return_frame_and_warn`:
  monkeypatch `sportsdataverse.cfb.on3_rankings._scrape_get` to return the
  `fixtures/on3/on3_player_rankings.json` payload; call `on3_player_rankings(year=2026)` inside
  `pytest.warns(DeprecationWarning)`, assert it returns a `pl.DataFrame` with
  `df.height == 3` and `"person_name" in df.columns`. Repeat for `on3_team_rankings` with the
  team fixture.
- [ ] Add `test_all_four_legacy_names_importable` asserting the 4 names import from
  `sportsdataverse.cfb` and are callable.
- [ ] Run `uv run pytest tests/test_on3_rankings_continuity.py -q` → fails
  (`ImportError: cannot import name 'on3_rankings'`).
- [ ] Implement `sportsdataverse/cfb/on3_rankings.py`: import `_scrape_get` from
  `on3_runtime`, `parse_on3_rankings`/`parse_on3_team_rankings` from `on3_parsers`; define the 4
  wrappers. Each builds the logical `https://www.on3.com/rivals/rankings/<type>/<sport_slug>/<year>.json`
  URL (same routes `_scrape_get`/`_RANKINGS_PATH_RE` expect), warns, calls `_scrape_get`, parses.
  Full Google-style docstrings (Args/Returns/Example/See Also `recruitR`).
- [ ] In `sportsdataverse/cfb/__init__.py` add `from sportsdataverse.cfb.on3_rankings import *`
  AFTER the `from sportsdataverse.cfb.on3 import *` line (so the generated `on3.py` RDB names and
  the shim names both export; the shim owns the 4 legacy names, `on3.py` no longer defines them).
- [ ] **Migration:** in `tests/test_on3_parsers.py`, re-point
  `test_wrapper_routes_fixture_through_parser` and `test_all_four_wrappers_exported` to import the
  4 names from `sportsdataverse.cfb.on3_rankings` (monkeypatch `on3_rankings._scrape_get`), OR
  delete those two tests here and rely on the continuity test above — prefer re-pointing to keep
  coverage. Wrap the call site in `pytest.warns(DeprecationWarning)`.
- [ ] Add `sportsdataverse/cfb/on3_rankings.py` to `[tool.mypy] files` in `pyproject.toml`.
- [ ] Run `uv run pytest tests/test_on3_rankings_continuity.py tests/test_on3_parsers.py -q` → passes.
- [ ] `uv run mypy sportsdataverse/cfb/on3_rankings.py` → clean.
- [ ] `git add sportsdataverse/cfb/on3_rankings.py sportsdataverse/cfb/__init__.py pyproject.toml
  tests/test_on3_rankings_continuity.py tests/test_on3_parsers.py &&
  git commit -m "feat(on3): keep 4 rankings names via a deprecated _next/data shim (RDB continuity)"`
- [ ] `git status` → `uv.lock` unchanged (else `git checkout uv.lock`).

---

## Task 5 — `parse_on3_rdb` for the three RDB envelope shapes + real-capture tests

**Files**
- Modify: `sportsdataverse/cfb/on3_parsers.py`
- Add fixtures: `tests/fixtures/on3/team_ranking_team_rankings.json`,
  `tests/fixtures/on3/player_profile.json`, `tests/fixtures/on3/player_all_rankings.json`,
  `tests/fixtures/on3/filters_status.json` (copy the trimmed bodies from
  `sdv-internal-refs/on3/captures/_sample/`); update `tests/fixtures/on3/README.md` provenance.
- Test: `tests/test_on3_rdb.py` (extend)

**Interfaces**
- Produces: `parse_on3_rdb(raw, *, return_as_pandas=False) -> Union[pl.DataFrame, pd.DataFrame]`
  in `on3_parsers.py`, added to `__all__`. Handles:
  - **paged** `{"list": [...], "pagination": {...}}` → rows = `raw["list"]`
  - **single object** `dict` without a `list` key → rows = `[raw]`
  - **bare array** `list` → rows = `raw`
  - `None` / `{}` / `[]` / non-dict-non-list → zero-row `pl.DataFrame()`
  Reuses the existing `_rows_to_frame` (json_normalize + dedup snake names + JSON-stringify
  list/dict cells). Confirmed envelope shapes against
  `captures/_sample/{team_ranking_team_rankings,player_profile,player_all_rankings}.json`.

Steps:
- [ ] Copy the 4 capture files:
  `cp "C:/Users/saiem/Documents/sdv-internal-refs/on3/captures/_sample/team_ranking_team_rankings.json" tests/fixtures/on3/`
  (+ `player_profile.json`, `player_all_rankings.json`, `filters_status.json`).
- [ ] Write `tests/test_on3_rdb.py::test_parse_on3_rdb_paged`: load
  `team_ranking_team_rankings.json`, assert `parse_on3_rdb(...)` is a `pl.DataFrame` with
  `df.height > 0` and an `organization`-ish or `rank` column present (inspect the capture first;
  assert on a real column name from it).
- [ ] Add `test_parse_on3_rdb_single_object` (load `player_profile.json`, assert `df.height == 1`
  and a real key like `key`/`personSportKey` is a column), `test_parse_on3_rdb_bare_array`
  (load `player_all_rankings.json`, assert `df.height == 6` and `rankingType` present), and
  `test_parse_on3_rdb_empty` parametrized over `[None, {}, [], {"list": []}]` → all `height == 0`.
- [ ] Run `uv run pytest tests/test_on3_rdb.py -k parse_on3_rdb -q` → fails
  (`AttributeError`/`ImportError: parse_on3_rdb`).
- [ ] Implement `parse_on3_rdb` in `on3_parsers.py`:

  ```python
  def parse_on3_rdb(raw, *, return_as_pandas=False):
      if isinstance(raw, list):
          rows = raw
      elif isinstance(raw, dict):
          rows = raw.get("list") if isinstance(raw.get("list"), list) else [raw] if raw else []
      else:
          rows = []
      df = _rows_to_frame(rows)
      return df.to_pandas() if return_as_pandas else df
  ```

  Add `"parse_on3_rdb"` to `__all__`; full Google-style docstring documenting the three shapes +
  the zero-row contract + a `recruitR` See-Also.
- [ ] Run `uv run pytest tests/test_on3_rdb.py -q` → passes.
- [ ] `uv run mypy sportsdataverse/cfb/on3_parsers.py` → clean.
- [ ] Update `tests/fixtures/on3/README.md` with the 4 new fixtures' source URLs + capture date
  (2026-07-08, from `captures/manifest.csv`).
- [ ] `git add sportsdataverse/cfb/on3_parsers.py tests/test_on3_rdb.py tests/fixtures/on3/ &&
  git commit -m "feat(on3): add parse_on3_rdb for the RDB paged/object/array envelopes"`

---

## Task 6 — Regenerate wrappers + docs; verify `FLAT_APIS`/`_FLAT_API_DOC`; `--check`

**Files**
- Verify (no edit expected): `tools/codegen/generate.py` (`FLAT_APIS` line 1469
  `("on3", "cfb")`; `_FLAT_API_DOC` line 1960 `"on3": "On3 Recruiting (on3.com)"`)
- Modify (generated): `sportsdataverse/cfb/on3.py`, `docs/docs/cfb/reference/on3.md`
- Test: `tests/test_on3_rdb.py` (extend — wrapper wiring)

**Interfaces**
- The regenerated `on3.py` defines `on3_<short>` wrappers (host prepended to path), each routing
  through `parse_on3_rdb` when `return_parsed=True`. The 4 legacy names are NO LONGER here
  (they moved to `on3_rankings.py` in Task 4) — the codegen `__all__` must not collide with the
  shim names.

Steps:
- [ ] Optionally refine `_FLAT_API_DOC["on3"]` to `"On3 Recruit Database (api.on3.com)"` in
  `tools/codegen/generate.py` to reflect the retarget (small, correct); leave `FLAT_APIS` as is.
- [ ] Run `uv run python tools/codegen/generate.py` → regenerates `sportsdataverse/cfb/on3.py`
  + `docs/docs/cfb/reference/on3.md` (and everything else; expect only on3 files to change).
- [ ] Write `tests/test_on3_rdb.py::test_wrapper_routes_through_parse_on3_rdb`: monkeypatch
  `sportsdataverse.cfb.on3._get` to return the `team_ranking_team_rankings.json` fixture; call a
  real generated wrapper (e.g. `on3.on3_team_ranking_team_rankings(sport="football", year=2025)`)
  and assert a `pl.DataFrame` with `height > 0`; call with `return_parsed=False` and assert the
  raw dict/list comes back. (Confirm the exact wrapper name + its path params from the
  regenerated `on3.py` before finalizing the assertion.)
- [ ] Run `uv run pytest tests/test_on3_rdb.py -q` → passes.
- [ ] Run the drift gate: `uv run python tools/codegen/generate.py --check` → exits 0
  (no drift). If it flags stale docs, re-run `generate.py` and re-stage.
- [ ] `uv run pytest tests/test_on3_parsers.py tests/test_on3_rdb.py tests/test_on3_rankings_continuity.py -q`
  → all green (full on3 offline surface).
- [ ] `git add sportsdataverse/cfb/on3.py docs/docs/cfb/reference/on3.md tools/codegen/generate.py
  tests/test_on3_rdb.py &&
  git commit -m "feat(on3): regenerate RDB wrappers + reference docs"`
- [ ] `git status` → `uv.lock` unchanged.

---

## Task 7 — Returns-table descriptions (pilots) + defer the `native/on3` bucket

**Files**
- Modify: `tools/codegen/manual_column_descriptions.yaml`
- Modify: `tools/codegen/extract_residual_columns.py` (`_DEFERRED_BUCKETS`)

**Interfaces**
- Consumes: `on3-recruit-database-returns.md` (per-endpoint column reference) as the source of
  human descriptions. Produces schema-keyed description blocks in
  `manual_column_descriptions.yaml` for a handful of pilot RDB schemas; adds `"native/on3"` to
  the deferred set so the un-authored long tail does not fail the coverage ratchet (exactly how
  `nba_stats`/`wnba_stats` are handled — see `_DEFERRED_BUCKETS` at
  `extract_residual_columns.py:54`).

Steps:
- [ ] Add `"native/on3"` to `_DEFERRED_BUCKETS` in `tools/codegen/extract_residual_columns.py`
  (line 54: `{"native/nba_stats", "native/wnba_stats", "native/on3"}`).
- [ ] Author pilot description blocks in `manual_column_descriptions.yaml` keyed by the new RDB
  schema names for 2-3 flagship endpoints (`team_ranking_team_rankings`,
  `players_industry_comparision`, `commits_latest`), pulling column text from
  `on3-recruit-database-returns.md`. Keep the existing `on3_player_rankings:` /
  `on3_team_rankings:` blocks (they still back the demoted shim schemas).
- [ ] Run the coverage check the drift gate uses:
  `uv run python tools/codegen/generate.py --check` → 0 (deferred bucket keeps the ratchet green).
- [ ] Sanity-print the deferred count:
  `uv run python -c "from tools.codegen.extract_residual_columns import deferred_columns; print(len(deferred_columns()))"`
  → a positive number (the tracked follow-up).
- [ ] `git add tools/codegen/manual_column_descriptions.yaml tools/codegen/extract_residual_columns.py &&
  git commit -m "docs(on3): seed pilot RDB column descriptions; defer native/on3 bucket"`

---

## Task 8 — Live-gated smoke tests (standard `SDV_PY_LIVE_TESTS` gate)

**Files**
- Modify: `tests/test_on3_rdb.py` (add a live section) or Create `tests/cfb/test_on3_live.py`
- Consumes: `@skip_if_no_live` from `tests/conftest.py`

**Interfaces**
- Live smoke over the auth-free RDB confirmed-200 endpoints (from
  `ENDPOINTS.md` / `manifest.csv`): `on3_filters_status()`, `on3_commits_latest(sport_key=1)`,
  `on3_team_ranking_team_rankings(sport="football", year=2025)`. Each returns a non-empty
  `pl.DataFrame`. Gate: `@skip_if_no_live` (RDB is auth-free, non-JA3, residential-friendly —
  the standard gate, NOT `SDV_PY_NBA_STATS_LIVE`).

Steps:
- [ ] Write `test_on3_filters_status_live` / `test_on3_commits_latest_live` /
  `test_on3_team_ranking_live` decorated with `@skip_if_no_live`, each asserting `df.height > 0`
  and a documented column present.
- [ ] Run gated (default CI mode): `uv run pytest tests/test_on3_rdb.py -q` → the live tests SKIP.
- [ ] Run live locally (residential): `SDV_PY_LIVE_TESTS=1 uv run pytest tests/test_on3_rdb.py -k live -q`
  → passes against api.on3.com (auth-free).
- [ ] `git add tests/test_on3_rdb.py && git commit -m "test(on3): live-gated RDB smoke tests"`

---

## Task 9 — Gate sweep (polars-1x, mypy, ruff, drift) + close-out

**Files**: none new — verification pass over the whole track.

Steps:
- [ ] `uv run ruff check sportsdataverse/cfb/on3_runtime.py sportsdataverse/cfb/on3_parsers.py
  sportsdataverse/cfb/on3_rankings.py sportsdataverse/cfb/on3.py tools/codegen/gen_on3.py` → clean.
- [ ] `uv run mypy sportsdataverse/cfb/on3_runtime.py sportsdataverse/cfb/on3_parsers.py
  sportsdataverse/cfb/on3_rankings.py` → clean (the ratchet modules).
- [ ] Invoke the `polars-1x-reviewer` agent on `on3_parsers.py` + `on3_rankings.py` (union/modern
  API, explicit bool masks, no lookaround regex, no numpy-scalar downcast) — address any finding.
- [ ] `uv run python tools/codegen/generate.py --check` → 0 (final drift gate).
- [ ] Full offline on3 suite:
  `uv run pytest tests/test_on3_parsers.py tests/test_on3_rdb.py tests/test_on3_rankings_continuity.py tests/codegen/test_gen_on3.py -q`
  → all green.
- [ ] `git status` → clean tree, `uv.lock` unchanged.
- [ ] Open the PR (Conventional Commit title, no AI co-author):
  `T2: on3 retarget → api.on3.com RDB (rankings continuity preserved)`. Body notes: the 4 legacy
  rankings names now emit `DeprecationWarning` and keep working via the demoted `_next/data`
  scrape (`_scrape_get`); `native/on3` returns-table descriptions deferred (tracked follow-up).

---

## Parallel-contention notes (for the orchestrator running T1-T4 together)

- **`tools/codegen/generate.py` `FLAT_APIS` / `_FLAT_API_DOC`** are shared append points across
  T1 (pff core/shims) and T4 (`sports247_site_pages`). T2 only *verifies* `on3` is present (no
  append) and optionally tweaks the `_FLAT_API_DOC["on3"]` label — a one-line, non-colliding
  edit. Land T2's label tweak in its own commit to keep the diff isolated.
- **`tools/codegen/manual_column_descriptions.yaml`** and
  **`extract_residual_columns._DEFERRED_BUCKETS`** are shared across all four tracks. T2 appends
  `native/on3` and pilot blocks only — but a concurrent T1/T3/T4 editing the same files will
  conflict. Serialize the description/deferred-bucket edits (Task 7) or rebase them last.
- **Canonical `generate.py` re-run at integration.** After all tracks' endpoint YAML + schemas
  land, run one final `uv run python tools/codegen/generate.py && ... --check` so the generated
  wrappers/docs reflect every stem at once (stacked-PR codegen needs a full re-run — a partial
  regen leaves cross-stem drift).
```
