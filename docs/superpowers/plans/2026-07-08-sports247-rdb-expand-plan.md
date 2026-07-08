<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [247Sports RDB Expansion — Implementation Plan (Track 3)](#247sports-rdb-expansion--implementation-plan-track-3)
  - [Global Constraints (copy exactly — non-negotiable)](#global-constraints-copy-exactly--non-negotiable)
  - [Route inventory (verified 2026-07-08 against the spec)](#route-inventory-verified-2026-07-08-against-the-spec)
  - [File Structure](#file-structure)
  - [Task 1 — `gen_sports247.py` generator reproduces the current 11 byte-identical](#task-1--gen_sports247py-generator-reproduces-the-current-11-byte-identical)
  - [Task 2 — Live probe classifies the candidate routes (gated) + bearer-only policy documented](#task-2--live-probe-classifies-the-candidate-routes-gated--bearer-only-policy-documented)
  - [Task 3 — Expand the allowlist + regenerate endpoints/schemas (assert 11 preserved)](#task-3--expand-the-allowlist--regenerate-endpointsschemas-assert-11-preserved)
  - [Task 4 — Parser handles any new envelope shapes + offline fixture tests](#task-4--parser-handles-any-new-envelope-shapes--offline-fixture-tests)
  - [Task 5 — Returns-table descriptions in `manual_column_descriptions.yaml`](#task-5--returns-table-descriptions-in-manual_column_descriptionsyaml)
  - [Task 6 — Regenerate wrappers + docs, drift-check, commit](#task-6--regenerate-wrappers--docs-drift-check-commit)
  - [Task 7 — Live-gated tests for the new routes (shared `skip_if_no_247_live` gate)](#task-7--live-gated-tests-for-the-new-routes-shared-skip_if_no_247_live-gate)
  - [Task 8 — Gate sweep + regression guard on the existing 11](#task-8--gate-sweep--regression-guard-on-the-existing-11)
  - [Definition of Done (Track 3)](#definition-of-done-track-3)
  - [Assumptions](#assumptions)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 247Sports RDB Expansion — Implementation Plan (Track 3)

> Written for agentic workers executing TDD. Each task: write the failing test → run it → watch it fail → implement → run it → watch it pass → commit. No task is "done" until its test is green and the drift/lint gates pass.

**Goal.** Turn the hand-authored 11-endpoint `sports247` stem into a spec-driven,
idempotent codegen stem generated from the full authoritative
`247sports/recruit-database.openapi.yaml`, and **add the guest-JWT-usable routes
beyond today's 11** (probe-confirmed reference/lookup routes). The guest-JWT
runtime is unchanged (it already works). Existing 11 wrapper names + their tests
must remain byte-stable (regression guard).

**Architecture.** Add `tools/codegen/gen_sports247.py` (modelled on
`tools/codegen/gen_nba_stats.py`) that reads the OpenAPI spec, enumerates every
`GET /rdb/v1/*` route, keeps the ones in an explicit `_GUEST_USABLE` allowlist
(the spec cannot self-classify — **every** operation carries `security: [bearer: []]`
and declares a `403`, so guest-usability is a curated status map exactly like
`nba_stats`' `_APPLICABLE = ("live",)` gate), merges hand-tuned prose from an
`_OVERRIDES` overlay, and emits `endpoints/sports247.yaml` +
`schemas/native/sports247/*.yaml`. `tools/codegen/generate.py` then renders the
wrappers/docs from that YAML unchanged. Bearer-only routes are **omitted** from
the wrapper set (functional-by-default) and documented.

**Tech stack.** Python 3.9–3.14, polars 1.x, uv, pytest; codegen = PyYAML +
Jinja; runtime transport = `curl_cffi` (lazy optional import).

## Global Constraints (copy exactly — non-negotiable)

- **polars `>=1.0,<2.0`, modern API only.** `group_by` not `groupby`;
  `with_row_index`; `map_elements(…, return_dtype=)`; `pl.len()`; `how="full",
  coalesce=True`; `cum_sum`; `str.strip_chars`. Bool masks explicit:
  `pl.col("c") == True`. **Rust/polars regex has no lookaround** — use the inline
  case toggle `(?i)prefix(?-i: NAMES)`.
- **Returns-table column descriptions live ONLY in
  `tools/codegen/manual_column_descriptions.yaml`** (schema-keyed, e.g.
  `sports247_positions:`), **NEVER** in `schemas/**.yaml` (clobbered on
  re-generation).
- **mypy files-ratchet.** New/edited hand-written typed modules
  (`gen_sports247.py`, any `sports247_parsers.py` additions) must type cleanly;
  append their path to `[tool.mypy] files = [...]` in `pyproject.toml` if not
  already listed. Generated `sports247.py` is excluded (generated).
- **uv for everything:** `uv run pytest|ruff|mypy|python`. After any
  `uv run mypy`/`pytest`, check `git status` — never let a silent `uv.lock`
  re-lock ride into a commit (`git checkout uv.lock` if it moved unintentionally).
- **Regenerate + `--check` before every commit that touched endpoint YAML,
  schemas, generator, or docstrings:** `uv run python tools/codegen/generate.py`
  then `uv run python tools/codegen/generate.py --check`. The drift gate runs in
  CI + the `sdv-codegen` pre-commit hook.
- **Conventional Commits; NO AI co-author trailer** (human is sole author).
- **`curl_cffi` is a lazy optional import** (`tests`/`all` extras) — never a hard
  runtime dep. The runtime transport stays injectable so all offline tests run
  without it.
- **sports247 tests are fully OFFLINE** (`tests/test_sports247_parsers.py` — no
  `skip_if_no_live` decorator; transport-injectable `_get`, monkeypatched
  `_mint_guest_jwt`). New offline tests follow suit. New **live** tests use a
  **new dedicated gate** `@skip_if_no_247_live` (env `SDV_PY_247_LIVE=1`) that NO
  workflow sets — the Fastly edge TLS-block on `ipa.247sports.com` is the same
  class as stats.nba.com, so it must not run in CI/datacenter (mirror the
  `skip_if_no_nba_stats_live` decision).
- **ID/join-key dtype discipline.** RDB numeric ids arrive as ints *and* numeric
  strings depending on route. Existing parser stringifies list/dict cells but
  keeps scalar ints (tests assert `team_id`/`institution_key` are integer). Do
  not silently flip an id's dtype; assert on both sides before any join.

## Route inventory (verified 2026-07-08 against the spec)

Spec: `C:/Users/saiem/Documents/sdv-internal-refs/247sports/recruit-database.openapi.yaml`.
Every operation carries `security: [bearer: []]` + a declared `403`; README
(`247sports/README.md`) is the ground truth: **guest JWT unlocks 11/25 GET
routes; the remaining ~14 stay 403 (need a logged-in/premium session).**

**Already wrapped (11 — MUST stay byte-stable):** `teams`, `institution_rankings`,
`recruits`, `transfers`, `coaches`, `transfer_portal_player_feed`,
`composite_team_ranking_feed`, `transfer_portal_team_feed`, `target_predictions`,
`sport_years`, `tags_autocomplete`.

**Candidate additions to probe (read-only reference/lookup GET routes, currently
unwrapped — likely guest-open):**

| Route | proposed short | query params |
|---|---|---|
| `GET /rdb/v1/positions` (spec L764) | `positions` | `rankingKey`, `sportKey`, `year` |
| `GET /rdb/v1/sports` (L1034) | `sports` | (none / minimal) |
| `GET /rdb/v1/year` (L934) | `years` | (global class-year list; distinct from `sport_years` = `/sports/{k}/year`) |
| `GET /rdb/v1/rankings` (L811) | `rankings` | `year`, `sportKey`, `rankingType`, … |
| `GET /rdb/v1/institutionGroups` (L151) | `institution_groups` | (minimal) |

**Bearer-only — POLICY: OMIT from wrappers, document as "not wrapped
(premium/bearer-only)":** `playerSportRankings` (GET list L441 + POST/PUT/DELETE
CRUD L380/L408 + `/{key}` L342 + `/{key}/adjustRank` L303),
`transferPlayerSportRankings` (L523), `unrankedRecruits` (L606),
`rankings/{rankingKey}` (non-GET verb, L853), `rankings/{rankingKey}/biggestMovers`
(L725), `rankings/{rankingKey}/archivedPlayerRankings` (L893),
`rankings/{rankingKey}/playerSportsUnderSpecialEvaluation` (L694),
`transferrankings/{rankingKey}/unrankedtransfers` (L1391), `rankings/publish`
(POST, L962), `tags/{prefixedKey}/photos` (L1119), `tags/{type}/{key}/photos`
(GET L1161 + POST L1213). Non-GET verbs are auto-dropped by the generator (GET
only); the GET bearer-only routes are dropped by the allowlist.

## File Structure

```
tools/codegen/
  gen_sports247.py                       # NEW — generator (reads OpenAPI spec)
  endpoints/sports247.yaml               # REGENERATED (11 → 11 + probe-confirmed)
  schemas/native/sports247/*.yaml        # REGENERATED (+ one per new route)
  manual_column_descriptions.yaml        # APPEND new-schema keys (shared file)
  generate.py                            # UNCHANGED — sports247 already in FLAT_APIS (L1470) + _FLAT_API_DOC (L1961)
sportsdataverse/cfb/
  sports247.py                           # REGENERATED wrappers (generated — do not hand-edit)
  sports247_parsers.py                   # EDIT only if a new envelope shape needs handling
  sports247_runtime.py                   # UNCHANGED (guest-JWT works); docstring route-list refreshed
tests/
  conftest.py                            # skip_if_no_247_live — IMPORT (defined in Wave 0 F1), do not redefine
  test_sports247_parsers.py              # ADD new-route offline tests (existing tests untouched)
  test_sports247_live.py                 # NEW — gated live tests for new routes
  fixtures/sports247/*.json              # ADD one real capture per new route
docs/docs/cfb/reference/sports247.md     # REGENERATED (docs)
```

Parallel-contention notes: `FLAT_APIS` / `_FLAT_API_DOC` in `generate.py` are
**unchanged for this track** (`sports247` already registered at L1470 / L1961) —
no edit, no conflict with sibling tracks. `manual_column_descriptions.yaml` is a
**shared append point** across all four recruiting/pff tracks — append only your
`sports247_*` keys, never reflow the file. The canonical
`generate.py` full re-run happens once at integration; within this track run it
to keep the tree drift-free per commit.

---

## Task 1 — `gen_sports247.py` generator reproduces the current 11 byte-identical

Idempotency contract: running the generator with `_GUEST_USABLE` = the current 11
shorts must reproduce the committed `endpoints/sports247.yaml` + all 11
`schemas/native/sports247/*.yaml` **byte-for-byte**. Hand-tuned prose (summaries,
`example_args`, param `description`s) lives in an `_OVERRIDES` overlay so the
generator merges spec-structure + curated prose.

**Files**
- Create: `tools/codegen/gen_sports247.py`
- Create (test): `tests/codegen/test_gen_sports247.py`
- Read-only inputs: `C:/Users/saiem/Documents/sdv-internal-refs/247sports/recruit-database.openapi.yaml`, current `tools/codegen/endpoints/sports247.yaml`

**Interfaces**
- Consumes: OpenAPI 3.0 spec (`paths`, per-op `parameters` with `in: path|query`,
  `name`, `schema.type`, `schema.format`). Spec path resolved via
  `SDV_INTERNAL_REFS_REPO` env, defaulting to
  `../../sdv-internal-refs/247sports/recruit-database.openapi.yaml` relative to
  repo root (mirror `gen_nba_stats.py`'s `ROOT`).
- Produces: `main()` writing `endpoints/sports247.yaml` + `schemas/native/sports247/<short>.yaml`
  via a `_write_yaml(path, doc)` helper byte-identical to `gen_nba_stats._write_yaml`
  (same `yaml.safe_dump(..., sort_keys=False, default_flow_style=False)` settings —
  verify the exact dump kwargs by reading `gen_nba_stats.py`'s `_write_yaml`).
- Module-level constants:
  - `_GUEST_USABLE: dict[str, str]` = `{operationId_or_path → short}` for the 11
    (start value; grows in Task 3).
  - `_OVERRIDES: dict[str, dict]` = per-short `{summary, example_args,
    param_descriptions, query_key_case, defaults}` matching today's YAML exactly
    (e.g. `coaches` uses `pageSize` while `recruits` uses `pagesize` — preserve
    the per-route casing; `recruits`/`transfers` default `year: 2026`,
    `sport_key: 1`, `page_size: 50`; `sport_years` has no query params).

**Steps**
- [ ] Write `tests/codegen/test_gen_sports247.py::test_regen_reproduces_committed_yaml`:
      snapshot the current `endpoints/sports247.yaml` bytes, run
      `gen_sports247.main()`, assert the file bytes are unchanged and all 11
      `schemas/native/sports247/*.yaml` are unchanged.
      ```python
      import subprocess, sys, pathlib
      ROOT = pathlib.Path(__file__).resolve().parents[2]
      def test_regen_reproduces_committed_yaml():
          ep = ROOT / "tools/codegen/endpoints/sports247.yaml"
          before = ep.read_bytes()
          subprocess.run([sys.executable, "tools/codegen/gen_sports247.py"], cwd=ROOT, check=True)
          assert ep.read_bytes() == before, "gen_sports247 must reproduce the committed YAML byte-for-byte"
      ```
- [ ] Run `uv run pytest tests/codegen/test_gen_sports247.py -q` → **fails**
      (`ModuleNotFoundError` / file not found: generator doesn't exist).
- [ ] Implement `gen_sports247.py`: load spec YAML; iterate `paths`; keep GET ops
      whose op maps into `_GUEST_USABLE`; for each, split `parameters` into
      `path_params` (`in: path`) and `extra_params` (`in: query`, `query_key` =
      raw OpenAPI name, python `name` = snake_case, `type` from `schema.type`
      mapped `integer→int`/`string→str` with `int|str` for the year path param);
      overlay `_OVERRIDES[short]` (summary/example_args/descriptions/defaults);
      emit the endpoint entry + returns-schema stub (`schema`, `kind: dataframe`,
      `columns: []` seeded from the representative capture — mirror
      `gen_nba_stats` schema block). Emit the file header comment listing the
      **bearer-only omitted routes** for provenance.
- [ ] Run `uv run pytest tests/codegen/test_gen_sports247.py -q` → **passes**
      (byte-identical). If a field re-orders, fix `_OVERRIDES`/emit order until the
      diff is empty. Confirm `git status` shows `endpoints/sports247.yaml`
      unmodified.
- [ ] `uv run ruff check tools/codegen/gen_sports247.py` and
      `uv run mypy tools/codegen/gen_sports247.py` → clean (append to
      `[tool.mypy] files` if the ratchet requires it).
- [ ] Commit: `feat(codegen): add idempotent gen_sports247.py reproducing the 11-route stem`

---

## Task 2 — Live probe classifies the candidate routes (gated) + bearer-only policy documented

Determines which of the 5 candidate reference routes return `200` under the guest
JWT (→ add) vs stay `403` (→ bearer-only bucket). Must run from a **residential
IP** with `curl_cffi` installed. If the probe cannot run in this environment, the
default is **no additions** (generator stays at 11, idempotent) and the candidate
set is carried forward as a documented TODO — the stem never ships a dead wrapper.

**Files**
- Create (throwaway, scratchpad — NOT committed): a probe script under the
  session scratchpad that calls `sports247_runtime._get` against each candidate
  URL and records `status`.
- Edit: `sportsdataverse/cfb/sports247_runtime.py` (docstring only — refresh the
  "unlocks 11 of 25" route list to the confirmed count + list bearer-only routes).

**Interfaces**
- Consumes: `sportsdataverse.cfb.sports247_runtime._get(url, auth=True)` (real
  curl_cffi transport, guest-JWT mint).
- Produces: a `{short → status}` map recorded in the plan's task notes; the
  confirmed-usable subset becomes the Task 3 allowlist delta.

**Steps**
- [ ] Write the probe (scratchpad): for each of
      `positions`, `sports`, `year`, `rankings` (with `year=2026&sportKey=1`),
      `institutionGroups`, call
      `_get("https://ipa.247sports.com/rdb/v1/<path>/", params=..., auth=True)`
      and print `status_code` + top-level shape (array vs `{key: [...]}` envelope).
- [ ] Run it from a residential shell (guest JWT mints from `247sports.com/`;
      `ipa` needs curl_cffi Chrome impersonation). Record which return `200`.
- [ ] For each `200` route, **save its real body** to
      `tests/fixtures/sports247/sports247_<short>_fb_2026.json` (or the minimal
      params variant) — real captures only (never synthetic; see Common Pitfalls).
- [ ] Refresh the `sports247_runtime.py` module docstring: correct the unlocked
      count, and add an explicit **"Bearer-only, not wrapped"** list (playerSportRankings*,
      unrankedRecruits, biggestMovers, archivedPlayerRankings,
      transferPlayerSportRankings, playerSportsUnderSpecialEvaluation,
      unrankedtransfers, photos, publish). No code change.
- [ ] Commit: `docs(sports247): document guest-usable vs bearer-only RDB route split`
      (docstring + captured fixtures).

> Fallback if no residential probe is available this session: skip fixture
> capture, leave `_GUEST_USABLE` at 11, and mark Tasks 3–7 blocked-on-probe. The
> generator + regression guard (Tasks 1, 8) still land value.

---

## Task 3 — Expand the allowlist + regenerate endpoints/schemas (assert 11 preserved)

**Files**
- Edit: `tools/codegen/gen_sports247.py` (`_GUEST_USABLE` += probe-confirmed shorts;
  `_OVERRIDES` += summary/example_args/param descriptions for each new route)
- Regenerate: `tools/codegen/endpoints/sports247.yaml`,
  `tools/codegen/schemas/native/sports247/sports247_<new>.yaml`
- Edit (test): `tests/codegen/test_gen_sports247.py`

**Interfaces**
- Consumes: the confirmed candidate subset from Task 2.
- Produces: N new endpoint entries with the exact param signatures mined from the
  spec + recruitR-py defaults (`sport_key: int = 1`, `year: int|str`,
  `page_size: int = 50` where the route paginates). New wrapper short names:
  `sports247_positions`, `sports247_sports`, `sports247_years`,
  `sports247_rankings`, `sports247_institution_groups` (only those confirmed).

**Steps**
- [ ] Extend the generator test with
      `test_existing_11_shorts_preserved`: after regen, load
      `endpoints/sports247.yaml`, assert the 11 original `short` values are all
      present and unchanged, and that each new short has a `returns_schema` +
      `parser` key.
      ```python
      import yaml
      ORIG11 = {"teams","institution_rankings","recruits","transfers","coaches",
                "transfer_portal_player_feed","composite_team_ranking_feed",
                "transfer_portal_team_feed","target_predictions","sport_years","tags_autocomplete"}
      def test_existing_11_shorts_preserved():
          doc = yaml.safe_load((ROOT/"tools/codegen/endpoints/sports247.yaml").read_text())
          shorts = {e["short"] for e in doc["endpoints"]}
          assert ORIG11 <= shorts
          assert all("returns_schema" in e and "parser" in e for e in doc["endpoints"])
      ```
- [ ] Run `uv run pytest tests/codegen/test_gen_sports247.py -q` → **fails**
      (new shorts not yet in `_GUEST_USABLE`).
- [ ] Add the confirmed shorts to `_GUEST_USABLE` + `_OVERRIDES`; run
      `uv run python tools/codegen/gen_sports247.py`.
- [ ] Run the generator test → **passes**. Inspect
      `git diff tools/codegen/endpoints/sports247.yaml` — confirm the diff is
      **additive only** (11 blocks unchanged, N new blocks appended).
- [ ] Commit: `feat(sports247): add <N> guest-usable RDB reference routes to the codegen stem`
      (generator + regenerated endpoints yaml + new returns-schemas).

---

## Task 4 — Parser handles any new envelope shapes + offline fixture tests

Most reference routes are bare arrays (`positions`, `sports`, `year` →
`[{...}]`) handled by the existing `parse_sports247_result_set` `_extract_rows`
already. Only add code if a new route ships an envelope key the current
`_LIST_KEYS = ("players","results","rankings","list","items")` tuple misses.

**Files**
- Edit (only if needed): `sportsdataverse/cfb/sports247_parsers.py`
- Edit (test): `tests/test_sports247_parsers.py`
- Inputs: the Task-2 captures in `tests/fixtures/sports247/`

**Interfaces**
- Consumes: `parse_sports247_result_set(raw, *, return_as_pandas=False)` — the
  generic parser every new route routes through (default `parser:
  parse_sports247_result_set` in the yaml).
- Produces: tidy zero-or-more-row frames; zero-row on empty/malformed.

**Steps**
- [ ] Extend the existing parametrized test
      `test_new_endpoint_fixtures_flatten_with_expected_columns` with one row per
      confirmed new route asserting `df.height > 0` and a `min_cols` subset drawn
      from the **real capture** (e.g. `positions` → `{"key","abbreviation"}`;
      confirm actual column names from the saved fixture, not guessed).
- [ ] Run `uv run pytest tests/test_sports247_parsers.py -q` → **fails** for any
      new route whose envelope key isn't in `_LIST_KEYS`, or **passes** immediately
      if all are bare arrays / known envelopes.
- [ ] If a miss: add the new envelope key to `_LIST_KEYS` (one-line change). Do
      NOT add a bespoke parser — the generic path covers all RDB shapes. Keep
      polars-1x conventions (no new regex; explicit bool masks).
- [ ] Run `uv run pytest tests/test_sports247_parsers.py -q` → **passes**
      (all offline, including the untouched original 11 tests).
- [ ] Commit (only if the parser changed):
      `fix(sports247): recognize <envelope> row key in the generic RDB parser`

---

## Task 5 — Returns-table descriptions in `manual_column_descriptions.yaml`

**Files**
- Edit: `tools/codegen/manual_column_descriptions.yaml` (append new schema keys —
  shared file; append only)

**Interfaces**
- Consumes: the new `schemas/native/sports247/sports247_<new>.yaml` column lists.
- Produces: `sports247_<new>:` blocks mapping each column → description, seeded
  from the spec's `#/components/schemas/<Type>` property descriptions
  (e.g. `Position`, `Sport`, `Ranking` component schemas in the OpenAPI file).

**Steps**
- [ ] Append one `sports247_<new>:` block per new route (schema-keyed by the
      returns-schema basename, matching the existing `sports247_teams:` /
      `sports247_recruits:` style at lines ~6380–6562), authoring a description for
      each column name emitted by the parser on the real capture. Descriptions
      here ONLY — never in `schemas/**.yaml`.
- [ ] Run `uv run python tools/codegen/generate.py` then
      `uv run python tools/codegen/generate.py --check` → the returns tables in
      `docs/docs/cfb/reference/sports247.md` populate with descriptions; drift
      gate green.
- [ ] If the coverage ratchet flags the un-authored columns, either finish them or
      add the `native/sports247` bucket to the deferred set with a tracked
      follow-up (as `nba_stats` did) — state which in the commit body.
- [ ] Commit: `docs(sports247): author returns-table descriptions for the new RDB routes`

---

## Task 6 — Regenerate wrappers + docs, drift-check, commit

**Files**
- Regenerate: `sportsdataverse/cfb/sports247.py`, `docs/docs/cfb/reference/sports247.md`
- Verify: no edit to `generate.py` (`FLAT_APIS`/`_FLAT_API_DOC` already carry `sports247`)

**Interfaces**
- Consumes: the regenerated `endpoints/sports247.yaml` + schemas.
- Produces: N new `sports247_<new>(...)` wrapper functions in the generated module
  `__all__`, each `return_parsed=True` default → polars, `return_as_pandas`,
  `return_parsed=False` → raw dict.

**Steps**
- [ ] Add `tests/test_sports247_parsers.py::test_new_wrappers_exported_and_callable`
      asserting each confirmed new wrapper imports from `sportsdataverse.cfb` and
      is callable (extend the existing `test_all_unlocked_wrappers_exported`
      pattern).
- [ ] Run `uv run pytest tests/test_sports247_parsers.py -q` → **fails**
      (wrappers not yet generated).
- [ ] Run `uv run python tools/codegen/generate.py`. Confirm
      `sportsdataverse/cfb/sports247.py` gained the new wrappers + `__all__`
      entries, and `sportsdataverse/cfb/__init__.py` re-exports them (check how the
      existing 11 are re-exported and match it).
- [ ] Run `uv run pytest tests/test_sports247_parsers.py -q` → **passes**.
- [ ] Run `uv run python tools/codegen/generate.py --check` → drift gate green
      (watch the known pre-existing `_symbol`-span renderer noise; ignore it).
- [ ] `git status` — confirm `uv.lock` did not move.
- [ ] Commit: `feat(sports247): regenerate wrappers + docs for the expanded RDB stem`

---

## Task 7 — Live-gated tests for the new routes (shared `skip_if_no_247_live` gate)

**Files**
- Create: `tests/test_sports247_live.py`

**Interfaces**
- Consumes: real `ipa.247sports.com` via the unchanged guest-JWT runtime, and
  `skip_if_no_247_live` **imported from `tests/conftest.py`** — this gate is defined
  once in **Wave 0 F1** (master plan), shared with T4 site-pages. Do NOT redefine it
  here (Wave 0 runs before this track, so it already exists; if for some reason it
  does not, add it to `conftest.py` per the F1 snippet — env `SDV_PY_247_LIVE`).

**Steps**
- [ ] `from tests.conftest import skip_if_no_247_live`. Write `tests/test_sports247_live.py` with `pytestmark = skip_if_no_247_live`
      — one smoke test per new wrapper asserting `df.height >= 0` (tolerate
      zero-row: some reference routes may be sport-specific) and expected columns
      present. Keep it resilient to upstream flakiness.
- [ ] Run `uv run pytest tests/test_sports247_live.py -q` → **skipped**
      (gate off — correct in CI). Then, from a residential shell,
      `SDV_PY_247_LIVE=1 uv run pytest tests/test_sports247_live.py -q` →
      **passes** against the real API. Record the residential run result.
- [ ] Confirm no workflow sets `SDV_PY_247_LIVE` (grep `.github/workflows/`) — the
      gate must stay CI-off.
- [ ] Commit: `test(sports247): add residential-gated live tests for the new RDB routes`

---

## Task 8 — Gate sweep + regression guard on the existing 11

**Files** (no new files — verification + fixups)

**Steps**
- [ ] **Regression guard (the load-bearing assertion):** run the full existing
      offline suite untouched —
      `uv run pytest tests/test_sports247_parsers.py -q` → all original tests
      (`test_parse_sports247_teams_real_capture`,
      `test_parse_sports247_institution_rankings_real_capture`,
      `test_wrappers_route_fixtures_through_parsers`,
      `test_all_unlocked_wrappers_exported`, the runtime `_get` tests, etc.) still
      **pass**. The 11 wrapper names and their behavior are unchanged.
- [ ] `uv run ruff check sportsdataverse/cfb/ tools/codegen/gen_sports247.py`
      → clean (ruff autofix strips a not-yet-used import — add imports in the same
      edit as first use).
- [ ] `uv run mypy tools/codegen/gen_sports247.py sportsdataverse/cfb/sports247_parsers.py`
      → clean (per the files-ratchet).
- [ ] **polars-1x review** the parser delta (if Task 4 changed it): invoke the
      `polars-1x-reviewer` agent on `sportsdataverse/cfb/sports247_parsers.py` —
      no 0.18 API, explicit bool masks, no lookaround regex.
- [ ] Final drift check: `uv run python tools/codegen/generate.py --check` → green.
- [ ] `git status` clean except intended files; `uv.lock` unmoved.
- [ ] `uv run pytest tests/codegen/test_gen_sports247.py tests/test_sports247_parsers.py -q`
      → all green. Done-for-real.

---

## Definition of Done (Track 3)

- `gen_sports247.py` regenerates `endpoints/sports247.yaml` idempotently from the
  full OpenAPI spec; re-running with the 11-only allowlist is byte-identical.
- N probe-confirmed guest-usable reference routes added as `sports247_<new>`
  wrappers (or, if no residential probe this session, Tasks 3–7 documented as
  blocked-on-probe with the candidate set + procedure carried forward — Tasks 1 &
  8 still land).
- Bearer-only routes **omitted** from the wrapper set and **documented** in
  `sports247_runtime.py` + the generated `endpoints/sports247.yaml` header comment
  (chosen policy: functional-by-default wrapper set).
- The existing 11 wrapper names + `tests/test_sports247_parsers.py` pass unchanged
  (regression guard).
- Returns-schemas + `manual_column_descriptions.yaml` authored for new columns;
  docs regenerated; drift/`--check`, ruff, mypy, polars-1x all green; `uv.lock`
  unchanged.
- Live tests present under the new CI-off `SDV_PY_247_LIVE` gate.

## Assumptions

1. **Guest-usability is a curated allowlist, not spec-derived** — every RDB op
   carries `security: [bearer: []]` + a `403`, so the spec cannot self-classify;
   the README's "11/25" is authoritative and expansion requires a live probe. This
   is the exact analog of `nba_stats`' `_APPLICABLE=("live",)` capture-confirmed
   gate.
2. The 5 candidate reference routes (`positions`, `sports`, `year`, `rankings`,
   `institutionGroups`) are the realistic guest-open additions the original recon
   didn't exhaustively probe; the actual added set is whatever returns `200` under
   the guest JWT.
3. Idempotency to today's hand-tuned YAML requires an `_OVERRIDES` prose overlay in
   the generator (per-route summaries/examples/param descriptions/query-key
   casing) — pure spec-derivation won't reproduce the curated strings.
4. New live tests need a new dedicated gate (`SDV_PY_247_LIVE`) because
   `ipa.247sports.com`'s Fastly TLS-block is the same datacenter-hostile class as
   stats.nba.com; the generic `SDV_PY_LIVE_TESTS` (set by CI) must not run them.
5. `FLAT_APIS`/`_FLAT_API_DOC` need no edit (sports247 already registered), so this
   track has no `generate.py` merge contention with the sibling recruiting/pff
   tracks; `manual_column_descriptions.yaml` is the only shared append point.
