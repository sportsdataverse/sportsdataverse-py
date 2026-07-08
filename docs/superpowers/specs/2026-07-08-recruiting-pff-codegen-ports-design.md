<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Design — Recruiting + PFF codegen ports into sdv-py](#design--recruiting--pff-codegen-ports-into-sdv-py)
  - [Goal](#goal)
  - [Current state (verified 2026-07-08)](#current-state-verified-2026-07-08)
  - [Track 1 — `pff` (NEW; the hard one)](#track-1--pff-new-the-hard-one)
  - [Track 2 — `on3` retarget → RDB](#track-2--on3-retarget-%E2%86%92-rdb)
  - [Track 3 — `sports247` RDB expand](#track-3--sports247-rdb-expand)
  - [Track 4 — `sports247_site_pages` (NEW)](#track-4--sports247_site_pages-new)
  - [Cross-cutting mechanics](#cross-cutting-mechanics)
  - [Rollout / PR structure](#rollout--pr-structure)
  - [Risks / open questions](#risks--open-questions)
  - [Definition of done (per track)](#definition-of-done-per-track)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Design — Recruiting + PFF codegen ports into sdv-py

- **Date:** 2026-07-08
- **Status:** approved (design) → implementation plan next
- **Scope:** port four reverse-engineered OpenAPI specs (in `sdv-internal-refs` +
  mirrored to `sdv-swagger`) into sdv-py as codegen-generated flat-API stems, with
  full parsers, returns-schemas, returns-table descriptions, docs-site entries,
  tests, and the sdv-swagger mirror seam — the established `nba_stats`/`on3`/
  `sports247` pattern.

## Goal

Wrap these live surfaces with the same rigor as the NBA-stats family:

| Spec (internal-refs) | Host | Auth | New/Change |
|---|---|---|---|
| `pff/pff-premium.openapi.yaml` | `premium.pff.com/api/v1` | session cookie (Clerk `__session` + Phoenix `_premium_key`) | **NEW** |
| `on3/on3-recruit-database.openapi.yaml` | `api.on3.com/public/rdb/v1` | none (public) | **RETARGET** existing `on3` scrape stem |
| `247sports/recruit-database.openapi.yaml` | `ipa.247sports.com/rdb/v1` | guest JWT | **EXPAND** existing `sports247` stem |
| `247sports/site-pages.openapi.yaml` | `247sports.com/*.json` | none (browser TLS) | **NEW** `sports247_site_pages` stem |

Every wrapper returns a tidy `polars.DataFrame` by default (`return_as_pandas=True`
for pandas; `return_parsed=False`/`raw=True` for the raw dict), matching the
existing flat-API contract.

## Current state (verified 2026-07-08)

- **Flat-API contract:** `FLAT_APIS = [(stem, league_prefix), ...]` in
  `tools/codegen/generate.py`; each stem has `endpoints/<stem>.yaml`
  (`host`, `name_pattern: <stem>_{short}`, `parser_module`, `getter_module`,
  `runtime_imports: [_get]`, `passthrough_query`, per-endpoint `path`/`short`/
  `path_params`/`extra_params`/`returns_schema`/`summary`), returns-schemas under
  `schemas/native/<stem>/`, a `_FLAT_API_DOC` label, and a docs grouping on the
  league index. `auth: true` adds a `headers=` kwarg + auth-aware `_get` import.
- **`on3`** (prefix `cfb`): 4 endpoints, wraps the `www.on3.com/_next/data` Next.js
  scrape via `on3_runtime` (buildId discovery + stale-buildId retry). Its docstring
  wrongly assumes `api.on3.com` is auth-gated — the new spec disproves that.
- **`sports247`** (prefix `cfb`): 11 endpoints on `ipa.247sports.com/rdb/v1`;
  `sports247_runtime` mints a **guest JWT** (`GET 247sports.com/` sets a `JWT`
  cookie, ~12h TTL) via curl_cffi `impersonate="chrome"`, caches it, refreshes on
  401/403. This is the authoritative pattern to reuse.
- **Package layout:** `sportsdataverse/{nfl,cfb}/` exist; `sportsdataverse/football/`
  exists with `cfl/`, `ufl/`, `xfl/` (each an ESPN `*_espn_ext.py`). **No `aaf/`
  yet** — create `sportsdataverse/football/aaf/`.
- **Thin-league-shim precedent:** `wnba/wnba_team_roster.py` is a shim over
  `wbb/wbb_team_roster._espn_basketball_*` fixing the league slug; ESPN uses
  `make_league_module()` per league. PFF Track 1 follows this.

## Track 1 — `pff` (NEW; the hard one)

**Surface:** 46 paths. Four view families over 34 stat reports, uniform across
nfl/ncaa/aaf/ufl (schemas are the column *union across rows* — PFF omits null keys
per row). `{prefix}{report}`: `facet{report}` (By Position; `+franchiseId` By Team;
`+gameId`+`league`+`season` By Game) and `player{report}` (`player_id` **snake_case**;
`career=true` variant), plus singletons (`leagues`, `teams`, `teams/overview`,
`games`, `players`, `player/seasons`, `player/position/pivot`).

**Placement — shared core + 4 thin per-league modules** (per user):

- Generated core wrappers (league-parameterized) in `sportsdataverse/nfl/pff_core.py`
  (nfl is the `FLAT_APIS` prefix that owns the generated stem; the four public
  modules — `nfl/pff.py` included — are thin shims over this core, none "owns" it).
- Public thin modules, each binding its `league` slug and its own docs grouping:
  - `sportsdataverse/nfl/pff.py` → `league="nfl"`
  - `sportsdataverse/cfb/pff.py` → `league="ncaa"`
  - `sportsdataverse/football/aaf/pff.py` → `league="aaf"` (**new subpackage**)
  - `sportsdataverse/football/ufl/pff.py` → `league="ufl"`
- Wrapper names: `pff_{view}_{report}` (e.g. `pff_facet_passing_summary`,
  `pff_player_passing_summary`, `pff_teams_overview`, `pff_leagues`). Each league
  module exposes the same names with `league` pre-bound.

**No codegen change (simpler than a shim-mode).** The core is a *normal* generated
flat stem (module `pff_core`, prefix `nfl`, `league` as an `extra_param`). The 4
public league modules are **one-line hand-written shims** over a small runtime helper
`make_pff_league_module(globals(), "<slug>")` — a **self-contained** helper
(installs league-bound `functools.partial` + `functools.update_wrapper` wrappers
preserving `__name__`/`__qualname__`/`__doc__`). Patterned after the *now-retired*
ESPN `make_league_module` factory and the live wnba/wbb shim precedent — it does
NOT import an ESPN factory (`_common_espn.py` is now just host constants + `_get`).
This matches the wnba/wbb + ESPN precedent, keeps the shared `generate.py`
untouched, and removes any foundation dependency for the other three tracks. Docs:
the core reference generates under `nfl`; the 4 shims get short hand-written stub
pages pointing at it.

**Parsers** (`nfl/pff_parsers.py`): `parse_pff_report` (facet envelope
`{key: [rows]}` → tidy frame; matrix reports `{defenders,receivers,versus}` handled
explicitly, never silently flattened) and `parse_pff_player_detail`
(`{report: {subject, weeks|seasons, week_totals|career}}` → the per-week frame with
`subject`/`game` join keys). Empty/malformed → zero-row frame with documented schema.

**Auth (`nfl/pff_runtime.py`, `auth: true`):**

- **T0 spike (do first):** determine the *minimal* cookie set. Test whether
  `_premium_key` alone authorizes `/api/v1/*` for its TTL, or whether the 60s Clerk
  `__session` JWT must be present/refreshed per call. Outcome decides how much Clerk
  logic ships. (Hypothesis from recon: `_premium_key` carries the entitlement
  server-side; `__session` may be re-validated.)
- `_get(url, params, headers=None, cookies=None)` accepts a user-supplied cookie
  dict / `requests.Session`, sourced (precedence) from an explicit arg > env
  (`SDV_PY_PFF_PREMIUM_KEY`, `SDV_PY_PFF_SESSION`, or `SDV_PY_PFF_COOKIES`) > error
  with a clear "log in and supply cookies" message. Transport injectable for offline
  tests (like `nba_stats_runtime`). curl_cffi not required (PFF doesn't JA3-block),
  but reuse the shared retry layer.
- **Optional `pff_login(email, password)` Clerk helper** (best-effort; ships iff the
  spike says `__session` is required): drives Clerk FAPI
  (`clerk.pff.com/v1/client/sign_ins` password strategy → session token), returns a
  cookie dict. Credentials via `SDV_PY_PFF_EMAIL`/`SDV_PY_PFF_PASSWORD`, never logged.
  Marked experimental; the cookie-supply path is the supported default.

**Tests:** offline fixtures from the already-committed
`sdv-internal-refs/pff/captures/samples/*.json` (copy into `tests/fixtures/pff/`);
live tests gated `@skip_if_no_pff_live` (env `SDV_PY_PFF_LIVE=1`) — paywalled +
residential, **not** on the generic `SDV_PY_LIVE_TESTS` gate (mirror the
`skip_if_no_nba_stats_live` decision). Returns-schemas seeded from `pff-returns.md`.

## Track 2 — `on3` retarget → RDB

Regenerate the `on3` stem from `on3-recruit-database.openapi.yaml` (82 endpoints,
`api.on3.com/public/rdb/v1`, auth-free). Prefer live-validated shapes: the spec
stamps `x-source: on3_ts_api|live|call-site` + `x-confidence` + `x-live-validated`;
codegen consumes the `On3*Live` schemas where present, else the `Api`-class type.

- **Rankings continuity:** keep the 4 existing rankings wrappers working. If the RDB
  exposes equivalent rankings routes, map them; for anything RDB lacks, retain a thin
  `_next/data` shim in `on3_runtime` (demoted from primary to fallback). No public
  wrapper name is silently dropped — renamed/aliased with a `DeprecationWarning` if it
  moves.
- **Runtime:** rewrite `on3_runtime._get` to target the RDB host (plain `requests`
  with a browser UA; drop buildId discovery from the hot path). `passthrough_query`.
- **Schemas/docs:** regenerate `schemas/native/on3/` + `docs/docs/cfb/reference/on3.md`.

## Track 3 — `sports247` RDB expand

Regenerate `sports247` from the full `recruit-database.openapi.yaml`. Add the usable
routes beyond today's 11 (spec: 11/25 guest-JWT-usable — add any that resolve; keep
bearer-only routes documented but omitted from the wrapper set, or emit with a note).
Runtime unchanged (guest-JWT works). Expand `schemas/native/sports247/` + docs.
Borrow param signatures/defaults (`sport_key`, `year`, `page_size`) from recruitR-py.

## Track 4 — `sports247_site_pages` (NEW)

New stem from `site-pages.openapi.yaml` (35 routes, `247sports.com/*.json`,
auth-free front-end page-model). `FLAT_APIS += ("sports247_site_pages", "cfb")`.

- **Module:** `sportsdataverse/cfb/sports247_site_pages.py` (+ `_parsers`, `_runtime`).
- **Parser gotchas (baked in):** numeric fields serialize as **strings** (cast at the
  boundary per ID-dtype discipline); nested entities are **bare integer FKs** (it's a
  normalized graph you traverse by following each entity's `.json` sub-route — the
  parser surfaces the FK columns, does not try to inline); season path segment is
  `{year}-{Sport}` (e.g. `2022-Football`).
- **Runtime:** curl_cffi `impersonate="chrome"` (Fastly edge blocks plain requests),
  no JWT. Reuse the `sports247_runtime` transport shape.
- **Discovery seed:** recruitR-py `notes/247links.csv` + `notes/247_swagger_gets.json`
  (Institution/Location/State/TimelineEvents/League-Institutions/CompositeTeamRankings-
  Preview/Recruits routes + param patterns) to sanity-check the spec's route+param set
  and author examples.

## Cross-cutting mechanics

- **`FLAT_APIS` / `_FLAT_API_DOC`:** register `sports247_site_pages` (and the pff
  core/shims); on3 + sports247 already present. Labels: "PFF Premium Stats
  (premium.pff.com)", "247Sports Site Pages (247sports.com)".
- **Per-spec generators:** `tools/codegen/gen_pff.py`, `gen_on3.py`, `gen_sports247.py`,
  `gen_sports247_site_pages.py` — each reads the internal-refs OpenAPI (via
  `SDV_SWAGGER_REPO`/`SDV_INTERNAL_REFS_REPO` env, defaulting to the local workspace)
  and emits `endpoints/<stem>.yaml` + `schemas/native/<stem>/*.yaml`. Idempotent;
  re-capture → regenerate. Model on `gen_nba_stats.py`.
- **Returns-table descriptions:** authored in `manual_column_descriptions.yaml`
  (schema-keyed), **never** in `schemas/**.yaml` (clobbered on recapture). Seed pff
  from `pff-returns.md` (glossary + prefix decomposition); seed 247/on3 from the specs'
  returns docs. Keep the coverage ratchet green (or add the new buckets to the deferred
  set with a tracked follow-up, as nba_stats did).
- **Docs website:** `python tools/codegen/generate.py --docs` regenerates the reference
  subtree; pff shims add groupings under nfl/cfb/football-ufl/football-aaf; the 247/on3
  stems refresh under cfb. Watch the `_symbol`-span renderer bug (known, pre-existing).
- **Quality gates:** mypy files-ratchet for new typed modules; polars-1x for parsers
  (union/modern API, bool masks explicit, no lookaround regex); ID/join-key dtype
  discipline (247 string-numeric ids, pff int ids, `player_id` snake_case); guard
  `uv.lock` against silent re-lock; regenerate generated files before every push
  (drift gate).

## Rollout / PR structure

Four self-contained PRs, lowest-risk first (each = codegen regen + parser + schemas +
descriptions + docs + tests, green drift/mypy/polars gates):

1. **T3 sports247 RDB expand** — smallest, runtime unchanged, proves the regenerate loop.
2. **T4 sports247 site-pages** — new stem, new parser gotchas, no auth.
3. **T2 on3 retarget** — new host + a deprecation (rankings continuity).
4. **T1 pff** — gated on the T0 auth spike; core + 4 league shims + new `football/aaf/`.

Each PR mirrors any regenerated spec back to sdv-swagger if the internal-refs spec
changed (none expected here — specs are frozen; only the sdv-py side changes).

## Risks / open questions

- **pff auth (T0 spike):** minimal cookie set + whether the Clerk login helper is
  needed. Blocks T1 detail. Everything else is unblocked.
- **on3 rankings continuity:** confirm the RDB exposes rankings equivalent to the 4
  scrape wrappers; if not, the `_next/data` shim stays. No public name dropped silently.
- **247 site-pages FK graph depth:** the parser surfaces FKs and does not auto-traverse
  (YAGNI); document the traversal pattern rather than build a graph resolver.
- **AAF data thinness:** 2019 only, `kickoff/summary` empty — tests must tolerate
  zero-row reports for aaf.
- **Docs `_symbol` renderer bug:** pre-existing; don't chase it in this program.

## Definition of done (per track)

Wrappers generated + registered; parser returns tidy frames with documented empty-frame
schema; returns-schemas + `manual_column_descriptions.yaml` authored; docs regenerated
(drift gate green); offline fixture tests pass; live-gated tests present; mypy + polars-1x
+ ruff green; `uv.lock` unchanged; example added to the relevant intro notebook where it
fits. pff additionally: T0 spike documented, 4 league shims exercised, `football/aaf/`
created with `__init__`.
