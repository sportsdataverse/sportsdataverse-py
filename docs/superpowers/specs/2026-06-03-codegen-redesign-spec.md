<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [API Function Codegen Redesign — Specification](#api-function-codegen-redesign--specification)
  - [1. Problem & Goals](#1-problem--goals)
    - [Current state (runtime "magic")](#current-state-runtime-magic)
    - [Goals](#goals)
  - [2. Scope boundary: thin wrappers only](#2-scope-boundary-thin-wrappers-only)
  - [3. Metadata](#3-metadata)
    - [3.1 ESPN endpoints — defined once, fanned out by scope](#31-espn-endpoints--defined-once-fanned-out-by-scope)
    - [3.2 NHL / MLB endpoints — flat list, clean `nhl_{short}` / `mlb_{short}` names](#32-nhl--mlb-endpoints--flat-list-clean-nhl_short--mlb_short-names)
    - [3.3 Field reference](#33-field-reference)
    - [3.4 Irregular-but-thin endpoints — three declarative constructs](#34-irregular-but-thin-endpoints--three-declarative-constructs)
    - [3.5 Column-schema registry (the `@return` source — point c-iii)](#35-column-schema-registry-the-return-source--point-c-iii)
    - [3.6 NHL provenance & liveness (research-grounded)](#36-nhl-provenance--liveness-research-grounded)
    - [3.7 MLB Stats API provenance & nuances (research-grounded)](#37-mlb-stats-api-provenance--nuances-research-grounded)
    - [3.8 Shared parameter registry + validation status (adopted from nba_api)](#38-shared-parameter-registry--validation-status-adopted-from-nba_api)
    - [3.9 Release-loader manifest (`releases.yaml`) — 404-safe `load_*`](#39-release-loader-manifest-releasesyaml--404-safe-load_)
  - [4. Generator](#4-generator)
  - [5. Generated output shapes](#5-generated-output-shapes)
    - [5.1 ESPN league module (with full `@return` docstring)](#51-espn-league-module-with-full-return-docstring)
    - [5.2 Shared runtime helper (hand-written, not generated)](#52-shared-runtime-helper-hand-written-not-generated)
    - [5.3 Concrete `parsed.*` module](#53-concrete-parsed-module)
    - [5.4 Docs reference pages (fully generated — points c-i…c-iv)](#54-docs-reference-pages-fully-generated--points-c-ic-iv)
    - [5.5 League `__init__.py` re-exports (point c-v)](#55-league-__init__py-re-exports-point-c-v)
  - [6. Migration plan (phased, behavior-preserving)](#6-migration-plan-phased-behavior-preserving)
  - [7. Verification strategy](#7-verification-strategy)
  - [8. Resolved decisions](#8-resolved-decisions)
  - [9. Risks](#9-risks)
  - [10. The generation script & how it's run](#10-the-generation-script--how-its-run)
  - [11. Fully automated docs website (no hand-curation)](#11-fully-automated-docs-website-no-hand-curation)
  - [12. Drift prevention — pre-commit + CI](#12-drift-prevention--pre-commit--ci)
  - [13. Prior art — `nba_api` best practices](#13-prior-art--nba_api-best-practices)
  - [14. Loader gap analysis (sportsdataverse-data releases vs `load_*`)](#14-loader-gap-analysis-sportsdataverse-data-releases-vs-load_)
  - [15. Example notebooks (`examples/notebooks/`) — expansion](#15-example-notebooks-examplesnotebooks--expansion)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# API Function Codegen Redesign — Specification

**Date:** 2026-06-03 (rev. 2026-06-04)
**Status:** Design Review — HARD GATE: do **not** implement until the user approves this spec.
**Decisions locked (user):** Scope = **Everything** (unify ESPN + NHL + MLB thin wrappers); Approach = **Fully declarative** (URL/params/docs live in metadata; retire the runtime factory); Metadata = **YAML specs** under `tools/codegen/endpoints/`.

**Rev 2026-06-04 — incorporates user review feedback:**

- (a) Clean function names (final name *is* the YAML `short`); collisions resolved declaratively. **Also generate the ESPN `hockey/nhl` surface** (`espn_nhl_*`), currently missing.
- (b) **Delete** the deprecated `nhl_api.py` (legacy `statsapi.web.nhl.com`).
- (c-i) Full runnable example per endpoint. (c-ii) Clickable concrete sample URL rendered from `example_args`. (c-iii) Per-column `@return` tables (col_name · type · description) matching the sportsdataverse-R style. (c-iv) Function names kept in parity with cfbfastR/hoopR/wehoop/fastRhockey/baseballR where a 1:1 exists. (c-v) Every generated function exported from its league `__init__.py`.

**Rev 2026-06-04b — further user feedback:**

- **No aliases.** Functions are renamed outright to the clean names; old names are dropped (breaking change, published as an old→new map in `NEWS.md`). No deprecated pass-throughs.
- **NHL/MLB → `nhl_{short}` / `mlb_{short}`** (R-export names), qualified only on collision with a reserved composite.
- **Fully automated docs website** (no hand-curated overviews) — the generator owns the entire `docs/docs/{league}/` subtree (league index + per-API reference + sidebar metadata), drift-checked.
- **Docs generation runs in pre-commit** so docs can never lag behind a YAML/schema edit.

---

## 1. Problem & Goals

### Current state (runtime "magic")

- **ESPN** (`_common_espn.py`): 127 universal + 3 NCAA + 2 football + 1 MLB *core functions* (`_site_v2_*`, web v3 `_espn_*`, `_core_v2_*`), each `(_xxx(sport, league, …params, **kwargs) -> Dict)`. `make_league_module()` → `_bind()` mutates each league module's `globals()` at import, binding `(sport, league)` via `functools.partial` and wrapping a `return_parsed`/`return_as_pandas` shim. Parser dispatch is a second table, `ENDPOINT_PARSERS` (short → parser), in `_common_espn_parsers.py`. **NHL is *not* among the bound leagues — `espn_nhl_*` does not exist.**
- **NHL** (`nhl_api_web.py`, `nhl_edge.py`, `nhl_stats_rest.py`, `nhl_records.py`): hand-written concrete `def`s + companion parser registries. Plus a deprecated `nhl_api.py` (retired `statsapi.web.nhl.com`).
- **MLB** (`mlb_api.py`): hand-written concrete `def`s + `mlb_api_parsers.py`.
- **parsed.*** (`parsed/__init__.py`): virtual modules built at import via `types.ModuleType` + `sys.modules` registration.
- **Docs**: hand-written Markdown per league; no API reference is generated; no per-column return tables.

**Pain:** ESPN functions don't exist as source (can't grep / jump-to-def / autocomplete); three drift-prone surfaces (wrapper table, parser registry, docs) hand-synced; the `espn_nhl_*` surface is simply absent; no machine-checked column documentation.

### Goals

1. **One source of truth** — declarative YAML endpoint specs (+ a column-schema registry) per API.
2. **Concrete generated code** — every `espn_*` / `nhl_*` / `mlb_*` thin wrapper is a real `def`: greppable, IDE-introspectable, fully docstringed. No `globals()` mutation / `functools.partial` / `types.ModuleType`.
3. **Unify all APIs** — ESPN (incl. **hockey/nhl**) + NHL native (api-web, edge, stats-rest, records) + MLB stats under one generator/schema.
4. **Rich generated docs** — docusaurus reference pages with full examples, clickable sample URLs, and per-column `@return` tables (sportsdataverse-R parity).
5. **CI drift detection** — `generate.py --check` fails on staleness; wired into pre-commit + CI.
6. **Clean canonical names (breaking rename, no aliases)** — functions take their clean names outright; old names are dropped. The converter emits a committed **old→new rename map** published in `NEWS.md`; consumers update once.
7. **Fully automated, drift-proof docs** — the generator owns the entire `docs/docs/{league}/` subtree (no hand-curated pages); `generate.py` runs in pre-commit so docs/code can't fall out of sync.

---

## 2. Scope boundary: thin wrappers only

The generator owns **thin endpoint wrappers**: *one endpoint → one function → raw `Dict`*, whose only logic is "build URL from params, call `download()`, optionally dispatch to a named parser."

**In scope (generated):**

| API | Host | Current source | Generated target | Names |
|---|---|---|---|---|
| ESPN site v2 | `site.api.espn.com/apis/site/v2/sports` | `_common_espn.py` | `sportsdataverse/{league}/{league}_espn_ext.py` | `espn_{prefix}_*` |
| ESPN site v2 alt | `site.api.espn.com/apis/v2/sports` | same | same | `espn_{prefix}_standings` |
| ESPN web v3 | `site.web.api.espn.com/apis/common/v3/sports` | same | same | `espn_{prefix}_athlete_*`, `espn_{prefix}_leaders` |
| ESPN core v2 | `sports.core.api.espn.com/v2/sports` | same | same | `espn_{prefix}_*` |
| NHL api-web | `api-web.nhle.com` | `nhl_api_web.py` | `sportsdataverse/nhl/nhl_api_web.py` | `nhl_web_*` |
| NHL EDGE | edge host | `nhl_edge.py` | `sportsdataverse/nhl/nhl_edge.py` | `nhl_edge_*` |
| NHL stats REST | `api.nhle.com/stats/rest` | `nhl_stats_rest.py` | `sportsdataverse/nhl/nhl_stats_rest.py` | `nhl_stats_rest_*` |
| NHL records | `records.nhl.com` | `nhl_records.py` | `sportsdataverse/nhl/nhl_records.py` | `nhl_records_*` |
| MLB stats | `statsapi.mlb.com` | `mlb_api.py` | `sportsdataverse/mlb/mlb_api.py` | `mlb_api_*` |

**ESPN leagues covered (now 8):** `nba`, `wnba`, `mbb`, `wbb`, `cfb`, `nfl`, `mlb`, **`nhl` (new — `hockey/nhl`)**. The NHL league gains a new generated `sportsdataverse/nhl/nhl_espn_ext.py` exposing `espn_nhl_*`, mirroring every other league.

**Also generated — release-asset loaders (`load_*`).** Originally out of scope, now **in scope**
as a *second generated surface* (§3.9): `load_{league}_{dataset}(seasons)` functions that
read sportsdataverse-data release parquet are templatable (URL + season loop + concat) and are
generated from a **release manifest** (`releases.yaml`), made **404-safe** (skip+warn missing
seasons). This closes the large loader gap (§14). *Composites* — `nhl_pbp`/`mlb_pbp` etc. that
**stitch multiple live API calls** — stay hand-written (not 1:1 with a release asset).

**Out of scope (stay hand-written):**

- **Parser bodies** — `_common_espn_parsers.py`, `nhl_*_parsers.py`, `mlb_api_parsers.py`. Referenced by name only.
- **Composites** — `nhl_pbp`, `nhl_schedule`, `nhl_teams`, `nhl_game_rosters`, `mlb_pbp/schedule/teams/game_rosters` (multi-call stitchers, not release-asset loaders).
- **`mlb_statcast.py`** — CSV surface; bespoke.
- **Runtime HTTP** — `dl_utils.download`.
- **Discovery** — `discover.py` / `find.py` (introspect via `dir()`; unaffected).

**Deleted (not merely left alone):**

- **`nhl_api.py`** (deprecated legacy `statsapi.web.nhl.com`). Only dependent is `nhl/__init__.py:3` (`from sportsdataverse.nhl.nhl_api import *`). Removal: delete the module, drop that import line, and remove the deprecation table rows in `docs/docs/nhl/index.md`. Verified no composite (`nhl_pbp` etc.) imports it.

**Rule of thumb:** more than "URL + params + optional named parser" ⇒ not in the YAML ⇒ stays hand-written.

---

## 3. Metadata

```
tools/codegen/
  endpoints/
    leagues.yaml          # ESPN league slug map (prefix → sport/league/scopes)
    parameters.yaml       # SHARED parameter registry (nba_api pattern — §3.8)
    espn_site_v2.yaml     # site v2 (+ site v2 alt via per-endpoint host override)
    espn_web_v3.yaml      # web v3 athlete/leaders
    espn_core_v2.yaml     # core v2
    nhl_api_web.yaml  nhl_edge.yaml  nhl_stats_rest.yaml  nhl_records.yaml
    mlb_stats.yaml
  schemas/                # per-column return tables (the @return registry)
    <schema_name>.yaml
```

> **Prior art:** this layout follows `nba_api`'s proven pipeline — `tools/stats/endpoint_analysis`
> (probe+validate→metadata) → `endpoint_py_file_generator` + `endpoint_documentation_generator` +
> `parameter_documentation_generator`, sharing a `library/mapping.py` + `parameters.py`. §13 lists
> what we adopt. Our `parameters.yaml` (§3.8), per-endpoint `status`/`last_validated`, and generated
> parameter reference page come straight from that model.

### 3.1 ESPN endpoints — defined once, fanned out by scope

```yaml
# leagues.yaml
hosts:
  site_v2:     "https://site.api.espn.com/apis/site/v2/sports"
  site_v2_alt: "https://site.api.espn.com/apis/v2/sports"
  web_v3:      "https://site.web.api.espn.com/apis/common/v3/sports"
  core_v2:     "https://sports.core.api.espn.com/v2/sports"

leagues:
  - { prefix: nba,  sport: basketball, league: nba,                        scopes: [universal] }
  - { prefix: wnba, sport: basketball, league: wnba,                       scopes: [universal] }
  - { prefix: mbb,  sport: basketball, league: mens-college-basketball,    scopes: [universal, ncaa] }
  - { prefix: wbb,  sport: basketball, league: womens-college-basketball,  scopes: [universal, ncaa] }
  - { prefix: cfb,  sport: football,   league: college-football,           scopes: [universal, ncaa, football] }
  - { prefix: nfl,  sport: football,   league: nfl,                        scopes: [universal, football] }
  - { prefix: mlb,  sport: baseball,   league: mlb,                        scopes: [universal, mlb] }
  - { prefix: nhl,  sport: hockey,     league: nhl,                        scopes: [universal] }   # NEW — empirically crawl-verified (§3.6)
```

```yaml
# espn_site_v2.yaml
api: espn_site_v2
host: site_v2
name_pattern: "espn_{prefix}_{short}"

endpoints:
  - short: scoreboard                 # final name = espn_{prefix}_scoreboard
    scope: universal
    summary: "GET /scoreboard. `dates`: YYYYMMDD, YYYYMMDD-YYYYMMDD, or a season year."
    path: "/{sport}/{league}/scoreboard"
    query_params:
      - { name: dates,       type: "int|str" }
      - { name: week,        type: "int" }
      - { name: season_type, type: "int", query_key: seasontype }
      - { name: groups,      type: "int|str" }
      - { name: limit,       type: "int", default: 500 }
    parser: parse_scoreboard
    returns_schema: scoreboard        # → schemas/scoreboard.yaml (per-column table)
    r_equivalent: { hoopR: "espn_nba_scoreboard", wehoop: "espn_wnba_scoreboard", cfbfastR: "espn_cfb_scoreboard" }
    example_args: { dates: "20240115" }   # drives the rendered call + clickable URL

  - short: teams                      # renamed from espn_{prefix}_teams_site (old name dropped)
    scope: universal
    summary: "GET /teams — all teams."
    path: "/{sport}/{league}/teams"
    query_params: [ { name: limit, type: "int", default: 1000 } ]
    parser: parse_teams
    returns_schema: teams
    example_args: {}

  - short: rankings
    scope: ncaa                       # only mbb/wbb/cfb
    exclude_leagues: []               # optional escape valve for league-specific 404s
    summary: "GET /rankings — poll rankings (NCAA leagues only)."
    path: "/{sport}/{league}/rankings"
    parser: parse_items
    returns_schema: rankings
    example_args: {}
```

`espn_core_v2.yaml` carries the `_core`-suffixed shorts where they collide with a site endpoint (`teams_core`, `standings_core`, `leaders_core`, `team_core`, `athlete_core`) — the suffix lives in the `short`, so the naming decision is **in the YAML**, not in generator logic.

### 3.2 NHL / MLB endpoints — flat list, clean `nhl_{short}` / `mlb_{short}` names

**Naming rule (per user — minimize abnormality, align to R).** Native NHL/MLB functions
default to **`nhl_{short}` / `mlb_{short}`** (dropping the current `_web`/`_edge`/
`_stats_rest`/`_records` / `_api` infixes), and the preferred `short` is the **R-package
function name** where a 1:1 endpoint exists (fastRhockey has 138 `nhl_*` exports, baseballR
88 `mlb_*` — they already use clean names like `nhl_club_schedule`, `nhl_draft`,
`mlb_attendance`). This satisfies the clean-name *and* R-parity goals at once. A qualifier is
re-introduced **only on collision** against (a) another generated endpoint or (b) a
**reserved** hand-written composite/loader (`nhl_pbp`, `nhl_teams`, `nhl_schedule`,
`load_nhl_*`, `mlb_pbp`, `mlb_schedule`, `mlb_teams`, `load_mlb_*`, `mlb_statcast_*`, …). The
qualifier matches R where R also qualifies (R itself ships `nhl_edge_goalie_detail`). The old
sdv-py names (`nhl_web_pbp`, `mlb_api_schedule`, …) are **dropped, not aliased** — recorded in
the old→new rename map (§7). The generator's uniqueness check spans generated **+ reserved**
names.

```yaml
# nhl_api_web.yaml
api: nhl_api_web
host: "https://api-web.nhle.com"
name_pattern: "nhl_{short}"          # default clean; converter qualifies on collision

endpoints:
  - short: pbp                       # → nhl_pbp (free name; matches fastRhockey::nhl_pbp)
    summary: "Play-by-play feed for one NHL game (raw api-web)."
    path: "/v1/gamecenter/{game_id}/play-by-play"
    path_params: [ { name: game_id, type: "int" } ]
    parser: parse_nhl_web_pbp
    returns_schema: nhl_pbp
    r_equivalent: { fastRhockey: "nhl_pbp" }
    example_args: { game_id: 2024020001 }

  # Collision example: `teams` → nhl_teams is a RESERVED composite, so this
  # api-web endpoint keeps a qualifier:  short: teams  →  name: nhl_web_teams

  - short: club_schedule            # no collision → clean nhl_club_schedule (matches R)
    summary: "Full-season club schedule."
    path: "/v1/club-schedule-season/{team}/{season}"
    now_variant: "/v1/club-schedule-season/{team}/now"
    path_params:
      - { name: team,   type: "str" }
      - { name: season, type: "int|str", required: false, transform: format_nhl_season }
    parser: parse_nhl_web_club_schedule
    returns_schema: nhl_club_schedule
    r_equivalent: { fastRhockey: "nhl_club_schedule" }
    example_args: { team: "TOR", season: 2025 }
```

MLB mirrors this: `mlb_api.yaml` → `name_pattern: "mlb_{short}"`; e.g. `mlb_api_attendance`
→ **`mlb_attendance`** (matches baseballR), but `mlb_api_schedule` keeps a qualifier because
`mlb_schedule` is a reserved composite. R names seed both the `short` and `r_equivalent`.

### 3.3 Field reference

| Field | Where | Meaning |
|---|---|---|
| `api` / `host` / `name_pattern` | file head | API id; host key (ESPN) or absolute base (NHL/MLB), per-endpoint overridable; function-name template. |
| `short` | endpoint | **The final function name suffix** — chosen clean during curation. ESPN keys also match `ENDPOINT_PARSERS`. |
| `r_equivalent` | endpoint | Map of `{r_package: fn_name}` for the cross-reference line in docs (point c-iv); for NHL/MLB it also seeds the clean `short`. Informational only — **not** emitted as an alias. |
| `scope` | ESPN endpoint | `universal` (default) / `ncaa` / `football` / `mlb`; emitted into a league only if in that league's `scopes`. |
| `exclude_leagues` | ESPN endpoint | Rare opt-out for a league where the endpoint 404s (e.g. web v3 gamelog on nhl). |
| `summary` | endpoint | One-line docstring lead. |
| `path` | endpoint | URL path. `{token}` = substituted; `[/{token}]` = optional segment (rendered when arg not `None`). |
| `params` | endpoint | List of **registry keys** (`parameters.yaml`, §3.8) resolved to full param defs — the DRY path (nba_api pattern). |
| `extra_params` | endpoint | Endpoint-only params declared inline (same shape as `path_params`/`query_params` entries). |
| `path_params` / `query_params` | endpoint | Inline form when not using the registry. Each: `name`, `type`, `required`(=true), `default`, `default_from`, `transform`, `query_key`, `optional_segment`, `pattern` (regex, docs/validation). `None` query values stripped pre-request. |
| `parser` | endpoint | Parser callable name (imported + dispatched under `return_parsed`). Omit → raw-`Dict`-only (no `return_parsed` kwarg). |
| `returns_schema` | endpoint | Name of a `schemas/*.yaml` column table (point c-iii). Omit → render prose only until authored. |
| `example_args` | endpoint | Arg → value map. Generator renders (1) the runnable call, (2) the concrete clickable URL. |
| `status` / `last_validated` | endpoint | `success`/`fail` + ISO date, written by the refresh/probe tools (§3.8). `fail` → skipped + warned; the date is stamped in docstring + docs. |

### 3.4 Irregular-but-thin endpoints — three declarative constructs

- **Optional trailing segment** — `/powerindex[/{team_id}]`, `/statistics[/{stat_type}]` (`optional_segment: true`).
- **Default-from-another-arg** — `cid` defaults to `event_id` (`default_from: event_id` → `c = cid if cid is not None else event_id`).
- **Mid-path optional segment / two-branch** — `season_qbr` (`[/groups/{group_id}]`) modeled as an optional segment or two named endpoints sharing a parser.

Anything not expressible this way is not a thin wrapper → stays hand-written.

### 3.5 Column-schema registry (the `@return` source — point c-iii)

```yaml
# schemas/scoreboard.yaml
schema: scoreboard
kind: dataframe                 # dataframe | dict | frames (multi-frame, e.g. summary)
description: "One row per event on the scoreboard for the requested date(s)."
columns:
  - { name: game_id,    type: integer,   description: "ESPN event id." }
  - { name: season,     type: integer,   description: "Four-digit season year." }
  - { name: game_date,  type: character, description: "ISO 8601 kickoff timestamp (UTC)." }
  # ...
```

```yaml
# schemas/summary.yaml  (multi-frame parser → list of datasets)
schema: summary
kind: frames
datasets:
  boxscore_team:   { description: "...", columns: [ { name: team_id, type: integer, description: "..." }, ... ] }
  plays:           { description: "...", columns: [ ... ] }
  # ... one block per SUMMARY_SECTION_PARSERS key
```

- **Sharing:** specific parsers (`parse_scoreboard`, `parse_team_roster`) → one schema reused by every endpoint that uses them. Generic parsers (`parse_items`, `parse_single_entity`) → a per-endpoint schema (columns reflect that endpoint's payload).
- **Bootstrapping (two sources, merged):**
  1. **Live/fixture introspection** — `extract.py --schemas` calls each parser (or flattens the raw JSON for parser-less endpoints) against a captured payload → emits `name` + inferred `type` for every column.
  2. **R `@return` roxygen mining** — `extract.py --schemas-from-r` parses the `@return` markdown tables in **fastRhockey** (170 R files) and **baseballR** (141 R files) keyed by the `r_equivalent` function name → supplies the `description` (and confirms types) for NHL/MLB columns. The two are merged (introspection = column truth, R roxygen = descriptions); gaps are hand-filled.
- **NHL/MLB are fully authored, not deferred** (per user). The `schemas/*.yaml` for every generated NHL + MLB endpoint is a committed deliverable so `@return` docs render fully — the R-roxygen merge makes that tractable. (ESPN keeps the incremental prose-fallback for its long tail.)
- **Incremental fallback:** absent/stub schema ⇒ generator renders the prose `returns` only; never blocks.
- **Raw-`Dict` endpoints:** `kind: dict` documents top-level response keys instead of DataFrame columns.

### 3.6 NHL provenance & liveness (research-grounded)

**ESPN `hockey/nhl` (`espn_nhl_*`):** empirically verified, not assumed. The ESPN
crawler (`sdv-internal-refs/_notes/crawler_output_nhl/`) hit **2,000 URLs, 100% data,
94 distinct core-v2 templates, back to 1917-18**; a backfill crawl confirmed 798×200.
The deep NHL crawl surfaced **universal core-v2 endpoints currently missing from the
factory for *all* leagues** — fold these into `scope: universal` while regenerating:
`teams/{id}/records/{N}` (axis: 0 overall · 2 conf · 3 div · 5 vs-winning · 6 last-10),
`types/{t}/groups/{g}/standings/{id}`, `calendar/{offdays,blacklist,whitelist}`,
`competitions/{cid}/relevancy`, `leaders/{id}`, `tournaments/{id}/seasons`.
NHL exception: web-v3 `athlete_gamelog` → 404 (`exclude_leagues: [nhl]`; use core-v2
`statisticslog`).

**Native NHL (api-web / edge / stats-rest / records):** the YAML is **derived from the
fastRhockey OpenAPI specs** (`hockey-dev/fastRhockey/data-raw/*_openapi.{json,yaml}`),
which stay the upstream source of truth. A `tools/codegen/openapi_to_endpoints.py`
converter emits the codegen YAML; `nhl_missing_endpoint_function_mapping.md` bridges
spec `operationId` → curated short name (`gamecenter_play_by_play` → `pbp`). EDGE is a
**path-prefix partition** of the api-web spec (`/v1/edge/`, `/v1/cat/edge/` →
`nhl_edge.yaml`; the rest → `nhl_api_web.yaml`). **The OpenAPI alone is not enough to feed
the shared `_docstring.jinja` (§4)** — it has no example values, no return columns, no R
names. So the converter **also seeds** per endpoint: `example_args` (curated — e.g. the
liveness-probe values), `r_equivalent` (fastRhockey), a `returns_schema` stub (fixture-
introspected per §3.5), and `documented_options` for vocab params. With those fields
populated, NHL functions render the full 8-section docstring + clickable example URL +
`@return` table — identical to §5.1. **Naming + R sourcing:** the converter assigns the
clean `nhl_{short}` from the fastRhockey export name (§3.2 rule), qualifies only on collision
with the reserved composite set (old name dropped, recorded in the rename map); the
`@return` column **descriptions** are mined from the fastRhockey `@return` roxygen keyed by
that R name (§3.5). records endpoints with no fastRhockey equivalent get introspected columns
plus hand-authored descriptions.

**Liveness — verified 2026-06-04** (`_notes/nhl_liveness_probe_2026-06-04.md`):
probed all 592 GET paths. **0 dead endpoints**; 540/592 return data with naive params;
the other 52 are live but param/enum/query/id-gated. This grounds the two NHL-specific
constructs below and the records-scope decision (§8).

**Two NHL-specific declarative constructs (beyond §3.4):**

- **`now_variant`** — nearly every api-web/edge path has a `.../now` (or `/current`)
  form *and* a `.../{season}/{gameType}` form. Model as one function: when the gated
  arg is `None`, render the `/now` path; else the parameterized path.

  ```yaml
  - short: club_schedule_season
    path: "/v1/club-schedule-season/{team}/{season}"
    now_variant: "/v1/club-schedule-season/{team}/now"   # used when season is None
    path_params:
      - { name: team,   type: str }
      - { name: season, type: "int|str", required: false, transform: format_nhl_season }
  ```

- **Leading `{lang}` + query API (stats-rest)** — paths start `/{lang}/…`
  (`lang` default `"en"`); the surface is Cayenne-query-driven, so endpoints carry
  `query_params` like `cayenneExp`, `sort`, `start`, `limit`, `factCayenneExp`.

**EDGE top-10 enum vocab — scraped into docstrings.** `position`/`positions`/
`category`/`sortBy`/`strength` stay **required string args** (no defaults, parser-less),
but their valid slugs are **not** in the OpenAPI spec — they live in the `*-landing`
payloads. A build-time scraper `tools/codegen/scrape_edge_vocab.py` fetches the landing
endpoints once, extracts the allowed slugs, and writes a cached
`tools/codegen/endpoints/_edge_vocab.yaml`. The generator renders these as **"documented
options"** in each top-10 function's docstring + docs page (args remain plain `str`; no
runtime validation). The vocab cache is a committed snapshot, refreshed on demand
(network-dependent → **excluded from `--check`**).

### 3.7 MLB Stats API provenance & nuances (research-grounded)

**Provenance.** The MLB YAML is **converted from `sdv-internal-refs/mlb/mlb-stats-api.openapi.yaml`**
(40 paths, host `statsapi.mlb.com`) via the same `openapi_to_endpoints.py` as NHL — *not*
`extract.py`. Unlike NHL records, MLB is **already at full parity**: 40 OpenAPI paths ↔ 40
`mlb_api_*` functions, **zero coverage gap**. So MLB generation is a *re-expression* of the
existing wrappers as concrete generated code, not an expansion — the lowest-risk surface.
As with NHL, the converter **seeds the doc fields the OpenAPI lacks** — `example_args`
(curated probe values), `r_equivalent` (baseballR), `returns_schema` stubs (fixture-
introspected), and `documented_options` for `hydrate`/`fields`/`{metaType}` — so every MLB
function renders the full §5.1 docstring (8 sections + clickable URL + `@return` table).
**Naming + R sourcing:** clean `mlb_{short}` from the baseballR export name (e.g.
`mlb_attendance`, `mlb_draft`, `mlb_game_content`), qualified only on collision with reserved
composites (`mlb_schedule`/`mlb_pbp`/`mlb_teams`); current `mlb_api_*` names are dropped (rename
map, no aliases); `@return` descriptions mined from the baseballR `@return` roxygen (141 R files).

**Liveness — verified 2026-06-04** (`_notes/mlb_liveness_probe_2026-06-04.md`): probed all
40 paths → **38/40 return data, 0 dead**. The two param-gated: `…/feed/live/diffPatch`
(needs `startTimecode`/`endTimecode`), `…/stats/streaks` (exact `streakType`/`streakSpan`
combo). Several large payloads (`draft/prospects/{year}` 4.6 MB, `sports/{id}/players`
1.5 MB) — note in docstrings; smaller than NHL records.

**MLB-specific nuances the schema must handle:**

- **`hydrate` + `fields` DSL** — the defining MLB irregularity, present on nearly every
  endpoint. `hydrate` is a nested paren-list (`team(roster(person))`) that expands related
  resources inline; `fields` is a JSON-key allow-list that trims the payload. Model both as
  **free-string `query_params`** (`transform: passthrough`); valid hydration tokens are huge
  and endpoint-specific (same "vocab-not-in-spec" shape as EDGE enums) → **document the
  common tokens per endpoint in the docstring** (sourced from toddrob99/MLB-StatsAPI
  `endpoints.py`), no runtime validation.
- **`{metaType}` meta-dispatch** — `/api/v1/{metaType}` is *one* path serving ~70 enum
  types (`leagueLeaderTypes`, `gameTypes`, `statGroups`, …; probe: `/api/v1/leagueLeaderTypes`
  → 70 items). Generated as `mlb_api_meta(meta_type)` with the valid `meta_type` vocab listed
  in the docstring (scraped via the same on-demand vocab step as EDGE, into `_mlb_meta_vocab.yaml`).
- **`/api/v1.1/` version anomaly** — only `…/feed/live` is `v1.1`; everything else `v1`. The
  per-endpoint `host`/path field already covers it.
- **`sportId` / `leagueId` / `gameType` conventions** — `sportId=1` MLB (default), `103` AL /
  `104` NL, `gameType` `R/F/D/L/W/S/A/E/PO`. Encode as documented defaults.
- **Out of scope:** `mlb_statcast.py` (Baseball Savant, CSV, 25k-row cap; its
  `statcast-api.openapi.yaml` reference stays bespoke/hand-written).

### 3.8 Shared parameter registry + validation status (adopted from nba_api)

**Parameter registry (`endpoints/parameters.yaml`).** Like nba_api's `parameters.py`/`mapping.py`,
common parameters are defined **once** and referenced by key, instead of re-declaring `season`/
`limit`/`game_id` on every endpoint. Each entry carries the canonical python name, type, default,
optional value `pattern` (regex, for docs/validation), and nullability:

```yaml
# endpoints/parameters.yaml
params:
  game_id:    { api: GameID,    type: "int|str", pattern: '^\d{10}$', required: true }
  season:     { api: season,    type: "int|str", default: most_recent_season }
  limit:      { api: limit,     type: int,       default: 500 }
  season_type:{ api: seasontype,type: int,       nullable: true }
  # … one definition reused across hundreds of endpoints
```

An endpoint then references registry params and only adds endpoint-specific ones inline:

```yaml
  - short: scoreboard
    params: [dates, week, season_type, groups, limit]   # resolved from parameters.yaml
    extra_params:                                        # endpoint-only, declared inline
      - { name: foo, type: int }
```

Benefits (all from the nba_api model): one edit fixes a param everywhere; deterministic
`API name → python name` mapping; a **generated parameter reference page** (§5.4) falls out for
free; and the regex `pattern` documents (and can validate) accepted values.

**Validation status + freshness (`status` / `last_validated`).** nba_api bakes a
`status: success|fail` and `last_validated_date` into its analysis metadata; generators **skip
non-success** endpoints and docs stamp the date. We adopt this so liveness is a **permanent,
machine-checked property** rather than a one-off probe: the refresh tools (`openapi_to_endpoints.py`,
`extract.py`, the liveness probe) write `status` + `last_validated` per endpoint into the YAML;
`generate.py` **skips/warns on `status: fail`** and renders `Last validated YYYY-MM-DD` in every
docstring + docs page. (The 2026-06-04 NHL/MLB probes seed the first `status`/`last_validated`.)

### 3.9 Release-loader manifest (`releases.yaml`) — 404-safe `load_*`

Loaders read published datasets (sportsdataverse-data releases or `*-data` raw repos), not live
APIs. Today they're hand-written from 69 `config.py` URL constants and are **not 404-safe**
(`pl.read_parquet(URL.format(season=i))` crashes on any missing season). We generate them from a
manifest instead — same metadata→code→docs pipeline, same `@return` tables and drift checks.

```yaml
# tools/codegen/endpoints/releases.yaml
bases:
  sdv_releases: "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
  raw_data:     "https://raw.githubusercontent.com/sportsdataverse/"

loaders:
  - fn: load_wnba_shots                 # league=wnba, dataset=shots
    base: sdv_releases
    url: "espn_wnba_shots/shot_locations_{season}.parquet"
    tag: espn_wnba_shots                # for drift-check vs `gh release list`
    min_season: 2002
    returns_schema: wnba_shots
    r_equivalent: { wehoop: "load_wnba_team_box" }
    example_args: { seasons: 2024 }
    notebook: "08_loaders"              # examples/notebooks page linked from the docs
    automation:                         # → docs automation badge/table
      repo: sportsdataverse/wehoop-data
      workflow: wehoop-data-nightly.yaml
    status: success
    last_validated: 2026-06-04

  - fn: load_pwhl_pbp
    base: sdv_releases
    url: "pwhl_pbp/play_by_play_{season}.parquet"
    tag: pwhl_pbp
    min_season: 2024
    returns_schema: pwhl_pbp
    r_equivalent: { fastRhockey: "load_pwhl_pbp" }
    example_args: { seasons: 2024 }
```

**404-safe generated loader** (`load_module.py.jinja`):

```python
def load_wnba_shots(seasons, return_as_pandas: bool = False):
    """Load WNBA shot-location data (sportsdataverse-data release `espn_wnba_shots`).

    Source: https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_shots
    R equivalent: wehoop::load_wnba_…   ·   Last validated 2026-06-04

    Args:
        seasons: int or iterable of seasons (>= 2002).
    Returns: polars (or pandas) DataFrame; one row per shot. (@return table from returns_schema.)
    Example:
        >>> load_wnba_shots(seasons=2024)
    """
    frames, missing = [], []
    for s in _as_season_list(seasons):
        if int(s) < 2002:
            raise SeasonNotFoundError("season cannot be less than 2002")
        url = f"{_SDV_RELEASES}espn_wnba_shots/shot_locations_{s}.parquet"
        df = _read_release_parquet(url)          # 404-safe: returns None on HTTP 404 / missing
        if df is None:
            missing.append(s); continue
        frames.append(df)
    if missing:
        cli_warn(f"load_wnba_shots: no data for season(s) {missing} (skipped)")
    out = pl.concat(frames, how="vertical_relaxed") if frames else pl.DataFrame()
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out
```

`_read_release_parquet` (a small hand-written runtime helper, like `_get`) does a HEAD/GET and
returns `None` on 404 rather than raising — **that is the "404-safe" the user asked for**: a
range like `seasons=range(2002, 2027)` returns the seasons that exist and warns the rest, instead
of crashing on the first gap.

**Manifest provenance + drift.** `extract.py --releases` seeds `releases.yaml` from the current
`config.py` URL constants **and** `gh release list -R sportsdataverse/sportsdataverse-data`, so
**every release tag gets a loader** (§14 gap). A CI check (network, not `--check`) flags
manifest tags absent from the live release list and vice-versa. Column schemas are introspected
from one season's parquet (`extract.py --schemas`), descriptions merged from R `@return`
(wehoop/hoopR/fastRhockey/baseballR). `config.py` constants are regenerated from the manifest (or
the manifest supersedes them) so there's one source of truth.

**League/dataset specifics:**

- **PWHL** — new wiring: no PWHL config/loaders today (only the defunct **PHF** predecessor);
  add `bases`/`min_season=2024` + the 15 `pwhl_*` loaders.
- **WNBA** — expand 4 → full `espn_wnba_*` + `wnba_stats_*` set (draft, shots, standings, rosters,
  officials, game_rosters, season stats, lineups, player_game_logs, coaches, …).
- **NHL/WBB/NBA/CFB** — expand to the full `nhl_*` / `espn_*` release families (§14).
- **CFB cutover** — current CFB loaders read `cfbfastR-data` (raw repo); repoint to the new
  `espn_cfb_*` releases as that pipeline lands.
- **MLB — blocked on releases.** `load_mlb_*` are explicit stubs and **no `mlb_*` releases exist**
  in sportsdataverse-data. The manifest cannot fabricate data: MLB loaders stay stubs (raise a
  clear "no release yet — use the live `mlb_api_*`/`espn_mlb_*` wrappers" message) until MLB data
  is published, then a one-line manifest add wires them.

---

## 4. Generator

```
tools/codegen/
  generate.py             # CLI: build | --check
  spec.py                 # YAML → dataclasses (Api, Endpoint, Param, League, Schema) + validation
  render.py               # Jinja env + filters (type_hint, py_repr, sample_url, md_table)
  extract.py              # one-time seeding: ESPN core fns → YAML; parsers+fixtures → schema stubs
  openapi_to_endpoints.py # convert fastRhockey OpenAPI specs → nhl_*.yaml (+ records allow-list)
  scrape_edge_vocab.py    # fetch *-landing payloads → _edge_vocab.yaml (network; on demand)
  templates/
    _docstring.jinja              # SHARED macro: renders the 8-section docstring from one endpoint
    _reference_block.jinja        # SHARED macro: renders the per-function docs block (same fields)
    espn_league_module.py.jinja   # → {league}_espn_ext.py   (imports _docstring.jinja)
    api_module.py.jinja           # → NHL/MLB concrete modules (imports _docstring.jinja)
    load_module.py.jinja          # → {league}_loaders.py — 404-safe load_* (imports _docstring.jinja)
    parsed_module.py.jinja        # → parsed/{league}.py
    reference_page.md.jinja       # → docs/docs/{league}/reference/{api}.md (imports _reference_block.jinja)
    league_index.md.jinja         # → docs/docs/{league}/index.md (overview + example-notebook links)
    loaders_page.md.jinja         # → docs/docs/{league}/reference/loaders.md (links + Mermaid diagram + automation badges/table)
    parameter_reference.md.jinja  # → docs/docs/reference/parameters.md (from parameters.yaml — nba_api pattern)
    packages_page.mdx.jinja       # → docs/docs/packages.mdx + navbar block (from projects.json)
    category_json.jinja           # → docs/docs/{league}[/reference]/_category_.json (sidebar metadata)
    init_block.py.jinja           # → managed re-export region in {league}/__init__.py
```

**Documentation-parity contract.** Every generated function — ESPN, NHL (web/edge/
stats-rest/records), MLB — renders its docstring through the **single** `_docstring.jinja`
macro, and every docs page renders through `_reference_block.jinja`. Both macros take one
normalized `Endpoint` object (§3.3 fields), so all three families emit the **identical
8 sections**: (1) summary, (2) binding note *(ESPN only)*, (3) endpoint template string,
(4) **concrete clickable example URL** (from `example_args`, `now_variant`-aware), (5) R
equivalent, (6) **Args** — every path+query param with type + description, plus
*documented-options* lines for vocab params (EDGE enums, MLB `hydrate`/`{metaType}`),
(7) **Returns** — the per-column `@return` table from `returns_schema` (or prose + size
warning when unauthored), (8) runnable Example. A doc improvement to a macro lands across
all ~1,400 functions at once; `generate.py --check` enforces it. **There is no second-class
surface — NHL/MLB get exactly the §5.1 treatment.**

**Flow:** load YAML + `parameters.yaml` + `releases.yaml` + schemas → resolve each endpoint's `params` registry keys → validate (parser names import; every `{token}` has a param; `returns_schema` resolves; registry keys exist; **no duplicate final names per league**, counting reserved composites/loaders) → **skip endpoints with `status: fail`** (warn) → render ESPN league modules + **404-safe loader modules** (from `releases.yaml`) (endpoints where `scope ∈ league.scopes` and league ∉ `exclude_leagues`) → render NHL/MLB modules → render `parsed/{league}.py` → render docs pages → update each league `__init__.py` managed region (point c-v) → `--check` diffs byte-for-byte and exits non-zero on drift.

**Determinism & formatting:** stable YAML ordering; the generator runs `ruff format` on emitted Python so `--check` and the formatter agree.

**Dependencies:** `PyYAML` + `Jinja2` in the **dev** group only; the shipped package imports neither.

---

## 5. Generated output shapes

### 5.1 ESPN league module (with full `@return` docstring)

```python
# GENERATED by tools/codegen/generate.py — DO NOT EDIT.
"""ESPN endpoint wrappers for CFB (football/college-football). Generated."""
from __future__ import annotations
from typing import Dict, Optional, Union
from sportsdataverse._codegen_runtime import _get
from sportsdataverse._common_espn_parsers import parse_scoreboard, parse_teams, ...

_SITE_V2 = "https://site.api.espn.com/apis/site/v2/sports"

__all__ = ["espn_cfb_scoreboard", "espn_cfb_teams", "espn_cfb_teams_site", ...]


def espn_cfb_scoreboard(
    dates: Optional[Union[int, str]] = None,
    week: Optional[int] = None,
    season_type: Optional[int] = None,
    groups: Optional[Union[int, str]] = None,
    limit: int = 500,
    *,
    return_parsed: bool = False,
    return_as_pandas: bool = False,
    **kwargs,
) -> Dict:
    """GET /scoreboard. `dates`: YYYYMMDD, YYYYMMDD-YYYYMMDD, or a season year.

    Bound to sport='football', league='college-football'.

    Endpoint: ``GET https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/scoreboard``
    Example URL: https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates=20240115

    R equivalent: cfbfastR::espn_cfb_scoreboard()

    Args:
        dates: ...
        return_parsed: dispatch through parse_scoreboard → polars DataFrame.
        return_as_pandas: with return_parsed, return pandas instead of polars.

    Returns (return_parsed=True): polars/pandas DataFrame, one row per event:

        | col_name  | type      | description                       |
        |:----------|:----------|:----------------------------------|
        | game_id   | integer   | ESPN event id.                    |
        | season    | integer   | Four-digit season year.           |
        | ...       | ...       | ...                               |

    Example:
        >>> espn_cfb_scoreboard(dates="20240115")
    """
    raw = _get(
        f"{_SITE_V2}/football/college-football/scoreboard",
        params={"dates": dates, "week": week, "seasontype": season_type,
                "groups": groups, "limit": limit},
        **kwargs,
    )
    if return_parsed:
        return parse_scoreboard(raw, return_as_pandas=return_as_pandas)
    return raw
```

Endpoints without a parser omit the `return_parsed`/`return_as_pandas` kwargs and the dispatch block. **No alias functions are generated** — `teams_site` simply becomes `teams` (the old name is recorded in the rename map, §7, and removed).

**NHL native module — identical doc contract** (`sportsdataverse/nhl/nhl_api_web.py`, via
`api_module.py.jinja` + the shared `_docstring.jinja`). Note the `now_variant` branch and the
concrete example URL:

```python
def nhl_web_club_schedule_season(
    team: str,
    season: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """Full-season club schedule for one NHL team.

    Endpoint: ``GET https://api-web.nhle.com/v1/club-schedule-season/{team}/{season}``
              (``season=None`` → ``/v1/club-schedule-season/{team}/now``)
    Example URL: https://api-web.nhle.com/v1/club-schedule-season/TOR/20242025

    R equivalent: fastRhockey::nhl_team_schedule()

    Args:
        team: 3-letter team abbreviation (e.g. "TOR", "BOS", "VGK").
        season: 8-digit season ("20242025") or 4-digit end year (2025 → "20242025");
            None → the current season ("/now").

    Returns: raw Dict — games[] with date, gameType, venue, home/away teams + scores.
        (no parser registered → raw Dict; return_parsed kwarg omitted.)

    Example:
        >>> nhl_web_club_schedule_season("TOR", 2025)
    """
    s = format_nhl_season(season)
    path = f"/v1/club-schedule-season/{team}/now" if s is None else f"/v1/club-schedule-season/{team}/{s}"
    return _get(f"{_NHL_API_WEB}{path}", **kwargs)
```

**MLB module — identical doc contract** (`sportsdataverse/mlb/mlb_api.py`). Note the
`hydrate`/`fields` free-string params with **documented-options** lines:

```python
def mlb_api_team_roster(
    team_id: int,
    roster_type: Optional[str] = None,
    season: Optional[Union[int, str]] = None,
    hydrate: Optional[str] = None,
    fields: Optional[str] = None,
    **kwargs,
) -> Dict:
    """Roster for one MLB team.

    Endpoint: ``GET https://statsapi.mlb.com/api/v1/teams/{teamId}/roster``
    Example URL: https://statsapi.mlb.com/api/v1/teams/147/roster?season=2024

    R equivalent: baseballr::mlb_rosters()

    Args:
        team_id: MLB team id (e.g. 147 = NYY).
        roster_type: e.g. "active", "40Man", "fullSeason" (see mlb_api_meta("rosterTypes")).
        season: 4-digit season year.
        hydrate: related-resource expansion, nested paren-list. Common options:
            "person", "person(stats)", "team". (Vocab is endpoint-specific; not validated.)
        fields: comma-separated JSON-key allow-list to trim the payload.

    Returns: raw Dict — roster[] with person, jerseyNumber, position, status.

    Example:
        >>> mlb_api_team_roster(147, season=2024, hydrate="person(stats(type=season))")
    """
    return _get(
        f"{_MLB_BASE}/api/v1/teams/{team_id}/roster",
        params={"rosterType": roster_type, "season": season, "hydrate": hydrate, "fields": fields},
        **kwargs,
    )
```

Both came from the **same** `_docstring.jinja` macro as the ESPN example — the only
differences are the data the converter fed it (`now_variant`, `documented_options`, the
`r_equivalent` package).

### 5.2 Shared runtime helper (hand-written, not generated)

`sportsdataverse/_codegen_runtime.py` — extracted `_get` / `_csv` / named transforms (`format_nhl_season`, …). Generated ESPN + NHL + MLB modules all call it (NHL `_fetch(path)` → `_get(base + path)`).

### 5.3 Concrete `parsed.*` module

`sportsdataverse/parsed/cfb.py` — generated thin wrappers that `kwargs.setdefault("return_parsed", True)` over the raw league functions; pass-through re-export for parser-less names. `parsed/__init__.py` shrinks to importing the 8 submodules; `types.ModuleType` builder deleted.

### 5.4 Docs reference pages (fully generated — points c-i…c-iv)

The generator **owns the entire `docs/docs/{league}/` subtree** (it deletes + rewrites it each
run — nothing inside is hand-edited). Per league it emits:

- `docs/docs/{league}/index.md` — generated league overview: API-surface table (each API,
  function count, base URL, season convention) + links to the reference pages. Built from the
  registry, so it never drifts (replaces the hand-curated overview). **Includes an "Examples"
  section linking the relevant `examples/notebooks/*.ipynb`** (e.g. nhl → `07_nhl_intro` + the
  loaders notebook), so every module page points at runnable tutorials (§15).
- `docs/docs/{league}/reference/{api}.md` — every function via `_reference_block.jinja`:
  signature; **Endpoint URL** + **Valid URL** (concrete clickable, from `example_args` — the
  nba_api "Valid URL" pattern); a **parameter table** with nba_api's columns *(API name · python
  var · Pattern · Required · Nullable)*; **runnable example**; **R-equivalent**; **full per-column
  `@return` table(s)** from `returns_schema` (`kind: frames` → one table per frame); a
  `Last validated YYYY-MM-DD` stamp (§3.8); and a footer link to the league's example notebook(s).
- `docs/docs/{league}/reference/loaders.md` — the **dataset-loaders page** (from `releases.yaml`),
  enriched beyond the API pages with:
  - **Links** per dataset — the source data repo, the release **tag** page, and the asset URL pattern.
  - A **data-flow diagram** (Mermaid): `scrape/raw → enrich → release asset → load_*()` so the
    pipeline is legible at a glance.
  - An **automation-status table** with one row per dataset: release tag, producing data repo +
    workflow, and a live **GitHub Actions status badge**
    (`![](https://github.com/{org}/{repo}/actions/workflows/{wf}.yml/badge.svg)`) + last-run/last-
    validated date. Sourced from a small `releases.yaml` `automation:` block (repo/workflow per tag).
  - The standard per-`load_*` block (signature, `@return` table, 404-safe note, example, notebook link).
- `docs/docs/reference/parameters.md` — generated **parameter reference** from `parameters.yaml`
  (every param: python name, API name, type, default, regex pattern, nullable) — the nba_api
  `parameter_documentation_generator` analog; reference pages link param names here.
- `docs/docs/{league}/_category_.json` + `reference/_category_.json` — Docusaurus category
  metadata (label, position, collapsed) so the **sidebar autogenerates from the directory tree**
  (no `sidebars.ts` edits — see §11).

Each page carries generated frontmatter (`title`, `sidebar_label`, `sidebar_position`,
`description`). Output is MDX-safe by construction; `render.py` defensively escapes any
free-text mined from upstream (R roxygen, OpenAPI summaries). Conceptual prose
(`architecture/`, `parsers/`, `intro.md`, `quality-of-life.md`) lives **outside** the generated
subtree and is the only hand-maintained docs.

### 5.5 League `__init__.py` re-exports (point c-v)

The generator manages a marker-delimited region so every generated module is `import *`-ed and all functions reach `sportsdataverse.{league}` (and thus `discover.list_functions`):

```python
# >>> codegen: generated espn/nhl/mlb wrappers (managed) >>>
from sportsdataverse.nhl.nhl_espn_ext import *      # NEW for nhl
from sportsdataverse.nhl.nhl_api_web import *
# <<< codegen <<<
```

Hand-written imports (loaders, pbp, parsers) outside the markers are preserved.

---

## 6. Migration plan (phased, behavior-preserving)

Each phase ends green (`pytest` + offline tests), independently committable.

1. **Scaffold** — `tools/codegen/{spec,render,generate,extract,openapi_to_endpoints,scrape_edge_vocab}.py` + templates; add `PyYAML`/`Jinja2` dev deps.
2. **Runtime helper** — extract `_codegen_runtime.py` (`_get`/`_csv`/transforms); repoint `_common_espn._get` and NHL/MLB `_fetch`/`_get`. Pure refactor.
3. **Seed ESPN YAML + parameter registry** — `extract.py` → `espn_site_v2/web_v3/core_v2.yaml` + `leagues.yaml` (incl. **nhl**) + `parameters.yaml` (shared registry, §3.8), proposing **clean shorts**; emit the **old→new rename map** (`tools/codegen/rename_map.yaml`); hand-verify §3.4 irregulars and naming.
4. **Schemas** — `extract.py --schemas` (introspection) + `extract.py --schemas-from-r` (mine fastRhockey/baseballR `@return` roxygen) → `schemas/*.yaml`. **NHL + MLB authored in full** (R-merge makes this tractable); ESPN authors the high-traffic ones, long tail prose-fallback.
5. **Generate ESPN + parity-test** — generate the **8** `{league}_espn_ext.py` (incl. new `nhl_espn_ext.py`); parity test: every pre-gen name maps through `rename_map.yaml` to exactly one post-gen name with an unchanged signature (no silent drops); swap each league file + manage `__init__.py` region; update `NEWS.md` with the rename map.
6. **Retire ESPN factory** — delete `make_league_module`, `_bind`, `_*_WRAPPERS`, the 127 core fns. Keep `ENDPOINT_PARSERS`/`parser_for`.
7. **NHL/MLB YAML + generate** — `openapi_to_endpoints.py` converts the fastRhockey specs → `nhl_api_web/edge/stats_rest/records.yaml` (records via a **curated allow-list**) **and** `sdv-internal-refs/mlb/mlb-stats-api.openapi.yaml` → `mlb_stats.yaml` (full 40-path parity); `scrape_edge_vocab.py` → `_edge_vocab.yaml`, `_mlb_meta_vocab.yaml`. Generate concrete modules; parity-test; delete hand-written endpoint bodies (parsers untouched). **Delete `nhl_api.py`** + its `__init__` import + `docs/docs/nhl/index.md` deprecation rows.
8. **parsed.* concretization** — generate `parsed/{league}.py`; reduce `parsed/__init__.py`; delete `types.ModuleType` builder; parity-test.
9. **Loaders (404-safe, §3.9 / §14)** — add the `_read_release_parquet` runtime helper; `extract.py --releases` seeds `releases.yaml` from `config.py` + `gh release list`; generate the ~90 missing `load_*` into `{league}_loaders.py` (priority **WNBA, PWHL**, then NHL/WBB/NBA/CFB); introspect loader `@return` schemas; CFB cutover to `espn_cfb_*`; MLB stays stubs; parity-test names + (network) manifest-vs-release-list check.
10. **Docs generation (deletes `create_docs.sh`)** — `generate.py` regenerates the full `docs/docs/{league}/` subtree (index + reference + `_category_.json`) **+ `docs/docs/reference/parameters.md`** (from `parameters.yaml`); switch the `docs` sidebar to `{type:'autogenerated'}` (no per-page `sidebars.ts` edits); relocate any genuinely conceptual prose out of the league dirs into `architecture/`/`parsers/`; `docusaurus build` sanity check; **delete `create_docs.sh` + `Sphinx-docs/`** (§11).
11. **Example notebooks (§15)** — update `examples/notebooks/` for the renames + add ESPN/NHL/MLB endpoint + loaders notebooks; wire headless CI execution (`nbmake`, network-gated).
12. **Tests + CI** — update `test_cli.py` / `test_espn_live.py` / `test_qol.py` to the new canonical names (old names removed; no alias fallback); add the **codegen pre-commit hook** + `generate.py --check` CI job + `docusaurus build` gate (§12); document the edit-YAML→generate→commit workflow in `CLAUDE.md`/`CONTRIBUTING`.

**Rollback:** phases 5/7/8 each replace one surface; revert that phase's commit if parity fails.

---

## 7. Verification strategy

- **Inventory parity via rename map** (5/7/8): capture `{name: str(signature)}` before the swap; assert **every** old name appears in the committed `rename_map.yaml` and resolves to exactly one post-swap name whose signature is unchanged — no silent drops, no accidental renames outside the map. **New `espn_nhl_*` and renamed NHL/MLB names are expected**; the map is the source of truth and is published in `NEWS.md`.
- **Behavior parity:** existing fixture parser tests stay green unchanged; a sampled smoke set compares a generated wrapper's payload shape to the pre-gen one.
- **Generator golden test:** `generate.py --check` (CI + pre-commit); a unit test renders a tiny fixture YAML and asserts the emitted module imports + exposes the expected `def`s.
- **Schema/doc test:** assert every `returns_schema` resolves and renders a non-empty table; assert each rendered sample URL is well-formed.
- **Doc-parity test:** across **all** families (ESPN/NHL/MLB), assert every generated function's docstring contains the 8 required sections (endpoint, example URL, R-equiv where set, Args for every param, Returns, Example) — i.e. no second-class surface. Enforced because all three templates share `_docstring.jinja`.
- **Import/collection:** `import sportsdataverse` + full `pytest` collection succeed after each phase.

---

## 8. Resolved decisions

| Question | Resolution | Rationale |
|---|---|---|
| Function naming abnormalities (a) | **Final name = YAML `short`** (clean); collisions encoded as suffixed shorts in YAML; **no aliases** — old names dropped, captured in `rename_map.yaml` + `NEWS.md` | Declarative, predictable; clean surface; breaking rename done once. |
| NHL/MLB naming pattern | **`nhl_{short}` / `mlb_{short}`**, `short` = the fastRhockey/baseballR export name; qualify only on collision with another endpoint or a reserved composite; current `nhl_web_*`/`mlb_api_*` **dropped** (rename map, no aliases) | Drops infixes, matches R 1:1 (clean name *and* r-parity in one). |
| NHL/MLB return docs | **Fully authored** `schemas/*.yaml` for every generated endpoint, descriptions mined from fastRhockey/baseballR `@return` roxygen + introspected columns | User: develop returns docs fully, not deferred. |
| Missing ESPN NHL (a) | **Generate `espn_nhl_*`** via `hockey/nhl` (new `nhl_espn_ext.py`) | Closes the gap; uniform 8-league surface. |
| Legacy `nhl_api.py` (b) | **Delete** (module + `__init__` import + docs rows) | Already superseded by `nhl_api_web`; no dependents. |
| Examples (c-i) | **Required `example_args` per endpoint** → rendered runnable call | Every function self-documents. |
| Clickable URL (c-ii) | **Render concrete URL** from `example_args` substituted into path+query | Copy-paste-testable endpoint. |
| Per-column `@return` (c-iii) | **Schema registry** (`schemas/*.yaml`), parser-keyed where stable / per-endpoint where generic; bootstrapped from fixtures; incremental | Matches sportsdataverse-R `@return` tables; declarative + reusable. |
| R-package name parity (c-iv) | **`r_equivalent` cross-ref** in docstrings/docs; for NHL/MLB the R name *is* the chosen `short` | Keeps Python ↔ R discoverability; no alias emitted. |
| `__init__` exports (c-v) | **Generator manages a marker region** import-star-ing every generated module | All generated funcs reach `sportsdataverse.{league}` + `discover`. |
| Doc parity ESPN vs NHL/MLB | **Shared `_docstring.jinja` / `_reference_block.jinja` macros** used by all family templates; converters seed `example_args`/`r_equivalent`/`returns_schema`/`documented_options` | One doc contract for all ~1,400 functions; no second-class surface; `--check` + doc-parity test enforce it. |
| Replace NHL/MLB hand-written | **Replace** (parsers stay hand-written) | Scope = Everything. |
| Native NHL YAML source | **Convert from fastRhockey OpenAPI specs** (`openapi_to_endpoints.py`); EDGE = path-prefix partition of the api-web spec | Specs are the upstream truth; no double-maintenance. |
| MLB Stats API YAML source | **Convert from `mlb-stats-api.openapi.yaml`** (40 paths, full parity — no gap); model `hydrate`/`fields` as free-string query params; `{metaType}` as one `mlb_api_meta(meta_type)` with documented vocab | OpenAPI-grounded + liveness-verified (38/40); MLB already 1:1 wrapped. |
| records.nhl.com scope (442 paths) | **Curate to a high-value subset** — the ~50 currently wrapped + clearly high-value families (franchise, draft, trophy/awards, season, player/team milestones, common streak records). Long tail stays in the OpenAPI spec for future opt-in via a converter allow-list. | 442 hyper-specific paths are mostly low-value; curation keeps the surface scannable. Liveness (430/441 live) means the long tail *can* be added anytime. |
| EDGE top-10 enum vocab | **Scrape `*-landing` payloads** → `_edge_vocab.yaml`; render valid slugs as documented options in docstrings/docs (args stay `str`, no runtime validation) | Vocab isn't in the spec; documenting it makes the leaderboards usable. Cached snapshot, network-refreshed, not CI-checked. |
| Missing parser | **Raw-`Dict`-only function + warn** | Mirrors today's `parser_for() is None`. |
| `parsed.*` mechanism | **Concrete generated files**; `types.ModuleType` deleted | Removes runtime metaprogramming. |
| Release-asset loaders (`load_*`) | **Generated + 404-safe** from `releases.yaml` (§3.9); fills the ~90-loader gap (§14); composites stay hand-written; MLB stays stubs (no releases) | One missing-season-tolerant loader per release tag; user-requested WNBA/PWHL/NHL coverage. |
| Example notebooks | **Expand + CI-execute** `examples/notebooks/` for ESPN/NHL/MLB endpoints + WNBA/MLB/PWHL loaders; hand-authored, name-checked vs generated surface (§15); **cross-linked per module** in the docs + a sidebar category | Examples can't silently break on a rename; every module page points at a runnable tutorial. |
| Loaders docs page | **Generated `loaders.md` per league** with repo/release links, a **Mermaid data-flow diagram**, and an **automation-status table with GH Actions badges** (from `releases.yaml: automation:`) (§5.4) | Surfaces pipeline health + provenance next to each dataset. |
| Sidebar package list | **Generate the "SDV Packages" navbar/sidebar from `projects.json`** (the sportsdataverse.org/packages source — all 26) instead of the hand-maintained subset (missing ~10) (§11) | Sidebar always matches the website; no manual sync. |

---

## 9. Risks

- **Extraction fidelity (ESPN).** Mitigated by explicit f-strings/signatures/docstrings + named parsers + §7 inventory parity.
- **Column-schema authoring volume.** Real effort (per-endpoint for generic parsers). Mitigated: incremental (prose fallback), fixture-seeded stubs, parser-keyed sharing for stable shapes.
- **ESPN NHL endpoint coverage.** Some endpoints 404 on `hockey/nhl` (e.g. web v3 gamelog). Mitigated: `exclude_leagues` + a per-endpoint note; 404s already return `{}` harmlessly.
- **Generated-vs-formatter drift.** Generator runs `ruff format`.
- **Naming rename breakage (intentional, no aliases).** Renaming ~230 native NHL/MLB functions + the ESPN `_site`/`_core` cleanups is a **breaking change by design**. Mitigation is *visibility*, not back-compat: a committed `rename_map.yaml` is published in `NEWS.md`, the parity test guarantees every old name maps to exactly one new name (no silent drops), and the bump is a major/minor release. Callers update once.
- **Generated LOC volume** (~1,150 ESPN + ~220 native NHL functions, with records curated to a high-value subset rather than the full 442). Accepted: YAML stays DRY; bulk is generated, never hand-edited.
- **NHL liveness (verified 2026-06-04 — 0 dead of 592 probed).** Residual gating, not dead ends: (i) EDGE top-10 enum vocab scraped from `*-landing` into docstrings (args stay `str`); (ii) stats-rest needs `cayenneExp`/report slugs → expose query params, raw-`Dict`; (iii) a few records endpoints return **100–530 MB unpaginated** (`team-by-game-stats`, …) → docstring size warning + default `start`/`limit` where the API supports it; (iv) records `/{id}` need record-system ids (resolve via the list endpoint).
- **Records curation drift.** The curated subset omits the long tail by design; the converter keeps an allow-list so additions are a one-line YAML change, and the OpenAPI spec retains the full inventory.
- **OpenAPI ≠ columns.** NHL + MLB specs give paths/params/summaries only; `@return` tables still require fixture introspection (same as ESPN).
- **MLB liveness (verified 2026-06-04 — 0 dead of 40).** Residual gating: `diffPatch` needs `startTimecode`; `stats/streaks` needs an exact param combo → document required params. `hydrate`/`fields` vocab is endpoint-specific and documented, not validated. MLB is already at full 40/40 wrapper parity, so generation carries the least risk of any surface.

---

## 10. The generation script & how it's run

`tools/codegen/generate.py` is the **single entrypoint** for all of ESPN + NHL + MLB. It is
pure-build (reads YAML + schemas + vocab caches, writes/--checks files); the network-touching
steps (`openapi_to_endpoints.py`, `scrape_edge_vocab.py`, `extract.py --schemas`) are
**separate, on-demand refresh tools** whose committed outputs `generate.py` consumes — so the
default build is deterministic and offline.

```text
                 (on-demand, network)                        (offline, deterministic)
 fastRhockey/*.openapi.yaml ─┐                          ┌── sportsdataverse/{league}/{league}_espn_ext.py
 mlb-stats-api.openapi.yaml ─┼─ openapi_to_endpoints.py │   sportsdataverse/{nhl,mlb}/<api>.py
                             │      → endpoints/*.yaml ──┤   sportsdataverse/parsed/{league}.py
 *-landing / {metaType} ─────┼─ scrape_edge_vocab.py     │   docs/docs/{league}/ (index+reference+ ┐ generate.py
                             │      → _*_vocab.yaml ──────┤      _category_.json — full subtree) │  reads only
 live payloads + R roxygen ──┴─ extract.py --schemas[-from-r]   {league}/__init__.py (managed)   ┘  committed YAML
                                    → schemas/*.yaml ────────────────────────────────────────────────────────
```

**Daily / normal use (offline, what CI runs):**

```bash
python tools/codegen/generate.py            # regenerate every module + parsed.* + docs
python tools/codegen/generate.py --check    # CI/pre-commit: fail if any committed output is stale
python tools/codegen/generate.py --only nhl # optional: limit to one league/api while iterating
```

`generate.py` (no flags) runs, in order: load+validate YAML/schemas/vocab → render the 8 ESPN
league modules → render the NHL (web/edge/stats-rest/records) + MLB modules → render
`parsed/{league}.py` → **delete + regenerate the whole `docs/docs/{league}/` subtree**
(index + `reference/*.md` + `_category_.json`) → rewrite each `__init__.py` managed region →
`ruff format` the emitted Python. `--check` does all of the above to a temp buffer and diffs
byte-for-byte (code **and** docs).

**Source-refresh use (occasional, network; outputs are committed):**

```bash
python tools/codegen/openapi_to_endpoints.py   # NHL specs + mlb-stats-api.openapi.yaml → endpoints/*.yaml
python tools/codegen/scrape_edge_vocab.py      # *-landing + {metaType} → endpoints/_edge_vocab.yaml, _mlb_meta_vocab.yaml
python tools/codegen/extract.py --schemas      # introspect live/fixture payloads → schemas/*.yaml columns
python tools/codegen/extract.py --schemas-from-r --r-root <fastRhockey|baseballr>  # merge @return descriptions
python tools/codegen/extract.py --releases     # config.py + `gh release list` → releases.yaml (loader manifest, §3.9)
python tools/codegen/extract.py --packages     # fetch sportsdataverse-web projects.json → packages snapshot (§11)
# review the diffs, then:
python tools/codegen/generate.py && pytest
```

Adding/altering an endpoint is therefore: edit the YAML (or re-run the relevant refresh tool),
`generate.py`, `pytest`, commit. The committed YAML + schemas + vocab caches are the source of
truth; `--check` guards against forgetting to regenerate.

## 11. Fully automated docs website (no hand-curation)

**Goal:** the docs site never lags the API and needs zero manual upkeep per endpoint. The
generator owns a subtree it can wholesale delete-and-rewrite; the directory structure drives
the sidebar; pre-commit + CI make staleness impossible.

**Ownership boundary (the core of robustness).**

| Path | Owner | Regeneration |
|---|---|---|
| `docs/docs/{league}/**` (index w/ example links, reference, `loaders.md`, `_category_.json`) | **generator** | deleted + rewritten every run |
| `docs/docs/reference/parameters.md` | **generator** | rewritten every run |
| `docs/docs/packages.mdx` (or sidebar section) | **generator** (from `projects.json`) | rewritten every run |
| `docs/docs/{architecture,parsers}/`, `intro.md`, `quality-of-life.md`, the example notebooks themselves | hand-written | never touched by codegen |
| `docs/sidebars.ts` | mostly static | the **`docs` sidebar uses `{type:'autogenerated'}`** so generated dirs self-register |

Because no generated file is ever hand-edited, regeneration is a safe clobber — the failure mode
of "merge generated output into a hand-curated page" cannot occur.

**Sidebar = directory structure.** Each generated dir gets a `_category_.json`
(`{"label": "...", "position": N, "collapsed": true}`); the Docusaurus `autogenerated` sidebar
builds the tree from the filesystem. Adding an endpoint → a function block appears on the API's
reference page (and a new API → a new file + category) with **no `sidebars.ts` edit**. The
generated sidebar therefore surfaces, per league: the **overview** (with example-notebook links),
the **API reference** pages, and the **Dataset loaders** page — plus the top-level **Parameters
reference**, **Examples/notebooks** category, and **Packages** section below.

**Examples & loaders in the sidebar.** The `examples/notebooks/` series is published as a docs
category (rendered notebooks, e.g. via `docusaurus-plugin-…ipynb` or pre-rendered MD) with its own
`_category_.json`, and each league overview/reference page links to its relevant notebook (§5.4,
§15). The per-league **Dataset loaders** page (§5.4) sits under the league's reference category, so
loaders are reachable both per-league and from the Examples loaders notebook.

**Generated "SportsDataVerse Packages" section (from `projects.json`).** The package list in the
navbar/sidebar must match **sportsdataverse.org/packages**, whose source of truth is
`sportsdataverse-web` `frontend/data/projects.json` (26 packages — 21 R + 5 Python: hoopR, wehoop,
cfbfastR, fastRhockey, baseballr, softballR, worldfootballR, hockeyR, sportyR, cfbplotR, mlbplotR,
soccerAnimate, ggshakeR, cfb4th, puntr, oddsapiR, gamezoneR, nwslR, recruitR, usfootballR, chessR,
sportsdataverse(-py), sportypy, collegebaseball, nwslpy, recruitr). The generator **fetches that
JSON** (the same file the website renders) and emits a `docs/docs/packages.mdx` table + a navbar
"SDV" dropdown block, so the package list is **generated, not the hand-maintained subset currently
in `docusaurus.config.ts`** (which is missing ~10: softballR, hockeyR, cfbplotR, mlbplotR, cfb4th,
puntr, gamezoneR, nwslR, usfootballR, chessR). On-demand refresh (network), committed, drift-checked.

**MDX safety by construction.** The generator emits clean MDX (it controls every byte); the
`<factory>`/brace/`{}` scrubbing that `create_docs.sh` did post-hoc is unnecessary for generated
text and is kept in `render.py` only as a defensive pass over free-text mined from upstream
(R roxygen, OpenAPI summaries).

**Versioning.** The generator targets the unversioned "next" tree (`docs/docs/`); on release,
`docusaurus docs:version X.Y.Z` snapshots it into `versioned_docs/` (frozen, never regenerated).

**`create_docs.sh` is deleted** (phase 10) — the Sphinx-apidoc pipeline (3,479-line autodoc
`index.md` dumps, omits `mlb`, still documents the deprecated `nhl_api`, CRLF) is fully replaced
by `generate.py`. *Interim, already applied:* until phase 10 lands, the live script was made safe
(skips frontmatter/curated `index.md`, prints a deprecation banner, LF-normalized) so it can't
damage pages during the transition.

## 12. Drift prevention — pre-commit + CI

Two enforcement layers so docs/code can never fall out of sync with the YAML by neglect:

**Pre-commit (local, fast, auto-fixing).** A `local` hook regenerates in place whenever a codegen
**input** changes, so a forgotten regen *fails the commit with the regenerated diff staged* — the
same ergonomics as `ruff-format`/`doctoc`:

```yaml
# .pre-commit-config.yaml
- repo: local
  hooks:
    - id: sdv-codegen
      name: regenerate API wrappers + docs from endpoint YAML
      entry: python tools/codegen/generate.py
      language: system
      pass_filenames: false
      files: ^tools/codegen/(endpoints|schemas|templates)/.*$   # inputs only → no-op otherwise
```

It runs offline (the network refresh tools are **not** in pre-commit — their committed outputs
are). Editing a `schemas/*.yaml` regenerates **both** the docstrings and the docs reference page,
so the docs cannot lag a column-doc change.

**CI (backstop, in case hooks were skipped).**

```yaml
# .github/workflows/codegen.yml
- run: python tools/codegen/generate.py --check   # fail if any committed output (code or docs) is stale
- run: cd docs && yarn install --frozen-lockfile && yarn build   # docusaurus build → catches MDX errors
- run: pytest                                                     # incl. inventory-parity + doc-parity tests
```

`--check` covers the whole generated surface (API modules + **loader modules** + `parsed.*` + the
entire `docs/docs/{league}/` subtree), so "edited YAML, forgot to regenerate, skipped the hook" is
caught before merge. (The manifest-vs-live-release-list audit is a separate **network** CI job, not
part of offline `--check`.)

---

## 13. Prior art — `nba_api` best practices

`nba_api` (`swar/nba_api`) is the reference implementation for exactly this problem and its
architecture **is** this design, proven in production. What we take, and where we go further:

**Adopted:**

| nba_api | Our equivalent |
|---|---|
| `tools/stats/endpoint_analysis/analysis.py` — probe live API, validate, write metadata JSON with `status`/`last_validated` | `openapi_to_endpoints.py` + `extract.py` + liveness probes → YAML with `status`/`last_validated` (§3.8) |
| `endpoint_py_file_generator` (string templates) | `generate.py` + Jinja `*_module.py.jinja` (§4) |
| `endpoint_documentation_generator` — per-endpoint `.md` (Endpoint URL, **Valid URL** with real params, param table, data sets, validated date) | `reference_page.md.jinja` + `_reference_block.jinja` (§5.4) — same shape |
| `parameter_documentation_generator` — generated param reference | `parameter_reference.md.jinja` → `docs/docs/reference/parameters.md` (§5.4) |
| `library/mapping.py` + `parameters.py` — **central parameter registry** (one `API name → python name`, default, regex `pattern`, nullable) | `endpoints/parameters.yaml` (§3.8) referenced by `params:` keys |
| `expected_data` = declared result-set names + column lists, captured by the analyzer | `returns_schema` (§3.5), columns captured by `extract.py --schemas` |
| `status: success/fail` gate — generators skip non-success | `generate.py` skips `status: fail` (§3.8) |
| Deterministic arg ordering (required first, nullable last) | same (§3.4 / generator) |
| Each tool documented in `docs/.../tools/.../generator.md` | a `tools/codegen/README.md` + the edit→generate→commit workflow in `CLAUDE.md` |
| Release via `gen-release.sh` + semantic-release (conventional commits → version + CHANGELOG) | informs the release flow (out of codegen scope; sdv-py already uses Conventional Commits) |

**Where we go further than nba_api:**

- **Codegen in pre-commit (§12).** nba_api runs its generators manually; we wire `generate.py` into
  pre-commit + `--check` in CI so output can't drift by neglect — the explicit user requirement.
- **Per-column `@return` *descriptions*** (§3.5), not just column *names* — nba_api lists headers
  only; we mine fastRhockey/baseballR `@return` roxygen for prose.
- **Fully generated docs site** (§11) — nba_api still ships some hand-written docs and a
  Sphinx-ish path; our generator owns the whole league subtree + autogenerated sidebar.
- **Multi-API unification** — nba_api is one host; we unify ESPN (8 leagues × 3 APIs) + NHL ×4 + MLB
  under one generator/schema.

**Deliberately *not* adopted:** `src/` layout migration (sdv-py is flat `sportsdataverse/`);
the `Endpoint` class + nested `DataSet` model (we keep thin functions + `return_parsed` polars, the
established sdv-py shape); Makefile entrypoint (our CLI is `generate.py`).

---

## 14. Loader gap analysis (sportsdataverse-data releases vs `load_*`)

Audited **2026-06-04**: `gh release list -R sportsdataverse/sportsdataverse-data` (≈120 tags) vs
the 69 `load_*` functions. Every release tag should have a 404-safe loader (§3.9); most don't.

| Family | Releases | Loaders today | Gap (generate these) |
|---|---:|---:|---|
| **PWHL** (`pwhl_*`) | 15 | **0** | **all 15** — game_info, game_rosters, goalie/skater/player/team_boxscores, officials, pbp, penalty_summary, rosters, schedules, scoring_summary, shootout, shots_by_period, three_stars |
| **WNBA** (`espn_wnba_*` + `wnba_stats_*`) | ~26 | 4 | draft, game_rosters, officials, rosters, shots, standings, player/team_season_stats + **all `wnba_stats_*`** (coaches, lineups, player_game_logs, …) |
| **NHL** (`nhl_*`) | 19 | 4 | game_info, game_rosters, goalie/skater_boxscores, linescore, officials, pbp_full, penalties, rosters, scoring, scratches, shifts, shootout, shots_by_period, three_stars |
| **WBB** (`espn_womens_college_basketball_*`) | 11 | 4 | game_rosters, officials, rosters, shots, standings, player/team_season_stats |
| **NBA** (`espn_nba_*` + `nba_stats_*`) | 12 | 4 | game_rosters, officials, shots, standings + **all `nba_stats_*`** |
| **CFB** (`espn_cfb_*`) | ~22 | 5 | adv_* (8), drives, game_rosters, injuries, linescores, play_participants, power_index, player/team_box — **+ cutover** from `cfbfastR-data` to `espn_cfb_*` |
| **MBB** (`espn_mens_college_basketball_*`) | 5 | 4 | shots |
| **NCAA baseball** (`ncaa_baseball_*`) | 2 | 0 | pbp, schedules |
| **MLB** | **0 releases** | 5 stubs | **blocked** — no data published; loaders stay stubs (§3.9) |

Net: **~90 new 404-safe loaders** generated from `releases.yaml`. Priority order (user-flagged):
**WNBA, PWHL** first, then NHL/WBB/NBA/CFB; MLB when releases exist. Legacy `ESPN`/`cfbfastR_cfb_pbp`
tags are excluded (superseded).

## 15. Example notebooks (`examples/notebooks/`) — expansion

The curated 2026 series (`01_quickstart … 07_nhl_intro`) is the maintained example surface (the
pre-2024 throwaways were removed 2026-06-04). Expand it to cover the new generated surfaces:

- **Update** existing notebooks for the **renamed** functions (no aliases — §6) and the new
  `return_parsed`/`parsed.*` ergonomics.
- **Add** notebooks for the new API expansion: an **ESPN endpoint tour** (incl. the new
  `espn_nhl_*`), **NHL native** (`nhl_*` web/edge/stats-rest/records), and **MLB Stats API**
  (`mlb_api_*`, incl. `hydrate`/`fields`).
- **Add a loaders notebook** demonstrating the new 404-safe `load_*` surface across **WNBA, MLB,
  PWHL** (and NHL/CFB) — show season-range loading with graceful skips.
- **CI-validated:** notebooks execute headless (e.g. `nbmake`/`jupyter nbconvert --execute`, the
  network ones gated like the live tests) so examples can't silently break on a rename — the same
  drift-prevention principle as §12. They are **hand-authored** (tutorials, not generated), but the
  function names/signatures they call are checked against the generated surface.
- **Cross-linked both ways (§5.4, §11):** the generator links each league's docs `index.md` +
  reference pages to the relevant notebook(s) via a `notebook:` field on endpoints/loaders (and a
  `league → notebook` map), and the notebooks series is published as its own sidebar category. So
  a reader on `espn_nhl_scoreboard`'s reference page reaches the NHL/loaders notebook in one click,
  and the notebook category is browsable from the sidebar.

---

**Status:** Ready for user review (rev 2026-06-04). No implementation until approved. After approval → `superpowers:writing-plans`.
