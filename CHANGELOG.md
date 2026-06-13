<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Unreleased](#unreleased)
  - [CFB — cross-source crosswalk loaders (`load_cfb_*_crosswalk`)](#cfb--cross-source-crosswalk-loaders-load_cfb__crosswalk)
  - [ESPN — NCAA men's & women's college hockey (`espn_mch_*`, `espn_wch_*`)](#espn--ncaa-mens--womens-college-hockey-espn_mch_-espn_wch_)
  - [ESPN — NCAA college baseball + softball (`espn_college_baseball_*`, `espn_college_softball_*`)](#espn--ncaa-college-baseball--softball-espn_college_baseball_-espn_college_softball_)
- [0.0.58 Release: June 12, 2026](#0058-release-june-12-2026)
  - [Loaders — NHL core + new NBA/MBB datasets aligned to `sportsdataverse-data` releases](#loaders--nhl-core--new-nbambb-datasets-aligned-to-sportsdataverse-data-releases)
  - [Robustness & infrastructure — typing, CI gates, HTTP, deprecation policy](#robustness--infrastructure--typing-ci-gates-http-deprecation-policy)
  - [The Odds API wrappers (`sportsdataverse.odds`, `toa_*`)](#the-odds-api-wrappers-sportsdataverseodds-toa_)
  - [Yahoo Sports college football wrappers (`yahoo_cfb_*`)](#yahoo-sports-college-football-wrappers-yahoo_cfb_)
  - [NFL — `api.nfl.com` wrappers cut over to generated; "NFL.com API" docs grouping](#nfl--apinflcom-wrappers-cut-over-to-generated-nflcom-api-docs-grouping)
  - [NFL — automatic `api.nfl.com` token caching + `NFL_ACCESS_TOKEN` override](#nfl--automatic-apinflcom-token-caching--nfl_access_token-override)
  - [Documentation — `api.nfl.com` OpenAPI spec](#documentation--apinflcom-openapi-spec)
  - [Bug fixes](#bug-fixes)
  - [Internal — Fox data key single-sourced](#internal--fox-data-key-single-sourced)
- [0.0.57 Release: June 10, 2026](#0057-release-june-10-2026)
  - [Fox Sports Bifrost wrappers (CFB, NBA, MBB, NHL, MLB)](#fox-sports-bifrost-wrappers-cfb-nba-mbb-nhl-mlb)
    - [CFB — Fox as a backup source for the EPA/WPA play processor (`fox_cfb_play_process`)](#cfb--fox-as-a-backup-source-for-the-epawpa-play-processor-fox_cfb_play_process)
- [0.0.56 Release: June 9, 2026](#0056-release-june-9-2026)
  - [HockeyTech — live multi-league scraper (PWHL + AHL/OHL/WHL/QMJHL) + on-ice/Corsi/TOI analytics](#hockeytech--live-multi-league-scraper-pwhl--ahlohlwhlqmjhl--on-icecorsitoi-analytics)
  - [NFL — Next Gen Stats (`nfl_ngs_*`) + api.nfl.com football/v2 (`nfl_*`) modules](#nfl--next-gen-stats-nfl_ngs_--apinflcom-footballv2-nfl_-modules)
  - [NFL — restored the api.nfl.com game schedule + play-by-play wrappers](#nfl--restored-the-apinflcom-game-schedule--play-by-play-wrappers)
  - [ESPN — remove always-erroring endpoint variants + NFL R-parity](#espn--remove-always-erroring-endpoint-variants--nfl-r-parity)
  - [Documentation — per-league Python ↔ R parity tables](#documentation--per-league-python-%E2%86%94-r-parity-tables)
  - [Documentation — example notebooks repaired, expanded, and rendered on-site](#documentation--example-notebooks-repaired-expanded-and-rendered-on-site)
  - [NHL / PWHL — loader naming-parity aliases + games-manifest loaders (fastRhockey parity)](#nhl--pwhl--loader-naming-parity-aliases--games-manifest-loaders-fastrhockey-parity)
  - [Documentation — NFL return-table descriptions mined from nflverse](#documentation--nfl-return-table-descriptions-mined-from-nflverse)
  - [Documentation — class methods rendered on autodoc pages (CFB / NFL)](#documentation--class-methods-rendered-on-autodoc-pages-cfb--nfl)
  - [Documentation — accuracy-audit fixes](#documentation--accuracy-audit-fixes)
- [0.0.55 Release: June 8, 2026](#0055-release-june-8-2026)
  - [Documentation — richer per-function reference](#documentation--richer-per-function-reference)
  - [Bug fixes](#bug-fixes-1)
- [0.0.54 Release: June 8, 2026](#0054-release-june-8-2026)
  - [Per-sport return schemas (correctness)](#per-sport-return-schemas-correctness)
  - [BREAKING — parser-backed wrappers return a DataFrame by default](#breaking--parser-backed-wrappers-return-a-dataframe-by-default)
  - [Docs coverage gate + autodoc](#docs-coverage-gate--autodoc)
  - [MLB - full MLB Stats API coverage](#mlb---full-mlb-stats-api-coverage)
  - [Deprecations](#deprecations)
- [0.0.53 Release: June 8, 2026](#0053-release-june-8-2026)
  - [ESPN — declarative codegen + factory retirement](#espn--declarative-codegen--factory-retirement)
  - [NHL native — codegen cutover + clean names (api-web; in progress)](#nhl-native--codegen-cutover--clean-names-api-web-in-progress)
  - [Dataset loaders — release manifest + drift audit](#dataset-loaders--release-manifest--drift-audit)
  - [Generated documentation — reference pages + drift gate](#generated-documentation--reference-pages--drift-gate)
  - [CFB — advanced box score expansion (`create_box_score`)](#cfb--advanced-box-score-expansion-create_box_score)
  - [CFB — box-score attribution correctness + ESPN-sourced totals (`create_box_score`)](#cfb--box-score-attribution-correctness--espn-sourced-totals-create_box_score)
  - [CFB — play-type reclassification: interception-return-fumble guard (`__add_new_play_types`)](#cfb--play-type-reclassification-interception-return-fumble-guard-__add_new_play_types)
  - [CFB — blocked-kick turnover flags + ESPN native-flag tripwires](#cfb--blocked-kick-turnover-flags--espn-native-flag-tripwires)
  - [CFB — pre-2014 era support (`CFBPlayProcess`)](#cfb--pre-2014-era-support-cfbplayprocess)
  - [Removed — NCAA bracketology](#removed--ncaa-bracketology)
- [0.0.52 Release: June 3, 2026](#0052-release-june-3-2026)
  - [CFB — offline reprocess support (`CFBPlayProcess`)](#cfb--offline-reprocess-support-cfbplayprocess)
- [0.0.51 Release: May 30, 2026](#0051-release-may-30-2026)
  - [User-facing quality-of-life additions](#user-facing-quality-of-life-additions)
  - [New: MLB module (greenfield)](#new-mlb-module-greenfield)
  - [New: NHL — `api-web.nhle.com` migration + EDGE / Stats REST / Records](#new-nhl--api-webnhlecom-migration--edge--stats-rest--records)
  - [New: ESPN cross-league port](#new-espn-cross-league-port)
  - [New: NCAA bracketology](#new-ncaa-bracketology)
  - [New: `_common_espn_parsers.py` (polars / pandas parser layer)](#new-_common_espn_parserspy-polars--pandas-parser-layer)
  - [New: `return_parsed=True` dispatch shim](#new-return_parsedtrue-dispatch-shim)
  - [New: `nhl_edge_parsers.py`](#new-nhl_edge_parserspy)
  - [New: Site v2 summary dispatcher (20 sub-parsers)](#new-site-v2-summary-dispatcher-20-sub-parsers)
  - [New: 100% ENDPOINT_PARSERS coverage (121/121)](#new-100%25-endpoint_parsers-coverage-121121)
  - [New: weekly cron live-test drift detector](#new-weekly-cron-live-test-drift-detector)
  - [New: MLB Stats API parser layer](#new-mlb-stats-api-parser-layer)
  - [New: NHL Stats REST + Records parser layers](#new-nhl-stats-rest--records-parser-layers)
  - [New: NHL api-web parser layer](#new-nhl-api-web-parser-layer)
  - [Bug fixes](#bug-fixes-2)
  - [New: NFL drive-plays parser (true PBP parity)](#new-nfl-drive-plays-parser-true-pbp-parity)
  - [Test infrastructure](#test-infrastructure)
  - [Documentation](#documentation)
- [0.0.50 Release: May 7, 2026](#0050-release-may-7-2026)
  - [Packaging modernization](#packaging-modernization)
  - [Conda installability](#conda-installability)
  - [Linting & pre-commit modernization](#linting--pre-commit-modernization)
  - [Documentation toolchain](#documentation-toolchain)
  - [Runnable docstring examples (~190 functions)](#runnable-docstring-examples-190-functions)
  - [Example notebooks](#example-notebooks)
  - [Contributor docs and templates](#contributor-docs-and-templates)
  - [NFL — nflreadpy parity](#nfl--nflreadpy-parity)
  - [NFL — caching and configuration](#nfl--caching-and-configuration)
  - [NFL — static datasets](#nfl--static-datasets)
  - [NFL — pickcenter / odds modern path](#nfl--pickcenter--odds-modern-path)
  - [NFL — `load_nfl_schedule` parquet port](#nfl--load_nfl_schedule-parquet-port)
  - [WBB / WNBA — new ESPN scrape modules](#wbb--wnba--new-espn-scrape-modules)
  - [CFB — `cfb_play_participants` and `__add_player_cols` collapse](#cfb--cfb_play_participants-and-__add_player_cols-collapse)
  - [CFB — pandas → polars 1.x bug-fix reconciliation (`0.36-live` → `main`)](#cfb--pandas-%E2%86%92-polars-1x-bug-fix-reconciliation-036-live-%E2%86%92-main)
  - [Infrastructure and tooling](#infrastructure-and-tooling)
  - [Bug fixes](#bug-fixes-3)
  - [Deprecations](#deprecations-1)
- [0.0.40 Release: December 6, 2025](#0040-release-december-6-2025)
- [0.0.38-39 Release: August 28, 2023](#0038-39-release-august-28-2023)
- [0.0.36-37 Release: July 9, 2023](#0036-37-release-july-9-2023)
- [0.0.34-35 Release: May 7-9, 2023](#0034-35-release-may-7-9-2023)
- [0.0.18 Release: July 25, 2022](#0018-release-july-25-2022)
- [0.0.17 Release: July 9, 2022](#0017-release-july-9-2022)
- [0.0.15 Release: May 8, 2022](#0015-release-may-8-2022)
- [0.0.14 Release: March 16, 2022](#0014-release-march-16-2022)
- [0.0.12 Release: February 24, 2022](#0012-release-february-24-2022)
- [0.0.5 Release: October 20, 2021](#005-release-october-20-2021)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Unreleased

### CFB — cross-source crosswalk loaders (`load_cfb_*_crosswalk`)

New 404-safe dataset loaders read pre-built CFB identity crosswalks from the `cfb_crosswalk` release tag on `sportsdataverse-data`, so callers can translate ids across providers without re-scraping every source. They cache the output of the live `cfb_teams_crosswalk` / `cfb_schedule_crosswalk` / `cfb_rosters_crosswalk` builders (ESPN × Fox × Yahoo, keyed on an aggressively-normalized team name; see `sportsdataverse.cfb.cfb_crosswalk`) — a full-season schedule build otherwise fans out hundreds of requests across three providers.

### ESPN — NCAA men's & women's college hockey (`espn_mch_*`, `espn_wch_*`)

- feat(espn): add NCAA men's & women's college hockey (espn_mch_*, espn_wch_*)

### ESPN — NCAA college baseball + softball (`espn_college_baseball_*`, `espn_college_softball_*`)

- feat(espn): add NCAA college baseball + softball (espn_college_baseball_*, espn_college_softball_*)

- **`load_cfb_teams_crosswalk(seasons=)`** and **`load_cfb_schedule_crosswalk(seasons=)`** are **per-season** (`min_season` 2014) — teams and schedules are genuinely historical, published per year for 2014–2025.
- **`load_cfb_rosters_crosswalk()`** is **season-less**: ESPN's and Fox's team-roster endpoints expose only the *current* roster, so the artifact is a single snapshot (no `seasons=` argument) rather than a misleading per-season series.

All accept `return_as_pandas=`. Artifacts are produced by `cfbfastR-cfb-data/scripts/build_cfb_crosswalk.py` (the rosters table fans the per-team `cfb_rosters_crosswalk` out over the current season's ESPN↔Fox team-id pairs and concatenates them with `espn_team_id` / `fox_team_id` provenance). The companion on-demand builder `cfb_odds_events_crosswalk` (The Odds API event-id ↔ ESPN game-id) remains live-only — near-term events aren't worth caching.

## 0.0.58 Release: June 12, 2026

### Loaders — NHL core + new NBA/MBB datasets aligned to `sportsdataverse-data` releases

The four core NHL loaders (`load_nhl_pbp`, `load_nhl_player_boxscore`, `load_nhl_team_boxscore`, `load_nhl_schedule`) now read the SDV-native `sportsdataverse-data` releases (`nhl_pbp_full`, `nhl_player_boxscores`, `nhl_team_boxscores`, `nhl_schedules`) instead of the legacy R `fastRhockey-data` branch — gaining the 2010 season (`min_season` 2011 → 2010). Added loaders for NBA/MBB datasets that were already published but had no loader, bringing them to parity with the WBB/WNBA surface: `load_nba_player_season_stats`, `load_nba_team_season_stats`, `load_nba_draft`, `load_nba_rosters`, and `load_mbb_standings`, `load_mbb_player_season_stats`, `load_mbb_team_season_stats`, `load_mbb_rosters`, `load_mbb_officials`, `load_mbb_game_rosters` — each with a generated return-schema table. Also fixed the `--audit-releases` drift check to key on the release **tag** (it parsed the human-readable title), which had been falsely flagging valid releases as missing.

### Robustness & infrastructure — typing, CI gates, HTTP, deprecation policy

A package-wide hardening pass with no change to public data outputs:

- **Typing + CI:** ships a PEP 561 `py.typed` marker; a new `quality.yml` CI gate runs `ruff` + `ruff format --check` + `mypy` on every PR, with a `[tool.mypy] files` ratchet (modules join the strict gate as they reach clean typing), and the test workflow now emits coverage.
- **Errors + logging:** a `SportsDataverseError` base class (with `SeasonNotFoundError` / `NoESPNDataError` re-parented under it) and a package logger with a `NullHandler`; previously-silent `except` paths now log.
- **HTTP layer:** `dl_utils.download()` reuses a module-level pooled `requests.Session` and backs off honoring `Retry-After` (numeric **and** RFC 7231 HTTP-date, clamped non-negative, 120s ceiling) instead of a fixed sleep.
- **Deprecation policy:** a centralized `sportsdataverse._deprecation` (`warn_deprecated` + `@deprecated`) with a documented removal window; the 11 per-type NFL loader aliases migrated to it.
- **Codegen determinism:** generator output is LF-only on every platform and the ruff format pass is pinned to the project's ruff (no CRLF phantom diffs); idempotency tests lock it in.
- **Tests:** a VCR-style record/replay harness (committed cassettes, secret-scrubbing) exercises the real `download()` → parser call path offline.

### The Odds API wrappers (`sportsdataverse.odds`, `toa_*`)

New `sportsdataverse.odds` module wrapping [The Odds API](https://the-odds-api.com) v4 — live + historical sports betting odds, scores, events, markets and participants across a wide range of bookmakers. Mirrors the sister R package [oddsapiR](https://oddsapir.sportsdataverse.org)'s `toa_*` surface: `toa_sports`, `toa_sports_odds`, `toa_sports_scores`, `toa_sports_events`, `toa_event_odds`, `toa_event_markets`, `toa_sports_participants`, the three `*_history` snapshot variants, and `toa_usage` (cached quota, no network). The odds endpoints return tidy **long-format** frames (one row per event × bookmaker × market × outcome). Auth resolves from the `ODDS_API_KEY` env var (same variable as `oddsapiR`) or an `api_key=` argument; the call routes through the shared `dl_utils.download()` gateway. Same `return_parsed` / `return_as_pandas` contract (polars by default). Built from the `the_odds_api` OpenAPI spec.

### Yahoo Sports college football wrappers (`yahoo_cfb_*`)

Read-only Yahoo Sports wrappers for college football over Yahoo's shangrila stats graph (`graphite-secure.sports.yahoo.com/v1/query/shangrila`) and editorial feed (`api-secure.sports.yahoo.com/v1/editorial/s`): `yahoo_cfb_player_season_stats`, `yahoo_cfb_team_season_stats`, the legacy per-category `*_season_stats_legacy` variants, `yahoo_cfb_scoreboard`, and a `yahoo_cfb_boxscore` scaffold. Same `return_parsed` / `return_as_pandas` contract (polars by default).

### NFL — `api.nfl.com` wrappers cut over to generated; "NFL.com API" docs grouping

The hand-written `sportsdataverse.nfl.nfl_api` wrappers (`nfl_standings`, `nfl_rosters`, `nfl_injuries`, …) are now **generated** from `tools/codegen/endpoints/nfl_api.yaml`, like the NHL/MLB native families. The flat-API codegen gained `getter_module` + `auth` support so an authenticated family (the NFL.com `WEB_DESKTOP` bearer token) can be generated; the auth getter lives in `nfl_api_runtime.py` and the per-endpoint record extraction in `nfl_api_parsers.py`. As a result the NFL docs index now lists a dedicated **"NFL.com API"** reference grouping (11 functions) instead of burying those wrappers in "Additional functions". Wrapper signatures gain `return_parsed` / `**kwargs`.

### NFL — automatic `api.nfl.com` token caching + `NFL_ACCESS_TOKEN` override

The `api.nfl.com` bearer token is now minted once and cached in-process, then auto-renewed just before its JWT `exp` — so back-to-back `nfl_*` / `nfl_api_*` calls reuse a single token instead of POSTing to `/identity/v3/token` on every call, with no setup and no manual refresh. A new optional `NFL_ACCESS_TOKEN` env var injects a pre-minted bearer token verbatim (skipping the mint + cache); the existing `NFL_CLIENT_KEY` / `NFL_CLIENT_SECRET` credential overrides still apply. `nfl_clear_token_cache()` forces a fresh mint, and `nfl_token_gen(force_refresh=True)` re-mints on demand.

### Documentation — `api.nfl.com` OpenAPI spec

Added an OpenAPI 3.1 description of the modern NFL.com "Shield" data API (`api.nfl.com`: `/identity/v3/token` device-token auth + `/football/v2/*` + `/experience/*`) to the reference repos (`sdv-internal-refs/nfl/`, `sdv-swagger/nfl_api_openapi.yaml`).

### Bug fixes

- `load_nfl_players()` now reads the nflverse **players** release (`players/players.parquet`) on both the polars and pandas paths; the default polars path previously returned the **officials** dataset by mistake.
- The generated `api.nfl.com` wrappers route their HTTP call through the shared `sportsdataverse.dl_utils.download()` gateway (retries + cache + ESPN-aware error handling) like every other wrapper, instead of calling `requests.get()` directly. Boolean query flags and the `nfl_weeks` `season` / `season_type` path params are hardened so `None` can no longer leak onto the wire.

### Internal — Fox data key single-sourced

`sportsdataverse.cfb.cfb_fox_ext.FOX_DATA_KEY` is now imported from `sportsdataverse._fox_layout.DATA_KEY` so the bundled public Fox key and its `SDV_PY_FOX_DATA_KEY` env override live in exactly one place instead of being duplicated.

## 0.0.57 Release: June 10, 2026

### Fox Sports Bifrost wrappers (CFB, NBA, MBB, NHL, MLB)

Read-only Fox Sports "Bifrost" wrappers (`fox_<sport>_*`) over `api.foxsports.com/bifrost/v1/<sport>/*`, complementing the `espn_<sport>_*` families. The Bifrost API is a layout API (sections → tables → rows → cells) that is uniform across sports; a shared parsing layer (`sportsdataverse/_fox_layout.py`) backs every league module. Same `return_parsed` / `return_as_pandas` contract (polars by default).

**CFB** (`cfb` module): `fox_cfb_pbp` (quarters → drives → plays), `fox_cfb_boxscore`, `fox_cfb_odds`, `fox_cfb_team_roster`, `fox_cfb_team_stats`, `fox_cfb_team_gamelog`, `fox_cfb_standings`, `fox_cfb_league_leaders`.

**NBA / MBB / NHL** (`nba` / `mbb` / `nhl` modules): the same eight wrappers per sport (`fox_<sport>_pbp`, `_boxscore`, `_odds`, `_team_roster`, `_team_stats`, `_team_gamelog`, `_standings`, `_league_leaders`). Play-by-play is period-based (QUARTER / HALF / PERIOD → plays); boxscore is tidy long per player-stat.

**MLB** (`mlb` module): `fox_mlb_team_roster`, `fox_mlb_team_stats`, `fox_mlb_team_gamelog`, `fox_mlb_standings`, `fox_mlb_league_leaders`, `fox_mlb_odds`. Fox does not expose MLB play-by-play or boxscore via `event/{id}/data`, so those two are intentionally omitted.

Live-tested (gated behind `SDV_PY_LIVE_TESTS=1`). Reverse-engineering notes + an OpenAPI 3.1 spec live in the `sdv-internal-refs` repo. Parallels the cfbfastR / hoopR / fastRhockey / baseballr `fox_*` families.

#### CFB — Fox as a backup source for the EPA/WPA play processor (`fox_cfb_play_process`)

Where `fox_cfb_pbp` returns the raw Fox play rows, `fox_cfb_play_process` runs Fox data through the **same** `CFBPlayProcess` pipeline ESPN games use — producing EPA / WPA / advanced box score — as a backup/alternative when ESPN is unavailable. The new module `sportsdataverse.cfb.cfb_pbp_fox` adapts a Fox `cfb/event/{id}/data` payload into the ESPN-`summary` shape the processor consumes (`fox_to_espn_summary`), so the 6,000-line pipeline runs unmodified.

- **`fox_cfb_play_process(event_id)`** — fetch + adapt + `run_processing_pipeline` (or `process=False` for cleaning-only, `raw=True` for the adapted summary). Returns the processed payload tagged `source="fox"`.
- **`fox_to_espn_summary(fox_data)`** — the adapter (`modalPlay.events[].yardStart` → yards-to-goal, play title → down/distance, `events[].text` → ESPN `type.text` vocab, team logo → possession).
- Validated offline (5 tests) on a captured blowout (FSU 66-10 → FSU +0.50 vs Kent −0.94 EPA/play — game-consistent). High fidelity on the structured/numeric path (down/distance/yards-to-goal/EPA/WPA); text-grammar features (detailed player attribution, penalty yards) degrade vs ESPN. Archive-format Fox games (no `modalPlay` geometry) are detected and rejected. A Fox event id differs from an ESPN game id; backing up a specific ESPN game needs matching by teams + date.

## 0.0.56 Release: June 9, 2026

### HockeyTech — live multi-league scraper (PWHL + AHL/OHL/WHL/QMJHL) + on-ice/Corsi/TOI analytics

A new `sportsdataverse.hockeytech` core powers live wrappers over the HockeyTech
feeds, alongside the existing offline `load_pwhl_*` loaders:

- **PWHL** (`sportsdataverse.pwhl`): 20 live `pwhl_*()` functions at fastRhockey
  output parity — `pwhl_schedule`, `pwhl_scorebar`, `pwhl_game_info`,
  `pwhl_game_summary`, `pwhl_pbp`, `pwhl_player_box`, `pwhl_teams`,
  `pwhl_team_roster`, `pwhl_standings`, `pwhl_player_info`, `pwhl_player_stats`,
  `pwhl_player_game_log`, `pwhl_player_search`, `pwhl_stats`, `pwhl_leaders`,
  `pwhl_streaks`, `pwhl_transactions`, `pwhl_playoff_bracket`, `pwhl_season_id`,
  and `most_recent_pwhl_season`.
- **AHL / OHL / WHL / QMJHL** (`sportsdataverse.{ahl,ohl,whl,qmjhl}`): per-league
  families (schedule, pbp, standings, teams, team_roster, player_stats, leaders,
  game_summary, season_id, `most_recent_<lg>_season`) over one shared core.
- **Analytics** across all five leagues: `<lg>_game_shifts`, `<lg>_player_toi`,
  and `<lg>_game_corsi` (player-level on-ice Corsi/Fenwick), reconstructed from
  the shift tables via countdown-clock interval matching. `<lg>_pbp` is enriched
  to a superset (coordinate transforms, clock columns, shot distance/angle,
  scoring chances, on-ice players, game-meta join, `blocked_shot`/`hit` events).
- **Corsi/Fenwick caveat**: the HockeyTech feed has no missed-shot event, so both
  metrics are computed from shots-on-goal + blocked + goals and every analytics
  output carries `corsi_includes_missed = False`.
- All returned columns are snake_case; PWHL columns match fastRhockey exactly. A
  companion fastRhockey (R) release mirrors this surface, verified by a
  cross-language parity test pinning identical Corsi/TOI numbers.

### NFL — Next Gen Stats (`nfl_ngs_*`) + api.nfl.com football/v2 (`nfl_*`) modules

- New `sportsdataverse/nfl/nfl_ngs.py` — **token-free** Next Gen Stats wrappers over
  `nextgenstats.nfl.com/api` (browser session, no auth). 10 functions / 21 endpoints:
  `nfl_ngs_statboard` (passing/receiving/rushing), `nfl_ngs_statboard_leaders`,
  `nfl_ngs_leaders` (speed/distance/time-to-sack + completion/ery/yac expectation,
  season & week), `nfl_ngs_league_schedule[_current]`, `nfl_ngs_league_teams`,
  `nfl_ngs_gamecenter_overview`, `nfl_ngs_microsite_chart[_players]`,
  `nfl_ngs_play_is_highlight`. The `/live/*` NGS endpoints are anonymous-403 (need
  elevated auth) and are documented as omitted.
- New `sportsdataverse/nfl/nfl_api.py` — `api.nfl.com/football/v2` + `/experience`
  wrappers on the bearer token (reuses `nfl_headers_gen`). 11 functions:
  `nfl_standings`, `nfl_rosters`, `nfl_teams_history`, `nfl_team`, `nfl_weeks`,
  `nfl_weeks_by_date`, `nfl_combine_profiles`, `nfl_draft_picks`, `nfl_injuries`,
  `nfl_game_summaries`, `nfl_weekly_game_details`.
- Both return tidy polars DataFrames by default (`return_as_pandas` supported) and are
  documented on the NFL reference pages. Catalogued from a full crawl of the NFL API
  surface (api.nfl.com + NGS).
- Captured autodoc **return-column tables** for all 23 new NFL functions (live
  introspection -> `schemas/autodoc/nfl/*.yaml` + `autodoc_example_args.yaml`), so
  each renders a `col_name | type | description` table on its reference page.

### NFL — restored the api.nfl.com game schedule + play-by-play wrappers

- `nfl_game_schedule` / `nfl_game_details` were broken because NFL.com retired the
  old `/v1/reroute` client-credentials token endpoint (404 -> `JSONDecodeError`).
  Rebuilt `sportsdataverse/nfl/nfl_games.py` on the modern flow the NFL.com web app
  (and nflverse's `nflapi`) now use: `nfl_token_gen()` mints a bearer token from
  `/identity/v3/token` (form-encoded device grant, `X-Domain-Id: 100`);
  `nfl_game_schedule()` reads `/football/v2/games/season/{s}/seasonType/{t}/week/{w}`;
  `nfl_game_details()` reads `/experience/v1/gamedetails/{id}` and unwraps the shield
  `data.viewer.gameDetail` object (plays, drives, scoring summaries, line scores).
- Auth uses the NFL.com public `WEB_DESKTOP` web-client credentials as defaults,
  overridable via `NFL_CLIENT_KEY` / `NFL_CLIENT_SECRET` env vars or function args
  (no personal account; the token carries the anonymous `free` plan). Verified live:
  16 games for 2024 REG wk1, 194 plays / 20 drives for the opener.
- Added a **parsed surface** over the raw dicts: `nfl_game_pbp(game_id)` returns a
  tidy polars/pandas DataFrame (one row per play, with `game_id`/`home_team`/
  `visitor_team` context), and `nfl_week_games(season, season_type, week)` returns
  one row per game. (Named to avoid colliding with the `nfl_pbp`/`nfl_schedule`
  submodules.)

### ESPN — remove always-erroring endpoint variants + NFL R-parity

- **Removed dead ESPN endpoint variants (all leagues).** A live health sweep found
  these generated wrappers 404 / `NoESPNDataError` at ESPN for every league and
  season: the season-less `espn_*_coaches` list (`/leagues/{league}/coaches`) and
  the four `espn_*_calendar_{offseason,regular_season,postseason,ondays}` sub-paths.
  They are dropped from the codegen so the package no longer ships endpoints that
  always raise. The working counterparts remain: `espn_*_season_coaches`
  (`/seasons/{season}/coaches`), the coach-detail endpoints (`espn_*_coach`, ...),
  and the base `espn_*_calendar`. (~40 dead functions removed across 8 leagues.)
- **NFL Python ↔ R parity.** Added curated `r_parity_aliases.yaml` entries mapping
  the canonical `load_nfl_*` loaders to their nflreadr equivalents (e.g.
  `load_nfl_pbp` → `load_pbp`, `load_nfl_schedule` → `load_schedules`), so the NFL
  parity table links both naming styles (nfl rows 26 → 49). The `load_nfl_*` /
  bare `load_*` dual-naming itself was verified already consistent (intentional
  nflreadpy parity; the only unaliased `load_nfl_*` are deprecated or sdv-specific).

### Documentation — per-league Python ↔ R parity tables

- Each league's `index.md` now carries a **Python ↔ R parity** table mapping every
  `sportsdataverse` function to its equivalent in the sister R package
  (cfbfastR / hoopR / wehoop / baseballr / fastRhockey), linking the Python doc page
  and the R pkgdown reference. Driven by a new `tools/codegen/build_r_exports.py`
  miner (NAMESPACE → committed `r_exports.yaml`, so links never 404 and the offline
  `--check` stays deterministic) plus a curated `r_parity_aliases.yaml` for
  divergent names (e.g. `mlb_api_*` → baseballr `mlb_*`, +36 verified). Coverage:
  nhl 202, mlb 107, wnba 83, nba/wbb 74, mbb 69, cfb 55, nfl 26, pwhl 15.
- Fixed a self-referential codegen bug the parity table exposed: `render_autodoc_page`
  computed "already documented" against a corpus that included the index, so the
  index's parity table (which names autodoc functions) caused those functions to be
  dropped from `additional.md` and their parity links to 404. It now uses the
  reference-pages corpus only, matching the autodoc-name count used for the index.

### Documentation — example notebooks repaired, expanded, and rendered on-site

- **Repaired the example notebooks.** Live execution (`nbclient`) surfaced runtime
  schema/usage drift that import/compile checks miss: ESPN schedule team columns
  renamed to `home_display_name`/`away_display_name`; `espn_*_pbp()['plays']` is a
  raw list using dot-notation keys (`period.number`, `clock.displayValue`,
  `scoringPlay`, `shootingPlay`, `coordinate.x/.y`) built via
  `pl.DataFrame(...)`; ESPN scores are strings (cast before arithmetic);
  `espn_cfb_schedule` takes `dates=` not `season=`; ESPN team rosters use
  `full_name`; `espn_*_team_stats` returns a dict `{Averages, Totals, Misc}`; some
  hardcoded dates had no games. All notebooks now execute clean end-to-end.
- **Split + expanded the suite to ten notebooks.** The combined `wbb_wnba` notebook
  was split into separate `05_wbb_intro` and `08_wnba_intro`, both expanded; the
  NHL notebook gained an ESPN-NHL section alongside the native api-web surface; and
  two new notebooks were added: `09_mlb_intro` (MLB Stats API + Statcast + ESPN MLB)
  and `10_pwhl_intro` (PWHL loaders).
- **On-site rendered Tutorials.** New `tools/codegen/render_notebooks.py` executes
  each notebook and renders it (with real outputs, as clean monospace tables) to a
  themed page under `docs/docs/tutorials/`, surfaced in a new **Tutorials** sidebar
  section. Execution is quarantined to the weekly `live-tests-cron` workflow, which
  now re-executes + renders and opens a refresh PR (main is branch-protected); the
  normal offline docs build just consumes the committed pages. Each league index's
  **Examples** section now links the on-site tutorial pages instead of GitHub.

### NHL / PWHL — loader naming-parity aliases + games-manifest loaders (fastRhockey parity)

- Added 4 NHL short-name aliases in `sportsdataverse/nhl/nhl_loaders.py`:
  `load_nhl_team_box` → `load_nhl_team_boxscore`,
  `load_nhl_player_box` → `load_nhl_player_boxscore`,
  `load_nhl_skater_box` → `load_nhl_skater_boxscores`,
  `load_nhl_goalie_box` → `load_nhl_goalie_boxscores`.
- Added 5 PWHL short-name aliases in `sportsdataverse/pwhl/pwhl_loaders.py`:
  `load_pwhl_team_box` → `load_pwhl_team_boxscores`,
  `load_pwhl_player_box` → `load_pwhl_player_boxscores`,
  `load_pwhl_skater_box` → `load_pwhl_skater_boxscores`,
  `load_pwhl_goalie_box` → `load_pwhl_goalie_boxscores`,
  `load_pwhl_schedule` → `load_pwhl_schedules`.
- Added `load_nhl_games()` (no `seasons` arg) reading the NHL games-in-data-repo
  manifest parquet from the `nhl_schedules` release asset (primary URL verified
  working: `sportsdataverse-data/releases/download/nhl_schedules/nhl_games_in_data_repo.parquet`).
- Added `load_pwhl_games()` (no `seasons` arg) reading the PWHL games-in-data-repo
  manifest parquet from the `pwhl_schedules` release asset (primary URL verified
  working: `sportsdataverse-data/releases/download/pwhl_schedules/pwhl_games_in_data_repo.parquet`).
- Added `tests/test_loader_parity.py` covering importability, `__all__` membership,
  docstring-based forwarding assertions, and live alias shape-parity + manifest tests
  (gated behind `SDV_PY_LIVE_TESTS=1`).

### Documentation — NFL return-table descriptions mined from nflverse

- Extended `tools/codegen/build_r_col_descriptions.py` with two nflverse source
  parsers: `mine_csv_dictionaries()` reads nflreadr's canonical
  `data-raw/dictionary_*.csv` field docs (delimiter-sniffing for the
  semicolon-delimited NGS file, BOM-stripping for `roster_status`, and
  case-insensitive Field/Description column resolution across 6 header variants),
  and `mine_item_list()` reads nflfastR's `data-raw/variable_list.txt`
  (`\item{Field}{Description}` form). Yields `nflreadr` (941 columns) and
  `nflfastR` (372 columns) dictionaries in `r_column_descriptions.yaml`.
- Mapped `nfl → nflreadr` in `generate.py`'s `_LEAGUE_R_PACKAGE`; nflfastR's
  fields still contribute via the `_merged` cross-package fallback.
- NFL generated reference-page description fill rose from ~36% to ~85%. The
  enlarged `_merged` union (7.3k → 8.1k columns) also backfilled previously-blank
  shared football/stat columns on the CFB and MLB reference pages
  (e.g. `passing_yards`, `receptions`, `kicker_player_name`, `name_short`).

### Documentation — class methods rendered on autodoc pages (CFB / NFL)

- Hand-written classes (`CFBPlayProcess`, `NFLPlayProcess`) previously rendered on
  the `additional` reference pages as a bare constructor signature with no
  description and an empty parameter table — their public methods, returns, and
  examples were omitted entirely. The autodoc renderer now treats a class
  specially: `_doc_view()` attaches a per-method doc-view list (via
  `_augment_class_view()`), and the `autodoc_page.md.jinja` template renders each
  public method as a nested `#### Class.method(...)` entry with its description,
  parameters, returns, and example. Both classes now document all 7 of their
  public methods (`espn_*_pbp`, `*_pbp_disk`, `*_pbp_json`, `corrupt_pbp_check`,
  `create_box_score`, `run_cleaning_pipeline`, `run_processing_pipeline`).
- Constructor parameter descriptions are backfilled from the class's `__init__`
  docstring when the class object itself carries none (`CFBPlayProcess` documents
  its ctor args on `__init__`), so the constructor parameter table now renders
  with descriptions instead of blank cells.
- Added a class-level docstring to `CFBPlayProcess` (it had none) mirroring
  `NFLPlayProcess`, so the class entry leads with an overview + runnable example
  instead of a `No description available.` placeholder.

### Documentation — accuracy-audit fixes

- **Stable autodoc anchors.** Every autodoc function/class heading now carries an
  explicit `{#name}` id, so it is reliably deep-linkable instead of relying on a
  signature-derived slug. This fixes a broken cross-link in `ecosystem.md`
  (`espn_nhl_teams` now resolves to its `additional` page entry) and future-proofs
  any reference to a hand-written wrapper.
- **Invalid example code.** `_clean_example()` mis-handled reST literal-block
  intros that wrap across multiple prose lines (only the line ending in `::` was
  recognized), leaking a prose sentence into the rendered ` ```python ` block as a
  broken statement. It now absorbs the preceding contiguous intro lines into the
  step comment. Fixes the `NflConfig` and `espn_wbb_team_stats` examples; all
  non-REPL doc examples now compile.
- **Notebook reachability.** `ecosystem.md` now links all seven example notebooks
  individually (previously only `01_quickstart` was linked; the per-sport intros
  02–07 were an un-linked "for your league" mention). Each league's generated
  `index.md` landing page also gained an **Examples** section linking the quickstart
  plus that sport's intro notebook (`render_league_index` + a league→notebook map
  in `generate.py`); mlb/pwhl show the quickstart until they get a dedicated intro.

## 0.0.55 Release: June 8, 2026

### Documentation — richer per-function reference

- Autodoc "Additional functions" pages now render full **Parameters** tables
  (name/type/default/description), **Returns**, and runnable **Example** blocks
  parsed from each function's docstring (previously just a signature + one line).
- Endpoint reference pages gained a **Description** column on the parameter table;
  shared query params carry authored descriptions.
- Function **Returns** are now `col_name | type | description` tables: endpoint
  pages from introspected per-sport schemas, and autodoc DataFrame functions from
  a new `generate.py --autodoc-schemas` live-introspection pass (best-effort, with
  prose fallback where a function can't be introspected offline).
- Return-table column **descriptions** are filled by column name from the sibling
  SDV R packages' `@return` docs (cfbfastR / hoopR / wehoop / baseballr), mined to
  `tools/codegen/r_column_descriptions.yaml` and applied at render time
  (hand-curated descriptions take precedence; unmatched columns stay blank).

### Bug fixes

- `espn_mbb_game_rosters` / `espn_wbb_game_rosters` / `espn_nfl_game_rosters`:
  fixed a `ShapeError` (positional column rename broke when ESPN ships extra
  `*_$ref` fields); columns are now renamed by source key.
- `espn_nhl_schedule`: fixed `'NoneType' object has no attribute 'get'` with
  default args (a helper was missing its `return event`).
- The `espn_*_game_rosters` rename-by-source-key fix is additive: NBA/WNBA roster
  frames now include a `team_alternate_ids_sdr` column when ESPN ships it (the old
  positional rename would have raised once that field appeared).

## 0.0.54 Release: June 8, 2026

### Per-sport return schemas (correctness)

`@return` tables are now derived per league by running the real parsers against
captured per-sport fixtures (`generate.py --schemas`), replacing the previous
sport-agnostic schemas that showed (e.g.) basketball boxscore columns on MLB/NHL
pages. Native API pages (`nhl_api_web`/`nhl_edge`/`nhl_records`/`nhl_stats_rest`/
`mlb_api`) gained accurate return schemas. Schemas are now introspected truth,
gated by `generate.py --check`.

### BREAKING — parser-backed wrappers return a DataFrame by default

`return_parsed` now defaults to **`True`** for the parser-backed wrappers; they
return a tidy polars DataFrame instead of the raw `Dict`. Pass
`return_parsed=False` to recover the raw `Dict`; `return_as_pandas=True` switches
polars→pandas. Wrappers without a registered parser are unchanged (still `Dict`).
The `sportsdataverse.parsed.{league}` mirror modules are unaffected.

### Docs coverage gate + autodoc

Every user-facing function now reaches the docs. A new `generate.py --coverage`
audit enumerates in-scope exported functions per league and fails `--check` if
any is undocumented (allowlist for cross-cutting internals in
`tools/codegen/coverage_allowlist.yaml`). ~180 hand-written wrappers/loaders/
statcast/utility functions that the endpoint-YAML codegen never documented are
now rendered into per-league "Additional functions" reference pages
(autodoc from live signatures + docstrings).

### MLB - full MLB Stats API coverage

The codegen now wraps the full `statsapi.mlb.com` surface: 38 previously
unwrapped endpoints were added (home run derby, all-star ballots, conferences,
free agents, game pace, jobs/datacasters/official-scorers, team coaches/
personnel, schedule variants, seasons/all, sport, teams history/stats, etc.).
28 are publicly serviceable and ship with captured fixtures + introspected
return schemas; the remaining handful are auth-gated/internal MLBAM feeds
(analytics/guids/color), wrapped with valid example args for if/when access
exists.

### Deprecations

- `sportsdataverse.parsed.{league}` is **deprecated** (since the default modules
  now return parsed DataFrames by default). Importing a `parsed.*` module emits a
  `DeprecationWarning`; it still works and will be removed in a future release.
  Migrate to `from sportsdataverse.{league} import <fn>` directly.

## 0.0.53 Release: June 8, 2026

### ESPN — declarative codegen + factory retirement

The runtime "magic" that mass-registered each league's `espn_<league>_*` ESPN
wrappers at import time (`_common_espn.make_league_module` / `_bind` + the
`_UNIVERSAL_WRAPPERS` / `_NCAA_WRAPPERS` / `_FOOTBALL_WRAPPERS` / `_MLB_WRAPPERS`
tables + ~127 private `_site_v2_*` / `_espn_*` / `_core_v2_*` core functions) has
been replaced by a **declarative codegen pipeline** (`tools/codegen/`). Endpoint
metadata lives in `tools/codegen/endpoints/*.yaml`; `generate.py` renders concrete,
fully-documented wrapper modules into `sportsdataverse/<league>/<league>_espn_ext.py`.

- **New `espn_nhl_*` surface (115 functions).** NHL previously had no ESPN
  cross-league wrappers; it now gets the full Site v2 / Web v3 / Core v2 surface,
  and `find()` works for NHL for free.
- **Identical behavior, real signatures.** Every generated function builds a
  byte-identical URL + query string to the function it replaced (verified by a
  URL+params parity gate across all scopes), but now exposes concrete parameter
  names, type hints, and docstrings instead of an opaque `*args, **kwargs` shim.
- **Names aligned to the R sister packages (universal, token-level convention).**
  Across all eight leagues the generated `espn_*` names follow the
  cfbfastR/hoopR/wehoop taxonomy (behavior unchanged). The rename is applied at the
  underscore-**token** level (not just prefixes), so `athlete`/`event` convert in
  every position incl. plurals: `athlete -> player` (`athlete_vs_athlete ->
  player_vs_player`, `athletes_index -> players_index`, `season_athletes ->
  season_players`), `event -> game` (bare `event -> game`, `events -> games`,
  `event_* -> game_*`, `season_week_events -> season_week_games`). Two combined
  mappings run first: `event_competitor* -> game_team*` (a competitor is the game's
  team) and `event_competition -> game_competition` / `event_competition_* ->
  game_*`. Compound tokens like `eventlog` are preserved (`athlete_eventlog ->
  player_eventlog`). cfb additionally gets `season_*` cleanups vs cfbfastR
  (`futures`/`groups`/`recruits`/`week_rankings`; `powerindex -> team_powerindex`).
  Rule engine: `generate._convention_rename`; cfb-specific exceptions:
  `tools/codegen/espn_rename_map.yaml`.
- **Collision-guarded.** Renames that would clash with a hand-written sibling or
  another generated name are skipped automatically: `teams_site` (raw endpoint, !=
  parsed `espn_*_teams`) and `espn_cfb_season_{team,awards,coaches}` (vs the
  catalog). SAME-endpoint duplicates are dropped: the generated raw
  `espn_{wbb,wnba}_game_officials` is suppressed (via `espn_rename_map.yaml` `drop:`)
  because the hand-written parsed `espn_{wbb,wnba}_game_officials` (renamed from
  `event_officials`, core-api officials with ids) exposes the same endpoint. One->many
  splits (e.g. `summary`) remain for curation
  (see `docs/superpowers/specs/espn-r-naming-worksheet.md`).
- **Versioned collision rule (dynamic, "one stays bare").** When a generated name
  would collide with an existing function but they hit *different* endpoints, both are
  kept: ONE keeps the bare name and the larger/newer one is version-qualified. This is
  now decided dynamically by the generator (`_league_module_source` pass 2 +
  `_versioned_on_collision`), not hard-coded. The web-common-v3 `/athletes/{id}/stats`
  endpoint wants the bare `player_stats`; it is version-qualified to
  `espn_*_player_stats_v3` *only* when a hand-written bare `player_stats` already claims
  the name — a league without that sibling would get the bare name automatically (no
  orphaned `*_v3`).
- **Cross-league `player_stats` parity (core-v2 season) for ALL eight ESPN leagues.**
  Every league now exposes a bare `espn_<league>_player_stats` (core-v2
  `/seasons/{season}/types/{type}/athletes/{id}/statistics` season line) returning
  **one wide, self-describing row** (athlete identity + season line as
  `{category}_{stat}` columns + `team_*` identity), plus the generated
  `espn_<league>_player_stats_v3` (web-v3 comprehensive) — matching the
  hoopR/wehoop/cfbfastR convention exactly. nba, mbb, nfl, nhl, mlb, and cfb gain new
  hand-written wrappers; wnba/wbb were already converted. A single sport-aware core
  (`sportsdataverse._common_espn_player_stats._espn_player_stats`) backs all eight
  (basketball/football/baseball/hockey share the core-v2 `splits.categories[].stats[]`
  shape and athlete/team `$ref` graph). New `season_type` (`"regular"`/`"postseason"`)
  and `total` params mirror the wehoop signature. **BREAKING:** `espn_wnba_player_stats`
  / `espn_wbb_player_stats` previously hit web-v3 and returned a `dict` of category
  frames; they now return a single core-v2 season `DataFrame` (the web-v3 payload moved
  to `*_player_stats_v3`).
- **`_get` / `_csv` single source.** The HTTP + coercion helpers now live in
  `sportsdataverse._codegen_runtime` (shared by all generated wrappers);
  `_common_espn` re-exports them. **Note for test authors:** mock
  `sportsdataverse._codegen_runtime.download` (not `_common_espn._get`) to
  intercept the generated wrappers.
- **Drift guard.** `python tools/codegen/generate.py --check` (and the
  `sdv-codegen` pre-commit hook) fail if the committed wrappers fall out of sync
  with the endpoint metadata.

**BREAKING (internal):** `sportsdataverse._common_espn` no longer exposes the
factory (`make_league_module` / `_bind` / the `_*_WRAPPERS` tables) or the private
`_site_v2_*` / `_core_v2_*` core functions. Public `espn_<league>_*` wrappers are
unchanged in name and behavior.

### NHL native — codegen cutover + clean names (api-web; in progress)

The hand-written NHL native modules are being regenerated from endpoint specs
(via `tools/codegen/extract_native.py` -> flat-API YAML -> `generate.py`) with
**clean, R-aligned names**, family by family. **First family: `nhl_api_web`.**

- **BREAKING renames** (`nhl_web_* -> nhl_*` where the clean name is free; the
  qualifier is kept only on collision with a hand-written composite, so
  `nhl_web_pbp` and `nhl_web_schedule` are unchanged): e.g. `nhl_web_boxscore ->
  nhl_boxscore`, `nhl_web_standings -> nhl_standings`, `nhl_web_roster ->
  nhl_roster`, `nhl_web_scoreboard -> nhl_scoreboard` (26 functions; full map in
  `tools/codegen/rename_map.yaml`). Behavior (URL + params + parser) is identical
  -- faithfulness was verified by `test_parity_native` before the swap.
- `nhl_scoreboard` (the 3-way team/date/now branch) stays hand-written in
  `nhl_api_web_extra.py` -- the single-URL-builder codegen can't represent it.
- **Removed** the deprecated `sportsdataverse.nhl.nhl_api` module (targeted the
  retired `statsapi.web.nhl.com`); use `nhl_api_web` / `nhl_pbp` instead.
- **`nhl_edge`** (family 2) and **`nhl_stats_rest`** (family 3) are now generated
  too. Both keep their meaningful API namespaces (`nhl_edge_*`, `nhl_stats_rest_*`)
  so they are **non-breaking** codegen-ifications (35 + 21 functions). stats_rest's
  arbitrary `**filters` power feature (cayenneExp/sort/limit/...) is preserved via
  a new `passthrough_query` engine mode that forwards None-filtered `**kwargs` as
  query params; `return_parsed` is additionally wired where a parser exists.
- **`nhl_records`** (family 4) is generated too -- kept `nhl_records_*` (distinct
  records.nhl.com product), **non-breaking** (50 functions: 44 generated +
  `passthrough_query`, 6 value-embedded/scope-conditional ones preserved
  hand-written in `nhl_records_extra.py`).
- **`mlb_api`** (family 5, final) is generated too -- kept `mlb_api_*` (the raw MLB
  Stats API namespace, distinct from the curated `mlb_*` composites),
  **non-breaking** (41 names: 26 generated + `passthrough_query` for hydrate/fields,
  15 conditional-`_csv` / multi-param / `/api/v1.1/`-host functions preserved
  hand-written in `mlb_api_extra.py`).
- **All five native families are now codegen-generated.** Only `nhl_api_web` was a
  breaking rename (its `web` qualifier was host-noise); the other four kept their
  meaningful API namespaces. The codegen engine gained flat-API collision
  resolution (`FlatApi.qualifier` + `resolve_name`), `passthrough_query`, and a
  `build_flat`/`--check` drift gate. `test_parity_native` locked in each family's
  faithfulness before its swap.

### Dataset loaders — release manifest + drift audit

- **`releases.yaml` manifest expanded 24 -> 92 loaders**, seeded from the live
  sportsdataverse-data release list: every release tag shipping season-partitioned
  `*.parquet` assets gets a 404-safe loader entry whose URL is derived from the
  actual asset names (verified to resolve). New coverage: WNBA (`espn_wnba_*` +
  `wnba_stats_*`), **PWHL** (15 datasets, a new league), NHL (full `nhl_*` family
  incl. EDGE/lite/boxscores), WBB, NBA, MBB.
- **`generate.py --audit-releases`** compares the manifest against the live release
  list (gh CLI) and reports tags with no loader (gaps) + orphans -- a CI-oriented
  drift gate (separate from the offline `--check`). `tests/codegen/fixtures/release_tags.txt`
  snapshots the live tags for offline coverage tests.
- Release tags that don't yet ship parquet (empty / csv-only / season-less -- e.g.
  several `espn_cfb_*` advanced-box tags, `nba_stats_*` boxscores) are intentionally
  absent and surfaced by the audit until parquet lands.
- **`@return` column tables** (Task 4): every non-stub loader's parquet footer is
  introspected into `tools/codegen/schemas/loader_schemas.yaml` (92 datasets) and
  rendered as a `|col_name|type|` table in the generated loader docstrings
  (reproducible via `generate.py --loader-schemas`).
- **All loader modules are now generated** (Task 5 complete). The new
  `sportsdataverse.pwhl` league (15 loaders) plus the six existing leagues
  (`cfb`/`mbb`/`nba`/`nhl`/`wbb`/`wnba`) are rendered from the manifest into
  `{league}/{league}_loaders.py` -- expanding from 4 hand-written loaders per league
  to the full release-backed set (nhl 24, wnba 25, wbb 11, nba 9, pwhl 15, ...),
  each with `@return` column tables. **Zero loss** (verified before/after): the
  season-less / helper functions the loop template can't express are preserved
  hand-written in `{league}_loaders_extra.py` residuals -- `cfb`:
  `load_cfb_betting_lines` + `get_cfb_teams`; `nhl`: `nhl_teams`. The codegen
  `build`/`--check` drift gate covers all generated loader modules
  (`_GENERATED_LOADER_LEAGUES`). Verified live: `load_pwhl_pbp(2024)` -> 10,456 rows,
  `load_nhl_pbp_lite(2010)` -> 400,512, `load_wnba_shots(2024)` -> 45,480.

### Generated documentation — reference pages + drift gate

- **`generate.py --docs`** renders the full reference docs tree from the same
  endpoint/loader/parameter metadata that drives the wrappers, directly into the
  live Docusaurus "Next" surface (`docs/docs/{league}/`). 64 files: per-league
  `index.md` + `_category_.json`, a per-API reference page for every ESPN API
  (`site`/`web`/`core`) and native flat family
  (`nhl_api_web`/`nhl_edge`/`nhl_stats_rest`/`nhl_records`/`mlb_api`), a
  `reference/loaders.md` per loader league, and a shared `reference/parameters.md`.
  This **replaces the legacy Sphinx apidoc dumps** — the 7 per-league
  `index.md` Sphinx pages plus the hand-authored NHL/MLB conceptual pages were
  regenerated/removed; package-wide conceptual pages (`intro`, `quality-of-life`,
  `architecture/`, `parsers/`) are preserved untouched.
- **8-section function block** (`templates/_reference_block.jinja`): summary,
  endpoint URL, a concrete valid example URL, an nba_api-style
  `| API Parameter | Python | Pattern | Required | Nullable |` table, a `@return`
  column table sourced from the `returns_schema` (handles both `kind: dataframe`
  and multi-frame `kind: frames` payloads), a runnable `python` example, and a
  validated-date line.
- **Names never drift from code**: the per-endpoint name-resolution passes were
  extracted into shared `_espn_league_views()` / `_flat_views()` helpers used by
  both the module renderer and the docs renderer, so a reference page always
  documents the exact wrapper name that gets emitted (e.g. the collision-qualified
  `nhl_web_pbp` alongside the clean `nhl_boxscore`).
- **Drift gate**: `--check` now also fails on stale generated docs (and orphans
  inside the fully-generated league/`reference/` dirs — conceptual pages outside
  them are never flagged); the default `build` writes them.
  `tools/codegen/fetch_packages.py` snapshots the SDV package list for an optional
  packages page (network tool; the gate stays offline by omitting the page when no
  snapshot is committed). New offline tests `tests/codegen/test_docs.py` +
  `test_doc_parity.py` assert the 8-section contract across every league x API and
  that the live tree is current.
- **Docusaurus migration**: `docs/sidebars.ts` now drives each league as a
  clickable category (link → generated `index`) expanding to an autogenerated
  reference subtree, so new endpoints surface in the nav with no sidebar edit;
  added a top-level "Parameter reference" entry. The legacy Sphinx pipeline
  (`create_docs.sh` + `Sphinx-docs/`) is **deleted**, along with its now-unused
  `sphinx`/`sphinx-markdown-builder`/`sphinx-material` dev dependencies (dropped
  from the `docs` extra/group + the `all` extra; `uv.lock` re-resolved). `yarn build`
  passes with a link-clean `/docs/next/` surface (remaining broken-anchor warnings
  are confined to the frozen `0.0.50` version + the CHANGELOG doctoc fragments).
- **Example notebooks are CI-executed** (`nbmake`): the example notebooks were
  audited against the post-rename API — only `02_cfb_intro` broke (the standalone
  `espn_cfb_pbp(game_id=...)` is gone), and its PBP cells were rewritten to the
  `CFBPlayProcess(gameId=...).espn_cfb_pbp()` + `.run_processing_pipeline()` flow
  (verified live). `nbmake` was added to the `test` dependency group, and the
  weekly `live-tests-cron` workflow now runs `pytest --nbmake examples/notebooks/`
  as an informational (non-blocking) leg so notebook breakage surfaces as drift.
- **Cohesive intro docs**: a new [Ecosystem & philosophy](https://py.sportsdataverse.org/docs/ecosystem)
  page ties the docs together — the design philosophy, the full function-naming
  paradigm (`espn_<league>_*`, native `<league>_*`, `load_<league>_*`, `parse_*`,
  plus the R-aligned athlete→player / event→game conventions and collision rules),
  the Python ↔ R sister mapping (hoopR / wehoop / cfbfastR / baseballr / fastRhockey,
  plus oddsapiR / recruitR / sportyR / sportypy / sportsdataverse.js), and how the
  package relates to nflverse (the NFL module mirrors nflreadpy) and the wider
  PySport ecosystem. `intro.md` and all seven example notebooks now open with a
  consistent philosophy/naming blurb and link to it. The page also documents the
  companion **data repositories** (sportsdataverse-data releases, cfbfastR-data,
  fastRhockey-data, nflverse-data) behind the `load_*` family and links each
  league's generated *Automation status* loader table, and includes a **1:1
  function map** — a table whose `sportsdataverse-py` functions deep-link to their
  reference pages and whose R-sister functions link to the matching
  hoopR/wehoop/cfbfastR/fastRhockey/baseballr pkgdown docs (verified against each
  package's NAMESPACE). The
  [ESPN cross-league architecture](https://py.sportsdataverse.org/docs/architecture/espn-cross-league)
  page was realigned from the retired `make_league_module()` runtime factory to the
  current declarative-codegen reality.
- **Docs default flipped to the overhauled tree + per-release snapshot policy**:
  `docusaurus.config.ts` sets `lastVersion: 'current'` (labelled `main`), so the
  generated reference + conceptual docs are the live DEFAULT at the root `/docs/`
  and auto-refresh on every deploy instead of sitting at `/docs/next/` behind the
  frozen 0.0.50 Sphinx dumps; the legacy docs stay archived at `/docs/0.0.50/`. The
  site builds on [Vercel](https://vercel.com) on push to `main` (no in-repo deploy
  workflow — a GitHub Pages action would double-publish). At each release, freeze a
  per-release archive with the new `cd docs && yarn version:docs <x.y.z>` helper
  (keeping `current`/`main` the default) — so the live docs never drift from the
  code (codegen `--check`-gated) yet every release still gets a frozen record. The
  release step is documented in CLAUDE.md.
- **Home page refreshed**: `docs/src/pages/index.tsx` was rewritten from the stale
  MBB/CFB/EPA cards to the full current surface — Basketball / Football / Baseball /
  Hockey (incl. native NHL & MLB APIs, loaders, the tidy-by-default parser layer) —
  each card naming its R sister, with an "Ecosystem & philosophy" call-to-action.
- Declined follow-up: a data-driven SDV navbar dropdown — `projects.json` carries
  no canonical doc URLs, so the curated navbar in `docusaurus.config.ts` (which
  has them) stays authoritative.

### CFB — advanced box score expansion (`create_box_score`)

`CFBPlayProcess.create_box_score()` (and therefore `run_processing_pipeline()`'s
`advBoxScore`) now emits two additional per-player sections alongside the existing eight:

- **`defensive_players`** — per-defender havoc events attributed by player and defensive
  team: `sacks` (+`sacks_yards`), `pass_breakups`, `interceptions` (+`interceptions_yards`),
  `forced_fumbles`, `fumble_recoveries` (+`fumble_recoveries_yards`). Keyed by
  `def_pos_team` + `player_name`. Columns present vary per game (only populated stats appear);
  all values derive from existing enriched play columns (no new tracking data).
- **`specialists`** — per-player kicking/punting/return production keyed by `pos_team` +
  `player_name`: `field_goals` (+`field_goals_yards`), `punts` (+`punts_yards`),
  `kick_returns` (+`kick_returns_yards`), `punt_returns` (+`punt_returns_yards`).

Both are additive and degrade to `[]` when no events are attributable. The existing
`pass`/`rush`/`receiver`/`team`/`situational`/`defensive`/`turnover`/`drives` sections are
unchanged.

### CFB — box-score attribution correctness + ESPN-sourced totals (`create_box_score`)

A correctness pass on team/player attribution in the advanced box score, reconciled
against ESPN's official box score for a 5-game fixture set (all turnover totals now match
ESPN exactly). All output is additive — existing field names are preserved; previously
wrong values are corrected and new fields/sections are added.

- **Per-play attribution layer** (`__add_attribution_cols`): resolves the credited team for
  every play from the play text + flags, aware that `pos_team`/`def_pos_team` swap roles by
  play type (on a kickoff `pos_team` is the receiving team; on a punt it is the punting
  team). Produces `kicking_team`, `return_team`, `fumbling_team`, `recovery_team`,
  `recovery_team_2`, `penalized_team`, and per-side turnover flags.
- **Special-teams turnovers are now counted.** Previously the turnover box filtered to
  scrimmage plays, dropping muffed punts, kickoff-return fumbles, and blocked-kick
  recoveries; these are now included. Muffs (`"muffed by …"`) are detected as fumbles, and
  overturned plays (`"(Original Play: …)"` after a reversed review) are stripped before
  parsing so a reversed fumble is not counted.
- **Per-side turnover model.** A single play can register a turnover for **both** teams via
  `is_pos_team_turnover` / `is_def_pos_team_turnover` and a 2-deep recovery chain — e.g. an
  interception returned and fumbled back, or a sack-strip where the recovering defense
  fumbles it back. Turnover margins/luck are keyed by team identity (fixing a prior
  group-order bug that could swap or sign-flip them). The `turnover` list is now ordered
  `[home, away]` and every row carries `team_id` — consumers should key by `team_id`
  rather than list position (the previous order came from an unordered group-by).
- **Correct team attribution** for fumble recoveries (own recoveries credited to the
  recovering team, not always the defense), punt returns (credited to the returning team,
  not the punting team), and **penalty yards** (charged to the penalized team via
  `penalty_yards`, with the legacy `total_pen_yards` retained).
- **End-of-period play-drop fix.** A dedup heuristic was dropping the real play immediately
  before an "End of period/half/game" marker (which inherits its start state) — losing
  end-of-half turnovers such as a Hail Mary interception. Guarded so end markers never
  trigger dedup of the preceding play.
- **ESPN-sourced totals.** New **`espn_team`** and **`espn_players`** sections surface
  ESPN's official box verbatim (turnovers, fumbles lost, interceptions, total/passing/
  rushing yards, penalties, first downs, player stat lines) as the authoritative source for
  countable totals. The `turnover` section sources `turnovers`/`Int`/`fumbles_lost` from the
  ESPN box (`espn_sourced=True`), keeping the play-by-play derivation under `*_pbp` keys as
  the fallback and as a validated cross-check.
- **Clean player names.** `run_processing_pipeline()` joins ESPN's per-play participants
  (`espn_cfb_play_participants`) to replace regex-extracted names (which carried team
  prefixes, e.g. `"BYU Dayan Ghanwoloku"`) with clean display names, with graceful fallback
  to the regex names when offline. Set `join_participants = False` to skip the fetch
  (used by offline reprocessing and the offline test suite).

### CFB — play-type reclassification: interception-return-fumble guard (`__add_new_play_types`)

- **Interceptions are no longer mislabeled as fumble recoveries.** The "strip-sack →
  fumble" reclassification rules fire on `fumble_vec & pass & change_of_poss==1`. An
  interception also sets `change_of_poss=1`, so a pick whose returner subsequently fumbled
  matched the predicate and was relabeled `"Fumble Recovery (Opponent)"`, erasing the
  interception (and, because the downstream `int` flag is derived from `type.text`, zeroing
  it for EPA/WPA and the box score). Both pass strip-sack rules now additionally require
  `type.text` not be an interception label (`int_vec`), so these plays keep their
  interception classification (normalized to `"Interception Return"` later in the method).
  Genuine strip-sacks — and the rush strip-sack rule, which cannot match an interception —
  are unchanged. Verified across a 20-game / 3,439-play before/after diff: exactly one play
  changed (`Fumble Recovery (Opponent)` → `Interception Return`), zero other plays affected.
- **Post-attribution play-type refinement (`__refine_play_types_post_attribution`).** A new
  pipeline step (after `__add_attribution_cols`) corrects two labels that need the turnover
  signal the step-5 reclassifier lacks (it can only see `change_of_poss`, which is `True` on
  every possession flip, not just turnovers):
  - A sack-fumble the offense **recovers itself** was relabeled `Fumble Recovery (Opponent)`
    (spurious `change_of_poss`); `is_turnover == False` restores `Fumble Recovery (Own)`.
  - A punt-return fumble the **punting team** recovers (`recovery_team == pos_team`) becomes
    `Punt Team Fumble Recovery` instead of staying `Punt Return`.

  Only the package's own first-pass relabels are undone (guarded on `orig_play_type`); the two
  frozen `type.text`-derived columns EPA/WPA read (`downs_turnover`, `pos_score_diff_end`) are
  recomputed so EPA stays consistent (e.g. a 4th-down self-recovery short of the sticks is now
  correctly scored a turnover on downs). ESPN-sourced box turnover totals are unaffected.
  Verified across a 20-game / 3,439-play before/after diff: exactly two plays changed (both
  intended relabels), with EPA moving only on those two plays — no collateral drift.

### CFB — blocked-kick turnover flags + ESPN native-flag tripwires

- **New `is_blocked_punt_turnover` / `is_blocked_fg_turnover` per-play flags (additive).**
  `is_turnover` models only *giveaways* (INT + fumbles lost) to match ESPN's official-box
  `turnovers` definition (so the `*_pbp` cross-check stays exact). A blocked kick the defense
  recovers is a possession loss but **not** a giveaway — ESPN's official box does not count it
  (verified) — so each is surfaced as a standalone flag kept out of `is_turnover` /
  `is_st_turnover`: `True` on a `Blocked Punt`/`Blocked Field Goal` Touchdown, or the non-TD
  variant with a possession change. These are the possession-losing classes ESPN's per-play
  `isTurnover` flag catches that the giveaway-based derivation does not.
- **Blocked-FG mislabel fix.** ESPN sometimes types a blocked field goal returned by the defense
  as `Extra Point Missed`, routing it through PAT-scoring EPA logic. `__add_new_play_types` now
  relabels these to `Blocked Field Goal[ Touchdown]` (gated on `"blocked"` + an FG/`field goal`
  text token, so a genuine blocked PAT is untouched), which also corrects the EPA. Because the
  relabel runs before the `type.text`-derived flag computation, all downstream flags recompute
  cleanly (no staleness).
- **ESPN native `isTurnover` / `isPenalty` are kept as cross-checks, not sources of truth.** They
  pass through the flattener as columns (populated back to 2018). `isTurnover` is coarser (it
  silently drops ~16% of plain interceptions on sparse-text plays and has no per-side/ST concept);
  `isPenalty` flags only *primary*-penalty plays. New regression tripwires
  (`test_espn_flag_tripwires.py`) assert `isTurnover ⇒ is_turnover OR is_blocked_punt_turnover OR
  is_blocked_fg_turnover` and `isPenalty ⇒ penalty_flag` on the fixtures — the first would have
  caught the interception-erasure bug above. Validated across 150 games (24,876 plays): all
  blocked-punt and blocked-FG possession losses captured with 100% ESPN agreement and zero leakage
  into the giveaway signals; penalty tripwire 0 violations; `isTurnover`/`is_turnover` agreed 99.6%
  (residual disagreements are ESPN false positives — self-recovered fumbles — the stricter
  derivation correctly excludes).

### CFB — pre-2014 era support (`CFBPlayProcess`)

Validated across a 240-game sweep (15 games × 2004-2019): every game that has play-by-play
produces valid EPA/WPA and a full advanced box score in every era (209/209 of the sampled
games-with-plays; games without PBP exit early gracefully). Legacy ESPN labels that only appear in
older seasons are now normalized in `__add_new_play_types` (each rule is gated on the raw label, so
it is a no-op on modern data):

- **`2pt Conversion`** — ESPN's pre-2014 *successful* two-point label — is resolved via
  `scoringPlay` to `Two-Point Conversion Good` / `Two-Point Conversion Missed`, so it routes
  through the two-point EPA/scoring path instead of being scored as a generic play.
- **2004 `Unknown` rows** are relabeled from their text: period/game markers → `End Period` (so
  these non-plays are excluded from aggregates instead of producing garbage EPA), and the handful
  of misclassified kicks → `Field Goal Missed` / `Extra Point Missed` / `… Good`.
- **`Kickoff Return (Defense)`** (pre-2014 onside-kick-recovered) → `Kickoff`.
- **Separate extra-point rows** are normalized to the no-down sentinel (`down`/`distance = -1`) for
  the few pre-2005 games that ship a real down on them (2005+ and two-point rows already use it).

Era notes (documented in the architecture reference): pre-2014 player attribution is
text-extraction only (the participants endpoint returns nothing before 2014; `__join_participants`
already falls back to regex names); ESPN's own win-probability array is empty before ~2016 but
`wpa` is computed in-house in every era; and PBP coverage is sparse before 2008 (~47% of 2004
games have no PBP), handled by the existing early-exit.

### Removed — NCAA bracketology

- **`espn_mbb_bracketology()` / `espn_wbb_bracketology()` removed.** The non-league
  `sports.core.api.espn.com/v2/tournament/{22,23}/seasons/{y}/bracketology` wrappers added in
  0.0.51 — along with the `_common_ncaa.py` module and the `register_ncaa_bracketology()`
  registration machinery — have been removed. The endpoint is ephemeral (ESPN only publishes it
  during the Jan–Mar projection window) and sat outside the per-league URL pattern, so it is no
  longer carried. The universal `espn_mbb_*` / `espn_wbb_*` wrappers are unaffected.

## 0.0.52 Release: June 3, 2026

### CFB — offline reprocess support (`CFBPlayProcess`)

Three additive, non-breaking changes that let college-football games be rebuilt from
on-disk raw JSON without re-hitting ESPN, in support of the `cfbfastR-cfb-raw` scraper's
reprocess pipeline:

- **Raw summary allowlist now keeps `injuries` and `gameNotes`.**
  - Before: `CFBPlayProcess(gameId=..., raw=True).espn_cfb_pbp()` filtered the ESPN summary
    to 15 keys and dropped `injuries`/`gameNotes` even when ESPN returned them.
  - After: both keys are retained (defaulting to `[]` when ESPN omits them). All previously
    returned keys are unchanged — this is purely additive.
- **New `CFBPlayProcess.odds_source` attribute.**
  - Before: there was no way to tell where the resolved spread/total came from.
  - After: `proc.odds_source` is set to one of `"summary_pickcenter"`, `"core_odds_api"`,
    `"default"`, or `"injected"` during odds resolution.
- **New `CFBPlayProcess(odds_override=...)` constructor argument.**
  - Before: odds resolution always consulted the summary `pickcenter` and, for 2024+ games
    with an empty `pickcenter`, cascaded to the live `sports.core.api.espn.com` odds
    endpoint — falling back to hardcoded defaults `(2.5, 55.5, True, False)` on failure.
    An offline rebuild could therefore silently hit the network or inherit wrong spread
    inputs that corrupt every play's EPA/WPA.
  - After: passing `odds_override={"gameSpread": ..., "overUnder": ..., "homeFavorite": ...,
    "gameSpreadAvailable": ...}` short-circuits resolution to use exactly those values, sets
    `odds_source="injected"`, and never touches the network or the defaults. With no
    override supplied (the default), behavior is unchanged. The override is validated and
    type-coerced at the constructor (a missing key or non-dict raises `ValueError` instead
    of a later `KeyError`).
- **`odds_source` is also written into the returned payload** (not just the instance
  attribute), so dict consumers of `run_processing_pipeline()` / `run_cleaning_pipeline()`
  retain odds provenance.

## 0.0.51 Release: May 30, 2026

### User-facing quality-of-life additions

Three top-level helpers that significantly reduce friction for new
users and notebook-driven exploration.

**`sportsdataverse.parsed.*`** — DataFrame-by-default mirror of every
league's wrappers. The standard `sportsdataverse.nba.espn_nba_scoreboard()`
returns raw `Dict`; the new `sportsdataverse.parsed.nba.espn_nba_scoreboard()`
returns a polars `DataFrame`. Both share the same underlying function
and accept the same `return_parsed=False` / `return_as_pandas=True`
overrides, but the default flips per import path. Available for all 8
leagues (`parsed.nba`, `parsed.wnba`, `parsed.mbb`, `parsed.wbb`,
`parsed.cfb`, `parsed.nfl`, `parsed.mlb`, `parsed.nhl`). Wrappers
without a registered parser pass through unchanged.

**`find_team` / `find_athlete` / `find_event`** — name-to-ID
resolvers in `sportsdataverse.find` (also re-exported at the package
top level). Eliminates the "what's the magic ID for X" friction:

```python
from sportsdataverse import find_team, find_event

find_team("lakers", league="nba")["id"]                     # '13'
find_event(date="2024-06-17", league="nba", home="Boston")  # NBA Finals G5
```

All three support `multi=True` for every match, case-insensitive
substring matching against the relevant fields, and an in-process
team-list cache (clearable via `clear_team_cache(league=None)`).

**`list_functions` / `function_count`** — searchable function index
in `sportsdataverse.discover` (also re-exported at the package top
level). Replaces `dir()` + grep:

```python
from sportsdataverse import list_functions, function_count

function_count()
# {'cfb': 149, 'mbb': 146, 'mlb': 196, 'nba': 143, 'nfl': 208,
#  'nhl': 199, 'wbb': 151, 'wnba': 148} — 1,340 callables total

list_functions(search="pbp")          # cross-league PBP wrapper inventory
list_functions(league="mlb", parsers_only=True)  # just the parsers
list_functions(league="nhl", wrappers_only=True) # everything except parse_*
```

24 new offline tests in `tests/test_qol.py` cover all three QoL
additions including the backwards-compatibility invariant (importing
`parsed.*` must NOT mutate the raw module's default).

New doc page `docs/quality-of-life.md` with a side-by-side comparison
showing the four-line "before 0.0.51" equivalent vs the two-line
"after" recipe (find_event → parsed.espn_nba_summary). Intro page
Quickstart updated to show the parsed.* import path first.

**Tiered TTL response cache** — new `sportsdataverse.cache` module
adds a six-tier HTTP cache layer that `sportsdataverse.dl_utils.download`
consults before hitting the network. Three modes (`off` (default),
`memory`, `filesystem`) and six TTL tiers picked by URL inspection:

```python
import sportsdataverse as sdv

sdv.set_cache_mode("filesystem")  # persists to ~/.cache/sportsdataverse/
# IMMUTABLE (30d): completed-game PBP/boxscore, glossaries, NHL Records
# REFERENCE  (7d): venues, franchises, divisions, seasons, draft picks
# SLOW      (24h): team rosters, athlete /landing
# MODERATE   (1h): default — leaders, season-to-date stats
# FAST       (5m): news, injuries
# LIVE         (0): /scoreboard/now, /standings/now — never cached
```

Scoreboard URLs with `dates=YYYYMMDD` get special handling: past dates
become IMMUTABLE (game results don't change), future dates stay LIVE.
Per-call `cache_ttl=` kwarg on `download()` overrides the tier picker,
and `$SDV_PY_CACHE_DIR` overrides the on-disk location. Invalidation:
`sdv.clear_cache()`, `sdv.clear_cache(pattern="*roster*")`,
`sdv.clear_cache(url="https://...")`. 19 offline tests in
`tests/test_cache.py`.

**404 error messages with actionable next-action hints** —
`NoESPNDataError` messages now include a tailored `Suggestion:` line
inferred from the URL. A 404 on `/teams/9999/roster` suggests
`find_team(name, league='nfl')`; an athlete 404 suggests
`find_athlete(name, league='mlb', team=<team>)`; a summary 404 suggests
`find_event(date, league='nba', home=..., away=...)`. League is
extracted from both ESPN URL shapes (the flat `site.api/.../sports/<sport>/<league>/`
form and the nested `sports.core.api/v2/sports/<sport>/leagues/<league>/`
form). 14 offline tests in `tests/test_errors_suggest.py`.

**`sdv` console script** — argparse-based CLI installed via
`[project.scripts]` in `pyproject.toml`. Six subcommands wrap the
top-level QoL helpers so users can poke at the package without
spinning up a Python REPL:

```sh
sdv find-team lakers --league nba
sdv find-event 2024-06-17 --league nba --home Boston
sdv list-functions --league mlb --search statcast
sdv function-count
sdv cache mode --set filesystem
sdv cache stats
sdv cache clear --pattern "*roster*"
```

A `--json` flag on any command emits raw JSON for piping to `jq`; the
default is a human-readable format. Exit codes: 0=success, 1=no match,
2=runtime error. 19 offline tests in `tests/test_cli.py`.

---


A second big release on top of `0.0.50`. The headline items:

- **New `sportsdataverse.mlb` module** (greenfield) — 175 functions
  spanning three data surfaces:
  - 113 ESPN cross-league wrappers + 5 ESPN originals
  - 40 official MLB Stats API wrappers (`statsapi.mlb.com`)
  - 17 Baseball Savant / Statcast wrappers including auto-chunked
    25,000-row truncation handling on `/statcast_search/csv`
- **NHL migrated to `api-web.nhle.com/v1/`** — the deprecated
  `statsapi.web.nhl.com` host is gone; replaced with 26 modern
  `nhl_web_*` wrappers grounded in the OpenAPI spec at
  `fastRhockey/data-raw/nhl_api_web_openapi.yaml`.
- **Cross-league ESPN port from hoopR / wehoop / cfbfastR** — 804 new
  wrappers across 8 leagues (NBA, MBB, WNBA, WBB, CFB, NFL, MLB, NHL)
  via a single ~80-function core (`_common_espn.py`) parameterized on
  the `(sport, league)` slug.  Each per-league extension module is a
  5-line file calling `make_league_module()` to mass-register the
  wrappers with proper `__name__` / `__qualname__` / `__doc__` for IDE
  discoverability.
- **3 new NHL modules** for the historical / Statcast surfaces:
  - `nhl_edge` — 35 wrappers for the NHL EDGE player-tracking system
    (`api-web.nhle.com/v1/edge/*`)
  - `nhl_stats_rest` — 21 wrappers for the official stats REST API
    (`api.nhle.com/stats/rest/`) with verbatim Cayenne filter expression
    support
  - `nhl_records` — 50 wrappers for the records site
    (`records.nhl.com/site/api/`) covering awards, coaches, franchises,
    HOF, draft, all-star, GMs
- **NCAA bracketology** — `espn_mbb_bracketology()` and
  `espn_wbb_bracketology()` for the non-league
  `sports.core.api.espn.com/v2/tournament/{22,23}/seasons/{y}/bracketology`
  endpoint (live during the projection window, Jan-Mar).
- **20 polars/pandas parsers** in `_common_espn_parsers.py` covering
  the most-used ESPN payload shapes (scoreboard, teams, standings,
  groups, athlete overview/stats/gamelog/splits, leaders, coaches,
  draft, event-competitor surface, team schedule/roster, news,
  injuries, generic Core v2 paginated lists).
- **4 NHL EDGE family parsers** + 3 sub-frame parsers in
  `nhl_edge_parsers.py`, schema-grounded against live captures from
  2026-05-23.
- **`return_parsed=True` dispatch shim** — every wrapper whose short
  name has a registered parser (**57** keys currently in
  `ENDPOINT_PARSERS`) gains an optional `return_parsed=True` kwarg that
  routes the raw response through the parser and returns a polars
  DataFrame (pandas via `return_as_pandas=True`).  The raw-Dict path is
  unchanged — the shim is backwards-compatible and strictly additive.
- **80 offline parser tests** (NHL EDGE 32 + universal ESPN 16 + the
  cross-league shim suite) + **32 live-gated integration tests** under
  `SDV_PY_LIVE_TESTS=1` so default test runs never hit live endpoints.

### New: MLB module (greenfield)

- New top-level `sportsdataverse.mlb` package with 8 submodules.
- `mlb_api.py` (40 functions) wraps the official MLB Stats API.
  IDs to know: `sportId=1` is MLB, `leagueId` `103`=AL / `104`=NL,
  `gameType` slugs `R`/`F`/`D`/`L`/`W`/`S`/`A`/`E`/`PO`.  Player IDs
  (`personId` / `batter` / `pitcher`) are the same MLBAM id space
  shared with Baseball Savant.
- `mlb_statcast.py` (17 functions) wraps Baseball Savant.  The
  unofficial CSV search at `/statcast_search/csv` truncates at exactly
  25,000 rows with no pagination; `statcast_search` raises
  `RuntimeError` when the response hits that cap (default,
  `raise_on_truncation=True`).  Use `statcast_search_chunked` for
  multi-week ranges — it auto-chunks the date range and stitches
  client-side.
- `mlb_espn_ext.py` registers 113 cross-league ESPN wrappers via
  `make_league_module(..., include_mlb=True)`, which adds the MLB-only
  `espn_mlb_athlete_hotzones` to the universal surface.

### New: NHL — `api-web.nhle.com` migration + EDGE / Stats REST / Records

- The deprecated `statsapi.web.nhl.com` is gone.  `nhl_api.py` keeps a
  small set of backward-compatible aliases that warn and delegate to
  `nhl_api_web`.
- `nhl_api_web.py` (26 functions) covers the modern game-feed API at
  `https://api-web.nhle.com/v1/`.
- `nhl_edge.py` (35 functions) wraps the NHL EDGE player-tracking
  surface — skater / goalie / team detail, shot-location, shot-speed,
  skating distance, zone time, plus 12 `*_top_10` leaderboards.

  **Note:** all 12 `*_top_10` URL paths return 404 as of 2026-05-23 —
  the OpenAPI spec lists them but they're not live.  The wrappers and
  `parse_edge_top10` are kept for forward-compatibility.
- `nhl_stats_rest.py` (21 functions) wraps the official Stats REST
  API at `api.nhle.com/stats/rest/`.  Verbatim Cayenne filter
  expression support via `cayenneExp` / `factCayenneExp` kwargs.
- `nhl_records.py` (50 functions) wraps the records site at
  `records.nhl.com/site/api/` — awards, coaches, franchises, skaters,
  goalies, draft, all-star, HOF, GMs, attendance, fastest goals, team
  records.

### New: ESPN cross-league port

- `_common_espn.py` exposes ~80 core functions parameterized on
  `(sport, league)`.
- `make_league_module(sport, league, prefix, globals(), include_ncaa=,
  include_football=, include_mlb=)` mass-registers wrappers in the
  caller's namespace.  Each per-league extension file is a 5-line wrapper.
- Wrappers use `functools.partial` with explicit
  `__name__`/`__qualname__`/`__doc__` so they behave like real functions
  for `help()`, IDE auto-complete, and `inspect.signature()`.
- The `_NCAA_WRAPPERS` table adds `rankings`, `season_recruits`,
  `season_week_rankings` for `mbb`, `wbb`, `cfb`.
- The `_FOOTBALL_WRAPPERS` table adds `season_qbr`, `season_qbr_week`
  for `nfl`, `cfb`.
- The new `_MLB_WRAPPERS` table adds `athlete_hotzones` for `mlb`.

### New: NCAA bracketology

- `espn_mbb_bracketology(season, iteration=None)` / `espn_wbb_bracketology(...)`
  at `sports.core.api.espn.com/v2/tournament/{22,23}/seasons/{y}/bracketology`.
- The endpoint is **seasonal** — live during the projection window
  (roughly January through March each year) and 404s the rest of the
  year.  Integration tests handle this with `pytest.xfail` so off-season
  CI runs don't fail.

### New: `_common_espn_parsers.py` (polars / pandas parser layer)

- 20 parsers covering the highest-traffic ESPN payload shapes.  All
  parsers are **league-agnostic** — the same parser handles MLB, NFL,
  NBA, etc. because ESPN's payload shapes are identical across leagues.
- Every parser returns polars by default; `return_as_pandas=True` yields
  pandas.  Empty / malformed payloads return zero-row frames rather
  than raising.
- Output columns snake-cased via `sportsdataverse.dl_utils.underscore`.
- `ENDPOINT_PARSERS` registry has 57 short-name keys mapped to 20
  unique parsers; covers the universal table plus NCAA / football /
  MLB extras.
- `parser_for(short_name)` lookup helper.

### New: `return_parsed=True` dispatch shim

- `_bind()` in `_common_espn.py` was extended with an optional
  `parser=` argument.  When present, the bound wrapper is a closure
  that adds `return_parsed=False` and `return_as_pandas=False` kwargs;
  when `return_parsed=True`, the closure dispatches the raw response
  through the parser and returns a DataFrame.
- `make_league_module()` looks up the parser via `parser_for(short)`
  on each wrapper registration.  The lookup is lazy-imported so a
  missing parsers module doesn't break the package.
- API contract: every existing caller continues to get raw `Dict` —
  the shim is opt-in via the new kwargs.

### New: `nhl_edge_parsers.py`

- 4 family parsers (`parse_edge_top10`, `parse_edge_detail`,
  `parse_edge_shot_location`, `parse_edge_zone_time`) + generic
  fallback (`parse_edge_payload`).
- 3 sub-frame parsers (`parse_edge_sog_details`,
  `parse_edge_sog_summary`, `parse_edge_hardest_shots`) for unrolling
  the rich nested lists inside detail payloads that
  `parse_edge_detail` deliberately stringifies.
- `EDGE_ENDPOINT_PARSERS` registers 33 of the 35 EDGE wrappers (the
  remaining 2 fall through to the generic parser via
  `parser_for_edge`).
- `EDGE_SUBFRAME_PARSERS` maps each detail wrapper to the tuple of
  sub-frame parsers that apply.

### New: Site v2 summary dispatcher (20 sub-parsers)

The Site v2 `summary` endpoint
(`espn_{league}_summary(event_id=...)`) ships ~19-22 top-level sections
per game (~700 KB to 1.8 MB per call). Rather than collapse that into
one parser, the summary surface now has 20 targeted sub-parsers plus a
dispatcher:

- `parse_summary_boxscore_player` — one row per (team × athlete) with
  the parallel `keys`/`stats` arrays zipped (e.g. NBA produces 27 rows
  with `min`, `fg`, `3pt`, `ft`, `reb`, `ast`, columns).
- `parse_summary_boxscore_team` — one row per (team × stat) with
  `stat_name`, `stat_label`, `stat_display_value`.
- `parse_summary_plays` — one row per play (~450 rows per NBA game).
- `parse_summary_winprobability` — one row per win-prob tick (joinable
  to plays via `play_id`).
- `parse_summary_leaders` — one row per (team × category × leader)
  from the 3-level `leaders[]` nesting.
- `parse_summary_game_info`, `parse_summary_officials`,
  `parse_summary_header`, `parse_summary_season_series`,
  `parse_summary_against_the_spread`, `parse_summary_standings`,
  `parse_summary_broadcasts`, `parse_summary_format`,
  `parse_summary_pickcenter`, `parse_summary_odds`,
  `parse_summary_article`, `parse_summary_injuries`,
  `parse_summary_news` — one row per (or one row total for) the
  corresponding summary section.
- `parse_summary_drives`, `parse_summary_scoring_plays` — NFL / CFB
  specific (NFL summary ships `drives.previous[]` + `scoringPlays`
  instead of top-level `plays`). Return zero-row frames for non-football
  leagues.
- `parse_summary(payload, section=None)` — dispatcher. With
  `section=None` returns a dict of all 20 sub-frames keyed by section
  name; with `section="<name>"` returns just that frame. Empty payload
  returns a dict of 20 zero-row frames.
- `SUMMARY_SECTION_PARSERS` — public registry mapping section name to
  parser.

Cross-league parity tests verify the dispatcher works against captured
fixtures for NBA / MLB / NFL / NHL / WNBA — same code path handles
every league's summary endpoint.

### New: 100% ENDPOINT_PARSERS coverage (121/121)

Every wrapper short name across all 4 wrapper tables
(`_UNIVERSAL_WRAPPERS`, `_NCAA_WRAPPERS`, `_FOOTBALL_WRAPPERS`,
`_MLB_WRAPPERS`) is now registered in `ENDPOINT_PARSERS`. Every
factory-bound wrapper plus the hand-bound NCAA bracketology helpers
accepts `return_parsed=True` and `return_as_pandas=True`.

Two new generic fall-through parsers cover the long tail:

- `parse_single_entity` — flattens any single-resource Core v2 payload
  (team, venue, franchise, coach, award, position, season_info,
  athlete_core, event_competitor, etc.) to a one-row frame.
- `parse_items` was already generic for `{items: [...]}` Core v2 lists
  and Core v2 `{entries: [...]}` (athlete_statisticslog); this release
  expands its registration to ~30 more list-shape endpoints (calendar
  variants, event lists, season_powerindex, talentpicks, etc.).

`register_ncaa_bracketology` was upgraded to wrap the bracketology
helpers in the same `return_parsed=True` shim used by `make_league_module`
— previously they were hand-bound without the shim.

Three regression tests lock in the invariant:

- `test_every_wrapper_short_name_has_a_registered_parser`
- `test_no_stale_entries_in_endpoint_parsers_registry`
- `test_return_parsed_shim_active_on_every_wrapper_across_all_leagues`
  (walks the `__all__` of every league extension module and verifies
  819+ wrappers carry the shim).

### New: weekly cron live-test drift detector

`.github/workflows/live-tests-cron.yml` runs the full live test suite
(`tests/test_espn_live.py` and any other `SDV_PY_LIVE_TESTS=1` gated
tests) every Monday 13:00 UTC and on `workflow_dispatch`. On failure,
the workflow uses `actions/github-script` to find or create a tracking
issue labeled `live-tests:drift`:

- First failure opens a new issue with the last 4 KB of pytest output
  plus a run URL.
- Subsequent failures comment on the existing open issue instead of
  duplicating.
- Closing the issue resets state.

Catches upstream API drift (ESPN schema changes, NHL EDGE 404s, MLB
Stats API URL moves) on a regular cadence even when the repo is
otherwise quiet between releases.

### New: MLB Stats API parser layer

`sportsdataverse.mlb.mlb_api_parsers` turns the 40 raw-Dict
`mlb_api_*` wrappers into tidy polars / pandas DataFrames. Mirrors
the design of `_common_espn_parsers`:

- Every parser returns polars by default; pandas via
  `return_as_pandas=True`.
- Empty / malformed payloads return zero-row frames.
- Output columns snake-cased via
  `sportsdataverse.dl_utils.underscore`.
- Most parsers use `pandas.json_normalize` for one-pass flattening.

Five dedicated parsers handle the high-traffic endpoints with their
own unrolling logic:

- `parse_mlb_api_schedule` — walks `dates[].games[]` and prefixes the
  schedule date onto each game row (one row per game with
  `teams.home.*` / `teams.away.*` / `venue.*` / `status.*` flattened).
- `parse_mlb_api_teams` — one row per team from `teams[]`.
- `parse_mlb_api_team_roster` — one row per player from `roster[]`
  with `person`, `position`, `status` sub-dicts flattened.
- `parse_mlb_api_standings` — walks `records[].teamRecords[]`,
  prefixes division identifiers (namespaced `standings_*` to avoid
  column collisions with team-record fields like `lastUpdated`), and
  produces one row per (division × team).
- `parse_mlb_api_person_stats` — walks `stats[].splits[]` (also
  handles `mlb_api_team_stats` with the same shape), prefixes
  `stats_type` / `stats_group` from the parent block, and flattens
  the inner `stat` block to wide stat columns.

A generic `parse_mlb_api_list` fallback handles every list-shape
endpoint that doesn't need extra unrolling (venues, sports, leagues,
divisions, seasons, awards, umpires, draft, draft_prospects,
attendance, team_leaders, team_alumni, team_affiliates, stats,
stats_leaders, stats_streaks, people, sport_players).

`MLB_API_ENDPOINT_PARSERS` registry has 26 entries (7 dedicated + 19
generic). `parser_for_mlb_api(fn_name)` returns the registered
parser; unknown names fall back to `parse_mlb_api_list` so the
caller always gets a DataFrame-returning callable.

Test fixtures captured 2026-05-24 from `statsapi.mlb.com` (8 captures
in `tests/fixtures/mlb_api/`). 17 offline tests in
`tests/test_mlb_api_parsers.py` exercise each dedicated parser plus
the generic fallback against the live fixtures.

### New: NHL Stats REST + Records parser layers

`sportsdataverse.nhl.nhl_stats_rest_parsers.parse_nhl_stats_rest` and
`sportsdataverse.nhl.nhl_records_parsers.parse_nhl_records` turn every
wrapper in their respective surfaces into a tidy polars / pandas
DataFrame.

Both APIs ship the **identical** `{data: [...], total: N}` envelope on
every endpoint, so a single parser handles every wrapper:

- `parse_nhl_stats_rest` covers the 21 wrappers in
  `sportsdataverse.nhl.nhl_stats_rest` (api.nhle.com/stats/rest/en/*).
- `parse_nhl_records` covers the 50 wrappers in
  `sportsdataverse.nhl.nhl_records` (records.nhl.com/site/api/*).

The meta Stats REST endpoints (`config`, `componentSeason`, `ping`) ship
non-`data`-keyed payloads — both parsers return zero-row frames for
those instead of raising.

Registries: `NHL_STATS_REST_ENDPOINT_PARSERS` has 17 entries (excluding
the meta endpoints).  `parser_for_nhl_stats_rest` and
`parser_for_nhl_records` always return a callable (fall back to the
generic parser — never return `None`).

### New: NHL api-web parser layer

`sportsdataverse.nhl.nhl_api_web_parsers` covers the modern game-feed
API at `api-web.nhle.com/v1/` — 16 dedicated parsers + 2 dispatchers
covering all 26 `nhl_web_*` wrappers across game-center, schedule,
score, scoreboard, standings, team, player, leaders, and draft
families.

Game-center parsers:

- `parse_nhl_web_pbp` — one row per play (~330 plays per game) with
  `eventId`, `typeCode`, `typeDescKey`, `periodDescriptor`, `details`
  flattened.
- `parse_nhl_web_boxscore` — unrolls the 6-bucket
  `playerByGameStats: {away,home}Team.{forwards,defense,goalies}`
  structure into one long-form frame, tagging each row with
  `home_away` and `position_group`.
- `parse_nhl_web_landing` — single-row game profile with venue,
  teams, periodDescriptor, gameState, summary stringified.
- `parse_nhl_web_right_rail` — **dispatcher** returning 6 sub-frames:
  `season_series`, `shots_by_period`, `team_game_stats`, `game_info`,
  `linescore_by_period`, `season_series_wins`. With `section="..."`
  returns just one frame.

Schedule / score parsers:

- `parse_nhl_web_schedule` — walks `gameWeek[].games[]`, prefixes the
  day's date onto each game row.
- `parse_nhl_web_score` — flattens `games[]` for a single date.
- `parse_nhl_web_scoreboard` — walks `gamesByDate[].games[]`,
  prefixes `scoreboard_date` (multi-day scoreboard).
- `parse_nhl_web_club_schedule` — flattens `games[]` with
  `club_timezone` / `club_current_season` / `club_previous_season` /
  `club_next_season` context columns from the parent payload.

Standings + team / player parsers:

- `parse_nhl_web_standings` — one row per team (84 stat columns
  covering full win/loss/OT/SO/ROW/L10/streak/home/away breakdowns).
- `parse_nhl_web_standings_season` — one row per season
  (108 NHL seasons since 1917-18).
- `parse_nhl_web_club_stats` — **dispatcher** returning
  `{skaters, goalies}` as separate frames.
- `parse_nhl_web_roster` — merges `forwards`, `defensemen`, `goalies`
  into one long-form frame with a `position_group` column.
- `parse_nhl_web_player_landing` — single-row player profile
  (~130 columns for a player like McDavid with full career totals,
  features, recent games).
- `parse_nhl_web_player_game_log` — one row per game from `gameLog[]`.

Leaders + draft:

- `parse_nhl_web_leaders` — walks the category-keyed leaders payload
  (`{points: [...], goals: [...]}` for skaters; `{wins: [...],
  savePctg: [...]}` for goalies), tags each row with the category
  it came from, concatenates.
- `parse_nhl_web_draft_picks` — one row per pick.

Registry: `NHL_API_WEB_ENDPOINT_PARSERS` has 24 entries covering all
the data endpoints. `parser_for_nhl_api_web(fn_name)` returns the
registered parser or `None` for the 2 idiosyncratic endpoints
(`playoff_series`, `player_spotlight`, `draft_rankings`,
`draft_rankings_now`) whose payloads are too idiosyncratic for a
useful generic fallback — callers null-check.

Test fixtures captured 2026-05-24 (17 captures from
`api-web.nhle.com/v1/`). 37 offline tests in
`tests/test_nhl_api_web_parsers.py` verify each parser against the
captured fixtures plus dispatcher contracts, empty payload contract,
pandas opt-in, and registry consistency.

---

Test fixtures captured 2026-05-24 (8 from `api.nhle.com/stats/rest/`,
6 from `records.nhl.com/site/api/`). 21 offline tests in
`tests/test_nhl_aux_parsers.py` verify parsing across:

- 7 Stats REST data endpoints (season, franchise, country, glossary,
  skater_summary, goalie_summary, team_summary).
- 6 Records endpoints (franchise, franchise_team_totals, coach, draft,
  player, attendance).
- Empty-payload contract, pandas opt-in, registry consistency, and the
  config-as-meta zero-row case.

### Bug fixes

- `parse_team_roster` now handles **both** ESPN roster shapes. The
  flat shape (`athletes[]` = list of athlete dicts; used by NBA /
  WNBA / MBB / WBB) continues to work unchanged. The newly-
  handled position-grouped shape (`athletes[i] = {position, items}`;
  used by MLB / NFL / NHL / CFB) is auto-detected by inspecting the
  first element — each player from a group's `items[]` is tagged
  with a `position_group` column carried over from the parent group.
  Without the fix, MLB / NFL / NHL / CFB rosters were collapsing to
  ~5-6 group rows instead of unrolling to the full per-player list
  (e.g. Alabama CFB went from 6 group rows to 100 player rows).

### New: NFL drive-plays parser (true PBP parity)

`parse_summary_drive_plays` rounds out the football PBP story. NFL
and CFB summary payloads don't ship a top-level `plays[]` array (the
NBA / MLB / NHL / WNBA convention); they nest plays inside each
drive at `drives.previous[i].plays[]`. The existing
`parse_summary_drives` returns one row per drive with the plays
stringified. This new parser unrolls those nested plays into a true
one-row-per-play frame with `drive_id` + `drive_sequence` columns
carried over from the parent drive — letting callers join back to
the drives frame for drive-level context.

Verified against Super Bowl LIX: 26 drives + 186 plays unrolled
into a 186-row × 43-column polars frame. Returns zero rows for
NBA / MLB / NHL / WNBA fixtures (those leagues use top-level
`plays[]`, exercised by `parse_summary_plays`).

`SUMMARY_SECTION_PARSERS` registry grows from 20 to 21 entries.
The summary dispatcher's output dict now includes the `drive_plays`
section alongside `drives` and `scoring_plays`.

### Test infrastructure

- New `tests/test_espn_universal_parsers.py` (128 tests, +22 since
  last roll-up: 8 sparse-section tests covering `broadcasts`
  (present for MLB / NHL, empty for NBA / NFL / WNBA in past-game
  captures) and the universally-sparse `against_the_spread` /
  `pickcenter` / `odds`; 3 MBB/WBB/CFB NCAA summary fixture additions
  to the cross-league parametrized tests, expanding the dispatcher +
  boxscore_player + plays + drives + officials assertions from
  5 leagues to all 8 ESPN leagues),
  `tests/test_mlb_api_parsers.py` (17 tests),
  `tests/test_nhl_aux_parsers.py` (21 tests),
  `tests/test_nhl_api_web_parsers.py` (37 tests), and
  `tests/test_nhl_edge_parsers.py` (32 tests) run offline against
  captured fixtures.
- New `tests/test_espn_live.py` (**56 live tests**, +24 since last
  roll-up: 9 NCAA-side wrapper tests (CFB/MBB/WBB × team_roster/
  news/team_schedule), 3 NCAA summary dispatcher tests, 3 MLB
  Statcast pitch-search tests (small-range happy path / multi-week
  chunked stitch / raise-on-truncation guard), plus 9 parametrized
  `return_parsed=True` shim-parity tests confirming the raw-Dict /
  polars / pandas round-trip is internally consistent for the NCAA
  surface — same wrapper invocation with vs without the kwarg must
  produce equivalent data, and `return_as_pandas=True` row count
  must match the polars row count). Gated by `SDV_PY_LIVE_TESTS=1`
  for live integration verification.
- Captured fixtures live under `tests/fixtures/espn/` (43 captures —
  the original 7 plus summary captures for **all 8 ESPN leagues**
  (NBA / MLB / NFL / NHL / WNBA + the new NCAA captures: MBB final
  Purdue@UConn, WBB final Iowa@SC, CFB national championship OSU@ND)
  plus the 28-fixture cross-league parity set covering
  `team_roster` / `team_schedule` / `news` / `injuries` for each
  league),
  `tests/fixtures/mlb_api/` (8 captures: schedule, teams, roster,
  standings, person_stats, venues, sports, divisions),
  `tests/fixtures/nhl_stats_rest/` (8 captures: season, franchise,
  country, glossary, config, skater_summary, goalie_summary,
  team_summary), `tests/fixtures/nhl_records/` (6 captures:
  franchise, franchise_team_totals, coach, draft, player,
  attendance), `tests/fixtures/nhl_api_web/` (17 captures: pbp,
  boxscore, landing, right_rail, schedule, score, scoreboard,
  standings, standings_season, club_schedule, club_stats, roster,
  player_landing, player_gamelog, skater_leaders, goalie_leaders,
  draft_picks), and `tests/fixtures/nhl_edge/` (7 captures), each
  with a README documenting provenance.
- Parametrized cross-league parity tests in
  `test_espn_universal_parsers.py` exercise the summary dispatcher
  against all 5 captured leagues and assert the full 20-section
  dispatch contract for each (boxscore_player + boxscore_team + plays +
  winprobability + leaders + 13 metadata sections + 2 football-only).

### Documentation

- README.md and docs/docs/intro.md both gain two new sections:
  - "Supported leagues and data sources" — a per-league table showing
    every module + the data surfaces it covers + wrapper counts
    (NBA=118, WNBA=124, MBB=121, WBB=126, CFB=123, NFL=119, MLB=175,
    NHL=132, total ~1,030).
  - "Polars / pandas parser layer" — quick overview of the
    `return_parsed=True` shim for ESPN wrappers + the
    compose-wrapper-with-parser pattern for the NHL / MLB sibling
    APIs. Links to the architecture + parsers docs pages.
- New documentation pages:
  - `docs/architecture/espn-cross-league.md` — the factory + shim
    architecture.
  - `docs/parsers/index.md` — the parser layer + `ENDPOINT_PARSERS`.
  - `docs/mlb/index.md` — MLB module overview (ESPN + Stats API +
    Statcast); brief pointers to the new dedicated `parsers` and
    `statcast` pages.
  - `docs/mlb/parsers.md` — dedicated MLB Stats API parsers page
    (split out from `index.md`) with the full parser table, registry +
    `parser_for_mlb_api`, four chaining examples, and a fixture
    inventory.
  - `docs/mlb/statcast.md` — dedicated Baseball Savant / Statcast
    page (split out from `index.md`) covering the 17 `statcast_*`
    wrappers, the 25,000-row truncation handling + the
    `statcast_search_chunked` auto-chunked variant, Statcast
    coverage windows by metric, MLBAM ID-space chaining with the
    Stats API, and two end-to-end examples (catcher pop times +
    World Series pitch-by-pitch). Both new pages are wired into
    the MLB category in `docs/sidebars.ts`.
  - `docs/parsers/fixtures.md` — comprehensive index of all 89
    captured live payloads across the 6 fixture directories
    (`espn/`, `mlb_api/`, `nhl_api_web/`, `nhl_edge/`,
    `nhl_stats_rest/`, `nhl_records/`). Includes the full
    endpoint mapping table per directory, the championship-game
    event IDs used for the cross-league summary captures, and a
    maintenance section explaining how to refresh a fixture.
  - `docs/architecture/building-blocks.md` — meta-documentation
    page enumerating the five low-level patterns reused across
    every parser module: `_bind` shim factory, `make_league_module`
    factory call, `_row_per_item` / `_single_row` json_normalize
    helpers, the `ENDPOINT_PARSERS` registry + `parser_for_*`
    lookup, and the dispatcher pattern (used by `parse_summary`,
    `parse_nhl_web_right_rail`, `parse_nhl_web_club_stats`). Closes
    with a step-by-step "Adding a new parser" checklist. Sidebar
    entry added under the Architecture category.
- `docs/sidebars.ts` regrouped by sport family — leagues now cluster
  by basketball (NBA / WNBA / MBB / WBB) / football (NFL / CFB) /
  baseball (MLB) / hockey (NHL) instead of alphabetical, surfacing
  the cross-league helper relationships (e.g. NCAA basketball pair
  with NBA via the same ESPN factory). Architecture + Parsers
  categories now default to expanded (`collapsed: false`) so
  newcomers see the package-wide overview first.
- `nhl/nhl_loaders.py` lint cleanup: 4 sites of
  `if type(seasons) is int:` replaced with `isinstance(seasons, int)`
  to clear pre-existing `E721` ruff warnings (no behaviour change —
  both forms accept the same input).
- `tests/conftest.py` is now the single source of truth for the
  `SDV_PY_LIVE_TESTS=1` gating mechanism. `tests/test_espn_live.py`
  was previously redefining `LIVE` + its own `pytestmark.skipif`
  marker; it now imports the shared `skip_if_no_live` from
  `conftest` and assigns it directly to `pytestmark`. Behaviour is
  identical (no env var → 56 tests skip; env var set → 56 tests
  run) but the duplication is gone and the conftest docstring now
  documents both the per-test decorator and module-level marker
  patterns for future `test_*_live.py` files.
- `tests/conftest.py` also gains a shared `load_fixture(category,
  stem)` helper that all 5 parser test modules now use instead of
  each carrying their own copy of the same
  `json.loads((FIXTURE_DIR / f"{stem}.json").read_text(...))`
  boilerplate + per-file `FIXTURE_DIR` constant. The helper raises
  `FileNotFoundError` with the expected path baked into the
  message when a fixture is missing — easier debugging of typo'd
  stems. Each test file still keeps its thin local `_load(stem)`
  alias bound to its category, so call sites (`_load("summary_nba")`)
  remain unchanged. `test_nhl_aux_parsers.py` keeps its 2-arg
  `_load(directory, stem)` signature for its dual-category load
  pattern but the underlying helper is now shared.
- `pyproject.toml` `keywords` expanded from 6 to 21 entries
  reflecting the 0.0.51 surface — full league set (nba, wnba, nfl,
  college football, ncaa basketball, mlb, nhl), data sources (espn,
  mlb stats api, statcast, baseball savant, nhl edge, nhl api-web),
  and concepts (data, epa, statistics, win probability,
  play-by-play, web scraping, polars, parser). Improves PyPI
  search discoverability for users searching by individual league
  or data source.
- New `local` pre-commit hook `sync-docs-changelog` (in
  `.pre-commit-config.yaml`): when staging changes to `CHANGELOG.md`,
  automatically re-copies the file to `docs/src/pages/CHANGELOG.md`
  (the docusaurus-rendered copy) and stages the synced file so both
  copies land in the same commit. Replaces the manual
  `cp CHANGELOG.md docs/src/pages/CHANGELOG.md` step that contributors
  used to remember by hand.
- Module docstrings on every parser + wrapper module now carry a
  `Documentation:` block linking to the matching docs page so
  `help()` / `pydoc` users land on the right reference without
  hunting. Updated modules:
  `_common_espn.py`, `_common_espn_parsers.py`,
  `nhl/nhl_api_web.py`, `nhl/nhl_api_web_parsers.py`,
  `nhl/nhl_edge.py`, `nhl/nhl_edge_parsers.py`,
  `nhl/nhl_stats_rest.py`, `nhl/nhl_stats_rest_parsers.py`,
  `nhl/nhl_records.py`, `nhl/nhl_records_parsers.py`,
  `mlb/mlb_api.py`, `mlb/mlb_api_parsers.py`,
  `mlb/mlb_statcast.py`.
- `nhl/nhl_pbp.py::espn_nhl_pbp` docstring gains a prominent
  cross-reference + comparison table distinguishing it from the
  modern `nhl_web_pbp` / `parse_nhl_web_pbp` surface (different ID
  spaces, different schemas, not interchangeable). A matching
  `:::caution:::` admonition added to `docs/nhl/api-web.md` so
  users coming from either direction find the cross-reference.
  - `docs/nhl/api-web.md` gains a "Parser deep-dive" section
    between the registry and the full example: documents the
    `parse_nhl_web_boxscore` 6-bucket unrolling pattern, both
    dispatchers (`right_rail` 6-section + `club_stats` 2-section
    breakdowns with example invocations), the roster
    merge-with-tag pattern, and the leaders category-keyed
    payload unrolling.
- `docs/docs/intro.md` gains a "Quickstart" section directly under
  the goal paragraph showing three one-liners across NBA / MLB /
  NHL covering the three primary usage modes (return_parsed shim,
  Stats API compose-with-parser, NHL EDGE compose-with-parser).
- `CLAUDE.md` gains two new top-level sections ("ESPN Cross-League
  Architecture (0.0.51+)" and "Parser Layer (0.0.51+)") that
  document the factory pattern, `make_league_module`,
  `_bind`+shim, ENDPOINT_PARSERS invariant, summary dispatcher
  contract, cross-league shape divergences captured by tests, the
  fixture inventory, and the test-file structure. ~210 lines added
  to keep future AI assistants and contributors aligned on the
  parser-layer conventions.
  - `docs/nhl/api-web.md` — the modern game-feed surface
    (`api-web.nhle.com/v1/`) with the full endpoint table and a parser
    layer section covering all 16 dedicated parsers + 2 dispatchers
    (`right_rail`, `club_stats`).
  - `docs/nhl/edge.md`, `edge-parsers.md`, `stats-rest.md`,
    `records.md` — the NHL surface (EDGE, Stats REST, Records). Each
    now includes cross-links to the other three NHL docs pages and a
    "Parser layer" section.

## 0.0.50 Release: May 7, 2026

This release is a big one. The headline items:

- A near-drop-in nflreadpy-parity surface inside `sportsdataverse.nfl`: six new loaders, two unified per-type loaders, a caching layer, runtime config, three static datasets, 25 `load_*` aliases, and current-season / current-week helpers.
- 11 new ESPN scrape modules across `wbb` and `wnba` (team rosters, season player & team stats, standings, draft, event officials), each with full `@overload` typing.
- A new `cfb_play_participants` module and a corresponding ~340-line collapse inside `cfb_pbp.__add_player_cols`.
- The long-running `0.36-live` → `main` polars-1.x reconciliation across all seven `*_pbp.py` modules (~165 API translation sites).
- Packaging fully modernized to PEP 621 `pyproject.toml` (no more `setup.py`), conda-installable via the new `recipe/meta.yaml`.
- Lint chain re-baselined on Ruff (replacing black + isort + pycln + flake8) plus a richer pre-commit set.
- Runnable `Example:` sections on ~190 public callables and seven new intro / intermediate Jupyter notebooks under `examples/notebooks/`.
- Sphinx docs build is clean under `sphinx-build -W`.

Round bump to `0.0.50` (rather than `0.0.41`) to signal scope; we are still alpha.

### Packaging modernization

- Migrated all packaging metadata from `setup.py` to PEP 621 `[project]` in `pyproject.toml`. `setup.py` is removed; `python -m build` is the only supported build path.
- License switched from classifier (`License :: OSI Approved :: MIT License`) to SPDX expression (`license = "MIT"` + `license-files = ["LICENSE"]`) for Metadata 2.4 compliance.
- Python target widened to 3.9–3.14 (3.6/3.7/3.8 dropped). Dependency lower bounds modernized (`polars>=1.0,<2.0`, `pyarrow>=14.0`, `numpy>=1.23`, `pandas>=2.0`, etc.).
- `[tool.setuptools.packages.find]` excludes `tests*`, `Sphinx-docs*`, `docs*`, `examples*`, `archive*`, `recipe*`, `dev*` from the wheel. `[tool.setuptools.package-data]` retains the `cfb/models/*` + `nfl/models/*` shipping list.
- `MANIFEST.in` trimmed to current-relevance patterns. `.gitignore` extended to ignore `dev/`, `dist_check/`, and the Sphinx `_build/` + `_static/` artifacts; tracked `Sphinx-docs/_build/` files were untracked.

### Conda installability

- New `recipe/meta.yaml`: `noarch: python` conda-build recipe that mirrors `[project.dependencies]` and consumes `pyproject.toml` directly. Two source modes documented — local `path: ..` for dev, PyPI `url:` + `sha256:` for conda-forge submission.
- New `recipe/README.md`: walks through the local `conda build recipe/` workflow and the conda-forge `staged-recipes` submission flow.
- New `.github/workflows/conda-build.yml`: verifies the recipe on every PR that touches `recipe/` or `pyproject.toml`, plus on every release. Uses `conda-incubator/setup-miniconda@v3` + miniforge / mamba; builds, installs the resulting `.conda`, smoke-imports all seven sport subpackages, uploads the built package as a workflow artifact.

### Linting & pre-commit modernization

- Replaced the legacy black + isort + pycln + flake8 chain with **Ruff** (lint, import-sort, pyupgrade, format, unused-import removal). `pyproject.toml [tool.ruff]` pins `line-length = 120`, `fix = true`, `show-fixes = true`. The standalone `isort` hook is retained ONLY to inject `from __future__ import annotations` at the top of every Python file via its `--add-import` flag — Ruff handles all other import concerns.
- `pyproject.toml [tool.ruff.lint]` ignores `E712` (intentional `pl.col(...) == True/False` for polars boolean masks), `E501` / `E402` (long-URL docstrings + module-level imports), `F601` / `F841` (legacy parser idioms). Per-file ignores cover star-imports + re-exports in `__init__.py` files (`F401` / `F403`).
- New pre-commit hooks alongside Ruff:
  - `pre-commit-hooks` (trailing-whitespace, check-merge-conflict, check-ast, check-toml/json/xml/yaml, check-symlinks, end-of-file-fixer, requirements-txt-fixer, check-added-large-files, debug-statements). The `check-yaml` hook excludes `recipe/meta.yaml` because its Jinja2 templating isn't valid pre-substitution YAML.
  - `pygrep-hooks`: `python-use-type-annotations`, `python-no-eval`, `python-no-log-warn`, `rst-backticks`, `rst-directive-colons`, `rst-inline-touching-normal`, `text-unicode-replacement-char`, `python-check-mock-methods`, `python-check-blanket-noqa`, `python-check-blanket-type-ignore`.
  - `add-trailing-comma`, `sync-pre-commit-deps`.
  - `check-jsonschema --check-github-workflows` validates `.github/workflows/*.yml` against the GitHub Actions schema.
  - `actionlint` for workflow expressions / shell.
  - `yamlfmt` (config in `.yamlfmt`: `line_ending: lf`, `eof_newline: true`).
  - `doctoc` regenerates Markdown TOCs.
  - `markdownlint-cli2` against `.markdownlint-cli2.yaml`. The config disables a handful of rules that fight legacy README / CHANGELOG content (MD013 line-length, MD030 list-marker-space, MD045 alt-text, MD051 link-fragments, MD060 table-column-style) and allows `<a>`, `<img>`, `<br>`, `<sub>`, `<sup>` in `MD033` for the README's badge / logo HTML.

### Documentation toolchain

- Added `sphinx.ext.napoleon` to `Sphinx-docs/conf.py` with explicit Google-style settings — the new `wbb` / `wnba` / `nfl` / `cfb` modules use Google-style docstrings (`Args:` / `Returns:` / `Raises:`) and these were producing 22 docutils warnings on build before napoleon was wired up.
- Added a no-op `visit_abbreviation` shim to the markdown translator in `Sphinx-docs/conf.py`. Sphinx 9 emits `abbreviation` nodes for the keyword-only `*` separator in rendered function signatures, and `sphinx-markdown-builder` 0.6.10 has no visitor for that node type. The shim emits the inner text and skips the node, so the build is now warning-free under `sphinx-build -W`.
- Module docstrings in `cfb_play_participants.py` and `nfl/utils_date.py` had bullet lists immediately following a `Caveats:` / `NFL season convention:` paragraph header. Added the required blank line + asterisk markers so docutils parses them as proper RST bullet lists.
- `Sphinx-docs/sportsdataverse.{cfb,mbb,nba,nfl,nhl,wbb,wnba}.rst` register `automodule` entries for every new ESPN scrape module shipped this release.
- `Sphinx-docs/setup.rst` deleted (was an auto-generated apidoc page for the now-removed `setup.py`). `Sphinx-docs/index.rst` fixed a single-backtick `\`toctree\`` typo so the `rst-backticks` pre-commit hook passes.

### Runnable docstring examples (~190 functions)

- Every public callable across `cfb`, `nfl`, `nba`, `nhl`, `mbb`, `wbb`, `wnba`, `dl_utils`, `decorators`, `errors`, `nfl/cache`, `nfl/config`, `nfl/datasets`, `nfl/utils_date`, and the top-level package now ships a multi-block `Example:` section: a quick-start invocation, one or two useful parameter combinations, a one-line pipeline next-step, and a `See Also:` block with cross-links to companion R packages (`wehoop`, `hoopR`, `cfbfastR`, `baseballr`, `fastRhockey`), `nflverse`, `nflreadpy`, `nba_api`, and `nhl-api-py` where applicable.
- Examples use the napoleon literal-block format (heading + `::` + 4-space indented code) so they render as proper code blocks in the markdown docs without triggering `sphinx.ext.doctest`. Users can copy-paste any block and run it as-is.
- Existing one-line backtick-wrapped examples (the legacy `Example: <inline call>` shape) were replaced (not appended) so each function has exactly one `Example:` section.

### Example notebooks

- Seven new Jupyter notebooks under `examples/notebooks/`: `01_quickstart.ipynb`, `02_cfb_intro.ipynb`, `03_nfl_intro.ipynb`, `04_nba_intro.ipynb`, `05_wbb_wnba_intro.ipynb`, `06_mbb_intro.ipynb`, `07_nhl_intro.ipynb`. Intro / intermediate level — schedule, pbp, team / player / season-stats endpoints, the `nfl.update_config` / `clear_cache` / `get_current_*` runtime surface, and a small pipeline example per sport. Outputs cleared so the user runs them locally; cross-references link to companion R packages and alternative Python libraries.
- `.gitignore` keeps `*.ipynb` ignored at the repo level (so scratch + checkpoint notebooks aren't accidentally tracked) but adds a negative pattern `!examples/notebooks/*.ipynb` so the curated tutorial notebooks are explicitly tracked.

### Contributor docs and templates

- New `CLAUDE.md` and `.github/copilot-instructions.md` capture the project conventions for AI-assisted development: branching, conventional commit messages, polars 1.x rules, HTTP layer, module patterns, NFL nflreadpy-parity surface, CFB `cfb_play_participants`, test conventions, packaging, Sphinx toolchain, the docstring conventions for new functions, common pitfalls.
- New `CONTRIBUTING.md`: canonical onboarding doc covering uv workflow, conda fallback, Python target 3.9–3.14, code standards (ruff, mypy), polars 1.x rules, test gating with `skip_if_no_live`, new-module spec.
- New `.github/PULL_REQUEST_TEMPLATE.md` and `.github/ISSUE_TEMPLATE/` (`config.yml`, `bug_report.yml`, `feature_request.yml`, `data_quality.yml`). The PR template includes an "I have NOT included AI agents (Claude / Copilot / Cursor / GPT / Gemini) as commit co-authors" checkbox enforcing project policy.

### NFL — nflreadpy parity

- Six new loaders: `load_nfl_team_stats`, `load_nfl_ftn_charting`, `load_nfl_trades`, `load_nfl_ff_playerids`, `load_nfl_ff_rankings`, `load_nfl_ff_opportunity`.
- Two new utility helpers in `nfl/utils_date.py`: `get_current_nfl_season()`, `get_current_nfl_week()`.
- Unified `load_nfl_nextgen_stats(stat_type=...)` consolidating the per-type variants. The per-type functions are kept as aliases that emit `DeprecationWarning` and forward to the unified entry point.
- Unified `load_nfl_pfr_advstats(stat_type=, summary_level=)` consolidating eight per-type / per-summary functions, with the same deprecation alias pattern.
- 25 nflreadpy-parity aliases inside `sportsdataverse.nfl` (`load_pbp` ↔ `load_nfl_pbp`, etc.). Identity-equivalent — no perf overhead, just a friendlier import surface for nflreadpy users.
- `kind=` parameter added to `load_nfl_ff_rankings` as the preferred name; `type=` retained for nflreadpy parity.

### NFL — caching and configuration

- New caching layer in `sportsdataverse.nfl.cache` with both memory and filesystem backends and TTL support.
- `clear_cache()` for explicit invalidation.
- New `NflConfig` plus `update_config()` / `get_config()` / `reset_config()`, with env-var initialization: `SDV_PY_NFL_CACHE`, `SDV_PY_NFL_CACHE_DIR`, `SDV_PY_NFL_CACHE_DURATION`, `SDV_PY_NFL_VERBOSE`, `SDV_PY_NFL_TIMEOUT`, `SDV_PY_NFL_USER_AGENT`.
- All 23 canonical loaders plus the 11 deprecated aliases are decorated with `@cached_loader`.
- `return_as_pandas=True` round-trips correctly through the cache: a single polars frame is stored, and conversion happens on read.

### NFL — static datasets

- `team_abbr_mapping` (143 entries, relocations folded into the modern abbreviation: `OAK -> LV`, `SD -> LAC`, `STL -> LA`).
- `team_abbr_mapping_norelocate` (143 entries, history preserved).
- `player_name_mapping` (136 entries, common-variant → canonical).
- All three are eagerly loaded at import time and inline-bundled in the package — no separate JSON files to ship.

### NFL — pickcenter / odds modern path

- `__helper__espn_nfl_odds_information__` now hits the modern `sports.core.api.espn.com/v2/.../events/{gid}/competitions/{gid}/odds` endpoint when the legacy `summary?event=` `pickcenter` array is empty (true for all 2024+ games).
- Cascades to defaults `(2.5, 55.5, True, False)` only if both modern and legacy paths fail.
- For example, the 2024 CFP semifinal previously returned `(2.5, 55.5, True, False)` and now correctly returns `(-3.5, 67.5, True, True)`.

### NFL — `load_nfl_schedule` parquet port

- Switched from the stale `nflverse-pbp/master/schedules/sched_{season}.rds` (which was 404'ing on every season) to the modern `nflverse-data/releases/download/schedules/games.parquet`. One combined file, 1999–2025, 7,276 rows × 46 cols.

### WBB / WNBA — new ESPN scrape modules

Eleven new modules across `sportsdataverse.wbb` and `sportsdataverse.wnba`, plus their `__init__.py` re-exports and live-gated smoke tests. The WNBA modules (other than `wnba_draft`) are thin shims onto a shared `_espn_basketball_*` helper that lives in the corresponding `wbb_*.py` file (league slug fixed to `"wnba"`), keeping the wbb/wnba pair DRY.

- `wbb_team_roster` / `wnba_team_roster`: per `(team_id, season)` roster, flattened to one row per athlete. Snake-case columns; stable schema on empty rosters.
- `wbb_player_stats` / `wnba_player_stats`: per `(athlete_id, season)` stats. Multi-table dict with canonical keys `Averages` / `Totals` / `Misc` (always present, empty-frame fallback) plus an `Other` bucket only added when ESPN ships a non-canonical category.
- `wbb_team_stats` / `wnba_team_stats`: per `(team_id, season)` stats. Same multi-table shape as player stats; ESPN ships these as `General` / `Offensive` / `Defensive` categories that map onto the canonical Averages / Totals / Misc keys. Endpoint corrected to `site.web.api.espn.com/.../teams/{id}/statistics?season=...` (the `common/v3` path the original spec named 404s).
- `wbb_standings` / `wnba_standings`: one-row-per-team season standings. WBB defaults to `group=50` (Division I women); WNBA has no group filter.
- `wnba_draft`: one-row-per-pick draft history. Modern endpoint at `site.web.api.espn.com/apis/site/v2/sports/basketball/wnba/draft` (the `site/v3` variant 404s).
- `wbb_event_officials` / `wnba_event_officials`: one-row-per-official game-level officials list.
- All eleven ship with full `@overload` typing (mypy-strict), polars 1.x APIs, and `snake_case` columns via `dl_utils.underscore`.

### CFB — `cfb_play_participants` and `__add_player_cols` collapse

- New `cfb_play_participants` module hits the ESPN `events/{gid}/competitions/{gid}/plays` participants endpoint, with `$ref` resolution (default-on, `resolve_missing=True`) for athletes missing from the sidecar.
- `cfb_pbp.__add_player_cols` shrunk from 471 lines of regex extraction to ~130 lines that delegate to the participants module.
- All 19 legacy `_player_name` columns preserved via an alias mapping.
- Hybrid scalar + list-column output: `{type}_player_name` plus `{type}_player_names`, so multi-entry types like split sacks aren't silently collapsed to a single name.
- Targeted regex fallbacks retained as a tertiary safety net for `sack_player_name2`, `fg_block_player_name`, `punt_block_player_name`, and `interception_player_name` — ESPN's sidecar has documented gaps for those.

### CFB — pandas → polars 1.x bug-fix reconciliation (`0.36-live` → `main`)

- Foundation: new `cleaned_text` column normalizes ESPN play descriptions and is the single source of truth for downstream feature extraction.
- Behavioral: kneel-down semantics flag plus `scrimmage_play` exclusion.
- Yardage: structural rewrite of `__add_yardage_cols` (~150-line `np.select` chain → `pl.when().then()` chain), pass-yards regex tightened from `(?<=for)` to `(?<=[\s,]for)`, full punt rewrite, fair-catch fix.
- Helper-features: end-state edge cases, NCG 2025 GW play hardcode, `lead_half` end-of-half fix, OOB punts block, FG classification correction, `end.TimeSecsRem` shift direction flipped from lag to lead — which is what WPA inputs expected all along.
- WPA: `__process_wpa` end-of-game branch rewrite plus onside-kick rewrite, plus `penalty_assessed_on_kickoff` plumbing across `__setup_penalty_data` + `__process_epa` + `__process_wpa`.
- Player names: extraction migrated to `cleaned_text` everywhere.

### Infrastructure and tooling

- **Polars 1.x migration** across `cfb/cfb_pbp.py`, `nfl/nfl_pbp.py`, `mbb/mbb_pbp.py`, `nba/nba_pbp.py`, `nhl/nhl_pbp.py`, `wbb/wbb_pbp.py`, `wnba/wnba_pbp.py`. Roughly 165 API translation sites: `groupby` → `group_by`, `with_row_count` → `with_row_index`, `apply` → `map_elements` (with explicit `return_dtype`), struct list-arg → varargs, `shift_and_fill` → `shift`, `cumsum` → `cum_sum`, `str.strip` → `str.strip_chars`, `str.n_chars` → `str.len_chars`, outer-join → `full` + `coalesce`, `write_json` kwargs.
- Polars 1.x `is_in` same-datatype deprecation: switched to `.implode()` for the global-containment idiom.
- `pkg_resources.resource_filename` → `importlib.resources.files()` in `cfb_pbp.py` and `nfl_pbp.py` via small `_cfb_resource_filename` / `_nfl_resource_filename` helpers. Setuptools 81+ removed `pkg_resources`, which made the legacy import emit a `UserWarning` at module load and (eventually) break entirely.
- `download()` retry rewrite: iterative loop instead of recursion, defensive `response = None` init, re-raises the last captured exception when the retry budget is exhausted.
- `psutil` made optional in `decorators.py` (lazy import, previously an undeclared transitive dep that broke autodoc).
- `pytest.ini` filterwarnings for the transitive `sphinxcontrib-jsmath` legacy `nspkg.pth` `UserWarning` and the `pkg_resources` API `DeprecationWarning` surfacing from setuptools 81+.
- New tests under `tests/wbb/`, `tests/wnba/`, `tests/conftest.py` (with the `@skip_if_no_live` decorator gated by `SDV_PY_LIVE_TESTS=1`), and `tests/README.md` capturing the test conventions. NFL test files renamed to drop legacy-phase-jargon filenames in favor of descriptive names (`test_nfl_loaders_parity_loaders.py`, `_unified.py`, `_aliases.py`).

### Bug fixes

- `test_havoc_rate` corrected for both `cfb` and `nfl`: `def_int` field name fix, bounded `<=` assertion, `def_box.sort()` for deterministic group_by emit order, and `turnover_box` now produces a cli warning instead of silently padding an empty dict.
- `yds_punted` duplicate definition removed.
- `drive.id` NCG 2025 GW play hardcode.
- `is_in(col)` → `is_in(col.implode())` for global containment, applied across `cfb_pbp` and `nfl_pbp`.
- Pickcenter regression test added for both CFB and NFL: a 2024+ game must NOT silently fall back to the `(2.5, 55.5, True, False)` defaults; a pre-2024 game with populated legacy `pickcenter` must continue to use that legacy path.
- NFL `__helper_nfl_pbp_features` defensive cast for the case where ESPN returns `overUnder` as a Python float (no `.astype()`); same shape fix as the cfb_pbp version.

### Deprecations

- Four NFL loader families now consolidate per-type variants into a single unified function: `load_nfl_nextgen_stats(stat_type=...)` and `load_nfl_pfr_advstats(stat_type=, summary_level=)`. The per-type names continue to work but emit a `DeprecationWarning` pointing at the unified function. No removal yet.

## 0.0.40 Release: December 6, 2025

- Minor changes to mbb_calendar and wbb_calendar functions to include all games, even when top 25 teams are not competing

## 0.0.38-39 Release: August 28, 2023

- Minor changes to cfb_pbp functions to improve WP calculation and player parsing.

## 0.0.36-37 Release: July 9, 2023

- Switched most under the hood dataframe operations to use the python `polars` library and many functions now have a parameter `return_as_pandas` which defaults to `False` but can be set to `True` to return a pandas dataframe instead of a polars dataframe. This is a **breaking change.**
- Added `**kwargs` which pass arguments to the `dl_utils.download()` function, including `headers`, `proxy`, `timeout` (default 30s), `num_retries` (default = 15), `logger` (default = None)
- Function `espn_cfb_game_rosters()` added.
- Function `espn_nba_game_rosters()` added.
- Function `espn_nfl_game_rosters()` added.
- Function `espn_nhl_game_rosters()` added.
- Function `espn_wbb_game_rosters()` added.
- Function `espn_wnba_game_rosters()` added.
- Function `load_cfb_betting_lines()` added (only 2006 through 2019).

## 0.0.34-35 Release: May 7-9, 2023

- Reconfigured some imports
- Improved compliance with pandas upgrades
- Updated loader locations to use sportsdataverse-data releases and nflverse releases
- Flattened the returned results somewhat for "sportsdataverse.cfb.espn_cfb_schedule()" functions, but also now including some nested data frame and list columns

## 0.0.18 Release: July 25, 2022

- Added ondays parameter to ESPN calendar functions
- Renamed "sportsdataverse.cfb.cfb_teams()" to "sportsdataverse.cfb.espn_cfb_teams()" to avoid an edge case issue when running the function.

## 0.0.17 Release: July 9, 2022

- Added MLBAM API functionality to the sportsdataverse-py package. For more information on how to use these new functions, refer to the docs.
- Fixed a bug where the "sportsdataverse.nfl.load_nfl_schedule()" function would cause a 404 error when run.
- For functions where multiple files are loaded in, progress bars have been added to indicate how far along the sportsdataverse-py package is in completing its task(s).
- Renamed "sportsdataverse.cfb.cfb_teams()" to "sportsdataverse.cfb.get_cfb_teams()" to avoid an edge case issue when running the function.

## 0.0.15 Release: May 8, 2022

- Refactor schedule and teams functions for all existing leagues.
- Created more robust home/away mappings to simplify assignment.

## 0.0.14 Release: March 16, 2022

- Refactor schedule and teams functions for all existing leagues.
- Created more robust home/away mappings to simplify assignment.

## 0.0.12 Release: February 24, 2022

- Minor refactor to all the pbp functions, attempting to normalize behavior.
- Adding raw parameter to same functions to return object as it comes in without any transformation
- Adding some config file corrections.

## 0.0.5 Release: October 20, 2021

- f'in round
- findin' out
