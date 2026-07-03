<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Unreleased](#unreleased)
  - [NBA — RAPM variants (WP2)](#nba--rapm-variants-wp2)
  - [NBA — through-date ratings panel, WAR, and single-game BPM (WP4)](#nba--through-date-ratings-panel-war-and-single-game-bpm-wp4)
  - [NBA — v3-to-v2 play-by-play adapter (`nba_v3_to_v2_pbp`)](#nba--v3-to-v2-play-by-play-adapter-nba_v3_to_v2_pbp)
  - [NBA / WNBA — stats.nba.com / stats.wnba.com flat-API family (`nba_stats` / `wnba_stats`)](#nba--wnba--statsnbacom--statswnbacom-flat-api-family-nba_stats--wnba_stats)
  - [NBA — possession event-detail columns, per-shooter shooting frame, `game_date`](#nba--possession-event-detail-columns-per-shooter-shooting-frame-game_date)
  - [NBA — faithful possession boundaries (pbpstats parity)](#nba--faithful-possession-boundaries-pbpstats-parity)
- [0.0.71 Release: June 24, 2026](#0071-release-june-24-2026)
  - [CFB — opponent-adjusted EPA (`cfb_adjusted_epa`): season + walk-forward](#cfb--opponent-adjusted-epa-cfb_adjusted_epa-season--walk-forward)
  - [NFL — era-aware decision models + both-path (ESPN + nflverse) model parity](#nfl--era-aware-decision-models--both-path-espn--nflverse-model-parity)
- [0.0.70 Release: June 24, 2026](#0070-release-june-24-2026)
  - [CFB — `qbr` / `fg` / `wp_spread` models refreshed on the consensus-odds full-corpus reprocess](#cfb--qbr--fg--wp_spread-models-refreshed-on-the-consensus-odds-full-corpus-reprocess)
- [0.0.69 Release: June 23, 2026](#0069-release-june-23-2026)
  - [CFB — roster-backed `{type}_player_id` + player-name cleanup fixes](#cfb--roster-backed-type_player_id--player-name-cleanup-fixes)
- [0.0.68 Release: June 23, 2026](#0068-release-june-23-2026)
  - [CFB — completion-probability (`cp`/`cpoe`) + expected-pass (`xpass`/`pass_oe`) surface](#cfb--completion-probability-cpcpoe--expected-pass-xpasspass_oe-surface)
  - [CFB — spread-free (naive) win-probability surface (`wp_*_naive`)](#cfb--spread-free-naive-win-probability-surface-wp__naive)
  - [CFB — QBR model retrained on the full 2004–2025 history](#cfb--qbr-model-retrained-on-the-full-20042025-history)
  - [CFB — fourth-down decision surface (`get_4th_down_probs`, cfb4th port)](#cfb--fourth-down-decision-surface-get_4th_down_probs-cfb4th-port)
  - [CFB — two-point-conversion decision surface (`get_2pt_probs`, cfb4th port)](#cfb--two-point-conversion-decision-surface-get_2pt_probs-cfb4th-port)
  - [CFB — rule-era QBR / FG / fourth-down models + `spread_time` sign fix](#cfb--rule-era-qbr--fg--fourth-down-models--spread_time-sign-fix)
  - [CFB — pre-2014 play-text player-name extraction](#cfb--pre-2014-play-text-player-name-extraction)
  - [NFL — expected pass (`xpass` / `pass_oe`) + nfl4th fourth-down decision surface](#nfl--expected-pass-xpass--pass_oe--nfl4th-fourth-down-decision-surface)
  - [NFL — self-trained XGBoost field-goal model in the fourth-down surface](#nfl--self-trained-xgboost-field-goal-model-in-the-fourth-down-surface)
  - [NFL — `load_nfl_espn_qbr` (ESPN QBR loader, nflreadpy parity)](#nfl--load_nfl_espn_qbr-espn-qbr-loader-nflreadpy-parity)
  - [NFL — bundled self-derived xpass model (offline, no first-use download)](#nfl--bundled-self-derived-xpass-model-offline-no-first-use-download)
- [0.0.67 Release: June 17, 2026](#0067-release-june-17-2026)
  - [Documentation — return-table column descriptions filled (~3,061 columns)](#documentation--return-table-column-descriptions-filled-3061-columns)
  - [Documentation — doctest-prompt cleanup, native returns-tables, new tutorials](#documentation--doctest-prompt-cleanup-native-returns-tables-new-tutorials)
  - [NFL — PBP ETL ↔ nflfastR alignment + faithful model artifacts](#nfl--pbp-etl-%E2%86%94-nflfastr-alignment--faithful-model-artifacts)
  - [CFB — EP + WP models retrained on the full 2004–2025 history](#cfb--ep--wp-models-retrained-on-the-full-20042025-history)
- [0.0.66 Release: June 17, 2026](#0066-release-june-17-2026)
  - [CFB — `cfb_pbp` sparse-game `ColumnNotFoundError` guard (`end.team.id` et al.)](#cfb--cfb_pbp-sparse-game-columnnotfounderror-guard-endteamid-et-al)
- [0.0.65 Release: June 17, 2026](#0065-release-june-17-2026)
  - [Namespace — minor/alias leagues nested under sport-group packages](#namespace--minoralias-leagues-nested-under-sport-group-packages)
  - [All sports — `espn_*_game_rosters` vectorized logo extraction](#all-sports--espn__game_rosters-vectorized-logo-extraction)
  - [MLB — `mlb_api_*` renamed to `mlb_*`](#mlb--mlb_api_-renamed-to-mlb_)
- [0.0.64 Release: June 17, 2026](#0064-release-june-17-2026)
  - [MLB — comprehensive Baseball Savant / Statcast surface (`mlb_statcast_*`, 43 endpoints)](#mlb--comprehensive-baseball-savant--statcast-surface-mlb_statcast_-43-endpoints)
  - [Documentation — `nfl_api` (NFL.com Shield) returns-schema tables](#documentation--nfl_api-nflcom-shield-returns-schema-tables)
- [0.0.63 Release: June 16, 2026](#0063-release-june-16-2026)
  - [All sports — `espn_*_game_rosters` diagonal per-team concat (fixes silent roster loss)](#all-sports--espn__game_rosters-diagonal-per-team-concat-fixes-silent-roster-loss)
  - [HTTP — `download()` no longer retries a definitive 404](#http--download-no-longer-retries-a-definitive-404)
- [0.0.62 Release: June 16, 2026](#0062-release-june-16-2026)
  - [All sports — `espn_*_game_rosters` robust to long-tail ESPN payloads](#all-sports--espn__game_rosters-robust-to-long-tail-espn-payloads)
- [0.0.61 Release: June 16, 2026](#0061-release-june-16-2026)
  - [CFB — `espn_cfb_game_rosters` robust to long-tail ESPN payloads](#cfb--espn_cfb_game_rosters-robust-to-long-tail-espn-payloads)
- [0.0.60 Release: June 15, 2026](#0060-release-june-15-2026)
  - [NFL — expected points, win probability, completion probability (CP/CPOE), and expected YAC (XYAC) models](#nfl--expected-points-win-probability-completion-probability-cpcpoe-and-expected-yac-xyac-models)
  - [CFB — `espn_cfb_schedule` guards null-competitor placeholder events](#cfb--espn_cfb_schedule-guards-null-competitor-placeholder-events)
- [0.0.59 Release: June 13, 2026](#0059-release-june-13-2026)
  - [CFB — cross-source crosswalk loaders (`load_cfb_*_crosswalk`)](#cfb--cross-source-crosswalk-loaders-load_cfb__crosswalk)
  - [ESPN — NCAA men's & women's college hockey (`espn_mch_*`, `espn_wch_*`)](#espn--ncaa-mens--womens-college-hockey-espn_mch_-espn_wch_)
  - [ESPN — NCAA college baseball + softball (`espn_college_baseball_*`, `espn_college_softball_*`)](#espn--ncaa-college-baseball--softball-espn_college_baseball_-espn_college_softball_)
  - [ESPN — UFL, XFL, and CFL (`espn_ufl_*`, `espn_xfl_*`, `espn_cfl_*`)](#espn--ufl-xfl-and-cfl-espn_ufl_-espn_xfl_-espn_cfl_)
  - [ESPN — soccer/cricket param families + soccer headline aliases (`espn_soccer_*(league=)`, `espn_cricket_*(league=)`, `espn_epl_*`, `espn_ucl_*`, `espn_mls_*`, ...)](#espn--soccercricket-param-families--soccer-headline-aliases-espn_soccer_league-espn_cricket_league-espn_epl_-espn_ucl_-espn_mls_-)
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

### NBA — RAPM variants (WP2)

- feat(nba): RAPM variants (`nba_rapm_variants`) — luck-adjusted (`nba_la_rapm`),
  four-factor (`nba_four_factor_rapm`), and time-decay (`nba_decay_rapm`) RAPM, all
  reusing the plain-RAPM design matrix; concurrent-validity vs the Ryan Davis oracle
  CSVs gated on `SDV_PY_NBA_ORACLE_DIR`.

### NBA — through-date ratings panel, WAR, and single-game BPM (WP4)

- feat(nba): through-date ratings panel (`nba_ratings_panel` + `ratings_as_of`
  primitive, leakage-free by construction — works with any harness model),
  WAR layer (`nba_war` + `calibrate_pts_per_win`/`calibrate_replacement_level`
  calibration helpers), and `nba_bpm(granularity="game")` single-game BPM 2.0.

### NBA — v3-to-v2 play-by-play adapter (`nba_v3_to_v2_pbp`)

New `sportsdataverse/nba/nba_v3_v2_adapter.py` ports hoopR's `.v3_to_v2_format()` to Python:
`nba_v3_to_v2_pbp(pbp_v3, box_v3, *, return_as_pandas=False)` turns a `playbyplayv3` payload +
`boxscoretraditionalv3` boxscore into the full 61-column v2-schema frame (NBA retired `playbyplayv2`,
which now returns 0 rows for every season — this restores v2-dataset compatibility from the live v3 feed).

- **Recovers the secondary players v3 drops**: assist via the `(Name N AST)` description parenthetical,
  block/steal via the standalone `actionType == ""` rows (the blocker/stealer ships as `personId`,
  associated to the shot/turnover at the same period+clock), sub-in via `SUB: X FOR Y`, and jump-ball
  via `vs. / Tip to` — each resolved through a 4-tier roster name-match (family → name_i → "F. Family" →
  fuzzy). Validated 1-to-1 against the cdn live feed's structured `assistPersonId`/`blockPersonId`/
  `stealPersonId` fields: **100% agreement on all three committed fixture games**. Documented gap: the
  foul-drawn player is unrecoverable from v3 (fouls carry null `player2`/`player3`).
- **v2 schema faithful to hoopR** — event/action-type codes (EVENTMSGTYPE/EVENTMSGACTIONTYPE),
  home/neutral/visitor description split by `location`, forward-filled `score`/`score_margin`/
  `team_leading`, person types, time columns from the ISO clock, string-typed ids (leading zeros
  preserved), plus the v3 passthrough columns. One deliberate divergence: `player2`/`player3` are
  enriched **by id** from the extraction rather than hoopR's name re-resolution (which can mismatch on
  family-name collisions).
- **pbpstats interop**: a `stats_nba` feed shim (`resultSets` envelope) lets the adapted frame drive the
  [`pbpstats`](https://github.com/dblackrun/pbpstats) library's v2 provider. A gated round-trip test
  feeds our v3-derived output through pbpstats-`stats_nba` and matches pbpstats' own `live` provider on
  the same games (possession counts within 0–2, period starters exact 8/8). The round-trip surfaced two
  fixes: `PLAYER1_TEAM_ID` serializes as null (not 0) on team-rebound rows, and the `"Transition Take"`
  foul subtype (EVENTMSGACTIONTYPE 31, added with the 2022-23 transition take foul rule) joined the foul
  table. Opt in locally with `SDV_PBPSTATS_ROOT=<path to a pbpstats checkout>`.
- Six cdn oracle fixtures committed under `tests/fixtures/nba_engine/{gid}/cdn_{playbyplay,boxscore}.json`
  (provenance documented in the fixtures README); exported as `sportsdataverse.nba.nba_v3_to_v2_pbp`.

### NBA / WNBA — stats.nba.com / stats.wnba.com flat-API family (`nba_stats` / `wnba_stats`)

Two new codegen-generated flat-API stems wrap the official stats API surface:

- **`nba_stats`** (`sportsdataverse/nba/nba_stats.py`) — **112 wrappers** targeting `stats.nba.com`. League routing is a single `league_id` parameter on each endpoint: `"00"` → NBA, `"20"` → G-League, `"15"` → Summer League. Named `nba_stats_<slug>` (e.g. `nba_stats_leaguedashplayerstats`, `nba_stats_playercareerstats`, `nba_stats_boxscoreplayertrackv3`).
- **`wnba_stats`** (`sportsdataverse/wnba/wnba_stats.py`) — **95 wrappers** targeting `stats.wnba.com` (WNBA `LeagueID=10`), named `wnba_stats_<slug>`. Implemented as a thin shim re-exporting the NBA-stats runtime with the WNBA host.
- **Codegen surface = capture-confirmed live, non-deprecated endpoints only.** The wrapper count is driven by a live capture sweep (committed under `sdv-internal-refs/nba/`): endpoints that an `nba_api`/`hoopR`/`wehoop` source marks deprecated (`lifecycle::deprecate_*`, runtime warnings, or release-note retirements — 26 endpoints) and endpoints with no capture confirming a populated table (`untested`/`barren`/`dead` for that league) are **excluded**. The full active/dying/barren/dead matrix lives in `sdv-internal-refs/nba/ENDPOINT_HEALTH.md`.
- **One generic parser** `parse_nba_stats_result_sets(raw, result_set=None, *, return_as_pandas=False)` handles the uniform `{resultSets: [{name, headers, rowSet}]}` envelope. Returns a single `polars.DataFrame` when a `result_set` name is given or the payload has one set; returns `dict[str, DataFrame]` for multi-set payloads (e.g. `playercareerstats`). Empty / malformed payloads return a zero-row frame; columns are snake-cased via `dl_utils.underscore`. It also handles the two non-uniform shapes in this family: the shot-location endpoints (`leaguedash{player,team}shotlocations`) whose `resultSets` is a single dict with 2-level grouped headers (flattened to composite columns like `less_than_5_ft_fgm`), and `scoreboardv3` whose data lives under `scoreboard.games` (one row per game, home/away team objects inlined). `parse_wnba_stats_result_sets` is a re-export alias.
- **Browser-TLS runtime:** `stats.nba.com` TLS/JA3-fingerprint-blocks plain `requests` (silent timeout, not an IP block). The runtime `_get` uses **`curl_cffi` with `impersonate="chrome"`**. `curl_cffi` is a **lazy optional import** shipped under the `tests` and `all` extras — not a hard runtime dep. A clear `ImportError` guides users to `pip install curl_cffi` (or `pip install sportsdataverse[all]`). The HTTP transport is injectable so wrappers and tests can run fully offline.
- Wrappers default to `return_parsed=True` (tidy polars DataFrame). Pass `return_parsed=False` for the raw `Dict` or `return_as_pandas=True` for pandas. There is no user-facing `headers=` param — the TLS impersonation is handled inside the runtime, not via a user token.
- Generated from the enriched canonical catalog (`tools/codegen/gen_nba_stats.py`) and registered in `FLAT_APIS` in `tools/codegen/generate.py`. Param `default`/`example` values are mined from the hoopR/wehoop roxygen signatures + `@examples`. Returns-table descriptions are authored for the pilot slugs and back-filled by column name from the SDV R-package docs (`_r_col_desc`); the remaining un-authored `native/nba_stats` + `native/wnba_stats` columns are a tracked follow-up exempted from the coverage ratchet via `extract_residual_columns._DEFERRED_BUCKETS` (surfaced by `deferred_columns()`).

### NBA — possession event-detail columns, per-shooter shooting frame, `game_date`

- feat(nba): possession event-detail columns (`fg2a/fg2m/fg3a/fg3m/fta/ftm/oreb/tov`),
  per-shooter `build_possession_shooting` companion frame, and `game_date` on
  `compile_nba_season` output (possession cache `PIPELINE_VERSION` 1 -> 2).

### NBA — faithful possession boundaries (pbpstats parity)

- feat(nba): `_build_possession_groups` rewritten to pbpstats `stats_nba`
  `is_possession_ending_event` semantics (and-1 + FT-trip exceptions, real-rebound
  and no-turnover filtering, jump-ball logic); technical FTs are inline again with
  team-filtered event detail (per-possession points identity preserved exactly).
- feat(nba): possessions gain `dreb`, `number_in_period`, `possession_start_type`
  (coarse vocabulary), `count_as_possession`; shooting frame gains `team_id`
  (possession cache `PIPELINE_VERSION` 2 -> 3).
- test(nba): pbpstats-live oracle gate — like-for-like possession counts +
  boundary-by-boundary diff on the committed cdn fixtures (`SDV_PBPSTATS_ROOT`).

## 0.0.71 Release: June 24, 2026

### CFB — opponent-adjusted EPA (`cfb_adjusted_epa`): season + walk-forward

`sportsdataverse.cfb.cfb_adjusted_epa()` and `cfb_adjusted_epa_by_game()` add a reusable ridge / RAPM-style opponent-adjustment primitive — separating a team's per-play EPA from its schedule with a ridge regression on offense/defense team indicators (plus home-field), fit over the competitive (`0.1 ≤ wp_before ≤ 0.9`) pass and rush plays. The season function returns one row per team (adjusted off / def / net EPA + ranks); the **walk-forward** function returns one row per team-game and is point-in-time — each week is adjusted using opponent strengths fit only on *prior* weeks, so the values are leak-free and valid as in-season power-rating or model inputs (week 1 has no prior, so its adjustments are null; not-yet-seen opponents fall back to the league baseline, the intended early-season shrinkage). This is an in-sample per-season estimator lifted out of the cfb-data `team_summaries` builder — not a bundled `.ubj` artifact. `scikit-learn` is now a runtime dependency.

### NFL — era-aware decision models + both-path (ESPN + nflverse) model parity

Ships the era-aware NFL model suite and brings both PBP construction paths to model parity. Rule-era one-hots (`era0..era4`, cuts 2001/2005/2013/2017) are added to the `xpass` / fourth-down / `fg` models so the curves are era-aware across all of 1999–2025 (fourth-down 14-feature, fg 7-feature, xpass 19-feature), and the bundled `nfl/models/*` are refreshed to the 1999–2025 retrain (two-point on 2010–2025).

Both builders now produce the same modeled columns: the **ESPN path** (`NFLPlayProcess`) gains `qb_epa`, `wp` / `vegas_wp` (+ `def_wp` / `home_wp` / `away_wp`), and `xpass` / `pass_oe`, wired into `run_processing_pipeline` in nflfastR order; the **nflverse path** (`enrich_nfl_pbp`) gains the per-play QBR EPA components. The fourth-down decision surface is **default-on in both builders**, scored on its play-type subset and merged back by play id, with each model applied on its correct play-type shape (xpass on scrimmage dropbacks, cp/xyac on pass + air-yards, fourth-down on `down == 4`). xYAC remains the documented null stub on the ESPN path (no `air_epa`). A latent bug the era refresh introduced is fixed: `calculate_xpass` now backfills the `era0`/`era1` features `_make_cp_mutations` did not build. Pairs with the nfl-data 1999–2025 retrain that produced the artifacts.

## 0.0.70 Release: June 24, 2026

### CFB — `qbr` / `fg` / `wp_spread` models refreshed on the consensus-odds full-corpus reprocess

The bundled CFB `qbr_model`, `fg_model`, and `wp_spread` XGBoost artifacts are retrained on the full 2004–2025 play-by-play corpus after it was re-reprocessed with two upgraded modeling inputs: the **`cfb_line_odds` multi-book consensus** pregame spread/total (replacing ESPN's single pickcenter as the EPA/WPA odds source) and **roster-backed pre-2014 player IDs**. Feature contracts are byte-identical to the shipped models (`qbr` 10-feature incl. `era0–3`, `fg` 5-feature, `wp_spread` 13-feature), so this is a drop-in artifact refresh — no model-application changes.

Leave-one-season-out CV over all 22 seasons confirms the gains: **`qbr` RMSE 17.60 → 17.29** (r² 0.598 → 0.612), **`fg` logloss 0.5265 → 0.5247**, and **`wp_spread` baseline logloss 0.3616 → 0.3486** — the win-probability model improves most, since the consensus odds sharpen the `spread_time` feature directly (the signal the rule-era one-hot dummies previously had to recover). The `fourth_down` model is intentionally left unchanged: on the refreshed corpus its era variant no longer beats the consensus-odds baseline, so it was not promoted.

## 0.0.69 Release: June 23, 2026

### CFB — roster-backed `{type}_player_id` + player-name cleanup fixes

`CFBPlayProcess` now emits a `{type}_player_id` for every extracted `{type}_player_name`, resolved **team-aware** against the game roster: each player type maps to the team that fielded it (offense `pos_team` / defense `def_pos_team` / special-teams `kicking_team` / `return_team` / recovery), so identical names on opposing rosters don't collide; a globally-unique name is the fallback. Ids resolve for **all years** — pre-2014 (no structured `participants[]` array) via the roster, 2014+ from the clean participant names.

- **New `CFBPlayProcess(game_roster=, participants=)` constructor params** let offline rebuilds pass the stored roster + participants — fetch-free, and keeping 2014+ clean names when `join_participants` is off. `__join_participants` now accepts a caller-supplied participant frame / `{"data": [...]}` / row list instead of always fetching.
- **Player-name cleanup fixes the roster-match exposed:** the receiver state-abbrev strip (`ST`/`GA`/`FL`/…, with the leading space) is anchored to a *trailing standalone token* so it can't corrupt real names (it used to eat the " St" inside "Stewart" → "ewart"); a garbage guard nulls obvious play-text artifacts ("bea loss of") before the id-join.

## 0.0.68 Release: June 23, 2026

### CFB — completion-probability (`cp`/`cpoe`) + expected-pass (`xpass`/`pass_oe`) surface

`CFBPlayProcess` now emits per-play completion-probability and expected-pass columns, mirroring nflfastR's `cp`/`cpoe` and `xpass`/`pass_oe`.

- **Two new bundled models** — `cfb/models/cfb_cp_model.ubj` (8-feat `binary:logistic`: `down`, `distance`, `yards_to_goal`, `score_diff`, `seconds_remaining`, `is_home`, `period`, `passing_down`) and `cfb/models/xpass_model.ubj` (7-feat `binary:logistic`: `down`, `distance`, `yards_to_goal`, `pos_score_diff`, `TimeSecsRem`, `era`, `period`). Both are ~400 KB and ship via the existing `cfb/models/*` package-data glob (no download-on-demand).
- **New per-play columns** — `cp` = P(complete) on pass plays with `cpoe = 100 * (completion - cp)` (percentage-point scale, null on non-pass plays); `xpass` = P(pass) on scrimmage rush-or-pass plays with `pass_oe = 100 * (pass - xpass)` (null elsewhere). Added as two pipe steps (`__process_cpoe` / `__process_xpass`) after the EPA/WPA steps in `run_processing_pipeline()`; each degrades to null columns rather than raising when a source column is absent.

### CFB — spread-free (naive) win-probability surface (`wp_*_naive`)

`CFBPlayProcess` now emits a second, **spread-free** win-probability surface alongside the existing spread WP, completing the play-level model handoff begun in 0.0.67 (which retrained EP + spread WP on the full 2004–2025 history).

- **New bundled model `cfb/models/wp_naive.ubj`** — the faithful cfbscrapR "naive" recipe (12-feat = `wp_final_names` minus `spread_time`, `binary:logistic`, 65 rounds), retrained on the same full-history corpus (2,219,607 plays, 2004–2025) as the spread model. Ships via the existing `cfb/models/*` package-data glob.
- **New per-play columns** `wp_before_naive` / `wp_after_naive` / `wpa_naive` (plus `def_`/`home_`/`away_` analogues), mirroring the spread columns under a `_naive` suffix. The naive surface answers "given only game state, who wins?" while the spread surface bakes in the pregame line; the two correlate ~0.90, diverging most early-game where the market prior carries the most information.
- **Refactor (no behavior change to the spread surface):** the win-probability prediction + game-logic derivation in `__process_wpa` was factored into shared `_wp_predict` / `_apply_wp_derivation` helpers routed once per model. The spread (un-suffixed) output is **byte-identical** to the prior release — verified against a captured per-play baseline.

### CFB — QBR model retrained on the full 2004–2025 history

The bundled `cfb/models/qbr_model.ubj` (6-feat XGBoost: `qbr_epa` / `sack_epa` / `pass_epa` / `rush_epa` / `pen_epa` / `spread`) was retrained on the full-history corpus, replacing the legacy 2020-lineage model.

- **Decisively better against the ESPN raw-QBR reference.** On a 2021–2025 holdout (out-of-sample for the legacy model): RMSE 23.2 → **16.1** (−31%), MAE 18.7 → **12.5**, R² 0.29 → **0.66**, correlation 0.69 → **0.82**. The retrained model's honest leave-one-season-out metrics (RMSE 17.9, R² 0.585) confirm the gains are real generalization, not in-sample fit.
- **Drop-in swap** — same 6-feature contract, ships via the existing `cfb/models/*` package-data glob; no caller changes.

### CFB — fourth-down decision surface (`get_4th_down_probs`, cfb4th port)

A full college-football fourth-down decision surface, a faithful Python port of [cfb4th](https://github.com/sportsdataverse/cfb4th)'s `add_4th_probs()`, against this package's bundled EP / WP-spread boosters.

- **`sportsdataverse.cfb.get_4th_down_probs(pbp_df)`** scores all three options on a frame of fourth-down situations and adds: `go_wp` / `first_down_prob` / `wp_succeed` / `wp_fail` (go), `punt_wp` (punt), `fg_make_prob` / `make_fg_wp` / `miss_fg_wp` / `fg_wp` (field goal), a `fourth_down_recommendation` ∈ {`go`, `punt`, `field_goal`} (max-WP choice), per-option `*_wp_diff`, and `go_boost` (cfb4th's headline `100·(go_wp − max(fg_wp, punt_wp))`).
- **`CFBPlayProcess.add_fourth_down_probs()`** applies the same to a processed game's fourth-down rows after `run_processing_pipeline()`.
- **New models:** `fg_model.ubj` (CFB-native field-goal make-probability by distance, trained on 42.6k attempts) and `punt_distribution.parquet` (punt end-yardline distribution) are bundled under `cfb/models/`; the 6-feat / 76-class `fd_model.ubj` (yards-gained, with the ordinal CFB rule-era factor) is **download-on-demand** (~16 MB, fetched from the `espn_cfb_model_artifacts` release and cached under `~/.cache/sportsdataverse/cfb_models/`, mirroring the NFL xYAC pattern; override with `SDV_PY_CFB_MODEL_DIR`). The go path reuses the reviewed cfb-data decision-layer machinery; punt/FG mirror cfb4th's possession-flip + end-game scoring.

### CFB — two-point-conversion decision surface (`get_2pt_probs`, cfb4th port)

The extra-point vs go-for-2 decision, a faithful Python port of [cfb4th](https://github.com/sportsdataverse/cfb4th)'s `get_2pt_wp()`, against this package's bundled EP / WP-spread boosters and a new bundled CFB two-point model.

- **`sportsdataverse.cfb.get_2pt_probs(pbp_df)`** treats each row as "the scoring team just made a touchdown; decide". For each of the three point outcomes (`0` / `1` / `2`) it subtracts the points, flips to the opponent's ensuing kickoff-return drive (1st-&-10 at the 25, `yards_to_goal = 75`), scores EP → WP, and flips WP back to the scoring team. It adds `two_pt_wp` (= `prob_2pt·wp(2) + (1−prob_2pt)·wp(0)`), `xp_wp` (= `prob_xp·wp(1) + (1−prob_xp)·wp(0)`), `prob_2pt`, a `two_pt_recommendation` ∈ {`go_for_2`, `kick_xp`} (go for 2 iff `two_pt_wp > xp_wp`), and `two_pt_wp_diff` (= `two_pt_wp − xp_wp`, positive ⇒ go for 2). The ensuing-drive frame reuses the reviewed 4th-down state machinery (`_flip_team_state` + EP/WP scorers).
- **`CFBPlayProcess.add_2pt_probs()`** applies the same to a processed game's point-after / two-point-conversion rows (those with `pointAfterAttempt.text` present) after `run_processing_pipeline()`; every other row carries nulls.
- **New model:** `two_pt_model.ubj` (a `binary:logistic` 4-feature booster — `posteam_spread`, `posteam_total`, `pos_score_diff`, ordinal `era`) is bundled under `cfb/models/`. `prob_2pt` comes from this model (cfb4th hardcodes 0.45); `prob_xp` is the empirical CFB extra-point make rate `0.9851` (cfb4th derives XP from its FG GAM, but the empirical rate is more accurate for CFB).

### CFB — rule-era QBR / FG / fourth-down models + `spread_time` sign fix

The QBR, field-goal, and fourth-down (yards) models gain one-hot rule-era dummies (`era0..era3`, cuts 2006/2013/2020) where they improve out-of-fold, and the bundled boosters are swapped to the era-augmented versions.

- **QBR** — `qbr_vars` gains `era0..era3` (LOSO RMSE 17.9 → 17.4); `__process_qbr` injects the per-game era one-hot before prediction; bundled `qbr_model.ubj` swapped to the 10-feature era model.
- **Fourth-down** — `fd_model.ubj` switched to the 9-feature one-hot era model (first-down cal-MAE 0.0035 → 0.0027) and **bundled** in the package (was download-on-demand); `fg_model.ubj` swapped to the 5-feature era model.
- **WP-spread** — bundled `wp_spread.ubj` retrained on the odds-backfilled frame (the ~2,167 missing-spread games now carry real consensus spreads; LOSO logloss 0.362 → 0.352; same 13-feature contract, no inference change).
- **`spread_time` sign fix** — `_predict_wp` computed `spread_time = −pos_team_spread·exp(…)`, inverted vs the trained-on convention (favorites scored as underdogs in `get_go_wp`/`get_fg_wp`/`get_punt_wp`); corrected to `+pos_team_spread·exp(−4·elapsed_share)`.
- **Decision surfaces on by default** — `run_processing_pipeline(fourth_down_probs=True, two_pt_probs=True)` now appends the fourth-down and two-point decision columns to a processed game by default.

### CFB — pre-2014 play-text player-name extraction

`CFBPlayProcess` now recovers per-play player names for **2004–2013** games, where ESPN ships no structured per-play participants array (only `teamParticipants`). Two latent bugs in the play-text regex extraction were fixed: a multi-alternative `str.extract` group-index bug (the matched branch's name landed in a non-default capture group, returning null for ESPN "rush" / "Punt by" / "on-side" / "returned by" phrasings) and a `\d`-escaping bug (a literal backslash instead of a digit, which broke field-goal-kicker extraction). Pre-2014 games now populate rusher / passer / receiver / sack / fg-kicker / punter / returner / fumble player names (all null before); 2014+ output is unchanged (the structured-participants overwrite still wins).

### NFL — expected pass (`xpass` / `pass_oe`) + nfl4th fourth-down decision surface

`calculate_xpass` adds `xpass` (P(dropback)) and `pass_oe = 100·(pass − xpass)` to the enriched NFL PBP, plus a faithful Python port of [nfl4th](https://github.com/nflverse/nfl4th)'s fourth-down decision surface (`nfl/nfl_fourth_down.py`) scoring go / field-goal / punt win probability + a recommendation.

- **`calculate_xpass`** — the self-derived dropback booster; `xpass`/`pass_oe` mirror nflfastR's `add_xpass`.
- **nfl4th surface** — go / FG / punt WP via the download-on-demand `fd_model` / `wp_model` artifacts (cached on first use), mirroring nfl4th's `add_4th_probs`.

### NFL — self-trained XGBoost field-goal model in the fourth-down surface

`get_fg_wp` / `get_2pt_wp` switched from the mgcv-GAM prediction grid to a self-trained `binary:logistic` XGBoost FG model (`fg_model.ubj`, features `yardline_100` / `fg_roof` / `fg_era`) with the unchanged nfl4th long-kick clamps. Oracle parity (2022): `fg_wp` 0.9995, `go_wp` 0.9998, `punt_wp` 0.9996.

### NFL — `load_nfl_espn_qbr` (ESPN QBR loader, nflreadpy parity)

New `load_nfl_espn_qbr` (also aliased `load_espn_qbr`) — the last nflreadpy dataset without an sdv-py loader. `summary_type=` season|week, 2006+ floor, `source=` dual (nflverse `espn_data` release or the SDV-native `nfl_espn_qbr` release, 2006–2025), read-once-then-filter, with a 23-column returns-schema.

### NFL — bundled self-derived xpass model (offline, no first-use download)

`xpass_model.ubj` (the self-derived dropback booster, 1121 trees, 7.4 MB) moves from download-on-demand to bundled under `nfl/models/`, so `calculate_xpass` works offline. It is the **same** model the release ships — xpass output is unchanged; removed from `_MODEL_URLS` (the bundled path wins in `_load_model`'s resolution order).

## 0.0.67 Release: June 17, 2026

### Documentation — return-table column descriptions filled (~3,061 columns)

Every generated reference page renders a `col_name | type | description` returns table; ~3,061 of those cells previously rendered **blank** because the column name had no entry in the R-package-mined dictionary that backfills descriptions at render time (sdv-py-/provider-specific columns: ESPN Site v2, MLB Stats API, NHL api-web / EDGE, nflverse Shield, HockeyTech, etc.). Those cells are now filled.

- **New hand-curated source** `tools/codegen/manual_column_descriptions.yaml`, keyed by the schema's `schema:` field (with a `_global` table-agnostic fallback), consumed at render time by `generate.py:_table_cell_desc`. Resolution order: captured-stored value → `manual[schema][col]` → `manual._global[col]` → R-dict mined fill → empty. Descriptions live **only** here (the `schemas/**.yaml` are clobbered blank on every capture), so they survive re-capture.
- **Coverage:** NFL (1,158 — nflverse / Next Gen Stats / Pro Football Reference / ESPN), MLB (599 — Stats API + ESPN), NHL (588 — api-web / EDGE / ESPN), CFB (177 — ESPN + cfbfastR), plus the ESPN cross-league game **summary** (sport-agnostic), NBA/WNBA/MBB/WBB, PWHL + CHL junior hockey (OHL/QMJHL/WHL/AHL), and the shared `standings`/`leaders`/`team_roster`/`news`/`team_schedule` schemas.
- **Regression guard:** `tools/codegen/extract_residual_columns.py` computes the render-blank residual; `tests/codegen/test_manual_descriptions.py` asserts it stays at **0** (a newly-captured undocumented column fails CI until authored), plus an orphan guard (no stale dict keys) and a filler-lint (rejects terse/generic descriptions). Every bucket was adversarially accuracy-reviewed; corrections included PFR `rec_br`, MLB `base_on_balls`, NHL EDGE goalie goal-differential / pbp assist totals, and the long-format `load_cfb_betting_lines` columns.

### Documentation — doctest-prompt cleanup, native returns-tables, new tutorials

- **No more raw `>>>` doctest prompts.** The generated ESPN-wrapper + loader docstring templates emitted `>>> call` under `Example:` (which `sphinx.ext.doctest` would try to verify); both emission sites now produce the napoleon `Quick start::` literal block, clearing ~3,559 generated hazards. The remaining ~55 hand-written prompts (NFL NGS / parsers, The Odds API, `find`/`discover`, etc.) were converted in source.
- **78 new native returns-tables.** Wired `returns_schema` for NHL api-web (9), stats-rest (10), records (37), EDGE (15), and MLB Stats API (8) endpoints that previously rendered no return table — captured from live fixtures; the 676 new columns are fully described. (24 endpoints were skipped: off-season EDGE top-10 leaderboards, retired record paths, and auth-gated MLB endpoints.)
- **`refresh_return_schemas` no longer writes 0-column per-league schemas** — an empty `columns: []` file shadowed and suppressed the generic `schemas/{name}.yaml` fallback, leaving some leagues with no table; it now skips them so the generic table renders.
- **Three new intro tutorials** under `examples/notebooks/` (rendered to `docs/docs/tutorials/`): **Soccer** (`espn_soccer_*(league=)` + headline aliases), **Cricket** (`espn_cricket_*` + the 8-section matchcard summary), and **Other ESPN leagues** (UFL/XFL/CFL, college baseball/softball, NCAA M/W hockey).

### NFL — PBP ETL ↔ nflfastR alignment + faithful model artifacts

- **`enrich_nfl_pbp()` lead-diff orchestrator** computes nflverse-native EP/EPA/WP/WPA/CP/xYAC on a real nflverse PBP frame, aligned to nflfastR; runs on live nflverse data.
- **Shared derivations** `calculate_epa()` / `calculate_wpa()` lifted into `sportsdataverse/nfl/ep_wp.py`; the NFL EP/WP constants + shared column contract centralized in `sportsdataverse/nfl/model_vars.py`.
- **Faithful NFL model artifacts** replace the byte-identical CFB 8-feature placeholders that previously shipped under `nfl/models/`: `ep_model.ubj` (18 features), `wp_spread.ubj` (12), `wp_naive.ubj` (11), `cp_model.ubj` (18) — resolving the long-standing `xgboost num_feature >= num_col (8 vs 18)` mismatch that left the NFL model path red.
- **New test coverage:** `tests/nfl/` gains enrich, enrich-derive, EPA, WPA, and column-contract suites.

### CFB — EP + WP models retrained on the full 2004–2025 history

- Canonical `cfb/models/ep_model.ubj` and `cfb/models/wp_spread.ubj` retrained on the complete cfbfastR-cfb-raw finals — **2,219,607 cleaned/labeled/weighted plays, seasons 2004–2025** — now that the raw backfill is complete. Shipped XGBoost recipes unchanged (EP `multi:softprob` 7-class/525 rounds; WP-spread `binary:logistic`/760 rounds).
- **Leave-one-season-out validated** (22 folds, out-of-fold): EP mlogloss 1.233 / accuracy 0.500 / EP-value calibration MAE 0.014 pts; WP logloss 0.362 / Brier 0.118 / AUC 0.916 / weighted-cal-error 0.0147. Drop-in safe (feature names/order match `cfb_pbp.ep_final_names`/`wp_final_names`). **QBR is intentionally unchanged** (LOSO R² 0.585 — remains the Dec-2020 canonical model).

## 0.0.66 Release: June 17, 2026

### CFB — `cfb_pbp` sparse-game `ColumnNotFoundError` guard (`end.team.id` et al.)

Sparse pre-2010 games (e.g. 2005 game `252440154`) crashed `CFBPlayProcess.run_processing_pipeline()` with `polars.exceptions.ColumnNotFoundError: unable to find column "end.team.id"`. The per-play `start.*`/`end.*`/`period.*`/`clock.*`/`type.*` columns are produced only by `pd.json_normalize` flattening the plays array, so when *no* play in a game carries a given nested object the column is never created — and the downstream `with_columns` chain dereferences it via `pl.col(...)` unconditionally, which raises at plan time before the existing `fill_null` / `when-otherwise` logic can substitute a value.

Added a column-materialization guard in `__helper_cfb_pbp_features` (after the early-return length checks, before the main play chain) that diffs the 15 unconditionally-referenced json_normalize-origin columns against the live frame and creates any missing one as a Null literal:

- String-typed source columns (`clock.displayValue`, `type.text`, `text`, `start.downDistanceText`) are created as `pl.lit(None, dtype=pl.String)` because the chain runs `.str.*` ops on them (an untyped Null column raises `SchemaError`).
- Numeric/bool columns stay untyped `pl.lit(None)` so their explicit downstream `.cast(...)` owns the final dtype.
- The guard is a **no-op for healthy games** — `with_columns` is skipped when nothing is missing, so output is byte-identical (verified: 5 control games reprocessed to identical 406-column frames and exact play counts). Resolves all 7 known-failing 2005 games.

## 0.0.65 Release: June 17, 2026

### Namespace — minor/alias leagues nested under sport-group packages

- refactor(namespace): nest minor/alias leagues under sport-group packages (`sportsdataverse.soccer.epl`, `.hockey.ahl`, `.football.ufl`, `.baseball.college_baseball`); the 8 majors + pwhl/soccer/cricket stay top-level. Legacy names (`sportsdataverse.epl`, `import sportsdataverse.ufl`) still resolve with a `DeprecationWarning`. NOTE: `discover.function_count()`/`list_functions()` keep flat-leaf keys (`function_count(league="ufl")` works); `import sportsdataverse` now eagerly loads the 12 soccer-alias submodules to support attribute access.

### All sports — `espn_*_game_rosters` vectorized logo extraction

Pre-2010 ESPN team payloads omit the `logos` key entirely, causing `helper_{sport}_team_items` to raise `polars.exceptions.ColumnNotFoundError: "logos" not found`. The row-by-row item-assignment fallback (`teams_df[row, "logo_href"] = ...`) also triggers `TypeError: the truth value of a Series is ambiguous` in polars 1.x because the row-index selector internally evaluates `Series.__bool__`.

Replaced the logos block in all seven `espn_*_game_rosters` modules (`cfb`, `mbb`, `nba`, `nfl`, `nhl`, `wbb`, `wnba`) with vectorized `with_columns`:

- `if "logos" in teams_df.columns:` guard handles pre-2010 payloads where the key is absent.
- `pl.col("logos").list.get(i).struct.field("href").fill_null("")` — expression-engine extraction, null-safe, no Python-level row iteration.
- `except Exception:` fallback to empty-string literals if the logos payload doesn't match the expected `List(Struct)` shape.

### MLB — `mlb_api_*` renamed to `mlb_*`

The 64 Stats API wrapper functions in `mlb_api.py` (generated via `tools/codegen/endpoints/mlb_api.yaml`) and the 15 hand-written functions in `mlb_api_extra.py` were renamed from `mlb_api_{short}` to `mlb_{short}` — parallel to the `statcast_*` → `mlb_statcast_*` rename in 0.0.64. The `_api_` infix was a disambiguation artifact from when multiple backends shared the module; it is now redundant. **No aliases — update call sites accordingly.**

## 0.0.64 Release: June 17, 2026

### MLB — comprehensive Baseball Savant / Statcast surface (`mlb_statcast_*`, 43 endpoints)

Expanded the Baseball Savant integration from a 12-endpoint representative slice to the full **~43-endpoint catalog** under the `mlb_statcast_<family>_<name>` naming (`search` / `leaderboard` / `gamefeed` / `player`), with **every endpoint parsed to a tidy frame by default** (`return_parsed=False` / `raw=True` for the raw payload).

- **39 codegen-generated wrappers** — 37 leaderboards (expected stats, sprint speed, bat tracking, pitch arsenals/movement/tempo, OAA, arm strength, catcher framing/blocking/throwing, baserunning, park factors, …) plus `mlb_statcast_gamefeed` (one row per pitch) and `mlb_statcast_schedule` (one row per game). Savant mixes CSV / JSON / HTML, so the family uses a content-type-aware getter (`dict` for JSON, `str` for CSV/HTML); the two HTML-embedded leaderboards (`fielding-run-value`, `statcast-park-factors`) are parsed from their embedded `data[]` blob.
- **Hand-written search** — `mlb_statcast_search` (+ `_minors`, `_wbc`) auto-chunks the 25,000-row Savant cap and translates friendly filters (`season`, `pitch_type`, `at_bat_result`, `batters_lookup`, …) to Savant's `hf*` params. `mlb_statcast_player` parses a player page's `serverVals` section (default `statcast`, ~260 metrics) to a tidy frame (`section=` for others, `raw=True` for HTML).
- **Returns-schemas** (`col_name | type | description`) for every frame-returning function, and `examples/notebooks/09_mlb_intro.ipynb` modernized to the new surface.
- The pre-0.0.64 `statcast_*` names were **renamed (no aliases)** to the `mlb_statcast_*` convention.

### Documentation — `nfl_api` (NFL.com Shield) returns-schema tables

Added live-captured `col_name | type | description` returns-schemas for all 11 `api.nfl.com` endpoints (`standings`, `rosters`, `teams_history`, `team`, `weeks`, `weeks_by_date`, `combine_profiles`, `draft_picks`, `injuries`, `game_summaries`, `weekly_game_details`), wired via `returns_schema:` into `nfl_api.yaml` and rendered into the reference docs — bringing `nfl_api` to parity with the other six native API families (mlb_api, nhl_*). Docs/codegen-metadata only; no runtime change.

## 0.0.63 Release: June 16, 2026

### All sports — `espn_*_game_rosters` diagonal per-team concat (fixes silent roster loss)

The per-team roster concat in `espn_wbb/wnba/nba/mbb/nfl/cfb_game_rosters` used `pl.concat(..., how="vertical")`, which hard-fails with `polars.exceptions.ShapeError` when a game's two teams ship different roster columns (e.g. one entry list has `jersey`, the other `didNotPlay`). The whole game then errored and was discarded as empty despite having roster data. Switched to `how="diagonal"` (union + null-fill), matching `nhl_game_rosters` and the `teams`/`athletes` concats in the same modules.

### HTTP — `download()` no longer retries a definitive 404

`sportsdataverse.dl_utils.download` retried a `NoESPNDataError` (ESPN 404 / `code:404` body) for the full `num_retries` budget — wasting ~51s of backoff and N requests per genuinely-absent resource, amplifying load against a rate-limited host. A 404 is definitive "no data", so it now fails fast (one attempt) instead of retrying. Connection/timeout/5xx errors still retry as before.

## 0.0.62 Release: June 16, 2026

### All sports — `espn_*_game_rosters` robust to long-tail ESPN payloads

Applies the two `espn_cfb_game_rosters` robustness fixes from 0.0.61 to every sibling rosters builder — `espn_wbb_game_rosters`, `espn_wnba_game_rosters`, `espn_nba_game_rosters`, `espn_mbb_game_rosters`, `espn_nhl_game_rosters`, and `espn_nfl_game_rosters` — which were templated from the same source and shared both bugs verbatim:

- **`statistics_href` strict-rename** of the competitors payload now renames only keys actually present, so older games that omit the team-level `statistics` `$ref` no longer raise `polars.exceptions.ColumnNotFoundError`.
- **Per-team roster 404** is now tolerated: a single team's missing `/roster` (`NoESPNDataError`) no longer fails the whole game; the other team's roster is recovered, and `NoESPNDataError` is raised only when every team is empty.

Adds parametrized offline regression tests across all six modules (`tests/test_sibling_game_rosters.py`).

## 0.0.61 Release: June 16, 2026

### CFB — `espn_cfb_game_rosters` robust to long-tail ESPN payloads

Surfaced by the 2004–2023 `cfbfastR-cfb-raw` backfill, two deterministic failures used to empty a game's rosters entirely (then get caught upstream and banked as empty "hollow" extras):

- **`statistics_href` strict-rename.** Older games (e.g. pre-2021) omit the team-level `statistics` `$ref` in the competitors payload, so `statistics_href` never exists and the unconditional `items.rename({..., "statistics_href": "team_statistics_href"})` raised `polars.exceptions.ColumnNotFoundError` for the whole game. The renamed column is unused downstream, so the rename now applies only to keys actually present.
- **Per-team roster 404.** A single team's `/roster` sub-endpoint can 404 (`NoESPNDataError`) — common for older games and FCS opponents — while the other team's roster exists. The per-team loop now skips a 404 team and recovers the other, raising `NoESPNDataError` only when *every* team is empty (genuinely no roster data).

Adds offline helper unit tests (`tests/cfb/test_cfb_game_rosters.py`, no network).

## 0.0.60 Release: June 15, 2026

### NFL — expected points, win probability, completion probability (CP/CPOE), and expected YAC (XYAC) models

`sportsdataverse.nfl.ep_wp` gains nflfastR-parity modeling functions — `calculate_expected_points`, `calculate_win_probability`, `calculate_completion_probability` (CP + CPOE), and `calculate_xyac` (four XYAC sub-models: mean/median/SD yardage + completion probability) — fed by ESPN-adapter feature builders and wired into `NFLPlayProcess`. Ships the bundled XGBoost `.ubj` model files.

### CFB — `espn_cfb_schedule` guards null-competitor placeholder events

ESPN's 2010 and 2014 college-football scoreboards include placeholder events with null `competitions`/`competitors`. `espn_cfb_schedule` now skips those events instead of raising `TypeError: 'NoneType' object is not subscriptable` and failing the entire season.

## 0.0.59 Release: June 13, 2026

### CFB — cross-source crosswalk loaders (`load_cfb_*_crosswalk`)

New 404-safe dataset loaders read pre-built CFB identity crosswalks from the `cfb_crosswalk` release tag on `sportsdataverse-data`, so callers can translate ids across providers without re-scraping every source. They cache the output of the live `cfb_teams_crosswalk` / `cfb_schedule_crosswalk` / `cfb_rosters_crosswalk` builders (ESPN × Fox × Yahoo, keyed on an aggressively-normalized team name; see `sportsdataverse.cfb.cfb_crosswalk`) — a full-season schedule build otherwise fans out hundreds of requests across three providers.

### ESPN — NCAA men's & women's college hockey (`espn_mch_*`, `espn_wch_*`)

- feat(espn): add NCAA men's & women's college hockey (espn_mch_*, espn_wch_*)

### ESPN — NCAA college baseball + softball (`espn_college_baseball_*`, `espn_college_softball_*`)

- feat(espn): add NCAA college baseball + softball (espn_college_baseball_*, espn_college_softball_*)

### ESPN — UFL, XFL, and CFL (`espn_ufl_*`, `espn_xfl_*`, `espn_cfl_*`)

- feat(espn): add UFL, XFL, and CFL (espn_ufl_*, espn_xfl_*, espn_cfl_*)

### ESPN — soccer/cricket param families + soccer headline aliases (`espn_soccer_*(league=)`, `espn_cricket_*(league=)`, `espn_epl_*`, `espn_ucl_*`, `espn_mls_*`, ...)

- feat(espn): add league-parameterized soccer + cricket families (espn_soccer_*(league=), espn_cricket_*(league=)) + soccer headline aliases (espn_epl_*, espn_ucl_*, espn_mls_*, ...)
- feat(soccer): full-parity soccer parsers — scoreboard→matches, standings→league table (group column), summary→11-section dispatcher (header/lineups/key_events/team_stats/commentary/leaders/standings/head_to_head/last_five/game_info/shootout), teams, roster — routed via per-sport codegen overrides; feat(cricket): cricket parsers — scoreboard, standings, summary→8-section matchcard dispatcher (batting/bowling/partnerships)

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
