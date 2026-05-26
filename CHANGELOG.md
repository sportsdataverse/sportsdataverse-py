<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [0.0.51 (unreleased)](#0051-unreleased)
  - [New: MLB module (greenfield)](#new-mlb-module-greenfield)
  - [New: NHL — `api-web.nhle.com` migration + EDGE / Stats REST / Records](#new-nhl---api-webnhlecom-migration--edge--stats-rest--records)
  - [New: ESPN cross-league port](#new-espn-cross-league-port)
  - [New: NCAA bracketology](#new-ncaa-bracketology)
  - [New: `_common_espn_parsers.py` (polars / pandas parser layer)](#new-_common_espn_parserspy-polars--pandas-parser-layer)
  - [New: `return_parsed=True` dispatch shim](#new-return_parsedtrue-dispatch-shim)
  - [New: `nhl_edge_parsers.py`](#new-nhl_edge_parserspy)
  - [New: Site v2 summary dispatcher (20 sub-parsers)](#new-site-v2-summary-dispatcher-20-sub-parsers)
  - [New: 100% ENDPOINT_PARSERS coverage (121/121)](#new-100-endpoint_parsers-coverage-121121)
  - [New: weekly cron live-test drift detector](#new-weekly-cron-live-test-drift-detector)
  - [New: MLB Stats API parser layer](#new-mlb-stats-api-parser-layer)
  - [New: NHL Stats REST + Records parser layers](#new-nhl-stats-rest--records-parser-layers)
  - [New: NHL api-web parser layer](#new-nhl-api-web-parser-layer)
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
  - [Bug fixes](#bug-fixes)
  - [Deprecations](#deprecations)
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

## 0.0.51 (unreleased)

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
  to the cross-league parametrized tests, expanding the dispatcher
  + boxscore_player + plays + drives + officials assertions from
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
  dispatch contract for each (boxscore_player + boxscore_team + plays
  + winprobability + leaders + 13 metadata sections + 2 football-only).

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
    (split out from `index.md`) with the full parser table, registry
    + `parser_for_mlb_api`, four chaining examples, and a fixture
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
