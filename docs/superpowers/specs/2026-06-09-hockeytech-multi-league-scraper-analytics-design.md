<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [HockeyTech Multi-League Scraper + Analytics — Design](#hockeytech-multi-league-scraper--analytics--design)
  - [Goal](#goal)
  - [Source material](#source-material)
  - [Decisions (locked during brainstorming)](#decisions-locked-during-brainstorming)
    - [Asymmetric starting points](#asymmetric-starting-points)
  - [Live-feed findings (probed 2026-06-09, PWHL game_id 42)](#live-feed-findings-probed-2026-06-09-pwhl-game_id-42)
    - [League registry (from scrapernhl `config.py`)](#league-registry-from-scrapernhl-configpy)
    - [Endpoint catalog (views used)](#endpoint-catalog-views-used)
    - [PBP event structure (gameCenterPlayByPlay / pxpverbose)](#pbp-event-structure-gamecenterplaybyplay--pxpverbose)
    - [Shift structure (modulekit `gameshifts`)](#shift-structure-modulekit-gameshifts)
  - [Architecture & module layout](#architecture--module-layout)
    - [Part A — sdv-py](#part-a--sdv-py)
    - [Part B — fastRhockey (R)](#part-b--fastrhockey-r)
  - [Function inventory (end state, both packages)](#function-inventory-end-state-both-packages)
  - [Data model & key algorithms (language-agnostic)](#data-model--key-algorithms-language-agnostic)
  - [Output conventions](#output-conventions)
  - [Error handling](#error-handling)
  - [Testing](#testing)
  - [Docs & integration](#docs--integration)
  - [Build sequencing → two implementation plans](#build-sequencing-%E2%86%92-two-implementation-plans)
  - [Out of scope](#out-of-scope)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# HockeyTech Multi-League Scraper + Analytics — Design

**Date:** 2026-06-09
**Status:** Approved (brainstorming) — ready for implementation planning
**Repos:** `sdv-py` (Part A, primary) and `fastRhockey` (Part B, R mirror)

## Goal

Add a live HockeyTech scraper to sdv-py covering the PWHL (full fastRhockey
output parity) plus the AHL, OHL, WHL, and QMJHL (core data set), and layer on
on-ice / Corsi-Fenwick / TOI analytics that the existing data feeds support.
Then mirror the entire surface into the `fastRhockey` R package so both packages
expose the same functions across all five leagues.

## Source material

Three upstream references, used for different things:

| Source | Used for |
|--------|----------|
| `IsabelleLefebvre97/pwhl-scraper` (Python) | HockeyTech endpoint catalog; client/JSONP reference. **Not** ported verbatim (it is SQLite/CLI oriented). |
| `maxtixador/scrapernhl` (Python) | The multi-league `LEAGUES` registry (client codes / keys / ids / base URLs / pbp dialects); analytics reference (NHL-only there). |
| `fastRhockey` (R) | The **output contract** for every PWHL function, and the helper/season-resolution logic. Approach 3 = transliterate the R for the PWHL parity layer. |

NHL is **out of scope** — different backend (`api-web.nhle.com`), already covered
by sdv-py's `nhl/` module and fastRhockey's NHL surface.

## Decisions (locked during brainstorming)

1. **PWHL scope:** full parity — all ~19 live fastRhockey `pwhl_*()` functions.
2. **Other leagues:** per-league prefixes (`ahl_*`, `ohl_*`, `whl_*`, `qmjhl_*`)
   over a shared core; juniors/AHL get the **core data set** (9 functions).
3. **Porting strategy (Approach 3):** fastRhockey-first transliteration for the
   PWHL parity layer; the shared core is the fastRhockey helper layer generalized
   to take a league config (sourced from scrapernhl's registry).
4. **Analytics in scope:** on-ice / Corsi-Fenwick / TOI / scoring chances +
   enriched PBP, with a documented missed-shot caveat (see Data model §4).
5. **R port (Part B):** **full mirror** of the sdv-py surface in `fastRhockey` —
   all five leagues + analytics. End state: the two packages are 1:1.

### Asymmetric starting points

- `sdv-py` today has only `load_pwhl_*` offline loaders → it gains the entire
  live surface.
- `fastRhockey` today already has the 19 PWHL parity functions → it gains the
  analytics, the richer PBP, and the four new league families.
- Therefore: **PWHL core 19 flow R → Python** (transliteration); the
  **analytics + junior/AHL leagues are built fresh in Python as the source of
  truth, then ported to R.**

## Live-feed findings (probed 2026-06-09, PWHL game_id 42)

Base URL `https://lscluster.hockeytech.com/feed/index.php` (QMJHL:
`https://cluster.leaguestat.com/feed/index.php`). Responses are JSONP wrapped in
`angular.callbacks._N(...)` (occasionally bare `(...)`) — strip, then parse.

**Key quirk:** the modulekit/statviewfeed key is `446521baf8c38984`, but
fastRhockey's `pwhl_pbp` uses a different key `694cfeed58c932ee` for the
`gameCenterPlayByPlay` view. Both observed working; the port must keep the
working key per endpoint and verify live.

### League registry (from scrapernhl `config.py`)

| League | client_code | api_key (public default; env-overridable) | league_id | site_id | base_url host | pbp dialect | reg-season OT |
|--------|-------------|-------------------------------------------|-----------|---------|---------------|-------------|---------------|
| PWHL | `pwhl` | `446521baf8c38984` | 1 | 0 | lscluster | hockeytech_a | 600s (10-min 3v3) |
| AHL | `ahl` | `ccb91f29d6744675` | 4 | 3 | lscluster | hockeytech_a | 300s |
| OHL | `ohl` | `f1aa699db3d81487` | 1 | 1 | lscluster | hockeytech_b | 300s |
| WHL | `whl` | `f1aa699db3d81487` | 7 | 0 | lscluster | hockeytech_b | 300s |
| QMJHL | `lhjmq` | `f322673b6bcae299` | 6 | 0 | cluster.leaguestat | hockeytech_b | 300s |

Keys are public web-client defaults; both packages expose env-var overrides
(`SCRAPERNHL_<LG>_API_KEY`-style or a sdv-py-native equivalent).

### Endpoint catalog (views used)

`statviewfeed`: `bootstrap`, `player`, `players` (skaters/goalies), `teams`,
`gameSummary`, `gameCenterPlayByPlay`, `leadersExtended`. `modulekit`:
`scorebar`, `seasons`, `teamsbyseason`, `roster`, `brackets`, `statviewtype`
(skaters/goalies), `player` (seasonstats/gamebygame), **`gameshifts`**. `gc`:
`gamesummary`, `pxpverbose`, `clock`, `preview`.

### PBP event structure (gameCenterPlayByPlay / pxpverbose)

Event types observed: `goalie_change`, `faceoff`, `blocked_shot`, `shot`,
`hit`, `goal`, `penalty`. Salient detail fields:

- `shot`: `shooter{id,firstName,lastName,position,jerseyNumber}`, `goalie`,
  `shooterTeamId`, `isGoal`, `shotQuality`, `shotType`, `xLocation`, `yLocation`.
- `blocked_shot`: `shooter`, `blocker`, `goalie`, `shooterTeamId`, `shotQuality`,
  `shotType`, `xLocation`, `yLocation`.
- `goal`: `scoredBy`, `assists[]`, `team{id}`, `plus_players`, `minus_players`,
  `properties{isEmptyNet,isGameWinningGoal,...}`, `xLocation`, `yLocation`.
- `faceoff`: `homePlayer`, `visitingPlayer`, `homeWin`, `xLocation`, `yLocation`.
- `hit`: `player`, `onPlayer`, `teamId`, `xLocation`, `yLocation`.
- `penalty`: `takenBy`, `servedBy`, `againstTeam`, `minutes`, `isPowerPlay`,
  `description`, `isBench`.

On-ice players appear **only on `goal` events** (`plus`/`minus`). All other shot
attempts require on-ice reconstruction from shifts (Data model §3).

### Shift structure (modulekit `gameshifts`)

`SiteKit.Gameshifts.{home,visitor}` → list of players, each:
`{player_id, first_name, last_name, jersey_number, home, shifts:[...]}`. Each
shift: `{period, start_time, end_time, length, goal_on_shift, penalty_on_shift}`.
**The clock counts down** within a period (`start_time` > `end_time`, e.g.
`03:16 → 03:06` is a 10-second shift). ~19–20 skaters per side.

## Architecture & module layout

### Part A — sdv-py

```
sportsdataverse/
  hockeytech/                 # NEW shared core (fastRhockey helpers, generalized by league config)
    __init__.py
    _client.py                # hockeytech_api(): build URL, fetch, strip JSONP, retry, rate-limit
    _leagues.py               # LEAGUES registry (table above) + env-var key overrides + season helpers
    _parsers.py               # JSON -> tidy polars frames; pbp dialects a/b; shifts parser
    _analytics.py             # PURE frame->frame: shot_distance_angle, scoring_chances,
                              #   build_on_ice(pbp, shifts), corsi_fenwick(on_ice),
                              #   player_toi(shifts), strength_state(pbp), per60_rates
  pwhl/
    pwhl_loaders.py           # existing offline loaders — untouched
    pwhl_loaders_extra.py     # existing — untouched
    pwhl_api.py               # NEW: 19 live pwhl_*() parity functions
    pwhl_analytics.py         # NEW: pwhl_game_shifts/player_toi/game_corsi; enriched pbp glue
    __init__.py               # + import pwhl_api, pwhl_analytics
  ahl/  __init__.py, ahl_api.py, ahl_analytics.py     # NEW (core set + analytics)
  ohl/  __init__.py, ohl_api.py, ohl_analytics.py     # NEW
  whl/  __init__.py, whl_api.py, whl_analytics.py     # NEW
  qmjhl/__init__.py, qmjhl_api.py, qmjhl_analytics.py # NEW
```

Per-league public modules are thin shims that inject their `LEAGUES` config into
the shared core. All HTTP and parsing live in `hockeytech/`. Analytics are pure
functions on DataFrames (no network) → reused identically across all leagues.

### Part B — fastRhockey (R)

```
R/
  hockeytech_helpers.R        # generalize existing .pwhl_api / *_url helpers to take a league config
  hockeytech_analytics.R      # pure: shot distance/angle, on-ice build, corsi/fenwick, toi, strength
  pwhl_*.R                    # existing 19 — enrich pwhl_pbp (superset cols); add pwhl_game_shifts/
                              #   pwhl_player_toi/pwhl_game_corsi
  ahl_*.R / ohl_*.R / whl_*.R / qmjhl_*.R   # NEW core-set families + analytics
```

## Function inventory (end state, both packages)

Prefix `<lg>` ∈ {`pwhl`, `ahl`, `ohl`, `whl`, `qmjhl`}.

- **PWHL — full parity (19):** `pwhl_schedule`, `pwhl_scorebar`, `pwhl_game_info`,
  `pwhl_game_summary`, `pwhl_pbp`, `pwhl_player_box`, `pwhl_teams`,
  `pwhl_team_roster`, `pwhl_standings`, `pwhl_player_info`, `pwhl_player_stats`,
  `pwhl_player_game_log`, `pwhl_player_search`, `pwhl_stats`, `pwhl_leaders`,
  `pwhl_streaks`, `pwhl_transactions`, `pwhl_playoff_bracket`, `pwhl_season_id`
  + `most_recent_pwhl_season`.
- **AHL/OHL/WHL/QMJHL — core set (9):** `<lg>_schedule`, `<lg>_pbp`,
  `<lg>_standings`, `<lg>_teams`, `<lg>_team_roster`, `<lg>_player_stats`,
  `<lg>_leaders`, `<lg>_game_summary`, `<lg>_season_id`
  + `most_recent_<lg>_season`.
- **Analytics — all 5 leagues (where `gameshifts` returns data):**
  `<lg>_game_shifts`, `<lg>_player_toi`, `<lg>_game_corsi`, and **enriched**
  `<lg>_pbp` (superset of fastRhockey columns).

## Data model & key algorithms (language-agnostic)

1. **Feeds & JSONP** — three feeds (`statviewfeed`, `modulekit`, `gc`); strip
   `angular.callbacks._N(...)` / `(...)`; parse. League config supplies
   `client_code`/`key`/`league_id`/`site_id`/`base_url`/`pbp_style`.
2. **Season resolution** — `<lg>_season_id()` lists seasons from the `seasons`
   view. Live functions accept `season` as **end-year** (e.g. `2026` ⇒ 2025-26),
   resolved to the integer HockeyTech `season_id` by parsing `season_name`, with a
   hardcoded PWHL fallback (ported from fastRhockey). A raw `season_id` is also
   accepted.
3. **Countdown-clock on-ice reconstruction** — clock counts down; a player is
   on-ice for an event at `(period, t)` iff a shift has
   `start_time ≥ t ≥ end_time` in that period. Yields `on_ice_home` /
   `on_ice_away` skater sets per event. (Same interval technique as WNBA
   `players_on_court`.)
4. **Corsi / Fenwick (documented gap)** — shot attempts =
   `shot` + `blocked_shot` + `goal`. `CF/CA` include blocks; `FF/FA` exclude
   blocks. **Missed shots are not in the feed**, so both are proxies. Every
   analytics output carries a `corsi_includes_missed = False` flag and a
   docstring / `@return` note. Per-60 = `metric / on_ice_toi_seconds * 3600`.
5. **Strength state** — from `penalty` events (`isPowerPlay`/minutes → PP/PK
   windows) and goalie-on-ice (empty-net). Tags each event `EV`/`PP`/`PK`/`EN`.
6. **Coordinates** — `xLocation`/`yLocation` on a ~850×400 canvas (PWHL/AHL) /
   ~600×300 (juniors); transform to a standard rink frame to match fastRhockey's
   `x_coord`/`y_coord`, then derive `shot_distance`/`shot_angle`/`scoring_chance`.

## Output conventions

- **sdv-py:** polars default + `return_as_pandas: bool` (matches every existing
  loader/wrapper). PWHL columns match fastRhockey exactly; `<lg>_pbp` is a strict
  **superset** of fastRhockey columns. Returns echo `season`/`game_id`/`league`
  (self-describing, per repo memory).
- **fastRhockey:** `fastRhockey_data`-classed tibbles; roxygen `@return` tables;
  `%||%` null-safety; existing `pwhl_*` column contracts preserved (enrichment
  only appends columns).

## Error handling

- **sdv-py:** initialize the return variable before any `try`; on failure log and
  return a typed-empty frame (mirrors `nfl_*` / `load_*`). All network goes
  through one retrying, rate-limited `hockeytech_api()`.
- **fastRhockey:** `tryCatch` + `cli::cli_alert_*`, return empty `fastRhockey_data`
  on error (matches existing `pwhl_*`). PWHL season fallback already exists.

## Testing

- **Offline (default, CI-safe):** captured JSON fixtures per endpoint/league →
  parser + analytics unit tests. Analytics are pure → Corsi/TOI/on-ice are tested
  on a **hand-checked fixture game** with known answers. Cross-language **parity
  tests** assert sdv-py and fastRhockey produce the same columns and the same
  Corsi/TOI numbers on the same fixture game.
- **Live (env-gated):** `HOCKEYTECH_TESTS=1` (Python) / `skip_hockeytech_test()`
  (R); subset-direction column assertions (expected ⊆ actual) per repo
  convention; rate-limited.

## Docs & integration

- **sdv-py:** new functions flow through codegen autodoc return-tables + pkgdown
  reference; add a PWHL/HockeyTech notebook section; the R-parity table picks up
  the new `pwhl_*`/`<lg>_*` ↔ fastRhockey mappings.
- **fastRhockey:** `devtools::document()`; `NEWS.md` / `_pkgdown.yml` /
  `cran-comments.md` triad; doctoc TOCs — per that repo's CLAUDE.md.

## Build sequencing → two implementation plans

**Plan 1 — sdv-py (A1–A4):**
- A1: HockeyTech core (client, leagues, parsers) + `pwhl_*` parity (R→Py) + tests.
- A2: Analytics (shifts/TOI/Corsi/on-ice/scoring-chances + enriched pbp), fixture-validated — source of truth.
- A3: AHL/OHL/WHL/QMJHL core families + analytics (verify each league's `gameshifts`).
- A4: Docs / codegen / notebook.

**Plan 2 — fastRhockey (B1–B4):** mirror A1–A4 in R (port the validated logic), then the R docs triad.

## Out of scope

- The upstream SQLite database, `setup/update/export` CLI, and any persistence.
- NHL (already covered in both packages).
- Missed-shot tracking (not in the HockeyTech feed) — Corsi/Fenwick are proxies.
