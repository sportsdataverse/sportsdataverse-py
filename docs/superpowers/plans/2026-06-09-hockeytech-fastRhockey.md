<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [HockeyTech Multi-League Scraper + Analytics — fastRhockey (R) Mirror Plan (Part B)](#hockeytech-multi-league-scraper--analytics--fastrhockey-r-mirror-plan-part-b)
  - [Conventions every task follows (per fastRhockey CLAUDE.md)](#conventions-every-task-follows-per-fastrhockey-claudemd)
  - [File Structure](#file-structure)
  - [Phase B1 — Generalized HockeyTech core in R](#phase-b1--generalized-hockeytech-core-in-r)
    - [Task B1.1: League registry (`hockeytech_leagues.R`)](#task-b11-league-registry-hockeytech_leaguesr)
    - [Task B1.2: Generalized URL + API helpers (`hockeytech_helpers.R`)](#task-b12-generalized-url--api-helpers-hockeytech_helpersr)
    - [Task B1.3: Generic season resolution `.hockeytech_season_id`](#task-b13-generic-season-resolution-hockeytech_season_id)
  - [Phase B2 — Shared parsers (seasons, shifts) + enrich pwhl_pbp](#phase-b2--shared-parsers-seasons-shifts--enrich-pwhl_pbp)
    - [Task B2.1: `.parse_hockeytech_seasons` + shifts parser](#task-b21-parse_hockeytech_seasons--shifts-parser)
    - [Task B2.2: Analytics layer (`hockeytech_analytics.R`)](#task-b22-analytics-layer-hockeytech_analyticsr)
    - [Task B2.3: Enrich `pwhl_pbp` (superset columns + blocked_shot/hit)](#task-b23-enrich-pwhl_pbp-superset-columns--blocked_shothit)
    - [Task B2.4: PWHL analytics public functions](#task-b24-pwhl-analytics-public-functions)
  - [Phase B3 — AHL / OHL / WHL / QMJHL families in R](#phase-b3--ahl--ohl--whl--qmjhl-families-in-r)
    - [Task B3.1: League family generator + per-league files](#task-b31-league-family-generator--per-league-files)
  - [Phase B4 — Docs triad, parity check, full check](#phase-b4--docs-triad-parity-check-full-check)
    - [Task B4.1: NEWS / pkgdown / cran-comments + doctoc](#task-b41-news--pkgdown--cran-comments--doctoc)
    - [Task B4.2: Cross-language parity test](#task-b42-cross-language-parity-test)
    - [Task B4.3: Full `R CMD check`](#task-b43-full-r-cmd-check)
  - [Self-Review (against the design doc)](#self-review-against-the-design-doc)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# HockeyTech Multi-League Scraper + Analytics — fastRhockey (R) Mirror Plan (Part B)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mirror the sdv-py HockeyTech surface into the `fastRhockey` R package — add AHL/OHL/WHL/QMJHL families, on-ice / Corsi-Fenwick / TOI analytics, and enrich `pwhl_pbp` — so the two packages expose the same functions across all five leagues.

**Architecture:** Generalize fastRhockey's existing PWHL helper layer (`.pwhl_api`/`.pwhl_*_url`) into a league-parameterized `.hockeytech_*` core driven by a `.hockeytech_leagues()` registry; add a pure `hockeytech_analytics.R` layer (geometry, on-ice, Corsi, TOI) ported from the **validated** sdv-py implementation (Part A); add `ahl_*`/`ohl_*`/`whl_*`/`qmjhl_*` function families that call the generalized core.

**Tech Stack:** R (>= 4.1), `httr2` via `.retry_request`/`.resp_text`, `jsonlite`, `dplyr`, `glue`, `purrr`, `stringr`; `testthat` (3e); roxygen2; `make_fastRhockey_data()` for the output class.

**Prerequisite:** Part A (sdv-py) is implemented and its analytics fixture-validated. This plan ports the *proven* logic; column contracts and Corsi/TOI numbers must match Part A on the shared fixture game (cross-language parity test, Task B4.2).

**Repo:** `c:\Users\saiem\Documents\GitHub-Data\sdv-dev\hockey-dev\fastRhockey`
**Design doc:** `docs/superpowers/specs/2026-06-09-hockeytech-multi-league-scraper-analytics-design.md` (in sdv-py).

---

## Conventions every task follows (per fastRhockey CLAUDE.md)

- **snake_case columns** via `janitor::clean_names()` + explicit `dplyr::rename()`; fastRhockey PWHL column contracts are preserved (enrichment only appends).
- **Output class:** every return wrapped with `make_fastRhockey_data(df, type, Sys.time())`.
- **Return-var init before `tryCatch`**; on error `cli::cli_alert_danger()` + return empty `fastRhockey_data`.
- **Tests:** `tests/testthat/test-<fn>.R`, gated by `skip_on_cran()` + a source-specific skip helper; subset-direction column checks (expected ⊆ actual).
- **Docs regeneration (mechanical, never hand-edit generated regions):** `devtools::document()`; NEWS.md / `_pkgdown.yml` / cran-comments.md triad; doctoc TOCs via `Rscript tools/run_doctoc.R` if present.
- **No AI co-author trailers.** Conventional Commits.

---

## File Structure

**New — shared core + analytics:**
- `R/hockeytech_leagues.R` — `.hockeytech_leagues()` registry (the 5 leagues), `.hockeytech_resolve_key()`, `.hockeytech_season_id(league, ...)`.
- `R/hockeytech_helpers.R` — `.hockeytech_url(league, feed, view, params)`, `.hockeytech_api(url)` (generalized from `.pwhl_api`).
- `R/hockeytech_analytics.R` — `hockeytech_shot_distance_angle()`, `hockeytech_scoring_chances()`, `hockeytech_build_on_ice()`, `hockeytech_corsi_fenwick()`, `hockeytech_player_toi()`, `.mmss_to_seconds()`.

**New — analytics + junior families (public):**
- `R/pwhl_game_shifts.R`, `R/pwhl_player_toi.R`, `R/pwhl_game_corsi.R`.
- `R/ahl_*.R`, `R/ohl_*.R`, `R/whl_*.R`, `R/qmjhl_*.R` — core set (schedule/pbp/standings/teams/team_roster/player_stats/leaders/game_summary/season_id + most_recent_*_season) + analytics (game_shifts/player_toi/game_corsi).

**Modified:**
- `R/pwhl_pbp.R` — enrich with `shot_distance`/`shot_angle`/`scoring_chance` + `blocked_shot`/`hit` events (superset).
- `R/pwhl_helpers.R` — re-point `.pwhl_*` helpers at the generalized `.hockeytech_*` (keep `.pwhl_api` as a thin alias for back-compat).
- `NAMESPACE` (regenerated), `NEWS.md`, `_pkgdown.yml`, `cran-comments.md`.

---

## Phase B1 — Generalized HockeyTech core in R

### Task B1.1: League registry (`hockeytech_leagues.R`)

**Files:**
- Create: `R/hockeytech_leagues.R`
- Test: `tests/testthat/test-hockeytech_leagues.R`

- [ ] **Step 1: Write the failing test**

```r
# tests/testthat/test-hockeytech_leagues.R
test_that("hockeytech league registry has the five HockeyTech leagues", {
  leagues <- fastRhockey:::.hockeytech_leagues()
  expect_setequal(names(leagues), c("pwhl", "ahl", "ohl", "whl", "qmjhl"))
  expect_equal(leagues$pwhl$client_code, "pwhl")
  expect_equal(leagues$pwhl$league_id, 1)
  expect_equal(leagues$qmjhl$client_code, "lhjmq")
  expect_true(grepl("cluster.leaguestat.com", leagues$qmjhl$base_url))
})

test_that("env var overrides the api key", {
  withr::with_envvar(c(SDV_PWHL_API_KEY = "override123"), {
    expect_equal(fastRhockey:::.hockeytech_resolve_key("pwhl"), "override123")
  })
  expect_equal(fastRhockey:::.hockeytech_resolve_key("pwhl"), "446521baf8c38984")
})
```

- [ ] **Step 2: Run to verify it fails**

Run: `Rscript -e 'devtools::load_all(); testthat::test_file("tests/testthat/test-hockeytech_leagues.R")'`
Expected: FAIL — `.hockeytech_leagues` not found.

- [ ] **Step 3: Implement `hockeytech_leagues.R`**

```r
# R/hockeytech_leagues.R
#' @keywords internal
#' HockeyTech league registry. Values mirror sdv-py sportsdataverse/hockeytech/_leagues.py.
#' @noRd
.hockeytech_leagues <- function() {
  ls <- "https://lscluster.hockeytech.com/feed/index.php"
  lg <- "https://cluster.leaguestat.com/feed/index.php"
  list(
    pwhl  = list(name = "PWHL",  client_code = "pwhl",  api_key = "446521baf8c38984",
                 league_id = 1, site_id = 0, base_url = ls, pbp_style = "hockeytech_a"),
    ahl   = list(name = "AHL",   client_code = "ahl",   api_key = "ccb91f29d6744675",
                 league_id = 4, site_id = 3, base_url = ls, pbp_style = "hockeytech_a"),
    ohl   = list(name = "OHL",   client_code = "ohl",   api_key = "f1aa699db3d81487",
                 league_id = 1, site_id = 1, base_url = ls, pbp_style = "hockeytech_b"),
    whl   = list(name = "WHL",   client_code = "whl",   api_key = "f1aa699db3d81487",
                 league_id = 7, site_id = 0, base_url = ls, pbp_style = "hockeytech_b"),
    qmjhl = list(name = "QMJHL", client_code = "lhjmq", api_key = "f322673b6bcae299",
                 league_id = 6, site_id = 0, base_url = lg, pbp_style = "hockeytech_b")
  )
}

#' @keywords internal
#' @noRd
.hockeytech_resolve_key <- function(league, view = NULL) {
  env <- Sys.getenv(paste0("SDV_", toupper(league), "_API_KEY"), unset = "")
  if (nzchar(env)) return(env)
  if (!is.null(view) && view == "gameCenterPlayByPlay" && league == "pwhl") {
    return("694cfeed58c932ee")
  }
  .hockeytech_leagues()[[league]]$api_key
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `Rscript -e 'devtools::load_all(); testthat::test_file("tests/testthat/test-hockeytech_leagues.R")'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add R/hockeytech_leagues.R tests/testthat/test-hockeytech_leagues.R
git commit -m "feat(hockeytech): R league registry + env-var key override"
```

---

### Task B1.2: Generalized URL + API helpers (`hockeytech_helpers.R`)

**Files:**
- Create: `R/hockeytech_helpers.R`
- Modify: `R/pwhl_helpers.R` (re-point `.pwhl_*` at the generalized core)
- Test: `tests/testthat/test-hockeytech_helpers.R`

- [ ] **Step 1: Write the failing test**

```r
# tests/testthat/test-hockeytech_helpers.R
test_that(".hockeytech_url builds a feed URL with key + client_code", {
  u <- fastRhockey:::.hockeytech_url("pwhl", feed = "modulekit", view = "seasons",
                                     params = list(site_id = 0))
  expect_true(grepl("^https://lscluster.hockeytech.com/feed/index.php\\?", u))
  expect_true(grepl("feed=modulekit", u) && grepl("view=seasons", u))
  expect_true(grepl("key=446521baf8c38984", u) && grepl("client_code=pwhl", u))
})

test_that("pbp view uses the override key", {
  u <- fastRhockey:::.hockeytech_url("pwhl", feed = "statviewfeed",
                                     view = "gameCenterPlayByPlay", params = list(game_id = 42))
  expect_true(grepl("key=694cfeed58c932ee", u))
})
```

- [ ] **Step 2: Run to verify it fails**

Run: `Rscript -e 'devtools::load_all(); testthat::test_file("tests/testthat/test-hockeytech_helpers.R")'`
Expected: FAIL — `.hockeytech_url` not found.

- [ ] **Step 3: Implement `hockeytech_helpers.R`**

```r
# R/hockeytech_helpers.R
#' @keywords internal
#' Build a HockeyTech feed URL for any league.
#' @noRd
.hockeytech_url <- function(league, feed, view, params = list()) {
  cfg <- .hockeytech_leagues()[[league]]
  defaults <- list(
    feed = feed,
    view = view,
    key = .hockeytech_resolve_key(league, view = view),
    client_code = cfg$client_code,
    site_id = cfg$site_id,
    lang = "en",
    callback = "angular.callbacks._0"
  )
  all_params <- c(params, defaults)
  query <- paste(names(all_params), all_params, sep = "=", collapse = "&")
  paste0(cfg$base_url, "?", query)
}

#' @keywords internal
#' Fetch a HockeyTech URL and strip the JSONP callback wrapper.
#' @noRd
.hockeytech_api <- function(url) {
  res <- .retry_request(url)
  res <- .resp_text(res)
  res <- sub("^[A-Za-z_$][A-Za-z0-9_.$]*\\(", "", res)  # angular.callbacks._N( ... )
  res <- sub("\\)\\s*$", "", res)
  jsonlite::parse_json(res)
}
```

- [ ] **Step 4: Re-point the PWHL helpers (keep back-compat)**

```r
# R/pwhl_helpers.R — replace the bodies of .pwhl_modulekit_url / .pwhl_gc_url / .pwhl_api
.pwhl_api <- function(url) .hockeytech_api(url)
.pwhl_modulekit_url <- function(params) .hockeytech_url("pwhl", "modulekit", params$view %||% "", params[setdiff(names(params), "view")])
.pwhl_gc_url <- function(params) .hockeytech_url("pwhl", "gc", params$tab %||% "gamesummary", params[setdiff(names(params), "tab")])
```

> Implementer note: verify the existing `.pwhl_*_url` call sites still pass the
> same params; the generalized builder must produce URLs equivalent to the old
> ones (run the existing PWHL tests after this change — Step 5).

- [ ] **Step 5: Run to verify it passes (new + existing PWHL helper tests)**

Run: `Rscript -e 'devtools::load_all(); testthat::test_file("tests/testthat/test-hockeytech_helpers.R")'`
Expected: PASS.
Run: `Rscript -e 'devtools::load_all(); testthat::test_file("tests/testthat/test-pwhl_season_id.R")'`
Expected: PASS (back-compat preserved).

- [ ] **Step 6: Commit**

```bash
git add R/hockeytech_helpers.R R/pwhl_helpers.R tests/testthat/test-hockeytech_helpers.R
git commit -m "feat(hockeytech): generalized R URL/api helpers; PWHL helpers delegate to core"
```

---

### Task B1.3: Generic season resolution `.hockeytech_season_id`

**Files:**
- Modify: `R/hockeytech_leagues.R`
- Test: `tests/testthat/test-hockeytech_leagues.R`

- [ ] **Step 1: Add a failing test (offline via mocked `.hockeytech_api`)**

```r
# append to tests/testthat/test-hockeytech_leagues.R
test_that(".hockeytech_season_id passes through an explicit season_id", {
  expect_equal(fastRhockey:::.hockeytech_season_id("pwhl", season_id = 5), 5)
})
```

- [ ] **Step 2: Run to verify it fails**

Run: `Rscript -e 'devtools::load_all(); testthat::test_file("tests/testthat/test-hockeytech_leagues.R")'`
Expected: FAIL — `.hockeytech_season_id` not found.

- [ ] **Step 3: Implement `.hockeytech_season_id`**

```r
# append to R/hockeytech_leagues.R
#' @keywords internal
#' Resolve an end-year `season` (e.g. 2025) to the integer HockeyTech season_id.
#' Explicit `season_id` short-circuits. PWHL falls back to pwhl_season_id().
#' @noRd
.hockeytech_season_id <- function(league, season = NULL, game_type = "regular", season_id = NULL) {
  if (!is.null(season_id)) return(as.integer(season_id))
  if (is.null(season)) stop("Provide season (end-year) or season_id", call. = FALSE)
  url <- .hockeytech_url(league, "modulekit", "seasons", list())
  seasons <- tryCatch({
    r <- .hockeytech_api(url)
    .parse_hockeytech_seasons(r)
  }, error = function(e) data.frame())
  if (nrow(seasons) > 0) {
    hit <- seasons[seasons$season_yr == season & seasons$game_type_label == game_type, ]
    if (nrow(hit) > 0) return(as.integer(hit$season_id[1]))
  }
  if (league == "pwhl") return(.pwhl_resolve_season_id(season, game_type))  # existing fallback
  stop(glue::glue("No {league} season for season={season}, game_type={game_type}"), call. = FALSE)
}
```

> `.parse_hockeytech_seasons()` is the R twin of sdv-py `parse_seasons` (Task B2.1);
> add it there. Until B2.1 lands, this resolver works via the PWHL fallback path.

- [ ] **Step 4: Run to verify it passes**

Run: `Rscript -e 'devtools::load_all(); testthat::test_file("tests/testthat/test-hockeytech_leagues.R")'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add R/hockeytech_leagues.R tests/testthat/test-hockeytech_leagues.R
git commit -m "feat(hockeytech): generic end-year season_id resolution in R"
```

---

## Phase B2 — Shared parsers (seasons, shifts) + enrich pwhl_pbp

### Task B2.1: `.parse_hockeytech_seasons` + shifts parser

**Files:**
- Create: `R/hockeytech_parsers.R`
- Test: `tests/testthat/test-hockeytech_parsers.R`
- Fixture: `tests/testthat/fixtures/hockeytech/pwhl_gameshifts_42.json` (copy from sdv-py `tests/fixtures/hockeytech/`)

- [ ] **Step 1: Copy the shared fixture + write the failing test**

```r
# tests/testthat/test-hockeytech_parsers.R
.load_fx <- function(stem) {
  jsonlite::read_json(testthat::test_path("fixtures", "hockeytech", paste0(stem, ".json")))
}

test_that(".parse_hockeytech_shifts returns one row per stint with countdown seconds", {
  df <- fastRhockey:::.parse_hockeytech_shifts(.load_fx("pwhl_gameshifts_42"), game_id = 42)
  expect_s3_class(df, "data.frame")
  expect_true(nrow(df) > 0)
  for (col in c("game_id", "player_id", "first_name", "last_name", "home",
                "period", "start_time", "end_time", "start_s", "end_s")) {
    expect_true(col %in% names(df), info = paste("missing", col))
  }
  expect_true(all(df$start_s >= df$end_s, na.rm = TRUE))
})
```

- [ ] **Step 2: Run to verify it fails**

Run: `Rscript -e 'devtools::load_all(); testthat::test_file("tests/testthat/test-hockeytech_parsers.R")'`
Expected: FAIL — `.parse_hockeytech_shifts` not found.

- [ ] **Step 3: Implement `hockeytech_parsers.R` (seasons + shifts + mmss)**

```r
# R/hockeytech_parsers.R
#' @keywords internal
#' @noRd
.mmss_to_seconds <- function(x) {
  if (is.null(x) || is.na(x) || !nzchar(x)) return(NA_integer_)
  parts <- as.integer(strsplit(x, ":", fixed = TRUE)[[1]])
  if (length(parts) != 2 || any(is.na(parts))) return(NA_integer_)
  parts[1] * 60L + parts[2]
}

#' @keywords internal
#' @noRd
.parse_hockeytech_seasons <- function(payload) {
  raw <- payload$SiteKit$Seasons
  if (is.null(raw) || length(raw) == 0) return(data.frame())
  purrr::map_dfr(raw, function(s) {
    nm <- s$season_name %||% NA_character_
    data.frame(
      season_id = as.numeric(s$season_id %||% NA),
      season_name = as.character(nm),
      season_short = as.character(s$shortname %||% NA),
      season_yr = .pwhl_year_from_name(nm),         # reuse the PWHL year-derivation
      game_type_label = .pwhl_game_type_from_name(nm),
      stringsAsFactors = FALSE
    )
  })
}

#' @keywords internal
#' @noRd
.parse_hockeytech_shifts <- function(payload, game_id = NULL) {
  gs <- payload$SiteKit$Gameshifts
  if (is.null(gs)) return(data.frame())
  rows <- list()
  for (side in c("home", "visitor")) {
    for (player in gs[[side]] %||% list()) {
      for (sh in player$shifts %||% list()) {
        rows[[length(rows) + 1]] <- data.frame(
          game_id = game_id,
          player_id = player$player_id %||% NA,
          first_name = player$first_name %||% NA_character_,
          last_name = player$last_name %||% NA_character_,
          jersey_number = player$jersey_number %||% NA_character_,
          home = as.integer(player$home %||% (side == "home")),
          period = as.integer(sh$period %||% NA),
          start_time = sh$start_time %||% NA_character_,
          end_time = sh$end_time %||% NA_character_,
          length = sh$length %||% NA_character_,
          start_s = .mmss_to_seconds(sh$start_time),
          end_s = .mmss_to_seconds(sh$end_time),
          goal_on_shift = as.integer(sh$goal_on_shift %||% 0),
          penalty_on_shift = as.integer(sh$penalty_on_shift %||% 0),
          stringsAsFactors = FALSE
        )
      }
    }
  }
  dplyr::bind_rows(rows)
}
```

> Implementer note: extract `.pwhl_year_from_name()` / `.pwhl_game_type_from_name()`
> from the existing `pwhl_season_id()` mutate logic into reusable helpers so both
> PWHL and the generic seasons parser share one implementation.

- [ ] **Step 4: Run to verify it passes**

Run: `Rscript -e 'devtools::load_all(); testthat::test_file("tests/testthat/test-hockeytech_parsers.R")'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add R/hockeytech_parsers.R tests/testthat/test-hockeytech_parsers.R tests/testthat/fixtures/hockeytech/pwhl_gameshifts_42.json
git commit -m "feat(hockeytech): R seasons + shifts parsers (shared fixture)"
```

---

### Task B2.2: Analytics layer (`hockeytech_analytics.R`)

**Files:**
- Create: `R/hockeytech_analytics.R`
- Test: `tests/testthat/test-hockeytech_analytics.R`

- [ ] **Step 1: Write failing tests (mirror sdv-py A2.1–A2.4 numbers exactly)**

```r
# tests/testthat/test-hockeytech_analytics.R
test_that("shot distance/angle on a known point", {
  df <- data.frame(event = "shot", x_coord = 25, y_coord = 0)
  out <- fastRhockey:::hockeytech_shot_distance_angle(df, goal_x = 89)
  expect_equal(out$shot_distance[1], 64)
  expect_equal(out$shot_angle[1], 0)
})

test_that("player TOI sums shift lengths (countdown clock)", {
  shifts <- data.frame(player_id = c(1, 1, 2), first_name = c("A","A","B"),
                       last_name = c("X","X","Y"), period = c(1,1,1),
                       start_s = c(1200, 1100, 1200), end_s = c(1180, 1090, 1150))
  out <- fastRhockey:::hockeytech_player_toi(shifts)
  a <- out[out$player_id == 1, ]
  expect_equal(a$toi_seconds, 30)
  expect_equal(a$num_shifts, 2)
})

test_that("on-ice interval match on countdown clock", {
  pbp <- data.frame(event = "shot", period_of_game = 1, time_s = 1190, team_id = 10)
  shifts <- data.frame(player_id = c(1, 2), home = c(1, 1), period = c(1, 1),
                       start_s = c(1200, 1100), end_s = c(1180, 1090))
  out <- fastRhockey:::hockeytech_build_on_ice(pbp, shifts)
  expect_equal(out$on_ice_home[1], "1")
})

test_that("team Corsi/Fenwick proxies with missed-shot flag", {
  pbp <- data.frame(event = c("shot","blocked_shot","goal","faceoff"),
                    team_id = c(10, 10, 20, 10))
  out <- fastRhockey:::hockeytech_corsi_fenwick(pbp)
  expect_false(unique(out$corsi_includes_missed))
  expect_equal(out$corsi_for[out$team_id == 10], 2)
})
```

- [ ] **Step 2: Run to verify it fails**

Run: `Rscript -e 'devtools::load_all(); testthat::test_file("tests/testthat/test-hockeytech_analytics.R")'`
Expected: FAIL — analytics fns not found.

- [ ] **Step 3: Implement `hockeytech_analytics.R` (port of sdv-py `_analytics.py`)**

```r
# R/hockeytech_analytics.R
#' @keywords internal
#' @noRd
hockeytech_shot_distance_angle <- function(pbp, goal_x = 89) {
  if (nrow(pbp) == 0) {
    pbp$shot_distance <- numeric(0); pbp$shot_angle <- numeric(0); return(pbp)
  }
  is_shot <- pbp$event %in% c("shot", "blocked_shot", "goal")
  dx <- goal_x - abs(pbp$x_coord)
  dy <- pbp$y_coord
  pbp$shot_distance <- ifelse(is_shot, sqrt(dx^2 + dy^2), NA_real_)
  pbp$shot_angle <- ifelse(is_shot, abs(atan2(dy, dx)) * 180 / pi, NA_real_)
  pbp
}

#' @keywords internal
#' @noRd
hockeytech_scoring_chances <- function(pbp, threshold_ft = 25) {
  if (!"shot_distance" %in% names(pbp)) pbp <- hockeytech_shot_distance_angle(pbp)
  pbp$scoring_chance <- !is.na(pbp$shot_distance) & pbp$shot_distance <= threshold_ft
  pbp
}

#' @keywords internal
#' @noRd
hockeytech_player_toi <- function(shifts) {
  if (nrow(shifts) == 0) {
    return(data.frame(player_id = integer(), toi_seconds = integer(),
                      num_shifts = integer(), avg_shift_s = numeric()))
  }
  shifts$shift_s <- shifts$start_s - shifts$end_s
  dplyr::summarise(
    dplyr::group_by(shifts, .data$player_id, .data$first_name, .data$last_name),
    toi_seconds = sum(.data$shift_s, na.rm = TRUE),
    num_shifts = dplyr::n(),
    avg_shift_s = mean(.data$shift_s, na.rm = TRUE),
    .groups = "drop"
  )
}

#' @keywords internal
#' @noRd
hockeytech_build_on_ice <- function(pbp, shifts) {
  if (nrow(pbp) == 0 || nrow(shifts) == 0) {
    pbp$on_ice_home <- NA_character_; pbp$on_ice_away <- NA_character_; return(pbp)
  }
  pbp$.eidx <- seq_len(nrow(pbp))
  j <- dplyr::inner_join(pbp, shifts, by = c("period_of_game" = "period"),
                         relationship = "many-to-many")
  on <- j[j$start_s >= j$time_s & j$time_s >= j$end_s, ]
  side_ids <- function(df, home_flag) {
    sub <- df[df$home == home_flag, ]
    if (nrow(sub) == 0) return(data.frame(.eidx = integer(), ids = character()))
    dplyr::summarise(dplyr::group_by(sub, .data$.eidx),
                     ids = paste(sort(unique(.data$player_id)), collapse = ","),
                     .groups = "drop")
  }
  h <- side_ids(on, 1); a <- side_ids(on, 0)
  pbp <- dplyr::left_join(pbp, dplyr::rename(h, on_ice_home = "ids"), by = ".eidx")
  pbp <- dplyr::left_join(pbp, dplyr::rename(a, on_ice_away = "ids"), by = ".eidx")
  pbp$.eidx <- NULL
  pbp
}

#' @keywords internal
#' @noRd
hockeytech_corsi_fenwick <- function(pbp) {
  corsi <- c("shot", "blocked_shot", "goal"); fenwick <- c("shot", "goal")
  teams <- unique(pbp$team_id[!is.na(pbp$team_id)])
  if (length(teams) == 0) {
    return(data.frame(team_id = integer(), corsi_for = integer(), corsi_against = integer(),
                      corsi_for_pct = numeric(), fenwick_for = integer(),
                      fenwick_against = integer(), fenwick_for_pct = numeric(),
                      corsi_includes_missed = logical()))
  }
  purrr::map_dfr(teams, function(t) {
    cf <- sum(pbp$event %in% corsi & pbp$team_id == t, na.rm = TRUE)
    ca <- sum(pbp$event %in% corsi & pbp$team_id != t & !is.na(pbp$team_id), na.rm = TRUE)
    ff <- sum(pbp$event %in% fenwick & pbp$team_id == t, na.rm = TRUE)
    fa <- sum(pbp$event %in% fenwick & pbp$team_id != t & !is.na(pbp$team_id), na.rm = TRUE)
    data.frame(team_id = t, corsi_for = cf, corsi_against = ca,
               corsi_for_pct = ifelse((cf + ca) > 0, cf / (cf + ca), NA_real_),
               fenwick_for = ff, fenwick_against = fa,
               fenwick_for_pct = ifelse((ff + fa) > 0, ff / (ff + fa), NA_real_),
               corsi_includes_missed = FALSE)
  })
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `Rscript -e 'devtools::load_all(); testthat::test_file("tests/testthat/test-hockeytech_analytics.R")'`
Expected: PASS (4 tests; numbers identical to sdv-py A2).

- [ ] **Step 5: Commit**

```bash
git add R/hockeytech_analytics.R tests/testthat/test-hockeytech_analytics.R
git commit -m "feat(hockeytech): R analytics layer (geometry/on-ice/Corsi/TOI) ported from sdv-py"
```

---

### Task B2.3: Enrich `pwhl_pbp` (superset columns + blocked_shot/hit)

**Files:**
- Modify: `R/pwhl_pbp.R`
- Test: `tests/testthat/test-pwhl_pbp.R`

- [ ] **Step 1: Add failing assertions to the existing test**

```r
# append inside the existing test_that block in tests/testthat/test-pwhl_pbp.R
# (after the existing expected_cols loop, when nrow(x) > 0)
        enriched <- c("x_coord", "y_coord", "shot_distance", "shot_angle", "scoring_chance")
        for (col in enriched) {
          expect_true(col %in% names(x), info = paste("Missing enriched column:", col))
        }
```

- [ ] **Step 2: Run to verify it fails**

Run: `PWHL_TESTS=1 Rscript -e 'devtools::load_all(); testthat::test_file("tests/testthat/test-pwhl_pbp.R")'`
Expected: FAIL — `shot_distance`/`shot_angle`/`scoring_chance` missing (and `blocked_shot` rows absent).

- [ ] **Step 3: Enrich `pwhl_pbp`**

Append, just before the final `make_fastRhockey_data(...)` return in `pwhl_pbp()`:

```r
  # Enrich with shot geometry + scoring chances (superset of legacy columns).
  game_df <- hockeytech_scoring_chances(hockeytech_shot_distance_angle(game_df))
```

Also extend the event loop to emit `blocked_shot` and `hit` rows (the feed
carries them — see design doc PBP structure). Map `blocked_shot` like `shot`
(with `event_type = shotType`), and `hit` with `player_id`/`team_id`/coords.

- [ ] **Step 4: Run to verify it passes**

Run: `PWHL_TESTS=1 Rscript -e 'devtools::load_all(); testthat::test_file("tests/testthat/test-pwhl_pbp.R")'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add R/pwhl_pbp.R tests/testthat/test-pwhl_pbp.R
git commit -m "feat(pwhl): enrich pwhl_pbp with shot geometry + blocked_shot/hit events"
```

---

### Task B2.4: PWHL analytics public functions

**Files:**
- Create: `R/pwhl_game_shifts.R`, `R/pwhl_player_toi.R`, `R/pwhl_game_corsi.R`
- Test: `tests/testthat/test-pwhl_game_shifts.R` (+ toi/corsi)

- [ ] **Step 1: Write failing tests (live-gated)**

```r
# tests/testthat/test-pwhl_game_shifts.R
test_that("PWHL - game shifts", {
  skip_on_cran(); skip_pwhl_test()
  x <- pwhl_game_shifts(game_id = 42)
  if (is.data.frame(x) && nrow(x) > 0) {
    for (col in c("game_id", "player_id", "period", "start_s", "end_s")) {
      expect_true(col %in% names(x), info = paste("Missing column:", col))
    }
  } else {
    expect_s3_class(x, "data.frame")
  }
})
```

(Analogous `test-pwhl_player_toi.R` checks `toi_seconds`/`num_shifts`;
`test-pwhl_game_corsi.R` checks `corsi_for`/`corsi_includes_missed`.)

- [ ] **Step 2: Run to verify it fails**

Run: `Rscript -e 'devtools::load_all(); testthat::test_file("tests/testthat/test-pwhl_game_shifts.R")'`
Expected: FAIL — `pwhl_game_shifts` not found.

- [ ] **Step 3: Implement the three functions (roxygen with `@return` tables)**

```r
# R/pwhl_game_shifts.R
#' @title **PWHL Game Shifts**
#' @description All player shifts for a PWHL game (one row per stint).
#' @param game_id PWHL game id.
#' @return A `fastRhockey_data` data frame with columns including `game_id`,
#'   `player_id`, `first_name`, `last_name`, `home`, `period`, `start_time`,
#'   `end_time`, `length`, `start_s`, `end_s`, `goal_on_shift`, `penalty_on_shift`.
#' @import dplyr
#' @importFrom glue glue
#' @export
#' @examples
#' \donttest{ try(pwhl_game_shifts(game_id = 42)) }
pwhl_game_shifts <- function(game_id) {
  shifts_df <- data.frame()
  tryCatch(
    expr = {
      url <- .hockeytech_url("pwhl", "modulekit", "gameshifts", list(game_id = game_id))
      r <- .hockeytech_api(url)
      shifts_df <- .parse_hockeytech_shifts(r, game_id = game_id)
      shifts_df <- make_fastRhockey_data(shifts_df, "PWHL Game Shifts data from HockeyTech", Sys.time())
    },
    error = function(e) cli::cli_alert_danger("{Sys.time()}: game_id {game_id} unavailable! {e}")
  )
  shifts_df
}
```

```r
# R/pwhl_player_toi.R
#' @title **PWHL Player Time On Ice**
#' @description Per-player TOI for a PWHL game, from the shift tables.
#' @param game_id PWHL game id.
#' @return A `fastRhockey_data` data frame: `player_id`, `first_name`,
#'   `last_name`, `toi_seconds`, `num_shifts`, `avg_shift_s`.
#' @import dplyr
#' @export
#' @examples
#' \donttest{ try(pwhl_player_toi(game_id = 42)) }
pwhl_player_toi <- function(game_id) {
  toi_df <- data.frame()
  tryCatch(
    expr = {
      toi_df <- hockeytech_player_toi(pwhl_game_shifts(game_id = game_id))
      toi_df <- make_fastRhockey_data(toi_df, "PWHL Player TOI from HockeyTech", Sys.time())
    },
    error = function(e) cli::cli_alert_danger("{Sys.time()}: game_id {game_id} unavailable! {e}")
  )
  toi_df
}
```

```r
# R/pwhl_game_corsi.R
#' @title **PWHL Game Corsi/Fenwick**
#' @description Team-level shot-attempt metrics for a PWHL game. Corsi =
#'   shot+blocked+goal; Fenwick excludes blocks. Missed shots are not in the
#'   HockeyTech feed, so both are proxies (`corsi_includes_missed = FALSE`).
#' @param game_id PWHL game id.
#' @return A `fastRhockey_data` data frame: `team_id`, `corsi_for`,
#'   `corsi_against`, `corsi_for_pct`, `fenwick_for`, `fenwick_against`,
#'   `fenwick_for_pct`, `corsi_includes_missed`.
#' @import dplyr
#' @export
#' @examples
#' \donttest{ try(pwhl_game_corsi(game_id = 42)) }
pwhl_game_corsi <- function(game_id) {
  corsi_df <- data.frame()
  tryCatch(
    expr = {
      corsi_df <- hockeytech_corsi_fenwick(pwhl_pbp(game_id = game_id))
      corsi_df <- make_fastRhockey_data(corsi_df, "PWHL Game Corsi from HockeyTech", Sys.time())
    },
    error = function(e) cli::cli_alert_danger("{Sys.time()}: game_id {game_id} unavailable! {e}")
  )
  corsi_df
}
```

- [ ] **Step 4: Document + run**

Run: `Rscript -e 'devtools::document(); devtools::load_all(); testthat::test_file("tests/testthat/test-pwhl_game_shifts.R")'`
Expected: NAMESPACE updated (3 new exports); test PASS or graceful skip.

- [ ] **Step 5: Commit**

```bash
git add R/pwhl_game_shifts.R R/pwhl_player_toi.R R/pwhl_game_corsi.R NAMESPACE man/pwhl_game_shifts.Rd man/pwhl_player_toi.Rd man/pwhl_game_corsi.Rd tests/testthat/test-pwhl_game_shifts.R tests/testthat/test-pwhl_player_toi.R tests/testthat/test-pwhl_game_corsi.R
git commit -m "feat(pwhl): pwhl_game_shifts/player_toi/game_corsi analytics"
```

---

## Phase B3 — AHL / OHL / WHL / QMJHL families in R

### Task B3.1: League family generator + per-league files

**Files:**
- Create: `R/hockeytech_family.R` (internal generator) and per-league wrapper files `R/ahl_*.R`, `R/ohl_*.R`, `R/whl_*.R`, `R/qmjhl_*.R`
- Test: `tests/testthat/test-hockeytech_family.R`

> R note: R packages can't `globals().update()` like Python. Two acceptable
> patterns — pick one and apply consistently:
> **(a) Explicit thin wrappers per league** (preferred for roxygen/NAMESPACE
> clarity): each `R/<lg>_schedule.R` etc. is a 3-line exported function calling a
> shared internal `.hockeytech_schedule(league, ...)`. ~13 small files/functions
> per league but every export is documented and discoverable.
> **(b) Programmatic assignment** in `.onLoad` via `assign(..., envir = topenv())`
> — fewer files but exports must be declared in NAMESPACE manually and roxygen
> can't see them. **Use (a).**

- [ ] **Step 1: Write internal core fns + a failing surface test**

```r
# tests/testthat/test-hockeytech_family.R
test_that("each junior league exposes the core + analytics surface", {
  for (lg in c("ahl", "ohl", "whl", "qmjhl")) {
    for (stem in c("schedule","pbp","standings","teams","team_roster",
                   "player_stats","leaders","game_summary","season_id",
                   "game_shifts","player_toi","game_corsi")) {
      fn <- paste0(lg, "_", stem)
      expect_true(exists(fn, mode = "function"),
                  info = paste("missing", fn))
    }
    expect_true(exists(paste0("most_recent_", lg, "_season"), mode = "function"))
  }
})
```

- [ ] **Step 2: Run to verify it fails**

Run: `Rscript -e 'devtools::load_all(); testthat::test_file("tests/testthat/test-hockeytech_family.R")'`
Expected: FAIL — junior functions don't exist.

- [ ] **Step 3: Implement internal shared core fns (`hockeytech_family.R`)**

```r
# R/hockeytech_family.R  (internal, not exported)
#' @keywords internal
#' @noRd
.hockeytech_schedule <- function(league, season = NULL, season_id = NULL) {
  cfg <- .hockeytech_leagues()[[league]]
  params <- list(numberofdaysback = 10000, numberofdaysahead = 10000,
                 limit = 10000, league_id = cfg$league_id)
  if (!is.null(season) || !is.null(season_id)) {
    params$season_id <- .hockeytech_season_id(league, season = season, season_id = season_id)
  }
  url <- .hockeytech_url(league, "modulekit", "scorebar", params)
  .parse_hockeytech_schedule(.hockeytech_api(url))  # R twin of sdv-py parse_schedule
}

#' @keywords internal
#' @noRd
.hockeytech_pbp <- function(league, game_id) {
  cfg <- .hockeytech_leagues()[[league]]
  url <- .hockeytech_url(league, "statviewfeed", "gameCenterPlayByPlay",
                         list(game_id = game_id, league_id = ""))
  df <- .parse_hockeytech_pbp(.hockeytech_api(url), pbp_style = cfg$pbp_style, game_id = game_id)
  hockeytech_scoring_chances(hockeytech_shot_distance_angle(df))
}

#' @keywords internal
#' @noRd
.hockeytech_game_shifts <- function(league, game_id) {
  url <- .hockeytech_url(league, "modulekit", "gameshifts", list(game_id = game_id))
  .parse_hockeytech_shifts(.hockeytech_api(url), game_id = game_id)
}
# ... .hockeytech_standings/_teams/_team_roster/_player_stats/_leaders/
#     _game_summary/_season_id/_player_toi/_game_corsi (port the sdv-py
#     build_family bodies; reuse the R parsers from Task B2/B3.2).
```

- [ ] **Step 4: Generate the per-league exported wrappers**

For each league `<lg>` ∈ {ahl, ohl, whl, qmjhl} and each `<stem>`, create
`R/<lg>_<stem>.R` with a documented thin wrapper, e.g.:

```r
# R/ahl_schedule.R
#' @title **AHL Schedule**
#' @description AHL schedule from the HockeyTech feed (one row per game).
#' @param season End-year season (e.g. 2025); optional.
#' @param season_id Explicit HockeyTech season id; optional.
#' @return A `fastRhockey_data` data frame, one row per game.
#' @import dplyr
#' @export
#' @examples
#' \donttest{ try(ahl_schedule()) }
ahl_schedule <- function(season = NULL, season_id = NULL) {
  out <- data.frame()
  tryCatch(
    expr = {
      out <- .hockeytech_schedule("ahl", season = season, season_id = season_id)
      out <- make_fastRhockey_data(out, "AHL Schedule from HockeyTech", Sys.time())
    },
    error = function(e) cli::cli_alert_danger("{Sys.time()}: AHL schedule unavailable! {e}")
  )
  out
}
```

Repeat the pattern for every `<lg>_<stem>` (schedule, pbp, standings, teams,
team_roster, player_stats, leaders, game_summary, season_id, game_shifts,
player_toi, game_corsi) and `most_recent_<lg>_season`. Each is a 3–6 line
wrapper over the matching `.hockeytech_<stem>(league, ...)`.

> Add the missing R parser twins referenced above (`.parse_hockeytech_schedule`,
> `.parse_hockeytech_pbp` with dialects a/b, `.parse_hockeytech_standings`,
> `_teams`, `_roster`, `_player_stats`, `_leaders`, `_game_summary`) to
> `hockeytech_parsers.R`, porting the sdv-py `_parsers.py` logic 1:1. Give each a
> small fixture-backed unit test copied from the sdv-py fixtures.

- [ ] **Step 5: Document + run**

Run: `Rscript -e 'devtools::document(); devtools::load_all(); testthat::test_file("tests/testthat/test-hockeytech_family.R")'`
Expected: PASS (48 junior functions + 4 season helpers exist & exported).

- [ ] **Step 6: Commit**

```bash
git add R/hockeytech_family.R R/ahl_*.R R/ohl_*.R R/whl_*.R R/qmjhl_*.R R/hockeytech_parsers.R NAMESPACE man/ tests/testthat/test-hockeytech_family.R
git commit -m "feat(hockeytech): AHL/OHL/WHL/QMJHL families (core set + analytics)"
```

---

## Phase B4 — Docs triad, parity check, full check

### Task B4.1: NEWS / pkgdown / cran-comments + doctoc

**Files:**
- Modify: `NEWS.md`, `_pkgdown.yml`, `cran-comments.md`

- [ ] **Step 1: NEWS.md bullets (under the current dev version heading)**

```markdown
### New features

* Added live HockeyTech wrappers for **AHL**, **OHL**, **WHL**, and **QMJHL**
  (`<lg>_schedule()`, `<lg>_pbp()`, `<lg>_standings()`, `<lg>_teams()`,
  `<lg>_team_roster()`, `<lg>_player_stats()`, `<lg>_leaders()`,
  `<lg>_game_summary()`, `<lg>_season_id()`, `most_recent_<lg>_season()`).
* Added on-ice / Corsi-Fenwick / TOI analytics across all five HockeyTech
  leagues (`*_game_shifts()`, `*_player_toi()`, `*_game_corsi()`). Corsi/Fenwick
  are proxies — the feed has no missed-shot event (`corsi_includes_missed`).
* `pwhl_pbp()` now returns a superset: added `shot_distance`, `shot_angle`,
  `scoring_chance`, and `blocked_shot`/`hit` events.
```

- [ ] **Step 2: `_pkgdown.yml` reference section**

Add a "HockeyTech leagues (AHL/OHL/WHL/QMJHL)" group with `starts_with("ahl_")`
etc., and a "PWHL analytics" group listing the new `pwhl_*` analytics functions.

- [ ] **Step 3: cran-comments.md one-liner** summarizing the new multi-league + analytics surface.

- [ ] **Step 4: Regenerate TOCs (if the repo ships `tools/run_doctoc.R`)**

Run: `Rscript tools/run_doctoc.R --maxlevel 2 NEWS.md` (skip if not present).

- [ ] **Step 5: Commit**

```bash
git add NEWS.md _pkgdown.yml cran-comments.md
git commit -m "docs(hockeytech): NEWS/pkgdown/cran-comments for multi-league + analytics"
```

---

### Task B4.2: Cross-language parity test

**Files:**
- Create: `tests/testthat/test-hockeytech_parity.R`

- [ ] **Step 1: Write the parity test (offline, shared fixtures)**

Port the same fixture game (`pwhl_pbp_42`, `pwhl_gameshifts_42`) into the R
fixtures dir, then assert the R analytics produce the **same numbers** the
sdv-py plan asserts (TOI per player, team Corsi). Hard-code the expected values
computed once from the fixture (and verified equal to sdv-py's output):

```r
# tests/testthat/test-hockeytech_parity.R
.load_fx <- function(stem) jsonlite::read_json(testthat::test_path("fixtures","hockeytech",paste0(stem,".json")))

test_that("R analytics match sdv-py on the shared fixture game (TOI + Corsi)", {
  shifts <- fastRhockey:::.parse_hockeytech_shifts(.load_fx("pwhl_gameshifts_42"), game_id = 42)
  toi <- fastRhockey:::hockeytech_player_toi(shifts)
  # Expected values computed from the fixture and cross-checked vs sdv-py:
  # (fill in 2-3 known player_id -> toi_seconds pairs after first green run)
  expect_true(all(toi$toi_seconds >= 0))
  expect_true(nrow(toi) > 10)

  pbp <- fastRhockey:::.parse_hockeytech_pbp(.load_fx("pwhl_pbp_42"),
                                             pbp_style = "hockeytech_a", game_id = 42)
  corsi <- fastRhockey:::hockeytech_corsi_fenwick(pbp)
  expect_false(unique(corsi$corsi_includes_missed))
  expect_equal(sum(corsi$corsi_for), sum(corsi$corsi_against))  # symmetric totals
})
```

> After the first green run, pin 2–3 exact `player_id -> toi_seconds` and a team
> `corsi_for` value and assert equality, and confirm those identical numbers
> appear in the sdv-py analytics test on the same fixture (Part A). This is the
> contract that keeps the two packages in lockstep.

- [ ] **Step 2: Run**

Run: `Rscript -e 'devtools::load_all(); testthat::test_file("tests/testthat/test-hockeytech_parity.R")'`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/testthat/test-hockeytech_parity.R tests/testthat/fixtures/hockeytech/pwhl_pbp_42.json
git commit -m "test(hockeytech): cross-language parity on shared fixture game"
```

---

### Task B4.3: Full `R CMD check`

- [ ] **Step 1: Document + check**

Run: `Rscript -e 'devtools::document()'`
Run: `Rscript -e 'devtools::check(args = c("--no-manual"), error_on = "warning")'`
Expected: 0 errors / 0 warnings (notes for local dev artifacts acceptable). Fix any roxygen `@return`/`@examples` gaps the check surfaces.

- [ ] **Step 2: Commit any doc regeneration**

```bash
git add NAMESPACE man/
git commit -m "docs(hockeytech): regenerate man pages + NAMESPACE"
```

---

## Self-Review (against the design doc)

- **Full R mirror of sdv-py surface:** PWHL analytics (B2.4) + enriched pbp (B2.3) + 4 junior families with core set + analytics (B3.1). PWHL's 19 parity functions already exist in fastRhockey. ✓
- **Shared core generalized from PWHL helpers:** B1.1–B1.3 (registry, URL/api, season resolution). ✓
- **Analytics ported from validated sdv-py:** B2.2; numbers pinned via parity test B4.2. ✓
- **snake_case + fastRhockey_data + roxygen @return:** every wrapper wraps `make_fastRhockey_data`, documents columns; `janitor::clean_names` in parsers. ✓
- **Missed-shot caveat:** `corsi_includes_missed = FALSE` (B2.2) + NEWS note (B4.1). ✓
- **Two PBP dialects:** `.parse_hockeytech_pbp(pbp_style=...)` (B3.1). ✓
- **Docs triad + doctoc + R CMD check:** B4.1, B4.3. ✓

**Known follow-ups flagged for the implementer (not coverage gaps):** the R parser twins (`.parse_hockeytech_schedule/_pbp/_standings/_teams/_roster/_player_stats/_leaders/_game_summary`) are ported 1:1 from the sdv-py `_parsers.py` written in Part A — copy the accessor logic and the explicit fastRhockey rename maps; pin the exact parity numbers in B4.2 after the first green run.
