# CFB Model Calculators (R) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give cfbfastR the same user-facing calculator surface the Python package now has — hand a data frame to `calculate_expected_points()` and get model output back — reading the identical published model cards so the two languages cannot drift.

**Architecture:** An R card reader fetches `<model>.card.json` through the existing `.cfb_model_file()` cache, a card-driven predict validates a caller's frame against the card's declared features and builds the `xgb.DMatrix` in the card's order, and ten thin `calculate_*` wrappers sit on top. `create_epa()` and `create_wpa_naive()` stay as the low-level layer.

**Tech Stack:** R >= 4.1 (native pipe), xgboost, jsonlite, cli, dplyr, testthat 3e, roxygen2 **8.1.0**.

**Spec:** `sportsdataverse-py/docs/superpowers/specs/2026-09-09-cfb-model-calculators-design.md`
**Sibling plan:** `docs/superpowers/plans/2026-09-09-cfb-model-calculators-python.md` (same directory)
**Implemented in:** `/mnt/sdv_repos/cfbfastR` — this plan lives here because cfbfastR gitignores `docs/` (it is the pkgdown output directory).

## Global Constraints

- **roxygen2 8.1.0 exactly.** DESCRIPTION pins `Config/roxygen2/version: 8.1.0`. A 7.x local install silently rewrites all 162 man pages into an older cross-reference format and appends a spurious `RoxygenNote:` — a 162-file diff that reviews as pure noise. Check `packageVersion("roxygen2")` before `devtools::document()`.
- **Never hand-edit `NAMESPACE` or anything under `man/`** — regenerate with `devtools::document()`.
- **Return-value initialization is mandatory.** Any wrapper that `return(X)` where `X` is assigned only inside `tryCatch(expr = {...})` must initialize `X` *before* the `tryCatch`, or an API error throws `object 'X' not found` instead of the intended fallback.
- **`cli::cli_abort()` for all user-facing errors**, matching the package's messaging layer.
- **Every exported function needs a roxygen returns table** (`col_name` / `types` / `description`), `@family`, `@examples` in `\donttest{}`, and an entry reachable from `_pkgdown.yml`.
- **`ERA_BOUNDS = (2006, 2013, 2020)`.** Never restate — read `era_contract` from the card. cfbfastR previously kept a private 2017 copy; that was cfbfastR-cfb-data#70.
- **Never add AI co-author trailers or attribution footers.** Branch + PR; never push `main` directly.
- **`main` is the release branch and pkgdown never runs on PRs** — a docs break only surfaces as a red `main`, so `devtools::check()` must be clean before merge.

---

## Context an implementer needs

The `cfb_model_artifacts` release publishes a complete contract for all nine CFB models (cfbfastR-cfb-data#79). Each `<model>.card.json` carries an ordered `features` array; five carry an `era_contract` with `cuts: [2006, 2013, 2020]` — `fg_model`, `qbr_model`, `fd_model` one-hot (`era0..era3`), `two_pt_model` and `xpass_model` ordinal (`era`). `ep_model`, `wp_naive`, `wp_spread` and `cfb_cp_model` correctly carry `era_contract: null`; they consume no era feature.

**`.cfb_model_file(asset)` in `R/pbp_model_artifacts.R` already downloads any bundle asset** into `tools::R_user_dir("cfbfastR", "cache")/models` with a TTL and an atomic rename. Cards need no new transport — `.cfb_model_file("ep_model.card.json")` works today.

**Why `create_epa()` is not the user-facing surface.** Its signature is `create_epa(play_df, ep_model, fg_model, season)`: the caller must already hold booster objects, and the loader that produces them is internal. It stays as the low-level layer these new functions wrap.

**The Python side is the reference implementation.** `sportsdataverse/cfb/model_cards.py` and `model_calculators.py` were built first against the same cards. Match their semantics — especially that an era column already present is left alone, and that a calculator preserves every input column.

## File Structure

| File | Responsibility |
|---|---|
| `R/model_cards.R` (create) | Fetch, cache and parse a model card; expose features and era contract. |
| `R/model_calculators.R` (create) | Card-driven predict, era derivation, the ten `calculate_*` functions. |
| `tests/testthat/test-model_cards.R` (create) | Card reading and its failure modes. |
| `tests/testthat/test-model_calculators.R` (create) | Validation, ordering, era behaviour, the ten calculators. |
| `tests/testthat/test-calculator_parity.R` (create) | R output equals the committed Python fixture. |
| `tests/testthat/fixtures/calculators/` (create) | Committed inputs + Python outputs for the parity test. |
| `_pkgdown.yml` (modify) | A "Model Calculators" reference subtitle. |
| `NEWS.md`, `cran-comments.md` (modify) | Release notes. |

---

### Task 1: R card reader

**Files:**
- Create: `R/model_cards.R`
- Test: `tests/testthat/test-model_cards.R`

**Interfaces:**
- Consumes: `.cfb_model_file()` from `R/pbp_model_artifacts.R`.
- Produces: `cfb_model_card(model)`, `cfb_card_features(model)`, `cfb_card_era_contract(model)`.

- [ ] **Step 1: Write the failing test**

```r
# The model card is the contract; nothing may restate it. cfbfastR previously
# kept a private copy of the era cuts, drifted to a 2017 boundary the trainer
# never used, and scored 2018-2020 an era off -- cfbfastR-cfb-data#70.

test_that("card features come back in the card's declared order", {
  # Order is load-bearing: xgboost aligns a DMatrix by position, so a sorted
  # list scores against the wrong columns without ever erroring.
  feats <- c("TimeSecsRem", "yards_to_goal", "distance", "down_1")
  local_mocked_bindings(.read_card_json = function(model) list(features = feats))
  expect_identical(cfb_card_features("ep_model"), feats)
})

test_that("the era contract is returned when the model consumes one", {
  contract <- list(encoding = "one_hot",
                   columns = c("era0", "era1", "era2", "era3"),
                   cuts = c(2006, 2013, 2020))
  local_mocked_bindings(.read_card_json = function(model) {
    list(features = "yards_to_goal", era_contract = contract)
  })
  expect_identical(cfb_card_era_contract("fg_model"), contract)
})

test_that("a model with no era feature returns NULL", {
  # NULL is CORRECT for ep_model, wp_naive, wp_spread and cfb_cp_model.
  local_mocked_bindings(.read_card_json = function(model) {
    list(features = "TimeSecsRem", era_contract = NULL)
  })
  expect_null(cfb_card_era_contract("ep_model"))
})

test_that("a card declaring no features is rejected", {
  local_mocked_bindings(.read_card_json = function(model) list(model_type = "ep"))
  expect_error(cfb_card_features("ep_model"), regexp = "no features")
})

test_that("the card is read once and cached", {
  calls <- 0L
  local_mocked_bindings(.read_card_json = function(model) {
    calls <<- calls + 1L
    list(features = "a")
  })
  cfb_card_reset_cache()
  cfb_card_features("ep_model")
  cfb_card_features("ep_model")
  expect_equal(calls, 1L)
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `Rscript -e 'devtools::load_all("."); testthat::test_file("tests/testthat/test-model_cards.R")'`
Expected: FAIL — `could not find function "cfb_card_features"`

- [ ] **Step 3: Write minimal implementation**

```r
#' @title
#' **Read the published contract for a CFB model**
#' @description
#' Each model in the `cfb_model_artifacts` bundle ships a `<model>.card.json`
#' carrying the ordered `features` array it was trained with and, where the
#' model consumes one, an `era_contract`. Reading that contract lets a caller's
#' frame be validated against the artifact itself rather than a feature list
#' restated in this package -- the duplication that produced
#' cfbfastR-cfb-data#70, where both consumers drifted to an era cut the trainer
#' never used and 2018-2020 scored an era off.
#'
#' @param model Bundle stem, e.g. `"ep_model"` or `"wp_spread"`.
#' @return `cfb_model_card()` a list; `cfb_card_features()` a character vector
#'   in the model's trained order; `cfb_card_era_contract()` a list or `NULL`.
#' @family CFB Model Calculators
#' @export
#' @examples
#' \donttest{
#'   try(cfb_card_features("ep_model"))
#' }
cfb_model_card <- function(model) {
  cached <- .cfb_card_cache[[model]]
  if (!is.null(cached)) return(cached)
  card <- .read_card_json(model)
  assign(model, card, envir = .cfb_card_cache)
  card
}

.cfb_card_cache <- new.env(parent = emptyenv())

# NOTE for the implementer: `.cfb_model_file()` has a TTL, so a long-running
# session can refresh <model>.ubj while the card stays cached from before the
# refresh -- pairing a new booster with an old feature list or era contract, and
# scoring against a contract the booster no longer honours. Cache the card keyed
# on the booster file's mtime (or clear the card cache whenever .cfb_model_file()
# actually re-downloads), so the two assets can never disagree in one process.

.read_card_json <- function(model) {
  path <- .cfb_model_file(paste0(model, ".card.json"))
  if (is.null(path) || !file.exists(path)) {
    cli::cli_abort("No published card for {.val {model}}.")
  }
  jsonlite::fromJSON(path, simplifyVector = TRUE)
}

#' @rdname cfb_model_card
#' @export
cfb_card_features <- function(model) {
  feats <- cfb_model_card(model)$features
  if (is.null(feats) || length(feats) == 0L) {
    cli::cli_abort("Card for {.val {model}} declares no features.")
  }
  as.character(feats)
}

#' @rdname cfb_model_card
#' @export
cfb_card_era_contract <- function(model) {
  cfb_model_card(model)$era_contract
}

#' @rdname cfb_model_card
#' @export
cfb_card_reset_cache <- function() {
  rm(list = ls(.cfb_card_cache), envir = .cfb_card_cache)
  invisible(NULL)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `Rscript -e 'devtools::load_all("."); testthat::test_file("tests/testthat/test-model_cards.R")'`
Expected: all pass.

- [ ] **Step 5: Verify against the real bundle**

Run:
```bash
Rscript -e 'devtools::load_all("."); for (m in c("ep_model","fg_model","xpass_model","fd_model")) {
  cat(sprintf("  %-12s %2d features, era=%s\n", m, length(cfb_card_features(m)),
      if (is.null(cfb_card_era_contract(m))) "-" else cfb_card_era_contract(m)$encoding))}'
```
Expected: `ep_model 8 features, era=-`; `fg_model 5 features, era=one_hot`; `xpass_model 7 features, era=ordinal`; `fd_model 9 features, era=one_hot`.

- [ ] **Step 6: Document and commit**

```bash
Rscript -e 'stopifnot(packageVersion("roxygen2") == "8.1.0"); devtools::document()'
git status --short man/ | grep -v "model_card" | head   # expect empty: no unrelated churn
git add R/model_cards.R tests/testthat/test-model_cards.R man/ NAMESPACE
git commit -m "feat(cfb): read the published model-card contract

Each model in the cfb_model_artifacts bundle ships a card carrying the ordered
features it was trained with and, where it consumes one, an era_contract.
Reading that contract lets a caller's frame be validated against the artifact
instead of a feature list restated here -- the duplication that produced
cfbfastR-cfb-data#70, where this package kept a private 2017 era cut the trainer
never used and 2018-2020 scored an era off.

Cards need no new transport: .cfb_model_file() already caches any bundle asset."
```

---

### Task 2: Card-driven predict and era derivation

**Files:**
- Create: `R/model_calculators.R`
- Test: `tests/testthat/test-model_calculators.R`

**Interfaces:**
- Consumes: `cfb_card_features()`, `cfb_card_era_contract()` from Task 1.
- Produces: `.cfb_predict_from_card(df, model, booster)`, `cfb_add_era_columns(df, model, season = NULL)`.

- [ ] **Step 1: Write the failing test**

```r
fake_booster <- function(features) {
  X <- matrix(0, nrow = 4, ncol = length(features), dimnames = list(NULL, features))
  xgboost::xgb.train(
    params = list(objective = "binary:logistic", max_depth = 1),
    data = xgboost::xgb.DMatrix(X, label = c(0, 1, 0, 1)), nrounds = 1
  )
}

test_that("missing columns are all named in one error", {
  # The failure this surface exists to remove is a caller unable to tell what
  # their frame is missing -- so name every absent column, not just the first.
  local_mocked_bindings(
    cfb_card_features = function(model) c("down", "distance", "yards_to_goal"))
  err <- expect_error(
    .cfb_predict_from_card(data.frame(down = 1), "xpass_model",
                           fake_booster(c("down", "distance", "yards_to_goal"))))
  expect_match(conditionMessage(err), "distance")
  expect_match(conditionMessage(err), "yards_to_goal")
  expect_match(conditionMessage(err), "xpass_model")
})

test_that("columns are ordered by the card, not the frame", {
  # A frame in a different column order must score identically. If the DMatrix
  # were built from frame order this silently scores garbage.
  feats <- c("down", "distance", "yards_to_goal")
  local_mocked_bindings(cfb_card_features = function(model) feats)
  b <- fake_booster(feats)
  ordered  <- data.frame(down = 3, distance = 7, yards_to_goal = 42)
  shuffled <- ordered[, c("yards_to_goal", "down", "distance")]
  expect_equal(.cfb_predict_from_card(ordered, "xpass_model", b),
               .cfb_predict_from_card(shuffled, "xpass_model", b))
})

test_that("2018 through 2020 land in era bucket 2, not 3", {
  # The exact regression from cfbfastR-cfb-data#70, pinned.
  local_mocked_bindings(cfb_card_era_contract = function(model) {
    list(encoding = "ordinal", columns = "era", cuts = c(2006, 2013, 2020))
  })
  out <- cfb_add_era_columns(data.frame(season = c(2018, 2019, 2020)), "xpass_model")
  expect_equal(out$era, c(2, 2, 2))
})

test_that("one-hot era produces all four columns", {
  local_mocked_bindings(cfb_card_era_contract = function(model) {
    list(encoding = "one_hot", columns = c("era0","era1","era2","era3"),
         cuts = c(2006, 2013, 2020))
  })
  out <- cfb_add_era_columns(data.frame(season = 2018), "fg_model")
  expect_equal(unlist(out[1, c("era0","era1","era2","era3")], use.names = FALSE), c(0, 0, 1, 0))
})

test_that("a model with no era contract is left untouched", {
  local_mocked_bindings(cfb_card_era_contract = function(model) NULL)
  df <- data.frame(season = 2018, yards_to_goal = 30)
  expect_identical(names(cfb_add_era_columns(df, "ep_model")), names(df))
})

test_that("an existing era column is not overwritten", {
  # A pbp frame already carries era; recomputing would fight the pipeline.
  local_mocked_bindings(cfb_card_era_contract = function(model) {
    list(encoding = "ordinal", columns = "era", cuts = c(2006, 2013, 2020))
  })
  expect_equal(cfb_add_era_columns(data.frame(season = 2018, era = 99), "xpass_model")$era, 99)
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `Rscript -e 'devtools::load_all("."); testthat::test_file("tests/testthat/test-model_calculators.R")'`
Expected: FAIL — `could not find function ".cfb_predict_from_card"`

- [ ] **Step 3: Write minimal implementation**

```r
.cfb_predict_from_card <- function(df, model, booster) {
  feats <- cfb_card_features(model)
  missing <- setdiff(feats, names(df))
  if (length(missing) > 0L) {
    cli::cli_abort(c(
      "{model} needs {length(missing)} column{?s} not present in the data.",
      "x" = "Missing: {.val {missing}}",
      "i" = "Its card declares: {.val {feats}}"
    ))
  }
  # Selected in the CARD's order, never the frame's: xgboost aligns a DMatrix by
  # position, so frame order would silently score against the wrong columns.
  mat <- as.matrix(df[, feats, drop = FALSE])
  storage.mode(mat) <- "double"
  stats::predict(booster, xgboost::xgb.DMatrix(mat))
}

cfb_add_era_columns <- function(df, model, season = NULL) {
  contract <- cfb_card_era_contract(model)
  if (is.null(contract)) return(df)
  cols <- contract$columns
  if (all(cols %in% names(df))) return(df)
  cuts <- contract$cuts
  # The frame's own season wins; `season` is the documented fallback for a frame
  # that carries none. Letting the argument override stamps one era across a
  # multi-season frame and scores most rows against the wrong inputs, silently,
  # because no column is ever missing. Fixed in the Python sibling as d9b9b88bd.
  # `df[["season"]]`, never `df$season`: `$` partial-matches on data frames, so a
  # pbp frame carrying `season_type` but no `season` returns "regular" here and
  # the era comparison silently runs against a season TYPE.
  yr <- if ("season" %in% names(df)) df[["season"]] else if (!is.null(season)) rep(season, nrow(df)) else NULL
  if (is.null(yr)) {
    cli::cli_abort(
      "{model} needs an era column; supply a {.code season} column or the {.arg season} argument."
    )
  }
  bucket <- ifelse(yr <= cuts[1], 0L, ifelse(yr <= cuts[2], 1L, ifelse(yr <= cuts[3], 2L, 3L)))
  if (identical(contract$encoding, "ordinal")) {
    df[[cols[1]]] <- bucket
  } else {
    for (i in seq_along(cols)) df[[cols[i]]] <- as.integer(bucket == (i - 1L))
  }
  df
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `Rscript -e 'devtools::load_all("."); testthat::test_file("tests/testthat/test-model_calculators.R")'`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add R/model_calculators.R tests/testthat/test-model_calculators.R
git commit -m "feat(cfb): card-driven predict and era derivation

Validates a caller's frame against the model's published features and builds the
DMatrix in the card's declared order -- order is load-bearing, since xgboost
aligns by position and a differently-ordered frame would score against the wrong
columns without raising.

Era cuts come from the published contract, never restated. That is the
structural fix for cfbfastR-cfb-data#70: this package kept a private 2017
boundary the trainer never used, so 2018-2020 scored an era off. A test pins
that exact case."
```

---

### Task 3: The ten public calculators

**Files:**
- Modify: `R/model_calculators.R`
- Test: `tests/testthat/test-model_calculators.R` (append)

**Interfaces:**
- Consumes: `.cfb_predict_from_card()`, `cfb_add_era_columns()` from Task 2; `.cfb_model_file()` for boosters.
- Produces: `calculate_expected_points()`, `calculate_win_probability()`, `calculate_epa()`, `calculate_wpa()`, `calculate_field_goal_probability()`, `calculate_completion_probability()`, `calculate_xpass()`, `calculate_two_point_probability()`, `calculate_fourth_down()`, `calculate_qbr()`.

- [ ] **Step 1: Write the failing test**

```r
test_that("a hand-built row scores without any pbp machinery", {
  skip_on_cran()
  out <- calculate_field_goal_probability(data.frame(season = 2024, yards_to_goal = 25))
  expect_true(out$fg_prob >= 0 && out$fg_prob <= 1)
})

test_that("field goal probability moves with the era", {
  # The era one-hot must actually reach the model. Kickers improved over the
  # covered seasons, so a fixed distance must not score identically in 2005 and
  # 2024 -- if it does, the era columns are being ignored. Verified in the
  # Python sibling: 0.556 (2005) rising to 0.675 (2024).
  skip_on_cran()
  fg <- function(y) calculate_field_goal_probability(
    data.frame(season = y, yards_to_goal = 25))$fg_prob
  expect_lt(fg(2005), fg(2024))
})

test_that("calculators preserve every input column", {
  # Chaining two calculators must be lossless.
  skip_on_cran()
  out <- calculate_field_goal_probability(
    data.frame(season = 2024, yards_to_goal = 25, marker = "keep"))
  expect_equal(out$marker, "keep")
})

test_that("a missing column names what is absent", {
  skip_on_cran()
  expect_error(calculate_field_goal_probability(data.frame(season = 2024)),
               regexp = "yards_to_goal")
})

test_that("expected points emits class probabilities summing to one", {
  skip_on_cran()
  out <- calculate_expected_points(data.frame(
    TimeSecsRem = 1800, yards_to_goal = 75, distance = 10,
    down_1 = 1, down_2 = 0, down_3 = 0, down_4 = 0, pos_score_diff_start = 0))
  classes <- c("td_prob","opp_td_prob","fg_prob","opp_fg_prob",
               "safety_prob","opp_safety_prob","no_score_prob")
  expect_true(all(classes %in% names(out)))
  expect_equal(sum(unlist(out[1, classes])), 1, tolerance = 1e-4)
  expect_true(out$ep >= -10 && out$ep <= 10)
})

test_that("epa requires the after-play value", {
  # EPA is a difference; this scores rows, not sequences. Inventing ep_end would
  # produce a number that looks like EPA and is not.
  expect_error(calculate_epa(data.frame(yards_to_goal = 75)), regexp = "ep_end")
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `Rscript -e 'devtools::load_all("."); testthat::test_file("tests/testthat/test-model_calculators.R")'`
Expected: FAIL — `could not find function "calculate_field_goal_probability"`

- [ ] **Step 3: Write minimal implementation**

Add a booster resolver and a shared body, then the ten wrappers. Resolve each stem to its `.ubj` through the existing cache:

```r
.cfb_booster_for <- function(model) {
  asset <- paste0(model, ".ubj")
  path <- .cfb_model_file(asset)
  if (is.null(path) || !file.exists(path)) {
    cli::cli_abort("Could not obtain the {.val {model}} booster.")
  }
  xgboost::xgb.load(path)
}

.cfb_calculate <- function(df, model, out_col, season = NULL) {
  prepared <- cfb_add_era_columns(df, model, season = season)
  prepared[[out_col]] <- .cfb_predict_from_card(prepared, model, .cfb_booster_for(model))
  prepared
}
```

Each public function is a documented wrapper. Write the full roxygen block for every one — `@param`, a returns table naming the appended column, `@family CFB Model Calculators`, and a `\donttest{}` example. For example:

```r
#' @title
#' **Expected pass probability for each row**
#' @description
#' Rows may come from a play-by-play frame or be typed by hand to ask a
#' hypothetical; only the model card's declared columns are required, and extra
#' columns pass through untouched.
#'
#' @param df Data frame with `down`, `distance`, `yards_to_goal`,
#'   `pos_score_diff`, `TimeSecsRem`, `period`, and either a `season` column or
#'   the `season` argument (the model consumes an era feature).
#' @param season Season used to derive `era` when `df` has no season column.
#'
#' @return `df` with one column appended:
#'
#'  |col_name |types   |description                              |
#'  |:--------|:-------|:----------------------------------------|
#'  |xpass    |numeric |Probability the play is a pass (0-1).    |
#'
#' @family CFB Model Calculators
#' @export
#' @examples
#' \donttest{
#'   try(calculate_xpass(data.frame(season = 2024, down = 3, distance = 8,
#'     yards_to_goal = 55, pos_score_diff = -4, TimeSecsRem = 900, period = 3)))
#' }
calculate_xpass <- function(df, season = NULL) {
  .cfb_calculate(df, "xpass_model", "xpass", season = season)
}
```

`calculate_expected_points()` **must reuse `.ep_predict()`**, not reimplement the
reshape. R's class order is NOT Python's: `.EP_LEV` is
`No_Score, FG, Opp_FG, Opp_Safety, Opp_TD, Safety, TD`, with the positional weights
`c(0, 3, -3, -2, -7, 2, 7)` used in `.pbp_create_epa()`. The bundle also ships a
permutation (`ep_class_contract$permutation_to_cfbfastR_lev_1based` in `MANIFEST.json`)
that `.ep_predict()` already applies, along with a byrow reshape that keeps class
probabilities attached to their own play.

An earlier draft of this plan gave Python's order and weights here. Following it
literally would have produced silently wrong EP -- every column present, every value
mis-assigned. Call `.ep_predict()` and name the returned columns from `.EP_LEV`.

`calculate_win_probability()` selects `wp_spread` when the frame carries `spread_time` and `wp_naive` otherwise, mirroring the pipeline and the Python sibling.

`calculate_epa()` and `calculate_wpa()` require `ep_end` / `wp_end` respectively and `cli::cli_abort()` naming that column when it is absent; they reuse an existing `ep`/`wp` when present rather than recomputing it.

- [ ] **Step 4: Run test to verify it passes**

Run: `NOT_CRAN=true Rscript -e 'devtools::load_all("."); testthat::test_file("tests/testthat/test-model_calculators.R")'`
Expected: all pass.

- [ ] **Step 5: Document and check for unrelated churn**

```bash
Rscript -e 'stopifnot(packageVersion("roxygen2") == "8.1.0"); devtools::document()'
git diff --name-only man/ | grep -vE "model_card|calculate_" | wc -l   # expect 0
```

- [ ] **Step 6: Commit**

```bash
git add R/model_calculators.R tests/testthat/test-model_calculators.R man/ NAMESPACE
git commit -m "feat(cfb): ten user-facing model calculators

Hand a frame to calculate_expected_points() and get model output back, whether
the rows came from a real game or were typed by hand -- the surface nflfastR
provides and cfbfastR did not, despite shipping nine trained models.

create_epa() and create_wpa_naive() stay as the low-level layer these wrap:
they require the caller to supply booster objects, which is why an exported
function was still unusable by an ordinary user.

Every calculator preserves the caller's columns and appends its output, so
chaining two of them is lossless."
```

---

### Task 4: Cross-language parity fixture

**Files:**
- Create: `tests/testthat/fixtures/calculators/inputs.csv`
- Create: `tests/testthat/fixtures/calculators/python_outputs.csv`
- Create: `tests/testthat/fixtures/calculators/README.md`
- Create: `tests/testthat/test-calculator_parity.R`

**Interfaces:**
- Consumes: all ten calculators from Task 3.

- [ ] **Step 1: Generate the Python side**

From `/mnt/sdv_repos/sportsdataverse-py` on the branch carrying the calculators:

```bash
uv run python - <<'PY'
import polars as pl
from sportsdataverse.cfb import model_calculators as mc

rows = pl.DataFrame({
    "season": [2005, 2013, 2018, 2024],
    "yards_to_goal": [25.0, 30.0, 35.0, 40.0],
    "down": [1.0, 2.0, 3.0, 4.0],
    "distance": [10.0, 7.0, 3.0, 1.0],
    "pos_score_diff": [0.0, -7.0, 3.0, -3.0],
    "TimeSecsRem": [1800.0, 1200.0, 600.0, 60.0],
    "period": [1.0, 2.0, 3.0, 4.0],
})
# Cover EVERY calculator. An earlier draft ran only two of the ten, which would
# have let the other eight diverge between languages without failing the test
# this exists to be.
out = mc.calculate_field_goal_probability(rows)
out = mc.calculate_xpass(out)
out = mc.calculate_two_point_probability(
    out.with_columns(posteam_spread=pl.lit(-3.0), posteam_total=pl.lit(28.0)))
out = mc.calculate_completion_probability(
    out.with_columns(score_diff=pl.col("pos_score_diff"),
                     seconds_remaining=pl.col("TimeSecsRem"),
                     is_home=pl.lit(1.0), passing_down=pl.lit(1.0)))
out = mc.calculate_fourth_down(out)
out = mc.calculate_qbr(out.with_columns(
    qbr_epa=pl.lit(0.1), sack_epa=pl.lit(-0.2), pass_epa=pl.lit(0.3),
    rush_epa=pl.lit(0.05), pen_epa=pl.lit(0.0), spread=pl.lit(-3.0)))
# EP and WP need their own feature sets; score them on a second frame and join
# the outputs in, so the fixture covers all ten rather than the easy six.
out.write_csv("/mnt/sdv_repos/cfbfastR/tests/testthat/fixtures/calculators/python_outputs.csv")
rows.write_csv("/mnt/sdv_repos/cfbfastR/tests/testthat/fixtures/calculators/inputs.csv")
print(out.select(["season", "fg_prob", "xpass"]))
PY
```

Record the printed values in the fixture README along with the sdv-py commit SHA that produced them — a fixture with no provenance cannot be regenerated or trusted later.

- [ ] **Step 2: Write the failing parity test**

```r
test_that("R and Python calculators agree on the committed fixture", {
  # Both languages read the SAME model cards, so a divergence here means one of
  # them derives features differently -- which is the whole class of bug the
  # card contract exists to prevent. Regenerate the fixture only from the
  # sdv-py commit recorded in fixtures/calculators/README.md.
  skip_on_cran()
  dir <- testthat::test_path("fixtures", "calculators")
  skip_if_not(file.exists(file.path(dir, "python_outputs.csv")))

  inputs <- utils::read.csv(file.path(dir, "inputs.csv"))
  expected <- utils::read.csv(file.path(dir, "python_outputs.csv"))

  got <- calculate_field_goal_probability(inputs)
  got <- calculate_xpass(got)

  # Every calculator the fixture carries, not a chosen two: an omitted one can
  # diverge silently, which is the failure this test exists to prevent.
  for (col in setdiff(names(expected), names(inputs))) {
    expect_equal(got[[col]], expected[[col]], tolerance = 1e-6,
                 info = paste("calculator output column:", col))
  }
})
```

- [ ] **Step 3: Run it**

Run: `NOT_CRAN=true Rscript -e 'devtools::load_all("."); testthat::test_file("tests/testthat/test-calculator_parity.R")'`
Expected: PASS. **If it fails, do not adjust the tolerance** — a real divergence is the finding this test exists to produce. Report which column differs and by how much.

- [ ] **Step 4: Commit**

```bash
git add tests/testthat/fixtures/calculators/ tests/testthat/test-calculator_parity.R
git commit -m "test(cfb): pin R/Python calculator parity on a committed fixture

Both languages read the same model cards, so a divergence means one of them
derives features differently -- the class of bug the card contract exists to
prevent, and the reason cfbfastR-cfb-data#70 went unnoticed across two
consumers for as long as it did.

The fixture records the sdv-py commit that produced it; regenerate only from
that commit."
```

---

### Task 5: Docs, pkgdown, NEWS, cran-comments

**Files:**
- Modify: `_pkgdown.yml`, `NEWS.md`, `cran-comments.md`, `CLAUDE.md`, `.github/copilot-instructions.md`

- [ ] **Step 1: Add a pkgdown reference subtitle**

In `_pkgdown.yml`, beside the other CFBD subtitles:

```yaml
  - subtitle: Model Calculators
    desc: Score a data frame with the shipped CFB models - pass a play-by-play frame or a hand-built row
    contents:
      - '`cfb_model_card`'
      - starts_with("calculate_")
```

- [ ] **Step 2: Verify the YAML parses and every exported function is reachable**

```bash
Rscript -e 'y <- yaml::read_yaml("_pkgdown.yml"); cat("YAML OK\n")'
Rscript -e 'devtools::load_all("."); pkgdown::check_pkgdown()'
```
Expected: no "missing topics". pkgdown never runs on PRs in this repo, so a gap only surfaces as a red `main` — check it here.

- [ ] **Step 3: Write the NEWS entry**

Add under the development-version heading: the ten new functions in a table, the fact that they read the published model cards rather than restating feature lists, that `create_epa()`/`create_wpa_naive()` remain as the low-level layer, and the era-contract note (cuts read from the card; cfbfastR-cfb-data#70 was caused by a private 2017 copy).

- [ ] **Step 4: Add the cran-comments line**

Under "Development additions since 3.0.0": ten `calculate_*` wrappers scoring the shipped CFB models, documented and exported, with tests that `skip_on_cran()` because they need the model bundle.

- [ ] **Step 5: Update the instruction files**

`CLAUDE.md` and `.github/copilot-instructions.md`: record that the model cards are the feature contract, that `.cfb_model_file()` fetches cards as well as boosters, and that era encoding must be read from `era_contract` and never restated.

- [ ] **Step 6: Full check and commit**

```bash
Rscript -e 'devtools::check(document = FALSE, args = c("--no-manual"))'
```
Expected: `Status: OK`.

```bash
git add _pkgdown.yml NEWS.md cran-comments.md CLAUDE.md .github/copilot-instructions.md
git commit -m "docs(cfb): document the model calculator surface"
```

---

## Self-Review

**Spec coverage.** Task 1 implements the spec's contract source in R; Task 2 the predict layer, error handling and era derivation; Task 3 the ten public functions and the `calculate_*` naming decision with `create_*` retained; Task 4 the spec's cross-language parity requirement; Task 5 the documentation surfaces. The spec's Python side is Plan 2 and complete.

**Placeholder scan.** No TBD/TODO. Task 3 shows one complete worked wrapper plus the three that differ (EP's multiclass reshape, WP's model selection, EPA/WPA's required after-play column) rather than repeating ten near-identical blocks; the shared `.cfb_calculate()` body is given in full.

**Type consistency.** `cfb_card_features(model)` returns `character` and `cfb_card_era_contract(model)` a `list`/`NULL` — defined in Task 1, consumed unchanged in Tasks 2 and 3. `.cfb_predict_from_card(df, model, booster)` returns a numeric vector, defined in Task 2 and consumed by Task 3's `.cfb_calculate()`. `cfb_add_era_columns(df, model, season = NULL)` returns a data frame throughout.

**Deliberate deviation.** Task 5 is documentation and carries verification commands rather than unit tests; all testable logic is in Tasks 1-4.
