---
name: port-python-to-r
description: Use when porting Python logic from the SportsDataverse Python package (sdv-py) into a SportsDataverse R package — cfbfastR, hoopR, wehoop, baseballr, fastRhockey, softballR, etc. Enforces a parity-test-first workflow (golden fixture from the Python output → failing testthat test → port → green) plus the tidyverse/data.table idiom map and R package conventions (roxygen2 docs, snake_case, tibble returns, pkgdown reference coverage). Invoke for "port this to cfbfastR", "translate this polars logic to dplyr", "re-implement the sdv-py parser in R", or "bring this Python fix back to the R package".
---

# Port Python (polars) → R, parity-test-first

The reverse of `port-r-to-python`. Same failure mode, same fix: make the divergence a
**failing testthat test before the port**, so a wrong/empty column surfaces immediately
rather than after a downstream join or a pkgdown example breaks.

**Create one todo per numbered step below.**

## 1. Pin the canonical Python source + target R package

Identify the sdv-py function you are porting (read it directly — `sportsdataverse/<sport>/...`)
and the target R package + repo in the workspace:

- `cfbfastR-dev/cfbfastR`, `hoopR-dev/hoopR`, `wehoop-dev/wehoop`,
  `baseball-dev/baseballr`, `hockey-dev/fastRhockey`, `softballR-dev/softballR`.

Note the cross-package naming: R uses snake_case function names (`cfbfastR::cfbd_*`,
`espn_cfb_*`) and returns **tibbles**. Quote the Python function + line range in the
test/roxygen provenance.

## 2. Capture a golden fixture (the Python output)

Run the sdv-py function on a small representative input and persist the output
(`df.write_csv(...)` / `write_parquet`) as the parity oracle. Capture the asserted columns
plus join keys, and record the sdv-py version + inputs alongside it. Place it under the R
package's `tests/testthat/` fixtures area (or a scratch dir for a one-off reconciliation).

## 3. Write the failing testthat test FIRST

Before porting, add a `test_that(...)` that reads the golden fixture and asserts the
(not-yet-written) R output matches — `expect_equal()` for categorical/id columns,
`expect_equal(..., tolerance = ...)` for float model columns. State the tolerance and why.
Run `devtools::test()`; confirm it fails for the right reason.

## 4. Translate idioms — polars/pandas → tidyverse (or data.table)

| polars / pandas idiom | tidyverse R |
|---|---|
| `df.with_columns((...).alias("x"))` | `dplyr::mutate(x = ...)` |
| `pl.when(c1).then(v1)...otherwise(d)` | `dplyr::case_when(c1 ~ v1, ..., TRUE ~ d)` |
| `df.group_by("g").agg(...)` | `dplyr::group_by(g) %>% dplyr::summarise(...)` |
| `pl.col("x").shift(1).over("game_id")` | `dplyr::group_by(game_id) %>% dplyr::mutate(dplyr::lag(x))` |
| `pl.col("x").cum_sum().over("g")` | `dplyr::group_by(g) %>% dplyr::mutate(cumsum(x))` |
| `df.filter(pl.col("c") == True)` | `dplyr::filter(c)` |
| `df.join(o, on="k", how="left")` | `dplyr::left_join(o, by = "k")` |
| `df.join(o, how="full", coalesce=True)` | `dplyr::full_join(o, by="k")` then coalesce |
| `pl.col("s").str.extract(re, g)` | `stringr::str_match(s, re)[, g+1]` |
| `df.pivot(...)` | `tidyr::pivot_wider(...)` |
| `df.melt(...)` / `unpivot` | `tidyr::pivot_longer(...)` |
| `df.with_row_index("i")` | `dplyr::mutate(i = dplyr::row_number() - 1)` |

Hard rules (SDV R conventions — the high-frequency port bugs in this direction):

- **Return a tibble**, snake_case columns, stable column set even on empty input
  (`tibble::tibble(col = character())`), matching the sibling functions in the package.
- **Vectorize, don't loop.** `case_when`/`if_else` over rowwise loops; mirror the
  polars expression structure rather than transcribing an imperative pass.
- **Join-key dtype discipline carries over.** Ids must match type on both sides; an
  integer id stringified as `"123.0"` is the same foot-gun in R — coerce the raw integer.
  Match player names case-insensitively (`stringr::regex(..., ignore_case = TRUE)`).
- **Regex flavor differs.** R's ICU/PCRE regex *does* support lookaround (unlike polars),
  but don't assume a polars pattern is portable verbatim — re-test extraction on the fixture.
- **`NA` vs polars null.** polars null → R `NA` of the right type; watch `NA_real_` vs
  `NA_character_` in `case_when` branches (all branches must share a type).

## 5. Document + wire pkgdown

Every exported R function needs a complete roxygen block: `@param` for each arg, an
`@return` describing the tibble (ideally a column table), and runnable `@examples`. Then:

- `devtools::document()` to regenerate `man/*.Rd` + `NAMESPACE`.
- Add the function to `_pkgdown.yml` reference so it appears on the site.
- Run the `roxygen-doc-reviewer` / `returns-table-auditor` agents to check completeness.

## 6. Go green, then run the R gate

```r
devtools::document()
devtools::test()                 # parity test + package tests
devtools::check()                # R CMD check (or the package's CI-parity target)
```

Match the package's existing lint/style (most SDV R packages follow tidyverse style;
`styler::style_pkg()` if configured). Commit with the package's Conventional-Commit scope
(`feat(cfb): ...`) and **no AI co-author trailer** (SDV ecosystem rule).

## See also

- `port-r-to-python` — the reverse direction (R → sdv-py polars).
- `r-skills:tdd` / `r-skills:r-package-development` — testthat-first R workflow + package structure.
- The target package's `CLAUDE.md` / `CONTRIBUTING.md` is authoritative for its conventions.
