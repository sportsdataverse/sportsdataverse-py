<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [`cfb_2025_ratings`: real 2025 FBS ratings and postseason games](#cfb_2025_ratings-real-2025-fbs-ratings-and-postseason-games)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# `cfb_2025_ratings`: real 2025 FBS ratings and postseason games

Byte-identical copies of cfbseedR's `tests/testthat/fixtures/cfb_2025_ratings/` CSVs
(copied from git with LF line endings), read by `tests/cfb/test_cfb_ratings_compute_results.py`.

- **`ratings.csv`**: one row per FBS team (136), its end-of-regular-season `games`
  (FBS-vs-FBS games played) and three ratings: `points` (ridge least-squares margin rating),
  `raw_epa` (net EPA per play) and `adj_epa` (opponent-adjusted net EPA per play). Built by
  cfbseedR's `data-raw/cfb_generator_constants.R` from the cfbfastR schedule and play-by-play
  release assets for 2025.
- **`games.csv`**: the 47 games from week 16 (Army-Navy) through the postseason, engine schema,
  results blanked.
- **`expected.csv`**: cfbseedR's expected home margin and margin SD for every game, per rating
  type and per `variant` of the `games` column: `full` (as recorded), `half` (`games // 2`),
  `quarter` (`games // 4`) and `missing` (Georgia Southern dropped from `ratings`, Navy's `games`
  null). Written by cfbseedR's `capture.R` with 10 decimals; compare with an absolute tolerance
  of about 1e-9.

The `adj_epa` column is the fitting script's own adjustment, not this package's `adj_net`; the
fixture checks the margin formula, the bucket lookup and the defaults, not the ratings. Rebuild
it in cfbseedR (`Rscript tests/testthat/fixtures/cfb_2025_ratings/capture.R`) and copy the three
CSVs here whenever the defaults change on either side.
