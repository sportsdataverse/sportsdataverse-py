<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [nflseedR golden parity fixtures](#nflseedr-golden-parity-fixtures)
  - [Provenance](#provenance)
  - [Files](#files)
  - [Determinism](#determinism)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# nflseedR golden parity fixtures

Oracle captures for the nflseedR v2 standings/simulation port
(`sportsdataverse/nfl/nfl_season_standings.py`, `sportsdataverse/nfl/nfl_simulations.py`).

## Provenance

- Captured: 2026-07-03
- R: 4.5.3 (2026-03-11 ucrt), Windows
- nflseedR: 2.0.2 (CRAN), nflreadr: 1.5.1
- Command (run from the repo root; `set.seed(4)` was set although the capture
  required no random tiebreaks):

```r
library(nflseedR)
set.seed(4)
sched <- nflreadr::load_schedules(2023)
games <- sched[, c("season","game_type","week","away_team","home_team",
                   "away_score","home_score","result","location",
                   "away_rest","home_rest")]
data.table::fwrite(games, "tests/fixtures/seedr/games_2023.csv")
st <- nflseedR::nfl_standings(games, ranks = "DRAFT",
                              tiebreaker_depth = "SOS", verbosity = "NONE")
data.table::fwrite(st, "tests/fixtures/seedr/standings_2023_draft.csv")
data.table::fwrite(nflseedR::divisions, "tests/fixtures/seedr/divisions.csv")
```

## Files

| File | Rows | Content |
|---|---|---|
| `games_2023.csv` | 285 | Complete 2023 schedule + results (REG + playoffs) from `nflreadr::load_schedules(2023)` |
| `standings_2023_draft.csv` | 32 | `nfl_standings(games, ranks = "DRAFT", tiebreaker_depth = "SOS")` output |
| `divisions.csv` | 36 | `nflseedR::divisions` team/conference/division lookup (source of the `_DIVISIONS` constant) |

## Determinism

The captured standings contain **zero** "Coin Toss" values in
`div_tie_broken_by` / `conf_tie_broken_by` / `draft_tie_broken_by`
(verified at capture time), i.e. every 2023 rank resolves within the SOS
tiebreaker depth and the parity test
(`tests/nfl/test_nfl_standings.py::TestParity2023`) can assert exact
equality on all rank columns. If a future re-capture introduces coin-toss
rows, either regenerate with a different season or exclude those rows with
a comment.
