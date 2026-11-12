<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Current impact corpus (BB-D39)](#current-impact-corpus-bb-d39)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Current impact corpus (BB-D39)

Real NBA data for `tests/nba/test_nba_current_impact.py` and `tests/tools/test_fit_current_impact.py`.
Nothing is synthetic or edited: rows are only filtered and columns only dropped. Parquet files are zstd
level 19. Seasons are named by their end year (2015 = 2014-15). Player and team ids are Int64 and `game_id` is
a zero-padded string (`0021400001`), as in the sources.

Inputs read on 2026-11-10: the `nba_stats_possessions` release assets 2005-2026 (`nba_possessions_{yr}.parquet`,
pulled 2026-11-08), the hoopR-nba-stats-raw checkout at `5da1f96bf5` (`leaguegamelog/{yr}/regular-season{,_p}.json`
for box lines and game dates) and the BB-D38 run of `tools/fit_box_prior_v2.py` (`model_{yr}.npz`, ages,
height / position), all through `tools/fit_current_impact.py inputs`.

Provenance: the BB-D38 inputs are that PR's final clean run (its code merged as sdv-py `1624574990`; the
`model_{yr}.npz` files are not committed or released: re-create them with `tools/fit_box_prior_v2.py panel`
and `prior`). The BB-D39 `inputs` / `fit` / `gate` / `report` stages ran from this PR's branch code
(2026-11-10 / -11). `fit.json` is the second Nelder-Mead run (a x1.65 start simplex, 80 evaluations): a first
run with scipy's default 5% simplex (it barely moves log 1.0) reached 1.231590 against 1.231571 and was set
aside on the fit objective, before any gate number existed. The tuned baselines were added after the first
gate run, on a reviewer's request; their penalties come from 2007-2013 only (`baseline_fit.json`).

| File | Rows | What |
|---|---:|---|
| `nba_poss_2014_2015_slice.parquet` | 38,465 | `poss_{yr}.parquet` of `inputs`: regular-season possessions of 2013-14 dated 2014-04-01 or later and of 2014-15 dated before 2014-11-08 (203 games). Cols: `game_id, game_date, season, points, offense_team_id, defense_team_id, off_player_1..5, def_player_1..5`. |
| `nba_window_priors_2014_2015_slice.parquet` | 2,577 | The `window` priors of `inputs` at the evaluation dates 2014-04-01, -04-08, -04-15, 2014-10-28 (opening night: last season's box lines) and 2014-11-04. Cols: `player_id, prior_date, o_prior, d_prior`. |
| `gate.parquet` | see `report.json` | `gate`: one row per season 2007-2026, evaluation date and method, with the 28-day forward `sse`, `n` (possessions) and the `fingerprint` of what it was scored with (`fingerprints()`: the filter SDs, the tuned penalty or nothing). Rows scored before the column existed were stamped once from `fit.json` (written 17:08:51, before the first gate run began at 17:08:52) and `baseline_fit.json`; `report` re-ran to an identical `report.json`. |
| `report.json` | | `report`: pooled MSE per method, margins against the season-to-date rating with season-cluster bootstrap SE, ablations, margins by days into the season, `gate_pass`. |
| `fit.json` | | `fit`: the fitted SDs, the objective and the Nelder-Mead trace. |
| `baseline_fit.json` | | `gate`: the tuned baselines' 2007-2013 forward MSE per penalty and the pick (7,500 for both priors). |
