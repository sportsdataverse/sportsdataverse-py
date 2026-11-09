<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Lane 10 rank sample gates: split-half reliability curves](#lane-10-rank-sample-gates-split-half-reliability-curves)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Lane 10 rank sample gates: split-half reliability curves

The curves `tests/tools/test_fit_rate_gates.py` re-fits to pin
`sportsdataverse._common.rate_gates.TEAM_MIN_N` and `ON_OFF_MIN_N`. Each file is
`tools/fit_rate_gates.py --league <lg> --write`, run on 2026-11-06 against the
sportsdataverse-data releases. Nothing is synthetic: every point is a correlation of real
odd-game and even-game rates.

| File | Rows | Source | Kept |
|---|---:|---|---|
| `{nba,wnba,mbb,wbb}_fits.csv` | 40 each | `team`: ESPN team box scores (`load_{nba,wnba,mbb,wbb}_team_boxscore`, regular season, `season_type == 2`). `on_off`: the BB-D6 game impact box (`nba_game_player_impact`, `mbb_game_player_impact`) | One row per `(kind, rate, span)`. `kind` is `team` (the 14 team and lineup rates) or `on_off` (the 6 ranked on/off ratings). `span` is `train` or `holdout`, and `seasons` gives its END-year range (the `SPANS` of the tool). `m` is the fitted r = 0.5 sample (`inf` when it would sit past twice the curve's largest `n`: the curve never comes near 0.5, so a finite value would be an extrapolation). `curve` is `n:r:entities;...`, where `n` is the sample in the table's `_n` units, `r` the split-half correlation and `entities` the team-seasons (`team`) or player-team-seasons (`on_off`) at that point. |

Seasons: team NBA 2015-2023 train / 2024-2026 holdout, WNBA 2015-2022 / 2023-2025, MBB and WBB
2022-2024 / 2025-2026. On/off NBA 2019-2023 / 2024-2026, WNBA 2018-2022 / 2023-2025, MBB and WBB
2022-2024 / 2025-2026.

Team possessions are the box estimate `FGA + 0.44 FTA - OREB + TOV`, averaged over both teams. The
table's `net_rtg_n` counts both sides, so the `net_rtg` curve's `n` is twice that. `pace`'s `n`
is games. Ids never leave the tool: a curve holds only counts and correlations.
