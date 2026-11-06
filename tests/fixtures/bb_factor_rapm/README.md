<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D10 factor-RAPM corpus](#bb-d10-factor-rapm-corpus)
  - [Observed on the full NBA seasons (not in the fixture)](#observed-on-the-full-nba-seasons-not-in-the-fixture)
  - [Standard errors (WNBA 2025, on the fixture)](#standard-errors-wnba-2025-on-the-fixture)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D10 factor-RAPM corpus

Real captures for `tests/nba/test_nba_player_factor_rapm.py` (BB-D10), downloaded from the
sportsdataverse-data releases on 2026-11-05 and projected to the columns the split reads (zstd
level 19). Nothing is synthetic or edited: no row is dropped and columns are only dropped.

| File | Rows | Release asset | Kept |
|---|---:|---|---|
| `wnba_possessions_{2024,2025}.parquet` | 41,429 / 48,401 | `wnba_stats_possessions/wnba_possessions_{2024,2025}` | Every possession of the season, every season type. Cols: `game_id, season, off_player_1..5, def_player_1..5, points, fg2a, fg2m, fg3a, fg3m, oreb, tov`. |
| `wnba_schedule_{2024,2025}.parquet` | 262 / 310 | `wnba_stats_schedules/wnba_schedule_{2024,2025}` | Every game. Cols: `game_id, season, season_type` (regular-season 240 / 286, playoffs 22 / 24). |
| `wnba_player_impact_2024_2025.parquet` | 338 | `wnba_player_impact/wnba_player_impact_{2024,2025}` | The `Regular Season` rows of both seasons, the oracle (157 and 181 players). Cols: `season, player_id, o_rapm, d_rapm, rapm, off_poss, def_poss`. |

Season naming: the WNBA season is its calendar year.

## Observed on the full NBA seasons (not in the fixture)

The NBA possessions release is 1.8 MB a season in this projection (2024-25), so the NBA checks were
run from local copies of the releases and are recorded here:

| Season | Players | Possessions | Recombination max error | vs `nba_player_impact` o_rapm / d_rapm |
|---|---:|---:|---:|---|
| 2024-25 | 569 | 243,735 | 4.9e-12 | corr 0.9978 / 0.9973, max diff 0.43 / 0.37 |
| 2025-26 | 582 | 245,646 | 1.0e-11 | corr 0.9966 / 0.9969, max diff 1.31 / 1.00 |

The NBA differences are input drift, not the fit: on the same possessions the split's `o_rapm` /
`d_rapm` equal `nba_rapm`'s to 5.4e-12, but the possessions release is not the exact frame the
published RAPM was fit on (a player's `off_poss` differs by up to 184 in 2024-25, 104 in 2025-26).

Year over year, 2024-25 to 2025-26, the 293 players over 2,000 possessions in both seasons:
Spearman 0.32 (`rapm`), 0.29 (`o_rapm`), 0.36 (`d_rapm`); the components run from 0.23
(`d_drb_rapm`) to 0.43 (`o_orb_rapm`).

## Standard errors (WNBA 2025, on the fixture)

`<m>_se` is the game-clustered ridge sandwich. A delete-one-game jackknife of the same ridge (286
refits at the season's penalty, done once from these files, 400 s, not a test) gives `o_rapm` / `d_rapm`
SEs a median 1.03 times the sandwich's (1.01 to 1.07) for the 133 players over 1,000 possessions.
Their median `o_rapm_se` is 1.08 and `rapm_se` 1.54 per 100, against a spread (sd) across them of
1.55 (`o_rapm`) and 2.28 (`rapm`): single-season RAPM ranks are noisy, and the components more so.
