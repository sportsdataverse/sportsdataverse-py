<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D10 factor-RAPM corpus](#bb-d10-factor-rapm-corpus)
  - [Observed on the full seasons (the release assets)](#observed-on-the-full-seasons-the-release-assets)
  - [Why the NBA totals differ from `nba_player_impact`](#why-the-nba-totals-differ-from-nba_player_impact)
  - [The penalty](#the-penalty)
  - [Each part's own RidgeCV penalty](#each-parts-own-ridgecv-penalty)
  - [Year over year](#year-over-year)
  - [Standard errors (WNBA 2025, on the fixture)](#standard-errors-wnba-2025-on-the-fixture)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D10 factor-RAPM corpus

Real captures for `tests/nba/test_nba_player_factor_rapm.py` (BB-D10), downloaded from the
sportsdataverse-data releases on 2026-11-05 and projected to the columns the split reads (zstd
level 19). Nothing is synthetic or edited: no row is dropped and columns are only dropped.

| File | Rows | Release asset | Kept |
|---|---:|---|---|
| `wnba_possessions_{2024,2025}.parquet` | 41,429 / 48,401 | `wnba_stats_possessions/wnba_possessions_{2024,2025}` | Every possession of the season, every season type. Cols: `game_id, season, off_player_1..5, def_player_1..5, points, fg2a, fg2m, fg3a, fg3m, fta, oreb, tov`. |
| `wnba_schedule_{2024,2025}.parquet` | 262 / 310 | `wnba_stats_schedules/wnba_schedule_{2024,2025}` | Every game. Cols: `game_id, season, season_type` (regular-season 240 / 286, playoffs 22 / 24). |
| `wnba_player_impact_2024_2025.parquet` | 338 | `wnba_player_impact/wnba_player_impact_{2024,2025}` | The `Regular Season` rows of both seasons, the oracle (157 and 181 players). Cols: `season, player_id, o_rapm, d_rapm, rapm, off_poss, def_poss`. |

Season naming: the WNBA season is its calendar year.

## Observed on the full seasons (the release assets)

Every number below comes from `tools/models/factor_rapm/factor_rapm_evidence.py`, which reads the
release assets (run 2026-11-05). The NBA possessions release is 1.8 MB a season in this
projection, so it is not a fixture; the live test `test_nba_totals_track_the_published_release`
(`SDV_PY_LIVE_TESTS=1`) holds the NBA comparison.

| Season | Players | Possessions | Recombination max error | vs `player_impact` o_rapm / d_rapm | Remainder non-zero |
|---|---:|---:|---:|---|---:|
| NBA 2024-25 | 569 | 243,735 | 4.9e-12 | r 0.9978 / 0.9973, max diff 0.43 / 0.37 | 0.80% |
| NBA 2025-26 | 582 | 245,646 | 1.0e-11 | r 0.9966 / 0.9969, max diff 1.31 / 1.00 | 1.11% |
| WNBA 2024 | 157 | 38,037 | 4.6e-12 | equal to 4.9e-12, `off_poss` equal | 1.02% |
| WNBA 2025 | 181 | 44,589 | 7.0e-12 | equal to 7.2e-12, `off_poss` equal | 1.02% |

The remainder (`other`) is non-zero on about 1% of possessions; 12.4% / 12.7% of the WNBA ones
have a free-throw attempt.

## Why the NBA totals differ from `nba_player_impact`

Lineups, not possessions. On the same rows the split's `o_rapm` / `d_rapm` equal `nba_rapm`'s to
5.4e-12, and each season's total possessions are the same on both sides (243,735 and 245,646;
the per-player `off_poss` differences, up to 184 in 2024-25 and 104 in 2025-26, sum to zero over
444 and 169 players). So it is neither `count_as_possession` (the release keeps its 586 / 851
`False` rows and the totals still agree, so the impact fit kept them too) nor a filter on heaves:
some possessions sit on different on-floor lineups. The possessions release
assets were last uploaded 2026-08-13 and `nba_player_impact` 2026-10-12: the impact fit ran on a
later re-compile of the possessions in the producer (hoopR-nba-stats-data) that was never
re-published. Re-publishing `nba_stats_possessions` from that compile would close the gap.

## The penalty

The split fits every part at plain RAPM's penalty (so the parts add up): since November 2026 the fixed
league value `nba_rapm.RAPM_PENALTY` (NBA 2,683, WNBA 1,931, fitted by `tools/fit_rapm_penalty.py`), the
same in every season. Before that it was RidgeCV's leave-one-possession-out pick per season over
`np.logspace(2, 5, 8)`, which the `player_impact` releases still carry until they are re-published:
NBA 5,179 for 2024-25 and 1,931 for 2025-26 (so the NBA comparison in the table above is at those
picks), WNBA 1,931 for both seasons here (the fixed value, so the WNBA release still matches).

## Each part's own RidgeCV penalty

Each part's own RidgeCV pick over that grid, beside the shared penalty:

| Season | Shared (points) | efg | tov | orb | ftr | other |
|---|---:|---:|---:|---:|---:|---:|
| NBA 2024-25 | 2,683 | 5,179 | 1,931 | 1,931 | 1,931 | 5,179 |
| NBA 2025-26 | 2,683 | 5,179 | 1,931 | 1,931 | 1,931 | 1,931 |
| WNBA 2024 | 1,931 | 5,179 | 1,931 | 1,931 | 1,931 | 13,895 |
| WNBA 2025 | 1,931 | 5,179 | 1,931 | 5,179 | 5,179 | 100,000 |

Shooting would take a heavier penalty than the shared one in all four seasons, the remainder the
grid's heaviest in WNBA 2025: their shared-penalty values are the least shrunk of the parts.

## Year over year

Spearman of each metric between consecutive seasons, over players above a possession floor in both
(NBA at the fixed 2,683; at the old picks it read 0.320 / 0.291 / 0.364, 0.314 / 0.328 / 0.431 / 0.362 /
0.255 and 0.248 / 0.384 / 0.230 / 0.348 / 0.366):

| Metric | NBA 2024-25 to 2025-26 (293 over 2,000) | WNBA 2024 to 2025 (92 over 1,000) |
|---|---:|---:|
| rapm / o_rapm / d_rapm | 0.322 / 0.292 / 0.361 | 0.374 / 0.352 / 0.163 |
| o_efg / o_tov / o_orb / o_ftr / o_other | 0.308 / 0.334 / 0.423 / 0.366 / 0.259 | 0.304 / 0.357 / 0.307 / 0.391 / 0.157 |
| d_efg / d_tov / d_drb / d_ftr / d_other | 0.247 / 0.387 / 0.229 / 0.341 / 0.376 | 0.128 / 0.337 / 0.128 / 0.306 / 0.343 |

## Standard errors (WNBA 2025, on the fixture)

`<m>_se` is the game-clustered ridge sandwich. A delete-one-game jackknife of the same ridge (286
refits at the season's penalty, done once from these files, 400 s, not a test) gives `o_rapm` / `d_rapm`
SEs a median 1.03 times the sandwich's (1.01 to 1.07) for the 133 players over 1,000 possessions.
Their median `o_rapm_se` is 1.08 and `rapm_se` 1.54 per 100, against a spread (sd) across them of
1.55 (`o_rapm`) and 2.28 (`rapm`): single-season RAPM ranks are noisy, and the components more so.
