<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [WBB prediction-stack oracle corpus (season 2024)](#wbb-prediction-stack-oracle-corpus-season-2024)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# WBB prediction-stack oracle corpus (season 2024)

Women's mirror of [`tests/fixtures/mbb_prediction/`](../mbb_prediction/README.md)
(same capture script with `PRED_LEAGUE=womens`, same column contracts, same
`Utf8` id convention). Captured **2026-07-07** for the **2024** season
(2023-24 women's Division I). Regenerate with
`PRED_LEAGUE=womens uv run python dev/mbb_prediction/capture_oracle.py <target>`.

| File | Rows | Notes |
|---|---:|---|
| `results_2024.parquet` | 5908 | `load_wbb_schedule([2024])`, completed games. |
| `team_box_2024.parquet` | 11796 | `load_wbb_team_boxscore([2024])` possession inputs. |
| `torvik_2024.parquet` | 348 | Women's barttorvik `https://barttorvik.com/ncaaw/2024_team_results.csv`; 348/360 matched (same 12 naming irregularities as the men's capture). |
| `espn_bpi_2024.parquet` | 360 | `espn_wbb_season_powerindex(2024, team_id=...)` per team. |
| `espn_predictor_sample.parquet` | 308 | `espn_wbb_game_predictor`, every 20th completed game (3 of 311 missing predictor data). |
| `espn_odds_sample.parquet` | 175 | `espn_wbb_game_odds` closing lines; women's book coverage is thin (136 of 311 sampled games had no usable book). |
| `pbp_sample_2024.parquet` | 55170 | Every 25th play of all 4,070 eligible games + as-of `pregame_home_prob` + `home_win` label. |
| `ncaa_tourney_2024.parquet` | 134 | Actual 2024 women's tournament (67 games, 68 teams, seeds 1–16 via scoreboard `curatedRank`; headline filter `NCAA Women's Championship`). |

Observed gate values at capture time (same thresholds as the men's gates;
the spread/total MAE floors are women's observed-floor values — see
`tests/wbb/test_wbb_prediction_backtest.py`): Torvik Spearman 0.9948, Brier
0.1613 vs ESPN 0.1570, spread MAE 3.61 / total MAE 5.56 (n=97), WP decile gap
0.0224, SoS Spearman 0.9846, seed-order Spearman 0.9756, neutral-site slope
0.9165.
