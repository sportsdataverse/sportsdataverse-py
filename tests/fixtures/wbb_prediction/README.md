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
| `turnover_key_team_box.parquet` | 316 | `load_wbb_team_boxscore([2008, 2009, 2010, 2014])`, captured 2026-09-29: both team rows of every 2008 and 2009 Purdue (`2509`) game, every 2010 Stanford (`24`) and UConn (`41`) game, and every 2014 UConn game; raw ESPN `Int32` ids. Cols: `game_id, season, team_id, team_abbreviation, team_score, field_goals_attempted, offensive_rebounds, turnovers, total_turnovers, team_turnovers, free_throws_attempted`. 2009-2010 carry the count in `team_turnovers` with `turnovers`/`total_turnovers` 0 (ESPN `teamTurnovers`; the 2010 final is `300960041`, 10 each); 2009 game `290252509` has 0 under every key; 2008 has 0 under every key in all 22 games; 2014 uses `turnovers`, with a different `team_turnovers`. |
| `turnover_key_schedule.parquet` | 158 | `load_wbb_schedule([2008, 2009, 2010, 2014])`, captured 2026-09-29: those 158 games, raw ESPN columns and dtypes (`game_id, season, date, game_date, home_id, away_id, neutral_site`). Regression slice for the `team_turnovers` fallback and the zero-turnover game drop in `raw_game_efficiency`. |
| `wbb_late_game_2026.parquet` | 218240 | `load_wbb_pbp([2026])` (ESPN `espn_womens_college_basketball_pbp` release, read 2026-11-04): every play of the fourth quarter with `start_game_seconds_remaining <= 120`, plus every overtime play, of all 6,011 holdout games, scores forward-filled per game, with the schedule's `neutral_site`. Holdout late-game gate (tests/mbb/test_mbb_in_game_wp_late_game.py), written by `tools/models/mbb_prediction/train_in_game_wp.py`; 2026 is not a training season. |
| `baseline_in_game_wp/wbb_in_game_wp_eb4207476e.ubj` | -- | `sportsdataverse/mbb/models/wbb_in_game_wp.ubj` at eb4207476e: the pre-2026-11 women's booster (2023-only fit; WP moved away from the leader at 94 final-minute grid steps). The late-game gates must fail on it. |
| `wbb_rated_prior_games.parquet` | 11483 | Source: `load_wbb_pbp([2025, 2026])` + `mbb_win_prob._pregame_probs` (ESPN `espn_womens_college_basketball_pbp` / schedule / team-box releases, read 2026-11-06). The college pregame prior gate (tests/mbb/test_mbb_wp_model_cards.py): one row per finished, untied game of 2025 and 2026, `season, game_id, neutral_site, home_team_spread` (null: no college lines), `game_spread_available` (false), `rated_home_prob` (the weekly as-of rating; null for the 1128 games of the season's first days) and `y` (home won). Written by `tools/models/mbb_prediction/wp_model_cards.py evaluate`. |
| `wbb_holdout_gate_sums.parquet` | 287894 | Source: `load_wbb_pbp([2026])` scored through `score_win_prob` (ESPN `espn_womens_college_basketball_pbp` release, read 2026-11-06). The 2026 holdout gate's per-game block totals (tests/mbb/test_mbb_wp_model_cards.py recomputes the gate from them): `game_id, block` (`live`, each period, `ot`, `final_minute`, `bin0`-`bin9`), `n` (live plays), `se` (summed squared error), `sp` / `sy` (summed prediction / outcome), `prior` (`hfa` / `rated`) and `model` (`shipped` / `replaced`, the eb4207476e booster). Written by `tools/models/mbb_prediction/wp_model_cards.py evaluate`. |

Observed gate values at capture time (same thresholds as the men's gates;
the spread/total MAE floors are women's observed-floor values — see
`tests/wbb/test_wbb_prediction_backtest.py`): Torvik Spearman 0.9948, Brier
0.1613 vs ESPN 0.1570, spread MAE 3.61 / total MAE 5.56 (n=97), WP decile gap
0.0224 (a retired gate: tests/mbb/test_mbb_wp_model_cards.py replaced it), SoS Spearman 0.9846, seed-order Spearman 0.9756, neutral-site slope
0.9165.
