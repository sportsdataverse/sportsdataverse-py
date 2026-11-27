<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [nfl_adjusted_epa fixtures — provenance](#nfl_adjusted_epa-fixtures--provenance)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# nfl_adjusted_epa fixtures — provenance

| file | rows | source | notes |
|---|---|---|---|
| `nfl_plays_2024_reg.parquet` | 45,247 | sportsdataverse-data release `nfl_model_pbp/model_pbp_2024.parquet` (downloaded 2026-11-27), `season_type == "REG"`, joined on `game_id` with `load_nfl_schedule(seasons=[2024])`'s `location` | Every regular-season play (kicks, penalties, kneels and spikes kept, so the scrimmage filter is exercised), down-selected to 14 columns. 272 games; 5 at a neutral site. On this file `nfl_adjusted_epa_ledger`'s leave-one-game-out value equals the season fit run without the game for all 272 games (max gap 5.6e-17), and every team's contributions sum to its season value (max gap 5.6e-17). |
| `espn_processed_plays_2024_wk1_2.parquet` | 550 | `NFLPlayProcess(gameId).espn_nfl_pbp()` then `.run_processing_pipeline()` on sdv-py `a42b65a39` against ESPN's live summaries, 2026-11-27: 401671789 (BAL at KC, week 1), 401671624 (LV at BAL, week 2), 401671670 (CIN at KC, week 2) | The live processor's column names (`pos_team` / `def_pos_team` are the numeric ids), down-selected to 17 columns. No neutral-site column: all three were at the home stadium (nflverse schedule `location` "Home"), so the tests add `neutral_site=False`. |
