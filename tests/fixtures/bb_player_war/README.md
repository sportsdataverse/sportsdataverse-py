<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D26 WAR and VORP corpus](#bb-d26-war-and-vorp-corpus)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D26 WAR and VORP corpus

Real captures for `tests/nba/test_nba_player_war.py` and `tests/tools/test_fit_player_war.py`,
built on 2026-11-05 from the sportsdataverse-data releases and Basketball-Reference (zstd level
19). Nothing is synthetic: rows are only filtered, columns only dropped, and the player-seasons
are the production functions' own output. The college tests (`tests/mbb/test_mbb_player_war.py`)
reuse the player-value corpus (`../mbb_player_value/`, `../wbb_player_value/`) and BB-D22's
season records (`../bb_team_luck/`).

| File | Rows | Source | Kept |
|---|---:|---|---|
| `{nba,wnba}_impact_2025.parquet` | 569 / 181 | `nba_player_impact/nba_player_impact_2025`, `wnba_player_impact/wnba_player_impact_2025` (`load_nba_player_impact(2025)`, `load_wnba_player_impact(2025)`) | `season_type == "Regular Season"`. Cols: `season, season_type, player_id, player_name, team_id, min, off_poss, def_poss, bpm, adj_rapm, boban_filtered_skill, war` (the release's own `war`, kept to show it adds up to more wins than the league played). |
| `{nba,wnba}_player_games_2025.parquet` / `{nba,wnba}_team_games_2025.parquet` | 25,837 / 2,460, 5,399 / 572 | `nba_stats_possessions/nba_possessions_2025` + `nba_stats_schedules/nba_schedule_2025` (`load_nba_stats_possessions(2024)`, `load_nba_stats_schedules(2024)`), and the WNBA twins (`(2025)`) | `nba_player_war._possession_games` over the whole regular season: per `(season, team_id, game_id, player_id)` the on-court `poss`, and per `(season, team_id, game_id)` the team's `poss` and `margin`. The tests sum them to the season with `season_rows` (equal to the stints and net the PR first committed, to the last possession). |
| `mbb_stints_2025.parquet` / `mbb_net_2025.parquet` / `mbb_player_games_2025.parquet` / `mbb_team_games_2025.parquet` | 568 / 40 / 13,324 / 1,368 | `mbb_box_bpm(2025)`, `mbb_team_ratings(2025)` and `load_mbb_player_boxscore([2025])`, captured together on 2026-11-05 | `mbb_player_war._war_stints` and `._game_rows`, kept to the 40 smallest ESPN team ids among D1 teams: a self-consistent capture (the game possessions sum to the stints' to 1e-12) for the interval test. |
| `mbb_box_slice_2025.parquet` / `mbb_ratings_slice_2025.parquet` | 684 / 1 | the same box release and ratings | Every box row of ESPN team 2's 2024-25 games (both sides) and its ratings row. Cols: `season, game_id, team_id, athlete_id, minutes, team_score, opponent_team_score`; `season, team_id, games, adj_tempo`. |
| `nba_possessions_slice_2025.parquet` / `nba_schedule_slice_2025.parquet` | 2,575 / 13 | the same two NBA assets | The first 12 regular-season games by `game_id` and the first playoff game. Possession cols: `game_id, offense_team_id, defense_team_id, off_player_1..5, def_player_1..5, fg2m, fg3m, ftm, season`; schedule cols: `game_id, season, season_type, home_team_id, away_team_id, home_pts, away_pts`. |
| `{nba,wnba,mbb,wbb}_player_seasons.parquet` | 7,875 / 2,479 / 63,730 / 45,719 | `tools/fit_player_war.py --league <lg> --write`, which runs `nba_player_war` / `mbb_player_war` per season over `SPANS` (NBA 2012-2026, WNBA 2010-2025, MBB 2014-2026, WBB 2017-2026) | Cols: `season` (Int16), `poss` (to 0.1) and `rating_bpm` (the anchored box rating, to 0.001; `rating` in college), Float32. Neither depends on the fitted constants. |
| `bref_nba_advanced_2025.parquet` / `bref_wnba_advanced_2025.parquet` | 736 / 234 | Basketball-Reference's 2024-25 NBA and 2025 WNBA advanced tables (`bref_players_stats(2025, table="advanced")`, `league="wnba"`) | NBA cols `player, team, mp, bpm, vorp, ws, season`; WNBA `player, team, mp, ws, season` (Basketball-Reference has no WNBA VORP or BPM). A traded player has a season row (`2TM` / `3TM` in the NBA, `TOT` in the WNBA) plus one per team, and the test keeps the season row. Names are as captured: `bref_players_stats` returns UTF-8 read as Latin-1 (`Nikola JokiÄ\x87`), which the test undoes. A comparison oracle only: WAR never reads it. |

Id dtypes: `player_id` / `team_id` Int64 (stats.nba.com ids); `season` Int64 (Int16 in the
player-seasons), named by the END year as the releases stamp it.

Known gaps: one NBA player of 375 with 500+ minutes has no name match in our output; every
WNBA player with 500+ minutes matches (106).
