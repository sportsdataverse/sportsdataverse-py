<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Game story fixtures](#game-story-fixtures)
  - [Hockey, baseball and soccer (phase 2)](#hockey-baseball-and-soccer-phase-2)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Game story fixtures

Real released plays, projected to the columns the football game-story adapter reads, sorted by `game_id`,
`game_play_number`. Cut 2026-11-26 from the `sportsdataverse-data` release assets. Used by
`tests/common/test_game_story.py`; the basketball tests read the real ESPN summaries in
`tests/fixtures/espn/basketball_pbp/`.

| File | Source | Games | Why |
|---|---|---|---|
| `espn_cfb_pbp_games.parquet` | `espn_cfb_pbp/play_by_play_2024.parquet` | 401628319 (63-0), 401628333 (34-27, OT), 401628337 | a blowout, an overtime game, and a game whose plays end 20-21 while the release's `homeFinalScore` / `awayFinalScore` say 14-21 (the period gate reports it, `periods_agree` false) |
| `espn_nfl_pbp_games.parquet` | `espn_nfl_pbp/play_by_play_2024.parquet`, plus 401772921 from `play_by_play_2025.parquet` | 401671492 (23-20), 401671495 (40-14), 401671626 (38-41, OT), 401772921 (GB 40, DAL 40, 2025 week 4, OT) | a close game, a blowout, an overtime game and a real tie; finals checked against the nflverse schedule |
| `nba_401810169_{pbp,team_box,player_box}.parquet` | hoopR-nba-data releases via `load_nba_pbp` / `load_nba_team_boxscore` / `load_nba_player_boxscore`, season 2026 | 401810169 (away 120, home 125) | the away row's `total_turnovers` is null (its `turnovers` is 19): the four-factor parts must still add up to the margin; found by the held-out gate run |

The 2024 games come from the gate pilot seasons. The tie and the NBA game come from held-out seasons: ties are not in
the edge-direction gate; the NBA game was added after the held-out run, as a regression test for the defect it found.

## Hockey, baseball and soccer (phase 2)

All from the gate pilot seasons, cut 2026-11-26 from the release assets (raw ESPN summaries for soccer).

| File | Source | Games | Why |
|---|---|---|---|
| `nhl_{pbp,schedule,skater_box}_games.parquet` | `load_nhl_pbp_full` / `load_nhl_schedules` / `load_nhl_skater_boxscores`, season 2025 | 2024020022 (shootout), 2024020009 (overtime), 2024020001 (regulation) | the shootout's goal, which no play carries, and the `SO` period |
| `pwhl_{pbp,schedule,skater_box,xg}_games.parquet` | `load_pwhl_pbp` / `load_pwhl_schedules` / `load_pwhl_skater_boxscores` / `load_pwhl_xg_pbp`, season 2025 | 106 (shootout), 107 (overtime), 105 (regulation) | the running score built from goal events; `deciding_score` moments |
| `mlb_{pbp,schedule,wpa,re24}_games.parquet` | `load_mlb_pbp` / `load_mlb_schedule` / `load_mlb_wpa` / `load_mlb_re24_matrix`, season 2024 | 744802 (extra innings), 744795 (road win), 744797 (home win) | extra innings keep their number; the release keys a PA's WPA to the next index |
| `mlb_{pbp,schedule,wpa}_716356.parquet`, `mlb_re24_2023.parquet` | the same loaders, season 2023 | 716356 (11 innings) | the automatic runner scores on the first PA of the 10th and 11th, so no base shows him after it |
| `mlb_{pbp,schedule}_748542.parquet` | the same loaders, season 2023 | 748542 (World Series, 11 innings) | no automatic runner: its extra halves open empty |
| `eng1_<event>.json.gz` | `espn_soccer_summary("eng.1", event_id, return_parsed=False)`, 2023-24 Premier League | 671031 (a goal following a corner), 671033 (a draw), 671048 (an own goal) | goal kinds, a draw's missing moment, own goals credited to the side they count for |
