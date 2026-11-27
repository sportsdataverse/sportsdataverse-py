<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Game story fixtures](#game-story-fixtures)

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
