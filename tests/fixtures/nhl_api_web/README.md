# NHL api-web fixture payloads

Captured 2026-05-24 from `https://api-web.nhle.com/v1/`. Used by
`tests/test_nhl_api_web_parsers.py` to exercise parsers offline.

| File | Endpoint | Notes |
|---|---|---|
| `pbp_2024_scf_g7.json`           | `/gamecenter/2023030417/play-by-play` | 2024 Stanley Cup Final G7 EDM @ FLA — 331 plays |
| `boxscore_2024_scf_g7.json`      | `/gamecenter/2023030417/boxscore`     | Same game; 40 players × 36 stat cols across 6 (team × position) buckets |
| `landing_2024_scf_g7.json`       | `/gamecenter/2023030417/landing`      | Game header + summary (scoring, threeStars, penalties) |
| `right_rail_2024_scf_g7.json`    | `/gamecenter/2023030417/right-rail`   | Series + shots + game info + linescore — 6 sub-frames |
| `schedule_2024_06_24.json`       | `/schedule/2024-06-24`                | Week-of view, 1 game |
| `score_2024_06_24.json`          | `/score/2024-06-24`                   | Single-day score, 1 game |
| `scoreboard_now.json`            | `/scoreboard/now`                     | Multi-day scoreboard, 11 days |
| `standings_now.json`             | `/standings/now`                      | All 32 NHL teams × 84 stat cols |
| `standings_season.json`          | `/standings-season`                   | 108 NHL seasons since 1917-18 |
| `club_schedule_edm_2024.json`    | `/club-schedule-season/EDM/20232024`  | Edmonton's full 2023-24 schedule (115 games incl. playoffs) |
| `club_stats_edm_2024.json`       | `/club-stats/EDM/20232024/2`          | Skaters (27) + goalies (3) for the regular season |
| `roster_edm_2024.json`           | `/roster/EDM/20232024`                | 24 players across forwards / defensemen / goalies |
| `player_mcdavid_landing.json`    | `/player/8478402/landing`             | Connor McDavid; 130-column rich profile |
| `player_mcdavid_gamelog.json`    | `/player/8478402/game-log/20232024/2` | 76 regular-season games |
| `skater_leaders_now.json`        | `/skater-stats-leaders/current?categories=points&limit=10` | Top 10 by points |
| `goalie_leaders_now.json`        | `/goalie-stats-leaders/current?categories=wins&limit=10`   | Top 10 by wins |
| `draft_picks_2024_r1.json`       | `/draft/picks/2024/1`                 | Full 1st round of the 2024 NHL Draft (32 picks) |
