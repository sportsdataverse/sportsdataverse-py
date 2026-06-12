# MLB Stats API fixture payloads

Captured 2026-05-24 from `https://statsapi.mlb.com/api/v1/`. Used by
`tests/test_mlb_api_parsers.py` to exercise parsers offline.

| File | Endpoint | Notes |
|---|---|---|
| `schedule_2024_09_29.json`       | `/schedule?sportId=1&date=2024-09-29` | 15 games on the final day of the regular season |
| `teams_2024.json`                | `/teams?sportId=1&season=2024`        | 30 MLB teams |
| `team_roster_yankees_2024.json`  | `/teams/147/roster?season=2024`       | NYY full-season roster (~54 players) |
| `standings_2024.json`            | `/standings?leagueId=103,104&season=2024` | 6 divisions × 5 teams each = 30 teams |
| `person_stats_judge_2024.json`   | `/people/592450/stats?stats=season&season=2024` | Aaron Judge's 2024 season splits |
| `venues_active.json`             | `/venues?activeStatus=Y`              | 1,646 active venues (MLB + MiLB + amateur) |
| `sports.json`                    | `/sports`                             | 20 sports (MLB, AAA, AA, A+, A, Rookie, KBO, NPB, etc.) |
| `divisions.json`                 | `/divisions`                          | 61 divisions across all sport IDs |

### 0.0.55 — list-shaped prose endpoints (auto `@return` tables)

Captured 2026-06-12. These feed `returns_schema: native/mlb_api/<short>` via
`tools/codegen/native_fixture_map.yaml` so the generic `parse_mlb_api_list` /
`parse_mlb_api_schedule` parsers emit documented `@return` column tables.

| File | Endpoint | Notes |
|---|---|---|
| `leagues.json`                   | `/leagues?sportId=1`                       | 15 leagues (MLB AL/NL + MiLB) × 40 cols |
| `awards.json`                    | `/awards`                                  | 682 award definitions |
| `award_recipients_almvp.json`    | `/awards/ALMVP/recipients`                 | 95 AL MVP recipients |
| `team_147.json`                  | `/teams/147`                               | single team (NYY) detail |
| `team_alumni_147_2024.json`      | `/teams/147/alumni?season=2024&group=hitting` | 31 NYY hitting alumni |
| `team_affiliates_147.json`       | `/teams/affiliates?teamIds=147`            | 11 NYY org affiliates |
| `season_2024.json`               | `/seasons/2024?sportId=1`                  | single season metadata |
| `venue_3313.json`                | `/venues/3313`                             | single venue detail |
| `sport_players_2024.json`        | `/sports/1/players?season=2024`            | 1,454 MLB players |
| `umpires.json`                   | `/jobs/umpires`                            | 98 active umpires |
| `schedule_postseason_2024.json`  | `/schedule/postseason?season=2024&sportId=1` | 43 postseason games |

### 0.0.56 — dedicated game-endpoint parsers

Captured 2026-06-12 from game `745282`. These use purpose-built parsers
(`parse_mlb_api_boxscore` / `_linescore` / `_play_by_play` / `_win_probability`)
because their payloads nest under keys the generic `parse_mlb_api_list`
does not recognize.

| File | Endpoint | Notes |
|---|---|---|
| `boxscore_745282.json`           | `/game/745282/boxscore`        | per-player batting + pitching box |
| `linescore_745282.json`          | `/game/745282/linescore`       | per-inning home/away line score |
| `play_by_play_745282.json`       | `/game/745282/playByPlay`      | per-play event log (`allPlays`) |
| `win_probability_745282.json`    | `/game/745282/winProbability`  | per-play win-probability series |

To refresh, re-capture with the same URLs and overwrite the files
(stem-matched). The parser tests are payload-agnostic so newer captures
will keep working as long as the schema doesn't change.
