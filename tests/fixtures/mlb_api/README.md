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

To refresh, re-capture with the same URLs and overwrite the files
(stem-matched). The parser tests are payload-agnostic so newer captures
will keep working as long as the schema doesn't change.
