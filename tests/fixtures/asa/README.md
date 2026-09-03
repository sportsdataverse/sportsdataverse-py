# ASA fixtures

Real, trimmed responses from the American Soccer Analysis public API
(`https://app.americansocceranalysis.com/api/v1/mls/...`), captured 2026-07-18
and copied byte-for-byte from `sdv-internal-refs/asa/captures/mls/`.

| File | Route |
|---|---|
| `teams.json` | `/mls/teams` |
| `players.json` | `/mls/players` |
| `games.json` | `/mls/games` |
| `players_xgoals.json` | `/mls/players/xgoals` |
| `players_salaries.json` | `/mls/players/salaries` |
| `players_goals-added.json` | `/mls/players/goals-added` |
| `teams_goals-added.json` | `/mls/teams/goals-added` |

Each body is the real top-level JSON array trimmed to three rows. Regenerate by
re-copying from the reference repo; do not hand-edit.
