# nfl_api fixtures

Trimmed real bodies from `api.nfl.com`, captured anonymously (the web app's
`/identity/v3/token` device grant, plan `free`) during an in-progress game:
DEN @ KC, 2026 REG week 1, Shield id `a9a890ed-4feb-11f1-abca-2c54536568a9`,
2026-09-15 01:00 UTC.

| file | route | trimmed |
|---|---|---|
| `live_team_statistics.json` | `/football/v2/stats/live/team-statistics/{game_id}` | no |
| `live_player_statistics.json` | `/football/v2/stats/live/player-statistics/{game_id}` | first 3 players per side |
| `game_details_v2.json` | `/experience/v2/gamedetails/{game_id}` (all `include*` flags on) | drive-chart lists and replays cut to their first items |
| `game_details_by_slug.json` | `/experience/v1/gamedetailsbyslug/{slug}` (`broncos-at-chiefs-2026-reg-1`, no flags; captured 2026-09-15 after the final) | drive-chart lists cut to their first two items |

Full bodies: `sdv-internal-refs/nfl/captures/shield/2026-09-14/`.
