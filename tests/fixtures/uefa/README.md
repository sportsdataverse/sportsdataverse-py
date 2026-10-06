# UEFA fixtures

Byte copies of every capture in `sdv-internal-refs/uefa/captures/` (source commit
`e330067`, 2026-10-06): real responses from the keyless UEFA front-end APIs (four
`*.uefa.com` hosts), captured live on 2026-10-06 with `Accept: application/json` and
`Origin: https://www.uefa.com`, every array cut to its first 3 elements at every depth.
File names keep the capture's slug (`<host-stem>__<path>`), so a re-sync is a plain
`cp sdv-internal-refs/uefa/captures/*.json tests/fixtures/uefa/`. Do not hand-edit.

| File | Wrapper | Route |
|---|---|---|
| `comp__v2__competitions.json` | `uefa_competitions` | `comp.uefa.com/v2/competitions?competitionIds=1,3,2019` |
| `comp__v2__teams.json` | `uefa_teams` | `comp.uefa.com/v2/teams?competitionId=1&seasonYear=2026&limit=3&offset=0` |
| `comp__v2__players.json` | `uefa_players` | `comp.uefa.com/v2/players?competitionId=1&seasonYear=2026&limit=3&offset=0` |
| `match__v5__matches.json` | `uefa_matches` | `match.uefa.com/v5/matches?competitionId=1&seasonYear=2026&limit=3&offset=0` |
| `match__v5__livescore.json` | `uefa_livescore` | `match.uefa.com/v5/livescore` |
| `standings__v1__standings.json` | `uefa_standings` | `standings.uefa.com/v1/standings?competitionId=1&seasonYear=2026` |
| `matchstats__v1__team-statistics__2047742.json` | `uefa_team_statistics` | `matchstats.uefa.com/v1/team-statistics/2047742` |

Every body is a bare top-level JSON array -- `team-statistics` included (one element
per team, two per match), so no route answers a page object or an envelope.
