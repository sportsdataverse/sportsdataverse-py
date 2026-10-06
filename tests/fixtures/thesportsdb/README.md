# TheSportsDB API v1 fixtures

Copied verbatim from `sdv-internal-refs/thesportsdb/captures/` (refs main `22b148b`).

Server: `https://www.thesportsdb.com/api/v1/json/3` (the free test key, 30 req/min).
Captured **2026-10-06** live at 2 s pacing; every array is cut to its first 3 elements at
every depth. League `4328` (English Premier League), season `2025-2026`, team `133604`
(Arsenal), player `34145937`, event `2267073`, player search `Danny Welbeck`.

| File | Route | Example params | Envelope key |
|---|---|---|---|
| `all_sports.json` | `/all_sports.php` | — | `sports` |
| `all_leagues.json` | `/all_leagues.php` | — | `leagues` |
| `lookupleague.json` | `/lookupleague.php` | `id=4328` | `leagues` |
| `search_all_teams.json` | `/search_all_teams.php` | `l=English Premier League` | `teams` |
| `lookupteam.json` | `/lookupteam.php` | `id=133604` | `teams` |
| `lookup_all_players.json` | `/lookup_all_players.php` | `id=133604` | `player` |
| `searchplayers.json` | `/searchplayers.php` | `p=Danny Welbeck` | `player` |
| `lookupplayer.json` | `/lookupplayer.php` | `id=34145937` | `players` |
| `eventsseason.json` | `/eventsseason.php` | `id=4328&s=2025-2026` | `events` |
| `lookupevent.json` | `/lookupevent.php` | `id=2267073` | `events` |
| `eventsnextleague.json` | `/eventsnextleague.php` | `id=4328` | `events` |
| `lookuptable.json` | `/lookuptable.php` | `l=4328&s=2025-2026` | `table` |

The envelope key names the resource and varies per route — note `player` for
`/lookup_all_players.php` and `/searchplayers.php` but `players` for
`/lookupplayer.php`. That inconsistency is upstream, not a capture artifact.
