# tpbl fixtures

Source: `https://api.tpbl.basketball/api` (the Laravel JSON API behind tpbl.basketball), keyless, no headers.

## `captures/`

Copies of the committed samples in `sdv-internal-refs/tpbl/captures/` (source commit `239f4bc`, the last commit that
touched `tpbl/`; captured live 2026-10-12, chrome TLS, requests >= 1 s apart). Every array is cut to 3 rows and every
number-keyed object (quarters, roster ids) to 3 keys, so these are per-route shape fixtures, not dtype sources. The
`@key` suffix names the variant (`games__game_id@6` = game 6, `seasons__season_id__games@3` = season id 3,
`...@655_4` = division 655 / team 4, `...@2_4_r1` = division 2 / team 4 / `rounds[]=1`). The refs `presence/` records
and `captures/README.md` are not copied. Players' `birthday` was replaced by `<redacted>` on copy (the runtime redacts
birth dates and contact fields from every raw body, so no frame can carry one).

## `full/`

FULL, untruncated bodies, fetched live 2026-10-13 through `tpbl_runtime._get` (so each passed the runtime's own scope
checks) and stored unwrapped: the dtypes of the returns schemas come from these.

| File | Route | Notes |
|---|---|---|
| `seasons.json` | `/seasons` | all 3 seasons |
| `season_games_2027.json` | `/seasons/3/games` | 133 games of 2026-27: 7 preseason games played, the rest `NOT_STARTED` (scores and attendance absent) |
| `game_6.json` | `/games/6` | 2024-25 preseason, Hsinchu Lioneers 111-108 Taipei Mars, 4 quarters, 3 referees |
| `game_stats_6.json` | `/games/6/stats` | the same game's box score: 17 + 16 player lines, 4 quarters (456 KB, over the ~300 KB guide: one game is already the smallest unit) |
| `roster_2_4.json` | `/divisions/2/teams/4/players` | 13 registered players of Hsinchu, 2024-25 regular season |
| `standings_2.json` | `/divisions/2/games/standings` | 7 teams with head-to-head records |
| `team_shots_2_4_r1.json` | `/divisions/2/teams/4/shooting-spots?rounds[]=1` | 777 first-quarter shots |
| `player_games_75_div2.json` | `/players/75/games/stats?division_id=2` | the FIRST 20 of the player's 33 game rows (the whole body is 380 KB) |

`../spec_paths.json` is the 11 routes of `tpbl.openapi.yaml`, written by `tools/codegen/gen_tpbl.py` so the tests do not
read the private refs checkout.
