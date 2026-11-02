<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [legabasket fixtures](#legabasket-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# legabasket fixtures

Lega Basket Serie A site API (`https://www.legabasket.it/api`), keyless, used by `tests/basketball/lba/`.

- `captures/<season>/*.json`: the maintainers' truncated captures (3 rows per array; play-by-play: 3 picked actions),
  taken 2026-10-12. Seasons 2026 (2026-27), 2017
  (2017-18) and 1995 (1995-96). The file name is the route with `/` -> `__`.
- `full/*.json`: ONE untruncated body per family route, fetched 2026-10-12 (a plain `GET`, 2.2 s apart) and re-saved
  compact. The dtypes of the returns schemas come from these, not from the captures (which mistype rates as integers).
  Game 25602 (Treviso 109-94 Roma, round 1 of championship 602, 2026-27 Regular Season), team 1764, player 969.

| File | Request |
|---|---|
| `full/championships__s2026.json` | `get-championships?s=2026&cs_id=1&items=1000` |
| `full/calendar__602__d1.json` | `get-championships-calendar-by-id?id=602&d=1` |
| `full/matches__25602.json` | `get-championships-matches-by-id?id=25602&sc=1` (header + box score) |
| `full/pbp__25602.json` | `get-championships-matches-play-by-play?id=25602&info=true&sort=asc` (605 actions, ~330 KB) |
| `full/ranking__602__d3.json` | `get-championship-ranking?id=602&d=3` |
| `full/teams__2026.json` | `get-teams?year=2026&items=50` |
| `full/team_players_stats__1764.json` | `get-team-players-stats?id=1764&s=2026&cs_id=1` |
| `full/player_matches__969.json` | `get-player-matches?id=969&s=2026&cs_id=1` |
| `full/players_statistics__points.json` | `get-players-statistics?s=2026&cs_id=1&cat=points` |
| `full/teams_statistics__offensive.json` | `get-teams-statistics-by-type?type=teams_offensive&s=2026&cs_id=1` |

`spec_paths.json` is the spec's route list, written by `tools/codegen/gen_legabasket.py` so the tests do not read the
maintainers' private reference checkout. Season parameters are START years on the wire (`s=2026` = 2026-27); sdv's public key is the END
year (`season=2027`).
