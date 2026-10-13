# Basketligaen fixtures (`www.basketligaen.dk/api`)

Real bodies from the Basketligaen site API (keyless GET JSON; Sportality platform, stats by Genius Sports LiveStats:
`genius-basketball`). Basketligaen is the Danish men's basketball league (series code DBL).

## Truncated route samples (`*.json` beside this file)

Copied unchanged from `sdv-internal-refs/sportality/captures/` at refs commit `cb7a95a` (`origin/main`, captured
2026-10-12 by `sportality/tools/capture.py`). Every array is cut to 3 items and every id-keyed map to 3 keys; the
capture gate had already checked each body against what was asked for (game uuid, Genius id, teams in side order,
final score, whole play-by-play feed, ssgt) before banking it. Personal keys (birth date, e-mail, phone, address,
contact) hold the literal `<redacted>`. File names follow `capture.py`'s slug rule: the path with `/` -> `__` and the
braces dropped, `__module__<module>` for stats-info, and `@<uuid>` for a second sample of the same route:

| Suffix | Sample |
|---|---|
| (none) | game `c4hh5nig6t` (2026-09-17, Holbaek-Stenhus 84-100 Randers Cimbria, Genius match 2868126); the 2026/27 regular season (ssgt `xs07bd3fmp`) for the table routes |
| `@j3nfc1b8og` | the 2023/24 opening game (Svendborg 80-77 Horsens), the first with gameday data |
| `@fb379yqcy5` | the 2025/26 overtime game (BK Amager 96-90 Holbaek-Stenhus) |
| `@qzw34da6jg` / `@al4a0u2go5` / `@rejluryl18` | 2025/26 regular season, 2025/26 playoffs, 2002/03 regular season |

Because the cut keeps three rows and three map keys, a sample's box-score rows need not match its name maps, and its
`teamList` is not a season's: tests that run the runtime's scope checks use the full bodies below.

`spec_paths.json` is the route list of `sportality.openapi.yaml` (42 paths: 24 routes, `stats-info` as one path per
module), written by `tools/codegen/gen_sportality.py` so the tests need no refs checkout.

## Full bodies (`full/`)

Fetched live on 2026-10-13 (UTC) with plain GETs, 1.5 s apart; personal keys removed with
`sportality_parsers._scrub` (the same removal the runtime applies). The column dtypes of the returns schemas and the dtype tests
come from these, not from the 3-row samples:

| File | URL |
|---|---|
| `gameday__play-by-play__c4hh5nig6t.json` | `https://www.basketligaen.dk/api/gameday/play-by-play/c4hh5nig6t`. **Trimmed**: 129 of the 804 events (the first 100 in feed order, the last 15 incl. the game-end line, and the first event of each of the 41 `type` / `sub_type` pairs), to stay under 300 KB. Untrimmed it was 913 KB |
| `gameday__boxscore__c4hh5nig6t.json` | `https://www.basketligaen.dk/api/gameday/boxscore/c4hh5nig6t` |
| `gameday__team-stats__c4hh5nig6t.json` | `https://www.basketligaen.dk/api/gameday/team-stats/c4hh5nig6t` (the 3-row sample keeps 3 of the 7 stat keys) |
| `sports-v2__game-schedule__qzw34da6jg.json` | `https://www.basketligaen.dk/api/sports-v2/game-schedule?seasonUuid=ggp2aapun2&seriesUuid=m1ay3mpa4t&gameTypeUuid=aprlbjgxlp` (192 games, 2025/26 regular season) |
| `statistics-v2__stats-info__players_summary__qzw34da6jg.json` | `https://www.basketligaen.dk/api/statistics-v2/stats-info/players_summary?ssgtUuid=qzw34da6jg&count=1000` (208 rows, 206 players) |
| `statistics-v2__stats-info__teams_summary__qzw34da6jg.json` | `.../stats-info/teams_summary?ssgtUuid=qzw34da6jg&count=1000` (12 teams) |
| `statistics-v2__stats-info__games_games__qzw34da6jg.json` | `.../stats-info/games_games?ssgtUuid=qzw34da6jg&count=1000` (192 games) |
| `statistics-v2__layout-info__directory.json` | **Derived** from `https://www.basketligaen.dk/api/statistics-v2/layout-info` (330 KB): the first item only, every ssgt's `ssgtUuid`, `state`, `gameType`, `season` and `series` (uuid, code, name) kept, each team list cut to `uuid` + `instanceId`, plus `ssgtDefaultValue`; the module lists, filters and overview blocks dropped. All 49 ssgts, 2002/03 to 2026/27 |

Ten live requests were spent in all: eight for these bodies and two for the runtime smoke test (`sportality_ssgt_uuid` and
`sportality_game_overview`), each at least 1 s apart.
