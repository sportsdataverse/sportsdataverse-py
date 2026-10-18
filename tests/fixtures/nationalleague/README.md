# National League app API fixtures (`nationalleague`)

Keyless JSON from `https://www.nationalleague.ch/api`. robots.txt disallows `/api`; the owner approved
building on it on 2026-10-12 for polite, incremental use (at least 1 s between requests).

## Route captures (truncated)

Byte copies of `sdv-internal-refs/nationalleague/captures/*.json` (refs commit `007ac1a`, captured
2026-10-12). Arrays are cut to their first 3 elements at every depth, so they are fine for the row and
column shape but not for dtypes: a rate that is `90` in the three sampled rows is `86.36` elsewhere.
`teams__teamId__info.json` carries `fields.apiKey` already replaced with `<redacted>` by the capture tool.

| File | Request |
|---|---|
| `games.json` | `/games?season=2026` |
| `games__current.json` | `/games/current?season=2027` |
| `games__playoffs.json` | `/games/playoffs?season=2026` |
| `games__playouts.json` | `/games/playouts?season=2026` |
| `games__team__teamId.json` | `/games/team/101144?season=2026` (EV Zug) |
| `games__gameId.json` | `/games/20261105000325?season=2026` (HC Davos 4-3 SCL Tigers) |
| `player.json` | `/player?season=2026&phase=4940` |
| `player__topscorer.json` | `/player/topscorer?take=3&season=2026&phase=4940` |
| `player__team__teamId.json` | `/player/team/101144?season=2026&phase=4940` |
| `player__playerId.json` | `/player/305519?season=2026&phase=4940` (Tobias Geisser) |
| `teams.json` | `/teams?season=2026&phase=4940` |
| `teams__teamId__info.json` | `/teams/101144/info` |

## Full bodies (`full/`, the dtype source)

Fetched 2026-10-12 through `sportsdataverse.hockey.euro_hockey_runtime._get` (3 requests, 1 s apart),
written as compact JSON. The documented schemas and the dtype tests come from these, not from the
truncated captures.

| File | Request | Kept |
|---|---|---|
| `game_20261105000325.json` | `/games/20261105000325?season=2026` | whole body (105 shots, 17 events, 44 box-score rows) |
| `player_2026_4940.json` | `/player?season=2026&phase=4940` | 90 of 502 rows, every column: the 5 rows that between them hold every column's value types (a non-whole rate, a null, the one integer `position`), then the body's first rows. The whole body is 969 KB. |
| `teams_2026_4940.json` | `/teams?season=2026&phase=4940` | whole body (14 rows) |
