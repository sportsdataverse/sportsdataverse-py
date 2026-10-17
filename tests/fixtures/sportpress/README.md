# BNXT League fixtures (`bnxt.sportpress.info/api/v1`, the Sportpress CMS)

Provenance of every file here. Nothing reads the private `sdv-internal-refs` checkout at test time (CI has none).

| Path | What |
|---|---|
| `captures/*.json` | The 80 refs samples of `sdv-internal-refs/sportpress/captures/` (refs commit `c6ebad8`, captured 2026-10-12), copied verbatim: lists cut to 3 rows. Source URL: `https://bnxt.sportpress.info/api/v1<route>`. `@<id>` in a name is a second sample of the same route (another season or game). They are truncated, so a rate that is whole in the sample is an integer there; they type nothing. The social-handle keys in them are null in every sample. |
| `full/*.json`, `full/film__...json.gz` | **Full, untruncated** bodies fetched live on 2026-10-12 through `bnxt_runtime` (the public header key read from the bnxtleague.com bundle, `X-Localization: en`, >= 1 s apart, contact fields dropped by the runtime), compacted JSON with the values untouched. They exist because the dtype of a statistic comes from the whole body: the full box mixes `75` and `41.7` in one percentage column. |
| `spec_paths.json` | The spec's 23 routes, written by `tools/codegen/gen_sportpress.py` so the tests check the YAML without the refs checkout. |

The `full/` bodies (live requests: the bundle page and script, then one request per body; one `/schedule/find` was fetched twice):

| File | Request | Cut |
|---|---|---|
| `boxscore__game__competitionId__gameId.json` | `/boxscore/game/25/11295` (2026-27, Antwerp 113-106 Kortrijk, double overtime) | none (2 teams, 21 player lines) |
| `film__game__competitionId__gameId.json.gz` | `/film/game/25/11295` | none (645 events, 950 KB, so gzipped) |
| `schedule__find__gameId.json` | `/schedule/find/11295` | none (six period scores per side) |
| `schedule__overview__gameId.json` | `/schedule/overview/11295` | none |
| `schedule__club__season.json` | `/schedule/club/2027?clubs[0]=1&clubs[1]=2&month=-1` (240 games, 469 KB whole) | first 24 games, every kept row complete |
| `schedule__game-slider__competitionId.json` | `/schedule/game-slider/25` (14 finished + 15 upcoming) | none |
| `competition-team__player-averages__competitionTeamId.json` | `/competition-team/player-averages/541` | none |
| `roster__season-players__season.json` | `/roster/season-players/2027` | first 30 entries (of the season's roster, 154 KB whole) |

No fixture holds the header key (a test greps for it and for the header name).
