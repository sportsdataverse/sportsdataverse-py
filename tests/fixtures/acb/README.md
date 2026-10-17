# ACB fixtures (`api2.acb.com`)

| Path | What |
|---|---|
| `full/*.json`, `full/play-by-play.json.gz` | **Full, untruncated** bodies fetched live on 2026-10-12 (10 requests, 1.2 s apart, plain `requests`, the public `X-APIKEY`): match 105401 (Barca 89-86 Valencia Basket, Liga 2026-27 round 3) `match-header`, `boxscores`, `play-by-play` (735 plays, 417 KB, so gzipped), `match-shots` (188 shots), `lead-tracker`, `lineup`, `match-leaders`; the Liga 2026-27 round 3 game list (`matches`, `roundId=6017`), the standings (`standings`, 18 teams with their per-round history) and Barca's `team-stats` (`clubId=2`). The returns-schema dtypes and the dtype tests come from these: the refs captures cut arrays to 3 rows and type whole-number rates as integers. |
| `captures/*.json` | The sdv-internal-refs samples (`acb/captures/`, 2026-10-12), arrays cut to 3 rows, one per route (plus a few variants: 2016 and 2005 matches for the legacy shot grid and old box scores, a Copa round). They feed the wrappers that have no full body. |
| `spec_paths.json` | The spec's routes, written by `tools/codegen/gen_acb.py` so the tests check the YAML without the refs checkout (CI has none). |

The API key is public (acb.com's bundle) and is not stored in any body.
