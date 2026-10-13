# VTB league fixtures (`api.vtb-league.com/v2`)

Every file is a body as the API sends it: the `{"meta": ..., "data": [...]}` envelope. The tests stamp the request under
`_request` the way `vtb_league_runtime._get` does.

| Path | What |
|---|---|
| `full/matches_vtb_2026.json.gz` | **Full, untruncated** body of `/v2/leagues/vtb/seasons/2026/matches?limit=1000` (273 matches, 2025-09-24 to the 2026 playoffs; 594 KB raw, so gzipped), fetched 2026-10-13 with plain `requests` through the shared polite session (1 s apart, no key, no proxy). The returns-schema dtypes (`vtb_dtypes.py`) and the dtype tests come from it: the refs captures cut arrays to 3 rows. |
| `captures/*.json` | The sdv-internal-refs samples (`vtb/captures/api/`, refs commit `0697dd9` (vtb/, 2026-10-12), captured 2026-10-12 from the droplet with `curl_cffi`, direct, no proxy), arrays cut to 3 rows: `/v2/leagues`, the seasons of `vtb` and `wbc`, and the 2026 matches of `vtb` and `wbc`. |
| `spec_paths.json` | The spec's 3 routes, written by `tools/codegen/gen_vtb.py` so the tests check the YAML without the refs checkout (CI has none). |

Source URLs: `https://api.vtb-league.com/v2/leagues`, `.../v2/leagues/{league}/seasons`,
`.../v2/leagues/{league}/seasons/{season}/matches?limit=1000`. Capture dates: 2026-10-12 (captures), 2026-10-13 (full).
`meta.time`, `meta.request` and `attendance` depend on the capture day.
