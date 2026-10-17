# P. LEAGUE+ site fixtures: provenance

Real pages and bodies from `pleagueofficial.com`, captured live on **2026-10-12** (curl_cffi Chrome, keyless, 1 s
apart) by `sdv-internal-refs/pleague/tools/capture.py`; copied here from sdv-internal-refs commit **`c090529`**
(`feat(pleague): capture + OpenAPI specs for P. LEAGUE+ and the Preciser vendor API (#99)`), plus one full play-by-play
fetched **2026-10-13** (URL below). The tests never read the private refs checkout. Nothing is synthetic; the negative
tests mutate these real bodies.

| File | Source URL | What |
|---|---|---|
| `full/pbp_197.json` | `https://pleagueofficial.com/api/playbyplay.php?uuid=e6f98675-3e5b-11ed-949e-5d17350689cf` (fetched 2026-10-13) | The FULL, untruncated parsed play-by-play record (`pleague_parsers.pbp_record`) of gid 197 (2022-23 opener, Hsinchu 94 : 102 Taipei): 630 events, 4 periods. The numeric dtypes of the pbp frame come from it. The 575 KB HTML fragment itself is not committed, and the parsed record holds no clip url (they carry a vendor access key) |
| `schedule_regular_2022-23.html`, `schedule_regular_2020-21.html`, `schedule_finals_2022-23.html`, `schedule_future_2026-27.html` | `/schedule-regular-season/2022-23`, `/schedule-regular-season/2020-21`, `/schedule-finals/2022-23`, `/schedule-regular-season/2026-27` | Refs cut-down pages: title, month buttons and the first 3 (finals: all 6) game rows |
| `game_197.html`, `game_13.html`, `game_206.html`, `game_716.html` | `/game/197` (2022-23 opener), `/game/13` (2020-21 opener, empty uuid), `/game/206` (double overtime), `/game/716` (2025-26 finals) | Refs cut-down pages: the header card, the officials line and the two inline scripts holding `window.gid` and the uuid calls |
| `box_197.json`, `box_13.json`, `box_206.json`, `box_716.json` | `/api/boxscore.preciser.php?id=<gid>&away_tab=total&home_tab=total` | WHOLE bodies: era B, era A (2020-21), double overtime, 2025-26 finals |
| `pbp_197.json`, `pbp_206.json` | `/api/playbyplay.php?uuid=` for gid 197 and 206 | Refs cut-down bodies (`data` is the HTML table): every period header and the closing `Final` row kept, events cut to the first 10 of Q1, 2 of each later period and the last 3 of the game. Clip urls carry `key=<redacted>` in their base64 blob |
| `standings_2022-23.html`, `stat_player_2022-23_2.html` | `/standings/2022-23`, `/stat-player/2022-23/2` | Refs cut-down pages: title, selects, the table cut to 3 rows |
| `player_1JbPozLB.html`, `player_1P42uoZg.html` | `/player/1JbPozLB` (several seasons plus an EASL block), `/player/1P42uoZg` (one season) | The profile section and the season tables |
| `captures/*.json` | the refs `pleague/captures/` records | The refs PARSED records, every array cut to 3 rows. File names are the route slug, then `@<gid or key>` for all but the first sample. Includes the 2020-21 era-A box (`@13`) and the double-overtime game (`@206`) |
| `spec_paths.json` | `pleague.openapi.yaml` | The spec's ten routes, written by `tools/codegen/gen_pleague.py` |

Personal data: officials appear by name only (as printed); the box and play-by-play carry player names and jersey
numbers as printed. The player pages print a date of birth; the parser drops it and no frame carries it (the HTML
fixtures are the public pages as served).
