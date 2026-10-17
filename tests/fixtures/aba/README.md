# ABA League fixtures: provenance

Real pages and bodies from the ABA League sites, captured live on **2026-10-12** (keyless, 1 s apart) by
`sdv-internal-refs/aba-liga/tools/capture.py`; copied here from sdv-internal-refs commit **`14f731b`**
(`feat(aba-liga): capture + OpenAPI spec for the ABA League sites (#93)`), plus one full page fetched
2026-10-13. The tests never read the private refs checkout.

| File | Source URL | What |
|---|---|---|
| `full/match_6_25_1.json` | `https://www.aba-liga.com/match/6/25/1/Overview` (fetched 2026-10-13) | The FULL, untruncated parsed record (`aba_parsers.record_for("match", ...)`) of ABA 2025-26 game 6, Buducnost VOLI 93:97 Spartak Office Shoes after overtime: 24 box rows (2 did-not-play), 659 play-by-play events, 5 periods. The numeric dtypes of the match frames come from it. The 478 KB HTML page itself is not committed |
| `match_6_25_1.html`, `match_1_8_1.html` | `/match/6/25/1/Overview`, `/match/1/8/1/Overview` (2008-09, box only) | Refs cut-down copies (head, scripts and other tabs dropped; each play-by-play table keeps its first 4 and last 13 rows plus one scoring, timeout and team-only row) |
| `calendar_25_1.html` | `/calendar/25/1/` | Refs cut-down copy: 3 rounds, at most 2 games each |
| `standings_25_1.html` | `/standings/25/1/` | Every table whole (the balance checks need every row) |
| `player_4807_25_1.html` | `/player/4807/25/1/carlik-jones/` | Refs cut-down copy: info box and Clubs box whole, game log cut to two games and the Average row |
| `create_shooting_chart_6_25_1.json`, `create_json_6_25_1.json` | `/live-match/rezultati-1718/create_shooting_chart.php?id=6&sez=25&lea=1`, `create_json.php?id=6&sez=25&lea=1` | Whole bodies (the scope checks need every row) |
| `create_scoredifference_unknown.json` | `create_scoredifference.php?id=9999&sez=25&lea=1` | The 40-row zero placeholder an unknown id gets (refused) |
| `captures/*.json` | the refs `aba-liga/captures/` records | The refs parsed records, every array cut to 3 rows: the per-route fixtures (`calendar@8_1` is 2008-09, `*@25_2*` ABA League 2, `*@17_1_1` 2017-18). HTML pages are banked as the parsed record, never as HTML |
| `spec_paths.json` | `aba-liga.openapi.yaml` | The spec's seven routes, written by `tools/codegen/gen_aba.py` |

Nothing is masked: no official's personal data appears on these pages (names only); the player bio is the public profile.
