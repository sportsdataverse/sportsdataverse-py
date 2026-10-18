# FEB fixtures (`intrafeb.feb.es` LiveStats API + `baloncestoenvivo.feb.es`)

| Path | What |
|---|---|
| `full/*.json`, `full/KeyFacts.json.gz` | **Full, untruncated** bodies of ONE game, fetched 2026-10-12 with the public site token: game 2535725 (Primera FEB 2026-27, J1, Albacete 69-95 Palencia): `BoxScore`, `KeyFacts` (570 plays, 350 KB, so gzipped), `ShotChart` (122 shots), `TeamStats`, `Ranking`, plus the `overview` window of that day. The returns-schema dtypes and the dtype tests come from these: the refs captures mistype rates as integers. No bearer token is in any body (a test greps for JWT-shaped strings). |
| `captures/*.json` | The sdv-internal-refs samples (`feb/captures/`, refs commit `38e5cf0`, 2026-10-12), arrays cut to 3 rows: five games per game route (`@2535725`-less names are the primary sample; `@2171130` and `@2207129` are 2020-21 text-only play-by-play, `@2522775` Liga Femenina Endesa 2026-27, `@2536115` Segunda FEB), and the parsed records of the schedule pages (`calendario*.json`, `resultados*.json`: the shape `feb_runtime` returns). |
| `calendario_g1_t2026.html`, `resultados_g1_t2026.html`, `resultados_g1_t2025.html` | Cut-down real pages (refs `tests/fixtures/feb/`): the header block and the content section, season select cut to 3 options plus the selected one, 2 rounds x 2 games on the calendar, 2 rows per results table; the token input is `TOKEN_REDACTED`. The HTML parser's input in the runtime tests. |
| `spec_paths.json` | The spec's routes, written by `tools/codegen/gen_feb.py` so the tests check the YAML without the refs checkout (CI has none). |

`t` in the page names is the site's season START year (`t=2026` is 2026-27, the wrappers' `season=2027`).
