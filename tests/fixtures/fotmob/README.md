# FotMob fixtures

Real, truncated responses from FotMob's unofficial site JSON
(`https://www.fotmob.com/api/data/*`, plus `/api/trendingnews` at the host root),
captured 2026-10-05/06 keyless and copied byte-for-byte from
`sdv-internal-refs/fotmob/captures/` at commit `e330067` (2026-10-06). Every array,
and every id-keyed map with more than 20 keys, is cut to its first 3 entries at every
depth; the top-level key set of each body is intact, so the parser's shape rule sees
exactly what the live route serves.

| File | Route | Example params |
|---|---|---|
| `allLeagues.json` | `/allLeagues` | — |
| `leagues.json` | `/leagues` | `id=47&tab=overview&type=league&timeZone=UTC` |
| `matches.json` | `/matches` | `date=20260301&timezone=UTC` |
| `matchDetails.json` | `/matchDetails` | `matchId=4813647` |
| `table.json` | `/table` | `url=http://data.fotmob.com/tables.ext.47.fot.gz` |
| `teams.json` | `/teams` | `id=8650&tab=overview&type=team&timeZone=UTC` |
| `playerData.json` | `/playerData` | `id=1077894` |
| `search__suggest.json` | `/search/suggest` | `term=haaland&lang=en` |
| `tlnews.json` | `/tlnews` | `id=47&type=league&startIndex=0` |
| `top-transfers.json` | `/top-transfers` | — |
| `tvlistings.json` | `/tvlistings` | `countryCode=GB` |
| `team-of-the-week__rounds.json` | `/team-of-the-week/rounds` | `leagueId=47&season=2025/2026` |
| `audio-matches.json` | `/audio-matches` | — |
| `trendingnews.json` | `/api/trendingnews` (host root) | `lang=en` |

File names follow the reference repo's slug rule (`tools/capture.py`): strip the leading
`/` and `api/`, then `/` -> `__`. Regenerate by re-copying from the reference repo; do
not hand-edit.
