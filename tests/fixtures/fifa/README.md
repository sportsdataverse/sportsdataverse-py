# FIFA fixtures

Real, truncated first-page responses from the keyless FIFA public API v3
(`https://api.fifa.com/api/v3/...`), captured 2026-10-06 and copied byte-for-byte
from `sdv-internal-refs/fifa/captures/` at commit `e330067` (2026-10-06).

File names follow the capture slug rule (`tools/capture.py`): the route path with the
leading slash dropped and `/` replaced by `__`, path ids substituted by the captured
example (`17` = FIFA World Cup, `43922` = Argentina).

| File | Route | Query |
|---|---|---|
| `calendar__matches.json` | `/calendar/matches` | `idCompetition=17&idSeason=285023&language=en&count=3` |
| `competitions.json` | `/competitions` | `language=en&count=3` |
| `competitions__17.json` | `/competitions/{idCompetition}` | `language=en` |
| `live__football.json` | `/live/football` | `language=en` |
| `players__search.json` | `/players/search` | `name=Messi&language=en` |
| `seasons.json` | `/seasons` | `idCompetition=17&language=en&count=3` |
| `stadiums.json` | `/stadiums` | `language=en&count=3` |
| `teams__43922.json` | `/teams/{idTeam}` | `language=en` |
| `teams__search.json` | `/teams/search` | `name=Argentina&language=en` |

List routes answer the `{ContinuationToken, ContinuationHash, Results: [...]}` envelope
(first page only; every array cut to 3 elements at every depth); `/competitions/{id}`
and `/teams/{id}` answer one object. Regenerate by re-copying from the reference repo;
do not hand-edit.
