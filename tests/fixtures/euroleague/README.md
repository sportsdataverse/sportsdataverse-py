# EuroLeague fixtures

Real, trimmed responses from the EuroLeague Competition Engine API
(`https://api-live.euroleague.net/v2`, keyless, `Accept: application/json`), captured
2026-10-05 and copied byte-for-byte from `sdv-internal-refs/euroleague/captures/E2025/`
(source commit `e330067`, copied 2026-10-06). Season `E2025` (2025-26), game code `1`.
Every array is cut to its first 3 elements at every depth.

| File | Route |
|---|---|
| `competitions.json` | `/competitions` |
| `competitions__E__seasons.json` | `/competitions/E/seasons` |
| `competitions__E__seasons__E2025__rounds.json` | `/competitions/E/seasons/E2025/rounds` |
| `competitions__E__seasons__E2025__clubs.json` | `/competitions/E/seasons/E2025/clubs` |
| `competitions__E__seasons__E2025__people.json` | `/competitions/E/seasons/E2025/people?limit=3` |
| `competitions__E__seasons__E2025__games.json` | `/competitions/E/seasons/E2025/games?limit=3` |
| `competitions__E__seasons__E2025__games__1__stats.json` | `/competitions/E/seasons/E2025/games/1/stats` |

List routes answer `{"total": n, "data": [...]}`; the box score is a `{"local", "road"}`
page object. Regenerate by re-copying from the reference repo; do not hand-edit.
