# NHL EDGE fixture payloads

Captured 2026-05-23 from `api-web.nhle.com/v1/edge/*` against the
2024-25 regular season. Used by `tests/test_nhl_edge_parsers.py` to
exercise parsers offline.

| File | Endpoint | Notes |
|---|---|---|
| `skater_detail.json` | `/edge/skater-detail/8478402/20242025/2` | Connor McDavid |
| `skater_zone_time.json` | `/edge/skater-zone-time/8478402/20242025/2` | 4-row strength splits |
| `skater_shot_speed.json` | `/edge/skater-shot-speed-detail/8478402/20242025/2` | + 10-row `hardestShots` |
| `team_detail.json` | `/edge/team-detail/22/20242025/2` | EDM |
| `team_shot_loc.json` | `/edge/team-shot-location-detail/22/20242025/2` | 17 cells + 12-row totals |
| `goalie_detail.json` | `/edge/goalie-detail/8480313/20242025/2` | Stuart Skinner |
| `goalie_shot_loc.json` | `/edge/goalie-shot-location-detail/8480313/20242025/2` | 17 cells + 4-row totals |

To refresh, capture new payloads with the same URL patterns and place
them here (same stems) — the parser tests are payload-agnostic so newer
captures will keep working as long as the schema doesn't change.
