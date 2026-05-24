# NHL Stats REST fixture payloads

Captured 2026-05-24 from `https://api.nhle.com/stats/rest/en/`. Used
by `tests/test_nhl_aux_parsers.py` to exercise the
`parse_nhl_stats_rest` parser offline.

| File | Endpoint | Notes |
|---|---|---|
| `stats_rest_season.json`              | `/season`                                        | 108 NHL seasons (1917-18 → present) |
| `stats_rest_franchise.json`           | `/franchise`                                     | 40 franchises (active + defunct) |
| `stats_rest_country.json`             | `/country`                                       | 49 countries with NHL player history |
| `stats_rest_glossary.json`            | `/glossary`                                      | 321 stat definitions |
| `stats_rest_config.json`              | `/config`                                        | Meta config (no `data` key — parser returns 0 rows) |
| `stats_rest_skater_summary_2024.json` | `/skater/summary?cayenneExp=seasonId=20232024…`  | Top 20 by points, 2023-24 regular season |
| `stats_rest_goalie_summary_2024.json` | `/goalie/summary?…`                              | Top 10 goalies, 2023-24 regular season |
| `stats_rest_team_summary_2024.json`   | `/team/summary?…`                                | All 32 teams, 2023-24 regular season |

Every endpoint ships the same `{data: [...], total: N}` shape so a
single parser handles all of them.
