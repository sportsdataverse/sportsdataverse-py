# ESPN universal-endpoint fixture payloads

Captured 2026-05-23 from `site.api.espn.com` and `sports.core.api.espn.com`
against the NBA league. Used by `tests/test_espn_universal_parsers.py`.

| File | Endpoint | Notes |
|---|---|---|
| `team_schedule_nba.json`     | Site v2 `teams/13/schedule`                    | LAL, 10 events |
| `team_roster_nba.json`       | Site v2 `teams/13/roster`                      | LAL, 17 athletes + 1 coach |
| `news_nba.json`              | Site v2 `news?limit=5`                         | 5 articles |
| `injuries_nba.json`          | Site v2 `injuries`                             | 26 teams reporting |
| `venues_core_nba.json`       | Core v2 `venues?limit=5`                       | 5 `$ref`-only items |
| `events_core_nba.json`       | Core v2 `events?limit=3`                       | 1 `$ref` item (off-season) |
| `athlete_statslog_lbj.json`  | Core v2 `athletes/1966/statisticslog`          | LeBron, 23 `entries` |

Endpoints are league-agnostic so capturing against NBA is sufficient — the
parsers run identically against MLB, NFL, NHL, WNBA, MBB, WBB, CFB payloads
of the same shape family.

To refresh, re-capture with the same URLs and overwrite the files
(stem-matched). The parser tests are payload-agnostic so newer captures
will keep working as long as the schema doesn't change.
