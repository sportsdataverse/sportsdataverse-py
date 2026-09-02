<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [ESPN universal-endpoint fixture payloads](#espn-universal-endpoint-fixture-payloads)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

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
| `recruiting_years_mbb.json`  | Core v2 MBB `recruiting` (captured 2026-07-07) | 23 `$ref`-only year items |
| `recruiting_athletes_mbb_2026.json` | Core v2 MBB `recruiting/2026/athletes?limit=5` (captured 2026-07-07) | 5 INLINE athlete objects (not $ref-only) |
| `recruiting_rankings_mbb_2026.json` | Core v2 MBB `recruiting/2026/rankings` (captured 2026-07-07) | 1 `$ref` ranking-set item ("ESPN Class Rankings") |
| `summary_nba.json`           | Site v2 `summary?event=401585607`              | 2024 NBA Finals G5 BOS@DAL; ~700KB, 19 top-level sections |
| `summary_mlb.json`           | Site v2 `summary?event=401701044`              | 2024 World Series G5 LAD@NYY; ~1.8MB, 22 top-level sections |
| `summary_nfl.json`           | Site v2 `summary?event=401671889`              | Super Bowl LIX KC@PHI; ~950KB, 19 sections (uses drives.previous[]) |
| `summary_nhl.json`           | Site v2 `summary?event=401675111`              | 2024 Stanley Cup Final G7 EDM@FLA; ~880KB, 19 sections (no winprob) |
| `summary_wnba.json`          | Site v2 `summary?event=401726992`              | 2024 WNBA Finals G5 MIN@NY; ~760KB, 19 sections |
| `team_roster_{mlb,nfl,nhl,wnba}.json` | Site v2 `teams/{id}/roster` | Cross-league parity captures for `parse_team_roster` (MLB=NYY id 10, NFL=KC id 12, NHL=EDM id 22, WNBA=NYL id 20) |
| `team_schedule_{mlb,nfl,nhl,wnba}.json` | Site v2 `teams/{id}/schedule` | Cross-league captures for `parse_team_schedule` |
| `news_{mlb,nfl,nhl,wnba}.json` | Site v2 `news?limit=5` | Cross-league captures for `parse_news` |
| `injuries_{mlb,nfl,nhl,wnba}.json` | Site v2 `injuries` | Cross-league captures for `parse_injuries`; NFL is the largest (~15 MB) |
| `depthcharts_{nfl,nba,mlb}.json` | Site v2 `teams/{id}/depthcharts` (captured 2026-09-02) | One team each for `parse_depthchart_snapshot`: NFL=ARI id 22 (3 groups / 68 slots, incl. the wr1/wr2/wr3 slots that share one position id), NBA=ATL id 1 (1 / 39), MLB=SEA id 29 (1 / 76) |
| `depthcharts_nhl.json` | Site v2 `teams/25/depthcharts` (captured 2026-09-02) | The empty case, and the reason NHL/WNBA/CFB are excluded: HTTP 200 with the `depthchart` key **absent entirely** (558 bytes) |

Endpoints are league-agnostic so capturing against NBA is sufficient — the
parsers run identically against MLB, NFL, NHL, WNBA, MBB, WBB, CFB payloads
of the same shape family.

To refresh, re-capture with the same URLs and overwrite the files
(stem-matched). The parser tests are payload-agnostic so newer captures
will keep working as long as the schema doesn't change.
