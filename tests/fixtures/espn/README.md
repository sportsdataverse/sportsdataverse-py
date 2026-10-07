<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [ESPN universal-endpoint fixture payloads](#espn-universal-endpoint-fixture-payloads)
  - [Returns-table captures (2026-10-07)](#returns-table-captures-2026-10-07)

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
| `summary_nba.json`           | Site v2 `summary?event=401585607`              | 2024-03-17 regular season TOR@ORL; ~700KB, 19 top-level sections |
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
| `standings_nbagl.json` | Site v2 alt `apis/v2/sports/basketball/nba-development/standings?season=2026` (captured 2026-10-05) | NBA G League 2025-26: 2 conferences, 31 teams. Used by `tests/nbagl/test_nbagl_espn.py` |
| `teams_nbagl.json` | Site v2 `basketball/nba-development/teams` (captured 2026-10-05) | NBA G League, 34 teams |
| `scoreboard_nbagl.json` | Site v2 `basketball/nba-development/scoreboard?dates=20260115&limit=500` (captured 2026-10-05) | NBA G League, 7 completed games on 2026-01-15 |

Endpoints are league-agnostic so capturing against NBA is sufficient — the
parsers run identically against MLB, NFL, NHL, WNBA, MBB, WBB, CFB payloads
of the same shape family.

To refresh, re-capture with the same URLs and overwrite the files
(stem-matched). The parser tests are payload-agnostic so newer captures
will keep working as long as the schema doesn't change.

## Returns-table captures (2026-10-07)

One live payload per ESPN endpoint that had no returns schema, captured by
`tools/codegen/capture_fixtures.py` with the wrapper's documented example (raw, `return_parsed=False`)
for one representative league, then trimmed to 25 elements per list (`recruiting_athletes` to 100,
which keeps every parsed column). `generate.py --schemas` turns each into `schemas/<short>.yaml`.

| file | endpoint | league | URL |
|---|---|---|---|
| `athlete_contracts_nba.json` | espn_core_v2 `athlete_contracts` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239/contracts |
| `athlete_core_nba.json` | espn_core_v2 `athlete_core` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239 |
| `athlete_gamelog_nba.json` | espn_web_v3 `athlete_gamelog` | nba | https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba/athletes/4239/gamelog |
| `athlete_hotzones_mlb.json` | espn_core_v2 `athlete_hotzones` | mlb | https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/athletes/4239/hotzones |
| `athlete_overview_nba.json` | espn_web_v3 `athlete_overview` | nba | https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba/athletes/4239/overview |
| `athlete_seasons_nba.json` | espn_core_v2 `athlete_seasons` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239/seasons |
| `athlete_splits_nba.json` | espn_web_v3 `athlete_splits` | nba | https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba/athletes/4239/splits |
| `athlete_statisticslog_nba.json` | espn_core_v2 `athlete_statisticslog` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes/4239/statisticslog |
| `athletes_index_nba.json` | espn_core_v2 `athletes_index` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/athletes?active=true&limit=100&page=1 |
| `award_nba.json` | espn_core_v2 `award` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/awards/1 |
| `awards_nba.json` | espn_core_v2 `awards` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/awards?limit=200 |
| `coach_mlb.json` | espn_core_v2 `coach` | mlb | https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/coaches/1 |
| `coach_record_mlb.json` | espn_core_v2 `coach_record` | mlb | https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/coaches/1/record/0 |
| `conferences_nba.json` | espn_site_v2 `conferences` | nba | https://site.api.espn.com/apis/site/v2/sports/basketball/nba/groups |
| `event_broadcasts_nba.json` | espn_core_v2 `event_broadcasts` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/broadcasts |
| `event_competition_nba.json` | espn_core_v2 `event_competition` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793 |
| `event_competitor_nba.json` | espn_core_v2 `event_competitor` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/competitors/15 |
| `event_competitor_roster_nba.json` | espn_core_v2 `event_competitor_roster` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/competitors/15/roster |
| `event_competitors_nba.json` | espn_core_v2 `event_competitors` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/competitors |
| `event_nba.json` | espn_core_v2 `event` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793 |
| `event_odds_nba.json` | espn_core_v2 `event_odds` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/odds |
| `event_official_detail_nba.json` | espn_core_v2 `event_official_detail` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/officials/1 |
| `event_officials_nba.json` | espn_core_v2 `event_officials` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/officials |
| `event_play_nba.json` | espn_core_v2 `event_play` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/plays/4015847934 |
| `event_plays_nba.json` | espn_core_v2 `event_plays` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/plays?limit=1000 |
| `event_powerindex_nba.json` | espn_core_v2 `event_powerindex` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/powerindex |
| `event_predictor_nba.json` | espn_core_v2 `event_predictor` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/predictor |
| `event_probabilities_nba.json` | espn_core_v2 `event_probabilities` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/probabilities?limit=300 |
| `event_situation_nba.json` | espn_core_v2 `event_situation` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/situation |
| `event_status_nba.json` | espn_core_v2 `event_status` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/status |
| `events_nba.json` | espn_core_v2 `events` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events?limit=500 |
| `franchise_nba.json` | espn_core_v2 `franchise` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/franchises/2 |
| `franchises_nba.json` | espn_core_v2 `franchises` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/franchises?limit=200 |
| `league_root_nba.json` | espn_core_v2 `league_root` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba |
| `position_nba.json` | espn_core_v2 `position` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/positions/1 |
| `positions_nba.json` | espn_core_v2 `positions` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/positions?limit=200 |
| `recruiting_athletes_cfb.json` | espn_core_v2 `recruiting_athletes` | cfb | https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/recruiting/2026/athletes?limit=1000&page=1 |
| `recruiting_rankings_cfb.json` | espn_core_v2 `recruiting_rankings` | cfb | https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/recruiting/2026/rankings |
| `recruiting_years_cfb.json` | espn_core_v2 `recruiting_years` | cfb | https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/recruiting |
| `season_athletes_nba.json` | espn_core_v2 `season_athletes` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/seasons/2024/athletes?limit=100&page=1 |
| `season_awards_nba.json` | espn_core_v2 `season_awards` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/seasons/2024/awards?limit=200 |
| `season_coaches_nba.json` | espn_core_v2 `season_coaches` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/seasons/2024/coaches?limit=500 |
| `season_futures_nba.json` | espn_core_v2 `season_futures` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/seasons/2024/futures |
| `season_group_cfb.json` | espn_core_v2 `season_group` | cfb | https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/groups/80 |
| `season_groups_nba.json` | espn_core_v2 `season_groups` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/seasons/2024/types/2/groups |
| `season_info_nba.json` | espn_core_v2 `season_info` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/seasons/2024 |
| `season_pointer_nba.json` | espn_core_v2 `season_pointer` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/season |
| `season_powerindex_nba.json` | espn_core_v2 `season_powerindex` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/seasons/2024/powerindex |
| `season_qbr_week_nfl.json` | espn_core_v2 `season_qbr_week` | nfl | https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2/weeks/1/qbr/0 |
| `season_recruits_cfb.json` | espn_core_v2 `season_recruits` | cfb | https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/recruits?limit=1000&page=1 |
| `season_team_nba.json` | espn_core_v2 `season_team` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/seasons/2024/teams/4 |
| `season_teams_nba.json` | espn_core_v2 `season_teams` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/seasons/2024/teams?limit=1000&page=1 |
| `season_type_nba.json` | espn_core_v2 `season_type` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/seasons/2024/types/2 |
| `season_types_nba.json` | espn_core_v2 `season_types` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/seasons/2024/types |
| `season_week_nba.json` | espn_core_v2 `season_week` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/seasons/2024/types/2/weeks/1 |
| `season_week_rankings_cfb.json` | espn_core_v2 `season_week_rankings` | cfb | https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/weeks/1/rankings |
| `season_weeks_nba.json` | espn_core_v2 `season_weeks` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/seasons/2024/types/2/weeks |
| `seasons_nba.json` | espn_core_v2 `seasons` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/seasons?limit=200 |
| `team_core_nba.json` | espn_core_v2 `team_core` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/teams/4 |
| `team_nba.json` | espn_site_v2 `team` | nba | https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/4 |
| `tournaments_nba.json` | espn_core_v2 `tournaments` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/tournaments?limit=200 |
| `transactions_nba.json` | espn_site_v2 `transactions` | nba | https://site.api.espn.com/apis/site/v2/sports/basketball/nba/transactions?limit=500 |
| `venue_nfl.json` | espn_core_v2 `venue` | nfl | https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/venues/3663 |
| `venues_nba.json` | espn_core_v2 `venues` | nba | https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/venues?limit=1000 |
