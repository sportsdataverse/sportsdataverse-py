---
title: Test fixtures
sidebar_label: Test fixtures
sidebar_position: 2
---

# Test fixtures

The parser layer is exercised offline against **89 captured live
payloads** spanning 6 fixture directories. This page indexes them
all so contributors who want to extend a parser can find a
representative payload to test against without making a fresh
network call.

Each fixture directory also has its own `README.md` documenting
provenance — see those files for the exact URL each capture was
made from + the date.

## ESPN cross-league — `tests/fixtures/espn/` (43 captures)

### Summary endpoint (8 — one per ESPN league)

The richest payload in the package (~700KB–1.8MB per game).
Captured against championship / playoff games where every section
has data.

| File | Game | Captured |
|---|---|---|
| `summary_nba.json` | 2024 NBA Finals G5 BOS @ DAL | event 401585607 |
| `summary_mlb.json` | 2024 World Series G5 LAD @ NYY | event 401701044 |
| `summary_nfl.json` | Super Bowl LIX KC @ PHI | event 401671889 |
| `summary_nhl.json` | 2024 Stanley Cup Final G7 EDM @ FLA | event 401675111 |
| `summary_wnba.json` | 2024 WNBA Finals G5 MIN @ NY | event 401726992 |
| `summary_mbb.json` | 2024 NCAA M Championship Purdue @ UConn | event 401638645 |
| `summary_wbb.json` | 2024 NCAA W Championship Iowa @ SC | event 401637613 |
| `summary_cfb.json` | 2025 CFB National Championship OSU @ ND | event 401677192 |

### Team-scoped + league-wide endpoints (28 — 4 endpoints × 7 leagues)

Cross-league parity captures for `parse_team_roster`,
`parse_team_schedule`, `parse_news`, `parse_injuries` — one
representative team per league. NHL fixtures use the
`hockey/nhl` slug (ESPN's view); the modern `api-web.nhle.com`
NHL data lives under `tests/fixtures/nhl_api_web/`.

| Endpoint | NBA | MLB | NFL | NHL | WNBA | MBB | WBB | CFB |
|---|---|---|---|---|---|---|---|---|
| `team_roster` | LAL (13) | NYY (10) | KC (12) | EDM (22) | NYL (20) | Duke (150) | UConn (41) | Alabama (333) |
| `team_schedule` | LAL | NYY | KC | EDM | NYL | Duke | UConn | Alabama |
| `news` | NBA-wide | MLB-wide | NFL-wide | NHL-wide | WNBA-wide | MBB-wide | WBB-wide | CFB-wide |
| `injuries` | NBA-wide | MLB-wide | NFL-wide | NHL-wide | WNBA-wide | (empty) | (empty) | CFB-wide |

**Shape divergence captured**: MBB/WBB league-wide injuries return
empty payloads because ESPN doesn't publish them; NHL/CFB schedules
sometimes return empty because of off-season (NHL May, CFB May).
The parsers handle all these as zero-row frames.

### Core v2 single + list endpoints (7)

| File | Endpoint |
|---|---|
| `venues_core_nba.json` | Core v2 `venues?limit=5` — 5 `$ref`-only items |
| `events_core_nba.json` | Core v2 `events?limit=3` — 1 `$ref` item (off-season) |
| `athlete_statslog_lbj.json` | Core v2 `athletes/1966/statisticslog` — LeBron, 23 `entries` |

## MLB Stats API — `tests/fixtures/mlb_api/` (8 captures)

Captured from `statsapi.mlb.com`. Used by `tests/test_mlb_api_parsers.py`.

| File | Endpoint | Notes |
|---|---|---|
| `schedule_2024_09_29.json` | `/schedule?sportId=1&date=2024-09-29` | 15 games, final regular-season day |
| `teams_2024.json` | `/teams?sportId=1&season=2024` | 30 MLB teams |
| `team_roster_yankees_2024.json` | `/teams/147/roster?season=2024` | NYY ~54 players |
| `standings_2024.json` | `/standings?leagueId=103,104&season=2024` | 6 divisions × 5 teams |
| `person_stats_judge_2024.json` | `/people/592450/stats?stats=season&season=2024` | Aaron Judge season splits |
| `venues_active.json` | `/venues?activeStatus=Y` | 1,646 active venues |
| `sports.json` | `/sports` | 20 sport IDs |
| `divisions.json` | `/divisions` | 61 divisions |

## NHL api-web — `tests/fixtures/nhl_api_web/` (17 captures)

Captured from `api-web.nhle.com/v1/`. Used by
`tests/test_nhl_api_web_parsers.py`. Most game-center fixtures
focus on the 2024 Stanley Cup Final G7 (game 2023030417) so the
boxscore / pbp / landing / right-rail captures all cross-reference.

| File | Endpoint |
|---|---|
| `pbp_2024_scf_g7.json` | `/gamecenter/2023030417/play-by-play` — 331 plays |
| `boxscore_2024_scf_g7.json` | `/gamecenter/2023030417/boxscore` |
| `landing_2024_scf_g7.json` | `/gamecenter/2023030417/landing` |
| `right_rail_2024_scf_g7.json` | `/gamecenter/2023030417/right-rail` — 6 sub-frames |
| `schedule_2024_06_24.json` | `/schedule/2024-06-24` |
| `score_2024_06_24.json` | `/score/2024-06-24` |
| `scoreboard_now.json` | `/scoreboard/now` |
| `standings_now.json` | `/standings/now` — 32 teams × 84 cols |
| `standings_season.json` | `/standings-season` — 108 NHL seasons |
| `club_schedule_edm_2024.json` | `/club-schedule-season/EDM/20232024` |
| `club_stats_edm_2024.json` | `/club-stats/EDM/20232024/2` |
| `roster_edm_2024.json` | `/roster/EDM/20232024` |
| `player_mcdavid_landing.json` | `/player/8478402/landing` — 130-col profile |
| `player_mcdavid_gamelog.json` | `/player/8478402/game-log/20232024/2` — 76 games |
| `skater_leaders_now.json` | `/skater-stats-leaders/current?categories=points&limit=10` |
| `goalie_leaders_now.json` | `/goalie-stats-leaders/current?categories=wins&limit=10` |
| `draft_picks_2024_r1.json` | `/draft/picks/2024/1` — 32 first-round picks |

## NHL EDGE — `tests/fixtures/nhl_edge/` (7 captures)

Captured from `api-web.nhle.com/v1/edge/*`. Used by
`tests/test_nhl_edge_parsers.py`. Player-tracking / Statcast-style
deep dives focused on Connor McDavid + an EDM/EDG team comparison.

| File | Endpoint |
|---|---|
| `skater_detail.json` | `/edge/skater-detail/8478402/20242025/2` — McDavid |
| `skater_zone_time.json` | `/edge/skater-zone-time/8478402/20242025/2` — 4-row strength splits |
| `skater_shot_speed.json` | `/edge/skater-shot-speed-detail/8478402/20242025/2` — + 10-row `hardestShots` |
| `team_detail.json` | `/edge/team-detail/22/20242025/2` — EDM |
| `team_shot_loc.json` | `/edge/team-shot-location-detail/22/20242025/2` — 17-cell grid + 12-row totals |
| `goalie_detail.json` | `/edge/goalie-detail/8480313/20242025/2` — Stuart Skinner |
| `goalie_shot_loc.json` | `/edge/goalie-shot-location-detail/8480313/20242025/2` — 17-cell grid + 4-row totals |

## NHL Stats REST — `tests/fixtures/nhl_stats_rest/` (8 captures)

Captured from `api.nhle.com/stats/rest/en/`. Used by
`tests/test_nhl_aux_parsers.py` (shared with the Records fixtures).

| File | Endpoint | Notes |
|---|---|---|
| `stats_rest_season.json` | `/season` | 108 NHL seasons (1917-18 → present) |
| `stats_rest_franchise.json` | `/franchise` | 40 franchises (active + defunct) |
| `stats_rest_country.json` | `/country` | 49 countries with NHL player history |
| `stats_rest_glossary.json` | `/glossary` | 321 stat definitions |
| `stats_rest_config.json` | `/config` | Meta (no `data` key → 0 rows) |
| `stats_rest_skater_summary_2024.json` | `/skater/summary?cayenneExp=…` | Top 20 by points, 2023-24 regular season |
| `stats_rest_goalie_summary_2024.json` | `/goalie/summary?…` | Top 10 goalies |
| `stats_rest_team_summary_2024.json` | `/team/summary?…` | All 32 teams |

## NHL Records — `tests/fixtures/nhl_records/` (6 captures)

Captured from `records.nhl.com/site/api/`. Used by
`tests/test_nhl_aux_parsers.py`.

| File | Endpoint | Notes |
|---|---|---|
| `records_franchise.json` | `/franchise` | 40 franchises |
| `records_franchise_team_totals.json` | `/franchise-team-totals?limit=10` | 10 of 120 |
| `records_coach.json` | `/coach?limit=10` | 10 of 574 |
| `records_draft.json` | `/draft?limit=10` | 10 of 13,152 |
| `records_player_records.json` | `/player?limit=10` | 10 of 23,313 |
| `records_attendance.json` | `/attendance` | 80 years of attendance |

## Maintenance

To refresh a fixture, re-capture from the URL in the directory's
`README.md` and overwrite the file (stems must match what the test
loaders expect). The parser tests are payload-agnostic so newer
captures will keep working as long as the schema doesn't change —
when a schema does drift, the offline tests catch it immediately
and the weekly cron drift detector
(`.github/workflows/live-tests-cron.yml`) catches API-level changes
even when the repo is otherwise quiet.

## See also

- [Parsers (general overview)](./index.md)
- [Architecture](../architecture/espn-cross-league)
