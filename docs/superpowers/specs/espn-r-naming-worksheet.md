<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [ESPN -> R naming curation worksheet (for review)](#espn---r-naming-curation-worksheet-for-review)
  - [Totals](#totals)
  - [nba (vs hoopR)](#nba-vs-hoopr)
    - [generated-only — decide name](#generated-only--decide-name)
    - [R-only exports (candidate rename targets / not yet wrapped)](#r-only-exports-candidate-rename-targets--not-yet-wrapped)
  - [mbb (vs hoopR)](#mbb-vs-hoopr)
    - [generated-only — decide name](#generated-only--decide-name-1)
    - [R-only exports (candidate rename targets / not yet wrapped)](#r-only-exports-candidate-rename-targets--not-yet-wrapped-1)
  - [wnba (vs wehoop)](#wnba-vs-wehoop)
    - [generated-only — decide name](#generated-only--decide-name-2)
    - [R-only exports (candidate rename targets / not yet wrapped)](#r-only-exports-candidate-rename-targets--not-yet-wrapped-2)
  - [wbb (vs wehoop)](#wbb-vs-wehoop)
    - [generated-only — decide name](#generated-only--decide-name-3)
    - [R-only exports (candidate rename targets / not yet wrapped)](#r-only-exports-candidate-rename-targets--not-yet-wrapped-3)
  - [cfb (vs cfbfastR)](#cfb-vs-cfbfastr)
    - [generated-only — decide name](#generated-only--decide-name-4)
    - [R-only exports (candidate rename targets / not yet wrapped)](#r-only-exports-candidate-rename-targets--not-yet-wrapped-4)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# ESPN -> R naming curation worksheet (for review)

## Totals

| league | generated | exact match | generated-only | R-only |
|---|--:|--:|--:|--:|
Behavior of the generated wrappers is fixed (Plan 2, URL-parity-proven). This worksheet is for deciding **public names** vs the R sister packages. Fill the `-> ?` column for the generated-only rows: `rename to <r_name>`, `keep`, or `qualify` (collision fallback).

| cfb | 120 | 24 | 96 | 48 |
| wbb | 118 | 62 | 56 | 26 |
| wnba | 115 | 62 | 53 | 30 |
| mbb | 118 | 63 | 55 | 31 |
| nba | 115 | 66 | 49 | 39 |

## nba (vs hoopR)

- generated: 115 | exact-name match (no rename): 66 | generated-only: 49 | R-only: 39

### generated-only — decide name

| generated | host | path | -> ? |
|-----------|------|------|------|
| `espn_nba_athlete_bio` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}/bio` |  |
| `espn_nba_athlete_core` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}` |  |
| `espn_nba_athlete_injuries` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/injuries` |  |
| `espn_nba_athlete_news` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}/news` |  |
| `espn_nba_athlete_notes` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/notes` |  |
| `espn_nba_athlete_records` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/records` |  |
| `espn_nba_athlete_vs_athlete` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/vsathlete/{opp_id}` |  |
| `espn_nba_awards` | core_v2 | `/{sport}/leagues/{league}/awards` |  |
| `espn_nba_calendar_offseason` | site_v2 | `/{sport}/{league}/calendar/offseason` |  |
| `espn_nba_calendar_ondays` | site_v2 | `/{sport}/{league}/calendar/ondays` |  |
| `espn_nba_calendar_postseason` | site_v2 | `/{sport}/{league}/calendar/postseason` |  |
| `espn_nba_calendar_regular_season` | site_v2 | `/{sport}/{league}/calendar/regular-season` |  |
| `espn_nba_event` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}` |  |
| `espn_nba_event_competition` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}` |  |
| `espn_nba_event_competitor` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}` |  |
| `espn_nba_event_competitor_record` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/record` |  |
| `espn_nba_event_competitors` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors` |  |
| `espn_nba_event_leaders` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/leaders` |  |
| `espn_nba_event_plays` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/plays` |  |
| `espn_nba_event_scoringplays` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/scoringplays` |  |
| `espn_nba_event_status` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/status` |  |
| `espn_nba_events` | core_v2 | `/{sport}/leagues/{league}/events` |  |
| `espn_nba_leaders_core` | core_v2 | `/{sport}/leagues/{league}/leaders` |  |
| `espn_nba_league_notes` | core_v2 | `/{sport}/leagues/{league}/notes` |  |
| `espn_nba_league_root` | core_v2 | `/{sport}/leagues/{league}` |  |
| `espn_nba_season_athletes` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/athletes` |  |
| `espn_nba_season_coaches` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/coaches` |  |
| `espn_nba_season_draft_round_picks` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/draft/rounds/{round_num}/picks` |  |
| `espn_nba_season_freeagents` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/freeagents` |  |
| `espn_nba_season_futures` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/futures` |  |
| `espn_nba_season_pointer` | core_v2 | `/{sport}/leagues/{league}/season` |  |
| `espn_nba_season_powerindex` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/powerindex[/{team_id}]` |  |
| `espn_nba_season_powerindex_leaders` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/powerindex/leaders` |  |
| `espn_nba_season_team` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/teams/{team_id}` |  |
| `espn_nba_season_teams` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/teams` |  |
| `espn_nba_season_type_corrections` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/corrections` |  |
| `espn_nba_season_type_leaders` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/leaders` |  |
| `espn_nba_season_week_events` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}/events` |  |
| `espn_nba_standings_core` | core_v2 | `/{sport}/leagues/{league}/standings` |  |
| `espn_nba_statistics_league` | site_v2 | `/{sport}/{league}/statistics` |  |
| `espn_nba_summary` | site_v2 | `/{sport}/{league}/summary` |  |
| `espn_nba_talentpicks` | core_v2 | `/{sport}/leagues/{league}/talentpicks` |  |
| `espn_nba_team_core` | core_v2 | `/{sport}/leagues/{league}/teams/{team_id}` |  |
| `espn_nba_team_depthcharts` | site_v2 | `/{sport}/{league}/teams/{team_id}/depthcharts` |  |
| `espn_nba_team_history` | site_v2 | `/{sport}/{league}/teams/{team_id}/history` |  |
| `espn_nba_team_transactions` | site_v2 | `/{sport}/{league}/teams/{team_id}/transactions` |  |
| `espn_nba_teams_core` | core_v2 | `/{sport}/leagues/{league}/teams` |  |
| `espn_nba_teams_site` | site_v2 | `/{sport}/{league}/teams` |  |
| `espn_nba_venue` | core_v2 | `/{sport}/leagues/{league}/venues/{venue_id}` |  |

### R-only exports (candidate rename targets / not yet wrapped)

`espn_nba_athlete_contract`, `espn_nba_athlete_eventlog_v2`, `espn_nba_betting`, `espn_nba_draft_athlete_detail`, `espn_nba_draft_athletes`, `espn_nba_draft_pick`, `espn_nba_draft_rounds`, `espn_nba_draft_status`, `espn_nba_event_competitor_records`, `espn_nba_event_competitor_roster_entry`, `espn_nba_event_competitor_score`, `espn_nba_event_player_box`, `espn_nba_freeagents`, `espn_nba_futures`, `espn_nba_game_all`, `espn_nba_game_rosters`, `espn_nba_pbp`, `espn_nba_player_box`, `espn_nba_player_stats`, `espn_nba_powerindex`, `espn_nba_season_leaders`, `espn_nba_season_ranking`, `espn_nba_season_rankings`, `espn_nba_team_box`, `espn_nba_team_current_roster`, `espn_nba_team_depthchart`, `espn_nba_team_odds_records`, `espn_nba_team_record_detail`, `espn_nba_team_season_profile`, `espn_nba_team_season_roster`, `espn_nba_team_season_statistics`, `espn_nba_team_stats`, `espn_nba_teams`, `espn_nba_tournament`, `espn_nba_tournament_season`, `espn_nba_tournament_seasons`, `espn_nba_week_ranking`, `espn_nba_week_rankings`, `espn_nba_wp`

## mbb (vs hoopR)

- generated: 118 | exact-name match (no rename): 63 | generated-only: 55 | R-only: 31

### generated-only — decide name

| generated | host | path | -> ? |
|-----------|------|------|------|
| `espn_mbb_athlete_bio` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}/bio` |  |
| `espn_mbb_athlete_contracts` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/contracts` |  |
| `espn_mbb_athlete_core` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}` |  |
| `espn_mbb_athlete_injuries` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/injuries` |  |
| `espn_mbb_athlete_news` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}/news` |  |
| `espn_mbb_athlete_notes` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/notes` |  |
| `espn_mbb_athlete_records` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/records` |  |
| `espn_mbb_athlete_vs_athlete` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/vsathlete/{opp_id}` |  |
| `espn_mbb_awards` | core_v2 | `/{sport}/leagues/{league}/awards` |  |
| `espn_mbb_calendar_offseason` | site_v2 | `/{sport}/{league}/calendar/offseason` |  |
| `espn_mbb_calendar_ondays` | site_v2 | `/{sport}/{league}/calendar/ondays` |  |
| `espn_mbb_calendar_postseason` | site_v2 | `/{sport}/{league}/calendar/postseason` |  |
| `espn_mbb_calendar_regular_season` | site_v2 | `/{sport}/{league}/calendar/regular-season` |  |
| `espn_mbb_draft` | site_v2 | `/{sport}/{league}/draft` |  |
| `espn_mbb_event` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}` |  |
| `espn_mbb_event_competition` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}` |  |
| `espn_mbb_event_competitor` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}` |  |
| `espn_mbb_event_competitor_record` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/record` |  |
| `espn_mbb_event_competitors` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors` |  |
| `espn_mbb_event_leaders` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/leaders` |  |
| `espn_mbb_event_plays` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/plays` |  |
| `espn_mbb_event_scoringplays` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/scoringplays` |  |
| `espn_mbb_event_status` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/status` |  |
| `espn_mbb_events` | core_v2 | `/{sport}/leagues/{league}/events` |  |
| `espn_mbb_leaders_core` | core_v2 | `/{sport}/leagues/{league}/leaders` |  |
| `espn_mbb_league_notes` | core_v2 | `/{sport}/leagues/{league}/notes` |  |
| `espn_mbb_league_root` | core_v2 | `/{sport}/leagues/{league}` |  |
| `espn_mbb_season_athletes` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/athletes` |  |
| `espn_mbb_season_coaches` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/coaches` |  |
| `espn_mbb_season_draft` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/draft` |  |
| `espn_mbb_season_draft_round_picks` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/draft/rounds/{round_num}/picks` |  |
| `espn_mbb_season_freeagents` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/freeagents` |  |
| `espn_mbb_season_futures` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/futures` |  |
| `espn_mbb_season_pointer` | core_v2 | `/{sport}/leagues/{league}/season` |  |
| `espn_mbb_season_powerindex` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/powerindex[/{team_id}]` |  |
| `espn_mbb_season_powerindex_leaders` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/powerindex/leaders` |  |
| `espn_mbb_season_recruits` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/recruits` |  |
| `espn_mbb_season_team` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/teams/{team_id}` |  |
| `espn_mbb_season_teams` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/teams` |  |
| `espn_mbb_season_type_corrections` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/corrections` |  |
| `espn_mbb_season_type_leaders` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/leaders` |  |
| `espn_mbb_season_week_events` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}/events` |  |
| `espn_mbb_season_week_rankings` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}/rankings` |  |
| `espn_mbb_standings_core` | core_v2 | `/{sport}/leagues/{league}/standings` |  |
| `espn_mbb_statistics_league` | site_v2 | `/{sport}/{league}/statistics` |  |
| `espn_mbb_summary` | site_v2 | `/{sport}/{league}/summary` |  |
| `espn_mbb_talentpicks` | core_v2 | `/{sport}/leagues/{league}/talentpicks` |  |
| `espn_mbb_team_core` | core_v2 | `/{sport}/leagues/{league}/teams/{team_id}` |  |
| `espn_mbb_team_depthcharts` | site_v2 | `/{sport}/{league}/teams/{team_id}/depthcharts` |  |
| `espn_mbb_team_history` | site_v2 | `/{sport}/{league}/teams/{team_id}/history` |  |
| `espn_mbb_team_transactions` | site_v2 | `/{sport}/{league}/teams/{team_id}/transactions` |  |
| `espn_mbb_teams_core` | core_v2 | `/{sport}/leagues/{league}/teams` |  |
| `espn_mbb_teams_site` | site_v2 | `/{sport}/{league}/teams` |  |
| `espn_mbb_transactions` | site_v2 | `/{sport}/{league}/transactions` |  |
| `espn_mbb_venue` | core_v2 | `/{sport}/leagues/{league}/venues/{venue_id}` |  |

### R-only exports (candidate rename targets / not yet wrapped)

`espn_mbb_athlete_eventlog_v2`, `espn_mbb_betting`, `espn_mbb_event_competitor_records`, `espn_mbb_event_competitor_roster_entry`, `espn_mbb_event_competitor_score`, `espn_mbb_event_player_box`, `espn_mbb_futures`, `espn_mbb_game_all`, `espn_mbb_game_rosters`, `espn_mbb_pbp`, `espn_mbb_player_box`, `espn_mbb_player_stats`, `espn_mbb_powerindex`, `espn_mbb_season_leaders`, `espn_mbb_season_ranking`, `espn_mbb_season_rankings`, `espn_mbb_team_box`, `espn_mbb_team_current_roster`, `espn_mbb_team_odds_records`, `espn_mbb_team_record_detail`, `espn_mbb_team_season_profile`, `espn_mbb_team_season_roster`, `espn_mbb_team_season_statistics`, `espn_mbb_team_stats`, `espn_mbb_teams`, `espn_mbb_tournament`, `espn_mbb_tournament_season`, `espn_mbb_tournament_seasons`, `espn_mbb_week_ranking`, `espn_mbb_week_rankings`, `espn_mbb_wp`

## wnba (vs wehoop)

- generated: 115 | exact-name match (no rename): 62 | generated-only: 53 | R-only: 30

### generated-only — decide name

| generated | host | path | -> ? |
|-----------|------|------|------|
| `espn_wnba_athlete_bio` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}/bio` |  |
| `espn_wnba_athlete_contracts` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/contracts` |  |
| `espn_wnba_athlete_core` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}` |  |
| `espn_wnba_athlete_injuries` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/injuries` |  |
| `espn_wnba_athlete_news` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}/news` |  |
| `espn_wnba_athlete_notes` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/notes` |  |
| `espn_wnba_athlete_records` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/records` |  |
| `espn_wnba_athlete_vs_athlete` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/vsathlete/{opp_id}` |  |
| `espn_wnba_awards` | core_v2 | `/{sport}/leagues/{league}/awards` |  |
| `espn_wnba_calendar_offseason` | site_v2 | `/{sport}/{league}/calendar/offseason` |  |
| `espn_wnba_calendar_ondays` | site_v2 | `/{sport}/{league}/calendar/ondays` |  |
| `espn_wnba_calendar_postseason` | site_v2 | `/{sport}/{league}/calendar/postseason` |  |
| `espn_wnba_calendar_regular_season` | site_v2 | `/{sport}/{league}/calendar/regular-season` |  |
| `espn_wnba_coach` | core_v2 | `/{sport}/leagues/{league}/coaches/{coach_id}` |  |
| `espn_wnba_coach_record` | core_v2 | `/{sport}/leagues/{league}/coaches/{coach_id}/record/{record_type}` |  |
| `espn_wnba_event` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}` |  |
| `espn_wnba_event_competition` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}` |  |
| `espn_wnba_event_competitor` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}` |  |
| `espn_wnba_event_competitor_record` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/record` |  |
| `espn_wnba_event_competitors` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors` |  |
| `espn_wnba_event_leaders` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/leaders` |  |
| `espn_wnba_event_plays` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/plays` |  |
| `espn_wnba_event_scoringplays` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/scoringplays` |  |
| `espn_wnba_event_status` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/status` |  |
| `espn_wnba_events` | core_v2 | `/{sport}/leagues/{league}/events` |  |
| `espn_wnba_leaders_core` | core_v2 | `/{sport}/leagues/{league}/leaders` |  |
| `espn_wnba_league_notes` | core_v2 | `/{sport}/leagues/{league}/notes` |  |
| `espn_wnba_league_root` | core_v2 | `/{sport}/leagues/{league}` |  |
| `espn_wnba_season_athletes` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/athletes` |  |
| `espn_wnba_season_coaches` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/coaches` |  |
| `espn_wnba_season_draft_round_picks` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/draft/rounds/{round_num}/picks` |  |
| `espn_wnba_season_freeagents` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/freeagents` |  |
| `espn_wnba_season_futures` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/futures` |  |
| `espn_wnba_season_pointer` | core_v2 | `/{sport}/leagues/{league}/season` |  |
| `espn_wnba_season_powerindex` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/powerindex[/{team_id}]` |  |
| `espn_wnba_season_powerindex_leaders` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/powerindex/leaders` |  |
| `espn_wnba_season_team` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/teams/{team_id}` |  |
| `espn_wnba_season_teams` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/teams` |  |
| `espn_wnba_season_type_corrections` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/corrections` |  |
| `espn_wnba_season_type_leaders` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/leaders` |  |
| `espn_wnba_season_week_events` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}/events` |  |
| `espn_wnba_standings_core` | core_v2 | `/{sport}/leagues/{league}/standings` |  |
| `espn_wnba_statistics_league` | site_v2 | `/{sport}/{league}/statistics` |  |
| `espn_wnba_summary` | site_v2 | `/{sport}/{league}/summary` |  |
| `espn_wnba_talentpicks` | core_v2 | `/{sport}/leagues/{league}/talentpicks` |  |
| `espn_wnba_team_core` | core_v2 | `/{sport}/leagues/{league}/teams/{team_id}` |  |
| `espn_wnba_team_depthcharts` | site_v2 | `/{sport}/{league}/teams/{team_id}/depthcharts` |  |
| `espn_wnba_team_history` | site_v2 | `/{sport}/{league}/teams/{team_id}/history` |  |
| `espn_wnba_team_transactions` | site_v2 | `/{sport}/{league}/teams/{team_id}/transactions` |  |
| `espn_wnba_teams_core` | core_v2 | `/{sport}/leagues/{league}/teams` |  |
| `espn_wnba_teams_site` | site_v2 | `/{sport}/{league}/teams` |  |
| `espn_wnba_tournaments` | core_v2 | `/{sport}/leagues/{league}/tournaments` |  |
| `espn_wnba_venue` | core_v2 | `/{sport}/leagues/{league}/venues/{venue_id}` |  |

### R-only exports (candidate rename targets / not yet wrapped)

`espn_wnba_athlete_eventlog_v2`, `espn_wnba_draft_athlete_detail`, `espn_wnba_draft_athletes`, `espn_wnba_draft_pick`, `espn_wnba_draft_rounds`, `espn_wnba_draft_status`, `espn_wnba_event_competitor_records`, `espn_wnba_event_competitor_roster_entry`, `espn_wnba_event_competitor_score`, `espn_wnba_event_player_box`, `espn_wnba_freeagents`, `espn_wnba_futures`, `espn_wnba_game_all`, `espn_wnba_game_rosters`, `espn_wnba_pbp`, `espn_wnba_player_box`, `espn_wnba_player_stats`, `espn_wnba_powerindex`, `espn_wnba_season_leaders`, `espn_wnba_season_ranking`, `espn_wnba_season_rankings`, `espn_wnba_team_box`, `espn_wnba_team_record_detail`, `espn_wnba_team_season_profile`, `espn_wnba_team_season_roster`, `espn_wnba_team_season_statistics`, `espn_wnba_team_stats`, `espn_wnba_teams`, `espn_wnba_week_ranking`, `espn_wnba_week_rankings`

## wbb (vs wehoop)

- generated: 118 | exact-name match (no rename): 62 | generated-only: 56 | R-only: 26

### generated-only — decide name

| generated | host | path | -> ? |
|-----------|------|------|------|
| `espn_wbb_athlete_bio` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}/bio` |  |
| `espn_wbb_athlete_contracts` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/contracts` |  |
| `espn_wbb_athlete_core` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}` |  |
| `espn_wbb_athlete_injuries` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/injuries` |  |
| `espn_wbb_athlete_news` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}/news` |  |
| `espn_wbb_athlete_notes` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/notes` |  |
| `espn_wbb_athlete_records` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/records` |  |
| `espn_wbb_athlete_vs_athlete` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/vsathlete/{opp_id}` |  |
| `espn_wbb_awards` | core_v2 | `/{sport}/leagues/{league}/awards` |  |
| `espn_wbb_calendar_offseason` | site_v2 | `/{sport}/{league}/calendar/offseason` |  |
| `espn_wbb_calendar_ondays` | site_v2 | `/{sport}/{league}/calendar/ondays` |  |
| `espn_wbb_calendar_postseason` | site_v2 | `/{sport}/{league}/calendar/postseason` |  |
| `espn_wbb_calendar_regular_season` | site_v2 | `/{sport}/{league}/calendar/regular-season` |  |
| `espn_wbb_draft` | site_v2 | `/{sport}/{league}/draft` |  |
| `espn_wbb_event` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}` |  |
| `espn_wbb_event_competition` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}` |  |
| `espn_wbb_event_competitor` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}` |  |
| `espn_wbb_event_competitor_record` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/record` |  |
| `espn_wbb_event_competitors` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors` |  |
| `espn_wbb_event_leaders` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/leaders` |  |
| `espn_wbb_event_plays` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/plays` |  |
| `espn_wbb_event_scoringplays` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/scoringplays` |  |
| `espn_wbb_event_status` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/status` |  |
| `espn_wbb_events` | core_v2 | `/{sport}/leagues/{league}/events` |  |
| `espn_wbb_leaders_core` | core_v2 | `/{sport}/leagues/{league}/leaders` |  |
| `espn_wbb_league_notes` | core_v2 | `/{sport}/leagues/{league}/notes` |  |
| `espn_wbb_league_root` | core_v2 | `/{sport}/leagues/{league}` |  |
| `espn_wbb_season_athletes` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/athletes` |  |
| `espn_wbb_season_coaches` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/coaches` |  |
| `espn_wbb_season_draft` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/draft` |  |
| `espn_wbb_season_draft_round_picks` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/draft/rounds/{round_num}/picks` |  |
| `espn_wbb_season_freeagents` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/freeagents` |  |
| `espn_wbb_season_futures` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/futures` |  |
| `espn_wbb_season_pointer` | core_v2 | `/{sport}/leagues/{league}/season` |  |
| `espn_wbb_season_powerindex` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/powerindex[/{team_id}]` |  |
| `espn_wbb_season_powerindex_leaders` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/powerindex/leaders` |  |
| `espn_wbb_season_recruits` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/recruits` |  |
| `espn_wbb_season_team` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/teams/{team_id}` |  |
| `espn_wbb_season_teams` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/teams` |  |
| `espn_wbb_season_type_corrections` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/corrections` |  |
| `espn_wbb_season_type_leaders` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/leaders` |  |
| `espn_wbb_season_week_events` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}/events` |  |
| `espn_wbb_season_week_rankings` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}/rankings` |  |
| `espn_wbb_standings_core` | core_v2 | `/{sport}/leagues/{league}/standings` |  |
| `espn_wbb_statistics_league` | site_v2 | `/{sport}/{league}/statistics` |  |
| `espn_wbb_summary` | site_v2 | `/{sport}/{league}/summary` |  |
| `espn_wbb_talentpicks` | core_v2 | `/{sport}/leagues/{league}/talentpicks` |  |
| `espn_wbb_team_core` | core_v2 | `/{sport}/leagues/{league}/teams/{team_id}` |  |
| `espn_wbb_team_depthcharts` | site_v2 | `/{sport}/{league}/teams/{team_id}/depthcharts` |  |
| `espn_wbb_team_history` | site_v2 | `/{sport}/{league}/teams/{team_id}/history` |  |
| `espn_wbb_team_record` | site_v2 | `/{sport}/{league}/teams/{team_id}/record` |  |
| `espn_wbb_team_transactions` | site_v2 | `/{sport}/{league}/teams/{team_id}/transactions` |  |
| `espn_wbb_teams_core` | core_v2 | `/{sport}/leagues/{league}/teams` |  |
| `espn_wbb_teams_site` | site_v2 | `/{sport}/{league}/teams` |  |
| `espn_wbb_transactions` | site_v2 | `/{sport}/{league}/transactions` |  |
| `espn_wbb_venue` | core_v2 | `/{sport}/leagues/{league}/venues/{venue_id}` |  |

### R-only exports (candidate rename targets / not yet wrapped)

`espn_wbb_athlete_eventlog_v2`, `espn_wbb_event_competitor_records`, `espn_wbb_event_competitor_roster_entry`, `espn_wbb_event_competitor_score`, `espn_wbb_event_player_box`, `espn_wbb_game_all`, `espn_wbb_game_rosters`, `espn_wbb_pbp`, `espn_wbb_player_box`, `espn_wbb_player_stats`, `espn_wbb_powerindex`, `espn_wbb_season_leaders`, `espn_wbb_season_ranking`, `espn_wbb_season_rankings`, `espn_wbb_team_box`, `espn_wbb_team_record_detail`, `espn_wbb_team_season_profile`, `espn_wbb_team_season_roster`, `espn_wbb_team_season_statistics`, `espn_wbb_team_stats`, `espn_wbb_teams`, `espn_wbb_tournament`, `espn_wbb_tournament_season`, `espn_wbb_tournament_seasons`, `espn_wbb_week_ranking`, `espn_wbb_week_rankings`

## cfb (vs cfbfastR)

- generated: 120 | exact-name match (no rename): 24 | generated-only: 96 | R-only: 48

### generated-only — decide name

| generated | host | path | -> ? |
|-----------|------|------|------|
| `espn_cfb_athlete_awards` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/awards` |  |
| `espn_cfb_athlete_bio` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}/bio` |  |
| `espn_cfb_athlete_career_stats` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/statistics[/{stat_type}]` |  |
| `espn_cfb_athlete_contracts` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/contracts` |  |
| `espn_cfb_athlete_core` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}` |  |
| `espn_cfb_athlete_eventlog` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/eventlog` |  |
| `espn_cfb_athlete_gamelog` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/gamelog` |  |
| `espn_cfb_athlete_info` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}` |  |
| `espn_cfb_athlete_injuries` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/injuries` |  |
| `espn_cfb_athlete_news` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}/news` |  |
| `espn_cfb_athlete_notes` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/notes` |  |
| `espn_cfb_athlete_overview` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/overview` |  |
| `espn_cfb_athlete_records` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/records` |  |
| `espn_cfb_athlete_seasons` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/seasons` |  |
| `espn_cfb_athlete_splits` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/splits` |  |
| `espn_cfb_athlete_statisticslog` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/statisticslog` |  |
| `espn_cfb_athlete_stats` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/stats` |  |
| `espn_cfb_athlete_vs_athlete` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/vsathlete/{opp_id}` |  |
| `espn_cfb_athletes_index` | core_v2 | `/{sport}/leagues/{league}/athletes` |  |
| `espn_cfb_calendar_offseason` | site_v2 | `/{sport}/{league}/calendar/offseason` |  |
| `espn_cfb_calendar_ondays` | site_v2 | `/{sport}/{league}/calendar/ondays` |  |
| `espn_cfb_calendar_postseason` | site_v2 | `/{sport}/{league}/calendar/postseason` |  |
| `espn_cfb_calendar_regular_season` | site_v2 | `/{sport}/{league}/calendar/regular-season` |  |
| `espn_cfb_coach_season` | core_v2 | `/{sport}/leagues/{league}/coaches/{coach_id}/seasons/{season}` |  |
| `espn_cfb_conferences` | site_v2 | `/{sport}/{league}/groups` |  |
| `espn_cfb_draft` | site_v2 | `/{sport}/{league}/draft` |  |
| `espn_cfb_event` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}` |  |
| `espn_cfb_event_broadcasts` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/broadcasts` |  |
| `espn_cfb_event_competition` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}` |  |
| `espn_cfb_event_competitor` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}` |  |
| `espn_cfb_event_competitor_leaders` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/leaders` |  |
| `espn_cfb_event_competitor_linescores` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/linescores` |  |
| `espn_cfb_event_competitor_record` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/record` |  |
| `espn_cfb_event_competitor_roster` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/roster` |  |
| `espn_cfb_event_competitor_statistics` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/statistics` |  |
| `espn_cfb_event_competitors` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors` |  |
| `espn_cfb_event_leaders` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/leaders` |  |
| `espn_cfb_event_odds` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/odds` |  |
| `espn_cfb_event_official_detail` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/officials/{official_id}` |  |
| `espn_cfb_event_officials` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/officials` |  |
| `espn_cfb_event_play` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/plays/{play_id}` |  |
| `espn_cfb_event_play_personnel` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/plays/{play_id}/personnel` |  |
| `espn_cfb_event_plays` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/plays` |  |
| `espn_cfb_event_powerindex` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/powerindex` |  |
| `espn_cfb_event_predictor` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/predictor` |  |
| `espn_cfb_event_probabilities` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/probabilities` |  |
| `espn_cfb_event_propbets` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/propbets` |  |
| `espn_cfb_event_scoringplays` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/scoringplays` |  |
| `espn_cfb_event_situation` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/situation` |  |
| `espn_cfb_event_status` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/status` |  |
| `espn_cfb_events` | core_v2 | `/{sport}/leagues/{league}/events` |  |
| `espn_cfb_injuries` | site_v2 | `/{sport}/{league}/injuries` |  |
| `espn_cfb_leaders` | web_v3 | `/{sport}/{league}/statistics/byathlete` |  |
| `espn_cfb_leaders_core` | core_v2 | `/{sport}/leagues/{league}/leaders` |  |
| `espn_cfb_league_notes` | core_v2 | `/{sport}/leagues/{league}/notes` |  |
| `espn_cfb_league_root` | core_v2 | `/{sport}/leagues/{league}` |  |
| `espn_cfb_news` | site_v2 | `/{sport}/{league}/news` |  |
| `espn_cfb_season_athletes` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/athletes` |  |
| `espn_cfb_season_awards` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/awards` |  |
| `espn_cfb_season_coaches` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/coaches` |  |
| `espn_cfb_season_draft` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/draft` |  |
| `espn_cfb_season_draft_round_picks` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/draft/rounds/{round_num}/picks` |  |
| `espn_cfb_season_freeagents` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/freeagents` |  |
| `espn_cfb_season_futures` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/futures` |  |
| `espn_cfb_season_group` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups/{group_id}` |  |
| `espn_cfb_season_group_children` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups/{group_id}/children` |  |
| `espn_cfb_season_group_teams` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups/{group_id}/teams` |  |
| `espn_cfb_season_groups` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups` |  |
| `espn_cfb_season_pointer` | core_v2 | `/{sport}/leagues/{league}/season` |  |
| `espn_cfb_season_powerindex` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/powerindex[/{team_id}]` |  |
| `espn_cfb_season_powerindex_leaders` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/powerindex/leaders` |  |
| `espn_cfb_season_qbr` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}[/groups/{group_id}]/qbr/{split}` |  |
| `espn_cfb_season_qbr_week` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}/qbr/{split}` |  |
| `espn_cfb_season_recruits` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/recruits` |  |
| `espn_cfb_season_team` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/teams/{team_id}` |  |
| `espn_cfb_season_teams` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/teams` |  |
| `espn_cfb_season_type` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}` |  |
| `espn_cfb_season_type_corrections` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/corrections` |  |
| `espn_cfb_season_type_leaders` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/leaders` |  |
| `espn_cfb_season_week` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}` |  |
| `espn_cfb_season_week_events` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}/events` |  |
| `espn_cfb_season_week_rankings` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}/rankings` |  |
| `espn_cfb_standings_core` | core_v2 | `/{sport}/leagues/{league}/standings` |  |
| `espn_cfb_statistics_league` | site_v2 | `/{sport}/{league}/statistics` |  |
| `espn_cfb_summary` | site_v2 | `/{sport}/{league}/summary` |  |
| `espn_cfb_talentpicks` | core_v2 | `/{sport}/leagues/{league}/talentpicks` |  |
| `espn_cfb_team_core` | core_v2 | `/{sport}/leagues/{league}/teams/{team_id}` |  |
| `espn_cfb_team_depthcharts` | site_v2 | `/{sport}/{league}/teams/{team_id}/depthcharts` |  |
| `espn_cfb_team_history` | site_v2 | `/{sport}/{league}/teams/{team_id}/history` |  |
| `espn_cfb_team_injuries` | site_v2 | `/{sport}/{league}/teams/{team_id}/injuries` |  |
| `espn_cfb_team_news` | site_v2 | `/{sport}/{league}/teams/{team_id}/news` |  |
| `espn_cfb_team_transactions` | site_v2 | `/{sport}/{league}/teams/{team_id}/transactions` |  |
| `espn_cfb_teams_core` | core_v2 | `/{sport}/leagues/{league}/teams` |  |
| `espn_cfb_teams_site` | site_v2 | `/{sport}/{league}/teams` |  |
| `espn_cfb_tournaments` | core_v2 | `/{sport}/leagues/{league}/tournaments` |  |
| `espn_cfb_transactions` | site_v2 | `/{sport}/{league}/transactions` |  |

### R-only exports (candidate rename targets / not yet wrapped)

`espn_cfb_clear_cache`, `espn_cfb_futures`, `espn_cfb_game_broadcasts`, `espn_cfb_game_drive_plays`, `espn_cfb_game_drives`, `espn_cfb_game_leaders`, `espn_cfb_game_odds`, `espn_cfb_game_pbp`, `espn_cfb_game_play`, `espn_cfb_game_player_box`, `espn_cfb_game_player_statistics`, `espn_cfb_game_powerindex`, `espn_cfb_game_predictor`, `espn_cfb_game_probabilities`, `espn_cfb_game_situation`, `espn_cfb_game_status`, `espn_cfb_game_team_leaders`, `espn_cfb_game_team_linescores`, `espn_cfb_game_team_records`, `espn_cfb_game_team_roster`, `espn_cfb_game_team_statistics`, `espn_cfb_game_teams`, `espn_cfb_groups`, `espn_cfb_pbp`, `espn_cfb_pbp_v2`, `espn_cfb_player`, `espn_cfb_player_eventlog`, `espn_cfb_player_gamelog`, `espn_cfb_player_overview`, `espn_cfb_player_seasons`, `espn_cfb_player_splits`, `espn_cfb_player_statistics`, `espn_cfb_player_stats`, `espn_cfb_players`, `espn_cfb_powerindex`, `espn_cfb_qbr`, `espn_cfb_recruits`, `espn_cfb_schedule`, `espn_cfb_team_ats`, `espn_cfb_team_awards`, `espn_cfb_team_coaches`, `espn_cfb_team_events`, `espn_cfb_team_powerindex`, `espn_cfb_team_ranks`, `espn_cfb_team_stats`, `espn_cfb_teams`, `espn_cfb_unnest_plays`, `espn_cfb_week_rankings`
