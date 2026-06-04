<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [ESPN -> R naming worksheet (best-guess, for review)](#espn---r-naming-worksheet-best-guess-for-review)
  - [Totals](#totals)
  - [nba (vs hoopR)](#nba-vs-hoopr)
  - [mbb (vs hoopR)](#mbb-vs-hoopr)
  - [wnba (vs wehoop)](#wnba-vs-wehoop)
  - [wbb (vs wehoop)](#wbb-vs-wehoop)
  - [cfb (vs cfbfastR)](#cfb-vs-cfbfastr)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# ESPN -> R naming worksheet (best-guess, for review)

## Totals

| league | gen | exact | rename | review | keep |
|---|--:|--:|--:|--:|--:|
Behavior is fixed (Plan 2, URL-parity-proven); this is for **public names**. Suggestions come from URL matching against hoopR/wehoop/cfbfastR. Edit the `suggestion` column: `rename to <r>` / `keep` / `qualify`. `review:` rows are one raw endpoint that feeds several R functions — pick or split.

| cfb | 120 | 24 | 29 | 3 | 64 |
| wbb | 118 | 62 | 2 | 1 | 53 |
| wnba | 115 | 62 | 2 | 1 | 50 |
| mbb | 118 | 63 | 2 | 1 | 52 |
| nba | 115 | 66 | 2 | 1 | 46 |

## nba (vs hoopR)

- generated 115 | exact 66 | suggested-rename 2 | review(1->many) 1 | keep 46

| generated | host | path | suggestion |
|-----------|------|------|------------|
| `espn_nba_athlete_awards` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/awards` | keep (exact R match) |
| `espn_nba_athlete_bio` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}/bio` | keep (no R twin found) |
| `espn_nba_athlete_career_stats` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/statistics[/{stat_type}]` | keep (exact R match) |
| `espn_nba_athlete_contracts` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/contracts` | keep (exact R match) |
| `espn_nba_athlete_core` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}` | keep (no R twin found) |
| `espn_nba_athlete_eventlog` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/eventlog` | keep (exact R match) |
| `espn_nba_athlete_gamelog` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/gamelog` | keep (exact R match) |
| `espn_nba_athlete_info` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}` | keep (exact R match) |
| `espn_nba_athlete_injuries` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/injuries` | keep (no R twin found) |
| `espn_nba_athlete_news` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}/news` | keep (no R twin found) |
| `espn_nba_athlete_notes` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/notes` | keep (no R twin found) |
| `espn_nba_athlete_overview` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/overview` | keep (exact R match) |
| `espn_nba_athlete_records` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/records` | keep (no R twin found) |
| `espn_nba_athlete_seasons` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/seasons` | keep (exact R match) |
| `espn_nba_athlete_splits` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/splits` | keep (exact R match) |
| `espn_nba_athlete_statisticslog` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/statisticslog` | keep (exact R match) |
| `espn_nba_athlete_stats` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/stats` | keep (exact R match) |
| `espn_nba_athlete_vs_athlete` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/vsathlete/{opp_id}` | keep (no R twin found) |
| `espn_nba_athletes_index` | core_v2 | `/{sport}/leagues/{league}/athletes` | keep (exact R match) |
| `espn_nba_award` | core_v2 | `/{sport}/leagues/{league}/awards/{award_id}` | keep (exact R match) |
| `espn_nba_awards` | core_v2 | `/{sport}/leagues/{league}/awards` | keep (no R twin found) |
| `espn_nba_calendar` | site_v2 | `/{sport}/{league}/calendar` | keep (exact R match) |
| `espn_nba_calendar_offseason` | site_v2 | `/{sport}/{league}/calendar/offseason` | keep (no R twin found) |
| `espn_nba_calendar_ondays` | site_v2 | `/{sport}/{league}/calendar/ondays` | keep (no R twin found) |
| `espn_nba_calendar_postseason` | site_v2 | `/{sport}/{league}/calendar/postseason` | keep (no R twin found) |
| `espn_nba_calendar_regular_season` | site_v2 | `/{sport}/{league}/calendar/regular-season` | keep (no R twin found) |
| `espn_nba_coach` | core_v2 | `/{sport}/leagues/{league}/coaches/{coach_id}` | keep (exact R match) |
| `espn_nba_coach_record` | core_v2 | `/{sport}/leagues/{league}/coaches/{coach_id}/record/{record_type}` | keep (exact R match) |
| `espn_nba_coach_season` | core_v2 | `/{sport}/leagues/{league}/coaches/{coach_id}/seasons/{season}` | keep (exact R match) |
| `espn_nba_coaches` | core_v2 | `/{sport}/leagues/{league}/coaches` | keep (exact R match) |
| `espn_nba_conferences` | site_v2 | `/{sport}/{league}/groups` | keep (exact R match) |
| `espn_nba_draft` | site_v2 | `/{sport}/{league}/draft` | keep (exact R match) |
| `espn_nba_event` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}` | keep (no R twin found) |
| `espn_nba_event_broadcasts` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/broadcasts` | keep (exact R match) |
| `espn_nba_event_competition` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}` | keep (no R twin found) |
| `espn_nba_event_competitor` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}` | keep (no R twin found) |
| `espn_nba_event_competitor_leaders` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/leaders` | keep (exact R match) |
| `espn_nba_event_competitor_linescores` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/linescores` | keep (exact R match) |
| `espn_nba_event_competitor_record` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/record` | keep (no R twin found) |
| `espn_nba_event_competitor_roster` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/roster` | keep (exact R match) |
| `espn_nba_event_competitor_statistics` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/statistics` | keep (exact R match) |
| `espn_nba_event_competitors` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors` | keep (no R twin found) |
| `espn_nba_event_leaders` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/leaders` | keep (no R twin found) |
| `espn_nba_event_odds` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/odds` | keep (exact R match) |
| `espn_nba_event_official_detail` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/officials/{official_id}` | keep (exact R match) |
| `espn_nba_event_officials` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/officials` | keep (exact R match) |
| `espn_nba_event_play` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/plays/{play_id}` | keep (exact R match) |
| `espn_nba_event_play_personnel` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/plays/{play_id}/personnel` | keep (exact R match) |
| `espn_nba_event_plays` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/plays` | keep (no R twin found) |
| `espn_nba_event_powerindex` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/powerindex` | keep (exact R match) |
| `espn_nba_event_predictor` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/predictor` | keep (exact R match) |
| `espn_nba_event_probabilities` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/probabilities` | keep (exact R match) |
| `espn_nba_event_propbets` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/propbets` | keep (exact R match) |
| `espn_nba_event_scoringplays` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/scoringplays` | keep (no R twin found) |
| `espn_nba_event_situation` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/situation` | keep (exact R match) |
| `espn_nba_event_status` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/status` | keep (no R twin found) |
| `espn_nba_events` | core_v2 | `/{sport}/leagues/{league}/events` | rename to `espn_nba_game_rosters` (check) |
| `espn_nba_franchise` | core_v2 | `/{sport}/leagues/{league}/franchises/{franchise_id}` | keep (exact R match) |
| `espn_nba_franchises` | core_v2 | `/{sport}/leagues/{league}/franchises` | keep (exact R match) |
| `espn_nba_injuries` | site_v2 | `/{sport}/{league}/injuries` | keep (exact R match) |
| `espn_nba_leaders` | web_v3 | `/{sport}/{league}/statistics/byathlete` | keep (exact R match) |
| `espn_nba_leaders_core` | core_v2 | `/{sport}/leagues/{league}/leaders` | keep (no R twin found) |
| `espn_nba_league_notes` | core_v2 | `/{sport}/leagues/{league}/notes` | keep (no R twin found) |
| `espn_nba_league_root` | core_v2 | `/{sport}/leagues/{league}` | keep (no R twin found) |
| `espn_nba_news` | site_v2 | `/{sport}/{league}/news` | keep (exact R match) |
| `espn_nba_position` | core_v2 | `/{sport}/leagues/{league}/positions/{position_id}` | keep (exact R match) |
| `espn_nba_positions` | core_v2 | `/{sport}/leagues/{league}/positions` | keep (exact R match) |
| `espn_nba_scoreboard` | site_v2 | `/{sport}/{league}/scoreboard` | keep (exact R match) |
| `espn_nba_season_athletes` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/athletes` | keep (no R twin found) |
| `espn_nba_season_awards` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/awards` | keep (exact R match) |
| `espn_nba_season_coaches` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/coaches` | keep (no R twin found) |
| `espn_nba_season_draft` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/draft` | keep (exact R match) |
| `espn_nba_season_draft_round_picks` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/draft/rounds/{round_num}/picks` | keep (no R twin found) |
| `espn_nba_season_freeagents` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/freeagents` | keep (no R twin found) |
| `espn_nba_season_futures` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/futures` | keep (no R twin found) |
| `espn_nba_season_group` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups/{group_id}` | keep (exact R match) |
| `espn_nba_season_group_children` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups/{group_id}/children` | keep (exact R match) |
| `espn_nba_season_group_teams` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups/{group_id}/teams` | keep (exact R match) |
| `espn_nba_season_groups` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups` | keep (exact R match) |
| `espn_nba_season_info` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}` | keep (exact R match) |
| `espn_nba_season_pointer` | core_v2 | `/{sport}/leagues/{league}/season` | keep (no R twin found) |
| `espn_nba_season_powerindex` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/powerindex[/{team_id}]` | keep (no R twin found) |
| `espn_nba_season_powerindex_leaders` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/powerindex/leaders` | keep (no R twin found) |
| `espn_nba_season_team` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/teams/{team_id}` | keep (no R twin found) |
| `espn_nba_season_teams` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/teams` | keep (no R twin found) |
| `espn_nba_season_type` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}` | keep (exact R match) |
| `espn_nba_season_type_corrections` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/corrections` | keep (no R twin found) |
| `espn_nba_season_type_leaders` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/leaders` | keep (no R twin found) |
| `espn_nba_season_types` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types` | keep (exact R match) |
| `espn_nba_season_week` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}` | keep (exact R match) |
| `espn_nba_season_week_events` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}/events` | keep (no R twin found) |
| `espn_nba_season_weeks` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks` | keep (exact R match) |
| `espn_nba_seasons` | core_v2 | `/{sport}/leagues/{league}/seasons` | keep (exact R match) |
| `espn_nba_standings` | site_v2_alt | `/{sport}/{league}/standings` | keep (exact R match) |
| `espn_nba_standings_core` | core_v2 | `/{sport}/leagues/{league}/standings` | keep (no R twin found) |
| `espn_nba_statistics_league` | site_v2 | `/{sport}/{league}/statistics` | keep (no R twin found) |
| `espn_nba_summary` | site_v2 | `/{sport}/{league}/summary` | review: `espn_nba_betting` | `espn_nba_game_all` | `espn_nba_pbp` | `espn_nba_player_box` | `espn_nba_team_box` | `espn_nba_wp` |
| `espn_nba_talentpicks` | core_v2 | `/{sport}/leagues/{league}/talentpicks` | keep (no R twin found) |
| `espn_nba_team` | site_v2 | `/{sport}/{league}/teams/{team_id}` | keep (exact R match) |
| `espn_nba_team_core` | core_v2 | `/{sport}/leagues/{league}/teams/{team_id}` | keep (no R twin found) |
| `espn_nba_team_depthcharts` | site_v2 | `/{sport}/{league}/teams/{team_id}/depthcharts` | keep (no R twin found) |
| `espn_nba_team_history` | site_v2 | `/{sport}/{league}/teams/{team_id}/history` | keep (no R twin found) |
| `espn_nba_team_injuries` | site_v2 | `/{sport}/{league}/teams/{team_id}/injuries` | keep (exact R match) |
| `espn_nba_team_leaders` | site_v2 | `/{sport}/{league}/teams/{team_id}/leaders` | keep (exact R match) |
| `espn_nba_team_news` | site_v2 | `/{sport}/{league}/teams/{team_id}/news` | keep (exact R match) |
| `espn_nba_team_record` | site_v2 | `/{sport}/{league}/teams/{team_id}/record` | keep (exact R match) |
| `espn_nba_team_roster` | site_v2 | `/{sport}/{league}/teams/{team_id}/roster` | keep (exact R match) |
| `espn_nba_team_schedule` | site_v2 | `/{sport}/{league}/teams/{team_id}/schedule` | keep (exact R match) |
| `espn_nba_team_transactions` | site_v2 | `/{sport}/{league}/teams/{team_id}/transactions` | keep (no R twin found) |
| `espn_nba_teams_core` | core_v2 | `/{sport}/leagues/{league}/teams` | keep (no R twin found) |
| `espn_nba_teams_site` | site_v2 | `/{sport}/{league}/teams` | rename to `espn_nba_teams` (high) |
| `espn_nba_tournaments` | core_v2 | `/{sport}/leagues/{league}/tournaments` | keep (exact R match) |
| `espn_nba_transactions` | site_v2 | `/{sport}/{league}/transactions` | keep (exact R match) |
| `espn_nba_venue` | core_v2 | `/{sport}/leagues/{league}/venues/{venue_id}` | keep (no R twin found) |
| `espn_nba_venues` | core_v2 | `/{sport}/leagues/{league}/venues` | keep (exact R match) |

## mbb (vs hoopR)

- generated 118 | exact 63 | suggested-rename 2 | review(1->many) 1 | keep 52

| generated | host | path | suggestion |
|-----------|------|------|------------|
| `espn_mbb_athlete_awards` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/awards` | keep (exact R match) |
| `espn_mbb_athlete_bio` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}/bio` | keep (no R twin found) |
| `espn_mbb_athlete_career_stats` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/statistics[/{stat_type}]` | keep (exact R match) |
| `espn_mbb_athlete_contracts` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/contracts` | keep (no R twin found) |
| `espn_mbb_athlete_core` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}` | keep (no R twin found) |
| `espn_mbb_athlete_eventlog` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/eventlog` | keep (exact R match) |
| `espn_mbb_athlete_gamelog` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/gamelog` | keep (exact R match) |
| `espn_mbb_athlete_info` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}` | keep (exact R match) |
| `espn_mbb_athlete_injuries` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/injuries` | keep (no R twin found) |
| `espn_mbb_athlete_news` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}/news` | keep (no R twin found) |
| `espn_mbb_athlete_notes` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/notes` | keep (no R twin found) |
| `espn_mbb_athlete_overview` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/overview` | keep (exact R match) |
| `espn_mbb_athlete_records` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/records` | keep (no R twin found) |
| `espn_mbb_athlete_seasons` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/seasons` | keep (exact R match) |
| `espn_mbb_athlete_splits` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/splits` | keep (exact R match) |
| `espn_mbb_athlete_statisticslog` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/statisticslog` | keep (exact R match) |
| `espn_mbb_athlete_stats` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/stats` | keep (exact R match) |
| `espn_mbb_athlete_vs_athlete` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/vsathlete/{opp_id}` | keep (no R twin found) |
| `espn_mbb_athletes_index` | core_v2 | `/{sport}/leagues/{league}/athletes` | keep (exact R match) |
| `espn_mbb_award` | core_v2 | `/{sport}/leagues/{league}/awards/{award_id}` | keep (exact R match) |
| `espn_mbb_awards` | core_v2 | `/{sport}/leagues/{league}/awards` | keep (no R twin found) |
| `espn_mbb_calendar` | site_v2 | `/{sport}/{league}/calendar` | keep (exact R match) |
| `espn_mbb_calendar_offseason` | site_v2 | `/{sport}/{league}/calendar/offseason` | keep (no R twin found) |
| `espn_mbb_calendar_ondays` | site_v2 | `/{sport}/{league}/calendar/ondays` | keep (no R twin found) |
| `espn_mbb_calendar_postseason` | site_v2 | `/{sport}/{league}/calendar/postseason` | keep (no R twin found) |
| `espn_mbb_calendar_regular_season` | site_v2 | `/{sport}/{league}/calendar/regular-season` | keep (no R twin found) |
| `espn_mbb_coach` | core_v2 | `/{sport}/leagues/{league}/coaches/{coach_id}` | keep (exact R match) |
| `espn_mbb_coach_record` | core_v2 | `/{sport}/leagues/{league}/coaches/{coach_id}/record/{record_type}` | keep (exact R match) |
| `espn_mbb_coach_season` | core_v2 | `/{sport}/leagues/{league}/coaches/{coach_id}/seasons/{season}` | keep (exact R match) |
| `espn_mbb_coaches` | core_v2 | `/{sport}/leagues/{league}/coaches` | keep (exact R match) |
| `espn_mbb_conferences` | site_v2 | `/{sport}/{league}/groups` | keep (exact R match) |
| `espn_mbb_draft` | site_v2 | `/{sport}/{league}/draft` | keep (no R twin found) |
| `espn_mbb_event` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}` | keep (no R twin found) |
| `espn_mbb_event_broadcasts` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/broadcasts` | keep (exact R match) |
| `espn_mbb_event_competition` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}` | keep (no R twin found) |
| `espn_mbb_event_competitor` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}` | keep (no R twin found) |
| `espn_mbb_event_competitor_leaders` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/leaders` | keep (exact R match) |
| `espn_mbb_event_competitor_linescores` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/linescores` | keep (exact R match) |
| `espn_mbb_event_competitor_record` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/record` | keep (no R twin found) |
| `espn_mbb_event_competitor_roster` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/roster` | keep (exact R match) |
| `espn_mbb_event_competitor_statistics` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/statistics` | keep (exact R match) |
| `espn_mbb_event_competitors` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors` | keep (no R twin found) |
| `espn_mbb_event_leaders` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/leaders` | keep (no R twin found) |
| `espn_mbb_event_odds` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/odds` | keep (exact R match) |
| `espn_mbb_event_official_detail` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/officials/{official_id}` | keep (exact R match) |
| `espn_mbb_event_officials` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/officials` | keep (exact R match) |
| `espn_mbb_event_play` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/plays/{play_id}` | keep (exact R match) |
| `espn_mbb_event_play_personnel` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/plays/{play_id}/personnel` | keep (exact R match) |
| `espn_mbb_event_plays` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/plays` | keep (no R twin found) |
| `espn_mbb_event_powerindex` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/powerindex` | keep (exact R match) |
| `espn_mbb_event_predictor` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/predictor` | keep (exact R match) |
| `espn_mbb_event_probabilities` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/probabilities` | keep (exact R match) |
| `espn_mbb_event_propbets` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/propbets` | keep (exact R match) |
| `espn_mbb_event_scoringplays` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/scoringplays` | keep (no R twin found) |
| `espn_mbb_event_situation` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/situation` | keep (exact R match) |
| `espn_mbb_event_status` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/status` | keep (no R twin found) |
| `espn_mbb_events` | core_v2 | `/{sport}/leagues/{league}/events` | rename to `espn_mbb_game_rosters` (check) |
| `espn_mbb_franchise` | core_v2 | `/{sport}/leagues/{league}/franchises/{franchise_id}` | keep (exact R match) |
| `espn_mbb_franchises` | core_v2 | `/{sport}/leagues/{league}/franchises` | keep (exact R match) |
| `espn_mbb_injuries` | site_v2 | `/{sport}/{league}/injuries` | keep (exact R match) |
| `espn_mbb_leaders` | web_v3 | `/{sport}/{league}/statistics/byathlete` | keep (exact R match) |
| `espn_mbb_leaders_core` | core_v2 | `/{sport}/leagues/{league}/leaders` | keep (no R twin found) |
| `espn_mbb_league_notes` | core_v2 | `/{sport}/leagues/{league}/notes` | keep (no R twin found) |
| `espn_mbb_league_root` | core_v2 | `/{sport}/leagues/{league}` | keep (no R twin found) |
| `espn_mbb_news` | site_v2 | `/{sport}/{league}/news` | keep (exact R match) |
| `espn_mbb_position` | core_v2 | `/{sport}/leagues/{league}/positions/{position_id}` | keep (exact R match) |
| `espn_mbb_positions` | core_v2 | `/{sport}/leagues/{league}/positions` | keep (exact R match) |
| `espn_mbb_rankings` | site_v2 | `/{sport}/{league}/rankings` | keep (exact R match) |
| `espn_mbb_scoreboard` | site_v2 | `/{sport}/{league}/scoreboard` | keep (exact R match) |
| `espn_mbb_season_athletes` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/athletes` | keep (no R twin found) |
| `espn_mbb_season_awards` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/awards` | keep (exact R match) |
| `espn_mbb_season_coaches` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/coaches` | keep (no R twin found) |
| `espn_mbb_season_draft` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/draft` | keep (no R twin found) |
| `espn_mbb_season_draft_round_picks` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/draft/rounds/{round_num}/picks` | keep (no R twin found) |
| `espn_mbb_season_freeagents` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/freeagents` | keep (no R twin found) |
| `espn_mbb_season_futures` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/futures` | keep (no R twin found) |
| `espn_mbb_season_group` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups/{group_id}` | keep (exact R match) |
| `espn_mbb_season_group_children` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups/{group_id}/children` | keep (exact R match) |
| `espn_mbb_season_group_teams` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups/{group_id}/teams` | keep (exact R match) |
| `espn_mbb_season_groups` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups` | keep (exact R match) |
| `espn_mbb_season_info` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}` | keep (exact R match) |
| `espn_mbb_season_pointer` | core_v2 | `/{sport}/leagues/{league}/season` | keep (no R twin found) |
| `espn_mbb_season_powerindex` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/powerindex[/{team_id}]` | keep (no R twin found) |
| `espn_mbb_season_powerindex_leaders` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/powerindex/leaders` | keep (no R twin found) |
| `espn_mbb_season_recruits` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/recruits` | keep (no R twin found) |
| `espn_mbb_season_team` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/teams/{team_id}` | keep (no R twin found) |
| `espn_mbb_season_teams` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/teams` | keep (no R twin found) |
| `espn_mbb_season_type` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}` | keep (exact R match) |
| `espn_mbb_season_type_corrections` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/corrections` | keep (no R twin found) |
| `espn_mbb_season_type_leaders` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/leaders` | keep (no R twin found) |
| `espn_mbb_season_types` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types` | keep (exact R match) |
| `espn_mbb_season_week` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}` | keep (exact R match) |
| `espn_mbb_season_week_events` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}/events` | keep (no R twin found) |
| `espn_mbb_season_week_rankings` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}/rankings` | keep (no R twin found) |
| `espn_mbb_season_weeks` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks` | keep (exact R match) |
| `espn_mbb_seasons` | core_v2 | `/{sport}/leagues/{league}/seasons` | keep (exact R match) |
| `espn_mbb_standings` | site_v2_alt | `/{sport}/{league}/standings` | keep (exact R match) |
| `espn_mbb_standings_core` | core_v2 | `/{sport}/leagues/{league}/standings` | keep (no R twin found) |
| `espn_mbb_statistics_league` | site_v2 | `/{sport}/{league}/statistics` | keep (no R twin found) |
| `espn_mbb_summary` | site_v2 | `/{sport}/{league}/summary` | review: `espn_mbb_betting` | `espn_mbb_game_all` | `espn_mbb_pbp` | `espn_mbb_player_box` | `espn_mbb_team_box` | `espn_mbb_wp` |
| `espn_mbb_talentpicks` | core_v2 | `/{sport}/leagues/{league}/talentpicks` | keep (no R twin found) |
| `espn_mbb_team` | site_v2 | `/{sport}/{league}/teams/{team_id}` | keep (exact R match) |
| `espn_mbb_team_core` | core_v2 | `/{sport}/leagues/{league}/teams/{team_id}` | keep (no R twin found) |
| `espn_mbb_team_depthcharts` | site_v2 | `/{sport}/{league}/teams/{team_id}/depthcharts` | keep (no R twin found) |
| `espn_mbb_team_history` | site_v2 | `/{sport}/{league}/teams/{team_id}/history` | keep (no R twin found) |
| `espn_mbb_team_injuries` | site_v2 | `/{sport}/{league}/teams/{team_id}/injuries` | keep (exact R match) |
| `espn_mbb_team_leaders` | site_v2 | `/{sport}/{league}/teams/{team_id}/leaders` | keep (exact R match) |
| `espn_mbb_team_news` | site_v2 | `/{sport}/{league}/teams/{team_id}/news` | keep (exact R match) |
| `espn_mbb_team_record` | site_v2 | `/{sport}/{league}/teams/{team_id}/record` | keep (exact R match) |
| `espn_mbb_team_roster` | site_v2 | `/{sport}/{league}/teams/{team_id}/roster` | keep (exact R match) |
| `espn_mbb_team_schedule` | site_v2 | `/{sport}/{league}/teams/{team_id}/schedule` | keep (exact R match) |
| `espn_mbb_team_transactions` | site_v2 | `/{sport}/{league}/teams/{team_id}/transactions` | keep (no R twin found) |
| `espn_mbb_teams_core` | core_v2 | `/{sport}/leagues/{league}/teams` | keep (no R twin found) |
| `espn_mbb_teams_site` | site_v2 | `/{sport}/{league}/teams` | rename to `espn_mbb_teams` (high) |
| `espn_mbb_tournaments` | core_v2 | `/{sport}/leagues/{league}/tournaments` | keep (exact R match) |
| `espn_mbb_transactions` | site_v2 | `/{sport}/{league}/transactions` | keep (no R twin found) |
| `espn_mbb_venue` | core_v2 | `/{sport}/leagues/{league}/venues/{venue_id}` | keep (no R twin found) |
| `espn_mbb_venues` | core_v2 | `/{sport}/leagues/{league}/venues` | keep (exact R match) |

## wnba (vs wehoop)

- generated 115 | exact 62 | suggested-rename 2 | review(1->many) 1 | keep 50

| generated | host | path | suggestion |
|-----------|------|------|------------|
| `espn_wnba_athlete_awards` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/awards` | keep (exact R match) |
| `espn_wnba_athlete_bio` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}/bio` | keep (no R twin found) |
| `espn_wnba_athlete_career_stats` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/statistics[/{stat_type}]` | keep (exact R match) |
| `espn_wnba_athlete_contracts` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/contracts` | keep (no R twin found) |
| `espn_wnba_athlete_core` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}` | keep (no R twin found) |
| `espn_wnba_athlete_eventlog` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/eventlog` | keep (exact R match) |
| `espn_wnba_athlete_gamelog` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/gamelog` | keep (exact R match) |
| `espn_wnba_athlete_info` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}` | keep (exact R match) |
| `espn_wnba_athlete_injuries` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/injuries` | keep (no R twin found) |
| `espn_wnba_athlete_news` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}/news` | keep (no R twin found) |
| `espn_wnba_athlete_notes` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/notes` | keep (no R twin found) |
| `espn_wnba_athlete_overview` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/overview` | keep (exact R match) |
| `espn_wnba_athlete_records` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/records` | keep (no R twin found) |
| `espn_wnba_athlete_seasons` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/seasons` | keep (exact R match) |
| `espn_wnba_athlete_splits` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/splits` | keep (exact R match) |
| `espn_wnba_athlete_statisticslog` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/statisticslog` | keep (exact R match) |
| `espn_wnba_athlete_stats` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/stats` | keep (exact R match) |
| `espn_wnba_athlete_vs_athlete` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/vsathlete/{opp_id}` | keep (no R twin found) |
| `espn_wnba_athletes_index` | core_v2 | `/{sport}/leagues/{league}/athletes` | keep (exact R match) |
| `espn_wnba_award` | core_v2 | `/{sport}/leagues/{league}/awards/{award_id}` | keep (exact R match) |
| `espn_wnba_awards` | core_v2 | `/{sport}/leagues/{league}/awards` | keep (no R twin found) |
| `espn_wnba_calendar` | site_v2 | `/{sport}/{league}/calendar` | keep (exact R match) |
| `espn_wnba_calendar_offseason` | site_v2 | `/{sport}/{league}/calendar/offseason` | keep (no R twin found) |
| `espn_wnba_calendar_ondays` | site_v2 | `/{sport}/{league}/calendar/ondays` | keep (no R twin found) |
| `espn_wnba_calendar_postseason` | site_v2 | `/{sport}/{league}/calendar/postseason` | keep (no R twin found) |
| `espn_wnba_calendar_regular_season` | site_v2 | `/{sport}/{league}/calendar/regular-season` | keep (no R twin found) |
| `espn_wnba_coach` | core_v2 | `/{sport}/leagues/{league}/coaches/{coach_id}` | keep (no R twin found) |
| `espn_wnba_coach_record` | core_v2 | `/{sport}/leagues/{league}/coaches/{coach_id}/record/{record_type}` | keep (no R twin found) |
| `espn_wnba_coach_season` | core_v2 | `/{sport}/leagues/{league}/coaches/{coach_id}/seasons/{season}` | keep (exact R match) |
| `espn_wnba_coaches` | core_v2 | `/{sport}/leagues/{league}/coaches` | keep (exact R match) |
| `espn_wnba_conferences` | site_v2 | `/{sport}/{league}/groups` | keep (exact R match) |
| `espn_wnba_draft` | site_v2 | `/{sport}/{league}/draft` | keep (exact R match) |
| `espn_wnba_event` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}` | keep (no R twin found) |
| `espn_wnba_event_broadcasts` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/broadcasts` | keep (exact R match) |
| `espn_wnba_event_competition` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}` | keep (no R twin found) |
| `espn_wnba_event_competitor` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}` | keep (no R twin found) |
| `espn_wnba_event_competitor_leaders` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/leaders` | keep (exact R match) |
| `espn_wnba_event_competitor_linescores` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/linescores` | keep (exact R match) |
| `espn_wnba_event_competitor_record` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/record` | keep (no R twin found) |
| `espn_wnba_event_competitor_roster` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/roster` | keep (exact R match) |
| `espn_wnba_event_competitor_statistics` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/statistics` | keep (exact R match) |
| `espn_wnba_event_competitors` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors` | keep (no R twin found) |
| `espn_wnba_event_leaders` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/leaders` | keep (no R twin found) |
| `espn_wnba_event_odds` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/odds` | keep (exact R match) |
| `espn_wnba_event_official_detail` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/officials/{official_id}` | keep (exact R match) |
| `espn_wnba_event_officials` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/officials` | keep (exact R match) |
| `espn_wnba_event_play` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/plays/{play_id}` | keep (exact R match) |
| `espn_wnba_event_play_personnel` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/plays/{play_id}/personnel` | keep (exact R match) |
| `espn_wnba_event_plays` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/plays` | keep (no R twin found) |
| `espn_wnba_event_powerindex` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/powerindex` | keep (exact R match) |
| `espn_wnba_event_predictor` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/predictor` | keep (exact R match) |
| `espn_wnba_event_probabilities` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/probabilities` | keep (exact R match) |
| `espn_wnba_event_propbets` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/propbets` | keep (exact R match) |
| `espn_wnba_event_scoringplays` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/scoringplays` | keep (no R twin found) |
| `espn_wnba_event_situation` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/situation` | keep (exact R match) |
| `espn_wnba_event_status` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/status` | keep (no R twin found) |
| `espn_wnba_events` | core_v2 | `/{sport}/leagues/{league}/events` | rename to `espn_wnba_game_rosters` (check) |
| `espn_wnba_franchise` | core_v2 | `/{sport}/leagues/{league}/franchises/{franchise_id}` | keep (exact R match) |
| `espn_wnba_franchises` | core_v2 | `/{sport}/leagues/{league}/franchises` | keep (exact R match) |
| `espn_wnba_injuries` | site_v2 | `/{sport}/{league}/injuries` | keep (exact R match) |
| `espn_wnba_leaders` | web_v3 | `/{sport}/{league}/statistics/byathlete` | keep (exact R match) |
| `espn_wnba_leaders_core` | core_v2 | `/{sport}/leagues/{league}/leaders` | keep (no R twin found) |
| `espn_wnba_league_notes` | core_v2 | `/{sport}/leagues/{league}/notes` | keep (no R twin found) |
| `espn_wnba_league_root` | core_v2 | `/{sport}/leagues/{league}` | keep (no R twin found) |
| `espn_wnba_news` | site_v2 | `/{sport}/{league}/news` | keep (exact R match) |
| `espn_wnba_position` | core_v2 | `/{sport}/leagues/{league}/positions/{position_id}` | keep (exact R match) |
| `espn_wnba_positions` | core_v2 | `/{sport}/leagues/{league}/positions` | keep (exact R match) |
| `espn_wnba_scoreboard` | site_v2 | `/{sport}/{league}/scoreboard` | keep (exact R match) |
| `espn_wnba_season_athletes` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/athletes` | keep (no R twin found) |
| `espn_wnba_season_awards` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/awards` | keep (exact R match) |
| `espn_wnba_season_coaches` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/coaches` | keep (no R twin found) |
| `espn_wnba_season_draft` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/draft` | keep (exact R match) |
| `espn_wnba_season_draft_round_picks` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/draft/rounds/{round_num}/picks` | keep (no R twin found) |
| `espn_wnba_season_freeagents` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/freeagents` | keep (no R twin found) |
| `espn_wnba_season_futures` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/futures` | keep (no R twin found) |
| `espn_wnba_season_group` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups/{group_id}` | keep (exact R match) |
| `espn_wnba_season_group_children` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups/{group_id}/children` | keep (exact R match) |
| `espn_wnba_season_group_teams` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups/{group_id}/teams` | keep (exact R match) |
| `espn_wnba_season_groups` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups` | keep (exact R match) |
| `espn_wnba_season_info` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}` | keep (exact R match) |
| `espn_wnba_season_pointer` | core_v2 | `/{sport}/leagues/{league}/season` | keep (no R twin found) |
| `espn_wnba_season_powerindex` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/powerindex[/{team_id}]` | keep (no R twin found) |
| `espn_wnba_season_powerindex_leaders` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/powerindex/leaders` | keep (no R twin found) |
| `espn_wnba_season_team` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/teams/{team_id}` | keep (no R twin found) |
| `espn_wnba_season_teams` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/teams` | keep (no R twin found) |
| `espn_wnba_season_type` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}` | keep (exact R match) |
| `espn_wnba_season_type_corrections` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/corrections` | keep (no R twin found) |
| `espn_wnba_season_type_leaders` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/leaders` | keep (no R twin found) |
| `espn_wnba_season_types` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types` | keep (exact R match) |
| `espn_wnba_season_week` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}` | keep (exact R match) |
| `espn_wnba_season_week_events` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}/events` | keep (no R twin found) |
| `espn_wnba_season_weeks` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks` | keep (exact R match) |
| `espn_wnba_seasons` | core_v2 | `/{sport}/leagues/{league}/seasons` | keep (exact R match) |
| `espn_wnba_standings` | site_v2_alt | `/{sport}/{league}/standings` | keep (exact R match) |
| `espn_wnba_standings_core` | core_v2 | `/{sport}/leagues/{league}/standings` | keep (no R twin found) |
| `espn_wnba_statistics_league` | site_v2 | `/{sport}/{league}/statistics` | keep (no R twin found) |
| `espn_wnba_summary` | site_v2 | `/{sport}/{league}/summary` | review: `espn_wnba_game_all` | `espn_wnba_pbp` | `espn_wnba_player_box` | `espn_wnba_team_box` |
| `espn_wnba_talentpicks` | core_v2 | `/{sport}/leagues/{league}/talentpicks` | keep (no R twin found) |
| `espn_wnba_team` | site_v2 | `/{sport}/{league}/teams/{team_id}` | keep (exact R match) |
| `espn_wnba_team_core` | core_v2 | `/{sport}/leagues/{league}/teams/{team_id}` | keep (no R twin found) |
| `espn_wnba_team_depthcharts` | site_v2 | `/{sport}/{league}/teams/{team_id}/depthcharts` | keep (no R twin found) |
| `espn_wnba_team_history` | site_v2 | `/{sport}/{league}/teams/{team_id}/history` | keep (no R twin found) |
| `espn_wnba_team_injuries` | site_v2 | `/{sport}/{league}/teams/{team_id}/injuries` | keep (exact R match) |
| `espn_wnba_team_leaders` | site_v2 | `/{sport}/{league}/teams/{team_id}/leaders` | keep (exact R match) |
| `espn_wnba_team_news` | site_v2 | `/{sport}/{league}/teams/{team_id}/news` | keep (exact R match) |
| `espn_wnba_team_record` | site_v2 | `/{sport}/{league}/teams/{team_id}/record` | keep (exact R match) |
| `espn_wnba_team_roster` | site_v2 | `/{sport}/{league}/teams/{team_id}/roster` | keep (exact R match) |
| `espn_wnba_team_schedule` | site_v2 | `/{sport}/{league}/teams/{team_id}/schedule` | keep (exact R match) |
| `espn_wnba_team_transactions` | site_v2 | `/{sport}/{league}/teams/{team_id}/transactions` | keep (no R twin found) |
| `espn_wnba_teams_core` | core_v2 | `/{sport}/leagues/{league}/teams` | keep (no R twin found) |
| `espn_wnba_teams_site` | site_v2 | `/{sport}/{league}/teams` | rename to `espn_wnba_teams` (high) |
| `espn_wnba_tournaments` | core_v2 | `/{sport}/leagues/{league}/tournaments` | keep (no R twin found) |
| `espn_wnba_transactions` | site_v2 | `/{sport}/{league}/transactions` | keep (exact R match) |
| `espn_wnba_venue` | core_v2 | `/{sport}/leagues/{league}/venues/{venue_id}` | keep (no R twin found) |
| `espn_wnba_venues` | core_v2 | `/{sport}/leagues/{league}/venues` | keep (exact R match) |

## wbb (vs wehoop)

- generated 118 | exact 62 | suggested-rename 2 | review(1->many) 1 | keep 53

| generated | host | path | suggestion |
|-----------|------|------|------------|
| `espn_wbb_athlete_awards` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/awards` | keep (exact R match) |
| `espn_wbb_athlete_bio` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}/bio` | keep (no R twin found) |
| `espn_wbb_athlete_career_stats` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/statistics[/{stat_type}]` | keep (exact R match) |
| `espn_wbb_athlete_contracts` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/contracts` | keep (no R twin found) |
| `espn_wbb_athlete_core` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}` | keep (no R twin found) |
| `espn_wbb_athlete_eventlog` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/eventlog` | keep (exact R match) |
| `espn_wbb_athlete_gamelog` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/gamelog` | keep (exact R match) |
| `espn_wbb_athlete_info` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}` | keep (exact R match) |
| `espn_wbb_athlete_injuries` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/injuries` | keep (no R twin found) |
| `espn_wbb_athlete_news` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}/news` | keep (no R twin found) |
| `espn_wbb_athlete_notes` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/notes` | keep (no R twin found) |
| `espn_wbb_athlete_overview` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/overview` | keep (exact R match) |
| `espn_wbb_athlete_records` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/records` | keep (no R twin found) |
| `espn_wbb_athlete_seasons` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/seasons` | keep (exact R match) |
| `espn_wbb_athlete_splits` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/splits` | keep (exact R match) |
| `espn_wbb_athlete_statisticslog` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/statisticslog` | keep (exact R match) |
| `espn_wbb_athlete_stats` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/stats` | keep (exact R match) |
| `espn_wbb_athlete_vs_athlete` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/vsathlete/{opp_id}` | keep (no R twin found) |
| `espn_wbb_athletes_index` | core_v2 | `/{sport}/leagues/{league}/athletes` | keep (exact R match) |
| `espn_wbb_award` | core_v2 | `/{sport}/leagues/{league}/awards/{award_id}` | keep (exact R match) |
| `espn_wbb_awards` | core_v2 | `/{sport}/leagues/{league}/awards` | keep (no R twin found) |
| `espn_wbb_calendar` | site_v2 | `/{sport}/{league}/calendar` | keep (exact R match) |
| `espn_wbb_calendar_offseason` | site_v2 | `/{sport}/{league}/calendar/offseason` | keep (no R twin found) |
| `espn_wbb_calendar_ondays` | site_v2 | `/{sport}/{league}/calendar/ondays` | keep (no R twin found) |
| `espn_wbb_calendar_postseason` | site_v2 | `/{sport}/{league}/calendar/postseason` | keep (no R twin found) |
| `espn_wbb_calendar_regular_season` | site_v2 | `/{sport}/{league}/calendar/regular-season` | keep (no R twin found) |
| `espn_wbb_coach` | core_v2 | `/{sport}/leagues/{league}/coaches/{coach_id}` | keep (exact R match) |
| `espn_wbb_coach_record` | core_v2 | `/{sport}/leagues/{league}/coaches/{coach_id}/record/{record_type}` | keep (exact R match) |
| `espn_wbb_coach_season` | core_v2 | `/{sport}/leagues/{league}/coaches/{coach_id}/seasons/{season}` | keep (exact R match) |
| `espn_wbb_coaches` | core_v2 | `/{sport}/leagues/{league}/coaches` | keep (exact R match) |
| `espn_wbb_conferences` | site_v2 | `/{sport}/{league}/groups` | keep (exact R match) |
| `espn_wbb_draft` | site_v2 | `/{sport}/{league}/draft` | keep (no R twin found) |
| `espn_wbb_event` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}` | keep (no R twin found) |
| `espn_wbb_event_broadcasts` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/broadcasts` | keep (exact R match) |
| `espn_wbb_event_competition` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}` | keep (no R twin found) |
| `espn_wbb_event_competitor` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}` | keep (no R twin found) |
| `espn_wbb_event_competitor_leaders` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/leaders` | keep (exact R match) |
| `espn_wbb_event_competitor_linescores` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/linescores` | keep (exact R match) |
| `espn_wbb_event_competitor_record` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/record` | keep (no R twin found) |
| `espn_wbb_event_competitor_roster` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/roster` | keep (exact R match) |
| `espn_wbb_event_competitor_statistics` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/statistics` | keep (exact R match) |
| `espn_wbb_event_competitors` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors` | keep (no R twin found) |
| `espn_wbb_event_leaders` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/leaders` | keep (no R twin found) |
| `espn_wbb_event_odds` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/odds` | keep (exact R match) |
| `espn_wbb_event_official_detail` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/officials/{official_id}` | keep (exact R match) |
| `espn_wbb_event_officials` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/officials` | keep (exact R match) |
| `espn_wbb_event_play` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/plays/{play_id}` | keep (exact R match) |
| `espn_wbb_event_play_personnel` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/plays/{play_id}/personnel` | keep (exact R match) |
| `espn_wbb_event_plays` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/plays` | keep (no R twin found) |
| `espn_wbb_event_powerindex` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/powerindex` | keep (exact R match) |
| `espn_wbb_event_predictor` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/predictor` | keep (exact R match) |
| `espn_wbb_event_probabilities` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/probabilities` | keep (exact R match) |
| `espn_wbb_event_propbets` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/propbets` | keep (exact R match) |
| `espn_wbb_event_scoringplays` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/scoringplays` | keep (no R twin found) |
| `espn_wbb_event_situation` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/situation` | keep (exact R match) |
| `espn_wbb_event_status` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/status` | keep (no R twin found) |
| `espn_wbb_events` | core_v2 | `/{sport}/leagues/{league}/events` | rename to `espn_wbb_game_rosters` (check) |
| `espn_wbb_franchise` | core_v2 | `/{sport}/leagues/{league}/franchises/{franchise_id}` | keep (exact R match) |
| `espn_wbb_franchises` | core_v2 | `/{sport}/leagues/{league}/franchises` | keep (exact R match) |
| `espn_wbb_injuries` | site_v2 | `/{sport}/{league}/injuries` | keep (exact R match) |
| `espn_wbb_leaders` | web_v3 | `/{sport}/{league}/statistics/byathlete` | keep (exact R match) |
| `espn_wbb_leaders_core` | core_v2 | `/{sport}/leagues/{league}/leaders` | keep (no R twin found) |
| `espn_wbb_league_notes` | core_v2 | `/{sport}/leagues/{league}/notes` | keep (no R twin found) |
| `espn_wbb_league_root` | core_v2 | `/{sport}/leagues/{league}` | keep (no R twin found) |
| `espn_wbb_news` | site_v2 | `/{sport}/{league}/news` | keep (exact R match) |
| `espn_wbb_position` | core_v2 | `/{sport}/leagues/{league}/positions/{position_id}` | keep (exact R match) |
| `espn_wbb_positions` | core_v2 | `/{sport}/leagues/{league}/positions` | keep (exact R match) |
| `espn_wbb_rankings` | site_v2 | `/{sport}/{league}/rankings` | keep (exact R match) |
| `espn_wbb_scoreboard` | site_v2 | `/{sport}/{league}/scoreboard` | keep (exact R match) |
| `espn_wbb_season_athletes` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/athletes` | keep (no R twin found) |
| `espn_wbb_season_awards` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/awards` | keep (exact R match) |
| `espn_wbb_season_coaches` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/coaches` | keep (no R twin found) |
| `espn_wbb_season_draft` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/draft` | keep (no R twin found) |
| `espn_wbb_season_draft_round_picks` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/draft/rounds/{round_num}/picks` | keep (no R twin found) |
| `espn_wbb_season_freeagents` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/freeagents` | keep (no R twin found) |
| `espn_wbb_season_futures` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/futures` | keep (no R twin found) |
| `espn_wbb_season_group` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups/{group_id}` | keep (exact R match) |
| `espn_wbb_season_group_children` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups/{group_id}/children` | keep (exact R match) |
| `espn_wbb_season_group_teams` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups/{group_id}/teams` | keep (exact R match) |
| `espn_wbb_season_groups` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups` | keep (exact R match) |
| `espn_wbb_season_info` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}` | keep (exact R match) |
| `espn_wbb_season_pointer` | core_v2 | `/{sport}/leagues/{league}/season` | keep (no R twin found) |
| `espn_wbb_season_powerindex` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/powerindex[/{team_id}]` | keep (no R twin found) |
| `espn_wbb_season_powerindex_leaders` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/powerindex/leaders` | keep (no R twin found) |
| `espn_wbb_season_recruits` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/recruits` | keep (no R twin found) |
| `espn_wbb_season_team` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/teams/{team_id}` | keep (no R twin found) |
| `espn_wbb_season_teams` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/teams` | keep (no R twin found) |
| `espn_wbb_season_type` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}` | keep (exact R match) |
| `espn_wbb_season_type_corrections` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/corrections` | keep (no R twin found) |
| `espn_wbb_season_type_leaders` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/leaders` | keep (no R twin found) |
| `espn_wbb_season_types` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types` | keep (exact R match) |
| `espn_wbb_season_week` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}` | keep (exact R match) |
| `espn_wbb_season_week_events` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}/events` | keep (no R twin found) |
| `espn_wbb_season_week_rankings` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}/rankings` | keep (no R twin found) |
| `espn_wbb_season_weeks` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks` | keep (exact R match) |
| `espn_wbb_seasons` | core_v2 | `/{sport}/leagues/{league}/seasons` | keep (exact R match) |
| `espn_wbb_standings` | site_v2_alt | `/{sport}/{league}/standings` | keep (exact R match) |
| `espn_wbb_standings_core` | core_v2 | `/{sport}/leagues/{league}/standings` | keep (no R twin found) |
| `espn_wbb_statistics_league` | site_v2 | `/{sport}/{league}/statistics` | keep (no R twin found) |
| `espn_wbb_summary` | site_v2 | `/{sport}/{league}/summary` | review: `espn_wbb_game_all` | `espn_wbb_pbp` | `espn_wbb_player_box` | `espn_wbb_team_box` |
| `espn_wbb_talentpicks` | core_v2 | `/{sport}/leagues/{league}/talentpicks` | keep (no R twin found) |
| `espn_wbb_team` | site_v2 | `/{sport}/{league}/teams/{team_id}` | keep (exact R match) |
| `espn_wbb_team_core` | core_v2 | `/{sport}/leagues/{league}/teams/{team_id}` | keep (no R twin found) |
| `espn_wbb_team_depthcharts` | site_v2 | `/{sport}/{league}/teams/{team_id}/depthcharts` | keep (no R twin found) |
| `espn_wbb_team_history` | site_v2 | `/{sport}/{league}/teams/{team_id}/history` | keep (no R twin found) |
| `espn_wbb_team_injuries` | site_v2 | `/{sport}/{league}/teams/{team_id}/injuries` | keep (exact R match) |
| `espn_wbb_team_leaders` | site_v2 | `/{sport}/{league}/teams/{team_id}/leaders` | keep (exact R match) |
| `espn_wbb_team_news` | site_v2 | `/{sport}/{league}/teams/{team_id}/news` | keep (exact R match) |
| `espn_wbb_team_record` | site_v2 | `/{sport}/{league}/teams/{team_id}/record` | keep (no R twin found) |
| `espn_wbb_team_roster` | site_v2 | `/{sport}/{league}/teams/{team_id}/roster` | keep (exact R match) |
| `espn_wbb_team_schedule` | site_v2 | `/{sport}/{league}/teams/{team_id}/schedule` | keep (exact R match) |
| `espn_wbb_team_transactions` | site_v2 | `/{sport}/{league}/teams/{team_id}/transactions` | keep (no R twin found) |
| `espn_wbb_teams_core` | core_v2 | `/{sport}/leagues/{league}/teams` | keep (no R twin found) |
| `espn_wbb_teams_site` | site_v2 | `/{sport}/{league}/teams` | rename to `espn_wbb_teams` (high) |
| `espn_wbb_tournaments` | core_v2 | `/{sport}/leagues/{league}/tournaments` | keep (exact R match) |
| `espn_wbb_transactions` | site_v2 | `/{sport}/{league}/transactions` | keep (no R twin found) |
| `espn_wbb_venue` | core_v2 | `/{sport}/leagues/{league}/venues/{venue_id}` | keep (no R twin found) |
| `espn_wbb_venues` | core_v2 | `/{sport}/leagues/{league}/venues` | keep (exact R match) |

## cfb (vs cfbfastR)

- generated 120 | exact 24 | suggested-rename 29 | review(1->many) 3 | keep 64

| generated | host | path | suggestion |
|-----------|------|------|------------|
| `espn_cfb_athlete_awards` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/awards` | keep (no R twin found) |
| `espn_cfb_athlete_bio` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}/bio` | keep (no R twin found) |
| `espn_cfb_athlete_career_stats` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/statistics[/{stat_type}]` | keep (no R twin found) |
| `espn_cfb_athlete_contracts` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/contracts` | keep (no R twin found) |
| `espn_cfb_athlete_core` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}` | keep (no R twin found) |
| `espn_cfb_athlete_eventlog` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/eventlog` | keep (no R twin found) |
| `espn_cfb_athlete_gamelog` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/gamelog` | rename to `espn_cfb_player_gamelog` (high) |
| `espn_cfb_athlete_info` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}` | keep (no R twin found) |
| `espn_cfb_athlete_injuries` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/injuries` | keep (no R twin found) |
| `espn_cfb_athlete_news` | site_v2 | `/{sport}/{league}/athletes/{athlete_id}/news` | keep (no R twin found) |
| `espn_cfb_athlete_notes` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/notes` | keep (no R twin found) |
| `espn_cfb_athlete_overview` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/overview` | rename to `espn_cfb_player_overview` (high) |
| `espn_cfb_athlete_records` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/records` | keep (no R twin found) |
| `espn_cfb_athlete_seasons` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/seasons` | keep (no R twin found) |
| `espn_cfb_athlete_splits` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/splits` | rename to `espn_cfb_player_splits` (high) |
| `espn_cfb_athlete_statisticslog` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/statisticslog` | rename to `espn_cfb_player_seasons` (check) |
| `espn_cfb_athlete_stats` | web_v3 | `/{sport}/{league}/athletes/{athlete_id}/stats` | keep (no R twin found) |
| `espn_cfb_athlete_vs_athlete` | core_v2 | `/{sport}/leagues/{league}/athletes/{athlete_id}/vsathlete/{opp_id}` | keep (no R twin found) |
| `espn_cfb_athletes_index` | core_v2 | `/{sport}/leagues/{league}/athletes` | keep (no R twin found) |
| `espn_cfb_award` | core_v2 | `/{sport}/leagues/{league}/awards/{award_id}` | keep (exact R match) |
| `espn_cfb_awards` | core_v2 | `/{sport}/leagues/{league}/awards` | keep (exact R match) |
| `espn_cfb_calendar` | site_v2 | `/{sport}/{league}/calendar` | keep (exact R match) |
| `espn_cfb_calendar_offseason` | site_v2 | `/{sport}/{league}/calendar/offseason` | keep (no R twin found) |
| `espn_cfb_calendar_ondays` | site_v2 | `/{sport}/{league}/calendar/ondays` | keep (no R twin found) |
| `espn_cfb_calendar_postseason` | site_v2 | `/{sport}/{league}/calendar/postseason` | keep (no R twin found) |
| `espn_cfb_calendar_regular_season` | site_v2 | `/{sport}/{league}/calendar/regular-season` | keep (no R twin found) |
| `espn_cfb_coach` | core_v2 | `/{sport}/leagues/{league}/coaches/{coach_id}` | keep (exact R match) |
| `espn_cfb_coach_record` | core_v2 | `/{sport}/leagues/{league}/coaches/{coach_id}/record/{record_type}` | keep (exact R match) |
| `espn_cfb_coach_season` | core_v2 | `/{sport}/leagues/{league}/coaches/{coach_id}/seasons/{season}` | keep (no R twin found) |
| `espn_cfb_coaches` | core_v2 | `/{sport}/leagues/{league}/coaches` | keep (exact R match) |
| `espn_cfb_conferences` | site_v2 | `/{sport}/{league}/groups` | keep (no R twin found) |
| `espn_cfb_draft` | site_v2 | `/{sport}/{league}/draft` | keep (no R twin found) |
| `espn_cfb_event` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}` | rename to `espn_cfb_pbp` (check) |
| `espn_cfb_event_broadcasts` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/broadcasts` | rename to `espn_cfb_game_broadcasts` (high) |
| `espn_cfb_event_competition` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}` | rename to `espn_cfb_game_player_statistics` (check) |
| `espn_cfb_event_competitor` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}` | keep (no R twin found) |
| `espn_cfb_event_competitor_leaders` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/leaders` | rename to `espn_cfb_game_team_leaders` (high) |
| `espn_cfb_event_competitor_linescores` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/linescores` | rename to `espn_cfb_game_team_linescores` (high) |
| `espn_cfb_event_competitor_record` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/record` | keep (no R twin found) |
| `espn_cfb_event_competitor_roster` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/roster` | rename to `espn_cfb_game_team_roster` (high) |
| `espn_cfb_event_competitor_statistics` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors/{team_id}/statistics` | review: `espn_cfb_game_player_box` | `espn_cfb_game_team_statistics` |
| `espn_cfb_event_competitors` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/competitors` | review: `espn_cfb_game_player_box` | `espn_cfb_game_team_leaders` | `espn_cfb_game_team_linescores` | `espn_cfb_game_team_records` | `espn_cfb_game_team_roster` | `espn_cfb_game_team_statistics` | `espn_cfb_game_teams` |
| `espn_cfb_event_leaders` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/leaders` | rename to `espn_cfb_game_leaders` (high) |
| `espn_cfb_event_odds` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/odds` | rename to `espn_cfb_game_odds` (high) |
| `espn_cfb_event_official_detail` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/officials/{official_id}` | keep (no R twin found) |
| `espn_cfb_event_officials` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/officials` | keep (no R twin found) |
| `espn_cfb_event_play` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/plays/{play_id}` | rename to `espn_cfb_game_play` (high) |
| `espn_cfb_event_play_personnel` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/plays/{play_id}/personnel` | keep (no R twin found) |
| `espn_cfb_event_plays` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/plays` | review: `espn_cfb_game_drives` | `espn_cfb_game_pbp` |
| `espn_cfb_event_powerindex` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/powerindex` | rename to `espn_cfb_game_powerindex` (high) |
| `espn_cfb_event_predictor` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/predictor` | rename to `espn_cfb_game_predictor` (high) |
| `espn_cfb_event_probabilities` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/probabilities` | rename to `espn_cfb_game_probabilities` (high) |
| `espn_cfb_event_propbets` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/propbets` | keep (no R twin found) |
| `espn_cfb_event_scoringplays` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/scoringplays` | keep (no R twin found) |
| `espn_cfb_event_situation` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/situation` | rename to `espn_cfb_game_situation` (high) |
| `espn_cfb_event_status` | core_v2 | `/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/status` | rename to `espn_cfb_game_status` (high) |
| `espn_cfb_events` | core_v2 | `/{sport}/leagues/{league}/events` | keep (no R twin found) |
| `espn_cfb_franchise` | core_v2 | `/{sport}/leagues/{league}/franchises/{franchise_id}` | keep (exact R match) |
| `espn_cfb_franchises` | core_v2 | `/{sport}/leagues/{league}/franchises` | keep (exact R match) |
| `espn_cfb_injuries` | site_v2 | `/{sport}/{league}/injuries` | keep (no R twin found) |
| `espn_cfb_leaders` | web_v3 | `/{sport}/{league}/statistics/byathlete` | keep (no R twin found) |
| `espn_cfb_leaders_core` | core_v2 | `/{sport}/leagues/{league}/leaders` | keep (no R twin found) |
| `espn_cfb_league_notes` | core_v2 | `/{sport}/leagues/{league}/notes` | keep (no R twin found) |
| `espn_cfb_league_root` | core_v2 | `/{sport}/leagues/{league}` | keep (no R twin found) |
| `espn_cfb_news` | site_v2 | `/{sport}/{league}/news` | keep (no R twin found) |
| `espn_cfb_position` | core_v2 | `/{sport}/leagues/{league}/positions/{position_id}` | keep (exact R match) |
| `espn_cfb_positions` | core_v2 | `/{sport}/leagues/{league}/positions` | keep (exact R match) |
| `espn_cfb_rankings` | site_v2 | `/{sport}/{league}/rankings` | keep (exact R match) |
| `espn_cfb_scoreboard` | site_v2 | `/{sport}/{league}/scoreboard` | keep (exact R match) |
| `espn_cfb_season_athletes` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/athletes` | rename to `espn_cfb_players` (check) |
| `espn_cfb_season_awards` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/awards` | rename to `espn_cfb_awards` (high) |
| `espn_cfb_season_coaches` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/coaches` | rename to `espn_cfb_coaches` (high) |
| `espn_cfb_season_draft` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/draft` | keep (no R twin found) |
| `espn_cfb_season_draft_round_picks` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/draft/rounds/{round_num}/picks` | keep (no R twin found) |
| `espn_cfb_season_freeagents` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/freeagents` | keep (no R twin found) |
| `espn_cfb_season_futures` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/futures` | rename to `espn_cfb_futures` (high) |
| `espn_cfb_season_group` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups/{group_id}` | keep (no R twin found) |
| `espn_cfb_season_group_children` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups/{group_id}/children` | keep (no R twin found) |
| `espn_cfb_season_group_teams` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups/{group_id}/teams` | keep (no R twin found) |
| `espn_cfb_season_groups` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups` | rename to `espn_cfb_groups` (high) |
| `espn_cfb_season_info` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}` | keep (exact R match) |
| `espn_cfb_season_pointer` | core_v2 | `/{sport}/leagues/{league}/season` | keep (no R twin found) |
| `espn_cfb_season_powerindex` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/powerindex[/{team_id}]` | rename to `espn_cfb_team_powerindex` (high) |
| `espn_cfb_season_powerindex_leaders` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/powerindex/leaders` | keep (no R twin found) |
| `espn_cfb_season_qbr` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}[/groups/{group_id}]/qbr/{split}` | keep (no R twin found) |
| `espn_cfb_season_qbr_week` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}/qbr/{split}` | keep (no R twin found) |
| `espn_cfb_season_recruits` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/recruits` | rename to `espn_cfb_recruits` (high) |
| `espn_cfb_season_team` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/teams/{team_id}` | rename to `espn_cfb_team` (high) |
| `espn_cfb_season_teams` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/teams` | keep (no R twin found) |
| `espn_cfb_season_type` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}` | keep (no R twin found) |
| `espn_cfb_season_type_corrections` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/corrections` | keep (no R twin found) |
| `espn_cfb_season_type_leaders` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/leaders` | keep (no R twin found) |
| `espn_cfb_season_types` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types` | keep (exact R match) |
| `espn_cfb_season_week` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}` | keep (no R twin found) |
| `espn_cfb_season_week_events` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}/events` | keep (no R twin found) |
| `espn_cfb_season_week_rankings` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}/rankings` | rename to `espn_cfb_week_rankings` (high) |
| `espn_cfb_season_weeks` | core_v2 | `/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks` | keep (exact R match) |
| `espn_cfb_seasons` | core_v2 | `/{sport}/leagues/{league}/seasons` | keep (exact R match) |
| `espn_cfb_standings` | site_v2_alt | `/{sport}/{league}/standings` | keep (exact R match) |
| `espn_cfb_standings_core` | core_v2 | `/{sport}/leagues/{league}/standings` | keep (no R twin found) |
| `espn_cfb_statistics_league` | site_v2 | `/{sport}/{league}/statistics` | keep (no R twin found) |
| `espn_cfb_summary` | site_v2 | `/{sport}/{league}/summary` | rename to `espn_cfb_pbp` (check) |
| `espn_cfb_talentpicks` | core_v2 | `/{sport}/leagues/{league}/talentpicks` | keep (no R twin found) |
| `espn_cfb_team` | site_v2 | `/{sport}/{league}/teams/{team_id}` | keep (exact R match) |
| `espn_cfb_team_core` | core_v2 | `/{sport}/leagues/{league}/teams/{team_id}` | keep (no R twin found) |
| `espn_cfb_team_depthcharts` | site_v2 | `/{sport}/{league}/teams/{team_id}/depthcharts` | keep (no R twin found) |
| `espn_cfb_team_history` | site_v2 | `/{sport}/{league}/teams/{team_id}/history` | keep (no R twin found) |
| `espn_cfb_team_injuries` | site_v2 | `/{sport}/{league}/teams/{team_id}/injuries` | keep (no R twin found) |
| `espn_cfb_team_leaders` | site_v2 | `/{sport}/{league}/teams/{team_id}/leaders` | keep (exact R match) |
| `espn_cfb_team_news` | site_v2 | `/{sport}/{league}/teams/{team_id}/news` | keep (no R twin found) |
| `espn_cfb_team_record` | site_v2 | `/{sport}/{league}/teams/{team_id}/record` | keep (exact R match) |
| `espn_cfb_team_roster` | site_v2 | `/{sport}/{league}/teams/{team_id}/roster` | keep (exact R match) |
| `espn_cfb_team_schedule` | site_v2 | `/{sport}/{league}/teams/{team_id}/schedule` | keep (exact R match) |
| `espn_cfb_team_transactions` | site_v2 | `/{sport}/{league}/teams/{team_id}/transactions` | keep (no R twin found) |
| `espn_cfb_teams_core` | core_v2 | `/{sport}/leagues/{league}/teams` | keep (no R twin found) |
| `espn_cfb_teams_site` | site_v2 | `/{sport}/{league}/teams` | rename to `espn_cfb_teams` (high) |
| `espn_cfb_tournaments` | core_v2 | `/{sport}/leagues/{league}/tournaments` | keep (no R twin found) |
| `espn_cfb_transactions` | site_v2 | `/{sport}/{league}/transactions` | keep (no R twin found) |
| `espn_cfb_venue` | core_v2 | `/{sport}/leagues/{league}/venues/{venue_id}` | keep (exact R match) |
| `espn_cfb_venues` | core_v2 | `/{sport}/leagues/{league}/venues` | keep (exact R match) |
