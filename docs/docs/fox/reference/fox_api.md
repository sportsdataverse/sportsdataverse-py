---
title: Fox Sports — Fox Sports API (api.foxsports.com)
sidebar_label: Fox Sports API (api.foxsports.com)
description: "Fox Sports — Fox Sports API (api.foxsports.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# Fox Sports — Fox Sports API (api.foxsports.com)

`sportsdataverse.providers.fox` — 33 endpoints.

## Event

| Function | Summary |
|---|---|
| [fox_api_event_data](fox_api/event.md#fox_api_event_data) | GET /bifrost/v1/{sport}/event/{event_id}/data -- Fox Sports API event data. |
| [fox_api_event_matchup](fox_api/event.md#fox_api_event_matchup) | GET /bifrost/v1/{sport}/event/{event_id}/matchup -- Fox Sports API event matchup. |
| [fox_api_event_odds](fox_api/event.md#fox_api_event_odds) | GET /bifrost/v1/{sport}/event/{event_id}/odds -- Fox Sports API event odds. |
| [fox_api_event_recap](fox_api/event.md#fox_api_event_recap) | GET /bifrost/v1/{sport}/event/{event_id}/recap -- Fox Sports API event recap. |
| [fox_api_event_standings](fox_api/event.md#fox_api_event_standings) | GET /bifrost/v1/{sport}/event/{event_id}/standings -- Fox Sports API event standings. |

## League

| Function | Summary |
|---|---|
| [fox_api_league_conferences](fox_api/league.md#fox_api_league_conferences) | GET /bifrost/v1/{sport}/league/conferences -- Fox Sports API league conferences. |
| [fox_api_league_header](fox_api/league.md#fox_api_league_header) | GET /bifrost/v1/{sport}/league/header -- Fox Sports API league header. |
| [fox_api_league_odds](fox_api/league.md#fox_api_league_odds) | GET /bifrost/v1/{sport}/league/odds -- Fox Sports API league odds. |
| [fox_api_league_playernews](fox_api/league.md#fox_api_league_playernews) | GET /bifrost/v1/{sport}/league/playernews -- Fox Sports API league playernews. |
| [fox_api_league_polls](fox_api/league.md#fox_api_league_polls) | GET /bifrost/v1/{sport}/league/polls -- Fox Sports API league polls. |
| [fox_api_league_schedule](fox_api/league.md#fox_api_league_schedule) | GET /bifrost/v1/{sport}/league/schedule -- Fox Sports API league schedule. |
| [fox_api_league_scores](fox_api/league.md#fox_api_league_scores) | GET /bifrost/v1/{sport}/league/scores -- Fox Sports API league scores. |
| [fox_api_league_scores_segment](fox_api/league.md#fox_api_league_scores_segment) | GET /bifrost/v1/{sport}/league/scores-segment/{segment_id} -- Fox Sports API league scores segment. |
| [fox_api_league_standings](fox_api/league.md#fox_api_league_standings) | GET /bifrost/v1/{sport}/league/standings -- Fox Sports API league standings. |
| [fox_api_league_stats](fox_api/league.md#fox_api_league_stats) | GET /bifrost/v1/{sport}/league/stats -- Fox Sports API league stats. |
| [fox_api_league_stats_con](fox_api/league.md#fox_api_league_stats_con) | GET /bifrost/v1/{sport}/league/stats-con/{who}/{category}/{page} -- Fox Sports API league stats con. |
| [fox_api_league_teamnav](fox_api/league.md#fox_api_league_teamnav) | GET /bifrost/v1/{sport}/league/teamnav -- Fox Sports API league teamnav. |

## Team

| Function | Summary |
|---|---|
| [fox_api_team_gamelog](fox_api/team.md#fox_api_team_gamelog) | GET /bifrost/v1/{sport}/team/{team_id}/gamelog -- Fox Sports API team gamelog. |
| [fox_api_team_header](fox_api/team.md#fox_api_team_header) | GET /bifrost/v1/{sport}/team/{team_id}/header -- Fox Sports API team header. |
| [fox_api_team_roster](fox_api/team.md#fox_api_team_roster) | GET /bifrost/v1/{sport}/team/{team_id}/roster -- Fox Sports API team roster. |
| [fox_api_team_standings](fox_api/team.md#fox_api_team_standings) | GET /bifrost/v1/{sport}/team/{team_id}/standings -- Fox Sports API team standings. |
| [fox_api_team_stats](fox_api/team.md#fox_api_team_stats) | GET /bifrost/v1/{sport}/team/{team_id}/stats -- Fox Sports API team stats. |

## Other

| Function | Summary |
|---|---|
| [fox_api_scoreboard](fox_api/other.md#fox_api_scoreboard) | GET /bifrost/v1/{sport}/scoreboard/main -- Fox Sports API scoreboard. |
| [fox_api_scorechip](fox_api/other.md#fox_api_scorechip) | GET /bifrost/v1/{sport}/scorechip/{chip_id} -- one game's score chip (this route 400s if api-version is sent). |
| [fox_api_topevents_scoreboard_segment](fox_api/other.md#fox_api_topevents_scoreboard_segment) | GET /bifrost/v1/topevents/scoreboard/segment/{segment} -- Fox Sports API topevents scoreboard segment. |
| [fox_api_explore_browse](fox_api/other.md#fox_api_explore_browse) | GET /bifrost/v1/explore/browse/{section}/main -- Fox Sports API explore browse. |
| [fox_api_explore_odds](fox_api/other.md#fox_api_explore_odds) | GET /bifrost/v1/explore/odds/main -- Fox Sports API explore odds. |
| [fox_api_search_content](fox_api/other.md#fox_api_search_content) | GET /bifrost/v1/search/content -- Fox Sports API search content. |
| [fox_api_search_entities](fox_api/other.md#fox_api_search_entities) | GET /bifrost/v1/search/entities -- Fox Sports API search entities. |
| [fox_api_search_popular](fox_api/other.md#fox_api_search_popular) | GET /bifrost/v1/search/popular -- Fox Sports API search popular. |
| [fox_api_trending_articles](fox_api/other.md#fox_api_trending_articles) | GET /bifrost/v1/general/trending/articles -- Fox Sports API trending articles. |
| [fox_api_trending_videos](fox_api/other.md#fox_api_trending_videos) | GET /bifrost/v1/general/trending/videos -- Fox Sports API trending videos. |
| [fox_api_foxpolls](fox_api/other.md#fox_api_foxpolls) | GET /foxpolls/v1/polls -- Fox Sports API foxpolls. |
