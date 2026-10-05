---
title: NHL — NHL Web API
sidebar_label: NHL Web API
description: "NHL — NHL Web API — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# NHL — NHL Web API

`sportsdataverse.nhl` — 27 endpoints.

## Club

| Function | Summary |
|---|---|
| [nhl_club_schedule_season](nhl_api_web/club.md#nhl_club_schedule_season) | Pull a team's full-season schedule. |
| [nhl_club_schedule_month](nhl_api_web/club.md#nhl_club_schedule_month) | Pull a team's schedule for one month. |
| [nhl_club_schedule_week](nhl_api_web/club.md#nhl_club_schedule_week) | Pull a team's schedule for one week. |
| [nhl_club_stats](nhl_api_web/club.md#nhl_club_stats) | Pull a team's season stat block. |
| [nhl_club_stats_season](nhl_api_web/club.md#nhl_club_stats_season) | Pull the seasons a team has stats for. |

## Draft

| Function | Summary |
|---|---|
| [nhl_draft_picks](nhl_api_web/draft.md#nhl_draft_picks) | Pull NHL draft picks for a year (and optionally one round). |
| [nhl_draft_rankings](nhl_api_web/draft.md#nhl_draft_rankings) | Pull NHL Central Scouting rankings for a draft year. |
| [nhl_draft_picks_now](nhl_api_web/draft.md#nhl_draft_picks_now) | Pull the current / most recent draft pick set. |
| [nhl_draft_rankings_now](nhl_api_web/draft.md#nhl_draft_rankings_now) | Pull the current Central Scouting rankings. |
| [nhl_draft_tracker_picks_now](nhl_api_web/draft.md#nhl_draft_tracker_picks_now) | Pull the live draft-tracker pick list (during the draft itself). |

## Player

| Function | Summary |
|---|---|
| [nhl_player_landing](nhl_api_web/player.md#nhl_player_landing) | Pull the player profile / overview. |
| [nhl_player_game_log](nhl_api_web/player.md#nhl_player_game_log) | Pull a player's game-by-game log. |
| [nhl_player_spotlight](nhl_api_web/player.md#nhl_player_spotlight) | Pull the league's currently featured players. |

## Web

| Function | Summary |
|---|---|
| [nhl_web_pbp](nhl_api_web/web.md#nhl_web_pbp) | Pull the play-by-play feed for one NHL game. |
| [nhl_web_schedule](nhl_api_web/web.md#nhl_web_schedule) | Pull the week-of NHL schedule rooted at `date`. |

## Other

| Function | Summary |
|---|---|
| [nhl_boxscore](nhl_api_web/other.md#nhl_boxscore) | Pull the boxscore for one NHL game. |
| [nhl_landing](nhl_api_web/other.md#nhl_landing) | Pull the gamecenter landing payload for one NHL game. |
| [nhl_right_rail](nhl_api_web/other.md#nhl_right_rail) | Pull the gamecenter right-rail payload (in-game widgets). |
| [nhl_score](nhl_api_web/other.md#nhl_score) | Pull the single-day scoreboard for `date`. |
| [nhl_schedule_calendar](nhl_api_web/other.md#nhl_schedule_calendar) | Pull the calendar of game-days for the season. |
| [nhl_playoff_series](nhl_api_web/other.md#nhl_playoff_series) | Pull a single playoff series payload. |
| [nhl_standings](nhl_api_web/other.md#nhl_standings) | Pull the NHL standings. |
| [nhl_standings_season](nhl_api_web/other.md#nhl_standings_season) | Pull the per-season standings cutover dates. |
| [nhl_roster](nhl_api_web/other.md#nhl_roster) | Pull a team's roster. |
| [nhl_roster_season](nhl_api_web/other.md#nhl_roster_season) | Pull every season a team has had on file. |
| [nhl_skater_leaders](nhl_api_web/other.md#nhl_skater_leaders) | Pull skater stat leaders. |
| [nhl_goalie_leaders](nhl_api_web/other.md#nhl_goalie_leaders) | Pull goalie stat leaders. |
