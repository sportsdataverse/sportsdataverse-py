---
title: NFL — nflpro
sidebar_label: nflpro
description: "NFL — nflpro — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 11
toc_max_heading_level: 2
---
# NFL — nflpro

`sportsdataverse.nfl` — 16 endpoints.

## Defense

| Function | Summary |
|---|---|
| [nfl_pro_defense_overview_season](nflpro/defense.md#nfl_pro_defense_overview_season) | GET /api/secured/stats/defense/overview/season — one row per defender for the season — defensive overview incl. snap counts, pressures and havoc stops. |
| [nfl_pro_defense_overview_week](nflpro/defense.md#nfl_pro_defense_overview_week) | GET /api/secured/stats/defense/overview/week — one row per defender per week — defensive overview incl. snap counts, pressures and havoc stops. |
| [nfl_pro_defense_nearest_season](nflpro/defense.md#nfl_pro_defense_nearest_season) | GET /api/secured/stats/defense/nearest/season — one row per defender for the season — nearest-defender coverage incl. targets, catch rate and CROE allowed. |
| [nfl_pro_defense_nearest_week](nflpro/defense.md#nfl_pro_defense_nearest_week) | GET /api/secured/stats/defense/nearest/week — one row per defender per week — nearest-defender coverage incl. targets, catch rate and CROE allowed. |

## Fantasy

| Function | Summary |
|---|---|
| [nfl_pro_fantasy_season](nflpro/fantasy.md#nfl_pro_fantasy_season) | GET /api/secured/stats/fantasy/season — one row per player for the season — fantasy points, opportunity and usage. |
| [nfl_pro_fantasy_game](nflpro/fantasy.md#nfl_pro_fantasy_game) | GET /api/secured/stats/fantasy/game — one row per player-game — fantasy scoring by game. Requires `position_group`. |

## Players

| Function | Summary |
|---|---|
| [nfl_pro_players_offense_passing_season](nflpro/players.md#nfl_pro_players_offense_passing_season) | GET /api/secured/stats/players-offense/passing/season — one row per passer for the season — quarterback passing incl. Next Gen time-to-throw, aggressiveness … |
| [nfl_pro_players_offense_passing_week](nflpro/players.md#nfl_pro_players_offense_passing_week) | GET /api/secured/stats/players-offense/passing/week — one row per passer per week — quarterback passing incl. Next Gen time-to-throw, aggressiveness and CPOE. |
| [nfl_pro_players_offense_rushing_season](nflpro/players.md#nfl_pro_players_offense_rushing_season) | GET /api/secured/stats/players-offense/rushing/season — one row per rusher for the season — rushing incl. Next Gen efficiency, yards over expected and … |
| [nfl_pro_players_offense_rushing_week](nflpro/players.md#nfl_pro_players_offense_rushing_week) | GET /api/secured/stats/players-offense/rushing/week — one row per rusher per week — rushing incl. Next Gen efficiency, yards over expected and defenders-in-box. |
| [nfl_pro_players_offense_receiving_season](nflpro/players.md#nfl_pro_players_offense_receiving_season) | GET /api/secured/stats/players-offense/receiving/season — one row per receiver for the season — receiving incl. Next Gen separation, cushion and catch rate … |
| [nfl_pro_players_offense_receiving_week](nflpro/players.md#nfl_pro_players_offense_receiving_week) | GET /api/secured/stats/players-offense/receiving/week — one row per receiver per week — receiving incl. Next Gen separation, cushion and catch rate over … |

## Team

| Function | Summary |
|---|---|
| [nfl_pro_team_offense_overview_season](nflpro/team.md#nfl_pro_team_offense_overview_season) | GET /api/secured/stats/team-offense/overview/season — one row per team for the season — team offensive overview incl. EPA per play, pass and rush splits. |
| [nfl_pro_team_offense_overview_week](nflpro/team.md#nfl_pro_team_offense_overview_week) | GET /api/secured/stats/team-offense/overview/week — one row per team per week — team offensive overview incl. EPA per play, pass and rush splits. |
| [nfl_pro_team_defense_overview_season](nflpro/team.md#nfl_pro_team_defense_overview_season) | GET /api/secured/stats/team-defense/overview/season — one row per team for the season — team defensive overview incl. EPA allowed per play and takeaways. |
| [nfl_pro_team_defense_overview_week](nflpro/team.md#nfl_pro_team_defense_overview_week) | GET /api/secured/stats/team-defense/overview/week — one row per team per week — team defensive overview incl. EPA allowed per play and takeaways. |
