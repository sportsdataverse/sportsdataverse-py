---
title: ATRIUM — additional Python functions
sidebar_label: Additional functions
description: "ATRIUM — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# ATRIUM — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.basketball.atrium`
not covered by the generated API-endpoint reference above.

## Sportradar Atrium embed API

| Function | Summary |
|---|---|
| [lmb_pbp](additional/sportradar-atrium-embed-api.md#lmb_pbp) | Play-by-play of a Liga Mayor de Baloncesto (El Salvador) game. |
| [lmb_player_box](additional/sportradar-atrium-embed-api.md#lmb_player_box) | Player box score of a Liga Mayor de Baloncesto (El Salvador) game. |
| [lmb_schedule](additional/sportradar-atrium-embed-api.md#lmb_schedule) | Fixtures (schedule and results) of a Liga Mayor de Baloncesto (El Salvador) season. |
| [lmb_seasons](additional/sportradar-atrium-embed-api.md#lmb_seasons) | Every season and competition id of Liga Mayor de Baloncesto (El Salvador). |
| [lmb_shots](additional/sportradar-atrium-embed-api.md#lmb_shots) | Shot chart of a Liga Mayor de Baloncesto (El Salvador) game. |
| [lmb_standings](additional/sportradar-atrium-embed-api.md#lmb_standings) | Standings of a Liga Mayor de Baloncesto (El Salvador) season. |
| [lmb_team_box](additional/sportradar-atrium-embed-api.md#lmb_team_box) | Team box score of a Liga Mayor de Baloncesto (El Salvador) game. |
| [lmb_teams](additional/sportradar-atrium-embed-api.md#lmb_teams) | Teams of a Liga Mayor de Baloncesto (El Salvador) season. |
| [lnb_pbp](additional/sportradar-atrium-embed-api.md#lnb_pbp) | Play-by-play of a LNB (France) game. |
| [lnb_player_box](additional/sportradar-atrium-embed-api.md#lnb_player_box) | Player box score of a LNB (France) game. |
| [lnb_schedule](additional/sportradar-atrium-embed-api.md#lnb_schedule) | Fixtures (schedule and results) of a LNB (France) season. |
| [lnb_seasons](additional/sportradar-atrium-embed-api.md#lnb_seasons) | Every season and competition id of LNB (France). |
| [lnb_shots](additional/sportradar-atrium-embed-api.md#lnb_shots) | Shot chart of a LNB (France) game. |
| [lnb_standings](additional/sportradar-atrium-embed-api.md#lnb_standings) | Standings of a LNB (France) season. |
| [lnb_team_box](additional/sportradar-atrium-embed-api-2.md#lnb_team_box) | Team box score of a LNB (France) game. |
| [lnb_teams](additional/sportradar-atrium-embed-api-2.md#lnb_teams) | Teams of a LNB (France) season. |
| [lnbp_pbp](additional/sportradar-atrium-embed-api-2.md#lnbp_pbp) | Play-by-play of a LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) game. |
| [lnbp_player_box](additional/sportradar-atrium-embed-api-2.md#lnbp_player_box) | Player box score of a LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) game. |
| [lnbp_schedule](additional/sportradar-atrium-embed-api-2.md#lnbp_schedule) | Fixtures (schedule and results) of a LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) season. |
| [lnbp_seasons](additional/sportradar-atrium-embed-api-2.md#lnbp_seasons) | Every season and competition id of LNBP (Mexico, with COPA VALUE and LNBP FEMENIL). |
| [lnbp_shots](additional/sportradar-atrium-embed-api-2.md#lnbp_shots) | Shot chart of a LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) game. |
| [lnbp_standings](additional/sportradar-atrium-embed-api-2.md#lnbp_standings) | Standings of a LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) season. |
| [lnbp_team_box](additional/sportradar-atrium-embed-api-2.md#lnbp_team_box) | Team box score of a LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) game. |
| [lnbp_teams](additional/sportradar-atrium-embed-api-2.md#lnbp_teams) | Teams of a LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) season. |
| [lpb_pbp](additional/sportradar-atrium-embed-api-2.md#lpb_pbp) | Play-by-play of a LPB / LPBF Liga Profesional de Baloncesto (Panama) game. |
| [lpb_player_box](additional/sportradar-atrium-embed-api-2.md#lpb_player_box) | Player box score of a LPB / LPBF Liga Profesional de Baloncesto (Panama) game. |
| [lpb_schedule](additional/sportradar-atrium-embed-api-2.md#lpb_schedule) | Fixtures (schedule and results) of a LPB / LPBF Liga Profesional de Baloncesto (Panama) season. |
| [lpb_seasons](additional/sportradar-atrium-embed-api-2.md#lpb_seasons) | Every season and competition id of LPB / LPBF Liga Profesional de Baloncesto (Panama). |
| [lpb_shots](additional/sportradar-atrium-embed-api-2.md#lpb_shots) | Shot chart of a LPB / LPBF Liga Profesional de Baloncesto (Panama) game. |
| [lpb_standings](additional/sportradar-atrium-embed-api-3.md#lpb_standings) | Standings of a LPB / LPBF Liga Profesional de Baloncesto (Panama) season. |
| [lpb_team_box](additional/sportradar-atrium-embed-api-3.md#lpb_team_box) | Team box score of a LPB / LPBF Liga Profesional de Baloncesto (Panama) game. |
| [lpb_teams](additional/sportradar-atrium-embed-api-3.md#lpb_teams) | Teams of a LPB / LPBF Liga Profesional de Baloncesto (Panama) season. |
| [nbl1_pbp](additional/sportradar-atrium-embed-api-3.md#nbl1_pbp) | Play-by-play of a NBL1 (Australia) game. |
| [nbl1_player_box](additional/sportradar-atrium-embed-api-3.md#nbl1_player_box) | Player box score of a NBL1 (Australia) game. |
| [nbl1_schedule](additional/sportradar-atrium-embed-api-3.md#nbl1_schedule) | Fixtures (schedule and results) of a NBL1 (Australia) season. |
| [nbl1_seasons](additional/sportradar-atrium-embed-api-3.md#nbl1_seasons) | Every season and competition id of NBL1 (Australia). |
| [nbl1_shots](additional/sportradar-atrium-embed-api-3.md#nbl1_shots) | Shot chart of a NBL1 (Australia) game. |
| [nbl1_standings](additional/sportradar-atrium-embed-api-3.md#nbl1_standings) | Standings of a NBL1 (Australia) season. |
| [nbl1_team_box](additional/sportradar-atrium-embed-api-3.md#nbl1_team_box) | Team box score of a NBL1 (Australia) game. |
| [nbl1_teams](additional/sportradar-atrium-embed-api-3.md#nbl1_teams) | Teams of a NBL1 (Australia) season. |

## Dates and seasons

| Function | Summary |
|---|---|
| [atrium_season_id](additional/dates-and-seasons.md#atrium_season_id) | The `seasonId` of a site's season, from its END year and optionally its competition. |
