# ICE Hockey League — ICE Hockey League data feed (eSports hokejovyzapis S3 bucket, Austria)

> ICE Hockey League — ICE Hockey League data feed (eSports hokejovyzapis S3 bucket, Austria) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.hockey.icehl` — 12 endpoints.

## Game

| Function | Summary |
|---|---|
| [icehl_game_info](icehl_esports/game.md#icehl_game_info) | Game info of one match: state, arena, attendance, officials, coaches, period scores. |
| [icehl_game_center](icehl_esports/game.md#icehl_game_center) | Game-center bundle of one match as one row: game data, team stats, goal and penalty lists. |

## Other

| Function | Summary |
|---|---|
| [icehl_schedule](icehl_esports/other.md#icehl_schedule) | Schedule and results of one competition season, one row per match, played or not. |
| [icehl_league_part_player_stats](icehl_esports/other.md#icehl_league_part_player_stats) | Every skater's stats in one league part (a competition stage), one row per player. |
| [icehl_team_player_stats](icehl_esports/other.md#icehl_team_player_stats) | Season player stats of one team in one competition, one row per player. |
| [icehl_top_scorers](icehl_esports/other.md#icehl_top_scorers) | Skater scoring leaderboard of one competition season. |
| [icehl_faceoffs](icehl_esports/other.md#icehl_faceoffs) | Every faceoff of one match: time, dot, winner and loser. |
| [icehl_pbp](icehl_esports/other.md#icehl_pbp) | Play-by-play of one match: goals, penalties, goalie changes and timeouts, one row per event. |
| [icehl_player_box](icehl_esports/other.md#icehl_player_box) | Player boxscore of one team in one match, one row per player. |
| [icehl_goalie_box](icehl_esports/other.md#icehl_goalie_box) | Goalie lines of one match, one row per goalie who played. |
| [icehl_standings](icehl_esports/other.md#icehl_standings) | Standings of one competition season, one row per team. |
| [icehl_shots](icehl_esports/other.md#icehl_shots) | Shots of one match with rink coordinates and result, one row per shot. |
