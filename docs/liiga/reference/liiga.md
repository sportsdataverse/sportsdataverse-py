# LIIGA — Liiga API v2 (liiga.fi/api/v2, Finland)

> LIIGA — Liiga API v2 (liiga.fi/api/v2, Finland) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.hockey.liiga` — 23 endpoints.

## Game

| Function | Summary |
|---|---|
| [liiga_game_goals](liiga/game.md#liiga_game_goals) | One game's goals: scorer, assists, strength codes and skaters on ice. |
| [liiga_game_penalties](liiga/game.md#liiga_game_penalties) | One game's penalties, both teams: player, fault, minutes, start and end. |
| [liiga_game_shootout](liiga/game.md#liiga_game_shootout) | One game's shootout attempts: shooter, goalkeeper and result (none for a game without one). |
| [liiga_game_lineups](liiga/game.md#liiga_game_lineups) | One game's lineups, both teams: line, role, jersey, captaincy and bio. |
| [liiga_game_players_stats](liiga/game.md#liiga_game_players_stats) | One game's lineups with each player's all-time, season, season-in-team and versus-team stats. |
| [liiga_game_stats](liiga/game.md#liiga_game_stats) | One game's per-period player box score, skaters and goalies, with tracking where Liiga has it. |
| [liiga_game_team_stats](liiga/game.md#liiga_game_team_stats) | One game's per-period team stats with puck possession. |

## Player

| Function | Summary |
|---|---|
| [liiga_players_info](liiga/player.md#liiga_players_info) | Roster and bio of every player in a season range. |
| [liiga_player](liiga/player.md#liiga_player) | One player's bio, teams by season and active seasons. |
| [liiga_player_career](liiga/player.md#liiga_player_career) | One player's career: season totals by tournament. |
| [liiga_player_game_log](liiga/player.md#liiga_player_game_log) | One player's game log for a season, every tournament. |
| [liiga_player_stats](liiga/player.md#liiga_player_stats) | Player stat table summed over a season range, by data_type (the whole league, unfiltered). |

## Team

| Function | Summary |
|---|---|
| [liiga_teams_history](liiga/team.md#liiga_teams_history) | Every team's regular-season record, one row per team and season since 1976. |
| [liiga_teams](liiga/team.md#liiga_teams) | Team directory: every current and past team with logos, venue capacity and club info. |
| [liiga_team_stats](liiga/team.md#liiga_team_stats) | Team stat table of a season range, by data_type. |

## Other

| Function | Summary |
|---|---|
| [liiga_tournament](liiga/other.md#liiga_tournament) | Current state of the league: game week of the season in progress and its last game week. |
| [liiga_games](liiga/other.md#liiga_games) | Every game of a season and tournament: teams, score, xG, power plays, periods and rink. |
| [liiga_goals](liiga/other.md#liiga_goals) | Every goal of a season and tournament: scorer, assists, strength codes and skaters on ice. |
| [liiga_schedule](liiga/other.md#liiga_schedule) | Schedule of a season and tournament: game ids, teams, goals, xG, start and rink. |
| [liiga_gameweeks](liiga/other.md#liiga_gameweeks) | Number of game weeks of a season and tournament. |
| [liiga_game](liiga/other.md#liiga_game) | One game's header: teams, score, xG, power plays, periods, rink, referees and attendance. |
| [liiga_shotmap](liiga/other.md#liiga_shotmap) | One game's shots with rink-image coordinates (pixels), shooter, blocker and skaters on ice; from 2016. |
| [liiga_standings](liiga/other.md#liiga_standings) | Standings of a season: regular season, playoffs, preseason, playout and qualifications tables. |
