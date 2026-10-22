# SIHF — SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league)

> SIHF — SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.hockey.sihf` — 33 endpoints.

## Game

| Function | Summary |
|---|---|
| [sihf_game_info](sihf_api_web/game.md#sihf_game_info) | Header of one game: league, phase, teams, final and per-period score and shots, venue, attendance, officials. |
| [sihf_game_player_stats](sihf_api_web/game.md#sihf_game_player_stats) | Skater box score of one game, both sides, with ice time and faceoffs. |
| [sihf_game_goalie_stats](sihf_api_web/game.md#sihf_game_goalie_stats) | Goaltender box score of one game, both sides. |
| [sihf_game_team_stats](sihf_api_web/game.md#sihf_game_team_stats) | Team stats of one game: shots by period, faceoffs by zone, penalty minutes, power play and penalty kill. |
| [sihf_game_lineups](sihf_api_web/game.md#sihf_game_lineups) | Lineups of one game by unit and slot, plus the absentees. |

## Goalie

| Function | Summary |
|---|---|
| [sihf_goalie_stats](sihf_api_web/goalie.md#sihf_goalie_stats) | Goaltender season totals: games, starts, goals against, GAA, shots, saves, save percentage, ice time. |
| [sihf_goalie_shots](sihf_api_web/goalie.md#sihf_goalie_shots) | Goaltender shots against: on goal (total, slot), missed, post, with per-game rates and the slot save percentage. |
| [sihf_goalie_wins](sihf_api_web/goalie.md#sihf_goalie_wins) | Goaltender decisions: starts, wins and losses in regulation, overtime and shootout, and shutouts. |
| [sihf_goalie_shootouts](sihf_api_web/goalie.md#sihf_goalie_shootouts) | Goaltender shootouts: attempts faced, goals against, saves and save percentage. |

## Player

| Function | Summary |
|---|---|
| [sihf_player_stats](sihf_api_web/player.md#sihf_player_stats) | Skater season totals: games, goals, assists, points, penalty minutes, plus-minus. |
| [sihf_player_goals_assists](sihf_api_web/player.md#sihf_player_goals_assists) | Skater goals and assists by situation: even strength, power play, short-handed, game-winning and overtime goals, hat tricks. |
| [sihf_player_shots](sihf_api_web/player.md#sihf_player_shots) | Skater shots: on goal (total, slot, shooting percentage), missed, post and blocked, with per-game rates. |
| [sihf_player_penalties](sihf_api_web/player.md#sihf_player_penalties) | Skater penalties: penalty minutes, 2-, 5- and 10-minute penalties, game misconducts and match penalties. |
| [sihf_player_shootouts](sihf_api_web/player.md#sihf_player_shootouts) | Skater shootouts: attempts, goals, misses, percentage and shootout-winning goals. |
| [sihf_player_faceoffs](sihf_api_web/player.md#sihf_player_faceoffs) | Skater faceoffs: taken, won, lost, per game, and win percentage overall and by zone. |
| [sihf_player_faceoffs_by_zone](sihf_api_web/player.md#sihf_player_faceoffs_by_zone) | Skater faceoffs by zone (offensive, neutral, defensive): taken, won, lost and win percentage. |
| [sihf_player_faceoffs_by_zone_per_game](sihf_api_web/player.md#sihf_player_faceoffs_by_zone_per_game) | Skater faceoffs by zone (offensive, neutral, defensive) per game: taken, won and lost. |
| [sihf_player_time_on_ice](sihf_api_web/player.md#sihf_player_time_on_ice) | Skater ice time by situation (even strength, power play, penalty kill), totals and per game, plus shifts. |

## Team

| Function | Summary |
|---|---|
| [sihf_team_goals](sihf_api_web/team.md#sihf_team_goals) | Team goals for and against per season: totals and per game, even strength, power play and short-handed. |
| [sihf_team_goals_by_position](sihf_api_web/team.md#sihf_team_goals_by_position) | Team goals by scorer: forwards, defensemen and import players (totals and shares), plus shootout-winning goals for and against. |
| [sihf_team_shots](sihf_api_web/team.md#sihf_team_shots) | Team shots per season: on goal (total, slot, shooting percentage), missed, post, blocked, and shots against, with per-game rates. |
| [sihf_team_power_play](sihf_api_web/team.md#sihf_team_power_play) | Team power play per season: opportunities, goals, conversion percentage, power-play time, and short-handed goals against. |
| [sihf_team_penalty_kill](sihf_api_web/team.md#sihf_team_penalty_kill) | Team penalty kill per season: times short-handed, goals against, kill percentage, penalty-kill time, and short-handed goals for. |
| [sihf_team_penalties](sihf_api_web/team.md#sihf_team_penalties) | Team penalties per season: 2-, 5- and 10-minute penalties, game misconducts, match penalties and penalty minutes. |
| [sihf_team_shootouts](sihf_api_web/team.md#sihf_team_shootouts) | Team shootouts per season: shootout wins and losses, attempts, goals and percentage, and attempts, goals against and saves. |
| [sihf_team_faceoffs](sihf_api_web/team.md#sihf_team_faceoffs) | Team faceoffs per season: taken, won, lost, per game, and win percentage overall and by zone. |
| [sihf_team_faceoffs_by_zone](sihf_api_web/team.md#sihf_team_faceoffs_by_zone) | Team faceoffs by zone (offensive, neutral, defensive) per season: taken, won, lost and win percentage. |
| [sihf_team_faceoffs_by_zone_per_game](sihf_api_web/team.md#sihf_team_faceoffs_by_zone_per_game) | Team faceoffs by zone (offensive, neutral, defensive) per game: taken, won and lost. |
| [sihf_team_attendance](sihf_api_web/team.md#sihf_team_attendance) | Team home attendance per season: home games, total and average spectators, average arena capacity and utilisation. |

## Other

| Function | Summary |
|---|---|
| [sihf_results](sihf_api_web/other.md#sihf_results) | Games of a league season: date, teams, score, per-period score, decision, status and game_id. |
| [sihf_standings](sihf_api_web/other.md#sihf_standings) | League table of a season (every game, or home / away games only): rank, games, points, regulation / overtime / shootout wins and losses, goals, streak. |
| [sihf_phases](sihf_api_web/other.md#sihf_phases) | Phases of a league season (regular season, play-in and playoff rounds, playout): the phase_id values the table wrappers take, with each phase's name and game … |
| [sihf_pbp](sihf_api_web/other.md#sihf_pbp) | Event timeline of one game: goals, penalties, goaltender changes, revoked goals and shootout attempts. |
