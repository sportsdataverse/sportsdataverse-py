# Liiga

> sdv-py Liiga: endpoint references, dataset loaders and parsers for Liiga in the SportsDataverse Python package.

# Liiga (`sportsdataverse.hockey.liiga`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [Liiga API](#liiga-api) | `liiga.fi` | 23 | none |

## Liiga API {#liiga-api}

| Reference | Functions |
|---|---:|
| [Liiga API v2 (liiga.fi/api/v2, Finland)](reference/liiga) | 23 |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`fastRhockey`](https://fastRhockey.sportsdataverse.org). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.hockey.liiga` (Python) | `fastRhockey` (R) |
|---|---|
| [`liiga_game`](reference/liiga/other#liiga_game) | [`liiga_game`](https://fastRhockey.sportsdataverse.org/reference/liiga_game.html) |
| [`liiga_game_goals`](reference/liiga/game#liiga_game_goals) | [`liiga_game_goals`](https://fastRhockey.sportsdataverse.org/reference/liiga_game_goals.html) |
| [`liiga_game_lineups`](reference/liiga/game#liiga_game_lineups) | [`liiga_game_lineups`](https://fastRhockey.sportsdataverse.org/reference/liiga_game_lineups.html) |
| [`liiga_game_penalties`](reference/liiga/game#liiga_game_penalties) | [`liiga_game_penalties`](https://fastRhockey.sportsdataverse.org/reference/liiga_game_penalties.html) |
| [`liiga_game_players_stats`](reference/liiga/game#liiga_game_players_stats) | [`liiga_game_players_stats`](https://fastRhockey.sportsdataverse.org/reference/liiga_game_players_stats.html) |
| [`liiga_game_shootout`](reference/liiga/game#liiga_game_shootout) | [`liiga_game_shootout`](https://fastRhockey.sportsdataverse.org/reference/liiga_game_shootout.html) |
| [`liiga_game_stats`](reference/liiga/game#liiga_game_stats) | [`liiga_game_stats`](https://fastRhockey.sportsdataverse.org/reference/liiga_game_stats.html) |
| [`liiga_game_team_stats`](reference/liiga/game#liiga_game_team_stats) | [`liiga_game_team_stats`](https://fastRhockey.sportsdataverse.org/reference/liiga_game_team_stats.html) |
| [`liiga_games`](reference/liiga/other#liiga_games) | [`liiga_games`](https://fastRhockey.sportsdataverse.org/reference/liiga_games.html) |
| [`liiga_gameweeks`](reference/liiga/other#liiga_gameweeks) | [`liiga_gameweeks`](https://fastRhockey.sportsdataverse.org/reference/liiga_gameweeks.html) |
| [`liiga_goals`](reference/liiga/other#liiga_goals) | [`liiga_goals`](https://fastRhockey.sportsdataverse.org/reference/liiga_goals.html) |
| [`liiga_player`](reference/liiga/player#liiga_player) | [`liiga_player`](https://fastRhockey.sportsdataverse.org/reference/liiga_player.html) |
| [`liiga_player_career`](reference/liiga/player#liiga_player_career) | [`liiga_player_career`](https://fastRhockey.sportsdataverse.org/reference/liiga_player_career.html) |
| [`liiga_player_game_log`](reference/liiga/player#liiga_player_game_log) | [`liiga_player_game_log`](https://fastRhockey.sportsdataverse.org/reference/liiga_player_game_log.html) |
| [`liiga_player_stats`](reference/liiga/player#liiga_player_stats) | [`liiga_player_stats`](https://fastRhockey.sportsdataverse.org/reference/liiga_player_stats.html) |
| [`liiga_players_info`](reference/liiga/player#liiga_players_info) | [`liiga_players_info`](https://fastRhockey.sportsdataverse.org/reference/liiga_players_info.html) |
| [`liiga_schedule`](reference/liiga/other#liiga_schedule) | [`liiga_schedule`](https://fastRhockey.sportsdataverse.org/reference/liiga_schedule.html) |
| [`liiga_shotmap`](reference/liiga/other#liiga_shotmap) | [`liiga_shotmap`](https://fastRhockey.sportsdataverse.org/reference/liiga_shotmap.html) |
| [`liiga_standings`](reference/liiga/other#liiga_standings) | [`liiga_standings`](https://fastRhockey.sportsdataverse.org/reference/liiga_standings.html) |
| [`liiga_team_stats`](reference/liiga/team#liiga_team_stats) | [`liiga_team_stats`](https://fastRhockey.sportsdataverse.org/reference/liiga_team_stats.html) |
| [`liiga_teams`](reference/liiga/team#liiga_teams) | [`liiga_teams`](https://fastRhockey.sportsdataverse.org/reference/liiga_teams.html) |
| [`liiga_teams_history`](reference/liiga/team#liiga_teams_history) | [`liiga_teams_history`](https://fastRhockey.sportsdataverse.org/reference/liiga_teams_history.html) |
| [`liiga_tournament`](reference/liiga/other#liiga_tournament) | [`liiga_tournament`](https://fastRhockey.sportsdataverse.org/reference/liiga_tournament.html) |
