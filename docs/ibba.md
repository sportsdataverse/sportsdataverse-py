# Israeli Premier League

> sdv-py Israeli Premier League: endpoint references, dataset loaders and parsers for Israeli Premier League in the SportsDataverse Python package.

# Israeli Premier League (`sportsdataverse.basketball.ibba`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [IBBA (Israeli Premier League) and Segev Stats](#ibba-israeli-premier-league-and-segev-stats) | `basket.co.il`, `stats.segevstats.com` | 22 | none |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 1 | — |

## IBBA (Israeli Premier League) and Segev Stats {#ibba-israeli-premier-league-and-segev-stats}

| Reference | Functions |
|---|---:|
| [IBBA ASMX web service (basket.co.il, Israel)](reference/ibba) | 16 |
| [Segev Stats live-stats API (stats.segevstats.com, Israel play-by-play)](reference/ibba_segev) | 6 |
## Tools and helpers

### IDs and crosswalks {#ids-and-crosswalks}

- [`ibba_segev_game_crosswalk`](reference/additional#ibba_segev_game_crosswalk)

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`hoopR`](https://hoopR.sportsdataverse.org). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.basketball.ibba` (Python) | `hoopR` (R) |
|---|---|
| [`ibba_boards`](reference/ibba#ibba_boards) | [`ibba_boards`](https://hoopR.sportsdataverse.org/reference/ibba_boards.html) |
| [`ibba_boards_ex`](reference/ibba#ibba_boards_ex) | [`ibba_boards_ex`](https://hoopR.sportsdataverse.org/reference/ibba_boards_ex.html) |
| [`ibba_box_score`](reference/ibba#ibba_box_score) | [`ibba_box_score`](https://hoopR.sportsdataverse.org/reference/ibba_box_score.html) |
| [`ibba_coaches`](reference/ibba#ibba_coaches) | [`ibba_coaches`](https://hoopR.sportsdataverse.org/reference/ibba_coaches.html) |
| [`ibba_configuration`](reference/ibba#ibba_configuration) | [`ibba_configuration`](https://hoopR.sportsdataverse.org/reference/ibba_configuration.html) |
| [`ibba_games`](reference/ibba#ibba_games) | [`ibba_games`](https://hoopR.sportsdataverse.org/reference/ibba_games.html) |
| [`ibba_games_by_team`](reference/ibba#ibba_games_by_team) | [`ibba_games_by_team`](https://hoopR.sportsdataverse.org/reference/ibba_games_by_team.html) |
| [`ibba_games_ex`](reference/ibba#ibba_games_ex) | [`ibba_games_ex`](https://hoopR.sportsdataverse.org/reference/ibba_games_ex.html) |
| [`ibba_leaders`](reference/ibba#ibba_leaders) | [`ibba_leaders`](https://hoopR.sportsdataverse.org/reference/ibba_leaders.html) |
| [`ibba_player`](reference/ibba#ibba_player) | [`ibba_player`](https://hoopR.sportsdataverse.org/reference/ibba_player.html) |
| [`ibba_player_stats`](reference/ibba#ibba_player_stats) | [`ibba_player_stats`](https://hoopR.sportsdataverse.org/reference/ibba_player_stats.html) |
| [`ibba_players`](reference/ibba#ibba_players) | [`ibba_players`](https://hoopR.sportsdataverse.org/reference/ibba_players.html) |
| [`ibba_segev_game_crosswalk`](reference/additional#ibba_segev_game_crosswalk) | [`ibba_segev_game_crosswalk`](https://hoopR.sportsdataverse.org/reference/ibba_segev_game_crosswalk.html) |
| [`ibba_segev_game_info`](reference/ibba_segev#ibba_segev_game_info) | [`ibba_segev_game_info`](https://hoopR.sportsdataverse.org/reference/ibba_segev_game_info.html) |
| [`ibba_segev_pbp`](reference/ibba_segev#ibba_segev_pbp) | [`ibba_segev_pbp`](https://hoopR.sportsdataverse.org/reference/ibba_segev_pbp.html) |
| [`ibba_segev_period_scores`](reference/ibba_segev#ibba_segev_period_scores) | [`ibba_segev_period_scores`](https://hoopR.sportsdataverse.org/reference/ibba_segev_period_scores.html) |
| [`ibba_segev_player_box`](reference/ibba_segev#ibba_segev_player_box) | [`ibba_segev_player_box`](https://hoopR.sportsdataverse.org/reference/ibba_segev_player_box.html) |
| [`ibba_segev_rosters`](reference/ibba_segev#ibba_segev_rosters) | [`ibba_segev_rosters`](https://hoopR.sportsdataverse.org/reference/ibba_segev_rosters.html) |
| [`ibba_segev_team_box`](reference/ibba_segev#ibba_segev_team_box) | [`ibba_segev_team_box`](https://hoopR.sportsdataverse.org/reference/ibba_segev_team_box.html) |
| [`ibba_stats_types`](reference/ibba#ibba_stats_types) | [`ibba_stats_types`](https://hoopR.sportsdataverse.org/reference/ibba_stats_types.html) |
| [`ibba_team`](reference/ibba#ibba_team) | [`ibba_team`](https://hoopR.sportsdataverse.org/reference/ibba_team.html) |
| [`ibba_team_stats`](reference/ibba#ibba_team_stats) | [`ibba_team_stats`](https://hoopR.sportsdataverse.org/reference/ibba_team_stats.html) |
| [`ibba_teams`](reference/ibba#ibba_teams) | [`ibba_teams`](https://hoopR.sportsdataverse.org/reference/ibba_teams.html) |
