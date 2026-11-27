# SHL

> sdv-py SHL: endpoint references, dataset loaders and parsers for SHL in the SportsDataverse Python package.

# SHL (`sportsdataverse.hockey.shl`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [SHL site API](#shl-site-api) | `www.shl.se` | 29 | none |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 2 | — |

## SHL site API {#shl-site-api}

| Reference | Functions |
|---|---:|
| [SHL site API (www.shl.se/api, Sweden)](reference/shl) | 27 |
| [Hand-written wrappers](reference/additional) | 2 |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`fastRhockey`](https://fastRhockey.sportsdataverse.org). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.hockey.shl` (Python) | `fastRhockey` (R) |
|---|---|
| [`shl_athlete_career`](reference/shl/athlete#shl_athlete_career) | [`shl_athlete_career`](https://fastRhockey.sportsdataverse.org/reference/shl_athlete_career.html) |
| [`shl_athlete_details`](reference/shl/athlete#shl_athlete_details) | [`shl_athlete_details`](https://fastRhockey.sportsdataverse.org/reference/shl_athlete_details.html) |
| [`shl_athlete_game_log`](reference/shl/athlete#shl_athlete_game_log) | [`shl_athlete_game_log`](https://fastRhockey.sportsdataverse.org/reference/shl_athlete_game_log.html) |
| [`shl_athlete_profile`](reference/shl/athlete#shl_athlete_profile) | [`shl_athlete_profile`](https://fastRhockey.sportsdataverse.org/reference/shl_athlete_profile.html) |
| [`shl_athlete_seasons`](reference/shl/athlete#shl_athlete_seasons) | [`shl_athlete_seasons`](https://fastRhockey.sportsdataverse.org/reference/shl_athlete_seasons.html) |
| [`shl_boxscore`](reference/shl/other#shl_boxscore) | [`shl_boxscore`](https://fastRhockey.sportsdataverse.org/reference/shl_boxscore.html) |
| [`shl_game_header`](reference/shl/game#shl_game_header) | [`shl_game_header`](https://fastRhockey.sportsdataverse.org/reference/shl_game_header.html) |
| [`shl_game_overview`](reference/shl/game#shl_game_overview) | [`shl_game_overview`](https://fastRhockey.sportsdataverse.org/reference/shl_game_overview.html) |
| [`shl_game_summary`](reference/shl/game#shl_game_summary) | [`shl_game_summary`](https://fastRhockey.sportsdataverse.org/reference/shl_game_summary.html) |
| [`shl_last_game_day`](reference/shl/other#shl_last_game_day) | [`shl_last_game_day`](https://fastRhockey.sportsdataverse.org/reference/shl_last_game_day.html) |
| [`shl_layout_info`](reference/shl/other#shl_layout_info) | [`shl_layout_info`](https://fastRhockey.sportsdataverse.org/reference/shl_layout_info.html) |
| [`shl_next_game_day`](reference/shl/other#shl_next_game_day) | [`shl_next_game_day`](https://fastRhockey.sportsdataverse.org/reference/shl_next_game_day.html) |
| [`shl_pbp`](reference/shl/other#shl_pbp) | [`shl_pbp`](https://fastRhockey.sportsdataverse.org/reference/shl_pbp.html) |
| [`shl_player_box`](reference/shl/other#shl_player_box) | [`shl_player_box`](https://fastRhockey.sportsdataverse.org/reference/shl_player_box.html) |
| [`shl_player_crosswalk`](reference/additional#shl_player_crosswalk) | [`shl_player_crosswalk`](https://fastRhockey.sportsdataverse.org/reference/shl_player_crosswalk.html) |
| [`shl_player_h2h`](reference/shl/other#shl_player_h2h) | [`shl_player_h2h`](https://fastRhockey.sportsdataverse.org/reference/shl_player_h2h.html) |
| [`shl_post_game_team_stats`](reference/shl/other#shl_post_game_team_stats) | [`shl_post_game_team_stats`](https://fastRhockey.sportsdataverse.org/reference/shl_post_game_team_stats.html) |
| [`shl_roster`](reference/shl/other#shl_roster) | [`shl_roster`](https://fastRhockey.sportsdataverse.org/reference/shl_roster.html) |
| [`shl_schedule`](reference/shl/other#shl_schedule) | [`shl_schedule`](https://fastRhockey.sportsdataverse.org/reference/shl_schedule.html) |
| [`shl_season_filter`](reference/shl/other#shl_season_filter) | [`shl_season_filter`](https://fastRhockey.sportsdataverse.org/reference/shl_season_filter.html) |
| [`shl_sites`](reference/shl/other#shl_sites) | [`shl_sites`](https://fastRhockey.sportsdataverse.org/reference/shl_sites.html) |
| [`shl_ssgt_provider`](reference/shl/other#shl_ssgt_provider) | [`shl_ssgt_provider`](https://fastRhockey.sportsdataverse.org/reference/shl_ssgt_provider.html) |
| [`shl_ssgt_uuid`](reference/additional#shl_ssgt_uuid) | [`shl_ssgt_uuid`](https://fastRhockey.sportsdataverse.org/reference/shl_ssgt_uuid.html) |
| [`shl_standings`](reference/shl/other-2#shl_standings) | [`shl_standings`](https://fastRhockey.sportsdataverse.org/reference/shl_standings.html) |
| [`shl_stats_info`](reference/shl/other-2#shl_stats_info) | [`shl_stats_info`](https://fastRhockey.sportsdataverse.org/reference/shl_stats_info.html) |
| [`shl_team`](reference/shl/team#shl_team) | [`shl_team`](https://fastRhockey.sportsdataverse.org/reference/shl_team.html) |
| [`shl_team_played_games`](reference/shl/team#shl_team_played_games) | [`shl_team_played_games`](https://fastRhockey.sportsdataverse.org/reference/shl_team_played_games.html) |
| [`shl_team_stats`](reference/shl/team#shl_team_stats) | [`shl_team_stats`](https://fastRhockey.sportsdataverse.org/reference/shl_team_stats.html) |
| [`shl_team_upcoming_games`](reference/shl/team#shl_team_upcoming_games) | [`shl_team_upcoming_games`](https://fastRhockey.sportsdataverse.org/reference/shl_team_upcoming_games.html) |
