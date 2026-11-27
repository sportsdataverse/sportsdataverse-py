# KHL

> sdv-py KHL: endpoint references, dataset loaders and parsers for KHL in the SportsDataverse Python package.

# KHL (`sportsdataverse.hockey.khl`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [KHL (site REST + webcaster API)](#khl-site-rest-webcaster-api) | `khl.api.webcaster.pro`, `www.khl.ru` | 25 | none |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 2 | — |

## KHL (site REST + webcaster API) {#khl-site-rest-webcaster-api}

| Reference | Functions |
|---|---:|
| [KHL site REST (www.khl.ru/rest)](reference/khl_rest) | 14 |
| [KHL webcaster mobile API (khl.api.webcaster.pro)](reference/khl_webcaster) | 9 |
| [Hand-written wrappers](reference/additional) | 2 |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`fastRhockey`](https://fastRhockey.sportsdataverse.org). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.hockey.khl` (Python) | `fastRhockey` (R) |
|---|---|
| [`khl_game_header`](reference/khl_rest#khl_game_header) | [`khl_game_header`](https://fastRhockey.sportsdataverse.org/reference/khl_game_header.html) |
| [`khl_game_info`](reference/khl_rest#khl_game_info) | [`khl_game_info`](https://fastRhockey.sportsdataverse.org/reference/khl_game_info.html) |
| [`khl_game_protocol`](reference/khl_rest#khl_game_protocol) | [`khl_game_protocol`](https://fastRhockey.sportsdataverse.org/reference/khl_game_protocol.html) |
| [`khl_pbp`](reference/khl_rest#khl_pbp) | [`khl_pbp`](https://fastRhockey.sportsdataverse.org/reference/khl_pbp.html) |
| [`khl_player_box`](reference/khl_rest#khl_player_box) | [`khl_player_box`](https://fastRhockey.sportsdataverse.org/reference/khl_player_box.html) |
| [`khl_player_career`](reference/khl_rest#khl_player_career) | [`khl_player_career`](https://fastRhockey.sportsdataverse.org/reference/khl_player_career.html) |
| [`khl_player_profile`](reference/khl_rest#khl_player_profile) | [`khl_player_profile`](https://fastRhockey.sportsdataverse.org/reference/khl_player_profile.html) |
| [`khl_player_stats`](reference/khl_rest#khl_player_stats) | [`khl_player_stats`](https://fastRhockey.sportsdataverse.org/reference/khl_player_stats.html) |
| [`khl_playoff_bracket`](reference/khl_rest#khl_playoff_bracket) | [`khl_playoff_bracket`](https://fastRhockey.sportsdataverse.org/reference/khl_playoff_bracket.html) |
| [`khl_roster`](reference/khl_rest#khl_roster) | [`khl_roster`](https://fastRhockey.sportsdataverse.org/reference/khl_roster.html) |
| [`khl_schedule`](reference/khl_rest#khl_schedule) | [`khl_schedule`](https://fastRhockey.sportsdataverse.org/reference/khl_schedule.html) |
| [`khl_shots`](reference/khl_rest#khl_shots) | [`khl_shots`](https://fastRhockey.sportsdataverse.org/reference/khl_shots.html) |
| [`khl_standings`](reference/khl_rest#khl_standings) | [`khl_standings`](https://fastRhockey.sportsdataverse.org/reference/khl_standings.html) |
| [`khl_stat_filters`](reference/khl_rest#khl_stat_filters) | [`khl_stat_filters`](https://fastRhockey.sportsdataverse.org/reference/khl_stat_filters.html) |
| [`khl_webcaster_event`](reference/khl_webcaster#khl_webcaster_event) | [`khl_webcaster_event`](https://fastRhockey.sportsdataverse.org/reference/khl_webcaster_event.html) |
| [`khl_webcaster_events`](reference/khl_webcaster#khl_webcaster_events) | [`khl_webcaster_events`](https://fastRhockey.sportsdataverse.org/reference/khl_webcaster_events.html) |
| [`khl_webcaster_leaders`](reference/khl_webcaster#khl_webcaster_leaders) | [`khl_webcaster_leaders`](https://fastRhockey.sportsdataverse.org/reference/khl_webcaster_leaders.html) |
| [`khl_webcaster_players`](reference/khl_webcaster#khl_webcaster_players) | [`khl_webcaster_players`](https://fastRhockey.sportsdataverse.org/reference/khl_webcaster_players.html) |
| [`khl_webcaster_players_light`](reference/khl_webcaster#khl_webcaster_players_light) | [`khl_webcaster_players_light`](https://fastRhockey.sportsdataverse.org/reference/khl_webcaster_players_light.html) |
| [`khl_webcaster_stages`](reference/khl_webcaster#khl_webcaster_stages) | [`khl_webcaster_stages`](https://fastRhockey.sportsdataverse.org/reference/khl_webcaster_stages.html) |
| [`khl_webcaster_tables`](reference/khl_webcaster#khl_webcaster_tables) | [`khl_webcaster_tables`](https://fastRhockey.sportsdataverse.org/reference/khl_webcaster_tables.html) |
| [`khl_webcaster_team`](reference/khl_webcaster#khl_webcaster_team) | [`khl_webcaster_team`](https://fastRhockey.sportsdataverse.org/reference/khl_webcaster_team.html) |
| [`khl_webcaster_teams`](reference/khl_webcaster#khl_webcaster_teams) | [`khl_webcaster_teams`](https://fastRhockey.sportsdataverse.org/reference/khl_webcaster_teams.html) |
