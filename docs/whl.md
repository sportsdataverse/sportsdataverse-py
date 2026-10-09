# WHL

> sdv-py WHL: endpoint references, dataset loaders and parsers for WHL in the SportsDataverse Python package.

# WHL (`sportsdataverse.whl`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [HockeyTech / LeagueStat](#hockeytech-leaguestat) | `lscluster.hockeytech.com` | 11 | per-league public key (SDV__API_KEY) |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 13 | — |

## HockeyTech / LeagueStat {#hockeytech-leaguestat}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional) | 11 |
## Tools and helpers

### Dates and seasons {#dates-and-seasons}

- [`most_recent_whl_season`](reference/additional#most_recent_whl_season)
- [`whl_season_id`](reference/additional#whl_season_id)

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
- [Junior & minor hockey tutorial](../tutorials/11_junior_hockey_intro.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`fastRhockey`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.whl` (Python) | `fastRhockey` (R) |
|---|---|
| [`most_recent_whl_season`](reference/additional#most_recent_whl_season) | [`most_recent_whl_season`](https://fastRhockey.sportsdataverse.org/reference/most_recent_whl_season.html) |
| [`whl_game_corsi`](reference/additional#whl_game_corsi) | [`whl_game_corsi`](https://fastRhockey.sportsdataverse.org/reference/whl_game_corsi.html) |
| [`whl_game_shifts`](reference/additional#whl_game_shifts) | [`whl_game_shifts`](https://fastRhockey.sportsdataverse.org/reference/whl_game_shifts.html) |
| [`whl_game_summary`](reference/additional#whl_game_summary) | [`whl_game_summary`](https://fastRhockey.sportsdataverse.org/reference/whl_game_summary.html) |
| [`whl_leaders`](reference/additional#whl_leaders) | [`whl_leaders`](https://fastRhockey.sportsdataverse.org/reference/whl_leaders.html) |
| [`whl_pbp`](reference/additional#whl_pbp) | [`whl_pbp`](https://fastRhockey.sportsdataverse.org/reference/whl_pbp.html) |
| [`whl_player_stats`](reference/additional#whl_player_stats) | [`whl_player_stats`](https://fastRhockey.sportsdataverse.org/reference/whl_player_stats.html) |
| [`whl_player_toi`](reference/additional#whl_player_toi) | [`whl_player_toi`](https://fastRhockey.sportsdataverse.org/reference/whl_player_toi.html) |
| [`whl_schedule`](reference/additional#whl_schedule) | [`whl_schedule`](https://fastRhockey.sportsdataverse.org/reference/whl_schedule.html) |
| [`whl_season_id`](reference/additional#whl_season_id) | [`whl_season_id`](https://fastRhockey.sportsdataverse.org/reference/whl_season_id.html) |
| [`whl_standings`](reference/additional#whl_standings) | [`whl_standings`](https://fastRhockey.sportsdataverse.org/reference/whl_standings.html) |
| [`whl_team_roster`](reference/additional#whl_team_roster) | [`whl_team_roster`](https://fastRhockey.sportsdataverse.org/reference/whl_team_roster.html) |
| [`whl_teams`](reference/additional#whl_teams) | [`whl_teams`](https://fastRhockey.sportsdataverse.org/reference/whl_teams.html) |
