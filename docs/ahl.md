# AHL

> sdv-py AHL: endpoint references, dataset loaders and parsers for AHL in the SportsDataverse Python package.

# AHL (`sportsdataverse.ahl`)

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

- [`ahl_season_id`](reference/additional#ahl_season_id)
- [`most_recent_ahl_season`](reference/additional#most_recent_ahl_season)

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
- [Junior & minor hockey tutorial](../tutorials/11_junior_hockey_intro.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`fastRhockey`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.ahl` (Python) | `fastRhockey` (R) |
|---|---|
| [`ahl_game_corsi`](reference/additional#ahl_game_corsi) | [`ahl_game_corsi`](https://fastRhockey.sportsdataverse.org/reference/ahl_game_corsi.html) |
| [`ahl_game_shifts`](reference/additional#ahl_game_shifts) | [`ahl_game_shifts`](https://fastRhockey.sportsdataverse.org/reference/ahl_game_shifts.html) |
| [`ahl_game_summary`](reference/additional#ahl_game_summary) | [`ahl_game_summary`](https://fastRhockey.sportsdataverse.org/reference/ahl_game_summary.html) |
| [`ahl_leaders`](reference/additional#ahl_leaders) | [`ahl_leaders`](https://fastRhockey.sportsdataverse.org/reference/ahl_leaders.html) |
| [`ahl_pbp`](reference/additional#ahl_pbp) | [`ahl_pbp`](https://fastRhockey.sportsdataverse.org/reference/ahl_pbp.html) |
| [`ahl_player_stats`](reference/additional#ahl_player_stats) | [`ahl_player_stats`](https://fastRhockey.sportsdataverse.org/reference/ahl_player_stats.html) |
| [`ahl_player_toi`](reference/additional#ahl_player_toi) | [`ahl_player_toi`](https://fastRhockey.sportsdataverse.org/reference/ahl_player_toi.html) |
| [`ahl_schedule`](reference/additional#ahl_schedule) | [`ahl_schedule`](https://fastRhockey.sportsdataverse.org/reference/ahl_schedule.html) |
| [`ahl_season_id`](reference/additional#ahl_season_id) | [`ahl_season_id`](https://fastRhockey.sportsdataverse.org/reference/ahl_season_id.html) |
| [`ahl_standings`](reference/additional#ahl_standings) | [`ahl_standings`](https://fastRhockey.sportsdataverse.org/reference/ahl_standings.html) |
| [`ahl_team_roster`](reference/additional#ahl_team_roster) | [`ahl_team_roster`](https://fastRhockey.sportsdataverse.org/reference/ahl_team_roster.html) |
| [`ahl_teams`](reference/additional#ahl_teams) | [`ahl_teams`](https://fastRhockey.sportsdataverse.org/reference/ahl_teams.html) |
| [`most_recent_ahl_season`](reference/additional#most_recent_ahl_season) | [`most_recent_ahl_season`](https://fastRhockey.sportsdataverse.org/reference/most_recent_ahl_season.html) |
