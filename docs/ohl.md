# OHL

> sdv-py OHL: endpoint references, dataset loaders and parsers for OHL in the SportsDataverse Python package.

# OHL (`sportsdataverse.ohl`)

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

- [`most_recent_ohl_season`](reference/additional#most_recent_ohl_season)
- [`ohl_season_id`](reference/additional#ohl_season_id)

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
- [Junior & minor hockey tutorial](../tutorials/11_junior_hockey_intro.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`fastRhockey`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.ohl` (Python) | `fastRhockey` (R) |
|---|---|
| [`most_recent_ohl_season`](reference/additional#most_recent_ohl_season) | [`most_recent_ohl_season`](https://fastRhockey.sportsdataverse.org/reference/most_recent_ohl_season.html) |
| [`ohl_game_corsi`](reference/additional#ohl_game_corsi) | [`ohl_game_corsi`](https://fastRhockey.sportsdataverse.org/reference/ohl_game_corsi.html) |
| [`ohl_game_shifts`](reference/additional#ohl_game_shifts) | [`ohl_game_shifts`](https://fastRhockey.sportsdataverse.org/reference/ohl_game_shifts.html) |
| [`ohl_game_summary`](reference/additional#ohl_game_summary) | [`ohl_game_summary`](https://fastRhockey.sportsdataverse.org/reference/ohl_game_summary.html) |
| [`ohl_leaders`](reference/additional#ohl_leaders) | [`ohl_leaders`](https://fastRhockey.sportsdataverse.org/reference/ohl_leaders.html) |
| [`ohl_pbp`](reference/additional#ohl_pbp) | [`ohl_pbp`](https://fastRhockey.sportsdataverse.org/reference/ohl_pbp.html) |
| [`ohl_player_stats`](reference/additional#ohl_player_stats) | [`ohl_player_stats`](https://fastRhockey.sportsdataverse.org/reference/ohl_player_stats.html) |
| [`ohl_player_toi`](reference/additional#ohl_player_toi) | [`ohl_player_toi`](https://fastRhockey.sportsdataverse.org/reference/ohl_player_toi.html) |
| [`ohl_schedule`](reference/additional#ohl_schedule) | [`ohl_schedule`](https://fastRhockey.sportsdataverse.org/reference/ohl_schedule.html) |
| [`ohl_season_id`](reference/additional#ohl_season_id) | [`ohl_season_id`](https://fastRhockey.sportsdataverse.org/reference/ohl_season_id.html) |
| [`ohl_standings`](reference/additional#ohl_standings) | [`ohl_standings`](https://fastRhockey.sportsdataverse.org/reference/ohl_standings.html) |
| [`ohl_team_roster`](reference/additional#ohl_team_roster) | [`ohl_team_roster`](https://fastRhockey.sportsdataverse.org/reference/ohl_team_roster.html) |
| [`ohl_teams`](reference/additional#ohl_teams) | [`ohl_teams`](https://fastRhockey.sportsdataverse.org/reference/ohl_teams.html) |
