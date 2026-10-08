---
title: QMJHL
sidebar_label: QMJHL
description: "sdv-py QMJHL: endpoint references, dataset loaders and parsers for QMJHL in the SportsDataverse Python package."
---
# QMJHL (`sportsdataverse.qmjhl`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [HockeyTech / LeagueStat](#hockeytech-leaguestat) | `lscluster.hockeytech.com` | 11 | per-league public key (SDV_<LEAGUE>_API_KEY) |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 13 | — |

## HockeyTech / LeagueStat {#hockeytech-leaguestat}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional) | 11 |
## Tools and helpers

### Dates and seasons {#dates-and-seasons}

- [`most_recent_qmjhl_season`](reference/additional#most_recent_qmjhl_season)
- [`qmjhl_season_id`](reference/additional#qmjhl_season_id)


## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
- [Junior & minor hockey tutorial](../tutorials/11_junior_hockey_intro.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`fastRhockey`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.qmjhl` (Python) | `fastRhockey` (R) |
|---|---|
| [`most_recent_qmjhl_season`](reference/additional#most_recent_qmjhl_season) | [`most_recent_qmjhl_season`](https://fastRhockey.sportsdataverse.org/reference/most_recent_qmjhl_season.html) |
| [`qmjhl_game_corsi`](reference/additional#qmjhl_game_corsi) | [`qmjhl_game_corsi`](https://fastRhockey.sportsdataverse.org/reference/qmjhl_game_corsi.html) |
| [`qmjhl_game_shifts`](reference/additional#qmjhl_game_shifts) | [`qmjhl_game_shifts`](https://fastRhockey.sportsdataverse.org/reference/qmjhl_game_shifts.html) |
| [`qmjhl_game_summary`](reference/additional#qmjhl_game_summary) | [`qmjhl_game_summary`](https://fastRhockey.sportsdataverse.org/reference/qmjhl_game_summary.html) |
| [`qmjhl_leaders`](reference/additional#qmjhl_leaders) | [`qmjhl_leaders`](https://fastRhockey.sportsdataverse.org/reference/qmjhl_leaders.html) |
| [`qmjhl_pbp`](reference/additional#qmjhl_pbp) | [`qmjhl_pbp`](https://fastRhockey.sportsdataverse.org/reference/qmjhl_pbp.html) |
| [`qmjhl_player_stats`](reference/additional#qmjhl_player_stats) | [`qmjhl_player_stats`](https://fastRhockey.sportsdataverse.org/reference/qmjhl_player_stats.html) |
| [`qmjhl_player_toi`](reference/additional#qmjhl_player_toi) | [`qmjhl_player_toi`](https://fastRhockey.sportsdataverse.org/reference/qmjhl_player_toi.html) |
| [`qmjhl_schedule`](reference/additional#qmjhl_schedule) | [`qmjhl_schedule`](https://fastRhockey.sportsdataverse.org/reference/qmjhl_schedule.html) |
| [`qmjhl_season_id`](reference/additional#qmjhl_season_id) | [`qmjhl_season_id`](https://fastRhockey.sportsdataverse.org/reference/qmjhl_season_id.html) |
| [`qmjhl_standings`](reference/additional#qmjhl_standings) | [`qmjhl_standings`](https://fastRhockey.sportsdataverse.org/reference/qmjhl_standings.html) |
| [`qmjhl_team_roster`](reference/additional#qmjhl_team_roster) | [`qmjhl_team_roster`](https://fastRhockey.sportsdataverse.org/reference/qmjhl_team_roster.html) |
| [`qmjhl_teams`](reference/additional#qmjhl_teams) | [`qmjhl_teams`](https://fastRhockey.sportsdataverse.org/reference/qmjhl_teams.html) |
