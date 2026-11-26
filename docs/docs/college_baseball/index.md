---
title: College baseball
sidebar_label: College baseball
description: "sdv-py College baseball: endpoint references, dataset loaders and parsers for College baseball in the SportsDataverse Python package."
---
# College baseball (`sportsdataverse.baseball.college_baseball`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [ESPN](#espn) | `cdn.espn.com`, `site.api.espn.com`, `site.web.api.espn.com` +1 more | 122 | none |
| [stats.ncaa.org](#stats-ncaa-org) | `stats.ncaa.org` | 3 | none (Terms gate + rate rotation) |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 4 | — |

## ESPN {#espn}

| Reference | Functions |
|---|---:|
| [ESPN site API (v2)](reference/site) | 25 |
| [ESPN web API (v3)](reference/web) | 5 |
| [ESPN core API (v2)](reference/core) | 87 |
| [ESPN FPI API (fitt v3)](reference/fitt) | 1 |
| [ESPN CDN API (cdn.espn.com)](reference/cdn) | 4 |

## stats.ncaa.org {#stats-ncaa-org}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional) | 3 |
## Tools and helpers

### Play-by-play processing {#play-by-play-processing}

- [`decompose_college_baseball_plays`](reference/additional#decompose_college_baseball_plays)


## See also

- [sdvplot](https://sdvplot.sportsdataverse.org/) — `surface("ncaa_baseball")` draws the field and `team_colors("ncaa_baseball", ...)` the school colors
- [sportypy](https://sportypy.sportsdataverse.org/) — the college baseball field sdvplot draws with

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`baseballr`](https://billpetti.github.io/baseballr). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.baseball.college_baseball` (Python) | `baseballr` (R) |
|---|---|
| [`espn_college_baseball_calendar`](reference/site#espn_college_baseball_calendar) | [`espn_college_baseball_calendar`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_calendar.html) |
| [`espn_college_baseball_coach`](reference/core/other#espn_college_baseball_coach) | [`espn_college_baseball_coach`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_coach.html) |
| [`espn_college_baseball_coach_record`](reference/core/other#espn_college_baseball_coach_record) | [`espn_college_baseball_coach_record`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_coach_record.html) |
| [`espn_college_baseball_coach_season`](reference/core/other#espn_college_baseball_coach_season) | [`espn_college_baseball_coach_season`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_coach_season.html) |
| [`espn_college_baseball_conferences`](reference/site#espn_college_baseball_conferences) | [`espn_college_baseball_conferences`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_conferences.html) |
| [`espn_college_baseball_game_broadcasts`](reference/core/game#espn_college_baseball_game_broadcasts) | [`espn_college_baseball_game_broadcasts`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_game_broadcasts.html) |
| [`espn_college_baseball_game_official_detail`](reference/core/game#espn_college_baseball_game_official_detail) | [`espn_college_baseball_game_official_detail`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_game_official_detail.html) |
| [`espn_college_baseball_game_officials`](reference/core/game#espn_college_baseball_game_officials) | [`espn_college_baseball_game_officials`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_game_officials.html) |
| [`espn_college_baseball_game_play`](reference/core/game#espn_college_baseball_game_play) | [`espn_college_baseball_game_play`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_game_play.html) |
| [`espn_college_baseball_game_play_personnel`](reference/core/game#espn_college_baseball_game_play_personnel) | [`espn_college_baseball_game_play_personnel`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_game_play_personnel.html) |
| [`espn_college_baseball_game_situation`](reference/core/game#espn_college_baseball_game_situation) | [`espn_college_baseball_game_situation`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_game_situation.html) |
| [`espn_college_baseball_game_team_leaders`](reference/core/game#espn_college_baseball_game_team_leaders) | [`espn_college_baseball_game_team_leaders`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_game_team_leaders.html) |
| [`espn_college_baseball_game_team_linescores`](reference/core/game#espn_college_baseball_game_team_linescores) | [`espn_college_baseball_game_team_linescores`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_game_team_linescores.html) |
| [`espn_college_baseball_game_team_roster`](reference/core/game#espn_college_baseball_game_team_roster) | [`espn_college_baseball_game_team_roster`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_game_team_roster.html) |
| [`espn_college_baseball_game_team_statistics`](reference/core/game#espn_college_baseball_game_team_statistics) | [`espn_college_baseball_game_team_statistics`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_game_team_statistics.html) |
| [`espn_college_baseball_leaders`](reference/web#espn_college_baseball_leaders) | [`espn_college_baseball_leaders`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_leaders.html) |
| [`espn_college_baseball_news`](reference/site#espn_college_baseball_news) | [`espn_college_baseball_news`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_news.html) |
| [`espn_college_baseball_player_awards`](reference/core/player#espn_college_baseball_player_awards) | [`espn_college_baseball_player_awards`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_player_awards.html) |
| [`espn_college_baseball_player_career_stats`](reference/core/player#espn_college_baseball_player_career_stats) | [`espn_college_baseball_player_career_stats`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_player_career_stats.html) |
| [`espn_college_baseball_player_eventlog`](reference/core/player#espn_college_baseball_player_eventlog) | [`espn_college_baseball_player_eventlog`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_player_eventlog.html) |
| [`espn_college_baseball_player_gamelog`](reference/web#espn_college_baseball_player_gamelog) | [`espn_college_baseball_player_gamelog`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_player_gamelog.html) |
| [`espn_college_baseball_player_info`](reference/site#espn_college_baseball_player_info) | [`espn_college_baseball_player_info`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_player_info.html) |
| [`espn_college_baseball_player_overview`](reference/web#espn_college_baseball_player_overview) | [`espn_college_baseball_player_overview`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_player_overview.html) |
| [`espn_college_baseball_player_seasons`](reference/core/player#espn_college_baseball_player_seasons) | [`espn_college_baseball_player_seasons`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_player_seasons.html) |
| [`espn_college_baseball_player_splits`](reference/web#espn_college_baseball_player_splits) | [`espn_college_baseball_player_splits`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_player_splits.html) |
| [`espn_college_baseball_player_statisticslog`](reference/core/player#espn_college_baseball_player_statisticslog) | [`espn_college_baseball_player_statisticslog`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_player_statisticslog.html) |
| [`espn_college_baseball_scoreboard`](reference/site#espn_college_baseball_scoreboard) | [`espn_college_baseball_scoreboard`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_scoreboard.html) |
| [`espn_college_baseball_season_group`](reference/core/season#espn_college_baseball_season_group) | [`espn_college_baseball_season_group`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_season_group.html) |
| [`espn_college_baseball_season_group_children`](reference/core/season#espn_college_baseball_season_group_children) | [`espn_college_baseball_season_group_children`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_season_group_children.html) |
| [`espn_college_baseball_season_group_teams`](reference/core/season#espn_college_baseball_season_group_teams) | [`espn_college_baseball_season_group_teams`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_season_group_teams.html) |
| [`espn_college_baseball_season_groups`](reference/core/season#espn_college_baseball_season_groups) | [`espn_college_baseball_season_groups`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_season_groups.html) |
| [`espn_college_baseball_season_info`](reference/core/season#espn_college_baseball_season_info) | [`espn_college_baseball_season_info`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_season_info.html) |
| [`espn_college_baseball_season_type`](reference/core/season#espn_college_baseball_season_type) | [`espn_college_baseball_season_type`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_season_type.html) |
| [`espn_college_baseball_season_types`](reference/core/season#espn_college_baseball_season_types) | [`espn_college_baseball_season_types`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_season_types.html) |
| [`espn_college_baseball_season_week`](reference/core/season#espn_college_baseball_season_week) | [`espn_college_baseball_season_week`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_season_week.html) |
| [`espn_college_baseball_season_weeks`](reference/core/season#espn_college_baseball_season_weeks) | [`espn_college_baseball_season_weeks`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_season_weeks.html) |
| [`espn_college_baseball_seasons`](reference/core/other#espn_college_baseball_seasons) | [`espn_college_baseball_seasons`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_seasons.html) |
| [`espn_college_baseball_standings`](reference/site#espn_college_baseball_standings) | [`espn_college_baseball_standings`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_standings.html) |
| [`espn_college_baseball_team`](reference/site#espn_college_baseball_team) | [`espn_college_baseball_team`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_team.html) |
| [`espn_college_baseball_team_leaders`](reference/site#espn_college_baseball_team_leaders) | [`espn_college_baseball_team_leaders`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_team_leaders.html) |
| [`espn_college_baseball_team_news`](reference/site#espn_college_baseball_team_news) | [`espn_college_baseball_team_news`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_team_news.html) |
| [`espn_college_baseball_team_record`](reference/site#espn_college_baseball_team_record) | [`espn_college_baseball_team_record`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_team_record.html) |
| [`espn_college_baseball_team_roster`](reference/site#espn_college_baseball_team_roster) | [`espn_college_baseball_team_roster`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_team_roster.html) |
| [`espn_college_baseball_team_schedule`](reference/site#espn_college_baseball_team_schedule) | [`espn_college_baseball_team_schedule`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_team_schedule.html) |
| [`espn_college_baseball_tournaments`](reference/core/other#espn_college_baseball_tournaments) | [`espn_college_baseball_tournaments`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_tournaments.html) |
| [`espn_college_baseball_venues`](reference/core/other#espn_college_baseball_venues) | [`espn_college_baseball_venues`](https://billpetti.github.io/baseballr/reference/espn_college_baseball_venues.html) |
