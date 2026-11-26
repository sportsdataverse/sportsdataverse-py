---
title: B.LEAGUE
sidebar_label: B.LEAGUE
description: "sdv-py B.LEAGUE: endpoint references, dataset loaders and parsers for B.LEAGUE in the SportsDataverse Python package."
---
# B.LEAGUE (`sportsdataverse.basketball.bleague`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [B.LEAGUE (Japan)](#b-league-japan) | `b-league.s3.amazonaws.com`, `www.bleague.jp` | 14 | none |

## B.LEAGUE (Japan) {#b-league-japan}

| Reference | Functions |
|---|---:|
| [B.LEAGUE game contexts (S3) and pages (www.bleague.jp, Japan)](reference/bleague) | 14 |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`hoopR`](https://hoopR.sportsdataverse.org). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.basketball.bleague` (Python) | `hoopR` (R) |
|---|---|
| [`bleague_game_info`](reference/bleague/other#bleague_game_info) | [`bleague_game_info`](https://hoopR.sportsdataverse.org/reference/bleague_game_info.html) |
| [`bleague_leaders`](reference/bleague/other#bleague_leaders) | [`bleague_leaders`](https://hoopR.sportsdataverse.org/reference/bleague_leaders.html) |
| [`bleague_pbp`](reference/bleague/other#bleague_pbp) | [`bleague_pbp`](https://hoopR.sportsdataverse.org/reference/bleague_pbp.html) |
| [`bleague_period_scores`](reference/bleague/other#bleague_period_scores) | [`bleague_period_scores`](https://hoopR.sportsdataverse.org/reference/bleague_period_scores.html) |
| [`bleague_player_box`](reference/bleague/other#bleague_player_box) | [`bleague_player_box`](https://hoopR.sportsdataverse.org/reference/bleague_player_box.html) |
| [`bleague_schedule`](reference/bleague/schedule#bleague_schedule) | [`bleague_schedule`](https://hoopR.sportsdataverse.org/reference/bleague_schedule.html) |
| [`bleague_schedule_clubs`](reference/bleague/schedule#bleague_schedule_clubs) | [`bleague_schedule_clubs`](https://hoopR.sportsdataverse.org/reference/bleague_schedule_clubs.html) |
| [`bleague_schedule_days`](reference/bleague/schedule#bleague_schedule_days) | [`bleague_schedule_days`](https://hoopR.sportsdataverse.org/reference/bleague_schedule_days.html) |
| [`bleague_schedule_events`](reference/bleague/schedule#bleague_schedule_events) | [`bleague_schedule_events`](https://hoopR.sportsdataverse.org/reference/bleague_schedule_events.html) |
| [`bleague_score_track`](reference/bleague/other#bleague_score_track) | [`bleague_score_track`](https://hoopR.sportsdataverse.org/reference/bleague_score_track.html) |
| [`bleague_shots`](reference/bleague/other#bleague_shots) | [`bleague_shots`](https://hoopR.sportsdataverse.org/reference/bleague_shots.html) |
| [`bleague_standings`](reference/bleague/other#bleague_standings) | [`bleague_standings`](https://hoopR.sportsdataverse.org/reference/bleague_standings.html) |
| [`bleague_team_box`](reference/bleague/team#bleague_team_box) | [`bleague_team_box`](https://hoopR.sportsdataverse.org/reference/bleague_team_box.html) |
| [`bleague_team_summary`](reference/bleague/team#bleague_team_summary) | [`bleague_team_summary`](https://hoopR.sportsdataverse.org/reference/bleague_team_summary.html) |
