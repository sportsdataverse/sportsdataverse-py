---
title: BLEAGUE — B.LEAGUE game contexts (S3) and pages (www.bleague.jp, Japan)
sidebar_label: B.LEAGUE game contexts (S3) and pages (www.bleague.jp, Japan)
description: "BLEAGUE — B.LEAGUE game contexts (S3) and pages (www.bleague.jp, Japan) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# BLEAGUE — B.LEAGUE game contexts (S3) and pages (www.bleague.jp, Japan)

`sportsdataverse.basketball.bleague` — 14 endpoints.

> **Note:** Unofficial and unsupported: the game contexts are JSON objects in B.LEAGUE's own public S3 bucket (b-league.s3.amazonaws.com, the store its game pages read), and schedule / standings / game_detail are pages of www.bleague.jp parsed here. Neither host publishes terms for this use (the site's /terms/ answers 404; robots.txt sets a Crawl-delay only for msnbot and bingbot), and `bleague_runtime` spaces requests at least 1 s apart. SportsDataverse has not ruled on the site's terms, so payloads are not redistributed as release assets.

> **Note:** Seasons: the site's `Year` is the season START year; every wrapper takes `season`, the END year (2026 = 2025-26), and `bleague_runtime` sends `season - 1`. Game wrappers take the global `schedule_key` (one integer sequence across B1, B2, B3 and every season: 1 = the 2016-17 opener, 5856 in 2020-21, 500017 in 2022-23, 504734 / 505508 B1 / B2 and 10693 B3 in 2025-26); list a day's keys with `bleague_schedule`. The runtime reads the feed at run time: v2 (B1/B2 from 2020-21), then v1 (B3 and B1/B2 2018-2020), then the game page's embedded copy (the only source of 2016-17 and 2017-18).

> **Note:** Coverage: game wrappers serve FINISHED games only (an unfinished game raises AssetFetchError; 2019-20 game 4153 is an unfinished feed). The two feeds differ: v2 ids are strings, v1 ids integers (both become text here); v1 adds leaders and the score track; the team-rebound codes 18 / 19 are swapped between them and a v1 substitution is one row where v2 has two (see `bleague_pbp`). Shot x / y exist from 2022-23 on v2 only, `side` from 2025-26; before that `bleague_shots` is empty. Player and team names are Japanese (English names only on the box score and header); the code tables are in `bleague_codes`.

## Schedule

| Function | Summary |
|---|---|
| [bleague_schedule](bleague/schedule.md#bleague_schedule) | Games of one day of one competition and division: schedule keys, clubs, scores, venue, tip-off. |
| [bleague_schedule_clubs](bleague/schedule.md#bleague_schedule_clubs) | Clubs of the season and division from the schedule page's club filter, with their team ids. |
| [bleague_schedule_events](bleague/schedule.md#bleague_schedule_events) | Competitions the schedule page offers for the season and division. |
| [bleague_schedule_days](bleague/schedule.md#bleague_schedule_days) | Game days of the requested month for the competition. |

## Team

| Function | Summary |
|---|---|
| [bleague_team_box](bleague/team.md#bleague_team_box) | Team rows of the box score (team-credited stats and team totals) per period category. |
| [bleague_team_summary](bleague/team.md#bleague_team_summary) | Team summary per period category in long form: shooting, rebounds, paint, fast-break and second-chance points. |

## Other

| Function | Summary |
|---|---|
| [bleague_game_info](bleague/other.md#bleague_game_info) | Game header: competition, round, venue, officials, tip-off, both teams and the scores. |
| [bleague_period_scores](bleague/other.md#bleague_period_scores) | Points each team scored in each period of a game, overtime included. |
| [bleague_player_box](bleague/other.md#bleague_player_box) | Player box score per period category: minutes, points, shooting, rebounds, assists, efficiency. |
| [bleague_pbp](bleague/other.md#bleague_pbp) | Play-by-play of a finished game in game order, with decoded action codes and x / y from 2022-23. |
| [bleague_shots](bleague/other.md#bleague_shots) | Field-goal attempts with court position (2022-23 onward, v2 feed). |
| [bleague_leaders](bleague/other.md#bleague_leaders) | Team leaders per statistic and period category (v1 feed only). |
| [bleague_score_track](bleague/other.md#bleague_score_track) | Running score after each scoring event (v1 feed only). |
| [bleague_standings](bleague/other.md#bleague_standings) | Standings of one season and division, per conference / division table and wild card. |
