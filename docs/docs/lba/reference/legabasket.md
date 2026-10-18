---
title: LBA — Lega Basket Serie A site API (www.legabasket.it/api, Italy)
sidebar_label: Lega Basket Serie A site API (www.legabasket.it/api, Italy)
description: "LBA — Lega Basket Serie A site API (www.legabasket.it/api, Italy) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# LBA — Lega Basket Serie A site API (www.legabasket.it/api, Italy)

`sportsdataverse.basketball.lba` — 19 endpoints.

> **Note:** Unofficial and unsupported: these wrappers read the JSON API that the Next.js site www.legabasket.it itself calls. The site's terms say "Non è consentito copiare … dati e informazioni senza autorizzazione scritta" (copying data and information without written authorisation is not allowed), except for personal, non-commercial use. The owner accepted the restriction for this intake on 2026-10-12. Payloads are not redistributed as release assets, and any published data release of this source needs separate sign-off.

> **Note:** robots.txt allows /api and sets Crawl-delay: 2. The runtime waits 2 s before every request, retries included. Crawl incrementally: fetch a final game once (keyed by game_id), find new games with one lba_calendar call per round, and do not backfill a history in a burst.

> **Note:** Seasons are END years (2027 = 2026-27); the site sends START years and the runtime maps them. Game and player ids are global; team ids are re-issued every season (join lba_teams for the stable club_id); a championship id is one season AND one phase (Regular Season and Play Off differ).

> **Note:** Play-by-play starts in 2017-18 and changes shape in 2019-20 (pixel frame to court percent) and 2021-22; see lba_pbp. Box scores reach back to 1995-96, where the period scores were not recorded (0 means missing).

## Player

| Function | Summary |
|---|---|
| [lba_players](legabasket/player.md#lba_players) | Players of a season, 50 per page (walk the pages with page). |
| [lba_player](legabasket/player.md#lba_player) | Player bio with current-season totals, highs and averages (one row). |
| [lba_player_stats](legabasket/player.md#lba_player_stats) | A player's career by championship: per-game averages, single-game highs and totals. |
| [lba_players_leaderboard](legabasket/player.md#lba_players_leaderboard) | Season player leaderboard for one category (per-game average). |

## Team

| Function | Summary |
|---|---|
| [lba_team_box_score](legabasket/team.md#lba_team_box_score) | Team lines of a final game's box score: the team-booked stats and the totals of each side. |
| [lba_teams](legabasket/team.md#lba_teams) | Teams of a season, one row per team-season with the stable club id. |
| [lba_team_players_stats](legabasket/team.md#lba_team_players_stats) | Season totals of each player of a team-season. |
| [lba_teams_leaderboard](legabasket/team.md#lba_teams_leaderboard) | Season team leaderboards, offensive or defensive, one row per team and category. |

## Other

| Function | Summary |
|---|---|
| [lba_championships](legabasket/other.md#lba_championships) | Championships of a season, one per phase (Regular Season, Play Off); every season when none is given. |
| [lba_calendar](legabasket/other.md#lba_calendar) | Games of one round of a championship: teams, score, venue, referees and broadcasters. |
| [lba_game](legabasket/other.md#lba_game) | Game header: teams, venue, quarter scores, coaches, referees and broadcasters. |
| [lba_box_score](legabasket/other.md#lba_box_score) | Box score of a final game: one row per player of both teams (Italian-named stat columns). |
| [lba_pbp](legabasket/other.md#lba_pbp) | Play-by-play of a final game (2017-18 on), one row per action with x/y; coordinate frame by era. |
| [lba_pbp_legend](legabasket/other.md#lba_pbp_legend) | The play-by-play qualifier code table (59 codes) with its Italian labels. |
| [lba_shots](legabasket/other.md#lba_shots) | Shooting by court zone per team for a final game (made / attempted aggregates; zone 7 includes free throws). |
| [lba_standings](legabasket/other.md#lba_standings) | Standings of a championship after a round, one row per team. |
| [lba_roster](legabasket/other.md#lba_roster) | Players of one team-season with bio, number and role. |
| [lba_roster_coach](legabasket/other.md#lba_roster_coach) | Head coach of one team-season. |
| [lba_game_log](legabasket/other.md#lba_game_log) | A player's game log for one season, one row per game. |
