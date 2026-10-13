---
title: TBF — TBF app API (miniappapi.tbf.org.tr, Turkey: BSL, TBL, KBSL)
sidebar_label: TBF app API (miniappapi.tbf.org.tr, Turkey: BSL, TBL, KBSL)
description: "TBF — TBF app API (miniappapi.tbf.org.tr, Turkey: BSL, TBL, KBSL) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# TBF — TBF app API (miniappapi.tbf.org.tr, Turkey: BSL, TBL, KBSL)

`sportsdataverse.basketball.tbf` — 32 endpoints.

> **Note:** Official TBF app API, unofficial for SportsDataverse: the backend of the Turkish Basketball Federation's mobile app (miniappapi.tbf.org.tr), which serves BSL, TBL, KBSL and the other TBF leagues. It needs no key; robots.txt answers 404 on the host, and `tbf_runtime` spaces requests at least 1 s apart. SportsDataverse has not ruled on TBF's terms, so payloads are not redistributed as release assets. The vendor's origin hosts (nikayazilim.com) serve the same data without Cloudflare but are deliberately not used, redirects are never followed, and www.tbf.org.tr's interactive Turnstile is neither used nor automated.

> **Note:** Coverage: season and competition are parameters (`bsl` Basketbol Super Ligi, back to 1966-67 in the league list; `tbl` second tier; `kbsl` women, both from 2015-16). Game wrappers serve FINISHED games only (an unfinished game raises AssetFetchError). Box scores exist from 2006-07 (no starters flagged that season); play-by-play and shots from 2025-26 only, an earlier finished game raises NoDataError. Every game carries a Genius LiveStats id (`genius_id`, joins the `genius` family) from 2018-19 on some seasons and on every game of 2025-26 and later. Birth dates, contact details, photos and logos are dropped. Names and category labels are Turkish; the column descriptions give the English gloss.

## Leaders

| Function | Summary |
|---|---|
| [tbf_leaders_points](tbf/leaders.md#tbf_leaders_points) | Season leaders in points: the server's top 20 by season total and by per-game average. |
| [tbf_leaders_assists](tbf/leaders.md#tbf_leaders_assists) | Season leaders in assists: the server's top 20 by season total and by per-game average. |
| [tbf_leaders_blocks](tbf/leaders.md#tbf_leaders_blocks) | Season leaders in blocks: the server's top 20 by season total and by per-game average. |
| [tbf_leaders_rebounds](tbf/leaders.md#tbf_leaders_rebounds) | Season leaders in rebounds: the server's top 20 by season total and by per-game average. |
| [tbf_leaders_steals](tbf/leaders.md#tbf_leaders_steals) | Season leaders in steals: the server's top 20 by season total and by per-game average. |
| [tbf_leaders_minutes](tbf/leaders.md#tbf_leaders_minutes) | Season leaders in minutes played: the server's top 20 by season total and by per-game average. |
| [tbf_leaders_threes](tbf/leaders.md#tbf_leaders_threes) | Season leaders in three-point field goals made: the server's top 20 by season total and by per-game average. |
| [tbf_leaders_turnovers](tbf/leaders.md#tbf_leaders_turnovers) | Season leaders in turnovers: the server's top 20 by season total and by per-game average. |
| [tbf_leaders_efficiency](tbf/leaders.md#tbf_leaders_efficiency) | Season leaders in efficiency rating: the server's top 20 by season total and by per-game average. |

## Player

| Function | Summary |
|---|---|
| [tbf_player_box](tbf/player.md#tbf_player_box) | Player box score of a finished game: minutes, shooting splits, rebounds, assists, +/- and efficiency. |
| [tbf_players](tbf/player.md#tbf_players) | Every player registered in one league-season (birth dates are not returned). |
| [tbf_player_career](tbf/player.md#tbf_player_career) | One player's career: a row per season and competition with totals (birth date is not returned). |

## Team

| Function | Summary |
|---|---|
| [tbf_team_box](tbf/team.md#tbf_team_box) | Team totals of a finished game with the team-credited rebounds, turnovers and fouls. |
| [tbf_teams](tbf/team.md#tbf_teams) | Every team registered in one league-season with its team-season id (team_process_id) and club id. |
| [tbf_team_player_stats](tbf/team.md#tbf_team_player_stats) | Season player statistics of one team-season: totals, percentages and per-game averages. |
| [tbf_team_games](tbf/team.md#tbf_team_games) | Games and results of one team-season, scheduled games included. |
| [tbf_team_roster](tbf/team.md#tbf_team_roster) | Roster of one team-season with games, minutes and per-game points and efficiency (birth dates dropped). |
| [tbf_team_staff](tbf/team.md#tbf_team_staff) | Coaching and club staff of one team-season. |

## Other

| Function | Summary |
|---|---|
| [tbf_seasons](tbf/other.md#tbf_seasons) | The 20 most recent TBF seasons, domestic and international (a server cap), newest first. |
| [tbf_leagues](tbf/other.md#tbf_leagues) | The featured leagues (league-season activities) of one TBF season: BSL, TBL, KBSL and the others. |
| [tbf_league_seasons](tbf/other.md#tbf_league_seasons) | Every season of one TBF league (bsl back to 1966-67) with its activity and season ids. |
| [tbf_weeks](tbf/other.md#tbf_weeks) | The weeks (rounds) of one league-season, playoff rounds included. |
| [tbf_standings](tbf/other.md#tbf_standings) | League table of one league-season after a week (default the latest), one row per team and group. |
| [tbf_schedule](tbf/other.md#tbf_schedule) | Schedule and results of one league-season: the games of one week, or of every week. |
| [tbf_daily_matches](tbf/other.md#tbf_daily_matches) | Every TBF game played on one date, grouped by competition, optionally for one league-season. |
| [tbf_game_header](tbf/other.md#tbf_game_header) | Game header of a finished game: teams, coaches, final score, officials, venue and the Genius LiveStats id. |
| [tbf_game_players](tbf/other.md#tbf_game_players) | Game roster from the header: every listed player of both sides (names and shirt numbers, no usable ids). |
| [tbf_pbp](tbf/other.md#tbf_pbp) | Play-by-play of a finished game (2025-26 onward), in game order, with the running score and an English gloss. |
| [tbf_shots](tbf/other.md#tbf_shots) | Every field-goal attempt of a finished game (2025-26 onward) with its court position. |
| [tbf_period_scores](tbf/other.md#tbf_period_scores) | Points each side scored in each period of a finished game, overtime included. |
| [tbf_highlights](tbf/other.md#tbf_highlights) | Team comparison of a finished game: rebounds, paint points, fast-break points, lead changes and more. |
| [tbf_game_leaders](tbf/other.md#tbf_game_leaders) | Game leaders of a finished game per statistic category (top five each). |
