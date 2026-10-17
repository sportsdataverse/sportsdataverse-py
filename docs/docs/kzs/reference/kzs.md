---
title: KZS — KZS FMS public API (Basketball Federation of Slovenia)
sidebar_label: KZS FMS public API (Basketball Federation of Slovenia)
description: "KZS — KZS FMS public API (Basketball Federation of Slovenia) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# KZS — KZS FMS public API (Basketball Federation of Slovenia)

`sportsdataverse.basketball.kzs` — 23 endpoints.

> **Note:** Unofficial and unsupported: these wrappers read the public read API (Java/Spring, built by OnixWeb) that the kzs.si site itself calls. No terms of use were located (unverified), api.kzs.si/robots.txt answers 401 and www.kzs.si/robots.txt 404, so no policy or Crawl-delay is published. The runtime waits at least 1 s before every request, retries included. Payloads are not redistributed as release assets.

> **Note:** Personal data: the API serves officials' and coaches' emails, phones and home towns, and clubs' bank accounts and addresses, publicly. The parsers never output them (officials appear by name only; club addresses, personnel and the commissioner report are dropped). Do not add them back.

> **Note:** Seasons are END years (2027 = 2026/2027); KZS' own season ids are opaque (27 = 2026/2027) and the runtime maps them. Competition ids are per season. Three team keys are never interchangeable: the match-team id (per match and side), team_id (one club team in one season) and club_id (across seasons). player_id is the registry id, person_id the person behind it.

> **Note:** Errors are HTTP 400 for everything (an unknown id and a bad parameter alike), so the runtime raises AssetFetchError for it, never ValueError or an empty frame. List routes default to 150 rows with no truncation signal: every list wrapper asks for a large limit and refuses a full page. The play-by-play and box-score wrappers, the leaderboards, teams, rounds and playing days each make one extra, cached request (the match header or the competition) to prove the answer is the one asked for and the game is final.

> **Note:** Coverage: scores from 2007/08; box scores and play-by-play from 2015/16. Some 2020/21 men's games have a box score but no play-by-play (NoDataError): the Genius data.json (genius_match_id in kzs_matches) is the fallback. Shot x/y are Genius court percent; side (LEFT/RIGHT) names the basket end and teams switch ends at half time, so normalise with side before stacking shots. Advanced box-score fields are 0 in every observed game.

## Player

| Function | Summary |
|---|---|
| [kzs_player_leaderboard](kzs/player.md#kzs_player_leaderboard) | Player leaderboard of a competition for one stat category (total, per-game average and single-game best). |
| [kzs_player](kzs/player.md#kzs_player) | Player profile: names, year of birth, nationality and positions. |
| [kzs_player_stats](kzs/player.md#kzs_player_stats) | A player's totals and per-game averages in each competition of one season. |

## Team

| Function | Summary |
|---|---|
| [kzs_team_leaderboard](kzs/team.md#kzs_team_leaderboard) | Team leaderboard of a competition for one stat category (total, per-game average and single-game best). |
| [kzs_team_box_score](kzs/team.md#kzs_team_box_score) | Team totals of a final game's box score, one row per side. |
| [kzs_teams](kzs/team.md#kzs_teams) | Teams entered in a competition. |
| [kzs_team](kzs/team.md#kzs_team) | Team profile: club, home hall, colours and the competitions the team entered. |
| [kzs_team_general](kzs/team.md#kzs_team_general) | A team's record, form and best players in one competition (the body echoes neither id). |

## Other

| Function | Summary |
|---|---|
| [kzs_seasons](kzs/other.md#kzs_seasons) | Every season (39: 1991/1992 to 2029/2030) with its opaque id and the sdv END year. |
| [kzs_competitions](kzs/other.md#kzs_competitions) | Competitions of a season (leagues, cups, youth, 3x3, national teams), or of every season. |
| [kzs_competition](kzs/other.md#kzs_competition) | One competition: season, gender, category, rank and type (its phases are kzs_phases). |
| [kzs_phases](kzs/other.md#kzs_phases) | The phases of a competition (regular season, play-offs, finals) with dates and type. |
| [kzs_rounds](kzs/other.md#kzs_rounds) | Number of rounds of each phase of a competition. |
| [kzs_matches](kzs/other.md#kzs_matches) | Matches of a competition (optionally inside a from/to window) with the Genius LiveStats id. |
| [kzs_match](kzs/other.md#kzs_match) | Match header: teams, scores, venue, officials' names, best performers and status. |
| [kzs_period_scores](kzs/other.md#kzs_period_scores) | Points by match part (quarters and overtimes) from the match header. |
| [kzs_pbp](kzs/other.md#kzs_pbp) | Play-by-play of a final game (2015/16 on, with gaps), one row per action with shot x/y. |
| [kzs_pbp_players](kzs/other.md#kzs_pbp_players) | The players the play-by-play feed lists for each side of a final game. |
| [kzs_box_score](kzs/other.md#kzs_box_score) | Box score of a final game (2015/16 on): one row per player of both teams. |
| [kzs_score_changes](kzs/other.md#kzs_score_changes) | The score at the end of each minute of a final game. |
| [kzs_standings](kzs/other.md#kzs_standings) | Standings of a competition, one row per team in each phase (and group). |
| [kzs_playing_days](kzs/other.md#kzs_playing_days) | Match days of a competition and the number of games on each. |
| [kzs_roster](kzs/other.md#kzs_roster) | A team's roster in one competition (the rows carry no team or competition id). |
