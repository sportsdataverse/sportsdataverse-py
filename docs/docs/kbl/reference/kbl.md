---
title: KBL — KBL API (api.kbl.or.kr, Korea)
sidebar_label: KBL API (api.kbl.or.kr, Korea)
description: "KBL — KBL API (api.kbl.or.kr, Korea) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# KBL — KBL API (api.kbl.or.kr, Korea)

`sportsdataverse.basketball.kbl` — 22 endpoints.

> **Note:** Unofficial and unsupported: these wrappers read api.kbl.or.kr, the data API behind the www.kbl.or.kr React site (stats vendor sports2i). No key, cookie or proxy is needed, but two request headers must be present (Channel and TeamCode; the runtime sends WEB and XX). www.kbl.or.kr/robots.txt returns the SPA's HTML and api.kbl.or.kr/robots.txt a 500 error envelope, so no policy or Crawl-delay is published (checked 2026-10-12); the site's terms (/terms/list) were not reviewed for data reuse. The runtime waits at least 1 s before every request, retries included. Payloads are not redistributed as release assets.

> **Note:** Not wrapped: the site's /record/* and /player/player/{pcode} pages are rendered by the sports2i widget from two other hosts (kbl-api.sports2i.com and api-stats.kbl.or.kr: team four factors, clutch, hustle, shot dashboards, all-time leaders) and need their own intake. WKBL (the women's league) is out of scope by owner ruling.

> **Note:** Seasons are END years (2026 = 2025-26). KBL numbers seasons AND D-League (reserve league) editions with one sequential seasonCode; division picks 'kbl' or 'd_league' and game_code the competition inside the season, and kbl_runtime sends the API's glkey. The playoff routes take the REGULAR-SEASON key; the old route serves seasonCode below 45 and the new one 45 on, and the wrapper picks it.

> **Note:** Ids: gmkey S{seasonCode}G{gameCode}N{gameNo} (a string, an object only inside the new playoff bracket), team_code (two-digit string with a leading zero, not stable across ownership changes) and pcode (six-digit string, the same on every route). The play-by-play carries no pcode: join its player_name to the game's box score on name and team_code. isEnded is unreliable (0 on every finished game through 2005-06), so a game is final when isEnded is 1 or when it is dated before today (KST), both teams scored and its play-by-play reached the game-end row; anything else raises AssetFetchError. The first game call also fetches the header (and, for such a game, the play-by-play) once; the answer is cached for the process.

> **Note:** Coverage: box scores and play-by-play from the first season (1997); shot x/y from 2006-07 (an earlier game's shot chart is empty: NoDataError); advanced rates are filled from 1997-98 on (the vendor computes them from the box); screen assists and deflections are 0 before the recent seasons. fg is TWO-point field goals made (dunks included), fgt all field goals, and a made dunk is its own row in the play-by-play. Shot x/y are pixels on the site's 726 x 412 full-court image; basket names the target end and teams switch at half-time, so normalise with basket before stacking shots.

## Player

| Function | Summary |
|---|---|
| [kbl_player_box](kbl/player.md#kbl_player_box) | Player box score of a final game with the vendor's advanced rates, one row per player of both teams. |
| [kbl_player_totals](kbl/player.md#kbl_player_totals) | Season player totals of one competition (every player with at least one game). |
| [kbl_players](kbl/player.md#kbl_players) | The site's player directory (current and former players) with their current or last team. |

## Team

| Function | Summary |
|---|---|
| [kbl_teams](kbl/team.md#kbl_teams) | The current ten clubs with Korean and English names and the code of each club's D-League side. |
| [kbl_team_box](kbl/team.md#kbl_team_box) | Team box score of a final game with advanced rates and team-only counts, one row per team. |
| [kbl_team_tracks](kbl/team.md#kbl_team_tracks) | Each team's standing after every game of the season, one row per team and game. |

## Other

| Function | Summary |
|---|---|
| [kbl_seasons](kbl/other.md#kbl_seasons) | Every KBL regular season (1997 to the next one) with its competition key and date range, newest first. |
| [kbl_schedule](kbl/other.md#kbl_schedule) | Every game (all competitions, KBL and D-League) of a season or of a date window, with scores and venue. |
| [kbl_game](kbl/other.md#kbl_game) | Game header of a final game: teams, final score, overtimes, venue, attendance, officials and coaches. |
| [kbl_period_scores](kbl/other.md#kbl_period_scores) | Points by period (quarters and overtimes) and side of a final game, from the game header. |
| [kbl_pbp](kbl/other.md#kbl_pbp) | Play-by-play (the site's text cast) of a final game, one row per event, from 1997-98 on. |
| [kbl_shots](kbl/other.md#kbl_shots) | Shot chart of a final game (2006-07 on): every charted shot with x/y pixels on the full-court image. |
| [kbl_score_track](kbl/other.md#kbl_score_track) | Lead tracker and score chart of a final game: home margin and both scores at eight checkpoints per period. |
| [kbl_game_leaders](kbl/other.md#kbl_game_leaders) | Each team's leader per category (points, rebounds, assists, steals, blocks) in a final game. |
| [kbl_standings](kbl/other.md#kbl_standings) | Standings of one competition with each team's season totals. |
| [kbl_rounds](kbl/other.md#kbl_rounds) | The season's round calendar (the KBL plays six rounds) with each round's first and last date. |
| [kbl_playoff_seeds](kbl/other.md#kbl_playoff_seeds) | The regular-season finish that seeds the playoffs (the top six). |
| [kbl_playoff_series](kbl/other.md#kbl_playoff_series) | The playoff series (quarterfinals, semifinals, final): wins and losses of each team in each series. |
| [kbl_playoff_games](kbl/other.md#kbl_playoff_games) | The games of the playoff bracket (2024-25 on; the older bracket format lists no games). |
| [kbl_round_mvps](kbl/other.md#kbl_round_mvps) | The round MVPs of a season with their totals over the round. |
| [kbl_current_standings](kbl/other.md#kbl_current_standings) | Current standings of the active season (no parameter): record, games behind, streaks, splits. |
| [kbl_season_leaders](kbl/other.md#kbl_season_leaders) | Current season leaders per category (no parameter): points, rebounds, assists, steals, blocks, shooting. |
