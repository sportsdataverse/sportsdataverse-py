# WJBL — W League API (api.wjbl.01core.app, Japan women)

> WJBL — W League API (api.wjbl.01core.app, Japan women) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.basketball.wjbl` — 19 endpoints.

> **Note:** Unofficial and unsupported: these wrappers read the JSON backend of www.wjbl.org (api.wjbl.01core.app), keyless, found in the site's own JavaScript. The site's terms of use (www.wjbl.org/kiyaku/) forbid reproducing, republishing or otherwise using site content without permission: a copyright clause, not an anti-scraping one. The wrappers fetch on the user's behalf; a published data release or a hosted mirror needs the owner's decision first. The site has no robots.txt (404, checked 2026-10-12), so no Crawl-delay is published; the runtime waits at least 1 s before every request, retries included. Payloads are not redistributed as release assets.

> **Note:** Women's league: the W League is Japan's top women's basketball league (the SportsDataverse R counterpart is wehoop). All text is Japanese (team names, phases, play-by-play actions); the column descriptions translate the vocabulary.

> **Note:** Seasons are END years (2026 = 2025-26, 2000 = 1999-2000). The API keys a season edition by an opaque league_id (45 = '25-26 Wリーグ'; every all-star game, cup and camp has one too) and the wrappers map the END year with the live league list (one extra, cached request; END = 2000 + the second YY of league_title). Phases inside an edition (season_id: 169 = regular season Premier, 170 = Future, 176 = playoff final; two divisions from 2024-25) are native ids, passed as given: wjbl_standing_seasons lists them with their END year.

> **Note:** The schedule is paged by month: wjbl_schedule answers ONE month (game_month, YYYY-MM; omitted = the season's latest month only). List the months with wjbl_game_months and loop. The game routes take only the global play_schedule_id and serve final games only: each makes one extra, cached request (the game header) to prove the game is final and that the answer is the game and the teams asked for.

> **Note:** Errors are HTTP 200: an unknown id answers [] (NoDataError) and a missing or non-numeric parameter answers an error envelope (AssetFetchError). Coverage: schedules from 1999-2000, box scores from 2013-14, play-by-play and the period stats from 2018-19 (earlier games answer [] on the /pbp routes: NoDataError). No route carries shot coordinates.

> **Note:** Personal data: the organiser's contact details (phones, e-mail) and the player's blood type are never output, and phone numbers and e-mail addresses inside free text are replaced by [redacted-phone] / [redacted-email]. Do not add them back. Player bio fields and referee names are public and stay.

## Game

| Function | Summary |
|---|---|
| [wjbl_game_months](wjbl/game.md#wjbl_game_months) | Months (YYYY-MM) that have games in a season, newest first; each is one page of wjbl_schedule. |
| [wjbl_game_info](wjbl/game.md#wjbl_game_info) | Game information of a final game: venue, times, referees, links and the Japanese game report, one row. |

## Player

| Function | Summary |
|---|---|
| [wjbl_player_period_leaders](wjbl/player.md#wjbl_player_period_leaders) | Leading scorer, passer and rebounder of each team per period, half and game (2018-19 on). |
| [wjbl_player](wjbl/player.md#wjbl_player) | Player profile: current team, names, bio and career notes, one row. |

## Team

| Function | Summary |
|---|---|
| [wjbl_team_box_score](wjbl/team.md#wjbl_team_box_score) | Team line and totals of each side of a final game's box score, with the shooting percentages. |
| [wjbl_team_period_stats](wjbl/team.md#wjbl_team_period_stats) | Team stat summary per period, half and game (2018-19 on): percentages, rebounds, assists, turnovers, lead changes. |
| [wjbl_team_standings](wjbl/team.md#wjbl_team_standings) | League table of one or more season phases: rank and record of every team. |
| [wjbl_team_ranking](wjbl/team.md#wjbl_team_ranking) | League table with per-game team averages (points, shooting, rebounds, assists) of one or more season phases. |
| [wjbl_teams](wjbl/team.md#wjbl_teams) | Every franchise (team_id) the site knows, all-star sides included. |

## Other

| Function | Summary |
|---|---|
| [wjbl_leagues](wjbl/other.md#wjbl_leagues) | Every competition edition (league_id): the W League seasons back to 1999-2000, all-star games, cups and camps. |
| [wjbl_schedule](wjbl/other.md#wjbl_schedule) | Games of a season in ONE month (default: the season's latest month) with teams, venue, status and score. |
| [wjbl_game](wjbl/other.md#wjbl_game) | Game header of a final game: both teams, the result flag and the score by period, one row. |
| [wjbl_period_scores](wjbl/other.md#wjbl_period_scores) | Points of each side in each period played (quarters and overtimes) from the game header. |
| [wjbl_box_score](wjbl/other.md#wjbl_box_score) | Box score of a final game (2013-14 on): one row per player of both teams. |
| [wjbl_pbp](wjbl/other.md#wjbl_pbp) | Play-by-play of a final game (2018-19 on): one row per play, Japanese action vocabulary, no shot x/y. |
| [wjbl_standing_seasons](wjbl/other.md#wjbl_standing_seasons) | Every season phase (season_id) with its edition name and standings flags. |
| [wjbl_head_to_head](wjbl/other.md#wjbl_head_to_head) | Head-to-head results grid (hoshitori-hyo) of one or more season phases: one row per game played. |
| [wjbl_roster](wjbl/other.md#wjbl_roster) | Players of a team in a season phase, with the head coach. |
| [wjbl_staff](wjbl/other.md#wjbl_staff) | Staff of a team in a season phase (general manager, assistant coaches, support staff). |
