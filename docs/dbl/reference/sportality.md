# DBL — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality)

> DBL — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.basketball.dbl` — 25 endpoints.

> **Note:** Unofficial: the JSON API behind www.basketligaen.dk, the Danish men's basketball league (Basketligaen, series code DBL). The site runs on the Sportality platform (the vendor behind the SHL and HockeyAllsvenskan wrappers). The terms page was not read; payloads are not redistributed as release assets.

> **Note:** Stats provider: genius-basketball, a relay of Genius Sports LiveStats. The Genius match, team and person ids in play-by-play, box scores and stats tables (game_id, team_id, player_id, all Int64) join the genius family; the platform uuids (game, team, athlete, ssgt) are not Genius ids and exist only on this site. The Genius ORG code of Denmark is not exposed.

> **Note:** Season = the END year (2026 = 2025/26) everywhere; the runtime maps it, with game_type ('regular' or 'finals', the playoffs), to the site's ssgt (season + series + game type) through layout-info. Schedules, standings and season tables start in 2002/03; game-level data (play-by-play with shot x/y, box scores, team stats) starts in 2023/24.

> **Note:** Source defects the runtime refuses (AssetFetchError): an unfinished game or a truncated play-by-play feed, a box score or feed that names another game, a cut stats table. The 2002/03 regular-season schedule lists 109 finished games where its own standings count 110: a source gap.

> **Note:** Personal data is removed from every body: birth dates, e-mail addresses, phone numbers, addresses and contact blocks.

> **Note:** No women's league: Dameligaen has its own domain (dameligaen.dk), which answers 401 to anonymous requests.

## Athlete

| Function | Summary |
|---|---|
| [sportality_athlete_career](sportality/athlete.md#sportality_athlete_career) | Player career totals, one row per ssgt. |
| [sportality_athlete_details](sportality/athlete.md#sportality_athlete_details) | Player bio plus playerExtIds (the Genius person id). |
| [sportality_athlete_profile](sportality/athlete.md#sportality_athlete_profile) | Player profile: age, height, position, season and career totals. |

## Game

| Function | Summary |
|---|---|
| [sportality_game_header](sportality/game.md#sportality_game_header) | Game header: Genius match id (extId), arena, state, teams, scores. |
| [sportality_game_overview](sportality/game.md#sportality_game_overview) | Score, state, clock and the Genius ids of one game (2023/24 on). |
| [sportality_game_summary](sportality/game.md#sportality_game_summary) | Points per period, team comparison and best scorers (2023/24 on). |

## Player

| Function | Summary |
|---|---|
| [sportality_player_box](sportality/player.md#sportality_player_box) | Box score with jersey and Genius shirt numbers (2023/24 on). |
| [sportality_player_h2h](sportality/player.md#sportality_player_h2h) | Player head-to-head of a matchup with per-game and season lines. |

## Team

| Function | Summary |
|---|---|
| [sportality_team](sportality/team.md#sportality_team) | One team. |
| [sportality_team_played_games](sportality/team.md#sportality_team_played_games) | A team's recent games (current season view). |
| [sportality_team_stats](sportality/team.md#sportality_team_stats) | Team totals per period (2023/24 on). |
| [sportality_team_upcoming_games](sportality/team.md#sportality_team_upcoming_games) | A team's next games (current season view). |

## Other

| Function | Summary |
|---|---|
| [sportality_boxscore](sportality/other.md#sportality_boxscore) | Player box score and team rows of both sides (2023/24 on). |
| [sportality_last_game_day](sportality/other.md#sportality_last_game_day) | Games of the last played match day of an ssgt. |
| [sportality_layout_info](sportality/other.md#sportality_layout_info) | Directory of every ssgt (season + series + game type) with its teams, and the stats modules. |
| [sportality_next_game_day](sportality/other.md#sportality_next_game_day) | Games of the next match day of an ssgt. |
| [sportality_pbp](sportality/other.md#sportality_pbp) | Play-by-play events with shot coordinates and the Genius ids (2023/24 on). |
| [sportality_post_game_team_stats](sportality/other.md#sportality_post_game_team_stats) | Post-game team comparison (2023/24 on). |
| [sportality_roster](sportality/other.md#sportality_roster) | Current roster of a team, grouped by position. |
| [sportality_schedule](sportality/other.md#sportality_schedule) | Schedule and results of one season + series + game type. |
| [sportality_season_filter](sportality/other.md#sportality_season_filter) | Series, the 25 seasons (2002/03 on), game types and the default ssgt. |
| [sportality_sites](sportality/other.md#sportality_sites) | Clubs (sites) taking part in an ssgt. |
| [sportality_ssgt_provider](sportality/other.md#sportality_ssgt_provider) | Stats provider of an ssgt, a bare word (genius-basketball). |
| [sportality_standings](sportality/other.md#sportality_standings) | Standings of an ssgt (gameday widget). |
| [sportality_stats_info](sportality/other.md#sportality_stats_info) | Stats table of one module for a season (players, teams, games, standings). |
