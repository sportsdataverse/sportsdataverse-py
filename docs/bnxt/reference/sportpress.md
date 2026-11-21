# BNXT League — BNXT League Sportpress API (bnxt.sportpress.info, Belgium and the Netherlands)

> BNXT League — BNXT League Sportpress API (bnxt.sportpress.info, Belgium and the Netherlands) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.basketball.bnxt` — 28 endpoints.

> **Note:** Unofficial and unsupported: the Sportpress API is the JSON backend of bnxtleague.com, not a documented public API. Every route needs a header key; it is public (hard-coded in the site bundle), `bnxt_runtime` reads it from the bundle at run time, falls back to a documented value when the bundle cannot be read, and never prints or logs it. A 401 raises AssetFetchError (`key rotated?`). robots.txt allows everything; requests are spaced at least 1 s apart. Payloads are not redistributed as release assets.

> **Note:** Season key: the END year (`season=2026` is 2025-26), the API's own key on the season routes. Every other route is keyed by the competition id (one per season: 1 = 2021-22, 2 = 2020-21, 3 = 2022-23, 12 = 2023-24, 22 = 2024-25, 24 = 2025-26, 25 = 2026-27), the competition-team id (new every season; `uu_team_id` is the stable club), the roster id (player x team x season) or the game id (unique across seasons). `sportpress_phases(season)` maps a season to its competition id.

> **Note:** Coverage: schedule and rosters from 2021-22 (2020-21 is empty); box scores for 2021-22, 2024-25, 2025-26 and 2026-27 (2022-23 and 2023-24 answer empty); standings and averages for 2021-22 and 2024-25 on; play-by-play ('film', with court x/y) for 2021-22 and 2026-27 only: for 2022-23 to 2025-26 use Genius play-by-play through `genius_id` (`sportpress_game_slider`, organisation BNXT). The game routes (box score, film, overview) refuse a game that is not finished (AssetFetchError).

> **Note:** Data notes: a team's box total can differ from the schedule final score on about 18 of 1,246 sampled team boxes (mostly 2024-25, by 2-21 points); the frames report what the API serves. `nationality_code` is dirty (lower case, `USA/BEL` pairs, empty, `HOL`). Names differ by language and sponsor: join on ids. Contact and social fields are dropped; only a player's public birth date is kept. The sibling tenants sopron.sportpress.info and beac.sportpress.info are not wrapped.

## Game

| Function | Summary |
|---|---|
| [sportpress_game_slider](sportpress/game.md#sportpress_game_slider) | The latest 14 finished games of a competition plus every upcoming game, with the Genius match id. |
| [sportpress_game_find](sportpress/game.md#sportpress_game_find) | One game: teams, scores, phase, venue and the neighbouring game ids. |
| [sportpress_game_periods](sportpress/game.md#sportpress_game_periods) | Points each side scored in each period of a game (periods 5 and up are overtimes). |
| [sportpress_game_overview](sportpress/game.md#sportpress_game_overview) | Team totals of both sides of a finished game. |
| [sportpress_game_top_performers](sportpress/game.md#sportpress_game_top_performers) | The top performer of each side of a finished game, with his full line. |
| [sportpress_game_leaders](sportpress/game.md#sportpress_game_leaders) | The game leader of each side in every statistic (one row per side and statistic). |
| [sportpress_game_box](sportpress/game.md#sportpress_game_box) | Per-player box score of a finished game (one row per player line). |
| [sportpress_game_team_box](sportpress/game.md#sportpress_game_team_box) | Team totals of both sides from the box score of a finished game. |
| [sportpress_game_film](sportpress/game.md#sportpress_game_film) | Play-by-play ('film') of a finished game with court x/y (2021-22 and 2026-27 only). |

## Player

| Function | Summary |
|---|---|
| [sportpress_player_rankings](sportpress/player.md#sportpress_player_rankings) | Qualified-player per-game leaders in nine categories (one row per player and category). |
| [sportpress_player_career](sportpress/player.md#sportpress_player_career) | Season-by-season per-game averages of the player of a roster entry. |
| [sportpress_player_games](sportpress/player.md#sportpress_player_games) | Game-by-game box lines of a roster entry (including games with 0 minutes). |

## Team

| Function | Summary |
|---|---|
| [sportpress_team_rankings](sportpress/team.md#sportpress_team_rankings) | Team per-game leaders in nine categories (one row per team and category). |
| [sportpress_team_roster](sportpress/team.md#sportpress_team_roster) | Roster of one competition team (current entries only). |
| [sportpress_team_find](sportpress/team.md#sportpress_team_find) | One competition team with its season scoring averages. |
| [sportpress_team_player_averages](sportpress/team.md#sportpress_team_player_averages) | Per-game averages of each player of a competition team. |
| [sportpress_team_games](sportpress/team.md#sportpress_team_games) | Every finished game of a competition team with that team's totals. |

## Other

| Function | Summary |
|---|---|
| [sportpress_ui_strings](sportpress/other.md#sportpress_ui_strings) | UI string tables of the site in English, French and Dutch (one row per language and key). |
| [sportpress_club_seasons](sportpress/other.md#sportpress_club_seasons) | Seasons (END years) in which Belgian and Dutch clubs have teams. |
| [sportpress_referees](sportpress/other.md#sportpress_referees) | Referees of the league (staff title 2). |
| [sportpress_phases](sportpress/other.md#sportpress_phases) | The competition of a season with its phases (regular season, playoffs, supercup). |
| [sportpress_competition_teams](sportpress/other.md#sportpress_competition_teams) | Every competition team of one competition: ids, names and country. |
| [sportpress_competition_staff](sportpress/other.md#sportpress_competition_staff) | Coaching staff (head coach, assistants) of every competition team of one competition. |
| [sportpress_standings](sportpress/other.md#sportpress_standings) | Standings of one phase of a competition: record, points, home/away split, last five and streak. |
| [sportpress_schedule](sportpress/other.md#sportpress_schedule) | Every game of a season for Belgian and Dutch clubs, with teams, scores, phase and venue. |
| [sportpress_season_roster](sportpress/other.md#sportpress_season_roster) | Every roster entry of a season: player, team, jersey, height, weight and position. |
| [sportpress_nationality_codes](sportpress/other.md#sportpress_nationality_codes) | Distinct player nationality codes of a season (dirty: lower case, pairs, empty). |
| [sportpress_roster_find](sportpress/other.md#sportpress_roster_find) | One roster entry with its player, team and season. |
