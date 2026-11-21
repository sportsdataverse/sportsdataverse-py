# VTB — VTB United League Infobasket (Russia)

> VTB — VTB United League Infobasket (Russia) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.basketball.vtb` — 35 endpoints.

> **Note:** Owner flag: the VTB United League is run from Russia and several clubs are owned or sponsored by sanctioned state companies (the KHL precedent); Infobasket is the Russian basketball federation's stats platform. Fetching on your own machine is fine; any published data release, hosted mirror or redistribution needs owner (sanctions / legal) sign-off first. No accounts or payments are involved.

> **Note:** Unofficial and unsupported: both hosts are keyless public JSON backends of vtb-league.com. No terms of use are published; robots.txt is a 404 on api.vtb-league.com and org.infobasket.su (no rules; vtb-league.com disallows only /wp/wp-admin/). The runtime waits at least 1 s before every request, retries included, and honours Retry-After on 429 / 503. No key, cookie or proxy is used (live-verified direct from the droplet 2026-10-12).

> **Note:** Seasons are END years (2026 = 2025-26) on both backends. Infobasket keys everything by competition id: vtb_infobasket_seasons maps the END year to the season-level CompID, and the season-keyed wrappers do that mapping for you. CompID means three things: a season (50714), a stage below it (50720 regular season, 54281 playoff; stage-level wrappers take the stage id from vtb_infobasket_comp_issue) and, in the player season stats, an END year.

> **Note:** Ids: the v2 matchId is the Infobasket GameID, the v2 teamId the Infobasket TeamID and the v2 venueId the Infobasket ArenaId. team_id is the key on every team route; comp_team_id (a registration in one stage) and the club-record id of the team page are other key spaces. person_id is one person across clubs and seasons.

> **Note:** Errors are HTTP 200: an unknown id answers {MessageType, Message}, an unknown game the string 'BadRequest'; both are AssetFetchError (a failed fetch), never an empty frame. A body for another game, person, team, competition or season than the one asked for is AssetFetchError too, as is a game that is not final (box score, plays and header need GameStatus 1) and a partial GetTeamStats page. An empty list is NoDataError.

> **Note:** Coverage: calendars and box scores from 2009-10; play-by-play and shot x/y from 2022-23 only (earlier games give an empty pbp frame), officials in the line-ups from 2022-23. Shot x/y are decimetres from the basket centre (orientation unverified). Counting statistics are null instead of 0 on the wire (the parsers fill 0), percentages and averages are comma-decimal strings (the parsers return floats), the paint statistics are broken and dropped, and dates come in three encodings (the parsers return ISO strings; .NET instants as Moscow time with its +03:00 offset).

> **Note:** Personal data: the runtime removes licence numbers, contact and operator-account fields for everyone and the birth dates, birth years and ages of coaches, referees and other officials from every body (also with return_parsed=False). Players' public bio (birth date, height, weight, nationality) is kept.

> **Note:** Stage CompIDs: the team routes (TeamRoster, TeamStats, TeamGames) also accept a stage CompID; pass it as params={'compId': 50720} instead of season. Other tags (vtbyouth, wbc, wpremier, rfb) answer through the tag argument; only vtb was captured.

## Best

| Function | Summary |
|---|---|
| [vtb_infobasket_best_players](vtb_infobasket/best.md#vtb_infobasket_best_players) | Player leaders of a season for the first `count` leader categories. |
| [vtb_infobasket_best_teams](vtb_infobasket/best.md#vtb_infobasket_best_teams) | Team leaders of a season for the first `count` leader categories. |
| [vtb_infobasket_best_player_parameters](vtb_infobasket/best.md#vtb_infobasket_best_player_parameters) | Player leader categories (a code table). |
| [vtb_infobasket_best_team_parameters](vtb_infobasket/best.md#vtb_infobasket_best_team_parameters) | Team leader categories (a code table). |

## Player

| Function | Summary |
|---|---|
| [vtb_infobasket_player](vtb_infobasket/player.md#vtb_infobasket_player) | Player profile: names, birth date, height, weight, position. |
| [vtb_infobasket_player_season_stats](vtb_infobasket/player.md#vtb_infobasket_player_season_stats) | A player's season-by-season totals in the competition tag. |
| [vtb_infobasket_player_career_stats](vtb_infobasket/player.md#vtb_infobasket_player_career_stats) | A player's career totals in the competition tag. |
| [vtb_infobasket_player_game_log](vtb_infobasket/player.md#vtb_infobasket_player_game_log) | A player's game log for one season and one team, one row per game. |

## Team

| Function | Summary |
|---|---|
| [vtb_infobasket_team_stats](vtb_infobasket/team.md#vtb_infobasket_team_stats) | Season totals of every team of a season (all stages). |
| [vtb_infobasket_team_stats_players](vtb_infobasket/team.md#vtb_infobasket_team_stats_players) | Season totals of every player of every team of a season. |
| [vtb_infobasket_team_stats_coaches](vtb_infobasket/team.md#vtb_infobasket_team_stats_coaches) | Technical fouls and games of every coach of every team of a season. |
| [vtb_infobasket_team_preview](vtb_infobasket/team.md#vtb_infobasket_team_preview) | Season totals of a game's two teams (the pre-game preview). |
| [vtb_infobasket_team_box_score](vtb_infobasket/team.md#vtb_infobasket_team_box_score) | Team box score of a finished game, with the head coach's technical fouls. |
| [vtb_infobasket_team](vtb_infobasket/team.md#vtb_infobasket_team) | Club profile: names, region, country, status. |
| [vtb_infobasket_team_seasons](vtb_infobasket/team.md#vtb_infobasket_team_seasons) | The seasons a club has data for (END year and the club's competition id). |
| [vtb_infobasket_team_comps](vtb_infobasket/team.md#vtb_infobasket_team_comps) | The competitions a club played in one season. |
| [vtb_infobasket_team_games](vtb_infobasket/team.md#vtb_infobasket_team_games) | A club's games in one season (the calendar's row shape). |
| [vtb_infobasket_team_season_stats](vtb_infobasket/team.md#vtb_infobasket_team_season_stats) | A club's season totals in one season. |
| [vtb_infobasket_team_player_stats](vtb_infobasket/team-2.md#vtb_infobasket_team_player_stats) | Season totals of every player of a club in one season. |
| [vtb_infobasket_team_coach_stats](vtb_infobasket/team-2.md#vtb_infobasket_team_coach_stats) | Technical fouls and games of every coach of a club in one season. |

## Other

| Function | Summary |
|---|---|
| [vtb_infobasket_seasons](vtb_infobasket/other.md#vtb_infobasket_seasons) | Season END year -> season-level CompID for a competition tag (the table every season-keyed wrapper maps with). |
| [vtb_infobasket_calendar](vtb_infobasket/other.md#vtb_infobasket_calendar) | Every game of a season (finished, live and planned): teams, scores, date, venue, attendance. |
| [vtb_infobasket_game](vtb_infobasket/other.md#vtb_infobasket_game) | Header of a finished game: stage, venue, rules, the two teams and their scores. |
| [vtb_infobasket_box_score](vtb_infobasket/other.md#vtb_infobasket_box_score) | Player box score of a finished game (one row per player of each team, DNPs included). |
| [vtb_infobasket_pbp](vtb_infobasket/other.md#vtb_infobasket_pbp) | Play-by-play of a finished game with shot x/y (2022-23 onwards; earlier games have no plays). |
| [vtb_infobasket_game_staff](vtb_infobasket/other.md#vtb_infobasket_game_staff) | The coaches and officials of a finished game (names and roles only). |
| [vtb_infobasket_game_page](vtb_infobasket/other.md#vtb_infobasket_game_page) | Thin header of a finished game (use vtb_infobasket_game for teams, box score and plays). |
| [vtb_infobasket_comp_issue](vtb_infobasket/other.md#vtb_infobasket_comp_issue) | Stage tree of a season (regular season, playoff, series): the stage ids the standings wrappers take. |
| [vtb_infobasket_standings](vtb_infobasket/other.md#vtb_infobasket_standings) | Standings of one stage (a round-robin stage such as the regular season), one row per team. |
| [vtb_infobasket_cross_table](vtb_infobasket/other.md#vtb_infobasket_cross_table) | Head-to-head results grid of a round-robin stage (every game twice, once from each side). |
| [vtb_infobasket_playoff](vtb_infobasket/other.md#vtb_infobasket_playoff) | Playoff bracket of a playoff stage, one row per series. |
| [vtb_infobasket_playoff_games](vtb_infobasket/other.md#vtb_infobasket_playoff_games) | The games of every series of a playoff stage. |
| [vtb_infobasket_coach](vtb_infobasket/other.md#vtb_infobasket_coach) | Coach profile: names, height, weight (no birth data). |
| [vtb_infobasket_roster](vtb_infobasket/other.md#vtb_infobasket_roster) | A club's player roster in one season. |
| [vtb_infobasket_roster_coaches](vtb_infobasket/other.md#vtb_infobasket_roster_coaches) | A club's coaches in one season (no birth data). |
