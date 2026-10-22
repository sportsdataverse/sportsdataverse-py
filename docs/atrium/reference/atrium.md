# ATRIUM — Sportradar Atrium embed API (embed-api.eui.connect.sportradar.com)

> ATRIUM — Sportradar Atrium embed API (embed-api.eui.connect.sportradar.com) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.basketball.atrium` — 22 endpoints.

> **Note:** Unofficial: this is the public endpoint the Sportradar Atrium / Synergy `Connect` widget calls from every visitor's browser (keyless; the host has no robots.txt). The data is Sportradar's and the leagues'. The wrappers read it on the caller's behalf, one request at least 1 s after the last, and never republish it as release assets. Keep crawls small and incremental.

> **Note:** `website_id` picks the league site (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro); one site carries many competitions. Seasons are END years (`season=2026`): the site's own year is the START year on LNB and the calendar year elsewhere, and a year can hold several competitions, so pair `season` with `competition_id` (see `atrium_seasons`) or pass `season_id` directly. `atrium_end_year(website_id, year)` maps a site's own year to that END year.

> **Note:** Columns are configured per site: a statistic one site omits is null. The player and team ids are shared across sites; the player stat tables carry names only, so `person_id` there is decoded from the widget link beside each name.

## Player

| Function | Summary |
|---|---|
| [atrium_players](atrium/player.md#atrium_players) | The players of a season, with position, nationality, birth date, age and height. |
| [atrium_player](atrium/player.md#atrium_player) | A player page: bio and the season averages the card shows. |
| [atrium_player_seasons](atrium/player.md#atrium_player_seasons) | A player's per-game averages, one row per competition-season. |
| [atrium_player_game_log](atrium/player.md#atrium_player_game_log) | A player's game log: one row per game with the opponent, result and the statistics the site shows. |
| [atrium_player_stats](atrium/player.md#atrium_player_stats) | Player season statistics in long form (one row per player, table and statistic); ids decoded from the widget links. |

## Team

| Function | Summary |
|---|---|
| [atrium_team_box_score](atrium/team.md#atrium_team_box_score) | One game's team lines: the team totals, the team-level counts and the game aggregates (lead changes, biggest lead). |
| [atrium_teams](atrium/team.md#atrium_teams) | The teams of a season. |
| [atrium_team](atrium/team.md#atrium_team) | A team page (summary tab): the team and its last and next game. |
| [atrium_team_stats](atrium/team.md#atrium_team_stats) | Team season statistics in long form (one row per team, table and statistic). |

## Other

| Function | Summary |
|---|---|
| [atrium_fixtures](atrium/other.md#atrium_fixtures) | Fixtures (schedule and results) of a season, home and away side by side. |
| [atrium_seasons](atrium/other.md#atrium_seasons) | Every season (competition and year) a site lists, with the sdv END year of each. |
| [atrium_fixtures_ribbon](atrium/other.md#atrium_fixtures_ribbon) | The fixtures ribbon: recent and upcoming games across the site's competitions (empty on LNB off-season). |
| [atrium_fixture](atrium/other.md#atrium_fixture) | One game's header: teams, score, venue, status, rules (periods, overtime, shot clock) and captains. |
| [atrium_fixture_periods](atrium/other.md#atrium_fixture_periods) | One game's periods with the points each team scored in each, overtime periods included (ids 11+). |
| [atrium_box_score](atrium/other.md#atrium_box_score) | One game's box score: one row per rostered player with the statistics the site configures. |
| [atrium_pbp](atrium/other.md#atrium_pbp) | One game's play-by-play: every event with the period clock, the player and the running score. |
| [atrium_shots](atrium/other.md#atrium_shots) | One game's 2pt / 3pt attempts with their court location (percent of the court) and result. |
| [atrium_standings](atrium/other.md#atrium_standings) | Standings of a season, with the columns the site configures (LNB home / away splits, LNBP played and points). |
| [atrium_standings_summary](atrium/other.md#atrium_standings_summary) | Compact standings of a season: position, wins, losses and games played. |
| [atrium_leaders](atrium/other.md#atrium_leaders) | Season leaders per statistic (top 5 each), with their player and team ids. |
| [atrium_competition_player_stats](atrium/other.md#atrium_competition_player_stats) | Player statistics of a competition in long form (names only: the bodies carry no ids). |
| [atrium_competition_team_stats](atrium/other.md#atrium_competition_team_stats) | Team statistics of a competition in long form (names only: the bodies carry no ids). |
