# NFL — NFL.com API

> NFL — NFL.com API — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.nfl` — 15 endpoints.

## Game

| Function | Summary |
|---|---|
| [nfl_game_summaries](nfl_api/game.md#nfl_game_summaries) | GET /football/v2/stats/live/game-summaries — one row per game (live state). |
| [nfl_game_details_v2](nfl_api/game.md#nfl_game_details_v2) | GET /experience/v2/gamedetails/{game_id} — one row: the flat v2 game detail (game, summary, optional drive chart / replays / standings). |
| [nfl_game_details_by_slug](nfl_api/game.md#nfl_game_details_by_slug) | GET /experience/v1/gamedetailsbyslug/{slug} — one row: the flat game detail looked up by nfl.com slug. |

## Live

| Function | Summary |
|---|---|
| [nfl_live_team_statistics](nfl_api/live.md#nfl_live_team_statistics) | GET /football/v2/stats/live/team-statistics/{game_id} — one row per side (away, home): the live team box score. |
| [nfl_live_player_statistics](nfl_api/live.md#nfl_live_player_statistics) | GET /football/v2/stats/live/player-statistics/{game_id} — one row per player per side: the live player box score. |

## Other

| Function | Summary |
|---|---|
| [nfl_standings](nfl_api/other.md#nfl_standings) | GET /football/v2/standings — one row per team standing across the returned week(s). |
| [nfl_rosters](nfl_api/other.md#nfl_rosters) | GET /football/v2/rosters — one row per team roster for the season. |
| [nfl_teams_history](nfl_api/other.md#nfl_teams_history) | GET /football/v2/teams/history — one row per team for a season. |
| [nfl_team](nfl_api/other.md#nfl_team) | GET /football/v2/teams/{team_id} — single-team detail (one row). |
| [nfl_weeks](nfl_api/other.md#nfl_weeks) | GET /football/v2/weeks/season/{season}/seasonType/{season_type} — week calendar (one row per week). |
| [nfl_weeks_by_date](nfl_api/other.md#nfl_weeks_by_date) | GET /football/v2/weeks/date/{YYYY-MM-DD} — the week containing a date (one row). |
| [nfl_combine_profiles](nfl_api/other.md#nfl_combine_profiles) | GET /football/v2/combine/profiles — one row per combine prospect. |
| [nfl_draft_picks](nfl_api/other.md#nfl_draft_picks) | GET /football/v2/draft/picks/report — one row per draft pick. |
| [nfl_injuries](nfl_api/other.md#nfl_injuries) | GET /football/v2/injuries — one row per injured player. |
| [nfl_weekly_game_details](nfl_api/other.md#nfl_weekly_game_details) | GET /football/v2/experience/weekly-game-details — one row per game (bare list). |
