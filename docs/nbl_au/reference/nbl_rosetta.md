# NBL Australia (Rosetta) — NBL / WNBL / NBL1 Rosetta proxy (prod.rosetta.nbl.com.au, Australia)

> NBL Australia (Rosetta) — NBL / WNBL / NBL1 Rosetta proxy (prod.rosetta.nbl.com.au, Australia) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.basketball.nbl_au` — 22 endpoints.

> **Note:** Unofficial: the JSON proxy behind nbl.com.au (not an API the NBL documents or supports). The terms of use were not reviewed; payloads are not redistributed as release assets.

> **Note:** Gated by a referer, not a key: every request carries Origin / Referer set to nbl.com.au (the runtime sends them); without them the host answers 403 MISSING_API_KEY, which is raised as AssetFetchError.

> **Note:** Fills a hole in ESPN: ESPN's nbl feed has box scores but no plays for the 2025-26 NBL season (the stats provider changed from Genius Sports to Sportradar DataCore that year). nbl_rosetta_match_pbp has the full event log with shot x/y for 2025-26 and, in the same shape, back to 2011-12 (WNBL about 2014-15). Matches from 1979 have box scores only.

> **Note:** One host serves three organisations; pass org='nbl' (default), 'wnbl' or 'nbl1'. NBL1 is the second tier (men's and women's conferences, calendar-year seasons).

> **Note:** Season = the END year (2026 = 2025-26) everywhere. Rosetta's own year is the START year for the NBL and WNBL from 1998-99 and a calendar year for NBL1 and the 1979-1997 winter seasons; the runtime maps it. Year 1998 holds two seasons (calendar 1998 and 1998-99).

> **Note:** Ids: <entity>_id columns are DataCore ids (UUID v1; join on them), rosetta_<entity>_id Rosetta's own. match_id is the DataCore fixture id every match route is keyed on.

> **Note:** Unknown ids, orgs and types answer 200 with no rows (NoDataError). Rosetta caches a body per path and serves it whatever the query string, so a page cut by someone else's limit is refused (AssetFetchError); retry after the cache entry expires.

## Match

| Function | Summary |
|---|---|
| [nbl_rosetta_match_by_id](nbl_rosetta/match.md#nbl_rosetta_match_by_id) | One match's header (the same body as nbl_rosetta_match): status, scores, venue, season and teams. |
| [nbl_rosetta_match_player_box](nbl_rosetta/match.md#nbl_rosetta_match_player_box) | One match's official player box score, one row per player line (period 0 = whole game). |
| [nbl_rosetta_match_team_box](nbl_rosetta/match.md#nbl_rosetta_match_team_box) | One match's team box score by period, one row per team and period (0 = whole game). |
| [nbl_rosetta_match_player_period_box](nbl_rosetta/match.md#nbl_rosetta_match_player_period_box) | One match's player box score by period (Rosetta-computed from the event log), one row per player and period. |
| [nbl_rosetta_match_pbp](nbl_rosetta/match.md#nbl_rosetta_match_pbp) | One match's play-by-play with shot x/y, oldest first (NBL 2025-26 included, which ESPN's nbl feed lacks). |

## Next

| Function | Summary |
|---|---|
| [nbl_rosetta_next_match](nbl_rosetta/next.md#nbl_rosetta_next_match) | The organisation's next scheduled match (empty between seasons). |
| [nbl_rosetta_next_match_team](nbl_rosetta/next.md#nbl_rosetta_next_match_team) | Next match; the team segment is ignored by Rosetta (the body is the organisation's next match). |
| [nbl_rosetta_next_matches](nbl_rosetta/next.md#nbl_rosetta_next_matches) | Each team's next matches: opponent, venue and start time. |

## Player

| Function | Summary |
|---|---|
| [nbl_rosetta_players](nbl_rosetta/player.md#nbl_rosetta_players) | Roster rows of every team in a season: player bio, jersey, position and team. |
| [nbl_rosetta_player_game_log](nbl_rosetta/player.md#nbl_rosetta_player_game_log) | A player's game log for a season: the box-score line of each game (period 0 = whole game) and the game. |
| [nbl_rosetta_player_stats](nbl_rosetta/player.md#nbl_rosetta_player_stats) | A player's season totals and averages for every season, by period. |

## Team

| Function | Summary |
|---|---|
| [nbl_rosetta_teams](nbl_rosetta/team.md#nbl_rosetta_teams) | Every team record of an organisation: current, defunct, exhibition and development sides. |
| [nbl_rosetta_team_players](nbl_rosetta/team.md#nbl_rosetta_team_players) | Roster of one team in a season: player bio, jersey and position. |
| [nbl_rosetta_team_stats_leaders](nbl_rosetta/team.md#nbl_rosetta_team_stats_leaders) | Player season totals of one team in a season, optionally one period and sorted. |
| [nbl_rosetta_team_stats](nbl_rosetta/team.md#nbl_rosetta_team_stats) | Team season totals and averages by period (0 = whole game, 1-4 quarters, 5 and up overtime). |

## Other

| Function | Summary |
|---|---|
| [nbl_rosetta_seasons](nbl_rosetta/other.md#nbl_rosetta_seasons) | Every season of an organisation: regular, preseason, in-season cups and test seasons, with dates. |
| [nbl_rosetta_seasons_current](nbl_rosetta/other.md#nbl_rosetta_seasons_current) | The season in progress (empty between seasons: NoDataError). |
| [nbl_rosetta_matches](nbl_rosetta/other.md#nbl_rosetta_matches) | Matches of a season: ids, teams, venue, status, scores and start time (UTC). |
| [nbl_rosetta_match](nbl_rosetta/other.md#nbl_rosetta_match) | One match's header: status, scores, clock, venue, season and teams. Box score and play-by-play: the match_* frames. |
| [nbl_rosetta_standings](nbl_rosetta/other.md#nbl_rosetta_standings) | Regular-season ladder: position, record, points for and against, home and away splits and form. |
| [nbl_rosetta_outcomes](nbl_rosetta/other.md#nbl_rosetta_outcomes) | Win-loss record and last 10, 5 and 3 results per team, keyed by team name. |
| [nbl_rosetta_stats_leaders](nbl_rosetta/other.md#nbl_rosetta_stats_leaders) | Player season totals and averages of a season (the leaderboard), one row per player and period. |
