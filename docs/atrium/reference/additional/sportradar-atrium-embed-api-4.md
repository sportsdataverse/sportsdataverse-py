# Sportradar Atrium — additional Python functions — Sportradar Atrium embed API: nbl1_standings–nbl1_teams

> Sportradar Atrium — additional Python functions — Sportradar Atrium embed API: nbl1_standings–nbl1_teams — function reference in sdv-py, the SportsDataverse Python package.

### nbl1_standings {#nbl1_standings}

`nbl1_standings(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Standings of a NBL1 (Australia) season.

NBL1 (Australia) (men's and women's); season rule: calendar year, Mar-Aug (END year); conferences are competition_ids.

Convenience wrapper: a thin binding of `atrium_standings` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: Unofficial: this is the public endpoint the Sportradar Atrium / Synergy `Connect` widget calls from every visitor's browser (keyless; the host has no robots.txt). The data is Sportradar's and the leagues'. The wrappers read it on the caller's behalf, one request at least 1 s after the last, and never republish it as release assets. Keep crawls small and incremental. Women's competition data: flagged for the wehoop mirror (Phase C).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season END year (see the season rule above). Omit season and season_id for the site's current season. |
| `season_id` | `Optional[str]` | `None` | Season UUID (season_id in the seasons listing); skips the END-year lookup. |
| `competition_id` | `Optional[str]` | `None` | Competition UUID (a site carries many: conferences, cups); needed when several share the season. |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | String | Season UUID of the table. |
| `season_end_year` | Int64 | Season END year (the sdv key): the site's year + 1 on LNB (site 12), the calendar year on the other registered sites; null when the site is unregistered, lists no seasons or the body has no season list. |
| `group` | String | Label of the table when the standings hold several groups; null for a single table. |
| `position` | Int64 | League table position (1 = first). |
| `entity_id` | String | Team UUID of the row. |
| `team_name` | String | Team name as the site prints it. |
| `is_current_team` | Boolean | True for the team the page was opened for (team context); false on a plain standings request. |
| `logo_url` | String | URL of the team logo image. |
| `wins` | Int64 | Games won; -1 for two teams in the 2025-26 LNB table (an administrative ranking, unverified), kept as sent (team, this table). |
| `losses` | Int64 | Games lost (team, this table). |
| `played` | Int64 | Games played (LNBP and the compact standings) (team, this table). |
| `standing_points` | Int64 | League table points (LNBP) (team, this table). |
| `win_percentage` | Float64 | Share of games won as a 0-1 fraction (not 0-100) (team, this table). |
| `scored_for` | Int64 | Points scored over the season (team, this table). |
| `scored_against` | Int64 | Points conceded over the season (team, this table). |
| `point_difference` | Int64 | Point differential over the season (team, this table). |
| `result_string_home` | String | Home results as a string of result letters in the site's language, one per game (order and letters per locale; unverified) (team, this table). |
| `scored_for_home` | Int64 | Points scored in home games (team, this table). |
| `scored_against_home` | Int64 | Points conceded in home games (team, this table). |
| `point_difference_home` | Int64 | Point differential in home games (team, this table). |
| `result_string_away` | String | Away results as a string of result letters in the site's language, one per game (order and letters per locale; unverified) (team, this table). |
| `scored_for_away` | Int64 | Points scored in away games (team, this table). |
| `scored_against_away` | Int64 | Points conceded in away games (team, this table). |
| `point_difference_away` | Int64 | Point differential in away games (team, this table). |
| `streak` | Int64 | Length of the current run of results (the win / loss sign convention is unverified) (team, this table). |
| `streak_home` | Int64 | Length of the current run of home results (sign convention unverified) (team, this table). |
| `streak_away` | Int64 | Length of the current run of away results (sign convention unverified) (team, this table). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import nbl1_standings
nbl1_standings(season=2026)
```

### nbl1_team_box {#nbl1_team_box}

`nbl1_team_box(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Team box score of a NBL1 (Australia) game.

NBL1 (Australia) (men's and women's); season rule: calendar year, Mar-Aug (END year); conferences are competition_ids.

Convenience wrapper: a thin binding of `atrium_team_box_score` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: Unofficial: this is the public endpoint the Sportradar Atrium / Synergy `Connect` widget calls from every visitor's browser (keyless; the host has no robots.txt). The data is Sportradar's and the leagues'. The wrappers read it on the caller's behalf, one request at least 1 s after the last, and never republish it as release assets. Keep crawls small and incremental. Women's competition data: flagged for the wehoop mirror (Phase C).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fixture_id` | `str` |  | Fixture UUID (fixture_id in the schedule). |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `fixture_id` | String | Game UUID (fixture); the key of every game frame and of the fixture_detail routes. |
| `season_id` | String | Season UUID of the game. |
| `entity_id` | String | Team UUID (from the fixture's isHome flag). |
| `side` | String | Side of the game the row belongs to: home or away (from the fixture's isHome flag). |
| `minutes` | String | Time played as an ISO 8601 duration (PT7M23S); a team's is the sum over its players (PT200M) (team, this game). |
| `seconds_played` | Int64 | Time played in whole seconds, parsed from minutes (team, this game). |
| `points` | Int64 | Points scored (team, this game). |
| `rebounds` | Int64 | Total rebounds (team, this game). |
| `rebounds_offensive` | Int64 | Offensive rebounds (team, this game). |
| `rebounds_defensive` | Int64 | Defensive rebounds (team, this game). |
| `assists` | Int64 | Assists (team, this game). |
| `steals` | Int64 | Steals (team, this game). |
| `blocks` | Int64 | Shots blocked (team, this game). |
| `blocks_received` | Int64 | Own shots blocked by an opponent (LNB only) (team, this game). |
| `turnovers` | Int64 | Turnovers (team, this game). |
| `fouls_total` | Int64 | Fouls committed (team, this game). |
| `fouls_drawn` | Int64 | Fouls drawn (committed against the player or team; LNBP only) (team, this game). |
| `plus_minus` | Int64 | Plus/minus, the point differential while on court (LNB only; null in some team lines) (team, this game). |
| `efficiency` | Int64 | Efficiency rating, from the site's own formula (team, this game). |
| `field_goals_made` | Int64 | Field goals made, two- and three-pointers together (LNBP only) (team, this game). |
| `field_goals_attempted` | Int64 | Field goals attempted (LNBP only) (team, this game). |
| `field_goals_percentage` | Float64 | Field-goal percentage, 0-100 (LNBP only) (team, this game). |
| `points_two_made` | Int64 | Two-point field goals made (team, this game). |
| `points_two_attempted` | Int64 | Two-point field goals attempted (team, this game). |
| `points_two_percentage` | Float64 | Two-point percentage, 0-100 (team, this game). |
| `points_three_made` | Int64 | Three-point field goals made (team, this game). |
| `points_three_attempted` | Int64 | Three-point field goals attempted (team, this game). |
| `points_three_percentage` | Float64 | Three-point percentage, 0-100 (team, this game). |
| `free_throws_made` | Int64 | Free throws made (team, this game). |
| `free_throws_attempted` | Int64 | Free throws attempted (team, this game). |
| `free_throws_percentage` | Float64 | Free-throw percentage, 0-100 (team, this game). |
| `points_fast_break` | Int64 | Fast-break points (LNBP only) (team, this game). |
| `extra_fouls_total` | Int64 | Fouls committed (team, credited to the team rather than a player). |
| `extra_rebounds` | Int64 | Total rebounds (team, credited to the team rather than a player). |
| `extra_rebounds_defensive` | Int64 | Defensive rebounds (team, credited to the team rather than a player). |
| `extra_rebounds_offensive` | Int64 | Offensive rebounds (team, credited to the team rather than a player). |
| `extra_turnovers` | Int64 | Turnovers (team, credited to the team rather than a player). |
| `total_biggest_lead` | Int64 | Biggest lead of the game, in points (team, game total). |
| `total_biggest_scoring_run` | Int64 | Longest unanswered scoring run of the game, in points (team, game total). |
| `total_field_goals_percentage` | Float64 | Field-goal percentage, 0-100 (LNBP only) (team, game total). |
| `total_free_throws_percentage` | Float64 | Free-throw percentage, 0-100 (team, game total). |
| `total_lead_changes` | Int64 | Number of lead changes in the game (team, game total). |
| `total_points_fast_break` | Int64 | Fast-break points (LNBP only) (team, game total). |
| `total_points_from_bench` | Int64 | Points scored by non-starters (LNBP only) (team, game total). |
| `total_points_from_turnover` | Int64 | Points scored off opponent turnovers (LNBP only) (team, game total). |
| `total_time_in_lead` | String | Time the team led, as an ISO 8601 duration (PT36M11S) (team, game total). |
| `total_time_in_lead_seconds` | Int64 | Time the team led, in whole seconds (team, game total). |
| `total_turnovers` | Int64 | Turnovers (team, game total). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import nbl1_schedule, nbl1_team_box
games = nbl1_schedule()
nbl1_team_box(fixture_id=games['fixture_id'][0])
```

### nbl1_teams {#nbl1_teams}

`nbl1_teams(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Teams of a NBL1 (Australia) season.

NBL1 (Australia) (men's and women's); season rule: calendar year, Mar-Aug (END year); conferences are competition_ids.

Convenience wrapper: a thin binding of `atrium_teams` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: Unofficial: this is the public endpoint the Sportradar Atrium / Synergy `Connect` widget calls from every visitor's browser (keyless; the host has no robots.txt). The data is Sportradar's and the leagues'. The wrappers read it on the caller's behalf, one request at least 1 s after the last, and never republish it as release assets. Keep crawls small and incremental. Women's competition data: flagged for the wehoop mirror (Phase C).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season END year (see the season rule above). Omit season and season_id for the site's current season. |
| `season_id` | `Optional[str]` | `None` | Season UUID (season_id in the seasons listing); skips the END-year lookup. |
| `competition_id` | `Optional[str]` | `None` | Competition UUID (a site carries many: conferences, cups); needed when several share the season. |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | String | Season UUID of the team list. |
| `entity_id` | String | Team UUID (entity): one id per club across seasons, shared by every site that shows the team. |
| `name` | String | Team name in the site's language. |
| `image_url` | String | URL of the Sportradar-hosted image (400 px). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import nbl1_teams
nbl1_teams(season=2026)
```
