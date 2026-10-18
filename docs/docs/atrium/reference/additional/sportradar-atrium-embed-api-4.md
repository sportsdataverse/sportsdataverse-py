---
title: "ATRIUM — additional Python functions — Sportradar Atrium embed API: nbl1_shots–nbl1_teams"
sidebar_label: "Sportradar Atrium embed API: nbl1_shots–nbl1_teams"
sidebar_position: 4
description: "ATRIUM — additional Python functions — Sportradar Atrium embed API: nbl1_shots–nbl1_teams — function reference in sdv-py, the SportsDataverse Python package."
---
# ATRIUM — additional Python functions — Sportradar Atrium embed API: nbl1_shots–nbl1_teams

### nbl1_shots {#nbl1_shots}

`nbl1_shots(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Shot chart of a NBL1 (Australia) game.

NBL1 (Australia) (men's and women's); season rule: calendar year, Mar-Aug (END year); conferences are competition_ids.

Convenience wrapper: a thin binding of `atrium_shots` to this competition, minted from the
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
| `website_id` | integer | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `fixture_id` | character | Game UUID (fixture); the key of every game frame and of the fixture_detail routes. |
| `season_id` | character | Season UUID of the game. |
| `sequence` | integer | 1-based position of the shot among the shots of the game (not the pbp sequence). |
| `event_id` | character | Event UUID of the shot; the same event in atrium_pbp. |
| `period_id` | integer | Period number: 1-4 regulation, 11, 12, ... overtimes (not 5). |
| `clock` | character | Time REMAINING in the period as an ISO 8601 duration (PT10M0S at the start). |
| `clock_seconds` | integer | Seconds remaining in the period, parsed from clock. |
| `entity_id` | character | Team UUID the event is credited to. |
| `side` | character | Side of the game the row belongs to: home or away (from the fixture's isHome flag). |
| `person_id` | character | Player UUID of the shooter. |
| `person_name` | character | Name of the shooter. |
| `bib` | character | Jersey number of the player, as text; null for team events. |
| `event_type` | character | Event type: 2pt, 3pt, freeThrow, rebound, assist, steal, block, turnover, foul, substitution, timeOut, jumpBall, ... (language-independent). |
| `event_sub_type` | character | Event detail: jumpShot, layup, offensive, defensive, in, out, 1Of2, personal, ...; null when none. |
| `description` | character | The event's text in the site's language (e.g. Entre-deux remporte). |
| `success` | logical | True when the shot was made, false when missed. |
| `success_label` | character | The made / missed label in the site's language; sent on shot events only. |
| `x` | numeric | Shot location along the 28 m court length as percent of the court (0-100, origin at a corner); the home team attacks the low-x basket all game; LNBP and NBL1 clamp rim attempts to 5 / 95. |
| `y` | numeric | Shot location across the 15 m width as percent of the court (0-100); which sideline is 0 is unverified. |
| `home_score` | integer | Home team's running score after the event. |
| `away_score` | integer | Away team's running score after the event. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, a fixture id the site does not serve: unknown, or a game of another site, which the API answers alike with an empty game, so it is not called a wrong scope). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import nbl1_schedule, nbl1_shots
games = nbl1_schedule()
nbl1_shots(fixture_id=games['fixture_id'][0])
```

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
| `website_id` | integer | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | character | Season UUID of the table. |
| `season_end_year` | integer | Season END year (the sdv key): the site's year + 1 on LNB (site 12), the calendar year on the other registered sites; null when the site is unregistered, lists no seasons or the body has no season list. |
| `group` | character | Label of the table when the standings hold several groups; null for a single table. |
| `position` | integer | League table position (1 = first). |
| `entity_id` | character | Team UUID of the row. |
| `team_name` | character | Team name as the site prints it. |
| `is_current_team` | logical | True for the team the page was opened for (team context); false on a plain standings request. |
| `logo_url` | character | URL of the team logo image. |
| `wins` | integer | Games won; -1 for two teams in the 2025-26 LNB table (an administrative ranking, unverified), kept as sent (team, this table). |
| `losses` | integer | Games lost (team, this table). |
| `played` | integer | Games played (LNBP and the compact standings) (team, this table). |
| `standing_points` | integer | League table points (LNBP) (team, this table). |
| `win_percentage` | numeric | Share of games won as a 0-1 fraction (not 0-100) (team, this table). |
| `scored_for` | integer | Points scored over the season (team, this table). |
| `scored_against` | integer | Points conceded over the season (team, this table). |
| `point_difference` | integer | Point differential over the season (team, this table). |
| `result_string_home` | character | Home results as a string of result letters in the site's language, one per game (order and letters per locale; unverified) (team, this table). |
| `scored_for_home` | integer | Points scored in home games (team, this table). |
| `scored_against_home` | integer | Points conceded in home games (team, this table). |
| `point_difference_home` | integer | Point differential in home games (team, this table). |
| `result_string_away` | character | Away results as a string of result letters in the site's language, one per game (order and letters per locale; unverified) (team, this table). |
| `scored_for_away` | integer | Points scored in away games (team, this table). |
| `scored_against_away` | integer | Points conceded in away games (team, this table). |
| `point_difference_away` | integer | Point differential in away games (team, this table). |
| `streak` | integer | Length of the current run of results (the win / loss sign convention is unverified) (team, this table). |
| `streak_home` | integer | Length of the current run of home results (sign convention unverified) (team, this table). |
| `streak_away` | integer | Length of the current run of away results (sign convention unverified) (team, this table). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, a fixture id the site does not serve: unknown, or a game of another site, which the API answers alike with an empty game, so it is not called a wrong scope). |
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
| `website_id` | integer | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `fixture_id` | character | Game UUID (fixture); the key of every game frame and of the fixture_detail routes. |
| `season_id` | character | Season UUID of the game. |
| `entity_id` | character | Team UUID (from the fixture's isHome flag). |
| `side` | character | Side of the game the row belongs to: home or away (from the fixture's isHome flag). |
| `minutes` | character | Time played as an ISO 8601 duration (PT7M23S); a team's is the sum over its players (PT200M) (team, this game). |
| `seconds_played` | integer | Time played in whole seconds, parsed from minutes (team, this game). |
| `points` | integer | Points scored (team, this game). |
| `rebounds` | integer | Total rebounds (team, this game). |
| `rebounds_offensive` | integer | Offensive rebounds (team, this game). |
| `rebounds_defensive` | integer | Defensive rebounds (team, this game). |
| `assists` | integer | Assists (team, this game). |
| `steals` | integer | Steals (team, this game). |
| `blocks` | integer | Shots blocked (team, this game). |
| `blocks_received` | integer | Own shots blocked by an opponent (LNB only) (team, this game). |
| `turnovers` | integer | Turnovers (team, this game). |
| `fouls_total` | integer | Fouls committed (team, this game). |
| `fouls_drawn` | integer | Fouls drawn (committed against the player or team; LNBP only) (team, this game). |
| `plus_minus` | integer | Plus/minus, the point differential while on court (LNB only; null in some team lines) (team, this game). |
| `efficiency` | integer | Efficiency rating, from the site's own formula (team, this game). |
| `field_goals_made` | integer | Field goals made, two- and three-pointers together (LNBP only) (team, this game). |
| `field_goals_attempted` | integer | Field goals attempted (LNBP only) (team, this game). |
| `field_goals_percentage` | numeric | Field-goal percentage, 0-100 (LNBP only) (team, this game). |
| `points_two_made` | integer | Two-point field goals made (team, this game). |
| `points_two_attempted` | integer | Two-point field goals attempted (team, this game). |
| `points_two_percentage` | numeric | Two-point percentage, 0-100 (team, this game). |
| `points_three_made` | integer | Three-point field goals made (team, this game). |
| `points_three_attempted` | integer | Three-point field goals attempted (team, this game). |
| `points_three_percentage` | numeric | Three-point percentage, 0-100 (team, this game). |
| `free_throws_made` | integer | Free throws made (team, this game). |
| `free_throws_attempted` | integer | Free throws attempted (team, this game). |
| `free_throws_percentage` | numeric | Free-throw percentage, 0-100 (team, this game). |
| `points_fast_break` | integer | Fast-break points (LNBP only) (team, this game). |
| `extra_fouls_total` | integer | Fouls committed (team, credited to the team rather than a player). |
| `extra_rebounds` | integer | Total rebounds (team, credited to the team rather than a player). |
| `extra_rebounds_defensive` | integer | Defensive rebounds (team, credited to the team rather than a player). |
| `extra_rebounds_offensive` | integer | Offensive rebounds (team, credited to the team rather than a player). |
| `extra_turnovers` | integer | Turnovers (team, credited to the team rather than a player). |
| `total_biggest_lead` | integer | Biggest lead of the game, in points (team, game total). |
| `total_biggest_scoring_run` | integer | Longest unanswered scoring run of the game, in points (team, game total). |
| `total_field_goals_percentage` | numeric | Field-goal percentage, 0-100 (LNBP only) (team, game total). |
| `total_free_throws_percentage` | numeric | Free-throw percentage, 0-100 (team, game total). |
| `total_lead_changes` | integer | Number of lead changes in the game (team, game total). |
| `total_points_fast_break` | integer | Fast-break points (LNBP only) (team, game total). |
| `total_points_from_bench` | integer | Points scored by non-starters (LNBP only) (team, game total). |
| `total_points_from_turnover` | integer | Points scored off opponent turnovers (LNBP only) (team, game total). |
| `total_time_in_lead` | character | Time the team led, as an ISO 8601 duration (PT36M11S) (team, game total). |
| `total_time_in_lead_seconds` | integer | Time the team led, in whole seconds (team, game total). |
| `total_turnovers` | integer | Turnovers (team, game total). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, a fixture id the site does not serve: unknown, or a game of another site, which the API answers alike with an empty game, so it is not called a wrong scope). |
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
| `website_id` | integer | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | character | Season UUID of the team list. |
| `entity_id` | character | Team UUID (entity): one id per club across seasons, shared by every site that shows the team. |
| `name` | character | Team name in the site's language. |
| `image_url` | character | URL of the Sportradar-hosted image (400 px). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, a fixture id the site does not serve: unknown, or a game of another site, which the API answers alike with an empty game, so it is not called a wrong scope). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import nbl1_teams
nbl1_teams(season=2026)
```
