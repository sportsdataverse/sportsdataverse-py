---
title: "ATRIUM — additional Python functions — Sportradar Atrium embed API: lnb_standings–lpb_player"
sidebar_label: "Sportradar Atrium embed API: lnb_standings–lpb_player"
sidebar_position: 2
description: "ATRIUM — additional Python functions — Sportradar Atrium embed API: lnb_standings–lpb_player — function reference in sdv-py, the SportsDataverse Python package."
---
# ATRIUM — additional Python functions — Sportradar Atrium embed API: lnb_standings–lpb_player

### lnb_standings {#lnb_standings}

`lnb_standings(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Standings of a LNB (France) season.

LNB (France) (men's); season rule: END year (2026 = 2025-26; the site's own year is the START year).

Convenience wrapper: a thin binding of `atrium_standings` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: Unofficial: this is the public endpoint the Sportradar Atrium / Synergy `Connect` widget calls from every visitor's browser (keyless; the host has no robots.txt). The data is Sportradar's and the leagues'. The wrappers read it on the caller's behalf, one request at least 1 s after the last, and never republish it as release assets. Keep crawls small and incremental.

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
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404). |
| `AssetFetchError` | also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import lnb_standings
lnb_standings(season=2026)
```

### lnb_team_box {#lnb_team_box}

`lnb_team_box(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Team box score of a LNB (France) game.

LNB (France) (men's); season rule: END year (2026 = 2025-26; the site's own year is the START year).

Convenience wrapper: a thin binding of `atrium_team_box_score` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: Unofficial: this is the public endpoint the Sportradar Atrium / Synergy `Connect` widget calls from every visitor's browser (keyless; the host has no robots.txt). The data is Sportradar's and the leagues'. The wrappers read it on the caller's behalf, one request at least 1 s after the last, and never republish it as release assets. Keep crawls small and incremental.

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
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404). |
| `AssetFetchError` | also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import lnb_schedule, lnb_team_box
games = lnb_schedule()
lnb_team_box(fixture_id=games['fixture_id'][0])
```

### lnb_teams {#lnb_teams}

`lnb_teams(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Teams of a LNB (France) season.

LNB (France) (men's); season rule: END year (2026 = 2025-26; the site's own year is the START year).

Convenience wrapper: a thin binding of `atrium_teams` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: Unofficial: this is the public endpoint the Sportradar Atrium / Synergy `Connect` widget calls from every visitor's browser (keyless; the host has no robots.txt). The data is Sportradar's and the leagues'. The wrappers read it on the caller's behalf, one request at least 1 s after the last, and never republish it as release assets. Keep crawls small and incremental.

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
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404). |
| `AssetFetchError` | also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import lnb_teams
lnb_teams(season=2026)
```

### lnbp_pbp {#lnbp_pbp}

`lnbp_pbp(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Play-by-play of a LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) game.

LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_pbp` to this competition, minted from the
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
| `sequence` | integer | 1-based position of the event in the game (periods in order, events chronological). |
| `event_id` | character | Event UUID, shared with the same event in atrium_shots. |
| `period_id` | integer | Period number: 1-4 regulation, 11, 12, ... overtimes (not 5). |
| `clock` | character | Time REMAINING in the period as an ISO 8601 duration (PT10M0S at the start). |
| `clock_seconds` | integer | Seconds remaining in the period, parsed from clock. |
| `entity_id` | character | Team UUID the event is credited to. |
| `side` | character | Side of the game the row belongs to: home or away (from the fixture's isHome flag). |
| `person_id` | character | Player UUID; null for team events (time-outs, team rebounds). |
| `person_name` | character | Player the event is credited to; null for team events (time-outs, team rebounds). |
| `bib` | character | Jersey number of the player, as text; null for team events. |
| `event_type` | character | Event type: 2pt, 3pt, freeThrow, rebound, assist, steal, block, turnover, foul, substitution, timeOut, jumpBall, ... (language-independent). |
| `event_sub_type` | character | Event detail: jumpShot, layup, offensive, defensive, in, out, 1Of2, personal, ...; null when none. |
| `description` | character | The event's text in the site's language (e.g. Entre-deux remporte). |
| `success` | logical | True when a shot or free throw was made, false when missed; null for other events. |
| `success_label` | character | The made / missed label in the site's language; sent on shot events only. |
| `x` | numeric | Shot location along the 28 m court length as percent of the court (0-100, origin at a corner); the home team attacks the low-x basket all game; null for non-shots. |
| `y` | numeric | Shot location across the 15 m width as percent of the court (0-100); which sideline is 0 is unverified; null for non-shots. |
| `home_score` | integer | Home team's running score after the event. |
| `away_score` | integer | Away team's running score after the event. |

**Raises**

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404). |
| `AssetFetchError` | also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import lnbp_schedule, lnbp_pbp
games = lnbp_schedule()
lnbp_pbp(fixture_id=games['fixture_id'][0])
```

### lnbp_player_box {#lnbp_player_box}

`lnbp_player_box(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Player box score of a LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) game.

LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_box_score` to this competition, minted from the
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
| `entity_id` | character | Team UUID the player is rostered with for this game. |
| `side` | character | Side of the game the row belongs to: home or away (from the fixture's isHome flag). |
| `person_id` | character | Player UUID (person), shared across seasons and sites. |
| `person_name` | character | Player name as the site prints it (upper case on LNBP and NBL1). |
| `bib` | character | Jersey number the player wears, as text. |
| `position` | character | Position code on the roster (LNB, e.g. PG); null on LNBP. |
| `starter` | logical | True when the player started the game. |
| `active` | logical | True when the player is on the court in a live game. |
| `participated` | logical | True when the player entered the game; false rows have null statistics. |
| `did_not_play_reason` | character | Reason the player did not play; null in every body seen. |
| `minutes` | character | Time played as an ISO 8601 duration (PT7M23S); a team's is the sum over its players (PT200M) (player, this game). |
| `seconds_played` | integer | Time played in whole seconds, parsed from minutes (player, this game). |
| `points` | integer | Points scored (player, this game). |
| `rebounds` | integer | Total rebounds (player, this game). |
| `rebounds_offensive` | integer | Offensive rebounds (player, this game). |
| `rebounds_defensive` | integer | Defensive rebounds (player, this game). |
| `assists` | integer | Assists (player, this game). |
| `steals` | integer | Steals (player, this game). |
| `blocks` | integer | Shots blocked (player, this game). |
| `blocks_received` | integer | Own shots blocked by an opponent (LNB only) (player, this game). |
| `turnovers` | integer | Turnovers (player, this game). |
| `fouls_total` | integer | Fouls committed (player, this game). |
| `fouls_drawn` | integer | Fouls drawn (committed against the player or team; LNBP only) (player, this game). |
| `plus_minus` | integer | Plus/minus, the point differential while on court (LNB only; null in some team lines) (player, this game). |
| `efficiency` | integer | Efficiency rating, from the site's own formula (player, this game). |
| `field_goals_made` | integer | Field goals made, two- and three-pointers together (LNBP only) (player, this game). |
| `field_goals_attempted` | integer | Field goals attempted (LNBP only) (player, this game). |
| `field_goals_percentage` | numeric | Field-goal percentage, 0-100 (LNBP only) (player, this game). |
| `points_two_made` | integer | Two-point field goals made (player, this game). |
| `points_two_attempted` | integer | Two-point field goals attempted (player, this game). |
| `points_two_percentage` | numeric | Two-point percentage, 0-100 (player, this game). |
| `points_three_made` | integer | Three-point field goals made (player, this game). |
| `points_three_attempted` | integer | Three-point field goals attempted (player, this game). |
| `points_three_percentage` | numeric | Three-point percentage, 0-100 (player, this game). |
| `free_throws_made` | integer | Free throws made (player, this game). |
| `free_throws_attempted` | integer | Free throws attempted (player, this game). |
| `free_throws_percentage` | numeric | Free-throw percentage, 0-100 (player, this game). |
| `points_fast_break` | integer | Fast-break points (LNBP only) (player, this game). |

**Raises**

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404). |
| `AssetFetchError` | also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import lnbp_schedule, lnbp_player_box
games = lnbp_schedule()
lnbp_player_box(fixture_id=games['fixture_id'][0])
```

### lnbp_schedule {#lnbp_schedule}

`lnbp_schedule(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Fixtures (schedule and results) of a LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) season.

LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_fixtures` to this competition, minted from the
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
| `season_id` | character | Season UUID the page is about (the site's default season when none was asked). |
| `competition_id` | character | Competition UUID of the season. |
| `competition_name` | character | Competition name in the site's language, e.g. Betclic ELITE or LIGA CALIENTE.MX LNBP. |
| `season_end_year` | integer | Season END year (the sdv key): the site's year + 1 on LNB (site 12), the calendar year on the other registered sites; null when the site is unregistered, lists no seasons or the body has no season list. |
| `fixture_id` | character | Game UUID (fixture); the key of every game frame and of the fixture_detail routes. |
| `fixture_type` | character | Kind of game as sent: REGULAR, PLAYOFF, ... (the set is open). |
| `name` | character | Fixture name as the site prints it (e.g. Paris - Lyon-Villeurbanne); null on LNBP. |
| `round` | character | Round of the game as the site words it, e.g. Journee: 30. |
| `round_code` | character | Round code; null in every body seen. |
| `pool` | character | Pool the game belongs to; null when the competition has none. |
| `series` | character | Playoff series the game belongs to as sent (JSON text when an object); null outside a series. |
| `stage_code` | character | Opaque 30-hex stage code of the game; null in every body seen. |
| `status` | character | Status code of the game, e.g. CONFIRMED (final), as sent. |
| `status_label` | character | Status of the game as the site words it in its own language (e.g. Termine, Final). |
| `start_time_utc` | datetime | Scheduled start, naive datetime in UTC (the API sends no offset). |
| `start_time_local` | datetime | Scheduled start, naive datetime in the venue's local time (timezone gives the zone; no offset sent). |
| `timezone` | character | IANA time zone of the venue (e.g. Europe/Paris, Mexico/General) that start_time_local is in. |
| `times_unconfirmed` | logical | True while the start time is not final. |
| `venue` | character | Name of the arena where the game is played. |
| `venue_id` | character | Venue UUID; joins no other route. |
| `attendance` | integer | Spectators at the game; null when not reported. |
| `home_entity_id` | character | Team UUID of the home team. |
| `home_code` | character | Short code of the home team (e.g. PAB). |
| `home_name` | character | Name of the home team. |
| `home_score` | integer | Points scored by the home team (the API sends a string; Int64 here). |
| `home_result_place` | integer | Result place of the home team: 1 for the winner, 2 for the loser. |
| `home_result_status` | character | Result status of the home team's score, e.g. CONFIRMED. |
| `home_draw` | logical | True when the game ended level for the home team. |
| `home_shootout_attempts` | character | Shootout attempts of the home team as sent (an empty string becomes null). |
| `home_secondary_score` | character | Secondary score of the home team (unused in the samples; shown as text). |
| `away_entity_id` | character | Team UUID of the away team. |
| `away_code` | character | Short code of the away team (e.g. PAB). |
| `away_name` | character | Name of the away team. |
| `away_score` | integer | Points scored by the away team (the API sends a string; Int64 here). |
| `away_result_place` | integer | Result place of the away team: 1 for the winner, 2 for the loser. |
| `away_result_status` | character | Result status of the away team's score, e.g. CONFIRMED. |
| `away_draw` | logical | True when the game ended level for the away team. |
| `away_shootout_attempts` | character | Shootout attempts of the away team as sent (an empty string becomes null). |
| `away_secondary_score` | character | Secondary score of the away team (unused in the samples; shown as text). |

**Raises**

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404). |
| `AssetFetchError` | also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import lnbp_schedule
lnbp_schedule(season=2026)
```

### lnbp_seasons {#lnbp_seasons}

`lnbp_seasons(locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Every season and competition id of LNBP (Mexico, with COPA VALUE and LNBP FEMENIL).

LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_seasons` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: Unofficial: this is the public endpoint the Sportradar Atrium / Synergy `Connect` widget calls from every visitor's browser (keyless; the host has no robots.txt). The data is Sportradar's and the leagues'. The wrappers read it on the caller's behalf, one request at least 1 s after the last, and never republish it as release assets. Keep crawls small and incremental. Women's competition data: flagged for the wehoop mirror (Phase C).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `website_id` | integer | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | character | Season UUID; pass it as season_id to any season-scoped wrapper. |
| `competition_id` | character | Competition UUID; pass it as competition_id to pick one of several seasons of an END year. |
| `competition_name` | character | Competition name in the site's language; the same name can appear for several years. |
| `season_name` | character | Season name in the site's language, e.g. Betclic ELITE 2025. |
| `abbreviation_local` | character | Local abbreviation of the season; null in every body seen. |
| `year` | integer | The site's own year of the season: the START year on LNB (site 12), the calendar year elsewhere. |
| `season_end_year` | integer | Season END year (the sdv key): the site's year + 1 on LNB (site 12), the calendar year on the other registered sites; null when the site is unregistered, lists no seasons or the body has no season list. |
| `is_default_season` | logical | True for the season the body is about (the site's default when none was asked). |

**Raises**

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404). |
| `AssetFetchError` | also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import lnbp_seasons
lnbp_seasons()
```

### lnbp_shots {#lnbp_shots}

`lnbp_shots(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Shot chart of a LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) game.

LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) (men's and women's); season rule: calendar year.

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
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404). |
| `AssetFetchError` | also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import lnbp_schedule, lnbp_shots
games = lnbp_schedule()
lnbp_shots(fixture_id=games['fixture_id'][0])
```

### lnbp_standings {#lnbp_standings}

`lnbp_standings(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Standings of a LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) season.

LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) (men's and women's); season rule: calendar year.

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
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404). |
| `AssetFetchError` | also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import lnbp_standings
lnbp_standings(season=2026)
```

### lnbp_team_box {#lnbp_team_box}

`lnbp_team_box(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Team box score of a LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) game.

LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) (men's and women's); season rule: calendar year.

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
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404). |
| `AssetFetchError` | also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import lnbp_schedule, lnbp_team_box
games = lnbp_schedule()
lnbp_team_box(fixture_id=games['fixture_id'][0])
```

### lnbp_teams {#lnbp_teams}

`lnbp_teams(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Teams of a LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) season.

LNBP (Mexico, with COPA VALUE and LNBP FEMENIL) (men's and women's); season rule: calendar year.

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
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404). |
| `AssetFetchError` | also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import lnbp_teams
lnbp_teams(season=2026)
```

### lpb_pbp {#lpb_pbp}

`lpb_pbp(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Play-by-play of a LPB / LPBF Liga Profesional de Baloncesto (Panama) game.

LPB / LPBF Liga Profesional de Baloncesto (Panama) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_pbp` to this competition, minted from the
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
| `sequence` | integer | 1-based position of the event in the game (periods in order, events chronological). |
| `event_id` | character | Event UUID, shared with the same event in atrium_shots. |
| `period_id` | integer | Period number: 1-4 regulation, 11, 12, ... overtimes (not 5). |
| `clock` | character | Time REMAINING in the period as an ISO 8601 duration (PT10M0S at the start). |
| `clock_seconds` | integer | Seconds remaining in the period, parsed from clock. |
| `entity_id` | character | Team UUID the event is credited to. |
| `side` | character | Side of the game the row belongs to: home or away (from the fixture's isHome flag). |
| `person_id` | character | Player UUID; null for team events (time-outs, team rebounds). |
| `person_name` | character | Player the event is credited to; null for team events (time-outs, team rebounds). |
| `bib` | character | Jersey number of the player, as text; null for team events. |
| `event_type` | character | Event type: 2pt, 3pt, freeThrow, rebound, assist, steal, block, turnover, foul, substitution, timeOut, jumpBall, ... (language-independent). |
| `event_sub_type` | character | Event detail: jumpShot, layup, offensive, defensive, in, out, 1Of2, personal, ...; null when none. |
| `description` | character | The event's text in the site's language (e.g. Entre-deux remporte). |
| `success` | logical | True when a shot or free throw was made, false when missed; null for other events. |
| `success_label` | character | The made / missed label in the site's language; sent on shot events only. |
| `x` | numeric | Shot location along the 28 m court length as percent of the court (0-100, origin at a corner); the home team attacks the low-x basket all game; null for non-shots. |
| `y` | numeric | Shot location across the 15 m width as percent of the court (0-100); which sideline is 0 is unverified; null for non-shots. |
| `home_score` | integer | Home team's running score after the event. |
| `away_score` | integer | Away team's running score after the event. |

**Raises**

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404). |
| `AssetFetchError` | also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import lpb_schedule, lpb_pbp
games = lpb_schedule()
lpb_pbp(fixture_id=games['fixture_id'][0])
```

### lpb_player_box {#lpb_player_box}

`lpb_player_box(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Player box score of a LPB / LPBF Liga Profesional de Baloncesto (Panama) game.

LPB / LPBF Liga Profesional de Baloncesto (Panama) (men's and women's); season rule: calendar year.

Convenience wrapper: a thin binding of `atrium_box_score` to this competition, minted from the
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
| `entity_id` | character | Team UUID the player is rostered with for this game. |
| `side` | character | Side of the game the row belongs to: home or away (from the fixture's isHome flag). |
| `person_id` | character | Player UUID (person), shared across seasons and sites. |
| `person_name` | character | Player name as the site prints it (upper case on LNBP and NBL1). |
| `bib` | character | Jersey number the player wears, as text. |
| `position` | character | Position code on the roster (LNB, e.g. PG); null on LNBP. |
| `starter` | logical | True when the player started the game. |
| `active` | logical | True when the player is on the court in a live game. |
| `participated` | logical | True when the player entered the game; false rows have null statistics. |
| `did_not_play_reason` | character | Reason the player did not play; null in every body seen. |
| `minutes` | character | Time played as an ISO 8601 duration (PT7M23S); a team's is the sum over its players (PT200M) (player, this game). |
| `seconds_played` | integer | Time played in whole seconds, parsed from minutes (player, this game). |
| `points` | integer | Points scored (player, this game). |
| `rebounds` | integer | Total rebounds (player, this game). |
| `rebounds_offensive` | integer | Offensive rebounds (player, this game). |
| `rebounds_defensive` | integer | Defensive rebounds (player, this game). |
| `assists` | integer | Assists (player, this game). |
| `steals` | integer | Steals (player, this game). |
| `blocks` | integer | Shots blocked (player, this game). |
| `blocks_received` | integer | Own shots blocked by an opponent (LNB only) (player, this game). |
| `turnovers` | integer | Turnovers (player, this game). |
| `fouls_total` | integer | Fouls committed (player, this game). |
| `fouls_drawn` | integer | Fouls drawn (committed against the player or team; LNBP only) (player, this game). |
| `plus_minus` | integer | Plus/minus, the point differential while on court (LNB only; null in some team lines) (player, this game). |
| `efficiency` | integer | Efficiency rating, from the site's own formula (player, this game). |
| `field_goals_made` | integer | Field goals made, two- and three-pointers together (LNBP only) (player, this game). |
| `field_goals_attempted` | integer | Field goals attempted (LNBP only) (player, this game). |
| `field_goals_percentage` | numeric | Field-goal percentage, 0-100 (LNBP only) (player, this game). |
| `points_two_made` | integer | Two-point field goals made (player, this game). |
| `points_two_attempted` | integer | Two-point field goals attempted (player, this game). |
| `points_two_percentage` | numeric | Two-point percentage, 0-100 (player, this game). |
| `points_three_made` | integer | Three-point field goals made (player, this game). |
| `points_three_attempted` | integer | Three-point field goals attempted (player, this game). |
| `points_three_percentage` | numeric | Three-point percentage, 0-100 (player, this game). |
| `free_throws_made` | integer | Free throws made (player, this game). |
| `free_throws_attempted` | integer | Free throws attempted (player, this game). |
| `free_throws_percentage` | numeric | Free-throw percentage, 0-100 (player, this game). |
| `points_fast_break` | integer | Fast-break points (LNBP only) (player, this game). |

**Raises**

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404). |
| `AssetFetchError` | also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import lpb_schedule, lpb_player_box
games = lpb_schedule()
lpb_player_box(fixture_id=games['fixture_id'][0])
```
