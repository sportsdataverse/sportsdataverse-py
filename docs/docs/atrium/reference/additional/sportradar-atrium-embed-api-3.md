---
title: "Sportradar Atrium — additional Python functions — Sportradar Atrium embed API: lpb_player–nbl1_shots"
sidebar_label: "Sportradar Atrium embed API: lpb_player–nbl1_shots"
sidebar_position: 3
description: "Sportradar Atrium — additional Python functions — Sportradar Atrium embed API: lpb_player–nbl1_shots — function reference in sdv-py, the SportsDataverse Python package."
---
# Sportradar Atrium — additional Python functions — Sportradar Atrium embed API: lpb_player–nbl1_shots

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
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `fixture_id` | String | Game UUID (fixture); the key of every game frame and of the fixture_detail routes. |
| `season_id` | String | Season UUID of the game. |
| `entity_id` | String | Team UUID the player is rostered with for this game. |
| `side` | String | Side of the game the row belongs to: home or away (from the fixture's isHome flag). |
| `person_id` | String | Player UUID (person), shared across seasons and sites. |
| `person_name` | String | Player name as the site prints it (upper case on LNBP and NBL1). |
| `bib` | String | Jersey number the player wears, as text. |
| `position` | String | Position code on the roster (LNB, e.g. PG); null on LNBP. |
| `starter` | Boolean | True when the player started the game. |
| `active` | Boolean | True when the player is on the court in a live game. |
| `participated` | Boolean | True when the player entered the game; false rows have null statistics. |
| `did_not_play_reason` | String | Reason the player did not play; null in every body seen. |
| `minutes` | String | Time played as an ISO 8601 duration (PT7M23S); a team's is the sum over its players (PT200M) (player, this game). |
| `seconds_played` | Int64 | Time played in whole seconds, parsed from minutes (player, this game). |
| `points` | Int64 | Points scored (player, this game). |
| `rebounds` | Int64 | Total rebounds (player, this game). |
| `rebounds_offensive` | Int64 | Offensive rebounds (player, this game). |
| `rebounds_defensive` | Int64 | Defensive rebounds (player, this game). |
| `assists` | Int64 | Assists (player, this game). |
| `steals` | Int64 | Steals (player, this game). |
| `blocks` | Int64 | Shots blocked (player, this game). |
| `blocks_received` | Int64 | Own shots blocked by an opponent (LNB only) (player, this game). |
| `turnovers` | Int64 | Turnovers (player, this game). |
| `fouls_total` | Int64 | Fouls committed (player, this game). |
| `fouls_drawn` | Int64 | Fouls drawn (committed against the player or team; LNBP only) (player, this game). |
| `plus_minus` | Int64 | Plus/minus, the point differential while on court (LNB only; null in some team lines) (player, this game). |
| `efficiency` | Int64 | Efficiency rating, from the site's own formula (player, this game). |
| `field_goals_made` | Int64 | Field goals made, two- and three-pointers together (LNBP only) (player, this game). |
| `field_goals_attempted` | Int64 | Field goals attempted (LNBP only) (player, this game). |
| `field_goals_percentage` | Float64 | Field-goal percentage, 0-100 (LNBP only) (player, this game). |
| `points_two_made` | Int64 | Two-point field goals made (player, this game). |
| `points_two_attempted` | Int64 | Two-point field goals attempted (player, this game). |
| `points_two_percentage` | Float64 | Two-point percentage, 0-100 (player, this game). |
| `points_three_made` | Int64 | Three-point field goals made (player, this game). |
| `points_three_attempted` | Int64 | Three-point field goals attempted (player, this game). |
| `points_three_percentage` | Float64 | Three-point percentage, 0-100 (player, this game). |
| `free_throws_made` | Int64 | Free throws made (player, this game). |
| `free_throws_attempted` | Int64 | Free throws attempted (player, this game). |
| `free_throws_percentage` | Float64 | Free-throw percentage, 0-100 (player, this game). |
| `points_fast_break` | Int64 | Fast-break points (LNBP only) (player, this game). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import lpb_schedule, lpb_player_box
games = lpb_schedule()
lpb_player_box(fixture_id=games['fixture_id'][0])
```

### lpb_schedule {#lpb_schedule}

`lpb_schedule(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Fixtures (schedule and results) of a LPB / LPBF Liga Profesional de Baloncesto (Panama) season.

LPB / LPBF Liga Profesional de Baloncesto (Panama) (men's and women's); season rule: calendar year.

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
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | String | Season UUID the page is about (the site's default season when none was asked). |
| `competition_id` | String | Competition UUID of the season. |
| `competition_name` | String | Competition name in the site's language, e.g. Betclic ELITE or LIGA CALIENTE.MX LNBP. |
| `season_end_year` | Int64 | Season END year (the sdv key): the site's year + 1 on LNB (site 12), the calendar year on the other registered sites; null when the site is unregistered, lists no seasons or the body has no season list. |
| `fixture_id` | String | Game UUID (fixture); the key of every game frame and of the fixture_detail routes. |
| `fixture_type` | String | Kind of game as sent: REGULAR, PLAYOFF, ... (the set is open). |
| `name` | String | Fixture name as the site prints it (e.g. Paris - Lyon-Villeurbanne); null on LNBP. |
| `round` | String | Round of the game as the site words it, e.g. Journee: 30. |
| `round_code` | String | Round code; null in every body seen. |
| `pool` | String | Pool the game belongs to; null when the competition has none. |
| `series` | String | Playoff series the game belongs to as sent (JSON text when an object); null outside a series. |
| `stage_code` | String | Opaque 30-hex stage code of the game; null in every body seen. |
| `status` | String | Status code of the game, e.g. CONFIRMED (final), as sent. |
| `status_label` | String | Status of the game as the site words it in its own language (e.g. Termine, Final). |
| `start_time_utc` | Datetime | Scheduled start, naive datetime in UTC (the API sends no offset). |
| `start_time_local` | Datetime | Scheduled start, naive datetime in the venue's local time (timezone gives the zone; no offset sent). |
| `timezone` | String | IANA time zone of the venue (e.g. Europe/Paris, Mexico/General) that start_time_local is in. |
| `times_unconfirmed` | Boolean | True while the start time is not final. |
| `venue` | String | Name of the arena where the game is played. |
| `venue_id` | String | Venue UUID; joins no other route. |
| `attendance` | Int64 | Spectators at the game; null when not reported. |
| `home_entity_id` | String | Team UUID of the home team. |
| `home_code` | String | Short code of the home team (e.g. PAB). |
| `home_name` | String | Name of the home team. |
| `home_score` | Int64 | Points scored by the home team (the API sends a string; Int64 here). |
| `home_result_place` | Int64 | Result place of the home team: 1 for the winner, 2 for the loser. |
| `home_result_status` | String | Result status of the home team's score, e.g. CONFIRMED. |
| `home_draw` | Boolean | True when the game ended level for the home team. |
| `home_shootout_attempts` | String | Shootout attempts of the home team as sent (an empty string becomes null). |
| `home_secondary_score` | String | Secondary score of the home team (unused in the samples; shown as text). |
| `away_entity_id` | String | Team UUID of the away team. |
| `away_code` | String | Short code of the away team (e.g. PAB). |
| `away_name` | String | Name of the away team. |
| `away_score` | Int64 | Points scored by the away team (the API sends a string; Int64 here). |
| `away_result_place` | Int64 | Result place of the away team: 1 for the winner, 2 for the loser. |
| `away_result_status` | String | Result status of the away team's score, e.g. CONFIRMED. |
| `away_draw` | Boolean | True when the game ended level for the away team. |
| `away_shootout_attempts` | String | Shootout attempts of the away team as sent (an empty string becomes null). |
| `away_secondary_score` | String | Secondary score of the away team (unused in the samples; shown as text). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import lpb_schedule
lpb_schedule(season=2026)
```

### lpb_seasons {#lpb_seasons}

`lpb_seasons(locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Every season and competition id of LPB / LPBF Liga Profesional de Baloncesto (Panama).

LPB / LPBF Liga Profesional de Baloncesto (Panama) (men's and women's); season rule: calendar year.

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
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | String | Season UUID; pass it as season_id to any season-scoped wrapper. |
| `competition_id` | String | Competition UUID; pass it as competition_id to pick one of several seasons of an END year. |
| `competition_name` | String | Competition name in the site's language; the same name can appear for several years. |
| `season_name` | String | Season name in the site's language, e.g. Betclic ELITE 2025. |
| `abbreviation_local` | String | Local abbreviation of the season; null in every body seen. |
| `year` | Int64 | The site's own year of the season: the START year on LNB (site 12), the calendar year elsewhere. |
| `season_end_year` | Int64 | Season END year (the sdv key): the site's year + 1 on LNB (site 12), the calendar year on the other registered sites; null when the site is unregistered, lists no seasons or the body has no season list. |
| `is_default_season` | Boolean | True for the season the body is about (the site's default when none was asked). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import lpb_seasons
lpb_seasons()
```

### lpb_shots {#lpb_shots}

`lpb_shots(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Shot chart of a LPB / LPBF Liga Profesional de Baloncesto (Panama) game.

LPB / LPBF Liga Profesional de Baloncesto (Panama) (men's and women's); season rule: calendar year.

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
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `fixture_id` | String | Game UUID (fixture); the key of every game frame and of the fixture_detail routes. |
| `season_id` | String | Season UUID of the game. |
| `sequence` | Int64 | 1-based position of the shot among the shots of the game (not the pbp sequence). |
| `event_id` | String | Event UUID of the shot; the same event in atrium_pbp. |
| `period_id` | Int64 | Period number: 1-4 regulation, 11, 12, ... overtimes (not 5). |
| `clock` | String | Time REMAINING in the period as an ISO 8601 duration (PT10M0S at the start). |
| `clock_seconds` | Int64 | Seconds remaining in the period, parsed from clock. |
| `entity_id` | String | Team UUID the event is credited to. |
| `side` | String | Side of the game the row belongs to: home or away (from the fixture's isHome flag). |
| `person_id` | String | Player UUID of the shooter. |
| `person_name` | String | Name of the shooter. |
| `bib` | String | Jersey number of the player, as text; null for team events. |
| `event_type` | String | Event type: 2pt, 3pt, freeThrow, rebound, assist, steal, block, turnover, foul, substitution, timeOut, jumpBall, ... (language-independent). |
| `event_sub_type` | String | Event detail: jumpShot, layup, offensive, defensive, in, out, 1Of2, personal, ...; null when none. |
| `description` | String | The event's text in the site's language (e.g. Entre-deux remporte). |
| `success` | Boolean | True when the shot was made, false when missed. |
| `success_label` | String | The made / missed label in the site's language; sent on shot events only. |
| `x` | Float64 | Shot location along the 28 m court length as percent of the court (0-100, origin at a corner); the home team attacks the low-x basket all game; LNBP and NBL1 clamp rim attempts to 5 / 95. |
| `y` | Float64 | Shot location across the 15 m width as percent of the court (0-100); which sideline is 0 is unverified. |
| `home_score` | Int64 | Home team's running score after the event. |
| `away_score` | Int64 | Away team's running score after the event. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import lpb_schedule, lpb_shots
games = lpb_schedule()
lpb_shots(fixture_id=games['fixture_id'][0])
```

### lpb_standings {#lpb_standings}

`lpb_standings(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Standings of a LPB / LPBF Liga Profesional de Baloncesto (Panama) season.

LPB / LPBF Liga Profesional de Baloncesto (Panama) (men's and women's); season rule: calendar year.

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
from sportsdataverse.basketball.atrium import lpb_standings
lpb_standings(season=2026)
```

### lpb_team_box {#lpb_team_box}

`lpb_team_box(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Team box score of a LPB / LPBF Liga Profesional de Baloncesto (Panama) game.

LPB / LPBF Liga Profesional de Baloncesto (Panama) (men's and women's); season rule: calendar year.

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
from sportsdataverse.basketball.atrium import lpb_schedule, lpb_team_box
games = lpb_schedule()
lpb_team_box(fixture_id=games['fixture_id'][0])
```

### lpb_teams {#lpb_teams}

`lpb_teams(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Teams of a LPB / LPBF Liga Profesional de Baloncesto (Panama) season.

LPB / LPBF Liga Profesional de Baloncesto (Panama) (men's and women's); season rule: calendar year.

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
from sportsdataverse.basketball.atrium import lpb_teams
lpb_teams(season=2026)
```

### nbl1_pbp {#nbl1_pbp}

`nbl1_pbp(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Play-by-play of a NBL1 (Australia) game.

NBL1 (Australia) (men's and women's); season rule: calendar year, Mar-Aug (END year); conferences are competition_ids.

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
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `fixture_id` | String | Game UUID (fixture); the key of every game frame and of the fixture_detail routes. |
| `season_id` | String | Season UUID of the game. |
| `sequence` | Int64 | 1-based position of the event in the game (periods in order, events chronological). |
| `event_id` | String | Event UUID, shared with the same event in atrium_shots. |
| `period_id` | Int64 | Period number: 1-4 regulation, 11, 12, ... overtimes (not 5). |
| `clock` | String | Time REMAINING in the period as an ISO 8601 duration (PT10M0S at the start). |
| `clock_seconds` | Int64 | Seconds remaining in the period, parsed from clock. |
| `entity_id` | String | Team UUID the event is credited to. |
| `side` | String | Side of the game the row belongs to: home or away (from the fixture's isHome flag). |
| `person_id` | String | Player UUID; null for team events (time-outs, team rebounds). |
| `person_name` | String | Player the event is credited to; null for team events (time-outs, team rebounds). |
| `bib` | String | Jersey number of the player, as text; null for team events. |
| `event_type` | String | Event type: 2pt, 3pt, freeThrow, rebound, assist, steal, block, turnover, foul, substitution, timeOut, jumpBall, ... (language-independent). |
| `event_sub_type` | String | Event detail: jumpShot, layup, offensive, defensive, in, out, 1Of2, personal, ...; null when none. |
| `description` | String | The event's text in the site's language (e.g. Entre-deux remporte). |
| `success` | Boolean | True when a shot or free throw was made, false when missed; null for other events. |
| `success_label` | String | The made / missed label in the site's language; sent on shot events only. |
| `x` | Float64 | Shot location along the 28 m court length as percent of the court (0-100, origin at a corner); the home team attacks the low-x basket all game; null for non-shots. |
| `y` | Float64 | Shot location across the 15 m width as percent of the court (0-100); which sideline is 0 is unverified; null for non-shots. |
| `home_score` | Int64 | Home team's running score after the event. |
| `away_score` | Int64 | Away team's running score after the event. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import nbl1_schedule, nbl1_pbp
games = nbl1_schedule()
nbl1_pbp(fixture_id=games['fixture_id'][0])
```

### nbl1_player_box {#nbl1_player_box}

`nbl1_player_box(fixture_id: 'str', locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Player box score of a NBL1 (Australia) game.

NBL1 (Australia) (men's and women's); season rule: calendar year, Mar-Aug (END year); conferences are competition_ids.

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
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `fixture_id` | String | Game UUID (fixture); the key of every game frame and of the fixture_detail routes. |
| `season_id` | String | Season UUID of the game. |
| `entity_id` | String | Team UUID the player is rostered with for this game. |
| `side` | String | Side of the game the row belongs to: home or away (from the fixture's isHome flag). |
| `person_id` | String | Player UUID (person), shared across seasons and sites. |
| `person_name` | String | Player name as the site prints it (upper case on LNBP and NBL1). |
| `bib` | String | Jersey number the player wears, as text. |
| `position` | String | Position code on the roster (LNB, e.g. PG); null on LNBP. |
| `starter` | Boolean | True when the player started the game. |
| `active` | Boolean | True when the player is on the court in a live game. |
| `participated` | Boolean | True when the player entered the game; false rows have null statistics. |
| `did_not_play_reason` | String | Reason the player did not play; null in every body seen. |
| `minutes` | String | Time played as an ISO 8601 duration (PT7M23S); a team's is the sum over its players (PT200M) (player, this game). |
| `seconds_played` | Int64 | Time played in whole seconds, parsed from minutes (player, this game). |
| `points` | Int64 | Points scored (player, this game). |
| `rebounds` | Int64 | Total rebounds (player, this game). |
| `rebounds_offensive` | Int64 | Offensive rebounds (player, this game). |
| `rebounds_defensive` | Int64 | Defensive rebounds (player, this game). |
| `assists` | Int64 | Assists (player, this game). |
| `steals` | Int64 | Steals (player, this game). |
| `blocks` | Int64 | Shots blocked (player, this game). |
| `blocks_received` | Int64 | Own shots blocked by an opponent (LNB only) (player, this game). |
| `turnovers` | Int64 | Turnovers (player, this game). |
| `fouls_total` | Int64 | Fouls committed (player, this game). |
| `fouls_drawn` | Int64 | Fouls drawn (committed against the player or team; LNBP only) (player, this game). |
| `plus_minus` | Int64 | Plus/minus, the point differential while on court (LNB only; null in some team lines) (player, this game). |
| `efficiency` | Int64 | Efficiency rating, from the site's own formula (player, this game). |
| `field_goals_made` | Int64 | Field goals made, two- and three-pointers together (LNBP only) (player, this game). |
| `field_goals_attempted` | Int64 | Field goals attempted (LNBP only) (player, this game). |
| `field_goals_percentage` | Float64 | Field-goal percentage, 0-100 (LNBP only) (player, this game). |
| `points_two_made` | Int64 | Two-point field goals made (player, this game). |
| `points_two_attempted` | Int64 | Two-point field goals attempted (player, this game). |
| `points_two_percentage` | Float64 | Two-point percentage, 0-100 (player, this game). |
| `points_three_made` | Int64 | Three-point field goals made (player, this game). |
| `points_three_attempted` | Int64 | Three-point field goals attempted (player, this game). |
| `points_three_percentage` | Float64 | Three-point percentage, 0-100 (player, this game). |
| `free_throws_made` | Int64 | Free throws made (player, this game). |
| `free_throws_attempted` | Int64 | Free throws attempted (player, this game). |
| `free_throws_percentage` | Float64 | Free-throw percentage, 0-100 (player, this game). |
| `points_fast_break` | Int64 | Fast-break points (LNBP only) (player, this game). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import nbl1_schedule, nbl1_player_box
games = nbl1_schedule()
nbl1_player_box(fixture_id=games['fixture_id'][0])
```

### nbl1_schedule {#nbl1_schedule}

`nbl1_schedule(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Fixtures (schedule and results) of a NBL1 (Australia) season.

NBL1 (Australia) (men's and women's); season rule: calendar year, Mar-Aug (END year); conferences are competition_ids.

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
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | String | Season UUID the page is about (the site's default season when none was asked). |
| `competition_id` | String | Competition UUID of the season. |
| `competition_name` | String | Competition name in the site's language, e.g. Betclic ELITE or LIGA CALIENTE.MX LNBP. |
| `season_end_year` | Int64 | Season END year (the sdv key): the site's year + 1 on LNB (site 12), the calendar year on the other registered sites; null when the site is unregistered, lists no seasons or the body has no season list. |
| `fixture_id` | String | Game UUID (fixture); the key of every game frame and of the fixture_detail routes. |
| `fixture_type` | String | Kind of game as sent: REGULAR, PLAYOFF, ... (the set is open). |
| `name` | String | Fixture name as the site prints it (e.g. Paris - Lyon-Villeurbanne); null on LNBP. |
| `round` | String | Round of the game as the site words it, e.g. Journee: 30. |
| `round_code` | String | Round code; null in every body seen. |
| `pool` | String | Pool the game belongs to; null when the competition has none. |
| `series` | String | Playoff series the game belongs to as sent (JSON text when an object); null outside a series. |
| `stage_code` | String | Opaque 30-hex stage code of the game; null in every body seen. |
| `status` | String | Status code of the game, e.g. CONFIRMED (final), as sent. |
| `status_label` | String | Status of the game as the site words it in its own language (e.g. Termine, Final). |
| `start_time_utc` | Datetime | Scheduled start, naive datetime in UTC (the API sends no offset). |
| `start_time_local` | Datetime | Scheduled start, naive datetime in the venue's local time (timezone gives the zone; no offset sent). |
| `timezone` | String | IANA time zone of the venue (e.g. Europe/Paris, Mexico/General) that start_time_local is in. |
| `times_unconfirmed` | Boolean | True while the start time is not final. |
| `venue` | String | Name of the arena where the game is played. |
| `venue_id` | String | Venue UUID; joins no other route. |
| `attendance` | Int64 | Spectators at the game; null when not reported. |
| `home_entity_id` | String | Team UUID of the home team. |
| `home_code` | String | Short code of the home team (e.g. PAB). |
| `home_name` | String | Name of the home team. |
| `home_score` | Int64 | Points scored by the home team (the API sends a string; Int64 here). |
| `home_result_place` | Int64 | Result place of the home team: 1 for the winner, 2 for the loser. |
| `home_result_status` | String | Result status of the home team's score, e.g. CONFIRMED. |
| `home_draw` | Boolean | True when the game ended level for the home team. |
| `home_shootout_attempts` | String | Shootout attempts of the home team as sent (an empty string becomes null). |
| `home_secondary_score` | String | Secondary score of the home team (unused in the samples; shown as text). |
| `away_entity_id` | String | Team UUID of the away team. |
| `away_code` | String | Short code of the away team (e.g. PAB). |
| `away_name` | String | Name of the away team. |
| `away_score` | Int64 | Points scored by the away team (the API sends a string; Int64 here). |
| `away_result_place` | Int64 | Result place of the away team: 1 for the winner, 2 for the loser. |
| `away_result_status` | String | Result status of the away team's score, e.g. CONFIRMED. |
| `away_draw` | Boolean | True when the game ended level for the away team. |
| `away_shootout_attempts` | String | Shootout attempts of the away team as sent (an empty string becomes null). |
| `away_secondary_score` | String | Secondary score of the away team (unused in the samples; shown as text). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import nbl1_schedule
nbl1_schedule(season=2026)
```

### nbl1_seasons {#nbl1_seasons}

`nbl1_seasons(locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Every season and competition id of NBL1 (Australia).

NBL1 (Australia) (men's and women's); season rule: calendar year, Mar-Aug (END year); conferences are competition_ids.

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
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | String | Season UUID; pass it as season_id to any season-scoped wrapper. |
| `competition_id` | String | Competition UUID; pass it as competition_id to pick one of several seasons of an END year. |
| `competition_name` | String | Competition name in the site's language; the same name can appear for several years. |
| `season_name` | String | Season name in the site's language, e.g. Betclic ELITE 2025. |
| `abbreviation_local` | String | Local abbreviation of the season; null in every body seen. |
| `year` | Int64 | The site's own year of the season: the START year on LNB (site 12), the calendar year elsewhere. |
| `season_end_year` | Int64 | Season END year (the sdv key): the site's year + 1 on LNB (site 12), the calendar year on the other registered sites; null when the site is unregistered, lists no seasons or the body has no season list. |
| `is_default_season` | Boolean | True for the season the body is about (the site's default when none was asked). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import nbl1_seasons
nbl1_seasons()
```

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
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `fixture_id` | String | Game UUID (fixture); the key of every game frame and of the fixture_detail routes. |
| `season_id` | String | Season UUID of the game. |
| `sequence` | Int64 | 1-based position of the shot among the shots of the game (not the pbp sequence). |
| `event_id` | String | Event UUID of the shot; the same event in atrium_pbp. |
| `period_id` | Int64 | Period number: 1-4 regulation, 11, 12, ... overtimes (not 5). |
| `clock` | String | Time REMAINING in the period as an ISO 8601 duration (PT10M0S at the start). |
| `clock_seconds` | Int64 | Seconds remaining in the period, parsed from clock. |
| `entity_id` | String | Team UUID the event is credited to. |
| `side` | String | Side of the game the row belongs to: home or away (from the fixture's isHome flag). |
| `person_id` | String | Player UUID of the shooter. |
| `person_name` | String | Name of the shooter. |
| `bib` | String | Jersey number of the player, as text; null for team events. |
| `event_type` | String | Event type: 2pt, 3pt, freeThrow, rebound, assist, steal, block, turnover, foul, substitution, timeOut, jumpBall, ... (language-independent). |
| `event_sub_type` | String | Event detail: jumpShot, layup, offensive, defensive, in, out, 1Of2, personal, ...; null when none. |
| `description` | String | The event's text in the site's language (e.g. Entre-deux remporte). |
| `success` | Boolean | True when the shot was made, false when missed. |
| `success_label` | String | The made / missed label in the site's language; sent on shot events only. |
| `x` | Float64 | Shot location along the 28 m court length as percent of the court (0-100, origin at a corner); the home team attacks the low-x basket all game; LNBP and NBL1 clamp rim attempts to 5 / 95. |
| `y` | Float64 | Shot location across the 15 m width as percent of the court (0-100); which sideline is 0 is unverified. |
| `home_score` | Int64 | Home team's running score after the event. |
| `away_score` | Int64 | Away team's running score after the event. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import nbl1_schedule, nbl1_shots
games = nbl1_schedule()
nbl1_shots(fixture_id=games['fixture_id'][0])
```
