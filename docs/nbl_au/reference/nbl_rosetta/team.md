# NBL Australia (Rosetta) — NBL / WNBL / NBL1 Rosetta proxy (prod.rosetta.nbl.com.au, Australia) — Team

> NBL Australia (Rosetta) — NBL / WNBL / NBL1 Rosetta proxy (prod.rosetta.nbl.com.au, Australia) — Team — function reference in sdv-py, the SportsDataverse Python package.

## nbl_rosetta_teams

Every team record of an organisation: current, defunct, exhibition and development sides.

**Endpoint URL:** `GET https://prod.rosetta.nbl.com.au/get/{org}/teams`

**Valid URL:** [https://prod.rosetta.nbl.com.au/get/nbl/teams](https://prod.rosetta.nbl.com.au/get/nbl/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `org` | `org` |  |  | `Y` | Organisation: nbl (men's NBL), wnbl (women's) or nbl1 (the second tier, men's and women's conferences). |
| `limit` | `limit` |  |  | `Y` | Rows to return. Omitted = a page large enough for the whole list (a short page cut by Rosetta's path cache is refused). |

### Returns {#nbl_rosetta_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `org` | String | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `team_id` | String | DataCore team id (UUID v1); stable across seasons. |
| `rosetta_team_id` | String | Rosetta team id (UUID v4; v5 on NBL1); stable across seasons; accepted as {team_id}. |
| `name` | String | Team name as Rosetta spells it, e.g. Perth Wildcats. |
| `team_code` | String | Team code, e.g. PER (the {team_code} path value) |
| `established` | String | Year the club was founded; often null. |
| `head_coach` | String | Head coach; often null. |
| `championships` | String | Championships won; often null. |
| `website` | String | Club website URL. |
| `external_media_competitor_id` | String | Team id on the media/OTT service; often null. |
| `conference` | String | NBL1 conference of the team (name); null for NBL and WNBL teams. |
| `division` | String | NBL1 division of the team (name); null for NBL and WNBL teams. |
| `venue_name` | String | Name of the venue (arena), e.g. RAC Arena. |
| `venue_address` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_state` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_country` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_timezone` | String | IANA time zone of the venue, e.g. Australia/Sydney. |
| `venue_latitude` | Float64 | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_longitude` | Float64 | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_capacity` | Int64 | Unobserved in samples (null in every capture); type and meaning unverified. |
| `conference_name` | String | Name of the NBL1 conference the team plays in (e.g. North, South); null outside NBL1. |
| `division_name` | String | Division name (NBL1 conference + gender) |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_teams-example}

```python
nbl_rosetta_teams(org='nbl')
```

## nbl_rosetta_team_players

Roster of one team in a season: player bio, jersey and position.

**Endpoint URL:** `GET https://prod.rosetta.nbl.com.au/get/{org}/players/for/team/{team_id}/in/season/{season}`

**Valid URL:** [https://prod.rosetta.nbl.com.au/get/nbl/players/for/team/befee10a-410c-11f0-b3f4-1b3c55488bbb/in/season/2026](https://prod.rosetta.nbl.com.au/get/nbl/players/for/team/befee10a-410c-11f0-b3f4-1b3c55488bbb/in/season/2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Team id: the DataCore id (team_id of nbl_rosetta_teams) or Rosetta's own id; either is accepted. |
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26). The runtime maps it to Rosetta's year: the START year for the NBL and WNBL from 1998-99, the calendar year for NBL1 and the NBL / WNBL winter seasons up to 1998. END 1998 and 1999 both read Rosetta year 1998, which holds two seasons; the matches frame keeps the one asked for. |
| `org` | `org` |  |  | `Y` | Organisation: nbl (men's NBL), wnbl (women's) or nbl1 (the second tier, men's and women's conferences). |

### Returns {#nbl_rosetta_team_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `org` | String | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `jersey_number` | String | Jersey number, a STRING. |
| `playing_position` | String | Playing position code (G, F, C, PG, SF, FC, ...); null when not recorded. |
| `team_id` | String | DataCore id (UUID v1, Sportradar) of the team; stable across seasons and the id to join on across routes. |
| `rosetta_team_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the team; routes that take this entity accept it as well as the DataCore id. |
| `team_name` | String | Name of the team as Rosetta spells it, e.g. Perth Wildcats. |
| `team_code` | String | Team code, e.g. PER (the {team_code} path value) |
| `team_gender` | String | Gender (MALE/FEMALE upper case on seasons and players, male/female lower case on competitions and divisions; UNKNOWN seen) |
| `player_id` | String | DataCore id (UUID v1, Sportradar) of the player; stable across seasons and the id to join on across routes. |
| `rosetta_player_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the player; routes that take this entity accept it as well as the DataCore id. |
| `player_first_name` | String | The player's given name, e.g. Elijah. |
| `player_last_name` | String | The player's family name, e.g. Pepper. |
| `player_full_name` | String | Given and family name. |
| `player_jersey_number` | String | Jersey number, a STRING. |
| `player_playing_position` | String | Playing position code (G, F, C, PG, SF, FC, ...); null when not recorded. |
| `player_active` | Boolean | Player record is active. |
| `player_date_of_birth` | String | Date of birth, ISO date. |
| `player_nationality` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `player_height` | Float64 | Height in centimetres. |
| `player_weight` | Float64 | Weight in kilograms. |
| `player_dominant_hand` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `player_gender` | String | Gender (MALE/FEMALE upper case on seasons and players, male/female lower case on competitions and divisions; UNKNOWN seen) |
| `player_nationality_code` | String | Nationality, ISO 3166-1 alpha-3 code. |
| `player_birthplace` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `player_rookie_year` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `season_end_year` | Int64 | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_team_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_team_players-example}

```python
nbl_rosetta_team_players(org='nbl', season=2026, team_id='befee10a-410c-11f0-b3f4-1b3c55488bbb')
```

## nbl_rosetta_team_stats_leaders

Player season totals of one team in a season, optionally one period and sorted.

**Endpoint URL:** `GET https://prod.rosetta.nbl.com.au/get/{org}/stats/leaders/for/season/id/{season_id}/team/{team_code}`

**Valid URL:** [https://prod.rosetta.nbl.com.au/get/nbl/stats/leaders/for/season/id/1f8e4a79-e98b-457b-85a5-e4b898c6c0bd/team/PER?filter%5Bperiod%5D=0&sort=-points](https://prod.rosetta.nbl.com.au/get/nbl/stats/leaders/for/season/id/1f8e4a79-e98b-457b-85a5-e4b898c6c0bd/team/PER?filter%5Bperiod%5D=0&sort=-points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | Season id: the DataCore season id (season_id of nbl_rosetta_seasons) or Rosetta's own id (rosetta_season_id); either is accepted. Not the year. |
| `team_code` | `team_code` |  | `Y` |  | Team code (team_code of nbl_rosetta_teams), e.g. PER for the Perth Wildcats. |
| `org` | `org` |  |  | `Y` | Organisation: nbl (men's NBL), wnbl (women's) or nbl1 (the second tier, men's and women's conferences). |
| `limit` | `limit` |  |  | `Y` | Rows to return. Omitted = a page large enough for the whole list (a short page cut by Rosetta's path cache is refused). |
| `filter[period]` | `period` |  |  | `Y` | Period to keep: 0 = whole game, 1-4 quarters, 5 and up overtime. |
| `sort` | `sort` |  |  | `Y` | Sort key; a leading minus sorts descending, e.g. -points. |

### Returns {#nbl_rosetta_team_stats_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `org` | String | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `assists_average` | Float64 | Assists per game. |
| `blocks_average` | Float64 | Blocks per game. |
| `field_goals_attempted` | Int64 | Field goals attempted, season total. |
| `field_goals_made` | Int64 | Field goals made, season total. |
| `field_goals_attempted_average` | Float64 | Field goals attempted per game. |
| `field_goals_made_average` | Float64 | Field goals made per game. |
| `field_goals_percentage` | Float64 | Field goals made / attempted, a 0-1 FRACTION. |
| `fouls_average` | Float64 | Fouls committed (personal + technical + others) per game. |
| `free_throws_attempted` | Int64 | Free throws attempted, season total. |
| `free_throws_made` | Int64 | Free throws made, season total. |
| `free_throws_attempted_average` | Float64 | Free throws attempted per game. |
| `free_throws_made_average` | Float64 | Free throws made per game. |
| `free_throws_percentage` | Float64 | Free throws made / attempted, a 0-1 FRACTION. |
| `games` | Int64 | Games played in the season (in the period rows, by the player or team). |
| `games_started` | Int64 | Games in the starting five. |
| `offensive_rebounds` | Int64 | Offensive rebounds, season total. |
| `fouls` | Int64 | Fouls committed (personal + technical + others), season total. |
| `personal_fouls` | Int64 | Personal fouls, season total. |
| `technical_fouls` | Int64 | Technical fouls, season total. |
| `period` | Int64 | Period as a STRING: 0 = game or season total, 1-4 = quarters, 11/12/13 = first/second/third overtime (inferred: fewer teams have each) |
| `three_points_attempted` | Int64 | Three-point field goals attempted, season total. |
| `three_points_attempted_average` | Float64 | Three-point field goals attempted per game. |
| `three_points_made` | Int64 | Three-point field goals made, season total. |
| `three_points_made_average` | Float64 | Three-point field goals made per game. |
| `three_points_percentage` | Float64 | Three-point field goals made / attempted, a 0-1 FRACTION. |
| `turnovers_average` | Float64 | Turnovers per game. |
| `turnovers` | Int64 | Turnovers, season total. |
| `two_points_attempted` | Int64 | Two-point field goals attempted, season total. |
| `two_points_attempted_average` | Float64 | Two-point field goals attempted per game. |
| `two_points_made` | Int64 | Two-point field goals made, season total. |
| `two_points_made_average` | Float64 | Two-point field goals made per game. |
| `two_points_percentage` | Float64 | Two-point field goals made / attempted, a 0-1 FRACTION. |
| `minutes` | Float64 | Minutes played, decimal minutes. |
| `minutes_average` | Float64 | Minutes per game, decimal minutes. |
| `points` | Int64 | Points, season total. |
| `rebounds` | Int64 | Total rebounds, season total. |
| `assists` | Int64 | Assists, season total. |
| `blocks` | Int64 | Blocks, season total. |
| `steals` | Int64 | Steals, season total. |
| `plus_minus` | Int64 | Point differential while on court, season total. |
| `plus_minus_average` | Float64 | Point differential while on court per game. |
| `efficiency` | Int64 | Efficiency rating (PTS + REB + AST + STL + BLK - missed FG - missed FT - TOV), season total. |
| `efficiency_average` | Float64 | Efficiency rating (PTS + REB + AST + STL + BLK - missed FG - missed FT - TOV) per game. |
| `points_average` | Float64 | Points per game. |
| `rebounds_average` | Float64 | Total rebounds per game. |
| `steals_average` | Float64 | Steals per game. |
| `player_id` | String | DataCore id (UUID v1, Sportradar) of the player; stable across seasons and the id to join on across routes. |
| `rosetta_player_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the player; routes that take this entity accept it as well as the DataCore id. |
| `player_first_name` | String | The player's given name, e.g. Elijah. |
| `player_jersey_number` | String | Jersey number, a STRING. |
| `player_last_name` | String | The player's family name, e.g. Pepper. |
| `player_playing_position` | String | Playing position code (G, F, C, PG, SF, FC, ...); null when not recorded. |
| `team_id` | String | DataCore id (UUID v1, Sportradar) of the team; stable across seasons and the id to join on across routes. |
| `rosetta_team_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the team; routes that take this entity accept it as well as the DataCore id. |
| `team_name` | String | Name of the team as Rosetta spells it, e.g. Perth Wildcats. |
| `team_code` | String | Team code, e.g. PER (the {team_code} path value) |
| `season_id` | String | DataCore id (UUID v1, Sportradar) of the season; stable across seasons and the id to join on across routes. |
| `rosetta_season_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the season; routes that take this entity accept it as well as the DataCore id. |
| `season_year` | Int64 | Rosetta's year of the season: the START year for the NBL and WNBL from 1998-99 (2025 = 2025-26), the calendar year for NBL1 and the 1979-1997 winter seasons. See season_end_year for the sdv key. |
| `season_type` | String | Season type: regular (includes finals), preseason, in_season (cups, blitz, exhibition), test. |
| `season_competition_division` | String | Always an empty array in samples; element shape and meaning unverified. |
| `season_end_year` | Int64 | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_team_stats_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_team_stats_leaders-example}

```python
nbl_rosetta_team_stats_leaders(org='nbl', period=0, season_id='1f8e4a79-e98b-457b-85a5-e4b898c6c0bd', sort='-points', team_code='PER')
```

## nbl_rosetta_team_stats

Team season totals and averages by period (0 = whole game, 1-4 quarters, 5 and up overtime).

**Endpoint URL:** `GET https://prod.rosetta.nbl.com.au/get/{org}/team/stats/for/season/{season}/regular`

**Valid URL:** [https://prod.rosetta.nbl.com.au/get/nbl/team/stats/for/season/2026/regular](https://prod.rosetta.nbl.com.au/get/nbl/team/stats/for/season/2026/regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26). The runtime maps it to Rosetta's year: the START year for the NBL and WNBL from 1998-99, the calendar year for NBL1 and the NBL / WNBL winter seasons up to 1998. END 1998 and 1999 both read Rosetta year 1998, which holds two seasons; the matches frame keeps the one asked for. |
| `org` | `org` |  |  | `Y` | Organisation: nbl (men's NBL), wnbl (women's) or nbl1 (the second tier, men's and women's conferences). |
| `limit` | `limit` |  |  | `Y` | Rows to return. Omitted = a page large enough for the whole list (a short page cut by Rosetta's path cache is refused). |

### Returns {#nbl_rosetta_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `org` | String | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `assists` | Int64 | Assists, team season total (in that period on period rows) |
| `assists_average` | Float64 | Assists per game. |
| `blocks` | Int64 | Blocks, team season total (in that period on period rows) |
| `blocks_average` | Float64 | Blocks per game. |
| `defensive_rebounds` | Int64 | Defensive rebounds, team season total (in that period on period rows) |
| `field_goals_attempted` | Int64 | Field goals attempted, team season total (in that period on period rows) |
| `field_goals_attempted_average` | Float64 | Field goals attempted per game. |
| `field_goals_made` | Int64 | Field goals made, team season total (in that period on period rows) |
| `field_goals_made_average` | Float64 | Field goals made per game. |
| `field_goals_percentage` | Float64 | Field goals made / attempted, a 0-1 FRACTION. |
| `fouls` | Int64 | Fouls committed (personal + technical + others), team season total (in that period on period rows) |
| `fouls_average` | Float64 | Fouls committed (personal + technical + others) per game. |
| `free_throws_attempted` | Int64 | Free throws attempted, team season total (in that period on period rows) |
| `free_throws_attempted_average` | Float64 | Free throws attempted per game. |
| `free_throws_made` | Int64 | Free throws made, team season total (in that period on period rows) |
| `free_throws_made_average` | Float64 | Free throws made per game. |
| `free_throws_percentage` | Float64 | Free throws made / attempted, a 0-1 FRACTION. |
| `games` | Int64 | Games played in the season (in the period rows, by the player or team). |
| `minutes` | Float64 | Player-minutes played (5 x game minutes) |
| `offensive_rebounds` | Int64 | Offensive rebounds, team season total (in that period on period rows) |
| `offensive_rebounds_average` | Float64 | Offensive rebounds per game. |
| `period` | Int64 | Period as a STRING: 0 = game or season total, 1-4 = quarters, 11/12/13 = first/second/third overtime (inferred: fewer teams have each) |
| `personal_fouls_average` | Float64 | Personal fouls per game. |
| `points` | Int64 | Points, team season total (in that period on period rows) |
| `points_average` | Float64 | Points per game. |
| `rebounds` | Int64 | Total rebounds, team season total (in that period on period rows) |
| `rebounds_average` | Float64 | Total rebounds per game. |
| `results_string` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `steals` | Int64 | Steals, team season total (in that period on period rows) |
| `steals_average` | Float64 | Steals per game. |
| `three_points_attempted` | Int64 | Three-point field goals attempted, team season total (in that period on period rows) |
| `three_points_attempted_average` | Float64 | Three-point field goals attempted per game. |
| `three_points_made` | Int64 | Three-point field goals made, team season total (in that period on period rows) |
| `three_points_made_average` | Float64 | Three-point field goals made per game. |
| `three_points_percentage` | Float64 | Three-point field goals made / attempted, a 0-1 FRACTION. |
| `turnovers` | Int64 | Turnovers, team season total (in that period on period rows) |
| `turnovers_average` | Float64 | Turnovers per game. |
| `two_points_attempted` | Int64 | Two-point field goals attempted, team season total (in that period on period rows) |
| `two_points_attempted_average` | Float64 | Two-point field goals attempted per game. |
| `two_points_made` | Int64 | Two-point field goals made, team season total (in that period on period rows) |
| `two_points_made_average` | Float64 | Two-point field goals made per game. |
| `two_points_percentage` | Float64 | Two-point field goals made / attempted, a 0-1 FRACTION. |
| `team_id` | String | DataCore id (UUID v1, Sportradar) of the team; stable across seasons and the id to join on across routes. |
| `rosetta_team_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the team; routes that take this entity accept it as well as the DataCore id. |
| `team_name` | String | Name of the team as Rosetta spells it, e.g. Perth Wildcats. |
| `team_code` | String | Team code, e.g. PER (the {team_code} path value) |
| `season_id` | String | DataCore id (UUID v1, Sportradar) of the season; stable across seasons and the id to join on across routes. |
| `rosetta_season_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the season; routes that take this entity accept it as well as the DataCore id. |
| `season_year` | Int64 | Rosetta's year of the season: the START year for the NBL and WNBL from 1998-99 (2025 = 2025-26), the calendar year for NBL1 and the 1979-1997 winter seasons. See season_end_year for the sdv key. |
| `season_type` | String | Season type: regular (includes finals), preseason, in_season (cups, blitz, exhibition), test. |
| `season_competition_division` | String | NBL1 divisions (conference x gender); empty for NBL and WNBL. |
| `season_end_year` | Int64 | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_team_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_team_stats-example}

```python
nbl_rosetta_team_stats(org='nbl', season=2026)
```
