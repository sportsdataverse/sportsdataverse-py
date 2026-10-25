---
title: "NBL Australia (Rosetta) — NBL / WNBL / NBL1 Rosetta proxy (prod.rosetta.nbl.com.au, Australia) — Team"
sidebar_label: "Team"
sidebar_position: 4
description: "NBL Australia (Rosetta) — NBL / WNBL / NBL1 Rosetta proxy (prod.rosetta.nbl.com.au, Australia) — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBL Australia (Rosetta) — NBL / WNBL / NBL1 Rosetta proxy (prod.rosetta.nbl.com.au, Australia) — Team

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
| `org` | character | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `team_id` | character | DataCore team id (UUID v1); stable across seasons. |
| `rosetta_team_id` | character | Rosetta team id (UUID v4; v5 on NBL1); stable across seasons; accepted as {team_id}. |
| `name` | character | Team name as Rosetta spells it, e.g. Perth Wildcats. |
| `team_code` | character | Team code, e.g. PER (the {team_code} path value) |
| `established` | character | Year the club was founded; often null. |
| `head_coach` | character | Head coach; often null. |
| `championships` | character | Championships won; often null. |
| `website` | character | Club website URL. |
| `external_media_competitor_id` | character | Team id on the media/OTT service; often null. |
| `conference` | character | NBL1 conference of the team (name); null for NBL and WNBL teams. |
| `division` | character | NBL1 division of the team (name); null for NBL and WNBL teams. |
| `venue_name` | character | Name of the venue (arena), e.g. RAC Arena. |
| `venue_address` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_state` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_country` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_timezone` | character | IANA time zone of the venue, e.g. Australia/Sydney. |
| `venue_latitude` | numeric | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_longitude` | numeric | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_capacity` | integer | Unobserved in samples (null in every capture); type and meaning unverified. |
| `conference_name` | character | Name of the NBL1 conference the team plays in (e.g. North, South); null outside NBL1. |
| `division_name` | character | Division name (NBL1 conference + gender) |

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
| `org` | character | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `jersey_number` | character | Jersey number, a STRING. |
| `playing_position` | character | Playing position code (G, F, C, PG, SF, FC, ...); null when not recorded. |
| `team_id` | character | DataCore id (UUID v1, Sportradar) of the team; stable across seasons and the id to join on across routes. |
| `rosetta_team_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the team; routes that take this entity accept it as well as the DataCore id. |
| `team_name` | character | Name of the team as Rosetta spells it, e.g. Perth Wildcats. |
| `team_code` | character | Team code, e.g. PER (the {team_code} path value) |
| `team_gender` | character | Gender (MALE/FEMALE upper case on seasons and players, male/female lower case on competitions and divisions; UNKNOWN seen) |
| `player_id` | character | DataCore id (UUID v1, Sportradar) of the player; stable across seasons and the id to join on across routes. |
| `rosetta_player_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the player; routes that take this entity accept it as well as the DataCore id. |
| `player_first_name` | character | The player's given name, e.g. Elijah. |
| `player_last_name` | character | The player's family name, e.g. Pepper. |
| `player_full_name` | character | Given and family name. |
| `player_jersey_number` | character | Jersey number, a STRING. |
| `player_playing_position` | character | Playing position code (G, F, C, PG, SF, FC, ...); null when not recorded. |
| `player_active` | logical | Player record is active. |
| `player_date_of_birth` | character | Date of birth, ISO date. |
| `player_nationality` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `player_height` | numeric | Height in centimetres. |
| `player_weight` | numeric | Weight in kilograms. |
| `player_dominant_hand` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `player_gender` | character | Gender (MALE/FEMALE upper case on seasons and players, male/female lower case on competitions and divisions; UNKNOWN seen) |
| `player_nationality_code` | character | Nationality, ISO 3166-1 alpha-3 code. |
| `player_birthplace` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `player_rookie_year` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `season_end_year` | integer | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

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
| `org` | character | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `assists_average` | numeric | Assists per game. |
| `blocks_average` | numeric | Blocks per game. |
| `field_goals_attempted` | integer | Field goals attempted, season total. |
| `field_goals_made` | integer | Field goals made, season total. |
| `field_goals_attempted_average` | numeric | Field goals attempted per game. |
| `field_goals_made_average` | numeric | Field goals made per game. |
| `field_goals_percentage` | numeric | Field goals made / attempted, a 0-1 FRACTION. |
| `fouls_average` | numeric | Fouls committed (personal + technical + others) per game. |
| `free_throws_attempted` | integer | Free throws attempted, season total. |
| `free_throws_made` | integer | Free throws made, season total. |
| `free_throws_attempted_average` | numeric | Free throws attempted per game. |
| `free_throws_made_average` | numeric | Free throws made per game. |
| `free_throws_percentage` | numeric | Free throws made / attempted, a 0-1 FRACTION. |
| `games` | integer | Games played in the season (in the period rows, by the player or team). |
| `games_started` | integer | Games in the starting five. |
| `offensive_rebounds` | integer | Offensive rebounds, season total. |
| `fouls` | integer | Fouls committed (personal + technical + others), season total. |
| `personal_fouls` | integer | Personal fouls, season total. |
| `technical_fouls` | integer | Technical fouls, season total. |
| `period` | integer | Period as a STRING: 0 = game or season total, 1-4 = quarters, 11/12/13 = first/second/third overtime (inferred: fewer teams have each) |
| `three_points_attempted` | integer | Three-point field goals attempted, season total. |
| `three_points_attempted_average` | numeric | Three-point field goals attempted per game. |
| `three_points_made` | integer | Three-point field goals made, season total. |
| `three_points_made_average` | numeric | Three-point field goals made per game. |
| `three_points_percentage` | numeric | Three-point field goals made / attempted, a 0-1 FRACTION. |
| `turnovers_average` | numeric | Turnovers per game. |
| `turnovers` | integer | Turnovers, season total. |
| `two_points_attempted` | integer | Two-point field goals attempted, season total. |
| `two_points_attempted_average` | numeric | Two-point field goals attempted per game. |
| `two_points_made` | integer | Two-point field goals made, season total. |
| `two_points_made_average` | numeric | Two-point field goals made per game. |
| `two_points_percentage` | numeric | Two-point field goals made / attempted, a 0-1 FRACTION. |
| `minutes` | numeric | Minutes played, decimal minutes. |
| `minutes_average` | numeric | Minutes per game, decimal minutes. |
| `points` | integer | Points, season total. |
| `rebounds` | integer | Total rebounds, season total. |
| `assists` | integer | Assists, season total. |
| `blocks` | integer | Blocks, season total. |
| `steals` | integer | Steals, season total. |
| `plus_minus` | integer | Point differential while on court, season total. |
| `plus_minus_average` | numeric | Point differential while on court per game. |
| `efficiency` | integer | Efficiency rating (PTS + REB + AST + STL + BLK - missed FG - missed FT - TOV), season total. |
| `efficiency_average` | numeric | Efficiency rating (PTS + REB + AST + STL + BLK - missed FG - missed FT - TOV) per game. |
| `points_average` | numeric | Points per game. |
| `rebounds_average` | numeric | Total rebounds per game. |
| `steals_average` | numeric | Steals per game. |
| `player_id` | character | DataCore id (UUID v1, Sportradar) of the player; stable across seasons and the id to join on across routes. |
| `rosetta_player_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the player; routes that take this entity accept it as well as the DataCore id. |
| `player_first_name` | character | The player's given name, e.g. Elijah. |
| `player_jersey_number` | character | Jersey number, a STRING. |
| `player_last_name` | character | The player's family name, e.g. Pepper. |
| `player_playing_position` | character | Playing position code (G, F, C, PG, SF, FC, ...); null when not recorded. |
| `team_id` | character | DataCore id (UUID v1, Sportradar) of the team; stable across seasons and the id to join on across routes. |
| `rosetta_team_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the team; routes that take this entity accept it as well as the DataCore id. |
| `team_name` | character | Name of the team as Rosetta spells it, e.g. Perth Wildcats. |
| `team_code` | character | Team code, e.g. PER (the {team_code} path value) |
| `season_id` | character | DataCore id (UUID v1, Sportradar) of the season; stable across seasons and the id to join on across routes. |
| `rosetta_season_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the season; routes that take this entity accept it as well as the DataCore id. |
| `season_year` | integer | Rosetta's year of the season: the START year for the NBL and WNBL from 1998-99 (2025 = 2025-26), the calendar year for NBL1 and the 1979-1997 winter seasons. See season_end_year for the sdv key. |
| `season_type` | character | Season type: regular (includes finals), preseason, in_season (cups, blitz, exhibition), test. |
| `season_competition_division` | character | Always an empty array in samples; element shape and meaning unverified. |
| `season_end_year` | integer | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

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
| `org` | character | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `assists` | integer | Assists, team season total (in that period on period rows) |
| `assists_average` | numeric | Assists per game. |
| `blocks` | integer | Blocks, team season total (in that period on period rows) |
| `blocks_average` | numeric | Blocks per game. |
| `defensive_rebounds` | integer | Defensive rebounds, team season total (in that period on period rows) |
| `field_goals_attempted` | integer | Field goals attempted, team season total (in that period on period rows) |
| `field_goals_attempted_average` | numeric | Field goals attempted per game. |
| `field_goals_made` | integer | Field goals made, team season total (in that period on period rows) |
| `field_goals_made_average` | numeric | Field goals made per game. |
| `field_goals_percentage` | numeric | Field goals made / attempted, a 0-1 FRACTION. |
| `fouls` | integer | Fouls committed (personal + technical + others), team season total (in that period on period rows) |
| `fouls_average` | numeric | Fouls committed (personal + technical + others) per game. |
| `free_throws_attempted` | integer | Free throws attempted, team season total (in that period on period rows) |
| `free_throws_attempted_average` | numeric | Free throws attempted per game. |
| `free_throws_made` | integer | Free throws made, team season total (in that period on period rows) |
| `free_throws_made_average` | numeric | Free throws made per game. |
| `free_throws_percentage` | numeric | Free throws made / attempted, a 0-1 FRACTION. |
| `games` | integer | Games played in the season (in the period rows, by the player or team). |
| `minutes` | numeric | Player-minutes played (5 x game minutes) |
| `offensive_rebounds` | integer | Offensive rebounds, team season total (in that period on period rows) |
| `offensive_rebounds_average` | numeric | Offensive rebounds per game. |
| `period` | integer | Period as a STRING: 0 = game or season total, 1-4 = quarters, 11/12/13 = first/second/third overtime (inferred: fewer teams have each) |
| `personal_fouls_average` | numeric | Personal fouls per game. |
| `points` | integer | Points, team season total (in that period on period rows) |
| `points_average` | numeric | Points per game. |
| `rebounds` | integer | Total rebounds, team season total (in that period on period rows) |
| `rebounds_average` | numeric | Total rebounds per game. |
| `results_string` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `steals` | integer | Steals, team season total (in that period on period rows) |
| `steals_average` | numeric | Steals per game. |
| `three_points_attempted` | integer | Three-point field goals attempted, team season total (in that period on period rows) |
| `three_points_attempted_average` | numeric | Three-point field goals attempted per game. |
| `three_points_made` | integer | Three-point field goals made, team season total (in that period on period rows) |
| `three_points_made_average` | numeric | Three-point field goals made per game. |
| `three_points_percentage` | numeric | Three-point field goals made / attempted, a 0-1 FRACTION. |
| `turnovers` | integer | Turnovers, team season total (in that period on period rows) |
| `turnovers_average` | numeric | Turnovers per game. |
| `two_points_attempted` | integer | Two-point field goals attempted, team season total (in that period on period rows) |
| `two_points_attempted_average` | numeric | Two-point field goals attempted per game. |
| `two_points_made` | integer | Two-point field goals made, team season total (in that period on period rows) |
| `two_points_made_average` | numeric | Two-point field goals made per game. |
| `two_points_percentage` | numeric | Two-point field goals made / attempted, a 0-1 FRACTION. |
| `team_id` | character | DataCore id (UUID v1, Sportradar) of the team; stable across seasons and the id to join on across routes. |
| `rosetta_team_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the team; routes that take this entity accept it as well as the DataCore id. |
| `team_name` | character | Name of the team as Rosetta spells it, e.g. Perth Wildcats. |
| `team_code` | character | Team code, e.g. PER (the {team_code} path value) |
| `season_id` | character | DataCore id (UUID v1, Sportradar) of the season; stable across seasons and the id to join on across routes. |
| `rosetta_season_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the season; routes that take this entity accept it as well as the DataCore id. |
| `season_year` | integer | Rosetta's year of the season: the START year for the NBL and WNBL from 1998-99 (2025 = 2025-26), the calendar year for NBL1 and the 1979-1997 winter seasons. See season_end_year for the sdv key. |
| `season_type` | character | Season type: regular (includes finals), preseason, in_season (cups, blitz, exhibition), test. |
| `season_competition_division` | character | NBL1 divisions (conference x gender); empty for NBL and WNBL. |
| `season_end_year` | integer | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

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
