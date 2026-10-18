---
title: "NBL Australia (Rosetta) — NBL / WNBL / NBL1 Rosetta proxy (prod.rosetta.nbl.com.au, Australia) — Match"
sidebar_label: "Match"
sidebar_position: 1
description: "NBL Australia (Rosetta) — NBL / WNBL / NBL1 Rosetta proxy (prod.rosetta.nbl.com.au, Australia) — Match — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBL Australia (Rosetta) — NBL / WNBL / NBL1 Rosetta proxy (prod.rosetta.nbl.com.au, Australia) — Match

## nbl_rosetta_match_by_id

One match's header (the same body as nbl_rosetta_match): status, scores, venue, season and teams.

**Endpoint URL:** `GET https://prod.rosetta.nbl.com.au/get/match/{match_id}`

**Valid URL:** [https://prod.rosetta.nbl.com.au/get/match/b217a699-4bef-11f0-bc08-933162362355](https://prod.rosetta.nbl.com.au/get/match/b217a699-4bef-11f0-bc08-933162362355)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | DataCore fixture id of the match (match_id of nbl_rosetta_matches), a UUID, e.g. b217a699-4bef-11f0-bc08-933162362355. |

### Returns {#nbl_rosetta_match_by_id-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `org` | character | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `match_id` | character | DataCore fixture id (UUID v1); equal to `id`. |
| `match_status` | character | Status of the match in Rosetta's words (complete for a finished game): box score and play-by-play of any other status are partial. |
| `start_time_datetime` | character | Tip-off, UTC; same value as start_time. |
| `match_type` | character | Match type: regular, final (FINAL upper case seen) |
| `attendance` | integer | Attendance, a STRING; null when not recorded. |
| `clock` | character | Game clock of the latest event, HH:MM:SS remaining in the period. |
| `current_period` | integer | Latest period number as a STRING; null on older matches. |
| `clock_running` | logical | Game clock is running. |
| `home_score` | integer | Home final score, a STRING; null before tip-off. |
| `away_score` | integer | Away final score, a STRING; null before tip-off. |
| `round` | character | Round label: a number as a string in the regular season ("13"), words in the finals ("SEMI FINAL SERIES 1", "CHAMPIONSHIP") |
| `start_time` | character | Tip-off, UTC, with no offset or a Z suffix (ESPN dates the same Perth v SE Melbourne game 2025-12-18T10:30Z; Rosetta says 2025-12-18T10:30:00) |
| `status` | character | Provider fixture status: CONFIRMED, SCHEDULED. |
| `external_media_id` | character | Media id. Through NBL25 (2024-25): the Genius Sports match id (7 digits, e.g. 2636769; 6 or fewer in older seasons). From NBL26: mostly an 8-digit number of unverified origin (e.g. 61386925), date-like codes on some games (05042026 = DDMMYYYY on the 2026 Championship game, 1409252 = DDMMYY + n on the Blitz), null on many preseason games. NBL1 and WNBL 2025 carry 8-digit numbers. |
| `match_slug` | character | URL slug of the match page on nbl.com.au. |
| `match_title` | character | Match title ("<home> v <away> <men\|women> DD-MM-YYYY") |
| `secondary_external_id` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `live_exclusive` | logical | Match is a live-stream exclusive. |
| `season_id` | character | DataCore id (UUID v1, Sportradar) of the season; stable across seasons and the id to join on across routes. |
| `rosetta_season_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the season; routes that take this entity accept it as well as the DataCore id. |
| `season_year` | integer | Rosetta's year of the season: the START year for the NBL and WNBL from 1998-99 (2025 = 2025-26), the calendar year for NBL1 and the 1979-1997 winter seasons. See season_end_year for the sdv key. |
| `season_type` | character | Season type: regular (includes finals), preseason, in_season (cups, blitz, exhibition), test. |
| `season_competition_id` | character | Rosetta competition id (UUID v4) |
| `season_competition_name` | character | Name of the competition the season belongs to (e.g. NBL, WNBL, NBL1 North Men). |
| `season_competition_gender` | character | Gender (MALE/FEMALE upper case on seasons and players, male/female lower case on competitions and divisions; UNKNOWN seen) |
| `season_competition_league_name` | character | Name of the league the competition belongs to (NBL, WNBL or NBL1). |
| `season_competition_division` | character | NBL1 divisions (conference x gender); empty for NBL and WNBL. |
| `venue_id` | character | DataCore id (UUID v1, Sportradar) of the venue; stable across seasons and the id to join on across routes. |
| `rosetta_venue_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the venue; routes that take this entity accept it as well as the DataCore id. |
| `venue_name` | character | Name of the venue (arena), e.g. RAC Arena. |
| `venue_address` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_capacity` | integer | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_timezone` | character | IANA time zone of the venue, e.g. Australia/Sydney. |
| `venue_code` | character | Venue code; often null. |
| `venue_state` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_suburb` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_latitude` | numeric | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_longitude` | numeric | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_country` | character | Country; often null. |
| `home_team_id` | character | DataCore id (UUID v1, Sportradar) of the home team; stable across seasons and the id to join on across routes. |
| `home_team_name` | character | Display name of the team. |
| `home_team_external_team_name` | character | Team name as DataCore spells it. |
| `home_team_code` | character | Team code of the home team, e.g. PER or SYD. |
| `away_team_id` | character | DataCore id (UUID v1, Sportradar) of the away team; stable across seasons and the id to join on across routes. |
| `away_team_name` | character | Display name of the team. |
| `away_team_external_team_name` | character | Team name as DataCore spells it. |
| `away_team_code` | character | Team code of the away team, e.g. PER or SYD. |
| `data_source` | character | Backend that built the body: nexus-api or lambda-cache (observed). It varies between requests for the same match (the NBL 2025-26 sample game came back lambda-cache from /live/all and nexus-api from /match/{match_id} minutes apart, with identical play-by-play), so it says nothing about the data. |
| `match_status_string` | character | Status banner text, e.g. FINAL. |
| `play_by_play_updated_datetime` | character | Last play-by-play update, ISO 8601 UTC. |
| `season_end_year` | integer | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_match_by_id-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `sportsdataverse.errors.AssetFetchError` | the 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_match_by_id-example}

```python
nbl_rosetta_match_by_id(match_id='b217a699-4bef-11f0-bc08-933162362355')
```

_Last validated n/a._

## nbl_rosetta_match_player_box

One match's official player box score, one row per player line (period 0 = whole game).

**Endpoint URL:** `GET https://prod.rosetta.nbl.com.au/get/match/{match_id}/live/all`

**Valid URL:** [https://prod.rosetta.nbl.com.au/get/match/b217a699-4bef-11f0-bc08-933162362355/live/all](https://prod.rosetta.nbl.com.au/get/match/b217a699-4bef-11f0-bc08-933162362355/live/all)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | DataCore fixture id of the match (match_id of nbl_rosetta_matches), a UUID, e.g. b217a699-4bef-11f0-bc08-933162362355. |

### Returns {#nbl_rosetta_match_player_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | character | DataCore fixture id of the match (UUID v1): the {match_id} route key and fixtureId in play-by-play. Join on it. |
| `match_status` | character | Status of the match in Rosetta's words (complete for a finished game): box score and play-by-play of any other status are partial. |
| `stat_line_id` | character | Provider record hash of this statistics row (64 hex characters) |
| `rosetta_stat_line_id` | character | Rosetta id of this statistics row (UUID v4) |
| `assists` | integer | Assists in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `blocks` | integer | Blocks in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `defensive_rebounds` | integer | Defensive rebounds in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `efficiency` | integer | Efficiency rating (PTS + REB + AST + STL + BLK - missed FG - missed FT - TOV) in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `field_goals_attempted` | integer | Field goals attempted in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `field_goals_made` | integer | Field goals made in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `field_goals_percentage` | numeric | Field goals made / attempted, a 0-1 FRACTION. |
| `fouls` | integer | Fouls committed (personal + technical + others) in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `free_throws_attempted` | integer | Free throws attempted in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `free_throws_made` | integer | Free throws made in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `free_throws_percentage` | numeric | Free throws made / attempted, a 0-1 FRACTION. |
| `jersey_number` | character | Jersey number, a STRING. |
| `minutes` | numeric | Minutes played: "MM:SS" string in player_match_statistics, decimal minutes in calculated_player_statistics, player-minutes (integer, 5 x period length) in team rows; "00:00" or 0 where not recorded. |
| `minutes_string` | character | Minutes played as an ISO 8601 duration (PT30M32S); null on NBL1. |
| `offensive_rebounds` | integer | Offensive rebounds in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `participated` | logical | Player took part in the game. |
| `period` | integer | Period as a STRING: 0 = game or season total, 1-4 = quarters, 11/12/13 = first/second/third overtime (inferred: fewer teams have each) |
| `period_type` | character | Period type: regular (overtime unobserved) |
| `personal_fouls` | integer | Personal fouls in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `playing_position` | character | Playing position code (G, F, C, PG, SF, FC, ...); null when not recorded. |
| `plus_minus` | integer | Point differential while on court in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `points` | integer | Points in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `rebounds` | integer | Total rebounds in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `starter` | logical | Player started the game. |
| `steals` | integer | Steals in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `technical_fouls` | integer | Technical fouls in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `three_points_attempted` | integer | Three-point field goals attempted in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `three_points_made` | integer | Three-point field goals made in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `three_points_percentage` | numeric | Three-point field goals made / attempted, a 0-1 FRACTION. |
| `turnovers` | integer | Turnovers in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `two_points_attempted` | integer | Two-point field goals attempted in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `two_points_made` | integer | Two-point field goals made in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `two_points_percentage` | numeric | Two-point field goals made / attempted, a 0-1 FRACTION. |
| `rosetta_match_id` | character | Rosetta's match RECORD id (UUID v4) as carried by box-score and play-by-play rows; no route accepts it. Join on match_id. |
| `player_id` | character | DataCore id (UUID v1, Sportradar) of the player; stable across seasons and the id to join on across routes. |
| `player_first_name` | character | The player's given name, e.g. Elijah. |
| `player_last_name` | character | The player's family name, e.g. Pepper. |
| `player_full_name` | character | Given and family name. |
| `player_jersey_number` | character | Jersey number, a STRING. |
| `player_playing_position` | character | Playing position code (G, F, C, PG, SF, FC, ...); null when not recorded. |
| `team_id` | character | DataCore id (UUID v1, Sportradar) of the team; stable across seasons and the id to join on across routes. |
| `team_code` | character | Team code, e.g. PER (the {team_code} path value) |
| `team_name` | character | Name of the team as Rosetta spells it, e.g. Perth Wildcats. |
| `sc` | integer | NBL SuperCoach fantasy score for the game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_match_player_box-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `sportsdataverse.errors.AssetFetchError` | the 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_match_player_box-example}

```python
nbl_rosetta_match_player_box(match_id='b217a699-4bef-11f0-bc08-933162362355')
```

_Last validated n/a._

## nbl_rosetta_match_team_box

One match's team box score by period, one row per team and period (0 = whole game).

**Endpoint URL:** `GET https://prod.rosetta.nbl.com.au/get/match/{match_id}/live/all`

**Valid URL:** [https://prod.rosetta.nbl.com.au/get/match/b217a699-4bef-11f0-bc08-933162362355/live/all](https://prod.rosetta.nbl.com.au/get/match/b217a699-4bef-11f0-bc08-933162362355/live/all)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | DataCore fixture id of the match (match_id of nbl_rosetta_matches), a UUID, e.g. b217a699-4bef-11f0-bc08-933162362355. |

### Returns {#nbl_rosetta_match_team_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | character | DataCore fixture id of the match (UUID v1): the {match_id} route key and fixtureId in play-by-play. Join on it. |
| `match_status` | character | Status of the match in Rosetta's words (complete for a finished game): box score and play-by-play of any other status are partial. |
| `stat_line_id` | character | Provider record hash of this statistics row (64 hex characters) |
| `rosetta_stat_line_id` | character | Rosetta id of this statistics row (UUID v4) |
| `assists` | integer | Assists in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `blocks` | integer | Blocks in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `defensive_rebounds` | integer | Defensive rebounds in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `field_goals_attempted` | integer | Field goals attempted in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `field_goals_made` | integer | Field goals made in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `field_goals_percentage` | numeric | Field goals made / attempted, a 0-1 FRACTION. |
| `fouls` | integer | Fouls committed (personal + technical + others) in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `free_throws_attempted` | integer | Free throws attempted in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `free_throws_made` | integer | Free throws made in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `free_throws_percentage` | numeric | Free throws made / attempted, a 0-1 FRACTION. |
| `minutes` | numeric | Minutes played: "MM:SS" string in player_match_statistics, decimal minutes in calculated_player_statistics, player-minutes (integer, 5 x period length) in team rows; "00:00" or 0 where not recorded. |
| `offensive_rebounds` | integer | Offensive rebounds in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `period` | integer | Period as a STRING: 0 = game or season total, 1-4 = quarters, 11/12/13 = first/second/third overtime (inferred: fewer teams have each) |
| `period_type` | character | Period type: regular (overtime unobserved) |
| `personal_fouls` | integer | Personal fouls in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `points` | integer | Points in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `rebounds` | integer | Total rebounds in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `steals` | integer | Steals in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `technical_fouls` | integer | Technical fouls in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `three_points_attempted` | integer | Three-point field goals attempted in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `three_points_made` | integer | Three-point field goals made in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `three_points_percentage` | numeric | Three-point field goals made / attempted, a 0-1 FRACTION. |
| `turnovers` | integer | Turnovers in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `two_points_attempted` | integer | Two-point field goals attempted in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `two_points_made` | integer | Two-point field goals made in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `two_points_percentage` | numeric | Two-point field goals made / attempted, a 0-1 FRACTION. |
| `rosetta_match_id` | character | Rosetta's match RECORD id (UUID v4) as carried by box-score and play-by-play rows; no route accepts it. Join on match_id. |
| `team_id` | character | DataCore id (UUID v1, Sportradar) of the team; stable across seasons and the id to join on across routes. |
| `team_code` | character | Team code, e.g. PER (the {team_code} path value) |
| `team_name` | character | Name of the team as Rosetta spells it, e.g. Perth Wildcats. |
| `timeouts` | integer | Timeouts taken in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_match_team_box-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `sportsdataverse.errors.AssetFetchError` | the 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_match_team_box-example}

```python
nbl_rosetta_match_team_box(match_id='b217a699-4bef-11f0-bc08-933162362355')
```

_Last validated n/a._

## nbl_rosetta_match_player_period_box

One match's player box score by period (Rosetta-computed from the event log), one row per player and period.

**Endpoint URL:** `GET https://prod.rosetta.nbl.com.au/get/match/{match_id}/live/all`

**Valid URL:** [https://prod.rosetta.nbl.com.au/get/match/b217a699-4bef-11f0-bc08-933162362355/live/all](https://prod.rosetta.nbl.com.au/get/match/b217a699-4bef-11f0-bc08-933162362355/live/all)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | DataCore fixture id of the match (match_id of nbl_rosetta_matches), a UUID, e.g. b217a699-4bef-11f0-bc08-933162362355. |

### Returns {#nbl_rosetta_match_player_period_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | character | DataCore fixture id of the match (UUID v1): the {match_id} route key and fixtureId in play-by-play. Join on it. |
| `match_status` | character | Status of the match in Rosetta's words (complete for a finished game): box score and play-by-play of any other status are partial. |
| `player_id` | character | DataCore id (UUID v1, Sportradar) of the player; stable across seasons and the id to join on across routes. |
| `rosetta_player_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the player; routes that take this entity accept it as well as the DataCore id. |
| `player_first_name` | character | The player's given name, e.g. Elijah. |
| `player_last_name` | character | The player's family name, e.g. Pepper. |
| `player_jersey_number` | character | Jersey number, a STRING. |
| `player_playing_position` | character | Playing position code (G, F, C, PG, SF, FC, ...); null when not recorded. |
| `team_id` | character | DataCore id (UUID v1, Sportradar) of the team; stable across seasons and the id to join on across routes. |
| `rosetta_team_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the team; routes that take this entity accept it as well as the DataCore id. |
| `period` | integer | Period as a STRING: 0 = game or season total, 1-4 = quarters, 11/12/13 = first/second/third overtime (inferred: fewer teams have each) |
| `period_type` | character | Period type: regular (overtime unobserved) |
| `rosetta_match_id` | character | Rosetta's match RECORD id (UUID v4) as carried by box-score and play-by-play rows; no route accepts it. Join on match_id. |
| `assists` | integer | Assists in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `blocks` | integer | Blocks in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `defensive_rebounds` | integer | Defensive rebounds in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `field_goals_attempted` | integer | Field goals attempted in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `field_goals_made` | integer | Field goals made in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `field_goals_percentage` | numeric | Field goals made / attempted, a 0-1 FRACTION. |
| `fouls` | integer | Fouls committed (personal + technical + others) in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `free_throws_attempted` | integer | Free throws attempted in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `free_throws_made` | integer | Free throws made in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `free_throws_percentage` | numeric | Free throws made / attempted, a 0-1 FRACTION. |
| `minutes` | numeric | Minutes played: "MM:SS" string in player_match_statistics, decimal minutes in calculated_player_statistics, player-minutes (integer, 5 x period length) in team rows; "00:00" or 0 where not recorded. |
| `offensive_rebounds` | integer | Offensive rebounds in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `personal_fouls` | integer | Personal fouls in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `points` | integer | Points in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `rebounds` | integer | Total rebounds in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `steals` | integer | Steals in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `technical_fouls` | integer | Technical fouls in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `three_points_attempted` | integer | Three-point field goals attempted in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `three_points_made` | integer | Three-point field goals made in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `three_points_percentage` | numeric | Three-point field goals made / attempted, a 0-1 FRACTION. |
| `turnovers` | integer | Turnovers in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `two_points_attempted` | integer | Two-point field goals attempted in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `two_points_made` | integer | Two-point field goals made in the match, or in the period on period rows; 0 where the era did not record it (results-only seasons before pbp) |
| `two_points_percentage` | numeric | Two-point field goals made / attempted, a 0-1 FRACTION. |
| `jersey_number` | character | Jersey number, a STRING. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_match_player_period_box-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `sportsdataverse.errors.AssetFetchError` | the 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_match_player_period_box-example}

```python
nbl_rosetta_match_player_period_box(match_id='b217a699-4bef-11f0-bc08-933162362355')
```

_Last validated n/a._

## nbl_rosetta_match_pbp

One match's play-by-play with shot x/y, oldest first (NBL 2025-26 included, which ESPN's nbl feed lacks).

**Endpoint URL:** `GET https://prod.rosetta.nbl.com.au/get/match/{match_id}/live/all`

**Valid URL:** [https://prod.rosetta.nbl.com.au/get/match/b217a699-4bef-11f0-bc08-933162362355/live/all](https://prod.rosetta.nbl.com.au/get/match/b217a699-4bef-11f0-bc08-933162362355/live/all)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | DataCore fixture id of the match (match_id of nbl_rosetta_matches), a UUID, e.g. b217a699-4bef-11f0-bc08-933162362355. |

### Returns {#nbl_rosetta_match_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | character | DataCore fixture id of the match (UUID v1): the {match_id} route key and fixtureId in play-by-play. Join on it. |
| `rosetta_match_id` | character | Rosetta's match RECORD id (UUID v4) as carried by box-score and play-by-play rows; no route accepts it. Join on match_id. |
| `match_status` | character | Status of the match in Rosetta's words (complete for a finished game): box score and play-by-play of any other status are partial. |
| `action_id` | integer | Event sequence number within the match (ascending in feed time) |
| `period` | integer | Period number, an INTEGER here (1-4 = quarters) |
| `period_type` | character | Period type: regular (overtime unobserved) |
| `clock` | character | Game clock at the event, MM:SS remaining in the period; null on some feed-control events. |
| `clock_seconds` | numeric | Game clock at the event as seconds (parsed from clock, MM:SS); null for events without a clock. |
| `shot_clock` | character | Shot clock at the event, MM:SS; in Genius-era matches it repeats the game clock (no shot-clock data) |
| `shot_clock_seconds` | numeric | Shot clock at the event as seconds (parsed from shot_clock); in Genius-era backfills it repeats the game clock. |
| `score_home` | integer | Home team's running score after the event (score_1 in the body). |
| `score_away` | integer | Away team's running score after the event (score_2 in the body). |
| `action_type` | character | Event type: 2pt, 3pt, freeThrow, rebound, assist, steal, block, turnover, foul, substitution, timeOut, jumpBall, possession, possessionArrow, period, fixture, main (clock start/stop) |
| `sub_type` | character | Event detail (shot type, foul type, "1Of2" free throw, In/Out...); absent when the event has none. Two vocabularies: Title Case ("Jump Shot") in NBL/WNBL 2025-26 feeds (events carry fixtureId), camelCase ("jumpShot") in older backfills and NBL1. |
| `sub_type_key` | character | sub_type folded to snake_case, so the Title Case 2025-26 spelling and the camelCase backfill spelling match. |
| `success` | logical | Shot or free throw made (true) or missed (false). DataCore-era feeds (events with fixtureId) and NBL1 carry it only on shots and free throws (absent elsewhere); older backfills carry it on every event, true on non-shot events. |
| `team_id` | character | DataCore id (UUID v1, Sportradar) of the team; stable across seasons and the id to join on across routes. |
| `team_code` | character | Team code, e.g. PER (the {team_code} path value) |
| `team_name` | character | Name of the team as Rosetta spells it, e.g. Perth Wildcats. |
| `is_home` | logical | True when the event's team is the match's home team; null when the event has no team. |
| `player_id` | character | DataCore id (UUID v1, Sportradar) of the player; stable across seasons and the id to join on across routes. |
| `player_name` | character | Full name of the player the event belongs to; null for events no player owns (team rebounds, period starts). |
| `jersey_number` | character | Jersey number of the player at the event, as text (a WNBL backfill writes 6.0). |
| `x` | numeric | Shot or event position along the court length, percent 0-100, in game orientation (each team's shots sit at x > 50 for two periods and x < 50 for the others). Unverified: which end is x = 0; DataCore-era free-throw, foul and turnover coordinates are not at the free-throw line. Null where none was recorded. |
| `y` | numeric | Shot or event position across the court width, percent 0-100 (free throws and close shots cluster near 50). Unverified: which sideline is y = 0. Null where none was recorded. |
| `readable_action_type` | character | Human-readable event label, e.g. "Game - Game completed". |
| `system_action_type` | character | Machine event label, e.g. game_game_completed. |
| `timestamp` | character | Wall-clock time of the event, ISO 8601 without offset (UTC) |
| `feed` | character | Feed shape: datacore (NBL and WNBL 2025-26: events carry fixtureId, arrive newest first, Title Case sub_type, a real shot clock) or backfill (older seasons and NBL1 2025: no fixtureId, oldest first, camelCase sub_type, shot_clock repeating the game clock). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_match_pbp-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `sportsdataverse.errors.AssetFetchError` | the 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_match_pbp-example}

```python
nbl_rosetta_match_pbp(match_id='b217a699-4bef-11f0-bc08-933162362355')
```

_Last validated n/a._
