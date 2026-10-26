---
title: "NBL Australia (Rosetta) — NBL / WNBL / NBL1 Rosetta proxy (prod.rosetta.nbl.com.au, Australia) — Other"
sidebar_label: "Other"
sidebar_position: 5
description: "NBL Australia (Rosetta) — NBL / WNBL / NBL1 Rosetta proxy (prod.rosetta.nbl.com.au, Australia) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBL Australia (Rosetta) — NBL / WNBL / NBL1 Rosetta proxy (prod.rosetta.nbl.com.au, Australia) — Other

## nbl_rosetta_seasons

Every season of an organisation: regular, preseason, in-season cups and test seasons, with dates.

**Endpoint URL:** `GET https://prod.rosetta.nbl.com.au/get/{org}/seasons`

**Valid URL:** [https://prod.rosetta.nbl.com.au/get/nbl/seasons](https://prod.rosetta.nbl.com.au/get/nbl/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `org` | `org` |  |  | `Y` | Organisation: nbl (men's NBL), wnbl (women's) or nbl1 (the second tier, men's and women's conferences). |
| `limit` | `limit` |  |  | `Y` | Rows to return. Omitted = a page large enough for the whole list (a short page cut by Rosetta's path cache is refused). |

### Returns {#nbl_rosetta_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `org` | String | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `season_id` | String | DataCore season id (UUID v1); accepted as {season_id}. |
| `rosetta_season_id` | String | Rosetta season id (UUID v4; v5 on NBL1); the {season_id} route key. |
| `name` | String | Season name as Rosetta spells it (NBL26, WNBL 2024/25, 2025 South Men); names drift, key on season_year. |
| `season_year` | Int64 | Rosetta's year of the season: the START year for the NBL and WNBL from 1998-99 (2025 = 2025-26), the calendar year for NBL1 and the 1979-1997 winter seasons. See season_end_year for the sdv key. |
| `season_type` | String | Season type: regular (includes finals), preseason, in_season (cups, blitz, exhibition), test. |
| `start_date` | String | First day of the season, ISO date-time without offset. |
| `end_date` | String | Last day of the season, ISO date-time without offset; null on older seasons. |
| `competition_league_name` | String | Name of the league the season's competition belongs to (NBL, WNBL or NBL1). |
| `competition_division` | String | NBL1 divisions (conference x gender); empty for NBL and WNBL. |
| `season_end_year` | Int64 | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_seasons-example}

```python
nbl_rosetta_seasons(org='nbl')
```

## nbl_rosetta_seasons_current

The season in progress (empty between seasons: NoDataError).

**Endpoint URL:** `GET https://prod.rosetta.nbl.com.au/get/{org}/seasons/current`

**Valid URL:** [https://prod.rosetta.nbl.com.au/get/nbl/seasons/current](https://prod.rosetta.nbl.com.au/get/nbl/seasons/current)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `org` | `org` |  |  | `Y` | Organisation: nbl (men's NBL), wnbl (women's) or nbl1 (the second tier, men's and women's conferences). |

### Returns {#nbl_rosetta_seasons_current-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `org` | String | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `season_id` | String | DataCore season id (UUID v1); accepted as {season_id}. |
| `rosetta_season_id` | String | Rosetta season id (UUID v4; v5 on NBL1); the {season_id} route key. |
| `name` | String | Season name as Rosetta spells it (NBL26, WNBL 2024/25); names drift, key on season_year. |
| `season_year` | Int64 | Rosetta's year of the season: the START year for the NBL and WNBL from 1998-99 (2025 = 2025-26), the calendar year for NBL1 and the 1979-1997 winter seasons. See season_end_year for the sdv key. |
| `season_type` | String | Season type: regular (includes finals), preseason, in_season (cups, blitz, exhibition), test. |
| `start_date` | String | First day of the season, ISO date-time without offset. |
| `end_date` | String | Last day of the season, ISO date-time without offset; null on older seasons. |
| `competition_name` | String | Name of the competition the season belongs to (e.g. NBL). |
| `competition_age_group` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `next_match_start_datetime` | String | Tip-off of the season's next match, UTC; null when none. |
| `season_end_year` | Int64 | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_seasons_current-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_seasons_current-example}

```python
nbl_rosetta_seasons_current(org='nbl')
```

## nbl_rosetta_matches

Matches of a season: ids, teams, venue, status, scores and start time (UTC).

**Endpoint URL:** `GET https://prod.rosetta.nbl.com.au/get/{org}/matches/in/season/{season}/{season_type}`

**Valid URL:** [https://prod.rosetta.nbl.com.au/get/nbl/matches/in/season/2026/regular](https://prod.rosetta.nbl.com.au/get/nbl/matches/in/season/2026/regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26). The runtime maps it to Rosetta's year: the START year for the NBL and WNBL from 1998-99, the calendar year for NBL1 and the NBL / WNBL winter seasons up to 1998. END 1998 and 1999 both read Rosetta year 1998, which holds two seasons; the matches frame keeps the one asked for. |
| `org` | `org` |  |  | `Y` | Organisation: nbl (men's NBL), wnbl (women's) or nbl1 (the second tier, men's and women's conferences). |
| `season_type` | `season_type` |  |  | `Y` | Season type: all (every type of the year), regular (includes finals), preseason, in_season (cups, Blitz, NBAxNBL, All-Star) or test. |
| `limit` | `limit` |  |  | `Y` | Rows to return. Omitted = a page large enough for the whole list (a short page cut by Rosetta's path cache is refused). |
| `offset` | `offset` |  |  | `Y` | Rows to skip. Pages can overlap, so prefer one large limit. |

### Returns {#nbl_rosetta_matches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `org` | String | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `match_id` | String | DataCore fixture id (UUID v1); equal to `id`. |
| `external_media_id` | String | Media id. Through NBL25 (2024-25): the Genius Sports match id (7 digits, e.g. 2636769; 6 or fewer in older seasons). From NBL26: mostly an 8-digit number of unverified origin (e.g. 61386925), date-like codes on some games (05042026 = DDMMYYYY on the 2026 Championship game, 1409252 = DDMMYY + n on the Blitz), null on many preseason games. NBL1 and WNBL 2025 carry 8-digit numbers. |
| `date_updated` | String | Record last-edit time, ISO 8601 UTC. |
| `start_time` | String | Tip-off, UTC, with no offset or a Z suffix (ESPN dates the same Perth v SE Melbourne game 2025-12-18T10:30Z; Rosetta says 2025-12-18T10:30:00) |
| `start_time_datetime` | String | Tip-off, UTC; same value as start_time. |
| `round` | String | Round label: a number as a string in the regular season ("13"), words in the finals ("SEMI FINAL SERIES 1", "CHAMPIONSHIP") |
| `match_type` | String | Match type: regular, final (FINAL upper case seen) |
| `match_status` | String | Status of the match in Rosetta's words (complete for a finished game): box score and play-by-play of any other status are partial. |
| `status` | String | Provider fixture status: CONFIRMED, SCHEDULED. |
| `home_score` | Int64 | Home final score, a STRING; null before tip-off. |
| `away_score` | Int64 | Away final score, a STRING; null before tip-off. |
| `attendance` | Int64 | Attendance, a STRING; null when not recorded. |
| `match_slug` | String | URL slug of the match page on nbl.com.au. |
| `match_title` | String | Match title ("<home> v <away> <men\|women> DD-MM-YYYY") |
| `venue_id` | String | DataCore id (UUID v1, Sportradar) of the venue; stable across seasons and the id to join on across routes. |
| `rosetta_venue_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the venue; routes that take this entity accept it as well as the DataCore id. |
| `venue_name` | String | Name of the venue (arena), e.g. RAC Arena. |
| `venue_address` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_suburb` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_state` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_country` | String | Country; often null. |
| `venue_capacity` | Int64 | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_timezone` | String | IANA time zone of the venue, e.g. Australia/Sydney. |
| `venue_code` | String | Venue code; often null. |
| `venue_latitude` | Float64 | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_longitude` | Float64 | Unobserved in samples (null in every capture); type and meaning unverified. |
| `home_team_id` | String | DataCore id (UUID v1, Sportradar) of the home team; stable across seasons and the id to join on across routes. |
| `rosetta_home_team_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the home team; routes that take this entity accept it as well as the DataCore id. |
| `home_team_name` | String | Display name of the team. |
| `home_team_external_team_name` | String | Team name as DataCore spells it. |
| `home_team_code` | String | Team code of the home team, e.g. PER or SYD. |
| `home_team_division_abbreviation` | String | Division code, e.g. EM (East Men) |
| `home_team_division_name` | String | Division name (NBL1 conference + gender) |
| `home_team_conference_name` | String | Name of the NBL1 conference of the home team; null outside NBL1. |
| `away_team_id` | String | DataCore id (UUID v1, Sportradar) of the away team; stable across seasons and the id to join on across routes. |
| `rosetta_away_team_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the away team; routes that take this entity accept it as well as the DataCore id. |
| `away_team_name` | String | Display name of the team. |
| `away_team_external_team_name` | String | Team name as DataCore spells it. |
| `away_team_code` | String | Team code of the away team, e.g. PER or SYD. |
| `away_team_division_abbreviation` | String | Division code, e.g. EM (East Men) |
| `away_team_division_name` | String | Division name (NBL1 conference + gender) |
| `away_team_conference_name` | String | Name of the NBL1 conference of the away team; null outside NBL1. |
| `season_id` | String | DataCore id (UUID v1, Sportradar) of the season; stable across seasons and the id to join on across routes. |
| `rosetta_season_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the season; routes that take this entity accept it as well as the DataCore id. |
| `season_name` | String | Season name (NBL26, WNBL 2024/25, 2025 South Men) |
| `season_year` | Int64 | Rosetta's year of the season: the START year for the NBL and WNBL from 1998-99 (2025 = 2025-26), the calendar year for NBL1 and the 1979-1997 winter seasons. See season_end_year for the sdv key. |
| `season_type` | String | Season type: regular (includes finals), preseason, in_season (cups, blitz, exhibition), test. |
| `season_gender` | String | Gender (MALE/FEMALE upper case on seasons and players, male/female lower case on competitions and divisions; UNKNOWN seen) |
| `season_competition_id` | String | Rosetta competition id (UUID v4) |
| `season_competition_name` | String | Name of the competition the season belongs to (e.g. NBL, WNBL, NBL1 North Men). |
| `season_competition_gender` | String | Gender (MALE/FEMALE upper case on seasons and players, male/female lower case on competitions and divisions; UNKNOWN seen) |
| `season_competition_league_name` | String | Name of the league the competition belongs to (NBL, WNBL or NBL1). |
| `season_competition_division` | String | NBL1 divisions (conference x gender); empty for NBL and WNBL. |
| `season_end_year` | Int64 | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |
| `home_team_division` | String | NBL1 divisions (conference x gender); empty for NBL and WNBL. |
| `home_team_conference` | String | Conference of the division. |
| `away_team_division` | String | NBL1 divisions (conference x gender); empty for NBL and WNBL. |
| `away_team_conference` | String | Conference of the division. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_matches-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_matches-example}

```python
nbl_rosetta_matches(org='nbl', season=2026, season_type='regular')
```

## nbl_rosetta_match

One match's header: status, scores, clock, venue, season and teams. Box score and play-by-play: the match_* frames.

**Endpoint URL:** `GET https://prod.rosetta.nbl.com.au/get/match/{match_id}/live/all`

**Valid URL:** [https://prod.rosetta.nbl.com.au/get/match/b217a699-4bef-11f0-bc08-933162362355/live/all](https://prod.rosetta.nbl.com.au/get/match/b217a699-4bef-11f0-bc08-933162362355/live/all)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | DataCore fixture id of the match (match_id of nbl_rosetta_matches), a UUID, e.g. b217a699-4bef-11f0-bc08-933162362355. |

### Returns {#nbl_rosetta_match-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `org` | String | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `match_id` | String | DataCore fixture id (UUID v1); equal to `id`. |
| `match_status` | String | Status of the match in Rosetta's words (complete for a finished game): box score and play-by-play of any other status are partial. |
| `start_time_datetime` | String | Tip-off, UTC; same value as start_time. |
| `match_type` | String | Match type: regular, final (FINAL upper case seen) |
| `attendance` | Int64 | Attendance, a STRING; null when not recorded. |
| `clock` | String | Game clock of the latest event, HH:MM:SS remaining in the period. |
| `current_period` | Int64 | Latest period number as a STRING; null on older matches. |
| `clock_running` | Boolean | Game clock is running. |
| `home_score` | Int64 | Home final score, a STRING; null before tip-off. |
| `away_score` | Int64 | Away final score, a STRING; null before tip-off. |
| `round` | String | Round label: a number as a string in the regular season ("13"), words in the finals ("SEMI FINAL SERIES 1", "CHAMPIONSHIP") |
| `start_time` | String | Tip-off, UTC, with no offset or a Z suffix (ESPN dates the same Perth v SE Melbourne game 2025-12-18T10:30Z; Rosetta says 2025-12-18T10:30:00) |
| `status` | String | Provider fixture status: CONFIRMED, SCHEDULED. |
| `external_media_id` | String | Media id. Through NBL25 (2024-25): the Genius Sports match id (7 digits, e.g. 2636769; 6 or fewer in older seasons). From NBL26: mostly an 8-digit number of unverified origin (e.g. 61386925), date-like codes on some games (05042026 = DDMMYYYY on the 2026 Championship game, 1409252 = DDMMYY + n on the Blitz), null on many preseason games. NBL1 and WNBL 2025 carry 8-digit numbers. |
| `match_slug` | String | URL slug of the match page on nbl.com.au. |
| `match_title` | String | Match title ("<home> v <away> <men\|women> DD-MM-YYYY") |
| `secondary_external_id` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `live_exclusive` | Boolean | Match is a live-stream exclusive. |
| `season_id` | String | DataCore id (UUID v1, Sportradar) of the season; stable across seasons and the id to join on across routes. |
| `rosetta_season_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the season; routes that take this entity accept it as well as the DataCore id. |
| `season_year` | Int64 | Rosetta's year of the season: the START year for the NBL and WNBL from 1998-99 (2025 = 2025-26), the calendar year for NBL1 and the 1979-1997 winter seasons. See season_end_year for the sdv key. |
| `season_type` | String | Season type: regular (includes finals), preseason, in_season (cups, blitz, exhibition), test. |
| `season_competition_id` | String | Rosetta competition id (UUID v4) |
| `season_competition_name` | String | Name of the competition the season belongs to (e.g. NBL, WNBL, NBL1 North Men). |
| `season_competition_gender` | String | Gender (MALE/FEMALE upper case on seasons and players, male/female lower case on competitions and divisions; UNKNOWN seen) |
| `season_competition_league_name` | String | Name of the league the competition belongs to (NBL, WNBL or NBL1). |
| `season_competition_division` | String | NBL1 divisions (conference x gender); empty for NBL and WNBL. |
| `venue_id` | String | DataCore id (UUID v1, Sportradar) of the venue; stable across seasons and the id to join on across routes. |
| `rosetta_venue_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the venue; routes that take this entity accept it as well as the DataCore id. |
| `venue_name` | String | Name of the venue (arena), e.g. RAC Arena. |
| `venue_address` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_capacity` | Int64 | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_timezone` | String | IANA time zone of the venue, e.g. Australia/Sydney. |
| `venue_code` | String | Venue code; often null. |
| `venue_state` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_suburb` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_latitude` | Float64 | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_longitude` | Float64 | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_country` | String | Country; often null. |
| `home_team_id` | String | DataCore id (UUID v1, Sportradar) of the home team; stable across seasons and the id to join on across routes. |
| `home_team_name` | String | Display name of the team. |
| `home_team_external_team_name` | String | Team name as DataCore spells it. |
| `home_team_code` | String | Team code of the home team, e.g. PER or SYD. |
| `away_team_id` | String | DataCore id (UUID v1, Sportradar) of the away team; stable across seasons and the id to join on across routes. |
| `away_team_name` | String | Display name of the team. |
| `away_team_external_team_name` | String | Team name as DataCore spells it. |
| `away_team_code` | String | Team code of the away team, e.g. PER or SYD. |
| `data_source` | String | Backend that built the body: nexus-api or lambda-cache (observed). It varies between requests for the same match (the NBL 2025-26 sample game came back lambda-cache from /live/all and nexus-api from /match/{match_id} minutes apart, with identical play-by-play), so it says nothing about the data. |
| `match_status_string` | String | Status banner text, e.g. FINAL. |
| `play_by_play_updated_datetime` | String | Last play-by-play update, ISO 8601 UTC. |
| `season_end_year` | Int64 | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_match-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_match-example}

```python
nbl_rosetta_match(match_id='b217a699-4bef-11f0-bc08-933162362355')
```

## nbl_rosetta_standings

Regular-season ladder: position, record, points for and against, home and away splits and form.

**Endpoint URL:** `GET https://prod.rosetta.nbl.com.au/get/{org}/standings/{season}/regular`

**Valid URL:** [https://prod.rosetta.nbl.com.au/get/nbl/standings/2026/regular](https://prod.rosetta.nbl.com.au/get/nbl/standings/2026/regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26). The runtime maps it to Rosetta's year: the START year for the NBL and WNBL from 1998-99, the calendar year for NBL1 and the NBL / WNBL winter seasons up to 1998. END 1998 and 1999 both read Rosetta year 1998, which holds two seasons; the matches frame keeps the one asked for. |
| `org` | `org` |  |  | `Y` | Organisation: nbl (men's NBL), wnbl (women's) or nbl1 (the second tier, men's and women's conferences). |
| `limit` | `limit` |  |  | `Y` | Rows to return. Omitted = a page large enough for the whole list (a short page cut by Rosetta's path cache is refused). |

### Returns {#nbl_rosetta_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `org` | String | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `rosetta_standing_id` | String | Rosetta's id of this ladder row (UUID v4); not an entity id. |
| `position` | Int64 | Ladder position (1 = top) |
| `won` | Int64 | Wins in the season. |
| `lost` | Int64 | Losses in the season. |
| `played` | Int64 | Games played in the season (wins plus losses). |
| `points_percentage` | Float64 | points_for / points_against, a RATIO (1.1379 = 113.79 %) |
| `win_percentage` | Float64 | Won / played, a 0-1 FRACTION. |
| `last_5` | String | Results string, W/L oldest first. Despite the name it can hold the whole season (33 characters in NBL 2025-26) |
| `streak` | Int64 | Current streak length (the type is the last letter of last_5) |
| `points_for` | Int64 | Points scored by the team in the season. |
| `points_against` | Int64 | Points conceded. |
| `away_losses` | Int64 | Away losses in the season. |
| `away_wins` | Int64 | Away wins in the season. |
| `home_losses` | Int64 | Home losses in the season. |
| `home_wins` | Int64 | Home wins in the season. |
| `team_id` | String | DataCore id (UUID v1, Sportradar) of the team; stable across seasons and the id to join on across routes. |
| `rosetta_team_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the team; routes that take this entity accept it as well as the DataCore id. |
| `team_code` | String | Team code, e.g. PER (the {team_code} path value) |
| `team_name` | String | Name of the team as Rosetta spells it, e.g. Perth Wildcats. |
| `season_year` | Int64 | Rosetta's year of the season: the START year for the NBL and WNBL from 1998-99 (2025 = 2025-26), the calendar year for NBL1 and the 1979-1997 winter seasons. See season_end_year for the sdv key. |
| `season_type` | String | Season type: regular (includes finals), preseason, in_season (cups, blitz, exhibition), test. |
| `season_competition_division` | String | NBL1 divisions (conference x gender); empty for NBL and WNBL. |
| `season_end_year` | Int64 | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_standings-example}

```python
nbl_rosetta_standings(org='nbl', season=2026)
```

## nbl_rosetta_outcomes

Win-loss record and last 10, 5 and 3 results per team, keyed by team name.

**Endpoint URL:** `GET https://prod.rosetta.nbl.com.au/get/match/outcomes/for/{org}/teams/in/season/{season}/{season_type}`

**Valid URL:** [https://prod.rosetta.nbl.com.au/get/match/outcomes/for/nbl/teams/in/season/2026/regular](https://prod.rosetta.nbl.com.au/get/match/outcomes/for/nbl/teams/in/season/2026/regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26). The runtime maps it to Rosetta's year: the START year for the NBL and WNBL from 1998-99, the calendar year for NBL1 and the NBL / WNBL winter seasons up to 1998. END 1998 and 1999 both read Rosetta year 1998, which holds two seasons; the matches frame keeps the one asked for. |
| `org` | `org` |  |  | `Y` | Organisation: nbl (men's NBL), wnbl (women's) or nbl1 (the second tier, men's and women's conferences). |
| `season_type` | `season_type` |  |  | `Y` | Season type: all (every type of the year), regular (includes finals), preseason, in_season (cups, Blitz, NBAxNBL, All-Star) or test. |

### Returns {#nbl_rosetta_outcomes-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `org` | String | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `team_name` | String | Name of the team, the key of Rosetta's outcomes map (the body carries no team id). |
| `wins` | Int64 | Wins of the team in the season type asked for. |
| `losses` | Int64 | Losses of the team in the season type asked for. |
| `last10` | String | Results of the team's last 10 games as a W/L string, oldest first. |
| `last5` | String | Results of the team's last 5 games as a W/L string, oldest first. |
| `last3` | String | Results of the team's last 3 games as a W/L string, oldest first. |
| `season_end_year` | Int64 | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_outcomes-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_outcomes-example}

```python
nbl_rosetta_outcomes(org='nbl', season=2026, season_type='regular')
```

## nbl_rosetta_stats_leaders

Player season totals and averages of a season (the leaderboard), one row per player and period.

**Endpoint URL:** `GET https://prod.rosetta.nbl.com.au/get/{org}/stats/leaders/for/season/id/{season_id}`

**Valid URL:** [https://prod.rosetta.nbl.com.au/get/nbl/stats/leaders/for/season/id/1f8e4a79-e98b-457b-85a5-e4b898c6c0bd](https://prod.rosetta.nbl.com.au/get/nbl/stats/leaders/for/season/id/1f8e4a79-e98b-457b-85a5-e4b898c6c0bd)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | Season id: the DataCore season id (season_id of nbl_rosetta_seasons) or Rosetta's own id (rosetta_season_id); either is accepted. Not the year. |
| `org` | `org` |  |  | `Y` | Organisation: nbl (men's NBL), wnbl (women's) or nbl1 (the second tier, men's and women's conferences). |
| `limit` | `limit` |  |  | `Y` | Rows to return. Omitted = a page large enough for the whole list (a short page cut by Rosetta's path cache is refused). |

### Returns {#nbl_rosetta_stats_leaders-returns}

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
| `season_competition_division` | String | NBL1 divisions (conference x gender); empty for NBL and WNBL. |
| `sc_total_score` | Int64 | SuperCoach score including double-double / triple-double bonuses. |
| `sc_total_score_without_dd_and_td` | Int64 | SuperCoach score without those bonuses. |
| `sc_breakdown_points_value` | Int64 | The stat's value for the game or season. |
| `sc_breakdown_points_points` | Int64 | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_rebounds_value` | Int64 | The stat's value for the game or season. |
| `sc_breakdown_rebounds_points` | Int64 | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_assists_value` | Int64 | The stat's value for the game or season. |
| `sc_breakdown_assists_points` | Int64 | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_blocks_value` | Int64 | The stat's value for the game or season. |
| `sc_breakdown_blocks_points` | Int64 | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_steals_value` | Int64 | The stat's value for the game or season. |
| `sc_breakdown_steals_points` | Int64 | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_turnovers_value` | Int64 | The stat's value for the game or season. |
| `sc_breakdown_turnovers_points` | Int64 | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_triple_double_value` | Int64 | The stat's value for the game or season. |
| `sc_breakdown_triple_double_points` | Int64 | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_double_figure_count` | Int64 | Stats in double figures (for the double-double / triple-double bonus) |
| `season_end_year` | Int64 | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_stats_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_stats_leaders-example}

```python
nbl_rosetta_stats_leaders(org='nbl', season_id='1f8e4a79-e98b-457b-85a5-e4b898c6c0bd')
```
