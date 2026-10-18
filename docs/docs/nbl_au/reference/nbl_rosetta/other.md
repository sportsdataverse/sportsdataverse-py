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
| `org` | character | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `season_id` | character | DataCore season id (UUID v1); accepted as {season_id}. |
| `rosetta_season_id` | character | Rosetta season id (UUID v4; v5 on NBL1); the {season_id} route key. |
| `name` | character | Season name as Rosetta spells it (NBL26, WNBL 2024/25, 2025 South Men); names drift, key on season_year. |
| `season_year` | integer | Rosetta's year of the season: the START year for the NBL and WNBL from 1998-99 (2025 = 2025-26), the calendar year for NBL1 and the 1979-1997 winter seasons. See season_end_year for the sdv key. |
| `season_type` | character | Season type: regular (includes finals), preseason, in_season (cups, blitz, exhibition), test. |
| `start_date` | character | First day of the season, ISO date-time without offset. |
| `end_date` | character | Last day of the season, ISO date-time without offset; null on older seasons. |
| `competition_league_name` | character | Name of the league the season's competition belongs to (NBL, WNBL or NBL1). |
| `competition_division` | character | NBL1 divisions (conference x gender); empty for NBL and WNBL. |
| `season_end_year` | integer | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_seasons-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `sportsdataverse.errors.AssetFetchError` | the 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_seasons-example}

```python
nbl_rosetta_seasons(org='nbl')
```

_Last validated n/a._

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
| `org` | character | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `season_id` | character | DataCore season id (UUID v1); accepted as {season_id}. |
| `rosetta_season_id` | character | Rosetta season id (UUID v4; v5 on NBL1); the {season_id} route key. |
| `name` | character | Season name as Rosetta spells it (NBL26, WNBL 2024/25); names drift, key on season_year. |
| `season_year` | integer | Rosetta's year of the season: the START year for the NBL and WNBL from 1998-99 (2025 = 2025-26), the calendar year for NBL1 and the 1979-1997 winter seasons. See season_end_year for the sdv key. |
| `season_type` | character | Season type: regular (includes finals), preseason, in_season (cups, blitz, exhibition), test. |
| `start_date` | character | First day of the season, ISO date-time without offset. |
| `end_date` | character | Last day of the season, ISO date-time without offset; null on older seasons. |
| `competition_name` | character | Name of the competition the season belongs to (e.g. NBL). |
| `competition_age_group` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `next_match_start_datetime` | character | Tip-off of the season's next match, UTC; null when none. |
| `season_end_year` | integer | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_seasons_current-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `sportsdataverse.errors.AssetFetchError` | the 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_seasons_current-example}

```python
nbl_rosetta_seasons_current(org='nbl')
```

_Last validated n/a._

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
| `org` | character | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `match_id` | character | DataCore fixture id (UUID v1); equal to `id`. |
| `external_media_id` | character | Media id. Through NBL25 (2024-25): the Genius Sports match id (7 digits, e.g. 2636769; 6 or fewer in older seasons). From NBL26: mostly an 8-digit number of unverified origin (e.g. 61386925), date-like codes on some games (05042026 = DDMMYYYY on the 2026 Championship game, 1409252 = DDMMYY + n on the Blitz), null on many preseason games. NBL1 and WNBL 2025 carry 8-digit numbers. |
| `date_updated` | character | Record last-edit time, ISO 8601 UTC. |
| `start_time` | character | Tip-off, UTC, with no offset or a Z suffix (ESPN dates the same Perth v SE Melbourne game 2025-12-18T10:30Z; Rosetta says 2025-12-18T10:30:00) |
| `start_time_datetime` | character | Tip-off, UTC; same value as start_time. |
| `round` | character | Round label: a number as a string in the regular season ("13"), words in the finals ("SEMI FINAL SERIES 1", "CHAMPIONSHIP") |
| `match_type` | character | Match type: regular, final (FINAL upper case seen) |
| `match_status` | character | Status of the match in Rosetta's words (complete for a finished game): box score and play-by-play of any other status are partial. |
| `status` | character | Provider fixture status: CONFIRMED, SCHEDULED. |
| `home_score` | integer | Home final score, a STRING; null before tip-off. |
| `away_score` | integer | Away final score, a STRING; null before tip-off. |
| `attendance` | integer | Attendance, a STRING; null when not recorded. |
| `match_slug` | character | URL slug of the match page on nbl.com.au. |
| `match_title` | character | Match title ("<home> v <away> <men\|women> DD-MM-YYYY") |
| `venue_id` | character | DataCore id (UUID v1, Sportradar) of the venue; stable across seasons and the id to join on across routes. |
| `rosetta_venue_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the venue; routes that take this entity accept it as well as the DataCore id. |
| `venue_name` | character | Name of the venue (arena), e.g. RAC Arena. |
| `venue_address` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_suburb` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_state` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_country` | character | Country; often null. |
| `venue_capacity` | integer | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_timezone` | character | IANA time zone of the venue, e.g. Australia/Sydney. |
| `venue_code` | character | Venue code; often null. |
| `venue_latitude` | numeric | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_longitude` | numeric | Unobserved in samples (null in every capture); type and meaning unverified. |
| `home_team_id` | character | DataCore id (UUID v1, Sportradar) of the home team; stable across seasons and the id to join on across routes. |
| `rosetta_home_team_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the home team; routes that take this entity accept it as well as the DataCore id. |
| `home_team_name` | character | Display name of the team. |
| `home_team_external_team_name` | character | Team name as DataCore spells it. |
| `home_team_code` | character | Team code of the home team, e.g. PER or SYD. |
| `home_team_division_abbreviation` | character | Division code, e.g. EM (East Men) |
| `home_team_division_name` | character | Division name (NBL1 conference + gender) |
| `home_team_conference_name` | character | Name of the NBL1 conference of the home team; null outside NBL1. |
| `away_team_id` | character | DataCore id (UUID v1, Sportradar) of the away team; stable across seasons and the id to join on across routes. |
| `rosetta_away_team_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the away team; routes that take this entity accept it as well as the DataCore id. |
| `away_team_name` | character | Display name of the team. |
| `away_team_external_team_name` | character | Team name as DataCore spells it. |
| `away_team_code` | character | Team code of the away team, e.g. PER or SYD. |
| `away_team_division_abbreviation` | character | Division code, e.g. EM (East Men) |
| `away_team_division_name` | character | Division name (NBL1 conference + gender) |
| `away_team_conference_name` | character | Name of the NBL1 conference of the away team; null outside NBL1. |
| `season_id` | character | DataCore id (UUID v1, Sportradar) of the season; stable across seasons and the id to join on across routes. |
| `rosetta_season_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the season; routes that take this entity accept it as well as the DataCore id. |
| `season_name` | character | Season name (NBL26, WNBL 2024/25, 2025 South Men) |
| `season_year` | integer | Rosetta's year of the season: the START year for the NBL and WNBL from 1998-99 (2025 = 2025-26), the calendar year for NBL1 and the 1979-1997 winter seasons. See season_end_year for the sdv key. |
| `season_type` | character | Season type: regular (includes finals), preseason, in_season (cups, blitz, exhibition), test. |
| `season_gender` | character | Gender (MALE/FEMALE upper case on seasons and players, male/female lower case on competitions and divisions; UNKNOWN seen) |
| `season_competition_id` | character | Rosetta competition id (UUID v4) |
| `season_competition_name` | character | Name of the competition the season belongs to (e.g. NBL, WNBL, NBL1 North Men). |
| `season_competition_gender` | character | Gender (MALE/FEMALE upper case on seasons and players, male/female lower case on competitions and divisions; UNKNOWN seen) |
| `season_competition_league_name` | character | Name of the league the competition belongs to (NBL, WNBL or NBL1). |
| `season_competition_division` | character | NBL1 divisions (conference x gender); empty for NBL and WNBL. |
| `season_end_year` | integer | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |
| `home_team_division` | character | NBL1 divisions (conference x gender); empty for NBL and WNBL. |
| `home_team_conference` | character | Conference of the division. |
| `away_team_division` | character | NBL1 divisions (conference x gender); empty for NBL and WNBL. |
| `away_team_conference` | character | Conference of the division. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_matches-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `sportsdataverse.errors.AssetFetchError` | the 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_matches-example}

```python
nbl_rosetta_matches(org='nbl', season=2026, season_type='regular')
```

_Last validated n/a._

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

### Raises {#nbl_rosetta_match-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `sportsdataverse.errors.AssetFetchError` | the 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_match-example}

```python
nbl_rosetta_match(match_id='b217a699-4bef-11f0-bc08-933162362355')
```

_Last validated n/a._

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
| `org` | character | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `rosetta_standing_id` | character | Rosetta's id of this ladder row (UUID v4); not an entity id. |
| `position` | integer | Ladder position (1 = top) |
| `won` | integer | Wins in the season. |
| `lost` | integer | Losses in the season. |
| `played` | integer | Games played in the season (wins plus losses). |
| `points_percentage` | numeric | points_for / points_against, a RATIO (1.1379 = 113.79 %) |
| `win_percentage` | numeric | Won / played, a 0-1 FRACTION. |
| `last_5` | character | Results string, W/L oldest first. Despite the name it can hold the whole season (33 characters in NBL 2025-26) |
| `streak` | integer | Current streak length (the type is the last letter of last_5) |
| `points_for` | integer | Points scored by the team in the season. |
| `points_against` | integer | Points conceded. |
| `away_losses` | integer | Away losses in the season. |
| `away_wins` | integer | Away wins in the season. |
| `home_losses` | integer | Home losses in the season. |
| `home_wins` | integer | Home wins in the season. |
| `team_id` | character | DataCore id (UUID v1, Sportradar) of the team; stable across seasons and the id to join on across routes. |
| `rosetta_team_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the team; routes that take this entity accept it as well as the DataCore id. |
| `team_code` | character | Team code, e.g. PER (the {team_code} path value) |
| `team_name` | character | Name of the team as Rosetta spells it, e.g. Perth Wildcats. |
| `season_year` | integer | Rosetta's year of the season: the START year for the NBL and WNBL from 1998-99 (2025 = 2025-26), the calendar year for NBL1 and the 1979-1997 winter seasons. See season_end_year for the sdv key. |
| `season_type` | character | Season type: regular (includes finals), preseason, in_season (cups, blitz, exhibition), test. |
| `season_competition_division` | character | NBL1 divisions (conference x gender); empty for NBL and WNBL. |
| `season_end_year` | integer | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_standings-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `sportsdataverse.errors.AssetFetchError` | the 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_standings-example}

```python
nbl_rosetta_standings(org='nbl', season=2026)
```

_Last validated n/a._

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
| `org` | character | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `team_name` | character | Name of the team, the key of Rosetta's outcomes map (the body carries no team id). |
| `wins` | integer | Wins of the team in the season type asked for. |
| `losses` | integer | Losses of the team in the season type asked for. |
| `last10` | character | Results of the team's last 10 games as a W/L string, oldest first. |
| `last5` | character | Results of the team's last 5 games as a W/L string, oldest first. |
| `last3` | character | Results of the team's last 3 games as a W/L string, oldest first. |
| `season_end_year` | integer | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_outcomes-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `sportsdataverse.errors.AssetFetchError` | the 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_outcomes-example}

```python
nbl_rosetta_outcomes(org='nbl', season=2026, season_type='regular')
```

_Last validated n/a._

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
| `season_competition_division` | character | NBL1 divisions (conference x gender); empty for NBL and WNBL. |
| `sc_total_score` | integer | SuperCoach score including double-double / triple-double bonuses. |
| `sc_total_score_without_dd_and_td` | integer | SuperCoach score without those bonuses. |
| `sc_breakdown_points_value` | integer | The stat's value for the game or season. |
| `sc_breakdown_points_points` | integer | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_rebounds_value` | integer | The stat's value for the game or season. |
| `sc_breakdown_rebounds_points` | integer | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_assists_value` | integer | The stat's value for the game or season. |
| `sc_breakdown_assists_points` | integer | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_blocks_value` | integer | The stat's value for the game or season. |
| `sc_breakdown_blocks_points` | integer | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_steals_value` | integer | The stat's value for the game or season. |
| `sc_breakdown_steals_points` | integer | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_turnovers_value` | integer | The stat's value for the game or season. |
| `sc_breakdown_turnovers_points` | integer | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_triple_double_value` | integer | The stat's value for the game or season. |
| `sc_breakdown_triple_double_points` | integer | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_double_figure_count` | integer | Stats in double figures (for the double-double / triple-double bonus) |
| `season_end_year` | integer | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_stats_leaders-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `sportsdataverse.errors.AssetFetchError` | the 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_stats_leaders-example}

```python
nbl_rosetta_stats_leaders(org='nbl', season_id='1f8e4a79-e98b-457b-85a5-e4b898c6c0bd')
```

_Last validated n/a._
