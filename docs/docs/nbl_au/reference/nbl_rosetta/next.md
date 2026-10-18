---
title: "NBL Australia (Rosetta) — NBL / WNBL / NBL1 Rosetta proxy (prod.rosetta.nbl.com.au, Australia) — Next"
sidebar_label: "Next"
sidebar_position: 2
description: "NBL Australia (Rosetta) — NBL / WNBL / NBL1 Rosetta proxy (prod.rosetta.nbl.com.au, Australia) — Next — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBL Australia (Rosetta) — NBL / WNBL / NBL1 Rosetta proxy (prod.rosetta.nbl.com.au, Australia) — Next

## nbl_rosetta_next_match

The organisation's next scheduled match (empty between seasons).

**Endpoint URL:** `GET https://prod.rosetta.nbl.com.au/get/next/match/for/{org}/{season}`

**Valid URL:** [https://prod.rosetta.nbl.com.au/get/next/match/for/nbl/2027](https://prod.rosetta.nbl.com.au/get/next/match/for/nbl/2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `org` | `org` |  |  | `Y` | Organisation: nbl (men's NBL), wnbl (women's) or nbl1 (the second tier, men's and women's conferences). |
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26). The runtime maps it to Rosetta's year: the START year for the NBL and WNBL from 1998-99, the calendar year for NBL1 and the NBL / WNBL winter seasons up to 1998. END 1998 and 1999 both read Rosetta year 1998, which holds two seasons; the matches frame keeps the one asked for. |

### Returns {#nbl_rosetta_next_match-returns}

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
| `home_score` | integer | Unobserved in samples (null in every capture); type and meaning unverified. |
| `away_score` | integer | Unobserved in samples (null in every capture); type and meaning unverified. |
| `attendance` | integer | Unobserved in samples (null in every capture); type and meaning unverified. |
| `match_slug` | character | URL slug of the match page on nbl.com.au. |
| `match_title` | character | Match title ("<home> v <away> <men\|women> DD-MM-YYYY") |
| `venue_id` | character | DataCore id (UUID v1, Sportradar) of the venue; stable across seasons and the id to join on across routes. |
| `rosetta_venue_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the venue; routes that take this entity accept it as well as the DataCore id. |
| `venue_name` | character | Name of the venue (arena), e.g. RAC Arena. |
| `venue_address` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_suburb` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_state` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_country` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
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
| `home_team_division` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `home_team_conference` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `away_team_id` | character | DataCore id (UUID v1, Sportradar) of the away team; stable across seasons and the id to join on across routes. |
| `rosetta_away_team_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the away team; routes that take this entity accept it as well as the DataCore id. |
| `away_team_name` | character | Display name of the team. |
| `away_team_external_team_name` | character | Team name as DataCore spells it. |
| `away_team_code` | character | Team code of the away team, e.g. PER or SYD. |
| `away_team_division` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `away_team_conference` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
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
| `season_competition_division` | character | Always an empty array in samples; element shape and meaning unverified. |
| `season_end_year` | integer | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_next_match-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_next_match-example}

```python
nbl_rosetta_next_match(org='nbl', season=2027)
```

_Last validated n/a._

## nbl_rosetta_next_match_team

Next match; the team segment is ignored by Rosetta (the body is the organisation's next match).

**Endpoint URL:** `GET https://prod.rosetta.nbl.com.au/get/next/match/for/team/{team_code}/{org}/{season}`

**Valid URL:** [https://prod.rosetta.nbl.com.au/get/next/match/for/team/PER/nbl/2027](https://prod.rosetta.nbl.com.au/get/next/match/for/team/PER/nbl/2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_code` | `team_code` |  | `Y` |  | Team code (team_code of nbl_rosetta_teams), e.g. PER for the Perth Wildcats. |
| `org` | `org` |  |  | `Y` | Organisation: nbl (men's NBL), wnbl (women's) or nbl1 (the second tier, men's and women's conferences). |
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26). The runtime maps it to Rosetta's year: the START year for the NBL and WNBL from 1998-99, the calendar year for NBL1 and the NBL / WNBL winter seasons up to 1998. END 1998 and 1999 both read Rosetta year 1998, which holds two seasons; the matches frame keeps the one asked for. |

### Returns {#nbl_rosetta_next_match_team-returns}

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
| `home_score` | integer | Unobserved in samples (null in every capture); type and meaning unverified. |
| `away_score` | integer | Unobserved in samples (null in every capture); type and meaning unverified. |
| `attendance` | integer | Unobserved in samples (null in every capture); type and meaning unverified. |
| `match_slug` | character | URL slug of the match page on nbl.com.au. |
| `match_title` | character | Match title ("<home> v <away> <men\|women> DD-MM-YYYY") |
| `venue_id` | character | DataCore id (UUID v1, Sportradar) of the venue; stable across seasons and the id to join on across routes. |
| `rosetta_venue_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the venue; routes that take this entity accept it as well as the DataCore id. |
| `venue_name` | character | Name of the venue (arena), e.g. RAC Arena. |
| `venue_address` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_suburb` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_state` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_country` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_capacity` | integer | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_timezone` | character | IANA time zone of the venue, e.g. Australia/Sydney. |
| `venue_code` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_latitude` | numeric | Unobserved in samples (null in every capture); type and meaning unverified. |
| `venue_longitude` | numeric | Unobserved in samples (null in every capture); type and meaning unverified. |
| `home_team_id` | character | DataCore id (UUID v1, Sportradar) of the home team; stable across seasons and the id to join on across routes. |
| `rosetta_home_team_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the home team; routes that take this entity accept it as well as the DataCore id. |
| `home_team_name` | character | Display name of the team. |
| `home_team_external_team_name` | character | Team name as DataCore spells it. |
| `home_team_code` | character | Team code of the home team, e.g. PER or SYD. |
| `home_team_division` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `home_team_conference` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `away_team_id` | character | DataCore id (UUID v1, Sportradar) of the away team; stable across seasons and the id to join on across routes. |
| `rosetta_away_team_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the away team; routes that take this entity accept it as well as the DataCore id. |
| `away_team_name` | character | Display name of the team. |
| `away_team_external_team_name` | character | Team name as DataCore spells it. |
| `away_team_code` | character | Team code of the away team, e.g. PER or SYD. |
| `away_team_division` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `away_team_conference` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `season_id` | character | DataCore id (UUID v1, Sportradar) of the season; stable across seasons and the id to join on across routes. |
| `rosetta_season_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the season; routes that take this entity accept it as well as the DataCore id. |
| `season_name` | character | Season name (NBL26, WNBL 2024/25, 2025 South Men) |
| `season_year` | integer | Rosetta's year of the season: the START year for the NBL and WNBL from 1998-99 (2025 = 2025-26), the calendar year for NBL1 and the 1979-1997 winter seasons. See season_end_year for the sdv key. |
| `season_type` | character | Season type: regular (includes finals), preseason, in_season (cups, blitz, exhibition), test. |
| `season_gender` | character | Gender (MALE/FEMALE upper case on seasons and players, male/female lower case on competitions and divisions; UNKNOWN seen) |
| `season_competition_id` | character | Rosetta competition id (UUID v4) |
| `season_competition_name` | character | Name of the competition the season belongs to (e.g. NBL, WNBL, NBL1 North Men). |
| `season_competition_gender` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `season_competition_league_name` | character | Name of the league the competition belongs to (NBL, WNBL or NBL1). |
| `season_competition_division` | character | Always an empty array in samples; element shape and meaning unverified. |
| `season_end_year` | integer | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_next_match_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_next_match_team-example}

```python
nbl_rosetta_next_match_team(org='nbl', season=2027, team_code='PER')
```

_Last validated n/a._

## nbl_rosetta_next_matches

Each team's next matches: opponent, venue and start time.

**Endpoint URL:** `GET https://prod.rosetta.nbl.com.au/get/next/matches/for/teams/in/{org}/{season}`

**Valid URL:** [https://prod.rosetta.nbl.com.au/get/next/matches/for/teams/in/nbl/2027](https://prod.rosetta.nbl.com.au/get/next/matches/for/teams/in/nbl/2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `org` | `org` |  |  | `Y` | Organisation: nbl (men's NBL), wnbl (women's) or nbl1 (the second tier, men's and women's conferences). |
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26). The runtime maps it to Rosetta's year: the START year for the NBL and WNBL from 1998-99, the calendar year for NBL1 and the NBL / WNBL winter seasons up to 1998. END 1998 and 1999 both read Rosetta year 1998, which holds two seasons; the matches frame keeps the one asked for. |

### Returns {#nbl_rosetta_next_matches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `org` | character | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `rosetta_team_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the team; routes that take this entity accept it as well as the DataCore id. |
| `team_name` | character | Name of the team as Rosetta spells it, e.g. Perth Wildcats. |
| `team_code` | character | Team code, e.g. PER (the {team_code} path value) |
| `division` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `conference` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `match_slug` | character | URL slug of the upcoming match, e.g. sydney-kings-v-nz-breakers-men-14-10-2026. |
| `match_title` | character | Title of the upcoming match, e.g. Sydney Kings v NZ Breakers men 14-10-2026. |
| `start_time_datetime` | character | Start of the upcoming match, UTC, ISO 8601 text without an offset. |
| `is_home` | logical | True when the row's team is the home team of the upcoming match. |
| `opponent_name` | character | Name of the opponent in the upcoming match. |
| `venue` | character | Name of the venue (arena) of the upcoming match. |
| `match_id` | character | DataCore fixture id of the match (UUID v1): the {match_id} route key and fixtureId in play-by-play. Join on it. |
| `rosetta_match_id` | character | Rosetta's match RECORD id (UUID v4) as carried by box-score and play-by-play rows; no route accepts it. Join on match_id. |
| `rosetta_opponent_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the opponent; routes that take this entity accept it as well as the DataCore id. |
| `season_end_year` | integer | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_next_matches-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_next_matches-example}

```python
nbl_rosetta_next_matches(org='nbl', season=2027)
```

_Last validated n/a._
