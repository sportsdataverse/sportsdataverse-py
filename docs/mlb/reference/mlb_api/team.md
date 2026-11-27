# MLB — MLB Stats API — Team

> MLB — MLB Stats API — Team — function reference in sdv-py, the SportsDataverse Python package.

## mlb_team

GET /api/v1/teams/{teamId} — single team detail.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/{team_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/10?sportId=1](https://statsapi.mlb.com/api/v1/teams/10?sportId=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `all_star_status` | String | All-star status flag. |
| `id` | Int64 | Id. |
| `name` | String | Display name. |
| `link` | String | API link to the game feed. |
| `season` | Int64 | Season year. |
| `team_code` | String | Internal team code. |
| `file_code` | String | File code abbreviation. |
| `abbreviation` | String | Short abbreviation. |
| `team_name` | String | Team name. |
| `location_name` | String | Team location (city). |
| `first_year_of_play` | String | First year the franchise played. |
| `short_name` | String | Short display name. |
| `franchise_name` | String | Franchise name. |
| `club_name` | String | Club name. |
| `active` | Boolean | Whether the player is currently active. |
| `spring_league_id` | Int64 | Spring league MLBAM ID. |
| `spring_league_name` | String | Spring league name. |
| `spring_league_link` | String | API link to the spring league. |
| `spring_league_abbreviation` | String | Spring league abbreviation. |
| `venue_id` | Int64 | MLBAM venue ID. |
| `venue_name` | String | Venue name. |
| `venue_link` | String | API link to the venue. |
| `spring_venue_id` | Int64 | Spring training venue MLBAM ID. |
| `spring_venue_link` | String | API link to the spring venue. |
| `league_id` | Int64 | League MLBAM ID. |
| `league_name` | String | League name. |
| `league_link` | String | API link to the league. |
| `division_id` | Int64 | Division MLBAM ID. |
| `division_name` | String | Division name. |
| `division_link` | String | API link to the division. |
| `sport_id` | Int64 | Sport MLBAM ID. |
| `sport_link` | String | API link to the sport. |
| `sport_name` | String | Sport name (e.g., Major League Baseball). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_team-example}

```python
mlb_team(team_id=10)
```

## mlb_team_roster

GET /api/v1/teams/{teamId}/roster — team roster.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/{team_id}/roster`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/10/roster?rosterType=active](https://statsapi.mlb.com/api/v1/teams/10/roster?rosterType=active)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `rosterType` | `roster_type` |  |  | `Y` | rosterType query parameter. |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_team_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `jersey_number` | String | Jersey number worn (often blank for non-uniformed roles). |
| `person_id` | Int64 | MLB player ID. |
| `person_full_name` | String | Player full name. |
| `person_link` | String | API relative link to the person. |
| `position_code` | String | Numeric scorekeeping position code. |
| `position_name` | String | Full position name (e.g. 'Point Guard', 'Goalkeeper'). |
| `position_type` | String | Position category (e.g. 'Pitcher', 'Infielder'). |
| `position_abbreviation` | String | Position abbreviation. |
| `status_code` | String | Status code identifier (e.g. 'S', 'P', 'I', 'F'). |
| `status_description` | String | Roster status description (e.g. 'Active'). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_team_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_team_roster-example}

```python
mlb_team_roster(team_id=10)
```

## mlb_team_alumni

GET /api/v1/teams/{teamId}/alumni — players who played for this team in a season.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/{team_id}/alumni`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/10/alumni?group=hitting](https://statsapi.mlb.com/api/v1/teams/10/alumni?group=hitting)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `group` | `group` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |

### Returns {#mlb_team_alumni-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Id. |
| `full_name` | String | Player's full name. |
| `link` | String | API link to the game feed. |
| `first_name` | String | Player first name. |
| `last_name` | String | Player last name. |
| `primary_number` | String | Player uniform number. |
| `birth_date` | String | Date of birth (YYYY-MM-DD). |
| `current_age` | Int64 | Current age in years. |
| `birth_city` | String | City of birth. |
| `birth_country` | String | Country of birth. |
| `height` | String | Height (feet and inches). |
| `weight` | Int64 | Weight in pounds. |
| `active` | Boolean | Whether the player is currently active. |
| `use_name` | String | Preferred first name. |
| `use_last_name` | String | Preferred last name. |
| `middle_name` | String | Player middle name. |
| `boxscore_name` | String | Name as shown in box scores. |
| `nick_name` | String | Player nickname. |
| `gender` | String | Player gender. |
| `is_player` | Boolean | Whether the person is a player. |
| `is_verified` | Boolean | Whether the player profile is verified. |
| `pronunciation` | String | Phonetic name pronunciation. |
| `mlb_debut_date` | String | MLB debut date (YYYY-MM-DD). |
| `name_first_last` | String | Name in first-last order. |
| `name_slug` | String | URL-friendly name slug. |
| `first_last_name` | String | First and last name. |
| `last_first_name` | String | Name in last, first order. |
| `last_init_name` | String | Last name with first initial. |
| `init_last_name` | String | First initial with last name. |
| `full_fml_name` | String | Full name (first-middle-last). |
| `full_lfm_name` | String | Full name (last-first-middle). |
| `strike_zone_top` | Float64 | Top of the player's strike zone (feet). |
| `strike_zone_bottom` | Float64 | Bottom of the player's strike zone (feet). |
| `alumni_last_season` | String | Last season the player was with the team. |
| `primary_position_code` | String | Primary position code. |
| `primary_position_name` | String | Primary fielding position name. |
| `primary_position_type` | String | Primary position type (e.g. Infielder). |
| `primary_position_abbreviation` | String | Primary position abbreviation. |
| `bat_side_code` | String | Batting side code (L/R/S). |
| `bat_side_description` | String | Batting side description. |
| `pitch_hand_code` | String | Throwing hand code (L/R). |
| `pitch_hand_description` | String | Throwing hand description. |
| `birth_state_province` | String | State or province of birth. |
| `draft_year` | Float64 | Year the player was drafted. |
| `last_played_date` | String | Date of last MLB game played. |
| `name_matrilineal` | String | Maternal family name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_team_alumni-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_team_alumni-example}

```python
mlb_team_alumni(team_id=10)
```

## mlb_team_affiliates

GET /api/v1/teams/affiliates — org affiliates (MLB parent → minor league chain).

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/affiliates`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/affiliates?sportId=1](https://statsapi.mlb.com/api/v1/teams/affiliates?sportId=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamIds` | `team_ids` |  |  | `Y` | teamIds query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |

### Returns {#mlb_team_affiliates-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `all_star_status` | String | All-star status flag. |
| `id` | Int64 | Id. |
| `name` | String | Display name. |
| `link` | String | API link to the game feed. |
| `season` | Int64 | Season year. |
| `team_code` | String | Internal team code. |
| `file_code` | String | File code abbreviation. |
| `abbreviation` | String | Short abbreviation. |
| `team_name` | String | Team name. |
| `location_name` | String | Team location (city). |
| `first_year_of_play` | String | First year the franchise played. |
| `short_name` | String | Short display name. |
| `franchise_name` | String | Franchise name. |
| `club_name` | String | Club name. |
| `active` | Boolean | Whether the player is currently active. |
| `spring_league_id` | Float64 | Spring league MLBAM ID. |
| `spring_league_name` | String | Spring league name. |
| `spring_league_link` | String | API link to the spring league. |
| `spring_league_abbreviation` | String | Spring league abbreviation. |
| `venue_id` | Int64 | MLBAM venue ID. |
| `venue_name` | String | Venue name. |
| `venue_link` | String | API link to the venue. |
| `spring_venue_id` | Float64 | Spring training venue MLBAM ID. |
| `spring_venue_link` | String | API link to the spring venue. |
| `league_id` | Float64 | League MLBAM ID. |
| `league_name` | String | League name. |
| `league_link` | String | API link to the league. |
| `division_id` | Float64 | Division MLBAM ID. |
| `division_name` | String | Division name. |
| `division_link` | String | API link to the division. |
| `sport_id` | Int64 | Sport MLBAM ID. |
| `sport_link` | String | API link to the sport. |
| `sport_name` | String | Sport name (e.g., Major League Baseball). |
| `parent_org_name` | String | Parent organization name. |
| `parent_org_id` | Float64 | Parent organization MLBAM ID. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_team_affiliates-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_team_affiliates-example}

```python
mlb_team_affiliates()
```

## mlb_teams_history

View historical records for a list of teams.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/history`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/history?teamIds=147](https://statsapi.mlb.com/api/v1/teams/history?teamIds=147)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamIds` | `team_ids` |  |  | `Y` | teamIds query parameter. |
| `startSeason` | `start_season` |  |  | `Y` | startSeason query parameter. |
| `endSeason` | `end_season` |  |  | `Y` | endSeason query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_teams_history-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `all_star_status` | String | All-star status flag. |
| `id` | Int64 | Id. |
| `name` | String | Display name. |
| `link` | String | API link to the game feed. |
| `season` | Int64 | Season year. |
| `team_code` | String | Internal team code. |
| `file_code` | String | File code abbreviation. |
| `abbreviation` | String | Short abbreviation. |
| `team_name` | String | Team name. |
| `location_name` | String | Team location (city). |
| `first_year_of_play` | String | First year the franchise played. |
| `short_name` | String | Short display name. |
| `franchise_name` | String | Franchise name. |
| `club_name` | String | Club name. |
| `active` | Boolean | Whether the player is currently active. |
| `venue_id` | Int64 | MLBAM venue ID. |
| `venue_name` | String | Venue name. |
| `venue_link` | String | API link to the venue. |
| `spring_venue_id` | Float64 | Spring training venue MLBAM ID. |
| `spring_venue_link` | String | API link to the spring venue. |
| `league_id` | Int64 | League MLBAM ID. |
| `league_name` | String | League name. |
| `league_link` | String | API link to the league. |
| `sport_id` | Int64 | Sport MLBAM ID. |
| `sport_link` | String | API link to the sport. |
| `sport_name` | String | Sport name (e.g., Major League Baseball). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_teams_history-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_teams_history-example}

```python
mlb_teams_history(team_ids='147')
```

## mlb_teams_stats

View team stats.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/stats`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/stats?season=2023&sportIds=1&group=hitting&stats=season](https://statsapi.mlb.com/api/v1/teams/stats?season=2023&sportIds=1&group=hitting&stats=season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `sportIds` | `sport_ids` |  |  | `Y` | sportIds query parameter. |
| `group` | `stat_group` |  |  | `Y` | group query parameter. |
| `gameType` | `game_type` |  |  | `Y` | gameType query parameter. |
| `stats` | `stats` |  |  | `Y` | stats query parameter. |
| `order` | `order` |  |  | `Y` | order query parameter. |
| `sortStat` | `sort_stat` |  |  | `Y` | sortStat query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_teams_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `total_splits` | Int64 | Total number of splits in the leaderboard. |
| `exemptions` | String | A serialized list of any statistical exemption notes or flags associated with the team's stat splits (e.g., players exempt from qualifying thresholds). |
| `splits` | String | Splits. |
| `splits_tied_with_offset` | String | Players tied at the offset boundary. |
| `splits_tied_with_limit` | String | Players tied at the limit boundary. |
| `type_display_name` | String | Stat type display name. |
| `group_display_name` | String | Stat group display name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_teams_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_teams_stats-example}

```python
mlb_teams_stats(season='2023', sport_ids='1', stat_group='hitting', stats='season')
```

## mlb_teams_stats_leaders

View leaders for a statistic.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/stats/leaders`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/stats/leaders?leaderCategories=homeRuns&season=2023](https://statsapi.mlb.com/api/v1/teams/stats/leaders?leaderCategories=homeRuns&season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leaderCategories` | `leader_categories` |  |  | `Y` | leaderCategories query parameter. |
| `sitCodes` | `sit_codes` |  |  | `Y` | sitCodes query parameter. |
| `gameTypes` | `game_types` |  |  | `Y` | gameTypes query parameter. |
| `statGroup` | `stat_group` |  |  | `Y` | statGroup query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `leagueIds` | `league_ids` |  |  | `Y` | leagueIds query parameter. |
| `startDate` | `start_date` |  |  | `Y` | startDate query parameter. |
| `endDate` | `end_date` |  |  | `Y` | endDate query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_teams_stats_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `leader_category` | String | Team leader category (e.g., homeRuns). |
| `season` | String | Season year. |
| `leaders` | String | Serialized representation of the statistical leaders entries for the team stat category returned by the MLB Stats API. |
| `stat_group` | String | Stat group (e.g., hitting). |
| `total_splits` | Int64 | Total number of splits in the leaderboard. |
| `game_type_id` | String | Game type code (e.g., R for regular season). |
| `game_type_description` | String | Game type description. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_teams_stats_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_teams_stats_leaders-example}

```python
mlb_teams_stats_leaders(leader_categories='homeRuns', season='2023')
```

## mlb_team_coaches

View biographical  information on all coaches for a given club.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/{team_id}/coaches`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/147/coaches?season=2023](https://statsapi.mlb.com/api/v1/teams/147/coaches?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_team_coaches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `jersey_number` | String | Jersey number worn (often blank for non-uniformed roles). |
| `job` | String | Job title (e.g. 'Umpire'). |
| `job_id` | String | Job code identifier. |
| `title` | String | Specific role title for the assignment. |
| `person_id` | Int64 | MLB player ID. |
| `person_full_name` | String | Player full name. |
| `person_link` | String | API relative link to the person. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_team_coaches-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_team_coaches-example}

```python
mlb_team_coaches(team_id=147, season='2023')
```

## mlb_team_personnel

View biographical  information on all personnel for a given club.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/{team_id}/personnel`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/147/personnel](https://statsapi.mlb.com/api/v1/teams/147/personnel)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_team_personnel-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `jersey_number` | String | Jersey number worn (often blank for non-uniformed roles). |
| `job` | String | Job title (e.g. 'Umpire'). |
| `job_id` | String | Job code identifier. |
| `title` | String | Specific role title for the assignment. |
| `person_id` | Int64 | MLB player ID. |
| `person_full_name` | String | Player full name. |
| `person_link` | String | API relative link to the person. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_team_personnel-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_team_personnel-example}

```python
mlb_team_personnel(team_id=147)
```

## mlb_team_roster_type

View biographical and statistical information for a club's roster based on roster type.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/{team_id}/roster/{roster_type}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/147/roster/active?season=2023](https://statsapi.mlb.com/api/v1/teams/147/roster/active?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `roster_type` | `roster_type` |  | `Y` |  | roster_type path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_team_roster_type-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `jersey_number` | String | Jersey number worn (often blank for non-uniformed roles). |
| `person_id` | Int64 | MLB player ID. |
| `person_full_name` | String | Player full name. |
| `person_link` | String | API relative link to the person. |
| `position_code` | String | Numeric scorekeeping position code. |
| `position_name` | String | Position name. |
| `position_type` | String | Position category (e.g. 'Pitcher', 'Infielder'). |
| `position_abbreviation` | String | Position abbreviation. |
| `status_code` | String | Status code identifier (e.g. 'S', 'P', 'I', 'F'). |
| `status_description` | String | Roster status description (e.g. 'Active'). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_team_roster_type-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_team_roster_type-example}

```python
mlb_team_roster_type(team_id=147, roster_type='active', season='2023')
```
