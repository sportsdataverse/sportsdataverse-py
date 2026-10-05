---
title: "MLB — MLB Stats API — Team"
sidebar_label: "Team"
sidebar_position: 7
description: "MLB — MLB Stats API — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# MLB — MLB Stats API — Team

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
| `all_star_status` | character | All-star status flag. |
| `id` | integer | Id. |
| `name` | character | Display name. |
| `link` | character | API link to the game feed. |
| `season` | integer | Season year. |
| `team_code` | character | Internal team code. |
| `file_code` | character | File code abbreviation. |
| `abbreviation` | character | Short abbreviation. |
| `team_name` | character | Team name. |
| `location_name` | character | Team location (city). |
| `first_year_of_play` | character | First year the franchise played. |
| `short_name` | character | Short display name. |
| `franchise_name` | character | Franchise name. |
| `club_name` | character | Club name. |
| `active` | logical | Whether the player is currently active. |
| `spring_league_id` | integer | Spring league MLBAM ID. |
| `spring_league_name` | character | Spring league name. |
| `spring_league_link` | character | API link to the spring league. |
| `spring_league_abbreviation` | character | Spring league abbreviation. |
| `venue_id` | integer | MLBAM venue ID. |
| `venue_name` | character | Venue name. |
| `venue_link` | character | API link to the venue. |
| `spring_venue_id` | integer | Spring training venue MLBAM ID. |
| `spring_venue_link` | character | API link to the spring venue. |
| `league_id` | integer | League MLBAM ID. |
| `league_name` | character | League name. |
| `league_link` | character | API link to the league. |
| `division_id` | integer | Division MLBAM ID. |
| `division_name` | character | Division name. |
| `division_link` | character | API link to the division. |
| `sport_id` | integer | Sport MLBAM ID. |
| `sport_link` | character | API link to the sport. |
| `sport_name` | character | Sport name (e.g., Major League Baseball). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_team-example}

```python
mlb_team(team_id=10)
```

_Last validated n/a._

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
| `jersey_number` | character | Jersey number worn (often blank for non-uniformed roles). |
| `person_id` | integer | MLB player ID. |
| `person_full_name` | character | Player full name. |
| `person_link` | character | API relative link to the person. |
| `position_code` | character | Numeric scorekeeping position code. |
| `position_name` | character | Full position name (e.g. 'Point Guard', 'Goalkeeper'). |
| `position_type` | character | Position category (e.g. 'Pitcher', 'Infielder'). |
| `position_abbreviation` | character | Position abbreviation. |
| `status_code` | character | Status code identifier (e.g. 'S', 'P', 'I', 'F'). |
| `status_description` | character | Roster status description (e.g. 'Active'). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_team_roster-example}

```python
mlb_team_roster(team_id=10)
```

_Last validated n/a._

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
| `id` | integer | Id. |
| `full_name` | character | Player's full name. |
| `link` | character | API link to the game feed. |
| `first_name` | character | Player first name. |
| `last_name` | character | Player last name. |
| `primary_number` | character | Player uniform number. |
| `birth_date` | character | Date of birth (YYYY-MM-DD). |
| `current_age` | integer | Current age in years. |
| `birth_city` | character | City of birth. |
| `birth_country` | character | Country of birth. |
| `height` | character | Height (feet and inches). |
| `weight` | integer | Weight in pounds. |
| `active` | logical | Whether the player is currently active. |
| `use_name` | character | Preferred first name. |
| `use_last_name` | character | Preferred last name. |
| `middle_name` | character | Player middle name. |
| `boxscore_name` | character | Name as shown in box scores. |
| `nick_name` | character | Player nickname. |
| `gender` | character | Player gender. |
| `is_player` | logical | Whether the person is a player. |
| `is_verified` | logical | Whether the player profile is verified. |
| `pronunciation` | character | Phonetic name pronunciation. |
| `mlb_debut_date` | character | MLB debut date (YYYY-MM-DD). |
| `name_first_last` | character | Name in first-last order. |
| `name_slug` | character | URL-friendly name slug. |
| `first_last_name` | character | First and last name. |
| `last_first_name` | character | Name in last, first order. |
| `last_init_name` | character | Last name with first initial. |
| `init_last_name` | character | First initial with last name. |
| `full_fml_name` | character | Full name (first-middle-last). |
| `full_lfm_name` | character | Full name (last-first-middle). |
| `strike_zone_top` | double | Top of the player's strike zone (feet). |
| `strike_zone_bottom` | double | Bottom of the player's strike zone (feet). |
| `alumni_last_season` | character | Last season the player was with the team. |
| `primary_position_code` | character | Primary position code. |
| `primary_position_name` | character | Primary fielding position name. |
| `primary_position_type` | character | Primary position type (e.g. Infielder). |
| `primary_position_abbreviation` | character | Primary position abbreviation. |
| `bat_side_code` | character | Batting side code (L/R/S). |
| `bat_side_description` | character | Batting side description. |
| `pitch_hand_code` | character | Throwing hand code (L/R). |
| `pitch_hand_description` | character | Throwing hand description. |
| `birth_state_province` | character | State or province of birth. |
| `draft_year` | double | Year the player was drafted. |
| `last_played_date` | character | Date of last MLB game played. |
| `name_matrilineal` | character | Maternal family name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_team_alumni-example}

```python
mlb_team_alumni(team_id=10)
```

_Last validated n/a._

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
| `all_star_status` | character | All-star status flag. |
| `id` | integer | Id. |
| `name` | character | Display name. |
| `link` | character | API link to the game feed. |
| `season` | integer | Season year. |
| `team_code` | character | Internal team code. |
| `file_code` | character | File code abbreviation. |
| `abbreviation` | character | Short abbreviation. |
| `team_name` | character | Team name. |
| `location_name` | character | Team location (city). |
| `first_year_of_play` | character | First year the franchise played. |
| `short_name` | character | Short display name. |
| `franchise_name` | character | Franchise name. |
| `club_name` | character | Club name. |
| `active` | logical | Whether the player is currently active. |
| `spring_league_id` | double | Spring league MLBAM ID. |
| `spring_league_name` | character | Spring league name. |
| `spring_league_link` | character | API link to the spring league. |
| `spring_league_abbreviation` | character | Spring league abbreviation. |
| `venue_id` | integer | MLBAM venue ID. |
| `venue_name` | character | Venue name. |
| `venue_link` | character | API link to the venue. |
| `spring_venue_id` | double | Spring training venue MLBAM ID. |
| `spring_venue_link` | character | API link to the spring venue. |
| `league_id` | double | League MLBAM ID. |
| `league_name` | character | League name. |
| `league_link` | character | API link to the league. |
| `division_id` | double | Division MLBAM ID. |
| `division_name` | character | Division name. |
| `division_link` | character | API link to the division. |
| `sport_id` | integer | Sport MLBAM ID. |
| `sport_link` | character | API link to the sport. |
| `sport_name` | character | Sport name (e.g., Major League Baseball). |
| `parent_org_name` | character | Parent organization name. |
| `parent_org_id` | double | Parent organization MLBAM ID. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_team_affiliates-example}

```python
mlb_team_affiliates()
```

_Last validated n/a._

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
| `all_star_status` | character | All-star status flag. |
| `id` | integer | Id. |
| `name` | character | Display name. |
| `link` | character | API link to the game feed. |
| `season` | integer | Season year. |
| `team_code` | character | Internal team code. |
| `file_code` | character | File code abbreviation. |
| `abbreviation` | character | Short abbreviation. |
| `team_name` | character | Team name. |
| `location_name` | character | Team location (city). |
| `first_year_of_play` | character | First year the franchise played. |
| `short_name` | character | Short display name. |
| `franchise_name` | character | Franchise name. |
| `club_name` | character | Club name. |
| `active` | logical | Whether the player is currently active. |
| `venue_id` | integer | MLBAM venue ID. |
| `venue_name` | character | Venue name. |
| `venue_link` | character | API link to the venue. |
| `spring_venue_id` | double | Spring training venue MLBAM ID. |
| `spring_venue_link` | character | API link to the spring venue. |
| `league_id` | integer | League MLBAM ID. |
| `league_name` | character | League name. |
| `league_link` | character | API link to the league. |
| `sport_id` | integer | Sport MLBAM ID. |
| `sport_link` | character | API link to the sport. |
| `sport_name` | character | Sport name (e.g., Major League Baseball). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_teams_history-example}

```python
mlb_teams_history(team_ids='147')
```

_Last validated n/a._

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
| `total_splits` | integer | Total number of splits in the leaderboard. |
| `exemptions` | character | A serialized list of any statistical exemption notes or flags associated with the team's stat splits (e.g., players exempt from qualifying thresholds). |
| `splits` | character | Splits. |
| `splits_tied_with_offset` | character | Players tied at the offset boundary. |
| `splits_tied_with_limit` | character | Players tied at the limit boundary. |
| `type_display_name` | character | Stat type display name. |
| `group_display_name` | character | Stat group display name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_teams_stats-example}

```python
mlb_teams_stats(season='2023', sport_ids='1', stat_group='hitting', stats='season')
```

_Last validated n/a._

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
| `leader_category` | character | Team leader category (e.g., homeRuns). |
| `season` | character | Season year. |
| `leaders` | character | Serialized representation of the statistical leaders entries for the team stat category returned by the MLB Stats API. |
| `stat_group` | character | Stat group (e.g., hitting). |
| `total_splits` | integer | Total number of splits in the leaderboard. |
| `game_type_id` | character | Game type code (e.g., R for regular season). |
| `game_type_description` | character | Game type description. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_teams_stats_leaders-example}

```python
mlb_teams_stats_leaders(leader_categories='homeRuns', season='2023')
```

_Last validated n/a._

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
| `jersey_number` | character | Jersey number worn (often blank for non-uniformed roles). |
| `job` | character | Job title (e.g. 'Umpire'). |
| `job_id` | character | Job code identifier. |
| `title` | character | Specific role title for the assignment. |
| `person_id` | integer | MLB player ID. |
| `person_full_name` | character | Player full name. |
| `person_link` | character | API relative link to the person. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_team_coaches-example}

```python
mlb_team_coaches(team_id=147, season='2023')
```

_Last validated n/a._

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
| `jersey_number` | character | Jersey number worn (often blank for non-uniformed roles). |
| `job` | character | Job title (e.g. 'Umpire'). |
| `job_id` | character | Job code identifier. |
| `title` | character | Specific role title for the assignment. |
| `person_id` | integer | MLB player ID. |
| `person_full_name` | character | Player full name. |
| `person_link` | character | API relative link to the person. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_team_personnel-example}

```python
mlb_team_personnel(team_id=147)
```

_Last validated n/a._

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
| `jersey_number` | character | Jersey number worn (often blank for non-uniformed roles). |
| `person_id` | integer | MLB player ID. |
| `person_full_name` | character | Player full name. |
| `person_link` | character | API relative link to the person. |
| `position_code` | character | Numeric scorekeeping position code. |
| `position_name` | character | Position name. |
| `position_type` | character | Position category (e.g. 'Pitcher', 'Infielder'). |
| `position_abbreviation` | character | Position abbreviation. |
| `status_code` | character | Status code identifier (e.g. 'S', 'P', 'I', 'F'). |
| `status_description` | character | Roster status description (e.g. 'Active'). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_team_roster_type-example}

```python
mlb_team_roster_type(team_id=147, roster_type='active', season='2023')
```

_Last validated n/a._
