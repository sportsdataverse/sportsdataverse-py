---
title: MLS — MLS official web API (mlssoccer.com)
sidebar_label: MLS official web API (mlssoccer.com)
description: "MLS — MLS official web API (mlssoccer.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# MLS — MLS official web API (mlssoccer.com)

`sportsdataverse.soccer.mls` — 12 endpoints.

## mls_club

Single club detail (stats-api).

**Endpoint URL:** `GET https://stats-api.mlssoccer.com/clubs/{club_id}`

**Valid URL:** [https://stats-api.mlssoccer.com/clubs/MLS-CLU-000001](https://stats-api.mlssoccer.com/clubs/MLS-CLU-000001)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `club_id` | `club_id` |  | `Y` |  | club_id path parameter. |

### Returns {#mls_club-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `club_id` | String | Sportec club id |
| `club_name` | String | Full club name |
| `three_letter_code` | String | 3-letter code |
| `short_name` | String | Short name |
| `club_short_name` | String | Alternate short name |
| `club_three_letter_code` | String | Alternate 3-letter code |
| `city` | String | Home city |
| `country` | String | Country |
| `long_name` | String | Long/legal club name |
| `founded` | String | Year founded |
| `stadium_id` | String | Stadium id |
| `stadium_name` | String | Stadium name |
| `club_color_one_club_color` | String | Primary club colour: club colour name. |
| `club_color_one_club_color_rgb` | String | Primary club colour: club colour as an RGB hex string. |
| `club_color_two_club_color` | String | Secondary club colour: club colour name. |
| `club_color_two_club_color_rgb` | String | Secondary club colour: club colour as an RGB hex string. |
| `club_color_three_club_color` | String | Tertiary club colour: club colour name. |
| `club_color_three_club_color_rgb` | String | Tertiary club colour: club colour as an RGB hex string. |
| `shirt_one_shirt_main_color` | String | First-choice kit: main shirt colour name. |
| `shirt_one_shirt_main_color_rgb` | String | First-choice kit: main shirt colour as an RGB hex string. |
| `shirt_one_shirt_secondary_color` | String | First-choice kit: secondary shirt colour name. |
| `shirt_one_shirt_secondary_color_rgb` | String | First-choice kit: secondary shirt colour as an RGB hex string. |
| `shirt_one_shirt_number_color` | String | First-choice kit: shirt-number colour name. |
| `shirt_one_shirt_number_color_rgb` | String | First-choice kit: shirt-number colour as an RGB hex string. |
| `shirt_two_shirt_main_color` | String | Second-choice kit: main shirt colour name. |
| `shirt_two_shirt_main_color_rgb` | String | Second-choice kit: main shirt colour as an RGB hex string. |
| `shirt_two_shirt_secondary_color` | String | Second-choice kit: secondary shirt colour name. |
| `shirt_two_shirt_secondary_color_rgb` | String | Second-choice kit: secondary shirt colour as an RGB hex string. |
| `shirt_two_shirt_number_color` | String | Second-choice kit: shirt-number colour name. |
| `shirt_two_shirt_number_color_rgb` | String | Second-choice kit: shirt-number colour as an RGB hex string. |
| `shirt_three_shirt_main_color` | String | Third-choice kit: main shirt colour name. |
| `shirt_three_shirt_main_color_rgb` | String | Third-choice kit: main shirt colour as an RGB hex string. |
| `shirt_three_shirt_secondary_color` | String | Third-choice kit: secondary shirt colour name. |
| `shirt_three_shirt_secondary_color_rgb` | String | Third-choice kit: secondary shirt colour as an RGB hex string. |
| `shirt_three_shirt_number_color` | String | Third-choice kit: shirt-number colour name. |
| `shirt_three_shirt_number_color_rgb` | String | Third-choice kit: shirt-number colour as an RGB hex string. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mls_club-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. The MLS host returned 404 (unknown id, or an unplayed match on `/matches/{matchId}`). On the season-addressable stats-api endpoints (`season_matches`, `standings`, `competition_seasons`) a season before 2024 has no addressable id -- the stats-api serves 2024+ only. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mls_club-example}

```python
mls_club(club_id='MLS-CLU-000001')
```

## mls_competition_seasons

List seasons for a competition (stats-api).

**Endpoint URL:** `GET https://stats-api.mlssoccer.com/competitions/{competition_id}/seasons`

**Valid URL:** [https://stats-api.mlssoccer.com/competitions/MLS-COM-000001/seasons](https://stats-api.mlssoccer.com/competitions/MLS-COM-000001/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_id` | `competition_id` |  | `Y` |  | competition_id path parameter. |

### Returns {#mls_competition_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_id` | String | Sportec season id |
| `season` | Int64 | Season year |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mls_competition_seasons-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. The MLS host returned 404 (unknown id, or an unplayed match on `/matches/{matchId}`). On the season-addressable stats-api endpoints (`season_matches`, `standings`, `competition_seasons`) a season before 2024 has no addressable id -- the stats-api serves 2024+ only. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mls_competition_seasons-example}

```python
mls_competition_seasons(competition_id='MLS-COM-000001')
```

## mls_competitions

List competitions (stats-api).

**Endpoint URL:** `GET https://stats-api.mlssoccer.com/competitions`

**Valid URL:** [https://stats-api.mlssoccer.com/competitions](https://stats-api.mlssoccer.com/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#mls_competitions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | String | Sportec competition id |
| `competition_name` | String | Competition display name |
| `competition_name_french` | String | French competition name |
| `country` | String | Country |
| `competition_type` | String | `League` or `Tournament` |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mls_competitions-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. The MLS host returned 404 (unknown id, or an unplayed match on `/matches/{matchId}`). On the season-addressable stats-api endpoints (`season_matches`, `standings`, `competition_seasons`) a season before 2024 has no addressable id -- the stats-api serves 2024+ only. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mls_competitions-example}

```python
mls_competitions()
```

## mls_content_season

Season content entity by slug (dapi / Contentful).

**Endpoint URL:** `GET https://dapi.mlssoccer.com/v2/content/en-us/seasons/{slug}`

**Valid URL:** [https://dapi.mlssoccer.com/v2/content/en-us/seasons/mls-regular-season-2026](https://dapi.mlssoccer.com/v2/content/en-us/seasons/mls-regular-season-2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `slug` | `slug` |  | `Y` |  | slug path parameter. |

### Returns {#mls_content_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `type` | String | Type discriminator for the record. |
| `_translation_id` | String | Contentful translation-group id shared by every locale of this entry. |
| `_entity_id` | String | Contentful entity id for the content entry. |
| `self_url` | String | Canonical API URL of this content entry. |
| `slug` | String | URL slug. |
| `title` | String | Display title. |
| `tags` | String | Content tags, JSON-encoded. |
| `relations` | String | Related content entries, JSON-encoded. |
| `created_by` | String | User or service that created the content entry. |
| `last_updated_by` | String | User or service that last updated the content entry. |
| `last_updated_date` | String | Timestamp of the last content update (ISO 8601). |
| `content_date` | String | Editorial content date (ISO 8601). |
| `featured` | Int64 | Whether the entry is flagged as featured (1 = featured). |
| `entity_code` | String | Contentful entity-type code. |
| `_list_availability` | Int64 | Contentful flag controlling whether the entry appears in listings. |
| `references_list_of_clubs_clinched_x` | String | Contentful reference list: reference list of clubs that have clinched, list variant x. |
| `references_list_of_clubs_clinched_e` | String | Contentful reference list: reference list of clubs that have clinched, list variant e. |
| `references_list_of_clubs_clinched_s` | String | Contentful reference list: reference list of clubs that have clinched, list variant s. |
| `references_list_of_clubs_clinched_y` | String | Contentful reference list: reference list of clubs that have clinched, list variant y. |
| `fields_opta_id` | String | Season content field: parallel Opta integer id for the entity. |
| `fields_sportec_id` | String | Season content field: Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `fields_name` | String | Season content field: display name. |
| `fields_competition_opta_id` | String | Season content field: parallel Opta integer id of the competition. |
| `fields_competition_sportec_id` | String | Season content field: Sportec opaque id of the competition (Utf8 join key). |
| `fields_standings_legend` | String | Season content field: prose legend explaining the standings qualification bands. |
| `fields_top_clubs` | Int64 | Season content field: number of clubs qualifying from the top of the table. |
| `fields_top_clubs_legend` | String | Season content field: prose legend explaining the top-clubs cut line. |
| `fields_home_advantage` | Int64 | Season content field: points of home advantage applied by the playoff seeding rules. |
| `fields_home_advantage_legend` | String | Season content field: prose legend explaining the home-advantage rule. |
| `fields_playoff_qualified_east_conference` | Int64 | Season content field: count of Eastern Conference clubs that have clinched a playoff berth. |
| `fields_playoff_qualified_west_conference` | Int64 | Season content field: count of Western Conference clubs that have clinched a playoff berth. |
| `fields_competition_sportec_id_overwrite` | Boolean | Season content field: competition: whether the season's Sportec id is manually overridden in the CMS. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mls_content_season-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. The MLS host returned 404 (unknown id, or an unplayed match on `/matches/{matchId}`). On the season-addressable stats-api endpoints (`season_matches`, `standings`, `competition_seasons`) a season before 2024 has no addressable id -- the stats-api serves 2024+ only. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mls_content_season-example}

```python
mls_content_season(slug='mls-regular-season-2026')
```

## mls_content_seasons

Query season content entities (dapi / Contentful).

**Endpoint URL:** `GET https://dapi.mlssoccer.com/v2/content/en-us/seasons`

**Valid URL:** [https://dapi.mlssoccer.com/v2/content/en-us/seasons](https://dapi.mlssoccer.com/v2/content/en-us/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `fields.competitionSportecId` | `competition_sportec_id` |  |  | `Y` | Filter by competition Sportec id (indexed field) |
| `fields.sportecId` | `sportec_id` |  |  | `Y` | Filter by season Sportec id (indexed field) |

### Returns {#mls_content_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `type` | String | Type discriminator for the record. |
| `_translation_id` | String | Contentful translation-group id shared by every locale of this entry. |
| `_entity_id` | String | Contentful entity id for the content entry. |
| `self_url` | String | Canonical API URL of this content entry. |
| `slug` | String | URL slug. |
| `title` | String | Display title. |
| `tags` | String | Content tags, JSON-encoded. |
| `created_by` | String | User or service that created the content entry. |
| `last_updated_by` | String | User or service that last updated the content entry. |
| `last_updated_date` | String | Timestamp of the last content update (ISO 8601). |
| `content_date` | String | Editorial content date (ISO 8601). |
| `featured` | Int64 | Whether the entry is flagged as featured (1 = featured). |
| `entity_code` | String | Contentful entity-type code. |
| `fields_opta_id` | String | Season content field: parallel Opta integer id for the entity. |
| `fields_sportec_id` | String | Season content field: Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `fields_name` | String | Season content field: display name. |
| `fields_competition_opta_id` | String | Season content field: parallel Opta integer id of the competition. |
| `fields_competition_sportec_id` | String | Season content field: Sportec opaque id of the competition (Utf8 join key). |
| `fields_standings_legend` | String | Season content field: prose legend explaining the standings qualification bands. |
| `fields_top_clubs` | Int64 | Season content field: number of clubs qualifying from the top of the table. |
| `fields_top_clubs_legend` | String | Season content field: prose legend explaining the top-clubs cut line. |
| `fields_home_advantage` | Int64 | Season content field: points of home advantage applied by the playoff seeding rules. |
| `fields_home_advantage_legend` | String | Season content field: prose legend explaining the home-advantage rule. |
| `fields_playoff_qualified_east_conference` | Int64 | Season content field: count of Eastern Conference clubs that have clinched a playoff berth. |
| `fields_playoff_qualified_west_conference` | Int64 | Season content field: count of Western Conference clubs that have clinched a playoff berth. |
| `fields_competition_sportec_id_overwrite` | Boolean | Season content field: competition: whether the season's Sportec id is manually overridden in the CMS. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mls_content_seasons-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. The MLS host returned 404 (unknown id, or an unplayed match on `/matches/{matchId}`). On the season-addressable stats-api endpoints (`season_matches`, `standings`, `competition_seasons`) a season before 2024 has no addressable id -- the stats-api serves 2024+ only. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mls_content_seasons-example}

```python
mls_content_seasons()
```

## mls_match

Single match detail incl. lineups + referees (stats-api).

**Endpoint URL:** `GET https://stats-api.mlssoccer.com/matches/{match_id}`

**Valid URL:** [https://stats-api.mlssoccer.com/matches/MLS-MAT-0009H8](https://stats-api.mlssoccer.com/matches/MLS-MAT-0009H8)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | match_id path parameter. |

### Returns {#mls_match-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | String | Sportec competition id |
| `competition_name` | String | Competition display name |
| `away_team_goals` | Int64 | Away goals |
| `home_team_goals` | Int64 | Home goals |
| `kickoff_time` | String | Actual kickoff timestamp (ISO 8601) |
| `match_day` | Int64 | Matchday / round number |
| `match_id` | String | Sportec match id (MLS-MAT-*) |
| `match_title` | String | Home:Away title, e.g. `CF Montréal:Toronto FC` |
| `planned_kickoff_time` | String | Scheduled kickoff timestamp (ISO 8601) |
| `result` | String | Final score string, e.g. `0:0` |
| `season` | Int64 | Season year |
| `season_id` | String | Sportec season id |
| `competition_type` | String | `League` or `Tournament` |
| `section_name` | String | Season section / phase |
| `competition_label` | String | Short competition label |
| `series_type` | String | Series type (e.g. regular) |
| `total_time_first_half` | Int64 | First-half elapsed seconds |
| `total_time_second_half` | Int64 | Second-half elapsed seconds |
| `playing_time_first_half` | Int64 | First-half playing seconds |
| `playing_time_second_half` | Int64 | Second-half playing seconds |
| `total_time_first_half_extra` | Int64 | First-half stoppage seconds |
| `total_time_second_half_extra` | Int64 | Second-half stoppage seconds |
| `total_time_penalty` | Int64 | Penalty-shootout total seconds |
| `playing_time_penalty` | Int64 | Penalty-shootout playing seconds |
| `other_information` | String | Free-form notes |
| `match_type` | String | Match classification |
| `match_scheduled` | Boolean | Whether kickoff is scheduled |
| `date_quality` | String | Confidence of the match date |
| `end_date` | String | Match end date |
| `sub_league` | String | Sub-league grouping |
| `group` | String | Group name (tournaments) |
| `competition_name_french` | String | French competition name |
| `match_status` | String | Status (e.g. `Live`, `FullTime`) |
| `minute_of_play` | String | Current minute (live) |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mls_match-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. The MLS host returned 404 (unknown id, or an unplayed match on `/matches/{matchId}`). On the season-addressable stats-api endpoints (`season_matches`, `standings`, `competition_seasons`) a season before 2024 has no addressable id -- the stats-api serves 2024+ only. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mls_match-example}

```python
mls_match(match_id='MLS-MAT-0009H8')
```

## mls_season_matches

List matches in a season, date-windowed (stats-api).

**Endpoint URL:** `GET https://stats-api.mlssoccer.com/matches/seasons/{season_id}`

**Valid URL:** [https://stats-api.mlssoccer.com/matches/seasons/MLS-SEA-0001KA](https://stats-api.mlssoccer.com/matches/seasons/MLS-SEA-0001KA)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | season_id path parameter. |
| `match_date[gte]` | `match_date_gte` |  |  | `Y` | Window start date (YYYY-MM-DD) |
| `match_date[lte]` | `match_date_lte` |  |  | `Y` | Window end date (YYYY-MM-DD) |
| `competition_id` | `competition_id` |  |  | `Y` | Filter by competition id |
| `per_page` | `per_page` |  |  | `Y` | Page size |
| `sort` | `sort` |  |  | `Y` | Sort spec, e.g. planned_kickoff_time:asc,home_team_name:asc |
| `series_name` | `series_name` |  |  | `Y` | Filter by series/round name |

### Returns {#mls_season_matches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | String | Sportec competition id |
| `competition_name` | String | Competition display name |
| `competition_type` | String | `League` or `Tournament` |
| `end_date` | String | Match end date |
| `away_team_id` | String | Away Sportec club id |
| `away_team_name` | String | Away club name |
| `home_team_id` | String | Home Sportec club id |
| `home_team_name` | String | Home club name |
| `home_team_short_name` | String | Home short name |
| `home_team_three_letter_code` | String | Home 3-letter code |
| `away_team_short_name` | String | Away short name |
| `away_team_three_letter_code` | String | Away 3-letter code |
| `match_scheduled` | Boolean | Whether kickoff is scheduled |
| `match_day` | Int64 | Matchday / round number |
| `match_day_id` | String | Sportec matchday id |
| `match_id` | String | Sportec match id (MLS-MAT-*) |
| `match_type` | String | Match classification |
| `planned_kickoff_time` | String | Scheduled kickoff timestamp (ISO 8601) |
| `season` | Int64 | Season year |
| `season_id` | String | Sportec season id |
| `stadium_id` | String | Stadium id |
| `stadium_name` | String | Stadium name |
| `neutral_venue` | Boolean | Neutral-venue flag |
| `start_date` | String | Match start date |
| `sub_league` | String | Sub-league grouping |
| `group` | String | Group name (tournaments) |
| `date_quality` | String | Confidence of the match date |
| `official_information` | String | Officiating notes |
| `match_date_time_status` | String | Kickoff time status |
| `section_name` | String | Season section / phase |
| `competition_label` | String | Short competition label |
| `series_type` | String | Series type (e.g. regular) |
| `competition_name_french` | String | French competition name |
| `result` | String | Final score string, e.g. `0:0` |
| `away_team_goals` | Int64 | Away goals |
| `home_team_goals` | Int64 | Home goals |
| `match_status` | String | Status (e.g. `Live`, `FullTime`) |
| `minute_of_play` | String | Current minute (live) |
| `stadium_city` | String | Stadium city |
| `stadium_country` | String | Stadium country |
| `bracket_structure_id` | String | Identifier of the playoff bracket structure. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mls_season_matches-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. The MLS host returned 404 (unknown id, or an unplayed match on `/matches/{matchId}`). On the season-addressable stats-api endpoints (`season_matches`, `standings`, `competition_seasons`) a season before 2024 has no addressable id -- the stats-api serves 2024+ only. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mls_season_matches-example}

```python
mls_season_matches(season_id='MLS-SEA-0001KA')
```

## mls_sportapi_club_players

Club roster with player Sportec ids (sportapi).

**Endpoint URL:** `GET https://sportapi.mlssoccer.com/api/players/byClub/{club_id}`

**Valid URL:** [https://sportapi.mlssoccer.com/api/players/byClub/MLS-CLU-000001](https://sportapi.mlssoccer.com/api/players/byClub/MLS-CLU-000001)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `club_id` | `club_id` |  | `Y` |  | club_id path parameter. |
| `culture` | `culture` |  |  | `Y` | Locale, e.g. en-us |

### Returns {#mls_sportapi_club_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `opta_id` | String | Parallel Opta integer id for the entity. |
| `full_name` | String | Full name |
| `first_name` | String | First name |
| `last_name` | String | Last name |
| `known_name` | String | Known / display name |
| `on_loan` | Boolean | On-loan flag |
| `club_opta_id` | String | Club Opta id |
| `club_sportec_id` | String | Club Sportec id |
| `sportec_id` | String | Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `height` | String | Height |
| `weight` | String | Weight |
| `position` | String | Table position/rank |
| `roster_category` | String | Roster category |
| `player_categories` | String | Roster category tags |
| `jersey_number` | String | Jersey number |
| `player_status_list` | String | Status tags (e.g. `Loaned Out`) |
| `date_of_birth` | String | Birth date |
| `player_slug` | String | URL slug |
| `thumbnail_slug` | String | Player thumbnail image: URL slug. |
| `thumbnail_self_url` | String | Player thumbnail image: canonical API URL of this content entry. |
| `thumbnail_title` | String | Player thumbnail image: display title. |
| `thumbnail_template_url` | String | Player thumbnail image: templated image URL with substitutable size tokens. |
| `thumbnail_thumbnail_url` | String | Player thumbnail image: URL of the rendered thumbnail image. |
| `thumbnail_format` | String | Player thumbnail image: image format of the asset. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mls_sportapi_club_players-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. The MLS host returned 404 (unknown id, or an unplayed match on `/matches/{matchId}`). On the season-addressable stats-api endpoints (`season_matches`, `standings`, `competition_seasons`) a season before 2024 has no addressable id -- the stats-api serves 2024+ only. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mls_sportapi_club_players-example}

```python
mls_sportapi_club_players(club_id='MLS-CLU-000001')
```

## mls_sportapi_clubs_by_sportec_ids

Batch club detail by Sportec ids (sportapi).

**Endpoint URL:** `GET https://sportapi.mlssoccer.com/api/clubs/bySportecIds/{ids}`

**Valid URL:** [https://sportapi.mlssoccer.com/api/clubs/bySportecIds/MLS-MAT-0009H8](https://sportapi.mlssoccer.com/api/clubs/bySportecIds/MLS-MAT-0009H8)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ids` | `ids` |  | `Y` |  | ids path parameter. |

### Returns {#mls_sportapi_clubs_by_sportec_ids-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `opta_id` | String | Parallel Opta integer id for the entity. |
| `sportec_id` | String | Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `full_name` | String | Full name |
| `slug` | String | URL slug. |
| `short_name` | String | Short name |
| `abbreviation` | String |  |
| `background_color` | String | Brand background colour (hex). |
| `logo_bw_slug` | String | Asset slug for the black-and-white logo. |
| `logo_color_slug` | String | Asset slug for the full-colour logo. |
| `logo_color_url` | String | URL of the full-colour logo asset. |
| `crest_color_slug` | String | Asset slug for the full-colour club crest. |
| `ecal_widget_id` | String | Identifier of the eCal calendar-subscription widget. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mls_sportapi_clubs_by_sportec_ids-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. The MLS host returned 404 (unknown id, or an unplayed match on `/matches/{matchId}`). On the season-addressable stats-api endpoints (`season_matches`, `standings`, `competition_seasons`) a season before 2024 has no addressable id -- the stats-api serves 2024+ only. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mls_sportapi_clubs_by_sportec_ids-example}

```python
mls_sportapi_clubs_by_sportec_ids(ids='MLS-MAT-0009H8')
```

## mls_sportapi_match

Single match detail (sportapi / .NET).

**Endpoint URL:** `GET https://sportapi.mlssoccer.com/api/matches/{match_id}`

**Valid URL:** [https://sportapi.mlssoccer.com/api/matches/MLS-MAT-0009H8](https://sportapi.mlssoccer.com/api/matches/MLS-MAT-0009H8)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | match_id path parameter. |

### Returns {#mls_sportapi_match-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `opta_id` | String | Parallel Opta integer id for the entity. |
| `sportec_id` | String | Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `slug` | String | URL slug. |
| `league_match_title` | String | Home:Away match title as rendered in league listings. |
| `broadcasters` | String | Broadcast listings for the match, JSON-encoded. |
| `match_date` | String | Scheduled kickoff timestamp (ISO 8601). |
| `tags` | String | Content tags, JSON-encoded. |
| `home_club_broadcasters` | String | Home club: club-specific broadcast listings, JSON-encoded. |
| `away_club_broadcasters` | String | Away club: club-specific broadcast listings, JSON-encoded. |
| `club_broadcasters` | String | Club-specific broadcast listings, JSON-encoded. |
| `is_time_tbd` | Boolean | Whether the kickoff time is still to be confirmed. |
| `mgm_id` | String | MGM sportsbook partner identifier. |
| `apple_stream_url` | String | Apple broadcast: Apple TV stream URL for the match. |
| `apple_subscription_tier` | String | Apple broadcast: Apple subscription tier required to watch. |
| `apple_advertisement_category` | String | Apple broadcast: Apple advertising category for the stream. |
| `round_name` | String | Name of the round or series this match belongs to. |
| `competition_phase` | String | Competition: competition phase of the match (regular season, playoffs, ...). |
| `home_club_rank` | String | Home club: club's standings rank at the time of the request. |
| `away_club_rank` | String | Away club: club's standings rank at the time of the request. |
| `round_number` | Int64 |  |
| `round_group` | String | Group label within the round (tournaments). |
| `match_day` | String | Matchday / round number |
| `calendar_url` | String | Calendar (.ics) subscription URL for the match. |
| `delayed_match` | Boolean | Whether the match is flagged as delayed. |
| `priority_match_date_to` | String | Priority-match window: end of the display window (ISO 8601). |
| `priority_match_sponsor` | String | Priority-match window: sponsor name attached to the priority match. |
| `home_opta_id` | String | Home club: parallel Opta integer id for the entity. |
| `home_sportec_id` | String | Home club: Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `home_full_name` | String | Home club: full name |
| `home_slug` | String | Home club: URL slug. |
| `home_short_name` | String | Home club: short name |
| `home_abbreviation` | String |  |
| `home_background_color` | String | Home club: brand background colour (hex). |
| `home_logo_bw_slug` | String | Home club: asset slug for the black-and-white logo. |
| `home_logo_color_slug` | String | Home club: asset slug for the full-colour logo. |
| `home_logo_color_url` | String | Home club: URL of the full-colour logo asset. |
| `home_crest_color_slug` | String | Home club: asset slug for the full-colour club crest. |
| `home_ecal_widget_id` | String | Home club: identifier of the eCal calendar-subscription widget. |
| `away_opta_id` | String | Away club: parallel Opta integer id for the entity. |
| `away_sportec_id` | String | Away club: Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `away_full_name` | String | Away club: full name |
| `away_slug` | String | Away club: URL slug. |
| `away_short_name` | String | Away club: short name |
| `away_abbreviation` | String |  |
| `away_background_color` | String | Away club: brand background colour (hex). |
| `away_logo_bw_slug` | String | Away club: asset slug for the black-and-white logo. |
| `away_logo_color_slug` | String | Away club: asset slug for the full-colour logo. |
| `away_logo_color_url` | String | Away club: URL of the full-colour logo asset. |
| `away_crest_color_slug` | String | Away club: asset slug for the full-colour club crest. |
| `away_ecal_widget_id` | String | Away club: identifier of the eCal calendar-subscription widget. |
| `venue_venue_sportec_id` | String | Venue: Sportec opaque id of the venue (Utf8 join key). |
| `venue_background_image_slug` | String | Venue: asset slug for the background image. |
| `venue_name` | String | Venue: display name. |
| `venue_city` | String | Venue: home city |
| `season_slug` | String | Season: URL slug. |
| `season_opta_id` | String | Season: parallel Opta integer id for the entity. |
| `season_sportec_id` | String | Season: Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `season_competition_opta_id` | String | Season: parallel Opta integer id of the competition. |
| `season_name` | String | Season: display name. |
| `competition_opta_id` | String | Parallel Opta integer id of the competition. |
| `competition_sportec_id` | String | Sportec opaque id of the competition (Utf8 join key). |
| `competition_name` | String | Competition display name |
| `competition_widget_id` | String | Competition: identifier of the embedded provider widget. |
| `competition_slug` | String | Competition: URL slug. |
| `competition_short_name` | String | Competition: short name |
| `competition_match_type` | String | Competition: Match classification |
| `competition_logo_light_slug` | String | Competition light-theme logo: URL slug. |
| `competition_logo_dark_slug` | String | Competition dark-theme logo: URL slug. |
| `competition_block_header_name` | String | Competition: header label used for this competition on the site. |
| `competition_mgm_id` | String | Competition: MGM sportsbook partner identifier. |
| `competition_nextgen_ecal_match_hub_display` | Boolean | Competition: whether the match hub shows the eCal subscribe control. |
| `competition_player_headshot_thumbnail_field` | String | Competition: content field the site reads player headshots from. |
| `league_promo_image_asset_url` | String | League promo image: URL of the image asset. |
| `first_party_tickets_display_text` | String | MLS-operated ticketing link: button label for the ticketing link. |
| `first_party_tickets_accessible_text` | String | MLS-operated ticketing link: accessible (screen-reader) label for the ticketing link. |
| `first_party_tickets_url` | String | MLS-operated ticketing link: link URL. |
| `first_party_tickets_open_in_new_tab` | Boolean | MLS-operated ticketing link: whether the link opens in a new tab. |
| `first_party_tickets_is_visible` | Boolean | MLS-operated ticketing link: whether the link is shown. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mls_sportapi_match-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. The MLS host returned 404 (unknown id, or an unplayed match on `/matches/{matchId}`). On the season-addressable stats-api endpoints (`season_matches`, `standings`, `competition_seasons`) a season before 2024 has no addressable id -- the stats-api serves 2024+ only. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mls_sportapi_match-example}

```python
mls_sportapi_match(match_id='MLS-MAT-0009H8')
```

## mls_sportapi_matches_by_sportec_ids

Batch match detail by Sportec ids (sportapi).

**Endpoint URL:** `GET https://sportapi.mlssoccer.com/api/matches/bySportecIds/{ids}`

**Valid URL:** [https://sportapi.mlssoccer.com/api/matches/bySportecIds/MLS-MAT-0009H8](https://sportapi.mlssoccer.com/api/matches/bySportecIds/MLS-MAT-0009H8)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ids` | `ids` |  | `Y` |  | ids path parameter. |

### Returns {#mls_sportapi_matches_by_sportec_ids-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `opta_id` | String | Parallel Opta integer id for the entity. |
| `sportec_id` | String | Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `slug` | String | URL slug. |
| `league_match_title` | String | Home:Away match title as rendered in league listings. |
| `broadcasters` | String | Broadcast listings for the match, JSON-encoded. |
| `match_date` | String | Scheduled kickoff timestamp (ISO 8601). |
| `tags` | String | Content tags, JSON-encoded. |
| `home_club_broadcasters` | String | Home club: club-specific broadcast listings, JSON-encoded. |
| `away_club_broadcasters` | String | Away club: club-specific broadcast listings, JSON-encoded. |
| `club_broadcasters` | String | Club-specific broadcast listings, JSON-encoded. |
| `is_time_tbd` | Boolean | Whether the kickoff time is still to be confirmed. |
| `mgm_id` | String | MGM sportsbook partner identifier. |
| `apple_stream_url` | String | Apple broadcast: Apple TV stream URL for the match. |
| `apple_subscription_tier` | String | Apple broadcast: Apple subscription tier required to watch. |
| `apple_advertisement_category` | String | Apple broadcast: Apple advertising category for the stream. |
| `round_name` | String | Name of the round or series this match belongs to. |
| `competition_phase` | String | Competition: competition phase of the match (regular season, playoffs, ...). |
| `home_club_rank` | String | Home club: club's standings rank at the time of the request. |
| `away_club_rank` | String | Away club: club's standings rank at the time of the request. |
| `round_number` | Int64 |  |
| `round_group` | String | Group label within the round (tournaments). |
| `match_day` | String | Matchday / round number |
| `calendar_url` | String | Calendar (.ics) subscription URL for the match. |
| `delayed_match` | Boolean | Whether the match is flagged as delayed. |
| `priority_match_date_to` | String | Priority-match window: end of the display window (ISO 8601). |
| `priority_match_sponsor` | String | Priority-match window: sponsor name attached to the priority match. |
| `home_opta_id` | String | Home club: parallel Opta integer id for the entity. |
| `home_sportec_id` | String | Home club: Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `home_full_name` | String | Home club: full name |
| `home_slug` | String | Home club: URL slug. |
| `home_short_name` | String | Home club: short name |
| `home_abbreviation` | String |  |
| `home_background_color` | String | Home club: brand background colour (hex). |
| `home_logo_bw_slug` | String | Home club: asset slug for the black-and-white logo. |
| `home_logo_color_slug` | String | Home club: asset slug for the full-colour logo. |
| `home_logo_color_url` | String | Home club: URL of the full-colour logo asset. |
| `home_crest_color_slug` | String | Home club: asset slug for the full-colour club crest. |
| `away_opta_id` | String | Away club: parallel Opta integer id for the entity. |
| `away_sportec_id` | String | Away club: Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `away_full_name` | String | Away club: full name |
| `away_slug` | String | Away club: URL slug. |
| `away_short_name` | String | Away club: short name |
| `away_abbreviation` | String |  |
| `away_background_color` | String | Away club: brand background colour (hex). |
| `away_logo_bw_slug` | String | Away club: asset slug for the black-and-white logo. |
| `away_logo_color_slug` | String | Away club: asset slug for the full-colour logo. |
| `away_logo_color_url` | String | Away club: URL of the full-colour logo asset. |
| `away_crest_color_slug` | String | Away club: asset slug for the full-colour club crest. |
| `venue_venue_sportec_id` | String | Venue: Sportec opaque id of the venue (Utf8 join key). |
| `venue_name` | String | Venue: display name. |
| `venue_city` | String | Venue: home city |
| `season_slug` | String | Season: URL slug. |
| `season_opta_id` | String | Season: parallel Opta integer id for the entity. |
| `season_sportec_id` | String | Season: Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `season_competition_opta_id` | String | Season: parallel Opta integer id of the competition. |
| `season_name` | String | Season: display name. |
| `competition_opta_id` | String | Parallel Opta integer id of the competition. |
| `competition_sportec_id` | String | Sportec opaque id of the competition (Utf8 join key). |
| `competition_name` | String | Competition display name |
| `competition_widget_id` | String | Competition: identifier of the embedded provider widget. |
| `competition_slug` | String | Competition: URL slug. |
| `competition_short_name` | String | Competition: short name |
| `competition_match_type` | String | Competition: Match classification |
| `competition_logo_light_slug` | String | Competition light-theme logo: URL slug. |
| `competition_logo_dark_slug` | String | Competition dark-theme logo: URL slug. |
| `competition_block_header_name` | String | Competition: header label used for this competition on the site. |
| `competition_mgm_id` | String | Competition: MGM sportsbook partner identifier. |
| `competition_nextgen_ecal_match_hub_display` | Boolean | Competition: whether the match hub shows the eCal subscribe control. |
| `competition_player_headshot_thumbnail_field` | String | Competition: content field the site reads player headshots from. |
| `league_promo_image_asset_url` | String | League promo image: URL of the image asset. |
| `third_party_tickets_url` | String | Third-party ticketing link: link URL. |
| `third_party_tickets_open_in_new_tab` | Boolean | Third-party ticketing link: whether the link opens in a new tab. |
| `third_party_tickets_is_visible` | Boolean | Third-party ticketing link: whether the link is shown. |
| `priority_match_date_from` | String | Priority-match window: start of the display window (ISO 8601). |
| `home_ecal_widget_id` | String | Home club: identifier of the eCal calendar-subscription widget. |
| `away_ecal_widget_id` | String | Away club: identifier of the eCal calendar-subscription widget. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mls_sportapi_matches_by_sportec_ids-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. The MLS host returned 404 (unknown id, or an unplayed match on `/matches/{matchId}`). On the season-addressable stats-api endpoints (`season_matches`, `standings`, `competition_seasons`) a season before 2024 has no addressable id -- the stats-api serves 2024+ only. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mls_sportapi_matches_by_sportec_ids-example}

```python
mls_sportapi_matches_by_sportec_ids(ids='MLS-MAT-0009H8')
```

## mls_standings

Standings table for a competition season (stats-api).

**Endpoint URL:** `GET https://stats-api.mlssoccer.com/competitions/{competition_id}/seasons/{season_id}/standings`

**Valid URL:** [https://stats-api.mlssoccer.com/competitions/MLS-COM-000001/seasons/MLS-SEA-0001KA/standings](https://stats-api.mlssoccer.com/competitions/MLS-COM-000001/seasons/MLS-SEA-0001KA/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_id` | `competition_id` |  | `Y` |  | competition_id path parameter. |
| `season_id` | `season_id` |  | `Y` |  | season_id path parameter. |
| `category` | `category` |  |  | `Y` | Standings grouping: conference \| overall |
| `type` | `standings_type` |  |  | `Y` | home \| away split (optional) |
| `is_live` | `is_live` |  |  | `Y` | Include live in-progress results |

### Returns {#mls_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | String | Sportec competition id |
| `season_id` | String | Sportec season id |
| `group` | String | Group name (tournaments) |
| `category` | String |  |
| `type` | String | Type discriminator for the record. |
| `position` | Int64 | Table position/rank |
| `club` | String | Club full name |
| `club_id` | String | Sportec club id |
| `team` | String | Team name |
| `team_id` | String | Sportec club id |
| `team_short_name` | String | Short club name |
| `team_three_letter_code` | String | 3-letter club code |
| `games_played` | Int64 | Matches played |
| `wins` | Int64 | Wins |
| `draws` | Int64 | Draws |
| `losses` | Int64 | Losses |
| `goals_scored` | Int64 | Goals for |
| `goals_against` | Int64 | Goals against |
| `goals_difference` | Int64 | Goal differential |
| `points` | Int64 | Points |
| `qualification` | String | Playoff/qualification marker |
| `tendency` | String | Movement vs prior matchday |
| `points_per_game` | Float64 | Points per game |
| `goals_scored_per_game` | Float64 | Goals-for per game |
| `goals_against_per_game` | Float64 | Goals-against per game |
| `goals_difference_per_game` | Float64 | Goal-diff per game |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mls_standings-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. The MLS host returned 404 (unknown id, or an unplayed match on `/matches/{matchId}`). On the season-addressable stats-api endpoints (`season_matches`, `standings`, `competition_seasons`) a season before 2024 has no addressable id -- the stats-api serves 2024+ only. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mls_standings-example}

```python
mls_standings(competition_id='MLS-COM-000001', season_id='MLS-SEA-0001KA')
```
