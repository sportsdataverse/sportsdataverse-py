---
title: MLS — MLS official web API (mlssoccer.com)
sidebar_label: MLS official web API (mlssoccer.com)
description: "MLS — MLS official web API (mlssoccer.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
---
# MLS — MLS official web API (mlssoccer.com)

`sportsdataverse.mls` — 12 endpoints.

## `mls_club`

Single club detail (stats-api).

**Endpoint URL:** `GET https://stats-api.mlssoccer.com/clubs/{club_id}`

**Valid URL:** [https://stats-api.mlssoccer.com/clubs/MLS-CLU-000001](https://stats-api.mlssoccer.com/clubs/MLS-CLU-000001)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `club_id` | `club_id` |  | `Y` |  | club_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `club_id` | character | Sportec club id |
| `club_name` | character | Full club name |
| `three_letter_code` | character | 3-letter code |
| `short_name` | character | Short name |
| `club_short_name` | character | Alternate short name |
| `club_three_letter_code` | character | Alternate 3-letter code |
| `city` | character | Home city |
| `country` | character | Country |
| `long_name` | character | Long/legal club name |
| `founded` | character | Year founded |
| `stadium_id` | character | Stadium id |
| `stadium_name` | character | Stadium name |
| `club_color_one_club_color` | character | Primary club colour: club colour name. |
| `club_color_one_club_color_rgb` | character | Primary club colour: club colour as an RGB hex string. |
| `club_color_two_club_color` | character | Secondary club colour: club colour name. |
| `club_color_two_club_color_rgb` | character | Secondary club colour: club colour as an RGB hex string. |
| `club_color_three_club_color` | character | Tertiary club colour: club colour name. |
| `club_color_three_club_color_rgb` | character | Tertiary club colour: club colour as an RGB hex string. |
| `shirt_one_shirt_main_color` | character | First-choice kit: main shirt colour name. |
| `shirt_one_shirt_main_color_rgb` | character | First-choice kit: main shirt colour as an RGB hex string. |
| `shirt_one_shirt_secondary_color` | character | First-choice kit: secondary shirt colour name. |
| `shirt_one_shirt_secondary_color_rgb` | character | First-choice kit: secondary shirt colour as an RGB hex string. |
| `shirt_one_shirt_number_color` | character | First-choice kit: shirt-number colour name. |
| `shirt_one_shirt_number_color_rgb` | character | First-choice kit: shirt-number colour as an RGB hex string. |
| `shirt_two_shirt_main_color` | character | Second-choice kit: main shirt colour name. |
| `shirt_two_shirt_main_color_rgb` | character | Second-choice kit: main shirt colour as an RGB hex string. |
| `shirt_two_shirt_secondary_color` | character | Second-choice kit: secondary shirt colour name. |
| `shirt_two_shirt_secondary_color_rgb` | character | Second-choice kit: secondary shirt colour as an RGB hex string. |
| `shirt_two_shirt_number_color` | character | Second-choice kit: shirt-number colour name. |
| `shirt_two_shirt_number_color_rgb` | character | Second-choice kit: shirt-number colour as an RGB hex string. |
| `shirt_three_shirt_main_color` | character | Third-choice kit: main shirt colour name. |
| `shirt_three_shirt_main_color_rgb` | character | Third-choice kit: main shirt colour as an RGB hex string. |
| `shirt_three_shirt_secondary_color` | character | Third-choice kit: secondary shirt colour name. |
| `shirt_three_shirt_secondary_color_rgb` | character | Third-choice kit: secondary shirt colour as an RGB hex string. |
| `shirt_three_shirt_number_color` | character | Third-choice kit: shirt-number colour name. |
| `shirt_three_shirt_number_color_rgb` | character | Third-choice kit: shirt-number colour as an RGB hex string. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mls_club(club_id='MLS-CLU-000001')
```

_Last validated n/a._

## `mls_competition_seasons`

List seasons for a competition (stats-api).

**Endpoint URL:** `GET https://stats-api.mlssoccer.com/competitions/{competition_id}/seasons`

**Valid URL:** [https://stats-api.mlssoccer.com/competitions/MLS-COM-000001/seasons](https://stats-api.mlssoccer.com/competitions/MLS-COM-000001/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_id` | `competition_id` |  | `Y` |  | competition_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season_id` | character | Sportec season id |
| `season` | integer | Season year |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mls_competition_seasons(competition_id='MLS-COM-000001')
```

_Last validated n/a._

## `mls_competitions`

List competitions (stats-api).

**Endpoint URL:** `GET https://stats-api.mlssoccer.com/competitions`

**Valid URL:** [https://stats-api.mlssoccer.com/competitions](https://stats-api.mlssoccer.com/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `competition_id` | character | Sportec competition id |
| `competition_name` | character | Competition display name |
| `competition_name_french` | character | French competition name |
| `country` | character | Country |
| `competition_type` | character | `League` or `Tournament` |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mls_competitions()
```

_Last validated n/a._

## `mls_content_season`

Season content entity by slug (dapi / Contentful).

**Endpoint URL:** `GET https://dapi.mlssoccer.com/v2/content/en-us/seasons/{slug}`

**Valid URL:** [https://dapi.mlssoccer.com/v2/content/en-us/seasons/mls-regular-season-2026](https://dapi.mlssoccer.com/v2/content/en-us/seasons/mls-regular-season-2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `slug` | `slug` |  | `Y` |  | slug path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `type` | character | Type discriminator for the record. |
| `_translation_id` | character | Contentful translation-group id shared by every locale of this entry. |
| `_entity_id` | character | Contentful entity id for the content entry. |
| `self_url` | character | Canonical API URL of this content entry. |
| `slug` | character | URL slug. |
| `title` | character | Display title. |
| `tags` | character | Content tags, JSON-encoded. |
| `relations` | character | Related content entries, JSON-encoded. |
| `created_by` | character | User or service that created the content entry. |
| `last_updated_by` | character | User or service that last updated the content entry. |
| `last_updated_date` | character | Timestamp of the last content update (ISO 8601). |
| `content_date` | character | Editorial content date (ISO 8601). |
| `featured` | integer | Whether the entry is flagged as featured (1 = featured). |
| `entity_code` | character | Contentful entity-type code. |
| `_list_availability` | integer | Contentful flag controlling whether the entry appears in listings. |
| `references_list_of_clubs_clinched_x` | character | Contentful reference list: reference list of clubs that have clinched, list variant x. |
| `references_list_of_clubs_clinched_e` | character | Contentful reference list: reference list of clubs that have clinched, list variant e. |
| `references_list_of_clubs_clinched_s` | character | Contentful reference list: reference list of clubs that have clinched, list variant s. |
| `references_list_of_clubs_clinched_y` | character | Contentful reference list: reference list of clubs that have clinched, list variant y. |
| `fields_opta_id` | character | Season content field: parallel Opta integer id for the entity. |
| `fields_sportec_id` | character | Season content field: Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `fields_name` | character | Season content field: display name. |
| `fields_competition_opta_id` | character | Season content field: parallel Opta integer id of the competition. |
| `fields_competition_sportec_id` | character | Season content field: Sportec opaque id of the competition (Utf8 join key). |
| `fields_standings_legend` | character | Season content field: prose legend explaining the standings qualification bands. |
| `fields_top_clubs` | integer | Season content field: number of clubs qualifying from the top of the table. |
| `fields_top_clubs_legend` | character | Season content field: prose legend explaining the top-clubs cut line. |
| `fields_home_advantage` | integer | Season content field: points of home advantage applied by the playoff seeding rules. |
| `fields_home_advantage_legend` | character | Season content field: prose legend explaining the home-advantage rule. |
| `fields_playoff_qualified_east_conference` | integer | Season content field: count of Eastern Conference clubs that have clinched a playoff berth. |
| `fields_playoff_qualified_west_conference` | integer | Season content field: count of Western Conference clubs that have clinched a playoff berth. |
| `fields_competition_sportec_id_overwrite` | logical | Season content field: competition: whether the season's Sportec id is manually overridden in the CMS. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mls_content_season(slug='mls-regular-season-2026')
```

_Last validated n/a._

## `mls_content_seasons`

Query season content entities (dapi / Contentful).

**Endpoint URL:** `GET https://dapi.mlssoccer.com/v2/content/en-us/seasons`

**Valid URL:** [https://dapi.mlssoccer.com/v2/content/en-us/seasons](https://dapi.mlssoccer.com/v2/content/en-us/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `fields.competitionSportecId` | `competition_sportec_id` |  |  | `Y` | Filter by competition Sportec id (indexed field) |
| `fields.sportecId` | `sportec_id` |  |  | `Y` | Filter by season Sportec id (indexed field) |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `type` | character | Type discriminator for the record. |
| `_translation_id` | character | Contentful translation-group id shared by every locale of this entry. |
| `_entity_id` | character | Contentful entity id for the content entry. |
| `self_url` | character | Canonical API URL of this content entry. |
| `slug` | character | URL slug. |
| `title` | character | Display title. |
| `tags` | character | Content tags, JSON-encoded. |
| `created_by` | character | User or service that created the content entry. |
| `last_updated_by` | character | User or service that last updated the content entry. |
| `last_updated_date` | character | Timestamp of the last content update (ISO 8601). |
| `content_date` | character | Editorial content date (ISO 8601). |
| `featured` | integer | Whether the entry is flagged as featured (1 = featured). |
| `entity_code` | character | Contentful entity-type code. |
| `fields_opta_id` | character | Season content field: parallel Opta integer id for the entity. |
| `fields_sportec_id` | character | Season content field: Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `fields_name` | character | Season content field: display name. |
| `fields_competition_opta_id` | character | Season content field: parallel Opta integer id of the competition. |
| `fields_competition_sportec_id` | character | Season content field: Sportec opaque id of the competition (Utf8 join key). |
| `fields_standings_legend` | character | Season content field: prose legend explaining the standings qualification bands. |
| `fields_top_clubs` | integer | Season content field: number of clubs qualifying from the top of the table. |
| `fields_top_clubs_legend` | character | Season content field: prose legend explaining the top-clubs cut line. |
| `fields_home_advantage` | integer | Season content field: points of home advantage applied by the playoff seeding rules. |
| `fields_home_advantage_legend` | character | Season content field: prose legend explaining the home-advantage rule. |
| `fields_playoff_qualified_east_conference` | integer | Season content field: count of Eastern Conference clubs that have clinched a playoff berth. |
| `fields_playoff_qualified_west_conference` | integer | Season content field: count of Western Conference clubs that have clinched a playoff berth. |
| `fields_competition_sportec_id_overwrite` | logical | Season content field: competition: whether the season's Sportec id is manually overridden in the CMS. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mls_content_seasons()
```

_Last validated n/a._

## `mls_match`

Single match detail incl. lineups + referees (stats-api).

**Endpoint URL:** `GET https://stats-api.mlssoccer.com/matches/{match_id}`

**Valid URL:** [https://stats-api.mlssoccer.com/matches/MLS-MAT-0009H8](https://stats-api.mlssoccer.com/matches/MLS-MAT-0009H8)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | match_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `competition_id` | character | Sportec competition id |
| `competition_name` | character | Competition display name |
| `away_team_goals` | integer | Away goals |
| `home_team_goals` | integer | Home goals |
| `kickoff_time` | character | Actual kickoff timestamp (ISO 8601) |
| `match_day` | integer | Matchday / round number |
| `match_id` | character | Sportec match id (MLS-MAT-*) |
| `match_title` | character | Home:Away title, e.g. `CF Montréal:Toronto FC` |
| `planned_kickoff_time` | character | Scheduled kickoff timestamp (ISO 8601) |
| `result` | character | Final score string, e.g. `0:0` |
| `season` | integer | Season year |
| `season_id` | character | Sportec season id |
| `competition_type` | character | `League` or `Tournament` |
| `section_name` | character | Season section / phase |
| `competition_label` | character | Short competition label |
| `series_type` | character | Series type (e.g. regular) |
| `total_time_first_half` | integer | First-half elapsed seconds |
| `total_time_second_half` | integer | Second-half elapsed seconds |
| `playing_time_first_half` | integer | First-half playing seconds |
| `playing_time_second_half` | integer | Second-half playing seconds |
| `total_time_first_half_extra` | integer | First-half stoppage seconds |
| `total_time_second_half_extra` | integer | Second-half stoppage seconds |
| `total_time_penalty` | integer | Penalty-shootout total seconds |
| `playing_time_penalty` | integer | Penalty-shootout playing seconds |
| `other_information` | character | Free-form notes |
| `match_type` | character | Match classification |
| `match_scheduled` | logical | Whether kickoff is scheduled |
| `date_quality` | character | Confidence of the match date |
| `end_date` | character | Match end date |
| `sub_league` | character | Sub-league grouping |
| `group` | character | Group name (tournaments) |
| `competition_name_french` | character | French competition name |
| `match_status` | character | Status (e.g. `Live`, `FullTime`) |
| `minute_of_play` | character | Current minute (live) |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mls_match(match_id='MLS-MAT-0009H8')
```

_Last validated n/a._

## `mls_season_matches`

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

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `competition_id` | character | Sportec competition id |
| `competition_name` | character | Competition display name |
| `competition_type` | character | `League` or `Tournament` |
| `end_date` | character | Match end date |
| `away_team_id` | character | Away Sportec club id |
| `away_team_name` | character | Away club name |
| `home_team_id` | character | Home Sportec club id |
| `home_team_name` | character | Home club name |
| `home_team_short_name` | character | Home short name |
| `home_team_three_letter_code` | character | Home 3-letter code |
| `away_team_short_name` | character | Away short name |
| `away_team_three_letter_code` | character | Away 3-letter code |
| `match_scheduled` | logical | Whether kickoff is scheduled |
| `match_day` | integer | Matchday / round number |
| `match_day_id` | character | Sportec matchday id |
| `match_id` | character | Sportec match id (MLS-MAT-*) |
| `match_type` | character | Match classification |
| `planned_kickoff_time` | character | Scheduled kickoff timestamp (ISO 8601) |
| `season` | integer | Season year |
| `season_id` | character | Sportec season id |
| `stadium_id` | character | Stadium id |
| `stadium_name` | character | Stadium name |
| `neutral_venue` | logical | Neutral-venue flag |
| `start_date` | character | Match start date |
| `sub_league` | character | Sub-league grouping |
| `group` | character | Group name (tournaments) |
| `date_quality` | character | Confidence of the match date |
| `official_information` | character | Officiating notes |
| `match_date_time_status` | character | Kickoff time status |
| `section_name` | character | Season section / phase |
| `competition_label` | character | Short competition label |
| `series_type` | character | Series type (e.g. regular) |
| `competition_name_french` | character | French competition name |
| `result` | character | Final score string, e.g. `0:0` |
| `away_team_goals` | integer | Away goals |
| `home_team_goals` | integer | Home goals |
| `match_status` | character | Status (e.g. `Live`, `FullTime`) |
| `minute_of_play` | character | Current minute (live) |
| `stadium_city` | character | Stadium city |
| `stadium_country` | character | Stadium country |
| `bracket_structure_id` | character | Identifier of the playoff bracket structure. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mls_season_matches(season_id='MLS-SEA-0001KA')
```

_Last validated n/a._

## `mls_sportapi_club_players`

Club roster with player Sportec ids (sportapi).

**Endpoint URL:** `GET https://sportapi.mlssoccer.com/api/players/byClub/{club_id}`

**Valid URL:** [https://sportapi.mlssoccer.com/api/players/byClub/MLS-CLU-000001](https://sportapi.mlssoccer.com/api/players/byClub/MLS-CLU-000001)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `club_id` | `club_id` |  | `Y` |  | club_id path parameter. |
| `culture` | `culture` |  |  | `Y` | Locale, e.g. en-us |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `opta_id` | character | Parallel Opta integer id for the entity. |
| `full_name` | character | Full name |
| `first_name` | character | First name |
| `last_name` | character | Last name |
| `known_name` | character | Known / display name |
| `on_loan` | logical | On-loan flag |
| `club_opta_id` | character | Club Opta id |
| `club_sportec_id` | character | Club Sportec id |
| `sportec_id` | character | Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `height` | character | Height |
| `weight` | character | Weight |
| `position` | character | Table position/rank |
| `roster_category` | character | Roster category |
| `player_categories` | character | Roster category tags |
| `jersey_number` | character | Jersey number |
| `player_status_list` | character | Status tags (e.g. `Loaned Out`) |
| `date_of_birth` | character | Birth date |
| `player_slug` | character | URL slug |
| `thumbnail_slug` | character | Player thumbnail image: URL slug. |
| `thumbnail_self_url` | character | Player thumbnail image: canonical API URL of this content entry. |
| `thumbnail_title` | character | Player thumbnail image: display title. |
| `thumbnail_template_url` | character | Player thumbnail image: templated image URL with substitutable size tokens. |
| `thumbnail_thumbnail_url` | character | Player thumbnail image: URL of the rendered thumbnail image. |
| `thumbnail_format` | character | Player thumbnail image: image format of the asset. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mls_sportapi_club_players(club_id='MLS-CLU-000001')
```

_Last validated n/a._

## `mls_sportapi_clubs_by_sportec_ids`

Batch club detail by Sportec ids (sportapi).

**Endpoint URL:** `GET https://sportapi.mlssoccer.com/api/clubs/bySportecIds/{ids}`

**Valid URL:** [https://sportapi.mlssoccer.com/api/clubs/bySportecIds/MLS-MAT-0009H8](https://sportapi.mlssoccer.com/api/clubs/bySportecIds/MLS-MAT-0009H8)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ids` | `ids` |  | `Y` |  | ids path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `opta_id` | character | Parallel Opta integer id for the entity. |
| `sportec_id` | character | Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `full_name` | character | Full name |
| `slug` | character | URL slug. |
| `short_name` | character | Short name |
| `abbreviation` | character | Short abbreviation. |
| `background_color` | character | Brand background colour (hex). |
| `logo_bw_slug` | character | Asset slug for the black-and-white logo. |
| `logo_color_slug` | character | Asset slug for the full-colour logo. |
| `logo_color_url` | character | URL of the full-colour logo asset. |
| `crest_color_slug` | character | Asset slug for the full-colour club crest. |
| `ecal_widget_id` | character | Identifier of the eCal calendar-subscription widget. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mls_sportapi_clubs_by_sportec_ids(ids='MLS-MAT-0009H8')
```

_Last validated n/a._

## `mls_sportapi_match`

Single match detail (sportapi / .NET).

**Endpoint URL:** `GET https://sportapi.mlssoccer.com/api/matches/{match_id}`

**Valid URL:** [https://sportapi.mlssoccer.com/api/matches/MLS-MAT-0009H8](https://sportapi.mlssoccer.com/api/matches/MLS-MAT-0009H8)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | match_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `opta_id` | character | Parallel Opta integer id for the entity. |
| `sportec_id` | character | Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `slug` | character | URL slug. |
| `league_match_title` | character | Home:Away match title as rendered in league listings. |
| `broadcasters` | character | Broadcast listings for the match, JSON-encoded. |
| `match_date` | character | Scheduled kickoff timestamp (ISO 8601). |
| `tags` | character | Content tags, JSON-encoded. |
| `home_club_broadcasters` | character | Home club: club-specific broadcast listings, JSON-encoded. |
| `away_club_broadcasters` | character | Away club: club-specific broadcast listings, JSON-encoded. |
| `club_broadcasters` | character | Club-specific broadcast listings, JSON-encoded. |
| `is_time_tbd` | logical | Whether the kickoff time is still to be confirmed. |
| `mgm_id` | character | MGM sportsbook partner identifier. |
| `apple_stream_url` | character | Apple broadcast: Apple TV stream URL for the match. |
| `apple_subscription_tier` | character | Apple broadcast: Apple subscription tier required to watch. |
| `apple_advertisement_category` | character | Apple broadcast: Apple advertising category for the stream. |
| `round_name` | character | Name of the round or series this match belongs to. |
| `competition_phase` | character | Competition: competition phase of the match (regular season, playoffs, ...). |
| `home_club_rank` | character | Home club: club's standings rank at the time of the request. |
| `away_club_rank` | character | Away club: club's standings rank at the time of the request. |
| `round_number` | integer | Draft round number. |
| `round_group` | character | Group label within the round (tournaments). |
| `match_day` | character | Matchday / round number |
| `calendar_url` | character | Calendar (.ics) subscription URL for the match. |
| `delayed_match` | logical | Whether the match is flagged as delayed. |
| `priority_match_date_to` | character | Priority-match window: end of the display window (ISO 8601). |
| `priority_match_sponsor` | character | Priority-match window: sponsor name attached to the priority match. |
| `home_opta_id` | character | Home club: parallel Opta integer id for the entity. |
| `home_sportec_id` | character | Home club: Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `home_full_name` | character | Home club: full name |
| `home_slug` | character | Home club: URL slug. |
| `home_short_name` | character | Home club: short name |
| `home_abbreviation` | character | Home team's abbreviation. |
| `home_background_color` | character | Home club: brand background colour (hex). |
| `home_logo_bw_slug` | character | Home club: asset slug for the black-and-white logo. |
| `home_logo_color_slug` | character | Home club: asset slug for the full-colour logo. |
| `home_logo_color_url` | character | Home club: URL of the full-colour logo asset. |
| `home_crest_color_slug` | character | Home club: asset slug for the full-colour club crest. |
| `home_ecal_widget_id` | character | Home club: identifier of the eCal calendar-subscription widget. |
| `away_opta_id` | character | Away club: parallel Opta integer id for the entity. |
| `away_sportec_id` | character | Away club: Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `away_full_name` | character | Away club: full name |
| `away_slug` | character | Away club: URL slug. |
| `away_short_name` | character | Away club: short name |
| `away_abbreviation` | character | Away team's abbreviation. |
| `away_background_color` | character | Away club: brand background colour (hex). |
| `away_logo_bw_slug` | character | Away club: asset slug for the black-and-white logo. |
| `away_logo_color_slug` | character | Away club: asset slug for the full-colour logo. |
| `away_logo_color_url` | character | Away club: URL of the full-colour logo asset. |
| `away_crest_color_slug` | character | Away club: asset slug for the full-colour club crest. |
| `away_ecal_widget_id` | character | Away club: identifier of the eCal calendar-subscription widget. |
| `venue_venue_sportec_id` | character | Venue: Sportec opaque id of the venue (Utf8 join key). |
| `venue_background_image_slug` | character | Venue: asset slug for the background image. |
| `venue_name` | character | Venue: display name. |
| `venue_city` | character | Venue: home city |
| `season_slug` | character | Season: URL slug. |
| `season_opta_id` | character | Season: parallel Opta integer id for the entity. |
| `season_sportec_id` | character | Season: Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `season_competition_opta_id` | character | Season: parallel Opta integer id of the competition. |
| `season_name` | character | Season: display name. |
| `competition_opta_id` | character | Parallel Opta integer id of the competition. |
| `competition_sportec_id` | character | Sportec opaque id of the competition (Utf8 join key). |
| `competition_name` | character | Competition display name |
| `competition_widget_id` | character | Competition: identifier of the embedded provider widget. |
| `competition_slug` | character | Competition: URL slug. |
| `competition_short_name` | character | Competition: short name |
| `competition_match_type` | character | Competition: Match classification |
| `competition_logo_light_slug` | character | Competition light-theme logo: URL slug. |
| `competition_logo_dark_slug` | character | Competition dark-theme logo: URL slug. |
| `competition_block_header_name` | character | Competition: header label used for this competition on the site. |
| `competition_mgm_id` | character | Competition: MGM sportsbook partner identifier. |
| `competition_nextgen_ecal_match_hub_display` | logical | Competition: whether the match hub shows the eCal subscribe control. |
| `competition_player_headshot_thumbnail_field` | character | Competition: content field the site reads player headshots from. |
| `league_promo_image_asset_url` | character | League promo image: URL of the image asset. |
| `first_party_tickets_display_text` | character | MLS-operated ticketing link: button label for the ticketing link. |
| `first_party_tickets_accessible_text` | character | MLS-operated ticketing link: accessible (screen-reader) label for the ticketing link. |
| `first_party_tickets_url` | character | MLS-operated ticketing link: link URL. |
| `first_party_tickets_open_in_new_tab` | logical | MLS-operated ticketing link: whether the link opens in a new tab. |
| `first_party_tickets_is_visible` | logical | MLS-operated ticketing link: whether the link is shown. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mls_sportapi_match(match_id='MLS-MAT-0009H8')
```

_Last validated n/a._

## `mls_sportapi_matches_by_sportec_ids`

Batch match detail by Sportec ids (sportapi).

**Endpoint URL:** `GET https://sportapi.mlssoccer.com/api/matches/bySportecIds/{ids}`

**Valid URL:** [https://sportapi.mlssoccer.com/api/matches/bySportecIds/MLS-MAT-0009H8](https://sportapi.mlssoccer.com/api/matches/bySportecIds/MLS-MAT-0009H8)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ids` | `ids` |  | `Y` |  | ids path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `opta_id` | character | Parallel Opta integer id for the entity. |
| `sportec_id` | character | Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `slug` | character | URL slug. |
| `league_match_title` | character | Home:Away match title as rendered in league listings. |
| `broadcasters` | character | Broadcast listings for the match, JSON-encoded. |
| `match_date` | character | Scheduled kickoff timestamp (ISO 8601). |
| `tags` | character | Content tags, JSON-encoded. |
| `home_club_broadcasters` | character | Home club: club-specific broadcast listings, JSON-encoded. |
| `away_club_broadcasters` | character | Away club: club-specific broadcast listings, JSON-encoded. |
| `club_broadcasters` | character | Club-specific broadcast listings, JSON-encoded. |
| `is_time_tbd` | logical | Whether the kickoff time is still to be confirmed. |
| `mgm_id` | character | MGM sportsbook partner identifier. |
| `apple_stream_url` | character | Apple broadcast: Apple TV stream URL for the match. |
| `apple_subscription_tier` | character | Apple broadcast: Apple subscription tier required to watch. |
| `apple_advertisement_category` | character | Apple broadcast: Apple advertising category for the stream. |
| `round_name` | character | Name of the round or series this match belongs to. |
| `competition_phase` | character | Competition: competition phase of the match (regular season, playoffs, ...). |
| `home_club_rank` | character | Home club: club's standings rank at the time of the request. |
| `away_club_rank` | character | Away club: club's standings rank at the time of the request. |
| `round_number` | integer | Draft round number. |
| `round_group` | character | Group label within the round (tournaments). |
| `match_day` | character | Matchday / round number |
| `calendar_url` | character | Calendar (.ics) subscription URL for the match. |
| `delayed_match` | logical | Whether the match is flagged as delayed. |
| `priority_match_date_to` | character | Priority-match window: end of the display window (ISO 8601). |
| `priority_match_sponsor` | character | Priority-match window: sponsor name attached to the priority match. |
| `home_opta_id` | character | Home club: parallel Opta integer id for the entity. |
| `home_sportec_id` | character | Home club: Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `home_full_name` | character | Home club: full name |
| `home_slug` | character | Home club: URL slug. |
| `home_short_name` | character | Home club: short name |
| `home_abbreviation` | character | Home team's abbreviation. |
| `home_background_color` | character | Home club: brand background colour (hex). |
| `home_logo_bw_slug` | character | Home club: asset slug for the black-and-white logo. |
| `home_logo_color_slug` | character | Home club: asset slug for the full-colour logo. |
| `home_logo_color_url` | character | Home club: URL of the full-colour logo asset. |
| `home_crest_color_slug` | character | Home club: asset slug for the full-colour club crest. |
| `away_opta_id` | character | Away club: parallel Opta integer id for the entity. |
| `away_sportec_id` | character | Away club: Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `away_full_name` | character | Away club: full name |
| `away_slug` | character | Away club: URL slug. |
| `away_short_name` | character | Away club: short name |
| `away_abbreviation` | character | Away team's abbreviation. |
| `away_background_color` | character | Away club: brand background colour (hex). |
| `away_logo_bw_slug` | character | Away club: asset slug for the black-and-white logo. |
| `away_logo_color_slug` | character | Away club: asset slug for the full-colour logo. |
| `away_logo_color_url` | character | Away club: URL of the full-colour logo asset. |
| `away_crest_color_slug` | character | Away club: asset slug for the full-colour club crest. |
| `venue_venue_sportec_id` | character | Venue: Sportec opaque id of the venue (Utf8 join key). |
| `venue_name` | character | Venue: display name. |
| `venue_city` | character | Venue: home city |
| `season_slug` | character | Season: URL slug. |
| `season_opta_id` | character | Season: parallel Opta integer id for the entity. |
| `season_sportec_id` | character | Season: Sportec opaque id for the entity (Utf8 join key, never numeric). |
| `season_competition_opta_id` | character | Season: parallel Opta integer id of the competition. |
| `season_name` | character | Season: display name. |
| `competition_opta_id` | character | Parallel Opta integer id of the competition. |
| `competition_sportec_id` | character | Sportec opaque id of the competition (Utf8 join key). |
| `competition_name` | character | Competition display name |
| `competition_widget_id` | character | Competition: identifier of the embedded provider widget. |
| `competition_slug` | character | Competition: URL slug. |
| `competition_short_name` | character | Competition: short name |
| `competition_match_type` | character | Competition: Match classification |
| `competition_logo_light_slug` | character | Competition light-theme logo: URL slug. |
| `competition_logo_dark_slug` | character | Competition dark-theme logo: URL slug. |
| `competition_block_header_name` | character | Competition: header label used for this competition on the site. |
| `competition_mgm_id` | character | Competition: MGM sportsbook partner identifier. |
| `competition_nextgen_ecal_match_hub_display` | logical | Competition: whether the match hub shows the eCal subscribe control. |
| `competition_player_headshot_thumbnail_field` | character | Competition: content field the site reads player headshots from. |
| `league_promo_image_asset_url` | character | League promo image: URL of the image asset. |
| `third_party_tickets_url` | character | Third-party ticketing link: link URL. |
| `third_party_tickets_open_in_new_tab` | character | Third-party ticketing link: whether the link opens in a new tab. |
| `third_party_tickets_is_visible` | character | Third-party ticketing link: whether the link is shown. |
| `priority_match_date_from` | character | Priority-match window: start of the display window (ISO 8601). |
| `home_ecal_widget_id` | character | Home club: identifier of the eCal calendar-subscription widget. |
| `away_ecal_widget_id` | character | Away club: identifier of the eCal calendar-subscription widget. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mls_sportapi_matches_by_sportec_ids(ids='MLS-MAT-0009H8')
```

_Last validated n/a._

## `mls_standings`

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

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `competition_id` | character | Sportec competition id |
| `season_id` | character | Sportec season id |
| `group` | character | Group name (tournaments) |
| `category` | character | Category label. |
| `type` | character | Type discriminator for the record. |
| `position` | integer | Table position/rank |
| `club` | character | Club full name |
| `club_id` | character | Sportec club id |
| `team` | character | Team name |
| `team_id` | character | Sportec club id |
| `team_short_name` | character | Short club name |
| `team_three_letter_code` | character | 3-letter club code |
| `games_played` | integer | Matches played |
| `wins` | integer | Wins |
| `draws` | integer | Draws |
| `losses` | integer | Losses |
| `goals_scored` | integer | Goals for |
| `goals_against` | integer | Goals against |
| `goals_difference` | integer | Goal differential |
| `points` | integer | Points |
| `qualification` | character | Playoff/qualification marker |
| `tendency` | character | Movement vs prior matchday |
| `points_per_game` | numeric | Points per game |
| `goals_scored_per_game` | numeric | Goals-for per game |
| `goals_against_per_game` | numeric | Goals-against per game |
| `goals_difference_per_game` | numeric | Goal-diff per game |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mls_standings(competition_id='MLS-COM-000001', season_id='MLS-SEA-0001KA')
```

_Last validated n/a._
