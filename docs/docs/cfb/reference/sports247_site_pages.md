---
title: CFB — 247Sports Site Pages (247sports.com)
sidebar_label: 247Sports Site Pages (247sports.com)
description: "CFB — 247Sports Site Pages (247sports.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 12
---
# CFB — 247Sports Site Pages (247sports.com)

`sportsdataverse.cfb` — 35 endpoints.

## `sports247_site_pages_coach`

Coach identity detail.

**Endpoint URL:** `GET https://247sports.com/Coach/{key}.json`

**Valid URL:** [https://247sports.com/Coach](https://247sports.com/Coach)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `first_name` | character | Athlete first name. |
| `last_name` | character | Athlete last name. |
| `full_name` | character | Venue full name (e.g. `Tenney Stadium`). |
| `birthdate` | character | Birthdate |
| `hometown` | integer | Prospect hometown. |
| `alma_mater` | integer | School the coach graduated from, per 247Sports. |
| `cbs_key` | character | CBS Sports identifier for the coach (247Sports is a CBS Sports property). |
| `twitter_contact` | character | Coach's Twitter/X handle on the 247Sports profile. |
| `predictions_locked` | character | 247Sports flag that Crystal Ball prediction entries tied to the coach are locked. |
| `primary_coach_job` | integer | Nested 247Sports record for the coach's current job (stringified). |
| `default_asset` | integer | Nested 247Sports image asset for the coach's headshot (stringified). |
| `hero_asset` | character | Nested 247Sports hero (banner) image asset for the coach page (stringified). |
| `quote_asset` | character | Nested 247Sports image asset used alongside the coach's quote block (stringified). |
| `default_name` | character | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_coach()
```

_Last validated n/a._

## `sports247_site_pages_coach_alma_mater`

Coach alma-mater Institution.

**Endpoint URL:** `GET https://247sports.com/Coach/{key}/AlmaMater.json`

**Valid URL:** [https://247sports.com/Coach/1504/AlmaMater.json](https://247sports.com/Coach/1504/AlmaMater.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `name` | character | Position name (e.g. `Quarterback`). |
| `type` | character | Institution type code (college / pro / high school). |
| `group` | character | Institution group (division/level) bitmask code. |
| `location` | integer | FK -> Location (`/Institution/{Location}/Location.json`). |
| `state` | integer | FK -> State entity. |
| `latitude` | character | Venue latitude in decimal degrees. |
| `longitude` | character | Venue longitude in decimal degrees. |
| `rankable` | character | Whether the institution participates in class rankings. |
| `mascot` | character | Team mascot. |
| `abbreviation` | character | Metric abbreviation. |
| `primary_color` | character | Primary team color (hex). |
| `secondary_color` | character | Secondary team color (hex). |
| `is_foreign` | character | Whether the institution is located outside the United States. |
| `site` | integer | FK -> team Site (network site key). |
| `default_asset` | integer | Nested 247Sports image asset for the institution's primary logo (stringified). |
| `alternate_asset` | integer | Nested 247Sports image asset for the institution's alternate logo (stringified). |
| `light_asset` | integer | Nested 247Sports image asset for the light-background logo variant (stringified). |
| `default_name` | character | Server-rendered display label for the entity. |
| `address` | character | Institution's street address. |
| `telephone` | character | Institution's telephone number. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_coach_alma_mater(key=1504)
```

_Last validated n/a._

## `sports247_site_pages_coach_hometown`

Coach hometown Location.

**Endpoint URL:** `GET https://247sports.com/Coach/{key}/Hometown.json`

**Valid URL:** [https://247sports.com/Coach/1504/Hometown.json](https://247sports.com/Coach/1504/Hometown.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `postal_code` | character | Postal code of the venue. |
| `city` | character | Venue city. |
| `state` | integer | Venue state. |
| `latitude` | character | Venue latitude in decimal degrees. |
| `longitude` | character | Venue longitude in decimal degrees. |
| `county_tax_rate` | character | County income-tax rate for the location, carried on the 247Sports location record. |
| `city_tax_rate` | character | City income-tax rate for the location, carried on the 247Sports location record. |
| `special_tax_rate` | character | Special-district tax rate for the location, carried on the 247Sports location record. |
| `region_name` | character | Name of the region (state/province) for the location. |
| `default_name` | character | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_coach_hometown(key=1504)
```

_Last validated n/a._

## `sports247_site_pages_coach_ranking`

Single CoachRanking row.

**Endpoint URL:** `GET https://247sports.com/CoachRanking/{key}.json`

**Valid URL:** [https://247sports.com/CoachRanking](https://247sports.com/CoachRanking)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `coach` | integer | Coach. |
| `institution` | integer | Nested 247Sports institution the coach recruited for during the ranking cycle (stringified). |
| `conference` | integer | Conference of the team. |
| `ranking` | integer | FK -> the Ranking snapshot this row belongs to. |
| `sport` | integer | Nested 247Sports sport the ranking covers (stringified). |
| `recruitment` | character | Nested 247Sports recruitment record credited to the coach on this row (stringified). |
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `scout_rating` | character | Total 247Sports in-house (scout) rating points credited to the coach's commits. |
| `composite_rating` | character | Composite class rating for the coach's haul. |
| `commits` | character | Number of commits credited to the coach in the ranking. |
| `total` | character | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `composite_total` | character | Total 247Sports Composite rating points credited to the coach's commits. |
| `five_stars` | character | Number of five-star commits credited to the coach. |
| `scout_five_stars` | character | Number of five-star commits by 247Sports' own (scout) rating. |
| `composite_five_stars` | character | Number of five-star commits by the 247Sports Composite rating. |
| `four_stars` | character | Number of four-star commits credited to the coach. |
| `scout_four_stars` | character | Number of four-star commits by 247Sports' own (scout) rating. |
| `composite_four_stars` | character | Number of four-star commits by the 247Sports Composite rating. |
| `three_stars` | character | Number of three-star commits credited to the coach. |
| `scout_three_stars` | character | Number of three-star commits by 247Sports' own (scout) rating. |
| `composite_three_stars` | character | Number of three-star commits by the 247Sports Composite rating. |
| `two_stars` | character | Number of two-star commits credited to the coach. |
| `scout_two_stars` | character | Number of two-star commits by 247Sports' own (scout) rating. |
| `composite_two_stars` | character | Number of two-star commits by the 247Sports Composite rating. |
| `average_rating` | character | Average rating across the coach's credited commits. |
| `average_scout_rating` | character | Average 247Sports in-house (scout) rating across the credited commits. |
| `composite_average_rating` | character | Average 247Sports Composite rating across the credited commits. |
| `overall_rank` | character | Overall national coach-recruiting rank. |
| `composite_overall_rank` | character | Coach's national recruiter rank by Composite points. |
| `scout_overall_rank` | character | Coach's national recruiter rank by 247Sports' own (scout) points. |
| `division_rank` | character | Coach's recruiter rank within the division. |
| `scout_division_rank` | character | Coach's division recruiter rank by 247Sports' own (scout) points. |
| `composite_division_rank` | character | Coach's division recruiter rank by Composite points. |
| `conference_rank` | character | Rank within conference. |
| `scout_conference_rank` | character | Coach's conference recruiter rank by 247Sports' own (scout) points. |
| `composite_conference_rank` | character | Coach's conference recruiter rank by Composite points. |
| `previous_coach_ranking` | integer | Nested prior-cycle recruiter-ranking row for the coach (stringified). |
| `default_name` | character | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_coach_ranking()
```

_Last validated n/a._

## `sports247_site_pages_coach_rankings`

Coach's recruiting-ranking history (one row per Ranking snapshot).

**Endpoint URL:** `GET https://247sports.com/Coach/{key}/CoachRankings.json`

**Valid URL:** [https://247sports.com/Coach/1531/CoachRankings.json](https://247sports.com/Coach/1531/CoachRankings.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `coach` | integer | Coach. |
| `institution` | integer | Nested 247Sports institution the coach recruited for during the ranking cycle (stringified). |
| `conference` | integer | Conference of the team. |
| `ranking` | integer | FK -> the Ranking snapshot this row belongs to. |
| `sport` | integer | Nested 247Sports sport the ranking covers (stringified). |
| `recruitment` | character | Nested 247Sports recruitment record credited to the coach on this row (stringified). |
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `scout_rating` | character | Total 247Sports in-house (scout) rating points credited to the coach's commits. |
| `composite_rating` | character | Composite class rating for the coach's haul. |
| `commits` | character | Number of commits credited to the coach in the ranking. |
| `total` | character | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `composite_total` | character | Total 247Sports Composite rating points credited to the coach's commits. |
| `five_stars` | character | Number of five-star commits credited to the coach. |
| `scout_five_stars` | character | Number of five-star commits by 247Sports' own (scout) rating. |
| `composite_five_stars` | character | Number of five-star commits by the 247Sports Composite rating. |
| `four_stars` | character | Number of four-star commits credited to the coach. |
| `scout_four_stars` | character | Number of four-star commits by 247Sports' own (scout) rating. |
| `composite_four_stars` | character | Number of four-star commits by the 247Sports Composite rating. |
| `three_stars` | character | Number of three-star commits credited to the coach. |
| `scout_three_stars` | character | Number of three-star commits by 247Sports' own (scout) rating. |
| `composite_three_stars` | character | Number of three-star commits by the 247Sports Composite rating. |
| `two_stars` | character | Number of two-star commits credited to the coach. |
| `scout_two_stars` | character | Number of two-star commits by 247Sports' own (scout) rating. |
| `composite_two_stars` | character | Number of two-star commits by the 247Sports Composite rating. |
| `average_rating` | character | Average rating across the coach's credited commits. |
| `average_scout_rating` | character | Average 247Sports in-house (scout) rating across the credited commits. |
| `composite_average_rating` | character | Average 247Sports Composite rating across the credited commits. |
| `overall_rank` | character | Overall national coach-recruiting rank. |
| `composite_overall_rank` | character | Coach's national recruiter rank by Composite points. |
| `scout_overall_rank` | character | Coach's national recruiter rank by 247Sports' own (scout) points. |
| `division_rank` | character | Coach's recruiter rank within the division. |
| `scout_division_rank` | character | Coach's division recruiter rank by 247Sports' own (scout) points. |
| `composite_division_rank` | character | Coach's division recruiter rank by Composite points. |
| `conference_rank` | character | Rank within conference. |
| `scout_conference_rank` | character | Coach's conference recruiter rank by 247Sports' own (scout) points. |
| `composite_conference_rank` | character | Coach's conference recruiter rank by Composite points. |
| `previous_coach_ranking` | integer | Nested prior-cycle recruiter-ranking row for the coach (stringified). |
| `default_name` | character | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_coach_rankings(key=1531)
```

_Last validated n/a._

## `sports247_site_pages_event`

Recruiting event detail (camp/combine/regional).

**Endpoint URL:** `GET https://247sports.com/Event/{slug}.json`

**Valid URL:** [https://247sports.com/Event](https://247sports.com/Event)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `slug` | `slug` |  | `Y` |  | slug path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `name` | character | Position name (e.g. `Quarterback`). |
| `event_group` | integer | Grouping or series the event belongs to (e.g. a camp circuit) on 247Sports. |
| `event_type` | integer | Event / play type code (V2 PBP). |
| `event_date` | character | Event date-time in ISO 8601 (e.g. '2017-07-11T00:00:00Z'). |
| `default_asset` | integer | Nested 247Sports image asset for the event (stringified). |
| `primary_color` | character | Primary team color (hex). |
| `year` | integer | Four-digit season year (e.g. 2019). |
| `default_name` | character | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_event()
```

_Last validated n/a._

## `sports247_site_pages_institution`

Institution (school/team) detail.

**Endpoint URL:** `GET https://247sports.com/Institution/{key}.json`

**Valid URL:** [https://247sports.com/Institution](https://247sports.com/Institution)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `name` | character | Position name (e.g. `Quarterback`). |
| `type` | character | Institution type code (college / pro / high school). |
| `group` | character | Institution group (division/level) bitmask code. |
| `location` | integer | FK -> Location (`/Institution/{Location}/Location.json`). |
| `state` | integer | FK -> State entity. |
| `latitude` | character | Venue latitude in decimal degrees. |
| `longitude` | character | Venue longitude in decimal degrees. |
| `rankable` | character | Whether the institution participates in class rankings. |
| `mascot` | character | Team mascot. |
| `abbreviation` | character | Metric abbreviation. |
| `primary_color` | character | Primary team color (hex). |
| `secondary_color` | character | Secondary team color (hex). |
| `is_foreign` | character | Whether the institution is located outside the United States. |
| `site` | integer | FK -> team Site (network site key). |
| `default_asset` | integer | Nested 247Sports image asset for the institution's primary logo (stringified). |
| `alternate_asset` | integer | Nested 247Sports image asset for the institution's alternate logo (stringified). |
| `light_asset` | integer | Nested 247Sports image asset for the light-background logo variant (stringified). |
| `default_name` | character | Server-rendered display label for the entity. |
| `address` | character | Institution's street address. |
| `telephone` | character | Institution's telephone number. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_institution()
```

_Last validated n/a._

## `sports247_site_pages_institution_list`

Institution directory (paginated list).

**Endpoint URL:** `GET https://247sports.com/Institution.json`

**Valid URL:** [https://247sports.com/Institution.json](https://247sports.com/Institution.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `items` | `items` |  |  | `Y` | items query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `name` | character | Position name (e.g. `Quarterback`). |
| `type` | character | Institution type code (college / pro / high school). |
| `group` | character | Institution group (division/level) bitmask code. |
| `location` | integer | FK -> Location (`/Institution/{Location}/Location.json`). |
| `state` | integer | FK -> State entity. |
| `latitude` | character | Venue latitude in decimal degrees. |
| `longitude` | character | Venue longitude in decimal degrees. |
| `rankable` | character | Whether the institution participates in class rankings. |
| `mascot` | character | Team mascot. |
| `abbreviation` | character | Metric abbreviation. |
| `primary_color` | character | Primary team color (hex). |
| `secondary_color` | character | Secondary team color (hex). |
| `is_foreign` | character | Whether the institution is located outside the United States. |
| `site` | integer | FK -> team Site (network site key). |
| `default_asset` | integer | Nested 247Sports image asset for the institution's primary logo (stringified). |
| `alternate_asset` | integer | Nested 247Sports image asset for the institution's alternate logo (stringified). |
| `light_asset` | integer | Nested 247Sports image asset for the light-background logo variant (stringified). |
| `default_name` | character | Server-rendered display label for the entity. |
| `address` | character | Institution's street address. |
| `telephone` | character | Institution's telephone number. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_institution_list()
```

_Last validated n/a._

## `sports247_site_pages_institution_location`

Institution location (city/state/coords/tax).

**Endpoint URL:** `GET https://247sports.com/Institution/{key}/Location.json`

**Valid URL:** [https://247sports.com/Institution/24099/Location.json](https://247sports.com/Institution/24099/Location.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `postal_code` | character | Postal code of the venue. |
| `city` | character | Venue city. |
| `state` | integer | Venue state. |
| `latitude` | character | Venue latitude in decimal degrees. |
| `longitude` | character | Venue longitude in decimal degrees. |
| `county_tax_rate` | character | County income-tax rate for the location, carried on the 247Sports location record. |
| `city_tax_rate` | character | City income-tax rate for the location, carried on the 247Sports location record. |
| `special_tax_rate` | character | Special-district tax rate for the location, carried on the 247Sports location record. |
| `region_name` | character | Name of the region (state/province) for the location. |
| `default_name` | character | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_institution_location(key=24099)
```

_Last validated n/a._

## `sports247_site_pages_institution_timeline_events`

Institution recruiting timeline (site-authored event blurbs).

**Endpoint URL:** `GET https://247sports.com/college/{school_slug}/Institution/{key}/TimelineEvents.json`

**Valid URL:** [https://247sports.com/college/florida/Institution/24099/TimelineEvents.json](https://247sports.com/college/florida/Institution/24099/TimelineEvents.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `school_slug` | `school_slug` |  | `Y` |  | school_slug path parameter. |
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `body` | character | Text body of the timeline entry. |
| `date` | character | Date of the poll release. |
| `author_first_name` | character | First name of the entry's author. |
| `author_last_name` | character | Last name of the entry's author. |
| `author_affiliation` | character | Outlet or site the author writes for, per 247Sports. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_institution_timeline_events(key=24099, school_slug='florida')
```

_Last validated n/a._

## `sports247_site_pages_league_draft_picks`

Pro-draft picks embed for a league/year/round.

**Endpoint URL:** `GET https://247sports.com/League/{league_slug}/DraftPicks/ConfigureEmbed/.json`

**Valid URL:** [https://247sports.com/League/NFL/DraftPicks/ConfigureEmbed/.json](https://247sports.com/League/NFL/DraftPicks/ConfigureEmbed/.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `round` | `round` |  |  | `Y` | round query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `pro_team` | integer | Nested 247Sports record for the professional team that made the pick (stringified). |
| `pro_team_name` | character | Name of the professional team that made the pick. |
| `year` | character | Four-digit season year (e.g. 2019). |
| `round` | character | Draft round number (1-based) the pick belongs to. |
| `pick` | character | Pick number within the round. |
| `overall_pick` | character | Overall selection number in the draft. |
| `player` | integer | Player name. |
| `player_first_name` | character | Player's first name |
| `player_last_name` | character | Player's last name |
| `college_team` | integer | College team name. |
| `college_team_name` | character | Name of the college the player was drafted out of. |
| `position_abbreviation` | character | Player's position at draft. |
| `traded_from_team` | character | Team the pick was traded from, when it changed hands. |
| `pick_type` | character | Type of the selection (e.g. regular, compensatory, supplemental). |
| `league` | integer | League slug. |
| `mock` | character | Whether this is a mock-draft projection vs an actual pick. |
| `default_name` | character | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_league_draft_picks(league_slug='NFL')
```

_Last validated n/a._

## `sports247_site_pages_league_institutions`

Institutions belonging to a league.

**Endpoint URL:** `GET https://247sports.com/League/{league_id}/Institutions.json`

**Valid URL:** [https://247sports.com/League/6/Institutions.json](https://247sports.com/League/6/Institutions.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | league_id path parameter. |
| `items` | `items` |  |  | `Y` | items query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `name` | character | Position name (e.g. `Quarterback`). |
| `type` | character | Institution type code (college / pro / high school). |
| `group` | character | Institution group (division/level) bitmask code. |
| `location` | integer | FK -> Location (`/Institution/{Location}/Location.json`). |
| `state` | integer | FK -> State entity. |
| `latitude` | character | Venue latitude in decimal degrees. |
| `longitude` | character | Venue longitude in decimal degrees. |
| `rankable` | character | Whether the institution participates in class rankings. |
| `mascot` | character | Team mascot. |
| `abbreviation` | character | Metric abbreviation. |
| `primary_color` | character | Primary team color (hex). |
| `secondary_color` | character | Secondary team color (hex). |
| `is_foreign` | character | Whether the institution is located outside the United States. |
| `site` | integer | FK -> team Site (network site key). |
| `default_asset` | integer | Nested 247Sports image asset for the institution's primary logo (stringified). |
| `alternate_asset` | integer | Nested 247Sports image asset for the institution's alternate logo (stringified). |
| `light_asset` | integer | Nested 247Sports image asset for the light-background logo variant (stringified). |
| `default_name` | character | Server-rendered display label for the entity. |
| `address` | character | Institution's street address. |
| `telephone` | character | Institution's telephone number. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_league_institutions(league_id=6)
```

_Last validated n/a._

## `sports247_site_pages_page_feeds`

News/headline feed items for a site Page.

**Endpoint URL:** `GET https://247sports.com/Page/{page_id}/Feeds.json`

**Valid URL:** [https://247sports.com/Page/100134/Feeds.json](https://247sports.com/Page/100134/Feeds.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `page_id` | `page_id` |  | `Y` |  | page_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `uid` | character | ESPN global unique identifier. |
| `update_date` | character | Date the feed item was published or updated. |
| `title_text` | character | Headline text of the feed item. |
| `main_text` | character | Body text of the feed item. |
| `redirection_url` | character | URL the feed item links out to. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_page_feeds(page_id=100134)
```

_Last validated n/a._

## `sports247_site_pages_player`

Player detail (identity + primary-sport rating/ranks).

**Endpoint URL:** `GET https://247sports.com/Player/{key}.json`

**Valid URL:** [https://247sports.com/Player](https://247sports.com/Player)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `first_name` | character | Athlete first name. |
| `last_name` | character | Athlete last name. |
| `full_name` | character | Venue full name (e.g. `Tenney Stadium`). |
| `height` | character | Listed height (inches). |
| `weight` | numeric | Listed weight (lbs). |
| `bio` | character | Player biography text authored on 247Sports. |
| `scout_evaluation` | character | 247Sports scouting evaluation text for the player. |
| `birthdate` | character | Birthdate |
| `modified_user` | character | 247Sports user who last modified the player record. |
| `modified_date` | character | Date the player record was last modified. |
| `cbs_key` | integer | Cross-reference key into the CBS Sports id space. |
| `url` | character | RotoWire player page URL. |
| `last_recruitment_player_institution` | integer | Nested player-institution record from the player's most recent recruitment (stringified). |
| `current_player_institution` | integer | FK -> PlayerInstitution (current school). |
| `twitter_contact` | integer | Player's Twitter/X handle on the 247Sports profile. |
| `mobile_phone_contact` | character | Player's mobile phone contact field on the 247Sports record. |
| `primary_player_sport` | integer | FK -> PlayerSport (`/PlayerSport/{id}.json`). |
| `primary_recruitment` | integer | Nested 247Sports record for the player's primary recruitment (stringified). |
| `default_name` | character | Server-rendered display label for the entity. |
| `default_asset` | integer | Nested 247Sports image asset for the player's headshot (stringified). |
| `default_asset_url` | character | URL of the player's headshot image. |
| `hero_asset` | character | Nested 247Sports hero (banner) image asset for the player page (stringified). |
| `quote_asset` | character | Nested 247Sports image asset used alongside the player's quote block (stringified). |
| `user` | character | 247Sports user account linked to the player profile (nested, stringified). |
| `pro_stat_player` | integer | Reference tying the profile to a professional stats player record (247Sports field). |
| `college_stat_player` | integer | Reference tying the profile to a college stats player record (247Sports field). |
| `bio_or_default` | character | Player bio text, falling back to a default blurb when none is authored. |
| `rating` | integer | 247Sports numeric rating (0-1 scale) for the primary sport. |
| `star_rating` | integer | Star tier (2-5) derived from the rating. |
| `national_rank` | integer | Overall national rank in the recruit's class. |
| `position_rank` | integer | Rank within position for the class. |
| `state_rank` | integer | Rank within home state for the class. |
| `hometown_state` | integer | Recruit hometown state. |
| `hometown_city` | character | Recruit hometown city. |
| `player_high_school_name` | character | Name of the player's high school. |
| `primary_player_position_abbreviation` | character | Abbreviation of the player's primary position. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_player()
```

_Last validated n/a._

## `sports247_site_pages_player_current_institution`

Player's current PlayerInstitution (committed/enrolled school).

**Endpoint URL:** `GET https://247sports.com/Player/{key}/CurrentPlayerInstitution.json`

**Valid URL:** [https://247sports.com/Player/46083769/CurrentPlayerInstitution.json](https://247sports.com/Player/46083769/CurrentPlayerInstitution.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player` | integer | Player name. |
| `institution` | integer | Nested 247Sports institution for the stint (stringified). |
| `state` | integer | Venue state. |
| `agent` | character | Listed player agent. |
| `end_year` | character | Span ending year. |
| `end_date` | character | Season end timestamp (ISO 8601, UTC). |
| `early_enrollee` | character | Whether the player enrolled early at the institution. |
| `early_signee` | character | Whether the player signed in the early signing period. |
| `height` | character | Listed height (inches). |
| `weight` | character | Listed weight (lbs). |
| `transfer_institution` | character | Nested institution involved in the player's transfer, for transfer-portal stints (stringified). |
| `transfer_season` | character | Season of the player's transfer, when applicable. |
| `transfer_eligibility` | character | Player's eligibility status for the transfer, per 247Sports. |
| `created_date` | character | Date the player-institution record was created. |
| `modified_date` | character | Date the player-institution record was last modified. |
| `lead_expert` | integer | 247Sports expert assigned as the lead on the recruitment (nested, stringified). |
| `player_institution_evaluation` | integer | Nested 247Sports evaluation attached to this player-institution stint (stringified). |
| `primary_player_sport` | integer | Nested 247Sports player-sport profile the stint belongs to (stringified). |
| `default_asset` | integer | Nested 247Sports image asset for the stint (stringified). |
| `hero_asset` | character | Nested 247Sports hero (banner) image asset for the stint (stringified). |
| `primary_recruitment` | integer | Nested 247Sports record for the recruitment behind the stint (stringified). |
| `default_name` | character | Server-rendered display label for the entity. |
| `end_year_or_current` | character | Stint's end year, or the current year for an active stint. |
| `start_year_or_expected` | character | Stint's start year, or the expected start year for a future stint. |
| `end_year_or_expected` | character | Stint's end year, or the expected end year for an active stint. |
| `next_institution_type` | character | Level of the player's next institution (e.g. college, professional), per 247Sports. |
| `next_institution_group` | character | Grouping (e.g. conference/division) of the player's next institution, per 247Sports. |
| `start_year` | character | Span starting year. |
| `start_date` | character | Season start timestamp (ISO 8601, UTC). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_player_current_institution(key=46083769)
```

_Last validated n/a._

## `sports247_site_pages_player_high_school`

Player's high-school PlayerInstitution row.

**Endpoint URL:** `GET https://247sports.com/Player/{key}/PlayerHighSchool.json`

**Valid URL:** [https://247sports.com/Player/46051367/PlayerHighSchool.json](https://247sports.com/Player/46051367/PlayerHighSchool.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player` | integer | Player name. |
| `institution` | integer | Nested 247Sports institution for the stint (stringified). |
| `state` | integer | Venue state. |
| `agent` | character | Listed player agent. |
| `end_year` | character | Span ending year. |
| `end_date` | character | Season end timestamp (ISO 8601, UTC). |
| `early_enrollee` | character | Whether the player enrolled early at the institution. |
| `early_signee` | character | Whether the player signed in the early signing period. |
| `height` | character | Listed height (inches). |
| `weight` | character | Listed weight (lbs). |
| `transfer_institution` | character | Nested institution involved in the player's transfer, for transfer-portal stints (stringified). |
| `transfer_season` | character | Season of the player's transfer, when applicable. |
| `transfer_eligibility` | character | Player's eligibility status for the transfer, per 247Sports. |
| `created_date` | character | Date the player-institution record was created. |
| `modified_date` | character | Date the player-institution record was last modified. |
| `lead_expert` | integer | 247Sports expert assigned as the lead on the recruitment (nested, stringified). |
| `player_institution_evaluation` | integer | Nested 247Sports evaluation attached to this player-institution stint (stringified). |
| `primary_player_sport` | integer | Nested 247Sports player-sport profile the stint belongs to (stringified). |
| `default_asset` | integer | Nested 247Sports image asset for the stint (stringified). |
| `hero_asset` | character | Nested 247Sports hero (banner) image asset for the stint (stringified). |
| `primary_recruitment` | integer | Nested 247Sports record for the recruitment behind the stint (stringified). |
| `default_name` | character | Server-rendered display label for the entity. |
| `end_year_or_current` | character | Stint's end year, or the current year for an active stint. |
| `start_year_or_expected` | character | Stint's start year, or the expected start year for a future stint. |
| `end_year_or_expected` | character | Stint's end year, or the expected end year for an active stint. |
| `next_institution_type` | character | Level of the player's next institution (e.g. college, professional), per 247Sports. |
| `next_institution_group` | character | Grouping (e.g. conference/division) of the player's next institution, per 247Sports. |
| `start_year` | character | Span starting year. |
| `start_date` | character | Season start timestamp (ISO 8601, UTC). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_player_high_school(key=46051367)
```

_Last validated n/a._

## `sports247_site_pages_player_institution`

Player-at-institution association detail.

**Endpoint URL:** `GET https://247sports.com/PlayerInstitution/{key}.json`

**Valid URL:** [https://247sports.com/PlayerInstitution](https://247sports.com/PlayerInstitution)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player` | integer | Player name. |
| `institution` | integer | Nested 247Sports institution for the stint (stringified). |
| `state` | integer | Venue state. |
| `agent` | character | Listed player agent. |
| `end_year` | character | Span ending year. |
| `end_date` | character | Season end timestamp (ISO 8601, UTC). |
| `early_enrollee` | character | Whether the player enrolled early at the institution. |
| `early_signee` | character | Whether the player signed in the early signing period. |
| `height` | character | Listed height (inches). |
| `weight` | character | Listed weight (lbs). |
| `transfer_institution` | character | Nested institution involved in the player's transfer, for transfer-portal stints (stringified). |
| `transfer_season` | character | Season of the player's transfer, when applicable. |
| `transfer_eligibility` | character | Player's eligibility status for the transfer, per 247Sports. |
| `created_date` | character | Date the player-institution record was created. |
| `modified_date` | character | Date the player-institution record was last modified. |
| `lead_expert` | integer | 247Sports expert assigned as the lead on the recruitment (nested, stringified). |
| `player_institution_evaluation` | integer | Nested 247Sports evaluation attached to this player-institution stint (stringified). |
| `primary_player_sport` | integer | Nested 247Sports player-sport profile the stint belongs to (stringified). |
| `default_asset` | integer | Nested 247Sports image asset for the stint (stringified). |
| `hero_asset` | character | Nested 247Sports hero (banner) image asset for the stint (stringified). |
| `primary_recruitment` | integer | Nested 247Sports record for the recruitment behind the stint (stringified). |
| `default_name` | character | Server-rendered display label for the entity. |
| `end_year_or_current` | character | Stint's end year, or the current year for an active stint. |
| `start_year_or_expected` | character | Stint's start year, or the expected start year for a future stint. |
| `end_year_or_expected` | character | Stint's end year, or the expected end year for an active stint. |
| `next_institution_type` | character | Level of the player's next institution (e.g. college, professional), per 247Sports. |
| `next_institution_group` | character | Grouping (e.g. conference/division) of the player's next institution, per 247Sports. |
| `start_year` | character | Span starting year. |
| `start_date` | character | Season start timestamp (ISO 8601, UTC). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_player_institution()
```

_Last validated n/a._

## `sports247_site_pages_player_institution_evaluation`

Scout evaluation of a player-institution fit.

**Endpoint URL:** `GET https://247sports.com/PlayerInstitutionEvaluation/{key}.json`

**Valid URL:** [https://247sports.com/PlayerInstitutionEvaluation](https://247sports.com/PlayerInstitutionEvaluation)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player_institution` | integer | Nested player-institution stint the evaluation is attached to (stringified). |
| `user` | integer | 247Sports user account of the evaluator (nested, stringified). |
| `evaluated_date` | character | Date the evaluation was written. |
| `comparison_player` | integer | Established player the evaluator compares the prospect to. |
| `projection` | character | Evaluator's projection for the player (e.g. draft round or college level). |
| `primary` | character | Whether this is the primary (featured) evaluation for the stint. |
| `scout_evaluation` | character | Full text of the 247Sports scouting evaluation. |
| `event` | character | Binary flag indicating the row is a counted game event (excludes end markers). |
| `default_name` | character | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_player_institution_evaluation()
```

_Last validated n/a._

## `sports247_site_pages_player_primary_sport`

Player's primary PlayerSport (rating/class/positions).

**Endpoint URL:** `GET https://247sports.com/Player/{key}/PrimaryPlayerSport.json`

**Valid URL:** [https://247sports.com/Player/46051367/PrimaryPlayerSport.json](https://247sports.com/Player/46051367/PrimaryPlayerSport.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player` | integer | Player name. |
| `player_institution` | integer | Nested player-institution stint the player-sport profile points to (stringified). |
| `state` | integer | Venue state. |
| `sport` | integer | Nested 247Sports sport for the profile (stringified). |
| `rating` | character | 247Sports rating string (0-1). |
| `rating_or_default` | character | 247Sports in-house rating, falling back to a default value when unrated. |
| `local_index` | character | 247Sports' own industry-index value for the player, alongside the Rivals and ESPN indexes. |
| `rivals_grade` | character | Rivals source grade (industry composite input). |
| `rivals_rank` | character | Player's rank in the Rivals industry ranking, as tracked by 247Sports. |
| `rivals_index` | character | Rivals index value for the player, as tracked by 247Sports. |
| `espn_grade` | character | ESPN source grade (industry composite input). |
| `espn_rank` | character | Player's rank in the ESPN industry ranking, as tracked by 247Sports. |
| `espn_index` | character | ESPN index value for the player, as tracked by 247Sports. |
| `composite_strength` | character | Composite strength points (team-ranking weight). |
| `composite_rating` | character | 247Sports Composite rating (industry blend). |
| `composite_rating_or_default` | character | 247Sports Composite rating, falling back to a default value when unrated. |
| `average_rank` | character | Player's average rank across the tracked industry services. |
| `previous_recruitment` | integer | Nested record for the player's previous recruitment (stringified). |
| `primary` | character | Whether this is the player's primary sport. |
| `class_year_override` | character | Override of the player's recruiting class year, when 247Sports reassigns it. |
| `class_year` | character | Recruiting class year. |
| `recruitment` | integer | FK -> Recruitment aggregate for this player-sport. |
| `primary_institution_prediction` | integer | Nested leading Crystal Ball institution prediction for the player (stringified). |
| `secondary_institution_prediction` | integer | Nested second-place Crystal Ball institution prediction (stringified). |
| `primary_institution_prediction_percentage` | character | Share of Crystal Ball predictions favoring the leading institution. |
| `show_unranked_rating` | character | 247Sports display flag to show the rating even while the player is unranked. |
| `current_player_sport_year` | integer | Current ranking-cycle year for the player-sport profile. |
| `unpublished_player_sport_ranking` | integer | Nested not-yet-published ranking row for the player (stringified). |
| `current_player_sport_ranking` | integer | Nested current published ranking row for the player (stringified). |
| `primary_player_position` | integer | Nested 247Sports record for the player's primary position (stringified). |
| `primary_position` | integer | Player's primary position on the 247Sports profile. |
| `primary_position_group` | integer | Position group the player's primary position belongs to. |
| `default_name` | character | Server-rendered display label for the entity. |
| `star_rating` | character | Star tier (2-5). |
| `secondary_institution_prediction_percentage` | character | Share of Crystal Ball predictions favoring the second-place institution. |
| `jersey` | character | Jersey number. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_player_primary_sport(key=46051367)
```

_Last validated n/a._

## `sports247_site_pages_player_search`

Player name search.

**Endpoint URL:** `GET https://247sports.com/Player.json`

**Valid URL:** [https://247sports.com/Player.json](https://247sports.com/Player.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `FirstName` | `first_name` |  |  | `Y` | FirstName query parameter. |
| `LastName` | `last_name` |  |  | `Y` | LastName query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `first_name` | character | Athlete first name. |
| `last_name` | character | Athlete last name. |
| `full_name` | character | Venue full name (e.g. `Tenney Stadium`). |
| `height` | character | Listed height (inches). |
| `weight` | numeric | Listed weight (lbs). |
| `bio` | character | Player biography text authored on 247Sports. |
| `scout_evaluation` | character | 247Sports scouting evaluation text for the player. |
| `birthdate` | character | Birthdate |
| `modified_user` | character | 247Sports user who last modified the player record. |
| `modified_date` | character | Date the player record was last modified. |
| `cbs_key` | integer | Cross-reference key into the CBS Sports id space. |
| `url` | character | RotoWire player page URL. |
| `last_recruitment_player_institution` | integer | Nested player-institution record from the player's most recent recruitment (stringified). |
| `current_player_institution` | integer | FK -> PlayerInstitution (current school). |
| `twitter_contact` | integer | Player's Twitter/X handle on the 247Sports profile. |
| `mobile_phone_contact` | character | Player's mobile phone contact field on the 247Sports record. |
| `primary_player_sport` | integer | FK -> PlayerSport (`/PlayerSport/{id}.json`). |
| `primary_recruitment` | integer | Nested 247Sports record for the player's primary recruitment (stringified). |
| `default_name` | character | Server-rendered display label for the entity. |
| `default_asset` | integer | Nested 247Sports image asset for the player's headshot (stringified). |
| `default_asset_url` | character | URL of the player's headshot image. |
| `hero_asset` | character | Nested 247Sports hero (banner) image asset for the player page (stringified). |
| `quote_asset` | character | Nested 247Sports image asset used alongside the player's quote block (stringified). |
| `user` | character | 247Sports user account linked to the player profile (nested, stringified). |
| `pro_stat_player` | integer | Reference tying the profile to a professional stats player record (247Sports field). |
| `college_stat_player` | integer | Reference tying the profile to a college stats player record (247Sports field). |
| `bio_or_default` | character | Player bio text, falling back to a default blurb when none is authored. |
| `rating` | integer | 247Sports numeric rating (0-1 scale) for the primary sport. |
| `star_rating` | integer | Star tier (2-5) derived from the rating. |
| `national_rank` | integer | Overall national rank in the recruit's class. |
| `position_rank` | integer | Rank within position for the class. |
| `state_rank` | integer | Rank within home state for the class. |
| `hometown_state` | integer | Recruit hometown state. |
| `hometown_city` | character | Recruit hometown city. |
| `player_high_school_name` | character | Name of the player's high school. |
| `primary_player_position_abbreviation` | character | Abbreviation of the player's primary position. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_player_search()
```

_Last validated n/a._

## `sports247_site_pages_playersport`

PlayerSport detail (note lowercase route segment).

**Endpoint URL:** `GET https://247sports.com/playersport/{key}.json`

**Valid URL:** [https://247sports.com/playersport](https://247sports.com/playersport)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player` | integer | Player name. |
| `player_institution` | integer | Nested player-institution stint the player-sport profile points to (stringified). |
| `state` | integer | Venue state. |
| `sport` | integer | Nested 247Sports sport for the profile (stringified). |
| `rating` | character | 247Sports rating string (0-1). |
| `rating_or_default` | character | 247Sports in-house rating, falling back to a default value when unrated. |
| `local_index` | character | 247Sports' own industry-index value for the player, alongside the Rivals and ESPN indexes. |
| `rivals_grade` | character | Rivals source grade (industry composite input). |
| `rivals_rank` | character | Player's rank in the Rivals industry ranking, as tracked by 247Sports. |
| `rivals_index` | character | Rivals index value for the player, as tracked by 247Sports. |
| `espn_grade` | character | ESPN source grade (industry composite input). |
| `espn_rank` | character | Player's rank in the ESPN industry ranking, as tracked by 247Sports. |
| `espn_index` | character | ESPN index value for the player, as tracked by 247Sports. |
| `composite_strength` | character | Composite strength points (team-ranking weight). |
| `composite_rating` | character | 247Sports Composite rating (industry blend). |
| `composite_rating_or_default` | character | 247Sports Composite rating, falling back to a default value when unrated. |
| `average_rank` | character | Player's average rank across the tracked industry services. |
| `previous_recruitment` | integer | Nested record for the player's previous recruitment (stringified). |
| `primary` | character | Whether this is the player's primary sport. |
| `class_year_override` | character | Override of the player's recruiting class year, when 247Sports reassigns it. |
| `class_year` | character | Recruiting class year. |
| `recruitment` | integer | FK -> Recruitment aggregate for this player-sport. |
| `primary_institution_prediction` | integer | Nested leading Crystal Ball institution prediction for the player (stringified). |
| `secondary_institution_prediction` | integer | Nested second-place Crystal Ball institution prediction (stringified). |
| `primary_institution_prediction_percentage` | character | Share of Crystal Ball predictions favoring the leading institution. |
| `show_unranked_rating` | character | 247Sports display flag to show the rating even while the player is unranked. |
| `current_player_sport_year` | integer | Current ranking-cycle year for the player-sport profile. |
| `unpublished_player_sport_ranking` | integer | Nested not-yet-published ranking row for the player (stringified). |
| `current_player_sport_ranking` | integer | Nested current published ranking row for the player (stringified). |
| `primary_player_position` | integer | Nested 247Sports record for the player's primary position (stringified). |
| `primary_position` | integer | Player's primary position on the 247Sports profile. |
| `primary_position_group` | integer | Position group the player's primary position belongs to. |
| `default_name` | character | Server-rendered display label for the entity. |
| `star_rating` | character | Star tier (2-5). |
| `secondary_institution_prediction_percentage` | character | Share of Crystal Ball predictions favoring the second-place institution. |
| `jersey` | character | Jersey number. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_playersport()
```

_Last validated n/a._

## `sports247_site_pages_playersport_institution`

PlayerInstitution linked to a PlayerSport.

**Endpoint URL:** `GET https://247sports.com/PlayerSport/{key}/PlayerInstitution.json`

**Valid URL:** [https://247sports.com/PlayerSport/279200/PlayerInstitution.json](https://247sports.com/PlayerSport/279200/PlayerInstitution.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player` | integer | Player name. |
| `institution` | integer | Nested 247Sports institution for the stint (stringified). |
| `state` | integer | Venue state. |
| `agent` | character | Listed player agent. |
| `end_year` | character | Span ending year. |
| `end_date` | character | Season end timestamp (ISO 8601, UTC). |
| `early_enrollee` | character | Whether the player enrolled early at the institution. |
| `early_signee` | character | Whether the player signed in the early signing period. |
| `height` | character | Listed height (inches). |
| `weight` | character | Listed weight (lbs). |
| `transfer_institution` | character | Nested institution involved in the player's transfer, for transfer-portal stints (stringified). |
| `transfer_season` | character | Season of the player's transfer, when applicable. |
| `transfer_eligibility` | character | Player's eligibility status for the transfer, per 247Sports. |
| `created_date` | character | Date the player-institution record was created. |
| `modified_date` | character | Date the player-institution record was last modified. |
| `lead_expert` | integer | 247Sports expert assigned as the lead on the recruitment (nested, stringified). |
| `player_institution_evaluation` | integer | Nested 247Sports evaluation attached to this player-institution stint (stringified). |
| `primary_player_sport` | integer | Nested 247Sports player-sport profile the stint belongs to (stringified). |
| `default_asset` | integer | Nested 247Sports image asset for the stint (stringified). |
| `hero_asset` | character | Nested 247Sports hero (banner) image asset for the stint (stringified). |
| `primary_recruitment` | integer | Nested 247Sports record for the recruitment behind the stint (stringified). |
| `default_name` | character | Server-rendered display label for the entity. |
| `end_year_or_current` | character | Stint's end year, or the current year for an active stint. |
| `start_year_or_expected` | character | Stint's start year, or the expected start year for a future stint. |
| `end_year_or_expected` | character | Stint's end year, or the expected end year for an active stint. |
| `next_institution_type` | character | Level of the player's next institution (e.g. college, professional), per 247Sports. |
| `next_institution_group` | character | Grouping (e.g. conference/division) of the player's next institution, per 247Sports. |
| `start_year` | character | Span starting year. |
| `start_date` | character | Season start timestamp (ISO 8601, UTC). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_playersport_institution(key=279200)
```

_Last validated n/a._

## `sports247_site_pages_playersport_rank_history`

Ranking history for a PlayerSport (one row per Ranking snapshot).

**Endpoint URL:** `GET https://247sports.com/PlayerSport/{key}/RecruitRankHistory.json`

**Valid URL:** [https://247sports.com/PlayerSport/250563/RecruitRankHistory.json](https://247sports.com/PlayerSport/250563/RecruitRankHistory.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `ranking` | integer | FK -> Ranking snapshot. |
| `sport` | integer | Nested 247Sports sport the ranking row covers (stringified). |
| `player_sport` | integer | Nested player-sport profile the ranking row belongs to (stringified). |
| `committed_institution` | integer | FK -> committed Institution (null if uncommitted). |
| `order` | character | Team order within the competition (0 = first). |
| `position` | integer | Athlete position. |
| `position_group` | integer | Position group of the recruits (e.g. Offensive Line, Defensive Back). |
| `platoon` | integer | 247Sports platoon (side-of-ball grouping) identifier on the ranking row. |
| `state` | integer | Venue state. |
| `region` | integer | Broadcast region code. |
| `institution` | integer | Nested institution the player was committed or signed to at ranking time (stringified). |
| `institution_group` | character | Grouping (e.g. conference/division) of the player's institution, per 247Sports. |
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `composite_strength` | character | 247Sports field describing the strength of the industry inputs behind the Composite rating. |
| `composite_rating` | character | Player's 247Sports Composite rating, blending the major services' ratings. |
| `overall_rank` | character | Overall national rank in the snapshot. |
| `composite_overall_rank` | character | Player's national rank by 247Sports Composite rating. |
| `group_rank` | character | League/season rank for group. |
| `composite_group_rank` | character | Player's rank within their position group by Composite rating. |
| `position_rank` | character | Rank within position. |
| `previous_player_sport_ranking` | integer | Nested prior-cycle ranking row for the player (stringified). |
| `composite_position_rank` | character | Player's rank at their position by Composite rating. |
| `state_rank` | character | Rank within home state. |
| `composite_state_rank` | character | Player's rank within their home state by Composite rating. |
| `default_name` | character | Server-rendered display label for the entity. |
| `position_group_rank` | character | Player's rank within their position group in the 247Sports ranking. |
| `region_rank` | character | Region ranking. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_playersport_rank_history(key=250563)
```

_Last validated n/a._

## `sports247_site_pages_position_rankings`

Player-sport rankings for a position.

**Endpoint URL:** `GET https://247sports.com/Position/{key}/playersportrankings.json`

**Valid URL:** [https://247sports.com/Position/14/playersportrankings.json](https://247sports.com/Position/14/playersportrankings.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `ranking` | integer | FK -> Ranking snapshot. |
| `sport` | integer | Nested 247Sports sport the ranking row covers (stringified). |
| `player_sport` | integer | Nested player-sport profile the ranking row belongs to (stringified). |
| `committed_institution` | integer | FK -> committed Institution (null if uncommitted). |
| `order` | character | Team order within the competition (0 = first). |
| `position` | integer | Athlete position. |
| `position_group` | integer | Position group of the recruits (e.g. Offensive Line, Defensive Back). |
| `platoon` | integer | 247Sports platoon (side-of-ball grouping) identifier on the ranking row. |
| `state` | integer | Venue state. |
| `region` | integer | Broadcast region code. |
| `institution` | integer | Nested institution the player was committed or signed to at ranking time (stringified). |
| `institution_group` | character | Grouping (e.g. conference/division) of the player's institution, per 247Sports. |
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `composite_strength` | character | 247Sports field describing the strength of the industry inputs behind the Composite rating. |
| `composite_rating` | character | Player's 247Sports Composite rating, blending the major services' ratings. |
| `overall_rank` | character | Overall national rank in the snapshot. |
| `composite_overall_rank` | character | Player's national rank by 247Sports Composite rating. |
| `group_rank` | character | League/season rank for group. |
| `composite_group_rank` | character | Player's rank within their position group by Composite rating. |
| `position_rank` | character | Rank within position. |
| `previous_player_sport_ranking` | integer | Nested prior-cycle ranking row for the player (stringified). |
| `composite_position_rank` | character | Player's rank at their position by Composite rating. |
| `state_rank` | character | Rank within home state. |
| `composite_state_rank` | character | Player's rank within their home state by Composite rating. |
| `default_name` | character | Server-rendered display label for the entity. |
| `position_group_rank` | character | Player's rank within their position group in the 247Sports ranking. |
| `region_rank` | character | Region ranking. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_position_rankings(key=14)
```

_Last validated n/a._

## `sports247_site_pages_recruit_interest`

Single recruit-interest (school<->recruit link) detail.

**Endpoint URL:** `GET https://247sports.com/RecruitInterest/{key}.json`

**Valid URL:** [https://247sports.com/RecruitInterest](https://247sports.com/RecruitInterest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `recruitment` | integer | FK -> parent Recruitment. |
| `player_sport` | integer | Nested player-sport profile the interest entry belongs to (stringified). |
| `recruit_state` | integer | 247Sports status of the recruit's interest entry for the school (e.g. committed, signed, decommitted). |
| `institution` | integer | FK -> the interested/interesting Institution. |
| `lock_prediction` | character | Crystal Ball lock-prediction value for the school on this recruitment (247Sports field). |
| `recruits_interest` | character | Recruit's stated interest level in the school, per 247Sports. |
| `primary_coach` | integer | Lead recruiting coach at the school for this recruit. |
| `secondary_coach` | character | Secondary recruiting coach at the school for this recruit. |
| `keeper_coach` | character | Coach designated as the keeper contact for the recruitment (247Sports field). |
| `institutions_interest` | character | School's interest level in the recruit, per 247Sports. |
| `position` | integer | Athlete position. |
| `position_group` | integer | Position group of the recruits (e.g. Offensive Line, Defensive Back). |
| `platoon` | integer | 247Sports platoon (side-of-ball grouping) identifier on the interest entry. |
| `offered` | character | Whether the school has extended an offer. |
| `gray_shirt` | character | Whether the offer or commitment is a grayshirt (delayed enrollment) arrangement. |
| `walk_on` | character | Whether the recruit would join the program as a walk-on. |
| `official_visit` | integer | Date of the recruit's official visit to the school. |
| `second_official_visit` | character | Date of the recruit's second official visit to the school. |
| `soft_commit` | character | Whether 247Sports marks the commitment as a soft commit. |
| `hard_commit` | integer | FK -> the RecruitInterestEvent marking a hard commit. |
| `signing_date` | integer | Date the recruit signed with the school. |
| `enrollment_date` | integer | Date the recruit enrolled at the school. |
| `decommit` | character | Date the recruit decommitted from the school, when applicable. |
| `offer` | character | Whether the school has extended a scholarship offer to the recruit. |
| `highest_recruit_interest_event` | integer | Nested highest-signal event on the interest timeline (e.g. commitment) (stringified). |
| `commit_status` | character | Commitment status label (e.g. Committed, Signed). |
| `default_name` | character | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_recruit_interest()
```

_Last validated n/a._

## `sports247_site_pages_recruitment_final_choice`

Final-choice PlayerSport/commit for a recruitment.

**Endpoint URL:** `GET https://247sports.com/Recruitment/{key}/FinalChoice.json`

**Valid URL:** [https://247sports.com/Recruitment/114978/FinalChoice.json](https://247sports.com/Recruitment/114978/FinalChoice.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player` | integer | Player name. |
| `player_institution` | integer | Nested player-institution stint the player-sport profile points to (stringified). |
| `state` | integer | Venue state. |
| `sport` | integer | Nested 247Sports sport for the profile (stringified). |
| `rating` | character | 247Sports rating string (0-1). |
| `rating_or_default` | character | 247Sports in-house rating, falling back to a default value when unrated. |
| `local_index` | character | 247Sports' own industry-index value for the player, alongside the Rivals and ESPN indexes. |
| `rivals_grade` | character | Rivals source grade (industry composite input). |
| `rivals_rank` | character | Player's rank in the Rivals industry ranking, as tracked by 247Sports. |
| `rivals_index` | character | Rivals index value for the player, as tracked by 247Sports. |
| `espn_grade` | character | ESPN source grade (industry composite input). |
| `espn_rank` | character | Player's rank in the ESPN industry ranking, as tracked by 247Sports. |
| `espn_index` | character | ESPN index value for the player, as tracked by 247Sports. |
| `composite_strength` | character | Composite strength points (team-ranking weight). |
| `composite_rating` | character | 247Sports Composite rating (industry blend). |
| `composite_rating_or_default` | character | 247Sports Composite rating, falling back to a default value when unrated. |
| `average_rank` | character | Player's average rank across the tracked industry services. |
| `previous_recruitment` | integer | Nested record for the player's previous recruitment (stringified). |
| `primary` | character | Whether this is the player's primary sport. |
| `class_year_override` | character | Override of the player's recruiting class year, when 247Sports reassigns it. |
| `class_year` | character | Recruiting class year. |
| `recruitment` | integer | FK -> Recruitment aggregate for this player-sport. |
| `primary_institution_prediction` | integer | Nested leading Crystal Ball institution prediction for the player (stringified). |
| `secondary_institution_prediction` | integer | Nested second-place Crystal Ball institution prediction (stringified). |
| `primary_institution_prediction_percentage` | character | Share of Crystal Ball predictions favoring the leading institution. |
| `show_unranked_rating` | character | 247Sports display flag to show the rating even while the player is unranked. |
| `current_player_sport_year` | integer | Current ranking-cycle year for the player-sport profile. |
| `unpublished_player_sport_ranking` | integer | Nested not-yet-published ranking row for the player (stringified). |
| `current_player_sport_ranking` | integer | Nested current published ranking row for the player (stringified). |
| `primary_player_position` | integer | Nested 247Sports record for the player's primary position (stringified). |
| `primary_position` | integer | Player's primary position on the 247Sports profile. |
| `primary_position_group` | integer | Position group the player's primary position belongs to. |
| `default_name` | character | Server-rendered display label for the entity. |
| `star_rating` | character | Star tier (2-5). |
| `secondary_institution_prediction_percentage` | character | Share of Crystal Ball predictions favoring the second-place institution. |
| `jersey` | character | Jersey number. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_recruitment_final_choice(key=114978)
```

_Last validated n/a._

## `sports247_site_pages_recruitment_institution`

Committed institution for a recruitment.

**Endpoint URL:** `GET https://247sports.com/Recruitment/{key}/Institution.json`

**Valid URL:** [https://247sports.com/Recruitment/114978/Institution.json](https://247sports.com/Recruitment/114978/Institution.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `name` | character | Position name (e.g. `Quarterback`). |
| `type` | character | Institution type code (college / pro / high school). |
| `group` | character | Institution group (division/level) bitmask code. |
| `location` | integer | FK -> Location (`/Institution/{Location}/Location.json`). |
| `state` | integer | FK -> State entity. |
| `latitude` | character | Venue latitude in decimal degrees. |
| `longitude` | character | Venue longitude in decimal degrees. |
| `rankable` | character | Whether the institution participates in class rankings. |
| `mascot` | character | Team mascot. |
| `abbreviation` | character | Metric abbreviation. |
| `primary_color` | character | Primary team color (hex). |
| `secondary_color` | character | Secondary team color (hex). |
| `is_foreign` | character | Whether the institution is located outside the United States. |
| `site` | integer | FK -> team Site (network site key). |
| `default_asset` | integer | Nested 247Sports image asset for the institution's primary logo (stringified). |
| `alternate_asset` | integer | Nested 247Sports image asset for the institution's alternate logo (stringified). |
| `light_asset` | integer | Nested 247Sports image asset for the light-background logo variant (stringified). |
| `default_name` | character | Server-rendered display label for the entity. |
| `address` | character | Institution's street address. |
| `telephone` | character | Institution's telephone number. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_recruitment_institution(key=114978)
```

_Last validated n/a._

## `sports247_site_pages_recruitment_interests`

All institutions the recruit has interest links with.

**Endpoint URL:** `GET https://247sports.com/Recruitment/{key}/Interests.json`

**Valid URL:** [https://247sports.com/Recruitment/114978/Interests.json](https://247sports.com/Recruitment/114978/Interests.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `name` | character | Position name (e.g. `Quarterback`). |
| `type` | character | Institution type code (college / pro / high school). |
| `group` | character | Institution group (division/level) bitmask code. |
| `location` | integer | FK -> Location (`/Institution/{Location}/Location.json`). |
| `state` | integer | FK -> State entity. |
| `latitude` | character | Venue latitude in decimal degrees. |
| `longitude` | character | Venue longitude in decimal degrees. |
| `rankable` | character | Whether the institution participates in class rankings. |
| `mascot` | character | Team mascot. |
| `abbreviation` | character | Metric abbreviation. |
| `primary_color` | character | Primary team color (hex). |
| `secondary_color` | character | Secondary team color (hex). |
| `is_foreign` | character | Whether the institution is located outside the United States. |
| `site` | integer | FK -> team Site (network site key). |
| `default_asset` | integer | Nested 247Sports image asset for the institution's primary logo (stringified). |
| `alternate_asset` | integer | Nested 247Sports image asset for the institution's alternate logo (stringified). |
| `light_asset` | integer | Nested 247Sports image asset for the light-background logo variant (stringified). |
| `default_name` | character | Server-rendered display label for the entity. |
| `address` | character | Institution's street address. |
| `telephone` | character | Institution's telephone number. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_recruitment_interests(key=114978)
```

_Last validated n/a._

## `sports247_site_pages_recruitment_offers`

Institutions that have offered the recruit.

**Endpoint URL:** `GET https://247sports.com/Recruitment/{key}/Offers.json`

**Valid URL:** [https://247sports.com/Recruitment/114978/Offers.json](https://247sports.com/Recruitment/114978/Offers.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `name` | character | Position name (e.g. `Quarterback`). |
| `type` | character | Institution type code (college / pro / high school). |
| `group` | character | Institution group (division/level) bitmask code. |
| `location` | integer | FK -> Location (`/Institution/{Location}/Location.json`). |
| `state` | integer | FK -> State entity. |
| `latitude` | character | Venue latitude in decimal degrees. |
| `longitude` | character | Venue longitude in decimal degrees. |
| `rankable` | character | Whether the institution participates in class rankings. |
| `mascot` | character | Team mascot. |
| `abbreviation` | character | Metric abbreviation. |
| `primary_color` | character | Primary team color (hex). |
| `secondary_color` | character | Secondary team color (hex). |
| `is_foreign` | character | Whether the institution is located outside the United States. |
| `site` | integer | FK -> team Site (network site key). |
| `default_asset` | integer | Nested 247Sports image asset for the institution's primary logo (stringified). |
| `alternate_asset` | integer | Nested 247Sports image asset for the institution's alternate logo (stringified). |
| `light_asset` | integer | Nested 247Sports image asset for the light-background logo variant (stringified). |
| `default_name` | character | Server-rendered display label for the entity. |
| `address` | character | Institution's street address. |
| `telephone` | character | Institution's telephone number. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_recruitment_offers(key=114978)
```

_Last validated n/a._

## `sports247_site_pages_recruitment_player_sport`

PlayerSport underlying a recruitment.

**Endpoint URL:** `GET https://247sports.com/Recruitment/{key}/PlayerSport.json`

**Valid URL:** [https://247sports.com/Recruitment/114978/PlayerSport.json](https://247sports.com/Recruitment/114978/PlayerSport.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player` | integer | Player name. |
| `player_institution` | integer | Nested player-institution stint the player-sport profile points to (stringified). |
| `state` | integer | Venue state. |
| `sport` | integer | Nested 247Sports sport for the profile (stringified). |
| `rating` | character | 247Sports rating string (0-1). |
| `rating_or_default` | character | 247Sports in-house rating, falling back to a default value when unrated. |
| `local_index` | character | 247Sports' own industry-index value for the player, alongside the Rivals and ESPN indexes. |
| `rivals_grade` | character | Rivals source grade (industry composite input). |
| `rivals_rank` | character | Player's rank in the Rivals industry ranking, as tracked by 247Sports. |
| `rivals_index` | character | Rivals index value for the player, as tracked by 247Sports. |
| `espn_grade` | character | ESPN source grade (industry composite input). |
| `espn_rank` | character | Player's rank in the ESPN industry ranking, as tracked by 247Sports. |
| `espn_index` | character | ESPN index value for the player, as tracked by 247Sports. |
| `composite_strength` | character | Composite strength points (team-ranking weight). |
| `composite_rating` | character | 247Sports Composite rating (industry blend). |
| `composite_rating_or_default` | character | 247Sports Composite rating, falling back to a default value when unrated. |
| `average_rank` | character | Player's average rank across the tracked industry services. |
| `previous_recruitment` | integer | Nested record for the player's previous recruitment (stringified). |
| `primary` | character | Whether this is the player's primary sport. |
| `class_year_override` | character | Override of the player's recruiting class year, when 247Sports reassigns it. |
| `class_year` | character | Recruiting class year. |
| `recruitment` | integer | FK -> Recruitment aggregate for this player-sport. |
| `primary_institution_prediction` | integer | Nested leading Crystal Ball institution prediction for the player (stringified). |
| `secondary_institution_prediction` | integer | Nested second-place Crystal Ball institution prediction (stringified). |
| `primary_institution_prediction_percentage` | character | Share of Crystal Ball predictions favoring the leading institution. |
| `show_unranked_rating` | character | 247Sports display flag to show the rating even while the player is unranked. |
| `current_player_sport_year` | integer | Current ranking-cycle year for the player-sport profile. |
| `unpublished_player_sport_ranking` | integer | Nested not-yet-published ranking row for the player (stringified). |
| `current_player_sport_ranking` | integer | Nested current published ranking row for the player (stringified). |
| `primary_player_position` | integer | Nested 247Sports record for the player's primary position (stringified). |
| `primary_position` | integer | Player's primary position on the 247Sports profile. |
| `primary_position_group` | integer | Position group the player's primary position belongs to. |
| `default_name` | character | Server-rendered display label for the entity. |
| `star_rating` | character | Star tier (2-5). |
| `secondary_institution_prediction_percentage` | character | Share of Crystal Ball predictions favoring the second-place institution. |
| `jersey` | character | Jersey number. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_recruitment_player_sport(key=114978)
```

_Last validated n/a._

## `sports247_site_pages_season_current_expert_predictions`

Current expert 'crystal ball' predictions for a season.

**Endpoint URL:** `GET https://247sports.com/Season/{season}/CurrentExpertPredictions.json`

**Valid URL:** [https://247sports.com/Season/2026-Football/CurrentExpertPredictions.json](https://247sports.com/Season/2026-Football/CurrentExpertPredictions.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season path segment in `{year}-{Sport}` form, e.g. `2026-Football`. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player_institution` | integer | Nested player-institution stint the Crystal Ball prediction targets (stringified). |
| `institution` | integer | FK -> predicted destination Institution. |
| `user` | integer | 247Sports user account of the predictor (nested, stringified). |
| `updated_on` | character | Date the prediction was last updated. |
| `prediction_status` | character | Crystal-ball prediction status code. |
| `days_correct` | character | Number of days the prediction has stood as correct. |
| `premium` | character | Whether the article is premium content. |
| `score` | character | Expert accuracy score at time of prediction. |
| `confidence` | character | Expert confidence 1-10. |
| `parent` | character | Parent prediction record this entry updates (247Sports field). |
| `is_zero_zone` | character | Whether the prediction fell in 247Sports' zero zone (logged too close to the announcement to earn accuracy credit). |
| `default_name` | character | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_season_current_expert_predictions(season='2026-Football')
```

_Last validated n/a._

## `sports247_site_pages_season_recruit_interest_events`

Recruit-interest timeline events for a season (offers/visits/commits).

**Endpoint URL:** `GET https://247sports.com/Season/{season}/RecruitInterestEvents.json`

**Valid URL:** [https://247sports.com/Season/2026-Football/RecruitInterestEvents.json](https://247sports.com/Season/2026-Football/RecruitInterestEvents.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season path segment in `{year}-{Sport}` form, e.g. `2026-Football`. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `institution` | integer | Nested 247Sports institution the interest event involves (stringified). |
| `recruitment` | integer | Nested 247Sports recruitment the event belongs to (stringified). |
| `recruit_interest` | integer | Nested recruit-interest record the event belongs to (stringified). |
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `date` | character | Date of the poll release. |
| `default_name` | character | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_season_recruit_interest_events(season='2026-Football')
```

_Last validated n/a._

## `sports247_site_pages_season_recruit_interests`

All recruit interests for a season (paginated).

**Endpoint URL:** `GET https://247sports.com/Season/{season}/RecruitInterests.json`

**Valid URL:** [https://247sports.com/Season/2026-Football/RecruitInterests.json](https://247sports.com/Season/2026-Football/RecruitInterests.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season path segment in `{year}-{Sport}` form, e.g. `2026-Football`. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `recruitment` | integer | FK -> parent Recruitment. |
| `player_sport` | integer | Nested player-sport profile the interest entry belongs to (stringified). |
| `recruit_state` | integer | 247Sports status of the recruit's interest entry for the school (e.g. committed, signed, decommitted). |
| `institution` | integer | FK -> the interested/interesting Institution. |
| `lock_prediction` | character | Crystal Ball lock-prediction value for the school on this recruitment (247Sports field). |
| `recruits_interest` | character | Recruit's stated interest level in the school, per 247Sports. |
| `primary_coach` | integer | Lead recruiting coach at the school for this recruit. |
| `secondary_coach` | character | Secondary recruiting coach at the school for this recruit. |
| `keeper_coach` | character | Coach designated as the keeper contact for the recruitment (247Sports field). |
| `institutions_interest` | character | School's interest level in the recruit, per 247Sports. |
| `position` | integer | Athlete position. |
| `position_group` | integer | Position group of the recruits (e.g. Offensive Line, Defensive Back). |
| `platoon` | integer | 247Sports platoon (side-of-ball grouping) identifier on the interest entry. |
| `offered` | character | Whether the school has extended an offer. |
| `gray_shirt` | character | Whether the offer or commitment is a grayshirt (delayed enrollment) arrangement. |
| `walk_on` | character | Whether the recruit would join the program as a walk-on. |
| `official_visit` | integer | Date of the recruit's official visit to the school. |
| `second_official_visit` | character | Date of the recruit's second official visit to the school. |
| `soft_commit` | character | Whether 247Sports marks the commitment as a soft commit. |
| `hard_commit` | integer | FK -> the RecruitInterestEvent marking a hard commit. |
| `signing_date` | integer | Date the recruit signed with the school. |
| `enrollment_date` | integer | Date the recruit enrolled at the school. |
| `decommit` | character | Date the recruit decommitted from the school, when applicable. |
| `offer` | character | Whether the school has extended a scholarship offer to the recruit. |
| `highest_recruit_interest_event` | integer | Nested highest-signal event on the interest timeline (e.g. commitment) (stringified). |
| `commit_status` | character | Commitment status label (e.g. Committed, Signed). |
| `default_name` | character | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_season_recruit_interests(season='2026-Football')
```

_Last validated n/a._

## `sports247_site_pages_season_recruits`

Recruit class rankings for a season (rich per-recruit rows with inlined Player).

**Endpoint URL:** `GET https://247sports.com/Season/{season}/Recruits.json`

**Valid URL:** [https://247sports.com/Season/2026-Football/Recruits.json](https://247sports.com/Season/2026-Football/Recruits.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season path segment in `{year}-{Sport}` form, e.g. `2026-Football`. |
| `Items` | `items` |  |  | `Y` | Items query parameter. |
| `Page` | `page` |  |  | `Y` | Page query parameter. |
| `Player.FullName` | `player_full_name` |  |  | `Y` | Player.FullName query parameter. |
| `Institution` | `institution` |  |  | `Y` | Institution query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player_institution` | integer | Nested player-institution stint behind the recruit row (stringified). |
| `year` | integer | Four-digit season year (e.g. 2019). |
| `announcement_date` | character | Date the recruit announced their decision. |
| `signed_institution` | integer | Nested institution the recruit signed with (stringified). |
| `position` | integer | Athlete position. |
| `institution` | integer | Nested institution the recruit row is scoped to (stringified). |
| `state` | integer | Venue state. |
| `player_sport` | integer | Nested player-sport profile for the recruit (stringified). |
| `composite_strength` | character | Composite strength points contributed to team ranking. |
| `final_choice` | integer | Whether this entry represents the recruit's final school choice. |
| `highest_recruit_interest_event_type` | character | Type of the highest-signal event on the recruit's interest timeline (e.g. commit, signing). |
| `highest_recruit_interest_event` | integer | Nested highest-signal event on the recruit's interest timeline (stringified). |
| `committed_recruit_interest` | integer | Nested interest record for the school the recruit committed to (stringified). |
| `committed_institution` | integer | FK -> committed Institution. |
| `highest_recruit_interest` | integer | Nested interest record carrying the recruit's highest interest signal (stringified). |
| `primary_player_position` | integer | Nested 247Sports record for the recruit's primary position (stringified). |
| `primary_position` | integer | Recruit's primary position on the 247Sports profile. |
| `default_name` | character | Server-rendered display label for the entity. |
| `commited_institution_team_image` | character | Team image asset for the committed institution (the 'commited' spelling is 247Sports' own field name). |
| `recruit_interest_count` | integer | Number of tracked school interests. |
| `recruit_interests_url` | character | Site URL to the recruit's interest timeline. |
| `player_key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player_first_name` | character | Player's first name |
| `player_last_name` | character | Player's last name |
| `player_full_name` | character | Player full name. |
| `player_height` | character | Participant height (e.g. "6' 5\""). |
| `player_weight` | numeric | Participant weight in pounds. |
| `player_bio` | character | Player biography text authored on 247Sports. |
| `player_scout_evaluation` | character | 247Sports scouting evaluation text for the player. |
| `player_birthdate` | character | Player's date of birth, per 247Sports. |
| `player_modified_user` | character | 247Sports user who last modified the player record. |
| `player_modified_date` | character | Date the player record was last modified. |
| `player_cbs_key` | integer | Cross-reference key into the CBS Sports id space. |
| `player_url` | character | Full stats.ncaa.org url for the player page. |
| `player_last_recruitment_player_institution` | integer | Nested player-institution record from the player's most recent recruitment (stringified). |
| `player_current_player_institution` | integer | FK -> PlayerInstitution (current school). |
| `player_twitter_contact` | integer | Player's Twitter/X handle on the 247Sports profile. |
| `player_mobile_phone_contact` | character | Player's mobile phone contact field on the 247Sports record. |
| `player_primary_player_sport` | integer | FK -> PlayerSport (`/PlayerSport/{id}.json`). |
| `player_primary_recruitment` | integer | Nested 247Sports record for the player's primary recruitment (stringified). |
| `player_default_name` | character | Server-rendered display label for the entity. |
| `player_default_asset` | integer | Nested 247Sports image asset for the player's headshot (stringified). |
| `player_default_asset_url` | character | URL of the player's headshot image. |
| `player_hero_asset` | character | Nested 247Sports hero (banner) image asset for the player page (stringified). |
| `player_quote_asset` | character | Nested 247Sports image asset used alongside the player's quote block (stringified). |
| `player_user` | character | 247Sports user account linked to the player profile (nested, stringified). |
| `player_pro_stat_player` | integer | Reference tying the profile to a professional stats player record (247Sports field). |
| `player_college_stat_player` | integer | Reference tying the profile to a college stats player record (247Sports field). |
| `player_bio_or_default` | character | Player bio text, falling back to a default blurb when none is authored. |
| `player_rating` | integer | 247Sports numeric rating (0-1 scale) for the primary sport. |
| `player_star_rating` | integer | Star tier (2-5) derived from the rating. |
| `player_national_rank` | integer | Overall national rank in the recruit's class. |
| `player_position_rank` | integer | Rank within position for the class. |
| `player_state_rank` | integer | Rank within home state for the class. |
| `player_hometown_state` | integer | State of the player's hometown. |
| `player_hometown_city` | character | City of the player's hometown. |
| `player_player_high_school_name` | character | Name of the player's high school. |
| `player_primary_player_position_abbreviation` | character | Abbreviation of the player's primary position. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_season_recruits(season='2026-Football')
```

_Last validated n/a._

## `sports247_site_pages_season_roster_embed`

Signed-class roster embed (PlayerSport rows). Accuracy can lag.

**Endpoint URL:** `GET https://247sports.com/Season/{season}/Roster/Embed.json`

**Valid URL:** [https://247sports.com/Season/2020-Football/Roster/Embed.json](https://247sports.com/Season/2020-Football/Roster/Embed.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season path segment in `{year}-{Sport}` form, e.g. `2026-Football`. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player` | integer | Player name. |
| `player_institution` | integer | Nested player-institution stint the player-sport profile points to (stringified). |
| `state` | integer | Venue state. |
| `sport` | integer | Nested 247Sports sport for the profile (stringified). |
| `rating` | character | 247Sports rating string (0-1). |
| `rating_or_default` | character | 247Sports in-house rating, falling back to a default value when unrated. |
| `local_index` | character | 247Sports' own industry-index value for the player, alongside the Rivals and ESPN indexes. |
| `rivals_grade` | character | Rivals source grade (industry composite input). |
| `rivals_rank` | character | Player's rank in the Rivals industry ranking, as tracked by 247Sports. |
| `rivals_index` | character | Rivals index value for the player, as tracked by 247Sports. |
| `espn_grade` | character | ESPN source grade (industry composite input). |
| `espn_rank` | character | Player's rank in the ESPN industry ranking, as tracked by 247Sports. |
| `espn_index` | character | ESPN index value for the player, as tracked by 247Sports. |
| `composite_strength` | character | Composite strength points (team-ranking weight). |
| `composite_rating` | character | 247Sports Composite rating (industry blend). |
| `composite_rating_or_default` | character | 247Sports Composite rating, falling back to a default value when unrated. |
| `average_rank` | character | Player's average rank across the tracked industry services. |
| `previous_recruitment` | integer | Nested record for the player's previous recruitment (stringified). |
| `primary` | character | Whether this is the player's primary sport. |
| `class_year_override` | character | Override of the player's recruiting class year, when 247Sports reassigns it. |
| `class_year` | character | Recruiting class year. |
| `recruitment` | integer | FK -> Recruitment aggregate for this player-sport. |
| `primary_institution_prediction` | integer | Nested leading Crystal Ball institution prediction for the player (stringified). |
| `secondary_institution_prediction` | integer | Nested second-place Crystal Ball institution prediction (stringified). |
| `primary_institution_prediction_percentage` | character | Share of Crystal Ball predictions favoring the leading institution. |
| `show_unranked_rating` | character | 247Sports display flag to show the rating even while the player is unranked. |
| `current_player_sport_year` | integer | Current ranking-cycle year for the player-sport profile. |
| `unpublished_player_sport_ranking` | integer | Nested not-yet-published ranking row for the player (stringified). |
| `current_player_sport_ranking` | integer | Nested current published ranking row for the player (stringified). |
| `primary_player_position` | integer | Nested 247Sports record for the player's primary position (stringified). |
| `primary_position` | integer | Player's primary position on the 247Sports profile. |
| `primary_position_group` | integer | Position group the player's primary position belongs to. |
| `default_name` | character | Server-rendered display label for the entity. |
| `star_rating` | character | Star tier (2-5). |
| `secondary_institution_prediction_percentage` | character | Share of Crystal Ball predictions favoring the second-place institution. |
| `jersey` | character | Jersey number. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_season_roster_embed(season='2020-Football')
```

_Last validated n/a._
