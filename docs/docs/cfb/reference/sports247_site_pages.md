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
| `alma_mater` | integer |  |
| `cbs_key` | character |  |
| `twitter_contact` | character |  |
| `predictions_locked` | character |  |
| `primary_coach_job` | integer |  |
| `default_asset` | integer |  |
| `hero_asset` | character |  |
| `quote_asset` | character |  |
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
| `is_foreign` | character |  |
| `site` | integer | FK -> team Site (network site key). |
| `default_asset` | integer |  |
| `alternate_asset` | integer |  |
| `light_asset` | integer |  |
| `default_name` | character | Server-rendered display label for the entity. |
| `address` | character |  |
| `telephone` | character |  |

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
| `county_tax_rate` | character |  |
| `city_tax_rate` | character |  |
| `special_tax_rate` | character |  |
| `region_name` | character |  |
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
| `institution` | integer |  |
| `conference` | integer | Conference of the team. |
| `ranking` | integer | FK -> the Ranking snapshot this row belongs to. |
| `sport` | integer |  |
| `recruitment` | character |  |
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `scout_rating` | character |  |
| `composite_rating` | character | Composite class rating for the coach's haul. |
| `commits` | character | Number of commits in the position group. |
| `total` | character | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `composite_total` | character |  |
| `five_stars` | character |  |
| `scout_five_stars` | character |  |
| `composite_five_stars` | character |  |
| `four_stars` | character |  |
| `scout_four_stars` | character |  |
| `composite_four_stars` | character |  |
| `three_stars` | character | Whether three stars data is available. |
| `scout_three_stars` | character |  |
| `composite_three_stars` | character |  |
| `two_stars` | character |  |
| `scout_two_stars` | character |  |
| `composite_two_stars` | character |  |
| `average_rating` | character |  |
| `average_scout_rating` | character |  |
| `composite_average_rating` | character |  |
| `overall_rank` | character | Overall national coach-recruiting rank. |
| `composite_overall_rank` | character |  |
| `scout_overall_rank` | character |  |
| `division_rank` | character |  |
| `scout_division_rank` | character |  |
| `composite_division_rank` | character |  |
| `conference_rank` | character | Rank within conference. |
| `scout_conference_rank` | character |  |
| `composite_conference_rank` | character |  |
| `previous_coach_ranking` | integer |  |
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
| `institution` | integer |  |
| `conference` | integer | Conference of the team. |
| `ranking` | integer | FK -> the Ranking snapshot this row belongs to. |
| `sport` | integer |  |
| `recruitment` | character |  |
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `scout_rating` | character |  |
| `composite_rating` | character | Composite class rating for the coach's haul. |
| `commits` | character | Number of commits in the position group. |
| `total` | character | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `composite_total` | character |  |
| `five_stars` | character |  |
| `scout_five_stars` | character |  |
| `composite_five_stars` | character |  |
| `four_stars` | character |  |
| `scout_four_stars` | character |  |
| `composite_four_stars` | character |  |
| `three_stars` | character | Whether three stars data is available. |
| `scout_three_stars` | character |  |
| `composite_three_stars` | character |  |
| `two_stars` | character |  |
| `scout_two_stars` | character |  |
| `composite_two_stars` | character |  |
| `average_rating` | character |  |
| `average_scout_rating` | character |  |
| `composite_average_rating` | character |  |
| `overall_rank` | character | Overall national coach-recruiting rank. |
| `composite_overall_rank` | character |  |
| `scout_overall_rank` | character |  |
| `division_rank` | character |  |
| `scout_division_rank` | character |  |
| `composite_division_rank` | character |  |
| `conference_rank` | character | Rank within conference. |
| `scout_conference_rank` | character |  |
| `composite_conference_rank` | character |  |
| `previous_coach_ranking` | integer |  |
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
| `event_group` | integer |  |
| `event_type` | integer | Event / play type code (V2 PBP). |
| `event_date` | character | Event date-time in ISO 8601 (e.g. '2017-07-11T00:00:00Z'). |
| `default_asset` | integer |  |
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
| `is_foreign` | character |  |
| `site` | integer | FK -> team Site (network site key). |
| `default_asset` | integer |  |
| `alternate_asset` | integer |  |
| `light_asset` | integer |  |
| `default_name` | character | Server-rendered display label for the entity. |
| `address` | character |  |
| `telephone` | character |  |

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
| `is_foreign` | character |  |
| `site` | integer | FK -> team Site (network site key). |
| `default_asset` | integer |  |
| `alternate_asset` | integer |  |
| `light_asset` | integer |  |
| `default_name` | character | Server-rendered display label for the entity. |
| `address` | character |  |
| `telephone` | character |  |

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
| `county_tax_rate` | character |  |
| `city_tax_rate` | character |  |
| `special_tax_rate` | character |  |
| `region_name` | character |  |
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
| `body` | character |  |
| `date` | character | Date of the poll release. |
| `author_first_name` | character |  |
| `author_last_name` | character |  |
| `author_affiliation` | character |  |

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
| `pro_team` | integer |  |
| `pro_team_name` | character |  |
| `year` | character | Four-digit season year (e.g. 2019). |
| `round` | character | Draft round number (1-based) the pick belongs to. |
| `pick` | character | Pick number within the round. |
| `overall_pick` | character | Overall selection number in the draft. |
| `player` | integer | Player name. |
| `player_first_name` | character | Player's first name |
| `player_last_name` | character | Player's last name |
| `college_team` | integer | College team name. |
| `college_team_name` | character |  |
| `position_abbreviation` | character | Player's position at draft. |
| `traded_from_team` | character |  |
| `pick_type` | character |  |
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
| `is_foreign` | character |  |
| `site` | integer | FK -> team Site (network site key). |
| `default_asset` | integer |  |
| `alternate_asset` | integer |  |
| `light_asset` | integer |  |
| `default_name` | character | Server-rendered display label for the entity. |
| `address` | character |  |
| `telephone` | character |  |

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
| `update_date` | character |  |
| `title_text` | character |  |
| `main_text` | character |  |
| `redirection_url` | character |  |

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
| `bio` | character |  |
| `scout_evaluation` | character |  |
| `birthdate` | character | Birthdate |
| `modified_user` | character |  |
| `modified_date` | character |  |
| `cbs_key` | integer | Cross-reference key into the CBS Sports id space. |
| `url` | character | RotoWire player page URL. |
| `last_recruitment_player_institution` | integer |  |
| `current_player_institution` | integer | FK -> PlayerInstitution (current school). |
| `twitter_contact` | integer |  |
| `mobile_phone_contact` | character |  |
| `primary_player_sport` | integer | FK -> PlayerSport (`/PlayerSport/{id}.json`). |
| `primary_recruitment` | integer |  |
| `default_name` | character | Server-rendered display label for the entity. |
| `default_asset` | integer |  |
| `default_asset_url` | character |  |
| `hero_asset` | character |  |
| `quote_asset` | character |  |
| `user` | character |  |
| `pro_stat_player` | integer |  |
| `college_stat_player` | integer |  |
| `bio_or_default` | character |  |
| `rating` | integer | 247Sports numeric rating (0-1 scale) for the primary sport. |
| `star_rating` | integer | Star tier (2-5) derived from the rating. |
| `national_rank` | integer | Overall national rank in the recruit's class. |
| `position_rank` | integer | Rank within position for the class. |
| `state_rank` | integer | Rank within home state for the class. |
| `hometown_state` | integer | Recruit hometown state. |
| `hometown_city` | character | Recruit hometown city. |
| `player_high_school_name` | character |  |
| `primary_player_position_abbreviation` | character |  |

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
| `institution` | integer |  |
| `state` | integer | Venue state. |
| `agent` | character | Listed player agent. |
| `end_year` | character | Span ending year. |
| `end_date` | character | Season end timestamp (ISO 8601, UTC). |
| `early_enrollee` | character |  |
| `early_signee` | character |  |
| `height` | character | Listed height (inches). |
| `weight` | character | Listed weight (lbs). |
| `transfer_institution` | character |  |
| `transfer_season` | character |  |
| `transfer_eligibility` | character |  |
| `created_date` | character |  |
| `modified_date` | character |  |
| `lead_expert` | integer |  |
| `player_institution_evaluation` | integer |  |
| `primary_player_sport` | integer |  |
| `default_asset` | integer |  |
| `hero_asset` | character |  |
| `primary_recruitment` | integer |  |
| `default_name` | character | Server-rendered display label for the entity. |
| `end_year_or_current` | character |  |
| `start_year_or_expected` | character |  |
| `end_year_or_expected` | character |  |
| `next_institution_type` | character |  |
| `next_institution_group` | character |  |
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
| `institution` | integer |  |
| `state` | integer | Venue state. |
| `agent` | character | Listed player agent. |
| `end_year` | character | Span ending year. |
| `end_date` | character | Season end timestamp (ISO 8601, UTC). |
| `early_enrollee` | character |  |
| `early_signee` | character |  |
| `height` | character | Listed height (inches). |
| `weight` | character | Listed weight (lbs). |
| `transfer_institution` | character |  |
| `transfer_season` | character |  |
| `transfer_eligibility` | character |  |
| `created_date` | character |  |
| `modified_date` | character |  |
| `lead_expert` | integer |  |
| `player_institution_evaluation` | integer |  |
| `primary_player_sport` | integer |  |
| `default_asset` | integer |  |
| `hero_asset` | character |  |
| `primary_recruitment` | integer |  |
| `default_name` | character | Server-rendered display label for the entity. |
| `end_year_or_current` | character |  |
| `start_year_or_expected` | character |  |
| `end_year_or_expected` | character |  |
| `next_institution_type` | character |  |
| `next_institution_group` | character |  |
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
| `institution` | integer |  |
| `state` | integer | Venue state. |
| `agent` | character | Listed player agent. |
| `end_year` | character | Span ending year. |
| `end_date` | character | Season end timestamp (ISO 8601, UTC). |
| `early_enrollee` | character |  |
| `early_signee` | character |  |
| `height` | character | Listed height (inches). |
| `weight` | character | Listed weight (lbs). |
| `transfer_institution` | character |  |
| `transfer_season` | character |  |
| `transfer_eligibility` | character |  |
| `created_date` | character |  |
| `modified_date` | character |  |
| `lead_expert` | integer |  |
| `player_institution_evaluation` | integer |  |
| `primary_player_sport` | integer |  |
| `default_asset` | integer |  |
| `hero_asset` | character |  |
| `primary_recruitment` | integer |  |
| `default_name` | character | Server-rendered display label for the entity. |
| `end_year_or_current` | character |  |
| `start_year_or_expected` | character |  |
| `end_year_or_expected` | character |  |
| `next_institution_type` | character |  |
| `next_institution_group` | character |  |
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
| `player_institution` | integer |  |
| `user` | integer |  |
| `evaluated_date` | character |  |
| `comparison_player` | integer |  |
| `projection` | character |  |
| `primary` | character |  |
| `scout_evaluation` | character |  |
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
| `player_institution` | integer |  |
| `state` | integer | Venue state. |
| `sport` | integer |  |
| `rating` | character | 247Sports rating string (0-1). |
| `rating_or_default` | character |  |
| `local_index` | character |  |
| `rivals_grade` | character | Rivals source grade (industry composite input). |
| `rivals_rank` | character |  |
| `rivals_index` | character |  |
| `espn_grade` | character | ESPN source grade (industry composite input). |
| `espn_rank` | character |  |
| `espn_index` | character |  |
| `composite_strength` | character | Composite strength points (team-ranking weight). |
| `composite_rating` | character | 247Sports Composite rating (industry blend). |
| `composite_rating_or_default` | character |  |
| `average_rank` | character |  |
| `previous_recruitment` | integer |  |
| `primary` | character |  |
| `class_year_override` | character |  |
| `class_year` | character | Recruiting class year. |
| `recruitment` | integer | FK -> Recruitment aggregate for this player-sport. |
| `primary_institution_prediction` | integer |  |
| `secondary_institution_prediction` | integer |  |
| `primary_institution_prediction_percentage` | character |  |
| `show_unranked_rating` | character |  |
| `current_player_sport_year` | integer |  |
| `unpublished_player_sport_ranking` | integer |  |
| `current_player_sport_ranking` | integer |  |
| `primary_player_position` | integer |  |
| `primary_position` | integer |  |
| `primary_position_group` | integer |  |
| `default_name` | character | Server-rendered display label for the entity. |
| `star_rating` | character | Star tier (2-5). |
| `secondary_institution_prediction_percentage` | character |  |
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
| `bio` | character |  |
| `scout_evaluation` | character |  |
| `birthdate` | character | Birthdate |
| `modified_user` | character |  |
| `modified_date` | character |  |
| `cbs_key` | integer | Cross-reference key into the CBS Sports id space. |
| `url` | character | RotoWire player page URL. |
| `last_recruitment_player_institution` | integer |  |
| `current_player_institution` | integer | FK -> PlayerInstitution (current school). |
| `twitter_contact` | integer |  |
| `mobile_phone_contact` | character |  |
| `primary_player_sport` | integer | FK -> PlayerSport (`/PlayerSport/{id}.json`). |
| `primary_recruitment` | integer |  |
| `default_name` | character | Server-rendered display label for the entity. |
| `default_asset` | integer |  |
| `default_asset_url` | character |  |
| `hero_asset` | character |  |
| `quote_asset` | character |  |
| `user` | character |  |
| `pro_stat_player` | integer |  |
| `college_stat_player` | integer |  |
| `bio_or_default` | character |  |
| `rating` | integer | 247Sports numeric rating (0-1 scale) for the primary sport. |
| `star_rating` | integer | Star tier (2-5) derived from the rating. |
| `national_rank` | integer | Overall national rank in the recruit's class. |
| `position_rank` | integer | Rank within position for the class. |
| `state_rank` | integer | Rank within home state for the class. |
| `hometown_state` | integer | Recruit hometown state. |
| `hometown_city` | character | Recruit hometown city. |
| `player_high_school_name` | character |  |
| `primary_player_position_abbreviation` | character |  |

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
| `player_institution` | integer |  |
| `state` | integer | Venue state. |
| `sport` | integer |  |
| `rating` | character | 247Sports rating string (0-1). |
| `rating_or_default` | character |  |
| `local_index` | character |  |
| `rivals_grade` | character | Rivals source grade (industry composite input). |
| `rivals_rank` | character |  |
| `rivals_index` | character |  |
| `espn_grade` | character | ESPN source grade (industry composite input). |
| `espn_rank` | character |  |
| `espn_index` | character |  |
| `composite_strength` | character | Composite strength points (team-ranking weight). |
| `composite_rating` | character | 247Sports Composite rating (industry blend). |
| `composite_rating_or_default` | character |  |
| `average_rank` | character |  |
| `previous_recruitment` | integer |  |
| `primary` | character |  |
| `class_year_override` | character |  |
| `class_year` | character | Recruiting class year. |
| `recruitment` | integer | FK -> Recruitment aggregate for this player-sport. |
| `primary_institution_prediction` | integer |  |
| `secondary_institution_prediction` | integer |  |
| `primary_institution_prediction_percentage` | character |  |
| `show_unranked_rating` | character |  |
| `current_player_sport_year` | integer |  |
| `unpublished_player_sport_ranking` | integer |  |
| `current_player_sport_ranking` | integer |  |
| `primary_player_position` | integer |  |
| `primary_position` | integer |  |
| `primary_position_group` | integer |  |
| `default_name` | character | Server-rendered display label for the entity. |
| `star_rating` | character | Star tier (2-5). |
| `secondary_institution_prediction_percentage` | character |  |
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
| `institution` | integer |  |
| `state` | integer | Venue state. |
| `agent` | character | Listed player agent. |
| `end_year` | character | Span ending year. |
| `end_date` | character | Season end timestamp (ISO 8601, UTC). |
| `early_enrollee` | character |  |
| `early_signee` | character |  |
| `height` | character | Listed height (inches). |
| `weight` | character | Listed weight (lbs). |
| `transfer_institution` | character |  |
| `transfer_season` | character |  |
| `transfer_eligibility` | character |  |
| `created_date` | character |  |
| `modified_date` | character |  |
| `lead_expert` | integer |  |
| `player_institution_evaluation` | integer |  |
| `primary_player_sport` | integer |  |
| `default_asset` | integer |  |
| `hero_asset` | character |  |
| `primary_recruitment` | integer |  |
| `default_name` | character | Server-rendered display label for the entity. |
| `end_year_or_current` | character |  |
| `start_year_or_expected` | character |  |
| `end_year_or_expected` | character |  |
| `next_institution_type` | character |  |
| `next_institution_group` | character |  |
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
| `sport` | integer |  |
| `player_sport` | integer |  |
| `committed_institution` | integer | FK -> committed Institution (null if uncommitted). |
| `order` | character | Team order within the competition (0 = first). |
| `position` | integer | Athlete position. |
| `position_group` | integer | Position group of the recruits (e.g. Offensive Line, Defensive Back). |
| `platoon` | integer |  |
| `state` | integer | Venue state. |
| `region` | integer | Broadcast region code. |
| `institution` | integer |  |
| `institution_group` | character |  |
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `composite_strength` | character |  |
| `composite_rating` | character |  |
| `overall_rank` | character | Overall national rank in the snapshot. |
| `composite_overall_rank` | character |  |
| `group_rank` | character | League/season rank for group. |
| `composite_group_rank` | character |  |
| `position_rank` | character | Rank within position. |
| `previous_player_sport_ranking` | integer |  |
| `composite_position_rank` | character |  |
| `state_rank` | character | Rank within home state. |
| `composite_state_rank` | character |  |
| `default_name` | character | Server-rendered display label for the entity. |
| `position_group_rank` | character |  |
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
| `sport` | integer |  |
| `player_sport` | integer |  |
| `committed_institution` | integer | FK -> committed Institution (null if uncommitted). |
| `order` | character | Team order within the competition (0 = first). |
| `position` | integer | Athlete position. |
| `position_group` | integer | Position group of the recruits (e.g. Offensive Line, Defensive Back). |
| `platoon` | integer |  |
| `state` | integer | Venue state. |
| `region` | integer | Broadcast region code. |
| `institution` | integer |  |
| `institution_group` | character |  |
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `composite_strength` | character |  |
| `composite_rating` | character |  |
| `overall_rank` | character | Overall national rank in the snapshot. |
| `composite_overall_rank` | character |  |
| `group_rank` | character | League/season rank for group. |
| `composite_group_rank` | character |  |
| `position_rank` | character | Rank within position. |
| `previous_player_sport_ranking` | integer |  |
| `composite_position_rank` | character |  |
| `state_rank` | character | Rank within home state. |
| `composite_state_rank` | character |  |
| `default_name` | character | Server-rendered display label for the entity. |
| `position_group_rank` | character |  |
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
| `player_sport` | integer |  |
| `recruit_state` | integer |  |
| `institution` | integer | FK -> the interested/interesting Institution. |
| `lock_prediction` | character |  |
| `recruits_interest` | character |  |
| `primary_coach` | integer |  |
| `secondary_coach` | character |  |
| `keeper_coach` | character |  |
| `institutions_interest` | character |  |
| `position` | integer | Athlete position. |
| `position_group` | integer | Position group of the recruits (e.g. Offensive Line, Defensive Back). |
| `platoon` | integer |  |
| `offered` | character | Whether the school has extended an offer. |
| `gray_shirt` | character |  |
| `walk_on` | character |  |
| `official_visit` | integer |  |
| `second_official_visit` | character |  |
| `soft_commit` | character |  |
| `hard_commit` | integer | FK -> the RecruitInterestEvent marking a hard commit. |
| `signing_date` | integer |  |
| `enrollment_date` | integer |  |
| `decommit` | character |  |
| `offer` | character |  |
| `highest_recruit_interest_event` | integer |  |
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
| `player_institution` | integer |  |
| `state` | integer | Venue state. |
| `sport` | integer |  |
| `rating` | character | 247Sports rating string (0-1). |
| `rating_or_default` | character |  |
| `local_index` | character |  |
| `rivals_grade` | character | Rivals source grade (industry composite input). |
| `rivals_rank` | character |  |
| `rivals_index` | character |  |
| `espn_grade` | character | ESPN source grade (industry composite input). |
| `espn_rank` | character |  |
| `espn_index` | character |  |
| `composite_strength` | character | Composite strength points (team-ranking weight). |
| `composite_rating` | character | 247Sports Composite rating (industry blend). |
| `composite_rating_or_default` | character |  |
| `average_rank` | character |  |
| `previous_recruitment` | integer |  |
| `primary` | character |  |
| `class_year_override` | character |  |
| `class_year` | character | Recruiting class year. |
| `recruitment` | integer | FK -> Recruitment aggregate for this player-sport. |
| `primary_institution_prediction` | integer |  |
| `secondary_institution_prediction` | integer |  |
| `primary_institution_prediction_percentage` | character |  |
| `show_unranked_rating` | character |  |
| `current_player_sport_year` | integer |  |
| `unpublished_player_sport_ranking` | integer |  |
| `current_player_sport_ranking` | integer |  |
| `primary_player_position` | integer |  |
| `primary_position` | integer |  |
| `primary_position_group` | integer |  |
| `default_name` | character | Server-rendered display label for the entity. |
| `star_rating` | character | Star tier (2-5). |
| `secondary_institution_prediction_percentage` | character |  |
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
| `is_foreign` | character |  |
| `site` | integer | FK -> team Site (network site key). |
| `default_asset` | integer |  |
| `alternate_asset` | integer |  |
| `light_asset` | integer |  |
| `default_name` | character | Server-rendered display label for the entity. |
| `address` | character |  |
| `telephone` | character |  |

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
| `is_foreign` | character |  |
| `site` | integer | FK -> team Site (network site key). |
| `default_asset` | integer |  |
| `alternate_asset` | integer |  |
| `light_asset` | integer |  |
| `default_name` | character | Server-rendered display label for the entity. |
| `address` | character |  |
| `telephone` | character |  |

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
| `is_foreign` | character |  |
| `site` | integer | FK -> team Site (network site key). |
| `default_asset` | integer |  |
| `alternate_asset` | integer |  |
| `light_asset` | integer |  |
| `default_name` | character | Server-rendered display label for the entity. |
| `address` | character |  |
| `telephone` | character |  |

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
| `player_institution` | integer |  |
| `state` | integer | Venue state. |
| `sport` | integer |  |
| `rating` | character | 247Sports rating string (0-1). |
| `rating_or_default` | character |  |
| `local_index` | character |  |
| `rivals_grade` | character | Rivals source grade (industry composite input). |
| `rivals_rank` | character |  |
| `rivals_index` | character |  |
| `espn_grade` | character | ESPN source grade (industry composite input). |
| `espn_rank` | character |  |
| `espn_index` | character |  |
| `composite_strength` | character | Composite strength points (team-ranking weight). |
| `composite_rating` | character | 247Sports Composite rating (industry blend). |
| `composite_rating_or_default` | character |  |
| `average_rank` | character |  |
| `previous_recruitment` | integer |  |
| `primary` | character |  |
| `class_year_override` | character |  |
| `class_year` | character | Recruiting class year. |
| `recruitment` | integer | FK -> Recruitment aggregate for this player-sport. |
| `primary_institution_prediction` | integer |  |
| `secondary_institution_prediction` | integer |  |
| `primary_institution_prediction_percentage` | character |  |
| `show_unranked_rating` | character |  |
| `current_player_sport_year` | integer |  |
| `unpublished_player_sport_ranking` | integer |  |
| `current_player_sport_ranking` | integer |  |
| `primary_player_position` | integer |  |
| `primary_position` | integer |  |
| `primary_position_group` | integer |  |
| `default_name` | character | Server-rendered display label for the entity. |
| `star_rating` | character | Star tier (2-5). |
| `secondary_institution_prediction_percentage` | character |  |
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
| `player_institution` | integer |  |
| `institution` | integer | FK -> predicted destination Institution. |
| `user` | integer |  |
| `updated_on` | character |  |
| `prediction_status` | character | Crystal-ball prediction status code. |
| `days_correct` | character |  |
| `premium` | character | Whether the article is premium content. |
| `score` | character | Expert accuracy score at time of prediction. |
| `confidence` | character | Expert confidence 1-10. |
| `parent` | character |  |
| `is_zero_zone` | character |  |
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
| `institution` | integer |  |
| `recruitment` | integer |  |
| `recruit_interest` | integer |  |
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
| `player_sport` | integer |  |
| `recruit_state` | integer |  |
| `institution` | integer | FK -> the interested/interesting Institution. |
| `lock_prediction` | character |  |
| `recruits_interest` | character |  |
| `primary_coach` | integer |  |
| `secondary_coach` | character |  |
| `keeper_coach` | character |  |
| `institutions_interest` | character |  |
| `position` | integer | Athlete position. |
| `position_group` | integer | Position group of the recruits (e.g. Offensive Line, Defensive Back). |
| `platoon` | integer |  |
| `offered` | character | Whether the school has extended an offer. |
| `gray_shirt` | character |  |
| `walk_on` | character |  |
| `official_visit` | integer |  |
| `second_official_visit` | character |  |
| `soft_commit` | character |  |
| `hard_commit` | integer | FK -> the RecruitInterestEvent marking a hard commit. |
| `signing_date` | integer |  |
| `enrollment_date` | integer |  |
| `decommit` | character |  |
| `offer` | character |  |
| `highest_recruit_interest_event` | integer |  |
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
| `player_institution` | integer |  |
| `year` | integer | Four-digit season year (e.g. 2019). |
| `announcement_date` | character |  |
| `signed_institution` | integer |  |
| `position` | integer | Athlete position. |
| `institution` | integer |  |
| `state` | integer | Venue state. |
| `player_sport` | integer |  |
| `composite_strength` | character | Composite strength points contributed to team ranking. |
| `final_choice` | integer |  |
| `highest_recruit_interest_event_type` | character |  |
| `highest_recruit_interest_event` | integer |  |
| `committed_recruit_interest` | integer |  |
| `committed_institution` | integer | FK -> committed Institution. |
| `highest_recruit_interest` | integer |  |
| `primary_player_position` | integer |  |
| `primary_position` | integer |  |
| `default_name` | character | Server-rendered display label for the entity. |
| `commited_institution_team_image` | character |  |
| `recruit_interest_count` | integer | Number of tracked school interests. |
| `recruit_interests_url` | character | Site URL to the recruit's interest timeline. |
| `player_key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player_first_name` | character | Player's first name |
| `player_last_name` | character | Player's last name |
| `player_full_name` | character | Player full name. |
| `player_height` | character | Participant height (e.g. "6' 5\""). |
| `player_weight` | numeric | Participant weight in pounds. |
| `player_bio` | character |  |
| `player_scout_evaluation` | character |  |
| `player_birthdate` | character |  |
| `player_modified_user` | character |  |
| `player_modified_date` | character |  |
| `player_cbs_key` | integer | Cross-reference key into the CBS Sports id space. |
| `player_url` | character | Full stats.ncaa.org url for the player page. |
| `player_last_recruitment_player_institution` | integer |  |
| `player_current_player_institution` | integer | FK -> PlayerInstitution (current school). |
| `player_twitter_contact` | integer |  |
| `player_mobile_phone_contact` | character |  |
| `player_primary_player_sport` | integer | FK -> PlayerSport (`/PlayerSport/{id}.json`). |
| `player_primary_recruitment` | integer |  |
| `player_default_name` | character | Server-rendered display label for the entity. |
| `player_default_asset` | integer |  |
| `player_default_asset_url` | character |  |
| `player_hero_asset` | character |  |
| `player_quote_asset` | character |  |
| `player_user` | character |  |
| `player_pro_stat_player` | integer |  |
| `player_college_stat_player` | integer |  |
| `player_bio_or_default` | character |  |
| `player_rating` | integer | 247Sports numeric rating (0-1 scale) for the primary sport. |
| `player_star_rating` | integer | Star tier (2-5) derived from the rating. |
| `player_national_rank` | integer | Overall national rank in the recruit's class. |
| `player_position_rank` | integer | Rank within position for the class. |
| `player_state_rank` | integer | Rank within home state for the class. |
| `player_hometown_state` | integer |  |
| `player_hometown_city` | character |  |
| `player_player_high_school_name` | character |  |
| `player_primary_player_position_abbreviation` | character |  |

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
| `player_institution` | integer |  |
| `state` | integer | Venue state. |
| `sport` | integer |  |
| `rating` | character | 247Sports rating string (0-1). |
| `rating_or_default` | character |  |
| `local_index` | character |  |
| `rivals_grade` | character | Rivals source grade (industry composite input). |
| `rivals_rank` | character |  |
| `rivals_index` | character |  |
| `espn_grade` | character | ESPN source grade (industry composite input). |
| `espn_rank` | character |  |
| `espn_index` | character |  |
| `composite_strength` | character | Composite strength points (team-ranking weight). |
| `composite_rating` | character | 247Sports Composite rating (industry blend). |
| `composite_rating_or_default` | character |  |
| `average_rank` | character |  |
| `previous_recruitment` | integer |  |
| `primary` | character |  |
| `class_year_override` | character |  |
| `class_year` | character | Recruiting class year. |
| `recruitment` | integer | FK -> Recruitment aggregate for this player-sport. |
| `primary_institution_prediction` | integer |  |
| `secondary_institution_prediction` | integer |  |
| `primary_institution_prediction_percentage` | character |  |
| `show_unranked_rating` | character |  |
| `current_player_sport_year` | integer |  |
| `unpublished_player_sport_ranking` | integer |  |
| `current_player_sport_ranking` | integer |  |
| `primary_player_position` | integer |  |
| `primary_position` | integer |  |
| `primary_position_group` | integer |  |
| `default_name` | character | Server-rendered display label for the entity. |
| `star_rating` | character | Star tier (2-5). |
| `secondary_institution_prediction_percentage` | character |  |
| `jersey` | character | Jersey number. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
sports247_site_pages_season_roster_embed(season='2020-Football')
```

_Last validated n/a._
