---
title: "CFB — 247Sports Site Pages (247sports.com) — Recruitment"
sidebar_label: "Recruitment"
sidebar_position: 3
description: "CFB — 247Sports Site Pages (247sports.com) — Recruitment — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB — 247Sports Site Pages (247sports.com) — Recruitment

## sports247_site_pages_recruitment_final_choice

Final-choice PlayerSport/commit for a recruitment.

**Endpoint URL:** `GET https://247sports.com/Recruitment/{key}/FinalChoice.json`

**Valid URL:** [https://247sports.com/Recruitment/114978/FinalChoice.json](https://247sports.com/Recruitment/114978/FinalChoice.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_recruitment_final_choice-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player` | integer | Player name. |
| `player_institution` | integer | Nested player-institution stint the player-sport profile points to (stringified). |
| `state` | integer | Home state of the recruit, per 247Sports. |
| `sport` | integer | Nested 247Sports sport for the profile (stringified). |
| `rating` | integer | 247Sports rating string (0-1). |
| `rating_or_default` | integer | 247Sports in-house rating, falling back to a default value when unrated. |
| `local_index` | integer | 247Sports' own industry-index value for the player, alongside the Rivals and ESPN indexes. |
| `rivals_grade` | numeric | Rivals source grade (industry composite input). |
| `rivals_rank` | integer | Player's rank in the Rivals industry ranking, as tracked by 247Sports. |
| `rivals_index` | numeric | Rivals index value for the player, as tracked by 247Sports. |
| `espn_grade` | integer | ESPN source grade (industry composite input). |
| `espn_rank` | integer | Player's rank in the ESPN industry ranking, as tracked by 247Sports. |
| `espn_index` | numeric | ESPN index value for the player, as tracked by 247Sports. |
| `composite_strength` | integer | Composite strength points (team-ranking weight). |
| `composite_rating` | numeric | 247Sports Composite rating (industry blend). |
| `composite_rating_or_default` | numeric | 247Sports Composite rating, falling back to a default value when unrated. |
| `average_rank` | numeric | Player's average rank across the tracked industry services. |
| `previous_recruitment` | integer | Nested record for the player's previous recruitment (stringified). |
| `primary` | character | Whether this is the player's primary sport. |
| `class_year_override` | character | Override of the player's recruiting class year, when 247Sports reassigns it. |
| `class_year` | character | Recruiting class year. |
| `recruitment` | integer | FK -> Recruitment aggregate for this player-sport. |
| `primary_institution_prediction` | numeric | Nested leading Crystal Ball institution prediction for the player (stringified). |
| `secondary_institution_prediction` | integer | Nested second-place Crystal Ball institution prediction (stringified). |
| `primary_institution_prediction_percentage` | numeric | Share of Crystal Ball predictions favoring the leading institution. |
| `show_unranked_rating` | character | 247Sports display flag to show the rating even while the player is unranked. |
| `current_player_sport_year` | numeric | Current ranking-cycle year for the player-sport profile. |
| `unpublished_player_sport_ranking` | numeric | Nested not-yet-published ranking row for the player (stringified). |
| `current_player_sport_ranking` | numeric | Nested current published ranking row for the player (stringified). |
| `primary_player_position` | integer | Nested 247Sports record for the player's primary position (stringified). |
| `primary_position` | integer | Player's primary position on the 247Sports profile. |
| `primary_position_group` | integer | Position group the player's primary position belongs to. |
| `default_name` | character | Server-rendered display label for the entity. |
| `star_rating` | integer | Star tier (2-5). |
| `secondary_institution_prediction_percentage` | numeric | Share of Crystal Ball predictions favoring the second-place institution. |
| `jersey` | integer | Jersey number. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_recruitment_final_choice-example}

```python
sports247_site_pages_recruitment_final_choice(key=114978)
```

_Last validated n/a._

## sports247_site_pages_recruitment_institution

Committed institution for a recruitment.

**Endpoint URL:** `GET https://247sports.com/Recruitment/{key}/Institution.json`

**Valid URL:** [https://247sports.com/Recruitment/114978/Institution.json](https://247sports.com/Recruitment/114978/Institution.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_recruitment_institution-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `name` | character | Position name (e.g. `Quarterback`). |
| `type` | character | Institution type code (college / pro / high school). |
| `group` | character | Institution group (division/level) bitmask code. |
| `location` | integer | FK -> Location (`/Institution/{Location}/Location.json`). |
| `state` | integer | FK -> State entity. |
| `latitude` | numeric | Venue latitude in decimal degrees. |
| `longitude` | numeric | Venue longitude in decimal degrees. |
| `rankable` | character | Whether the institution participates in class rankings. |
| `mascot` | character | Team mascot. |
| `abbreviation` | character | Metric abbreviation. |
| `primary_color` | character | Primary team color (hex). |
| `secondary_color` | character | Secondary team color (hex). |
| `is_foreign` | character | Whether the institution is located outside the United States. |
| `site` | integer | FK -> team Site (network site key). |
| `default_asset` | numeric | Nested 247Sports image asset for the institution's primary logo (stringified). |
| `alternate_asset` | numeric | Nested 247Sports image asset for the institution's alternate logo (stringified). |
| `light_asset` | numeric | Nested 247Sports image asset for the light-background logo variant (stringified). |
| `default_name` | character | Server-rendered display label for the entity. |
| `address` | character | Institution's street address. |
| `telephone` | character | Institution's telephone number. |
| `website` | character | Institution's website URL. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_recruitment_institution-example}

```python
sports247_site_pages_recruitment_institution(key=114978)
```

_Last validated n/a._

## sports247_site_pages_recruitment_interests

All institutions the recruit has interest links with.

**Endpoint URL:** `GET https://247sports.com/Recruitment/{key}/Interests.json`

**Valid URL:** [https://247sports.com/Recruitment/114978/Interests.json](https://247sports.com/Recruitment/114978/Interests.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_recruitment_interests-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `name` | character | Position name (e.g. `Quarterback`). |
| `type` | character | Institution type code (college / pro / high school). |
| `group` | character | Institution group (division/level) bitmask code. |
| `location` | integer | FK -> Location (`/Institution/{Location}/Location.json`). |
| `state` | integer | FK -> State entity. |
| `latitude` | numeric | Venue latitude in decimal degrees. |
| `longitude` | numeric | Venue longitude in decimal degrees. |
| `rankable` | character | Whether the institution participates in class rankings. |
| `mascot` | character | Team mascot. |
| `abbreviation` | character | Metric abbreviation. |
| `primary_color` | character | Primary team color (hex). |
| `secondary_color` | character | Secondary team color (hex). |
| `is_foreign` | character | Whether the institution is located outside the United States. |
| `site` | integer | FK -> team Site (network site key). |
| `default_asset` | numeric | Nested 247Sports image asset for the institution's primary logo (stringified). |
| `alternate_asset` | numeric | Nested 247Sports image asset for the institution's alternate logo (stringified). |
| `light_asset` | numeric | Nested 247Sports image asset for the light-background logo variant (stringified). |
| `default_name` | character | Server-rendered display label for the entity. |
| `address` | character | Institution's street address. |
| `telephone` | character | Institution's telephone number. |
| `website` | character | Institution's website URL. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_recruitment_interests-example}

```python
sports247_site_pages_recruitment_interests(key=114978)
```

_Last validated n/a._

## sports247_site_pages_recruitment_offers

Institutions that have offered the recruit.

**Endpoint URL:** `GET https://247sports.com/Recruitment/{key}/Offers.json`

**Valid URL:** [https://247sports.com/Recruitment/114978/Offers.json](https://247sports.com/Recruitment/114978/Offers.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_recruitment_offers-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `name` | character | Position name (e.g. `Quarterback`). |
| `type` | character | Institution type code (college / pro / high school). |
| `group` | character | Institution group (division/level) bitmask code. |
| `location` | integer | FK -> Location (`/Institution/{Location}/Location.json`). |
| `state` | integer | FK -> State entity. |
| `latitude` | numeric | Venue latitude in decimal degrees. |
| `longitude` | numeric | Venue longitude in decimal degrees. |
| `rankable` | character | Whether the institution participates in class rankings. |
| `mascot` | character | Team mascot. |
| `abbreviation` | character | Metric abbreviation. |
| `primary_color` | character | Primary team color (hex). |
| `secondary_color` | character | Secondary team color (hex). |
| `is_foreign` | character | Whether the institution is located outside the United States. |
| `site` | integer | FK -> team Site (network site key). |
| `default_asset` | numeric | Nested 247Sports image asset for the institution's primary logo (stringified). |
| `alternate_asset` | numeric | Nested 247Sports image asset for the institution's alternate logo (stringified). |
| `light_asset` | numeric | Nested 247Sports image asset for the light-background logo variant (stringified). |
| `default_name` | character | Server-rendered display label for the entity. |
| `address` | character | Institution's street address. |
| `telephone` | character | Institution's telephone number. |
| `website` | character | Institution's website URL. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_recruitment_offers-example}

```python
sports247_site_pages_recruitment_offers(key=114978)
```

_Last validated n/a._

## sports247_site_pages_recruitment_player_sport

PlayerSport underlying a recruitment.

**Endpoint URL:** `GET https://247sports.com/Recruitment/{key}/PlayerSport.json`

**Valid URL:** [https://247sports.com/Recruitment/114978/PlayerSport.json](https://247sports.com/Recruitment/114978/PlayerSport.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_recruitment_player_sport-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player` | integer | Player name. |
| `player_institution` | integer | Nested player-institution stint the player-sport profile points to (stringified). |
| `state` | integer | Home state of the recruit, per 247Sports. |
| `sport` | integer | Nested 247Sports sport for the profile (stringified). |
| `rating` | integer | 247Sports rating string (0-1). |
| `rating_or_default` | integer | 247Sports in-house rating, falling back to a default value when unrated. |
| `local_index` | integer | 247Sports' own industry-index value for the player, alongside the Rivals and ESPN indexes. |
| `rivals_grade` | numeric | Rivals source grade (industry composite input). |
| `rivals_rank` | integer | Player's rank in the Rivals industry ranking, as tracked by 247Sports. |
| `rivals_index` | numeric | Rivals index value for the player, as tracked by 247Sports. |
| `espn_grade` | integer | ESPN source grade (industry composite input). |
| `espn_rank` | integer | Player's rank in the ESPN industry ranking, as tracked by 247Sports. |
| `espn_index` | numeric | ESPN index value for the player, as tracked by 247Sports. |
| `composite_strength` | integer | Composite strength points (team-ranking weight). |
| `composite_rating` | numeric | 247Sports Composite rating (industry blend). |
| `composite_rating_or_default` | numeric | 247Sports Composite rating, falling back to a default value when unrated. |
| `average_rank` | numeric | Player's average rank across the tracked industry services. |
| `previous_recruitment` | integer | Nested record for the player's previous recruitment (stringified). |
| `primary` | character | Whether this is the player's primary sport. |
| `class_year_override` | character | Override of the player's recruiting class year, when 247Sports reassigns it. |
| `class_year` | character | Recruiting class year. |
| `recruitment` | integer | FK -> Recruitment aggregate for this player-sport. |
| `primary_institution_prediction` | numeric | Nested leading Crystal Ball institution prediction for the player (stringified). |
| `secondary_institution_prediction` | integer | Nested second-place Crystal Ball institution prediction (stringified). |
| `primary_institution_prediction_percentage` | numeric | Share of Crystal Ball predictions favoring the leading institution. |
| `show_unranked_rating` | character | 247Sports display flag to show the rating even while the player is unranked. |
| `current_player_sport_year` | numeric | Current ranking-cycle year for the player-sport profile. |
| `unpublished_player_sport_ranking` | numeric | Nested not-yet-published ranking row for the player (stringified). |
| `current_player_sport_ranking` | numeric | Nested current published ranking row for the player (stringified). |
| `primary_player_position` | integer | Nested 247Sports record for the player's primary position (stringified). |
| `primary_position` | integer | Player's primary position on the 247Sports profile. |
| `primary_position_group` | integer | Position group the player's primary position belongs to. |
| `default_name` | character | Server-rendered display label for the entity. |
| `star_rating` | integer | Star tier (2-5). |
| `secondary_institution_prediction_percentage` | numeric | Share of Crystal Ball predictions favoring the second-place institution. |
| `jersey` | integer | Jersey number. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_recruitment_player_sport-example}

```python
sports247_site_pages_recruitment_player_sport(key=114978)
```

_Last validated n/a._
