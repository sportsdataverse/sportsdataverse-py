# College football — 247Sports Site Pages (247sports.com) — Recruitment

> College football — 247Sports Site Pages (247sports.com) — Recruitment — function reference in sdv-py, the SportsDataverse Python package.

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
| `key` | Int64 | Primary key of this entity (the id used in its `.json` route). |
| `player` | Int64 | Player name. |
| `player_institution` | Int64 | Nested player-institution stint the player-sport profile points to (stringified). |
| `state` | Int64 | Home state of the recruit, per 247Sports. |
| `sport` | Int64 | Nested 247Sports sport for the profile (stringified). |
| `rating` | Int64 | 247Sports rating string (0-1). |
| `rating_or_default` | Int64 | 247Sports in-house rating, falling back to a default value when unrated. |
| `local_index` | Int64 | 247Sports' own industry-index value for the player, alongside the Rivals and ESPN indexes. |
| `rivals_grade` | Float64 | Rivals source grade (industry composite input). |
| `rivals_rank` | Int64 | Player's rank in the Rivals industry ranking, as tracked by 247Sports. |
| `rivals_index` | Float64 | Rivals index value for the player, as tracked by 247Sports. |
| `espn_grade` | Int64 | ESPN source grade (industry composite input). |
| `espn_rank` | Int64 | Player's rank in the ESPN industry ranking, as tracked by 247Sports. |
| `espn_index` | Float64 | ESPN index value for the player, as tracked by 247Sports. |
| `composite_strength` | Int64 | Composite strength points (team-ranking weight). |
| `composite_rating` | Float64 | 247Sports Composite rating (industry blend). |
| `composite_rating_or_default` | Float64 | 247Sports Composite rating, falling back to a default value when unrated. |
| `average_rank` | Float64 | Player's average rank across the tracked industry services. |
| `previous_recruitment` | Int64 | Nested record for the player's previous recruitment (stringified). |
| `primary` | String | Whether this is the player's primary sport. |
| `class_year_override` | String | Override of the player's recruiting class year, when 247Sports reassigns it. |
| `class_year` | String | Recruiting class year. |
| `recruitment` | Int64 | FK -> Recruitment aggregate for this player-sport. |
| `primary_institution_prediction` | Float64 | Nested leading Crystal Ball institution prediction for the player (stringified). |
| `secondary_institution_prediction` | Int64 | Nested second-place Crystal Ball institution prediction (stringified). |
| `primary_institution_prediction_percentage` | Float64 | Share of Crystal Ball predictions favoring the leading institution. |
| `show_unranked_rating` | String | 247Sports display flag to show the rating even while the player is unranked. |
| `current_player_sport_year` | Float64 | Current ranking-cycle year for the player-sport profile. |
| `unpublished_player_sport_ranking` | Float64 | Nested not-yet-published ranking row for the player (stringified). |
| `current_player_sport_ranking` | Float64 | Nested current published ranking row for the player (stringified). |
| `primary_player_position` | Int64 | Nested 247Sports record for the player's primary position (stringified). |
| `primary_position` | Int64 | Player's primary position on the 247Sports profile. |
| `primary_position_group` | Int64 | Position group the player's primary position belongs to. |
| `default_name` | String | Server-rendered display label for the entity. |
| `star_rating` | Int64 | Star tier (2-5). |
| `secondary_institution_prediction_percentage` | Float64 | Share of Crystal Ball predictions favoring the second-place institution. |
| `jersey` | String | Jersey number. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_site_pages_recruitment_final_choice-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_site_pages_recruitment_final_choice-example}

```python
sports247_site_pages_recruitment_final_choice(key=114978)
```

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
| `key` | Int64 | Primary key of this entity (the id used in its `.json` route). |
| `name` | String | Position name (e.g. `Quarterback`). |
| `type` | String | Institution type code (college / pro / high school). |
| `group` | String | Institution group (division/level) bitmask code. |
| `location` | Int64 | FK -> Location (`/Institution/{Location}/Location.json`). |
| `state` | Int64 | FK -> State entity. |
| `latitude` | Float64 | Venue latitude in decimal degrees. |
| `longitude` | Float64 | Venue longitude in decimal degrees. |
| `rankable` | String | Whether the institution participates in class rankings. |
| `mascot` | String | Team mascot. |
| `abbreviation` | String | Metric abbreviation. |
| `primary_color` | String |  |
| `secondary_color` | String |  |
| `is_foreign` | String | Whether the institution is located outside the United States. |
| `site` | Int64 | FK -> team Site (network site key). |
| `default_asset` | Float64 | Nested 247Sports image asset for the institution's primary logo (stringified). |
| `alternate_asset` | Float64 | Nested 247Sports image asset for the institution's alternate logo (stringified). |
| `light_asset` | Float64 | Nested 247Sports image asset for the light-background logo variant (stringified). |
| `default_name` | String | Server-rendered display label for the entity. |
| `address` | String | Institution's street address. |
| `telephone` | String | Institution's telephone number. |
| `website` | String | Institution's website URL. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_site_pages_recruitment_institution-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_site_pages_recruitment_institution-example}

```python
sports247_site_pages_recruitment_institution(key=114978)
```

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
| `key` | Int64 | Primary key of this entity (the id used in its `.json` route). |
| `name` | String | Position name (e.g. `Quarterback`). |
| `type` | String | Institution type code (college / pro / high school). |
| `group` | String | Institution group (division/level) bitmask code. |
| `location` | Int64 | FK -> Location (`/Institution/{Location}/Location.json`). |
| `state` | Int64 | FK -> State entity. |
| `latitude` | Float64 | Venue latitude in decimal degrees. |
| `longitude` | Float64 | Venue longitude in decimal degrees. |
| `rankable` | String | Whether the institution participates in class rankings. |
| `mascot` | String | Team mascot. |
| `abbreviation` | String | Metric abbreviation. |
| `primary_color` | String |  |
| `secondary_color` | String |  |
| `is_foreign` | String | Whether the institution is located outside the United States. |
| `site` | Int64 | FK -> team Site (network site key). |
| `default_asset` | Float64 | Nested 247Sports image asset for the institution's primary logo (stringified). |
| `alternate_asset` | Float64 | Nested 247Sports image asset for the institution's alternate logo (stringified). |
| `light_asset` | Float64 | Nested 247Sports image asset for the light-background logo variant (stringified). |
| `default_name` | String | Server-rendered display label for the entity. |
| `address` | String | Institution's street address. |
| `telephone` | String | Institution's telephone number. |
| `website` | String | Institution's website URL. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_site_pages_recruitment_interests-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_site_pages_recruitment_interests-example}

```python
sports247_site_pages_recruitment_interests(key=114978)
```

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
| `key` | Int64 | Primary key of this entity (the id used in its `.json` route). |
| `name` | String | Position name (e.g. `Quarterback`). |
| `type` | String | Institution type code (college / pro / high school). |
| `group` | String | Institution group (division/level) bitmask code. |
| `location` | Int64 | FK -> Location (`/Institution/{Location}/Location.json`). |
| `state` | Int64 | FK -> State entity. |
| `latitude` | Float64 | Venue latitude in decimal degrees. |
| `longitude` | Float64 | Venue longitude in decimal degrees. |
| `rankable` | String | Whether the institution participates in class rankings. |
| `mascot` | String | Team mascot. |
| `abbreviation` | String | Metric abbreviation. |
| `primary_color` | String |  |
| `secondary_color` | String |  |
| `is_foreign` | String | Whether the institution is located outside the United States. |
| `site` | Int64 | FK -> team Site (network site key). |
| `default_asset` | Float64 | Nested 247Sports image asset for the institution's primary logo (stringified). |
| `alternate_asset` | Float64 | Nested 247Sports image asset for the institution's alternate logo (stringified). |
| `light_asset` | Float64 | Nested 247Sports image asset for the light-background logo variant (stringified). |
| `default_name` | String | Server-rendered display label for the entity. |
| `address` | String | Institution's street address. |
| `telephone` | String | Institution's telephone number. |
| `website` | String | Institution's website URL. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_site_pages_recruitment_offers-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_site_pages_recruitment_offers-example}

```python
sports247_site_pages_recruitment_offers(key=114978)
```

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
| `key` | Int64 | Primary key of this entity (the id used in its `.json` route). |
| `player` | Int64 | Player name. |
| `player_institution` | Int64 | Nested player-institution stint the player-sport profile points to (stringified). |
| `state` | Int64 | Home state of the recruit, per 247Sports. |
| `sport` | Int64 | Nested 247Sports sport for the profile (stringified). |
| `rating` | Int64 | 247Sports rating string (0-1). |
| `rating_or_default` | Int64 | 247Sports in-house rating, falling back to a default value when unrated. |
| `local_index` | Int64 | 247Sports' own industry-index value for the player, alongside the Rivals and ESPN indexes. |
| `rivals_grade` | Float64 | Rivals source grade (industry composite input). |
| `rivals_rank` | Int64 | Player's rank in the Rivals industry ranking, as tracked by 247Sports. |
| `rivals_index` | Float64 | Rivals index value for the player, as tracked by 247Sports. |
| `espn_grade` | Int64 | ESPN source grade (industry composite input). |
| `espn_rank` | Int64 | Player's rank in the ESPN industry ranking, as tracked by 247Sports. |
| `espn_index` | Float64 | ESPN index value for the player, as tracked by 247Sports. |
| `composite_strength` | Int64 | Composite strength points (team-ranking weight). |
| `composite_rating` | Float64 | 247Sports Composite rating (industry blend). |
| `composite_rating_or_default` | Float64 | 247Sports Composite rating, falling back to a default value when unrated. |
| `average_rank` | Float64 | Player's average rank across the tracked industry services. |
| `previous_recruitment` | Int64 | Nested record for the player's previous recruitment (stringified). |
| `primary` | String | Whether this is the player's primary sport. |
| `class_year_override` | String | Override of the player's recruiting class year, when 247Sports reassigns it. |
| `class_year` | String | Recruiting class year. |
| `recruitment` | Int64 | FK -> Recruitment aggregate for this player-sport. |
| `primary_institution_prediction` | Float64 | Nested leading Crystal Ball institution prediction for the player (stringified). |
| `secondary_institution_prediction` | Int64 | Nested second-place Crystal Ball institution prediction (stringified). |
| `primary_institution_prediction_percentage` | Float64 | Share of Crystal Ball predictions favoring the leading institution. |
| `show_unranked_rating` | String | 247Sports display flag to show the rating even while the player is unranked. |
| `current_player_sport_year` | Float64 | Current ranking-cycle year for the player-sport profile. |
| `unpublished_player_sport_ranking` | Float64 | Nested not-yet-published ranking row for the player (stringified). |
| `current_player_sport_ranking` | Float64 | Nested current published ranking row for the player (stringified). |
| `primary_player_position` | Int64 | Nested 247Sports record for the player's primary position (stringified). |
| `primary_position` | Int64 | Player's primary position on the 247Sports profile. |
| `primary_position_group` | Int64 | Position group the player's primary position belongs to. |
| `default_name` | String | Server-rendered display label for the entity. |
| `star_rating` | Int64 | Star tier (2-5). |
| `secondary_institution_prediction_percentage` | Float64 | Share of Crystal Ball predictions favoring the second-place institution. |
| `jersey` | String | Jersey number. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_site_pages_recruitment_player_sport-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_site_pages_recruitment_player_sport-example}

```python
sports247_site_pages_recruitment_player_sport(key=114978)
```
