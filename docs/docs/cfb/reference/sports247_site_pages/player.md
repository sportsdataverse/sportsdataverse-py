---
title: "College football — 247Sports Site Pages (247sports.com) — Player"
sidebar_label: "Player"
sidebar_position: 2
description: "College football — 247Sports Site Pages (247sports.com) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# College football — 247Sports Site Pages (247sports.com) — Player

## sports247_site_pages_player

Player detail (identity + primary-sport rating/ranks).

**Endpoint URL:** `GET https://247sports.com/Player/{key}.json`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_player-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | Int64 | Primary key of this entity (the id used in its `.json` route). |
| `first_name` | String | Athlete first name. |
| `last_name` | String | Athlete last name. |
| `full_name` | String | Venue full name (e.g. `Tenney Stadium`). |
| `height` | String | Listed height (inches). |
| `weight` | Float64 | Listed weight (lbs). |
| `bio` | String | Player biography text authored on 247Sports. |
| `scout_evaluation` | String | 247Sports scouting evaluation text for the player. |
| `birthdate` | String |  |
| `modified_user` | String | 247Sports user who last modified the player record. |
| `modified_date` | String | Date the player record was last modified. |
| `cbs_key` | Int64 | Cross-reference key into the CBS Sports id space. |
| `url` | String |  |
| `last_recruitment_player_institution` | Int64 | Nested player-institution record from the player's most recent recruitment (stringified). |
| `current_player_institution` | Int64 | FK -> PlayerInstitution (current school). |
| `twitter_contact` | Int64 | Nested 247Sports contact record for the player's Twitter/X account (stringified). |
| `mobile_phone_contact` | String | Player's mobile phone contact field on the 247Sports record. |
| `primary_player_sport` | Int64 | FK -> PlayerSport (`/PlayerSport/{id}.json`). |
| `primary_recruitment` | Int64 | Nested 247Sports record for the player's primary recruitment (stringified). |
| `default_name` | String | Server-rendered display label for the entity. |
| `default_asset` | Int64 | Nested 247Sports image asset for the player's headshot (stringified). |
| `default_asset_url` | String | URL of the player's headshot image. |
| `hero_asset` | String | Nested 247Sports hero (banner) image asset for the player page (stringified). |
| `quote_asset` | String | Nested 247Sports image asset used alongside the player's quote block (stringified). |
| `user` | String | 247Sports user account linked to the player profile (nested, stringified). |
| `pro_stat_player` | Int64 | Reference tying the profile to a professional stats player record (247Sports field). |
| `college_stat_player` | Int64 | Reference tying the profile to a college stats player record (247Sports field). |
| `bio_or_default` | String | Player bio text, falling back to a default blurb when none is authored. |
| `rating` | Int64 | 247Sports numeric rating (0-1 scale) for the primary sport. |
| `star_rating` | Int64 | Star tier (2-5) derived from the rating. |
| `national_rank` | Int64 | Overall national rank in the recruit's class. |
| `position_rank` | Int64 | Rank within position for the class. |
| `state_rank` | Int64 | Rank within home state for the class. |
| `hometown_state` | String | Recruit hometown state. |
| `hometown_city` | String | Recruit hometown city. |
| `player_high_school_name` | String | Name of the player's high school. |
| `primary_player_position_abbreviation` | String | Abbreviation of the player's primary position. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_site_pages_player-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_site_pages_player-example}

```python
sports247_site_pages_player()
```

## sports247_site_pages_player_current_institution

Player's current PlayerInstitution (committed/enrolled school).

**Endpoint URL:** `GET https://247sports.com/Player/{key}/CurrentPlayerInstitution.json`

**Valid URL:** [https://247sports.com/Player/46083769/CurrentPlayerInstitution.json](https://247sports.com/Player/46083769/CurrentPlayerInstitution.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_player_current_institution-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | Int64 | Primary key of this entity (the id used in its `.json` route). |
| `player` | Int64 | Player name. |
| `institution` | Int64 | Nested 247Sports institution for the stint (stringified). |
| `state` | Int64 | Nested 247Sports state record for the institution's location (stringified). |
| `agent` | String |  |
| `end_year` | Int64 | Span ending year. |
| `end_date` | String | Season end timestamp (ISO 8601, UTC). |
| `early_enrollee` | String | Whether the player enrolled early at the institution. |
| `early_signee` | String | Whether the player signed in the early signing period. |
| `height` | Float64 | Listed height (inches). |
| `weight` | Float64 | Listed weight (lbs). |
| `transfer_institution` | String | Nested institution involved in the player's transfer, for transfer-portal stints (stringified). |
| `transfer_season` | String | Season of the player's transfer, when applicable. |
| `transfer_eligibility` | String | Player's eligibility status for the transfer, per 247Sports. |
| `created_date` | String | Date the player-institution record was created. |
| `modified_date` | String | Date the player-institution record was last modified. |
| `lead_expert` | Int64 | 247Sports expert assigned as the lead on the recruitment (nested, stringified). |
| `player_institution_evaluation` | Int64 | Nested 247Sports evaluation attached to this player-institution stint (stringified). |
| `primary_player_sport` | Int64 | Nested 247Sports player-sport profile the stint belongs to (stringified). |
| `default_asset` | Int64 | Nested 247Sports image asset for the stint (stringified). |
| `hero_asset` | String | Nested 247Sports hero (banner) image asset for the stint (stringified). |
| `primary_recruitment` | Int64 | Nested 247Sports record for the recruitment behind the stint (stringified). |
| `default_name` | String | Server-rendered display label for the entity. |
| `end_year_or_current` | Int64 | Stint's end year, or the current year for an active stint. |
| `start_year_or_expected` | Int64 | Stint's start year, or the expected start year for a future stint. |
| `end_year_or_expected` | Int64 | Stint's end year, or the expected end year for an active stint. |
| `next_institution_type` | String | Level of the player's next institution (e.g. college, professional), per 247Sports. |
| `next_institution_group` | String | Grouping (e.g. conference/division) of the player's next institution, per 247Sports. |
| `start_year` | Int64 | Span starting year. |
| `start_date` | String | Season start timestamp (ISO 8601, UTC). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_site_pages_player_current_institution-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_site_pages_player_current_institution-example}

```python
sports247_site_pages_player_current_institution(key=46083769)
```

## sports247_site_pages_player_high_school

Player's high-school PlayerInstitution row.

**Endpoint URL:** `GET https://247sports.com/Player/{key}/PlayerHighSchool.json`

**Valid URL:** [https://247sports.com/Player/46051367/PlayerHighSchool.json](https://247sports.com/Player/46051367/PlayerHighSchool.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_player_high_school-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | Int64 | Primary key of this entity (the id used in its `.json` route). |
| `player` | Int64 | Player name. |
| `institution` | Int64 | Nested 247Sports institution for the stint (stringified). |
| `state` | Int64 | Nested 247Sports state record for the institution's location (stringified). |
| `agent` | String |  |
| `end_year` | Int64 | Span ending year. |
| `end_date` | String | Season end timestamp (ISO 8601, UTC). |
| `early_enrollee` | String | Whether the player enrolled early at the institution. |
| `early_signee` | String | Whether the player signed in the early signing period. |
| `height` | Float64 | Listed height (inches). |
| `weight` | Float64 | Listed weight (lbs). |
| `transfer_institution` | String | Nested institution involved in the player's transfer, for transfer-portal stints (stringified). |
| `transfer_season` | String | Season of the player's transfer, when applicable. |
| `transfer_eligibility` | String | Player's eligibility status for the transfer, per 247Sports. |
| `created_date` | String | Date the player-institution record was created. |
| `modified_date` | String | Date the player-institution record was last modified. |
| `lead_expert` | Int64 | 247Sports expert assigned as the lead on the recruitment (nested, stringified). |
| `player_institution_evaluation` | Int64 | Nested 247Sports evaluation attached to this player-institution stint (stringified). |
| `primary_player_sport` | Int64 | Nested 247Sports player-sport profile the stint belongs to (stringified). |
| `default_asset` | Int64 | Nested 247Sports image asset for the stint (stringified). |
| `hero_asset` | String | Nested 247Sports hero (banner) image asset for the stint (stringified). |
| `primary_recruitment` | Int64 | Nested 247Sports record for the recruitment behind the stint (stringified). |
| `default_name` | String | Server-rendered display label for the entity. |
| `end_year_or_current` | Int64 | Stint's end year, or the current year for an active stint. |
| `start_year_or_expected` | Int64 | Stint's start year, or the expected start year for a future stint. |
| `end_year_or_expected` | Int64 | Stint's end year, or the expected end year for an active stint. |
| `next_institution_type` | String | Level of the player's next institution (e.g. college, professional), per 247Sports. |
| `next_institution_group` | String | Grouping (e.g. conference/division) of the player's next institution, per 247Sports. |
| `start_year` | Int64 | Span starting year. |
| `start_date` | String | Season start timestamp (ISO 8601, UTC). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_site_pages_player_high_school-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_site_pages_player_high_school-example}

```python
sports247_site_pages_player_high_school(key=46051367)
```

## sports247_site_pages_player_institution

Player-at-institution association detail.

**Endpoint URL:** `GET https://247sports.com/PlayerInstitution/{key}.json`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_player_institution-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | Int64 | Primary key of this entity (the id used in its `.json` route). |
| `player` | Int64 | Player name. |
| `institution` | Int64 | Nested 247Sports institution for the stint (stringified). |
| `state` | Int64 | Nested 247Sports state record for the institution's location (stringified). |
| `agent` | String |  |
| `end_year` | Int64 | Span ending year. |
| `end_date` | String | Season end timestamp (ISO 8601, UTC). |
| `early_enrollee` | String | Whether the player enrolled early at the institution. |
| `early_signee` | String | Whether the player signed in the early signing period. |
| `height` | Float64 | Listed height (inches). |
| `weight` | Float64 | Listed weight (lbs). |
| `transfer_institution` | String | Nested institution involved in the player's transfer, for transfer-portal stints (stringified). |
| `transfer_season` | String | Season of the player's transfer, when applicable. |
| `transfer_eligibility` | String | Player's eligibility status for the transfer, per 247Sports. |
| `created_date` | String | Date the player-institution record was created. |
| `modified_date` | String | Date the player-institution record was last modified. |
| `lead_expert` | Int64 | 247Sports expert assigned as the lead on the recruitment (nested, stringified). |
| `player_institution_evaluation` | Int64 | Nested 247Sports evaluation attached to this player-institution stint (stringified). |
| `primary_player_sport` | Int64 | Nested 247Sports player-sport profile the stint belongs to (stringified). |
| `default_asset` | Int64 | Nested 247Sports image asset for the stint (stringified). |
| `hero_asset` | String | Nested 247Sports hero (banner) image asset for the stint (stringified). |
| `primary_recruitment` | Int64 | Nested 247Sports record for the recruitment behind the stint (stringified). |
| `default_name` | String | Server-rendered display label for the entity. |
| `end_year_or_current` | Int64 | Stint's end year, or the current year for an active stint. |
| `start_year_or_expected` | Int64 | Stint's start year, or the expected start year for a future stint. |
| `end_year_or_expected` | Int64 | Stint's end year, or the expected end year for an active stint. |
| `next_institution_type` | String | Level of the player's next institution (e.g. college, professional), per 247Sports. |
| `next_institution_group` | String | Grouping (e.g. conference/division) of the player's next institution, per 247Sports. |
| `start_year` | Int64 | Span starting year. |
| `start_date` | String | Season start timestamp (ISO 8601, UTC). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_site_pages_player_institution-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_site_pages_player_institution-example}

```python
sports247_site_pages_player_institution()
```

## sports247_site_pages_player_institution_evaluation

Scout evaluation of a player-institution fit.

**Endpoint URL:** `GET https://247sports.com/PlayerInstitutionEvaluation/{key}.json`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_player_institution_evaluation-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | Int64 | Primary key of this entity (the id used in its `.json` route). |
| `player_institution` | Int64 | Nested player-institution stint the evaluation is attached to (stringified). |
| `user` | Int64 | 247Sports user account of the evaluator (nested, stringified). |
| `evaluated_date` | String | Date the evaluation was written. |
| `comparison_player` | Int64 | Established player the evaluator compares the prospect to. |
| `projection` | String | Evaluator's projection for the player (e.g. draft round or college level). |
| `primary` | String | Whether this is the primary (featured) evaluation for the stint. |
| `scout_evaluation` | String | Full text of the 247Sports scouting evaluation. |
| `event` | String | Binary flag indicating the row is a counted game event (excludes end markers). |
| `default_name` | Int64 | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_site_pages_player_institution_evaluation-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_site_pages_player_institution_evaluation-example}

```python
sports247_site_pages_player_institution_evaluation()
```

## sports247_site_pages_player_primary_sport

Player's primary PlayerSport (rating/class/positions).

**Endpoint URL:** `GET https://247sports.com/Player/{key}/PrimaryPlayerSport.json`

**Valid URL:** [https://247sports.com/Player/46051367/PrimaryPlayerSport.json](https://247sports.com/Player/46051367/PrimaryPlayerSport.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_player_primary_sport-returns}

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

### Raises {#sports247_site_pages_player_primary_sport-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_site_pages_player_primary_sport-example}

```python
sports247_site_pages_player_primary_sport(key=46051367)
```

## sports247_site_pages_player_search

Player name search.

**Endpoint URL:** `GET https://247sports.com/Player.json`

**Valid URL:** [https://247sports.com/Player.json](https://247sports.com/Player.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `FirstName` | `first_name` |  |  | `Y` | FirstName query parameter. |
| `LastName` | `last_name` |  |  | `Y` | LastName query parameter. |

### Returns {#sports247_site_pages_player_search-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | Int64 | Primary key of this entity (the id used in its `.json` route). |
| `first_name` | String | Athlete first name. |
| `last_name` | String | Athlete last name. |
| `full_name` | String | Venue full name (e.g. `Tenney Stadium`). |
| `height` | String | Listed height (inches). |
| `weight` | Float64 | Listed weight (lbs). |
| `bio` | String | Player biography text authored on 247Sports. |
| `scout_evaluation` | String | 247Sports scouting evaluation text for the player. |
| `birthdate` | String |  |
| `modified_user` | String | 247Sports user who last modified the player record. |
| `modified_date` | String | Date the player record was last modified. |
| `cbs_key` | Int64 | Cross-reference key into the CBS Sports id space. |
| `url` | String |  |
| `last_recruitment_player_institution` | Int64 | Nested player-institution record from the player's most recent recruitment (stringified). |
| `current_player_institution` | Int64 | FK -> PlayerInstitution (current school). |
| `twitter_contact` | Int64 | Nested 247Sports contact record for the player's Twitter/X account (stringified). |
| `mobile_phone_contact` | String | Player's mobile phone contact field on the 247Sports record. |
| `primary_player_sport` | Int64 | FK -> PlayerSport (`/PlayerSport/{id}.json`). |
| `primary_recruitment` | Int64 | Nested 247Sports record for the player's primary recruitment (stringified). |
| `default_name` | String | Server-rendered display label for the entity. |
| `default_asset` | Int64 | Nested 247Sports image asset for the player's headshot (stringified). |
| `default_asset_url` | String | URL of the player's headshot image. |
| `hero_asset` | String | Nested 247Sports hero (banner) image asset for the player page (stringified). |
| `quote_asset` | String | Nested 247Sports image asset used alongside the player's quote block (stringified). |
| `user` | String | 247Sports user account linked to the player profile (nested, stringified). |
| `pro_stat_player` | Int64 | Reference tying the profile to a professional stats player record (247Sports field). |
| `college_stat_player` | Int64 | Reference tying the profile to a college stats player record (247Sports field). |
| `bio_or_default` | String | Player bio text, falling back to a default blurb when none is authored. |
| `rating` | Int64 | 247Sports numeric rating (0-1 scale) for the primary sport. |
| `star_rating` | Int64 | Star tier (2-5) derived from the rating. |
| `national_rank` | Int64 | Overall national rank in the recruit's class. |
| `position_rank` | Int64 | Rank within position for the class. |
| `state_rank` | Int64 | Rank within home state for the class. |
| `hometown_state` | String | Recruit hometown state. |
| `hometown_city` | String | Recruit hometown city. |
| `player_high_school_name` | String | Name of the player's high school. |
| `primary_player_position_abbreviation` | String | Abbreviation of the player's primary position. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_site_pages_player_search-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_site_pages_player_search-example}

```python
sports247_site_pages_player_search()
```

## sports247_site_pages_playersport

PlayerSport detail (note lowercase route segment).

**Endpoint URL:** `GET https://247sports.com/playersport/{key}.json`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_playersport-returns}

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

### Raises {#sports247_site_pages_playersport-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_site_pages_playersport-example}

```python
sports247_site_pages_playersport()
```
