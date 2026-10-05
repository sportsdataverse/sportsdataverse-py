---
title: "CFB — 247Sports Site Pages (247sports.com) — Player"
sidebar_label: "Player"
sidebar_position: 2
description: "CFB — 247Sports Site Pages (247sports.com) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB — 247Sports Site Pages (247sports.com) — Player

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
| `twitter_contact` | integer | Nested 247Sports contact record for the player's Twitter/X account (stringified). |
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
| `hometown_state` | character | Recruit hometown state. |
| `hometown_city` | character | Recruit hometown city. |
| `player_high_school_name` | character | Name of the player's high school. |
| `primary_player_position_abbreviation` | character | Abbreviation of the player's primary position. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_player-example}

```python
sports247_site_pages_player()
```

_Last validated n/a._

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
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player` | integer | Player name. |
| `institution` | integer | Nested 247Sports institution for the stint (stringified). |
| `state` | integer | Nested 247Sports state record for the institution's location (stringified). |
| `agent` | character | Listed player agent. |
| `end_year` | integer | Span ending year. |
| `end_date` | character | Season end timestamp (ISO 8601, UTC). |
| `early_enrollee` | character | Whether the player enrolled early at the institution. |
| `early_signee` | character | Whether the player signed in the early signing period. |
| `height` | numeric | Listed height (inches). |
| `weight` | numeric | Listed weight (lbs). |
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
| `end_year_or_current` | integer | Stint's end year, or the current year for an active stint. |
| `start_year_or_expected` | integer | Stint's start year, or the expected start year for a future stint. |
| `end_year_or_expected` | integer | Stint's end year, or the expected end year for an active stint. |
| `next_institution_type` | character | Level of the player's next institution (e.g. college, professional), per 247Sports. |
| `next_institution_group` | character | Grouping (e.g. conference/division) of the player's next institution, per 247Sports. |
| `start_year` | integer | Span starting year. |
| `start_date` | character | Season start timestamp (ISO 8601, UTC). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_player_current_institution-example}

```python
sports247_site_pages_player_current_institution(key=46083769)
```

_Last validated n/a._

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
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player` | integer | Player name. |
| `institution` | integer | Nested 247Sports institution for the stint (stringified). |
| `state` | integer | Nested 247Sports state record for the institution's location (stringified). |
| `agent` | character | Listed player agent. |
| `end_year` | integer | Span ending year. |
| `end_date` | character | Season end timestamp (ISO 8601, UTC). |
| `early_enrollee` | character | Whether the player enrolled early at the institution. |
| `early_signee` | character | Whether the player signed in the early signing period. |
| `height` | numeric | Listed height (inches). |
| `weight` | numeric | Listed weight (lbs). |
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
| `end_year_or_current` | integer | Stint's end year, or the current year for an active stint. |
| `start_year_or_expected` | integer | Stint's start year, or the expected start year for a future stint. |
| `end_year_or_expected` | integer | Stint's end year, or the expected end year for an active stint. |
| `next_institution_type` | character | Level of the player's next institution (e.g. college, professional), per 247Sports. |
| `next_institution_group` | character | Grouping (e.g. conference/division) of the player's next institution, per 247Sports. |
| `start_year` | integer | Span starting year. |
| `start_date` | character | Season start timestamp (ISO 8601, UTC). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_player_high_school-example}

```python
sports247_site_pages_player_high_school(key=46051367)
```

_Last validated n/a._

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
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player` | integer | Player name. |
| `institution` | integer | Nested 247Sports institution for the stint (stringified). |
| `state` | integer | Nested 247Sports state record for the institution's location (stringified). |
| `agent` | character | Listed player agent. |
| `end_year` | integer | Span ending year. |
| `end_date` | character | Season end timestamp (ISO 8601, UTC). |
| `early_enrollee` | character | Whether the player enrolled early at the institution. |
| `early_signee` | character | Whether the player signed in the early signing period. |
| `height` | numeric | Listed height (inches). |
| `weight` | numeric | Listed weight (lbs). |
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
| `end_year_or_current` | integer | Stint's end year, or the current year for an active stint. |
| `start_year_or_expected` | integer | Stint's start year, or the expected start year for a future stint. |
| `end_year_or_expected` | integer | Stint's end year, or the expected end year for an active stint. |
| `next_institution_type` | character | Level of the player's next institution (e.g. college, professional), per 247Sports. |
| `next_institution_group` | character | Grouping (e.g. conference/division) of the player's next institution, per 247Sports. |
| `start_year` | integer | Span starting year. |
| `start_date` | character | Season start timestamp (ISO 8601, UTC). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_player_institution-example}

```python
sports247_site_pages_player_institution()
```

_Last validated n/a._

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
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player_institution` | integer | Nested player-institution stint the evaluation is attached to (stringified). |
| `user` | integer | 247Sports user account of the evaluator (nested, stringified). |
| `evaluated_date` | character | Date the evaluation was written. |
| `comparison_player` | integer | Established player the evaluator compares the prospect to. |
| `projection` | character | Evaluator's projection for the player (e.g. draft round or college level). |
| `primary` | character | Whether this is the primary (featured) evaluation for the stint. |
| `scout_evaluation` | character | Full text of the 247Sports scouting evaluation. |
| `event` | character | Binary flag indicating the row is a counted game event (excludes end markers). |
| `default_name` | integer | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_player_institution_evaluation-example}

```python
sports247_site_pages_player_institution_evaluation()
```

_Last validated n/a._

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

### Example {#sports247_site_pages_player_primary_sport-example}

```python
sports247_site_pages_player_primary_sport(key=46051367)
```

_Last validated n/a._

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
| `twitter_contact` | integer | Nested 247Sports contact record for the player's Twitter/X account (stringified). |
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
| `hometown_state` | character | Recruit hometown state. |
| `hometown_city` | character | Recruit hometown city. |
| `player_high_school_name` | character | Name of the player's high school. |
| `primary_player_position_abbreviation` | character | Abbreviation of the player's primary position. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_player_search-example}

```python
sports247_site_pages_player_search()
```

_Last validated n/a._

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

### Example {#sports247_site_pages_playersport-example}

```python
sports247_site_pages_playersport()
```

_Last validated n/a._
