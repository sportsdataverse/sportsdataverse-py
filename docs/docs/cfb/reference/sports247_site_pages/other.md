---
title: "CFB — 247Sports Site Pages (247sports.com) — Other"
sidebar_label: "Other"
sidebar_position: 5
description: "CFB — 247Sports Site Pages (247sports.com) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB — 247Sports Site Pages (247sports.com) — Other

## sports247_site_pages_coach

Coach identity detail.

**Endpoint URL:** `GET https://247sports.com/Coach/{key}.json`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_coach-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `first_name` | character | Athlete first name. |
| `last_name` | character | Athlete last name. |
| `full_name` | character | Venue full name (e.g. `Tenney Stadium`). |
| `birthdate` | character |  |
| `hometown` | integer |  |
| `alma_mater` | integer | School the coach graduated from, per 247Sports. |
| `cbs_key` | integer | CBS Sports identifier for the coach (247Sports is a CBS Sports property). |
| `twitter_contact` | character | Coach's Twitter/X handle on the 247Sports profile. |
| `predictions_locked` | character | 247Sports flag that Crystal Ball prediction entries tied to the coach are locked. |
| `primary_coach_job` | integer | Nested 247Sports record for the coach's current job (stringified). |
| `default_asset` | integer | Nested 247Sports image asset for the coach's headshot (stringified). |
| `hero_asset` | character | Nested 247Sports hero (banner) image asset for the coach page (stringified). |
| `quote_asset` | character | Nested 247Sports image asset used alongside the coach's quote block (stringified). |
| `default_name` | character | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_coach-example}

```python
sports247_site_pages_coach()
```

_Last validated n/a._

## sports247_site_pages_event

Recruiting event detail (camp/combine/regional).

**Endpoint URL:** `GET https://247sports.com/Event/{slug}.json`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `slug` | `slug` |  | `Y` |  | slug path parameter. |

### Returns {#sports247_site_pages_event-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `name` | character | Position name (e.g. `Quarterback`). |
| `event_group` | integer | Grouping or series the event belongs to (e.g. a camp circuit) on 247Sports. |
| `event_type` | integer | Numeric code for the kind of 247Sports recruiting event on this row. |
| `event_date` | character |  |
| `default_asset` | integer | Nested 247Sports image asset for the event (stringified). |
| `primary_color` | integer |  |
| `year` | integer | Four-digit season year (e.g. 2019). |
| `default_name` | character | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_event-example}

```python
sports247_site_pages_event()
```

_Last validated n/a._

## sports247_site_pages_institution

Institution (school/team) detail.

**Endpoint URL:** `GET https://247sports.com/Institution/{key}.json`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_institution-returns}

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
| `primary_color` | character |  |
| `secondary_color` | character |  |
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

### Example {#sports247_site_pages_institution-example}

```python
sports247_site_pages_institution()
```

_Last validated n/a._

## sports247_site_pages_institution_list

Institution directory (paginated list).

**Endpoint URL:** `GET https://247sports.com/Institution.json`

**Valid URL:** [https://247sports.com/Institution.json](https://247sports.com/Institution.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `items` | `items` |  |  | `Y` | items query parameter. |

### Returns {#sports247_site_pages_institution_list-returns}

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
| `primary_color` | character |  |
| `secondary_color` | character |  |
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

### Example {#sports247_site_pages_institution_list-example}

```python
sports247_site_pages_institution_list()
```

_Last validated n/a._

## sports247_site_pages_institution_location

Institution location (city/state/coords/tax).

**Endpoint URL:** `GET https://247sports.com/Institution/{key}/Location.json`

**Valid URL:** [https://247sports.com/Institution/24099/Location.json](https://247sports.com/Institution/24099/Location.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_institution_location-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `postal_code` | integer |  |
| `city` | character | Venue city. |
| `state` | integer | U.S. state of the location record, per 247Sports. |
| `latitude` | numeric | Venue latitude in decimal degrees. |
| `longitude` | numeric | Venue longitude in decimal degrees. |
| `county_tax_rate` | numeric | County income-tax rate for the location, carried on the 247Sports location record. |
| `city_tax_rate` | numeric | City income-tax rate for the location, carried on the 247Sports location record. |
| `special_tax_rate` | numeric | Special-district tax rate for the location, carried on the 247Sports location record. |
| `region_name` | character | Name of the region (state/province) for the location. |
| `default_name` | character | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_institution_location-example}

```python
sports247_site_pages_institution_location(key=24099)
```

_Last validated n/a._

## sports247_site_pages_institution_timeline_events

Institution recruiting timeline (site-authored event blurbs).

**Endpoint URL:** `GET https://247sports.com/college/{school_slug}/Institution/{key}/TimelineEvents.json`

**Valid URL:** [https://247sports.com/college/florida/Institution/24099/TimelineEvents.json](https://247sports.com/college/florida/Institution/24099/TimelineEvents.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `school_slug` | `school_slug` |  | `Y` |  | school_slug path parameter. |
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_institution_timeline_events-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `body` | character | Text body of the timeline entry. |
| `date` | character | Date of the institution timeline entry, per 247Sports. |
| `author_first_name` | character | First name of the entry's author. |
| `author_last_name` | character | Last name of the entry's author. |
| `author_affiliation` | character | Outlet or site the author writes for, per 247Sports. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_institution_timeline_events-example}

```python
sports247_site_pages_institution_timeline_events(key=24099, school_slug='florida')
```

_Last validated n/a._

## sports247_site_pages_league_draft_picks

Pro-draft picks embed for a league/year/round.

**Endpoint URL:** `GET https://247sports.com/League/{league_slug}/DraftPicks/ConfigureEmbed/.json`

**Valid URL:** [https://247sports.com/League/NFL/DraftPicks/ConfigureEmbed/.json](https://247sports.com/League/NFL/DraftPicks/ConfigureEmbed/.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `round` | `round` |  |  | `Y` | round query parameter. |

### Returns {#sports247_site_pages_league_draft_picks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `pro_team` | integer | Nested 247Sports record for the professional team that made the pick (stringified). |
| `pro_team_name` | character | Name of the professional team that made the pick. |
| `year` | integer | Four-digit season year (e.g. 2019). |
| `round` | integer | Draft round number (1-based) the pick belongs to. |
| `pick` | integer | Pick number within the round. |
| `overall_pick` | integer | Overall selection number in the draft. |
| `player` | integer | Player name. |
| `player_first_name` | character |  |
| `player_last_name` | character |  |
| `college_team` | integer | College team name. |
| `college_team_name` | character | Name of the college the player was drafted out of. |
| `position_abbreviation` | character | Player's position at draft. |
| `traded_from_team` | character | Team the pick was traded from, when it changed hands. |
| `pick_type` | character | Type of the selection (e.g. regular, compensatory, supplemental). |
| `league` | integer |  |
| `mock` | character | Whether this is a mock-draft projection vs an actual pick. |
| `default_name` | integer | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_league_draft_picks-example}

```python
sports247_site_pages_league_draft_picks(league_slug='NFL')
```

_Last validated n/a._

## sports247_site_pages_league_institutions

Institutions belonging to a league.

**Endpoint URL:** `GET https://247sports.com/League/{league_id}/Institutions.json`

**Valid URL:** [https://247sports.com/League/6/Institutions.json](https://247sports.com/League/6/Institutions.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | league_id path parameter. |
| `items` | `items` |  |  | `Y` | items query parameter. |

### Returns {#sports247_site_pages_league_institutions-returns}

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
| `primary_color` | character |  |
| `secondary_color` | character |  |
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

### Example {#sports247_site_pages_league_institutions-example}

```python
sports247_site_pages_league_institutions(league_id=6)
```

_Last validated n/a._

## sports247_site_pages_page_feeds

News/headline feed items for a site Page.

**Endpoint URL:** `GET https://247sports.com/Page/{page_id}/Feeds.json`

**Valid URL:** [https://247sports.com/Page/100134/Feeds.json](https://247sports.com/Page/100134/Feeds.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `page_id` | `page_id` |  | `Y` |  | page_id path parameter. |

### Returns {#sports247_site_pages_page_feeds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `uid` | character | ESPN global unique identifier. |
| `update_date` | character | Date the feed item was published or updated. |
| `title_text` | character | Headline text of the feed item. |
| `main_text` | character | Body text of the feed item. |
| `redirection_url` | character | URL the feed item links out to. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_page_feeds-example}

```python
sports247_site_pages_page_feeds(page_id=100134)
```

_Last validated n/a._

## sports247_site_pages_playersport_institution

PlayerInstitution linked to a PlayerSport.

**Endpoint URL:** `GET https://247sports.com/PlayerSport/{key}/PlayerInstitution.json`

**Valid URL:** [https://247sports.com/PlayerSport/279200/PlayerInstitution.json](https://247sports.com/PlayerSport/279200/PlayerInstitution.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_playersport_institution-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `player` | integer | Player name. |
| `institution` | integer | Nested 247Sports institution for the stint (stringified). |
| `state` | integer | Nested 247Sports state record for the institution's location (stringified). |
| `agent` | character |  |
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

### Example {#sports247_site_pages_playersport_institution-example}

```python
sports247_site_pages_playersport_institution(key=279200)
```

_Last validated n/a._

## sports247_site_pages_playersport_rank_history

Ranking history for a PlayerSport (one row per Ranking snapshot).

**Endpoint URL:** `GET https://247sports.com/PlayerSport/{key}/RecruitRankHistory.json`

**Valid URL:** [https://247sports.com/PlayerSport/250563/RecruitRankHistory.json](https://247sports.com/PlayerSport/250563/RecruitRankHistory.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_playersport_rank_history-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `ranking` | integer | FK -> Ranking snapshot. |
| `sport` | integer | Nested 247Sports sport the ranking row covers (stringified). |
| `player_sport` | integer | Nested player-sport profile the ranking row belongs to (stringified). |
| `committed_institution` | integer | FK -> committed Institution (null if uncommitted). |
| `order` | integer | Display order of the entry within the 247Sports ranking list. |
| `position` | integer | Athlete position. |
| `position_group` | integer | Position group of the recruits (e.g. Offensive Line, Defensive Back). |
| `platoon` | integer | 247Sports platoon (side-of-ball grouping) identifier on the ranking row. |
| `state` | integer | Nested 247Sports state record for the recruit's home state (stringified). |
| `region` | integer | Nested 247Sports region record for the recruit's home region (stringified). |
| `institution` | integer | Nested institution the player was committed or signed to at ranking time (stringified). |
| `institution_group` | character | Grouping (e.g. conference/division) of the player's institution, per 247Sports. |
| `rating` | integer | Nested 247Sports rating record attached to the ranking row (stringified). |
| `composite_strength` | integer | 247Sports field describing the strength of the industry inputs behind the Composite rating. |
| `composite_rating` | numeric | Player's 247Sports Composite rating, blending the major services' ratings. |
| `overall_rank` | integer | Overall national rank in the snapshot. |
| `composite_overall_rank` | integer | Player's national rank by 247Sports Composite rating. |
| `group_rank` | integer |  |
| `composite_group_rank` | integer | Player's rank within their position group by Composite rating. |
| `position_rank` | integer | Rank within position. |
| `previous_player_sport_ranking` | numeric | Nested prior-cycle ranking row for the player (stringified). |
| `composite_position_rank` | integer | Player's rank at their position by Composite rating. |
| `state_rank` | integer | Rank within home state. |
| `composite_state_rank` | integer | Player's rank within their home state by Composite rating. |
| `default_name` | character | Server-rendered display label for the entity. |
| `position_group_rank` | integer | Player's rank within their position group in the 247Sports ranking. |
| `region_rank` | integer | Region ranking. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_playersport_rank_history-example}

```python
sports247_site_pages_playersport_rank_history(key=250563)
```

_Last validated n/a._

## sports247_site_pages_position_rankings

Player-sport rankings for a position.

**Endpoint URL:** `GET https://247sports.com/Position/{key}/playersportrankings.json`

**Valid URL:** [https://247sports.com/Position/14/playersportrankings.json](https://247sports.com/Position/14/playersportrankings.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_position_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `ranking` | integer | FK -> Ranking snapshot. |
| `sport` | integer | Nested 247Sports sport the ranking row covers (stringified). |
| `player_sport` | integer | Nested player-sport profile the ranking row belongs to (stringified). |
| `committed_institution` | integer | FK -> committed Institution (null if uncommitted). |
| `order` | integer | Display order of the entry within the 247Sports ranking list. |
| `position` | integer | Athlete position. |
| `position_group` | integer | Position group of the recruits (e.g. Offensive Line, Defensive Back). |
| `platoon` | integer | 247Sports platoon (side-of-ball grouping) identifier on the ranking row. |
| `state` | integer | Nested 247Sports state record for the recruit's home state (stringified). |
| `region` | integer | Nested 247Sports region record for the recruit's home region (stringified). |
| `institution` | integer | Nested institution the player was committed or signed to at ranking time (stringified). |
| `institution_group` | character | Grouping (e.g. conference/division) of the player's institution, per 247Sports. |
| `rating` | integer | Nested 247Sports rating record attached to the ranking row (stringified). |
| `composite_strength` | integer | 247Sports field describing the strength of the industry inputs behind the Composite rating. |
| `composite_rating` | numeric | Player's 247Sports Composite rating, blending the major services' ratings. |
| `overall_rank` | integer | Overall national rank in the snapshot. |
| `composite_overall_rank` | integer | Player's national rank by 247Sports Composite rating. |
| `group_rank` | integer |  |
| `composite_group_rank` | integer | Player's rank within their position group by Composite rating. |
| `position_rank` | integer | Rank within position. |
| `previous_player_sport_ranking` | numeric | Nested prior-cycle ranking row for the player (stringified). |
| `composite_position_rank` | integer | Player's rank at their position by Composite rating. |
| `state_rank` | integer | Rank within home state. |
| `composite_state_rank` | integer | Player's rank within their home state by Composite rating. |
| `default_name` | character | Server-rendered display label for the entity. |
| `position_group_rank` | integer | Player's rank within their position group in the 247Sports ranking. |
| `region_rank` | integer | Region ranking. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_position_rankings-example}

```python
sports247_site_pages_position_rankings(key=14)
```

_Last validated n/a._

## sports247_site_pages_recruit_interest

Single recruit-interest (school<->recruit link) detail.

**Endpoint URL:** `GET https://247sports.com/RecruitInterest/{key}.json`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_recruit_interest-returns}

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
| `primary_coach` | numeric | Lead recruiting coach at the school for this recruit. |
| `secondary_coach` | character | Secondary recruiting coach at the school for this recruit. |
| `keeper_coach` | character | Coach designated as the keeper contact for the recruitment (247Sports field). |
| `institutions_interest` | character | School's interest level in the recruit, per 247Sports. |
| `position` | integer | Athlete position. |
| `position_group` | integer | Position group of the recruits (e.g. Offensive Line, Defensive Back). |
| `platoon` | integer | 247Sports platoon (side-of-ball grouping) identifier on the interest entry. |
| `offered` | character | Whether the school has extended an offer. |
| `gray_shirt` | character | Whether the offer or commitment is a grayshirt (delayed enrollment) arrangement. |
| `walk_on` | character | Whether the recruit would join the program as a walk-on. |
| `official_visit` | numeric | Date of the recruit's official visit to the school. |
| `second_official_visit` | character | Date of the recruit's second official visit to the school. |
| `soft_commit` | character | Whether 247Sports marks the commitment as a soft commit. |
| `hard_commit` | numeric | FK -> the RecruitInterestEvent marking a hard commit. |
| `signing_date` | numeric | Date the recruit signed with the school. |
| `enrollment_date` | numeric | Date the recruit enrolled at the school. |
| `decommit` | character | Date the recruit decommitted from the school, when applicable. |
| `offer` | character | Whether the school has extended a scholarship offer to the recruit. |
| `highest_recruit_interest_event` | numeric | Nested highest-signal event on the interest timeline (e.g. commitment) (stringified). |
| `commit_status` | character | Commitment status label (e.g. Committed, Signed). |
| `default_name` | character | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_recruit_interest-example}

```python
sports247_site_pages_recruit_interest()
```

_Last validated n/a._
