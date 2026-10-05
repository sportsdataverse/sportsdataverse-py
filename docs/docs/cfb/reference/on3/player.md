---
title: "CFB — On3 Recruit Database (api.on3.com) — Player"
sidebar_label: "Player"
sidebar_position: 5
description: "CFB — On3 Recruit Database (api.on3.com) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB — On3 Recruit Database (api.on3.com) — Player

## on3_player_all_rankings

GET /rdb/v1/player/{personKey}/all-rankings

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{person_key}/all-rankings`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/all-rankings](https://api.on3.com/public/rdb/v1/player/89617/all-rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_player_all_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `link` | character | API link to the game feed. |
| `ranking_key` | integer | On3 key of the ranking cycle the row belongs to. |
| `ranking_year` | integer |  |
| `ranking_type` | character | Poll type code (e.g. `ap`, `coaches`, `cfp`). |
| `rating` | numeric | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `sport` | character | Nested On3 sport object for the ranking row (stringified). |
| `class_year` | integer | Recruiting class year the ranking covers. |
| `state_rank` | integer | State ranking. |
| `state_abbr` | character | Two-letter abbreviation of the player's home state. |
| `position_rank` | integer | Position ranking. |
| `position_abbr` | character | Position abbreviation. |
| `overall_rank` | integer | Overall recruit ranking (top recruits only; may be `NA`). |
| `stars` | integer | Recruit star rating on the 247Sports scale (2-5). |
| `five_star_plus` | logical | Whether On3 designates the player a Five-Star Plus+ prospect. |
| `nearly_five_star_plus` | logical | On3 flag that the player narrowly missed the Five-Star Plus+ designation. |
| `change_1` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_player_all_rankings-example}

```python
on3_player_all_rankings(person_key=89617)
```

_Last validated n/a._

## on3_player_database_updates

GET /rdb/v1/player/{personKey}/database-updates

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{person_key}/database-updates`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/database-updates](https://api.on3.com/public/rdb/v1/player/89617/database-updates)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_player_database_updates-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the database-update entry. |
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `text` | character | Full play description. |
| `replacement_text` | character | Rendered text of the update entry (with references substituted in). |
| `link` | character | API link to the game feed. |
| `date_added` | integer | Date the update entry was logged. |
| `date_occurred` | integer | Date the underlying event occurred. |
| `object_key` | integer | On3 key of the object the update refers to. |
| `sport_key` | integer | On3 sport key the update is scoped to. |
| `person_key` | integer | On3 person key of the player the update concerns. |
| `organization_key` | integer | On3 organization key involved in the update, when any. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_player_database_updates-example}

```python
on3_player_database_updates(person_key=89617)
```

_Last validated n/a._

## on3_player_images

GET /rdb/v1/player/{personKey}/images

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{person_key}/images`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/images](https://api.on3.com/public/rdb/v1/player/89617/images)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_player_images-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | integer | On3 asset key for the image. |
| `domain_override` | character | Override CDN domain for serving the image, when set. |
| `domain` | character | CDN domain the image is served from. |
| `source_override` | character | Override source attribution for the image, when set. |
| `source` | character | News source. |
| `title` | character | Specific role title for the assignment. |
| `description` | character | ESPN's description of the stat. |
| `caption` | character | Caption text for the image. |
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |
| `alt_text` | character | Alt text for the image. |
| `height` | integer | Listed height (inches). |
| `width` | integer | Image width in pixels. |
| `asset_type` | character | Type of the asset (e.g. image) in On3's asset system. |
| `file_system` | character | Storage file system the asset lives on (On3 asset metadata). |
| `path` | character | Storage path of the image file. |
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `thumbnail` | character | URL or path of the image's thumbnail rendition. |
| `duration` | integer | Duration. |
| `mime_type` | character | MIME type of the image file (e.g. image/jpeg). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_player_images-example}

```python
on3_player_images(person_key=89617)
```

_Last validated n/a._

## on3_player_organizations

GET /rdb/v1/player/{personKey}/organizations

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{person_key}/organizations`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/organizations](https://api.on3.com/public/rdb/v1/player/89617/organizations)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_player_organizations-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_player_organizations-example}

```python
on3_player_organizations(person_key=89617)
```

_Last validated n/a._

## on3_player_organizations_org_key

GET /rdb/v1/player/{playerKey}/organizations/{orgKey}

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{player_key}/organizations/{org_key}`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/organizations/1867](https://api.on3.com/public/rdb/v1/player/89617/organizations/1867)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_key` | `player_key` |  | `Y` |  | player_key path parameter. |
| `org_key` | `org_key` |  | `Y` |  | org_key path parameter. |

### Returns {#on3_player_organizations_org_key-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the row has nested objects whose flattened column names depend on which are null in the data; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_player_organizations_org_key-example}

```python
on3_player_organizations_org_key(org_key=1867, player_key=89617)
```

_Last validated n/a._

## on3_player_person_rankings

GET /rdb/v1/player/{personKey}/rankings

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{person_key}/rankings`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/rankings](https://api.on3.com/public/rdb/v1/player/89617/rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_player_person_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the player's ranking row. |
| `ranking_key` | integer | On3 key of the ranking cycle the row belongs to. |
| `rating` | integer | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `state_rank` | integer | State ranking. |
| `state_abbr` | character | Two-letter abbreviation of the player's home state. |
| `position_rank` | integer | Position ranking. |
| `position_abbr` | character | Position abbreviation. |
| `overall_rank` | integer | Overall recruit ranking (top recruits only; may be `NA`). |
| `stars` | integer | Recruit star rating on the 247Sports scale (2-5). |
| `consensus_rating` | numeric | Player's industry-consensus rating (blend of the major recruiting services). |
| `consensus_state_rank` | integer | Player's consensus rank within their home state. |
| `consensus_position_rank` | integer | Player's consensus rank at their position. |
| `consensus_overall_rank` | integer | Player's national consensus rank. |
| `consensus_stars` | integer | Player's star rating under the industry consensus. |
| `strength` | integer | Strength label (Even, Power Play, Shorthanded). |
| `five_star_plus` | logical | Whether On3 designates the player a Five-Star Plus+ prospect. |
| `ranking_type` | character | Poll type code (e.g. `ap`, `coaches`, `cfp`). |
| `ranking_key_2` | integer |  |
| `ranking_sport_key` | integer |  |
| `ranking_sport_key_2` | integer |  |
| `ranking_sport_name` | character |  |
| `ranking_year` | integer |  |
| `change_38` | character |  |
| `consensus_change_41` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_player_person_rankings-example}

```python
on3_player_person_rankings(person_key=89617)
```

_Last validated n/a._

## on3_player_profile

GET /rdb/v1/player/{personKey}/profile

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{person_key}/profile`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/profile](https://api.on3.com/public/rdb/v1/player/89617/profile)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_player_profile-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the player profile. |
| `class_year_recruitment_key` | integer | On3 recruitment key for the player's recruiting-class cycle. |
| `recruitment_key` | integer | On3 key of the player's active recruitment record. |
| `person_can_manage_recruitment` | logical | Whether the athlete can self-manage the recruitment on On3. |
| `ranking_key` | integer | On3 key of the ranking cycle the profile's rating belongs to. |
| `person_sport_key` | integer | On3 key of the athlete-sport profile (person x sport). |
| `oracle_key` | character | On3's internal oracle identifier for the player record. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `slug` | character | URL slug for the team. |
| `high_school_name` | character | Recruit high-school name. |
| `hometown_name` | character | Player's hometown, as listed by On3. |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`); `position_detail = TRUE` only. |
| `class_rank` | character | Player's rank within their recruiting class. |
| `height` | character | Listed height (inches). |
| `weight` | integer | Listed weight (lbs). |
| `class_year` | integer | Player's recruiting class year. |
| `degree` | character | Degree the player earned or is pursuing, when listed. |
| `age` | integer | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `sports` | character | Sports the player is profiled in, as a stringified list. |
| `description` | character | ESPN's description of the stat. |
| `bio_pro_prospect` | character | Bio text framing the player as a pro prospect (On3 RDB). |
| `bio_college_recruit` | character | Bio text framing the player as a college recruit (On3 RDB). |
| `organization_level` | character | Level of the player's current organization (e.g. high school, college, professional). |
| `high_school_org_key` | integer | On3 organization key of the player's high school. |
| `prep_school_org_key` | character | On3 organization key of the player's prep school, when attended. |
| `junior_college_org_key` | character | On3 organization key of the player's junior college, when attended. |
| `college_org_key` | integer | On3 organization key of the player's college. |
| `nil_value` | integer | Player's On3 NIL valuation in dollars. |
| `athlete_verified` | logical | Whether the athlete has verified their own On3 profile. |
| `prospect_verified` | logical | Whether On3 has verified the prospect's profile information. |
| `is_coach` | logical | Whether the person record is a coach. |
| `is_athlete` | logical | Whether the person record is an athlete. |
| `visibility` | character | Profile visibility setting on On3. |
| `tier` | character | On3 profile tier classification for the player. |
| `review_status` | character | Editorial review status of the profile in the On3 database. |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `badge` | character | Profile badge assigned by On3, when any. |
| `ncaa_id` | character | Player's NCAA identifier, when known to On3. |
| `managed_by_user` | logical | On3 user account that manages the player's profile, when claimed. |
| `ranking_key_2` | integer |  |
| `ranking_rating` | numeric |  |
| `ranking_stars` | integer |  |
| `ranking_national_rank` | integer |  |
| `ranking_position_rank` | integer |  |
| `ranking_state_rank` | integer |  |
| `ranking_position_abbr` | character |  |
| `ranking_state_abbr` | character |  |
| `ranking_five_star_plus` | logical |  |
| `high_school_key` | integer |  |
| `high_school_full_name` | character |  |
| `high_school_name_2` | character |  |
| `high_school_known_as` | character |  |
| `high_school_mascot` | character |  |
| `high_school_abbreviation` | character |  |
| `high_school_asset_url` | character |  |
| `high_school_default_asset_key` | integer |  |
| `high_school_default_asset_domain_override` | character |  |
| `high_school_default_asset_domain` | character |  |
| `high_school_default_asset_source_override` | character |  |
| `high_school_default_asset_source` | character |  |
| `high_school_default_asset_title` | character |  |
| `high_school_default_asset_description` | character |  |
| `high_school_default_asset_caption` | character |  |
| `high_school_default_asset_category` | character |  |
| `high_school_default_asset_alt_text` | character |  |
| `high_school_default_asset_height` | integer |  |
| `high_school_default_asset_width` | integer |  |
| `high_school_default_asset_asset_type` | character |  |
| `high_school_default_asset_file_system` | character |  |
| `high_school_default_asset_path` | character |  |
| `high_school_default_asset_type` | character |  |
| `high_school_default_asset_thumbnail` | character |  |
| `high_school_default_asset_duration` | integer |  |
| `high_school_default_asset_mime_type` | character |  |
| `high_school_slug` | character |  |
| `high_school_primary_color` | character |  |
| `high_school_org_type` | character |  |
| `high_school_org_type_enum` | character |  |
| `high_school_division` | character |  |
| `high_school_site_keys` | character |  |
| `high_school_url_slug` | character |  |
| `hometown_state_key` | integer |  |
| `hometown_state_name` | character |  |
| `hometown_state_abbreviation` | character | Recruit hometown state abbreviation. |
| `hometown_state_country_key` | integer |  |
| `current_state_key` | integer |  |
| `current_state_name` | character |  |
| `current_state_abbreviation` | character |  |
| `current_state_country_key` | integer |  |
| `default_asset_key` | integer |  |
| `default_asset_domain_override` | character |  |
| `default_asset_domain` | character |  |
| `default_asset_source_override` | character |  |
| `default_asset_source` | character |  |
| `default_asset_title` | character |  |
| `default_asset_description` | character |  |
| `default_asset_caption` | character |  |
| `default_asset_category` | character |  |
| `default_asset_alt_text` | character |  |
| `default_asset_height` | integer |  |
| `default_asset_width` | integer |  |
| `default_asset_asset_type` | character |  |
| `default_asset_file_system` | character |  |
| `default_asset_path` | character |  |
| `default_asset_type` | character |  |
| `default_asset_thumbnail` | character |  |
| `default_asset_duration` | integer |  |
| `default_asset_mime_type` | character |  |
| `primary_position_key` | integer |  |
| `primary_position_name` | character | Primary fielding position name. |
| `primary_position_abbreviation` | character | Primary position abbreviation. |
| `primary_position_sport_key` | integer |  |
| `primary_position_sport_key_2` | integer |  |
| `primary_position_sport_name` | character |  |
| `primary_position_sport_slug` | character |  |
| `primary_position_sport_abbreviation` | character |  |
| `primary_position_sport_is_rankable` | logical |  |
| `primary_position_sport_is_industry_rankable` | logical |  |
| `primary_position_sport_is_scoutable` | logical |  |
| `primary_position_position_type` | character |  |
| `default_sport_key` | integer |  |
| `default_sport_name` | character |  |
| `player_status_type` | character |  |
| `player_status_short_term_signee` | logical |  |
| `player_status_date` | character |  |
| `player_status_committed_asset_key` | integer |  |
| `player_status_committed_asset_url` | character |  |
| `player_status_committed_asset_slug` | character |  |
| `player_status_committed_asset_full_name` | character |  |
| `player_status_committed_asset_res_key` | integer |  |
| `player_status_committed_asset_res_domain_override` | character |  |
| `player_status_committed_asset_res_domain` | character |  |
| `player_status_committed_asset_res_source_override` | character |  |
| `player_status_committed_asset_res_source` | character |  |
| `player_status_committed_asset_res_title` | character |  |
| `player_status_committed_asset_res_description` | character |  |
| `player_status_committed_asset_res_caption` | character |  |
| `player_status_committed_asset_res_category` | character |  |
| `player_status_committed_asset_res_alt_text` | character |  |
| `player_status_committed_asset_res_height` | integer |  |
| `player_status_committed_asset_res_width` | integer |  |
| `player_status_committed_asset_res_asset_type` | character |  |
| `player_status_committed_asset_res_file_system` | character |  |
| `player_status_committed_asset_res_path` | character |  |
| `player_status_committed_asset_res_type` | character |  |
| `player_status_committed_asset_res_thumbnail` | character |  |
| `player_status_committed_asset_res_duration` | integer |  |
| `player_status_committed_asset_res_mime_type` | character |  |
| `player_status_transferred_asset_key` | integer |  |
| `player_status_transferred_asset_url` | character |  |
| `player_status_transferred_asset_slug` | character |  |
| `player_status_transferred_asset_full_name` | character |  |
| `player_status_transferred_asset_res_key` | integer |  |
| `player_status_transferred_asset_res_domain_override` | character |  |
| `player_status_transferred_asset_res_domain` | character |  |
| `player_status_transferred_asset_res_source_override` | character |  |
| `player_status_transferred_asset_res_source` | character |  |
| `player_status_transferred_asset_res_title` | character |  |
| `player_status_transferred_asset_res_description` | character |  |
| `player_status_transferred_asset_res_caption` | character |  |
| `player_status_transferred_asset_res_category` | character |  |
| `player_status_transferred_asset_res_alt_text` | character |  |
| `player_status_transferred_asset_res_height` | integer |  |
| `player_status_transferred_asset_res_width` | integer |  |
| `player_status_transferred_asset_res_asset_type` | character |  |
| `player_status_transferred_asset_res_file_system` | character |  |
| `player_status_transferred_asset_res_path` | character |  |
| `player_status_transferred_asset_res_type` | character |  |
| `player_status_transferred_asset_res_thumbnail` | character |  |
| `player_status_transferred_asset_res_duration` | integer |  |
| `player_status_transferred_asset_res_mime_type` | character |  |
| `player_status_committed_organization_key` | integer |  |
| `player_status_committed_organization_full_name` | character |  |
| `player_status_committed_organization_name` | character |  |
| `player_status_committed_organization_mascot` | character |  |
| `player_status_committed_organization_abbreviation` | character |  |
| `player_status_committed_organization_asset_url` | character |  |
| `player_status_committed_organization_asset_key` | integer |  |
| `player_status_committed_organization_asset_domain_override` | character |  |
| `player_status_committed_organization_asset_domain` | character |  |
| `player_status_committed_organization_asset_source_override` | character |  |
| `player_status_committed_organization_asset_source` | character |  |
| `player_status_committed_organization_asset_title` | character |  |
| `player_status_committed_organization_asset_description` | character |  |
| `player_status_committed_organization_asset_caption` | character |  |
| `player_status_committed_organization_asset_category` | character |  |
| `player_status_committed_organization_asset_alt_text` | character |  |
| `player_status_committed_organization_asset_height` | integer |  |
| `player_status_committed_organization_asset_width` | integer |  |
| `player_status_committed_organization_asset_asset_type` | character |  |
| `player_status_committed_organization_asset_file_system` | character |  |
| `player_status_committed_organization_asset_path` | character |  |
| `player_status_committed_organization_asset_type` | character |  |
| `player_status_committed_organization_asset_thumbnail` | character |  |
| `player_status_committed_organization_asset_duration` | integer |  |
| `player_status_committed_organization_asset_mime_type` | character |  |
| `player_status_committed_organization_slug` | character |  |
| `player_status_committed_organization_primary_color` | character |  |
| `player_status_class_rank` | character |  |
| `player_status_transfer_entered` | character |  |
| `player_status_recruitment_year` | character |  |
| `player_status_decommitted_asset` | character |  |
| `player_status_transfer` | logical |  |
| `player_status_expected_to_transfer` | logical |  |
| `player_status_recruitment_key` | integer |  |
| `player_status_withdrawn_transfer` | logical |  |
| `player_status_withdrawn_transfer_date` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_player_profile-example}

```python
on3_player_profile(person_key=89617)
```

_Last validated n/a._

## on3_player_team_targets

GET /rdb/v1/player/{playerKey}/team-targets

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{player_key}/team-targets`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/team-targets](https://api.on3.com/public/rdb/v1/player/89617/team-targets)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_key` | `player_key` |  | `Y` |  | player_key path parameter. |

### Returns {#on3_player_team_targets-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the row has nested objects whose flattened column names depend on which are null in the data; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_player_team_targets-example}

```python
on3_player_team_targets(player_key=89617)
```

_Last validated n/a._

## on3_player_verified

GET /rdb/v1/player/verified

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/verified`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/verified](https://api.on3.com/public/rdb/v1/player/verified)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_player_verified-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the player profile. |
| `class_year_recruitment_key` | character | On3 recruitment key for the player's recruiting-class cycle. |
| `recruitment_key` | integer | On3 key of the player's active recruitment record. |
| `person_can_manage_recruitment` | character | Whether the athlete can self-manage the recruitment on On3. |
| `ranking_key` | integer | On3 key of the ranking cycle the profile's rating belongs to. |
| `person_sport_key` | integer | On3 key of the athlete-sport profile (person x sport). |
| `ranking` | character | National rank of the team's overall SP+ rating (1 = best). |
| `oracle_key` | character | On3's internal oracle identifier for the player record. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `slug` | character | URL slug for the team. |
| `high_school_name` | character | Recruit high-school name. |
| `hometown_name` | character | Player's hometown, as listed by On3. |
| `hometown_state` | character | Recruit hometown state. |
| `current_state` | character | Current home venue state. |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`); `position_detail = TRUE` only. |
| `primary_position` | character | Nested On3 object for the player's primary position (stringified). |
| `class_rank` | character | Player's rank within their recruiting class. |
| `height` | character | Listed height (inches). |
| `weight` | integer | Listed weight (lbs). |
| `class_year` | integer | Player's recruiting class year. |
| `degree` | character | Degree the player earned or is pursuing, when listed. |
| `age` | integer | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `sports` | character | Sports the player is profiled in, as a stringified list. |
| `description` | character | ESPN's description of the stat. |
| `bio_pro_prospect` | character | Bio text framing the player as a pro prospect (On3 RDB). |
| `bio_college_recruit` | character | Bio text framing the player as a college recruit (On3 RDB). |
| `organization_level` | character | Level of the player's current organization (e.g. high school, college, professional). |
| `high_school_org_key` | numeric | On3 organization key of the player's high school. |
| `prep_school_org_key` | character | On3 organization key of the player's prep school, when attended. |
| `junior_college_org_key` | character | On3 organization key of the player's junior college, when attended. |
| `college_org_key` | character | On3 organization key of the player's college. |
| `nil_value` | integer | Player's On3 NIL valuation in dollars. |
| `athlete_verified` | logical | Whether the athlete has verified their own On3 profile. |
| `prospect_verified` | logical | Whether On3 has verified the prospect's profile information. |
| `player_status` | character | Player's current status per On3 (e.g. active, transfer portal). |
| `is_coach` | logical | Whether the person record is a coach. |
| `is_athlete` | logical | Whether the person record is an athlete. |
| `visibility` | character | Profile visibility setting on On3. |
| `tier` | character | On3 profile tier classification for the player. |
| `review_status` | character | Editorial review status of the profile in the On3 database. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `badge` | character | Profile badge assigned by On3, when any. |
| `ncaa_id` | character | Player's NCAA identifier, when known to On3. |
| `managed_by_user` | logical | On3 user account that manages the player's profile, when claimed. |
| `high_school_key` | integer |  |
| `high_school_full_name` | character |  |
| `high_school_name_2` | character |  |
| `high_school_known_as` | character |  |
| `high_school_mascot` | character |  |
| `high_school_abbreviation` | character |  |
| `high_school_asset_url` | character |  |
| `high_school_default_asset_key` | integer |  |
| `high_school_default_asset_domain_override` | character |  |
| `high_school_default_asset_domain` | character |  |
| `high_school_default_asset_source_override` | character |  |
| `high_school_default_asset_source` | character |  |
| `high_school_default_asset_title` | character |  |
| `high_school_default_asset_description` | character |  |
| `high_school_default_asset_caption` | character |  |
| `high_school_default_asset_category` | character |  |
| `high_school_default_asset_alt_text` | character |  |
| `high_school_default_asset_height` | integer |  |
| `high_school_default_asset_width` | integer |  |
| `high_school_default_asset_asset_type` | character |  |
| `high_school_default_asset_file_system` | character |  |
| `high_school_default_asset_path` | character |  |
| `high_school_default_asset_type` | character |  |
| `high_school_default_asset_thumbnail` | character |  |
| `high_school_default_asset_duration` | integer |  |
| `high_school_default_asset_mime_type` | character |  |
| `high_school_slug` | character |  |
| `high_school_primary_color` | character |  |
| `high_school_org_type` | character |  |
| `high_school_org_type_enum` | character |  |
| `high_school_division` | character |  |
| `high_school_site_keys` | character |  |
| `high_school_url_slug` | character |  |
| `default_asset_key` | integer |  |
| `default_asset_domain_override` | character |  |
| `default_asset_domain` | character |  |
| `default_asset_source_override` | character |  |
| `default_asset_source` | character |  |
| `default_asset_title` | character |  |
| `default_asset_description` | character |  |
| `default_asset_caption` | character |  |
| `default_asset_category` | character |  |
| `default_asset_alt_text` | character |  |
| `default_asset_height` | integer |  |
| `default_asset_width` | integer |  |
| `default_asset_asset_type` | character |  |
| `default_asset_file_system` | character |  |
| `default_asset_path` | character |  |
| `default_asset_type` | character |  |
| `default_asset_thumbnail` | character |  |
| `default_asset_duration` | integer |  |
| `default_asset_mime_type` | character |  |
| `default_sport_key` | integer |  |
| `default_sport_name` | character |  |
| `hometown_state_key` | numeric |  |
| `hometown_state_name` | character |  |
| `hometown_state_abbreviation` | character | Recruit hometown state abbreviation. |
| `hometown_state_country_key` | numeric |  |
| `current_state_key` | numeric |  |
| `current_state_name` | character |  |
| `current_state_abbreviation` | character |  |
| `current_state_country_key` | numeric |  |
| `primary_position_key` | numeric |  |
| `primary_position_name` | character | Primary fielding position name. |
| `primary_position_abbreviation` | character | Primary position abbreviation. |
| `primary_position_sport_key` | numeric |  |
| `primary_position_sport_key_2` | numeric |  |
| `primary_position_sport_name` | character |  |
| `primary_position_sport_slug` | character |  |
| `primary_position_sport_abbreviation` | character |  |
| `primary_position_sport_is_rankable` | character |  |
| `primary_position_sport_is_industry_rankable` | character |  |
| `primary_position_sport_is_scoutable` | character |  |
| `primary_position_position_type` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_player_verified-example}

```python
on3_player_verified()
```

_Last validated n/a._

## on3_player_videos

GET /rdb/v1/player/{personKey}/videos

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{person_key}/videos`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/videos](https://api.on3.com/public/rdb/v1/player/89617/videos)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_player_videos-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the video record. |
| `source_url` | character | Source URL of the hosted video. |
| `title` | character | Specific role title for the assignment. |
| `thumbnail` | character | URL of the video's thumbnail image. |
| `description` | character | ESPN's description of the stat. |
| `date` | integer | Publication date of the video, per On3. |
| `person_key` | integer | On3 person key of the featured athlete. |
| `person_sport` | character | Nested athlete-sport profile the video is attached to (stringified). |
| `is_featured` | logical | Whether the video is featured on the player's On3 profile. |
| `featured_order` | character |  |
| `category_key` | integer |  |
| `category_value` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_player_videos-example}

```python
on3_player_videos(person_key=89617)
```

_Last validated n/a._

## on3_player_visit_center

GET /rdb/v1/player/{playerKey}/visit-center

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{player_key}/visit-center`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/visit-center](https://api.on3.com/public/rdb/v1/player/89617/visit-center)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_key` | `player_key` |  | `Y` |  | player_key path parameter. |

### Returns {#on3_player_visit_center-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the row has nested objects whose flattened column names depend on which are null in the data; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_player_visit_center-example}

```python
on3_player_visit_center(player_key=89617)
```

_Last validated n/a._

## on3_players_industry_comparision

GET /rdb/v1/players/industry-comparision

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/players/industry-comparision`

**Valid URL:** [https://api.on3.com/public/rdb/v1/players/industry-comparision](https://api.on3.com/public/rdb/v1/players/industry-comparision)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `stateAbbr` | `state_abbr` |  |  | `Y` | stateAbbr query parameter. |
| `positionAbbr` | `position_abbr` |  |  | `Y` | positionAbbr query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `sortByIndustry` | `sort_by_industry` |  |  | `Y` | sortByIndustry query parameter. |

### Returns {#on3_players_industry_comparision-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `ratings` | character | List of per-service rating entries (On3, Rivals, 247, ESPN) composing the industry comparison. |
| `nil_value` | integer | On3 NIL valuation for the player (US dollars). |
| `person_key` | integer |  |
| `person_name` | character |  |
| `person_slug` | character |  |
| `person_high_school_name` | character |  |
| `person_high_school_key` | integer |  |
| `person_high_school_full_name` | character |  |
| `person_high_school_name_2` | character |  |
| `person_high_school_known_as` | character |  |
| `person_high_school_mascot` | character |  |
| `person_high_school_abbreviation` | character |  |
| `person_high_school_asset_url` | character |  |
| `person_high_school_default_asset_key` | integer |  |
| `person_high_school_default_asset_domain_override` | character |  |
| `person_high_school_default_asset_domain` | character |  |
| `person_high_school_default_asset_source_override` | character |  |
| `person_high_school_default_asset_source` | character |  |
| `person_high_school_default_asset_title` | character |  |
| `person_high_school_default_asset_description` | character |  |
| `person_high_school_default_asset_caption` | character |  |
| `person_high_school_default_asset_category` | character |  |
| `person_high_school_default_asset_alt_text` | character |  |
| `person_high_school_default_asset_height` | integer |  |
| `person_high_school_default_asset_width` | integer |  |
| `person_high_school_default_asset_asset_type` | character |  |
| `person_high_school_default_asset_file_system` | character |  |
| `person_high_school_default_asset_path` | character |  |
| `person_high_school_default_asset_type` | character |  |
| `person_high_school_default_asset_thumbnail` | character |  |
| `person_high_school_default_asset_duration` | integer |  |
| `person_high_school_default_asset_mime_type` | character |  |
| `person_high_school_slug` | character |  |
| `person_high_school_primary_color` | character |  |
| `person_high_school_org_type` | character |  |
| `person_high_school_org_type_enum` | character |  |
| `person_high_school_division` | character |  |
| `person_high_school_site_keys` | character |  |
| `person_high_school_url_slug` | character |  |
| `person_home_town_name` | character |  |
| `person_default_asset_url` | character |  |
| `person_default_asset_key` | integer |  |
| `person_default_asset_domain_override` | character |  |
| `person_default_asset_domain` | character |  |
| `person_default_asset_source_override` | character |  |
| `person_default_asset_source` | character |  |
| `person_default_asset_title` | character |  |
| `person_default_asset_description` | character |  |
| `person_default_asset_caption` | character |  |
| `person_default_asset_category` | character |  |
| `person_default_asset_alt_text` | character |  |
| `person_default_asset_height` | integer |  |
| `person_default_asset_width` | integer |  |
| `person_default_asset_asset_type` | character |  |
| `person_default_asset_file_system` | character |  |
| `person_default_asset_path` | character |  |
| `person_default_asset_type` | character |  |
| `person_default_asset_thumbnail` | character |  |
| `person_default_asset_duration` | integer |  |
| `person_default_asset_mime_type` | character |  |
| `person_early_signee` | logical |  |
| `person_early_enrollee` | logical |  |
| `person_position_abbreviation` | character |  |
| `person_height` | numeric | Height (feet and inches). |
| `person_formatted_height` | character |  |
| `person_weight` | integer | Weight in pounds. |
| `person_class_year` | integer |  |
| `person_athlete_verified` | logical |  |
| `person_prospect_verified` | logical |  |
| `person_class_rank` | character |  |
| `person_recruitment_key` | integer |  |
| `person_age` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_players_industry_comparision-example}

```python
on3_players_industry_comparision()
```

_Last validated n/a._

## on3_players_industry_comparision_list

GET /rdb/v1/players/industry-comparision-list

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/players/industry-comparision-list`

**Valid URL:** [https://api.on3.com/public/rdb/v1/players/industry-comparision-list](https://api.on3.com/public/rdb/v1/players/industry-comparision-list)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_players_industry_comparision_list-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_players_industry_comparision_list-example}

```python
on3_players_industry_comparision_list()
```

_Last validated n/a._
