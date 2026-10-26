---
title: "College football — On3 Recruit Database (api.on3.com) — Player"
sidebar_label: "Player"
sidebar_position: 7
description: "College football — On3 Recruit Database (api.on3.com) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# College football — On3 Recruit Database (api.on3.com) — Player

## on3_player_all_rankings

Player all rankings.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{person_key}/all-rankings`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/all-rankings](https://api.on3.com/public/rdb/v1/player/89617/all-rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_player_all_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `type` | String | Type label of the row (vocabulary depends on the endpoint). |
| `link` | String | Site-relative On3 link for the row, when any. |
| `ranking_key` | Int64 | On3 key of the ranking cycle the row belongs to. |
| `ranking_year` | Int64 | Ranking cycle year. |
| `ranking_type` | String | Ranking type (e.g. Player, TransferPortal, Team). |
| `rating` | Float64 | On3 rating of the player (0-100 scale), when published. |
| `sport` | String | Nested On3 sport object for the ranking row (stringified). |
| `class_year` | Int64 | Recruiting class year the ranking covers. |
| `state_rank` | Int64 | Rank within the player's state in the ranking. |
| `state_abbr` | String | Two-letter abbreviation of the player's home state. |
| `position_rank` | Int64 | Rank at the player's position in the ranking. |
| `position_abbr` | String | Position abbreviation the ranking entry was assigned at. |
| `overall_rank` | Int64 | Overall national rank in the ranking. |
| `stars` | Int64 | Star rating (2-5). |
| `five_star_plus` | Boolean | Whether On3 designates the player a Five-Star Plus+ prospect. |
| `nearly_five_star_plus` | Boolean | On3 flag that the player narrowly missed the Five-Star Plus+ designation. |
| `change_1` | String | Direction of the rating change since the previous update (e.g. Increase); the numeric suffix is a json_normalize de-duplication artefact. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_player_all_rankings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_player_all_rankings-example}

```python
on3_player_all_rankings(person_key=89617)
```

## on3_player_database_updates

Player database updates.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{person_key}/database-updates`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/database-updates](https://api.on3.com/public/rdb/v1/player/89617/database-updates)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_player_database_updates-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | Int64 | On3 RDB key for the database-update entry. |
| `type` | String | Type label of the row (vocabulary depends on the endpoint). |
| `text` | String | Text of the update as On3 displays it. |
| `replacement_text` | String | Rendered text of the update entry (with references substituted in). |
| `link` | String | Site-relative On3 link for the row, when any. |
| `date_added` | Int64 | Date the update entry was logged. |
| `date_occurred` | Int64 | Date the underlying event occurred. |
| `object_key` | Int64 | On3 key of the object the update refers to. |
| `sport_key` | Int64 | On3 sport key the update is scoped to. |
| `person_key` | Int64 | On3 person key of the player the update concerns. |
| `organization_key` | Int64 | On3 organization key involved in the update, when any. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_player_database_updates-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_player_database_updates-example}

```python
on3_player_database_updates(person_key=89617)
```

## on3_player_images

Player images.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{person_key}/images`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/images](https://api.on3.com/public/rdb/v1/player/89617/images)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_player_images-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | Int64 | On3 asset key for the image. |
| `domain_override` | String | Override CDN domain for serving the image, when set. |
| `domain` | String | CDN domain the image is served from. |
| `source_override` | String | Override source attribution for the image, when set. |
| `source` | String | CDN-relative source path of the image asset. |
| `title` | String | Title of the row's record. |
| `description` | String | Free-text description or biography shipped by On3. |
| `caption` | String | Caption text for the image. |
| `category` | String | Category label On3 attaches to the row. |
| `alt_text` | String | Alt text for the image. |
| `height` | Int64 | Height as a formatted string (e.g. '6-3.5'). |
| `width` | Int64 | Image width in pixels. |
| `asset_type` | String | Type of the asset (e.g. image) in On3's asset system. |
| `file_system` | String | Storage file system the asset lives on (On3 asset metadata). |
| `path` | String | Storage path of the image file. |
| `type` | String | Type label of the row (vocabulary depends on the endpoint). |
| `thumbnail` | String | URL or path of the image's thumbnail rendition. |
| `duration` | Int64 | Duration of the asset when it is a video (0 for images). |
| `mime_type` | String | MIME type of the image file (e.g. image/jpeg). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_player_images-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_player_images-example}

```python
on3_player_images(person_key=89617)
```

## on3_player_organizations

Player organizations.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{person_key}/organizations`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/organizations](https://api.on3.com/public/rdb/v1/player/89617/organizations)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_player_organizations-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_player_organizations-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_player_organizations-example}

```python
on3_player_organizations(person_key=89617)
```

## on3_player_organizations_org_key

Player organizations org key.

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

### Raises {#on3_player_organizations_org_key-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_player_organizations_org_key-example}

```python
on3_player_organizations_org_key(org_key=1867, player_key=89617)
```

## on3_player_person_rankings

Player person rankings.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{person_key}/rankings`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/rankings](https://api.on3.com/public/rdb/v1/player/89617/rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_player_person_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | Int64 | On3 RDB key for the player's ranking row. |
| `ranking_key` | Int64 | On3 key of the ranking cycle the row belongs to. |
| `rating` | Int64 | On3 rating of the player (0-100 scale), when published. |
| `state_rank` | Int64 | Rank within the player's state in the ranking. |
| `state_abbr` | String | Two-letter abbreviation of the player's home state. |
| `position_rank` | Int64 | Rank at the player's position in the ranking. |
| `position_abbr` | String | Position abbreviation the ranking entry was assigned at. |
| `overall_rank` | Int64 | Overall national rank in the ranking. |
| `stars` | Int64 | Star rating (2-5). |
| `consensus_rating` | Float64 | Player's industry-consensus rating (blend of the major recruiting services). |
| `consensus_state_rank` | Int64 | Player's consensus rank within their home state. |
| `consensus_position_rank` | Int64 | Player's consensus rank at their position. |
| `consensus_overall_rank` | Int64 | Player's national consensus rank. |
| `consensus_stars` | Int64 | Player's star rating under the industry consensus. |
| `strength` | Int64 | Strength score On3 attaches to the ranking entry. |
| `five_star_plus` | Boolean | Whether On3 designates the player a Five-Star Plus+ prospect. |
| `ranking_type` | String | Ranking type (e.g. Player, TransferPortal, Team). |
| `ranking_key_2` | Int64 | Ranking key repeated from the nested ranking object (json_normalize de-duplication suffix). |
| `ranking_sport_key` | Int64 | On3 numeric key of the sport the ranking is in. |
| `ranking_sport_key_2` | Int64 | On3 numeric key of the sport the ranking is in (json_normalize de-duplication suffix). |
| `ranking_sport_name` | String | Name of the sport the ranking is in (e.g. Football). |
| `ranking_year` | Int64 | Ranking cycle year. |
| `change_38` | String | Direction of the On3 rating change since the previous update (e.g. Increase); the numeric suffix is a json_normalize de-duplication artefact. |
| `consensus_change_41` | String | Direction of the consensus rating change since the previous update (e.g. Increase); the numeric suffix is a json_normalize de-duplication artefact. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_player_person_rankings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_player_person_rankings-example}

```python
on3_player_person_rankings(person_key=89617)
```

## on3_player_profile

Player profile.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{person_key}/profile`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/profile](https://api.on3.com/public/rdb/v1/player/89617/profile)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_player_profile-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | Int64 | On3 RDB key for the player profile. |
| `class_year_recruitment_key` | Int64 | On3 recruitment key for the player's recruiting-class cycle. |
| `recruitment_key` | Int64 | On3 key of the player's active recruitment record. |
| `person_can_manage_recruitment` | Boolean | Whether the athlete can self-manage the recruitment on On3. |
| `ranking_key` | Int64 | On3 key of the ranking cycle the profile's rating belongs to. |
| `person_sport_key` | Int64 | On3 key of the athlete-sport profile (person x sport). |
| `oracle_key` | String | On3's internal oracle identifier for the player record. |
| `name` | String | Display name of the row's record. |
| `slug` | String | URL slug of the row's record on On3. |
| `high_school_name` | String | High-school display name (top-level field). |
| `hometown_name` | String | Player's hometown, as listed by On3. |
| `position_abbreviation` | String | Position abbreviation on the record. |
| `class_rank` | String | Player's rank within their recruiting class. |
| `height` | String | Height as a formatted string (e.g. '6-3.5'). |
| `weight` | Int64 | Weight in pounds. |
| `class_year` | Int64 | Player's recruiting class year. |
| `degree` | String | Degree the player earned or is pursuing, when listed. |
| `age` | Int64 | Age in years, when known. |
| `sports` | String | Sports the player is profiled in, as a stringified list. |
| `description` | String | Free-text description or biography shipped by On3. |
| `bio_pro_prospect` | String | Bio text framing the player as a pro prospect (On3 RDB). |
| `bio_college_recruit` | String | Bio text framing the player as a college recruit (On3 RDB). |
| `organization_level` | String | Level of the player's current organization (e.g. high school, college, professional). |
| `high_school_org_key` | Int64 | On3 organization key of the player's high school. |
| `prep_school_org_key` | String | On3 organization key of the player's prep school, when attended. |
| `junior_college_org_key` | String | On3 organization key of the player's junior college, when attended. |
| `college_org_key` | Int64 | On3 organization key of the player's college. |
| `nil_value` | Int64 | Player's On3 NIL valuation in dollars. |
| `athlete_verified` | Boolean | Whether the athlete has verified their own On3 profile. |
| `prospect_verified` | Boolean | Whether On3 has verified the prospect's profile information. |
| `is_coach` | Boolean | Whether the person record is a coach. |
| `is_athlete` | Boolean | Whether the person record is an athlete. |
| `visibility` | String | Profile visibility setting on On3. |
| `tier` | String | On3 profile tier classification for the player. |
| `review_status` | String | Editorial review status of the profile in the On3 database. |
| `jersey_number` | Int64 | Jersey number, when listed. |
| `badge` | String | Profile badge assigned by On3, when any. |
| `ncaa_id` | String | Player's NCAA identifier, when known to On3. |
| `managed_by_user` | Boolean | On3 user account that manages the player's profile, when claimed. |
| `ranking_key_2` | Int64 | Ranking key repeated from the nested ranking object (json_normalize de-duplication suffix). |
| `ranking_rating` | Float64 | Numeric value of the ranking (0-100 scale). |
| `ranking_stars` | Int64 | Star rating of the ranking (2-5). |
| `ranking_national_rank` | Int64 | National rank of the ranking. |
| `ranking_position_rank` | Int64 | Position rank of the ranking. |
| `ranking_state_rank` | Int64 | State rank of the ranking. |
| `ranking_position_abbr` | String | Position abbreviation the ranking was assigned at. |
| `ranking_state_abbr` | String | State abbreviation the ranking was assigned in. |
| `ranking_five_star_plus` | Boolean | Five-star-plus flag on the ranking. |
| `high_school_key` | Int64 | On3 numeric key of the high school. |
| `high_school_full_name` | String | Full name of the high school (with mascot). |
| `high_school_name_2` | String | Name field of the nested high-school object (json_normalize de-duplication of high_school_name). |
| `high_school_known_as` | String | Common short name of the high school, when On3 lists one. |
| `high_school_mascot` | String | High-school mascot. |
| `high_school_abbreviation` | String | High-school abbreviation. |
| `high_school_asset_url` | String | Convenience CDN URL of the high-school logo. |
| `high_school_default_asset_key` | Int64 | On3 asset key of the high school's logo asset. |
| `high_school_default_asset_domain_override` | String | CDN domain override for the high school's logo asset (usually null). |
| `high_school_default_asset_domain` | String | CDN domain serving the high school's logo asset. |
| `high_school_default_asset_source_override` | String | Source-path override for the high school's logo asset (usually null). |
| `high_school_default_asset_source` | String | CDN-relative source path of the high school's logo asset. |
| `high_school_default_asset_title` | String | Editorial title attached to the high school's logo asset. |
| `high_school_default_asset_description` | String | Editorial description attached to the high school's logo asset (usually null). |
| `high_school_default_asset_caption` | String | Editorial caption attached to the high school's logo asset (usually null). |
| `high_school_default_asset_category` | String | Editorial category label of the high school's logo asset (usually null). |
| `high_school_default_asset_alt_text` | String | Accessibility alt text of the high school's logo asset (usually null). |
| `high_school_default_asset_height` | Int64 | Pixel height of the high school's logo asset. |
| `high_school_default_asset_width` | Int64 | Pixel width of the high school's logo asset. |
| `high_school_default_asset_asset_type` | String | On3 asset-type discriminator of the high school's logo asset (e.g. Image). |
| `high_school_default_asset_file_system` | String | Storage file-system flag of the high school's logo asset. |
| `high_school_default_asset_path` | String | Storage path of the high school's logo asset. |
| `high_school_default_asset_type` | String | Media type field of the high school's logo asset (file extension, e.g. png). |
| `high_school_default_asset_thumbnail` | String | Thumbnail variant of the high school's logo asset (video assets; usually null). |
| `high_school_default_asset_duration` | Int64 | Duration of the high school's logo asset when it is a video (usually null or 0). |
| `high_school_default_asset_mime_type` | String | MIME type of the high school's logo asset. |
| `high_school_slug` | String | URL slug of the high school on On3. |
| `high_school_primary_color` | String | Primary hex color of the high school. |
| `high_school_org_type` | String | Organization type label of the school (e.g. HighSchool). |
| `high_school_org_type_enum` | String | Organization type enum of the school. |
| `high_school_division` | String | Division or classification of the high school, when listed. |
| `high_school_site_keys` | String | JSON-encoded On3 site keys covering the school (usually null). |
| `high_school_url_slug` | String | URL slug variant of the high-school page, with the key appended. |
| `hometown_state_key` | Int64 | On3 numeric key of the home-town state. |
| `hometown_state_name` | String | Name of the home-town state. |
| `hometown_state_abbreviation` | String | Two-letter abbreviation of the home-town state. |
| `hometown_state_country_key` | Int64 | On3 numeric key of the home-town state's country. |
| `current_state_key` | Int64 | On3 numeric key of the current state. |
| `current_state_name` | String | Name of the current state. |
| `current_state_abbreviation` | String | Two-letter abbreviation of the current state. |
| `current_state_country_key` | Int64 | On3 numeric key of the current state's country. |
| `default_asset_key` | Int64 | On3 asset key of the default asset. |
| `default_asset_domain_override` | String | CDN domain override for the record's default asset (usually null). |
| `default_asset_domain` | String | CDN domain serving the default asset. |
| `default_asset_source_override` | String | Source-path override for the default asset (usually null). |
| `default_asset_source` | String | CDN-relative source path of the default asset. |
| `default_asset_title` | String | Editorial title attached to the default asset. |
| `default_asset_description` | String | Editorial description attached to the default asset (usually null). |
| `default_asset_caption` | String | Editorial caption attached to the default asset (usually null). |
| `default_asset_category` | String | Editorial category label of the default asset (usually null). |
| `default_asset_alt_text` | String | Accessibility alt text of the default asset (usually null). |
| `default_asset_height` | Int64 | Pixel height of the default asset. |
| `default_asset_width` | Int64 | Pixel width of the default asset. |
| `default_asset_asset_type` | String | On3 asset-type discriminator of the default asset (e.g. Image). |
| `default_asset_file_system` | String | Storage file-system flag of the default asset. |
| `default_asset_path` | String | Storage path of the default asset. |
| `default_asset_type` | String | Media type field of the default asset (file extension, e.g. png). |
| `default_asset_thumbnail` | String | Thumbnail variant of the default asset (video assets; usually null). |
| `default_asset_duration` | Int64 | Duration of the default asset when it is a video (usually null or 0). |
| `default_asset_mime_type` | String | MIME type of the default asset. |
| `primary_position_key` | Int64 | On3 numeric key of the primary position. |
| `primary_position_name` | String | Name of the primary position (e.g. Quarterback). |
| `primary_position_abbreviation` | String | Abbreviation of the primary position (e.g. QB). |
| `primary_position_sport_key` | Int64 | On3 numeric key of the primary position's sport. |
| `primary_position_sport_key_2` | Int64 | On3 numeric key of the primary position's sport (json_normalize de-duplication suffix). |
| `primary_position_sport_name` | String | Name of the primary position's sport (e.g. Football). |
| `primary_position_sport_slug` | String | URL slug of the primary position's sport. |
| `primary_position_sport_abbreviation` | String | Abbreviation of the primary position's sport. |
| `primary_position_sport_is_rankable` | Boolean | Whether On3 ranks players in the primary position's sport. |
| `primary_position_sport_is_industry_rankable` | Boolean | Whether industry-consensus rankings exist for the primary position's sport. |
| `primary_position_sport_is_scoutable` | Boolean | Whether On3 scouting reports exist for the primary position's sport. |
| `primary_position_position_type` | String | Position type of the primary position (e.g. Offense, Defense). |
| `default_sport_key` | Int64 | On3 numeric key of the primary sport. |
| `default_sport_name` | String | Name of the primary sport (e.g. Football). |
| `player_status_type` | String | Type of the player's recruiting status (e.g. Committed, Signed, Enrolled, None). |
| `player_status_short_term_signee` | Boolean | Short-term-signee flag of the player's recruiting status (null when not applicable). |
| `player_status_date` | String | Date the player's recruiting status took effect (ISO timestamp string). |
| `player_status_committed_asset_key` | Int64 | On3 numeric key of the player's recruiting status's committed-to program. |
| `player_status_committed_asset_url` | String | CDN URL of the player's recruiting status's committed-to program's logo. |
| `player_status_committed_asset_slug` | String | URL slug of the player's recruiting status's committed-to program on On3. |
| `player_status_committed_asset_full_name` | String | Full name of the player's recruiting status's committed-to program (e.g. 'Alabama Crimson Tide'). |
| `player_status_committed_asset_res_key` | Int64 | On3 asset key of the player's recruiting status's committed-to program's logo asset. |
| `player_status_committed_asset_res_domain_override` | String | CDN domain override for the player's recruiting status's committed-to program's logo asset (usually null). |
| `player_status_committed_asset_res_domain` | String | CDN domain serving the player's recruiting status's committed-to program's logo asset. |
| `player_status_committed_asset_res_source_override` | String | Source-path override for the player's recruiting status's committed-to program's logo asset (usually null). |
| `player_status_committed_asset_res_source` | String | CDN-relative source path of the player's recruiting status's committed-to program's logo asset. |
| `player_status_committed_asset_res_title` | String | Editorial title attached to the player's recruiting status's committed-to program's logo asset. |
| `player_status_committed_asset_res_description` | String | Editorial description attached to the player's recruiting status's committed-to program's logo asset (usually null). |
| `player_status_committed_asset_res_caption` | String | Editorial caption attached to the player's recruiting status's committed-to program's logo asset (usually null). |
| `player_status_committed_asset_res_category` | String | Editorial category label of the player's recruiting status's committed-to program's logo asset (usually null). |
| `player_status_committed_asset_res_alt_text` | String | Accessibility alt text of the player's recruiting status's committed-to program's logo asset (usually null). |
| `player_status_committed_asset_res_height` | Int64 | Pixel height of the player's recruiting status's committed-to program's logo asset. |
| `player_status_committed_asset_res_width` | Int64 | Pixel width of the player's recruiting status's committed-to program's logo asset. |
| `player_status_committed_asset_res_asset_type` | String | On3 asset-type discriminator of the player's recruiting status's committed-to program's logo asset (e.g. Image). |
| `player_status_committed_asset_res_file_system` | String | Storage file-system flag of the player's recruiting status's committed-to program's logo asset. |
| `player_status_committed_asset_res_path` | String | Storage path of the player's recruiting status's committed-to program's logo asset. |
| `player_status_committed_asset_res_type` | String | Media type field of the player's recruiting status's committed-to program's logo asset (file extension, e.g. png). |
| `player_status_committed_asset_res_thumbnail` | String | Thumbnail variant of the player's recruiting status's committed-to program's logo asset (video assets; usually null). |
| `player_status_committed_asset_res_duration` | Int64 | Duration of the player's recruiting status's committed-to program's logo asset when it is a video (usually null or 0). |
| `player_status_committed_asset_res_mime_type` | String | MIME type of the player's recruiting status's committed-to program's logo asset. |
| `player_status_transferred_asset_key` | Int64 | On3 numeric key of the player's recruiting status's program transferred to. |
| `player_status_transferred_asset_url` | String | CDN URL of the player's recruiting status's program transferred to's logo. |
| `player_status_transferred_asset_slug` | String | URL slug of the player's recruiting status's program transferred to on On3. |
| `player_status_transferred_asset_full_name` | String | Full name of the player's recruiting status's program transferred to (e.g. 'Alabama Crimson Tide'). |
| `player_status_transferred_asset_res_key` | Int64 | On3 asset key of the player's recruiting status's program transferred to's logo asset. |
| `player_status_transferred_asset_res_domain_override` | String | CDN domain override for the player's recruiting status's program transferred to's logo asset (usually null). |
| `player_status_transferred_asset_res_domain` | String | CDN domain serving the player's recruiting status's program transferred to's logo asset. |
| `player_status_transferred_asset_res_source_override` | String | Source-path override for the player's recruiting status's program transferred to's logo asset (usually null). |
| `player_status_transferred_asset_res_source` | String | CDN-relative source path of the player's recruiting status's program transferred to's logo asset. |
| `player_status_transferred_asset_res_title` | String | Editorial title attached to the player's recruiting status's program transferred to's logo asset. |
| `player_status_transferred_asset_res_description` | String | Editorial description attached to the player's recruiting status's program transferred to's logo asset (usually null). |
| `player_status_transferred_asset_res_caption` | String | Editorial caption attached to the player's recruiting status's program transferred to's logo asset (usually null). |
| `player_status_transferred_asset_res_category` | String | Editorial category label of the player's recruiting status's program transferred to's logo asset (usually null). |
| `player_status_transferred_asset_res_alt_text` | String | Accessibility alt text of the player's recruiting status's program transferred to's logo asset (usually null). |
| `player_status_transferred_asset_res_height` | Int64 | Pixel height of the player's recruiting status's program transferred to's logo asset. |
| `player_status_transferred_asset_res_width` | Int64 | Pixel width of the player's recruiting status's program transferred to's logo asset. |
| `player_status_transferred_asset_res_asset_type` | String | On3 asset-type discriminator of the player's recruiting status's program transferred to's logo asset (e.g. Image). |
| `player_status_transferred_asset_res_file_system` | String | Storage file-system flag of the player's recruiting status's program transferred to's logo asset. |
| `player_status_transferred_asset_res_path` | String | Storage path of the player's recruiting status's program transferred to's logo asset. |
| `player_status_transferred_asset_res_type` | String | Media type field of the player's recruiting status's program transferred to's logo asset (file extension, e.g. png). |
| `player_status_transferred_asset_res_thumbnail` | String | Thumbnail variant of the player's recruiting status's program transferred to's logo asset (video assets; usually null). |
| `player_status_transferred_asset_res_duration` | Int64 | Duration of the player's recruiting status's program transferred to's logo asset when it is a video (usually null or 0). |
| `player_status_transferred_asset_res_mime_type` | String | MIME type of the player's recruiting status's program transferred to's logo asset. |
| `player_status_committed_organization_key` | Int64 | On3 key of the committed-to program (the player's recruiting status). |
| `player_status_committed_organization_full_name` | String | Full name of the player's recruiting status's committed-to program (e.g. 'Alabama Crimson Tide'). |
| `player_status_committed_organization_name` | String | Short name of the player's recruiting status's committed-to program. |
| `player_status_committed_organization_mascot` | String | Mascot of the player's recruiting status's committed-to program. |
| `player_status_committed_organization_abbreviation` | String | Abbreviation of the player's recruiting status's committed-to program. |
| `player_status_committed_organization_asset_url` | String | CDN URL of the committed-to program's logo (the player's recruiting status). |
| `player_status_committed_organization_asset_key` | Int64 | On3 asset key of the player's recruiting status's committed-to program's logo asset. |
| `player_status_committed_organization_asset_domain_override` | String | CDN domain override for the player's recruiting status's committed-to program's logo asset (usually null). |
| `player_status_committed_organization_asset_domain` | String | CDN domain serving the player's recruiting status's committed-to program's logo asset. |
| `player_status_committed_organization_asset_source_override` | String | Source-path override for the player's recruiting status's committed-to program's logo asset (usually null). |
| `player_status_committed_organization_asset_source` | String | CDN-relative source path of the player's recruiting status's committed-to program's logo asset. |
| `player_status_committed_organization_asset_title` | String | Editorial title attached to the player's recruiting status's committed-to program's logo asset. |
| `player_status_committed_organization_asset_description` | String | Editorial description attached to the player's recruiting status's committed-to program's logo asset (usually null). |
| `player_status_committed_organization_asset_caption` | String | Editorial caption attached to the player's recruiting status's committed-to program's logo asset (usually null). |
| `player_status_committed_organization_asset_category` | String | Editorial category label of the player's recruiting status's committed-to program's logo asset (usually null). |
| `player_status_committed_organization_asset_alt_text` | String | Accessibility alt text of the player's recruiting status's committed-to program's logo asset (usually null). |
| `player_status_committed_organization_asset_height` | Int64 | Pixel height of the player's recruiting status's committed-to program's logo asset. |
| `player_status_committed_organization_asset_width` | Int64 | Pixel width of the player's recruiting status's committed-to program's logo asset. |
| `player_status_committed_organization_asset_asset_type` | String | On3 asset-type discriminator of the player's recruiting status's committed-to program's logo asset (e.g. Image). |
| `player_status_committed_organization_asset_file_system` | String | Storage file-system flag of the player's recruiting status's committed-to program's logo asset. |
| `player_status_committed_organization_asset_path` | String | Storage path of the player's recruiting status's committed-to program's logo asset. |
| `player_status_committed_organization_asset_type` | String | Media type field of the player's recruiting status's committed-to program's logo asset (file extension, e.g. png). |
| `player_status_committed_organization_asset_thumbnail` | String | Thumbnail variant of the player's recruiting status's committed-to program's logo asset (video assets; usually null). |
| `player_status_committed_organization_asset_duration` | Int64 | Duration of the player's recruiting status's committed-to program's logo asset when it is a video (usually null or 0). |
| `player_status_committed_organization_asset_mime_type` | String | MIME type of the player's recruiting status's committed-to program's logo asset. |
| `player_status_committed_organization_slug` | String | URL slug of the committed-to program (the player's recruiting status). |
| `player_status_committed_organization_primary_color` | String | Primary hex color of the player's recruiting status's committed-to program. |
| `player_status_class_rank` | String | Academic class standing recorded on the player's recruiting status (e.g. Senior). |
| `player_status_transfer_entered` | String | Date the player entered the transfer portal (the player's recruiting status; null when never entered). |
| `player_status_recruitment_year` | String | Recruiting-cycle year the player's recruiting status belongs to. |
| `player_status_decommitted_asset` | String | Nested asset of the program decommitted from (the player's recruiting status; usually null). |
| `player_status_transfer` | Boolean | Transfer flag of the player's recruiting status (null when not applicable). |
| `player_status_expected_to_transfer` | Boolean | Expected-to-transfer flag of the player's recruiting status (null when not applicable). |
| `player_status_recruitment_key` | Int64 | On3 key of the recruitment record the player's recruiting status belongs to. |
| `player_status_withdrawn_transfer` | Boolean | Whether the player withdrew from the transfer portal (the player's recruiting status). |
| `player_status_withdrawn_transfer_date` | String | Date the player withdrew from the transfer portal (the player's recruiting status; null when never withdrawn). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_player_profile-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_player_profile-example}

```python
on3_player_profile(person_key=89617)
```

## on3_player_team_targets

Player team targets.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{player_key}/team-targets`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/team-targets](https://api.on3.com/public/rdb/v1/player/89617/team-targets)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_key` | `player_key` |  | `Y` |  | player_key path parameter. |

### Returns {#on3_player_team_targets-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the row has nested objects whose flattened column names depend on which are null in the data; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_player_team_targets-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_player_team_targets-example}

```python
on3_player_team_targets(player_key=89617)
```

## on3_player_verified

Player verified.

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
| `key` | Int64 | On3 RDB key for the player profile. |
| `class_year_recruitment_key` | String | On3 recruitment key for the player's recruiting-class cycle. |
| `recruitment_key` | Int64 | On3 key of the player's active recruitment record. |
| `person_can_manage_recruitment` | String | Whether the athlete can self-manage the recruitment on On3. |
| `ranking_key` | Int64 | On3 key of the ranking cycle the profile's rating belongs to. |
| `person_sport_key` | Int64 | On3 key of the athlete-sport profile (person x sport). |
| `ranking` | String | Nested ranking object (stringified; usually null). |
| `oracle_key` | String | On3's internal oracle identifier for the player record. |
| `name` | String | Display name of the row's record. |
| `slug` | String | URL slug of the row's record on On3. |
| `high_school_name` | String | High-school display name (top-level field). |
| `hometown_name` | String | Player's hometown, as listed by On3. |
| `hometown_state` | String | Home-town state, when listed. |
| `current_state` | String | State the player currently plays in, when listed. |
| `position_abbreviation` | String | Position abbreviation on the record. |
| `primary_position` | String | Nested On3 object for the player's primary position (stringified). |
| `class_rank` | String | Player's rank within their recruiting class. |
| `height` | String | Height as a formatted string (e.g. '6-3.5'). |
| `weight` | Int64 | Weight in pounds. |
| `class_year` | Int64 | Player's recruiting class year. |
| `degree` | String | Degree the player earned or is pursuing, when listed. |
| `age` | Int64 | Age in years, when known. |
| `sports` | String | Sports the player is profiled in, as a stringified list. |
| `description` | String | Free-text description or biography shipped by On3. |
| `bio_pro_prospect` | String | Bio text framing the player as a pro prospect (On3 RDB). |
| `bio_college_recruit` | String | Bio text framing the player as a college recruit (On3 RDB). |
| `organization_level` | String | Level of the player's current organization (e.g. high school, college, professional). |
| `high_school_org_key` | Float64 | On3 organization key of the player's high school. |
| `prep_school_org_key` | String | On3 organization key of the player's prep school, when attended. |
| `junior_college_org_key` | String | On3 organization key of the player's junior college, when attended. |
| `college_org_key` | String | On3 organization key of the player's college. |
| `nil_value` | Int64 | Player's On3 NIL valuation in dollars. |
| `athlete_verified` | Boolean | Whether the athlete has verified their own On3 profile. |
| `prospect_verified` | Boolean | Whether On3 has verified the prospect's profile information. |
| `player_status` | String | Player's current status per On3 (e.g. active, transfer portal). |
| `is_coach` | Boolean | Whether the person record is a coach. |
| `is_athlete` | Boolean | Whether the person record is an athlete. |
| `visibility` | String | Profile visibility setting on On3. |
| `tier` | String | On3 profile tier classification for the player. |
| `review_status` | String | Editorial review status of the profile in the On3 database. |
| `jersey_number` | String | Jersey number, when listed. |
| `badge` | String | Profile badge assigned by On3, when any. |
| `ncaa_id` | String | Player's NCAA identifier, when known to On3. |
| `managed_by_user` | Boolean | On3 user account that manages the player's profile, when claimed. |
| `high_school_key` | Int64 | On3 numeric key of the high school. |
| `high_school_full_name` | String | Full name of the high school (with mascot). |
| `high_school_name_2` | String | Name field of the nested high-school object (json_normalize de-duplication of high_school_name). |
| `high_school_known_as` | String | Common short name of the high school, when On3 lists one. |
| `high_school_mascot` | String | High-school mascot. |
| `high_school_abbreviation` | String | High-school abbreviation. |
| `high_school_asset_url` | String | Convenience CDN URL of the high-school logo. |
| `high_school_default_asset_key` | Int64 | On3 asset key of the high school's logo asset. |
| `high_school_default_asset_domain_override` | String | CDN domain override for the high school's logo asset (usually null). |
| `high_school_default_asset_domain` | String | CDN domain serving the high school's logo asset. |
| `high_school_default_asset_source_override` | String | Source-path override for the high school's logo asset (usually null). |
| `high_school_default_asset_source` | String | CDN-relative source path of the high school's logo asset. |
| `high_school_default_asset_title` | String | Editorial title attached to the high school's logo asset. |
| `high_school_default_asset_description` | String | Editorial description attached to the high school's logo asset (usually null). |
| `high_school_default_asset_caption` | String | Editorial caption attached to the high school's logo asset (usually null). |
| `high_school_default_asset_category` | String | Editorial category label of the high school's logo asset (usually null). |
| `high_school_default_asset_alt_text` | String | Accessibility alt text of the high school's logo asset (usually null). |
| `high_school_default_asset_height` | Int64 | Pixel height of the high school's logo asset. |
| `high_school_default_asset_width` | Int64 | Pixel width of the high school's logo asset. |
| `high_school_default_asset_asset_type` | String | On3 asset-type discriminator of the high school's logo asset (e.g. Image). |
| `high_school_default_asset_file_system` | String | Storage file-system flag of the high school's logo asset. |
| `high_school_default_asset_path` | String | Storage path of the high school's logo asset. |
| `high_school_default_asset_type` | String | Media type field of the high school's logo asset (file extension, e.g. png). |
| `high_school_default_asset_thumbnail` | String | Thumbnail variant of the high school's logo asset (video assets; usually null). |
| `high_school_default_asset_duration` | Int64 | Duration of the high school's logo asset when it is a video (usually null or 0). |
| `high_school_default_asset_mime_type` | String | MIME type of the high school's logo asset. |
| `high_school_slug` | String | URL slug of the high school on On3. |
| `high_school_primary_color` | String | Primary hex color of the high school. |
| `high_school_org_type` | String | Organization type label of the school (e.g. HighSchool). |
| `high_school_org_type_enum` | String | Organization type enum of the school. |
| `high_school_division` | String | Division or classification of the high school, when listed. |
| `high_school_site_keys` | String | JSON-encoded On3 site keys covering the school (usually null). |
| `high_school_url_slug` | String | URL slug variant of the high-school page, with the key appended. |
| `default_asset_key` | Int64 | On3 asset key of the default asset. |
| `default_asset_domain_override` | String | CDN domain override for the record's default asset (usually null). |
| `default_asset_domain` | String | CDN domain serving the default asset. |
| `default_asset_source_override` | String | Source-path override for the default asset (usually null). |
| `default_asset_source` | String | CDN-relative source path of the default asset. |
| `default_asset_title` | String | Editorial title attached to the default asset. |
| `default_asset_description` | String | Editorial description attached to the default asset (usually null). |
| `default_asset_caption` | String | Editorial caption attached to the default asset (usually null). |
| `default_asset_category` | String | Editorial category label of the default asset (usually null). |
| `default_asset_alt_text` | String | Accessibility alt text of the default asset (usually null). |
| `default_asset_height` | Int64 | Pixel height of the default asset. |
| `default_asset_width` | Int64 | Pixel width of the default asset. |
| `default_asset_asset_type` | String | On3 asset-type discriminator of the default asset (e.g. Image). |
| `default_asset_file_system` | String | Storage file-system flag of the default asset. |
| `default_asset_path` | String | Storage path of the default asset. |
| `default_asset_type` | String | Media type field of the default asset (file extension, e.g. png). |
| `default_asset_thumbnail` | String | Thumbnail variant of the default asset (video assets; usually null). |
| `default_asset_duration` | Int64 | Duration of the default asset when it is a video (usually null or 0). |
| `default_asset_mime_type` | String | MIME type of the default asset. |
| `default_sport_key` | Int64 | On3 numeric key of the primary sport. |
| `default_sport_name` | String | Name of the primary sport (e.g. Football). |
| `hometown_state_key` | Float64 | On3 numeric key of the home-town state. |
| `hometown_state_name` | String | Name of the home-town state. |
| `hometown_state_abbreviation` | String | Two-letter abbreviation of the home-town state. |
| `hometown_state_country_key` | Float64 | On3 numeric key of the home-town state's country. |
| `current_state_key` | Float64 | On3 numeric key of the current state. |
| `current_state_name` | String | Name of the current state. |
| `current_state_abbreviation` | String | Two-letter abbreviation of the current state. |
| `current_state_country_key` | Float64 | On3 numeric key of the current state's country. |
| `primary_position_key` | Float64 | On3 numeric key of the primary position. |
| `primary_position_name` | String | Name of the primary position (e.g. Quarterback). |
| `primary_position_abbreviation` | String | Abbreviation of the primary position (e.g. QB). |
| `primary_position_sport_key` | Float64 | On3 numeric key of the primary position's sport. |
| `primary_position_sport_key_2` | Float64 | On3 numeric key of the primary position's sport (json_normalize de-duplication suffix). |
| `primary_position_sport_name` | String | Name of the primary position's sport (e.g. Football). |
| `primary_position_sport_slug` | String | URL slug of the primary position's sport. |
| `primary_position_sport_abbreviation` | String | Abbreviation of the primary position's sport. |
| `primary_position_sport_is_rankable` | String | Whether On3 ranks players in the primary position's sport. |
| `primary_position_sport_is_industry_rankable` | String | Whether industry-consensus rankings exist for the primary position's sport. |
| `primary_position_sport_is_scoutable` | String | Whether On3 scouting reports exist for the primary position's sport. |
| `primary_position_position_type` | String | Position type of the primary position (e.g. Offense, Defense). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_player_verified-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_player_verified-example}

```python
on3_player_verified()
```

## on3_player_videos

Player videos.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{person_key}/videos`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/videos](https://api.on3.com/public/rdb/v1/player/89617/videos)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_player_videos-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | Int64 | On3 RDB key for the video record. |
| `source_url` | String | Source URL of the hosted video. |
| `title` | String | Title of the row's record. |
| `thumbnail` | String | URL of the video's thumbnail image. |
| `description` | String | Free-text description or biography shipped by On3. |
| `date` | Int64 | Publication date of the video, per On3. |
| `person_key` | Int64 | On3 person key of the featured athlete. |
| `person_sport` | String | Nested athlete-sport profile the video is attached to (stringified). |
| `is_featured` | Boolean | Whether the video is featured on the player's On3 profile. |
| `featured_order` | String | Display order among featured videos, when featured. |
| `category_key` | Int64 | On3 key of the video category. |
| `category_value` | String | Video category label (e.g. Highlights). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_player_videos-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_player_videos-example}

```python
on3_player_videos(person_key=89617)
```

## on3_player_visit_center

Player visit center.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{player_key}/visit-center`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/visit-center](https://api.on3.com/public/rdb/v1/player/89617/visit-center)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_key` | `player_key` |  | `Y` |  | player_key path parameter. |

### Returns {#on3_player_visit_center-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the row has nested objects whose flattened column names depend on which are null in the data; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_player_visit_center-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_player_visit_center-example}

```python
on3_player_visit_center(player_key=89617)
```

## on3_players_industry_comparision

Players industry comparision.

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
| `ratings` | String | List of per-service rating entries (On3, Rivals, 247, ESPN) composing the industry comparison. |
| `nil_value` | Int64 | On3 NIL valuation for the player (US dollars). |
| `person_key` | Int64 | On3 numeric key of the person. |
| `person_name` | String | Display name of the person. |
| `person_slug` | String | URL slug of the person's On3 profile. |
| `person_high_school_name` | String | High-school display name on the person's record. |
| `person_high_school_key` | Int64 | On3 numeric key of the person's high school. |
| `person_high_school_full_name` | String | Full name of the person's high school (e.g. 'Alabama Crimson Tide'). |
| `person_high_school_name_2` | String | High-school display name on the person's record (json_normalize de-duplication suffix). |
| `person_high_school_known_as` | String | Common short name of the person's high school, when On3 lists one. |
| `person_high_school_mascot` | String | Mascot of the person's high school. |
| `person_high_school_abbreviation` | String | Abbreviation of the person's high school. |
| `person_high_school_asset_url` | String | Convenience CDN URL of the person's high school's logo. |
| `person_high_school_default_asset_key` | Int64 | On3 asset key of the person's high school's logo asset. |
| `person_high_school_default_asset_domain_override` | String | CDN domain override for the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_domain` | String | CDN domain serving the person's high school's logo asset. |
| `person_high_school_default_asset_source_override` | String | Source-path override for the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_source` | String | CDN-relative source path of the person's high school's logo asset. |
| `person_high_school_default_asset_title` | String | Editorial title attached to the person's high school's logo asset. |
| `person_high_school_default_asset_description` | String | Editorial description attached to the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_caption` | String | Editorial caption attached to the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_category` | String | Editorial category label of the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_alt_text` | String | Accessibility alt text of the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_height` | Int64 | Pixel height of the person's high school's logo asset. |
| `person_high_school_default_asset_width` | Int64 | Pixel width of the person's high school's logo asset. |
| `person_high_school_default_asset_asset_type` | String | On3 asset-type discriminator of the person's high school's logo asset (e.g. Image). |
| `person_high_school_default_asset_file_system` | String | Storage file-system flag of the person's high school's logo asset. |
| `person_high_school_default_asset_path` | String | Storage path of the person's high school's logo asset. |
| `person_high_school_default_asset_type` | String | Media type field of the person's high school's logo asset (file extension, e.g. png). |
| `person_high_school_default_asset_thumbnail` | String | Thumbnail variant of the person's high school's logo asset (video assets; usually null). |
| `person_high_school_default_asset_duration` | Int64 | Duration of the person's high school's logo asset when it is a video (usually null or 0). |
| `person_high_school_default_asset_mime_type` | String | MIME type of the person's high school's logo asset. |
| `person_high_school_slug` | String | URL slug of the person's high school on On3. |
| `person_high_school_primary_color` | String | Primary hex color of the person's high school. |
| `person_high_school_org_type` | String | Organization type label of the person's high school (e.g. HighSchool, College). |
| `person_high_school_org_type_enum` | String | Organization type enum of the person's high school (same vocabulary as org_type). |
| `person_high_school_division` | String | Division or classification of the person's high school (e.g. NCAA-FB). |
| `person_high_school_site_keys` | String | JSON-encoded On3 site keys covering the person's high school (usually null). |
| `person_high_school_url_slug` | String | URL slug variant of the person's high school's page, with the key appended. |
| `person_home_town_name` | String | Home town of the person as On3 lists it (e.g. 'Belleville, MI'). |
| `person_default_asset_url` | String | Convenience CDN URL of the person's headshot. |
| `person_default_asset_key` | Int64 | On3 asset key of the person's headshot asset. |
| `person_default_asset_domain_override` | String | CDN domain override for the person's headshot asset (usually null). |
| `person_default_asset_domain` | String | CDN domain serving the person's headshot asset. |
| `person_default_asset_source_override` | String | Source-path override for the person's headshot asset (usually null). |
| `person_default_asset_source` | String | CDN-relative source path of the person's headshot asset. |
| `person_default_asset_title` | String | Editorial title attached to the person's headshot asset. |
| `person_default_asset_description` | String | Editorial description attached to the person's headshot asset (usually null). |
| `person_default_asset_caption` | String | Editorial caption attached to the person's headshot asset (usually null). |
| `person_default_asset_category` | String | Editorial category label of the person's headshot asset (usually null). |
| `person_default_asset_alt_text` | String | Accessibility alt text of the person's headshot asset (usually null). |
| `person_default_asset_height` | Int64 | Pixel height of the person's headshot asset. |
| `person_default_asset_width` | Int64 | Pixel width of the person's headshot asset. |
| `person_default_asset_asset_type` | String | On3 asset-type discriminator of the person's headshot asset (e.g. Image). |
| `person_default_asset_file_system` | String | Storage file-system flag of the person's headshot asset. |
| `person_default_asset_path` | String | Storage path of the person's headshot asset. |
| `person_default_asset_type` | String | Media type field of the person's headshot asset (file extension, e.g. png). |
| `person_default_asset_thumbnail` | String | Thumbnail variant of the person's headshot asset (video assets; usually null). |
| `person_default_asset_duration` | Int64 | Duration of the person's headshot asset when it is a video (usually null or 0). |
| `person_default_asset_mime_type` | String | MIME type of the person's headshot asset. |
| `person_early_signee` | Boolean | Whether the person signed during the early signing period. |
| `person_early_enrollee` | Boolean | Whether the person early-enrolled at college. |
| `person_position_abbreviation` | String | Position abbreviation on the person's record. |
| `person_height` | Float64 | Height of the person: a formatted string (e.g. '6-8') or inches, depending on the endpoint. |
| `person_formatted_height` | String | Human-formatted height of the person (e.g. '6-3.5'). |
| `person_weight` | Int64 | Weight of the person in pounds. |
| `person_class_year` | Int64 | High-school graduating class year of the person. |
| `person_athlete_verified` | Boolean | Whether the person's athlete profile is verified by On3. |
| `person_prospect_verified` | Boolean | Whether the person's prospect measurables are verified by On3. |
| `person_class_rank` | String | Academic class standing of the person (e.g. Senior, RedShirt Senior). |
| `person_recruitment_key` | Int64 | On3 key of the person's active recruitment record. |
| `person_age` | Int64 | Age of the person in years, when known. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_players_industry_comparision-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_players_industry_comparision-example}

```python
on3_players_industry_comparision()
```

## on3_players_industry_comparision_list

Players industry comparision list.

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

### Raises {#on3_players_industry_comparision_list-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_players_industry_comparision_list-example}

```python
on3_players_industry_comparision_list()
```
