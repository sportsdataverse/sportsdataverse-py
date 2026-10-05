---
title: "CFB — On3 Recruit Database (api.on3.com) — Player"
sidebar_label: "Player"
sidebar_position: 4
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
| `change` | character | Rank movement since the previous ranking cycle. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `organizations` | character | Player's organization stints (high school, college, pro) as a stringified list of nested objects. |
| `draft` | character | Nested On3 draft record for the player, when drafted (stringified). |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `organization` | character | Organization. |
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `position_abbr` | character | Position abbreviation. |
| `exp_min` | integer | Minimum years of experience listed for the player's stint with the organization (On3 RDB). |
| `exp_max` | integer | Maximum years of experience listed for the player's stint with the organization (On3 RDB). |
| `year` | character | Four-digit season year (e.g. 2019). |
| `age` | integer | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |

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
| `rating` | numeric | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `state_rank` | integer | State ranking. |
| `state_abbr` | character | Two-letter abbreviation of the player's home state. |
| `position_rank` | integer | Position ranking. |
| `position_abbr` | character | Position abbreviation. |
| `overall_rank` | integer | Overall recruit ranking (top recruits only; may be `NA`). |
| `stars` | integer | Recruit star rating on the 247Sports scale (2-5). |
| `change` | character | Rank movement since the previous ranking cycle. |
| `consensus_rating` | numeric | Player's industry-consensus rating (blend of the major recruiting services). |
| `consensus_state_rank` | integer | Player's consensus rank within their home state. |
| `consensus_position_rank` | integer | Player's consensus rank at their position. |
| `consensus_overall_rank` | integer | Player's national consensus rank. |
| `consensus_stars` | integer | Player's star rating under the industry consensus. |
| `consensus_change` | character | Consensus rank movement since the previous cycle. |
| `strength` | integer | Strength label (Even, Power Play, Shorthanded). |
| `five_star_plus` | logical | Whether On3 designates the player a Five-Star Plus+ prospect. |
| `ranking_type` | character | Poll type code (e.g. `ap`, `coaches`, `cfp`). |

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
| `high_school` | character | High school |
| `hometown_name` | character | Player's hometown, as listed by On3. |
| `hometown_state` | character | Recruit hometown state. |
| `current_state` | character | Current home venue state. |
| `default_asset` | character | Nested On3 asset object for the player's headshot (stringified). |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`); `position_detail = TRUE` only. |
| `primary_position` | character | Nested On3 object for the player's primary position (stringified). |
| `class_rank` | character | Player's rank within their recruiting class. |
| `height` | character | Listed height (inches). |
| `weight` | integer | Listed weight (lbs). |
| `class_year` | integer | Player's recruiting class year. |
| `degree` | character | Degree the player earned or is pursuing, when listed. |
| `age` | integer | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `default_sport` | character | Nested On3 object for the player's primary sport (stringified). |
| `sports` | character | Sports the player is profiled in, as a stringified list. |
| `description` | character | ESPN's description of the stat. |
| `bio_pro_prospect` | character | Bio text framing the player as a pro prospect (On3 RDB). |
| `bio_college_recruit` | character | Bio text framing the player as a college recruit (On3 RDB). |
| `organization_level` | character | Level of the player's current organization (e.g. high school, college, professional). |
| `high_school_org_key` | integer | On3 organization key of the player's high school. |
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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team` | character | Team name. |
| `year` | integer | Four-digit season year (e.g. 2019). |
| `sport` | character | Nested On3 sport object for the target entry (stringified). |
| `coaches` | character | Recruiting coaches at the target school tied to the recruitment (stringified list). |
| `status` | character | Game status (e.g. "scheduled", "in_progress", "completed"). |
| `interest` | integer | Recruit's interest level in the target school, per On3. |
| `distance` | numeric | Yards to gain for a first down (or to the goal line in goal-to-go situations). |
| `class_rank` | integer | Recruit's rank within their recruiting class. |
| `official_visit_count` | integer | Number of official visits the recruit has taken to the school. |
| `un_official_visit_count` | integer | Number of unofficial visits the recruit has taken to the school. |
| `prediction` | numeric | Pre-game prediction (favorite, score, win %). |
| `committed_date` | character | Date the recruit committed to the school, when applicable. |
| `draft_position_count` | integer | Number of players at the recruit's position the school has had drafted. |
| `draft_total` | integer | Total number of players the school has had drafted. |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`); `position_detail = TRUE` only. |
| `position_key` | integer | On3 key of the recruit's position. |

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
| `high_school` | character | High school |
| `hometown_name` | character | Player's hometown, as listed by On3. |
| `hometown_state` | character | Recruit hometown state. |
| `current_state` | character | Current home venue state. |
| `default_asset` | character | Nested On3 asset object for the player's headshot (stringified). |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`); `position_detail = TRUE` only. |
| `primary_position` | character | Nested On3 object for the player's primary position (stringified). |
| `class_rank` | character | Player's rank within their recruiting class. |
| `height` | character | Listed height (inches). |
| `weight` | integer | Listed weight (lbs). |
| `class_year` | integer | Player's recruiting class year. |
| `degree` | character | Degree the player earned or is pursuing, when listed. |
| `age` | integer | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `default_sport` | character | Nested On3 object for the player's primary sport (stringified). |
| `sports` | character | Sports the player is profiled in, as a stringified list. |
| `description` | character | ESPN's description of the stat. |
| `bio_pro_prospect` | character | Bio text framing the player as a pro prospect (On3 RDB). |
| `bio_college_recruit` | character | Bio text framing the player as a college recruit (On3 RDB). |
| `organization_level` | character | Level of the player's current organization (e.g. high school, college, professional). |
| `high_school_org_key` | integer | On3 organization key of the player's high school. |
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
| `person_key` | integer | On3 person key of the featured athlete. |
| `source_url` | character | Source URL of the hosted video. |
| `title` | character | Specific role title for the assignment. |
| `thumbnail` | character | URL of the video's thumbnail image. |
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |
| `description` | character | ESPN's description of the stat. |
| `person_sport` | character | Nested athlete-sport profile the video is attached to (stringified). |
| `is_featured` | logical | Whether the video is featured on the player's On3 profile. |
| `date` | integer | Publication date of the video, per On3. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `sport` | character | Nested On3 sport object for the visit-center entry (stringified). |
| `year` | integer | Four-digit season year (e.g. 2019). |
| `total_visits` | integer | Total number of school visits logged for the recruit. |
| `visits` | character | The recruit's school visits with dates and types, as a stringified list. |

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
| `person` | character | Nested person object (identity, school, position, status) for the player. |
| `nil_value` | integer | On3 NIL valuation for the player (US dollars). |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `ratings` | character | Player's ratings across the industry services, as a stringified list. |
| `person` | character | Nested On3 person object for the compared player (stringified). |
| `nil_value` | integer | Player's On3 NIL valuation in dollars. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_players_industry_comparision_list-example}

```python
on3_players_industry_comparision_list()
```

_Last validated n/a._
