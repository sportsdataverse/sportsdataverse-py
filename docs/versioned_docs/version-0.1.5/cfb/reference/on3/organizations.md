---
title: "CFB — On3 Recruit Database (api.on3.com) — Organizations"
sidebar_label: "Organizations"
sidebar_position: 4
description: "CFB — On3 Recruit Database (api.on3.com) — Organizations — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB — On3 Recruit Database (api.on3.com) — Organizations

## on3_organizations_draft_class_by_state

GET /rdb/v1/organizations/{organizationKey}/draft-class-by-state

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/draft-class-by-state`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/draft-class-by-state](https://api.on3.com/public/rdb/v1/organizations/1867/draft-class-by-state)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_organizations_draft_class_by_state-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_organizations_draft_class_by_state-example}

```python
on3_organizations_draft_class_by_state(organization_key=1867)
```

_Last validated n/a._

## on3_organizations_draft_class_by_year

GET /rdb/v1/organizations/{organizationKey}/draft-class-by-year

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/draft-class-by-year`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/draft-class-by-year](https://api.on3.com/public/rdb/v1/organizations/1867/draft-class-by-year)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_organizations_draft_class_by_year-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_organizations_draft_class_by_year-example}

```python
on3_organizations_draft_class_by_year(organization_key=1867)
```

_Last validated n/a._

## on3_organizations_draft_count_by_stars

GET /rdb/v1/organizations/{organizationKey}/draft-count-by-stars

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/draft-count-by-stars`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/draft-count-by-stars](https://api.on3.com/public/rdb/v1/organizations/1867/draft-count-by-stars)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |

### Returns {#on3_organizations_draft_count_by_stars-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_organizations_draft_count_by_stars-example}

```python
on3_organizations_draft_count_by_stars(organization_key=1867)
```

_Last validated n/a._

## on3_organizations_draft_count_by_year

GET /rdb/v1/organizations/{organizationKey}/draft-count-by-year

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/draft-count-by-year`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/draft-count-by-year](https://api.on3.com/public/rdb/v1/organizations/1867/draft-count-by-year)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_organizations_draft_count_by_year-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_organizations_draft_count_by_year-example}

```python
on3_organizations_draft_count_by_year(organization_key=1867)
```

_Last validated n/a._

## on3_organizations_draft_ranking_summary

GET /rdb/v1/organizations/{organizationKey}/draft-ranking-summary

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/draft-ranking-summary`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/draft-ranking-summary](https://api.on3.com/public/rdb/v1/organizations/1867/draft-ranking-summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |

### Returns {#on3_organizations_draft_ranking_summary-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_organizations_draft_ranking_summary-example}

```python
on3_organizations_draft_ranking_summary(organization_key=1867)
```

_Last validated n/a._

## on3_organizations_drafted_players

GET /rdb/v1/organizations/{organizationKey}/drafted-players

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/drafted-players`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/drafted-players](https://api.on3.com/public/rdb/v1/organizations/1867/drafted-players)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_organizations_drafted_players-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_organizations_drafted_players-example}

```python
on3_organizations_drafted_players(organization_key=1867)
```

_Last validated n/a._

## on3_organizations_drafts_by_stars_summary

GET /rdb/v1/organizations/{organizationKey}/drafts-by-stars-summary

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/drafts-by-stars-summary`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/drafts-by-stars-summary](https://api.on3.com/public/rdb/v1/organizations/1867/drafts-by-stars-summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |

### Returns {#on3_organizations_drafts_by_stars_summary-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_organizations_drafts_by_stars_summary-example}

```python
on3_organizations_drafts_by_stars_summary(organization_key=1867)
```

_Last validated n/a._

## on3_organizations_roster

GET /rdb/v1/organizations/{organizationKey}/roster

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/roster`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/roster](https://api.on3.com/public/rdb/v1/organizations/1867/roster)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_organizations_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `pso_key` | integer | On3 player-sport-organization (PSO) key for the roster entry. |
| `status` | character | Roster status label On3 attaches to the player (usually null). |
| `nil_value` | character | Player's On3 NIL valuation in dollars. |
| `rpm` | character | Nested On3 Recruiting Prediction Machine (RPM) data for the player (stringified). |
| `industry_comparison` | character | Nested comparison of the player's On3 rating against the industry-consensus rating (stringified). |
| `player_key` | integer | On3 numeric key of the player. |
| `player_recruitment_key` | numeric | On3 key of the player's active recruitment record. |
| `player_first_name` | character | First name of the player. |
| `player_last_name` | character | Last name of the player. |
| `player_full_name` | character | Full name of the player. |
| `player_slug` | character | URL slug of the player's On3 profile. |
| `player_date_of_birth` | character | Date of birth of the player (ISO timestamp string). |
| `player_age` | integer | Age of the player in years, when known. |
| `player_high_school_name` | character | High-school display name on the player's record. |
| `player_high_school_key` | integer | On3 numeric key of the player's high school. |
| `player_high_school_full_name` | character | Full name of the player's high school (e.g. 'Alabama Crimson Tide'). |
| `player_high_school_name_2` | character | High-school display name on the player's record (json_normalize de-duplication suffix). |
| `player_high_school_known_as` | character | Common short name of the player's high school, when On3 lists one. |
| `player_high_school_mascot` | character | Mascot of the player's high school. |
| `player_high_school_abbreviation` | character | Abbreviation of the player's high school. |
| `player_high_school_asset_url` | character | Convenience CDN URL of the player's high school's logo. |
| `player_high_school_default_asset_key` | integer | On3 asset key of the player's high school's logo asset. |
| `player_high_school_default_asset_domain_override` | character | CDN domain override for the player's high school's logo asset (usually null). |
| `player_high_school_default_asset_domain` | character | CDN domain serving the player's high school's logo asset. |
| `player_high_school_default_asset_source_override` | character | Source-path override for the player's high school's logo asset (usually null). |
| `player_high_school_default_asset_source` | character | CDN-relative source path of the player's high school's logo asset. |
| `player_high_school_default_asset_title` | character | Editorial title attached to the player's high school's logo asset. |
| `player_high_school_default_asset_description` | character | Editorial description attached to the player's high school's logo asset (usually null). |
| `player_high_school_default_asset_caption` | character | Editorial caption attached to the player's high school's logo asset (usually null). |
| `player_high_school_default_asset_category` | character | Editorial category label of the player's high school's logo asset (usually null). |
| `player_high_school_default_asset_alt_text` | character | Accessibility alt text of the player's high school's logo asset (usually null). |
| `player_high_school_default_asset_height` | integer | Pixel height of the player's high school's logo asset. |
| `player_high_school_default_asset_width` | integer | Pixel width of the player's high school's logo asset. |
| `player_high_school_default_asset_asset_type` | character | On3 asset-type discriminator of the player's high school's logo asset (e.g. Image). |
| `player_high_school_default_asset_file_system` | character | Storage file-system flag of the player's high school's logo asset. |
| `player_high_school_default_asset_path` | character | Storage path of the player's high school's logo asset. |
| `player_high_school_default_asset_type` | character | Media type field of the player's high school's logo asset (file extension, e.g. png). |
| `player_high_school_default_asset_thumbnail` | character | Thumbnail variant of the player's high school's logo asset (video assets; usually null). |
| `player_high_school_default_asset_duration` | integer | Duration of the player's high school's logo asset when it is a video (usually null or 0). |
| `player_high_school_default_asset_mime_type` | character | MIME type of the player's high school's logo asset. |
| `player_high_school_slug` | character | URL slug of the player's high school on On3. |
| `player_high_school_primary_color` | character | Primary hex color of the player's high school. |
| `player_high_school_org_type` | character | Organization type label of the player's high school (e.g. HighSchool, College). |
| `player_high_school_org_type_enum` | character | Organization type enum of the player's high school (same vocabulary as org_type). |
| `player_high_school_division` | character | Division or classification of the player's high school (e.g. NCAA-FB). |
| `player_high_school_site_keys` | character | JSON-encoded On3 site keys covering the player's high school (usually null). |
| `player_high_school_url_slug` | character | URL slug variant of the player's high school's page, with the key appended. |
| `player_hometown_key` | integer | On3 numeric key of the player's home town. |
| `player_hometown_name` | character | Home town of the player as On3 lists it. |
| `player_hometown_abbr` | character | Abbreviated home town of the player (city, state). |
| `player_state_key` | integer | On3 numeric key of the player's home state. |
| `player_state_name` | character | Name of the player's home state. |
| `player_state_abbr` | character | State abbreviation of the player's home town. |
| `player_early_enrollee` | logical | Whether the player early-enrolled at college. |
| `player_early_signee` | logical | Whether the player signed during the early signing period. |
| `player_default_asset_key` | integer | On3 asset key of the player's headshot asset. |
| `player_default_asset_domain_override` | character | CDN domain override for the player's headshot asset (usually null). |
| `player_default_asset_domain` | character | CDN domain serving the player's headshot asset. |
| `player_default_asset_source_override` | character | Source-path override for the player's headshot asset (usually null). |
| `player_default_asset_source` | character | CDN-relative source path of the player's headshot asset. |
| `player_default_asset_title` | character | Editorial title attached to the player's headshot asset. |
| `player_default_asset_description` | character | Editorial description attached to the player's headshot asset (usually null). |
| `player_default_asset_caption` | character | Editorial caption attached to the player's headshot asset (usually null). |
| `player_default_asset_category` | character | Editorial category label of the player's headshot asset (usually null). |
| `player_default_asset_alt_text` | character | Accessibility alt text of the player's headshot asset (usually null). |
| `player_default_asset_height` | integer | Pixel height of the player's headshot asset. |
| `player_default_asset_width` | integer | Pixel width of the player's headshot asset. |
| `player_default_asset_asset_type` | character | On3 asset-type discriminator of the player's headshot asset (e.g. Image). |
| `player_default_asset_file_system` | character | Storage file-system flag of the player's headshot asset. |
| `player_default_asset_path` | character | Storage path of the player's headshot asset. |
| `player_default_asset_type` | character | Media type field of the player's headshot asset (file extension, e.g. png). |
| `player_default_asset_thumbnail` | character | Thumbnail variant of the player's headshot asset (video assets; usually null). |
| `player_default_asset_duration` | integer | Duration of the player's headshot asset when it is a video (usually null or 0). |
| `player_default_asset_mime_type` | character | MIME type of the player's headshot asset. |
| `player_class_year` | integer | High-school graduating class year of the player. |
| `player_class_rank` | character | Academic class standing of the player (e.g. Senior, RedShirt Senior). |
| `player_position_key` | integer | On3 numeric key of the player's position. |
| `player_position_name` | character | Name of the player's position (e.g. Quarterback). |
| `player_position_abbr` | character | Abbreviation of the player's position (e.g. QB). |
| `player_default_sport` | character | Nested primary-sport object of the player (null when On3 ships none). |
| `player_jersey_number` | integer | Jersey number of the player, when listed. |
| `player_height` | character | Height of the player: a formatted string (e.g. '6-8') or inches, depending on the endpoint. |
| `player_weight` | integer | Weight of the player in pounds. |
| `player_division` | character | Division of the player's current organization (e.g. NCAA-FB, NCAA-BK). |
| `player_athlete_verified` | logical | Whether the player's athlete profile is verified by On3. |
| `player_prospect_verified` | logical | Whether the player's prospect measurables are verified by On3. |
| `player_tier` | character | On3 profile tier of the player (e.g. Star). |
| `organization_key` | integer | On3 numeric key of the program. |
| `organization_full_name` | character | Full name of the program (e.g. 'Alabama Crimson Tide'). |
| `organization_name` | character | Short name of the program. |
| `organization_known_as` | character | Common short name of the program, when On3 lists one. |
| `organization_mascot` | character | Mascot of the program. |
| `organization_abbreviation` | character | Abbreviation of the program. |
| `organization_asset_url` | character | Convenience CDN URL of the program's logo. |
| `organization_default_asset_key` | integer | On3 asset key of the program's logo asset. |
| `organization_default_asset_domain_override` | character | CDN domain override for the program's logo asset (usually null). |
| `organization_default_asset_domain` | character | CDN domain serving the program's logo asset. |
| `organization_default_asset_source_override` | character | Source-path override for the program's logo asset (usually null). |
| `organization_default_asset_source` | character | CDN-relative source path of the program's logo asset. |
| `organization_default_asset_title` | character | Editorial title attached to the program's logo asset. |
| `organization_default_asset_description` | character | Editorial description attached to the program's logo asset (usually null). |
| `organization_default_asset_caption` | character | Editorial caption attached to the program's logo asset (usually null). |
| `organization_default_asset_category` | character | Editorial category label of the program's logo asset (usually null). |
| `organization_default_asset_alt_text` | character | Accessibility alt text of the program's logo asset (usually null). |
| `organization_default_asset_height` | integer | Pixel height of the program's logo asset. |
| `organization_default_asset_width` | integer | Pixel width of the program's logo asset. |
| `organization_default_asset_asset_type` | character | On3 asset-type discriminator of the program's logo asset (e.g. Image). |
| `organization_default_asset_file_system` | character | Storage file-system flag of the program's logo asset. |
| `organization_default_asset_path` | character | Storage path of the program's logo asset. |
| `organization_default_asset_type` | character | Media type field of the program's logo asset (file extension, e.g. png). |
| `organization_default_asset_thumbnail` | character | Thumbnail variant of the program's logo asset (video assets; usually null). |
| `organization_default_asset_duration` | integer | Duration of the program's logo asset when it is a video (usually null or 0). |
| `organization_default_asset_mime_type` | character | MIME type of the program's logo asset. |
| `organization_slug` | character | URL slug of the program on On3. |
| `organization_primary_color` | character | Primary hex color of the program. |
| `organization_org_type` | character | Organization type label of the program (e.g. HighSchool, College). |
| `organization_org_type_enum` | character | Organization type enum of the program (same vocabulary as org_type). |
| `organization_division` | character | Division or classification of the program (e.g. NCAA-FB). |
| `organization_site_keys` | character | JSON-encoded On3 site keys covering the program (usually null). |
| `organization_url_slug` | character | URL slug variant of the program's page, with the key appended. |
| `rating_key` | integer | On3 key of the On3 rating record. |
| `rating_rating` | integer | Numeric value of the On3 rating (0-100 scale). |
| `rating_stars` | integer | Star rating of the On3 rating (2-5). |
| `rating_national_rank` | integer | National rank of the On3 rating. |
| `rating_position_rank` | integer | Position rank of the On3 rating. |
| `rating_state_rank` | integer | State rank of the On3 rating. |
| `rating_position_abbr` | character | Position abbreviation the On3 rating was assigned at. |
| `rating_state_abbr` | character | State abbreviation the On3 rating was assigned in. |
| `rating_five_star_plus` | logical | Five-star-plus flag on the On3 rating. |
| `rating_nearly_five_star_plus` | logical | Near-five-star-plus flag on the On3 rating. |
| `rating_consensus_rating` | numeric | Industry-consensus numeric rating paired with the On3 rating (0-100 scale). |
| `rating_consensus_stars` | integer | Industry-consensus star rating paired with the On3 rating (2-5). |
| `rating_consensus_national_rank` | integer | Industry-consensus national rank paired with the On3 rating. |
| `rating_consensus_position_rank` | integer | Industry-consensus position rank paired with the On3 rating. |
| `rating_consensus_state_rank` | integer | Industry-consensus state rank paired with the On3 rating. |
| `rating_year` | integer | Ranking cycle year of the On3 rating. |
| `rating_sport_key` | integer | On3 numeric key of the sport the On3 rating is in. |
| `rating_sport_name` | character | Name of the sport the On3 rating is in (e.g. Football). |
| `rating_sport_abbr` | character | Abbreviation of the sport the On3 rating is in. |
| `roster_rating_key` | numeric | On3 key of the On3 roster rating record. |
| `roster_rating_rating` | numeric | Numeric value of the On3 roster rating (0-100 scale). |
| `roster_rating_stars` | numeric | Star rating of the On3 roster rating (2-5). |
| `roster_rating_national_rank` | character | National rank of the On3 roster rating. |
| `roster_rating_position_rank` | numeric | Position rank of the On3 roster rating. |
| `roster_rating_state_rank` | character | State rank of the On3 roster rating. |
| `roster_rating_position_abbr` | character | Position abbreviation the On3 roster rating was assigned at. |
| `roster_rating_state_abbr` | character | State abbreviation the On3 roster rating was assigned in. |
| `roster_rating_five_star_plus` | character | Five-star-plus flag on the On3 roster rating. |
| `roster_rating_nearly_five_star_plus` | character | Near-five-star-plus flag on the On3 roster rating. |
| `roster_rating_consensus_rating` | character | Industry-consensus numeric rating paired with the On3 roster rating (0-100 scale). |
| `roster_rating_consensus_stars` | numeric | Industry-consensus star rating paired with the On3 roster rating (2-5). |
| `roster_rating_consensus_national_rank` | character | Industry-consensus national rank paired with the On3 roster rating. |
| `roster_rating_consensus_position_rank` | character | Industry-consensus position rank paired with the On3 roster rating. |
| `roster_rating_consensus_state_rank` | character | Industry-consensus state rank paired with the On3 roster rating. |
| `roster_rating_year` | numeric | Ranking cycle year of the On3 roster rating. |
| `roster_rating_sport_key` | numeric | On3 numeric key of the sport the On3 roster rating is in. |
| `roster_rating_sport_name` | character | Name of the sport the On3 roster rating is in (e.g. Football). |
| `roster_rating_sport_abbr` | character | Abbreviation of the sport the On3 roster rating is in. |
| `roster_rating` | character | Nested On3 roster rating object for the player (stringified). |
| `nil_value_key` | numeric | On3 key of the player's NIL valuation record. |
| `nil_value_nil_status` | character | Status of the NIL valuation (e.g. Normal). |
| `nil_value_total_value` | numeric | The NIL valuation in US dollars. |
| `nil_value_rank` | numeric | Overall rank of the NIL valuation across On3's NIL 100. |
| `nil_value_group_rank` | numeric | Rank of the NIL valuation within its group (sport or position). |
| `nil_value_whisper` | character | On3 whisper valuation (reported deal value) behind the NIL valuation, in US dollars. |
| `status_type` | character | Type of the recruiting status (e.g. Committed, Signed, Enrolled, None). |
| `status_date` | character | Date the recruiting status took effect (ISO timestamp string). |
| `status_committed_asset` | character | Nested asset of the committed-to program (the recruiting status; usually null). |
| `status_transferred_asset` | character | Nested asset of the program transferred to (the recruiting status; usually null). |
| `status_decommitted_asset` | character | Nested asset of the program decommitted from (the recruiting status; usually null). |
| `status_transfer_entered` | character | Date the player entered the transfer portal (the recruiting status; null when never entered). |
| `status_transfer_withdrawn` | character | Date the player withdrew from the transfer portal (the recruiting status; null when never withdrawn). |
| `status_recruitment_year` | numeric | Recruiting-cycle year the recruiting status belongs to. |
| `status_short_term_signee` | character | Short-term-signee flag of the recruiting status (null when not applicable). |
| `status_transfer` | character | Transfer flag of the recruiting status (null when not applicable). |
| `status_draft` | character | Draft flag of the recruiting status (null when not applicable). |
| `status_expected_to_transfer` | character | Expected-to-transfer flag of the recruiting status (null when not applicable). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_organizations_roster-example}

```python
on3_organizations_roster(organization_key=1867)
```

_Last validated n/a._

## on3_organizations_roster_header

GET /rdb/v1/organizations/{organizationKey}/roster-header

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/roster-header`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/roster-header](https://api.on3.com/public/rdb/v1/organizations/1867/roster-header)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |

### Returns {#on3_organizations_roster_header-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `talent_rank` | character | Program's current national roster-talent rank per On3. |
| `prev_talent_rank` | character | Program's roster-talent rank in the previous cycle. |
| `conference_rank` | character | Program's roster-talent rank within its conference. |
| `prev_conference_rank` | character | Program's conference roster-talent rank in the previous cycle. |
| `average_rating` | character | Average On3 rating across the roster. |
| `prev_average_rating` | character | Average On3 roster rating in the previous cycle. |
| `average_nil_value` | numeric | Average On3 NIL valuation across the roster, in dollars. |
| `total_nil_value` | integer | Total On3 NIL valuation across the roster, in dollars. |
| `head_coach_key` | integer | On3 numeric key of the head coach. |
| `head_coach_first_name` | character | First name of the head coach. |
| `head_coach_last_name` | character | Last name of the head coach. |
| `head_coach_known_as_name` | character | Preferred name of the head coach, when it differs from the given name. |
| `head_coach_full_name` | character | Full name of the head coach. |
| `head_coach_slug` | character | URL slug of the head coach's On3 profile. |
| `head_coach_default_asset_key` | integer | On3 asset key of the head coach's headshot asset. |
| `head_coach_default_asset_domain_override` | character | CDN domain override for the head coach's headshot asset (usually null). |
| `head_coach_default_asset_domain` | character | CDN domain serving the head coach's headshot asset. |
| `head_coach_default_asset_source_override` | character | Source-path override for the head coach's headshot asset (usually null). |
| `head_coach_default_asset_source` | character | CDN-relative source path of the head coach's headshot asset. |
| `head_coach_default_asset_title` | character | Editorial title attached to the head coach's headshot asset. |
| `head_coach_default_asset_description` | character | Editorial description attached to the head coach's headshot asset (usually null). |
| `head_coach_default_asset_caption` | character | Editorial caption attached to the head coach's headshot asset (usually null). |
| `head_coach_default_asset_category` | character | Editorial category label of the head coach's headshot asset (usually null). |
| `head_coach_default_asset_alt_text` | character | Accessibility alt text of the head coach's headshot asset (usually null). |
| `head_coach_default_asset_height` | integer | Pixel height of the head coach's headshot asset. |
| `head_coach_default_asset_width` | integer | Pixel width of the head coach's headshot asset. |
| `head_coach_default_asset_asset_type` | character | On3 asset-type discriminator of the head coach's headshot asset (e.g. Image). |
| `head_coach_default_asset_file_system` | character | Storage file-system flag of the head coach's headshot asset. |
| `head_coach_default_asset_path` | character | Storage path of the head coach's headshot asset. |
| `head_coach_default_asset_type` | character | Media type field of the head coach's headshot asset (file extension, e.g. png). |
| `head_coach_default_asset_thumbnail` | character | Thumbnail variant of the head coach's headshot asset (video assets; usually null). |
| `head_coach_default_asset_duration` | integer | Duration of the head coach's headshot asset when it is a video (usually null or 0). |
| `head_coach_default_asset_mime_type` | character | MIME type of the head coach's headshot asset. |
| `head_coach_organization_key` | integer | On3 numeric key of the head coach's program. |
| `head_coach_organization_full_name` | character | Full name of the head coach's program (e.g. 'Alabama Crimson Tide'). |
| `head_coach_organization_name` | character | Short name of the head coach's program. |
| `head_coach_organization_known_as` | character | Common short name of the head coach's program, when On3 lists one. |
| `head_coach_organization_mascot` | character | Mascot of the head coach's program. |
| `head_coach_organization_abbreviation` | character | Abbreviation of the head coach's program. |
| `head_coach_organization_asset_url` | character | Convenience CDN URL of the head coach's program's logo. |
| `head_coach_organization_default_asset_key` | integer | On3 asset key of the head coach's program's logo asset. |
| `head_coach_organization_default_asset_domain_override` | character | CDN domain override for the head coach's program's logo asset (usually null). |
| `head_coach_organization_default_asset_domain` | character | CDN domain serving the head coach's program's logo asset. |
| `head_coach_organization_default_asset_source_override` | character | Source-path override for the head coach's program's logo asset (usually null). |
| `head_coach_organization_default_asset_source` | character | CDN-relative source path of the head coach's program's logo asset. |
| `head_coach_organization_default_asset_title` | character | Editorial title attached to the head coach's program's logo asset. |
| `head_coach_organization_default_asset_description` | character | Editorial description attached to the head coach's program's logo asset (usually null). |
| `head_coach_organization_default_asset_caption` | character | Editorial caption attached to the head coach's program's logo asset (usually null). |
| `head_coach_organization_default_asset_category` | character | Editorial category label of the head coach's program's logo asset (usually null). |
| `head_coach_organization_default_asset_alt_text` | character | Accessibility alt text of the head coach's program's logo asset (usually null). |
| `head_coach_organization_default_asset_height` | integer | Pixel height of the head coach's program's logo asset. |
| `head_coach_organization_default_asset_width` | integer | Pixel width of the head coach's program's logo asset. |
| `head_coach_organization_default_asset_asset_type` | character | On3 asset-type discriminator of the head coach's program's logo asset (e.g. Image). |
| `head_coach_organization_default_asset_file_system` | character | Storage file-system flag of the head coach's program's logo asset. |
| `head_coach_organization_default_asset_path` | character | Storage path of the head coach's program's logo asset. |
| `head_coach_organization_default_asset_type` | character | Media type field of the head coach's program's logo asset (file extension, e.g. png). |
| `head_coach_organization_default_asset_thumbnail` | character | Thumbnail variant of the head coach's program's logo asset (video assets; usually null). |
| `head_coach_organization_default_asset_duration` | integer | Duration of the head coach's program's logo asset when it is a video (usually null or 0). |
| `head_coach_organization_default_asset_mime_type` | character | MIME type of the head coach's program's logo asset. |
| `head_coach_organization_slug` | character | URL slug of the head coach's program on On3. |
| `head_coach_organization_primary_color` | character | Primary hex color of the head coach's program. |
| `head_coach_organization_org_type` | character | Organization type label of the head coach's program (e.g. HighSchool, College). |
| `head_coach_organization_org_type_enum` | character | Organization type enum of the head coach's program (same vocabulary as org_type). |
| `head_coach_organization_division` | character | Division or classification of the head coach's program (e.g. NCAA-FB). |
| `head_coach_organization_site_keys` | character | JSON-encoded On3 site keys covering the head coach's program (usually null). |
| `head_coach_organization_url_slug` | character | URL slug variant of the head coach's program's page, with the key appended. |
| `head_coach_primary_position_key` | integer | On3 numeric key of the head coach's primary position. |
| `head_coach_primary_position_name` | character | Name of the head coach's primary position (e.g. Quarterback). |
| `head_coach_primary_position_abbr` | character | Abbreviation of the head coach's primary position (e.g. QB). |
| `head_coach_secondary_position` | character | Secondary position of the head coach, when listed. |
| `head_coach_org_season_count` | integer | Seasons the head coach has spent at the program. |
| `head_coach_years_active` | integer | Years the head coach has been active in coaching. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_organizations_roster_header-example}

```python
on3_organizations_roster_header(organization_key=1867)
```

_Last validated n/a._
