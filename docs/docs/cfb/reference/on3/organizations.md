---
title: "CFB — On3 Recruit Database (api.on3.com) — Organizations"
sidebar_label: "Organizations"
sidebar_position: 3
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
| `status` | character | Game status (e.g. "scheduled", "in_progress", "completed"). |
| `nil_value` | character | Player's On3 NIL valuation in dollars. |
| `rpm` | character | Nested On3 Recruiting Prediction Machine (RPM) data for the player (stringified). |
| `industry_comparison` | character | Nested comparison of the player's On3 rating against the industry-consensus rating (stringified). |
| `player_key` | integer |  |
| `player_recruitment_key` | numeric |  |
| `player_first_name` | character | Player's first name |
| `player_last_name` | character | Player's last name |
| `player_full_name` | character | Player full name. |
| `player_slug` | character | URL-safe player identifier. |
| `player_date_of_birth` | character |  |
| `player_age` | integer |  |
| `player_high_school_name` | character |  |
| `player_high_school_key` | integer |  |
| `player_high_school_full_name` | character |  |
| `player_high_school_name_2` | character |  |
| `player_high_school_known_as` | character |  |
| `player_high_school_mascot` | character |  |
| `player_high_school_abbreviation` | character |  |
| `player_high_school_asset_url` | character |  |
| `player_high_school_default_asset_key` | integer |  |
| `player_high_school_default_asset_domain_override` | character |  |
| `player_high_school_default_asset_domain` | character |  |
| `player_high_school_default_asset_source_override` | character |  |
| `player_high_school_default_asset_source` | character |  |
| `player_high_school_default_asset_title` | character |  |
| `player_high_school_default_asset_description` | character |  |
| `player_high_school_default_asset_caption` | character |  |
| `player_high_school_default_asset_category` | character |  |
| `player_high_school_default_asset_alt_text` | character |  |
| `player_high_school_default_asset_height` | integer |  |
| `player_high_school_default_asset_width` | integer |  |
| `player_high_school_default_asset_asset_type` | character |  |
| `player_high_school_default_asset_file_system` | character |  |
| `player_high_school_default_asset_path` | character |  |
| `player_high_school_default_asset_type` | character |  |
| `player_high_school_default_asset_thumbnail` | character |  |
| `player_high_school_default_asset_duration` | integer |  |
| `player_high_school_default_asset_mime_type` | character |  |
| `player_high_school_slug` | character |  |
| `player_high_school_primary_color` | character |  |
| `player_high_school_org_type` | character |  |
| `player_high_school_org_type_enum` | character |  |
| `player_high_school_division` | character |  |
| `player_high_school_site_keys` | character |  |
| `player_high_school_url_slug` | character |  |
| `player_hometown_key` | integer |  |
| `player_hometown_name` | character |  |
| `player_hometown_abbr` | character |  |
| `player_state_key` | integer |  |
| `player_state_name` | character |  |
| `player_state_abbr` | character |  |
| `player_early_enrollee` | logical |  |
| `player_early_signee` | logical |  |
| `player_default_asset_key` | integer |  |
| `player_default_asset_domain_override` | character |  |
| `player_default_asset_domain` | character |  |
| `player_default_asset_source_override` | character |  |
| `player_default_asset_source` | character |  |
| `player_default_asset_title` | character |  |
| `player_default_asset_description` | character |  |
| `player_default_asset_caption` | character |  |
| `player_default_asset_category` | character |  |
| `player_default_asset_alt_text` | character |  |
| `player_default_asset_height` | integer |  |
| `player_default_asset_width` | integer |  |
| `player_default_asset_asset_type` | character |  |
| `player_default_asset_file_system` | character |  |
| `player_default_asset_path` | character |  |
| `player_default_asset_type` | character |  |
| `player_default_asset_thumbnail` | character |  |
| `player_default_asset_duration` | integer |  |
| `player_default_asset_mime_type` | character |  |
| `player_class_year` | integer |  |
| `player_class_rank` | character |  |
| `player_position_key` | integer |  |
| `player_position_name` | character |  |
| `player_position_abbr` | character |  |
| `player_default_sport` | character |  |
| `player_jersey_number` | integer | Player's jersey number |
| `player_height` | character | Participant height (e.g. "6' 5\""). |
| `player_weight` | integer | Participant weight in pounds. |
| `player_division` | character |  |
| `player_athlete_verified` | logical |  |
| `player_prospect_verified` | logical |  |
| `player_tier` | character |  |
| `organization_key` | integer |  |
| `organization_full_name` | character |  |
| `organization_name` | character |  |
| `organization_known_as` | character |  |
| `organization_mascot` | character |  |
| `organization_abbreviation` | character |  |
| `organization_asset_url` | character |  |
| `organization_default_asset_key` | integer |  |
| `organization_default_asset_domain_override` | character |  |
| `organization_default_asset_domain` | character |  |
| `organization_default_asset_source_override` | character |  |
| `organization_default_asset_source` | character |  |
| `organization_default_asset_title` | character |  |
| `organization_default_asset_description` | character |  |
| `organization_default_asset_caption` | character |  |
| `organization_default_asset_category` | character |  |
| `organization_default_asset_alt_text` | character |  |
| `organization_default_asset_height` | integer |  |
| `organization_default_asset_width` | integer |  |
| `organization_default_asset_asset_type` | character |  |
| `organization_default_asset_file_system` | character |  |
| `organization_default_asset_path` | character |  |
| `organization_default_asset_type` | character |  |
| `organization_default_asset_thumbnail` | character |  |
| `organization_default_asset_duration` | integer |  |
| `organization_default_asset_mime_type` | character |  |
| `organization_slug` | character |  |
| `organization_primary_color` | character |  |
| `organization_org_type` | character |  |
| `organization_org_type_enum` | character |  |
| `organization_division` | character |  |
| `organization_site_keys` | character |  |
| `organization_url_slug` | character |  |
| `rating_key` | integer |  |
| `rating_rating` | integer |  |
| `rating_stars` | integer |  |
| `rating_national_rank` | integer |  |
| `rating_position_rank` | integer |  |
| `rating_state_rank` | integer |  |
| `rating_position_abbr` | character |  |
| `rating_state_abbr` | character |  |
| `rating_five_star_plus` | logical |  |
| `rating_nearly_five_star_plus` | logical |  |
| `rating_consensus_rating` | numeric |  |
| `rating_consensus_stars` | integer |  |
| `rating_consensus_national_rank` | integer |  |
| `rating_consensus_position_rank` | integer |  |
| `rating_consensus_state_rank` | integer |  |
| `rating_year` | integer |  |
| `rating_sport_key` | integer |  |
| `rating_sport_name` | character |  |
| `rating_sport_abbr` | character |  |
| `roster_rating_key` | numeric |  |
| `roster_rating_rating` | numeric |  |
| `roster_rating_stars` | numeric |  |
| `roster_rating_national_rank` | character |  |
| `roster_rating_position_rank` | numeric |  |
| `roster_rating_state_rank` | character |  |
| `roster_rating_position_abbr` | character |  |
| `roster_rating_state_abbr` | character |  |
| `roster_rating_five_star_plus` | character |  |
| `roster_rating_nearly_five_star_plus` | character |  |
| `roster_rating_consensus_rating` | character |  |
| `roster_rating_consensus_stars` | numeric |  |
| `roster_rating_consensus_national_rank` | character |  |
| `roster_rating_consensus_position_rank` | character |  |
| `roster_rating_consensus_state_rank` | character |  |
| `roster_rating_year` | numeric |  |
| `roster_rating_sport_key` | numeric |  |
| `roster_rating_sport_name` | character |  |
| `roster_rating_sport_abbr` | character |  |
| `roster_rating` | character | Nested On3 roster rating object for the player (stringified). |
| `nil_value_key` | numeric |  |
| `nil_value_nil_status` | character |  |
| `nil_value_total_value` | numeric |  |
| `nil_value_rank` | numeric |  |
| `nil_value_group_rank` | numeric |  |
| `nil_value_whisper` | character |  |
| `status_type` | character | Status type. |
| `status_date` | character |  |
| `status_committed_asset` | character |  |
| `status_transferred_asset` | character |  |
| `status_decommitted_asset` | character |  |
| `status_transfer_entered` | character |  |
| `status_transfer_withdrawn` | character |  |
| `status_recruitment_year` | numeric |  |
| `status_short_term_signee` | character |  |
| `status_transfer` | character |  |
| `status_draft` | character |  |
| `status_expected_to_transfer` | character |  |

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
| `head_coach_key` | integer |  |
| `head_coach_first_name` | character |  |
| `head_coach_last_name` | character |  |
| `head_coach_known_as_name` | character |  |
| `head_coach_full_name` | character |  |
| `head_coach_slug` | character |  |
| `head_coach_default_asset_key` | integer |  |
| `head_coach_default_asset_domain_override` | character |  |
| `head_coach_default_asset_domain` | character |  |
| `head_coach_default_asset_source_override` | character |  |
| `head_coach_default_asset_source` | character |  |
| `head_coach_default_asset_title` | character |  |
| `head_coach_default_asset_description` | character |  |
| `head_coach_default_asset_caption` | character |  |
| `head_coach_default_asset_category` | character |  |
| `head_coach_default_asset_alt_text` | character |  |
| `head_coach_default_asset_height` | integer |  |
| `head_coach_default_asset_width` | integer |  |
| `head_coach_default_asset_asset_type` | character |  |
| `head_coach_default_asset_file_system` | character |  |
| `head_coach_default_asset_path` | character |  |
| `head_coach_default_asset_type` | character |  |
| `head_coach_default_asset_thumbnail` | character |  |
| `head_coach_default_asset_duration` | integer |  |
| `head_coach_default_asset_mime_type` | character |  |
| `head_coach_organization_key` | integer |  |
| `head_coach_organization_full_name` | character |  |
| `head_coach_organization_name` | character |  |
| `head_coach_organization_known_as` | character |  |
| `head_coach_organization_mascot` | character |  |
| `head_coach_organization_abbreviation` | character |  |
| `head_coach_organization_asset_url` | character |  |
| `head_coach_organization_default_asset_key` | integer |  |
| `head_coach_organization_default_asset_domain_override` | character |  |
| `head_coach_organization_default_asset_domain` | character |  |
| `head_coach_organization_default_asset_source_override` | character |  |
| `head_coach_organization_default_asset_source` | character |  |
| `head_coach_organization_default_asset_title` | character |  |
| `head_coach_organization_default_asset_description` | character |  |
| `head_coach_organization_default_asset_caption` | character |  |
| `head_coach_organization_default_asset_category` | character |  |
| `head_coach_organization_default_asset_alt_text` | character |  |
| `head_coach_organization_default_asset_height` | integer |  |
| `head_coach_organization_default_asset_width` | integer |  |
| `head_coach_organization_default_asset_asset_type` | character |  |
| `head_coach_organization_default_asset_file_system` | character |  |
| `head_coach_organization_default_asset_path` | character |  |
| `head_coach_organization_default_asset_type` | character |  |
| `head_coach_organization_default_asset_thumbnail` | character |  |
| `head_coach_organization_default_asset_duration` | integer |  |
| `head_coach_organization_default_asset_mime_type` | character |  |
| `head_coach_organization_slug` | character |  |
| `head_coach_organization_primary_color` | character |  |
| `head_coach_organization_org_type` | character |  |
| `head_coach_organization_org_type_enum` | character |  |
| `head_coach_organization_division` | character |  |
| `head_coach_organization_site_keys` | character |  |
| `head_coach_organization_url_slug` | character |  |
| `head_coach_primary_position_key` | integer |  |
| `head_coach_primary_position_name` | character |  |
| `head_coach_primary_position_abbr` | character |  |
| `head_coach_secondary_position` | character |  |
| `head_coach_org_season_count` | integer |  |
| `head_coach_years_active` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_organizations_roster_header-example}

```python
on3_organizations_roster_header(organization_key=1867)
```

_Last validated n/a._
