---
title: "CFB — On3 Recruit Database (api.on3.com) — Nil"
sidebar_label: "Nil"
sidebar_position: 2
description: "CFB — On3 Recruit Database (api.on3.com) — Nil — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB — On3 Recruit Database (api.on3.com) — Nil

## on3_nil_100

GET /rdb/v1/nil-100

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/nil-100`

**Valid URL:** [https://api.on3.com/public/rdb/v1/nil-100](https://api.on3.com/public/rdb/v1/nil-100)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | year query parameter. |

### Returns {#on3_nil_100-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_nil_100-example}

```python
on3_nil_100()
```

_Last validated n/a._

## on3_nil_100_v2

GET /rdb/v2/nil-100

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v2/nil-100`

**Valid URL:** [https://api.on3.com/public/rdb/v2/nil-100](https://api.on3.com/public/rdb/v2/nil-100)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | year query parameter. |
| `orgKey` | `org_key` |  |  | `Y` | orgKey query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#on3_nil_100_v2-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `person_rating_consensus_rating` | numeric |  |
| `person_rating_consensus_stars` | integer |  |
| `person_rating_consensus_national_rank` | integer |  |
| `person_rating_consensus_position_rank` | integer |  |
| `person_rating_consensus_state_rank` | integer |  |
| `person_rating_key` | integer |  |
| `person_rating_rating` | numeric |  |
| `person_rating_stars` | integer |  |
| `person_rating_national_rank` | integer |  |
| `person_rating_position_rank` | integer |  |
| `person_rating_state_rank` | integer |  |
| `person_rating_position_abbr` | character |  |
| `person_rating_state_abbr` | character |  |
| `person_rating_five_star_plus` | logical |  |
| `person_division` | character |  |
| `person_default_sport_key` | integer |  |
| `person_default_sport_name` | character |  |
| `person_organization_level` | character |  |
| `person_age` | numeric |  |
| `person_tags` | character |  |
| `person_key` | integer |  |
| `person_recruitment_key` | integer |  |
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
| `person_high_school_default_asset_key` | numeric |  |
| `person_high_school_default_asset_domain_override` | character |  |
| `person_high_school_default_asset_domain` | character |  |
| `person_high_school_default_asset_source_override` | character |  |
| `person_high_school_default_asset_source` | character |  |
| `person_high_school_default_asset_title` | character |  |
| `person_high_school_default_asset_description` | character |  |
| `person_high_school_default_asset_caption` | character |  |
| `person_high_school_default_asset_category` | character |  |
| `person_high_school_default_asset_alt_text` | character |  |
| `person_high_school_default_asset_height` | numeric |  |
| `person_high_school_default_asset_width` | numeric |  |
| `person_high_school_default_asset_asset_type` | character |  |
| `person_high_school_default_asset_file_system` | character |  |
| `person_high_school_default_asset_path` | character |  |
| `person_high_school_default_asset_type` | character |  |
| `person_high_school_default_asset_thumbnail` | character |  |
| `person_high_school_default_asset_duration` | numeric |  |
| `person_high_school_default_asset_mime_type` | character |  |
| `person_high_school_slug` | character |  |
| `person_high_school_primary_color` | character |  |
| `person_high_school_org_type` | character |  |
| `person_high_school_org_type_enum` | character |  |
| `person_high_school_division` | character |  |
| `person_high_school_site_keys` | character |  |
| `person_high_school_url_slug` | character |  |
| `person_home_town_name` | character |  |
| `person_early_enrollee` | logical |  |
| `person_early_signee` | logical |  |
| `person_default_asset_url` | character |  |
| `person_class_year` | integer |  |
| `person_athlete_verified` | logical |  |
| `person_prospect_verified` | logical |  |
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
| `person_position_abbreviation` | character |  |
| `person_height` | character | Height (feet and inches). |
| `person_weight` | integer | Weight in pounds. |
| `person_roster_rating` | character |  |
| `person_commit_status_type` | character |  |
| `person_commit_status_short_term_signee` | logical |  |
| `person_commit_status_date` | character |  |
| `person_commit_status_committed_asset` | character |  |
| `person_commit_status_committed_asset_res` | character |  |
| `person_commit_status_transferred_asset_key` | integer |  |
| `person_commit_status_transferred_asset_url` | character |  |
| `person_commit_status_transferred_asset_slug` | character |  |
| `person_commit_status_transferred_asset_full_name` | character |  |
| `person_commit_status_transferred_asset_res` | character |  |
| `person_commit_status_committed_organization_key` | integer |  |
| `person_commit_status_committed_organization_full_name` | character |  |
| `person_commit_status_committed_organization_name` | character |  |
| `person_commit_status_committed_organization_mascot` | character |  |
| `person_commit_status_committed_organization_abbreviation` | character |  |
| `person_commit_status_committed_organization_asset_url` | character |  |
| `person_commit_status_committed_organization_asset_key` | integer |  |
| `person_commit_status_committed_organization_asset_domain_override` | character |  |
| `person_commit_status_committed_organization_asset_domain` | character |  |
| `person_commit_status_committed_organization_asset_source_override` | character |  |
| `person_commit_status_committed_organization_asset_source` | character |  |
| `person_commit_status_committed_organization_asset_title` | character |  |
| `person_commit_status_committed_organization_asset_description` | character |  |
| `person_commit_status_committed_organization_asset_caption` | character |  |
| `person_commit_status_committed_organization_asset_category` | character |  |
| `person_commit_status_committed_organization_asset_alt_text` | character |  |
| `person_commit_status_committed_organization_asset_height` | integer |  |
| `person_commit_status_committed_organization_asset_width` | integer |  |
| `person_commit_status_committed_organization_asset_asset_type` | character |  |
| `person_commit_status_committed_organization_asset_file_system` | character |  |
| `person_commit_status_committed_organization_asset_path` | character |  |
| `person_commit_status_committed_organization_asset_type` | character |  |
| `person_commit_status_committed_organization_asset_thumbnail` | character |  |
| `person_commit_status_committed_organization_asset_duration` | integer |  |
| `person_commit_status_committed_organization_asset_mime_type` | character |  |
| `person_commit_status_committed_organization_slug` | character |  |
| `person_commit_status_committed_organization_primary_color` | character |  |
| `person_commit_status_class_rank` | character |  |
| `person_commit_status_transfer_entered` | character |  |
| `person_commit_status_recruitment_year` | integer |  |
| `person_commit_status_decommitted_asset` | character |  |
| `person_commit_status_transfer` | logical |  |
| `person_commit_status_expected_to_transfer` | logical |  |
| `person_commit_status_recruitment_key` | integer |  |
| `person_commit_status_withdrawn_transfer` | logical |  |
| `person_commit_status_withdrawn_transfer_date` | character |  |
| `person_predictions` | character |  |
| `person_nil_status` | character |  |
| `person_nil_value` | numeric |  |
| `person_sport` | character |  |
| `valuation_nil_status` | character |  |
| `valuation_valuation` | integer |  |
| `valuation_valuation_change` | integer |  |
| `valuation_followers` | integer |  |
| `valuation_rank` | integer |  |
| `valuation_last_updated` | integer |  |
| `valuation_whisper` | numeric |  |
| `valuation_whisper_change` | numeric |  |
| `valuation_social_valuations` | character |  |
| `valuation_group_rank` | integer |  |
| `valuation_group_name` | character |  |
| `valuation_tags` | character |  |
| `valuation_roster_value` | character |  |
| `valuation_nil_value` | character |  |
| `person_high_school_default_asset` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_nil_100_v2-example}

```python
on3_nil_100_v2()
```

_Last validated n/a._

## on3_nil_compliances_state

GET /rdb/v1/nil-compliances/state

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/nil-compliances/state`

**Valid URL:** [https://api.on3.com/public/rdb/v1/nil-compliances/state](https://api.on3.com/public/rdb/v1/nil-compliances/state)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `stateKey` | `state_key` |  |  | `Y` | stateKey query parameter. |

### Returns {#on3_nil_compliances_state-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_nil_compliances_state-example}

```python
on3_nil_compliances_state()
```

_Last validated n/a._

## on3_nil_rankings

GET /rdb/v1/nil-rankings

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/nil-rankings`

**Valid URL:** [https://api.on3.com/public/rdb/v1/nil-rankings](https://api.on3.com/public/rdb/v1/nil-rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `gender` | `gender` |  |  | `Y` | gender query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `orgType` | `org_type` |  |  | `Y` | orgType query parameter. |
| `positionAbbr` | `position_abbr` |  |  | `Y` | positionAbbr query parameter. |
| `stateAbbr` | `state_abbr` |  |  | `Y` | stateAbbr query parameter. |

### Returns {#on3_nil_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `person_default_sport_key` | integer |  |
| `person_default_sport_name` | character |  |
| `person_default_sport_slug` | character |  |
| `person_default_sport_abbreviation` | character |  |
| `person_default_sport_is_rankable` | logical |  |
| `person_default_sport_is_industry_rankable` | logical |  |
| `person_default_sport_is_scoutable` | logical |  |
| `person_rating_consensus_rating` | numeric |  |
| `person_rating_consensus_stars` | integer |  |
| `person_rating_consensus_national_rank` | integer |  |
| `person_rating_consensus_position_rank` | integer |  |
| `person_rating_consensus_state_rank` | integer |  |
| `person_rating_key` | integer |  |
| `person_rating_rating` | numeric |  |
| `person_rating_stars` | integer |  |
| `person_rating_national_rank` | integer |  |
| `person_rating_position_rank` | integer |  |
| `person_rating_state_rank` | integer |  |
| `person_rating_position_abbr` | character |  |
| `person_rating_state_abbr` | character |  |
| `person_rating_five_star_plus` | logical |  |
| `person_status_is_committed` | logical |  |
| `person_status_is_signed` | logical |  |
| `person_status_is_transfer` | logical |  |
| `person_status_is_enrolled` | logical |  |
| `person_status_commitment_date` | character |  |
| `person_status_committed_organization_key` | numeric |  |
| `person_status_committed_organization_slug` | character |  |
| `person_status_committed_organization_asset_url` | character |  |
| `person_status_committed_organization_asset_key` | numeric |  |
| `person_status_committed_organization_asset_domain_override` | character |  |
| `person_status_committed_organization_asset_domain` | character |  |
| `person_status_committed_organization_asset_source_override` | character |  |
| `person_status_committed_organization_asset_source` | character |  |
| `person_status_committed_organization_asset_title` | character |  |
| `person_status_committed_organization_asset_description` | character |  |
| `person_status_committed_organization_asset_caption` | character |  |
| `person_status_committed_organization_asset_category` | character |  |
| `person_status_committed_organization_asset_alt_text` | character |  |
| `person_status_committed_organization_asset_height` | numeric |  |
| `person_status_committed_organization_asset_width` | numeric |  |
| `person_status_committed_organization_asset_asset_type` | character |  |
| `person_status_committed_organization_asset_file_system` | character |  |
| `person_status_committed_organization_asset_path` | character |  |
| `person_status_committed_organization_asset_type` | character |  |
| `person_status_committed_organization_asset_thumbnail` | character |  |
| `person_status_committed_organization_asset_duration` | numeric |  |
| `person_status_committed_organization_asset_mime_type` | character |  |
| `person_status_committed_organization_primary_color` | character |  |
| `person_status_transferred_from_organization_asset_url` | character |  |
| `person_status_transferred_from_organization_slug` | character |  |
| `person_status_highest_interest_level` | numeric |  |
| `person_status_interest_count` | integer |  |
| `person_status_recruitment_year` | integer |  |
| `person_status_sport_name` | character |  |
| `person_status_short_term_signee` | logical |  |
| `person_predictions` | character |  |
| `person_tags` | character |  |
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
| `valuation_nil_status` | character |  |
| `valuation_valuation` | integer |  |
| `valuation_valuation_change` | integer |  |
| `valuation_followers` | integer |  |
| `valuation_rank` | integer |  |
| `valuation_last_updated` | integer |  |
| `valuation_whisper` | character |  |
| `valuation_whisper_change` | character |  |
| `valuation_social_valuations` | character |  |
| `valuation_group_rank` | integer |  |
| `valuation_group_name` | character |  |
| `valuation_tags` | character |  |
| `valuation_roster_value` | character |  |
| `valuation_nil_value` | character |  |
| `person_status_committed_organization_asset` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_nil_rankings-example}

```python
on3_nil_rankings()
```

_Last validated n/a._
