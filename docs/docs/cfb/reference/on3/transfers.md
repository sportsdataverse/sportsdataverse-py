---
title: "CFB — On3 Recruit Database (api.on3.com) — Transfers"
sidebar_label: "Transfers"
sidebar_position: 7
description: "CFB — On3 Recruit Database (api.on3.com) — Transfers — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB — On3 Recruit Database (api.on3.com) — Transfers

## on3_transfers_best_available

GET /rdb/v1/transfers/best-available

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/transfers/best-available`

**Valid URL:** [https://api.on3.com/public/rdb/v1/transfers/best-available](https://api.on3.com/public/rdb/v1/transfers/best-available)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `orgKey` | `org_key` |  |  | `Y` | orgKey query parameter. |
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `positionAbbr` | `position_abbr` |  |  | `Y` | positionAbbr query parameter. |
| `status` | `status` |  |  | `Y` | status query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `cutoff` | `cutoff` |  |  | `Y` | cutoff query parameter. |
| `orderBy` | `order_by` |  |  | `Y` | orderBy query parameter. |

### Returns {#on3_transfers_best_available-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the transfer entry. |
| `recruitment_key` | integer | On3 recruitment key of the player's transfer recruitment. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `slug` | character | URL slug for the team. |
| `high_school_name` | character | Recruit high-school name. |
| `home_town_name` | character | Player's hometown, as listed by On3. |
| `early_enrollee` | logical | Whether the player is an early enrollee. |
| `early_signee` | logical | Whether the player signed in the early signing period. |
| `default_asset_url` | character | URL of the player's headshot image. |
| `class_year` | integer | Player's original recruiting class year. |
| `athlete_verified` | logical | Whether the athlete has verified their own On3 profile. |
| `prospect_verified` | logical | Whether On3 has verified the prospect's profile information. |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`). |
| `height` | character | Listed height (inches). |
| `weight` | integer | Listed weight (lbs). |
| `transfer_industry_comparison` | character |  |
| `predictions` | character | RPM prediction entries for the transfer destination, as a stringified list. |
| `nil_status` | character | Status of the player's On3 NIL valuation (e.g. active, inactive). |
| `nil_value` | integer | Player's On3 NIL valuation in dollars. |
| `sport` | character | Nested On3 sport object for the transfer entry (stringified). |
| `eligibility` | character | Eligibility status. |
| `organization_history` | character |  |
| `industry_comparison` | character |  |
| `entered_article` | character | On3 article link covering the player entering the transfer portal (stringified). |
| `exited_article` | character | On3 article link covering the player exiting the portal (stringified). |
| `person_sport_key` | integer |  |
| `withdrawn_transfer` | logical |  |
| `withdrawn_transfer_date` | character |  |
| `rec_status` | character |  |
| `index` | integer | Index of the play event within the at-bat. |
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
| `rating_consensus_rating` | numeric |  |
| `rating_consensus_stars` | integer |  |
| `rating_consensus_national_rank` | integer |  |
| `rating_consensus_position_rank` | integer |  |
| `rating_consensus_state_rank` | integer |  |
| `rating_key` | integer |  |
| `rating_rating` | numeric |  |
| `rating_stars` | integer |  |
| `rating_national_rank` | integer |  |
| `rating_position_rank` | integer |  |
| `rating_state_rank` | integer |  |
| `rating_position_abbr` | character |  |
| `rating_state_abbr` | character |  |
| `rating_five_star_plus` | logical |  |
| `roster_rating_consensus_rating` | numeric |  |
| `roster_rating_consensus_stars` | integer |  |
| `roster_rating_consensus_national_rank` | integer |  |
| `roster_rating_consensus_position_rank` | integer |  |
| `roster_rating_consensus_state_rank` | character |  |
| `roster_rating_key` | integer |  |
| `roster_rating_rating` | integer |  |
| `roster_rating_stars` | integer |  |
| `roster_rating_national_rank` | integer |  |
| `roster_rating_position_rank` | integer |  |
| `roster_rating_state_rank` | integer |  |
| `roster_rating_position_abbr` | character |  |
| `roster_rating_state_abbr` | character |  |
| `roster_rating_five_star_plus` | logical |  |
| `transfer_rating_consensus_rating` | numeric |  |
| `transfer_rating_consensus_stars` | integer |  |
| `transfer_rating_consensus_national_rank` | integer |  |
| `transfer_rating_consensus_position_rank` | integer |  |
| `transfer_rating_consensus_state_rank` | character |  |
| `transfer_rating_key` | integer |  |
| `transfer_rating_rating` | integer |  |
| `transfer_rating_stars` | integer |  |
| `transfer_rating_national_rank` | integer |  |
| `transfer_rating_position_rank` | integer |  |
| `transfer_rating_state_rank` | character |  |
| `transfer_rating_position_abbr` | character |  |
| `transfer_rating_state_abbr` | character |  |
| `transfer_rating_five_star_plus` | logical |  |
| `commit_status_type` | character |  |
| `commit_status_short_term_signee` | logical |  |
| `commit_status_date` | character |  |
| `commit_status_committed_asset` | character |  |
| `commit_status_committed_asset_res` | character |  |
| `commit_status_transferred_asset` | character |  |
| `commit_status_transferred_asset_res` | character |  |
| `commit_status_committed_organization_key` | integer |  |
| `commit_status_committed_organization_full_name` | character |  |
| `commit_status_committed_organization_name` | character |  |
| `commit_status_committed_organization_mascot` | character |  |
| `commit_status_committed_organization_abbreviation` | character |  |
| `commit_status_committed_organization_asset_url` | character |  |
| `commit_status_committed_organization_asset_key` | integer |  |
| `commit_status_committed_organization_asset_domain_override` | character |  |
| `commit_status_committed_organization_asset_domain` | character |  |
| `commit_status_committed_organization_asset_source_override` | character |  |
| `commit_status_committed_organization_asset_source` | character |  |
| `commit_status_committed_organization_asset_title` | character |  |
| `commit_status_committed_organization_asset_description` | character |  |
| `commit_status_committed_organization_asset_caption` | character |  |
| `commit_status_committed_organization_asset_category` | character |  |
| `commit_status_committed_organization_asset_alt_text` | character |  |
| `commit_status_committed_organization_asset_height` | integer |  |
| `commit_status_committed_organization_asset_width` | integer |  |
| `commit_status_committed_organization_asset_asset_type` | character |  |
| `commit_status_committed_organization_asset_file_system` | character |  |
| `commit_status_committed_organization_asset_path` | character |  |
| `commit_status_committed_organization_asset_type` | character |  |
| `commit_status_committed_organization_asset_thumbnail` | character |  |
| `commit_status_committed_organization_asset_duration` | integer |  |
| `commit_status_committed_organization_asset_mime_type` | character |  |
| `commit_status_committed_organization_slug` | character |  |
| `commit_status_committed_organization_primary_color` | character |  |
| `commit_status_class_rank` | character |  |
| `commit_status_transfer_entered` | character |  |
| `commit_status_recruitment_year` | integer |  |
| `commit_status_decommitted_asset` | character |  |
| `commit_status_transfer` | logical |  |
| `commit_status_expected_to_transfer` | logical |  |
| `commit_status_recruitment_key` | integer |  |
| `commit_status_withdrawn_transfer` | logical |  |
| `commit_status_withdrawn_transfer_date` | character |  |
| `valuation_key` | integer |  |
| `valuation_nil_status` | character |  |
| `valuation_total_value` | integer |  |
| `valuation_rank` | numeric |  |
| `valuation_group_rank` | numeric |  |
| `valuation_whisper` | numeric |  |
| `last_team_asset_url_key` | integer |  |
| `last_team_asset_url_url` | character |  |
| `last_team_asset_url_slug` | character |  |
| `last_team_asset_url_full_name` | character |  |
| `last_team_full_name` | character |  |
| `last_team_key` | integer |  |
| `last_team_name` | character |  |
| `last_team_mascot` | character |  |
| `last_team_abbreviation` | character |  |
| `last_team_asset_key` | integer |  |
| `last_team_asset_domain_override` | character |  |
| `last_team_asset_domain` | character |  |
| `last_team_asset_source_override` | character |  |
| `last_team_asset_source` | character |  |
| `last_team_asset_title` | character |  |
| `last_team_asset_description` | character |  |
| `last_team_asset_caption` | character |  |
| `last_team_asset_category` | character |  |
| `last_team_asset_alt_text` | character |  |
| `last_team_asset_height` | integer |  |
| `last_team_asset_width` | integer |  |
| `last_team_asset_asset_type` | character |  |
| `last_team_asset_file_system` | character |  |
| `last_team_asset_path` | character |  |
| `last_team_asset_type` | character |  |
| `last_team_asset_thumbnail` | character |  |
| `last_team_asset_duration` | integer |  |
| `last_team_asset_mime_type` | character |  |
| `last_team_slug` | character |  |
| `last_team_primary_color` | character |  |
| `committed_article_key` | numeric |  |
| `committed_article_slug` | character |  |
| `committed_article_full_url` | character |  |
| `committed_article_title` | character |  |
| `committed_article_is_premium` | character |  |
| `committed_article_date_published_gmt` | character |  |
| `interest_status_type` | character |  |
| `interest_status_short_term_signee` | logical |  |
| `interest_status_date` | character |  |
| `interest_status_committed_asset` | character |  |
| `interest_status_committed_asset_res` | character |  |
| `interest_status_transferred_asset` | character |  |
| `interest_status_transferred_asset_res` | character |  |
| `interest_status_committed_organization_key` | integer |  |
| `interest_status_committed_organization_full_name` | character |  |
| `interest_status_committed_organization_name` | character |  |
| `interest_status_committed_organization_mascot` | character |  |
| `interest_status_committed_organization_abbreviation` | character |  |
| `interest_status_committed_organization_asset_url` | character |  |
| `interest_status_committed_organization_asset_key` | integer |  |
| `interest_status_committed_organization_asset_domain_override` | character |  |
| `interest_status_committed_organization_asset_domain` | character |  |
| `interest_status_committed_organization_asset_source_override` | character |  |
| `interest_status_committed_organization_asset_source` | character |  |
| `interest_status_committed_organization_asset_title` | character |  |
| `interest_status_committed_organization_asset_description` | character |  |
| `interest_status_committed_organization_asset_caption` | character |  |
| `interest_status_committed_organization_asset_category` | character |  |
| `interest_status_committed_organization_asset_alt_text` | character |  |
| `interest_status_committed_organization_asset_height` | integer |  |
| `interest_status_committed_organization_asset_width` | integer |  |
| `interest_status_committed_organization_asset_asset_type` | character |  |
| `interest_status_committed_organization_asset_file_system` | character |  |
| `interest_status_committed_organization_asset_path` | character |  |
| `interest_status_committed_organization_asset_type` | character |  |
| `interest_status_committed_organization_asset_thumbnail` | character |  |
| `interest_status_committed_organization_asset_duration` | integer |  |
| `interest_status_committed_organization_asset_mime_type` | character |  |
| `interest_status_committed_organization_slug` | character |  |
| `interest_status_committed_organization_primary_color` | character |  |
| `interest_status_class_rank` | character |  |
| `interest_status_transfer_entered` | character |  |
| `interest_status_recruitment_year` | character |  |
| `interest_status_decommitted_asset` | character |  |
| `interest_status_transfer` | logical |  |
| `interest_status_expected_to_transfer` | logical |  |
| `interest_status_recruitment_key` | integer |  |
| `interest_status_withdrawn_transfer` | logical |  |
| `interest_status_withdrawn_transfer_date` | character |  |
| `committed_article` | character | On3 article link covering the player's transfer commitment (stringified). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_transfers_best_available-example}

```python
on3_transfers_best_available()
```

_Last validated n/a._

## on3_transfers_latest

GET /rdb/v1/transfers/latest

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/transfers/latest`

**Valid URL:** [https://api.on3.com/public/rdb/v1/transfers/latest](https://api.on3.com/public/rdb/v1/transfers/latest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `orgKey` | `org_key` |  |  | `Y` | orgKey query parameter. |
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `positionAbbr` | `position_abbr` |  |  | `Y` | positionAbbr query parameter. |
| `status` | `status` |  |  | `Y` | status query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#on3_transfers_latest-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the transfer entry. |
| `recruitment_key` | integer | On3 recruitment key of the player's transfer recruitment. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `slug` | character | URL slug for the team. |
| `high_school_name` | character | Recruit high-school name. |
| `home_town_name` | character | Player's hometown, as listed by On3. |
| `early_enrollee` | logical | Whether the player is an early enrollee. |
| `early_signee` | logical | Whether the player signed in the early signing period. |
| `default_asset_url` | character | URL of the player's headshot image. |
| `class_year` | integer | Player's original recruiting class year. |
| `athlete_verified` | logical | Whether the athlete has verified their own On3 profile. |
| `prospect_verified` | logical | Whether On3 has verified the prospect's profile information. |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`). |
| `height` | character | Listed height (inches). |
| `weight` | integer | Listed weight (lbs). |
| `transfer_industry_comparison` | character |  |
| `predictions` | character | RPM prediction entries for the transfer destination, as a stringified list. |
| `nil_status` | character | Status of the player's On3 NIL valuation (e.g. active, inactive). |
| `nil_value` | numeric | Player's On3 NIL valuation in dollars. |
| `valuation` | character |  |
| `sport` | character | Nested On3 sport object for the transfer entry (stringified). |
| `eligibility` | character | Eligibility status. |
| `organization_history` | character |  |
| `industry_comparison` | character |  |
| `entered_article` | character | On3 article link covering the player entering the transfer portal (stringified). |
| `exited_article` | character | On3 article link covering the player exiting the portal (stringified). |
| `person_sport_key` | integer |  |
| `withdrawn_transfer` | logical |  |
| `withdrawn_transfer_date` | character |  |
| `rec_status` | character |  |
| `index` | integer | Index of the play event within the at-bat. |
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
| `rating_consensus_rating` | character |  |
| `rating_consensus_stars` | numeric |  |
| `rating_consensus_national_rank` | character |  |
| `rating_consensus_position_rank` | character |  |
| `rating_consensus_state_rank` | character |  |
| `rating_key` | numeric |  |
| `rating_rating` | numeric |  |
| `rating_stars` | numeric |  |
| `rating_national_rank` | integer |  |
| `rating_position_rank` | integer |  |
| `rating_state_rank` | integer |  |
| `rating_position_abbr` | character |  |
| `rating_state_abbr` | character |  |
| `rating_five_star_plus` | character |  |
| `roster_rating_consensus_rating` | integer |  |
| `roster_rating_consensus_stars` | integer |  |
| `roster_rating_consensus_national_rank` | integer |  |
| `roster_rating_consensus_position_rank` | integer |  |
| `roster_rating_consensus_state_rank` | character |  |
| `roster_rating_key` | integer |  |
| `roster_rating_rating` | integer |  |
| `roster_rating_stars` | integer |  |
| `roster_rating_national_rank` | integer |  |
| `roster_rating_position_rank` | integer |  |
| `roster_rating_state_rank` | integer |  |
| `roster_rating_position_abbr` | character |  |
| `roster_rating_state_abbr` | character |  |
| `roster_rating_five_star_plus` | logical |  |
| `transfer_rating_consensus_rating` | integer |  |
| `transfer_rating_consensus_stars` | integer |  |
| `transfer_rating_consensus_national_rank` | integer |  |
| `transfer_rating_consensus_position_rank` | integer |  |
| `transfer_rating_consensus_state_rank` | character |  |
| `transfer_rating_key` | integer |  |
| `transfer_rating_rating` | integer |  |
| `transfer_rating_stars` | integer |  |
| `transfer_rating_national_rank` | character |  |
| `transfer_rating_position_rank` | integer |  |
| `transfer_rating_state_rank` | character |  |
| `transfer_rating_position_abbr` | character |  |
| `transfer_rating_state_abbr` | character |  |
| `transfer_rating_five_star_plus` | logical |  |
| `commit_status_type` | character |  |
| `commit_status_short_term_signee` | logical |  |
| `commit_status_date` | character |  |
| `commit_status_committed_asset` | character |  |
| `commit_status_committed_asset_res` | character |  |
| `commit_status_transferred_asset` | character |  |
| `commit_status_transferred_asset_res` | character |  |
| `commit_status_committed_organization_key` | integer |  |
| `commit_status_committed_organization_full_name` | character |  |
| `commit_status_committed_organization_name` | character |  |
| `commit_status_committed_organization_mascot` | character |  |
| `commit_status_committed_organization_abbreviation` | character |  |
| `commit_status_committed_organization_asset_url` | character |  |
| `commit_status_committed_organization_asset_key` | integer |  |
| `commit_status_committed_organization_asset_domain_override` | character |  |
| `commit_status_committed_organization_asset_domain` | character |  |
| `commit_status_committed_organization_asset_source_override` | character |  |
| `commit_status_committed_organization_asset_source` | character |  |
| `commit_status_committed_organization_asset_title` | character |  |
| `commit_status_committed_organization_asset_description` | character |  |
| `commit_status_committed_organization_asset_caption` | character |  |
| `commit_status_committed_organization_asset_category` | character |  |
| `commit_status_committed_organization_asset_alt_text` | character |  |
| `commit_status_committed_organization_asset_height` | integer |  |
| `commit_status_committed_organization_asset_width` | integer |  |
| `commit_status_committed_organization_asset_asset_type` | character |  |
| `commit_status_committed_organization_asset_file_system` | character |  |
| `commit_status_committed_organization_asset_path` | character |  |
| `commit_status_committed_organization_asset_type` | character |  |
| `commit_status_committed_organization_asset_thumbnail` | character |  |
| `commit_status_committed_organization_asset_duration` | integer |  |
| `commit_status_committed_organization_asset_mime_type` | character |  |
| `commit_status_committed_organization_slug` | character |  |
| `commit_status_committed_organization_primary_color` | character |  |
| `commit_status_class_rank` | character |  |
| `commit_status_transfer_entered` | character |  |
| `commit_status_recruitment_year` | integer |  |
| `commit_status_decommitted_asset` | character |  |
| `commit_status_transfer` | logical |  |
| `commit_status_expected_to_transfer` | logical |  |
| `commit_status_recruitment_key` | integer |  |
| `commit_status_withdrawn_transfer` | logical |  |
| `commit_status_withdrawn_transfer_date` | character |  |
| `last_team_asset_url_key` | integer |  |
| `last_team_asset_url_url` | character |  |
| `last_team_asset_url_slug` | character |  |
| `last_team_asset_url_full_name` | character |  |
| `last_team_full_name` | character |  |
| `last_team_key` | integer |  |
| `last_team_name` | character |  |
| `last_team_mascot` | character |  |
| `last_team_abbreviation` | character |  |
| `last_team_asset_key` | integer |  |
| `last_team_asset_domain_override` | character |  |
| `last_team_asset_domain` | character |  |
| `last_team_asset_source_override` | character |  |
| `last_team_asset_source` | character |  |
| `last_team_asset_title` | character |  |
| `last_team_asset_description` | character |  |
| `last_team_asset_caption` | character |  |
| `last_team_asset_category` | character |  |
| `last_team_asset_alt_text` | character |  |
| `last_team_asset_height` | integer |  |
| `last_team_asset_width` | integer |  |
| `last_team_asset_asset_type` | character |  |
| `last_team_asset_file_system` | character |  |
| `last_team_asset_path` | character |  |
| `last_team_asset_type` | character |  |
| `last_team_asset_thumbnail` | character |  |
| `last_team_asset_duration` | integer |  |
| `last_team_asset_mime_type` | character |  |
| `last_team_slug` | character |  |
| `last_team_primary_color` | character |  |
| `committed_article_key` | numeric |  |
| `committed_article_slug` | character |  |
| `committed_article_full_url` | character |  |
| `committed_article_title` | character |  |
| `committed_article_is_premium` | character |  |
| `committed_article_date_published_gmt` | character |  |
| `interest_status_type` | character |  |
| `interest_status_short_term_signee` | logical |  |
| `interest_status_date` | character |  |
| `interest_status_committed_asset` | character |  |
| `interest_status_committed_asset_res` | character |  |
| `interest_status_transferred_asset` | character |  |
| `interest_status_transferred_asset_res` | character |  |
| `interest_status_committed_organization_key` | integer |  |
| `interest_status_committed_organization_full_name` | character |  |
| `interest_status_committed_organization_name` | character |  |
| `interest_status_committed_organization_mascot` | character |  |
| `interest_status_committed_organization_abbreviation` | character |  |
| `interest_status_committed_organization_asset_url` | character |  |
| `interest_status_committed_organization_asset_key` | integer |  |
| `interest_status_committed_organization_asset_domain_override` | character |  |
| `interest_status_committed_organization_asset_domain` | character |  |
| `interest_status_committed_organization_asset_source_override` | character |  |
| `interest_status_committed_organization_asset_source` | character |  |
| `interest_status_committed_organization_asset_title` | character |  |
| `interest_status_committed_organization_asset_description` | character |  |
| `interest_status_committed_organization_asset_caption` | character |  |
| `interest_status_committed_organization_asset_category` | character |  |
| `interest_status_committed_organization_asset_alt_text` | character |  |
| `interest_status_committed_organization_asset_height` | integer |  |
| `interest_status_committed_organization_asset_width` | integer |  |
| `interest_status_committed_organization_asset_asset_type` | character |  |
| `interest_status_committed_organization_asset_file_system` | character |  |
| `interest_status_committed_organization_asset_path` | character |  |
| `interest_status_committed_organization_asset_type` | character |  |
| `interest_status_committed_organization_asset_thumbnail` | character |  |
| `interest_status_committed_organization_asset_duration` | integer |  |
| `interest_status_committed_organization_asset_mime_type` | character |  |
| `interest_status_committed_organization_slug` | character |  |
| `interest_status_committed_organization_primary_color` | character |  |
| `interest_status_class_rank` | character |  |
| `interest_status_transfer_entered` | character |  |
| `interest_status_recruitment_year` | character |  |
| `interest_status_decommitted_asset` | character |  |
| `interest_status_transfer` | logical |  |
| `interest_status_expected_to_transfer` | logical |  |
| `interest_status_recruitment_key` | integer |  |
| `interest_status_withdrawn_transfer` | logical |  |
| `interest_status_withdrawn_transfer_date` | character |  |
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `committed_article` | character | On3 article link covering the player's transfer commitment (stringified). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_transfers_latest-example}

```python
on3_transfers_latest()
```

_Last validated n/a._
