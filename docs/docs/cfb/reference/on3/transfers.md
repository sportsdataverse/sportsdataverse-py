---
title: "College football — On3 Recruit Database (api.on3.com) — Transfers"
sidebar_label: "Transfers"
sidebar_position: 10
description: "College football — On3 Recruit Database (api.on3.com) — Transfers — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# College football — On3 Recruit Database (api.on3.com) — Transfers

## on3_transfers_best_available

Transfers best available.

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
| `key` | Int64 | On3 RDB key for the transfer entry. |
| `recruitment_key` | Int64 | On3 recruitment key of the player's transfer recruitment. |
| `name` | String | Display name of the row's record. |
| `slug` | String | URL slug of the row's record on On3. |
| `high_school_name` | String | High-school display name (top-level field). |
| `home_town_name` | String | Player's hometown, as listed by On3. |
| `early_enrollee` | Boolean | Whether the player is an early enrollee. |
| `early_signee` | Boolean | Whether the player signed in the early signing period. |
| `default_asset_url` | String | URL of the player's headshot image. |
| `class_year` | Int64 | Player's original recruiting class year. |
| `athlete_verified` | Boolean | Whether the athlete has verified their own On3 profile. |
| `prospect_verified` | Boolean | Whether On3 has verified the prospect's profile information. |
| `position_abbreviation` | String | Position abbreviation on the record. |
| `height` | String | Height as a formatted string (e.g. '6-3.5'). |
| `weight` | Int64 | Weight in pounds. |
| `transfer_industry_comparison` | String | JSON-encoded list of the player's transfer-portal rankings across industry services. |
| `predictions` | String | RPM prediction entries for the transfer destination, as a stringified list. |
| `nil_status` | String | Status of the player's On3 NIL valuation (e.g. active, inactive). |
| `nil_value` | Int64 | Player's On3 NIL valuation in dollars. |
| `sport` | String | Nested On3 sport object for the transfer entry (stringified). |
| `eligibility` | String | Eligibility note On3 attaches to the transfer, when any. |
| `organization_history` | String | JSON-encoded list of the programs the player has been on, newest first. |
| `industry_comparison` | String | JSON-encoded list of the player's rankings across industry services. |
| `entered_article` | String | On3 article link covering the player entering the transfer portal (stringified). |
| `exited_article` | String | On3 article link covering the player exiting the portal (stringified). |
| `person_sport_key` | Int64 | On3 key of the athlete-sport profile (person x sport). |
| `withdrawn_transfer` | Boolean | Whether the player withdrew from the transfer portal. |
| `withdrawn_transfer_date` | String | Date the player withdrew from the transfer portal, when withdrawn. |
| `rec_status` | String | Recruiting status of the player (e.g. Committed, Enrolled). |
| `index` | Int64 | Zero-based position of the row in the On3 response list. |
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
| `rating_consensus_rating` | Float64 | Industry-consensus numeric rating paired with the On3 rating (0-100 scale). |
| `rating_consensus_stars` | Int64 | Industry-consensus star rating paired with the On3 rating (2-5). |
| `rating_consensus_national_rank` | Int64 | Industry-consensus national rank paired with the On3 rating. |
| `rating_consensus_position_rank` | Int64 | Industry-consensus position rank paired with the On3 rating. |
| `rating_consensus_state_rank` | Int64 | Industry-consensus state rank paired with the On3 rating. |
| `rating_key` | Int64 | On3 key of the On3 rating record. |
| `rating_rating` | Float64 | Numeric value of the On3 rating (0-100 scale). |
| `rating_stars` | Int64 | Star rating of the On3 rating (2-5). |
| `rating_national_rank` | Int64 | National rank of the On3 rating. |
| `rating_position_rank` | Int64 | Position rank of the On3 rating. |
| `rating_state_rank` | Int64 | State rank of the On3 rating. |
| `rating_position_abbr` | String | Position abbreviation the On3 rating was assigned at. |
| `rating_state_abbr` | String | State abbreviation the On3 rating was assigned in. |
| `rating_five_star_plus` | Boolean | Five-star-plus flag on the On3 rating. |
| `roster_rating_consensus_rating` | Float64 | Industry-consensus numeric rating paired with the On3 roster rating (0-100 scale). |
| `roster_rating_consensus_stars` | Int64 | Industry-consensus star rating paired with the On3 roster rating (2-5). |
| `roster_rating_consensus_national_rank` | Int64 | Industry-consensus national rank paired with the On3 roster rating. |
| `roster_rating_consensus_position_rank` | Int64 | Industry-consensus position rank paired with the On3 roster rating. |
| `roster_rating_consensus_state_rank` | String | Industry-consensus state rank paired with the On3 roster rating. |
| `roster_rating_key` | Int64 | On3 key of the On3 roster rating record. |
| `roster_rating_rating` | Int64 | Numeric value of the On3 roster rating (0-100 scale). |
| `roster_rating_stars` | Int64 | Star rating of the On3 roster rating (2-5). |
| `roster_rating_national_rank` | Int64 | National rank of the On3 roster rating. |
| `roster_rating_position_rank` | Int64 | Position rank of the On3 roster rating. |
| `roster_rating_state_rank` | Int64 | State rank of the On3 roster rating. |
| `roster_rating_position_abbr` | String | Position abbreviation the On3 roster rating was assigned at. |
| `roster_rating_state_abbr` | String | State abbreviation the On3 roster rating was assigned in. |
| `roster_rating_five_star_plus` | Boolean | Five-star-plus flag on the On3 roster rating. |
| `transfer_rating_consensus_rating` | Float64 | Industry-consensus numeric rating paired with the On3 transfer-portal rating (0-100 scale). |
| `transfer_rating_consensus_stars` | Int64 | Industry-consensus star rating paired with the On3 transfer-portal rating (2-5). |
| `transfer_rating_consensus_national_rank` | Int64 | Industry-consensus national rank paired with the On3 transfer-portal rating. |
| `transfer_rating_consensus_position_rank` | Int64 | Industry-consensus position rank paired with the On3 transfer-portal rating. |
| `transfer_rating_consensus_state_rank` | String | Industry-consensus state rank paired with the On3 transfer-portal rating. |
| `transfer_rating_key` | Int64 | On3 key of the On3 transfer-portal rating record. |
| `transfer_rating_rating` | Int64 | Numeric value of the On3 transfer-portal rating (0-100 scale). |
| `transfer_rating_stars` | Int64 | Star rating of the On3 transfer-portal rating (2-5). |
| `transfer_rating_national_rank` | Int64 | National rank of the On3 transfer-portal rating. |
| `transfer_rating_position_rank` | Int64 | Position rank of the On3 transfer-portal rating. |
| `transfer_rating_state_rank` | String | State rank of the On3 transfer-portal rating. |
| `transfer_rating_position_abbr` | String | Position abbreviation the On3 transfer-portal rating was assigned at. |
| `transfer_rating_state_abbr` | String | State abbreviation the On3 transfer-portal rating was assigned in. |
| `transfer_rating_five_star_plus` | Boolean | Five-star-plus flag on the On3 transfer-portal rating. |
| `commit_status_type` | String | Type of the commitment status (e.g. Committed, Signed, Enrolled, None). |
| `commit_status_short_term_signee` | Boolean | Short-term-signee flag of the commitment status (null when not applicable). |
| `commit_status_date` | String | Date the commitment status took effect (ISO timestamp string). |
| `commit_status_committed_asset` | String | Nested asset of the committed-to program (the commitment status; usually null). |
| `commit_status_committed_asset_res` | String | Nested logo asset of the committed-to program (the commitment status; usually null). |
| `commit_status_transferred_asset` | String | Nested asset of the program transferred to (the commitment status; usually null). |
| `commit_status_transferred_asset_res` | String | Nested logo asset of the program transferred to (the commitment status; usually null). |
| `commit_status_committed_organization_key` | Int64 | On3 key of the committed-to program (the commitment status). |
| `commit_status_committed_organization_full_name` | String | Full name of the commitment status's committed-to program (e.g. 'Alabama Crimson Tide'). |
| `commit_status_committed_organization_name` | String | Short name of the commitment status's committed-to program. |
| `commit_status_committed_organization_mascot` | String | Mascot of the commitment status's committed-to program. |
| `commit_status_committed_organization_abbreviation` | String | Abbreviation of the commitment status's committed-to program. |
| `commit_status_committed_organization_asset_url` | String | CDN URL of the committed-to program's logo (the commitment status). |
| `commit_status_committed_organization_asset_key` | Int64 | On3 asset key of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_domain_override` | String | CDN domain override for the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_domain` | String | CDN domain serving the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_source_override` | String | Source-path override for the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_source` | String | CDN-relative source path of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_title` | String | Editorial title attached to the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_description` | String | Editorial description attached to the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_caption` | String | Editorial caption attached to the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_category` | String | Editorial category label of the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_alt_text` | String | Accessibility alt text of the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_height` | Int64 | Pixel height of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_width` | Int64 | Pixel width of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_asset_type` | String | On3 asset-type discriminator of the commitment status's committed-to program's logo asset (e.g. Image). |
| `commit_status_committed_organization_asset_file_system` | String | Storage file-system flag of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_path` | String | Storage path of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_type` | String | Media type field of the commitment status's committed-to program's logo asset (file extension, e.g. png). |
| `commit_status_committed_organization_asset_thumbnail` | String | Thumbnail variant of the commitment status's committed-to program's logo asset (video assets; usually null). |
| `commit_status_committed_organization_asset_duration` | Int64 | Duration of the commitment status's committed-to program's logo asset when it is a video (usually null or 0). |
| `commit_status_committed_organization_asset_mime_type` | String | MIME type of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_slug` | String | URL slug of the committed-to program (the commitment status). |
| `commit_status_committed_organization_primary_color` | String | Primary hex color of the commitment status's committed-to program. |
| `commit_status_class_rank` | String | Academic class standing recorded on the commitment status (e.g. Senior). |
| `commit_status_transfer_entered` | String | Date the player entered the transfer portal (the commitment status; null when never entered). |
| `commit_status_recruitment_year` | Int64 | Recruiting-cycle year the commitment status belongs to. |
| `commit_status_decommitted_asset` | String | Nested asset of the program decommitted from (the commitment status; usually null). |
| `commit_status_transfer` | Boolean | Transfer flag of the commitment status (null when not applicable). |
| `commit_status_expected_to_transfer` | Boolean | Expected-to-transfer flag of the commitment status (null when not applicable). |
| `commit_status_recruitment_key` | Int64 | On3 key of the recruitment record the commitment status belongs to. |
| `commit_status_withdrawn_transfer` | Boolean | Whether the player withdrew from the transfer portal (the commitment status). |
| `commit_status_withdrawn_transfer_date` | String | Date the player withdrew from the transfer portal (the commitment status; null when never withdrawn). |
| `valuation_key` | Int64 | On3 key of the NIL valuation record. |
| `valuation_nil_status` | String | Status of the NIL valuation (e.g. Normal). |
| `valuation_total_value` | Int64 | The NIL valuation in US dollars. |
| `valuation_rank` | Float64 | Overall rank of the NIL valuation across On3's NIL 100. |
| `valuation_group_rank` | Float64 | Rank of the NIL valuation within its group (sport or position). |
| `valuation_whisper` | Float64 | On3 whisper valuation (reported deal value) behind the NIL valuation, in US dollars. |
| `last_team_asset_url_key` | Int64 | On3 asset key behind the previous program's logo link. |
| `last_team_asset_url_url` | String | Full CDN URL of the previous program's logo link. |
| `last_team_asset_url_slug` | String | URL slug of the program the previous program's logo link belongs to. |
| `last_team_asset_url_full_name` | String | Full name of the program the previous program's logo link belongs to. |
| `last_team_full_name` | String | Full name of the previous program (e.g. 'Alabama Crimson Tide'). |
| `last_team_key` | Int64 | On3 numeric key of the previous program. |
| `last_team_name` | String | Short name of the previous program. |
| `last_team_mascot` | String | Mascot of the previous program. |
| `last_team_abbreviation` | String | Abbreviation of the previous program. |
| `last_team_asset_key` | Int64 | On3 asset key of the previous program's logo asset. |
| `last_team_asset_domain_override` | String | CDN domain override for the previous program's logo asset (usually null). |
| `last_team_asset_domain` | String | CDN domain serving the previous program's logo asset. |
| `last_team_asset_source_override` | String | Source-path override for the previous program's logo asset (usually null). |
| `last_team_asset_source` | String | CDN-relative source path of the previous program's logo asset. |
| `last_team_asset_title` | String | Editorial title attached to the previous program's logo asset. |
| `last_team_asset_description` | String | Editorial description attached to the previous program's logo asset (usually null). |
| `last_team_asset_caption` | String | Editorial caption attached to the previous program's logo asset (usually null). |
| `last_team_asset_category` | String | Editorial category label of the previous program's logo asset (usually null). |
| `last_team_asset_alt_text` | String | Accessibility alt text of the previous program's logo asset (usually null). |
| `last_team_asset_height` | Int64 | Pixel height of the previous program's logo asset. |
| `last_team_asset_width` | Int64 | Pixel width of the previous program's logo asset. |
| `last_team_asset_asset_type` | String | On3 asset-type discriminator of the previous program's logo asset (e.g. Image). |
| `last_team_asset_file_system` | String | Storage file-system flag of the previous program's logo asset. |
| `last_team_asset_path` | String | Storage path of the previous program's logo asset. |
| `last_team_asset_type` | String | Media type field of the previous program's logo asset (file extension, e.g. png). |
| `last_team_asset_thumbnail` | String | Thumbnail variant of the previous program's logo asset (video assets; usually null). |
| `last_team_asset_duration` | Int64 | Duration of the previous program's logo asset when it is a video (usually null or 0). |
| `last_team_asset_mime_type` | String | MIME type of the previous program's logo asset. |
| `last_team_slug` | String | URL slug of the previous program on On3. |
| `last_team_primary_color` | String | Primary hex color of the previous program. |
| `committed_article_key` | Float64 | On3 article key of the commitment article. |
| `committed_article_slug` | String | URL slug of the commitment article. |
| `committed_article_full_url` | String | Site-relative URL of the commitment article. |
| `committed_article_title` | String | Headline of the commitment article. |
| `committed_article_is_premium` | String | Whether the commitment article is behind the On3+ paywall. |
| `committed_article_date_published_gmt` | String | Publication timestamp of the commitment article (GMT, ISO string). |
| `interest_status_type` | String | Type of the interest status (e.g. Committed, Signed, Enrolled, None). |
| `interest_status_short_term_signee` | Boolean | Short-term-signee flag of the interest status (null when not applicable). |
| `interest_status_date` | String | Date the interest status took effect (ISO timestamp string). |
| `interest_status_committed_asset` | String | Nested asset of the committed-to program (the interest status; usually null). |
| `interest_status_committed_asset_res` | String | Nested logo asset of the committed-to program (the interest status; usually null). |
| `interest_status_transferred_asset` | String | Nested asset of the program transferred to (the interest status; usually null). |
| `interest_status_transferred_asset_res` | String | Nested logo asset of the program transferred to (the interest status; usually null). |
| `interest_status_committed_organization_key` | Int64 | On3 key of the committed-to program (the interest status). |
| `interest_status_committed_organization_full_name` | String | Full name of the interest status's committed-to program (e.g. 'Alabama Crimson Tide'). |
| `interest_status_committed_organization_name` | String | Short name of the interest status's committed-to program. |
| `interest_status_committed_organization_mascot` | String | Mascot of the interest status's committed-to program. |
| `interest_status_committed_organization_abbreviation` | String | Abbreviation of the interest status's committed-to program. |
| `interest_status_committed_organization_asset_url` | String | CDN URL of the committed-to program's logo (the interest status). |
| `interest_status_committed_organization_asset_key` | Int64 | On3 asset key of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_domain_override` | String | CDN domain override for the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_domain` | String | CDN domain serving the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_source_override` | String | Source-path override for the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_source` | String | CDN-relative source path of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_title` | String | Editorial title attached to the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_description` | String | Editorial description attached to the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_caption` | String | Editorial caption attached to the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_category` | String | Editorial category label of the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_alt_text` | String | Accessibility alt text of the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_height` | Int64 | Pixel height of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_width` | Int64 | Pixel width of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_asset_type` | String | On3 asset-type discriminator of the interest status's committed-to program's logo asset (e.g. Image). |
| `interest_status_committed_organization_asset_file_system` | String | Storage file-system flag of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_path` | String | Storage path of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_type` | String | Media type field of the interest status's committed-to program's logo asset (file extension, e.g. png). |
| `interest_status_committed_organization_asset_thumbnail` | String | Thumbnail variant of the interest status's committed-to program's logo asset (video assets; usually null). |
| `interest_status_committed_organization_asset_duration` | Int64 | Duration of the interest status's committed-to program's logo asset when it is a video (usually null or 0). |
| `interest_status_committed_organization_asset_mime_type` | String | MIME type of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_slug` | String | URL slug of the committed-to program (the interest status). |
| `interest_status_committed_organization_primary_color` | String | Primary hex color of the interest status's committed-to program. |
| `interest_status_class_rank` | String | Academic class standing recorded on the interest status (e.g. Senior). |
| `interest_status_transfer_entered` | String | Date the player entered the transfer portal (the interest status; null when never entered). |
| `interest_status_recruitment_year` | String | Recruiting-cycle year the interest status belongs to. |
| `interest_status_decommitted_asset` | String | Nested asset of the program decommitted from (the interest status; usually null). |
| `interest_status_transfer` | Boolean | Transfer flag of the interest status (null when not applicable). |
| `interest_status_expected_to_transfer` | Boolean | Expected-to-transfer flag of the interest status (null when not applicable). |
| `interest_status_recruitment_key` | Int64 | On3 key of the recruitment record the interest status belongs to. |
| `interest_status_withdrawn_transfer` | Boolean | Whether the player withdrew from the transfer portal (the interest status). |
| `interest_status_withdrawn_transfer_date` | String | Date the player withdrew from the transfer portal (the interest status; null when never withdrawn). |
| `committed_article` | String | On3 article link covering the player's transfer commitment (stringified). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_transfers_best_available-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_transfers_best_available-example}

```python
on3_transfers_best_available()
```

## on3_transfers_latest

Transfers latest.

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
| `key` | Int64 | On3 RDB key for the transfer entry. |
| `recruitment_key` | Int64 | On3 recruitment key of the player's transfer recruitment. |
| `name` | String | Display name of the row's record. |
| `slug` | String | URL slug of the row's record on On3. |
| `high_school_name` | String | High-school display name (top-level field). |
| `home_town_name` | String | Player's hometown, as listed by On3. |
| `early_enrollee` | Boolean | Whether the player is an early enrollee. |
| `early_signee` | Boolean | Whether the player signed in the early signing period. |
| `default_asset_url` | String | URL of the player's headshot image. |
| `class_year` | Int64 | Player's original recruiting class year. |
| `athlete_verified` | Boolean | Whether the athlete has verified their own On3 profile. |
| `prospect_verified` | Boolean | Whether On3 has verified the prospect's profile information. |
| `position_abbreviation` | String | Position abbreviation on the record. |
| `height` | String | Height as a formatted string (e.g. '6-3.5'). |
| `weight` | Int64 | Weight in pounds. |
| `transfer_industry_comparison` | String | JSON-encoded list of the player's transfer-portal rankings across industry services. |
| `predictions` | String | RPM prediction entries for the transfer destination, as a stringified list. |
| `nil_status` | String | Status of the player's On3 NIL valuation (e.g. active, inactive). |
| `nil_value` | Float64 | Player's On3 NIL valuation in dollars. |
| `valuation` | String | Nested NIL valuation object (stringified; usually null). |
| `sport` | String | Nested On3 sport object for the transfer entry (stringified). |
| `eligibility` | String | Eligibility note On3 attaches to the transfer, when any. |
| `organization_history` | String | JSON-encoded list of the programs the player has been on, newest first. |
| `industry_comparison` | String | JSON-encoded list of the player's rankings across industry services. |
| `entered_article` | String | On3 article link covering the player entering the transfer portal (stringified). |
| `exited_article` | String | On3 article link covering the player exiting the portal (stringified). |
| `person_sport_key` | Int64 | On3 key of the athlete-sport profile (person x sport). |
| `withdrawn_transfer` | Boolean | Whether the player withdrew from the transfer portal. |
| `withdrawn_transfer_date` | String | Date the player withdrew from the transfer portal, when withdrawn. |
| `rec_status` | String | Recruiting status of the player (e.g. Committed, Enrolled). |
| `index` | Int64 | Zero-based position of the row in the On3 response list. |
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
| `rating_consensus_rating` | String | Industry-consensus numeric rating paired with the On3 rating (0-100 scale). |
| `rating_consensus_stars` | Float64 | Industry-consensus star rating paired with the On3 rating (2-5). |
| `rating_consensus_national_rank` | String | Industry-consensus national rank paired with the On3 rating. |
| `rating_consensus_position_rank` | String | Industry-consensus position rank paired with the On3 rating. |
| `rating_consensus_state_rank` | String | Industry-consensus state rank paired with the On3 rating. |
| `rating_key` | Float64 | On3 key of the On3 rating record. |
| `rating_rating` | Float64 | Numeric value of the On3 rating (0-100 scale). |
| `rating_stars` | Float64 | Star rating of the On3 rating (2-5). |
| `rating_national_rank` | Int64 | National rank of the On3 rating. |
| `rating_position_rank` | Int64 | Position rank of the On3 rating. |
| `rating_state_rank` | Int64 | State rank of the On3 rating. |
| `rating_position_abbr` | String | Position abbreviation the On3 rating was assigned at. |
| `rating_state_abbr` | String | State abbreviation the On3 rating was assigned in. |
| `rating_five_star_plus` | String | Five-star-plus flag on the On3 rating. |
| `roster_rating_consensus_rating` | Int64 | Industry-consensus numeric rating paired with the On3 roster rating (0-100 scale). |
| `roster_rating_consensus_stars` | Int64 | Industry-consensus star rating paired with the On3 roster rating (2-5). |
| `roster_rating_consensus_national_rank` | Int64 | Industry-consensus national rank paired with the On3 roster rating. |
| `roster_rating_consensus_position_rank` | Int64 | Industry-consensus position rank paired with the On3 roster rating. |
| `roster_rating_consensus_state_rank` | String | Industry-consensus state rank paired with the On3 roster rating. |
| `roster_rating_key` | Int64 | On3 key of the On3 roster rating record. |
| `roster_rating_rating` | Int64 | Numeric value of the On3 roster rating (0-100 scale). |
| `roster_rating_stars` | Int64 | Star rating of the On3 roster rating (2-5). |
| `roster_rating_national_rank` | Int64 | National rank of the On3 roster rating. |
| `roster_rating_position_rank` | Int64 | Position rank of the On3 roster rating. |
| `roster_rating_state_rank` | Int64 | State rank of the On3 roster rating. |
| `roster_rating_position_abbr` | String | Position abbreviation the On3 roster rating was assigned at. |
| `roster_rating_state_abbr` | String | State abbreviation the On3 roster rating was assigned in. |
| `roster_rating_five_star_plus` | Boolean | Five-star-plus flag on the On3 roster rating. |
| `transfer_rating_consensus_rating` | Int64 | Industry-consensus numeric rating paired with the On3 transfer-portal rating (0-100 scale). |
| `transfer_rating_consensus_stars` | Int64 | Industry-consensus star rating paired with the On3 transfer-portal rating (2-5). |
| `transfer_rating_consensus_national_rank` | Int64 | Industry-consensus national rank paired with the On3 transfer-portal rating. |
| `transfer_rating_consensus_position_rank` | Int64 | Industry-consensus position rank paired with the On3 transfer-portal rating. |
| `transfer_rating_consensus_state_rank` | String | Industry-consensus state rank paired with the On3 transfer-portal rating. |
| `transfer_rating_key` | Int64 | On3 key of the On3 transfer-portal rating record. |
| `transfer_rating_rating` | Int64 | Numeric value of the On3 transfer-portal rating (0-100 scale). |
| `transfer_rating_stars` | Int64 | Star rating of the On3 transfer-portal rating (2-5). |
| `transfer_rating_national_rank` | String | National rank of the On3 transfer-portal rating. |
| `transfer_rating_position_rank` | Int64 | Position rank of the On3 transfer-portal rating. |
| `transfer_rating_state_rank` | String | State rank of the On3 transfer-portal rating. |
| `transfer_rating_position_abbr` | String | Position abbreviation the On3 transfer-portal rating was assigned at. |
| `transfer_rating_state_abbr` | String | State abbreviation the On3 transfer-portal rating was assigned in. |
| `transfer_rating_five_star_plus` | Boolean | Five-star-plus flag on the On3 transfer-portal rating. |
| `commit_status_type` | String | Type of the commitment status (e.g. Committed, Signed, Enrolled, None). |
| `commit_status_short_term_signee` | Boolean | Short-term-signee flag of the commitment status (null when not applicable). |
| `commit_status_date` | String | Date the commitment status took effect (ISO timestamp string). |
| `commit_status_committed_asset` | String | Nested asset of the committed-to program (the commitment status; usually null). |
| `commit_status_committed_asset_res` | String | Nested logo asset of the committed-to program (the commitment status; usually null). |
| `commit_status_transferred_asset` | String | Nested asset of the program transferred to (the commitment status; usually null). |
| `commit_status_transferred_asset_res` | String | Nested logo asset of the program transferred to (the commitment status; usually null). |
| `commit_status_committed_organization_key` | Int64 | On3 key of the committed-to program (the commitment status). |
| `commit_status_committed_organization_full_name` | String | Full name of the commitment status's committed-to program (e.g. 'Alabama Crimson Tide'). |
| `commit_status_committed_organization_name` | String | Short name of the commitment status's committed-to program. |
| `commit_status_committed_organization_mascot` | String | Mascot of the commitment status's committed-to program. |
| `commit_status_committed_organization_abbreviation` | String | Abbreviation of the commitment status's committed-to program. |
| `commit_status_committed_organization_asset_url` | String | CDN URL of the committed-to program's logo (the commitment status). |
| `commit_status_committed_organization_asset_key` | Int64 | On3 asset key of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_domain_override` | String | CDN domain override for the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_domain` | String | CDN domain serving the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_source_override` | String | Source-path override for the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_source` | String | CDN-relative source path of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_title` | String | Editorial title attached to the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_description` | String | Editorial description attached to the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_caption` | String | Editorial caption attached to the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_category` | String | Editorial category label of the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_alt_text` | String | Accessibility alt text of the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_height` | Int64 | Pixel height of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_width` | Int64 | Pixel width of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_asset_type` | String | On3 asset-type discriminator of the commitment status's committed-to program's logo asset (e.g. Image). |
| `commit_status_committed_organization_asset_file_system` | String | Storage file-system flag of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_path` | String | Storage path of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_type` | String | Media type field of the commitment status's committed-to program's logo asset (file extension, e.g. png). |
| `commit_status_committed_organization_asset_thumbnail` | String | Thumbnail variant of the commitment status's committed-to program's logo asset (video assets; usually null). |
| `commit_status_committed_organization_asset_duration` | Int64 | Duration of the commitment status's committed-to program's logo asset when it is a video (usually null or 0). |
| `commit_status_committed_organization_asset_mime_type` | String | MIME type of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_slug` | String | URL slug of the committed-to program (the commitment status). |
| `commit_status_committed_organization_primary_color` | String | Primary hex color of the commitment status's committed-to program. |
| `commit_status_class_rank` | String | Academic class standing recorded on the commitment status (e.g. Senior). |
| `commit_status_transfer_entered` | String | Date the player entered the transfer portal (the commitment status; null when never entered). |
| `commit_status_recruitment_year` | Int64 | Recruiting-cycle year the commitment status belongs to. |
| `commit_status_decommitted_asset` | String | Nested asset of the program decommitted from (the commitment status; usually null). |
| `commit_status_transfer` | Boolean | Transfer flag of the commitment status (null when not applicable). |
| `commit_status_expected_to_transfer` | Boolean | Expected-to-transfer flag of the commitment status (null when not applicable). |
| `commit_status_recruitment_key` | Int64 | On3 key of the recruitment record the commitment status belongs to. |
| `commit_status_withdrawn_transfer` | Boolean | Whether the player withdrew from the transfer portal (the commitment status). |
| `commit_status_withdrawn_transfer_date` | String | Date the player withdrew from the transfer portal (the commitment status; null when never withdrawn). |
| `last_team_asset_url_key` | Int64 | On3 asset key behind the previous program's logo link. |
| `last_team_asset_url_url` | String | Full CDN URL of the previous program's logo link. |
| `last_team_asset_url_slug` | String | URL slug of the program the previous program's logo link belongs to. |
| `last_team_asset_url_full_name` | String | Full name of the program the previous program's logo link belongs to. |
| `last_team_full_name` | String | Full name of the previous program (e.g. 'Alabama Crimson Tide'). |
| `last_team_key` | Int64 | On3 numeric key of the previous program. |
| `last_team_name` | String | Short name of the previous program. |
| `last_team_mascot` | String | Mascot of the previous program. |
| `last_team_abbreviation` | String | Abbreviation of the previous program. |
| `last_team_asset_key` | Int64 | On3 asset key of the previous program's logo asset. |
| `last_team_asset_domain_override` | String | CDN domain override for the previous program's logo asset (usually null). |
| `last_team_asset_domain` | String | CDN domain serving the previous program's logo asset. |
| `last_team_asset_source_override` | String | Source-path override for the previous program's logo asset (usually null). |
| `last_team_asset_source` | String | CDN-relative source path of the previous program's logo asset. |
| `last_team_asset_title` | String | Editorial title attached to the previous program's logo asset. |
| `last_team_asset_description` | String | Editorial description attached to the previous program's logo asset (usually null). |
| `last_team_asset_caption` | String | Editorial caption attached to the previous program's logo asset (usually null). |
| `last_team_asset_category` | String | Editorial category label of the previous program's logo asset (usually null). |
| `last_team_asset_alt_text` | String | Accessibility alt text of the previous program's logo asset (usually null). |
| `last_team_asset_height` | Int64 | Pixel height of the previous program's logo asset. |
| `last_team_asset_width` | Int64 | Pixel width of the previous program's logo asset. |
| `last_team_asset_asset_type` | String | On3 asset-type discriminator of the previous program's logo asset (e.g. Image). |
| `last_team_asset_file_system` | String | Storage file-system flag of the previous program's logo asset. |
| `last_team_asset_path` | String | Storage path of the previous program's logo asset. |
| `last_team_asset_type` | String | Media type field of the previous program's logo asset (file extension, e.g. png). |
| `last_team_asset_thumbnail` | String | Thumbnail variant of the previous program's logo asset (video assets; usually null). |
| `last_team_asset_duration` | Int64 | Duration of the previous program's logo asset when it is a video (usually null or 0). |
| `last_team_asset_mime_type` | String | MIME type of the previous program's logo asset. |
| `last_team_slug` | String | URL slug of the previous program on On3. |
| `last_team_primary_color` | String | Primary hex color of the previous program. |
| `committed_article_key` | Float64 | On3 article key of the commitment article. |
| `committed_article_slug` | String | URL slug of the commitment article. |
| `committed_article_full_url` | String | Site-relative URL of the commitment article. |
| `committed_article_title` | String | Headline of the commitment article. |
| `committed_article_is_premium` | String | Whether the commitment article is behind the On3+ paywall. |
| `committed_article_date_published_gmt` | String | Publication timestamp of the commitment article (GMT, ISO string). |
| `interest_status_type` | String | Type of the interest status (e.g. Committed, Signed, Enrolled, None). |
| `interest_status_short_term_signee` | Boolean | Short-term-signee flag of the interest status (null when not applicable). |
| `interest_status_date` | String | Date the interest status took effect (ISO timestamp string). |
| `interest_status_committed_asset` | String | Nested asset of the committed-to program (the interest status; usually null). |
| `interest_status_committed_asset_res` | String | Nested logo asset of the committed-to program (the interest status; usually null). |
| `interest_status_transferred_asset` | String | Nested asset of the program transferred to (the interest status; usually null). |
| `interest_status_transferred_asset_res` | String | Nested logo asset of the program transferred to (the interest status; usually null). |
| `interest_status_committed_organization_key` | Int64 | On3 key of the committed-to program (the interest status). |
| `interest_status_committed_organization_full_name` | String | Full name of the interest status's committed-to program (e.g. 'Alabama Crimson Tide'). |
| `interest_status_committed_organization_name` | String | Short name of the interest status's committed-to program. |
| `interest_status_committed_organization_mascot` | String | Mascot of the interest status's committed-to program. |
| `interest_status_committed_organization_abbreviation` | String | Abbreviation of the interest status's committed-to program. |
| `interest_status_committed_organization_asset_url` | String | CDN URL of the committed-to program's logo (the interest status). |
| `interest_status_committed_organization_asset_key` | Int64 | On3 asset key of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_domain_override` | String | CDN domain override for the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_domain` | String | CDN domain serving the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_source_override` | String | Source-path override for the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_source` | String | CDN-relative source path of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_title` | String | Editorial title attached to the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_description` | String | Editorial description attached to the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_caption` | String | Editorial caption attached to the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_category` | String | Editorial category label of the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_alt_text` | String | Accessibility alt text of the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_height` | Int64 | Pixel height of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_width` | Int64 | Pixel width of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_asset_type` | String | On3 asset-type discriminator of the interest status's committed-to program's logo asset (e.g. Image). |
| `interest_status_committed_organization_asset_file_system` | String | Storage file-system flag of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_path` | String | Storage path of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_type` | String | Media type field of the interest status's committed-to program's logo asset (file extension, e.g. png). |
| `interest_status_committed_organization_asset_thumbnail` | String | Thumbnail variant of the interest status's committed-to program's logo asset (video assets; usually null). |
| `interest_status_committed_organization_asset_duration` | Int64 | Duration of the interest status's committed-to program's logo asset when it is a video (usually null or 0). |
| `interest_status_committed_organization_asset_mime_type` | String | MIME type of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_slug` | String | URL slug of the committed-to program (the interest status). |
| `interest_status_committed_organization_primary_color` | String | Primary hex color of the interest status's committed-to program. |
| `interest_status_class_rank` | String | Academic class standing recorded on the interest status (e.g. Senior). |
| `interest_status_transfer_entered` | String | Date the player entered the transfer portal (the interest status; null when never entered). |
| `interest_status_recruitment_year` | String | Recruiting-cycle year the interest status belongs to. |
| `interest_status_decommitted_asset` | String | Nested asset of the program decommitted from (the interest status; usually null). |
| `interest_status_transfer` | Boolean | Transfer flag of the interest status (null when not applicable). |
| `interest_status_expected_to_transfer` | Boolean | Expected-to-transfer flag of the interest status (null when not applicable). |
| `interest_status_recruitment_key` | Int64 | On3 key of the recruitment record the interest status belongs to. |
| `interest_status_withdrawn_transfer` | Boolean | Whether the player withdrew from the transfer portal (the interest status). |
| `interest_status_withdrawn_transfer_date` | String | Date the player withdrew from the transfer portal (the interest status; null when never withdrawn). |
| `rating` | String | On3 rating of the player (0-100 scale), when published. |
| `committed_article` | String | On3 article link covering the player's transfer commitment (stringified). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_transfers_latest-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_transfers_latest-example}

```python
on3_transfers_latest()
```
