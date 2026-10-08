---
title: "CFB — On3 Recruit Database (api.on3.com) — Transfers"
sidebar_label: "Transfers"
sidebar_position: 9
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
| `name` | character | Display name of the row's record. |
| `slug` | character | URL slug of the row's record on On3. |
| `high_school_name` | character | High-school display name (top-level field). |
| `home_town_name` | character | Player's hometown, as listed by On3. |
| `early_enrollee` | logical | Whether the player is an early enrollee. |
| `early_signee` | logical | Whether the player signed in the early signing period. |
| `default_asset_url` | character | URL of the player's headshot image. |
| `class_year` | integer | Player's original recruiting class year. |
| `athlete_verified` | logical | Whether the athlete has verified their own On3 profile. |
| `prospect_verified` | logical | Whether On3 has verified the prospect's profile information. |
| `position_abbreviation` | character | Position abbreviation on the record. |
| `height` | character | Height as a formatted string (e.g. '6-3.5'). |
| `weight` | integer | Weight in pounds. |
| `transfer_industry_comparison` | character | JSON-encoded list of the player's transfer-portal rankings across industry services. |
| `predictions` | character | RPM prediction entries for the transfer destination, as a stringified list. |
| `nil_status` | character | Status of the player's On3 NIL valuation (e.g. active, inactive). |
| `nil_value` | integer | Player's On3 NIL valuation in dollars. |
| `sport` | character | Nested On3 sport object for the transfer entry (stringified). |
| `eligibility` | character | Eligibility note On3 attaches to the transfer, when any. |
| `organization_history` | character | JSON-encoded list of the programs the player has been on, newest first. |
| `industry_comparison` | character | JSON-encoded list of the player's rankings across industry services. |
| `entered_article` | character | On3 article link covering the player entering the transfer portal (stringified). |
| `exited_article` | character | On3 article link covering the player exiting the portal (stringified). |
| `person_sport_key` | integer | On3 key of the athlete-sport profile (person x sport). |
| `withdrawn_transfer` | logical | Whether the player withdrew from the transfer portal. |
| `withdrawn_transfer_date` | character | Date the player withdrew from the transfer portal, when withdrawn. |
| `rec_status` | character | Recruiting status of the player (e.g. Committed, Enrolled). |
| `index` | integer | Zero-based position of the row in the On3 response list. |
| `high_school_key` | integer | On3 numeric key of the high school. |
| `high_school_full_name` | character | Full name of the high school (with mascot). |
| `high_school_name_2` | character | Name field of the nested high-school object (json_normalize de-duplication of high_school_name). |
| `high_school_known_as` | character | Common short name of the high school, when On3 lists one. |
| `high_school_mascot` | character | High-school mascot. |
| `high_school_abbreviation` | character | High-school abbreviation. |
| `high_school_asset_url` | character | Convenience CDN URL of the high-school logo. |
| `high_school_default_asset_key` | integer | On3 asset key of the high school's logo asset. |
| `high_school_default_asset_domain_override` | character | CDN domain override for the high school's logo asset (usually null). |
| `high_school_default_asset_domain` | character | CDN domain serving the high school's logo asset. |
| `high_school_default_asset_source_override` | character | Source-path override for the high school's logo asset (usually null). |
| `high_school_default_asset_source` | character | CDN-relative source path of the high school's logo asset. |
| `high_school_default_asset_title` | character | Editorial title attached to the high school's logo asset. |
| `high_school_default_asset_description` | character | Editorial description attached to the high school's logo asset (usually null). |
| `high_school_default_asset_caption` | character | Editorial caption attached to the high school's logo asset (usually null). |
| `high_school_default_asset_category` | character | Editorial category label of the high school's logo asset (usually null). |
| `high_school_default_asset_alt_text` | character | Accessibility alt text of the high school's logo asset (usually null). |
| `high_school_default_asset_height` | integer | Pixel height of the high school's logo asset. |
| `high_school_default_asset_width` | integer | Pixel width of the high school's logo asset. |
| `high_school_default_asset_asset_type` | character | On3 asset-type discriminator of the high school's logo asset (e.g. Image). |
| `high_school_default_asset_file_system` | character | Storage file-system flag of the high school's logo asset. |
| `high_school_default_asset_path` | character | Storage path of the high school's logo asset. |
| `high_school_default_asset_type` | character | Media type field of the high school's logo asset (file extension, e.g. png). |
| `high_school_default_asset_thumbnail` | character | Thumbnail variant of the high school's logo asset (video assets; usually null). |
| `high_school_default_asset_duration` | integer | Duration of the high school's logo asset when it is a video (usually null or 0). |
| `high_school_default_asset_mime_type` | character | MIME type of the high school's logo asset. |
| `high_school_slug` | character | URL slug of the high school on On3. |
| `high_school_primary_color` | character | Primary hex color of the high school. |
| `high_school_org_type` | character | Organization type label of the school (e.g. HighSchool). |
| `high_school_org_type_enum` | character | Organization type enum of the school. |
| `high_school_division` | character | Division or classification of the high school, when listed. |
| `high_school_site_keys` | character | JSON-encoded On3 site keys covering the school (usually null). |
| `high_school_url_slug` | character | URL slug variant of the high-school page, with the key appended. |
| `default_asset_key` | integer | On3 asset key of the default asset. |
| `default_asset_domain_override` | character | CDN domain override for the record's default asset (usually null). |
| `default_asset_domain` | character | CDN domain serving the default asset. |
| `default_asset_source_override` | character | Source-path override for the default asset (usually null). |
| `default_asset_source` | character | CDN-relative source path of the default asset. |
| `default_asset_title` | character | Editorial title attached to the default asset. |
| `default_asset_description` | character | Editorial description attached to the default asset (usually null). |
| `default_asset_caption` | character | Editorial caption attached to the default asset (usually null). |
| `default_asset_category` | character | Editorial category label of the default asset (usually null). |
| `default_asset_alt_text` | character | Accessibility alt text of the default asset (usually null). |
| `default_asset_height` | integer | Pixel height of the default asset. |
| `default_asset_width` | integer | Pixel width of the default asset. |
| `default_asset_asset_type` | character | On3 asset-type discriminator of the default asset (e.g. Image). |
| `default_asset_file_system` | character | Storage file-system flag of the default asset. |
| `default_asset_path` | character | Storage path of the default asset. |
| `default_asset_type` | character | Media type field of the default asset (file extension, e.g. png). |
| `default_asset_thumbnail` | character | Thumbnail variant of the default asset (video assets; usually null). |
| `default_asset_duration` | integer | Duration of the default asset when it is a video (usually null or 0). |
| `default_asset_mime_type` | character | MIME type of the default asset. |
| `rating_consensus_rating` | numeric | Industry-consensus numeric rating paired with the On3 rating (0-100 scale). |
| `rating_consensus_stars` | integer | Industry-consensus star rating paired with the On3 rating (2-5). |
| `rating_consensus_national_rank` | integer | Industry-consensus national rank paired with the On3 rating. |
| `rating_consensus_position_rank` | integer | Industry-consensus position rank paired with the On3 rating. |
| `rating_consensus_state_rank` | integer | Industry-consensus state rank paired with the On3 rating. |
| `rating_key` | integer | On3 key of the On3 rating record. |
| `rating_rating` | numeric | Numeric value of the On3 rating (0-100 scale). |
| `rating_stars` | integer | Star rating of the On3 rating (2-5). |
| `rating_national_rank` | integer | National rank of the On3 rating. |
| `rating_position_rank` | integer | Position rank of the On3 rating. |
| `rating_state_rank` | integer | State rank of the On3 rating. |
| `rating_position_abbr` | character | Position abbreviation the On3 rating was assigned at. |
| `rating_state_abbr` | character | State abbreviation the On3 rating was assigned in. |
| `rating_five_star_plus` | logical | Five-star-plus flag on the On3 rating. |
| `roster_rating_consensus_rating` | numeric | Industry-consensus numeric rating paired with the On3 roster rating (0-100 scale). |
| `roster_rating_consensus_stars` | integer | Industry-consensus star rating paired with the On3 roster rating (2-5). |
| `roster_rating_consensus_national_rank` | integer | Industry-consensus national rank paired with the On3 roster rating. |
| `roster_rating_consensus_position_rank` | integer | Industry-consensus position rank paired with the On3 roster rating. |
| `roster_rating_consensus_state_rank` | character | Industry-consensus state rank paired with the On3 roster rating. |
| `roster_rating_key` | integer | On3 key of the On3 roster rating record. |
| `roster_rating_rating` | integer | Numeric value of the On3 roster rating (0-100 scale). |
| `roster_rating_stars` | integer | Star rating of the On3 roster rating (2-5). |
| `roster_rating_national_rank` | integer | National rank of the On3 roster rating. |
| `roster_rating_position_rank` | integer | Position rank of the On3 roster rating. |
| `roster_rating_state_rank` | integer | State rank of the On3 roster rating. |
| `roster_rating_position_abbr` | character | Position abbreviation the On3 roster rating was assigned at. |
| `roster_rating_state_abbr` | character | State abbreviation the On3 roster rating was assigned in. |
| `roster_rating_five_star_plus` | logical | Five-star-plus flag on the On3 roster rating. |
| `transfer_rating_consensus_rating` | numeric | Industry-consensus numeric rating paired with the On3 transfer-portal rating (0-100 scale). |
| `transfer_rating_consensus_stars` | integer | Industry-consensus star rating paired with the On3 transfer-portal rating (2-5). |
| `transfer_rating_consensus_national_rank` | integer | Industry-consensus national rank paired with the On3 transfer-portal rating. |
| `transfer_rating_consensus_position_rank` | integer | Industry-consensus position rank paired with the On3 transfer-portal rating. |
| `transfer_rating_consensus_state_rank` | character | Industry-consensus state rank paired with the On3 transfer-portal rating. |
| `transfer_rating_key` | integer | On3 key of the On3 transfer-portal rating record. |
| `transfer_rating_rating` | integer | Numeric value of the On3 transfer-portal rating (0-100 scale). |
| `transfer_rating_stars` | integer | Star rating of the On3 transfer-portal rating (2-5). |
| `transfer_rating_national_rank` | integer | National rank of the On3 transfer-portal rating. |
| `transfer_rating_position_rank` | integer | Position rank of the On3 transfer-portal rating. |
| `transfer_rating_state_rank` | character | State rank of the On3 transfer-portal rating. |
| `transfer_rating_position_abbr` | character | Position abbreviation the On3 transfer-portal rating was assigned at. |
| `transfer_rating_state_abbr` | character | State abbreviation the On3 transfer-portal rating was assigned in. |
| `transfer_rating_five_star_plus` | logical | Five-star-plus flag on the On3 transfer-portal rating. |
| `commit_status_type` | character | Type of the commitment status (e.g. Committed, Signed, Enrolled, None). |
| `commit_status_short_term_signee` | logical | Short-term-signee flag of the commitment status (null when not applicable). |
| `commit_status_date` | character | Date the commitment status took effect (ISO timestamp string). |
| `commit_status_committed_asset` | character | Nested asset of the committed-to program (the commitment status; usually null). |
| `commit_status_committed_asset_res` | character | Nested logo asset of the committed-to program (the commitment status; usually null). |
| `commit_status_transferred_asset` | character | Nested asset of the program transferred to (the commitment status; usually null). |
| `commit_status_transferred_asset_res` | character | Nested logo asset of the program transferred to (the commitment status; usually null). |
| `commit_status_committed_organization_key` | integer | On3 key of the committed-to program (the commitment status). |
| `commit_status_committed_organization_full_name` | character | Full name of the commitment status's committed-to program (e.g. 'Alabama Crimson Tide'). |
| `commit_status_committed_organization_name` | character | Short name of the commitment status's committed-to program. |
| `commit_status_committed_organization_mascot` | character | Mascot of the commitment status's committed-to program. |
| `commit_status_committed_organization_abbreviation` | character | Abbreviation of the commitment status's committed-to program. |
| `commit_status_committed_organization_asset_url` | character | CDN URL of the committed-to program's logo (the commitment status). |
| `commit_status_committed_organization_asset_key` | integer | On3 asset key of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_domain_override` | character | CDN domain override for the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_domain` | character | CDN domain serving the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_source_override` | character | Source-path override for the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_source` | character | CDN-relative source path of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_title` | character | Editorial title attached to the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_description` | character | Editorial description attached to the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_caption` | character | Editorial caption attached to the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_category` | character | Editorial category label of the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_alt_text` | character | Accessibility alt text of the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_height` | integer | Pixel height of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_width` | integer | Pixel width of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_asset_type` | character | On3 asset-type discriminator of the commitment status's committed-to program's logo asset (e.g. Image). |
| `commit_status_committed_organization_asset_file_system` | character | Storage file-system flag of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_path` | character | Storage path of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_type` | character | Media type field of the commitment status's committed-to program's logo asset (file extension, e.g. png). |
| `commit_status_committed_organization_asset_thumbnail` | character | Thumbnail variant of the commitment status's committed-to program's logo asset (video assets; usually null). |
| `commit_status_committed_organization_asset_duration` | integer | Duration of the commitment status's committed-to program's logo asset when it is a video (usually null or 0). |
| `commit_status_committed_organization_asset_mime_type` | character | MIME type of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_slug` | character | URL slug of the committed-to program (the commitment status). |
| `commit_status_committed_organization_primary_color` | character | Primary hex color of the commitment status's committed-to program. |
| `commit_status_class_rank` | character | Academic class standing recorded on the commitment status (e.g. Senior). |
| `commit_status_transfer_entered` | character | Date the player entered the transfer portal (the commitment status; null when never entered). |
| `commit_status_recruitment_year` | integer | Recruiting-cycle year the commitment status belongs to. |
| `commit_status_decommitted_asset` | character | Nested asset of the program decommitted from (the commitment status; usually null). |
| `commit_status_transfer` | logical | Transfer flag of the commitment status (null when not applicable). |
| `commit_status_expected_to_transfer` | logical | Expected-to-transfer flag of the commitment status (null when not applicable). |
| `commit_status_recruitment_key` | integer | On3 key of the recruitment record the commitment status belongs to. |
| `commit_status_withdrawn_transfer` | logical | Whether the player withdrew from the transfer portal (the commitment status). |
| `commit_status_withdrawn_transfer_date` | character | Date the player withdrew from the transfer portal (the commitment status; null when never withdrawn). |
| `valuation_key` | integer | On3 key of the NIL valuation record. |
| `valuation_nil_status` | character | Status of the NIL valuation (e.g. Normal). |
| `valuation_total_value` | integer | The NIL valuation in US dollars. |
| `valuation_rank` | numeric | Overall rank of the NIL valuation across On3's NIL 100. |
| `valuation_group_rank` | numeric | Rank of the NIL valuation within its group (sport or position). |
| `valuation_whisper` | numeric | On3 whisper valuation (reported deal value) behind the NIL valuation, in US dollars. |
| `last_team_asset_url_key` | integer | On3 asset key behind the previous program's logo link. |
| `last_team_asset_url_url` | character | Full CDN URL of the previous program's logo link. |
| `last_team_asset_url_slug` | character | URL slug of the program the previous program's logo link belongs to. |
| `last_team_asset_url_full_name` | character | Full name of the program the previous program's logo link belongs to. |
| `last_team_full_name` | character | Full name of the previous program (e.g. 'Alabama Crimson Tide'). |
| `last_team_key` | integer | On3 numeric key of the previous program. |
| `last_team_name` | character | Short name of the previous program. |
| `last_team_mascot` | character | Mascot of the previous program. |
| `last_team_abbreviation` | character | Abbreviation of the previous program. |
| `last_team_asset_key` | integer | On3 asset key of the previous program's logo asset. |
| `last_team_asset_domain_override` | character | CDN domain override for the previous program's logo asset (usually null). |
| `last_team_asset_domain` | character | CDN domain serving the previous program's logo asset. |
| `last_team_asset_source_override` | character | Source-path override for the previous program's logo asset (usually null). |
| `last_team_asset_source` | character | CDN-relative source path of the previous program's logo asset. |
| `last_team_asset_title` | character | Editorial title attached to the previous program's logo asset. |
| `last_team_asset_description` | character | Editorial description attached to the previous program's logo asset (usually null). |
| `last_team_asset_caption` | character | Editorial caption attached to the previous program's logo asset (usually null). |
| `last_team_asset_category` | character | Editorial category label of the previous program's logo asset (usually null). |
| `last_team_asset_alt_text` | character | Accessibility alt text of the previous program's logo asset (usually null). |
| `last_team_asset_height` | integer | Pixel height of the previous program's logo asset. |
| `last_team_asset_width` | integer | Pixel width of the previous program's logo asset. |
| `last_team_asset_asset_type` | character | On3 asset-type discriminator of the previous program's logo asset (e.g. Image). |
| `last_team_asset_file_system` | character | Storage file-system flag of the previous program's logo asset. |
| `last_team_asset_path` | character | Storage path of the previous program's logo asset. |
| `last_team_asset_type` | character | Media type field of the previous program's logo asset (file extension, e.g. png). |
| `last_team_asset_thumbnail` | character | Thumbnail variant of the previous program's logo asset (video assets; usually null). |
| `last_team_asset_duration` | integer | Duration of the previous program's logo asset when it is a video (usually null or 0). |
| `last_team_asset_mime_type` | character | MIME type of the previous program's logo asset. |
| `last_team_slug` | character | URL slug of the previous program on On3. |
| `last_team_primary_color` | character | Primary hex color of the previous program. |
| `committed_article_key` | numeric | On3 article key of the commitment article. |
| `committed_article_slug` | character | URL slug of the commitment article. |
| `committed_article_full_url` | character | Site-relative URL of the commitment article. |
| `committed_article_title` | character | Headline of the commitment article. |
| `committed_article_is_premium` | character | Whether the commitment article is behind the On3+ paywall. |
| `committed_article_date_published_gmt` | character | Publication timestamp of the commitment article (GMT, ISO string). |
| `interest_status_type` | character | Type of the interest status (e.g. Committed, Signed, Enrolled, None). |
| `interest_status_short_term_signee` | logical | Short-term-signee flag of the interest status (null when not applicable). |
| `interest_status_date` | character | Date the interest status took effect (ISO timestamp string). |
| `interest_status_committed_asset` | character | Nested asset of the committed-to program (the interest status; usually null). |
| `interest_status_committed_asset_res` | character | Nested logo asset of the committed-to program (the interest status; usually null). |
| `interest_status_transferred_asset` | character | Nested asset of the program transferred to (the interest status; usually null). |
| `interest_status_transferred_asset_res` | character | Nested logo asset of the program transferred to (the interest status; usually null). |
| `interest_status_committed_organization_key` | integer | On3 key of the committed-to program (the interest status). |
| `interest_status_committed_organization_full_name` | character | Full name of the interest status's committed-to program (e.g. 'Alabama Crimson Tide'). |
| `interest_status_committed_organization_name` | character | Short name of the interest status's committed-to program. |
| `interest_status_committed_organization_mascot` | character | Mascot of the interest status's committed-to program. |
| `interest_status_committed_organization_abbreviation` | character | Abbreviation of the interest status's committed-to program. |
| `interest_status_committed_organization_asset_url` | character | CDN URL of the committed-to program's logo (the interest status). |
| `interest_status_committed_organization_asset_key` | integer | On3 asset key of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_domain_override` | character | CDN domain override for the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_domain` | character | CDN domain serving the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_source_override` | character | Source-path override for the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_source` | character | CDN-relative source path of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_title` | character | Editorial title attached to the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_description` | character | Editorial description attached to the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_caption` | character | Editorial caption attached to the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_category` | character | Editorial category label of the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_alt_text` | character | Accessibility alt text of the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_height` | integer | Pixel height of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_width` | integer | Pixel width of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_asset_type` | character | On3 asset-type discriminator of the interest status's committed-to program's logo asset (e.g. Image). |
| `interest_status_committed_organization_asset_file_system` | character | Storage file-system flag of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_path` | character | Storage path of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_type` | character | Media type field of the interest status's committed-to program's logo asset (file extension, e.g. png). |
| `interest_status_committed_organization_asset_thumbnail` | character | Thumbnail variant of the interest status's committed-to program's logo asset (video assets; usually null). |
| `interest_status_committed_organization_asset_duration` | integer | Duration of the interest status's committed-to program's logo asset when it is a video (usually null or 0). |
| `interest_status_committed_organization_asset_mime_type` | character | MIME type of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_slug` | character | URL slug of the committed-to program (the interest status). |
| `interest_status_committed_organization_primary_color` | character | Primary hex color of the interest status's committed-to program. |
| `interest_status_class_rank` | character | Academic class standing recorded on the interest status (e.g. Senior). |
| `interest_status_transfer_entered` | character | Date the player entered the transfer portal (the interest status; null when never entered). |
| `interest_status_recruitment_year` | character | Recruiting-cycle year the interest status belongs to. |
| `interest_status_decommitted_asset` | character | Nested asset of the program decommitted from (the interest status; usually null). |
| `interest_status_transfer` | logical | Transfer flag of the interest status (null when not applicable). |
| `interest_status_expected_to_transfer` | logical | Expected-to-transfer flag of the interest status (null when not applicable). |
| `interest_status_recruitment_key` | integer | On3 key of the recruitment record the interest status belongs to. |
| `interest_status_withdrawn_transfer` | logical | Whether the player withdrew from the transfer portal (the interest status). |
| `interest_status_withdrawn_transfer_date` | character | Date the player withdrew from the transfer portal (the interest status; null when never withdrawn). |
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
| `name` | character | Display name of the row's record. |
| `slug` | character | URL slug of the row's record on On3. |
| `high_school_name` | character | High-school display name (top-level field). |
| `home_town_name` | character | Player's hometown, as listed by On3. |
| `early_enrollee` | logical | Whether the player is an early enrollee. |
| `early_signee` | logical | Whether the player signed in the early signing period. |
| `default_asset_url` | character | URL of the player's headshot image. |
| `class_year` | integer | Player's original recruiting class year. |
| `athlete_verified` | logical | Whether the athlete has verified their own On3 profile. |
| `prospect_verified` | logical | Whether On3 has verified the prospect's profile information. |
| `position_abbreviation` | character | Position abbreviation on the record. |
| `height` | character | Height as a formatted string (e.g. '6-3.5'). |
| `weight` | integer | Weight in pounds. |
| `transfer_industry_comparison` | character | JSON-encoded list of the player's transfer-portal rankings across industry services. |
| `predictions` | character | RPM prediction entries for the transfer destination, as a stringified list. |
| `nil_status` | character | Status of the player's On3 NIL valuation (e.g. active, inactive). |
| `nil_value` | numeric | Player's On3 NIL valuation in dollars. |
| `valuation` | character | Nested NIL valuation object (stringified; usually null). |
| `sport` | character | Nested On3 sport object for the transfer entry (stringified). |
| `eligibility` | character | Eligibility note On3 attaches to the transfer, when any. |
| `organization_history` | character | JSON-encoded list of the programs the player has been on, newest first. |
| `industry_comparison` | character | JSON-encoded list of the player's rankings across industry services. |
| `entered_article` | character | On3 article link covering the player entering the transfer portal (stringified). |
| `exited_article` | character | On3 article link covering the player exiting the portal (stringified). |
| `person_sport_key` | integer | On3 key of the athlete-sport profile (person x sport). |
| `withdrawn_transfer` | logical | Whether the player withdrew from the transfer portal. |
| `withdrawn_transfer_date` | character | Date the player withdrew from the transfer portal, when withdrawn. |
| `rec_status` | character | Recruiting status of the player (e.g. Committed, Enrolled). |
| `index` | integer | Zero-based position of the row in the On3 response list. |
| `high_school_key` | integer | On3 numeric key of the high school. |
| `high_school_full_name` | character | Full name of the high school (with mascot). |
| `high_school_name_2` | character | Name field of the nested high-school object (json_normalize de-duplication of high_school_name). |
| `high_school_known_as` | character | Common short name of the high school, when On3 lists one. |
| `high_school_mascot` | character | High-school mascot. |
| `high_school_abbreviation` | character | High-school abbreviation. |
| `high_school_asset_url` | character | Convenience CDN URL of the high-school logo. |
| `high_school_default_asset_key` | integer | On3 asset key of the high school's logo asset. |
| `high_school_default_asset_domain_override` | character | CDN domain override for the high school's logo asset (usually null). |
| `high_school_default_asset_domain` | character | CDN domain serving the high school's logo asset. |
| `high_school_default_asset_source_override` | character | Source-path override for the high school's logo asset (usually null). |
| `high_school_default_asset_source` | character | CDN-relative source path of the high school's logo asset. |
| `high_school_default_asset_title` | character | Editorial title attached to the high school's logo asset. |
| `high_school_default_asset_description` | character | Editorial description attached to the high school's logo asset (usually null). |
| `high_school_default_asset_caption` | character | Editorial caption attached to the high school's logo asset (usually null). |
| `high_school_default_asset_category` | character | Editorial category label of the high school's logo asset (usually null). |
| `high_school_default_asset_alt_text` | character | Accessibility alt text of the high school's logo asset (usually null). |
| `high_school_default_asset_height` | integer | Pixel height of the high school's logo asset. |
| `high_school_default_asset_width` | integer | Pixel width of the high school's logo asset. |
| `high_school_default_asset_asset_type` | character | On3 asset-type discriminator of the high school's logo asset (e.g. Image). |
| `high_school_default_asset_file_system` | character | Storage file-system flag of the high school's logo asset. |
| `high_school_default_asset_path` | character | Storage path of the high school's logo asset. |
| `high_school_default_asset_type` | character | Media type field of the high school's logo asset (file extension, e.g. png). |
| `high_school_default_asset_thumbnail` | character | Thumbnail variant of the high school's logo asset (video assets; usually null). |
| `high_school_default_asset_duration` | integer | Duration of the high school's logo asset when it is a video (usually null or 0). |
| `high_school_default_asset_mime_type` | character | MIME type of the high school's logo asset. |
| `high_school_slug` | character | URL slug of the high school on On3. |
| `high_school_primary_color` | character | Primary hex color of the high school. |
| `high_school_org_type` | character | Organization type label of the school (e.g. HighSchool). |
| `high_school_org_type_enum` | character | Organization type enum of the school. |
| `high_school_division` | character | Division or classification of the high school, when listed. |
| `high_school_site_keys` | character | JSON-encoded On3 site keys covering the school (usually null). |
| `high_school_url_slug` | character | URL slug variant of the high-school page, with the key appended. |
| `default_asset_key` | integer | On3 asset key of the default asset. |
| `default_asset_domain_override` | character | CDN domain override for the record's default asset (usually null). |
| `default_asset_domain` | character | CDN domain serving the default asset. |
| `default_asset_source_override` | character | Source-path override for the default asset (usually null). |
| `default_asset_source` | character | CDN-relative source path of the default asset. |
| `default_asset_title` | character | Editorial title attached to the default asset. |
| `default_asset_description` | character | Editorial description attached to the default asset (usually null). |
| `default_asset_caption` | character | Editorial caption attached to the default asset (usually null). |
| `default_asset_category` | character | Editorial category label of the default asset (usually null). |
| `default_asset_alt_text` | character | Accessibility alt text of the default asset (usually null). |
| `default_asset_height` | integer | Pixel height of the default asset. |
| `default_asset_width` | integer | Pixel width of the default asset. |
| `default_asset_asset_type` | character | On3 asset-type discriminator of the default asset (e.g. Image). |
| `default_asset_file_system` | character | Storage file-system flag of the default asset. |
| `default_asset_path` | character | Storage path of the default asset. |
| `default_asset_type` | character | Media type field of the default asset (file extension, e.g. png). |
| `default_asset_thumbnail` | character | Thumbnail variant of the default asset (video assets; usually null). |
| `default_asset_duration` | integer | Duration of the default asset when it is a video (usually null or 0). |
| `default_asset_mime_type` | character | MIME type of the default asset. |
| `rating_consensus_rating` | character | Industry-consensus numeric rating paired with the On3 rating (0-100 scale). |
| `rating_consensus_stars` | numeric | Industry-consensus star rating paired with the On3 rating (2-5). |
| `rating_consensus_national_rank` | character | Industry-consensus national rank paired with the On3 rating. |
| `rating_consensus_position_rank` | character | Industry-consensus position rank paired with the On3 rating. |
| `rating_consensus_state_rank` | character | Industry-consensus state rank paired with the On3 rating. |
| `rating_key` | numeric | On3 key of the On3 rating record. |
| `rating_rating` | numeric | Numeric value of the On3 rating (0-100 scale). |
| `rating_stars` | numeric | Star rating of the On3 rating (2-5). |
| `rating_national_rank` | integer | National rank of the On3 rating. |
| `rating_position_rank` | integer | Position rank of the On3 rating. |
| `rating_state_rank` | integer | State rank of the On3 rating. |
| `rating_position_abbr` | character | Position abbreviation the On3 rating was assigned at. |
| `rating_state_abbr` | character | State abbreviation the On3 rating was assigned in. |
| `rating_five_star_plus` | character | Five-star-plus flag on the On3 rating. |
| `roster_rating_consensus_rating` | integer | Industry-consensus numeric rating paired with the On3 roster rating (0-100 scale). |
| `roster_rating_consensus_stars` | integer | Industry-consensus star rating paired with the On3 roster rating (2-5). |
| `roster_rating_consensus_national_rank` | integer | Industry-consensus national rank paired with the On3 roster rating. |
| `roster_rating_consensus_position_rank` | integer | Industry-consensus position rank paired with the On3 roster rating. |
| `roster_rating_consensus_state_rank` | character | Industry-consensus state rank paired with the On3 roster rating. |
| `roster_rating_key` | integer | On3 key of the On3 roster rating record. |
| `roster_rating_rating` | integer | Numeric value of the On3 roster rating (0-100 scale). |
| `roster_rating_stars` | integer | Star rating of the On3 roster rating (2-5). |
| `roster_rating_national_rank` | integer | National rank of the On3 roster rating. |
| `roster_rating_position_rank` | integer | Position rank of the On3 roster rating. |
| `roster_rating_state_rank` | integer | State rank of the On3 roster rating. |
| `roster_rating_position_abbr` | character | Position abbreviation the On3 roster rating was assigned at. |
| `roster_rating_state_abbr` | character | State abbreviation the On3 roster rating was assigned in. |
| `roster_rating_five_star_plus` | logical | Five-star-plus flag on the On3 roster rating. |
| `transfer_rating_consensus_rating` | integer | Industry-consensus numeric rating paired with the On3 transfer-portal rating (0-100 scale). |
| `transfer_rating_consensus_stars` | integer | Industry-consensus star rating paired with the On3 transfer-portal rating (2-5). |
| `transfer_rating_consensus_national_rank` | integer | Industry-consensus national rank paired with the On3 transfer-portal rating. |
| `transfer_rating_consensus_position_rank` | integer | Industry-consensus position rank paired with the On3 transfer-portal rating. |
| `transfer_rating_consensus_state_rank` | character | Industry-consensus state rank paired with the On3 transfer-portal rating. |
| `transfer_rating_key` | integer | On3 key of the On3 transfer-portal rating record. |
| `transfer_rating_rating` | integer | Numeric value of the On3 transfer-portal rating (0-100 scale). |
| `transfer_rating_stars` | integer | Star rating of the On3 transfer-portal rating (2-5). |
| `transfer_rating_national_rank` | character | National rank of the On3 transfer-portal rating. |
| `transfer_rating_position_rank` | integer | Position rank of the On3 transfer-portal rating. |
| `transfer_rating_state_rank` | character | State rank of the On3 transfer-portal rating. |
| `transfer_rating_position_abbr` | character | Position abbreviation the On3 transfer-portal rating was assigned at. |
| `transfer_rating_state_abbr` | character | State abbreviation the On3 transfer-portal rating was assigned in. |
| `transfer_rating_five_star_plus` | logical | Five-star-plus flag on the On3 transfer-portal rating. |
| `commit_status_type` | character | Type of the commitment status (e.g. Committed, Signed, Enrolled, None). |
| `commit_status_short_term_signee` | logical | Short-term-signee flag of the commitment status (null when not applicable). |
| `commit_status_date` | character | Date the commitment status took effect (ISO timestamp string). |
| `commit_status_committed_asset` | character | Nested asset of the committed-to program (the commitment status; usually null). |
| `commit_status_committed_asset_res` | character | Nested logo asset of the committed-to program (the commitment status; usually null). |
| `commit_status_transferred_asset` | character | Nested asset of the program transferred to (the commitment status; usually null). |
| `commit_status_transferred_asset_res` | character | Nested logo asset of the program transferred to (the commitment status; usually null). |
| `commit_status_committed_organization_key` | integer | On3 key of the committed-to program (the commitment status). |
| `commit_status_committed_organization_full_name` | character | Full name of the commitment status's committed-to program (e.g. 'Alabama Crimson Tide'). |
| `commit_status_committed_organization_name` | character | Short name of the commitment status's committed-to program. |
| `commit_status_committed_organization_mascot` | character | Mascot of the commitment status's committed-to program. |
| `commit_status_committed_organization_abbreviation` | character | Abbreviation of the commitment status's committed-to program. |
| `commit_status_committed_organization_asset_url` | character | CDN URL of the committed-to program's logo (the commitment status). |
| `commit_status_committed_organization_asset_key` | integer | On3 asset key of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_domain_override` | character | CDN domain override for the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_domain` | character | CDN domain serving the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_source_override` | character | Source-path override for the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_source` | character | CDN-relative source path of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_title` | character | Editorial title attached to the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_description` | character | Editorial description attached to the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_caption` | character | Editorial caption attached to the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_category` | character | Editorial category label of the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_alt_text` | character | Accessibility alt text of the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_organization_asset_height` | integer | Pixel height of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_width` | integer | Pixel width of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_asset_type` | character | On3 asset-type discriminator of the commitment status's committed-to program's logo asset (e.g. Image). |
| `commit_status_committed_organization_asset_file_system` | character | Storage file-system flag of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_path` | character | Storage path of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_asset_type` | character | Media type field of the commitment status's committed-to program's logo asset (file extension, e.g. png). |
| `commit_status_committed_organization_asset_thumbnail` | character | Thumbnail variant of the commitment status's committed-to program's logo asset (video assets; usually null). |
| `commit_status_committed_organization_asset_duration` | integer | Duration of the commitment status's committed-to program's logo asset when it is a video (usually null or 0). |
| `commit_status_committed_organization_asset_mime_type` | character | MIME type of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_organization_slug` | character | URL slug of the committed-to program (the commitment status). |
| `commit_status_committed_organization_primary_color` | character | Primary hex color of the commitment status's committed-to program. |
| `commit_status_class_rank` | character | Academic class standing recorded on the commitment status (e.g. Senior). |
| `commit_status_transfer_entered` | character | Date the player entered the transfer portal (the commitment status; null when never entered). |
| `commit_status_recruitment_year` | integer | Recruiting-cycle year the commitment status belongs to. |
| `commit_status_decommitted_asset` | character | Nested asset of the program decommitted from (the commitment status; usually null). |
| `commit_status_transfer` | logical | Transfer flag of the commitment status (null when not applicable). |
| `commit_status_expected_to_transfer` | logical | Expected-to-transfer flag of the commitment status (null when not applicable). |
| `commit_status_recruitment_key` | integer | On3 key of the recruitment record the commitment status belongs to. |
| `commit_status_withdrawn_transfer` | logical | Whether the player withdrew from the transfer portal (the commitment status). |
| `commit_status_withdrawn_transfer_date` | character | Date the player withdrew from the transfer portal (the commitment status; null when never withdrawn). |
| `last_team_asset_url_key` | integer | On3 asset key behind the previous program's logo link. |
| `last_team_asset_url_url` | character | Full CDN URL of the previous program's logo link. |
| `last_team_asset_url_slug` | character | URL slug of the program the previous program's logo link belongs to. |
| `last_team_asset_url_full_name` | character | Full name of the program the previous program's logo link belongs to. |
| `last_team_full_name` | character | Full name of the previous program (e.g. 'Alabama Crimson Tide'). |
| `last_team_key` | integer | On3 numeric key of the previous program. |
| `last_team_name` | character | Short name of the previous program. |
| `last_team_mascot` | character | Mascot of the previous program. |
| `last_team_abbreviation` | character | Abbreviation of the previous program. |
| `last_team_asset_key` | integer | On3 asset key of the previous program's logo asset. |
| `last_team_asset_domain_override` | character | CDN domain override for the previous program's logo asset (usually null). |
| `last_team_asset_domain` | character | CDN domain serving the previous program's logo asset. |
| `last_team_asset_source_override` | character | Source-path override for the previous program's logo asset (usually null). |
| `last_team_asset_source` | character | CDN-relative source path of the previous program's logo asset. |
| `last_team_asset_title` | character | Editorial title attached to the previous program's logo asset. |
| `last_team_asset_description` | character | Editorial description attached to the previous program's logo asset (usually null). |
| `last_team_asset_caption` | character | Editorial caption attached to the previous program's logo asset (usually null). |
| `last_team_asset_category` | character | Editorial category label of the previous program's logo asset (usually null). |
| `last_team_asset_alt_text` | character | Accessibility alt text of the previous program's logo asset (usually null). |
| `last_team_asset_height` | integer | Pixel height of the previous program's logo asset. |
| `last_team_asset_width` | integer | Pixel width of the previous program's logo asset. |
| `last_team_asset_asset_type` | character | On3 asset-type discriminator of the previous program's logo asset (e.g. Image). |
| `last_team_asset_file_system` | character | Storage file-system flag of the previous program's logo asset. |
| `last_team_asset_path` | character | Storage path of the previous program's logo asset. |
| `last_team_asset_type` | character | Media type field of the previous program's logo asset (file extension, e.g. png). |
| `last_team_asset_thumbnail` | character | Thumbnail variant of the previous program's logo asset (video assets; usually null). |
| `last_team_asset_duration` | integer | Duration of the previous program's logo asset when it is a video (usually null or 0). |
| `last_team_asset_mime_type` | character | MIME type of the previous program's logo asset. |
| `last_team_slug` | character | URL slug of the previous program on On3. |
| `last_team_primary_color` | character | Primary hex color of the previous program. |
| `committed_article_key` | numeric | On3 article key of the commitment article. |
| `committed_article_slug` | character | URL slug of the commitment article. |
| `committed_article_full_url` | character | Site-relative URL of the commitment article. |
| `committed_article_title` | character | Headline of the commitment article. |
| `committed_article_is_premium` | character | Whether the commitment article is behind the On3+ paywall. |
| `committed_article_date_published_gmt` | character | Publication timestamp of the commitment article (GMT, ISO string). |
| `interest_status_type` | character | Type of the interest status (e.g. Committed, Signed, Enrolled, None). |
| `interest_status_short_term_signee` | logical | Short-term-signee flag of the interest status (null when not applicable). |
| `interest_status_date` | character | Date the interest status took effect (ISO timestamp string). |
| `interest_status_committed_asset` | character | Nested asset of the committed-to program (the interest status; usually null). |
| `interest_status_committed_asset_res` | character | Nested logo asset of the committed-to program (the interest status; usually null). |
| `interest_status_transferred_asset` | character | Nested asset of the program transferred to (the interest status; usually null). |
| `interest_status_transferred_asset_res` | character | Nested logo asset of the program transferred to (the interest status; usually null). |
| `interest_status_committed_organization_key` | integer | On3 key of the committed-to program (the interest status). |
| `interest_status_committed_organization_full_name` | character | Full name of the interest status's committed-to program (e.g. 'Alabama Crimson Tide'). |
| `interest_status_committed_organization_name` | character | Short name of the interest status's committed-to program. |
| `interest_status_committed_organization_mascot` | character | Mascot of the interest status's committed-to program. |
| `interest_status_committed_organization_abbreviation` | character | Abbreviation of the interest status's committed-to program. |
| `interest_status_committed_organization_asset_url` | character | CDN URL of the committed-to program's logo (the interest status). |
| `interest_status_committed_organization_asset_key` | integer | On3 asset key of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_domain_override` | character | CDN domain override for the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_domain` | character | CDN domain serving the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_source_override` | character | Source-path override for the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_source` | character | CDN-relative source path of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_title` | character | Editorial title attached to the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_description` | character | Editorial description attached to the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_caption` | character | Editorial caption attached to the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_category` | character | Editorial category label of the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_alt_text` | character | Accessibility alt text of the interest status's committed-to program's logo asset (usually null). |
| `interest_status_committed_organization_asset_height` | integer | Pixel height of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_width` | integer | Pixel width of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_asset_type` | character | On3 asset-type discriminator of the interest status's committed-to program's logo asset (e.g. Image). |
| `interest_status_committed_organization_asset_file_system` | character | Storage file-system flag of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_path` | character | Storage path of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_asset_type` | character | Media type field of the interest status's committed-to program's logo asset (file extension, e.g. png). |
| `interest_status_committed_organization_asset_thumbnail` | character | Thumbnail variant of the interest status's committed-to program's logo asset (video assets; usually null). |
| `interest_status_committed_organization_asset_duration` | integer | Duration of the interest status's committed-to program's logo asset when it is a video (usually null or 0). |
| `interest_status_committed_organization_asset_mime_type` | character | MIME type of the interest status's committed-to program's logo asset. |
| `interest_status_committed_organization_slug` | character | URL slug of the committed-to program (the interest status). |
| `interest_status_committed_organization_primary_color` | character | Primary hex color of the interest status's committed-to program. |
| `interest_status_class_rank` | character | Academic class standing recorded on the interest status (e.g. Senior). |
| `interest_status_transfer_entered` | character | Date the player entered the transfer portal (the interest status; null when never entered). |
| `interest_status_recruitment_year` | character | Recruiting-cycle year the interest status belongs to. |
| `interest_status_decommitted_asset` | character | Nested asset of the program decommitted from (the interest status; usually null). |
| `interest_status_transfer` | logical | Transfer flag of the interest status (null when not applicable). |
| `interest_status_expected_to_transfer` | logical | Expected-to-transfer flag of the interest status (null when not applicable). |
| `interest_status_recruitment_key` | integer | On3 key of the recruitment record the interest status belongs to. |
| `interest_status_withdrawn_transfer` | logical | Whether the player withdrew from the transfer portal (the interest status). |
| `interest_status_withdrawn_transfer_date` | character | Date the player withdrew from the transfer portal (the interest status; null when never withdrawn). |
| `rating` | character | On3 rating of the player (0-100 scale), when published. |
| `committed_article` | character | On3 article link covering the player's transfer commitment (stringified). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_transfers_latest-example}

```python
on3_transfers_latest()
```

_Last validated n/a._
