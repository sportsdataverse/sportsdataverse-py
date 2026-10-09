# CFB — On3 Recruit Database (api.on3.com) — Nil

> CFB — On3 Recruit Database (api.on3.com) — Nil — function reference in sdv-py, the SportsDataverse Python package.

## on3_nil_100

GET /rdb/v1/nil-100

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/nil-100`

**Valid URL:** [https://api.on3.com/public/rdb/v1/nil-100](https://api.on3.com/public/rdb/v1/nil-100)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | year query parameter. |

### Returns {#on3_nil_100-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the row has nested objects whose flattened column names depend on which are null in the data; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

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
| `person_rating_consensus_rating` | numeric | Industry-consensus numeric rating paired with the person's On3 rating (0-100 scale). |
| `person_rating_consensus_stars` | integer | Industry-consensus star rating paired with the person's On3 rating (2-5). |
| `person_rating_consensus_national_rank` | integer | Industry-consensus national rank paired with the person's On3 rating. |
| `person_rating_consensus_position_rank` | integer | Industry-consensus position rank paired with the person's On3 rating. |
| `person_rating_consensus_state_rank` | integer | Industry-consensus state rank paired with the person's On3 rating. |
| `person_rating_key` | integer | On3 key of the person's On3 rating record. |
| `person_rating_rating` | numeric | Numeric value of the person's On3 rating (0-100 scale). |
| `person_rating_stars` | integer | Star rating of the person's On3 rating (2-5). |
| `person_rating_national_rank` | integer | National rank of the person's On3 rating. |
| `person_rating_position_rank` | integer | Position rank of the person's On3 rating. |
| `person_rating_state_rank` | integer | State rank of the person's On3 rating. |
| `person_rating_position_abbr` | character | Position abbreviation the person's On3 rating was assigned at. |
| `person_rating_state_abbr` | character | State abbreviation the person's On3 rating was assigned in. |
| `person_rating_five_star_plus` | logical | Five-star-plus flag on the person's On3 rating. |
| `person_division` | character | Division of the person's current organization (e.g. NCAA-FB, NCAA-BK). |
| `person_default_sport_key` | integer | On3 numeric key of the person's primary sport. |
| `person_default_sport_name` | character | Name of the person's primary sport (e.g. Football). |
| `person_organization_level` | character | Level of the person's current organization (e.g. HighSchool, College, Professional). |
| `person_age` | numeric | Age of the person in years, when known. |
| `person_tags` | character | JSON-encoded list of On3 profile tags on the person (e.g. Influencer). |
| `person_key` | integer | On3 numeric key of the person. |
| `person_recruitment_key` | integer | On3 key of the person's active recruitment record. |
| `person_name` | character | Display name of the person. |
| `person_slug` | character | URL slug of the person's On3 profile. |
| `person_high_school_name` | character | High-school display name on the person's record. |
| `person_high_school_key` | integer | On3 numeric key of the person's high school. |
| `person_high_school_full_name` | character | Full name of the person's high school (e.g. 'Alabama Crimson Tide'). |
| `person_high_school_name_2` | character | High-school display name on the person's record (json_normalize de-duplication suffix). |
| `person_high_school_known_as` | character | Common short name of the person's high school, when On3 lists one. |
| `person_high_school_mascot` | character | Mascot of the person's high school. |
| `person_high_school_abbreviation` | character | Abbreviation of the person's high school. |
| `person_high_school_asset_url` | character | Convenience CDN URL of the person's high school's logo. |
| `person_high_school_default_asset_key` | numeric | On3 asset key of the person's high school's logo asset. |
| `person_high_school_default_asset_domain_override` | character | CDN domain override for the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_domain` | character | CDN domain serving the person's high school's logo asset. |
| `person_high_school_default_asset_source_override` | character | Source-path override for the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_source` | character | CDN-relative source path of the person's high school's logo asset. |
| `person_high_school_default_asset_title` | character | Editorial title attached to the person's high school's logo asset. |
| `person_high_school_default_asset_description` | character | Editorial description attached to the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_caption` | character | Editorial caption attached to the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_category` | character | Editorial category label of the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_alt_text` | character | Accessibility alt text of the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_height` | numeric | Pixel height of the person's high school's logo asset. |
| `person_high_school_default_asset_width` | numeric | Pixel width of the person's high school's logo asset. |
| `person_high_school_default_asset_asset_type` | character | On3 asset-type discriminator of the person's high school's logo asset (e.g. Image). |
| `person_high_school_default_asset_file_system` | character | Storage file-system flag of the person's high school's logo asset. |
| `person_high_school_default_asset_path` | character | Storage path of the person's high school's logo asset. |
| `person_high_school_default_asset_type` | character | Media type field of the person's high school's logo asset (file extension, e.g. png). |
| `person_high_school_default_asset_thumbnail` | character | Thumbnail variant of the person's high school's logo asset (video assets; usually null). |
| `person_high_school_default_asset_duration` | numeric | Duration of the person's high school's logo asset when it is a video (usually null or 0). |
| `person_high_school_default_asset_mime_type` | character | MIME type of the person's high school's logo asset. |
| `person_high_school_slug` | character | URL slug of the person's high school on On3. |
| `person_high_school_primary_color` | character | Primary hex color of the person's high school. |
| `person_high_school_org_type` | character | Organization type label of the person's high school (e.g. HighSchool, College). |
| `person_high_school_org_type_enum` | character | Organization type enum of the person's high school (same vocabulary as org_type). |
| `person_high_school_division` | character | Division or classification of the person's high school (e.g. NCAA-FB). |
| `person_high_school_site_keys` | character | JSON-encoded On3 site keys covering the person's high school (usually null). |
| `person_high_school_url_slug` | character | URL slug variant of the person's high school's page, with the key appended. |
| `person_home_town_name` | character | Home town of the person as On3 lists it (e.g. 'Belleville, MI'). |
| `person_early_enrollee` | logical | Whether the person early-enrolled at college. |
| `person_early_signee` | logical | Whether the person signed during the early signing period. |
| `person_default_asset_url` | character | Convenience CDN URL of the person's headshot. |
| `person_class_year` | integer | High-school graduating class year of the person. |
| `person_athlete_verified` | logical | Whether the person's athlete profile is verified by On3. |
| `person_prospect_verified` | logical | Whether the person's prospect measurables are verified by On3. |
| `person_default_asset_key` | integer | On3 asset key of the person's headshot asset. |
| `person_default_asset_domain_override` | character | CDN domain override for the person's headshot asset (usually null). |
| `person_default_asset_domain` | character | CDN domain serving the person's headshot asset. |
| `person_default_asset_source_override` | character | Source-path override for the person's headshot asset (usually null). |
| `person_default_asset_source` | character | CDN-relative source path of the person's headshot asset. |
| `person_default_asset_title` | character | Editorial title attached to the person's headshot asset. |
| `person_default_asset_description` | character | Editorial description attached to the person's headshot asset (usually null). |
| `person_default_asset_caption` | character | Editorial caption attached to the person's headshot asset (usually null). |
| `person_default_asset_category` | character | Editorial category label of the person's headshot asset (usually null). |
| `person_default_asset_alt_text` | character | Accessibility alt text of the person's headshot asset (usually null). |
| `person_default_asset_height` | integer | Pixel height of the person's headshot asset. |
| `person_default_asset_width` | integer | Pixel width of the person's headshot asset. |
| `person_default_asset_asset_type` | character | On3 asset-type discriminator of the person's headshot asset (e.g. Image). |
| `person_default_asset_file_system` | character | Storage file-system flag of the person's headshot asset. |
| `person_default_asset_path` | character | Storage path of the person's headshot asset. |
| `person_default_asset_type` | character | Media type field of the person's headshot asset (file extension, e.g. png). |
| `person_default_asset_thumbnail` | character | Thumbnail variant of the person's headshot asset (video assets; usually null). |
| `person_default_asset_duration` | integer | Duration of the person's headshot asset when it is a video (usually null or 0). |
| `person_default_asset_mime_type` | character | MIME type of the person's headshot asset. |
| `person_position_abbreviation` | character | Position abbreviation on the person's record. |
| `person_height` | character | Height of the person: a formatted string (e.g. '6-8') or inches, depending on the endpoint. |
| `person_weight` | integer | Weight of the person in pounds. |
| `person_roster_rating` | character | On3 roster (transfer-portal-adjusted) rating of the person, when published. |
| `person_commit_status_type` | character | Type of the person's commitment status (e.g. Committed, Signed, Enrolled, None). |
| `person_commit_status_short_term_signee` | logical | Short-term-signee flag of the person's commitment status (null when not applicable). |
| `person_commit_status_date` | character | Date the person's commitment status took effect (ISO timestamp string). |
| `person_commit_status_committed_asset` | character | Nested asset of the committed-to program (the person's commitment status; usually null). |
| `person_commit_status_committed_asset_res` | character | Nested logo asset of the committed-to program (the person's commitment status; usually null). |
| `person_commit_status_transferred_asset_key` | integer | On3 numeric key of the person's commitment status's program transferred to. |
| `person_commit_status_transferred_asset_url` | character | CDN URL of the person's commitment status's program transferred to's logo. |
| `person_commit_status_transferred_asset_slug` | character | URL slug of the person's commitment status's program transferred to on On3. |
| `person_commit_status_transferred_asset_full_name` | character | Full name of the person's commitment status's program transferred to (e.g. 'Alabama Crimson Tide'). |
| `person_commit_status_transferred_asset_res` | character | Nested logo asset of the program transferred to (the person's commitment status; usually null). |
| `person_commit_status_committed_organization_key` | integer | On3 key of the committed-to program (the person's commitment status). |
| `person_commit_status_committed_organization_full_name` | character | Full name of the person's commitment status's committed-to program (e.g. 'Alabama Crimson Tide'). |
| `person_commit_status_committed_organization_name` | character | Short name of the person's commitment status's committed-to program. |
| `person_commit_status_committed_organization_mascot` | character | Mascot of the person's commitment status's committed-to program. |
| `person_commit_status_committed_organization_abbreviation` | character | Abbreviation of the person's commitment status's committed-to program. |
| `person_commit_status_committed_organization_asset_url` | character | CDN URL of the committed-to program's logo (the person's commitment status). |
| `person_commit_status_committed_organization_asset_key` | integer | On3 asset key of the person's commitment status's committed-to program's logo asset. |
| `person_commit_status_committed_organization_asset_domain_override` | character | CDN domain override for the person's commitment status's committed-to program's logo asset (usually null). |
| `person_commit_status_committed_organization_asset_domain` | character | CDN domain serving the person's commitment status's committed-to program's logo asset. |
| `person_commit_status_committed_organization_asset_source_override` | character | Source-path override for the person's commitment status's committed-to program's logo asset (usually null). |
| `person_commit_status_committed_organization_asset_source` | character | CDN-relative source path of the person's commitment status's committed-to program's logo asset. |
| `person_commit_status_committed_organization_asset_title` | character | Editorial title attached to the person's commitment status's committed-to program's logo asset. |
| `person_commit_status_committed_organization_asset_description` | character | Editorial description attached to the person's commitment status's committed-to program's logo asset (usually null). |
| `person_commit_status_committed_organization_asset_caption` | character | Editorial caption attached to the person's commitment status's committed-to program's logo asset (usually null). |
| `person_commit_status_committed_organization_asset_category` | character | Editorial category label of the person's commitment status's committed-to program's logo asset (usually null). |
| `person_commit_status_committed_organization_asset_alt_text` | character | Accessibility alt text of the person's commitment status's committed-to program's logo asset (usually null). |
| `person_commit_status_committed_organization_asset_height` | integer | Pixel height of the person's commitment status's committed-to program's logo asset. |
| `person_commit_status_committed_organization_asset_width` | integer | Pixel width of the person's commitment status's committed-to program's logo asset. |
| `person_commit_status_committed_organization_asset_asset_type` | character | On3 asset-type discriminator of the person's commitment status's committed-to program's logo asset (e.g. Image). |
| `person_commit_status_committed_organization_asset_file_system` | character | Storage file-system flag of the person's commitment status's committed-to program's logo asset. |
| `person_commit_status_committed_organization_asset_path` | character | Storage path of the person's commitment status's committed-to program's logo asset. |
| `person_commit_status_committed_organization_asset_type` | character | Media type field of the person's commitment status's committed-to program's logo asset (file extension, e.g. png). |
| `person_commit_status_committed_organization_asset_thumbnail` | character | Thumbnail variant of the person's commitment status's committed-to program's logo asset (video assets; usually null). |
| `person_commit_status_committed_organization_asset_duration` | integer | Duration of the person's commitment status's committed-to program's logo asset when it is a video (usually null or 0). |
| `person_commit_status_committed_organization_asset_mime_type` | character | MIME type of the person's commitment status's committed-to program's logo asset. |
| `person_commit_status_committed_organization_slug` | character | URL slug of the committed-to program (the person's commitment status). |
| `person_commit_status_committed_organization_primary_color` | character | Primary hex color of the person's commitment status's committed-to program. |
| `person_commit_status_class_rank` | character | Academic class standing recorded on the person's commitment status (e.g. Senior). |
| `person_commit_status_transfer_entered` | character | Date the player entered the transfer portal (the person's commitment status; null when never entered). |
| `person_commit_status_recruitment_year` | integer | Recruiting-cycle year the person's commitment status belongs to. |
| `person_commit_status_decommitted_asset` | character | Nested asset of the program decommitted from (the person's commitment status; usually null). |
| `person_commit_status_transfer` | logical | Transfer flag of the person's commitment status (null when not applicable). |
| `person_commit_status_expected_to_transfer` | logical | Expected-to-transfer flag of the person's commitment status (null when not applicable). |
| `person_commit_status_recruitment_key` | integer | On3 key of the recruitment record the person's commitment status belongs to. |
| `person_commit_status_withdrawn_transfer` | logical | Whether the player withdrew from the transfer portal (the person's commitment status). |
| `person_commit_status_withdrawn_transfer_date` | character | Date the player withdrew from the transfer portal (the person's commitment status; null when never withdrawn). |
| `person_predictions` | character | JSON-encoded On3 RPM (Recruiting Prediction Machine) entries for the person. |
| `person_nil_status` | character | On3 NIL valuation status of the person (e.g. Normal). |
| `person_nil_value` | numeric | On3 NIL valuation of the person in US dollars. |
| `person_sport` | character | Sport slug the video belongs to. |
| `valuation_nil_status` | character | Status of the NIL valuation (e.g. Normal). |
| `valuation_valuation` | integer | The NIL valuation in US dollars. |
| `valuation_valuation_change` | integer | Change in the NIL valuation since the previous update, in US dollars. |
| `valuation_followers` | integer | Social-media follower count feeding the NIL valuation. |
| `valuation_rank` | integer | Overall rank of the NIL valuation across On3's NIL 100. |
| `valuation_last_updated` | integer | Unix timestamp (seconds) of the last update to the NIL valuation. |
| `valuation_whisper` | numeric | On3 whisper valuation (reported deal value) behind the NIL valuation, in US dollars. |
| `valuation_whisper_change` | numeric | Change in the whisper valuation behind the NIL valuation, in US dollars. |
| `valuation_social_valuations` | character | JSON-encoded per-platform social valuations behind the NIL valuation. |
| `valuation_group_rank` | integer | Rank of the NIL valuation within its group (sport or position). |
| `valuation_group_name` | character | Name of the group the NIL valuation is ranked within (usually null). |
| `valuation_tags` | character | JSON-encoded list of NIL tags on the NIL valuation (e.g. Influencer). |
| `valuation_roster_value` | character | Nested roster-value object of the NIL valuation (usually null). |
| `valuation_nil_value` | character | Nested NIL-value object of the NIL valuation (usually null). |
| `person_high_school_default_asset` | character | Nested default-asset object of the person's high school (null when On3 ships none). |

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

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

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
| `person_default_sport_key` | integer | On3 numeric key of the person's primary sport. |
| `person_default_sport_name` | character | Name of the person's primary sport (e.g. Football). |
| `person_default_sport_slug` | character | URL slug of the person's primary sport. |
| `person_default_sport_abbreviation` | character | Abbreviation of the person's primary sport. |
| `person_default_sport_is_rankable` | logical | Whether On3 ranks players in the person's primary sport. |
| `person_default_sport_is_industry_rankable` | logical | Whether industry-consensus rankings exist for the person's primary sport. |
| `person_default_sport_is_scoutable` | logical | Whether On3 scouting reports exist for the person's primary sport. |
| `person_rating_consensus_rating` | numeric | Industry-consensus numeric rating paired with the person's On3 rating (0-100 scale). |
| `person_rating_consensus_stars` | integer | Industry-consensus star rating paired with the person's On3 rating (2-5). |
| `person_rating_consensus_national_rank` | integer | Industry-consensus national rank paired with the person's On3 rating. |
| `person_rating_consensus_position_rank` | integer | Industry-consensus position rank paired with the person's On3 rating. |
| `person_rating_consensus_state_rank` | integer | Industry-consensus state rank paired with the person's On3 rating. |
| `person_rating_key` | integer | On3 key of the person's On3 rating record. |
| `person_rating_rating` | numeric | Numeric value of the person's On3 rating (0-100 scale). |
| `person_rating_stars` | integer | Star rating of the person's On3 rating (2-5). |
| `person_rating_national_rank` | integer | National rank of the person's On3 rating. |
| `person_rating_position_rank` | integer | Position rank of the person's On3 rating. |
| `person_rating_state_rank` | integer | State rank of the person's On3 rating. |
| `person_rating_position_abbr` | character | Position abbreviation the person's On3 rating was assigned at. |
| `person_rating_state_abbr` | character | State abbreviation the person's On3 rating was assigned in. |
| `person_rating_five_star_plus` | logical | Five-star-plus flag on the person's On3 rating. |
| `person_status_is_committed` | logical | Whether the recruit is committed (the person's recruiting status). |
| `person_status_is_signed` | logical | Whether the recruit has signed (the person's recruiting status). |
| `person_status_is_transfer` | logical | Whether the recruit is a transfer (the person's recruiting status). |
| `person_status_is_enrolled` | logical | Whether the recruit is enrolled (the person's recruiting status). |
| `person_status_commitment_date` | character | Date of the commitment (the person's recruiting status; ISO timestamp string). |
| `person_status_committed_organization_key` | numeric | On3 key of the committed-to program (the person's recruiting status). |
| `person_status_committed_organization_slug` | character | URL slug of the committed-to program (the person's recruiting status). |
| `person_status_committed_organization_asset_url` | character | CDN URL of the committed-to program's logo (the person's recruiting status). |
| `person_status_committed_organization_asset_key` | numeric | On3 asset key of the person's recruiting status's committed-to program's logo asset. |
| `person_status_committed_organization_asset_domain_override` | character | CDN domain override for the person's recruiting status's committed-to program's logo asset (usually null). |
| `person_status_committed_organization_asset_domain` | character | CDN domain serving the person's recruiting status's committed-to program's logo asset. |
| `person_status_committed_organization_asset_source_override` | character | Source-path override for the person's recruiting status's committed-to program's logo asset (usually null). |
| `person_status_committed_organization_asset_source` | character | CDN-relative source path of the person's recruiting status's committed-to program's logo asset. |
| `person_status_committed_organization_asset_title` | character | Editorial title attached to the person's recruiting status's committed-to program's logo asset. |
| `person_status_committed_organization_asset_description` | character | Editorial description attached to the person's recruiting status's committed-to program's logo asset (usually null). |
| `person_status_committed_organization_asset_caption` | character | Editorial caption attached to the person's recruiting status's committed-to program's logo asset (usually null). |
| `person_status_committed_organization_asset_category` | character | Editorial category label of the person's recruiting status's committed-to program's logo asset (usually null). |
| `person_status_committed_organization_asset_alt_text` | character | Accessibility alt text of the person's recruiting status's committed-to program's logo asset (usually null). |
| `person_status_committed_organization_asset_height` | numeric | Pixel height of the person's recruiting status's committed-to program's logo asset. |
| `person_status_committed_organization_asset_width` | numeric | Pixel width of the person's recruiting status's committed-to program's logo asset. |
| `person_status_committed_organization_asset_asset_type` | character | On3 asset-type discriminator of the person's recruiting status's committed-to program's logo asset (e.g. Image). |
| `person_status_committed_organization_asset_file_system` | character | Storage file-system flag of the person's recruiting status's committed-to program's logo asset. |
| `person_status_committed_organization_asset_path` | character | Storage path of the person's recruiting status's committed-to program's logo asset. |
| `person_status_committed_organization_asset_type` | character | Media type field of the person's recruiting status's committed-to program's logo asset (file extension, e.g. png). |
| `person_status_committed_organization_asset_thumbnail` | character | Thumbnail variant of the person's recruiting status's committed-to program's logo asset (video assets; usually null). |
| `person_status_committed_organization_asset_duration` | numeric | Duration of the person's recruiting status's committed-to program's logo asset when it is a video (usually null or 0). |
| `person_status_committed_organization_asset_mime_type` | character | MIME type of the person's recruiting status's committed-to program's logo asset. |
| `person_status_committed_organization_primary_color` | character | Primary hex color of the person's recruiting status's committed-to program. |
| `person_status_transferred_from_organization_asset_url` | character | Convenience CDN URL of the person's recruiting status's program transferred from's logo. |
| `person_status_transferred_from_organization_slug` | character | URL slug of the person's recruiting status's program transferred from on On3. |
| `person_status_highest_interest_level` | numeric | Highest interest level a program has logged for the recruit (the person's recruiting status). |
| `person_status_interest_count` | integer | Number of programs with logged interest in the recruit (the person's recruiting status). |
| `person_status_recruitment_year` | integer | Recruiting-cycle year the person's recruiting status belongs to. |
| `person_status_sport_name` | character | Sport the person's recruiting status applies to. |
| `person_status_short_term_signee` | logical | Short-term-signee flag of the person's recruiting status (null when not applicable). |
| `person_predictions` | character | JSON-encoded On3 RPM (Recruiting Prediction Machine) entries for the person. |
| `person_tags` | character | JSON-encoded list of On3 profile tags on the person (e.g. Influencer). |
| `person_key` | integer | On3 numeric key of the person. |
| `person_name` | character | Display name of the person. |
| `person_slug` | character | URL slug of the person's On3 profile. |
| `person_high_school_name` | character | High-school display name on the person's record. |
| `person_high_school_key` | integer | On3 numeric key of the person's high school. |
| `person_high_school_full_name` | character | Full name of the person's high school (e.g. 'Alabama Crimson Tide'). |
| `person_high_school_name_2` | character | High-school display name on the person's record (json_normalize de-duplication suffix). |
| `person_high_school_known_as` | character | Common short name of the person's high school, when On3 lists one. |
| `person_high_school_mascot` | character | Mascot of the person's high school. |
| `person_high_school_abbreviation` | character | Abbreviation of the person's high school. |
| `person_high_school_asset_url` | character | Convenience CDN URL of the person's high school's logo. |
| `person_high_school_default_asset_key` | integer | On3 asset key of the person's high school's logo asset. |
| `person_high_school_default_asset_domain_override` | character | CDN domain override for the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_domain` | character | CDN domain serving the person's high school's logo asset. |
| `person_high_school_default_asset_source_override` | character | Source-path override for the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_source` | character | CDN-relative source path of the person's high school's logo asset. |
| `person_high_school_default_asset_title` | character | Editorial title attached to the person's high school's logo asset. |
| `person_high_school_default_asset_description` | character | Editorial description attached to the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_caption` | character | Editorial caption attached to the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_category` | character | Editorial category label of the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_alt_text` | character | Accessibility alt text of the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_height` | integer | Pixel height of the person's high school's logo asset. |
| `person_high_school_default_asset_width` | integer | Pixel width of the person's high school's logo asset. |
| `person_high_school_default_asset_asset_type` | character | On3 asset-type discriminator of the person's high school's logo asset (e.g. Image). |
| `person_high_school_default_asset_file_system` | character | Storage file-system flag of the person's high school's logo asset. |
| `person_high_school_default_asset_path` | character | Storage path of the person's high school's logo asset. |
| `person_high_school_default_asset_type` | character | Media type field of the person's high school's logo asset (file extension, e.g. png). |
| `person_high_school_default_asset_thumbnail` | character | Thumbnail variant of the person's high school's logo asset (video assets; usually null). |
| `person_high_school_default_asset_duration` | integer | Duration of the person's high school's logo asset when it is a video (usually null or 0). |
| `person_high_school_default_asset_mime_type` | character | MIME type of the person's high school's logo asset. |
| `person_high_school_slug` | character | URL slug of the person's high school on On3. |
| `person_high_school_primary_color` | character | Primary hex color of the person's high school. |
| `person_high_school_org_type` | character | Organization type label of the person's high school (e.g. HighSchool, College). |
| `person_high_school_org_type_enum` | character | Organization type enum of the person's high school (same vocabulary as org_type). |
| `person_high_school_division` | character | Division or classification of the person's high school (e.g. NCAA-FB). |
| `person_high_school_site_keys` | character | JSON-encoded On3 site keys covering the person's high school (usually null). |
| `person_high_school_url_slug` | character | URL slug variant of the person's high school's page, with the key appended. |
| `person_home_town_name` | character | Home town of the person as On3 lists it (e.g. 'Belleville, MI'). |
| `person_default_asset_url` | character | Convenience CDN URL of the person's headshot. |
| `person_default_asset_key` | integer | On3 asset key of the person's headshot asset. |
| `person_default_asset_domain_override` | character | CDN domain override for the person's headshot asset (usually null). |
| `person_default_asset_domain` | character | CDN domain serving the person's headshot asset. |
| `person_default_asset_source_override` | character | Source-path override for the person's headshot asset (usually null). |
| `person_default_asset_source` | character | CDN-relative source path of the person's headshot asset. |
| `person_default_asset_title` | character | Editorial title attached to the person's headshot asset. |
| `person_default_asset_description` | character | Editorial description attached to the person's headshot asset (usually null). |
| `person_default_asset_caption` | character | Editorial caption attached to the person's headshot asset (usually null). |
| `person_default_asset_category` | character | Editorial category label of the person's headshot asset (usually null). |
| `person_default_asset_alt_text` | character | Accessibility alt text of the person's headshot asset (usually null). |
| `person_default_asset_height` | integer | Pixel height of the person's headshot asset. |
| `person_default_asset_width` | integer | Pixel width of the person's headshot asset. |
| `person_default_asset_asset_type` | character | On3 asset-type discriminator of the person's headshot asset (e.g. Image). |
| `person_default_asset_file_system` | character | Storage file-system flag of the person's headshot asset. |
| `person_default_asset_path` | character | Storage path of the person's headshot asset. |
| `person_default_asset_type` | character | Media type field of the person's headshot asset (file extension, e.g. png). |
| `person_default_asset_thumbnail` | character | Thumbnail variant of the person's headshot asset (video assets; usually null). |
| `person_default_asset_duration` | integer | Duration of the person's headshot asset when it is a video (usually null or 0). |
| `person_default_asset_mime_type` | character | MIME type of the person's headshot asset. |
| `person_early_signee` | logical | Whether the person signed during the early signing period. |
| `person_early_enrollee` | logical | Whether the person early-enrolled at college. |
| `person_position_abbreviation` | character | Position abbreviation on the person's record. |
| `person_height` | numeric | Height of the person: a formatted string (e.g. '6-8') or inches, depending on the endpoint. |
| `person_formatted_height` | character | Human-formatted height of the person (e.g. '6-3.5'). |
| `person_weight` | integer | Weight of the person in pounds. |
| `person_class_year` | integer | High-school graduating class year of the person. |
| `person_athlete_verified` | logical | Whether the person's athlete profile is verified by On3. |
| `person_prospect_verified` | logical | Whether the person's prospect measurables are verified by On3. |
| `person_class_rank` | character | Academic class standing of the person (e.g. Senior, RedShirt Senior). |
| `person_recruitment_key` | integer | On3 key of the person's active recruitment record. |
| `person_age` | integer | Age of the person in years, when known. |
| `valuation_nil_status` | character | Status of the NIL valuation (e.g. Normal). |
| `valuation_valuation` | integer | The NIL valuation in US dollars. |
| `valuation_valuation_change` | integer | Change in the NIL valuation since the previous update, in US dollars. |
| `valuation_followers` | integer | Social-media follower count feeding the NIL valuation. |
| `valuation_rank` | integer | Overall rank of the NIL valuation across On3's NIL 100. |
| `valuation_last_updated` | integer | Unix timestamp (seconds) of the last update to the NIL valuation. |
| `valuation_whisper` | character | On3 whisper valuation (reported deal value) behind the NIL valuation, in US dollars. |
| `valuation_whisper_change` | character | Change in the whisper valuation behind the NIL valuation, in US dollars. |
| `valuation_social_valuations` | character | JSON-encoded per-platform social valuations behind the NIL valuation. |
| `valuation_group_rank` | integer | Rank of the NIL valuation within its group (sport or position). |
| `valuation_group_name` | character | Name of the group the NIL valuation is ranked within (usually null). |
| `valuation_tags` | character | JSON-encoded list of NIL tags on the NIL valuation (e.g. Influencer). |
| `valuation_roster_value` | character | Nested roster-value object of the NIL valuation (usually null). |
| `valuation_nil_value` | character | Nested NIL-value object of the NIL valuation (usually null). |
| `person_status_committed_organization_asset` | character | Nested logo asset of the committed-to program (the person's recruiting status; stringified or null). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_nil_rankings-example}

```python
on3_nil_rankings()
```

_Last validated n/a._
