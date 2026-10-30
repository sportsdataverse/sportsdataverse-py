# College football — On3 Recruit Database (api.on3.com) — Nil

> College football — On3 Recruit Database (api.on3.com) — Nil — function reference in sdv-py, the SportsDataverse Python package.

## on3_nil_100

Nil 100.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/nil-100`

**Valid URL:** [https://api.on3.com/public/rdb/v1/nil-100](https://api.on3.com/public/rdb/v1/nil-100)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | year query parameter. |

### Returns {#on3_nil_100-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the row has nested objects whose flattened column names depend on which are null in the data; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_nil_100-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_nil_100-example}

```python
on3_nil_100()
```

## on3_nil_100_v2

Nil 100 v2.

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
| `person_rating_consensus_rating` | Float64 | Industry-consensus numeric rating paired with the person's On3 rating (0-100 scale). |
| `person_rating_consensus_stars` | Int64 | Industry-consensus star rating paired with the person's On3 rating (2-5). |
| `person_rating_consensus_national_rank` | Int64 | Industry-consensus national rank paired with the person's On3 rating. |
| `person_rating_consensus_position_rank` | Int64 | Industry-consensus position rank paired with the person's On3 rating. |
| `person_rating_consensus_state_rank` | Int64 | Industry-consensus state rank paired with the person's On3 rating. |
| `person_rating_key` | Int64 | On3 key of the person's On3 rating record. |
| `person_rating_rating` | Float64 | Numeric value of the person's On3 rating (0-100 scale). |
| `person_rating_stars` | Int64 | Star rating of the person's On3 rating (2-5). |
| `person_rating_national_rank` | Int64 | National rank of the person's On3 rating. |
| `person_rating_position_rank` | Int64 | Position rank of the person's On3 rating. |
| `person_rating_state_rank` | Int64 | State rank of the person's On3 rating. |
| `person_rating_position_abbr` | String | Position abbreviation the person's On3 rating was assigned at. |
| `person_rating_state_abbr` | String | State abbreviation the person's On3 rating was assigned in. |
| `person_rating_five_star_plus` | Boolean | Five-star-plus flag on the person's On3 rating. |
| `person_division` | String | Division of the person's current organization (e.g. NCAA-FB, NCAA-BK). |
| `person_default_sport_key` | Int64 | On3 numeric key of the person's primary sport. |
| `person_default_sport_name` | String | Name of the person's primary sport (e.g. Football). |
| `person_organization_level` | String | Level of the person's current organization (e.g. HighSchool, College, Professional). |
| `person_age` | Float64 | Age of the person in years, when known. |
| `person_tags` | String | JSON-encoded list of On3 profile tags on the person (e.g. Influencer). |
| `person_key` | Int64 | On3 numeric key of the person. |
| `person_recruitment_key` | Int64 | On3 key of the person's active recruitment record. |
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
| `person_high_school_default_asset_key` | Float64 | On3 asset key of the person's high school's logo asset. |
| `person_high_school_default_asset_domain_override` | String | CDN domain override for the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_domain` | String | CDN domain serving the person's high school's logo asset. |
| `person_high_school_default_asset_source_override` | String | Source-path override for the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_source` | String | CDN-relative source path of the person's high school's logo asset. |
| `person_high_school_default_asset_title` | String | Editorial title attached to the person's high school's logo asset. |
| `person_high_school_default_asset_description` | String | Editorial description attached to the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_caption` | String | Editorial caption attached to the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_category` | String | Editorial category label of the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_alt_text` | String | Accessibility alt text of the person's high school's logo asset (usually null). |
| `person_high_school_default_asset_height` | Float64 | Pixel height of the person's high school's logo asset. |
| `person_high_school_default_asset_width` | Float64 | Pixel width of the person's high school's logo asset. |
| `person_high_school_default_asset_asset_type` | String | On3 asset-type discriminator of the person's high school's logo asset (e.g. Image). |
| `person_high_school_default_asset_file_system` | String | Storage file-system flag of the person's high school's logo asset. |
| `person_high_school_default_asset_path` | String | Storage path of the person's high school's logo asset. |
| `person_high_school_default_asset_type` | String | Media type field of the person's high school's logo asset (file extension, e.g. png). |
| `person_high_school_default_asset_thumbnail` | String | Thumbnail variant of the person's high school's logo asset (video assets; usually null). |
| `person_high_school_default_asset_duration` | Float64 | Duration of the person's high school's logo asset when it is a video (usually null or 0). |
| `person_high_school_default_asset_mime_type` | String | MIME type of the person's high school's logo asset. |
| `person_high_school_slug` | String | URL slug of the person's high school on On3. |
| `person_high_school_primary_color` | String | Primary hex color of the person's high school. |
| `person_high_school_org_type` | String | Organization type label of the person's high school (e.g. HighSchool, College). |
| `person_high_school_org_type_enum` | String | Organization type enum of the person's high school (same vocabulary as org_type). |
| `person_high_school_division` | String | Division or classification of the person's high school (e.g. NCAA-FB). |
| `person_high_school_site_keys` | String | JSON-encoded On3 site keys covering the person's high school (usually null). |
| `person_high_school_url_slug` | String | URL slug variant of the person's high school's page, with the key appended. |
| `person_home_town_name` | String | Home town of the person as On3 lists it (e.g. 'Belleville, MI'). |
| `person_early_enrollee` | Boolean | Whether the person early-enrolled at college. |
| `person_early_signee` | Boolean | Whether the person signed during the early signing period. |
| `person_default_asset_url` | String | Convenience CDN URL of the person's headshot. |
| `person_class_year` | Int64 | High-school graduating class year of the person. |
| `person_athlete_verified` | Boolean | Whether the person's athlete profile is verified by On3. |
| `person_prospect_verified` | Boolean | Whether the person's prospect measurables are verified by On3. |
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
| `person_position_abbreviation` | String | Position abbreviation on the person's record. |
| `person_height` | String | Height of the person: a formatted string (e.g. '6-8') or inches, depending on the endpoint. |
| `person_weight` | Int64 | Weight of the person in pounds. |
| `person_roster_rating` | String | On3 roster (transfer-portal-adjusted) rating of the person, when published. |
| `person_commit_status_type` | String | Type of the person's commitment status (e.g. Committed, Signed, Enrolled, None). |
| `person_commit_status_short_term_signee` | Boolean | Short-term-signee flag of the person's commitment status (null when not applicable). |
| `person_commit_status_date` | String | Date the person's commitment status took effect (ISO timestamp string). |
| `person_commit_status_committed_asset` | String | Nested asset of the committed-to program (the person's commitment status; usually null). |
| `person_commit_status_committed_asset_res` | String | Nested logo asset of the committed-to program (the person's commitment status; usually null). |
| `person_commit_status_transferred_asset_key` | Int64 | On3 numeric key of the person's commitment status's program transferred to. |
| `person_commit_status_transferred_asset_url` | String | CDN URL of the person's commitment status's program transferred to's logo. |
| `person_commit_status_transferred_asset_slug` | String | URL slug of the person's commitment status's program transferred to on On3. |
| `person_commit_status_transferred_asset_full_name` | String | Full name of the person's commitment status's program transferred to (e.g. 'Alabama Crimson Tide'). |
| `person_commit_status_transferred_asset_res` | String | Nested logo asset of the program transferred to (the person's commitment status; usually null). |
| `person_commit_status_committed_organization_key` | Int64 | On3 key of the committed-to program (the person's commitment status). |
| `person_commit_status_committed_organization_full_name` | String | Full name of the person's commitment status's committed-to program (e.g. 'Alabama Crimson Tide'). |
| `person_commit_status_committed_organization_name` | String | Short name of the person's commitment status's committed-to program. |
| `person_commit_status_committed_organization_mascot` | String | Mascot of the person's commitment status's committed-to program. |
| `person_commit_status_committed_organization_abbreviation` | String | Abbreviation of the person's commitment status's committed-to program. |
| `person_commit_status_committed_organization_asset_url` | String | CDN URL of the committed-to program's logo (the person's commitment status). |
| `person_commit_status_committed_organization_asset_key` | Int64 | On3 asset key of the person's commitment status's committed-to program's logo asset. |
| `person_commit_status_committed_organization_asset_domain_override` | String | CDN domain override for the person's commitment status's committed-to program's logo asset (usually null). |
| `person_commit_status_committed_organization_asset_domain` | String | CDN domain serving the person's commitment status's committed-to program's logo asset. |
| `person_commit_status_committed_organization_asset_source_override` | String | Source-path override for the person's commitment status's committed-to program's logo asset (usually null). |
| `person_commit_status_committed_organization_asset_source` | String | CDN-relative source path of the person's commitment status's committed-to program's logo asset. |
| `person_commit_status_committed_organization_asset_title` | String | Editorial title attached to the person's commitment status's committed-to program's logo asset. |
| `person_commit_status_committed_organization_asset_description` | String | Editorial description attached to the person's commitment status's committed-to program's logo asset (usually null). |
| `person_commit_status_committed_organization_asset_caption` | String | Editorial caption attached to the person's commitment status's committed-to program's logo asset (usually null). |
| `person_commit_status_committed_organization_asset_category` | String | Editorial category label of the person's commitment status's committed-to program's logo asset (usually null). |
| `person_commit_status_committed_organization_asset_alt_text` | String | Accessibility alt text of the person's commitment status's committed-to program's logo asset (usually null). |
| `person_commit_status_committed_organization_asset_height` | Int64 | Pixel height of the person's commitment status's committed-to program's logo asset. |
| `person_commit_status_committed_organization_asset_width` | Int64 | Pixel width of the person's commitment status's committed-to program's logo asset. |
| `person_commit_status_committed_organization_asset_asset_type` | String | On3 asset-type discriminator of the person's commitment status's committed-to program's logo asset (e.g. Image). |
| `person_commit_status_committed_organization_asset_file_system` | String | Storage file-system flag of the person's commitment status's committed-to program's logo asset. |
| `person_commit_status_committed_organization_asset_path` | String | Storage path of the person's commitment status's committed-to program's logo asset. |
| `person_commit_status_committed_organization_asset_type` | String | Media type field of the person's commitment status's committed-to program's logo asset (file extension, e.g. png). |
| `person_commit_status_committed_organization_asset_thumbnail` | String | Thumbnail variant of the person's commitment status's committed-to program's logo asset (video assets; usually null). |
| `person_commit_status_committed_organization_asset_duration` | Int64 | Duration of the person's commitment status's committed-to program's logo asset when it is a video (usually null or 0). |
| `person_commit_status_committed_organization_asset_mime_type` | String | MIME type of the person's commitment status's committed-to program's logo asset. |
| `person_commit_status_committed_organization_slug` | String | URL slug of the committed-to program (the person's commitment status). |
| `person_commit_status_committed_organization_primary_color` | String | Primary hex color of the person's commitment status's committed-to program. |
| `person_commit_status_class_rank` | String | Academic class standing recorded on the person's commitment status (e.g. Senior). |
| `person_commit_status_transfer_entered` | String | Date the player entered the transfer portal (the person's commitment status; null when never entered). |
| `person_commit_status_recruitment_year` | Int64 | Recruiting-cycle year the person's commitment status belongs to. |
| `person_commit_status_decommitted_asset` | String | Nested asset of the program decommitted from (the person's commitment status; usually null). |
| `person_commit_status_transfer` | Boolean | Transfer flag of the person's commitment status (null when not applicable). |
| `person_commit_status_expected_to_transfer` | Boolean | Expected-to-transfer flag of the person's commitment status (null when not applicable). |
| `person_commit_status_recruitment_key` | Int64 | On3 key of the recruitment record the person's commitment status belongs to. |
| `person_commit_status_withdrawn_transfer` | Boolean | Whether the player withdrew from the transfer portal (the person's commitment status). |
| `person_commit_status_withdrawn_transfer_date` | String | Date the player withdrew from the transfer portal (the person's commitment status; null when never withdrawn). |
| `person_predictions` | String | JSON-encoded On3 RPM (Recruiting Prediction Machine) entries for the person. |
| `person_nil_status` | String | On3 NIL valuation status of the person (e.g. Normal). |
| `person_nil_value` | Float64 | On3 NIL valuation of the person in US dollars. |
| `person_sport` | String | Sport slug the video belongs to. |
| `valuation_nil_status` | String | Status of the NIL valuation (e.g. Normal). |
| `valuation_valuation` | Int64 | The NIL valuation in US dollars. |
| `valuation_valuation_change` | Int64 | Change in the NIL valuation since the previous update, in US dollars. |
| `valuation_followers` | Int64 | Social-media follower count feeding the NIL valuation. |
| `valuation_rank` | Int64 | Overall rank of the NIL valuation across On3's NIL 100. |
| `valuation_last_updated` | Int64 | Unix timestamp (seconds) of the last update to the NIL valuation. |
| `valuation_whisper` | Float64 | On3 whisper valuation (reported deal value) behind the NIL valuation, in US dollars. |
| `valuation_whisper_change` | Float64 | Change in the whisper valuation behind the NIL valuation, in US dollars. |
| `valuation_social_valuations` | String | JSON-encoded per-platform social valuations behind the NIL valuation. |
| `valuation_group_rank` | Int64 | Rank of the NIL valuation within its group (sport or position). |
| `valuation_group_name` | String | Name of the group the NIL valuation is ranked within (usually null). |
| `valuation_tags` | String | JSON-encoded list of NIL tags on the NIL valuation (e.g. Influencer). |
| `valuation_roster_value` | String | Nested roster-value object of the NIL valuation (usually null). |
| `valuation_nil_value` | String | Nested NIL-value object of the NIL valuation (usually null). |
| `person_high_school_default_asset` | String | Nested default-asset object of the person's high school (null when On3 ships none). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_nil_100_v2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_nil_100_v2-example}

```python
on3_nil_100_v2()
```

## on3_nil_compliances_state

Nil compliances state.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/nil-compliances/state`

**Valid URL:** [https://api.on3.com/public/rdb/v1/nil-compliances/state](https://api.on3.com/public/rdb/v1/nil-compliances/state)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `stateKey` | `state_key` |  |  | `Y` | stateKey query parameter. |

### Returns {#on3_nil_compliances_state-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_nil_compliances_state-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_nil_compliances_state-example}

```python
on3_nil_compliances_state()
```

## on3_nil_rankings

Nil rankings.

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
| `person_default_sport_key` | Int64 | On3 numeric key of the person's primary sport. |
| `person_default_sport_name` | String | Name of the person's primary sport (e.g. Football). |
| `person_default_sport_slug` | String | URL slug of the person's primary sport. |
| `person_default_sport_abbreviation` | String | Abbreviation of the person's primary sport. |
| `person_default_sport_is_rankable` | Boolean | Whether On3 ranks players in the person's primary sport. |
| `person_default_sport_is_industry_rankable` | Boolean | Whether industry-consensus rankings exist for the person's primary sport. |
| `person_default_sport_is_scoutable` | Boolean | Whether On3 scouting reports exist for the person's primary sport. |
| `person_rating_consensus_rating` | Float64 | Industry-consensus numeric rating paired with the person's On3 rating (0-100 scale). |
| `person_rating_consensus_stars` | Int64 | Industry-consensus star rating paired with the person's On3 rating (2-5). |
| `person_rating_consensus_national_rank` | Int64 | Industry-consensus national rank paired with the person's On3 rating. |
| `person_rating_consensus_position_rank` | Int64 | Industry-consensus position rank paired with the person's On3 rating. |
| `person_rating_consensus_state_rank` | Int64 | Industry-consensus state rank paired with the person's On3 rating. |
| `person_rating_key` | Int64 | On3 key of the person's On3 rating record. |
| `person_rating_rating` | Float64 | Numeric value of the person's On3 rating (0-100 scale). |
| `person_rating_stars` | Int64 | Star rating of the person's On3 rating (2-5). |
| `person_rating_national_rank` | Int64 | National rank of the person's On3 rating. |
| `person_rating_position_rank` | Int64 | Position rank of the person's On3 rating. |
| `person_rating_state_rank` | Int64 | State rank of the person's On3 rating. |
| `person_rating_position_abbr` | String | Position abbreviation the person's On3 rating was assigned at. |
| `person_rating_state_abbr` | String | State abbreviation the person's On3 rating was assigned in. |
| `person_rating_five_star_plus` | Boolean | Five-star-plus flag on the person's On3 rating. |
| `person_status_is_committed` | Boolean | Whether the recruit is committed (the person's recruiting status). |
| `person_status_is_signed` | Boolean | Whether the recruit has signed (the person's recruiting status). |
| `person_status_is_transfer` | Boolean | Whether the recruit is a transfer (the person's recruiting status). |
| `person_status_is_enrolled` | Boolean | Whether the recruit is enrolled (the person's recruiting status). |
| `person_status_commitment_date` | String | Date of the commitment (the person's recruiting status; ISO timestamp string). |
| `person_status_committed_organization_key` | Float64 | On3 key of the committed-to program (the person's recruiting status). |
| `person_status_committed_organization_slug` | String | URL slug of the committed-to program (the person's recruiting status). |
| `person_status_committed_organization_asset_url` | String | CDN URL of the committed-to program's logo (the person's recruiting status). |
| `person_status_committed_organization_asset_key` | Float64 | On3 asset key of the person's recruiting status's committed-to program's logo asset. |
| `person_status_committed_organization_asset_domain_override` | String | CDN domain override for the person's recruiting status's committed-to program's logo asset (usually null). |
| `person_status_committed_organization_asset_domain` | String | CDN domain serving the person's recruiting status's committed-to program's logo asset. |
| `person_status_committed_organization_asset_source_override` | String | Source-path override for the person's recruiting status's committed-to program's logo asset (usually null). |
| `person_status_committed_organization_asset_source` | String | CDN-relative source path of the person's recruiting status's committed-to program's logo asset. |
| `person_status_committed_organization_asset_title` | String | Editorial title attached to the person's recruiting status's committed-to program's logo asset. |
| `person_status_committed_organization_asset_description` | String | Editorial description attached to the person's recruiting status's committed-to program's logo asset (usually null). |
| `person_status_committed_organization_asset_caption` | String | Editorial caption attached to the person's recruiting status's committed-to program's logo asset (usually null). |
| `person_status_committed_organization_asset_category` | String | Editorial category label of the person's recruiting status's committed-to program's logo asset (usually null). |
| `person_status_committed_organization_asset_alt_text` | String | Accessibility alt text of the person's recruiting status's committed-to program's logo asset (usually null). |
| `person_status_committed_organization_asset_height` | Float64 | Pixel height of the person's recruiting status's committed-to program's logo asset. |
| `person_status_committed_organization_asset_width` | Float64 | Pixel width of the person's recruiting status's committed-to program's logo asset. |
| `person_status_committed_organization_asset_asset_type` | String | On3 asset-type discriminator of the person's recruiting status's committed-to program's logo asset (e.g. Image). |
| `person_status_committed_organization_asset_file_system` | String | Storage file-system flag of the person's recruiting status's committed-to program's logo asset. |
| `person_status_committed_organization_asset_path` | String | Storage path of the person's recruiting status's committed-to program's logo asset. |
| `person_status_committed_organization_asset_type` | String | Media type field of the person's recruiting status's committed-to program's logo asset (file extension, e.g. png). |
| `person_status_committed_organization_asset_thumbnail` | String | Thumbnail variant of the person's recruiting status's committed-to program's logo asset (video assets; usually null). |
| `person_status_committed_organization_asset_duration` | Float64 | Duration of the person's recruiting status's committed-to program's logo asset when it is a video (usually null or 0). |
| `person_status_committed_organization_asset_mime_type` | String | MIME type of the person's recruiting status's committed-to program's logo asset. |
| `person_status_committed_organization_primary_color` | String | Primary hex color of the person's recruiting status's committed-to program. |
| `person_status_transferred_from_organization_asset_url` | String | Convenience CDN URL of the person's recruiting status's program transferred from's logo. |
| `person_status_transferred_from_organization_slug` | String | URL slug of the person's recruiting status's program transferred from on On3. |
| `person_status_highest_interest_level` | Float64 | Highest interest level a program has logged for the recruit (the person's recruiting status). |
| `person_status_interest_count` | Int64 | Number of programs with logged interest in the recruit (the person's recruiting status). |
| `person_status_recruitment_year` | Int64 | Recruiting-cycle year the person's recruiting status belongs to. |
| `person_status_sport_name` | String | Sport the person's recruiting status applies to. |
| `person_status_short_term_signee` | Boolean | Short-term-signee flag of the person's recruiting status (null when not applicable). |
| `person_predictions` | String | JSON-encoded On3 RPM (Recruiting Prediction Machine) entries for the person. |
| `person_tags` | String | JSON-encoded list of On3 profile tags on the person (e.g. Influencer). |
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
| `valuation_nil_status` | String | Status of the NIL valuation (e.g. Normal). |
| `valuation_valuation` | Int64 | The NIL valuation in US dollars. |
| `valuation_valuation_change` | Int64 | Change in the NIL valuation since the previous update, in US dollars. |
| `valuation_followers` | Int64 | Social-media follower count feeding the NIL valuation. |
| `valuation_rank` | Int64 | Overall rank of the NIL valuation across On3's NIL 100. |
| `valuation_last_updated` | Int64 | Unix timestamp (seconds) of the last update to the NIL valuation. |
| `valuation_whisper` | String | On3 whisper valuation (reported deal value) behind the NIL valuation, in US dollars. |
| `valuation_whisper_change` | String | Change in the whisper valuation behind the NIL valuation, in US dollars. |
| `valuation_social_valuations` | String | JSON-encoded per-platform social valuations behind the NIL valuation. |
| `valuation_group_rank` | Int64 | Rank of the NIL valuation within its group (sport or position). |
| `valuation_group_name` | String | Name of the group the NIL valuation is ranked within (usually null). |
| `valuation_tags` | String | JSON-encoded list of NIL tags on the NIL valuation (e.g. Influencer). |
| `valuation_roster_value` | String | Nested roster-value object of the NIL valuation (usually null). |
| `valuation_nil_value` | String | Nested NIL-value object of the NIL valuation (usually null). |
| `person_status_committed_organization_asset` | String | Nested logo asset of the committed-to program (the person's recruiting status; stringified or null). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_nil_rankings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_nil_rankings-example}

```python
on3_nil_rankings()
```
