# CFB — On3 Recruit Database (api.on3.com) — Recruitment

> CFB — On3 Recruit Database (api.on3.com) — Recruitment — function reference in sdv-py, the SportsDataverse Python package.

## on3_recruitment_primary_recruitment_evaluation

GET /rdb/v1/recruitment/{recruitmentKey}/primary-recruitment-evaluation

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/recruitment/{recruitment_key}/primary-recruitment-evaluation`

**Valid URL:** [https://api.on3.com/public/rdb/v1/recruitment/270036/primary-recruitment-evaluation](https://api.on3.com/public/rdb/v1/recruitment/270036/primary-recruitment-evaluation)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `recruitment_key` | `recruitment_key` |  | `Y` |  | recruitment_key path parameter. |

### Returns {#on3_recruitment_primary_recruitment_evaluation-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_recruitment_primary_recruitment_evaluation-example}

```python
on3_recruitment_primary_recruitment_evaluation(recruitment_key=270036)
```

_Last validated n/a._

## on3_recruitment_recruitment_evaluations

GET /rdb/v1/recruitment/{recruitmentKey}/recruitment-evaluations

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/recruitment/{recruitment_key}/recruitment-evaluations`

**Valid URL:** [https://api.on3.com/public/rdb/v1/recruitment/270036/recruitment-evaluations](https://api.on3.com/public/rdb/v1/recruitment/270036/recruitment-evaluations)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `recruitment_key` | `recruitment_key` |  | `Y` |  | recruitment_key path parameter. |

### Returns {#on3_recruitment_recruitment_evaluations-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: its committed capture has 0 rows, so the parser emits no columns; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_recruitment_recruitment_evaluations-example}

```python
on3_recruitment_recruitment_evaluations(recruitment_key=270036)
```

_Last validated n/a._

## on3_recruitments_latest_rpm_picks

Latest RPM (prediction) picks feed — paged {list,pagination}

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/recruitments/latest-rpm-picks`

**Valid URL:** [https://api.on3.com/public/rdb/v1/recruitments/latest-rpm-picks](https://api.on3.com/public/rdb/v1/recruitments/latest-rpm-picks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `orgKey` | `org_key` |  |  | `Y` | orgKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_recruitments_latest_rpm_picks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `pick_key` | integer | On3 key of the RPM pick. |
| `pick_organization` | character | Nested program object the RPM pick predicts (stringified; usually null). |
| `pick_year` | integer | Recruiting class year the RPM pick applies to (0 when unset). |
| `pick_date_added` | character | Timestamp the RPM pick was logged (ISO string). |
| `pick_expert` | character | Nested expert object of the RPM pick (stringified; usually null). |
| `pick_confidence` | numeric | Confidence the expert attached to the RPM pick, in percent. |
| `pick_article_link` | character | On3 article link accompanying the RPM pick, when any. |
| `pick_premium` | logical | Whether the RPM pick is behind the On3+ paywall. |
| `pick_correct` | character | Whether the RPM pick proved correct ('True'/'False' string; null while open). |
| `pick_days_correct` | numeric | Days the RPM pick stood correct. |
| `pick_flipped_from_organization` | character | Program the recruit flipped from when the RPM pick was logged, when any. |
| `pick_previous_confidence` | character | Confidence of the expert's previous pick on the recruit, when any. |
| `pick_previous_date_added` | character | Timestamp of the expert's previous pick on the recruit (a 0001-01-01 sentinel when none). |
| `pick_type` | character | Type of the RPM pick (e.g. NewLeader, Flip). |
| `pick_expert_accuracy` | numeric | Lifetime pick accuracy of the pick's expert, in percent. |
| `pick_top_teams` | character | JSON-encoded list of the recruit's top teams at the time of the RPM pick. |
| `player_key` | integer | On3 numeric key of the player. |
| `player_recruitment_key` | integer | On3 key of the player's active recruitment record. |
| `player_name` | character | Display name of the player. |
| `player_slug` | character | URL slug of the player's On3 profile. |
| `player_high_school_name` | character | High-school display name on the player's record. |
| `player_high_school` | character | High-school display name on the player's record. |
| `player_home_town_name` | character | Home town of the player as On3 lists it (e.g. 'Belleville, MI'). |
| `player_early_enrollee` | logical | Whether the player early-enrolled at college. |
| `player_early_signee` | logical | Whether the player signed during the early signing period. |
| `player_default_asset_url` | character | Convenience CDN URL of the player's headshot. |
| `player_class_year` | character | High-school graduating class year of the player. |
| `player_athlete_verified` | logical | Whether the player's athlete profile is verified by On3. |
| `player_prospect_verified` | logical | Whether the player's prospect measurables are verified by On3. |
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
| `player_position_abbreviation` | character | Position abbreviation on the player's record. |
| `player_height` | character | Height of the player: a formatted string (e.g. '6-8') or inches, depending on the endpoint. |
| `player_weight` | integer | Weight of the player in pounds. |
| `player_rating` | character | On3 rating of the player (0-100 scale), when published. |
| `player_roster_rating` | character | On3 roster (transfer-portal-adjusted) rating of the player, when published. |
| `player_commit_status_type` | character | Type of the player's commitment status (e.g. Committed, Signed, Enrolled, None). |
| `player_commit_status_short_term_signee` | logical | Short-term-signee flag of the player's commitment status (null when not applicable). |
| `player_commit_status_date` | character | Date the player's commitment status took effect (ISO timestamp string). |
| `player_commit_status_committed_asset_key` | integer | On3 numeric key of the player's commitment status's committed-to program. |
| `player_commit_status_committed_asset_url` | character | CDN URL of the player's commitment status's committed-to program's logo. |
| `player_commit_status_committed_asset_slug` | character | URL slug of the player's commitment status's committed-to program on On3. |
| `player_commit_status_committed_asset_full_name` | character | Full name of the player's commitment status's committed-to program (e.g. 'Alabama Crimson Tide'). |
| `player_commit_status_committed_asset_res_key` | integer | On3 asset key of the player's commitment status's committed-to program's logo asset. |
| `player_commit_status_committed_asset_res_domain_override` | character | CDN domain override for the player's commitment status's committed-to program's logo asset (usually null). |
| `player_commit_status_committed_asset_res_domain` | character | CDN domain serving the player's commitment status's committed-to program's logo asset. |
| `player_commit_status_committed_asset_res_source_override` | character | Source-path override for the player's commitment status's committed-to program's logo asset (usually null). |
| `player_commit_status_committed_asset_res_source` | character | CDN-relative source path of the player's commitment status's committed-to program's logo asset. |
| `player_commit_status_committed_asset_res_title` | character | Editorial title attached to the player's commitment status's committed-to program's logo asset. |
| `player_commit_status_committed_asset_res_description` | character | Editorial description attached to the player's commitment status's committed-to program's logo asset (usually null). |
| `player_commit_status_committed_asset_res_caption` | character | Editorial caption attached to the player's commitment status's committed-to program's logo asset (usually null). |
| `player_commit_status_committed_asset_res_category` | character | Editorial category label of the player's commitment status's committed-to program's logo asset (usually null). |
| `player_commit_status_committed_asset_res_alt_text` | character | Accessibility alt text of the player's commitment status's committed-to program's logo asset (usually null). |
| `player_commit_status_committed_asset_res_height` | integer | Pixel height of the player's commitment status's committed-to program's logo asset. |
| `player_commit_status_committed_asset_res_width` | integer | Pixel width of the player's commitment status's committed-to program's logo asset. |
| `player_commit_status_committed_asset_res_asset_type` | character | On3 asset-type discriminator of the player's commitment status's committed-to program's logo asset (e.g. Image). |
| `player_commit_status_committed_asset_res_file_system` | character | Storage file-system flag of the player's commitment status's committed-to program's logo asset. |
| `player_commit_status_committed_asset_res_path` | character | Storage path of the player's commitment status's committed-to program's logo asset. |
| `player_commit_status_committed_asset_res_type` | character | Media type field of the player's commitment status's committed-to program's logo asset (file extension, e.g. png). |
| `player_commit_status_committed_asset_res_thumbnail` | character | Thumbnail variant of the player's commitment status's committed-to program's logo asset (video assets; usually null). |
| `player_commit_status_committed_asset_res_duration` | integer | Duration of the player's commitment status's committed-to program's logo asset when it is a video (usually null or 0). |
| `player_commit_status_committed_asset_res_mime_type` | character | MIME type of the player's commitment status's committed-to program's logo asset. |
| `player_commit_status_transferred_asset_key` | integer | On3 numeric key of the player's commitment status's program transferred to. |
| `player_commit_status_transferred_asset_url` | character | CDN URL of the player's commitment status's program transferred to's logo. |
| `player_commit_status_transferred_asset_slug` | character | URL slug of the player's commitment status's program transferred to on On3. |
| `player_commit_status_transferred_asset_full_name` | character | Full name of the player's commitment status's program transferred to (e.g. 'Alabama Crimson Tide'). |
| `player_commit_status_transferred_asset_res_key` | integer | On3 asset key of the player's commitment status's program transferred to's logo asset. |
| `player_commit_status_transferred_asset_res_domain_override` | character | CDN domain override for the player's commitment status's program transferred to's logo asset (usually null). |
| `player_commit_status_transferred_asset_res_domain` | character | CDN domain serving the player's commitment status's program transferred to's logo asset. |
| `player_commit_status_transferred_asset_res_source_override` | character | Source-path override for the player's commitment status's program transferred to's logo asset (usually null). |
| `player_commit_status_transferred_asset_res_source` | character | CDN-relative source path of the player's commitment status's program transferred to's logo asset. |
| `player_commit_status_transferred_asset_res_title` | character | Editorial title attached to the player's commitment status's program transferred to's logo asset. |
| `player_commit_status_transferred_asset_res_description` | character | Editorial description attached to the player's commitment status's program transferred to's logo asset (usually null). |
| `player_commit_status_transferred_asset_res_caption` | character | Editorial caption attached to the player's commitment status's program transferred to's logo asset (usually null). |
| `player_commit_status_transferred_asset_res_category` | character | Editorial category label of the player's commitment status's program transferred to's logo asset (usually null). |
| `player_commit_status_transferred_asset_res_alt_text` | character | Accessibility alt text of the player's commitment status's program transferred to's logo asset (usually null). |
| `player_commit_status_transferred_asset_res_height` | integer | Pixel height of the player's commitment status's program transferred to's logo asset. |
| `player_commit_status_transferred_asset_res_width` | integer | Pixel width of the player's commitment status's program transferred to's logo asset. |
| `player_commit_status_transferred_asset_res_asset_type` | character | On3 asset-type discriminator of the player's commitment status's program transferred to's logo asset (e.g. Image). |
| `player_commit_status_transferred_asset_res_file_system` | character | Storage file-system flag of the player's commitment status's program transferred to's logo asset. |
| `player_commit_status_transferred_asset_res_path` | character | Storage path of the player's commitment status's program transferred to's logo asset. |
| `player_commit_status_transferred_asset_res_type` | character | Media type field of the player's commitment status's program transferred to's logo asset (file extension, e.g. png). |
| `player_commit_status_transferred_asset_res_thumbnail` | character | Thumbnail variant of the player's commitment status's program transferred to's logo asset (video assets; usually null). |
| `player_commit_status_transferred_asset_res_duration` | integer | Duration of the player's commitment status's program transferred to's logo asset when it is a video (usually null or 0). |
| `player_commit_status_transferred_asset_res_mime_type` | character | MIME type of the player's commitment status's program transferred to's logo asset. |
| `player_commit_status_committed_organization_key` | integer | On3 key of the committed-to program (the player's commitment status). |
| `player_commit_status_committed_organization_full_name` | character | Full name of the player's commitment status's committed-to program (e.g. 'Alabama Crimson Tide'). |
| `player_commit_status_committed_organization_name` | character | Short name of the player's commitment status's committed-to program. |
| `player_commit_status_committed_organization_mascot` | character | Mascot of the player's commitment status's committed-to program. |
| `player_commit_status_committed_organization_abbreviation` | character | Abbreviation of the player's commitment status's committed-to program. |
| `player_commit_status_committed_organization_asset_url` | character | CDN URL of the committed-to program's logo (the player's commitment status). |
| `player_commit_status_committed_organization_asset_key` | integer | On3 asset key of the player's commitment status's committed-to program's logo asset. |
| `player_commit_status_committed_organization_asset_domain_override` | character | CDN domain override for the player's commitment status's committed-to program's logo asset (usually null). |
| `player_commit_status_committed_organization_asset_domain` | character | CDN domain serving the player's commitment status's committed-to program's logo asset. |
| `player_commit_status_committed_organization_asset_source_override` | character | Source-path override for the player's commitment status's committed-to program's logo asset (usually null). |
| `player_commit_status_committed_organization_asset_source` | character | CDN-relative source path of the player's commitment status's committed-to program's logo asset. |
| `player_commit_status_committed_organization_asset_title` | character | Editorial title attached to the player's commitment status's committed-to program's logo asset. |
| `player_commit_status_committed_organization_asset_description` | character | Editorial description attached to the player's commitment status's committed-to program's logo asset (usually null). |
| `player_commit_status_committed_organization_asset_caption` | character | Editorial caption attached to the player's commitment status's committed-to program's logo asset (usually null). |
| `player_commit_status_committed_organization_asset_category` | character | Editorial category label of the player's commitment status's committed-to program's logo asset (usually null). |
| `player_commit_status_committed_organization_asset_alt_text` | character | Accessibility alt text of the player's commitment status's committed-to program's logo asset (usually null). |
| `player_commit_status_committed_organization_asset_height` | integer | Pixel height of the player's commitment status's committed-to program's logo asset. |
| `player_commit_status_committed_organization_asset_width` | integer | Pixel width of the player's commitment status's committed-to program's logo asset. |
| `player_commit_status_committed_organization_asset_asset_type` | character | On3 asset-type discriminator of the player's commitment status's committed-to program's logo asset (e.g. Image). |
| `player_commit_status_committed_organization_asset_file_system` | character | Storage file-system flag of the player's commitment status's committed-to program's logo asset. |
| `player_commit_status_committed_organization_asset_path` | character | Storage path of the player's commitment status's committed-to program's logo asset. |
| `player_commit_status_committed_organization_asset_type` | character | Media type field of the player's commitment status's committed-to program's logo asset (file extension, e.g. png). |
| `player_commit_status_committed_organization_asset_thumbnail` | character | Thumbnail variant of the player's commitment status's committed-to program's logo asset (video assets; usually null). |
| `player_commit_status_committed_organization_asset_duration` | integer | Duration of the player's commitment status's committed-to program's logo asset when it is a video (usually null or 0). |
| `player_commit_status_committed_organization_asset_mime_type` | character | MIME type of the player's commitment status's committed-to program's logo asset. |
| `player_commit_status_committed_organization_slug` | character | URL slug of the committed-to program (the player's commitment status). |
| `player_commit_status_committed_organization_primary_color` | character | Primary hex color of the player's commitment status's committed-to program. |
| `player_commit_status_class_rank` | character | Academic class standing recorded on the player's commitment status (e.g. Senior). |
| `player_commit_status_transfer_entered` | character | Date the player entered the transfer portal (the player's commitment status; null when never entered). |
| `player_commit_status_recruitment_year` | character | Recruiting-cycle year the player's commitment status belongs to. |
| `player_commit_status_decommitted_asset` | character | Nested asset of the program decommitted from (the player's commitment status; usually null). |
| `player_commit_status_transfer` | logical | Transfer flag of the player's commitment status (null when not applicable). |
| `player_commit_status_expected_to_transfer` | logical | Expected-to-transfer flag of the player's commitment status (null when not applicable). |
| `player_commit_status_recruitment_key` | integer | On3 key of the recruitment record the player's commitment status belongs to. |
| `player_commit_status_withdrawn_transfer` | logical | Whether the player withdrew from the transfer portal (the player's commitment status). |
| `player_commit_status_withdrawn_transfer_date` | character | Date the player withdrew from the transfer portal (the player's commitment status; null when never withdrawn). |
| `player_predictions` | character | JSON-encoded On3 RPM (Recruiting Prediction Machine) entries for the player. |
| `player_nil_status` | character | On3 NIL valuation status of the player (e.g. Normal). |
| `player_nil_value` | integer | On3 NIL valuation of the player in US dollars. |
| `player_sport` | character | Nested sport object of the player (null when On3 ships none). |
| `pick_organization_asset_url_key` | numeric | On3 asset key behind the RPM pick's program's logo link. |
| `pick_organization_asset_url_url` | character | Full CDN URL of the RPM pick's program's logo link. |
| `pick_organization_asset_url_slug` | character | URL slug of the program the RPM pick's program's logo link belongs to. |
| `pick_organization_asset_url_full_name` | character | Full name of the program the RPM pick's program's logo link belongs to. |
| `pick_organization_full_name` | character | Full name of the RPM pick's program (e.g. 'Alabama Crimson Tide'). |
| `pick_organization_key` | numeric | On3 numeric key of the RPM pick's program. |
| `pick_organization_name` | character | Short name of the RPM pick's program. |
| `pick_organization_mascot` | character | Mascot of the RPM pick's program. |
| `pick_organization_abbreviation` | character | Abbreviation of the RPM pick's program. |
| `pick_organization_asset_key` | numeric | On3 asset key of the RPM pick's program's logo asset. |
| `pick_organization_asset_domain_override` | character | CDN domain override for the RPM pick's program's logo asset (usually null). |
| `pick_organization_asset_domain` | character | CDN domain serving the RPM pick's program's logo asset. |
| `pick_organization_asset_source_override` | character | Source-path override for the RPM pick's program's logo asset (usually null). |
| `pick_organization_asset_source` | character | CDN-relative source path of the RPM pick's program's logo asset. |
| `pick_organization_asset_title` | character | Editorial title attached to the RPM pick's program's logo asset. |
| `pick_organization_asset_description` | character | Editorial description attached to the RPM pick's program's logo asset (usually null). |
| `pick_organization_asset_caption` | character | Editorial caption attached to the RPM pick's program's logo asset (usually null). |
| `pick_organization_asset_category` | character | Editorial category label of the RPM pick's program's logo asset (usually null). |
| `pick_organization_asset_alt_text` | character | Accessibility alt text of the RPM pick's program's logo asset (usually null). |
| `pick_organization_asset_height` | numeric | Pixel height of the RPM pick's program's logo asset. |
| `pick_organization_asset_width` | numeric | Pixel width of the RPM pick's program's logo asset. |
| `pick_organization_asset_asset_type` | character | On3 asset-type discriminator of the RPM pick's program's logo asset (e.g. Image). |
| `pick_organization_asset_file_system` | character | Storage file-system flag of the RPM pick's program's logo asset. |
| `pick_organization_asset_path` | character | Storage path of the RPM pick's program's logo asset. |
| `pick_organization_asset_type` | character | Media type field of the RPM pick's program's logo asset (file extension, e.g. png). |
| `pick_organization_asset_thumbnail` | character | Thumbnail variant of the RPM pick's program's logo asset (video assets; usually null). |
| `pick_organization_asset_duration` | numeric | Duration of the RPM pick's program's logo asset when it is a video (usually null or 0). |
| `pick_organization_asset_mime_type` | character | MIME type of the RPM pick's program's logo asset. |
| `pick_organization_slug` | character | URL slug of the RPM pick's program on On3. |
| `pick_organization_primary_color` | character | Primary hex color of the RPM pick's program. |
| `pick_expert_key` | numeric | On3 key of the pick's expert. |
| `pick_expert_name` | character | Display name of the pick's expert. |
| `pick_expert_nice_name` | character | URL slug of the pick's expert. |
| `pick_expert_twitter_handle` | character | Twitter/X handle of the pick's expert, when listed. |
| `pick_expert_instagram_handle` | character | Instagram handle of the pick's expert, when listed. |
| `pick_expert_youtube_url` | character | YouTube URL of the pick's expert, when listed. |
| `pick_expert_bio` | character | Biography text of the pick's expert, when listed. |
| `pick_expert_job_title` | character | Job title of the pick's expert, when listed. |
| `pick_expert_site_affiliation` | character | On3 site the pick's expert writes for, when listed. |
| `pick_expert_profile_picture` | character | Profile picture of the pick's expert (nested; usually null). |
| `pick_expert_profile_picture_response_key` | numeric | On3 asset key of the expert's profile-picture asset. |
| `pick_expert_profile_picture_response_domain_override` | character | CDN domain override for the expert's profile-picture asset (usually null). |
| `pick_expert_profile_picture_response_domain` | character | CDN domain serving the expert's profile-picture asset. |
| `pick_expert_profile_picture_response_source_override` | character | Source-path override for the expert's profile-picture asset (usually null). |
| `pick_expert_profile_picture_response_source` | character | CDN-relative source path of the expert's profile-picture asset. |
| `pick_expert_profile_picture_response_title` | character | Editorial title attached to the expert's profile-picture asset. |
| `pick_expert_profile_picture_response_description` | character | Editorial description attached to the expert's profile-picture asset (usually null). |
| `pick_expert_profile_picture_response_caption` | character | Editorial caption attached to the expert's profile-picture asset (usually null). |
| `pick_expert_profile_picture_response_category` | character | Editorial category label of the expert's profile-picture asset (usually null). |
| `pick_expert_profile_picture_response_alt_text` | character | Accessibility alt text of the expert's profile-picture asset (usually null). |
| `pick_expert_profile_picture_response_height` | character | Pixel height of the expert's profile-picture asset. |
| `pick_expert_profile_picture_response_width` | character | Pixel width of the expert's profile-picture asset. |
| `pick_expert_profile_picture_response_asset_type` | character | On3 asset-type discriminator of the expert's profile-picture asset (e.g. Image). |
| `pick_expert_profile_picture_response_file_system` | character | Storage file-system flag of the expert's profile-picture asset. |
| `pick_expert_profile_picture_response_path` | character | Storage path of the expert's profile-picture asset. |
| `pick_expert_profile_picture_response_type` | character | Media type field of the expert's profile-picture asset (file extension, e.g. png). |
| `pick_expert_profile_picture_response_thumbnail` | character | Thumbnail variant of the expert's profile-picture asset (video assets; usually null). |
| `pick_expert_profile_picture_response_duration` | numeric | Duration of the expert's profile-picture asset when it is a video (usually null or 0). |
| `pick_expert_profile_picture_response_mime_type` | character | MIME type of the expert's profile-picture asset. |
| `player_high_school_key` | numeric | On3 numeric key of the player's high school. |
| `player_high_school_full_name` | character | Full name of the player's high school (e.g. 'Alabama Crimson Tide'). |
| `player_high_school_name_2` | character | High-school display name on the player's record (json_normalize de-duplication suffix). |
| `player_high_school_known_as` | character | Common short name of the player's high school, when On3 lists one. |
| `player_high_school_mascot` | character | Mascot of the player's high school. |
| `player_high_school_abbreviation` | character | Abbreviation of the player's high school. |
| `player_high_school_asset_url` | character | Convenience CDN URL of the player's high school's logo. |
| `player_high_school_default_asset_key` | numeric | On3 asset key of the player's high school's logo asset. |
| `player_high_school_default_asset_domain_override` | character | CDN domain override for the player's high school's logo asset (usually null). |
| `player_high_school_default_asset_domain` | character | CDN domain serving the player's high school's logo asset. |
| `player_high_school_default_asset_source_override` | character | Source-path override for the player's high school's logo asset (usually null). |
| `player_high_school_default_asset_source` | character | CDN-relative source path of the player's high school's logo asset. |
| `player_high_school_default_asset_title` | character | Editorial title attached to the player's high school's logo asset. |
| `player_high_school_default_asset_description` | character | Editorial description attached to the player's high school's logo asset (usually null). |
| `player_high_school_default_asset_caption` | character | Editorial caption attached to the player's high school's logo asset (usually null). |
| `player_high_school_default_asset_category` | character | Editorial category label of the player's high school's logo asset (usually null). |
| `player_high_school_default_asset_alt_text` | character | Accessibility alt text of the player's high school's logo asset (usually null). |
| `player_high_school_default_asset_height` | numeric | Pixel height of the player's high school's logo asset. |
| `player_high_school_default_asset_width` | numeric | Pixel width of the player's high school's logo asset. |
| `player_high_school_default_asset_asset_type` | character | On3 asset-type discriminator of the player's high school's logo asset (e.g. Image). |
| `player_high_school_default_asset_file_system` | character | Storage file-system flag of the player's high school's logo asset. |
| `player_high_school_default_asset_path` | character | Storage path of the player's high school's logo asset. |
| `player_high_school_default_asset_type` | character | Media type field of the player's high school's logo asset (file extension, e.g. png). |
| `player_high_school_default_asset_thumbnail` | character | Thumbnail variant of the player's high school's logo asset (video assets; usually null). |
| `player_high_school_default_asset_duration` | numeric | Duration of the player's high school's logo asset when it is a video (usually null or 0). |
| `player_high_school_default_asset_mime_type` | character | MIME type of the player's high school's logo asset. |
| `player_high_school_slug` | character | URL slug of the player's high school on On3. |
| `player_high_school_primary_color` | character | Primary hex color of the player's high school. |
| `player_high_school_org_type` | character | Organization type label of the player's high school (e.g. HighSchool, College). |
| `player_high_school_org_type_enum` | character | Organization type enum of the player's high school (same vocabulary as org_type). |
| `player_high_school_division` | character | Division or classification of the player's high school (e.g. NCAA-FB). |
| `player_high_school_site_keys` | character | JSON-encoded On3 site keys covering the player's high school (usually null). |
| `player_high_school_url_slug` | character | URL slug variant of the player's high school's page, with the key appended. |
| `player_rating_key` | numeric | On3 key of the player's On3 rating record. |
| `player_rating_rating` | numeric | Numeric value of the player's On3 rating (0-100 scale). |
| `player_rating_stars` | numeric | Star rating of the player's On3 rating (2-5). |
| `player_rating_national_rank` | numeric | National rank of the player's On3 rating. |
| `player_rating_position_rank` | numeric | Position rank of the player's On3 rating. |
| `player_rating_state_rank` | numeric | State rank of the player's On3 rating. |
| `player_rating_position_abbr` | character | Position abbreviation the player's On3 rating was assigned at. |
| `player_rating_state_abbr` | character | State abbreviation the player's On3 rating was assigned in. |
| `player_rating_five_star_plus` | character | Five-star-plus flag on the player's On3 rating. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_recruitments_latest_rpm_picks-example}

```python
on3_recruitments_latest_rpm_picks()
```

_Last validated n/a._

## on3_recruitments_profile

GET /rdb/v1/recruitments/{recKey}/profile

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/recruitments/{rec_key}/profile`

**Valid URL:** [https://api.on3.com/public/rdb/v1/recruitments/270036/profile](https://api.on3.com/public/rdb/v1/recruitments/270036/profile)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `rec_key` | `rec_key` |  | `Y` |  | rec_key path parameter. |

### Returns {#on3_recruitments_profile-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `class_year` | integer | Recruiting class year of the recruitment. |
| `high_school` | character | High-school display name on the record. |
| `home_town` | character | Home town as On3 lists it (e.g. 'Pewaukee, WI'). |
| `rating_key` | integer | On3 key of the On3 rating record. |
| `rating_rating` | numeric | Numeric value of the On3 rating (0-100 scale). |
| `rating_stars` | integer | Star rating of the On3 rating (2-5). |
| `rating_national_rank` | integer | National rank of the On3 rating. |
| `rating_position_rank` | integer | Position rank of the On3 rating. |
| `rating_state_rank` | integer | State rank of the On3 rating. |
| `rating_position_abbr` | character | Position abbreviation the On3 rating was assigned at. |
| `rating_state_abbr` | character | State abbreviation the On3 rating was assigned in. |
| `rating_five_star_plus` | logical | Five-star-plus flag on the On3 rating. |
| `committed_status_type` | character | Type of the commitment status (e.g. Committed, Signed, Enrolled, None). |
| `committed_status_short_term_signee` | logical | Short-term-signee flag of the commitment status (null when not applicable). |
| `committed_status_date` | character | Date the commitment status took effect (ISO timestamp string). |
| `committed_status_committed_asset_key` | integer | On3 numeric key of the committed-to program. |
| `committed_status_committed_asset_url` | character | CDN URL of the committed-to program's logo. |
| `committed_status_committed_asset_slug` | character | URL slug of the committed-to program on On3. |
| `committed_status_committed_asset_full_name` | character | Full name of the committed-to program (e.g. 'Alabama Crimson Tide'). |
| `committed_status_committed_asset_res_key` | integer | On3 asset key of the committed-to program's logo asset. |
| `committed_status_committed_asset_res_domain_override` | character | CDN domain override for the committed-to program's logo asset (usually null). |
| `committed_status_committed_asset_res_domain` | character | CDN domain serving the committed-to program's logo asset. |
| `committed_status_committed_asset_res_source_override` | character | Source-path override for the committed-to program's logo asset (usually null). |
| `committed_status_committed_asset_res_source` | character | CDN-relative source path of the committed-to program's logo asset. |
| `committed_status_committed_asset_res_title` | character | Editorial title attached to the committed-to program's logo asset. |
| `committed_status_committed_asset_res_description` | character | Editorial description attached to the committed-to program's logo asset (usually null). |
| `committed_status_committed_asset_res_caption` | character | Editorial caption attached to the committed-to program's logo asset (usually null). |
| `committed_status_committed_asset_res_category` | character | Editorial category label of the committed-to program's logo asset (usually null). |
| `committed_status_committed_asset_res_alt_text` | character | Accessibility alt text of the committed-to program's logo asset (usually null). |
| `committed_status_committed_asset_res_height` | integer | Pixel height of the committed-to program's logo asset. |
| `committed_status_committed_asset_res_width` | integer | Pixel width of the committed-to program's logo asset. |
| `committed_status_committed_asset_res_asset_type` | character | On3 asset-type discriminator of the committed-to program's logo asset (e.g. Image). |
| `committed_status_committed_asset_res_file_system` | character | Storage file-system flag of the committed-to program's logo asset. |
| `committed_status_committed_asset_res_path` | character | Storage path of the committed-to program's logo asset. |
| `committed_status_committed_asset_res_type` | character | Media type field of the committed-to program's logo asset (file extension, e.g. png). |
| `committed_status_committed_asset_res_thumbnail` | character | Thumbnail variant of the committed-to program's logo asset (video assets; usually null). |
| `committed_status_committed_asset_res_duration` | integer | Duration of the committed-to program's logo asset when it is a video (usually null or 0). |
| `committed_status_committed_asset_res_mime_type` | character | MIME type of the committed-to program's logo asset. |
| `committed_status_transferred_asset` | character | Nested asset of the program transferred to (the commitment status; usually null). |
| `committed_status_transferred_asset_res` | character | Nested logo asset of the program transferred to (the commitment status; usually null). |
| `committed_status_committed_organization_key` | integer | On3 key of the committed-to program (the commitment status). |
| `committed_status_committed_organization_full_name` | character | Full name of the commitment status's committed-to program (e.g. 'Alabama Crimson Tide'). |
| `committed_status_committed_organization_name` | character | Short name of the commitment status's committed-to program. |
| `committed_status_committed_organization_mascot` | character | Mascot of the commitment status's committed-to program. |
| `committed_status_committed_organization_abbreviation` | character | Abbreviation of the commitment status's committed-to program. |
| `committed_status_committed_organization_asset_url` | character | CDN URL of the committed-to program's logo (the commitment status). |
| `committed_status_committed_organization_asset` | character | Nested logo asset of the committed-to program (the commitment status; stringified or null). |
| `committed_status_committed_organization_slug` | character | URL slug of the committed-to program (the commitment status). |
| `committed_status_committed_organization_primary_color` | character | Primary hex color of the commitment status's committed-to program. |
| `committed_status_class_rank` | character | Academic class standing recorded on the commitment status (e.g. Senior). |
| `committed_status_transfer_entered` | character | Date the player entered the transfer portal (the commitment status; null when never entered). |
| `committed_status_recruitment_year` | character | Recruiting-cycle year the commitment status belongs to. |
| `committed_status_decommitted_asset` | character | Nested asset of the program decommitted from (the commitment status; usually null). |
| `committed_status_transfer` | logical | Transfer flag of the commitment status (null when not applicable). |
| `committed_status_expected_to_transfer` | logical | Expected-to-transfer flag of the commitment status (null when not applicable). |
| `committed_status_recruitment_key` | integer | On3 key of the recruitment record the commitment status belongs to. |
| `committed_status_withdrawn_transfer` | logical | Whether the player withdrew from the transfer portal (the commitment status). |
| `committed_status_withdrawn_transfer_date` | character | Date the player withdrew from the transfer portal (the commitment status; null when never withdrawn). |
| `high_school_org_key` | integer | On3 numeric key of the high school. |
| `high_school_org_full_name` | character | Full name of the high school (e.g. 'Alabama Crimson Tide'). |
| `high_school_org_name` | character | Short name of the high school. |
| `high_school_org_known_as` | character | Common short name of the high school, when On3 lists one. |
| `high_school_org_mascot` | character | Mascot of the high school. |
| `high_school_org_abbreviation` | character | Abbreviation of the high school. |
| `high_school_org_asset_url` | character | Convenience CDN URL of the high school's logo. |
| `high_school_org_default_asset_key` | integer | On3 asset key of the high school's logo asset. |
| `high_school_org_default_asset_domain_override` | character | CDN domain override for the high school's logo asset (usually null). |
| `high_school_org_default_asset_domain` | character | CDN domain serving the high school's logo asset. |
| `high_school_org_default_asset_source_override` | character | Source-path override for the high school's logo asset (usually null). |
| `high_school_org_default_asset_source` | character | CDN-relative source path of the high school's logo asset. |
| `high_school_org_default_asset_title` | character | Editorial title attached to the high school's logo asset. |
| `high_school_org_default_asset_description` | character | Editorial description attached to the high school's logo asset (usually null). |
| `high_school_org_default_asset_caption` | character | Editorial caption attached to the high school's logo asset (usually null). |
| `high_school_org_default_asset_category` | character | Editorial category label of the high school's logo asset (usually null). |
| `high_school_org_default_asset_alt_text` | character | Accessibility alt text of the high school's logo asset (usually null). |
| `high_school_org_default_asset_height` | integer | Pixel height of the high school's logo asset. |
| `high_school_org_default_asset_width` | integer | Pixel width of the high school's logo asset. |
| `high_school_org_default_asset_asset_type` | character | On3 asset-type discriminator of the high school's logo asset (e.g. Image). |
| `high_school_org_default_asset_file_system` | character | Storage file-system flag of the high school's logo asset. |
| `high_school_org_default_asset_path` | character | Storage path of the high school's logo asset. |
| `high_school_org_default_asset_type` | character | Media type field of the high school's logo asset (file extension, e.g. png). |
| `high_school_org_default_asset_thumbnail` | character | Thumbnail variant of the high school's logo asset (video assets; usually null). |
| `high_school_org_default_asset_duration` | integer | Duration of the high school's logo asset when it is a video (usually null or 0). |
| `high_school_org_default_asset_mime_type` | character | MIME type of the high school's logo asset. |
| `high_school_org_slug` | character | URL slug of the high school on On3. |
| `high_school_org_primary_color` | character | Primary hex color of the high school. |
| `high_school_org_org_type` | character | Organization type label of the high school (e.g. HighSchool, College). |
| `high_school_org_org_type_enum` | character | Organization type enum of the high school (same vocabulary as org_type). |
| `high_school_org_division` | character | Division or classification of the high school (e.g. NCAA-FB). |
| `high_school_org_site_keys` | character | JSON-encoded On3 site keys covering the high school (usually null). |
| `high_school_org_url_slug` | character | URL slug variant of the high school's page, with the key appended. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_recruitments_profile-example}

```python
on3_recruitments_profile(rec_key=270036)
```

_Last validated n/a._

## on3_recruitments_rpm_picks

GET /rdb/v1/recruitments/{recKey}/rpm-picks

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/recruitments/{rec_key}/rpm-picks`

**Valid URL:** [https://api.on3.com/public/rdb/v1/recruitments/270036/rpm-picks](https://api.on3.com/public/rdb/v1/recruitments/270036/rpm-picks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `rec_key` | `rec_key` |  | `Y` |  | rec_key path parameter. |

### Returns {#on3_recruitments_rpm_picks-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: its committed capture has 0 rows, so the parser emits no columns; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_recruitments_rpm_picks-example}

```python
on3_recruitments_rpm_picks(rec_key=270036)
```

_Last validated n/a._

## on3_recruitments_rpm_summary

GET /rdb/v1/recruitments/{recKey}/rpm-summary

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/recruitments/{rec_key}/rpm-summary`

**Valid URL:** [https://api.on3.com/public/rdb/v1/recruitments/270036/rpm-summary](https://api.on3.com/public/rdb/v1/recruitments/270036/rpm-summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `rec_key` | `rec_key` |  | `Y` |  | rec_key path parameter. |

### Returns {#on3_recruitments_rpm_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `predictions` | character | Per-team RPM prediction percentages for the recruitment, as a stringified list. |
| `locked` | logical | Whether the RPM prediction for the recruitment is locked (no longer updating). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_recruitments_rpm_summary-example}

```python
on3_recruitments_rpm_summary(rec_key=270036)
```

_Last validated n/a._
