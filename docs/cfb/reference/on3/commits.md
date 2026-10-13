# CFB — On3 Recruit Database (api.on3.com) — Commits

> CFB — On3 Recruit Database (api.on3.com) — Commits — function reference in sdv-py, the SportsDataverse Python package.

## on3_commits_latest

GET /rdb/v1/commits/latest

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/commits/latest`

**Valid URL:** [https://api.on3.com/public/rdb/v1/commits/latest](https://api.on3.com/public/rdb/v1/commits/latest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_commits_latest-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | integer | On3 person key (stable athlete identifier) for the recruit. |
| `recruitment_key` | integer | On3 recruitment key for the recruit's active recruitment. |
| `name` | character | Full name of the recruit. |
| `slug` | character | URL slug for the recruit's On3 profile. |
| `high_school_name` | character | Name of the recruit's high school. |
| `home_town_name` | character | Recruit's hometown (city, state). |
| `early_enrollee` | logical | Whether the recruit early-enrolled at their college. |
| `early_signee` | logical | Whether the recruit signed during the early signing period. |
| `default_asset_url` | character | URL of the recruit's primary headshot image. |
| `class_year` | integer | Recruiting class (graduation) year of the recruit. |
| `athlete_verified` | logical | Whether On3 has verified the recruit's athletic identity. |
| `prospect_verified` | logical | Whether On3 has verified the recruit as a prospect. |
| `position_abbreviation` | character | Abbreviated primary position of the recruit. |
| `height` | character | Recruit height (formatted string, e.g. "6-2"). |
| `weight` | integer | Recruit weight in pounds. |
| `roster_rating` | character | On3 roster (transfer-portal-adjusted) rating for the recruit. |
| `predictions` | character | List of recruiting-prediction entries (RPM picks) for the recruit. |
| `nil_status` | character | Nested NIL (name/image/likeness) status object for the recruit. |
| `nil_value` | integer | On3 NIL valuation for the recruit (US dollars). |
| `high_school_key` | integer | On3 numeric key of the high school. |
| `high_school_full_name` | character | Full name of the high school (with mascot). |
| `high_school_name_2` | character | Name field of the nested high-school object (json_normalize de-duplication of high_school_name). |
| `high_school_known_as` | character | Common short name of the high school, when On3 lists one. |
| `high_school_mascot` | character | High-school mascot. |
| `high_school_abbreviation` | character | High-school abbreviation. |
| `high_school_asset_url` | character | Convenience CDN URL of the high-school logo. |
| `high_school_default_asset_key` | numeric | On3 asset key of the high school's logo asset. |
| `high_school_default_asset_domain_override` | character | CDN domain override for the high school's logo asset (usually null). |
| `high_school_default_asset_domain` | character | CDN domain serving the high school's logo asset. |
| `high_school_default_asset_source_override` | character | Source-path override for the high school's logo asset (usually null). |
| `high_school_default_asset_source` | character | CDN-relative source path of the high school's logo asset. |
| `high_school_default_asset_title` | character | Editorial title attached to the high school's logo asset. |
| `high_school_default_asset_description` | character | Editorial description attached to the high school's logo asset (usually null). |
| `high_school_default_asset_caption` | character | Editorial caption attached to the high school's logo asset (usually null). |
| `high_school_default_asset_category` | character | Editorial category label of the high school's logo asset (usually null). |
| `high_school_default_asset_alt_text` | character | Accessibility alt text of the high school's logo asset (usually null). |
| `high_school_default_asset_height` | numeric | Pixel height of the high school's logo asset. |
| `high_school_default_asset_width` | numeric | Pixel width of the high school's logo asset. |
| `high_school_default_asset_asset_type` | character | On3 asset-type discriminator of the high school's logo asset (e.g. Image). |
| `high_school_default_asset_file_system` | character | Storage file-system flag of the high school's logo asset. |
| `high_school_default_asset_path` | character | Storage path of the high school's logo asset. |
| `high_school_default_asset_type` | character | Media type field of the high school's logo asset (file extension, e.g. png). |
| `high_school_default_asset_thumbnail` | character | Thumbnail variant of the high school's logo asset (video assets; usually null). |
| `high_school_default_asset_duration` | numeric | Duration of the high school's logo asset when it is a video (usually null or 0). |
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
| `rating_key` | numeric | On3 key of the On3 rating record. |
| `rating_rating` | numeric | Numeric value of the On3 rating (0-100 scale). |
| `rating_stars` | numeric | Star rating of the On3 rating (2-5). |
| `rating_national_rank` | numeric | National rank of the On3 rating. |
| `rating_position_rank` | numeric | Position rank of the On3 rating. |
| `rating_state_rank` | numeric | State rank of the On3 rating. |
| `rating_position_abbr` | character | Position abbreviation the On3 rating was assigned at. |
| `rating_state_abbr` | character | State abbreviation the On3 rating was assigned in. |
| `rating_five_star_plus` | character | Five-star-plus flag on the On3 rating. |
| `commit_status_type` | character | Type of the commitment status (e.g. Committed, Signed, Enrolled, None). |
| `commit_status_short_term_signee` | logical | Short-term-signee flag of the commitment status (null when not applicable). |
| `commit_status_date` | character | Date the commitment status took effect (ISO timestamp string). |
| `commit_status_committed_asset_key` | integer | On3 numeric key of the commitment status's committed-to program. |
| `commit_status_committed_asset_url` | character | CDN URL of the commitment status's committed-to program's logo. |
| `commit_status_committed_asset_slug` | character | URL slug of the commitment status's committed-to program on On3. |
| `commit_status_committed_asset_full_name` | character | Full name of the commitment status's committed-to program (e.g. 'Alabama Crimson Tide'). |
| `commit_status_committed_asset_res_key` | integer | On3 asset key of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_asset_res_domain_override` | character | CDN domain override for the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_asset_res_domain` | character | CDN domain serving the commitment status's committed-to program's logo asset. |
| `commit_status_committed_asset_res_source_override` | character | Source-path override for the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_asset_res_source` | character | CDN-relative source path of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_asset_res_title` | character | Editorial title attached to the commitment status's committed-to program's logo asset. |
| `commit_status_committed_asset_res_description` | character | Editorial description attached to the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_asset_res_caption` | character | Editorial caption attached to the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_asset_res_category` | character | Editorial category label of the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_asset_res_alt_text` | character | Accessibility alt text of the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_asset_res_height` | integer | Pixel height of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_asset_res_width` | integer | Pixel width of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_asset_res_asset_type` | character | On3 asset-type discriminator of the commitment status's committed-to program's logo asset (e.g. Image). |
| `commit_status_committed_asset_res_file_system` | character | Storage file-system flag of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_asset_res_path` | character | Storage path of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_asset_res_type` | character | Media type field of the commitment status's committed-to program's logo asset (file extension, e.g. png). |
| `commit_status_committed_asset_res_thumbnail` | character | Thumbnail variant of the commitment status's committed-to program's logo asset (video assets; usually null). |
| `commit_status_committed_asset_res_duration` | integer | Duration of the commitment status's committed-to program's logo asset when it is a video (usually null or 0). |
| `commit_status_committed_asset_res_mime_type` | character | MIME type of the commitment status's committed-to program's logo asset. |
| `commit_status_transferred_asset` | character | Nested asset of the program transferred to (the commitment status; usually null). |
| `commit_status_transferred_asset_res` | character | Nested logo asset of the program transferred to (the commitment status; usually null). |
| `commit_status_committed_organization_key` | integer | On3 key of the committed-to program (the commitment status). |
| `commit_status_committed_organization_full_name` | character | Full name of the commitment status's committed-to program (e.g. 'Alabama Crimson Tide'). |
| `commit_status_committed_organization_name` | character | Short name of the commitment status's committed-to program. |
| `commit_status_committed_organization_mascot` | character | Mascot of the commitment status's committed-to program. |
| `commit_status_committed_organization_abbreviation` | character | Abbreviation of the commitment status's committed-to program. |
| `commit_status_committed_organization_asset_url` | character | CDN URL of the committed-to program's logo (the commitment status). |
| `commit_status_committed_organization_asset` | character | Nested logo asset of the committed-to program (the commitment status; stringified or null). |
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
| `sport_key` | integer | On3 numeric key of the sport. |
| `sport_name` | character | Name of the sport (e.g. Football). |
| `rating` | character | On3 rating for the recruit. |
| `high_school_default_asset` | character | Nested default-asset object of the high school (null when On3 ships none). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_commits_latest-example}

```python
on3_commits_latest()
```

_Last validated n/a._

## on3_commits_organizations_latest_commits

GET /rdb/v1/commits/organizations/{orgKey}/latest-commits

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/commits/organizations/{org_key}/latest-commits`

**Valid URL:** [https://api.on3.com/public/rdb/v1/commits/organizations/1867/latest-commits](https://api.on3.com/public/rdb/v1/commits/organizations/1867/latest-commits)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `org_key` | `org_key` |  | `Y` |  | org_key path parameter. |

### Returns {#on3_commits_organizations_latest_commits-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the row has nested objects whose flattened column names depend on which are null in the data; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_commits_organizations_latest_commits-example}

```python
on3_commits_organizations_latest_commits(org_key=1867)
```

_Last validated n/a._

## on3_commits_organizations_org_key

GET /rdb/v1/commits/organizations/{orgKey}

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/commits/organizations/{org_key}`

**Valid URL:** [https://api.on3.com/public/rdb/v1/commits/organizations/1867](https://api.on3.com/public/rdb/v1/commits/organizations/1867)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `org_key` | `org_key` |  | `Y` |  | org_key path parameter. |

### Returns {#on3_commits_organizations_org_key-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_commits_organizations_org_key-example}

```python
on3_commits_organizations_org_key(org_key=1867)
```

_Last validated n/a._
