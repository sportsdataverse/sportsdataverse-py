---
title: "College football — On3 Recruit Database (api.on3.com) — Commits"
sidebar_label: "Commits"
sidebar_position: 2
description: "College football — On3 Recruit Database (api.on3.com) — Commits — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# College football — On3 Recruit Database (api.on3.com) — Commits

## on3_commits_latest

Commits latest.

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
| `key` | Int64 | On3 person key (stable athlete identifier) for the recruit. |
| `recruitment_key` | Int64 | On3 recruitment key for the recruit's active recruitment. |
| `name` | String | Full name of the recruit. |
| `slug` | String | URL slug for the recruit's On3 profile. |
| `high_school_name` | String | Name of the recruit's high school. |
| `home_town_name` | String | Recruit's hometown (city, state). |
| `early_enrollee` | Boolean | Whether the recruit early-enrolled at their college. |
| `early_signee` | Boolean | Whether the recruit signed during the early signing period. |
| `default_asset_url` | String | URL of the recruit's primary headshot image. |
| `class_year` | Int64 | Recruiting class (graduation) year of the recruit. |
| `athlete_verified` | Boolean | Whether On3 has verified the recruit's athletic identity. |
| `prospect_verified` | Boolean | Whether On3 has verified the recruit as a prospect. |
| `position_abbreviation` | String | Abbreviated primary position of the recruit. |
| `height` | String | Recruit height (formatted string, e.g. "6-2"). |
| `weight` | Int64 | Recruit weight in pounds. |
| `roster_rating` | String | On3 roster (transfer-portal-adjusted) rating for the recruit. |
| `predictions` | String | List of recruiting-prediction entries (RPM picks) for the recruit. |
| `nil_status` | String | Nested NIL (name/image/likeness) status object for the recruit. |
| `nil_value` | Int64 | On3 NIL valuation for the recruit (US dollars). |
| `high_school_key` | Int64 | On3 numeric key of the high school. |
| `high_school_full_name` | String | Full name of the high school (with mascot). |
| `high_school_name_2` | String | Name field of the nested high-school object (json_normalize de-duplication of high_school_name). |
| `high_school_known_as` | String | Common short name of the high school, when On3 lists one. |
| `high_school_mascot` | String | High-school mascot. |
| `high_school_abbreviation` | String | High-school abbreviation. |
| `high_school_asset_url` | String | Convenience CDN URL of the high-school logo. |
| `high_school_default_asset_key` | Float64 | On3 asset key of the high school's logo asset. |
| `high_school_default_asset_domain_override` | String | CDN domain override for the high school's logo asset (usually null). |
| `high_school_default_asset_domain` | String | CDN domain serving the high school's logo asset. |
| `high_school_default_asset_source_override` | String | Source-path override for the high school's logo asset (usually null). |
| `high_school_default_asset_source` | String | CDN-relative source path of the high school's logo asset. |
| `high_school_default_asset_title` | String | Editorial title attached to the high school's logo asset. |
| `high_school_default_asset_description` | String | Editorial description attached to the high school's logo asset (usually null). |
| `high_school_default_asset_caption` | String | Editorial caption attached to the high school's logo asset (usually null). |
| `high_school_default_asset_category` | String | Editorial category label of the high school's logo asset (usually null). |
| `high_school_default_asset_alt_text` | String | Accessibility alt text of the high school's logo asset (usually null). |
| `high_school_default_asset_height` | Float64 | Pixel height of the high school's logo asset. |
| `high_school_default_asset_width` | Float64 | Pixel width of the high school's logo asset. |
| `high_school_default_asset_asset_type` | String | On3 asset-type discriminator of the high school's logo asset (e.g. Image). |
| `high_school_default_asset_file_system` | String | Storage file-system flag of the high school's logo asset. |
| `high_school_default_asset_path` | String | Storage path of the high school's logo asset. |
| `high_school_default_asset_type` | String | Media type field of the high school's logo asset (file extension, e.g. png). |
| `high_school_default_asset_thumbnail` | String | Thumbnail variant of the high school's logo asset (video assets; usually null). |
| `high_school_default_asset_duration` | Float64 | Duration of the high school's logo asset when it is a video (usually null or 0). |
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
| `rating_key` | Float64 | On3 key of the On3 rating record. |
| `rating_rating` | Float64 | Numeric value of the On3 rating (0-100 scale). |
| `rating_stars` | Float64 | Star rating of the On3 rating (2-5). |
| `rating_national_rank` | Float64 | National rank of the On3 rating. |
| `rating_position_rank` | Float64 | Position rank of the On3 rating. |
| `rating_state_rank` | Float64 | State rank of the On3 rating. |
| `rating_position_abbr` | String | Position abbreviation the On3 rating was assigned at. |
| `rating_state_abbr` | String | State abbreviation the On3 rating was assigned in. |
| `rating_five_star_plus` | String | Five-star-plus flag on the On3 rating. |
| `commit_status_type` | String | Type of the commitment status (e.g. Committed, Signed, Enrolled, None). |
| `commit_status_short_term_signee` | Boolean | Short-term-signee flag of the commitment status (null when not applicable). |
| `commit_status_date` | String | Date the commitment status took effect (ISO timestamp string). |
| `commit_status_committed_asset_key` | Int64 | On3 numeric key of the commitment status's committed-to program. |
| `commit_status_committed_asset_url` | String | CDN URL of the commitment status's committed-to program's logo. |
| `commit_status_committed_asset_slug` | String | URL slug of the commitment status's committed-to program on On3. |
| `commit_status_committed_asset_full_name` | String | Full name of the commitment status's committed-to program (e.g. 'Alabama Crimson Tide'). |
| `commit_status_committed_asset_res_key` | Int64 | On3 asset key of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_asset_res_domain_override` | String | CDN domain override for the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_asset_res_domain` | String | CDN domain serving the commitment status's committed-to program's logo asset. |
| `commit_status_committed_asset_res_source_override` | String | Source-path override for the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_asset_res_source` | String | CDN-relative source path of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_asset_res_title` | String | Editorial title attached to the commitment status's committed-to program's logo asset. |
| `commit_status_committed_asset_res_description` | String | Editorial description attached to the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_asset_res_caption` | String | Editorial caption attached to the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_asset_res_category` | String | Editorial category label of the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_asset_res_alt_text` | String | Accessibility alt text of the commitment status's committed-to program's logo asset (usually null). |
| `commit_status_committed_asset_res_height` | Int64 | Pixel height of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_asset_res_width` | Int64 | Pixel width of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_asset_res_asset_type` | String | On3 asset-type discriminator of the commitment status's committed-to program's logo asset (e.g. Image). |
| `commit_status_committed_asset_res_file_system` | String | Storage file-system flag of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_asset_res_path` | String | Storage path of the commitment status's committed-to program's logo asset. |
| `commit_status_committed_asset_res_type` | String | Media type field of the commitment status's committed-to program's logo asset (file extension, e.g. png). |
| `commit_status_committed_asset_res_thumbnail` | String | Thumbnail variant of the commitment status's committed-to program's logo asset (video assets; usually null). |
| `commit_status_committed_asset_res_duration` | Int64 | Duration of the commitment status's committed-to program's logo asset when it is a video (usually null or 0). |
| `commit_status_committed_asset_res_mime_type` | String | MIME type of the commitment status's committed-to program's logo asset. |
| `commit_status_transferred_asset` | String | Nested asset of the program transferred to (the commitment status; usually null). |
| `commit_status_transferred_asset_res` | String | Nested logo asset of the program transferred to (the commitment status; usually null). |
| `commit_status_committed_organization_key` | Int64 | On3 key of the committed-to program (the commitment status). |
| `commit_status_committed_organization_full_name` | String | Full name of the commitment status's committed-to program (e.g. 'Alabama Crimson Tide'). |
| `commit_status_committed_organization_name` | String | Short name of the commitment status's committed-to program. |
| `commit_status_committed_organization_mascot` | String | Mascot of the commitment status's committed-to program. |
| `commit_status_committed_organization_abbreviation` | String | Abbreviation of the commitment status's committed-to program. |
| `commit_status_committed_organization_asset_url` | String | CDN URL of the committed-to program's logo (the commitment status). |
| `commit_status_committed_organization_asset` | String | Nested logo asset of the committed-to program (the commitment status; stringified or null). |
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
| `sport_key` | Int64 | On3 numeric key of the sport. |
| `sport_name` | String | Name of the sport (e.g. Football). |
| `rating` | String | On3 rating for the recruit. |
| `high_school_default_asset` | String | Nested default-asset object of the high school (null when On3 ships none). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_commits_latest-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_commits_latest-example}

```python
on3_commits_latest()
```

## on3_commits_organizations_latest_commits

Commits organizations latest commits.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/commits/organizations/{org_key}/latest-commits`

**Valid URL:** [https://api.on3.com/public/rdb/v1/commits/organizations/1867/latest-commits](https://api.on3.com/public/rdb/v1/commits/organizations/1867/latest-commits)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `org_key` | `org_key` |  | `Y` |  | org_key path parameter. |

### Returns {#on3_commits_organizations_latest_commits-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the row has nested objects whose flattened column names depend on which are null in the data; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_commits_organizations_latest_commits-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_commits_organizations_latest_commits-example}

```python
on3_commits_organizations_latest_commits(org_key=1867)
```

## on3_commits_organizations_org_key

Commits organizations org key.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/commits/organizations/{org_key}`

**Valid URL:** [https://api.on3.com/public/rdb/v1/commits/organizations/1867](https://api.on3.com/public/rdb/v1/commits/organizations/1867)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `org_key` | `org_key` |  | `Y` |  | org_key path parameter. |

### Returns {#on3_commits_organizations_org_key-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_commits_organizations_org_key-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_commits_organizations_org_key-example}

```python
on3_commits_organizations_org_key(org_key=1867)
```
