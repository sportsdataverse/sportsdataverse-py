---
title: "CFB — On3 Recruit Database (api.on3.com) — Commits"
sidebar_label: "Commits"
sidebar_position: 1
description: "CFB — On3 Recruit Database (api.on3.com) — Commits — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB — On3 Recruit Database (api.on3.com) — Commits

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
| `high_school_key` | integer |  |
| `high_school_full_name` | character |  |
| `high_school_name_2` | character |  |
| `high_school_known_as` | character |  |
| `high_school_mascot` | character |  |
| `high_school_abbreviation` | character |  |
| `high_school_asset_url` | character |  |
| `high_school_default_asset_key` | numeric |  |
| `high_school_default_asset_domain_override` | character |  |
| `high_school_default_asset_domain` | character |  |
| `high_school_default_asset_source_override` | character |  |
| `high_school_default_asset_source` | character |  |
| `high_school_default_asset_title` | character |  |
| `high_school_default_asset_description` | character |  |
| `high_school_default_asset_caption` | character |  |
| `high_school_default_asset_category` | character |  |
| `high_school_default_asset_alt_text` | character |  |
| `high_school_default_asset_height` | numeric |  |
| `high_school_default_asset_width` | numeric |  |
| `high_school_default_asset_asset_type` | character |  |
| `high_school_default_asset_file_system` | character |  |
| `high_school_default_asset_path` | character |  |
| `high_school_default_asset_type` | character |  |
| `high_school_default_asset_thumbnail` | character |  |
| `high_school_default_asset_duration` | numeric |  |
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
| `rating_key` | numeric |  |
| `rating_rating` | numeric |  |
| `rating_stars` | numeric |  |
| `rating_national_rank` | numeric |  |
| `rating_position_rank` | numeric |  |
| `rating_state_rank` | numeric |  |
| `rating_position_abbr` | character |  |
| `rating_state_abbr` | character |  |
| `rating_five_star_plus` | character |  |
| `commit_status_type` | character |  |
| `commit_status_short_term_signee` | logical |  |
| `commit_status_date` | character |  |
| `commit_status_committed_asset_key` | integer |  |
| `commit_status_committed_asset_url` | character |  |
| `commit_status_committed_asset_slug` | character |  |
| `commit_status_committed_asset_full_name` | character |  |
| `commit_status_committed_asset_res_key` | integer |  |
| `commit_status_committed_asset_res_domain_override` | character |  |
| `commit_status_committed_asset_res_domain` | character |  |
| `commit_status_committed_asset_res_source_override` | character |  |
| `commit_status_committed_asset_res_source` | character |  |
| `commit_status_committed_asset_res_title` | character |  |
| `commit_status_committed_asset_res_description` | character |  |
| `commit_status_committed_asset_res_caption` | character |  |
| `commit_status_committed_asset_res_category` | character |  |
| `commit_status_committed_asset_res_alt_text` | character |  |
| `commit_status_committed_asset_res_height` | integer |  |
| `commit_status_committed_asset_res_width` | integer |  |
| `commit_status_committed_asset_res_asset_type` | character |  |
| `commit_status_committed_asset_res_file_system` | character |  |
| `commit_status_committed_asset_res_path` | character |  |
| `commit_status_committed_asset_res_type` | character |  |
| `commit_status_committed_asset_res_thumbnail` | character |  |
| `commit_status_committed_asset_res_duration` | integer |  |
| `commit_status_committed_asset_res_mime_type` | character |  |
| `commit_status_transferred_asset` | character |  |
| `commit_status_transferred_asset_res` | character |  |
| `commit_status_committed_organization_key` | integer |  |
| `commit_status_committed_organization_full_name` | character |  |
| `commit_status_committed_organization_name` | character |  |
| `commit_status_committed_organization_mascot` | character |  |
| `commit_status_committed_organization_abbreviation` | character |  |
| `commit_status_committed_organization_asset_url` | character |  |
| `commit_status_committed_organization_asset` | character |  |
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
| `sport_key` | integer |  |
| `sport_name` | character | Sport name (e.g., Major League Baseball). |
| `rating` | character | On3 rating for the recruit. |
| `high_school_default_asset` | character |  |

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
