---
title: "College football — On3 Recruit Database (api.on3.com) — Collective"
sidebar_label: "Collective"
sidebar_position: 1
description: "College football — On3 Recruit Database (api.on3.com) — Collective — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# College football — On3 Recruit Database (api.on3.com) — Collective

## on3_collective_groups

Collective groups.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/collective-groups`

**Valid URL:** [https://api.on3.com/public/rdb/v1/collective-groups](https://api.on3.com/public/rdb/v1/collective-groups)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `organizationKey` | `organization_key` |  |  | `Y` | organizationKey query parameter. |
| `query` | `query` |  |  | `Y` | query query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_collective_groups-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | Int64 | On3 RDB key for the NIL collective group. |
| `name` | String | Display name of the row's record. |
| `default_asset_key` | Int64 | On3 asset key for the collective's primary logo image. |
| `social_asset_key` | Int64 | On3 asset key for the collective's social-media image. |
| `organization_key` | Int64 | On3 organization key of the school the collective supports. |
| `launch_date` | String | Date the NIL collective launched. |
| `organization_type` | String | Legal form of the collective (e.g. LLC, 501(c)(3)). |
| `twitter_handle` | String | Collective's Twitter/X account handle. |
| `instagram_handle` | String | Collective's Instagram account handle. |
| `tik_tok_handle` | String | Collective's TikTok account handle. |
| `youtube_handle` | String | Collective's YouTube channel handle. |
| `linked_in_handle` | String | Collective's LinkedIn account handle. |
| `website_name` | String | Display name of the collective's website. |
| `website_url` | String | URL of the collective's website. |
| `mission_statement` | String | Collective's stated mission, as published to On3. |
| `description` | String | Free-text description or biography shipped by On3. |
| `annual_goal_amount` | Float64 | Collective's annual fundraising goal in dollars, as reported to On3. |
| `confirmed_raised_amount` | Float64 | Dollar amount the collective has confirmed raising, per On3. |
| `merged_into_group_key` | Int64 | On3 key of the collective this group merged into, when applicable. |
| `merged_into_group` | String | Nested On3 record for the collective this group merged into (stringified). |
| `slug` | String | URL slug of the row's record on On3. |
| `founders` | String | Founders of the collective, as a stringified list. |
| `sports` | String | Sports the collective funds, as a stringified list. |
| `default_asset_key_2` | Int64 | Asset key repeated from the nested default-asset object (json_normalize de-duplication suffix). |
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
| `social_asset_key_2` | Int64 | Asset key repeated from the nested social-asset object (json_normalize de-duplication suffix). |
| `social_asset_domain_override` | String | CDN domain override for the social-media asset (usually null). |
| `social_asset_domain` | String | CDN domain serving the social-media asset. |
| `social_asset_source_override` | String | Source-path override for the social-media asset (usually null). |
| `social_asset_source` | String | CDN-relative source path of the social-media asset. |
| `social_asset_title` | String | Editorial title attached to the social-media asset. |
| `social_asset_description` | String | Editorial description attached to the social-media asset (usually null). |
| `social_asset_caption` | String | Editorial caption attached to the social-media asset (usually null). |
| `social_asset_category` | String | Editorial category label of the social-media asset (usually null). |
| `social_asset_alt_text` | String | Accessibility alt text of the social-media asset (usually null). |
| `social_asset_height` | Int64 | Pixel height of the social-media asset. |
| `social_asset_width` | Int64 | Pixel width of the social-media asset. |
| `social_asset_asset_type` | String | On3 asset-type discriminator of the social-media asset (e.g. Image). |
| `social_asset_file_system` | String | Storage file-system flag of the social-media asset. |
| `social_asset_path` | String | Storage path of the social-media asset. |
| `social_asset_type` | String | Media type field of the social-media asset (file extension, e.g. png). |
| `social_asset_thumbnail` | String | Thumbnail variant of the social-media asset (video assets; usually null). |
| `social_asset_duration` | Int64 | Duration of the social-media asset when it is a video (usually null or 0). |
| `social_asset_mime_type` | String | MIME type of the social-media asset. |
| `organization_key_2` | Int64 | Organization key repeated from the nested organization object (json_normalize de-duplication suffix). |
| `organization_full_name` | String | Full name of the program (e.g. 'Alabama Crimson Tide'). |
| `organization_name` | String | Short name of the program. |
| `organization_known_as` | String | Common short name of the program, when On3 lists one. |
| `organization_mascot` | String | Mascot of the program. |
| `organization_abbreviation` | String | Abbreviation of the program. |
| `organization_asset_url` | String | Convenience CDN URL of the program's logo. |
| `organization_default_asset_key` | Int64 | On3 asset key of the program's logo asset. |
| `organization_default_asset_domain_override` | String | CDN domain override for the program's logo asset (usually null). |
| `organization_default_asset_domain` | String | CDN domain serving the program's logo asset. |
| `organization_default_asset_source_override` | String | Source-path override for the program's logo asset (usually null). |
| `organization_default_asset_source` | String | CDN-relative source path of the program's logo asset. |
| `organization_default_asset_title` | String | Editorial title attached to the program's logo asset. |
| `organization_default_asset_description` | String | Editorial description attached to the program's logo asset (usually null). |
| `organization_default_asset_caption` | String | Editorial caption attached to the program's logo asset (usually null). |
| `organization_default_asset_category` | String | Editorial category label of the program's logo asset (usually null). |
| `organization_default_asset_alt_text` | String | Accessibility alt text of the program's logo asset (usually null). |
| `organization_default_asset_height` | Int64 | Pixel height of the program's logo asset. |
| `organization_default_asset_width` | Int64 | Pixel width of the program's logo asset. |
| `organization_default_asset_asset_type` | String | On3 asset-type discriminator of the program's logo asset (e.g. Image). |
| `organization_default_asset_file_system` | String | Storage file-system flag of the program's logo asset. |
| `organization_default_asset_path` | String | Storage path of the program's logo asset. |
| `organization_default_asset_type` | String | Media type field of the program's logo asset (file extension, e.g. png). |
| `organization_default_asset_thumbnail` | String | Thumbnail variant of the program's logo asset (video assets; usually null). |
| `organization_default_asset_duration` | Int64 | Duration of the program's logo asset when it is a video (usually null or 0). |
| `organization_default_asset_mime_type` | String | MIME type of the program's logo asset. |
| `organization_slug` | String | URL slug of the program on On3. |
| `organization_primary_color` | String | Primary hex color of the program. |
| `organization_org_type` | String | Organization type label of the program (e.g. HighSchool, College). |
| `organization_org_type_enum` | String | Organization type enum of the program (same vocabulary as org_type). |
| `organization_division` | String | Division or classification of the program (e.g. NCAA-FB). |
| `organization_site_keys` | String | JSON-encoded On3 site keys covering the program (usually null). |
| `organization_url_slug` | String | URL slug variant of the program's page, with the key appended. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_collective_groups-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_collective_groups-example}

```python
on3_collective_groups()
```

## on3_collective_groups_deals

Collective groups deals.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/collective-groups/{key}/deals`

**Valid URL:** [https://api.on3.com/public/rdb/v1/collective-groups/1/deals](https://api.on3.com/public/rdb/v1/collective-groups/1/deals)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_collective_groups_deals-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_collective_groups_deals-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_collective_groups_deals-example}

```python
on3_collective_groups_deals(key=1)
```

## on3_collective_groups_key

Collective groups key.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/collective-groups/{key}`

**Valid URL:** [https://api.on3.com/public/rdb/v1/collective-groups/1](https://api.on3.com/public/rdb/v1/collective-groups/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#on3_collective_groups_key-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_collective_groups_key-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_collective_groups_key-example}

```python
on3_collective_groups_key(key=1)
```
