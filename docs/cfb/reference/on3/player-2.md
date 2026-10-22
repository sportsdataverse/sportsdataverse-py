# CFB — On3 Recruit Database (api.on3.com) — Player: industry

> CFB — On3 Recruit Database (api.on3.com) — Player: industry — function reference in sdv-py, the SportsDataverse Python package.

## on3_players_industry_comparision

GET /rdb/v1/players/industry-comparision

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/players/industry-comparision`

**Valid URL:** [https://api.on3.com/public/rdb/v1/players/industry-comparision](https://api.on3.com/public/rdb/v1/players/industry-comparision)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `stateAbbr` | `state_abbr` |  |  | `Y` | stateAbbr query parameter. |
| `positionAbbr` | `position_abbr` |  |  | `Y` | positionAbbr query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `sortByIndustry` | `sort_by_industry` |  |  | `Y` | sortByIndustry query parameter. |

### Returns {#on3_players_industry_comparision-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `ratings` | character | List of per-service rating entries (On3, Rivals, 247, ESPN) composing the industry comparison. |
| `nil_value` | integer | On3 NIL valuation for the player (US dollars). |
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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_players_industry_comparision-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_players_industry_comparision-example}

```python
on3_players_industry_comparision()
```

_Last validated n/a._

## on3_players_industry_comparision_list

GET /rdb/v1/players/industry-comparision-list

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/players/industry-comparision-list`

**Valid URL:** [https://api.on3.com/public/rdb/v1/players/industry-comparision-list](https://api.on3.com/public/rdb/v1/players/industry-comparision-list)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_players_industry_comparision_list-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_players_industry_comparision_list-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_players_industry_comparision_list-example}

```python
on3_players_industry_comparision_list()
```

_Last validated n/a._
