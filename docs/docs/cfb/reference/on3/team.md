---
title: "CFB — On3 Recruit Database (api.on3.com) — Team"
sidebar_label: "Team"
sidebar_position: 8
description: "CFB — On3 Recruit Database (api.on3.com) — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB — On3 Recruit Database (api.on3.com) — Team

## on3_team_ranking

GET /rdb/v1/team-ranking

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/team-ranking`

**Valid URL:** [https://api.on3.com/public/rdb/v1/team-ranking](https://api.on3.com/public/rdb/v1/team-ranking)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_team_ranking-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_team_ranking-example}

```python
on3_team_ranking()
```

_Last validated n/a._

## on3_team_ranking_bluechips_team_rankings

GET /rdb/v1/team-ranking/{sport}-{year}/bluechips-team-rankings

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/team-ranking/{sport_slug}-{year}/bluechips-team-rankings`

**Valid URL:** [https://api.on3.com/public/rdb/v1/team-ranking/football-2025/bluechips-team-rankings](https://api.on3.com/public/rdb/v1/team-ranking/football-2025/bluechips-team-rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport_slug` | `sport_slug` |  | `Y` |  | sport_slug path parameter. |
| `year` | `year` |  | `Y` |  | year path parameter. |

### Returns {#on3_team_ranking_bluechips_team_rankings-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the row has nested objects whose flattened column names depend on which are null in the data; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_team_ranking_bluechips_team_rankings-example}

```python
on3_team_ranking_bluechips_team_rankings(sport_slug='football', year=2025)
```

_Last validated n/a._

## on3_team_ranking_consensus_team_rankings

GET /rdb/v1/team-ranking/{sport}-{year}/consensus-team-rankings

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/team-ranking/{sport_slug}-{year}/consensus-team-rankings`

**Valid URL:** [https://api.on3.com/public/rdb/v1/team-ranking/football-2025/consensus-team-rankings](https://api.on3.com/public/rdb/v1/team-ranking/football-2025/consensus-team-rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport_slug` | `sport_slug` |  | `Y` |  | sport_slug path parameter. |
| `year` | `year` |  | `Y` |  | year path parameter. |

### Returns {#on3_team_ranking_consensus_team_rankings-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the row has nested objects whose flattened column names depend on which are null in the data; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_team_ranking_consensus_team_rankings-example}

```python
on3_team_ranking_consensus_team_rankings(sport_slug='football', year=2025)
```

_Last validated n/a._

## on3_team_ranking_organizations_summary

GET /rdb/v1/team-ranking/organizations/{orgKey}/summary

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/team-ranking/organizations/{org_key}/summary`

**Valid URL:** [https://api.on3.com/public/rdb/v1/team-ranking/organizations/1867/summary](https://api.on3.com/public/rdb/v1/team-ranking/organizations/1867/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `org_key` | `org_key` |  | `Y` |  | org_key path parameter. |

### Returns {#on3_team_ranking_organizations_summary-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the row has nested objects whose flattened column names depend on which are null in the data; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_team_ranking_organizations_summary-example}

```python
on3_team_ranking_organizations_summary(org_key=1867)
```

_Last validated n/a._

## on3_team_ranking_team_rankings

GET /rdb/v1/team-ranking/{sport}-{year}/team-rankings

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/team-ranking/{sport_slug}-{year}/team-rankings`

**Valid URL:** [https://api.on3.com/public/rdb/v1/team-ranking/football-2025/team-rankings](https://api.on3.com/public/rdb/v1/team-ranking/football-2025/team-rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport_slug` | `sport_slug` |  | `Y` |  | sport_slug path parameter. |
| `year` | `year` |  | `Y` |  | year path parameter. |

### Returns {#on3_team_ranking_team_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | integer | On3 organization-ranking key for the class row. |
| `year` | integer | Recruiting class year of the row. |
| `applied_total_rating` | numeric | Total On3 rating applied to the class after deductions. |
| `applied_total_consensus_rating` | numeric | Total consensus rating applied to the class after deductions. |
| `applied_average_rating` | numeric | Average On3 rating applied to the class after deductions. |
| `applied_average_consensus_rating` | numeric | Average consensus rating applied to the class after deductions. |
| `commits` | integer | Number of commits in the recruiting class. |
| `applied_commits` | integer | Number of commits counted toward the applied class rating. |
| `deductions` | numeric | Rating deductions applied to the class (e.g. for roster limits). |
| `deductions_description` | character | Human-readable explanation of any applied deductions. |
| `five_stars` | integer | Count of On3 five-star commits in the class. |
| `consensus_five_stars` | integer | Count of consensus five-star commits in the class. |
| `four_stars` | integer | Count of On3 four-star commits in the class. |
| `consensus_four_stars` | integer | Count of consensus four-star commits in the class. |
| `three_stars` | integer | Count of On3 three-star commits in the class. |
| `consensus_three_stars` | integer | Count of consensus three-star commits in the class. |
| `overall_rank` | integer | National rank of the class by On3 score. |
| `overall_consensus_rank` | integer | National rank of the class by consensus score. |
| `dispay_consensus_score` | numeric | Display consensus score for the class (On3 sic spelling of "display"). |
| `dispay_on3_score` | numeric | Display On3 score for the class (On3 sic spelling of "display"). |
| `average_nil_value` | numeric | Average On3 NIL valuation across the class's commits (US dollars). |
| `conference_rank` | integer | Rank of the class within its conference by On3 score. |
| `conference_consensus_rank` | integer | Rank of the class within its conference by consensus score. |
| `organization_key` | integer | On3 numeric key of the program. |
| `organization_full_name` | character | Full name of the program (e.g. 'Alabama Crimson Tide'). |
| `organization_name` | character | Short name of the program. |
| `organization_mascot` | character | Mascot of the program. |
| `organization_abbreviation` | character | Abbreviation of the program. |
| `organization_asset_url` | character | Convenience CDN URL of the program's logo. |
| `organization_asset_key` | integer | On3 asset key of the program's logo asset. |
| `organization_asset_domain_override` | character | CDN domain override for the program's logo asset (usually null). |
| `organization_asset_domain` | character | CDN domain serving the program's logo asset. |
| `organization_asset_source_override` | character | Source-path override for the program's logo asset (usually null). |
| `organization_asset_source` | character | CDN-relative source path of the program's logo asset. |
| `organization_asset_title` | character | Editorial title attached to the program's logo asset. |
| `organization_asset_description` | character | Editorial description attached to the program's logo asset (usually null). |
| `organization_asset_caption` | character | Editorial caption attached to the program's logo asset (usually null). |
| `organization_asset_category` | character | Editorial category label of the program's logo asset (usually null). |
| `organization_asset_alt_text` | character | Accessibility alt text of the program's logo asset (usually null). |
| `organization_asset_height` | integer | Pixel height of the program's logo asset. |
| `organization_asset_width` | integer | Pixel width of the program's logo asset. |
| `organization_asset_asset_type` | character | On3 asset-type discriminator of the program's logo asset (e.g. Image). |
| `organization_asset_file_system` | character | Storage file-system flag of the program's logo asset. |
| `organization_asset_path` | character | Storage path of the program's logo asset. |
| `organization_asset_type` | character | Media type field of the program's logo asset (file extension, e.g. png). |
| `organization_asset_thumbnail` | character | Thumbnail variant of the program's logo asset (video assets; usually null). |
| `organization_asset_duration` | integer | Duration of the program's logo asset when it is a video (usually null or 0). |
| `organization_asset_mime_type` | character | MIME type of the program's logo asset. |
| `organization_slug` | character | URL slug of the program on On3. |
| `organization_primary_color` | character | Primary hex color of the program. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_team_ranking_team_rankings-example}

```python
on3_team_ranking_team_rankings(sport_slug='football', year=2025)
```

_Last validated n/a._
