---
title: "CFB — On3 Recruit Database (api.on3.com) — Other"
sidebar_label: "Other"
sidebar_position: 8
description: "CFB — On3 Recruit Database (api.on3.com) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB — On3 Recruit Database (api.on3.com) — Other

## on3_coaches_history

GET /rdb/v1/coaches/{personKey}/history

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/coaches/{person_key}/history`

**Valid URL:** [https://api.on3.com/public/rdb/v1/coaches/89617/history](https://api.on3.com/public/rdb/v1/coaches/89617/history)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_coaches_history-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_coaches_history-example}

```python
on3_coaches_history(person_key=89617)
```

_Last validated n/a._

## on3_coaches_profile

GET /rdb/v1/coaches/{personKey}/profile

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/coaches/{person_key}/profile`

**Valid URL:** [https://api.on3.com/public/rdb/v1/coaches/89617/profile](https://api.on3.com/public/rdb/v1/coaches/89617/profile)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_coaches_profile-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_coaches_profile-example}

```python
on3_coaches_profile(person_key=89617)
```

_Last validated n/a._

## on3_collective_groups

GET /rdb/v1/collective-groups

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
| `key` | integer | On3 RDB key for the NIL collective group. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `default_asset_key` | integer | On3 asset key for the collective's primary logo image. |
| `social_asset_key` | integer | On3 asset key for the collective's social-media image. |
| `organization_key` | integer | On3 organization key of the school the collective supports. |
| `launch_date` | character | Date the NIL collective launched. |
| `organization_type` | character | Organization type. |
| `twitter_handle` | character | Collective's Twitter/X account handle. |
| `instagram_handle` | character | Collective's Instagram account handle. |
| `tik_tok_handle` | character | Collective's TikTok account handle. |
| `youtube_handle` | character | Collective's YouTube channel handle. |
| `linked_in_handle` | character | Collective's LinkedIn account handle. |
| `website_name` | character | Display name of the collective's website. |
| `website_url` | character | URL of the collective's website. |
| `mission_statement` | character | Collective's stated mission, as published to On3. |
| `description` | character | ESPN's description of the stat. |
| `annual_goal_amount` | numeric | Collective's annual fundraising goal in dollars, as reported to On3. |
| `confirmed_raised_amount` | numeric | Dollar amount the collective has confirmed raising, per On3. |
| `merged_into_group_key` | integer | On3 key of the collective this group merged into, when applicable. |
| `merged_into_group` | character | Nested On3 record for the collective this group merged into (stringified). |
| `slug` | character | URL slug for the team. |
| `founders` | character | Founders of the collective, as a stringified list. |
| `sports` | character | Sports the collective funds, as a stringified list. |
| `default_asset_key_2` | integer |  |
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
| `social_asset_key_2` | integer |  |
| `social_asset_domain_override` | character |  |
| `social_asset_domain` | character |  |
| `social_asset_source_override` | character |  |
| `social_asset_source` | character |  |
| `social_asset_title` | character |  |
| `social_asset_description` | character |  |
| `social_asset_caption` | character |  |
| `social_asset_category` | character |  |
| `social_asset_alt_text` | character |  |
| `social_asset_height` | integer |  |
| `social_asset_width` | integer |  |
| `social_asset_asset_type` | character |  |
| `social_asset_file_system` | character |  |
| `social_asset_path` | character |  |
| `social_asset_type` | character |  |
| `social_asset_thumbnail` | character |  |
| `social_asset_duration` | integer |  |
| `social_asset_mime_type` | character |  |
| `organization_key_2` | integer |  |
| `organization_full_name` | character |  |
| `organization_name` | character |  |
| `organization_known_as` | character |  |
| `organization_mascot` | character |  |
| `organization_abbreviation` | character |  |
| `organization_asset_url` | character |  |
| `organization_default_asset_key` | integer |  |
| `organization_default_asset_domain_override` | character |  |
| `organization_default_asset_domain` | character |  |
| `organization_default_asset_source_override` | character |  |
| `organization_default_asset_source` | character |  |
| `organization_default_asset_title` | character |  |
| `organization_default_asset_description` | character |  |
| `organization_default_asset_caption` | character |  |
| `organization_default_asset_category` | character |  |
| `organization_default_asset_alt_text` | character |  |
| `organization_default_asset_height` | integer |  |
| `organization_default_asset_width` | integer |  |
| `organization_default_asset_asset_type` | character |  |
| `organization_default_asset_file_system` | character |  |
| `organization_default_asset_path` | character |  |
| `organization_default_asset_type` | character |  |
| `organization_default_asset_thumbnail` | character |  |
| `organization_default_asset_duration` | integer |  |
| `organization_default_asset_mime_type` | character |  |
| `organization_slug` | character |  |
| `organization_primary_color` | character |  |
| `organization_org_type` | character |  |
| `organization_org_type_enum` | character |  |
| `organization_division` | character |  |
| `organization_site_keys` | character |  |
| `organization_url_slug` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_collective_groups-example}

```python
on3_collective_groups()
```

_Last validated n/a._

## on3_collective_groups_deals

GET /rdb/v1/collective-groups/{key}/deals

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/collective-groups/{key}/deals`

**Valid URL:** [https://api.on3.com/public/rdb/v1/collective-groups/1/deals](https://api.on3.com/public/rdb/v1/collective-groups/1/deals)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_collective_groups_deals-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_collective_groups_deals-example}

```python
on3_collective_groups_deals(key=1)
```

_Last validated n/a._

## on3_collective_groups_key

GET /rdb/v1/collective-groups/{key}

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/collective-groups/{key}`

**Valid URL:** [https://api.on3.com/public/rdb/v1/collective-groups/1](https://api.on3.com/public/rdb/v1/collective-groups/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#on3_collective_groups_key-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_collective_groups_key-example}

```python
on3_collective_groups_key(key=1)
```

_Last validated n/a._

## on3_draft_organization_rank

GET /rdb/v1/draft-organization-rank

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/draft-organization-rank`

**Valid URL:** [https://api.on3.com/public/rdb/v1/draft-organization-rank](https://api.on3.com/public/rdb/v1/draft-organization-rank)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_draft_organization_rank-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_draft_organization_rank-example}

```python
on3_draft_organization_rank()
```

_Last validated n/a._

## on3_draft_pick_organization_rank

GET /rdb/v1/draft-pick-organization-rank

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/draft-pick-organization-rank`

**Valid URL:** [https://api.on3.com/public/rdb/v1/draft-pick-organization-rank](https://api.on3.com/public/rdb/v1/draft-pick-organization-rank)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_draft_pick_organization_rank-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_draft_pick_organization_rank-example}

```python
on3_draft_pick_organization_rank()
```

_Last validated n/a._

## on3_drafts

GET /rdb/v1/drafts

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/drafts`

**Valid URL:** [https://api.on3.com/public/rdb/v1/drafts](https://api.on3.com/public/rdb/v1/drafts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `round` | `round` |  |  | `Y` | round query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |

### Returns {#on3_drafts-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_drafts-example}

```python
on3_drafts()
```

_Last validated n/a._

## on3_drafts_by_stars

GET /rdb/v1/drafts-by-stars

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/drafts-by-stars`

**Valid URL:** [https://api.on3.com/public/rdb/v1/drafts-by-stars](https://api.on3.com/public/rdb/v1/drafts-by-stars)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `yearSpan` | `year_span` |  |  | `Y` | yearSpan query parameter. |

### Returns {#on3_drafts_by_stars-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `blue_chip_percent` | numeric | Percent of the drafted group who were blue-chip (four- or five-star) recruits. |
| `population_percent` | numeric | Percent of the overall recruit population holding this star rating. |
| `talent_ratio` | numeric | Ratio of the star tier's draft share to its population share (On3's talent ratio). |
| `five_stars` | integer | Number of drafted players who were five-star recruits. |
| `four_stars` | integer | Number of drafted players who were four-star recruits. |
| `three_stars` | integer | Number of drafted players who were three-star recruits. |
| `zero_stars` | integer | Number of drafted players who were unrated (zero-star) recruits. |
| `total` | integer | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `state_key` | integer |  |
| `state_name` | character |  |
| `state_abbreviation` | character |  |
| `state_country_key` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_drafts_by_stars-example}

```python
on3_drafts_by_stars()
```

_Last validated n/a._

## on3_drafts_by_stars_summary

GET /rdb/v1/drafts-by-stars-summary

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/drafts-by-stars-summary`

**Valid URL:** [https://api.on3.com/public/rdb/v1/drafts-by-stars-summary](https://api.on3.com/public/rdb/v1/drafts-by-stars-summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |

### Returns {#on3_drafts_by_stars_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_drafts_by_stars_summary-example}

```python
on3_drafts_by_stars_summary()
```

_Last validated n/a._

## on3_drafts_players

GET /rdb/v1/drafts/{orgKey}/players

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/drafts/{org_key}/players`

**Valid URL:** [https://api.on3.com/public/rdb/v1/drafts/1867/players](https://api.on3.com/public/rdb/v1/drafts/1867/players)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `org_key` | `org_key` |  | `Y` |  | org_key path parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |

### Returns {#on3_drafts_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_drafts_players-example}

```python
on3_drafts_players(org_key=1867)
```

_Last validated n/a._

## on3_filters_conferences

GET /rdb/v1/filters/conferences

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/conferences`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/conferences](https://api.on3.com/public/rdb/v1/filters/conferences)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | year query parameter. |
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |

### Returns {#on3_filters_conferences-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the conference filter option. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `abbreviation` | character | Metric abbreviation. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_filters_conferences-example}

```python
on3_filters_conferences()
```

_Last validated n/a._

## on3_filters_draft_rounds

GET /rdb/v1/filters/draft-rounds

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/draft-rounds`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/draft-rounds](https://api.on3.com/public/rdb/v1/filters/draft-rounds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | year query parameter. |
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |

### Returns {#on3_filters_draft_rounds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `round` | integer | Round of NFL draft the draftee was picked in. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_filters_draft_rounds-example}

```python
on3_filters_draft_rounds()
```

_Last validated n/a._

## on3_filters_positions

GET /rdb/v1/filters/positions

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/positions`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/positions](https://api.on3.com/public/rdb/v1/filters/positions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `positionType` | `position_type` |  |  | `Y` | positionType query parameter. |

### Returns {#on3_filters_positions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the position filter option. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `abbreviation` | character | Metric abbreviation. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_filters_positions-example}

```python
on3_filters_positions()
```

_Last validated n/a._

## on3_filters_sports

GET /rdb/v1/filters/sports

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/sports`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/sports](https://api.on3.com/public/rdb/v1/filters/sports)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#on3_filters_sports-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the sport filter option. |
| `name` | character | Position name (e.g. `Quarterback`). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_filters_sports-example}

```python
on3_filters_sports()
```

_Last validated n/a._

## on3_filters_status

GET /rdb/v1/filters/status

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/status`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/status](https://api.on3.com/public/rdb/v1/filters/status)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#on3_filters_status-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `value` | character | Metric value. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_filters_status-example}

```python
on3_filters_status()
```

_Last validated n/a._

## on3_filters_teams

GET /rdb/v1/filters/teams

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/teams`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/teams](https://api.on3.com/public/rdb/v1/filters/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `groupBy` | `group_by` |  |  | `Y` | groupBy query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |

### Returns {#on3_filters_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `conference_key` | integer | On3 RDB key of the conference the team belongs to. |
| `conference_abbr` | character | Conference abbreviation. |
| `teams` | character | Nested list of member-team membership spans. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_filters_teams-example}

```python
on3_filters_teams()
```

_Last validated n/a._

## on3_filters_years

GET /rdb/v1/filters/years

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/years`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/years](https://api.on3.com/public/rdb/v1/filters/years)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#on3_filters_years-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `value` | character | Metric value. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_filters_years-example}

```python
on3_filters_years()
```

_Last validated n/a._

## on3_person_connections_connection_key

GET /rdb/v1/person-connections/{connectionKey}

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/person-connections/{connection_key}`

**Valid URL:** [https://api.on3.com/public/rdb/v1/person-connections/89617](https://api.on3.com/public/rdb/v1/person-connections/89617)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `connection_key` | `connection_key` |  | `Y` |  | connection_key path parameter. |

### Returns {#on3_person_connections_connection_key-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_person_connections_connection_key-example}

```python
on3_person_connections_connection_key(connection_key=89617)
```

_Last validated n/a._

## on3_person_primary_recruitment_evaluation

GET /rdb/v1/person/{personKey}/primary-recruitment-evaluation

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/person/{person_key}/primary-recruitment-evaluation`

**Valid URL:** [https://api.on3.com/public/rdb/v1/person/89617/primary-recruitment-evaluation](https://api.on3.com/public/rdb/v1/person/89617/primary-recruitment-evaluation)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_person_primary_recruitment_evaluation-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the scouting evaluation. |
| `recruitment_key` | integer | On3 recruitment key the evaluation is attached to. |
| `author_key` | integer | On3 user key of the evaluation's author. |
| `author_name` | character | Name of the On3 scout who wrote the evaluation. |
| `author_title` | character | Job title of the On3 scout who wrote the evaluation. |
| `title` | character | Specific role title for the assignment. |
| `premium` | logical | Whether the article is premium content. |
| `body` | character | Full text of the scouting evaluation. |
| `primary` | logical | Whether this is the primary (featured) evaluation for the recruitment. |
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |
| `date_updated_unix` | integer | Unix timestamp of the evaluation's last update. |
| `date_added` | character | Date the evaluation was added. |
| `date_updated` | character | Date the evaluation was last updated. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_person_primary_recruitment_evaluation-example}

```python
on3_person_primary_recruitment_evaluation(person_key=89617)
```

_Last validated n/a._

## on3_person_recruitment_evaluations

GET /rdb/v1/person/{personKey}/recruitment-evaluations

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/person/{person_key}/recruitment-evaluations`

**Valid URL:** [https://api.on3.com/public/rdb/v1/person/89617/recruitment-evaluations](https://api.on3.com/public/rdb/v1/person/89617/recruitment-evaluations)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_person_recruitment_evaluations-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the scouting evaluation. |
| `recruitment_key` | integer | On3 recruitment key the evaluation is attached to. |
| `author_key` | integer | On3 user key of the evaluation's author. |
| `author_name` | character | Name of the On3 scout who wrote the evaluation. |
| `author_title` | character | Job title of the On3 scout who wrote the evaluation. |
| `title` | character | Specific role title for the assignment. |
| `premium` | logical | Whether the article is premium content. |
| `body` | character | Full text of the scouting evaluation. |
| `primary` | logical | Whether this is the primary (featured) evaluation for the recruitment. |
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |
| `date_updated_unix` | integer | Unix timestamp of the evaluation's last update. |
| `date_added` | character | Date the evaluation was added. |
| `date_updated` | character | Date the evaluation was last updated. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_person_recruitment_evaluations-example}

```python
on3_person_recruitment_evaluations(person_key=89617)
```

_Last validated n/a._

## on3_person_sport_profile_recruit

GET /rdb/v1/person-sport/{psKey}/profile-recruit

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/person-sport/{ps_key}/profile-recruit`

**Valid URL:** [https://api.on3.com/public/rdb/v1/person-sport/89617/profile-recruit](https://api.on3.com/public/rdb/v1/person-sport/89617/profile-recruit)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ps_key` | `ps_key` |  | `Y` |  | ps_key path parameter. |

### Returns {#on3_person_sport_profile_recruit-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_person_sport_profile_recruit-example}

```python
on3_person_sport_profile_recruit(ps_key=89617)
```

_Last validated n/a._

## on3_person_sport_rankings

GET /rdb/v1/person-sport-rankings

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/person-sport-rankings`

**Valid URL:** [https://api.on3.com/public/rdb/v1/person-sport-rankings](https://api.on3.com/public/rdb/v1/person-sport-rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_person_sport_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_person_sport_rankings-example}

```python
on3_person_sport_rankings()
```

_Last validated n/a._

## on3_predictions_user_key

Expert prediction accuracy + feed (see PredictionAccuracies)

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/predictions/{user_key}`

**Valid URL:** [https://api.on3.com/public/rdb/v1/predictions/89617](https://api.on3.com/public/rdb/v1/predictions/89617)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `user_key` | `user_key` |  | `Y` |  | user_key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_predictions_user_key-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_predictions_user_key-example}

```python
on3_predictions_user_key(user_key=89617)
```

_Last validated n/a._

## on3_quotes

GET /rdb/v1/quotes

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/quotes`

**Valid URL:** [https://api.on3.com/public/rdb/v1/quotes](https://api.on3.com/public/rdb/v1/quotes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_quotes-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the quote record. |
| `body` | character | Full text of the quote. |
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |
| `person_key` | integer | On3 person key of the person quoted or quoted about. |
| `date_added` | character | Date the quote was added to the On3 database. |
| `date_updated` | character | Date the quote was last updated. |
| `person_key_2` | integer |  |
| `person_known_as_name` | character |  |
| `person_first_name` | character | Player first name. |
| `person_last_name` | character | Player last name. |
| `person_twitter_handle` | character |  |
| `person_instagram_profile` | character |  |
| `person_tik_tok_handle` | character |  |
| `person_espn_profile` | character |  |
| `person_class_year` | integer |  |
| `person_two_four_seven_profile` | character |  |
| `person_rivals_profile` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_quotes-example}

```python
on3_quotes()
```

_Last validated n/a._

## on3_quotes_key

GET /rdb/v1/quotes/{key}

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/quotes/{key}`

**Valid URL:** [https://api.on3.com/public/rdb/v1/quotes/1](https://api.on3.com/public/rdb/v1/quotes/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#on3_quotes_key-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_quotes_key-example}

```python
on3_quotes_key(key=1)
```

_Last validated n/a._

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
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
| `year` | integer | Four-digit season year (e.g. 2019). |
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
| `organization_key` | integer |  |
| `organization_full_name` | character |  |
| `organization_name` | character |  |
| `organization_mascot` | character |  |
| `organization_abbreviation` | character |  |
| `organization_asset_url` | character |  |
| `organization_asset_key` | integer |  |
| `organization_asset_domain_override` | character |  |
| `organization_asset_domain` | character |  |
| `organization_asset_source_override` | character |  |
| `organization_asset_source` | character |  |
| `organization_asset_title` | character |  |
| `organization_asset_description` | character |  |
| `organization_asset_caption` | character |  |
| `organization_asset_category` | character |  |
| `organization_asset_alt_text` | character |  |
| `organization_asset_height` | integer |  |
| `organization_asset_width` | integer |  |
| `organization_asset_asset_type` | character |  |
| `organization_asset_file_system` | character |  |
| `organization_asset_path` | character |  |
| `organization_asset_type` | character |  |
| `organization_asset_thumbnail` | character |  |
| `organization_asset_duration` | integer |  |
| `organization_asset_mime_type` | character |  |
| `organization_slug` | character |  |
| `organization_primary_color` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_team_ranking_team_rankings-example}

```python
on3_team_ranking_team_rankings(sport_slug='football', year=2025)
```

_Last validated n/a._

## on3_videos_video_key

GET /rdb/v1/videos/{videoKey}

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/videos/{video_key}`

**Valid URL:** [https://api.on3.com/public/rdb/v1/videos/1](https://api.on3.com/public/rdb/v1/videos/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `video_key` | `video_key` |  | `Y` |  | video_key path parameter. |

### Returns {#on3_videos_video_key-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_videos_video_key-example}

```python
on3_videos_video_key(video_key=1)
```

_Last validated n/a._
