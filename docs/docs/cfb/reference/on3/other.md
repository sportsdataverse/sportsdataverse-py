---
title: "CFB — On3 Recruit Database (api.on3.com) — Other"
sidebar_label: "Other"
sidebar_position: 7
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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `start_pso_key` | integer | On3 player-sport-organization (PSO) key for the first season of the coaching stint. |
| `latest_pso_key` | integer | On3 player-sport-organization (PSO) key for the most recent season of the stint. |
| `start_year` | integer | Span starting year. |
| `latest_year` | integer | Most recent year of the coaching stint. |
| `fired` | logical | Whether the stint ended with the coach being fired. |
| `promoted` | logical | Whether the stint ended with the coach being promoted within the organization. |
| `resigned` | logical | Whether the stint ended with the coach resigning. |
| `end_of_team` | logical | On3 RDB flag that the stint ended because the team or program itself ended. |
| `deceased` | logical | Whether the player is deceased. |
| `is_present` | logical | Whether this is the coach's current (ongoing) stint. |
| `organization` | character | Organization. |
| `position` | character | Athlete position. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `salary` | numeric | Total cap-counting salary for the season ($). |
| `age` | integer | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `high_school_name` | character | Recruit high-school name. |
| `home_town_name` | character | Coach's hometown, as listed by On3. |
| `description` | character | ESPN's description of the stat. |
| `alma_mater` | character | School the coach graduated from. |
| `alma_mater_class_year` | integer | Coach's graduating class year at their alma mater. |
| `degree` | character | Degree the coach earned, when listed. |
| `key` | integer | On3 RDB key for the coach profile. |
| `first_name` | character | Athlete first name. |
| `last_name` | character | Athlete last name. |
| `known_as_name` | character | Name the coach publicly goes by, when it differs from the legal name. |
| `full_name` | character | Venue full name (e.g. `Tenney Stadium`). |
| `slug` | character | URL slug for the team. |
| `default_asset` | character | Nested On3 asset object for the coach's headshot (stringified). |
| `organization` | character | Organization. |
| `primary_position` | character | Nested On3 object for the coach's primary coaching role (stringified). |
| `org_season_count` | integer | Number of seasons the coach has spent with the current organization. |
| `years_active` | integer | Span of years the coach has been active, per On3. |

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
| `default_asset` | character | Nested On3 asset object for the collective's primary logo (stringified). |
| `social_asset_key` | integer | On3 asset key for the collective's social-media image. |
| `social_asset` | character | Nested On3 asset object for the collective's social-media image (stringified). |
| `organization_key` | integer | On3 organization key of the school the collective supports. |
| `organization` | character | Organization. |
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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the NIL deal record. |
| `person` | character | Nested On3 person object for the athlete in the deal (stringified). |
| `company` | character | Nested On3 record for the company on the other side of the NIL deal (stringified). |
| `agent` | character | Listed player agent. |
| `collective_group` | character | Nested On3 record for the collective brokering the deal (stringified). |
| `amount` | numeric | Reported dollar amount of the NIL deal. |
| `date` | character | Date of the NIL collective deal, per On3. |
| `verified` | logical | Whether On3 verified the deal. |
| `source_url` | character | URL of the source reporting the deal. |
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `roster_rating` | character | Nested On3 roster rating object for the athlete at deal time (stringified). |
| `status` | character | Game status (e.g. "scheduled", "in_progress", "completed"). |
| `rpm` | character | Nested On3 Recruiting Prediction Machine (RPM) data for the athlete (stringified). |
| `nil_status` | character | Status of the athlete's On3 NIL valuation (e.g. active, inactive). |
| `nil_value` | integer | Athlete's On3 NIL valuation in dollars. |
| `detail` | character | Detailed status text. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the NIL collective group. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `default_asset_key` | integer | On3 asset key for the collective's primary logo image. |
| `default_asset` | character | Nested On3 asset object for the collective's primary logo (stringified). |
| `social_asset_key` | integer | On3 asset key for the collective's social-media image. |
| `social_asset` | character | Nested On3 asset object for the collective's social-media image (stringified). |
| `organization_key` | integer | On3 organization key of the school the collective supports. |
| `organization` | character | Organization. |
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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_collective_groups_key-example}

```python
on3_collective_groups_key(key=1)
```

_Last validated n/a._

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
| `default_asset` | character | Nested primary media asset (headshot) object for the recruit. |
| `position_abbreviation` | character | Abbreviated primary position of the recruit. |
| `height` | character | Recruit height (formatted string, e.g. "6-2"). |
| `weight` | numeric | Recruit weight in pounds. |
| `rating` | character | On3 rating for the recruit. |
| `roster_rating` | character | On3 roster (transfer-portal-adjusted) rating for the recruit. |
| `commit_status` | character | Nested commitment status (committed organization, dates, flags) for the recruit. |
| `predictions` | character | List of recruiting-prediction entries (RPM picks) for the recruit. |
| `nil_status` | character | Nested NIL (name/image/likeness) status object for the recruit. |
| `nil_value` | numeric | On3 NIL valuation for the recruit (US dollars). |
| `sport` | character | Nested sport object (key/name/slug) the ranking pertains to. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `status_type` | character | Status type. |
| `commits` | character | Number of commitments in the organization's latest recruiting class. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `status_type` | character | Status type. |
| `commits` | character | Number of commitments in the organization's recruiting class for the season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_commits_organizations_org_key-example}

```python
on3_commits_organizations_org_key(org_key=1867)
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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_filters_years-example}

```python
on3_filters_years()
```

_Last validated n/a._

## on3_nil_100

GET /rdb/v1/nil-100

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/nil-100`

**Valid URL:** [https://api.on3.com/public/rdb/v1/nil-100](https://api.on3.com/public/rdb/v1/nil-100)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | year query parameter. |

### Returns {#on3_nil_100-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `person` | character | Nested On3 person object for the ranked athlete (stringified). |
| `valuation` | character | Athlete's On3 NIL valuation in dollars. |

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
| `person` | character | Nested On3 person object for the ranked athlete (stringified). |
| `valuation` | character | Athlete's On3 NIL valuation in dollars. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the state NIL-compliance record. |
| `organization_type` | character | Organization type. |
| `state_key` | integer | On3 key of the U.S. state the compliance record covers. |
| `state` | character | U.S. state whose NIL compliance rules the record describes. |
| `monetization_allowed` | logical | Whether the state allows high-school athletes to monetize their NIL. |
| `governing_rule_label` | character | Name of the governing body or rule for NIL in the state. |
| `governing_rule_url` | character | URL of the governing NIL rule or policy document. |
| `current_rules` | character | Text of the state's current NIL rules, as tracked by On3. |

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
| `person` | character | Nested On3 person object for the ranked athlete (stringified). |
| `valuation` | character | Athlete's On3 NIL valuation in dollars. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_nil_rankings-example}

```python
on3_nil_rankings()
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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the person-connection record. |
| `person_key` | integer | On3 person key of the profile the connection belongs to. |
| `connected_person_key` | integer | On3 person key of the connected person. |
| `sport_key` | integer | On3 sport key the connection is scoped to. |
| `description` | character | ESPN's description of the stat. |
| `connected_person_sport` | character | Nested athlete-sport profile of the connected person (stringified). |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the player's ranking row. |
| `ranking_key` | integer | On3 key of the ranking cycle the row belongs to. |
| `rating` | numeric | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `state_rank` | integer | State ranking. |
| `state_abbr` | character | Two-letter abbreviation of the player's home state. |
| `position_rank` | integer | Position ranking. |
| `position_abbr` | character | Position abbreviation. |
| `overall_rank` | integer | Overall recruit ranking (top recruits only; may be `NA`). |
| `stars` | integer | Recruit star rating on the 247Sports scale (2-5). |
| `change` | character | Rank movement since the previous ranking cycle. |
| `consensus_rating` | numeric | Player's industry-consensus rating (blend of the major recruiting services). |
| `consensus_state_rank` | integer | Player's consensus rank within their home state. |
| `consensus_position_rank` | integer | Player's consensus rank at their position. |
| `consensus_overall_rank` | integer | Player's national consensus rank. |
| `consensus_stars` | integer | Player's star rating under the industry consensus. |
| `consensus_change` | character | Consensus rank movement since the previous cycle. |
| `strength` | integer | Strength label (Even, Power Play, Shorthanded). |
| `five_star_plus` | logical | Whether On3 designates the player a Five-Star Plus+ prospect. |
| `ranking_type` | character | Poll type code (e.g. `ap`, `coaches`, `cfp`). |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `ratings` | character | Athlete's ratings across ranking cycles, as a stringified list. |
| `person` | character | Nested On3 person object for the ranked athlete (stringified). |
| `nil_value` | integer | Athlete's On3 NIL valuation in dollars. |

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
| `person` | character | Nested On3 person object for the quote's subject (stringified). |
| `date_added` | character | Date the quote was added to the On3 database. |
| `date_updated` | character | Date the quote was last updated. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the quote record. |
| `body` | character | Full text of the quote. |
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |
| `person_key` | integer | On3 person key of the person quoted or quoted about. |
| `person` | character | Nested On3 person object for the quote's subject (stringified). |
| `date_added` | character | Date the quote was added to the On3 database. |
| `date_updated` | character | Date the quote was last updated. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_quotes_key-example}

```python
on3_quotes_key(key=1)
```

_Last validated n/a._

## on3_transfers_best_available

GET /rdb/v1/transfers/best-available

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/transfers/best-available`

**Valid URL:** [https://api.on3.com/public/rdb/v1/transfers/best-available](https://api.on3.com/public/rdb/v1/transfers/best-available)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `orgKey` | `org_key` |  |  | `Y` | orgKey query parameter. |
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `positionAbbr` | `position_abbr` |  |  | `Y` | positionAbbr query parameter. |
| `status` | `status` |  |  | `Y` | status query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `cutoff` | `cutoff` |  |  | `Y` | cutoff query parameter. |
| `orderBy` | `order_by` |  |  | `Y` | orderBy query parameter. |

### Returns {#on3_transfers_best_available-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `eligibility` | character | Eligibility status. |
| `last_team` | character | Nested On3 record for the team the player is transferring from (stringified). |
| `entered_article` | character | On3 article link covering the player entering the transfer portal (stringified). |
| `committed_article` | character | On3 article link covering the player's transfer commitment (stringified). |
| `exited_article` | character | On3 article link covering the player exiting the portal (stringified). |
| `key` | integer | On3 RDB key for the transfer entry. |
| `recruitment_key` | integer | On3 recruitment key of the player's transfer recruitment. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `slug` | character | URL slug for the team. |
| `high_school_name` | character | Recruit high-school name. |
| `home_town_name` | character | Player's hometown, as listed by On3. |
| `early_enrollee` | logical | Whether the player is an early enrollee. |
| `early_signee` | logical | Whether the player signed in the early signing period. |
| `default_asset_url` | character | URL of the player's headshot image. |
| `class_year` | integer | Player's original recruiting class year. |
| `athlete_verified` | logical | Whether the athlete has verified their own On3 profile. |
| `prospect_verified` | logical | Whether On3 has verified the prospect's profile information. |
| `default_asset` | character | Nested On3 asset object for the player's headshot (stringified). |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`); `position_detail = TRUE` only. |
| `height` | character | Listed height (inches). |
| `weight` | numeric | Listed weight (lbs). |
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `roster_rating` | character | Nested On3 roster rating object for the player (stringified). |
| `commit_status` | character | Nested commitment status of the transfer recruitment (stringified). |
| `predictions` | character | RPM prediction entries for the transfer destination, as a stringified list. |
| `nil_status` | character | Status of the player's On3 NIL valuation (e.g. active, inactive). |
| `nil_value` | numeric | Player's On3 NIL valuation in dollars. |
| `sport` | character | Nested On3 sport object for the transfer entry (stringified). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_transfers_best_available-example}

```python
on3_transfers_best_available()
```

_Last validated n/a._

## on3_transfers_latest

GET /rdb/v1/transfers/latest

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/transfers/latest`

**Valid URL:** [https://api.on3.com/public/rdb/v1/transfers/latest](https://api.on3.com/public/rdb/v1/transfers/latest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `orgKey` | `org_key` |  |  | `Y` | orgKey query parameter. |
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `positionAbbr` | `position_abbr` |  |  | `Y` | positionAbbr query parameter. |
| `status` | `status` |  |  | `Y` | status query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#on3_transfers_latest-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `eligibility` | character | Eligibility status. |
| `last_team` | character | Nested On3 record for the team the player is transferring from (stringified). |
| `entered_article` | character | On3 article link covering the player entering the transfer portal (stringified). |
| `committed_article` | character | On3 article link covering the player's transfer commitment (stringified). |
| `exited_article` | character | On3 article link covering the player exiting the portal (stringified). |
| `key` | integer | On3 RDB key for the transfer entry. |
| `recruitment_key` | integer | On3 recruitment key of the player's transfer recruitment. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `slug` | character | URL slug for the team. |
| `high_school_name` | character | Recruit high-school name. |
| `home_town_name` | character | Player's hometown, as listed by On3. |
| `early_enrollee` | logical | Whether the player is an early enrollee. |
| `early_signee` | logical | Whether the player signed in the early signing period. |
| `default_asset_url` | character | URL of the player's headshot image. |
| `class_year` | integer | Player's original recruiting class year. |
| `athlete_verified` | logical | Whether the athlete has verified their own On3 profile. |
| `prospect_verified` | logical | Whether On3 has verified the prospect's profile information. |
| `default_asset` | character | Nested On3 asset object for the player's headshot (stringified). |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`); `position_detail = TRUE` only. |
| `height` | character | Listed height (inches). |
| `weight` | numeric | Listed weight (lbs). |
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `roster_rating` | character | Nested On3 roster rating object for the player (stringified). |
| `commit_status` | character | Nested commitment status of the transfer recruitment (stringified). |
| `predictions` | character | RPM prediction entries for the transfer destination, as a stringified list. |
| `nil_status` | character | Status of the player's On3 NIL valuation (e.g. active, inactive). |
| `nil_value` | numeric | Player's On3 NIL valuation in dollars. |
| `sport` | character | Nested On3 sport object for the transfer entry (stringified). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_transfers_latest-example}

```python
on3_transfers_latest()
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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the video record. |
| `person_key` | integer | On3 person key of the featured athlete. |
| `source_url` | character | Source URL of the hosted video. |
| `title` | character | Specific role title for the assignment. |
| `thumbnail` | character | URL of the video's thumbnail image. |
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |
| `description` | character | ESPN's description of the stat. |
| `person_sport` | character | Nested athlete-sport profile the video is attached to (stringified). |
| `is_featured` | logical | Whether the video is featured on the player's On3 profile. |
| `date` | integer | Publication date of the video, per On3. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_videos_video_key-example}

```python
on3_videos_video_key(video_key=1)
```

_Last validated n/a._
