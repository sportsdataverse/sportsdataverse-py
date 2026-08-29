---
title: CFB — On3 Recruit Database (api.on3.com)
sidebar_label: On3 Recruit Database (api.on3.com)
description: "CFB — On3 Recruit Database (api.on3.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
---
# CFB — On3 Recruit Database (api.on3.com)

`sportsdataverse.cfb` — 78 endpoints.

## `on3_coaches_history`

GET /rdb/v1/coaches/{personKey}/history

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/coaches/{person_key}/history`

**Valid URL:** [https://api.on3.com/public/rdb/v1/coaches/89617/history](https://api.on3.com/public/rdb/v1/coaches/89617/history)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `start_pso_key` | integer |  |
| `latest_pso_key` | integer |  |
| `start_year` | integer | Span starting year. |
| `latest_year` | integer |  |
| `fired` | logical |  |
| `promoted` | logical |  |
| `resigned` | logical |  |
| `end_of_team` | logical |  |
| `deceased` | logical | Whether the player is deceased. |
| `is_present` | logical |  |
| `organization` | character | Organization. |
| `position` | character | Athlete position. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_coaches_history(person_key=89617)
```

_Last validated n/a._

## `on3_coaches_profile`

GET /rdb/v1/coaches/{personKey}/profile

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/coaches/{person_key}/profile`

**Valid URL:** [https://api.on3.com/public/rdb/v1/coaches/89617/profile](https://api.on3.com/public/rdb/v1/coaches/89617/profile)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `salary` | numeric | Total cap-counting salary for the season ($). |
| `age` | integer | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `high_school_name` | character | Recruit high-school name. |
| `home_town_name` | character |  |
| `description` | character | ESPN's description of the stat. |
| `alma_mater` | character |  |
| `alma_mater_class_year` | integer |  |
| `degree` | character |  |
| `key` | integer |  |
| `first_name` | character | Athlete first name. |
| `last_name` | character | Athlete last name. |
| `known_as_name` | character |  |
| `full_name` | character | Venue full name (e.g. `Tenney Stadium`). |
| `slug` | character | URL slug for the team. |
| `default_asset` | character |  |
| `organization` | character | Organization. |
| `primary_position` | character |  |
| `org_season_count` | integer |  |
| `years_active` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_coaches_profile(person_key=89617)
```

_Last validated n/a._

## `on3_collective_groups`

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

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `name` | character | Position name (e.g. `Quarterback`). |
| `default_asset_key` | integer |  |
| `default_asset` | character |  |
| `social_asset_key` | integer |  |
| `social_asset` | character |  |
| `organization_key` | integer |  |
| `organization` | character | Organization. |
| `launch_date` | character |  |
| `organization_type` | character | Organization type. |
| `twitter_handle` | character |  |
| `instagram_handle` | character |  |
| `tik_tok_handle` | character |  |
| `youtube_handle` | character |  |
| `linked_in_handle` | character |  |
| `website_name` | character |  |
| `website_url` | character |  |
| `mission_statement` | character |  |
| `description` | character | ESPN's description of the stat. |
| `annual_goal_amount` | numeric |  |
| `confirmed_raised_amount` | numeric |  |
| `merged_into_group_key` | integer |  |
| `merged_into_group` | character |  |
| `slug` | character | URL slug for the team. |
| `founders` | character |  |
| `sports` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_collective_groups()
```

_Last validated n/a._

## `on3_collective_groups_deals`

GET /rdb/v1/collective-groups/{key}/deals

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/collective-groups/{key}/deals`

**Valid URL:** [https://api.on3.com/public/rdb/v1/collective-groups/1/deals](https://api.on3.com/public/rdb/v1/collective-groups/1/deals)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `person` | character |  |
| `company` | character |  |
| `agent` | character | Listed player agent. |
| `collective_group` | character |  |
| `amount` | numeric |  |
| `date` | character | Date of the poll release. |
| `verified` | logical |  |
| `source_url` | character |  |
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `roster_rating` | character |  |
| `status` | character | Game status (e.g. "scheduled", "in_progress", "completed"). |
| `rpm` | character |  |
| `nil_status` | character |  |
| `nil_value` | integer |  |
| `detail` | character | Detailed status text. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_collective_groups_deals(key=1)
```

_Last validated n/a._

## `on3_collective_groups_key`

GET /rdb/v1/collective-groups/{key}

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/collective-groups/{key}`

**Valid URL:** [https://api.on3.com/public/rdb/v1/collective-groups/1](https://api.on3.com/public/rdb/v1/collective-groups/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `name` | character | Position name (e.g. `Quarterback`). |
| `default_asset_key` | integer |  |
| `default_asset` | character |  |
| `social_asset_key` | integer |  |
| `social_asset` | character |  |
| `organization_key` | integer |  |
| `organization` | character | Organization. |
| `launch_date` | character |  |
| `organization_type` | character | Organization type. |
| `twitter_handle` | character |  |
| `instagram_handle` | character |  |
| `tik_tok_handle` | character |  |
| `youtube_handle` | character |  |
| `linked_in_handle` | character |  |
| `website_name` | character |  |
| `website_url` | character |  |
| `mission_statement` | character |  |
| `description` | character | ESPN's description of the stat. |
| `annual_goal_amount` | numeric |  |
| `confirmed_raised_amount` | numeric |  |
| `merged_into_group_key` | integer |  |
| `merged_into_group` | character |  |
| `slug` | character | URL slug for the team. |
| `founders` | character |  |
| `sports` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_collective_groups_key(key=1)
```

_Last validated n/a._

## `on3_commits_latest`

GET /rdb/v1/commits/latest

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/commits/latest`

**Valid URL:** [https://api.on3.com/public/rdb/v1/commits/latest](https://api.on3.com/public/rdb/v1/commits/latest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns

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

### Example

```python
on3_commits_latest()
```

_Last validated n/a._

## `on3_commits_organizations_latest_commits`

GET /rdb/v1/commits/organizations/{orgKey}/latest-commits

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/commits/organizations/{org_key}/latest-commits`

**Valid URL:** [https://api.on3.com/public/rdb/v1/commits/organizations/1867/latest-commits](https://api.on3.com/public/rdb/v1/commits/organizations/1867/latest-commits)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `org_key` | `org_key` |  | `Y` |  | org_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `status_type` | character | Status type. |
| `commits` | character | Number of commits in the position group. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_commits_organizations_latest_commits(org_key=1867)
```

_Last validated n/a._

## `on3_commits_organizations_org_key`

GET /rdb/v1/commits/organizations/{orgKey}

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/commits/organizations/{org_key}`

**Valid URL:** [https://api.on3.com/public/rdb/v1/commits/organizations/1867](https://api.on3.com/public/rdb/v1/commits/organizations/1867)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `org_key` | `org_key` |  | `Y` |  | org_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `status_type` | character | Status type. |
| `commits` | character | Number of commits in the position group. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_commits_organizations_org_key(org_key=1867)
```

_Last validated n/a._

## `on3_draft_organization_rank`

GET /rdb/v1/draft-organization-rank

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/draft-organization-rank`

**Valid URL:** [https://api.on3.com/public/rdb/v1/draft-organization-rank](https://api.on3.com/public/rdb/v1/draft-organization-rank)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `organization` | character | Organization. |
| `rank` | integer | Position of the school within the poll for the given week (1 = top-ranked). |
| `five_stars` | integer |  |
| `four_stars` | integer |  |
| `three_stars` | integer | Whether three stars data is available. |
| `total` | integer | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `percent_drafted` | numeric |  |
| `draft_rate` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_draft_organization_rank()
```

_Last validated n/a._

## `on3_draft_pick_organization_rank`

GET /rdb/v1/draft-pick-organization-rank

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/draft-pick-organization-rank`

**Valid URL:** [https://api.on3.com/public/rdb/v1/draft-pick-organization-rank](https://api.on3.com/public/rdb/v1/draft-pick-organization-rank)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `organization` | character | Organization. |
| `rank` | integer | Position of the school within the poll for the given week (1 = top-ranked). |
| `first_round` | integer |  |
| `second_round` | integer |  |
| `third_round` | integer |  |
| `fourth_through_seventh_round` | integer |  |
| `total` | integer | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_draft_pick_organization_rank()
```

_Last validated n/a._

## `on3_drafts`

GET /rdb/v1/drafts

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/drafts`

**Valid URL:** [https://api.on3.com/public/rdb/v1/drafts](https://api.on3.com/public/rdb/v1/drafts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `round` | `round` |  |  | `Y` | round query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `recruitment_key` | integer |  |
| `organization` | character | Organization. |
| `drafted_from_organization` | character |  |
| `high_school_organization` | character |  |
| `college_organization` | character |  |
| `hometown` | character | Prospect hometown. |
| `state` | character | Venue state. |
| `pick` | integer | Pick number of the NFL draftee within the round they were picked in. |
| `compensatory` | logical |  |
| `supplementary` | logical |  |
| `traded` | logical | Whether the pick was traded. |
| `forfeited` | logical |  |
| `trading_organization` | character |  |
| `through_organization_one` | character |  |
| `through_organization_two` | character |  |
| `through_organization_three` | character |  |
| `key` | integer |  |
| `round` | integer | Round of NFL draft the draftee was picked in. |
| `overall_pick` | integer | Overall pick number in the draft. |
| `person` | character |  |
| `position` | character | Athlete position. |
| `age` | numeric | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_drafts()
```

_Last validated n/a._

## `on3_drafts_by_stars`

GET /rdb/v1/drafts-by-stars

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/drafts-by-stars`

**Valid URL:** [https://api.on3.com/public/rdb/v1/drafts-by-stars](https://api.on3.com/public/rdb/v1/drafts-by-stars)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `yearSpan` | `year_span` |  |  | `Y` | yearSpan query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `state` | character | Venue state. |
| `blue_chip_percent` | numeric |  |
| `population_percent` | numeric |  |
| `talent_ratio` | numeric |  |
| `five_stars` | integer |  |
| `four_stars` | integer |  |
| `three_stars` | integer | Whether three stars data is available. |
| `zero_stars` | integer |  |
| `total` | integer | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_drafts_by_stars()
```

_Last validated n/a._

## `on3_drafts_by_stars_summary`

GET /rdb/v1/drafts-by-stars-summary

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/drafts-by-stars-summary`

**Valid URL:** [https://api.on3.com/public/rdb/v1/drafts-by-stars-summary](https://api.on3.com/public/rdb/v1/drafts-by-stars-summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `five_stars` | character |  |
| `four_stars` | character |  |
| `three_stars` | character | Whether three stars data is available. |
| `zero_stars` | character |  |
| `total_drafted` | integer |  |
| `total_recruited` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_drafts_by_stars_summary()
```

_Last validated n/a._

## `on3_drafts_players`

GET /rdb/v1/drafts/{orgKey}/players

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/drafts/{org_key}/players`

**Valid URL:** [https://api.on3.com/public/rdb/v1/drafts/1867/players](https://api.on3.com/public/rdb/v1/drafts/1867/players)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `org_key` | `org_key` |  | `Y` |  | org_key path parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `recruitment_key` | integer |  |
| `organization` | character | Organization. |
| `drafted_from_organization` | character |  |
| `high_school_organization` | character |  |
| `college_organization` | character |  |
| `hometown` | character | Prospect hometown. |
| `state` | character | Venue state. |
| `pick` | integer | Pick number of the NFL draftee within the round they were picked in. |
| `compensatory` | logical |  |
| `supplementary` | logical |  |
| `traded` | logical | Whether the pick was traded. |
| `forfeited` | logical |  |
| `trading_organization` | character |  |
| `through_organization_one` | character |  |
| `through_organization_two` | character |  |
| `through_organization_three` | character |  |
| `key` | integer |  |
| `round` | integer | Round of NFL draft the draftee was picked in. |
| `overall_pick` | integer | Overall pick number in the draft. |
| `person` | character |  |
| `position` | character | Athlete position. |
| `age` | numeric | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_drafts_players(org_key=1867)
```

_Last validated n/a._

## `on3_filters_conferences`

GET /rdb/v1/filters/conferences

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/conferences`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/conferences](https://api.on3.com/public/rdb/v1/filters/conferences)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | year query parameter. |
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `name` | character | Position name (e.g. `Quarterback`). |
| `abbreviation` | character | Metric abbreviation. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_filters_conferences()
```

_Last validated n/a._

## `on3_filters_draft_rounds`

GET /rdb/v1/filters/draft-rounds

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/draft-rounds`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/draft-rounds](https://api.on3.com/public/rdb/v1/filters/draft-rounds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | year query parameter. |
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_filters_draft_rounds()
```

_Last validated n/a._

## `on3_filters_positions`

GET /rdb/v1/filters/positions

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/positions`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/positions](https://api.on3.com/public/rdb/v1/filters/positions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `positionType` | `position_type` |  |  | `Y` | positionType query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `name` | character | Position name (e.g. `Quarterback`). |
| `abbreviation` | character | Metric abbreviation. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_filters_positions()
```

_Last validated n/a._

## `on3_filters_sports`

GET /rdb/v1/filters/sports

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/sports`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/sports](https://api.on3.com/public/rdb/v1/filters/sports)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `name` | character | Position name (e.g. `Quarterback`). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_filters_sports()
```

_Last validated n/a._

## `on3_filters_status`

GET /rdb/v1/filters/status

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/status`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/status](https://api.on3.com/public/rdb/v1/filters/status)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_filters_status()
```

_Last validated n/a._

## `on3_filters_teams`

GET /rdb/v1/filters/teams

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/teams`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/teams](https://api.on3.com/public/rdb/v1/filters/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `groupBy` | `group_by` |  |  | `Y` | groupBy query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `conference_key` | integer |  |
| `conference_abbr` | character | Conference abbreviation. |
| `teams` | character | Nested list of member-team membership spans. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_filters_teams()
```

_Last validated n/a._

## `on3_filters_years`

GET /rdb/v1/filters/years

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/years`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/years](https://api.on3.com/public/rdb/v1/filters/years)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_filters_years()
```

_Last validated n/a._

## `on3_nil_100`

GET /rdb/v1/nil-100

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/nil-100`

**Valid URL:** [https://api.on3.com/public/rdb/v1/nil-100](https://api.on3.com/public/rdb/v1/nil-100)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | year query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `person` | character |  |
| `valuation` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_nil_100()
```

_Last validated n/a._

## `on3_nil_100_v2`

GET /rdb/v2/nil-100

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v2/nil-100`

**Valid URL:** [https://api.on3.com/public/rdb/v2/nil-100](https://api.on3.com/public/rdb/v2/nil-100)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | year query parameter. |
| `orgKey` | `org_key` |  |  | `Y` | orgKey query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `person` | character |  |
| `valuation` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_nil_100_v2()
```

_Last validated n/a._

## `on3_nil_compliances_state`

GET /rdb/v1/nil-compliances/state

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/nil-compliances/state`

**Valid URL:** [https://api.on3.com/public/rdb/v1/nil-compliances/state](https://api.on3.com/public/rdb/v1/nil-compliances/state)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `stateKey` | `state_key` |  |  | `Y` | stateKey query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `organization_type` | character | Organization type. |
| `state_key` | integer |  |
| `state` | character | Venue state. |
| `monetization_allowed` | logical |  |
| `governing_rule_label` | character |  |
| `governing_rule_url` | character |  |
| `current_rules` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_nil_compliances_state()
```

_Last validated n/a._

## `on3_nil_rankings`

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

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `person` | character |  |
| `valuation` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_nil_rankings()
```

_Last validated n/a._

## `on3_organizations_draft_class_by_state`

GET /rdb/v1/organizations/{organizationKey}/draft-class-by-state

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/draft-class-by-state`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/draft-class-by-state](https://api.on3.com/public/rdb/v1/organizations/1867/draft-class-by-state)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `state` | character | Venue state. |
| `count` | integer | Total number of players in the season index. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_organizations_draft_class_by_state(organization_key=1867)
```

_Last validated n/a._

## `on3_organizations_draft_class_by_year`

GET /rdb/v1/organizations/{organizationKey}/draft-class-by-year

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/draft-class-by-year`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/draft-class-by-year](https://api.on3.com/public/rdb/v1/organizations/1867/draft-class-by-year)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `year` | integer | Four-digit season year (e.g. 2019). |
| `count` | integer | Total number of players in the season index. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_organizations_draft_class_by_year(organization_key=1867)
```

_Last validated n/a._

## `on3_organizations_draft_count_by_stars`

GET /rdb/v1/organizations/{organizationKey}/draft-count-by-stars

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/draft-count-by-stars`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/draft-count-by-stars](https://api.on3.com/public/rdb/v1/organizations/1867/draft-count-by-stars)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `five_stars` | integer |  |
| `four_stars` | integer |  |
| `three_stars` | integer | Whether three stars data is available. |
| `zero_stars` | integer |  |
| `total` | integer | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_organizations_draft_count_by_stars(organization_key=1867)
```

_Last validated n/a._

## `on3_organizations_draft_count_by_year`

GET /rdb/v1/organizations/{organizationKey}/draft-count-by-year

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/draft-count-by-year`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/draft-count-by-year](https://api.on3.com/public/rdb/v1/organizations/1867/draft-count-by-year)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `year` | integer | Four-digit season year (e.g. 2019). |
| `blue_chip_percent` | numeric |  |
| `talent_ratio` | numeric |  |
| `five_stars` | integer |  |
| `four_stars` | integer |  |
| `three_stars` | integer | Whether three stars data is available. |
| `zero_stars` | integer |  |
| `total` | integer | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_organizations_draft_count_by_year(organization_key=1867)
```

_Last validated n/a._

## `on3_organizations_draft_ranking_summary`

GET /rdb/v1/organizations/{organizationKey}/draft-ranking-summary

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/draft-ranking-summary`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/draft-ranking-summary](https://api.on3.com/public/rdb/v1/organizations/1867/draft-ranking-summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `conference` | character | Conference of the team. |
| `year_summary` | character |  |
| `span_summary` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_organizations_draft_ranking_summary(organization_key=1867)
```

_Last validated n/a._

## `on3_organizations_drafted_players`

GET /rdb/v1/organizations/{organizationKey}/drafted-players

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/drafted-players`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/drafted-players](https://api.on3.com/public/rdb/v1/organizations/1867/drafted-players)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `recruitment_key` | integer |  |
| `organization` | character | Organization. |
| `drafted_from_organization` | character |  |
| `high_school_organization` | character |  |
| `college_organization` | character |  |
| `hometown` | character | Prospect hometown. |
| `state` | character | Venue state. |
| `pick` | integer | Pick number of the NFL draftee within the round they were picked in. |
| `compensatory` | logical |  |
| `supplementary` | logical |  |
| `traded` | logical | Whether the pick was traded. |
| `forfeited` | logical |  |
| `trading_organization` | character |  |
| `through_organization_one` | character |  |
| `through_organization_two` | character |  |
| `through_organization_three` | character |  |
| `key` | integer |  |
| `round` | integer | Round of NFL draft the draftee was picked in. |
| `overall_pick` | integer | Overall pick number in the draft. |
| `person` | character |  |
| `position` | character | Athlete position. |
| `age` | numeric | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_organizations_drafted_players(organization_key=1867)
```

_Last validated n/a._

## `on3_organizations_drafts_by_stars_summary`

GET /rdb/v1/organizations/{organizationKey}/drafts-by-stars-summary

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/drafts-by-stars-summary`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/drafts-by-stars-summary](https://api.on3.com/public/rdb/v1/organizations/1867/drafts-by-stars-summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `nat_total_drafted` | integer |  |
| `total_drafted` | integer |  |
| `draft_rank` | integer |  |
| `organization` | character | Organization. |
| `overall_star_summary` | character |  |
| `five_star_summary` | character |  |
| `four_star_summary` | character |  |
| `three_star_summary` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_organizations_drafts_by_stars_summary(organization_key=1867)
```

_Last validated n/a._

## `on3_organizations_roster`

GET /rdb/v1/organizations/{organizationKey}/roster

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/roster`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/roster](https://api.on3.com/public/rdb/v1/organizations/1867/roster)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `pso_key` | integer |  |
| `player` | character | Player name. |
| `organization` | character | Organization. |
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `roster_rating` | character |  |
| `status` | character | Game status (e.g. "scheduled", "in_progress", "completed"). |
| `nil_value` | character |  |
| `rpm` | character |  |
| `industry_comparison` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_organizations_roster(organization_key=1867)
```

_Last validated n/a._

## `on3_organizations_roster_header`

GET /rdb/v1/organizations/{organizationKey}/roster-header

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/roster-header`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/roster-header](https://api.on3.com/public/rdb/v1/organizations/1867/roster-header)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `head_coach` | character |  |
| `talent_rank` | character |  |
| `prev_talent_rank` | character |  |
| `conference_rank` | character |  |
| `prev_conference_rank` | character |  |
| `average_rating` | character |  |
| `prev_average_rating` | character |  |
| `average_nil_value` | numeric |  |
| `total_nil_value` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_organizations_roster_header(organization_key=1867)
```

_Last validated n/a._

## `on3_people_combine_measurements`

GET /rdb/v1/people/{personKey}/combine-measurements

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/combine-measurements`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/combine-measurements](https://api.on3.com/public/rdb/v1/people/89617/combine-measurements)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `measurement_type_key` | integer |  |
| `measurement_type` | character |  |
| `value` | numeric | Metric value. |
| `is_verified` | logical | Whether the player profile is verified. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_people_combine_measurements(person_key=89617)
```

_Last validated n/a._

## `on3_people_latest_valuation`

GET /rdb/v1/people/{personKey}/latest-valuation

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/latest-valuation`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/latest-valuation](https://api.on3.com/public/rdb/v1/people/89617/latest-valuation)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `nil_status` | character |  |
| `valuation` | integer |  |
| `valuation_change` | integer |  |
| `followers` | integer |  |
| `rank` | integer | Position of the school within the poll for the given week (1 = top-ranked). |
| `last_updated` | integer | Timestamp ESPN last refreshed the power index. |
| `social_valuations` | character |  |
| `group_rank` | integer | League/season rank for group. |
| `group_name` | character | Group name (conference / division). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_people_latest_valuation(person_key=89617)
```

_Last validated n/a._

## `on3_people_measurements`

GET /rdb/v1/people/{personKey}/measurements

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/measurements`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/measurements](https://api.on3.com/public/rdb/v1/people/89617/measurements)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `measurement_type` | character |  |
| `measurement_type_key` | integer |  |
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `value` | numeric | Metric value. |
| `delta` | numeric |  |
| `person_key` | integer |  |
| `verified` | logical |  |
| `verified_by_user_key` | integer |  |
| `elite` | logical |  |
| `event_key` | integer |  |
| `event_name` | character | Event name (e.g. 'All-Star Workout Day: Home Run Derby'). |
| `event` | character | Binary flag indicating the row is a counted game event (excludes end markers). |
| `age_measurement_occurred` | numeric |  |
| `top300_average` | numeric |  |
| `top_average_change_percent` | numeric |  |
| `drafted_average` | numeric |  |
| `record` | character | Team win-loss record for the season. |
| `draft_change_percent` | numeric |  |
| `date_added` | integer |  |
| `date_modified` | integer | Date and time that injury information was updated |
| `date_occurred` | integer |  |
| `is_current` | logical |  |
| `person_sport_org_key` | integer |  |
| `organization` | character | Organization. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_people_measurements(person_key=89617)
```

_Last validated n/a._

## `on3_people_measurements_averages`

GET /rdb/v1/people/{personKey}/measurements/averages

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/measurements/averages`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/measurements/averages](https://api.on3.com/public/rdb/v1/people/89617/measurements/averages)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `measurement_key` | integer |  |
| `measurement_name` | character |  |
| `current_person_measurement` | numeric |  |
| `current_measurement_verified` | logical |  |
| `top300_difference` | numeric |  |
| `top300_average` | numeric |  |
| `combine_drafted_average` | numeric |  |
| `combine_drafted_difference` | numeric |  |
| `sort` | character |  |
| `measurement_record` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_people_measurements_averages(person_key=89617)
```

_Last validated n/a._

## `on3_people_person_connections`

GET /rdb/v1/people/{personKey}/person-connections

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/person-connections`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/person-connections](https://api.on3.com/public/rdb/v1/people/89617/person-connections)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `connection` | character |  |
| `connected_player` | character |  |
| `connected_roster_rating` | character |  |
| `connected_rating` | character |  |
| `connected_college_organization` | character |  |
| `connected_draft` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_people_person_connections(person_key=89617)
```

_Last validated n/a._

## `on3_people_social`

GET /rdb/v1/people/{personKey}/social

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/social`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/social](https://api.on3.com/public/rdb/v1/people/89617/social)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `handle` | character |  |
| `handshake` | logical |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_people_social(person_key=89617)
```

_Last validated n/a._

## `on3_people_social_post_summary`

GET /rdb/v1/people/{personKey}/social-post-summary

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/social-post-summary`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/social-post-summary](https://api.on3.com/public/rdb/v1/people/89617/social-post-summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `social_type` | character |  |
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `followers` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_people_social_post_summary(person_key=89617)
```

_Last validated n/a._

## `on3_people_track_and_field_measurements`

GET /rdb/v1/people/{personKey}/track-and-field-measurements

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/track-and-field-measurements`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/track-and-field-measurements](https://api.on3.com/public/rdb/v1/people/89617/track-and-field-measurements)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `measurement_type` | character |  |
| `measurement_type_key` | integer |  |
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `value` | numeric | Metric value. |
| `delta` | numeric |  |
| `person_key` | integer |  |
| `verified` | logical |  |
| `verified_by_user_key` | integer |  |
| `elite` | logical |  |
| `event_key` | integer |  |
| `event_name` | character | Event name (e.g. 'All-Star Workout Day: Home Run Derby'). |
| `event` | character | Binary flag indicating the row is a counted game event (excludes end markers). |
| `age_measurement_occurred` | numeric |  |
| `top300_average` | numeric |  |
| `top_average_change_percent` | numeric |  |
| `drafted_average` | numeric |  |
| `record` | character | Team win-loss record for the season. |
| `draft_change_percent` | numeric |  |
| `date_added` | integer |  |
| `date_modified` | integer | Date and time that injury information was updated |
| `date_occurred` | integer |  |
| `is_current` | logical |  |
| `person_sport_org_key` | integer |  |
| `organization` | character | Organization. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_people_track_and_field_measurements(person_key=89617)
```

_Last validated n/a._

## `on3_people_valuation_growth`

GET /rdb/v1/people/{personKey}/valuation-growth

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/valuation-growth`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/valuation-growth](https://api.on3.com/public/rdb/v1/people/89617/valuation-growth)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `nil_status` | character |  |
| `valuation` | integer |  |
| `valuation_change` | integer |  |
| `date` | character | Date of the poll release. |
| `date_unix` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_people_valuation_growth(person_key=89617)
```

_Last validated n/a._

## `on3_person_connections_connection_key`

GET /rdb/v1/person-connections/{connectionKey}

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/person-connections/{connection_key}`

**Valid URL:** [https://api.on3.com/public/rdb/v1/person-connections/89617](https://api.on3.com/public/rdb/v1/person-connections/89617)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `connection_key` | `connection_key` |  | `Y` |  | connection_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `person_key` | integer |  |
| `connected_person_key` | integer |  |
| `sport_key` | integer |  |
| `description` | character | ESPN's description of the stat. |
| `connected_person_sport` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_person_connections_connection_key(connection_key=89617)
```

_Last validated n/a._

## `on3_person_primary_recruitment_evaluation`

GET /rdb/v1/person/{personKey}/primary-recruitment-evaluation

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/person/{person_key}/primary-recruitment-evaluation`

**Valid URL:** [https://api.on3.com/public/rdb/v1/person/89617/primary-recruitment-evaluation](https://api.on3.com/public/rdb/v1/person/89617/primary-recruitment-evaluation)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `recruitment_key` | integer |  |
| `author_key` | integer |  |
| `author_name` | character |  |
| `author_title` | character |  |
| `title` | character | Specific role title for the assignment. |
| `premium` | logical | Whether the article is premium content. |
| `body` | character |  |
| `primary` | logical |  |
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |
| `date_updated_unix` | integer |  |
| `date_added` | character |  |
| `date_updated` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_person_primary_recruitment_evaluation(person_key=89617)
```

_Last validated n/a._

## `on3_person_recruitment_evaluations`

GET /rdb/v1/person/{personKey}/recruitment-evaluations

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/person/{person_key}/recruitment-evaluations`

**Valid URL:** [https://api.on3.com/public/rdb/v1/person/89617/recruitment-evaluations](https://api.on3.com/public/rdb/v1/person/89617/recruitment-evaluations)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `recruitment_key` | integer |  |
| `author_key` | integer |  |
| `author_name` | character |  |
| `author_title` | character |  |
| `title` | character | Specific role title for the assignment. |
| `premium` | logical | Whether the article is premium content. |
| `body` | character |  |
| `primary` | logical |  |
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |
| `date_updated_unix` | integer |  |
| `date_added` | character |  |
| `date_updated` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_person_recruitment_evaluations(person_key=89617)
```

_Last validated n/a._

## `on3_person_sport_profile_recruit`

GET /rdb/v1/person-sport/{psKey}/profile-recruit

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/person-sport/{ps_key}/profile-recruit`

**Valid URL:** [https://api.on3.com/public/rdb/v1/person-sport/89617/profile-recruit](https://api.on3.com/public/rdb/v1/person-sport/89617/profile-recruit)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ps_key` | `ps_key` |  | `Y` |  | ps_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `ranking_key` | integer |  |
| `rating` | numeric | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `state_rank` | integer | State ranking. |
| `state_abbr` | character |  |
| `position_rank` | integer | Position ranking. |
| `position_abbr` | character | Position abbreviation. |
| `overall_rank` | integer | Overall recruit ranking (top recruits only; may be `NA`). |
| `stars` | integer | Recruit star rating on the 247Sports scale (2-5). |
| `change` | character |  |
| `consensus_rating` | numeric |  |
| `consensus_state_rank` | integer |  |
| `consensus_position_rank` | integer |  |
| `consensus_overall_rank` | integer |  |
| `consensus_stars` | integer |  |
| `consensus_change` | character |  |
| `strength` | integer | Strength label (Even, Power Play, Shorthanded). |
| `five_star_plus` | logical |  |
| `ranking_type` | character | Poll type code (e.g. `ap`, `coaches`, `cfp`). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_person_sport_profile_recruit(ps_key=89617)
```

_Last validated n/a._

## `on3_person_sport_rankings`

GET /rdb/v1/person-sport-rankings

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/person-sport-rankings`

**Valid URL:** [https://api.on3.com/public/rdb/v1/person-sport-rankings](https://api.on3.com/public/rdb/v1/person-sport-rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `ratings` | character |  |
| `person` | character |  |
| `nil_value` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_person_sport_rankings()
```

_Last validated n/a._

## `on3_player_all_rankings`

GET /rdb/v1/player/{personKey}/all-rankings

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{person_key}/all-rankings`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/all-rankings](https://api.on3.com/public/rdb/v1/player/89617/all-rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `link` | character | API link to the game feed. |
| `ranking_key` | integer |  |
| `ranking_type` | character | Poll type code (e.g. `ap`, `coaches`, `cfp`). |
| `rating` | numeric | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `sport` | character |  |
| `class_year` | integer |  |
| `state_rank` | integer | State ranking. |
| `state_abbr` | character |  |
| `position_rank` | integer | Position ranking. |
| `position_abbr` | character | Position abbreviation. |
| `overall_rank` | integer | Overall recruit ranking (top recruits only; may be `NA`). |
| `stars` | integer | Recruit star rating on the 247Sports scale (2-5). |
| `five_star_plus` | logical |  |
| `nearly_five_star_plus` | logical |  |
| `change` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_player_all_rankings(person_key=89617)
```

_Last validated n/a._

## `on3_player_database_updates`

GET /rdb/v1/player/{personKey}/database-updates

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{person_key}/database-updates`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/database-updates](https://api.on3.com/public/rdb/v1/player/89617/database-updates)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `text` | character | Full play description. |
| `replacement_text` | character |  |
| `link` | character | API link to the game feed. |
| `date_added` | integer |  |
| `date_occurred` | integer |  |
| `object_key` | integer |  |
| `sport_key` | integer |  |
| `person_key` | integer |  |
| `organization_key` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_player_database_updates(person_key=89617)
```

_Last validated n/a._

## `on3_player_images`

GET /rdb/v1/player/{personKey}/images

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{person_key}/images`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/images](https://api.on3.com/public/rdb/v1/player/89617/images)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `domain_override` | character |  |
| `domain` | character |  |
| `source_override` | character |  |
| `source` | character | News source. |
| `title` | character | Specific role title for the assignment. |
| `description` | character | ESPN's description of the stat. |
| `caption` | character |  |
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |
| `alt_text` | character |  |
| `height` | integer | Listed height (inches). |
| `width` | integer |  |
| `asset_type` | character |  |
| `file_system` | character |  |
| `path` | character |  |
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `thumbnail` | character |  |
| `duration` | integer | Duration. |
| `mime_type` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_player_images(person_key=89617)
```

_Last validated n/a._

## `on3_player_organizations`

GET /rdb/v1/player/{personKey}/organizations

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{person_key}/organizations`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/organizations](https://api.on3.com/public/rdb/v1/player/89617/organizations)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `organizations` | character |  |
| `draft` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_player_organizations(person_key=89617)
```

_Last validated n/a._

## `on3_player_organizations_org_key`

GET /rdb/v1/player/{playerKey}/organizations/{orgKey}

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{player_key}/organizations/{org_key}`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/organizations/1867](https://api.on3.com/public/rdb/v1/player/89617/organizations/1867)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_key` | `player_key` |  | `Y` |  | player_key path parameter. |
| `org_key` | `org_key` |  | `Y` |  | org_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `organization` | character | Organization. |
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `position_abbr` | character | Position abbreviation. |
| `exp_min` | integer |  |
| `exp_max` | integer |  |
| `year` | character | Four-digit season year (e.g. 2019). |
| `age` | integer | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_player_organizations_org_key(org_key=1867, player_key=89617)
```

_Last validated n/a._

## `on3_player_person_rankings`

GET /rdb/v1/player/{personKey}/rankings

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{person_key}/rankings`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/rankings](https://api.on3.com/public/rdb/v1/player/89617/rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `ranking_key` | integer |  |
| `rating` | numeric | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `state_rank` | integer | State ranking. |
| `state_abbr` | character |  |
| `position_rank` | integer | Position ranking. |
| `position_abbr` | character | Position abbreviation. |
| `overall_rank` | integer | Overall recruit ranking (top recruits only; may be `NA`). |
| `stars` | integer | Recruit star rating on the 247Sports scale (2-5). |
| `change` | character |  |
| `consensus_rating` | numeric |  |
| `consensus_state_rank` | integer |  |
| `consensus_position_rank` | integer |  |
| `consensus_overall_rank` | integer |  |
| `consensus_stars` | integer |  |
| `consensus_change` | character |  |
| `strength` | integer | Strength label (Even, Power Play, Shorthanded). |
| `five_star_plus` | logical |  |
| `ranking_type` | character | Poll type code (e.g. `ap`, `coaches`, `cfp`). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_player_person_rankings(person_key=89617)
```

_Last validated n/a._

## `on3_player_profile`

GET /rdb/v1/player/{personKey}/profile

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{person_key}/profile`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/profile](https://api.on3.com/public/rdb/v1/player/89617/profile)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `class_year_recruitment_key` | character |  |
| `recruitment_key` | integer |  |
| `person_can_manage_recruitment` | character |  |
| `ranking_key` | integer |  |
| `person_sport_key` | integer |  |
| `ranking` | character | National rank of the team's overall SP+ rating (1 = best). |
| `oracle_key` | character |  |
| `name` | character | Position name (e.g. `Quarterback`). |
| `slug` | character | URL slug for the team. |
| `high_school_name` | character | Recruit high-school name. |
| `high_school` | character | High school |
| `hometown_name` | character |  |
| `hometown_state` | character | Recruit hometown state. |
| `current_state` | character | Current home venue state. |
| `default_asset` | character |  |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`); `position_detail = TRUE` only. |
| `primary_position` | character |  |
| `class_rank` | character |  |
| `height` | character | Listed height (inches). |
| `weight` | integer | Listed weight (lbs). |
| `class_year` | integer |  |
| `degree` | character |  |
| `age` | integer | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `default_sport` | character |  |
| `sports` | character |  |
| `description` | character | ESPN's description of the stat. |
| `bio_pro_prospect` | character |  |
| `bio_college_recruit` | character |  |
| `organization_level` | character |  |
| `high_school_org_key` | integer |  |
| `prep_school_org_key` | character |  |
| `junior_college_org_key` | character |  |
| `college_org_key` | character |  |
| `nil_value` | integer |  |
| `athlete_verified` | logical |  |
| `prospect_verified` | logical |  |
| `player_status` | character |  |
| `is_coach` | logical |  |
| `is_athlete` | logical |  |
| `visibility` | character |  |
| `tier` | character |  |
| `review_status` | character |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `badge` | character |  |
| `ncaa_id` | character |  |
| `managed_by_user` | logical |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_player_profile(person_key=89617)
```

_Last validated n/a._

## `on3_player_team_targets`

GET /rdb/v1/player/{playerKey}/team-targets

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{player_key}/team-targets`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/team-targets](https://api.on3.com/public/rdb/v1/player/89617/team-targets)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_key` | `player_key` |  | `Y` |  | player_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team` | character | Team name. |
| `year` | integer | Four-digit season year (e.g. 2019). |
| `sport` | character |  |
| `coaches` | character |  |
| `status` | character | Game status (e.g. "scheduled", "in_progress", "completed"). |
| `interest` | integer |  |
| `distance` | numeric | Yards to gain for a first down (or to the goal line in goal-to-go situations). |
| `class_rank` | integer |  |
| `official_visit_count` | integer |  |
| `un_official_visit_count` | integer |  |
| `prediction` | numeric | Pre-game prediction (favorite, score, win %). |
| `committed_date` | character |  |
| `draft_position_count` | integer |  |
| `draft_total` | integer |  |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`); `position_detail = TRUE` only. |
| `position_key` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_player_team_targets(player_key=89617)
```

_Last validated n/a._

## `on3_player_verified`

GET /rdb/v1/player/verified

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/verified`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/verified](https://api.on3.com/public/rdb/v1/player/verified)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `class_year_recruitment_key` | character |  |
| `recruitment_key` | integer |  |
| `person_can_manage_recruitment` | character |  |
| `ranking_key` | integer |  |
| `person_sport_key` | integer |  |
| `ranking` | character | National rank of the team's overall SP+ rating (1 = best). |
| `oracle_key` | character |  |
| `name` | character | Position name (e.g. `Quarterback`). |
| `slug` | character | URL slug for the team. |
| `high_school_name` | character | Recruit high-school name. |
| `high_school` | character | High school |
| `hometown_name` | character |  |
| `hometown_state` | character | Recruit hometown state. |
| `current_state` | character | Current home venue state. |
| `default_asset` | character |  |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`); `position_detail = TRUE` only. |
| `primary_position` | character |  |
| `class_rank` | character |  |
| `height` | character | Listed height (inches). |
| `weight` | integer | Listed weight (lbs). |
| `class_year` | integer |  |
| `degree` | character |  |
| `age` | integer | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `default_sport` | character |  |
| `sports` | character |  |
| `description` | character | ESPN's description of the stat. |
| `bio_pro_prospect` | character |  |
| `bio_college_recruit` | character |  |
| `organization_level` | character |  |
| `high_school_org_key` | integer |  |
| `prep_school_org_key` | character |  |
| `junior_college_org_key` | character |  |
| `college_org_key` | character |  |
| `nil_value` | integer |  |
| `athlete_verified` | logical |  |
| `prospect_verified` | logical |  |
| `player_status` | character |  |
| `is_coach` | logical |  |
| `is_athlete` | logical |  |
| `visibility` | character |  |
| `tier` | character |  |
| `review_status` | character |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `badge` | character |  |
| `ncaa_id` | character |  |
| `managed_by_user` | logical |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_player_verified()
```

_Last validated n/a._

## `on3_player_videos`

GET /rdb/v1/player/{personKey}/videos

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{person_key}/videos`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/videos](https://api.on3.com/public/rdb/v1/player/89617/videos)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `person_key` | integer |  |
| `source_url` | character |  |
| `title` | character | Specific role title for the assignment. |
| `thumbnail` | character |  |
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |
| `description` | character | ESPN's description of the stat. |
| `person_sport` | character |  |
| `is_featured` | logical |  |
| `date` | integer | Date of the poll release. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_player_videos(person_key=89617)
```

_Last validated n/a._

## `on3_player_visit_center`

GET /rdb/v1/player/{playerKey}/visit-center

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/player/{player_key}/visit-center`

**Valid URL:** [https://api.on3.com/public/rdb/v1/player/89617/visit-center](https://api.on3.com/public/rdb/v1/player/89617/visit-center)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_key` | `player_key` |  | `Y` |  | player_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `sport` | character |  |
| `year` | integer | Four-digit season year (e.g. 2019). |
| `total_visits` | integer |  |
| `visits` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_player_visit_center(player_key=89617)
```

_Last validated n/a._

## `on3_players_industry_comparision`

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

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `ratings` | character | List of per-service rating entries (On3, Rivals, 247, ESPN) composing the industry comparison. |
| `person` | character | Nested person object (identity, school, position, status) for the player. |
| `nil_value` | integer | On3 NIL valuation for the player (US dollars). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_players_industry_comparision()
```

_Last validated n/a._

## `on3_players_industry_comparision_list`

GET /rdb/v1/players/industry-comparision-list

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/players/industry-comparision-list`

**Valid URL:** [https://api.on3.com/public/rdb/v1/players/industry-comparision-list](https://api.on3.com/public/rdb/v1/players/industry-comparision-list)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `ratings` | character |  |
| `person` | character |  |
| `nil_value` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_players_industry_comparision_list()
```

_Last validated n/a._

## `on3_predictions_user_key`

Expert prediction accuracy + feed (see PredictionAccuracies)

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/predictions/{user_key}`

**Valid URL:** [https://api.on3.com/public/rdb/v1/predictions/89617](https://api.on3.com/public/rdb/v1/predictions/89617)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `user_key` | `user_key` |  | `Y` |  | user_key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_on3_rdb`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_predictions_user_key(user_key=89617)
```

_Last validated n/a._

## `on3_quotes`

GET /rdb/v1/quotes

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/quotes`

**Valid URL:** [https://api.on3.com/public/rdb/v1/quotes](https://api.on3.com/public/rdb/v1/quotes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `body` | character |  |
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |
| `person_key` | integer |  |
| `person` | character |  |
| `date_added` | character |  |
| `date_updated` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_quotes()
```

_Last validated n/a._

## `on3_quotes_key`

GET /rdb/v1/quotes/{key}

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/quotes/{key}`

**Valid URL:** [https://api.on3.com/public/rdb/v1/quotes/1](https://api.on3.com/public/rdb/v1/quotes/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `body` | character |  |
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |
| `person_key` | integer |  |
| `person` | character |  |
| `date_added` | character |  |
| `date_updated` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_quotes_key(key=1)
```

_Last validated n/a._

## `on3_recruitment_primary_recruitment_evaluation`

GET /rdb/v1/recruitment/{recruitmentKey}/primary-recruitment-evaluation

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/recruitment/{recruitment_key}/primary-recruitment-evaluation`

**Valid URL:** [https://api.on3.com/public/rdb/v1/recruitment/270036/primary-recruitment-evaluation](https://api.on3.com/public/rdb/v1/recruitment/270036/primary-recruitment-evaluation)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `recruitment_key` | `recruitment_key` |  | `Y` |  | recruitment_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `recruitment_key` | integer |  |
| `author_key` | integer |  |
| `author_name` | character |  |
| `author_title` | character |  |
| `title` | character | Specific role title for the assignment. |
| `premium` | logical | Whether the article is premium content. |
| `body` | character |  |
| `primary` | logical |  |
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |
| `date_updated_unix` | integer |  |
| `date_added` | character |  |
| `date_updated` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_recruitment_primary_recruitment_evaluation(recruitment_key=270036)
```

_Last validated n/a._

## `on3_recruitment_recruitment_evaluations`

GET /rdb/v1/recruitment/{recruitmentKey}/recruitment-evaluations

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/recruitment/{recruitment_key}/recruitment-evaluations`

**Valid URL:** [https://api.on3.com/public/rdb/v1/recruitment/270036/recruitment-evaluations](https://api.on3.com/public/rdb/v1/recruitment/270036/recruitment-evaluations)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `recruitment_key` | `recruitment_key` |  | `Y` |  | recruitment_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `recruitment_key` | integer |  |
| `author_key` | integer |  |
| `author_name` | character |  |
| `author_title` | character |  |
| `title` | character | Specific role title for the assignment. |
| `premium` | logical | Whether the article is premium content. |
| `body` | character |  |
| `primary` | logical |  |
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |
| `date_updated_unix` | integer |  |
| `date_added` | character |  |
| `date_updated` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_recruitment_recruitment_evaluations(recruitment_key=270036)
```

_Last validated n/a._

## `on3_recruitments_latest_rpm_picks`

Latest RPM (prediction) picks feed — paged {list,pagination}

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/recruitments/latest-rpm-picks`

**Valid URL:** [https://api.on3.com/public/rdb/v1/recruitments/latest-rpm-picks](https://api.on3.com/public/rdb/v1/recruitments/latest-rpm-picks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `orgKey` | `org_key` |  |  | `Y` | orgKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `pick` | character | Pick number of the NFL draftee within the round they were picked in. |
| `player` | character | Player name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_recruitments_latest_rpm_picks()
```

_Last validated n/a._

## `on3_recruitments_profile`

GET /rdb/v1/recruitments/{recKey}/profile

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/recruitments/{rec_key}/profile`

**Valid URL:** [https://api.on3.com/public/rdb/v1/recruitments/270036/profile](https://api.on3.com/public/rdb/v1/recruitments/270036/profile)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `rec_key` | `rec_key` |  | `Y` |  | rec_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `class_year` | integer |  |
| `committed_status` | character |  |
| `high_school` | character | High school |
| `high_school_org` | character |  |
| `home_town` | character | Player home town. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_recruitments_profile(rec_key=270036)
```

_Last validated n/a._

## `on3_recruitments_rpm_picks`

GET /rdb/v1/recruitments/{recKey}/rpm-picks

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/recruitments/{rec_key}/rpm-picks`

**Valid URL:** [https://api.on3.com/public/rdb/v1/recruitments/270036/rpm-picks](https://api.on3.com/public/rdb/v1/recruitments/270036/rpm-picks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `rec_key` | `rec_key` |  | `Y` |  | rec_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `organization` | character | Organization. |
| `date_added` | character |  |
| `expert` | character |  |
| `confidence` | numeric |  |
| `description` | character | ESPN's description of the stat. |
| `article_link` | character |  |
| `premium` | logical | Whether the article is premium content. |
| `correct` | logical |  |
| `days_correct` | numeric |  |
| `flipped_from_organization` | character |  |
| `previous_confidence` | numeric |  |
| `previous_date_added` | character |  |
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `top_teams` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_recruitments_rpm_picks(rec_key=270036)
```

_Last validated n/a._

## `on3_recruitments_rpm_summary`

GET /rdb/v1/recruitments/{recKey}/rpm-summary

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/recruitments/{rec_key}/rpm-summary`

**Valid URL:** [https://api.on3.com/public/rdb/v1/recruitments/270036/rpm-summary](https://api.on3.com/public/rdb/v1/recruitments/270036/rpm-summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `rec_key` | `rec_key` |  | `Y` |  | rec_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `predictions` | character |  |
| `locked` | logical |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_recruitments_rpm_summary(rec_key=270036)
```

_Last validated n/a._

## `on3_team_ranking`

GET /rdb/v1/team-ranking

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/team-ranking`

**Valid URL:** [https://api.on3.com/public/rdb/v1/team-ranking](https://api.on3.com/public/rdb/v1/team-ranking)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `organization` | character | Organization. |
| `applied_total_rating` | numeric |  |
| `applied_total_consensus_rating` | numeric |  |
| `applied_average_rating` | numeric |  |
| `applied_average_consensus_rating` | numeric |  |
| `commits` | integer | Number of commits in the position group. |
| `applied_commits` | integer |  |
| `deductions` | numeric |  |
| `deductions_description` | character |  |
| `five_stars` | integer |  |
| `consensus_five_stars` | integer |  |
| `four_stars` | integer |  |
| `consensus_four_stars` | integer |  |
| `three_stars` | integer | Whether three stars data is available. |
| `consensus_three_stars` | integer |  |
| `overall_rank` | integer | Overall recruit ranking (top recruits only; may be `NA`). |
| `overall_consensus_rank` | integer |  |
| `dispay_consensus_score` | numeric |  |
| `dispay_on3_score` | numeric |  |
| `average_nil_value` | numeric |  |
| `conference_rank` | integer |  |
| `conference_consensus_rank` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_team_ranking()
```

_Last validated n/a._

## `on3_team_ranking_bluechips_team_rankings`

GET /rdb/v1/team-ranking/{sport}-{year}/bluechips-team-rankings

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/team-ranking/{sport_slug}-{year}/bluechips-team-rankings`

**Valid URL:** [https://api.on3.com/public/rdb/v1/team-ranking/football-2025/bluechips-team-rankings](https://api.on3.com/public/rdb/v1/team-ranking/football-2025/bluechips-team-rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport_slug` | `sport_slug` |  | `Y` |  | sport_slug path parameter. |
| `year` | `year` |  | `Y` |  | year path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `organization` | character | Organization. |
| `overall_rank` | integer | Overall recruit ranking (top recruits only; may be `NA`). |
| `conference_rank` | integer |  |
| `in_state_count` | numeric |  |
| `average_distance` | numeric |  |
| `blue_chips` | numeric |  |
| `social_nil_values` | numeric |  |
| `score` | numeric | Final score string. |
| `total_commits` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_team_ranking_bluechips_team_rankings(sport_slug='football', year=2025)
```

_Last validated n/a._

## `on3_team_ranking_consensus_team_rankings`

GET /rdb/v1/team-ranking/{sport}-{year}/consensus-team-rankings

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/team-ranking/{sport_slug}-{year}/consensus-team-rankings`

**Valid URL:** [https://api.on3.com/public/rdb/v1/team-ranking/football-2025/consensus-team-rankings](https://api.on3.com/public/rdb/v1/team-ranking/football-2025/consensus-team-rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport_slug` | `sport_slug` |  | `Y` |  | sport_slug path parameter. |
| `year` | `year` |  | `Y` |  | year path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `organization` | character | Organization. |
| `applied_total_rating` | numeric |  |
| `applied_total_consensus_rating` | numeric |  |
| `applied_average_rating` | numeric |  |
| `applied_average_consensus_rating` | numeric |  |
| `commits` | integer | Number of commits in the position group. |
| `applied_commits` | integer |  |
| `deductions` | numeric |  |
| `deductions_description` | character |  |
| `five_stars` | integer |  |
| `consensus_five_stars` | integer |  |
| `four_stars` | integer |  |
| `consensus_four_stars` | integer |  |
| `three_stars` | integer | Whether three stars data is available. |
| `consensus_three_stars` | integer |  |
| `overall_rank` | integer | Overall recruit ranking (top recruits only; may be `NA`). |
| `overall_consensus_rank` | integer |  |
| `dispay_consensus_score` | numeric |  |
| `dispay_on3_score` | numeric |  |
| `average_nil_value` | numeric |  |
| `conference_rank` | integer |  |
| `conference_consensus_rank` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_team_ranking_consensus_team_rankings(sport_slug='football', year=2025)
```

_Last validated n/a._

## `on3_team_ranking_organizations_summary`

GET /rdb/v1/team-ranking/organizations/{orgKey}/summary

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/team-ranking/organizations/{org_key}/summary`

**Valid URL:** [https://api.on3.com/public/rdb/v1/team-ranking/organizations/1867/summary](https://api.on3.com/public/rdb/v1/team-ranking/organizations/1867/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `org_key` | `org_key` |  | `Y` |  | org_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `conference` | character | Conference of the team. |
| `year` | integer | Four-digit season year (e.g. 2019). |
| `total_commits` | integer |  |
| `class_rating_current` | numeric |  |
| `class_rating_previous` | numeric |  |
| `class_rating_change` | character |  |
| `national_rank_current` | integer |  |
| `national_rank_previous` | integer |  |
| `national_rank_change` | character |  |
| `conference_rank_current` | integer |  |
| `conference_rank_previous` | integer |  |
| `conference_rank_change` | character |  |
| `conference_consensus_rank_current` | integer |  |
| `conference_consensus_rank_previous` | integer |  |
| `conference_consensus_rank_change` | character |  |
| `in_state` | numeric |  |
| `avg_distance` | numeric | Average batted-ball distance (feet). |
| `blue_chips` | numeric |  |
| `head_coach` | character |  |
| `director_personnel` | character |  |
| `average_nil_value` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_team_ranking_organizations_summary(org_key=1867)
```

_Last validated n/a._

## `on3_team_ranking_team_rankings`

GET /rdb/v1/team-ranking/{sport}-{year}/team-rankings

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/team-ranking/{sport_slug}-{year}/team-rankings`

**Valid URL:** [https://api.on3.com/public/rdb/v1/team-ranking/football-2025/team-rankings](https://api.on3.com/public/rdb/v1/team-ranking/football-2025/team-rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport_slug` | `sport_slug` |  | `Y` |  | sport_slug path parameter. |
| `year` | `year` |  | `Y` |  | year path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | On3 organization-ranking key for the class row. |
| `organization` | character | Nested organization object (school identity, logo, colors) for the class. |
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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_team_ranking_team_rankings(sport_slug='football', year=2025)
```

_Last validated n/a._

## `on3_transfers_best_available`

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

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `eligibility` | character | Eligibility status. |
| `last_team` | character |  |
| `entered_article` | character |  |
| `committed_article` | character |  |
| `exited_article` | character |  |
| `key` | integer |  |
| `recruitment_key` | integer |  |
| `name` | character | Position name (e.g. `Quarterback`). |
| `slug` | character | URL slug for the team. |
| `high_school_name` | character | Recruit high-school name. |
| `home_town_name` | character |  |
| `early_enrollee` | logical |  |
| `early_signee` | logical |  |
| `default_asset_url` | character |  |
| `class_year` | integer |  |
| `athlete_verified` | logical |  |
| `prospect_verified` | logical |  |
| `default_asset` | character |  |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`); `position_detail = TRUE` only. |
| `height` | character | Listed height (inches). |
| `weight` | numeric | Listed weight (lbs). |
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `roster_rating` | character |  |
| `commit_status` | character |  |
| `predictions` | character |  |
| `nil_status` | character |  |
| `nil_value` | numeric |  |
| `sport` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_transfers_best_available()
```

_Last validated n/a._

## `on3_transfers_latest`

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

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `eligibility` | character | Eligibility status. |
| `last_team` | character |  |
| `entered_article` | character |  |
| `committed_article` | character |  |
| `exited_article` | character |  |
| `key` | integer |  |
| `recruitment_key` | integer |  |
| `name` | character | Position name (e.g. `Quarterback`). |
| `slug` | character | URL slug for the team. |
| `high_school_name` | character | Recruit high-school name. |
| `home_town_name` | character |  |
| `early_enrollee` | logical |  |
| `early_signee` | logical |  |
| `default_asset_url` | character |  |
| `class_year` | integer |  |
| `athlete_verified` | logical |  |
| `prospect_verified` | logical |  |
| `default_asset` | character |  |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`); `position_detail = TRUE` only. |
| `height` | character | Listed height (inches). |
| `weight` | numeric | Listed weight (lbs). |
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `roster_rating` | character |  |
| `commit_status` | character |  |
| `predictions` | character |  |
| `nil_status` | character |  |
| `nil_value` | numeric |  |
| `sport` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_transfers_latest()
```

_Last validated n/a._

## `on3_videos_video_key`

GET /rdb/v1/videos/{videoKey}

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/videos/{video_key}`

**Valid URL:** [https://api.on3.com/public/rdb/v1/videos/1](https://api.on3.com/public/rdb/v1/videos/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `video_key` | `video_key` |  | `Y` |  | video_key path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer |  |
| `person_key` | integer |  |
| `source_url` | character |  |
| `title` | character | Specific role title for the assignment. |
| `thumbnail` | character |  |
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |
| `description` | character | ESPN's description of the stat. |
| `person_sport` | character |  |
| `is_featured` | logical |  |
| `date` | integer | Date of the poll release. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
on3_videos_video_key(video_key=1)
```

_Last validated n/a._
