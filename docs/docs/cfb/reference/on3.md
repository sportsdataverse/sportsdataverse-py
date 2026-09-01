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
| `commits` | character | Number of commitments in the organization's latest recruiting class. |

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
| `commits` | character | Number of commitments in the organization's recruiting class for the season. |

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
| `five_stars` | integer | Number of five-star recruits the organization signed over the ranking window. |
| `four_stars` | integer | Number of four-star recruits the organization signed over the ranking window. |
| `three_stars` | integer | Number of three-star recruits the organization signed over the ranking window. |
| `total` | integer | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `percent_drafted` | numeric | Share of the organization's recruits who went on to be drafted. |
| `draft_rate` | numeric | Organization's draft-production rate used in On3's draft ranking. |

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
| `first_round` | integer | Number of the organization's players drafted in the first round. |
| `second_round` | integer | Number of the organization's players drafted in the second round. |
| `third_round` | integer | Number of the organization's players drafted in the third round. |
| `fourth_through_seventh_round` | integer | Number of the organization's players drafted in rounds four through seven. |
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
| `recruitment_key` | integer | On3 RDB recruitment key linking the draft pick back to the player's recruitment record. |
| `organization` | character | Organization. |
| `drafted_from_organization` | character | Nested On3 organization object for the school the player was drafted out of (stringified). |
| `high_school_organization` | character | Nested On3 organization object for the player's high school (stringified). |
| `college_organization` | character | Nested On3 organization object for the college the player attended (stringified). |
| `hometown` | character | Prospect hometown. |
| `state` | character | Home state of the drafted player, per On3. |
| `pick` | integer | Pick number of the NFL draftee within the round they were picked in. |
| `compensatory` | logical | Whether the selection was a compensatory draft pick. |
| `supplementary` | logical | Whether the selection came in a supplemental draft. |
| `traded` | logical | Whether the pick was traded. |
| `forfeited` | logical | Whether the pick was forfeited. |
| `trading_organization` | character | Pro organization that traded the pick away, when it changed hands (On3 RDB). |
| `through_organization_one` | character | First intermediate organization the pick passed through in trades before being exercised (On3 RDB). |
| `through_organization_two` | character | Second intermediate organization the pick passed through in trades before being exercised (On3 RDB). |
| `through_organization_three` | character | Third intermediate organization the pick passed through in trades before being exercised (On3 RDB). |
| `key` | integer | On3 RDB key for the draft-pick record. |
| `round` | integer | Round of NFL draft the draftee was picked in. |
| `overall_pick` | integer | Overall pick number in the draft. |
| `person` | character | Nested On3 person object for the drafted player (stringified). |
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
| `state` | character | Home state associated with the draft row, per On3. |
| `blue_chip_percent` | numeric | Percent of the drafted group who were blue-chip (four- or five-star) recruits. |
| `population_percent` | numeric | Percent of the overall recruit population holding this star rating. |
| `talent_ratio` | numeric | Ratio of the star tier's draft share to its population share (On3's talent ratio). |
| `five_stars` | integer | Number of drafted players who were five-star recruits. |
| `four_stars` | integer | Number of drafted players who were four-star recruits. |
| `three_stars` | integer | Number of drafted players who were three-star recruits. |
| `zero_stars` | integer | Number of drafted players who were unrated (zero-star) recruits. |
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
| `five_stars` | character | Number of drafted players who were five-star recruits. |
| `four_stars` | character | Number of drafted players who were four-star recruits. |
| `three_stars` | character | Number of drafted players who were three-star recruits. |
| `zero_stars` | character | Number of drafted players who were unrated (zero-star) recruits. |
| `total_drafted` | integer | Total number of players drafted in the summarized group. |
| `total_recruited` | integer | Total number of recruits in the summarized group. |

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
| `recruitment_key` | integer | On3 RDB recruitment key linking the draft pick back to the player's recruitment record. |
| `organization` | character | Organization. |
| `drafted_from_organization` | character | Nested On3 organization object for the school the player was drafted out of (stringified). |
| `high_school_organization` | character | Nested On3 organization object for the player's high school (stringified). |
| `college_organization` | character | Nested On3 organization object for the college the player attended (stringified). |
| `hometown` | character | Prospect hometown. |
| `state` | character | Home state of the drafted player, per On3. |
| `pick` | integer | Pick number of the NFL draftee within the round they were picked in. |
| `compensatory` | logical | Whether the selection was a compensatory draft pick. |
| `supplementary` | logical | Whether the selection came in a supplemental draft. |
| `traded` | logical | Whether the pick was traded. |
| `forfeited` | logical | Whether the pick was forfeited. |
| `trading_organization` | character | Pro organization that traded the pick away, when it changed hands (On3 RDB). |
| `through_organization_one` | character | First intermediate organization the pick passed through in trades before being exercised (On3 RDB). |
| `through_organization_two` | character | Second intermediate organization the pick passed through in trades before being exercised (On3 RDB). |
| `through_organization_three` | character | Third intermediate organization the pick passed through in trades before being exercised (On3 RDB). |
| `key` | integer | On3 RDB key for the draft-pick record. |
| `round` | integer | Round of NFL draft the draftee was picked in. |
| `overall_pick` | integer | Overall pick number in the draft. |
| `person` | character | Nested On3 person object for the drafted player (stringified). |
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
| `key` | integer | On3 RDB key for the conference filter option. |
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
| `key` | integer | On3 RDB key for the position filter option. |
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
| `key` | integer | On3 RDB key for the sport filter option. |
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
| `conference_key` | integer | On3 RDB key of the conference the team belongs to. |
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
| `person` | character | Nested On3 person object for the ranked athlete (stringified). |
| `valuation` | character | Athlete's On3 NIL valuation in dollars. |

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
| `person` | character | Nested On3 person object for the ranked athlete (stringified). |
| `valuation` | character | Athlete's On3 NIL valuation in dollars. |

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
| `key` | integer | On3 RDB key for the state NIL-compliance record. |
| `organization_type` | character | Organization type. |
| `state_key` | integer | On3 key of the U.S. state the compliance record covers. |
| `state` | character | U.S. state whose NIL compliance rules the record describes. |
| `monetization_allowed` | logical | Whether the state allows high-school athletes to monetize their NIL. |
| `governing_rule_label` | character | Name of the governing body or rule for NIL in the state. |
| `governing_rule_url` | character | URL of the governing NIL rule or policy document. |
| `current_rules` | character | Text of the state's current NIL rules, as tracked by On3. |

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
| `person` | character | Nested On3 person object for the ranked athlete (stringified). |
| `valuation` | character | Athlete's On3 NIL valuation in dollars. |

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
| `state` | character | U.S. state the draft-class grouping covers. |
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
| `five_stars` | integer | Number of the organization's drafted players who were five-star recruits. |
| `four_stars` | integer | Number of the organization's drafted players who were four-star recruits. |
| `three_stars` | integer | Number of the organization's drafted players who were three-star recruits. |
| `zero_stars` | integer | Number of the organization's drafted players who were unrated (zero-star) recruits. |
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
| `blue_chip_percent` | numeric | Percent of the year's drafted players who were blue-chip (four- or five-star) recruits. |
| `talent_ratio` | numeric | Ratio of the group's draft share to its recruit-population share for the year (On3's talent ratio). |
| `five_stars` | integer | Number of the organization's players drafted that year who were five-star recruits. |
| `four_stars` | integer | Number of the organization's players drafted that year who were four-star recruits. |
| `three_stars` | integer | Number of the organization's players drafted that year who were three-star recruits. |
| `zero_stars` | integer | Number of the organization's players drafted that year who were unrated (zero-star) recruits. |
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
| `year_summary` | character | Nested summary of the organization's draft ranking for the single year (stringified). |
| `span_summary` | character | Nested summary of the organization's draft ranking over the multi-year span (stringified). |

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
| `recruitment_key` | integer | On3 RDB recruitment key linking the draft pick back to the player's recruitment record. |
| `organization` | character | Organization. |
| `drafted_from_organization` | character | Nested On3 organization object for the school the player was drafted out of (stringified). |
| `high_school_organization` | character | Nested On3 organization object for the player's high school (stringified). |
| `college_organization` | character | Nested On3 organization object for the college the player attended (stringified). |
| `hometown` | character | Prospect hometown. |
| `state` | character | Home state of the drafted player, per On3. |
| `pick` | integer | Pick number of the NFL draftee within the round they were picked in. |
| `compensatory` | logical | Whether the selection was a compensatory draft pick. |
| `supplementary` | logical | Whether the selection came in a supplemental draft. |
| `traded` | logical | Whether the pick was traded. |
| `forfeited` | logical | Whether the pick was forfeited. |
| `trading_organization` | character | Pro organization that traded the pick away, when it changed hands (On3 RDB). |
| `through_organization_one` | character | First intermediate organization the pick passed through in trades before being exercised (On3 RDB). |
| `through_organization_two` | character | Second intermediate organization the pick passed through in trades before being exercised (On3 RDB). |
| `through_organization_three` | character | Third intermediate organization the pick passed through in trades before being exercised (On3 RDB). |
| `key` | integer | On3 RDB key for the draft-pick record. |
| `round` | integer | Round of NFL draft the draftee was picked in. |
| `overall_pick` | integer | Overall pick number in the draft. |
| `person` | character | Nested On3 person object for the drafted player (stringified). |
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
| `nat_total_drafted` | integer | National total of drafted players in the comparison set. |
| `total_drafted` | integer | Total number of the organization's players drafted. |
| `draft_rank` | integer | Organization's national rank by draft production. |
| `organization` | character | Organization. |
| `overall_star_summary` | character | Nested draft summary across all star tiers (stringified). |
| `five_star_summary` | character | Nested draft summary for the organization's five-star recruits (stringified). |
| `four_star_summary` | character | Nested draft summary for the organization's four-star recruits (stringified). |
| `three_star_summary` | character | Nested draft summary for the organization's three-star recruits (stringified). |

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
| `pso_key` | integer | On3 player-sport-organization (PSO) key for the roster entry. |
| `player` | character | Player name. |
| `organization` | character | Organization. |
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `roster_rating` | character | Nested On3 roster rating object for the player (stringified). |
| `status` | character | Game status (e.g. "scheduled", "in_progress", "completed"). |
| `nil_value` | character | Player's On3 NIL valuation in dollars. |
| `rpm` | character | Nested On3 Recruiting Prediction Machine (RPM) data for the player (stringified). |
| `industry_comparison` | character | Nested comparison of the player's On3 rating against the industry-consensus rating (stringified). |

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
| `head_coach` | character | Nested On3 person object for the program's head coach (stringified). |
| `talent_rank` | character | Program's current national roster-talent rank per On3. |
| `prev_talent_rank` | character | Program's roster-talent rank in the previous cycle. |
| `conference_rank` | character | Program's roster-talent rank within its conference. |
| `prev_conference_rank` | character | Program's conference roster-talent rank in the previous cycle. |
| `average_rating` | character | Average On3 rating across the roster. |
| `prev_average_rating` | character | Average On3 roster rating in the previous cycle. |
| `average_nil_value` | numeric | Average On3 NIL valuation across the roster, in dollars. |
| `total_nil_value` | integer | Total On3 NIL valuation across the roster, in dollars. |

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
| `measurement_type_key` | integer | On3 key for the measurement category. |
| `measurement_type` | character | Measurement category (e.g. height, weight, 40-yard dash) per On3's measurement taxonomy. |
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
| `nil_status` | character | Status of the athlete's On3 NIL valuation (e.g. active, inactive). |
| `valuation` | integer | Athlete's latest On3 NIL valuation in dollars. |
| `valuation_change` | integer | Change in the NIL valuation since the previous update, in dollars. |
| `followers` | integer | Total social-media followers counted toward the valuation. |
| `rank` | integer | Position of the school within the poll for the given week (1 = top-ranked). |
| `last_updated` | integer | Timestamp ESPN last refreshed the power index. |
| `social_valuations` | character | Per-platform breakdown of the social components of the valuation (stringified list). |
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
| `key` | integer | On3 RDB key for the measurement record. |
| `measurement_type` | character | Measurement category (e.g. height, weight, 40-yard dash) per On3's measurement taxonomy. |
| `measurement_type_key` | integer | On3 key for the measurement category. |
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `value` | numeric | Metric value. |
| `delta` | numeric | Change in the measured value versus the player's previous measurement of the same type. |
| `person_key` | integer | On3 person key of the measured athlete. |
| `verified` | logical | Whether On3 verified the measurement. |
| `verified_by_user_key` | integer | On3 user key of the staffer who verified the measurement. |
| `elite` | logical | Whether On3 flags the result as elite for this measurement type. |
| `event_key` | integer | On3 key of the camp or combine event where the measurement was taken. |
| `event_name` | character | Event name (e.g. 'All-Star Workout Day: Home Run Derby'). |
| `event` | character | Binary flag indicating the row is a counted game event (excludes end markers). |
| `age_measurement_occurred` | numeric | Athlete's age when the measurement was taken. |
| `top300_average` | numeric | Average value of this measurement among On3 Top300-ranked players. |
| `top_average_change_percent` | numeric | Percent difference between the athlete's value and the Top300 average. |
| `drafted_average` | numeric | Average value of this measurement among drafted players at the combine. |
| `record` | character | Team win-loss record for the season. |
| `draft_change_percent` | numeric | Percent difference between the athlete's value and the drafted-player average. |
| `date_added` | integer | Date the measurement record was added to the On3 database. |
| `date_modified` | integer | Date and time that injury information was updated |
| `date_occurred` | integer | Date the measurement was actually taken. |
| `is_current` | logical | Whether this is the athlete's current (most recent) measurement of the type. |
| `person_sport_org_key` | integer | On3 player-sport-organization (PSO) key the measurement is attached to. |
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
| `measurement_key` | integer | On3 key for the measurement category being averaged. |
| `measurement_name` | character | Name of the measurement category (e.g. height, 40-yard dash). |
| `current_person_measurement` | numeric | Athlete's current value for the measurement. |
| `current_measurement_verified` | logical | Whether the athlete's current measurement is verified by On3. |
| `top300_difference` | numeric | Difference between the athlete's value and the On3 Top300 average. |
| `top300_average` | numeric | Average value of the measurement among On3 Top300-ranked players. |
| `combine_drafted_average` | numeric | Average combine value of the measurement among drafted players. |
| `combine_drafted_difference` | numeric | Difference between the athlete's value and the drafted-player combine average. |
| `sort` | character | Display sort order of the measurement row on the On3 profile. |
| `measurement_record` | character | Nested On3 record for the athlete's underlying measurement (stringified). |

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
| `connection` | character | Relationship type linking the two people (e.g. sibling, parent, teammate) per On3. |
| `connected_player` | character | Nested On3 person object for the connected player (stringified). |
| `connected_roster_rating` | character | Nested On3 roster rating object for the connected player (stringified). |
| `connected_rating` | character | Nested On3 recruiting rating object for the connected player (stringified). |
| `connected_college_organization` | character | Nested On3 organization object for the connected player's college (stringified). |
| `connected_draft` | character | Nested On3 draft record for the connected player, when drafted (stringified). |

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
| `handle` | character | Athlete's account handle on the social platform. |
| `handshake` | logical | On3 RDB handshake field on the social-account record (platform link/verification metadata). |

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
| `social_type` | character | Social platform the post summary covers (e.g. Twitter/X, Instagram). |
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `followers` | integer | Athlete's follower count on the platform. |

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
| `key` | integer | On3 RDB key for the measurement record. |
| `measurement_type` | character | Measurement category (e.g. height, weight, 40-yard dash) per On3's measurement taxonomy. |
| `measurement_type_key` | integer | On3 key for the measurement category. |
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `value` | numeric | Metric value. |
| `delta` | numeric | Change in the measured value versus the player's previous measurement of the same type. |
| `person_key` | integer | On3 person key of the measured athlete. |
| `verified` | logical | Whether On3 verified the measurement. |
| `verified_by_user_key` | integer | On3 user key of the staffer who verified the measurement. |
| `elite` | logical | Whether On3 flags the result as elite for this measurement type. |
| `event_key` | integer | On3 key of the camp or combine event where the measurement was taken. |
| `event_name` | character | Event name (e.g. 'All-Star Workout Day: Home Run Derby'). |
| `event` | character | Binary flag indicating the row is a counted game event (excludes end markers). |
| `age_measurement_occurred` | numeric | Athlete's age when the measurement was taken. |
| `top300_average` | numeric | Average value of this measurement among On3 Top300-ranked players. |
| `top_average_change_percent` | numeric | Percent difference between the athlete's value and the Top300 average. |
| `drafted_average` | numeric | Average value of this measurement among drafted players at the combine. |
| `record` | character | Team win-loss record for the season. |
| `draft_change_percent` | numeric | Percent difference between the athlete's value and the drafted-player average. |
| `date_added` | integer | Date the measurement record was added to the On3 database. |
| `date_modified` | integer | Date and time that injury information was updated |
| `date_occurred` | integer | Date the measurement was actually taken. |
| `is_current` | logical | Whether this is the athlete's current (most recent) measurement of the type. |
| `person_sport_org_key` | integer | On3 player-sport-organization (PSO) key the measurement is attached to. |
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
| `nil_status` | character | Status of the athlete's On3 NIL valuation at the snapshot (e.g. active, inactive). |
| `valuation` | integer | Athlete's On3 NIL valuation in dollars at the snapshot. |
| `valuation_change` | integer | Change in the NIL valuation versus the previous snapshot, in dollars. |
| `date` | character | Date of the On3 NIL valuation snapshot. |
| `date_unix` | integer | Unix timestamp of the valuation snapshot. |

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
| `key` | integer | On3 RDB key for the person-connection record. |
| `person_key` | integer | On3 person key of the profile the connection belongs to. |
| `connected_person_key` | integer | On3 person key of the connected person. |
| `sport_key` | integer | On3 sport key the connection is scoped to. |
| `description` | character | ESPN's description of the stat. |
| `connected_person_sport` | character | Nested athlete-sport profile of the connected person (stringified). |

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
| `ratings` | character | Athlete's ratings across ranking cycles, as a stringified list. |
| `person` | character | Nested On3 person object for the ranked athlete (stringified). |
| `nil_value` | integer | Athlete's On3 NIL valuation in dollars. |

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
| `ranking_key` | integer | On3 key of the ranking cycle the row belongs to. |
| `ranking_type` | character | Poll type code (e.g. `ap`, `coaches`, `cfp`). |
| `rating` | numeric | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `sport` | character | Nested On3 sport object for the ranking row (stringified). |
| `class_year` | integer | Recruiting class year the ranking covers. |
| `state_rank` | integer | State ranking. |
| `state_abbr` | character | Two-letter abbreviation of the player's home state. |
| `position_rank` | integer | Position ranking. |
| `position_abbr` | character | Position abbreviation. |
| `overall_rank` | integer | Overall recruit ranking (top recruits only; may be `NA`). |
| `stars` | integer | Recruit star rating on the 247Sports scale (2-5). |
| `five_star_plus` | logical | Whether On3 designates the player a Five-Star Plus+ prospect. |
| `nearly_five_star_plus` | logical | On3 flag that the player narrowly missed the Five-Star Plus+ designation. |
| `change` | character | Rank movement since the previous ranking cycle. |

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
| `key` | integer | On3 RDB key for the database-update entry. |
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `text` | character | Full play description. |
| `replacement_text` | character | Rendered text of the update entry (with references substituted in). |
| `link` | character | API link to the game feed. |
| `date_added` | integer | Date the update entry was logged. |
| `date_occurred` | integer | Date the underlying event occurred. |
| `object_key` | integer | On3 key of the object the update refers to. |
| `sport_key` | integer | On3 sport key the update is scoped to. |
| `person_key` | integer | On3 person key of the player the update concerns. |
| `organization_key` | integer | On3 organization key involved in the update, when any. |

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
| `key` | integer | On3 asset key for the image. |
| `domain_override` | character | Override CDN domain for serving the image, when set. |
| `domain` | character | CDN domain the image is served from. |
| `source_override` | character | Override source attribution for the image, when set. |
| `source` | character | News source. |
| `title` | character | Specific role title for the assignment. |
| `description` | character | ESPN's description of the stat. |
| `caption` | character | Caption text for the image. |
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |
| `alt_text` | character | Alt text for the image. |
| `height` | integer | Listed height (inches). |
| `width` | integer | Image width in pixels. |
| `asset_type` | character | Type of the asset (e.g. image) in On3's asset system. |
| `file_system` | character | Storage file system the asset lives on (On3 asset metadata). |
| `path` | character | Storage path of the image file. |
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `thumbnail` | character | URL or path of the image's thumbnail rendition. |
| `duration` | integer | Duration. |
| `mime_type` | character | MIME type of the image file (e.g. image/jpeg). |

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
| `organizations` | character | Player's organization stints (high school, college, pro) as a stringified list of nested objects. |
| `draft` | character | Nested On3 draft record for the player, when drafted (stringified). |

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
| `exp_min` | integer | Minimum years of experience listed for the player's stint with the organization (On3 RDB). |
| `exp_max` | integer | Maximum years of experience listed for the player's stint with the organization (On3 RDB). |
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
| `key` | integer | On3 RDB key for the player profile. |
| `class_year_recruitment_key` | character | On3 recruitment key for the player's recruiting-class cycle. |
| `recruitment_key` | integer | On3 key of the player's active recruitment record. |
| `person_can_manage_recruitment` | character | Whether the athlete can self-manage the recruitment on On3. |
| `ranking_key` | integer | On3 key of the ranking cycle the profile's rating belongs to. |
| `person_sport_key` | integer | On3 key of the athlete-sport profile (person x sport). |
| `ranking` | character | National rank of the team's overall SP+ rating (1 = best). |
| `oracle_key` | character | On3's internal oracle identifier for the player record. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `slug` | character | URL slug for the team. |
| `high_school_name` | character | Recruit high-school name. |
| `high_school` | character | High school |
| `hometown_name` | character | Player's hometown, as listed by On3. |
| `hometown_state` | character | Recruit hometown state. |
| `current_state` | character | Current home venue state. |
| `default_asset` | character | Nested On3 asset object for the player's headshot (stringified). |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`); `position_detail = TRUE` only. |
| `primary_position` | character | Nested On3 object for the player's primary position (stringified). |
| `class_rank` | character | Player's rank within their recruiting class. |
| `height` | character | Listed height (inches). |
| `weight` | integer | Listed weight (lbs). |
| `class_year` | integer | Player's recruiting class year. |
| `degree` | character | Degree the player earned or is pursuing, when listed. |
| `age` | integer | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `default_sport` | character | Nested On3 object for the player's primary sport (stringified). |
| `sports` | character | Sports the player is profiled in, as a stringified list. |
| `description` | character | ESPN's description of the stat. |
| `bio_pro_prospect` | character | Bio text framing the player as a pro prospect (On3 RDB). |
| `bio_college_recruit` | character | Bio text framing the player as a college recruit (On3 RDB). |
| `organization_level` | character | Level of the player's current organization (e.g. high school, college, professional). |
| `high_school_org_key` | integer | On3 organization key of the player's high school. |
| `prep_school_org_key` | character | On3 organization key of the player's prep school, when attended. |
| `junior_college_org_key` | character | On3 organization key of the player's junior college, when attended. |
| `college_org_key` | character | On3 organization key of the player's college. |
| `nil_value` | integer | Player's On3 NIL valuation in dollars. |
| `athlete_verified` | logical | Whether the athlete has verified their own On3 profile. |
| `prospect_verified` | logical | Whether On3 has verified the prospect's profile information. |
| `player_status` | character | Player's current status per On3 (e.g. active, transfer portal). |
| `is_coach` | logical | Whether the person record is a coach. |
| `is_athlete` | logical | Whether the person record is an athlete. |
| `visibility` | character | Profile visibility setting on On3. |
| `tier` | character | On3 profile tier classification for the player. |
| `review_status` | character | Editorial review status of the profile in the On3 database. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `badge` | character | Profile badge assigned by On3, when any. |
| `ncaa_id` | character | Player's NCAA identifier, when known to On3. |
| `managed_by_user` | logical | On3 user account that manages the player's profile, when claimed. |

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
| `sport` | character | Nested On3 sport object for the target entry (stringified). |
| `coaches` | character | Recruiting coaches at the target school tied to the recruitment (stringified list). |
| `status` | character | Game status (e.g. "scheduled", "in_progress", "completed"). |
| `interest` | integer | Recruit's interest level in the target school, per On3. |
| `distance` | numeric | Yards to gain for a first down (or to the goal line in goal-to-go situations). |
| `class_rank` | integer | Recruit's rank within their recruiting class. |
| `official_visit_count` | integer | Number of official visits the recruit has taken to the school. |
| `un_official_visit_count` | integer | Number of unofficial visits the recruit has taken to the school. |
| `prediction` | numeric | Pre-game prediction (favorite, score, win %). |
| `committed_date` | character | Date the recruit committed to the school, when applicable. |
| `draft_position_count` | integer | Number of players at the recruit's position the school has had drafted. |
| `draft_total` | integer | Total number of players the school has had drafted. |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`); `position_detail = TRUE` only. |
| `position_key` | integer | On3 key of the recruit's position. |

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
| `key` | integer | On3 RDB key for the player profile. |
| `class_year_recruitment_key` | character | On3 recruitment key for the player's recruiting-class cycle. |
| `recruitment_key` | integer | On3 key of the player's active recruitment record. |
| `person_can_manage_recruitment` | character | Whether the athlete can self-manage the recruitment on On3. |
| `ranking_key` | integer | On3 key of the ranking cycle the profile's rating belongs to. |
| `person_sport_key` | integer | On3 key of the athlete-sport profile (person x sport). |
| `ranking` | character | National rank of the team's overall SP+ rating (1 = best). |
| `oracle_key` | character | On3's internal oracle identifier for the player record. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `slug` | character | URL slug for the team. |
| `high_school_name` | character | Recruit high-school name. |
| `high_school` | character | High school |
| `hometown_name` | character | Player's hometown, as listed by On3. |
| `hometown_state` | character | Recruit hometown state. |
| `current_state` | character | Current home venue state. |
| `default_asset` | character | Nested On3 asset object for the player's headshot (stringified). |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`); `position_detail = TRUE` only. |
| `primary_position` | character | Nested On3 object for the player's primary position (stringified). |
| `class_rank` | character | Player's rank within their recruiting class. |
| `height` | character | Listed height (inches). |
| `weight` | integer | Listed weight (lbs). |
| `class_year` | integer | Player's recruiting class year. |
| `degree` | character | Degree the player earned or is pursuing, when listed. |
| `age` | integer | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `default_sport` | character | Nested On3 object for the player's primary sport (stringified). |
| `sports` | character | Sports the player is profiled in, as a stringified list. |
| `description` | character | ESPN's description of the stat. |
| `bio_pro_prospect` | character | Bio text framing the player as a pro prospect (On3 RDB). |
| `bio_college_recruit` | character | Bio text framing the player as a college recruit (On3 RDB). |
| `organization_level` | character | Level of the player's current organization (e.g. high school, college, professional). |
| `high_school_org_key` | integer | On3 organization key of the player's high school. |
| `prep_school_org_key` | character | On3 organization key of the player's prep school, when attended. |
| `junior_college_org_key` | character | On3 organization key of the player's junior college, when attended. |
| `college_org_key` | character | On3 organization key of the player's college. |
| `nil_value` | integer | Player's On3 NIL valuation in dollars. |
| `athlete_verified` | logical | Whether the athlete has verified their own On3 profile. |
| `prospect_verified` | logical | Whether On3 has verified the prospect's profile information. |
| `player_status` | character | Player's current status per On3 (e.g. active, transfer portal). |
| `is_coach` | logical | Whether the person record is a coach. |
| `is_athlete` | logical | Whether the person record is an athlete. |
| `visibility` | character | Profile visibility setting on On3. |
| `tier` | character | On3 profile tier classification for the player. |
| `review_status` | character | Editorial review status of the profile in the On3 database. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `badge` | character | Profile badge assigned by On3, when any. |
| `ncaa_id` | character | Player's NCAA identifier, when known to On3. |
| `managed_by_user` | logical | On3 user account that manages the player's profile, when claimed. |

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
| `sport` | character | Nested On3 sport object for the visit-center entry (stringified). |
| `year` | integer | Four-digit season year (e.g. 2019). |
| `total_visits` | integer | Total number of school visits logged for the recruit. |
| `visits` | character | The recruit's school visits with dates and types, as a stringified list. |

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
| `ratings` | character | Player's ratings across the industry services, as a stringified list. |
| `person` | character | Nested On3 person object for the compared player (stringified). |
| `nil_value` | integer | Player's On3 NIL valuation in dollars. |

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
| `key` | integer | On3 RDB key for the quote record. |
| `body` | character | Full text of the quote. |
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |
| `person_key` | integer | On3 person key of the person quoted or quoted about. |
| `person` | character | Nested On3 person object for the quote's subject (stringified). |
| `date_added` | character | Date the quote was added to the On3 database. |
| `date_updated` | character | Date the quote was last updated. |

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
| `key` | integer | On3 RDB key for the quote record. |
| `body` | character | Full text of the quote. |
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |
| `person_key` | integer | On3 person key of the person quoted or quoted about. |
| `person` | character | Nested On3 person object for the quote's subject (stringified). |
| `date_added` | character | Date the quote was added to the On3 database. |
| `date_updated` | character | Date the quote was last updated. |

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
| `class_year` | integer | Recruiting class year of the recruitment. |
| `committed_status` | character | Nested commitment status of the recruitment (stringified). |
| `high_school` | character | High school |
| `high_school_org` | character | Nested On3 organization object for the recruit's high school (stringified). |
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
| `key` | integer | On3 RDB key for the RPM pick. |
| `organization` | character | Organization. |
| `date_added` | character | Date the expert logged the RPM pick. |
| `expert` | character | Nested On3 record for the expert who logged the pick (stringified). |
| `confidence` | numeric | Expert's confidence level in the prediction. |
| `description` | character | ESPN's description of the stat. |
| `article_link` | character | Link to the On3 article accompanying the pick, when any. |
| `premium` | logical | Whether the article is premium content. |
| `correct` | logical | Whether the pick proved correct. |
| `days_correct` | numeric | Number of days the pick stood as correct. |
| `flipped_from_organization` | character | Organization the expert's pick flipped from, when the prediction changed (stringified). |
| `previous_confidence` | numeric | Expert's confidence level on their previous pick for this recruitment. |
| `previous_date_added` | character | Date the expert's previous pick was logged. |
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `top_teams` | character | Teams currently leading for the recruit per the pick, as a stringified list. |

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
| `predictions` | character | Per-team RPM prediction percentages for the recruitment, as a stringified list. |
| `locked` | logical | Whether the RPM prediction for the recruitment is locked (no longer updating). |

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
| `key` | integer | On3 RDB key for the team's class-ranking row. |
| `organization` | character | Organization. |
| `applied_total_rating` | numeric | Sum of counted commits' On3 ratings applied to the class score. |
| `applied_total_consensus_rating` | numeric | Sum of counted commits' industry-consensus ratings applied to the class score. |
| `applied_average_rating` | numeric | Average On3 rating across the counted commits. |
| `applied_average_consensus_rating` | numeric | Average industry-consensus rating across the counted commits. |
| `commits` | integer | Number of commits in the team's recruiting class. |
| `applied_commits` | integer | Number of commits counted toward the class score. |
| `deductions` | numeric | Points deducted from the team's class score (On3 ranking formula). |
| `deductions_description` | character | Explanation of the deductions applied to the class score. |
| `five_stars` | integer | Number of five-star commits by On3's own rating. |
| `consensus_five_stars` | integer | Number of five-star commits by industry-consensus rating. |
| `four_stars` | integer | Number of four-star commits by On3's own rating. |
| `consensus_four_stars` | integer | Number of four-star commits by industry-consensus rating. |
| `three_stars` | integer | Number of three-star commits by On3's own rating. |
| `consensus_three_stars` | integer | Number of three-star commits by industry-consensus rating. |
| `overall_rank` | integer | Overall recruit ranking (top recruits only; may be `NA`). |
| `overall_consensus_rank` | integer | Team's national class rank by industry-consensus score. |
| `dispay_consensus_score` | numeric | Display-formatted consensus class score (the 'dispay' spelling is On3's own field name). |
| `dispay_on3_score` | numeric | Display-formatted On3 class score (the 'dispay' spelling is On3's own field name). |
| `average_nil_value` | numeric | Average On3 NIL valuation across the class's commits, in dollars. |
| `conference_rank` | integer | Team's class rank within its conference by On3 score. |
| `conference_consensus_rank` | integer | Team's class rank within its conference by consensus score. |

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
| `conference_rank` | integer | Team's blue-chip class rank within its conference. |
| `in_state_count` | numeric | Number of commits from the school's home state. |
| `average_distance` | numeric | Average distance from the commits' hometowns to campus, in miles. |
| `blue_chips` | numeric | Number of blue-chip (four- or five-star) commits in the class. |
| `social_nil_values` | numeric | Nested aggregate of the class's social/NIL values (stringified). |
| `score` | numeric | Final score string. |
| `total_commits` | integer | Total number of commits in the class. |

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
| `key` | integer | On3 RDB key for the team's class-ranking row. |
| `organization` | character | Organization. |
| `applied_total_rating` | numeric | Sum of counted commits' On3 ratings applied to the class score. |
| `applied_total_consensus_rating` | numeric | Sum of counted commits' industry-consensus ratings applied to the class score. |
| `applied_average_rating` | numeric | Average On3 rating across the counted commits. |
| `applied_average_consensus_rating` | numeric | Average industry-consensus rating across the counted commits. |
| `commits` | integer | Number of commits in the team's recruiting class. |
| `applied_commits` | integer | Number of commits counted toward the class score. |
| `deductions` | numeric | Points deducted from the team's class score (On3 ranking formula). |
| `deductions_description` | character | Explanation of the deductions applied to the class score. |
| `five_stars` | integer | Number of five-star commits by On3's own rating. |
| `consensus_five_stars` | integer | Number of five-star commits by industry-consensus rating. |
| `four_stars` | integer | Number of four-star commits by On3's own rating. |
| `consensus_four_stars` | integer | Number of four-star commits by industry-consensus rating. |
| `three_stars` | integer | Number of three-star commits by the On3 consensus rating. |
| `consensus_three_stars` | integer | Number of three-star commits by industry-consensus rating. |
| `overall_rank` | integer | Overall recruit ranking (top recruits only; may be `NA`). |
| `overall_consensus_rank` | integer | Team's national class rank by industry-consensus score. |
| `dispay_consensus_score` | numeric | Display-formatted consensus class score (the 'dispay' spelling is On3's own field name). |
| `dispay_on3_score` | numeric | Display-formatted On3 class score (the 'dispay' spelling is On3's own field name). |
| `average_nil_value` | numeric | Average On3 NIL valuation across the class's commits, in dollars. |
| `conference_rank` | integer | Team's class rank within its conference by On3 score. |
| `conference_consensus_rank` | integer | Team's class rank within its conference by consensus score. |

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
| `total_commits` | integer | Total number of commits in the class. |
| `class_rating_current` | numeric | Team's current On3 class rating. |
| `class_rating_previous` | numeric | Team's class rating in the previous cycle. |
| `class_rating_change` | character | Change in the class rating versus the previous cycle. |
| `national_rank_current` | integer | Team's current national class rank. |
| `national_rank_previous` | integer | Team's national class rank in the previous cycle. |
| `national_rank_change` | character | Change in national class rank versus the previous cycle. |
| `conference_rank_current` | integer | Team's current class rank within its conference. |
| `conference_rank_previous` | integer | Team's conference class rank in the previous cycle. |
| `conference_rank_change` | character | Change in conference class rank versus the previous cycle. |
| `conference_consensus_rank_current` | integer | Team's current conference class rank by consensus score. |
| `conference_consensus_rank_previous` | integer | Team's conference consensus class rank in the previous cycle. |
| `conference_consensus_rank_change` | character | Change in conference consensus class rank versus the previous cycle. |
| `in_state` | numeric | Number of commits from the school's home state. |
| `avg_distance` | numeric | Average batted-ball distance (feet). |
| `blue_chips` | numeric | Number of blue-chip (four- or five-star) commits in the class. |
| `head_coach` | character | Nested On3 person object for the program's head coach (stringified). |
| `director_personnel` | character | Nested On3 person object for the program's director of player personnel (stringified). |
| `average_nil_value` | numeric | Average On3 NIL valuation across the class's commits, in dollars. |

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

### Example

```python
on3_videos_video_key(video_key=1)
```

_Last validated n/a._
