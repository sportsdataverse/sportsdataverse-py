---
title: "CFB — On3 Recruit Database (api.on3.com) — Organizations"
sidebar_label: "Organizations"
sidebar_position: 2
description: "CFB — On3 Recruit Database (api.on3.com) — Organizations — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB — On3 Recruit Database (api.on3.com) — Organizations

## on3_organizations_draft_class_by_state

GET /rdb/v1/organizations/{organizationKey}/draft-class-by-state

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/draft-class-by-state`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/draft-class-by-state](https://api.on3.com/public/rdb/v1/organizations/1867/draft-class-by-state)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_organizations_draft_class_by_state-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `state` | character | U.S. state the draft-class grouping covers. |
| `count` | integer | Total number of players in the season index. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_organizations_draft_class_by_state-example}

```python
on3_organizations_draft_class_by_state(organization_key=1867)
```

_Last validated n/a._

## on3_organizations_draft_class_by_year

GET /rdb/v1/organizations/{organizationKey}/draft-class-by-year

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/draft-class-by-year`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/draft-class-by-year](https://api.on3.com/public/rdb/v1/organizations/1867/draft-class-by-year)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_organizations_draft_class_by_year-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `year` | integer | Four-digit season year (e.g. 2019). |
| `count` | integer | Total number of players in the season index. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_organizations_draft_class_by_year-example}

```python
on3_organizations_draft_class_by_year(organization_key=1867)
```

_Last validated n/a._

## on3_organizations_draft_count_by_stars

GET /rdb/v1/organizations/{organizationKey}/draft-count-by-stars

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/draft-count-by-stars`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/draft-count-by-stars](https://api.on3.com/public/rdb/v1/organizations/1867/draft-count-by-stars)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |

### Returns {#on3_organizations_draft_count_by_stars-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `five_stars` | integer | Number of the organization's drafted players who were five-star recruits. |
| `four_stars` | integer | Number of the organization's drafted players who were four-star recruits. |
| `three_stars` | integer | Number of the organization's drafted players who were three-star recruits. |
| `zero_stars` | integer | Number of the organization's drafted players who were unrated (zero-star) recruits. |
| `total` | integer | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_organizations_draft_count_by_stars-example}

```python
on3_organizations_draft_count_by_stars(organization_key=1867)
```

_Last validated n/a._

## on3_organizations_draft_count_by_year

GET /rdb/v1/organizations/{organizationKey}/draft-count-by-year

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/draft-count-by-year`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/draft-count-by-year](https://api.on3.com/public/rdb/v1/organizations/1867/draft-count-by-year)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_organizations_draft_count_by_year-returns}

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

### Example {#on3_organizations_draft_count_by_year-example}

```python
on3_organizations_draft_count_by_year(organization_key=1867)
```

_Last validated n/a._

## on3_organizations_draft_ranking_summary

GET /rdb/v1/organizations/{organizationKey}/draft-ranking-summary

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/draft-ranking-summary`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/draft-ranking-summary](https://api.on3.com/public/rdb/v1/organizations/1867/draft-ranking-summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |

### Returns {#on3_organizations_draft_ranking_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `conference` | character | Conference of the team. |
| `year_summary` | character | Nested summary of the organization's draft ranking for the single year (stringified). |
| `span_summary` | character | Nested summary of the organization's draft ranking over the multi-year span (stringified). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_organizations_draft_ranking_summary-example}

```python
on3_organizations_draft_ranking_summary(organization_key=1867)
```

_Last validated n/a._

## on3_organizations_drafted_players

GET /rdb/v1/organizations/{organizationKey}/drafted-players

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/drafted-players`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/drafted-players](https://api.on3.com/public/rdb/v1/organizations/1867/drafted-players)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_organizations_drafted_players-returns}

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

### Example {#on3_organizations_drafted_players-example}

```python
on3_organizations_drafted_players(organization_key=1867)
```

_Last validated n/a._

## on3_organizations_drafts_by_stars_summary

GET /rdb/v1/organizations/{organizationKey}/drafts-by-stars-summary

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/drafts-by-stars-summary`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/drafts-by-stars-summary](https://api.on3.com/public/rdb/v1/organizations/1867/drafts-by-stars-summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |

### Returns {#on3_organizations_drafts_by_stars_summary-returns}

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

### Example {#on3_organizations_drafts_by_stars_summary-example}

```python
on3_organizations_drafts_by_stars_summary(organization_key=1867)
```

_Last validated n/a._

## on3_organizations_roster

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

### Returns {#on3_organizations_roster-returns}

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

### Example {#on3_organizations_roster-example}

```python
on3_organizations_roster(organization_key=1867)
```

_Last validated n/a._

## on3_organizations_roster_header

GET /rdb/v1/organizations/{organizationKey}/roster-header

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/organizations/{organization_key}/roster-header`

**Valid URL:** [https://api.on3.com/public/rdb/v1/organizations/1867/roster-header](https://api.on3.com/public/rdb/v1/organizations/1867/roster-header)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `organization_key` | `organization_key` |  | `Y` |  | organization_key path parameter. |
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |

### Returns {#on3_organizations_roster_header-returns}

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

### Example {#on3_organizations_roster_header-example}

```python
on3_organizations_roster_header(organization_key=1867)
```

_Last validated n/a._
