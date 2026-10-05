---
title: "CFB — On3 Recruit Database (api.on3.com) — Draft"
sidebar_label: "Draft"
sidebar_position: 1
description: "CFB — On3 Recruit Database (api.on3.com) — Draft — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB — On3 Recruit Database (api.on3.com) — Draft

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

### Example {#on3_drafts_players-example}

```python
on3_drafts_players(org_key=1867)
```

_Last validated n/a._
