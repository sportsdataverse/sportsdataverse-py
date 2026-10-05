---
title: "CFB — On3 Recruit Database (api.on3.com) — Team"
sidebar_label: "Team"
sidebar_position: 6
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

### Example {#on3_team_ranking_team_rankings-example}

```python
on3_team_ranking_team_rankings(sport_slug='football', year=2025)
```

_Last validated n/a._
