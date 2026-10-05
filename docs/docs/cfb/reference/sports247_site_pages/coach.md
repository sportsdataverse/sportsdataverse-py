---
title: "CFB — 247Sports Site Pages (247sports.com) — Coach"
sidebar_label: "Coach"
sidebar_position: 1
description: "CFB — 247Sports Site Pages (247sports.com) — Coach — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB — 247Sports Site Pages (247sports.com) — Coach

## sports247_site_pages_coach_alma_mater

Coach alma-mater Institution.

**Endpoint URL:** `GET https://247sports.com/Coach/{key}/AlmaMater.json`

**Valid URL:** [https://247sports.com/Coach/1504/AlmaMater.json](https://247sports.com/Coach/1504/AlmaMater.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_coach_alma_mater-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `name` | character | Position name (e.g. `Quarterback`). |
| `type` | character | Institution type code (college / pro / high school). |
| `group` | character | Institution group (division/level) bitmask code. |
| `location` | integer | FK -> Location (`/Institution/{Location}/Location.json`). |
| `state` | integer | FK -> State entity. |
| `latitude` | character | Venue latitude in decimal degrees. |
| `longitude` | character | Venue longitude in decimal degrees. |
| `rankable` | character | Whether the institution participates in class rankings. |
| `mascot` | character | Team mascot. |
| `abbreviation` | character | Metric abbreviation. |
| `primary_color` | character | Primary team color (hex). |
| `secondary_color` | character | Secondary team color (hex). |
| `is_foreign` | character | Whether the institution is located outside the United States. |
| `site` | integer | FK -> team Site (network site key). |
| `default_asset` | integer | Nested 247Sports image asset for the institution's primary logo (stringified). |
| `alternate_asset` | integer | Nested 247Sports image asset for the institution's alternate logo (stringified). |
| `light_asset` | integer | Nested 247Sports image asset for the light-background logo variant (stringified). |
| `default_name` | character | Server-rendered display label for the entity. |
| `address` | character | Institution's street address. |
| `telephone` | character | Institution's telephone number. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_coach_alma_mater-example}

```python
sports247_site_pages_coach_alma_mater(key=1504)
```

_Last validated n/a._

## sports247_site_pages_coach_hometown

Coach hometown Location.

**Endpoint URL:** `GET https://247sports.com/Coach/{key}/Hometown.json`

**Valid URL:** [https://247sports.com/Coach/1504/Hometown.json](https://247sports.com/Coach/1504/Hometown.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_coach_hometown-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `postal_code` | character | Postal code of the venue. |
| `city` | character | Venue city. |
| `state` | integer | U.S. state of the location record, per 247Sports. |
| `latitude` | character | Venue latitude in decimal degrees. |
| `longitude` | character | Venue longitude in decimal degrees. |
| `county_tax_rate` | character | County income-tax rate for the location, carried on the 247Sports location record. |
| `city_tax_rate` | character | City income-tax rate for the location, carried on the 247Sports location record. |
| `special_tax_rate` | character | Special-district tax rate for the location, carried on the 247Sports location record. |
| `region_name` | character | Name of the region (state/province) for the location. |
| `default_name` | character | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_coach_hometown-example}

```python
sports247_site_pages_coach_hometown(key=1504)
```

_Last validated n/a._

## sports247_site_pages_coach_ranking

Single CoachRanking row.

**Endpoint URL:** `GET https://247sports.com/CoachRanking/{key}.json`

**Valid URL:** [https://247sports.com/CoachRanking](https://247sports.com/CoachRanking)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_coach_ranking-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `coach` | integer | Coach. |
| `institution` | integer | Nested 247Sports institution the coach recruited for during the ranking cycle (stringified). |
| `conference` | integer | Conference of the team. |
| `ranking` | integer | FK -> the Ranking snapshot this row belongs to. |
| `sport` | integer | Nested 247Sports sport the ranking covers (stringified). |
| `recruitment` | character | Nested 247Sports recruitment record credited to the coach on this row (stringified). |
| `rating` | character | Overall rating score for the coach's recruiting haul in the 247Sports coach ranking. |
| `scout_rating` | character | Total 247Sports in-house (scout) rating points credited to the coach's commits. |
| `composite_rating` | character | Composite class rating for the coach's haul. |
| `commits` | character | Number of commits credited to the coach in the ranking. |
| `total` | character | Total ranking score for the coach's class, as reported by 247Sports. |
| `composite_total` | character | Total 247Sports Composite rating points credited to the coach's commits. |
| `five_stars` | character | Number of five-star commits credited to the coach. |
| `scout_five_stars` | character | Number of five-star commits by 247Sports' own (scout) rating. |
| `composite_five_stars` | character | Number of five-star commits by the 247Sports Composite rating. |
| `four_stars` | character | Number of four-star commits credited to the coach. |
| `scout_four_stars` | character | Number of four-star commits by 247Sports' own (scout) rating. |
| `composite_four_stars` | character | Number of four-star commits by the 247Sports Composite rating. |
| `three_stars` | character | Number of three-star commits credited to the coach. |
| `scout_three_stars` | character | Number of three-star commits by 247Sports' own (scout) rating. |
| `composite_three_stars` | character | Number of three-star commits by the 247Sports Composite rating. |
| `two_stars` | character | Number of two-star commits credited to the coach. |
| `scout_two_stars` | character | Number of two-star commits by 247Sports' own (scout) rating. |
| `composite_two_stars` | character | Number of two-star commits by the 247Sports Composite rating. |
| `average_rating` | character | Average rating across the coach's credited commits. |
| `average_scout_rating` | character | Average 247Sports in-house (scout) rating across the credited commits. |
| `composite_average_rating` | character | Average 247Sports Composite rating across the credited commits. |
| `overall_rank` | character | Overall national coach-recruiting rank. |
| `composite_overall_rank` | character | Coach's national recruiter rank by Composite points. |
| `scout_overall_rank` | character | Coach's national recruiter rank by 247Sports' own (scout) points. |
| `division_rank` | character | Coach's recruiter rank within the division. |
| `scout_division_rank` | character | Coach's division recruiter rank by 247Sports' own (scout) points. |
| `composite_division_rank` | character | Coach's division recruiter rank by Composite points. |
| `conference_rank` | character | Rank within conference. |
| `scout_conference_rank` | character | Coach's conference recruiter rank by 247Sports' own (scout) points. |
| `composite_conference_rank` | character | Coach's conference recruiter rank by Composite points. |
| `previous_coach_ranking` | integer | Nested prior-cycle recruiter-ranking row for the coach (stringified). |
| `default_name` | character | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_coach_ranking-example}

```python
sports247_site_pages_coach_ranking()
```

_Last validated n/a._

## sports247_site_pages_coach_rankings

Coach's recruiting-ranking history (one row per Ranking snapshot).

**Endpoint URL:** `GET https://247sports.com/Coach/{key}/CoachRankings.json`

**Valid URL:** [https://247sports.com/Coach/1531/CoachRankings.json](https://247sports.com/Coach/1531/CoachRankings.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_coach_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | Primary key of this entity (the id used in its `.json` route). |
| `coach` | integer | Coach. |
| `institution` | integer | Nested 247Sports institution the coach recruited for during the ranking cycle (stringified). |
| `conference` | integer | Conference of the team. |
| `ranking` | integer | FK -> the Ranking snapshot this row belongs to. |
| `sport` | integer | Nested 247Sports sport the ranking covers (stringified). |
| `recruitment` | character | Nested 247Sports recruitment record credited to the coach on this row (stringified). |
| `rating` | character | Overall rating score for the coach's recruiting haul in the 247Sports coach ranking. |
| `scout_rating` | character | Total 247Sports in-house (scout) rating points credited to the coach's commits. |
| `composite_rating` | character | Composite class rating for the coach's haul. |
| `commits` | character | Number of commits credited to the coach in the ranking. |
| `total` | character | Total ranking score for the coach's class, as reported by 247Sports. |
| `composite_total` | character | Total 247Sports Composite rating points credited to the coach's commits. |
| `five_stars` | character | Number of five-star commits credited to the coach. |
| `scout_five_stars` | character | Number of five-star commits by 247Sports' own (scout) rating. |
| `composite_five_stars` | character | Number of five-star commits by the 247Sports Composite rating. |
| `four_stars` | character | Number of four-star commits credited to the coach. |
| `scout_four_stars` | character | Number of four-star commits by 247Sports' own (scout) rating. |
| `composite_four_stars` | character | Number of four-star commits by the 247Sports Composite rating. |
| `three_stars` | character | Number of three-star commits credited to the coach. |
| `scout_three_stars` | character | Number of three-star commits by 247Sports' own (scout) rating. |
| `composite_three_stars` | character | Number of three-star commits by the 247Sports Composite rating. |
| `two_stars` | character | Number of two-star commits credited to the coach. |
| `scout_two_stars` | character | Number of two-star commits by 247Sports' own (scout) rating. |
| `composite_two_stars` | character | Number of two-star commits by the 247Sports Composite rating. |
| `average_rating` | character | Average rating across the coach's credited commits. |
| `average_scout_rating` | character | Average 247Sports in-house (scout) rating across the credited commits. |
| `composite_average_rating` | character | Average 247Sports Composite rating across the credited commits. |
| `overall_rank` | character | Overall national coach-recruiting rank. |
| `composite_overall_rank` | character | Coach's national recruiter rank by Composite points. |
| `scout_overall_rank` | character | Coach's national recruiter rank by 247Sports' own (scout) points. |
| `division_rank` | character | Coach's recruiter rank within the division. |
| `scout_division_rank` | character | Coach's division recruiter rank by 247Sports' own (scout) points. |
| `composite_division_rank` | character | Coach's division recruiter rank by Composite points. |
| `conference_rank` | character | Rank within conference. |
| `scout_conference_rank` | character | Coach's conference recruiter rank by 247Sports' own (scout) points. |
| `composite_conference_rank` | character | Coach's conference recruiter rank by Composite points. |
| `previous_coach_ranking` | integer | Nested prior-cycle recruiter-ranking row for the coach (stringified). |
| `default_name` | character | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sports247_site_pages_coach_rankings-example}

```python
sports247_site_pages_coach_rankings(key=1531)
```

_Last validated n/a._
