# College football — 247Sports Site Pages (247sports.com) — Coach

> College football — 247Sports Site Pages (247sports.com) — Coach — function reference in sdv-py, the SportsDataverse Python package.

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
| `key` | Int64 | Primary key of this entity (the id used in its `.json` route). |
| `name` | String | Position name (e.g. `Quarterback`). |
| `type` | String | Institution type code (college / pro / high school). |
| `group` | String | Institution group (division/level) bitmask code. |
| `location` | Int64 | FK -> Location (`/Institution/{Location}/Location.json`). |
| `state` | Int64 | FK -> State entity. |
| `latitude` | Float64 | Venue latitude in decimal degrees. |
| `longitude` | Float64 | Venue longitude in decimal degrees. |
| `rankable` | String | Whether the institution participates in class rankings. |
| `mascot` | String | Team mascot. |
| `abbreviation` | String | Metric abbreviation. |
| `primary_color` | String |  |
| `secondary_color` | String |  |
| `is_foreign` | String | Whether the institution is located outside the United States. |
| `site` | Int64 | FK -> team Site (network site key). |
| `default_asset` | Float64 | Nested 247Sports image asset for the institution's primary logo (stringified). |
| `alternate_asset` | Float64 | Nested 247Sports image asset for the institution's alternate logo (stringified). |
| `light_asset` | Float64 | Nested 247Sports image asset for the light-background logo variant (stringified). |
| `default_name` | String | Server-rendered display label for the entity. |
| `address` | String | Institution's street address. |
| `telephone` | String | Institution's telephone number. |
| `website` | String | Institution's website URL. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_site_pages_coach_alma_mater-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_site_pages_coach_alma_mater-example}

```python
sports247_site_pages_coach_alma_mater(key=1504)
```

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
| `key` | Int64 | Primary key of this entity (the id used in its `.json` route). |
| `postal_code` | String |  |
| `city` | String | Venue city. |
| `state` | Int64 | U.S. state of the location record, per 247Sports. |
| `latitude` | Float64 | Venue latitude in decimal degrees. |
| `longitude` | Float64 | Venue longitude in decimal degrees. |
| `county_tax_rate` | Float64 | County income-tax rate for the location, carried on the 247Sports location record. |
| `city_tax_rate` | Float64 | City income-tax rate for the location, carried on the 247Sports location record. |
| `special_tax_rate` | Float64 | Special-district tax rate for the location, carried on the 247Sports location record. |
| `region_name` | String | Name of the region (state/province) for the location. |
| `default_name` | String | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_site_pages_coach_hometown-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_site_pages_coach_hometown-example}

```python
sports247_site_pages_coach_hometown(key=1504)
```

## sports247_site_pages_coach_ranking

Single CoachRanking row.

**Endpoint URL:** `GET https://247sports.com/CoachRanking/{key}.json`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#sports247_site_pages_coach_ranking-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | Int64 | Primary key of this entity (the id used in its `.json` route). |
| `coach` | Int64 |  |
| `institution` | Int64 | Nested 247Sports institution the coach recruited for during the ranking cycle (stringified). |
| `conference` | Int64 | Conference of the team. |
| `ranking` | Int64 | FK -> the Ranking snapshot this row belongs to. |
| `sport` | Int64 | Nested 247Sports sport the ranking covers (stringified). |
| `recruitment` | String | Nested 247Sports recruitment record credited to the coach on this row (stringified). |
| `rating` | Float64 | Overall rating score for the coach's recruiting haul in the 247Sports coach ranking. |
| `scout_rating` | Float64 | Total 247Sports in-house (scout) rating points credited to the coach's commits. |
| `composite_rating` | Float64 | Composite class rating for the coach's haul. |
| `commits` | Int64 | Number of commits credited to the coach in the ranking. |
| `total` | Int64 | Total ranking score for the coach's class, as reported by 247Sports. |
| `composite_total` | Int64 | Total 247Sports Composite rating points credited to the coach's commits. |
| `five_stars` | Int64 | Number of five-star commits credited to the coach. |
| `scout_five_stars` | Int64 | Number of five-star commits by 247Sports' own (scout) rating. |
| `composite_five_stars` | Int64 | Number of five-star commits by the 247Sports Composite rating. |
| `four_stars` | Int64 | Number of four-star commits credited to the coach. |
| `scout_four_stars` | Int64 | Number of four-star commits by 247Sports' own (scout) rating. |
| `composite_four_stars` | Int64 | Number of four-star commits by the 247Sports Composite rating. |
| `three_stars` | Int64 | Number of three-star commits credited to the coach. |
| `scout_three_stars` | Int64 | Number of three-star commits by 247Sports' own (scout) rating. |
| `composite_three_stars` | Int64 | Number of three-star commits by the 247Sports Composite rating. |
| `two_stars` | Int64 | Number of two-star commits credited to the coach. |
| `scout_two_stars` | Int64 | Number of two-star commits by 247Sports' own (scout) rating. |
| `composite_two_stars` | Int64 | Number of two-star commits by the 247Sports Composite rating. |
| `average_rating` | Float64 | Average rating across the coach's credited commits. |
| `average_scout_rating` | Int64 | Average 247Sports in-house (scout) rating across the credited commits. |
| `composite_average_rating` | Float64 | Average 247Sports Composite rating across the credited commits. |
| `overall_rank` | Int64 | Overall national coach-recruiting rank. |
| `composite_overall_rank` | Int64 | Coach's national recruiter rank by Composite points. |
| `scout_overall_rank` | Int64 | Coach's national recruiter rank by 247Sports' own (scout) points. |
| `division_rank` | Int64 | Coach's recruiter rank within the division. |
| `scout_division_rank` | Int64 | Coach's division recruiter rank by 247Sports' own (scout) points. |
| `composite_division_rank` | Int64 | Coach's division recruiter rank by Composite points. |
| `conference_rank` | Int64 | Rank within conference. |
| `scout_conference_rank` | Int64 | Coach's conference recruiter rank by 247Sports' own (scout) points. |
| `composite_conference_rank` | Int64 | Coach's conference recruiter rank by Composite points. |
| `previous_coach_ranking` | Float64 | Nested prior-cycle recruiter-ranking row for the coach (stringified). |
| `default_name` | Int64 | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_site_pages_coach_ranking-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_site_pages_coach_ranking-example}

```python
sports247_site_pages_coach_ranking()
```

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
| `key` | Int64 | Primary key of this entity (the id used in its `.json` route). |
| `coach` | Int64 |  |
| `institution` | Int64 | Nested 247Sports institution the coach recruited for during the ranking cycle (stringified). |
| `conference` | Int64 | Conference of the team. |
| `ranking` | Int64 | FK -> the Ranking snapshot this row belongs to. |
| `sport` | Int64 | Nested 247Sports sport the ranking covers (stringified). |
| `recruitment` | String | Nested 247Sports recruitment record credited to the coach on this row (stringified). |
| `rating` | Float64 | Overall rating score for the coach's recruiting haul in the 247Sports coach ranking. |
| `scout_rating` | Float64 | Total 247Sports in-house (scout) rating points credited to the coach's commits. |
| `composite_rating` | Float64 | Composite class rating for the coach's haul. |
| `commits` | Int64 | Number of commits credited to the coach in the ranking. |
| `total` | Int64 | Total ranking score for the coach's class, as reported by 247Sports. |
| `composite_total` | Int64 | Total 247Sports Composite rating points credited to the coach's commits. |
| `five_stars` | Int64 | Number of five-star commits credited to the coach. |
| `scout_five_stars` | Int64 | Number of five-star commits by 247Sports' own (scout) rating. |
| `composite_five_stars` | Int64 | Number of five-star commits by the 247Sports Composite rating. |
| `four_stars` | Int64 | Number of four-star commits credited to the coach. |
| `scout_four_stars` | Int64 | Number of four-star commits by 247Sports' own (scout) rating. |
| `composite_four_stars` | Int64 | Number of four-star commits by the 247Sports Composite rating. |
| `three_stars` | Int64 | Number of three-star commits credited to the coach. |
| `scout_three_stars` | Int64 | Number of three-star commits by 247Sports' own (scout) rating. |
| `composite_three_stars` | Int64 | Number of three-star commits by the 247Sports Composite rating. |
| `two_stars` | Int64 | Number of two-star commits credited to the coach. |
| `scout_two_stars` | Int64 | Number of two-star commits by 247Sports' own (scout) rating. |
| `composite_two_stars` | Int64 | Number of two-star commits by the 247Sports Composite rating. |
| `average_rating` | Float64 | Average rating across the coach's credited commits. |
| `average_scout_rating` | Int64 | Average 247Sports in-house (scout) rating across the credited commits. |
| `composite_average_rating` | Float64 | Average 247Sports Composite rating across the credited commits. |
| `overall_rank` | Int64 | Overall national coach-recruiting rank. |
| `composite_overall_rank` | Int64 | Coach's national recruiter rank by Composite points. |
| `scout_overall_rank` | Int64 | Coach's national recruiter rank by 247Sports' own (scout) points. |
| `division_rank` | Int64 | Coach's recruiter rank within the division. |
| `scout_division_rank` | Int64 | Coach's division recruiter rank by 247Sports' own (scout) points. |
| `composite_division_rank` | Int64 | Coach's division recruiter rank by Composite points. |
| `conference_rank` | Int64 | Rank within conference. |
| `scout_conference_rank` | Int64 | Coach's conference recruiter rank by 247Sports' own (scout) points. |
| `composite_conference_rank` | Int64 | Coach's conference recruiter rank by Composite points. |
| `previous_coach_ranking` | Float64 | Nested prior-cycle recruiter-ranking row for the coach (stringified). |
| `default_name` | Int64 | Server-rendered display label for the entity. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sports247_site_pages_coach_rankings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sports247_site_pages_coach_rankings-example}

```python
sports247_site_pages_coach_rankings(key=1531)
```
