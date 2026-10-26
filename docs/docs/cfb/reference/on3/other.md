---
title: "College football — On3 Recruit Database (api.on3.com) — Other"
sidebar_label: "Other"
sidebar_position: 11
description: "College football — On3 Recruit Database (api.on3.com) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# College football — On3 Recruit Database (api.on3.com) — Other

## on3_coaches_history

Coaches history.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/coaches/{person_key}/history`

**Valid URL:** [https://api.on3.com/public/rdb/v1/coaches/89617/history](https://api.on3.com/public/rdb/v1/coaches/89617/history)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_coaches_history-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_coaches_history-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_coaches_history-example}

```python
on3_coaches_history(person_key=89617)
```

## on3_coaches_profile

Coaches profile.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/coaches/{person_key}/profile`

**Valid URL:** [https://api.on3.com/public/rdb/v1/coaches/89617/profile](https://api.on3.com/public/rdb/v1/coaches/89617/profile)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_coaches_profile-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_coaches_profile-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_coaches_profile-example}

```python
on3_coaches_profile(person_key=89617)
```

## on3_filters_conferences

Filters conferences.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/conferences`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/conferences](https://api.on3.com/public/rdb/v1/filters/conferences)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | year query parameter. |
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |

### Returns {#on3_filters_conferences-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_filters_conferences-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_filters_conferences-example}

```python
on3_filters_conferences()
```

## on3_filters_draft_rounds

Filters draft rounds.

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
| `round` | Int64 | Draft round number. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_filters_draft_rounds-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_filters_draft_rounds-example}

```python
on3_filters_draft_rounds()
```

## on3_filters_positions

Filters positions.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/positions`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/positions](https://api.on3.com/public/rdb/v1/filters/positions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `positionType` | `position_type` |  |  | `Y` | positionType query parameter. |

### Returns {#on3_filters_positions-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_filters_positions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_filters_positions-example}

```python
on3_filters_positions()
```

## on3_filters_sports

Filters sports.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/sports`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/sports](https://api.on3.com/public/rdb/v1/filters/sports)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#on3_filters_sports-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_filters_sports-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_filters_sports-example}

```python
on3_filters_sports()
```

## on3_filters_status

Filters status.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/status`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/status](https://api.on3.com/public/rdb/v1/filters/status)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#on3_filters_status-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `value` | String | Filter value as On3 lists it. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_filters_status-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_filters_status-example}

```python
on3_filters_status()
```

## on3_filters_teams

Filters teams.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/teams`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/teams](https://api.on3.com/public/rdb/v1/filters/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `groupBy` | `group_by` |  |  | `Y` | groupBy query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |

### Returns {#on3_filters_teams-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_filters_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_filters_teams-example}

```python
on3_filters_teams()
```

## on3_filters_years

Filters years.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/years`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/years](https://api.on3.com/public/rdb/v1/filters/years)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#on3_filters_years-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and each row is a bare scalar with no named fields; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_filters_years-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_filters_years-example}

```python
on3_filters_years()
```

## on3_person_connections_connection_key

Person connections connection key.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/person-connections/{connection_key}`

**Valid URL:** [https://api.on3.com/public/rdb/v1/person-connections/89617](https://api.on3.com/public/rdb/v1/person-connections/89617)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `connection_key` | `connection_key` |  | `Y` |  | connection_key path parameter. |

### Returns {#on3_person_connections_connection_key-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_person_connections_connection_key-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_person_connections_connection_key-example}

```python
on3_person_connections_connection_key(connection_key=89617)
```

## on3_person_primary_recruitment_evaluation

Person primary recruitment evaluation.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/person/{person_key}/primary-recruitment-evaluation`

**Valid URL:** [https://api.on3.com/public/rdb/v1/person/89617/primary-recruitment-evaluation](https://api.on3.com/public/rdb/v1/person/89617/primary-recruitment-evaluation)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_person_primary_recruitment_evaluation-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_person_primary_recruitment_evaluation-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_person_primary_recruitment_evaluation-example}

```python
on3_person_primary_recruitment_evaluation(person_key=89617)
```

## on3_person_recruitment_evaluations

Person recruitment evaluations.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/person/{person_key}/recruitment-evaluations`

**Valid URL:** [https://api.on3.com/public/rdb/v1/person/89617/recruitment-evaluations](https://api.on3.com/public/rdb/v1/person/89617/recruitment-evaluations)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_person_recruitment_evaluations-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_person_recruitment_evaluations-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_person_recruitment_evaluations-example}

```python
on3_person_recruitment_evaluations(person_key=89617)
```

## on3_person_sport_profile_recruit

Person sport profile recruit.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/person-sport/{ps_key}/profile-recruit`

**Valid URL:** [https://api.on3.com/public/rdb/v1/person-sport/89617/profile-recruit](https://api.on3.com/public/rdb/v1/person-sport/89617/profile-recruit)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ps_key` | `ps_key` |  | `Y` |  | ps_key path parameter. |

### Returns {#on3_person_sport_profile_recruit-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the row has nested objects whose flattened column names depend on which are null in the data; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_person_sport_profile_recruit-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_person_sport_profile_recruit-example}

```python
on3_person_sport_profile_recruit(ps_key=89617)
```

## on3_person_sport_rankings

Person sport rankings.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/person-sport-rankings`

**Valid URL:** [https://api.on3.com/public/rdb/v1/person-sport-rankings](https://api.on3.com/public/rdb/v1/person-sport-rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_person_sport_rankings-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_person_sport_rankings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_person_sport_rankings-example}

```python
on3_person_sport_rankings()
```

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

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_predictions_user_key-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_predictions_user_key-example}

```python
on3_predictions_user_key(user_key=89617)
```

## on3_quotes

Quotes.

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
| `key` | Int64 | On3 RDB key for the quote record. |
| `body` | String | Full text of the quote. |
| `category` | String | Category label On3 attaches to the row. |
| `person_key` | Int64 | On3 person key of the person quoted or quoted about. |
| `date_added` | String | Date the quote was added to the On3 database. |
| `date_updated` | String | Date the quote was last updated. |
| `person_key_2` | Int64 | Person key repeated from the nested person object (json_normalize de-duplication suffix). |
| `person_known_as_name` | String | Preferred name of the person, when it differs from the given name. |
| `person_first_name` | String | First name of the person. |
| `person_last_name` | String | Last name of the person. |
| `person_twitter_handle` | String | Twitter/X handle of the person, when listed. |
| `person_instagram_profile` | String | Instagram profile of the person, when listed. |
| `person_tik_tok_handle` | String | TikTok handle of the person, when listed. |
| `person_espn_profile` | String | ESPN profile link of the person, when listed. |
| `person_class_year` | Int64 | High-school graduating class year of the person. |
| `person_two_four_seven_profile` | String | 247Sports profile link of the person, when listed. |
| `person_rivals_profile` | String | Rivals profile link of the person, when listed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_quotes-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_quotes-example}

```python
on3_quotes()
```

## on3_quotes_key

Quotes key.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/quotes/{key}`

**Valid URL:** [https://api.on3.com/public/rdb/v1/quotes/1](https://api.on3.com/public/rdb/v1/quotes/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `key` | `key` |  | `Y` |  | key path parameter. |

### Returns {#on3_quotes_key-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_quotes_key-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_quotes_key-example}

```python
on3_quotes_key(key=1)
```

## on3_videos_video_key

Videos video key.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/videos/{video_key}`

**Valid URL:** [https://api.on3.com/public/rdb/v1/videos/1](https://api.on3.com/public/rdb/v1/videos/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `video_key` | `video_key` |  | `Y` |  | video_key path parameter. |

### Returns {#on3_videos_video_key-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the row has nested objects whose flattened column names depend on which are null in the data; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_videos_video_key-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_videos_video_key-example}

```python
on3_videos_video_key(video_key=1)
```
