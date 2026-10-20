---
title: "CFB — On3 Recruit Database (api.on3.com) — Other"
sidebar_label: "Other"
sidebar_position: 13
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

_Last validated n/a._

## on3_coaches_profile

GET /rdb/v1/coaches/{personKey}/profile

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

_Last validated n/a._

## on3_person_connections_connection_key

GET /rdb/v1/person-connections/{connectionKey}

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

_Last validated n/a._

## on3_person_primary_recruitment_evaluation

GET /rdb/v1/person/{personKey}/primary-recruitment-evaluation

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

_Last validated n/a._

## on3_person_recruitment_evaluations

GET /rdb/v1/person/{personKey}/recruitment-evaluations

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

_Last validated n/a._

## on3_person_sport_profile_recruit

GET /rdb/v1/person-sport/{psKey}/profile-recruit

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
| `category` | character | Category label On3 attaches to the row. |
| `person_key` | integer | On3 person key of the person quoted or quoted about. |
| `date_added` | character | Date the quote was added to the On3 database. |
| `date_updated` | character | Date the quote was last updated. |
| `person_key_2` | integer | Person key repeated from the nested person object (json_normalize de-duplication suffix). |
| `person_known_as_name` | character | Preferred name of the person, when it differs from the given name. |
| `person_first_name` | character | First name of the person. |
| `person_last_name` | character | Last name of the person. |
| `person_twitter_handle` | character | Twitter/X handle of the person, when listed. |
| `person_instagram_profile` | character | Instagram profile of the person, when listed. |
| `person_tik_tok_handle` | character | TikTok handle of the person, when listed. |
| `person_espn_profile` | character | ESPN profile link of the person, when listed. |
| `person_class_year` | integer | High-school graduating class year of the person. |
| `person_two_four_seven_profile` | character | 247Sports profile link of the person, when listed. |
| `person_rivals_profile` | character | Rivals profile link of the person, when listed. |

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

_Last validated n/a._

## on3_quotes_key

GET /rdb/v1/quotes/{key}

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

_Last validated n/a._

## on3_videos_video_key

GET /rdb/v1/videos/{videoKey}

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

_Last validated n/a._
