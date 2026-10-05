---
title: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Other (2)"
sidebar_label: "Other (2)"
sidebar_position: 11
description: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Other (2) — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Other (2)

## yahoo_tennis_tournaments_by_date

Yahoo shangrila persisted query `tennisTournamentsByDate` -> one row per `tennisTournaments` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisTournamentsByDate`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisTournamentsByDate](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/tennisTournamentsByDate)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `date` | `date` |  |  | `Y` | date query parameter. |

### Returns {#yahoo_tennis_tournaments_by_date-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_tennis_tournaments_by_date-example}

```python
yahoo_tennis_tournaments_by_date()
```

_Last validated n/a._

## yahoo_trending_event_ids

Yahoo shangrila persisted query `trendingEventIds` -> one row per `trendingEvents` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/trendingEventIds`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/trendingEventIds](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/trendingEventIds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `count` | `count` |  |  | `Y` | count query parameter. |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `dateFlipOffset` | `date_flip_offset` |  |  | `Y` | dateFlipOffset query parameter. |

### Returns {#yahoo_trending_event_ids-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_trending_event_ids-example}

```python
yahoo_trending_event_ids()
```

_Last validated n/a._

## yahoo_trending_game_ids

Yahoo shangrila persisted query `trendingGameIds` -> one row per `trendingGames` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/trendingGameIds`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/trendingGameIds](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/trendingGameIds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `count` | `count` |  |  | `Y` | count query parameter. |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `dateFlipOffset` | `date_flip_offset` |  |  | `Y` | dateFlipOffset query parameter. |
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |

### Returns {#yahoo_trending_game_ids-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_trending_game_ids-example}

```python
yahoo_trending_game_ids()
```

_Last validated n/a._
