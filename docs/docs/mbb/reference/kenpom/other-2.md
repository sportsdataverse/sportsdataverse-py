---
title: "MBB — KenPom (kenpom.com, subscription) — Other: referee–game_attributes"
sidebar_label: "Other: referee–game_attributes"
sidebar_position: 3
description: "MBB — KenPom (kenpom.com, subscription) — Other: referee–game_attributes — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# MBB — KenPom (kenpom.com, subscription) — Other: referee–game_attributes

## kenpom_referee

GET /referee.php - one referee's game log and splits for a season. Port of hoopR kp_referee().

**Endpoint URL:** `GET https://kenpom.com/referee.php`

**Valid URL:** [https://kenpom.com/referee.php?r=714294&y=2025](https://kenpom.com/referee.php?r=714294&y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `r` | `referee` |  | `Y` |  | KenPom referee id - the `r=` value on an officials-table link. |
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |

### Returns {#kenpom_referee-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**ratings_table**

| col_name | type | description |
|---|---|---|
| `column_0` | integer |  |
| `date` | character | Date in YYYY-MM-DD format. |
| `time_et` | character | Time et. |
| `game` | character | Game. |
| `location` | character | Location. |
| `venue` | character | Venue name. |
| `venue_rk` | character |  |
| `thrill_score` | double | Thrill score. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_referee-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_referee-example}

```python
kenpom_referee(referee=714294, year=2025)
```

_Last validated n/a._

## kenpom_game_attributes

GET /game_attrs.php - season game leaderboards by attribute (thrill score, comebacks, upsets, ...). Port of hoopR kp_game_attrs().

**Endpoint URL:** `GET https://kenpom.com/game_attrs.php`

**Valid URL:** [https://kenpom.com/game_attrs.php?y=2025&s=ThrillScore](https://kenpom.com/game_attrs.php?y=2025&s=ThrillScore)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |
| `s` | `attribute` |  |  | `Y` | Attribute slug, e.g. ThrillScore, Comeback, FanMatch, Upsets, Busts, MinutesPlayed, PossessionLength, LeadChanges. |

### Returns {#kenpom_game_attributes-returns}

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**ratings_table**

| col_name | type | description |
|---|---|---|
| `column_0` | integer |  |
| `date` | character | Date in YYYY-MM-DD format. |
| `game` | character | Game. |
| `game_rk` | character |  |
| `location` | character | Location. |
| `location_rk` | character |  |
| `value` | double | Numeric or string value field. |

**`return_parsed=False`** — the raw page HTML (`str`).

### Raises {#kenpom_game_attributes-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No KenPom credentials could be resolved (pass `email=`/`password=`, or set `KENPOM_EMAIL`/`KENPOM_PW`), or kenpom.com rejected them. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#kenpom_game_attributes-example}

```python
kenpom_game_attributes(year=2025, attribute='ThrillScore')
```

_Last validated n/a._
