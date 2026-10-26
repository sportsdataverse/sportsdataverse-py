---
title: Soccer (all) — OpenLigaDB (api.openligadb.de, community German football)
sidebar_label: OpenLigaDB (api.openligadb.de, community German football)
description: "Soccer (all) — OpenLigaDB (api.openligadb.de, community German football) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 15
toc_max_heading_level: 2
---
# Soccer (all) — OpenLigaDB (api.openligadb.de, community German football)

`sportsdataverse.soccer` — 11 endpoints.

## openligadb_current_group

Current matchday.

**Endpoint URL:** `GET https://api.openligadb.de/getcurrentgroup/{league_slug}`

**Valid URL:** [https://api.openligadb.de/getcurrentgroup/bl1](https://api.openligadb.de/getcurrentgroup/bl1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | League shortcut from /getavailableleagues, e.g. bl1 (Bundesliga). |

### Returns {#openligadb_current_group-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group_name` | String |  |
| `group_order_id` | String |  |
| `group_id` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#openligadb_current_group-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. OpenLigaDB answered 404 (unknown league, season, matchday or match id -- /getnextmatchbyleagueteam 404s once the season is over). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#openligadb_current_group-example}

```python
openligadb_current_group(league_slug='bl1')
```

## openligadb_goalgetters

Top scorers.

**Endpoint URL:** `GET https://api.openligadb.de/getgoalgetters/{league_slug}/{season}`

**Valid URL:** [https://api.openligadb.de/getgoalgetters/bl1/2025](https://api.openligadb.de/getgoalgetters/bl1/2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | League shortcut from /getavailableleagues, e.g. bl1 (Bundesliga). |
| `season` | `season` |  | `Y` |  | Season start year, e.g. 2025 for 2025/26. |

### Returns {#openligadb_goalgetters-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `goal_getter_id` | String |  |
| `goal_getter_name` | String |  |
| `goal_count` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#openligadb_goalgetters-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. OpenLigaDB answered 404 (unknown league, season, matchday or match id -- /getnextmatchbyleagueteam 404s once the season is over). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#openligadb_goalgetters-example}

```python
openligadb_goalgetters(league_slug='bl1', season='2025')
```

## openligadb_group_matches

Matches of one matchday.

**Endpoint URL:** `GET https://api.openligadb.de/getmatchdata/{league_slug}/{season}/{group}`

**Valid URL:** [https://api.openligadb.de/getmatchdata/bl1/2025/1](https://api.openligadb.de/getmatchdata/bl1/2025/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | League shortcut from /getavailableleagues, e.g. bl1 (Bundesliga). |
| `season` | `season` |  | `Y` |  | Season start year, e.g. 2025 for 2025/26. |
| `group` | `group` |  | `Y` |  | Matchday number (groupOrderID from /getavailablegroups), e.g. 1. |

### Returns {#openligadb_group_matches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | String |  |
| `match_date_time` | String |  |
| `time_zone_id` | String |  |
| `league_id` | String |  |
| `league_name` | String |  |
| `league_season` | Int64 |  |
| `league_shortcut` | String |  |
| `match_date_time_utc` | String |  |
| `last_update_date_time` | String |  |
| `match_is_finished` | Boolean |  |
| `match_results` | String |  |
| `goals` | String |  |
| `location` | String |  |
| `number_of_viewers` | String |  |
| `group_group_name` | String |  |
| `group_group_order_id` | String |  |
| `group_group_id` | String |  |
| `team1_team_id` | String |  |
| `team1_team_name` | String |  |
| `team1_short_name` | String |  |
| `team1_team_icon_url` | String |  |
| `team1_team_group_name` | String |  |
| `team2_team_id` | String |  |
| `team2_team_name` | String |  |
| `team2_short_name` | String |  |
| `team2_team_icon_url` | String |  |
| `team2_team_group_name` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#openligadb_group_matches-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. OpenLigaDB answered 404 (unknown league, season, matchday or match id -- /getnextmatchbyleagueteam 404s once the season is over). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#openligadb_group_matches-example}

```python
openligadb_group_matches(group='1', league_slug='bl1', season='2025')
```

## openligadb_groups

Matchdays of a league-season.

**Endpoint URL:** `GET https://api.openligadb.de/getavailablegroups/{league_slug}/{season}`

**Valid URL:** [https://api.openligadb.de/getavailablegroups/bl1/2025](https://api.openligadb.de/getavailablegroups/bl1/2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | League shortcut from /getavailableleagues, e.g. bl1 (Bundesliga). |
| `season` | `season` |  | `Y` |  | Season start year, e.g. 2025 for 2025/26. |

### Returns {#openligadb_groups-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group_name` | String |  |
| `group_order_id` | String |  |
| `group_id` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#openligadb_groups-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. OpenLigaDB answered 404 (unknown league, season, matchday or match id -- /getnextmatchbyleagueteam 404s once the season is over). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#openligadb_groups-example}

```python
openligadb_groups(league_slug='bl1', season='2025')
```

## openligadb_last_change_date

Last change timestamp (bare ISO string).

**Endpoint URL:** `GET https://api.openligadb.de/getlastchangedate/{league_slug}/{season}/{group}`

**Valid URL:** [https://api.openligadb.de/getlastchangedate/bl1/2025/1](https://api.openligadb.de/getlastchangedate/bl1/2025/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | League shortcut from /getavailableleagues, e.g. bl1 (Bundesliga). |
| `season` | `season` |  | `Y` |  | Season start year, e.g. 2025 for 2025/26. |
| `group` | `group` |  | `Y` |  | Matchday number (groupOrderID from /getavailablegroups), e.g. 1. |

### Returns {#openligadb_last_change_date-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `value` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#openligadb_last_change_date-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. OpenLigaDB answered 404 (unknown league, season, matchday or match id -- /getnextmatchbyleagueteam 404s once the season is over). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#openligadb_last_change_date-example}

```python
openligadb_last_change_date(group='1', league_slug='bl1', season='2025')
```

## openligadb_leagues

All leagues (shortcut, season, sport).

**Endpoint URL:** `GET https://api.openligadb.de/getavailableleagues`

**Valid URL:** [https://api.openligadb.de/getavailableleagues](https://api.openligadb.de/getavailableleagues)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#openligadb_leagues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | String |  |
| `league_name` | String |  |
| `league_shortcut` | String |  |
| `league_season` | String |  |
| `sport_sport_id` | String | Yahoo identifier of the sport the league belongs to. |
| `sport_sport_name` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#openligadb_leagues-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. OpenLigaDB answered 404 (unknown league, season, matchday or match id -- /getnextmatchbyleagueteam 404s once the season is over). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#openligadb_leagues-example}

```python
openligadb_leagues()
```

## openligadb_match

One match.

**Endpoint URL:** `GET https://api.openligadb.de/getmatchdata/{match_id}`

**Valid URL:** [https://api.openligadb.de/getmatchdata/77264](https://api.openligadb.de/getmatchdata/77264)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (matchID from any getmatchdata list), e.g. 77264. |

### Returns {#openligadb_match-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | String |  |
| `match_date_time` | String |  |
| `time_zone_id` | String |  |
| `league_id` | String |  |
| `league_name` | String |  |
| `league_season` | Int64 |  |
| `league_shortcut` | String |  |
| `match_date_time_utc` | String |  |
| `last_update_date_time` | String |  |
| `match_is_finished` | Boolean |  |
| `match_results` | String |  |
| `goals` | String |  |
| `location` | String |  |
| `number_of_viewers` | String |  |
| `group_group_name` | String |  |
| `group_group_order_id` | String |  |
| `group_group_id` | String |  |
| `team1_team_id` | String |  |
| `team1_team_name` | String |  |
| `team1_short_name` | String |  |
| `team1_team_icon_url` | String |  |
| `team1_team_group_name` | String |  |
| `team2_team_id` | String |  |
| `team2_team_name` | String |  |
| `team2_short_name` | String |  |
| `team2_team_icon_url` | String |  |
| `team2_team_group_name` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#openligadb_match-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. OpenLigaDB answered 404 (unknown league, season, matchday or match id -- /getnextmatchbyleagueteam 404s once the season is over). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#openligadb_match-example}

```python
openligadb_match(match_id='77264')
```

## openligadb_next_match

Next match of a team (404 text once the season is over).

**Endpoint URL:** `GET https://api.openligadb.de/getnextmatchbyleagueteam/{league_id}/{team_id}`

**Valid URL:** [https://api.openligadb.de/getnextmatchbyleagueteam/4937/40](https://api.openligadb.de/getnextmatchbyleagueteam/4937/40)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | Numeric leagueId from /getavailableleagues of a season still in progress, e.g. 4937 (bl1 2026); a finished season answers 404 text. |
| `team_id` | `team_id` |  | `Y` |  | Team id (teamId from /getavailableteams), e.g. 40 (FC Bayern München). |

### Returns {#openligadb_next_match-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | String |  |
| `match_date_time` | String |  |
| `time_zone_id` | String |  |
| `league_id` | String |  |
| `league_name` | String |  |
| `league_season` | Int64 |  |
| `league_shortcut` | String |  |
| `match_date_time_utc` | String |  |
| `last_update_date_time` | String |  |
| `match_is_finished` | Boolean |  |
| `match_results` | String |  |
| `goals` | String |  |
| `location` | String |  |
| `number_of_viewers` | String |  |
| `group_group_name` | String |  |
| `group_group_order_id` | String |  |
| `group_group_id` | String |  |
| `team1_team_id` | String |  |
| `team1_team_name` | String |  |
| `team1_short_name` | String |  |
| `team1_team_icon_url` | String |  |
| `team1_team_group_name` | String |  |
| `team2_team_id` | String |  |
| `team2_team_name` | String |  |
| `team2_short_name` | String |  |
| `team2_team_icon_url` | String |  |
| `team2_team_group_name` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#openligadb_next_match-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. OpenLigaDB answered 404 (unknown league, season, matchday or match id -- /getnextmatchbyleagueteam 404s once the season is over). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#openligadb_next_match-example}

```python
openligadb_next_match(league_id='4937', team_id='40')
```

## openligadb_season_matches

All matches of a season.

**Endpoint URL:** `GET https://api.openligadb.de/getmatchdata/{league_slug}/{season}`

**Valid URL:** [https://api.openligadb.de/getmatchdata/bl1/2025](https://api.openligadb.de/getmatchdata/bl1/2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | League shortcut from /getavailableleagues, e.g. bl1 (Bundesliga). |
| `season` | `season` |  | `Y` |  | Season start year, e.g. 2025 for 2025/26. |

### Returns {#openligadb_season_matches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | String |  |
| `match_date_time` | String |  |
| `time_zone_id` | String |  |
| `league_id` | String |  |
| `league_name` | String |  |
| `league_season` | Int64 |  |
| `league_shortcut` | String |  |
| `match_date_time_utc` | String |  |
| `last_update_date_time` | String |  |
| `match_is_finished` | Boolean |  |
| `match_results` | String |  |
| `goals` | String |  |
| `location` | String |  |
| `number_of_viewers` | String |  |
| `group_group_name` | String |  |
| `group_group_order_id` | String |  |
| `group_group_id` | String |  |
| `team1_team_id` | String |  |
| `team1_team_name` | String |  |
| `team1_short_name` | String |  |
| `team1_team_icon_url` | String |  |
| `team1_team_group_name` | String |  |
| `team2_team_id` | String |  |
| `team2_team_name` | String |  |
| `team2_short_name` | String |  |
| `team2_team_icon_url` | String |  |
| `team2_team_group_name` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#openligadb_season_matches-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. OpenLigaDB answered 404 (unknown league, season, matchday or match id -- /getnextmatchbyleagueteam 404s once the season is over). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#openligadb_season_matches-example}

```python
openligadb_season_matches(league_slug='bl1', season='2025')
```

## openligadb_table

League table.

**Endpoint URL:** `GET https://api.openligadb.de/getbltable/{league_slug}/{season}`

**Valid URL:** [https://api.openligadb.de/getbltable/bl1/2025](https://api.openligadb.de/getbltable/bl1/2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | League shortcut from /getavailableleagues, e.g. bl1 (Bundesliga). |
| `season` | `season` |  | `Y` |  | Season start year, e.g. 2025 for 2025/26. |

### Returns {#openligadb_table-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_info_id` | String |  |
| `team_name` | String |  |
| `short_name` | String |  |
| `team_icon_url` | String |  |
| `points` | Int64 |  |
| `opponent_goals` | Int64 |  |
| `goals` | Int64 |  |
| `matches` | Int64 |  |
| `won` | Int64 |  |
| `lost` | Int64 |  |
| `draw` | Int64 |  |
| `goal_diff` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#openligadb_table-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. OpenLigaDB answered 404 (unknown league, season, matchday or match id -- /getnextmatchbyleagueteam 404s once the season is over). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#openligadb_table-example}

```python
openligadb_table(league_slug='bl1', season='2025')
```

## openligadb_teams

Teams of a league-season.

**Endpoint URL:** `GET https://api.openligadb.de/getavailableteams/{league_slug}/{season}`

**Valid URL:** [https://api.openligadb.de/getavailableteams/bl1/2025](https://api.openligadb.de/getavailableteams/bl1/2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | League shortcut from /getavailableleagues, e.g. bl1 (Bundesliga). |
| `season` | `season` |  | `Y` |  | Season start year, e.g. 2025 for 2025/26. |

### Returns {#openligadb_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | String | ESPN numeric identifier for the team. |
| `team_name` | String |  |
| `short_name` | String |  |
| `team_icon_url` | String |  |
| `team_group_name` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#openligadb_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. OpenLigaDB answered 404 (unknown league, season, matchday or match id -- /getnextmatchbyleagueteam 404s once the season is over). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#openligadb_teams-example}

```python
openligadb_teams(league_slug='bl1', season='2025')
```
