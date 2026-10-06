---
title: SOCCER — OpenLigaDB (api.openligadb.de, community German football)
sidebar_label: OpenLigaDB (api.openligadb.de, community German football)
description: "SOCCER — OpenLigaDB (api.openligadb.de, community German football) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 15
toc_max_heading_level: 2
---
# SOCCER — OpenLigaDB (api.openligadb.de, community German football)

`sportsdataverse.soccer` — 11 endpoints.

## openligadb_current_group

Current matchday.

**Endpoint URL:** `GET https://api.openligadb.de/getcurrentgroup/{league_slug}`

**Valid URL:** [https://api.openligadb.de/getcurrentgroup/bl1](https://api.openligadb.de/getcurrentgroup/bl1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |

### Returns {#openligadb_current_group-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group_name` | character | Group name (conference / division). |
| `group_order_id` | character |  |
| `group_id` | character | ESPN group id. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#openligadb_current_group-example}

```python
openligadb_current_group(league_slug='bl1')
```

_Last validated n/a._

## openligadb_goalgetters

Top scorers.

**Endpoint URL:** `GET https://api.openligadb.de/getgoalgetters/{league_slug}/{season}`

**Valid URL:** [https://api.openligadb.de/getgoalgetters/bl1/2025](https://api.openligadb.de/getgoalgetters/bl1/2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#openligadb_goalgetters-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `goal_getter_id` | character |  |
| `goal_getter_name` | character |  |
| `goal_count` | integer | Total goals recorded. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#openligadb_goalgetters-example}

```python
openligadb_goalgetters(league_slug='bl1', season='2025')
```

_Last validated n/a._

## openligadb_group_matches

Matches of one matchday.

**Endpoint URL:** `GET https://api.openligadb.de/getmatchdata/{league_slug}/{season}/{group}`

**Valid URL:** [https://api.openligadb.de/getmatchdata/bl1/2025/1](https://api.openligadb.de/getmatchdata/bl1/2025/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |
| `season` | `season` |  | `Y` |  | season path parameter. |
| `group` | `group` |  | `Y` |  | group path parameter. |

### Returns {#openligadb_group_matches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | character |  |
| `match_date_time` | character |  |
| `time_zone_id` | character |  |
| `league_id` | character | League identifier ('10' = WNBA). |
| `league_name` | character | League name. |
| `league_season` | integer | Season year for the league record. |
| `league_shortcut` | character |  |
| `match_date_time_utc` | character |  |
| `last_update_date_time` | character |  |
| `match_is_finished` | logical |  |
| `match_results` | character |  |
| `goals` | character | Goals scored. |
| `location` | character | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `number_of_viewers` | character |  |
| `group_group_name` | character |  |
| `group_group_order_id` | character |  |
| `group_group_id` | character |  |
| `team1_team_id` | character |  |
| `team1_team_name` | character |  |
| `team1_short_name` | character |  |
| `team1_team_icon_url` | character |  |
| `team1_team_group_name` | character |  |
| `team2_team_id` | character |  |
| `team2_team_name` | character |  |
| `team2_short_name` | character |  |
| `team2_team_icon_url` | character |  |
| `team2_team_group_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#openligadb_group_matches-example}

```python
openligadb_group_matches(group='1', league_slug='bl1', season='2025')
```

_Last validated n/a._

## openligadb_groups

Matchdays of a league-season.

**Endpoint URL:** `GET https://api.openligadb.de/getavailablegroups/{league_slug}/{season}`

**Valid URL:** [https://api.openligadb.de/getavailablegroups/bl1/2025](https://api.openligadb.de/getavailablegroups/bl1/2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#openligadb_groups-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group_name` | character | Group name (conference / division). |
| `group_order_id` | character |  |
| `group_id` | character | ESPN group id. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#openligadb_groups-example}

```python
openligadb_groups(league_slug='bl1', season='2025')
```

_Last validated n/a._

## openligadb_last_change_date

Last change timestamp (bare ISO string).

**Endpoint URL:** `GET https://api.openligadb.de/getlastchangedate/{league_slug}/{season}/{group}`

**Valid URL:** [https://api.openligadb.de/getlastchangedate/bl1/2025/1](https://api.openligadb.de/getlastchangedate/bl1/2025/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |
| `season` | `season` |  | `Y` |  | season path parameter. |
| `group` | `group` |  | `Y` |  | group path parameter. |

### Returns {#openligadb_last_change_date-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `value` | character | Numeric or string value field. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#openligadb_last_change_date-example}

```python
openligadb_last_change_date(group='1', league_slug='bl1', season='2025')
```

_Last validated n/a._

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
| `league_id` | character | League identifier ('10' = WNBA). |
| `league_name` | character | League name. |
| `league_shortcut` | character |  |
| `league_season` | character | Season year for the league record. |
| `sport_sport_id` | character | Yahoo identifier of the sport the league belongs to. |
| `sport_sport_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#openligadb_leagues-example}

```python
openligadb_leagues()
```

_Last validated n/a._

## openligadb_match

One match.

**Endpoint URL:** `GET https://api.openligadb.de/getmatchdata/{match_id}`

**Valid URL:** [https://api.openligadb.de/getmatchdata/77264](https://api.openligadb.de/getmatchdata/77264)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | match_id path parameter. |

### Returns {#openligadb_match-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | character |  |
| `match_date_time` | character |  |
| `time_zone_id` | character |  |
| `league_id` | character | League identifier ('10' = WNBA). |
| `league_name` | character | League name. |
| `league_season` | integer | Season year for the league record. |
| `league_shortcut` | character |  |
| `match_date_time_utc` | character |  |
| `last_update_date_time` | character |  |
| `match_is_finished` | logical |  |
| `match_results` | character |  |
| `goals` | character | Goals scored. |
| `location` | character | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `number_of_viewers` | character |  |
| `group_group_name` | character |  |
| `group_group_order_id` | character |  |
| `group_group_id` | character |  |
| `team1_team_id` | character |  |
| `team1_team_name` | character |  |
| `team1_short_name` | character |  |
| `team1_team_icon_url` | character |  |
| `team1_team_group_name` | character |  |
| `team2_team_id` | character |  |
| `team2_team_name` | character |  |
| `team2_short_name` | character |  |
| `team2_team_icon_url` | character |  |
| `team2_team_group_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#openligadb_match-example}

```python
openligadb_match(match_id='77264')
```

_Last validated n/a._

## openligadb_next_match

Next match of a team (404 text once the season is over).

**Endpoint URL:** `GET https://api.openligadb.de/getnextmatchbyleagueteam/{league_id}/{team_id}`

**Valid URL:** [https://api.openligadb.de/getnextmatchbyleagueteam/4937/40](https://api.openligadb.de/getnextmatchbyleagueteam/4937/40)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | league_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#openligadb_next_match-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | character |  |
| `match_date_time` | character |  |
| `time_zone_id` | character |  |
| `league_id` | character | League identifier ('10' = WNBA). |
| `league_name` | character | League name. |
| `league_season` | integer | Season year for the league record. |
| `league_shortcut` | character |  |
| `match_date_time_utc` | character |  |
| `last_update_date_time` | character |  |
| `match_is_finished` | logical |  |
| `match_results` | character |  |
| `goals` | character | Goals scored. |
| `location` | character | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `number_of_viewers` | character |  |
| `group_group_name` | character |  |
| `group_group_order_id` | character |  |
| `group_group_id` | character |  |
| `team1_team_id` | character |  |
| `team1_team_name` | character |  |
| `team1_short_name` | character |  |
| `team1_team_icon_url` | character |  |
| `team1_team_group_name` | character |  |
| `team2_team_id` | character |  |
| `team2_team_name` | character |  |
| `team2_short_name` | character |  |
| `team2_team_icon_url` | character |  |
| `team2_team_group_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#openligadb_next_match-example}

```python
openligadb_next_match(league_id='4937', team_id='40')
```

_Last validated n/a._

## openligadb_season_matches

All matches of a season.

**Endpoint URL:** `GET https://api.openligadb.de/getmatchdata/{league_slug}/{season}`

**Valid URL:** [https://api.openligadb.de/getmatchdata/bl1/2025](https://api.openligadb.de/getmatchdata/bl1/2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#openligadb_season_matches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | character |  |
| `match_date_time` | character |  |
| `time_zone_id` | character |  |
| `league_id` | character | League identifier ('10' = WNBA). |
| `league_name` | character | League name. |
| `league_season` | integer | Season year for the league record. |
| `league_shortcut` | character |  |
| `match_date_time_utc` | character |  |
| `last_update_date_time` | character |  |
| `match_is_finished` | logical |  |
| `match_results` | character |  |
| `goals` | character | Goals scored. |
| `location` | character | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `number_of_viewers` | character |  |
| `group_group_name` | character |  |
| `group_group_order_id` | character |  |
| `group_group_id` | character |  |
| `team1_team_id` | character |  |
| `team1_team_name` | character |  |
| `team1_short_name` | character |  |
| `team1_team_icon_url` | character |  |
| `team1_team_group_name` | character |  |
| `team2_team_id` | character |  |
| `team2_team_name` | character |  |
| `team2_short_name` | character |  |
| `team2_team_icon_url` | character |  |
| `team2_team_group_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#openligadb_season_matches-example}

```python
openligadb_season_matches(league_slug='bl1', season='2025')
```

_Last validated n/a._

## openligadb_table

League table.

**Endpoint URL:** `GET https://api.openligadb.de/getbltable/{league_slug}/{season}`

**Valid URL:** [https://api.openligadb.de/getbltable/bl1/2025](https://api.openligadb.de/getbltable/bl1/2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#openligadb_table-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_info_id` | character |  |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `short_name` | character | Short display name. |
| `team_icon_url` | character |  |
| `points` | integer | Points scored. |
| `opponent_goals` | integer |  |
| `goals` | integer | Goals scored. |
| `matches` | integer |  |
| `won` | integer |  |
| `lost` | integer |  |
| `draw` | integer |  |
| `goal_diff` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#openligadb_table-example}

```python
openligadb_table(league_slug='bl1', season='2025')
```

_Last validated n/a._

## openligadb_teams

Teams of a league-season.

**Endpoint URL:** `GET https://api.openligadb.de/getavailableteams/{league_slug}/{season}`

**Valid URL:** [https://api.openligadb.de/getavailableteams/bl1/2025](https://api.openligadb.de/getavailableteams/bl1/2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_slug` | `league_slug` |  | `Y` |  | league_slug path parameter. |
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#openligadb_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | character | ESPN numeric identifier for the team. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `short_name` | character | Short display name. |
| `team_icon_url` | character |  |
| `team_group_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#openligadb_teams-example}

```python
openligadb_teams(league_slug='bl1', season='2025')
```

_Last validated n/a._
