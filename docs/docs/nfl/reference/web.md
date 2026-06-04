---
title: NFL — ESPN web API (v3)
sidebar_label: ESPN web API (v3)
---
# NFL — ESPN web API (v3)

`sportsdataverse.nfl` — 5 endpoints.

## `espn_nfl_player_overview`

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/{athlete_id}/overview`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/4239/overview](https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/4239/overview)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_player_overview(athlete_id='4239')
```

_Last validated n/a._

## `espn_nfl_player_stats_v3`

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/{athlete_id}/stats`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/4239/stats](https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/4239/stats)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_player_stats_v3(athlete_id='4239')
```

_Last validated n/a._

## `espn_nfl_player_gamelog`

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/{athlete_id}/gamelog`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/4239/gamelog](https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/4239/gamelog)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_player_gamelog(athlete_id='4239')
```

_Last validated n/a._

## `espn_nfl_player_splits`

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/{athlete_id}/splits`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/4239/splits](https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/4239/splits)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_player_splits(athlete_id='4239')
```

_Last validated n/a._

## `espn_nfl_leaders`

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/statistics/byathlete`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/statistics/byathlete](https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/statistics/byathlete)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `category` | `category` |  |  | `Y` |
| `season` | `season` |  |  | `Y` |
| `seasontype` | `season_type` |  |  | `Y` |
| `limit` | `limit` |  |  | `Y` |
| `page` | `page` |  |  | `Y` |
| `sort` | `sort` |  |  | `Y` |

### Returns

| col_name | type | description |
|---|---|---|
| `athletes` | character | Athletes. |
| `glossary` | character | Glossary. |
| `categories` | character | Categories. |
| `pagination_count` | integer | Pagination count. |
| `pagination_limit` | integer | Pagination limit. |
| `pagination_page` | integer | Pagination page. |
| `pagination_pages` | integer | Pagination pages. |
| `pagination_first` | character | Pagination first. |
| `pagination_next` | character | Pagination next. |
| `pagination_last` | character | Pagination last. |
| `league_id` | character | League id. |
| `league_uid` | character | League uid. |
| `league_name` | character | League name. |
| `league_abbreviation` | character | League abbreviation. |
| `league_slug` | character | League slug. |
| `league_short_name` | character | League short name. |
| `current_season_year` | integer | Current season year. |
| `current_season_display_name` | character | Current season display name. |
| `current_season_start_date` | character | Current season start date. |
| `current_season_end_date` | character | Current season end date. |
| `current_season_type_id` | character | Current season type id. |
| `current_season_type_type` | integer | Current season type type. |
| `current_season_type_name` | character | Current season type name. |
| `current_season_type_start_date` | character | Current season type start date. |
| `current_season_type_end_date` | character | Current season type end date. |
| `requested_season_year` | integer | Requested season year. |
| `requested_season_display_name` | character | Requested season display name. |
| `requested_season_start_date` | character | Requested season start date. |
| `requested_season_end_date` | character | Requested season end date. |
| `requested_season_type_id` | character | Requested season type id. |
| `requested_season_type_type` | integer | Requested season type type. |
| `requested_season_type_name` | character | Requested season type name. |
| `requested_season_type_start_date` | character | Requested season type start date. |
| `requested_season_type_end_date` | character | Requested season type end date. |

### Example

```python
espn_nfl_leaders()
```

_Last validated n/a._
