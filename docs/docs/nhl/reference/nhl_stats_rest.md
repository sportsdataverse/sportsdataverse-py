---
title: NHL — NHL Stats REST API
sidebar_label: NHL Stats REST API
sidebar_position: 12
---
# NHL — NHL Stats REST API

`sportsdataverse.nhl` — 21 endpoints.

## `nhl_stats_rest_ping`

Ping the NHL Stats REST API database.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/ping`

**Valid URL:** [https://api.nhle.com/stats/rest/ping](https://api.nhle.com/stats/rest/ping)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Ping the NHL Stats REST API database.

### Example

```python
nhl_stats_rest_ping()
```

_Last validated n/a._

## `nhl_stats_rest_component_season`

Retrieve the component-season configuration.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/componentSeason`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns

Retrieve the component-season configuration.

### Example

```python
nhl_stats_rest_component_season()
```

_Last validated n/a._

## `nhl_stats_rest_config`

Retrieve the Stats REST API configuration payload.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/config`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns

Retrieve the Stats REST API configuration payload.

### Example

```python
nhl_stats_rest_config()
```

_Last validated n/a._

## `nhl_stats_rest_content_module`

Retrieve a content module by template key.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/content/module/{template_key}`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `template_key` | `template_key` |  | `Y` |  | template_key path parameter. |
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns

Retrieve a content module by template key.

### Example

```python
nhl_stats_rest_content_module(template_key='X')
```

_Last validated n/a._

## `nhl_stats_rest_country`

Retrieve the list of countries used in NHL data.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/country`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | character | Unique player identifier. |
| `country3_code` | character |  |
| `country_code` | character | Player country code. |
| `country_name` | character |  |
| `has_player_stats` | integer |  |
| `image_url` | character | Player headshot URL. |
| `ioc_code` | character |  |
| `is_active` | integer | Whether the team is active. |
| `nationality_name` | character |  |
| `olympic_url` | character |  |
| `thumbnail_url` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_stats_rest_country()
```

_Last validated n/a._

## `nhl_stats_rest_draft`

Retrieve draft data, optionally filtered with Cayenne expressions.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/draft`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_stats_rest`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_stats_rest_draft()
```

_Last validated n/a._

## `nhl_stats_rest_franchise`

Retrieve franchise data.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/franchise`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `full_name` | character | Player full name. |
| `team_common_name` | character | Team common (nickname) name. |
| `team_place_name` | character | Team place (city/location) name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_stats_rest_franchise()
```

_Last validated n/a._

## `nhl_stats_rest_game`

Retrieve game-level data.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/game`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_stats_rest`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_stats_rest_game()
```

_Last validated n/a._

## `nhl_stats_rest_glossary`

Retrieve the NHL Stats glossary of stat definitions.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/glossary`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `abbreviation` | character | Team abbreviation. |
| `definition` | character | Definition of the stat or term. |
| `first_season_for_stat` | double | First season the stat was tracked (YYYYYYYY). |
| `full_name` | character | Player full name. |
| `language_code` | character | Language code of the entry. |
| `last_updated` | character | Timestamp the entry was last updated. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_stats_rest_glossary()
```

_Last validated n/a._

## `nhl_stats_rest_goalie_report`

Retrieve a goalie statistical report.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/goalie/{report}`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `report` | `report` |  | `Y` |  | report path parameter. |
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `assists` | integer | Assists. |
| `games_played` | integer | Games played. |
| `games_started` | integer | Games started (goalies). |
| `goalie_full_name` | character | Goalie full name. |
| `goals` | integer | Goals scored. |
| `goals_against` | integer | Goals against. |
| `goals_against_average` | double | Goals against average. |
| `last_name` | character | Player last name. |
| `losses` | integer | Losses. |
| `ot_losses` | integer | Overtime losses. |
| `penalty_minutes` | integer | Penalty minutes. |
| `player_id` | integer | Unique player identifier. |
| `points` | integer | Total points (goals + assists). |
| `save_pct` | double | Save percentage. |
| `saves` | integer | Saves made. |
| `season_id` | integer | Season identifier. |
| `shoots_catches` | character | Handedness (shoots/catches). |
| `shots_against` | integer | Shots faced. |
| `shutouts` | integer | Shutouts recorded. |
| `team_abbrevs` | character | Team abbreviation(s). |
| `ties` | character | Total ties. |
| `time_on_ice` | integer | Time on ice in seconds. |
| `wins` | integer | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_stats_rest_goalie_report(report='summary')
```

_Last validated n/a._

## `nhl_stats_rest_leaders_goalies`

Retrieve league leaders for a goalie statistical attribute.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/leaders/goalies/{attribute}`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `attribute` | `attribute` |  | `Y` |  | attribute path parameter. |
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_stats_rest`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_stats_rest_leaders_goalies(attribute='X')
```

_Last validated n/a._

## `nhl_stats_rest_leaders_skaters`

Retrieve league leaders for a skater statistical attribute.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/leaders/skaters/{attribute}`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `attribute` | `attribute` |  | `Y` |  | attribute path parameter. |
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_stats_rest`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_stats_rest_leaders_skaters(attribute='X')
```

_Last validated n/a._

## `nhl_stats_rest_milestones_goalies`

Retrieve milestone data for goalies.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/milestones/goalies`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_stats_rest`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_stats_rest_milestones_goalies()
```

_Last validated n/a._

## `nhl_stats_rest_milestones_skaters`

Retrieve milestone data for skaters.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/milestones/skaters`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_stats_rest`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_stats_rest_milestones_skaters()
```

_Last validated n/a._

## `nhl_stats_rest_players`

Retrieve the NHL player registry.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/players`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_stats_rest`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_stats_rest_players()
```

_Last validated n/a._

## `nhl_stats_rest_season`

Retrieve the list of all NHL seasons.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/season`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `all_star_game_in_use` | integer | Whether an All-Star Game was held this season. |
| `conferences_in_use` | integer | Whether conferences were in use that season. |
| `divisions_in_use` | integer | Whether divisions were in use that season. |
| `end_date` | character | Season end date. |
| `entry_draft_in_use` | integer | Whether an entry draft was in use this season. |
| `formatted_season_id` | character | Human-readable season string (e.g. "2023-24"). |
| `minimum_playoff_minutes_for_goalie_stats_leaders` | integer | Minimum playoff minutes to qualify for goalie stats leaders. |
| `minimum_regular_games_for_goalie_stats_leaders` | integer | Minimum regular-season games to qualify for goalie stats leaders. |
| `nhl_stanley_cup_owner` | integer | Whether the NHL owned the Stanley Cup this season. |
| `number_of_games` | integer | Number of games per team this season. |
| `olympics_participation` | integer | Whether NHL players participated in the Olympics this season. |
| `point_for_ot_loss_in_use` | integer | Whether the overtime-loss point was in use this season. |
| `preseason_startdate` | character | Preseason start date. |
| `regular_season_end_date` | character | Regular-season end date. |
| `row_in_use` | integer | Whether the regulation/overtime/shootout format was in use. |
| `season_ordinal` | integer | Ordinal sequence number of the season. |
| `start_date` | character | Season start date. |
| `supplemental_draft_in_use` | integer | Whether a supplemental draft was in use this season. |
| `ties_in_use` | integer | Whether ties were in use that season. |
| `total_playoff_games` | integer | Total number of playoff games this season. |
| `total_regular_season_games` | integer | Total number of regular-season games this season. |
| `wildcard_in_use` | integer | Whether the wild-card playoff format was in use this season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_stats_rest_season()
```

_Last validated n/a._

## `nhl_stats_rest_shiftcharts`

Retrieve shift-chart data.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/shiftcharts`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_stats_rest`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_stats_rest_shiftcharts()
```

_Last validated n/a._

## `nhl_stats_rest_skater_report`

Retrieve a skater statistical report.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/skater/{report}`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `report` | `report` |  | `Y` |  | report path parameter. |
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `assists` | integer | Assists. |
| `ev_goals` | integer | Even-strength goals. |
| `ev_points` | integer | Even-strength points. |
| `faceoff_win_pct` | double | Faceoff win percentage. |
| `game_winning_goals` | integer | Game-winning goals. |
| `games_played` | integer | Games played. |
| `goals` | integer | Goals scored. |
| `last_name` | character | Player last name. |
| `ot_goals` | integer | Overtime goals. |
| `penalty_minutes` | integer | Penalty minutes. |
| `player_id` | integer | Unique player identifier. |
| `plus_minus` | integer | Plus/minus rating. |
| `points` | integer | Total points (goals + assists). |
| `points_per_game` | double | Points per game. |
| `position_code` | character | Player position code. |
| `pp_goals` | integer | Power-play goals. |
| `pp_points` | integer | Power-play points. |
| `season_id` | integer | Season identifier. |
| `sh_goals` | integer | Short-handed goals. |
| `sh_points` | integer | Short-handed points. |
| `shooting_pct` | double | Shooting percentage. |
| `shoots_catches` | character | Handedness (shoots/catches). |
| `shots` | integer | Shots on goal. |
| `skater_full_name` | character | Player full name. |
| `team_abbrevs` | character | Team abbreviation(s). |
| `time_on_ice_per_game` | double | Average time on ice per game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_stats_rest_skater_report(report='summary')
```

_Last validated n/a._

## `nhl_stats_rest_team`

Retrieve the list of all NHL teams.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/team`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_stats_rest`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_stats_rest_team()
```

_Last validated n/a._

## `nhl_stats_rest_team_by_id`

Retrieve a single team by its numeric ID.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/team/id/{team_id}`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_stats_rest`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_stats_rest_team_by_id(team_id=10)
```

_Last validated n/a._

## `nhl_stats_rest_team_report`

Retrieve a team statistical report.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/team/{report}`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `report` | `report` |  | `Y` |  | report path parameter. |
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `faceoff_win_pct` | double | Faceoff win percentage. |
| `games_played` | integer | Games played. |
| `goals_against` | integer | Goals against. |
| `goals_against_per_game` | double | Goals against per game. |
| `goals_for` | integer | Goals for. |
| `goals_for_per_game` | double | Goals for per game. |
| `losses` | integer | Losses. |
| `ot_losses` | integer | Overtime losses. |
| `penalty_kill_net_pct` | double | Net penalty kill percentage. |
| `penalty_kill_pct` | double | Penalty kill percentage. |
| `point_pct` | double | Points percentage. |
| `points` | integer | Total points (goals + assists). |
| `power_play_net_pct` | double | Net power play percentage. |
| `power_play_pct` | double | Power play percentage. |
| `regulation_and_ot_wins` | integer | Wins in regulation and overtime. |
| `season_id` | integer | Season identifier. |
| `shots_against_per_game` | double | Shots against per game. |
| `shots_for_per_game` | double | Shots for per game. |
| `team_full_name` | character | Full team name. |
| `team_id` | integer | Unique team identifier. |
| `team_shutouts` | integer | Team shutouts. |
| `ties` | character | Total ties. |
| `wins` | integer | Wins. |
| `wins_in_regulation` | integer | Wins in regulation. |
| `wins_in_shootout` | integer | Wins in shootout. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_stats_rest_team_report(report='summary')
```

_Last validated n/a._
