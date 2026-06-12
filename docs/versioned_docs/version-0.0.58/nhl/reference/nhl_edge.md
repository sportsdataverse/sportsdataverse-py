---
title: NHL — NHL EDGE API
sidebar_label: NHL EDGE API
sidebar_position: 11
---
# NHL — NHL EDGE API

`sportsdataverse.nhl` — 35 endpoints.

## `nhl_edge_skater_detail`

Pull EDGE detail stats for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-detail/8480801](https://api-web.nhle.com/v1/edge/skater-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character |  |
| `sog_summary` | character |  |
| `sog_details` | character |  |
| `player_id` | integer | Unique player identifier. |
| `player_first_name_default` | character | Player first name (default language). |
| `player_last_name_default` | character | Player last name (default language). |
| `player_birth_date` | character | Participant birth date (YYYY-MM-DD). |
| `player_shoots_catches` | character |  |
| `player_sweater_number` | integer | Player jersey number. |
| `player_position` | character | Primary player position. |
| `player_slug` | character | URL slug for the player. |
| `player_headshot` | character | URL to the player headshot image. |
| `player_goals` | integer |  |
| `player_assists` | integer |  |
| `player_points` | integer | Player points. |
| `player_games_played` | integer |  |
| `player_team_common_name_default` | character | Player team common name (default locale). |
| `player_team_place_name_with_preposition_default` | character | Player team place name with preposition (default locale). |
| `player_team_place_name_with_preposition_fr` | character | Player team place name with preposition (French locale). |
| `player_team_abbrev` | character | Player team abbreviation. |
| `player_team_team_logo_light` | character | Player team light-mode logo URL. |
| `player_team_team_logo_dark` | character | Player team dark-mode logo URL. |
| `top_shot_speed_imperial` | double |  |
| `top_shot_speed_metric` | double |  |
| `top_shot_speed_percentile` | double |  |
| `top_shot_speed_league_avg_imperial` | double |  |
| `top_shot_speed_league_avg_metric` | double |  |
| `top_shot_speed_overlay_player_first_name_default` | character |  |
| `top_shot_speed_overlay_player_last_name_default` | character |  |
| `top_shot_speed_overlay_game_date` | character |  |
| `top_shot_speed_overlay_away_team_abbrev` | character |  |
| `top_shot_speed_overlay_away_team_score` | integer |  |
| `top_shot_speed_overlay_home_team_abbrev` | character |  |
| `top_shot_speed_overlay_home_team_score` | integer |  |
| `top_shot_speed_overlay_game_outcome_last_period_type` | character |  |
| `top_shot_speed_overlay_period_descriptor_max_regulation_periods` | integer |  |
| `top_shot_speed_overlay_period_descriptor_number` | integer |  |
| `top_shot_speed_overlay_period_descriptor_period_type` | character |  |
| `top_shot_speed_overlay_time_in_period` | character |  |
| `top_shot_speed_overlay_game_type` | integer |  |
| `skating_speed_speed_max_imperial` | double |  |
| `skating_speed_speed_max_metric` | double |  |
| `skating_speed_speed_max_percentile` | double |  |
| `skating_speed_speed_max_league_avg_imperial` | double |  |
| `skating_speed_speed_max_league_avg_metric` | double |  |
| `skating_speed_speed_max_overlay_player_first_name_default` | character |  |
| `skating_speed_speed_max_overlay_player_last_name_default` | character |  |
| `skating_speed_speed_max_overlay_game_date` | character |  |
| `skating_speed_speed_max_overlay_away_team_abbrev` | character |  |
| `skating_speed_speed_max_overlay_away_team_score` | integer |  |
| `skating_speed_speed_max_overlay_home_team_abbrev` | character |  |
| `skating_speed_speed_max_overlay_home_team_score` | integer |  |
| `skating_speed_speed_max_overlay_game_outcome_last_period_type` | character |  |
| `skating_speed_speed_max_overlay_period_descriptor_max_regulation_periods` | integer |  |
| `skating_speed_speed_max_overlay_period_descriptor_number` | integer |  |
| `skating_speed_speed_max_overlay_period_descriptor_period_type` | character |  |
| `skating_speed_speed_max_overlay_time_in_period` | character |  |
| `skating_speed_speed_max_overlay_game_type` | integer |  |
| `skating_speed_bursts_over20_value` | integer |  |
| `skating_speed_bursts_over20_percentile` | double |  |
| `skating_speed_bursts_over20_league_avg_value` | double |  |
| `total_distance_skated_imperial` | double |  |
| `total_distance_skated_metric` | double |  |
| `total_distance_skated_percentile` | double |  |
| `total_distance_skated_league_avg_imperial` | double |  |
| `total_distance_skated_league_avg_metric` | double |  |
| `distance_max_game_imperial` | double |  |
| `distance_max_game_metric` | double |  |
| `distance_max_game_percentile` | double |  |
| `distance_max_game_league_avg_imperial` | double |  |
| `distance_max_game_league_avg_metric` | double |  |
| `distance_max_game_overlay_player_first_name_default` | character |  |
| `distance_max_game_overlay_player_last_name_default` | character |  |
| `distance_max_game_overlay_game_date` | character |  |
| `distance_max_game_overlay_away_team_abbrev` | character |  |
| `distance_max_game_overlay_away_team_score` | integer |  |
| `distance_max_game_overlay_home_team_abbrev` | character |  |
| `distance_max_game_overlay_home_team_score` | integer |  |
| `distance_max_game_overlay_game_outcome_last_period_type` | character |  |
| `distance_max_game_overlay_game_outcome_ot_periods` | integer |  |
| `distance_max_game_overlay_period_descriptor_max_regulation_periods` | integer |  |
| `distance_max_game_overlay_period_descriptor_number` | integer |  |
| `distance_max_game_overlay_period_descriptor_period_type` | character |  |
| `distance_max_game_overlay_game_type` | integer |  |
| `zone_time_details_offensive_zone_pctg` | double |  |
| `zone_time_details_offensive_zone_percentile` | double |  |
| `zone_time_details_offensive_zone_league_avg` | double |  |
| `zone_time_details_offensive_zone_ev_pctg` | double |  |
| `zone_time_details_offensive_zone_ev_percentile` | double |  |
| `zone_time_details_offensive_zone_ev_league_avg` | double |  |
| `zone_time_details_neutral_zone_pctg` | double |  |
| `zone_time_details_neutral_zone_percentile` | double |  |
| `zone_time_details_neutral_zone_league_avg` | double |  |
| `zone_time_details_defensive_zone_pctg` | double |  |
| `zone_time_details_defensive_zone_percentile` | double |  |
| `zone_time_details_defensive_zone_league_avg` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_skater_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_skater_comparison`

Pull EDGE comparison data for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-comparison/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-comparison/8480801](https://api-web.nhle.com/v1/edge/skater-comparison/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_skater_comparison(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_skater_shot_location_detail`

Pull EDGE shot-location detail for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-shot-location-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-shot-location-detail/8480801](https://api-web.nhle.com/v1/edge/skater-shot-location-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_shot_location`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_skater_shot_location_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_skater_shot_location_top_10`

Pull the EDGE top-10 skaters for a shot-location category.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-shot-location-top-10/{position}/{category}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-shot-location-top-10/forwards/shots/points](https://api-web.nhle.com/v1/edge/skater-shot-location-top-10/forwards/shots/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `position` | `position` |  | `Y` |  | position path parameter. |
| `category` | `category` |  | `Y` |  | category path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_skater_shot_location_top_10(position='forwards', category='shots', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_skater_shot_speed_detail`

Pull EDGE shot-speed detail for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-shot-speed-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-shot-speed-detail/8480801](https://api-web.nhle.com/v1/edge/skater-shot-speed-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `hardest_shots` | character |  |
| `shot_speed_details_top_shot_speed_imperial` | double |  |
| `shot_speed_details_top_shot_speed_metric` | double |  |
| `shot_speed_details_top_shot_speed_percentile` | double |  |
| `shot_speed_details_top_shot_speed_league_avg_imperial` | double |  |
| `shot_speed_details_top_shot_speed_league_avg_metric` | double |  |
| `shot_speed_details_top_shot_speed_overlay_player_first_name_default` | character |  |
| `shot_speed_details_top_shot_speed_overlay_player_last_name_default` | character |  |
| `shot_speed_details_top_shot_speed_overlay_game_date` | character |  |
| `shot_speed_details_top_shot_speed_overlay_away_team_abbrev` | character |  |
| `shot_speed_details_top_shot_speed_overlay_away_team_score` | integer |  |
| `shot_speed_details_top_shot_speed_overlay_home_team_abbrev` | character |  |
| `shot_speed_details_top_shot_speed_overlay_home_team_score` | integer |  |
| `shot_speed_details_top_shot_speed_overlay_game_outcome_last_period_type` | character |  |
| `shot_speed_details_top_shot_speed_overlay_period_descriptor_max_regulation_periods` | integer |  |
| `shot_speed_details_top_shot_speed_overlay_period_descriptor_number` | integer |  |
| `shot_speed_details_top_shot_speed_overlay_period_descriptor_period_type` | character |  |
| `shot_speed_details_top_shot_speed_overlay_time_in_period` | character |  |
| `shot_speed_details_top_shot_speed_overlay_game_type` | integer |  |
| `shot_speed_details_avg_shot_speed_imperial` | double |  |
| `shot_speed_details_avg_shot_speed_metric` | double |  |
| `shot_speed_details_avg_shot_speed_percentile` | double |  |
| `shot_speed_details_avg_shot_speed_league_avg_imperial` | double |  |
| `shot_speed_details_avg_shot_speed_league_avg_metric` | double |  |
| `shot_speed_details_shot_attempts_over100_value` | integer |  |
| `shot_speed_details_shot_attempts_over100_percentile` | double |  |
| `shot_speed_details_shot_attempts_over100_league_avg` | double |  |
| `shot_speed_details_shot_attempts90_to100_value` | integer |  |
| `shot_speed_details_shot_attempts90_to100_percentile` | double |  |
| `shot_speed_details_shot_attempts90_to100_league_avg` | double |  |
| `shot_speed_details_shot_attempts80_to90_value` | integer |  |
| `shot_speed_details_shot_attempts80_to90_percentile` | double |  |
| `shot_speed_details_shot_attempts80_to90_league_avg` | double |  |
| `shot_speed_details_shot_attempts70_to80_value` | integer |  |
| `shot_speed_details_shot_attempts70_to80_percentile` | double |  |
| `shot_speed_details_shot_attempts70_to80_league_avg` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_skater_shot_speed_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_skater_shot_speed_top_10`

Pull the EDGE top-10 skaters by shot speed.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-shot-speed-top-10/{positions}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-shot-speed-top-10/defense/points](https://api-web.nhle.com/v1/edge/skater-shot-speed-top-10/defense/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `positions` | `positions` |  | `Y` |  | positions path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_skater_shot_speed_top_10(positions='defense', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_skater_skating_distance_detail`

Pull EDGE skating-distance detail for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-skating-distance-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-skating-distance-detail/8480801](https://api-web.nhle.com/v1/edge/skater-skating-distance-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_skater_skating_distance_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_skater_skating_speed_detail`

Pull EDGE skating-speed detail for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-skating-speed-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-skating-speed-detail/8480801](https://api-web.nhle.com/v1/edge/skater-skating-speed-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_skater_skating_speed_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_skater_speed_top_10`

Pull the EDGE top-10 skaters by skating speed.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-speed-top-10/{positions}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-speed-top-10/defense/points](https://api-web.nhle.com/v1/edge/skater-speed-top-10/defense/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `positions` | `positions` |  | `Y` |  | positions path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_skater_speed_top_10(positions='defense', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_skater_distance_top_10`

Pull the EDGE top-10 skaters by skating distance.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-distance-top-10/{positions}/{strength}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-distance-top-10/defense/ev/points](https://api-web.nhle.com/v1/edge/skater-distance-top-10/defense/ev/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `positions` | `positions` |  | `Y` |  | positions path parameter. |
| `strength` | `strength` |  | `Y` |  | strength path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_skater_distance_top_10(positions='defense', strength='ev', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_skater_zone_time`

Pull EDGE zone-time detail for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-zone-time/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-zone-time/8480801](https://api-web.nhle.com/v1/edge/skater-zone-time/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `strength_code` | character | Strength state code (e.g., all, even, pp, pk). |
| `offensive_zone_pctg` | double | Percentage of time spent in the offensive zone. |
| `offensive_zone_percentile` | double | League percentile rank for offensive-zone time. |
| `offensive_zone_league_avg` | double | League average offensive-zone time percentage. |
| `neutral_zone_pctg` | double | Percentage of time spent in the neutral zone. |
| `neutral_zone_percentile` | double | League percentile rank for neutral-zone time. |
| `neutral_zone_league_avg` | double | League average neutral-zone time percentage. |
| `defensive_zone_pctg` | double | Percentage of time spent in the defensive zone. |
| `defensive_zone_percentile` | double | League percentile rank for defensive-zone time. |
| `defensive_zone_league_avg` | double | League average defensive-zone time percentage. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_skater_zone_time(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_skater_zone_time_top_10`

Pull the EDGE top-10 skaters by zone time.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-zone-time-top-10/{positions}/{strength}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-zone-time-top-10/defense/ev/points](https://api-web.nhle.com/v1/edge/skater-zone-time-top-10/defense/ev/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `positions` | `positions` |  | `Y` |  | positions path parameter. |
| `strength` | `strength` |  | `Y` |  | strength path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_skater_zone_time_top_10(positions='defense', strength='ev', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_skater_landing`

Pull the EDGE skater landing page (summary across all skaters).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-landing/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-landing](https://api-web.nhle.com/v1/edge/skater-landing)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_skater_landing()
```

_Last validated n/a._

## `nhl_edge_goalie_detail`

Pull EDGE detail stats for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-detail/8480801](https://api-web.nhle.com/v1/edge/goalie-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character |  |
| `shot_location_summary` | character |  |
| `shot_location_details` | character |  |
| `player_id` | integer | Unique player identifier. |
| `player_first_name_default` | character | Player first name (default language). |
| `player_last_name_default` | character | Player last name (default language). |
| `player_birth_date` | character | Participant birth date (YYYY-MM-DD). |
| `player_shoots_catches` | character |  |
| `player_sweater_number` | integer | Player jersey number. |
| `player_slug` | character | URL slug for the player. |
| `player_headshot` | character | URL to the player headshot image. |
| `player_wins` | integer |  |
| `player_losses` | integer |  |
| `player_overtime_losses` | integer |  |
| `player_goals_against_avg` | double |  |
| `player_save_pctg` | double |  |
| `player_games_played` | integer |  |
| `player_team_common_name_default` | character | Player team common name (default locale). |
| `player_team_place_name_with_preposition_default` | character | Player team place name with preposition (default locale). |
| `player_team_place_name_with_preposition_fr` | character | Player team place name with preposition (French locale). |
| `player_team_abbrev` | character | Player team abbreviation. |
| `player_team_team_logo_light` | character | Player team light-mode logo URL. |
| `player_team_team_logo_dark` | character | Player team dark-mode logo URL. |
| `stats_goals_against_avg_value` | double |  |
| `stats_goals_against_avg_percentile` | double |  |
| `stats_goals_against_avg_league_avg` | double |  |
| `stats_games_above900_value` | double |  |
| `stats_games_above900_percentile` | double |  |
| `stats_games_above900_league_avg` | double |  |
| `stats_goal_differential_per60_value` | double |  |
| `stats_goal_differential_per60_percentile` | double |  |
| `stats_goal_differential_per60_league_avg` | double |  |
| `stats_goal_support_avg_value` | double |  |
| `stats_goal_support_avg_percentile` | double |  |
| `stats_goal_support_avg_league_avg` | double |  |
| `stats_point_pctg_value` | double |  |
| `stats_point_pctg_percentile` | double |  |
| `stats_point_pctg_league_avg` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_goalie_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_goalie_5v5_detail`

Pull EDGE 5-on-5 detail stats for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-5v5-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-5v5-detail/8480801](https://api-web.nhle.com/v1/edge/goalie-5v5-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_goalie_5v5_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_goalie_5v5_top_10`

Pull the EDGE top-10 goalies by 5-on-5 metrics.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-5v5-top-10/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-5v5-top-10/points](https://api-web.nhle.com/v1/edge/goalie-5v5-top-10/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_goalie_5v5_top_10(sort_by='points')
```

_Last validated n/a._

## `nhl_edge_goalie_comparison`

Pull EDGE comparison data for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-comparison/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-comparison/8480801](https://api-web.nhle.com/v1/edge/goalie-comparison/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_goalie_comparison(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_goalie_save_percentage_detail`

Pull EDGE save-percentage detail for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-save-percentage-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-save-percentage-detail/8480801](https://api-web.nhle.com/v1/edge/goalie-save-percentage-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_goalie_save_percentage_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_goalie_edge_save_pctg_top_10`

Pull the EDGE top-10 goalies by save-percentage.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-edge-save-pctg-top-10/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-edge-save-pctg-top-10/points](https://api-web.nhle.com/v1/edge/goalie-edge-save-pctg-top-10/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_goalie_edge_save_pctg_top_10(sort_by='points')
```

_Last validated n/a._

## `nhl_edge_goalie_shot_location_detail`

Pull EDGE shot-location detail for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-shot-location-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-shot-location-detail/8480801](https://api-web.nhle.com/v1/edge/goalie-shot-location-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `area` | character | Net/ice zone the shots were taken from. |
| `shots_against` | integer | Shots faced. |
| `saves` | integer | Saves made. |
| `goals_against` | integer | Goals against. |
| `save_pctg` | double | Save percentage. |
| `shots_against_percentile` | double | League percentile rank for shots against. |
| `saves_percentile` | double | League percentile rank for saves. |
| `goals_against_percentile` | double | League percentile rank for goals against. |
| `save_pctg_percentile` | double | League percentile rank for save percentage. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_goalie_shot_location_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_goalie_shot_location_top_10`

Pull the EDGE top-10 goalies for a shot-location category.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-shot-location-top-10/{category}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-shot-location-top-10/shots/points](https://api-web.nhle.com/v1/edge/goalie-shot-location-top-10/shots/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `category` | `category` |  | `Y` |  | category path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_goalie_shot_location_top_10(category='shots', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_goalie_landing`

Pull the EDGE goalie landing page (summary across all goalies).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-landing/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-landing](https://api-web.nhle.com/v1/edge/goalie-landing)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_goalie_landing()
```

_Last validated n/a._

## `nhl_edge_team_detail`

Pull EDGE detail stats for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-detail/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-detail/10](https://api-web.nhle.com/v1/edge/team-detail/10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character |  |
| `sog_summary` | character |  |
| `sog_details` | character |  |
| `team_id` | integer | Unique team identifier. |
| `team_common_name_default` | character | Team common name (default language). |
| `team_place_name_with_preposition_default` | character | Team place name with preposition (default). |
| `team_place_name_with_preposition_fr` | character | Team place name with preposition (French). |
| `team_abbrev` | character | Team abbreviation. |
| `team_team_logo_light` | character | URL to the team light logo. |
| `team_team_logo_dark` | character | URL to the team dark logo. |
| `team_slug` | character | Team URL slug. |
| `team_conference` | character |  |
| `team_division` | character |  |
| `team_wins` | integer | Team wins. |
| `team_losses` | integer | Team losses. |
| `team_ot_losses` | integer |  |
| `team_games_played` | integer |  |
| `team_points` | integer | Total points scored by the player's team in this game. |
| `shot_speed_shot_attempts_over90_value` | integer |  |
| `shot_speed_shot_attempts_over90_rank` | integer |  |
| `shot_speed_top_shot_speed_imperial` | double |  |
| `shot_speed_top_shot_speed_metric` | double |  |
| `shot_speed_top_shot_speed_rank` | integer |  |
| `shot_speed_top_shot_speed_league_avg_imperial` | double |  |
| `shot_speed_top_shot_speed_league_avg_metric` | double |  |
| `shot_speed_top_shot_speed_overlay_player_first_name_default` | character |  |
| `shot_speed_top_shot_speed_overlay_player_last_name_default` | character |  |
| `shot_speed_top_shot_speed_overlay_game_date` | character |  |
| `shot_speed_top_shot_speed_overlay_away_team_abbrev` | character |  |
| `shot_speed_top_shot_speed_overlay_away_team_score` | integer |  |
| `shot_speed_top_shot_speed_overlay_home_team_abbrev` | character |  |
| `shot_speed_top_shot_speed_overlay_home_team_score` | integer |  |
| `shot_speed_top_shot_speed_overlay_game_outcome_last_period_type` | character |  |
| `shot_speed_top_shot_speed_overlay_game_outcome_ot_periods` | integer |  |
| `shot_speed_top_shot_speed_overlay_period_descriptor_max_regulation_periods` | integer |  |
| `shot_speed_top_shot_speed_overlay_period_descriptor_number` | integer |  |
| `shot_speed_top_shot_speed_overlay_period_descriptor_period_type` | character |  |
| `shot_speed_top_shot_speed_overlay_time_in_period` | character |  |
| `shot_speed_top_shot_speed_overlay_game_type` | integer |  |
| `skating_speed_bursts_over22_value` | integer |  |
| `skating_speed_bursts_over22_rank` | integer |  |
| `skating_speed_bursts_over20_value` | integer |  |
| `skating_speed_bursts_over20_rank` | integer |  |
| `skating_speed_bursts_over20_league_avg_value` | integer |  |
| `skating_speed_speed_max_imperial` | double |  |
| `skating_speed_speed_max_metric` | double |  |
| `skating_speed_speed_max_rank` | integer |  |
| `skating_speed_speed_max_league_avg_imperial` | double |  |
| `skating_speed_speed_max_league_avg_metric` | double |  |
| `skating_speed_speed_max_overlay_player_first_name_default` | character |  |
| `skating_speed_speed_max_overlay_player_last_name_default` | character |  |
| `skating_speed_speed_max_overlay_game_date` | character |  |
| `skating_speed_speed_max_overlay_away_team_abbrev` | character |  |
| `skating_speed_speed_max_overlay_away_team_score` | integer |  |
| `skating_speed_speed_max_overlay_home_team_abbrev` | character |  |
| `skating_speed_speed_max_overlay_home_team_score` | integer |  |
| `skating_speed_speed_max_overlay_game_outcome_last_period_type` | character |  |
| `skating_speed_speed_max_overlay_period_descriptor_max_regulation_periods` | integer |  |
| `skating_speed_speed_max_overlay_period_descriptor_number` | integer |  |
| `skating_speed_speed_max_overlay_period_descriptor_period_type` | character |  |
| `skating_speed_speed_max_overlay_time_in_period` | character |  |
| `skating_speed_speed_max_overlay_game_type` | integer |  |
| `distance_skated_total_imperial` | double |  |
| `distance_skated_total_metric` | double |  |
| `distance_skated_total_rank` | integer |  |
| `distance_skated_total_league_avg_imperial` | double |  |
| `distance_skated_total_league_avg_metric` | double |  |
| `zone_time_details_offensive_zone_pctg` | double |  |
| `zone_time_details_offensive_zone_rank` | integer |  |
| `zone_time_details_offensive_zone_league_avg` | double |  |
| `zone_time_details_offensive_zone_ev_pctg` | double |  |
| `zone_time_details_offensive_zone_ev_rank` | integer |  |
| `zone_time_details_offensive_zone_ev_league_avg` | double |  |
| `zone_time_details_neutral_zone_pctg` | double |  |
| `zone_time_details_neutral_zone_rank` | integer |  |
| `zone_time_details_neutral_zone_league_avg` | double |  |
| `zone_time_details_defensive_zone_pctg` | double |  |
| `zone_time_details_defensive_zone_rank` | integer |  |
| `zone_time_details_defensive_zone_league_avg` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_team_detail(team_id=10)
```

_Last validated n/a._

## `nhl_edge_team_landing`

Pull the EDGE team landing page (summary across all teams).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-landing/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-landing](https://api-web.nhle.com/v1/edge/team-landing)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_team_landing()
```

_Last validated n/a._

## `nhl_edge_team_shot_location_detail`

Pull EDGE shot-location detail for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-shot-location-detail/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-shot-location-detail/10](https://api-web.nhle.com/v1/edge/team-shot-location-detail/10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `area` | character | Net/ice zone the shots were taken from. |
| `sog` | integer | Shots on goal from the area. |
| `sog_rank` | integer | League rank for shots on goal from the area. |
| `goals` | integer | Goals scored. |
| `goals_rank` | integer | League rank for goals scored from the area. |
| `shooting_pctg` | double | Shooting percentage from the area. |
| `shooting_pctg_rank` | integer | League rank for shooting percentage from the area. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_team_shot_location_detail(team_id=10)
```

_Last validated n/a._

## `nhl_edge_team_shot_location_top_10`

Pull the EDGE top-10 teams for a shot-location category.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-shot-location-top-10/{position}/{category}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-shot-location-top-10/forwards/shots/points](https://api-web.nhle.com/v1/edge/team-shot-location-top-10/forwards/shots/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `position` | `position` |  | `Y` |  | position path parameter. |
| `category` | `category` |  | `Y` |  | category path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_team_shot_location_top_10(position='forwards', category='shots', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_team_shot_speed_detail`

Pull EDGE shot-speed detail for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-shot-speed-detail/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-shot-speed-detail/10](https://api-web.nhle.com/v1/edge/team-shot-speed-detail/10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_team_shot_speed_detail(team_id=10)
```

_Last validated n/a._

## `nhl_edge_team_skating_distance_detail`

Pull EDGE skating-distance detail for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-skating-distance-detail/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-skating-distance-detail/10](https://api-web.nhle.com/v1/edge/team-skating-distance-detail/10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull EDGE skating-distance detail for a single team.

### Example

```python
nhl_edge_team_skating_distance_detail(team_id=10)
```

_Last validated n/a._

## `nhl_edge_team_skating_distance_top_10`

Pull the EDGE top-10 teams by skating distance.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-skating-distance-top-10/{positions}/{strength}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-skating-distance-top-10/defense/ev/points](https://api-web.nhle.com/v1/edge/team-skating-distance-top-10/defense/ev/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `positions` | `positions` |  | `Y` |  | positions path parameter. |
| `strength` | `strength` |  | `Y` |  | strength path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_team_skating_distance_top_10(positions='defense', strength='ev', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_team_skating_speed_detail`

Pull EDGE skating-speed detail for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-skating-speed-detail/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-skating-speed-detail/10](https://api-web.nhle.com/v1/edge/team-skating-speed-detail/10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull EDGE skating-speed detail for a single team.

### Example

```python
nhl_edge_team_skating_speed_detail(team_id=10)
```

_Last validated n/a._

## `nhl_edge_team_skating_speed_top_10`

Pull the EDGE top-10 teams by skating speed.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-skating-speed-top-10/{positions}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-skating-speed-top-10/defense/points](https://api-web.nhle.com/v1/edge/team-skating-speed-top-10/defense/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `positions` | `positions` |  | `Y` |  | positions path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_team_skating_speed_top_10(positions='defense', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_team_zone_time_details`

Pull EDGE zone-time details for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-zone-time-details/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-zone-time-details/10](https://api-web.nhle.com/v1/edge/team-zone-time-details/10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_zone_time`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_team_zone_time_details(team_id=10)
```

_Last validated n/a._

## `nhl_edge_team_zone_time_top_10`

Pull the EDGE top-10 teams by zone time.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-zone-time-top-10/{strength}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-zone-time-top-10/ev/points](https://api-web.nhle.com/v1/edge/team-zone-time-top-10/ev/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `strength` | `strength` |  | `Y` |  | strength path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_team_zone_time_top_10(strength='ev', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_cat_skater_detail`

Pull categorized (cat) EDGE detail stats for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/cat/edge/skater-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/cat/edge/skater-detail/8480801](https://api-web.nhle.com/v1/cat/edge/skater-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_cat_skater_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_cat_goalie_detail`

Pull categorized (cat) EDGE detail stats for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/cat/edge/goalie-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/cat/edge/goalie-detail/8480801](https://api-web.nhle.com/v1/cat/edge/goalie-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_cat_goalie_detail(player_id=8480801)
```

_Last validated n/a._
