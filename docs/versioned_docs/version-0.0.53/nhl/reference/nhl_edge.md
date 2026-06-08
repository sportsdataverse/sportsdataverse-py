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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `player_id` | `player_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull EDGE detail stats for a single skater.

### Example

```python
nhl_edge_skater_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_skater_comparison`

Pull EDGE comparison data for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-comparison/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-comparison/8480801](https://api-web.nhle.com/v1/edge/skater-comparison/8480801)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `player_id` | `player_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull EDGE comparison data for a single skater.

### Example

```python
nhl_edge_skater_comparison(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_skater_shot_location_detail`

Pull EDGE shot-location detail for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-shot-location-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-shot-location-detail/8480801](https://api-web.nhle.com/v1/edge/skater-shot-location-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `player_id` | `player_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull EDGE shot-location detail for a single skater.

### Example

```python
nhl_edge_skater_shot_location_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_skater_shot_location_top_10`

Pull the EDGE top-10 skaters for a shot-location category.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-shot-location-top-10/{position}/{category}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-shot-location-top-10/forwards/shots/points](https://api-web.nhle.com/v1/edge/skater-shot-location-top-10/forwards/shots/points)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `position` | `position` |  | `Y` |  |
| `category` | `category` |  | `Y` |  |
| `sort_by` | `sort_by` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull the EDGE top-10 skaters for a shot-location category.

### Example

```python
nhl_edge_skater_shot_location_top_10(position='forwards', category='shots', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_skater_shot_speed_detail`

Pull EDGE shot-speed detail for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-shot-speed-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-shot-speed-detail/8480801](https://api-web.nhle.com/v1/edge/skater-shot-speed-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `player_id` | `player_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull EDGE shot-speed detail for a single skater.

### Example

```python
nhl_edge_skater_shot_speed_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_skater_shot_speed_top_10`

Pull the EDGE top-10 skaters by shot speed.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-shot-speed-top-10/{positions}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-shot-speed-top-10/defense/points](https://api-web.nhle.com/v1/edge/skater-shot-speed-top-10/defense/points)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `positions` | `positions` |  | `Y` |  |
| `sort_by` | `sort_by` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull the EDGE top-10 skaters by shot speed.

### Example

```python
nhl_edge_skater_shot_speed_top_10(positions='defense', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_skater_skating_distance_detail`

Pull EDGE skating-distance detail for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-skating-distance-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-skating-distance-detail/8480801](https://api-web.nhle.com/v1/edge/skater-skating-distance-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `player_id` | `player_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull EDGE skating-distance detail for a single skater.

### Example

```python
nhl_edge_skater_skating_distance_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_skater_skating_speed_detail`

Pull EDGE skating-speed detail for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-skating-speed-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-skating-speed-detail/8480801](https://api-web.nhle.com/v1/edge/skater-skating-speed-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `player_id` | `player_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull EDGE skating-speed detail for a single skater.

### Example

```python
nhl_edge_skater_skating_speed_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_skater_speed_top_10`

Pull the EDGE top-10 skaters by skating speed.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-speed-top-10/{positions}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-speed-top-10/defense/points](https://api-web.nhle.com/v1/edge/skater-speed-top-10/defense/points)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `positions` | `positions` |  | `Y` |  |
| `sort_by` | `sort_by` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull the EDGE top-10 skaters by skating speed.

### Example

```python
nhl_edge_skater_speed_top_10(positions='defense', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_skater_distance_top_10`

Pull the EDGE top-10 skaters by skating distance.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-distance-top-10/{positions}/{strength}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-distance-top-10/defense/ev/points](https://api-web.nhle.com/v1/edge/skater-distance-top-10/defense/ev/points)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `positions` | `positions` |  | `Y` |  |
| `strength` | `strength` |  | `Y` |  |
| `sort_by` | `sort_by` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull the EDGE top-10 skaters by skating distance.

### Example

```python
nhl_edge_skater_distance_top_10(positions='defense', strength='ev', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_skater_zone_time`

Pull EDGE zone-time detail for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-zone-time/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-zone-time/8480801](https://api-web.nhle.com/v1/edge/skater-zone-time/8480801)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `player_id` | `player_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull EDGE zone-time detail for a single skater.

### Example

```python
nhl_edge_skater_zone_time(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_skater_zone_time_top_10`

Pull the EDGE top-10 skaters by zone time.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-zone-time-top-10/{positions}/{strength}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-zone-time-top-10/defense/ev/points](https://api-web.nhle.com/v1/edge/skater-zone-time-top-10/defense/ev/points)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `positions` | `positions` |  | `Y` |  |
| `strength` | `strength` |  | `Y` |  |
| `sort_by` | `sort_by` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull the EDGE top-10 skaters by zone time.

### Example

```python
nhl_edge_skater_zone_time_top_10(positions='defense', strength='ev', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_skater_landing`

Pull the EDGE skater landing page (summary across all skaters).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-landing/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-landing](https://api-web.nhle.com/v1/edge/skater-landing)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull the EDGE skater landing page (summary across all skaters).

### Example

```python
nhl_edge_skater_landing()
```

_Last validated n/a._

## `nhl_edge_goalie_detail`

Pull EDGE detail stats for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-detail/8480801](https://api-web.nhle.com/v1/edge/goalie-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `player_id` | `player_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull EDGE detail stats for a single goalie.

### Example

```python
nhl_edge_goalie_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_goalie_5v5_detail`

Pull EDGE 5-on-5 detail stats for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-5v5-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-5v5-detail/8480801](https://api-web.nhle.com/v1/edge/goalie-5v5-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `player_id` | `player_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull EDGE 5-on-5 detail stats for a single goalie.

### Example

```python
nhl_edge_goalie_5v5_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_goalie_5v5_top_10`

Pull the EDGE top-10 goalies by 5-on-5 metrics.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-5v5-top-10/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-5v5-top-10/points](https://api-web.nhle.com/v1/edge/goalie-5v5-top-10/points)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `sort_by` | `sort_by` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull the EDGE top-10 goalies by 5-on-5 metrics.

### Example

```python
nhl_edge_goalie_5v5_top_10(sort_by='points')
```

_Last validated n/a._

## `nhl_edge_goalie_comparison`

Pull EDGE comparison data for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-comparison/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-comparison/8480801](https://api-web.nhle.com/v1/edge/goalie-comparison/8480801)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `player_id` | `player_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull EDGE comparison data for a single goalie.

### Example

```python
nhl_edge_goalie_comparison(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_goalie_save_percentage_detail`

Pull EDGE save-percentage detail for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-save-percentage-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-save-percentage-detail/8480801](https://api-web.nhle.com/v1/edge/goalie-save-percentage-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `player_id` | `player_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull EDGE save-percentage detail for a single goalie.

### Example

```python
nhl_edge_goalie_save_percentage_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_goalie_edge_save_pctg_top_10`

Pull the EDGE top-10 goalies by save-percentage.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-edge-save-pctg-top-10/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-edge-save-pctg-top-10/points](https://api-web.nhle.com/v1/edge/goalie-edge-save-pctg-top-10/points)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `sort_by` | `sort_by` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull the EDGE top-10 goalies by save-percentage.

### Example

```python
nhl_edge_goalie_edge_save_pctg_top_10(sort_by='points')
```

_Last validated n/a._

## `nhl_edge_goalie_shot_location_detail`

Pull EDGE shot-location detail for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-shot-location-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-shot-location-detail/8480801](https://api-web.nhle.com/v1/edge/goalie-shot-location-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `player_id` | `player_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull EDGE shot-location detail for a single goalie.

### Example

```python
nhl_edge_goalie_shot_location_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_goalie_shot_location_top_10`

Pull the EDGE top-10 goalies for a shot-location category.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-shot-location-top-10/{category}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-shot-location-top-10/shots/points](https://api-web.nhle.com/v1/edge/goalie-shot-location-top-10/shots/points)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `category` | `category` |  | `Y` |  |
| `sort_by` | `sort_by` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull the EDGE top-10 goalies for a shot-location category.

### Example

```python
nhl_edge_goalie_shot_location_top_10(category='shots', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_goalie_landing`

Pull the EDGE goalie landing page (summary across all goalies).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-landing/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-landing](https://api-web.nhle.com/v1/edge/goalie-landing)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull the EDGE goalie landing page (summary across all goalies).

### Example

```python
nhl_edge_goalie_landing()
```

_Last validated n/a._

## `nhl_edge_team_detail`

Pull EDGE detail stats for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-detail/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-detail/10](https://api-web.nhle.com/v1/edge/team-detail/10)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull EDGE detail stats for a single team.

### Example

```python
nhl_edge_team_detail(team_id=10)
```

_Last validated n/a._

## `nhl_edge_team_landing`

Pull the EDGE team landing page (summary across all teams).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-landing/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-landing](https://api-web.nhle.com/v1/edge/team-landing)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull the EDGE team landing page (summary across all teams).

### Example

```python
nhl_edge_team_landing()
```

_Last validated n/a._

## `nhl_edge_team_shot_location_detail`

Pull EDGE shot-location detail for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-shot-location-detail/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-shot-location-detail/10](https://api-web.nhle.com/v1/edge/team-shot-location-detail/10)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull EDGE shot-location detail for a single team.

### Example

```python
nhl_edge_team_shot_location_detail(team_id=10)
```

_Last validated n/a._

## `nhl_edge_team_shot_location_top_10`

Pull the EDGE top-10 teams for a shot-location category.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-shot-location-top-10/{position}/{category}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-shot-location-top-10/forwards/shots/points](https://api-web.nhle.com/v1/edge/team-shot-location-top-10/forwards/shots/points)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `position` | `position` |  | `Y` |  |
| `category` | `category` |  | `Y` |  |
| `sort_by` | `sort_by` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull the EDGE top-10 teams for a shot-location category.

### Example

```python
nhl_edge_team_shot_location_top_10(position='forwards', category='shots', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_team_shot_speed_detail`

Pull EDGE shot-speed detail for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-shot-speed-detail/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-shot-speed-detail/10](https://api-web.nhle.com/v1/edge/team-shot-speed-detail/10)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull EDGE shot-speed detail for a single team.

### Example

```python
nhl_edge_team_shot_speed_detail(team_id=10)
```

_Last validated n/a._

## `nhl_edge_team_skating_distance_detail`

Pull EDGE skating-distance detail for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-skating-distance-detail/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-skating-distance-detail/10](https://api-web.nhle.com/v1/edge/team-skating-distance-detail/10)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `positions` | `positions` |  | `Y` |  |
| `strength` | `strength` |  | `Y` |  |
| `sort_by` | `sort_by` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull the EDGE top-10 teams by skating distance.

### Example

```python
nhl_edge_team_skating_distance_top_10(positions='defense', strength='ev', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_team_skating_speed_detail`

Pull EDGE skating-speed detail for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-skating-speed-detail/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-skating-speed-detail/10](https://api-web.nhle.com/v1/edge/team-skating-speed-detail/10)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `positions` | `positions` |  | `Y` |  |
| `sort_by` | `sort_by` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull the EDGE top-10 teams by skating speed.

### Example

```python
nhl_edge_team_skating_speed_top_10(positions='defense', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_team_zone_time_details`

Pull EDGE zone-time details for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-zone-time-details/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-zone-time-details/10](https://api-web.nhle.com/v1/edge/team-zone-time-details/10)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull EDGE zone-time details for a single team.

### Example

```python
nhl_edge_team_zone_time_details(team_id=10)
```

_Last validated n/a._

## `nhl_edge_team_zone_time_top_10`

Pull the EDGE top-10 teams by zone time.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-zone-time-top-10/{strength}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-zone-time-top-10/ev/points](https://api-web.nhle.com/v1/edge/team-zone-time-top-10/ev/points)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `strength` | `strength` |  | `Y` |  |
| `sort_by` | `sort_by` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull the EDGE top-10 teams by zone time.

### Example

```python
nhl_edge_team_zone_time_top_10(strength='ev', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_cat_skater_detail`

Pull categorized (cat) EDGE detail stats for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/cat/edge/skater-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/cat/edge/skater-detail/8480801](https://api-web.nhle.com/v1/cat/edge/skater-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `player_id` | `player_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull categorized (cat) EDGE detail stats for a single skater.

### Example

```python
nhl_edge_cat_skater_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_cat_goalie_detail`

Pull categorized (cat) EDGE detail stats for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/cat/edge/goalie-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/cat/edge/goalie-detail/8480801](https://api-web.nhle.com/v1/cat/edge/goalie-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `player_id` | `player_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull categorized (cat) EDGE detail stats for a single goalie.

### Example

```python
nhl_edge_cat_goalie_detail(player_id=8480801)
```

_Last validated n/a._
