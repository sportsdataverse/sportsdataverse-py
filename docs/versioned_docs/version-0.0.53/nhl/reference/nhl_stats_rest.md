---
title: NHL — nhl stats rest
sidebar_label: nhl stats rest
---
# NHL — nhl stats rest

`sportsdataverse.nhl` — 21 endpoints.

## `nhl_stats_rest_ping`

Ping the NHL Stats REST API database.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/ping`

**Valid URL:** [https://api.nhle.com/stats/rest/ping](https://api.nhle.com/stats/rest/ping)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `lang` | `lang` |  |  | `Y` |

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `lang` | `lang` |  |  | `Y` |

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `template_key` | `template_key` |  | `Y` |  |
| `lang` | `lang` |  |  | `Y` |

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `lang` | `lang` |  |  | `Y` |

### Returns

Retrieve the list of countries used in NHL data.

### Example

```python
nhl_stats_rest_country()
```

_Last validated n/a._

## `nhl_stats_rest_draft`

Retrieve draft data, optionally filtered with Cayenne expressions.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/draft`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `lang` | `lang` |  |  | `Y` |

### Returns

Retrieve draft data, optionally filtered with Cayenne expressions.

### Example

```python
nhl_stats_rest_draft()
```

_Last validated n/a._

## `nhl_stats_rest_franchise`

Retrieve franchise data.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/franchise`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `lang` | `lang` |  |  | `Y` |

### Returns

Retrieve franchise data.

### Example

```python
nhl_stats_rest_franchise()
```

_Last validated n/a._

## `nhl_stats_rest_game`

Retrieve game-level data.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/game`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `lang` | `lang` |  |  | `Y` |

### Returns

Retrieve game-level data.

### Example

```python
nhl_stats_rest_game()
```

_Last validated n/a._

## `nhl_stats_rest_glossary`

Retrieve the NHL Stats glossary of stat definitions.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/glossary`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `lang` | `lang` |  |  | `Y` |

### Returns

Retrieve the NHL Stats glossary of stat definitions.

### Example

```python
nhl_stats_rest_glossary()
```

_Last validated n/a._

## `nhl_stats_rest_goalie_report`

Retrieve a goalie statistical report.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/goalie/{report}`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `report` | `report` |  | `Y` |  |
| `lang` | `lang` |  |  | `Y` |

### Returns

Retrieve a goalie statistical report.

### Example

```python
nhl_stats_rest_goalie_report(report='summary')
```

_Last validated n/a._

## `nhl_stats_rest_leaders_goalies`

Retrieve league leaders for a goalie statistical attribute.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/leaders/goalies/{attribute}`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `attribute` | `attribute` |  | `Y` |  |
| `lang` | `lang` |  |  | `Y` |

### Returns

Retrieve league leaders for a goalie statistical attribute.

### Example

```python
nhl_stats_rest_leaders_goalies(attribute='X')
```

_Last validated n/a._

## `nhl_stats_rest_leaders_skaters`

Retrieve league leaders for a skater statistical attribute.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/leaders/skaters/{attribute}`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `attribute` | `attribute` |  | `Y` |  |
| `lang` | `lang` |  |  | `Y` |

### Returns

Retrieve league leaders for a skater statistical attribute.

### Example

```python
nhl_stats_rest_leaders_skaters(attribute='X')
```

_Last validated n/a._

## `nhl_stats_rest_milestones_goalies`

Retrieve milestone data for goalies.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/milestones/goalies`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `lang` | `lang` |  |  | `Y` |

### Returns

Retrieve milestone data for goalies.

### Example

```python
nhl_stats_rest_milestones_goalies()
```

_Last validated n/a._

## `nhl_stats_rest_milestones_skaters`

Retrieve milestone data for skaters.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/milestones/skaters`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `lang` | `lang` |  |  | `Y` |

### Returns

Retrieve milestone data for skaters.

### Example

```python
nhl_stats_rest_milestones_skaters()
```

_Last validated n/a._

## `nhl_stats_rest_players`

Retrieve the NHL player registry.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/players`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `lang` | `lang` |  |  | `Y` |

### Returns

Retrieve the NHL player registry.

### Example

```python
nhl_stats_rest_players()
```

_Last validated n/a._

## `nhl_stats_rest_season`

Retrieve the list of all NHL seasons.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/season`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `lang` | `lang` |  |  | `Y` |

### Returns

Retrieve the list of all NHL seasons.

### Example

```python
nhl_stats_rest_season()
```

_Last validated n/a._

## `nhl_stats_rest_shiftcharts`

Retrieve shift-chart data.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/shiftcharts`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `lang` | `lang` |  |  | `Y` |

### Returns

Retrieve shift-chart data.

### Example

```python
nhl_stats_rest_shiftcharts()
```

_Last validated n/a._

## `nhl_stats_rest_skater_report`

Retrieve a skater statistical report.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/skater/{report}`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `report` | `report` |  | `Y` |  |
| `lang` | `lang` |  |  | `Y` |

### Returns

Retrieve a skater statistical report.

### Example

```python
nhl_stats_rest_skater_report(report='summary')
```

_Last validated n/a._

## `nhl_stats_rest_team`

Retrieve the list of all NHL teams.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/team`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `lang` | `lang` |  |  | `Y` |

### Returns

Retrieve the list of all NHL teams.

### Example

```python
nhl_stats_rest_team()
```

_Last validated n/a._

## `nhl_stats_rest_team_by_id`

Retrieve a single team by its numeric ID.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/team/id/{team_id}`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |
| `lang` | `lang` |  |  | `Y` |

### Returns

Retrieve a single team by its numeric ID.

### Example

```python
nhl_stats_rest_team_by_id(team_id=10)
```

_Last validated n/a._

## `nhl_stats_rest_team_report`

Retrieve a team statistical report.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/team/{report}`

**Valid URL:** [https://api.nhle.com/stats/rest](https://api.nhle.com/stats/rest)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `report` | `report` |  | `Y` |  |
| `lang` | `lang` |  |  | `Y` |

### Returns

Retrieve a team statistical report.

### Example

```python
nhl_stats_rest_team_report(report='summary')
```

_Last validated n/a._
