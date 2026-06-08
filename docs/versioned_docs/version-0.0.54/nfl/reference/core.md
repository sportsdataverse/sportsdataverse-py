---
title: NFL — ESPN core API (v2)
sidebar_label: ESPN core API (v2)
sidebar_position: 22
---
# NFL — ESPN core API (v2)

`sportsdataverse.nfl` — 84 endpoints.

## `espn_nfl_league_root`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_league_root()
```

_Last validated n/a._

## `espn_nfl_season_pointer`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/season`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/season](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/season)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_pointer()
```

_Last validated n/a._

## `espn_nfl_seasons`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_seasons()
```

_Last validated n/a._

## `espn_nfl_season_info`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_info(season=2024)
```

_Last validated n/a._

## `espn_nfl_season_types`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/types`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_types(season=2024)
```

_Last validated n/a._

## `espn_nfl_season_type`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/types/{season_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `season_type` | `season_type` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_type(season=2024, season_type=2)
```

_Last validated n/a._

## `espn_nfl_season_group`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/types/{season_type}/groups/{group_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2/groups/80](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2/groups/80)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `season_type` | `season_type` |  | `Y` |  |
| `group_id` | `group_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_group(season=2024, season_type=2, group_id=80)
```

_Last validated n/a._

## `espn_nfl_season_groups`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/types/{season_type}/groups`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2/groups](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2/groups)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `season_type` | `season_type` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_groups(season=2024, season_type=2)
```

_Last validated n/a._

## `espn_nfl_season_group_teams`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/types/{season_type}/groups/{group_id}/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2/groups/80/teams](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2/groups/80/teams)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `season_type` | `season_type` |  | `Y` |  |
| `group_id` | `group_id` |  | `Y` |  |
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_group_teams(season=2024, season_type=2, group_id=80)
```

_Last validated n/a._

## `espn_nfl_season_group_children`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/types/{season_type}/groups/{group_id}/children`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2/groups/80/children](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2/groups/80/children)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `season_type` | `season_type` |  | `Y` |  |
| `group_id` | `group_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_group_children(season=2024, season_type=2, group_id=80)
```

_Last validated n/a._

## `espn_nfl_season_type_leaders`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/types/{season_type}/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2/leaders](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2/leaders)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `season_type` | `season_type` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_type_leaders(season=2024, season_type=2)
```

_Last validated n/a._

## `espn_nfl_season_type_corrections`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/types/{season_type}/corrections`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2/corrections](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2/corrections)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `season_type` | `season_type` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_type_corrections(season=2024, season_type=2)
```

_Last validated n/a._

## `espn_nfl_season_weeks`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/types/{season_type}/weeks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2/weeks](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2/weeks)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `season_type` | `season_type` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_weeks(season=2024, season_type=2)
```

_Last validated n/a._

## `espn_nfl_season_week`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/types/{season_type}/weeks/{week}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2/weeks/1](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2/weeks/1)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `season_type` | `season_type` |  | `Y` |  |
| `week` | `week` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_week(season=2024, season_type=2, week=1)
```

_Last validated n/a._

## `espn_nfl_season_week_games`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/types/{season_type}/weeks/{week}/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2/weeks/1/events](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2/weeks/1/events)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `season_type` | `season_type` |  | `Y` |  |
| `week` | `week` |  | `Y` |  |
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_week_games(season=2024, season_type=2, week=1)
```

_Last validated n/a._

## `espn_nfl_season_teams`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/teams](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/teams)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_teams(season=2024)
```

_Last validated n/a._

## `espn_nfl_season_team`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/teams/4](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/teams/4)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `team_id` | `team_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_team(season=2024, team_id='4')
```

_Last validated n/a._

## `espn_nfl_season_players`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/athletes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/athletes](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/athletes)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `limit` | `limit` |  |  | `Y` |
| `page` | `page` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_players(season=2024)
```

_Last validated n/a._

## `espn_nfl_season_coaches`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/coaches`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/coaches](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/coaches)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_coaches(season=2024)
```

_Last validated n/a._

## `espn_nfl_season_draft`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/draft`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/draft](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/draft)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_draft(season=2024)
```

_Last validated n/a._

## `espn_nfl_season_draft_round_picks`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/draft/rounds/{round_num}/picks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/draft/rounds/1/picks](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/draft/rounds/1/picks)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `round_num` | `round_num` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_draft_round_picks(season=2024, round_num='1')
```

_Last validated n/a._

## `espn_nfl_season_futures`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/futures`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/futures](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/futures)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_futures(season=2024)
```

_Last validated n/a._

## `espn_nfl_season_freeagents`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/freeagents`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/freeagents](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/freeagents)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_freeagents(season=2024)
```

_Last validated n/a._

## `espn_nfl_season_powerindex`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/powerindex[/{team_id}]`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/powerindex](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/powerindex)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `team_id` | `team_id` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_powerindex(season=2024)
```

_Last validated n/a._

## `espn_nfl_season_powerindex_leaders`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/powerindex/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/powerindex/leaders](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/powerindex/leaders)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_powerindex_leaders(season=2024)
```

_Last validated n/a._

## `espn_nfl_season_awards`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/awards](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/awards)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_awards(season=2024)
```

_Last validated n/a._

## `espn_nfl_players_index`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `active` | `active` |  |  | `Y` |
| `limit` | `limit` |  |  | `Y` |
| `page` | `page` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_players_index()
```

_Last validated n/a._

## `espn_nfl_player_core`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/{athlete_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4239](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4239)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_player_core(athlete_id='4239')
```

_Last validated n/a._

## `espn_nfl_player_career_stats`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/{athlete_id}/statistics[/{stat_type}]`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4239/statistics](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4239/statistics)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |
| `stat_type` | `stat_type` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_player_career_stats(athlete_id='4239')
```

_Last validated n/a._

## `espn_nfl_player_statisticslog`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/{athlete_id}/statisticslog`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4239/statisticslog](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4239/statisticslog)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_player_statisticslog(athlete_id='4239')
```

_Last validated n/a._

## `espn_nfl_player_eventlog`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/{athlete_id}/eventlog`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4239/eventlog](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4239/eventlog)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_player_eventlog(athlete_id='4239')
```

_Last validated n/a._

## `espn_nfl_player_contracts`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/{athlete_id}/contracts`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4239/contracts](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4239/contracts)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_player_contracts(athlete_id='4239')
```

_Last validated n/a._

## `espn_nfl_player_awards`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/{athlete_id}/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4239/awards](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4239/awards)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_player_awards(athlete_id='4239')
```

_Last validated n/a._

## `espn_nfl_player_seasons`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/{athlete_id}/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4239/seasons](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4239/seasons)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_player_seasons(athlete_id='4239')
```

_Last validated n/a._

## `espn_nfl_player_records`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/{athlete_id}/records`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4239/records](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4239/records)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_player_records(athlete_id='4239')
```

_Last validated n/a._

## `espn_nfl_player_injuries`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/{athlete_id}/injuries`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4239/injuries](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4239/injuries)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_player_injuries(athlete_id='4239')
```

_Last validated n/a._

## `espn_nfl_player_notes`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/{athlete_id}/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4239/notes](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4239/notes)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_player_notes(athlete_id='4239')
```

_Last validated n/a._

## `espn_nfl_player_vs_player`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/{athlete_id}/vsathlete/{opp_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4239/vsathlete/5](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes/4239/vsathlete/5)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |
| `opp_id` | `opp_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_player_vs_player(athlete_id='4239', opp_id='5')
```

_Last validated n/a._

## `espn_nfl_games`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `dates` | `dates` |  |  | `Y` |
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_games()
```

_Last validated n/a._

## `espn_nfl_game`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game(event_id='401584793')
```

_Last validated n/a._

## `espn_nfl_game_competition`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}/competitions/{cid}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game_competition(event_id='401584793')
```

_Last validated n/a._

## `espn_nfl_game_teams`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}/competitions/{cid}/competitors`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game_teams(event_id='401584793')
```

_Last validated n/a._

## `espn_nfl_game_team`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}/competitions/{cid}/competitors/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `team_id` | `team_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game_team(event_id='401584793', team_id='4')
```

_Last validated n/a._

## `espn_nfl_game_team_roster`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}/competitions/{cid}/competitors/{team_id}/roster`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `team_id` | `team_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game_team_roster(event_id='401584793', team_id='4')
```

_Last validated n/a._

## `espn_nfl_game_team_linescores`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}/competitions/{cid}/competitors/{team_id}/linescores`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `team_id` | `team_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game_team_linescores(event_id='401584793', team_id='4')
```

_Last validated n/a._

## `espn_nfl_game_team_statistics`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}/competitions/{cid}/competitors/{team_id}/statistics`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `team_id` | `team_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game_team_statistics(event_id='401584793', team_id='4')
```

_Last validated n/a._

## `espn_nfl_game_team_record`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}/competitions/{cid}/competitors/{team_id}/record`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `team_id` | `team_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game_team_record(event_id='401584793', team_id='4')
```

_Last validated n/a._

## `espn_nfl_game_team_leaders`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}/competitions/{cid}/competitors/{team_id}/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `team_id` | `team_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game_team_leaders(event_id='401584793', team_id='4')
```

_Last validated n/a._

## `espn_nfl_game_odds`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}/competitions/{cid}/odds`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game_odds(event_id='401584793')
```

_Last validated n/a._

## `espn_nfl_game_probabilities`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}/competitions/{cid}/probabilities`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game_probabilities(event_id='401584793')
```

_Last validated n/a._

## `espn_nfl_game_plays`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}/competitions/{cid}/plays`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game_plays(event_id='401584793')
```

_Last validated n/a._

## `espn_nfl_game_play`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}/competitions/{cid}/plays/{play_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `play_id` | `play_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game_play(event_id='401584793', play_id='1')
```

_Last validated n/a._

## `espn_nfl_game_play_personnel`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}/competitions/{cid}/plays/{play_id}/personnel`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `play_id` | `play_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game_play_personnel(event_id='401584793', play_id='1')
```

_Last validated n/a._

## `espn_nfl_game_situation`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}/competitions/{cid}/situation`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game_situation(event_id='401584793')
```

_Last validated n/a._

## `espn_nfl_game_status`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}/competitions/{cid}/status`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game_status(event_id='401584793')
```

_Last validated n/a._

## `espn_nfl_game_officials`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}/competitions/{cid}/officials`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game_officials(event_id='401584793')
```

_Last validated n/a._

## `espn_nfl_game_broadcasts`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}/competitions/{cid}/broadcasts`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game_broadcasts(event_id='401584793')
```

_Last validated n/a._

## `espn_nfl_game_predictor`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}/competitions/{cid}/predictor`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game_predictor(event_id='401584793')
```

_Last validated n/a._

## `espn_nfl_game_powerindex`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}/competitions/{cid}/powerindex`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game_powerindex(event_id='401584793')
```

_Last validated n/a._

## `espn_nfl_game_propbets`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}/competitions/{cid}/propbets`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game_propbets(event_id='401584793')
```

_Last validated n/a._

## `espn_nfl_game_leaders`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}/competitions/{cid}/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game_leaders(event_id='401584793')
```

_Last validated n/a._

## `espn_nfl_game_scoringplays`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}/competitions/{cid}/scoringplays`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game_scoringplays(event_id='401584793')
```

_Last validated n/a._

## `espn_nfl_game_official_detail`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{event_id}/competitions/{cid}/officials/{official_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `official_id` | `official_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_game_official_detail(event_id='401584793', official_id='1')
```

_Last validated n/a._

## `espn_nfl_teams_core`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/teams](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/teams)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `limit` | `limit` |  |  | `Y` |

### Returns

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | character | Short team abbreviation (e.g. "BOS"). |
| `team_alternate_color` | character | Secondary team color as a hex string (no leading '#'). |
| `team_color` | character | Primary team color as a hex string (no leading '#'). |
| `team_display_name` | character | Full team display name (location + nickname). |
| `team_id` | character | ESPN team id (stable join key across ESPN endpoints). |
| `team_is_active` | logical | Whether the team is currently active. |
| `team_is_all_star` | logical | Whether the entry is an all-star squad rather than a franchise. |
| `team_location` | character | Team location / city (e.g. "Boston"). |
| `team_logos` | character | Pipe-delimited logo image URLs. |
| `team_name` | character | Team nickname/mascot (e.g. "Celtics"). |
| `team_nickname` | character | Team nickname as ESPN labels it (often equals team_name). |
| `team_short_display_name` | character | Abbreviated display name for compact UIs. |
| `team_slug` | character | URL slug used in ESPN web paths. |
| `team_uid` | character | ESPN global UID (encodes sport/league/team). |

### Example

```python
espn_nfl_teams_core()
```

_Last validated n/a._

## `espn_nfl_team_core`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/teams/4](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/teams/4)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_team_core(team_id='4')
```

_Last validated n/a._

## `espn_nfl_venues`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/venues`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/venues](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/venues)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_venues()
```

_Last validated n/a._

## `espn_nfl_venue`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/venues/{venue_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/venues/3663](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/venues/3663)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `venue_id` | `venue_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_venue(venue_id='3663')
```

_Last validated n/a._

## `espn_nfl_franchises`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/franchises`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/franchises](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/franchises)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_franchises()
```

_Last validated n/a._

## `espn_nfl_franchise`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/franchises/{franchise_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/franchises/2](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/franchises/2)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `franchise_id` | `franchise_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_franchise(franchise_id='2')
```

_Last validated n/a._

## `espn_nfl_coaches`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/coaches`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/coaches](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/coaches)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_coaches()
```

_Last validated n/a._

## `espn_nfl_coach`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/coaches/{coach_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/coaches/1](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/coaches/1)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `coach_id` | `coach_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_coach(coach_id='1')
```

_Last validated n/a._

## `espn_nfl_coach_record`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/coaches/{coach_id}/record/{record_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/coaches/1/record](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/coaches/1/record)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `coach_id` | `coach_id` |  | `Y` |  |
| `record_type` | `record_type` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_coach_record(coach_id='1')
```

_Last validated n/a._

## `espn_nfl_coach_season`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/coaches/{coach_id}/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/coaches/1/seasons/2024](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/coaches/1/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `coach_id` | `coach_id` |  | `Y` |  |
| `season` | `season` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_coach_season(coach_id='1', season=2024)
```

_Last validated n/a._

## `espn_nfl_positions`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/positions`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/positions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/positions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_positions()
```

_Last validated n/a._

## `espn_nfl_position`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/positions/{position_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/positions/1](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/positions/1)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `position_id` | `position_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_position(position_id='1')
```

_Last validated n/a._

## `espn_nfl_tournaments`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/tournaments`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/tournaments](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/tournaments)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_tournaments()
```

_Last validated n/a._

## `espn_nfl_awards`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/awards](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/awards)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_awards()
```

_Last validated n/a._

## `espn_nfl_award`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/awards/{award_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/awards/1](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/awards/1)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `award_id` | `award_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_award(award_id='1')
```

_Last validated n/a._

## `espn_nfl_standings_core`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/standings`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/standings](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/standings)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

| col_name | type | description |
|---|---|---|
| `group_name` | character | Group name. |
| `group_abbreviation` | character | Group abbreviation. |
| `team_id` | character | Team id. |
| `team_name` | character | Team name. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Team display name. |
| `team_location` | character | Team location. |
| `team_logo` | character | Team logo. |
| `clincher` | double | Clincher. |
| `differential` | double | Differential. |
| `games_behind` | double | Games behind. |
| `losses` | double | Losses. |
| `playoff_seed` | double | Playoff seed. |
| `point_differential` | double | Point differential. |
| `points_against` | double | Points against. |
| `points_for` | double | Points for. |
| `streak` | double | Streak. |
| `ties` | double |  |
| `win_percent` | double | Win percent. |
| `wins` | double | Wins. |
| `division_losses` | double |  |
| `division_record` | double |  |
| `division_ties` | double |  |
| `division_wins` | double |  |
| `overall` | character | Overall. |
| `home` | character | Home. |
| `road` | character | Road. |
| `vs. div.` | character | Vs. div.. |
| `vs. conf.` | character | Vs. conf.. |

### Example

```python
espn_nfl_standings_core()
```

_Last validated n/a._

## `espn_nfl_leaders_core`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/leaders](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/leaders)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_leaders_core()
```

_Last validated n/a._

## `espn_nfl_league_notes`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/notes](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/notes)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_league_notes()
```

_Last validated n/a._

## `espn_nfl_talentpicks`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/talentpicks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/talentpicks](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/talentpicks)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_talentpicks()
```

_Last validated n/a._

## `espn_nfl_season_qbr`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/types/{season_type}[/groups/{group_id}]/qbr/{split}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `season_type` | `season_type` |  |  | `Y` |
| `group_id` | `group_id` |  |  | `Y` |
| `split` | `split` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_qbr(season=2024)
```

_Last validated n/a._

## `espn_nfl_season_qbr_week`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{season}/types/{season_type}/weeks/{week}/qbr/{split}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `week` | `week` |  | `Y` |  |
| `season_type` | `season_type` |  |  | `Y` |
| `split` | `split` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nfl_season_qbr_week(season=2024, week=1)
```

_Last validated n/a._
