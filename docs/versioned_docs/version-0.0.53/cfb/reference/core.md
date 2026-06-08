---
title: CFB — ESPN core API (v2)
sidebar_label: ESPN core API (v2)
sidebar_position: 22
---
# CFB — ESPN core API (v2)

`sportsdataverse.cfb` — 86 endpoints.

## `espn_cfb_league_root`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_league_root()
```

_Last validated n/a._

## `espn_cfb_season_pointer`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/season`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/season](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/season)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_season_pointer()
```

_Last validated n/a._

## `espn_cfb_seasons`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_seasons()
```

_Last validated n/a._

## `espn_cfb_season_info`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_season_info(season=2024)
```

_Last validated n/a._

## `espn_cfb_season_types`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/types`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_season_types(season=2024)
```

_Last validated n/a._

## `espn_cfb_season_type`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/types/{season_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `season_type` | `season_type` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_season_type(season=2024, season_type=2)
```

_Last validated n/a._

## `espn_cfb_season_group`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/types/{season_type}/groups/{group_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/groups/80](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/groups/80)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `season_type` | `season_type` |  | `Y` |  |
| `group_id` | `group_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_season_group(season=2024, season_type=2, group_id=80)
```

_Last validated n/a._

## `espn_cfb_groups`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/types/{season_type}/groups`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/groups](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/groups)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `season_type` | `season_type` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_groups(season=2024, season_type=2)
```

_Last validated n/a._

## `espn_cfb_season_group_teams`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/types/{season_type}/groups/{group_id}/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/groups/80/teams](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/groups/80/teams)

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
espn_cfb_season_group_teams(season=2024, season_type=2, group_id=80)
```

_Last validated n/a._

## `espn_cfb_season_group_children`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/types/{season_type}/groups/{group_id}/children`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/groups/80/children](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/groups/80/children)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `season_type` | `season_type` |  | `Y` |  |
| `group_id` | `group_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_season_group_children(season=2024, season_type=2, group_id=80)
```

_Last validated n/a._

## `espn_cfb_season_type_leaders`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/types/{season_type}/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/leaders](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/leaders)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `season_type` | `season_type` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_season_type_leaders(season=2024, season_type=2)
```

_Last validated n/a._

## `espn_cfb_season_type_corrections`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/types/{season_type}/corrections`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/corrections](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/corrections)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `season_type` | `season_type` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_season_type_corrections(season=2024, season_type=2)
```

_Last validated n/a._

## `espn_cfb_season_weeks`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/types/{season_type}/weeks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/weeks](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/weeks)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `season_type` | `season_type` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_season_weeks(season=2024, season_type=2)
```

_Last validated n/a._

## `espn_cfb_season_week`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/types/{season_type}/weeks/{week}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/weeks/1](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/weeks/1)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `season_type` | `season_type` |  | `Y` |  |
| `week` | `week` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_season_week(season=2024, season_type=2, week=1)
```

_Last validated n/a._

## `espn_cfb_season_week_games`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/types/{season_type}/weeks/{week}/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/weeks/1/events](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/weeks/1/events)

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
espn_cfb_season_week_games(season=2024, season_type=2, week=1)
```

_Last validated n/a._

## `espn_cfb_season_teams`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/teams](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/teams)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_season_teams(season=2024)
```

_Last validated n/a._

## `espn_cfb_season_team`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/teams/4](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/teams/4)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `team_id` | `team_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_season_team(season=2024, team_id='4')
```

_Last validated n/a._

## `espn_cfb_season_players`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/athletes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/athletes](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/athletes)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `limit` | `limit` |  |  | `Y` |
| `page` | `page` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_season_players(season=2024)
```

_Last validated n/a._

## `espn_cfb_season_coaches`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/coaches`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/coaches](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/coaches)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_season_coaches(season=2024)
```

_Last validated n/a._

## `espn_cfb_season_draft`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/draft`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/draft](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/draft)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_season_draft(season=2024)
```

_Last validated n/a._

## `espn_cfb_season_draft_round_picks`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/draft/rounds/{round_num}/picks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/draft/rounds/1/picks](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/draft/rounds/1/picks)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `round_num` | `round_num` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_season_draft_round_picks(season=2024, round_num='1')
```

_Last validated n/a._

## `espn_cfb_futures`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/futures`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/futures](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/futures)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_futures(season=2024)
```

_Last validated n/a._

## `espn_cfb_season_freeagents`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/freeagents`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/freeagents](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/freeagents)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_season_freeagents(season=2024)
```

_Last validated n/a._

## `espn_cfb_team_powerindex`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/powerindex[/{team_id}]`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/powerindex](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/powerindex)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `team_id` | `team_id` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_team_powerindex(season=2024)
```

_Last validated n/a._

## `espn_cfb_season_powerindex_leaders`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/powerindex/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/powerindex/leaders](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/powerindex/leaders)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_season_powerindex_leaders(season=2024)
```

_Last validated n/a._

## `espn_cfb_season_awards`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/awards](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/awards)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_season_awards(season=2024)
```

_Last validated n/a._

## `espn_cfb_players_index`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `active` | `active` |  |  | `Y` |
| `limit` | `limit` |  |  | `Y` |
| `page` | `page` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_players_index()
```

_Last validated n/a._

## `espn_cfb_player_core`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/{athlete_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_player_core(athlete_id='4239')
```

_Last validated n/a._

## `espn_cfb_player_career_stats`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/{athlete_id}/statistics[/{stat_type}]`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/statistics](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/statistics)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |
| `stat_type` | `stat_type` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_player_career_stats(athlete_id='4239')
```

_Last validated n/a._

## `espn_cfb_player_statisticslog`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/{athlete_id}/statisticslog`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/statisticslog](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/statisticslog)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_player_statisticslog(athlete_id='4239')
```

_Last validated n/a._

## `espn_cfb_player_eventlog`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/{athlete_id}/eventlog`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/eventlog](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/eventlog)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_player_eventlog(athlete_id='4239')
```

_Last validated n/a._

## `espn_cfb_player_contracts`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/{athlete_id}/contracts`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/contracts](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/contracts)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_player_contracts(athlete_id='4239')
```

_Last validated n/a._

## `espn_cfb_player_awards`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/{athlete_id}/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/awards](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/awards)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_player_awards(athlete_id='4239')
```

_Last validated n/a._

## `espn_cfb_player_seasons`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/{athlete_id}/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/seasons](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/seasons)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_player_seasons(athlete_id='4239')
```

_Last validated n/a._

## `espn_cfb_player_records`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/{athlete_id}/records`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/records](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/records)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_player_records(athlete_id='4239')
```

_Last validated n/a._

## `espn_cfb_player_injuries`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/{athlete_id}/injuries`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/injuries](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/injuries)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_player_injuries(athlete_id='4239')
```

_Last validated n/a._

## `espn_cfb_player_notes`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/{athlete_id}/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/notes](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/notes)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_player_notes(athlete_id='4239')
```

_Last validated n/a._

## `espn_cfb_player_vs_player`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/{athlete_id}/vsathlete/{opp_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/vsathlete/5](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/athletes/4239/vsathlete/5)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |
| `opp_id` | `opp_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_player_vs_player(athlete_id='4239', opp_id='5')
```

_Last validated n/a._

## `espn_cfb_games`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `dates` | `dates` |  |  | `Y` |
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_games()
```

_Last validated n/a._

## `espn_cfb_game`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game(event_id='401584793')
```

_Last validated n/a._

## `espn_cfb_game_competition`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{cid}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game_competition(event_id='401584793')
```

_Last validated n/a._

## `espn_cfb_game_teams`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{cid}/competitors`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game_teams(event_id='401584793')
```

_Last validated n/a._

## `espn_cfb_game_team`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{cid}/competitors/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `team_id` | `team_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game_team(event_id='401584793', team_id='4')
```

_Last validated n/a._

## `espn_cfb_game_team_roster`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{cid}/competitors/{team_id}/roster`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `team_id` | `team_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game_team_roster(event_id='401584793', team_id='4')
```

_Last validated n/a._

## `espn_cfb_game_team_linescores`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{cid}/competitors/{team_id}/linescores`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `team_id` | `team_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game_team_linescores(event_id='401584793', team_id='4')
```

_Last validated n/a._

## `espn_cfb_game_team_statistics`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{cid}/competitors/{team_id}/statistics`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `team_id` | `team_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game_team_statistics(event_id='401584793', team_id='4')
```

_Last validated n/a._

## `espn_cfb_game_team_record`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{cid}/competitors/{team_id}/record`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `team_id` | `team_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game_team_record(event_id='401584793', team_id='4')
```

_Last validated n/a._

## `espn_cfb_game_team_leaders`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{cid}/competitors/{team_id}/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `team_id` | `team_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game_team_leaders(event_id='401584793', team_id='4')
```

_Last validated n/a._

## `espn_cfb_game_odds`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{cid}/odds`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game_odds(event_id='401584793')
```

_Last validated n/a._

## `espn_cfb_game_probabilities`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{cid}/probabilities`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game_probabilities(event_id='401584793')
```

_Last validated n/a._

## `espn_cfb_game_plays`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{cid}/plays`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game_plays(event_id='401584793')
```

_Last validated n/a._

## `espn_cfb_game_play`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{cid}/plays/{play_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `play_id` | `play_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game_play(event_id='401584793', play_id='1')
```

_Last validated n/a._

## `espn_cfb_game_play_personnel`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{cid}/plays/{play_id}/personnel`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `play_id` | `play_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game_play_personnel(event_id='401584793', play_id='1')
```

_Last validated n/a._

## `espn_cfb_game_situation`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{cid}/situation`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game_situation(event_id='401584793')
```

_Last validated n/a._

## `espn_cfb_game_status`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{cid}/status`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game_status(event_id='401584793')
```

_Last validated n/a._

## `espn_cfb_game_officials`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{cid}/officials`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game_officials(event_id='401584793')
```

_Last validated n/a._

## `espn_cfb_game_broadcasts`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{cid}/broadcasts`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game_broadcasts(event_id='401584793')
```

_Last validated n/a._

## `espn_cfb_game_predictor`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{cid}/predictor`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game_predictor(event_id='401584793')
```

_Last validated n/a._

## `espn_cfb_game_powerindex`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{cid}/powerindex`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game_powerindex(event_id='401584793')
```

_Last validated n/a._

## `espn_cfb_game_propbets`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{cid}/propbets`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game_propbets(event_id='401584793')
```

_Last validated n/a._

## `espn_cfb_game_leaders`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{cid}/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game_leaders(event_id='401584793')
```

_Last validated n/a._

## `espn_cfb_game_scoringplays`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{cid}/scoringplays`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game_scoringplays(event_id='401584793')
```

_Last validated n/a._

## `espn_cfb_game_official_detail`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{cid}/officials/{official_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/401584793/competitions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event_id` | `event_id` |  | `Y` |  |
| `official_id` | `official_id` |  | `Y` |  |
| `cid` | `cid` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_game_official_detail(event_id='401584793', official_id='1')
```

_Last validated n/a._

## `espn_cfb_teams_core`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/teams](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/teams)

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
espn_cfb_teams_core()
```

_Last validated n/a._

## `espn_cfb_team_core`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/teams/4](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/teams/4)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_team_core(team_id='4')
```

_Last validated n/a._

## `espn_cfb_venues`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/venues`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/venues](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/venues)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_venues()
```

_Last validated n/a._

## `espn_cfb_venue`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/venues/{venue_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/venues/3663](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/venues/3663)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `venue_id` | `venue_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_venue(venue_id='3663')
```

_Last validated n/a._

## `espn_cfb_franchises`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/franchises`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/franchises](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/franchises)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_franchises()
```

_Last validated n/a._

## `espn_cfb_franchise`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/franchises/{franchise_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/franchises/2](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/franchises/2)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `franchise_id` | `franchise_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_franchise(franchise_id='2')
```

_Last validated n/a._

## `espn_cfb_coaches`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/coaches`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/coaches](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/coaches)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_coaches()
```

_Last validated n/a._

## `espn_cfb_coach`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/coaches/{coach_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/coaches/1](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/coaches/1)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `coach_id` | `coach_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_coach(coach_id='1')
```

_Last validated n/a._

## `espn_cfb_coach_record`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/coaches/{coach_id}/record/{record_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/coaches/1/record](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/coaches/1/record)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `coach_id` | `coach_id` |  | `Y` |  |
| `record_type` | `record_type` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_coach_record(coach_id='1')
```

_Last validated n/a._

## `espn_cfb_coach_season`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/coaches/{coach_id}/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/coaches/1/seasons/2024](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/coaches/1/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `coach_id` | `coach_id` |  | `Y` |  |
| `season` | `season` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_coach_season(coach_id='1', season=2024)
```

_Last validated n/a._

## `espn_cfb_positions`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/positions`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/positions](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/positions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_positions()
```

_Last validated n/a._

## `espn_cfb_position`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/positions/{position_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/positions/1](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/positions/1)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `position_id` | `position_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_position(position_id='1')
```

_Last validated n/a._

## `espn_cfb_tournaments`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/tournaments`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/tournaments](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/tournaments)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_tournaments()
```

_Last validated n/a._

## `espn_cfb_awards`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/awards](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/awards)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_awards()
```

_Last validated n/a._

## `espn_cfb_award`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/awards/{award_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/awards/1](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/awards/1)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `award_id` | `award_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_award(award_id='1')
```

_Last validated n/a._

## `espn_cfb_standings_core`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/standings`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/standings](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/standings)

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
| `avg_points_against` | double | Avg points against. |
| `avg_points_for` | double | Avg points for. |
| `clincher` | double | Clincher. |
| `differential` | double | Differential. |
| `division_win_percent` | double | Division win percent. |
| `games_behind` | double | Games behind. |
| `league_win_percent` | double | League win percent. |
| `losses` | double | Losses. |
| `playoff_seed` | double | Playoff seed. |
| `point_differential` | double | Point differential. |
| `points` | double | Points. |
| `points_against` | double | Points against. |
| `points_for` | double | Points for. |
| `streak` | double | Streak. |
| `win_percent` | double | Win percent. |
| `wins` | double | Wins. |
| `games_ahead` | double | Games ahead. |
| `overall` | character | Overall. |
| `home` | character | Home. |
| `road` | character | Road. |
| `vs. div.` | character | Vs. div.. |
| `vs. conf.` | character | Vs. conf.. |
| `last ten games` | character | Last ten games. |

### Example

```python
espn_cfb_standings_core()
```

_Last validated n/a._

## `espn_cfb_leaders_core`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/leaders](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/leaders)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_leaders_core()
```

_Last validated n/a._

## `espn_cfb_league_notes`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/notes](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/notes)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_league_notes()
```

_Last validated n/a._

## `espn_cfb_talentpicks`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/talentpicks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/talentpicks](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/talentpicks)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_talentpicks()
```

_Last validated n/a._

## `espn_cfb_recruits`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/recruits`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/recruits](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/recruits)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_recruits(season=2024)
```

_Last validated n/a._

## `espn_cfb_week_rankings`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/types/{season_type}/weeks/{week}/rankings`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/weeks/1/rankings](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types/2/weeks/1/rankings)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `season_type` | `season_type` |  | `Y` |  |
| `week` | `week` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_week_rankings(season=2024, season_type=2, week=1)
```

_Last validated n/a._

## `espn_cfb_season_qbr`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/types/{season_type}[/groups/{group_id}]/qbr/{split}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types)

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
espn_cfb_season_qbr(season=2024)
```

_Last validated n/a._

## `espn_cfb_season_qbr_week`

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/types/{season_type}/weeks/{week}/qbr/{split}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types](https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/2024/types)

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
espn_cfb_season_qbr_week(season=2024, week=1)
```

_Last validated n/a._
