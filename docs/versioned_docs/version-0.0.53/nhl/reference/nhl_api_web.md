---
title: NHL — NHL Web API
sidebar_label: NHL Web API
sidebar_position: 10
---
# NHL — NHL Web API

`sportsdataverse.nhl` — 27 endpoints.

## `nhl_web_pbp`

Pull the play-by-play feed for one NHL game.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play`

**Valid URL:** [https://api-web.nhle.com/v1/gamecenter/2024020001/play-by-play](https://api-web.nhle.com/v1/gamecenter/2024020001/play-by-play)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_id` | `game_id` |  | `Y` |  |

### Returns

Pull the play-by-play feed for one NHL game.

### Example

```python
nhl_web_pbp(game_id=2024020001)
```

_Last validated n/a._

## `nhl_boxscore`

Pull the boxscore for one NHL game.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore`

**Valid URL:** [https://api-web.nhle.com/v1/gamecenter/2024020001/boxscore](https://api-web.nhle.com/v1/gamecenter/2024020001/boxscore)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_id` | `game_id` |  | `Y` |  |

### Returns

Pull the boxscore for one NHL game.

### Example

```python
nhl_boxscore(game_id=2024020001)
```

_Last validated n/a._

## `nhl_landing`

Pull the gamecenter landing payload for one NHL game.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/gamecenter/{game_id}/landing`

**Valid URL:** [https://api-web.nhle.com/v1/gamecenter/2024020001/landing](https://api-web.nhle.com/v1/gamecenter/2024020001/landing)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_id` | `game_id` |  | `Y` |  |

### Returns

Pull the gamecenter landing payload for one NHL game.

### Example

```python
nhl_landing(game_id=2024020001)
```

_Last validated n/a._

## `nhl_right_rail`

Pull the gamecenter right-rail payload (in-game widgets).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/gamecenter/{game_id}/right-rail`

**Valid URL:** [https://api-web.nhle.com/v1/gamecenter/2024020001/right-rail](https://api-web.nhle.com/v1/gamecenter/2024020001/right-rail)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_id` | `game_id` |  | `Y` |  |

### Returns

Pull the gamecenter right-rail payload (in-game widgets).

### Example

```python
nhl_right_rail(game_id=2024020001)
```

_Last validated n/a._

## `nhl_web_schedule`

Pull the week-of NHL schedule rooted at ``date``.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/schedule/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/schedule](https://api-web.nhle.com/v1/schedule)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `date` | `date` |  |  | `Y` |

### Returns

Pull the week-of NHL schedule rooted at ``date``.

### Example

```python
nhl_web_schedule()
```

_Last validated n/a._

## `nhl_score`

Pull the single-day scoreboard for ``date``.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/score/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/score](https://api-web.nhle.com/v1/score)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `date` | `date` |  |  | `Y` |

### Returns

Pull the single-day scoreboard for ``date``.

### Example

```python
nhl_score()
```

_Last validated n/a._

## `nhl_schedule_calendar`

Pull the calendar of game-days for the season.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/schedule-calendar/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/schedule-calendar](https://api-web.nhle.com/v1/schedule-calendar)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `date` | `date` |  |  | `Y` |

### Returns

Pull the calendar of game-days for the season.

### Example

```python
nhl_schedule_calendar()
```

_Last validated n/a._

## `nhl_playoff_series`

Pull a single playoff series payload.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/schedule/playoff-series/{season}/{series_letter}`

**Valid URL:** [https://api-web.nhle.com/v1/schedule/playoff-series/2025/a](https://api-web.nhle.com/v1/schedule/playoff-series/2025/a)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `series_letter` | `series_letter` |  | `Y` |  |

### Returns

Pull a single playoff series payload.

### Example

```python
nhl_playoff_series(season=2025, series_letter='a')
```

_Last validated n/a._

## `nhl_standings`

Pull the NHL standings.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/standings/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/standings](https://api-web.nhle.com/v1/standings)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `date` | `date` |  |  | `Y` |

### Returns

Pull the NHL standings.

### Example

```python
nhl_standings()
```

_Last validated n/a._

## `nhl_standings_season`

Pull the per-season standings cutover dates.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/standings-season`

**Valid URL:** [https://api-web.nhle.com/v1/standings-season](https://api-web.nhle.com/v1/standings-season)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Pull the per-season standings cutover dates.

### Example

```python
nhl_standings_season()
```

_Last validated n/a._

## `nhl_club_schedule_season`

Pull a team's full-season schedule.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-schedule-season/{team}/{season}`

**Valid URL:** [https://api-web.nhle.com/v1/club-schedule-season/TOR](https://api-web.nhle.com/v1/club-schedule-season/TOR)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team` | `team` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |

### Returns

Pull a team's full-season schedule.

### Example

```python
nhl_club_schedule_season(team='TOR')
```

_Last validated n/a._

## `nhl_club_schedule_month`

Pull a team's schedule for one month.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-schedule/{team}/month/{month}`

**Valid URL:** [https://api-web.nhle.com/v1/club-schedule/TOR/month](https://api-web.nhle.com/v1/club-schedule/TOR/month)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team` | `team` |  | `Y` |  |
| `month` | `month` |  |  | `Y` |

### Returns

Pull a team's schedule for one month.

### Example

```python
nhl_club_schedule_month(team='TOR')
```

_Last validated n/a._

## `nhl_club_schedule_week`

Pull a team's schedule for one week.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-schedule/{team}/week/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/club-schedule/TOR/week](https://api-web.nhle.com/v1/club-schedule/TOR/week)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team` | `team` |  | `Y` |  |
| `date` | `date` |  |  | `Y` |

### Returns

Pull a team's schedule for one week.

### Example

```python
nhl_club_schedule_week(team='TOR')
```

_Last validated n/a._

## `nhl_club_stats`

Pull a team's season stat block.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-stats/{team}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/club-stats/TOR](https://api-web.nhle.com/v1/club-stats/TOR)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team` | `team` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull a team's season stat block.

### Example

```python
nhl_club_stats(team='TOR')
```

_Last validated n/a._

## `nhl_club_stats_season`

Pull the seasons a team has stats for.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-stats-season/{team}`

**Valid URL:** [https://api-web.nhle.com/v1/club-stats-season/TOR](https://api-web.nhle.com/v1/club-stats-season/TOR)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team` | `team` |  | `Y` |  |

### Returns

Pull the seasons a team has stats for.

### Example

```python
nhl_club_stats_season(team='TOR')
```

_Last validated n/a._

## `nhl_roster`

Pull a team's roster.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/roster/{team}/{season}`

**Valid URL:** [https://api-web.nhle.com/v1/roster/TOR](https://api-web.nhle.com/v1/roster/TOR)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team` | `team` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |

### Returns

Pull a team's roster.

### Example

```python
nhl_roster(team='TOR')
```

_Last validated n/a._

## `nhl_roster_season`

Pull every season a team has had on file.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/roster-season/{team}`

**Valid URL:** [https://api-web.nhle.com/v1/roster-season/TOR](https://api-web.nhle.com/v1/roster-season/TOR)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team` | `team` |  | `Y` |  |

### Returns

Pull every season a team has had on file.

### Example

```python
nhl_roster_season(team='TOR')
```

_Last validated n/a._

## `nhl_player_landing`

Pull the player profile / overview.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/player/{player_id}/landing`

**Valid URL:** [https://api-web.nhle.com/v1/player/8480801/landing](https://api-web.nhle.com/v1/player/8480801/landing)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `player_id` | `player_id` |  | `Y` |  |

### Returns

Pull the player profile / overview.

### Example

```python
nhl_player_landing(player_id=8480801)
```

_Last validated n/a._

## `nhl_player_game_log`

Pull a player's game-by-game log.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/player/{player_id}/game-log/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/player/8480801/game-log](https://api-web.nhle.com/v1/player/8480801/game-log)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `player_id` | `player_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull a player's game-by-game log.

### Example

```python
nhl_player_game_log(player_id=8480801)
```

_Last validated n/a._

## `nhl_player_spotlight`

Pull the league's currently featured players.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/player-spotlight`

**Valid URL:** [https://api-web.nhle.com/v1/player-spotlight](https://api-web.nhle.com/v1/player-spotlight)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Pull the league's currently featured players.

### Example

```python
nhl_player_spotlight()
```

_Last validated n/a._

## `nhl_skater_leaders`

Pull skater stat leaders.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/skater-stats-leaders/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/skater-stats-leaders](https://api-web.nhle.com/v1/skater-stats-leaders)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull skater stat leaders.

### Example

```python
nhl_skater_leaders()
```

_Last validated n/a._

## `nhl_goalie_leaders`

Pull goalie stat leaders.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/goalie-stats-leaders/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/goalie-stats-leaders](https://api-web.nhle.com/v1/goalie-stats-leaders)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

Pull goalie stat leaders.

### Example

```python
nhl_goalie_leaders()
```

_Last validated n/a._

## `nhl_draft_picks`

Pull NHL draft picks for a year (and optionally one round).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/draft/picks/{year}/{round_}`

**Valid URL:** [https://api-web.nhle.com/v1/draft/picks/2024](https://api-web.nhle.com/v1/draft/picks/2024)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `year` | `year` |  | `Y` |  |
| `round_` | `round_` |  |  | `Y` |

### Returns

Pull NHL draft picks for a year (and optionally one round).

### Example

```python
nhl_draft_picks(year=2024)
```

_Last validated n/a._

## `nhl_draft_rankings`

Pull NHL Central Scouting rankings for a draft year.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/draft/rankings/{year}/{category}`

**Valid URL:** [https://api-web.nhle.com/v1/draft/rankings/2024](https://api-web.nhle.com/v1/draft/rankings/2024)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `year` | `year` |  | `Y` |  |
| `category` | `category` |  |  | `Y` |

### Returns

Pull NHL Central Scouting rankings for a draft year.

### Example

```python
nhl_draft_rankings(year=2024)
```

_Last validated n/a._

## `nhl_draft_picks_now`

Pull the current / most recent draft pick set.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/draft/picks/now`

**Valid URL:** [https://api-web.nhle.com/v1/draft/picks/now](https://api-web.nhle.com/v1/draft/picks/now)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Pull the current / most recent draft pick set.

### Example

```python
nhl_draft_picks_now()
```

_Last validated n/a._

## `nhl_draft_rankings_now`

Pull the current Central Scouting rankings.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/draft/rankings/now`

**Valid URL:** [https://api-web.nhle.com/v1/draft/rankings/now](https://api-web.nhle.com/v1/draft/rankings/now)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Pull the current Central Scouting rankings.

### Example

```python
nhl_draft_rankings_now()
```

_Last validated n/a._

## `nhl_draft_tracker_picks_now`

Pull the live draft-tracker pick list (during the draft itself).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/draft-tracker/picks/now`

**Valid URL:** [https://api-web.nhle.com/v1/draft-tracker/picks/now](https://api-web.nhle.com/v1/draft-tracker/picks/now)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Pull the live draft-tracker pick list (during the draft itself).

### Example

```python
nhl_draft_tracker_picks_now()
```

_Last validated n/a._
