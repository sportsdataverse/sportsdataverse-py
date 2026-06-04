---
title: MLB — mlb api
sidebar_label: mlb api
---
# MLB — mlb api

`sportsdataverse.mlb` — 26 endpoints.

## `mlb_api_schedule_postseason`

GET /api/v1/schedule/postseason — postseason-only schedule for a season.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/schedule/postseason`

**Valid URL:** [https://statsapi.mlb.com/api/v1/schedule/postseason](https://statsapi.mlb.com/api/v1/schedule/postseason)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  |  | `Y` |
| `sportId` | `sport_id` |  |  | `Y` |
| `hydrate` | `hydrate` |  |  | `Y` |

### Returns

GET /api/v1/schedule/postseason — postseason-only schedule for a season.

### Example

```python
mlb_api_schedule_postseason()
```

_Last validated n/a._

## `mlb_api_pbp`

GET /api/v1.1/game/{gamePk}/feed/live — live firehose (v1.1).

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live`

**Valid URL:** [https://statsapi.mlb.com/api/v1.1/game/716390/feed/live](https://statsapi.mlb.com/api/v1.1/game/716390/feed/live)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_pk` | `game_pk` |  | `Y` |  |
| `language` | `language` |  |  | `Y` |
| `language` | `timecode` |  |  | `Y` |
| `hydrate` | `hydrate` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

GET /api/v1.1/game/{gamePk}/feed/live — live firehose (v1.1).

### Example

```python
mlb_api_pbp(game_pk=716390)
```

_Last validated n/a._

## `mlb_api_boxscore`

GET /api/v1/game/{gamePk}/boxscore — team + player boxscore for one game.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/boxscore](https://statsapi.mlb.com/api/v1/game/716390/boxscore)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_pk` | `game_pk` |  | `Y` |  |
| `timecode` | `timecode` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

GET /api/v1/game/{gamePk}/boxscore — team + player boxscore for one game.

### Example

```python
mlb_api_boxscore(game_pk=716390)
```

_Last validated n/a._

## `mlb_api_linescore`

GET /api/v1/game/{gamePk}/linescore — inning-by-inning + current game state.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/linescore`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/linescore](https://statsapi.mlb.com/api/v1/game/716390/linescore)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_pk` | `game_pk` |  | `Y` |  |
| `timecode` | `timecode` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

GET /api/v1/game/{gamePk}/linescore — inning-by-inning + current game state.

### Example

```python
mlb_api_linescore(game_pk=716390)
```

_Last validated n/a._

## `mlb_api_play_by_play`

GET /api/v1/game/{gamePk}/playByPlay — play-by-play with at-bat detail.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/playByPlay`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/playByPlay](https://statsapi.mlb.com/api/v1/game/716390/playByPlay)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_pk` | `game_pk` |  | `Y` |  |
| `timecode` | `timecode` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

GET /api/v1/game/{gamePk}/playByPlay — play-by-play with at-bat detail.

### Example

```python
mlb_api_play_by_play(game_pk=716390)
```

_Last validated n/a._

## `mlb_api_game_context_metrics`

GET /api/v1/game/{gamePk}/contextMetrics — WP, leverage index, in-game context.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/contextMetrics`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/contextMetrics](https://statsapi.mlb.com/api/v1/game/716390/contextMetrics)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_pk` | `game_pk` |  | `Y` |  |
| `fields` | `fields` |  |  | `Y` |

### Returns

GET /api/v1/game/{gamePk}/contextMetrics — WP, leverage index, in-game context.

### Example

```python
mlb_api_game_context_metrics(game_pk=716390)
```

_Last validated n/a._

## `mlb_api_win_probability`

GET /api/v1/game/{gamePk}/winProbability — per-play WP timeline.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/winProbability`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/winProbability](https://statsapi.mlb.com/api/v1/game/716390/winProbability)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_pk` | `game_pk` |  | `Y` |  |
| `fields` | `fields` |  |  | `Y` |

### Returns

GET /api/v1/game/{gamePk}/winProbability — per-play WP timeline.

### Example

```python
mlb_api_win_probability(game_pk=716390)
```

_Last validated n/a._

## `mlb_api_game_content`

GET /api/v1/game/{gamePk}/content — articles, highlights, editorial content.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/content`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/content](https://statsapi.mlb.com/api/v1/game/716390/content)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_pk` | `game_pk` |  | `Y` |  |

### Returns

GET /api/v1/game/{gamePk}/content — articles, highlights, editorial content.

### Example

```python
mlb_api_game_content(game_pk=716390)
```

_Last validated n/a._

## `mlb_api_team`

GET /api/v1/teams/{teamId} — single team detail.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/{team_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/10](https://statsapi.mlb.com/api/v1/teams/10)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `sportId` | `sport_id` |  |  | `Y` |
| `hydrate` | `hydrate` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

GET /api/v1/teams/{teamId} — single team detail.

### Example

```python
mlb_api_team(team_id=10)
```

_Last validated n/a._

## `mlb_api_team_roster`

GET /api/v1/teams/{teamId}/roster — team roster.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/{team_id}/roster`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/10/roster](https://statsapi.mlb.com/api/v1/teams/10/roster)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `rosterType` | `roster_type` |  |  | `Y` |
| `date` | `date` |  |  | `Y` |
| `hydrate` | `hydrate` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

GET /api/v1/teams/{teamId}/roster — team roster.

### Example

```python
mlb_api_team_roster(team_id=10)
```

_Last validated n/a._

## `mlb_api_team_alumni`

GET /api/v1/teams/{teamId}/alumni — players who played for this team in a season.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/{team_id}/alumni`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/10/alumni](https://statsapi.mlb.com/api/v1/teams/10/alumni)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `group` | `group` |  |  | `Y` |
| `hydrate` | `hydrate` |  |  | `Y` |

### Returns

GET /api/v1/teams/{teamId}/alumni — players who played for this team in a season.

### Example

```python
mlb_api_team_alumni(team_id=10)
```

_Last validated n/a._

## `mlb_api_team_affiliates`

GET /api/v1/teams/affiliates — org affiliates (MLB parent → minor league chain).

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/affiliates`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/affiliates](https://statsapi.mlb.com/api/v1/teams/affiliates)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `teamIds` | `team_ids` |  |  | `Y` |
| `sportId` | `sport_id` |  |  | `Y` |
| `season` | `season` |  |  | `Y` |
| `hydrate` | `hydrate` |  |  | `Y` |

### Returns

GET /api/v1/teams/affiliates — org affiliates (MLB parent → minor league chain).

### Example

```python
mlb_api_team_affiliates()
```

_Last validated n/a._

## `mlb_api_people`

GET /api/v1/people?personIds=... — bulk person lookup by MLBAM id.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/people`

**Valid URL:** [https://statsapi.mlb.com/api/v1/people](https://statsapi.mlb.com/api/v1/people)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `personIds` | `person_ids` |  |  | `Y` |
| `hydrate` | `hydrate` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

GET /api/v1/people?personIds=... — bulk person lookup by MLBAM id.

### Example

```python
mlb_api_people()
```

_Last validated n/a._

## `mlb_api_person`

GET /api/v1/people/{personId} — single person detail.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/people/{person_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/people/660271](https://statsapi.mlb.com/api/v1/people/660271)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `person_id` | `person_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `hydrate` | `hydrate` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

GET /api/v1/people/{personId} — single person detail.

### Example

```python
mlb_api_person(person_id=660271)
```

_Last validated n/a._

## `mlb_api_person_game_stats`

GET /api/v1/people/{personId}/stats/game/{gamePk} — one player, one game.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/people/{person_id}/stats/game/{game_pk}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/people/660271/stats/game/716390](https://statsapi.mlb.com/api/v1/people/660271/stats/game/716390)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `person_id` | `person_id` |  | `Y` |  |
| `game_pk` | `game_pk` |  | `Y` |  |
| `fields` | `fields` |  |  | `Y` |

### Returns

GET /api/v1/people/{personId}/stats/game/{gamePk} — one player, one game.

### Example

```python
mlb_api_person_game_stats(person_id=660271, game_pk=716390)
```

_Last validated n/a._

## `mlb_api_sport_players`

GET /api/v1/sports/{sportId}/players — every player in a sport for a season.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/sports/{sport_id}/players`

**Valid URL:** [https://statsapi.mlb.com/api/v1/sports](https://statsapi.mlb.com/api/v1/sports)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `sport_id` | `sport_id` |  |  | `Y` |
| `season` | `season` |  |  | `Y` |
| `hydrate` | `hydrate` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

GET /api/v1/sports/{sportId}/players — every player in a sport for a season.

### Example

```python
mlb_api_sport_players()
```

_Last validated n/a._

## `mlb_api_sports`

GET /api/v1/sports — list known sports (MLB, MiLB, KBO, NPB, …).

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/sports`

**Valid URL:** [https://statsapi.mlb.com/api/v1/sports](https://statsapi.mlb.com/api/v1/sports)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `sportId` | `sport_id` |  |  | `Y` |

### Returns

GET /api/v1/sports — list known sports (MLB, MiLB, KBO, NPB, …).

### Example

```python
mlb_api_sports()
```

_Last validated n/a._

## `mlb_api_leagues`

GET /api/v1/leagues — list leagues.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/leagues`

**Valid URL:** [https://statsapi.mlb.com/api/v1/leagues](https://statsapi.mlb.com/api/v1/leagues)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `sportId` | `sport_id` |  |  | `Y` |
| `season` | `season` |  |  | `Y` |
| `leagueIds` | `league_ids` |  |  | `Y` |

### Returns

GET /api/v1/leagues — list leagues.

### Example

```python
mlb_api_leagues()
```

_Last validated n/a._

## `mlb_api_season`

GET /api/v1/seasons/{seasonId} — single season detail.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/seasons/{season_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/seasons/X](https://statsapi.mlb.com/api/v1/seasons/X)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season_id` | `season_id` |  | `Y` |  |
| `sportId` | `sport_id` |  |  | `Y` |

### Returns

GET /api/v1/seasons/{seasonId} — single season detail.

### Example

```python
mlb_api_season(season_id='X')
```

_Last validated n/a._

## `mlb_api_venues`

GET /api/v1/venues — list venues.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/venues`

**Valid URL:** [https://statsapi.mlb.com/api/v1/venues](https://statsapi.mlb.com/api/v1/venues)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  |  | `Y` |
| `sportIds` | `sport_ids` |  |  | `Y` |
| `hydrate` | `hydrate` |  |  | `Y` |

### Returns

GET /api/v1/venues — list venues.

### Example

```python
mlb_api_venues()
```

_Last validated n/a._

## `mlb_api_venue`

GET /api/v1/venues/{venueId} — single venue detail.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/venues/{venue_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/venues/15](https://statsapi.mlb.com/api/v1/venues/15)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `venue_id` | `venue_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `hydrate` | `hydrate` |  |  | `Y` |

### Returns

GET /api/v1/venues/{venueId} — single venue detail.

### Example

```python
mlb_api_venue(venue_id=15)
```

_Last validated n/a._

## `mlb_api_meta`

GET /api/v1/{metaType} — enum lookup (the API's self-describing surface).

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/{meta_type}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/leagueLeaderTypes](https://statsapi.mlb.com/api/v1/leagueLeaderTypes)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `meta_type` | `meta_type` |  | `Y` |  |

### Returns

GET /api/v1/{metaType} — enum lookup (the API's self-describing surface).

### Example

```python
mlb_api_meta(meta_type='leagueLeaderTypes')
```

_Last validated n/a._

## `mlb_api_awards`

GET /api/v1/awards — list award IDs (call with no params to enumerate).

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/awards`

**Valid URL:** [https://statsapi.mlb.com/api/v1/awards](https://statsapi.mlb.com/api/v1/awards)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `sportId` | `sport_id` |  |  | `Y` |

### Returns

GET /api/v1/awards — list award IDs (call with no params to enumerate).

### Example

```python
mlb_api_awards()
```

_Last validated n/a._

## `mlb_api_award_recipients`

GET /api/v1/awards/{awardId}/recipients — historical winners of one award.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/awards/{award_id}/recipients`

**Valid URL:** [https://statsapi.mlb.com/api/v1/awards/MLBHOF/recipients](https://statsapi.mlb.com/api/v1/awards/MLBHOF/recipients)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `award_id` | `award_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `sportId` | `sport_id` |  |  | `Y` |
| `hydrate` | `hydrate` |  |  | `Y` |

### Returns

GET /api/v1/awards/{awardId}/recipients — historical winners of one award.

### Example

```python
mlb_api_award_recipients(award_id='MLBHOF')
```

_Last validated n/a._

## `mlb_api_draft`

GET /api/v1/draft/{year} — draft results for a year (optionally one round).

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/draft/{year}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/draft/2024](https://statsapi.mlb.com/api/v1/draft/2024)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `year` | `year` |  | `Y` |  |
| `round` | `round_` |  |  | `Y` |
| `teamId` | `team_id` |  |  | `Y` |
| `playerId` | `player_id` |  |  | `Y` |
| `limit` | `limit` |  |  | `Y` |

### Returns

GET /api/v1/draft/{year} — draft results for a year (optionally one round).

### Example

```python
mlb_api_draft(year=2024)
```

_Last validated n/a._

## `mlb_api_umpires`

GET /api/v1/jobs/umpires — current umpire crew assignments.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/jobs/umpires`

**Valid URL:** [https://statsapi.mlb.com/api/v1/jobs/umpires](https://statsapi.mlb.com/api/v1/jobs/umpires)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

GET /api/v1/jobs/umpires — current umpire crew assignments.

### Example

```python
mlb_api_umpires()
```

_Last validated n/a._
