---
title: MLB — MLB Stats API
sidebar_label: MLB Stats API
sidebar_position: 10
---
# MLB — MLB Stats API

`sportsdataverse.mlb` — 64 endpoints.

## `mlb_api_schedule_postseason`

GET /api/v1/schedule/postseason — postseason-only schedule for a season.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/schedule/postseason`

**Valid URL:** [https://statsapi.mlb.com/api/v1/schedule/postseason](https://statsapi.mlb.com/api/v1/schedule/postseason)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |

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

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `language` | `language` |  |  | `Y` | language query parameter. |
| `language` | `timecode` |  |  | `Y` | language query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

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

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `timecode` | `timecode` |  |  | `Y` | timecode query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

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

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `timecode` | `timecode` |  |  | `Y` | timecode query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

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

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `timecode` | `timecode` |  |  | `Y` | timecode query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

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

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

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

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

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

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |

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

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

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

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `rosterType` | `roster_type` |  |  | `Y` | rosterType query parameter. |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `jersey_number` | character |  |
| `person_id` | integer |  |
| `person_full_name` | character |  |
| `person_link` | character |  |
| `position_code` | character |  |
| `position_name` | character |  |
| `position_type` | character |  |
| `position_abbreviation` | character |  |
| `status_code` | character |  |
| `status_description` | character |  |

### Example

```python
mlb_api_team_roster(team_id=10)
```

_Last validated n/a._

## `mlb_api_team_alumni`

GET /api/v1/teams/{teamId}/alumni — players who played for this team in a season.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/{team_id}/alumni`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/10/alumni](https://statsapi.mlb.com/api/v1/teams/10/alumni)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `group` | `group` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |

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

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamIds` | `team_ids` |  |  | `Y` | teamIds query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |

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

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `personIds` | `person_ids` |  |  | `Y` | personIds query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

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

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_id` | `person_id` |  | `Y` |  | person_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

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

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_id` | `person_id` |  | `Y` |  | person_id path parameter. |
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

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

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport_id` | `sport_id` |  |  | `Y` | sport_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

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

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `id` | integer |  |
| `code` | character |  |
| `link` | character |  |
| `name` | character |  |
| `abbreviation` | character |  |
| `sort_order` | integer |  |
| `active_status` | logical |  |

### Example

```python
mlb_api_sports()
```

_Last validated n/a._

## `mlb_api_leagues`

GET /api/v1/leagues — list leagues.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/leagues`

**Valid URL:** [https://statsapi.mlb.com/api/v1/leagues](https://statsapi.mlb.com/api/v1/leagues)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `leagueIds` | `league_ids` |  |  | `Y` | leagueIds query parameter. |

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

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | season_id path parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |

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

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `sportIds` | `sport_ids` |  |  | `Y` | sportIds query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `id` | integer |  |
| `name` | character |  |
| `link` | character |  |
| `active` | logical |  |
| `season` | character |  |

### Example

```python
mlb_api_venues()
```

_Last validated n/a._

## `mlb_api_venue`

GET /api/v1/venues/{venueId} — single venue detail.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/venues/{venue_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/venues/15](https://statsapi.mlb.com/api/v1/venues/15)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `venue_id` | `venue_id` |  | `Y` |  | venue_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |

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

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `meta_type` | `meta_type` |  | `Y` |  | meta_type path parameter. |

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

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |

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

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `award_id` | `award_id` |  | `Y` |  | award_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |

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

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `round` | `round_` |  |  | `Y` | round query parameter. |
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `playerId` | `player_id` |  |  | `Y` | playerId query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

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

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

GET /api/v1/jobs/umpires — current umpire crew assignments.

### Example

```python
mlb_api_umpires()
```

_Last validated n/a._

## `mlb_api_conferences`

View all PCL conferences.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/conferences`

**Valid URL:** [https://statsapi.mlb.com/api/v1/conferences](https://statsapi.mlb.com/api/v1/conferences)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `conferenceId` | `conference_id` |  |  | `Y` | conferenceId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `id` | integer |  |
| `name` | character |  |
| `link` | character |  |
| `abbreviation` | character |  |
| `has_wildcard` | logical |  |
| `name_short` | character |  |
| `league_id` | integer |  |
| `league_link` | character |  |
| `sport_id` | integer |  |
| `sport_link` | character |  |

### Example

```python
mlb_api_conferences()
```

_Last validated n/a._

## `mlb_api_conference`

View PCL conferences by conferenceId.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/conferences/{conference_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/conferences/301](https://statsapi.mlb.com/api/v1/conferences/301)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `conference_id` | `conference_id` |  | `Y` |  | conference_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `id` | integer |  |
| `name` | character |  |
| `link` | character |  |
| `abbreviation` | character |  |
| `has_wildcard` | logical |  |
| `name_short` | character |  |
| `league_id` | integer |  |
| `league_link` | character |  |
| `sport_id` | integer |  |
| `sport_link` | character |  |

### Example

```python
mlb_api_conference(conference_id=301)
```

_Last validated n/a._

## `mlb_api_draft_latest`

View latest player drafted, endpoint best used when draft is currently open.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/draft/{year}/latest`

**Valid URL:** [https://statsapi.mlb.com/api/v1/draft/2023/latest](https://statsapi.mlb.com/api/v1/draft/2023/latest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `number` | integer |  |
| `next_up` | character |  |
| `pick_pick_round` | character |  |
| `pick_pick_number` | integer |  |
| `pick_display_pick_number` | integer |  |
| `pick_round_pick_number` | integer |  |
| `pick_signing_bonus` | character |  |
| `pick_home_city` | character |  |
| `pick_home_state` | character |  |
| `pick_home_country` | character |  |
| `pick_school_name` | character |  |
| `pick_school_school_class` | character |  |
| `pick_school_city` | character |  |
| `pick_school_country` | character |  |
| `pick_school_state` | character |  |
| `pick_headshot_link` | character |  |
| `pick_person_id` | integer |  |
| `pick_person_full_name` | character |  |
| `pick_person_link` | character |  |
| `pick_person_first_name` | character |  |
| `pick_person_last_name` | character |  |
| `pick_person_birth_date` | character |  |
| `pick_person_current_age` | integer |  |
| `pick_person_birth_city` | character |  |
| `pick_person_birth_state_province` | character |  |
| `pick_person_birth_country` | character |  |
| `pick_person_height` | character |  |
| `pick_person_weight` | integer |  |
| `pick_person_active` | logical |  |
| `pick_person_primary_position_code` | character |  |
| `pick_person_primary_position_name` | character |  |
| `pick_person_primary_position_type` | character |  |
| `pick_person_primary_position_abbreviation` | character |  |
| `pick_person_use_name` | character |  |
| `pick_person_use_last_name` | character |  |
| `pick_person_middle_name` | character |  |
| `pick_person_boxscore_name` | character |  |
| `pick_person_gender` | character |  |
| `pick_person_is_player` | logical |  |
| `pick_person_is_verified` | logical |  |
| `pick_person_draft_year` | integer |  |
| `pick_person_bat_side_code` | character |  |
| `pick_person_bat_side_description` | character |  |
| `pick_person_pitch_hand_code` | character |  |
| `pick_person_pitch_hand_description` | character |  |
| `pick_person_name_first_last` | character |  |
| `pick_person_name_slug` | character |  |
| `pick_person_first_last_name` | character |  |
| `pick_person_last_first_name` | character |  |
| `pick_person_last_init_name` | character |  |
| `pick_person_init_last_name` | character |  |
| `pick_person_full_fml_name` | character |  |
| `pick_person_full_lfm_name` | character |  |
| `pick_person_strike_zone_top` | double |  |
| `pick_person_strike_zone_bottom` | double |  |
| `pick_person_xref_ids` | character |  |
| `pick_team_spring_league_id` | integer |  |
| `pick_team_spring_league_name` | character |  |
| `pick_team_spring_league_link` | character |  |
| `pick_team_spring_league_abbreviation` | character |  |
| `pick_team_all_star_status` | character |  |
| `pick_team_id` | integer |  |
| `pick_team_name` | character |  |
| `pick_team_link` | character |  |
| `pick_team_season` | integer |  |
| `pick_team_venue_id` | integer |  |
| `pick_team_venue_name` | character |  |
| `pick_team_venue_link` | character |  |
| `pick_team_spring_venue_id` | integer |  |
| `pick_team_spring_venue_link` | character |  |
| `pick_team_team_code` | character |  |
| `pick_team_file_code` | character |  |
| `pick_team_abbreviation` | character |  |
| `pick_team_team_name` | character |  |
| `pick_team_location_name` | character |  |
| `pick_team_first_year_of_play` | character |  |
| `pick_team_league_id` | integer |  |
| `pick_team_league_name` | character |  |
| `pick_team_league_link` | character |  |
| `pick_team_division_id` | integer |  |
| `pick_team_division_name` | character |  |
| `pick_team_division_link` | character |  |
| `pick_team_sport_id` | integer |  |
| `pick_team_sport_link` | character |  |
| `pick_team_sport_name` | character |  |
| `pick_team_short_name` | character |  |
| `pick_team_franchise_name` | character |  |
| `pick_team_club_name` | character |  |
| `pick_team_active` | logical |  |
| `pick_draft_type_code` | character |  |
| `pick_draft_type_description` | character |  |
| `pick_is_drafted` | logical |  |
| `pick_is_pass` | logical |  |
| `pick_year` | character |  |

### Example

```python
mlb_api_draft_latest(year=2023)
```

_Last validated n/a._

## `mlb_api_game_timestamps`

Retrieve all of the play timecodes for a game in GUMBO feed.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live/timestamps`

**Valid URL:** [https://statsapi.mlb.com/api/v1.1/game/716390/feed/live/timestamps](https://statsapi.mlb.com/api/v1.1/game/716390/feed/live/timestamps)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `timecode` | character |  |

### Example

```python
mlb_api_game_timestamps(game_pk=716390)
```

_Last validated n/a._

## `mlb_api_game_changes`

View corrected non Statcast information for games

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/changes`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/changes?updatedSince=2023-09-01T00%3A00%3A00Z&sportId=1](https://statsapi.mlb.com/api/v1/game/changes?updatedSince=2023-09-01T00%3A00%3A00Z&sportId=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `updatedSince` | `updated_since` |  |  | `Y` | updatedSince query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `schedule_date` | character |  |
| `game_pk` | integer |  |
| `game_guid` | character |  |
| `link` | character |  |
| `game_type` | character |  |
| `season` | character |  |
| `game_date` | character |  |
| `official_date` | character |  |
| `is_tie` | logical |  |
| `game_number` | integer |  |
| `public_facing` | logical |  |
| `double_header` | character |  |
| `gameday_type` | character |  |
| `tiebreaker` | character |  |
| `calendar_event_id` | character |  |
| `season_display` | character |  |
| `day_night` | character |  |
| `scheduled_innings` | integer |  |
| `reverse_home_away_status` | logical |  |
| `inning_break_length` | integer |  |
| `games_in_series` | double |  |
| `series_game_number` | double |  |
| `series_description` | character |  |
| `record_source` | character |  |
| `if_necessary` | character |  |
| `if_necessary_description` | character |  |
| `status_abstract_game_state` | character |  |
| `status_coded_game_state` | character |  |
| `status_detailed_state` | character |  |
| `status_status_code` | character |  |
| `status_start_time_tbd` | logical |  |
| `status_abstract_game_code` | character |  |
| `teams_away_team_id` | integer |  |
| `teams_away_team_name` | character |  |
| `teams_away_team_link` | character |  |
| `teams_away_league_record_wins` | integer |  |
| `teams_away_league_record_losses` | integer |  |
| `teams_away_league_record_ties` | integer |  |
| `teams_away_league_record_pct` | character |  |
| `teams_away_score` | integer |  |
| `teams_away_is_winner` | logical |  |
| `teams_away_split_squad` | logical |  |
| `teams_away_series_number` | double |  |
| `teams_home_team_id` | integer |  |
| `teams_home_team_name` | character |  |
| `teams_home_team_link` | character |  |
| `teams_home_league_record_wins` | integer |  |
| `teams_home_league_record_losses` | integer |  |
| `teams_home_league_record_ties` | integer |  |
| `teams_home_league_record_pct` | character |  |
| `teams_home_score` | integer |  |
| `teams_home_is_winner` | logical |  |
| `teams_home_split_squad` | logical |  |
| `teams_home_series_number` | double |  |
| `venue_id` | integer |  |
| `venue_name` | character |  |
| `venue_link` | character |  |
| `content_link` | character |  |
| `rescheduled_from` | character |  |
| `rescheduled_from_date` | character |  |
| `description` | character |  |
| `status_reason` | character |  |
| `resumed_from` | character |  |
| `resumed_from_date` | character |  |

### Example

```python
mlb_api_game_changes(sport_id=1, updated_since='2023-09-01T00:00:00Z')
```

_Last validated n/a._

## `mlb_api_analytics_games`

View timestamps of most recent data corrections made to games.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/analytics/game`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/analytics/game](https://statsapi.mlb.com/api/v1/game/analytics/game)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameModeId` | `game_mode_id` |  |  | `Y` | gameModeId query parameter. |
| `timecode` | `timecode` |  |  | `Y` | timecode query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `sortBy` | `sort_by` |  |  | `Y` | sortBy query parameter. |
| `isNonStatcast` | `is_non_statcast` |  |  | `Y` | isNonStatcast query parameter. |
| `offset` | `offset` |  |  | `Y` | offset query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

View timestamps of most recent data corrections made to games.

### Example

```python
mlb_api_analytics_games()
```

_Last validated n/a._

## `mlb_api_analytics_guids`

View timestamps of most recent data corrections made to GUIDs.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/analytics/guids`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/analytics/guids](https://statsapi.mlb.com/api/v1/game/analytics/guids)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameModeId` | `game_mode_id` |  |  | `Y` | gameModeId query parameter. |
| `timecode` | `timecode` |  |  | `Y` | timecode query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `sortBy` | `sort_by` |  |  | `Y` | sortBy query parameter. |
| `isNonStatcast` | `is_non_statcast` |  |  | `Y` | isNonStatcast query parameter. |
| `offset` | `offset` |  |  | `Y` | offset query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

View timestamps of most recent data corrections made to GUIDs.

### Example

```python
mlb_api_analytics_guids()
```

_Last validated n/a._

## `mlb_api_game_guids`

View Statcast data for a specific game.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/guids`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/guids](https://statsapi.mlb.com/api/v1/game/716390/guids)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `gameModeId` | `game_mode_id` |  |  | `Y` | gameModeId query parameter. |
| `updatedSince` | `updated_since` |  |  | `Y` | updatedSince query parameter. |
| `isPitch` | `is_pitch` |  |  | `Y` | isPitch query parameter. |
| `isHit` | `is_hit` |  |  | `Y` | isHit query parameter. |
| `isPickoff` | `is_pickoff` |  |  | `Y` | isPickoff query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `parsed/raw` | `parsed_raw` |  |  | `Y` | parsed/raw query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

View Statcast data for a specific game.

### Example

```python
mlb_api_game_guids(game_pk=716390)
```

_Last validated n/a._

## `mlb_api_play_analytics`

View Statcast data for a specific play.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/{guid}/analytics`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/90groovy-2438-test-guid-placeholder0/analytics](https://statsapi.mlb.com/api/v1/game/716390/90groovy-2438-test-guid-placeholder0/analytics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `guid` | `guid` |  | `Y` |  | guid path parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

View Statcast data for a specific play.

### Example

```python
mlb_api_play_analytics(game_pk=716390, guid='90groovy-2438-test-guid-placeholder0')
```

_Last validated n/a._

## `mlb_api_play_context_metrics_averages`

View Statcast contextMetrics data for a specific play.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/{guid}/contextMetricsAverages`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/90groovy-2438-test-guid-placeholder0/contextMetricsAverages](https://statsapi.mlb.com/api/v1/game/716390/90groovy-2438-test-guid-placeholder0/contextMetricsAverages)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `guid` | `guid` |  | `Y` |  | guid path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

View Statcast contextMetrics data for a specific play.

### Example

```python
mlb_api_play_context_metrics_averages(game_pk=716390, guid='90groovy-2438-test-guid-placeholder0')
```

_Last validated n/a._

## `mlb_api_game_color`

View game color commentary info.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/feed/color`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/feed/color](https://statsapi.mlb.com/api/v1/game/716390/feed/color)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `timecode` | `timecode` |  |  | `Y` | timecode query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

View game color commentary info.

### Example

```python
mlb_api_game_color(game_pk=716390)
```

_Last validated n/a._

## `mlb_api_game_color_diff`

View game color feed.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/feed/color/diffPatch`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/feed/color/diffPatch](https://statsapi.mlb.com/api/v1/game/716390/feed/color/diffPatch)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `startTimecode` | `start_timecode` |  |  | `Y` | startTimecode query parameter. |
| `endTimecode` | `end_timecode` |  |  | `Y` | endTimecode query parameter. |

### Returns

View game color feed.

### Example

```python
mlb_api_game_color_diff(game_pk=716390)
```

_Last validated n/a._

## `mlb_api_game_color_timestamps`

View all of the color timecodes for a game.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/feed/color/timestamps`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/feed/color/timestamps](https://statsapi.mlb.com/api/v1/game/716390/feed/color/timestamps)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |

### Returns

View all of the color timecodes for a game.

### Example

```python
mlb_api_game_color_timestamps(game_pk=716390)
```

_Last validated n/a._

## `mlb_api_game_pace`

View time of game info.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/gamePace`

**Valid URL:** [https://statsapi.mlb.com/api/v1/gamePace?season=2023](https://statsapi.mlb.com/api/v1/gamePace?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `teamIds` | `team_ids` |  |  | `Y` | teamIds query parameter. |
| `leagueIds` | `league_ids` |  |  | `Y` | leagueIds query parameter. |
| `leagueListId` | `league_list_id` |  |  | `Y` | leagueListId query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `gameType` | `game_type` |  |  | `Y` | gameType query parameter. |
| `startDate` | `start_date` |  |  | `Y` | startDate query parameter. |
| `endDate` | `end_date` |  |  | `Y` | endDate query parameter. |
| `venueIds` | `venue_ids` |  |  | `Y` | venueIds query parameter. |
| `orgType` | `org_type` |  |  | `Y` | orgType query parameter. |
| `includeChildren` | `include_children` |  |  | `Y` | includeChildren query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `hits_per9_inn` | double |  |
| `runs_per9_inn` | double |  |
| `pitches_per9_inn` | double |  |
| `plate_appearances_per9_inn` | double |  |
| `hits_per_game` | double |  |
| `runs_per_game` | double |  |
| `innings_played_per_game` | double |  |
| `pitches_per_game` | double |  |
| `pitchers_per_game` | double |  |
| `plate_appearances_per_game` | double |  |
| `total_game_time` | character |  |
| `total_innings_played` | double |  |
| `total_hits` | integer |  |
| `total_runs` | integer |  |
| `total_plate_appearances` | integer |  |
| `total_pitchers` | integer |  |
| `total_pitches` | integer |  |
| `total_games` | integer |  |
| `total7_inn_games` | integer |  |
| `total9_inn_games` | double |  |
| `total_extra_inn_games` | integer |  |
| `time_per_game` | character |  |
| `time_per_pitch` | character |  |
| `time_per_hit` | character |  |
| `time_per_run` | character |  |
| `time_per_plate_appearance` | character |  |
| `time_per9_inn` | character |  |
| `time_per77_plate_appearances` | character |  |
| `total_extra_inn_time` | character |  |
| `time_per7_inn_game_without_extra_inn` | character |  |
| `total9_inn_games_completed_early` | integer |  |
| `total9_inn_games_without_extra_inn` | double |  |
| `total9_inn_games_scheduled` | integer |  |
| `hits_per_run` | double |  |
| `pitches_per_pitcher` | double |  |
| `season` | character |  |
| `sport_id` | integer |  |
| `sport_code` | character |  |
| `sport_link` | character |  |
| `pr_portal_calculated_fields_total7_inn_games` | integer |  |
| `pr_portal_calculated_fields_total9_inn_games` | double |  |
| `pr_portal_calculated_fields_total_extra_inn_games` | integer |  |
| `pr_portal_calculated_fields_time_per7_inn_game` | character |  |
| `pr_portal_calculated_fields_time_per9_inn_game` | character |  |
| `pr_portal_calculated_fields_time_per_extra_inn_game` | character |  |
| `time_per7_inn_game` | character |  |
| `total7_inn_games_scheduled` | double |  |
| `total7_inn_games_without_extra_inn` | double |  |
| `total7_inn_games_completed_early` | double |  |

### Example

```python
mlb_api_game_pace(season='2023')
```

_Last validated n/a._

## `mlb_api_high_low`

View high/low stats by player or team.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/highLow/{org_type}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/highLow/player?statGroup=hitting&sortStat=homeRuns&season=2023](https://statsapi.mlb.com/api/v1/highLow/player?statGroup=hitting&sortStat=homeRuns&season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `org_type` | `org_type` |  | `Y` |  | org_type path parameter. |
| `statGroup` | `stat_group` |  |  | `Y` | statGroup query parameter. |
| `sortStat` | `sort_stat` |  |  | `Y` | sortStat query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `gameType` | `game_type` |  |  | `Y` | gameType query parameter. |
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `leagueId` | `league_id` |  |  | `Y` | leagueId query parameter. |
| `sportIds` | `sport_ids` |  |  | `Y` | sportIds query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `total_splits` | integer |  |
| `exemptions` | character |  |
| `splits` | character |  |
| `splits_tied_with_offset` | character |  |
| `splits_tied_with_limit` | character |  |
| `season` | character |  |
| `combined_stats` | logical |  |
| `group_display_name` | character |  |
| `game_type_id` | character |  |
| `game_type_description` | character |  |
| `sort_stat_name` | character |  |
| `sort_stat_lookup_param` | character |  |
| `sort_stat_is_counting` | logical |  |
| `sort_stat_label` | character |  |
| `sort_stat_stat_groups` | character |  |
| `sort_stat_org_types` | character |  |
| `sort_stat_high_low_types` | character |  |
| `sort_stat_streak_levels` | character |  |

### Example

```python
mlb_api_high_low(org_type='player', stat_group='hitting', sort_stat='homeRuns', season='2023')
```

_Last validated n/a._

## `mlb_api_home_run_derby`

View a home run derby object based on gamePk.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/homeRunDerby/{game_pk}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/homeRunDerby/511101](https://statsapi.mlb.com/api/v1/homeRunDerby/511101)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `id` | integer |  |
| `full_name` | character |  |
| `link` | character |  |
| `first_name` | character |  |
| `last_name` | character |  |
| `primary_number` | character |  |
| `birth_date` | character |  |
| `current_age` | integer |  |
| `birth_city` | character |  |
| `birth_state_province` | character |  |
| `birth_country` | character |  |
| `height` | character |  |
| `weight` | integer |  |
| `active` | logical |  |
| `use_name` | character |  |
| `use_last_name` | character |  |
| `middle_name` | character |  |
| `boxscore_name` | character |  |
| `nick_name` | character |  |
| `gender` | character |  |
| `is_player` | logical |  |
| `is_verified` | logical |  |
| `draft_year` | double |  |
| `pronunciation` | character |  |
| `stats` | character |  |
| `mlb_debut_date` | character |  |
| `name_first_last` | character |  |
| `name_slug` | character |  |
| `first_last_name` | character |  |
| `last_first_name` | character |  |
| `last_init_name` | character |  |
| `init_last_name` | character |  |
| `full_fml_name` | character |  |
| `full_lfm_name` | character |  |
| `strike_zone_top` | double |  |
| `strike_zone_bottom` | double |  |
| `current_team_spring_league_id` | double |  |
| `current_team_spring_league_name` | character |  |
| `current_team_spring_league_link` | character |  |
| `current_team_spring_league_abbreviation` | character |  |
| `current_team_all_star_status` | character |  |
| `current_team_id` | integer |  |
| `current_team_name` | character |  |
| `current_team_link` | character |  |
| `current_team_season` | integer |  |
| `current_team_venue_id` | integer |  |
| `current_team_venue_name` | character |  |
| `current_team_venue_link` | character |  |
| `current_team_spring_venue_id` | double |  |
| `current_team_spring_venue_link` | character |  |
| `current_team_team_code` | character |  |
| `current_team_file_code` | character |  |
| `current_team_abbreviation` | character |  |
| `current_team_team_name` | character |  |
| `current_team_location_name` | character |  |
| `current_team_first_year_of_play` | character |  |
| `current_team_league_id` | integer |  |
| `current_team_league_name` | character |  |
| `current_team_league_link` | character |  |
| `current_team_division_id` | double |  |
| `current_team_division_name` | character |  |
| `current_team_division_link` | character |  |
| `current_team_sport_id` | integer |  |
| `current_team_sport_link` | character |  |
| `current_team_sport_name` | character |  |
| `current_team_short_name` | character |  |
| `current_team_franchise_name` | character |  |
| `current_team_club_name` | character |  |
| `current_team_active` | logical |  |
| `primary_position_code` | character |  |
| `primary_position_name` | character |  |
| `primary_position_type` | character |  |
| `primary_position_abbreviation` | character |  |
| `bat_side_code` | character |  |
| `bat_side_description` | character |  |
| `pitch_hand_code` | character |  |
| `pitch_hand_description` | character |  |
| `last_played_date` | character |  |
| `name_matrilineal` | character |  |
| `current_team_parent_org_name` | character |  |
| `current_team_parent_org_id` | double |  |

### Example

```python
mlb_api_home_run_derby(game_pk=511101)
```

_Last validated n/a._

## `mlb_api_home_run_derby_bracket`

View a home run derby object based on bracket.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/homeRunDerby/{game_pk}/bracket`

**Valid URL:** [https://statsapi.mlb.com/api/v1/homeRunDerby/511101/bracket](https://statsapi.mlb.com/api/v1/homeRunDerby/511101/bracket)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `id` | integer |  |
| `full_name` | character |  |
| `link` | character |  |
| `first_name` | character |  |
| `last_name` | character |  |
| `primary_number` | character |  |
| `birth_date` | character |  |
| `current_age` | integer |  |
| `birth_city` | character |  |
| `birth_state_province` | character |  |
| `birth_country` | character |  |
| `height` | character |  |
| `weight` | integer |  |
| `active` | logical |  |
| `use_name` | character |  |
| `use_last_name` | character |  |
| `middle_name` | character |  |
| `boxscore_name` | character |  |
| `nick_name` | character |  |
| `gender` | character |  |
| `is_player` | logical |  |
| `is_verified` | logical |  |
| `draft_year` | double |  |
| `pronunciation` | character |  |
| `stats` | character |  |
| `mlb_debut_date` | character |  |
| `name_first_last` | character |  |
| `name_slug` | character |  |
| `first_last_name` | character |  |
| `last_first_name` | character |  |
| `last_init_name` | character |  |
| `init_last_name` | character |  |
| `full_fml_name` | character |  |
| `full_lfm_name` | character |  |
| `strike_zone_top` | double |  |
| `strike_zone_bottom` | double |  |
| `current_team_spring_league_id` | double |  |
| `current_team_spring_league_name` | character |  |
| `current_team_spring_league_link` | character |  |
| `current_team_spring_league_abbreviation` | character |  |
| `current_team_all_star_status` | character |  |
| `current_team_id` | integer |  |
| `current_team_name` | character |  |
| `current_team_link` | character |  |
| `current_team_season` | integer |  |
| `current_team_venue_id` | integer |  |
| `current_team_venue_name` | character |  |
| `current_team_venue_link` | character |  |
| `current_team_spring_venue_id` | double |  |
| `current_team_spring_venue_link` | character |  |
| `current_team_team_code` | character |  |
| `current_team_file_code` | character |  |
| `current_team_abbreviation` | character |  |
| `current_team_team_name` | character |  |
| `current_team_location_name` | character |  |
| `current_team_first_year_of_play` | character |  |
| `current_team_league_id` | integer |  |
| `current_team_league_name` | character |  |
| `current_team_league_link` | character |  |
| `current_team_division_id` | double |  |
| `current_team_division_name` | character |  |
| `current_team_division_link` | character |  |
| `current_team_sport_id` | integer |  |
| `current_team_sport_link` | character |  |
| `current_team_sport_name` | character |  |
| `current_team_short_name` | character |  |
| `current_team_franchise_name` | character |  |
| `current_team_club_name` | character |  |
| `current_team_active` | logical |  |
| `primary_position_code` | character |  |
| `primary_position_name` | character |  |
| `primary_position_type` | character |  |
| `primary_position_abbreviation` | character |  |
| `bat_side_code` | character |  |
| `bat_side_description` | character |  |
| `pitch_hand_code` | character |  |
| `pitch_hand_description` | character |  |
| `last_played_date` | character |  |
| `name_matrilineal` | character |  |
| `current_team_parent_org_name` | character |  |
| `current_team_parent_org_id` | double |  |

### Example

```python
mlb_api_home_run_derby_bracket(game_pk=511101)
```

_Last validated n/a._

## `mlb_api_home_run_derby_pool`

View a home run derby object based on pool.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/homeRunDerby/{game_pk}/pool`

**Valid URL:** [https://statsapi.mlb.com/api/v1/homeRunDerby/511101/pool](https://statsapi.mlb.com/api/v1/homeRunDerby/511101/pool)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `id` | integer |  |
| `full_name` | character |  |
| `link` | character |  |
| `first_name` | character |  |
| `last_name` | character |  |
| `primary_number` | character |  |
| `birth_date` | character |  |
| `current_age` | integer |  |
| `birth_city` | character |  |
| `birth_state_province` | character |  |
| `birth_country` | character |  |
| `height` | character |  |
| `weight` | integer |  |
| `active` | logical |  |
| `use_name` | character |  |
| `use_last_name` | character |  |
| `middle_name` | character |  |
| `boxscore_name` | character |  |
| `nick_name` | character |  |
| `gender` | character |  |
| `is_player` | logical |  |
| `is_verified` | logical |  |
| `draft_year` | double |  |
| `pronunciation` | character |  |
| `stats` | character |  |
| `mlb_debut_date` | character |  |
| `name_first_last` | character |  |
| `name_slug` | character |  |
| `first_last_name` | character |  |
| `last_first_name` | character |  |
| `last_init_name` | character |  |
| `init_last_name` | character |  |
| `full_fml_name` | character |  |
| `full_lfm_name` | character |  |
| `strike_zone_top` | double |  |
| `strike_zone_bottom` | double |  |
| `current_team_spring_league_id` | double |  |
| `current_team_spring_league_name` | character |  |
| `current_team_spring_league_link` | character |  |
| `current_team_spring_league_abbreviation` | character |  |
| `current_team_all_star_status` | character |  |
| `current_team_id` | integer |  |
| `current_team_name` | character |  |
| `current_team_link` | character |  |
| `current_team_season` | integer |  |
| `current_team_venue_id` | integer |  |
| `current_team_venue_name` | character |  |
| `current_team_venue_link` | character |  |
| `current_team_spring_venue_id` | double |  |
| `current_team_spring_venue_link` | character |  |
| `current_team_team_code` | character |  |
| `current_team_file_code` | character |  |
| `current_team_abbreviation` | character |  |
| `current_team_team_name` | character |  |
| `current_team_location_name` | character |  |
| `current_team_first_year_of_play` | character |  |
| `current_team_league_id` | integer |  |
| `current_team_league_name` | character |  |
| `current_team_league_link` | character |  |
| `current_team_division_id` | double |  |
| `current_team_division_name` | character |  |
| `current_team_division_link` | character |  |
| `current_team_sport_id` | integer |  |
| `current_team_sport_link` | character |  |
| `current_team_sport_name` | character |  |
| `current_team_short_name` | character |  |
| `current_team_franchise_name` | character |  |
| `current_team_club_name` | character |  |
| `current_team_active` | logical |  |
| `primary_position_code` | character |  |
| `primary_position_name` | character |  |
| `primary_position_type` | character |  |
| `primary_position_abbreviation` | character |  |
| `bat_side_code` | character |  |
| `bat_side_description` | character |  |
| `pitch_hand_code` | character |  |
| `pitch_hand_description` | character |  |
| `last_played_date` | character |  |
| `name_matrilineal` | character |  |
| `current_team_parent_org_name` | character |  |
| `current_team_parent_org_id` | double |  |

### Example

```python
mlb_api_home_run_derby_pool(game_pk=511101)
```

_Last validated n/a._

## `mlb_api_all_star_ballot`

View All-Star Ballots per league.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/league/{league_id}/allStarBallot`

**Valid URL:** [https://statsapi.mlb.com/api/v1/league/103/allStarBallot?season=2023](https://statsapi.mlb.com/api/v1/league/103/allStarBallot?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | league_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `id` | integer |  |
| `full_name` | character |  |
| `link` | character |  |
| `first_name` | character |  |
| `last_name` | character |  |
| `primary_number` | character |  |
| `birth_date` | character |  |
| `current_age` | integer |  |
| `birth_city` | character |  |
| `birth_country` | character |  |
| `height` | character |  |
| `weight` | integer |  |
| `active` | logical |  |
| `use_name` | character |  |
| `use_last_name` | character |  |
| `middle_name` | character |  |
| `boxscore_name` | character |  |
| `nick_name` | character |  |
| `gender` | character |  |
| `name_matrilineal` | character |  |
| `is_player` | logical |  |
| `is_verified` | logical |  |
| `pronunciation` | character |  |
| `last_played_date` | character |  |
| `mlb_debut_date` | character |  |
| `name_first_last` | character |  |
| `name_slug` | character |  |
| `first_last_name` | character |  |
| `last_first_name` | character |  |
| `last_init_name` | character |  |
| `init_last_name` | character |  |
| `full_fml_name` | character |  |
| `full_lfm_name` | character |  |
| `strike_zone_top` | double |  |
| `strike_zone_bottom` | double |  |
| `primary_position_code` | character |  |
| `primary_position_name` | character |  |
| `primary_position_type` | character |  |
| `primary_position_abbreviation` | character |  |
| `bat_side_code` | character |  |
| `bat_side_description` | character |  |
| `pitch_hand_code` | character |  |
| `pitch_hand_description` | character |  |
| `birth_state_province` | character |  |
| `draft_year` | double |  |
| `name_title` | character |  |
| `name_suffix` | character |  |

### Example

```python
mlb_api_all_star_ballot(league_id='103', season='2023')
```

_Last validated n/a._

## `mlb_api_all_star_write_ins`

View All-Star Write-ins per league.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/league/{league_id}/allStarWriteIns`

**Valid URL:** [https://statsapi.mlb.com/api/v1/league/103/allStarWriteIns?season=2023](https://statsapi.mlb.com/api/v1/league/103/allStarWriteIns?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | league_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `id` | integer |  |
| `full_name` | character |  |
| `link` | character |  |
| `first_name` | character |  |
| `last_name` | character |  |
| `birth_date` | character |  |
| `current_age` | integer |  |
| `birth_city` | character |  |
| `birth_state_province` | character |  |
| `birth_country` | character |  |
| `height` | character |  |
| `weight` | integer |  |
| `active` | logical |  |
| `use_name` | character |  |
| `use_last_name` | character |  |
| `boxscore_name` | character |  |
| `gender` | character |  |
| `is_player` | logical |  |
| `is_verified` | logical |  |
| `pronunciation` | character |  |
| `mlb_debut_date` | character |  |
| `name_first_last` | character |  |
| `name_slug` | character |  |
| `first_last_name` | character |  |
| `last_first_name` | character |  |
| `last_init_name` | character |  |
| `init_last_name` | character |  |
| `full_fml_name` | character |  |
| `full_lfm_name` | character |  |
| `strike_zone_top` | double |  |
| `strike_zone_bottom` | double |  |
| `bat_side_code` | character |  |
| `bat_side_description` | character |  |
| `pitch_hand_code` | character |  |
| `pitch_hand_description` | character |  |
| `primary_number` | character |  |
| `draft_year` | double |  |
| `middle_name` | character |  |
| `name_matrilineal` | character |  |
| `last_played_date` | character |  |
| `nick_name` | character |  |
| `name_title` | character |  |
| `name_suffix` | character |  |

### Example

```python
mlb_api_all_star_write_ins(league_id='103', season='2023')
```

_Last validated n/a._

## `mlb_api_all_star_final_vote`

View All-Star Final Vote per league.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/league/{league_id}/allStarFinalVote`

**Valid URL:** [https://statsapi.mlb.com/api/v1/league/103/allStarFinalVote?season=2023](https://statsapi.mlb.com/api/v1/league/103/allStarFinalVote?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | league_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `id` | integer |  |
| `full_name` | character |  |
| `link` | character |  |
| `first_name` | character |  |
| `last_name` | character |  |
| `primary_number` | character |  |
| `birth_date` | character |  |
| `current_age` | integer |  |
| `birth_city` | character |  |
| `birth_country` | character |  |
| `height` | character |  |
| `weight` | integer |  |
| `active` | logical |  |
| `use_name` | character |  |
| `use_last_name` | character |  |
| `boxscore_name` | character |  |
| `nick_name` | character |  |
| `gender` | character |  |
| `is_player` | logical |  |
| `is_verified` | logical |  |
| `pronunciation` | character |  |
| `mlb_debut_date` | character |  |
| `name_first_last` | character |  |
| `name_slug` | character |  |
| `first_last_name` | character |  |
| `last_first_name` | character |  |
| `last_init_name` | character |  |
| `init_last_name` | character |  |
| `full_fml_name` | character |  |
| `full_lfm_name` | character |  |
| `strike_zone_top` | double |  |
| `strike_zone_bottom` | double |  |
| `primary_position_code` | character |  |
| `primary_position_name` | character |  |
| `primary_position_type` | character |  |
| `primary_position_abbreviation` | character |  |
| `bat_side_code` | character |  |
| `bat_side_description` | character |  |
| `pitch_hand_code` | character |  |
| `pitch_hand_description` | character |  |
| `name_matrilineal` | character |  |
| `birth_state_province` | character |  |
| `name_title` | character |  |
| `name_suffix` | character |  |
| `middle_name` | character |  |
| `draft_year` | double |  |
| `last_played_date` | character |  |

### Example

```python
mlb_api_all_star_final_vote(league_id='103', season='2023')
```

_Last validated n/a._

## `mlb_api_free_agents`

View biographical information and stats for Free Agents.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/people/freeAgents`

**Valid URL:** [https://statsapi.mlb.com/api/v1/people/freeAgents?season=2023](https://statsapi.mlb.com/api/v1/people/freeAgents?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `order` | `order` |  |  | `Y` | order query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `notes` | character |  |
| `date_declared` | character |  |
| `player_id` | integer |  |
| `player_full_name` | character |  |
| `player_link` | character |  |
| `original_team_id` | double |  |
| `original_team_name` | character |  |
| `original_team_link` | character |  |
| `new_team_link` | character |  |
| `position_code` | character |  |
| `position_name` | character |  |
| `position_type` | character |  |
| `position_abbreviation` | character |  |
| `date_signed` | character |  |
| `new_team_id` | double |  |
| `new_team_name` | character |  |
| `sort_order` | double |  |

### Example

```python
mlb_api_free_agents(season='2023')
```

_Last validated n/a._

## `mlb_api_jobs`

View directory by jobType.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/jobs`

**Valid URL:** [https://statsapi.mlb.com/api/v1/jobs?jobType=UMPR](https://statsapi.mlb.com/api/v1/jobs?jobType=UMPR)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `jobType` | `job_type` |  |  | `Y` | jobType query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `jersey_number` | character |  |
| `job` | character |  |
| `job_id` | character |  |
| `title` | character |  |
| `person_id` | integer |  |
| `person_full_name` | character |  |
| `person_link` | character |  |

### Example

```python
mlb_api_jobs(job_type='UMPR')
```

_Last validated n/a._

## `mlb_api_datacasters`

View datacasters directory.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/jobs/datacasters`

**Valid URL:** [https://statsapi.mlb.com/api/v1/jobs/datacasters](https://statsapi.mlb.com/api/v1/jobs/datacasters)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `jersey_number` | character |  |
| `job` | character |  |
| `job_id` | character |  |
| `title` | character |  |
| `person_id` | integer |  |
| `person_full_name` | character |  |
| `person_link` | character |  |

### Example

```python
mlb_api_datacasters()
```

_Last validated n/a._

## `mlb_api_official_scorers`

View official scorer directory.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/jobs/officialScorers`

**Valid URL:** [https://statsapi.mlb.com/api/v1/jobs/officialScorers](https://statsapi.mlb.com/api/v1/jobs/officialScorers)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `jersey_number` | character |  |
| `job` | character |  |
| `job_id` | character |  |
| `title` | character |  |
| `person_id` | integer |  |
| `person_full_name` | character |  |
| `person_link` | character |  |

### Example

```python
mlb_api_official_scorers()
```

_Last validated n/a._

## `mlb_api_umpire_games`

Get umpires and associated game for umpireId.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/jobs/umpires/games/{umpire_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/jobs/umpires/games/596809?season=2023](https://statsapi.mlb.com/api/v1/jobs/umpires/games/596809?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `umpire_id` | `umpire_id` |  | `Y` |  | umpire_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

Get umpires and associated game for umpireId.

### Example

```python
mlb_api_umpire_games(umpire_id=596809, season='2023')
```

_Last validated n/a._

## `mlb_api_schedule_tied`

View tied game schedule info.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/schedule/games/tied`

**Valid URL:** [https://statsapi.mlb.com/api/v1/schedule/games/tied?season=2016](https://statsapi.mlb.com/api/v1/schedule/games/tied?season=2016)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameTypes` | `game_types` |  |  | `Y` | gameTypes query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `schedule_date` | character |  |
| `game_pk` | integer |  |
| `game_guid` | character |  |
| `link` | character |  |
| `game_type` | character |  |
| `season` | character |  |
| `game_date` | character |  |
| `official_date` | character |  |
| `is_tie` | logical |  |
| `game_number` | integer |  |
| `public_facing` | logical |  |
| `double_header` | character |  |
| `gameday_type` | character |  |
| `tiebreaker` | character |  |
| `calendar_event_id` | character |  |
| `season_display` | character |  |
| `day_night` | character |  |
| `scheduled_innings` | integer |  |
| `reverse_home_away_status` | logical |  |
| `inning_break_length` | integer |  |
| `games_in_series` | integer |  |
| `series_game_number` | integer |  |
| `series_description` | character |  |
| `record_source` | character |  |
| `if_necessary` | character |  |
| `if_necessary_description` | character |  |
| `status_abstract_game_state` | character |  |
| `status_coded_game_state` | character |  |
| `status_detailed_state` | character |  |
| `status_status_code` | character |  |
| `status_start_time_tbd` | logical |  |
| `status_reason` | character |  |
| `status_abstract_game_code` | character |  |
| `teams_away_team_id` | integer |  |
| `teams_away_team_name` | character |  |
| `teams_away_team_link` | character |  |
| `teams_away_league_record_wins` | integer |  |
| `teams_away_league_record_losses` | integer |  |
| `teams_away_league_record_ties` | integer |  |
| `teams_away_league_record_pct` | character |  |
| `teams_away_score` | integer |  |
| `teams_away_split_squad` | logical |  |
| `teams_away_series_number` | integer |  |
| `teams_home_team_id` | integer |  |
| `teams_home_team_name` | character |  |
| `teams_home_team_link` | character |  |
| `teams_home_league_record_wins` | integer |  |
| `teams_home_league_record_losses` | integer |  |
| `teams_home_league_record_ties` | integer |  |
| `teams_home_league_record_pct` | character |  |
| `teams_home_score` | integer |  |
| `teams_home_split_squad` | logical |  |
| `teams_home_series_number` | integer |  |
| `venue_id` | integer |  |
| `venue_name` | character |  |
| `venue_link` | character |  |
| `content_link` | character |  |

### Example

```python
mlb_api_schedule_tied(season='2016')
```

_Last validated n/a._

## `mlb_api_schedule_postseason_series`

View schedule info for postseason based on series.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/schedule/postseason/series`

**Valid URL:** [https://statsapi.mlb.com/api/v1/schedule/postseason/series?season=2023](https://statsapi.mlb.com/api/v1/schedule/postseason/series?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameTypes` | `game_types` |  |  | `Y` | gameTypes query parameter. |
| `seriesNumber` | `series_number` |  |  | `Y` | seriesNumber query parameter. |
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `total_items` | integer |  |
| `total_games` | integer |  |
| `total_games_in_progress` | integer |  |
| `games` | character |  |
| `sort_order` | integer |  |
| `series_id` | character |  |
| `series_sort_number` | integer |  |
| `series_is_default` | logical |  |
| `series_game_type` | character |  |

### Example

```python
mlb_api_schedule_postseason_series(season='2023')
```

_Last validated n/a._

## `mlb_api_schedule_postseason_tunein`

View schedule info for the tuneIn application.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/schedule/postseason/tuneIn`

**Valid URL:** [https://statsapi.mlb.com/api/v1/schedule/postseason/tuneIn?season=2023](https://statsapi.mlb.com/api/v1/schedule/postseason/tuneIn?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

View schedule info for the tuneIn application.

### Example

```python
mlb_api_schedule_postseason_tunein(season='2023')
```

_Last validated n/a._

## `mlb_api_seasons_all`

View information for all seasons based on id.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/seasons/all`

**Valid URL:** [https://statsapi.mlb.com/api/v1/seasons/all?sportId=1](https://statsapi.mlb.com/api/v1/seasons/all?sportId=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `divisionId` | `division_id` |  |  | `Y` | divisionId query parameter. |
| `leagueId` | `league_id` |  |  | `Y` | leagueId query parameter. |
| `withGameTypeDates` | `with_game_type_dates` |  |  | `Y` | withGameTypeDates query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `season_id` | character |  |
| `has_wildcard` | logical |  |
| `pre_season_start_date` | character |  |
| `season_start_date` | character |  |
| `regular_season_start_date` | character |  |
| `regular_season_end_date` | character |  |
| `season_end_date` | character |  |
| `offseason_start_date` | character |  |
| `off_season_end_date` | character |  |
| `season_level_gameday_type` | character |  |
| `game_level_gameday_type` | character |  |
| `qualifier_plate_appearances` | double |  |
| `qualifier_outs_pitched` | double |  |
| `post_season_start_date` | character |  |
| `post_season_end_date` | character |  |
| `last_date1st_half` | character |  |
| `all_star_date` | character |  |
| `first_date2nd_half` | character |  |
| `pre_season_end_date` | character |  |
| `spring_start_date` | character |  |
| `spring_end_date` | character |  |

### Example

```python
mlb_api_seasons_all(sport_id=1)
```

_Last validated n/a._

## `mlb_api_sport`

View information for any given sportId.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/sports/{sport_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/sports/1](https://statsapi.mlb.com/api/v1/sports/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport_id` | `sport_id` |  | `Y` |  | sport_id path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `id` | integer |  |
| `code` | character |  |
| `link` | character |  |
| `name` | character |  |
| `abbreviation` | character |  |
| `sort_order` | integer |  |
| `active_status` | logical |  |

### Example

```python
mlb_api_sport(sport_id=1)
```

_Last validated n/a._

## `mlb_api_stats_metrics`

View Statcast stats.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/stats/metrics`

**Valid URL:** [https://statsapi.mlb.com/api/v1/stats/metrics](https://statsapi.mlb.com/api/v1/stats/metrics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `stats` | `stats` |  |  | `Y` | stats query parameter. |
| `group` | `group` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `gameType` | `game_type` |  |  | `Y` | gameType query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `startDate` | `start_date` |  |  | `Y` | startDate query parameter. |
| `endDate` | `end_date` |  |  | `Y` | endDate query parameter. |
| `venueId` | `venue_id` |  |  | `Y` | venueId query parameter. |
| `minOccurrences` | `min_occurrences` |  |  | `Y` | minOccurrences query parameter. |
| `percentile` | `percentile` |  |  | `Y` | percentile query parameter. |
| `personId` | `person_id` |  |  | `Y` | personId query parameter. |
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `offset` | `offset` |  |  | `Y` | offset query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

View Statcast stats.

### Example

```python
mlb_api_stats_metrics()
```

_Last validated n/a._

## `mlb_api_teams_history`

View historical records for a list of teams.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/history`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/history?teamIds=147](https://statsapi.mlb.com/api/v1/teams/history?teamIds=147)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamIds` | `team_ids` |  |  | `Y` | teamIds query parameter. |
| `startSeason` | `start_season` |  |  | `Y` | startSeason query parameter. |
| `endSeason` | `end_season` |  |  | `Y` | endSeason query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `all_star_status` | character |  |
| `id` | integer |  |
| `name` | character |  |
| `link` | character |  |
| `season` | integer |  |
| `team_code` | character |  |
| `file_code` | character |  |
| `abbreviation` | character |  |
| `team_name` | character |  |
| `location_name` | character |  |
| `first_year_of_play` | character |  |
| `short_name` | character |  |
| `franchise_name` | character |  |
| `club_name` | character |  |
| `active` | logical |  |
| `venue_id` | integer |  |
| `venue_name` | character |  |
| `venue_link` | character |  |
| `spring_venue_id` | double |  |
| `spring_venue_link` | character |  |
| `league_id` | integer |  |
| `league_name` | character |  |
| `league_link` | character |  |
| `sport_id` | integer |  |
| `sport_link` | character |  |
| `sport_name` | character |  |

### Example

```python
mlb_api_teams_history(team_ids='147')
```

_Last validated n/a._

## `mlb_api_teams_stats`

View team stats.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/stats`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/stats?season=2023&sportIds=1&group=hitting&stats=season](https://statsapi.mlb.com/api/v1/teams/stats?season=2023&sportIds=1&group=hitting&stats=season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `sportIds` | `sport_ids` |  |  | `Y` | sportIds query parameter. |
| `group` | `stat_group` |  |  | `Y` | group query parameter. |
| `gameType` | `game_type` |  |  | `Y` | gameType query parameter. |
| `stats` | `stats` |  |  | `Y` | stats query parameter. |
| `order` | `order` |  |  | `Y` | order query parameter. |
| `sortStat` | `sort_stat` |  |  | `Y` | sortStat query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `total_splits` | integer |  |
| `exemptions` | character |  |
| `splits` | character |  |
| `splits_tied_with_offset` | character |  |
| `splits_tied_with_limit` | character |  |
| `type_display_name` | character |  |
| `group_display_name` | character |  |

### Example

```python
mlb_api_teams_stats(season='2023', sport_ids='1', stat_group='hitting', stats='season')
```

_Last validated n/a._

## `mlb_api_teams_stats_leaders`

View leaders for a statistic.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/stats/leaders`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/stats/leaders?leaderCategories=homeRuns&season=2023](https://statsapi.mlb.com/api/v1/teams/stats/leaders?leaderCategories=homeRuns&season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leaderCategories` | `leader_categories` |  |  | `Y` | leaderCategories query parameter. |
| `sitCodes` | `sit_codes` |  |  | `Y` | sitCodes query parameter. |
| `gameTypes` | `game_types` |  |  | `Y` | gameTypes query parameter. |
| `statGroup` | `stat_group` |  |  | `Y` | statGroup query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `leagueIds` | `league_ids` |  |  | `Y` | leagueIds query parameter. |
| `startDate` | `start_date` |  |  | `Y` | startDate query parameter. |
| `endDate` | `end_date` |  |  | `Y` | endDate query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `leader_category` | character |  |
| `season` | character |  |
| `leaders` | character |  |
| `stat_group` | character |  |
| `total_splits` | integer |  |
| `game_type_id` | character |  |
| `game_type_description` | character |  |

### Example

```python
mlb_api_teams_stats_leaders(leader_categories='homeRuns', season='2023')
```

_Last validated n/a._

## `mlb_api_team_coaches`

View biographical  information on all coaches for a given club.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/{team_id}/coaches`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/147/coaches?season=2023](https://statsapi.mlb.com/api/v1/teams/147/coaches?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `jersey_number` | character |  |
| `job` | character |  |
| `job_id` | character |  |
| `title` | character |  |
| `person_id` | integer |  |
| `person_full_name` | character |  |
| `person_link` | character |  |

### Example

```python
mlb_api_team_coaches(team_id=147, season='2023')
```

_Last validated n/a._

## `mlb_api_team_personnel`

View biographical  information on all personnel for a given club.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/{team_id}/personnel`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/147/personnel](https://statsapi.mlb.com/api/v1/teams/147/personnel)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `jersey_number` | character |  |
| `job` | character |  |
| `job_id` | character |  |
| `title` | character |  |
| `person_id` | integer |  |
| `person_full_name` | character |  |
| `person_link` | character |  |

### Example

```python
mlb_api_team_personnel(team_id=147)
```

_Last validated n/a._

## `mlb_api_team_roster_type`

View biographical and statistical information for a club's roster based on roster type.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/{team_id}/roster/{roster_type}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/147/roster/active?season=2023](https://statsapi.mlb.com/api/v1/teams/147/roster/active?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `roster_type` | `roster_type` |  | `Y` |  | roster_type path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `jersey_number` | character |  |
| `person_id` | integer |  |
| `person_full_name` | character |  |
| `person_link` | character |  |
| `position_code` | character |  |
| `position_name` | character |  |
| `position_type` | character |  |
| `position_abbreviation` | character |  |
| `status_code` | character |  |
| `status_description` | character |  |

### Example

```python
mlb_api_team_roster_type(team_id=147, roster_type='active', season='2023')
```

_Last validated n/a._
