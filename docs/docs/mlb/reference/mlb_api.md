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

## `mlb_api_conferences`

View all PCL conferences.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/conferences`

**Valid URL:** [https://statsapi.mlb.com/api/v1/conferences](https://statsapi.mlb.com/api/v1/conferences)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `conferenceId` | `conference_id` |  |  | `Y` |
| `season` | `season` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

View all PCL conferences.

### Example

```python
mlb_api_conferences()
```

_Last validated n/a._

## `mlb_api_conference`

View PCL conferences by conferenceId.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/conferences/{conference_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/conferences/100](https://statsapi.mlb.com/api/v1/conferences/100)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `conference_id` | `conference_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

View PCL conferences by conferenceId.

### Example

```python
mlb_api_conference(conference_id=100)
```

_Last validated n/a._

## `mlb_api_draft_latest`

View latest player drafted, endpoint best used when draft is currently open.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/draft/{year}/latest`

**Valid URL:** [https://statsapi.mlb.com/api/v1/draft/2023/latest](https://statsapi.mlb.com/api/v1/draft/2023/latest)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `year` | `year` |  | `Y` |  |

### Returns

View latest player drafted, endpoint best used when draft is currently open.

### Example

```python
mlb_api_draft_latest(year=2023)
```

_Last validated n/a._

## `mlb_api_game_timestamps`

Retrieve all of the play timecodes for a game in GUMBO feed.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live/timestamps`

**Valid URL:** [https://statsapi.mlb.com/api/v1.1/game/716390/feed/live/timestamps](https://statsapi.mlb.com/api/v1.1/game/716390/feed/live/timestamps)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_pk` | `game_pk` |  | `Y` |  |

### Returns

Retrieve all of the play timecodes for a game in GUMBO feed.

### Example

```python
mlb_api_game_timestamps(game_pk=716390)
```

_Last validated n/a._

## `mlb_api_game_changes`

View corrected non Statcast information for games

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/changes`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/changes](https://statsapi.mlb.com/api/v1/game/changes)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `updatedSince` | `updated_since` |  |  | `Y` |
| `sportId` | `sport_id` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

View corrected non Statcast information for games

### Example

```python
mlb_api_game_changes()
```

_Last validated n/a._

## `mlb_api_analytics_games`

View timestamps of most recent data corrections made to games.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/analytics/game`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/analytics/game](https://statsapi.mlb.com/api/v1/game/analytics/game)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `gameModeId` | `game_mode_id` |  |  | `Y` |
| `timecode` | `timecode` |  |  | `Y` |
| `limit` | `limit` |  |  | `Y` |
| `sortBy` | `sort_by` |  |  | `Y` |
| `isNonStatcast` | `is_non_statcast` |  |  | `Y` |
| `offset` | `offset` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `gameModeId` | `game_mode_id` |  |  | `Y` |
| `timecode` | `timecode` |  |  | `Y` |
| `limit` | `limit` |  |  | `Y` |
| `sortBy` | `sort_by` |  |  | `Y` |
| `isNonStatcast` | `is_non_statcast` |  |  | `Y` |
| `offset` | `offset` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_pk` | `game_pk` |  | `Y` |  |
| `gameModeId` | `game_mode_id` |  |  | `Y` |
| `updatedSince` | `updated_since` |  |  | `Y` |
| `isPitch` | `is_pitch` |  |  | `Y` |
| `isHit` | `is_hit` |  |  | `Y` |
| `isPickoff` | `is_pickoff` |  |  | `Y` |
| `hydrate` | `hydrate` |  |  | `Y` |
| `parsed/raw` | `parsed_raw` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_pk` | `game_pk` |  | `Y` |  |
| `guid` | `guid` |  | `Y` |  |
| `hydrate` | `hydrate` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_pk` | `game_pk` |  | `Y` |  |
| `guid` | `guid` |  | `Y` |  |
| `fields` | `fields` |  |  | `Y` |

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_pk` | `game_pk` |  | `Y` |  |
| `timecode` | `timecode` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_pk` | `game_pk` |  | `Y` |  |
| `startTimecode` | `start_timecode` |  |  | `Y` |
| `endTimecode` | `end_timecode` |  |  | `Y` |

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_pk` | `game_pk` |  | `Y` |  |

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

**Valid URL:** [https://statsapi.mlb.com/api/v1/gamePace](https://statsapi.mlb.com/api/v1/gamePace)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  |  | `Y` |
| `teamIds` | `team_ids` |  |  | `Y` |
| `leagueIds` | `league_ids` |  |  | `Y` |
| `leagueListId` | `league_list_id` |  |  | `Y` |
| `sportId` | `sport_id` |  |  | `Y` |
| `gameType` | `game_type` |  |  | `Y` |
| `startDate` | `start_date` |  |  | `Y` |
| `endDate` | `end_date` |  |  | `Y` |
| `venueIds` | `venue_ids` |  |  | `Y` |
| `orgType` | `org_type` |  |  | `Y` |
| `includeChildren` | `include_children` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

View time of game info.

### Example

```python
mlb_api_game_pace()
```

_Last validated n/a._

## `mlb_api_high_low`

View high/low stats by player or team.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/highLow/{org_type}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/highLow/player](https://statsapi.mlb.com/api/v1/highLow/player)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `org_type` | `org_type` |  | `Y` |  |
| `statGroup` | `stat_group` |  |  | `Y` |
| `sortStat` | `sort_stat` |  |  | `Y` |
| `season` | `season` |  |  | `Y` |
| `gameType` | `game_type` |  |  | `Y` |
| `teamId` | `team_id` |  |  | `Y` |
| `leagueId` | `league_id` |  |  | `Y` |
| `sportIds` | `sport_ids` |  |  | `Y` |
| `limit` | `limit` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

View high/low stats by player or team.

### Example

```python
mlb_api_high_low(org_type='player')
```

_Last validated n/a._

## `mlb_api_home_run_derby`

View a home run derby object based on gamePk.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/homeRunDerby/{game_pk}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/homeRunDerby/716390](https://statsapi.mlb.com/api/v1/homeRunDerby/716390)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_pk` | `game_pk` |  | `Y` |  |
| `fields` | `fields` |  |  | `Y` |

### Returns

View a home run derby object based on gamePk.

### Example

```python
mlb_api_home_run_derby(game_pk=716390)
```

_Last validated n/a._

## `mlb_api_home_run_derby_bracket`

View a home run derby object based on bracket.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/homeRunDerby/{game_pk}/bracket`

**Valid URL:** [https://statsapi.mlb.com/api/v1/homeRunDerby/716390/bracket](https://statsapi.mlb.com/api/v1/homeRunDerby/716390/bracket)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_pk` | `game_pk` |  | `Y` |  |
| `fields` | `fields` |  |  | `Y` |

### Returns

View a home run derby object based on bracket.

### Example

```python
mlb_api_home_run_derby_bracket(game_pk=716390)
```

_Last validated n/a._

## `mlb_api_home_run_derby_pool`

View a home run derby object based on pool.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/homeRunDerby/{game_pk}/pool`

**Valid URL:** [https://statsapi.mlb.com/api/v1/homeRunDerby/716390/pool](https://statsapi.mlb.com/api/v1/homeRunDerby/716390/pool)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_pk` | `game_pk` |  | `Y` |  |
| `fields` | `fields` |  |  | `Y` |

### Returns

View a home run derby object based on pool.

### Example

```python
mlb_api_home_run_derby_pool(game_pk=716390)
```

_Last validated n/a._

## `mlb_api_all_star_ballot`

View All-Star Ballots per league.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/league/{league_id}/allStarBallot`

**Valid URL:** [https://statsapi.mlb.com/api/v1/league/103/allStarBallot](https://statsapi.mlb.com/api/v1/league/103/allStarBallot)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `league_id` | `league_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

View All-Star Ballots per league.

### Example

```python
mlb_api_all_star_ballot(league_id=103)
```

_Last validated n/a._

## `mlb_api_all_star_write_ins`

View All-Star Write-ins per league.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/league/{league_id}/allStarWriteIns`

**Valid URL:** [https://statsapi.mlb.com/api/v1/league/103/allStarWriteIns](https://statsapi.mlb.com/api/v1/league/103/allStarWriteIns)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `league_id` | `league_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

View All-Star Write-ins per league.

### Example

```python
mlb_api_all_star_write_ins(league_id=103)
```

_Last validated n/a._

## `mlb_api_all_star_final_vote`

View All-Star Final Vote per league.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/league/{league_id}/allStarFinalVote`

**Valid URL:** [https://statsapi.mlb.com/api/v1/league/103/allStarFinalVote](https://statsapi.mlb.com/api/v1/league/103/allStarFinalVote)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `league_id` | `league_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

View All-Star Final Vote per league.

### Example

```python
mlb_api_all_star_final_vote(league_id=103)
```

_Last validated n/a._

## `mlb_api_free_agents`

View biographical information and stats for Free Agents.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/people/freeAgents`

**Valid URL:** [https://statsapi.mlb.com/api/v1/people/freeAgents](https://statsapi.mlb.com/api/v1/people/freeAgents)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `seasonId` | `season_id` |  |  | `Y` |
| `order` | `order` |  |  | `Y` |
| `hydrate` | `hydrate` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

View biographical information and stats for Free Agents.

### Example

```python
mlb_api_free_agents()
```

_Last validated n/a._

## `mlb_api_jobs`

View directory by jobType.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/jobs`

**Valid URL:** [https://statsapi.mlb.com/api/v1/jobs](https://statsapi.mlb.com/api/v1/jobs)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `jobType` | `job_type` |  |  | `Y` |
| `sportId` | `sport_id` |  |  | `Y` |
| `date` | `date` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

View directory by jobType.

### Example

```python
mlb_api_jobs()
```

_Last validated n/a._

## `mlb_api_datacasters`

View datacasters directory.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/jobs/datacasters`

**Valid URL:** [https://statsapi.mlb.com/api/v1/jobs/datacasters](https://statsapi.mlb.com/api/v1/jobs/datacasters)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `sportId` | `sport_id` |  |  | `Y` |
| `date` | `date` |  |  | `Y` |
| `hydrate` | `hydrate` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

View datacasters directory.

### Example

```python
mlb_api_datacasters()
```

_Last validated n/a._

## `mlb_api_official_scorers`

View official scorer directory.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/jobs/officialScorers`

**Valid URL:** [https://statsapi.mlb.com/api/v1/jobs/officialScorers](https://statsapi.mlb.com/api/v1/jobs/officialScorers)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `sportId` | `sport_id` |  |  | `Y` |
| `date` | `date` |  |  | `Y` |
| `hydrate` | `hydrate` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

View official scorer directory.

### Example

```python
mlb_api_official_scorers()
```

_Last validated n/a._

## `mlb_api_umpire_games`

Get umpires and associated game for umpireId.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/jobs/umpires/games/{umpire_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/jobs/umpires/games/427044](https://statsapi.mlb.com/api/v1/jobs/umpires/games/427044)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `umpire_id` | `umpire_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `hydrate` | `hydrate` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

Get umpires and associated game for umpireId.

### Example

```python
mlb_api_umpire_games(umpire_id=427044)
```

_Last validated n/a._

## `mlb_api_schedule_tied`

View tied game schedule info.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/schedule/games/tied`

**Valid URL:** [https://statsapi.mlb.com/api/v1/schedule/games/tied](https://statsapi.mlb.com/api/v1/schedule/games/tied)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `gameTypes` | `game_types` |  |  | `Y` |
| `season` | `season` |  |  | `Y` |
| `hydrate` | `hydrate` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

View tied game schedule info.

### Example

```python
mlb_api_schedule_tied()
```

_Last validated n/a._

## `mlb_api_schedule_postseason_series`

View schedule info for postseason based on series.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/schedule/postseason/series`

**Valid URL:** [https://statsapi.mlb.com/api/v1/schedule/postseason/series](https://statsapi.mlb.com/api/v1/schedule/postseason/series)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `gameTypes` | `game_types` |  |  | `Y` |
| `seriesNumber` | `series_number` |  |  | `Y` |
| `teamId` | `team_id` |  |  | `Y` |
| `sportId` | `sport_id` |  |  | `Y` |
| `season` | `season` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

View schedule info for postseason based on series.

### Example

```python
mlb_api_schedule_postseason_series()
```

_Last validated n/a._

## `mlb_api_schedule_postseason_tunein`

View schedule info for the tuneIn application.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/schedule/postseason/tuneIn`

**Valid URL:** [https://statsapi.mlb.com/api/v1/schedule/postseason/tuneIn](https://statsapi.mlb.com/api/v1/schedule/postseason/tuneIn)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `teamId` | `team_id` |  |  | `Y` |
| `sportId` | `sport_id` |  |  | `Y` |
| `season` | `season` |  |  | `Y` |
| `hydrate` | `hydrate` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

View schedule info for the tuneIn application.

### Example

```python
mlb_api_schedule_postseason_tunein()
```

_Last validated n/a._

## `mlb_api_seasons_all`

View information for all seasons based on id.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/seasons/all`

**Valid URL:** [https://statsapi.mlb.com/api/v1/seasons/all](https://statsapi.mlb.com/api/v1/seasons/all)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `divisionId` | `division_id` |  |  | `Y` |
| `leagueId` | `league_id` |  |  | `Y` |
| `withGameTypeDates` | `with_game_type_dates` |  |  | `Y` |
| `sportId` | `sport_id` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

View information for all seasons based on id.

### Example

```python
mlb_api_seasons_all()
```

_Last validated n/a._

## `mlb_api_sport`

View information for any given sportId.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/sports/{sport_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/sports/1](https://statsapi.mlb.com/api/v1/sports/1)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `sport_id` | `sport_id` |  | `Y` |  |
| `fields` | `fields` |  |  | `Y` |

### Returns

View information for any given sportId.

### Example

```python
mlb_api_sport(sport_id=1)
```

_Last validated n/a._

## `mlb_api_stats_metrics`

View Statcast stats.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/stats/metrics`

**Valid URL:** [https://statsapi.mlb.com/api/v1/stats/metrics](https://statsapi.mlb.com/api/v1/stats/metrics)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `stats` | `stats` |  |  | `Y` |
| `group` | `group` |  |  | `Y` |
| `gameType` | `game_type` |  |  | `Y` |
| `season` | `season` |  |  | `Y` |
| `startDate` | `start_date` |  |  | `Y` |
| `endDate` | `end_date` |  |  | `Y` |
| `venueId` | `venue_id` |  |  | `Y` |
| `minOccurrences` | `min_occurrences` |  |  | `Y` |
| `percentile` | `percentile` |  |  | `Y` |
| `personId` | `person_id` |  |  | `Y` |
| `teamId` | `team_id` |  |  | `Y` |
| `limit` | `limit` |  |  | `Y` |
| `offset` | `offset` |  |  | `Y` |
| `hydrate` | `hydrate` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

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

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/history](https://statsapi.mlb.com/api/v1/teams/history)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `teamIds` | `team_ids` |  |  | `Y` |
| `startSeason` | `start_season` |  |  | `Y` |
| `endSeason` | `end_season` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

View historical records for a list of teams.

### Example

```python
mlb_api_teams_history()
```

_Last validated n/a._

## `mlb_api_teams_stats`

View team stats.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/stats`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/stats](https://statsapi.mlb.com/api/v1/teams/stats)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  |  | `Y` |
| `sportIds` | `sport_ids` |  |  | `Y` |
| `statGroup` | `stat_group` |  |  | `Y` |
| `gameType` | `game_type` |  |  | `Y` |
| `stats` | `stats` |  |  | `Y` |
| `order` | `order` |  |  | `Y` |
| `sortStat` | `sort_stat` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

View team stats.

### Example

```python
mlb_api_teams_stats()
```

_Last validated n/a._

## `mlb_api_teams_stats_leaders`

View leaders for a statistic.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/stats/leaders`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/stats/leaders](https://statsapi.mlb.com/api/v1/teams/stats/leaders)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `leaderCategories` | `leader_categories` |  |  | `Y` |
| `sitCodes` | `sit_codes` |  |  | `Y` |
| `gameTypes` | `game_types` |  |  | `Y` |
| `statGroup` | `stat_group` |  |  | `Y` |
| `season` | `season` |  |  | `Y` |
| `leagueIds` | `league_ids` |  |  | `Y` |
| `startDate` | `start_date` |  |  | `Y` |
| `endDate` | `end_date` |  |  | `Y` |
| `sportId` | `sport_id` |  |  | `Y` |
| `hydrate` | `hydrate` |  |  | `Y` |
| `limit` | `limit` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

View leaders for a statistic.

### Example

```python
mlb_api_teams_stats_leaders()
```

_Last validated n/a._

## `mlb_api_team_coaches`

View biographical  information on all coaches for a given club.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/{team_id}/coaches`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/10/coaches](https://statsapi.mlb.com/api/v1/teams/10/coaches)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `date` | `date` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

View biographical  information on all coaches for a given club.

### Example

```python
mlb_api_team_coaches(team_id=10)
```

_Last validated n/a._

## `mlb_api_team_personnel`

View biographical  information on all personnel for a given club.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/{team_id}/personnel`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/10/personnel](https://statsapi.mlb.com/api/v1/teams/10/personnel)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |
| `date` | `date` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

View biographical  information on all personnel for a given club.

### Example

```python
mlb_api_team_personnel(team_id=10)
```

_Last validated n/a._

## `mlb_api_team_roster_type`

View biographical and statistical information for a club's roster based on roster type.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/teams/{team_id}/roster/{roster_type}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/teams/10/roster/active](https://statsapi.mlb.com/api/v1/teams/10/roster/active)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |
| `roster_type` | `roster_type` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `date` | `date` |  |  | `Y` |
| `hydrate` | `hydrate` |  |  | `Y` |
| `fields` | `fields` |  |  | `Y` |

### Returns

View biographical and statistical information for a club's roster based on roster type.

### Example

```python
mlb_api_team_roster_type(team_id=10, roster_type='active')
```

_Last validated n/a._
