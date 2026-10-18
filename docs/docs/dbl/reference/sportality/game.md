---
title: "DBL — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) — Game"
sidebar_label: "Game"
sidebar_position: 2
description: "DBL — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) — Game — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# DBL — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) — Game

## sportality_game_header

Game header: Genius match id (extId), arena, state, teams, scores.

**Endpoint URL:** `GET https://www.basketligaen.dk/api/sports-v2/game-info/{game_uuid}`

**Valid URL:** [https://www.basketligaen.dk/api/sports-v2/game-info/c4hh5nig6t](https://www.basketligaen.dk/api/sports-v2/game-info/c4hh5nig6t)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_uuid` | `game_uuid` |  | `Y` |  | Game uuid: `gameInfo[].uuid` of the schedule (10 lowercase letters and digits). Key space: platform game uuids; the Genius match id is `extId` / `gameSourceId` in the body. |

### Returns {#sportality_game_header-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `instance_context_site_team_is_in_game` | logical | Whether the requesting site's own team plays in the game. |
| `instance_context_site_team_is_home_team` | logical | Whether the requesting site's own team is the home team. |
| `instance_context_site_team_is_away_team` | logical | Whether the requesting site's own team is the away team. |
| `game_info_game_uuid` | character | Game uuid (platform key space). |
| `game_info_ext_id` | integer | Genius match id (Int64; the same number as gameday game_source_id). |
| `game_info_start_date_time` | character | Scheduled start, ISO 8601 UTC with milliseconds. |
| `game_info_arena_name` | character | Name of the arena (venue). |
| `game_info_state` | character | Game state, underscore form: post_game once played. Values: `post_game` = played (sports-v2 game-info). |
| `game_info_overtime` | logical | Whether the game went to overtime. |
| `game_info_shootout` | character | Unobserved: null in every sample and every full body. When filled: Shootout flag (null on every game seen). |
| `game_info_series_code` | character | Series code (DBL). |
| `game_info_series_name` | character | Series name (Basketligaen). |
| `game_info_round_number` | integer | Round number (0). |
| `game_info_round_label` | character | Round label (''). |
| `game_info_series_display_name` | character | Series display name (Basketligaen). |
| `home_team_names_code` | character | Three-letter team code (HOL, RAN ...); codes changed between seasons for some clubs (BAK in 2026, BKA in 2025 for Bakken Bears). |
| `home_team_names_short` | character | Short name of the team. |
| `home_team_names_long` | character | Long name of the team. |
| `home_team_names_full` | character | Full name of the team. |
| `home_team_names_code_site` | character | Team code as the club's own site shows it. |
| `home_team_names_short_site` | character | Short team name as the club's own site shows it. |
| `home_team_names_full_site` | character | Full team name as the club's own site shows it. |
| `home_team_names_long_site` | character | Long team name as the club's own site shows it. |
| `home_team_uuid` | character | Home team uuid (platform key space); joins /sports-v2/teams/{team_uuid}. |
| `home_team_instance_id` | character | Site instance id of the home club (platform key space). |
| `home_team_founded_on` | character | Year the club was founded ('' when unknown). |
| `home_team_icon` | character | Home team logo URL. |
| `home_team_score` | integer | Points scored by the home team. |
| `away_team_names_code` | character | Three-letter team code (HOL, RAN ...); codes changed between seasons for some clubs (BAK in 2026, BKA in 2025 for Bakken Bears). |
| `away_team_names_short` | character | Short name of the team. |
| `away_team_names_long` | character | Long name of the team. |
| `away_team_names_full` | character | Full name of the team. |
| `away_team_names_code_site` | character | Team code as the club's own site shows it. |
| `away_team_names_short_site` | character | Short team name as the club's own site shows it. |
| `away_team_names_full_site` | character | Full team name as the club's own site shows it. |
| `away_team_names_long_site` | character | Long team name as the club's own site shows it. |
| `away_team_uuid` | character | Away team uuid (platform key space); joins /sports-v2/teams/{team_uuid}. |
| `away_team_instance_id` | character | Site instance id of the away club (platform key space). |
| `away_team_founded_on` | character | Year the club was founded ('' when unknown). |
| `away_team_icon` | character | Away team logo URL. |
| `away_team_score` | integer | Points scored by the away team. |
| `ssgt_uuid` | character | ssgt uuid (platform key space: one season + series + game type); joins /statistics-v2/layout-info seasonSeriesGameTypeTeams[].ssgtUuid and every route keyed by ssgt. |
| `series_uuid` | character | Series uuid (platform key space; Basketligaen = m1ay3mpa4t, the only series on this host). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_game_header-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `sportsdataverse.errors.AssetFetchError` | an error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_game_header-example}

```python
sportality_game_header(game_uuid='c4hh5nig6t')
```

_Last validated n/a._

## sportality_game_overview

Score, state, clock and the Genius ids of one game (2023/24 on).

**Endpoint URL:** `GET https://www.basketligaen.dk/api/gameday/game-overview/{game_uuid}`

**Valid URL:** [https://www.basketligaen.dk/api/gameday/game-overview/c4hh5nig6t](https://www.basketligaen.dk/api/gameday/game-overview/c4hh5nig6t)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_uuid` | `game_uuid` |  | `Y` |  | Game uuid: `gameInfo[].uuid` of the schedule (10 lowercase letters and digits). Key space: platform game uuids; the Genius match id is `extId` / `gameSourceId` in the body. |

### Returns {#sportality_game_overview-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_uuid` | character | Platform game uuid (c4hh5nig6t); every gameday route takes it. |
| `home_team_game_source_id` | integer | Genius match id (Genius key space; integer here). |
| `home_team_game_id` | integer | Genius match id (Genius key space; the same number as gameSourceId). |
| `home_team_team_id` | integer | Genius team id of the home team (Genius key space; Int64). |
| `home_team_team_name` | character | Name of the home team. |
| `home_team_team_code` | character | Home team code as Genius spells it (mixed case, 'Hol'). |
| `home_team_team_number` | integer | Team number in the game; home is 1. |
| `home_team_place` | character | home. Values: `home` = home team. |
| `home_team_instance_id` | character | Site instance id of the home club (platform key space). |
| `home_team_source` | character | Feed that produced the row (genius-basketball-stream-reader). |
| `home_team_game_uuid` | character | Game uuid (platform key space). |
| `away_team_game_source_id` | integer | Genius match id (Genius key space; integer here). |
| `away_team_game_id` | integer | Genius match id (Genius key space; the same number as gameSourceId). |
| `away_team_team_id` | integer | Genius team id of the away team (Genius key space; Int64). |
| `away_team_team_name` | character | Name of the away team. |
| `away_team_team_code` | character | Away team code as Genius spells it. |
| `away_team_team_number` | integer | Team number in the game; away is 2. |
| `away_team_place` | character | away. Values: `away` = away team. |
| `away_team_instance_id` | character | Site instance id of the away club (platform key space). |
| `away_team_source` | character | Feed that produced the row (genius-basketball-stream-reader). |
| `away_team_game_uuid` | character | Game uuid (platform key space). |
| `home_goals` | integer | Points scored by the home team. |
| `away_goals` | integer | Points scored by the away team. |
| `state` | character | Game state. Schedule and day views: pre-game or post-game (hyphen). sports-v2 game-info: post_game (underscore). Gameday overview: GameEnded. Values: `GameEnded` = played (gameday overview). |
| `time_game_id` | integer | Genius match id (Genius key space). |
| `time_period` | integer | Period number the clock stands in (1-4 regulation, 5+ overtime). |
| `time_period_time` | character | Clock remaining in the period, 'hh:mm:ss' ('00:00:00' once the period or game is over). |
| `time_instance_id` | character | Site instance that owns the feed (platform key space; dbl1_dbl). |
| `time_source` | character | Feed that produced the row (genius-basketball-stream-reader). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_game_overview-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `sportsdataverse.errors.AssetFetchError` | an error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_game_overview-example}

```python
sportality_game_overview(game_uuid='c4hh5nig6t')
```

_Last validated n/a._

## sportality_game_summary

Points per period, team comparison and best scorers (2023/24 on).

**Endpoint URL:** `GET https://www.basketligaen.dk/api/gameday/game-info/{game_uuid}`

**Valid URL:** [https://www.basketligaen.dk/api/gameday/game-info/c4hh5nig6t](https://www.basketligaen.dk/api/gameday/game-info/c4hh5nig6t)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_uuid` | `game_uuid` |  | `Y` |  | Game uuid: `gameInfo[].uuid` of the schedule (10 lowercase letters and digits). Key space: platform game uuids; the Genius match id is `extId` / `gameSourceId` in the body. |

### Returns {#sportality_game_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_head2_head` | character | Team comparison rows (2P, 3P, FT, rebounds, ...), home and away side by side. |
| `team_period_caption` | character | Column caption of the table (G = goals/points). Values: `G` = goals/points column. |
| `team_period_overtime` | logical | Whether the game went to overtime. |
| `team_period_shootout` | logical | Whether the game was decided by a shootout (false). |
| `team_period_stats` | character | One row per period. |
| `best_scorers` | character | The game's top scorers, one per side (or the two best). |
| `statistics_provider` | character | Stats provider of the data (genius-basketball = Genius Sports LiveStats). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_game_summary-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `sportsdataverse.errors.AssetFetchError` | an error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_game_summary-example}

```python
sportality_game_summary(game_uuid='c4hh5nig6t')
```

_Last validated n/a._
