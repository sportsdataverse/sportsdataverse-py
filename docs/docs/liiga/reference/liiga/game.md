---
title: "Liiga — Liiga API v2 (liiga.fi/api/v2, Finland) — Game"
sidebar_label: "Game"
sidebar_position: 1
description: "Liiga — Liiga API v2 (liiga.fi/api/v2, Finland) — Game — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# Liiga — Liiga API v2 (liiga.fi/api/v2, Finland) — Game

## liiga_game_goals

One game's goals: scorer, assists, strength codes and skaters on ice.

**Endpoint URL:** `GET https://liiga.fi/api/v2/games/{season}/{game_id}`

**Valid URL:** [https://liiga.fi/api/v2/games/2026/1](https://liiga.fi/api/v2/games/2026/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26). Game ids repeat across seasons, so a game is (season, game_id). |
| `game_id` | `game_id` |  | `Y` |  | Game id (game_id in liiga_schedule or liiga_games), e.g. 1; unique only within a season. |

### Returns {#liiga_game_goals-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025-26). |
| `game_id` | Int64 | Game id, unique only within a season: key a game as (season, game_id). |
| `side` | String | Which team of the game the row belongs to: home or away. |
| `team_id` | Int64 | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | String | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `log_time` | String | Wall-clock time the event was logged, ISO 8601 UTC. |
| `winning_goal` | Boolean | Game-winning goal. |
| `game_time` | Int64 | Seconds from the start of the game. Regular season: 3900 (65:00) = the end of the 5-minute overtime; playoff overtime runs past it. |
| `period` | Int64 | Period number: 1-3 are regulation, 4 and up overtime (or the shootout). |
| `event_id` | Int64 | Event id within the game. |
| `home_team_score` | Int64 | Home score after the event. |
| `away_team_score` | Int64 | Away score after the event. |
| `goals_so_far_in_season` | Int64 | Scorer's season goal count after this goal. |
| `video_clip_url` | String | URL of the goal's video clip; null in every capture. |
| `video_clip_playlist_url` | String | URL of the goal's video clip playlist; null in every capture. |
| `video_thumbnail_url` | String | URL of the goal video's thumbnail image; null in every capture. |
| `scorer_player_id` | Int64 | Scorer's player id. |
| `scorer_first_name` | String | Scorer's first name. |
| `scorer_last_name` | String | Scorer's last name. |
| `assist1_player_id` | Int64 | First assist's player id. |
| `assist1_first_name` | String | First assist's first name. |
| `assist1_last_name` | String | First assist's last name. |
| `assist1_season_assists` | Int64 | First assister's season assist count after this goal. |
| `assist2_player_id` | Int64 | Second assist's player id. |
| `assist2_first_name` | String | Second assist's first name. |
| `assist2_last_name` | String | Second assist's last name. |
| `assist2_season_assists` | Int64 | Second assister's season assist count after this goal. |
| `goal_types` | String | Goal type codes, comma-separated Finnish abbreviations: YV power play, AV short-handed, TM empty net, RL penalty shot (others undocumented). |
| `plus_jerseys` | String | Jersey NUMBERS of this team's skaters on ice for the goal, space-separated. |
| `minus_jerseys` | String | Jersey NUMBERS of the other team's skaters on ice for the goal, space-separated. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#liiga_game_goals-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liiga has nothing for the request (an unknown game, a tournament not played that season, a shot map before 2016). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#liiga_game_goals-example}

```python
liiga_game_goals(game_id=1, season=2026)
```

## liiga_game_penalties

One game's penalties, both teams: player, fault, minutes, start and end.

**Endpoint URL:** `GET https://liiga.fi/api/v2/games/{season}/{game_id}`

**Valid URL:** [https://liiga.fi/api/v2/games/2026/1](https://liiga.fi/api/v2/games/2026/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26). Game ids repeat across seasons, so a game is (season, game_id). |
| `game_id` | `game_id` |  | `Y` |  | Game id (game_id in liiga_schedule or liiga_games), e.g. 1; unique only within a season. |

### Returns {#liiga_game_penalties-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025-26). |
| `game_id` | Int64 | Game id, unique only within a season: key a game as (season, game_id). |
| `side` | String | Which team of the game the row belongs to: home or away. |
| `team_id` | Int64 | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | String | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `player_id` | Int64 | Player id; the same number in lineups, rosters, stat tables and the profile (fihaId). |
| `sufferer_player_id` | Int64 | Player who served the penalty. |
| `event_id` | Int64 | Event id within the game. |
| `log_time` | String | Wall-clock time the event was logged, ISO 8601 UTC. |
| `game_time` | Int64 | Seconds from the start of the game. Regular season: 3900 (65:00) = the end of the 5-minute overtime; playoff overtime runs past it. |
| `period` | Int64 | Period number: 1-3 are regulation, 4 and up overtime (or the shootout). |
| `penalty_begintime` | Int64 | Penalty start, seconds from game start. |
| `penalty_endtime` | Int64 | Penalty end, seconds from game start. |
| `penalty_fault_name` | String | Infraction, in Finnish. |
| `penalty_fault_type` | String | Infraction code. |
| `penalty_info` | String | Extra penalty code ('' or a letter such as K; meaning unverified). |
| `penalty_minutes` | Int64 | Length of the penalty in minutes (2, 5, 10, 20, ...). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#liiga_game_penalties-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liiga has nothing for the request (an unknown game, a tournament not played that season, a shot map before 2016). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#liiga_game_penalties-example}

```python
liiga_game_penalties(game_id=1, season=2026)
```

## liiga_game_shootout

One game's shootout attempts: shooter, goalkeeper and result (none for a game without one).

**Endpoint URL:** `GET https://liiga.fi/api/v2/games/{season}/{game_id}`

**Valid URL:** [https://liiga.fi/api/v2/games/2026/1](https://liiga.fi/api/v2/games/2026/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26). Game ids repeat across seasons, so a game is (season, game_id). |
| `game_id` | `game_id` |  | `Y` |  | Game id (game_id in liiga_schedule or liiga_games), e.g. 1; unique only within a season. |

### Returns {#liiga_game_shootout-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025-26). |
| `game_id` | Int64 | Game id, unique only within a season: key a game as (season, game_id). |
| `shooting_player_id` | Int64 | Shooter's player id. |
| `blocking_player_id` | Int64 | Goalkeeper's player id. |
| `shot_number` | Int64 | Attempt number in the shootout. |
| `shooting_team_id` | Int64 | Team id of the shooter's team (the number of the `<num>:<slug>` key). |
| `shooting_team_slug` | String | Slug of the shooter's team (e.g. hpk). |
| `blocking_team_id` | Int64 | Goalkeeper's team id. |
| `blocking_team_slug` | String | Goalkeeper's team slug. |
| `goal_scored` | Boolean | The attempt scored. |
| `winning_goal` | Boolean | Game-winning goal. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#liiga_game_shootout-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liiga has nothing for the request (an unknown game, a tournament not played that season, a shot map before 2016). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#liiga_game_shootout-example}

```python
liiga_game_shootout(game_id=1, season=2026)
```

## liiga_game_lineups

One game's lineups, both teams: line, role, jersey, captaincy and bio.

**Endpoint URL:** `GET https://liiga.fi/api/v2/games/{season}/{game_id}`

**Valid URL:** [https://liiga.fi/api/v2/games/2026/1](https://liiga.fi/api/v2/games/2026/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26). Game ids repeat across seasons, so a game is (season, game_id). |
| `game_id` | `game_id` |  | `Y` |  | Game id (game_id in liiga_schedule or liiga_games), e.g. 1; unique only within a season. |

### Returns {#liiga_game_lineups-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025-26). |
| `game_id` | Int64 | Game id, unique only within a season: key a game as (season, game_id). |
| `side` | String | Which team of the game the row belongs to: home or away. |
| `player_id` | Int64 | Player id; the same number in lineups, rosters, stat tables and the profile (fihaId). |
| `team_id` | Int64 | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | String | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `team_name` | String | Team's display name, e.g. HPK or Tappara. |
| `line` | Int64 | Line number 1-4; null for goalkeepers and extras. |
| `country_of_birth` | String | Country of birth, 3-letter code. |
| `place_of_birth` | String | Player's place of birth (town or city). |
| `date_of_birth` | String | Date of birth, YYYY-MM-DD. |
| `nationality` | String | Nationality, 3-letter code. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `role` | String | Position: CENTER, LEFT_WING, RIGHT_WING, LEFT_DEFENSEMAN, RIGHT_DEFENSEMAN, SEVENTH_DEFENSEMAN, GOALIE. |
| `role_code` | String | Position code, Finnish: KH = centre, VL/OL = left/right wing, VP/OP = left/right defence, 7. P = seventh defenceman, MV = goalkeeper. |
| `handedness` | String | Shoots LEFT or RIGHT. |
| `height` | Int64 | Player's height in centimetres. |
| `weight` | Int64 | Player's weight in kilograms. |
| `captain` | Boolean | True when the player is the team captain (C). |
| `rookie` | Boolean | True when Liiga flags the player as a rookie. |
| `alternate_captain` | Boolean | True when the player is an alternate captain (A). |
| `jersey` | Int64 | Jersey (sweater) number the player wore. |
| `picture_url` | String | URL of the player's photo. |
| `injured` | Boolean | True when the player is listed as injured. |
| `suspended` | Boolean | True when the player is listed as suspended. |
| `removed` | Boolean | Removed from the roster. |
| `awards` | String | Player awards of the game (stars). |
| `sponsors` | String | Player's sponsors; null in every capture (unverified). |
| `extra_description` | String | Extra profile text for the player; null in every capture (meaning unverified). |
| `extra_strengths` | String | Extra profile field for the player's strengths; null in every capture (unverified). |
| `extra_rookie_picture` | Boolean | Flag for an extra rookie picture on the player's profile (meaning unverified). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#liiga_game_lineups-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liiga has nothing for the request (an unknown game, a tournament not played that season, a shot map before 2016). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#liiga_game_lineups-example}

```python
liiga_game_lineups(game_id=1, season=2026)
```

## liiga_game_players_stats

One game's lineups with each player's all-time, season, season-in-team and versus-team stats.

**Endpoint URL:** `GET https://liiga.fi/api/v2/games/{season}/{game_id}`

**Valid URL:** [https://liiga.fi/api/v2/games/2026/1?dataType=playersWithStats](https://liiga.fi/api/v2/games/2026/1?dataType=playersWithStats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26). Game ids repeat across seasons, so a game is (season, game_id). |
| `game_id` | `game_id` |  | `Y` |  | Game id (game_id in liiga_schedule or liiga_games), e.g. 1; unique only within a season. |

### Returns {#liiga_game_players_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025-26). |
| `game_id` | Int64 | Game id, unique only within a season: key a game as (season, game_id). |
| `side` | String | Which team of the game the row belongs to: home or away. |
| `player_id` | Int64 | Player id; the same number in lineups, rosters, stat tables and the profile (fihaId). |
| `team_id` | Int64 | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | String | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `team_name` | String | Team's display name, e.g. HPK or Tappara. |
| `line` | Int64 | Line number 1-4; null for goalkeepers and extras. |
| `country_of_birth` | String | Country of birth, 3-letter code. |
| `place_of_birth` | String | Player's place of birth (town or city). |
| `date_of_birth` | String | Date of birth, YYYY-MM-DD. |
| `nationality` | String | Nationality, 3-letter code. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `role` | String | Position: CENTER, LEFT_WING, RIGHT_WING, LEFT_DEFENSEMAN, RIGHT_DEFENSEMAN, SEVENTH_DEFENSEMAN, GOALIE. |
| `role_code` | String | Position code, Finnish: KH = centre, VL/OL = left/right wing, VP/OP = left/right defence, 7. P = seventh defenceman, MV = goalkeeper. |
| `handedness` | String | Shoots LEFT or RIGHT. |
| `height` | Int64 | Player's height in centimetres. |
| `weight` | Int64 | Player's weight in kilograms. |
| `captain` | Boolean | True when the player is the team captain (C). |
| `rookie` | Boolean | True when Liiga flags the player as a rookie. |
| `alternate_captain` | Boolean | True when the player is an alternate captain (A). |
| `jersey` | Int64 | Jersey (sweater) number the player wore. |
| `picture_url` | String | URL of the player's photo. |
| `injured` | Boolean | True when the player is listed as injured. |
| `suspended` | Boolean | True when the player is listed as suspended. |
| `removed` | Boolean | Removed from the roster. |
| `awards` | String | Player awards of the game (stars). |
| `sponsors` | String | Player's sponsors; null in every capture (unverified). |
| `extra_description` | String | Extra profile text for the player; null in every capture (meaning unverified). |
| `extra_strengths` | String | Extra profile field for the player's strengths; null in every capture (unverified). |
| `extra_rookie_picture` | Boolean | Flag for an extra rookie picture on the player's profile (meaning unverified). |
| `all_time_stats_games` | Int64 | Games (player, all-time). |
| `all_time_stats_goals` | Int64 | Goals (player, all-time). |
| `all_time_stats_assists` | Int64 | Assists (player, all-time). |
| `all_time_stats_points` | Int64 | Points (player, all-time). |
| `all_time_stats_plus` | Int64 | On ice for goals for, the + of plus/minus (all-time). |
| `all_time_stats_minus` | Int64 | On ice for goals against, the - of plus/minus (all-time). |
| `all_time_stats_plus_minus` | Int64 | Plus/minus, on-ice goals for minus against (all-time). |
| `all_time_stats_shots` | Int64 | Shots (player, all-time). |
| `all_time_stats_shot_percentage` | Float64 | Shot percentage (player, all-time). Percent, 0-100. |
| `all_time_stats_penalty_minutes` | Int64 | Penalty minutes (player, all-time). |
| `season_stats_games` | Int64 | Games (player, this season). |
| `season_stats_goals` | Int64 | Goals (player, this season). |
| `season_stats_assists` | Int64 | Assists (player, this season). |
| `season_stats_points` | Int64 | Points (player, this season). |
| `season_stats_plus` | Int64 | On ice for goals for, the + of plus/minus (this season). |
| `season_stats_minus` | Int64 | On ice for goals against, the - of plus/minus (this season). |
| `season_stats_plus_minus` | Int64 | Plus/minus, on-ice goals for minus against (this season). |
| `season_stats_shots` | Int64 | Shots (player, this season). |
| `season_stats_shot_percentage` | Float64 | Shot percentage (player, this season). Percent, 0-100. |
| `season_stats_penalty_minutes` | Int64 | Penalty minutes (player, this season). |
| `season_stats_in_team_games` | Int64 | Games (player, this season for this team). |
| `season_stats_in_team_goals` | Int64 | Goals (player, this season for this team). |
| `season_stats_in_team_assists` | Int64 | Assists (player, this season for this team). |
| `season_stats_in_team_points` | Int64 | Points (player, this season for this team). |
| `season_stats_in_team_plus` | Int64 | On ice for goals for, the + of plus/minus (this season with this team). |
| `season_stats_in_team_minus` | Int64 | On ice for goals against, the - of plus/minus (this season with this team). |
| `season_stats_in_team_plus_minus` | Int64 | Plus/minus, on-ice goals for minus against (this season with this team). |
| `season_stats_in_team_shots` | Int64 | Shots (player, this season for this team). |
| `season_stats_in_team_shot_percentage` | Float64 | Shot percentage (player, this season for this team). Percent, 0-100. |
| `season_stats_in_team_penalty_minutes` | Int64 | Penalty minutes (player, this season for this team). |
| `vs_team_stats_games` | Int64 | Games played by the player against this game's opponent (span unverified). |
| `vs_team_stats_goals` | Int64 | Goals by the player against this game's opponent (span unverified). |
| `vs_team_stats_assists` | Int64 | Assists by the player against this game's opponent (span unverified). |
| `vs_team_stats_points` | Int64 | Points by the player against this game's opponent (span unverified). |
| `vs_team_stats_plus` | Int64 | On ice for goals for, the + of plus/minus (against this game's opponent; span unverified). |
| `vs_team_stats_minus` | Int64 | On ice for goals against, the - of plus/minus (against this game's opponent; span unverified). |
| `vs_team_stats_plus_minus` | Int64 | Plus/minus, on-ice goals for minus against (against this game's opponent; span unverified). |
| `vs_team_stats_shots` | Int64 | Shots by the player against this game's opponent (span unverified). |
| `vs_team_stats_shot_percentage` | Float64 | Shot percentage against this game's opponent, 0-100 (span unverified). |
| `vs_team_stats_penalty_minutes` | Int64 | Penalty minutes by the player against this game's opponent (span unverified). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#liiga_game_players_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liiga has nothing for the request (an unknown game, a tournament not played that season, a shot map before 2016). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#liiga_game_players_stats-example}

```python
liiga_game_players_stats(game_id=1, season=2026)
```

## liiga_game_stats

One game's per-period player box score, skaters and goalies, with tracking where Liiga has it.

**Endpoint URL:** `GET https://liiga.fi/api/v2/games/stats/{season}/{game_id}`

**Valid URL:** [https://liiga.fi/api/v2/games/stats/2026/1](https://liiga.fi/api/v2/games/stats/2026/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26). Game ids repeat across seasons, so a game is (season, game_id). |
| `game_id` | `game_id` |  | `Y` |  | Game id (game_id in liiga_schedule or liiga_games), e.g. 1; unique only within a season. |

### Returns {#liiga_game_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025-26). |
| `game_id` | Int64 | Game id, unique only within a season: key a game as (season, game_id). |
| `side` | String | Which team of the game the row belongs to: home or away. |
| `team_id` | Int64 | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | String | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `period` | Int64 | Period number: 1-3 are regulation, 4 and up overtime (or the shootout). |
| `player_id` | Int64 | Player id; the same number in lineups, rosters, stat tables and the profile (fihaId). |
| `jersey` | Int64 | Jersey (sweater) number the player wore. |
| `is_goalie` | Boolean | Row is a goalkeeper's (goalie columns filled). |
| `points` | Int64 | Points (player, this period). |
| `assists` | Int64 | Assists (player, this period). |
| `goals` | Int64 | Goals (player, this period). |
| `plusminus` | Int64 | plus minus minus, this period. |
| `plus` | Int64 | On ice for goals for (the + of plus/minus). |
| `minus` | Int64 | On ice for goals against (the - of plus/minus). |
| `shots` | Int64 | Shots (player, this period). |
| `penaltyminutes` | Int64 | Penaltyminutes (player, this period). |
| `timeofice` | Int64 | Timeofice (player, this period). Seconds. |
| `powerplay_goals` | Int64 | Powerplay goals (player, this period). |
| `short_handed_goals` | Int64 | Short handed goals (player, this period). |
| `winning_goal` | Boolean | Game-winning goal. |
| `blocked_shots` | Int64 | Blocked shots (player, this period). |
| `faceoffs_total` | Int64 | Faceoffs the player took in the period; null for every player in the 2026 captures. |
| `faceoffs_won` | Int64 | Faceoffs the player won in the period; null for every player in the 2026 captures. |
| `corsi_for` | Int64 | Corsi (shot attempts) for (player, this period). |
| `corsi_against` | Int64 | Corsi (shot attempts) against (player, this period). |
| `faceoffs_center_total` | Int64 | Faceoffs the player took at centre ice in the period; null for every player in the 2026 captures. |
| `faceoffs_center_won` | Int64 | Faceoffs the player won at centre ice in the period; null for every player in the 2026 captures. |
| `faceoffs_defence_total` | Int64 | Faceoffs the player took in the defensive zone in the period; null for every player in the 2026 captures. |
| `faceoffs_defence_won` | Int64 | Faceoffs the player won in the defensive zone in the period; null for every player in the 2026 captures. |
| `faceoffs_offence_total` | Int64 | Faceoffs the player took in the offensive zone in the period; null for every player in the 2026 captures. |
| `faceoffs_offence_won` | Int64 | Faceoffs the player won in the offensive zone in the period; null for every player in the 2026 captures. |
| `fs_zone_starts_oz` | Int64 | Full-strength (fs, unverified) zone starts offensive zone (player, this period). |
| `fs_zone_starts_dz` | Int64 | Full-strength (fs, unverified) zone starts defensive zone (player, this period). |
| `powerplay2_goals` | Int64 | Powerplay 2 goals (player, this period). |
| `penaltykill2_goals` | Int64 | Penaltykill 2 goals (player, this period). |
| `powerplay_assists` | Int64 | Powerplay assists (player, this period). |
| `penaltykill_assists` | Int64 | Penaltykill assists (player, this period). |
| `goals_to_empty_goal` | Int64 | Goals to empty goal (player, this period). |
| `fs_team_shots` | Int64 | Full-strength (fs, unverified) team shots (player, this period). |
| `fs_team_goals` | Int64 | Full-strength (fs, unverified) team goals (player, this period). |
| `fs_team_shots_against` | Int64 | Full-strength (fs, unverified) team shots against (player, this period). |
| `fs_team_goals_against` | Int64 | Full-strength (fs, unverified) team goals against (player, this period). |
| `distance` | Float64 | Distance (player, this period). Distance; unit not stated (unverified). |
| `total_passes` | Int64 | Total passes (player, this period). |
| `successful_passes` | Int64 | Successful passes (player, this period). |
| `player_passes_under_pressure` | Int64 | Player passes under pressure (player, this period). |
| `player_successful_passes_under_pressure` | Int64 | Player successful passes under pressure (player, this period). |
| `player_passes_under_high_pressure` | Int64 | Player passes under high pressure (player, this period). |
| `player_successful_passes_under_high_pressure` | Int64 | Player successful passes under high pressure (player, this period). |
| `expected_goals_player` | Float64 | Expected goals (xG) of the player's own shots in the period, provider-computed (unverified scope). |
| `expected_goals_team` | Float64 | Team expected goals (xG) while the player was on ice in the period, provider-computed (unverified scope). |
| `expected_goals_against` | Float64 | Expected goals against (xG) faced in the period, provider-computed. |
| `expected_goals_against_shot_on_goal` | Float64 | Expected goals against (xG) of the shots on goal only, provider-computed. |
| `saves` | Int64 | Saves (goalkeeper, this period). |
| `goals_allowed` | Int64 | Goals allowed (goalkeeper, this period). |
| `saves_percentage` | Float64 | Save percentage, 0-100, sent as a STRING (e.g. "92.86"; "" when no shots), this period. |
| `shots_on_goal` | Int64 | Shots on goal (goalkeeper, this period). |
| `top_speed` | Float64 | Player's top skating speed in the game (unit not stated; values near 9 fit m/s). |
| `hardest_shot` | Float64 | Player's hardest shot in the game (unit not stated; values near 23 fit m/s). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#liiga_game_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liiga has nothing for the request (an unknown game, a tournament not played that season, a shot map before 2016). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#liiga_game_stats-example}

```python
liiga_game_stats(game_id=1, season=2026)
```

## liiga_game_team_stats

One game's per-period team stats with puck possession.

**Endpoint URL:** `GET https://liiga.fi/api/v2/games/stats/{season}/{game_id}`

**Valid URL:** [https://liiga.fi/api/v2/games/stats/2026/1](https://liiga.fi/api/v2/games/stats/2026/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26). Game ids repeat across seasons, so a game is (season, game_id). |
| `game_id` | `game_id` |  | `Y` |  | Game id (game_id in liiga_schedule or liiga_games), e.g. 1; unique only within a season. |

### Returns {#liiga_game_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025-26). |
| `game_id` | Int64 | Game id, unique only within a season: key a game as (season, game_id). |
| `side` | String | Which team of the game the row belongs to: home or away. |
| `period` | Int64 | Period number: 1-3 are regulation, 4 and up overtime (or the shootout). |
| `team_id` | Int64 | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | String | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `goals` | Int64 | Goals (team, this period). |
| `shots` | Int64 | Shots (team, this period). |
| `power_play_instances` | String | JSON list of Liiga's codes for the team's power plays in the period, one per power play (code meaning undocumented). |
| `power_play_goals` | Int64 | Power play goals (team, this period). |
| `short_handed_instances` | String | JSON list of Liiga's codes for the team's short-handed spells in the period, one per spell (code meaning undocumented). |
| `short_handed_goals_against` | Int64 | Short handed goals against (team, this period). |
| `penalty_minutes` | Int64 | Penalty minutes (team, this period). |
| `face_off_wins` | Int64 | Face off wins (team, this period). |
| `two_minute_penalties` | Int64 | Two minute penalties (team, this period). |
| `five_minute_penalties` | Int64 | Five minute penalties (team, this period). |
| `ten_minute_penalties` | Int64 | Ten minute penalties (team, this period). |
| `twenty_minute_penalties` | Int64 | Twenty minute penalties (team, this period). |
| `total_distance_travelled` | Float64 | Distance skated by the team in the period (unit not stated). |
| `puck_control_duration` | Float64 | Seconds this team controlled the puck in the period. |
| `opponent_puck_control_duration` | Float64 | Seconds the other team controlled the puck in the period. |
| `contested_control_duration` | Float64 | Seconds the puck was contested in the period. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#liiga_game_team_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Liiga has nothing for the request (an unknown game, a tournament not played that season, a shot map before 2016). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#liiga_game_team_stats-example}

```python
liiga_game_team_stats(game_id=1, season=2026)
```
