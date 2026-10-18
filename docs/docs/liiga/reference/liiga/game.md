---
title: "LIIGA — Liiga API v2 (liiga.fi/api/v2, Finland) — Game"
sidebar_label: "Game"
sidebar_position: 1
description: "LIIGA — Liiga API v2 (liiga.fi/api/v2, Finland) — Game — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# LIIGA — Liiga API v2 (liiga.fi/api/v2, Finland) — Game

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
| `season` | integer | Season END year (2026 = 2025-26). |
| `game_id` | integer | Game id, unique only within a season: key a game as (season, game_id). |
| `side` | character | Which team of the game the row belongs to: home or away. |
| `team_id` | integer | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | character | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `log_time` | character | Wall-clock time the event was logged, ISO 8601 UTC. |
| `winning_goal` | logical | Game-winning goal. |
| `game_time` | integer | Seconds from the start of the game. Regular season: 3900 (65:00) = the end of the 5-minute overtime; playoff overtime runs past it. |
| `period` | integer | Period number: 1-3 are regulation, 4 and up overtime (or the shootout). |
| `event_id` | integer | Event id within the game. |
| `home_team_score` | integer | Home score after the event. |
| `away_team_score` | integer | Away score after the event. |
| `goals_so_far_in_season` | integer | Scorer's season goal count after this goal. |
| `video_clip_url` | character | URL of the goal's video clip; null in every capture. |
| `video_clip_playlist_url` | character | URL of the goal's video clip playlist; null in every capture. |
| `video_thumbnail_url` | character | URL of the goal video's thumbnail image; null in every capture. |
| `scorer_player_id` | integer | Scorer's player id. |
| `scorer_first_name` | character | Scorer's first name. |
| `scorer_last_name` | character | Scorer's last name. |
| `assist1_player_id` | integer | First assist's player id. |
| `assist1_first_name` | character | First assist's first name. |
| `assist1_last_name` | character | First assist's last name. |
| `assist1_season_assists` | integer | First assister's season assist count after this goal. |
| `assist2_player_id` | integer | Second assist's player id. |
| `assist2_first_name` | character | Second assist's first name. |
| `assist2_last_name` | character | Second assist's last name. |
| `assist2_season_assists` | integer | Second assister's season assist count after this goal. |
| `goal_types` | character | Goal type codes, comma-separated Finnish abbreviations: YV power play, AV short-handed, TM empty net, RL penalty shot (others undocumented). |
| `plus_jerseys` | character | Jersey NUMBERS of this team's skaters on ice for the goal, space-separated. |
| `minus_jerseys` | character | Jersey NUMBERS of the other team's skaters on ice for the goal, space-separated. |

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

_Last validated n/a._

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
| `season` | integer | Season END year (2026 = 2025-26). |
| `game_id` | integer | Game id, unique only within a season: key a game as (season, game_id). |
| `side` | character | Which team of the game the row belongs to: home or away. |
| `team_id` | integer | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | character | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `player_id` | integer | Player id; the same number in lineups, rosters, stat tables and the profile (fihaId). |
| `sufferer_player_id` | integer | Player who served the penalty. |
| `event_id` | integer | Event id within the game. |
| `log_time` | character | Wall-clock time the event was logged, ISO 8601 UTC. |
| `game_time` | integer | Seconds from the start of the game. Regular season: 3900 (65:00) = the end of the 5-minute overtime; playoff overtime runs past it. |
| `period` | integer | Period number: 1-3 are regulation, 4 and up overtime (or the shootout). |
| `penalty_begintime` | integer | Penalty start, seconds from game start. |
| `penalty_endtime` | integer | Penalty end, seconds from game start. |
| `penalty_fault_name` | character | Infraction, in Finnish. |
| `penalty_fault_type` | character | Infraction code. |
| `penalty_info` | character | Extra penalty code ('' or a letter such as K; meaning unverified). |
| `penalty_minutes` | integer | Length of the penalty in minutes (2, 5, 10, 20, ...). |

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

_Last validated n/a._

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
| `season` | integer | Season END year (2026 = 2025-26). |
| `game_id` | integer | Game id, unique only within a season: key a game as (season, game_id). |
| `shooting_player_id` | integer | Shooter's player id. |
| `blocking_player_id` | integer | Goalkeeper's player id. |
| `shot_number` | integer | Attempt number in the shootout. |
| `shooting_team_id` | integer | Team id of the shooter's team (the number of the `<num>:<slug>` key). |
| `shooting_team_slug` | character | Slug of the shooter's team (e.g. hpk). |
| `blocking_team_id` | integer | Goalkeeper's team id. |
| `blocking_team_slug` | character | Goalkeeper's team slug. |
| `goal_scored` | logical | The attempt scored. |
| `winning_goal` | logical | Game-winning goal. |

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

_Last validated n/a._

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
| `season` | integer | Season END year (2026 = 2025-26). |
| `game_id` | integer | Game id, unique only within a season: key a game as (season, game_id). |
| `side` | character | Which team of the game the row belongs to: home or away. |
| `player_id` | integer | Player id; the same number in lineups, rosters, stat tables and the profile (fihaId). |
| `team_id` | integer | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | character | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `team_name` | character | Team's display name, e.g. HPK or Tappara. |
| `line` | integer | Line number 1-4; null for goalkeepers and extras. |
| `country_of_birth` | character | Country of birth, 3-letter code. |
| `place_of_birth` | character | Player's place of birth (town or city). |
| `date_of_birth` | character | Date of birth, YYYY-MM-DD. |
| `nationality` | character | Nationality, 3-letter code. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `role` | character | Position: CENTER, LEFT_WING, RIGHT_WING, LEFT_DEFENSEMAN, RIGHT_DEFENSEMAN, SEVENTH_DEFENSEMAN, GOALIE. |
| `role_code` | character | Position code, Finnish: KH = centre, VL/OL = left/right wing, VP/OP = left/right defence, 7. P = seventh defenceman, MV = goalkeeper. |
| `handedness` | character | Shoots LEFT or RIGHT. |
| `height` | integer | Player's height in centimetres. |
| `weight` | integer | Player's weight in kilograms. |
| `captain` | logical | True when the player is the team captain (C). |
| `rookie` | logical | True when Liiga flags the player as a rookie. |
| `alternate_captain` | logical | True when the player is an alternate captain (A). |
| `jersey` | integer | Jersey (sweater) number the player wore. |
| `picture_url` | character | URL of the player's photo. |
| `injured` | logical | True when the player is listed as injured. |
| `suspended` | logical | True when the player is listed as suspended. |
| `removed` | logical | Removed from the roster. |
| `awards` | character | Player awards of the game (stars). |
| `sponsors` | character | Player's sponsors; null in every capture (unverified). |
| `extra_description` | character | Extra profile text for the player; null in every capture (meaning unverified). |
| `extra_strengths` | character | Extra profile field for the player's strengths; null in every capture (unverified). |
| `extra_rookie_picture` | logical | Flag for an extra rookie picture on the player's profile (meaning unverified). |

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

_Last validated n/a._

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
| `season` | integer | Season END year (2026 = 2025-26). |
| `game_id` | integer | Game id, unique only within a season: key a game as (season, game_id). |
| `side` | character | Which team of the game the row belongs to: home or away. |
| `player_id` | integer | Player id; the same number in lineups, rosters, stat tables and the profile (fihaId). |
| `team_id` | integer | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | character | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `team_name` | character | Team's display name, e.g. HPK or Tappara. |
| `line` | integer | Line number 1-4; null for goalkeepers and extras. |
| `country_of_birth` | character | Country of birth, 3-letter code. |
| `place_of_birth` | character | Player's place of birth (town or city). |
| `date_of_birth` | character | Date of birth, YYYY-MM-DD. |
| `nationality` | character | Nationality, 3-letter code. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `role` | character | Position: CENTER, LEFT_WING, RIGHT_WING, LEFT_DEFENSEMAN, RIGHT_DEFENSEMAN, SEVENTH_DEFENSEMAN, GOALIE. |
| `role_code` | character | Position code, Finnish: KH = centre, VL/OL = left/right wing, VP/OP = left/right defence, 7. P = seventh defenceman, MV = goalkeeper. |
| `handedness` | character | Shoots LEFT or RIGHT. |
| `height` | integer | Player's height in centimetres. |
| `weight` | integer | Player's weight in kilograms. |
| `captain` | logical | True when the player is the team captain (C). |
| `rookie` | logical | True when Liiga flags the player as a rookie. |
| `alternate_captain` | logical | True when the player is an alternate captain (A). |
| `jersey` | integer | Jersey (sweater) number the player wore. |
| `picture_url` | character | URL of the player's photo. |
| `injured` | logical | True when the player is listed as injured. |
| `suspended` | logical | True when the player is listed as suspended. |
| `removed` | logical | Removed from the roster. |
| `awards` | character | Player awards of the game (stars). |
| `sponsors` | character | Player's sponsors; null in every capture (unverified). |
| `extra_description` | character | Extra profile text for the player; null in every capture (meaning unverified). |
| `extra_strengths` | character | Extra profile field for the player's strengths; null in every capture (unverified). |
| `extra_rookie_picture` | logical | Flag for an extra rookie picture on the player's profile (meaning unverified). |
| `all_time_stats_games` | integer | Games (player, all-time). |
| `all_time_stats_goals` | integer | Goals (player, all-time). |
| `all_time_stats_assists` | integer | Assists (player, all-time). |
| `all_time_stats_points` | integer | Points (player, all-time). |
| `all_time_stats_plus` | integer | On ice for goals for, the + of plus/minus (all-time). |
| `all_time_stats_minus` | integer | On ice for goals against, the - of plus/minus (all-time). |
| `all_time_stats_plus_minus` | integer | Plus/minus, on-ice goals for minus against (all-time). |
| `all_time_stats_shots` | integer | Shots (player, all-time). |
| `all_time_stats_shot_percentage` | numeric | Shot percentage (player, all-time). Percent, 0-100. |
| `all_time_stats_penalty_minutes` | integer | Penalty minutes (player, all-time). |
| `season_stats_games` | integer | Games (player, this season). |
| `season_stats_goals` | integer | Goals (player, this season). |
| `season_stats_assists` | integer | Assists (player, this season). |
| `season_stats_points` | integer | Points (player, this season). |
| `season_stats_plus` | integer | On ice for goals for, the + of plus/minus (this season). |
| `season_stats_minus` | integer | On ice for goals against, the - of plus/minus (this season). |
| `season_stats_plus_minus` | integer | Plus/minus, on-ice goals for minus against (this season). |
| `season_stats_shots` | integer | Shots (player, this season). |
| `season_stats_shot_percentage` | numeric | Shot percentage (player, this season). Percent, 0-100. |
| `season_stats_penalty_minutes` | integer | Penalty minutes (player, this season). |
| `season_stats_in_team_games` | integer | Games (player, this season for this team). |
| `season_stats_in_team_goals` | integer | Goals (player, this season for this team). |
| `season_stats_in_team_assists` | integer | Assists (player, this season for this team). |
| `season_stats_in_team_points` | integer | Points (player, this season for this team). |
| `season_stats_in_team_plus` | integer | On ice for goals for, the + of plus/minus (this season with this team). |
| `season_stats_in_team_minus` | integer | On ice for goals against, the - of plus/minus (this season with this team). |
| `season_stats_in_team_plus_minus` | integer | Plus/minus, on-ice goals for minus against (this season with this team). |
| `season_stats_in_team_shots` | integer | Shots (player, this season for this team). |
| `season_stats_in_team_shot_percentage` | numeric | Shot percentage (player, this season for this team). Percent, 0-100. |
| `season_stats_in_team_penalty_minutes` | integer | Penalty minutes (player, this season for this team). |
| `vs_team_stats_games` | integer | Games played by the player against this game's opponent (span unverified). |
| `vs_team_stats_goals` | integer | Goals by the player against this game's opponent (span unverified). |
| `vs_team_stats_assists` | integer | Assists by the player against this game's opponent (span unverified). |
| `vs_team_stats_points` | integer | Points by the player against this game's opponent (span unverified). |
| `vs_team_stats_plus` | integer | On ice for goals for, the + of plus/minus (against this game's opponent; span unverified). |
| `vs_team_stats_minus` | integer | On ice for goals against, the - of plus/minus (against this game's opponent; span unverified). |
| `vs_team_stats_plus_minus` | integer | Plus/minus, on-ice goals for minus against (against this game's opponent; span unverified). |
| `vs_team_stats_shots` | integer | Shots by the player against this game's opponent (span unverified). |
| `vs_team_stats_shot_percentage` | numeric | Shot percentage against this game's opponent, 0-100 (span unverified). |
| `vs_team_stats_penalty_minutes` | integer | Penalty minutes by the player against this game's opponent (span unverified). |

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

_Last validated n/a._

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
| `season` | integer | Season END year (2026 = 2025-26). |
| `game_id` | integer | Game id, unique only within a season: key a game as (season, game_id). |
| `side` | character | Which team of the game the row belongs to: home or away. |
| `team_id` | integer | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | character | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `period` | integer | Period number: 1-3 are regulation, 4 and up overtime (or the shootout). |
| `player_id` | integer | Player id; the same number in lineups, rosters, stat tables and the profile (fihaId). |
| `jersey` | integer | Jersey (sweater) number the player wore. |
| `is_goalie` | logical | Row is a goalkeeper's (goalie columns filled). |
| `points` | integer | Points (player, this period). |
| `assists` | integer | Assists (player, this period). |
| `goals` | integer | Goals (player, this period). |
| `plusminus` | integer | plus minus minus, this period. |
| `plus` | integer | On ice for goals for (the + of plus/minus). |
| `minus` | integer | On ice for goals against (the - of plus/minus). |
| `shots` | integer | Shots (player, this period). |
| `penaltyminutes` | integer | Penaltyminutes (player, this period). |
| `timeofice` | integer | Timeofice (player, this period). Seconds. |
| `powerplay_goals` | integer | Powerplay goals (player, this period). |
| `short_handed_goals` | integer | Short handed goals (player, this period). |
| `winning_goal` | logical | Game-winning goal. |
| `blocked_shots` | integer | Blocked shots (player, this period). |
| `faceoffs_total` | integer | Faceoffs the player took in the period; null for every player in the 2026 captures. |
| `faceoffs_won` | integer | Faceoffs the player won in the period; null for every player in the 2026 captures. |
| `corsi_for` | integer | Corsi (shot attempts) for (player, this period). |
| `corsi_against` | integer | Corsi (shot attempts) against (player, this period). |
| `faceoffs_center_total` | integer | Faceoffs the player took at centre ice in the period; null for every player in the 2026 captures. |
| `faceoffs_center_won` | integer | Faceoffs the player won at centre ice in the period; null for every player in the 2026 captures. |
| `faceoffs_defence_total` | integer | Faceoffs the player took in the defensive zone in the period; null for every player in the 2026 captures. |
| `faceoffs_defence_won` | integer | Faceoffs the player won in the defensive zone in the period; null for every player in the 2026 captures. |
| `faceoffs_offence_total` | integer | Faceoffs the player took in the offensive zone in the period; null for every player in the 2026 captures. |
| `faceoffs_offence_won` | integer | Faceoffs the player won in the offensive zone in the period; null for every player in the 2026 captures. |
| `fs_zone_starts_oz` | integer | Full-strength (fs, unverified) zone starts offensive zone (player, this period). |
| `fs_zone_starts_dz` | integer | Full-strength (fs, unverified) zone starts defensive zone (player, this period). |
| `powerplay2_goals` | integer | Powerplay 2 goals (player, this period). |
| `penaltykill2_goals` | integer | Penaltykill 2 goals (player, this period). |
| `powerplay_assists` | integer | Powerplay assists (player, this period). |
| `penaltykill_assists` | integer | Penaltykill assists (player, this period). |
| `goals_to_empty_goal` | integer | Goals to empty goal (player, this period). |
| `fs_team_shots` | integer | Full-strength (fs, unverified) team shots (player, this period). |
| `fs_team_goals` | integer | Full-strength (fs, unverified) team goals (player, this period). |
| `fs_team_shots_against` | integer | Full-strength (fs, unverified) team shots against (player, this period). |
| `fs_team_goals_against` | integer | Full-strength (fs, unverified) team goals against (player, this period). |
| `distance` | numeric | Distance (player, this period). Distance; unit not stated (unverified). |
| `total_passes` | integer | Total passes (player, this period). |
| `successful_passes` | integer | Successful passes (player, this period). |
| `player_passes_under_pressure` | integer | Player passes under pressure (player, this period). |
| `player_successful_passes_under_pressure` | integer | Player successful passes under pressure (player, this period). |
| `player_passes_under_high_pressure` | integer | Player passes under high pressure (player, this period). |
| `player_successful_passes_under_high_pressure` | integer | Player successful passes under high pressure (player, this period). |
| `expected_goals_player` | numeric | Expected goals (xG) of the player's own shots in the period, provider-computed (unverified scope). |
| `expected_goals_team` | numeric | Team expected goals (xG) while the player was on ice in the period, provider-computed (unverified scope). |
| `expected_goals_against` | numeric | Expected goals against (xG) faced in the period, provider-computed. |
| `expected_goals_against_shot_on_goal` | numeric | Expected goals against (xG) of the shots on goal only, provider-computed. |
| `saves` | integer | Saves (goalkeeper, this period). |
| `goals_allowed` | integer | Goals allowed (goalkeeper, this period). |
| `saves_percentage` | numeric | Save percentage, 0-100, sent as a STRING (e.g. "92.86"; "" when no shots), this period. |
| `shots_on_goal` | integer | Shots on goal (goalkeeper, this period). |
| `top_speed` | numeric | Player's top skating speed in the game (unit not stated; values near 9 fit m/s). |
| `hardest_shot` | numeric | Player's hardest shot in the game (unit not stated; values near 23 fit m/s). |

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

_Last validated n/a._

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
| `season` | integer | Season END year (2026 = 2025-26). |
| `game_id` | integer | Game id, unique only within a season: key a game as (season, game_id). |
| `side` | character | Which team of the game the row belongs to: home or away. |
| `period` | integer | Period number: 1-3 are regulation, 4 and up overtime (or the shootout). |
| `team_id` | integer | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | character | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `goals` | integer | Goals (team, this period). |
| `shots` | integer | Shots (team, this period). |
| `power_play_instances` | character | JSON list of Liiga's codes for the team's power plays in the period, one per power play (code meaning undocumented). |
| `power_play_goals` | integer | Power play goals (team, this period). |
| `short_handed_instances` | character | JSON list of Liiga's codes for the team's short-handed spells in the period, one per spell (code meaning undocumented). |
| `short_handed_goals_against` | integer | Short handed goals against (team, this period). |
| `penalty_minutes` | integer | Penalty minutes (team, this period). |
| `face_off_wins` | integer | Face off wins (team, this period). |
| `two_minute_penalties` | integer | Two minute penalties (team, this period). |
| `five_minute_penalties` | integer | Five minute penalties (team, this period). |
| `ten_minute_penalties` | integer | Ten minute penalties (team, this period). |
| `twenty_minute_penalties` | integer | Twenty minute penalties (team, this period). |
| `total_distance_travelled` | numeric | Distance skated by the team in the period (unit not stated). |
| `puck_control_duration` | numeric | Seconds this team controlled the puck in the period. |
| `opponent_puck_control_duration` | numeric | Seconds the other team controlled the puck in the period. |
| `contested_control_duration` | numeric | Seconds the puck was contested in the period. |

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

_Last validated n/a._
