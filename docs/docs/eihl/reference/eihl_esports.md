---
title: EIHL — EIHL data feed (eSports hokejovyzapis S3 bucket, UK)
sidebar_label: EIHL data feed (eSports hokejovyzapis S3 bucket, UK)
description: "EIHL — EIHL data feed (eSports hokejovyzapis S3 bucket, UK) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# EIHL — EIHL data feed (eSports hokejovyzapis S3 bucket, UK)

`sportsdataverse.hockey.eihl` — 9 endpoints.

## eihl_schedule

Schedule and results of one competition season, one row per match, played or not.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/league-matches/{season}/{league_id}.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/league-matches/2026/3.json](https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/league-matches/2026/3.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season by its END year (2027 = 2026/27). The bucket keys seasons by their START year; the wrapper requests season - 1. |
| `league_id` | `league_id` |  |  | `Y` | Competition id. 3 = Elite League, 4 = Challenge Cup, 5 = playoffs. |

### Returns {#eihl_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | Match id (per-bucket match key space); joins to the {match_id} path param of the game wrappers. |
| `start_date` | String | Scheduled face-off, local `YYYY-MM-DD HH:MM:SS`; DEL playoff placeholders read `0000-00-00 ...`. |
| `league_id` | Int64 | League id (per-bucket competition key space); joins to the `{league_id}` path param. |
| `league` | String | League display name, e.g. `DEL`, `EIHL`, `Metal Ligaen`. |
| `season` | Int64 | Season by its END year (2027 = 2026/27); the body's START-year key plus one. |
| `round` | String | Round number as a string (`"1"`, `"249"`); DEL values carry a leading U+FEFF byte-order mark, empty string in Denmark. |
| `group` | String | Group name in group stages; empty string or null otherwise. |
| `home_id` | Int64 | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `home_name` | String | Team display name as the feed prints it. |
| `home_shortcut` | String | Three-letter team code. |
| `guest_id` | Int64 | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `guest_name` | String | Team display name as the feed prints it. |
| `guest_shortcut` | String | Three-letter team code. |
| `status` | String | Match status: `BEFORE_MATCH`, `LIVE`, `AFTER_MATCH`. |
| `overtime_count` | Int64 | Number of overtime periods played; null in the samples. |
| `extra_time` | Boolean | True when the game went to overtime. |
| `shooting` | Boolean | True when the game was decided by a shootout. |
| `contumacy` | Boolean | True when the result was awarded by forfeit. |
| `final_score_home` | Int64 | Home team goals in that span. |
| `final_score_guest` | Int64 | Visiting team goals in that span. |
| `first_period_score_home` | Int64 | Home team goals in that span. |
| `first_period_score_guest` | Int64 | Visiting team goals in that span. |
| `second_period_score_home` | Int64 | Home team goals in that span. |
| `second_period_score_guest` | Int64 | Visiting team goals in that span. |
| `third_period_score_home` | Int64 | Home team goals in that span. |
| `third_period_score_guest` | Int64 | Visiting team goals in that span. |
| `actual_time_name` | String | Display label of the game state in the bucket's language, e.g. `end`, `Ende`, `before game`. |
| `actual_time_alias` | String | Short game-state code: `K` = finished (Czech `konec`), `0` = not started. |
| `last_event_time` | Int64 | Game second of the latest event, of game time from the opening face-off (0-3600 in regulation, continuing through overtime). |
| `arena` | String | Arena name; empty string or null where not set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#eihl_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The bucket has no such key (S3 404 NoSuchKey: unknown season, competition, match or team), or the file holds no rows (an unplayed game, an empty shot shell). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#eihl_schedule-example}

```python
eihl_schedule(league_id=3, season=2027)
```

## eihl_team_player_stats

Season player stats of one team in one competition, one row per player.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/league-team-stats/{season}/{league_id}/{team_id}.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/league-team-stats/2026/3/96.json](https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/league-team-stats/2026/3/96.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season by its END year (2027 = 2026/27). The bucket keys seasons by their START year; the wrapper requests season - 1. |
| `league_id` | `league_id` |  |  | `Y` | Competition id. 3 = Elite League, 4 = Challenge Cup, 5 = playoffs. |
| `team_id` | `team_id` |  | `Y` |  | Team id (per-bucket team key space: home_id / guest_id in eihl_schedule). |

### Returns {#eihl_team_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Player id (playerId key space, per bucket, stable across games and seasons). |
| `name` | String | Player full name (first and last). |
| `firstname` | String | First name of the player in this row. |
| `surname` | String | Player's last (family) name. |
| `birthdate` | String | Birth date `YYYY-MM-DD`. |
| `active` | Boolean | True when the player is active on the team. |
| `position` | String | Position code: `GK` goalie, `DE` defence, `FW` / `FO` forward (FO meaning unverified). |
| `stick` | String | Handedness: `left` or `right`; null when unknown. |
| `weight` | Int64 | Weight in kg; `0` placeholder when unknown. |
| `height` | Int64 | Height in cm; `0` placeholder when unknown. |
| `jersey` | Int64 | Player jersey (sweater) number. |
| `birth_country` | String | Player's country of birth (name). |
| `nationality` | String | Nationality, country name. |
| `nationality_short` | String | Nationality, three-letter code (`CAN`, `GER`; `UK` for Britain). |
| `team_shortcut` | String | Three-letter team code. |
| `games` | Int64 | Games played in the table's scope. |
| `goals` | Int64 | goals in total (goals_home + goals_away: the body splits it by home and away games). |
| `goals_home` | Int64 | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `goals_away` | Int64 | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `assists` | Int64 | assists in total (assists_home + assists_away: the body splits it by home and away games). |
| `assists_home` | Int64 | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `assists_away` | Int64 | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `points` | Int64 | points in total (points_home + points_away: the body splits it by home and away games). |
| `points_home` | Int64 | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `points_away` | Int64 | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `penalty_minutes` | Int64 | Penalty minutes assessed to the player (this game, or to date in the season / league-part files). |
| `positive` | Int64 | Plus: on ice for goals for at even strength or shorthanded. |
| `negative` | Int64 | Minus: on ice for goals against at even strength or on the power play. |
| `points_per_game` | Float64 | Average goals plus assists per game played. |
| `shot_efficiency` | Float64 | Shooting percentage, goals / shots on goal x 100. |
| `penalty_minutes_per_game` | Float64 | Average penalty minutes per game played. |
| `pp_goals` | Int64 | Power-play goals. |
| `sh_goals` | Int64 | Shorthanded goals. |
| `gw_goals` | Int64 | Game-winning goals. |
| `shots_attempts` | Int64 | Shot attempts (on goal + missed + blocked). |
| `shots_on_goal` | Int64 | shots_on_goal in total (shots_on_goal_home + shots_on_goal_away: the body splits it by home and away games). |
| `shots_on_goal_home` | Int64 | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `shots_on_goal_away` | Int64 | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `shots_missed` | Int64 | This player's shot attempts that missed the net. |
| `shots_blocked` | Int64 | This player's shot attempts that were blocked. |
| `blocked_shots_by_player` | Int64 | Opponent shots this player blocked. |
| `faceoffs_win` | Int64 | Face-offs the player won (this game, or to date in the season / league-part files). |
| `faceoffs_losses` | Int64 | Face-offs the player lost (this game, or to date in the season / league-part files). |
| `faceoffs_count` | Int64 | Face-offs the player took (this game, or to date in the season / league-part files). |
| `timeonice` | Float64 | Time on ice, seconds with decimals (tracking-based; differs from `timeOnIce`). |
| `timeonice_eq` | Float64 | Time on ice at even strength (`EQ`), on the power play (`PP`) or shorthanded (`SH`), seconds with decimals. |
| `timeonice_pp` | Float64 | Time on ice at even strength (`EQ`), on the power play (`PP`) or shorthanded (`SH`), seconds with decimals. |
| `timeonice_sh` | Float64 | Time on ice at even strength (`EQ`), on the power play (`PP`) or shorthanded (`SH`), seconds with decimals. |
| `hits` | Int64 | Body checks delivered. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#eihl_team_player_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The bucket has no such key (S3 404 NoSuchKey: unknown season, competition, match or team), or the file holds no rows (an unplayed game, an empty shot shell). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#eihl_team_player_stats-example}

```python
eihl_team_player_stats(league_id=3, season=2027, team_id=96)
```

## eihl_top_scorers

Skater scoring leaderboard of one competition season.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/league-top-scorers/{season}/{league_id}.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/league-top-scorers/2026/3.json](https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/league-top-scorers/2026/3.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season by its END year (2027 = 2026/27). The bucket keys seasons by their START year; the wrapper requests season - 1. |
| `league_id` | `league_id` |  |  | `Y` | Competition id. 3 = Elite League, 4 = Challenge Cup, 5 = playoffs. |

### Returns {#eihl_top_scorers-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Player id (player key space, per bucket, stable across games and seasons); joins to `playerId` in rosters, events, boxscores and leaderboards. |
| `name` | String | Player's first (given) name. |
| `surname` | String | Player's last (family) name. |
| `jersey` | Int64 | Player jersey (sweater) number. |
| `birthdate` | String | Birth date `YYYY-MM-DD`. |
| `team_id` | Int64 | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `team_name` | String | Team display name as the feed prints it. |
| `team_shortcut` | String | Three-letter team code. |
| `matches` | Int64 | Games played (two names for the same count). |
| `goals` | Int64 | Goals the player scored in the league-season. |
| `assists` | Int64 | Assists credited to the player in the league-season. |
| `points` | Int64 | Points, goals + assists. |
| `shots` | Int64 | Shots on goal (saved or scored). |
| `positives` | Int64 | Plus events of the player: on ice for a goal for (league-season). |
| `negatives` | Int64 | Minus events of the player: on ice for a goal against (league-season). |
| `positives_negatives` | Int64 | Plus/minus, positives - negatives. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#eihl_top_scorers-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The bucket has no such key (S3 404 NoSuchKey: unknown season, competition, match or team), or the file holds no rows (an unplayed game, an empty shot shell). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#eihl_top_scorers-example}

```python
eihl_top_scorers(league_id=3, season=2027)
```

## eihl_game_info

Game info of one match: state, arena, attendance, officials, coaches, period scores.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/matches/{match_id}/game-info.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/matches/4440/game-info.json](https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/matches/4440/game-info.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (per-bucket match key space: match_id in eihl_schedule). Not the league website's game id. |

### Returns {#eihl_game_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `actual_time_name` | String | Display label of the game state, e.g. `end` / `Ende`. |
| `actual_time_alias` | String | Short game-state code: `K` = finished. |
| `stadium` | String | Name of the arena the game was played in. |
| `number_of_viewers` | Int64 | Spectators at the game (attendance). |
| `referees_head_referee1` | String | Official's name (ICEHL/EIHL/Denmark: a full-name string; DEL: an object with `id` and `name`). |
| `referees_head_referee2` | String | Official's name (ICEHL/EIHL/Denmark: a full-name string; DEL: an object with `id` and `name`). |
| `referees_line_referee1` | String | Official's name (ICEHL/EIHL/Denmark: a full-name string; DEL: an object with `id` and `name`). |
| `referees_line_referee2` | String | Official's name (ICEHL/EIHL/Denmark: a full-name string; DEL: an object with `id` and `name`). |
| `team_info_home_id` | Int64 | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `team_info_home_name` | String | Team display name as the feed prints it. |
| `team_info_home_shortcut` | String | Three-letter team code. |
| `team_info_visitor_id` | Int64 | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `team_info_visitor_name` | String | Team display name as the feed prints it. |
| `team_info_visitor_shortcut` | String | Three-letter team code. |
| `trainers_home_head_coach` | String | Head coach (ICEHL/EIHL/Denmark: a full-name string; DEL: an object with `id` and `name`). |
| `trainers_visitor_head_coach` | String | Head coach (ICEHL/EIHL/Denmark: a full-name string; DEL: an object with `id` and `name`). |
| `team_bilance_home_win` | Int64 | Wins of that team in the head-to-head record (unverified). |
| `team_bilance_visitor_win` | Int64 | Wins of that team in the head-to-head record (unverified). |
| `results_extra_time` | Boolean | True when the game went to overtime. |
| `results_shooting` | Boolean | True when the game was decided by a shootout. |
| `results_contumacy` | Boolean | True when the result was awarded by forfeit. |
| `results_score_final_score_home` | Int64 | Home team goals in that span. |
| `results_score_final_score_guest` | Int64 | Visiting team goals in that span. |
| `results_score_first_period_score_home` | Int64 | Home team goals in that span. |
| `results_score_first_period_score_guest` | Int64 | Visiting team goals in that span. |
| `results_score_second_period_score_home` | Int64 | Home team goals in that span. |
| `results_score_second_period_score_guest` | Int64 | Visiting team goals in that span. |
| `results_score_third_period_score_home` | Int64 | Home team goals in that span. |
| `results_score_third_period_score_guest` | Int64 | Visiting team goals in that span. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#eihl_game_info-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The bucket has no such key (S3 404 NoSuchKey: unknown season, competition, match or team), or the file holds no rows (an unplayed game, an empty shot shell). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#eihl_game_info-example}

```python
eihl_game_info(match_id=4440)
```

## eihl_pbp

Play-by-play of one match: goals, penalties, goalie changes and timeouts, one row per event.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/matches/{match_id}/period-events.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/matches/4440/period-events.json](https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/matches/4440/period-events.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (per-bucket match key space: match_id in eihl_schedule). Not the league website's game id. |

### Returns {#eihl_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `event_index` | Int64 | Row order of the event within the game (0-based), across periods. |
| `period` | String | Period key as the body sends it: 1, 2, 3, OT (overtime) or SO (shootout). |
| `period_number` | Int64 | Period number: 1-3, 4 = overtime, 5 = shootout. |
| `time` | Int64 | Game second of the event from the opening face-off (0-3600 in regulation, continuing through overtime). |
| `event_type` | String | Event type: goal, penalty, goalkeeperChange, timeout, periodStart, periodEnd, videoReview, ... |
| `event_id` | Int64 | Event id within its event type. |
| `team_side` | String | Team of the row: home or visitor. |
| `goalie_player_id` | Int64 | Player id (playerId key space) of the goalie entering the game (goalkeeperChange). |
| `goalie_jersey` | Int64 | Jersey number of the goalie entering the game (goalkeeperChange). |
| `goalie_name` | String | Full name of the goalie entering the game (goalkeeperChange). |
| `current_score` | String | Score after the goal, as `home:visitor` text, e.g. `1:0`. |
| `real_time` | String | Wall-clock local time, `YYYY-MM-DD HH:MM:SS`. |
| `scoreboard_time` | Int64 | Seconds of game time from the opening face-off (0-3600 in regulation, continuing through overtime). |
| `balance` | String | Goal strength: `EQ` even, `PP1` / `PP2` power play, `SH1` / `SH2` shorthanded. |
| `extra_attacker` | Boolean | True when the scoring team had an extra attacker on (goalie pulled). |
| `empty_net` | Boolean | True when the goal went into an empty net. |
| `shooting` | Boolean | True for a goal scored on a penalty shot. |
| `technical` | Boolean | True for a technical penalty (unverified). |
| `scorer_player_id` | Int64 | Player id (playerId key space) of the goal scorer. |
| `scorer_jersey` | Int64 | Jersey number of the goal scorer. |
| `scorer_name` | String | Full name of the goal scorer. |
| `assist1_player_id` | Int64 | Player id (playerId key space) of the first assist. |
| `assist1_jersey` | Int64 | Jersey number of the first assist. |
| `assist1_name` | String | Full name of the first assist. |
| `assist2_player_id` | Int64 | Player id (playerId key space) of the second assist. |
| `assist2_jersey` | Int64 | Jersey number of the second assist. |
| `assist2_name` | String | Full name of the second assist. |
| `plus_player_ids` | String | Comma-separated player ids (playerId key space) of the scoring team's players on ice. |
| `minus_player_ids` | String | Comma-separated player ids (playerId key space) of the conceding team's players on ice. |
| `team_id` | Int64 | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `team_shortcut` | String | Three-letter team code. |
| `goal_review` | String | Video review subject, e.g. a goal (EIHL `videoReview` events). |
| `on_ice_decision` | String | Call on the ice before the review. |
| `reason_for_review` | String | Why the play was reviewed (free text). |
| `decision_after_review` | String | Final call after the review. |
| `outgoing_goalie_player_id` | Int64 | Player id (playerId key space) of the goalie leaving the game (goalkeeperChange). |
| `outgoing_goalie_jersey` | Int64 | Jersey number of the goalie leaving the game (goalkeeperChange). |
| `outgoing_goalie_name` | String | Full name of the goalie leaving the game (goalkeeperChange). |
| `create_time` | Int64 | Game second at which the event was entered (unverified). |
| `penalized_player_id` | Int64 | Player id (playerId key space) of the penalized player. |
| `penalized_jersey` | Int64 | Jersey number of the penalized player. |
| `penalized_name` | String | Full name of the penalized player. |
| `penalty_from_real_time` | String | Penalty start, local wall clock YYYY-MM-DD HH:MM:SS. |
| `penalty_from_scoreboard_time` | Int64 | Penalty start, game second. |
| `penalty_to_real_time` | String | Penalty end, local wall clock YYYY-MM-DD HH:MM:SS. |
| `penalty_to_scoreboard_time` | Int64 | Penalty end, game second. |
| `duration` | Int64 | Penalty length in seconds (120 = minor). |
| `disciplined_functionary` | Boolean | True when the penalty was assessed to a team official. |
| `disciplined_bm` | Boolean | True when the penalty was a bench minor. |
| `codename` | String | Penalty code, e.g. `HOOK`, `TRIP`, `ROUGH`, `BOARD`, `DELAY`. |
| `replaced_player_id` | Int64 | Player id (playerId key space) of the player serving the penalty in the penalized player's place. |
| `replaced_jersey` | Int64 | Jersey number of the player serving the penalty in the penalized player's place. |
| `replaced_name` | String | Full name of the player serving the penalty in the penalized player's place. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#eihl_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The bucket has no such key (S3 404 NoSuchKey: unknown season, competition, match or team), or the file holds no rows (an unplayed game, an empty shot shell). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#eihl_pbp-example}

```python
eihl_pbp(match_id=4440)
```

## eihl_player_box

Player boxscore of one team in one match, one row per player.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/matches/{match_id}/team-stats/{team_id}.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/matches/4440/team-stats/96.json](https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/matches/4440/team-stats/96.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (per-bucket match key space: match_id in eihl_schedule). Not the league website's game id. |
| `team_id` | `team_id` |  | `Y` |  | Team id (per-bucket team key space: home_id / guest_id in eihl_schedule). |

### Returns {#eihl_player_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Player id (playerId key space, per bucket, stable across games and seasons). |
| `name` | String | Player full name (first and last). |
| `firstname` | String | First name of the player in this row. |
| `surname` | String | Player's last (family) name. |
| `birthdate` | String | Birth date `YYYY-MM-DD`. |
| `active` | Boolean | True when the player is active on the team. |
| `position` | String | Position code: `GK` goalie, `DE` defence, `FW` / `FO` forward (FO meaning unverified). |
| `stick` | String | Handedness: `left` or `right`; null when unknown. |
| `weight` | Int64 | Weight in kg; `0` placeholder when unknown. |
| `height` | Int64 | Height in cm; `0` placeholder when unknown. |
| `jersey` | Int64 | Player jersey (sweater) number. |
| `birth_country` | String | Player's country of birth (name). |
| `nationality` | String | Nationality, country name. |
| `nationality_short` | String | Nationality, three-letter code (`CAN`, `GER`; `UK` for Britain). |
| `team_shortcut` | String | Three-letter team code. |
| `games` | Int64 | Games played in the table's scope. |
| `goals` | Int64 | goals in total (goals_home + goals_away: the body splits it by home and away games). |
| `goals_home` | Int64 | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `goals_away` | Int64 | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `assists` | Int64 | assists in total (assists_home + assists_away: the body splits it by home and away games). |
| `assists_home` | Int64 | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `assists_away` | Int64 | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `points` | Int64 | points in total (points_home + points_away: the body splits it by home and away games). |
| `points_home` | Int64 | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `points_away` | Int64 | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `penalty_minutes` | Int64 | Penalty minutes assessed to the player (this game, or to date in the season / league-part files). |
| `positive` | Int64 | Plus: on ice for goals for at even strength or shorthanded. |
| `negative` | Int64 | Minus: on ice for goals against at even strength or on the power play. |
| `points_per_game` | Float64 | Average goals plus assists per game played. |
| `shot_efficiency` | Float64 | Shooting percentage, goals / shots on goal x 100. |
| `penalty_minutes_per_game` | Float64 | Average penalty minutes per game played. |
| `pp_goals` | Int64 | Power-play goals. |
| `sh_goals` | Int64 | Shorthanded goals. |
| `gw_goals` | Int64 | Game-winning goals. |
| `shots_attempts` | Int64 | Shot attempts (on goal + missed + blocked). |
| `shots_on_goal` | Int64 | shots_on_goal in total (shots_on_goal_home + shots_on_goal_away: the body splits it by home and away games). |
| `shots_on_goal_home` | Int64 | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `shots_on_goal_away` | Int64 | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `shots_missed` | Int64 | This player's shot attempts that missed the net. |
| `shots_blocked` | Int64 | This player's shot attempts that were blocked. |
| `blocked_shots_by_player` | Int64 | Opponent shots this player blocked. |
| `faceoffs_win` | Int64 | Face-offs the player won (this game, or to date in the season / league-part files). |
| `faceoffs_losses` | Int64 | Face-offs the player lost (this game, or to date in the season / league-part files). |
| `faceoffs_count` | Int64 | Face-offs the player took (this game, or to date in the season / league-part files). |
| `timeonice` | Float64 | Time on ice, seconds with decimals (tracking-based; differs from `timeOnIce`). |
| `timeonice_eq` | Float64 | Time on ice at even strength (`EQ`), on the power play (`PP`) or shorthanded (`SH`), seconds with decimals. |
| `timeonice_pp` | Float64 | Time on ice at even strength (`EQ`), on the power play (`PP`) or shorthanded (`SH`), seconds with decimals. |
| `timeonice_sh` | Float64 | Time on ice at even strength (`EQ`), on the power play (`PP`) or shorthanded (`SH`), seconds with decimals. |
| `hits` | Int64 | Body checks delivered. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#eihl_player_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The bucket has no such key (S3 404 NoSuchKey: unknown season, competition, match or team), or the file holds no rows (an unplayed game, an empty shot shell). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#eihl_player_box-example}

```python
eihl_player_box(match_id=4440, team_id=96)
```

## eihl_goalie_box

Goalie lines of one match, one row per goalie who played.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/matches/{match_id}/top-goalies.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/matches/4440/top-goalies.json](https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/matches/4440/top-goalies.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (per-bucket match key space: match_id in eihl_schedule). Not the league website's game id. |

### Returns {#eihl_goalie_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Player id (player key space, per bucket, stable across games and seasons); joins to `playerId` in rosters, events, boxscores and leaderboards. |
| `name` | String | Goaltender's first (given) name. |
| `surname` | String | Goaltender's last (family) name. |
| `birthdate` | String | Birth date `YYYY-MM-DD`. |
| `team_id` | Int64 | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `team_name` | String | Team display name as the feed prints it. |
| `team_shortcut` | String | Three-letter team code. |
| `jersey` | Int64 | Player jersey (sweater) number. |
| `matches` | Int64 | Games played (1 in a per-game file). |
| `won_matches` | Int64 | Games the goalie won. |
| `lost_matches` | Int64 | Games the goalie lost. |
| `saves` | Int64 | Shots on goal the goalie stopped. |
| `goals_against` | Int64 | Goals the goalie allowed. |
| `saves_percent` | Float64 | Save percentage x 100 (e.g. `85.71`). |
| `average` | Float64 | Goals-against average per 60 minutes. |
| `zeros` | Int64 | Shutouts by the goalie (`zeros`: games with no goal allowed). |
| `time_on_ice` | Int64 | Time in goal, seconds. |
| `seconds_per_game` | Float64 | Time in goal per game, seconds. |
| `minutes_in_play` | Float64 | Share of the team's game time in goal, percent (100.0 = the whole game). |
| `visible_games` | Boolean | Widget flag: show the milestone for that stat. |
| `visible_shutouts` | Boolean | Widget flag: show the milestone for that stat. |
| `visible_wins` | Boolean | Widget flag: show the milestone for that stat. |
| `stars_games` | Int64 | Widget star marker for that stat (unverified). |
| `stars_shutouts` | Int64 | Widget star marker for that stat (unverified). |
| `stars_wins` | Int64 | Widget star marker for that stat (unverified). |
| `milestone_games` | Float64 | Widget value for the next milestone of that stat (unverified). |
| `milestone_shutouts` | Float64 | Widget value for the next milestone of that stat (unverified). |
| `milestone_wins` | Float64 | Widget value for the next milestone of that stat (unverified). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#eihl_goalie_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The bucket has no such key (S3 404 NoSuchKey: unknown season, competition, match or team), or the file holds no rows (an unplayed game, an empty shot shell). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#eihl_goalie_box-example}

```python
eihl_goalie_box(match_id=4440)
```

## eihl_standings

Standings of one competition season, one row per team.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/table/{season}/{league_id}.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/table/2026/3.json](https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/table/2026/3.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season by its END year (2027 = 2026/27). The bucket keys seasons by their START year; the wrapper requests season - 1. |
| `league_id` | `league_id` |  |  | `Y` | Competition id. 3 = Elite League, 4 = Challenge Cup, 5 = playoffs. |

### Returns {#eihl_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group_id` | Int64 | Group id (group key space); null when the stage has no groups, so unobserved here. |
| `group_name` | String | Group name; empty string when the stage has no groups. |
| `complete` | Boolean | True when the standings include every finished game. |
| `team_id` | Int64 | Team id (per-bucket team key space); joins to home_id / guest_id in the schedule. |
| `part_team_id` | Int64 | Team-in-league-part id (its own key space: one team in one league part); no other captured file carries it. |
| `name` | String | Team display name as the feed prints it. |
| `team_original` | String | Team's registered name. |
| `shortcut` | String | Three-letter team code. |
| `position` | Int64 | Table position, 1 = first. |
| `points` | Int64 | Points in the standings table. |
| `games` | Int64 | Games played in the table's scope. |
| `wins` | Int64 | Wins in regulation (DEL, EIHL, Denmark) or all wins (ICEHL, see `winsReg`); definition per bucket unverified. |
| `wins_ot` | Int64 | Wins after overtime or shootout. |
| `wins_reg` | Int64 | Wins in regulation time. |
| `ties` | Int64 | Ties (0 under current rules). |
| `losts_ot` | Int64 | Losses after overtime or shootout. |
| `losts` | Int64 | Losses in regulation time. |
| `score1` | Int64 | Goals the team scored (goals for). |
| `score2` | Int64 | Goals conceded (goals against). |
| `goals_away` | Int64 | Goals scored in away games. |
| `last10` | String | Form over the last ten games as a result string; empty string in the samples. |
| `only_ot_wins` | Int64 | Wins in overtime. |
| `only_so_wins` | Int64 | Wins in a shootout. |
| `only_ot_losts` | Int64 | Losses in overtime. |
| `only_so_losts` | Int64 | Losses in a shootout. |
| `qualified` | Boolean | True when the team has clinched qualification. |
| `ppg` | Float64 | Points per game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#eihl_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The bucket has no such key (S3 404 NoSuchKey: unknown season, competition, match or team), or the file holds no rows (an unplayed game, an empty shot shell). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#eihl_standings-example}

```python
eihl_standings(league_id=3, season=2027)
```

## eihl_shots

Shots of one match with rink coordinates and result, one row per shot.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/visualization/shots/{match_id}.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/visualization/shots/4440.json](https://s3.eu-west-1.amazonaws.com/eihl.hokejovyzapis.cz/visualization/shots/4440.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (per-bucket match key space: match_id in eihl_schedule). Not the league website's game id. |

### Returns {#eihl_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | Match id (per-bucket match key space); joins to the {match_id} path param of the game wrappers. |
| `date` | String | Game date `YYYY-MM-DD`. |
| `home_id` | Int64 | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `home_name` | String | Team display name as the feed prints it. |
| `home_shortcut` | String | Three-letter team code. |
| `visitor_id` | Int64 | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `visitor_name` | String | Team display name as the feed prints it. |
| `visitor_shortcut` | String | Three-letter team code. |
| `status` | String | Match status: `AFTER_MATCH`, `LIVE`, `BEFORE_MATCH`. |
| `shot_id` | Int64 | Shot id within the shot files. |
| `player_id` | Int64 | Player id (player key space, per bucket, stable across games and seasons); joins to `playerId` in rosters, events, boxscores and leaderboards. |
| `jersey` | Int64 | Player jersey (sweater) number. |
| `first_name` | String | First name of the shooter. |
| `last_name` | String | Last name of the shooter. |
| `team_id` | Int64 | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `match_shot_resutl_id` | Int64 | Shot result code (the provider's typo kept): `4` = goal; `1` / `2` / `3` probably on goal / missed / blocked (unverified). A code, not a joinable id. |
| `time` | Int64 | Seconds of game time from the opening face-off (0-3600 in regulation, continuing through overtime). |
| `coordinate_x` | Float64 | Shot x along the rink LENGTH, centre ice = 0, about -100..100 (units unverified, plausibly percent of half-length). Normalised to the attacking direction: each team keeps one sign for the whole game, the home team positive in the three sample games. DEL sends two-decimal floats, ICEHL and EIHL whole numbers (`-71.0`). |
| `coordinate_y` | Float64 | Shot y across the rink WIDTH, centre line = 0, about -100..100 (units and which board is positive unverified). DEL two-decimal floats, ICEHL and EIHL whole numbers. |
| `real_date` | String | Wall-clock local time, `YYYY-MM-DD HH:MM:SS`. |
| `xg` | Float64 | Expected-goals value of the shot (ICEHL and EIHL only; null elsewhere). |
| `xg_type` | String | Shot-danger class from the xG model, e.g. LD / MD / HD (ICEHL and EIHL only; null elsewhere). |
| `shot_result_id` | Int64 | Shot result code, the same value as match_shot_resutl_id: 4 = goal (verified); 1, 2 and 3 (on goal / missed / blocked in the widgets) are unverified. |
| `is_goal` | Boolean | True when the shot result code is 4 (goal). |
| `team_side` | String | Team of the row: home or visitor. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#eihl_shots-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The bucket has no such key (S3 404 NoSuchKey: unknown season, competition, match or team), or the file holds no rows (an unplayed game, an empty shot shell). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#eihl_shots-example}

```python
eihl_shots(match_id=4440)
```
