---
title: DEL — DEL data feed (eSports hokejovyzapis S3 bucket, Germany)
sidebar_label: DEL data feed (eSports hokejovyzapis S3 bucket, Germany)
description: "DEL — DEL data feed (eSports hokejovyzapis S3 bucket, Germany) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# DEL — DEL data feed (eSports hokejovyzapis S3 bucket, Germany)

`sportsdataverse.hockey.del_` — 12 endpoints.

## del_schedule

Schedule and results of one competition season, one row per match, played or not.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/league-matches/{season}/{league_id}.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/league-matches/2026/1.json](https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/league-matches/2026/1.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season by its END year (2027 = 2026/27). The bucket keys seasons by their START year; the wrapper requests season - 1. |
| `league_id` | `league_id` |  |  | `Y` | Competition id. 1 = DEL regular season, 3 = DEL playoffs (also 2 = Test-Liga pre-season, 4 = MagentaSport Cup 2020/21, 5 = NHL Global Series 2022/23). |

### Returns {#del_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | Match id (per-bucket match key space); joins to the {match_id} path param of the game wrappers. |
| `start_date` | character | Scheduled face-off, local `YYYY-MM-DD HH:MM:SS`; DEL playoff placeholders read `0000-00-00 ...`. |
| `real_end_date` | character | Actual end, local `YYYY-MM-DD HH:MM:SS`. |
| `league_id` | integer | League id (per-bucket competition key space); joins to the `{league_id}` path param. |
| `league` | character | League display name, e.g. `DEL`, `EIHL`, `Metal Ligaen`. |
| `season` | integer | Season by its END year (2027 = 2026/27); the body's START-year key plus one. |
| `round` | character | Round number as a string (`"1"`, `"249"`); DEL values carry a leading U+FEFF byte-order mark, empty string in Denmark. |
| `home_id` | integer | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `home_name` | character | Team display name as the feed prints it. |
| `home_shortcut` | character | Three-letter team code. |
| `guest_id` | integer | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `guest_name` | character | Team display name as the feed prints it. |
| `guest_shortcut` | character | Three-letter team code. |
| `status` | character | Match status: `BEFORE_MATCH`, `LIVE`, `AFTER_MATCH`. |
| `overtime_count` | integer | Number of overtime periods played; null in the samples. |
| `extra_time` | logical | True when the game went to overtime. |
| `shooting` | logical | True when the game was decided by a shootout. |
| `contumacy` | logical | True when the result was awarded by forfeit. |
| `final_score_home` | integer | Home team goals in that span. |
| `final_score_guest` | integer | Visiting team goals in that span. |
| `first_period_score_home` | integer | Home team goals in that span. |
| `first_period_score_guest` | integer | Visiting team goals in that span. |
| `second_period_score_home` | integer | Home team goals in that span. |
| `second_period_score_guest` | integer | Visiting team goals in that span. |
| `third_period_score_home` | integer | Home team goals in that span. |
| `third_period_score_guest` | integer | Visiting team goals in that span. |
| `actual_time_name` | character | Display label of the game state in the bucket's language, e.g. `end`, `Ende`, `before game`. |
| `actual_time_alias` | character | Short game-state code: `K` = finished (Czech `konec`), `0` = not started. |
| `last_event_time` | integer | Game second of the latest event, of game time from the opening face-off (0-3600 in regulation, continuing through overtime). |
| `overtime_score_home` | integer | Home team goals in overtime (present once a game of the season went to overtime). |
| `overtime_score_guest` | integer | Visiting team goals in overtime (present once a game of the season went to overtime). |
| `shootout_score_home` | integer | Home team shootout goals (present once a game of the season went to a shootout). |
| `shootout_score_guest` | integer | Visiting team shootout goals (present once a game of the season went to a shootout). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#del_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The bucket has no such key (S3 404 NoSuchKey: unknown season, competition, match or team), or the file holds no rows (an unplayed game, an empty shot shell). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#del_schedule-example}

```python
del_schedule(league_id=1, season=2027)
```

## del_team_player_stats

Season player stats of one team in one competition, one row per player.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/league-team-stats/{season}/{league_id}/{team_id}.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/league-team-stats/2026/1/12.json](https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/league-team-stats/2026/1/12.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season by its END year (2027 = 2026/27). The bucket keys seasons by their START year; the wrapper requests season - 1. |
| `league_id` | `league_id` |  |  | `Y` | Competition id. 1 = DEL regular season, 3 = DEL playoffs (also 2 = Test-Liga pre-season, 4 = MagentaSport Cup 2020/21, 5 = NHL Global Series 2022/23). |
| `team_id` | `team_id` |  | `Y` |  | Team id (per-bucket team key space: home_id / guest_id in del_schedule). |

### Returns {#del_team_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player id (playerId key space, per bucket, stable across games and seasons). |
| `holema_id` | integer | Unobserved (null in every sample); an external registry id by its name, key space unknown, joins to nothing captured. |
| `name` | character | Player full name (first and last). |
| `firstname` | character | First name of the player in this row. |
| `surname` | character | Player's last (family) name. |
| `date_of_birth` | character | Birth date `YYYY-MM-DD`. |
| `active` | logical | True when the player is active on the team. |
| `position` | character | Position code: `GK` goalie, `DE` defence, `FW` / `FO` forward (FO meaning unverified). |
| `stick` | character | Handedness: `left` or `right`; null when unknown. |
| `weight` | integer | Weight in kg; `0` placeholder when unknown. |
| `height` | integer | Height in cm; `0` placeholder when unknown. |
| `jersey` | integer | Player jersey (sweater) number. |
| `birth_country` | character | Player's country of birth (name). |
| `nationality` | character | Nationality, country name. |
| `nationality_short` | character | Nationality, three-letter code (`CAN`, `GER`; `UK` for Britain). |
| `team_shortcut` | character | Three-letter team code. |
| `games` | integer | Games played in the table's scope. |
| `goals` | integer | goals in total (goals_home + goals_away: the body splits it by home and away games). |
| `goals_home` | integer | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `goals_away` | integer | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `assists` | integer | assists in total (assists_home + assists_away: the body splits it by home and away games). |
| `assists_home` | integer | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `assists_away` | integer | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `points` | integer | points in total (points_home + points_away: the body splits it by home and away games). |
| `points_home` | integer | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `points_away` | integer | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `penalty_minutes` | integer | Penalty minutes assessed to the player (this game, or to date in the season / league-part files). |
| `positive` | integer | Plus: on ice for goals for at even strength or shorthanded. |
| `negative` | integer | Minus: on ice for goals against at even strength or on the power play. |
| `points_per_game` | numeric | Average goals plus assists per game played. |
| `shot_efficiency` | numeric | Shooting percentage, goals / shots on goal x 100. |
| `penalty_minutes_per_game` | numeric | Average penalty minutes per game played. |
| `pp_goals` | integer | Power-play goals. |
| `sh_goals` | integer | Shorthanded goals. |
| `gw_goals` | integer | Game-winning goals. |
| `shots_attempts` | integer | Shot attempts (on goal + missed + blocked). |
| `shots_on_goal` | integer | shots_on_goal in total (shots_on_goal_home + shots_on_goal_away: the body splits it by home and away games). |
| `shots_on_goal_home` | integer | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `shots_on_goal_away` | integer | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `shots_missed` | integer | This player's shot attempts that missed the net. |
| `shots_blocked` | integer | This player's shot attempts that were blocked. |
| `blocked_shots_by_player` | integer | Opponent shots this player blocked. |
| `faceoffs_win` | integer | Face-offs the player won (this game, or to date in the season / league-part files). |
| `faceoffs_losses` | integer | Face-offs the player lost (this game, or to date in the season / league-part files). |
| `faceoffs_count` | integer | Face-offs the player took (this game, or to date in the season / league-part files). |
| `time_on_ice` | integer | Time on ice, whole seconds (`0` when not tracked). |
| `time_on_ice_pp` | integer | Time on ice on the power play (`PP`) or shorthanded (`SH`), whole seconds. |
| `time_on_ice_sh` | integer | Time on ice on the power play (`PP`) or shorthanded (`SH`), whole seconds. |
| `time_on_ice_per_game` | numeric | Time on ice per game, seconds. |
| `shifts` | integer | Shifts the player took (this game, or to date in the season / league-part files). |
| `shifts_per_game` | numeric | Average shifts per game played. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#del_team_player_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The bucket has no such key (S3 404 NoSuchKey: unknown season, competition, match or team), or the file holds no rows (an unplayed game, an empty shot shell). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#del_team_player_stats-example}

```python
del_team_player_stats(league_id=1, season=2027, team_id=12)
```

## del_top_scorers

Skater scoring leaderboard of one competition season.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/league-top-scorers/{season}/{league_id}.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/league-top-scorers/2026/1.json](https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/league-top-scorers/2026/1.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season by its END year (2027 = 2026/27). The bucket keys seasons by their START year; the wrapper requests season - 1. |
| `league_id` | `league_id` |  |  | `Y` | Competition id. 1 = DEL regular season, 3 = DEL playoffs (also 2 = Test-Liga pre-season, 4 = MagentaSport Cup 2020/21, 5 = NHL Global Series 2022/23). |

### Returns {#del_top_scorers-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player id (player key space, per bucket, stable across games and seasons); joins to `playerId` in rosters, events, boxscores and leaderboards. |
| `name` | character | Player's first (given) name. |
| `surname` | character | Player's last (family) name. |
| `team_id` | integer | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `team_name` | character | Team display name as the feed prints it. |
| `team_shortcut` | character | Three-letter team code. |
| `goals` | integer | Goals the player scored in the league-season. |
| `assists` | integer | Assists credited to the player in the league-season. |
| `points` | integer | Points, goals + assists. |
| `shots` | integer | Shots on goal (saved or scored). |
| `games` | integer | Games played (two names for the same count). |
| `penalty_minutes` | integer | Penalty minutes assessed to the player in the league-season. |
| `time_on_ice` | integer | Time on ice, seconds; `0` placeholder in the samples. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#del_top_scorers-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The bucket has no such key (S3 404 NoSuchKey: unknown season, competition, match or team), or the file holds no rows (an unplayed game, an empty shot shell). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#del_top_scorers-example}

```python
del_top_scorers(league_id=1, season=2027)
```

## del_faceoffs

Every faceoff of one match: time, dot, winner and loser.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/matches/{match_id}/faceoffs.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/matches/4444/faceoffs.json](https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/matches/4444/faceoffs.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (per-bucket match key space: match_id in del_schedule). Not the league website's game id. |

### Returns {#del_faceoffs-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | integer | Face-off id (face-off key space); no other captured file joins to it. |
| `time` | integer | Seconds of game time from the opening face-off (0-3600 in regulation, continuing through overtime). |
| `realtime` | character | Wall-clock local time, `YYYY-MM-DD HH:MM:SS`. |
| `position` | character | Face-off dot: `BULY_CENTRAL` or `BULY_{HOME\|AWAY}_{DEFENSE\|BLUE}_{LEFT\|RIGHT}`. |
| `position_shortcut` | character | Short dot code, e.g. `C`, `HDL` (home defence left), `HDR`. |
| `winner_id` | integer | Player id (player key space, per bucket, stable across games and seasons); joins to `playerId` in rosters, events, boxscores and leaderboards. |
| `winner_name` | character | Player full name (first and last). |
| `loser_id` | integer | Player id (playerId key space) of the faceoff loser (DEL spells the key losser, ICEHL looser). |
| `loser_name` | character | Full name of the faceoff loser. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#del_faceoffs-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The bucket has no such key (S3 404 NoSuchKey: unknown season, competition, match or team), or the file holds no rows (an unplayed game, an empty shot shell). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#del_faceoffs-example}

```python
del_faceoffs(match_id=4444)
```

## del_game_info

Game info of one match: state, arena, attendance, officials, coaches, period scores.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/matches/{match_id}/game-header.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/matches/4444/game-header.json](https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/matches/4444/game-header.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (per-bucket match key space: match_id in del_schedule). Not the league website's game id. |

### Returns {#del_game_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `actual_time_name` | character | Display label of the game state, e.g. `end` / `Ende`. |
| `actual_time_alias` | character | Short game-state code: `K` = finished. |
| `stadium` | character | Name of the arena the game was played in. |
| `number_of_viewers` | integer | Spectators at the game (attendance). |
| `referees_head_referee1_id` | integer | Official (referee/linesman) person id, its own key space; not a player id; no other captured file joins to it. |
| `referees_head_referee1_name` | character | Official's full name. |
| `referees_head_referee2_id` | integer | Official (referee/linesman) person id, its own key space; not a player id; no other captured file joins to it. |
| `referees_head_referee2_name` | character | Official's full name. |
| `referees_line_referee1_id` | integer | Official (referee/linesman) person id, its own key space; not a player id; no other captured file joins to it. |
| `referees_line_referee1_name` | character | Official's full name. |
| `referees_line_referee2_id` | integer | Official (referee/linesman) person id, its own key space; not a player id; no other captured file joins to it. |
| `referees_line_referee2_name` | character | Official's full name. |
| `team_info_home_id` | integer | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `team_info_home_name` | character | Team display name as the feed prints it. |
| `team_info_home_shortcut` | character | Three-letter team code. |
| `team_info_visitor_id` | integer | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `team_info_visitor_name` | character | Team display name as the feed prints it. |
| `team_info_visitor_shortcut` | character | Three-letter team code. |
| `trainers_visitor_head_coach_id` | integer | Coach person id, its own key space; not a player id; no other captured file joins to it. |
| `trainers_visitor_head_coach_name` | character | Head coach full name. |
| `trainers_home_head_coach_id` | integer | Coach person id, its own key space; not a player id; no other captured file joins to it. |
| `trainers_home_head_coach_name` | character | Head coach full name. |
| `team_bilance_home_win` | integer | Wins of that team in the head-to-head record (unverified). |
| `team_bilance_visitor_win` | integer | Wins of that team in the head-to-head record (unverified). |
| `results_extra_time` | logical | True when the game went to overtime. |
| `results_shooting` | logical | True when the game was decided by a shootout. |
| `results_contumacy` | logical | True when the result was awarded by forfeit. |
| `results_score_final_score_home` | integer | Home team goals in that span. |
| `results_score_final_score_guest` | integer | Visiting team goals in that span. |
| `results_score_first_period_score_home` | integer | Home team goals in that span. |
| `results_score_first_period_score_guest` | integer | Visiting team goals in that span. |
| `results_score_second_period_score_home` | integer | Home team goals in that span. |
| `results_score_second_period_score_guest` | integer | Visiting team goals in that span. |
| `results_score_third_period_score_home` | integer | Home team goals in that span. |
| `results_score_third_period_score_guest` | integer | Visiting team goals in that span. |
| `best_players_home_id` | integer | Player id (player key space, per bucket, stable across games and seasons); joins to `playerId` in rosters, events, boxscores and leaderboards. |
| `best_players_home_name` | character | Player full name (first and last). |
| `best_players_visitor_id` | integer | Player id (player key space, per bucket, stable across games and seasons); joins to `playerId` in rosters, events, boxscores and leaderboards. |
| `best_players_visitor_name` | character | Player full name (first and last). |
| `overtime_count` | integer | Number of overtime periods; null in the samples. |
| `last_event_time` | integer | Game second of the latest event (3600 = end of regulation). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#del_game_info-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The bucket has no such key (S3 404 NoSuchKey: unknown season, competition, match or team), or the file holds no rows (an unplayed game, an empty shot shell). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#del_game_info-example}

```python
del_game_info(match_id=4444)
```

## del_pbp

Play-by-play of one match: goals, penalties, goalie changes and timeouts, one row per event.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/matches/{match_id}/period-events.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/matches/4444/period-events.json](https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/matches/4444/period-events.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (per-bucket match key space: match_id in del_schedule). Not the league website's game id. |

### Returns {#del_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `event_index` | integer | Row order of the event within the game (0-based), across periods. |
| `period` | character | Period key as the body sends it: 1, 2, 3, OT (overtime) or SO (shootout). |
| `period_number` | integer | Period number: 1-3, 4 = overtime, 5 = shootout. |
| `time` | integer | Game second of the event from the opening face-off (0-3600 in regulation, continuing through overtime). |
| `event_type` | character | Event type: goal, penalty, goalkeeperChange, timeout, periodStart, periodEnd, videoReview, ... |
| `event_id` | integer | Event id within its event type. |
| `team_side` | character | Team of the row: home or visitor. |
| `goalie_player_id` | integer | Player id (playerId key space) of the goalie entering the game (goalkeeperChange). |
| `goalie_jersey` | integer | Jersey number of the goalie entering the game (goalkeeperChange). |
| `goalie_name` | character | Full name of the goalie entering the game (goalkeeperChange). |
| `current_score` | character | Score after the goal, as `home:visitor` text, e.g. `1:0`. |
| `real_time` | character | Wall-clock local time, `YYYY-MM-DD HH:MM:SS`. |
| `scoreboard_time` | integer | Seconds of game time from the opening face-off (0-3600 in regulation, continuing through overtime). |
| `balance` | character | Goal strength: `EQ` even, `PP1` / `PP2` power play, `SH1` / `SH2` shorthanded. |
| `extra_attacker` | logical | True when the scoring team had an extra attacker on (goalie pulled). |
| `empty_net` | logical | True when the goal went into an empty net. |
| `shooting` | logical | True for a goal scored on a penalty shot. |
| `technical` | logical | True for a technical penalty (unverified). |
| `scorer_player_id` | integer | Player id (playerId key space) of the goal scorer. |
| `scorer_jersey` | integer | Jersey number of the goal scorer. |
| `scorer_name` | character | Full name of the goal scorer. |
| `assist1_player_id` | integer | Player id (playerId key space) of the first assist. |
| `assist1_jersey` | integer | Jersey number of the first assist. |
| `assist1_name` | character | Full name of the first assist. |
| `assist2_player_id` | integer | Player id (playerId key space) of the second assist. |
| `assist2_jersey` | integer | Jersey number of the second assist. |
| `assist2_name` | character | Full name of the second assist. |
| `plus_player_ids` | character | Comma-separated player ids (playerId key space) of the scoring team's players on ice. |
| `minus_player_ids` | character | Comma-separated player ids (playerId key space) of the conceding team's players on ice. |
| `create_time` | integer | Game second at which the event was entered (unverified). |
| `penalized_player_id` | integer | Player id (playerId key space) of the penalized player. |
| `penalized_jersey` | integer | Jersey number of the penalized player. |
| `penalized_name` | character | Full name of the penalized player. |
| `penalty_from_real_time` | character | Penalty start, local wall clock YYYY-MM-DD HH:MM:SS. |
| `penalty_from_scoreboard_time` | integer | Penalty start, game second. |
| `penalty_to_real_time` | character | Penalty end, local wall clock YYYY-MM-DD HH:MM:SS. |
| `penalty_to_scoreboard_time` | integer | Penalty end, game second. |
| `duration` | integer | Penalty length in seconds (120 = minor). |
| `disciplined_functionary` | logical | True when the penalty was assessed to a team official. |
| `disciplined_bm` | logical | True when the penalty was a bench minor. |
| `codename` | character | Penalty code, e.g. `HOOK`, `TRIP`, `ROUGH`, `BOARD`, `DELAY`. |
| `replaced_player_id` | integer | Player id (playerId key space) of the player serving the penalty in the penalized player's place. |
| `replaced_jersey` | integer | Jersey number of the player serving the penalty in the penalized player's place. |
| `replaced_name` | character | Full name of the player serving the penalty in the penalized player's place. |
| `outgoing_goalie_player_id` | integer | Player id (playerId key space) of the goalie leaving the game (goalkeeperChange). |
| `outgoing_goalie_jersey` | integer | Jersey number of the goalie leaving the game (goalkeeperChange). |
| `outgoing_goalie_name` | character | Full name of the goalie leaving the game (goalkeeperChange). |
| `team_id` | integer | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `team_shortcut` | character | Three-letter team code. |
| `goal_review` | character | Video review subject, e.g. a goal (EIHL `videoReview` events). |
| `on_ice_decision` | character | Call on the ice before the review. |
| `reason_for_review` | character | Why the play was reviewed (free text). |
| `decision_after_review` | character | Final call after the review. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#del_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The bucket has no such key (S3 404 NoSuchKey: unknown season, competition, match or team), or the file holds no rows (an unplayed game, an empty shot shell). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#del_pbp-example}

```python
del_pbp(match_id=4444)
```

## del_roster

Game roster of one match by line slot, one row per dressed player.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/matches/{match_id}/roster.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/matches/4444/roster.json](https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/matches/4444/roster.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (per-bucket match key space: match_id in del_schedule). Not the league website's game id. |

### Returns {#del_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_side` | character | Team of the row: home or visitor. |
| `slot` | character | Line slot, three digits <group><line><position>: group 1 = goalies (101, 102), 2 = defence pairs, 3 = forward lines. |
| `player_id` | integer | Player id (player key space, per bucket, stable across games and seasons); joins to `playerId` in rosters, events, boxscores and leaderboards. |
| `match_player_id` | integer | Match-player id (one row per player per game; a different key space from the player id); joins to `matchPlayerId` in the same game's roster and events only. |
| `name` | character | Player's first (given) name. |
| `surname` | character | Player's last (family) name. |
| `jersey` | integer | Player jersey (sweater) number. |
| `position` | character | Position code: `GK` goalie, `DE` defence, `FW` / `FO` forward (FO meaning unverified). |
| `captain` | logical | True for the team captain. |
| `starting_six` | logical | True for the six players who started the game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#del_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The bucket has no such key (S3 404 NoSuchKey: unknown season, competition, match or team), or the file holds no rows (an unplayed game, an empty shot shell). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#del_roster-example}

```python
del_roster(match_id=4444)
```

## del_shifts

Every shift of one match, one row per player shift.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/matches/{match_id}/shiftsSC.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/matches/4444/shiftsSC.json](https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/matches/4444/shiftsSC.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (per-bucket match key space: match_id in del_schedule). Not the league website's game id. |

### Returns {#del_shifts-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `shift_id` | integer | Shift id within the shift files. |
| `start_time` | integer | Shift start, game second. |
| `start_real_time` | character | Shift start, local wall clock. |
| `end_time` | integer | Shift end, game second. |
| `end_real_time` | character | Shift end, local wall clock. |
| `player_id` | integer | Player id (player key space, per bucket, stable across games and seasons); joins to `playerId` in rosters, events, boxscores and leaderboards. |
| `player_name` | character | Player full name (first and last). |
| `team_id` | integer | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `team_name` | character | Team display name as the feed prints it. |
| `duration` | integer | Shift length in seconds (end_time - start_time). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#del_shifts-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The bucket has no such key (S3 404 NoSuchKey: unknown season, competition, match or team), or the file holds no rows (an unplayed game, an empty shot shell). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#del_shifts-example}

```python
del_shifts(match_id=4444)
```

## del_player_box

Player boxscore of one team in one match, one row per player.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/matches/{match_id}/team-stats/{team_id}.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/matches/4444/team-stats/12.json](https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/matches/4444/team-stats/12.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (per-bucket match key space: match_id in del_schedule). Not the league website's game id. |
| `team_id` | `team_id` |  | `Y` |  | Team id (per-bucket team key space: home_id / guest_id in del_schedule). |

### Returns {#del_player_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player id (playerId key space, per bucket, stable across games and seasons). |
| `holema_id` | integer | Unobserved (null in every sample); an external registry id by its name, key space unknown, joins to nothing captured. |
| `name` | character | Player full name (first and last). |
| `firstname` | character | First name of the player in this row. |
| `surname` | character | Player's last (family) name. |
| `date_of_birth` | character | Birth date `YYYY-MM-DD`. |
| `active` | logical | True when the player is active on the team. |
| `position` | character | Position code: `GK` goalie, `DE` defence, `FW` / `FO` forward (FO meaning unverified). |
| `stick` | character | Handedness: `left` or `right`; null when unknown. |
| `weight` | integer | Weight in kg; `0` placeholder when unknown. |
| `height` | integer | Height in cm; `0` placeholder when unknown. |
| `jersey` | integer | Player jersey (sweater) number. |
| `birth_country` | character | Player's country of birth (name). |
| `nationality` | character | Nationality, country name. |
| `nationality_short` | character | Nationality, three-letter code (`CAN`, `GER`; `UK` for Britain). |
| `team_shortcut` | character | Three-letter team code. |
| `games` | integer | Games played in the table's scope. |
| `goals` | integer | goals in total (goals_home + goals_away: the body splits it by home and away games). |
| `goals_home` | integer | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `goals_away` | integer | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `assists` | integer | assists in total (assists_home + assists_away: the body splits it by home and away games). |
| `assists_home` | integer | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `assists_away` | integer | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `points` | integer | points in total (points_home + points_away: the body splits it by home and away games). |
| `points_home` | integer | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `points_away` | integer | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `penalty_minutes` | integer | Penalty minutes assessed to the player (this game, or to date in the season / league-part files). |
| `positive` | integer | Plus: on ice for goals for at even strength or shorthanded. |
| `negative` | integer | Minus: on ice for goals against at even strength or on the power play. |
| `points_per_game` | numeric | Average goals plus assists per game played. |
| `shot_efficiency` | numeric | Shooting percentage, goals / shots on goal x 100. |
| `penalty_minutes_per_game` | numeric | Average penalty minutes per game played. |
| `pp_goals` | integer | Power-play goals. |
| `sh_goals` | integer | Shorthanded goals. |
| `gw_goals` | integer | Game-winning goals. |
| `shots_attempts` | integer | Shot attempts (on goal + missed + blocked). |
| `shots_on_goal` | integer | shots_on_goal in total (shots_on_goal_home + shots_on_goal_away: the body splits it by home and away games). |
| `shots_on_goal_home` | integer | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `shots_on_goal_away` | integer | Split by venue: `home` = in the player's home games, `away` = in away games. |
| `shots_missed` | integer | This player's shot attempts that missed the net. |
| `shots_blocked` | integer | This player's shot attempts that were blocked. |
| `blocked_shots_by_player` | integer | Opponent shots this player blocked. |
| `faceoffs_win` | integer | Face-offs the player won (this game, or to date in the season / league-part files). |
| `faceoffs_losses` | integer | Face-offs the player lost (this game, or to date in the season / league-part files). |
| `faceoffs_count` | integer | Face-offs the player took (this game, or to date in the season / league-part files). |
| `time_on_ice` | integer | Time on ice, whole seconds (`0` when not tracked). |
| `time_on_ice_pp` | integer | Time on ice on the power play (`PP`) or shorthanded (`SH`), whole seconds. |
| `time_on_ice_sh` | integer | Time on ice on the power play (`PP`) or shorthanded (`SH`), whole seconds. |
| `time_on_ice_per_game` | numeric | Time on ice per game, seconds. |
| `shifts` | integer | Shifts the player took (this game, or to date in the season / league-part files). |
| `shifts_per_game` | numeric | Average shifts per game played. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#del_player_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The bucket has no such key (S3 404 NoSuchKey: unknown season, competition, match or team), or the file holds no rows (an unplayed game, an empty shot shell). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#del_player_box-example}

```python
del_player_box(match_id=4444, team_id=12)
```

## del_goalie_box

Goalie lines of one match, one row per goalie who played.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/matches/{match_id}/top-goalies.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/matches/4444/top-goalies.json](https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/matches/4444/top-goalies.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (per-bucket match key space: match_id in del_schedule). Not the league website's game id. |

### Returns {#del_goalie_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player id (player key space, per bucket, stable across games and seasons); joins to `playerId` in rosters, events, boxscores and leaderboards. |
| `name` | character | Goaltender's first (given) name. |
| `surname` | character | Goaltender's last (family) name. |
| `team_id` | integer | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `team_name` | character | Team display name as the feed prints it. |
| `team_shortcut` | character | Three-letter team code. |
| `jersey` | integer | Player jersey (sweater) number. |
| `matches` | integer | Games played (1 in a per-game file). |
| `won_matches` | integer | Games the goalie won. |
| `lost_matches` | integer | Games the goalie lost. |
| `saves` | integer | Shots on goal the goalie stopped. |
| `goals_against` | integer | Goals the goalie allowed. |
| `saves_percent` | numeric | Save percentage x 100 (e.g. `85.71`). |
| `average` | numeric | Goals-against average per 60 minutes. |
| `zeros` | integer | Shutouts by the goalie (`zeros`: games with no goal allowed). |
| `time_on_ice` | integer | Time in goal, seconds. |
| `seconds_per_game` | numeric | Time in goal per game, seconds. |
| `minutes_in_play` | numeric | Share of the team's game time in goal, percent (100.0 = the whole game). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#del_goalie_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The bucket has no such key (S3 404 NoSuchKey: unknown season, competition, match or team), or the file holds no rows (an unplayed game, an empty shot shell). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#del_goalie_box-example}

```python
del_goalie_box(match_id=4444)
```

## del_standings

Standings of one competition season, one row per team.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/tables/{season}/{league_id}.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/tables/2026/1.json](https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/tables/2026/1.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season by its END year (2027 = 2026/27). The bucket keys seasons by their START year; the wrapper requests season - 1. |
| `league_id` | `league_id` |  |  | `Y` | Competition id. 1 = DEL regular season, 3 = DEL playoffs (also 2 = Test-Liga pre-season, 4 = MagentaSport Cup 2020/21, 5 = NHL Global Series 2022/23). |

### Returns {#del_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_part_id` | integer | League part id (league-part key space, per bucket: one stage of a competition, e.g. ICEHL 56 = 2026/27 regular season); joins to `league_part_id` in league-matches and the `{league_part_id}` path param. |
| `league_part_name` | character | League part name, e.g. `DEL 2026/27`. |
| `state` | character | Standings state flag (e.g. live or final; values unverified). |
| `datetime` | character | Standings computed at, local time, `YYYY-MM-DD HH:MM:SS`. |
| `team_id` | integer | Team id (per-bucket team key space); joins to home_id / guest_id in the schedule. |
| `part_team_id` | integer | Team-in-league-part id (its own key space: one team in one league part); no other captured file carries it. |
| `name` | character | Team display name as the feed prints it. |
| `shortcut` | character | Three-letter team code. |
| `position` | integer | Table position, 1 = first. |
| `points` | integer | Points in the standings table. |
| `games` | integer | Games played in the table's scope. |
| `wins` | integer | Wins in regulation (DEL, EIHL, Denmark) or all wins (ICEHL, see `winsReg`); definition per bucket unverified. |
| `wins_ot` | integer | Wins after overtime or shootout. |
| `ties` | integer | Ties (0 under current rules). |
| `losses_ot` | integer | Losses after overtime or shootout. |
| `losses` | integer | Losses in regulation time. |
| `score1` | integer | Goals the team scored (goals for). |
| `score2` | integer | Goals conceded (goals against). |
| `only_so_wins` | integer | Wins in a shootout. |
| `only_so_losts` | integer | Losses in a shootout. |
| `only_ot_wins` | integer | Wins in overtime. |
| `only_ot_losts` | integer | Losses in overtime. |
| `bonus` | integer | Bonus or deduction points (DEL). |
| `ppg` | numeric | Points per game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#del_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The bucket has no such key (S3 404 NoSuchKey: unknown season, competition, match or team), or the file holds no rows (an unplayed game, an empty shot shell). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#del_standings-example}

```python
del_standings(league_id=1, season=2027)
```

## del_shots

Shots of one match with rink coordinates and result, one row per shot.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/visualization/shots/{match_id}.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/visualization/shots/4444.json](https://s3.eu-west-1.amazonaws.com/de.hokejovyzapis.cz/visualization/shots/4444.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (per-bucket match key space: match_id in del_schedule). Not the league website's game id. |

### Returns {#del_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | Match id (per-bucket match key space); joins to the {match_id} path param of the game wrappers. |
| `date` | character | Game date `YYYY-MM-DD`. |
| `home_id` | integer | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `home_name` | character | Team display name as the feed prints it. |
| `home_shortcut` | character | Three-letter team code. |
| `visitor_id` | integer | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `visitor_name` | character | Team display name as the feed prints it. |
| `visitor_shortcut` | character | Three-letter team code. |
| `shot_id` | integer | Shot id within the shot files. |
| `player_id` | integer | Player id (player key space, per bucket, stable across games and seasons); joins to `playerId` in rosters, events, boxscores and leaderboards. |
| `jersey` | integer | Player jersey (sweater) number. |
| `first_name` | character | First name of the shooter. |
| `last_name` | character | Last name of the shooter. |
| `team_id` | integer | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `match_shot_resutl_id` | integer | Shot result code (the provider's typo kept): `4` = goal; `1` / `2` / `3` probably on goal / missed / blocked (unverified). A code, not a joinable id. |
| `coordinate_x` | numeric | Shot x along the rink LENGTH, centre ice = 0, about -100..100 (units unverified, plausibly percent of half-length). Normalised to the attacking direction: each team keeps one sign for the whole game, the home team positive in the three sample games. DEL sends two-decimal floats, ICEHL and EIHL whole numbers (`-71.0`). |
| `coordinate_y` | numeric | Shot y across the rink WIDTH, centre line = 0, about -100..100 (units and which board is positive unverified). DEL two-decimal floats, ICEHL and EIHL whole numbers. |
| `real_date` | character | Wall-clock local time, `YYYY-MM-DD HH:MM:SS`. |
| `polygon` | character | Rink area of the shot, `{home\|away}_{top\|center\|bottom\|defense}_polygon` (DEL). |
| `time` | integer | Seconds of game time from the opening face-off (0-3600 in regulation, continuing through overtime). |
| `shot_result_id` | integer | Shot result code, the same value as match_shot_resutl_id: 4 = goal (verified); 1, 2 and 3 (on goal / missed / blocked in the widgets) are unverified. |
| `is_goal` | logical | True when the shot result code is 4 (goal). |
| `team_side` | character | Team of the row: home or visitor. |
| `xg` | numeric | Expected-goals value of the shot (ICEHL and EIHL only; null elsewhere). |
| `xg_type` | character | Shot-danger class from the xG model, e.g. LD / MD / HD (ICEHL and EIHL only; null elsewhere). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#del_shots-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The bucket has no such key (S3 404 NoSuchKey: unknown season, competition, match or team), or the file holds no rows (an unplayed game, an empty shot shell). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#del_shots-example}

```python
del_shots(match_id=4444)
```
