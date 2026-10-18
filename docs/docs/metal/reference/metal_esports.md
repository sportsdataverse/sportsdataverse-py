---
title: METAL — Metal Ligaen data feed (eSports hokejovyzapis S3 bucket, Denmark)
sidebar_label: Metal Ligaen data feed (eSports hokejovyzapis S3 bucket, Denmark)
description: "METAL — Metal Ligaen data feed (eSports hokejovyzapis S3 bucket, Denmark) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# METAL — Metal Ligaen data feed (eSports hokejovyzapis S3 bucket, Denmark)

`sportsdataverse.hockey.metal` — 8 endpoints.

## metal_schedule

Schedule and results of one competition season, one row per match, played or not.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/league-matches/{season}/{league_id}.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/league-matches/2026/1.json](https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/league-matches/2026/1.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season by its END year (2027 = 2026/27). The bucket keys seasons by their START year; the wrapper requests season - 1. |
| `league_id` | `league_id` |  |  | `Y` | Competition id. 1 = Metal Ligaen, regular season and playoffs in one file (split them by league_part_name); 2 = pre-season, 4 = Final-4 cup. |

### Returns {#metal_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | Match id (per-bucket match key space); joins to the {match_id} path param of the game wrappers. |
| `start_date` | character | Scheduled face-off, local `YYYY-MM-DD HH:MM:SS`; DEL playoff placeholders read `0000-00-00 ...`. |
| `real_start_date` | character | Actual start, local `YYYY-MM-DD HH:MM:SS`; `0000-00-00 00:00:00` placeholder before the game. |
| `real_end_date` | character | Actual end, local `YYYY-MM-DD HH:MM:SS`. |
| `league_id` | integer | League id (per-bucket competition key space); joins to the `{league_id}` path param. |
| `league` | character | League display name, e.g. `DEL`, `EIHL`, `Metal Ligaen`. |
| `league_part_id` | integer | League part id (league-part key space, per bucket: one stage of a competition, e.g. ICEHL 56 = 2026/27 regular season); joins to `league_part_id` in league-matches and the `{league_part_id}` path param. |
| `league_part_name` | character | League part name, e.g. `ICEHL 2026/27` or `... - Playoffs`; splits regular season from playoffs where one league id holds both. |
| `season` | integer | Season by its END year (2027 = 2026/27); the body's START-year key plus one. |
| `round` | character | Round number as a string (`"1"`, `"249"`); DEL values carry a leading U+FEFF byte-order mark, empty string in Denmark. |
| `group` | character | Group name in group stages; empty string or null otherwise. |
| `tickets_url` | character | URL of the ticket shop for the match. |
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
| `arena` | character | Arena name; empty string or null where not set. |
| `advanced_statistics` | logical | True when the game has advanced stats files (shots, boxscore). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#metal_schedule-example}

```python
metal_schedule(league_id=1, season=2027)
```

_Last validated n/a._

## metal_top_scorers

Skater scoring leaderboard of one competition season.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/league-top-scorers/{season}/{league_id}.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/league-top-scorers/2026/1.json](https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/league-top-scorers/2026/1.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season by its END year (2027 = 2026/27). The bucket keys seasons by their START year; the wrapper requests season - 1. |
| `league_id` | `league_id` |  |  | `Y` | Competition id. 1 = Metal Ligaen, regular season and playoffs in one file (split them by league_part_name); 2 = pre-season, 4 = Final-4 cup. |

### Returns {#metal_top_scorers-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player id (player key space, per bucket, stable across games and seasons); joins to `playerId` in rosters, events, boxscores and leaderboards. |
| `name` | character | Player's first (given) name. |
| `surname` | character | Player's last (family) name. |
| `birthdate` | character | Birth date `YYYY-MM-DD`. |
| `team_id` | integer | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `team_name` | character | Team display name as the feed prints it. |
| `team_shortcut` | character | Three-letter team code. |
| `goals` | integer | Goals the player scored in the league-season. |
| `assists` | integer | Assists credited to the player in the league-season. |
| `points` | integer | Points, goals + assists. |
| `shots` | integer | Shots on goal (saved or scored). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#metal_top_scorers-example}

```python
metal_top_scorers(league_id=1, season=2027)
```

_Last validated n/a._

## metal_game_info

Game info of one match: state, arena, attendance, officials, coaches, period scores.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/matches/{match_id}/game-header.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/matches/575/game-header.json](https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/matches/575/game-header.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (per-bucket match key space: match_id in metal_schedule). Not the league website's game id. |

### Returns {#metal_game_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `actual_time_name` | character | Display label of the game state, e.g. `end` / `Ende`. |
| `actual_time_alias` | character | Short game-state code: `K` = finished. |
| `status` | character | Match status: `BEFORE_MATCH`, `LIVE`, `AFTER_MATCH`. |
| `stadium` | character | Name of the arena the game was played in. |
| `number_of_viewers` | integer | Spectators at the game (attendance). |
| `referees_head_referee1` | character | Official's name (ICEHL/EIHL/Denmark: a full-name string; DEL: an object with `id` and `name`). |
| `referees_head_referee2` | character | Official's name (ICEHL/EIHL/Denmark: a full-name string; DEL: an object with `id` and `name`). |
| `referees_line_referee1` | character | Official's name (ICEHL/EIHL/Denmark: a full-name string; DEL: an object with `id` and `name`). |
| `referees_line_referee2` | character | Official's name (ICEHL/EIHL/Denmark: a full-name string; DEL: an object with `id` and `name`). |
| `referees_head_referee1number` | integer | Official's sweater number; null when not recorded. |
| `referees_head_referee2number` | integer | Official's sweater number; null when not recorded. |
| `referees_line_referee1number` | integer | Official's sweater number; null when not recorded. |
| `referees_line_referee2number` | integer | Official's sweater number; null when not recorded. |
| `referees_head_referee1id` | integer | Official (referee/linesman) person id, its own key space; not a player id; no other captured file joins to it. |
| `referees_head_referee2id` | integer | Official (referee/linesman) person id, its own key space; not a player id; no other captured file joins to it. |
| `referees_line_referee1id` | integer | Official (referee/linesman) person id, its own key space; not a player id; no other captured file joins to it. |
| `referees_line_referee2id` | integer | Official (referee/linesman) person id, its own key space; not a player id; no other captured file joins to it. |
| `team_info_home_id` | integer | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `team_info_home_name` | character | Team display name as the feed prints it. |
| `team_info_home_shortcut` | character | Three-letter team code. |
| `team_info_visitor_id` | integer | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `team_info_visitor_name` | character | Team display name as the feed prints it. |
| `team_info_visitor_shortcut` | character | Three-letter team code. |
| `trainers_home_head_coach` | character | Head coach (ICEHL/EIHL/Denmark: a full-name string; DEL: an object with `id` and `name`). |
| `trainers_visitor_head_coach` | character | Head coach (ICEHL/EIHL/Denmark: a full-name string; DEL: an object with `id` and `name`). |
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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#metal_game_info-example}

```python
metal_game_info(match_id=575)
```

_Last validated n/a._

## metal_pbp

Play-by-play of one match: goals, penalties, goalie changes and timeouts, one row per event.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/matches/{match_id}/period-events.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/matches/575/period-events.json](https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/matches/575/period-events.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (per-bucket match key space: match_id in metal_schedule). Not the league website's game id. |

### Returns {#metal_pbp-returns}

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
| `create_time` | integer | Game second at which the event was entered (unverified). |
| `penalized_player_id` | integer | Player id (playerId key space) of the penalized player. |
| `penalized_jersey` | integer | Jersey number of the penalized player. |
| `penalized_name` | character | Full name of the penalized player. |
| `penalty_from_real_time` | character | Penalty start, local wall clock YYYY-MM-DD HH:MM:SS. |
| `penalty_from_scoreboard_time` | integer | Penalty start, game second. |
| `penalty_to_real_time` | character | Penalty end, local wall clock YYYY-MM-DD HH:MM:SS. |
| `penalty_to_scoreboard_time` | integer | Penalty end, game second. |
| `duration` | integer | Penalty length in seconds (120 = minor). |
| `shooting` | logical | True for a goal scored on a penalty shot. |
| `disciplined_functionary` | logical | True when the penalty was assessed to a team official. |
| `disciplined_bm` | logical | True when the penalty was a bench minor. |
| `codename` | character | Penalty code, e.g. `HOOK`, `TRIP`, `ROUGH`, `BOARD`, `DELAY`. |
| `current_score` | character | Score after the goal, as `home:visitor` text, e.g. `1:0`. |
| `real_time` | character | Wall-clock local time, `YYYY-MM-DD HH:MM:SS`. |
| `scoreboard_time` | integer | Seconds of game time from the opening face-off (0-3600 in regulation, continuing through overtime). |
| `balance` | character | Goal strength: `EQ` even, `PP1` / `PP2` power play, `SH1` / `SH2` shorthanded. |
| `extra_attacker` | logical | True when the scoring team had an extra attacker on (goalie pulled). |
| `empty_net` | logical | True when the goal went into an empty net. |
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

### Example {#metal_pbp-example}

```python
metal_pbp(match_id=575)
```

_Last validated n/a._

## metal_player_box

Player boxscore of one team in one match, one row per player.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/matches/{match_id}/team-stats/{team_id}.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/matches/575/team-stats/8.json](https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/matches/575/team-stats/8.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (per-bucket match key space: match_id in metal_schedule). Not the league website's game id. |
| `team_id` | `team_id` |  | `Y` |  | Team id (per-bucket team key space: home_id / guest_id in metal_schedule). |

### Returns {#metal_player_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player id (playerId key space, per bucket, stable across games and seasons). |
| `name` | character | Player full name (first and last). |
| `firstname` | character | First name of the player in this row. |
| `surname` | character | Player's last (family) name. |
| `birthdate` | character | Birth date `YYYY-MM-DD`. |
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
| `timeonice` | numeric | Time on ice, seconds with decimals (tracking-based; differs from `timeOnIce`). |
| `timeonice_eq` | numeric | Time on ice at even strength (`EQ`), on the power play (`PP`) or shorthanded (`SH`), seconds with decimals. |
| `timeonice_pp` | numeric | Time on ice at even strength (`EQ`), on the power play (`PP`) or shorthanded (`SH`), seconds with decimals. |
| `timeonice_sh` | numeric | Time on ice at even strength (`EQ`), on the power play (`PP`) or shorthanded (`SH`), seconds with decimals. |
| `hits` | integer | Body checks delivered. |
| `xg` | numeric | Expected goals of the player's shots. |
| `top5` | logical | True when the player ranks in the team's top five. |
| `rank` | integer | Rank of the player in the file's default sort (points). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#metal_player_box-example}

```python
metal_player_box(match_id=575, team_id=8)
```

_Last validated n/a._

## metal_goalie_box

Goalie lines of one match, one row per goalie who played.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/matches/{match_id}/top-goalies.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/matches/575/top-goalies.json](https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/matches/575/top-goalies.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (per-bucket match key space: match_id in metal_schedule). Not the league website's game id. |

### Returns {#metal_goalie_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player id (player key space, per bucket, stable across games and seasons); joins to `playerId` in rosters, events, boxscores and leaderboards. |
| `name` | character | Goaltender's first (given) name. |
| `surname` | character | Goaltender's last (family) name. |
| `photo` | character | Goalie photo URL; null when none. |
| `birthdate` | character | Birth date `YYYY-MM-DD`. |
| `team_id` | integer | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `team_name` | character | Team display name as the feed prints it. |
| `team_shortcut` | character | Three-letter team code. |
| `team_type` | character | Side: `home` or `visitor`; null in DEL and EIHL. |
| `jersey` | integer | Player jersey (sweater) number. |
| `matches` | integer | Games played (1 in a per-game file). |
| `won_matches` | integer | Games the goalie won. |
| `lost_matches` | integer | Games the goalie lost. |
| `saves` | integer | Shots on goal the goalie stopped. |
| `goals_against` | integer | Goals the goalie allowed. |
| `shots_against` | integer | Shots on goal faced. |
| `saves_percent` | numeric | Save percentage x 100 (e.g. `85.71`). |
| `average` | numeric | Goals-against average per 60 minutes. |
| `zeros` | integer | Shutouts by the goalie (`zeros`: games with no goal allowed). |
| `time_on_ice` | integer | Time in goal, seconds. |
| `seconds_per_game` | numeric | Time in goal per game, seconds. |
| `minutes_in_play` | numeric | Share of the team's game time in goal, percent (100.0 = the whole game). |
| `visible_games` | logical | Widget flag: show the milestone for that stat. |
| `visible_shutouts` | logical | Widget flag: show the milestone for that stat. |
| `visible_wins` | logical | Widget flag: show the milestone for that stat. |
| `stars_games` | integer | Widget star marker for that stat (unverified). |
| `stars_shutouts` | integer | Widget star marker for that stat (unverified). |
| `stars_wins` | integer | Widget star marker for that stat (unverified). |
| `milestone_games` | numeric | Widget value for the next milestone of that stat (unverified). |
| `milestone_shutouts` | numeric | Widget value for the next milestone of that stat (unverified). |
| `milestone_wins` | numeric | Widget value for the next milestone of that stat (unverified). |
| `xg` | numeric | Expected goals of the goalie's own team (ICEHL only). |
| `xga` | numeric | Expected goals against (ICEHL only; null elsewhere). |
| `gsax` | numeric | Goals saved above expected, xGA minus goals against (ICEHL only). |
| `xs` | numeric | Expected saves (ICEHL only; unverified). |
| `gate_zone_ur_goals_against` | integer | Goals allowed in that net zone. |
| `gate_zone_ur_shots_against` | integer | Shots on goal faced in that net zone. |
| `gate_zone_ur_saves` | integer | Saves in that net zone. |
| `gate_zone_ur_saves_percent` | numeric | Save percentage x 100 in that net zone. |
| `gate_zone_ul_goals_against` | integer | Goals allowed in that net zone. |
| `gate_zone_ul_shots_against` | integer | Shots on goal faced in that net zone. |
| `gate_zone_ul_saves` | integer | Saves in that net zone. |
| `gate_zone_ul_saves_percent` | numeric | Save percentage x 100 in that net zone. |
| `gate_zone_br_goals_against` | integer | Goals allowed in that net zone. |
| `gate_zone_br_shots_against` | integer | Shots on goal faced in that net zone. |
| `gate_zone_br_saves` | integer | Saves in that net zone. |
| `gate_zone_br_saves_percent` | numeric | Save percentage x 100 in that net zone. |
| `gate_zone_fh_goals_against` | integer | Goals allowed in that net zone. |
| `gate_zone_fh_shots_against` | integer | Shots on goal faced in that net zone. |
| `gate_zone_fh_saves` | integer | Saves in that net zone. |
| `gate_zone_fh_saves_percent` | numeric | Save percentage x 100 in that net zone. |
| `gate_zone_bl_goals_against` | integer | Goals allowed in that net zone. |
| `gate_zone_bl_shots_against` | integer | Shots on goal faced in that net zone. |
| `gate_zone_bl_saves` | integer | Saves in that net zone. |
| `gate_zone_bl_saves_percent` | numeric | Save percentage x 100 in that net zone. |
| `gate_zone_ce_goals_against` | integer | Goals allowed in that net zone. |
| `gate_zone_ce_shots_against` | integer | Shots on goal faced in that net zone. |
| `gate_zone_ce_saves` | integer | Saves in that net zone. |
| `gate_zone_ce_saves_percent` | numeric | Save percentage x 100 in that net zone. |
| `rank` | integer | Rank among the game's goalies; null in DEL and EIHL. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#metal_goalie_box-example}

```python
metal_goalie_box(match_id=575)
```

_Last validated n/a._

## metal_standings

Standings of one competition season, one row per team.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/table/{season}/{league_id}.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/table/2026/1.json](https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/table/2026/1.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season by its END year (2027 = 2026/27). The bucket keys seasons by their START year; the wrapper requests season - 1. |
| `league_id` | `league_id` |  |  | `Y` | Competition id. 1 = Metal Ligaen, regular season and playoffs in one file (split them by league_part_name); 2 = pre-season, 4 = Final-4 cup. |

### Returns {#metal_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group_id` | integer | Group id (group key space); null when the stage has no groups, so unobserved here. |
| `group_name` | character | Group name; empty string when the stage has no groups. |
| `complete` | logical | True when the standings include every finished game. |
| `team_id` | integer | Team id (per-bucket team key space); joins to home_id / guest_id in the schedule. |
| `part_team_id` | integer | Team-in-league-part id (its own key space: one team in one league part); no other captured file carries it. |
| `name` | character | Team display name as the feed prints it. |
| `team_original` | character | Team's registered name. |
| `shortcut` | character | Three-letter team code. |
| `position` | integer | Table position, 1 = first. |
| `points` | integer | Points in the standings table. |
| `games` | integer | Games played in the table's scope. |
| `wins` | integer | Wins in regulation (DEL, EIHL, Denmark) or all wins (ICEHL, see `winsReg`); definition per bucket unverified. |
| `wins_ot` | integer | Wins after overtime or shootout. |
| `wins_reg` | integer | Wins in regulation time. |
| `ties` | integer | Ties (0 under current rules). |
| `losts_ot` | integer | Losses after overtime or shootout. |
| `losts` | integer | Losses in regulation time. |
| `score1` | integer | Goals the team scored (goals for). |
| `score2` | integer | Goals conceded (goals against). |
| `goals_away` | integer | Goals scored in away games. |
| `points_away` | integer | Table points from away games. |
| `last10` | character | Form over the last ten games as a result string; empty string in the samples. |
| `only_ot_wins` | integer | Wins in overtime. |
| `only_so_wins` | integer | Wins in a shootout. |
| `only_ot_losts` | integer | Losses in overtime. |
| `only_so_losts` | integer | Losses in a shootout. |
| `qualified` | logical | True when the team has clinched qualification. |
| `live` | logical | True while the team is playing a live game. |
| `live_game_score` | character | Score of the live game; null when none, so unobserved here. |
| `position_change` | integer | Change in table position since the previous round. |
| `ppg` | numeric | Points per game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#metal_standings-example}

```python
metal_standings(league_id=1, season=2027)
```

_Last validated n/a._

## metal_game_center

Game-center bundle of one match as one row: game data, team stats, goal and penalty lists.

**Endpoint URL:** `GET https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/widget/esports/match/{match_id}.json`

**Valid URL:** [https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/widget/esports/match/575.json](https://s3.eu-west-1.amazonaws.com/den.hokejovyzapis.cz/widget/esports/match/575.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (per-bucket match key space: match_id in metal_schedule). Not the league website's game id. |

### Returns {#metal_game_center-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `status_id` | integer | Widget API status code, a code and not a joinable id: `1` = OK. |
| `status_msg` | character | Widget API status text, `Ok`. |
| `data_game_data_id` | integer | Match id (match key space, per bucket); joins to `league-matches` `matches[].id` and the `{match_id}` path param. |
| `data_game_data_division_id` | integer | League part id under the widget's name `divisionId` (league-part key space, per bucket: one stage of a competition, e.g. ICEHL 56 = 2026/27 regular season); joins to `league_part_id` in league-matches and the `{league_part_id}` path param. |
| `data_game_data_division_longname` | character | League part name, e.g. `ICEHL 2026/27`. |
| `data_game_data_division_shortname` | character | League part name, e.g. `ICEHL 2026/27`. |
| `data_game_data_scheduled_date_formatted_short` | character | Short display text `DD.MM.YY` (plus `HH:MM` for times). |
| `data_game_data_scheduled_date_formatted_long` | character | Long display text, e.g. `18.September. 2026`. |
| `data_game_data_scheduled_date_timestamp` | integer | Epoch milliseconds. |
| `data_game_data_scheduled_date_value` | character | ISO text `YYYY-MM-DD` (plus ` HH:MM` for times). |
| `data_game_data_scheduled_date_diff_to_now` | integer | Difference to now as computed by the widget server; `0` in the samples. |
| `data_game_data_scheduled_time` | character | Local time `HH:MM` (scheduled face-off, actual start, actual end). |
| `data_game_data_start_time` | character | Local time `HH:MM` (scheduled face-off, actual start, actual end). |
| `data_game_data_end_time` | character | Local time `HH:MM` (scheduled face-off, actual start, actual end). |
| `data_game_data_is_overtime` | integer | Overtime flag sent as an integer, `1` = yes. |
| `data_game_data_is_shoot_out` | integer | Shootout flag sent as an integer, `1` = yes. |
| `data_game_data_home_team_id` | integer | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `data_game_data_home_team_longname` | character | Team name: long, short (three-letter code) or display ('flavour') form. |
| `data_game_data_home_team_shortname` | character | Team name: long, short (three-letter code) or display ('flavour') form. |
| `data_game_data_home_team_flavourname` | character | Team name: long, short (three-letter code) or display ('flavour') form. |
| `data_game_data_home_team_score` | integer | Final goals of the home or away team (by name). |
| `data_game_data_away_team_id` | integer | Team id (team key space, per bucket, stable across seasons); joins to `league-matches` `home.id` / `guest.id` and the `{team_id}` path param. |
| `data_game_data_away_team_longname` | character | Team name: long, short (three-letter code) or display ('flavour') form. |
| `data_game_data_away_team_shortname` | character | Team name: long, short (three-letter code) or display ('flavour') form. |
| `data_game_data_away_team_flavourname` | character | Team name: long, short (three-letter code) or display ('flavour') form. |
| `data_game_data_away_team_score` | integer | Final goals of the home or away team (by name). |
| `data_game_data_game_status` | integer | Game status code; `4` = finished in the samples (other codes unverified). |
| `data_game_data_game_extended_status` | integer | Extended status code; `0` in the samples. |
| `data_game_data_game_extended_status_info_text` | character | Extended status text; empty string in the samples. |
| `data_game_data_game_name` | character | Game number sent as a string (e.g. `"66"`). |
| `data_game_data_game_day` | character | Matchday number sent as a string (`"1"`); null in Denmark. |
| `data_game_data_game_round` | integer | Round number; `0` in the samples. |
| `data_game_data_location_longname` | character | Arena name; empty string when not set. |
| `data_game_data_location_shortname` | character | Arena name; empty string when not set. |
| `data_game_data_location_coordinates_lat` | integer | Latitude / longitude; `0` placeholder in the samples. |
| `data_game_data_location_coordinates_lng` | integer | Latitude / longitude; `0` placeholder in the samples. |
| `data_game_data_attendance` | integer | Spectators at the game (attendance). |
| `data_game_data_live_time` | integer | Game clock, seconds of game time from the opening face-off (0-3600 in regulation, continuing through overtime). 3600 at the end of regulation. |
| `data_game_data_live_time_formatted` | character | Clock text; the placeholder `END-GAME` after the game, `m:ss` while live. |
| `data_game_data_live_time_period` | integer | Current period number; `4` after the game. |
| `data_game_data_live_time_string` | character | Clock text; the placeholder `END-GAME` after the game, `m:ss` while live. |
| `data_game_data_live_time_game_phase` | character | Game phase text, e.g. `END OF GAME`. |
| `data_game_data_period_stats` | character | Per-period score and shots. |
| `data_game_data_game_officials_ref1_lastname` | character | Last name of that official or coach. |
| `data_game_data_game_officials_ref1_firstname` | character | First name of that official or coach. |
| `data_game_data_game_officials_ref2_lastname` | character | Last name of that official or coach. |
| `data_game_data_game_officials_ref2_firstname` | character | First name of that official or coach. |
| `data_game_data_game_officials_linesman1_lastname` | character | Last name of that official or coach. |
| `data_game_data_game_officials_linesman1_firstname` | character | First name of that official or coach. |
| `data_game_data_game_officials_linesman2_lastname` | character | Last name of that official or coach. |
| `data_game_data_game_officials_linesman2_firstname` | character | First name of that official or coach. |
| `data_game_data_team_officials_headcoachhome_lastname` | character | Last name of that official or coach. |
| `data_game_data_team_officials_headcoachhome_firstname` | character | First name of that official or coach. |
| `data_game_data_team_officials_headcoachaway_lastname` | character | Last name of that official or coach. |
| `data_game_data_team_officials_headcoachaway_firstname` | character | First name of that official or coach. |
| `data_game_data_live_time_period_bar` | character | Period progress bar; null in the samples, so unobserved. |
| `data_game_data_you_tube_link` | character | Video link; null in the samples, so unobserved. |
| `data_game_data_number_of_periods` | integer | Regulation periods (3) or the maximum overtimes allowed. |
| `data_game_data_length_of_period` | integer | Length of a period or overtime, seconds (1200). |
| `data_game_data_number_of_overtimes` | integer | Regulation periods (3) or the maximum overtimes allowed. |
| `data_game_data_length_of_overtime` | integer | Length of a period or overtime, seconds (1200). |
| `data_game_data_shootout_shots` | integer | Shootout attempts in the game. |
| `data_game_data_images_home_team_logo` | character | Team logo; null in the samples, so unobserved. |
| `data_game_data_images_away_team_logo` | character | Team logo; null in the samples, so unobserved. |
| `data_game_data_streams` | character | Video streams; empty array in the samples. |
| `data_game_data_home_team_logo_url` | character | Team logo URL; null in the samples, so unobserved. |
| `data_game_data_away_team_logo_url` | character | Team logo URL; null in the samples, so unobserved. |
| `data_home_goals` | character | Goals scored by that team. |
| `data_home_penalties` | character | Penalties of that team. |
| `data_home_field_players` | character | Skaters of that team with their game stats. |
| `data_home_goal_keepers` | character | Goalies of that team with their game stats. |
| `data_home_team_stats_faceoffs_won` | integer | Face-offs the team won in this game. |
| `data_home_team_stats_faceoffs_total` | integer | Face-offs the team took in this game. |
| `data_home_team_stats_shots_on_goal` | integer | Team shots that reached the goalie in this game. |
| `data_home_team_stats_shots_blocked` | integer | Team blocked-shot count in this game (direction unverified). |
| `data_home_team_stats_penalty_minutes` | integer | Penalty minutes assessed to the team in this game. |
| `data_home_team_stats_powerplay_goals` | integer | Goals the team scored on the power play in this game. |
| `data_home_team_stats_powerplay_opportunities` | integer | Power plays the team had in this game. |
| `data_home_team_stats_penaltykill_successes` | integer | Penalties killed without conceding. |
| `data_home_team_stats_penaltykill_situations` | integer | Times the team had to kill a penalty in this game. |
| `data_home_team_stats_time_values_leading` | integer | Seconds spent leading, tied or trailing (by name). |
| `data_home_team_stats_time_values_tied` | integer | Seconds spent leading, tied or trailing (by name). |
| `data_home_team_stats_time_values_trailing` | integer | Seconds spent leading, tied or trailing (by name). |
| `data_home_team_stats_time_values_time_game_strength_even` | integer | Seconds at that skater difference (even, +1, +2, -1, -2). |
| `data_home_team_stats_time_values_time_game_strength_plus_one` | integer | Seconds at that skater difference (even, +1, +2, -1, -2). |
| `data_home_team_stats_time_values_time_game_strength_plus_two` | integer | Seconds at that skater difference (even, +1, +2, -1, -2). |
| `data_home_team_stats_time_values_time_game_strength_minus_one` | integer | Seconds at that skater difference (even, +1, +2, -1, -2). |
| `data_home_team_stats_time_values_time_game_strength_minus_two` | integer | Seconds at that skater difference (even, +1, +2, -1, -2). |
| `data_home_team_stats_time_values_time_on_powerplay` | integer | Seconds on the power play. |
| `data_home_team_stats_time_values_time_while_shorthanded` | integer | Seconds the team played shorthanded in this game. |
| `data_away_goals` | character | Goals scored by that team. |
| `data_away_penalties` | character | Penalties of that team. |
| `data_away_field_players` | character | Skaters of that team with their game stats. |
| `data_away_goal_keepers` | character | Goalies of that team with their game stats. |
| `data_away_team_stats_faceoffs_won` | integer | Face-offs the team won in this game. |
| `data_away_team_stats_faceoffs_total` | integer | Face-offs the team took in this game. |
| `data_away_team_stats_shots_on_goal` | integer | Team shots that reached the goalie in this game. |
| `data_away_team_stats_shots_blocked` | integer | Team blocked-shot count in this game (direction unverified). |
| `data_away_team_stats_penalty_minutes` | integer | Penalty minutes assessed to the team in this game. |
| `data_away_team_stats_powerplay_goals` | integer | Goals the team scored on the power play in this game. |
| `data_away_team_stats_powerplay_opportunities` | integer | Power plays the team had in this game. |
| `data_away_team_stats_penaltykill_successes` | integer | Penalties killed without conceding. |
| `data_away_team_stats_penaltykill_situations` | integer | Times the team had to kill a penalty in this game. |
| `data_away_team_stats_time_values_leading` | integer | Seconds spent leading, tied or trailing (by name). |
| `data_away_team_stats_time_values_tied` | integer | Seconds spent leading, tied or trailing (by name). |
| `data_away_team_stats_time_values_trailing` | integer | Seconds spent leading, tied or trailing (by name). |
| `data_away_team_stats_time_values_time_game_strength_even` | integer | Seconds at that skater difference (even, +1, +2, -1, -2). |
| `data_away_team_stats_time_values_time_game_strength_plus_one` | integer | Seconds at that skater difference (even, +1, +2, -1, -2). |
| `data_away_team_stats_time_values_time_game_strength_plus_two` | integer | Seconds at that skater difference (even, +1, +2, -1, -2). |
| `data_away_team_stats_time_values_time_game_strength_minus_one` | integer | Seconds at that skater difference (even, +1, +2, -1, -2). |
| `data_away_team_stats_time_values_time_game_strength_minus_two` | integer | Seconds at that skater difference (even, +1, +2, -1, -2). |
| `data_away_team_stats_time_values_time_on_powerplay` | integer | Seconds on the power play. |
| `data_away_team_stats_time_values_time_while_shorthanded` | integer | Seconds the team played shorthanded in this game. |
| `data_home_goal_keeper_changes` | character | Goalie on/off events of that team. |
| `data_away_goal_keeper_changes` | character | Goalie on/off events of that team. |
| `data_game_actions` | character | Empty array in both samples; item shape unobserved. |
| `data_faceoffs_head_to_head` | character | Empty array in both samples; item shape unobserved. |
| `data_faceoffs_individual` | character | Empty array in both samples; item shape unobserved. |
| `data_game_situations` | character | Empty array in both samples; item shape unobserved. |
| `calc_time` | integer | Server computation time; `0` in the samples. |
| `last_update_formatted_short` | character | Short display text `DD.MM.YY` (plus `HH:MM` for times). |
| `last_update_formatted_long` | character | Long display text, e.g. `18.September. 2026`. |
| `last_update_timestamp` | integer | Epoch milliseconds. |
| `last_update_value` | character | ISO text `YYYY-MM-DD` (plus ` HH:MM` for times). |
| `served_by_server` | character | Host that built the bundle, e.g. `icehl.hokejovyzapis.cz`. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#metal_game_center-example}

```python
metal_game_center(match_id=575)
```

_Last validated n/a._
