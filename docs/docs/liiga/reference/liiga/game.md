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
| `side` | character | home or away. |
| `team_id` | integer | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | character | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `log_time` | character | Wall-clock time the event was logged, ISO 8601 UTC. |
| `winning_goal` | logical | Game-winning goal. |
| `game_time` | integer | Seconds from the start of the game. Regular season: 3900 (65:00) = the end of the 5-minute overtime; playoff overtime runs past it. |
| `period` | integer | Period number. |
| `event_id` | integer | Event id within the game. |
| `home_team_score` | integer | Home score after the event. |
| `away_team_score` | integer | Away score after the event. |
| `goals_so_far_in_season` | integer | Scorer's season goal count after this goal. |
| `video_clip_url` | character | Video clip url. |
| `video_clip_playlist_url` | character | Video clip playlist url. |
| `video_thumbnail_url` | character | Video thumbnail url. |
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
| `side` | character | home or away. |
| `team_id` | integer | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | character | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `player_id` | integer | Player id; the same number in lineups, rosters, stat tables and the profile (fihaId). |
| `sufferer_player_id` | integer | Player who served the penalty. |
| `event_id` | integer | Event id within the game. |
| `log_time` | character | Wall-clock time the event was logged, ISO 8601 UTC. |
| `game_time` | integer | Seconds from the start of the game. Regular season: 3900 (65:00) = the end of the 5-minute overtime; playoff overtime runs past it. |
| `period` | integer | Period number. |
| `penalty_begintime` | integer | Penalty start, seconds from game start. |
| `penalty_endtime` | integer | Penalty end, seconds from game start. |
| `penalty_fault_name` | character | Infraction, in Finnish. |
| `penalty_fault_type` | character | Infraction code. |
| `penalty_info` | character | Penalty info. |
| `penalty_minutes` | integer | Penalty minutes. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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
| `shooting_team_id` | integer | Shooting team id. |
| `shooting_team_slug` | character | Shooting team slug. |
| `blocking_team_id` | integer | Goalkeeper's team id. |
| `blocking_team_slug` | character | Goalkeeper's team slug. |
| `goal_scored` | logical | The attempt scored. |
| `winning_goal` | logical | Game-winning goal. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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
| `side` | character | home or away. |
| `player_id` | integer | Player id; the same number in lineups, rosters, stat tables and the profile (fihaId). |
| `team_id` | integer | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | character | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `team_name` | character | Team display name. |
| `line` | integer | Line number 1-4; null for goalkeepers and extras. |
| `country_of_birth` | character | Country of birth, 3-letter code. |
| `place_of_birth` | character | Place of birth. |
| `date_of_birth` | character | Date of birth, YYYY-MM-DD. |
| `nationality` | character | Nationality, 3-letter code. |
| `first_name` | character | First name. |
| `last_name` | character | Last name. |
| `role` | character | Position: CENTER, LEFT_WING, RIGHT_WING, LEFT_DEFENSEMAN, RIGHT_DEFENSEMAN, SEVENTH_DEFENSEMAN, GOALIE. |
| `role_code` | character | Position code, Finnish: KH = centre, VL/OL = left/right wing, VP/OP = left/right defence, 7. P = seventh defenceman, MV = goalkeeper. |
| `handedness` | character | Shoots LEFT or RIGHT. |
| `height` | integer | Height, cm. |
| `weight` | integer | Weight, kg. |
| `captain` | logical | Captain. |
| `rookie` | logical | Rookie. |
| `alternate_captain` | logical | Alternate captain. |
| `jersey` | integer | Jersey number. |
| `picture_url` | character | Photo URL. |
| `injured` | logical | Injured. |
| `suspended` | logical | Suspended. |
| `removed` | logical | Removed from the roster. |
| `awards` | character | Player awards of the game (stars). |
| `sponsors` | character | Sponsors. |
| `extra_description` | character | Extra description. |
| `extra_strengths` | character | Extra strengths. |
| `extra_rookie_picture` | logical | Extra rookie picture. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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
| `side` | character | home or away. |
| `player_id` | integer | Player id; the same number in lineups, rosters, stat tables and the profile (fihaId). |
| `team_id` | integer | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | character | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `team_name` | character | Team display name. |
| `line` | integer | Line number 1-4; null for goalkeepers and extras. |
| `country_of_birth` | character | Country of birth, 3-letter code. |
| `place_of_birth` | character | Place of birth. |
| `date_of_birth` | character | Date of birth, YYYY-MM-DD. |
| `nationality` | character | Nationality, 3-letter code. |
| `first_name` | character | First name. |
| `last_name` | character | Last name. |
| `role` | character | Position: CENTER, LEFT_WING, RIGHT_WING, LEFT_DEFENSEMAN, RIGHT_DEFENSEMAN, SEVENTH_DEFENSEMAN, GOALIE. |
| `role_code` | character | Position code, Finnish: KH = centre, VL/OL = left/right wing, VP/OP = left/right defence, 7. P = seventh defenceman, MV = goalkeeper. |
| `handedness` | character | Shoots LEFT or RIGHT. |
| `height` | integer | Height, cm. |
| `weight` | integer | Weight, kg. |
| `captain` | logical | Captain. |
| `rookie` | logical | Rookie. |
| `alternate_captain` | logical | Alternate captain. |
| `jersey` | integer | Jersey number. |
| `picture_url` | character | Photo URL. |
| `injured` | logical | Injured. |
| `suspended` | logical | Suspended. |
| `removed` | logical | Removed from the roster. |
| `awards` | character | Player awards of the game (stars). |
| `sponsors` | character | Sponsors. |
| `extra_description` | character | Extra description. |
| `extra_strengths` | character | Extra strengths. |
| `extra_rookie_picture` | logical | Extra rookie picture. |
| `all_time_stats_games` | integer | All-time stats: games (player, all-time). |
| `all_time_stats_goals` | integer | All-time stats: goals. |
| `all_time_stats_assists` | integer | All-time stats: assists (player, all-time). |
| `all_time_stats_points` | integer | All-time stats: points (player, all-time). |
| `all_time_stats_plus` | integer | All-time stats: on ice for goals for (the + of plus/minus). |
| `all_time_stats_minus` | integer | All-time stats: on ice for goals against (the - of plus/minus). |
| `all_time_stats_plus_minus` | integer | All-time stats: plus minus minus. |
| `all_time_stats_shots` | integer | All-time stats: shots (player, all-time). |
| `all_time_stats_shot_percentage` | numeric | All-time stats: shot percentage (player, all-time). Percent, 0-100. |
| `all_time_stats_penalty_minutes` | integer | All-time stats: penalty minutes. |
| `season_stats_games` | integer | Season stats: games (player, all-time). |
| `season_stats_goals` | integer | Season stats: goals. |
| `season_stats_assists` | integer | Season stats: assists (player, all-time). |
| `season_stats_points` | integer | Season stats: points (player, all-time). |
| `season_stats_plus` | integer | Season stats: on ice for goals for (the + of plus/minus). |
| `season_stats_minus` | integer | Season stats: on ice for goals against (the - of plus/minus). |
| `season_stats_plus_minus` | integer | Season stats: plus minus minus. |
| `season_stats_shots` | integer | Season stats: shots (player, all-time). |
| `season_stats_shot_percentage` | numeric | Season stats: shot percentage (player, all-time). Percent, 0-100. |
| `season_stats_penalty_minutes` | integer | Season stats: penalty minutes. |
| `season_stats_in_team_games` | integer | Season stats with this team: games (player, all-time). |
| `season_stats_in_team_goals` | integer | Season stats with this team: goals. |
| `season_stats_in_team_assists` | integer | Season stats with this team: assists (player, all-time). |
| `season_stats_in_team_points` | integer | Season stats with this team: points (player, all-time). |
| `season_stats_in_team_plus` | integer | Season stats with this team: on ice for goals for (the + of plus/minus). |
| `season_stats_in_team_minus` | integer | Season stats with this team: on ice for goals against (the - of plus/minus). |
| `season_stats_in_team_plus_minus` | integer | Season stats with this team: plus minus minus. |
| `season_stats_in_team_shots` | integer | Season stats with this team: shots (player, all-time). |
| `season_stats_in_team_shot_percentage` | numeric | Season stats with this team: shot percentage (player, all-time). Percent, 0-100. |
| `season_stats_in_team_penalty_minutes` | integer | Season stats with this team: penalty minutes. |
| `vs_team_stats_games` | integer | Stats versus this opponent: games (player, all-time). |
| `vs_team_stats_goals` | integer | Stats versus this opponent: goals. |
| `vs_team_stats_assists` | integer | Stats versus this opponent: assists (player, all-time). |
| `vs_team_stats_points` | integer | Stats versus this opponent: points (player, all-time). |
| `vs_team_stats_plus` | integer | Stats versus this opponent: on ice for goals for (the + of plus/minus). |
| `vs_team_stats_minus` | integer | Stats versus this opponent: on ice for goals against (the - of plus/minus). |
| `vs_team_stats_plus_minus` | integer | Stats versus this opponent: plus minus minus. |
| `vs_team_stats_shots` | integer | Stats versus this opponent: shots (player, all-time). |
| `vs_team_stats_shot_percentage` | numeric | Stats versus this opponent: shot percentage (player, all-time). Percent, 0-100. |
| `vs_team_stats_penalty_minutes` | integer | Stats versus this opponent: penalty minutes. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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
| `side` | character | home or away. |
| `team_id` | integer | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | character | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `period` | integer | Period number. |
| `player_id` | integer | Player id; the same number in lineups, rosters, stat tables and the profile (fihaId). |
| `jersey` | integer | Jersey number. |
| `is_goalie` | logical | Row is a goalkeeper's (goalie columns filled). |
| `points` | integer | Points (player, all-time). |
| `assists` | integer | Assists (player, all-time). |
| `goals` | integer | Goals. |
| `plusminus` | integer | plus minus minus, this period. |
| `plus` | integer | On ice for goals for (the + of plus/minus). |
| `minus` | integer | On ice for goals against (the - of plus/minus). |
| `shots` | integer | Shots (player, all-time). |
| `penaltyminutes` | integer | Penaltyminutes (goalkeeper, this period). |
| `timeofice` | integer | Timeofice (goalkeeper, this period). Seconds. |
| `powerplay_goals` | integer | Powerplay goals. |
| `short_handed_goals` | integer | Short handed goals. |
| `winning_goal` | logical | Game-winning goal. |
| `blocked_shots` | integer | Blocked shots (goalkeeper, this period). |
| `faceoffs_total` | integer | Faceoffs total. |
| `faceoffs_won` | integer | Faceoffs won. |
| `corsi_for` | integer | Corsi (shot attempts) for (goalkeeper, this period). |
| `corsi_against` | integer | Corsi (shot attempts) against (goalkeeper, this period). |
| `faceoffs_center_total` | integer | Faceoffs center total. |
| `faceoffs_center_won` | integer | Faceoffs center won. |
| `faceoffs_defence_total` | integer | Faceoffs defence total. |
| `faceoffs_defence_won` | integer | Faceoffs defence won. |
| `faceoffs_offence_total` | integer | Faceoffs offence total. |
| `faceoffs_offence_won` | integer | Faceoffs offence won. |
| `fs_zone_starts_oz` | integer | Full-strength (fs, unverified) zone starts offensive zone (goalkeeper, this period). |
| `fs_zone_starts_dz` | integer | Full-strength (fs, unverified) zone starts defensive zone (goalkeeper, this period). |
| `powerplay2_goals` | integer | Powerplay 2 goals (goalkeeper, this period). |
| `penaltykill2_goals` | integer | Penaltykill 2 goals (goalkeeper, this period). |
| `powerplay_assists` | integer | Powerplay assists (goalkeeper, this period). |
| `penaltykill_assists` | integer | Penaltykill assists (goalkeeper, this period). |
| `goals_to_empty_goal` | integer | Goals to empty goal (goalkeeper, this period). |
| `fs_team_shots` | integer | Full-strength (fs, unverified) team shots (goalkeeper, this period). |
| `fs_team_goals` | integer | Full-strength (fs, unverified) team goals (goalkeeper, this period). |
| `fs_team_shots_against` | integer | Full-strength (fs, unverified) team shots against (goalkeeper, this period). |
| `fs_team_goals_against` | integer | Full-strength (fs, unverified) team goals against (goalkeeper, this period). |
| `distance` | numeric | Distance (goalkeeper, this period). Distance; unit not stated (unverified). |
| `total_passes` | integer | Total passes. |
| `successful_passes` | integer | Successful passes. |
| `player_passes_under_pressure` | integer | Player passes under pressure. |
| `player_successful_passes_under_pressure` | integer | Player successful passes under pressure. |
| `player_passes_under_high_pressure` | integer | Player passes under high pressure. |
| `player_successful_passes_under_high_pressure` | integer | Player successful passes under high pressure. |
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
| `side` | character | home or away. |
| `period` | integer | Period number. |
| `team_id` | integer | Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer or a digit string depending on the route). |
| `team_slug` | character | Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk). |
| `goals` | integer | Goals. |
| `shots` | integer | Shots (player, all-time). |
| `power_play_instances` | character | Power play instances. |
| `power_play_goals` | integer | Power play goals (team, this period). |
| `short_handed_instances` | character | Short handed instances. |
| `short_handed_goals_against` | integer | Short handed goals against (team, this period). |
| `penalty_minutes` | integer | Penalty minutes. |
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

### Example {#liiga_game_team_stats-example}

```python
liiga_game_team_stats(game_id=1, season=2026)
```

_Last validated n/a._
