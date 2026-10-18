---
title: "NL — National League app API (www.nationalleague.ch/api, Switzerland) — Other"
sidebar_label: "Other"
sidebar_position: 3
description: "NL — National League app API (www.nationalleague.ch/api, Switzerland) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NL — National League app API (www.nationalleague.ch/api, Switzerland) — Other

## nl_schedule

Schedule and results for one season: NL plus exhibitions.

**Endpoint URL:** `GET https://www.nationalleague.ch/api/games`

**Valid URL:** [https://www.nationalleague.ch/api/games?season=2026](https://www.nationalleague.ch/api/games?season=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season END year: `2026` = 2025/26. Diffing 2026 vs 2027 changes every list, and /player/{playerId} gp (51 vs 7), so it scopes. Lists are served for 2019-2027 (the app's season picker, /api/content/metadata `seasons`) and patchily before: 2012 and 2017 answered, 2010/2014/2015/2016/2018 answered `[]` (probed 2026-10-12). |

### Returns {#nl_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | SIHF game id `<season(4)><competition(4)><sequence(6)>` (Int64; e.g. 20261105000325, 1105 = National League). The same id as the sihf family's game_id; its first four digits are the season END year. |
| `home_team_short_name` | character | Home team abbreviation (e.g. HCD) |
| `home_team_name` | character | Home team display name |
| `away_team_short_name` | character | Away team abbreviation |
| `away_team_name` | character | Away team display name |
| `home_team_id` | integer | Home team. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `away_team_id` | integer | Away team. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `vimeo_video_id` | integer | Full-game video. Vimeo video id (Int64): vimeo.com/<id>. |
| `date` | character | Scheduled start, ISO 8601 with UTC offset |
| `status` | character | Game state: finished, canceled, beforeStartOfPlay (others appear live) |
| `base_status` | character | Coarse game state; equal to status in every sampled game |
| `home_team_result` | integer | Home team goals (final or current score; 0 before the game) |
| `away_team_result` | integer | Away team goals (final or current score; 0 before the game) |
| `is_overtime` | logical | Game went to overtime |
| `is_shootout` | logical | Game went to a shootout |
| `highlight_vimeo_video_id` | integer | Highlights video. Vimeo video id (Int64): vimeo.com/<id>. |
| `has_highlight` | logical | A highlight video exists |
| `is_current` | logical | Game belongs to the current game day |
| `is_current_for_team` | logical | Game is the team's current game (team-scoped views) |
| `tv_partner` | character | Broadcaster logo image URLs; unobserved: null, absent or empty in every sampled row |
| `spectators` | integer | Attendance (Int64; the API sends a digit string). |
| `has_my_sports` | logical | Broadcast on MySports |
| `referees` | character | Officials as '#<number> <name>' strings (referees and linesmen; one entry in some games) |
| `is_exhibition` | logical | Exhibition game, not NL competition (competition code other than 1105) |
| `home_team_coach` | character | Home head coach name; filled in a game's `overview`, null in the season lists; unobserved: null, absent or empty in every sampled row |
| `away_team_coach` | character | Away head coach name; filled in a game's `overview`, null in the season lists; unobserved: null, absent or empty in every sampled row |
| `game_time` | integer | Elapsed game minutes; 0 on every game in the season lists, 60 in a finished game's overview |
| `show_game_time` | logical | App flag: show gameTime |
| `arena` | character | Name of the arena the game is played in |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nl_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A 404, or an empty answer (no games or rows for that season, team or phase). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A failed fetch, an error object in a 200, a body without the ids it must carry, or an answer for another season, team, game or player. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nl_schedule-example}

```python
nl_schedule(season=2026)
```

_Last validated n/a._

## nl_current_games

The current or next game day.

**Endpoint URL:** `GET https://www.nationalleague.ch/api/games/current`

**Valid URL:** [https://www.nationalleague.ch/api/games/current?season=2027](https://www.nationalleague.ch/api/games/current?season=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | The running season's END year (2027 on 2026-10-12). Season END year: `2026` = 2025/26. Diffing 2026 vs 2027 changes every list, and /player/{playerId} gp (51 vs 7), so it scopes. Lists are served for 2019-2027 (the app's season picker, /api/content/metadata `seasons`) and patchily before: 2012 and 2017 answered, 2010/2014/2015/2016/2018 answered `[]` (probed 2026-10-12). |

### Returns {#nl_current_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | SIHF game id `<season(4)><competition(4)><sequence(6)>` (Int64; e.g. 20261105000325, 1105 = National League). The same id as the sihf family's game_id; its first four digits are the season END year. |
| `home_team_short_name` | character | Home team abbreviation (e.g. HCD) |
| `home_team_name` | character | Home team display name |
| `away_team_short_name` | character | Away team abbreviation |
| `away_team_name` | character | Away team display name |
| `home_team_id` | integer | Home team. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `away_team_id` | integer | Away team. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `vimeo_video_id` | integer | Full-game video. Vimeo video id (Int64): vimeo.com/<id>. |
| `date` | character | Scheduled start, ISO 8601 with UTC offset |
| `status` | character | Game state: finished, canceled, beforeStartOfPlay (others appear live) |
| `base_status` | character | Coarse game state; equal to status in every sampled game |
| `home_team_result` | integer | Home team goals (final or current score; 0 before the game) |
| `away_team_result` | integer | Away team goals (final or current score; 0 before the game) |
| `is_overtime` | logical | Game went to overtime |
| `is_shootout` | logical | Game went to a shootout |
| `highlight_vimeo_video_id` | integer | Highlights video. Vimeo video id (Int64): vimeo.com/<id>. |
| `has_highlight` | logical | A highlight video exists |
| `is_current` | logical | Game belongs to the current game day |
| `is_current_for_team` | logical | Game is the team's current game (team-scoped views) |
| `tv_partner` | character | Broadcaster logo image URLs |
| `spectators` | integer | Attendance (Int64; the API sends a digit string). |
| `has_my_sports` | logical | Broadcast on MySports |
| `referees` | character | Officials as '#<number> <name>' strings (referees and linesmen; one entry in some games); unobserved: null, absent or empty in every sampled row |
| `is_exhibition` | logical | Exhibition game, not NL competition (competition code other than 1105) |
| `home_team_coach` | character | Home head coach name; filled in a game's `overview`, null in the season lists; unobserved: null, absent or empty in every sampled row |
| `away_team_coach` | character | Away head coach name; filled in a game's `overview`, null in the season lists; unobserved: null, absent or empty in every sampled row |
| `game_time` | integer | Elapsed game minutes; 0 on every game in the season lists, 60 in a finished game's overview |
| `show_game_time` | logical | App flag: show gameTime |
| `arena` | character | Name of the arena the game is played in |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nl_current_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A 404, or an empty answer (no games or rows for that season, team or phase). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A failed fetch, an error object in a 200, a body without the ids it must carry, or an answer for another season, team, game or player. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nl_current_games-example}

```python
nl_current_games(season=2027)
```

_Last validated n/a._

## nl_playoffs

Playoff and play-in series by round: one row per game, with its series score.

**Endpoint URL:** `GET https://www.nationalleague.ch/api/games/playoffs`

**Valid URL:** [https://www.nationalleague.ch/api/games/playoffs?season=2026](https://www.nationalleague.ch/api/games/playoffs?season=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season END year: `2026` = 2025/26. Diffing 2026 vs 2027 changes every list, and /player/{playerId} gp (51 vs 7), so it scopes. Lists are served for 2019-2027 (the app's season picker, /api/content/metadata `seasons`) and patchily before: 2012 and 2017 answered, 2010/2014/2015/2016/2018 answered `[]` (probed 2026-10-12). |

### Returns {#nl_playoffs-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `series_group` | integer | Series number within the round (1 = first pairing; 0 seen in playouts) |
| `series_home_team_short_name` | character | Abbreviation of the team with home ice |
| `series_away_team_short_name` | character | Abbreviation of the other team |
| `series_home_team_id` | integer | Team with home ice in the series. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `series_away_team_id` | integer | The other team in the series. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `series_home_team_result` | integer | Series wins of the team with home ice |
| `series_away_team_result` | integer | Series wins of the other team |
| `series_best_of` | integer | Series length in games (7; 2 in the play-in) |
| `series_title` | character | Series length label, e.g. 'Best of 7' |
| `round` | character | Round key: final, semifinal, quarterfinal, playIn (/playoffs); leagueQualifications, playoutFinal (/playouts) |
| `game_id` | integer | SIHF game id `<season(4)><competition(4)><sequence(6)>` (Int64; e.g. 20261105000325, 1105 = National League). The same id as the sihf family's game_id; its first four digits are the season END year. |
| `home_team_short_name` | character | Home team abbreviation (e.g. HCD) |
| `home_team_name` | character | Home team display name |
| `away_team_short_name` | character | Away team abbreviation |
| `away_team_name` | character | Away team display name |
| `home_team_id` | integer | Home team. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `away_team_id` | integer | Away team. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `vimeo_video_id` | integer | Full-game video. Vimeo video id (Int64): vimeo.com/<id>. |
| `date` | character | Scheduled start, ISO 8601 with UTC offset |
| `status` | character | Game state: finished, canceled, beforeStartOfPlay (others appear live) |
| `base_status` | character | Coarse game state; equal to status in every sampled game |
| `home_team_result` | integer | Home team goals (final or current score; 0 before the game) |
| `away_team_result` | integer | Away team goals (final or current score; 0 before the game) |
| `is_overtime` | logical | Game went to overtime |
| `is_shootout` | logical | Game went to a shootout |
| `highlight_vimeo_video_id` | integer | Highlights video. Vimeo video id (Int64): vimeo.com/<id>. |
| `has_highlight` | logical | A highlight video exists |
| `is_current` | logical | Game belongs to the current game day |
| `is_current_for_team` | logical | Game is the team's current game (team-scoped views) |
| `tv_partner` | character | Broadcaster logo image URLs |
| `spectators` | integer | Attendance (Int64; the API sends a digit string). |
| `has_my_sports` | logical | Broadcast on MySports |
| `referees` | character | Officials as '#<number> <name>' strings (referees and linesmen; one entry in some games) |
| `is_exhibition` | logical | Exhibition game, not NL competition (competition code other than 1105) |
| `home_team_coach` | character | Home head coach name; filled in a game's `overview`, null in the season lists; unobserved: null, absent or empty in every sampled row |
| `away_team_coach` | character | Away head coach name; filled in a game's `overview`, null in the season lists; unobserved: null, absent or empty in every sampled row |
| `game_time` | integer | Elapsed game minutes; 0 on every game in the season lists, 60 in a finished game's overview |
| `show_game_time` | logical | App flag: show gameTime |
| `arena` | character | Name of the arena the game is played in |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nl_playoffs-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A 404, or an empty answer (no games or rows for that season, team or phase). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A failed fetch, an error object in a 200, a body without the ids it must carry, or an answer for another season, team, game or player. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nl_playoffs-example}

```python
nl_playoffs(season=2026)
```

_Last validated n/a._

## nl_playouts

Playout and league-qualification series by round: one row per game, with its series score.

**Endpoint URL:** `GET https://www.nationalleague.ch/api/games/playouts`

**Valid URL:** [https://www.nationalleague.ch/api/games/playouts?season=2026](https://www.nationalleague.ch/api/games/playouts?season=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season END year: `2026` = 2025/26. Diffing 2026 vs 2027 changes every list, and /player/{playerId} gp (51 vs 7), so it scopes. Lists are served for 2019-2027 (the app's season picker, /api/content/metadata `seasons`) and patchily before: 2012 and 2017 answered, 2010/2014/2015/2016/2018 answered `[]` (probed 2026-10-12). |

### Returns {#nl_playouts-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `series_group` | integer | Series number within the round (1 = first pairing; 0 seen in playouts) |
| `series_home_team_short_name` | character | Abbreviation of the team with home ice |
| `series_away_team_short_name` | character | Abbreviation of the other team |
| `series_home_team_id` | integer | Team with home ice in the series. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `series_away_team_id` | integer | The other team in the series. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `series_home_team_result` | integer | Series wins of the team with home ice |
| `series_away_team_result` | integer | Series wins of the other team |
| `series_best_of` | integer | Series length in games (7; 2 in the play-in) |
| `series_title` | character | Series length label, e.g. 'Best of 7' |
| `round` | character | Round key: final, semifinal, quarterfinal, playIn (/playoffs); leagueQualifications, playoutFinal (/playouts) |
| `game_id` | integer | SIHF game id `<season(4)><competition(4)><sequence(6)>` (Int64; e.g. 20261105000325, 1105 = National League). The same id as the sihf family's game_id; its first four digits are the season END year. |
| `home_team_short_name` | character | Home team abbreviation (e.g. HCD) |
| `home_team_name` | character | Home team display name |
| `away_team_short_name` | character | Away team abbreviation |
| `away_team_name` | character | Away team display name |
| `home_team_id` | integer | Home team. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `away_team_id` | integer | Away team. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `vimeo_video_id` | integer | Full-game video. Vimeo video id (Int64): vimeo.com/<id>. |
| `date` | character | Scheduled start, ISO 8601 with UTC offset |
| `status` | character | Game state: finished, canceled, beforeStartOfPlay (others appear live) |
| `base_status` | character | Coarse game state; equal to status in every sampled game |
| `home_team_result` | integer | Home team goals (final or current score; 0 before the game) |
| `away_team_result` | integer | Away team goals (final or current score; 0 before the game) |
| `is_overtime` | logical | Game went to overtime |
| `is_shootout` | logical | Game went to a shootout |
| `highlight_vimeo_video_id` | integer | Highlights video. Vimeo video id (Int64): vimeo.com/<id>. |
| `has_highlight` | logical | A highlight video exists |
| `is_current` | logical | Game belongs to the current game day |
| `is_current_for_team` | logical | Game is the team's current game (team-scoped views) |
| `tv_partner` | character | Broadcaster logo image URLs |
| `spectators` | integer | Attendance (Int64; the API sends a digit string). |
| `has_my_sports` | logical | Broadcast on MySports |
| `referees` | character | Officials as '#<number> <name>' strings (referees and linesmen; one entry in some games) |
| `is_exhibition` | logical | Exhibition game, not NL competition (competition code other than 1105) |
| `home_team_coach` | character | Home head coach name; filled in a game's `overview`, null in the season lists; unobserved: null, absent or empty in every sampled row |
| `away_team_coach` | character | Away head coach name; filled in a game's `overview`, null in the season lists; unobserved: null, absent or empty in every sampled row |
| `game_time` | integer | Elapsed game minutes; 0 on every game in the season lists, 60 in a finished game's overview |
| `show_game_time` | logical | App flag: show gameTime |
| `arena` | character | Name of the arena the game is played in |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nl_playouts-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A 404, or an empty answer (no games or rows for that season, team or phase). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A failed fetch, an error object in a 200, a body without the ids it must carry, or an answer for another season, team, game or player. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nl_playouts-example}

```python
nl_playouts(season=2026)
```

_Last validated n/a._

## nl_game_overview

One game's header: teams, date, status, final and per-period score, coaches, referees, arena, attendance.

**Endpoint URL:** `GET https://www.nationalleague.ch/api/games/{game_id}`

**Valid URL:** [https://www.nationalleague.ch/api/games/20261105000325](https://www.nationalleague.ch/api/games/20261105000325)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | SIHF game id, 14 digits `<season(4)><competition(4)><sequence(6)>` (game_id in nl_schedule; the same id as the sihf family's game_id), e.g. 20261105000325. |

### Returns {#nl_game_overview-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | SIHF game id `<season(4)><competition(4)><sequence(6)>` (Int64; e.g. 20261105000325, 1105 = National League). The same id as the sihf family's game_id; its first four digits are the season END year. |
| `home_team_short_name` | character | Home team abbreviation (e.g. HCD) |
| `home_team_name` | character | Home team display name |
| `away_team_short_name` | character | Away team abbreviation |
| `away_team_name` | character | Away team display name |
| `home_team_id` | integer | Home team. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `away_team_id` | integer | Away team. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `vimeo_video_id` | integer | Full-game video. Vimeo video id (Int64): vimeo.com/<id>. |
| `date` | character | Scheduled start, ISO 8601 with UTC offset |
| `status` | character | Game state: finished, canceled, beforeStartOfPlay (others appear live) |
| `base_status` | character | Coarse game state; equal to status in every sampled game |
| `home_team_result` | integer | Home team goals (final or current score; 0 before the game) |
| `away_team_result` | integer | Away team goals (final or current score; 0 before the game) |
| `is_overtime` | logical | Game went to overtime |
| `is_shootout` | logical | Game went to a shootout |
| `highlight_vimeo_video_id` | integer | Highlights video. Vimeo video id (Int64): vimeo.com/<id>. |
| `has_highlight` | logical | A highlight video exists |
| `is_current` | logical | Game belongs to the current game day |
| `is_current_for_team` | logical | Game is the team's current game (team-scoped views) |
| `tv_partner` | character | Broadcaster logo image URLs |
| `spectators` | integer | Attendance (Int64; the API sends a digit string). |
| `has_my_sports` | logical | Broadcast on MySports |
| `referees` | character | Officials as '#<number> <name>' strings (referees and linesmen; one entry in some games) |
| `is_exhibition` | logical | Exhibition game, not NL competition (competition code other than 1105) |
| `home_team_coach` | character | Home head coach name; filled in a game's `overview`, null in the season lists |
| `away_team_coach` | character | Away head coach name; filled in a game's `overview`, null in the season lists |
| `game_time` | integer | Elapsed game minutes; 0 on every game in the season lists, 60 in a finished game's overview |
| `show_game_time` | logical | App flag: show gameTime |
| `arena` | character | Name of the arena the game is played in |
| `home_team_first_result` | integer | Home goals in the 1st period |
| `away_team_first_result` | integer | Away goals in the 1st period |
| `home_team_second_result` | integer | Home goals in the 2nd period |
| `away_team_second_result` | integer | Away goals in the 2nd period |
| `home_team_third_result` | integer | Home goals in the 3rd period |
| `away_team_third_result` | integer | Away goals in the 3rd period |
| `home_team_overtime_result` | integer | Home overtime goals, all overtime periods |
| `away_team_overtime_result` | integer | Away overtime goals, all overtime periods |
| `home_team_overtime1_result` | integer | Home goals in overtime period 1 (playoff games can have several) |
| `away_team_overtime1_result` | integer | Away goals in overtime period 1 |
| `home_team_overtime2_result` | integer | Home goals in overtime period 2 |
| `away_team_overtime2_result` | integer | Away goals in overtime period 2 |
| `home_team_overtime3_result` | integer | Home goals in overtime period 3 |
| `away_team_overtime3_result` | integer | Away goals in overtime period 3 |
| `home_team_shootout_result` | integer | Home shootout goals |
| `away_team_shootout_result` | integer | Away shootout goals |
| `home_aggregated_score` | character | Home aggregate over a two-legged tie; unobserved: null or empty in every sampled row |
| `away_aggregated_score` | character | Away aggregate over a two-legged tie; unobserved: null or empty in every sampled row |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nl_game_overview-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A 404, or an empty answer (no games or rows for that season, team or phase). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A failed fetch, an error object in a 200, a body without the ids it must carry, or an answer for another season, team, game or player. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nl_game_overview-example}

```python
nl_game_overview(game_id=20261105000325)
```

_Last validated n/a._

## nl_pbp

One game's event timeline: goals with assists, penalties, goaltender changes, revoked goals.

**Endpoint URL:** `GET https://www.nationalleague.ch/api/games/{game_id}`

**Valid URL:** [https://www.nationalleague.ch/api/games/20261105000325](https://www.nationalleague.ch/api/games/20261105000325)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | SIHF game id, 14 digits `<season(4)><competition(4)><sequence(6)>` (game_id in nl_schedule; the same id as the sihf family's game_id), e.g. 20261105000325. |

### Returns {#nl_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | SIHF game id `<season(4)><competition(4)><sequence(6)>` (Int64; e.g. 20261105000325, 1105 = National League). The same id as the sihf family's game_id; its first four digits are the season END year. |
| `event_index` | integer | 1-based position of the event in the game's timeline. |
| `team_side` | character | home or away: the side of the team this row belongs to. |
| `game_seconds` | integer | Elapsed game time of the event in seconds, from the MMSS `time` (822 = 8:22 -> 502). |
| `team_id` | integer | SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `home_team` | logical | Event belongs to the home team |
| `time` | character | Game clock elapsed as an MMSS string without separator ('822' = 8:22, '5954' = 59:54) |
| `video_time` | integer | Seconds into the game video; 0 when there is no clip |
| `video_vimeo_id` | integer | Clip of this event. Vimeo video id (Int64): vimeo.com/<id>. |
| `action` | character | Event type: goal, foul (penalty), in / out (goaltender enters / leaves), revokedGoal |
| `foul_type` | integer | SIHF penalty code (116-125 in the sample); 0 on non-penalty events |
| `foul_minutes` | integer | Penalty minutes; 0 on non-penalty events |
| `player_first_name` | character | Shooter's first name, from the game's lineup. |
| `player_last_name` | character | Shooter's last name, from the game's lineup. |
| `player_number` | character | Shooter's sweater number, from the game's lineup (a string). |
| `player_total` | integer | Scorer's goals this season including this one; 0 on non-goal events |
| `period` | integer | Period number, an integer (the parent `third` is a string) |
| `overtime` | integer | Overtime period number; 0 in regulation |
| `goal_situation` | character | Goal kind; 'normal' on every sampled event |
| `home_team_result` | integer | Home score after the event |
| `away_team_result` | integer | Away score after the event |
| `assist1_first_name` | character | First assist: first name |
| `assist1_last_name` | character | First assist: last name |
| `assist1_number` | character | First assist: sweater number, a string |
| `assist1_total` | integer | First assister's assists this season including this one |
| `assist2_first_name` | character | Second assist: first name |
| `assist2_last_name` | character | Second assist: last name |
| `assist2_number` | character | Second assist: sweater number, a string |
| `assist2_total` | integer | Second assister's assists this season including this one |
| `situation` | character | Goal tags, comma-separated: EQ (even strength), PP1 (power play), GWG (game-winning goal); null on non-goals |
| `revoked_goal_type` | integer | Revoked-goal category code (1 on every sampled event) |
| `revoked_goal_sub_type` | integer | Revoked-goal reason code (1 on the revoked goal, 0 elsewhere) |
| `review_successful` | character | Video-review outcome; unobserved: null or empty in every sampled row |
| `review_goal_counts` | logical | Goal stood after review |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nl_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A 404, or an empty answer (no games or rows for that season, team or phase). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A failed fetch, an error object in a 200, a body without the ids it must carry, or an answer for another season, team, game or player. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nl_pbp-example}

```python
nl_pbp(game_id=20261105000325)
```

_Last validated n/a._

## nl_lineups

One game's lineups: every dressed player by unit, with licence number, position and line.

**Endpoint URL:** `GET https://www.nationalleague.ch/api/games/{game_id}`

**Valid URL:** [https://www.nationalleague.ch/api/games/20261105000325](https://www.nationalleague.ch/api/games/20261105000325)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | SIHF game id, 14 digits `<season(4)><competition(4)><sequence(6)>` (game_id in nl_schedule; the same id as the sihf family's game_id), e.g. 20261105000325. |

### Returns {#nl_lineups-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | SIHF game id `<season(4)><competition(4)><sequence(6)>` (Int64; e.g. 20261105000325, 1105 = National League). The same id as the sihf family's game_id; its first four digits are the season END year. |
| `team_side` | character | home or away: the side of the team this row belongs to. |
| `team_id` | integer | SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `unit` | character | Lineup group: goalkeeper, or the line number 1-4 as a string. |
| `player_id` | integer | SIHF licence number (Int64; e.g. 305519 = Tobias Geisser). Joins the sihf family's player_id and every SIHF *LicenceNr. |
| `position` | character | goalkeeper, defender or forwarder (sic) |
| `line` | integer | Line number, an integer (goalkeepers 1-2 = first / backup) |
| `first_name` | character | First (given) name of the player |
| `last_name` | character | Last (family) name of the player |
| `number` | character | Sweater number, a string |
| `is_topscorer` | logical | Wears the team's PostFinance Top Scorer jersey |
| `is_captain` | logical | Whether the player is the team captain |
| `first_goalkeeper` | logical | Starting goaltender |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nl_lineups-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A 404, or an empty answer (no games or rows for that season, team or phase). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A failed fetch, an error object in a 200, a body without the ids it must carry, or an answer for another season, team, game or player. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nl_lineups-example}

```python
nl_lineups(game_id=20261105000325)
```

_Last validated n/a._

## nl_shots

One game's shot attempts with rink location (metres) and expected goals (xG); NL games only.

**Endpoint URL:** `GET https://www.nationalleague.ch/api/games/{game_id}`

**Valid URL:** [https://www.nationalleague.ch/api/games/20261105000325](https://www.nationalleague.ch/api/games/20261105000325)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | SIHF game id, 14 digits `<season(4)><competition(4)><sequence(6)>` (game_id in nl_schedule; the same id as the sihf family's game_id), e.g. 20261105000325. |

### Returns {#nl_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | SIHF game id `<season(4)><competition(4)><sequence(6)>` (Int64; e.g. 20261105000325, 1105 = National League). The same id as the sihf family's game_id; its first four digits are the season END year. |
| `team_side` | character | home or away: the side of the team this row belongs to. |
| `team_id` | integer | SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `player_id` | integer | SIHF licence number (Int64; e.g. 305519 = Tobias Geisser). Joins the sihf family's player_id and every SIHF *LicenceNr. |
| `player_number` | character | Shooter's sweater number, from the game's lineup (a string). |
| `player_first_name` | character | Shooter's first name, from the game's lineup. |
| `player_last_name` | character | Shooter's last name, from the game's lineup. |
| `period` | integer | Period number (1-3; overtime follows) |
| `time` | integer | Game clock elapsed in seconds (period 2 starts at 1200) |
| `strength` | character | Strength: EQ (even), PP (power play), PK (shorthanded) |
| `shot_type` | character | Outcome: SOG (saved shot on goal), MISS, POST, BLOCK, GOAL |
| `pos_x` | numeric | Across-ice position in metres, 0-30 (rink width). Both axes run in metres on a 30 x 60 m rink. |
| `pos_y` | numeric | Distance in metres from the end boards of the attacked end, 0-60 along the rink length. Shots are mirrored to one attacking end (inferred); above 30 is a shot from beyond centre; the goal line is 4 m out. |
| `pos_x_percentage` | numeric | posX / 30 (verified on 475 shots) |
| `pos_y_percentage` | numeric | pos_y / 24, the depth of the app's zone drawing; above 1 for long shots. |
| `video_time` | integer | Seconds into the game video (= unix - unixStart; verified on 475 shots) |
| `xg` | numeric | Expected goals of this shot, 0-1. Non-zero from the 2023 season (one sampled game per season; 0 in the 2021 and 2022 games); always 0 on a BLOCK. |
| `xg_sum` | numeric | Running sum of this team's xg up to and including this shot |
| `unix` | integer | Wall-clock time of the shot, Unix epoch seconds |
| `unix_start` | integer | Wall-clock start of the game video, Unix epoch seconds |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nl_shots-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A 404, or an empty answer (no games or rows for that season, team or phase). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A failed fetch, an error object in a 200, a body without the ids it must carry, or an answer for another season, team, game or player. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nl_shots-example}

```python
nl_shots(game_id=20261105000325)
```

_Last validated n/a._

## nl_top_scorers

Top scorers (same row shape as /player).

**Endpoint URL:** `GET https://www.nationalleague.ch/api/player/topscorer`

**Valid URL:** [https://www.nationalleague.ch/api/player/topscorer?take=3&season=2026&phase=4940](https://www.nationalleague.ch/api/player/topscorer?take=3&season=2026&phase=4940)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `take` | `take` |  |  | `Y` | Number of rows (take=3 answered 3). |
| `season` | `season` |  |  | `Y` | Season END year: `2026` = 2025/26. Diffing 2026 vs 2027 changes every list, and /player/{playerId} gp (51 vs 7), so it scopes. Lists are served for 2019-2027 (the app's season picker, /api/content/metadata `seasons`) and patchily before: 2012 and 2017 answered, 2010/2014/2015/2016/2018 answered `[]` (probed 2026-10-12). |
| `phase` | `phase` |  |  | `Y` | SIHF phase id (see x-phase-ids). On /player, omitting it served the regular season (byte-identical to phase=4940); that default is assumed but unverified on this route. Phase ids change every season; whether the metadata ids apply to 2027 is unverified. |

### Returns {#nl_top_scorers-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | SIHF licence number (Int64; e.g. 305519 = Tobias Geisser). Joins the sihf family's player_id and every SIHF *LicenceNr. |
| `first_name` | character | First (given) name of the player |
| `last_name` | character | Last (family) name of the player |
| `team_id` | integer | SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `team_short_name` | character | Team abbreviation |
| `team_name` | character | Team name as the league app displays it |
| `number` | character | Sweater number, a string |
| `position` | character | goalkeeper, defender or forwarder (sic); one of 502 rows in the full 2026 body has the integer 0 |
| `topscorer_points` | integer | PostFinance Top Scorer game score (an app fantasy rating, not on-ice points) |
| `topscorer_point_average` | numeric | PostFinance Top Scorer game score per game |
| `topscorer_market_value` | integer | PostFinance Top Scorer game market value |
| `topscorer_market_value_trend` | integer | Market-value trend: 1 up, 0 flat, -1 down |
| `topscorer_amount` | integer | CHF amount credited to the team's Top Scorer jersey holder (non-zero for 14 players, one per team) |
| `is_topscorer` | logical | Wears the team's PostFinance Top Scorer jersey (one per team) |
| `nationality` | character | Nationality; unobserved: null or empty in every sampled row |
| `birth` | character | Date of birth, YYYY-MM-DD |
| `height` | integer | Height; unobserved: null or empty in every sampled row |
| `weight` | integer | Weight; unobserved: null or empty in every sampled row |
| `hand` | character | Shooting or catching hand; unobserved: null or empty in every sampled row |
| `lcrid` | integer | Not determined (small integer, 0-4 in 2026) |
| `noc` | integer | Not determined (small integer, 0-2 in 2026) |
| `gp` | integer | Number of games played |
| `g` | integer | Number of goals scored |
| `a1` | integer | First (primary) assists credited |
| `a2` | integer | Second (secondary) assists credited |
| `penalties` | integer | Penalties taken (upstream `fo`: fo2 + fo5 + fo10 + gm + mp). Not faceoffs: see face_offs / fot. |
| `pim` | integer | Penalty minutes |
| `geq` | integer | Even-strength goals |
| `gpp` | integer | Power-play goals |
| `gpk` | integer | Shorthanded goals |
| `gwg` | integer | Game-winning goals |
| `ps` | integer | Shootout shots taken |
| `pg` | integer | Not determined; 0 on every 2026 row |
| `pl` | integer | Plus count: on ice for an even-strength or shorthanded goal for |
| `mi` | integer | Minus count: on ice for a goal against |
| `toi` | integer | Time on ice, seconds |
| `toi_eq` | integer | Even-strength time on ice, seconds |
| `toipp` | integer | Power-play time on ice, seconds (lower-case pp, unlike the box score's toiPp) |
| `toi_pk` | integer | Shorthanded time on ice, seconds |
| `sog` | integer | Number of shots on goal |
| `shm` | integer | Shots that missed the net |
| `shp` | integer | Shots that hit the post |
| `shb` | integer | Own shots blocked by an opponent |
| `bks` | integer | Opponent shots blocked |
| `fow` | integer | Number of faceoffs won |
| `fol` | integer | Number of faceoffs lost |
| `shi` | integer | Number of shifts played |
| `gwgk` | integer | Not determined; 0 on every 2026 row |
| `gw` | integer | Goaltender wins (incl. overtime and shootout) |
| `gl` | integer | Goaltender losses (incl. overtime and shootout) |
| `fk` | integer | Games started in goal (inferred: fk <= gp; 42 of 44 for the 2026 games-played leader) |
| `ga` | integer | Goals against: number of goals conceded |
| `sa` | integer | Shots on goal against |
| `svs` | integer | Number of saves made by the goaltender |
| `so` | integer | Number of shutouts (games without a goal conceded) |
| `mip` | integer | Goaltender time played, hundredths of a minute (6000 = 60:00; = seconds / 0.6) |
| `assists` | integer | Assists (a1 + a2; verified on every 2026 row) |
| `points` | integer | Points (g + assists; verified) |
| `goal_efficiency` | numeric | Shooting percentage, 100 * g / sog (verified); -1 with no shots |
| `save_percentage` | numeric | Save percentage, 0-100 |
| `face_offs_percentage` | numeric | Faceoff win percentage, 0-100; -1 with no faceoffs |
| `face_offs` | integer | Faceoffs taken (fow + fol; verified) |
| `is_captain` | logical | Whether the player is the team captain |
| `first_goalkeeper` | logical | Team's first-choice goaltender |
| `ga_per_game` | numeric | Goals against per game (ga / gp; verified) |
| `points_per_game` | numeric | Points per game |
| `svs_per_game` | numeric | Goaltender saves per game played |
| `bks_per_game` | numeric | Blocked shots per game |
| `sog_per_game` | numeric | Shots on goal per game |
| `shm_per_game` | numeric | Missed shots per game |
| `shp_per_game` | numeric | Post hits per game |
| `fow_per_game` | numeric | Faceoffs won per game |
| `fol_per_game` | numeric | Faceoffs lost per game |
| `toi_eq_per_game` | numeric | Even-strength time on ice per game, seconds |
| `toi_pp_per_game` | numeric | Power-play time on ice per game, seconds |
| `toi_pk_per_game` | numeric | Shorthanded time on ice per game, seconds |
| `face_offs_per_game_percentage` | numeric | Not determined; 0 on the sampled rows |
| `toi_average` | numeric | Time on ice per game, seconds |
| `plus_minus` | integer | Plus/minus (pl - mi; verified) |
| `tgs` | integer | Team's total game time, seconds (about 3600 per game incl. overtime) |
| `sep` | integer | Goaltender time played, seconds |
| `toi_percentage` | numeric | Share of the team's game time in goal, 100 * sep / tgs (verified) |
| `gwit` | integer | Wins in regulation time (gw = gwit + overtime/shootout wins; verified) |
| `glit` | integer | Losses in regulation time (gl = glit + overtime/shootout losses; verified) |
| `sag` | integer | Shots on goal against that were saved (= svs on every 2026 goaltender row) |
| `sago` | integer | Shots on goal against that went in (= ga on every 2026 goaltender row) |
| `sam` | integer | Shots against that missed the net |
| `sap` | integer | Shots against that hit the post |
| `sab` | integer | Shots against that were blocked |
| `saslot` | integer | Slot shots against |
| `sagoslot` | integer | Slot shots against that went in |
| `aeq` | integer | Even-strength assists |
| `app` | integer | Power-play assists |
| `apk` | integer | Shorthanded assists |
| `ha` | integer | Hat tricks (inferred; at most 2 in 2026) |
| `fo2` | integer | Minor (2-minute) penalties |
| `fo5` | integer | Major (5-minute) penalties |
| `fo10` | integer | 10-minute misconducts |
| `gm` | integer | Game misconducts |
| `mp` | integer | Match penalties |
| `pm` | integer | Not determined; 0 on every 2026 row |
| `pwg` | integer | Not determined; 0 on every 2026 row |
| `sgo` | integer | Not determined. Equals gff + gfd on every team row; skater values are close to but not always g |
| `fowoz` | integer | Offensive-zone faceoffs won |
| `fownz` | integer | Neutral-zone faceoffs won |
| `fowdz` | integer | Defensive-zone faceoffs won |
| `foloz` | integer | Offensive-zone faceoffs lost |
| `folnz` | integer | Neutral-zone faceoffs lost |
| `foldz` | integer | Defensive-zone faceoffs lost |
| `psg` | integer | Goals scored in shootouts |
| `pssvs` | integer | Saves made in shootouts |
| `got` | integer | Overtime goals (a subset of g) |
| `ot_rank` | integer | Overtime-scoring rank; 0 on the 2026 rows (filled in 2027) |
| `ot_g` | integer | Overtime goals in the overtime table; 0 on every 2026 row |
| `ot_a1` | integer | Overtime first assists; 0 on every 2026 row |
| `ot_a2` | integer | Overtime second assists; 0 on every 2026 row |
| `ot_toi` | integer | Overtime time on ice, seconds; 0 on every 2026 row |
| `ot_a` | integer | Overtime assists; 0 on every 2026 row |
| `ot_p` | integer | Overtime points; 0 on every 2026 row |
| `psgwg` | integer | Game-deciding shootout goals; 0 on every 2026 row |
| `sogslot` | integer | Shots on goal from the slot |
| `pswg` | integer | Shootout winners (inferred) |
| `toishi` | numeric | Time on ice per shift, seconds |
| `shigp` | numeric | Shifts per game |
| `saslotgp` | numeric | Slot shots against per game |
| `samgp` | numeric | Missed shots against per game |
| `sapgp` | numeric | Post hits against per game |
| `svs_pslot` | numeric | Slot save percentage, 100 * (saslot - sagoslot) / saslot |
| `sa_per_game` | numeric | Shots on goal against per game |
| `otw` | integer | Goaltender overtime or shootout wins |
| `otl` | integer | Goaltender overtime or shootout losses |
| `fopoz` | numeric | Offensive-zone faceoff win percentage, 0-100 |
| `fopnz` | numeric | Neutral-zone faceoff win percentage, 0-100 |
| `fopdz` | numeric | Defensive-zone faceoff win percentage, 0-100 |
| `fot` | integer | Faceoffs taken (fow + fol) |
| `fogp` | numeric | Faceoffs taken per game |
| `psm` | integer | Shootout misses (inferred; ps = psg + psm fails for 67 of 447 skaters) |
| `ps_percentage` | numeric | Shootout success percentage, 0-100 |
| `gw_shutout` | integer | Not determined; equals gw on goaltender rows |
| `sogslot_per_game` | numeric | Slot shots on goal per game |
| `psag` | integer | Shootout goals against (equals ps on goaltender rows) |
| `pssvs_percentage` | numeric | Shootout save percentage, 0-100; -1 with no shootout shots faced |
| `show_rank` | logical | App flag: show the row's rank |
| `chf` | integer | CHF credited for the player's points: 300 * points on every 2026 row (PostFinance youth-fund scheme; inferred) |
| `team_stats` | character | Per-team split for a player who changed teams; unobserved: null or empty in every sampled row |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nl_top_scorers-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A 404, or an empty answer (no games or rows for that season, team or phase). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A failed fetch, an error object in a 200, a body without the ids it must carry, or an answer for another season, team, game or player. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nl_top_scorers-example}

```python
nl_top_scorers(phase=4940, season=2026, take=3)
```

_Last validated n/a._

## nl_standings

Standings with team season stats.

**Endpoint URL:** `GET https://www.nationalleague.ch/api/teams`

**Valid URL:** [https://www.nationalleague.ch/api/teams?season=2026&phase=4940](https://www.nationalleague.ch/api/teams?season=2026&phase=4940)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season END year: `2026` = 2025/26. Diffing 2026 vs 2027 changes every list, and /player/{playerId} gp (51 vs 7), so it scopes. Lists are served for 2019-2027 (the app's season picker, /api/content/metadata `seasons`) and patchily before: 2012 and 2017 answered, 2010/2014/2015/2016/2018 answered `[]` (probed 2026-10-12). |
| `phase` | `phase` |  |  | `Y` | SIHF phase id (see x-phase-ids). On /player, omitting it served the regular season (byte-identical to phase=4940); that default is assumed but unverified on this route. Phase ids change every season; whether the metadata ids apply to 2027 is unverified. |

### Returns {#nl_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `rank` | integer | Position of the team in the standings table |
| `team_id` | integer | SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `name` | character | Team name as the league app displays it |
| `short_name` | character | Team abbreviation |
| `streak` | character | Last results, most recent first: W or L |
| `gp` | integer | Number of games played |
| `gw` | integer | Wins, all kinds |
| `gwot` | integer | Overtime + shootout wins (gwo + gwpe) |
| `gwpe` | integer | Number of wins decided in a shootout |
| `gl` | integer | Losses, all kinds |
| `glot` | integer | Overtime + shootout losses (glo + glpe) |
| `glpe` | integer | Shootout losses |
| `po` | integer | Standings points: 3 per regulation win, 2 per overtime/shootout win, 1 per overtime/shootout loss (verified) |
| `g` | integer | Number of goals scored |
| `geq` | integer | Even-strength goals (geq + gpp + gpk can fall short of g) |
| `gpp` | integer | Power-play goals |
| `gpk` | integer | Shorthanded goals |
| `ga` | integer | Goals against: number of goals conceded |
| `gaeq` | integer | Even-strength goals against |
| `gapp` | integer | Shorthanded goals allowed on the power play |
| `gapk` | integer | Power-play goals allowed while shorthanded |
| `go` | integer | Goal difference, g - ga (verified) |
| `ppo` | integer | Power-play opportunities |
| `pks` | integer | Times shorthanded |
| `sog` | integer | Number of shots on goal |
| `penalties` | integer | Penalties taken (upstream `fo`: fo2 + fo5 + fo10 + gm + mp). Not faceoffs: see face_offs / fot. |
| `pim` | integer | Penalty minutes |
| `fow` | integer | Number of faceoffs won |
| `fol` | integer | Number of faceoffs lost |
| `shm` | integer | Shots that missed the net |
| `shp` | integer | Shots that hit the post |
| `shb` | integer | Own shots blocked by an opponent |
| `bks` | integer | Opponent shots blocked |
| `sa` | integer | Shots on goal against |
| `sam` | integer | Shots against that missed the net; 0 on every 2026 team row |
| `sap` | integer | Shots against that hit the post; 0 on every 2026 team row |
| `sab` | integer | Shots against that were blocked; 0 on every 2026 team row |
| `sort_order` | integer | App sort key; not the rank |
| `website` | character | Club website URL |
| `position_change` | character | Rank movement since the last round ('none' on every 2026 row) |
| `is_live` | logical | Team is playing now |
| `current_result` | integer | Team's goals in a live game; 0 otherwise |
| `current_opponent_result` | integer | Opponent's goals in a live game; 0 otherwise |
| `pos` | integer | Standings position (= rank on every 2026 row) |
| `hg` | integer | Home games played (gp / 2 on every 2026 row) |
| `gsow` | integer | Shootout wins; duplicate of gwpe (equal on every 2026 row) |
| `gfd` | integer | Goals by defenders |
| `gff` | integer | Goals by forwards |
| `gfi` | integer | Goals by import (non-Swiss-licence) players (inferred; cf. the sihf/ teamGoalPosNation table) |
| `gasow` | integer | Shootout losses; duplicate of glpe (equal on every 2026 row) |
| `pp` | integer | Power-play time, seconds |
| `bp` | integer | Shorthanded (box-play) time, seconds |
| `fo2` | integer | Minor (2-minute) penalties |
| `fo5` | integer | Major (5-minute) penalties |
| `fo10` | integer | 10-minute misconducts |
| `gm` | integer | Game misconducts |
| `mp` | integer | Match penalties |
| `ps` | integer | Shootout attempts taken |
| `psg` | integer | Goals scored in shootouts |
| `psm` | integer | Shootout misses (ps = psg + psm on every 2026 team row) |
| `psa` | integer | Shootout shots faced (psga + pssvs) |
| `psga` | integer | Shootout goals against |
| `pssvs` | integer | Saves made in shootouts |
| `spec` | integer | Total home attendance |
| `cap` | integer | Total home arena capacity over the home games |
| `fowoz` | integer | Offensive-zone faceoffs won |
| `fownz` | integer | Neutral-zone faceoffs won |
| `fowdz` | integer | Defensive-zone faceoffs won |
| `foloz` | integer | Offensive-zone faceoffs lost |
| `folnz` | integer | Neutral-zone faceoffs lost |
| `foldz` | integer | Defensive-zone faceoffs lost |
| `sgo` | integer | Not determined. Equals gff + gfd on every team row; skater values are close to but not always g |
| `sslot` | integer | Shots from the slot |
| `sag` | integer | Shots on goal against that were saved; 0 on every 2026 team row (use sa) |
| `sago` | integer | Shots on goal against that went in; 0 on every 2026 team row (use ga) |
| `saslot` | integer | Slot shots against |
| `qualification_status` | character | Clinch marker ('none' on every 2026 row) |
| `is_qualified_chl` | logical | Qualified for the Champions Hockey League |
| `goal_efficiency` | numeric | Shooting percentage, 100 * g / sog (verified); -1 with no shots |
| `ppg_efficiency` | numeric | Power-play percentage, 100 * gpp / ppo (verified) |
| `ppggp` | numeric | Power-play goals per game |
| `ppopgp` | numeric | Power-play opportunities per game |
| `ppgp` | numeric | Power-play time per game, seconds |
| `gpg` | numeric | Goals scored per game played |
| `gagp` | numeric | Goals against per game |
| `gappgp` | numeric | Shorthanded goals against per game |
| `geqgp` | numeric | Even-strength goals per game |
| `gaeqgp` | numeric | Even-strength goals against per game |
| `bks_per_game` | numeric | Blocked shots per game |
| `pim_per_game` | numeric | Penalty minutes per game |
| `ps_percentage` | numeric | Shootout success percentage, 0-100 |
| `attendance` | integer | Average home attendance (spec / hg) |
| `capacity` | integer | Arena capacity (cap / hg) |
| `utilization` | numeric | Capacity utilisation, 100 * spec / cap |
| `soggp` | numeric | Shots on goal per game |
| `sslotgp` | numeric | Slot shots per game |
| `shmgp` | numeric | Missed shots per game |
| `shpgp` | numeric | Post hits per game |
| `gpkgp` | numeric | Shorthanded goals per game |
| `pk_efficiency` | numeric | Penalty-kill percentage, 100 * (1 - gapk / pks) (verified) |
| `pksgp` | numeric | Times shorthanded per game |
| `gapkgp` | numeric | Power-play goals against per game |
| `bpgp` | numeric | Shorthanded time per game, seconds |
| `fot` | integer | Faceoffs taken (fow + fol) |
| `face_offs_percentage` | numeric | Faceoff win percentage, 0-100; -1 with no faceoffs |
| `fogp` | numeric | Faceoffs taken per game |
| `fowgp` | numeric | Faceoffs won per game |
| `folgp` | numeric | Faceoffs lost per game |
| `fopoz` | numeric | Offensive-zone faceoff win percentage, 0-100 |
| `fopnz` | numeric | Neutral-zone faceoff win percentage, 0-100 |
| `fopdz` | numeric | Defensive-zone faceoff win percentage, 0-100 |
| `sagp` | numeric | Shots on goal against per game |
| `saslotgp` | numeric | Slot shots against per game |
| `gffd` | integer | Goals by forwards and defenders (gff + gfd; verified) |
| `gffp` | numeric | Share of goals by forwards, 100 * gff / g |
| `gfdp` | numeric | Share of goals by defenders, 100 * gfd / g |
| `gfip` | numeric | Share of goals by import players, 100 * gfi / g |
| `pssvs_percentage` | numeric | Shootout save percentage, 0-100; -1 with no shootout shots faced |
| `pgp` | numeric | Standings points per game |
| `gwit` | integer | Wins in regulation time (gw = gwit + overtime/shootout wins; verified) |
| `gwo` | integer | Number of wins in overtime |
| `glit` | integer | Losses in regulation time (gl = glit + overtime/shootout losses; verified) |
| `glo` | integer | Overtime losses |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nl_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A 404, or an empty answer (no games or rows for that season, team or phase). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A failed fetch, an error object in a 200, a body without the ids it must carry, or an answer for another season, team, game or player. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nl_standings-example}

```python
nl_standings(phase=4940, season=2026)
```

_Last validated n/a._
