# National League — National League app API (www.nationalleague.ch/api, Switzerland) — Other

> National League — National League app API (www.nationalleague.ch/api, Switzerland) — Other — function reference in sdv-py, the SportsDataverse Python package.

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
| `game_id` | Int64 | SIHF game id `<season(4)><competition(4)><sequence(6)>` (Int64; e.g. 20261105000325, 1105 = National League). The same id as the sihf family's game_id; its first four digits are the season END year. |
| `home_team_short_name` | String | Home team abbreviation (e.g. HCD) |
| `home_team_name` | String | Home team display name |
| `away_team_short_name` | String | Away team abbreviation |
| `away_team_name` | String | Away team display name |
| `home_team_id` | Int64 | Home team. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `away_team_id` | Int64 | Away team. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `vimeo_video_id` | Int64 | Full-game video. Vimeo video id (Int64): vimeo.com/<id>. |
| `date` | String | Scheduled start, ISO 8601 with UTC offset |
| `status` | String | Game state: finished, canceled, beforeStartOfPlay (others appear live) |
| `base_status` | String | Coarse game state; equal to status in every sampled game |
| `home_team_result` | Int64 | Home team goals (final or current score; 0 before the game) |
| `away_team_result` | Int64 | Away team goals (final or current score; 0 before the game) |
| `is_overtime` | Boolean | Game went to overtime |
| `is_shootout` | Boolean | Game went to a shootout |
| `highlight_vimeo_video_id` | Int64 | Highlights video. Vimeo video id (Int64): vimeo.com/<id>. |
| `has_highlight` | Boolean | A highlight video exists |
| `is_current` | Boolean | Game belongs to the current game day |
| `is_current_for_team` | Boolean | Game is the team's current game (team-scoped views) |
| `tv_partner` | String | Broadcaster logo image URLs; unobserved: null, absent or empty in every sampled row |
| `spectators` | Int64 | Attendance (Int64; the API sends a digit string). |
| `has_my_sports` | Boolean | Broadcast on MySports |
| `referees` | String | Officials as '#<number> <name>' strings (referees and linesmen; one entry in some games) |
| `is_exhibition` | Boolean | Exhibition game, not NL competition (competition code other than 1105) |
| `home_team_coach` | String | Home head coach name; filled in a game's `overview`, null in the season lists; unobserved: null, absent or empty in every sampled row |
| `away_team_coach` | String | Away head coach name; filled in a game's `overview`, null in the season lists; unobserved: null, absent or empty in every sampled row |
| `game_time` | Int64 | Elapsed game minutes; 0 on every game in the season lists, 60 in a finished game's overview |
| `show_game_time` | Boolean | App flag: show gameTime |
| `arena` | String | Name of the arena the game is played in |

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
| `game_id` | Int64 | SIHF game id `<season(4)><competition(4)><sequence(6)>` (Int64; e.g. 20261105000325, 1105 = National League). The same id as the sihf family's game_id; its first four digits are the season END year. |
| `home_team_short_name` | String | Home team abbreviation (e.g. HCD) |
| `home_team_name` | String | Home team display name |
| `away_team_short_name` | String | Away team abbreviation |
| `away_team_name` | String | Away team display name |
| `home_team_id` | Int64 | Home team. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `away_team_id` | Int64 | Away team. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `vimeo_video_id` | Int64 | Full-game video. Vimeo video id (Int64): vimeo.com/<id>. |
| `date` | String | Scheduled start, ISO 8601 with UTC offset |
| `status` | String | Game state: finished, canceled, beforeStartOfPlay (others appear live) |
| `base_status` | String | Coarse game state; equal to status in every sampled game |
| `home_team_result` | Int64 | Home team goals (final or current score; 0 before the game) |
| `away_team_result` | Int64 | Away team goals (final or current score; 0 before the game) |
| `is_overtime` | Boolean | Game went to overtime |
| `is_shootout` | Boolean | Game went to a shootout |
| `highlight_vimeo_video_id` | Int64 | Highlights video. Vimeo video id (Int64): vimeo.com/<id>. |
| `has_highlight` | Boolean | A highlight video exists |
| `is_current` | Boolean | Game belongs to the current game day |
| `is_current_for_team` | Boolean | Game is the team's current game (team-scoped views) |
| `tv_partner` | String | Broadcaster logo image URLs |
| `spectators` | Int64 | Attendance (Int64; the API sends a digit string). |
| `has_my_sports` | Boolean | Broadcast on MySports |
| `referees` | String | Officials as '#<number> <name>' strings (referees and linesmen; one entry in some games); unobserved: null, absent or empty in every sampled row |
| `is_exhibition` | Boolean | Exhibition game, not NL competition (competition code other than 1105) |
| `home_team_coach` | String | Home head coach name; filled in a game's `overview`, null in the season lists; unobserved: null, absent or empty in every sampled row |
| `away_team_coach` | String | Away head coach name; filled in a game's `overview`, null in the season lists; unobserved: null, absent or empty in every sampled row |
| `game_time` | Int64 | Elapsed game minutes; 0 on every game in the season lists, 60 in a finished game's overview |
| `show_game_time` | Boolean | App flag: show gameTime |
| `arena` | String | Name of the arena the game is played in |

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
| `series_group` | Int64 | Series number within the round (1 = first pairing; 0 seen in playouts) |
| `series_home_team_short_name` | String | Abbreviation of the team with home ice |
| `series_away_team_short_name` | String | Abbreviation of the other team |
| `series_home_team_id` | Int64 | Team with home ice in the series. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `series_away_team_id` | Int64 | The other team in the series. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `series_home_team_result` | Int64 | Series wins of the team with home ice |
| `series_away_team_result` | Int64 | Series wins of the other team |
| `series_best_of` | Int64 | Series length in games (7; 2 in the play-in) |
| `series_title` | String | Series length label, e.g. 'Best of 7' |
| `round` | String | Round key: final, semifinal, quarterfinal, playIn (/playoffs); leagueQualifications, playoutFinal (/playouts) |
| `game_id` | Int64 | SIHF game id `<season(4)><competition(4)><sequence(6)>` (Int64; e.g. 20261105000325, 1105 = National League). The same id as the sihf family's game_id; its first four digits are the season END year. |
| `home_team_short_name` | String | Home team abbreviation (e.g. HCD) |
| `home_team_name` | String | Home team display name |
| `away_team_short_name` | String | Away team abbreviation |
| `away_team_name` | String | Away team display name |
| `home_team_id` | Int64 | Home team. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `away_team_id` | Int64 | Away team. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `vimeo_video_id` | Int64 | Full-game video. Vimeo video id (Int64): vimeo.com/<id>. |
| `date` | String | Scheduled start, ISO 8601 with UTC offset |
| `status` | String | Game state: finished, canceled, beforeStartOfPlay (others appear live) |
| `base_status` | String | Coarse game state; equal to status in every sampled game |
| `home_team_result` | Int64 | Home team goals (final or current score; 0 before the game) |
| `away_team_result` | Int64 | Away team goals (final or current score; 0 before the game) |
| `is_overtime` | Boolean | Game went to overtime |
| `is_shootout` | Boolean | Game went to a shootout |
| `highlight_vimeo_video_id` | Int64 | Highlights video. Vimeo video id (Int64): vimeo.com/<id>. |
| `has_highlight` | Boolean | A highlight video exists |
| `is_current` | Boolean | Game belongs to the current game day |
| `is_current_for_team` | Boolean | Game is the team's current game (team-scoped views) |
| `tv_partner` | String | Broadcaster logo image URLs |
| `spectators` | Int64 | Attendance (Int64; the API sends a digit string). |
| `has_my_sports` | Boolean | Broadcast on MySports |
| `referees` | String | Officials as '#<number> <name>' strings (referees and linesmen; one entry in some games) |
| `is_exhibition` | Boolean | Exhibition game, not NL competition (competition code other than 1105) |
| `home_team_coach` | String | Home head coach name; filled in a game's `overview`, null in the season lists; unobserved: null, absent or empty in every sampled row |
| `away_team_coach` | String | Away head coach name; filled in a game's `overview`, null in the season lists; unobserved: null, absent or empty in every sampled row |
| `game_time` | Int64 | Elapsed game minutes; 0 on every game in the season lists, 60 in a finished game's overview |
| `show_game_time` | Boolean | App flag: show gameTime |
| `arena` | String | Name of the arena the game is played in |

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
| `series_group` | Int64 | Series number within the round (1 = first pairing; 0 seen in playouts) |
| `series_home_team_short_name` | String | Abbreviation of the team with home ice |
| `series_away_team_short_name` | String | Abbreviation of the other team |
| `series_home_team_id` | Int64 | Team with home ice in the series. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `series_away_team_id` | Int64 | The other team in the series. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `series_home_team_result` | Int64 | Series wins of the team with home ice |
| `series_away_team_result` | Int64 | Series wins of the other team |
| `series_best_of` | Int64 | Series length in games (7; 2 in the play-in) |
| `series_title` | String | Series length label, e.g. 'Best of 7' |
| `round` | String | Round key: final, semifinal, quarterfinal, playIn (/playoffs); leagueQualifications, playoutFinal (/playouts) |
| `game_id` | Int64 | SIHF game id `<season(4)><competition(4)><sequence(6)>` (Int64; e.g. 20261105000325, 1105 = National League). The same id as the sihf family's game_id; its first four digits are the season END year. |
| `home_team_short_name` | String | Home team abbreviation (e.g. HCD) |
| `home_team_name` | String | Home team display name |
| `away_team_short_name` | String | Away team abbreviation |
| `away_team_name` | String | Away team display name |
| `home_team_id` | Int64 | Home team. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `away_team_id` | Int64 | Away team. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `vimeo_video_id` | Int64 | Full-game video. Vimeo video id (Int64): vimeo.com/<id>. |
| `date` | String | Scheduled start, ISO 8601 with UTC offset |
| `status` | String | Game state: finished, canceled, beforeStartOfPlay (others appear live) |
| `base_status` | String | Coarse game state; equal to status in every sampled game |
| `home_team_result` | Int64 | Home team goals (final or current score; 0 before the game) |
| `away_team_result` | Int64 | Away team goals (final or current score; 0 before the game) |
| `is_overtime` | Boolean | Game went to overtime |
| `is_shootout` | Boolean | Game went to a shootout |
| `highlight_vimeo_video_id` | Int64 | Highlights video. Vimeo video id (Int64): vimeo.com/<id>. |
| `has_highlight` | Boolean | A highlight video exists |
| `is_current` | Boolean | Game belongs to the current game day |
| `is_current_for_team` | Boolean | Game is the team's current game (team-scoped views) |
| `tv_partner` | String | Broadcaster logo image URLs |
| `spectators` | Int64 | Attendance (Int64; the API sends a digit string). |
| `has_my_sports` | Boolean | Broadcast on MySports |
| `referees` | String | Officials as '#<number> <name>' strings (referees and linesmen; one entry in some games) |
| `is_exhibition` | Boolean | Exhibition game, not NL competition (competition code other than 1105) |
| `home_team_coach` | String | Home head coach name; filled in a game's `overview`, null in the season lists; unobserved: null, absent or empty in every sampled row |
| `away_team_coach` | String | Away head coach name; filled in a game's `overview`, null in the season lists; unobserved: null, absent or empty in every sampled row |
| `game_time` | Int64 | Elapsed game minutes; 0 on every game in the season lists, 60 in a finished game's overview |
| `show_game_time` | Boolean | App flag: show gameTime |
| `arena` | String | Name of the arena the game is played in |

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
| `game_id` | Int64 | SIHF game id `<season(4)><competition(4)><sequence(6)>` (Int64; e.g. 20261105000325, 1105 = National League). The same id as the sihf family's game_id; its first four digits are the season END year. |
| `home_team_short_name` | String | Home team abbreviation (e.g. HCD) |
| `home_team_name` | String | Home team display name |
| `away_team_short_name` | String | Away team abbreviation |
| `away_team_name` | String | Away team display name |
| `home_team_id` | Int64 | Home team. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `away_team_id` | Int64 | Away team. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `vimeo_video_id` | Int64 | Full-game video. Vimeo video id (Int64): vimeo.com/<id>. |
| `date` | String | Scheduled start, ISO 8601 with UTC offset |
| `status` | String | Game state: finished, canceled, beforeStartOfPlay (others appear live) |
| `base_status` | String | Coarse game state; equal to status in every sampled game |
| `home_team_result` | Int64 | Home team goals (final or current score; 0 before the game) |
| `away_team_result` | Int64 | Away team goals (final or current score; 0 before the game) |
| `is_overtime` | Boolean | Game went to overtime |
| `is_shootout` | Boolean | Game went to a shootout |
| `highlight_vimeo_video_id` | Int64 | Highlights video. Vimeo video id (Int64): vimeo.com/<id>. |
| `has_highlight` | Boolean | A highlight video exists |
| `is_current` | Boolean | Game belongs to the current game day |
| `is_current_for_team` | Boolean | Game is the team's current game (team-scoped views) |
| `tv_partner` | String | Broadcaster logo image URLs |
| `spectators` | Int64 | Attendance (Int64; the API sends a digit string). |
| `has_my_sports` | Boolean | Broadcast on MySports |
| `referees` | String | Officials as '#<number> <name>' strings (referees and linesmen; one entry in some games) |
| `is_exhibition` | Boolean | Exhibition game, not NL competition (competition code other than 1105) |
| `home_team_coach` | String | Home head coach name; filled in a game's `overview`, null in the season lists |
| `away_team_coach` | String | Away head coach name; filled in a game's `overview`, null in the season lists |
| `game_time` | Int64 | Elapsed game minutes; 0 on every game in the season lists, 60 in a finished game's overview |
| `show_game_time` | Boolean | App flag: show gameTime |
| `arena` | String | Name of the arena the game is played in |
| `home_team_first_result` | Int64 | Home goals in the 1st period |
| `away_team_first_result` | Int64 | Away goals in the 1st period |
| `home_team_second_result` | Int64 | Home goals in the 2nd period |
| `away_team_second_result` | Int64 | Away goals in the 2nd period |
| `home_team_third_result` | Int64 | Home goals in the 3rd period |
| `away_team_third_result` | Int64 | Away goals in the 3rd period |
| `home_team_overtime_result` | Int64 | Home overtime goals, all overtime periods |
| `away_team_overtime_result` | Int64 | Away overtime goals, all overtime periods |
| `home_team_overtime1_result` | Int64 | Home goals in overtime period 1 (playoff games can have several) |
| `away_team_overtime1_result` | Int64 | Away goals in overtime period 1 |
| `home_team_overtime2_result` | Int64 | Home goals in overtime period 2 |
| `away_team_overtime2_result` | Int64 | Away goals in overtime period 2 |
| `home_team_overtime3_result` | Int64 | Home goals in overtime period 3 |
| `away_team_overtime3_result` | Int64 | Away goals in overtime period 3 |
| `home_team_shootout_result` | Int64 | Home shootout goals |
| `away_team_shootout_result` | Int64 | Away shootout goals |
| `home_aggregated_score` | String | Home aggregate over a two-legged tie; unobserved: null or empty in every sampled row |
| `away_aggregated_score` | String | Away aggregate over a two-legged tie; unobserved: null or empty in every sampled row |

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
| `game_id` | Int64 | SIHF game id `<season(4)><competition(4)><sequence(6)>` (Int64; e.g. 20261105000325, 1105 = National League). The same id as the sihf family's game_id; its first four digits are the season END year. |
| `event_index` | Int64 | 1-based position of the event in the game's timeline. |
| `team_side` | String | home or away: the side of the team this row belongs to. |
| `game_seconds` | Int64 | Elapsed game time of the event in seconds, from the MMSS `time` (822 = 8:22 -> 502). |
| `team_id` | Int64 | SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `home_team` | Boolean | Event belongs to the home team |
| `time` | String | Game clock elapsed as an MMSS string without separator ('822' = 8:22, '5954' = 59:54) |
| `video_time` | Int64 | Seconds into the game video; 0 when there is no clip |
| `video_vimeo_id` | Int64 | Clip of this event. Vimeo video id (Int64): vimeo.com/<id>. |
| `action` | String | Event type: goal, foul (penalty), in / out (goaltender enters / leaves), revokedGoal |
| `foul_type` | Int64 | SIHF penalty code (116-125 in the sample); 0 on non-penalty events |
| `foul_minutes` | Int64 | Penalty minutes; 0 on non-penalty events |
| `player_first_name` | String | Shooter's first name, from the game's lineup. |
| `player_last_name` | String | Shooter's last name, from the game's lineup. |
| `player_number` | String | Shooter's sweater number, from the game's lineup (a string). |
| `player_total` | Int64 | Scorer's goals this season including this one; 0 on non-goal events |
| `period` | Int64 | Period number, an integer (the parent `third` is a string) |
| `overtime` | Int64 | Overtime period number; 0 in regulation |
| `goal_situation` | String | Goal kind; 'normal' on every sampled event |
| `home_team_result` | Int64 | Home score after the event |
| `away_team_result` | Int64 | Away score after the event |
| `assist1_first_name` | String | First assist: first name |
| `assist1_last_name` | String | First assist: last name |
| `assist1_number` | String | First assist: sweater number, a string |
| `assist1_total` | Int64 | First assister's assists this season including this one |
| `assist2_first_name` | String | Second assist: first name |
| `assist2_last_name` | String | Second assist: last name |
| `assist2_number` | String | Second assist: sweater number, a string |
| `assist2_total` | Int64 | Second assister's assists this season including this one |
| `situation` | String | Goal tags, comma-separated: EQ (even strength), PP1 (power play), GWG (game-winning goal); null on non-goals |
| `revoked_goal_type` | Int64 | Revoked-goal category code (1 on every sampled event) |
| `revoked_goal_sub_type` | Int64 | Revoked-goal reason code (1 on the revoked goal, 0 elsewhere) |
| `review_successful` | String | Video-review outcome; unobserved: null or empty in every sampled row |
| `review_goal_counts` | Boolean | Goal stood after review |

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
| `game_id` | Int64 | SIHF game id `<season(4)><competition(4)><sequence(6)>` (Int64; e.g. 20261105000325, 1105 = National League). The same id as the sihf family's game_id; its first four digits are the season END year. |
| `team_side` | String | home or away: the side of the team this row belongs to. |
| `team_id` | Int64 | SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `unit` | String | Lineup group: goalkeeper, or the line number 1-4 as a string. |
| `player_id` | Int64 | SIHF licence number (Int64; e.g. 305519 = Tobias Geisser). Joins the sihf family's player_id and every SIHF *LicenceNr. |
| `position` | String | goalkeeper, defender or forwarder (sic) |
| `line` | Int64 | Line number, an integer (goalkeepers 1-2 = first / backup) |
| `first_name` | String | First (given) name of the player |
| `last_name` | String | Last (family) name of the player |
| `number` | String | Sweater number, a string |
| `is_topscorer` | Boolean | Wears the team's PostFinance Top Scorer jersey |
| `is_captain` | Boolean | Whether the player is the team captain |
| `first_goalkeeper` | Boolean | Starting goaltender |

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
| `game_id` | Int64 | SIHF game id `<season(4)><competition(4)><sequence(6)>` (Int64; e.g. 20261105000325, 1105 = National League). The same id as the sihf family's game_id; its first four digits are the season END year. |
| `team_side` | String | home or away: the side of the team this row belongs to. |
| `team_id` | Int64 | SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `player_id` | Int64 | SIHF licence number (Int64; e.g. 305519 = Tobias Geisser). Joins the sihf family's player_id and every SIHF *LicenceNr. |
| `player_number` | String | Shooter's sweater number, from the game's lineup (a string). |
| `player_first_name` | String | Shooter's first name, from the game's lineup. |
| `player_last_name` | String | Shooter's last name, from the game's lineup. |
| `period` | Int64 | Period number (1-3; overtime follows) |
| `time` | Int64 | Game clock elapsed in seconds (period 2 starts at 1200) |
| `strength` | String | Strength: EQ (even), PP (power play), PK (shorthanded) |
| `shot_type` | String | Outcome: SOG (saved shot on goal), MISS, POST, BLOCK, GOAL |
| `pos_x` | Float64 | Across-ice position in metres, 0-30 (rink width). Both axes run in metres on a 30 x 60 m rink. |
| `pos_y` | Float64 | Distance in metres from the end boards of the attacked end, 0-60 along the rink length. Shots are mirrored to one attacking end (inferred); above 30 is a shot from beyond centre; the goal line is 4 m out. |
| `pos_x_percentage` | Float64 | posX / 30 (verified on 475 shots) |
| `pos_y_percentage` | Float64 | pos_y / 24, the depth of the app's zone drawing; above 1 for long shots. |
| `video_time` | Int64 | Seconds into the game video (= unix - unixStart; verified on 475 shots) |
| `xg` | Float64 | Expected goals of this shot, 0-1. Non-zero from the 2023 season (one sampled game per season; 0 in the 2021 and 2022 games); always 0 on a BLOCK. |
| `xg_sum` | Float64 | Running sum of this team's xg up to and including this shot |
| `unix` | Int64 | Wall-clock time of the shot, Unix epoch seconds |
| `unix_start` | Int64 | Wall-clock start of the game video, Unix epoch seconds |

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
| `player_id` | Int64 | SIHF licence number (Int64; e.g. 305519 = Tobias Geisser). Joins the sihf family's player_id and every SIHF *LicenceNr. |
| `first_name` | String | First (given) name of the player |
| `last_name` | String | Last (family) name of the player |
| `team_id` | Int64 | SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `team_short_name` | String | Team abbreviation |
| `team_name` | String | Team name as the league app displays it |
| `number` | String | Sweater number, a string |
| `position` | String | goalkeeper, defender or forwarder (sic); one of 502 rows in the full 2026 body has the integer 0 |
| `topscorer_points` | Int64 | PostFinance Top Scorer game score (an app fantasy rating, not on-ice points) |
| `topscorer_point_average` | Float64 | PostFinance Top Scorer game score per game |
| `topscorer_market_value` | Int64 | PostFinance Top Scorer game market value |
| `topscorer_market_value_trend` | Int64 | Market-value trend: 1 up, 0 flat, -1 down |
| `topscorer_amount` | Int64 | CHF amount credited to the team's Top Scorer jersey holder (non-zero for 14 players, one per team) |
| `is_topscorer` | Boolean | Wears the team's PostFinance Top Scorer jersey (one per team) |
| `nationality` | String | Nationality; unobserved: null or empty in every sampled row |
| `birth` | String | Date of birth, YYYY-MM-DD |
| `height` | Int64 | Height; unobserved: null or empty in every sampled row |
| `weight` | Int64 | Weight; unobserved: null or empty in every sampled row |
| `hand` | String | Shooting or catching hand; unobserved: null or empty in every sampled row |
| `lcrid` | Int64 | Not determined (small integer, 0-4 in 2026) |
| `noc` | Int64 | Not determined (small integer, 0-2 in 2026) |
| `gp` | Int64 | Number of games played |
| `g` | Int64 | Number of goals scored |
| `a1` | Int64 | First (primary) assists credited |
| `a2` | Int64 | Second (secondary) assists credited |
| `penalties` | Int64 | Penalties taken (upstream `fo`: fo2 + fo5 + fo10 + gm + mp). Not faceoffs: see face_offs / fot. |
| `pim` | Int64 | Penalty minutes |
| `geq` | Int64 | Even-strength goals |
| `gpp` | Int64 | Power-play goals |
| `gpk` | Int64 | Shorthanded goals |
| `gwg` | Int64 | Game-winning goals |
| `ps` | Int64 | Shootout shots taken |
| `pg` | Int64 | Not determined; 0 on every 2026 row |
| `pl` | Int64 | Plus count: on ice for an even-strength or shorthanded goal for |
| `mi` | Int64 | Minus count: on ice for a goal against |
| `toi` | Int64 | Time on ice, seconds |
| `toi_eq` | Int64 | Even-strength time on ice, seconds |
| `toipp` | Int64 | Power-play time on ice, seconds (lower-case pp, unlike the box score's toiPp) |
| `toi_pk` | Int64 | Shorthanded time on ice, seconds |
| `sog` | Int64 | Number of shots on goal |
| `shm` | Int64 | Shots that missed the net |
| `shp` | Int64 | Shots that hit the post |
| `shb` | Int64 | Own shots blocked by an opponent |
| `bks` | Int64 | Opponent shots blocked |
| `fow` | Int64 | Number of faceoffs won |
| `fol` | Int64 | Number of faceoffs lost |
| `shi` | Int64 | Number of shifts played |
| `gwgk` | Int64 | Not determined; 0 on every 2026 row |
| `gw` | Int64 | Goaltender wins (incl. overtime and shootout) |
| `gl` | Int64 | Goaltender losses (incl. overtime and shootout) |
| `fk` | Int64 | Games started in goal (inferred: fk <= gp; 42 of 44 for the 2026 games-played leader) |
| `ga` | Int64 | Goals against: number of goals conceded |
| `sa` | Int64 | Shots on goal against |
| `svs` | Int64 | Number of saves made by the goaltender |
| `so` | Int64 | Number of shutouts (games without a goal conceded) |
| `mip` | Int64 | Goaltender time played, hundredths of a minute (6000 = 60:00; = seconds / 0.6) |
| `assists` | Int64 | Assists (a1 + a2; verified on every 2026 row) |
| `points` | Int64 | Points (g + assists; verified) |
| `goal_efficiency` | Float64 | Shooting percentage, 100 * g / sog (verified); -1 with no shots |
| `save_percentage` | Float64 | Save percentage, 0-100 |
| `face_offs_percentage` | Float64 | Faceoff win percentage, 0-100; -1 with no faceoffs |
| `face_offs` | Int64 | Faceoffs taken (fow + fol; verified) |
| `is_captain` | Boolean | Whether the player is the team captain |
| `first_goalkeeper` | Boolean | Team's first-choice goaltender |
| `ga_per_game` | Float64 | Goals against per game (ga / gp; verified) |
| `points_per_game` | Float64 | Points per game |
| `svs_per_game` | Float64 | Goaltender saves per game played |
| `bks_per_game` | Float64 | Blocked shots per game |
| `sog_per_game` | Float64 | Shots on goal per game |
| `shm_per_game` | Float64 | Missed shots per game |
| `shp_per_game` | Float64 | Post hits per game |
| `fow_per_game` | Float64 | Faceoffs won per game |
| `fol_per_game` | Float64 | Faceoffs lost per game |
| `toi_eq_per_game` | Float64 | Even-strength time on ice per game, seconds |
| `toi_pp_per_game` | Float64 | Power-play time on ice per game, seconds |
| `toi_pk_per_game` | Float64 | Shorthanded time on ice per game, seconds |
| `face_offs_per_game_percentage` | Float64 | Not determined; 0 on the sampled rows |
| `toi_average` | Float64 | Time on ice per game, seconds |
| `plus_minus` | Int64 | Plus/minus (pl - mi; verified) |
| `tgs` | Int64 | Team's total game time, seconds (about 3600 per game incl. overtime) |
| `sep` | Int64 | Goaltender time played, seconds |
| `toi_percentage` | Float64 | Share of the team's game time in goal, 100 * sep / tgs (verified) |
| `gwit` | Int64 | Wins in regulation time (gw = gwit + overtime/shootout wins; verified) |
| `glit` | Int64 | Losses in regulation time (gl = glit + overtime/shootout losses; verified) |
| `sag` | Int64 | Shots on goal against that were saved (= svs on every 2026 goaltender row) |
| `sago` | Int64 | Shots on goal against that went in (= ga on every 2026 goaltender row) |
| `sam` | Int64 | Shots against that missed the net |
| `sap` | Int64 | Shots against that hit the post |
| `sab` | Int64 | Shots against that were blocked |
| `saslot` | Int64 | Slot shots against |
| `sagoslot` | Int64 | Slot shots against that went in |
| `aeq` | Int64 | Even-strength assists |
| `app` | Int64 | Power-play assists |
| `apk` | Int64 | Shorthanded assists |
| `ha` | Int64 | Hat tricks (inferred; at most 2 in 2026) |
| `fo2` | Int64 | Minor (2-minute) penalties |
| `fo5` | Int64 | Major (5-minute) penalties |
| `fo10` | Int64 | 10-minute misconducts |
| `gm` | Int64 | Game misconducts |
| `mp` | Int64 | Match penalties |
| `pm` | Int64 | Not determined; 0 on every 2026 row |
| `pwg` | Int64 | Not determined; 0 on every 2026 row |
| `sgo` | Int64 | Not determined. Equals gff + gfd on every team row; skater values are close to but not always g |
| `fowoz` | Int64 | Offensive-zone faceoffs won |
| `fownz` | Int64 | Neutral-zone faceoffs won |
| `fowdz` | Int64 | Defensive-zone faceoffs won |
| `foloz` | Int64 | Offensive-zone faceoffs lost |
| `folnz` | Int64 | Neutral-zone faceoffs lost |
| `foldz` | Int64 | Defensive-zone faceoffs lost |
| `psg` | Int64 | Goals scored in shootouts |
| `pssvs` | Int64 | Saves made in shootouts |
| `got` | Int64 | Overtime goals (a subset of g) |
| `ot_rank` | Int64 | Overtime-scoring rank; 0 on the 2026 rows (filled in 2027) |
| `ot_g` | Int64 | Overtime goals in the overtime table; 0 on every 2026 row |
| `ot_a1` | Int64 | Overtime first assists; 0 on every 2026 row |
| `ot_a2` | Int64 | Overtime second assists; 0 on every 2026 row |
| `ot_toi` | Int64 | Overtime time on ice, seconds; 0 on every 2026 row |
| `ot_a` | Int64 | Overtime assists; 0 on every 2026 row |
| `ot_p` | Int64 | Overtime points; 0 on every 2026 row |
| `psgwg` | Int64 | Game-deciding shootout goals; 0 on every 2026 row |
| `sogslot` | Int64 | Shots on goal from the slot |
| `pswg` | Int64 | Shootout winners (inferred) |
| `toishi` | Float64 | Time on ice per shift, seconds |
| `shigp` | Float64 | Shifts per game |
| `saslotgp` | Float64 | Slot shots against per game |
| `samgp` | Float64 | Missed shots against per game |
| `sapgp` | Float64 | Post hits against per game |
| `svs_pslot` | Float64 | Slot save percentage, 100 * (saslot - sagoslot) / saslot |
| `sa_per_game` | Float64 | Shots on goal against per game |
| `otw` | Int64 | Goaltender overtime or shootout wins |
| `otl` | Int64 | Goaltender overtime or shootout losses |
| `fopoz` | Float64 | Offensive-zone faceoff win percentage, 0-100 |
| `fopnz` | Float64 | Neutral-zone faceoff win percentage, 0-100 |
| `fopdz` | Float64 | Defensive-zone faceoff win percentage, 0-100 |
| `fot` | Int64 | Faceoffs taken (fow + fol) |
| `fogp` | Float64 | Faceoffs taken per game |
| `psm` | Int64 | Shootout misses (inferred; ps = psg + psm fails for 67 of 447 skaters) |
| `ps_percentage` | Float64 | Shootout success percentage, 0-100 |
| `gw_shutout` | Int64 | Not determined; equals gw on goaltender rows |
| `sogslot_per_game` | Float64 | Slot shots on goal per game |
| `psag` | Int64 | Shootout goals against (equals ps on goaltender rows) |
| `pssvs_percentage` | Float64 | Shootout save percentage, 0-100; -1 with no shootout shots faced |
| `show_rank` | Boolean | App flag: show the row's rank |
| `chf` | Int64 | CHF credited for the player's points: 300 * points on every 2026 row (PostFinance youth-fund scheme; inferred) |
| `team_stats` | String | Per-team split for a player who changed teams; unobserved: null or empty in every sampled row |

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
| `rank` | Int64 | Position of the team in the standings table |
| `team_id` | Int64 | SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `name` | String | Team name as the league app displays it |
| `short_name` | String | Team abbreviation |
| `streak` | String | Last results, most recent first: W or L |
| `gp` | Int64 | Number of games played |
| `gw` | Int64 | Wins, all kinds |
| `gwot` | Int64 | Overtime + shootout wins (gwo + gwpe) |
| `gwpe` | Int64 | Number of wins decided in a shootout |
| `gl` | Int64 | Losses, all kinds |
| `glot` | Int64 | Overtime + shootout losses (glo + glpe) |
| `glpe` | Int64 | Shootout losses |
| `po` | Int64 | Standings points: 3 per regulation win, 2 per overtime/shootout win, 1 per overtime/shootout loss (verified) |
| `g` | Int64 | Number of goals scored |
| `geq` | Int64 | Even-strength goals (geq + gpp + gpk can fall short of g) |
| `gpp` | Int64 | Power-play goals |
| `gpk` | Int64 | Shorthanded goals |
| `ga` | Int64 | Goals against: number of goals conceded |
| `gaeq` | Int64 | Even-strength goals against |
| `gapp` | Int64 | Shorthanded goals allowed on the power play |
| `gapk` | Int64 | Power-play goals allowed while shorthanded |
| `go` | Int64 | Goal difference, g - ga (verified) |
| `ppo` | Int64 | Power-play opportunities |
| `pks` | Int64 | Times shorthanded |
| `sog` | Int64 | Number of shots on goal |
| `penalties` | Int64 | Penalties taken (upstream `fo`: fo2 + fo5 + fo10 + gm + mp). Not faceoffs: see face_offs / fot. |
| `pim` | Int64 | Penalty minutes |
| `fow` | Int64 | Number of faceoffs won |
| `fol` | Int64 | Number of faceoffs lost |
| `shm` | Int64 | Shots that missed the net |
| `shp` | Int64 | Shots that hit the post |
| `shb` | Int64 | Own shots blocked by an opponent |
| `bks` | Int64 | Opponent shots blocked |
| `sa` | Int64 | Shots on goal against |
| `sam` | Int64 | Shots against that missed the net; 0 on every 2026 team row |
| `sap` | Int64 | Shots against that hit the post; 0 on every 2026 team row |
| `sab` | Int64 | Shots against that were blocked; 0 on every 2026 team row |
| `sort_order` | Int64 | App sort key; not the rank |
| `website` | String | Club website URL |
| `position_change` | String | Rank movement since the last round ('none' on every 2026 row) |
| `is_live` | Boolean | Team is playing now |
| `current_result` | Int64 | Team's goals in a live game; 0 otherwise |
| `current_opponent_result` | Int64 | Opponent's goals in a live game; 0 otherwise |
| `pos` | Int64 | Standings position (= rank on every 2026 row) |
| `hg` | Int64 | Home games played (gp / 2 on every 2026 row) |
| `gsow` | Int64 | Shootout wins; duplicate of gwpe (equal on every 2026 row) |
| `gfd` | Int64 | Goals by defenders |
| `gff` | Int64 | Goals by forwards |
| `gfi` | Int64 | Goals by import (non-Swiss-licence) players (inferred; cf. the sihf/ teamGoalPosNation table) |
| `gasow` | Int64 | Shootout losses; duplicate of glpe (equal on every 2026 row) |
| `pp` | Int64 | Power-play time, seconds |
| `bp` | Int64 | Shorthanded (box-play) time, seconds |
| `fo2` | Int64 | Minor (2-minute) penalties |
| `fo5` | Int64 | Major (5-minute) penalties |
| `fo10` | Int64 | 10-minute misconducts |
| `gm` | Int64 | Game misconducts |
| `mp` | Int64 | Match penalties |
| `ps` | Int64 | Shootout attempts taken |
| `psg` | Int64 | Goals scored in shootouts |
| `psm` | Int64 | Shootout misses (ps = psg + psm on every 2026 team row) |
| `psa` | Int64 | Shootout shots faced (psga + pssvs) |
| `psga` | Int64 | Shootout goals against |
| `pssvs` | Int64 | Saves made in shootouts |
| `spec` | Int64 | Total home attendance |
| `cap` | Int64 | Total home arena capacity over the home games |
| `fowoz` | Int64 | Offensive-zone faceoffs won |
| `fownz` | Int64 | Neutral-zone faceoffs won |
| `fowdz` | Int64 | Defensive-zone faceoffs won |
| `foloz` | Int64 | Offensive-zone faceoffs lost |
| `folnz` | Int64 | Neutral-zone faceoffs lost |
| `foldz` | Int64 | Defensive-zone faceoffs lost |
| `sgo` | Int64 | Not determined. Equals gff + gfd on every team row; skater values are close to but not always g |
| `sslot` | Int64 | Shots from the slot |
| `sag` | Int64 | Shots on goal against that were saved; 0 on every 2026 team row (use sa) |
| `sago` | Int64 | Shots on goal against that went in; 0 on every 2026 team row (use ga) |
| `saslot` | Int64 | Slot shots against |
| `qualification_status` | String | Clinch marker ('none' on every 2026 row) |
| `is_qualified_chl` | Boolean | Qualified for the Champions Hockey League |
| `goal_efficiency` | Float64 | Shooting percentage, 100 * g / sog (verified); -1 with no shots |
| `ppg_efficiency` | Float64 | Power-play percentage, 100 * gpp / ppo (verified) |
| `ppggp` | Float64 | Power-play goals per game |
| `ppopgp` | Float64 | Power-play opportunities per game |
| `ppgp` | Float64 | Power-play time per game, seconds |
| `gpg` | Float64 | Goals scored per game played |
| `gagp` | Float64 | Goals against per game |
| `gappgp` | Float64 | Shorthanded goals against per game |
| `geqgp` | Float64 | Even-strength goals per game |
| `gaeqgp` | Float64 | Even-strength goals against per game |
| `bks_per_game` | Float64 | Blocked shots per game |
| `pim_per_game` | Float64 | Penalty minutes per game |
| `ps_percentage` | Float64 | Shootout success percentage, 0-100 |
| `attendance` | Int64 | Average home attendance (spec / hg) |
| `capacity` | Int64 | Arena capacity (cap / hg) |
| `utilization` | Float64 | Capacity utilisation, 100 * spec / cap |
| `soggp` | Float64 | Shots on goal per game |
| `sslotgp` | Float64 | Slot shots per game |
| `shmgp` | Float64 | Missed shots per game |
| `shpgp` | Float64 | Post hits per game |
| `gpkgp` | Float64 | Shorthanded goals per game |
| `pk_efficiency` | Float64 | Penalty-kill percentage, 100 * (1 - gapk / pks) (verified) |
| `pksgp` | Float64 | Times shorthanded per game |
| `gapkgp` | Float64 | Power-play goals against per game |
| `bpgp` | Float64 | Shorthanded time per game, seconds |
| `fot` | Int64 | Faceoffs taken (fow + fol) |
| `face_offs_percentage` | Float64 | Faceoff win percentage, 0-100; -1 with no faceoffs |
| `fogp` | Float64 | Faceoffs taken per game |
| `fowgp` | Float64 | Faceoffs won per game |
| `folgp` | Float64 | Faceoffs lost per game |
| `fopoz` | Float64 | Offensive-zone faceoff win percentage, 0-100 |
| `fopnz` | Float64 | Neutral-zone faceoff win percentage, 0-100 |
| `fopdz` | Float64 | Defensive-zone faceoff win percentage, 0-100 |
| `sagp` | Float64 | Shots on goal against per game |
| `saslotgp` | Float64 | Slot shots against per game |
| `gffd` | Int64 | Goals by forwards and defenders (gff + gfd; verified) |
| `gffp` | Float64 | Share of goals by forwards, 100 * gff / g |
| `gfdp` | Float64 | Share of goals by defenders, 100 * gfd / g |
| `gfip` | Float64 | Share of goals by import players, 100 * gfi / g |
| `pssvs_percentage` | Float64 | Shootout save percentage, 0-100; -1 with no shootout shots faced |
| `pgp` | Float64 | Standings points per game |
| `gwit` | Int64 | Wins in regulation time (gw = gwit + overtime/shootout wins; verified) |
| `gwo` | Int64 | Number of wins in overtime |
| `glit` | Int64 | Losses in regulation time (gl = glit + overtime/shootout losses; verified) |
| `glo` | Int64 | Overtime losses |

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
