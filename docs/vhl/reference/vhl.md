# VHL — VHL (vhlru.ru server-rendered pages, parsed)

> VHL — VHL (vhlru.ru server-rendered pages, parsed) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.hockey.vhl` — 13 endpoints.

> **Note:** Owner flag: the VHL is run by Russian league operators, and several clubs are owned or sponsored by sanctioned state companies. Fetching on your own machine is fine; a published data release or a hosted mirror needs the owner's sanctions / legal sign-off first.

## vhl_schedule

Schedule and results of one season stage: every game with its club ids, score and decision.

**Endpoint URL:** `GET https://www.vhlru.ru/en/calendar/{season}/0/`

**Valid URL:** [https://www.vhlru.ru/en/calendar/2026/0/?stage=regular](https://www.vhlru.ru/en/calendar/2026/0/?stage=regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025/26; the calendar covers 2010/11 on). The runtime maps it, with ``stage``, to the site's tournament id (tn) in this path segment. |
| `stage` | `stage` |  |  | `Y` | Season stage: regular (default) or playoff. |

### Returns {#vhl_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025/26). |
| `stage` | String | Season stage: regular or playoff. |
| `tn` | Int64 | Tournament id of the season stage (2025/26 regular season = 1374, playoffs = 1375); vhl_tn maps season + stage to it. |
| `game_id` | Int64 | Game id (the calendar's game_id; the report's and the broadcast's idgame). The report and broadcast pages do not print it: it comes from the request. |
| `game_number` | Int64 | Official game number within the tournament (from '№986'). |
| `game_date` | String | Game date, ISO 'YYYY-MM-DD': the calendar day heading plus the season (August-December = START year). |
| `date` | String | Day header in English, day and month only ('18 March'); the page prints no year and no start time. |
| `weekday` | String | Weekday of the day header (English). |
| `home_team` | String | Home team name (Latin, FULL form: 'Kristall S'; standings, stats and the team page use the short 'Kristall'). |
| `home_club_id` | Int64 | Home club id: stable across seasons (the img.khl.ru logo path id). Neither a team-season id nor a team page id. |
| `away_team` | String | Away team name (Latin, FULL form, like home_team). |
| `away_club_id` | Int64 | Away club id: stable across seasons (the img.khl.ru logo path id). |
| `city` | String | City of the home team's arena (English). |
| `score` | String | Final score 'home:away'; '-:-' before the game. |
| `home_score` | Int64 | Home goals including the shootout winner goal; null before the game. |
| `away_score` | Int64 | Away goals including the shootout winner goal; null before the game. |
| `decision` | String | How the game ended beyond regulation: OT (overtime) or SO (shootout); null for regulation or unplayed games. |
| `winner` | String | Winning side from the win-home / win-visitor class: home or away; null before the game. |
| `status` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `online_url` | String | Text-broadcast URL (online.vhlru.ru/online/{game_id}.html). |

**`return_parsed=False`** — the page's parsed record (`dict`), plus `_request` (season, stage, tn and the ids asked for).

### Raises {#vhl_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The VHL has nothing for the request (a season without that stage, a game not in the season's calendar, an unknown game, player or team page). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The page could not be fetched, or it is not the page asked for (another season, game or player; a missing table or header drift). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#vhl_schedule-example}

```python
vhl_schedule(season=2026)
```

## vhl_game_info

Game header of a report: date, attendance, score by period, teams, coaches and officials.

**Endpoint URL:** `GET https://www.vhlru.ru/en/report/{season}/`

**Valid URL:** [https://www.vhlru.ru/en/report/2026/?idgame=899223&stage=regular](https://www.vhlru.ru/en/report/2026/?idgame=899223&stage=regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025/26; the calendar covers 2010/11 on). The runtime maps it, with ``stage``, to the site's tournament id (tn) in this path segment. |
| `idgame` | `game_id` |  | `Y` |  | Required. Game id (game_id in vhl_schedule; the report's and the broadcast's idgame), e.g. 899223. |
| `stage` | `stage` |  |  | `Y` | Season stage: regular (default) or playoff. |

### Returns {#vhl_game_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game id (the calendar's game_id; the report's and the broadcast's idgame). The report and broadcast pages do not print it: it comes from the request. |
| `season` | Int64 | Season END year (2026 = 2025/26). |
| `stage` | String | Season stage: regular or playoff. |
| `game_number` | Int64 | Official game number (from '№984'); checked against the request. |
| `tn` | Int64 | Tournament id of the season stage (2025/26 regular season = 1374, playoffs = 1375); vhl_tn maps season + stage to it. |
| `date` | String | Game date, ISO 'YYYY-MM-DD', parsed from date_text. |
| `local_time` | String | Start time 'HH:MM' from date_text; local time zone unverified (likely Moscow). |
| `date_text` | String | Date line verbatim; RUSSIAN month and weekday even on /en/ ('18 Март 2026, Среда, 17:00'). The <time datetime> attribute is a constant placeholder (2020-01-23 18:00) and is ignored. |
| `attendance` | Int64 | Attendance (from '910 sp'). |
| `score` | String | Final score 'home:away' (the shootout counts as one goal). |
| `home_score` | Int64 | Home final goals. |
| `away_score` | Int64 | Away final goals. |
| `decision` | String | OT or SO when the game went past regulation; null otherwise. |
| `period_scores` | String | Score of each period 'home:away', overtime and shootout included (SO game: '0:2','0:0','2:0','0:0','0:1'). |
| `home_team_page_id` | Int64 | Home team page id. Team page id (vhlru.ru only): the {team_id} of /en/teams/{team_id}/ (Toros = 390). Neither a club id nor a team-season id; it joins nothing else (bridge: the calendar row of the same game_id). |
| `home_team` | String | Home team name (Latin, FULL form: 'Kristall S'; the report's tables use the short 'Kristall'). |
| `home_short` | String | Home team short name (the report tables' form). |
| `home_city` | String | Home team city. |
| `home_coach` | String | Home head coach, 'Surname Given'. |
| `away_team_page_id` | Int64 | Away team page id. Team page id (vhlru.ru only): the {team_id} of /en/teams/{team_id}/ (Toros = 390). Neither a club id nor a team-season id; it joins nothing else (bridge: the calendar row of the same game_id). |
| `away_team` | String | Away team name (Latin, FULL form). |
| `away_short` | String | Away team short name (the report tables' form). |
| `away_city` | String | Away team city. |
| `away_coach` | String | Away head coach, 'Surname Given'. |
| `referee1_number` | Int64 | First referee's sweater number. |
| `referee1` | String | First referee, 'Surname Given'. |
| `referee2_number` | Int64 | Second referee's sweater number. |
| `referee2` | String | Second referee, 'Surname Given'. |
| `linesman1_number` | Int64 | First linesman's sweater number. |
| `linesman1` | String | First linesman, 'Surname Given'. |
| `linesman2_number` | Int64 | Second linesman's sweater number. |
| `linesman2` | String | Second linesman, 'Surname Given'. |

**`return_parsed=False`** — the page's parsed record (`dict`), plus `_request` (season, stage, tn and the ids asked for).

### Raises {#vhl_game_info-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The VHL has nothing for the request (a season without that stage, a game not in the season's calendar, an unknown game, player or team page). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The page could not be fetched, or it is not the page asked for (another season, game or player; a missing table or header drift). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#vhl_game_info-example}

```python
vhl_game_info(game_id=899223, season=2026)
```

## vhl_goals

Goals of a game with scorer, assists, strength and the jerseys on ice for both teams.

**Endpoint URL:** `GET https://www.vhlru.ru/en/report/{season}/`

**Valid URL:** [https://www.vhlru.ru/en/report/2026/?idgame=899223&stage=regular](https://www.vhlru.ru/en/report/2026/?idgame=899223&stage=regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025/26; the calendar covers 2010/11 on). The runtime maps it, with ``stage``, to the site's tournament id (tn) in this path segment. |
| `idgame` | `game_id` |  | `Y` |  | Required. Game id (game_id in vhl_schedule; the report's and the broadcast's idgame), e.g. 899223. |
| `stage` | `stage` |  |  | `Y` | Season stage: regular (default) or playoff. |

### Returns {#vhl_goals-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game id (the calendar's game_id; the report's and the broadcast's idgame). The report and broadcast pages do not print it: it comes from the request. |
| `season` | Int64 | Season END year (2026 = 2025/26). |
| `stage` | String | Season stage: regular or playoff. |
| `game_number` | Int64 | Official game number (from '№984'); checked against the request. |
| `team_side` | String | Side of the game the row belongs to: home or away. |
| `team_page_id` | Int64 | Team page id (vhlru.ru /en/teams/{team_page_id}/), stable across seasons; for report rows the page id of the row's side. |
| `goal_number` | Int64 | Goal number in the game (G). |
| `period` | String | Period (Per) as printed: '1', '2', '3', overtime unverified, 'РБ' (Cyrillic even on /en/: shootout, решающий бросок). |
| `time` | String | Elapsed game time 'mm:ss' from the opening faceoff (Time); the shootout row says '65:00'. |
| `score` | String | Score after the goal 'home:away' (Score). |
| `strength` | String | Goal strength code (PMG): eq (even), ppg (power play), shg (shorthanded); null on the shootout row. |
| `scorer_id` | Int64 | Player id of the scorer: the {player_id} of /en/players/{player_id}/, stable across seasons and teams; also the photo id on img.khl.ru. Same numeric sequence as khl.ru player ids; whether it equals the same person's khl.ru id is unverified. |
| `scorer` | String | Goal scorer's name, 'Surname Given' as the report prints it. |
| `scorer_jersey` | Int64 | Scorer jersey number. |
| `scorer_season_total` | Int64 | The scorer's season goal count after this goal (the '(13)' suffix); null on the shootout row. |
| `assist1_id` | Int64 | Player id of the first assist; null when unassisted. |
| `assist1` | String | First assist name; null when unassisted. |
| `assist1_jersey` | Int64 | First assist jersey number. |
| `assist1_season_total` | Int64 | The first assist's season assist count after this goal (the '(14)' suffix). |
| `assist2_id` | Int64 | Player id of the second assist; null when none. |
| `assist2` | String | Second assist name; null when none. |
| `assist2_jersey` | Int64 | Second assist jersey number. |
| `assist2_season_total` | Int64 | The second assist's season assist count after this goal. |
| `home_on_ice` | String | Jersey numbers of the home players on ice at the goal (goalie included); the column header is the home team's short name. Shootout row: the goalie only. |
| `away_on_ice` | String | Jersey numbers of the away players on ice at the goal (goalie included); the column header is the away team's short name. Empty on the shootout row. |

**`return_parsed=False`** — the page's parsed record (`dict`), plus `_request` (season, stage, tn and the ids asked for).

### Raises {#vhl_goals-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The VHL has nothing for the request (a season without that stage, a game not in the season's calendar, an unknown game, player or team page). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The page could not be fetched, or it is not the page asked for (another season, game or player; a missing table or header drift). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#vhl_goals-example}

```python
vhl_goals(game_id=899223, season=2026)
```

## vhl_penalties

Penalties of a game: team, time, player, minutes and infraction.

**Endpoint URL:** `GET https://www.vhlru.ru/en/report/{season}/`

**Valid URL:** [https://www.vhlru.ru/en/report/2026/?idgame=899223&stage=regular](https://www.vhlru.ru/en/report/2026/?idgame=899223&stage=regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025/26; the calendar covers 2010/11 on). The runtime maps it, with ``stage``, to the site's tournament id (tn) in this path segment. |
| `idgame` | `game_id` |  | `Y` |  | Required. Game id (game_id in vhl_schedule; the report's and the broadcast's idgame), e.g. 899223. |
| `stage` | `stage` |  |  | `Y` | Season stage: regular (default) or playoff. |

### Returns {#vhl_penalties-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game id (the calendar's game_id; the report's and the broadcast's idgame). The report and broadcast pages do not print it: it comes from the request. |
| `season` | Int64 | Season END year (2026 = 2025/26). |
| `stage` | String | Season stage: regular or playoff. |
| `game_number` | Int64 | Official game number (from '№984'); checked against the request. |
| `team_side` | String | Side of the game the row belongs to: home or away. |
| `team_page_id` | Int64 | Team page id (vhlru.ru /en/teams/{team_page_id}/), stable across seasons; for report rows the page id of the row's side. |
| `team` | String | Team short name (Latin; the report tables' form). |
| `time` | String | Elapsed game time of the penalty 'mm:ss'. |
| `player_id` | Int64 | Player id of the penalised player; null for a bench (team) penalty. |
| `player` | String | Penalised player name; null for a bench (team) penalty. |
| `player_jersey` | Int64 | Penalised player's jersey number. |
| `minutes` | Int64 | Penalty length in minutes (the unlabelled third column). |
| `infraction` | String | Infraction in English ('hooking'); null when the site leaves it blank. |

**`return_parsed=False`** — the page's parsed record (`dict`), plus `_request` (season, stage, tn and the ids asked for).

### Raises {#vhl_penalties-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The VHL has nothing for the request (a season without that stage, a game not in the season's calendar, an unknown game, player or team page). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The page could not be fetched, or it is not the page asked for (another season, game or player; a missing table or header drift). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#vhl_penalties-example}

```python
vhl_penalties(game_id=899223, season=2026)
```

## vhl_player_box

Skater box score of a game: scoring, shots, faceoffs, time on ice, hits and blocks.

**Endpoint URL:** `GET https://www.vhlru.ru/en/report/{season}/`

**Valid URL:** [https://www.vhlru.ru/en/report/2026/?idgame=899223&stage=regular](https://www.vhlru.ru/en/report/2026/?idgame=899223&stage=regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025/26; the calendar covers 2010/11 on). The runtime maps it, with ``stage``, to the site's tournament id (tn) in this path segment. |
| `idgame` | `game_id` |  | `Y` |  | Required. Game id (game_id in vhl_schedule; the report's and the broadcast's idgame), e.g. 899223. |
| `stage` | `stage` |  |  | `Y` | Season stage: regular (default) or playoff. |

### Returns {#vhl_player_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game id (the calendar's game_id; the report's and the broadcast's idgame). The report and broadcast pages do not print it: it comes from the request. |
| `season` | Int64 | Season END year (2026 = 2025/26). |
| `stage` | String | Season stage: regular or playoff. |
| `game_number` | Int64 | Official game number (from '№984'); checked against the request. |
| `team_side` | String | Side of the game the row belongs to: home or away. |
| `team_page_id` | Int64 | Team page id (vhlru.ru /en/teams/{team_page_id}/), stable across seasons; for report rows the page id of the row's side. |
| `team` | String | Team short name (Latin; the report tables' form). |
| `position_group` | String | Position group: defense or forward. |
| `jersey` | Int64 | Jersey number (integer). |
| `player_id` | Int64 | Player id: the {player_id} of /en/players/{player_id}/, stable across seasons and teams; also the photo id on img.khl.ru. Same numeric sequence as khl.ru player ids; whether it equals the same person's khl.ru id is unverified. |
| `player` | String | Player name, 'Surname Given' (Latin on /en/ pages). |
| `captaincy` | String | Captaincy marker from the name suffix: C (captain), A (alternate); null otherwise. |
| `games_played` | Int64 | Games played (GP). |
| `goals` | Int64 | Goals (G) the player scored in the row's scope: the game in a box score, the season stage in stats and career rows. |
| `assists` | Int64 | Assists (A) credited to the player in the row's scope: the game in a box score, the season stage in stats and career rows. |
| `points` | Int64 | Points, goals + assists (PTS). |
| `plus_minus` | Int64 | Plus/minus (+/-). |
| `penalty_minutes` | Int64 | Penalty minutes (PIM). |
| `even_strength_goals` | Int64 | Even-strength goals (ESG). |
| `power_play_goals` | Int64 | Power-play goals (PPG). |
| `shorthanded_goals` | Int64 | Shorthanded goals (SHG). |
| `overtime_goals` | Int64 | Overtime goals (OTG). |
| `game_winning_goals` | Int64 | Game-winning goals (GWG). |
| `shootout_deciding_goals` | Int64 | Shootout-deciding goals (SDS; the legend says 'Shootouts deciding shots'). |
| `shots_on_goal` | Int64 | Shots on goal (SOG). |
| `shot_pct` | Float64 | Shooting percentage, goals / shots on goal x 100 (%SOG); 0-100 scale; null when the page shows '-'. |
| `faceoffs` | Int64 | Faceoffs taken (FO). |
| `faceoffs_won` | Int64 | Faceoffs won (FOW). |
| `faceoff_pct` | Float64 | Faceoff win percentage (%FO); 0-100 scale; null when the page shows '-' (no faceoffs taken). |
| `toi_per_game` | String | Time on ice in this game (TOI/G with GP 1), 'mm:ss'. |
| `shifts_per_game` | Float64 | Shifts in this game (Sft/G with GP 1). |
| `hits` | Int64 | Body checks delivered (Hits). |
| `blocked_shots` | Int64 | Blocked shots (BLS). |

**`return_parsed=False`** — the page's parsed record (`dict`), plus `_request` (season, stage, tn and the ids asked for).

### Raises {#vhl_player_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The VHL has nothing for the request (a season without that stage, a game not in the season's calendar, an unknown game, player or team page). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The page could not be fetched, or it is not the page asked for (another season, game or player; a missing table or header drift). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#vhl_player_box-example}

```python
vhl_player_box(game_id=899223, season=2026)
```

## vhl_goalie_box

Goalie box score of a game: shots against, saves, save percentage and time on ice.

**Endpoint URL:** `GET https://www.vhlru.ru/en/report/{season}/`

**Valid URL:** [https://www.vhlru.ru/en/report/2026/?idgame=899223&stage=regular](https://www.vhlru.ru/en/report/2026/?idgame=899223&stage=regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025/26; the calendar covers 2010/11 on). The runtime maps it, with ``stage``, to the site's tournament id (tn) in this path segment. |
| `idgame` | `game_id` |  | `Y` |  | Required. Game id (game_id in vhl_schedule; the report's and the broadcast's idgame), e.g. 899223. |
| `stage` | `stage` |  |  | `Y` | Season stage: regular (default) or playoff. |

### Returns {#vhl_goalie_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game id (the calendar's game_id; the report's and the broadcast's idgame). The report and broadcast pages do not print it: it comes from the request. |
| `season` | Int64 | Season END year (2026 = 2025/26). |
| `stage` | String | Season stage: regular or playoff. |
| `game_number` | Int64 | Official game number (from '№984'); checked against the request. |
| `team_side` | String | Side of the game the row belongs to: home or away. |
| `team_page_id` | Int64 | Team page id (vhlru.ru /en/teams/{team_page_id}/), stable across seasons; for report rows the page id of the row's side. |
| `team` | String | Team short name (Latin; the report tables' form). |
| `jersey` | Int64 | Jersey number (integer). |
| `player_id` | Int64 | Player id: the {player_id} of /en/players/{player_id}/, stable across seasons and teams; also the photo id on img.khl.ru. Same numeric sequence as khl.ru player ids; whether it equals the same person's khl.ru id is unverified. |
| `player` | String | Player name, 'Surname Given' (Latin on /en/ pages). |
| `captaincy` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `games_played` | Int64 | Games played (GP). |
| `wins` | Int64 | Goalie wins (W). |
| `losses` | Int64 | Goalie losses (L). |
| `shootouts_played` | Int64 | Shootouts played (SOP). |
| `shots_against` | Int64 | Shots on goal faced (the SOG column of goalie tables). |
| `goals_against` | Int64 | Goals allowed (GA). |
| `saves` | Int64 | Saves (Sv): shots on goal the goalie stopped in the row's scope (the game, or the season stage in stats and career rows). |
| `save_pct` | Float64 | Save percentage (%Sv); 0-100 scale (93.8 = 93.8 %); null when the page shows '-'. |
| `gaa` | Float64 | Goals-against average (GAA) = 60 x GA / TOI minutes; null when the page shows '-'. |
| `assists` | Int64 | Assists (A) credited to the player in the row's scope: the game in a box score, the season stage in stats and career rows. |
| `shutouts` | Int64 | Shutouts (SO): games in the row's scope in which the goalie allowed no goal. |
| `penalty_minutes` | Int64 | Penalty minutes (PIM). |
| `toi` | String | Time on ice in this game, 'mm:ss'; null for a dressed goalie who did not play ('-'). |

**`return_parsed=False`** — the page's parsed record (`dict`), plus `_request` (season, stage, tn and the ids asked for).

### Raises {#vhl_goalie_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The VHL has nothing for the request (a season without that stage, a game not in the season's calendar, an unknown game, player or team page). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The page could not be fetched, or it is not the page asked for (another season, game or player; a missing table or header drift). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#vhl_goalie_box-example}

```python
vhl_goalie_box(game_id=899223, season=2026)
```

## vhl_pbp

Text broadcast of a game (online.vhlru.ru, Russian text): goals, penalties, shootout attempts, goalie changes and comments. No shot coordinates.

**Endpoint URL:** `GET https://online.vhlru.ru/online/{game_id}.html`

**Valid URL:** [https://online.vhlru.ru/online/899223.html?season=2026&stage=regular](https://online.vhlru.ru/online/899223.html?season=2026&stage=regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Game id (game_id in vhl_schedule; the report's and the broadcast's idgame), e.g. 899223. |
| `season` | `season` |  | `Y` |  | Required. Season END year of the game; with ``stage`` it locates the game in the calendar, whose game number the broadcast page must carry (the page names no game id). |
| `stage` | `stage` |  |  | `Y` | Season stage: regular (default) or playoff. |

### Returns {#vhl_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game id (the calendar's game_id; the report's and the broadcast's idgame). The report and broadcast pages do not print it: it comes from the request. |
| `season` | Int64 | Season END year (2026 = 2025/26). |
| `stage` | String | Season stage: regular or playoff. |
| `game_number` | Int64 | Official game number ('Игра номер 984'); checked against the request. |
| `event_index` | Int64 | Order of the event on the page, 0 = the first listed (the broadcast lists the latest event first). |
| `kind` | String | Event kind from the CSS modifier: game_state (ga), comment (co), goal (go), penalty (pn), goalie_change (gc), video_review (vr), shootout_attempt (bu). |
| `kind_code` | String | Raw CSS modifier of .game-event-item--<code>. |
| `time` | String | Time label verbatim: 'mm:ss' elapsed game time, 'NN мин' (minute of a comment), or 'HH:MM' wall clock on game_state events; null when absent. See clock. |
| `clock` | String | What time means: game (elapsed mm:ss), minute ('58 мин'), wall (local wall clock, game_state rows); null when there is no time. |
| `score` | String | Score 'home-away' (dash, not colon) after a goal; on a shootout attempt, the shootout score printed beside it (before or after that attempt: unverified). |
| `strength_code` | String | Goal strength code in Cyrillic: РАВ (even), БОЛ (power play), МЕН (shorthanded, unobserved), БУЛ (penalty shot / shootout); empty string seen as null. |
| `strength` | String | strength_code in English: even, power_play, shorthanded, penalty_shot_or_shootout. |
| `text` | String | Event text verbatim in RUSSIAN (team, players, infraction, period stats); not translated. |
| `player_ids` | String | Player ids linked in the event text (//vhlru.ru/players/{id}/), scorer first; empty for most kinds. |
| `home_team` | String | Home team name in Cyrillic. |
| `away_team` | String | Away team name in Cyrillic. |

**`return_parsed=False`** — the page's parsed record (`dict`), plus `_request` (season, stage, tn and the ids asked for).

### Raises {#vhl_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The VHL has nothing for the request (a season without that stage, a game not in the season's calendar, an unknown game, player or team page). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The page could not be fetched, or it is not the page asked for (another season, game or player; a missing table or header drift). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#vhl_pbp-example}

```python
vhl_pbp(game_id=899223, season=2026)
```

## vhl_standings

League standings of one regular season (/standings/league/; /standings/regular/ ignores the season).

**Endpoint URL:** `GET https://www.vhlru.ru/en/standings/league/{season}/`

**Valid URL:** [https://www.vhlru.ru/en/standings/league/2026/](https://www.vhlru.ru/en/standings/league/2026/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025/26; the calendar covers 2010/11 on). The runtime maps it, with ``stage``, to the site's tournament id (tn) in this path segment. |

### Returns {#vhl_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025/26). |
| `stage` | String | Season stage: regular or playoff. |
| `tn` | Int64 | Tournament id of the season stage (2025/26 regular season = 1374, playoffs = 1375); vhl_tn maps season + stage to it. |
| `rank` | Int64 | Rank (unlabelled first column). |
| `team` | String | Team short name (Latin; the report tables' form). |
| `team_season_id` | Int64 | Team-season id (khl/ terminology): one id per team per tournament, from the KHL's sequence (khl/: Spartak = 14934 in 1369; here Metallurg = 15017 in 1374). The {id} of /calendar/{tn}/0/{id}/ and /stats/players/{tn}/all/{id}/. Not the stable club id (*_club_id) and not the team page id. |
| `games_played` | Int64 | Games played (GP). |
| `wins` | Int64 | Regulation wins (W). |
| `ot_wins` | Int64 | Overtime wins (OTW). |
| `so_wins` | Int64 | Shootout wins (SOW). |
| `so_losses` | Int64 | Shootout losses (SOL). |
| `ot_losses` | Int64 | Overtime losses (OTL). |
| `losses` | Int64 | Regulation losses (L). |
| `goals_for` | Int64 | Goals for (left side of the G column '226-131'). |
| `goals_against` | Int64 | Goals against (right side of the G column). |
| `points` | Int64 | Points (PTS): 2 per win of any kind, 1 per OT or SO loss. |

**`return_parsed=False`** — the page's parsed record (`dict`), plus `_request` (season, stage, tn and the ids asked for).

### Raises {#vhl_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The VHL has nothing for the request (a season without that stage, a game not in the season's calendar, an unknown game, player or team page). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The page could not be fetched, or it is not the page asked for (another season, game or player; a missing table or header drift). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#vhl_standings-example}

```python
vhl_standings(season=2026)
```

## vhl_skater_stats

Season stats of every defenseman and forward of one season stage.

**Endpoint URL:** `GET https://www.vhlru.ru/en/stats/players/{season}/`

**Valid URL:** [https://www.vhlru.ru/en/stats/players/2026/?stage=regular](https://www.vhlru.ru/en/stats/players/2026/?stage=regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025/26; the calendar covers 2010/11 on). The runtime maps it, with ``stage``, to the site's tournament id (tn) in this path segment. |
| `stage` | `stage` |  |  | `Y` | Season stage: regular (default) or playoff. |

### Returns {#vhl_skater_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025/26). |
| `stage` | String | Season stage: regular or playoff. |
| `tn` | Int64 | Tournament id of the season stage (2025/26 regular season = 1374, playoffs = 1375); vhl_tn maps season + stage to it. |
| `position_group` | String | Position group: defense or forward. |
| `player_id` | Int64 | Player id: the {player_id} of /en/players/{player_id}/, stable across seasons and teams; also the photo id on img.khl.ru. Same numeric sequence as khl.ru player ids; whether it equals the same person's khl.ru id is unverified. |
| `player` | String | Player name, 'Surname Given' (Latin on /en/ pages). |
| `jersey` | Int64 | Jersey number (#; the page legend mislabels this column 'Position'). |
| `team` | String | Team short name (Latin; the report tables' form). |
| `games_played` | Int64 | Games played (GP). |
| `goals` | Int64 | Goals (G) the player scored in the row's scope: the game in a box score, the season stage in stats and career rows. |
| `assists` | Int64 | Assists (A) credited to the player in the row's scope: the game in a box score, the season stage in stats and career rows. |
| `points` | Int64 | Points, goals + assists (PTS). |
| `plus_minus` | Int64 | Plus/minus (+/-). |
| `plus` | Int64 | Plus: on-ice goals for that count toward plus/minus (+). |
| `minus` | Int64 | Minus: on-ice goals against that count toward plus/minus (-). |
| `penalty_minutes` | Int64 | Penalty minutes (PIM). |
| `even_strength_goals` | Int64 | Even-strength goals (ESG). |
| `power_play_goals` | Int64 | Power-play goals (PPG). |
| `shorthanded_goals` | Int64 | Shorthanded goals (SHG). |
| `overtime_goals` | Int64 | Overtime goals (OTG). |
| `game_winning_goals` | Int64 | Game-winning goals (GWG). |
| `shootout_deciding_goals` | Int64 | Shootout-deciding goals (SDS; the legend says 'Shootouts deciding shots'). |
| `shots_on_goal` | Int64 | Shots on goal (SOG). |
| `shot_pct` | Float64 | Shooting percentage, goals / shots on goal x 100 (%SOG); 0-100 scale; null when the page shows '-'. |
| `shots_per_game` | Float64 | Shots on goal per game (S/G). |
| `faceoffs` | Int64 | Faceoffs taken (FO). |
| `faceoffs_won` | Int64 | Faceoffs won (FOW). |
| `faceoff_pct` | Float64 | Faceoff win percentage (%FO); 0-100 scale; null when the page shows '-' (no faceoffs taken). |
| `toi_per_game` | String | Average time on ice per game (TOI/G), 'm:ss' / 'mm:ss' string; null when the page shows '-'. |
| `shifts_per_game` | Float64 | Average shifts per game (Sft/G); integer in a one-game report, decimal in season tables. |
| `hits` | Int64 | Body checks delivered (Hits). |
| `blocked_shots` | Int64 | Blocked shots (BLS). |

**`return_parsed=False`** — the page's parsed record (`dict`), plus `_request` (season, stage, tn and the ids asked for).

### Raises {#vhl_skater_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The VHL has nothing for the request (a season without that stage, a game not in the season's calendar, an unknown game, player or team page). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The page could not be fetched, or it is not the page asked for (another season, game or player; a missing table or header drift). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#vhl_skater_stats-example}

```python
vhl_skater_stats(season=2026)
```

## vhl_goalie_stats

Season stats of every goalie of one season stage.

**Endpoint URL:** `GET https://www.vhlru.ru/en/stats/players/{season}/`

**Valid URL:** [https://www.vhlru.ru/en/stats/players/2026/?stage=regular](https://www.vhlru.ru/en/stats/players/2026/?stage=regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025/26; the calendar covers 2010/11 on). The runtime maps it, with ``stage``, to the site's tournament id (tn) in this path segment. |
| `stage` | `stage` |  |  | `Y` | Season stage: regular (default) or playoff. |

### Returns {#vhl_goalie_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025/26). |
| `stage` | String | Season stage: regular or playoff. |
| `tn` | Int64 | Tournament id of the season stage (2025/26 regular season = 1374, playoffs = 1375); vhl_tn maps season + stage to it. |
| `player_id` | Int64 | Player id: the {player_id} of /en/players/{player_id}/, stable across seasons and teams; also the photo id on img.khl.ru. Same numeric sequence as khl.ru player ids; whether it equals the same person's khl.ru id is unverified. |
| `player` | String | Player name, 'Surname Given' (Latin on /en/ pages). |
| `jersey` | Int64 | Jersey number (#; the page legend mislabels this column 'Position'). |
| `team` | String | Team short name (Latin; the report tables' form). |
| `games_played` | Int64 | Games played (GP). |
| `wins` | Int64 | Goalie wins (W). |
| `losses` | Int64 | Goalie losses (L). |
| `shootouts_played` | Int64 | Shootouts played (SOP). |
| `shots_against` | Int64 | Shots on goal faced (the SOG column of goalie tables). |
| `goals_against` | Int64 | Goals allowed (GA). |
| `saves` | Int64 | Saves (Sv): shots on goal the goalie stopped in the row's scope (the game, or the season stage in stats and career rows). |
| `save_pct` | Float64 | Save percentage (%Sv); 0-100 scale (93.8 = 93.8 %); null when the page shows '-'. |
| `gaa` | Float64 | Goals-against average (GAA) = 60 x GA / TOI minutes; null when the page shows '-'. |
| `goals` | Int64 | Goals (G) the player scored in the row's scope: the game in a box score, the season stage in stats and career rows. |
| `assists` | Int64 | Assists (A) credited to the player in the row's scope: the game in a box score, the season stage in stats and career rows. |
| `shutouts` | Int64 | Shutouts (SO): games in the row's scope in which the goalie allowed no goal. |
| `penalty_minutes` | Int64 | Penalty minutes (PIM). |
| `toi` | String | Time on ice (TOI), 'mm:ss' string; minutes run past 59 over a season ('2314:48'); null when the page shows '-'. |

**`return_parsed=False`** — the page's parsed record (`dict`), plus `_request` (season, stage, tn and the ids asked for).

### Raises {#vhl_goalie_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The VHL has nothing for the request (a season without that stage, a game not in the season's calendar, an unknown game, player or team page). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The page could not be fetched, or it is not the page asked for (another season, game or player; a missing table or header drift). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#vhl_goalie_stats-example}

```python
vhl_goalie_stats(season=2026)
```

## vhl_player

Player card: position, height, weight, birth date, age, country and photo.

**Endpoint URL:** `GET https://www.vhlru.ru/en/players/{player_id}/`

**Valid URL:** [https://www.vhlru.ru/en/players/34526/](https://www.vhlru.ru/en/players/34526/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Player id (player_id in any VHL frame), e.g. 34526. |

### Returns {#vhl_player-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Player id, read from the page's own /players/{id}/games/ tab link; equals the requested id or the capture refuses. |
| `name` | String | Player name, 'Surname Given' (Latin), from h1 without the number. |
| `jersey` | Int64 | Current jersey number (the h1 '#79' badge); null when the page shows none. |
| `position` | String | Position in English (h2.player-card__subtitle): Forward and Goalkeeper seen; the defence label is unverified. |
| `height_cm` | Int64 | Height in centimetres (Height). |
| `weight_kg` | Int64 | Weight in kilograms (Weight). |
| `born` | String | Birth date 'DD.MM.YYYY' as the public page prints it. |
| `age` | Int64 | Age in years on the capture date (from '24 years old'). |
| `country` | String | Country (English); null when the card omits it. |
| `photo_url` | String | Photo URL on img.khl.ru, /teamplayers/{team_season_id}/{player_id}/320.jpg. The team-season id is the one the photo was filed under, not the current team: 34526's photo uses 15424 = Magnitka in tn 1375 (25/26 playoffs, verified on /en/stats/players/1375/) while his newest row is 15516 (Dynamo-Altai, 1430). |

**`return_parsed=False`** — the page's parsed record (`dict`), plus `_request` (season, stage, tn and the ids asked for).

### Raises {#vhl_player-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The VHL has nothing for the request (a season without that stage, a game not in the season's calendar, an unknown game, player or team page). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The page could not be fetched, or it is not the page asked for (another season, game or player; a missing table or header drift). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#vhl_player-example}

```python
vhl_player(player_id=34526)
```

## vhl_player_career

Player career by season stage and team (skater and goalie columns in one frame).

**Endpoint URL:** `GET https://www.vhlru.ru/en/players/{player_id}/`

**Valid URL:** [https://www.vhlru.ru/en/players/34526/](https://www.vhlru.ru/en/players/34526/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Player id (player_id in any VHL frame), e.g. 34526. |

### Returns {#vhl_player_career-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Player id, read from the page's own /players/{id}/games/ tab link; equals the requested id or the capture refuses. |
| `season` | Int64 | Season END year (2026 = 2025/26). |
| `stage` | String | Season stage: regular or playoff. |
| `tn` | Int64 | Tournament id of the season stage (2025/26 regular season = 1374, playoffs = 1375); vhl_tn maps season + stage to it. |
| `team_season_id` | Int64 | Team-season id (khl/ terminology): one id per team per tournament, from the KHL's sequence (khl/: Spartak = 14934 in 1369; here Metallurg = 15017 in 1374). The {id} of /calendar/{tn}/0/{id}/ and /stats/players/{tn}/all/{id}/. Not the stable club id (*_club_id) and not the team page id. With tn, the bridge from a player to every tournament team he played for. |
| `team` | String | Team short name (Latin; the report tables' form). |
| `jersey` | Int64 | Jersey number worn for that team that season (#). |
| `games_played` | Int64 | Games played (GP). |
| `goals` | Int64 | Goals (G) the player scored in the row's scope: the game in a box score, the season stage in stats and career rows. |
| `assists` | Int64 | Assists (A) credited to the player in the row's scope: the game in a box score, the season stage in stats and career rows. |
| `points` | Int64 | Points, goals + assists (PTS). |
| `plus_minus` | Int64 | Plus/minus (+/-). |
| `penalty_minutes` | Int64 | Penalty minutes (PIM). |
| `even_strength_goals` | Int64 | Even-strength goals (ESG). |
| `power_play_goals` | Int64 | Power-play goals (PPG). |
| `shorthanded_goals` | Int64 | Shorthanded goals (SHG). |
| `overtime_goals` | Int64 | Overtime goals (OTG). |
| `game_winning_goals` | Int64 | Game-winning goals (GWG). |
| `shootout_deciding_goals` | Int64 | Shootout-deciding goals (SDS; the legend says 'Shootouts deciding shots'). |
| `shots_on_goal` | Int64 | Shots on goal (SOG). |
| `shot_pct` | Float64 | Shooting percentage, goals / shots on goal x 100 (%SOG); 0-100 scale; null when the page shows '-'. |
| `shots_per_game` | Float64 | Shots on goal per game (S/G). |
| `faceoffs` | Int64 | Faceoffs taken (FO). |
| `faceoffs_won` | Int64 | Faceoffs won (FOW). |
| `faceoff_pct` | Float64 | Faceoff win percentage (%FO); 0-100 scale; null when the page shows '-' (no faceoffs taken). |
| `toi_per_game` | String | Average time on ice per game (TOI/G), 'm:ss' / 'mm:ss' string; null when the page shows '-'. |
| `shifts_per_game` | Float64 | Average shifts per game (Sft/G); integer in a one-game report, decimal in season tables. |
| `hits` | Int64 | Body checks delivered (Hits). |
| `blocked_shots` | Int64 | Blocked shots (BLS). |
| `wins` | Int64 | Goalie wins (W). |
| `losses` | Int64 | Goalie losses (L). |
| `shootouts_played` | Int64 | Shootouts played (SOP). |
| `shots_against` | Int64 | Shots on goal faced (the SOG column of goalie tables). |
| `goals_against` | Int64 | Goals allowed (GA). |
| `saves` | Int64 | Saves (Sv): shots on goal the goalie stopped in the row's scope (the game, or the season stage in stats and career rows). |
| `save_pct` | Float64 | Save percentage (%Sv); 0-100 scale (93.8 = 93.8 %); null when the page shows '-'. |
| `gaa` | Float64 | Goals-against average (GAA) = 60 x GA / TOI minutes; null when the page shows '-'. |
| `shutouts` | Int64 | Shutouts (SO): games in the row's scope in which the goalie allowed no goal. |
| `toi` | String | Time on ice (TOI), 'mm:ss' string; minutes run past 59 over a season ('2314:48'); null when the page shows '-'. |

**`return_parsed=False`** — the page's parsed record (`dict`), plus `_request` (season, stage, tn and the ids asked for).

### Raises {#vhl_player_career-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The VHL has nothing for the request (a season without that stage, a game not in the season's calendar, an unknown game, player or team page). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The page could not be fetched, or it is not the page asked for (another season, game or player; a missing table or header drift). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#vhl_player_career-example}

```python
vhl_player_career(player_id=34526)
```

## vhl_roster

CURRENT roster of a team page (player names in Cyrillic even on /en/).

**Endpoint URL:** `GET https://www.vhlru.ru/en/teams/{team_page_id}/`

**Valid URL:** [https://www.vhlru.ru/en/teams/390/](https://www.vhlru.ru/en/teams/390/)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_page_id` | `team_page_id` |  | `Y` |  | Team page id (home_team_page_id / away_team_page_id in vhl_game_info), e.g. 390. Neither a club id nor a team-season id. |

### Returns {#vhl_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_page_id` | Int64 | Team page id (vhlru.ru /en/teams/{team_page_id}/), stable across seasons; for report rows the page id of the row's side. |
| `team` | String | Team short name (Latin; the report tables' form). |
| `calendar_tn` | Int64 | Tournament id of the team's CURRENT season stage (the page's 'To the calendar' link). |
| `calendar_team_season_id` | Int64 | The team's team-season id in calendar_tn. |
| `roster_as_of` | String | Date the roster is valid for, as printed ('12.10.2026'). |
| `jersey` | Int64 | Jersey number (integer). |
| `position` | String | Position in English, lower case: goaltender, defense, forward. |
| `player_id` | Int64 | Player id: the {player_id} of /en/players/{player_id}/, stable across seasons and teams; also the photo id on img.khl.ru. Same numeric sequence as khl.ru player ids; whether it equals the same person's khl.ru id is unverified. |
| `player` | String | Player name, 'Surname Given' — in CYRILLIC even on the /en/ page. |
| `born` | String | Birth date 'DD.MM.YYYY' as the public page prints it. |
| `country` | String | Citizenship (English). |

**`return_parsed=False`** — the page's parsed record (`dict`), plus `_request` (season, stage, tn and the ids asked for).

### Raises {#vhl_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The VHL has nothing for the request (a season without that stage, a game not in the season's calendar, an unknown game, player or team page). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The page could not be fetched, or it is not the page asked for (another season, game or player; a missing table or header drift). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#vhl_roster-example}

```python
vhl_roster(team_page_id=390)
```
