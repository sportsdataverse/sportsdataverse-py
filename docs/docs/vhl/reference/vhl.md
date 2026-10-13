---
title: VHL — VHL (vhlru.ru server-rendered pages, parsed)
sidebar_label: VHL (vhlru.ru server-rendered pages, parsed)
description: "VHL — VHL (vhlru.ru server-rendered pages, parsed) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# VHL — VHL (vhlru.ru server-rendered pages, parsed)

`sportsdataverse.hockey.vhl` — 13 endpoints.

> **Note:** Owner flag (sdv-internal-refs/vhl/README.md): the VHL is run by Russian league operators, and several clubs are owned or sponsored by sanctioned state companies. Fetching on your own machine is fine; a published data release or a hosted mirror needs the owner's sanctions / legal sign-off first.

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
| `season` | integer | Season END year (2026 = 2025/26). |
| `stage` | character | Season stage: regular or playoff. |
| `tn` | integer | Tournament id of the season stage (2025/26 regular season = 1374, playoffs = 1375); vhl_tn maps season + stage to it. |
| `game_id` | integer | Game id (the calendar's game_id; the report's and the broadcast's idgame). The report and broadcast pages do not print it: it comes from the request. |
| `game_number` | integer | Official game number within the tournament (from '№986'). |
| `game_date` | character | Game date, ISO 'YYYY-MM-DD': the calendar day heading plus the season (August-December = START year). |
| `date` | character | Day header in English, day and month only ('18 March'); the page prints no year and no start time. |
| `weekday` | character | Weekday of the day header (English). |
| `home_team` | character | Home team name (Latin, FULL form: 'Kristall S'; standings, stats and the team page use the short 'Kristall'). |
| `home_club_id` | integer | Home club id: stable across seasons (the img.khl.ru logo path id). Neither a team-season id nor a team page id. |
| `away_team` | character | Away team name (Latin, FULL form, like home_team). |
| `away_club_id` | integer | Away club id: stable across seasons (the img.khl.ru logo path id). |
| `city` | character | City of the home team's arena (English). |
| `score` | character | Final score 'home:away'; '-:-' before the game. |
| `home_score` | integer | Home goals including the shootout winner goal; null before the game. |
| `away_score` | integer | Away goals including the shootout winner goal; null before the game. |
| `decision` | character | How the game ended beyond regulation: OT (overtime) or SO (shootout); null for regulation or unplayed games. |
| `winner` | character | Winning side from the win-home / win-visitor class: home or away; null before the game. |
| `status` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `online_url` | character | Text-broadcast URL (online.vhlru.ru/online/{game_id}.html). |

**`return_parsed=False`** — the page's parsed record (`dict`): the sdv-internal-refs capture shape, plus `_request` (season, stage, tn and the ids asked for).

### Example {#vhl_schedule-example}

```python
vhl_schedule(season=2026)
```

_Last validated n/a._

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
| `game_id` | integer | Game id (the calendar's game_id; the report's and the broadcast's idgame). The report and broadcast pages do not print it: it comes from the request. |
| `season` | integer | Season END year (2026 = 2025/26). |
| `stage` | character | Season stage: regular or playoff. |
| `game_number` | integer | Official game number (from '№984'); checked against the request. |
| `tn` | integer | Tournament id of the season stage (2025/26 regular season = 1374, playoffs = 1375); vhl_tn maps season + stage to it. |
| `date` | character | Game date, ISO 'YYYY-MM-DD', parsed from date_text. |
| `local_time` | character | Start time 'HH:MM' from date_text; local time zone unverified (likely Moscow). |
| `date_text` | character | Date line verbatim; RUSSIAN month and weekday even on /en/ ('18 Март 2026, Среда, 17:00'). The <time datetime> attribute is a constant placeholder (2020-01-23 18:00) and is ignored. |
| `attendance` | integer | Attendance (from '910 sp'). |
| `score` | character | Final score 'home:away' (the shootout counts as one goal). |
| `home_score` | integer | Home final goals. |
| `away_score` | integer | Away final goals. |
| `decision` | character | OT or SO when the game went past regulation; null otherwise. |
| `period_scores` | character | Score of each period 'home:away', overtime and shootout included (SO game: '0:2','0:0','2:0','0:0','0:1'). |
| `home_team_page_id` | integer | Home team page id. Team page id (vhlru.ru only): the {team_id} of /en/teams/{team_id}/ (Toros = 390). Neither a club id nor a team-season id; it joins nothing else (bridge: the calendar row of the same game_id). |
| `home_team` | character | Home team name (Latin, FULL form: 'Kristall S'; the report's tables use the short 'Kristall'). |
| `home_short` | character | Home team short name (the report tables' form). |
| `home_city` | character | Home team city. |
| `home_coach` | character | Home head coach, 'Surname Given'. |
| `away_team_page_id` | integer | Away team page id. Team page id (vhlru.ru only): the {team_id} of /en/teams/{team_id}/ (Toros = 390). Neither a club id nor a team-season id; it joins nothing else (bridge: the calendar row of the same game_id). |
| `away_team` | character | Away team name (Latin, FULL form). |
| `away_short` | character | Away team short name (the report tables' form). |
| `away_city` | character | Away team city. |
| `away_coach` | character | Away head coach, 'Surname Given'. |
| `referee1_number` | integer | First referee's sweater number. |
| `referee1` | character | First referee, 'Surname Given'. |
| `referee2_number` | integer | Second referee's sweater number. |
| `referee2` | character | Second referee, 'Surname Given'. |
| `linesman1_number` | integer | First linesman's sweater number. |
| `linesman1` | character | First linesman, 'Surname Given'. |
| `linesman2_number` | integer | Second linesman's sweater number. |
| `linesman2` | character | Second linesman, 'Surname Given'. |

**`return_parsed=False`** — the page's parsed record (`dict`): the sdv-internal-refs capture shape, plus `_request` (season, stage, tn and the ids asked for).

### Example {#vhl_game_info-example}

```python
vhl_game_info(game_id=899223, season=2026)
```

_Last validated n/a._

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
| `game_id` | integer | Game id (the calendar's game_id; the report's and the broadcast's idgame). The report and broadcast pages do not print it: it comes from the request. |
| `season` | integer | Season END year (2026 = 2025/26). |
| `stage` | character | Season stage: regular or playoff. |
| `game_number` | integer | Official game number (from '№984'); checked against the request. |
| `team_side` | character | Side of the game the row belongs to: home or away. |
| `team_page_id` | integer | Team page id (vhlru.ru /en/teams/{team_page_id}/), stable across seasons; for report rows the page id of the row's side. |
| `goal_number` | integer | Goal number in the game (G). |
| `period` | character | Period (Per) as printed: '1', '2', '3', overtime unverified, 'РБ' (Cyrillic even on /en/: shootout, решающий бросок). |
| `time` | character | Elapsed game time 'mm:ss' from the opening faceoff (Time); the shootout row says '65:00'. |
| `score` | character | Score after the goal 'home:away' (Score). |
| `strength` | character | Goal strength code (PMG): eq (even), ppg (power play), shg (shorthanded); null on the shootout row. |
| `scorer_id` | integer | Player id of the scorer: the {player_id} of /en/players/{player_id}/, stable across seasons and teams; also the photo id on img.khl.ru. Same numeric sequence as khl.ru player ids; whether it equals the same person's khl.ru id is unverified. |
| `scorer` | character | Goal scorer's name, 'Surname Given' as the report prints it. |
| `scorer_jersey` | integer | Scorer jersey number. |
| `scorer_season_total` | integer | The scorer's season goal count after this goal (the '(13)' suffix); null on the shootout row. |
| `assist1_id` | integer | Player id of the first assist; null when unassisted. |
| `assist1` | character | First assist name; null when unassisted. |
| `assist1_jersey` | integer | First assist jersey number. |
| `assist1_season_total` | integer | The first assist's season assist count after this goal (the '(14)' suffix). |
| `assist2_id` | integer | Player id of the second assist; null when none. |
| `assist2` | character | Second assist name; null when none. |
| `assist2_jersey` | integer | Second assist jersey number. |
| `assist2_season_total` | integer | The second assist's season assist count after this goal. |
| `home_on_ice` | character | Jersey numbers of the home players on ice at the goal (goalie included); the column header is the home team's short name. Shootout row: the goalie only. |
| `away_on_ice` | character | Jersey numbers of the away players on ice at the goal (goalie included); the column header is the away team's short name. Empty on the shootout row. |

**`return_parsed=False`** — the page's parsed record (`dict`): the sdv-internal-refs capture shape, plus `_request` (season, stage, tn and the ids asked for).

### Example {#vhl_goals-example}

```python
vhl_goals(game_id=899223, season=2026)
```

_Last validated n/a._

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
| `game_id` | integer | Game id (the calendar's game_id; the report's and the broadcast's idgame). The report and broadcast pages do not print it: it comes from the request. |
| `season` | integer | Season END year (2026 = 2025/26). |
| `stage` | character | Season stage: regular or playoff. |
| `game_number` | integer | Official game number (from '№984'); checked against the request. |
| `team_side` | character | Side of the game the row belongs to: home or away. |
| `team_page_id` | integer | Team page id (vhlru.ru /en/teams/{team_page_id}/), stable across seasons; for report rows the page id of the row's side. |
| `team` | character | Team short name (Latin; the report tables' form). |
| `time` | character | Elapsed game time of the penalty 'mm:ss'. |
| `player_id` | integer | Player id of the penalised player; null for a bench (team) penalty. |
| `player` | character | Penalised player name; null for a bench (team) penalty. |
| `player_jersey` | integer | Penalised player's jersey number. |
| `minutes` | integer | Penalty length in minutes (the unlabelled third column). |
| `infraction` | character | Infraction in English ('hooking'); null when the site leaves it blank. |

**`return_parsed=False`** — the page's parsed record (`dict`): the sdv-internal-refs capture shape, plus `_request` (season, stage, tn and the ids asked for).

### Example {#vhl_penalties-example}

```python
vhl_penalties(game_id=899223, season=2026)
```

_Last validated n/a._

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
| `game_id` | integer | Game id (the calendar's game_id; the report's and the broadcast's idgame). The report and broadcast pages do not print it: it comes from the request. |
| `season` | integer | Season END year (2026 = 2025/26). |
| `stage` | character | Season stage: regular or playoff. |
| `game_number` | integer | Official game number (from '№984'); checked against the request. |
| `team_side` | character | Side of the game the row belongs to: home or away. |
| `team_page_id` | integer | Team page id (vhlru.ru /en/teams/{team_page_id}/), stable across seasons; for report rows the page id of the row's side. |
| `team` | character | Team short name (Latin; the report tables' form). |
| `position_group` | character | Position group: defense or forward. |
| `jersey` | integer | Jersey number (integer). |
| `player_id` | integer | Player id: the {player_id} of /en/players/{player_id}/, stable across seasons and teams; also the photo id on img.khl.ru. Same numeric sequence as khl.ru player ids; whether it equals the same person's khl.ru id is unverified. |
| `player` | character | Player name, 'Surname Given' (Latin on /en/ pages). |
| `captaincy` | character | Captaincy marker from the name suffix: C (captain), A (alternate); null otherwise. |
| `games_played` | integer | Games played (GP). |
| `goals` | integer | Goals (G) the player scored in the row's scope: the game in a box score, the season stage in stats and career rows. |
| `assists` | integer | Assists (A) credited to the player in the row's scope: the game in a box score, the season stage in stats and career rows. |
| `points` | integer | Points, goals + assists (PTS). |
| `plus_minus` | integer | Plus/minus (+/-). |
| `penalty_minutes` | integer | Penalty minutes (PIM). |
| `even_strength_goals` | integer | Even-strength goals (ESG). |
| `power_play_goals` | integer | Power-play goals (PPG). |
| `shorthanded_goals` | integer | Shorthanded goals (SHG). |
| `overtime_goals` | integer | Overtime goals (OTG). |
| `game_winning_goals` | integer | Game-winning goals (GWG). |
| `shootout_deciding_goals` | integer | Shootout-deciding goals (SDS; the legend says 'Shootouts deciding shots'). |
| `shots_on_goal` | integer | Shots on goal (SOG). |
| `shot_pct` | numeric | Shooting percentage, goals / shots on goal x 100 (%SOG); 0-100 scale; null when the page shows '-'. |
| `faceoffs` | integer | Faceoffs taken (FO). |
| `faceoffs_won` | integer | Faceoffs won (FOW). |
| `faceoff_pct` | numeric | Faceoff win percentage (%FO); 0-100 scale; null when the page shows '-' (no faceoffs taken). |
| `toi_per_game` | character | Time on ice in this game (TOI/G with GP 1), 'mm:ss'. |
| `shifts_per_game` | numeric | Shifts in this game (Sft/G with GP 1). |
| `hits` | integer | Body checks delivered (Hits). |
| `blocked_shots` | integer | Blocked shots (BLS). |

**`return_parsed=False`** — the page's parsed record (`dict`): the sdv-internal-refs capture shape, plus `_request` (season, stage, tn and the ids asked for).

### Example {#vhl_player_box-example}

```python
vhl_player_box(game_id=899223, season=2026)
```

_Last validated n/a._

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
| `game_id` | integer | Game id (the calendar's game_id; the report's and the broadcast's idgame). The report and broadcast pages do not print it: it comes from the request. |
| `season` | integer | Season END year (2026 = 2025/26). |
| `stage` | character | Season stage: regular or playoff. |
| `game_number` | integer | Official game number (from '№984'); checked against the request. |
| `team_side` | character | Side of the game the row belongs to: home or away. |
| `team_page_id` | integer | Team page id (vhlru.ru /en/teams/{team_page_id}/), stable across seasons; for report rows the page id of the row's side. |
| `team` | character | Team short name (Latin; the report tables' form). |
| `jersey` | integer | Jersey number (integer). |
| `player_id` | integer | Player id: the {player_id} of /en/players/{player_id}/, stable across seasons and teams; also the photo id on img.khl.ru. Same numeric sequence as khl.ru player ids; whether it equals the same person's khl.ru id is unverified. |
| `player` | character | Player name, 'Surname Given' (Latin on /en/ pages). |
| `captaincy` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `games_played` | integer | Games played (GP). |
| `wins` | integer | Goalie wins (W). |
| `losses` | integer | Goalie losses (L). |
| `shootouts_played` | integer | Shootouts played (SOP). |
| `shots_against` | integer | Shots on goal faced (the SOG column of goalie tables). |
| `goals_against` | integer | Goals allowed (GA). |
| `saves` | integer | Saves (Sv): shots on goal the goalie stopped in the row's scope (the game, or the season stage in stats and career rows). |
| `save_pct` | numeric | Save percentage (%Sv); 0-100 scale (93.8 = 93.8 %); null when the page shows '-'. |
| `gaa` | numeric | Goals-against average (GAA) = 60 x GA / TOI minutes; null when the page shows '-'. |
| `assists` | integer | Assists (A) credited to the player in the row's scope: the game in a box score, the season stage in stats and career rows. |
| `shutouts` | integer | Shutouts (SO): games in the row's scope in which the goalie allowed no goal. |
| `penalty_minutes` | integer | Penalty minutes (PIM). |
| `toi` | character | Time on ice in this game, 'mm:ss'; null for a dressed goalie who did not play ('-'). |

**`return_parsed=False`** — the page's parsed record (`dict`): the sdv-internal-refs capture shape, plus `_request` (season, stage, tn and the ids asked for).

### Example {#vhl_goalie_box-example}

```python
vhl_goalie_box(game_id=899223, season=2026)
```

_Last validated n/a._

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
| `game_id` | integer | Game id (the calendar's game_id; the report's and the broadcast's idgame). The report and broadcast pages do not print it: it comes from the request. |
| `season` | integer | Season END year (2026 = 2025/26). |
| `stage` | character | Season stage: regular or playoff. |
| `game_number` | integer | Official game number ('Игра номер 984'); checked against the request. |
| `event_index` | integer | Order of the event on the page, 0 = the first listed (the broadcast lists the latest event first). |
| `kind` | character | Event kind from the CSS modifier: game_state (ga), comment (co), goal (go), penalty (pn), goalie_change (gc), video_review (vr), shootout_attempt (bu). |
| `kind_code` | character | Raw CSS modifier of .game-event-item--<code>. |
| `time` | character | Time label verbatim: 'mm:ss' elapsed game time, 'NN мин' (minute of a comment), or 'HH:MM' wall clock on game_state events; null when absent. See clock. |
| `clock` | character | What time means: game (elapsed mm:ss), minute ('58 мин'), wall (local wall clock, game_state rows); null when there is no time. |
| `score` | character | Score 'home-away' (dash, not colon) after a goal; on a shootout attempt, the shootout score printed beside it (before or after that attempt: unverified). |
| `strength_code` | character | Goal strength code in Cyrillic: РАВ (even), БОЛ (power play), МЕН (shorthanded, unobserved), БУЛ (penalty shot / shootout); empty string seen as null. |
| `strength` | character | strength_code in English: even, power_play, shorthanded, penalty_shot_or_shootout. |
| `text` | character | Event text verbatim in RUSSIAN (team, players, infraction, period stats); not translated. |
| `player_ids` | character | Player ids linked in the event text (//vhlru.ru/players/{id}/), scorer first; empty for most kinds. |
| `home_team` | character | Home team name in Cyrillic. |
| `away_team` | character | Away team name in Cyrillic. |

**`return_parsed=False`** — the page's parsed record (`dict`): the sdv-internal-refs capture shape, plus `_request` (season, stage, tn and the ids asked for).

### Example {#vhl_pbp-example}

```python
vhl_pbp(game_id=899223, season=2026)
```

_Last validated n/a._

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
| `season` | integer | Season END year (2026 = 2025/26). |
| `stage` | character | Season stage: regular or playoff. |
| `tn` | integer | Tournament id of the season stage (2025/26 regular season = 1374, playoffs = 1375); vhl_tn maps season + stage to it. |
| `rank` | integer | Rank (unlabelled first column). |
| `team` | character | Team short name (Latin; the report tables' form). |
| `team_season_id` | integer | Team-season id (khl/ terminology): one id per team per tournament, from the KHL's sequence (khl/: Spartak = 14934 in 1369; here Metallurg = 15017 in 1374). The {id} of /calendar/{tn}/0/{id}/ and /stats/players/{tn}/all/{id}/. Not the stable club id (*_club_id) and not the team page id. |
| `games_played` | integer | Games played (GP). |
| `wins` | integer | Regulation wins (W). |
| `ot_wins` | integer | Overtime wins (OTW). |
| `so_wins` | integer | Shootout wins (SOW). |
| `so_losses` | integer | Shootout losses (SOL). |
| `ot_losses` | integer | Overtime losses (OTL). |
| `losses` | integer | Regulation losses (L). |
| `goals_for` | integer | Goals for (left side of the G column '226-131'). |
| `goals_against` | integer | Goals against (right side of the G column). |
| `points` | integer | Points (PTS): 2 per win of any kind, 1 per OT or SO loss. |

**`return_parsed=False`** — the page's parsed record (`dict`): the sdv-internal-refs capture shape, plus `_request` (season, stage, tn and the ids asked for).

### Example {#vhl_standings-example}

```python
vhl_standings(season=2026)
```

_Last validated n/a._

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
| `season` | integer | Season END year (2026 = 2025/26). |
| `stage` | character | Season stage: regular or playoff. |
| `tn` | integer | Tournament id of the season stage (2025/26 regular season = 1374, playoffs = 1375); vhl_tn maps season + stage to it. |
| `position_group` | character | Position group: defense or forward. |
| `player_id` | integer | Player id: the {player_id} of /en/players/{player_id}/, stable across seasons and teams; also the photo id on img.khl.ru. Same numeric sequence as khl.ru player ids; whether it equals the same person's khl.ru id is unverified. |
| `player` | character | Player name, 'Surname Given' (Latin on /en/ pages). |
| `jersey` | integer | Jersey number (#; the page legend mislabels this column 'Position'). |
| `team` | character | Team short name (Latin; the report tables' form). |
| `games_played` | integer | Games played (GP). |
| `goals` | integer | Goals (G) the player scored in the row's scope: the game in a box score, the season stage in stats and career rows. |
| `assists` | integer | Assists (A) credited to the player in the row's scope: the game in a box score, the season stage in stats and career rows. |
| `points` | integer | Points, goals + assists (PTS). |
| `plus_minus` | integer | Plus/minus (+/-). |
| `plus` | integer | Plus: on-ice goals for that count toward plus/minus (+). |
| `minus` | integer | Minus: on-ice goals against that count toward plus/minus (-). |
| `penalty_minutes` | integer | Penalty minutes (PIM). |
| `even_strength_goals` | integer | Even-strength goals (ESG). |
| `power_play_goals` | integer | Power-play goals (PPG). |
| `shorthanded_goals` | integer | Shorthanded goals (SHG). |
| `overtime_goals` | integer | Overtime goals (OTG). |
| `game_winning_goals` | integer | Game-winning goals (GWG). |
| `shootout_deciding_goals` | integer | Shootout-deciding goals (SDS; the legend says 'Shootouts deciding shots'). |
| `shots_on_goal` | integer | Shots on goal (SOG). |
| `shot_pct` | numeric | Shooting percentage, goals / shots on goal x 100 (%SOG); 0-100 scale; null when the page shows '-'. |
| `shots_per_game` | numeric | Shots on goal per game (S/G). |
| `faceoffs` | integer | Faceoffs taken (FO). |
| `faceoffs_won` | integer | Faceoffs won (FOW). |
| `faceoff_pct` | numeric | Faceoff win percentage (%FO); 0-100 scale; null when the page shows '-' (no faceoffs taken). |
| `toi_per_game` | character | Average time on ice per game (TOI/G), 'm:ss' / 'mm:ss' string; null when the page shows '-'. |
| `shifts_per_game` | numeric | Average shifts per game (Sft/G); integer in a one-game report, decimal in season tables. |
| `hits` | integer | Body checks delivered (Hits). |
| `blocked_shots` | integer | Blocked shots (BLS). |

**`return_parsed=False`** — the page's parsed record (`dict`): the sdv-internal-refs capture shape, plus `_request` (season, stage, tn and the ids asked for).

### Example {#vhl_skater_stats-example}

```python
vhl_skater_stats(season=2026)
```

_Last validated n/a._

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
| `season` | integer | Season END year (2026 = 2025/26). |
| `stage` | character | Season stage: regular or playoff. |
| `tn` | integer | Tournament id of the season stage (2025/26 regular season = 1374, playoffs = 1375); vhl_tn maps season + stage to it. |
| `player_id` | integer | Player id: the {player_id} of /en/players/{player_id}/, stable across seasons and teams; also the photo id on img.khl.ru. Same numeric sequence as khl.ru player ids; whether it equals the same person's khl.ru id is unverified. |
| `player` | character | Player name, 'Surname Given' (Latin on /en/ pages). |
| `jersey` | integer | Jersey number (#; the page legend mislabels this column 'Position'). |
| `team` | character | Team short name (Latin; the report tables' form). |
| `games_played` | integer | Games played (GP). |
| `wins` | integer | Goalie wins (W). |
| `losses` | integer | Goalie losses (L). |
| `shootouts_played` | integer | Shootouts played (SOP). |
| `shots_against` | integer | Shots on goal faced (the SOG column of goalie tables). |
| `goals_against` | integer | Goals allowed (GA). |
| `saves` | integer | Saves (Sv): shots on goal the goalie stopped in the row's scope (the game, or the season stage in stats and career rows). |
| `save_pct` | numeric | Save percentage (%Sv); 0-100 scale (93.8 = 93.8 %); null when the page shows '-'. |
| `gaa` | numeric | Goals-against average (GAA) = 60 x GA / TOI minutes; null when the page shows '-'. |
| `goals` | integer | Goals (G) the player scored in the row's scope: the game in a box score, the season stage in stats and career rows. |
| `assists` | integer | Assists (A) credited to the player in the row's scope: the game in a box score, the season stage in stats and career rows. |
| `shutouts` | integer | Shutouts (SO): games in the row's scope in which the goalie allowed no goal. |
| `penalty_minutes` | integer | Penalty minutes (PIM). |
| `toi` | character | Time on ice (TOI), 'mm:ss' string; minutes run past 59 over a season ('2314:48'); null when the page shows '-'. |

**`return_parsed=False`** — the page's parsed record (`dict`): the sdv-internal-refs capture shape, plus `_request` (season, stage, tn and the ids asked for).

### Example {#vhl_goalie_stats-example}

```python
vhl_goalie_stats(season=2026)
```

_Last validated n/a._

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
| `player_id` | integer | Player id, read from the page's own /players/{id}/games/ tab link; equals the requested id or the capture refuses. |
| `name` | character | Player name, 'Surname Given' (Latin), from h1 without the number. |
| `jersey` | integer | Current jersey number (the h1 '#79' badge); null when the page shows none. |
| `position` | character | Position in English (h2.player-card__subtitle): Forward and Goalkeeper seen; the defence label is unverified. |
| `height_cm` | integer | Height in centimetres (Height). |
| `weight_kg` | integer | Weight in kilograms (Weight). |
| `born` | character | Birth date 'DD.MM.YYYY' as the public page prints it. |
| `age` | integer | Age in years on the capture date (from '24 years old'). |
| `country` | character | Country (English); null when the card omits it. |
| `photo_url` | character | Photo URL on img.khl.ru, /teamplayers/{team_season_id}/{player_id}/320.jpg. The team-season id is the one the photo was filed under, not the current team: 34526's photo uses 15424 = Magnitka in tn 1375 (25/26 playoffs, verified on /en/stats/players/1375/) while his newest row is 15516 (Dynamo-Altai, 1430). |

**`return_parsed=False`** — the page's parsed record (`dict`): the sdv-internal-refs capture shape, plus `_request` (season, stage, tn and the ids asked for).

### Example {#vhl_player-example}

```python
vhl_player(player_id=34526)
```

_Last validated n/a._

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
| `player_id` | integer | Player id, read from the page's own /players/{id}/games/ tab link; equals the requested id or the capture refuses. |
| `season` | integer | Season END year (2026 = 2025/26). |
| `stage` | character | Season stage: regular or playoff. |
| `tn` | integer | Tournament id of the season stage (2025/26 regular season = 1374, playoffs = 1375); vhl_tn maps season + stage to it. |
| `team_season_id` | integer | Team-season id (khl/ terminology): one id per team per tournament, from the KHL's sequence (khl/: Spartak = 14934 in 1369; here Metallurg = 15017 in 1374). The {id} of /calendar/{tn}/0/{id}/ and /stats/players/{tn}/all/{id}/. Not the stable club id (*_club_id) and not the team page id. With tn, the bridge from a player to every tournament team he played for. |
| `team` | character | Team short name (Latin; the report tables' form). |
| `jersey` | integer | Jersey number worn for that team that season (#). |
| `games_played` | integer | Games played (GP). |
| `goals` | integer | Goals (G) the player scored in the row's scope: the game in a box score, the season stage in stats and career rows. |
| `assists` | integer | Assists (A) credited to the player in the row's scope: the game in a box score, the season stage in stats and career rows. |
| `points` | integer | Points, goals + assists (PTS). |
| `plus_minus` | integer | Plus/minus (+/-). |
| `penalty_minutes` | integer | Penalty minutes (PIM). |
| `even_strength_goals` | integer | Even-strength goals (ESG). |
| `power_play_goals` | integer | Power-play goals (PPG). |
| `shorthanded_goals` | integer | Shorthanded goals (SHG). |
| `overtime_goals` | integer | Overtime goals (OTG). |
| `game_winning_goals` | integer | Game-winning goals (GWG). |
| `shootout_deciding_goals` | integer | Shootout-deciding goals (SDS; the legend says 'Shootouts deciding shots'). |
| `shots_on_goal` | integer | Shots on goal (SOG). |
| `shot_pct` | numeric | Shooting percentage, goals / shots on goal x 100 (%SOG); 0-100 scale; null when the page shows '-'. |
| `shots_per_game` | numeric | Shots on goal per game (S/G). |
| `faceoffs` | integer | Faceoffs taken (FO). |
| `faceoffs_won` | integer | Faceoffs won (FOW). |
| `faceoff_pct` | numeric | Faceoff win percentage (%FO); 0-100 scale; null when the page shows '-' (no faceoffs taken). |
| `toi_per_game` | character | Average time on ice per game (TOI/G), 'm:ss' / 'mm:ss' string; null when the page shows '-'. |
| `shifts_per_game` | numeric | Average shifts per game (Sft/G); integer in a one-game report, decimal in season tables. |
| `hits` | integer | Body checks delivered (Hits). |
| `blocked_shots` | integer | Blocked shots (BLS). |
| `wins` | integer | Goalie wins (W). |
| `losses` | integer | Goalie losses (L). |
| `shootouts_played` | integer | Shootouts played (SOP). |
| `shots_against` | integer | Shots on goal faced (the SOG column of goalie tables). |
| `goals_against` | integer | Goals allowed (GA). |
| `saves` | integer | Saves (Sv): shots on goal the goalie stopped in the row's scope (the game, or the season stage in stats and career rows). |
| `save_pct` | numeric | Save percentage (%Sv); 0-100 scale (93.8 = 93.8 %); null when the page shows '-'. |
| `gaa` | numeric | Goals-against average (GAA) = 60 x GA / TOI minutes; null when the page shows '-'. |
| `shutouts` | integer | Shutouts (SO): games in the row's scope in which the goalie allowed no goal. |
| `toi` | character | Time on ice (TOI), 'mm:ss' string; minutes run past 59 over a season ('2314:48'); null when the page shows '-'. |

**`return_parsed=False`** — the page's parsed record (`dict`): the sdv-internal-refs capture shape, plus `_request` (season, stage, tn and the ids asked for).

### Example {#vhl_player_career-example}

```python
vhl_player_career(player_id=34526)
```

_Last validated n/a._

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
| `team_page_id` | integer | Team page id (vhlru.ru /en/teams/{team_page_id}/), stable across seasons; for report rows the page id of the row's side. |
| `team` | character | Team short name (Latin; the report tables' form). |
| `calendar_tn` | integer | Tournament id of the team's CURRENT season stage (the page's 'To the calendar' link). |
| `calendar_team_season_id` | integer | The team's team-season id in calendar_tn. |
| `roster_as_of` | character | Date the roster is valid for, as printed ('12.10.2026'). |
| `jersey` | integer | Jersey number (integer). |
| `position` | character | Position in English, lower case: goaltender, defense, forward. |
| `player_id` | integer | Player id: the {player_id} of /en/players/{player_id}/, stable across seasons and teams; also the photo id on img.khl.ru. Same numeric sequence as khl.ru player ids; whether it equals the same person's khl.ru id is unverified. |
| `player` | character | Player name, 'Surname Given' — in CYRILLIC even on the /en/ page. |
| `born` | character | Birth date 'DD.MM.YYYY' as the public page prints it. |
| `country` | character | Citizenship (English). |

**`return_parsed=False`** — the page's parsed record (`dict`): the sdv-internal-refs capture shape, plus `_request` (season, stage, tn and the ids asked for).

### Example {#vhl_roster-example}

```python
vhl_roster(team_page_id=390)
```

_Last validated n/a._
