---
title: "Basketligaen — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) — Other"
sidebar_label: "Other"
sidebar_position: 5
description: "Basketligaen — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# Basketligaen — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) — Other

## sportality_boxscore

Player box score and team rows of both sides (2023/24 on).

**Endpoint URL:** `GET https://www.basketligaen.dk/api/gameday/boxscore/{game_uuid}`

**Valid URL:** [https://www.basketligaen.dk/api/gameday/boxscore/c4hh5nig6t](https://www.basketligaen.dk/api/gameday/boxscore/c4hh5nig6t)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_uuid` | `game_uuid` |  | `Y` |  | Game uuid: `gameInfo[].uuid` of the schedule (10 lowercase letters and digits). Key space: platform game uuids; the Genius match id is `extId` / `gameSourceId` in the body. |

### Returns {#sportality_boxscore-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_side` | String | home or away: the side the row belongs to. |
| `row_type` | String | Kind of row: player, team_coach (the bench row: team rebounds, turnovers and fouls charged to the team or coach) or team_totals. |
| `player_id` | Int64 | Genius person id (Int64). A string key in the players maps, an integer in play-by-play and box-score rows; cast here so the frames join, and join the genius family. |
| `team_id` | Int64 | Genius team id (Int64): in gameday bodies, an integer in standings and stats tables. |
| `team_name` | String | Name of the team. |
| `period` | Int64 | Period of the line: 0 = the whole game (the only value seen). |
| `full_name` | String | Full display name of the player. |
| `first_name` | String | Given (first) name(s) of the player. |
| `last_name` | String | Family name (surname) of the player. |
| `nr` | Int64 | Jersey number (null on the bench and totals rows). |
| `pos` | String | Position code of the line (G, F, C; null on the bench and totals rows). |
| `mins` | String | Minutes played as 'MM:SS' (null on the bench and totals rows). |
| `pts` | Int64 | Number of points scored. |
| `fg` | String | Field goals as 'made-attempted'. |
| `fg_perc` | Float64 | Field-goal percentage, 0-100. |
| `2_p` | String | Two-pointers as 'made-attempted'. |
| `2_p_perc` | Float64 | Two-point percentage, 0-100. |
| `3_p` | String | Three-pointers as 'made-attempted'. |
| `3_p_perc` | Float64 | Three-point percentage, 0-100. |
| `ft` | String | Free throws as 'made-attempted'. |
| `ft_perc` | Float64 | Free-throw percentage, 0-100. |
| `off` | Int64 | Offensive rebounds. |
| `def` | Int64 | Defensive rebounds. |
| `reb` | Int64 | Total rebounds. |
| `ast` | Int64 | Number of assists credited. |
| `to` | Int64 | Number of turnovers committed. |
| `stl` | Int64 | Number of steals. |
| `blk` | Int64 | Number of blocked shots made. |
| `blkr` | Int64 | Blocks received (shots of the player that were blocked). |
| `pf` | Int64 | Personal fouls. |
| `f_on` | Int64 | Fouls on (fouls drawn). |
| `plus_minus` | Int64 | Plus/minus while on court. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_boxscore-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_boxscore-example}

```python
sportality_boxscore(game_uuid='c4hh5nig6t')
```

## sportality_last_game_day

Games of the last played match day of an ssgt.

**Endpoint URL:** `GET https://www.basketligaen.dk/api/sports-v2/last-game-day`

**Valid URL:** [https://www.basketligaen.dk/api/sports-v2/last-game-day?gameType=regular](https://www.basketligaen.dk/api/sports-v2/last-game-day?gameType=regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025/26), mapped to its ssgt through layout-info. None = the active ssgt (these match-day views follow the current season). |
| `gameType` | `game_type` |  |  | `Y` | Game type: regular (the league season) or finals (the playoffs, Slutspil). |

### Returns {#sportality_last_game_day-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_uuid` | String | Platform game uuid (c4hh5nig6t); every gameday route takes it. |
| `start_date_time` | String | Scheduled start. On the schedule route this is the LOCAL (Europe/Copenhagen) wall time 'YYYY-MM-DD HH:MM:SS' with no zone; on the other routes it is ISO 8601 UTC with milliseconds (compare rawStartDateTime). |
| `state` | String | Game state. Schedule and day views: pre-game or post-game (hyphen). sports-v2 game-info: post_game (underscore). Gameday overview: GameEnded. Values: `post-game` = played (schedule, day views). |
| `overtime` | Boolean | Whether the game went to overtime. |
| `shootout` | String | Unobserved: null in every sample and every full body. When filled: Whether the game was decided by a shootout (null on every game seen; basketball has none). |
| `ssgt_uuid` | String | ssgt uuid (platform key space: one season + series + game type); joins /statistics-v2/layout-info seasonSeriesGameTypeTeams[].ssgtUuid and every route keyed by ssgt. |
| `home_team_status` | String | WIN or LOSE for the home team once the game is played; absent or empty before. Values: `LOSE` = the team lost; `WIN` = the team won. |
| `home_team_uuid` | String | Home team uuid (platform key space); joins /sports-v2/teams/{team_uuid}. |
| `home_team_owner_instance_id` | String | Site instance id of the club that owns the team (platform key space, e.g. hol2_hol). |
| `home_team_code` | String | Three-letter code of the home team. |
| `home_team_names_code` | String | Three-letter team code (HOL, RAN ...); codes changed between seasons for some clubs (BAK in 2026, BKA in 2025 for Bakken Bears). |
| `home_team_names_short` | String | Short name of the team. |
| `home_team_names_long` | String | Long name of the team. |
| `home_team_names_full` | String | Full name of the team. |
| `home_team_names_code_site` | String | Team code as the club's own site shows it. |
| `home_team_names_short_site` | String | Short team name as the club's own site shows it. |
| `home_team_names_full_site` | String | Full team name as the club's own site shows it. |
| `home_team_names_long_site` | String | Long team name as the club's own site shows it. |
| `home_team_score` | Int64 | Home points; null before the game is played. |
| `home_team_icon` | String | Home team logo URL. |
| `away_team_status` | String | WIN or LOSE for the away team once the game is played; absent or empty before. Values: `LOSE` = the team lost; `WIN` = the team won. |
| `away_team_uuid` | String | Away team uuid (platform key space); joins /sports-v2/teams/{team_uuid}. |
| `away_team_owner_instance_id` | String | Site instance id of the club that owns the team (platform key space, e.g. hol2_hol). |
| `away_team_code` | String | Three-letter code of the away team. |
| `away_team_names_code` | String | Three-letter team code (HOL, RAN ...); codes changed between seasons for some clubs (BAK in 2026, BKA in 2025 for Bakken Bears). |
| `away_team_names_short` | String | Short name of the team. |
| `away_team_names_long` | String | Long name of the team. |
| `away_team_names_full` | String | Full name of the team. |
| `away_team_names_code_site` | String | Team code as the club's own site shows it. |
| `away_team_names_short_site` | String | Short team name as the club's own site shows it. |
| `away_team_names_full_site` | String | Full team name as the club's own site shows it. |
| `away_team_names_long_site` | String | Long team name as the club's own site shows it. |
| `away_team_score` | Int64 | Away points; null before the game is played. |
| `away_team_icon` | String | Away team logo URL. |
| `venue_uuid` | String | Venue uuid (platform key space). |
| `venue_name` | String | Venue name; absent when unknown. |
| `series_uuid` | String | Series uuid (platform key space). |
| `series_code` | String | Series display name (Basketligaen; NOT the DBL code). |
| `series_display_name` | String | Series display name (Basketligaen). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_last_game_day-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_last_game_day-example}

```python
sportality_last_game_day()
```

## sportality_layout_info

Directory of every ssgt (season + series + game type) with its teams, and the stats modules.

**Endpoint URL:** `GET https://www.basketligaen.dk/api/statistics-v2/layout-info`

**Valid URL:** [https://www.basketligaen.dk/api/statistics-v2/layout-info](https://www.basketligaen.dk/api/statistics-v2/layout-info)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#sportality_layout_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025/26). |
| `season_code` | String | Provider season code: the START year (2025 = 2025/26). |
| `season_name` | String | Season name as 'YYYY/YYYY' (start/end year). |
| `game_type` | String | Game type of the game ('' on every line seen). |
| `game_type_name` | String | Game type name in Danish (Grundspil = regular season, Slutspil = playoffs). |
| `series_code` | String | Series code (DBL = Basketligaen). |
| `ssgt_uuid` | String | ssgt uuid (platform key space: one season + series + game type); joins /statistics-v2/layout-info seasonSeriesGameTypeTeams[].ssgtUuid and every route keyed by ssgt. |
| `state` | String | Game state. Schedule and day views: pre-game or post-game (hyphen). sports-v2 game-info: post_game (underscore). Gameday overview: GameEnded. Values: `active` = current ssgt; `closed` = past ssgt. |
| `season_uuid` | String | Season uuid (platform key space). |
| `series_uuid` | String | Series uuid (platform key space). |
| `game_type_uuid` | String | Game type uuid (platform key space). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_layout_info-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_layout_info-example}

```python
sportality_layout_info()
```

## sportality_next_game_day

Games of the next match day of an ssgt.

**Endpoint URL:** `GET https://www.basketligaen.dk/api/sports-v2/next-game-day`

**Valid URL:** [https://www.basketligaen.dk/api/sports-v2/next-game-day?gameType=regular](https://www.basketligaen.dk/api/sports-v2/next-game-day?gameType=regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025/26), mapped to its ssgt through layout-info. None = the active ssgt (these match-day views follow the current season). |
| `gameType` | `game_type` |  |  | `Y` | Game type: regular (the league season) or finals (the playoffs, Slutspil). |

### Returns {#sportality_next_game_day-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_uuid` | String | Platform game uuid (c4hh5nig6t); every gameday route takes it. |
| `start_date_time` | String | Scheduled start. On the schedule route this is the LOCAL (Europe/Copenhagen) wall time 'YYYY-MM-DD HH:MM:SS' with no zone; on the other routes it is ISO 8601 UTC with milliseconds (compare rawStartDateTime). |
| `state` | String | Game state. Schedule and day views: pre-game or post-game (hyphen). sports-v2 game-info: post_game (underscore). Gameday overview: GameEnded. Values: `pre-game` = not yet played (schedule, day views). |
| `overtime` | Boolean | Whether the game went to overtime. |
| `shootout` | String | Unobserved: null in every sample and every full body. When filled: Whether the game was decided by a shootout (null on every game seen; basketball has none). |
| `ssgt_uuid` | String | ssgt uuid (platform key space: one season + series + game type); joins /statistics-v2/layout-info seasonSeriesGameTypeTeams[].ssgtUuid and every route keyed by ssgt. |
| `home_team_status` | String | WIN or LOSE for the home team once the game is played; absent or empty before. Values: `N/A` = no result yet / not applicable. |
| `home_team_uuid` | String | Home team uuid (platform key space); joins /sports-v2/teams/{team_uuid}. |
| `home_team_owner_instance_id` | String | Site instance id of the club that owns the team (platform key space, e.g. hol2_hol). |
| `home_team_code` | String | Three-letter code of the home team. |
| `home_team_names_code` | String | Three-letter team code (HOL, RAN ...); codes changed between seasons for some clubs (BAK in 2026, BKA in 2025 for Bakken Bears). |
| `home_team_names_short` | String | Short name of the team. |
| `home_team_names_long` | String | Long name of the team. |
| `home_team_names_full` | String | Full name of the team. |
| `home_team_names_code_site` | String | Team code as the club's own site shows it. |
| `home_team_names_short_site` | String | Short team name as the club's own site shows it. |
| `home_team_names_full_site` | String | Full team name as the club's own site shows it. |
| `home_team_names_long_site` | String | Long team name as the club's own site shows it. |
| `home_team_score` | Int64 | Home points; null before the game is played. |
| `home_team_icon` | String | Home team logo URL. |
| `away_team_status` | String | WIN or LOSE for the away team once the game is played; absent or empty before. Values: `N/A` = no result yet / not applicable. |
| `away_team_uuid` | String | Away team uuid (platform key space); joins /sports-v2/teams/{team_uuid}. |
| `away_team_owner_instance_id` | String | Site instance id of the club that owns the team (platform key space, e.g. hol2_hol). |
| `away_team_code` | String | Three-letter code of the away team. |
| `away_team_names_code` | String | Three-letter team code (HOL, RAN ...); codes changed between seasons for some clubs (BAK in 2026, BKA in 2025 for Bakken Bears). |
| `away_team_names_short` | String | Short name of the team. |
| `away_team_names_long` | String | Long name of the team. |
| `away_team_names_full` | String | Full name of the team. |
| `away_team_names_code_site` | String | Team code as the club's own site shows it. |
| `away_team_names_short_site` | String | Short team name as the club's own site shows it. |
| `away_team_names_full_site` | String | Full team name as the club's own site shows it. |
| `away_team_names_long_site` | String | Long team name as the club's own site shows it. |
| `away_team_score` | Int64 | Away points; null before the game is played. |
| `away_team_icon` | String | Away team logo URL. |
| `venue_uuid` | String | Venue uuid (platform key space). |
| `venue_name` | String | Venue name; absent when unknown. |
| `series_uuid` | String | Series uuid (platform key space). |
| `series_code` | String | Series display name (Basketligaen; NOT the DBL code). |
| `series_display_name` | String | Series display name (Basketligaen). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_next_game_day-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_next_game_day-example}

```python
sportality_next_game_day()
```

## sportality_pbp

Play-by-play events with shot coordinates and the Genius ids (2023/24 on).

**Endpoint URL:** `GET https://www.basketligaen.dk/api/gameday/play-by-play/{game_uuid}`

**Valid URL:** [https://www.basketligaen.dk/api/gameday/play-by-play/c4hh5nig6t](https://www.basketligaen.dk/api/gameday/play-by-play/c4hh5nig6t)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_uuid` | `game_uuid` |  | `Y` |  | Game uuid: `gameInfo[].uuid` of the schedule (10 lowercase letters and digits). Key space: platform game uuids; the Genius match id is `extId` / `gameSourceId` in the body. |

### Returns {#sportality_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_source_id` | Int64 | Genius match id (Int64), the same number as game_id and sports-v2 game_info_ext_id. |
| `game_id` | Int64 | Genius match id (Int64), the join to the genius family; on play-by-play lines, an integer elsewhere. |
| `event_id` | Int64 | Genius event number within the game (Int64), increasing through the game. |
| `type` | String | Event type: one line per action; shots are the shooter's line, assists, blocks, steals and fouls drawn are further lines. Values: `2pt` = two-point attempt (made when isSuccessful = 1); `3pt` = three-point attempt (made when isSuccessful = 1); `assist` = assist on the previous made shot; `block` = block; `clock` = clock start, stop or adjustment; `foul` = foul committed; `foulon` = foul drawn (fouled player); `freethrow` = free-throw attempt; `game` = game start / end line; `headcoachchallenge` = head coach challenge; `jumpball` = jump ball or held ball; `period` = period start / end line; `rebound` = rebound; `steal` = steal; `substitution` = substitution (in or out); `timeout` = timeout; `turnover` = turnover. |
| `event_uuid` | String | Event uuid (UUID v5 derived from the event; unique per event). |
| `period` | Int64 | Period: 0 = the whole game, 1-4 the quarters, 5 and up overtime. |
| `period_type` | String | REGULAR or OVERTIME; absent on the period marker lines. Values: `OVERTIME` = overtime period; `REGULAR` = regulation period. |
| `time` | String | Clock REMAINING in the period as mm:ss:cc (cc looks like hundredths of a second, only ever 00, 10, 30, 50 ... seen; unverified). Absent on the per-period marker lines. |
| `real_world_time` | String | Wall-clock time of the event, ISO 8601 UTC. |
| `updated_time` | String | When the feed last updated the event, ISO 8601 UTC. |
| `home_team_id` | Int64 | Genius team id of the home team (Genius key space; Int64). |
| `home_team_code` | String | Home team code as Genius spells it (mixed case, 'Hol'). |
| `home_team_name` | String | Name of the home team. |
| `home_abbreviation` | String | Home team abbreviation. |
| `home_team_number` | Int64 | Team number in the game; home is 1. |
| `home_place` | String | Side of the home block: always home. |
| `home_score` | Int64 | Points scored by the home team. |
| `away_team_id` | Int64 | Genius team id of the away team (Genius key space; Int64). |
| `away_team_code` | String | Away team code as Genius spells it. |
| `away_team_name` | String | Name of the away team. |
| `away_abbreviation` | String | Away team abbreviation. |
| `away_team_number` | Int64 | Team number in the game; away is 2. |
| `away_place` | String | Side of the away block: always away. |
| `away_score` | Int64 | Points scored by the away team. |
| `revision` | Int64 | Revision counter of the event. |
| `game_state` | String | Game state at the event: ongoing during play, GameEnded on the final line. Values: `GameEnded` = game over (the final line of a complete feed); `ongoing` = game in progress. |
| `game_type` | String | Game type of the game ('' on every line seen). |
| `round` | Int64 | Round number (0). |
| `start_date_and_time` | String | Start of the game ('' on every line seen). |
| `sub_type` | String | Refinement of `type` (the shot type, rebound kind, foul kind, ...); null on types that have none. Values: `1of1` = free throw 1 of 1; `1of2` = free throw 1 of 2; `1of3` = free throw 1 of 3; `24sec` = shot-clock violation; `2of2` = free throw 2 of 2; `2of3` = free throw 2 of 3; `3of3` = free throw 3 of 3; `3sec` = three-second violation; `5sec` = five-second violation; `8sec` = eight-second violation; `adjustment` = clock adjustment; `alleyoop` = alley-oop shot; `alleyoopdunk` = alley-oop dunk; `backcourt` = backcourt violation; `badpass` = bad pass; `ballhandling` = ball-handling error; `benchTechnical` = technical foul on the bench; `coachTechnical` = technical foul on the coach; `defensive` = defensive rebound; `disqualifying` = disqualifying foul; `disruptive` = disruptive foul (unverified); `doubledribble` = double dribble; `drivinglayup` = driving layup; `dunk` = dunk; `end` = end of the period or game; `eurostep` = euro-step layup; `fadeaway` = fadeaway jump shot; `flagrant` = flagrant foul; `floatingjumpshot` = floating jump shot; `full` = full timeout; `heldball` = held ball; `hookshot` = hook shot; `in` = player enters; `jumpshot` = jump shot; `layup` = layup; `lodgedball` = lodged ball; `lost` = lost the jump ball; `offensive` = offensive rebound or offensive foul; `offensivegoaltending` = offensive goaltending; `officials` = official timeout; `other` = other; `out` = player leaves; `outofbounds` = out of bounds; `outofboundsrebound` = rebound off an out-of-bounds ball (unverified); `personal` = personal foul; `pullupjumpshot` = pull-up jump shot; `reverselayup` = reverse layup; `start` = start of the period or game, or clock start; `startperiod` = start of a period (jump ball line); `stepbackjumpshot` = step-back jump shot; `stop` = clock stop; `technical` = technical foul; `throwIn` = throw-in violation (unverified); `tipindunk` = tip-in dunk; `tipinlayup` = tip-in layup; `travel` = traveling; `turnaroundjumpshot` = turnaround jump shot; `unsportsmanlike` = unsportsmanlike foul; `won` = won the jump ball. |
| `qualifiers` | String | Event qualifiers joined by ';' (a list in recent games, a ';'-joined string or null in older ones). |
| `is_successful` | Int64 | 1 when the action succeeded (a made shot or free throw, a recorded rebound ...). ABSENT, not 0, for a missed shot, so count misses as rows without it. |
| `instance_id` | String | Site instance id of the club or league (platform key space, '<code><n>_<code>' such as hol2_hol; the league itself is dbl1_dbl). |
| `source` | String | Feed that produced the line (genius-basketball-stream-reader). |
| `game_uuid` | String | Platform game uuid (c4hh5nig6t); every gameday route takes it. |
| `event_team_id` | Int64 | Genius team id (Genius key space; Int64). |
| `event_team_code` | String | Team code as Genius spells it. |
| `event_team_name` | String | Name of the team. |
| `event_abbreviation` | String | Team abbreviation. |
| `event_team_number` | Int64 | Team number in the game (1 home, 2 away). |
| `event_place` | String | Side of the team the event belongs to: home or away. |
| `player_id` | Int64 | Genius person id (Int64). A string key in the players maps, an integer in play-by-play and box-score rows; cast here so the frames join, and join the genius family. |
| `player_pno` | Int64 | Genius shirt-table number of the player. |
| `player_team_number` | Int64 | Team number in the game (1 home, 2 away). |
| `player_first_name` | String | Given (first) name(s) of the player. |
| `player_family_name` | String | Family name (this route says familyName; the others lastName). |
| `coordinates_x` | Float64 | Shot or event x coordinate in Genius court units (Float64); origin and scale unverified. |
| `coordinates_y` | Float64 | Shot or event y coordinate in Genius court units (Float64); origin and orientation unverified. |
| `period_time` | Int64 | Marker lines only: 0 (an integer), the start of the period. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_pbp-example}

```python
sportality_pbp(game_uuid='c4hh5nig6t')
```

## sportality_post_game_team_stats

Post-game team comparison (2023/24 on).

**Endpoint URL:** `GET https://www.basketligaen.dk/api/gameday/post-game-data/team-stats/{game_uuid}`

**Valid URL:** [https://www.basketligaen.dk/api/gameday/post-game-data/team-stats/c4hh5nig6t?ssgtUuid=xs07bd3fmp&homeTeamUuid=rfo82vr830&awayTeamUuid=gvvuxe9ysw](https://www.basketligaen.dk/api/gameday/post-game-data/team-stats/c4hh5nig6t?ssgtUuid=xs07bd3fmp&homeTeamUuid=rfo82vr830&awayTeamUuid=gvvuxe9ysw)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_uuid` | `game_uuid` |  | `Y` |  | Game uuid: `gameInfo[].uuid` of the schedule (10 lowercase letters and digits). Key space: platform game uuids; the Genius match id is `extId` / `gameSourceId` in the body. |
| `ssgtUuid` | `ssgt_uuid` |  | `Y` |  | Required. ssgt uuid. Without it the answer is an empty 200 body. |
| `homeTeamUuid` | `home_team_uuid` |  | `Y` |  | Required. Home team uuid; without it the home side comes back blank. |
| `awayTeamUuid` | `away_team_uuid` |  | `Y` |  | Required. Away team uuid; without it the away side comes back blank. |

### Returns {#sportality_post_game_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `home_team_side_translate_key` | String | Translation key of the comparison row's label (s_2P, s_3P, s_FT, ...); the site translates it. |
| `home_team_left_type` | String | Stat code of the left value (2PM = two-pointers made). |
| `home_team_left_value` | Int64 | Value of the left-hand statistic of the comparison row. |
| `home_team_right_type` | String | Stat code of the right value (2PA = two-pointers attempted). |
| `home_team_right_value` | Int64 | Value of the right-hand statistic of the comparison row. |
| `home_team_center_type` | String | Stat code of the centre value (2PPerc = two-point percentage). |
| `home_team_center_value` | Int64 | Centre value (percentage as an integer 0-100). |
| `away_team_side_translate_key` | String | Translation key of the comparison row's label (s_2P, s_3P, s_FT, ...); the site translates it. |
| `away_team_left_type` | String | Stat code of the left value (2PM = two-pointers made). |
| `away_team_left_value` | Int64 | Value of the left-hand statistic of the comparison row. |
| `away_team_right_type` | String | Stat code of the right value (2PA = two-pointers attempted). |
| `away_team_right_value` | Int64 | Value of the right-hand statistic of the comparison row. |
| `away_team_center_type` | String | Stat code of the centre value (2PPerc = two-point percentage). |
| `away_team_center_value` | Int64 | Centre value (percentage as an integer 0-100). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_post_game_team_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_post_game_team_stats-example}

```python
sportality_post_game_team_stats(away_team_uuid='gvvuxe9ysw', game_uuid='c4hh5nig6t', home_team_uuid='rfo82vr830', ssgt_uuid='xs07bd3fmp')
```

## sportality_roster

Current roster of a team, grouped by position.

**Endpoint URL:** `GET https://www.basketligaen.dk/api/sports-v2/athletes/by-team-uuid/{team_uuid}`

**Valid URL:** [https://www.basketligaen.dk/api/sports-v2/athletes/by-team-uuid/rfo82vr830](https://www.basketligaen.dk/api/sports-v2/athletes/by-team-uuid/rfo82vr830)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_uuid` | `team_uuid` |  | `Y` |  | Team uuid: `homeTeamInfo.uuid` / `awayTeamInfo.uuid` in the schedule or `teamList[].uuid`. Key space: platform team uuids (not the numeric Genius team id). |

### Returns {#sportality_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `position` | String | Playing position. |
| `position_code` | String | Position code (G, F, C, or a finer code such as SG on the profile route). Values: `C` = center; `F` = forward; `G` = guard. |
| `player_type` | String | Record type (athlete). Values: `athlete` = a player. |
| `gender` | String | Gender of the person as recorded by the site (male in every row seen). |
| `uuid` | String | Athlete uuid (platform key space). |
| `first_name` | String | Given (first) name(s) of the player. |
| `last_name` | String | Family name (surname) of the player. |
| `full_name` | String | Full display name of the player. |
| `nationality` | String | ISO 3166-1 alpha-2 country code. |
| `jersey_number` | Int64 | Jersey (shirt) number of the player. |
| `portrait_url` | String | URL of the player's latest rendered portrait. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_roster-example}

```python
sportality_roster(team_uuid='rfo82vr830')
```

## sportality_schedule

Schedule and results of one season + series + game type.

**Endpoint URL:** `GET https://www.basketligaen.dk/api/sports-v2/game-schedule`

**Valid URL:** [https://www.basketligaen.dk/api/sports-v2/game-schedule?season=2026&gameType=regular](https://www.basketligaen.dk/api/sports-v2/game-schedule?season=2026&gameType=regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025/26). Mapped to the provider's ssgt through /statistics-v2/layout-info (Basketligaen 2002/03 on); a season it does not list raises NoDataError. |
| `gameType` | `game_type` |  |  | `Y` | Game type: regular (the league season) or finals (the playoffs, Slutspil). |

### Returns {#sportality_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_uuid` | String | Platform game uuid (c4hh5nig6t); every gameday route takes it. |
| `raw_start_date_time` | String | Scheduled start, ISO 8601 UTC. |
| `start_date_time` | String | Scheduled start. On the schedule route this is the LOCAL (Europe/Copenhagen) wall time 'YYYY-MM-DD HH:MM:SS' with no zone; on the other routes it is ISO 8601 UTC with milliseconds (compare rawStartDateTime). |
| `state` | String | Game state. Schedule and day views: pre-game or post-game (hyphen). sports-v2 game-info: post_game (underscore). Gameday overview: GameEnded. Values: `post-game` = played (schedule, day views); `pre-game` = not yet played (schedule, day views). |
| `overtime` | Boolean | Whether the game went to overtime. |
| `shootout` | String | Unobserved: null in every sample and every full body. When filled: Whether the game was decided by a shootout (null on every game seen; basketball has none). |
| `ssgt_uuid` | String | ssgt uuid (platform key space: one season + series + game type); joins /statistics-v2/layout-info seasonSeriesGameTypeTeams[].ssgtUuid and every route keyed by ssgt. |
| `home_team_status` | String | WIN or LOSE for the home team once the game is played; absent or empty before. Values: `LOSE` = the team lost; `N/A` = no result yet / not applicable; `WIN` = the team won. |
| `home_team_uuid` | String | Home team uuid (platform key space); joins /sports-v2/teams/{team_uuid}. |
| `home_team_owner_instance_id` | String | Site instance id of the club that owns the team (platform key space, e.g. hol2_hol). |
| `home_team_code` | String | Three-letter code of the home team. |
| `home_team_names_code` | String | Three-letter team code (HOL, RAN ...); codes changed between seasons for some clubs (BAK in 2026, BKA in 2025 for Bakken Bears). |
| `home_team_names_short` | String | Short name of the team. |
| `home_team_names_long` | String | Long name of the team. |
| `home_team_names_full` | String | Full name of the team. |
| `home_team_names_code_site` | String | Team code as the club's own site shows it. |
| `home_team_names_short_site` | String | Short team name as the club's own site shows it. |
| `home_team_names_full_site` | String | Full team name as the club's own site shows it. |
| `home_team_names_long_site` | String | Long team name as the club's own site shows it. |
| `home_team_score` | Int64 | Home points; null before the game is played. |
| `home_team_icon` | String | Home team logo URL. |
| `away_team_status` | String | WIN or LOSE for the away team once the game is played; absent or empty before. Values: `LOSE` = the team lost; `N/A` = no result yet / not applicable; `WIN` = the team won. |
| `away_team_uuid` | String | Away team uuid (platform key space); joins /sports-v2/teams/{team_uuid}. |
| `away_team_owner_instance_id` | String | Site instance id of the club that owns the team (platform key space, e.g. hol2_hol). |
| `away_team_code` | String | Three-letter code of the away team. |
| `away_team_names_code` | String | Three-letter team code (HOL, RAN ...); codes changed between seasons for some clubs (BAK in 2026, BKA in 2025 for Bakken Bears). |
| `away_team_names_short` | String | Short name of the team. |
| `away_team_names_long` | String | Long name of the team. |
| `away_team_names_full` | String | Full name of the team. |
| `away_team_names_code_site` | String | Team code as the club's own site shows it. |
| `away_team_names_short_site` | String | Short team name as the club's own site shows it. |
| `away_team_names_full_site` | String | Full team name as the club's own site shows it. |
| `away_team_names_long_site` | String | Long team name as the club's own site shows it. |
| `away_team_score` | Int64 | Away points; null before the game is played. |
| `away_team_icon` | String | Away team logo URL. |
| `venue_uuid` | String | Venue uuid (platform key space). |
| `venue_name` | String | Venue name; absent when unknown. |
| `series_uuid` | String | Series uuid (platform key space). |
| `series_code` | String | Series display name (Basketligaen; NOT the DBL code). |
| `series_display_name` | String | Series display name (Basketligaen). |
| `round_number` | Int64 | Round number (0 for every game seen; the league has no rounds). |
| `round_label` | String | Round label ('' for every game seen). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_schedule-example}

```python
sportality_schedule(season=2026)
```

## sportality_season_filter

Series, the 25 seasons (2002/03 on), game types and the default ssgt.

**Endpoint URL:** `GET https://www.basketligaen.dk/api/sports-v2/season-series-game-types-filter`

**Valid URL:** [https://www.basketligaen.dk/api/sports-v2/season-series-game-types-filter](https://www.basketligaen.dk/api/sports-v2/season-series-game-types-filter)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#sportality_season_filter-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `kind` | String | Directory entry kind: series, season or game_type. |
| `code` | String | Series code (DBL = Basketligaen). |
| `name` | String | Game type name in Danish (Grundspil = regular season, Slutspil = playoffs). |
| `uuid` | String | Series uuid (platform key space); the seriesUuid of /sports-v2/game-schedule. |
| `deleted` | Boolean | Soft-delete flag; false on every record seen. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_season_filter-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_season_filter-example}

```python
sportality_season_filter()
```

## sportality_sites

Clubs (sites) taking part in an ssgt.

**Endpoint URL:** `GET https://www.basketligaen.dk/api/sports-v2/all-sites/{ssgt_uuid}`

**Valid URL:** [https://www.basketligaen.dk/api/sports-v2/all-sites/xs07bd3fmp](https://www.basketligaen.dk/api/sports-v2/all-sites/xs07bd3fmp)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ssgt_uuid` | `ssgt_uuid` |  | `Y` |  | ssgt uuid: one season + series + game type, e.g. xs07bd3fmp = 2026/2027 regular season. Directory: `/statistics-v2/layout-info` `seasonSeriesGameTypeTeams[].ssgtUuid`. |

### Returns {#sportality_sites-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | String | Site instance id of the club (platform key space; the same as instanceId). |
| `instance_id` | String | Site instance id of the club or league (platform key space, '<code><n>_<code>' such as hol2_hol; the league itself is dbl1_dbl). |
| `public_url` | String | The club's own website. |
| `name` | String | Display name of the club. |
| `icon` | String | Team logo URL (SVG on sportality.cdn.s8y.se). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_sites-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_sites-example}

```python
sportality_sites(ssgt_uuid='xs07bd3fmp')
```

## sportality_ssgt_provider

Stats provider of an ssgt, a bare word (genius-basketball).

**Endpoint URL:** `GET https://www.basketligaen.dk/api/sports-v2/ssgt/{ssgt_uuid}/provider`

**Valid URL:** [https://www.basketligaen.dk/api/sports-v2/ssgt/xs07bd3fmp/provider](https://www.basketligaen.dk/api/sports-v2/ssgt/xs07bd3fmp/provider)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ssgt_uuid` | `ssgt_uuid` |  | `Y` |  | ssgt uuid: one season + series + game type, e.g. xs07bd3fmp = 2026/2027 regular season. Directory: `/statistics-v2/layout-info` `seasonSeriesGameTypeTeams[].ssgtUuid`. |

### Returns {#sportality_ssgt_provider-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `provider` | String | Stats provider of the ssgt (genius-basketball, a Genius Sports LiveStats relay). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_ssgt_provider-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_ssgt_provider-example}

```python
sportality_ssgt_provider(ssgt_uuid='xs07bd3fmp')
```

## sportality_standings

Standings of an ssgt (gameday widget).

**Endpoint URL:** `GET https://www.basketligaen.dk/api/gameday/team-standings/{ssgt_uuid}`

**Valid URL:** [https://www.basketligaen.dk/api/gameday/team-standings/xs07bd3fmp](https://www.basketligaen.dk/api/gameday/team-standings/xs07bd3fmp)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ssgt_uuid` | `ssgt_uuid` |  | `Y` |  | ssgt uuid: one season + series + game type, e.g. xs07bd3fmp = 2026/2027 regular season. Directory: `/statistics-v2/layout-info` `seasonSeriesGameTypeTeams[].ssgtUuid`. |

### Returns {#sportality_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Genius team id (Int64): in gameday bodies, an integer in standings and stats tables. |
| `team_uuid` | String | Platform team uuid. |
| `team_code` | String | Platform team code (HOL, RAN). |
| `team_name` | String | Name of the team. |
| `rank` | Int64 | Rank in the standings table, 1 = first. |
| `gp` | Int64 | Number of games played. |
| `w` | Int64 | Number of wins. |
| `l` | Int64 | Number of losses. |
| `tp` | Int64 | League table points (2 per win). |
| `for` | Int64 | Points scored by the team (points for). |
| `ga` | Int64 | Points conceded. |
| `plus_minus` | Int64 | Point difference. |
| `strk` | Int64 | Current streak as a signed integer: positive = wins in a row, negative = losses in a row. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_standings-example}

```python
sportality_standings(ssgt_uuid='xs07bd3fmp')
```

## sportality_stats_info

Stats table of one module for a season (players, teams, games, standings).

**Endpoint URL:** `GET https://www.basketligaen.dk/api/statistics-v2/stats-info/{module}`

**Valid URL:** [https://www.basketligaen.dk/api/statistics-v2/stats-info/players_summary?count=1000&season=2026&gameType=regular](https://www.basketligaen.dk/api/statistics-v2/stats-info/players_summary?count=1000&season=2026&gameType=regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `module` | `module` |  | `Y` |  | Stats table: players_* (summary, fieldGoal, rebound, point, offensive, defensive, foul), teams_* (summary, attendance, possession, fieldGoal, point, rebound, offensive, defensive, foul), games_games or standings_standings. |
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025/26). Mapped to the provider's ssgt through /statistics-v2/layout-info (Basketligaen 2002/03 on); a season it does not list raises NoDataError. |
| `gameType` | `game_type` |  |  | `Y` | Game type: regular (the league season) or finals (the playoffs, Slutspil). |

### Returns {#sportality_stats_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Genius person id (Int64). A string key in the players maps, an integer in play-by-play and box-score rows; cast here so the frames join, and join the genius family. |
| `gp` | Int64 | Number of games played. |
| `eff` | Float64 | Efficiency (valuation) per game; the formula is the site's own (unverified). |
| `mins` | Float64 | Average minutes played per game ('27.3'). |
| `ppg` | Float64 | Points per game. |
| `rpg` | Float64 | Rebounds per game. |
| `apg` | Float64 | Assists per game. |
| `stpg` | Float64 | Steals per game. |
| `blkpg` | Float64 | Blocks per game. |
| `info_uuid` | String | Athlete uuid on player tables, team uuid on team tables (platform key space). |
| `info_full_name` | String | Full display name of the player. |
| `info_first_name` | String | Given (first) name(s) of the player. |
| `info_last_name` | String | Family name (surname) of the player. |
| `info_nationality` | String | ISO 3166-1 alpha-2 country code. |
| `info_shoots` | String | Shooting hand (null). |
| `info_gender` | String | Gender of the person as recorded by the site (male in every row seen). |
| `info_weight_value` | Int64 | Weight of the player in kilograms. |
| `info_weight_format` | String | Unit of the value (kg). Values: `kg` = kilograms. |
| `info_height_value` | Int64 | Height of the player in centimetres. |
| `info_height_format` | String | Unit of the value (cm). Values: `cm` = centimetres. |
| `info_team_uuid` | String | Team uuid (platform key space); joins /sports-v2/teams/{team_uuid}. |
| `info_team_name` | String | Name of the team. |
| `info_team_code` | String | Three-letter team code. |
| `info_team_media` | String | URL of the team logo image. |
| `info_team_owner_instance_id` | String | Site instance id of the club that owns the team (platform key space, e.g. hol2_hol). |
| `info_team_site_display_name` | String | Team name as the league site displays it. |
| `info_team_id` | Int64 | Genius team id (Genius key space; an integer here). |
| `info_number` | Int64 | Jersey (shirt) number of the player. |
| `info_position` | String | Position code (G, F, C, SG, SF ...). |
| `w` | Int64 | Wins. Team and standings modules: wins of the team in the ssgt. |
| `p_avg` | Float64 | Points per game. |
| `r_avg` | Float64 | Rebounds per game. |
| `a_avg` | Float64 | Assists per game. |
| `s_avg` | Float64 | Steals per game. |
| `b_avg` | Float64 | Blocks per game. |
| `f_avg` | Float64 | Fouls per game. |
| `2_p_perc` | Float64 | Two-point percentage, 0-100. |
| `3_p_perc` | Float64 | Three-point percentage, 0-100. |
| `ft_perc` | Float64 | Free-throw percentage, 0-100. |
| `info_name` | String | Name of the team. |
| `info_site_display_name` | String | Team name as the league site displays it. |
| `info_owner_instance_id` | String | Site instance id of the club that owns the team (platform key space, e.g. hol2_hol). |
| `date_time` | String | Start time 'YYYY-MM-DD HH:MM:SS', LOCAL (Europe/Copenhagen) wall time with no zone. |
| `home_team` | String | Name of the home team. |
| `away_team` | String | Name of the away team. |
| `venue` | String | Name of the venue the game was played in. |
| `attendance` | Int64 | Attendance as an integer; present and non-null on all 706 rows of the six seasons sampled (2012/13-2026/27). |
| `home_score` | Int64 | Points scored by the home team (games_games; Int64 on the wire). |
| `away_score` | Int64 | Points scored by the away team (games_games; Int64 on the wire). |
| `decided_in` | String | How the game was decided: null after regulation, otherwise the extra period (OT = overtime). |
| `info_game_id` | Int64 | Genius match id of the row (Genius key space). |
| `info_home_team_id` | Int64 | Genius team id of the home team (Genius key space). |
| `info_away_team_id` | Int64 | Genius team id of the away team (Genius key space). |
| `blk` | Int64 | Blocks (total). |
| `br` | Int64 | Blocks received (shots of the player that were blocked), total (read from BLKR in the box score; unverified). |
| `stl` | Int64 | Steals (total). |
| `plus` | Int64 | Team points scored while the player was on court (total; unverified). |
| `minus` | Int64 | Opponent points scored while the player was on court (total; unverified). |
| `fgm` | Int64 | Field goals made (total). |
| `fga` | Int64 | Field goals attempted (total). |
| `fg_perc` | Float64 | Field-goal percentage, 0-100. |
| `fge_perc` | Float64 | Effective field-goal percentage ((FGM + 0.5 x 3PM) / FGA), 0-100. |
| `3_pm` | Int64 | Three-pointers made (total). |
| `3_pa` | Int64 | Three-pointers attempted (total). |
| `2_pm` | Int64 | Two-pointers made (total). |
| `2_pa` | Int64 | Two-pointers attempted (total). |
| `pf` | Int64 | Personal fouls (total). |
| `tf` | Int64 | Technical fouls (total). |
| `of` | Int64 | Offensive fouls (total). |
| `f_on` | Float64 | Fouls on (fouls drawn), total. |
| `uf` | Int64 | Unsportsmanlike fouls (total). |
| `f_outs` | Int64 | Times fouled out. |
| `o_rat` | Float64 | Offensive rating (points produced per 100 possessions, the site's formula; unverified). |
| `tsa` | Int64 | True-shooting attempts (FGA + 0.44 x FTA), total. |
| `ts_perc` | Float64 | True-shooting percentage, 0-100. |
| `a` | Int64 | Assists (total). |
| `atr` | Float64 | Assist-to-turnover ratio. |
| `u_perc` | Float64 | Usage percentage, 0-100. |
| `egs` | Float64 | Game score per game (unverified). |
| `tp` | Int64 | Total points scored. |
| `fbp` | Int64 | Fast-break points (total). |
| `fbp_perc` | Float64 | Fast-break points as a percentage of the player's points (unverified). |
| `pitp` | Int64 | Points in the paint (total). |
| `pitp_perc` | Float64 | Points in the paint as a percentage of the player's points (unverified). |
| `2nd_ch_p` | Int64 | Second-chance points (total). |
| `2nd_ch_p_perc` | Float64 | Second-chance points as a percentage of the player's points (unverified). |
| `plus_minus` | Int64 | Plus/minus (team points minus opponent points while on court), total. |
| `dr` | Float64 | Defensive rebounds (total). |
| `or` | Int64 | Offensive rebounds (total). |
| `reb` | Int64 | Total rebounds. |
| `or_perc` | Float64 | Offensive rebound percentage; the denominator is the site's own (unverified). |
| `dr_perc` | Float64 | Defensive rebound percentage; values far below ORPerc in the same row suggest a different base (unverified). |
| `rank` | Int64 | Unobserved: null in every sample and every full body. When filled: Always null on this table: the rows are sorted, not ranked. |
| `l` | Int64 | Losses. Standings module: losses of the team in the ssgt. |
| `for` | Int64 | Points scored by the team (points for). |
| `ga` | Int64 | Points conceded. |
| `strk` | String | Current streak: a positive number = wins in a row, '-n' = losses in a row. |
| `gp_home` | Int64 | Home games played. |
| `att_total_overall` | Int64 | Total attendance over all games (the site's own sum). |
| `att_avg_overall` | Int64 | Average attendance per game. |
| `br_avg` | Float64 | Blocks received per game. |
| `dr_avg` | Float64 | Defensive rebounds per game. |
| `td` | Int64 | Takeaways or turnovers forced (unverified). |
| `td_avg` | Float64 | Per-game average of TD (unverified). |
| `fgm_avg` | Float64 | Field goals made per game. |
| `fga_avg` | Float64 | Field goals attempted per game. |
| `fg_perc_avg` | Float64 | Field-goal percentage per game average (values near 0.1 in the rows seen; unverified). |
| `3_pa_avg` | Float64 | Three-pointers attempted per game. |
| `3_pm_avg` | Float64 | Three-pointers made per game. |
| `2_pm_avg` | Float64 | Two-pointers made per game. |
| `2_pa_avg` | Float64 | Two-pointers attempted per game. |
| `fd` | Int64 | Fouls drawn (0 in every row seen; unverified). |
| `fd_avg` | Int64 | Fouls drawn per game (unverified, as FD). |
| `of_avg` | Float64 | Offensive fouls per game. |
| `f_on_avg` | Float64 | Fouls on per game. |
| `pf_avg` | Float64 | Personal fouls per game. |
| `tf_avg` | Float64 | Technical fouls per game. |
| `tfo` | Int64 | Technical fouls on the opponent (unverified). |
| `tfo_avg` | Int64 | Per-game average of TFO (unverified). |
| `uf_avg` | Float64 | Unsportsmanlike fouls per game. |
| `fo` | Int64 | Fouls-out count (unverified). |
| `fo_avg` | Int64 | Per-game average of FO (unverified). |
| `fcd` | Int64 | Coach disqualifying fouls (unverified). |
| `fcd_avg` | Int64 | Per-game average of FCD (unverified). |
| `fct` | Int64 | Coach technical fouls (read from the play-by-play foul subtype coachTechnical; unverified). |
| `fct_avg` | Float64 | Coach technical fouls per game. |
| `fbt` | Int64 | Bench technical fouls (read from the play-by-play foul subtype benchTechnical; unverified). |
| `fbt_avg` | Float64 | Bench technical fouls per game. |
| `f` | Int64 | Total personal fouls committed. |
| `f_team` | Int64 | Team fouls (unverified). |
| `f_team_avg` | Float64 | Team fouls per game (unverified). |
| `o_rat_avg` | Float64 | Offensive rating per game average. |
| `ad` | Int64 | Assists difference or assists allowed (0 in every row seen; unverified). |
| `ad_avg` | Int64 | Per-game average of AD (0 in every row seen; unverified). |
| `ec` | Float64 | Unverified (in the hundreds, e.g. '509.0'; possibly an efficiency sum). |
| `to` | Int64 | Turnovers (total). |
| `to_avg` | Float64 | Turnovers per game. |
| `fbp_avg` | Float64 | Fast-break points per game. |
| `fbpa` | Int64 | Fast-break points attempts (unverified). |
| `fbpm` | Int64 | Fast-break points made (unverified). |
| `pitp_avg` | Float64 | Points in the paint per game. |
| `pitpa` | Int64 | Paint shots attempted (unverified). |
| `pitpa_avg` | Float64 | Paint shots attempted per game (unverified). |
| `pitpm` | Int64 | Paint shots made (unverified). |
| `pitpm_avg` | Float64 | Paint shots made per game (unverified). |
| `pfto` | Int64 | Points from turnovers (total). |
| `pfto_avg` | Float64 | Points from turnovers per game. |
| `2nd_ch_pm` | Int64 | Second-chance field goals made (unverified). |
| `2nd_ch_pm_avg` | Float64 | Second-chance field goals made per game (unverified). |
| `2nd_ch_pa` | Int64 | Second-chance field goals attempted (unverified). |
| `2nd_ch_pa_avg` | Float64 | Second-chance field goals attempted per game (unverified). |
| `ftm` | Int64 | Free throws made (total). |
| `ftm_avg` | Float64 | Free throws made per game. |
| `fta` | Int64 | Free throws attempted (total). |
| `fta_avg` | Float64 | Free throws attempted per game. |
| `poss` | Int64 | Possessions (total). |
| `poss_avg` | Float64 | Possessions per game. |
| `poss_o` | Int64 | Offensive possessions (total; equal to Poss in every row seen). |
| `poss_o_avg` | Float64 | Offensive possessions per game. |
| `dppp` | Float64 | Defensive points per possession (unverified). |
| `dppp_avg` | Float64 | Defensive points per possession, per game average (unverified). |
| `oppp` | Float64 | Offensive points per possession. |
| `oppp_avg` | Float64 | Offensive points per possession, per game average. |
| `pace` | Int64 | Pace (possessions per 40 minutes; 0 in every row seen, unverified). |
| `pace_avg` | Int64 | Pace, per game average (0 in every row seen). |
| `tl` | Int64 | Team leads / lead changes (unverified). |
| `otr` | Int64 | Offensive team rebounds (unverified). |
| `dtr` | Int64 | Defensive team rebounds (unverified). |
| `tr` | Int64 | Total team rebounds (unverified). |
| `tr_avg` | Float64 | Team rebounds per game (unverified). |
| `pr` | Int64 | Player rebounds (total; Reb minus TR, unverified). |
| `pr_avg` | Float64 | Player rebounds per game (unverified). |
| `ord` | Int64 | Offensive rebounds allowed or the opponent's offensive rebounds (0 in every row seen; unverified). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_stats_info-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_stats_info-example}

```python
sportality_stats_info(module='players_summary', season=2026)
```
