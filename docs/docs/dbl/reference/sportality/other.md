---
title: "DBL — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) — Other"
sidebar_label: "Other"
sidebar_position: 5
description: "DBL — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# DBL — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) — Other

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
| `team_side` | character | home or away: the side the row belongs to. |
| `row_type` | character | Kind of row: player, team_coach (the bench row: team rebounds, turnovers and fouls charged to the team or coach) or team_totals. |
| `player_id` | integer | Genius person id (Int64). A string key in the players maps, an integer in play-by-play and box-score rows; cast here so the frames join, and join the genius family. |
| `team_id` | integer | Genius team id (Int64): in gameday bodies, an integer in standings and stats tables. |
| `team_name` | character | Name of the team. |
| `period` | integer | Period of the line: 0 = the whole game (the only value seen). |
| `full_name` | character | Full display name of the player. |
| `first_name` | character | Given (first) name(s) of the player. |
| `last_name` | character | Family name (surname) of the player. |
| `nr` | integer | Jersey number (null on the bench and totals rows). |
| `pos` | character | Position code of the line (G, F, C; null on the bench and totals rows). |
| `mins` | character | Minutes played as 'MM:SS' (null on the bench and totals rows). |
| `pts` | integer | Number of points scored. |
| `fg` | character | Field goals as 'made-attempted'. |
| `fg_perc` | numeric | Field-goal percentage, 0-100. |
| `2_p` | character | Two-pointers as 'made-attempted'. |
| `2_p_perc` | numeric | Two-point percentage, 0-100. |
| `3_p` | character | Three-pointers as 'made-attempted'. |
| `3_p_perc` | numeric | Three-point percentage, 0-100. |
| `ft` | character | Free throws as 'made-attempted'. |
| `ft_perc` | numeric | Free-throw percentage, 0-100. |
| `off` | integer | Offensive rebounds. |
| `def` | integer | Defensive rebounds. |
| `reb` | integer | Total rebounds. |
| `ast` | integer | Number of assists credited. |
| `to` | integer | Number of turnovers committed. |
| `stl` | integer | Number of steals. |
| `blk` | integer | Number of blocked shots made. |
| `blkr` | integer | Blocks received (shots of the player that were blocked). |
| `pf` | integer | Personal fouls. |
| `f_on` | integer | Fouls on (fouls drawn). |
| `plus_minus` | integer | Plus/minus while on court. |

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

_Last validated n/a._

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
| `game_uuid` | character | Platform game uuid (c4hh5nig6t); every gameday route takes it. |
| `start_date_time` | character | Scheduled start. On the schedule route this is the LOCAL (Europe/Copenhagen) wall time 'YYYY-MM-DD HH:MM:SS' with no zone; on the other routes it is ISO 8601 UTC with milliseconds (compare rawStartDateTime). |
| `state` | character | Game state. Schedule and day views: pre-game or post-game (hyphen). sports-v2 game-info: post_game (underscore). Gameday overview: GameEnded. Values: `post-game` = played (schedule, day views). |
| `overtime` | logical | Whether the game went to overtime. |
| `shootout` | character | Unobserved: null in every sample and every full body. When filled: Whether the game was decided by a shootout (null on every game seen; basketball has none). |
| `ssgt_uuid` | character | ssgt uuid (platform key space: one season + series + game type); joins /statistics-v2/layout-info seasonSeriesGameTypeTeams[].ssgtUuid and every route keyed by ssgt. |
| `home_team_status` | character | WIN or LOSE for the home team once the game is played; absent or empty before. Values: `LOSE` = the team lost; `WIN` = the team won. |
| `home_team_uuid` | character | Home team uuid (platform key space); joins /sports-v2/teams/{team_uuid}. |
| `home_team_owner_instance_id` | character | Site instance id of the club that owns the team (platform key space, e.g. hol2_hol). |
| `home_team_code` | character | Three-letter code of the home team. |
| `home_team_names_code` | character | Three-letter team code (HOL, RAN ...); codes changed between seasons for some clubs (BAK in 2026, BKA in 2025 for Bakken Bears). |
| `home_team_names_short` | character | Short name of the team. |
| `home_team_names_long` | character | Long name of the team. |
| `home_team_names_full` | character | Full name of the team. |
| `home_team_names_code_site` | character | Team code as the club's own site shows it. |
| `home_team_names_short_site` | character | Short team name as the club's own site shows it. |
| `home_team_names_full_site` | character | Full team name as the club's own site shows it. |
| `home_team_names_long_site` | character | Long team name as the club's own site shows it. |
| `home_team_score` | integer | Home points; null before the game is played. |
| `home_team_icon` | character | Home team logo URL. |
| `away_team_status` | character | WIN or LOSE for the away team once the game is played; absent or empty before. Values: `LOSE` = the team lost; `WIN` = the team won. |
| `away_team_uuid` | character | Away team uuid (platform key space); joins /sports-v2/teams/{team_uuid}. |
| `away_team_owner_instance_id` | character | Site instance id of the club that owns the team (platform key space, e.g. hol2_hol). |
| `away_team_code` | character | Three-letter code of the away team. |
| `away_team_names_code` | character | Three-letter team code (HOL, RAN ...); codes changed between seasons for some clubs (BAK in 2026, BKA in 2025 for Bakken Bears). |
| `away_team_names_short` | character | Short name of the team. |
| `away_team_names_long` | character | Long name of the team. |
| `away_team_names_full` | character | Full name of the team. |
| `away_team_names_code_site` | character | Team code as the club's own site shows it. |
| `away_team_names_short_site` | character | Short team name as the club's own site shows it. |
| `away_team_names_full_site` | character | Full team name as the club's own site shows it. |
| `away_team_names_long_site` | character | Long team name as the club's own site shows it. |
| `away_team_score` | integer | Away points; null before the game is played. |
| `away_team_icon` | character | Away team logo URL. |
| `venue_uuid` | character | Venue uuid (platform key space). |
| `venue_name` | character | Venue name; absent when unknown. |
| `series_uuid` | character | Series uuid (platform key space). |
| `series_code` | character | Series display name (Basketligaen; NOT the DBL code). |
| `series_display_name` | character | Series display name (Basketligaen). |

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

_Last validated n/a._

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
| `season` | integer | Season END year (2026 = 2025/26). |
| `season_code` | character | Provider season code: the START year (2025 = 2025/26). |
| `season_name` | character | Season name as 'YYYY/YYYY' (start/end year). |
| `game_type` | character | Game type of the game ('' on every line seen). |
| `game_type_name` | character | Game type name in Danish (Grundspil = regular season, Slutspil = playoffs). |
| `series_code` | character | Series code (DBL = Basketligaen). |
| `ssgt_uuid` | character | ssgt uuid (platform key space: one season + series + game type); joins /statistics-v2/layout-info seasonSeriesGameTypeTeams[].ssgtUuid and every route keyed by ssgt. |
| `state` | character | Game state. Schedule and day views: pre-game or post-game (hyphen). sports-v2 game-info: post_game (underscore). Gameday overview: GameEnded. Values: `active` = current ssgt; `closed` = past ssgt. |
| `season_uuid` | character | Season uuid (platform key space). |
| `series_uuid` | character | Series uuid (platform key space). |
| `game_type_uuid` | character | Game type uuid (platform key space). |

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

_Last validated n/a._

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
| `game_uuid` | character | Platform game uuid (c4hh5nig6t); every gameday route takes it. |
| `start_date_time` | character | Scheduled start. On the schedule route this is the LOCAL (Europe/Copenhagen) wall time 'YYYY-MM-DD HH:MM:SS' with no zone; on the other routes it is ISO 8601 UTC with milliseconds (compare rawStartDateTime). |
| `state` | character | Game state. Schedule and day views: pre-game or post-game (hyphen). sports-v2 game-info: post_game (underscore). Gameday overview: GameEnded. Values: `pre-game` = not yet played (schedule, day views). |
| `overtime` | logical | Whether the game went to overtime. |
| `shootout` | character | Unobserved: null in every sample and every full body. When filled: Whether the game was decided by a shootout (null on every game seen; basketball has none). |
| `ssgt_uuid` | character | ssgt uuid (platform key space: one season + series + game type); joins /statistics-v2/layout-info seasonSeriesGameTypeTeams[].ssgtUuid and every route keyed by ssgt. |
| `home_team_status` | character | WIN or LOSE for the home team once the game is played; absent or empty before. Values: `N/A` = no result yet / not applicable. |
| `home_team_uuid` | character | Home team uuid (platform key space); joins /sports-v2/teams/{team_uuid}. |
| `home_team_owner_instance_id` | character | Site instance id of the club that owns the team (platform key space, e.g. hol2_hol). |
| `home_team_code` | character | Three-letter code of the home team. |
| `home_team_names_code` | character | Three-letter team code (HOL, RAN ...); codes changed between seasons for some clubs (BAK in 2026, BKA in 2025 for Bakken Bears). |
| `home_team_names_short` | character | Short name of the team. |
| `home_team_names_long` | character | Long name of the team. |
| `home_team_names_full` | character | Full name of the team. |
| `home_team_names_code_site` | character | Team code as the club's own site shows it. |
| `home_team_names_short_site` | character | Short team name as the club's own site shows it. |
| `home_team_names_full_site` | character | Full team name as the club's own site shows it. |
| `home_team_names_long_site` | character | Long team name as the club's own site shows it. |
| `home_team_score` | integer | Home points; null before the game is played. |
| `home_team_icon` | character | Home team logo URL. |
| `away_team_status` | character | WIN or LOSE for the away team once the game is played; absent or empty before. Values: `N/A` = no result yet / not applicable. |
| `away_team_uuid` | character | Away team uuid (platform key space); joins /sports-v2/teams/{team_uuid}. |
| `away_team_owner_instance_id` | character | Site instance id of the club that owns the team (platform key space, e.g. hol2_hol). |
| `away_team_code` | character | Three-letter code of the away team. |
| `away_team_names_code` | character | Three-letter team code (HOL, RAN ...); codes changed between seasons for some clubs (BAK in 2026, BKA in 2025 for Bakken Bears). |
| `away_team_names_short` | character | Short name of the team. |
| `away_team_names_long` | character | Long name of the team. |
| `away_team_names_full` | character | Full name of the team. |
| `away_team_names_code_site` | character | Team code as the club's own site shows it. |
| `away_team_names_short_site` | character | Short team name as the club's own site shows it. |
| `away_team_names_full_site` | character | Full team name as the club's own site shows it. |
| `away_team_names_long_site` | character | Long team name as the club's own site shows it. |
| `away_team_score` | integer | Away points; null before the game is played. |
| `away_team_icon` | character | Away team logo URL. |
| `venue_uuid` | character | Venue uuid (platform key space). |
| `venue_name` | character | Venue name; absent when unknown. |
| `series_uuid` | character | Series uuid (platform key space). |
| `series_code` | character | Series display name (Basketligaen; NOT the DBL code). |
| `series_display_name` | character | Series display name (Basketligaen). |

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

_Last validated n/a._

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
| `game_source_id` | integer | Genius match id (Int64), the same number as game_id and sports-v2 game_info_ext_id. |
| `game_id` | integer | Genius match id (Int64), the join to the genius family; on play-by-play lines, an integer elsewhere. |
| `event_id` | integer | Genius event number within the game (Int64), increasing through the game. |
| `type` | character | Event type: one line per action; shots are the shooter's line, assists, blocks, steals and fouls drawn are further lines. Values: `2pt` = two-point attempt (made when isSuccessful = 1); `3pt` = three-point attempt (made when isSuccessful = 1); `assist` = assist on the previous made shot; `block` = block; `clock` = clock start, stop or adjustment; `foul` = foul committed; `foulon` = foul drawn (fouled player); `freethrow` = free-throw attempt; `game` = game start / end line; `headcoachchallenge` = head coach challenge; `jumpball` = jump ball or held ball; `period` = period start / end line; `rebound` = rebound; `steal` = steal; `substitution` = substitution (in or out); `timeout` = timeout; `turnover` = turnover. |
| `event_uuid` | character | Event uuid (UUID v5 derived from the event; unique per event). |
| `period` | integer | Period: 0 = the whole game, 1-4 the quarters, 5 and up overtime. |
| `period_type` | character | REGULAR or OVERTIME; absent on the period marker lines. Values: `OVERTIME` = overtime period; `REGULAR` = regulation period. |
| `time` | character | Clock REMAINING in the period as mm:ss:cc (cc looks like hundredths of a second, only ever 00, 10, 30, 50 ... seen; unverified). Absent on the per-period marker lines. |
| `real_world_time` | character | Wall-clock time of the event, ISO 8601 UTC. |
| `updated_time` | character | When the feed last updated the event, ISO 8601 UTC. |
| `home_team_id` | integer | Genius team id of the home team (Genius key space; Int64). |
| `home_team_code` | character | Home team code as Genius spells it (mixed case, 'Hol'). |
| `home_team_name` | character | Name of the home team. |
| `home_abbreviation` | character | Home team abbreviation. |
| `home_team_number` | integer | Team number in the game; home is 1. |
| `home_place` | character | Side of the home block: always home. |
| `home_score` | integer | Points scored by the home team. |
| `away_team_id` | integer | Genius team id of the away team (Genius key space; Int64). |
| `away_team_code` | character | Away team code as Genius spells it. |
| `away_team_name` | character | Name of the away team. |
| `away_abbreviation` | character | Away team abbreviation. |
| `away_team_number` | integer | Team number in the game; away is 2. |
| `away_place` | character | Side of the away block: always away. |
| `away_score` | integer | Points scored by the away team. |
| `revision` | integer | Revision counter of the event. |
| `game_state` | character | Game state at the event: ongoing during play, GameEnded on the final line. Values: `GameEnded` = game over (the final line of a complete feed); `ongoing` = game in progress. |
| `game_type` | character | Game type of the game ('' on every line seen). |
| `round` | integer | Round number (0). |
| `start_date_and_time` | character | Start of the game ('' on every line seen). |
| `sub_type` | character | Refinement of `type` (the shot type, rebound kind, foul kind, ...); null on types that have none. Values: `1of1` = free throw 1 of 1; `1of2` = free throw 1 of 2; `1of3` = free throw 1 of 3; `24sec` = shot-clock violation; `2of2` = free throw 2 of 2; `2of3` = free throw 2 of 3; `3of3` = free throw 3 of 3; `3sec` = three-second violation; `5sec` = five-second violation; `8sec` = eight-second violation; `adjustment` = clock adjustment; `alleyoop` = alley-oop shot; `alleyoopdunk` = alley-oop dunk; `backcourt` = backcourt violation; `badpass` = bad pass; `ballhandling` = ball-handling error; `benchTechnical` = technical foul on the bench; `coachTechnical` = technical foul on the coach; `defensive` = defensive rebound; `disqualifying` = disqualifying foul; `disruptive` = disruptive foul (unverified); `doubledribble` = double dribble; `drivinglayup` = driving layup; `dunk` = dunk; `end` = end of the period or game; `eurostep` = euro-step layup; `fadeaway` = fadeaway jump shot; `flagrant` = flagrant foul; `floatingjumpshot` = floating jump shot; `full` = full timeout; `heldball` = held ball; `hookshot` = hook shot; `in` = player enters; `jumpshot` = jump shot; `layup` = layup; `lodgedball` = lodged ball; `lost` = lost the jump ball; `offensive` = offensive rebound or offensive foul; `offensivegoaltending` = offensive goaltending; `officials` = official timeout; `other` = other; `out` = player leaves; `outofbounds` = out of bounds; `outofboundsrebound` = rebound off an out-of-bounds ball (unverified); `personal` = personal foul; `pullupjumpshot` = pull-up jump shot; `reverselayup` = reverse layup; `start` = start of the period or game, or clock start; `startperiod` = start of a period (jump ball line); `stepbackjumpshot` = step-back jump shot; `stop` = clock stop; `technical` = technical foul; `throwIn` = throw-in violation (unverified); `tipindunk` = tip-in dunk; `tipinlayup` = tip-in layup; `travel` = traveling; `turnaroundjumpshot` = turnaround jump shot; `unsportsmanlike` = unsportsmanlike foul; `won` = won the jump ball. |
| `qualifiers` | character | Event qualifiers joined by ';' (a list in recent games, a ';'-joined string or null in older ones). |
| `is_successful` | integer | 1 when the action succeeded (a made shot or free throw, a recorded rebound ...). ABSENT, not 0, for a missed shot, so count misses as rows without it. |
| `instance_id` | character | Site instance id of the club or league (platform key space, '<code><n>_<code>' such as hol2_hol; the league itself is dbl1_dbl). |
| `source` | character | Feed that produced the line (genius-basketball-stream-reader). |
| `game_uuid` | character | Platform game uuid (c4hh5nig6t); every gameday route takes it. |
| `event_team_id` | integer | Genius team id (Genius key space; Int64). |
| `event_team_code` | character | Team code as Genius spells it. |
| `event_team_name` | character | Name of the team. |
| `event_abbreviation` | character | Team abbreviation. |
| `event_team_number` | integer | Team number in the game (1 home, 2 away). |
| `event_place` | character | Side of the team the event belongs to: home or away. |
| `player_id` | integer | Genius person id (Int64). A string key in the players maps, an integer in play-by-play and box-score rows; cast here so the frames join, and join the genius family. |
| `player_pno` | integer | Genius shirt-table number of the player. |
| `player_team_number` | integer | Team number in the game (1 home, 2 away). |
| `player_first_name` | character | Given (first) name(s) of the player. |
| `player_family_name` | character | Family name (this route says familyName; the others lastName). |
| `coordinates_x` | numeric | Shot or event x coordinate in Genius court units (Float64); origin and scale unverified. |
| `coordinates_y` | numeric | Shot or event y coordinate in Genius court units (Float64); origin and orientation unverified. |
| `period_time` | integer | Marker lines only: 0 (an integer), the start of the period. |

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

_Last validated n/a._

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
| `home_team_side_translate_key` | character | Translation key of the comparison row's label (s_2P, s_3P, s_FT, ...); the site translates it. |
| `home_team_left_type` | character | Stat code of the left value (2PM = two-pointers made). |
| `home_team_left_value` | integer | Value of the left-hand statistic of the comparison row. |
| `home_team_right_type` | character | Stat code of the right value (2PA = two-pointers attempted). |
| `home_team_right_value` | integer | Value of the right-hand statistic of the comparison row. |
| `home_team_center_type` | character | Stat code of the centre value (2PPerc = two-point percentage). |
| `home_team_center_value` | integer | Centre value (percentage as an integer 0-100). |
| `away_team_side_translate_key` | character | Translation key of the comparison row's label (s_2P, s_3P, s_FT, ...); the site translates it. |
| `away_team_left_type` | character | Stat code of the left value (2PM = two-pointers made). |
| `away_team_left_value` | integer | Value of the left-hand statistic of the comparison row. |
| `away_team_right_type` | character | Stat code of the right value (2PA = two-pointers attempted). |
| `away_team_right_value` | integer | Value of the right-hand statistic of the comparison row. |
| `away_team_center_type` | character | Stat code of the centre value (2PPerc = two-point percentage). |
| `away_team_center_value` | integer | Centre value (percentage as an integer 0-100). |

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

_Last validated n/a._

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
| `position` | character | Playing position. |
| `position_code` | character | Position code (G, F, C, or a finer code such as SG on the profile route). Values: `C` = center; `F` = forward; `G` = guard. |
| `player_type` | character | Record type (athlete). Values: `athlete` = a player. |
| `gender` | character | Gender of the person as recorded by the site (male in every row seen). |
| `uuid` | character | Athlete uuid (platform key space). |
| `first_name` | character | Given (first) name(s) of the player. |
| `last_name` | character | Family name (surname) of the player. |
| `full_name` | character | Full display name of the player. |
| `nationality` | character | ISO 3166-1 alpha-2 country code. |
| `jersey_number` | integer | Jersey (shirt) number of the player. |
| `portrait_url` | character | URL of the player's latest rendered portrait. |

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

_Last validated n/a._

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
| `game_uuid` | character | Platform game uuid (c4hh5nig6t); every gameday route takes it. |
| `raw_start_date_time` | character | Scheduled start, ISO 8601 UTC. |
| `start_date_time` | character | Scheduled start. On the schedule route this is the LOCAL (Europe/Copenhagen) wall time 'YYYY-MM-DD HH:MM:SS' with no zone; on the other routes it is ISO 8601 UTC with milliseconds (compare rawStartDateTime). |
| `state` | character | Game state. Schedule and day views: pre-game or post-game (hyphen). sports-v2 game-info: post_game (underscore). Gameday overview: GameEnded. Values: `post-game` = played (schedule, day views); `pre-game` = not yet played (schedule, day views). |
| `overtime` | logical | Whether the game went to overtime. |
| `shootout` | character | Unobserved: null in every sample and every full body. When filled: Whether the game was decided by a shootout (null on every game seen; basketball has none). |
| `ssgt_uuid` | character | ssgt uuid (platform key space: one season + series + game type); joins /statistics-v2/layout-info seasonSeriesGameTypeTeams[].ssgtUuid and every route keyed by ssgt. |
| `home_team_status` | character | WIN or LOSE for the home team once the game is played; absent or empty before. Values: `LOSE` = the team lost; `N/A` = no result yet / not applicable; `WIN` = the team won. |
| `home_team_uuid` | character | Home team uuid (platform key space); joins /sports-v2/teams/{team_uuid}. |
| `home_team_owner_instance_id` | character | Site instance id of the club that owns the team (platform key space, e.g. hol2_hol). |
| `home_team_code` | character | Three-letter code of the home team. |
| `home_team_names_code` | character | Three-letter team code (HOL, RAN ...); codes changed between seasons for some clubs (BAK in 2026, BKA in 2025 for Bakken Bears). |
| `home_team_names_short` | character | Short name of the team. |
| `home_team_names_long` | character | Long name of the team. |
| `home_team_names_full` | character | Full name of the team. |
| `home_team_names_code_site` | character | Team code as the club's own site shows it. |
| `home_team_names_short_site` | character | Short team name as the club's own site shows it. |
| `home_team_names_full_site` | character | Full team name as the club's own site shows it. |
| `home_team_names_long_site` | character | Long team name as the club's own site shows it. |
| `home_team_score` | integer | Home points; null before the game is played. |
| `home_team_icon` | character | Home team logo URL. |
| `away_team_status` | character | WIN or LOSE for the away team once the game is played; absent or empty before. Values: `LOSE` = the team lost; `N/A` = no result yet / not applicable; `WIN` = the team won. |
| `away_team_uuid` | character | Away team uuid (platform key space); joins /sports-v2/teams/{team_uuid}. |
| `away_team_owner_instance_id` | character | Site instance id of the club that owns the team (platform key space, e.g. hol2_hol). |
| `away_team_code` | character | Three-letter code of the away team. |
| `away_team_names_code` | character | Three-letter team code (HOL, RAN ...); codes changed between seasons for some clubs (BAK in 2026, BKA in 2025 for Bakken Bears). |
| `away_team_names_short` | character | Short name of the team. |
| `away_team_names_long` | character | Long name of the team. |
| `away_team_names_full` | character | Full name of the team. |
| `away_team_names_code_site` | character | Team code as the club's own site shows it. |
| `away_team_names_short_site` | character | Short team name as the club's own site shows it. |
| `away_team_names_full_site` | character | Full team name as the club's own site shows it. |
| `away_team_names_long_site` | character | Long team name as the club's own site shows it. |
| `away_team_score` | integer | Away points; null before the game is played. |
| `away_team_icon` | character | Away team logo URL. |
| `venue_uuid` | character | Venue uuid (platform key space). |
| `venue_name` | character | Venue name; absent when unknown. |
| `series_uuid` | character | Series uuid (platform key space). |
| `series_code` | character | Series display name (Basketligaen; NOT the DBL code). |
| `series_display_name` | character | Series display name (Basketligaen). |
| `round_number` | integer | Round number (0 for every game seen; the league has no rounds). |
| `round_label` | character | Round label ('' for every game seen). |

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

_Last validated n/a._

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
| `kind` | character | Directory entry kind: series, season or game_type. |
| `code` | character | Series code (DBL = Basketligaen). |
| `name` | character | Game type name in Danish (Grundspil = regular season, Slutspil = playoffs). |
| `uuid` | character | Series uuid (platform key space); the seriesUuid of /sports-v2/game-schedule. |
| `deleted` | logical | Soft-delete flag; false on every record seen. |

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

_Last validated n/a._

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
| `id` | character | Site instance id of the club (platform key space; the same as instanceId). |
| `instance_id` | character | Site instance id of the club or league (platform key space, '<code><n>_<code>' such as hol2_hol; the league itself is dbl1_dbl). |
| `public_url` | character | The club's own website. |
| `name` | character | Display name of the club. |
| `icon` | character | Team logo URL (SVG on sportality.cdn.s8y.se). |

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

_Last validated n/a._

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
| `provider` | character | Stats provider of the ssgt (genius-basketball, a Genius Sports LiveStats relay). |

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

_Last validated n/a._

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
| `team_id` | integer | Genius team id (Int64): in gameday bodies, an integer in standings and stats tables. |
| `team_uuid` | character | Platform team uuid. |
| `team_code` | character | Platform team code (HOL, RAN). |
| `team_name` | character | Name of the team. |
| `rank` | integer | Rank in the standings table, 1 = first. |
| `gp` | integer | Number of games played. |
| `w` | integer | Number of wins. |
| `l` | integer | Number of losses. |
| `tp` | integer | League table points (2 per win). |
| `for` | integer | Points scored by the team (points for). |
| `ga` | integer | Points conceded. |
| `plus_minus` | integer | Point difference. |
| `strk` | integer | Current streak as a signed integer: positive = wins in a row, negative = losses in a row. |

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

_Last validated n/a._

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
| `player_id` | integer | Genius person id (Int64). A string key in the players maps, an integer in play-by-play and box-score rows; cast here so the frames join, and join the genius family. |
| `gp` | integer | Number of games played. |
| `eff` | numeric | Efficiency (valuation) per game; the formula is the site's own (unverified). |
| `mins` | numeric | Average minutes played per game ('27.3'). |
| `ppg` | numeric | Points per game. |
| `rpg` | numeric | Rebounds per game. |
| `apg` | numeric | Assists per game. |
| `stpg` | numeric | Steals per game. |
| `blkpg` | numeric | Blocks per game. |
| `info_uuid` | character | Athlete uuid on player tables, team uuid on team tables (platform key space). |
| `info_full_name` | character | Full display name of the player. |
| `info_first_name` | character | Given (first) name(s) of the player. |
| `info_last_name` | character | Family name (surname) of the player. |
| `info_nationality` | character | ISO 3166-1 alpha-2 country code. |
| `info_shoots` | character | Shooting hand (null). |
| `info_gender` | character | Gender of the person as recorded by the site (male in every row seen). |
| `info_weight_value` | integer | Weight of the player in kilograms. |
| `info_weight_format` | character | Unit of the value (kg). Values: `kg` = kilograms. |
| `info_height_value` | integer | Height of the player in centimetres. |
| `info_height_format` | character | Unit of the value (cm). Values: `cm` = centimetres. |
| `info_team_uuid` | character | Team uuid (platform key space); joins /sports-v2/teams/{team_uuid}. |
| `info_team_name` | character | Name of the team. |
| `info_team_code` | character | Three-letter team code. |
| `info_team_media` | character | URL of the team logo image. |
| `info_team_owner_instance_id` | character | Site instance id of the club that owns the team (platform key space, e.g. hol2_hol). |
| `info_team_site_display_name` | character | Team name as the league site displays it. |
| `info_team_id` | integer | Genius team id (Genius key space; an integer here). |
| `info_number` | integer | Jersey (shirt) number of the player. |
| `info_position` | character | Position code (G, F, C, SG, SF ...). |
| `w` | integer | Wins. Team and standings modules: wins of the team in the ssgt. |
| `p_avg` | numeric | Points per game. |
| `r_avg` | numeric | Rebounds per game. |
| `a_avg` | numeric | Assists per game. |
| `s_avg` | numeric | Steals per game. |
| `b_avg` | numeric | Blocks per game. |
| `f_avg` | numeric | Fouls per game. |
| `2_p_perc` | numeric | Two-point percentage, 0-100. |
| `3_p_perc` | numeric | Three-point percentage, 0-100. |
| `ft_perc` | numeric | Free-throw percentage, 0-100. |
| `info_name` | character | Name of the team. |
| `info_site_display_name` | character | Team name as the league site displays it. |
| `info_owner_instance_id` | character | Site instance id of the club that owns the team (platform key space, e.g. hol2_hol). |
| `date_time` | character | Start time 'YYYY-MM-DD HH:MM:SS', LOCAL (Europe/Copenhagen) wall time with no zone. |
| `home_team` | character | Name of the home team. |
| `away_team` | character | Name of the away team. |
| `venue` | character | Name of the venue the game was played in. |
| `attendance` | integer | Attendance as an integer; present and non-null on all 706 rows of the six seasons sampled (2012/13-2026/27). |
| `home_score` | integer | Points scored by the home team (games_games; Int64 on the wire). |
| `away_score` | integer | Points scored by the away team (games_games; Int64 on the wire). |
| `decided_in` | character | How the game was decided: null after regulation, otherwise the extra period (OT = overtime). |
| `info_game_id` | integer | Genius match id of the row (Genius key space). |
| `info_home_team_id` | integer | Genius team id of the home team (Genius key space). |
| `info_away_team_id` | integer | Genius team id of the away team (Genius key space). |
| `blk` | integer | Blocks (total). |
| `br` | integer | Blocks received (shots of the player that were blocked), total (read from BLKR in the box score; unverified). |
| `stl` | integer | Steals (total). |
| `plus` | integer | Team points scored while the player was on court (total; unverified). |
| `minus` | integer | Opponent points scored while the player was on court (total; unverified). |
| `fgm` | integer | Field goals made (total). |
| `fga` | integer | Field goals attempted (total). |
| `fg_perc` | numeric | Field-goal percentage, 0-100. |
| `fge_perc` | numeric | Effective field-goal percentage ((FGM + 0.5 x 3PM) / FGA), 0-100. |
| `3_pm` | integer | Three-pointers made (total). |
| `3_pa` | integer | Three-pointers attempted (total). |
| `2_pm` | integer | Two-pointers made (total). |
| `2_pa` | integer | Two-pointers attempted (total). |
| `pf` | integer | Personal fouls (total). |
| `tf` | integer | Technical fouls (total). |
| `of` | integer | Offensive fouls (total). |
| `f_on` | numeric | Fouls on (fouls drawn), total. |
| `uf` | integer | Unsportsmanlike fouls (total). |
| `f_outs` | integer | Times fouled out. |
| `o_rat` | numeric | Offensive rating (points produced per 100 possessions, the site's formula; unverified). |
| `tsa` | integer | True-shooting attempts (FGA + 0.44 x FTA), total. |
| `ts_perc` | numeric | True-shooting percentage, 0-100. |
| `a` | integer | Assists (total). |
| `atr` | numeric | Assist-to-turnover ratio. |
| `u_perc` | numeric | Usage percentage, 0-100. |
| `egs` | numeric | Game score per game (unverified). |
| `tp` | integer | Total points scored. |
| `fbp` | integer | Fast-break points (total). |
| `fbp_perc` | numeric | Fast-break points as a percentage of the player's points (unverified). |
| `pitp` | integer | Points in the paint (total). |
| `pitp_perc` | numeric | Points in the paint as a percentage of the player's points (unverified). |
| `2nd_ch_p` | integer | Second-chance points (total). |
| `2nd_ch_p_perc` | numeric | Second-chance points as a percentage of the player's points (unverified). |
| `plus_minus` | integer | Plus/minus (team points minus opponent points while on court), total. |
| `dr` | numeric | Defensive rebounds (total). |
| `or` | integer | Offensive rebounds (total). |
| `reb` | integer | Total rebounds. |
| `or_perc` | numeric | Offensive rebound percentage; the denominator is the site's own (unverified). |
| `dr_perc` | numeric | Defensive rebound percentage; values far below ORPerc in the same row suggest a different base (unverified). |
| `rank` | integer | Unobserved: null in every sample and every full body. When filled: Always null on this table: the rows are sorted, not ranked. |
| `l` | integer | Losses. Standings module: losses of the team in the ssgt. |
| `for` | integer | Points scored by the team (points for). |
| `ga` | integer | Points conceded. |
| `strk` | character | Current streak: a positive number = wins in a row, '-n' = losses in a row. |
| `gp_home` | integer | Home games played. |
| `att_total_overall` | integer | Total attendance over all games (the site's own sum). |
| `att_avg_overall` | integer | Average attendance per game. |
| `br_avg` | numeric | Blocks received per game. |
| `dr_avg` | numeric | Defensive rebounds per game. |
| `td` | integer | Takeaways or turnovers forced (unverified). |
| `td_avg` | numeric | Per-game average of TD (unverified). |
| `fgm_avg` | numeric | Field goals made per game. |
| `fga_avg` | numeric | Field goals attempted per game. |
| `fg_perc_avg` | numeric | Field-goal percentage per game average (values near 0.1 in the rows seen; unverified). |
| `3_pa_avg` | numeric | Three-pointers attempted per game. |
| `3_pm_avg` | numeric | Three-pointers made per game. |
| `2_pm_avg` | numeric | Two-pointers made per game. |
| `2_pa_avg` | numeric | Two-pointers attempted per game. |
| `fd` | integer | Fouls drawn (0 in every row seen; unverified). |
| `fd_avg` | integer | Fouls drawn per game (unverified, as FD). |
| `of_avg` | numeric | Offensive fouls per game. |
| `f_on_avg` | numeric | Fouls on per game. |
| `pf_avg` | numeric | Personal fouls per game. |
| `tf_avg` | numeric | Technical fouls per game. |
| `tfo` | integer | Technical fouls on the opponent (unverified). |
| `tfo_avg` | integer | Per-game average of TFO (unverified). |
| `uf_avg` | numeric | Unsportsmanlike fouls per game. |
| `fo` | integer | Fouls-out count (unverified). |
| `fo_avg` | integer | Per-game average of FO (unverified). |
| `fcd` | integer | Coach disqualifying fouls (unverified). |
| `fcd_avg` | integer | Per-game average of FCD (unverified). |
| `fct` | integer | Coach technical fouls (read from the play-by-play foul subtype coachTechnical; unverified). |
| `fct_avg` | numeric | Coach technical fouls per game. |
| `fbt` | integer | Bench technical fouls (read from the play-by-play foul subtype benchTechnical; unverified). |
| `fbt_avg` | numeric | Bench technical fouls per game. |
| `f` | integer | Total personal fouls committed. |
| `f_team` | integer | Team fouls (unverified). |
| `f_team_avg` | numeric | Team fouls per game (unverified). |
| `o_rat_avg` | numeric | Offensive rating per game average. |
| `ad` | integer | Assists difference or assists allowed (0 in every row seen; unverified). |
| `ad_avg` | integer | Per-game average of AD (0 in every row seen; unverified). |
| `ec` | numeric | Unverified (in the hundreds, e.g. '509.0'; possibly an efficiency sum). |
| `to` | integer | Turnovers (total). |
| `to_avg` | numeric | Turnovers per game. |
| `fbp_avg` | numeric | Fast-break points per game. |
| `fbpa` | integer | Fast-break points attempts (unverified). |
| `fbpm` | integer | Fast-break points made (unverified). |
| `pitp_avg` | numeric | Points in the paint per game. |
| `pitpa` | integer | Paint shots attempted (unverified). |
| `pitpa_avg` | numeric | Paint shots attempted per game (unverified). |
| `pitpm` | integer | Paint shots made (unverified). |
| `pitpm_avg` | numeric | Paint shots made per game (unverified). |
| `pfto` | integer | Points from turnovers (total). |
| `pfto_avg` | numeric | Points from turnovers per game. |
| `2nd_ch_pm` | integer | Second-chance field goals made (unverified). |
| `2nd_ch_pm_avg` | numeric | Second-chance field goals made per game (unverified). |
| `2nd_ch_pa` | integer | Second-chance field goals attempted (unverified). |
| `2nd_ch_pa_avg` | numeric | Second-chance field goals attempted per game (unverified). |
| `ftm` | integer | Free throws made (total). |
| `ftm_avg` | numeric | Free throws made per game. |
| `fta` | integer | Free throws attempted (total). |
| `fta_avg` | numeric | Free throws attempted per game. |
| `poss` | integer | Possessions (total). |
| `poss_avg` | numeric | Possessions per game. |
| `poss_o` | integer | Offensive possessions (total; equal to Poss in every row seen). |
| `poss_o_avg` | numeric | Offensive possessions per game. |
| `dppp` | numeric | Defensive points per possession (unverified). |
| `dppp_avg` | numeric | Defensive points per possession, per game average (unverified). |
| `oppp` | numeric | Offensive points per possession. |
| `oppp_avg` | numeric | Offensive points per possession, per game average. |
| `pace` | integer | Pace (possessions per 40 minutes; 0 in every row seen, unverified). |
| `pace_avg` | integer | Pace, per game average (0 in every row seen). |
| `tl` | integer | Team leads / lead changes (unverified). |
| `otr` | integer | Offensive team rebounds (unverified). |
| `dtr` | integer | Defensive team rebounds (unverified). |
| `tr` | integer | Total team rebounds (unverified). |
| `tr_avg` | numeric | Team rebounds per game (unverified). |
| `pr` | integer | Player rebounds (total; Reb minus TR, unverified). |
| `pr_avg` | numeric | Player rebounds per game (unverified). |
| `ord` | integer | Offensive rebounds allowed or the opponent's offensive rebounds (0 in every row seen; unverified). |

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

_Last validated n/a._
