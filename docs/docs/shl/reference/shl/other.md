---
title: "SHL — SHL site API (www.shl.se/api, Sweden) — Other: boxscore–ssgt_provider"
sidebar_label: "Other: boxscore–ssgt_provider"
sidebar_position: 4
description: "SHL — SHL site API (www.shl.se/api, Sweden) — Other: boxscore–ssgt_provider — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# SHL — SHL site API (www.shl.se/api, Sweden) — Other: boxscore–ssgt_provider

## shl_boxscore

Skater and goalie box score per team (2023/24 on).

**Endpoint URL:** `GET https://www.shl.se/api/gameday/boxscore/{game_uuid}`

**Valid URL:** [https://www.shl.se/api/gameday/boxscore/bdhvuc5tex](https://www.shl.se/api/gameday/boxscore/bdhvuc5tex)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_uuid` | `game_uuid` |  | `Y` |  | Game uuid (gameInfo[].uuid from /sports-v2/game-schedule). gameday/* has data from 2023/24 on; older games answer 200 with an empty body. |

### Returns {#shl_boxscore-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_side` | character | Side of the game: home or away. |
| `position_group` | character | skater or goalie (goalies come from the body's gkStats rows). |
| `player_id` | integer | isa (StatNet) player id, Int64. Play-by-play sends it as a string, every other route as an integer; it is cast here so the frames join. |
| `team_id` | character | Team code (e.g. FHC) in gameday and standings payloads; not the team uuid. |
| `period` | integer | Period: 0 = the whole game, 1-3 the periods, 4 and up overtime. |
| `full_name` | character | Given name and surname. |
| `first_name` | character | Given (first) name of the player. |
| `last_name` | character | Surname (family name) of the player. |
| `nr` | integer | Jersey (shirt) number of the player. |
| `pos` | character | Position code in this game (e.g. CE, LW, RW, D, GK). |
| `line` | integer | Line (unit) number the player was listed on. |
| `g` | numeric | Number of goals scored. |
| `a` | numeric | Number of assists credited. |
| `ppg` | numeric | Power-play goals. |
| `sw` | numeric | Number of shots that missed the net. |
| `pim` | numeric | Penalty minutes. |
| `sog` | numeric | Number of shots on goal. |
| `ppsog` | numeric | Power-play shots on goal. |
| `plus_minus` | numeric | Plus/minus in the game. |
| `toi` | character | Time on ice (mm:ss). Clock/duration as an mm:ss string. |
| `hits` | numeric | Body checks delivered. |
| `fow` | numeric | Number of face-offs won. |
| `fol` | numeric | Face-offs lost. |
| `fo_perc` | numeric | Face-off win %. |
| `ga` | numeric | Number of goals conceded (goals against). |
| `soga` | numeric | Shots on goal against. |
| `spga` | numeric | Unverified goalie column (box score). |
| `svs` | numeric | Number of saves made by the goalie. |
| `svs_perc` | numeric | Save percentage of the game as a FRACTION 0-1 (e.g. '0.9655...'); integer 0 for a goalie without shots. Numeric, but sent as a string in some rows and as a number in others. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_boxscore-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_boxscore-example}

```python
shl_boxscore(game_uuid='bdhvuc5tex')
```

_Last validated n/a._

## shl_last_game_day

Games of the last played match day of an ssgt.

**Endpoint URL:** `GET https://www.shl.se/api/sports-v2/last-game-day`

**Valid URL:** [https://www.shl.se/api/sports-v2/last-game-day?gameType=regular](https://www.shl.se/api/sports-v2/last-game-day?gameType=regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025/26), mapped to its ssgt through layout-info. None = the active ssgt (these match-day views follow the current season). |
| `gameType` | `game_type` |  |  | `Y` | Game type: regular (Grundserie), playoff (Slutspel), qualdown (relegation qualification) or playin (2023/24 on). |

### Returns {#shl_last_game_day-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_uuid` | character | Platform game uuid (bdhvuc5tex); every gameday route takes it. |
| `start_date_time` | character | Scheduled start, local time (Europe/Stockholm). |
| `state` | character | Game state: pre-game or post-game (hyphenated; a live value was not observed). |
| `overtime` | logical | Whether the game went to overtime. |
| `shootout` | logical | Whether the game went to a shootout. |
| `ssgt_uuid` | character | ssgt uuid (platform key space: one season + series + game type); label via layout-info seasonSeriesGameTypeTeams[]. |
| `home_team_status` | character | Result for the team (WIN, LOSE) or record status. |
| `home_team_uuid` | character | Team uuid (platform key space, e.g. 087a-087aTQv9u); joins to /sports-v2/teams/{team_uuid} and schedule homeTeamInfo/awayTeamInfo.uuid. Not the team code. |
| `home_team_owner_instance_id` | character | Club site instance id (e.g. fhc1_fhc); also names the logo at sportality.cdn.s8y.se/team-logos/{id}.svg. |
| `home_team_code` | character | Home team code (e.g. FHC); joins to gameday teamId and the code-keyed teams{} maps. |
| `home_team_names_code` | character | Display abbreviation; differs from the team code for some clubs (SKE vs SAIK, ÖRE vs OHK). |
| `home_team_names_short` | character | Short club name ('Herr' = men's team). |
| `home_team_names_long` | character | Long club name ('Herr' = men's team). |
| `home_team_names_full` | character | Full club name ('Herr' = men's team). |
| `home_team_names_code_site` | character | Display abbreviation on the site; 'Herr' = men's team (e.g. 'DIF Herr'). |
| `home_team_names_short_site` | character | Short club name as shown on the site ('Herr' = men's team). |
| `home_team_names_full_site` | character | Full club name as shown on the site ('Herr' = men's team). |
| `home_team_names_long_site` | character | Long club name as shown on the site ('Herr' = men's team). |
| `home_team_score` | integer | Goals scored by the team in the game. |
| `home_team_icon` | character | URL of the team logo image. |
| `away_team_status` | character | Result for the team (WIN, LOSE) or record status. |
| `away_team_uuid` | character | Team uuid (platform key space, e.g. 087a-087aTQv9u); joins to /sports-v2/teams/{team_uuid} and schedule homeTeamInfo/awayTeamInfo.uuid. Not the team code. |
| `away_team_owner_instance_id` | character | Club site instance id (e.g. fhc1_fhc); also names the logo at sportality.cdn.s8y.se/team-logos/{id}.svg. |
| `away_team_code` | character | Away team code (e.g. LHC); joins to gameday teamId and the code-keyed teams{} maps. |
| `away_team_names_code` | character | Display abbreviation; differs from the team code for some clubs (SKE vs SAIK, ÖRE vs OHK). |
| `away_team_names_short` | character | Short club name ('Herr' = men's team). |
| `away_team_names_long` | character | Long club name ('Herr' = men's team). |
| `away_team_names_full` | character | Full club name ('Herr' = men's team). |
| `away_team_names_code_site` | character | Display abbreviation on the site; 'Herr' = men's team (e.g. 'DIF Herr'). |
| `away_team_names_short_site` | character | Short club name as shown on the site ('Herr' = men's team). |
| `away_team_names_full_site` | character | Full club name as shown on the site ('Herr' = men's team). |
| `away_team_names_long_site` | character | Long club name as shown on the site ('Herr' = men's team). |
| `away_team_score` | integer | Goals scored by the team in the game. |
| `away_team_icon` | character | URL of the team logo image. |
| `venue_uuid` | character | Arena uuid (platform key space). |
| `venue_name` | character | Name of the arena (venue). |
| `series_uuid` | character | Series uuid (platform key space; SHL = qQ9-bb0bzEWUk). |
| `series_code` | character | Series code (SHL, CHL). |
| `series_display_name` | character | Series label shown on the site (SHL, CHL). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_last_game_day-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_last_game_day-example}

```python
shl_last_game_day()
```

_Last validated n/a._

## shl_layout_info

Directory of every SHL ssgt (season + series + game type) with teams and the stats modules.

**Endpoint URL:** `GET https://www.shl.se/api/statistics-v2/layout-info`

**Valid URL:** [https://www.shl.se/api/statistics-v2/layout-info](https://www.shl.se/api/statistics-v2/layout-info)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#shl_layout_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025/26). |
| `season_code` | character | Provider season code: the START year as a string (2025 = 2025/26). |
| `season_name` | character | Season label (e.g. 2025/2026). |
| `game_type` | character | Game type record (uuid, code regular / playoff / qualdown, Swedish name). |
| `game_type_name` | character | Game type label in Swedish: Grundserie = regular season, Slutspel = playoffs, Kvalspel nedflyttning = relegation qualification, Kvalserien = the old promotion/relegation series. |
| `series_code` | character | Series code (SHL, CHL; HA and Kvalserien in old career logs). |
| `ssgt_uuid` | character | ssgt uuid (platform key space: one season + series + game type); label via layout-info seasonSeriesGameTypeTeams[]. |
| `state` | character | ssgt state: active (the current season) or closed. |
| `season_uuid` | character | Season uuid (platform key space; code 2025 = 2025/2026); the seasonUuid param of game-schedule. |
| `series_uuid` | character | Series uuid (platform key space; SHL = qQ9-bb0bzEWUk). |
| `game_type_uuid` | character | Game type uuid (platform key space; regular qQ9-af37Ti40B); the gameTypeUuid param of game-schedule. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_layout_info-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_layout_info-example}

```python
shl_layout_info()
```

_Last validated n/a._

## shl_next_game_day

Games of the next match day of an ssgt.

**Endpoint URL:** `GET https://www.shl.se/api/sports-v2/next-game-day`

**Valid URL:** [https://www.shl.se/api/sports-v2/next-game-day?gameType=regular](https://www.shl.se/api/sports-v2/next-game-day?gameType=regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025/26), mapped to its ssgt through layout-info. None = the active ssgt (these match-day views follow the current season). |
| `gameType` | `game_type` |  |  | `Y` | Game type: regular (Grundserie), playoff (Slutspel), qualdown (relegation qualification) or playin (2023/24 on). |

### Returns {#shl_next_game_day-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_uuid` | character | Platform game uuid (bdhvuc5tex); every gameday route takes it. |
| `start_date_time` | character | Scheduled start, local time (Europe/Stockholm). |
| `state` | character | Game state: pre-game or post-game (hyphenated; a live value was not observed). |
| `overtime` | logical | Whether the game went to overtime. |
| `shootout` | logical | Whether the game went to a shootout. |
| `ssgt_uuid` | character | ssgt uuid (platform key space: one season + series + game type); label via layout-info seasonSeriesGameTypeTeams[]. |
| `home_team_status` | character | Result for the team (WIN, LOSE) or record status. |
| `home_team_uuid` | character | Team uuid (platform key space, e.g. 087a-087aTQv9u); joins to /sports-v2/teams/{team_uuid} and schedule homeTeamInfo/awayTeamInfo.uuid. Not the team code. |
| `home_team_owner_instance_id` | character | Club site instance id (e.g. fhc1_fhc); also names the logo at sportality.cdn.s8y.se/team-logos/{id}.svg. |
| `home_team_code` | character | Home team code (e.g. FHC); joins to gameday teamId and the code-keyed teams{} maps. |
| `home_team_names_code` | character | Display abbreviation; differs from the team code for some clubs (SKE vs SAIK, ÖRE vs OHK). |
| `home_team_names_short` | character | Short club name ('Herr' = men's team). |
| `home_team_names_long` | character | Long club name ('Herr' = men's team). |
| `home_team_names_full` | character | Full club name ('Herr' = men's team). |
| `home_team_names_code_site` | character | Display abbreviation on the site; 'Herr' = men's team (e.g. 'DIF Herr'). |
| `home_team_names_short_site` | character | Short club name as shown on the site ('Herr' = men's team). |
| `home_team_names_full_site` | character | Full club name as shown on the site ('Herr' = men's team). |
| `home_team_names_long_site` | character | Long club name as shown on the site ('Herr' = men's team). |
| `home_team_score` | integer | Goals scored by the team in the game. |
| `home_team_icon` | character | URL of the team logo image. |
| `away_team_status` | character | Result for the team (WIN, LOSE) or record status. |
| `away_team_uuid` | character | Team uuid (platform key space, e.g. 087a-087aTQv9u); joins to /sports-v2/teams/{team_uuid} and schedule homeTeamInfo/awayTeamInfo.uuid. Not the team code. |
| `away_team_owner_instance_id` | character | Club site instance id (e.g. fhc1_fhc); also names the logo at sportality.cdn.s8y.se/team-logos/{id}.svg. |
| `away_team_code` | character | Away team code (e.g. LHC); joins to gameday teamId and the code-keyed teams{} maps. |
| `away_team_names_code` | character | Display abbreviation; differs from the team code for some clubs (SKE vs SAIK, ÖRE vs OHK). |
| `away_team_names_short` | character | Short club name ('Herr' = men's team). |
| `away_team_names_long` | character | Long club name ('Herr' = men's team). |
| `away_team_names_full` | character | Full club name ('Herr' = men's team). |
| `away_team_score` | integer | Goals scored by the team in the game. |
| `away_team_icon` | character | URL of the team logo image. |
| `venue_uuid` | character | Arena uuid (platform key space). |
| `venue_name` | character | Name of the arena (venue). |
| `series_uuid` | character | Series uuid (platform key space; SHL = qQ9-bb0bzEWUk). |
| `series_code` | character | Series code (SHL, CHL). |
| `series_display_name` | character | Series label shown on the site (SHL, CHL). |
| `away_team_names_code_site` | character | Display abbreviation on the site; 'Herr' = men's team (e.g. 'DIF Herr'). |
| `away_team_names_short_site` | character | Short club name as shown on the site ('Herr' = men's team). |
| `away_team_names_full_site` | character | Full club name as shown on the site ('Herr' = men's team). |
| `away_team_names_long_site` | character | Long club name as shown on the site ('Herr' = men's team). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_next_game_day-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_next_game_day-example}

```python
shl_next_game_day()
```

_Last validated n/a._

## shl_pbp

Play-by-play events with shot coordinates (2023/24 on).

**Endpoint URL:** `GET https://www.shl.se/api/gameday/play-by-play/{game_uuid}`

**Valid URL:** [https://www.shl.se/api/gameday/play-by-play/bdhvuc5tex](https://www.shl.se/api/gameday/play-by-play/bdhvuc5tex)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_uuid` | `game_uuid` |  | `Y` |  | Game uuid (gameInfo[].uuid from /sports-v2/game-schedule). gameday/* has data from 2023/24 on; older games answer 200 with an empty body. |

### Returns {#shl_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_source_id` | character | StatNet game key, date-HOME-AWAY (e.g. 20250913-FHC-LHC); the same game as gameId. |
| `game_id` | integer | StatNet game number (StatNet key space, e.g. 22003); keys stats-info games{} and equals sports-v2 game-info extId. Not the game uuid. |
| `event_id` | integer | StatNet event sequence number; unique within one game only (pair it with gameId or gameUuid). |
| `event_uuid` | character | Event uuid (platform key space), unique across games. eventId is the StatNet per-game sequence number. |
| `round` | integer | Schedule round of the game (1 = first game day). |
| `game_type` | character | StatNet league label: Elitserien (SHL's name until 2013). |
| `arena` | character | Name of the arena hosting the game. |
| `attendance` | integer | Spectators at the game. |
| `start_date_and_time` | character | Scheduled start (local). |
| `period` | integer | Period (1-3, 4 = overtime, 5 = shootout). |
| `time` | character | Game clock in the period (MM:SS, counting up from 00:00). Clock/duration as an mm:ss string. |
| `game_state` | character | Game state (e.g. GameEnded). |
| `revision` | integer | Event revision counter. |
| `type` | character | Event type: goal, shot, penalty, goalkeeper (goalie in / out), period (start / end), timeout. |
| `real_world_time` | character | Wall-clock time of the event (local). |
| `updated_time` | character | Last update time of the event. |
| `home_team_id` | character | Team code (e.g. FHC) in gameday and standings payloads; not the team uuid. |
| `home_team_name` | character | Team display name (e.g. Frölunda HC). |
| `home_team_code` | character | Team code (FHC, LHC, ...). |
| `home_score` | integer | Goals scored by the team in the game. |
| `away_team_id` | character | Team code (e.g. FHC) in gameday and standings payloads; not the team uuid. |
| `away_team_name` | character | Team display name (e.g. Frölunda HC). |
| `away_team_code` | character | Team code (FHC, LHC, ...). |
| `away_score` | integer | Goals scored by the team in the game. |
| `event_team_id` | character | Team code (e.g. FHC) in gameday and standings payloads; not the team uuid. |
| `event_place` | character | Side of the game: home or away. |
| `event_team_code` | character | Team code (FHC, LHC, ...). |
| `event_team_name` | character | Team display name (e.g. Frölunda HC). |
| `player_id` | integer | isa (StatNet) player id, Int64. Play-by-play sends it as a string, every other route as an integer; it is cast here so the frames join. |
| `player_first_name` | character | Given (first) name of the player. |
| `player_family_name` | character | Surname (family name) of the player. |
| `player_jersey_today` | character | Jersey number in this game. Numeric value sent as a string. |
| `is_entering` | logical | Goalie event, true when the goalie enters the ice and false when leaving. |
| `game_uuid` | character | Platform game uuid (bdhvuc5tex); every gameday route takes it. |
| `location_x` | integer | Shot x coordinate. Units, origin and orientation unverified; range -29..481 over the 118 located events of game bdhvuc5tex (discovery download, 2026-10-12). |
| `location_y` | integer | Shot y coordinate. Units, origin and orientation unverified; range -140..129 over the 118 located events of game bdhvuc5tex (discovery download, 2026-10-12). |
| `goal_section` | integer | Goal-mouth section of the shot/goal (-3 lines up with blocked shots; unverified). |
| `is_penalty_shot` | logical | Whether the shot/goal was a penalty shot. |
| `variant_short_name` | character | Penalty type (Minor, Major, ...). |
| `variant_minor_time` | character | Minor-penalty minutes. Numeric value sent as a string. |
| `variant_double_minor_time` | character | Double-minor minutes in the penalty. Numeric value sent as a string. |
| `variant_bench_time` | character | Bench minor minutes in the penalty. Numeric value sent as a string. |
| `variant_major_time` | character | Major-penalty minutes. Numeric value sent as a string. |
| `variant_misconduct_time` | character | Misconduct minutes. Numeric value sent as a string. |
| `variant_g_m_time` | character | Game-misconduct minutes in the penalty. Numeric value sent as a string. |
| `variant_m_p_time` | character | Match-penalty minutes. Numeric value sent as a string. |
| `variant_description` | character | Text description (penalty, media). |
| `offence` | character | Penalty offence code (e.g. CROSS, HOOK, TRIP). |
| `did_render_in_penalty_shot` | logical | Whether the penalty led to a penalty shot. |
| `home_goals` | integer | Home goals after this event (running score). |
| `away_goals` | integer | Away goals after this event (running score). |
| `is_empty_net_goal` | logical | Whether the goal went into an empty net. |
| `pop_player_ids` | character | isa ids of the scoring side's players on the ice for the goal (List[Int64]). |
| `nep_player_ids` | character | isa ids of the conceding side's players on the ice for the goal (List[Int64]). |
| `assist1_player_id` | integer | isa id of the first assist. |
| `assist1_first_name` | character | First assist: given name. |
| `assist1_family_name` | character | First assist: surname. |
| `assist1_jersey_today` | character | First assist: jersey number in this game (string). |
| `assist2_player_id` | integer | isa id of the second assist. |
| `assist2_first_name` | character | Second assist: given name. |
| `assist2_family_name` | character | Second assist: surname. |
| `assist2_jersey_today` | character | Second assist: jersey number in this game (string). |
| `goal_status` | character | Goal strength (e.g. EQ, PP1, SH1). |
| `started` | logical | Whether the period has started. |
| `started_at` | character | Period start time (ISO 8601 UTC). |
| `finished` | logical | Whether the period is finished. |
| `finished_at` | character | Period end time (ISO 8601 UTC). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_pbp-example}

```python
shl_pbp(game_uuid='bdhvuc5tex')
```

_Last validated n/a._

## shl_player_box

Extended skater and goalie game stats per team (2023/24 on).

**Endpoint URL:** `GET https://www.shl.se/api/gameday/player-stats/{game_uuid}`

**Valid URL:** [https://www.shl.se/api/gameday/player-stats/bdhvuc5tex](https://www.shl.se/api/gameday/player-stats/bdhvuc5tex)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_uuid` | `game_uuid` |  | `Y` |  | Game uuid (gameInfo[].uuid from /sports-v2/game-schedule). gameday/* has data from 2023/24 on; older games answer 200 with an empty body. |

### Returns {#shl_player_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_side` | character | Side of the game: home or away. |
| `position_group` | character | skater or goalie (goalies come from the body's gkStats rows). |
| `player_id` | integer | isa (StatNet) player id, Int64. Play-by-play sends it as a string, every other route as an integer; it is cast here so the frames join. |
| `team_id` | character | Team code (e.g. FHC) in gameday and standings payloads; not the team uuid. |
| `period` | integer | Period: 0 = the whole game, 1-3 the periods, 4 and up overtime. |
| `full_name` | character | Given name and surname. |
| `first_name` | character | Given (first) name of the player. |
| `last_name` | character | Surname (family name) of the player. |
| `nr` | integer | Jersey (shirt) number of the player. |
| `pos` | character | Position code in this game (e.g. CE, LW, RW, D, GK). |
| `line` | integer | Line (unit) number the player was listed on. |
| `g` | numeric | Number of goals scored. |
| `a` | numeric | Number of assists credited. |
| `ppg` | numeric | Power-play goals. |
| `sw` | numeric | Number of shots that missed the net. |
| `pim` | numeric | Penalty minutes. |
| `sog` | numeric | Number of shots on goal. |
| `ppsog` | numeric | Power-play shots on goal. |
| `plus_minus` | numeric | Plus/minus in the game. |
| `toi` | character | Time on ice (mm:ss). Clock/duration as an mm:ss string. |
| `hits` | numeric | Body checks delivered. |
| `fow` | numeric | Number of face-offs won. |
| `fol` | numeric | Face-offs lost. |
| `fo_perc` | numeric | Face-off win %. |
| `ga` | numeric | Number of goals conceded (goals against). |
| `soga` | numeric | Shots on goal against. |
| `spga` | numeric | Unverified goalie column (box score). |
| `svs` | numeric | Number of saves made by the goalie. |
| `svs_perc` | numeric | Save percentage of the game as a FRACTION 0-1 (e.g. '0.9655...'); integer 0 for a goalie without shots. Numeric, but sent as a string in some rows and as a number in others. |
| `tp` | numeric | Points (goals + assists). |
| `shg` | numeric | Shorthanded goals. |
| `nep` | numeric | On ice for goals against (count). |
| `pop` | numeric | On ice for goals for (count). |
| `fo_tot` | numeric | Face-offs taken. |
| `num_pen` | numeric | Number of penalties. |
| `ms` | numeric | Missed shots (wide + blocked). |
| `bk_s` | numeric | Number of blocked shots. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_player_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_player_box-example}

```python
shl_player_box(game_uuid='bdhvuc5tex')
```

_Last validated n/a._

## shl_player_h2h

Player head-to-head for a matchup.

**Endpoint URL:** `GET https://www.shl.se/api/statistics-v2/player-h2h-v2/{ssgt_uuid}/{home_team_uuid}/{away_team_uuid}`

**Valid URL:** [https://www.shl.se/api/statistics-v2/player-h2h-v2/iuzqg7dqk9/087a-087aTQv9u/41c4-41c4BiYZU?gameUuid=bdhvuc5tex](https://www.shl.se/api/statistics-v2/player-h2h-v2/iuzqg7dqk9/087a-087aTQv9u/41c4-41c4BiYZU?gameUuid=bdhvuc5tex)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ssgt_uuid` | `ssgt_uuid` |  | `Y` |  | ssgt uuid: one season + series + game type (e.g. 2025/2026 SHL regular season). Directory: /statistics-v2/layout-info seasonSeriesGameTypeTeams[].ssgtUuid. |
| `home_team_uuid` | `home_team_uuid` |  | `Y` |  | Home team uuid of the game. |
| `away_team_uuid` | `away_team_uuid` |  | `Y` |  | Away team uuid of the game. |
| `gameUuid` | `game_uuid` |  |  | `Y` | Game uuid; narrows the head-to-head to that game's line-ups. |

### Returns {#shl_player_h2h-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `uuid` | character | Athlete uuid (platform key space; qQ9-... or 10-char form); joins to the athlete routes and stats-info stats[].info.uuid. Not the isa or ramses_legacy id. |
| `first_name` | character | Given (first) name of the player. |
| `last_name` | character | Surname (family name) of the player. |
| `full_name` | character | Given name and surname. |
| `birth_date` | character | Date of birth (YYYY-MM-DD). |
| `nationality` | character | Nationality (ISO country code). |
| `gender` | character | Sex of the athlete or team (male / female). |
| `jersey_number` | character | Jersey (shirt) number of the player. |
| `position` | character | Position label in English (e.g. Forward); N/A when unknown. |
| `position_code` | character | Position code (GK, D, F; CE / LW / RW in stats rows); N/A when unknown. |
| `age_value` | integer | Age of the player in whole years. |
| `age_format` | character | Unit of value: years. |
| `height_value` | integer | Height of the player in centimetres. |
| `height_format` | character | Unit of value: cm. |
| `weight_value` | integer | Weight of the player in kilograms. |
| `weight_format` | character | Unit of value: kg. |
| `team_uuid` | character | Platform team uuid. |
| `team_instance_id` | character | Club site instance id (e.g. fhc1_fhc). |
| `team_name` | character | Name of the team. |
| `team_code` | character | StatNet team code (SAIK, FHC). |
| `team_media` | character | Media URL or string. |
| `media_url` | character | Portrait URL on the imgix CDN. |
| `media_alt` | character | Image alt text. Empty string in every sample (placeholder). |
| `media_srcset` | character | Responsive image srcset. |
| `season_stats` | character | Current-season stat rows of the player (empty in the sample). |
| `career_stats` | character | Career stat rows of the player (empty in the sample). |
| `match_stats_nr` | integer | Jersey (shirt) number of the player. |
| `match_stats_pos` | character | Position code in this game (e.g. CE, LW, RW, D, GK). |
| `match_stats_line` | integer | Line (unit) number the player was listed on. |
| `match_stats_g` | integer | Number of goals scored. |
| `match_stats_a` | integer | Number of assists credited. |
| `match_stats_tp` | integer | Points (goals + assists). |
| `match_stats_shg` | integer | Shorthanded goals. |
| `match_stats_ppg` | integer | Power-play goals. |
| `match_stats_sw` | integer | Number of shots that missed the net. |
| `match_stats_pim` | integer | Penalty minutes. |
| `match_stats_sog` | integer | Number of shots on goal. |
| `match_stats_ppsog` | integer | Power-play shots on goal. |
| `match_stats_nep` | integer | On ice for goals against (count). |
| `match_stats_pop` | integer | On ice for goals for (count). |
| `match_stats_toi` | character | Time on ice (mm:ss). Clock/duration as an mm:ss string. |
| `match_stats_hits` | integer | Body checks delivered. |
| `match_stats_fo_tot` | integer | Face-offs taken. |
| `match_stats_fow` | integer | Number of face-offs won. |
| `match_stats_fol` | integer | Face-offs lost. |
| `match_stats_num_pen` | integer | Number of penalties. |
| `match_stats_fo_perc` | numeric | Face-off win %. |
| `match_stats_ms` | integer | Missed shots (wide + blocked). |
| `match_stats_bk_s` | integer | Number of blocked shots. |
| `match_stats_plus_minus` | integer | Goals for minus goals against while on ice (+/-). |
| `season_avg_stats` | character | Head-to-head season averages. |
| `statistics_provider` | character | Stats provider of the data (statnet seen). |
| `is_in_squad` | logical | Whether the player is in the current squad. |
| `effective_ssgt_uuid` | character | The ssgt the head-to-head was computed for: the CURRENT season, whatever the path asked. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_player_h2h-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_player_h2h-example}

```python
shl_player_h2h(away_team_uuid='41c4-41c4BiYZU', game_uuid='bdhvuc5tex', home_team_uuid='087a-087aTQv9u', ssgt_uuid='iuzqg7dqk9')
```

_Last validated n/a._

## shl_post_game_team_stats

Post-game team comparison.

**Endpoint URL:** `GET https://www.shl.se/api/gameday/post-game-data/team-stats/{game_uuid}`

**Valid URL:** [https://www.shl.se/api/gameday/post-game-data/team-stats/bdhvuc5tex?ssgtUuid=iuzqg7dqk9&homeTeamUuid=087a-087aTQv9u&awayTeamUuid=41c4-41c4BiYZU](https://www.shl.se/api/gameday/post-game-data/team-stats/bdhvuc5tex?ssgtUuid=iuzqg7dqk9&homeTeamUuid=087a-087aTQv9u&awayTeamUuid=41c4-41c4BiYZU)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_uuid` | `game_uuid` |  | `Y` |  | Game uuid (gameInfo[].uuid from /sports-v2/game-schedule). gameday/* has data from 2023/24 on; older games answer 200 with an empty body. |
| `ssgtUuid` | `ssgt_uuid` |  | `Y` |  | Required. ssgt uuid. |
| `homeTeamUuid` | `home_team_uuid` |  | `Y` |  | Required. Home team uuid; without it the home side comes back blank. |
| `awayTeamUuid` | `away_team_uuid` |  | `Y` |  | Required. Away team uuid; without it the away side comes back blank. |

### Returns {#shl_post_game_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `home_team_side_translate_key` | character | Translation key of the side. |
| `home_team_left_type` | character | Stat code compared on the left (e.g. G, PPG). |
| `home_team_left_value` | integer | Value (string-encoded). |
| `home_team_right_type` | character | Stat code compared on the right (e.g. SOG, PPOpp). |
| `home_team_right_value` | integer | Value (string-encoded). |
| `home_team_center_type` | character | Stat code compared in the middle (e.g. SEff, PPPerc). |
| `home_team_center_value` | character | Value (string-encoded). Numeric, but sent as a string in some rows and as a number in others. |
| `away_team_side_translate_key` | character | Translation key of the side. |
| `away_team_left_type` | character | Stat code compared on the left (e.g. G, PPG). |
| `away_team_left_value` | integer | Value (string-encoded). |
| `away_team_right_type` | character | Stat code compared on the right (e.g. SOG, PPOpp). |
| `away_team_right_value` | integer | Value (string-encoded). |
| `away_team_center_type` | character | Stat code compared in the middle (e.g. SEff, PPPerc). |
| `away_team_center_value` | character | Value (string-encoded). Numeric, but sent as a string in some rows and as a number in others. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_post_game_team_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_post_game_team_stats-example}

```python
shl_post_game_team_stats(away_team_uuid='41c4-41c4BiYZU', game_uuid='bdhvuc5tex', home_team_uuid='087a-087aTQv9u', ssgt_uuid='iuzqg7dqk9')
```

_Last validated n/a._

## shl_roster

Current roster of a team, grouped by position.

**Endpoint URL:** `GET https://www.shl.se/api/sports-v2/athletes/by-team-uuid/{team_uuid}`

**Valid URL:** [https://www.shl.se/api/sports-v2/athletes/by-team-uuid/087a-087aTQv9u](https://www.shl.se/api/sports-v2/athletes/by-team-uuid/087a-087aTQv9u)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_uuid` | `team_uuid` |  | `Y` |  | Team uuid (homeTeamInfo.uuid / awayTeamInfo.uuid in the schedule, or teamList[].uuid). |

### Returns {#shl_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `position` | character | Position group label: Målvakter = goalkeepers, Backar = defencemen, Forwards. |
| `position_code` | character | Position code (GK, D, F; CE / LW / RW in stats rows); N/A when unknown. |
| `player_type` | character | Roster role of the player (unverified). |
| `gender` | character | Sex of the athlete or team (male / female). |
| `uuid` | character | Athlete uuid (platform key space; qQ9-... or 10-char form); joins to the athlete routes and stats-info stats[].info.uuid. Not the isa or ramses_legacy id. |
| `first_name` | character | Given (first) name of the player. |
| `last_name` | character | Surname (family name) of the player. |
| `full_name` | character | Given name and surname. |
| `nationality` | character | Nationality (ISO country code). |
| `jersey_number` | integer | Jersey (shirt) number of the player. |
| `portrait_url` | character | URL of the player's latest rendered portrait. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_roster-example}

```python
shl_roster(team_uuid='087a-087aTQv9u')
```

_Last validated n/a._

## shl_schedule

Schedule and results of one season + series + game type.

**Endpoint URL:** `GET https://www.shl.se/api/sports-v2/game-schedule`

**Valid URL:** [https://www.shl.se/api/sports-v2/game-schedule?season=2026&gameType=regular](https://www.shl.se/api/sports-v2/game-schedule?season=2026&gameType=regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025/26). Mapped to the provider's ssgt through /statistics-v2/layout-info (SHL 1975/76 on); a season it does not list raises NoDataError. |
| `gameType` | `game_type` |  |  | `Y` | Game type: regular (Grundserie), playoff (Slutspel), qualdown (relegation qualification) or playin (2023/24 on). |

### Returns {#shl_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_uuid` | character | Platform game uuid (bdhvuc5tex); every gameday route takes it. |
| `raw_start_date_time` | character | Start time, UTC ISO 8601. |
| `start_date_time` | character | Scheduled start, local time (Europe/Stockholm). |
| `state` | character | Game state: pre-game or post-game (hyphenated; a live value was not observed). |
| `overtime` | logical | Whether the game went to overtime. |
| `shootout` | logical | Whether the game went to a shootout. |
| `ssgt_uuid` | character | ssgt uuid (platform key space: one season + series + game type); label via layout-info seasonSeriesGameTypeTeams[]. |
| `home_team_status` | character | Result for the team (WIN, LOSE) or record status. |
| `home_team_uuid` | character | Team uuid (platform key space, e.g. 087a-087aTQv9u); joins to /sports-v2/teams/{team_uuid} and schedule homeTeamInfo/awayTeamInfo.uuid. Not the team code. |
| `home_team_owner_instance_id` | character | Club site instance id (e.g. fhc1_fhc); also names the logo at sportality.cdn.s8y.se/team-logos/{id}.svg. |
| `home_team_code` | character | Home team code (e.g. FHC); joins to gameday teamId and the code-keyed teams{} maps. |
| `home_team_names_code` | character | Display abbreviation; differs from the team code for some clubs (SKE vs SAIK, ÖRE vs OHK). |
| `home_team_names_short` | character | Short club name ('Herr' = men's team). |
| `home_team_names_long` | character | Long club name ('Herr' = men's team). |
| `home_team_names_full` | character | Full club name ('Herr' = men's team). |
| `home_team_names_code_site` | character | Display abbreviation on the site; 'Herr' = men's team (e.g. 'DIF Herr'). |
| `home_team_names_short_site` | character | Short club name as shown on the site ('Herr' = men's team). |
| `home_team_names_full_site` | character | Full club name as shown on the site ('Herr' = men's team). |
| `home_team_names_long_site` | character | Long club name as shown on the site ('Herr' = men's team). |
| `home_team_score` | integer | Goals scored by the team in the game. |
| `home_team_icon` | character | URL of the team logo image. |
| `away_team_status` | character | Result for the team (WIN, LOSE) or record status. |
| `away_team_uuid` | character | Team uuid (platform key space, e.g. 087a-087aTQv9u); joins to /sports-v2/teams/{team_uuid} and schedule homeTeamInfo/awayTeamInfo.uuid. Not the team code. |
| `away_team_owner_instance_id` | character | Club site instance id (e.g. fhc1_fhc); also names the logo at sportality.cdn.s8y.se/team-logos/{id}.svg. |
| `away_team_code` | character | Away team code (e.g. LHC); joins to gameday teamId and the code-keyed teams{} maps. |
| `away_team_names_code` | character | Display abbreviation; differs from the team code for some clubs (SKE vs SAIK, ÖRE vs OHK). |
| `away_team_names_short` | character | Short club name ('Herr' = men's team). |
| `away_team_names_long` | character | Long club name ('Herr' = men's team). |
| `away_team_names_full` | character | Full club name ('Herr' = men's team). |
| `away_team_names_code_site` | character | Display abbreviation on the site; 'Herr' = men's team (e.g. 'DIF Herr'). |
| `away_team_names_short_site` | character | Short club name as shown on the site ('Herr' = men's team). |
| `away_team_names_full_site` | character | Full club name as shown on the site ('Herr' = men's team). |
| `away_team_names_long_site` | character | Long club name as shown on the site ('Herr' = men's team). |
| `away_team_score` | integer | Goals scored by the team in the game. |
| `away_team_icon` | character | URL of the team logo image. |
| `venue_uuid` | character | Arena uuid (platform key space). |
| `venue_name` | character | Name of the arena (venue). |
| `series_uuid` | character | Series uuid (platform key space; SHL = qQ9-bb0bzEWUk). |
| `series_code` | character | Series code (SHL, CHL). |
| `series_display_name` | character | Series label shown on the site (SHL, CHL). |
| `round_number` | integer | Schedule round (1 = first game day of the season). |
| `round_label` | character | Round label (often empty). Empty string in every sample (placeholder). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_schedule-example}

```python
shl_schedule(season=2026)
```

_Last validated n/a._

## shl_season_filter

Series, seasons (1975/76 on) and game types with their uuids, plus the default ssgt.

**Endpoint URL:** `GET https://www.shl.se/api/sports-v2/season-series-game-types-filter`

**Valid URL:** [https://www.shl.se/api/sports-v2/season-series-game-types-filter?series=qQ9-bb0bzEWUk&season=xs4m9qupsi](https://www.shl.se/api/sports-v2/season-series-game-types-filter?series=qQ9-bb0bzEWUk&season=xs4m9qupsi)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `series` | `series_uuid` |  |  | `Y` | Series uuid (SHL qQ9-bb0bzEWUk, CHL qQ9-9397drVEh). No effect on the body in the drop-one probe. |
| `season` | `season_uuid` |  |  | `Y` | Season uuid; picks the default ssgt returned for that season (omit for the current season). |

### Returns {#shl_season_filter-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `kind` | character | Directory entry kind: series, season or game_type. |
| `code` | character | Game type code: regular, playoff, qualdown (legacy career logs: qualification). |
| `name` | character | Game type label in Swedish: Grundserie = regular season, Slutspel = playoffs, Kvalspel nedflyttning = relegation qualification, Kvalserien = the old promotion/relegation series. |
| `uuid` | character | Game type uuid (platform key space; regular qQ9-af37Ti40B); the gameTypeUuid param of game-schedule. |
| `deleted` | logical | Soft-delete flag of the record (false in every sample). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_season_filter-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_season_filter-example}

```python
shl_season_filter(season_uuid='xs4m9qupsi', series_uuid='qQ9-bb0bzEWUk')
```

_Last validated n/a._

## shl_sites

Teams (sites) taking part in an ssgt.

**Endpoint URL:** `GET https://www.shl.se/api/sports-v2/all-sites/{ssgt_uuid}`

**Valid URL:** [https://www.shl.se/api/sports-v2/all-sites/iuzqg7dqk9](https://www.shl.se/api/sports-v2/all-sites/iuzqg7dqk9)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ssgt_uuid` | `ssgt_uuid` |  | `Y` |  | ssgt uuid: one season + series + game type (e.g. 2025/2026 SHL regular season). Directory: /statistics-v2/layout-info seasonSeriesGameTypeTeams[].ssgtUuid. |

### Returns {#shl_sites-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | character | Club site instance id (e.g. bif1_bif); equals instanceId. Not a team uuid or code. |
| `instance_id` | character | Club site instance id (e.g. fhc1_fhc). |
| `public_url` | character | Public image URL. |
| `name` | character | Full name of the club. |
| `icon` | character | URL of the team logo image. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_sites-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_sites-example}

```python
shl_sites(ssgt_uuid='iuzqg7dqk9')
```

_Last validated n/a._

## shl_ssgt_provider

Stats provider of an ssgt, a bare string such as statnet.

**Endpoint URL:** `GET https://www.shl.se/api/sports-v2/ssgt/{ssgt_uuid}/provider`

**Valid URL:** [https://www.shl.se/api/sports-v2/ssgt/iuzqg7dqk9/provider](https://www.shl.se/api/sports-v2/ssgt/iuzqg7dqk9/provider)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ssgt_uuid` | `ssgt_uuid` |  | `Y` |  | ssgt uuid: one season + series + game type (e.g. 2025/2026 SHL regular season). Directory: /statistics-v2/layout-info seasonSeriesGameTypeTeams[].ssgtUuid. |

### Returns {#shl_ssgt_provider-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `provider` | character | Stats provider of the ssgt (statnet, impleo). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_ssgt_provider-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_ssgt_provider-example}

```python
shl_ssgt_provider(ssgt_uuid='iuzqg7dqk9')
```

_Last validated n/a._
