---
title: "SHL — SHL site API (www.shl.se/api, Sweden) — Other"
sidebar_label: "Other"
sidebar_position: 4
description: "SHL — SHL site API (www.shl.se/api, Sweden) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# SHL — SHL site API (www.shl.se/api, Sweden) — Other

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

### Example {#shl_ssgt_provider-example}

```python
shl_ssgt_provider(ssgt_uuid='iuzqg7dqk9')
```

_Last validated n/a._

## shl_standings

Standings of an ssgt (gameday widget).

**Endpoint URL:** `GET https://www.shl.se/api/gameday/team-standings/{ssgt_uuid}`

**Valid URL:** [https://www.shl.se/api/gameday/team-standings/iuzqg7dqk9](https://www.shl.se/api/gameday/team-standings/iuzqg7dqk9)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ssgt_uuid` | `ssgt_uuid` |  | `Y` |  | ssgt uuid: one season + series + game type (e.g. 2025/2026 SHL regular season). Directory: /statistics-v2/layout-info seasonSeriesGameTypeTeams[].ssgtUuid. |

### Returns {#shl_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025/26). |
| `ssgt_uuid` | character | ssgt uuid (platform key space: one season + series + game type); label via layout-info seasonSeriesGameTypeTeams[]. |
| `team_code` | character | StatNet team code (SAIK, FHC). |
| `team_uuid` | character | Platform team uuid. |
| `team_name` | character | Name of the team. |
| `team_short_name` | character | Short display name of the team. |
| `rank` | integer | Position in the table (1 = top). |
| `rank_diff` | integer | Change in standings rank, in places (the reference point is unverified). |
| `gp` | integer | Number of games played. |
| `w` | integer | Regulation wins. |
| `t` | integer | Games tied after regulation, decided in overtime or a shootout (= OTW + OTL). |
| `l` | integer | Regulation losses. |
| `otw` | integer | Overtime and shootout wins. |
| `otl` | integer | Overtime and shootout losses. |
| `p` | integer | Standings points (3 per regulation win, 2 per OT/SO win, 1 per OT/SO loss). |
| `gf` | integer | Number of goals scored (goals for). |
| `ga` | integer | Number of goals conceded (goals against). |
| `diff` | integer | Goal difference. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#shl_standings-example}

```python
shl_standings(ssgt_uuid='iuzqg7dqk9')
```

_Last validated n/a._

## shl_stats_info

Stats table of one module for an ssgt (standings, players, goalies, teams, games).

**Endpoint URL:** `GET https://www.shl.se/api/statistics-v2/stats-info/{module}`

**Valid URL:** [https://www.shl.se/api/statistics-v2/stats-info/players_summary?count=1000&season=2026&gameType=regular](https://www.shl.se/api/statistics-v2/stats-info/players_summary?count=1000&season=2026&gameType=regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `module` | `module` |  | `Y` |  | Stats module: <group>_<table>. Every name under layout-info [].filters.<group>.modules. |
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025/26). Mapped to the provider's ssgt through /statistics-v2/layout-info (SHL 1975/76 on); a season it does not list raises NoDataError. |
| `gameType` | `game_type` |  |  | `Y` | Game type: regular (Grundserie), playoff (Slutspel), qualdown (relegation qualification) or playin (2023/24 on). |

### Returns {#shl_stats_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `rank` | integer | Rank in the table. |
| `gp` | numeric | Number of games played. |
| `w` | numeric | Wins. Team modules (teams_result): total wins, regulation + overtime + shootout; goalkeeper modules: wins credited to the goalie; standings_standings: regulation wins only. |
| `l` | numeric | Losses. Team modules (teams_result): total losses, regulation + overtime + shootout; goalkeeper modules: losses charged to the goalie; standings_standings: regulation losses only. |
| `leading` | numeric | Times leading: how often the team was ahead (exact definition unverified). |
| `trailing` | numeric | Times trailing. |
| `g` | numeric | Number of goals scored. |
| `ga` | numeric | Number of goals conceded (goals against). |
| `gfg` | numeric | First goal of the game. |
| `otw` | numeric | Overtime wins, shootouts excluded (see SOW). |
| `otl` | numeric | Overtime losses, shootouts excluded (see SOL). |
| `sow` | numeric | Number of shootout wins. |
| `sol` | numeric | Shootout losses. |
| `p_gp_format` | numeric | Points per game (formatted). Numeric value sent as a string. |
| `gpg` | numeric | Goals per game. Numeric value sent as a string. |
| `gapg` | numeric | Goals against per game. Numeric value sent as a string. |
| `points` | numeric | Standings points (3 per regulation win, 2 per OT/SO win, 1 per OT/SO loss). |
| `info_type` | character | Record type of the row's subject: team in team modules, athlete in player and goalie modules. |
| `info_id` | character | Team code (e.g. SAIK) in team and standings modules; null in player, goalie and games modules (use info.uuid). |
| `info_uuid` | character | Athlete uuid (platform key space) in player and goalie modules, team uuid in team modules; joins to the athlete routes or /sports-v2/teams/{team_uuid}. |
| `info_created_at` | character | Record creation time (ISO 8601; often null). |
| `info_created_by` | character | Record creator (site instance id; often null). |
| `info_deleted` | logical | Soft-delete flag of the record (false in every sample). |
| `info_gender` | character | Sex of the athlete or team (male / female). |
| `info_graphics_library` | character | Graphics asset key. |
| `info_last_modified` | character | Record last-modified time (ISO 8601). |
| `info_nationality` | character | Nationality (ISO country code). |
| `info_owner_instance_id` | character | Club site instance id (e.g. fhc1_fhc); also names the logo at sportality.cdn.s8y.se/team-logos/{id}.svg. |
| `info_ownersite` | character | Owning site code. |
| `info_team_code` | character | Team code of the player's team (e.g. FHC). |
| `info_series` | character | Series with their uuids (SHL, CHL). |
| `info_sport` | character | Sport records the club belongs to (ice hockey). |
| `info_team_media` | character | Media URL or string. |
| `info_team_names_code` | character | Display abbreviation; differs from the team code for some clubs (SKE vs SAIK, ÖRE vs OHK). |
| `info_team_names_short` | character | Short club name ('Herr' = men's team). |
| `info_team_names_long` | character | Long club name ('Herr' = men's team). |
| `info_team_names_full` | character | Full club name ('Herr' = men's team). |
| `info_team_names_code_site` | character | Display abbreviation on the site; 'Herr' = men's team (e.g. 'DIF Herr'). |
| `info_team_names_short_site` | character | Short club name as shown on the site ('Herr' = men's team). |
| `info_team_names_full_site` | character | Full club name as shown on the site ('Herr' = men's team). |
| `info_team_names_long_site` | character | Long club name as shown on the site ('Herr' = men's team). |
| `info_club_name_code` | character | Display abbreviation; differs from the team code for some clubs (SKE vs SAIK, ÖRE vs OHK). |
| `info_club_name_short` | character | Short club name ('Herr' = men's team). |
| `info_club_name_long` | character | Long club name ('Herr' = men's team). |
| `info_club_name_full` | character | Full club name ('Herr' = men's team). |
| `info_team_info_founded` | character | Club founding year (text). Numeric value sent as a string. |
| `info_team_info_golds` | character | Club's championships (text). |
| `info_team_info_finals` | character | Club's final appearances (text). |
| `info_team_info_address` | character | Club postal address. |
| `info_team_info_phone` | character | Telephone number of the club. |
| `info_team_info_fax` | character | Fax number of the club. |
| `info_team_info_email` | character | Contact email address of the club. |
| `info_team_info_chairman` | character | Name of the club chairman. |
| `info_team_info_manager` | character | Club general manager. |
| `info_team_info_accreditation` | character | Press-accreditation contact, free Swedish text (e.g. 'Senast dagen före match till …' = 'no later than the day before the game, to …'). |
| `info_team_info_facebook` | character | Club Facebook URL. |
| `info_team_info_instagram` | character | Club Instagram URL. |
| `info_team_info_twitter` | character | Club X/Twitter URL. |
| `info_team_ext_ids` | character | External ids of the team. |
| `info_ext_ids` | character | External ids of the record. |
| `info_logo` | character | URL of the team logo image. |
| `info_social_media` | character | Club social links. |
| `info_display_name` | character | Team display-name override; unused. Empty string in every sample (placeholder). |
| `info_code` | character | Team code (e.g. SAIK) in team and standings modules; absent for players. |
| `info_site_display_name` | character | Club name as shown on its own site (e.g. Frölunda HC). |
| `info_team_info_retired_numbers` | character | Retired jersey numbers (text). |
| `info_team_info_contact` | character | Club contact text. |
| `info_club_name` | character | Club names by length. |
| `player_id` | integer | isa (StatNet) player id, Int64. Play-by-play sends it as a string, every other route as an integer; it is cast here so the frames join. |
| `svs` | numeric | Number of saves made by the goalie. |
| `svs_perc` | numeric | Save %. Numeric value sent as a string. |
| `gpi` | numeric | Games played with ice time. |
| `gaa` | numeric | Goals against average (per 60 minutes). Numeric value sent as a string. |
| `t` | numeric | Goalie ties (goalkeeper modules only); 0 in every sampled row, as overtime and shootouts now decide every game. |
| `so` | numeric | Number of shutouts (games without a goal against). |
| `mip` | character | Minutes played (mm:ss). Clock/duration as an mm:ss string. |
| `valid_ranking` | logical | Whether the row qualifies for the ranking (minimum games / minutes). |
| `info_full_name` | character | Given name and surname. |
| `info_first_name` | character | Given (first) name of the player. |
| `info_last_name` | character | Surname (family name) of the player. |
| `info_birth_date` | character | Date of birth (YYYY-MM-DD). |
| `info_number` | integer | Jersey (shirt) number of the player. |
| `info_position` | character | Position code (CE = centre, LW, RW, D, GK, F); N/A in the athlete game logs. |
| `info_shoots` | character | Shooting side (L / R; Left / Right). |
| `info_weight_value` | integer | Weight of the player in kilograms. |
| `info_weight_format` | character | Unit of value: kg. |
| `info_height_value` | integer | Height of the player in centimetres. |
| `info_height_format` | character | Unit of value: cm. |
| `info_player_media_id` | integer | Media-library id of the portrait (joins to /api/media; not a player id). |
| `info_player_media_media_string` | character | Media reference (type\|source\|id). |
| `info_player_media_type` | character | Image kind (portrait). |
| `info_player_media_sort_order` | integer | Ordering index for display. |
| `info_team_uuid` | character | Team uuid (platform key space, e.g. 087a-087aTQv9u); joins to /sports-v2/teams/{team_uuid} and schedule homeTeamInfo/awayTeamInfo.uuid. Not the team code. |
| `info_team_name` | character | Team name (e.g. Frölunda HC); empty in some rows. |
| `info_team_owner_instance_id` | character | Club site instance id (e.g. fhc1_fhc); also names the logo at sportality.cdn.s8y.se/team-logos/{id}.svg. |
| `info_team_site_display_name` | character | Club name as shown on its own site (e.g. Frölunda HC). |
| `info_team_id` | character | Team code (e.g. FHC) in gameday and standings payloads; not the team uuid. |
| `tp` | numeric | Points (goals + assists). |
| `a` | numeric | Number of assists credited. |
| `pim` | numeric | Penalty minutes. |
| `gwg` | numeric | Game-winning goals. |
| `ppg` | numeric | Power-play goals. |
| `sog` | numeric | Number of shots on goal. |
| `hits` | numeric | Body checks delivered. |
| `bk_s` | numeric | Number of blocked shots. |
| `plus` | numeric | Goals for while on ice. |
| `minus` | numeric | Goals against while on ice. |
| `plus_minus` | numeric | Goals for minus goals against while on ice (+/-). |
| `toi_gp` | character | Time on ice per game (mm:ss). Clock/duration as an mm:ss string. |
| `tppg` | numeric | Points per game. Numeric value sent as a string. |
| `a_firsts` | numeric | Primary assists. |
| `a_seconds` | numeric | Secondary assists. |
| `ppa` | numeric | Power-play assists. |
| `pptp` | numeric | Power-play points. |
| `eqg` | numeric | Even-strength goals. |
| `eqa` | numeric | Even-strength assists. |
| `eqtp` | numeric | Even-strength points. |
| `shg` | numeric | Shorthanded goals. |
| `sha` | numeric | Shorthanded assists. |
| `shtp` | numeric | Shorthanded points. |
| `trailing2` | numeric | Games trailing after period 2 (unverified). |
| `trailing1` | numeric | Games trailing after period 1 (unverified). |
| `tiedg` | numeric | Times tied (unverified). |
| `leading1` | numeric | Games leading after period 1 (unverified). |
| `leading2` | numeric | Games leading after period 2 (unverified). |
| `eng` | numeric | Empty-net goals. |
| `toi` | character | Time on ice (mm:ss). Duration as an hh:mm:ss string. |
| `pptoi_gp` | character | Power-play time on ice per game (mm:ss). Clock/duration as an mm:ss string. |
| `shtoi_gp` | character | Shorthanded time on ice per game (mm:ss). Clock/duration as an mm:ss string. |
| `pptoi_perc` | numeric | Share of time on ice on the power play (unverified). Numeric value sent as a string. |
| `shtoi_perc` | numeric | Share of time on ice shorthanded (unverified). Numeric value sent as a string. |
| `pptoi_pp_plus` | character | Power-play time on ice per power-play goal for (unverified). Clock/duration as an mm:ss string. |
| `ppsog` | numeric | Power-play shots on goal. |
| `ppsw` | numeric | Power-play shots wide. |
| `ppsg_perc` | numeric | Power-play shooting % (unverified). Numeric value sent as a string. |
| `pp_plus` | numeric | Goals for while on ice on the power play. |
| `pp_minus` | numeric | Goals against while on ice on the power play. |
| `pp_plus_minus` | numeric | Power-play plus/minus. |
| `shsog` | numeric | Shorthanded shots on goal. |
| `shsw` | numeric | Shorthanded shots wide. |
| `sh_plus` | numeric | Goals for while on ice shorthanded. |
| `sh_minus` | numeric | Goals against while on ice shorthanded. |
| `sh_plus_minus` | numeric | Shorthanded plus/minus. |
| `shtoi_sh_minus` | character | Shorthanded time on ice per shorthanded goal against (unverified). Clock/duration as an mm:ss string. |
| `esg` | numeric | Even-strength goals. |
| `esa` | numeric | Even-strength assists. |
| `estp` | numeric | Even-strength points. |
| `essog` | numeric | Even-strength shots on goal. |
| `essw` | numeric | Even-strength shots wide. |
| `es_plus` | numeric | Even-strength goals for while on ice. |
| `es_minus` | numeric | Even-strength goals against while on ice. |
| `es_plus_minus` | numeric | Even-strength plus/minus. |
| `estoi_gp` | character | Even-strength time on ice per game (mm:ss). Clock/duration as an mm:ss string. |
| `eff_pim` | numeric | Effective penalty minutes (served). |
| `pers_pim` | numeric | Personal penalty minutes. |
| `off_pim` | numeric | Offensive penalty minutes. |
| `minor` | numeric | Minor penalties. |
| `major` | numeric | Major penalties. |
| `misc` | numeric | Misconduct penalties. |
| `gm` | numeric | Game misconducts. |
| `mp` | numeric | Match penalties. |
| `fo_perc` | numeric | Face-off win %. Numeric value sent as a string. |
| `fo` | numeric | Face-offs taken. |
| `fow` | numeric | Number of face-offs won. |
| `fol` | numeric | Face-offs lost. |
| `ofo_perc` | numeric | Offensive-zone face-offs won %. Numeric value sent as a string. |
| `ofo` | numeric | Offensive-zone face-offs. |
| `ofow` | numeric | Offensive-zone face-offs won. |
| `ofol` | numeric | Offensive-zone face-offs lost. |
| `dfo_perc` | numeric | Defensive-zone face-offs won %. Numeric value sent as a string. |
| `dfo` | numeric | Defensive-zone face-offs. |
| `dfow` | numeric | Defensive-zone face-offs won. |
| `dfol` | numeric | Defensive-zone face-offs lost. |
| `fo_violation_perc` | numeric | Face-off violation %. Numeric value sent as a string. |
| `fo_violations` | numeric | Face-off violations. |
| `sg_perc` | numeric | Shooting %. Numeric value sent as a string. |
| `ms` | numeric | Missed shots (wide + blocked). |
| `sog_perc` | numeric | Shots on goal % of attempts. Numeric value sent as a string. |
| `ps` | numeric | Number of penalty shots. |
| `psg` | numeric | Penalty-shot goals. |
| `ps_perc` | numeric | Penalty-shot conversion %. Numeric value sent as a string. |
| `so_ps` | numeric | Shootout shots. |
| `so_psg` | numeric | Shootout goals. |
| `so_ps_perc` | numeric | Shootout shooting %. Numeric value sent as a string. |
| `cf` | numeric | Corsi for (shot attempts for). |
| `ca` | numeric | Corsi against (shot attempts against). |
| `cf_perc` | numeric | Corsi for percentage. Numeric value sent as a string. |
| `close_cf_perc` | numeric | Corsi for percentage in close-score situations. Numeric, but sent as a string in some rows and as a number in others. |
| `ff` | numeric | Fenwick for (unblocked shot attempts for). |
| `fa` | numeric | Fenwick against (unblocked shot attempts against). |
| `ff_perc` | numeric | Fenwick for percentage. Numeric, but sent as a string in some rows and as a number in others. |
| `close_ff_perc` | numeric | Fenwick for percentage in close-score situations. Numeric, but sent as a string in some rows and as a number in others. |
| `on_ice_goal_perc` | numeric | On-ice shooting % (unverified). |
| `on_ice_svs_perc` | numeric | On-ice save % (unverified). Numeric, but sent as a string in some rows and as a number in others. |
| `pdo` | numeric | PDO (on-ice shooting % + on-ice save %). Numeric, but sent as a string in some rows and as a number in others. |
| `info_player_media` | character | Portrait image reference (mediaString, type). |
| `shga` | numeric | Shorthanded goals against (conceded while on the power play). |
| `ppga` | numeric | Power-play goals against (conceded while shorthanded). |
| `enga` | numeric | Empty-net goals against. |
| `pp_opp` | numeric | Power-play opportunities. |
| `pp_perc` | numeric | Power-play %. Numeric value sent as a string. |
| `pp_time` | character | Time on the power play (hh:mm:ss). Duration as an hh:mm:ss string. |
| `st_perc` | numeric | Special-teams % (power-play % + penalty-kill %). Numeric value sent as a string. |
| `pk_opp` | numeric | Times shorthanded. |
| `pk_perc` | numeric | Penalty-kill %. Numeric value sent as a string. |
| `ppsoga` | numeric | Power-play shots on goal against. |
| `pk_time` | character | Time shorthanded (hh:mm:ss). Duration as an hh:mm:ss string. |
| `gp_home` | numeric | Home games played. |
| `att_avg_home` | numeric | Average home attendance per game. |
| `att_total_home` | numeric | Total home attendance. |
| `att_avg_overall` | numeric | Average attendance per game. |
| `att_total_overall` | numeric | Total attendance. |
| `gp_road` | numeric | Away games played. |
| `att_avg_road` | numeric | Average away attendance per game. |
| `att_total_road` | numeric | Total away attendance. |
| `team_pen` | numeric | Team (bench) penalties. |
| `s_eff` | numeric | Shooting %. Numeric value sent as a string. |
| `pps_eff` | numeric | Power-play shooting %. Numeric value sent as a string. |
| `sw` | numeric | Number of shots that missed the net. |
| `sib` | numeric | Shots blocked (blocked attempts). |
| `ss` | numeric | Scoring-chance shots (site label; unverified). |
| `soga` | numeric | Shots on goal against. |
| `msa` | numeric | Missed shots against (wide + blocked). |
| `swa` | numeric | Shots wide against. |
| `sfa` | numeric | Shots on goal against (unverified). |
| `ssa` | numeric | Scoring-chance shots against (site label; unverified). |
| `fo_violations_perc` | numeric | Face-off violation %. |
| `psa` | numeric | Penalty-shot attempts. |
| `pssvs` | numeric | Penalty shots saved. |
| `so_psa` | numeric | Shootout attempts against. |
| `so_pssvs` | numeric | Shootout saves. |
| `so_pssvs_perc` | numeric | Shootout save %. Numeric value sent as a string. |
| `freezes` | numeric | Puck freezes (stoppages caused). |
| `freezes_against` | numeric | Puck freezes by the opponent. |
| `icings` | numeric | Icing calls against the team. |
| `icing_against` | numeric | Icings by the opponent. |
| `offsides` | numeric | Offside calls against the team. |
| `offsides_against` | numeric | Offsides by the opponent. |
| `ssga` | numeric | Scoring-chance goals against (unverified). |
| `ssoga` | numeric | Scoring-chance shots on goal against (unverified). |
| `sssvs_perc` | numeric | Save % on scoring-chance shots. Numeric value sent as a string. |
| `ppsvs_perc` | numeric | Save % against the power play (unverified). Numeric value sent as a string. |
| `shsoga` | numeric | Shorthanded shots on goal against. |
| `shsvs_perc` | numeric | Shorthanded save % (unverified). Numeric value sent as a string. |
| `esga` | numeric | Even-strength goals against. |
| `essoga` | numeric | Even-strength shots on goal against. |
| `essvs_perc` | numeric | Even-strength save %. Numeric value sent as a string. |
| `psga` | numeric | Penalty-shot goals against. |
| `pssvs_perc` | numeric | Penalty-shot save %. |
| `so_psga` | numeric | Shootout goals against. |
| `date_time` | character | Game date and time. |
| `home_team` | character | Home team code (e.g. BIF). |
| `guest_team` | character | Away team code (e.g. HV71). |
| `game_score_home_score` | integer | Goals scored by the home team. |
| `game_score_away_score` | integer | Goals scored by the away team. |
| `game_score_decided_in` | character | Filter values for how a game was decided. |
| `venue` | character | Name of the arena the game is played in. |
| `attendance` | numeric | Spectators at the game. |
| `info_game_id` | integer | StatNet game number (StatNet key space, e.g. 22003); keys stats-info games{} and equals sports-v2 game-info extId. Not the game uuid. |
| `info_home_team_id` | character | Home team code (e.g. FHC); not a uuid. |
| `info_away_team_id` | character | Away team code (e.g. LHC); not a uuid. |
| `diff` | numeric | Goal difference. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#shl_stats_info-example}

```python
shl_stats_info(module='players_summary', season=2026)
```

_Last validated n/a._
