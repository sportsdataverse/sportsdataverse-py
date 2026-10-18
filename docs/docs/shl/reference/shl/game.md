---
title: "SHL — SHL site API (www.shl.se/api, Sweden) — Game"
sidebar_label: "Game"
sidebar_position: 2
description: "SHL — SHL site API (www.shl.se/api, Sweden) — Game — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# SHL — SHL site API (www.shl.se/api, Sweden) — Game

## shl_game_header

Game header: StatNet extId, arena, state, teams, scores.

**Endpoint URL:** `GET https://www.shl.se/api/sports-v2/game-info/{game_uuid}`

**Valid URL:** [https://www.shl.se/api/sports-v2/game-info/bdhvuc5tex](https://www.shl.se/api/sports-v2/game-info/bdhvuc5tex)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_uuid` | `game_uuid` |  | `Y` |  | Game uuid (gameInfo[].uuid from /sports-v2/game-schedule). gameday/* has data from 2023/24 on; older games answer 200 with an empty body. |

### Returns {#shl_game_header-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `instance_context_site_team_is_in_game` | logical | Whether the site's team plays in the game. |
| `instance_context_site_team_is_home_team` | logical | Whether the site's team is the home team. |
| `instance_context_site_team_is_away_team` | logical | Whether the site's team is the away team. |
| `game_info_game_uuid` | character | Game uuid (platform key space, e.g. bdhvuc5tex); joins to schedule gameInfo[].uuid and every gameday/{game_uuid} route. The StatNet number of the same game is gameId. |
| `game_info_ext_id` | integer | StatNet game number as a string (StatNet key space); equals gameday gameId. Numeric value sent as a string. |
| `game_info_start_date_time` | character | Scheduled start, local time (Europe/Stockholm). |
| `game_info_arena_name` | character | Name of the arena hosting the game. |
| `game_info_state` | character | Game state with an underscore (post_game seen); the schedule spells it post-game. |
| `game_info_overtime` | logical | Whether the game went to overtime. |
| `game_info_shootout` | logical | Whether the game went to a shootout. |
| `game_info_series_code` | character | Series code of the game (SHL). |
| `game_info_series_name` | character | Series label of the game (SHL). |
| `game_info_round_number` | integer | Schedule round (1 = first game day of the season). |
| `game_info_round_label` | character | Round label (often empty). Empty string in every sample (placeholder). |
| `game_info_series_display_name` | character | Series label shown on the site (SHL). |
| `home_team_names_code` | character | Display abbreviation; differs from the team code for some clubs (SKE vs SAIK, ÖRE vs OHK). |
| `home_team_names_short` | character | Short club name ('Herr' = men's team). |
| `home_team_names_long` | character | Long club name ('Herr' = men's team). |
| `home_team_names_full` | character | Full club name ('Herr' = men's team). |
| `home_team_names_code_site` | character | Display abbreviation on the site; 'Herr' = men's team (e.g. 'DIF Herr'). |
| `home_team_names_short_site` | character | Short club name as shown on the site ('Herr' = men's team). |
| `home_team_names_full_site` | character | Full club name as shown on the site ('Herr' = men's team). |
| `home_team_names_long_site` | character | Long club name as shown on the site ('Herr' = men's team). |
| `home_team_uuid` | character | Team uuid (platform key space, e.g. 087a-087aTQv9u); joins to /sports-v2/teams/{team_uuid} and schedule homeTeamInfo/awayTeamInfo.uuid. Not the team code. |
| `home_team_instance_id` | character | Club site instance id (e.g. fhc1_fhc). |
| `home_team_founded_on` | character | Club founding date. Numeric value sent as a string. |
| `home_team_address` | character | Club postal address. |
| `home_team_email` | character | Contact email address of the club. |
| `home_team_icon` | character | URL of the team logo image. |
| `home_team_score` | integer | Goals scored by the team in the game. |
| `away_team_names_code` | character | Display abbreviation; differs from the team code for some clubs (SKE vs SAIK, ÖRE vs OHK). |
| `away_team_names_short` | character | Short club name ('Herr' = men's team). |
| `away_team_names_long` | character | Long club name ('Herr' = men's team). |
| `away_team_names_full` | character | Full club name ('Herr' = men's team). |
| `away_team_names_code_site` | character | Display abbreviation on the site; 'Herr' = men's team (e.g. 'DIF Herr'). |
| `away_team_names_short_site` | character | Short club name as shown on the site ('Herr' = men's team). |
| `away_team_names_full_site` | character | Full club name as shown on the site ('Herr' = men's team). |
| `away_team_names_long_site` | character | Long club name as shown on the site ('Herr' = men's team). |
| `away_team_uuid` | character | Team uuid (platform key space, e.g. 087a-087aTQv9u); joins to /sports-v2/teams/{team_uuid} and schedule homeTeamInfo/awayTeamInfo.uuid. Not the team code. |
| `away_team_instance_id` | character | Club site instance id (e.g. fhc1_fhc). |
| `away_team_founded_on` | character | Club founding date. Numeric value sent as a string. |
| `away_team_address` | character | Club postal address. |
| `away_team_email` | character | Contact email address of the club. |
| `away_team_icon` | character | URL of the team logo image. |
| `away_team_score` | integer | Goals scored by the team in the game. |
| `ssgt_uuid` | character | ssgt uuid (platform key space: one season + series + game type); label via layout-info seasonSeriesGameTypeTeams[]. |
| `series_uuid` | character | Series uuid (platform key space; SHL = qQ9-bb0bzEWUk); the seriesUuid param of game-schedule. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#shl_game_header-example}

```python
shl_game_header(game_uuid='bdhvuc5tex')
```

_Last validated n/a._

## shl_game_overview

Score, state, clock, StatNet gameId and gameSourceId.

**Endpoint URL:** `GET https://www.shl.se/api/gameday/game-overview/{game_uuid}`

**Valid URL:** [https://www.shl.se/api/gameday/game-overview/bdhvuc5tex](https://www.shl.se/api/gameday/game-overview/bdhvuc5tex)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_uuid` | `game_uuid` |  | `Y` |  | Game uuid (gameInfo[].uuid from /sports-v2/game-schedule). gameday/* has data from 2023/24 on; older games answer 200 with an empty body. |

### Returns {#shl_game_overview-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_uuid` | character | Platform game uuid (bdhvuc5tex); every gameday route takes it. |
| `home_team_game_source_id` | character | StatNet game key, date-HOME-AWAY (e.g. 20250913-FHC-LHC); the same game as gameId. |
| `home_team_game_id` | integer | StatNet game number (StatNet key space, e.g. 22003); keys stats-info games{} and equals sports-v2 game-info extId. Not the game uuid. |
| `home_team_place` | character | Side of the game: home or away. |
| `home_team_score` | integer | Goals scored by the team in the game. |
| `home_team_team_id` | character | Team code (e.g. FHC) in gameday and standings payloads; not the team uuid. |
| `home_team_team_name` | character | Team display name (e.g. Frölunda HC). |
| `home_team_team_code` | character | Team code (FHC, LHC, ...). |
| `home_team_game_uuid` | character | Game uuid (platform key space, e.g. bdhvuc5tex); joins to schedule gameInfo[].uuid and every gameday/{game_uuid} route. The StatNet number of the same game is gameId. |
| `away_team_game_source_id` | character | StatNet game key, date-HOME-AWAY (e.g. 20250913-FHC-LHC); the same game as gameId. |
| `away_team_game_id` | integer | StatNet game number (StatNet key space, e.g. 22003); keys stats-info games{} and equals sports-v2 game-info extId. Not the game uuid. |
| `away_team_place` | character | Side of the game: home or away. |
| `away_team_score` | integer | Goals scored by the team in the game. |
| `away_team_team_id` | character | Team code (e.g. FHC) in gameday and standings payloads; not the team uuid. |
| `away_team_team_name` | character | Team display name (e.g. Frölunda HC). |
| `away_team_team_code` | character | Team code (FHC, LHC, ...). |
| `away_team_game_uuid` | character | Game uuid (platform key space, e.g. bdhvuc5tex); joins to schedule gameInfo[].uuid and every gameday/{game_uuid} route. The StatNet number of the same game is gameId. |
| `home_goals` | integer | Home goals after this event (running score). |
| `away_goals` | integer | Away goals after this event (running score). |
| `state` | character | StatNet game state (GameEnded seen). |
| `time_period` | integer | Period (1-3, 4 = overtime, 5 = shootout). |
| `time_period_time` | character | Time in period (MM:SS). Clock/duration as an mm:ss string. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#shl_game_overview-example}

```python
shl_game_overview(game_uuid='bdhvuc5tex')
```

_Last validated n/a._

## shl_game_summary

Goals by period, head-to-head summary, best scorers.

**Endpoint URL:** `GET https://www.shl.se/api/gameday/game-info/{game_uuid}`

**Valid URL:** [https://www.shl.se/api/gameday/game-info/bdhvuc5tex](https://www.shl.se/api/gameday/game-info/bdhvuc5tex)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_uuid` | `game_uuid` |  | `Y` |  | Game uuid (gameInfo[].uuid from /sports-v2/game-schedule). gameday/* has data from 2023/24 on; older games answer 200 with an empty body. |

### Returns {#shl_game_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_head2_head` | character | Previous meetings of the two teams. |
| `team_period_caption` | character | Caption text of the image. |
| `team_period_overtime` | logical | Whether the game went to overtime. |
| `team_period_shootout` | logical | Whether the game went to a shootout. |
| `team_period_stats` | character | Team stats per period: rows with home and away values. |
| `best_scorers` | character | Top scorers of the game. |
| `statistics_provider` | character | Stats provider of the data (statnet seen). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#shl_game_summary-example}

```python
shl_game_summary(game_uuid='bdhvuc5tex')
```

_Last validated n/a._
