---
title: "SHL — SHL site API (www.shl.se/api, Sweden) — Team"
sidebar_label: "Team"
sidebar_position: 3
description: "SHL — SHL site API (www.shl.se/api, Sweden) — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# SHL — SHL site API (www.shl.se/api, Sweden) — Team

## shl_team

One team.

**Endpoint URL:** `GET https://www.shl.se/api/sports-v2/teams/{team_uuid}`

**Valid URL:** [https://www.shl.se/api/sports-v2/teams/087a-087aTQv9u](https://www.shl.se/api/sports-v2/teams/087a-087aTQv9u)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_uuid` | `team_uuid` |  | `Y` |  | Team uuid (homeTeamInfo.uuid / awayTeamInfo.uuid in the schedule, or teamList[].uuid). |

### Returns {#shl_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `names_code` | String | Display abbreviation; differs from the team code for some clubs (SKE vs SAIK, ÖRE vs OHK). |
| `names_short` | String | Short club name ('Herr' = men's team). |
| `names_long` | String | Long club name ('Herr' = men's team). |
| `names_full` | String | Full club name ('Herr' = men's team). |
| `names_code_site` | String | Display abbreviation on the site; 'Herr' = men's team (e.g. 'DIF Herr'). |
| `names_short_site` | String | Short club name as shown on the site ('Herr' = men's team). |
| `names_full_site` | String | Full club name as shown on the site ('Herr' = men's team). |
| `names_long_site` | String | Long club name as shown on the site ('Herr' = men's team). |
| `info_founded` | String | Club founding year (text). Numeric value sent as a string. |
| `info_golds` | String | Club's championships (text). |
| `info_finals` | String | Club's final appearances (text). |
| `info_retired_numbers` | String | Retired jersey numbers (text). |
| `info_address` | String | Club postal address. |
| `info_phone` | String | Telephone number of the club. |
| `info_fax` | String | Fax number of the club. |
| `info_email` | String | Contact email address of the club. |
| `info_chairman` | String | Name of the club chairman. |
| `info_manager` | String | Club general manager. |
| `info_accreditation` | String | Press-accreditation contact, free Swedish text (e.g. 'Senast dagen före match till …' = 'no later than the day before the game, to …'). |
| `info_facebook` | String | Club Facebook URL. |
| `info_instagram` | String | Club Instagram URL. |
| `info_twitter` | String | Club X/Twitter URL. |
| `contact` | String | Club contact text. |
| `uuid` | String | Team uuid (platform key space, e.g. 087a-087aTQv9u); joins to /sports-v2/teams/{team_uuid} and schedule homeTeamInfo/awayTeamInfo.uuid. Not the team code. |
| `nationality` | String | Nationality (ISO country code). |
| `arena_name` | String | Name of the arena hosting the game. Empty string in every sample (placeholder). |
| `public_url` | String | Public image URL. |
| `icon` | String | URL of the team logo image. |
| `owner_instance_id` | String | Club site instance id (e.g. fhc1_fhc); also names the logo at sportality.cdn.s8y.se/team-logos/{id}.svg. |
| `site_display_name` | String | Club name as shown on its own site (e.g. Frölunda HC). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_team-example}

```python
shl_team(team_uuid='087a-087aTQv9u')
```

## shl_team_played_games

A team's recent games.

**Endpoint URL:** `GET https://www.shl.se/api/sports-v2/played-games/{team_uuid}`

**Valid URL:** [https://www.shl.se/api/sports-v2/played-games/087a-087aTQv9u](https://www.shl.se/api/sports-v2/played-games/087a-087aTQv9u)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_uuid` | `team_uuid` |  | `Y` |  | Team uuid (homeTeamInfo.uuid / awayTeamInfo.uuid in the schedule, or teamList[].uuid). |

### Returns {#shl_team_played_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `date` | String | Date (YYYY-MM-DD). |
| `time` | String | Game clock in the period (MM:SS, counting up from 00:00). Clock/duration as an mm:ss string. |
| `game_uuid` | String | Platform game uuid (bdhvuc5tex); every gameday route takes it. |
| `start_date_time` | String | Scheduled start, local time (Europe/Stockholm). |
| `state` | String | Game state: pre-game or post-game (hyphenated; a live value was not observed). |
| `overtime` | Boolean | Whether the game went to overtime. |
| `shootout` | Boolean | Whether the game went to a shootout. |
| `ssgt_uuid` | String | ssgt uuid (platform key space: one season + series + game type); label via layout-info seasonSeriesGameTypeTeams[]. |
| `home_team_status` | String | Result for the team (WIN, LOSE) or record status. |
| `home_team_uuid` | String | Team uuid (platform key space, e.g. 087a-087aTQv9u); joins to /sports-v2/teams/{team_uuid} and schedule homeTeamInfo/awayTeamInfo.uuid. Not the team code. |
| `home_team_owner_instance_id` | String | Club site instance id (e.g. fhc1_fhc); also names the logo at sportality.cdn.s8y.se/team-logos/{id}.svg. |
| `home_team_code` | String | Home team code (e.g. FHC); joins to gameday teamId and the code-keyed teams{} maps. |
| `home_team_names_code` | String | Display abbreviation; differs from the team code for some clubs (SKE vs SAIK, ÖRE vs OHK). |
| `home_team_names_short` | String | Short club name ('Herr' = men's team). |
| `home_team_names_long` | String | Long club name ('Herr' = men's team). |
| `home_team_names_full` | String | Full club name ('Herr' = men's team). |
| `home_team_names_code_site` | String | Display abbreviation on the site; 'Herr' = men's team (e.g. 'DIF Herr'). |
| `home_team_names_short_site` | String | Short club name as shown on the site ('Herr' = men's team). |
| `home_team_names_full_site` | String | Full club name as shown on the site ('Herr' = men's team). |
| `home_team_names_long_site` | String | Long club name as shown on the site ('Herr' = men's team). |
| `home_team_score` | Int64 | Goals scored by the team in the game. |
| `home_team_icon` | String | URL of the team logo image. |
| `away_team_status` | String | Result for the team (WIN, LOSE) or record status. |
| `away_team_uuid` | String | Team uuid (platform key space, e.g. 087a-087aTQv9u); joins to /sports-v2/teams/{team_uuid} and schedule homeTeamInfo/awayTeamInfo.uuid. Not the team code. |
| `away_team_owner_instance_id` | String | Club site instance id (e.g. fhc1_fhc); also names the logo at sportality.cdn.s8y.se/team-logos/{id}.svg. |
| `away_team_code` | String | Away team code (e.g. LHC); joins to gameday teamId and the code-keyed teams{} maps. |
| `away_team_names_code` | String | Display abbreviation; differs from the team code for some clubs (SKE vs SAIK, ÖRE vs OHK). |
| `away_team_names_short` | String | Short club name ('Herr' = men's team). |
| `away_team_names_long` | String | Long club name ('Herr' = men's team). |
| `away_team_names_full` | String | Full club name ('Herr' = men's team). |
| `away_team_names_code_site` | String | Display abbreviation on the site; 'Herr' = men's team (e.g. 'DIF Herr'). |
| `away_team_names_short_site` | String | Short club name as shown on the site ('Herr' = men's team). |
| `away_team_names_full_site` | String | Full club name as shown on the site ('Herr' = men's team). |
| `away_team_names_long_site` | String | Long club name as shown on the site ('Herr' = men's team). |
| `away_team_score` | Int64 | Goals scored by the team in the game. |
| `away_team_icon` | String | URL of the team logo image. |
| `venue_uuid` | String | Arena uuid (platform key space). |
| `venue_name` | String | Name of the arena (venue). |
| `series_uuid` | String | Series uuid (platform key space; SHL = qQ9-bb0bzEWUk). |
| `series_code` | String | Series code (SHL, CHL). |
| `series_display_name` | String | Series label shown on the site (SHL, CHL). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_team_played_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_team_played_games-example}

```python
shl_team_played_games(team_uuid='087a-087aTQv9u')
```

## shl_team_stats

Team stats per period and total (2023/24 on).

**Endpoint URL:** `GET https://www.shl.se/api/gameday/team-stats/{game_uuid}`

**Valid URL:** [https://www.shl.se/api/gameday/team-stats/bdhvuc5tex](https://www.shl.se/api/gameday/team-stats/bdhvuc5tex)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_uuid` | `game_uuid` |  | `Y` |  | Game uuid (gameInfo[].uuid from /sports-v2/game-schedule). gameday/* has data from 2023/24 on; older games answer 200 with an empty body. |

### Returns {#shl_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_side` | String | Side of the game: home or away. |
| `game_source_id` | String | StatNet game key, date-HOME-AWAY (e.g. 20250913-FHC-LHC); the same game as gameId. |
| `game_id` | Int64 | StatNet game number (StatNet key space, e.g. 22003); keys stats-info games{} and equals sports-v2 game-info extId. Not the game uuid. |
| `place` | String | Side of the game: home or away. |
| `score` | Int64 | Goals scored by the team in the game. |
| `team_id` | String | Team code (e.g. FHC) in gameday and standings payloads; not the team uuid. |
| `team_name` | String | Name of the team. |
| `team_code` | String | StatNet team code (SAIK, FHC). |
| `game_uuid` | String | Platform game uuid (bdhvuc5tex); every gameday route takes it. |
| `period` | Int64 | Period: 0 = the whole game, 1-3 the periods, 4 and up overtime. |
| `g` | Float64 | Number of goals scored. |
| `pim` | Float64 | Penalty minutes. |
| `fow` | Float64 | Number of face-offs won. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_team_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_team_stats-example}

```python
shl_team_stats(game_uuid='bdhvuc5tex')
```

## shl_team_upcoming_games

A team's next games.

**Endpoint URL:** `GET https://www.shl.se/api/sports-v2/upcoming-games/{team_uuid}`

**Valid URL:** [https://www.shl.se/api/sports-v2/upcoming-games/087a-087aTQv9u](https://www.shl.se/api/sports-v2/upcoming-games/087a-087aTQv9u)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_uuid` | `team_uuid` |  | `Y` |  | Team uuid (homeTeamInfo.uuid / awayTeamInfo.uuid in the schedule, or teamList[].uuid). |
| `gamePlace` | `game_place` |  |  | `Y` | Home/away filter; empty means all. No effect in the drop-one probe. |

### Returns {#shl_team_upcoming_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `date` | String | Date (YYYY-MM-DD). |
| `time` | String | Game clock in the period (MM:SS, counting up from 00:00). Clock/duration as an mm:ss string. |
| `game_uuid` | String | Platform game uuid (bdhvuc5tex); every gameday route takes it. |
| `start_date_time` | String | Scheduled start, local time (Europe/Stockholm). |
| `state` | String | Game state: pre-game or post-game (hyphenated; a live value was not observed). |
| `overtime` | Boolean | Whether the game went to overtime. |
| `shootout` | Boolean | Whether the game went to a shootout. |
| `ssgt_uuid` | String | ssgt uuid (platform key space: one season + series + game type); label via layout-info seasonSeriesGameTypeTeams[]. |
| `home_team_status` | String | Result for the team (WIN, LOSE) or record status. |
| `home_team_uuid` | String | Team uuid (platform key space, e.g. 087a-087aTQv9u); joins to /sports-v2/teams/{team_uuid} and schedule homeTeamInfo/awayTeamInfo.uuid. Not the team code. |
| `home_team_owner_instance_id` | String | Club site instance id (e.g. fhc1_fhc); also names the logo at sportality.cdn.s8y.se/team-logos/{id}.svg. |
| `home_team_code` | String | Home team code (e.g. FHC); joins to gameday teamId and the code-keyed teams{} maps. |
| `home_team_names_code` | String | Display abbreviation; differs from the team code for some clubs (SKE vs SAIK, ÖRE vs OHK). |
| `home_team_names_short` | String | Short club name ('Herr' = men's team). |
| `home_team_names_long` | String | Long club name ('Herr' = men's team). |
| `home_team_names_full` | String | Full club name ('Herr' = men's team). |
| `home_team_names_code_site` | String | Display abbreviation on the site; 'Herr' = men's team (e.g. 'DIF Herr'). |
| `home_team_names_short_site` | String | Short club name as shown on the site ('Herr' = men's team). |
| `home_team_names_full_site` | String | Full club name as shown on the site ('Herr' = men's team). |
| `home_team_names_long_site` | String | Long club name as shown on the site ('Herr' = men's team). |
| `home_team_score` | Int64 | Goals scored by the team in the game. |
| `home_team_icon` | String | URL of the team logo image. |
| `away_team_status` | String | Result for the team (WIN, LOSE) or record status. |
| `away_team_uuid` | String | Team uuid (platform key space, e.g. 087a-087aTQv9u); joins to /sports-v2/teams/{team_uuid} and schedule homeTeamInfo/awayTeamInfo.uuid. Not the team code. |
| `away_team_owner_instance_id` | String | Club site instance id (e.g. fhc1_fhc); also names the logo at sportality.cdn.s8y.se/team-logos/{id}.svg. |
| `away_team_code` | String | Away team code (e.g. LHC); joins to gameday teamId and the code-keyed teams{} maps. |
| `away_team_names_code` | String | Display abbreviation; differs from the team code for some clubs (SKE vs SAIK, ÖRE vs OHK). |
| `away_team_names_short` | String | Short club name ('Herr' = men's team). |
| `away_team_names_long` | String | Long club name ('Herr' = men's team). |
| `away_team_names_full` | String | Full club name ('Herr' = men's team). |
| `away_team_names_code_site` | String | Display abbreviation on the site; 'Herr' = men's team (e.g. 'DIF Herr'). |
| `away_team_names_short_site` | String | Short club name as shown on the site ('Herr' = men's team). |
| `away_team_names_full_site` | String | Full club name as shown on the site ('Herr' = men's team). |
| `away_team_names_long_site` | String | Long club name as shown on the site ('Herr' = men's team). |
| `away_team_score` | Int64 | Goals scored by the team in the game. |
| `away_team_icon` | String | URL of the team logo image. |
| `venue_uuid` | String | Arena uuid (platform key space). |
| `venue_name` | String | Name of the arena (venue). |
| `series_uuid` | String | Series uuid (platform key space; SHL = qQ9-bb0bzEWUk). |
| `series_code` | String | Series code (SHL, CHL). |
| `series_display_name` | String | Series label shown on the site (SHL, CHL). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_team_upcoming_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_team_upcoming_games-example}

```python
shl_team_upcoming_games(team_uuid='087a-087aTQv9u')
```
