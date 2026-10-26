---
title: "Basketligaen — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) — Player"
sidebar_label: "Player"
sidebar_position: 3
description: "Basketligaen — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# Basketligaen — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) — Player

## sportality_player_box

Box score with jersey and Genius shirt numbers (2023/24 on).

**Endpoint URL:** `GET https://www.basketligaen.dk/api/gameday/player-stats/{game_uuid}`

**Valid URL:** [https://www.basketligaen.dk/api/gameday/player-stats/c4hh5nig6t](https://www.basketligaen.dk/api/gameday/player-stats/c4hh5nig6t)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_uuid` | `game_uuid` |  | `Y` |  | Game uuid: `gameInfo[].uuid` of the schedule (10 lowercase letters and digits). Key space: platform game uuids; the Genius match id is `extId` / `gameSourceId` in the body. |

### Returns {#sportality_player_box-returns}

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
| `team_number` | Int64 | Team number in the game (1 home, 2 away) as Genius numbers the shirt table. |
| `pno` | Int64 | Genius shirt-table number of the player (player-stats only). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_player_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_player_box-example}

```python
sportality_player_box(game_uuid='c4hh5nig6t')
```

## sportality_player_h2h

Player head-to-head of a matchup with per-game and season lines.

**Endpoint URL:** `GET https://www.basketligaen.dk/api/statistics-v2/player-h2h-v2/{ssgt_uuid}/{home_team_uuid}/{away_team_uuid}`

**Valid URL:** [https://www.basketligaen.dk/api/statistics-v2/player-h2h-v2/xs07bd3fmp/rfo82vr830/gvvuxe9ysw?gameUuid=c4hh5nig6t](https://www.basketligaen.dk/api/statistics-v2/player-h2h-v2/xs07bd3fmp/rfo82vr830/gvvuxe9ysw?gameUuid=c4hh5nig6t)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ssgt_uuid` | `ssgt_uuid` |  | `Y` |  | ssgt uuid: one season + series + game type, e.g. xs07bd3fmp = 2026/2027 regular season. Directory: `/statistics-v2/layout-info` `seasonSeriesGameTypeTeams[].ssgtUuid`. |
| `home_team_uuid` | `home_team_uuid` |  | `Y` |  | Home team uuid of the game. |
| `away_team_uuid` | `away_team_uuid` |  | `Y` |  | Away team uuid of the game. |
| `gameUuid` | `game_uuid` |  |  | `Y` | Game uuid; selects the game whose box-score lines the head-to-head carries. |

### Returns {#sportality_player_h2h-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `uuid` | String | Athlete uuid (platform key space). |
| `first_name` | String | Given (first) name(s) of the player. |
| `last_name` | String | Family name (surname) of the player. |
| `full_name` | String | Full display name of the player. |
| `nationality` | String | ISO 3166-1 alpha-2 country code. |
| `gender` | String | Gender of the person as recorded by the site (male in every row seen). |
| `jersey_number` | Int64 | Jersey (shirt) number of the player. |
| `position` | String | Position name (Guard, Forward, Center ...). |
| `position_code` | String | Position code. Values: `C` = center; `G` = guard; `N/A` = not applicable (career rows); `PF` = power forward; `PG` = point guard; `SF` = small forward; `SG` = shooting guard. |
| `age_value` | Int64 | Age in whole years. |
| `age_format` | String | Unit of the value (years). Values: `years` = years. |
| `height_value` | Int64 | Height of the player in centimetres. |
| `height_format` | String | Unit of the value (cm). Values: `cm` = centimetres. |
| `weight_value` | Int64 | Weight of the player in kilograms. |
| `weight_format` | String | Unit of the value (kg). Values: `kg` = kilograms. |
| `team_uuid` | String | Platform team uuid. |
| `team_instance_id` | String | Site instance id of the club (platform key space). |
| `team_name` | String | Name of the team. |
| `team_code` | String | Platform team code (HOL, RAN). |
| `team_media` | String | URL of the team logo image. |
| `media_url` | String | Portrait image URL (imgix CDN). |
| `media_alt` | String | Image alt text (''). |
| `media_srcset` | String | Responsive image set (imgix CDN URLs with widths). |
| `season_stats` | String | Season totals as field/value pairs (absent for some players). |
| `career_stats` | String | Career totals as field/value pairs (absent for some players). |
| `match_stats_nr` | Int64 | Jersey (shirt) number of the player. |
| `match_stats_pos` | String | Playing position code of the player (G, F, C, ...). |
| `match_stats_team_number` | Int64 | Team number in the game. |
| `match_stats_pno` | Int64 | Genius shirt-table number. |
| `match_stats_mins` | String | Minutes played 'MM:SS'. |
| `match_stats_pts` | Int64 | Number of points scored. |
| `match_stats_fg` | String | Field goals 'made-attempted'. |
| `match_stats_fg_perc` | Float64 | Field-goal percentage, 0-100. |
| `match_stats_2_p` | String | Two-pointers 'made-attempted'. |
| `match_stats_2_p_perc` | Float64 | Two-point percentage, 0-100. |
| `match_stats_3_p` | String | Three-pointers 'made-attempted'. |
| `match_stats_3_p_perc` | Float64 | Three-point percentage, 0-100. |
| `match_stats_ft` | String | Free throws 'made-attempted'. |
| `match_stats_ft_perc` | Float64 | Free-throw percentage, 0-100. |
| `match_stats_off` | Int64 | Offensive rebounds. |
| `match_stats_def` | Int64 | Defensive rebounds. |
| `match_stats_reb` | Int64 | Total rebounds. |
| `match_stats_ast` | Int64 | Number of assists credited. |
| `match_stats_to` | Int64 | Number of turnovers committed. |
| `match_stats_stl` | Int64 | Number of steals. |
| `match_stats_blk` | Int64 | Number of blocked shots made. |
| `match_stats_blkr` | Int64 | Blocks received. |
| `match_stats_pf` | Int64 | Personal fouls. |
| `match_stats_f_on` | Int64 | Fouls on (fouls drawn). |
| `match_stats_plus_minus` | Int64 | Plus/minus while on court. |
| `season_avg_stats` | String | The player's season averages (an object keyed by stat). |
| `statistics_provider` | String | Stats provider (genius-basketball). |
| `is_in_squad` | Boolean | Whether the player is in the squad. |
| `effective_ssgt_uuid` | String | The ssgt the head-to-head was computed for: the CURRENT season, whatever the path asked. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_player_h2h-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. An error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_player_h2h-example}

```python
sportality_player_h2h(away_team_uuid='gvvuxe9ysw', game_uuid='c4hh5nig6t', home_team_uuid='rfo82vr830', ssgt_uuid='xs07bd3fmp')
```
