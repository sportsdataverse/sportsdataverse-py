---
title: "DBL — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) — Player"
sidebar_label: "Player"
sidebar_position: 3
description: "DBL — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# DBL — Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) — Player

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
| `team_number` | integer | Team number in the game (1 home, 2 away) as Genius numbers the shirt table. |
| `pno` | integer | Genius shirt-table number of the player (player-stats only). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_player_box-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `sportsdataverse.errors.AssetFetchError` | an error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_player_box-example}

```python
sportality_player_box(game_uuid='c4hh5nig6t')
```

_Last validated n/a._

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
| `uuid` | character | Athlete uuid (platform key space). |
| `first_name` | character | Given (first) name(s) of the player. |
| `last_name` | character | Family name (surname) of the player. |
| `full_name` | character | Full display name of the player. |
| `nationality` | character | ISO 3166-1 alpha-2 country code. |
| `gender` | character | Gender of the person as recorded by the site (male in every row seen). |
| `jersey_number` | integer | Jersey (shirt) number of the player. |
| `position` | character | Position name (Guard, Forward, Center ...). |
| `position_code` | character | Position code. Values: `C` = center; `G` = guard; `N/A` = not applicable (career rows); `PF` = power forward; `PG` = point guard; `SF` = small forward; `SG` = shooting guard. |
| `age_value` | integer | Age in whole years. |
| `age_format` | character | Unit of the value (years). Values: `years` = years. |
| `height_value` | integer | Height of the player in centimetres. |
| `height_format` | character | Unit of the value (cm). Values: `cm` = centimetres. |
| `weight_value` | integer | Weight of the player in kilograms. |
| `weight_format` | character | Unit of the value (kg). Values: `kg` = kilograms. |
| `team_uuid` | character | Platform team uuid. |
| `team_instance_id` | character | Site instance id of the club (platform key space). |
| `team_name` | character | Name of the team. |
| `team_code` | character | Platform team code (HOL, RAN). |
| `team_media` | character | URL of the team logo image. |
| `media_url` | character | Portrait image URL (imgix CDN). |
| `media_alt` | character | Image alt text (''). |
| `media_srcset` | character | Responsive image set (imgix CDN URLs with widths). |
| `season_stats` | character | Season totals as field/value pairs (absent for some players). |
| `career_stats` | character | Career totals as field/value pairs (absent for some players). |
| `match_stats_nr` | integer | Jersey (shirt) number of the player. |
| `match_stats_pos` | character | Playing position code of the player (G, F, C, ...). |
| `match_stats_team_number` | integer | Team number in the game. |
| `match_stats_pno` | integer | Genius shirt-table number. |
| `match_stats_mins` | character | Minutes played 'MM:SS'. |
| `match_stats_pts` | integer | Number of points scored. |
| `match_stats_fg` | character | Field goals 'made-attempted'. |
| `match_stats_fg_perc` | numeric | Field-goal percentage, 0-100. |
| `match_stats_2_p` | character | Two-pointers 'made-attempted'. |
| `match_stats_2_p_perc` | numeric | Two-point percentage, 0-100. |
| `match_stats_3_p` | character | Three-pointers 'made-attempted'. |
| `match_stats_3_p_perc` | numeric | Three-point percentage, 0-100. |
| `match_stats_ft` | character | Free throws 'made-attempted'. |
| `match_stats_ft_perc` | numeric | Free-throw percentage, 0-100. |
| `match_stats_off` | integer | Offensive rebounds. |
| `match_stats_def` | integer | Defensive rebounds. |
| `match_stats_reb` | integer | Total rebounds. |
| `match_stats_ast` | integer | Number of assists credited. |
| `match_stats_to` | integer | Number of turnovers committed. |
| `match_stats_stl` | integer | Number of steals. |
| `match_stats_blk` | integer | Number of blocked shots made. |
| `match_stats_blkr` | integer | Blocks received. |
| `match_stats_pf` | integer | Personal fouls. |
| `match_stats_f_on` | integer | Fouls on (fouls drawn). |
| `match_stats_plus_minus` | integer | Plus/minus while on court. |
| `season_avg_stats` | character | The player's season averages (an object keyed by stat). |
| `statistics_provider` | character | Stats provider (genius-basketball). |
| `is_in_squad` | logical | Whether the player is in the squad. |
| `effective_ssgt_uuid` | character | The ssgt the head-to-head was computed for: the CURRENT season, whatever the path asked. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sportality_player_h2h-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site answered 404, an empty body (gameday routes before 2023/24, or an unknown game), a table with no rows, or the season and game type are not in layout-info. |
| `sportsdataverse.errors.AssetFetchError` | an error envelope or non-JSON 200, another game / team / athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not retried). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportality_player_h2h-example}

```python
sportality_player_h2h(away_team_uuid='gvvuxe9ysw', game_uuid='c4hh5nig6t', home_team_uuid='rfo82vr830', ssgt_uuid='xs07bd3fmp')
```

_Last validated n/a._
