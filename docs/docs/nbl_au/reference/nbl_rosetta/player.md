---
title: "NBL Australia (Rosetta) — NBL / WNBL / NBL1 Rosetta proxy (prod.rosetta.nbl.com.au, Australia) — Player"
sidebar_label: "Player"
sidebar_position: 3
description: "NBL Australia (Rosetta) — NBL / WNBL / NBL1 Rosetta proxy (prod.rosetta.nbl.com.au, Australia) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBL Australia (Rosetta) — NBL / WNBL / NBL1 Rosetta proxy (prod.rosetta.nbl.com.au, Australia) — Player

## nbl_rosetta_players

Roster rows of every team in a season: player bio, jersey, position and team.

**Endpoint URL:** `GET https://prod.rosetta.nbl.com.au/get/{org}/players/in/season/{season}`

**Valid URL:** [https://prod.rosetta.nbl.com.au/get/nbl/players/in/season/2026](https://prod.rosetta.nbl.com.au/get/nbl/players/in/season/2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26). The runtime maps it to Rosetta's year: the START year for the NBL and WNBL from 1998-99, the calendar year for NBL1 and the NBL / WNBL winter seasons up to 1998. END 1998 and 1999 both read Rosetta year 1998, which holds two seasons; the matches frame keeps the one asked for. |
| `org` | `org` |  |  | `Y` | Organisation: nbl (men's NBL), wnbl (women's) or nbl1 (the second tier, men's and women's conferences). |
| `limit` | `limit` |  |  | `Y` | Rows to return. Omitted = a page large enough for the whole list (a short page cut by Rosetta's path cache is refused). |
| `offset` | `offset` |  |  | `Y` | Rows to skip. Pages can overlap, so prefer one large limit. |

### Returns {#nbl_rosetta_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `org` | String | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `jersey_number` | String | Jersey number, a STRING. |
| `playing_position` | String | Playing position code (G, F, C, PG, SF, FC, ...); null when not recorded. |
| `player_id` | String | DataCore id (UUID v1, Sportradar) of the player; stable across seasons and the id to join on across routes. |
| `rosetta_player_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the player; routes that take this entity accept it as well as the DataCore id. |
| `player_date_updated` | String | Record last-edit time, ISO 8601 UTC. |
| `player_first_name` | String | The player's given name, e.g. Elijah. |
| `player_last_name` | String | The player's family name, e.g. Pepper. |
| `player_date_of_birth` | String | Date of birth, ISO date. |
| `player_height` | Float64 | Height in centimetres. |
| `player_weight` | Float64 | Weight in kilograms. |
| `player_dominant_hand` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `player_gender` | String | Gender (MALE/FEMALE upper case on seasons and players, male/female lower case on competitions and divisions; UNKNOWN seen) |
| `player_nationality` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `player_nationality_code` | String | Nationality, ISO 3166-1 alpha-3 code. |
| `player_dominant_foot` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `player_jersey_number` | String | Jersey number, a STRING. |
| `player_playing_position` | String | Playing position code (G, F, C, PG, SF, FC, ...); null when not recorded. |
| `player_active` | Boolean | Player record is active. |
| `player_league` | String | Rosetta league record id, UUID v4. |
| `player_birthplace` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `player_rookie_year` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `player_person` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `player_latest_jersey_number` | String | Player's most recent jersey number, a STRING. |
| `player_full_name` | String | Given and family name. |
| `player_source_updated_at` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `player_source_provider` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `team_id` | String | DataCore id (UUID v1, Sportradar) of the team; stable across seasons and the id to join on across routes. |
| `rosetta_team_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the team; routes that take this entity accept it as well as the DataCore id. |
| `team_code` | String | Team code, e.g. PER (the {team_code} path value) |
| `team_name` | String | Name of the team as Rosetta spells it, e.g. Perth Wildcats. |
| `season_competition_division` | String | NBL1 divisions (conference x gender); empty for NBL and WNBL. |
| `season_end_year` | Int64 | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_players-example}

```python
nbl_rosetta_players(org='nbl', season=2026)
```

## nbl_rosetta_player_game_log

A player's game log for a season: the box-score line of each game (period 0 = whole game) and the game.

**Endpoint URL:** `GET https://prod.rosetta.nbl.com.au/get/{org}/player_boxscores/for/{player_id}/in/season/{season}/regular`

**Valid URL:** [https://prod.rosetta.nbl.com.au/get/nbl/player_boxscores/for/7619d3ea-410d-11f0-9235-2902fc2f936f/in/season/2026/regular?filter%5Bparticipated%5D=1](https://prod.rosetta.nbl.com.au/get/nbl/player_boxscores/for/7619d3ea-410d-11f0-9235-2902fc2f936f/in/season/2026/regular?filter%5Bparticipated%5D=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Player id: the DataCore person id (player_id of nbl_rosetta_players) or Rosetta's own id; either is accepted. |
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26). The runtime maps it to Rosetta's year: the START year for the NBL and WNBL from 1998-99, the calendar year for NBL1 and the NBL / WNBL winter seasons up to 1998. END 1998 and 1999 both read Rosetta year 1998, which holds two seasons; the matches frame keeps the one asked for. |
| `org` | `org` |  |  | `Y` | Organisation: nbl (men's NBL), wnbl (women's) or nbl1 (the second tier, men's and women's conferences). |
| `filter[participated]` | `participated` |  |  | `Y` | 1 = only games the player played in (without it, games on the roster that he sat out too). |

### Returns {#nbl_rosetta_player_game_log-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `org` | String | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `period` | Int64 | Period as a STRING: 0 = game or season total, 1-4 = quarters, 11/12/13 = first/second/third overtime (inferred: fewer teams have each) |
| `playing_position` | String | Playing position code (G, F, C, PG, SF, FC, ...); null when not recorded. |
| `points` | Int64 | Points in the game. |
| `rebounds` | Int64 | Total rebounds in the game. |
| `assists` | Int64 | Assists in the game. |
| `blocks` | Int64 | Blocks in the game. |
| `steals` | Int64 | Steals in the game. |
| `turnovers` | Int64 | Turnovers in the game. |
| `offensive_rebounds` | Int64 | Offensive rebounds in the game. |
| `defensive_rebounds` | Int64 | Defensive rebounds in the game. |
| `personal_fouls` | Int64 | Personal fouls in the game. |
| `three_points_attempted` | Int64 | Three-point field goals attempted in the game. |
| `three_points_made` | Int64 | Three-point field goals made in the game. |
| `three_points_percentage` | Float64 | Three-point field goals made / attempted, a 0-1 FRACTION. |
| `two_points_attempted` | Int64 | Two-point field goals attempted in the game. |
| `two_points_made` | Int64 | Two-point field goals made in the game. |
| `two_points_percentage` | Float64 | Two-point field goals made / attempted, a 0-1 FRACTION. |
| `field_goals_attempted` | Int64 | Field goals attempted in the game. |
| `field_goals_made` | Int64 | Field goals made in the game. |
| `field_goals_percentage` | Float64 | Field goals made / attempted, a 0-1 FRACTION. |
| `free_throws_attempted` | Int64 | Free throws attempted in the game. |
| `free_throws_made` | Int64 | Free throws made in the game. |
| `free_throws_percentage` | Float64 | Free throws made / attempted, a 0-1 FRACTION. |
| `minutes` | Float64 | Minutes played, decimal minutes. |
| `participated` | Boolean | Player took part in the game. |
| `match_id` | String | DataCore fixture id of the match (UUID v1): the {match_id} route key and fixtureId in play-by-play. Join on it. |
| `rosetta_match_id` | String | Rosetta's match RECORD id (UUID v4) as carried by box-score and play-by-play rows; no route accepts it. Join on match_id. |
| `match_start_time_datetime` | String | Tip-off, UTC; same value as start_time. |
| `match_status` | String | Status of the match in Rosetta's words (complete for a finished game): box score and play-by-play of any other status are partial. |
| `match_season_id` | String | DataCore id (UUID v1, Sportradar) of the match's season; stable across seasons and the id to join on across routes. |
| `rosetta_match_season_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the match's season; routes that take this entity accept it as well as the DataCore id. |
| `match_season_year` | Int64 | Season year as a STRING. NBL and WNBL from 1998-99: the START year ("2025" = 2025-26, sdv END = year + 1). NBL and WNBL 1979-1997 and every NBL1 season: the calendar year the season was played (END = year). Year 1998 holds two NBL and two WNBL seasons (calendar 1998 and 1998-99): tell them apart by start_date. |
| `match_season_type` | String | Season type: regular (includes finals), preseason, in_season (cups, blitz, exhibition), test. |
| `rosetta_match_home_team_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the match's home team; routes that take this entity accept it as well as the DataCore id. |
| `match_home_team_name` | String | Display name of the team. |
| `match_home_team_code` | String | Team code of the match's home team, e.g. PER or SYD. |
| `rosetta_match_away_team_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the match's away team; routes that take this entity accept it as well as the DataCore id. |
| `match_away_team_name` | String | Display name of the team. |
| `match_away_team_code` | String | Team code of the match's away team, e.g. PER or SYD. |
| `player_id` | String | DataCore id (UUID v1, Sportradar) of the player; stable across seasons and the id to join on across routes. |
| `rosetta_player_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the player; routes that take this entity accept it as well as the DataCore id. |
| `player_jersey_number` | String | Jersey number, a STRING. |
| `player_first_name` | String | The player's given name, e.g. Elijah. |
| `player_last_name` | String | The player's family name, e.g. Pepper. |
| `team_id` | String | DataCore id (UUID v1, Sportradar) of the team; stable across seasons and the id to join on across routes. |
| `rosetta_team_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the team; routes that take this entity accept it as well as the DataCore id. |
| `team_name` | String | Name of the team as Rosetta spells it, e.g. Perth Wildcats. |
| `team_code` | String | Team code, e.g. PER (the {team_code} path value) |
| `sc_total_score` | Int64 | SuperCoach score including double-double / triple-double bonuses. |
| `sc_total_score_without_dd_and_td` | Int64 | SuperCoach score without those bonuses. |
| `sc_breakdown_points_value` | Int64 | The stat's value for the game or season. |
| `sc_breakdown_points_points` | Int64 | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_rebounds_value` | Int64 | The stat's value for the game or season. |
| `sc_breakdown_rebounds_points` | Int64 | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_assists_value` | Int64 | The stat's value for the game or season. |
| `sc_breakdown_assists_points` | Int64 | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_blocks_value` | Int64 | The stat's value for the game or season. |
| `sc_breakdown_blocks_points` | Int64 | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_steals_value` | Int64 | The stat's value for the game or season. |
| `sc_breakdown_steals_points` | Int64 | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_turnovers_value` | Int64 | The stat's value for the game or season. |
| `sc_breakdown_turnovers_points` | Int64 | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_double_figure_count` | Int64 | Stats in double figures (for the double-double / triple-double bonus) |
| `sc_season_total` | Int64 | Player's SuperCoach total for the season so far. |
| `season_end_year` | Int64 | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_player_game_log-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_player_game_log-example}

```python
nbl_rosetta_player_game_log(org='nbl', participated=1, player_id='7619d3ea-410d-11f0-9235-2902fc2f936f', season=2026)
```

## nbl_rosetta_player_stats

A player's season totals and averages for every season, by period.

**Endpoint URL:** `GET https://prod.rosetta.nbl.com.au/get/{org}/statistics/for/player/{player_id}`

**Valid URL:** [https://prod.rosetta.nbl.com.au/get/nbl/statistics/for/player/7619d3ea-410d-11f0-9235-2902fc2f936f](https://prod.rosetta.nbl.com.au/get/nbl/statistics/for/player/7619d3ea-410d-11f0-9235-2902fc2f936f)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Player id: the DataCore person id (player_id of nbl_rosetta_players) or Rosetta's own id; either is accepted. |
| `org` | `org` |  |  | `Y` | Organisation: nbl (men's NBL), wnbl (women's) or nbl1 (the second tier, men's and women's conferences). |

### Returns {#nbl_rosetta_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `org` | String | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `assists_average` | Float64 | Assists per game. |
| `blocks_average` | Float64 | Blocks per game. |
| `field_goals_attempted` | Int64 | Field goals attempted, season total. |
| `field_goals_made` | Int64 | Field goals made, season total. |
| `field_goals_attempted_average` | Float64 | Field goals attempted per game. |
| `field_goals_made_average` | Float64 | Field goals made per game. |
| `field_goals_percentage` | Float64 | Field goals made / attempted, a 0-1 FRACTION. |
| `fouls_average` | Float64 | Fouls committed (personal + technical + others) per game. |
| `free_throws_attempted` | Int64 | Free throws attempted, season total. |
| `free_throws_made` | Int64 | Free throws made, season total. |
| `free_throws_attempted_average` | Float64 | Free throws attempted per game. |
| `free_throws_made_average` | Float64 | Free throws made per game. |
| `free_throws_percentage` | Float64 | Free throws made / attempted, a 0-1 FRACTION. |
| `games` | Int64 | Games played in the season (in the period rows, by the player or team). |
| `games_started` | Int64 | Games in the starting five. |
| `offensive_rebounds` | Int64 | Offensive rebounds, season total. |
| `fouls` | Int64 | Fouls committed (personal + technical + others), season total. |
| `personal_fouls` | Int64 | Personal fouls, season total. |
| `technical_fouls` | Int64 | Technical fouls, season total. |
| `period` | Int64 | Period as a STRING: 0 = game or season total, 1-4 = quarters, 11/12/13 = first/second/third overtime (inferred: fewer teams have each) |
| `three_points_attempted` | Int64 | Three-point field goals attempted, season total. |
| `three_points_attempted_average` | Float64 | Three-point field goals attempted per game. |
| `three_points_made` | Int64 | Three-point field goals made, season total. |
| `three_points_made_average` | Float64 | Three-point field goals made per game. |
| `three_points_percentage` | Float64 | Three-point field goals made / attempted, a 0-1 FRACTION. |
| `turnovers_average` | Float64 | Turnovers per game. |
| `turnovers` | Int64 | Turnovers, season total. |
| `two_points_attempted` | Int64 | Two-point field goals attempted, season total. |
| `two_points_attempted_average` | Float64 | Two-point field goals attempted per game. |
| `two_points_made` | Int64 | Two-point field goals made, season total. |
| `two_points_made_average` | Float64 | Two-point field goals made per game. |
| `two_points_percentage` | Float64 | Two-point field goals made / attempted, a 0-1 FRACTION. |
| `minutes` | Float64 | Minutes played, decimal minutes. |
| `minutes_average` | Float64 | Minutes per game, decimal minutes. |
| `points` | Int64 | Points, season total. |
| `rebounds` | Int64 | Total rebounds, season total. |
| `assists` | Int64 | Assists, season total. |
| `blocks` | Int64 | Blocks, season total. |
| `steals` | Int64 | Steals, season total. |
| `plus_minus` | Int64 | Point differential while on court, season total. |
| `plus_minus_average` | Float64 | Point differential while on court per game. |
| `efficiency` | Int64 | Efficiency rating (PTS + REB + AST + STL + BLK - missed FG - missed FT - TOV), season total. |
| `efficiency_average` | Float64 | Efficiency rating (PTS + REB + AST + STL + BLK - missed FG - missed FT - TOV) per game. |
| `points_average` | Float64 | Points per game. |
| `rebounds_average` | Float64 | Total rebounds per game. |
| `steals_average` | Float64 | Steals per game. |
| `player_id` | String | DataCore id (UUID v1, Sportradar) of the player; stable across seasons and the id to join on across routes. |
| `rosetta_player_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the player; routes that take this entity accept it as well as the DataCore id. |
| `player_first_name` | String | The player's given name, e.g. Elijah. |
| `player_jersey_number` | String | Jersey number, a STRING. |
| `player_last_name` | String | The player's family name, e.g. Pepper. |
| `player_playing_position` | String | Playing position code (G, F, C, PG, SF, FC, ...); null when not recorded. |
| `team_id` | String | DataCore id (UUID v1, Sportradar) of the team; stable across seasons and the id to join on across routes. |
| `rosetta_team_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the team; routes that take this entity accept it as well as the DataCore id. |
| `team_name` | String | Name of the team as Rosetta spells it, e.g. Perth Wildcats. |
| `team_code` | String | Team code, e.g. PER (the {team_code} path value) |
| `season_id` | String | DataCore id (UUID v1, Sportradar) of the season; stable across seasons and the id to join on across routes. |
| `rosetta_season_id` | String | Rosetta's own id (UUID v4; v5 on NBL1) of the season; routes that take this entity accept it as well as the DataCore id. |
| `season_year` | Int64 | Rosetta's year of the season: the START year for the NBL and WNBL from 1998-99 (2025 = 2025-26), the calendar year for NBL1 and the 1979-1997 winter seasons. See season_end_year for the sdv key. |
| `season_type` | String | Season type: regular (includes finals), preseason, in_season (cups, blitz, exhibition), test. |
| `season_competition_division` | String | NBL1 divisions (conference x gender); empty for NBL and WNBL. |
| `sc_total_score` | Int64 | SuperCoach score including double-double / triple-double bonuses. |
| `sc_total_score_without_dd_and_td` | Int64 | SuperCoach score without those bonuses. |
| `sc_breakdown_points_value` | Int64 | The stat's value for the game or season. |
| `sc_breakdown_points_points` | Int64 | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_rebounds_value` | Int64 | The stat's value for the game or season. |
| `sc_breakdown_rebounds_points` | Int64 | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_assists_value` | Int64 | The stat's value for the game or season. |
| `sc_breakdown_assists_points` | Int64 | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_blocks_value` | Int64 | The stat's value for the game or season. |
| `sc_breakdown_blocks_points` | Int64 | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_steals_value` | Int64 | The stat's value for the game or season. |
| `sc_breakdown_steals_points` | Int64 | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_turnovers_value` | Int64 | The stat's value for the game or season. |
| `sc_breakdown_turnovers_points` | Int64 | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_triple_double_value` | Int64 | The stat's value for the game or season. |
| `sc_breakdown_triple_double_points` | Int64 | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_double_figure_count` | Int64 | Stats in double figures (for the double-double / triple-double bonus) |
| `season_end_year` | Int64 | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |
| `sc_breakdown_double_double_value` | Int64 | The stat's value for the game or season. |
| `sc_breakdown_double_double_points` | Int64 | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `season_start_date` | String | First day of the season, ISO date-time without offset. |
| `player_full_name` | String | Given and family name. |
| `player_active` | Boolean | Player record is active. |
| `player_date_of_birth` | String | Date of birth, ISO date. |
| `player_nationality` | String | Unobserved in samples (null in every capture); type and meaning unverified. |
| `player_height` | Float64 | Height in centimetres. |
| `player_weight` | Float64 | Unobserved in samples (null in every capture); type and meaning unverified. |
| `team_gender` | String | Gender (MALE/FEMALE upper case on seasons and players, male/female lower case on competitions and divisions; UNKNOWN seen) |
| `synthetic_from_roster` | Boolean | Row built from the roster because the player has no season statistics (all stats 0) |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbl_rosetta_player_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Rosetta answered 404 or no rows (an unknown match, team, player or season id, an org or season type with nothing, or no season in progress). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The 403 gate body, a stub or error envelope, a page cut by Rosetta's path cache, or a body for another org, season or entity than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nbl_rosetta_player_stats-example}

```python
nbl_rosetta_player_stats(org='nbl', player_id='7619d3ea-410d-11f0-9235-2902fc2f936f')
```
