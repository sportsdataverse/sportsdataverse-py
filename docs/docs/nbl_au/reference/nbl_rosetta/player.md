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
| `org` | character | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `jersey_number` | character | Jersey number, a STRING. |
| `playing_position` | character | Playing position code (G, F, C, PG, SF, FC, ...); null when not recorded. |
| `player_id` | character | DataCore id (UUID v1, Sportradar) of the player; stable across seasons and the id to join on across routes. |
| `rosetta_player_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the player; routes that take this entity accept it as well as the DataCore id. |
| `player_date_updated` | character | Record last-edit time, ISO 8601 UTC. |
| `player_first_name` | character | The player's given name, e.g. Elijah. |
| `player_last_name` | character | The player's family name, e.g. Pepper. |
| `player_date_of_birth` | character | Date of birth, ISO date. |
| `player_height` | numeric | Height in centimetres. |
| `player_weight` | numeric | Weight in kilograms. |
| `player_dominant_hand` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `player_gender` | character | Gender (MALE/FEMALE upper case on seasons and players, male/female lower case on competitions and divisions; UNKNOWN seen) |
| `player_nationality` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `player_nationality_code` | character | Nationality, ISO 3166-1 alpha-3 code. |
| `player_dominant_foot` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `player_jersey_number` | character | Jersey number, a STRING. |
| `player_playing_position` | character | Playing position code (G, F, C, PG, SF, FC, ...); null when not recorded. |
| `player_active` | logical | Player record is active. |
| `player_league` | character | Rosetta league record id, UUID v4. |
| `player_birthplace` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `player_rookie_year` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `player_person` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `player_latest_jersey_number` | character | Player's most recent jersey number, a STRING. |
| `player_full_name` | character | Given and family name. |
| `player_source_updated_at` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `player_source_provider` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `team_id` | character | DataCore id (UUID v1, Sportradar) of the team; stable across seasons and the id to join on across routes. |
| `rosetta_team_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the team; routes that take this entity accept it as well as the DataCore id. |
| `team_code` | character | Team code, e.g. PER (the {team_code} path value) |
| `team_name` | character | Name of the team as Rosetta spells it, e.g. Perth Wildcats. |
| `season_competition_division` | character | NBL1 divisions (conference x gender); empty for NBL and WNBL. |
| `season_end_year` | integer | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

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
| `org` | character | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `period` | integer | Period as a STRING: 0 = game or season total, 1-4 = quarters, 11/12/13 = first/second/third overtime (inferred: fewer teams have each) |
| `playing_position` | character | Playing position code (G, F, C, PG, SF, FC, ...); null when not recorded. |
| `points` | integer | Points in the game. |
| `rebounds` | integer | Total rebounds in the game. |
| `assists` | integer | Assists in the game. |
| `blocks` | integer | Blocks in the game. |
| `steals` | integer | Steals in the game. |
| `turnovers` | integer | Turnovers in the game. |
| `offensive_rebounds` | integer | Offensive rebounds in the game. |
| `defensive_rebounds` | integer | Defensive rebounds in the game. |
| `personal_fouls` | integer | Personal fouls in the game. |
| `three_points_attempted` | integer | Three-point field goals attempted in the game. |
| `three_points_made` | integer | Three-point field goals made in the game. |
| `three_points_percentage` | numeric | Three-point field goals made / attempted, a 0-1 FRACTION. |
| `two_points_attempted` | integer | Two-point field goals attempted in the game. |
| `two_points_made` | integer | Two-point field goals made in the game. |
| `two_points_percentage` | numeric | Two-point field goals made / attempted, a 0-1 FRACTION. |
| `field_goals_attempted` | integer | Field goals attempted in the game. |
| `field_goals_made` | integer | Field goals made in the game. |
| `field_goals_percentage` | numeric | Field goals made / attempted, a 0-1 FRACTION. |
| `free_throws_attempted` | integer | Free throws attempted in the game. |
| `free_throws_made` | integer | Free throws made in the game. |
| `free_throws_percentage` | numeric | Free throws made / attempted, a 0-1 FRACTION. |
| `minutes` | numeric | Minutes played, decimal minutes. |
| `participated` | logical | Player took part in the game. |
| `match_id` | character | DataCore fixture id of the match (UUID v1): the {match_id} route key and fixtureId in play-by-play. Join on it. |
| `rosetta_match_id` | character | Rosetta's match RECORD id (UUID v4) as carried by box-score and play-by-play rows; no route accepts it. Join on match_id. |
| `match_start_time_datetime` | character | Tip-off, UTC; same value as start_time. |
| `match_status` | character | Status of the match in Rosetta's words (complete for a finished game): box score and play-by-play of any other status are partial. |
| `match_season_id` | character | DataCore id (UUID v1, Sportradar) of the match's season; stable across seasons and the id to join on across routes. |
| `rosetta_match_season_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the match's season; routes that take this entity accept it as well as the DataCore id. |
| `match_season_year` | integer | Season year as a STRING. NBL and WNBL from 1998-99: the START year ("2025" = 2025-26, sdv END = year + 1). NBL and WNBL 1979-1997 and every NBL1 season: the calendar year the season was played (END = year). Year 1998 holds two NBL and two WNBL seasons (calendar 1998 and 1998-99): tell them apart by start_date. |
| `match_season_type` | character | Season type: regular (includes finals), preseason, in_season (cups, blitz, exhibition), test. |
| `rosetta_match_home_team_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the match's home team; routes that take this entity accept it as well as the DataCore id. |
| `match_home_team_name` | character | Display name of the team. |
| `match_home_team_code` | character | Team code of the match's home team, e.g. PER or SYD. |
| `rosetta_match_away_team_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the match's away team; routes that take this entity accept it as well as the DataCore id. |
| `match_away_team_name` | character | Display name of the team. |
| `match_away_team_code` | character | Team code of the match's away team, e.g. PER or SYD. |
| `player_id` | character | DataCore id (UUID v1, Sportradar) of the player; stable across seasons and the id to join on across routes. |
| `rosetta_player_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the player; routes that take this entity accept it as well as the DataCore id. |
| `player_jersey_number` | character | Jersey number, a STRING. |
| `player_first_name` | character | The player's given name, e.g. Elijah. |
| `player_last_name` | character | The player's family name, e.g. Pepper. |
| `team_id` | character | DataCore id (UUID v1, Sportradar) of the team; stable across seasons and the id to join on across routes. |
| `rosetta_team_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the team; routes that take this entity accept it as well as the DataCore id. |
| `team_name` | character | Name of the team as Rosetta spells it, e.g. Perth Wildcats. |
| `team_code` | character | Team code, e.g. PER (the {team_code} path value) |
| `sc_total_score` | integer | SuperCoach score including double-double / triple-double bonuses. |
| `sc_total_score_without_dd_and_td` | integer | SuperCoach score without those bonuses. |
| `sc_breakdown_points_value` | integer | The stat's value for the game or season. |
| `sc_breakdown_points_points` | integer | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_rebounds_value` | integer | The stat's value for the game or season. |
| `sc_breakdown_rebounds_points` | integer | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_assists_value` | integer | The stat's value for the game or season. |
| `sc_breakdown_assists_points` | integer | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_blocks_value` | integer | The stat's value for the game or season. |
| `sc_breakdown_blocks_points` | integer | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_steals_value` | integer | The stat's value for the game or season. |
| `sc_breakdown_steals_points` | integer | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_turnovers_value` | integer | The stat's value for the game or season. |
| `sc_breakdown_turnovers_points` | integer | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_double_figure_count` | integer | Stats in double figures (for the double-double / triple-double bonus) |
| `sc_season_total` | integer | Player's SuperCoach total for the season so far. |
| `season_end_year` | integer | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |

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
| `org` | character | Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request. |
| `assists_average` | numeric | Assists per game. |
| `blocks_average` | numeric | Blocks per game. |
| `field_goals_attempted` | integer | Field goals attempted, season total. |
| `field_goals_made` | integer | Field goals made, season total. |
| `field_goals_attempted_average` | numeric | Field goals attempted per game. |
| `field_goals_made_average` | numeric | Field goals made per game. |
| `field_goals_percentage` | numeric | Field goals made / attempted, a 0-1 FRACTION. |
| `fouls_average` | numeric | Fouls committed (personal + technical + others) per game. |
| `free_throws_attempted` | integer | Free throws attempted, season total. |
| `free_throws_made` | integer | Free throws made, season total. |
| `free_throws_attempted_average` | numeric | Free throws attempted per game. |
| `free_throws_made_average` | numeric | Free throws made per game. |
| `free_throws_percentage` | numeric | Free throws made / attempted, a 0-1 FRACTION. |
| `games` | integer | Games played in the season (in the period rows, by the player or team). |
| `games_started` | integer | Games in the starting five. |
| `offensive_rebounds` | integer | Offensive rebounds, season total. |
| `fouls` | integer | Fouls committed (personal + technical + others), season total. |
| `personal_fouls` | integer | Personal fouls, season total. |
| `technical_fouls` | integer | Technical fouls, season total. |
| `period` | integer | Period as a STRING: 0 = game or season total, 1-4 = quarters, 11/12/13 = first/second/third overtime (inferred: fewer teams have each) |
| `three_points_attempted` | integer | Three-point field goals attempted, season total. |
| `three_points_attempted_average` | numeric | Three-point field goals attempted per game. |
| `three_points_made` | integer | Three-point field goals made, season total. |
| `three_points_made_average` | numeric | Three-point field goals made per game. |
| `three_points_percentage` | numeric | Three-point field goals made / attempted, a 0-1 FRACTION. |
| `turnovers_average` | numeric | Turnovers per game. |
| `turnovers` | integer | Turnovers, season total. |
| `two_points_attempted` | integer | Two-point field goals attempted, season total. |
| `two_points_attempted_average` | numeric | Two-point field goals attempted per game. |
| `two_points_made` | integer | Two-point field goals made, season total. |
| `two_points_made_average` | numeric | Two-point field goals made per game. |
| `two_points_percentage` | numeric | Two-point field goals made / attempted, a 0-1 FRACTION. |
| `minutes` | numeric | Minutes played, decimal minutes. |
| `minutes_average` | numeric | Minutes per game, decimal minutes. |
| `points` | integer | Points, season total. |
| `rebounds` | integer | Total rebounds, season total. |
| `assists` | integer | Assists, season total. |
| `blocks` | integer | Blocks, season total. |
| `steals` | integer | Steals, season total. |
| `plus_minus` | integer | Point differential while on court, season total. |
| `plus_minus_average` | numeric | Point differential while on court per game. |
| `efficiency` | integer | Efficiency rating (PTS + REB + AST + STL + BLK - missed FG - missed FT - TOV), season total. |
| `efficiency_average` | numeric | Efficiency rating (PTS + REB + AST + STL + BLK - missed FG - missed FT - TOV) per game. |
| `points_average` | numeric | Points per game. |
| `rebounds_average` | numeric | Total rebounds per game. |
| `steals_average` | numeric | Steals per game. |
| `player_id` | character | DataCore id (UUID v1, Sportradar) of the player; stable across seasons and the id to join on across routes. |
| `rosetta_player_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the player; routes that take this entity accept it as well as the DataCore id. |
| `player_first_name` | character | The player's given name, e.g. Elijah. |
| `player_jersey_number` | character | Jersey number, a STRING. |
| `player_last_name` | character | The player's family name, e.g. Pepper. |
| `player_playing_position` | character | Playing position code (G, F, C, PG, SF, FC, ...); null when not recorded. |
| `team_id` | character | DataCore id (UUID v1, Sportradar) of the team; stable across seasons and the id to join on across routes. |
| `rosetta_team_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the team; routes that take this entity accept it as well as the DataCore id. |
| `team_name` | character | Name of the team as Rosetta spells it, e.g. Perth Wildcats. |
| `team_code` | character | Team code, e.g. PER (the {team_code} path value) |
| `season_id` | character | DataCore id (UUID v1, Sportradar) of the season; stable across seasons and the id to join on across routes. |
| `rosetta_season_id` | character | Rosetta's own id (UUID v4; v5 on NBL1) of the season; routes that take this entity accept it as well as the DataCore id. |
| `season_year` | integer | Rosetta's year of the season: the START year for the NBL and WNBL from 1998-99 (2025 = 2025-26), the calendar year for NBL1 and the 1979-1997 winter seasons. See season_end_year for the sdv key. |
| `season_type` | character | Season type: regular (includes finals), preseason, in_season (cups, blitz, exhibition), test. |
| `season_competition_division` | character | NBL1 divisions (conference x gender); empty for NBL and WNBL. |
| `sc_total_score` | integer | SuperCoach score including double-double / triple-double bonuses. |
| `sc_total_score_without_dd_and_td` | integer | SuperCoach score without those bonuses. |
| `sc_breakdown_points_value` | integer | The stat's value for the game or season. |
| `sc_breakdown_points_points` | integer | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_rebounds_value` | integer | The stat's value for the game or season. |
| `sc_breakdown_rebounds_points` | integer | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_assists_value` | integer | The stat's value for the game or season. |
| `sc_breakdown_assists_points` | integer | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_blocks_value` | integer | The stat's value for the game or season. |
| `sc_breakdown_blocks_points` | integer | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_steals_value` | integer | The stat's value for the game or season. |
| `sc_breakdown_steals_points` | integer | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_turnovers_value` | integer | The stat's value for the game or season. |
| `sc_breakdown_turnovers_points` | integer | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_breakdown_triple_double_value` | integer | The stat's value for the game or season. |
| `sc_breakdown_triple_double_points` | integer | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `sc_double_figure_count` | integer | Stats in double figures (for the double-double / triple-double bonus) |
| `season_end_year` | integer | sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on the start date). Where the body carries no year it is the season asked for. |
| `sc_breakdown_double_double_value` | integer | The stat's value for the game or season. |
| `sc_breakdown_double_double_points` | integer | SuperCoach points this stat earns (e.g. 3 per steal or block, -2 per turnover) |
| `season_start_date` | character | First day of the season, ISO date-time without offset. |
| `player_full_name` | character | Given and family name. |
| `player_active` | logical | Player record is active. |
| `player_date_of_birth` | character | Date of birth, ISO date. |
| `player_nationality` | character | Unobserved in samples (null in every capture); type and meaning unverified. |
| `player_height` | numeric | Height in centimetres. |
| `player_weight` | numeric | Unobserved in samples (null in every capture); type and meaning unverified. |
| `team_gender` | character | Gender (MALE/FEMALE upper case on seasons and players, male/female lower case on competitions and divisions; UNKNOWN seen) |
| `synthetic_from_roster` | logical | Row built from the roster because the player has no season statistics (all stats 0) |

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
