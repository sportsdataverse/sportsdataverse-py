---
title: "SCB — SCB basketball API (BBL and DBBL, Germany) — Other"
sidebar_label: "Other"
sidebar_position: 3
description: "SCB — SCB basketball API (BBL and DBBL, Germany) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# SCB — SCB basketball API (BBL and DBBL, Germany) — Other

## scb_seasons

Seasons, newest first, with the active flag (BBL back to 1966, DBBL 2024 on).

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/seasons`

**Valid URL:** [https://api.basketball-bundesliga.de/seasons](https://api.basketball-bundesliga.de/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League: `bbl` (men, easyCredit Basketball Bundesliga, api.basketball-bundesliga.de) or `dbbl` (women, Toyota Damen-Basketball-Bundesliga, api-1.dbbl.scb.world). The two share one API; game ids overlap, so a game is (league, game_id). |

### Returns {#scb_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `season` | integer | Season END year (2026 = 2025-26), the sdv key; the API's `season_id` plus one. |
| `season_id` | integer | The API's own season key, the START year (2025 = 2025-26); `season` minus one. |
| `is_active` | logical | True for the season in progress. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#scb_seasons-example}

```python
scb_seasons()
```

_Last validated n/a._

## scb_season_players

Every player registered in a season: the id crosswalk (season_player_id, real_player_id, season_team_id).

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/seasons/{season}`

**Valid URL:** [https://api.basketball-bundesliga.de/seasons/2026](https://api.basketball-bundesliga.de/seasons/2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26, the sdv key). Sent to the API as its START-year seasonId (season - 1). |
| `league` | `league` |  |  | `Y` | League: `bbl` (men, easyCredit Basketball Bundesliga, api.basketball-bundesliga.de) or `dbbl` (women, Toyota Damen-Basketball-Bundesliga, api-1.dbbl.scb.world). The two share one API; game ids overlap, so a game is (league, game_id). |

### Returns {#scb_season_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `season` | integer | Season END year (2026 = 2025-26), the sdv key; the API's `season_id` plus one. |
| `season_id` | integer | The API's own season key, the START year (2025 = 2025-26); `season` minus one. |
| `season_player_id` | character | Season-player id: one player's engagement with one club (kept across seasons while he stays, new after a move; see README). Joins to box-score `seasonPlayer.id`, action `seasonPlayerId` and /season-players/{seasonPlayerId}. |
| `real_player_id` | integer | Global person id: the same across clubs and seasons. Joins to `realPlayerId` everywhere. |
| `season_team_id` | character | Season-team id; equal to the club `teamId` in every sample (BBL 2005-2026, DBBL 2024-2026). |
| `first_name` | character | Player's given name. |
| `last_name` | character | Player's family name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#scb_season_players-example}

```python
scb_season_players(season=2027)
```

_Last validated n/a._

## scb_season_teams

Every team of a season: club id, season-team id, name and three-letter code.

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/seasons/{season}`

**Valid URL:** [https://api.basketball-bundesliga.de/seasons/2026](https://api.basketball-bundesliga.de/seasons/2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26, the sdv key). Sent to the API as its START-year seasonId (season - 1). |
| `league` | `league` |  |  | `Y` | League: `bbl` (men, easyCredit Basketball Bundesliga, api.basketball-bundesliga.de) or `dbbl` (women, Toyota Damen-Basketball-Bundesliga, api-1.dbbl.scb.world). The two share one API; game ids overlap, so a game is (league, game_id). |

### Returns {#scb_season_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `season` | integer | Season END year (2026 = 2025-26), the sdv key; the API's `season_id` plus one. |
| `season_id` | integer | The API's own season key, the START year (2025 = 2025-26); `season` minus one. |
| `team_id` | character | Club id; equal to `seasonTeamId` in every sample. Joins to /teams/{teamId}. |
| `season_team_id` | character | Season-team id; equal to the club `teamId` in every sample (BBL 2005-2026, DBBL 2024-2026). |
| `team_name` | character | Team name as listed that season (sponsor names included). |
| `tlc` | character | Three-letter club code. |
| `logo_url` | character | Logo image URL (per season). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#scb_season_teams-example}

```python
scb_season_teams(season=2027)
```

_Last validated n/a._

## scb_playoff_series

Playoff bracket of a season: one row per series (quarter-finals, semi-finals, finals).

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/seasons/{season}/playoff-tree`

**Valid URL:** [https://api.basketball-bundesliga.de/seasons/2025/playoff-tree](https://api.basketball-bundesliga.de/seasons/2025/playoff-tree)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26, the sdv key). Sent to the API as its START-year seasonId (season - 1). |
| `league` | `league` |  |  | `Y` | League: `bbl` (men, easyCredit Basketball Bundesliga, api.basketball-bundesliga.de) or `dbbl` (women, Toyota Damen-Basketball-Bundesliga, api-1.dbbl.scb.world). The two share one API; game ids overlap, so a game is (league, game_id). |

### Returns {#scb_playoff_series-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `season` | integer | Season END year (2026 = 2025-26), the sdv key; the API's `season_id` plus one. |
| `season_id` | integer | Season, START year (2026 = 2026-27). |
| `round` | character | Round of the bracket: `quarter_finals`, `semi_finals` or `finals`. |
| `series` | integer | 1-based position of the series within its round, in the order the API lists them. |
| `match_winner_id` | character | Season-team id of the series winner, a string (the playoff tree sends an integer). |
| `is_match_finished` | logical | The series is decided. |
| `team_a_id` | character | Season-team id of series team A (the higher seed is A), a string (the playoff tree sends it as an integer); equals the club team id. |
| `team_a_name` | character | Name of series team A (the higher seed is A) as listed that season. |
| `team_a_tlc` | character | Three-letter club code of series team A (the higher seed is A). |
| `team_a_rank` | integer | Regular-season table position (the seed) of series team A (the higher seed is A). |
| `team_a_victories` | integer | Series wins of team A. |
| `team_b_id` | character | Season-team id of series team B (the higher seed is A), a string (the playoff tree sends it as an integer); equals the club team id. |
| `team_b_name` | character | Name of series team B (the higher seed is A) as listed that season. |
| `team_b_tlc` | character | Three-letter club code of series team B (the higher seed is A). |
| `team_b_rank` | integer | Regular-season table position (the seed) of series team B (the higher seed is A). |
| `team_b_victories` | integer | Series wins of team B. |
| `games` | integer | Games of the series. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#scb_playoff_series-example}

```python
scb_playoff_series(season=2026)
```

_Last validated n/a._

## scb_playoff_games

Every playoff game of a season with its series and the points of each series team.

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/seasons/{season}/playoff-tree`

**Valid URL:** [https://api.basketball-bundesliga.de/seasons/2025/playoff-tree](https://api.basketball-bundesliga.de/seasons/2025/playoff-tree)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26, the sdv key). Sent to the API as its START-year seasonId (season - 1). |
| `league` | `league` |  |  | `Y` | League: `bbl` (men, easyCredit Basketball Bundesliga, api.basketball-bundesliga.de) or `dbbl` (women, Toyota Damen-Basketball-Bundesliga, api-1.dbbl.scb.world). The two share one API; game ids overlap, so a game is (league, game_id). |

### Returns {#scb_playoff_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `season` | integer | Season END year (2026 = 2025-26), the sdv key; the API's `season_id` plus one. |
| `season_id` | integer | The API's own season key, the START year (2025 = 2025-26); `season` minus one. |
| `round` | character | Round of the bracket: `quarter_finals`, `semi_finals` or `finals`. |
| `series` | integer | 1-based position of the game's series within its round, in the order the API lists them. |
| `game_id` | character | Game id, a string (the playoff tree sends an integer); joins `scb_games.game_id`. |
| `scheduled_time` | character | Scheduled tip-off, ISO 8601 UTC. |
| `timestamp` | integer | Tip-off, Unix seconds. |
| `game_winner_id` | character | Season-team id of the game winner, a string (the playoff tree sends an integer). |
| `home_team_id` | character | Season-team id of the home team, a string (the playoff tree sends an integer). |
| `guest_team_id` | character | Season-team id of the guest (away) team, a string (the playoff tree sends an integer). |
| `team_a_id` | character | Season-team id of the series' team A (the higher seed); `points_team_a` is its score, whichever side hosted. |
| `team_b_id` | character | Season-team id of the series' team B; `points_team_b` is its score, whichever side hosted. |
| `points_team_a` | integer | Team A's points in the game. |
| `points_team_b` | integer | Team B's points in the game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#scb_playoff_games-example}

```python
scb_playoff_games(season=2026)
```

_Last validated n/a._

## scb_player_statistics

Season player stat table: totals and per-game values, one row per player and stat block.

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/seasons/{season}/player-statistics`

**Valid URL:** [https://api.basketball-bundesliga.de/seasons/2025/player-statistics?displayType=MAIN_ROUND](https://api.basketball-bundesliga.de/seasons/2025/player-statistics?displayType=MAIN_ROUND)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26, the sdv key). Sent to the API as its START-year seasonId (season - 1). |
| `league` | `league` |  |  | `Y` | League: `bbl` (men, easyCredit Basketball Bundesliga, api.basketball-bundesliga.de) or `dbbl` (women, Toyota Damen-Basketball-Bundesliga, api-1.dbbl.scb.world). The two share one API; game ids overlap, so a game is (league, game_id). |
| `displayType` | `display_type` |  |  | `Y` | Which games the table covers: MAIN_ROUND (regular season), PLAYOFFS, PLAYOFFS_TOTAL, MAIN_ROUND_AND_PLAY_OFFS or CUP. |

### Returns {#scb_player_statistics-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `season` | integer | Season END year (2026 = 2025-26), the sdv key; the API's `season_id` plus one. |
| `display_type` | character | Which games the table covers, echoed from the request (MAIN_ROUND, PLAYOFFS, PLAYOFFS_TOTAL, MAIN_ROUND_AND_PLAY_OFFS or CUP). |
| `season_player_id` | character | Season-player id: one player's engagement with one club (kept across seasons while he stays, new after a move; see README). Joins to box-score `seasonPlayer.id`, action `seasonPlayerId` and /season-players/{seasonPlayerId}. |
| `real_player_id` | integer | Global person id: the same across clubs and seasons. Joins to `realPlayerId` everywhere. |
| `first_name` | character | Player's given name. |
| `last_name` | character | Player's family name. |
| `shirt_number` | integer | Jersey number of the player, kept as text so a `00` jersey survives. |
| `position` | character | Position: POINT_GUARD, SHOOTING_GUARD, SMALL_FORWARD, POWER_FORWARD, CENTER. |
| `team_id` | character | Club id; equal to `seasonTeamId` in every sample. Joins to /teams/{teamId}. |
| `season_team_id` | character | Season-team id; equal to the club `teamId` in every sample (BBL 2005-2026, DBBL 2024-2026). |
| `team_name` | character | Team name as listed that season (sponsor names included, e.g. 'EWE Baskets Oldenburg'). |
| `team_tlc` | character | Three-letter club code. |
| `season_id` | integer | The API's own season key, the START year (2025 = 2025-26); `season` minus one. |
| `round_code` | integer | Provider round code; 1 for MAIN_ROUND in every sample (mapping unverified). |
| `competition` | character | Competition: BBL, BBL_CUP (Pokal), ALLSTAR, CHAMPIONS_CUP on the BBL host; DBBL, DBBL_CUP on the DBBL host. |
| `seconds_played` | integer | The player's season total playing time in SECONDS over displayType. |
| `points` | integer | The player's season total points over displayType. |
| `two_point_shots_made` | integer | The player's season total 2-point field goals made over displayType. |
| `two_point_shots_attempted` | integer | The player's season total 2-point field goals attempted over displayType. |
| `two_point_shot_success_percent` | numeric | The player's season total 2-point field-goal percentage over displaytype, 0-100 (one decimal); 0 when no attempts. |
| `three_point_shots_made` | integer | The player's season total 3-point field goals made over displayType. |
| `three_point_shots_attempted` | integer | The player's season total 3-point field goals attempted over displayType. |
| `three_point_shot_success_percent` | numeric | The player's season total 3-point field-goal percentage over displaytype, 0-100 (one decimal); 0 when no attempts. |
| `field_goals_made` | integer | The player's season total field goals made over displayType. |
| `field_goals_attempted` | integer | The player's season total field goals attempted over displayType. |
| `field_goals_success_percent` | numeric | The player's season total field-goal percentage over displaytype, 0-100 (one decimal); 0 when no attempts. |
| `free_throws_made` | integer | The player's season total free throws made over displayType. |
| `free_throws_attempted` | integer | The player's season total free throws attempted over displayType. |
| `free_throws_success_percent` | numeric | The player's season total free-throw percentage over displaytype, 0-100 (one decimal); 0 when no attempts. |
| `offensive_rebounds` | integer | The player's season total offensive rebounds over displayType. |
| `defensive_rebounds` | integer | The player's season total defensive rebounds over displayType. |
| `total_rebounds` | integer | The player's season total rebounds (offensive + defensive) over displayType. |
| `assists` | integer | The player's season total assists over displayType. |
| `steals` | integer | The player's season total steals over displayType. |
| `turnovers` | integer | The player's season total turnovers over displayType. |
| `blocks` | integer | The player's season total blocked shots over displayType. |
| `fouls_committed` | integer | The player's season total fouls committed over displayType. |
| `efficiency` | integer | The player's season total efficiency (EFF = PTS + REB + AST + STL + BLK - missed FG - missed FT - TO; checked on a 2026 box score) over displayType. |
| `plus_minus` | integer | The player's season total plus-minus over displaytype. |
| `games_played` | integer | The player's season total games played over displayType. |
| `seconds_per_game` | numeric | Per-game average of seconds, seconds. |
| `points_per_game` | numeric | Per-game average of points. |
| `two_point_shots_made_per_game` | numeric | Per-game average of 2-point field goals made. |
| `two_point_shots_attempted_per_game` | numeric | Per-game average of 2-point field goals attempted. |
| `three_point_shots_made_per_game` | numeric | Per-game average of 3-point field goals made. |
| `three_point_shots_attempted_per_game` | numeric | Per-game average of 3-point field goals attempted. |
| `field_goals_made_per_game` | numeric | Per-game average of field goals made. |
| `field_goals_attempted_per_game` | numeric | Per-game average of field goals attempted. |
| `field_goals_success_percent_per_game` | numeric | Per-game average of field-goal percentage (0 in every sample: unusable). |
| `free_throws_made_per_game` | numeric | Per-game average of free throws made. |
| `free_throws_attempted_per_game` | numeric | Per-game average of free throws attempted. |
| `defensive_rebounds_per_game` | numeric | Per-game average of defensive rebounds. |
| `offensive_rebounds_per_game` | numeric | Per-game average of offensive rebounds. |
| `total_rebounds_per_game` | numeric | Per-game average of rebounds (offensive + defensive). |
| `assists_per_game` | numeric | Per-game average of assists. |
| `steals_per_game` | numeric | Per-game average of steals. |
| `turnovers_per_game` | numeric | Per-game average of turnovers. |
| `blocks_per_game` | numeric | Per-game average of blocked shots. |
| `fouls_committed_per_game` | numeric | Per-game average of fouls committed. |
| `efficiency_per_game` | numeric | Per-game average of efficiency (EFF = PTS + REB + AST + STL + BLK - missed FG - missed FT - TO; checked on a 2026 box score). |
| `plus_minus_per_game` | numeric | Per-game average of plus-minus (point margin while on court). |
| `double_double` | integer | The player's season total double-doubles over displayType. |
| `double_double_per_game` | numeric | Per-game average of double-doubles. |
| `games_started` | integer | The player's season total games started over displayType. |
| `triple_double` | integer | The player's season total triple-doubles over displayType. |
| `triple_double_per_game` | numeric | Per-game average of triple-doubles. |
| `fouls_received` | integer | The player's season total fouls drawn over displayType. |
| `fouls_received_per_game` | numeric | Per-game average of fouls drawn. |
| `overall` | logical | False in every sample (meaning unverified). |
| `stage` | character | Stage: MAIN_ROUND (Hauptrunde, regular season), PLAYINS, FIRST_ROUND, SECOND_ROUND, ROUND_OF_16 (Achtelfinale), ROUND_OF_8 (Viertelfinale), SEMI_FINALS, FINALS, THIRD_PLACE_GAME, QUALIFICATION, GROUP_STAGE. |
| `aggregation` | character | Aggregation label as sent: STAGE in player tables, MAIN_ROUND in team tables. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#scb_player_statistics-example}

```python
scb_player_statistics(display_type='MAIN_ROUND', season=2026)
```

_Last validated n/a._

## scb_games

Games of a season (the whole season in one page): teams, score by period, officials, venue and top scorer.

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/games`

**Valid URL:** [https://api.basketball-bundesliga.de/games?currentPage=1&seasonId=2025&pageSize=500](https://api.basketball-bundesliga.de/games?currentPage=1&seasonId=2025&pageSize=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League: `bbl` (men, easyCredit Basketball Bundesliga, api.basketball-bundesliga.de) or `dbbl` (women, Toyota Damen-Basketball-Bundesliga, api-1.dbbl.scb.world). The two share one API; game ids overlap, so a game is (league, game_id). |
| `seasonId` | `season` |  |  | `Y` | Season END year (2026 = 2025-26, the sdv key). Sent to the API as its START-year seasonId (season - 1). Omitted = the season in progress. |
| `competition` | `competition` |  |  | `Y` | Competition filter: BBL, BBL_CUP (Pokal), ALLSTAR, CHAMPIONS_CUP on the BBL host; DBBL, DBBL_CUP on the DBBL host. Omitted = all. The other host's competition answers an empty page. |
| `gameType` | `game_type` |  |  | `Y` | Finished, scheduled or all games. Omitted = all. |
| `seasonTeamId` | `team_id` |  |  | `Y` | Only the games of this team, home or guest (the club id equals the season-team id). |
| `pageSize` | `page_size` |  |  | `Y` | Rows per page. The wrapper reads page 1 and refuses a result that needs more pages, so keep it large enough for the whole result (500 covers a season). |

### Returns {#scb_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `season` | integer | Season END year (2026 = 2025-26), the sdv key; the API's `season_id` plus one. |
| `season_id` | integer | Season, START year (2026 = 2026-27). |
| `game_id` | character | Game id, a string; unique within a league only (BBL and DBBL overlap), and not chronological. |
| `competition` | character | Competition: BBL, BBL_CUP (Pokal), ALLSTAR, CHAMPIONS_CUP on the BBL host; DBBL, DBBL_CUP on the DBBL host. |
| `stage` | character | Stage: MAIN_ROUND (Hauptrunde, regular season), PLAYINS, FIRST_ROUND, SECOND_ROUND, ROUND_OF_16 (Achtelfinale), ROUND_OF_8 (Viertelfinale), SEMI_FINALS, FINALS, THIRD_PLACE_GAME, QUALIFICATION, GROUP_STAGE. |
| `status` | character | Game status: PRE (scheduled), LIVE, POST (ended, not yet official), OFFICIAL (final). |
| `scheduled_time` | character | Scheduled tip-off, ISO 8601 UTC. |
| `match_day` | integer | Round number within the stage (Spieltag). |
| `is_tbd` | logical | Date or time not yet fixed. |
| `is_hidden` | logical | True when the API hides the game (0/1 on /games, a boolean on the header). |
| `home_team_id` | character | Club id; equal to `seasonTeamId` in every sample. Joins to /teams/{teamId}. |
| `home_season_team_id` | character | Season-team id; equal to the club `teamId` in every sample (BBL 2005-2026, DBBL 2024-2026). |
| `home_team_name` | character | Team name as listed that season (sponsor names included, e.g. 'EWE Baskets Oldenburg'). |
| `home_tlc` | character | Three-letter club code. |
| `home_is_foreign` | logical | The team is not in this league (cup opponents from the ProA / ProB). |
| `home_rank` | integer | The team's current table position. |
| `home_streak` | character | Current streak: W or L; empty when none. |
| `home_streak_count` | integer | Length of the current streak. |
| `home_last10_victories` | integer | Wins in the last 10 games. |
| `home_last10_losses` | integer | Losses in the last 10 games. |
| `home_total_games` | integer | Games the home team has played so far (the API sends it as a string). |
| `guest_team_id` | character | Club id; equal to `seasonTeamId` in every sample. Joins to /teams/{teamId}. |
| `guest_season_team_id` | character | Season-team id; equal to the club `teamId` in every sample (BBL 2005-2026, DBBL 2024-2026). |
| `guest_team_name` | character | Team name as listed that season (sponsor names included, e.g. 'EWE Baskets Oldenburg'). |
| `guest_tlc` | character | Three-letter club code. |
| `guest_is_foreign` | logical | The team is not in this league (cup opponents from the ProA / ProB). |
| `guest_rank` | integer | The team's current table position. |
| `guest_streak` | character | Current streak: W or L; empty when none. |
| `guest_streak_count` | integer | Length of the current streak. |
| `guest_last10_victories` | integer | Wins in the last 10 games. |
| `guest_last10_losses` | integer | Losses in the last 10 games. |
| `guest_total_games` | integer | Games the guest team has played so far (the API sends it as a string). |
| `home_score` | integer | Home team final score. |
| `guest_score` | integer | Guest team final score. |
| `latest_period` | integer | Number of the last period played (4 = Q4, 5 = OT1, ...); the API sends `Q4` on /games and 4 on /games/recent. Null before the game. |
| `spectators` | integer | Attendance; 0 when not recorded. |
| `home_q1_score` | integer | Home team points in quarter 1; 0 when quarter scores were not recorded. |
| `home_q2_score` | integer | Home team points in quarter 2; 0 when quarter scores were not recorded. |
| `home_q3_score` | integer | Home team points in quarter 3; 0 when quarter scores were not recorded. |
| `home_q4_score` | integer | Home team points in quarter 4; 0 when quarter scores were not recorded. |
| `home_ot1_score` | integer | Home team points in overtime 1; 0 when not played (null from OT5 on in old seasons). |
| `home_ot2_score` | integer | Home team points in overtime 2; 0 when not played (null from OT5 on in old seasons). |
| `home_ot3_score` | integer | Home team points in overtime 3; 0 when not played (null from OT5 on in old seasons). |
| `home_ot4_score` | integer | Home team points in overtime 4; 0 when not played (null from OT5 on in old seasons). |
| `home_ot5_score` | integer | Home team points in overtime 5; 0 when not played (null from OT5 on in old seasons). |
| `home_ot6_score` | integer | Home team points in overtime 6; 0 when not played (null from OT5 on in old seasons). |
| `home_ot7_score` | integer | Home team points in overtime 7; 0 when not played (null from OT5 on in old seasons). |
| `home_ot8_score` | integer | Home team points in overtime 8; 0 when not played (null from OT5 on in old seasons). |
| `home_ot9_score` | integer | Home team points in overtime 9; 0 when not played (null from OT5 on in old seasons). |
| `guest_q1_score` | integer | Guest team points in quarter 1; 0 when quarter scores were not recorded. |
| `guest_q2_score` | integer | Guest team points in quarter 2; 0 when quarter scores were not recorded. |
| `guest_q3_score` | integer | Guest team points in quarter 3; 0 when quarter scores were not recorded. |
| `guest_q4_score` | integer | Guest team points in quarter 4; 0 when quarter scores were not recorded. |
| `guest_ot1_score` | integer | Guest team points in overtime 1; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot2_score` | integer | Guest team points in overtime 2; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot3_score` | integer | Guest team points in overtime 3; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot4_score` | integer | Guest team points in overtime 4; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot5_score` | integer | Guest team points in overtime 5; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot6_score` | integer | Guest team points in overtime 6; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot7_score` | integer | Guest team points in overtime 7; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot8_score` | integer | Guest team points in overtime 8; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot9_score` | integer | Guest team points in overtime 9; 0 when not played (null from OT5 on in old seasons). |
| `referee1_id` | character | Official id; joins to /referees `refId` (= its `id`). /referees lists only the current officials (44), so older games' officials can be missing there: all 40 of 2026-27's matched, 6 of 10 of 2008-09's. |
| `referee1_first_name` | character | Official's given name. |
| `referee1_last_name` | character | Official's family name. |
| `referee2_id` | character | Official id; joins to /referees `refId` (= its `id`). /referees lists only the current officials (44), so older games' officials can be missing there: all 40 of 2026-27's matched, 6 of 10 of 2008-09's. |
| `referee2_first_name` | character | Official's given name. |
| `referee2_last_name` | character | Official's family name. |
| `referee3_id` | character | Official id; joins to /referees `refId` (= its `id`). /referees lists only the current officials (44), so older games' officials can be missing there: all 40 of 2026-27's matched, 6 of 10 of 2008-09's. |
| `referee3_first_name` | character | Official's given name. |
| `referee3_last_name` | character | Official's family name. |
| `commissioner_id` | character | Official id; joins to /referees `refId` (= its `id`). /referees lists only the current officials (44), so older games' officials can be missing there: all 40 of 2026-27's matched, 6 of 10 of 2008-09's. |
| `commissioner_first_name` | character | Official's given name. |
| `commissioner_last_name` | character | Official's family name. |
| `venue_id` | character | Arena id, stable across seasons (Uber Arena = 2 in both 2025-26 and 2026-27); a different key space from /teams `seasonVenues.id`, and no captured route is keyed by it. A string in /games and /games/{gameId}, an integer in /games/recent. |
| `venue_name` | character | Name of the arena the game is played in; `venue_id` is its stable arena id. |
| `venue_address` | character | Postal address, German. |
| `venue_capacity` | integer | Arena capacity, spectators. |
| `venue_is_main` | logical | The club's main arena. |
| `tv_channel` | character | TV channel(s) showing the game, comma-joined (a list on some routes, an empty string when none). |
| `tv_type` | character | TV coverage type; empty or null in every sample. |
| `is_free_tv` | logical | Shown on free-to-air TV. |
| `live_stream_url` | character | Live-stream link (DBBL games link YouTube); empty when none. |
| `top_score` | integer | Points of the game's top scorer; null before the game. |
| `top_scorer_season_player_id` | character | Season-player id. BBL: a string equal to `id`. DBBL: an INTEGER that matches /seasons/{seasonId} `seasonPlayerId` (5 of 5 rows checked, 2026-27). Joins to /season-players/{seasonPlayerId}. |
| `top_scorer_real_player_id` | character | Global person id: the same across clubs and seasons. Joins to `realPlayerId` everywhere. |
| `top_scorer_first_name` | character | Player's given name. |
| `top_scorer_last_name` | character | Player's family name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#scb_games-example}

```python
scb_games(season=2026)
```

_Last validated n/a._

## scb_game

One game header: teams, score by period, officials, venue and top scorer.

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/games/{game_id}`

**Valid URL:** [https://api.basketball-bundesliga.de/games/2006920](https://api.basketball-bundesliga.de/games/2006920)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Game id (`game_id` in `scb_games`), e.g. 2006920 (BBL) or 10130 (DBBL). One sequence per league. |
| `league` | `league` |  |  | `Y` | League: `bbl` (men, easyCredit Basketball Bundesliga, api.basketball-bundesliga.de) or `dbbl` (women, Toyota Damen-Basketball-Bundesliga, api-1.dbbl.scb.world). The two share one API; game ids overlap, so a game is (league, game_id). |

### Returns {#scb_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `season` | integer | Season END year (2026 = 2025-26), the sdv key; the API's `season_id` plus one. |
| `season_id` | integer | Season, START year (2026 = 2026-27). |
| `game_id` | character | Game id, a string; unique within a league only (BBL and DBBL overlap), and not chronological. |
| `competition` | character | Competition: BBL, BBL_CUP (Pokal), ALLSTAR, CHAMPIONS_CUP on the BBL host; DBBL, DBBL_CUP on the DBBL host. |
| `stage` | character | Stage: MAIN_ROUND (Hauptrunde, regular season), PLAYINS, FIRST_ROUND, SECOND_ROUND, ROUND_OF_16 (Achtelfinale), ROUND_OF_8 (Viertelfinale), SEMI_FINALS, FINALS, THIRD_PLACE_GAME, QUALIFICATION, GROUP_STAGE. |
| `status` | character | Game status: PRE (scheduled), LIVE, POST (ended, not yet official), OFFICIAL (final). |
| `stat_status` | character | Live-stats feed status code. Observed: E (ended; German Ende), N (not started), 1 (old games), empty string. Meaning inferred. |
| `game_progress` | character | Live progress code: E (ended), empty or null before the game. Meaning inferred. |
| `scheduled_time` | character | Scheduled tip-off, ISO 8601 UTC. |
| `match_day` | integer | Round number within the stage (Spieltag). |
| `is_tbd` | logical | Date or time not yet fixed. |
| `is_hidden` | logical | True when the API hides the game (0/1 on /games, a boolean on the header). |
| `home_team_id` | character | Club id; equal to `seasonTeamId` in every sample. Joins to /teams/{teamId}. |
| `home_season_team_id` | character | Season-team id; equal to the club `teamId` in every sample (BBL 2005-2026, DBBL 2024-2026). |
| `home_team_name` | character | Team name as listed that season (sponsor names included, e.g. 'EWE Baskets Oldenburg'). |
| `home_tlc` | character | Three-letter club code. |
| `home_is_foreign` | logical | The team is not in this league (cup opponents from the ProA / ProB). |
| `home_rank` | integer | The team's current table position. |
| `home_streak` | character | Current streak: W or L; empty when none. |
| `home_streak_count` | integer | Length of the current streak. |
| `home_last10_victories` | integer | Wins in the last 10 games. |
| `home_last10_losses` | integer | Losses in the last 10 games. |
| `home_total_games` | integer | Games the home team has played so far (the API sends it as a string). |
| `guest_team_id` | character | Club id; equal to `seasonTeamId` in every sample. Joins to /teams/{teamId}. |
| `guest_season_team_id` | character | Season-team id; equal to the club `teamId` in every sample (BBL 2005-2026, DBBL 2024-2026). |
| `guest_team_name` | character | Team name as listed that season (sponsor names included, e.g. 'EWE Baskets Oldenburg'). |
| `guest_tlc` | character | Three-letter club code. |
| `guest_is_foreign` | logical | The team is not in this league (cup opponents from the ProA / ProB). |
| `guest_rank` | integer | The team's current table position. |
| `guest_streak` | character | Current streak: W or L; empty when none. |
| `guest_streak_count` | integer | Length of the current streak. |
| `guest_last10_victories` | integer | Wins in the last 10 games. |
| `guest_last10_losses` | integer | Losses in the last 10 games. |
| `guest_total_games` | integer | Games the guest team has played so far (the API sends it as a string). |
| `home_score` | integer | Home team final score. |
| `guest_score` | integer | Guest team final score. |
| `latest_period` | integer | Number of the last period played (4 = Q4, 5 = OT1, ...); the API sends `Q4` on /games and 4 on /games/recent. Null before the game. |
| `spectators` | integer | Attendance; 0 when not recorded. |
| `home_q1_score` | integer | Home team points in quarter 1; 0 when quarter scores were not recorded. |
| `home_q2_score` | integer | Home team points in quarter 2; 0 when quarter scores were not recorded. |
| `home_q3_score` | integer | Home team points in quarter 3; 0 when quarter scores were not recorded. |
| `home_q4_score` | integer | Home team points in quarter 4; 0 when quarter scores were not recorded. |
| `home_ot1_score` | integer | Home team points in overtime 1; 0 when not played (null from OT5 on in old seasons). |
| `home_ot2_score` | integer | Home team points in overtime 2; 0 when not played (null from OT5 on in old seasons). |
| `home_ot3_score` | integer | Home team points in overtime 3; 0 when not played (null from OT5 on in old seasons). |
| `home_ot4_score` | integer | Home team points in overtime 4; 0 when not played (null from OT5 on in old seasons). |
| `home_ot5_score` | integer | Home team points in overtime 5; 0 when not played (null from OT5 on in old seasons). |
| `home_ot6_score` | integer | Home team points in overtime 6; 0 when not played (null from OT5 on in old seasons). |
| `home_ot7_score` | integer | Home team points in overtime 7; 0 when not played (null from OT5 on in old seasons). |
| `home_ot8_score` | integer | Home team points in overtime 8; 0 when not played (null from OT5 on in old seasons). |
| `home_ot9_score` | integer | Home team points in overtime 9; 0 when not played (null from OT5 on in old seasons). |
| `guest_q1_score` | integer | Guest team points in quarter 1; 0 when quarter scores were not recorded. |
| `guest_q2_score` | integer | Guest team points in quarter 2; 0 when quarter scores were not recorded. |
| `guest_q3_score` | integer | Guest team points in quarter 3; 0 when quarter scores were not recorded. |
| `guest_q4_score` | integer | Guest team points in quarter 4; 0 when quarter scores were not recorded. |
| `guest_ot1_score` | integer | Guest team points in overtime 1; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot2_score` | integer | Guest team points in overtime 2; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot3_score` | integer | Guest team points in overtime 3; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot4_score` | integer | Guest team points in overtime 4; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot5_score` | integer | Guest team points in overtime 5; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot6_score` | integer | Guest team points in overtime 6; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot7_score` | integer | Guest team points in overtime 7; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot8_score` | integer | Guest team points in overtime 8; 0 when not played (null from OT5 on in old seasons). |
| `guest_ot9_score` | integer | Guest team points in overtime 9; 0 when not played (null from OT5 on in old seasons). |
| `referee1_id` | character | Official id; joins to /referees `refId` (= its `id`). /referees lists only the current officials (44), so older games' officials can be missing there: all 40 of 2026-27's matched, 6 of 10 of 2008-09's. |
| `referee1_first_name` | character | Official's given name. |
| `referee1_last_name` | character | Official's family name. |
| `referee2_id` | character | Official id; joins to /referees `refId` (= its `id`). /referees lists only the current officials (44), so older games' officials can be missing there: all 40 of 2026-27's matched, 6 of 10 of 2008-09's. |
| `referee2_first_name` | character | Official's given name. |
| `referee2_last_name` | character | Official's family name. |
| `referee3_id` | character | Official id; joins to /referees `refId` (= its `id`). /referees lists only the current officials (44), so older games' officials can be missing there: all 40 of 2026-27's matched, 6 of 10 of 2008-09's. |
| `referee3_first_name` | character | Official's given name. |
| `referee3_last_name` | character | Official's family name. |
| `commissioner_id` | character | Official id; joins to /referees `refId` (= its `id`). /referees lists only the current officials (44), so older games' officials can be missing there: all 40 of 2026-27's matched, 6 of 10 of 2008-09's. |
| `commissioner_first_name` | character | Official's given name. |
| `commissioner_last_name` | character | Official's family name. |
| `venue_id` | character | Arena id, stable across seasons (Uber Arena = 2 in both 2025-26 and 2026-27); a different key space from /teams `seasonVenues.id`, and no captured route is keyed by it. A string in /games and /games/{gameId}, an integer in /games/recent. |
| `venue_name` | character | Name of the arena the game is played in; `venue_id` is its stable arena id. |
| `venue_address` | character | Postal address, German. |
| `venue_capacity` | integer | Arena capacity, spectators. |
| `venue_is_main` | logical | The club's main arena. |
| `tv_channel` | character | TV channel(s) showing the game, comma-joined (a list on some routes, an empty string when none). |
| `tv_type` | character | TV coverage type; empty or null in every sample. |
| `is_free_tv` | logical | Shown on free-to-air TV. |
| `live_stream_url` | character | Live-stream link (DBBL games link YouTube); empty when none. |
| `top_score` | integer | Points of the game's top scorer; null before the game. |
| `top_scorer_season_player_id` | character | Season-player id. BBL: a string equal to `id`. DBBL: an INTEGER that matches /seasons/{seasonId} `seasonPlayerId` (5 of 5 rows checked, 2026-27). Joins to /season-players/{seasonPlayerId}. |
| `top_scorer_real_player_id` | character | Global person id: the same across clubs and seasons. Joins to `realPlayerId` everywhere. |
| `top_scorer_first_name` | character | Player's given name. |
| `top_scorer_last_name` | character | Player's family name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#scb_game-example}

```python
scb_game(game_id='2006920')
```

_Last validated n/a._

## scb_standings

League table of a season: record, home / away split, streaks and tie-break detail (2005-06 on).

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/standings`

**Valid URL:** [https://api.basketball-bundesliga.de/standings?seasonId=2025](https://api.basketball-bundesliga.de/standings?seasonId=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League: `bbl` (men, easyCredit Basketball Bundesliga, api.basketball-bundesliga.de) or `dbbl` (women, Toyota Damen-Basketball-Bundesliga, api-1.dbbl.scb.world). The two share one API; game ids overlap, so a game is (league, game_id). |
| `seasonId` | `season` |  |  | `Y` | Season END year (2026 = 2025-26, the sdv key). Sent to the API as its START-year seasonId (season - 1). Omitted = the season in progress. Seasons before about 2000 answer an empty table. |
| `competition` | `competition` |  |  | `Y` | Competition: BBL, BBL_CUP, ALLSTAR, CHAMPIONS_CUP (BBL host) or DBBL, DBBL_CUP (DBBL host). BBL_CUP answers an empty table. |

### Returns {#scb_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `season` | integer | Season END year (2026 = 2025-26), the sdv key; the API's `season_id` plus one. |
| `standing_id` | character | Opaque id of the standings row, a string. |
| `season_team_id` | character | Season-team id; equal to the club `teamId` in every sample (BBL 2005-2026, DBBL 2024-2026). An integer here. |
| `team_id` | character | Club id; equal to `seasonTeamId` in every sample. Joins to /teams/{teamId}. |
| `team_name` | character | Team name; null on some rows. |
| `team_tlc` | character | Three-letter club code. |
| `team_name_short` | character | Short team name (null on DBBL). |
| `has_reached_playoffs` | logical | Clinched a playoff place. |
| `has_reached_playins` | logical | Clinched at least the play-ins. |
| `is_relegation_certain` | logical | Relegated for certain. |
| `season_id` | integer | Season, START year (2026 = 2026-27). |
| `competition` | character | Competition: BBL, BBL_CUP (Pokal), ALLSTAR, CHAMPIONS_CUP on the BBL host; DBBL, DBBL_CUP on the DBBL host. |
| `stage` | character | Stage: MAIN_ROUND (Hauptrunde, regular season), PLAYINS, FIRST_ROUND, SECOND_ROUND, ROUND_OF_16 (Achtelfinale), ROUND_OF_8 (Viertelfinale), SEMI_FINALS, FINALS, THIRD_PLACE_GAME, QUALIFICATION, GROUP_STAGE. |
| `rank` | integer | Table position. |
| `streak` | character | Current streak: W or L; empty when none. |
| `streak_count` | integer | Length of the current streak. |
| `point_corrections` | character | Table-point deductions; [] in every sample. |
| `home_victories` | integer | Home games: wins. |
| `home_losses` | integer | Home games: number of losses in the table. |
| `home_victory_percentage` | numeric | Home games: win percentage, 0-100. |
| `away_victories` | integer | Away games: wins. |
| `away_losses` | integer | Away games: number of losses in the table. |
| `away_victory_percentage` | numeric | Away games: win percentage, 0-100. |
| `table_points` | integer | Table points scored (2 per win), from the API's `totalPointsMade`; the provider's notes call its 'Points' fields table points. |
| `table_points_against` | integer | Table points conceded (2 per loss), from the API's `totalPointsAgainst`; per the provider's notes, table points. |
| `total_victory_percentage` | numeric | All games: win percentage, 0-100. |
| `total_games` | integer | Games played in the table (home and away together). |
| `total_victories` | integer | All games: wins. |
| `total_losses` | integer | Losses in the table over all games. |
| `last10_victories` | integer | Wins in the last 10 games. |
| `last10_losses` | integer | Losses in the last 10 games. |
| `last10_percentage` | numeric | Win percentage over the last 10 games, 0-100. |
| `last10_outcomes` | character | Results of the last games, newest first: W or L. |
| `home_last10_victories` | integer | Home games: last 10 games, wins. |
| `home_last10_losses` | integer | Home games: last 10 games, losses. |
| `home_last10_victory_percentage` | numeric | Home games: last 10 games, win percentage, 0-100. |
| `home_streak` | character | Home games: current streak, W or L. |
| `home_streak_count` | integer | Home games: length of the current streak. |
| `away_last10_victories` | integer | Away games: last 10 games, wins. |
| `away_last10_losses` | integer | Away games: last 10 games, losses. |
| `away_last10_victory_percentage` | numeric | Away games: last 10 games, win percentage, 0-100. |
| `away_streak` | character | Away games: current streak, W or L. |
| `away_streak_count` | integer | Away games: length of the current streak. |
| `updated_date` | character | Last update of the row, ISO 8601 UTC. |
| `ranking_comparison_info` | character | Tie-break detail among teams level on record. |
| `home_points_for` | integer | Home games: basketball points scored (the API's football-style `GoalsMade`). |
| `away_points_for` | integer | Away games: basketball points scored (the API's football-style `GoalsMade`). |
| `total_points_for` | integer | All games: basketball points scored (the API's football-style `GoalsMade`). |
| `home_points_against` | integer | Home games: basketball points allowed (the API's football-style `GoalsAgainst`). |
| `away_points_against` | integer | Away games: basketball points allowed (the API's football-style `GoalsAgainst`). |
| `total_points_against` | integer | All games: basketball points allowed (the API's football-style `GoalsAgainst`). |
| `home_point_difference` | integer | Home games: point differential, scored minus allowed (the API's `GoalDifference`). |
| `away_point_difference` | integer | Away games: point differential, scored minus allowed (the API's `GoalDifference`). |
| `total_point_difference` | integer | All games: point differential, scored minus allowed (the API's `GoalDifference`). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#scb_standings-example}

```python
scb_standings(season=2026)
```

_Last validated n/a._

## scb_season_player

Player profile by season-player id: bio, position, club and previous clubs.

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/season-players/{season_player_id}`

**Valid URL:** [https://api.basketball-bundesliga.de/season-players/56414](https://api.basketball-bundesliga.de/season-players/56414)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_player_id` | `season_player_id` |  | `Y` |  | Season-player id (`season_player_id` in a box score, an action or `scb_season_players`): one player's engagement with one club. |
| `league` | `league` |  |  | `Y` | League: `bbl` (men, easyCredit Basketball Bundesliga, api.basketball-bundesliga.de) or `dbbl` (women, Toyota Damen-Basketball-Bundesliga, api-1.dbbl.scb.world). The two share one API; game ids overlap, so a game is (league, game_id). |

### Returns {#scb_season_player-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `season_player_id` | character | Season-player id: one player's engagement with one club (kept across seasons while he stays, new after a move). Joins box-score lines, actions, records and `scb_season_player`. |
| `real_player_id` | integer | Global person id: the same across clubs and seasons. Joins to `realPlayerId` everywhere. |
| `active_season_player_id` | character | The player's current season-player id (his latest club engagement). Joins to /season-players/{seasonPlayerId}. |
| `first_name` | character | Player's given name. |
| `last_name` | character | Player's family name. |
| `birth_date` | character | Date of birth, YYYY-MM-DD. |
| `position` | character | Position: POINT_GUARD, SHOOTING_GUARD, SMALL_FORWARD, POWER_FORWARD, CENTER. |
| `shirt_number` | character | Jersey number. A number or a numeric string across rows. |
| `height` | numeric | Height in metres (float32 noise: 1.77 arrives as 1.7699999809265137). |
| `weight` | integer | Weight in kg; 0 when unknown. |
| `nationalities` | character | Nationalities, ISO 3166-1 alpha-2 codes. |
| `is_all_star` | logical | All-Star selection flag. |
| `is_young` | logical | Young-player flag (German-trained youth rule; inferred). |
| `has_changed_teams` | logical | Moved clubs during the season. |
| `bbl_games_played` | integer | Career league games played (BBL host; 0 on DBBL). |
| `team_id` | character | Club id, a string; equal to `season_team_id` in every sample. Joins to `scb_team` and `scb_season_teams`. |
| `team_name` | character | Team name as listed that season (sponsor names included). |
| `previous_teams` | character | Former clubs, free text (German names). |
| `season_ids` | character | Seasons (start years) with this player or club. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#scb_season_player-example}

```python
scb_season_player(season_player_id='56414')
```

_Last validated n/a._

## scb_referees

Current referees and commissioners (BBL only).

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/referees`

**Valid URL:** [https://api.basketball-bundesliga.de/referees?currentPage=1&pageSize=100](https://api.basketball-bundesliga.de/referees?currentPage=1&pageSize=100)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `pageSize` | `page_size` |  |  | `Y` | Rows per page. 100 returns all current officials in one page; a result that needs more pages is refused. |

### Returns {#scb_referees-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `referee_id` | character | Official id (`refId`, equal to the API's `id`); joins `referee1_id` .. `commissioner_id` of `scb_games`. |
| `first_name` | character | Official's given name. |
| `last_name` | character | Official's family name. |
| `official_function` | character | REFEREE or COMMISSIONER. |
| `participations` | integer | League games officiated. |
| `year_joined_bbl` | integer | Year the official joined the league. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#scb_referees-example}

```python
scb_referees()
```

_Last validated n/a._

## scb_transfers

Latest transfers, one row per move (BBL only).

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/transfers`

**Valid URL:** [https://api.basketball-bundesliga.de/transfers](https://api.basketball-bundesliga.de/transfers)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#scb_transfers-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `transfer_date` | character | Date of the transfer, YYYY-MM-DD (the API's `sortingDate`). |
| `season_player_id` | character | Season-player id: one player's engagement with one club (kept across seasons while he stays, new after a move). Joins box-score lines, actions, records and `scb_season_player`. |
| `real_player_id` | integer | Global person id (integer): the same player across clubs and seasons. Join players across seasons on this. |
| `first_name` | character | Player's given name. |
| `last_name` | character | Player's family name. |
| `position` | character | Position: POINT_GUARD, SHOOTING_GUARD, SMALL_FORWARD, POWER_FORWARD, CENTER. |
| `source_team_id` | character | Club id the player left; null when the source is not a league club. |
| `source_team_name` | character | Name of the club the player left (the free-text field, else the short club name). |
| `destination_team_id` | character | Club id the player joined; null when the destination is not a league club. |
| `destination_team_name` | character | Name of the club the player joined (the free-text field, else the short club name). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#scb_transfers-example}

```python
scb_transfers()
```

_Last validated n/a._

## scb_transfers_teams

Per club, the players joining and leaving, one row per move with its direction (BBL only).

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/transfers/teams`

**Valid URL:** [https://api.basketball-bundesliga.de/transfers/teams](https://api.basketball-bundesliga.de/transfers/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#scb_transfers_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | character | Club id, a string; equal to `season_team_id` in every sample. Joins to `scb_team` and `scb_season_teams`. |
| `direction` | character | `joining` or `leaving`: whether `team_id` is the destination or the source of the move. |
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `transfer_date` | character | Date of the transfer, YYYY-MM-DD (the API's `sortingDate`). |
| `season_player_id` | character | Season-player id: one player's engagement with one club (kept across seasons while he stays, new after a move). Joins box-score lines, actions, records and `scb_season_player`. |
| `real_player_id` | integer | Global person id (integer): the same player across clubs and seasons. Join players across seasons on this. |
| `first_name` | character | Player's given name. |
| `last_name` | character | Player's family name. |
| `position` | character | Position: POINT_GUARD, SHOOTING_GUARD, SMALL_FORWARD, POWER_FORWARD, CENTER. |
| `source_team_id` | character | Club id the player left; null when the source is not a league club. |
| `source_team_name` | character | Name of the club the player left (the free-text field, else the short club name). |
| `destination_team_id` | character | Club id the player joined; null when the destination is not a league club. |
| `destination_team_name` | character | Name of the club the player joined (the free-text field, else the short club name). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#scb_transfers_teams-example}

```python
scb_transfers_teams()
```

_Last validated n/a._

## scb_records_per_game

Single-game records: the season's and the all-time ones, one row per record.

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/records/per-game/{season}`

**Valid URL:** [https://api.basketball-bundesliga.de/records/per-game/2025](https://api.basketball-bundesliga.de/records/per-game/2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26, the sdv key). Sent to the API as its START-year seasonId (season - 1). |
| `league` | `league` |  |  | `Y` | League: `bbl` (men, easyCredit Basketball Bundesliga, api.basketball-bundesliga.de) or `dbbl` (women, Toyota Damen-Basketball-Bundesliga, api-1.dbbl.scb.world). The two share one API; game ids overlap, so a game is (league, game_id). |

### Returns {#scb_records_per_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `season` | integer | Season END year (2026 = 2025-26), the sdv key; the API's `season_id` plus one. |
| `season_id` | integer | The API's own season key, the START year (2025 = 2025-26); `season` minus one. |
| `scope` | character | `season` for the requested season's records, `all_time` for the all-time list. |
| `stat` | character | The record's stat: points, rebounds, assists, steals, blocks, turnovers, efficiency, free_throws_made, three_point_shots_made or free_throws_success_percentage. |
| `place` | integer | Rank within the list (1 = best); 0 on some rows. |
| `all_time_place` | integer | Rank on the all-time list; null when not on it. |
| `order_id` | integer | Sort order within the list. |
| `all_time_order_id` | integer | Sort order on the all-time list; null when not on it. |
| `value` | numeric | The record value. A STRING in /records/per-game ('52'); a number in /records/season-averages. |
| `ratio` | numeric | [made, attempted] as strings, on percentage records only; null otherwise. |
| `season_player_id` | character | Season-player id: one player's engagement with one club (kept across seasons while he stays, new after a move; see README). Joins to box-score `seasonPlayer.id`, action `seasonPlayerId` and /season-players/{seasonPlayerId}. |
| `real_player_id` | integer | Global person id: the same across clubs and seasons. Joins to `realPlayerId` everywhere. |
| `first_name` | character | Player's given name. |
| `last_name` | character | Player's family name. |
| `team_name` | character | Team name as listed that season (sponsor names included). |
| `team_tlc` | character | The player's team code. |
| `home_tlc` | character | Three-letter code of the home team of the record game. |
| `guest_tlc` | character | Three-letter code of the guest team of the record game. |
| `game_id` | character | Game id, a string. Joins to /games/{gameId}. |
| `scheduled_time` | character | Scheduled tip-off, ISO 8601 UTC. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#scb_records_per_game-example}

```python
scb_records_per_game(season=2026)
```

_Last validated n/a._

## scb_records_season_averages

All-time season-average records (points, rebounds, assists, ...), one row per record.

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/records/season-averages`

**Valid URL:** [https://api.basketball-bundesliga.de/records/season-averages](https://api.basketball-bundesliga.de/records/season-averages)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League: `bbl` (men, easyCredit Basketball Bundesliga, api.basketball-bundesliga.de) or `dbbl` (women, Toyota Damen-Basketball-Bundesliga, api-1.dbbl.scb.world). The two share one API; game ids overlap, so a game is (league, game_id). |

### Returns {#scb_records_season_averages-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `stat` | character | The record's stat list: assist_average, block_average, efficiency_average, foul_average, free_throws_success_percentage, points_average, rebound_average, steal_average, three_point_shot_average or turnover_average. |
| `season` | integer | Season END year (2026 = 2025-26), the sdv key; the API's `season_id` plus one. |
| `season_id` | integer | Season, START year (2026 = 2026-27). |
| `place` | integer | Rank within the list (1 = best); 0 on some rows. |
| `order_id` | integer | Sort order within the list. |
| `value` | numeric | The record value. A STRING in /records/per-game ('52'); a number in /records/season-averages. |
| `ratio` | numeric | [made, attempted] as strings, on percentage records only; null otherwise. |
| `season_player_id` | character | Season-player id: one player's engagement with one club (kept across seasons while he stays, new after a move; see README). Joins to box-score `seasonPlayer.id`, action `seasonPlayerId` and /season-players/{seasonPlayerId}. |
| `real_player_id` | integer | Global person id: the same across clubs and seasons. Joins to `realPlayerId` everywhere. |
| `first_name` | character | Player's given name. |
| `last_name` | character | Player's family name. |
| `team_name` | character | Team name as listed that season (sponsor names included). |
| `team_tlc` | character | The player's team code. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#scb_records_season_averages-example}

```python
scb_records_season_averages()
```

_Last validated n/a._
