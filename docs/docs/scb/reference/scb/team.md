---
title: "SCB — SCB basketball API (BBL and DBBL, Germany) — Team"
sidebar_label: "Team"
sidebar_position: 2
description: "SCB — SCB basketball API (BBL and DBBL, Germany) — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# SCB — SCB basketball API (BBL and DBBL, Germany) — Team

## scb_team_statistics

Season team stat table: totals and per-game values, one row per team and stat block.

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/seasons/{season}/team-statistics`

**Valid URL:** [https://api.basketball-bundesliga.de/seasons/2025/team-statistics?displayType=MAIN_ROUND](https://api.basketball-bundesliga.de/seasons/2025/team-statistics?displayType=MAIN_ROUND)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26, the sdv key). Sent to the API as its START-year seasonId (season - 1). |
| `league` | `league` |  |  | `Y` | League: `bbl` (men, easyCredit Basketball Bundesliga, api.basketball-bundesliga.de) or `dbbl` (women, Toyota Damen-Basketball-Bundesliga, api-1.dbbl.scb.world). The two share one API; game ids overlap, so a game is (league, game_id). |
| `displayType` | `display_type` |  |  | `Y` | Which games the table covers: MAIN_ROUND (regular season), PLAYOFFS, PLAYOFFS_TOTAL, MAIN_ROUND_AND_PLAY_OFFS or CUP. |

### Returns {#scb_team_statistics-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `season` | integer | Season END year (2026 = 2025-26), the sdv key; the API's `season_id` plus one. |
| `display_type` | character | Which games the table covers, echoed from the request (MAIN_ROUND, PLAYOFFS, PLAYOFFS_TOTAL, MAIN_ROUND_AND_PLAY_OFFS or CUP). |
| `team_id` | character | Club id; equal to `seasonTeamId` in every sample. Joins to /teams/{teamId}. |
| `season_team_id` | character | Season-team id; equal to the club `teamId` in every sample (BBL 2005-2026, DBBL 2024-2026). |
| `team_name` | character | Team name as listed that season (sponsor names included, e.g. 'EWE Baskets Oldenburg'). |
| `team_tlc` | character | Three-letter club code. |
| `season_id` | integer | Season, START year (2026 = 2026-27). |
| `round_code` | integer | Provider round code; 1 for MAIN_ROUND in every sample (mapping unverified). |
| `competition` | character | Competition: BBL, BBL_CUP (Pokal), ALLSTAR, CHAMPIONS_CUP on the BBL host; DBBL, DBBL_CUP on the DBBL host. |
| `aggregation` | character | Aggregation label as sent: STAGE in player tables, MAIN_ROUND in team tables. |
| `seconds_played` | integer | The team's season total playing time in SECONDS over displayType. |
| `points` | integer | The team's season total points over displayType. |
| `two_point_shots_made` | integer | The team's season total 2-point field goals made over displayType. |
| `two_point_shots_attempted` | integer | The team's season total 2-point field goals attempted over displayType. |
| `two_point_shot_success_percent` | numeric | The team's season total 2-point field-goal percentage over displaytype, 0-100 (one decimal); 0 when no attempts. |
| `three_point_shots_made` | integer | The team's season total 3-point field goals made over displayType. |
| `three_point_shots_attempted` | integer | The team's season total 3-point field goals attempted over displayType. |
| `three_point_shot_success_percent` | numeric | The team's season total 3-point field-goal percentage over displaytype, 0-100 (one decimal); 0 when no attempts. |
| `field_goals_made` | integer | The team's season total field goals made over displayType. |
| `field_goals_attempted` | integer | The team's season total field goals attempted over displayType. |
| `field_goals_success_percent` | numeric | The team's season total field-goal percentage over displaytype, 0-100 (one decimal); 0 when no attempts. |
| `free_throws_made` | integer | The team's season total free throws made over displayType. |
| `free_throws_attempted` | integer | The team's season total free throws attempted over displayType. |
| `free_throws_success_percent` | numeric | The team's season total free-throw percentage over displaytype, 0-100 (one decimal); 0 when no attempts. |
| `offensive_rebounds` | integer | The team's season total offensive rebounds over displayType. |
| `defensive_rebounds` | integer | The team's season total defensive rebounds over displayType. |
| `total_rebounds` | integer | The team's season total rebounds (offensive + defensive) over displayType. |
| `assists` | integer | The team's season total assists over displayType. |
| `steals` | integer | The team's season total steals over displayType. |
| `turnovers` | integer | The team's season total turnovers over displayType. |
| `blocks` | integer | The team's season total blocked shots over displayType. |
| `fouls_committed` | integer | The team's season total fouls committed over displayType. |
| `efficiency` | integer | The team's season total efficiency (EFF = PTS + REB + AST + STL + BLK - missed FG - missed FT - TO; checked on a 2026 box score) over displayType. |
| `plus_minus` | integer | The team's season total plus-minus over displaytype. |
| `games_played` | integer | The team's season total games played over displayType. |
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
| `double_double` | integer | The team's season total double-doubles over displayType. |
| `double_double_per_game` | numeric | Per-game average of double-doubles. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#scb_team_statistics-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a game not yet started, an empty table, a playoff bracket not yet drawn); a game before 2002-03 is zero rows. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#scb_team_statistics-example}

```python
scb_team_statistics(display_type='MAIN_ROUND', season=2026)
```

_Last validated n/a._

## scb_team

Club profile: names, code, home page, ticket shop, year joined and honours.

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/teams/{team_id}`

**Valid URL:** [https://api.basketball-bundesliga.de/teams/413](https://api.basketball-bundesliga.de/teams/413)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Club id (`team_id` in `scb_season_teams`), e.g. 413 (ALBA BERLIN). |
| `league` | `league` |  |  | `Y` | League: `bbl` (men, easyCredit Basketball Bundesliga, api.basketball-bundesliga.de) or `dbbl` (women, Toyota Damen-Basketball-Bundesliga, api-1.dbbl.scb.world). The two share one API; game ids overlap, so a game is (league, game_id). |

### Returns {#scb_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `team_id` | character | Club id, a string; equal to `season_team_id` in every sample. Joins to `scb_team` and `scb_season_teams`. |
| `tlc` | character | Three-letter club code. |
| `name_full` | character | Full club name as the API lists it today. |
| `name_short` | character | Short name (often the city); null on DBBL. |
| `mascot` | character | Club mascot name (null when none). |
| `homepage_url` | character | Club website address as sent (it can lack the scheme). |
| `ticket_url` | character | Ticket shop URL. |
| `year_joined_bbl` | integer | Year the club joined the league. |
| `order_id` | integer | Display order of the club on the site; null on some clubs. |
| `best_achievements` | character | Honours, German free text (Deutscher Meister = German champion, Pokalsieger = cup winner, Vizepokalsieger = cup runner-up). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#scb_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a game not yet started, an empty table, a playoff bracket not yet drawn); a game before 2002-03 is zero rows. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#scb_team-example}

```python
scb_team(team_id='413')
```

_Last validated n/a._

## scb_team_seasons

Every season a club played: its name that season and its championship and cup titles.

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/teams/{team_id}`

**Valid URL:** [https://api.basketball-bundesliga.de/teams/413](https://api.basketball-bundesliga.de/teams/413)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Club id (`team_id` in `scb_season_teams`), e.g. 413 (ALBA BERLIN). |
| `league` | `league` |  |  | `Y` | League: `bbl` (men, easyCredit Basketball Bundesliga, api.basketball-bundesliga.de) or `dbbl` (women, Toyota Damen-Basketball-Bundesliga, api-1.dbbl.scb.world). The two share one API; game ids overlap, so a game is (league, game_id). |

### Returns {#scb_team_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `team_id` | character | Club id, a string; equal to `season_team_id` in every sample. Joins to `scb_team` and `scb_season_teams`. |
| `season` | integer | Season END year (2026 = 2025-26), the sdv key; the API's `season_id` plus one. |
| `season_id` | integer | Season, START year (2026 = 2026-27). |
| `season_team_id` | character | Season-team id, a string; equal to the club `team_id` in every sample (BBL 2005-2026, DBBL 2024-2026). |
| `team_name` | character | Team name as listed that season (sponsor names included). |
| `is_championship_winner` | logical | Won the championship that season. |
| `is_cup_winner` | logical | Won the cup that season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#scb_team_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a game not yet started, an empty table, a playoff bracket not yet drawn); a game before 2002-03 is zero rows. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#scb_team_seasons-example}

```python
scb_team_seasons(team_id='413')
```

_Last validated n/a._

## scb_team_rosters

Per club, the current roster, one row per player (BBL only).

**Endpoint URL:** `GET https://api.basketball-bundesliga.de/transfers/teams`

**Valid URL:** [https://api.basketball-bundesliga.de/transfers/teams](https://api.basketball-bundesliga.de/transfers/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#scb_team_rosters-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league` | character | League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id). |
| `team_id` | character | Club id, a string; equal to `season_team_id` in every sample. Joins to `scb_team` and `scb_season_teams`. |
| `season` | integer | Season END year (2026 = 2025-26), the sdv key; the API's `season_id` plus one. |
| `season_id` | integer | Season, START year (2026 = 2026-27). |
| `season_team_id` | character | Season-team id; equal to the club `teamId` in every sample (BBL 2005-2026, DBBL 2024-2026). |
| `season_player_id` | character | Season-player id: one player's engagement with one club (kept across seasons while he stays, new after a move). Joins box-score lines, actions, records and `scb_season_player`. |
| `real_player_id` | integer | Global person id: the same across clubs and seasons. Joins to `realPlayerId` everywhere. |
| `first_name` | character | Player's given name. |
| `last_name` | character | Player's family name. |
| `birth_date` | character | Date of birth, YYYY-MM-DD. |
| `position` | character | Position: POINT_GUARD, SHOOTING_GUARD, SMALL_FORWARD, POWER_FORWARD, CENTER. |
| `shirt_number` | character | Jersey number as sent (a string: it can be `00`). |
| `height` | numeric | Height in metres (float32 noise: 1.77 arrives as 1.7699999809265137). |
| `weight` | integer | Weight in kg; 0 when unknown. |
| `nationalities` | character | Nationalities, ISO 3166-1 alpha-2 codes. |
| `is_all_star` | logical | All-Star selection flag. |
| `is_young` | logical | Young-player flag (German-trained youth rule; inferred). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#scb_team_rosters-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an unknown id, a game not yet started, an empty table, a playoff bracket not yet drawn); a game before 2002-03 is zero rows. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#scb_team_rosters-example}

```python
scb_team_rosters()
```

_Last validated n/a._
