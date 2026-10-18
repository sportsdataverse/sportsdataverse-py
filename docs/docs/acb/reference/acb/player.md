---
title: "ACB — ACB API (api2.acb.com, Spain) — Player"
sidebar_label: "Player"
sidebar_position: 2
description: "ACB — ACB API (api2.acb.com, Spain) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# ACB — ACB API (api2.acb.com, Spain) — Player

## acb_player_box

Per-player box score of a game, per period (quarter 0 = the whole game).

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/Result/boxscores`

**Valid URL:** [https://api2.acb.com/api/matchdata/Result/boxscores?matchId=105401](https://api2.acb.com/api/matchdata/Result/boxscores?matchId=105401)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `matchId` | `match_id` |  | `Y` |  | Required. ACB match id (integer), one global sequence for every competition: legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = the 2005-06 final); ids from 2020-21 start around 100000 (105401 = Barca 89-86 Valencia Basket, Liga 2026-27 round 3). |

### Returns {#acb_player_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `edition_id` | integer | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `team_side` | character | Side of the game the team played: home or away (the feed lists the home team first). |
| `team_id` | integer | Season-team entry id: one club in one competition edition, NEW every season (Barca = 4467 in Liga 2026-27). Not the club: use club_id to follow a club across seasons. |
| `club_id` | integer | Stable ACB club id (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket): the same across seasons and sponsor names, and the club_id request argument. |
| `team` | character | Team name as registered for that season (sponsor names change; the club id does not). |
| `team_abbr` | character | Three-letter team abbreviation (BAR, RMB, VBC). |
| `quarter` | integer | Period of the row: 0 = the whole game, 1-4 quarters, 5+ overtimes. |
| `player_id` | integer | ACB licence number: the person id everywhere. A player's box-score `player.id`, his play-by-play and shot `playerLicenseId` and the `playerLicenceId` / `playerLicense` request params hold the same number (all 24 players of match 105401 match). 20209407 = Edy Tavares. Older licences start 2020..., recent ones 3000.... |
| `player_name` | character | Display (sporting) name ('Edy Tavares') |
| `first_name` | character | Legal first name(s) |
| `last_name` | character | Legal last name(s) |
| `shirt_number` | character | Shirt number, a string ('' or null when unknown) |
| `game_role` | character | Position or staff role, in Spanish: Base (point guard), Escolta (shooting guard), Alero (small forward), Ala-pivot (power forward), Pivot (centre); Entrenador (head coach), Segundo Entrenador / Entrenador Ayudante (assistant coach). Null or '' in many rows |
| `on_court` | logical | True while the player is on court (false once the game is over). |
| `play_time` | character | Time on court as 'mm:ss' in the row's period. |
| `seconds_played` | integer | Time on court in whole seconds (play_time parsed). |
| `is_starter` | logical | True when the player started the game (the API's isStarted). |
| `points` | integer | Points (player box-score row) |
| `free_throws_made` | integer | Free throws the player made in the row's period (a count; quarter 0 is the whole game). |
| `free_throws_attempted` | integer | Free throws the player attempted in the row's period (a count; quarter 0 is the whole game). |
| `two_pointers_made` | integer | Two-point field goals made |
| `two_pointers_attempted` | integer | Two-point field goals attempted |
| `three_pointers_made` | integer | Three-point field goals made |
| `three_pointers_attempted` | integer | Three-point field goals attempted |
| `assists` | integer | Assists (player box-score row) |
| `off_rebounds` | integer | Offensive rebounds |
| `def_rebounds` | integer | Defensive rebounds |
| `total_rebounds` | integer | Total rebounds (player box-score row) |
| `steals` | integer | Steals (player box-score row) |
| `turnovers` | integer | Turnovers (player box-score row) |
| `blocks` | integer | Shots blocked (by this player or team) |
| `received_blocks` | integer | Own shots blocked by an opponent |
| `dunks` | integer | Dunks (player box-score row) |
| `personal_fouls` | integer | Fouls committed |
| `fouls_drawn` | integer | Fouls received (player box-score row) |
| `plus_minus` | integer | Point differential while on court (+/-) |
| `rating` | integer | Valoracion: ACB's efficiency rating (PIR) = points + rebounds + assists + steals + blocks + fouls received - missed field goals - missed free throws - turnovers - shots blocked by an opponent - fouls committed |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_player_box-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the ACB has nothing for the request (an unknown id, a game outside a playoff series, play-by-play or shots before 2016-17, a club with no games that season). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | a missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_player_box-example}

```python
acb_player_box(match_id=105401)
```

_Last validated n/a._

## acb_player_advanced_stats

One player's advanced statistics in a game against his season and win/loss splits.

**Endpoint URL:** `GET https://api2.acb.com/api/matchdata/AdvancedStats/player-advanced-stats`

**Valid URL:** [https://api2.acb.com/api/matchdata/AdvancedStats/player-advanced-stats?matchId=105401&playerLicense=20212243](https://api2.acb.com/api/matchdata/AdvancedStats/player-advanced-stats?matchId=105401&playerLicense=20212243)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `matchId` | `match_id` |  | `Y` |  | Required. ACB match id (integer), one global sequence for every competition: legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = the 2005-06 final); ids from 2020-21 start around 100000 (105401 = Barca 89-86 Valencia Basket, Liga 2026-27 round 3). |
| `playerLicense` | `player_id` |  | `Y` |  | Required. ACB licence number (integer): the person id everywhere (box score, play-by-play, shots, rosters). 20209407 = Edy Tavares, 20212243 = Olek Balcerowski. |

### Returns {#acb_player_advanced_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | integer | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `player_id` | integer | ACB licence number: the person id everywhere (box score, play-by-play, shots, rosters, the player routes). Taken from the request when the body does not name the player. |
| `group` | character | Statistic group in snake_case (four_factors, game_rhythm, ball_handling, shooting, points, ratings, rebounds). |
| `stat` | character | Statistic in snake_case (efg_pct, ts_pct, ast_pct, usage-style ratios, pace ...). |
| `match_value` | numeric | The statistic in this game (the API's partido). |
| `season_value` | numeric | The player's value over the season (temporada). |
| `win_value` | numeric | The player's value in his team's wins. |
| `loss_value` | numeric | The player's value in his team's losses. |
| `season_diff` | numeric | Game value minus season value (temporadaDiff). |
| `win_diff` | numeric | Game value minus the wins value (winDiff). |
| `loss_diff` | numeric | Game value minus the losses value (lossDiff). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_player_advanced_stats-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the ACB has nothing for the request (an unknown id, a game outside a playoff series, play-by-play or shots before 2016-17, a club with no games that season). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | a missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_player_advanced_stats-example}

```python
acb_player_advanced_stats(match_id=105401, player_id=20212243)
```

_Last validated n/a._

## acb_player_data

Player biography: birth, nationality, height, current team and social links.

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/PlayerProfile/player-data`

**Valid URL:** [https://api2.acb.com/api/seasondata/PlayerProfile/player-data?playerLicenceId=20209407](https://api2.acb.com/api/seasondata/PlayerProfile/player-data?playerLicenceId=20209407)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerLicenceId` | `player_id` |  | `Y` |  | Required. ACB licence number (integer): the person id everywhere (box score, play-by-play, shots, rosters). 20209407 = Edy Tavares, 20212243 = Olek Balcerowski. |

### Returns {#acb_player_data-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | ACB licence number of the player. |
| `player_name` | character | Display (sporting) name ('Edy Tavares') |
| `first_name` | character | Legal first name(s) |
| `last_name` | character | Legal last name(s) |
| `shirt_number` | character | Shirt number, a string ('' or null when unknown) |
| `game_role` | character | Position or staff role, in Spanish: Base (point guard), Escolta (shooting guard), Alero (small forward), Ala-pivot (power forward), Pivot (centre); Entrenador (head coach), Segundo Entrenador / Entrenador Ayudante (assistant coach). Null or '' in many rows |
| `is_license_active` | logical | True when the licence is active in the current season. |
| `licensing` | character | Licence code (for example EUR, JFL); meaning unverified. |
| `birth_date` | character | Birth date (the API sends DD-MM-YYYY). |
| `birth_place` | character | Birth place (town or city) as sent. |
| `birth_country` | character | Birth country as sent, a Spanish name. |
| `birth_country_iso` | character | ISO 3166-1 alpha-2 code of the birth country; null for an unseen name. |
| `nationality` | character | Nationality as sent, a Spanish country name. |
| `nationality_iso` | character | ISO 3166-1 alpha-2 code of nationality; null for an unseen name. |
| `height_m` | numeric | Height in metres (the API sends '2,20', a decimal comma). |
| `player_number` | integer | Current shirt number (integer). |
| `team_id` | integer | Current team's season-team entry id. |
| `club_id` | integer | Current team's stable club id. |
| `team` | character | Current team name for the edition. |
| `team_abbr` | character | Current team abbreviation. |
| `edition_id` | integer | Season id the player record was read for (START year - 1935). |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `instagram` | character | Instagram handle; null when none. |
| `twitter` | character | X / Twitter handle; null when none. |
| `facebook` | character | Facebook handle; null when none. |
| `youtube` | character | YouTube handle; null when none. |
| `tiktok` | character | TikTok handle; null when none. |
| `pronunciation_url` | character | URL of an audio clip of the name; null when none. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_player_data-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the ACB has nothing for the request (an unknown id, a game outside a playoff series, play-by-play or shots before 2016-17, a club with no games that season). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | a missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_player_data-example}

```python
acb_player_data(player_id=20209407)
```

_Last validated n/a._

## acb_player_stats

A player's season statistics, one row per team and edition.

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/PlayerProfile/player-stats`

**Valid URL:** [https://api2.acb.com/api/seasondata/PlayerProfile/player-stats?playerLicenceId=20209407](https://api2.acb.com/api/seasondata/PlayerProfile/player-stats?playerLicenceId=20209407)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerLicenceId` | `player_id` |  | `Y` |  | Required. ACB licence number (integer): the person id everywhere (box score, play-by-play, shots, rosters). 20209407 = Edy Tavares, 20212243 = Olek Balcerowski. |
| `competitionId` | `competition_id` |  |  | `Y` | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (youth). Omitted = the API's default competition. |
| `season` | `season` |  |  | `Y` | Season END year used as an UPPER bound: the rows keep the seasons up to this one (2026 drops the 2026-27 row). Omitted = all seasons. |

### Returns {#acb_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | ACB licence number asked for (the body names the player only in the totals block). |
| `team_id` | integer | Season-team entry the row covers (the API's teamEditionId). |
| `competition_id` | integer | Competition of the row. |
| `edition_id` | integer | Season id of the row (START year - 1935). |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `edition_year` | character | Season label as sent ('2026-2027'). |
| `club_id` | integer | Stable club id of the team. |
| `team` | character | Team name that season. |
| `team_abbr` | character | Team abbreviation that season. |
| `total_num_games` | integer | Season total of games played. |
| `total_time_played` | integer | Season total of time on court in seconds. |
| `total_starter` | integer | Season total of games started. |
| `total_points` | integer | Season total of points. |
| `total_max_points` | integer | Season total of most points in a single game. |
| `total_three_pointers_made` | integer | Season total of three-point field goals made. |
| `total_three_pointers_attempted` | integer | Season total of three-point field goals attempted. |
| `total_three_pointers_percentage` | numeric | Season total of three-point percentage (0-100). |
| `total_two_pointers_made` | integer | Season total of two-point field goals made. |
| `total_two_pointers_attempted` | integer | Season total of two-point field goals attempted. |
| `total_two_pointers_percentage` | numeric | Season total of two-point percentage (0-100). |
| `total_free_throws_made` | integer | Season total of free throws made. |
| `total_free_throws_attempted` | integer | Season total of free throws attempted. |
| `total_free_throws_percentage` | numeric | Season total of free-throw percentage (0-100). |
| `total_rebounds` | integer | Season total of total rebounds. |
| `total_offensive_rebounds` | integer | Season total of offensive rebounds. |
| `total_defensive_rebounds` | integer | Season total of defensive rebounds. |
| `total_assists` | integer | Season total of assists. |
| `total_steals` | integer | Season total of steals. |
| `total_turnovers` | integer | Season total of turnovers. |
| `total_blocks` | integer | Season total of blocks made. |
| `total_shots_rejected` | integer | Season total of own shots blocked. |
| `total_dunks` | integer | Season total of dunks. |
| `total_personal_fouls` | integer | Season total of personal fouls committed. |
| `total_fouls_received` | integer | Season total of fouls drawn. |
| `total_rating` | integer | Season total of ACB rating (valoracion). |
| `total_plus_minus` | integer | Season total of plus/minus. |
| `total_wins` | integer | Season total of team wins in the counted games (meaning unverified). |
| `total_losses` | integer | Season total of team losses in the counted games (meaning unverified). |
| `avg_num_games` | numeric | Per-game average of games played. |
| `avg_time_played` | numeric | Per-game average of time on court in seconds. |
| `avg_starter` | numeric | Per-game average of games started. |
| `avg_points` | numeric | Per-game average of points. |
| `avg_max_points` | numeric | Per-game average of most points in a single game. |
| `avg_three_pointers_made` | numeric | Per-game average of three-point field goals made. |
| `avg_three_pointers_attempted` | numeric | Per-game average of three-point field goals attempted. |
| `avg_three_pointers_percentage` | numeric | Per-game average of three-point percentage (0-100). |
| `avg_two_pointers_made` | numeric | Per-game average of two-point field goals made. |
| `avg_two_pointers_attempted` | numeric | Per-game average of two-point field goals attempted. |
| `avg_two_pointers_percentage` | numeric | Per-game average of two-point percentage (0-100). |
| `avg_free_throws_made` | numeric | Per-game average of free throws made. |
| `avg_free_throws_attempted` | numeric | Per-game average of free throws attempted. |
| `avg_free_throws_percentage` | numeric | Per-game average of free-throw percentage (0-100). |
| `avg_rebounds` | numeric | Per-game average of total rebounds. |
| `avg_offensive_rebounds` | numeric | Per-game average of offensive rebounds. |
| `avg_defensive_rebounds` | numeric | Per-game average of defensive rebounds. |
| `avg_assists` | numeric | Per-game average of assists. |
| `avg_steals` | numeric | Per-game average of steals. |
| `avg_turnovers` | numeric | Per-game average of turnovers. |
| `avg_blocks` | numeric | Per-game average of blocks made. |
| `avg_shots_rejected` | numeric | Per-game average of own shots blocked. |
| `avg_dunks` | numeric | Per-game average of dunks. |
| `avg_personal_fouls` | numeric | Per-game average of personal fouls committed. |
| `avg_fouls_received` | numeric | Per-game average of fouls drawn. |
| `avg_rating` | numeric | Per-game average of ACB rating (valoracion). |
| `avg_plus_minus` | numeric | Per-game average of plus/minus. |
| `avg_wins` | numeric | Per-game average of team wins in the counted games (meaning unverified). |
| `avg_losses` | numeric | Per-game average of team losses in the counted games (meaning unverified). |
| `per40_num_games` | numeric | Per-40-minute rate of games played. |
| `per40_time_played` | numeric | Always null: the per-40 block carries no time played. |
| `per40_starter` | numeric | Per-40-minute rate of games started. |
| `per40_points` | numeric | Per-40-minute rate of points. |
| `per40_max_points` | numeric | Per-40-minute rate of most points in a single game. |
| `per40_three_pointers_made` | numeric | Per-40-minute rate of three-point field goals made. |
| `per40_three_pointers_attempted` | numeric | Per-40-minute rate of three-point field goals attempted. |
| `per40_three_pointers_percentage` | numeric | Per-40-minute rate of three-point percentage (0-100). |
| `per40_two_pointers_made` | numeric | Per-40-minute rate of two-point field goals made. |
| `per40_two_pointers_attempted` | numeric | Per-40-minute rate of two-point field goals attempted. |
| `per40_two_pointers_percentage` | numeric | Per-40-minute rate of two-point percentage (0-100). |
| `per40_free_throws_made` | numeric | Per-40-minute rate of free throws made. |
| `per40_free_throws_attempted` | numeric | Per-40-minute rate of free throws attempted. |
| `per40_free_throws_percentage` | numeric | Per-40-minute rate of free-throw percentage (0-100). |
| `per40_rebounds` | numeric | Per-40-minute rate of total rebounds. |
| `per40_offensive_rebounds` | numeric | Per-40-minute rate of offensive rebounds. |
| `per40_defensive_rebounds` | numeric | Per-40-minute rate of defensive rebounds. |
| `per40_assists` | numeric | Per-40-minute rate of assists. |
| `per40_steals` | numeric | Per-40-minute rate of steals. |
| `per40_turnovers` | numeric | Per-40-minute rate of turnovers. |
| `per40_blocks` | numeric | Per-40-minute rate of blocks made. |
| `per40_shots_rejected` | numeric | Per-40-minute rate of own shots blocked. |
| `per40_dunks` | numeric | Per-40-minute rate of dunks. |
| `per40_personal_fouls` | numeric | Per-40-minute rate of personal fouls committed. |
| `per40_fouls_received` | numeric | Per-40-minute rate of fouls drawn. |
| `per40_rating` | numeric | Per-40-minute rate of ACB rating (valoracion). |
| `per40_plus_minus` | numeric | Per-40-minute rate of plus/minus. |
| `per40_wins` | numeric | Per-40-minute rate of team wins in the counted games (meaning unverified). |
| `per40_losses` | numeric | Per-40-minute rate of team losses in the counted games (meaning unverified). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_player_stats-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the ACB has nothing for the request (an unknown id, a game outside a playoff series, play-by-play or shots before 2016-17, a club with no games that season). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | a missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_player_stats-example}

```python
acb_player_stats(player_id=20209407)
```

_Last validated n/a._

## acb_player_totals

A player's career totals, averages and per-40 values.

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/PlayerProfile/player-stats`

**Valid URL:** [https://api2.acb.com/api/seasondata/PlayerProfile/player-stats?playerLicenceId=20209407](https://api2.acb.com/api/seasondata/PlayerProfile/player-stats?playerLicenceId=20209407)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerLicenceId` | `player_id` |  | `Y` |  | Required. ACB licence number (integer): the person id everywhere (box score, play-by-play, shots, rosters). 20209407 = Edy Tavares, 20212243 = Olek Balcerowski. |
| `competitionId` | `competition_id` |  |  | `Y` | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (youth). Omitted = the API's default competition. |
| `season` | `season` |  |  | `Y` | Season END year used as an UPPER bound for the career rows. Omitted = all seasons. |

### Returns {#acb_player_totals-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | ACB licence number of the player. |
| `player_name` | character | Display (sporting) name ('Edy Tavares') |
| `first_name` | character | Legal first name(s) |
| `last_name` | character | Legal last name(s) |
| `shirt_number` | character | Shirt number, a string ('' or null when unknown) |
| `game_role` | character | Position or staff role, in Spanish: Base (point guard), Escolta (shooting guard), Alero (small forward), Ala-pivot (power forward), Pivot (centre); Entrenador (head coach), Segundo Entrenador / Entrenador Ayudante (assistant coach). Null or '' in many rows |
| `total_num_games` | integer | Season total of games played. |
| `total_time_played` | integer | Season total of time on court in seconds. |
| `total_starter` | integer | Season total of games started. |
| `total_points` | integer | Season total of points. |
| `total_max_points` | integer | Season total of most points in a single game. |
| `total_three_pointers_made` | integer | Season total of three-point field goals made. |
| `total_three_pointers_attempted` | integer | Season total of three-point field goals attempted. |
| `total_three_pointers_percentage` | numeric | Season total of three-point percentage (0-100). |
| `total_two_pointers_made` | integer | Season total of two-point field goals made. |
| `total_two_pointers_attempted` | integer | Season total of two-point field goals attempted. |
| `total_two_pointers_percentage` | numeric | Season total of two-point percentage (0-100). |
| `total_free_throws_made` | integer | Season total of free throws made. |
| `total_free_throws_attempted` | integer | Season total of free throws attempted. |
| `total_free_throws_percentage` | numeric | Season total of free-throw percentage (0-100). |
| `total_rebounds` | integer | Season total of total rebounds. |
| `total_offensive_rebounds` | integer | Season total of offensive rebounds. |
| `total_defensive_rebounds` | integer | Season total of defensive rebounds. |
| `total_assists` | integer | Season total of assists. |
| `total_steals` | integer | Season total of steals. |
| `total_turnovers` | integer | Season total of turnovers. |
| `total_blocks` | integer | Season total of blocks made. |
| `total_shots_rejected` | integer | Season total of own shots blocked. |
| `total_dunks` | integer | Season total of dunks. |
| `total_personal_fouls` | integer | Season total of personal fouls committed. |
| `total_fouls_received` | integer | Season total of fouls drawn. |
| `total_rating` | integer | Season total of ACB rating (valoracion). |
| `total_plus_minus` | integer | Season total of plus/minus. |
| `total_wins` | integer | Season total of team wins in the counted games (meaning unverified). |
| `total_losses` | integer | Season total of team losses in the counted games (meaning unverified). |
| `avg_num_games` | numeric | Per-game average of games played. |
| `avg_time_played` | numeric | Per-game average of time on court in seconds. |
| `avg_starter` | numeric | Per-game average of games started. |
| `avg_points` | numeric | Per-game average of points. |
| `avg_max_points` | numeric | Per-game average of most points in a single game. |
| `avg_three_pointers_made` | numeric | Per-game average of three-point field goals made. |
| `avg_three_pointers_attempted` | numeric | Per-game average of three-point field goals attempted. |
| `avg_three_pointers_percentage` | numeric | Per-game average of three-point percentage (0-100). |
| `avg_two_pointers_made` | numeric | Per-game average of two-point field goals made. |
| `avg_two_pointers_attempted` | numeric | Per-game average of two-point field goals attempted. |
| `avg_two_pointers_percentage` | numeric | Per-game average of two-point percentage (0-100). |
| `avg_free_throws_made` | numeric | Per-game average of free throws made. |
| `avg_free_throws_attempted` | numeric | Per-game average of free throws attempted. |
| `avg_free_throws_percentage` | numeric | Per-game average of free-throw percentage (0-100). |
| `avg_rebounds` | numeric | Per-game average of total rebounds. |
| `avg_offensive_rebounds` | numeric | Per-game average of offensive rebounds. |
| `avg_defensive_rebounds` | numeric | Per-game average of defensive rebounds. |
| `avg_assists` | numeric | Per-game average of assists. |
| `avg_steals` | numeric | Per-game average of steals. |
| `avg_turnovers` | numeric | Per-game average of turnovers. |
| `avg_blocks` | numeric | Per-game average of blocks made. |
| `avg_shots_rejected` | numeric | Per-game average of own shots blocked. |
| `avg_dunks` | numeric | Per-game average of dunks. |
| `avg_personal_fouls` | numeric | Per-game average of personal fouls committed. |
| `avg_fouls_received` | numeric | Per-game average of fouls drawn. |
| `avg_rating` | numeric | Per-game average of ACB rating (valoracion). |
| `avg_plus_minus` | numeric | Per-game average of plus/minus. |
| `avg_wins` | numeric | Per-game average of team wins in the counted games (meaning unverified). |
| `avg_losses` | numeric | Per-game average of team losses in the counted games (meaning unverified). |
| `per40_num_games` | numeric | Per-40-minute rate of games played. |
| `per40_time_played` | numeric | Always null: the per-40 block carries no time played. |
| `per40_starter` | numeric | Per-40-minute rate of games started. |
| `per40_points` | numeric | Per-40-minute rate of points. |
| `per40_max_points` | numeric | Per-40-minute rate of most points in a single game. |
| `per40_three_pointers_made` | numeric | Per-40-minute rate of three-point field goals made. |
| `per40_three_pointers_attempted` | numeric | Per-40-minute rate of three-point field goals attempted. |
| `per40_three_pointers_percentage` | numeric | Per-40-minute rate of three-point percentage (0-100). |
| `per40_two_pointers_made` | numeric | Per-40-minute rate of two-point field goals made. |
| `per40_two_pointers_attempted` | numeric | Per-40-minute rate of two-point field goals attempted. |
| `per40_two_pointers_percentage` | numeric | Per-40-minute rate of two-point percentage (0-100). |
| `per40_free_throws_made` | numeric | Per-40-minute rate of free throws made. |
| `per40_free_throws_attempted` | numeric | Per-40-minute rate of free throws attempted. |
| `per40_free_throws_percentage` | numeric | Per-40-minute rate of free-throw percentage (0-100). |
| `per40_rebounds` | numeric | Per-40-minute rate of total rebounds. |
| `per40_offensive_rebounds` | numeric | Per-40-minute rate of offensive rebounds. |
| `per40_defensive_rebounds` | numeric | Per-40-minute rate of defensive rebounds. |
| `per40_assists` | numeric | Per-40-minute rate of assists. |
| `per40_steals` | numeric | Per-40-minute rate of steals. |
| `per40_turnovers` | numeric | Per-40-minute rate of turnovers. |
| `per40_blocks` | numeric | Per-40-minute rate of blocks made. |
| `per40_shots_rejected` | numeric | Per-40-minute rate of own shots blocked. |
| `per40_dunks` | numeric | Per-40-minute rate of dunks. |
| `per40_personal_fouls` | numeric | Per-40-minute rate of personal fouls committed. |
| `per40_fouls_received` | numeric | Per-40-minute rate of fouls drawn. |
| `per40_rating` | numeric | Per-40-minute rate of ACB rating (valoracion). |
| `per40_plus_minus` | numeric | Per-40-minute rate of plus/minus. |
| `per40_wins` | numeric | Per-40-minute rate of team wins in the counted games (meaning unverified). |
| `per40_losses` | numeric | Per-40-minute rate of team losses in the counted games (meaning unverified). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_player_totals-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the ACB has nothing for the request (an unknown id, a game outside a playoff series, play-by-play or shots before 2016-17, a club with no games that season). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | a missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_player_totals-example}

```python
acb_player_totals(player_id=20209407)
```

_Last validated n/a._

## acb_player_matches

A player's game log for an edition (default: the current one).

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/PlayerProfile/matches`

**Valid URL:** [https://api2.acb.com/api/seasondata/PlayerProfile/matches?playerLicenceId=20209407&season=2027](https://api2.acb.com/api/seasondata/PlayerProfile/matches?playerLicenceId=20209407&season=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerLicenceId` | `player_id` |  | `Y` |  | Required. ACB licence number (integer): the person id everywhere (box score, play-by-play, shots, rosters). 20209407 = Edy Tavares, 20212243 = Olek Balcerowski. |
| `season` | `season` |  |  | `Y` | Season END year (2027 = 2026-27; the first ACB season, 1983-84, is 1984). ``acb_runtime`` sends the API's ``editionId`` = season - 1936. Omitted = the current season. |

### Returns {#acb_player_matches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | ACB licence number asked for. |
| `match_id` | integer | ACB match id of the game. |
| `round_type` | character | Phase code of the game (LR regular season, PO playoff). |
| `round_number` | integer | Round number within the phase. |
| `date` | character | Tip-off instant (UTC). |
| `home_team_id` | integer | Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `home_club_id` | integer | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `home_team` | character | Team name that season (sponsor names change) |
| `home_team_abbr` | character | Three-letter abbreviation (BAR, RMB) |
| `away_team_id` | integer | Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `away_club_id` | integer | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `away_team` | character | Team name that season (sponsor names change) |
| `away_team_abbr` | character | Three-letter abbreviation (BAR, RMB) |
| `edition_id` | integer | Season id read from the home team entry (START year - 1935). |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `is_selected_local` | logical | True when the player's team played at home. |
| `has_won` | logical | True when the player's team won the game. |
| `play_time` | character | Time on court as 'mm:ss'. |
| `seconds_played` | integer | Time on court in whole seconds (play_time parsed). |
| `points` | integer | Points (player game-log row) |
| `three_pointers_made` | integer | Three-point field goals made |
| `three_pointers_attempted` | integer | Three-point field goals attempted |
| `three_pointers_percentage` | numeric | Three-point percentage, 0-100 with one decimal; 0 when there was no attempt |
| `two_pointers_made` | integer | Two-point field goals made |
| `two_pointers_attempted` | integer | Two-point field goals attempted |
| `two_pointers_percentage` | numeric | Two-point percentage, 0-100 with one decimal; 0 when there was no attempt |
| `free_throws_made` | integer | Free throws the player made in that match (a count, not a season total). |
| `free_throws_attempted` | integer | Free throws the player attempted in that match (a count, not a season total). |
| `free_throws_percentage` | numeric | Free-throw percentage, 0-100 with one decimal; 0 when there was no attempt |
| `rebounds` | integer | Total rebounds in the game. |
| `offensive_rebounds` | integer | Offensive rebounds the player took in that match (a count; part of the total rebounds). |
| `defensive_rebounds` | integer | Defensive rebounds the player took in that match (a count; part of the total rebounds). |
| `assists` | integer | Assists (player game-log row) |
| `steals` | integer | Steals (player game-log row) |
| `turnovers` | integer | Turnovers (player game-log row) |
| `blocks` | integer | Shots blocked (player game-log row) |
| `shots_rejected` | integer | Shots of the player that were blocked. |
| `dunks` | integer | Dunks (player game-log row) |
| `personal_fouls` | integer | Fouls committed |
| `fouls_received` | integer | Fouls drawn by the player in the game. |
| `rating` | integer | ACB rating (valoracion) of the game. |
| `plus_minus` | integer | Point differential while on court (+/-) |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_player_matches-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the ACB has nothing for the request (an unknown id, a game outside a playoff series, play-by-play or shots before 2016-17, a club with no games that season). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | a missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_player_matches-example}

```python
acb_player_matches(player_id=20209407, season=2027)
```

_Last validated n/a._
