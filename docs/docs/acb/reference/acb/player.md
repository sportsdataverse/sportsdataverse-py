---
title: "ACB — ACB API (api2.acb.com, Spain) — Player"
sidebar_label: "Player"
sidebar_position: 3
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
| `match_id` | Int64 | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `edition_id` | Int64 | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `team_side` | String | Side of the game the team played: home or away (the feed lists the home team first). |
| `team_id` | Int64 | Season-team entry id: one club in one competition edition, NEW every season (Barca = 4467 in Liga 2026-27). Not the club: use club_id to follow a club across seasons. |
| `club_id` | Int64 | Stable ACB club id (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket): the same across seasons and sponsor names, and the club_id request argument. |
| `team` | String | Team name as registered for that season (sponsor names change; the club id does not). |
| `team_abbr` | String | Three-letter team abbreviation (BAR, RMB, VBC). |
| `quarter` | Int64 | Period of the row: 0 = the whole game, 1-4 quarters, 5+ overtimes. |
| `player_id` | Int64 | ACB licence number: the person id everywhere. A player's box-score `player.id`, his play-by-play and shot `playerLicenseId` and the `playerLicenceId` / `playerLicense` request params hold the same number (all 24 players of match 105401 match). 20209407 = Edy Tavares. Older licences start 2020..., recent ones 3000.... |
| `player_name` | String | Display (sporting) name ('Edy Tavares') |
| `first_name` | String | Legal first name(s) |
| `last_name` | String | Legal last name(s) |
| `shirt_number` | String | Shirt number, a string ('' or null when unknown) |
| `game_role` | String | Position or staff role, in Spanish: Base (point guard), Escolta (shooting guard), Alero (small forward), Ala-pivot (power forward), Pivot (centre); Entrenador (head coach), Segundo Entrenador / Entrenador Ayudante (assistant coach). Null or '' in many rows |
| `on_court` | Boolean | True while the player is on court (false once the game is over). |
| `play_time` | String | Time on court as 'mm:ss' in the row's period. |
| `seconds_played` | Int64 | Time on court in whole seconds (play_time parsed). |
| `is_starter` | Boolean | True when the player started the game (the API's isStarted). |
| `points` | Int64 | Points (player box-score row) |
| `free_throws_made` | Int64 | Free throws the player made in the row's period (a count; quarter 0 is the whole game). |
| `free_throws_attempted` | Int64 | Free throws the player attempted in the row's period (a count; quarter 0 is the whole game). |
| `two_pointers_made` | Int64 | Two-point field goals made |
| `two_pointers_attempted` | Int64 | Two-point field goals attempted |
| `three_pointers_made` | Int64 | Three-point field goals made |
| `three_pointers_attempted` | Int64 | Three-point field goals attempted |
| `assists` | Int64 | Assists (player box-score row) |
| `off_rebounds` | Int64 | Offensive rebounds |
| `def_rebounds` | Int64 | Defensive rebounds |
| `total_rebounds` | Int64 | Total rebounds (player box-score row) |
| `steals` | Int64 | Steals (player box-score row) |
| `turnovers` | Int64 | Turnovers (player box-score row) |
| `blocks` | Int64 | Shots blocked (by this player or team) |
| `received_blocks` | Int64 | Own shots blocked by an opponent |
| `dunks` | Int64 | Dunks (player box-score row) |
| `personal_fouls` | Int64 | Fouls committed |
| `fouls_drawn` | Int64 | Fouls received (player box-score row) |
| `plus_minus` | Int64 | Point differential while on court (+/-) |
| `rating` | Int64 | Valoracion: ACB's efficiency rating (PIR) = points + rebounds + assists + steals + blocks + fouls received - missed field goals - missed free throws - turnovers - shots blocked by an opponent - fouls committed |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_player_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_player_box-example}

```python
acb_player_box(match_id=105401)
```

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
| `match_id` | Int64 | ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to 2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game. |
| `player_id` | Int64 | ACB licence number: the person id everywhere (box score, play-by-play, shots, rosters, the player routes). Taken from the request when the body does not name the player. |
| `group` | String | Statistic group in snake_case (four_factors, game_rhythm, ball_handling, shooting, points, ratings, rebounds). |
| `stat` | String | Statistic in snake_case (efg_pct, ts_pct, ast_pct, usage-style ratios, pace ...). |
| `match_value` | Float64 | The statistic in this game (the API's partido). |
| `season_value` | Float64 | The player's value over the season (temporada). |
| `win_value` | Float64 | The player's value in his team's wins. |
| `loss_value` | Float64 | The player's value in his team's losses. |
| `season_diff` | Float64 | Game value minus season value (temporadaDiff). |
| `win_diff` | Float64 | Game value minus the wins value (winDiff). |
| `loss_diff` | Float64 | Game value minus the losses value (lossDiff). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_player_advanced_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_player_advanced_stats-example}

```python
acb_player_advanced_stats(match_id=105401, player_id=20212243)
```

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
| `player_id` | Int64 | ACB licence number of the player. |
| `player_name` | String | Display (sporting) name ('Edy Tavares') |
| `first_name` | String | Legal first name(s) |
| `last_name` | String | Legal last name(s) |
| `shirt_number` | String | Shirt number, a string ('' or null when unknown) |
| `game_role` | String | Position or staff role, in Spanish: Base (point guard), Escolta (shooting guard), Alero (small forward), Ala-pivot (power forward), Pivot (centre); Entrenador (head coach), Segundo Entrenador / Entrenador Ayudante (assistant coach). Null or '' in many rows |
| `is_license_active` | Boolean | True when the licence is active in the current season. |
| `licensing` | String | Licence code (for example EUR, JFL); meaning unverified. |
| `birth_date` | String | Birth date (the API sends DD-MM-YYYY). |
| `birth_place` | String | Birth place (town or city) as sent. |
| `birth_country` | String | Birth country as sent, a Spanish name. |
| `birth_country_iso` | String | ISO 3166-1 alpha-2 code of the birth country; null for an unseen name. |
| `nationality` | String | Nationality as sent, a Spanish country name. |
| `nationality_iso` | String | ISO 3166-1 alpha-2 code of nationality; null for an unseen name. |
| `height_m` | Float64 | Height in metres (the API sends '2,20', a decimal comma). |
| `player_number` | Int64 | Current shirt number (integer). |
| `team_id` | Int64 | Current team's season-team entry id. |
| `club_id` | Int64 | Current team's stable club id. |
| `team` | String | Current team name for the edition. |
| `team_abbr` | String | Current team abbreviation. |
| `edition_id` | Int64 | Season id the player record was read for (START year - 1935). |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `instagram` | String | Instagram handle; null when none. |
| `twitter` | String | X / Twitter handle; null when none. |
| `facebook` | String | Facebook handle; null when none. |
| `youtube` | String | YouTube handle; null when none. |
| `tiktok` | String | TikTok handle; null when none. |
| `pronunciation_url` | String | URL of an audio clip of the name; null when none. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_player_data-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_player_data-example}

```python
acb_player_data(player_id=20209407)
```

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
| `player_id` | Int64 | ACB licence number asked for (the body names the player only in the totals block). |
| `team_id` | Int64 | Season-team entry the row covers (the API's teamEditionId). |
| `competition_id` | Int64 | Competition of the row. |
| `edition_id` | Int64 | Season id of the row (START year - 1935). |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `edition_year` | String | Season label as sent ('2026-2027'). |
| `club_id` | Int64 | Stable club id of the team. |
| `team` | String | Team name that season. |
| `team_abbr` | String | Team abbreviation that season. |
| `total_num_games` | Int64 | Season total of games played. |
| `total_time_played` | Int64 | Season total of time on court in seconds. |
| `total_starter` | Int64 | Season total of games started. |
| `total_points` | Int64 | Season total of points. |
| `total_max_points` | Int64 | Season total of most points in a single game. |
| `total_three_pointers_made` | Int64 | Season total of three-point field goals made. |
| `total_three_pointers_attempted` | Int64 | Season total of three-point field goals attempted. |
| `total_three_pointers_percentage` | Float64 | Season total of three-point percentage (0-100). |
| `total_two_pointers_made` | Int64 | Season total of two-point field goals made. |
| `total_two_pointers_attempted` | Int64 | Season total of two-point field goals attempted. |
| `total_two_pointers_percentage` | Float64 | Season total of two-point percentage (0-100). |
| `total_free_throws_made` | Int64 | Season total of free throws made. |
| `total_free_throws_attempted` | Int64 | Season total of free throws attempted. |
| `total_free_throws_percentage` | Float64 | Season total of free-throw percentage (0-100). |
| `total_rebounds` | Int64 | Season total of total rebounds. |
| `total_offensive_rebounds` | Int64 | Season total of offensive rebounds. |
| `total_defensive_rebounds` | Int64 | Season total of defensive rebounds. |
| `total_assists` | Int64 | Season total of assists. |
| `total_steals` | Int64 | Season total of steals. |
| `total_turnovers` | Int64 | Season total of turnovers. |
| `total_blocks` | Int64 | Season total of blocks made. |
| `total_shots_rejected` | Int64 | Season total of own shots blocked. |
| `total_dunks` | Int64 | Season total of dunks. |
| `total_personal_fouls` | Int64 | Season total of personal fouls committed. |
| `total_fouls_received` | Int64 | Season total of fouls drawn. |
| `total_rating` | Int64 | Season total of ACB rating (valoracion). |
| `total_plus_minus` | Int64 | Season total of plus/minus. |
| `total_wins` | Int64 | Season total of team wins in the counted games (meaning unverified). |
| `total_losses` | Int64 | Season total of team losses in the counted games (meaning unverified). |
| `avg_num_games` | Float64 | Per-game average of games played. |
| `avg_time_played` | Float64 | Per-game average of time on court in seconds. |
| `avg_starter` | Float64 | Per-game average of games started. |
| `avg_points` | Float64 | Per-game average of points. |
| `avg_max_points` | Float64 | Per-game average of most points in a single game. |
| `avg_three_pointers_made` | Float64 | Per-game average of three-point field goals made. |
| `avg_three_pointers_attempted` | Float64 | Per-game average of three-point field goals attempted. |
| `avg_three_pointers_percentage` | Float64 | Per-game average of three-point percentage (0-100). |
| `avg_two_pointers_made` | Float64 | Per-game average of two-point field goals made. |
| `avg_two_pointers_attempted` | Float64 | Per-game average of two-point field goals attempted. |
| `avg_two_pointers_percentage` | Float64 | Per-game average of two-point percentage (0-100). |
| `avg_free_throws_made` | Float64 | Per-game average of free throws made. |
| `avg_free_throws_attempted` | Float64 | Per-game average of free throws attempted. |
| `avg_free_throws_percentage` | Float64 | Per-game average of free-throw percentage (0-100). |
| `avg_rebounds` | Float64 | Per-game average of total rebounds. |
| `avg_offensive_rebounds` | Float64 | Per-game average of offensive rebounds. |
| `avg_defensive_rebounds` | Float64 | Per-game average of defensive rebounds. |
| `avg_assists` | Float64 | Per-game average of assists. |
| `avg_steals` | Float64 | Per-game average of steals. |
| `avg_turnovers` | Float64 | Per-game average of turnovers. |
| `avg_blocks` | Float64 | Per-game average of blocks made. |
| `avg_shots_rejected` | Float64 | Per-game average of own shots blocked. |
| `avg_dunks` | Float64 | Per-game average of dunks. |
| `avg_personal_fouls` | Float64 | Per-game average of personal fouls committed. |
| `avg_fouls_received` | Float64 | Per-game average of fouls drawn. |
| `avg_rating` | Float64 | Per-game average of ACB rating (valoracion). |
| `avg_plus_minus` | Float64 | Per-game average of plus/minus. |
| `avg_wins` | Float64 | Per-game average of team wins in the counted games (meaning unverified). |
| `avg_losses` | Float64 | Per-game average of team losses in the counted games (meaning unverified). |
| `per40_num_games` | Float64 | Per-40-minute rate of games played. |
| `per40_time_played` | Float64 | Always null: the per-40 block carries no time played. |
| `per40_starter` | Float64 | Per-40-minute rate of games started. |
| `per40_points` | Float64 | Per-40-minute rate of points. |
| `per40_max_points` | Float64 | Per-40-minute rate of most points in a single game. |
| `per40_three_pointers_made` | Float64 | Per-40-minute rate of three-point field goals made. |
| `per40_three_pointers_attempted` | Float64 | Per-40-minute rate of three-point field goals attempted. |
| `per40_three_pointers_percentage` | Float64 | Per-40-minute rate of three-point percentage (0-100). |
| `per40_two_pointers_made` | Float64 | Per-40-minute rate of two-point field goals made. |
| `per40_two_pointers_attempted` | Float64 | Per-40-minute rate of two-point field goals attempted. |
| `per40_two_pointers_percentage` | Float64 | Per-40-minute rate of two-point percentage (0-100). |
| `per40_free_throws_made` | Float64 | Per-40-minute rate of free throws made. |
| `per40_free_throws_attempted` | Float64 | Per-40-minute rate of free throws attempted. |
| `per40_free_throws_percentage` | Float64 | Per-40-minute rate of free-throw percentage (0-100). |
| `per40_rebounds` | Float64 | Per-40-minute rate of total rebounds. |
| `per40_offensive_rebounds` | Float64 | Per-40-minute rate of offensive rebounds. |
| `per40_defensive_rebounds` | Float64 | Per-40-minute rate of defensive rebounds. |
| `per40_assists` | Float64 | Per-40-minute rate of assists. |
| `per40_steals` | Float64 | Per-40-minute rate of steals. |
| `per40_turnovers` | Float64 | Per-40-minute rate of turnovers. |
| `per40_blocks` | Float64 | Per-40-minute rate of blocks made. |
| `per40_shots_rejected` | Float64 | Per-40-minute rate of own shots blocked. |
| `per40_dunks` | Float64 | Per-40-minute rate of dunks. |
| `per40_personal_fouls` | Float64 | Per-40-minute rate of personal fouls committed. |
| `per40_fouls_received` | Float64 | Per-40-minute rate of fouls drawn. |
| `per40_rating` | Float64 | Per-40-minute rate of ACB rating (valoracion). |
| `per40_plus_minus` | Float64 | Per-40-minute rate of plus/minus. |
| `per40_wins` | Float64 | Per-40-minute rate of team wins in the counted games (meaning unverified). |
| `per40_losses` | Float64 | Per-40-minute rate of team losses in the counted games (meaning unverified). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_player_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_player_stats-example}

```python
acb_player_stats(player_id=20209407)
```

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
| `player_id` | Int64 | ACB licence number of the player. |
| `player_name` | String | Display (sporting) name ('Edy Tavares') |
| `first_name` | String | Legal first name(s) |
| `last_name` | String | Legal last name(s) |
| `shirt_number` | String | Shirt number, a string ('' or null when unknown) |
| `game_role` | String | Position or staff role, in Spanish: Base (point guard), Escolta (shooting guard), Alero (small forward), Ala-pivot (power forward), Pivot (centre); Entrenador (head coach), Segundo Entrenador / Entrenador Ayudante (assistant coach). Null or '' in many rows |
| `total_num_games` | Int64 | Season total of games played. |
| `total_time_played` | Int64 | Season total of time on court in seconds. |
| `total_starter` | Int64 | Season total of games started. |
| `total_points` | Int64 | Season total of points. |
| `total_max_points` | Int64 | Season total of most points in a single game. |
| `total_three_pointers_made` | Int64 | Season total of three-point field goals made. |
| `total_three_pointers_attempted` | Int64 | Season total of three-point field goals attempted. |
| `total_three_pointers_percentage` | Float64 | Season total of three-point percentage (0-100). |
| `total_two_pointers_made` | Int64 | Season total of two-point field goals made. |
| `total_two_pointers_attempted` | Int64 | Season total of two-point field goals attempted. |
| `total_two_pointers_percentage` | Float64 | Season total of two-point percentage (0-100). |
| `total_free_throws_made` | Int64 | Season total of free throws made. |
| `total_free_throws_attempted` | Int64 | Season total of free throws attempted. |
| `total_free_throws_percentage` | Float64 | Season total of free-throw percentage (0-100). |
| `total_rebounds` | Int64 | Season total of total rebounds. |
| `total_offensive_rebounds` | Int64 | Season total of offensive rebounds. |
| `total_defensive_rebounds` | Int64 | Season total of defensive rebounds. |
| `total_assists` | Int64 | Season total of assists. |
| `total_steals` | Int64 | Season total of steals. |
| `total_turnovers` | Int64 | Season total of turnovers. |
| `total_blocks` | Int64 | Season total of blocks made. |
| `total_shots_rejected` | Int64 | Season total of own shots blocked. |
| `total_dunks` | Int64 | Season total of dunks. |
| `total_personal_fouls` | Int64 | Season total of personal fouls committed. |
| `total_fouls_received` | Int64 | Season total of fouls drawn. |
| `total_rating` | Int64 | Season total of ACB rating (valoracion). |
| `total_plus_minus` | Int64 | Season total of plus/minus. |
| `total_wins` | Int64 | Season total of team wins in the counted games (meaning unverified). |
| `total_losses` | Int64 | Season total of team losses in the counted games (meaning unverified). |
| `avg_num_games` | Float64 | Per-game average of games played. |
| `avg_time_played` | Float64 | Per-game average of time on court in seconds. |
| `avg_starter` | Float64 | Per-game average of games started. |
| `avg_points` | Float64 | Per-game average of points. |
| `avg_max_points` | Float64 | Per-game average of most points in a single game. |
| `avg_three_pointers_made` | Float64 | Per-game average of three-point field goals made. |
| `avg_three_pointers_attempted` | Float64 | Per-game average of three-point field goals attempted. |
| `avg_three_pointers_percentage` | Float64 | Per-game average of three-point percentage (0-100). |
| `avg_two_pointers_made` | Float64 | Per-game average of two-point field goals made. |
| `avg_two_pointers_attempted` | Float64 | Per-game average of two-point field goals attempted. |
| `avg_two_pointers_percentage` | Float64 | Per-game average of two-point percentage (0-100). |
| `avg_free_throws_made` | Float64 | Per-game average of free throws made. |
| `avg_free_throws_attempted` | Float64 | Per-game average of free throws attempted. |
| `avg_free_throws_percentage` | Float64 | Per-game average of free-throw percentage (0-100). |
| `avg_rebounds` | Float64 | Per-game average of total rebounds. |
| `avg_offensive_rebounds` | Float64 | Per-game average of offensive rebounds. |
| `avg_defensive_rebounds` | Float64 | Per-game average of defensive rebounds. |
| `avg_assists` | Float64 | Per-game average of assists. |
| `avg_steals` | Float64 | Per-game average of steals. |
| `avg_turnovers` | Float64 | Per-game average of turnovers. |
| `avg_blocks` | Float64 | Per-game average of blocks made. |
| `avg_shots_rejected` | Float64 | Per-game average of own shots blocked. |
| `avg_dunks` | Float64 | Per-game average of dunks. |
| `avg_personal_fouls` | Float64 | Per-game average of personal fouls committed. |
| `avg_fouls_received` | Float64 | Per-game average of fouls drawn. |
| `avg_rating` | Float64 | Per-game average of ACB rating (valoracion). |
| `avg_plus_minus` | Float64 | Per-game average of plus/minus. |
| `avg_wins` | Float64 | Per-game average of team wins in the counted games (meaning unverified). |
| `avg_losses` | Float64 | Per-game average of team losses in the counted games (meaning unverified). |
| `per40_num_games` | Float64 | Per-40-minute rate of games played. |
| `per40_time_played` | Float64 | Always null: the per-40 block carries no time played. |
| `per40_starter` | Float64 | Per-40-minute rate of games started. |
| `per40_points` | Float64 | Per-40-minute rate of points. |
| `per40_max_points` | Float64 | Per-40-minute rate of most points in a single game. |
| `per40_three_pointers_made` | Float64 | Per-40-minute rate of three-point field goals made. |
| `per40_three_pointers_attempted` | Float64 | Per-40-minute rate of three-point field goals attempted. |
| `per40_three_pointers_percentage` | Float64 | Per-40-minute rate of three-point percentage (0-100). |
| `per40_two_pointers_made` | Float64 | Per-40-minute rate of two-point field goals made. |
| `per40_two_pointers_attempted` | Float64 | Per-40-minute rate of two-point field goals attempted. |
| `per40_two_pointers_percentage` | Float64 | Per-40-minute rate of two-point percentage (0-100). |
| `per40_free_throws_made` | Float64 | Per-40-minute rate of free throws made. |
| `per40_free_throws_attempted` | Float64 | Per-40-minute rate of free throws attempted. |
| `per40_free_throws_percentage` | Float64 | Per-40-minute rate of free-throw percentage (0-100). |
| `per40_rebounds` | Float64 | Per-40-minute rate of total rebounds. |
| `per40_offensive_rebounds` | Float64 | Per-40-minute rate of offensive rebounds. |
| `per40_defensive_rebounds` | Float64 | Per-40-minute rate of defensive rebounds. |
| `per40_assists` | Float64 | Per-40-minute rate of assists. |
| `per40_steals` | Float64 | Per-40-minute rate of steals. |
| `per40_turnovers` | Float64 | Per-40-minute rate of turnovers. |
| `per40_blocks` | Float64 | Per-40-minute rate of blocks made. |
| `per40_shots_rejected` | Float64 | Per-40-minute rate of own shots blocked. |
| `per40_dunks` | Float64 | Per-40-minute rate of dunks. |
| `per40_personal_fouls` | Float64 | Per-40-minute rate of personal fouls committed. |
| `per40_fouls_received` | Float64 | Per-40-minute rate of fouls drawn. |
| `per40_rating` | Float64 | Per-40-minute rate of ACB rating (valoracion). |
| `per40_plus_minus` | Float64 | Per-40-minute rate of plus/minus. |
| `per40_wins` | Float64 | Per-40-minute rate of team wins in the counted games (meaning unverified). |
| `per40_losses` | Float64 | Per-40-minute rate of team losses in the counted games (meaning unverified). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_player_totals-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_player_totals-example}

```python
acb_player_totals(player_id=20209407)
```

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
| `player_id` | Int64 | ACB licence number asked for. |
| `match_id` | Int64 | ACB match id of the game. |
| `round_type` | String | Phase code of the game (LR regular season, PO playoff). |
| `round_number` | Int64 | Round number within the phase. |
| `date` | String | Tip-off instant (UTC). |
| `home_team_id` | Int64 | Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `home_club_id` | Int64 | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `home_team` | String | Team name that season (sponsor names change) |
| `home_team_abbr` | String | Three-letter abbreviation (BAR, RMB) |
| `away_team_id` | Int64 | Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `away_club_id` | Int64 | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `away_team` | String | Team name that season (sponsor names change) |
| `away_team_abbr` | String | Three-letter abbreviation (BAR, RMB) |
| `edition_id` | Int64 | Season id read from the home team entry (START year - 1935). |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `is_selected_local` | Boolean | True when the player's team played at home. |
| `has_won` | Boolean | True when the player's team won the game. |
| `play_time` | String | Time on court as 'mm:ss'. |
| `seconds_played` | Int64 | Time on court in whole seconds (play_time parsed). |
| `points` | Int64 | Points (player game-log row) |
| `three_pointers_made` | Int64 | Three-point field goals made |
| `three_pointers_attempted` | Int64 | Three-point field goals attempted |
| `three_pointers_percentage` | Float64 | Three-point percentage, 0-100 with one decimal; 0 when there was no attempt |
| `two_pointers_made` | Int64 | Two-point field goals made |
| `two_pointers_attempted` | Int64 | Two-point field goals attempted |
| `two_pointers_percentage` | Float64 | Two-point percentage, 0-100 with one decimal; 0 when there was no attempt |
| `free_throws_made` | Int64 | Free throws the player made in that match (a count, not a season total). |
| `free_throws_attempted` | Int64 | Free throws the player attempted in that match (a count, not a season total). |
| `free_throws_percentage` | Float64 | Free-throw percentage, 0-100 with one decimal; 0 when there was no attempt |
| `rebounds` | Int64 | Total rebounds in the game. |
| `offensive_rebounds` | Int64 | Offensive rebounds the player took in that match (a count; part of the total rebounds). |
| `defensive_rebounds` | Int64 | Defensive rebounds the player took in that match (a count; part of the total rebounds). |
| `assists` | Int64 | Assists (player game-log row) |
| `steals` | Int64 | Steals (player game-log row) |
| `turnovers` | Int64 | Turnovers (player game-log row) |
| `blocks` | Int64 | Shots blocked (player game-log row) |
| `shots_rejected` | Int64 | Shots of the player that were blocked. |
| `dunks` | Int64 | Dunks (player game-log row) |
| `personal_fouls` | Int64 | Fouls committed |
| `fouls_received` | Int64 | Fouls drawn by the player in the game. |
| `rating` | Int64 | ACB rating (valoracion) of the game. |
| `plus_minus` | Int64 | Point differential while on court (+/-) |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_player_matches-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_player_matches-example}

```python
acb_player_matches(player_id=20209407, season=2027)
```
