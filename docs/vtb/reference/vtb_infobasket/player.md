# VTB — VTB United League Infobasket (Russia) — Player

> VTB — VTB United League Infobasket (Russia) — Player — function reference in sdv-py, the SportsDataverse Python package.

## vtb_infobasket_player

Player profile: names, birth date, height, weight, position.

**Endpoint URL:** `GET https://org.infobasket.su/Widget/PlayerPage/{person_id}`

**Valid URL:** [https://org.infobasket.su/Widget/PlayerPage/423011?format=json&tag=vtb](https://org.infobasket.su/Widget/PlayerPage/423011?format=json&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_id` | `person_id` |  | `Y` |  | Infobasket PersonID (person_id in a box score or roster): one person across clubs and seasons, e.g. 423011. |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_player-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `person_id` | Int64 | Infobasket PersonID: one person across clubs and seasons; players, coaches and officials share the space. |
| `person_last_name_ru` | String | Last name, Russian. |
| `person_first_name_ru` | String | First name, Russian. |
| `person_last_name_en` | String | Last name, English. |
| `person_first_name_en` | String | First name, English. |
| `person_gender` | Int64 | Person gender: 1 on every sampled (male) person. |
| `birth_date` | String | Birth date (ISO 8601 date) of a player: public bio, kept; coaches' and officials' birth data is never output. |
| `person_height` | Int64 | Height in centimetres. |
| `person_weight` | Int64 | Weight in kilograms. |
| `person_status` | Int64 | Person record status (1 active, 0 otherwise); meaning unverified. |
| `player_position` | Int64 | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `player_hand` | String | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `is_naturalized` | Boolean | True for naturalised players. |
| `position_id` | Int64 | Position id of the player (PosID), with its names in position_name_ru / position_name_en. |
| `position_name_ru` | String | Position name in Russian. |
| `position_name_en` | String | Position name in English. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_player-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_player-example}

```python
vtb_infobasket_player(person_id=423011)
```

## vtb_infobasket_player_season_stats

A player's season-by-season totals in the competition tag.

**Endpoint URL:** `GET https://org.infobasket.su/Widget/PlayerSeasonStats/{person_id}`

**Valid URL:** [https://org.infobasket.su/Widget/PlayerSeasonStats/423011?format=json&tag=vtb](https://org.infobasket.su/Widget/PlayerSeasonStats/423011?format=json&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_id` | `person_id` |  | `Y` |  | Infobasket PersonID (person_id in a box score or roster): one person across clubs and seasons, e.g. 423011. |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_player_season_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `person_id` | Int64 | Infobasket person id (PersonID): one person (player, coach or official) across clubs and seasons. Joins to /Widget/PlayerPage/{personId} (players) and /Widget/CoachPage/{personId} (coaches). |
| `season` | Int64 | Season END year (2026 = 2025-26). The wire sends it as Season.CompID, a third meaning of CompID. |
| `season_name_ru` | String | Season name in Russian ('Сезон 2025/2026'). |
| `season_name_en` | String | Season name in English ('Season 2025/2026'). |
| `team_short_name_ru` | String | The the team's short name in Russian (as the competition names it). |
| `team_short_name_en` | String | The the team's short name in English (as the competition names it). |
| `team_name_ru` | String | The the team's name in Russian (as the competition names it). |
| `team_name_en` | String | The the team's name in English (as the competition names it). |
| `team_region_name_ru` | String | The the team's region or city in Russian (as the competition names it). |
| `team_region_name_en` | String | The the team's region or city in English (as the competition names it). |
| `team_id` | Int64 | Infobasket team id (TeamID): one club's entry in a competition family (Lokomotiv Kuban = 2650). Joins to /Widget/Team*/{teamId} and equals the v2 API `teamId`. |
| `game_count` | Int64 | Games in the totals. |
| `in_game_count` | Int64 | Games actually played (on the court). |
| `start_count` | Int64 | Games the person started. |
| `points` | Int64 | Points. The wire's null is 0 here (null counts are filled). |
| `shot1` | Int64 | Free throws attempted. The wire's null is 0 here (null counts are filled). |
| `goal1` | Int64 | Free throws made. The wire's null is 0 here (null counts are filled). |
| `shot2` | Int64 | 2-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal2` | Int64 | 2-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot3` | Int64 | 3-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal3` | Int64 | 3-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot1_percent` | Float64 | Free-throw percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot2_percent` | Float64 | 2-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot3_percent` | Float64 | 3-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `assist` | Int64 | Assists. The wire's null is 0 here (null counts are filled). |
| `blocks` | Int64 | Blocked shots. The wire's null is 0 here (null counts are filled). |
| `def_rebound` | Int64 | Defensive rebounds. The wire's null is 0 here (null counts are filled). |
| `off_rebound` | Int64 | Offensive rebounds. The wire's null is 0 here (null counts are filled). |
| `rebound` | Int64 | Total rebounds. The wire's null is 0 here (null counts are filled). |
| `steal` | Int64 | Steals. The wire's null is 0 here (null counts are filled). |
| `turnover` | Int64 | Turnovers. The wire's null is 0 here (null counts are filled). |
| `foul` | Int64 | Personal fouls committed. The wire's null is 0 here (null counts are filled). |
| `opponent_foul` | Int64 | Fouls drawn. The wire's null is 0 here (null counts are filled). |
| `avg_points` | Float64 | Per-game average of: points; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_assist` | Float64 | Per-game average of: assists; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_blocks` | Float64 | Per-game average of: blocked shots; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_def_rebound` | Float64 | Per-game average of: defensive rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_off_rebound` | Float64 | Per-game average of: offensive rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_rebound` | Float64 | Per-game average of: total rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_steal` | Float64 | Per-game average of: steals; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_turnover` | Float64 | Per-game average of: turnovers; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_foul` | Float64 | Per-game average of: personal fouls committed; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_opponent_foul` | Float64 | Per-game average of: fouls drawn; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_plus_minus` | Float64 | Per-game average of: plus-minus: point differential while on the court; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `seconds` | Int64 | Playing time in seconds. |
| `played_time` | String | Playing time as 'minutes:seconds' (team rows sum the five players: '200:00' = 40 minutes x 5). |
| `avg_played_time` | String | Average playing time per game as 'minutes:seconds'. |
| `plus_minus` | Int64 | Plus-minus: point differential while on the court. Null where the host sends none. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_player_season_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_player_season_stats-example}

```python
vtb_infobasket_player_season_stats(person_id=423011)
```

## vtb_infobasket_player_career_stats

A player's career totals in the competition tag.

**Endpoint URL:** `GET https://org.infobasket.su/Widget/PlayerSeasonStats/{person_id}`

**Valid URL:** [https://org.infobasket.su/Widget/PlayerSeasonStats/423011?format=json&tag=vtb](https://org.infobasket.su/Widget/PlayerSeasonStats/423011?format=json&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_id` | `person_id` |  | `Y` |  | Infobasket PersonID (person_id in a box score or roster): one person across clubs and seasons, e.g. 423011. |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_player_career_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `person_id` | Int64 | Infobasket person id (PersonID): one person (player, coach or official) across clubs and seasons. Joins to /Widget/PlayerPage/{personId} (players) and /Widget/CoachPage/{personId} (coaches). The scope check compares it with the request. |
| `game_count` | Int64 | Games in the totals. |
| `in_game_count` | Int64 | Games actually played (on the court). |
| `start_count` | Int64 | Games the person started. |
| `has_more_seasons` | Boolean | True when the host holds more seasons than listed (False in every sample). |
| `points` | Int64 | Points. The wire's null is 0 here (null counts are filled). |
| `shot1` | Int64 | Free throws attempted. The wire's null is 0 here (null counts are filled). |
| `goal1` | Int64 | Free throws made. The wire's null is 0 here (null counts are filled). |
| `shot2` | Int64 | 2-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal2` | Int64 | 2-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot3` | Int64 | 3-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal3` | Int64 | 3-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot1_percent` | Float64 | Free-throw percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot2_percent` | Float64 | 2-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot3_percent` | Float64 | 3-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `assist` | Int64 | Assists. The wire's null is 0 here (null counts are filled). |
| `blocks` | Int64 | Blocked shots. The wire's null is 0 here (null counts are filled). |
| `def_rebound` | Int64 | Defensive rebounds. The wire's null is 0 here (null counts are filled). |
| `off_rebound` | Int64 | Offensive rebounds. The wire's null is 0 here (null counts are filled). |
| `rebound` | Int64 | Total rebounds. The wire's null is 0 here (null counts are filled). |
| `steal` | Int64 | Steals. The wire's null is 0 here (null counts are filled). |
| `turnover` | Int64 | Turnovers. The wire's null is 0 here (null counts are filled). |
| `foul` | Int64 | Personal fouls committed. The wire's null is 0 here (null counts are filled). |
| `opponent_foul` | Int64 | Fouls drawn. The wire's null is 0 here (null counts are filled). |
| `avg_points` | Float64 | Per-game average of: points; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_assist` | Float64 | Per-game average of: assists; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_blocks` | Float64 | Per-game average of: blocked shots; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_def_rebound` | Float64 | Per-game average of: defensive rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_off_rebound` | Float64 | Per-game average of: offensive rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_rebound` | Float64 | Per-game average of: total rebounds; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_steal` | Float64 | Per-game average of: steals; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_turnover` | Float64 | Per-game average of: turnovers; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_foul` | Float64 | Per-game average of: personal fouls committed; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_opponent_foul` | Float64 | Per-game average of: fouls drawn; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `avg_plus_minus` | Float64 | Per-game average of: plus-minus: point differential while on the court; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `seconds` | Int64 | Playing time in seconds. |
| `played_time` | String | Playing time as 'minutes:seconds' (team rows sum the five players: '200:00' = 40 minutes x 5). |
| `avg_played_time` | String | Average playing time per game as 'minutes:seconds'. |
| `plus_minus` | Int64 | Plus-minus: point differential while on the court. Null where the host sends none. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_player_career_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_player_career_stats-example}

```python
vtb_infobasket_player_career_stats(person_id=423011)
```

## vtb_infobasket_player_game_log

A player's game log for one season and one team, one row per game.

**Endpoint URL:** `GET https://org.infobasket.su/Widget/PlayerStats/{person_id}`

**Valid URL:** [https://org.infobasket.su/Widget/PlayerStats/423011?format=json&season=2026&team=2650&filter=1&tag=vtb](https://org.infobasket.su/Widget/PlayerStats/423011?format=json&season=2026&team=2650&filter=1&tag=vtb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_id` | `person_id` |  | `Y` |  | Infobasket PersonID (person_id in a box score or roster): one person across clubs and seasons, e.g. 423011. |
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with vtb_infobasket_seasons (one extra request per tag, cached). Required. |
| `team` | `team` |  | `Y` |  | Infobasket TeamID (team_id in the calendar, a box score or the v2 matches), e.g. 2650 (Lokomotiv Kuban). Required: the body is the player's games for that club. |
| `filter` | `filter` |  |  | `Y` | Site filter code; the widget always sends 1 (meaning unverified). |
| `tag` | `tag` |  |  | `Y` | Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season table is read for this tag. |

### Returns {#vtb_infobasket_player_game_log-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `person_id` | Int64 | 0 on every game row (the player is the body's PersonID). |
| `game_id` | Int64 | Infobasket game id (GameID); equals the v2 API `matchId`. Joins to /Widget/GetOnline/{gameId} and /Widget/GamePage/{gameId}. |
| `game_number` | String | Game number within the competition, e.g. 'RS-002' (RS = regular season); playoff games use the series code. |
| `game_status` | Int64 | Game status in the game-record code set: 21 completed. |
| `team_id` | Int64 | Infobasket TeamID the player played for in that game. |
| `team_a_id` | Int64 | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `team_b_id` | Int64 | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `score_a` | Int64 | Final points of team A (listed first). |
| `score_b` | Int64 | Final points of team B (listed second). |
| `team_a_short_name_ru` | String | The team A (the home side in the calendar)'s short name in Russian (as the competition names it). |
| `team_a_short_name_en` | String | The team A (the home side in the calendar)'s short name in English (as the competition names it). |
| `team_a_name_ru` | String | The team A (the home side in the calendar)'s name in Russian (as the competition names it). |
| `team_a_name_en` | String | The team A (the home side in the calendar)'s name in English (as the competition names it). |
| `team_a_region_name_ru` | String | The team A (the home side in the calendar)'s region or city in Russian (as the competition names it). |
| `team_a_region_name_en` | String | The team A (the home side in the calendar)'s region or city in English (as the competition names it). |
| `team_b_short_name_ru` | String | The team B (the away side)'s short name in Russian (as the competition names it). |
| `team_b_short_name_en` | String | The team B (the away side)'s short name in English (as the competition names it). |
| `team_b_name_ru` | String | The team B (the away side)'s name in Russian (as the competition names it). |
| `team_b_name_en` | String | The team B (the away side)'s name in English (as the competition names it). |
| `team_b_region_name_ru` | String | The team B (the away side)'s region or city in Russian (as the competition names it). |
| `team_b_region_name_en` | String | The team B (the away side)'s region or city in English (as the competition names it). |
| `game_date` | String | Game date as an ISO 8601 date (YYYY-MM-DD); the wire sends DD.MM.YYYY. |
| `team_number` | Int64 | Side (1 or 2) the player's team had in the game. |
| `player_number` | Int64 | Shirt number as an integer (-1 when none). |
| `display_number` | String | Shirt number as displayed ('0' and '00' differ). |
| `points` | Int64 | Points. The wire's null is 0 here (null counts are filled). |
| `shot1` | Int64 | Free throws attempted. The wire's null is 0 here (null counts are filled). |
| `goal1` | Int64 | Free throws made. The wire's null is 0 here (null counts are filled). |
| `shot2` | Int64 | 2-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal2` | Int64 | 2-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot3` | Int64 | 3-point field goals attempted. The wire's null is 0 here (null counts are filled). |
| `goal3` | Int64 | 3-point field goals made. The wire's null is 0 here (null counts are filled). |
| `shot23` | Int64 | Field goals (2 + 3 points) attempted. The wire's null is 0 here (null counts are filled). |
| `goal23` | Int64 | Field goals (2 + 3 points) made. The wire's null is 0 here (null counts are filled). |
| `shot1_percent` | Float64 | Free-throw percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot2_percent` | Float64 | 2-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot3_percent` | Float64 | 3-point percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `shot23_percent` | Float64 | Field-goal percentage (0-100); parsed to a number (the wire sends a comma-decimal string, null when there were no attempts). |
| `assist` | Int64 | Assists. The wire's null is 0 here (null counts are filled). |
| `blocks` | Int64 | Unobserved: null in every sample, so its type and meaning are unconfirmed. |
| `def_rebound` | Int64 | Defensive rebounds. The wire's null is 0 here (null counts are filled). |
| `off_rebound` | Int64 | Offensive rebounds. The wire's null is 0 here (null counts are filled). |
| `rebound` | Int64 | Total rebounds. The wire's null is 0 here (null counts are filled). |
| `steal` | Int64 | Steals. The wire's null is 0 here (null counts are filled). |
| `turnover` | Int64 | Turnovers. The wire's null is 0 here (null counts are filled). |
| `foul` | Int64 | Personal fouls committed. The wire's null is 0 here (null counts are filled). |
| `opponent_foul` | Int64 | Fouls drawn. The wire's null is 0 here (null counts are filled). |
| `seconds` | Int64 | Playing time in seconds. |
| `played_time` | String | Playing time as 'minutes:seconds' (team rows sum the five players: '200:00' = 40 minutes x 5). |
| `plus_minus` | Int64 | Plus-minus: point differential while on the court. Null where the host sends none. |
| `is_start` | Boolean | True for a starter. |
| `start_mark` | String | '+' for a starter, else empty. |
| `kpi` | Int64 | Efficiency index (KPI, the platform's 'КПИ'); formula unverified. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#vtb_infobasket_player_game_log-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host has nothing for the request (an empty list, a league the API does not know, a season with no rows). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: an error envelope or string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, person, team, competition, league or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A season the competition tag does not have, or an argument that is not an integer id. |

### Example {#vtb_infobasket_player_game_log-example}

```python
vtb_infobasket_player_game_log(person_id=423011, season=2026, team=2650)
```
