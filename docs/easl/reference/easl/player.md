# EASL — EASL sync server (easl.joshricha.com, third-party over Sportradar DataCore) — Player

> EASL — EASL sync server (easl.joshricha.com, third-party over Sportradar DataCore) — Player — function reference in sdv-py, the SportsDataverse Python package.

## easl_player_box

Player box score of a finished game, one row per rostered player (non-players have null counting stats).

**Endpoint URL:** `GET https://easl.joshricha.com/api/v1/matches/{fixture_id}/boxscore`

**Valid URL:** [https://easl.joshricha.com/api/v1/matches/10af8fd4-106e-11f1-960b-5798087c7aa3/boxscore](https://easl.joshricha.com/api/v1/matches/10af8fd4-106e-11f1-960b-5798087c7aa3/boxscore)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `fixture_id` | `fixture_id` |  | `Y` |  | DataCore fixture (game) UUID v1, e.g. '10af8fd4-106e-11f1-960b-5798087c7aa3' (the 2025-26 EASL Championship): the 'id' of easl_schedule rows. Only finished (CONFIRMED) games are served; a malformed id is a ValueError. |

### Returns {#easl_player_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `fixture_id` | character | DataCore fixture (game) UUID v1 of the box row. |
| `team_id` | character | The player's team: DataCore team (entity) UUID v1 (the wire's entityId). |
| `team_name` | character | The player's team display name, joined from the body's teams array. |
| `team_name_native` | character | The player's team name in native script, joined from the body's teams array; blank for most clubs. |
| `player_id` | character | playerId: DataCore person UUID v1 (key space: EASL persons, the same id as the box score's `personId`; joins /players, leaders and play-by-play players). Not a team id. |
| `player_name` | character | Player display name in 'FAMILY Given' form with the family name upper-cased (OSAKABE Taiki), in Latin script. Absent on some rows. |
| `player_name_latin` | character | Alternative name: equal to nameFullLocal for 257 of 552 players, otherwise null; never non-Latin in the observed data. Absent on some rows. |
| `bib` | character | Jersey number as a string ('00' and '0' are distinct). |
| `position` | character | Playing position: G, F, C, PG, SG, SF, PF or GF; null on some rows. |
| `starter` | logical | True for a starter (five per team). |
| `is_player` | logical | True on every row (team officials are never listed). |
| `is_team_official` | logical | False on every row. |
| `played` | logical | True when the player has playing time (minutes present); False for the rostered players who did not play (their counting stats are null). |
| `minutes_played` | numeric | Playing time in minutes (decimal), parsed from the ISO 8601 minutes statistic (PT35M11S = 35.1833). |
| `assists` | integer | Assists: passes that directly lead to a made field goal. Null on some rows. |
| `assists_defensive` | integer | Defensive assists (DataCore column; meaning unverified). Null on some rows. |
| `assists_turnover_ratio` | numeric | Assists divided by turnovers (a ratio, not a percentage). Null on some rows. |
| `blocks` | integer | Shots blocked (made by this player or team). Null on some rows. |
| `blocks_received` | integer | The team's or player's own shots that were blocked by an opponent. Null on some rows. |
| `double_double` | integer | Double-double flag: 1 when exactly two of points, rebounds, assists, steals, blocks reached 10 in the game, else 0 (a triple-double is counted in tripleDouble instead). In a season line it is a count of such games. |
| `dunks` | integer | Dunks made. Null on some rows. |
| `efficiency` | integer | NBA-style efficiency: points + rebounds + assists + steals + blocks - missed field goals - missed free throws - turnovers (verified on all 2,143 played player-games). |
| `efficiency_custom` | integer | Custom efficiency slot (DataCore column; meaning unverified). Null on some rows. |
| `field_goals_attempted` | integer | Field goals attempted (2- and 3-point shots; free throws excluded). Null on some rows. |
| `field_goals_effective_percentage` | numeric | Effective field-goal percentage on a 0-100 scale: (FGM + 0.5 x 3PM) / FGA x 100; can exceed 100 on small samples. Null on some rows. |
| `field_goals_made` | integer | Field goals made (2- and 3-point shots). Null on some rows. |
| `field_goals_percentage` | numeric | Field-goal percentage on a 0-100 scale (made / attempted x 100). Null on some rows. |
| `fouls_coach_disqualifying` | integer | Disqualifying fouls on the coach. Null on some rows. |
| `fouls_coach_technical` | integer | Technical fouls on the coach. Null on some rows. |
| `fouls_disqualifying` | integer | Disqualifying fouls. Null on some rows. |
| `fouls_disruptive` | integer | Disruptive fouls (DataCore column; filled from 2026-27). Null on some rows. |
| `fouls_drawn` | integer | Fouls drawn (committed against this player or team by an opponent). Null on some rows. |
| `fouls_flagrant` | integer | Flagrant fouls (DataCore column; filled from 2026-27). Null on some rows. |
| `fouls_offensive` | integer | Offensive fouls committed. Counted separately from foulsPersonal. Null on some rows. |
| `fouls_personal` | integer | Personal fouls committed (offensive, technical and unsportsmanlike fouls are separate columns). Null on some rows. |
| `fouls_technical` | integer | Technical fouls committed by the player or team. Null on some rows. |
| `fouls_technical_cat1` | integer | Technical fouls of category 1 (DataCore column; filled from 2026-27). Null on some rows. |
| `fouls_technical_cat2` | integer | Technical fouls of category 2 (DataCore column; filled from 2026-27). Null on some rows. |
| `fouls_total` | integer | Fouls charged in total: personal + technical + unsportsmanlike + offensive (matches 2,142 of 2,143 played player-games; the exception is unexplained). |
| `fouls_unsportsmanlike` | integer | Unsportsmanlike fouls committed. Null on some rows. |
| `free_throws_attempted` | integer | Free throws attempted. Null on some rows. |
| `free_throws_made` | integer | Free throws made: a game count on the box-score and team-total routes, a season total on the season statistics routes. |
| `free_throws_percentage` | numeric | Free-throw percentage on a 0-100 scale. Null on some rows. |
| `index_of_success` | integer | Index of success: efficiency + fouls drawn - personal fouls - technical fouls - blocks received (verified on all 2,143 played player-games). It differs from pir only in which fouls it subtracts. |
| `max_assists` | integer | Highest assists in a single game: in a game row it equals assists; in a season line it is the best game. Null on some rows. |
| `max_free_throws_made` | integer | Highest free throws made in a single game (equals freeThrowsMade in a game row). |
| `max_points` | integer | Highest points in a single game (equals points in a game row). |
| `max_points_four_made` | integer | Highest 4-point shots made in a game (DataCore column; EASL has no 4-point line; filled from 2026-27). Null on some rows. |
| `max_points_three_made` | integer | Highest 3-pointers made in a single game. Null on some rows. |
| `max_points_two_made` | integer | Highest 2-pointers made in a single game. Null on some rows. |
| `max_rebounds` | integer | Highest rebounds in a single game. Null on some rows. |
| `max_rebounds_defensive` | integer | Highest defensive rebounds in a single game. Null on some rows. |
| `max_rebounds_offensive` | integer | Highest offensive rebounds in a single game. Null on some rows. |
| `max_steals` | integer | Highest steals in a single game. Null on some rows. |
| `minus` | integer | Points the opponents scored while the player was on the court. Null on some rows. |
| `minutes` | character | Playing time as an ISO 8601 duration PT{m}M{s}S with the minutes NOT wrapped at 60 (PT27M53S; a team's is the sum of its players', PT200M for a 40-minute game, PT250M with one overtime). Null on some rows. |
| `pir` | integer | Performance index rating: efficiency + fouls drawn - total fouls - blocks received (verified on all 2,143 played player-games). |
| `plus` | integer | Points the player's team scored while he was on the court. Null on some rows. |
| `plus_minus` | integer | plus - minus: the team's point margin while the player was on the court. Null on some rows. |
| `points` | integer | Points scored: a game count on the box-score and team-total routes, a season total on the season statistics routes. |
| `points_fast_break` | integer | Points scored on fast breaks. Null on some rows. |
| `points_fast_break_attempted` | integer | Fast-break field goals attempted. Null on some rows. |
| `points_fast_break_made` | integer | Fast-break field goals made. Null on some rows. |
| `points_four_attempted` | integer | 4-point shots attempted (DataCore column; EASL has no 4-point line; filled from 2026-27). Null on some rows. |
| `points_four_made` | integer | 4-point shots made (see pointsFourAttempted). Null on some rows. |
| `points_four_percentage` | numeric | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Float64). |
| `points_from_turnover` | integer | Points scored off opponent turnovers. Null on some rows. |
| `points_in_the_paint` | integer | Points scored in the paint. Null on some rows. |
| `points_in_the_paint_attempted` | integer | Field goals attempted in the paint. Null on some rows. |
| `points_in_the_paint_made` | integer | Field goals made in the paint. Null on some rows. |
| `points_second_chance` | integer | Points scored on second-chance possessions (after an offensive rebound). Null on some rows. |
| `points_second_chance_attempted` | integer | Second-chance field goals attempted. Null on some rows. |
| `points_second_chance_made` | integer | Second-chance field goals made. Null on some rows. |
| `points_three_attempted` | integer | 3-point field goals attempted. Null on some rows. |
| `points_three_made` | integer | 3-point field goals made. Null on some rows. |
| `points_three_percentage` | numeric | 3-point percentage on a 0-100 scale. Null on some rows. |
| `points_two_attempted` | integer | 2-point field goals attempted. Null on some rows. |
| `points_two_made` | integer | 2-point field goals made. Null on some rows. |
| `points_two_percentage` | numeric | 2-point percentage on a 0-100 scale. Null on some rows. |
| `power_play_points` | integer | Power-play points (an ice-hockey column; meaning in basketball unverified). Null on some rows. |
| `power_play_points_awarded` | integer | Power-play points awarded (an ice-hockey column; meaning in basketball unverified). Null on some rows. |
| `rebounds` | integer | Total rebounds: offensive + defensive. Team's totals include team rebounds. Null on some rows. |
| `rebounds_defensive` | integer | Defensive rebounds. Null on some rows. |
| `rebounds_defensive_percentage` | numeric | Share of available defensive rebounds taken while on the court, on a 0-100 scale; values above 100 (up to 108.9) occur, so the definition is unverified. Null on some rows. |
| `rebounds_offensive` | integer | Offensive rebounds. Null on some rows. |
| `rebounds_offensive_percentage` | numeric | Share of available offensive rebounds taken while on the court, 0-100 scale (up to 91.4 observed); definition unverified. Null on some rows. |
| `rebounds_percentage` | numeric | Share of all available rebounds taken while on the court, 0-100 scale; definition unverified. Null on some rows. |
| `steals` | integer | Steals. Null on some rows. |
| `triple_double` | integer | Triple-double flag: 1 when three or more of points, rebounds, assists, steals, blocks reached 10, else 0. |
| `true_shooting_attempts` | numeric | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Float64). |
| `true_shooting_percentage` | numeric | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Float64). |
| `turnovers` | integer | Turnovers committed by players. Null on some rows. |
| `turnovers_percentage` | numeric | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Float64). |
| `usage_rate` | numeric | Usage rate on a 0-100 scale: share of the team's possessions used while on the court (up to 136 observed); definition unverified. Null on some rows. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#easl_player_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The server has nothing for the request (an empty list or leaders object, a team or player without games, a 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown fixture is the server's HTTP 500), an error envelope, a game that is not finished or whose box score or play-by-play is incomplete, a stalled sync on the active season, or a body for another game, season, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed id or a season that is not one of the server's END years (raised before the request that would use it). |

### Example {#easl_player_box-example}

```python
easl_player_box(fixture_id='10af8fd4-106e-11f1-960b-5798087c7aa3')
```

_Last validated n/a._

## easl_players

Every EASL player of all seasons; with season and team_id a roster that adds jersey number and position.

**Endpoint URL:** `GET https://easl.joshricha.com/api/v1/players`

**Valid URL:** [https://easl.joshricha.com/api/v1/players?season=2026&teamId=865489e8-b0f5-11ed-8b3f-c199a9dea184](https://easl.joshricha.com/api/v1/players?season=2026&teamId=865489e8-b0f5-11ed-8b3f-c199a9dea184)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season END year (2026 = EASL 25-26; the four editions are 2024 to 2027). easl_runtime reads easl_seasons and maps it to the DataCore seasonId. Default: no season filter. |
| `teamId` | `team_id` |  |  | `Y` | DataCore team (entity) UUID v1, e.g. '865489e8-b0f5-11ed-8b3f-c199a9dea184' (Utsunomiya Brex): the team_id of easl_teams, easl_standings and the box scores. easl_team resolves the 12 historical teams easl_teams does not list. Needs season for a roster (jersey number and position). |

### Returns {#easl_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year of the filter, from the request; null when no season was passed. |
| `season_id` | character | DataCore season UUID v1 of the filter; null when no season was passed. |
| `player_row_id` | integer | The sync server's own integer row id of the player (not a DataCore id). |
| `player_id` | character | playerId: DataCore person UUID v1 (key space: EASL persons, the same id as the box score's `personId`; joins /players, leaders and play-by-play players). Not a team id. |
| `name` | character | Player display name in 'FAMILY Given' form with the family name upper-cased (OSAKABE Taiki), in Latin script. |
| `name_latin` | character | Alternative name: equal to nameFullLocal for 257 of 552 players, otherwise null; never non-Latin in the observed data. |
| `name_abbreviated` | character | Abbreviated name; null for 551 of 552 players (one empty string). |
| `nationality` | character | ISO 3166-1 alpha-3 code of the player's nationality; 36 codes occur, KOR 119, USA 102, JPN 81, PHL 80, TWN 61, CHN 22, MNG 16, HKG 15, MAC 13 ('PHI' also appears once, a non-ISO spelling of PHL). |
| `gender` | character | MALE on all 552 rows. |
| `dob` | date | Date of birth, 'YYYY-MM-DD' (a public biography field; 1980-10-05 to 2023-10-16 in the data, so the latest values are implausible); 5 players have none. Null on some rows. |
| `height_cm` | integer | Height in centimetres; 43 of 552 players have none. Null on some rows. |
| `height_feet` | character | Height in feet and inches as a string (6'9); null for most players. |
| `weight_kg` | integer | Weight in kilograms; many players have none. Null on some rows. |
| `weight_lbs` | integer | Weight in pounds; null for many players. |
| `status` | character | Registration state of the player in the database: ACTIVE, INACTIVE or UNREGISTERED. |
| `jersey_number` | character | Jersey number as a string. Present ONLY in the roster variant of /players (seasonId + teamId) and on /players/{playerId}; missing from the unfiltered list. Absent on some rows. |
| `position` | character | Playing position (G, F, C, PG, SG, SF, PF, GF). Present ONLY in the roster variant of /players (seasonId + teamId) and on /players/{playerId}; missing from the unfiltered list. Absent on some rows. |
| `organization_id` | character | DataCore organisation id: b1e0z (EASL). |
| `created_at` | character | When the sync server inserted the row, 'YYYY-MM-DD HH:MM:SS' (a different format from updatedAt; zone unstated, UTC assumed). |
| `updated_at` | character | When the sync server last synced the row, ISO 8601 with milliseconds and Z (UTC). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#easl_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The server has nothing for the request (an empty list or leaders object, a team or player without games, a 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown fixture is the server's HTTP 500), an error envelope, a game that is not finished or whose box score or play-by-play is incomplete, a stalled sync on the active season, or a body for another game, season, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed id or a season that is not one of the server's END years (raised before the request that would use it). |

### Example {#easl_players-example}

```python
easl_players(season=2026, team_id='865489e8-b0f5-11ed-8b3f-c199a9dea184')
```

_Last validated n/a._

## easl_player

One player's biography: names, nationality, birth date, height, weight.

**Endpoint URL:** `GET https://easl.joshricha.com/api/v1/players/{player_id}`

**Valid URL:** [https://easl.joshricha.com/api/v1/players/85c99dab-9f67-11f0-8780-a731edb5aad5](https://easl.joshricha.com/api/v1/players/85c99dab-9f67-11f0-8780-a731edb5aad5)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | DataCore person UUID v1, e.g. '85c99dab-9f67-11f0-8780-a731edb5aad5': the player_id of easl_players, the box scores, leaders and play-by-play. A malformed id is a ValueError. |

### Returns {#easl_player-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_row_id` | integer | The sync server's own integer row id of the player (not a DataCore id). |
| `player_id` | character | playerId: DataCore person UUID v1 (key space: EASL persons, the same id as the box score's `personId`; joins /players, leaders and play-by-play players). Not a team id. |
| `name` | character | Player display name in 'FAMILY Given' form with the family name upper-cased (OSAKABE Taiki), in Latin script. |
| `name_latin` | character | Alternative name: equal to nameFullLocal for 257 of 552 players, otherwise null; never non-Latin in the observed data. |
| `name_abbreviated` | character | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (String). |
| `nationality` | character | ISO 3166-1 alpha-3 code of the player's nationality; 36 codes occur, KOR 119, USA 102, JPN 81, PHL 80, TWN 61, CHN 22, MNG 16, HKG 15, MAC 13 ('PHI' also appears once, a non-ISO spelling of PHL). |
| `gender` | character | MALE on all 552 rows. |
| `dob` | date | Date of birth, 'YYYY-MM-DD' (a public biography field; 1980-10-05 to 2023-10-16 in the data, so the latest values are implausible); 5 players have none. |
| `height_cm` | integer | Height in centimetres; 43 of 552 players have none. |
| `height_feet` | character | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (String). |
| `weight_kg` | integer | Weight in kilograms; many players have none. |
| `weight_lbs` | integer | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Int64). |
| `status` | character | Registration state of the player in the database: ACTIVE, INACTIVE or UNREGISTERED. |
| `jersey_number` | character | Jersey number as a string. Present ONLY in the roster variant of /players (seasonId + teamId) and on /players/{playerId}; missing from the unfiltered list. Absent on some rows. |
| `position` | character | Playing position (G, F, C, PG, SG, SF, PF, GF). Present ONLY in the roster variant of /players (seasonId + teamId) and on /players/{playerId}; missing from the unfiltered list. Absent on some rows. |
| `organization_id` | character | DataCore organisation id: b1e0z (EASL). |
| `created_at` | character | When the sync server inserted the row, 'YYYY-MM-DD HH:MM:SS' (a different format from updatedAt; zone unstated, UTC assumed). |
| `updated_at` | character | When the sync server last synced the row, ISO 8601 with milliseconds and Z (UTC). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#easl_player-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The server has nothing for the request (an empty list or leaders object, a team or player without games, a 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown fixture is the server's HTTP 500), an error envelope, a game that is not finished or whose box score or play-by-play is incomplete, a stalled sync on the active season, or a body for another game, season, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed id or a season that is not one of the server's END years (raised before the request that would use it). |

### Example {#easl_player-example}

```python
easl_player(player_id='85c99dab-9f67-11f0-8780-a731edb5aad5')
```

_Last validated n/a._

## easl_player_stats

A player's cumulative statistics over ALL seasons (the server ignores a season filter).

**Endpoint URL:** `GET https://easl.joshricha.com/api/v1/players/{player_id}/stats`

**Valid URL:** [https://easl.joshricha.com/api/v1/players/85c99dab-9f67-11f0-8780-a731edb5aad5/stats](https://easl.joshricha.com/api/v1/players/85c99dab-9f67-11f0-8780-a731edb5aad5/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | DataCore person UUID v1, e.g. '85c99dab-9f67-11f0-8780-a731edb5aad5': the player_id of easl_players, the box scores, leaders and play-by-play. A malformed id is a ValueError. |

### Returns {#easl_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | character | DataCore person UUID v1, from the request (the body carries none). |
| `assists` | integer | Assists: passes that directly lead to a made field goal. Total over the player's games. |
| `assists_defensive` | integer | Defensive assists (DataCore column; meaning unverified). Total over the player's games. |
| `assists_per_game` | numeric | Assists: passes that directly lead to a made field goal. Per-game average: the total divided by `games`. |
| `assists_turnover_ratio` | numeric | Assists divided by turnovers (a ratio, not a percentage). Total over the player's games. |
| `blocks` | integer | Shots blocked (made by this player or team). Total over the player's games. |
| `blocks_per_game` | numeric | Shots blocked (made by this player or team). Per-game average: the total divided by `games`. |
| `blocks_received` | integer | The team's or player's own shots that were blocked by an opponent. Total over the player's games. |
| `blocks_received_per_game` | numeric | The team's or player's own shots that were blocked by an opponent. Per-game average: the total divided by `games`. |
| `double_double` | integer | Double-double flag: 1 when exactly two of points, rebounds, assists, steals, blocks reached 10 in the game, else 0 (a triple-double is counted in tripleDouble instead). In a season line it is a count of such games. Total over the player's games. |
| `draws` | integer | Drawn games (basketball has none). Total over the player's games. |
| `dunks` | integer | Dunks made. Total over the player's games. |
| `dunks_per_game` | numeric | Dunks made. Per-game average: the total divided by `games`. |
| `efficiency` | integer | NBA-style efficiency: points + rebounds + assists + steals + blocks - missed field goals - missed free throws - turnovers (verified on all 2,143 played player-games). Total over the player's games. |
| `efficiency_custom` | integer | Custom efficiency slot (DataCore column; meaning unverified). Total over the player's games. |
| `field_goals_attempted` | integer | Field goals attempted (2- and 3-point shots; free throws excluded). Total over the player's games. |
| `field_goals_effective_percentage` | numeric | Effective field-goal percentage on a 0-100 scale: (FGM + 0.5 x 3PM) / FGA x 100; can exceed 100 on small samples. Total over the player's games. |
| `field_goals_made` | integer | Field goals made (2- and 3-point shots). Total over the player's games. |
| `field_goals_percentage` | numeric | Field-goal percentage on a 0-100 scale (made / attempted x 100). Total over the player's games. |
| `fouls_coach_disqualifying` | integer | Disqualifying fouls on the coach. Total over the player's games. |
| `fouls_coach_technical` | integer | Technical fouls on the coach. Total over the player's games. |
| `fouls_disqualifying` | integer | Disqualifying fouls. Total over the player's games. |
| `fouls_disruptive` | integer | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Int64). |
| `fouls_drawn` | integer | Fouls drawn (committed against this player or team by an opponent). Total over the player's games. |
| `fouls_drawn_per_game` | numeric | Fouls drawn (committed against this player or team by an opponent). Per-game average: the total divided by `games`. |
| `fouls_flagrant` | integer | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Int64). |
| `fouls_offensive` | integer | Offensive fouls committed. Counted separately from foulsPersonal. Total over the player's games. |
| `fouls_personal` | integer | Personal fouls committed (offensive, technical and unsportsmanlike fouls are separate columns). Total over the player's games. |
| `fouls_personal_per_game` | numeric | Personal fouls committed (offensive, technical and unsportsmanlike fouls are separate columns). Per-game average: the total divided by `games`. |
| `fouls_technical` | integer | Technical fouls committed by the player or team. Total over the player's games. |
| `fouls_technical_cat1` | integer | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Int64). |
| `fouls_technical_cat2` | integer | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Int64). |
| `fouls_total` | integer | Fouls charged in total: personal + technical + unsportsmanlike + offensive (matches 2,142 of 2,143 played player-games; the exception is unexplained). Total over the player's games. |
| `fouls_unsportsmanlike` | integer | Unsportsmanlike fouls committed. Total over the player's games. |
| `free_throws_attempted` | integer | Free throws attempted. Total over the player's games. |
| `free_throws_attempted_per_game` | numeric | Free throws attempted. Per-game average: the total divided by `games`. |
| `free_throws_made` | integer | Free throws made. Total over the player's games. |
| `free_throws_made_per_game` | numeric | Free throws made. Per-game average: the total divided by `games`. |
| `free_throws_percentage` | numeric | Free-throw percentage on a 0-100 scale. Total over the player's games. |
| `games` | integer | Finished games counted in this line. Total over the player's games. |
| `games_percentage` | numeric | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Float64). |
| `games_started` | integer | Games the player started. Total over the player's games. |
| `index_of_success` | integer | Index of success: efficiency + fouls drawn - personal fouls - technical fouls - blocks received (verified on all 2,143 played player-games). It differs from pir only in which fouls it subtracts. Total over the player's games. |
| `losses` | integer | Games lost (for a player: by his team while he is in the line). Total over the player's games. |
| `max_assists` | integer | Highest assists in a single game: in a game row it equals assists; in a season line it is the best game. Total over the player's games. |
| `max_free_throws_made` | integer | Highest free throws made in a single game (equals freeThrowsMade in a game row). Total over the player's games. |
| `max_points` | integer | Highest points in a single game (equals points in a game row). Total over the player's games. |
| `max_points_four_made` | integer | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Int64). |
| `max_points_three_made` | integer | Highest 3-pointers made in a single game. Total over the player's games. |
| `max_points_two_made` | integer | Highest 2-pointers made in a single game. Total over the player's games. |
| `max_rebounds` | integer | Highest rebounds in a single game. Total over the player's games. |
| `max_rebounds_defensive` | integer | Highest defensive rebounds in a single game. Total over the player's games. |
| `max_rebounds_offensive` | integer | Highest offensive rebounds in a single game. Total over the player's games. |
| `max_steals` | integer | Highest steals in a single game. Total over the player's games. |
| `minus` | integer | Points the opponents scored while the player was on the court. Total over the player's games. |
| `minutes` | character | Playing time as an ISO 8601 duration PT{m}M{s}S with the minutes NOT wrapped at 60 (PT27M53S; a team's is the sum of its players', PT200M for a 40-minute game, PT250M with one overtime). Total over the player's games. |
| `minutes_per_game` | character | Playing time as an ISO 8601 duration PT{m}M{s}S with the minutes NOT wrapped at 60 (PT27M53S; a team's is the sum of its players', PT200M for a 40-minute game, PT250M with one overtime). Per-game average: the total divided by `games`. |
| `pir` | integer | Performance index rating: efficiency + fouls drawn - total fouls - blocks received (verified on all 2,143 played player-games). Total over the player's games. |
| `plus` | integer | Points the player's team scored while he was on the court. Total over the player's games. |
| `plus_minus` | integer | plus - minus: the team's point margin while the player was on the court. Total over the player's games. |
| `points` | integer | Points scored. Total over the player's games. |
| `points_fast_break` | integer | Points scored on fast breaks. Total over the player's games. |
| `points_fast_break_attempted` | integer | Fast-break field goals attempted. Total over the player's games. |
| `points_fast_break_made` | integer | Fast-break field goals made. Total over the player's games. |
| `points_four_attempted` | integer | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Int64). |
| `points_four_attempted_per_game` | numeric | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Float64). |
| `points_four_made` | integer | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Int64). |
| `points_four_made_per_game` | numeric | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Float64). |
| `points_four_percentage` | numeric | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Float64). |
| `points_from_turnover` | integer | Points scored off opponent turnovers. Total over the player's games. |
| `points_in_the_paint` | integer | Points scored in the paint. Total over the player's games. |
| `points_in_the_paint_attempted` | integer | Field goals attempted in the paint. Total over the player's games. |
| `points_in_the_paint_made` | integer | Field goals made in the paint. Total over the player's games. |
| `points_per_game` | numeric | Points scored. Per-game average: the total divided by `games`. |
| `points_second_chance` | integer | Points scored on second-chance possessions (after an offensive rebound). Total over the player's games. |
| `points_second_chance_attempted` | integer | Second-chance field goals attempted. Total over the player's games. |
| `points_second_chance_made` | integer | Second-chance field goals made. Total over the player's games. |
| `points_three_attempted` | integer | 3-point field goals attempted. Total over the player's games. |
| `points_three_attempted_per_game` | numeric | 3-point field goals attempted. Per-game average: the total divided by `games`. |
| `points_three_made` | integer | 3-point field goals made. Total over the player's games. |
| `points_three_made_per_game` | numeric | 3-point field goals made. Per-game average: the total divided by `games`. |
| `points_three_percentage` | numeric | 3-point percentage on a 0-100 scale. Total over the player's games. |
| `points_two_attempted` | integer | 2-point field goals attempted. Total over the player's games. |
| `points_two_attempted_per_game` | numeric | 2-point field goals attempted. Per-game average: the total divided by `games`. |
| `points_two_made` | integer | 2-point field goals made. Total over the player's games. |
| `points_two_made_per_game` | numeric | 2-point field goals made. Per-game average: the total divided by `games`. |
| `points_two_percentage` | numeric | 2-point percentage on a 0-100 scale. Total over the player's games. |
| `power_play_points` | integer | Power-play points (an ice-hockey column; meaning in basketball unverified). Total over the player's games. |
| `power_play_points_awarded` | integer | Power-play points awarded (an ice-hockey column; meaning in basketball unverified). Total over the player's games. |
| `rebounds` | integer | Total rebounds: offensive + defensive. Team's totals include team rebounds. Total over the player's games. |
| `rebounds_defensive` | integer | Defensive rebounds. Total over the player's games. |
| `rebounds_defensive_per_game` | numeric | Defensive rebounds. Per-game average: the total divided by `games`. |
| `rebounds_offensive` | integer | Offensive rebounds. Total over the player's games. |
| `rebounds_offensive_per_game` | numeric | Offensive rebounds. Per-game average: the total divided by `games`. |
| `rebounds_per_game` | numeric | Total rebounds: offensive + defensive. Team's totals include team rebounds. Per-game average: the total divided by `games`. |
| `steals` | integer | Steals. Total over the player's games. |
| `steals_per_game` | numeric | Steals. Per-game average: the total divided by `games`. |
| `triple_double` | integer | Triple-double flag: 1 when three or more of points, rebounds, assists, steals, blocks reached 10, else 0. Total over the player's games. |
| `true_shooting_attempts` | numeric | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Float64). |
| `true_shooting_percentage` | numeric | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Float64). |
| `turnovers` | integer | Turnovers committed by players. Total over the player's games. |
| `turnovers_per_game` | numeric | Turnovers committed by players. Per-game average: the total divided by `games`. |
| `turnovers_percentage` | numeric | Not yet populated upstream: null in every full body observed (2026-10-12), so its meaning is unverified; the 2026-27 bodies start filling some of these columns, so it is typed from its family (Float64). |
| `wins` | integer | Games won (for a player: by his team while he is in the line). Total over the player's games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#easl_player_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The server has nothing for the request (an empty list or leaders object, a team or player without games, a 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown fixture is the server's HTTP 500), an error envelope, a game that is not finished or whose box score or play-by-play is incomplete, a stalled sync on the active season, or a body for another game, season, team or player than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed id or a season that is not one of the server's END years (raised before the request that would use it). |

### Example {#easl_player_stats-example}

```python
easl_player_stats(player_id='85c99dab-9f67-11f0-8780-a731edb5aad5')
```

_Last validated n/a._
