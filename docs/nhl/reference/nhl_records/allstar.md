# NHL — NHL Records API — Allstar

> NHL — NHL Records API — Allstar — function reference in sdv-py, the SportsDataverse Python package.

## nhl_records_allstar_skater_career

All-Star Game career statistics for skaters.

**Endpoint URL:** `GET https://records.nhl.com/site/api/all-star-skater-career-stats`

**Valid URL:** [https://records.nhl.com/site/api/all-star-skater-career-stats](https://records.nhl.com/site/api/all-star-skater-career-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_allstar_skater_career-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `all_star_team_id` | Int64 | Identifier for the All-Star team roster to which the skater was assigned during the All-Star event. |
| `assists` | Int64 | Assists. |
| `first_name` | String | Player first name. |
| `full_name` | String | Player full name. |
| `games_played` | Int64 | Games played. |
| `goals` | Int64 | Goals scored. |
| `is_active` | Boolean | Whether the team is active. |
| `is_rookie` | Boolean | Whether the player is a rookie. |
| `last_name` | String | Player last name. |
| `losses` | Int64 | Losses. |
| `nhl_team_id` | Int64 | NHL identifier for the skater's regular-season team at the time of All-Star selection. |
| `penalties` | Float64 | Penalty count. |
| `penalty_minutes` | Float64 | Penalty minutes. |
| `player_id` | Int64 | Unique player identifier. |
| `points` | Int64 | Total points (goals + assists). |
| `position` | String | Player position. |
| `power_play_goals` | Int64 | Power-play goals. |
| `season_id` | Int64 | Season identifier. |
| `short_handed_goals` | Int64 | Short-handed goals. |
| `wins` | Int64 | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_allstar_skater_career-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_allstar_skater_career-example}

```python
nhl_records_allstar_skater_career()
```

## nhl_records_allstar_goalie_career

All-Star Game career statistics for goaltenders.

**Endpoint URL:** `GET https://records.nhl.com/site/api/all-star-goaltender-career-stats`

**Valid URL:** [https://records.nhl.com/site/api/all-star-goaltender-career-stats](https://records.nhl.com/site/api/all-star-goaltender-career-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_allstar_goalie_career-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `all_star_team_id` | Int64 | Identifier for the NHL All-Star team the goalie represented in their career All-Star appearances. |
| `first_name` | String | Player first name. |
| `full_name` | String | Player full name. |
| `games_played` | Int64 | Games played. |
| `goals_against` | Int64 | Goals against. |
| `goals_against_average` | Float64 | Goals against average. |
| `is_active` | Boolean | Whether the team is active. |
| `is_rookie` | Boolean | Whether the player is a rookie. |
| `last_name` | String | Player last name. |
| `losses` | Int64 | Losses. |
| `nhl_team_id` | Int64 | NHL identifier for the regular-season franchise the goalie was affiliated with during their All-Star career. |
| `ot_losses` | Int64 | Overtime losses. |
| `player_id` | Int64 | Unique player identifier. |
| `save_percentage` | Float64 | Save percentage (goalies). |
| `season_id` | Int64 | Season identifier. |
| `shots_against` | Int64 | Shots faced. |
| `team_losses` | Int64 |  |
| `team_wins` | Int64 |  |
| `ties` | Int64 | Total ties. |
| `time_on_ice` | Int64 | Time on ice in seconds. |
| `wins` | Int64 | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_allstar_goalie_career-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_allstar_goalie_career-example}

```python
nhl_records_allstar_goalie_career()
```

## nhl_records_allstar_coach_career

All-Star Game career records for coaches.

**Endpoint URL:** `GET https://records.nhl.com/site/api/all-star-coach-career-stats`

**Valid URL:** [https://records.nhl.com/site/api/all-star-coach-career-stats](https://records.nhl.com/site/api/all-star-coach-career-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_allstar_coach_career-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `all_star_team_id` | Int64 | NHL identifier for the All-Star team the coach was assigned to in a given All-Star game. |
| `coach_id` | Int64 | ESPN coach id parsed from the `$ref` URL. |
| `first_name` | String | Player first name. |
| `full_name` | String | Player full name. |
| `games_coached` | Int64 | Total number of All-Star games the coach has coached across their career. |
| `is_active` | Boolean | Whether the team is active. |
| `last_name` | String | Player last name. |
| `losses` | Int64 | Losses. |
| `ot_losses` | Int64 | Overtime losses. |
| `season_id` | Int64 | Season identifier. |
| `ties` | Int64 | Total ties. |
| `wins` | Int64 | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_allstar_coach_career-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_allstar_coach_career-example}

```python
nhl_records_allstar_coach_career()
```

## nhl_records_allstar_skater_game

All-Star Game single-game scoring records for skaters.

**Endpoint URL:** `GET https://records.nhl.com/site/api/all-star-skater-game-stats`

**Valid URL:** [https://records.nhl.com/site/api/all-star-skater-game-stats](https://records.nhl.com/site/api/all-star-skater-game-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_allstar_skater_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `all_star_team_id` | Int64 | Identifier for the All-Star team roster to which the skater was assigned for this game. |
| `all_star_team_score` | Int64 | Goals scored by the skater's All-Star team in this specific All-Star game. |
| `arena_name` | String |  |
| `assists` | Int64 | Assists. |
| `city` | String | City where the venue is located. |
| `first_name` | String | Player first name. |
| `full_name` | String | Player full name. |
| `game_date` | String | Game date. |
| `game_id` | Int64 | Unique game identifier. |
| `game_name` | String |  |
| `goals` | Int64 | Goals scored. |
| `home_road` | String | Designation indicating whether the skater's All-Star team was the home or road side for this game. |
| `is_active` | Boolean | Whether the team is active. |
| `is_rookie` | Boolean | Whether the player is a rookie. |
| `last_name` | String | Player last name. |
| `mvp` | String |  |
| `nhl_team_id` | Int64 | NHL identifier for the skater's regular-season team at the time this All-Star game was played. |
| `opponent_score` | Int64 |  |
| `opponent_team_id` | Int64 | Opponent team identifier. |
| `penalties` | Float64 | Penalty count. |
| `penalty_minutes` | Float64 | Penalty minutes. |
| `player_id` | Int64 | Unique player identifier. |
| `points` | Int64 | Total points (goals + assists). |
| `position` | String | Player position. |
| `power_play_goals` | Int64 | Power-play goals. |
| `season_id` | Int64 | Season identifier. |
| `short_handed_goals` | Int64 | Short-handed goals. |
| `state_province_code` | String | State or province code of the official. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_allstar_skater_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_allstar_skater_game-example}

```python
nhl_records_allstar_skater_game()
```

## nhl_records_allstar_goalie_game

All-Star Game single-game stats for goaltenders.

**Endpoint URL:** `GET https://records.nhl.com/site/api/all-star-goaltender-game-stats`

**Valid URL:** [https://records.nhl.com/site/api/all-star-goaltender-game-stats](https://records.nhl.com/site/api/all-star-goaltender-game-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_allstar_goalie_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `all_star_team_id` | Int64 | NHL identifier for the All-Star team the goalie was assigned to in the game. |
| `all_star_team_score` | Int64 | Goals scored by the goalie's All-Star team in that game. |
| `arena_name` | String |  |
| `city` | String | City where the venue is located. |
| `first_name` | String | Player first name. |
| `full_name` | String | Player full name. |
| `game_date` | String | Game date. |
| `game_id` | Int64 | Unique game identifier. |
| `game_name` | String |  |
| `goals_against` | Int64 | Goals against. |
| `home_road` | String | Indicates whether the goalie's All-Star team was the designated home or road squad for the game. |
| `is_active` | Boolean | Whether the team is active. |
| `is_rookie` | Boolean | Whether the player is a rookie. |
| `last_name` | String | Player last name. |
| `mvp` | String |  |
| `nhl_team_id` | Int64 | NHL identifier for the goalie's regular-season franchise at the time of the All-Star game. |
| `opponent_score` | Int64 |  |
| `opponent_team_id` | Int64 | Opponent team identifier. |
| `player_id` | Int64 | Unique player identifier. |
| `save_percentage` | Float64 | Save percentage (goalies). |
| `saves` | Int64 | Saves made. |
| `season_id` | Int64 | Season identifier. |
| `shots_against` | Int64 | Shots faced. |
| `state_province_code` | String | State or province code of the official. |
| `time_on_ice` | Int64 | Time on ice in seconds. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_allstar_goalie_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_allstar_goalie_game-example}

```python
nhl_records_allstar_goalie_game()
```
