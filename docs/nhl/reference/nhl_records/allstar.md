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
| `id` | integer | Unique player identifier. |
| `all_star_team_id` | integer | Identifier for the All-Star team roster to which the skater was assigned during the All-Star event. |
| `assists` | integer | Assists. |
| `first_name` | character | Player first name. |
| `full_name` | character | Player full name. |
| `games_played` | integer | Games played. |
| `goals` | integer | Goals scored. |
| `is_active` | logical | Whether the team is active. |
| `is_rookie` | logical | Whether the player is a rookie. |
| `last_name` | character | Player last name. |
| `losses` | integer | Losses. |
| `nhl_team_id` | integer | NHL identifier for the skater's regular-season team at the time of All-Star selection. |
| `penalties` | double | Penalty count. |
| `penalty_minutes` | double | Penalty minutes. |
| `player_id` | integer | Unique player identifier. |
| `points` | integer | Total points (goals + assists). |
| `position` | character | Player position. |
| `power_play_goals` | integer | Power-play goals. |
| `season_id` | integer | Season identifier. |
| `short_handed_goals` | integer | Short-handed goals. |
| `wins` | integer | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_allstar_skater_career-example}

```python
nhl_records_allstar_skater_career()
```

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `all_star_team_id` | integer | Identifier for the NHL All-Star team the goalie represented in their career All-Star appearances. |
| `first_name` | character | Player first name. |
| `full_name` | character | Player full name. |
| `games_played` | integer | Games played. |
| `goals_against` | integer | Goals against. |
| `goals_against_average` | double | Goals against average. |
| `is_active` | logical | Whether the team is active. |
| `is_rookie` | logical | Whether the player is a rookie. |
| `last_name` | character | Player last name. |
| `losses` | integer | Losses. |
| `nhl_team_id` | integer | NHL identifier for the regular-season franchise the goalie was affiliated with during their All-Star career. |
| `ot_losses` | integer | Overtime losses. |
| `player_id` | integer | Unique player identifier. |
| `save_percentage` | double | Save percentage (goalies). |
| `season_id` | integer | Season identifier. |
| `shots_against` | integer | Shots faced. |
| `team_losses` | integer |  |
| `team_wins` | integer |  |
| `ties` | integer | Total ties. |
| `time_on_ice` | integer | Time on ice in seconds. |
| `wins` | integer | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_allstar_goalie_career-example}

```python
nhl_records_allstar_goalie_career()
```

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `all_star_team_id` | integer | NHL identifier for the All-Star team the coach was assigned to in a given All-Star game. |
| `coach_id` | integer | ESPN coach id parsed from the `$ref` URL. |
| `first_name` | character | Player first name. |
| `full_name` | character | Player full name. |
| `games_coached` | integer | Total number of All-Star games the coach has coached across their career. |
| `is_active` | logical | Whether the team is active. |
| `last_name` | character | Player last name. |
| `losses` | integer | Losses. |
| `ot_losses` | integer | Overtime losses. |
| `season_id` | integer | Season identifier. |
| `ties` | integer | Total ties. |
| `wins` | integer | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_allstar_coach_career-example}

```python
nhl_records_allstar_coach_career()
```

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `all_star_team_id` | integer | Identifier for the All-Star team roster to which the skater was assigned for this game. |
| `all_star_team_score` | integer | Goals scored by the skater's All-Star team in this specific All-Star game. |
| `arena_name` | character |  |
| `assists` | integer | Assists. |
| `city` | character | City where the venue is located. |
| `first_name` | character | Player first name. |
| `full_name` | character | Player full name. |
| `game_date` | character | Game date. |
| `game_id` | integer | Unique game identifier. |
| `game_name` | character |  |
| `goals` | integer | Goals scored. |
| `home_road` | character | Designation indicating whether the skater's All-Star team was the home or road side for this game. |
| `is_active` | logical | Whether the team is active. |
| `is_rookie` | logical | Whether the player is a rookie. |
| `last_name` | character | Player last name. |
| `mvp` | character |  |
| `nhl_team_id` | integer | NHL identifier for the skater's regular-season team at the time this All-Star game was played. |
| `opponent_score` | integer |  |
| `opponent_team_id` | integer | Opponent team identifier. |
| `penalties` | double | Penalty count. |
| `penalty_minutes` | double | Penalty minutes. |
| `player_id` | integer | Unique player identifier. |
| `points` | integer | Total points (goals + assists). |
| `position` | character | Player position. |
| `power_play_goals` | integer | Power-play goals. |
| `season_id` | integer | Season identifier. |
| `short_handed_goals` | integer | Short-handed goals. |
| `state_province_code` | character | State or province code of the official. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_allstar_skater_game-example}

```python
nhl_records_allstar_skater_game()
```

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `all_star_team_id` | integer | NHL identifier for the All-Star team the goalie was assigned to in the game. |
| `all_star_team_score` | integer | Goals scored by the goalie's All-Star team in that game. |
| `arena_name` | character |  |
| `city` | character | City where the venue is located. |
| `first_name` | character | Player first name. |
| `full_name` | character | Player full name. |
| `game_date` | character | Game date. |
| `game_id` | integer | Unique game identifier. |
| `game_name` | character |  |
| `goals_against` | integer | Goals against. |
| `home_road` | character | Indicates whether the goalie's All-Star team was the designated home or road squad for the game. |
| `is_active` | logical | Whether the team is active. |
| `is_rookie` | logical | Whether the player is a rookie. |
| `last_name` | character | Player last name. |
| `mvp` | character |  |
| `nhl_team_id` | integer | NHL identifier for the goalie's regular-season franchise at the time of the All-Star game. |
| `opponent_score` | integer |  |
| `opponent_team_id` | integer | Opponent team identifier. |
| `player_id` | integer | Unique player identifier. |
| `save_percentage` | double | Save percentage (goalies). |
| `saves` | integer | Saves made. |
| `season_id` | integer | Season identifier. |
| `shots_against` | integer | Shots faced. |
| `state_province_code` | character | State or province code of the official. |
| `time_on_ice` | integer | Time on ice in seconds. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_allstar_goalie_game-example}

```python
nhl_records_allstar_goalie_game()
```

_Last validated n/a._
