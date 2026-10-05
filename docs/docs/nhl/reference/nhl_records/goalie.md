---
title: "NHL — NHL Records API — Goalie"
sidebar_label: "Goalie"
sidebar_position: 4
description: "NHL — NHL Records API — Goalie — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NHL — NHL Records API — Goalie

## nhl_records_goalie_career_stats

Goaltender career statistics (regular season).

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-career-stats`

**Valid URL:** [https://records.nhl.com/site/api/goalie-career-stats](https://records.nhl.com/site/api/goalie-career-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_goalie_career_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_player` | logical | Indicator of whether the player is active. |
| `first_name` | character | Player first name. |
| `first_season_for_game_type` | integer | First season ID for the game type. |
| `franchise_id` | double | Unique franchise identifier. |
| `game_seven_games_played` | character | Game seven games played. |
| `game_seven_losses` | character | Game seven losses. |
| `game_seven_wins` | character | Game seven wins. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `games_played` | integer | Games played. |
| `goals_against` | integer | Goals against. |
| `goals_against_average` | double | Goals against average. |
| `last_name` | character | Player last name. |
| `last_season_for_game_type` | integer | Last season ID for the game type. |
| `losses` | integer | Losses. |
| `overtime_games_played` | integer | Overtime games played. |
| `overtime_goals_against` | integer | Overtime goals against. |
| `overtime_goals_against_average` | character | Overtime goals against average. |
| `overtime_losses` | character | Total overtime losses. |
| `overtime_save_pctg` | double | Overtime save percentage. |
| `overtime_shots_against` | integer | Overtime shots against. |
| `overtime_ties` | integer | Overtime ties. |
| `overtime_time_on_ice` | double | Overtime time on ice (seconds). |
| `overtime_wins` | integer | Overtime wins. |
| `player_id` | integer | Unique player identifier. |
| `position_code` | character | Player position code. |
| `save_pctg` | double | Save percentage. |
| `saves` | integer | Saves made. |
| `seasons_played` | integer | Number of seasons played. |
| `shots_against` | integer | Shots faced. |
| `shutouts` | integer | Shutouts recorded. |
| `team_abbrevs` | character | Team abbreviation(s). |
| `team_names` | character | Team names. |
| `ties` | integer | Total ties. |
| `time_on_ice` | integer | Time on ice in seconds. |
| `time_on_ice_min_sec` | character | Total time on ice (MM:SS). |
| `wins` | integer | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_goalie_career_stats-example}

```python
nhl_records_goalie_career_stats()
```

_Last validated n/a._

## nhl_records_goalie_career_stats_with_playoffs

Goaltender career stats inclusive of regular season and playoffs.

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie_career_stats_incl_playoffs`

**Valid URL:** [https://records.nhl.com/site/api/goalie_career_stats_incl_playoffs](https://records.nhl.com/site/api/goalie_career_stats_incl_playoffs)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_goalie_career_stats_with_playoffs-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_player` | integer | Indicator of whether the player is active. |
| `first_name` | character | Player first name. |
| `franchise_id` | double | Unique franchise identifier. |
| `games_played` | integer | Games played. |
| `goals_against` | integer | Goals against. |
| `goals_against_average` | double | Goals against average. |
| `last_name` | character | Player last name. |
| `losses` | integer | Losses. |
| `overtime_losses` | character | Total overtime losses. |
| `player_id` | integer | Unique player identifier. |
| `position_code` | character | Player position code. |
| `save_pctg` | double | Save percentage. |
| `saves` | integer | Saves made. |
| `shots_against` | integer | Shots faced. |
| `shutouts` | integer | Shutouts recorded. |
| `team_abbrevs` | character | Team abbreviation(s). |
| `team_names` | character | Team names. |
| `ties` | integer | Total ties. |
| `time_on_ice` | integer | Time on ice in seconds. |
| `time_on_ice_min_sec` | character | Total time on ice (MM:SS). |
| `wins` | integer | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_goalie_career_stats_with_playoffs-example}

```python
nhl_records_goalie_career_stats_with_playoffs()
```

_Last validated n/a._

## nhl_records_goalie_season_stats

Goaltender single-season statistics.

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-season-stats`

**Valid URL:** [https://records.nhl.com/site/api/goalie-season-stats](https://records.nhl.com/site/api/goalie-season-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_goalie_season_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_player` | logical | Indicator of whether the player is active. |
| `first_name` | character | Player first name. |
| `franchise_id` | double | Unique franchise identifier. |
| `game_seven_games_played` | character | Game seven games played. |
| `game_seven_losses` | character | Game seven losses. |
| `game_seven_wins` | character | Game seven wins. |
| `game_type` | integer | Game type the row belongs to. |
| `games_played` | integer | Games played. |
| `games_started` | integer | Games started (goalies). |
| `goals_against` | integer | Goals against. |
| `goals_against_average` | double | Goals against average. |
| `last_name` | character | Player last name. |
| `losses` | integer | Losses. |
| `number_of_games_in_season` | integer | Number of games in the season. |
| `overtime_games_played` | integer | Overtime games played. |
| `overtime_goals_against` | integer | Overtime goals against. |
| `overtime_losses` | character | Total overtime losses. |
| `overtime_ties` | integer | Overtime ties. |
| `overtime_wins` | integer | Overtime wins. |
| `player_id` | integer | Unique player identifier. |
| `position_code` | character | Player position code. |
| `rookie_flag` | logical | Indicator of whether the player was a rookie. |
| `save_pctg` | double | Save percentage. |
| `saves` | integer | Saves made. |
| `season_id` | integer | Season identifier. |
| `shots_against` | integer | Shots faced. |
| `shutouts` | integer | Shutouts recorded. |
| `team_abbrevs` | character | Team abbreviation(s). |
| `team_names` | character | Team names. |
| `ties` | integer | Total ties. |
| `time_on_ice` | integer | Time on ice in seconds. |
| `time_on_ice_min_sec` | character | Total time on ice (MM:SS). |
| `wins` | integer | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_goalie_season_stats-example}

```python
nhl_records_goalie_season_stats()
```

_Last validated n/a._

## nhl_records_goalie_win_streak

Goaltenders with the longest consecutive-win streaks.

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-win-streak`

**Valid URL:** [https://records.nhl.com/site/api/goalie-win-streak](https://records.nhl.com/site/api/goalie-win-streak)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_goalie_win_streak-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_player` | logical | Indicator of whether the player is active. |
| `active_streak` | logical | Indicator of whether the streak is active. |
| `end_date` | character | Season end date. |
| `first_name` | character | Player first name. |
| `franchise_id` | double | Unique franchise identifier. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `last_name` | character | Player last name. |
| `player_id` | integer | Unique player identifier. |
| `rookie` | logical | Whether the player is a rookie. |
| `season_id` | integer | Season identifier. |
| `start_date` | character | Season start date. |
| `team_abbrev` | character | Team abbreviation. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Team name. |
| `win_streak` | integer | Number of consecutive wins recorded by the goalie in this streak. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_goalie_win_streak-example}

```python
nhl_records_goalie_win_streak()
```

_Last validated n/a._

## nhl_records_goalie_shutout_streak

Goaltenders with the longest consecutive-shutout streaks.

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-shutout-streak`

**Valid URL:** [https://records.nhl.com/site/api/goalie-shutout-streak](https://records.nhl.com/site/api/goalie-shutout-streak)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_goalie_shutout_streak-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_player` | logical | Indicator of whether the player is active. |
| `active_streak` | logical | Indicator of whether the streak is active. |
| `duration_min_sec` | character | Streak duration (MM:SS). |
| `duration_seconds` | integer | Streak duration in seconds. |
| `end_date` | character | Season end date. |
| `first_name` | character | Player first name. |
| `franchise_id` | integer | Unique franchise identifier. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `last_name` | character | Player last name. |
| `player_id` | integer | Unique player identifier. |
| `saves` | character | Saves made. |
| `season_id` | integer | Season identifier. |
| `start_date` | character | Season start date. |
| `team_abbrev` | character | Team abbreviation. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Team name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_goalie_shutout_streak-example}

```python
nhl_records_goalie_shutout_streak()
```

_Last validated n/a._

## nhl_records_goalie_win_plateaus

Goaltenders who reached each win plateau (100, 200, 300 …).

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-win-plateaus`

**Valid URL:** [https://records.nhl.com/site/api/goalie-win-plateaus](https://records.nhl.com/site/api/goalie-win-plateaus)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_goalie_win_plateaus-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_player` | logical | Indicator of whether the player is active. |
| `first_name` | character | Player first name. |
| `forty_win_seasons` | integer | Number of seasons in which the goalie recorded 40 or more wins, a rare single-season achievement. |
| `franchise_id` | double | Unique franchise identifier. |
| `last_name` | character | Player last name. |
| `player_id` | integer | Unique player identifier. |
| `seasons_played` | integer | Number of seasons played. |
| `team_abbrevs` | character | Team abbreviation(s). |
| `team_names` | character | Team names. |
| `thirty_win_seasons` | integer | Number of seasons in which the goalie recorded 30 or more wins. |
| `twenty_win_seasons` | integer | Number of seasons in which the goalie recorded 20 or more wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_goalie_win_plateaus-example}

```python
nhl_records_goalie_win_plateaus()
```

_Last validated n/a._

## nhl_records_goalie_playoff_streak

Goaltender consecutive playoff-win streaks.

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-playoff-streak`

**Valid URL:** [https://records.nhl.com/site/api/goalie-playoff-streak](https://records.nhl.com/site/api/goalie-playoff-streak)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_goalie_playoff_streak-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_player` | logical | Indicator of whether the player is active. |
| `active_streak` | logical | Indicator of whether the streak is active. |
| `consecutive_playoff_seasons` | integer | Number of consecutive playoff seasons in which the goalie appeared for the franchise during this streak. |
| `end_season` | integer | Last season (in YYYYYYYY format) of the goalie's consecutive playoff appearance streak. |
| `first_name` | character | Player first name. |
| `franchise_id` | double | Unique franchise identifier. |
| `last_name` | character | Player last name. |
| `player_id` | integer | Unique player identifier. |
| `playoff_seasons` | integer | Number of playoff seasons. |
| `stanley_cup_wins` | integer | Number of Stanley Cup championships. |
| `start_season` | integer | First season (in YYYYYYYY format) of the goalie's consecutive playoff appearance streak. |
| `team_abbrevs` | character | Team abbreviation(s). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_goalie_playoff_streak-example}

```python
nhl_records_goalie_playoff_streak()
```

_Last validated n/a._

## nhl_records_goalie_undefeated_streak

Goaltender longest undefeated streaks (wins + ties).

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-undefeated-streak`

**Valid URL:** [https://records.nhl.com/site/api/goalie-undefeated-streak](https://records.nhl.com/site/api/goalie-undefeated-streak)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_goalie_undefeated_streak-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_player` | logical | Indicator of whether the player is active. |
| `active_streak` | logical | Indicator of whether the streak is active. |
| `end_date` | character | Season end date. |
| `first_name` | character | Player first name. |
| `franchise_id` | double | Unique franchise identifier. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `last_name` | character | Player last name. |
| `player_id` | integer | Unique player identifier. |
| `season_id` | integer | Season identifier. |
| `start_date` | character | Season start date. |
| `team_abbrev` | character | Team abbreviation. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Team name. |
| `undefeated_streak` | integer | Number of consecutive games without a regulation loss (wins plus overtime or shootout losses) in the goalie's record streak. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_goalie_undefeated_streak-example}

```python
nhl_records_goalie_undefeated_streak()
```

_Last validated n/a._
