---
title: "NFL — nflpro — Team"
sidebar_label: "Team"
sidebar_position: 4
description: "NFL — nflpro — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — nflpro — Team

## nfl_pro_team_offense_overview_season

GET /api/secured/stats/team-offense/overview/season — one row per team for the season — team offensive overview incl. EPA per play, pass and rush splits.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/team-offense/overview/season`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/team-offense/overview/season?season=2024&seasonType=REG&limit=500](https://pro.nfl.com/api/secured/stats/team-offense/overview/season?season=2024&seasonType=REG&limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |

### Returns {#nfl_pro_team_offense_overview_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | character | ESPN team id. |
| `gp` | integer | Games played. |
| `total` | integer | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `pass` | integer | Binary indicator if the play was a pass play (sacks and scrambles included). |
| `run` | integer | Expected Points Added on run plays |
| `pass_pct` | double |  |
| `ppg` | double | Points per game. |
| `yds` | integer |  |
| `ypg` | double |  |
| `ypp` | double |  |
| `td` | integer |  |
| `epa` | double | Expected points added (EPA) by the posteam for the given play. |
| `epa_pp` | double |  |
| `pass_yds` | integer |  |
| `pass_ypg` | double |  |
| `pass_ypp` | double |  |
| `sacked_yds` | integer |  |
| `sacked_ypg` | double |  |
| `epa_pass` | double |  |
| `epa_pass_pp` | double |  |
| `rush_yds` | integer | Rushing yards gained on the play. |
| `rush_ypg` | double |  |
| `rush_ypp` | double |  |
| `epa_rush` | double |  |
| `epa_rush_pp` | double |  |
| `ryoe` | double |  |
| `ttt` | double |  |
| `qbp` | integer |  |
| `qbp_pct` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nfl_pro_team_offense_overview_season-example}

```python
nfl_pro_team_offense_overview_season(season=2024, season_type='REG')
```

_Last validated n/a._

## nfl_pro_team_offense_overview_week

GET /api/secured/stats/team-offense/overview/week — one row per team per week — team offensive overview incl. EPA per play, pass and rush splits.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/team-offense/overview/week`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/team-offense/overview/week?season=2024&seasonType=REG&limit=500](https://pro.nfl.com/api/secured/stats/team-offense/overview/week?season=2024&seasonType=REG&limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |

### Returns {#nfl_pro_team_offense_overview_week-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | character | ESPN team id. |
| `gp` | integer | Games played. |
| `total` | integer | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `pass` | integer | Binary indicator if the play was a pass play (sacks and scrambles included). |
| `run` | integer | Expected Points Added on run plays |
| `pass_pct` | double |  |
| `ppg` | integer | Points per game. |
| `yds` | integer |  |
| `ypg` | integer |  |
| `ypp` | double |  |
| `td` | integer |  |
| `epa` | double | Expected points added (EPA) by the posteam for the given play. |
| `epa_pp` | double |  |
| `pass_yds` | integer |  |
| `pass_ypg` | integer |  |
| `pass_ypp` | double |  |
| `sacked_yds` | integer |  |
| `sacked_ypg` | integer |  |
| `epa_pass` | double |  |
| `epa_pass_pp` | double |  |
| `rush_yds` | integer | Rushing yards gained on the play. |
| `rush_ypg` | integer |  |
| `rush_ypp` | double |  |
| `epa_rush` | double |  |
| `epa_rush_pp` | double |  |
| `ryoe` | double |  |
| `ttt` | double |  |
| `qbp` | integer |  |
| `qbp_pct` | double |  |
| `week_slug` | character |  |
| `opponent_team_id` | character | Unique identifier for the opponent team. |
| `game_result` | character | Game result for the player's team (`W`/`L`). |
| `final_score` | character |  |
| `is_home` | logical | Whether the subject team was the home team. |
| `game_id` | integer | Ten digit identifier for NFL game. |
| `fapi_game_id` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nfl_pro_team_offense_overview_week-example}

```python
nfl_pro_team_offense_overview_week(season=2024, season_type='REG')
```

_Last validated n/a._

## nfl_pro_team_defense_overview_season

GET /api/secured/stats/team-defense/overview/season — one row per team for the season — team defensive overview incl. EPA allowed per play and takeaways.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/team-defense/overview/season`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/team-defense/overview/season?season=2024&seasonType=REG&limit=500](https://pro.nfl.com/api/secured/stats/team-defense/overview/season?season=2024&seasonType=REG&limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |

### Returns {#nfl_pro_team_defense_overview_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | character | ESPN team id. |
| `gp` | integer | Games played. |
| `total` | integer | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `pass` | integer | Binary indicator if the play was a pass play (sacks and scrambles included). |
| `run` | integer | Expected Points Added on run plays |
| `pass_pct` | double |  |
| `ppg` | double | Points per game. |
| `yds` | integer |  |
| `ypg` | double |  |
| `ypp` | double |  |
| `td` | integer |  |
| `pass_td` | integer | Binary flag for a passing touchdown. |
| `rush_td` | integer | Binary flag for a rushing touchdown. |
| `epa` | double | Expected points added (EPA) by the posteam for the given play. |
| `epa_pp` | double |  |
| `pass_yds` | integer |  |
| `pass_ypg` | double |  |
| `pass_ypp` | double |  |
| `sacked_yds` | integer |  |
| `sacked_ypg` | double |  |
| `epa_pass` | double |  |
| `epa_pass_pp` | double |  |
| `rush_yds` | integer | Rushing yards gained on the play. |
| `rush_ypg` | double |  |
| `rush_ypp` | double |  |
| `epa_rush` | double |  |
| `epa_rush_pp` | double |  |
| `ryoe` | double |  |
| `ttt` | double |  |
| `qbp` | integer |  |
| `qbp_pct` | double |  |
| `interception` | integer | Binary indicator for if the pass was intercepted. |
| `forced_fumble` | integer |  |
| `fumble_recovered` | integer |  |
| `defensive_touchdown` | integer |  |
| `total_takeaways` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nfl_pro_team_defense_overview_season-example}

```python
nfl_pro_team_defense_overview_season(season=2024, season_type='REG')
```

_Last validated n/a._

## nfl_pro_team_defense_overview_week

GET /api/secured/stats/team-defense/overview/week — one row per team per week — team defensive overview incl. EPA allowed per play and takeaways.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/team-defense/overview/week`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/team-defense/overview/week?season=2024&seasonType=REG&limit=500](https://pro.nfl.com/api/secured/stats/team-defense/overview/week?season=2024&seasonType=REG&limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |

### Returns {#nfl_pro_team_defense_overview_week-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | character | ESPN team id. |
| `gp` | integer | Games played. |
| `total` | integer | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `pass` | integer | Binary indicator if the play was a pass play (sacks and scrambles included). |
| `run` | integer | Expected Points Added on run plays |
| `pass_pct` | double |  |
| `ppg` | integer | Points per game. |
| `yds` | integer |  |
| `ypg` | integer |  |
| `ypp` | double |  |
| `td` | integer |  |
| `pass_td` | integer | Binary flag for a passing touchdown. |
| `rush_td` | integer | Binary flag for a rushing touchdown. |
| `epa` | double | Expected points added (EPA) by the posteam for the given play. |
| `epa_pp` | double |  |
| `pass_yds` | integer |  |
| `pass_ypg` | integer |  |
| `pass_ypp` | double |  |
| `sacked_yds` | integer |  |
| `sacked_ypg` | integer |  |
| `epa_pass` | double |  |
| `epa_pass_pp` | double |  |
| `rush_yds` | integer | Rushing yards gained on the play. |
| `rush_ypg` | integer |  |
| `rush_ypp` | double |  |
| `epa_rush` | double |  |
| `epa_rush_pp` | double |  |
| `ryoe` | double |  |
| `ttt` | double |  |
| `qbp` | integer |  |
| `qbp_pct` | double |  |
| `interception` | integer | Binary indicator for if the pass was intercepted. |
| `forced_fumble` | integer |  |
| `fumble_recovered` | integer |  |
| `defensive_touchdown` | integer |  |
| `total_takeaways` | integer |  |
| `week_slug` | character |  |
| `opponent_team_id` | character | Unique identifier for the opponent team. |
| `game_result` | character | Game result for the player's team (`W`/`L`). |
| `final_score` | character |  |
| `is_home` | logical | Whether the subject team was the home team. |
| `game_id` | integer | Ten digit identifier for NFL game. |
| `fapi_game_id` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nfl_pro_team_defense_overview_week-example}

```python
nfl_pro_team_defense_overview_week(season=2024, season_type='REG')
```

_Last validated n/a._
