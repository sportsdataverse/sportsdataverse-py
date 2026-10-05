---
title: "NFL — nflpro — Defense"
sidebar_label: "Defense"
sidebar_position: 1
description: "NFL — nflpro — Defense — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — nflpro — Defense

## nfl_pro_defense_overview_season

GET /api/secured/stats/defense/overview/season — one row per defender for the season — defensive overview incl. snap counts, pressures and havoc stops.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/defense/overview/season`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/defense/overview/season?season=2024&seasonType=REG](https://pro.nfl.com/api/secured/stats/defense/overview/season?season=2024&seasonType=REG)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |
| `qualifiedDefender` | `qualified` |  |  | `Y` | Restrict to players meeting the league qualifying threshold. |

### Returns {#nfl_pro_defense_overview_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `nfl_id` | character | NFL ID of player (this is used in Big Data Bowl Data) |
| `display_name` | character | Full name of player |
| `short_name` | character | Player short name (i.e. "F.Last") |
| `headshot` | character | NFL headshot url for player |
| `team_id` | character | ESPN team id. |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `position` | character | Primary position as reported by NFL.com |
| `position_group` | character | Postion group of player as listed by NFL |
| `ngs_position` | character | Primary position as reported by the NextGen stats API. |
| `ngs_position_group` | character | Position group of player as listed by Next Gen Stats |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
| `tg` | integer |  |
| `total_tg` | integer |  |
| `snap` | integer |  |
| `snap_pct` | double |  |
| `rd` | integer |  |
| `pr` | integer |  |
| `tck` | integer |  |
| `t_stop` | integer |  |
| `h_stop` | integer |  |
| `qbp` | integer |  |
| `qbp_r` | double |  |
| `sack` | double | Binary indicator for if the play ended in a sack. |
| `tgt_nd` | integer |  |
| `rec_nd` | integer |  |
| `rec_yds_nd` | integer |  |
| `rec_td_nd` | integer |  |
| `int` | integer | Binary flag for an interception. |
| `pass_rating_nd` | double |  |
| `qd` | logical |  |
| `game_snap` | integer |  |
| `team_snap` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nfl_pro_defense_overview_season-example}

```python
nfl_pro_defense_overview_season(season=2024, season_type='REG')
```

_Last validated n/a._

## nfl_pro_defense_overview_week

GET /api/secured/stats/defense/overview/week — one row per defender per week — defensive overview incl. snap counts, pressures and havoc stops.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/defense/overview/week`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/defense/overview/week?season=2024&seasonType=REG](https://pro.nfl.com/api/secured/stats/defense/overview/week?season=2024&seasonType=REG)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |
| `qualifiedDefender` | `qualified` |  |  | `Y` | Restrict to players meeting the league qualifying threshold. |
| `nflId` | `nfl_id` |  |  | `Y` | Optional player filter; omit for the whole league-week table. Note ``week`` is a path scope here, not a query param. |

### Returns {#nfl_pro_defense_overview_week-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `nfl_id` | character | NFL ID of player (this is used in Big Data Bowl Data) |
| `display_name` | character | Full name of player |
| `short_name` | character | Player short name (i.e. "F.Last") |
| `headshot` | character | NFL headshot url for player |
| `team_id` | character | ESPN team id. |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `position` | character | Primary position as reported by NFL.com |
| `position_group` | character | Postion group of player as listed by NFL |
| `ngs_position` | character | Primary position as reported by the NextGen stats API. |
| `ngs_position_group` | character | Position group of player as listed by Next Gen Stats |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
| `tg` | integer |  |
| `total_tg` | integer |  |
| `week_slug` | character |  |
| `game_id` | integer | Ten digit identifier for NFL game. |
| `fapi_game_id` | character |  |
| `opponent_team_id` | character | Unique identifier for the opponent team. |
| `is_home` | logical | Whether the subject team was the home team. |
| `final_score` | character |  |
| `game_result` | character | Game result for the player's team (`W`/`L`). |
| `snap` | integer |  |
| `snap_pct` | double |  |
| `rd` | integer |  |
| `pr` | integer |  |
| `tck` | integer |  |
| `t_stop` | integer |  |
| `h_stop` | integer |  |
| `qbp` | integer |  |
| `qbp_r` | double |  |
| `sack` | integer | Binary indicator for if the play ended in a sack. |
| `tgt_nd` | integer |  |
| `rec_nd` | integer |  |
| `rec_yds_nd` | integer |  |
| `rec_td_nd` | integer |  |
| `int` | integer | Binary flag for an interception. |
| `pass_rating_nd` | double |  |
| `qd` | logical |  |
| `game_snap` | integer |  |
| `team_snap` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nfl_pro_defense_overview_week-example}

```python
nfl_pro_defense_overview_week(season=2024, season_type='REG')
```

_Last validated n/a._

## nfl_pro_defense_nearest_season

GET /api/secured/stats/defense/nearest/season — one row per defender for the season — nearest-defender coverage incl. targets, catch rate and CROE allowed.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/defense/nearest/season`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/defense/nearest/season?season=2024&seasonType=REG](https://pro.nfl.com/api/secured/stats/defense/nearest/season?season=2024&seasonType=REG)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |
| `qualifiedDefender` | `qualified` |  |  | `Y` | Restrict to players meeting the league qualifying threshold. |

### Returns {#nfl_pro_defense_nearest_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `nfl_id` | character | NFL ID of player (this is used in Big Data Bowl Data) |
| `display_name` | character | Full name of player |
| `short_name` | character | Player short name (i.e. "F.Last") |
| `headshot` | character | NFL headshot url for player |
| `team_id` | character | ESPN team id. |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `position` | character | Primary position as reported by NFL.com |
| `position_group` | character | Postion group of player as listed by NFL |
| `ngs_position` | character | Primary position as reported by the NextGen stats API. |
| `ngs_position_group` | character | Position group of player as listed by Next Gen Stats |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
| `tg` | integer |  |
| `total_tg` | integer |  |
| `cov` | integer |  |
| `cov_nd` | integer |  |
| `tgt_nd` | integer |  |
| `rec_nd` | integer |  |
| `rec_yds_nd` | integer |  |
| `rec_td_nd` | integer |  |
| `int` | integer | Binary flag for an interception. |
| `pass_rating_nd` | double |  |
| `catch_nd` | double |  |
| `croe_nd` | double |  |
| `tgt_epa_nd` | double |  |
| `tgt_r_nd` | double |  |
| `sep` | double |  |
| `twf_pct` | double |  |
| `bh_pct` | double |  |
| `yacpr_nd` | double |  |
| `qd` | logical |  |
| `game_snap` | integer |  |
| `team_snap` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nfl_pro_defense_nearest_season-example}

```python
nfl_pro_defense_nearest_season(season=2024, season_type='REG')
```

_Last validated n/a._

## nfl_pro_defense_nearest_week

GET /api/secured/stats/defense/nearest/week — one row per defender per week — nearest-defender coverage incl. targets, catch rate and CROE allowed.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/defense/nearest/week`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/defense/nearest/week?season=2024&seasonType=REG](https://pro.nfl.com/api/secured/stats/defense/nearest/week?season=2024&seasonType=REG)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |
| `qualifiedDefender` | `qualified` |  |  | `Y` | Restrict to players meeting the league qualifying threshold. |
| `nflId` | `nfl_id` |  |  | `Y` | Optional player filter; omit for the whole league-week table. Note ``week`` is a path scope here, not a query param. |

### Returns {#nfl_pro_defense_nearest_week-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `nfl_id` | character | NFL ID of player (this is used in Big Data Bowl Data) |
| `display_name` | character | Full name of player |
| `short_name` | character | Player short name (i.e. "F.Last") |
| `headshot` | character | NFL headshot url for player |
| `team_id` | character | ESPN team id. |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `position` | character | Primary position as reported by NFL.com |
| `position_group` | character | Postion group of player as listed by NFL |
| `ngs_position` | character | Primary position as reported by the NextGen stats API. |
| `ngs_position_group` | character | Position group of player as listed by Next Gen Stats |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
| `tg` | integer |  |
| `total_tg` | integer |  |
| `week_slug` | character |  |
| `game_id` | integer | Ten digit identifier for NFL game. |
| `fapi_game_id` | character |  |
| `opponent_team_id` | character | Unique identifier for the opponent team. |
| `is_home` | logical | Whether the subject team was the home team. |
| `final_score` | character |  |
| `game_result` | character | Game result for the player's team (`W`/`L`). |
| `cov` | integer |  |
| `cov_nd` | integer |  |
| `tgt_nd` | integer |  |
| `rec_nd` | integer |  |
| `rec_yds_nd` | integer |  |
| `rec_td_nd` | integer |  |
| `int` | integer | Binary flag for an interception. |
| `pass_rating_nd` | double |  |
| `catch_nd` | double |  |
| `croe_nd` | double |  |
| `tgt_epa_nd` | double |  |
| `tgt_r_nd` | double |  |
| `sep` | double |  |
| `twf_pct` | integer |  |
| `bh_pct` | double |  |
| `yacpr_nd` | double |  |
| `qd` | logical |  |
| `game_snap` | integer |  |
| `team_snap` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nfl_pro_defense_nearest_week-example}

```python
nfl_pro_defense_nearest_week(season=2024, season_type='REG')
```

_Last validated n/a._
