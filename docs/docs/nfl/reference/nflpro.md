---
title: NFL — nflpro
sidebar_label: nflpro
description: "NFL — nflpro — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 11
---
# NFL — nflpro

`sportsdataverse.nfl` — 16 endpoints.

## `nfl_pro_players_offense_passing_season`

GET /api/secured/stats/players-offense/passing/season — one row per passer for the season — quarterback passing incl. Next Gen time-to-throw, aggressiveness and CPOE.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/players-offense/passing/season`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/players-offense/passing/season?season=2024&seasonType=REG](https://pro.nfl.com/api/secured/stats/players-offense/passing/season?season=2024&seasonType=REG)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |
| `qualifiedPasser` | `qualified` |  |  | `Y` | Restrict to players meeting the league qualifying threshold. |

### Returns

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
| `cmp` | integer |  |
| `att` | integer |  |
| `yds` | integer |  |
| `td` | integer |  |
| `int` | integer | Binary flag for an interception. |
| `rating` | double | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `ypa` | double |  |
| `cmp_pct` | double |  |
| `sack` | integer | Binary indicator for if the play ended in a sack. |
| `x_cmp` | double |  |
| `cpoe` | double | For a single pass play this is 1 - cp when the pass was completed or 0 - cp when the pass was incomplete. Analyzed for a whole game or season an indicator for the passer how much over or under expectation his completion percentage was. |
| `db` | integer |  |
| `epa` | double | Expected points added (EPA) by the posteam for the given play. |
| `epa_db` | double |  |
| `avg_ttt` | double |  |
| `avg_ttp` | double |  |
| `avg_tts` | double |  |
| `qbp` | integer |  |
| `qbp_r` | double |  |
| `blitz_r` | double |  |
| `drop` | integer |  |
| `drop_r` | double |  |
| `ay` | double | Acceleration of the pitch in the y-direction at y=50 ft (ft/s^2). |
| `yac` | double |  |
| `x_yac` | double |  |
| `yac_pct` | double |  |
| `ay_att` | double |  |
| `avg_sep` | double |  |
| `deep_att_pct` | double |  |
| `tw_att_pct` | double |  |
| `pa_db_pct` | double |  |
| `qp` | logical |  |
| `cmp_pg` | double |  |
| `att_pg` | double |  |
| `yds_pg` | double |  |
| `td_pg` | double |  |
| `int_pg` | double |  |
| `sack_pg` | double |  |
| `db_pg` | double |  |
| `epa_pg` | double |  |
| `qbp_pg` | double |  |
| `drop_pg` | double |  |
| `tw_att_pg` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nfl_pro_players_offense_passing_season(season=2024, season_type='REG')
```

_Last validated n/a._

## `nfl_pro_players_offense_passing_week`

GET /api/secured/stats/players-offense/passing/week — one row per passer per week — quarterback passing incl. Next Gen time-to-throw, aggressiveness and CPOE.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/players-offense/passing/week`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/players-offense/passing/week?season=2024&seasonType=REG](https://pro.nfl.com/api/secured/stats/players-offense/passing/week?season=2024&seasonType=REG)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |
| `qualifiedPasser` | `qualified` |  |  | `Y` | Restrict to players meeting the league qualifying threshold. |
| `nflId` | `nfl_id` |  |  | `Y` | Optional player filter; omit for the whole league-week table. Note ``week`` is a path scope here, not a query param. |

### Returns

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
| `cmp` | integer |  |
| `att` | integer |  |
| `yds` | integer |  |
| `td` | integer |  |
| `int` | integer | Binary flag for an interception. |
| `rating` | double | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `ypa` | double |  |
| `cmp_pct` | double |  |
| `sack` | integer | Binary indicator for if the play ended in a sack. |
| `x_cmp` | double |  |
| `cpoe` | double | For a single pass play this is 1 - cp when the pass was completed or 0 - cp when the pass was incomplete. Analyzed for a whole game or season an indicator for the passer how much over or under expectation his completion percentage was. |
| `db` | integer |  |
| `epa` | double | Expected points added (EPA) by the posteam for the given play. |
| `epa_db` | double |  |
| `avg_ttt` | double |  |
| `avg_ttp` | double |  |
| `avg_tts` | double |  |
| `qbp` | integer |  |
| `qbp_r` | double |  |
| `blitz_r` | double |  |
| `drop` | integer |  |
| `drop_r` | double |  |
| `ay` | double | Acceleration of the pitch in the y-direction at y=50 ft (ft/s^2). |
| `yac` | double |  |
| `x_yac` | double |  |
| `yac_pct` | double |  |
| `ay_att` | double |  |
| `avg_sep` | double |  |
| `deep_att_pct` | double |  |
| `tw_att_pct` | double |  |
| `pa_db_pct` | double |  |
| `qp` | logical |  |
| `cmp_pg` | integer |  |
| `att_pg` | integer |  |
| `yds_pg` | integer |  |
| `td_pg` | integer |  |
| `int_pg` | integer |  |
| `sack_pg` | integer |  |
| `db_pg` | integer |  |
| `epa_pg` | double |  |
| `qbp_pg` | integer |  |
| `drop_pg` | integer |  |
| `tw_att_pg` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nfl_pro_players_offense_passing_week(season=2024, season_type='REG')
```

_Last validated n/a._

## `nfl_pro_players_offense_rushing_season`

GET /api/secured/stats/players-offense/rushing/season — one row per rusher for the season — rushing incl. Next Gen efficiency, yards over expected and defenders-in-box.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/players-offense/rushing/season`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/players-offense/rushing/season?season=2024&seasonType=REG](https://pro.nfl.com/api/secured/stats/players-offense/rushing/season?season=2024&seasonType=REG)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |
| `qualifiedRusher` | `qualified` |  |  | `Y` | Restrict to players meeting the league qualifying threshold. |

### Returns

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
| `att` | integer |  |
| `yds` | integer |  |
| `td` | integer |  |
| `ypc` | double |  |
| `epa` | double | Expected points added (EPA) by the posteam for the given play. |
| `epa_att` | double |  |
| `x_ry` | double |  |
| `x_ypc` | double |  |
| `ryoe` | double |  |
| `ryoe_att` | double |  |
| `yaco` | double |  |
| `yaco_att` | double |  |
| `ybco` | double |  |
| `ybco_att` | double |  |
| `success` | double | Binary indicator whether epa > 0 in the given play. |
| `fum` | integer |  |
| `lost` | integer |  |
| `rush10_p_yds` | integer |  |
| `rush15_p_mph` | integer |  |
| `rush20_p_mph` | integer |  |
| `eff` | double | Eff. |
| `in_t_pct` | double |  |
| `st_box_pct` | double |  |
| `under_pct` | double |  |
| `qr` | logical |  |
| `att_pg` | double |  |
| `yds_pg` | double |  |
| `td_pg` | double |  |
| `epa_pg` | double |  |
| `x_ry_pg` | double |  |
| `ryoe_pg` | double |  |
| `yaco_pg` | double |  |
| `ybco_pg` | double |  |
| `fum_pg` | double |  |
| `lost_pg` | double |  |
| `rush10_p_yds_pg` | double |  |
| `rush15_p_mph_pg` | double |  |
| `rush20_p_mph_pg` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nfl_pro_players_offense_rushing_season(season=2024, season_type='REG')
```

_Last validated n/a._

## `nfl_pro_players_offense_rushing_week`

GET /api/secured/stats/players-offense/rushing/week — one row per rusher per week — rushing incl. Next Gen efficiency, yards over expected and defenders-in-box.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/players-offense/rushing/week`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/players-offense/rushing/week?season=2024&seasonType=REG](https://pro.nfl.com/api/secured/stats/players-offense/rushing/week?season=2024&seasonType=REG)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |
| `qualifiedRusher` | `qualified` |  |  | `Y` | Restrict to players meeting the league qualifying threshold. |
| `nflId` | `nfl_id` |  |  | `Y` | Optional player filter; omit for the whole league-week table. Note ``week`` is a path scope here, not a query param. |

### Returns

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
| `att` | integer |  |
| `yds` | integer |  |
| `td` | integer |  |
| `ypc` | double |  |
| `epa` | double | Expected points added (EPA) by the posteam for the given play. |
| `epa_att` | double |  |
| `x_ry` | double |  |
| `x_ypc` | double |  |
| `ryoe` | double |  |
| `ryoe_att` | double |  |
| `yaco` | double |  |
| `yaco_att` | double |  |
| `ybco` | double |  |
| `ybco_att` | double |  |
| `success` | double | Binary indicator whether epa > 0 in the given play. |
| `fum` | integer |  |
| `lost` | integer |  |
| `rush10_p_yds` | integer |  |
| `rush15_p_mph` | integer |  |
| `rush20_p_mph` | integer |  |
| `eff` | double | Eff. |
| `in_t_pct` | double |  |
| `st_box_pct` | integer |  |
| `under_pct` | double |  |
| `qr` | logical |  |
| `att_pg` | integer |  |
| `yds_pg` | integer |  |
| `td_pg` | integer |  |
| `epa_pg` | double |  |
| `x_ry_pg` | double |  |
| `ryoe_pg` | double |  |
| `yaco_pg` | double |  |
| `ybco_pg` | double |  |
| `fum_pg` | integer |  |
| `lost_pg` | integer |  |
| `rush10_p_yds_pg` | integer |  |
| `rush15_p_mph_pg` | integer |  |
| `rush20_p_mph_pg` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nfl_pro_players_offense_rushing_week(season=2024, season_type='REG')
```

_Last validated n/a._

## `nfl_pro_players_offense_receiving_season`

GET /api/secured/stats/players-offense/receiving/season — one row per receiver for the season — receiving incl. Next Gen separation, cushion and catch rate over expected.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/players-offense/receiving/season`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/players-offense/receiving/season?season=2024&seasonType=REG](https://pro.nfl.com/api/secured/stats/players-offense/receiving/season?season=2024&seasonType=REG)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |
| `qualifiedReceiver` | `qualified` |  |  | `Y` | Restrict to players meeting the league qualifying threshold. |

### Returns

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
| `rt` | integer |  |
| `tgt` | integer |  |
| `rec` | integer |  |
| `yds` | integer |  |
| `td` | integer |  |
| `int` | integer | Binary flag for an interception. |
| `rating` | double | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `catch` | double |  |
| `x_catch` | double |  |
| `croe` | double |  |
| `yds_rec` | double |  |
| `yds_rt` | double |  |
| `epa` | double | Expected points added (EPA) by the posteam for the given play. |
| `epa_tgt` | double |  |
| `epa_rt` | double |  |
| `drop` | integer |  |
| `drop_tgt` | double |  |
| `yac` | integer |  |
| `x_yac` | integer |  |
| `yacoe` | integer |  |
| `yac_rec` | double |  |
| `avg_sep` | double |  |
| `ay` | double | Acceleration of the pitch in the y-direction at y=50 ft (ft/s^2). |
| `ay_tgt` | double |  |
| `tgt_rt` | double |  |
| `avg_rt_dep` | double |  |
| `ez_tgt` | integer |  |
| `ez_rec` | integer |  |
| `deep_tgt_pct` | double |  |
| `tw_pct` | double |  |
| `qr` | logical |  |
| `rt_pg` | double |  |
| `tgt_pg` | double |  |
| `rec_pg` | double |  |
| `yds_pg` | double |  |
| `td_pg` | double |  |
| `int_pg` | double |  |
| `epa_pg` | double |  |
| `drop_pg` | double |  |
| `yac_pg` | double |  |
| `x_yac_pg` | double |  |
| `yacoe_pg` | double |  |
| `ay_pg` | double |  |
| `ez_tgt_pg` | double |  |
| `ez_rec_pg` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nfl_pro_players_offense_receiving_season(season=2024, season_type='REG')
```

_Last validated n/a._

## `nfl_pro_players_offense_receiving_week`

GET /api/secured/stats/players-offense/receiving/week — one row per receiver per week — receiving incl. Next Gen separation, cushion and catch rate over expected.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/players-offense/receiving/week`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/players-offense/receiving/week?season=2024&seasonType=REG](https://pro.nfl.com/api/secured/stats/players-offense/receiving/week?season=2024&seasonType=REG)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |
| `qualifiedReceiver` | `qualified` |  |  | `Y` | Restrict to players meeting the league qualifying threshold. |
| `nflId` | `nfl_id` |  |  | `Y` | Optional player filter; omit for the whole league-week table. Note ``week`` is a path scope here, not a query param. |

### Returns

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
| `rt` | integer |  |
| `tgt` | integer |  |
| `rec` | integer |  |
| `yds` | integer |  |
| `td` | integer |  |
| `int` | integer | Binary flag for an interception. |
| `rating` | integer | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `catch` | integer |  |
| `x_catch` | integer |  |
| `croe` | integer |  |
| `yds_rec` | integer |  |
| `yds_rt` | integer |  |
| `epa` | integer | Expected points added (EPA) by the posteam for the given play. |
| `epa_tgt` | integer |  |
| `epa_rt` | integer |  |
| `drop` | integer |  |
| `drop_tgt` | integer |  |
| `yac` | integer |  |
| `x_yac` | integer |  |
| `yacoe` | integer |  |
| `yac_rec` | integer |  |
| `avg_sep` | integer |  |
| `ay` | integer | Acceleration of the pitch in the y-direction at y=50 ft (ft/s^2). |
| `ay_tgt` | integer |  |
| `tgt_rt` | integer |  |
| `avg_rt_dep` | integer |  |
| `ez_tgt` | integer |  |
| `ez_rec` | integer |  |
| `deep_tgt_pct` | integer |  |
| `tw_pct` | integer |  |
| `qr` | logical |  |
| `rt_pg` | integer |  |
| `tgt_pg` | integer |  |
| `rec_pg` | integer |  |
| `yds_pg` | integer |  |
| `td_pg` | integer |  |
| `int_pg` | integer |  |
| `epa_pg` | integer |  |
| `drop_pg` | integer |  |
| `yac_pg` | integer |  |
| `x_yac_pg` | integer |  |
| `yacoe_pg` | integer |  |
| `ay_pg` | integer |  |
| `ez_tgt_pg` | integer |  |
| `ez_rec_pg` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nfl_pro_players_offense_receiving_week(season=2024, season_type='REG')
```

_Last validated n/a._

## `nfl_pro_defense_overview_season`

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

### Returns

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

### Example

```python
nfl_pro_defense_overview_season(season=2024, season_type='REG')
```

_Last validated n/a._

## `nfl_pro_defense_overview_week`

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

### Returns

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

### Example

```python
nfl_pro_defense_overview_week(season=2024, season_type='REG')
```

_Last validated n/a._

## `nfl_pro_defense_nearest_season`

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

### Returns

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

### Example

```python
nfl_pro_defense_nearest_season(season=2024, season_type='REG')
```

_Last validated n/a._

## `nfl_pro_defense_nearest_week`

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

### Returns

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

### Example

```python
nfl_pro_defense_nearest_week(season=2024, season_type='REG')
```

_Last validated n/a._

## `nfl_pro_team_offense_overview_season`

GET /api/secured/stats/team-offense/overview/season — one row per team for the season — team offensive overview incl. EPA per play, pass and rush splits.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/team-offense/overview/season`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/team-offense/overview/season?season=2024&seasonType=REG](https://pro.nfl.com/api/secured/stats/team-offense/overview/season?season=2024&seasonType=REG)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |

### Returns

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

### Example

```python
nfl_pro_team_offense_overview_season(season=2024, season_type='REG')
```

_Last validated n/a._

## `nfl_pro_team_offense_overview_week`

GET /api/secured/stats/team-offense/overview/week — one row per team per week — team offensive overview incl. EPA per play, pass and rush splits.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/team-offense/overview/week`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/team-offense/overview/week?season=2024&seasonType=REG](https://pro.nfl.com/api/secured/stats/team-offense/overview/week?season=2024&seasonType=REG)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |

### Returns

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

### Example

```python
nfl_pro_team_offense_overview_week(season=2024, season_type='REG')
```

_Last validated n/a._

## `nfl_pro_team_defense_overview_season`

GET /api/secured/stats/team-defense/overview/season — one row per team for the season — team defensive overview incl. EPA allowed per play and takeaways.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/team-defense/overview/season`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/team-defense/overview/season?season=2024&seasonType=REG](https://pro.nfl.com/api/secured/stats/team-defense/overview/season?season=2024&seasonType=REG)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |

### Returns

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

### Example

```python
nfl_pro_team_defense_overview_season(season=2024, season_type='REG')
```

_Last validated n/a._

## `nfl_pro_team_defense_overview_week`

GET /api/secured/stats/team-defense/overview/week — one row per team per week — team defensive overview incl. EPA allowed per play and takeaways.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/team-defense/overview/week`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/team-defense/overview/week?season=2024&seasonType=REG](https://pro.nfl.com/api/secured/stats/team-defense/overview/week?season=2024&seasonType=REG)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |

### Returns

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

### Example

```python
nfl_pro_team_defense_overview_week(season=2024, season_type='REG')
```

_Last validated n/a._

## `nfl_pro_fantasy_season`

GET /api/secured/stats/fantasy/season — one row per player for the season — fantasy points, opportunity and usage.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/fantasy/season`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/fantasy/season?season=2024&seasonType=REG](https://pro.nfl.com/api/secured/stats/fantasy/season?season=2024&seasonType=REG)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `nflId` | `nfl_id` |  |  | `Y` | Optional player filter. |
| `positionGroup` | `position_group` |  |  | `Y` | Optional position-group filter, e.g. ``QB``. Optional on this season scope — it is the ``game`` scope that requires it. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``fpHalfPPR``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |

### Returns

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
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
| `o_snap` | integer |  |
| `o_snap3rd` | integer |  |
| `o_tm_snap` | integer |  |
| `o_snap_pg` | double |  |
| `pt_pct` | double |  |
| `pt_pct3rd` | double |  |
| `pass_cmp` | integer |  |
| `pass_att` | integer |  |
| `pass_yd` | integer |  |
| `pass_td` | integer | Binary flag for a passing touchdown. |
| `pass_int` | integer |  |
| `pass_two_pt_conv` | integer |  |
| `pass_db` | integer |  |
| `pass_cmp_pct` | double |  |
| `pass_exp_cmp_pct` | double |  |
| `pass_cpoe` | double |  |
| `pass_rating` | double |  |
| `pass_avg_ttt` | double |  |
| `pass_ay_pa` | double |  |
| `pass_deep_att_pct` | double |  |
| `pass_yac_pct` | double |  |
| `pass_qbp` | integer |  |
| `pass_qbp_pct` | double |  |
| `pass_sack` | integer |  |
| `pass_sack_pg` | double |  |
| `pass_cmp_pg` | double |  |
| `pass_att_pg` | double |  |
| `pass_yd_pg` | double |  |
| `pass_td_pg` | double |  |
| `pass_int_pg` | double |  |
| `pass_db_pg` | double |  |
| `rush_att` | integer |  |
| `rush_yd` | integer |  |
| `rush_td` | integer | Binary flag for a rushing touchdown. |
| `rush_two_pt_conv` | integer |  |
| `rush_exp_yd` | integer |  |
| `rush_ryoe` | integer |  |
| `rush_att_pg` | double |  |
| `rush_yd_pg` | double |  |
| `rush_yd_pa` | double |  |
| `rush_yaco` | double |  |
| `rush_ybco` | double |  |
| `rush_yaco_pa` | double |  |
| `rush_ybco_pa` | double |  |
| `rush_stuffed` | integer |  |
| `rush_td_pg` | double |  |
| `scr_rush_att` | integer |  |
| `scr_rush_yd` | integer |  |
| `scr_rush_td` | integer |  |
| `scr_rush_pct` | double |  |
| `design_rush_att` | integer |  |
| `design_rush_yd` | integer |  |
| `design_rush_td` | integer |  |
| `rush_rz_att` | integer |  |
| `rush_gl_att` | integer |  |
| `rush10_plus_yd` | integer |  |
| `rec_rt` | integer |  |
| `rec_tgt` | integer |  |
| `rec_rec` | integer |  |
| `rec_yd` | integer |  |
| `rec_td` | integer |  |
| `rec_two_pt_conv` | integer |  |
| `rec_rt_pg` | double |  |
| `rec_tgt_pg` | double |  |
| `rec_rec_pg` | double |  |
| `rec_yd_pg` | double |  |
| `rec_td_pg` | double |  |
| `rec_catch_pct` | double |  |
| `rec_ay_share` | double |  |
| `rec_tgt_rate` | double |  |
| `rec_tgt_share` | double |  |
| `rec_rt_part_pct` | double |  |
| `rec_tgt_quick` | integer |  |
| `rec_tgt_play_act` | integer |  |
| `rec_ez_tgt` | integer |  |
| `rec_ez_rec` | integer |  |
| `rec_rz_tgt` | integer |  |
| `rec_ay_tgt` | integer |  |
| `rec_ay_rec` | integer |  |
| `rec_ay_unrealized` | integer |  |
| `rec_ay_pt` | double |  |
| `rec_tgt_ay10_plus` | integer |  |
| `rec_yd_p_rt` | double |  |
| `rec_yd_pt` | double |  |
| `rec_yd_pr` | double |  |
| `rec_yac` | integer |  |
| `rec_exp_yac` | integer |  |
| `rec_yacoe` | integer |  |
| `kick_xp_att` | integer |  |
| `kick_xp_made` | integer |  |
| `kick_fg_att` | integer |  |
| `kick_fg_made` | integer |  |
| `kick_fg_miss` | integer |  |
| `kick_fg_made_less40` | integer |  |
| `kick_fg_made40_to49` | integer |  |
| `kick_fg_made50_to59` | integer |  |
| `kick_fg_made60_plus` | integer |  |
| `misc_kickoff_ret_td` | integer |  |
| `misc_punt_ret_td` | integer |  |
| `misc_fum_rec_td` | integer |  |
| `misc_fum_lost` | integer |  |
| `misc_fum` | integer |  |
| `misc_int_ret_td` | integer |  |
| `misc_fum_ret_td` | integer |  |
| `misc_blk_punt_fg_ret_td` | integer |  |
| `misc_two_pt_ret` | integer |  |
| `misc_one_pt_safety` | integer |  |
| `o_touch` | integer |  |
| `o_opp` | integer |  |
| `o_opp_pg` | double |  |
| `o_miss_tkl_forced` | integer |  |
| `o_miss_tkl_forced_pct` | double |  |
| `o_tm_db` | integer |  |
| `o_tm_pass_pct` | double |  |
| `o_tm_ppg` | double |  |
| `o_tm_yd_pg` | double |  |
| `rz_opp` | integer |  |
| `fp_std` | double |  |
| `fp_half_ppr` | double |  |
| `fp_ppr` | double |  |
| `fp_pass` | double |  |
| `fp_rush` | double |  |
| `fp_rec_std` | double |  |
| `fp_rec_half_ppr` | double |  |
| `fp_rec_ppr` | double |  |
| `fp_kick` | integer |  |
| `fp_misc` | integer |  |
| `fp_pg_std` | double |  |
| `fp_pg_half_ppr` | double |  |
| `fp_pgppr` | double |  |
| `fp_pos_rk_std` | integer |  |
| `fp_pos_rk_half_ppr` | integer |  |
| `fp_pos_rk_ppr` | integer |  |
| `fp_pos_rk_lbl_std` | character |  |
| `fp_pos_rk_lbl_half_ppr` | character |  |
| `fp_pos_rk_lbl_ppr` | character |  |
| `fp_ps_std` | double |  |
| `fp_ps_half_ppr` | double |  |
| `fp_psppr` | double |  |
| `fp_p_rt_std` | double |  |
| `fp_p_rt_half_ppr` | double |  |
| `fp_p_rt_ppr` | double |  |
| `fp_pt_std` | double |  |
| `fp_pt_half_ppr` | double |  |
| `fp_ptppr` | double |  |
| `fp_po_std` | double |  |
| `fp_po_half_ppr` | double |  |
| `fp_poppr` | double |  |
| `top5_qb_wk_std` | integer |  |
| `top12_qb_wk_std` | integer |  |
| `top12_rb_wk_std` | integer |  |
| `top24_rb_wk_std` | integer |  |
| `top12_wr_wk_std` | integer |  |
| `top24_wr_wk_std` | integer |  |
| `top36_wr_wk_std` | integer |  |
| `top5_te_wk_std` | integer |  |
| `top12_te_wk_std` | integer |  |
| `top5_k_wk_std` | integer |  |
| `top12_k_wk_std` | integer |  |
| `top5_qb_wk_half_ppr` | integer |  |
| `top12_qb_wk_half_ppr` | integer |  |
| `top12_rb_wk_half_ppr` | integer |  |
| `top24_rb_wk_half_ppr` | integer |  |
| `top12_wr_wk_half_ppr` | integer |  |
| `top24_wr_wk_half_ppr` | integer |  |
| `top36_wr_wk_half_ppr` | integer |  |
| `top5_te_wk_half_ppr` | integer |  |
| `top12_te_wk_half_ppr` | integer |  |
| `top5_k_wk_half_ppr` | integer |  |
| `top12_k_wk_half_ppr` | integer |  |
| `top5_qb_wk_ppr` | integer |  |
| `top12_qb_wk_ppr` | integer |  |
| `top12_rb_wk_ppr` | integer |  |
| `top24_rb_wk_ppr` | integer |  |
| `top12_wr_wk_ppr` | integer |  |
| `top24_wr_wk_ppr` | integer |  |
| `top36_wr_wk_ppr` | integer |  |
| `top5_te_wk_ppr` | integer |  |
| `top12_te_wk_ppr` | integer |  |
| `top5_k_wk_ppr` | integer |  |
| `top12_k_wk_ppr` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nfl_pro_fantasy_season(season=2024, season_type='REG')
```

_Last validated n/a._

## `nfl_pro_fantasy_game`

GET /api/secured/stats/fantasy/game — one row per player-game — fantasy scoring by game. Requires `position_group`.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/fantasy/game`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/fantasy/game?season=2024&seasonType=REG&positionGroup=QB](https://pro.nfl.com/api/secured/stats/fantasy/game?season=2024&seasonType=REG&positionGroup=QB)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `nflId` | `nfl_id` |  |  | `Y` | Optional player filter. |
| `positionGroup` | `position_group` |  | `Y` |  | Position group, e.g. ``QB``. **Required**: this scope returns HTTP 500 without it, so it is a required argument rather than an optional filter. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``fpHalfPPR``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |

### Returns

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
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
| `week_slug` | character |  |
| `game_id` | integer | Ten digit identifier for NFL game. |
| `fapi_game_id` | character |  |
| `opponent_team_id` | character | Unique identifier for the opponent team. |
| `is_home` | logical | Whether the subject team was the home team. |
| `final_score` | character |  |
| `game_result` | character | Game result for the player's team (`W`/`L`). |
| `o_snap` | integer |  |
| `o_snap3rd` | integer |  |
| `o_tm_snap` | integer |  |
| `o_snap_pg` | integer |  |
| `pt_pct` | double |  |
| `pt_pct3rd` | double |  |
| `pass_cmp` | integer |  |
| `pass_att` | integer |  |
| `pass_yd` | integer |  |
| `pass_td` | integer | Binary flag for a passing touchdown. |
| `pass_int` | integer |  |
| `pass_two_pt_conv` | integer |  |
| `pass_db` | integer |  |
| `pass_cmp_pct` | double |  |
| `pass_exp_cmp_pct` | double |  |
| `pass_cpoe` | double |  |
| `pass_rating` | double |  |
| `pass_avg_ttt` | double |  |
| `pass_ay_pa` | double |  |
| `pass_deep_att_pct` | double |  |
| `pass_yac_pct` | double |  |
| `pass_qbp` | integer |  |
| `pass_qbp_pct` | double |  |
| `pass_sack` | integer |  |
| `pass_sack_pg` | integer |  |
| `pass_cmp_pg` | integer |  |
| `pass_att_pg` | integer |  |
| `pass_yd_pg` | integer |  |
| `pass_td_pg` | integer |  |
| `pass_int_pg` | integer |  |
| `pass_db_pg` | integer |  |
| `rush_att` | integer |  |
| `rush_yd` | integer |  |
| `rush_td` | integer | Binary flag for a rushing touchdown. |
| `rush_two_pt_conv` | integer |  |
| `rush_exp_yd` | integer |  |
| `rush_ryoe` | integer |  |
| `rush_att_pg` | integer |  |
| `rush_yd_pg` | integer |  |
| `rush_yd_pa` | double |  |
| `rush_yaco` | double |  |
| `rush_ybco` | double |  |
| `rush_yaco_pa` | double |  |
| `rush_ybco_pa` | double |  |
| `rush_stuffed` | integer |  |
| `rush_td_pg` | integer |  |
| `scr_rush_att` | integer |  |
| `scr_rush_yd` | integer |  |
| `scr_rush_td` | integer |  |
| `scr_rush_pct` | double |  |
| `design_rush_att` | integer |  |
| `design_rush_yd` | integer |  |
| `design_rush_td` | integer |  |
| `rush_rz_att` | integer |  |
| `rush_gl_att` | integer |  |
| `rush10_plus_yd` | integer |  |
| `rec_rt` | integer |  |
| `rec_tgt` | integer |  |
| `rec_rec` | integer |  |
| `rec_yd` | integer |  |
| `rec_td` | integer |  |
| `rec_two_pt_conv` | integer |  |
| `rec_rt_pg` | integer |  |
| `rec_tgt_pg` | integer |  |
| `rec_rec_pg` | integer |  |
| `rec_yd_pg` | integer |  |
| `rec_td_pg` | integer |  |
| `rec_catch_pct` | integer |  |
| `rec_ay_share` | integer |  |
| `rec_tgt_rate` | integer |  |
| `rec_tgt_share` | integer |  |
| `rec_rt_part_pct` | double |  |
| `rec_tgt_quick` | integer |  |
| `rec_tgt_play_act` | integer |  |
| `rec_ez_tgt` | integer |  |
| `rec_ez_rec` | integer |  |
| `rec_rz_tgt` | integer |  |
| `rec_ay_tgt` | integer |  |
| `rec_ay_rec` | integer |  |
| `rec_ay_unrealized` | integer |  |
| `rec_ay_pt` | integer |  |
| `rec_tgt_ay10_plus` | integer |  |
| `rec_yd_p_rt` | integer |  |
| `rec_yd_pt` | integer |  |
| `rec_yd_pr` | integer |  |
| `rec_yac` | integer |  |
| `rec_exp_yac` | integer |  |
| `rec_yacoe` | integer |  |
| `kick_xp_att` | integer |  |
| `kick_xp_made` | integer |  |
| `kick_fg_att` | integer |  |
| `kick_fg_made` | integer |  |
| `kick_fg_miss` | integer |  |
| `kick_fg_made_less40` | integer |  |
| `kick_fg_made40_to49` | integer |  |
| `kick_fg_made50_to59` | integer |  |
| `kick_fg_made60_plus` | integer |  |
| `misc_kickoff_ret_td` | integer |  |
| `misc_punt_ret_td` | integer |  |
| `misc_fum_rec_td` | integer |  |
| `misc_fum_lost` | integer |  |
| `misc_fum` | integer |  |
| `misc_int_ret_td` | integer |  |
| `misc_fum_ret_td` | integer |  |
| `misc_blk_punt_fg_ret_td` | integer |  |
| `misc_two_pt_ret` | integer |  |
| `misc_one_pt_safety` | integer |  |
| `o_touch` | integer |  |
| `o_opp` | integer |  |
| `o_opp_pg` | integer |  |
| `o_miss_tkl_forced` | integer |  |
| `o_miss_tkl_forced_pct` | integer |  |
| `o_tm_db` | integer |  |
| `o_tm_pass_pct` | double |  |
| `o_tm_ppg` | integer |  |
| `o_tm_yd_pg` | integer |  |
| `rz_opp` | integer |  |
| `fp_std` | double |  |
| `fp_half_ppr` | double |  |
| `fp_ppr` | double |  |
| `fp_pass` | double |  |
| `fp_rush` | double |  |
| `fp_rec_std` | integer |  |
| `fp_rec_half_ppr` | integer |  |
| `fp_rec_ppr` | integer |  |
| `fp_kick` | integer |  |
| `fp_misc` | integer |  |
| `fp_pg_std` | double |  |
| `fp_pg_half_ppr` | double |  |
| `fp_pgppr` | double |  |
| `fp_pos_rk_std` | integer |  |
| `fp_pos_rk_half_ppr` | integer |  |
| `fp_pos_rk_ppr` | integer |  |
| `fp_pos_rk_lbl_std` | character |  |
| `fp_pos_rk_lbl_half_ppr` | character |  |
| `fp_pos_rk_lbl_ppr` | character |  |
| `fp_ps_std` | double |  |
| `fp_ps_half_ppr` | double |  |
| `fp_psppr` | double |  |
| `fp_p_rt_std` | double |  |
| `fp_p_rt_half_ppr` | double |  |
| `fp_p_rt_ppr` | double |  |
| `fp_pt_std` | integer |  |
| `fp_pt_half_ppr` | integer |  |
| `fp_ptppr` | integer |  |
| `fp_po_std` | double |  |
| `fp_po_half_ppr` | double |  |
| `fp_poppr` | double |  |
| `top5_qb_wk_std` | integer |  |
| `top12_qb_wk_std` | integer |  |
| `top12_rb_wk_std` | integer |  |
| `top24_rb_wk_std` | integer |  |
| `top12_wr_wk_std` | integer |  |
| `top24_wr_wk_std` | integer |  |
| `top36_wr_wk_std` | integer |  |
| `top5_te_wk_std` | integer |  |
| `top12_te_wk_std` | integer |  |
| `top5_k_wk_std` | integer |  |
| `top12_k_wk_std` | integer |  |
| `top5_qb_wk_half_ppr` | integer |  |
| `top12_qb_wk_half_ppr` | integer |  |
| `top12_rb_wk_half_ppr` | integer |  |
| `top24_rb_wk_half_ppr` | integer |  |
| `top12_wr_wk_half_ppr` | integer |  |
| `top24_wr_wk_half_ppr` | integer |  |
| `top36_wr_wk_half_ppr` | integer |  |
| `top5_te_wk_half_ppr` | integer |  |
| `top12_te_wk_half_ppr` | integer |  |
| `top5_k_wk_half_ppr` | integer |  |
| `top12_k_wk_half_ppr` | integer |  |
| `top5_qb_wk_ppr` | integer |  |
| `top12_qb_wk_ppr` | integer |  |
| `top12_rb_wk_ppr` | integer |  |
| `top24_rb_wk_ppr` | integer |  |
| `top12_wr_wk_ppr` | integer |  |
| `top24_wr_wk_ppr` | integer |  |
| `top36_wr_wk_ppr` | integer |  |
| `top5_te_wk_ppr` | integer |  |
| `top12_te_wk_ppr` | integer |  |
| `top5_k_wk_ppr` | integer |  |
| `top12_k_wk_ppr` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nfl_pro_fantasy_game(season=2024, season_type='REG', position_group='QB')
```

_Last validated n/a._
