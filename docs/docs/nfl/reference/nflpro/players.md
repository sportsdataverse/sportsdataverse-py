---
title: "NFL — nflpro — Players"
sidebar_label: "Players"
sidebar_position: 3
description: "NFL — nflpro — Players — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — nflpro — Players

## nfl_pro_players_offense_passing_season

GET /api/secured/stats/players-offense/passing/season — one row per passer for the season — quarterback passing incl. Next Gen time-to-throw, aggressiveness and CPOE.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/players-offense/passing/season`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/players-offense/passing/season?season=2024&seasonType=REG&limit=500](https://pro.nfl.com/api/secured/stats/players-offense/passing/season?season=2024&seasonType=REG&limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |
| `qualifiedPasser` | `qualified` |  |  | `Y` | Restrict to players meeting the league qualifying threshold. |

### Returns {#nfl_pro_players_offense_passing_season-returns}

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

### Example {#nfl_pro_players_offense_passing_season-example}

```python
nfl_pro_players_offense_passing_season(season=2024, season_type='REG')
```

_Last validated n/a._

## nfl_pro_players_offense_passing_week

GET /api/secured/stats/players-offense/passing/week — one row per passer per week — quarterback passing incl. Next Gen time-to-throw, aggressiveness and CPOE.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/players-offense/passing/week`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/players-offense/passing/week?season=2024&seasonType=REG&limit=500](https://pro.nfl.com/api/secured/stats/players-offense/passing/week?season=2024&seasonType=REG&limit=500)

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

### Returns {#nfl_pro_players_offense_passing_week-returns}

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

### Example {#nfl_pro_players_offense_passing_week-example}

```python
nfl_pro_players_offense_passing_week(season=2024, season_type='REG')
```

_Last validated n/a._

## nfl_pro_players_offense_rushing_season

GET /api/secured/stats/players-offense/rushing/season — one row per rusher for the season — rushing incl. Next Gen efficiency, yards over expected and defenders-in-box.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/players-offense/rushing/season`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/players-offense/rushing/season?season=2024&seasonType=REG&limit=500](https://pro.nfl.com/api/secured/stats/players-offense/rushing/season?season=2024&seasonType=REG&limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |
| `qualifiedRusher` | `qualified` |  |  | `Y` | Restrict to players meeting the league qualifying threshold. |

### Returns {#nfl_pro_players_offense_rushing_season-returns}

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

### Example {#nfl_pro_players_offense_rushing_season-example}

```python
nfl_pro_players_offense_rushing_season(season=2024, season_type='REG')
```

_Last validated n/a._

## nfl_pro_players_offense_rushing_week

GET /api/secured/stats/players-offense/rushing/week — one row per rusher per week — rushing incl. Next Gen efficiency, yards over expected and defenders-in-box.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/players-offense/rushing/week`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/players-offense/rushing/week?season=2024&seasonType=REG&limit=500](https://pro.nfl.com/api/secured/stats/players-offense/rushing/week?season=2024&seasonType=REG&limit=500)

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

### Returns {#nfl_pro_players_offense_rushing_week-returns}

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

### Example {#nfl_pro_players_offense_rushing_week-example}

```python
nfl_pro_players_offense_rushing_week(season=2024, season_type='REG')
```

_Last validated n/a._

## nfl_pro_players_offense_receiving_season

GET /api/secured/stats/players-offense/receiving/season — one row per receiver for the season — receiving incl. Next Gen separation, cushion and catch rate over expected.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/players-offense/receiving/season`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/players-offense/receiving/season?season=2024&seasonType=REG&limit=500](https://pro.nfl.com/api/secured/stats/players-offense/receiving/season?season=2024&seasonType=REG&limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season, as the STARTING year (2024 = the 2024-25 NFL season). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `limit` | `limit` |  |  | `Y` | Page size. Responses truncate silently at this many rows; the getter pages on ``offset`` until it has them all. |
| `offset` | `offset` |  |  | `Y` | Zero-based row offset. The getter pages on this automatically; set it only to fetch a specific slice. |
| `sortKey` | `sort_key` |  |  | `Y` | Field name to sort by, e.g. ``epa``. |
| `sortValue` | `sort_value` |  |  | `Y` | Sort direction: ``ASC`` or ``DESC``. |
| `qualifiedReceiver` | `qualified` |  |  | `Y` | Restrict to players meeting the league qualifying threshold. |

### Returns {#nfl_pro_players_offense_receiving_season-returns}

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

### Example {#nfl_pro_players_offense_receiving_season-example}

```python
nfl_pro_players_offense_receiving_season(season=2024, season_type='REG')
```

_Last validated n/a._

## nfl_pro_players_offense_receiving_week

GET /api/secured/stats/players-offense/receiving/week — one row per receiver per week — receiving incl. Next Gen separation, cushion and catch rate over expected.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/players-offense/receiving/week`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/players-offense/receiving/week?season=2024&seasonType=REG&limit=500](https://pro.nfl.com/api/secured/stats/players-offense/receiving/week?season=2024&seasonType=REG&limit=500)

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

### Returns {#nfl_pro_players_offense_receiving_week-returns}

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

### Example {#nfl_pro_players_offense_receiving_week-example}

```python
nfl_pro_players_offense_receiving_week(season=2024, season_type='REG')
```

_Last validated n/a._
