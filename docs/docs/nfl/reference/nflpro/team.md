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
| `team_id` | character | NFL team id as a zero-padded string (e.g. '0200'); casting it to a number drops the leading zero. |
| `gp` | integer | Games played in the season (and season type); the denominator of every per-game column. |
| `total` | integer | Offensive plays, season total (pass + run). |
| `pass` | integer | Pass plays, season total. |
| `run` | integer | Run plays, season total. |
| `pass_pct` | double | Share of plays that were passes (pass / total). |
| `ppg` | double | Points per game. |
| `yds` | integer | Yards from scrimmage, season total (pass_yds + rush_yds). |
| `ypg` | double | Yards per game (yds / gp). |
| `ypp` | double | Yards per play (yds / total). |
| `td` | integer | Touchdowns, season total. |
| `epa` | double | Total expected points added over all plays (epa_pass + epa_rush). |
| `epa_pp` | double | Expected points added per play (epa / total). |
| `pass_yds` | integer | Net passing yards, season total. |
| `pass_ypg` | double | Passing yards per game (pass_yds / gp). |
| `pass_ypp` | double | Passing yards per pass play (pass_yds / pass). |
| `sacked_yds` | integer | Yards lost to sacks, season total. |
| `sacked_ypg` | double | Sack yards lost per game (sacked_yds / gp). |
| `epa_pass` | double | Total expected points added on pass plays. |
| `epa_pass_pp` | double | Expected points added per pass play (epa_pass / pass). |
| `rush_yds` | integer | Rushing yards, season total. |
| `rush_ypg` | double | Rushing yards per game (rush_yds / gp). |
| `rush_ypp` | double | Rushing yards per run play (rush_yds / run). |
| `epa_rush` | double | Total expected points added on run plays. |
| `epa_rush_pp` | double | Expected points added per run play (epa_rush / run). |
| `ryoe` | double | Rushing yards over expected, season total, per Next Gen Stats. |
| `ttt` | double | Average time to throw in seconds on pass plays, per Next Gen Stats. |
| `qbp` | integer | Quarterback pressures on pass plays, season total. |
| `qbp_pct` | double | Pressure rate: share of pass plays with a quarterback pressure. |

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
| `team_id` | character | NFL team id as a zero-padded string (e.g. '0200'); casting it to a number drops the leading zero. |
| `gp` | integer | Games played in the season (and season type); the denominator of every per-game column. |
| `total` | integer | Offensive plays in the game (pass + run). |
| `pass` | integer | Pass plays in the game. |
| `run` | integer | Run plays in the game. |
| `pass_pct` | double | Share of plays that were passes (pass / total). |
| `ppg` | integer | Points per game. |
| `yds` | integer | Yards from scrimmage in the game (pass_yds + rush_yds). |
| `ypg` | integer | Yards per game (yds / gp). |
| `ypp` | double | Yards per play (yds / total). |
| `td` | integer | Touchdowns in the game. |
| `epa` | double | Total expected points added over all plays (epa_pass + epa_rush). |
| `epa_pp` | double | Expected points added per play (epa / total). |
| `pass_yds` | integer | Net passing yards in the game. |
| `pass_ypg` | integer | Passing yards per game (pass_yds / gp). |
| `pass_ypp` | double | Passing yards per pass play (pass_yds / pass). |
| `sacked_yds` | integer | Yards lost to sacks in the game. |
| `sacked_ypg` | integer | Sack yards lost per game (sacked_yds / gp). |
| `epa_pass` | double | Total expected points added on pass plays. |
| `epa_pass_pp` | double | Expected points added per pass play (epa_pass / pass). |
| `rush_yds` | integer | Rushing yards in the game. |
| `rush_ypg` | integer | Rushing yards per game (rush_yds / gp). |
| `rush_ypp` | double | Rushing yards per run play (rush_yds / run). |
| `epa_rush` | double | Total expected points added on run plays. |
| `epa_rush_pp` | double | Expected points added per run play (epa_rush / run). |
| `ryoe` | double | Rushing yards over expected in the game, per Next Gen Stats. |
| `ttt` | double | Average time to throw in seconds on pass plays, per Next Gen Stats. |
| `qbp` | integer | Quarterback pressures on pass plays in the game. |
| `qbp_pct` | double | Pressure rate: share of pass plays with a quarterback pressure. |
| `week_slug` | character | Week slug of the game (e.g. 'WEEK_1', 'WEEK_18'); the week scope of the row. |
| `opponent_team_id` | character | Opponent's NFL team id as a zero-padded string. |
| `game_result` | character | Result from the player's team's side: 'W', 'L' or 'T'. |
| `final_score` | character | Final score as 'own-opponent' (e.g. '19-32' for a 32-19 loss). |
| `is_home` | logical | Whether the player's team was the home team in the game. |
| `game_id` | integer | NFL game id as an integer (e.g. 2024090900, the date followed by a two-digit sequence). |
| `fapi_game_id` | character | NFL Football API (FAPI) UUID of the game, the id api.nfl.com uses for the same game. |

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
| `team_id` | character | NFL team id as a zero-padded string (e.g. '0200'); casting it to a number drops the leading zero. |
| `gp` | integer | Games played in the season (and season type); the denominator of every per-game column. |
| `total` | integer | Defensive plays faced, season total (pass + run). |
| `pass` | integer | Pass plays, season total, from the defense's side (allowed / faced). |
| `run` | integer | Run plays, season total, from the defense's side (allowed / faced). |
| `pass_pct` | double | Share of plays that were passes (pass / total), from the defense's side (allowed / faced). |
| `ppg` | double | Points per game, from the defense's side (allowed / faced). |
| `yds` | integer | Yards from scrimmage, season total (pass_yds + rush_yds), from the defense's side (allowed / faced). |
| `ypg` | double | Yards per game (yds / gp), from the defense's side (allowed / faced). |
| `ypp` | double | Yards per play (yds / total), from the defense's side (allowed / faced). |
| `td` | integer | Touchdowns, season total, from the defense's side (allowed / faced). |
| `pass_td` | integer | Passing touchdowns allowed, season total. |
| `rush_td` | integer | Rushing touchdowns allowed, season total. |
| `epa` | double | Total expected points added over all plays (epa_pass + epa_rush), from the defense's side (allowed / faced). |
| `epa_pp` | double | Expected points added per play (epa / total), from the defense's side (allowed / faced). |
| `pass_yds` | integer | Net passing yards, season total, from the defense's side (allowed / faced). |
| `pass_ypg` | double | Passing yards per game (pass_yds / gp), from the defense's side (allowed / faced). |
| `pass_ypp` | double | Passing yards per pass play (pass_yds / pass), from the defense's side (allowed / faced). |
| `sacked_yds` | integer | Yards lost to sacks, season total, from the defense's side (allowed / faced). |
| `sacked_ypg` | double | Sack yards lost per game (sacked_yds / gp), from the defense's side (allowed / faced). |
| `epa_pass` | double | Total expected points added on pass plays, from the defense's side (allowed / faced). |
| `epa_pass_pp` | double | Expected points added per pass play (epa_pass / pass), from the defense's side (allowed / faced). |
| `rush_yds` | integer | Rushing yards, season total, from the defense's side (allowed / faced). |
| `rush_ypg` | double | Rushing yards per game (rush_yds / gp), from the defense's side (allowed / faced). |
| `rush_ypp` | double | Rushing yards per run play (rush_yds / run), from the defense's side (allowed / faced). |
| `epa_rush` | double | Total expected points added on run plays, from the defense's side (allowed / faced). |
| `epa_rush_pp` | double | Expected points added per run play (epa_rush / run), from the defense's side (allowed / faced). |
| `ryoe` | double | Rushing yards over expected, season total, per Next Gen Stats, from the defense's side (allowed / faced). |
| `ttt` | double | Average time to throw in seconds on pass plays, per Next Gen Stats, from the defense's side (allowed / faced). |
| `qbp` | integer | Quarterback pressures on pass plays, season total, from the defense's side (allowed / faced). |
| `qbp_pct` | double | Pressure rate: share of pass plays with a quarterback pressure, from the defense's side (allowed / faced). |
| `interception` | integer | Interceptions made, season total. |
| `forced_fumble` | integer | Fumbles forced, season total. |
| `fumble_recovered` | integer | Opponent fumbles recovered, season total. |
| `defensive_touchdown` | integer | Defensive touchdowns scored, season total. |
| `total_takeaways` | integer | Takeaways, season total (interception + fumble_recovered). |

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
| `team_id` | character | NFL team id as a zero-padded string (e.g. '0200'); casting it to a number drops the leading zero. |
| `gp` | integer | Games played in the season (and season type); the denominator of every per-game column. |
| `total` | integer | Defensive plays faced in the game (pass + run). |
| `pass` | integer | Pass plays in the game, from the defense's side (allowed / faced). |
| `run` | integer | Run plays in the game, from the defense's side (allowed / faced). |
| `pass_pct` | double | Share of plays that were passes (pass / total), from the defense's side (allowed / faced). |
| `ppg` | integer | Points per game, from the defense's side (allowed / faced). |
| `yds` | integer | Yards from scrimmage in the game (pass_yds + rush_yds), from the defense's side (allowed / faced). |
| `ypg` | integer | Yards per game (yds / gp), from the defense's side (allowed / faced). |
| `ypp` | double | Yards per play (yds / total), from the defense's side (allowed / faced). |
| `td` | integer | Touchdowns in the game, from the defense's side (allowed / faced). |
| `pass_td` | integer | Passing touchdowns allowed in the game. |
| `rush_td` | integer | Rushing touchdowns allowed in the game. |
| `epa` | double | Total expected points added over all plays (epa_pass + epa_rush), from the defense's side (allowed / faced). |
| `epa_pp` | double | Expected points added per play (epa / total), from the defense's side (allowed / faced). |
| `pass_yds` | integer | Net passing yards in the game, from the defense's side (allowed / faced). |
| `pass_ypg` | integer | Passing yards per game (pass_yds / gp), from the defense's side (allowed / faced). |
| `pass_ypp` | double | Passing yards per pass play (pass_yds / pass), from the defense's side (allowed / faced). |
| `sacked_yds` | integer | Yards lost to sacks in the game, from the defense's side (allowed / faced). |
| `sacked_ypg` | integer | Sack yards lost per game (sacked_yds / gp), from the defense's side (allowed / faced). |
| `epa_pass` | double | Total expected points added on pass plays, from the defense's side (allowed / faced). |
| `epa_pass_pp` | double | Expected points added per pass play (epa_pass / pass), from the defense's side (allowed / faced). |
| `rush_yds` | integer | Rushing yards in the game, from the defense's side (allowed / faced). |
| `rush_ypg` | integer | Rushing yards per game (rush_yds / gp), from the defense's side (allowed / faced). |
| `rush_ypp` | double | Rushing yards per run play (rush_yds / run), from the defense's side (allowed / faced). |
| `epa_rush` | double | Total expected points added on run plays, from the defense's side (allowed / faced). |
| `epa_rush_pp` | double | Expected points added per run play (epa_rush / run), from the defense's side (allowed / faced). |
| `ryoe` | double | Rushing yards over expected in the game, per Next Gen Stats, from the defense's side (allowed / faced). |
| `ttt` | double | Average time to throw in seconds on pass plays, per Next Gen Stats, from the defense's side (allowed / faced). |
| `qbp` | integer | Quarterback pressures on pass plays in the game, from the defense's side (allowed / faced). |
| `qbp_pct` | double | Pressure rate: share of pass plays with a quarterback pressure, from the defense's side (allowed / faced). |
| `interception` | integer | Interceptions made in the game. |
| `forced_fumble` | integer | Fumbles forced in the game. |
| `fumble_recovered` | integer | Opponent fumbles recovered in the game. |
| `defensive_touchdown` | integer | Defensive touchdowns scored in the game. |
| `total_takeaways` | integer | Takeaways in the game (interception + fumble_recovered). |
| `week_slug` | character | Week slug of the game (e.g. 'WEEK_1', 'WEEK_18'); the week scope of the row. |
| `opponent_team_id` | character | Opponent's NFL team id as a zero-padded string. |
| `game_result` | character | Result from the player's team's side: 'W', 'L' or 'T'. |
| `final_score` | character | Final score as 'own-opponent' (e.g. '19-32' for a 32-19 loss). |
| `is_home` | logical | Whether the player's team was the home team in the game. |
| `game_id` | integer | NFL game id as an integer (e.g. 2024090900, the date followed by a two-digit sequence). |
| `fapi_game_id` | character | NFL Football API (FAPI) UUID of the game, the id api.nfl.com uses for the same game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nfl_pro_team_defense_overview_week-example}

```python
nfl_pro_team_defense_overview_week(season=2024, season_type='REG')
```

_Last validated n/a._
