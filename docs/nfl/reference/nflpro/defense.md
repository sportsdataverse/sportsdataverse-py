# NFL — nflpro — Defense

> NFL — nflpro — Defense — function reference in sdv-py, the SportsDataverse Python package.

## nfl_pro_defense_overview_season

GET /api/secured/stats/defense/overview/season — one row per defender for the season — defensive overview incl. snap counts, pressures and havoc stops.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/defense/overview/season`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/defense/overview/season?season=2024&seasonType=REG&limit=500](https://pro.nfl.com/api/secured/stats/defense/overview/season?season=2024&seasonType=REG&limit=500)

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
| `nfl_id` | character | NFL player id (`nflId`) as a string; the key the wrappers accept as `nfl_id`. |
| `display_name` | character | Player's full display name (e.g. 'Aaron Rodgers'). |
| `short_name` | character | Player's abbreviated name, first initial and surname (e.g. 'A.Rodgers'). |
| `headshot` | character | NFL headshot image URL template; the `{formatInstructions}` token must be replaced with a Cloudinary transform (e.g. `t_headshot_desktop`) before use. |
| `team_id` | character | NFL team id as a zero-padded string (e.g. '0200', '3430'); casting it to a number drops the leading zero. |
| `jersey_number` | integer | Jersey number the player wore in the season. |
| `position` | character | Roster position abbreviation (e.g. QB, WR, CB). |
| `position_group` | character | Position group the roster position rolls up to (e.g. QB, RB, WR, TE, DB). |
| `ngs_position` | character | Position Next Gen Stats assigns from tracking data; null for players NGS has not classified. |
| `ngs_position_group` | character | Position group of ngs_position; null when ngs_position is null. |
| `gp` | integer | Games played in the season (and season type) -- the denominator of every `*_pg` per-game column. |
| `gs` | integer | Games started in the season. |
| `tg` | integer | Games the player's team played while he was on the roster; equals gp in sampled data. |
| `total_tg` | integer | Total games the player's team played in the season type (17 for a full regular season). |
| `snap` | integer | Defensive snaps played, season total. |
| `snap_pct` | double | Share of the team's defensive snaps the player played (snap / team_snap). |
| `rd` | integer |  |
| `pr` | integer | Pass-rush snaps, season total; the denominator of qbp_r. |
| `tck` | integer | Tackles, season total. |
| `t_stop` | integer |  |
| `h_stop` | integer |  |
| `qbp` | integer | Quarterback pressures, season total. |
| `qbp_r` | double | Pressure rate: pressures per pass-rush snap (qbp / pr). |
| `sack` | double | Sacks, season total (a count, not a per-play flag). |
| `tgt_nd` | integer | Targets on which the player was the nearest defender, season total, per Next Gen Stats. |
| `rec_nd` | integer | Receptions allowed as the nearest defender, season total. |
| `rec_yds_nd` | integer | Receiving yards allowed as the nearest defender, season total. |
| `rec_td_nd` | integer | Receiving touchdowns allowed as the nearest defender, season total. |
| `int` | integer | Interceptions made, season total (a count, not a per-play flag). |
| `pass_rating_nd` | double | Passer rating allowed on targets where the player was the nearest defender. |
| `qd` | logical | Whether the defender meets the league qualifying threshold for the table (the `qualified` filter). |
| `game_snap` | integer | Defensive snaps the player played, season total; equals snap on the overview table. |
| `team_snap` | integer | Defensive snaps the player's team played, season total; the denominator of snap_pct. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nfl_pro_defense_overview_season-example}

```python
nfl_pro_defense_overview_season(season=2024, season_type='REG')
```

_Last validated n/a._

## nfl_pro_defense_overview_week

GET /api/secured/stats/defense/overview/week — one row per defender per week — defensive overview incl. snap counts, pressures and havoc stops.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/defense/overview/week`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/defense/overview/week?season=2024&seasonType=REG&limit=500](https://pro.nfl.com/api/secured/stats/defense/overview/week?season=2024&seasonType=REG&limit=500)

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
| `nfl_id` | character | NFL player id (`nflId`) as a string; the key the wrappers accept as `nfl_id`. |
| `display_name` | character | Player's full display name (e.g. 'Aaron Rodgers'). |
| `short_name` | character | Player's abbreviated name, first initial and surname (e.g. 'A.Rodgers'). |
| `headshot` | character | NFL headshot image URL template; the `{formatInstructions}` token must be replaced with a Cloudinary transform (e.g. `t_headshot_desktop`) before use. |
| `team_id` | character | NFL team id as a zero-padded string (e.g. '0200', '3430'); casting it to a number drops the leading zero. |
| `jersey_number` | integer | Jersey number the player wore in the season. |
| `position` | character | Roster position abbreviation (e.g. QB, WR, CB). |
| `position_group` | character | Position group the roster position rolls up to (e.g. QB, RB, WR, TE, DB). |
| `ngs_position` | character | Position Next Gen Stats assigns from tracking data; null for players NGS has not classified. |
| `ngs_position_group` | character | Position group of ngs_position; null when ngs_position is null. |
| `gp` | integer | Games played in the season (and season type) -- the denominator of every `*_pg` per-game column. |
| `gs` | integer | Games started in the season. |
| `tg` | integer | Games the player's team played while he was on the roster; equals gp in sampled data. |
| `total_tg` | integer | Total games the player's team played in the season type (17 for a full regular season). |
| `week_slug` | character | Week slug of the game (e.g. 'WEEK_1', 'WEEK_18'); the week scope of the row. |
| `game_id` | integer | NFL game id as an integer (e.g. 2024090900, the date followed by a two-digit sequence). |
| `fapi_game_id` | character | NFL Football API (FAPI) UUID of the game, the id api.nfl.com uses for the same game. |
| `opponent_team_id` | character | Opponent's NFL team id as a zero-padded string. |
| `is_home` | logical | Whether the player's team was the home team in the game. |
| `final_score` | character | Final score as 'own-opponent' (e.g. '19-32' for a 32-19 loss). |
| `game_result` | character | Result from the player's team's side: 'W', 'L' or 'T'. |
| `snap` | integer | Defensive snaps played in the game. |
| `snap_pct` | double | Share of the team's defensive snaps the player played (snap / team_snap). |
| `rd` | integer |  |
| `pr` | integer | Pass-rush snaps in the game; the denominator of qbp_r. |
| `tck` | integer | Tackles in the game. |
| `t_stop` | integer |  |
| `h_stop` | integer |  |
| `qbp` | integer | Quarterback pressures in the game. |
| `qbp_r` | double | Pressure rate: pressures per pass-rush snap (qbp / pr). |
| `sack` | integer | Sacks in the game (a count, not a per-play flag). |
| `tgt_nd` | integer | Targets on which the player was the nearest defender in the game, per Next Gen Stats. |
| `rec_nd` | integer | Receptions allowed as the nearest defender in the game. |
| `rec_yds_nd` | integer | Receiving yards allowed as the nearest defender in the game. |
| `rec_td_nd` | integer | Receiving touchdowns allowed as the nearest defender in the game. |
| `int` | integer | Interceptions made in the game (a count, not a per-play flag). |
| `pass_rating_nd` | double | Passer rating allowed on targets where the player was the nearest defender. |
| `qd` | logical | Whether the defender meets the league qualifying threshold for the table (the `qualified` filter). |
| `game_snap` | integer | Defensive snaps the player played in the game; equals snap on the overview table. |
| `team_snap` | integer | Defensive snaps the player's team played in the game; the denominator of snap_pct. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nfl_pro_defense_overview_week-example}

```python
nfl_pro_defense_overview_week(season=2024, season_type='REG')
```

_Last validated n/a._

## nfl_pro_defense_nearest_season

GET /api/secured/stats/defense/nearest/season — one row per defender for the season — nearest-defender coverage incl. targets, catch rate and CROE allowed.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/defense/nearest/season`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/defense/nearest/season?season=2024&seasonType=REG&limit=500](https://pro.nfl.com/api/secured/stats/defense/nearest/season?season=2024&seasonType=REG&limit=500)

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
| `nfl_id` | character | NFL player id (`nflId`) as a string; the key the wrappers accept as `nfl_id`. |
| `display_name` | character | Player's full display name (e.g. 'Aaron Rodgers'). |
| `short_name` | character | Player's abbreviated name, first initial and surname (e.g. 'A.Rodgers'). |
| `headshot` | character | NFL headshot image URL template; the `{formatInstructions}` token must be replaced with a Cloudinary transform (e.g. `t_headshot_desktop`) before use. |
| `team_id` | character | NFL team id as a zero-padded string (e.g. '0200', '3430'); casting it to a number drops the leading zero. |
| `jersey_number` | integer | Jersey number the player wore in the season. |
| `position` | character | Roster position abbreviation (e.g. QB, WR, CB). |
| `position_group` | character | Position group the roster position rolls up to (e.g. QB, RB, WR, TE, DB). |
| `ngs_position` | character | Position Next Gen Stats assigns from tracking data; null for players NGS has not classified. |
| `ngs_position_group` | character | Position group of ngs_position; null when ngs_position is null. |
| `gp` | integer | Games played in the season (and season type) -- the denominator of every `*_pg` per-game column. |
| `gs` | integer | Games started in the season. |
| `tg` | integer | Games the player's team played while he was on the roster; equals gp in sampled data. |
| `total_tg` | integer | Total games the player's team played in the season type (17 for a full regular season). |
| `cov` | integer |  |
| `cov_nd` | integer |  |
| `tgt_nd` | integer | Targets on which the player was the nearest defender, season total, per Next Gen Stats. |
| `rec_nd` | integer | Receptions allowed as the nearest defender, season total. |
| `rec_yds_nd` | integer | Receiving yards allowed as the nearest defender, season total. |
| `rec_td_nd` | integer | Receiving touchdowns allowed as the nearest defender, season total. |
| `int` | integer | Interceptions made, season total (a count, not a per-play flag). |
| `pass_rating_nd` | double | Passer rating allowed on targets where the player was the nearest defender. |
| `catch_nd` | double | Catch rate allowed as the nearest defender (rec_nd / tgt_nd), as a fraction. |
| `croe_nd` | double | Catch rate over expected allowed as the nearest defender (actual minus expected catch rate), per Next Gen Stats. |
| `tgt_epa_nd` | double | Total expected points added allowed on targets where the player was the nearest defender. |
| `tgt_r_nd` | double | Target rate as the nearest defender: share of coverage snaps on which the receiver he covered was targeted. |
| `sep` | double | Average separation in yards allowed at pass arrival as the nearest defender, per Next Gen Stats. |
| `twf_pct` | double |  |
| `bh_pct` | double |  |
| `yacpr_nd` | double |  |
| `qd` | logical | Whether the defender meets the league qualifying threshold for the table (the `qualified` filter). |
| `game_snap` | integer | Defensive snaps the player played, season total; equals snap on the overview table. |
| `team_snap` | integer | Defensive snaps the player's team played, season total; the denominator of snap_pct. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nfl_pro_defense_nearest_season-example}

```python
nfl_pro_defense_nearest_season(season=2024, season_type='REG')
```

_Last validated n/a._

## nfl_pro_defense_nearest_week

GET /api/secured/stats/defense/nearest/week — one row per defender per week — nearest-defender coverage incl. targets, catch rate and CROE allowed.

**Endpoint URL:** `GET https://pro.nfl.com/api/secured/stats/defense/nearest/week`

**Valid URL:** [https://pro.nfl.com/api/secured/stats/defense/nearest/week?season=2024&seasonType=REG&limit=500](https://pro.nfl.com/api/secured/stats/defense/nearest/week?season=2024&seasonType=REG&limit=500)

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
| `nfl_id` | character | NFL player id (`nflId`) as a string; the key the wrappers accept as `nfl_id`. |
| `display_name` | character | Player's full display name (e.g. 'Aaron Rodgers'). |
| `short_name` | character | Player's abbreviated name, first initial and surname (e.g. 'A.Rodgers'). |
| `headshot` | character | NFL headshot image URL template; the `{formatInstructions}` token must be replaced with a Cloudinary transform (e.g. `t_headshot_desktop`) before use. |
| `team_id` | character | NFL team id as a zero-padded string (e.g. '0200', '3430'); casting it to a number drops the leading zero. |
| `jersey_number` | integer | Jersey number the player wore in the season. |
| `position` | character | Roster position abbreviation (e.g. QB, WR, CB). |
| `position_group` | character | Position group the roster position rolls up to (e.g. QB, RB, WR, TE, DB). |
| `ngs_position` | character | Position Next Gen Stats assigns from tracking data; null for players NGS has not classified. |
| `ngs_position_group` | character | Position group of ngs_position; null when ngs_position is null. |
| `gp` | integer | Games played in the season (and season type) -- the denominator of every `*_pg` per-game column. |
| `gs` | integer | Games started in the season. |
| `tg` | integer | Games the player's team played while he was on the roster; equals gp in sampled data. |
| `total_tg` | integer | Total games the player's team played in the season type (17 for a full regular season). |
| `week_slug` | character | Week slug of the game (e.g. 'WEEK_1', 'WEEK_18'); the week scope of the row. |
| `game_id` | integer | NFL game id as an integer (e.g. 2024090900, the date followed by a two-digit sequence). |
| `fapi_game_id` | character | NFL Football API (FAPI) UUID of the game, the id api.nfl.com uses for the same game. |
| `opponent_team_id` | character | Opponent's NFL team id as a zero-padded string. |
| `is_home` | logical | Whether the player's team was the home team in the game. |
| `final_score` | character | Final score as 'own-opponent' (e.g. '19-32' for a 32-19 loss). |
| `game_result` | character | Result from the player's team's side: 'W', 'L' or 'T'. |
| `cov` | integer |  |
| `cov_nd` | integer |  |
| `tgt_nd` | integer | Targets on which the player was the nearest defender in the game, per Next Gen Stats. |
| `rec_nd` | integer | Receptions allowed as the nearest defender in the game. |
| `rec_yds_nd` | integer | Receiving yards allowed as the nearest defender in the game. |
| `rec_td_nd` | integer | Receiving touchdowns allowed as the nearest defender in the game. |
| `int` | integer | Interceptions made in the game (a count, not a per-play flag). |
| `pass_rating_nd` | double | Passer rating allowed on targets where the player was the nearest defender. |
| `catch_nd` | double | Catch rate allowed as the nearest defender (rec_nd / tgt_nd), as a fraction. |
| `croe_nd` | double | Catch rate over expected allowed as the nearest defender (actual minus expected catch rate), per Next Gen Stats. |
| `tgt_epa_nd` | double | Total expected points added allowed on targets where the player was the nearest defender. |
| `tgt_r_nd` | double | Target rate as the nearest defender: share of coverage snaps on which the receiver he covered was targeted. |
| `sep` | double | Average separation in yards allowed at pass arrival as the nearest defender, per Next Gen Stats. |
| `twf_pct` | integer |  |
| `bh_pct` | double |  |
| `yacpr_nd` | double |  |
| `qd` | logical | Whether the defender meets the league qualifying threshold for the table (the `qualified` filter). |
| `game_snap` | integer | Defensive snaps the player played in the game; equals snap on the overview table. |
| `team_snap` | integer | Defensive snaps the player's team played in the game; the denominator of snap_pct. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nfl_pro_defense_nearest_week-example}

```python
nfl_pro_defense_nearest_week(season=2024, season_type='REG')
```

_Last validated n/a._
