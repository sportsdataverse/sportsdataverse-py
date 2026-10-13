# NFL — nflpro — Players

> NFL — nflpro — Players — function reference in sdv-py, the SportsDataverse Python package.

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
| `cmp` | integer | Pass completions, season total. |
| `att` | integer | Pass attempts, season total. |
| `yds` | integer | Passing yards, season total. |
| `td` | integer | Passing touchdowns, season total. |
| `int` | integer | Interceptions thrown, season total (a count, not a per-play flag). |
| `rating` | double | NFL passer rating for the season (the standard 0-158.3 formula; equals nflverse `passer_rating`). |
| `ypa` | double | Yards per pass attempt (yds / att). |
| `cmp_pct` | double | Completion percentage as a fraction (cmp / att, e.g. 0.63). |
| `sack` | integer | Times sacked, season total (a count, not a per-play flag). |
| `x_cmp` | double | Expected completion percentage as a fraction, per Next Gen Stats (nflverse `expected_completion_percentage` / 100). |
| `cpoe` | double | Completion percentage over expected as a fraction: cmp_pct - x_cmp (nflverse `completion_percentage_above_expectation` / 100). |
| `db` | integer | Dropbacks, season total (attempts plus sacks and scrambles); the denominator of qbp_r and epa_db. |
| `epa` | double | Total expected points added on the passer's dropbacks. |
| `epa_db` | double | Expected points added per dropback (epa / db). |
| `avg_ttt` | double | Average time to throw in seconds, snap to release, per Next Gen Stats (nflverse `avg_time_to_throw`). |
| `avg_ttp` | double |  |
| `avg_tts` | double |  |
| `qbp` | integer | Dropbacks on which the passer was pressured, season total. |
| `qbp_r` | double | Pressure rate: share of dropbacks on which the passer was pressured (qbp / db). |
| `blitz_r` | double | Blitz rate: share of the passer's dropbacks on which the defense blitzed, per Next Gen Stats. |
| `drop` | integer | Passes dropped by the passer's receivers, season total. |
| `drop_r` | double | Drop rate: drops per pass attempt (drop / att). |
| `ay` | double | Total intended air yards on pass attempts; ay_att is this per attempt. |
| `yac` | double | Total yards after the catch gained on the passer's completions. |
| `x_yac` | double | Total expected yards after the catch on the passer's completions, per Next Gen Stats. |
| `yac_pct` | double | Share of passing yards gained after the catch (yac / yds). |
| `ay_att` | double | Average intended air yards per pass attempt (nflverse `avg_intended_air_yards`). |
| `avg_sep` | double | Average separation in yards between the targeted receiver and the nearest defender at pass arrival on the passer's targets, per Next Gen Stats. |
| `deep_att_pct` | double | Share of pass attempts NGS classifies as deep throws. |
| `tw_att_pct` | double | Aggressiveness: share of pass attempts into a tight window (defender within a yard of the receiver), per Next Gen Stats (nflverse `aggressiveness` / 100). |
| `pa_db_pct` | double | Share of dropbacks that used play action. |
| `qp` | logical | Whether the passer meets the league qualifying-attempts threshold (the `qualified` filter). |
| `cmp_pg` | double | Completions per game (cmp / gp). |
| `att_pg` | double | Pass attempts per game (att / gp). |
| `yds_pg` | double | Passing yards per game (yds / gp). |
| `td_pg` | double | Passing touchdowns per game (td / gp). |
| `int_pg` | double | Interceptions thrown per game (int / gp). |
| `sack_pg` | double | Sacks taken per game (sack / gp). |
| `db_pg` | double | Dropbacks per game (db / gp). |
| `epa_pg` | double | Expected points added per game (epa / gp). |
| `qbp_pg` | double | Pressured dropbacks per game (qbp / gp). |
| `drop_pg` | double | Receiver drops per game (drop / gp). |
| `tw_att_pg` | double | Tight-window pass attempts per game; tw_att_pct = tw_att_pg / att_pg. |

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
| `cmp` | integer | Pass completions in the game. |
| `att` | integer | Pass attempts in the game. |
| `yds` | integer | Passing yards in the game. |
| `td` | integer | Passing touchdowns in the game. |
| `int` | integer | Interceptions thrown in the game (a count, not a per-play flag). |
| `rating` | double | NFL passer rating for the season (the standard 0-158.3 formula; equals nflverse `passer_rating`). |
| `ypa` | double | Yards per pass attempt (yds / att). |
| `cmp_pct` | double | Completion percentage as a fraction (cmp / att, e.g. 0.63). |
| `sack` | integer | Times sacked in the game (a count, not a per-play flag). |
| `x_cmp` | double | Expected completion percentage as a fraction, per Next Gen Stats (nflverse `expected_completion_percentage` / 100). |
| `cpoe` | double | Completion percentage over expected as a fraction: cmp_pct - x_cmp (nflverse `completion_percentage_above_expectation` / 100). |
| `db` | integer | Dropbacks in the game (attempts plus sacks and scrambles); the denominator of qbp_r and epa_db. |
| `epa` | double | Total expected points added on the passer's dropbacks. |
| `epa_db` | double | Expected points added per dropback (epa / db). |
| `avg_ttt` | double | Average time to throw in seconds, snap to release, per Next Gen Stats (nflverse `avg_time_to_throw`). |
| `avg_ttp` | double |  |
| `avg_tts` | double |  |
| `qbp` | integer | Dropbacks on which the passer was pressured in the game. |
| `qbp_r` | double | Pressure rate: share of dropbacks on which the passer was pressured (qbp / db). |
| `blitz_r` | double | Blitz rate: share of the passer's dropbacks on which the defense blitzed, per Next Gen Stats. |
| `drop` | integer | Passes dropped by the passer's receivers in the game. |
| `drop_r` | double | Drop rate: drops per pass attempt (drop / att). |
| `ay` | double | Total intended air yards on pass attempts; ay_att is this per attempt. |
| `yac` | double | Total yards after the catch gained on the passer's completions. |
| `x_yac` | double | Total expected yards after the catch on the passer's completions, per Next Gen Stats. |
| `yac_pct` | double | Share of passing yards gained after the catch (yac / yds). |
| `ay_att` | double | Average intended air yards per pass attempt (nflverse `avg_intended_air_yards`). |
| `avg_sep` | double | Average separation in yards between the targeted receiver and the nearest defender at pass arrival on the passer's targets, per Next Gen Stats. |
| `deep_att_pct` | double | Share of pass attempts NGS classifies as deep throws. |
| `tw_att_pct` | double | Aggressiveness: share of pass attempts into a tight window (defender within a yard of the receiver), per Next Gen Stats (nflverse `aggressiveness` / 100). |
| `pa_db_pct` | double | Share of dropbacks that used play action. |
| `qp` | logical | Whether the passer meets the league qualifying-attempts threshold (the `qualified` filter). |
| `cmp_pg` | integer | Completions per game (cmp / gp). |
| `att_pg` | integer | Pass attempts per game (att / gp). |
| `yds_pg` | integer | Passing yards per game (yds / gp). |
| `td_pg` | integer | Passing touchdowns per game (td / gp). |
| `int_pg` | integer | Interceptions thrown per game (int / gp). |
| `sack_pg` | integer | Sacks taken per game (sack / gp). |
| `db_pg` | integer | Dropbacks per game (db / gp). |
| `epa_pg` | double | Expected points added per game (epa / gp). |
| `qbp_pg` | integer | Pressured dropbacks per game (qbp / gp). |
| `drop_pg` | integer | Receiver drops per game (drop / gp). |
| `tw_att_pg` | integer | Tight-window pass attempts per game; tw_att_pct = tw_att_pg / att_pg. |

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
| `att` | integer | Rush attempts, season total. |
| `yds` | integer | Rushing yards, season total. |
| `td` | integer | Rushing touchdowns, season total. |
| `ypc` | double | Yards per carry (yds / att). |
| `epa` | double | Total expected points added on the player's rush attempts. |
| `epa_att` | double | Expected points added per rush attempt (epa / att). |
| `x_ry` | double | Expected rushing yards, season total, per Next Gen Stats (nflverse `expected_rush_yards`); yds = x_ry + ryoe. |
| `x_ypc` | double | Expected yards per carry (x_ry / att). |
| `ryoe` | double | Rushing yards over expected, season total (yds - x_ry; nflverse `rush_yards_over_expected`). |
| `ryoe_att` | double | Rushing yards over expected per attempt (ryoe / att). |
| `yaco` | double | Yards after contact, season total. |
| `yaco_att` | double | Yards after contact per attempt (yaco / att). |
| `ybco` | double | Yards before contact, season total. |
| `ybco_att` | double | Yards before contact per attempt (ybco / att). |
| `success` | double | Success rate: share of rush attempts graded successful, as a fraction. |
| `fum` | integer | Fumbles on rush attempts, season total. |
| `lost` | integer | Fumbles lost on rush attempts, season total. |
| `rush10_p_yds` | integer | Rush attempts that gained 10 or more yards, season total. |
| `rush15_p_mph` | integer | Rush attempts on which the ball carrier reached 15+ mph, per Next Gen Stats. |
| `rush20_p_mph` | integer | Rush attempts on which the ball carrier reached 20+ mph, per Next Gen Stats. |
| `eff` | double | Rushing efficiency: distance travelled per rushing yard gained (lower is more direct), per Next Gen Stats (nflverse `efficiency`). |
| `in_t_pct` | double |  |
| `st_box_pct` | double | Share of rush attempts against a stacked box (8 or more defenders), per Next Gen Stats. |
| `under_pct` | double |  |
| `qr` | logical | Whether the player meets the league qualifying threshold for the table (the `qualified` filter). |
| `att_pg` | double | Rush attempts per game (att / gp). |
| `yds_pg` | double | Rushing yards per game (yds / gp). |
| `td_pg` | double | Rushing touchdowns per game (td / gp). |
| `epa_pg` | double | Expected points added per game (epa / gp). |
| `x_ry_pg` | double | Expected rushing yards per game (x_ry / gp). |
| `ryoe_pg` | double | Rushing yards over expected per game (ryoe / gp). |
| `yaco_pg` | double | Yards after contact per game (yaco / gp). |
| `ybco_pg` | double | Yards before contact per game (ybco / gp). |
| `fum_pg` | double | Fumbles per game (fum / gp). |
| `lost_pg` | double | Fumbles lost per game (lost / gp). |
| `rush10_p_yds_pg` | double | Rushes of 10+ yards per game (rush10_p_yds / gp). |
| `rush15_p_mph_pg` | double | Rushes reaching 15+ mph per game (rush15_p_mph / gp). |
| `rush20_p_mph_pg` | double | Rushes reaching 20+ mph per game (rush20_p_mph / gp). |

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
| `att` | integer | Rush attempts in the game. |
| `yds` | integer | Rushing yards in the game. |
| `td` | integer | Rushing touchdowns in the game. |
| `ypc` | double | Yards per carry (yds / att). |
| `epa` | double | Total expected points added on the player's rush attempts. |
| `epa_att` | double | Expected points added per rush attempt (epa / att). |
| `x_ry` | double | Expected rushing yards in the game, per Next Gen Stats (nflverse `expected_rush_yards`); yds = x_ry + ryoe. |
| `x_ypc` | double | Expected yards per carry (x_ry / att). |
| `ryoe` | double | Rushing yards over expected in the game (yds - x_ry; nflverse `rush_yards_over_expected`). |
| `ryoe_att` | double | Rushing yards over expected per attempt (ryoe / att). |
| `yaco` | double | Yards after contact in the game. |
| `yaco_att` | double | Yards after contact per attempt (yaco / att). |
| `ybco` | double | Yards before contact in the game. |
| `ybco_att` | double | Yards before contact per attempt (ybco / att). |
| `success` | double | Success rate: share of rush attempts graded successful, as a fraction. |
| `fum` | integer | Fumbles on rush attempts in the game. |
| `lost` | integer | Fumbles lost on rush attempts in the game. |
| `rush10_p_yds` | integer | Rush attempts that gained 10 or more yards in the game. |
| `rush15_p_mph` | integer | Rush attempts on which the ball carrier reached 15+ mph, per Next Gen Stats. |
| `rush20_p_mph` | integer | Rush attempts on which the ball carrier reached 20+ mph, per Next Gen Stats. |
| `eff` | double | Rushing efficiency: distance travelled per rushing yard gained (lower is more direct), per Next Gen Stats (nflverse `efficiency`). |
| `in_t_pct` | double |  |
| `st_box_pct` | integer | Share of rush attempts against a stacked box (8 or more defenders), per Next Gen Stats. |
| `under_pct` | double |  |
| `qr` | logical | Whether the player meets the league qualifying threshold for the table (the `qualified` filter). |
| `att_pg` | integer | Rush attempts per game (att / gp). |
| `yds_pg` | integer | Rushing yards per game (yds / gp). |
| `td_pg` | integer | Rushing touchdowns per game (td / gp). |
| `epa_pg` | double | Expected points added per game (epa / gp). |
| `x_ry_pg` | double | Expected rushing yards per game (x_ry / gp). |
| `ryoe_pg` | double | Rushing yards over expected per game (ryoe / gp). |
| `yaco_pg` | double | Yards after contact per game (yaco / gp). |
| `ybco_pg` | double | Yards before contact per game (ybco / gp). |
| `fum_pg` | integer | Fumbles per game (fum / gp). |
| `lost_pg` | integer | Fumbles lost per game (lost / gp). |
| `rush10_p_yds_pg` | integer | Rushes of 10+ yards per game (rush10_p_yds / gp). |
| `rush15_p_mph_pg` | integer | Rushes reaching 15+ mph per game (rush15_p_mph / gp). |
| `rush20_p_mph_pg` | integer | Rushes reaching 20+ mph per game (rush20_p_mph / gp). |

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
| `rt` | integer | Routes run, season total. |
| `tgt` | integer | Targets, season total. |
| `rec` | integer | Receptions, season total. |
| `yds` | integer | Receiving yards, season total. |
| `td` | integer | Receiving touchdowns, season total. |
| `int` | integer | Interceptions thrown on passes targeting the player, season total. |
| `rating` | double | Passer rating on throws targeting the player (the standard formula applied to his targets). |
| `catch` | double | Catch rate as a fraction (rec / tgt; nflverse `catch_percentage` / 100). |
| `x_catch` | double | Expected catch rate as a fraction, per Next Gen Stats; catch = x_catch + croe. |
| `croe` | double | Catch rate over expected as a fraction (catch - x_catch). |
| `yds_rec` | double | Yards per reception (yds / rec). |
| `yds_rt` | double | Yards per route run (yds / rt). |
| `epa` | double | Total expected points added on the player's targets. |
| `epa_tgt` | double | Expected points added per target (epa / tgt). |
| `epa_rt` | double | Expected points added per route run (epa / rt). |
| `drop` | integer | Drops, season total. |
| `drop_tgt` | double | Drop rate: drops per target (drop / tgt). |
| `yac` | integer | Yards after the catch, season total. |
| `x_yac` | integer | Expected yards after the catch, season total, per Next Gen Stats; yac = x_yac + yacoe. |
| `yacoe` | integer | Yards after the catch over expected, season total (yac - x_yac). |
| `yac_rec` | double | Yards after the catch per reception (yac / rec; nflverse `avg_yac`). |
| `avg_sep` | double | Average separation in yards from the nearest defender at pass arrival, per Next Gen Stats (nflverse `avg_separation`). |
| `ay` | double | Total intended air yards on the player's targets. |
| `ay_tgt` | double | Average intended air yards per target (ay / tgt; nflverse `avg_intended_air_yards`). |
| `tgt_rt` | double | Target rate: targets per route run (tgt / rt). |
| `avg_rt_dep` | double | Average route depth in yards, per Next Gen Stats. |
| `ez_tgt` | integer | End-zone targets, season total. |
| `ez_rec` | integer | End-zone receptions, season total. |
| `deep_tgt_pct` | double | Share of targets NGS classifies as deep. |
| `tw_pct` | double | Share of targets thrown into a tight window (defender within a yard), per Next Gen Stats. |
| `qr` | logical | Whether the player meets the league qualifying threshold for the table (the `qualified` filter). |
| `rt_pg` | double | Routes run per game (rt / gp). |
| `tgt_pg` | double | Targets per game (tgt / gp). |
| `rec_pg` | double | Receptions per game (rec / gp). |
| `yds_pg` | double | Receiving yards per game (yds / gp). |
| `td_pg` | double | Receiving touchdowns per game (td / gp). |
| `int_pg` | double | Interceptions on the player's targets per game (int / gp). |
| `epa_pg` | double | Expected points added per game (epa / gp). |
| `drop_pg` | double | Drops per game (drop / gp). |
| `yac_pg` | double | Yards after the catch per game (yac / gp). |
| `x_yac_pg` | double | Expected yards after the catch per game (x_yac / gp). |
| `yacoe_pg` | double | Yards after the catch over expected per game (yacoe / gp). |
| `ay_pg` | double | Intended air yards per game (ay / gp). |
| `ez_tgt_pg` | double | End-zone targets per game (ez_tgt / gp). |
| `ez_rec_pg` | double | End-zone receptions per game (ez_rec / gp). |

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
| `rt` | integer | Routes run in the game. |
| `tgt` | integer | Targets in the game. |
| `rec` | integer | Receptions in the game. |
| `yds` | integer | Receiving yards in the game. |
| `td` | integer | Receiving touchdowns in the game. |
| `int` | integer | Interceptions thrown on passes targeting the player in the game. |
| `rating` | integer | Passer rating on throws targeting the player (the standard formula applied to his targets). |
| `catch` | integer | Catch rate as a fraction (rec / tgt; nflverse `catch_percentage` / 100). |
| `x_catch` | integer | Expected catch rate as a fraction, per Next Gen Stats; catch = x_catch + croe. |
| `croe` | integer | Catch rate over expected as a fraction (catch - x_catch). |
| `yds_rec` | integer | Yards per reception (yds / rec). |
| `yds_rt` | integer | Yards per route run (yds / rt). |
| `epa` | integer | Total expected points added on the player's targets. |
| `epa_tgt` | integer | Expected points added per target (epa / tgt). |
| `epa_rt` | integer | Expected points added per route run (epa / rt). |
| `drop` | integer | Drops in the game. |
| `drop_tgt` | integer | Drop rate: drops per target (drop / tgt). |
| `yac` | integer | Yards after the catch in the game. |
| `x_yac` | integer | Expected yards after the catch in the game, per Next Gen Stats; yac = x_yac + yacoe. |
| `yacoe` | integer | Yards after the catch over expected in the game (yac - x_yac). |
| `yac_rec` | integer | Yards after the catch per reception (yac / rec; nflverse `avg_yac`). |
| `avg_sep` | integer | Average separation in yards from the nearest defender at pass arrival, per Next Gen Stats (nflverse `avg_separation`). |
| `ay` | integer | Total intended air yards on the player's targets. |
| `ay_tgt` | integer | Average intended air yards per target (ay / tgt; nflverse `avg_intended_air_yards`). |
| `tgt_rt` | integer | Target rate: targets per route run (tgt / rt). |
| `avg_rt_dep` | integer | Average route depth in yards, per Next Gen Stats. |
| `ez_tgt` | integer | End-zone targets in the game. |
| `ez_rec` | integer | End-zone receptions in the game. |
| `deep_tgt_pct` | integer | Share of targets NGS classifies as deep. |
| `tw_pct` | integer | Share of targets thrown into a tight window (defender within a yard), per Next Gen Stats. |
| `qr` | logical | Whether the player meets the league qualifying threshold for the table (the `qualified` filter). |
| `rt_pg` | integer | Routes run per game (rt / gp). |
| `tgt_pg` | integer | Targets per game (tgt / gp). |
| `rec_pg` | integer | Receptions per game (rec / gp). |
| `yds_pg` | integer | Receiving yards per game (yds / gp). |
| `td_pg` | integer | Receiving touchdowns per game (td / gp). |
| `int_pg` | integer | Interceptions on the player's targets per game (int / gp). |
| `epa_pg` | integer | Expected points added per game (epa / gp). |
| `drop_pg` | integer | Drops per game (drop / gp). |
| `yac_pg` | integer | Yards after the catch per game (yac / gp). |
| `x_yac_pg` | integer | Expected yards after the catch per game (x_yac / gp). |
| `yacoe_pg` | integer | Yards after the catch over expected per game (yacoe / gp). |
| `ay_pg` | integer | Intended air yards per game (ay / gp). |
| `ez_tgt_pg` | integer | End-zone targets per game (ez_tgt / gp). |
| `ez_rec_pg` | integer | End-zone receptions per game (ez_rec / gp). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nfl_pro_players_offense_receiving_week-example}

```python
nfl_pro_players_offense_receiving_week(season=2024, season_type='REG')
```

_Last validated n/a._
