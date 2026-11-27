# NFL — NFL Pro API (pro.nfl.com, NFL+ Premium account) — Players

> NFL — NFL Pro API (pro.nfl.com, NFL+ Premium account) — Players — function reference in sdv-py, the SportsDataverse Python package.

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
| `nfl_id` | String | NFL player id (`nflId`) as a string; the key the wrappers accept as `nfl_id`. |
| `display_name` | String | Player's full display name (e.g. 'Aaron Rodgers'). |
| `short_name` | String | Player's abbreviated name, first initial and surname (e.g. 'A.Rodgers'). |
| `headshot` | String | NFL headshot image URL template; the `{formatInstructions}` token must be replaced with a Cloudinary transform (e.g. `t_headshot_desktop`) before use. |
| `team_id` | String | NFL team id as a zero-padded string (e.g. '0200', '3430'); casting it to a number drops the leading zero. |
| `jersey_number` | Int64 | Jersey number the player wore in the season. |
| `position` | String | Roster position abbreviation (e.g. QB, WR, CB). |
| `position_group` | String | Position group the roster position rolls up to (e.g. QB, RB, WR, TE, DB). |
| `ngs_position` | String | Position Next Gen Stats assigns from tracking data; null for players NGS has not classified. |
| `ngs_position_group` | String | Position group of ngs_position; null when ngs_position is null. |
| `gp` | Int64 | Games played in the season (and season type) -- the denominator of every `*_pg` per-game column. |
| `gs` | Int64 | Games started in the season. |
| `tg` | Int64 | Games the player's team played while he was on the roster; equals gp in sampled data. |
| `total_tg` | Int64 | Total games the player's team played in the season type (17 for a full regular season). |
| `cmp` | Int64 | Pass completions, season total. |
| `att` | Int64 | Pass attempts, season total. |
| `yds` | Int64 | Passing yards, season total. |
| `td` | Int64 | Passing touchdowns, season total. |
| `int` | Int64 | Interceptions thrown, season total (a count, not a per-play flag). |
| `rating` | Float64 | NFL passer rating for the season (the standard 0-158.3 formula; equals nflverse `passer_rating`). |
| `ypa` | Float64 | Yards per pass attempt (yds / att). |
| `cmp_pct` | Float64 | Completion percentage as a fraction (cmp / att, e.g. 0.63). |
| `sack` | Int64 | Times sacked, season total (a count, not a per-play flag). |
| `x_cmp` | Float64 | Expected completion percentage as a fraction, per Next Gen Stats (nflverse `expected_completion_percentage` / 100). |
| `cpoe` | Float64 | Completion percentage over expected as a fraction: cmp_pct - x_cmp (nflverse `completion_percentage_above_expectation` / 100). |
| `db` | Int64 | Dropbacks, season total (attempts plus sacks and scrambles); the denominator of qbp_r and epa_db. |
| `epa` | Float64 | Total expected points added on the passer's dropbacks. |
| `epa_db` | Float64 | Expected points added per dropback (epa / db). |
| `avg_ttt` | Float64 | Average time to throw in seconds, snap to release, per Next Gen Stats (nflverse `avg_time_to_throw`). |
| `avg_ttp` | Float64 |  |
| `avg_tts` | Float64 |  |
| `qbp` | Int64 | Dropbacks on which the passer was pressured, season total. |
| `qbp_r` | Float64 | Pressure rate: share of dropbacks on which the passer was pressured (qbp / db). |
| `blitz_r` | Float64 | Blitz rate: share of the passer's dropbacks on which the defense blitzed, per Next Gen Stats. |
| `drop` | Int64 | Passes dropped by the passer's receivers, season total. |
| `drop_r` | Float64 | Drop rate: drops per pass attempt (drop / att). |
| `ay` | Float64 | Total intended air yards on pass attempts; ay_att is this per attempt. |
| `yac` | Float64 | Total yards after the catch gained on the passer's completions. |
| `x_yac` | Float64 | Total expected yards after the catch on the passer's completions, per Next Gen Stats. |
| `yac_pct` | Float64 | Share of passing yards gained after the catch (yac / yds). |
| `ay_att` | Float64 | Average intended air yards per pass attempt (nflverse `avg_intended_air_yards`). |
| `avg_sep` | Float64 | Average separation in yards between the targeted receiver and the nearest defender at pass arrival on the passer's targets, per Next Gen Stats. |
| `deep_att_pct` | Float64 | Share of pass attempts NGS classifies as deep throws. |
| `tw_att_pct` | Float64 | Aggressiveness: share of pass attempts into a tight window (defender within a yard of the receiver), per Next Gen Stats (nflverse `aggressiveness` / 100). |
| `pa_db_pct` | Float64 | Share of dropbacks that used play action. |
| `qp` | Boolean | Whether the passer meets the league qualifying-attempts threshold (the `qualified` filter). |
| `cmp_pg` | Float64 | Completions per game (cmp / gp). |
| `att_pg` | Float64 | Pass attempts per game (att / gp). |
| `yds_pg` | Float64 | Passing yards per game (yds / gp). |
| `td_pg` | Float64 | Passing touchdowns per game (td / gp). |
| `int_pg` | Float64 | Interceptions thrown per game (int / gp). |
| `sack_pg` | Float64 | Sacks taken per game (sack / gp). |
| `db_pg` | Float64 | Dropbacks per game (db / gp). |
| `epa_pg` | Float64 | Expected points added per game (epa / gp). |
| `qbp_pg` | Float64 | Pressured dropbacks per game (qbp / gp). |
| `drop_pg` | Float64 | Receiver drops per game (drop / gp). |
| `tw_att_pg` | Float64 | Tight-window pass attempts per game; tw_att_pct = tw_att_pg / att_pg. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_pro_players_offense_passing_season-raises}

| Exception | Description |
|---|---|
| `NFLProAuthError` | No user-bound NFL Pro token could be resolved (pass `token=` or `email=`/`password=`, or set `NFLPRO_TOKEN`, or `NFLPRO_EMAIL`/`NFLPRO_PW`), or the resolved token carries no active NFL_PLUS_* plan, or pro.nfl.com answered 401 / 403 (an `AssetFetchError`). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. Pro.nfl.com answered 400 / 422, or ignored `offset`. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other non-2xx or a connection failure after retries, or a 2xx whose body is not a JSON object or array -- including the HTTP 200 with an empty body that is how pro.nfl.com rejects unsupported query params. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |

### Example {#nfl_pro_players_offense_passing_season-example}

```python
nfl_pro_players_offense_passing_season(season=2024, season_type='REG')
```

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
| `nfl_id` | String | NFL player id (`nflId`) as a string; the key the wrappers accept as `nfl_id`. |
| `display_name` | String | Player's full display name (e.g. 'Aaron Rodgers'). |
| `short_name` | String | Player's abbreviated name, first initial and surname (e.g. 'A.Rodgers'). |
| `headshot` | String | NFL headshot image URL template; the `{formatInstructions}` token must be replaced with a Cloudinary transform (e.g. `t_headshot_desktop`) before use. |
| `team_id` | String | NFL team id as a zero-padded string (e.g. '0200', '3430'); casting it to a number drops the leading zero. |
| `jersey_number` | Int64 | Jersey number the player wore in the season. |
| `position` | String | Roster position abbreviation (e.g. QB, WR, CB). |
| `position_group` | String | Position group the roster position rolls up to (e.g. QB, RB, WR, TE, DB). |
| `ngs_position` | String | Position Next Gen Stats assigns from tracking data; null for players NGS has not classified. |
| `ngs_position_group` | String | Position group of ngs_position; null when ngs_position is null. |
| `gp` | Int64 | Games played in the season (and season type) -- the denominator of every `*_pg` per-game column. |
| `gs` | Int64 | Games started in the season. |
| `tg` | Int64 | Games the player's team played while he was on the roster; equals gp in sampled data. |
| `total_tg` | Int64 | Total games the player's team played in the season type (17 for a full regular season). |
| `week_slug` | String | Week slug of the game (e.g. 'WEEK_1', 'WEEK_18'); the week scope of the row. |
| `game_id` | Int64 | NFL game id as an integer (e.g. 2024090900, the date followed by a two-digit sequence). |
| `fapi_game_id` | String | NFL Football API (FAPI) UUID of the game, the id api.nfl.com uses for the same game. |
| `opponent_team_id` | String | Opponent's NFL team id as a zero-padded string. |
| `is_home` | Boolean | Whether the player's team was the home team in the game. |
| `final_score` | String | Final score as 'own-opponent' (e.g. '19-32' for a 32-19 loss). |
| `game_result` | String | Result from the player's team's side: 'W', 'L' or 'T'. |
| `cmp` | Int64 | Pass completions in the game. |
| `att` | Int64 | Pass attempts in the game. |
| `yds` | Int64 | Passing yards in the game. |
| `td` | Int64 | Passing touchdowns in the game. |
| `int` | Int64 | Interceptions thrown in the game (a count, not a per-play flag). |
| `rating` | Float64 | NFL passer rating for the season (the standard 0-158.3 formula; equals nflverse `passer_rating`). |
| `ypa` | Float64 | Yards per pass attempt (yds / att). |
| `cmp_pct` | Float64 | Completion percentage as a fraction (cmp / att, e.g. 0.63). |
| `sack` | Int64 | Times sacked in the game (a count, not a per-play flag). |
| `x_cmp` | Float64 | Expected completion percentage as a fraction, per Next Gen Stats (nflverse `expected_completion_percentage` / 100). |
| `cpoe` | Float64 | Completion percentage over expected as a fraction: cmp_pct - x_cmp (nflverse `completion_percentage_above_expectation` / 100). |
| `db` | Int64 | Dropbacks in the game (attempts plus sacks and scrambles); the denominator of qbp_r and epa_db. |
| `epa` | Float64 | Total expected points added on the passer's dropbacks. |
| `epa_db` | Float64 | Expected points added per dropback (epa / db). |
| `avg_ttt` | Float64 | Average time to throw in seconds, snap to release, per Next Gen Stats (nflverse `avg_time_to_throw`). |
| `avg_ttp` | Float64 |  |
| `avg_tts` | Float64 |  |
| `qbp` | Int64 | Dropbacks on which the passer was pressured in the game. |
| `qbp_r` | Float64 | Pressure rate: share of dropbacks on which the passer was pressured (qbp / db). |
| `blitz_r` | Float64 | Blitz rate: share of the passer's dropbacks on which the defense blitzed, per Next Gen Stats. |
| `drop` | Int64 | Passes dropped by the passer's receivers in the game. |
| `drop_r` | Float64 | Drop rate: drops per pass attempt (drop / att). |
| `ay` | Float64 | Total intended air yards on pass attempts; ay_att is this per attempt. |
| `yac` | Float64 | Total yards after the catch gained on the passer's completions. |
| `x_yac` | Float64 | Total expected yards after the catch on the passer's completions, per Next Gen Stats. |
| `yac_pct` | Float64 | Share of passing yards gained after the catch (yac / yds). |
| `ay_att` | Float64 | Average intended air yards per pass attempt (nflverse `avg_intended_air_yards`). |
| `avg_sep` | Float64 | Average separation in yards between the targeted receiver and the nearest defender at pass arrival on the passer's targets, per Next Gen Stats. |
| `deep_att_pct` | Float64 | Share of pass attempts NGS classifies as deep throws. |
| `tw_att_pct` | Float64 | Aggressiveness: share of pass attempts into a tight window (defender within a yard of the receiver), per Next Gen Stats (nflverse `aggressiveness` / 100). |
| `pa_db_pct` | Float64 | Share of dropbacks that used play action. |
| `qp` | Boolean | Whether the passer meets the league qualifying-attempts threshold (the `qualified` filter). |
| `cmp_pg` | Int64 | Completions per game (cmp / gp). |
| `att_pg` | Int64 | Pass attempts per game (att / gp). |
| `yds_pg` | Int64 | Passing yards per game (yds / gp). |
| `td_pg` | Int64 | Passing touchdowns per game (td / gp). |
| `int_pg` | Int64 | Interceptions thrown per game (int / gp). |
| `sack_pg` | Int64 | Sacks taken per game (sack / gp). |
| `db_pg` | Int64 | Dropbacks per game (db / gp). |
| `epa_pg` | Float64 | Expected points added per game (epa / gp). |
| `qbp_pg` | Int64 | Pressured dropbacks per game (qbp / gp). |
| `drop_pg` | Int64 | Receiver drops per game (drop / gp). |
| `tw_att_pg` | Int64 | Tight-window pass attempts per game; tw_att_pct = tw_att_pg / att_pg. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_pro_players_offense_passing_week-raises}

| Exception | Description |
|---|---|
| `NFLProAuthError` | No user-bound NFL Pro token could be resolved (pass `token=` or `email=`/`password=`, or set `NFLPRO_TOKEN`, or `NFLPRO_EMAIL`/`NFLPRO_PW`), or the resolved token carries no active NFL_PLUS_* plan, or pro.nfl.com answered 401 / 403 (an `AssetFetchError`). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. Pro.nfl.com answered 400 / 422, or ignored `offset`. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other non-2xx or a connection failure after retries, or a 2xx whose body is not a JSON object or array -- including the HTTP 200 with an empty body that is how pro.nfl.com rejects unsupported query params. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |

### Example {#nfl_pro_players_offense_passing_week-example}

```python
nfl_pro_players_offense_passing_week(season=2024, season_type='REG')
```

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
| `nfl_id` | String | NFL player id (`nflId`) as a string; the key the wrappers accept as `nfl_id`. |
| `display_name` | String | Player's full display name (e.g. 'Aaron Rodgers'). |
| `short_name` | String | Player's abbreviated name, first initial and surname (e.g. 'A.Rodgers'). |
| `headshot` | String | NFL headshot image URL template; the `{formatInstructions}` token must be replaced with a Cloudinary transform (e.g. `t_headshot_desktop`) before use. |
| `team_id` | String | NFL team id as a zero-padded string (e.g. '0200', '3430'); casting it to a number drops the leading zero. |
| `jersey_number` | Int64 | Jersey number the player wore in the season. |
| `position` | String | Roster position abbreviation (e.g. QB, WR, CB). |
| `position_group` | String | Position group the roster position rolls up to (e.g. QB, RB, WR, TE, DB). |
| `ngs_position` | String | Position Next Gen Stats assigns from tracking data; null for players NGS has not classified. |
| `ngs_position_group` | String | Position group of ngs_position; null when ngs_position is null. |
| `gp` | Int64 | Games played in the season (and season type) -- the denominator of every `*_pg` per-game column. |
| `gs` | Int64 | Games started in the season. |
| `tg` | Int64 | Games the player's team played while he was on the roster; equals gp in sampled data. |
| `total_tg` | Int64 | Total games the player's team played in the season type (17 for a full regular season). |
| `att` | Int64 | Rush attempts, season total. |
| `yds` | Int64 | Rushing yards, season total. |
| `td` | Int64 | Rushing touchdowns, season total. |
| `ypc` | Float64 | Yards per carry (yds / att). |
| `epa` | Float64 | Total expected points added on the player's rush attempts. |
| `epa_att` | Float64 | Expected points added per rush attempt (epa / att). |
| `x_ry` | Float64 | Expected rushing yards, season total, per Next Gen Stats (nflverse `expected_rush_yards`); yds = x_ry + ryoe. |
| `x_ypc` | Float64 | Expected yards per carry (x_ry / att). |
| `ryoe` | Float64 | Rushing yards over expected, season total (yds - x_ry; nflverse `rush_yards_over_expected`). |
| `ryoe_att` | Float64 | Rushing yards over expected per attempt (ryoe / att). |
| `yaco` | Float64 | Yards after contact, season total. |
| `yaco_att` | Float64 | Yards after contact per attempt (yaco / att). |
| `ybco` | Float64 | Yards before contact, season total. |
| `ybco_att` | Float64 | Yards before contact per attempt (ybco / att). |
| `success` | Float64 | Success rate: share of rush attempts graded successful, as a fraction. |
| `fum` | Int64 | Fumbles on rush attempts, season total. |
| `lost` | Int64 | Fumbles lost on rush attempts, season total. |
| `rush10_p_yds` | Int64 | Rush attempts that gained 10 or more yards, season total. |
| `rush15_p_mph` | Int64 | Rush attempts on which the ball carrier reached 15+ mph, per Next Gen Stats. |
| `rush20_p_mph` | Int64 | Rush attempts on which the ball carrier reached 20+ mph, per Next Gen Stats. |
| `eff` | Float64 | Rushing efficiency: distance travelled per rushing yard gained (lower is more direct), per Next Gen Stats (nflverse `efficiency`). |
| `in_t_pct` | Float64 |  |
| `st_box_pct` | Float64 | Share of rush attempts against a stacked box (8 or more defenders), per Next Gen Stats. |
| `under_pct` | Float64 |  |
| `qr` | Boolean | Whether the player meets the league qualifying threshold for the table (the `qualified` filter). |
| `att_pg` | Float64 | Rush attempts per game (att / gp). |
| `yds_pg` | Float64 | Rushing yards per game (yds / gp). |
| `td_pg` | Float64 | Rushing touchdowns per game (td / gp). |
| `epa_pg` | Float64 | Expected points added per game (epa / gp). |
| `x_ry_pg` | Float64 | Expected rushing yards per game (x_ry / gp). |
| `ryoe_pg` | Float64 | Rushing yards over expected per game (ryoe / gp). |
| `yaco_pg` | Float64 | Yards after contact per game (yaco / gp). |
| `ybco_pg` | Float64 | Yards before contact per game (ybco / gp). |
| `fum_pg` | Float64 | Fumbles per game (fum / gp). |
| `lost_pg` | Float64 | Fumbles lost per game (lost / gp). |
| `rush10_p_yds_pg` | Float64 | Rushes of 10+ yards per game (rush10_p_yds / gp). |
| `rush15_p_mph_pg` | Float64 | Rushes reaching 15+ mph per game (rush15_p_mph / gp). |
| `rush20_p_mph_pg` | Float64 | Rushes reaching 20+ mph per game (rush20_p_mph / gp). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_pro_players_offense_rushing_season-raises}

| Exception | Description |
|---|---|
| `NFLProAuthError` | No user-bound NFL Pro token could be resolved (pass `token=` or `email=`/`password=`, or set `NFLPRO_TOKEN`, or `NFLPRO_EMAIL`/`NFLPRO_PW`), or the resolved token carries no active NFL_PLUS_* plan, or pro.nfl.com answered 401 / 403 (an `AssetFetchError`). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. Pro.nfl.com answered 400 / 422, or ignored `offset`. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other non-2xx or a connection failure after retries, or a 2xx whose body is not a JSON object or array -- including the HTTP 200 with an empty body that is how pro.nfl.com rejects unsupported query params. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |

### Example {#nfl_pro_players_offense_rushing_season-example}

```python
nfl_pro_players_offense_rushing_season(season=2024, season_type='REG')
```

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
| `nfl_id` | String | NFL player id (`nflId`) as a string; the key the wrappers accept as `nfl_id`. |
| `display_name` | String | Player's full display name (e.g. 'Aaron Rodgers'). |
| `short_name` | String | Player's abbreviated name, first initial and surname (e.g. 'A.Rodgers'). |
| `headshot` | String | NFL headshot image URL template; the `{formatInstructions}` token must be replaced with a Cloudinary transform (e.g. `t_headshot_desktop`) before use. |
| `team_id` | String | NFL team id as a zero-padded string (e.g. '0200', '3430'); casting it to a number drops the leading zero. |
| `jersey_number` | Int64 | Jersey number the player wore in the season. |
| `position` | String | Roster position abbreviation (e.g. QB, WR, CB). |
| `position_group` | String | Position group the roster position rolls up to (e.g. QB, RB, WR, TE, DB). |
| `ngs_position` | String | Position Next Gen Stats assigns from tracking data; null for players NGS has not classified. |
| `ngs_position_group` | String | Position group of ngs_position; null when ngs_position is null. |
| `gp` | Int64 | Games played in the season (and season type) -- the denominator of every `*_pg` per-game column. |
| `gs` | Int64 | Games started in the season. |
| `tg` | Int64 | Games the player's team played while he was on the roster; equals gp in sampled data. |
| `total_tg` | Int64 | Total games the player's team played in the season type (17 for a full regular season). |
| `week_slug` | String | Week slug of the game (e.g. 'WEEK_1', 'WEEK_18'); the week scope of the row. |
| `game_id` | Int64 | NFL game id as an integer (e.g. 2024090900, the date followed by a two-digit sequence). |
| `fapi_game_id` | String | NFL Football API (FAPI) UUID of the game, the id api.nfl.com uses for the same game. |
| `opponent_team_id` | String | Opponent's NFL team id as a zero-padded string. |
| `is_home` | Boolean | Whether the player's team was the home team in the game. |
| `final_score` | String | Final score as 'own-opponent' (e.g. '19-32' for a 32-19 loss). |
| `game_result` | String | Result from the player's team's side: 'W', 'L' or 'T'. |
| `att` | Int64 | Rush attempts in the game. |
| `yds` | Int64 | Rushing yards in the game. |
| `td` | Int64 | Rushing touchdowns in the game. |
| `ypc` | Float64 | Yards per carry (yds / att). |
| `epa` | Float64 | Total expected points added on the player's rush attempts. |
| `epa_att` | Float64 | Expected points added per rush attempt (epa / att). |
| `x_ry` | Float64 | Expected rushing yards in the game, per Next Gen Stats (nflverse `expected_rush_yards`); yds = x_ry + ryoe. |
| `x_ypc` | Float64 | Expected yards per carry (x_ry / att). |
| `ryoe` | Float64 | Rushing yards over expected in the game (yds - x_ry; nflverse `rush_yards_over_expected`). |
| `ryoe_att` | Float64 | Rushing yards over expected per attempt (ryoe / att). |
| `yaco` | Float64 | Yards after contact in the game. |
| `yaco_att` | Float64 | Yards after contact per attempt (yaco / att). |
| `ybco` | Float64 | Yards before contact in the game. |
| `ybco_att` | Float64 | Yards before contact per attempt (ybco / att). |
| `success` | Float64 | Success rate: share of rush attempts graded successful, as a fraction. |
| `fum` | Int64 | Fumbles on rush attempts in the game. |
| `lost` | Int64 | Fumbles lost on rush attempts in the game. |
| `rush10_p_yds` | Int64 | Rush attempts that gained 10 or more yards in the game. |
| `rush15_p_mph` | Int64 | Rush attempts on which the ball carrier reached 15+ mph, per Next Gen Stats. |
| `rush20_p_mph` | Int64 | Rush attempts on which the ball carrier reached 20+ mph, per Next Gen Stats. |
| `eff` | Float64 | Rushing efficiency: distance travelled per rushing yard gained (lower is more direct), per Next Gen Stats (nflverse `efficiency`). |
| `in_t_pct` | Float64 |  |
| `st_box_pct` | Int64 | Share of rush attempts against a stacked box (8 or more defenders), per Next Gen Stats. |
| `under_pct` | Float64 |  |
| `qr` | Boolean | Whether the player meets the league qualifying threshold for the table (the `qualified` filter). |
| `att_pg` | Int64 | Rush attempts per game (att / gp). |
| `yds_pg` | Int64 | Rushing yards per game (yds / gp). |
| `td_pg` | Int64 | Rushing touchdowns per game (td / gp). |
| `epa_pg` | Float64 | Expected points added per game (epa / gp). |
| `x_ry_pg` | Float64 | Expected rushing yards per game (x_ry / gp). |
| `ryoe_pg` | Float64 | Rushing yards over expected per game (ryoe / gp). |
| `yaco_pg` | Float64 | Yards after contact per game (yaco / gp). |
| `ybco_pg` | Float64 | Yards before contact per game (ybco / gp). |
| `fum_pg` | Int64 | Fumbles per game (fum / gp). |
| `lost_pg` | Int64 | Fumbles lost per game (lost / gp). |
| `rush10_p_yds_pg` | Int64 | Rushes of 10+ yards per game (rush10_p_yds / gp). |
| `rush15_p_mph_pg` | Int64 | Rushes reaching 15+ mph per game (rush15_p_mph / gp). |
| `rush20_p_mph_pg` | Int64 | Rushes reaching 20+ mph per game (rush20_p_mph / gp). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_pro_players_offense_rushing_week-raises}

| Exception | Description |
|---|---|
| `NFLProAuthError` | No user-bound NFL Pro token could be resolved (pass `token=` or `email=`/`password=`, or set `NFLPRO_TOKEN`, or `NFLPRO_EMAIL`/`NFLPRO_PW`), or the resolved token carries no active NFL_PLUS_* plan, or pro.nfl.com answered 401 / 403 (an `AssetFetchError`). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. Pro.nfl.com answered 400 / 422, or ignored `offset`. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other non-2xx or a connection failure after retries, or a 2xx whose body is not a JSON object or array -- including the HTTP 200 with an empty body that is how pro.nfl.com rejects unsupported query params. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |

### Example {#nfl_pro_players_offense_rushing_week-example}

```python
nfl_pro_players_offense_rushing_week(season=2024, season_type='REG')
```

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
| `nfl_id` | String | NFL player id (`nflId`) as a string; the key the wrappers accept as `nfl_id`. |
| `display_name` | String | Player's full display name (e.g. 'Aaron Rodgers'). |
| `short_name` | String | Player's abbreviated name, first initial and surname (e.g. 'A.Rodgers'). |
| `headshot` | String | NFL headshot image URL template; the `{formatInstructions}` token must be replaced with a Cloudinary transform (e.g. `t_headshot_desktop`) before use. |
| `team_id` | String | NFL team id as a zero-padded string (e.g. '0200', '3430'); casting it to a number drops the leading zero. |
| `jersey_number` | Int64 | Jersey number the player wore in the season. |
| `position` | String | Roster position abbreviation (e.g. QB, WR, CB). |
| `position_group` | String | Position group the roster position rolls up to (e.g. QB, RB, WR, TE, DB). |
| `ngs_position` | String | Position Next Gen Stats assigns from tracking data; null for players NGS has not classified. |
| `ngs_position_group` | String | Position group of ngs_position; null when ngs_position is null. |
| `gp` | Int64 | Games played in the season (and season type) -- the denominator of every `*_pg` per-game column. |
| `gs` | Int64 | Games started in the season. |
| `tg` | Int64 | Games the player's team played while he was on the roster; equals gp in sampled data. |
| `total_tg` | Int64 | Total games the player's team played in the season type (17 for a full regular season). |
| `rt` | Int64 | Routes run, season total. |
| `tgt` | Int64 | Targets, season total. |
| `rec` | Int64 | Receptions, season total. |
| `yds` | Int64 | Receiving yards, season total. |
| `td` | Int64 | Receiving touchdowns, season total. |
| `int` | Int64 | Interceptions thrown on passes targeting the player, season total. |
| `rating` | Float64 | Passer rating on throws targeting the player (the standard formula applied to his targets). |
| `catch` | Float64 | Catch rate as a fraction (rec / tgt; nflverse `catch_percentage` / 100). |
| `x_catch` | Float64 | Expected catch rate as a fraction, per Next Gen Stats; catch = x_catch + croe. |
| `croe` | Float64 | Catch rate over expected as a fraction (catch - x_catch). |
| `yds_rec` | Float64 | Yards per reception (yds / rec). |
| `yds_rt` | Float64 | Yards per route run (yds / rt). |
| `epa` | Float64 | Total expected points added on the player's targets. |
| `epa_tgt` | Float64 | Expected points added per target (epa / tgt). |
| `epa_rt` | Float64 | Expected points added per route run (epa / rt). |
| `drop` | Int64 | Drops, season total. |
| `drop_tgt` | Float64 | Drop rate: drops per target (drop / tgt). |
| `yac` | Int64 | Yards after the catch, season total. |
| `x_yac` | Int64 | Expected yards after the catch, season total, per Next Gen Stats; yac = x_yac + yacoe. |
| `yacoe` | Int64 | Yards after the catch over expected, season total (yac - x_yac). |
| `yac_rec` | Float64 | Yards after the catch per reception (yac / rec; nflverse `avg_yac`). |
| `avg_sep` | Float64 | Average separation in yards from the nearest defender at pass arrival, per Next Gen Stats (nflverse `avg_separation`). |
| `ay` | Float64 | Total intended air yards on the player's targets. |
| `ay_tgt` | Float64 | Average intended air yards per target (ay / tgt; nflverse `avg_intended_air_yards`). |
| `tgt_rt` | Float64 | Target rate: targets per route run (tgt / rt). |
| `avg_rt_dep` | Float64 | Average route depth in yards, per Next Gen Stats. |
| `ez_tgt` | Int64 | End-zone targets, season total. |
| `ez_rec` | Int64 | End-zone receptions, season total. |
| `deep_tgt_pct` | Float64 | Share of targets NGS classifies as deep. |
| `tw_pct` | Float64 | Share of targets thrown into a tight window (defender within a yard), per Next Gen Stats. |
| `qr` | Boolean | Whether the player meets the league qualifying threshold for the table (the `qualified` filter). |
| `rt_pg` | Float64 | Routes run per game (rt / gp). |
| `tgt_pg` | Float64 | Targets per game (tgt / gp). |
| `rec_pg` | Float64 | Receptions per game (rec / gp). |
| `yds_pg` | Float64 | Receiving yards per game (yds / gp). |
| `td_pg` | Float64 | Receiving touchdowns per game (td / gp). |
| `int_pg` | Float64 | Interceptions on the player's targets per game (int / gp). |
| `epa_pg` | Float64 | Expected points added per game (epa / gp). |
| `drop_pg` | Float64 | Drops per game (drop / gp). |
| `yac_pg` | Float64 | Yards after the catch per game (yac / gp). |
| `x_yac_pg` | Float64 | Expected yards after the catch per game (x_yac / gp). |
| `yacoe_pg` | Float64 | Yards after the catch over expected per game (yacoe / gp). |
| `ay_pg` | Float64 | Intended air yards per game (ay / gp). |
| `ez_tgt_pg` | Float64 | End-zone targets per game (ez_tgt / gp). |
| `ez_rec_pg` | Float64 | End-zone receptions per game (ez_rec / gp). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_pro_players_offense_receiving_season-raises}

| Exception | Description |
|---|---|
| `NFLProAuthError` | No user-bound NFL Pro token could be resolved (pass `token=` or `email=`/`password=`, or set `NFLPRO_TOKEN`, or `NFLPRO_EMAIL`/`NFLPRO_PW`), or the resolved token carries no active NFL_PLUS_* plan, or pro.nfl.com answered 401 / 403 (an `AssetFetchError`). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. Pro.nfl.com answered 400 / 422, or ignored `offset`. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other non-2xx or a connection failure after retries, or a 2xx whose body is not a JSON object or array -- including the HTTP 200 with an empty body that is how pro.nfl.com rejects unsupported query params. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |

### Example {#nfl_pro_players_offense_receiving_season-example}

```python
nfl_pro_players_offense_receiving_season(season=2024, season_type='REG')
```

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
| `nfl_id` | String | NFL player id (`nflId`) as a string; the key the wrappers accept as `nfl_id`. |
| `display_name` | String | Player's full display name (e.g. 'Aaron Rodgers'). |
| `short_name` | String | Player's abbreviated name, first initial and surname (e.g. 'A.Rodgers'). |
| `headshot` | String | NFL headshot image URL template; the `{formatInstructions}` token must be replaced with a Cloudinary transform (e.g. `t_headshot_desktop`) before use. |
| `team_id` | String | NFL team id as a zero-padded string (e.g. '0200', '3430'); casting it to a number drops the leading zero. |
| `jersey_number` | Int64 | Jersey number the player wore in the season. |
| `position` | String | Roster position abbreviation (e.g. QB, WR, CB). |
| `position_group` | String | Position group the roster position rolls up to (e.g. QB, RB, WR, TE, DB). |
| `ngs_position` | String | Position Next Gen Stats assigns from tracking data; null for players NGS has not classified. |
| `ngs_position_group` | String | Position group of ngs_position; null when ngs_position is null. |
| `gp` | Int64 | Games played in the season (and season type) -- the denominator of every `*_pg` per-game column. |
| `gs` | Int64 | Games started in the season. |
| `tg` | Int64 | Games the player's team played while he was on the roster; equals gp in sampled data. |
| `total_tg` | Int64 | Total games the player's team played in the season type (17 for a full regular season). |
| `week_slug` | String | Week slug of the game (e.g. 'WEEK_1', 'WEEK_18'); the week scope of the row. |
| `game_id` | Int64 | NFL game id as an integer (e.g. 2024090900, the date followed by a two-digit sequence). |
| `fapi_game_id` | String | NFL Football API (FAPI) UUID of the game, the id api.nfl.com uses for the same game. |
| `opponent_team_id` | String | Opponent's NFL team id as a zero-padded string. |
| `is_home` | Boolean | Whether the player's team was the home team in the game. |
| `final_score` | String | Final score as 'own-opponent' (e.g. '19-32' for a 32-19 loss). |
| `game_result` | String | Result from the player's team's side: 'W', 'L' or 'T'. |
| `rt` | Int64 | Routes run in the game. |
| `tgt` | Int64 | Targets in the game. |
| `rec` | Int64 | Receptions in the game. |
| `yds` | Int64 | Receiving yards in the game. |
| `td` | Int64 | Receiving touchdowns in the game. |
| `int` | Int64 | Interceptions thrown on passes targeting the player in the game. |
| `rating` | Int64 | Passer rating on throws targeting the player (the standard formula applied to his targets). |
| `catch` | Int64 | Catch rate as a fraction (rec / tgt; nflverse `catch_percentage` / 100). |
| `x_catch` | Int64 | Expected catch rate as a fraction, per Next Gen Stats; catch = x_catch + croe. |
| `croe` | Int64 | Catch rate over expected as a fraction (catch - x_catch). |
| `yds_rec` | Int64 | Yards per reception (yds / rec). |
| `yds_rt` | Int64 | Yards per route run (yds / rt). |
| `epa` | Int64 | Total expected points added on the player's targets. |
| `epa_tgt` | Int64 | Expected points added per target (epa / tgt). |
| `epa_rt` | Int64 | Expected points added per route run (epa / rt). |
| `drop` | Int64 | Drops in the game. |
| `drop_tgt` | Int64 | Drop rate: drops per target (drop / tgt). |
| `yac` | Int64 | Yards after the catch in the game. |
| `x_yac` | Int64 | Expected yards after the catch in the game, per Next Gen Stats; yac = x_yac + yacoe. |
| `yacoe` | Int64 | Yards after the catch over expected in the game (yac - x_yac). |
| `yac_rec` | Int64 | Yards after the catch per reception (yac / rec; nflverse `avg_yac`). |
| `avg_sep` | Int64 | Average separation in yards from the nearest defender at pass arrival, per Next Gen Stats (nflverse `avg_separation`). |
| `ay` | Int64 | Total intended air yards on the player's targets. |
| `ay_tgt` | Int64 | Average intended air yards per target (ay / tgt; nflverse `avg_intended_air_yards`). |
| `tgt_rt` | Int64 | Target rate: targets per route run (tgt / rt). |
| `avg_rt_dep` | Int64 | Average route depth in yards, per Next Gen Stats. |
| `ez_tgt` | Int64 | End-zone targets in the game. |
| `ez_rec` | Int64 | End-zone receptions in the game. |
| `deep_tgt_pct` | Int64 | Share of targets NGS classifies as deep. |
| `tw_pct` | Int64 | Share of targets thrown into a tight window (defender within a yard), per Next Gen Stats. |
| `qr` | Boolean | Whether the player meets the league qualifying threshold for the table (the `qualified` filter). |
| `rt_pg` | Int64 | Routes run per game (rt / gp). |
| `tgt_pg` | Int64 | Targets per game (tgt / gp). |
| `rec_pg` | Int64 | Receptions per game (rec / gp). |
| `yds_pg` | Int64 | Receiving yards per game (yds / gp). |
| `td_pg` | Int64 | Receiving touchdowns per game (td / gp). |
| `int_pg` | Int64 | Interceptions on the player's targets per game (int / gp). |
| `epa_pg` | Int64 | Expected points added per game (epa / gp). |
| `drop_pg` | Int64 | Drops per game (drop / gp). |
| `yac_pg` | Int64 | Yards after the catch per game (yac / gp). |
| `x_yac_pg` | Int64 | Expected yards after the catch per game (x_yac / gp). |
| `yacoe_pg` | Int64 | Yards after the catch over expected per game (yacoe / gp). |
| `ay_pg` | Int64 | Intended air yards per game (ay / gp). |
| `ez_tgt_pg` | Int64 | End-zone targets per game (ez_tgt / gp). |
| `ez_rec_pg` | Int64 | End-zone receptions per game (ez_rec / gp). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nfl_pro_players_offense_receiving_week-raises}

| Exception | Description |
|---|---|
| `NFLProAuthError` | No user-bound NFL Pro token could be resolved (pass `token=` or `email=`/`password=`, or set `NFLPRO_TOKEN`, or `NFLPRO_EMAIL`/`NFLPRO_PW`), or the resolved token carries no active NFL_PLUS_* plan, or pro.nfl.com answered 401 / 403 (an `AssetFetchError`). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. Pro.nfl.com answered 400 / 422, or ignored `offset`. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other non-2xx or a connection failure after retries, or a 2xx whose body is not a JSON object or array -- including the HTTP 200 with an empty body that is how pro.nfl.com rejects unsupported query params. |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |

### Example {#nfl_pro_players_offense_receiving_week-example}

```python
nfl_pro_players_offense_receiving_week(season=2024, season_type='REG')
```
