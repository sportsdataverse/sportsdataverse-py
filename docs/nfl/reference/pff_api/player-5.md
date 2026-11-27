# NFL — PFF Developer API (api.pff.com, API key) — Player: receiving–special

> NFL — PFF Developer API (api.pff.com, API key) — Player: receiving–special — function reference in sdv-py, the SportsDataverse Python package.

## pff_api_player_receiving_summary

Receiving summary for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/receiving/summary`

**Valid URL:** [https://api.pff.com/v1/player/receiving/summary?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/receiving/summary?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns {#pff_api_player_receiving_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `targets` | Int64 | Times targeted. |
| `grades_offense` | Float64 | PFF overall offense grade (0-100). |
| `game_id` | Int64 | PFF game id of the game (integer join key). |
| `yards_after_catch_per_reception` | Float64 | Average yards after the catch per reception. |
| `grades_pass_route` | Float64 | PFF receiving/route grade (0-100). |
| `week` | Int64 | Week number of the game, as PFF numbers weeks. |
| `status` | String | "S" when the player started the game; PFF owns the value set. |
| `yprr` | Float64 | Yards per route run. |
| `wide_snaps` | Int64 | Receiving snaps aligned out wide. |
| `fumbles` | Int64 | Fumbles by the player after the catch. |
| `first_downs` | Int64 | Receptions that converted a first down. |
| `jersey_number` | String | Jersey number the player wore in the game (string; zero-padded, e.g. "09"). |
| `inline_snaps` | Int64 | Receiving snaps aligned inline, tight to the formation. |
| `away_team_name` | String | Abbreviation of the away team (e.g. "LV"). |
| `contested_targets` | Int64 | Contested targets. |
| `inline_rate` | Float64 | Share of receiving snaps aligned inline, tight to the formation. |
| `contested_catch_rate` | Float64 | Percentage of PFF-charted contested targets caught. |
| `yards` | Int64 | Receiving yards. |
| `player_franchise_id` | Int64 | PFF franchise id of the team the player played for in the game. |
| `receptions` | Int64 | Passes caught by the receiver. |
| `targeted_qb_rating` | Float64 | NFL passer rating on throws targeting the player. |
| `interceptions` | Int64 | Interceptions on passes targeting the player. |
| `caught_percent` | Float64 | Percentage of targets caught. |
| `positive_epa_plays` | Int64 | Plays with positive expected points added (positive_epa_percent = positive_epa_plays / plays_with_epa x 100). |
| `drop_rate` | Float64 | Share of catchable targets the player dropped. |
| `grades_hands_drop` | Float64 | PFF hands/drop grade (0-100). |
| `slot_rate` | Float64 | Share of receiving snaps aligned in the slot. |
| `plays_with_epa` | Int64 | Plays with an expected-points-added value, the denominator of positive_epa_percent. |
| `slot_snaps` | Int64 | Receiving snaps aligned in the slot. |
| `penalties` | Int64 | Penalties charged to the player over the covered span. |
| `wide_rate` | Float64 | Share of receiving snaps aligned out wide. |
| `pass_block_rate` | Float64 | Share of pass-play snaps spent pass blocking. |
| `declined_penalties` | Int64 | Penalties committed by the player that were declined. |
| `route_rate` | Float64 | Share of pass-play snaps on which the player ran a route. |
| `drops` | Int64 | Dropped passes. |
| `position` | String | PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `grades_hands_fumble` | Float64 | PFF ball-security (hands/fumble) grade, 0-100. |
| `longest` | Int64 | Longest reception in yards. |
| `pass_blocks` | Int64 | Pass-play snaps spent pass blocking. |
| `away_franchise_id` | Int64 | PFF franchise id of the away team. |
| `routes` | Int64 | Pass routes run by the receiver. |
| `pass_plays` | Int64 | Pass-play snaps. |
| `yards_per_reception` | Float64 | Average yards per reception. |
| `targets_percent` | Float64 | Targets per route run, as a percentage (targets / routes x 100). |
| `positive_epa_percent` | Float64 | Percentage of the receiver's plays with an EPA value (in practice their routes run) producing positive expected points added (positive_epa_plays / plays_with_epa x 100). |
| `contested_receptions` | Int64 | Contested catches made. |
| `yards_after_catch` | Int64 | Yards after the catch. |
| `home_franchise_id` | Int64 | PFF franchise id of the home team. |
| `home_team_name` | String | Abbreviation of the home team (e.g. "NE"). |
| `avg_depth_of_target` | Float64 | Average depth of target in yards downfield. |
| `epa` | Float64 | Expected points added per target to the player, as computed by PFF (an average such as -0.27, not a total). |
| `avoided_tackles` | Int64 | Tackles avoided after the catch. |
| `player_id` | Int64 | PFF player id (integer join key) of the player the report is about. |
| `touchdowns` | Int64 | Receiving touchdowns. |
| `game_away_franchise_id` | Int64 | Repeats away_franchise_id from the row's nested game object: PFF franchise id of the away team. |
| `game_away_team_name` | String | Repeats away_team_name from the row's nested game object: Abbreviation of the away team (e.g. "LV"). |
| `game_game_id` | Int64 | Repeats game_id from the row's nested game object: PFF game id of the game (integer join key). |
| `game_home_franchise_id` | Int64 | Repeats home_franchise_id from the row's nested game object: PFF franchise id of the home team. |
| `game_home_team_name` | String | Repeats home_team_name from the row's nested game object: Abbreviation of the home team (e.g. "NE"). |
| `game_player_franchise_id` | Int64 | Repeats player_franchise_id from the row's nested game object: PFF franchise id of the team the player played for in the game. |
| `game_position` | String | Repeats position from the row's nested game object: PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `game_status` | String | Repeats status from the row's nested game object: "S" when the player started the game; PFF owns the value set. |
| `game_week` | Int64 | Repeats week from the row's nested game object: Week number of the game, as PFF numbers weeks. |
| `league_id` | Int64 | PFF league id (1 = NFL, 2 = NCAA), filled from the report's subject. |
| `season` | Int64 | Season (starting year) of the report, filled from the report's subject. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_player_receiving_summary-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_player_receiving_summary-example}

```python
pff_api_player_receiving_summary(league='nfl', player_id=28022, season=2022)
```

## pff_api_player_defense_summary

Defense summary for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/defense/summary`

**Valid URL:** [https://api.pff.com/v1/player/defense/summary?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/defense/summary?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns {#pff_api_player_defense_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `targets` | Int64 | Passes thrown into the player's coverage (targets allowed). |
| `game_id` | Int64 | PFF game id of the game (integer join key). |
| `interception_touchdowns` | Int64 | Touchdowns scored on interception returns. |
| `week` | Int64 | Week number of the game, as PFF numbers weeks. |
| `forced_fumbles` | Int64 | Forced fumbles. |
| `missed_tackles` | Int64 | Missed tackles. |
| `status` | String | "S" when the player started the game; PFF owns the value set. |
| `catch_rate` | Float64 | Completion percentage allowed on targets into the player's coverage. |
| `tackles` | Int64 | Total tackles made by the defender. |
| `jersey_number` | String | Jersey number the player wore in the game (string; zero-padded, e.g. "09"). |
| `away_team_name` | String | Abbreviation of the away team (e.g. "LV"). |
| `snap_counts_offball` | Int64 | Snaps aligned as an off-ball linebacker. |
| `snap_counts_box` | Int64 | Snaps aligned in the box. |
| `sacks` | Int64 | Sacks credited. |
| `snap_counts_pass_rush` | Int64 | Pass-rush snaps played. |
| `snap_counts_dl` | Int64 | Snaps aligned on the defensive line. |
| `grades_tackle` | Float64 | PFF tackling grade, 0-100. |
| `yards` | Int64 | Receiving yards allowed in the player's coverage. |
| `player_franchise_id` | Int64 | PFF franchise id of the team the player played for in the game. |
| `receptions` | Int64 | Receptions allowed in the player's coverage. |
| `grades_coverage_defense` | Float64 | PFF coverage grade (0-100). |
| `hurries` | Int64 | Quarterback hurries recorded. |
| `interceptions` | Int64 | Interceptions made in coverage. |
| `snap_counts_coverage` | Int64 | Coverage snaps played. |
| `snap_counts_dl_over_t` | Int64 | Defensive-line snaps aligned head-up over the offensive tackle. |
| `snap_counts_dl_a_gap` | Int64 | Defensive-line snaps aligned in the A gap. |
| `fumble_recoveries` | Int64 | Opponent fumbles recovered by the player. |
| `grades_run_defense` | Float64 | PFF run-defense grade (0-100). |
| `snap_counts_corner` | Int64 | Snaps aligned at outside cornerback. |
| `hits` | Int64 | Quarterback hits recorded by the player. |
| `penalties` | Int64 | Penalties charged to the player over the covered span. |
| `batted_passes` | Int64 | Passes batted down at the line of scrimmage. |
| `stops` | Int64 | Tackles that constitute an offensive failure ("stops"). |
| `declined_penalties` | Int64 | Penalties committed by the player that were declined. |
| `total_pressures` | Int64 | Total quarterback pressures (sacks + hits + hurries). |
| `position` | String | PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `fumble_recovery_touchdowns` | Int64 | Touchdowns scored on fumble recoveries. |
| `longest` | Int64 | Longest completion allowed, in yards. |
| `away_franchise_id` | Int64 | PFF franchise id of the away team. |
| `snap_counts_slot` | Int64 | Snaps aligned in the slot. |
| `missed_tackle_rate` | Float64 | Share of tackle attempts the player missed. |
| `grades_defense` | Float64 | PFF overall defense grade (0-100). |
| `yards_per_reception` | Float64 | Average yards allowed per reception. |
| `grades_defense_penalty` | Float64 | PFF defensive penalty grade, 0-100. |
| `safeties` | Int64 | Safeties recorded by the player. |
| `snap_counts_defense` | Int64 | Total defensive snaps played. |
| `yards_after_catch` | Int64 | Yards after the catch allowed in the player's coverage. |
| `home_franchise_id` | Int64 | PFF franchise id of the home team. |
| `snap_counts_dl_b_gap` | Int64 | Defensive-line snaps aligned in the B gap. |
| `home_team_name` | String | Abbreviation of the home team (e.g. "NE"). |
| `pass_break_ups` | Int64 | Passes broken up. |
| `qb_rating_against` | Float64 | NFL passer rating allowed on targets into the player's coverage. |
| `snap_counts_run_defense` | Int64 | Run-defense snaps played. |
| `tackles_for_loss` | Int64 | Tackles for loss made by the player. |
| `assists` | Int64 | Assisted tackles. |
| `grades_pass_rush_defense` | Float64 | PFF pass-rush grade, 0-100. |
| `player_id` | Int64 | PFF player id (integer join key) of the player the report is about. |
| `snap_counts_fs` | Int64 | Snaps aligned at free safety. |
| `touchdowns` | Int64 | Touchdowns allowed into the player's coverage. |
| `snap_counts_dl_outside_t` | Int64 | Defensive-line snaps aligned outside the offensive tackle. |
| `game_away_franchise_id` | Int64 | Repeats away_franchise_id from the row's nested game object: PFF franchise id of the away team. |
| `game_away_team_name` | String | Repeats away_team_name from the row's nested game object: Abbreviation of the away team (e.g. "LV"). |
| `game_game_id` | Int64 | Repeats game_id from the row's nested game object: PFF game id of the game (integer join key). |
| `game_home_franchise_id` | Int64 | Repeats home_franchise_id from the row's nested game object: PFF franchise id of the home team. |
| `game_home_team_name` | String | Repeats home_team_name from the row's nested game object: Abbreviation of the home team (e.g. "NE"). |
| `game_player_franchise_id` | Int64 | Repeats player_franchise_id from the row's nested game object: PFF franchise id of the team the player played for in the game. |
| `game_position` | String | Repeats position from the row's nested game object: PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `game_status` | String | Repeats status from the row's nested game object: "S" when the player started the game; PFF owns the value set. |
| `game_week` | Int64 | Repeats week from the row's nested game object: Week number of the game, as PFF numbers weeks. |
| `league_id` | Int64 | PFF league id (1 = NFL, 2 = NCAA), filled from the report's subject. |
| `season` | Int64 | Season (starting year) of the report, filled from the report's subject. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_player_defense_summary-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_player_defense_summary-example}

```python
pff_api_player_defense_summary(league='nfl', player_id=28022, season=2022)
```

## pff_api_player_field_goal_summary

Field-goal kicking for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/field_goal/summary`

**Valid URL:** [https://api.pff.com/v1/player/field_goal/summary?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/field_goal/summary?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns {#pff_api_player_field_goal_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `twenty_attempts` | Int64 | Field goals attempted from 20-29 yards. |
| `game_id` | Int64 | PFF game id of the game (integer join key). |
| `pat_percent` | Float64 | Extra-point percentage. |
| `week` | Int64 | Week number of the game, as PFF numbers weeks. |
| `status` | String | "S" when the player started the game; PFF owns the value set. |
| `forty_made` | Int64 | Field goals made from 40-49 yards. |
| `fifty_percent` | Float64 | Field-goal percentage from 50 or more yards. |
| `total_made` | Int64 | Total field goals made across all distances. |
| `jersey_number` | String | Jersey number the player wore in the game (string; zero-padded, e.g. "09"). |
| `away_team_name` | String | Abbreviation of the away team (e.g. "LV"). |
| `one_made` | Int64 | Field goals made from 1-19 yards. |
| `fifty_attempts` | Int64 | Field goals attempted from 50 or more yards. |
| `forty_attempts` | Int64 | Field goals attempted from 40-49 yards. |
| `thirty_percent` | Float64 | Field-goal percentage from 30-39 yards. |
| `total_attempts` | Int64 | Total field goals attempted across all distances. |
| `pat_attempts` | Int64 | Extra points attempted. |
| `player_franchise_id` | Int64 | PFF franchise id of the team the player played for in the game. |
| `twenty_made` | Int64 | Field goals made from 20-29 yards. |
| `one_attempts` | Int64 | Field goals attempted from 1-19 yards. |
| `grades_fgep_kicker` | Float64 | PFF field-goal and extra-point kicking grade, 0-100. |
| `thirty_attempts` | Int64 | Field goals attempted from 30-39 yards. |
| `pat_made` | Int64 | Extra points made. |
| `one_percent` | Float64 | Field-goal percentage from 1-19 yards. |
| `total_percent` | Float64 | Overall field-goal percentage. |
| `position` | String | PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `twenty_percent` | Float64 | Field-goal percentage from 20-29 yards. |
| `away_franchise_id` | Int64 | PFF franchise id of the away team. |
| `forty_percent` | Float64 | Field-goal percentage from 40-49 yards. |
| `fifty_made` | Int64 | Field goals made from 50 or more yards. |
| `thirty_made` | Int64 | Field goals made from 30-39 yards. |
| `home_franchise_id` | Int64 | PFF franchise id of the home team. |
| `home_team_name` | String | Abbreviation of the home team (e.g. "NE"). |
| `player_id` | Int64 | PFF player id (integer join key) of the player the report is about. |
| `game_away_franchise_id` | Int64 | Repeats away_franchise_id from the row's nested game object: PFF franchise id of the away team. |
| `game_away_team_name` | String | Repeats away_team_name from the row's nested game object: Abbreviation of the away team (e.g. "LV"). |
| `game_game_id` | Int64 | Repeats game_id from the row's nested game object: PFF game id of the game (integer join key). |
| `game_home_franchise_id` | Int64 | Repeats home_franchise_id from the row's nested game object: PFF franchise id of the home team. |
| `game_home_team_name` | String | Repeats home_team_name from the row's nested game object: Abbreviation of the home team (e.g. "NE"). |
| `game_player_franchise_id` | Int64 | Repeats player_franchise_id from the row's nested game object: PFF franchise id of the team the player played for in the game. |
| `game_position` | String | Repeats position from the row's nested game object: PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `game_status` | String | Repeats status from the row's nested game object: "S" when the player started the game; PFF owns the value set. |
| `game_week` | Int64 | Repeats week from the row's nested game object: Week number of the game, as PFF numbers weeks. |
| `league_id` | Int64 | PFF league id (1 = NFL, 2 = NCAA), filled from the report's subject. |
| `season` | Int64 | Season (starting year) of the report, filled from the report's subject. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_player_field_goal_summary-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_player_field_goal_summary-example}

```python
pff_api_player_field_goal_summary(league='nfl', player_id=28022, season=2022)
```

## pff_api_player_kickoff_summary

Kickoffs for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/kickoff/summary`

**Valid URL:** [https://api.pff.com/v1/player/kickoff/summary?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/kickoff/summary?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns {#pff_api_player_kickoff_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `attempts` | Int64 | Kickoffs by the player. |
| `attempts_with_hangtime` | Int64 | Kickoffs with a PFF-recorded hangtime. |
| `average_distance` | Float64 | Average kickoff distance in yards. |
| `average_hangtime` | Float64 | Average kickoff hangtime in seconds. |
| `average_starting_field_position` | Float64 | Average opponent starting field position following the player's kickoffs. |
| `average_yards_per_return` | Float64 | Average return yards allowed per kickoff returned. |
| `away_franchise_id` | Int64 | PFF franchise id of the away team. |
| `away_team_name` | String | Abbreviation of the away team (e.g. "LV"). |
| `fair_catches` | Int64 | Kickoffs fair-caught by the return team. |
| `game_id` | Int64 | PFF game id of the game (integer join key). |
| `grades_kickoff_kicker` | Float64 | PFF kickoff kicking grade, 0-100. |
| `home_franchise_id` | Int64 | PFF franchise id of the home team. |
| `home_team_name` | String | Abbreviation of the home team (e.g. "NE"). |
| `jersey_number` | String | Jersey number the player wore in the game (string; zero-padded, e.g. "09"). |
| `kicked_yards` | Int64 | Total kickoff yards. |
| `kicks_returned` | Int64 | Kickoffs returned by the opponent. |
| `onside_kicks` | Int64 | Onside kicks attempted. |
| `percent_returned` | Float64 | Percentage of the player's kickoffs that were returned. |
| `player_franchise_id` | Int64 | PFF franchise id of the team the player played for in the game. |
| `player_id` | Int64 | PFF player id (integer join key) of the player the report is about. |
| `position` | String | PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `return_yards` | Int64 | Return yards gained by the return team on the player's kickoffs. |
| `status` | String | "S" when the player started the game; PFF owns the value set. |
| `total_hangtime` | Float64 | Total kickoff hangtime in seconds. |
| `touchbacks` | Int64 | Kickoffs resulting in touchbacks. |
| `week` | Int64 | Week number of the game, as PFF numbers weeks. |
| `game_away_franchise_id` | Int64 | Repeats away_franchise_id from the row's nested game object: PFF franchise id of the away team. |
| `game_away_team_name` | String | Repeats away_team_name from the row's nested game object: Abbreviation of the away team (e.g. "LV"). |
| `game_game_id` | Int64 | Repeats game_id from the row's nested game object: PFF game id of the game (integer join key). |
| `game_home_franchise_id` | Int64 | Repeats home_franchise_id from the row's nested game object: PFF franchise id of the home team. |
| `game_home_team_name` | String | Repeats home_team_name from the row's nested game object: Abbreviation of the home team (e.g. "NE"). |
| `game_player_franchise_id` | Int64 | Repeats player_franchise_id from the row's nested game object: PFF franchise id of the team the player played for in the game. |
| `game_position` | String | Repeats position from the row's nested game object: PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `game_status` | String | Repeats status from the row's nested game object: "S" when the player started the game; PFF owns the value set. |
| `game_week` | Int64 | Repeats week from the row's nested game object: Week number of the game, as PFF numbers weeks. |
| `league_id` | Int64 | PFF league id (1 = NFL, 2 = NCAA), filled from the report's subject. |
| `season` | Int64 | Season (starting year) of the report, filled from the report's subject. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_player_kickoff_summary-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_player_kickoff_summary-example}

```python
pff_api_player_kickoff_summary(league='nfl', player_id=28022, season=2022)
```

## pff_api_player_punting_summary

Punting for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/punting/summary`

**Valid URL:** [https://api.pff.com/v1/player/punting/summary?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/punting/summary?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns {#pff_api_player_punting_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | PFF game id of the game (integer join key). |
| `touchbacks` | Int64 | Punts resulting in touchbacks. |
| `attempts_with_hangtime` | Int64 | Punts with a PFF-recorded hangtime. |
| `percent_returned` | Float64 | Percentage of the player's punts that were returned. |
| `week` | Int64 | Week number of the game, as PFF numbers weeks. |
| `status` | String | "S" when the player started the game; PFF owns the value set. |
| `fair_catches` | Int64 | Punts fair-caught by the return team. |
| `jersey_number` | String | Jersey number the player wore in the game (string; zero-padded, e.g. "09"). |
| `away_team_name` | String | Abbreviation of the away team (e.g. "LV"). |
| `average_net_yards` | Float64 | Average net punting yards per attempt. |
| `yards` | Int64 | Gross punt yards: the summed distance of the player's punts, before any return. |
| `player_franchise_id` | Int64 | PFF franchise id of the team the player played for in the game. |
| `average_hangtime` | Float64 | Average punt hangtime in seconds. |
| `total_net_yards` | Int64 | Total net punting yards. |
| `attempts` | Int64 | Punts by the player. |
| `inside_twenties` | Int64 | Punts downed inside the opponent 20-yard line. |
| `out_of_bounds` | Int64 | Punts that went out of bounds. |
| `average_yards_per_return` | Float64 | Average return yards allowed per punt returned. |
| `total_hangtime` | Float64 | Total punt hangtime in seconds. |
| `returns` | Int64 | Punts returned by the opponent. |
| `position` | String | PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `long` | Int64 | Longest punt in yards. |
| `away_franchise_id` | Int64 | PFF franchise id of the away team. |
| `blocks` | Int64 | Punts that were blocked. |
| `average_yards_per_attempt` | Float64 | Average gross punting yards per attempt. |
| `grades_punter` | Float64 | PFF punting grade, 0-100. |
| `home_franchise_id` | Int64 | PFF franchise id of the home team. |
| `home_team_name` | String | Abbreviation of the home team (e.g. "NE"). |
| `return_yards` | Int64 | Return yards gained by the return team on the player's punts. |
| `downeds` | Int64 | Punts downed by the coverage unit. |
| `player_id` | Int64 | PFF player id (integer join key) of the player the report is about. |
| `snaps` | Int64 | Punting snaps played. |
| `game_away_franchise_id` | Int64 | Repeats away_franchise_id from the row's nested game object: PFF franchise id of the away team. |
| `game_away_team_name` | String | Repeats away_team_name from the row's nested game object: Abbreviation of the away team (e.g. "LV"). |
| `game_game_id` | Int64 | Repeats game_id from the row's nested game object: PFF game id of the game (integer join key). |
| `game_home_franchise_id` | Int64 | Repeats home_franchise_id from the row's nested game object: PFF franchise id of the home team. |
| `game_home_team_name` | String | Repeats home_team_name from the row's nested game object: Abbreviation of the home team (e.g. "NE"). |
| `game_player_franchise_id` | Int64 | Repeats player_franchise_id from the row's nested game object: PFF franchise id of the team the player played for in the game. |
| `game_position` | String | Repeats position from the row's nested game object: PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `game_status` | String | Repeats status from the row's nested game object: "S" when the player started the game; PFF owns the value set. |
| `game_week` | Int64 | Repeats week from the row's nested game object: Week number of the game, as PFF numbers weeks. |
| `league_id` | Int64 | PFF league id (1 = NFL, 2 = NCAA), filled from the report's subject. |
| `season` | Int64 | Season (starting year) of the report, filled from the report's subject. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_player_punting_summary-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_player_punting_summary-example}

```python
pff_api_player_punting_summary(league='nfl', player_id=28022, season=2022)
```

## pff_api_player_return_summary

Kick and punt returns for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/return/summary`

**Valid URL:** [https://api.pff.com/v1/player/return/summary?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/return/summary?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns {#pff_api_player_return_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `away_franchise_id` | Int64 | PFF franchise id of the away team. |
| `away_team_name` | String | Abbreviation of the away team (e.g. "LV"). |
| `game_id` | Int64 | PFF game id of the game (integer join key). |
| `grades_punt_return` | Float64 | PFF punt-return grade, 0-100. |
| `grades_return` | Float64 | PFF overall return grade, 0-100. |
| `home_franchise_id` | Int64 | PFF franchise id of the home team. |
| `home_team_name` | String | Abbreviation of the home team (e.g. "NE"). |
| `jersey_number` | String | Jersey number the player wore in the game (string; zero-padded, e.g. "09"). |
| `kickoff_attempts` | Int64 | Kickoff returns attempted. |
| `kickoff_count_of_yards` | Int64 | Kickoff returns counted toward the return-yardage figures (equal to kickoff_attempts in the captured rows). |
| `kickoff_fair_catches` | Int64 | Kickoffs fair-caught by the player. |
| `kickoff_long` | Int64 | Longest kickoff return in yards. |
| `kickoff_muffed_returns` | Int64 | Kickoff returns the player muffed. |
| `kickoff_touchdowns` | Int64 | Kickoff returns scoring a touchdown. |
| `kickoff_yards` | Int64 | Total kickoff-return yards. |
| `kickoff_ypa` | Float64 | Average yards per kickoff return. |
| `player_franchise_id` | Int64 | PFF franchise id of the team the player played for in the game. |
| `player_id` | Int64 | PFF player id (integer join key) of the player the report is about. |
| `position` | String | PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `punt_attempts` | Int64 | Punt returns attempted. |
| `punt_count_of_yards` | Int64 | Punt returns counted toward the return-yardage figures (equal to punt_attempts in the captured rows). |
| `punt_fair_catches` | Int64 | Punts fair-caught by the player. |
| `punt_long` | Int64 | Longest punt return in yards. |
| `punt_muffed_returns` | Int64 | Punt returns the player muffed. |
| `punt_touchdowns` | Int64 | Punt returns scoring a touchdown. |
| `punt_yards` | Int64 | Total punt-return yards. |
| `punt_ypa` | Float64 | Average yards per punt return. |
| `status` | String | "S" when the player started the game; PFF owns the value set. |
| `total_attempts` | Int64 | Total return attempts, kickoffs and punts combined. |
| `week` | Int64 | Week number of the game, as PFF numbers weeks. |
| `game_away_franchise_id` | Int64 | Repeats away_franchise_id from the row's nested game object: PFF franchise id of the away team. |
| `game_away_team_name` | String | Repeats away_team_name from the row's nested game object: Abbreviation of the away team (e.g. "LV"). |
| `game_game_id` | Int64 | Repeats game_id from the row's nested game object: PFF game id of the game (integer join key). |
| `game_home_franchise_id` | Int64 | Repeats home_franchise_id from the row's nested game object: PFF franchise id of the home team. |
| `game_home_team_name` | String | Repeats home_team_name from the row's nested game object: Abbreviation of the home team (e.g. "NE"). |
| `game_player_franchise_id` | Int64 | Repeats player_franchise_id from the row's nested game object: PFF franchise id of the team the player played for in the game. |
| `game_position` | String | Repeats position from the row's nested game object: PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `game_status` | String | Repeats status from the row's nested game object: "S" when the player started the game; PFF owns the value set. |
| `game_week` | Int64 | Repeats week from the row's nested game object: Week number of the game, as PFF numbers weeks. |
| `league_id` | Int64 | PFF league id (1 = NFL, 2 = NCAA), filled from the report's subject. |
| `season` | Int64 | Season (starting year) of the report, filled from the report's subject. |
| `grades_kick_return` | Float64 | PFF kickoff-return grade, 0-100. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_player_return_summary-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_player_return_summary-example}

```python
pff_api_player_return_summary(league='nfl', player_id=28022, season=2022)
```

## pff_api_player_special_summary

Special-teams summary for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/special/summary`

**Valid URL:** [https://api.pff.com/v1/player/special/summary?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/special/summary?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns {#pff_api_player_special_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `assists` | Int64 | Assisted tackles credited to the player on special-teams plays. |
| `away_franchise_id` | Int64 | PFF franchise id of the away team. |
| `away_team_name` | String | Abbreviation of the away team (e.g. "LV"). |
| `declined_penalties` | Int64 | Penalties committed by the player that were declined. |
| `game_id` | Int64 | PFF game id of the game (integer join key). |
| `grades_fgep_defense` | Float64 | PFF grade on field-goal and extra-point defense, 0-100. |
| `grades_misc_st` | Float64 | PFF miscellaneous special-teams grade, 0-100. |
| `grades_special_teams_penalty` | Float64 | PFF special-teams penalty grade, 0-100. |
| `home_franchise_id` | Int64 | PFF franchise id of the home team. |
| `home_team_name` | String | Abbreviation of the home team (e.g. "NE"). |
| `jersey_number` | String | Jersey number the player wore in the game (string; zero-padded, e.g. "09"). |
| `missed_tackles` | Int64 | Missed tackles on special-teams plays. |
| `penalties` | Int64 | Penalties charged to the player over the covered span. |
| `player_franchise_id` | Int64 | PFF franchise id of the team the player played for in the game. |
| `player_id` | Int64 | PFF player id (integer join key) of the player the report is about. |
| `position` | String | PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `pu_def_rush` | Int64 | PFF punt-defense rush count (PFF's pu_def_rush; PFF does not document it further). |
| `snap_counts_field_goal` | Int64 | Snaps on the field-goal and extra-point unit. |
| `snap_counts_field_goal_blocking` | Int64 | Snaps on the field-goal and extra-point block unit. |
| `snap_counts_kickoff` | Int64 | Snaps on the kickoff coverage unit. |
| `snap_counts_kickoff_return` | Int64 | Snaps on the kickoff return unit. |
| `snap_counts_punt_coverage` | Int64 | Snaps on the punt coverage unit. |
| `snap_counts_punt_return` | Int64 | Snaps on the punt return unit. |
| `status` | String | "S" when the player started the game; PFF owns the value set. |
| `tackles` | Int64 | Tackles made by the player on special-teams plays. |
| `week` | Int64 | Week number of the game, as PFF numbers weeks. |
| `game_away_franchise_id` | Int64 | Repeats away_franchise_id from the row's nested game object: PFF franchise id of the away team. |
| `game_away_team_name` | String | Repeats away_team_name from the row's nested game object: Abbreviation of the away team (e.g. "LV"). |
| `game_game_id` | Int64 | Repeats game_id from the row's nested game object: PFF game id of the game (integer join key). |
| `game_home_franchise_id` | Int64 | Repeats home_franchise_id from the row's nested game object: PFF franchise id of the home team. |
| `game_home_team_name` | String | Repeats home_team_name from the row's nested game object: Abbreviation of the home team (e.g. "NE"). |
| `game_player_franchise_id` | Int64 | Repeats player_franchise_id from the row's nested game object: PFF franchise id of the team the player played for in the game. |
| `game_position` | String | Repeats position from the row's nested game object: PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `game_status` | String | Repeats status from the row's nested game object: "S" when the player started the game; PFF owns the value set. |
| `game_week` | Int64 | Repeats week from the row's nested game object: Week number of the game, as PFF numbers weeks. |
| `league_id` | Int64 | PFF league id (1 = NFL, 2 = NCAA), filled from the report's subject. |
| `season` | Int64 | Season (starting year) of the report, filled from the report's subject. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_player_special_summary-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_player_special_summary-example}

```python
pff_api_player_special_summary(league='nfl', player_id=28022, season=2022)
```
