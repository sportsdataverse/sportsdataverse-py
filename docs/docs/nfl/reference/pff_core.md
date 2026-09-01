---
title: NFL — PFF Premium Stats (premium.pff.com)
sidebar_label: PFF Premium Stats (premium.pff.com)
description: "NFL — PFF Premium Stats (premium.pff.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 11
---
# NFL — PFF Premium Stats (premium.pff.com)

`sportsdataverse.nfl` — 46 endpoints.

## `pff_facet_run_defense_summary`

Facet report /defense/run (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/defense/run`

**Valid URL:** [https://premium.pff.com/api/v1/facet/defense/run](https://premium.pff.com/api/v1/facet/defense/run)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `assists` | numeric | Total assists. |
| `avg_depth_of_tackle` | numeric | Average depth downfield, in yards, at which the player made his tackles on run plays. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `forced_fumbles` | numeric | Fumbles forced by the player. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_coverage_defense` | numeric | PFF coverage grade, 0-100. |
| `grades_defense` | numeric | PFF overall defense grade, 0-100. |
| `grades_defense_penalty` | numeric | PFF defensive penalty grade, 0-100. |
| `grades_pass_rush_defense` | numeric | PFF pass-rush grade, 0-100. |
| `grades_run_defense` | numeric | PFF run-defense grade, 0-100. |
| `grades_tackle` | numeric | PFF tackling grade, 0-100. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `missed_tackle_rate` | numeric | Share of tackle attempts the player missed. |
| `missed_tackles` | numeric | Missed tackles. |
| `penalties` | numeric | Total number of penalties. |
| `player` | character | Player name |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `position` | character | Primary position as reported by NFL.com |
| `run_stop_opp` | numeric | Run-defense snaps PFF counts as run-stop opportunities. |
| `snap_counts_run` | numeric | Run-defense snaps played. |
| `stop_percent` | numeric | Percentage of run-stop opportunities converted into stops. |
| `stops` | numeric | Stops, PFF's tackles that constitute a failed play for the offense. |
| `tackles` | numeric | Team tackles. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_run_defense_summary()
```

_Last validated n/a._

## `pff_facet_field_goal_summary`

Facet report /field_goal/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/field_goal/summary`

**Valid URL:** [https://premium.pff.com/api/v1/facet/field_goal/summary](https://premium.pff.com/api/v1/facet/field_goal/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `twenty_attempts` | numeric | Field goals attempted from 20-29 yards. |
| `pat_percent` | numeric | Extra-point percentage. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `forty_made` | numeric | Field goals made from 40-49 yards. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `fifty_percent` | numeric | Field-goal percentage from 50 or more yards. |
| `total_made` | numeric | Total field goals made across all distances. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `one_made` | numeric | Field goals made from 1-19 yards. |
| `fifty_attempts` | numeric | Field goals attempted from 50 or more yards. |
| `forty_attempts` | numeric | Field goals attempted from 40-49 yards. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `thirty_percent` | numeric | Field-goal percentage from 30-39 yards. |
| `total_attempts` | numeric | Total field goals attempted across all distances. |
| `pat_attempts` | numeric | Extra points attempted. |
| `twenty_made` | numeric | Field goals made from 20-29 yards. |
| `one_attempts` | numeric | Field goals attempted from 1-19 yards. |
| `grades_fgep_kicker` | numeric | PFF field-goal and extra-point kicking grade, 0-100. |
| `thirty_attempts` | numeric | Field goals attempted from 30-39 yards. |
| `penalties` | numeric | Total number of penalties. |
| `pat_made` | numeric | Extra points made. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `one_percent` | numeric | Field-goal percentage from 1-19 yards. |
| `total_percent` | numeric | Overall field-goal percentage. |
| `position` | character | Primary position as reported by NFL.com |
| `twenty_percent` | numeric | Field-goal percentage from 20-29 yards. |
| `forty_percent` | numeric | Field-goal percentage from 40-49 yards. |
| `player` | character | Player name |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `fifty_made` | numeric | Field goals made from 50 or more yards. |
| `thirty_made` | numeric | Field goals made from 30-39 yards. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_field_goal_summary()
```

_Last validated n/a._

## `pff_facet_coverage_summary`

Facet report /defense/coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/defense/coverage`

**Valid URL:** [https://premium.pff.com/api/v1/facet/defense/coverage](https://premium.pff.com/api/v1/facet/defense/coverage)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `targets` | numeric | The number of pass plays where the player was the targeted receiver. |
| `yards_per_coverage_snap` | numeric | Receiving yards allowed per coverage snap. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `missed_tackles` | numeric | Missed tackles. |
| `catch_rate` | numeric | Completion percentage allowed on targets into the player's coverage. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `tackles` | numeric | Team tackles. |
| `coverage_percent` | numeric | Share of pass-play snaps spent in coverage. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `dropped_ints` | numeric | Interception chances PFF charted as dropped. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `grades_tackle` | numeric | PFF tackling grade, 0-100. |
| `yards` | numeric | The number of receiving yards |
| `receptions` | numeric | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `forced_incompletion_rate` | numeric | Share of targets into the player's coverage with a PFF-charted forced incompletion. |
| `grades_coverage_defense` | numeric | PFF coverage grade, 0-100. |
| `interceptions` | numeric | The number of interceptions thrown. |
| `snap_counts_coverage` | numeric | Coverage snaps played. |
| `grades_run_defense` | numeric | PFF run-defense grade, 0-100. |
| `snap_counts_pass_play` | numeric | Pass-play snaps. |
| `penalties` | numeric | Total number of penalties. |
| `forced_incompletes` | numeric | Incompletions forced by the player's coverage, per PFF charting. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `coverage_snaps_per_target` | numeric | Coverage snaps played per target into the player's coverage. |
| `stops` | numeric | Stops, PFF's tackles that constitute a failed play for the offense. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `position` | character | Primary position as reported by NFL.com |
| `longest` | numeric | Longest completion allowed, in yards. |
| `missed_tackle_rate` | numeric | Share of tackle attempts the player missed. |
| `grades_defense` | numeric | PFF overall defense grade, 0-100. |
| `yards_per_reception` | numeric | Average yards allowed per reception. |
| `grades_defense_penalty` | numeric | PFF defensive penalty grade, 0-100. |
| `player` | character | Player name |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `yards_after_catch` | numeric | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `avg_depth_of_target` | numeric | Average depth of targets into the player's coverage, in yards downfield. |
| `pass_break_ups` | numeric | Passes broken up. |
| `qb_rating_against` | numeric | NFL passer rating allowed on targets into the player's coverage. |
| `coverage_snaps_per_reception` | numeric | Coverage snaps played per reception allowed. |
| `assists` | numeric | Total assists. |
| `grades_pass_rush_defense` | numeric | PFF pass-rush grade, 0-100. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `touchdowns` | numeric | Touchdowns allowed into the player's coverage. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_coverage_summary()
```

_Last validated n/a._

## `pff_facet_kicking_summary`

Facet report /kickoff/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/kickoff/summary`

**Valid URL:** [https://premium.pff.com/api/v1/facet/kickoff/summary](https://premium.pff.com/api/v1/facet/kickoff/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `attempts` | numeric | The number of pass attempts as defined by the NFL. |
| `attempts_with_hangtime` | numeric | Kickoffs with a PFF-recorded hangtime. |
| `average_distance` | numeric | Average kickoff distance in yards. |
| `average_hangtime` | numeric | Average kickoff hangtime in seconds. |
| `average_starting_field_position` | numeric | Average opponent starting field position following the player's kickoffs. |
| `average_yards_per_return` | numeric | Average return yards allowed per kickoff returned. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `fair_catches` | numeric | Kickoffs fair-caught by the return team. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_kickoff_kicker` | numeric | PFF kickoff kicking grade, 0-100. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `kicked_yards` | numeric | Total kickoff yards. |
| `kicks_returned` | numeric | Kickoffs returned by the opponent. |
| `onside_kicks` | numeric | Onside kicks attempted. |
| `penalties` | numeric | Total number of penalties. |
| `percent_returned` | numeric | Percentage of the player's kickoffs that were returned. |
| `player` | character | Player name |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `position` | character | Primary position as reported by NFL.com |
| `return_yards` | numeric | Yards gained by the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `total_hangtime` | numeric | Total kickoff hangtime in seconds. |
| `touchbacks` | numeric | Kickoffs resulting in touchbacks. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_kicking_summary()
```

_Last validated n/a._

## `pff_facet_blocking_summary`

Facet report /offense/blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/offense/blocking`

**Valid URL:** [https://premium.pff.com/api/v1/facet/offense/blocking](https://premium.pff.com/api/v1/facet/offense/blocking)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `grades_pass_block` | numeric | PFF pass-blocking grade, 0-100. |
| `grades_offense` | numeric | PFF overall offense grade, 0-100. |
| `pbe` | numeric | PFF Pass Blocking Efficiency rating, pressures allowed per pass-blocking snap weighted toward sacks. |
| `non_spike_pass_block_percentage` | numeric | Share of non-spike pass-play snaps spent pass blocking. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `snap_counts_rg` | numeric | Snaps aligned at right guard. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `snap_counts_ce` | numeric | Snaps aligned at center. |
| `hits_allowed` | numeric | Quarterback hits allowed. |
| `block_percent` | numeric | Share of offensive snaps spent blocking. |
| `snap_counts_offense` | numeric | Offensive snaps played. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `snap_counts_block` | numeric | Total blocking snaps played. |
| `hurries_allowed` | numeric | Quarterback hurries allowed. |
| `grades_run_block` | numeric | PFF run-blocking grade, 0-100. |
| `snap_counts_run_block` | numeric | Run-blocking snaps played. |
| `pressures_allowed` | numeric | Total pressures allowed (sacks, hits, and hurries). |
| `snap_counts_pass_play` | numeric | Pass-play snaps. |
| `penalties` | numeric | Total number of penalties. |
| `sacks_allowed` | numeric | Opponent sacks. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `snap_counts_te` | numeric | Snaps aligned at tight end. |
| `snap_counts_rt` | numeric | Snaps aligned at right tackle. |
| `position` | character | Primary position as reported by NFL.com |
| `non_spike_pass_block` | numeric | Pass-blocking snaps excluding spike plays. |
| `snap_counts_lt` | numeric | Snaps aligned at left tackle. |
| `player` | character | Player name |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `snap_counts_pass_block` | numeric | Pass-blocking snaps played. |
| `pass_block_percent` | numeric | Share of pass-play snaps spent pass blocking. |
| `snap_counts_lg` | numeric | Snaps aligned at left guard. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_blocking_summary()
```

_Last validated n/a._

## `pff_facet_defense_summary`

Facet report /defense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/defense/summary`

**Valid URL:** [https://premium.pff.com/api/v1/facet/defense/summary](https://premium.pff.com/api/v1/facet/defense/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `targets` | numeric | The number of pass plays where the player was the targeted receiver. |
| `interception_touchdowns` | numeric | Touchdowns scored on interception returns. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `forced_fumbles` | numeric | Forced fumbles. |
| `missed_tackles` | numeric | Missed tackles. |
| `catch_rate` | numeric | Completion percentage allowed on targets into the player's coverage. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `tackles` | numeric | Total tackles made by the defender. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `snap_counts_offball` | numeric | Snaps aligned as an off-ball linebacker. |
| `snap_counts_box` | numeric | Snaps aligned in the box. |
| `sacks` | numeric | Sacks credited. |
| `snap_counts_pass_rush` | numeric | Pass-rush snaps played. |
| `player_game_count` | numeric | Games with at least one qualifying snap in the requested range. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `snap_counts_dl` | numeric | Snaps aligned on the defensive line. |
| `grades_tackle` | numeric | PFF tackling grade, 0-100. |
| `yards` | numeric | The number of receiving yards |
| `receptions` | numeric | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `grades_coverage_defense` | numeric | PFF coverage grade (0-100). |
| `hurries` | numeric | Quarterback hurries recorded. |
| `interceptions` | numeric | Interceptions made in coverage. |
| `snap_counts_coverage` | numeric | Coverage snaps played. |
| `snap_counts_dl_over_t` | numeric | Defensive-line snaps aligned head-up over the offensive tackle. |
| `snap_counts_dl_a_gap` | numeric | Defensive-line snaps aligned in the A gap. |
| `fumble_recoveries` | numeric | Opponent fumbles recovered by the player. |
| `grades_run_defense` | numeric | PFF run-defense grade (0-100). |
| `snap_counts_corner` | numeric | Snaps aligned at outside cornerback. |
| `hits` | numeric | Hits. |
| `penalties` | numeric | Total number of penalties. |
| `batted_passes` | numeric | Passes batted down at the line of scrimmage. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `stops` | numeric | Tackles that constitute an offensive failure ("stops"). |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `total_pressures` | numeric | Total quarterback pressures (sacks + hits + hurries). |
| `position` | character | Primary position as reported by NFL.com |
| `fumble_recovery_touchdowns` | numeric | Touchdowns scored on fumble recoveries. |
| `longest` | numeric | Longest completion allowed, in yards. |
| `snap_counts_slot` | numeric | Snaps aligned in the slot. |
| `missed_tackle_rate` | numeric | Share of tackle attempts the player missed. |
| `grades_defense` | numeric | PFF overall defense grade (0-100). |
| `yards_per_reception` | numeric | Average yards allowed per reception. |
| `grades_defense_penalty` | numeric | PFF defensive penalty grade, 0-100. |
| `safeties` | numeric | Safeties recorded by the player. |
| `player` | character | Player name |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `snap_counts_defense` | numeric | Total defensive snaps played. |
| `yards_after_catch` | numeric | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `snap_counts_dl_b_gap` | numeric | Defensive-line snaps aligned in the B gap. |
| `pass_break_ups` | numeric | Passes broken up. |
| `qb_rating_against` | numeric | NFL passer rating allowed on targets into the player's coverage. |
| `snap_counts_run_defense` | numeric | Run-defense snaps played. |
| `tackles_for_loss` | numeric | Team tackles for a loss. |
| `assists` | numeric | Assisted tackles. |
| `grades_pass_rush_defense` | numeric | PFF pass-rush grade, 0-100. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `snap_counts_fs` | numeric | Snaps aligned at free safety. |
| `touchdowns` | numeric | Touchdowns allowed into the player's coverage. |
| `snap_counts_dl_outside_t` | numeric | Defensive-line snaps aligned outside the offensive tackle. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_defense_summary()
```

_Last validated n/a._

## `pff_facet_offense_summary`

Facet report /offense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/offense/summary`

**Valid URL:** [https://premium.pff.com/api/v1/facet/offense/summary](https://premium.pff.com/api/v1/facet/offense/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `grades_hands_fumble` | numeric | PFF ball-security (hands/fumble) grade, 0-100. |
| `grades_offense` | numeric | PFF overall offense grade (0-100). |
| `grades_offense_penalty` | numeric | PFF offensive penalty grade, 0-100. |
| `grades_pass` | numeric | PFF passing grade (0-100). |
| `grades_run` | numeric | PFF rushing grade (0-100). |
| `grades_run_block` | numeric | PFF run-blocking grade (0-100). |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `penalties` | numeric | Total number of penalties. |
| `player` | character | Player name |
| `player_game_count` | numeric | Games with at least one qualifying snap in the requested range. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `position` | character | Primary position as reported by NFL.com |
| `snap_counts_pass` | numeric | Pass-play snaps spent as the passer, rather than blocking or running a route. |
| `snap_counts_pass_block` | numeric | Pass-blocking snaps played. |
| `snap_counts_pass_route` | numeric | Snaps spent running a pass route. |
| `snap_counts_run` | numeric | Run-play snaps spent as a runner, rather than run blocking. |
| `snap_counts_run_block` | numeric | Run-blocking snaps played. |
| `snap_counts_total` | numeric | Total offensive snaps played. |
| `snap_counts_total_pass` | numeric | Total pass-play snaps across passing, pass blocking, and route running. |
| `snap_counts_total_run` | numeric | Total run-play snaps across rushing and run blocking. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `grades_pass_block` | numeric | PFF pass-blocking grade (0-100). |
| `grades_pass_route` | numeric | PFF receiving/route grade (0-100). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_offense_summary()
```

_Last validated n/a._

## `pff_facet_passing_allowed_pressure`

Facet report /passing/allowed_pressure (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/passing/allowed_pressure`

**Valid URL:** [https://premium.pff.com/api/v1/facet/passing/allowed_pressure](https://premium.pff.com/api/v1/facet/passing/allowed_pressure)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `te_percent` | numeric | Share of allowed pressures attributed to tight ends, expressed as a percentage. |
| `draft_season` | numeric | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `pressures_lt` | numeric | Number of allowed pressures PFF attributes to left tackle. |
| `pressures_rg` | numeric | Number of allowed pressures PFF attributes to right guard. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `lt_percent` | numeric | Share of allowed pressures attributed to left tackle, expressed as a percentage. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `hits_allowed` | numeric | Number of quarterback hits allowed on the player's dropbacks, as charted by PFF. |
| `pressures_lg` | numeric | Number of allowed pressures PFF attributes to left guard. |
| `player_game_count` | numeric | Number of games the player appeared in during the period covered. |
| `eligible_season` | numeric | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `hurries_allowed` | numeric | Number of hurries allowed on the player's dropbacks, as charted by PFF. |
| `self_percent` | numeric | Share of allowed pressures attributed to the quarterback himself, expressed as a percentage. |
| `pressures_rt` | numeric | Number of allowed pressures PFF attributes to right tackle. |
| `pressures_allowed` | numeric | Total pressures allowed on the quarterback's dropbacks, as attributed by PFF. |
| `pressures_ol_te` | numeric | Number of allowed pressures PFF attributes to the offensive line and tight ends combined. |
| `pressures_ce` | numeric | Number of allowed pressures PFF attributes to center. |
| `penalties` | numeric | Total number of penalties. |
| `sacks_allowed` | numeric | Opponent sacks. |
| `ol_te_percent` | numeric | Share of allowed pressures attributed to the offensive line and tight ends combined, expressed as a percentage. |
| `allowed_pressure_dropbacks` | numeric | Number of dropbacks over which allowed pressures are attributed, from the PFF allowed-pressure facet. |
| `pressures_other` | numeric | Number of allowed pressures PFF attributes to other players outside the listed blocking positions. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric | Number of declined penalties committed by the player. |
| `ce_percent` | numeric | Share of allowed pressures attributed to center, expressed as a percentage. |
| `position` | character | Primary position as reported by NFL.com |
| `pressures_te` | numeric | Number of allowed pressures PFF attributes to tight ends. |
| `pressures_self` | numeric | Number of allowed pressures PFF attributes to the quarterback himself. |
| `lg_percent` | numeric | Share of allowed pressures attributed to left guard, expressed as a percentage. |
| `player` | character | Player name |
| `rt_percent` | numeric | Share of allowed pressures attributed to right tackle, expressed as a percentage. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `other_percent` | numeric | Share of allowed pressures attributed to other players outside the listed blocking positions, expressed as a percentage. |
| `pressures_off` | numeric | Number of allowed pressures PFF attributes to the offense without a specific blocker charged. |
| `rg_percent` | numeric | Share of allowed pressures attributed to right guard, expressed as a percentage. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_passing_allowed_pressure()
```

_Last validated n/a._

## `pff_facet_pass_rush_summary`

Facet report /defense/pass_rush (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/defense/pass_rush`

**Valid URL:** [https://premium.pff.com/api/v1/facet/defense/pass_rush](https://premium.pff.com/api/v1/facet/defense/pass_rush)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `true_pass_set_total_pressures` | numeric | Total pressures generated (sacks, hits, and hurries) on PFF-designated true pass sets. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `true_pass_set_prp` | numeric | PFF Pass Rush Productivity rating, pressure generated per pass-rush snap weighted toward sacks on PFF-designated true pass sets. |
| `true_pass_set_hurries` | numeric | Quarterback hurries recorded on PFF-designated true pass sets. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `prp` | numeric | PFF Pass Rush Productivity rating, pressure generated per pass-rush snap weighted toward sacks. |
| `true_pass_set_sacks` | numeric | Sacks recorded on PFF-designated true pass sets. |
| `pass_rush_win_rate` | numeric | Percentage of pass-rush snaps with a PFF-charted pass-rush win. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `sacks` | numeric | The Number of times sacked. |
| `snap_counts_pass_rush` | numeric | Pass-rush snaps played. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `true_pass_set_snap_counts_pass_play` | numeric | Pass-play snaps on PFF-designated true pass sets. |
| `pass_rush_wins` | numeric | PFF-charted pass-rush wins. |
| `hurries` | numeric | Quarterback hurries recorded. |
| `pass_rush_opp` | numeric | Pass-rush snaps PFF counts as pressure opportunities. |
| `snap_counts_pass_play` | numeric | Pass-play snaps. |
| `hits` | numeric | Hits. |
| `true_pass_set_pass_rush_win_rate` | numeric | Percentage of pass-rush snaps with a PFF-charted pass-rush win on PFF-designated true pass sets. |
| `penalties` | numeric | Total number of penalties. |
| `batted_passes` | numeric | Passes batted down at the line of scrimmage. |
| `true_pass_set_hits` | numeric | Quarterback hits recorded on PFF-designated true pass sets. |
| `true_pass_set_snap_counts_pass_rush` | numeric | Pass-rush snaps played on PFF-designated true pass sets. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `true_pass_set_pass_rush_wins` | numeric | PFF-charted pass-rush wins on PFF-designated true pass sets. |
| `total_pressures` | numeric | Total pressures generated (sacks, hits, and hurries). |
| `position` | character | Primary position as reported by NFL.com |
| `true_pass_set_grades_pass_rush_defense` | numeric | PFF pass-rush grade on PFF-designated true pass sets, 0-100. |
| `true_pass_set_batted_passes` | numeric | Passes batted down at the line of scrimmage on PFF-designated true pass sets. |
| `player` | character | Player name |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `pass_rush_percent` | numeric | Share of pass-play snaps spent rushing the passer. |
| `true_pass_set_pass_rush_opp` | numeric | Pass-rush snaps PFF counts as pressure opportunities on PFF-designated true pass sets. |
| `true_pass_set_pass_rush_percent` | numeric | Share of pass-play snaps spent rushing the passer on PFF-designated true pass sets. |
| `grades_pass_rush_defense` | numeric | PFF pass-rush grade, 0-100. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_pass_rush_summary()
```

_Last validated n/a._

## `pff_facet_passing_concept`

Facet report /passing/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/passing/concept`

**Valid URL:** [https://premium.pff.com/api/v1/facet/passing/concept](https://premium.pff.com/api/v1/facet/passing/concept)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `comp_pct_diff` | numeric | Difference in completion percentage between play-action and non-play-action attempts (PA minus non-PA), from the PFF passing-concept split. |
| `pa_grades_pass` | numeric | PFF passing grade (0-100) on play-action dropbacks. |
| `no_screen_grades_offense` | numeric | PFF overall offense grade for the player (0-100) excluding screen passes. |
| `pa_qb_rating` | numeric | Traditional NFL passer rating on play-action dropbacks. |
| `no_screen_qb_rating` | numeric | Traditional NFL passer rating excluding screen passes. |
| `pa_completions` | numeric | Number of completed passes on play-action dropbacks. |
| `pa_thrown_aways` | numeric | Number of intentional throwaways on play-action dropbacks. |
| `pa_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on play-action dropbacks, expressed as a percentage. |
| `ypa_diff` | numeric | Difference in yards per attempt between play-action and non-play-action attempts (PA minus non-PA), from the PFF passing-concept split. |
| `no_screen_drops` | numeric | Number of catchable passes dropped by receivers excluding screen passes. |
| `screen_completion_percent` | numeric | Percentage of pass attempts completed on screen passes. |
| `npa_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on non-play-action dropbacks. |
| `no_screen_thrown_aways` | numeric | Number of intentional throwaways excluding screen passes. |
| `pa_grades_run` | numeric | PFF rushing grade for the player (0-100) on play-action dropbacks. |
| `screen_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on screen passes. |
| `pa_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on play-action dropbacks. |
| `screen_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on screen passes. |
| `dropbacks` | numeric | Number of dropbacks. |
| `npa_thrown_aways` | numeric | Number of intentional throwaways on non-play-action dropbacks. |
| `no_screen_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks excluding screen passes, as charted by PFF. |
| `screen_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on screen passes. |
| `pa_touchdowns` | numeric | Number of passing touchdowns thrown on play-action dropbacks. |
| `npa_ypa` | numeric | Yards gained per pass attempt on non-play-action dropbacks. |
| `draft_season` | numeric | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `screen_avg_time_to_throw` | numeric | Average time from snap to release in seconds on screen passes. |
| `screen_thrown_aways` | numeric | Number of intentional throwaways on screen passes. |
| `npa_sacks` | numeric | Number of sacks taken on non-play-action dropbacks. |
| `npa_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on non-play-action dropbacks, as charted by PFF. |
| `no_screen_completions` | numeric | Number of completed passes excluding screen passes. |
| `no_screen_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added excluding screen passes. |
| `screen_spikes` | numeric | Number of clock-stopping spikes on screen passes. |
| `pa_first_downs` | numeric | Number of passing first downs gained on play-action dropbacks. |
| `pa_big_time_throws` | numeric | Number of big-time throws on play-action dropbacks, per PFF's highest-value, highest-difficulty throw designation. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `pa_spikes` | numeric | Number of clock-stopping spikes on play-action dropbacks. |
| `pa_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on play-action dropbacks. |
| `screen_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on screen passes, expressed as a percentage. |
| `no_screen_epa` | numeric | Total expected points added (EPA) on the player's dropbacks excluding screen passes. |
| `npa_avg_time_to_throw` | numeric | Average time from snap to release in seconds on non-play-action dropbacks. |
| `screen_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on screen passes. |
| `screen_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) on screen passes. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `no_screen_bats` | numeric | Number of pass attempts batted down at the line of scrimmage excluding screen passes. |
| `npa_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) excluding screen passes. |
| `screen_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on screen passes, as charted by PFF. |
| `pa_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on play-action dropbacks. |
| `screen_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on screen passes. |
| `no_screen_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts excluding screen passes, per PFF charting. |
| `npa_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on non-play-action dropbacks, per PFF charting. |
| `screen_interceptions` | numeric | Number of passes intercepted on screen passes. |
| `no_screen_passing_snaps` | numeric | Number of passing snaps played excluding screen passes. |
| `no_screen_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) excluding screen passes. |
| `screen_scrambles` | numeric | Number of scrambles on screen passes. |
| `screen_grades_pass` | numeric | PFF passing grade (0-100) on screen passes. |
| `npa_qb_rating` | numeric | Traditional NFL passer rating on non-play-action dropbacks. |
| `no_screen_grades_pass` | numeric | PFF passing grade (0-100) excluding screen passes. |
| `pa_avg_time_to_throw` | numeric | Average time from snap to release in seconds on play-action dropbacks. |
| `screen_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on screen passes, per PFF charting. |
| `player_game_count` | numeric | Number of games the player appeared in during the period covered. |
| `npa_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on non-play-action dropbacks. |
| `eligible_season` | numeric | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `npa_drops` | numeric | Number of catchable passes dropped by receivers on non-play-action dropbacks. |
| `screen_yards` | numeric | Passing yards gained on screen passes. |
| `no_screen_drop_rate` | numeric | Percentage of catchable passes dropped by receivers excluding screen passes. |
| `no_screen_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) excluding screen passes. |
| `npa_passing_snaps` | numeric | Number of passing snaps played on non-play-action dropbacks. |
| `screen_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on screen passes. |
| `screen_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on screen passes. |
| `screen_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) on screen passes. |
| `screen_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on screen passes. |
| `npa_spikes` | numeric | Number of clock-stopping spikes on non-play-action dropbacks. |
| `screen_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on screen passes, as charted by PFF. |
| `no_screen_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) excluding screen passes, as charted by PFF. |
| `screen_drops` | numeric | Number of catchable passes dropped by receivers on screen passes. |
| `screen_ypa` | numeric | Yards gained per pass attempt on screen passes. |
| `npa_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on non-play-action dropbacks, per PFF charting. |
| `screen_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on screen passes. |
| `npa_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on non-play-action dropbacks, as charted by PFF. |
| `pa_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on play-action dropbacks. |
| `pa_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on play-action dropbacks, as charted by PFF. |
| `screen_avg_depth_of_target` | numeric | Average depth of target in air yards on screen passes. |
| `pa_sacks` | numeric | Number of sacks taken on play-action dropbacks. |
| `screen_passing_snaps` | numeric | Number of passing snaps played on screen passes. |
| `no_screen_grades_run` | numeric | PFF rushing grade for the player (0-100) excluding screen passes. |
| `no_screen_first_downs` | numeric | Number of passing first downs gained excluding screen passes. |
| `pa_ypa` | numeric | Yards gained per pass attempt on play-action dropbacks. |
| `npa_scrambles` | numeric | Number of scrambles on non-play-action dropbacks. |
| `npa_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on non-play-action dropbacks. |
| `pa_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on play-action dropbacks, per PFF charting. |
| `no_screen_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts excluding screen passes, per PFF charting. |
| `npa_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on non-play-action dropbacks. |
| `screen_completions` | numeric | Number of completed passes on screen passes. |
| `screen_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on screen passes, per PFF charting. |
| `npa_grades_run` | numeric | PFF rushing grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_interceptions` | numeric | Number of passes intercepted excluding screen passes. |
| `npa_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_sacks` | numeric | Number of sacks taken excluding screen passes. |
| `penalties` | numeric | Total number of penalties. |
| `no_screen_big_time_throws` | numeric | Number of big-time throws excluding screen passes, per PFF's highest-value, highest-difficulty throw designation. |
| `npa_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on non-play-action dropbacks. |
| `npa_attempts` | numeric | Number of pass attempts on non-play-action dropbacks. |
| `screen_qb_rating` | numeric | Traditional NFL passer rating on screen passes. |
| `npa_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on non-play-action dropbacks. |
| `pa_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on play-action dropbacks. |
| `pa_attempts` | numeric | Number of pass attempts on play-action dropbacks. |
| `npa_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on non-play-action dropbacks, as charted by PFF. |
| `no_screen_avg_time_to_throw` | numeric | Average time from snap to release in seconds excluding screen passes. |
| `pa_yards` | numeric | Passing yards gained on play-action dropbacks. |
| `npa_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on non-play-action dropbacks. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `no_screen_scrambles` | numeric | Number of scrambles excluding screen passes. |
| `pa_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on play-action dropbacks, plays PFF charts as deserving of a turnover. |
| `pa_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on play-action dropbacks, as charted by PFF. |
| `declined_penalties` | numeric | Number of declined penalties committed by the player. |
| `pa_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on play-action dropbacks. |
| `screen_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on screen passes. |
| `no_screen_dropbacks` | numeric | Number of dropbacks excluding screen passes. |
| `no_screen_dropbacks_percent` | numeric | Share of the player's total dropbacks that came excluding screen passes, expressed as a percentage. |
| `npa_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) on non-play-action dropbacks. |
| `npa_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on non-play-action dropbacks. |
| `pa_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on play-action dropbacks. |
| `pa_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) on play-action dropbacks. |
| `no_screen_turnover_worthy_plays` | numeric | Number of turnover-worthy plays excluding screen passes, plays PFF charts as deserving of a turnover. |
| `position` | character | Primary position as reported by NFL.com |
| `pa_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) on play-action dropbacks. |
| `npa_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on non-play-action dropbacks. |
| `no_screen_avg_depth_of_target` | numeric | Average depth of target in air yards excluding screen passes. |
| `pa_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on play-action dropbacks. |
| `no_screen_completion_percent` | numeric | Percentage of pass attempts completed excluding screen passes. |
| `pa_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on play-action dropbacks, as charted by PFF. |
| `pa_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on play-action dropbacks. |
| `npa_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on non-play-action dropbacks, expressed as a percentage. |
| `npa_grades_pass` | numeric | PFF passing grade (0-100) on non-play-action dropbacks. |
| `screen_grades_run` | numeric | PFF rushing grade for the player (0-100) on screen passes. |
| `screen_first_downs` | numeric | Number of passing first downs gained on screen passes. |
| `npa_completion_percent` | numeric | Percentage of pass attempts completed on non-play-action dropbacks. |
| `no_screen_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) excluding screen passes. |
| `no_screen_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw excluding screen passes, as charted by PFF. |
| `screen_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on screen passes, plays PFF charts as deserving of a turnover. |
| `npa_avg_depth_of_target` | numeric | Average depth of target in air yards on non-play-action dropbacks. |
| `npa_dropbacks` | numeric | Number of dropbacks on non-play-action dropbacks. |
| `player` | character | Player name |
| `pa_drops` | numeric | Number of catchable passes dropped by receivers on play-action dropbacks. |
| `pa_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on play-action dropbacks, per PFF charting. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `screen_big_time_throws` | numeric | Number of big-time throws on screen passes, per PFF's highest-value, highest-difficulty throw designation. |
| `screen_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on screen passes, as charted by PFF. |
| `npa_touchdowns` | numeric | Number of passing touchdowns thrown on non-play-action dropbacks. |
| `screen_attempts` | numeric | Number of pass attempts on screen passes. |
| `screen_dropbacks` | numeric | Number of dropbacks on screen passes. |
| `no_screen_yards` | numeric | Passing yards gained excluding screen passes. |
| `pa_scrambles` | numeric | Number of scrambles on play-action dropbacks. |
| `pa_completion_percent` | numeric | Percentage of pass attempts completed on play-action dropbacks. |
| `pa_avg_depth_of_target` | numeric | Average depth of target in air yards on play-action dropbacks. |
| `pa_interceptions` | numeric | Number of passes intercepted on play-action dropbacks. |
| `no_screen_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF excluding screen passes. |
| `no_screen_spikes` | numeric | Number of clock-stopping spikes excluding screen passes. |
| `pa_dropbacks` | numeric | Number of dropbacks on play-action dropbacks. |
| `npa_big_time_throws` | numeric | Number of big-time throws on non-play-action dropbacks, per PFF's highest-value, highest-difficulty throw designation. |
| `no_screen_sack_percent` | numeric | Percentage of dropbacks that ended in a sack excluding screen passes. |
| `pa_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on play-action dropbacks. |
| `no_screen_attempts` | numeric | Number of pass attempts excluding screen passes. |
| `pa_passing_snaps` | numeric | Number of passing snaps played on play-action dropbacks. |
| `npa_completions` | numeric | Number of completed passes on non-play-action dropbacks. |
| `screen_touchdowns` | numeric | Number of passing touchdowns thrown on screen passes. |
| `npa_first_downs` | numeric | Number of passing first downs gained on non-play-action dropbacks. |
| `no_screen_touchdowns` | numeric | Number of passing touchdowns thrown excluding screen passes. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `npa_yards` | numeric | Passing yards gained on non-play-action dropbacks. |
| `no_screen_ypa` | numeric | Yards gained per pass attempt excluding screen passes. |
| `npa_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on non-play-action dropbacks, plays PFF charts as deserving of a turnover. |
| `npa_interceptions` | numeric | Number of passes intercepted on non-play-action dropbacks. |
| `no_screen_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack excluding screen passes. |
| `screen_sacks` | numeric | Number of sacks taken on screen passes. |
| `screen_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) on screen passes. |
| `screen_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) on screen passes. |
| `no_screen_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) excluding screen passes. |
| `pa_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) on play-action dropbacks. |
| `npa_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `pa_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) on play-action dropbacks. |
| `npa_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) excluding screen passes. |
| `pa_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) on play-action dropbacks. |
| `screen_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) on screen passes. |
| `no_screen_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) excluding screen passes. |
| `npa_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `pa_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) on play-action dropbacks. |
| `npa_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) on non-play-action dropbacks. |
| `npa_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) on non-play-action dropbacks. |
| `pa_grades_defense` | numeric | PFF overall defense grade for the player (0-100) on play-action dropbacks. |
| `screen_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) on screen passes. |
| `npa_grades_defense` | numeric | PFF overall defense grade for the player (0-100) on non-play-action dropbacks. |
| `screen_grades_defense` | numeric | PFF overall defense grade for the player (0-100) on screen passes. |
| `screen_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) on screen passes. |
| `no_screen_grades_defense` | numeric | PFF overall defense grade for the player (0-100) excluding screen passes. |
| `pa_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) on play-action dropbacks. |
| `no_screen_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) excluding screen passes. |
| `no_screen_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) excluding screen passes. |
| `no_screen_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) excluding screen passes. |
| `pa_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) on play-action dropbacks. |
| `pa_grades_tackle` | numeric | PFF tackling grade for the player (0-100) on play-action dropbacks. |
| `pa_grades_pass_rush_defense` | character | PFF pass-rush grade for the player (0-100) on play-action dropbacks. |
| `screen_grades_run_defense` | character | PFF run-defense grade for the player (0-100) on screen passes. |
| `pa_grades_run_defense` | character | PFF run-defense grade for the player (0-100) on play-action dropbacks. |
| `npa_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_grades_pass_rush_defense` | numeric | PFF pass-rush grade for the player (0-100) excluding screen passes. |
| `npa_grades_tackle` | numeric | PFF tackling grade for the player (0-100) on non-play-action dropbacks. |
| `screen_grades_tackle` | character | PFF tackling grade for the player (0-100) on screen passes. |
| `no_screen_grades_tackle` | numeric | PFF tackling grade for the player (0-100) excluding screen passes. |
| `npa_grades_pass_rush_defense` | numeric | PFF pass-rush grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_grades_run_defense` | numeric | PFF run-defense grade for the player (0-100) excluding screen passes. |
| `screen_grades_overall_tackle` | character | PFF overall tackling grade for the player (0-100) on screen passes. |
| `npa_grades_run_defense` | numeric | PFF run-defense grade for the player (0-100) on non-play-action dropbacks. |
| `screen_grades_pass_rush_defense` | numeric | PFF pass-rush grade for the player (0-100) on screen passes. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_passing_concept()
```

_Last validated n/a._

## `pff_facet_coverage_scheme`

Facet report /defense/coverage_scheme (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/defense/coverage_scheme`

**Valid URL:** [https://premium.pff.com/api/v1/facet/defense/coverage_scheme](https://premium.pff.com/api/v1/facet/defense/coverage_scheme)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `man_touchdowns` | numeric | Touchdowns allowed into the player's coverage when in man coverage. |
| `man_interceptions` | numeric | Interceptions made in coverage when in man coverage. |
| `zone_snap_counts_coverage_percent` | numeric | Share of the player's coverage snaps played when in zone coverage. |
| `man_avg_depth_of_target` | numeric | Average depth of targets into the player's coverage, in yards downfield when in man coverage. |
| `man_dropped_ints` | numeric | Interception chances PFF charted as dropped when in man coverage. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `zone_coverage_snaps_per_target` | numeric | Coverage snaps played per target into the player's coverage when in zone coverage. |
| `man_yards_per_reception` | numeric | Average yards allowed per reception when in man coverage. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `man_snap_counts_coverage` | numeric | Coverage snaps played when in man coverage. |
| `man_tackles` | numeric | Tackles made when in man coverage. |
| `zone_stops` | numeric | Stops, PFF's tackles that constitute a failed play for the offense when in zone coverage. |
| `zone_snap_counts_coverage` | numeric | Coverage snaps played when in zone coverage. |
| `zone_yards` | numeric | Receiving yards allowed when in zone coverage. |
| `man_coverage_snaps_per_target` | numeric | Coverage snaps played per target into the player's coverage when in man coverage. |
| `zone_coverage_percent` | numeric | Share of pass-play snaps spent in coverage when in zone coverage. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `zone_receptions` | numeric | Receptions allowed into the player's coverage when in zone coverage. |
| `zone_forced_incompletion_rate` | numeric | Share of targets into the player's coverage with a PFF-charted forced incompletion when in zone coverage. |
| `man_snap_counts_pass_play` | numeric | Pass-play snaps when in man coverage. |
| `zone_yards_per_reception` | numeric | Average yards allowed per reception when in zone coverage. |
| `man_longest` | numeric | Longest completion allowed, in yards when in man coverage. |
| `man_assists` | numeric | Assisted tackles when in man coverage. |
| `zone_yards_after_catch` | numeric | Yards after the catch allowed when in zone coverage. |
| `man_stops` | numeric | Stops, PFF's tackles that constitute a failed play for the offense when in man coverage. |
| `man_receptions` | numeric | Receptions allowed into the player's coverage when in man coverage. |
| `zone_tackles` | numeric | Tackles made when in zone coverage. |
| `man_coverage_percent` | numeric | Share of pass-play snaps spent in coverage when in man coverage. |
| `penalties` | numeric | Total number of penalties. |
| `zone_yards_per_coverage_snap` | numeric | Receiving yards allowed per coverage snap when in zone coverage. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `man_catch_rate` | numeric | Completion percentage allowed on targets into the player's coverage when in man coverage. |
| `man_grades_coverage_defense` | numeric | PFF coverage grade when in man coverage, 0-100. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `man_yards_after_catch` | numeric | Yards after the catch allowed when in man coverage. |
| `man_pass_break_ups` | numeric | Passes broken up when in man coverage. |
| `man_yards` | numeric | Receiving yards allowed when in man coverage. |
| `position` | character | Primary position as reported by NFL.com |
| `man_targets` | numeric | Targets into the player's coverage when in man coverage. |
| `man_missed_tackle_rate` | numeric | Share of tackle attempts the player missed when in man coverage. |
| `zone_assists` | numeric | Assisted tackles when in zone coverage. |
| `zone_snap_counts_pass_play` | numeric | Pass-play snaps when in zone coverage. |
| `zone_missed_tackle_rate` | numeric | Share of tackle attempts the player missed when in zone coverage. |
| `zone_touchdowns` | numeric | Touchdowns allowed into the player's coverage when in zone coverage. |
| `zone_coverage_snaps_per_reception` | numeric | Coverage snaps played per reception allowed when in zone coverage. |
| `man_coverage_snaps_per_reception` | numeric | Coverage snaps played per reception allowed when in man coverage. |
| `zone_avg_depth_of_target` | numeric | Average depth of targets into the player's coverage, in yards downfield when in zone coverage. |
| `man_snap_counts_coverage_percent` | numeric | Share of the player's coverage snaps played when in man coverage. |
| `player` | character | Player name |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `zone_forced_incompletes` | numeric | Incompletions forced by the player's coverage, per PFF charting when in zone coverage. |
| `man_forced_incompletion_rate` | numeric | Share of targets into the player's coverage with a PFF-charted forced incompletion when in man coverage. |
| `zone_targets` | numeric | Targets into the player's coverage when in zone coverage. |
| `zone_longest` | numeric | Longest completion allowed, in yards when in zone coverage. |
| `zone_dropped_ints` | numeric | Interception chances PFF charted as dropped when in zone coverage. |
| `zone_interceptions` | numeric | Interceptions made in coverage when in zone coverage. |
| `man_forced_incompletes` | numeric | Incompletions forced by the player's coverage, per PFF charting when in man coverage. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `zone_missed_tackles` | numeric | Missed tackles when in zone coverage. |
| `base_snap_counts_coverage` | numeric | Coverage snaps from the facet's unsplit base row, covering all coverage schemes. |
| `man_missed_tackles` | numeric | Missed tackles when in man coverage. |
| `zone_grades_coverage_defense` | numeric | PFF coverage grade when in zone coverage, 0-100. |
| `man_qb_rating_against` | numeric | NFL passer rating allowed on targets into the player's coverage when in man coverage. |
| `zone_pass_break_ups` | numeric | Passes broken up when in zone coverage. |
| `zone_qb_rating_against` | numeric | NFL passer rating allowed on targets into the player's coverage when in zone coverage. |
| `man_yards_per_coverage_snap` | numeric | Receiving yards allowed per coverage snap when in man coverage. |
| `zone_catch_rate` | numeric | Completion percentage allowed on targets into the player's coverage when in zone coverage. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_coverage_scheme()
```

_Last validated n/a._

## `pff_facet_passing_detail_stats`

Facet report /passing/detail (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/passing/detail`

**Valid URL:** [https://premium.pff.com/api/v1/facet/passing/detail](https://premium.pff.com/api/v1/facet/passing/detail)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `left_behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the left side of the field. |
| `left_short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws to the left side of the field. |
| `center_short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws to the center of the field. |
| `no_blitz_completion_percent` | numeric | Percentage of pass attempts completed when not blitzed. |
| `right_short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the right side of the field. |
| `left_behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage to the left side of the field. |
| `comp_pct_diff` | numeric | Difference in completion percentage between play-action and non-play-action attempts (PA minus non-PA), from the PFF passing-concept split. |
| `pa_grades_pass` | numeric | PFF passing grade (0-100) on play-action dropbacks. |
| `left_medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the right side of the field. |
| `right_medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage, expressed as a percentage. |
| `grades_offense` | numeric | PFF overall offense grade for the player (0-100). |
| `no_screen_grades_offense` | numeric | PFF overall offense grade for the player (0-100) excluding screen passes. |
| `pa_qb_rating` | numeric | Traditional NFL passer rating on play-action dropbacks. |
| `right_short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `no_screen_qb_rating` | numeric | Traditional NFL passer rating excluding screen passes. |
| `pa_completions` | numeric | Number of completed passes on play-action dropbacks. |
| `right_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the right side of the field. |
| `deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws, per PFF charting. |
| `center_medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws to the center of the field. |
| `medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws. |
| `left_deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws to the left side of the field. |
| `no_pressure_scrambles` | numeric | Number of scrambles from a clean pocket (no pressure). |
| `twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts, per PFF charting. |
| `behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage. |
| `medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws, as charted by PFF. |
| `pa_thrown_aways` | numeric | Number of intentional throwaways on play-action dropbacks. |
| `pa_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on play-action dropbacks, expressed as a percentage. |
| `ypa_diff` | numeric | Difference in yards per attempt between play-action and non-play-action attempts (PA minus non-PA), from the PFF passing-concept split. |
| `blitz_touchdowns` | numeric | Number of passing touchdowns thrown when blitzed. |
| `center_behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the center of the field. |
| `pressure_yards` | numeric | Passing yards gained when under pressure. |
| `no_pressure_spikes` | numeric | Number of clock-stopping spikes from a clean pocket (no pressure). |
| `blitz_ypa` | numeric | Yards gained per pass attempt when blitzed. |
| `center_behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage to the center of the field. |
| `no_screen_drops` | numeric | Number of catchable passes dropped by receivers excluding screen passes. |
| `behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage. |
| `no_blitz_grades_run` | numeric | PFF rushing grade for the player (0-100) when not blitzed. |
| `screen_completion_percent` | numeric | Percentage of pass attempts completed on screen passes. |
| `npa_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on non-play-action dropbacks. |
| `left_behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage to the left side of the field. |
| `left_deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws to the left side of the field. |
| `deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws. |
| `btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts, per PFF charting. |
| `center_short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the center of the field, per PFF charting. |
| `center_medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws to the center of the field. |
| `blitz_qb_rating` | numeric | Traditional NFL passer rating when blitzed. |
| `center_short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the center of the field. |
| `no_pressure_thrown_aways` | numeric | Number of intentional throwaways from a clean pocket (no pressure). |
| `no_screen_thrown_aways` | numeric | Number of intentional throwaways excluding screen passes. |
| `no_blitz_bats` | numeric | Number of pass attempts batted down at the line of scrimmage when not blitzed. |
| `center_deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws to the center of the field. |
| `left_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws to the center of the field. |
| `pa_grades_run` | numeric | PFF rushing grade for the player (0-100) on play-action dropbacks. |
| `center_behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage to the center of the field. |
| `right_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the right side of the field. |
| `no_blitz_drops` | numeric | Number of catchable passes dropped by receivers when not blitzed. |
| `center_deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `right_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the right side of the field. |
| `right_medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws. |
| `no_pressure_completion_percent` | numeric | Percentage of pass attempts completed from a clean pocket (no pressure). |
| `blitz_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) when blitzed, as charted by PFF. |
| `screen_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on screen passes. |
| `pa_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on play-action dropbacks. |
| `center_medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the center of the field. |
| `deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws. |
| `right_medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws to the right side of the field. |
| `spikes` | numeric | Spikes |
| `left_deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the left side of the field. |
| `screen_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on screen passes. |
| `right_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the right side of the field, expressed as a percentage. |
| `right_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the right side of the field, per PFF charting. |
| `center_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the center of the field, expressed as a percentage. |
| `deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws. |
| `center_behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage to the center of the field. |
| `dropbacks` | numeric | Number of dropbacks. |
| `right_behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the right side of the field, per PFF charting. |
| `center_deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws to the center of the field. |
| `npa_thrown_aways` | numeric | Number of intentional throwaways on non-play-action dropbacks. |
| `center_short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws to the center of the field. |
| `pressure_grades_run` | numeric | PFF rushing grade for the player (0-100) when under pressure. |
| `left_behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the left side of the field. |
| `center_short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the center of the field. |
| `deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws. |
| `no_blitz_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when not blitzed. |
| `no_pressure_bats` | numeric | Number of pass attempts batted down at the line of scrimmage from a clean pocket (no pressure). |
| `center_behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the center of the field. |
| `pressure_completions` | numeric | Number of completed passes when under pressure. |
| `blitz_big_time_throws` | numeric | Number of big-time throws when blitzed, per PFF's highest-value, highest-difficulty throw designation. |
| `center_deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the center of the field. |
| `right_short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws to the right side of the field. |
| `no_screen_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks excluding screen passes, as charted by PFF. |
| `right_behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage to the right side of the field. |
| `screen_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on screen passes. |
| `pa_touchdowns` | numeric | Number of passing touchdowns thrown on play-action dropbacks. |
| `short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws. |
| `center_medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws to the center of the field. |
| `left_behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage to the left side of the field. |
| `npa_ypa` | numeric | Yards gained per pass attempt on non-play-action dropbacks. |
| `no_blitz_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when not blitzed. |
| `blitz_spikes` | numeric | Number of clock-stopping spikes when blitzed. |
| `left_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the left side of the field. |
| `no_pressure_completions` | numeric | Number of completed passes from a clean pocket (no pressure). |
| `left_short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws to the left side of the field. |
| `thrown_aways` | numeric | Number of intentional throwaways. |
| `right_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the right side of the field. |
| `draft_season` | numeric | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `screen_avg_time_to_throw` | numeric | Average time from snap to release in seconds on screen passes. |
| `right_behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage to the right side of the field. |
| `screen_thrown_aways` | numeric | Number of intentional throwaways on screen passes. |
| `behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage, as charted by PFF. |
| `npa_sacks` | numeric | Number of sacks taken on non-play-action dropbacks. |
| `no_pressure_passing_snaps` | numeric | Number of passing snaps played from a clean pocket (no pressure). |
| `npa_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on non-play-action dropbacks, as charted by PFF. |
| `no_blitz_first_downs` | numeric | Number of passing first downs gained when not blitzed. |
| `right_short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws to the right side of the field. |
| `deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws, as charted by PFF. |
| `right_short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the right side of the field, per PFF charting. |
| `center_behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the center of the field. |
| `blitz_avg_time_to_throw` | numeric | Average time from snap to release in seconds when blitzed. |
| `left_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the left side of the field. |
| `deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws. |
| `no_pressure_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) from a clean pocket (no pressure). |
| `center_short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `center_behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage to the center of the field. |
| `pressure_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) when under pressure, as charted by PFF. |
| `no_screen_completions` | numeric | Number of completed passes excluding screen passes. |
| `right_short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `no_screen_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added excluding screen passes. |
| `center_short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws to the center of the field. |
| `medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws. |
| `right_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the right side of the field. |
| `blitz_sacks` | numeric | Number of sacks taken when blitzed. |
| `center_medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws to the center of the field. |
| `screen_spikes` | numeric | Number of clock-stopping spikes on screen passes. |
| `pa_first_downs` | numeric | Number of passing first downs gained on play-action dropbacks. |
| `medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws. |
| `no_pressure_interceptions` | numeric | Number of passes intercepted from a clean pocket (no pressure). |
| `right_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage. |
| `center_behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage to the center of the field. |
| `left_behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the left side of the field, per PFF charting. |
| `right_behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the right side of the field, per PFF charting. |
| `pa_big_time_throws` | numeric | Number of big-time throws on play-action dropbacks, per PFF's highest-value, highest-difficulty throw designation. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws. |
| `pa_spikes` | numeric | Number of clock-stopping spikes on play-action dropbacks. |
| `center_medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the center of the field. |
| `left_behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `pa_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on play-action dropbacks. |
| `deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws. |
| `right_behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the right side of the field. |
| `pressure_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when under pressure. |
| `center_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the center of the field. |
| `deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws. |
| `left_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the left side of the field, per PFF charting. |
| `medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws. |
| `blitz_completions` | numeric | Number of completed passes when blitzed. |
| `center_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the center of the field. |
| `blitz_attempts` | numeric | Number of pass attempts when blitzed. |
| `short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws, expressed as a percentage. |
| `right_behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage to the right side of the field. |
| `screen_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on screen passes, expressed as a percentage. |
| `center_medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws to the center of the field. |
| `pressure_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when under pressure, per PFF charting. |
| `pressure_sacks` | numeric | Number of sacks taken when under pressure. |
| `left_short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the left side of the field. |
| `no_blitz_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack when not blitzed. |
| `no_pressure_ypa` | numeric | Yards gained per pass attempt from a clean pocket (no pressure). |
| `no_screen_epa` | numeric | Total expected points added (EPA) on the player's dropbacks excluding screen passes. |
| `center_deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the center of the field. |
| `center_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the center of the field. |
| `left_behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `pressure_passing_snaps` | numeric | Number of passing snaps played when under pressure. |
| `medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `grades_pass` | numeric | PFF passing grade (0-100). |
| `npa_avg_time_to_throw` | numeric | Average time from snap to release in seconds on non-play-action dropbacks. |
| `deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws. |
| `right_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the right side of the field. |
| `left_deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `center_medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the center of the field. |
| `right_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the right side of the field. |
| `hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw, as charted by PFF. |
| `right_deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws to the right side of the field. |
| `left_deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws to the left side of the field. |
| `center_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the center of the field, per PFF charting. |
| `right_behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage to the right side of the field. |
| `left_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the left side of the field. |
| `screen_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on screen passes. |
| `right_deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the right side of the field. |
| `first_downs` | numeric | First downs earned by the team. |
| `screen_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) on screen passes. |
| `left_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the left side of the field. |
| `pressure_bats` | numeric | Number of pass attempts batted down at the line of scrimmage when under pressure. |
| `left_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the left side of the field. |
| `right_deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws to the right side of the field. |
| `left_deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage to the center of the field. |
| `blitz_thrown_aways` | numeric | Number of intentional throwaways when blitzed. |
| `right_deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws, as charted by PFF. |
| `right_short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws to the right side of the field. |
| `no_pressure_drops` | numeric | Number of catchable passes dropped by receivers from a clean pocket (no pressure). |
| `medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws, as charted by PFF. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `left_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the left side of the field. |
| `left_short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws to the left side of the field. |
| `pressure_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when under pressure. |
| `center_deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the center of the field, per PFF charting. |
| `right_medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws to the right side of the field. |
| `left_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the left side of the field. |
| `no_screen_bats` | numeric | Number of pass attempts batted down at the line of scrimmage excluding screen passes. |
| `left_behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage to the left side of the field. |
| `pressure_scrambles` | numeric | Number of scrambles when under pressure. |
| `blitz_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when blitzed. |
| `deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws. |
| `right_behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage to the right side of the field. |
| `sack_percent` | numeric | Percentage of dropbacks that ended in a sack. |
| `behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage. |
| `right_deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the right side of the field. |
| `right_deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws to the right side of the field. |
| `right_short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `npa_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on non-play-action dropbacks. |
| `right_short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws to the right side of the field. |
| `short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws. |
| `right_deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the right side of the field, per PFF charting. |
| `right_behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the right side of the field. |
| `screen_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on screen passes, as charted by PFF. |
| `deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws. |
| `pa_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on play-action dropbacks. |
| `center_medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws to the center of the field. |
| `screen_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on screen passes. |
| `blitz_completion_percent` | numeric | Percentage of pass attempts completed when blitzed. |
| `behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage, as charted by PFF. |
| `right_behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage to the right side of the field. |
| `bats` | numeric | Number of pass attempts batted down at the line of scrimmage. |
| `right_medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the right side of the field. |
| `no_screen_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts excluding screen passes, per PFF charting. |
| `medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws. |
| `short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws. |
| `center_behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the center of the field, per PFF charting. |
| `left_behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage to the left side of the field. |
| `short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws, as charted by PFF. |
| `short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws. |
| `right_short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the right side of the field. |
| `npa_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on non-play-action dropbacks, per PFF charting. |
| `medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws. |
| `left_behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the left side of the field. |
| `screen_interceptions` | numeric | Number of passes intercepted on screen passes. |
| `center_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the center of the field, expressed as a percentage. |
| `left_medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the left side of the field. |
| `center_short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the center of the field. |
| `left_deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws to the left side of the field. |
| `no_screen_passing_snaps` | numeric | Number of passing snaps played excluding screen passes. |
| `no_pressure_first_downs` | numeric | Number of passing first downs gained from a clean pocket (no pressure). |
| `center_behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage to the center of the field. |
| `center_short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws to the center of the field. |
| `blitz_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when blitzed. |
| `right_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the right side of the field, per PFF charting. |
| `right_medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws to the right side of the field. |
| `no_screen_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) excluding screen passes. |
| `medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws, per PFF charting. |
| `no_blitz_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when not blitzed. |
| `center_behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the center of the field, per PFF charting. |
| `blitz_interceptions` | numeric | Number of passes intercepted when blitzed. |
| `no_blitz_dropbacks` | numeric | Number of dropbacks when not blitzed. |
| `center_short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws to the center of the field. |
| `left_medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws to the left side of the field. |
| `right_short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws to the right side of the field. |
| `left_medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws to the left side of the field. |
| `no_blitz_grades_pass` | numeric | PFF passing grade (0-100) when not blitzed. |
| `right_medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the right side of the field. |
| `screen_scrambles` | numeric | Number of scrambles on screen passes. |
| `left_short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws, plays PFF charts as deserving of a turnover. |
| `no_blitz_scrambles` | numeric | Number of scrambles when not blitzed. |
| `pressure_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when under pressure. |
| `left_deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the left side of the field. |
| `no_blitz_yards` | numeric | Passing yards gained when not blitzed. |
| `left_medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `screen_grades_pass` | numeric | PFF passing grade (0-100) on screen passes. |
| `center_deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the center of the field. |
| `center_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the center of the field. |
| `sacks` | numeric | The Number of times sacked. |
| `pressure_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack, reported within the pressure split. |
| `center_behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage to the center of the field. |
| `right_short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws to the right side of the field. |
| `center_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the center of the field. |
| `no_blitz_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when not blitzed. |
| `right_short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the right side of the field. |
| `left_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the left side of the field. |
| `center_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the center of the field. |
| `center_behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the center of the field. |
| `npa_qb_rating` | numeric | Traditional NFL passer rating on non-play-action dropbacks. |
| `no_pressure_grades_offense` | numeric | PFF overall offense grade for the player (0-100) from a clean pocket (no pressure). |
| `medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws. |
| `no_screen_grades_pass` | numeric | PFF passing grade (0-100) excluding screen passes. |
| `medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws. |
| `left_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the left side of the field. |
| `deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws, per PFF charting. |
| `pa_avg_time_to_throw` | numeric | Average time from snap to release in seconds on play-action dropbacks. |
| `left_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the left side of the field. |
| `no_pressure_avg_time_to_throw` | numeric | Average time from snap to release in seconds from a clean pocket (no pressure). |
| `screen_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on screen passes, per PFF charting. |
| `pressure_dropbacks` | numeric | Number of dropbacks when under pressure. |
| `short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws, per PFF charting. |
| `short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws. |
| `left_deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws to the left side of the field. |
| `short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws. |
| `center_medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `no_blitz_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when not blitzed. |
| `behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage. |
| `right_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the right side of the field. |
| `right_medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `no_pressure_grades_run` | numeric | PFF rushing grade for the player (0-100) from a clean pocket (no pressure). |
| `short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws. |
| `player_game_count` | numeric | Number of games the player appeared in during the period covered. |
| `short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws, per PFF charting. |
| `right_short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws to the right side of the field. |
| `center_short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws to the center of the field. |
| `right_short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws to the right side of the field. |
| `behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage. |
| `right_medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `blitz_turnover_worthy_plays` | numeric | Number of turnover-worthy plays when blitzed, plays PFF charts as deserving of a turnover. |
| `deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws. |
| `npa_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on non-play-action dropbacks. |
| `no_blitz_touchdowns` | numeric | Number of passing touchdowns thrown when not blitzed. |
| `no_blitz_avg_depth_of_target` | numeric | Average depth of target in air yards when not blitzed. |
| `medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws. |
| `short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws. |
| `right_deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the right side of the field, expressed as a percentage. |
| `no_pressure_dropbacks_percent` | numeric | Share of the player's total dropbacks that came from a clean pocket (no pressure), expressed as a percentage. |
| `blitz_grades_pass` | numeric | PFF passing grade (0-100) when blitzed. |
| `eligible_season` | numeric | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage. |
| `blitz_avg_depth_of_target` | numeric | Average depth of target in air yards when blitzed. |
| `no_blitz_spikes` | numeric | Number of clock-stopping spikes when not blitzed. |
| `center_behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage to the center of the field. |
| `npa_drops` | numeric | Number of catchable passes dropped by receivers on non-play-action dropbacks. |
| `center_behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the center of the field. |
| `right_deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the right side of the field. |
| `center_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the left side of the field. |
| `no_pressure_dropbacks` | numeric | Number of dropbacks from a clean pocket (no pressure). |
| `center_deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws to the center of the field. |
| `right_behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws. |
| `right_medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws to the right side of the field. |
| `completions` | numeric | The number of completed passes. |
| `medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws. |
| `left_deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws to the left side of the field. |
| `screen_yards` | numeric | Passing yards gained on screen passes. |
| `right_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the right side of the field. |
| `center_medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `left_behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage to the left side of the field. |
| `blitz_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when blitzed, per PFF charting. |
| `no_screen_drop_rate` | numeric | Percentage of catchable passes dropped by receivers excluding screen passes. |
| `center_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the center of the field. |
| `right_short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the right side of the field. |
| `blitz_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when blitzed. |
| `no_pressure_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts from a clean pocket (no pressure), per PFF charting. |
| `left_short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws to the left side of the field. |
| `deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws. |
| `right_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the right side of the field. |
| `center_behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the center of the field, expressed as a percentage. |
| `no_screen_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) excluding screen passes. |
| `right_deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws to the right side of the field. |
| `left_short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws to the left side of the field. |
| `deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws. |
| `left_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the left side of the field. |
| `npa_passing_snaps` | numeric | Number of passing snaps played on non-play-action dropbacks. |
| `screen_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on screen passes. |
| `screen_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on screen passes. |
| `no_pressure_drop_rate` | numeric | Percentage of catchable passes dropped by receivers from a clean pocket (no pressure). |
| `no_blitz_turnover_worthy_plays` | numeric | Number of turnover-worthy plays when not blitzed, plays PFF charts as deserving of a turnover. |
| `yards` | numeric | The number of receiving yards |
| `right_deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage. |
| `screen_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on screen passes. |
| `right_deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws to the right side of the field. |
| `npa_spikes` | numeric | Number of clock-stopping spikes on non-play-action dropbacks. |
| `pressure_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when under pressure. |
| `screen_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on screen passes, as charted by PFF. |
| `left_behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `left_behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the left side of the field. |
| `deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws. |
| `left_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the left side of the field. |
| `medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws. |
| `no_screen_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) excluding screen passes, as charted by PFF. |
| `screen_drops` | numeric | Number of catchable passes dropped by receivers on screen passes. |
| `left_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the left side of the field. |
| `accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF. |
| `right_behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage to the right side of the field. |
| `screen_ypa` | numeric | Yards gained per pass attempt on screen passes. |
| `medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws. |
| `blitz_first_downs` | numeric | Number of passing first downs gained when blitzed. |
| `npa_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on non-play-action dropbacks, per PFF charting. |
| `behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage. |
| `left_short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws to the left side of the field. |
| `center_short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws to the center of the field. |
| `center_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the center of the field. |
| `right_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the right side of the field. |
| `right_medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the right side of the field. |
| `right_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the right side of the field. |
| `no_blitz_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when not blitzed, expressed as a percentage. |
| `right_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the right side of the field. |
| `screen_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on screen passes. |
| `right_behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage to the right side of the field. |
| `behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage. |
| `center_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the center of the field. |
| `left_deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws to the left side of the field. |
| `npa_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on non-play-action dropbacks, as charted by PFF. |
| `center_behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage to the center of the field. |
| `scrambles` | numeric | Number of scrambles. |
| `right_behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `left_short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws to the left side of the field. |
| `right_medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws to the right side of the field. |
| `left_behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage to the left side of the field. |
| `pa_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on play-action dropbacks. |
| `center_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the center of the field, per PFF charting. |
| `pressure_turnover_worthy_plays` | numeric | Number of turnover-worthy plays when under pressure, plays PFF charts as deserving of a turnover. |
| `medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws. |
| `no_pressure_epa` | numeric | Total expected points added (EPA) on the player's dropbacks from a clean pocket (no pressure). |
| `left_short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage to the left side of the field. |
| `short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws. |
| `right_medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the right side of the field. |
| `no_blitz_avg_time_to_throw` | numeric | Average time from snap to release in seconds when not blitzed. |
| `left_behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage to the left side of the field. |
| `pa_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on play-action dropbacks, as charted by PFF. |
| `right_medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws to the right side of the field. |
| `interceptions` | numeric | The number of interceptions thrown. |
| `screen_avg_depth_of_target` | numeric | Average depth of target in air yards on screen passes. |
| `pa_sacks` | numeric | Number of sacks taken on play-action dropbacks. |
| `short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws, plays PFF charts as deserving of a turnover. |
| `screen_passing_snaps` | numeric | Number of passing snaps played on screen passes. |
| `right_deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws to the right side of the field. |
| `drop_rate` | numeric | Percentage of catchable passes dropped by receivers. |
| `right_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the right side of the field. |
| `no_screen_grades_run` | numeric | PFF rushing grade for the player (0-100) excluding screen passes. |
| `right_short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws to the right side of the field. |
| `medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws. |
| `center_deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the center of the field, expressed as a percentage. |
| `left_short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws to the left side of the field. |
| `behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage. |
| `short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws. |
| `no_screen_first_downs` | numeric | Number of passing first downs gained excluding screen passes. |
| `no_blitz_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when not blitzed. |
| `left_medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws to the left side of the field. |
| `left_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the left side of the field. |
| `pressure_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when under pressure, per PFF charting. |
| `pa_ypa` | numeric | Yards gained per pass attempt on play-action dropbacks. |
| `behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage. |
| `npa_scrambles` | numeric | Number of scrambles on non-play-action dropbacks. |
| `no_pressure_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) from a clean pocket (no pressure), as charted by PFF. |
| `pressure_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when under pressure, expressed as a percentage. |
| `grades_run` | numeric | PFF rushing grade for the player (0-100). |
| `behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage. |
| `left_deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws to the left side of the field. |
| `left_short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `npa_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on non-play-action dropbacks. |
| `center_behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage to the center of the field. |
| `pa_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on play-action dropbacks, per PFF charting. |
| `no_pressure_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks from a clean pocket (no pressure), as charted by PFF. |
| `left_short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws to the left side of the field. |
| `pressure_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when under pressure. |
| `left_short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the left side of the field. |
| `center_short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws to the center of the field. |
| `center_short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws to the center of the field. |
| `short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws. |
| `pressure_completion_percent` | numeric | Percentage of pass attempts completed when under pressure. |
| `left_deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws to the left side of the field. |
| `deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws. |
| `center_behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage to the center of the field, plays PFF charts as deserving of a turnover. |
| `medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws. |
| `left_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the left side of the field. |
| `left_deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws to the left side of the field. |
| `deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws. |
| `pressure_avg_depth_of_target` | numeric | Average depth of target in air yards when under pressure. |
| `short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws. |
| `left_medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the left side of the field. |
| `blitz_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when blitzed. |
| `center_deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws to the center of the field. |
| `center_short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `left_medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws to the center of the field. |
| `right_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the right side of the field. |
| `right_short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws. |
| `no_screen_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts excluding screen passes, per PFF charting. |
| `left_behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage to the left side of the field. |
| `right_deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws to the right side of the field. |
| `pressure_drops` | numeric | Number of catchable passes dropped by receivers when under pressure. |
| `right_short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the right side of the field, per PFF charting. |
| `center_medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws to the center of the field. |
| `right_deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws to the right side of the field. |
| `no_blitz_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks when not blitzed, as charted by PFF. |
| `center_deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `pressure_attempts` | numeric | Number of pass attempts when under pressure. |
| `npa_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on non-play-action dropbacks. |
| `behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage. |
| `right_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the right side of the field, per PFF charting. |
| `right_deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `right_deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws to the right side of the field. |
| `deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `left_short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `qb_rating` | numeric | Traditional NFL passer rating. |
| `center_short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws to the center of the field. |
| `right_medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the right side of the field. |
| `left_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the left side of the field. |
| `pressure_big_time_throws` | numeric | Number of big-time throws when under pressure, per PFF's highest-value, highest-difficulty throw designation. |
| `no_blitz_thrown_aways` | numeric | Number of intentional throwaways when not blitzed. |
| `short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws. |
| `medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws. |
| `left_medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws to the left side of the field. |
| `right_behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage to the right side of the field. |
| `completion_percent` | numeric | Percentage of pass attempts completed. |
| `blitz_drops` | numeric | Number of catchable passes dropped by receivers when blitzed. |
| `behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage, per PFF charting. |
| `screen_completions` | numeric | Number of completed passes on screen passes. |
| `screen_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on screen passes, per PFF charting. |
| `npa_grades_run` | numeric | PFF rushing grade for the player (0-100) on non-play-action dropbacks. |
| `no_pressure_touchdowns` | numeric | Number of passing touchdowns thrown from a clean pocket (no pressure). |
| `no_screen_interceptions` | numeric | Number of passes intercepted excluding screen passes. |
| `medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws. |
| `center_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the center of the field. |
| `behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage. |
| `right_medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the right side of the field. |
| `no_pressure_sacks` | numeric | Number of sacks taken from a clean pocket (no pressure). |
| `center_short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws to the center of the field. |
| `npa_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `left_deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `left_medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws to the left side of the field. |
| `center_medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the center of the field. |
| `blitz_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when blitzed. |
| `no_screen_sacks` | numeric | Number of sacks taken excluding screen passes. |
| `center_short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the center of the field. |
| `left_short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the left side of the field, per PFF charting. |
| `pressure_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when under pressure. |
| `no_pressure_attempts` | numeric | Number of pass attempts from a clean pocket (no pressure). |
| `right_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the right side of the field, expressed as a percentage. |
| `no_blitz_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when not blitzed. |
| `attempts` | numeric | The number of pass attempts as defined by the NFL. |
| `blitz_scrambles` | numeric | Number of scrambles when blitzed. |
| `right_deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage. |
| `center_deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws to the center of the field. |
| `short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws. |
| `no_screen_big_time_throws` | numeric | Number of big-time throws excluding screen passes, per PFF's highest-value, highest-difficulty throw designation. |
| `left_deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage to the center of the field. |
| `blitz_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when blitzed, expressed as a percentage. |
| `behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage. |
| `left_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the left side of the field. |
| `medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws, per PFF charting. |
| `left_short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `npa_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on non-play-action dropbacks. |
| `medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws, plays PFF charts as deserving of a turnover. |
| `behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage, per PFF charting. |
| `left_behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage to the left side of the field. |
| `left_short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the left side of the field. |
| `center_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the center of the field. |
| `npa_attempts` | numeric | Number of pass attempts on non-play-action dropbacks. |
| `right_deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the right side of the field. |
| `screen_qb_rating` | numeric | Traditional NFL passer rating on screen passes. |
| `medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws. |
| `center_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the center of the field. |
| `center_medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws to the center of the field. |
| `center_behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage to the center of the field. |
| `npa_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on non-play-action dropbacks. |
| `no_pressure_qb_rating` | numeric | Traditional NFL passer rating from a clean pocket (no pressure). |
| `center_medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws to the center of the field. |
| `pa_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on play-action dropbacks. |
| `pa_attempts` | numeric | Number of pass attempts on play-action dropbacks. |
| `behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage. |
| `no_blitz_passing_snaps` | numeric | Number of passing snaps played when not blitzed. |
| `npa_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on non-play-action dropbacks, as charted by PFF. |
| `left_short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws. |
| `no_blitz_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) when not blitzed, as charted by PFF. |
| `blitz_yards` | numeric | Passing yards gained when blitzed. |
| `no_screen_avg_time_to_throw` | numeric | Average time from snap to release in seconds excluding screen passes. |
| `center_deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the center of the field. |
| `behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage. |
| `pa_yards` | numeric | Passing yards gained on play-action dropbacks. |
| `right_short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws to the right side of the field. |
| `left_deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws to the left side of the field. |
| `right_medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws to the right side of the field. |
| `npa_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on non-play-action dropbacks. |
| `right_behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage to the right side of the field, plays PFF charts as deserving of a turnover. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `no_screen_scrambles` | numeric | Number of scrambles excluding screen passes. |
| `pa_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on play-action dropbacks, plays PFF charts as deserving of a turnover. |
| `right_medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws to the right side of the field. |
| `behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage, as charted by PFF. |
| `no_blitz_attempts` | numeric | Number of pass attempts when not blitzed. |
| `pressure_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF when under pressure. |
| `left_medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the left side of the field. |
| `medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws. |
| `deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws, as charted by PFF. |
| `no_blitz_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw when not blitzed, as charted by PFF. |
| `right_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the right side of the field. |
| `pressure_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw when under pressure, as charted by PFF. |
| `short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws. |
| `deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws. |
| `blitz_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack when blitzed. |
| `pa_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on play-action dropbacks, as charted by PFF. |
| `deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws. |
| `pa_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on play-action dropbacks. |
| `screen_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on screen passes. |
| `left_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the left side of the field. |
| `no_screen_dropbacks` | numeric | Number of dropbacks excluding screen passes. |
| `right_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the right side of the field. |
| `no_screen_dropbacks_percent` | numeric | Share of the player's total dropbacks that came excluding screen passes, expressed as a percentage. |
| `no_blitz_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when not blitzed, per PFF charting. |
| `medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws, expressed as a percentage. |
| `right_behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage to the right side of the field. |
| `left_medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `left_behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the left side of the field. |
| `right_short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `no_blitz_ypa` | numeric | Yards gained per pass attempt when not blitzed. |
| `right_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the right side of the field, expressed as a percentage. |
| `center_short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `npa_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on non-play-action dropbacks. |
| `short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws, as charted by PFF. |
| `blitz_grades_run` | numeric | PFF rushing grade for the player (0-100) when blitzed. |
| `right_medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `left_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the left side of the field. |
| `center_behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the center of the field. |
| `pa_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on play-action dropbacks. |
| `pressure_interceptions` | numeric | Number of passes intercepted when under pressure. |
| `right_behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage to the right side of the field. |
| `deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws. |
| `left_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the left side of the field, per PFF charting. |
| `blitz_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when blitzed. |
| `passing_snaps` | numeric | Number of passing snaps played. |
| `pa_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) on play-action dropbacks. |
| `pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack. |
| `ypa` | numeric | Yards gained per pass attempt. |
| `right_behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the center of the field. |
| `drops` | numeric | Throws dropped |
| `center_deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws to the center of the field. |
| `short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws. |
| `left_short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the left side of the field, per PFF charting. |
| `no_screen_turnover_worthy_plays` | numeric | Number of turnover-worthy plays excluding screen passes, plays PFF charts as deserving of a turnover. |
| `no_pressure_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) from a clean pocket (no pressure). |
| `right_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the right side of the field. |
| `position` | character | Primary position as reported by NFL.com |
| `short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws, as charted by PFF. |
| `center_deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws to the center of the field. |
| `center_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the center of the field. |
| `blitz_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when blitzed. |
| `right_behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage to the right side of the field. |
| `right_medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws to the right side of the field. |
| `grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100). |
| `left_medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws to the left side of the field. |
| `npa_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on non-play-action dropbacks. |
| `no_screen_avg_depth_of_target` | numeric | Average depth of target in air yards excluding screen passes. |
| `medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws. |
| `left_medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the left side of the field. |
| `pa_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on play-action dropbacks. |
| `no_screen_completion_percent` | numeric | Percentage of pass attempts completed excluding screen passes. |
| `left_short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws to the left side of the field. |
| `pa_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on play-action dropbacks, as charted by PFF. |
| `medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws. |
| `center_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage to the left side of the field. |
| `left_deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the left side of the field. |
| `avg_time_to_throw` | numeric | Average time elapsed from the time of snap to throw on every pass attempt for a passer (sacks excluded). |
| `pa_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on play-action dropbacks. |
| `deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws. |
| `npa_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on non-play-action dropbacks, expressed as a percentage. |
| `left_medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws to the left side of the field. |
| `pressure_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks when under pressure, as charted by PFF. |
| `left_medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws to the left side of the field. |
| `npa_grades_pass` | numeric | PFF passing grade (0-100) on non-play-action dropbacks. |
| `pressure_grades_pass` | numeric | PFF passing grade (0-100) when under pressure. |
| `right_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the right side of the field. |
| `big_time_throws` | numeric | Number of big-time throws, per PFF's highest-value, highest-difficulty throw designation. |
| `screen_grades_run` | numeric | PFF rushing grade for the player (0-100) on screen passes. |
| `left_short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the left side of the field. |
| `screen_first_downs` | numeric | Number of passing first downs gained on screen passes. |
| `npa_completion_percent` | numeric | Percentage of pass attempts completed on non-play-action dropbacks. |
| `left_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the left side of the field. |
| `medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws. |
| `no_blitz_qb_rating` | numeric | Traditional NFL passer rating when not blitzed. |
| `blitz_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw when blitzed, as charted by PFF. |
| `behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage, plays PFF charts as deserving of a turnover. |
| `right_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the right side of the field. |
| `right_deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws to the right side of the field. |
| `deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws. |
| `center_medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws to the center of the field. |
| `left_medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws to the left side of the field. |
| `left_short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage to the left side of the field. |
| `no_screen_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) excluding screen passes. |
| `pressure_ypa` | numeric | Yards gained per pass attempt when under pressure. |
| `left_deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `no_screen_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw excluding screen passes, as charted by PFF. |
| `center_short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws to the center of the field. |
| `center_deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws to the center of the field. |
| `right_deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws to the right side of the field. |
| `center_behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage to the center of the field. |
| `screen_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on screen passes, plays PFF charts as deserving of a turnover. |
| `deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws. |
| `left_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the left side of the field, expressed as a percentage. |
| `pressure_avg_time_to_throw` | numeric | Average time from snap to release in seconds when under pressure. |
| `short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws. |
| `behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage. |
| `right_short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage to the right side of the field. |
| `center_short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws to the center of the field. |
| `center_behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `no_blitz_completions` | numeric | Number of completed passes when not blitzed. |
| `center_deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `left_medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the left side of the field. |
| `behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage. |
| `left_deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the left side of the field, expressed as a percentage. |
| `no_pressure_sack_percent` | numeric | Percentage of dropbacks that ended in a sack from a clean pocket (no pressure). |
| `center_deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `npa_avg_depth_of_target` | numeric | Average depth of target in air yards on non-play-action dropbacks. |
| `npa_dropbacks` | numeric | Number of dropbacks on non-play-action dropbacks. |
| `player` | character | Player name |
| `left_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the left side of the field. |
| `no_blitz_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF when not blitzed. |
| `behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage. |
| `center_medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws. |
| `left_deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the left side of the field, per PFF charting. |
| `center_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the center of the field. |
| `left_behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `pa_drops` | numeric | Number of catchable passes dropped by receivers on play-action dropbacks. |
| `no_pressure_pressure_to_sack_rate` | character | Pressure-to-sack rate as reported within the no-pressure split of the PFF passing-pressure facet. |
| `right_medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws to the right side of the field. |
| `pa_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on play-action dropbacks, per PFF charting. |
| `no_pressure_turnover_worthy_plays` | numeric | Number of turnover-worthy plays from a clean pocket (no pressure), plays PFF charts as deserving of a turnover. |
| `pressure_first_downs` | numeric | Number of passing first downs gained when under pressure. |
| `positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added. |
| `right_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the right side of the field. |
| `blitz_passing_snaps` | numeric | Number of passing snaps played when blitzed. |
| `center_medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `left_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the left side of the field. |
| `screen_big_time_throws` | numeric | Number of big-time throws on screen passes, per PFF's highest-value, highest-difficulty throw designation. |
| `left_short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage to the left side of the field. |
| `left_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the left side of the field, per PFF charting. |
| `screen_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on screen passes, as charted by PFF. |
| `right_deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage. |
| `no_pressure_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts from a clean pocket (no pressure), per PFF charting. |
| `npa_touchdowns` | numeric | Number of passing touchdowns thrown on non-play-action dropbacks. |
| `no_blitz_sacks` | numeric | Number of sacks taken when not blitzed. |
| `short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws. |
| `left_deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws to the left side of the field. |
| `right_behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage to the right side of the field. |
| `screen_attempts` | numeric | Number of pass attempts on screen passes. |
| `screen_dropbacks` | numeric | Number of dropbacks on screen passes. |
| `right_deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws to the right side of the field. |
| `left_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the left side of the field, expressed as a percentage. |
| `behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage, per PFF's highest-value, highest-difficulty throw designation. |
| `center_deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the center of the field. |
| `left_medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `right_behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the right side of the field. |
| `left_medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `no_screen_yards` | numeric | Passing yards gained excluding screen passes. |
| `right_deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `left_short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws to the left side of the field. |
| `left_medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `left_behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the left side of the field, per PFF charting. |
| `short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws. |
| `right_short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the right side of the field. |
| `right_short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws to the right side of the field. |
| `pa_scrambles` | numeric | Number of scrambles on play-action dropbacks. |
| `right_medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws to the right side of the field. |
| `no_pressure_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF from a clean pocket (no pressure). |
| `center_short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the center of the field. |
| `right_short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws to the right side of the field. |
| `center_short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `center_medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the center of the field. |
| `medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws. |
| `center_medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `center_short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws to the center of the field. |
| `pa_completion_percent` | numeric | Percentage of pass attempts completed on play-action dropbacks. |
| `deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws, expressed as a percentage. |
| `center_behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `left_short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws to the left side of the field. |
| `center_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws to the center of the field. |
| `pa_avg_depth_of_target` | numeric | Average depth of target in air yards on play-action dropbacks. |
| `pa_interceptions` | numeric | Number of passes intercepted on play-action dropbacks. |
| `no_screen_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF excluding screen passes. |
| `behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage. |
| `no_screen_spikes` | numeric | Number of clock-stopping spikes excluding screen passes. |
| `center_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the center of the field. |
| `pa_dropbacks` | numeric | Number of dropbacks on play-action dropbacks. |
| `left_deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `center_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the center of the field. |
| `left_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the left side of the field, expressed as a percentage. |
| `left_deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws to the left side of the field. |
| `no_blitz_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when not blitzed, per PFF charting. |
| `no_pressure_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw from a clean pocket (no pressure), as charted by PFF. |
| `right_short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws to the right side of the field. |
| `center_behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `right_short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws to the right side of the field. |
| `center_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the center of the field. |
| `no_pressure_grades_pass` | numeric | PFF passing grade (0-100) from a clean pocket (no pressure). |
| `npa_big_time_throws` | numeric | Number of big-time throws on non-play-action dropbacks, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws. |
| `no_screen_sack_percent` | numeric | Percentage of dropbacks that ended in a sack excluding screen passes. |
| `avg_depth_of_target` | numeric | Average depth of target in air yards. |
| `no_pressure_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added from a clean pocket (no pressure). |
| `turnover_worthy_plays` | numeric | Number of turnover-worthy plays, plays PFF charts as deserving of a turnover. |
| `epa` | numeric | Expected points added (EPA) by the posteam for the given play. |
| `pressure_spikes` | numeric | Number of clock-stopping spikes when under pressure. |
| `pressure_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when under pressure. |
| `left_deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the left side of the field. |
| `right_behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `pa_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on play-action dropbacks. |
| `pressure_qb_rating` | numeric | Traditional NFL passer rating when under pressure. |
| `center_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the center of the field. |
| `center_deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `no_screen_attempts` | numeric | Number of pass attempts excluding screen passes. |
| `pa_passing_snaps` | numeric | Number of passing snaps played on play-action dropbacks. |
| `aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways), as charted by PFF. |
| `blitz_bats` | numeric | Number of pass attempts batted down at the line of scrimmage when blitzed. |
| `pressure_touchdowns` | numeric | Number of passing touchdowns thrown when under pressure. |
| `npa_completions` | numeric | Number of completed passes on non-play-action dropbacks. |
| `short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws. |
| `pressure_thrown_aways` | numeric | Number of intentional throwaways when under pressure. |
| `right_behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage to the right side of the field. |
| `no_blitz_interceptions` | numeric | Number of passes intercepted when not blitzed. |
| `center_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the center of the field. |
| `short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws. |
| `deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws, as charted by PFF. |
| `right_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the right side of the field. |
| `short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws. |
| `screen_touchdowns` | numeric | Number of passing touchdowns thrown on screen passes. |
| `center_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the center of the field. |
| `left_medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws to the center of the field. |
| `left_deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `blitz_dropbacks` | numeric | Number of dropbacks when blitzed. |
| `center_medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws to the center of the field. |
| `center_medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws to the center of the field. |
| `blitz_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks when blitzed, as charted by PFF. |
| `npa_first_downs` | numeric | Number of passing first downs gained on non-play-action dropbacks. |
| `no_screen_touchdowns` | numeric | Number of passing touchdowns thrown excluding screen passes. |
| `right_deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `left_short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws to the left side of the field. |
| `medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `npa_yards` | numeric | Passing yards gained on non-play-action dropbacks. |
| `left_medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws to the right side of the field. |
| `no_screen_ypa` | numeric | Yards gained per pass attempt excluding screen passes. |
| `no_blitz_big_time_throws` | numeric | Number of big-time throws when not blitzed, per PFF's highest-value, highest-difficulty throw designation. |
| `center_short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws to the center of the field. |
| `npa_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on non-play-action dropbacks, plays PFF charts as deserving of a turnover. |
| `left_behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage to the left side of the field, plays PFF charts as deserving of a turnover. |
| `center_medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the center of the field. |
| `npa_interceptions` | numeric | Number of passes intercepted on non-play-action dropbacks. |
| `left_medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws to the left side of the field. |
| `right_deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws to the right side of the field. |
| `no_pressure_avg_depth_of_target` | numeric | Average depth of target in air yards from a clean pocket (no pressure). |
| `touchdowns` | numeric | Number of passing touchdowns thrown. |
| `medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws. |
| `right_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the right side of the field. |
| `deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws. |
| `no_screen_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack excluding screen passes. |
| `behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage. |
| `center_deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws to the center of the field. |
| `center_short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the center of the field, per PFF charting. |
| `behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage. |
| `left_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the left side of the field. |
| `no_pressure_yards` | numeric | Passing yards gained from a clean pocket (no pressure). |
| `screen_sacks` | numeric | Number of sacks taken on screen passes. |
| `def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks, as charted by PFF. |
| `right_medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws to the right side of the field. |
| `right_behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the right side of the field. |
| `left_behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage to the left side of the field. |
| `blitz_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when blitzed, per PFF charting. |
| `short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws. |
| `blitz_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF when blitzed. |
| `center_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the center of the field, per PFF charting. |
| `no_pressure_big_time_throws` | numeric | Number of big-time throws from a clean pocket (no pressure), per PFF's highest-value, highest-difficulty throw designation. |
| `left_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the left side of the field. |
| `no_blitz_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) when not blitzed. |
| `screen_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) on screen passes. |
| `screen_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) on screen passes. |
| `no_screen_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) excluding screen passes. |
| `pressure_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when under pressure. |
| `no_screen_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) excluding screen passes. |
| `pa_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) on play-action dropbacks. |
| `blitz_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) when blitzed. |
| `screen_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) on screen passes. |
| `no_pressure_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `npa_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `pressure_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) when under pressure. |
| `pa_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) on play-action dropbacks. |
| `no_pressure_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) from a clean pocket (no pressure). |
| `npa_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) on non-play-action dropbacks. |
| `npa_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) on non-play-action dropbacks. |
| `pa_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) on play-action dropbacks. |
| `blitz_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when blitzed. |
| `blitz_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when blitzed. |
| `no_screen_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) excluding screen passes. |
| `no_pressure_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) from a clean pocket (no pressure). |
| `no_blitz_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when not blitzed. |
| `pressure_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when under pressure. |
| `no_blitz_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when not blitzed. |
| `pa_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) on play-action dropbacks. |
| `blitz_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) when blitzed. |
| `screen_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) on screen passes. |
| `no_pressure_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `no_screen_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) excluding screen passes. |
| `no_blitz_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) when not blitzed. |
| `blitz_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when blitzed. |
| `pressure_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when under pressure. |
| `no_blitz_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when not blitzed. |
| `npa_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `pressure_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) when under pressure. |
| `no_pressure_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `pa_grades_defense_penalty` | character | PFF defensive penalty grade for the player (0-100) on play-action dropbacks. |
| `screen_grades_run_defense` | character | PFF run-defense grade for the player (0-100) on screen passes. |
| `npa_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) on non-play-action dropbacks. |
| `pa_grades_run_defense` | character | PFF run-defense grade for the player (0-100) on play-action dropbacks. |
| `pa_grades_defense` | character | PFF overall defense grade for the player (0-100) on play-action dropbacks. |
| `npa_grades_defense` | numeric | PFF overall defense grade for the player (0-100) on non-play-action dropbacks. |
| `screen_grades_defense` | character | PFF overall defense grade for the player (0-100) on screen passes. |
| `screen_grades_defense_penalty` | character | PFF defensive penalty grade for the player (0-100) on screen passes. |
| `no_screen_grades_run_defense` | numeric | PFF run-defense grade for the player (0-100) excluding screen passes. |
| `no_screen_grades_defense` | numeric | PFF overall defense grade for the player (0-100) excluding screen passes. |
| `no_screen_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) excluding screen passes. |
| `npa_grades_run_defense` | numeric | PFF run-defense grade for the player (0-100) on non-play-action dropbacks. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_passing_detail_stats()
```

_Last validated n/a._

## `pff_facet_run_blocking`

Facet report /offense/run_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/offense/run_blocking`

**Valid URL:** [https://premium.pff.com/api/v1/facet/offense/run_blocking](https://premium.pff.com/api/v1/facet/offense/run_blocking)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `gap_grades_run_block` | numeric | PFF run-blocking grade on gap-scheme runs, 0-100. |
| `gap_run_block_percent` | numeric | Share of run-play snaps spent run blocking on gap-scheme runs. |
| `gap_snap_counts_run_block` | numeric | Run-blocking snaps played on gap-scheme runs. |
| `gap_snap_counts_run_block_percent` | numeric | Share of the player's run-blocking snaps on gap-scheme runs. |
| `gap_snap_counts_run_play` | numeric | Run-play snaps on gap-scheme runs. |
| `grades_run_block` | numeric | PFF run-blocking grade, 0-100. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `penalties` | numeric | Total number of penalties. |
| `player` | character | Player name |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `position` | character | Primary position as reported by NFL.com |
| `run_block_percent` | numeric | Share of run-play snaps spent run blocking. |
| `snap_counts_run_block` | numeric | Run-blocking snaps played. |
| `snap_counts_run_play` | numeric | Run-play snaps. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `zone_grades_run_block` | numeric | PFF run-blocking grade on zone-scheme runs, 0-100. |
| `zone_run_block_percent` | numeric | Share of run-play snaps spent run blocking on zone-scheme runs. |
| `zone_snap_counts_run_block` | numeric | Run-blocking snaps played on zone-scheme runs. |
| `zone_snap_counts_run_block_percent` | numeric | Share of the player's run-blocking snaps on zone-scheme runs. |
| `zone_snap_counts_run_play` | numeric | Run-play snaps on zone-scheme runs. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_run_blocking()
```

_Last validated n/a._

## `pff_facet_pass_blocking`

Facet report /offense/pass_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/offense/pass_blocking`

**Valid URL:** [https://premium.pff.com/api/v1/facet/offense/pass_blocking](https://premium.pff.com/api/v1/facet/offense/pass_blocking)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `true_pass_set_non_spike_pass_block_percentage` | numeric | Share of non-spike pass-play snaps spent pass blocking on PFF-designated true pass sets. |
| `true_pass_set_pressures_allowed` | numeric | Total pressures allowed (sacks, hits, and hurries) on PFF-designated true pass sets. |
| `grades_pass_block` | numeric | PFF pass-blocking grade, 0-100. |
| `true_pass_set_pass_block_percent` | numeric | Share of pass-play snaps spent pass blocking on PFF-designated true pass sets. |
| `pbe` | numeric | PFF Pass Blocking Efficiency rating, pressures allowed per pass-blocking snap weighted toward sacks. |
| `non_spike_pass_block_percentage` | numeric | Share of non-spike pass-play snaps spent pass blocking. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `hits_allowed` | numeric | Quarterback hits allowed. |
| `true_pass_set_non_spike_pass_block` | numeric | Pass-blocking snaps excluding spike plays on PFF-designated true pass sets. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `hurries_allowed` | numeric | Quarterback hurries allowed. |
| `true_pass_set_hurries_allowed` | numeric | Quarterback hurries allowed on PFF-designated true pass sets. |
| `true_pass_set_snap_counts_pass_play` | numeric | Pass-play snaps on PFF-designated true pass sets. |
| `true_pass_set_hits_allowed` | numeric | Quarterback hits allowed on PFF-designated true pass sets. |
| `pressures_allowed` | numeric | Total pressures allowed (sacks, hits, and hurries). |
| `true_pass_set_pbe` | numeric | PFF Pass Blocking Efficiency rating, pressures allowed per pass-blocking snap weighted toward sacks on PFF-designated true pass sets. |
| `snap_counts_pass_play` | numeric | Pass-play snaps. |
| `penalties` | numeric | Total number of penalties. |
| `sacks_allowed` | numeric | Opponent sacks. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `position` | character | Primary position as reported by NFL.com |
| `true_pass_set_grades_pass_block` | numeric | PFF pass-blocking grade on PFF-designated true pass sets, 0-100. |
| `non_spike_pass_block` | numeric | Pass-blocking snaps excluding spike plays. |
| `true_pass_set_snap_counts_pass_block` | numeric | Pass-blocking snaps played on PFF-designated true pass sets. |
| `player` | character | Player name |
| `true_pass_set_sacks_allowed` | numeric | Sacks allowed on PFF-designated true pass sets. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `snap_counts_pass_block` | numeric | Pass-blocking snaps played. |
| `pass_block_percent` | numeric | Share of pass-play snaps spent pass blocking. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_pass_blocking()
```

_Last validated n/a._

## `pff_facet_passing_summary`

Facet report /passing/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/passing/summary`

**Valid URL:** [https://premium.pff.com/api/v1/facet/passing/summary](https://premium.pff.com/api/v1/facet/passing/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `grades_offense` | numeric | PFF overall offense grade (0-100). |
| `twp_rate` | numeric | Turnover-worthy-play rate. |
| `btt_rate` | numeric | Big-time-throw rate. |
| `spikes` | numeric | Clock-stopping spike plays. |
| `dropbacks` | numeric | Total quarterback dropbacks. |
| `thrown_aways` | numeric | Passes intentionally thrown away. |
| `draft_season` | numeric | Draft class (year) of the player. |
| `team_name` | character | Team name/abbreviation the player is credited to for the range. |
| `grades_pass` | numeric | PFF passing grade (0-100). |
| `hit_as_threw` | numeric | Plays where the quarterback was hit as he threw. |
| `first_downs` | numeric | Passing first downs. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `sack_percent` | numeric | Sack rate (sacks per dropback). |
| `bats` | numeric | Passes batted at the line. |
| `sacks` | numeric | Times the passer was sacked. |
| `player_game_count` | numeric | Games with at least one qualifying dropback in the requested range. |
| `eligible_season` | numeric | First eligible season for the player. |
| `completions` | numeric | Completed passes by the passer. |
| `yards` | numeric | Total passing yards gained. |
| `accuracy_percent` | numeric | Charted accuracy percentage. |
| `scrambles` | numeric | Scramble plays. |
| `interceptions` | numeric | Interceptions thrown. |
| `drop_rate` | numeric | Receiver drop rate on the quarterback's throws. |
| `grades_run` | numeric | PFF rushing grade (0-100). |
| `qb_rating` | numeric | NFL passer rating. |
| `completion_percent` | numeric | Completion percentage. |
| `penalties` | numeric | Penalties charged. |
| `attempts` | numeric | Pass attempts thrown by the passer. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | numeric | Declined penalties. |
| `passing_snaps` | numeric | Number of passing snaps played. |
| `pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack. |
| `ypa` | numeric | Yards gained per pass attempt. |
| `drops` | numeric | Throws dropped |
| `position` | character | Primary position as reported by NFL.com |
| `grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100). |
| `avg_time_to_throw` | numeric | Average time elapsed from the time of snap to throw on every pass attempt for a passer (sacks excluded). |
| `big_time_throws` | numeric | Number of big-time throws, per PFF's highest-value, highest-difficulty throw designation. |
| `player` | character | Player name |
| `positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `avg_depth_of_target` | numeric | Average depth of target in air yards. |
| `turnover_worthy_plays` | numeric | Number of turnover-worthy plays, plays PFF charts as deserving of a turnover. |
| `epa` | numeric | Expected points added (EPA) by the posteam for the given play. |
| `aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways), as charted by PFF. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `touchdowns` | numeric | Number of passing touchdowns thrown. |
| `def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks, as charted by PFF. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_passing_summary()
```

_Last validated n/a._

## `pff_facet_punting_summary`

Facet report /punting/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/punting/summary`

**Valid URL:** [https://premium.pff.com/api/v1/facet/punting/summary](https://premium.pff.com/api/v1/facet/punting/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `touchbacks` | numeric | Punts resulting in touchbacks. |
| `attempts_with_hangtime` | numeric | Punts with a PFF-recorded hangtime. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `percent_returned` | numeric | Percentage of the player's punts that were returned. |
| `fair_catches` | numeric | Punts fair-caught by the return team. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `average_net_yards` | numeric | Average net punting yards per attempt. |
| `yards` | numeric | The number of receiving yards |
| `average_hangtime` | numeric | Average punt hangtime in seconds. |
| `total_net_yards` | numeric | Total net punting yards. |
| `penalties` | numeric | Total number of penalties. |
| `attempts` | numeric | The number of pass attempts as defined by the NFL. |
| `inside_twenties` | numeric | Punts downed inside the opponent 20-yard line. |
| `out_of_bounds` | numeric | 1 if play description contains ran ob, pushed ob, or sacked ob; 0 otherwise. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `average_yards_per_return` | numeric | Average return yards allowed per punt returned. |
| `total_hangtime` | numeric | Total punt hangtime in seconds. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `returns` | numeric | Punts returned by the opponent. |
| `position` | character | Primary position as reported by NFL.com |
| `long` | numeric | Longest punt in yards. |
| `blocks` | numeric | Total blocks. |
| `average_yards_per_attempt` | numeric | Average gross punting yards per attempt. |
| `player` | character | Player name |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_punter` | numeric | PFF punting grade, 0-100. |
| `return_yards` | numeric | Yards gained by the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `downeds` | numeric | Punts downed by the coverage unit. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `snaps` | numeric | Punting snaps played. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_punting_summary()
```

_Last validated n/a._

## `pff_facet_passing_depth`

Facet report /passing/depth (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/passing/depth`

**Valid URL:** [https://premium.pff.com/api/v1/facet/passing/depth](https://premium.pff.com/api/v1/facet/passing/depth)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `left_behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the left side of the field. |
| `left_short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws to the left side of the field. |
| `center_short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws to the center of the field. |
| `right_short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the right side of the field. |
| `left_behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage to the left side of the field. |
| `left_medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the right side of the field. |
| `right_medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage, expressed as a percentage. |
| `right_short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `right_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the right side of the field. |
| `deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws, per PFF charting. |
| `center_medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws to the center of the field. |
| `medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws. |
| `left_deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws to the left side of the field. |
| `behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage. |
| `medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws, as charted by PFF. |
| `center_behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage to the center of the field. |
| `behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage. |
| `left_behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage to the left side of the field. |
| `left_deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws to the left side of the field. |
| `deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws. |
| `center_short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the center of the field, per PFF charting. |
| `center_medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws to the center of the field. |
| `center_short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the center of the field. |
| `center_deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws to the center of the field. |
| `left_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws to the center of the field. |
| `center_behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage to the center of the field. |
| `right_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the right side of the field. |
| `center_deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `right_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the right side of the field. |
| `right_medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws. |
| `center_medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the center of the field. |
| `deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws. |
| `right_medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws to the right side of the field. |
| `left_deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the left side of the field. |
| `right_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the right side of the field, expressed as a percentage. |
| `right_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the right side of the field, per PFF charting. |
| `center_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the center of the field, expressed as a percentage. |
| `deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws. |
| `center_behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage to the center of the field. |
| `right_behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the right side of the field, per PFF charting. |
| `center_deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws to the center of the field. |
| `center_short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the left side of the field. |
| `center_short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the center of the field. |
| `deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws. |
| `center_behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the center of the field. |
| `center_deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the center of the field. |
| `right_short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage to the right side of the field. |
| `short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws. |
| `center_medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws to the center of the field. |
| `left_behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage to the left side of the field. |
| `left_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the left side of the field. |
| `left_short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws to the left side of the field. |
| `right_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the right side of the field. |
| `draft_season` | numeric | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `right_behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage to the right side of the field. |
| `behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage, as charted by PFF. |
| `right_short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws to the right side of the field. |
| `deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws, as charted by PFF. |
| `right_short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the right side of the field, per PFF charting. |
| `center_behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the center of the field. |
| `left_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the left side of the field. |
| `deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws. |
| `center_short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `center_behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage to the center of the field. |
| `right_short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `center_short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws to the center of the field. |
| `medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws. |
| `right_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the right side of the field. |
| `center_medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws to the center of the field. |
| `medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws. |
| `right_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage. |
| `center_behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage to the center of the field. |
| `left_behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the left side of the field, per PFF charting. |
| `right_behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the right side of the field, per PFF charting. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws. |
| `center_medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the center of the field. |
| `left_behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws. |
| `right_behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the right side of the field. |
| `center_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the center of the field. |
| `deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws. |
| `left_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the left side of the field, per PFF charting. |
| `medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws. |
| `center_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the center of the field. |
| `short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws, expressed as a percentage. |
| `right_behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage to the right side of the field. |
| `center_medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws to the center of the field. |
| `left_short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the left side of the field. |
| `center_deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the center of the field. |
| `center_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the center of the field. |
| `left_behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws. |
| `right_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the right side of the field. |
| `left_deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `center_medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the center of the field. |
| `right_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the right side of the field. |
| `right_deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws to the right side of the field. |
| `left_deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws to the left side of the field. |
| `center_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the center of the field, per PFF charting. |
| `right_behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage to the right side of the field. |
| `left_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the left side of the field. |
| `right_deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the right side of the field. |
| `left_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the left side of the field. |
| `left_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the left side of the field. |
| `right_deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws to the right side of the field. |
| `left_deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage to the center of the field. |
| `right_deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws, as charted by PFF. |
| `right_short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws to the right side of the field. |
| `medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws, as charted by PFF. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `left_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the left side of the field. |
| `left_short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws to the left side of the field. |
| `center_deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the center of the field, per PFF charting. |
| `right_medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws to the right side of the field. |
| `left_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the left side of the field. |
| `left_behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage to the left side of the field. |
| `deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws. |
| `right_behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage to the right side of the field. |
| `behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage. |
| `right_deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the right side of the field. |
| `right_deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws to the right side of the field. |
| `right_short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `right_short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws to the right side of the field. |
| `short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws. |
| `right_deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the right side of the field, per PFF charting. |
| `right_behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the right side of the field. |
| `deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws. |
| `center_medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws to the center of the field. |
| `behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage, as charted by PFF. |
| `right_behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage to the right side of the field. |
| `right_medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the right side of the field. |
| `medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws. |
| `short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws. |
| `center_behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the center of the field, per PFF charting. |
| `left_behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage to the left side of the field. |
| `short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws, as charted by PFF. |
| `short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws. |
| `right_short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the right side of the field. |
| `medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws. |
| `left_behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the left side of the field. |
| `center_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the center of the field, expressed as a percentage. |
| `left_medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the left side of the field. |
| `center_short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the center of the field. |
| `left_deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage to the center of the field. |
| `center_short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws to the center of the field. |
| `right_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the right side of the field, per PFF charting. |
| `right_medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws to the right side of the field. |
| `medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws, per PFF charting. |
| `center_behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the center of the field, per PFF charting. |
| `center_short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws to the center of the field. |
| `left_medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws to the left side of the field. |
| `right_short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws to the right side of the field. |
| `left_medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the right side of the field. |
| `left_short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws, plays PFF charts as deserving of a turnover. |
| `left_deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the left side of the field. |
| `left_medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `base_dropbacks` | numeric | Number of dropbacks across all splits, the baseline total for this facet. |
| `center_deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the center of the field. |
| `center_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the center of the field. |
| `center_behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage to the center of the field. |
| `right_short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws to the right side of the field. |
| `center_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the center of the field. |
| `right_short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the right side of the field. |
| `left_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the left side of the field. |
| `center_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the center of the field. |
| `center_behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the center of the field. |
| `medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws. |
| `medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws. |
| `left_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the left side of the field. |
| `deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws, per PFF charting. |
| `left_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the left side of the field. |
| `short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws, per PFF charting. |
| `short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws. |
| `left_deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws to the left side of the field. |
| `short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws. |
| `center_medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage. |
| `right_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the right side of the field. |
| `right_medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws. |
| `player_game_count` | numeric | Number of games the player appeared in during the period covered. |
| `short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws, per PFF charting. |
| `right_short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws to the right side of the field. |
| `center_short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws to the center of the field. |
| `right_short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws to the right side of the field. |
| `behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage. |
| `right_medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws. |
| `medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws. |
| `short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws. |
| `right_deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the right side of the field, expressed as a percentage. |
| `eligible_season` | numeric | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage. |
| `center_behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the center of the field. |
| `right_deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the right side of the field. |
| `center_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the left side of the field. |
| `center_deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws to the center of the field. |
| `right_behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws. |
| `right_medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws to the right side of the field. |
| `medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws. |
| `left_deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws to the left side of the field. |
| `right_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the right side of the field. |
| `center_medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `left_behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage to the left side of the field. |
| `center_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the center of the field. |
| `right_short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the right side of the field. |
| `left_short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws to the left side of the field. |
| `deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws. |
| `right_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the right side of the field. |
| `center_behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the center of the field, expressed as a percentage. |
| `right_deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws to the right side of the field. |
| `left_short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws to the left side of the field. |
| `deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws. |
| `left_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the left side of the field. |
| `right_deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage. |
| `right_deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws to the right side of the field. |
| `left_behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `left_behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the left side of the field. |
| `deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws. |
| `left_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the left side of the field. |
| `medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws. |
| `left_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the left side of the field. |
| `right_behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage to the right side of the field. |
| `medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws. |
| `behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage. |
| `left_short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws to the left side of the field. |
| `center_short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws to the center of the field. |
| `center_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the center of the field. |
| `right_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the right side of the field. |
| `right_medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the right side of the field. |
| `right_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the right side of the field. |
| `right_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage to the right side of the field. |
| `behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage. |
| `center_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the center of the field. |
| `left_deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage to the center of the field. |
| `right_behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `left_short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws to the left side of the field. |
| `right_medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws to the right side of the field. |
| `left_behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage to the left side of the field. |
| `center_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the center of the field, per PFF charting. |
| `medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws. |
| `left_short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage to the left side of the field. |
| `short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws. |
| `right_medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the right side of the field. |
| `left_behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage to the left side of the field. |
| `right_medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws to the right side of the field. |
| `short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws, plays PFF charts as deserving of a turnover. |
| `right_deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws to the right side of the field. |
| `right_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the right side of the field. |
| `right_short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws to the right side of the field. |
| `medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws. |
| `center_deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the center of the field, expressed as a percentage. |
| `left_short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws to the left side of the field. |
| `behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage. |
| `short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws. |
| `left_medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws to the left side of the field. |
| `left_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the left side of the field. |
| `behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage. |
| `behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage. |
| `left_deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws to the left side of the field. |
| `left_short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `center_behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage to the center of the field. |
| `left_short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws to the left side of the field. |
| `left_short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the left side of the field. |
| `center_short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws to the center of the field. |
| `center_short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws to the center of the field. |
| `short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws. |
| `left_deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws to the left side of the field. |
| `deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws. |
| `center_behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage to the center of the field, plays PFF charts as deserving of a turnover. |
| `medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws. |
| `left_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the left side of the field. |
| `left_deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws to the left side of the field. |
| `deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws. |
| `short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws. |
| `left_medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws to the center of the field. |
| `center_short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `left_medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws to the center of the field. |
| `right_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the right side of the field. |
| `right_short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws. |
| `left_behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage to the left side of the field. |
| `right_deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws to the right side of the field. |
| `right_short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the right side of the field, per PFF charting. |
| `center_medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws to the center of the field. |
| `right_deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws to the right side of the field. |
| `center_deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage. |
| `right_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the right side of the field, per PFF charting. |
| `right_deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `right_deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws to the right side of the field. |
| `deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `left_short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `center_short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws to the center of the field. |
| `right_medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the right side of the field. |
| `left_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the left side of the field. |
| `short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws. |
| `medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws. |
| `left_medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws to the left side of the field. |
| `right_behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage to the right side of the field. |
| `behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage, per PFF charting. |
| `medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws. |
| `center_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the center of the field. |
| `behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage. |
| `right_medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the right side of the field. |
| `center_short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws to the center of the field. |
| `left_deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `left_medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws to the left side of the field. |
| `center_medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the center of the field. |
| `center_short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the center of the field. |
| `left_short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the left side of the field, per PFF charting. |
| `penalties` | numeric | Total number of penalties. |
| `right_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the right side of the field, expressed as a percentage. |
| `right_deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage. |
| `center_deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws to the center of the field. |
| `short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws. |
| `left_deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage to the center of the field. |
| `behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage. |
| `left_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the left side of the field. |
| `medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws, per PFF charting. |
| `left_short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws, plays PFF charts as deserving of a turnover. |
| `behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage, per PFF charting. |
| `left_behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage to the left side of the field. |
| `left_short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the left side of the field. |
| `center_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the center of the field. |
| `right_deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the right side of the field. |
| `medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws. |
| `center_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the center of the field. |
| `center_medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws to the center of the field. |
| `center_behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage to the center of the field. |
| `center_medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws to the center of the field. |
| `behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage. |
| `left_short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws. |
| `center_deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the center of the field. |
| `behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage. |
| `right_short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws to the right side of the field. |
| `left_deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws to the left side of the field. |
| `right_medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws to the right side of the field. |
| `right_behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage to the right side of the field, plays PFF charts as deserving of a turnover. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `right_medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws to the right side of the field. |
| `behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage, as charted by PFF. |
| `left_medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the left side of the field. |
| `medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws. |
| `deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws, as charted by PFF. |
| `right_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the right side of the field. |
| `short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws. |
| `deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws. |
| `declined_penalties` | numeric | Number of declined penalties committed by the player. |
| `deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws. |
| `left_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the left side of the field. |
| `right_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the right side of the field. |
| `medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws, expressed as a percentage. |
| `right_behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage to the right side of the field. |
| `left_medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `left_behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the left side of the field. |
| `right_short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `right_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the right side of the field, expressed as a percentage. |
| `center_short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws, as charted by PFF. |
| `right_medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `left_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the left side of the field. |
| `center_behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the center of the field. |
| `right_behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage to the right side of the field. |
| `deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws. |
| `left_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the left side of the field, per PFF charting. |
| `right_behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws to the center of the field. |
| `short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws. |
| `left_short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the left side of the field, per PFF charting. |
| `right_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the right side of the field. |
| `position` | character | Primary position as reported by NFL.com |
| `short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws, as charted by PFF. |
| `center_deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws to the center of the field. |
| `center_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the center of the field. |
| `right_behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage to the right side of the field. |
| `right_medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws to the right side of the field. |
| `left_medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws to the left side of the field. |
| `medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws. |
| `left_medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the left side of the field. |
| `left_short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws to the left side of the field. |
| `medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws. |
| `center_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage to the left side of the field. |
| `left_deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the left side of the field. |
| `deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws. |
| `left_medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws to the left side of the field. |
| `left_medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws to the left side of the field. |
| `right_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the right side of the field. |
| `left_short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the left side of the field. |
| `left_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the left side of the field. |
| `medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws. |
| `behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage, plays PFF charts as deserving of a turnover. |
| `right_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the right side of the field. |
| `right_deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws to the right side of the field. |
| `deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws. |
| `center_medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws to the center of the field. |
| `left_medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws to the left side of the field. |
| `left_short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage to the left side of the field. |
| `left_deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `center_short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws to the center of the field. |
| `center_deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws to the center of the field. |
| `right_deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws to the right side of the field. |
| `center_behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage to the center of the field. |
| `deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws. |
| `left_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the left side of the field, expressed as a percentage. |
| `short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws. |
| `behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage. |
| `right_short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage to the right side of the field. |
| `base_attempts` | numeric | Number of pass attempts across all splits, the baseline total for this facet. |
| `center_short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws to the center of the field. |
| `center_behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `center_deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `left_medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the left side of the field. |
| `behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage. |
| `left_deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the left side of the field, expressed as a percentage. |
| `center_deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `player` | character | Player name |
| `left_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the left side of the field. |
| `behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage. |
| `center_medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws. |
| `left_deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the left side of the field, per PFF charting. |
| `center_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the center of the field. |
| `left_behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `right_medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws to the right side of the field. |
| `right_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the right side of the field. |
| `center_medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `left_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the left side of the field. |
| `left_short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage to the left side of the field. |
| `left_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the left side of the field, per PFF charting. |
| `right_deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage. |
| `short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws. |
| `left_deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws to the left side of the field. |
| `right_behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage to the right side of the field. |
| `right_deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws to the right side of the field. |
| `left_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the left side of the field, expressed as a percentage. |
| `behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage, per PFF's highest-value, highest-difficulty throw designation. |
| `center_deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the center of the field. |
| `left_medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `right_behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the right side of the field. |
| `left_medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `right_deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `left_short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws to the left side of the field. |
| `left_medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `left_behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the left side of the field, per PFF charting. |
| `short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws. |
| `right_short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the right side of the field. |
| `right_short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws to the right side of the field. |
| `right_medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws to the right side of the field. |
| `center_short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the center of the field. |
| `right_short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws to the right side of the field. |
| `center_short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `center_medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the center of the field. |
| `medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws. |
| `center_medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `center_short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws to the center of the field. |
| `deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws, expressed as a percentage. |
| `center_behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `left_short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws to the left side of the field. |
| `center_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws to the center of the field. |
| `behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage. |
| `center_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the center of the field. |
| `left_deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `center_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the center of the field. |
| `left_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the left side of the field, expressed as a percentage. |
| `left_deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws to the left side of the field. |
| `right_short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws to the right side of the field. |
| `center_behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `right_short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws to the right side of the field. |
| `center_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the center of the field. |
| `deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws. |
| `left_deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the left side of the field. |
| `right_behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `center_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the center of the field. |
| `center_deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws. |
| `right_behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage to the right side of the field. |
| `center_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the center of the field. |
| `short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws. |
| `deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws, as charted by PFF. |
| `right_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the right side of the field. |
| `short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws. |
| `center_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the center of the field. |
| `left_medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws to the center of the field. |
| `left_deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws to the center of the field. |
| `center_medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws to the center of the field. |
| `right_deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `left_short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws to the left side of the field. |
| `medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `left_medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws to the right side of the field. |
| `center_short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage to the left side of the field, plays PFF charts as deserving of a turnover. |
| `center_medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the center of the field. |
| `left_medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws to the left side of the field. |
| `right_deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws to the right side of the field. |
| `medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws. |
| `right_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the right side of the field. |
| `deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws. |
| `behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage. |
| `center_deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws to the center of the field. |
| `center_short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the center of the field, per PFF charting. |
| `behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage. |
| `left_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws to the right side of the field. |
| `right_behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the right side of the field. |
| `left_behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage to the left side of the field. |
| `short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws. |
| `center_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the center of the field, per PFF charting. |
| `left_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the left side of the field. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_passing_depth()
```

_Last validated n/a._

## `pff_facet_passing_pressure`

Facet report /passing/pressure (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/passing/pressure`

**Valid URL:** [https://premium.pff.com/api/v1/facet/passing/pressure](https://premium.pff.com/api/v1/facet/passing/pressure)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `no_blitz_completion_percent` | numeric | Percentage of pass attempts completed when not blitzed. |
| `grades_offense` | numeric | PFF overall offense grade for the player (0-100). |
| `no_pressure_scrambles` | numeric | Number of scrambles from a clean pocket (no pressure). |
| `blitz_touchdowns` | numeric | Number of passing touchdowns thrown when blitzed. |
| `pressure_yards` | numeric | Passing yards gained when under pressure. |
| `no_pressure_spikes` | numeric | Number of clock-stopping spikes from a clean pocket (no pressure). |
| `blitz_ypa` | numeric | Yards gained per pass attempt when blitzed. |
| `no_blitz_grades_run` | numeric | PFF rushing grade for the player (0-100) when not blitzed. |
| `blitz_qb_rating` | numeric | Traditional NFL passer rating when blitzed. |
| `no_pressure_thrown_aways` | numeric | Number of intentional throwaways from a clean pocket (no pressure). |
| `no_blitz_bats` | numeric | Number of pass attempts batted down at the line of scrimmage when not blitzed. |
| `no_blitz_drops` | numeric | Number of catchable passes dropped by receivers when not blitzed. |
| `no_pressure_completion_percent` | numeric | Percentage of pass attempts completed from a clean pocket (no pressure). |
| `blitz_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) when blitzed, as charted by PFF. |
| `pressure_grades_run` | numeric | PFF rushing grade for the player (0-100) when under pressure. |
| `no_blitz_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when not blitzed. |
| `no_pressure_bats` | numeric | Number of pass attempts batted down at the line of scrimmage from a clean pocket (no pressure). |
| `pressure_completions` | numeric | Number of completed passes when under pressure. |
| `blitz_big_time_throws` | numeric | Number of big-time throws when blitzed, per PFF's highest-value, highest-difficulty throw designation. |
| `no_blitz_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when not blitzed. |
| `blitz_spikes` | numeric | Number of clock-stopping spikes when blitzed. |
| `no_pressure_completions` | numeric | Number of completed passes from a clean pocket (no pressure). |
| `draft_season` | numeric | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `no_pressure_passing_snaps` | numeric | Number of passing snaps played from a clean pocket (no pressure). |
| `no_blitz_first_downs` | numeric | Number of passing first downs gained when not blitzed. |
| `blitz_avg_time_to_throw` | numeric | Average time from snap to release in seconds when blitzed. |
| `no_pressure_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) from a clean pocket (no pressure). |
| `pressure_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) when under pressure, as charted by PFF. |
| `blitz_sacks` | numeric | Number of sacks taken when blitzed. |
| `no_pressure_interceptions` | numeric | Number of passes intercepted from a clean pocket (no pressure). |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `pressure_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when under pressure. |
| `blitz_completions` | numeric | Number of completed passes when blitzed. |
| `blitz_attempts` | numeric | Number of pass attempts when blitzed. |
| `pressure_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when under pressure, per PFF charting. |
| `pressure_sacks` | numeric | Number of sacks taken when under pressure. |
| `no_blitz_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack when not blitzed. |
| `no_pressure_ypa` | numeric | Yards gained per pass attempt from a clean pocket (no pressure). |
| `pressure_passing_snaps` | numeric | Number of passing snaps played when under pressure. |
| `grades_pass` | numeric | PFF passing grade (0-100). |
| `pressure_bats` | numeric | Number of pass attempts batted down at the line of scrimmage when under pressure. |
| `blitz_thrown_aways` | numeric | Number of intentional throwaways when blitzed. |
| `no_pressure_drops` | numeric | Number of catchable passes dropped by receivers from a clean pocket (no pressure). |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `pressure_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when under pressure. |
| `pressure_scrambles` | numeric | Number of scrambles when under pressure. |
| `blitz_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when blitzed. |
| `pressure_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when under pressure. |
| `blitz_completion_percent` | numeric | Percentage of pass attempts completed when blitzed. |
| `no_pressure_first_downs` | numeric | Number of passing first downs gained from a clean pocket (no pressure). |
| `blitz_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when blitzed. |
| `no_blitz_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when not blitzed. |
| `blitz_interceptions` | numeric | Number of passes intercepted when blitzed. |
| `no_blitz_dropbacks` | numeric | Number of dropbacks when not blitzed. |
| `no_blitz_grades_pass` | numeric | PFF passing grade (0-100) when not blitzed. |
| `no_blitz_scrambles` | numeric | Number of scrambles when not blitzed. |
| `pressure_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when under pressure. |
| `no_blitz_yards` | numeric | Passing yards gained when not blitzed. |
| `base_dropbacks` | numeric | Number of dropbacks across all splits, the baseline total for this facet. |
| `pressure_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack, reported within the pressure split. |
| `no_blitz_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when not blitzed. |
| `no_pressure_grades_offense` | numeric | PFF overall offense grade for the player (0-100) from a clean pocket (no pressure). |
| `no_pressure_avg_time_to_throw` | numeric | Average time from snap to release in seconds from a clean pocket (no pressure). |
| `pressure_dropbacks` | numeric | Number of dropbacks when under pressure. |
| `no_blitz_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when not blitzed. |
| `no_pressure_grades_run` | numeric | PFF rushing grade for the player (0-100) from a clean pocket (no pressure). |
| `player_game_count` | numeric | Number of games the player appeared in during the period covered. |
| `blitz_turnover_worthy_plays` | numeric | Number of turnover-worthy plays when blitzed, plays PFF charts as deserving of a turnover. |
| `no_blitz_touchdowns` | numeric | Number of passing touchdowns thrown when not blitzed. |
| `no_blitz_avg_depth_of_target` | numeric | Average depth of target in air yards when not blitzed. |
| `no_pressure_dropbacks_percent` | numeric | Share of the player's total dropbacks that came from a clean pocket (no pressure), expressed as a percentage. |
| `blitz_grades_pass` | numeric | PFF passing grade (0-100) when blitzed. |
| `eligible_season` | numeric | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `blitz_avg_depth_of_target` | numeric | Average depth of target in air yards when blitzed. |
| `no_blitz_spikes` | numeric | Number of clock-stopping spikes when not blitzed. |
| `no_pressure_dropbacks` | numeric | Number of dropbacks from a clean pocket (no pressure). |
| `blitz_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when blitzed, per PFF charting. |
| `blitz_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when blitzed. |
| `no_pressure_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts from a clean pocket (no pressure), per PFF charting. |
| `no_pressure_drop_rate` | numeric | Percentage of catchable passes dropped by receivers from a clean pocket (no pressure). |
| `no_blitz_turnover_worthy_plays` | numeric | Number of turnover-worthy plays when not blitzed, plays PFF charts as deserving of a turnover. |
| `pressure_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when under pressure. |
| `blitz_first_downs` | numeric | Number of passing first downs gained when blitzed. |
| `no_blitz_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when not blitzed, expressed as a percentage. |
| `pressure_turnover_worthy_plays` | numeric | Number of turnover-worthy plays when under pressure, plays PFF charts as deserving of a turnover. |
| `no_pressure_epa` | numeric | Total expected points added (EPA) on the player's dropbacks from a clean pocket (no pressure). |
| `no_blitz_avg_time_to_throw` | numeric | Average time from snap to release in seconds when not blitzed. |
| `no_blitz_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when not blitzed. |
| `pressure_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when under pressure, per PFF charting. |
| `no_pressure_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) from a clean pocket (no pressure), as charted by PFF. |
| `pressure_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when under pressure, expressed as a percentage. |
| `grades_run` | numeric | PFF rushing grade for the player (0-100). |
| `no_pressure_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks from a clean pocket (no pressure), as charted by PFF. |
| `pressure_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when under pressure. |
| `pressure_completion_percent` | numeric | Percentage of pass attempts completed when under pressure. |
| `pressure_avg_depth_of_target` | numeric | Average depth of target in air yards when under pressure. |
| `blitz_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when blitzed. |
| `pressure_drops` | numeric | Number of catchable passes dropped by receivers when under pressure. |
| `no_blitz_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks when not blitzed, as charted by PFF. |
| `pressure_attempts` | numeric | Number of pass attempts when under pressure. |
| `pressure_big_time_throws` | numeric | Number of big-time throws when under pressure, per PFF's highest-value, highest-difficulty throw designation. |
| `no_blitz_thrown_aways` | numeric | Number of intentional throwaways when not blitzed. |
| `blitz_drops` | numeric | Number of catchable passes dropped by receivers when blitzed. |
| `no_pressure_touchdowns` | numeric | Number of passing touchdowns thrown from a clean pocket (no pressure). |
| `no_pressure_sacks` | numeric | Number of sacks taken from a clean pocket (no pressure). |
| `blitz_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when blitzed. |
| `pressure_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when under pressure. |
| `penalties` | numeric | Total number of penalties. |
| `no_pressure_attempts` | numeric | Number of pass attempts from a clean pocket (no pressure). |
| `no_blitz_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when not blitzed. |
| `blitz_scrambles` | numeric | Number of scrambles when blitzed. |
| `blitz_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when blitzed, expressed as a percentage. |
| `no_pressure_qb_rating` | numeric | Traditional NFL passer rating from a clean pocket (no pressure). |
| `no_blitz_passing_snaps` | numeric | Number of passing snaps played when not blitzed. |
| `no_blitz_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) when not blitzed, as charted by PFF. |
| `blitz_yards` | numeric | Passing yards gained when blitzed. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `no_blitz_attempts` | numeric | Number of pass attempts when not blitzed. |
| `pressure_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF when under pressure. |
| `no_blitz_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw when not blitzed, as charted by PFF. |
| `pressure_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw when under pressure, as charted by PFF. |
| `blitz_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack when blitzed. |
| `declined_penalties` | numeric | Number of declined penalties committed by the player. |
| `no_blitz_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when not blitzed, per PFF charting. |
| `no_blitz_ypa` | numeric | Yards gained per pass attempt when not blitzed. |
| `blitz_grades_run` | numeric | PFF rushing grade for the player (0-100) when blitzed. |
| `pressure_interceptions` | numeric | Number of passes intercepted when under pressure. |
| `blitz_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when blitzed. |
| `no_pressure_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) from a clean pocket (no pressure). |
| `position` | character | Primary position as reported by NFL.com |
| `blitz_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when blitzed. |
| `grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100). |
| `pressure_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks when under pressure, as charted by PFF. |
| `pressure_grades_pass` | numeric | PFF passing grade (0-100) when under pressure. |
| `no_blitz_qb_rating` | numeric | Traditional NFL passer rating when not blitzed. |
| `blitz_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw when blitzed, as charted by PFF. |
| `pressure_ypa` | numeric | Yards gained per pass attempt when under pressure. |
| `blitz_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when blitzed. |
| `pressure_avg_time_to_throw` | numeric | Average time from snap to release in seconds when under pressure. |
| `no_blitz_completions` | numeric | Number of completed passes when not blitzed. |
| `no_pressure_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) from a clean pocket (no pressure). |
| `no_pressure_sack_percent` | numeric | Percentage of dropbacks that ended in a sack from a clean pocket (no pressure). |
| `player` | character | Player name |
| `no_blitz_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF when not blitzed. |
| `no_pressure_pressure_to_sack_rate` | character | Pressure-to-sack rate as reported within the no-pressure split of the PFF passing-pressure facet. |
| `no_pressure_turnover_worthy_plays` | numeric | Number of turnover-worthy plays from a clean pocket (no pressure), plays PFF charts as deserving of a turnover. |
| `pressure_first_downs` | numeric | Number of passing first downs gained when under pressure. |
| `blitz_passing_snaps` | numeric | Number of passing snaps played when blitzed. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `no_pressure_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts from a clean pocket (no pressure), per PFF charting. |
| `no_blitz_sacks` | numeric | Number of sacks taken when not blitzed. |
| `no_blitz_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when not blitzed. |
| `no_pressure_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF from a clean pocket (no pressure). |
| `no_blitz_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when not blitzed, per PFF charting. |
| `no_pressure_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw from a clean pocket (no pressure), as charted by PFF. |
| `no_pressure_grades_pass` | numeric | PFF passing grade (0-100) from a clean pocket (no pressure). |
| `no_pressure_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added from a clean pocket (no pressure). |
| `pressure_spikes` | numeric | Number of clock-stopping spikes when under pressure. |
| `pressure_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when under pressure. |
| `pressure_qb_rating` | numeric | Traditional NFL passer rating when under pressure. |
| `blitz_bats` | numeric | Number of pass attempts batted down at the line of scrimmage when blitzed. |
| `pressure_touchdowns` | numeric | Number of passing touchdowns thrown when under pressure. |
| `pressure_thrown_aways` | numeric | Number of intentional throwaways when under pressure. |
| `no_blitz_interceptions` | numeric | Number of passes intercepted when not blitzed. |
| `blitz_dropbacks` | numeric | Number of dropbacks when blitzed. |
| `blitz_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks when blitzed, as charted by PFF. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `no_blitz_big_time_throws` | numeric | Number of big-time throws when not blitzed, per PFF's highest-value, highest-difficulty throw designation. |
| `no_pressure_avg_depth_of_target` | numeric | Average depth of target in air yards from a clean pocket (no pressure). |
| `no_pressure_yards` | numeric | Passing yards gained from a clean pocket (no pressure). |
| `blitz_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when blitzed, per PFF charting. |
| `blitz_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF when blitzed. |
| `no_pressure_big_time_throws` | numeric | Number of big-time throws from a clean pocket (no pressure), per PFF's highest-value, highest-difficulty throw designation. |
| `no_blitz_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) when not blitzed. |
| `blitz_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) when blitzed. |
| `no_pressure_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `pressure_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) when under pressure. |
| `no_pressure_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) from a clean pocket (no pressure). |
| `blitz_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when blitzed. |
| `pressure_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when under pressure. |
| `no_blitz_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when not blitzed. |
| `blitz_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) when blitzed. |
| `no_pressure_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `no_blitz_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) when not blitzed. |
| `blitz_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when blitzed. |
| `pressure_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when under pressure. |
| `no_blitz_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when not blitzed. |
| `pressure_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) when under pressure. |
| `no_pressure_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `pressure_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) when under pressure. |
| `pressure_grades_defense` | numeric | PFF overall defense grade for the player (0-100) when under pressure. |
| `no_blitz_grades_defense` | numeric | PFF overall defense grade for the player (0-100) when not blitzed. |
| `no_blitz_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) when not blitzed. |
| `blitz_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) when blitzed. |
| `no_pressure_grades_defense` | numeric | PFF overall defense grade for the player (0-100) from a clean pocket (no pressure). |
| `no_pressure_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) from a clean pocket (no pressure). |
| `no_blitz_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) when not blitzed. |
| `blitz_grades_defense` | numeric | PFF overall defense grade for the player (0-100) when blitzed. |
| `blitz_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) when blitzed. |
| `no_pressure_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) from a clean pocket (no pressure). |
| `pressure_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) when under pressure. |
| `pressure_grades_pass_rush_defense` | numeric | PFF pass-rush grade for the player (0-100) when under pressure. |
| `blitz_grades_pass_rush_defense` | numeric | PFF pass-rush grade for the player (0-100) when blitzed. |
| `no_pressure_grades_tackle` | numeric | PFF tackling grade for the player (0-100) from a clean pocket (no pressure). |
| `blitz_grades_tackle` | numeric | PFF tackling grade for the player (0-100) when blitzed. |
| `blitz_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) when blitzed. |
| `no_pressure_grades_pass_rush_defense` | numeric | PFF pass-rush grade for the player (0-100) from a clean pocket (no pressure). |
| `pressure_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) when under pressure. |
| `no_blitz_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) when not blitzed. |
| `pressure_grades_tackle` | numeric | PFF tackling grade for the player (0-100) when under pressure. |
| `no_blitz_grades_pass_rush_defense` | character | PFF pass-rush grade for the player (0-100) when not blitzed. |
| `no_pressure_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) from a clean pocket (no pressure). |
| `no_blitz_grades_tackle` | numeric | PFF tackling grade for the player (0-100) when not blitzed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_passing_pressure()
```

_Last validated n/a._

## `pff_facet_receiving_summary`

Facet report /receiving/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/receiving/summary`

**Valid URL:** [https://premium.pff.com/api/v1/facet/receiving/summary](https://premium.pff.com/api/v1/facet/receiving/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `targets` | numeric | Times targeted. |
| `grades_pass_block` | numeric | PFF pass-blocking grade, 0-100. |
| `grades_offense` | numeric | PFF overall offense grade (0-100). |
| `yards_after_catch_per_reception` | numeric | Average yards after the catch per reception. |
| `grades_pass_route` | numeric | PFF receiving/route grade (0-100). |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `yprr` | numeric | Yards per route run. |
| `wide_snaps` | numeric | Receiving snaps aligned out wide. |
| `fumbles` | numeric | Fumbles by the player after the catch. |
| `first_downs` | numeric | First downs earned by the team. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `inline_snaps` | numeric | Receiving snaps aligned inline, tight to the formation. |
| `contested_targets` | numeric | Contested targets. |
| `player_game_count` | numeric | Games with at least one qualifying snap in the requested range. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `inline_rate` | numeric | Share of receiving snaps aligned inline, tight to the formation. |
| `contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught. |
| `yards` | numeric | Receiving yards. |
| `receptions` | numeric | Passes caught by the receiver. |
| `targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player. |
| `interceptions` | numeric | The number of interceptions thrown. |
| `caught_percent` | numeric | Percentage of targets caught. |
| `drop_rate` | numeric | Share of catchable targets the player dropped. |
| `grades_hands_drop` | numeric | PFF hands/drop grade (0-100). |
| `slot_rate` | numeric | Share of receiving snaps aligned in the slot. |
| `slot_snaps` | numeric | Receiving snaps aligned in the slot. |
| `penalties` | numeric | Total number of penalties. |
| `wide_rate` | numeric | Share of receiving snaps aligned out wide. |
| `pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `route_rate` | numeric | Share of pass-play snaps on which the player ran a route. |
| `drops` | numeric | Dropped passes. |
| `position` | character | Primary position as reported by NFL.com |
| `grades_hands_fumble` | numeric | PFF ball-security (hands/fumble) grade, 0-100. |
| `longest` | numeric | Longest reception in yards. |
| `pass_blocks` | numeric | Pass-play snaps spent pass blocking. |
| `routes` | numeric | Pass routes run by the receiver. |
| `pass_plays` | numeric | Pass-play snaps. |
| `yards_per_reception` | numeric | Average yards per reception. |
| `player` | character | Player name |
| `positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `contested_receptions` | numeric | Contested catches made. |
| `yards_after_catch` | numeric | Yards after the catch. |
| `avg_depth_of_target` | numeric | Average depth of target in yards downfield. |
| `epa` | numeric | Expected points added (EPA) by the posteam for the given play. |
| `avoided_tackles` | numeric | Tackles avoided after the catch. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `touchdowns` | numeric | Receiving touchdowns. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_receiving_summary()
```

_Last validated n/a._

## `pff_facet_return_summary`

Facet report /return/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/return/summary`

**Valid URL:** [https://premium.pff.com/api/v1/facet/return/summary](https://premium.pff.com/api/v1/facet/return/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_kick_return` | numeric | PFF kickoff-return grade, 0-100. |
| `grades_return` | numeric | PFF overall return grade, 0-100. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `kickoff_attempts` | numeric | Kickoff returns attempted. |
| `kickoff_fair_catches` | numeric | Kickoffs fair-caught by the player. |
| `kickoff_long` | numeric | Longest kickoff return in yards. |
| `kickoff_muffed_returns` | numeric | Kickoff returns the player muffed. |
| `kickoff_touchdowns` | numeric | Kickoff returns scoring a touchdown. |
| `kickoff_yards` | numeric | Total kickoff-return yards. |
| `kickoff_ypa` | numeric | Average yards per kickoff return. |
| `penalties` | numeric | Total number of penalties. |
| `player` | character | Player name |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `position` | character | Primary position as reported by NFL.com |
| `punt_attempts` | numeric | Punt returns attempted. |
| `punt_fair_catches` | numeric | Punts fair-caught by the player. |
| `punt_long` | numeric | Longest punt return in yards. |
| `punt_muffed_returns` | numeric | Punt returns the player muffed. |
| `punt_touchdowns` | numeric | Punt returns scoring a touchdown. |
| `punt_yards` | numeric | Total punt-return yards. |
| `punt_ypa` | numeric | Average yards per punt return. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `total_attempts` | numeric | Total return attempts, kickoffs and punts combined. |
| `grades_punt_return` | numeric | PFF punt-return grade, 0-100. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_return_summary()
```

_Last validated n/a._

## `pff_facet_rushing_direction_stats`

Facet report /rushing/direction (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/rushing/direction`

**Valid URL:** [https://premium.pff.com/api/v1/facet/rushing/direction](https://premium.pff.com/api/v1/facet/rushing/direction)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `directions` | list | Nested per-direction rushing splits (attempts and results by run direction) as returned by the PFF API. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `player` | character | Player name |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `position` | character | Primary position as reported by NFL.com |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `total_attempts` | numeric | Total rushing attempts across all run directions. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_rushing_direction_stats()
```

_Last validated n/a._

## `pff_facet_receiving_coverage_stats`

Facet report /defense/coverage_matchup (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/defense/coverage_matchup`

**Valid URL:** [https://premium.pff.com/api/v1/facet/defense/coverage_matchup](https://premium.pff.com/api/v1/facet/defense/coverage_matchup)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_report`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_receiving_coverage_stats()
```

_Last validated n/a._

## `pff_facet_rushing_summary`

Facet report /rushing/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/rushing/summary`

**Valid URL:** [https://premium.pff.com/api/v1/facet/rushing/summary](https://premium.pff.com/api/v1/facet/rushing/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `targets` | numeric | The number of pass plays where the player was the targeted receiver. |
| `grades_pass_block` | numeric | PFF pass-blocking grade, 0-100. |
| `grades_offense` | numeric | PFF overall offense grade (0-100). |
| `yards_after_contact` | numeric | Yards after contact. |
| `explosive` | numeric | Runs PFF designates as explosive. |
| `grades_pass_route` | numeric | PFF route-running (receiving) grade, 0-100. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `elu_rush_mtf` | numeric | Missed tackles forced as a rusher, an input to PFF's elusive rating. |
| `breakaway_attempts` | numeric | Runs of 15 or more yards, PFF's breakaway designation. |
| `designed_yards` | numeric | Rushing yards gained on designed runs, excluding scrambles. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `yprr` | numeric | Yards per route run. |
| `breakaway_percent` | numeric | Share of rushing yards gained on breakaway runs of 15 or more yards. |
| `fumbles` | numeric | Fumbles by the ball carrier. |
| `first_downs` | numeric | Rushing first downs. |
| `elusive_rating` | numeric | PFF elusive rating. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `breakaway_yards` | numeric | Breakaway (long-run) yards. |
| `player_game_count` | numeric | Games with at least one qualifying snap in the requested range. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `total_touches` | numeric | Combined carries and receptions. |
| `scramble_yards` | numeric | Rushing yards gained on scrambles. |
| `yco_attempt` | numeric | Average yards after contact per rushing attempt. |
| `yards` | numeric | Total rushing yards gained. |
| `grades_run_block` | numeric | PFF run-blocking grade, 0-100. |
| `receptions` | numeric | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `zone_attempts` | numeric | Rushing attempts on zone-scheme runs. |
| `scrambles` | numeric | Quarterback scrambles. |
| `grades_run` | numeric | PFF rushing grade (0-100). |
| `penalties` | numeric | Total number of penalties. |
| `attempts` | numeric | Rushing attempts (carries) by the runner. |
| `elu_yco` | numeric | Yards-after-contact component used in PFF's elusive rating. |
| `elu_recv_mtf` | numeric | Missed tackles forced as a receiver, an input to PFF's elusive rating. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `ypa` | numeric | Average yards per rushing attempt. |
| `drops` | numeric | Throws dropped |
| `position` | character | Primary position as reported by NFL.com |
| `grades_hands_fumble` | numeric | PFF ball-security (hands/fumble) grade, 0-100. |
| `longest` | numeric | Longest run in yards. |
| `routes` | numeric | Pass routes run by the player. |
| `player` | character | Player name |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `rec_yards` | numeric | Career receiving yards |
| `gap_attempts` | numeric | Rushing attempts on gap-scheme runs. |
| `run_plays` | numeric | Run-play snaps. |
| `avoided_tackles` | numeric | Missed tackles forced. |
| `grades_offense_penalty` | numeric | PFF offensive penalty grade, 0-100. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `touchdowns` | numeric | Rushing touchdowns. |
| `grades_pass` | numeric | PFF passing grade, 0-100. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_rushing_summary()
```

_Last validated n/a._

## `pff_facet_slot_coverages`

Facet report /signature/defense/slot_coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage`

**Valid URL:** [https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage](https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `coverage_snaps` | numeric | Coverage snaps played while covering the slot. |
| `coverage_snaps_per_reception` | numeric | Coverage snaps played per reception allowed while covering the slot. |
| `coverage_snaps_per_target` | numeric | Coverage snaps played per target into the player's coverage while covering the slot. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `interceptions` | numeric | The number of interceptions thrown. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `player` | character | Player name |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `position` | character | Primary position as reported by NFL.com |
| `qb_rating_against` | numeric | NFL passer rating allowed on targets into the player's coverage while covering the slot. |
| `receptions` | numeric | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | numeric | The number of pass plays where the player was the targeted receiver. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `touchdowns` | numeric | Touchdowns allowed into the player's coverage while covering the slot. |
| `yards` | numeric | The number of receiving yards |
| `yards_after_catch` | numeric | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `yards_per_coverage_snap` | numeric | Receiving yards allowed per coverage snap while covering the slot. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_slot_coverages()
```

_Last validated n/a._

## `pff_facet_pbes`

Facet report /signature/pass-blocking/efficiency/line (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line`

**Valid URL:** [https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line](https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `attempts` | numeric | The number of pass attempts as defined by the NFL. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `hits_allowed` | numeric | Quarterback hits allowed. |
| `hurries_allowed` | numeric | Quarterback hurries allowed. |
| `pass_snaps` | numeric | Pass-play snaps. |
| `pbe` | numeric | PFF Pass Blocking Efficiency rating, pressures allowed per pass-blocking snap weighted toward sacks. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `pressures_allowed` | numeric | Total pressures allowed (sacks, hits, and hurries). |
| `sacks_allowed` | numeric | Opponent sacks. |
| `season_id` | numeric | Unique season identifier. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_pbes()
```

_Last validated n/a._

## `pff_facet_prps`

Facet report /signature/defense/outside_pass_rush (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush`

**Valid URL:** [https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush](https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `lhs_sacks` | numeric | Sacks recorded when rushing from the left side. |
| `rhs_hits` | numeric | Quarterback hits recorded when rushing from the right side. |
| `rhs_prp` | numeric | PFF Pass Rush Productivity rating, pressure generated per pass-rush snap weighted toward sacks when rushing from the right side. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `pass_snaps` | numeric | Pass-play snaps. |
| `lhs_hurries` | numeric | Quarterback hurries recorded when rushing from the left side. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `prp` | numeric | PFF Pass Rush Productivity rating, pressure generated per pass-rush snap weighted toward sacks. |
| `tackles` | numeric | Team tackles. |
| `rhs_pressures` | numeric | Total pressures generated (sacks, hits, and hurries) when rushing from the right side. |
| `rhs_pass_rush_percent` | numeric | Share of pass-play snaps spent rushing the passer when rushing from the right side. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `lhs_pass_rush_percent` | numeric | Share of pass-play snaps spent rushing the passer when rushing from the left side. |
| `lhs_pass_rush_snaps` | numeric | Pass-rush snaps played when rushing from the left side. |
| `sacks` | numeric | The Number of times sacked. |
| `lhs_assists` | numeric | Assisted tackles when rushing from the left side. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `lhs_pressures` | numeric | Total pressures generated (sacks, hits, and hurries) when rushing from the left side. |
| `pass_rush_snaps` | numeric | Pass-rush snaps played. |
| `hurries` | numeric | Quarterback hurries recorded. |
| `rhs_pass_rush_snaps` | numeric | Pass-rush snaps played when rushing from the right side. |
| `lhs_prp` | numeric | PFF Pass Rush Productivity rating, pressure generated per pass-rush snap weighted toward sacks when rushing from the left side. |
| `hits` | numeric | Hits. |
| `lhs_hits` | numeric | Quarterback hits recorded when rushing from the left side. |
| `rhs_tackles` | numeric | Tackles made when rushing from the right side. |
| `lhs_stops` | numeric | Stops, PFF's tackles that constitute a failed play for the offense when rushing from the left side. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `stops` | numeric | Stops, PFF's tackles that constitute a failed play for the offense. |
| `rhs_misses` | numeric | Missed tackles when rushing from the right side. |
| `position` | character | Primary position as reported by NFL.com |
| `pressures` | numeric | Total pressures generated (sacks, hits, and hurries). |
| `misses` | numeric | Missed tackles. |
| `player` | character | Player name |
| `rhs_sacks` | numeric | Sacks recorded when rushing from the right side. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `pass_rush_percent` | numeric | Share of pass-play snaps spent rushing the passer. |
| `rhs_hurries` | numeric | Quarterback hurries recorded when rushing from the right side. |
| `rhs_assists` | numeric | Assisted tackles when rushing from the right side. |
| `rhs_stops` | numeric | Stops, PFF's tackles that constitute a failed play for the offense when rushing from the right side. |
| `assists` | numeric | Total assists. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `lhs_tackles` | numeric | Tackles made when rushing from the left side. |
| `lhs_misses` | numeric | Missed tackles when rushing from the left side. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_prps()
```

_Last validated n/a._

## `pff_facet_receiving_scheme`

Facet report /receiving/scheme (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/receiving/scheme`

**Valid URL:** [https://premium.pff.com/api/v1/facet/receiving/scheme](https://premium.pff.com/api/v1/facet/receiving/scheme)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `man_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught against man coverage. |
| `man_touchdowns` | numeric | Receiving touchdowns scored against man coverage. |
| `zone_targets_percent` | numeric | Share of the team's targets thrown to the player against zone coverage. |
| `man_interceptions` | numeric | Interceptions thrown on passes targeting the player against man coverage. |
| `zone_epa` | numeric | Total expected points added on targets to the player against zone coverage. |
| `man_avg_depth_of_target` | numeric | Average depth of target in yards downfield against man coverage. |
| `zone_pass_blocks` | numeric | Pass-play snaps spent pass blocking against zone coverage. |
| `zone_avoided_tackles` | numeric | Tackles avoided after the catch against zone coverage. |
| `man_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player against man coverage. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `zone_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added against zone coverage. |
| `man_targets_percent` | numeric | Share of the team's targets thrown to the player against man coverage. |
| `man_yards_per_reception` | numeric | Average yards per reception against man coverage. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `man_drop_rate` | numeric | Share of catchable targets the player dropped against man coverage. |
| `zone_grades_pass_route` | numeric | PFF route-running (receiving) grade against zone coverage, 0-100. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `zone_fumbles` | numeric | Fumbles by the player after the catch against zone coverage. |
| `man_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking against man coverage. |
| `zone_yards` | numeric | Receiving yards gained against zone coverage. |
| `man_yprr` | numeric | Yards per route run against man coverage. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `zone_drops` | numeric | PFF-charted drops against zone coverage. |
| `zone_receptions` | numeric | Receptions made against zone coverage. |
| `man_pass_plays` | numeric | Pass-play snaps against man coverage. |
| `man_epa` | numeric | Total expected points added on targets to the player against man coverage. |
| `zone_yards_per_reception` | numeric | Average yards per reception against zone coverage. |
| `man_contested_targets` | numeric | PFF-charted contested targets against man coverage. |
| `man_longest` | numeric | Longest reception in yards against man coverage. |
| `zone_yards_after_catch` | numeric | Yards gained after the catch against zone coverage. |
| `man_receptions` | numeric | Receptions made against man coverage. |
| `man_avoided_tackles` | numeric | Tackles avoided after the catch against man coverage. |
| `man_first_downs` | numeric | Receptions that converted a first down against man coverage. |
| `base_targets` | numeric | Total targets from the facet's unsplit base row, across all coverage schemes. |
| `zone_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught against zone coverage. |
| `zone_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player against zone coverage. |
| `man_routes` | numeric | Pass routes run by the player against man coverage. |
| `zone_grades_hands_drop` | numeric | PFF hands/drop grade against zone coverage, 0-100. |
| `man_route_rate` | numeric | Share of pass-play snaps on which the player ran a route against man coverage. |
| `zone_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking against zone coverage. |
| `man_grades_pass_route` | numeric | PFF route-running (receiving) grade against man coverage, 0-100. |
| `penalties` | numeric | Total number of penalties. |
| `zone_first_downs` | numeric | Receptions that converted a first down against zone coverage. |
| `zone_yprr` | numeric | Yards per route run against zone coverage. |
| `man_drops` | numeric | PFF-charted drops against man coverage. |
| `zone_caught_percent` | numeric | Percentage of targets caught against zone coverage. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `man_fumbles` | numeric | Fumbles by the player after the catch against man coverage. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `man_yards_after_catch` | numeric | Yards gained after the catch against man coverage. |
| `man_yards` | numeric | Receiving yards gained against man coverage. |
| `zone_pass_plays` | numeric | Pass-play snaps against zone coverage. |
| `position` | character | Primary position as reported by NFL.com |
| `man_targets` | numeric | Pass targets to the player against man coverage. |
| `man_grades_hands_drop` | numeric | PFF hands/drop grade against man coverage, 0-100. |
| `man_pass_blocks` | numeric | Pass-play snaps spent pass blocking against man coverage. |
| `zone_touchdowns` | numeric | Receiving touchdowns scored against zone coverage. |
| `zone_route_rate` | numeric | Share of pass-play snaps on which the player ran a route against zone coverage. |
| `zone_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception against zone coverage. |
| `zone_avg_depth_of_target` | numeric | Average depth of target in yards downfield against zone coverage. |
| `player` | character | Player name |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `zone_contested_targets` | numeric | PFF-charted contested targets against zone coverage. |
| `zone_contested_receptions` | numeric | Catches made on PFF-charted contested targets against zone coverage. |
| `man_contested_receptions` | numeric | Catches made on PFF-charted contested targets against man coverage. |
| `man_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception against man coverage. |
| `zone_targets` | numeric | Pass targets to the player against zone coverage. |
| `zone_longest` | numeric | Longest reception in yards against zone coverage. |
| `man_caught_percent` | numeric | Percentage of targets caught against man coverage. |
| `zone_drop_rate` | numeric | Share of catchable targets the player dropped against zone coverage. |
| `zone_interceptions` | numeric | Interceptions thrown on passes targeting the player against zone coverage. |
| `man_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added against man coverage. |
| `zone_routes` | numeric | Pass routes run by the player against zone coverage. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_receiving_scheme()
```

_Last validated n/a._

## `pff_facet_time_in_pockets`

Facet report /signature/passing/time_in_pocket (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket`

**Valid URL:** [https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket](https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `more_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on dropbacks with time in pocket of 2.5 seconds or more, per PFF charting. |
| `more_grades_run` | numeric | PFF rushing grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_ypa` | numeric | Yards gained per pass attempt on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_passing_snaps` | numeric | Number of passing snaps played on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on dropbacks with time in pocket under 2.5 seconds, as charted by PFF. |
| `dropbacks` | numeric | Number of dropbacks. |
| `less_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on dropbacks with time in pocket under 2.5 seconds. |
| `less_first_downs` | numeric | Number of passing first downs gained on dropbacks with time in pocket under 2.5 seconds. |
| `less_ypa` | numeric | Yards gained per pass attempt on dropbacks with time in pocket under 2.5 seconds. |
| `draft_season` | numeric | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `more_grades_pass_route` | character | PFF receiving (route) grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on dropbacks with time in pocket under 2.5 seconds. |
| `less_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on dropbacks with time in pocket under 2.5 seconds, as charted by PFF. |
| `more_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on dropbacks with time in pocket of 2.5 seconds or more, expressed as a percentage. |
| `less_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on dropbacks with time in pocket under 2.5 seconds. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `avg_ttt_scrambles` | numeric | Average time in the pocket in seconds on dropbacks ending in a scramble. |
| `more_yards` | numeric | Passing yards gained on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_attempts` | numeric | Number of pass attempts on dropbacks with time in pocket under 2.5 seconds. |
| `less_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on dropbacks with time in pocket under 2.5 seconds. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `more_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on dropbacks with time in pocket of 2.5 seconds or more, plays PFF charts as deserving of a turnover. |
| `more_interceptions` | numeric | Number of passes intercepted on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on dropbacks with time in pocket of 2.5 seconds or more, per PFF charting. |
| `less_completions` | numeric | Number of completed passes on dropbacks with time in pocket under 2.5 seconds. |
| `more_thrown_aways` | numeric | Number of intentional throwaways on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on dropbacks with time in pocket under 2.5 seconds, per PFF charting. |
| `more_big_time_throws` | numeric | Number of big-time throws on dropbacks with time in pocket of 2.5 seconds or more, per PFF's highest-value, highest-difficulty throw designation. |
| `less_qb_rating` | numeric | Traditional NFL passer rating on dropbacks with time in pocket under 2.5 seconds. |
| `less_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on dropbacks with time in pocket under 2.5 seconds, as charted by PFF. |
| `more_attempts` | numeric | Number of pass attempts on dropbacks with time in pocket of 2.5 seconds or more. |
| `player_game_count` | numeric | Number of games the player appeared in during the period covered. |
| `less_spikes` | numeric | Number of clock-stopping spikes on dropbacks with time in pocket under 2.5 seconds. |
| `eligible_season` | numeric | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `less_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on dropbacks with time in pocket under 2.5 seconds, expressed as a percentage. |
| `more_qb_rating` | numeric | Traditional NFL passer rating on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_dropbacks` | numeric | Number of dropbacks on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_avg_depth_of_target` | numeric | Average depth of target in air yards on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_scrambles` | numeric | Number of scrambles on dropbacks with time in pocket under 2.5 seconds. |
| `more_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_sacks` | numeric | Number of sacks taken on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_pass` | numeric | PFF passing grade (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_drops` | numeric | Number of catchable passes dropped by receivers on dropbacks with time in pocket under 2.5 seconds. |
| `more_sacks` | numeric | Number of sacks taken on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_first_downs` | numeric | Number of passing first downs gained on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_big_time_throws` | numeric | Number of big-time throws on dropbacks with time in pocket under 2.5 seconds, per PFF's highest-value, highest-difficulty throw designation. |
| `avg_ttt_attempts` | numeric | Average time from snap to release in seconds on dropbacks ending in a pass attempt. |
| `more_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on dropbacks with time in pocket of 2.5 seconds or more, as charted by PFF. |
| `less_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on dropbacks with time in pocket under 2.5 seconds. |
| `more_scrambles` | numeric | Number of scrambles on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on dropbacks with time in pocket under 2.5 seconds. |
| `more_spikes` | numeric | Number of clock-stopping spikes on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_pass_route` | character | PFF receiving (route) grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `avg_ttt_sacks` | numeric | Average time from snap to sack in seconds on dropbacks ending in a sack. |
| `more_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `less_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_run` | numeric | PFF rushing grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `position` | character | Primary position as reported by NFL.com |
| `less_touchdowns` | numeric | Number of passing touchdowns thrown on dropbacks with time in pocket under 2.5 seconds. |
| `less_yards` | numeric | Passing yards gained on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_run_block` | character | PFF run-blocking grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on dropbacks with time in pocket of 2.5 seconds or more, as charted by PFF. |
| `more_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on dropbacks with time in pocket of 2.5 seconds or more. |
| `avg_time_to_throw` | numeric | Average time elapsed from the time of snap to throw on every pass attempt for a passer (sacks excluded). |
| `less_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on dropbacks with time in pocket under 2.5 seconds, plays PFF charts as deserving of a turnover. |
| `less_completion_percent` | numeric | Percentage of pass attempts completed on dropbacks with time in pocket under 2.5 seconds. |
| `more_drops` | numeric | Number of catchable passes dropped by receivers on dropbacks with time in pocket of 2.5 seconds or more. |
| `player` | character | Player name |
| `more_touchdowns` | numeric | Number of passing touchdowns thrown on dropbacks with time in pocket of 2.5 seconds or more. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `more_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on dropbacks with time in pocket under 2.5 seconds, per PFF charting. |
| `more_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_thrown_aways` | numeric | Number of intentional throwaways on dropbacks with time in pocket under 2.5 seconds. |
| `less_avg_depth_of_target` | numeric | Average depth of target in air yards on dropbacks with time in pocket under 2.5 seconds. |
| `less_avg_time_to_throw` | numeric | Average time from snap to release in seconds on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_interceptions` | numeric | Number of passes intercepted on dropbacks with time in pocket under 2.5 seconds. |
| `more_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on dropbacks with time in pocket of 2.5 seconds or more, as charted by PFF. |
| `more_avg_time_to_throw` | numeric | Average time from snap to release in seconds on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_dropbacks` | numeric | Number of dropbacks on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_pass` | numeric | PFF passing grade (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `less_passing_snaps` | numeric | Number of passing snaps played on dropbacks with time in pocket under 2.5 seconds. |
| `more_completions` | numeric | Number of completed passes on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_run_block` | character | PFF run-blocking grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_completion_percent` | numeric | Percentage of pass attempts completed on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_hands_drop` | character | PFF hands (drop) grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_hands_drop` | character | PFF hands (drop) grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_pass_block` | character | PFF pass-blocking grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_pass_block` | character | PFF pass-blocking grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_screen_block` | character | PFF screen-blocking grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_screen_block` | character | PFF screen-blocking grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_defense_penalty` | character | PFF defensive penalty grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_coverage_defense` | character | PFF coverage grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_defense_penalty` | character | PFF defensive penalty grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_defense` | character | PFF overall defense grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_coverage_defense` | character | PFF coverage grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_defense` | character | PFF overall defense grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_overall_tackle` | character | PFF overall tackling grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_pass_rush_defense` | character | PFF pass-rush grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_run_defense` | character | PFF run-defense grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_tackle` | character | PFF tackling grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_pass_rush_defense` | character | PFF pass-rush grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_tackle` | character | PFF tackling grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_overall_tackle` | character | PFF overall tackling grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_run_defense` | character | PFF run-defense grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_time_in_pockets()
```

_Last validated n/a._

## `pff_facet_receiving_concept`

Facet report /receiving/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/receiving/concept`

**Valid URL:** [https://premium.pff.com/api/v1/facet/receiving/concept](https://premium.pff.com/api/v1/facet/receiving/concept)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `screen_caught_percent` | numeric | Percentage of targets caught on screen concepts. |
| `screen_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on screen concepts. |
| `slot_grades_pass_route` | numeric | PFF route-running (receiving) grade when aligned in the slot, 0-100. |
| `slot_avg_depth_of_target` | numeric | Average depth of target in yards downfield when aligned in the slot. |
| `slot_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added when aligned in the slot. |
| `screen_yprr` | numeric | Yards per route run on screen concepts. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `slot_routes` | numeric | Pass routes run by the player when aligned in the slot. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `screen_grades_hands_drop` | numeric | PFF hands/drop grade on screen concepts, 0-100. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `screen_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on screen concepts. |
| `slot_yards_per_reception` | numeric | Average yards per reception when aligned in the slot. |
| `screen_interceptions` | numeric | Interceptions thrown on passes targeting the player on screen concepts. |
| `slot_targets_percent` | numeric | Share of the team's targets thrown to the player when aligned in the slot. |
| `screen_longest` | numeric | Longest reception in yards on screen concepts. |
| `slot_avoided_tackles` | numeric | Tackles avoided after the catch when aligned in the slot. |
| `slot_yards_after_catch` | numeric | Yards gained after the catch when aligned in the slot. |
| `slot_grades_hands_drop` | numeric | PFF hands/drop grade when aligned in the slot, 0-100. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `screen_contested_targets` | numeric | PFF-charted contested targets on screen concepts. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `screen_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on screen concepts. |
| `screen_yards` | numeric | Receiving yards gained on screen concepts. |
| `slot_route_rate` | numeric | Share of pass-play snaps on which the player ran a route when aligned in the slot. |
| `screen_drop_rate` | numeric | Share of catchable targets the player dropped on screen concepts. |
| `screen_epa` | numeric | Total expected points added on targets to the player on screen concepts. |
| `screen_grades_pass_route` | numeric | PFF route-running (receiving) grade on screen concepts, 0-100. |
| `screen_drops` | numeric | PFF-charted drops on screen concepts. |
| `screen_fumbles` | numeric | Fumbles by the player after the catch on screen concepts. |
| `slot_interceptions` | numeric | Interceptions thrown on passes targeting the player when aligned in the slot. |
| `screen_yards_after_catch` | numeric | Yards gained after the catch on screen concepts. |
| `screen_avg_depth_of_target` | numeric | Average depth of target in yards downfield on screen concepts. |
| `screen_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on screen concepts. |
| `slot_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking when aligned in the slot. |
| `slot_yprr` | numeric | Yards per route run when aligned in the slot. |
| `base_targets` | numeric | Total targets from the facet's unsplit base row, across all concepts. |
| `slot_longest` | numeric | Longest reception in yards when aligned in the slot. |
| `slot_drops` | numeric | PFF-charted drops when aligned in the slot. |
| `screen_routes` | numeric | Pass routes run by the player on screen concepts. |
| `slot_fumbles` | numeric | Fumbles by the player after the catch when aligned in the slot. |
| `penalties` | numeric | Total number of penalties. |
| `slot_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught when aligned in the slot. |
| `slot_pass_plays` | numeric | Pass-play snaps when aligned in the slot. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `screen_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on screen concepts. |
| `slot_first_downs` | numeric | Receptions that converted a first down when aligned in the slot. |
| `screen_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on screen concepts. |
| `position` | character | Primary position as reported by NFL.com |
| `screen_pass_blocks` | numeric | Pass-play snaps spent pass blocking on screen concepts. |
| `slot_targets` | numeric | Pass targets to the player when aligned in the slot. |
| `slot_pass_blocks` | numeric | Pass-play snaps spent pass blocking when aligned in the slot. |
| `slot_receptions` | numeric | Receptions made when aligned in the slot. |
| `screen_first_downs` | numeric | Receptions that converted a first down on screen concepts. |
| `slot_caught_percent` | numeric | Percentage of targets caught when aligned in the slot. |
| `screen_avoided_tackles` | numeric | Tackles avoided after the catch on screen concepts. |
| `player` | character | Player name |
| `slot_epa` | numeric | Total expected points added on targets to the player when aligned in the slot. |
| `slot_drop_rate` | numeric | Share of catchable targets the player dropped when aligned in the slot. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `slot_touchdowns` | numeric | Receiving touchdowns scored when aligned in the slot. |
| `slot_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception when aligned in the slot. |
| `screen_receptions` | numeric | Receptions made on screen concepts. |
| `slot_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player when aligned in the slot. |
| `slot_contested_targets` | numeric | PFF-charted contested targets when aligned in the slot. |
| `screen_yards_per_reception` | numeric | Average yards per reception on screen concepts. |
| `slot_contested_receptions` | numeric | Catches made on PFF-charted contested targets when aligned in the slot. |
| `screen_pass_plays` | numeric | Pass-play snaps on screen concepts. |
| `screen_contested_receptions` | numeric | Catches made on PFF-charted contested targets on screen concepts. |
| `screen_touchdowns` | numeric | Receiving touchdowns scored on screen concepts. |
| `screen_targets` | numeric | Pass targets to the player on screen concepts. |
| `screen_targets_percent` | numeric | Share of the team's targets thrown to the player on screen concepts. |
| `slot_yards` | numeric | Receiving yards gained when aligned in the slot. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_receiving_concept()
```

_Last validated n/a._

## `pff_facet_special_teams_summary`

Facet report /special/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/special/summary`

**Valid URL:** [https://premium.pff.com/api/v1/facet/special/summary](https://premium.pff.com/api/v1/facet/special/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `assists` | numeric | Total assists. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_fgep_kicker` | numeric | PFF field-goal and extra-point kicking grade, 0-100. |
| `grades_kickoff_kicker` | numeric | PFF kickoff kicking grade, 0-100. |
| `grades_misc_st` | numeric | PFF miscellaneous special-teams grade, 0-100. |
| `grades_special_teams_penalty` | numeric | PFF special-teams penalty grade, 0-100. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `missed_tackles` | numeric | Missed tackles on special-teams plays. |
| `penalties` | numeric | Total number of penalties. |
| `player` | character | Player name |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `position` | character | Primary position as reported by NFL.com |
| `snap_counts_field_goal` | numeric | Snaps on the field-goal and extra-point unit. |
| `snap_counts_field_goal_blocking` | numeric | Snaps on the field-goal and extra-point block unit. |
| `snap_counts_kickoff` | numeric | Snaps on the kickoff coverage unit. |
| `snap_counts_kickoff_return` | numeric | Snaps on the kickoff return unit. |
| `snap_counts_punt_coverage` | numeric | Snaps on the punt coverage unit. |
| `snap_counts_punt_return` | numeric | Snaps on the punt return unit. |
| `tackles` | numeric | Team tackles. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `grades_fgep_defense` | numeric | PFF grade on field-goal and extra-point defense, 0-100. |
| `grades_fgep_offense` | numeric | PFF grade on the field-goal and extra-point protection unit, 0-100. |
| `grades_long_snap` | numeric | PFF long-snapping grade, 0-100. |
| `grades_punter` | numeric | PFF punting grade, 0-100. |
| `grades_kick_return` | numeric | PFF kickoff-return grade, 0-100. |
| `grades_punt_return` | numeric | PFF punt-return grade, 0-100. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_special_teams_summary()
```

_Last validated n/a._

## `pff_facet_receiving_depth`

Facet report /receiving/depth (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/receiving/depth`

**Valid URL:** [https://premium.pff.com/api/v1/facet/receiving/depth](https://premium.pff.com/api/v1/facet/receiving/depth)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `left_deep_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_short_first_downs` | numeric | Receptions that converted a first down on short passes (0-9 yards downfield) to the middle of the field. |
| `center_short_routes` | numeric | Pass routes run by the player on short passes (0-9 yards downfield) to the middle of the field. |
| `right_behind_los_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_behind_los_yards_after_catch` | numeric | Yards gained after the catch on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_behind_los_contested_targets` | numeric | PFF-charted contested targets on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_short_routes` | numeric | Pass routes run by the player on short passes (0-9 yards downfield) to the right third of the field. |
| `right_deep_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the right third of the field. |
| `medium_interceptions` | numeric | Interceptions thrown on passes targeting the player on medium passes (10-19 yards downfield). |
| `right_short_targets_percent` | numeric | Share of the team's targets thrown to the player on short passes (0-9 yards downfield) to the right third of the field. |
| `left_deep_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_medium_grades_hands_drop` | numeric | PFF hands/drop grade on medium passes (10-19 yards downfield) to the right third of the field, 0-100. |
| `left_short_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on short passes (0-9 yards downfield) to the left third of the field. |
| `right_medium_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on medium passes (10-19 yards downfield) to the right third of the field. |
| `short_targets` | numeric | Pass targets to the player on short passes (0-9 yards downfield). |
| `deep_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on deep passes (20 or more yards downfield). |
| `center_behind_los_drops` | numeric | PFF-charted drops on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_behind_los_interceptions` | numeric | Interceptions thrown on passes targeting the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `left_behind_los_interceptions` | numeric | Interceptions thrown on passes targeting the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `left_deep_first_downs` | numeric | Receptions that converted a first down on deep passes (20 or more yards downfield) to the left third of the field. |
| `left_behind_los_contested_targets` | numeric | PFF-charted contested targets on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_deep_avg_depth_of_target` | numeric | Average depth of target in yards downfield on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_short_drops` | numeric | PFF-charted drops on short passes (0-9 yards downfield) to the middle of the field. |
| `right_behind_los_targets` | numeric | Pass targets to the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_behind_los_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_deep_first_downs` | numeric | Receptions that converted a first down on deep passes (20 or more yards downfield) to the middle of the field. |
| `behind_los_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on passes thrown behind the line of scrimmage. |
| `right_deep_grades_pass_route` | numeric | PFF route-running (receiving) grade on deep passes (20 or more yards downfield) to the right third of the field, 0-100. |
| `left_short_fumbles` | numeric | Fumbles by the player after the catch on short passes (0-9 yards downfield) to the left third of the field. |
| `right_short_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on short passes (0-9 yards downfield) to the right third of the field. |
| `center_deep_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_behind_los_avoided_tackles` | numeric | Tackles avoided after the catch on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_short_longest` | numeric | Longest reception in yards on short passes (0-9 yards downfield) to the middle of the field. |
| `left_deep_drop_rate` | numeric | Share of catchable targets the player dropped on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_short_contested_receptions` | numeric | Catches made on PFF-charted contested targets on short passes (0-9 yards downfield) to the middle of the field. |
| `left_deep_routes` | numeric | Pass routes run by the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `deep_drops` | numeric | PFF-charted drops on deep passes (20 or more yards downfield). |
| `deep_contested_receptions` | numeric | Catches made on PFF-charted contested targets on deep passes (20 or more yards downfield). |
| `center_behind_los_touchdowns` | numeric | Receiving touchdowns scored on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_medium_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_short_yards_per_reception` | numeric | Average yards per reception on short passes (0-9 yards downfield) to the left third of the field. |
| `deep_touchdowns` | numeric | Receiving touchdowns scored on deep passes (20 or more yards downfield). |
| `left_short_yprr` | numeric | Yards per route run on short passes (0-9 yards downfield) to the left third of the field. |
| `center_deep_drop_rate` | numeric | Share of catchable targets the player dropped on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_behind_los_pass_plays` | numeric | Pass-play snaps on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_short_first_downs` | numeric | Receptions that converted a first down on short passes (0-9 yards downfield) to the right third of the field. |
| `right_behind_los_first_downs` | numeric | Receptions that converted a first down on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_behind_los_grades_pass_route` | numeric | PFF route-running (receiving) grade on passes thrown behind the line of scrimmage to the right third of the field, 0-100. |
| `short_interceptions` | numeric | Interceptions thrown on passes targeting the player on short passes (0-9 yards downfield). |
| `behind_los_routes` | numeric | Pass routes run by the player on passes thrown behind the line of scrimmage. |
| `right_behind_los_contested_targets` | numeric | PFF-charted contested targets on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_medium_grades_pass_route` | numeric | PFF route-running (receiving) grade on medium passes (10-19 yards downfield) to the left third of the field, 0-100. |
| `right_short_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on short passes (0-9 yards downfield) to the right third of the field. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `center_short_grades_hands_drop` | numeric | PFF hands/drop grade on short passes (0-9 yards downfield) to the middle of the field, 0-100. |
| `deep_receptions` | numeric | Receptions made on deep passes (20 or more yards downfield). |
| `center_deep_pass_blocks` | numeric | Pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_short_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on short passes (0-9 yards downfield) to the left third of the field. |
| `center_medium_avoided_tackles` | numeric | Tackles avoided after the catch on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_medium_grades_pass_route` | numeric | PFF route-running (receiving) grade on medium passes (10-19 yards downfield) to the middle of the field, 0-100. |
| `center_behind_los_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on passes thrown behind the line of scrimmage to the middle of the field. |
| `medium_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on medium passes (10-19 yards downfield). |
| `medium_receptions` | numeric | Receptions made on medium passes (10-19 yards downfield). |
| `deep_pass_blocks` | numeric | Pass-play snaps spent pass blocking on deep passes (20 or more yards downfield). |
| `right_deep_yards_per_reception` | numeric | Average yards per reception on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_medium_longest` | numeric | Longest reception in yards on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_deep_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on deep passes (20 or more yards downfield) to the middle of the field. |
| `medium_epa` | numeric | Total expected points added on targets to the player on medium passes (10-19 yards downfield). |
| `left_short_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on short passes (0-9 yards downfield) to the left third of the field. |
| `right_deep_grades_hands_drop` | numeric | PFF hands/drop grade on deep passes (20 or more yards downfield) to the right third of the field, 0-100. |
| `center_deep_targets` | numeric | Pass targets to the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `short_contested_receptions` | numeric | Catches made on PFF-charted contested targets on short passes (0-9 yards downfield). |
| `center_deep_pass_plays` | numeric | Pass-play snaps on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_medium_routes` | numeric | Pass routes run by the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `deep_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on deep passes (20 or more yards downfield). |
| `center_deep_contested_receptions` | numeric | Catches made on PFF-charted contested targets on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_short_pass_blocks` | numeric | Pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the middle of the field. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `deep_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on deep passes (20 or more yards downfield). |
| `short_touchdowns` | numeric | Receiving touchdowns scored on short passes (0-9 yards downfield). |
| `center_medium_drops` | numeric | PFF-charted drops on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_short_pass_blocks` | numeric | Pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the left third of the field. |
| `right_short_grades_pass_route` | numeric | PFF route-running (receiving) grade on short passes (0-9 yards downfield) to the right third of the field, 0-100. |
| `center_short_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on short passes (0-9 yards downfield) to the middle of the field. |
| `deep_avg_depth_of_target` | numeric | Average depth of target in yards downfield on deep passes (20 or more yards downfield). |
| `deep_grades_hands_drop` | numeric | PFF hands/drop grade on deep passes (20 or more yards downfield), 0-100. |
| `center_behind_los_avg_depth_of_target` | numeric | Average depth of target in yards downfield on passes thrown behind the line of scrimmage to the middle of the field. |
| `left_deep_caught_percent` | numeric | Percentage of targets caught on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_behind_los_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_medium_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on medium passes (10-19 yards downfield) to the middle of the field. |
| `short_grades_pass_route` | numeric | PFF route-running (receiving) grade on short passes (0-9 yards downfield), 0-100. |
| `center_behind_los_contested_receptions` | numeric | Catches made on PFF-charted contested targets on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_short_receptions` | numeric | Receptions made on short passes (0-9 yards downfield) to the middle of the field. |
| `center_short_yprr` | numeric | Yards per route run on short passes (0-9 yards downfield) to the middle of the field. |
| `medium_pass_blocks` | numeric | Pass-play snaps spent pass blocking on medium passes (10-19 yards downfield). |
| `medium_yards_per_reception` | numeric | Average yards per reception on medium passes (10-19 yards downfield). |
| `center_deep_yprr` | numeric | Yards per route run on deep passes (20 or more yards downfield) to the middle of the field. |
| `right_deep_drop_rate` | numeric | Share of catchable targets the player dropped on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_short_targets` | numeric | Pass targets to the player on short passes (0-9 yards downfield) to the middle of the field. |
| `center_medium_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_behind_los_drop_rate` | numeric | Share of catchable targets the player dropped on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_behind_los_targets_percent` | numeric | Share of the team's targets thrown to the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `short_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on short passes (0-9 yards downfield). |
| `behind_los_fumbles` | numeric | Fumbles by the player after the catch on passes thrown behind the line of scrimmage. |
| `left_medium_drop_rate` | numeric | Share of catchable targets the player dropped on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_medium_longest` | numeric | Longest reception in yards on medium passes (10-19 yards downfield) to the left third of the field. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `medium_yprr` | numeric | Yards per route run on medium passes (10-19 yards downfield). |
| `left_short_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on short passes (0-9 yards downfield) to the left third of the field. |
| `center_behind_los_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_short_fumbles` | numeric | Fumbles by the player after the catch on short passes (0-9 yards downfield) to the right third of the field. |
| `right_deep_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_short_pass_blocks` | numeric | Pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the right third of the field. |
| `left_short_longest` | numeric | Longest reception in yards on short passes (0-9 yards downfield) to the left third of the field. |
| `medium_contested_targets` | numeric | PFF-charted contested targets on medium passes (10-19 yards downfield). |
| `behind_los_interceptions` | numeric | Interceptions thrown on passes targeting the player on passes thrown behind the line of scrimmage. |
| `right_medium_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_deep_drops` | numeric | PFF-charted drops on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_short_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on short passes (0-9 yards downfield) to the right third of the field. |
| `medium_grades_pass_route` | numeric | PFF route-running (receiving) grade on medium passes (10-19 yards downfield), 0-100. |
| `right_deep_yards` | numeric | Receiving yards gained on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_medium_contested_targets` | numeric | PFF-charted contested targets on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_medium_targets` | numeric | Pass targets to the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `right_short_avg_depth_of_target` | numeric | Average depth of target in yards downfield on short passes (0-9 yards downfield) to the right third of the field. |
| `center_deep_longest` | numeric | Longest reception in yards on deep passes (20 or more yards downfield) to the middle of the field. |
| `right_behind_los_yards_per_reception` | numeric | Average yards per reception on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_medium_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_deep_yards_after_catch` | numeric | Yards gained after the catch on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_medium_receptions` | numeric | Receptions made on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_short_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on short passes (0-9 yards downfield) to the right third of the field. |
| `left_deep_targets_percent` | numeric | Share of the team's targets thrown to the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `behind_los_pass_blocks` | numeric | Pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage. |
| `right_behind_los_touchdowns` | numeric | Receiving touchdowns scored on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_medium_targets` | numeric | Pass targets to the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `deep_routes` | numeric | Pass routes run by the player on deep passes (20 or more yards downfield). |
| `left_medium_yprr` | numeric | Yards per route run on medium passes (10-19 yards downfield) to the left third of the field. |
| `deep_yards_per_reception` | numeric | Average yards per reception on deep passes (20 or more yards downfield). |
| `center_behind_los_avoided_tackles` | numeric | Tackles avoided after the catch on passes thrown behind the line of scrimmage to the middle of the field. |
| `left_deep_contested_targets` | numeric | PFF-charted contested targets on deep passes (20 or more yards downfield) to the left third of the field. |
| `medium_avg_depth_of_target` | numeric | Average depth of target in yards downfield on medium passes (10-19 yards downfield). |
| `short_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on short passes (0-9 yards downfield). |
| `left_behind_los_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on passes thrown behind the line of scrimmage to the left third of the field. |
| `left_medium_touchdowns` | numeric | Receiving touchdowns scored on medium passes (10-19 yards downfield) to the left third of the field. |
| `deep_targets_percent` | numeric | Share of the team's targets thrown to the player on deep passes (20 or more yards downfield). |
| `behind_los_targets` | numeric | Pass targets to the player on passes thrown behind the line of scrimmage. |
| `center_short_yards` | numeric | Receiving yards gained on short passes (0-9 yards downfield) to the middle of the field. |
| `center_medium_routes` | numeric | Pass routes run by the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_behind_los_grades_pass_route` | numeric | PFF route-running (receiving) grade on passes thrown behind the line of scrimmage to the middle of the field, 0-100. |
| `right_deep_yprr` | numeric | Yards per route run on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_deep_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_deep_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `deep_pass_plays` | numeric | Pass-play snaps on deep passes (20 or more yards downfield). |
| `center_short_avg_depth_of_target` | numeric | Average depth of target in yards downfield on short passes (0-9 yards downfield) to the middle of the field. |
| `right_short_yards` | numeric | Receiving yards gained on short passes (0-9 yards downfield) to the right third of the field. |
| `left_medium_first_downs` | numeric | Receptions that converted a first down on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_medium_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_medium_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_short_contested_receptions` | numeric | Catches made on PFF-charted contested targets on short passes (0-9 yards downfield) to the right third of the field. |
| `right_deep_avoided_tackles` | numeric | Tackles avoided after the catch on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_medium_fumbles` | numeric | Fumbles by the player after the catch on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_behind_los_fumbles` | numeric | Fumbles by the player after the catch on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_deep_drops` | numeric | PFF-charted drops on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_medium_avg_depth_of_target` | numeric | Average depth of target in yards downfield on medium passes (10-19 yards downfield) to the middle of the field. |
| `right_behind_los_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_short_yards_after_catch` | numeric | Yards gained after the catch on short passes (0-9 yards downfield) to the left third of the field. |
| `right_behind_los_pass_blocks` | numeric | Pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_behind_los_avg_depth_of_target` | numeric | Average depth of target in yards downfield on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_medium_epa` | numeric | Total expected points added on targets to the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_medium_targets_percent` | numeric | Share of the team's targets thrown to the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_behind_los_grades_pass_route` | numeric | PFF route-running (receiving) grade on passes thrown behind the line of scrimmage to the left third of the field, 0-100. |
| `center_behind_los_yards_per_reception` | numeric | Average yards per reception on passes thrown behind the line of scrimmage to the middle of the field. |
| `left_deep_targets` | numeric | Pass targets to the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `short_contested_targets` | numeric | PFF-charted contested targets on short passes (0-9 yards downfield). |
| `left_medium_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_deep_yprr` | numeric | Yards per route run on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_medium_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_medium_longest` | numeric | Longest reception in yards on medium passes (10-19 yards downfield) to the right third of the field. |
| `behind_los_drop_rate` | numeric | Share of catchable targets the player dropped on passes thrown behind the line of scrimmage. |
| `right_behind_los_epa` | numeric | Total expected points added on targets to the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `left_short_pass_plays` | numeric | Pass-play snaps on short passes (0-9 yards downfield) to the left third of the field. |
| `right_short_touchdowns` | numeric | Receiving touchdowns scored on short passes (0-9 yards downfield) to the right third of the field. |
| `behind_los_first_downs` | numeric | Receptions that converted a first down on passes thrown behind the line of scrimmage. |
| `right_deep_receptions` | numeric | Receptions made on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_medium_pass_plays` | numeric | Pass-play snaps on medium passes (10-19 yards downfield) to the left third of the field. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `center_behind_los_yards` | numeric | Receiving yards gained on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_medium_pass_blocks` | numeric | Pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_short_grades_pass_route` | numeric | PFF route-running (receiving) grade on short passes (0-9 yards downfield) to the left third of the field, 0-100. |
| `left_behind_los_targets_percent` | numeric | Share of the team's targets thrown to the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `behind_los_longest` | numeric | Longest reception in yards on passes thrown behind the line of scrimmage. |
| `right_medium_first_downs` | numeric | Receptions that converted a first down on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_deep_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on deep passes (20 or more yards downfield) to the middle of the field. |
| `deep_contested_targets` | numeric | PFF-charted contested targets on deep passes (20 or more yards downfield). |
| `behind_los_avoided_tackles` | numeric | Tackles avoided after the catch on passes thrown behind the line of scrimmage. |
| `left_behind_los_receptions` | numeric | Receptions made on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_short_yards_after_catch` | numeric | Yards gained after the catch on short passes (0-9 yards downfield) to the right third of the field. |
| `right_behind_los_yards_after_catch` | numeric | Yards gained after the catch on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_short_pass_plays` | numeric | Pass-play snaps on short passes (0-9 yards downfield) to the right third of the field. |
| `right_deep_epa` | numeric | Total expected points added on targets to the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_short_caught_percent` | numeric | Percentage of targets caught on short passes (0-9 yards downfield) to the left third of the field. |
| `center_behind_los_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_short_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on short passes (0-9 yards downfield) to the right third of the field. |
| `right_short_drop_rate` | numeric | Share of catchable targets the player dropped on short passes (0-9 yards downfield) to the right third of the field. |
| `left_short_yards` | numeric | Receiving yards gained on short passes (0-9 yards downfield) to the left third of the field. |
| `right_behind_los_avg_depth_of_target` | numeric | Average depth of target in yards downfield on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_deep_first_downs` | numeric | Receptions that converted a first down on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_deep_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on deep passes (20 or more yards downfield) to the left third of the field. |
| `short_yards_after_catch` | numeric | Yards gained after the catch on short passes (0-9 yards downfield). |
| `center_medium_yprr` | numeric | Yards per route run on medium passes (10-19 yards downfield) to the middle of the field. |
| `short_pass_blocks` | numeric | Pass-play snaps spent pass blocking on short passes (0-9 yards downfield). |
| `left_short_contested_receptions` | numeric | Catches made on PFF-charted contested targets on short passes (0-9 yards downfield) to the left third of the field. |
| `short_fumbles` | numeric | Fumbles by the player after the catch on short passes (0-9 yards downfield). |
| `left_short_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the left third of the field. |
| `center_deep_routes` | numeric | Pass routes run by the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_medium_avoided_tackles` | numeric | Tackles avoided after the catch on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_behind_los_pass_plays` | numeric | Pass-play snaps on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_deep_touchdowns` | numeric | Receiving touchdowns scored on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_medium_receptions` | numeric | Receptions made on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_behind_los_drops` | numeric | PFF-charted drops on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_short_targets_percent` | numeric | Share of the team's targets thrown to the player on short passes (0-9 yards downfield) to the middle of the field. |
| `deep_epa` | numeric | Total expected points added on targets to the player on deep passes (20 or more yards downfield). |
| `medium_touchdowns` | numeric | Receiving touchdowns scored on medium passes (10-19 yards downfield). |
| `left_medium_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_behind_los_grades_hands_drop` | numeric | PFF hands/drop grade on passes thrown behind the line of scrimmage to the left third of the field, 0-100. |
| `left_deep_fumbles` | numeric | Fumbles by the player after the catch on deep passes (20 or more yards downfield) to the left third of the field. |
| `medium_avoided_tackles` | numeric | Tackles avoided after the catch on medium passes (10-19 yards downfield). |
| `center_medium_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on medium passes (10-19 yards downfield) to the middle of the field. |
| `right_behind_los_routes` | numeric | Pass routes run by the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `short_targets_percent` | numeric | Share of the team's targets thrown to the player on short passes (0-9 yards downfield). |
| `left_behind_los_yprr` | numeric | Yards per route run on passes thrown behind the line of scrimmage to the left third of the field. |
| `medium_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on medium passes (10-19 yards downfield). |
| `left_short_first_downs` | numeric | Receptions that converted a first down on short passes (0-9 yards downfield) to the left third of the field. |
| `short_longest` | numeric | Longest reception in yards on short passes (0-9 yards downfield). |
| `right_behind_los_grades_hands_drop` | numeric | PFF hands/drop grade on passes thrown behind the line of scrimmage to the right third of the field, 0-100. |
| `behind_los_contested_targets` | numeric | PFF-charted contested targets on passes thrown behind the line of scrimmage. |
| `right_behind_los_avoided_tackles` | numeric | Tackles avoided after the catch on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_deep_longest` | numeric | Longest reception in yards on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_deep_contested_targets` | numeric | PFF-charted contested targets on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_deep_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_behind_los_pass_plays` | numeric | Pass-play snaps on passes thrown behind the line of scrimmage to the right third of the field. |
| `medium_contested_receptions` | numeric | Catches made on PFF-charted contested targets on medium passes (10-19 yards downfield). |
| `short_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on short passes (0-9 yards downfield). |
| `right_behind_los_interceptions` | numeric | Interceptions thrown on passes targeting the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `behind_los_targets_percent` | numeric | Share of the team's targets thrown to the player on passes thrown behind the line of scrimmage. |
| `center_deep_yards_after_catch` | numeric | Yards gained after the catch on deep passes (20 or more yards downfield) to the middle of the field. |
| `behind_los_yards` | numeric | Receiving yards gained on passes thrown behind the line of scrimmage. |
| `right_deep_targets_percent` | numeric | Share of the team's targets thrown to the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_medium_fumbles` | numeric | Fumbles by the player after the catch on medium passes (10-19 yards downfield) to the left third of the field. |
| `short_receptions` | numeric | Receptions made on short passes (0-9 yards downfield). |
| `left_short_interceptions` | numeric | Interceptions thrown on passes targeting the player on short passes (0-9 yards downfield) to the left third of the field. |
| `right_medium_interceptions` | numeric | Interceptions thrown on passes targeting the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_behind_los_yards` | numeric | Receiving yards gained on passes thrown behind the line of scrimmage to the left third of the field. |
| `medium_first_downs` | numeric | Receptions that converted a first down on medium passes (10-19 yards downfield). |
| `left_short_avg_depth_of_target` | numeric | Average depth of target in yards downfield on short passes (0-9 yards downfield) to the left third of the field. |
| `right_medium_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_medium_targets_percent` | numeric | Share of the team's targets thrown to the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_behind_los_touchdowns` | numeric | Receiving touchdowns scored on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_behind_los_drop_rate` | numeric | Share of catchable targets the player dropped on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_deep_fumbles` | numeric | Fumbles by the player after the catch on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_deep_contested_receptions` | numeric | Catches made on PFF-charted contested targets on deep passes (20 or more yards downfield) to the left third of the field. |
| `behind_los_drops` | numeric | PFF-charted drops on passes thrown behind the line of scrimmage. |
| `short_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on short passes (0-9 yards downfield). |
| `medium_pass_plays` | numeric | Pass-play snaps on medium passes (10-19 yards downfield). |
| `behind_los_pass_plays` | numeric | Pass-play snaps on passes thrown behind the line of scrimmage. |
| `left_deep_yards` | numeric | Receiving yards gained on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_behind_los_longest` | numeric | Longest reception in yards on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_medium_grades_hands_drop` | numeric | PFF hands/drop grade on medium passes (10-19 yards downfield) to the middle of the field, 0-100. |
| `left_behind_los_longest` | numeric | Longest reception in yards on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_short_interceptions` | numeric | Interceptions thrown on passes targeting the player on short passes (0-9 yards downfield) to the middle of the field. |
| `short_avg_depth_of_target` | numeric | Average depth of target in yards downfield on short passes (0-9 yards downfield). |
| `deep_yprr` | numeric | Yards per route run on deep passes (20 or more yards downfield). |
| `left_deep_touchdowns` | numeric | Receiving touchdowns scored on deep passes (20 or more yards downfield) to the left third of the field. |
| `base_targets` | numeric | Total targets from the facet's unsplit base row, across all depths and directions. |
| `deep_yards` | numeric | Receiving yards gained on deep passes (20 or more yards downfield). |
| `center_medium_yards_after_catch` | numeric | Yards gained after the catch on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_medium_epa` | numeric | Total expected points added on targets to the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_deep_avg_depth_of_target` | numeric | Average depth of target in yards downfield on deep passes (20 or more yards downfield) to the left third of the field. |
| `deep_first_downs` | numeric | Receptions that converted a first down on deep passes (20 or more yards downfield). |
| `short_drop_rate` | numeric | Share of catchable targets the player dropped on short passes (0-9 yards downfield). |
| `left_medium_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on medium passes (10-19 yards downfield) to the left third of the field. |
| `center_deep_touchdowns` | numeric | Receiving touchdowns scored on deep passes (20 or more yards downfield) to the middle of the field. |
| `right_behind_los_yprr` | numeric | Yards per route run on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_medium_pass_blocks` | numeric | Pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the middle of the field. |
| `short_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on short passes (0-9 yards downfield). |
| `medium_fumbles` | numeric | Fumbles by the player after the catch on medium passes (10-19 yards downfield). |
| `center_medium_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_short_routes` | numeric | Pass routes run by the player on short passes (0-9 yards downfield) to the left third of the field. |
| `behind_los_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on passes thrown behind the line of scrimmage. |
| `behind_los_epa` | numeric | Total expected points added on targets to the player on passes thrown behind the line of scrimmage. |
| `right_behind_los_contested_receptions` | numeric | Catches made on PFF-charted contested targets on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_medium_yards_after_catch` | numeric | Yards gained after the catch on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_short_touchdowns` | numeric | Receiving touchdowns scored on short passes (0-9 yards downfield) to the middle of the field. |
| `right_medium_drops` | numeric | PFF-charted drops on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_behind_los_routes` | numeric | Pass routes run by the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `left_deep_epa` | numeric | Total expected points added on targets to the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `left_short_targets` | numeric | Pass targets to the player on short passes (0-9 yards downfield) to the left third of the field. |
| `left_deep_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_deep_caught_percent` | numeric | Percentage of targets caught on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_short_grades_pass_route` | numeric | PFF route-running (receiving) grade on short passes (0-9 yards downfield) to the middle of the field, 0-100. |
| `left_behind_los_fumbles` | numeric | Fumbles by the player after the catch on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_medium_touchdowns` | numeric | Receiving touchdowns scored on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_short_fumbles` | numeric | Fumbles by the player after the catch on short passes (0-9 yards downfield) to the middle of the field. |
| `center_behind_los_routes` | numeric | Pass routes run by the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `behind_los_receptions` | numeric | Receptions made on passes thrown behind the line of scrimmage. |
| `medium_targets_percent` | numeric | Share of the team's targets thrown to the player on medium passes (10-19 yards downfield). |
| `left_medium_yards` | numeric | Receiving yards gained on medium passes (10-19 yards downfield) to the left third of the field. |
| `center_medium_touchdowns` | numeric | Receiving touchdowns scored on medium passes (10-19 yards downfield) to the middle of the field. |
| `medium_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on medium passes (10-19 yards downfield). |
| `center_short_drop_rate` | numeric | Share of catchable targets the player dropped on short passes (0-9 yards downfield) to the middle of the field. |
| `penalties` | numeric | Total number of penalties. |
| `short_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on short passes (0-9 yards downfield). |
| `right_deep_avg_depth_of_target` | numeric | Average depth of target in yards downfield on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_deep_yards` | numeric | Receiving yards gained on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_short_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on short passes (0-9 yards downfield) to the middle of the field. |
| `right_short_targets` | numeric | Pass targets to the player on short passes (0-9 yards downfield) to the right third of the field. |
| `behind_los_avg_depth_of_target` | numeric | Average depth of target in yards downfield on passes thrown behind the line of scrimmage. |
| `left_behind_los_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_short_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on short passes (0-9 yards downfield) to the middle of the field. |
| `left_behind_los_epa` | numeric | Total expected points added on targets to the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `deep_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on deep passes (20 or more yards downfield). |
| `short_yprr` | numeric | Yards per route run on short passes (0-9 yards downfield). |
| `deep_caught_percent` | numeric | Percentage of targets caught on deep passes (20 or more yards downfield). |
| `center_medium_drop_rate` | numeric | Share of catchable targets the player dropped on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_behind_los_yards_after_catch` | numeric | Yards gained after the catch on passes thrown behind the line of scrimmage to the middle of the field. |
| `medium_yards` | numeric | Receiving yards gained on medium passes (10-19 yards downfield). |
| `center_deep_avoided_tackles` | numeric | Tackles avoided after the catch on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_short_contested_targets` | numeric | PFF-charted contested targets on short passes (0-9 yards downfield) to the left third of the field. |
| `center_behind_los_first_downs` | numeric | Receptions that converted a first down on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_medium_interceptions` | numeric | Interceptions thrown on passes targeting the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_deep_yards_per_reception` | numeric | Average yards per reception on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_deep_contested_targets` | numeric | PFF-charted contested targets on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_short_targets_percent` | numeric | Share of the team's targets thrown to the player on short passes (0-9 yards downfield) to the left third of the field. |
| `short_grades_hands_drop` | numeric | PFF hands/drop grade on short passes (0-9 yards downfield), 0-100. |
| `right_deep_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_medium_contested_receptions` | numeric | Catches made on PFF-charted contested targets on medium passes (10-19 yards downfield) to the left third of the field. |
| `center_short_yards_per_reception` | numeric | Average yards per reception on short passes (0-9 yards downfield) to the middle of the field. |
| `center_deep_grades_hands_drop` | numeric | PFF hands/drop grade on deep passes (20 or more yards downfield) to the middle of the field, 0-100. |
| `deep_longest` | numeric | Longest reception in yards on deep passes (20 or more yards downfield). |
| `left_medium_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_short_interceptions` | numeric | Interceptions thrown on passes targeting the player on short passes (0-9 yards downfield) to the right third of the field. |
| `center_short_caught_percent` | numeric | Percentage of targets caught on short passes (0-9 yards downfield) to the middle of the field. |
| `left_deep_interceptions` | numeric | Interceptions thrown on passes targeting the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `short_yards_per_reception` | numeric | Average yards per reception on short passes (0-9 yards downfield). |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `center_short_avoided_tackles` | numeric | Tackles avoided after the catch on short passes (0-9 yards downfield) to the middle of the field. |
| `behind_los_yards_per_reception` | numeric | Average yards per reception on passes thrown behind the line of scrimmage. |
| `short_epa` | numeric | Total expected points added on targets to the player on short passes (0-9 yards downfield). |
| `deep_drop_rate` | numeric | Share of catchable targets the player dropped on deep passes (20 or more yards downfield). |
| `left_deep_yards_per_reception` | numeric | Average yards per reception on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_short_yprr` | numeric | Yards per route run on short passes (0-9 yards downfield) to the right third of the field. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `medium_longest` | numeric | Longest reception in yards on medium passes (10-19 yards downfield). |
| `left_short_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on short passes (0-9 yards downfield) to the left third of the field. |
| `right_medium_routes` | numeric | Pass routes run by the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_short_receptions` | numeric | Receptions made on short passes (0-9 yards downfield) to the left third of the field. |
| `left_deep_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on deep passes (20 or more yards downfield) to the left third of the field. |
| `deep_targets` | numeric | Pass targets to the player on deep passes (20 or more yards downfield). |
| `left_short_grades_hands_drop` | numeric | PFF hands/drop grade on short passes (0-9 yards downfield) to the left third of the field, 0-100. |
| `right_short_epa` | numeric | Total expected points added on targets to the player on short passes (0-9 yards downfield) to the right third of the field. |
| `left_behind_los_contested_receptions` | numeric | Catches made on PFF-charted contested targets on passes thrown behind the line of scrimmage to the left third of the field. |
| `medium_targets` | numeric | Pass targets to the player on medium passes (10-19 yards downfield). |
| `behind_los_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on passes thrown behind the line of scrimmage. |
| `center_short_contested_targets` | numeric | PFF-charted contested targets on short passes (0-9 yards downfield) to the middle of the field. |
| `center_short_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on short passes (0-9 yards downfield) to the middle of the field. |
| `right_medium_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_short_epa` | numeric | Total expected points added on targets to the player on short passes (0-9 yards downfield) to the left third of the field. |
| `left_deep_avoided_tackles` | numeric | Tackles avoided after the catch on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_behind_los_pass_blocks` | numeric | Pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the middle of the field. |
| `deep_interceptions` | numeric | Interceptions thrown on passes targeting the player on deep passes (20 or more yards downfield). |
| `right_short_caught_percent` | numeric | Percentage of targets caught on short passes (0-9 yards downfield) to the right third of the field. |
| `center_deep_caught_percent` | numeric | Percentage of targets caught on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_medium_fumbles` | numeric | Fumbles by the player after the catch on medium passes (10-19 yards downfield) to the middle of the field. |
| `behind_los_grades_hands_drop` | numeric | PFF hands/drop grade on passes thrown behind the line of scrimmage, 0-100. |
| `left_behind_los_targets` | numeric | Pass targets to the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_medium_yprr` | numeric | Yards per route run on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_short_receptions` | numeric | Receptions made on short passes (0-9 yards downfield) to the right third of the field. |
| `position` | character | Primary position as reported by NFL.com |
| `right_short_contested_targets` | numeric | PFF-charted contested targets on short passes (0-9 yards downfield) to the right third of the field. |
| `center_medium_yards_per_reception` | numeric | Average yards per reception on medium passes (10-19 yards downfield) to the middle of the field. |
| `right_behind_los_yards` | numeric | Receiving yards gained on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_behind_los_grades_hands_drop` | numeric | PFF hands/drop grade on passes thrown behind the line of scrimmage to the middle of the field, 0-100. |
| `right_behind_los_receptions` | numeric | Receptions made on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_medium_caught_percent` | numeric | Percentage of targets caught on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_medium_drops` | numeric | PFF-charted drops on medium passes (10-19 yards downfield) to the left third of the field. |
| `short_avoided_tackles` | numeric | Tackles avoided after the catch on short passes (0-9 yards downfield). |
| `center_short_epa` | numeric | Total expected points added on targets to the player on short passes (0-9 yards downfield) to the middle of the field. |
| `center_behind_los_targets` | numeric | Pass targets to the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `left_deep_pass_blocks` | numeric | Pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the left third of the field. |
| `left_short_drops` | numeric | PFF-charted drops on short passes (0-9 yards downfield) to the left third of the field. |
| `medium_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on medium passes (10-19 yards downfield). |
| `center_deep_receptions` | numeric | Receptions made on deep passes (20 or more yards downfield) to the middle of the field. |
| `right_medium_epa` | numeric | Total expected points added on targets to the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_short_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the right third of the field. |
| `center_deep_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_short_pass_plays` | numeric | Pass-play snaps on short passes (0-9 yards downfield) to the middle of the field. |
| `center_deep_interceptions` | numeric | Interceptions thrown on passes targeting the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_medium_yards_after_catch` | numeric | Yards gained after the catch on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_behind_los_pass_blocks` | numeric | Pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_behind_los_longest` | numeric | Longest reception in yards on passes thrown behind the line of scrimmage to the right third of the field. |
| `short_yards` | numeric | Receiving yards gained on short passes (0-9 yards downfield). |
| `left_deep_receptions` | numeric | Receptions made on deep passes (20 or more yards downfield) to the left third of the field. |
| `deep_grades_pass_route` | numeric | PFF route-running (receiving) grade on deep passes (20 or more yards downfield), 0-100. |
| `left_medium_targets` | numeric | Pass targets to the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `behind_los_grades_pass_route` | numeric | PFF route-running (receiving) grade on passes thrown behind the line of scrimmage, 0-100. |
| `right_deep_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `behind_los_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage. |
| `left_medium_pass_blocks` | numeric | Pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the left third of the field. |
| `player` | character | Player name |
| `left_medium_avg_depth_of_target` | numeric | Average depth of target in yards downfield on medium passes (10-19 yards downfield) to the left third of the field. |
| `deep_avoided_tackles` | numeric | Tackles avoided after the catch on deep passes (20 or more yards downfield). |
| `center_behind_los_yprr` | numeric | Yards per route run on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_behind_los_epa` | numeric | Total expected points added on targets to the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_behind_los_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_deep_fumbles` | numeric | Fumbles by the player after the catch on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_medium_grades_hands_drop` | numeric | PFF hands/drop grade on medium passes (10-19 yards downfield) to the left third of the field, 0-100. |
| `right_medium_yards` | numeric | Receiving yards gained on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_deep_yards_after_catch` | numeric | Yards gained after the catch on deep passes (20 or more yards downfield) to the left third of the field. |
| `left_deep_pass_plays` | numeric | Pass-play snaps on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_short_yards_after_catch` | numeric | Yards gained after the catch on short passes (0-9 yards downfield) to the middle of the field. |
| `center_behind_los_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_medium_drop_rate` | numeric | Share of catchable targets the player dropped on medium passes (10-19 yards downfield) to the right third of the field. |
| `medium_grades_hands_drop` | numeric | PFF hands/drop grade on medium passes (10-19 yards downfield), 0-100. |
| `right_deep_pass_blocks` | numeric | Pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the right third of the field. |
| `medium_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on medium passes (10-19 yards downfield). |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `center_behind_los_caught_percent` | numeric | Percentage of targets caught on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_behind_los_caught_percent` | numeric | Percentage of targets caught on passes thrown behind the line of scrimmage to the right third of the field. |
| `short_pass_plays` | numeric | Pass-play snaps on short passes (0-9 yards downfield). |
| `center_behind_los_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `deep_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on deep passes (20 or more yards downfield). |
| `center_short_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the middle of the field. |
| `right_short_avoided_tackles` | numeric | Tackles avoided after the catch on short passes (0-9 yards downfield) to the right third of the field. |
| `left_short_drop_rate` | numeric | Share of catchable targets the player dropped on short passes (0-9 yards downfield) to the left third of the field. |
| `left_medium_yards_per_reception` | numeric | Average yards per reception on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_medium_receptions` | numeric | Receptions made on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_medium_pass_plays` | numeric | Pass-play snaps on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_medium_contested_targets` | numeric | PFF-charted contested targets on medium passes (10-19 yards downfield) to the middle of the field. |
| `short_first_downs` | numeric | Receptions that converted a first down on short passes (0-9 yards downfield). |
| `center_medium_targets_percent` | numeric | Share of the team's targets thrown to the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `right_short_longest` | numeric | Longest reception in yards on short passes (0-9 yards downfield) to the right third of the field. |
| `left_deep_longest` | numeric | Longest reception in yards on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_short_drops` | numeric | PFF-charted drops on short passes (0-9 yards downfield) to the right third of the field. |
| `right_medium_contested_receptions` | numeric | Catches made on PFF-charted contested targets on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_deep_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_behind_los_receptions` | numeric | Receptions made on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_medium_caught_percent` | numeric | Percentage of targets caught on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_deep_targets` | numeric | Pass targets to the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_medium_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on medium passes (10-19 yards downfield) to the middle of the field. |
| `medium_drops` | numeric | PFF-charted drops on medium passes (10-19 yards downfield). |
| `left_deep_grades_hands_drop` | numeric | PFF hands/drop grade on deep passes (20 or more yards downfield) to the left third of the field, 0-100. |
| `behind_los_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on passes thrown behind the line of scrimmage. |
| `left_short_avoided_tackles` | numeric | Tackles avoided after the catch on short passes (0-9 yards downfield) to the left third of the field. |
| `right_medium_grades_pass_route` | numeric | PFF route-running (receiving) grade on medium passes (10-19 yards downfield) to the right third of the field, 0-100. |
| `right_short_grades_hands_drop` | numeric | PFF hands/drop grade on short passes (0-9 yards downfield) to the right third of the field, 0-100. |
| `left_behind_los_yards_per_reception` | numeric | Average yards per reception on passes thrown behind the line of scrimmage to the left third of the field. |
| `medium_yards_after_catch` | numeric | Yards gained after the catch on medium passes (10-19 yards downfield). |
| `center_medium_pass_plays` | numeric | Pass-play snaps on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_behind_los_caught_percent` | numeric | Percentage of targets caught on passes thrown behind the line of scrimmage to the left third of the field. |
| `left_medium_contested_targets` | numeric | PFF-charted contested targets on medium passes (10-19 yards downfield) to the left third of the field. |
| `center_medium_contested_receptions` | numeric | Catches made on PFF-charted contested targets on medium passes (10-19 yards downfield) to the middle of the field. |
| `behind_los_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on passes thrown behind the line of scrimmage. |
| `behind_los_yards_after_catch` | numeric | Yards gained after the catch on passes thrown behind the line of scrimmage. |
| `left_deep_drops` | numeric | PFF-charted drops on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_deep_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_behind_los_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_deep_routes` | numeric | Pass routes run by the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_deep_grades_pass_route` | numeric | PFF route-running (receiving) grade on deep passes (20 or more yards downfield) to the middle of the field, 0-100. |
| `deep_yards_after_catch` | numeric | Yards gained after the catch on deep passes (20 or more yards downfield). |
| `center_behind_los_drop_rate` | numeric | Share of catchable targets the player dropped on passes thrown behind the line of scrimmage to the middle of the field. |
| `short_drops` | numeric | PFF-charted drops on short passes (0-9 yards downfield). |
| `right_deep_contested_receptions` | numeric | Catches made on PFF-charted contested targets on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_behind_los_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_medium_avg_depth_of_target` | numeric | Average depth of target in yards downfield on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_medium_avoided_tackles` | numeric | Tackles avoided after the catch on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_medium_caught_percent` | numeric | Percentage of targets caught on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_behind_los_fumbles` | numeric | Fumbles by the player after the catch on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_medium_first_downs` | numeric | Receptions that converted a first down on medium passes (10-19 yards downfield) to the middle of the field. |
| `behind_los_caught_percent` | numeric | Percentage of targets caught on passes thrown behind the line of scrimmage. |
| `short_routes` | numeric | Pass routes run by the player on short passes (0-9 yards downfield). |
| `left_deep_grades_pass_route` | numeric | PFF route-running (receiving) grade on deep passes (20 or more yards downfield) to the left third of the field, 0-100. |
| `medium_routes` | numeric | Pass routes run by the player on medium passes (10-19 yards downfield). |
| `medium_caught_percent` | numeric | Percentage of targets caught on medium passes (10-19 yards downfield). |
| `behind_los_contested_receptions` | numeric | Catches made on PFF-charted contested targets on passes thrown behind the line of scrimmage. |
| `behind_los_yprr` | numeric | Yards per route run on passes thrown behind the line of scrimmage. |
| `left_short_touchdowns` | numeric | Receiving touchdowns scored on short passes (0-9 yards downfield) to the left third of the field. |
| `medium_drop_rate` | numeric | Share of catchable targets the player dropped on medium passes (10-19 yards downfield). |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `left_behind_los_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_deep_pass_plays` | numeric | Pass-play snaps on deep passes (20 or more yards downfield) to the right third of the field. |
| `short_caught_percent` | numeric | Percentage of targets caught on short passes (0-9 yards downfield). |
| `right_medium_yards_per_reception` | numeric | Average yards per reception on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_medium_yards` | numeric | Receiving yards gained on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_deep_epa` | numeric | Total expected points added on targets to the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_medium_interceptions` | numeric | Interceptions thrown on passes targeting the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_deep_interceptions` | numeric | Interceptions thrown on passes targeting the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `deep_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on deep passes (20 or more yards downfield). |
| `deep_fumbles` | numeric | Fumbles by the player after the catch on deep passes (20 or more yards downfield). |
| `center_short_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on short passes (0-9 yards downfield) to the middle of the field. |
| `center_behind_los_targets_percent` | numeric | Share of the team's targets thrown to the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_short_yards_per_reception` | numeric | Average yards per reception on short passes (0-9 yards downfield) to the right third of the field. |
| `center_deep_targets_percent` | numeric | Share of the team's targets thrown to the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_behind_los_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on passes thrown behind the line of scrimmage to the middle of the field. |
| `behind_los_touchdowns` | numeric | Receiving touchdowns scored on passes thrown behind the line of scrimmage. |
| `medium_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on medium passes (10-19 yards downfield). |
| `left_behind_los_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_behind_los_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_behind_los_drops` | numeric | PFF-charted drops on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_behind_los_first_downs` | numeric | Receptions that converted a first down on passes thrown behind the line of scrimmage to the left third of the field. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_receiving_depth()
```

_Last validated n/a._

## `pff_facet_receiving_coverage`

Facet report /receiving/coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/receiving/coverage`

**Valid URL:** [https://premium.pff.com/api/v1/facet/receiving/coverage](https://premium.pff.com/api/v1/facet/receiving/coverage)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_report`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_facet_receiving_coverage()
```

_Last validated n/a._

## `pff_player_passing_summary`

Player-detail report /passing/summary (per-week + totals for one player)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/player/passing/summary`

**Valid URL:** [https://premium.pff.com/api/v1/player/passing/summary](https://premium.pff.com/api/v1/player/passing/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `player_id` |  |  | `Y` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `career` |  |  | `Y` | Career-rollup flag ("true"/"false"); player-detail views only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_player_passing_summary()
```

_Last validated n/a._

## `pff_player_rushing_summary`

Player-detail report /rushing/summary (per-week + totals for one player)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/player/rushing/summary`

**Valid URL:** [https://premium.pff.com/api/v1/player/rushing/summary](https://premium.pff.com/api/v1/player/rushing/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `player_id` |  |  | `Y` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `career` |  |  | `Y` | Career-rollup flag ("true"/"false"); player-detail views only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_player_rushing_summary()
```

_Last validated n/a._

## `pff_player_receiving_summary`

Player-detail report /receiving/summary (per-week + totals for one player)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/player/receiving/summary`

**Valid URL:** [https://premium.pff.com/api/v1/player/receiving/summary](https://premium.pff.com/api/v1/player/receiving/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `player_id` |  |  | `Y` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `career` |  |  | `Y` | Career-rollup flag ("true"/"false"); player-detail views only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_player_receiving_summary()
```

_Last validated n/a._

## `pff_player_defense_summary`

Player-detail report /defense/summary (per-week + totals for one player)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/player/defense/summary`

**Valid URL:** [https://premium.pff.com/api/v1/player/defense/summary](https://premium.pff.com/api/v1/player/defense/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `player_id` |  |  | `Y` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `career` |  |  | `Y` | Career-rollup flag ("true"/"false"); player-detail views only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_player_defense_summary()
```

_Last validated n/a._

## `pff_player_offense_summary`

Player-detail report /offense/summary (per-week + totals for one player)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/player/offense/summary`

**Valid URL:** [https://premium.pff.com/api/v1/player/offense/summary](https://premium.pff.com/api/v1/player/offense/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `player_id` |  |  | `Y` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `career` |  |  | `Y` | Career-rollup flag ("true"/"false"); player-detail views only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_player_offense_summary()
```

_Last validated n/a._

## `pff_player_snaps_summary`

Player-detail report /snaps/summary (per-week + totals for one player)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/player/snaps/summary`

**Valid URL:** [https://premium.pff.com/api/v1/player/snaps/summary](https://premium.pff.com/api/v1/player/snaps/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `player_id` |  |  | `Y` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `career` |  |  | `Y` | Career-rollup flag ("true"/"false"); player-detail views only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_player_snaps_summary()
```

_Last validated n/a._

## `pff_player_offense_blocking`

Player-detail report /offense/blocking (per-week + totals for one player)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/player/offense/blocking`

**Valid URL:** [https://premium.pff.com/api/v1/player/offense/blocking](https://premium.pff.com/api/v1/player/offense/blocking)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `player_id` |  |  | `Y` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `career` |  |  | `Y` | Career-rollup flag ("true"/"false"); player-detail views only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_player_offense_blocking()
```

_Last validated n/a._

## `pff_leagues`

Leagues + seasons + week groups (bootstrap)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/leagues`

**Valid URL:** [https://premium.pff.com/api/v1/leagues](https://premium.pff.com/api/v1/leagues)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `abbreviation` | character | Metric abbreviation. |
| `default_season` | numeric | Season the source API currently treats as the default for this league. |
| `default_week` | numeric | Week number the source API currently treats as the default for this league. |
| `default_week_group` | character | Identifier of the week grouping (e.g., regular season or postseason phase) currently set as the league default. |
| `id` | numeric | ID of the player in the 'name' column. |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `seasons` | list | NBA seasons played. |
| `slug` | character | URL slug for the team. |
| `week_groups` | list | Nested list of week-group objects (phase label and week span) defined for the league. |
| `weeks` | list | Nested list of week objects available for the league. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_leagues()
```

_Last validated n/a._

## `pff_teams`

Teams / franchise groups + games for a league-season

**Endpoint URL:** `GET https://premium.pff.com/api/v1/teams`

**Valid URL:** [https://premium.pff.com/api/v1/teams](https://premium.pff.com/api/v1/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `heirarchy` | list | Nested hierarchy of franchise groupings as returned by the PFF API; the field name's spelling follows the source. |
| `id` | numeric | ID of the player in the 'name' column. |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `slug` | character | URL slug for the team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_teams()
```

_Last validated n/a._

## `pff_teams_overview`

Team overview table (By Team landing)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/teams/overview`

**Valid URL:** [https://premium.pff.com/api/v1/teams/overview](https://premium.pff.com/api/v1/teams/overview)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `abbreviation` | character | Team abbreviation. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `grades_coverage_defense` | numeric | PFF team coverage grade (0-100). |
| `grades_defense` | numeric | PFF team defense grade (0-100). |
| `grades_misc_st` | numeric | Team-level PFF miscellaneous special-teams grade, 0-100. |
| `grades_offense` | numeric | PFF team offense grade (0-100). |
| `grades_overall` | numeric | Team-level PFF overall grade, 0-100. |
| `grades_pass` | numeric | Team-level PFF passing grade, 0-100. |
| `grades_pass_block` | numeric | Team-level PFF pass-blocking grade, 0-100. |
| `grades_pass_route` | numeric | Team-level PFF receiving (route-running) grade, 0-100. |
| `grades_pass_rush_defense` | numeric | Team-level PFF pass-rush grade, 0-100. |
| `grades_run` | numeric | Team-level PFF rushing grade, 0-100. |
| `grades_run_block` | numeric | Team-level PFF run-blocking grade, 0-100. |
| `grades_run_defense` | numeric | Team-level PFF run-defense grade, 0-100. |
| `grades_tackle` | numeric | Team-level PFF tackling grade, 0-100. |
| `losses` | numeric | Losses against the spread in the split. |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `points_allowed` | numeric | Points for the opponent. |
| `points_scored` | numeric | Total points scored by the team over the covered span. |
| `ties` | numeric | Number of ties in the series. |
| `wins` | numeric | Wins against the spread in the split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_teams_overview()
```

_Last validated n/a._

## `pff_games`

Games list for league-season(-week)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/games`

**Valid URL:** [https://premium.pff.com/api/v1/games](https://premium.pff.com/api/v1/games)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Single week number. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `away_franchise_id` | numeric | PFF franchise id of the away team. |
| `away_team` | list | Away team object (JSON-stringified in the tidy frame). |
| `has_stats` | logical | Whether PFF has published stats for the game. |
| `home_franchise_id` | numeric | PFF franchise id of the home team. |
| `home_team` | list | Home team object (JSON-stringified in the tidy frame). |
| `id` | numeric | PFF game id (integer join key). |
| `league` | list | League slug. |
| `league_id` | numeric | PFF league id (integer). |
| `lock_status` | character | Data lock/publish status for the game. |
| `score` | list | Final score string. |
| `season` | numeric | Season (starting year) of the game. |
| `stadium_id` | numeric | PFF stadium identifier for the game venue. |
| `start` | character | Kickoff timestamp (ISO 8601 string). |
| `week` | numeric | Week number of the game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_games()
```

_Last validated n/a._

## `pff_players`

Player search (name=) or lookup (id=)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/players`

**Valid URL:** [https://premium.pff.com/api/v1/players](https://premium.pff.com/api/v1/players)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `name` | `name` |  |  | `Y` | Player-name search prefix. |
| `id` | `id` |  |  | `Y` | Entity id (player lookup). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `college` | character | Official college (usually the last one attended) |
| `current_class` | character | Player's current college class designation (e.g., Freshman, Senior), per PFF. |
| `current_eligible_year` | numeric | Year the player is or was first draft-eligible, per PFF. |
| `dob` | character | Player date of birth. |
| `draft` | list | Nested draft-selection details for the player (year, round, pick, and franchise) as returned by the source API. |
| `first_name` | character | First name of player |
| `height` | numeric | Official height, in inches |
| `id` | numeric | ID of the player in the 'name' column. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `last_name` | character | Last name of player |
| `position` | character | Primary position as reported by NFL.com |
| `speed` | numeric | Speed. |
| `team` | list | NFL team. Uses official abbreviations as per NFL.com |
| `weight` | numeric | Official weight, in pounds |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_players()
```

_Last validated n/a._

## `pff_player_seasons`

Seasons a player has data for

**Endpoint URL:** `GET https://premium.pff.com/api/v1/player/seasons`

**Valid URL:** [https://premium.pff.com/api/v1/player/seasons](https://premium.pff.com/api/v1/player/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `player_id` | `player_id` |  |  | `Y` | PFF player id (snake_case on the wire; matches the /players id). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_report`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_player_seasons()
```

_Last validated n/a._

## `pff_player_position_pivot`

Positional-pivot export (JSON; UI also uses this for CSV download)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/player/position/pivot`

**Valid URL:** [https://premium.pff.com/api/v1/player/position/pivot](https://premium.pff.com/api/v1/player/position/pivot)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `player_id` |  |  | `Y` | PFF player id (snake_case on the wire; matches the /players id). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_report`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
pff_player_position_pivot()
```

_Last validated n/a._
