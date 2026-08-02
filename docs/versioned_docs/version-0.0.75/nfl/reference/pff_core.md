---
title: NFL — PFF Premium Stats (premium.pff.com)
sidebar_label: PFF Premium Stats (premium.pff.com)
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
| `avg_depth_of_tackle` | numeric |  |
| `declined_penalties` | numeric |  |
| `draft_season` | numeric |  |
| `eligible_season` | numeric |  |
| `forced_fumbles` | numeric |  |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_coverage_defense` | numeric |  |
| `grades_defense` | numeric |  |
| `grades_defense_penalty` | numeric |  |
| `grades_pass_rush_defense` | numeric |  |
| `grades_run_defense` | numeric |  |
| `grades_tackle` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `missed_tackle_rate` | numeric |  |
| `missed_tackles` | numeric |  |
| `penalties` | numeric | Total number of penalties. |
| `player` | character | Player name |
| `player_game_count` | numeric |  |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `position` | character | Primary position as reported by NFL.com |
| `run_stop_opp` | numeric |  |
| `snap_counts_run` | numeric |  |
| `stop_percent` | numeric |  |
| `stops` | numeric |  |
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
| `twenty_attempts` | numeric |  |
| `pat_percent` | numeric |  |
| `draft_season` | numeric |  |
| `forty_made` | numeric |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `fifty_percent` | numeric |  |
| `total_made` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `one_made` | numeric |  |
| `fifty_attempts` | numeric |  |
| `forty_attempts` | numeric |  |
| `player_game_count` | numeric |  |
| `eligible_season` | numeric |  |
| `thirty_percent` | numeric |  |
| `total_attempts` | numeric |  |
| `pat_attempts` | numeric |  |
| `twenty_made` | numeric |  |
| `one_attempts` | numeric |  |
| `grades_fgep_kicker` | numeric |  |
| `thirty_attempts` | numeric |  |
| `penalties` | numeric | Total number of penalties. |
| `pat_made` | numeric |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric |  |
| `one_percent` | numeric |  |
| `total_percent` | numeric |  |
| `position` | character | Primary position as reported by NFL.com |
| `twenty_percent` | numeric |  |
| `forty_percent` | numeric |  |
| `player` | character | Player name |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `fifty_made` | numeric |  |
| `thirty_made` | numeric |  |
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
| `yards_per_coverage_snap` | numeric |  |
| `draft_season` | numeric |  |
| `missed_tackles` | numeric |  |
| `catch_rate` | numeric |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `tackles` | numeric | Team tackles. |
| `coverage_percent` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `dropped_ints` | numeric |  |
| `player_game_count` | numeric |  |
| `eligible_season` | numeric |  |
| `grades_tackle` | numeric |  |
| `yards` | numeric | The number of receiving yards |
| `receptions` | numeric | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `forced_incompletion_rate` | numeric |  |
| `grades_coverage_defense` | numeric |  |
| `interceptions` | numeric | The number of interceptions thrown. |
| `snap_counts_coverage` | numeric |  |
| `grades_run_defense` | numeric |  |
| `snap_counts_pass_play` | numeric |  |
| `penalties` | numeric | Total number of penalties. |
| `forced_incompletes` | numeric |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `coverage_snaps_per_target` | numeric |  |
| `stops` | numeric |  |
| `declined_penalties` | numeric |  |
| `position` | character | Primary position as reported by NFL.com |
| `longest` | numeric |  |
| `missed_tackle_rate` | numeric |  |
| `grades_defense` | numeric |  |
| `yards_per_reception` | numeric |  |
| `grades_defense_penalty` | numeric |  |
| `player` | character | Player name |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `yards_after_catch` | numeric | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `avg_depth_of_target` | numeric |  |
| `pass_break_ups` | numeric |  |
| `qb_rating_against` | numeric |  |
| `coverage_snaps_per_reception` | numeric |  |
| `assists` | numeric | Total assists. |
| `grades_pass_rush_defense` | numeric |  |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `touchdowns` | numeric |  |

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
| `attempts_with_hangtime` | numeric |  |
| `average_distance` | numeric |  |
| `average_hangtime` | numeric |  |
| `average_starting_field_position` | numeric |  |
| `average_yards_per_return` | numeric |  |
| `declined_penalties` | numeric |  |
| `draft_season` | numeric |  |
| `eligible_season` | numeric |  |
| `fair_catches` | numeric |  |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_kickoff_kicker` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `kicked_yards` | numeric |  |
| `kicks_returned` | numeric |  |
| `onside_kicks` | numeric |  |
| `penalties` | numeric | Total number of penalties. |
| `percent_returned` | numeric |  |
| `player` | character | Player name |
| `player_game_count` | numeric |  |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `position` | character | Primary position as reported by NFL.com |
| `return_yards` | numeric | Yards gained by the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `total_hangtime` | numeric |  |
| `touchbacks` | numeric |  |

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
| `grades_pass_block` | numeric |  |
| `grades_offense` | numeric |  |
| `pbe` | numeric |  |
| `non_spike_pass_block_percentage` | numeric |  |
| `draft_season` | numeric |  |
| `snap_counts_rg` | numeric |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `snap_counts_ce` | numeric |  |
| `hits_allowed` | numeric |  |
| `block_percent` | numeric |  |
| `snap_counts_offense` | numeric |  |
| `player_game_count` | numeric |  |
| `eligible_season` | numeric |  |
| `snap_counts_block` | numeric |  |
| `hurries_allowed` | numeric |  |
| `grades_run_block` | numeric |  |
| `snap_counts_run_block` | numeric |  |
| `pressures_allowed` | numeric |  |
| `snap_counts_pass_play` | numeric |  |
| `penalties` | numeric | Total number of penalties. |
| `sacks_allowed` | numeric | Opponent sacks. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric |  |
| `snap_counts_te` | numeric |  |
| `snap_counts_rt` | numeric |  |
| `position` | character | Primary position as reported by NFL.com |
| `non_spike_pass_block` | numeric |  |
| `snap_counts_lt` | numeric |  |
| `player` | character | Player name |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `snap_counts_pass_block` | numeric |  |
| `pass_block_percent` | numeric |  |
| `snap_counts_lg` | numeric |  |
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
| `draft_season` | numeric |  |
| `forced_fumbles` | numeric | Forced fumbles. |
| `missed_tackles` | numeric |  |
| `catch_rate` | numeric |  |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `tackles` | numeric | Total tackles made by the defender. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `snap_counts_offball` | numeric |  |
| `snap_counts_box` | numeric |  |
| `sacks` | numeric | Sacks credited. |
| `snap_counts_pass_rush` | numeric |  |
| `player_game_count` | numeric | Games with at least one qualifying snap in the requested range. |
| `eligible_season` | numeric |  |
| `snap_counts_dl` | numeric |  |
| `grades_tackle` | numeric |  |
| `yards` | numeric | The number of receiving yards |
| `receptions` | numeric | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `grades_coverage_defense` | numeric | PFF coverage grade (0-100). |
| `hurries` | numeric |  |
| `interceptions` | numeric | Interceptions made in coverage. |
| `snap_counts_coverage` | numeric |  |
| `snap_counts_dl_over_t` | numeric |  |
| `snap_counts_dl_a_gap` | numeric |  |
| `fumble_recoveries` | numeric |  |
| `grades_run_defense` | numeric | PFF run-defense grade (0-100). |
| `snap_counts_corner` | numeric |  |
| `hits` | numeric | Hits. |
| `penalties` | numeric | Total number of penalties. |
| `batted_passes` | numeric |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `stops` | numeric | Tackles that constitute an offensive failure ("stops"). |
| `declined_penalties` | numeric |  |
| `total_pressures` | numeric | Total quarterback pressures (sacks + hits + hurries). |
| `position` | character | Primary position as reported by NFL.com |
| `fumble_recovery_touchdowns` | numeric |  |
| `longest` | numeric |  |
| `snap_counts_slot` | numeric |  |
| `missed_tackle_rate` | numeric |  |
| `grades_defense` | numeric | PFF overall defense grade (0-100). |
| `yards_per_reception` | numeric |  |
| `grades_defense_penalty` | numeric |  |
| `safeties` | numeric |  |
| `player` | character | Player name |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `snap_counts_defense` | numeric |  |
| `yards_after_catch` | numeric | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `snap_counts_dl_b_gap` | numeric |  |
| `pass_break_ups` | numeric |  |
| `qb_rating_against` | numeric |  |
| `snap_counts_run_defense` | numeric |  |
| `tackles_for_loss` | numeric | Team tackles for a loss. |
| `assists` | numeric | Assisted tackles. |
| `grades_pass_rush_defense` | numeric |  |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `snap_counts_fs` | numeric |  |
| `touchdowns` | numeric |  |
| `snap_counts_dl_outside_t` | numeric |  |

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
| `declined_penalties` | numeric |  |
| `draft_season` | numeric |  |
| `eligible_season` | numeric |  |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `grades_hands_fumble` | numeric |  |
| `grades_offense` | numeric | PFF overall offense grade (0-100). |
| `grades_offense_penalty` | numeric |  |
| `grades_pass` | numeric | PFF passing grade (0-100). |
| `grades_run` | numeric | PFF rushing grade (0-100). |
| `grades_run_block` | numeric | PFF run-blocking grade (0-100). |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `penalties` | numeric | Total number of penalties. |
| `player` | character | Player name |
| `player_game_count` | numeric | Games with at least one qualifying snap in the requested range. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `position` | character | Primary position as reported by NFL.com |
| `snap_counts_pass` | numeric |  |
| `snap_counts_pass_block` | numeric |  |
| `snap_counts_pass_route` | numeric |  |
| `snap_counts_run` | numeric |  |
| `snap_counts_run_block` | numeric |  |
| `snap_counts_total` | numeric |  |
| `snap_counts_total_pass` | numeric |  |
| `snap_counts_total_run` | numeric |  |
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
| `te_percent` | numeric |  |
| `draft_season` | numeric |  |
| `pressures_lt` | numeric |  |
| `pressures_rg` | numeric |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `lt_percent` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `hits_allowed` | numeric |  |
| `pressures_lg` | numeric |  |
| `player_game_count` | numeric |  |
| `eligible_season` | numeric |  |
| `hurries_allowed` | numeric |  |
| `self_percent` | numeric |  |
| `pressures_rt` | numeric |  |
| `pressures_allowed` | numeric |  |
| `pressures_ol_te` | numeric |  |
| `pressures_ce` | numeric |  |
| `penalties` | numeric | Total number of penalties. |
| `sacks_allowed` | numeric | Opponent sacks. |
| `ol_te_percent` | numeric |  |
| `allowed_pressure_dropbacks` | numeric |  |
| `pressures_other` | numeric |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric |  |
| `ce_percent` | numeric |  |
| `position` | character | Primary position as reported by NFL.com |
| `pressures_te` | numeric |  |
| `pressures_self` | numeric |  |
| `lg_percent` | numeric |  |
| `player` | character | Player name |
| `rt_percent` | numeric |  |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `other_percent` | numeric |  |
| `pressures_off` | numeric |  |
| `rg_percent` | numeric |  |
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
| `true_pass_set_total_pressures` | numeric |  |
| `draft_season` | numeric |  |
| `true_pass_set_prp` | numeric |  |
| `true_pass_set_hurries` | numeric |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `prp` | numeric |  |
| `true_pass_set_sacks` | numeric |  |
| `pass_rush_win_rate` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `sacks` | numeric | The Number of times sacked. |
| `snap_counts_pass_rush` | numeric |  |
| `player_game_count` | numeric |  |
| `eligible_season` | numeric |  |
| `true_pass_set_snap_counts_pass_play` | numeric |  |
| `pass_rush_wins` | numeric |  |
| `hurries` | numeric |  |
| `pass_rush_opp` | numeric |  |
| `snap_counts_pass_play` | numeric |  |
| `hits` | numeric | Hits. |
| `true_pass_set_pass_rush_win_rate` | numeric |  |
| `penalties` | numeric | Total number of penalties. |
| `batted_passes` | numeric |  |
| `true_pass_set_hits` | numeric |  |
| `true_pass_set_snap_counts_pass_rush` | numeric |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric |  |
| `true_pass_set_pass_rush_wins` | numeric |  |
| `total_pressures` | numeric |  |
| `position` | character | Primary position as reported by NFL.com |
| `true_pass_set_grades_pass_rush_defense` | numeric |  |
| `true_pass_set_batted_passes` | numeric |  |
| `player` | character | Player name |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `pass_rush_percent` | numeric |  |
| `true_pass_set_pass_rush_opp` | numeric |  |
| `true_pass_set_pass_rush_percent` | numeric |  |
| `grades_pass_rush_defense` | numeric |  |
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
| `comp_pct_diff` | numeric |  |
| `pa_grades_pass` | numeric |  |
| `no_screen_grades_offense` | numeric |  |
| `pa_qb_rating` | numeric |  |
| `no_screen_qb_rating` | numeric |  |
| `pa_completions` | numeric |  |
| `pa_thrown_aways` | numeric |  |
| `pa_dropbacks_percent` | numeric |  |
| `ypa_diff` | numeric |  |
| `no_screen_drops` | numeric |  |
| `screen_completion_percent` | numeric |  |
| `npa_bats` | numeric |  |
| `no_screen_thrown_aways` | numeric |  |
| `pa_grades_run` | numeric |  |
| `screen_pressure_to_sack_rate` | numeric |  |
| `pa_drop_rate` | numeric |  |
| `screen_grades_offense` | numeric |  |
| `dropbacks` | numeric |  |
| `npa_thrown_aways` | numeric |  |
| `no_screen_def_gen_pressures` | numeric |  |
| `screen_grades_hands_fumble` | numeric |  |
| `pa_touchdowns` | numeric |  |
| `npa_ypa` | numeric |  |
| `draft_season` | numeric |  |
| `screen_avg_time_to_throw` | numeric |  |
| `screen_thrown_aways` | numeric |  |
| `npa_sacks` | numeric |  |
| `npa_aimed_passes` | numeric |  |
| `no_screen_completions` | numeric |  |
| `no_screen_positive_epa_percent` | numeric |  |
| `screen_spikes` | numeric |  |
| `pa_first_downs` | numeric |  |
| `pa_big_time_throws` | numeric |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `pa_spikes` | numeric |  |
| `pa_sack_percent` | numeric |  |
| `screen_dropbacks_percent` | numeric |  |
| `no_screen_epa` | numeric |  |
| `npa_avg_time_to_throw` | numeric |  |
| `screen_bats` | numeric |  |
| `screen_grades_run_block` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `no_screen_bats` | numeric |  |
| `npa_grades_offense` | numeric |  |
| `no_screen_grades_pass_route` | numeric |  |
| `screen_def_gen_pressures` | numeric |  |
| `pa_pressure_to_sack_rate` | numeric |  |
| `screen_sack_percent` | numeric |  |
| `no_screen_btt_rate` | numeric |  |
| `npa_btt_rate` | numeric |  |
| `screen_interceptions` | numeric |  |
| `no_screen_passing_snaps` | numeric |  |
| `no_screen_grades_hands_fumble` | numeric |  |
| `screen_scrambles` | numeric |  |
| `screen_grades_pass` | numeric |  |
| `npa_qb_rating` | numeric |  |
| `no_screen_grades_pass` | numeric |  |
| `pa_avg_time_to_throw` | numeric |  |
| `screen_twp_rate` | numeric |  |
| `player_game_count` | numeric |  |
| `npa_accuracy_percent` | numeric |  |
| `eligible_season` | numeric |  |
| `npa_drops` | numeric |  |
| `screen_yards` | numeric |  |
| `no_screen_drop_rate` | numeric |  |
| `no_screen_grades_offense_penalty` | numeric |  |
| `npa_passing_snaps` | numeric |  |
| `screen_drop_rate` | numeric |  |
| `screen_epa` | numeric |  |
| `screen_grades_pass_route` | numeric |  |
| `screen_grades_offense_penalty` | numeric |  |
| `npa_spikes` | numeric |  |
| `screen_hit_as_threw` | numeric |  |
| `no_screen_aimed_passes` | numeric |  |
| `screen_drops` | numeric |  |
| `screen_ypa` | numeric |  |
| `npa_twp_rate` | numeric |  |
| `screen_accuracy_percent` | numeric |  |
| `npa_hit_as_threw` | numeric |  |
| `pa_accuracy_percent` | numeric |  |
| `pa_def_gen_pressures` | numeric |  |
| `screen_avg_depth_of_target` | numeric |  |
| `pa_sacks` | numeric |  |
| `screen_passing_snaps` | numeric |  |
| `no_screen_grades_run` | numeric |  |
| `no_screen_first_downs` | numeric |  |
| `pa_ypa` | numeric |  |
| `npa_scrambles` | numeric |  |
| `npa_pressure_to_sack_rate` | numeric |  |
| `pa_twp_rate` | numeric |  |
| `no_screen_twp_rate` | numeric |  |
| `npa_grades_hands_fumble` | numeric |  |
| `screen_completions` | numeric |  |
| `screen_btt_rate` | numeric |  |
| `npa_grades_run` | numeric |  |
| `no_screen_interceptions` | numeric |  |
| `npa_grades_run_block` | numeric |  |
| `no_screen_sacks` | numeric |  |
| `penalties` | numeric | Total number of penalties. |
| `no_screen_big_time_throws` | numeric |  |
| `npa_drop_rate` | numeric |  |
| `npa_attempts` | numeric |  |
| `screen_qb_rating` | numeric |  |
| `npa_grades_offense_penalty` | numeric |  |
| `pa_bats` | numeric |  |
| `pa_attempts` | numeric |  |
| `npa_def_gen_pressures` | numeric |  |
| `no_screen_avg_time_to_throw` | numeric |  |
| `pa_yards` | numeric |  |
| `npa_positive_epa_percent` | numeric |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `no_screen_scrambles` | numeric |  |
| `pa_turnover_worthy_plays` | numeric |  |
| `pa_aimed_passes` | numeric |  |
| `declined_penalties` | numeric |  |
| `pa_grades_offense` | numeric |  |
| `screen_positive_epa_percent` | numeric |  |
| `no_screen_dropbacks` | numeric |  |
| `no_screen_dropbacks_percent` | numeric |  |
| `npa_grades_pass_route` | numeric |  |
| `npa_sack_percent` | numeric |  |
| `pa_positive_epa_percent` | numeric |  |
| `pa_grades_run_block` | numeric |  |
| `no_screen_turnover_worthy_plays` | numeric |  |
| `position` | character | Primary position as reported by NFL.com |
| `pa_grades_pass_route` | numeric |  |
| `npa_epa` | numeric |  |
| `no_screen_avg_depth_of_target` | numeric |  |
| `pa_grades_hands_fumble` | numeric |  |
| `no_screen_completion_percent` | numeric |  |
| `pa_hit_as_threw` | numeric |  |
| `pa_grades_offense_penalty` | numeric |  |
| `npa_dropbacks_percent` | numeric |  |
| `npa_grades_pass` | numeric |  |
| `screen_grades_run` | numeric |  |
| `screen_first_downs` | numeric |  |
| `npa_completion_percent` | numeric |  |
| `no_screen_grades_run_block` | numeric |  |
| `no_screen_hit_as_threw` | numeric |  |
| `screen_turnover_worthy_plays` | numeric |  |
| `npa_avg_depth_of_target` | numeric |  |
| `npa_dropbacks` | numeric |  |
| `player` | character | Player name |
| `pa_drops` | numeric |  |
| `pa_btt_rate` | numeric |  |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `screen_big_time_throws` | numeric |  |
| `screen_aimed_passes` | numeric |  |
| `npa_touchdowns` | numeric |  |
| `screen_attempts` | numeric |  |
| `screen_dropbacks` | numeric |  |
| `no_screen_yards` | numeric |  |
| `pa_scrambles` | numeric |  |
| `pa_completion_percent` | numeric |  |
| `pa_avg_depth_of_target` | numeric |  |
| `pa_interceptions` | numeric |  |
| `no_screen_accuracy_percent` | numeric |  |
| `no_screen_spikes` | numeric |  |
| `pa_dropbacks` | numeric |  |
| `npa_big_time_throws` | numeric |  |
| `no_screen_sack_percent` | numeric |  |
| `pa_epa` | numeric |  |
| `no_screen_attempts` | numeric |  |
| `pa_passing_snaps` | numeric |  |
| `npa_completions` | numeric |  |
| `screen_touchdowns` | numeric |  |
| `npa_first_downs` | numeric |  |
| `no_screen_touchdowns` | numeric |  |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `npa_yards` | numeric |  |
| `no_screen_ypa` | numeric |  |
| `npa_turnover_worthy_plays` | numeric |  |
| `npa_interceptions` | numeric |  |
| `no_screen_pressure_to_sack_rate` | numeric |  |
| `screen_sacks` | numeric |  |
| `screen_grades_hands_drop` | numeric |  |
| `screen_grades_pass_block` | numeric |  |
| `no_screen_grades_pass_block` | numeric |  |
| `pa_grades_hands_drop` | numeric |  |
| `npa_grades_pass_block` | numeric |  |
| `pa_grades_pass_block` | numeric |  |
| `npa_grades_hands_drop` | numeric |  |
| `no_screen_grades_hands_drop` | numeric |  |
| `pa_grades_screen_block` | numeric |  |
| `screen_grades_screen_block` | numeric |  |
| `no_screen_grades_screen_block` | numeric |  |
| `npa_grades_screen_block` | numeric |  |
| `pa_grades_defense_penalty` | numeric |  |
| `npa_grades_defense_penalty` | numeric |  |
| `npa_grades_coverage_defense` | numeric |  |
| `pa_grades_defense` | numeric |  |
| `screen_grades_coverage_defense` | numeric |  |
| `npa_grades_defense` | numeric |  |
| `screen_grades_defense` | numeric |  |
| `screen_grades_defense_penalty` | numeric |  |
| `no_screen_grades_defense` | numeric |  |
| `pa_grades_coverage_defense` | numeric |  |
| `no_screen_grades_defense_penalty` | numeric |  |
| `no_screen_grades_coverage_defense` | numeric |  |
| `no_screen_grades_overall_tackle` | numeric |  |
| `pa_grades_overall_tackle` | numeric |  |
| `pa_grades_tackle` | numeric |  |
| `pa_grades_pass_rush_defense` | character |  |
| `screen_grades_run_defense` | character |  |
| `pa_grades_run_defense` | character |  |
| `npa_grades_overall_tackle` | numeric |  |
| `no_screen_grades_pass_rush_defense` | numeric |  |
| `npa_grades_tackle` | numeric |  |
| `screen_grades_tackle` | character |  |
| `no_screen_grades_tackle` | numeric |  |
| `npa_grades_pass_rush_defense` | numeric |  |
| `no_screen_grades_run_defense` | numeric |  |
| `screen_grades_overall_tackle` | character |  |
| `npa_grades_run_defense` | numeric |  |
| `screen_grades_pass_rush_defense` | numeric |  |

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
| `man_touchdowns` | numeric |  |
| `man_interceptions` | numeric |  |
| `zone_snap_counts_coverage_percent` | numeric |  |
| `man_avg_depth_of_target` | numeric |  |
| `man_dropped_ints` | numeric |  |
| `draft_season` | numeric |  |
| `zone_coverage_snaps_per_target` | numeric |  |
| `man_yards_per_reception` | numeric |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `man_snap_counts_coverage` | numeric |  |
| `man_tackles` | numeric |  |
| `zone_stops` | numeric |  |
| `zone_snap_counts_coverage` | numeric |  |
| `zone_yards` | numeric |  |
| `man_coverage_snaps_per_target` | numeric |  |
| `zone_coverage_percent` | numeric |  |
| `player_game_count` | numeric |  |
| `eligible_season` | numeric |  |
| `zone_receptions` | numeric |  |
| `zone_forced_incompletion_rate` | numeric |  |
| `man_snap_counts_pass_play` | numeric |  |
| `zone_yards_per_reception` | numeric |  |
| `man_longest` | numeric |  |
| `man_assists` | numeric |  |
| `zone_yards_after_catch` | numeric |  |
| `man_stops` | numeric |  |
| `man_receptions` | numeric |  |
| `zone_tackles` | numeric |  |
| `man_coverage_percent` | numeric |  |
| `penalties` | numeric | Total number of penalties. |
| `zone_yards_per_coverage_snap` | numeric |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `man_catch_rate` | numeric |  |
| `man_grades_coverage_defense` | numeric |  |
| `declined_penalties` | numeric |  |
| `man_yards_after_catch` | numeric |  |
| `man_pass_break_ups` | numeric |  |
| `man_yards` | numeric |  |
| `position` | character | Primary position as reported by NFL.com |
| `man_targets` | numeric |  |
| `man_missed_tackle_rate` | numeric |  |
| `zone_assists` | numeric |  |
| `zone_snap_counts_pass_play` | numeric |  |
| `zone_missed_tackle_rate` | numeric |  |
| `zone_touchdowns` | numeric |  |
| `zone_coverage_snaps_per_reception` | numeric |  |
| `man_coverage_snaps_per_reception` | numeric |  |
| `zone_avg_depth_of_target` | numeric |  |
| `man_snap_counts_coverage_percent` | numeric |  |
| `player` | character | Player name |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `zone_forced_incompletes` | numeric |  |
| `man_forced_incompletion_rate` | numeric |  |
| `zone_targets` | numeric |  |
| `zone_longest` | numeric |  |
| `zone_dropped_ints` | numeric |  |
| `zone_interceptions` | numeric |  |
| `man_forced_incompletes` | numeric |  |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `zone_missed_tackles` | numeric |  |
| `base_snap_counts_coverage` | numeric |  |
| `man_missed_tackles` | numeric |  |
| `zone_grades_coverage_defense` | numeric |  |
| `man_qb_rating_against` | numeric |  |
| `zone_pass_break_ups` | numeric |  |
| `zone_qb_rating_against` | numeric |  |
| `man_yards_per_coverage_snap` | numeric |  |
| `zone_catch_rate` | numeric |  |

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
| `left_behind_los_accuracy_percent` | numeric |  |
| `left_short_scrambles` | numeric |  |
| `center_short_first_downs` | numeric |  |
| `no_blitz_completion_percent` | numeric |  |
| `right_short_bats` | numeric |  |
| `right_behind_los_positive_epa_percent` | numeric |  |
| `left_behind_los_completions` | numeric |  |
| `comp_pct_diff` | numeric |  |
| `pa_grades_pass` | numeric |  |
| `left_medium_dropbacks` | numeric |  |
| `right_medium_grades_pass` | numeric |  |
| `right_medium_hit_as_threw` | numeric |  |
| `behind_los_attempts_percent` | numeric |  |
| `grades_offense` | numeric |  |
| `no_screen_grades_offense` | numeric |  |
| `pa_qb_rating` | numeric |  |
| `right_short_turnover_worthy_plays` | numeric |  |
| `no_screen_qb_rating` | numeric |  |
| `pa_completions` | numeric |  |
| `right_medium_qb_rating` | numeric |  |
| `deep_twp_rate` | numeric |  |
| `center_medium_sacks` | numeric |  |
| `medium_interceptions` | numeric |  |
| `left_deep_completions` | numeric |  |
| `no_pressure_scrambles` | numeric |  |
| `twp_rate` | numeric |  |
| `behind_los_spikes` | numeric |  |
| `medium_aimed_passes` | numeric |  |
| `pa_thrown_aways` | numeric |  |
| `pa_dropbacks_percent` | numeric |  |
| `ypa_diff` | numeric |  |
| `blitz_touchdowns` | numeric |  |
| `center_behind_los_drops` | numeric |  |
| `pressure_yards` | numeric |  |
| `no_pressure_spikes` | numeric |  |
| `blitz_ypa` | numeric |  |
| `center_behind_los_interceptions` | numeric |  |
| `no_screen_drops` | numeric |  |
| `behind_los_dropbacks` | numeric |  |
| `no_blitz_grades_run` | numeric |  |
| `screen_completion_percent` | numeric |  |
| `npa_bats` | numeric |  |
| `left_behind_los_interceptions` | numeric |  |
| `left_deep_first_downs` | numeric |  |
| `deep_passing_snaps` | numeric |  |
| `btt_rate` | numeric |  |
| `center_short_btt_rate` | numeric |  |
| `center_medium_thrown_aways` | numeric |  |
| `center_deep_avg_depth_of_target` | numeric |  |
| `blitz_qb_rating` | numeric |  |
| `center_short_drops` | numeric |  |
| `no_pressure_thrown_aways` | numeric |  |
| `no_screen_thrown_aways` | numeric |  |
| `no_blitz_bats` | numeric |  |
| `center_deep_first_downs` | numeric |  |
| `left_medium_completion_percent` | numeric |  |
| `center_deep_attempts` | numeric |  |
| `pa_grades_run` | numeric |  |
| `center_behind_los_attempts` | numeric |  |
| `right_short_positive_epa_percent` | numeric |  |
| `no_blitz_drops` | numeric |  |
| `center_deep_aimed_passes` | numeric |  |
| `right_deep_grades_pass` | numeric |  |
| `right_medium_big_time_throws` | numeric |  |
| `deep_sack_percent` | numeric |  |
| `no_pressure_completion_percent` | numeric |  |
| `blitz_aimed_passes` | numeric |  |
| `screen_pressure_to_sack_rate` | numeric |  |
| `pa_drop_rate` | numeric |  |
| `center_medium_pressure_to_sack_rate` | numeric |  |
| `deep_sacks` | numeric |  |
| `right_medium_ypa` | numeric |  |
| `spikes` | numeric | Spikes |
| `left_deep_drop_rate` | numeric |  |
| `screen_grades_offense` | numeric |  |
| `right_medium_attempts_percent` | numeric |  |
| `right_deep_btt_rate` | numeric |  |
| `center_short_attempts_percent` | numeric |  |
| `deep_drops` | numeric |  |
| `center_behind_los_big_time_throws` | numeric |  |
| `center_behind_los_touchdowns` | numeric |  |
| `dropbacks` | numeric |  |
| `right_behind_los_twp_rate` | numeric |  |
| `center_deep_completions` | numeric |  |
| `npa_thrown_aways` | numeric |  |
| `center_short_attempts` | numeric |  |
| `pressure_grades_run` | numeric |  |
| `left_behind_los_sack_percent` | numeric |  |
| `center_short_pressure_to_sack_rate` | numeric |  |
| `deep_touchdowns` | numeric |  |
| `no_blitz_sack_percent` | numeric |  |
| `no_pressure_bats` | numeric |  |
| `center_behind_los_accuracy_percent` | numeric |  |
| `pressure_completions` | numeric |  |
| `blitz_big_time_throws` | numeric |  |
| `center_deep_drop_rate` | numeric |  |
| `right_short_first_downs` | numeric |  |
| `no_screen_def_gen_pressures` | numeric |  |
| `right_behind_los_first_downs` | numeric |  |
| `screen_grades_hands_fumble` | numeric |  |
| `pa_touchdowns` | numeric |  |
| `short_interceptions` | numeric |  |
| `center_medium_scrambles` | numeric |  |
| `left_behind_los_sacks` | numeric |  |
| `npa_ypa` | numeric |  |
| `no_blitz_drop_rate` | numeric |  |
| `blitz_spikes` | numeric |  |
| `left_short_ypa` | numeric |  |
| `no_pressure_completions` | numeric |  |
| `left_short_qb_rating` | numeric |  |
| `thrown_aways` | numeric |  |
| `right_behind_los_qb_rating` | numeric |  |
| `draft_season` | numeric |  |
| `screen_avg_time_to_throw` | numeric |  |
| `right_behind_los_dropbacks` | numeric |  |
| `screen_thrown_aways` | numeric |  |
| `behind_los_def_gen_pressures` | numeric |  |
| `npa_sacks` | numeric |  |
| `no_pressure_passing_snaps` | numeric |  |
| `npa_aimed_passes` | numeric |  |
| `no_blitz_first_downs` | numeric |  |
| `right_short_thrown_aways` | numeric |  |
| `deep_def_gen_pressures` | numeric |  |
| `right_short_btt_rate` | numeric |  |
| `center_behind_los_positive_epa_percent` | numeric |  |
| `blitz_avg_time_to_throw` | numeric |  |
| `left_behind_los_grades_pass` | numeric |  |
| `deep_grades_pass` | numeric |  |
| `no_pressure_grades_hands_fumble` | numeric |  |
| `center_short_turnover_worthy_plays` | numeric |  |
| `center_behind_los_scrambles` | numeric |  |
| `pressure_aimed_passes` | numeric |  |
| `no_screen_completions` | numeric |  |
| `right_short_aimed_passes` | numeric |  |
| `no_screen_positive_epa_percent` | numeric |  |
| `center_short_dropbacks` | numeric |  |
| `medium_epa` | numeric |  |
| `right_short_ypa` | numeric |  |
| `blitz_sacks` | numeric |  |
| `center_medium_ypa` | numeric |  |
| `screen_spikes` | numeric |  |
| `pa_first_downs` | numeric |  |
| `medium_attempts` | numeric |  |
| `no_pressure_interceptions` | numeric |  |
| `right_deep_accuracy_percent` | numeric |  |
| `behind_los_scrambles` | numeric |  |
| `center_behind_los_completion_percent` | numeric |  |
| `left_behind_los_btt_rate` | numeric |  |
| `right_behind_los_btt_rate` | numeric |  |
| `pa_big_time_throws` | numeric |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `short_touchdowns` | numeric |  |
| `pa_spikes` | numeric |  |
| `center_medium_drops` | numeric |  |
| `left_behind_los_aimed_passes` | numeric |  |
| `pa_sack_percent` | numeric |  |
| `deep_attempts` | numeric |  |
| `right_behind_los_sack_percent` | numeric |  |
| `pressure_epa` | numeric |  |
| `center_short_positive_epa_percent` | numeric |  |
| `deep_avg_depth_of_target` | numeric |  |
| `left_deep_btt_rate` | numeric |  |
| `medium_scrambles` | numeric |  |
| `blitz_completions` | numeric |  |
| `center_behind_los_avg_depth_of_target` | numeric |  |
| `blitz_attempts` | numeric |  |
| `short_attempts_percent` | numeric |  |
| `right_behind_los_sacks` | numeric |  |
| `screen_dropbacks_percent` | numeric |  |
| `center_medium_avg_time_to_throw` | numeric |  |
| `pressure_twp_rate` | numeric |  |
| `pressure_sacks` | numeric |  |
| `left_short_pressure_to_sack_rate` | numeric |  |
| `no_blitz_pressure_to_sack_rate` | numeric |  |
| `no_pressure_ypa` | numeric |  |
| `no_screen_epa` | numeric |  |
| `center_deep_sack_percent` | numeric |  |
| `center_deep_ypa` | numeric |  |
| `left_behind_los_hit_as_threw` | numeric |  |
| `pressure_passing_snaps` | numeric |  |
| `medium_big_time_throws` | numeric |  |
| `grades_pass` | numeric |  |
| `npa_avg_time_to_throw` | numeric |  |
| `deep_thrown_aways` | numeric |  |
| `right_short_accuracy_percent` | numeric |  |
| `left_deep_turnover_worthy_plays` | numeric |  |
| `center_medium_bats` | numeric |  |
| `right_short_grades_pass` | numeric |  |
| `hit_as_threw` | numeric |  |
| `right_deep_spikes` | numeric |  |
| `left_deep_passing_snaps` | numeric |  |
| `center_medium_twp_rate` | numeric |  |
| `right_behind_los_passing_snaps` | numeric |  |
| `left_deep_qb_rating` | numeric |  |
| `screen_bats` | numeric |  |
| `right_deep_drop_rate` | numeric |  |
| `first_downs` | numeric | First downs earned by the team. |
| `screen_grades_run_block` | numeric |  |
| `left_behind_los_drop_rate` | numeric |  |
| `pressure_bats` | numeric |  |
| `left_medium_drop_rate` | numeric |  |
| `right_deep_attempts` | numeric |  |
| `left_deep_spikes` | numeric |  |
| `center_behind_los_dropbacks` | numeric |  |
| `blitz_thrown_aways` | numeric |  |
| `right_deep_big_time_throws` | numeric |  |
| `medium_hit_as_threw` | numeric |  |
| `right_short_dropbacks` | numeric |  |
| `no_pressure_drops` | numeric |  |
| `medium_def_gen_pressures` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `left_short_positive_epa_percent` | numeric |  |
| `left_short_attempts` | numeric |  |
| `pressure_grades_offense_penalty` | numeric |  |
| `center_deep_twp_rate` | numeric |  |
| `right_medium_avg_time_to_throw` | numeric |  |
| `left_behind_los_qb_rating` | numeric |  |
| `no_screen_bats` | numeric |  |
| `left_behind_los_dropbacks` | numeric |  |
| `pressure_scrambles` | numeric |  |
| `blitz_drop_rate` | numeric |  |
| `deep_dropbacks` | numeric |  |
| `right_behind_los_scrambles` | numeric |  |
| `sack_percent` | numeric |  |
| `behind_los_interceptions` | numeric |  |
| `right_deep_drops` | numeric |  |
| `right_deep_yards` | numeric |  |
| `right_short_hit_as_threw` | numeric |  |
| `npa_grades_offense` | numeric |  |
| `right_short_avg_depth_of_target` | numeric |  |
| `short_bats` | numeric |  |
| `right_deep_twp_rate` | numeric |  |
| `right_behind_los_pressure_to_sack_rate` | numeric |  |
| `screen_def_gen_pressures` | numeric |  |
| `deep_spikes` | numeric |  |
| `pa_pressure_to_sack_rate` | numeric |  |
| `center_medium_spikes` | numeric |  |
| `screen_sack_percent` | numeric |  |
| `blitz_completion_percent` | numeric |  |
| `behind_los_aimed_passes` | numeric |  |
| `right_behind_los_touchdowns` | numeric |  |
| `bats` | numeric |  |
| `right_medium_pressure_to_sack_rate` | numeric |  |
| `no_screen_btt_rate` | numeric |  |
| `medium_thrown_aways` | numeric |  |
| `short_sacks` | numeric |  |
| `center_behind_los_twp_rate` | numeric |  |
| `left_behind_los_attempts` | numeric |  |
| `short_aimed_passes` | numeric |  |
| `short_completions` | numeric |  |
| `right_short_pressure_to_sack_rate` | numeric |  |
| `npa_btt_rate` | numeric |  |
| `medium_avg_depth_of_target` | numeric |  |
| `left_behind_los_positive_epa_percent` | numeric |  |
| `screen_interceptions` | numeric |  |
| `center_medium_attempts_percent` | numeric |  |
| `left_medium_touchdowns` | numeric |  |
| `center_short_bats` | numeric |  |
| `left_deep_avg_time_to_throw` | numeric |  |
| `no_screen_passing_snaps` | numeric |  |
| `no_pressure_first_downs` | numeric |  |
| `center_behind_los_passing_snaps` | numeric |  |
| `center_short_yards` | numeric |  |
| `blitz_grades_offense_penalty` | numeric |  |
| `right_medium_btt_rate` | numeric |  |
| `right_medium_scrambles` | numeric |  |
| `no_screen_grades_hands_fumble` | numeric |  |
| `medium_btt_rate` | numeric |  |
| `no_blitz_epa` | numeric |  |
| `center_behind_los_btt_rate` | numeric |  |
| `blitz_interceptions` | numeric |  |
| `no_blitz_dropbacks` | numeric |  |
| `center_short_avg_depth_of_target` | numeric |  |
| `left_medium_sacks` | numeric |  |
| `right_short_yards` | numeric |  |
| `left_medium_first_downs` | numeric |  |
| `no_blitz_grades_pass` | numeric |  |
| `right_medium_positive_epa_percent` | numeric |  |
| `screen_scrambles` | numeric |  |
| `left_short_big_time_throws` | numeric |  |
| `deep_turnover_worthy_plays` | numeric |  |
| `no_blitz_scrambles` | numeric |  |
| `pressure_drop_rate` | numeric |  |
| `left_deep_bats` | numeric |  |
| `no_blitz_yards` | numeric |  |
| `left_medium_turnover_worthy_plays` | numeric |  |
| `screen_grades_pass` | numeric |  |
| `center_deep_drops` | numeric |  |
| `center_medium_avg_depth_of_target` | numeric |  |
| `sacks` | numeric | The Number of times sacked. |
| `pressure_pressure_to_sack_rate` | numeric |  |
| `center_behind_los_ypa` | numeric |  |
| `right_short_attempts` | numeric |  |
| `center_medium_completion_percent` | numeric |  |
| `no_blitz_grades_hands_fumble` | numeric |  |
| `right_short_sack_percent` | numeric |  |
| `left_behind_los_avg_depth_of_target` | numeric |  |
| `center_medium_epa` | numeric |  |
| `center_behind_los_sack_percent` | numeric |  |
| `npa_qb_rating` | numeric |  |
| `no_pressure_grades_offense` | numeric |  |
| `medium_completions` | numeric |  |
| `no_screen_grades_pass` | numeric |  |
| `medium_sack_percent` | numeric |  |
| `left_short_grades_pass` | numeric |  |
| `deep_btt_rate` | numeric |  |
| `pa_avg_time_to_throw` | numeric |  |
| `left_short_accuracy_percent` | numeric |  |
| `no_pressure_avg_time_to_throw` | numeric |  |
| `screen_twp_rate` | numeric |  |
| `pressure_dropbacks` | numeric |  |
| `short_twp_rate` | numeric |  |
| `short_passing_snaps` | numeric |  |
| `left_deep_sacks` | numeric |  |
| `short_scrambles` | numeric |  |
| `center_medium_turnover_worthy_plays` | numeric |  |
| `no_blitz_grades_offense_penalty` | numeric |  |
| `behind_los_drop_rate` | numeric |  |
| `right_behind_los_epa` | numeric |  |
| `right_medium_aimed_passes` | numeric |  |
| `no_pressure_grades_run` | numeric |  |
| `short_accuracy_percent` | numeric |  |
| `player_game_count` | numeric |  |
| `short_btt_rate` | numeric |  |
| `right_short_touchdowns` | numeric |  |
| `center_short_completions` | numeric |  |
| `right_short_spikes` | numeric |  |
| `behind_los_first_downs` | numeric |  |
| `right_medium_def_gen_pressures` | numeric |  |
| `blitz_turnover_worthy_plays` | numeric |  |
| `deep_pressure_to_sack_rate` | numeric |  |
| `npa_accuracy_percent` | numeric |  |
| `no_blitz_touchdowns` | numeric |  |
| `no_blitz_avg_depth_of_target` | numeric |  |
| `medium_qb_rating` | numeric |  |
| `short_completion_percent` | numeric |  |
| `right_deep_attempts_percent` | numeric |  |
| `no_pressure_dropbacks_percent` | numeric |  |
| `blitz_grades_pass` | numeric |  |
| `eligible_season` | numeric |  |
| `short_big_time_throws` | numeric |  |
| `behind_los_attempts` | numeric |  |
| `blitz_avg_depth_of_target` | numeric |  |
| `no_blitz_spikes` | numeric |  |
| `center_behind_los_yards` | numeric |  |
| `npa_drops` | numeric |  |
| `center_behind_los_bats` | numeric |  |
| `center_behind_los_qb_rating` | numeric |  |
| `right_deep_pressure_to_sack_rate` | numeric |  |
| `center_short_accuracy_percent` | numeric |  |
| `left_behind_los_bats` | numeric |  |
| `no_pressure_dropbacks` | numeric |  |
| `center_deep_passing_snaps` | numeric |  |
| `right_behind_los_hit_as_threw` | numeric |  |
| `medium_bats` | numeric |  |
| `right_medium_first_downs` | numeric |  |
| `completions` | numeric | The number of completed passes. |
| `medium_spikes` | numeric |  |
| `left_deep_thrown_aways` | numeric |  |
| `screen_yards` | numeric |  |
| `right_deep_epa` | numeric |  |
| `center_medium_hit_as_threw` | numeric |  |
| `left_behind_los_completion_percent` | numeric |  |
| `blitz_btt_rate` | numeric |  |
| `no_screen_drop_rate` | numeric |  |
| `center_short_ypa` | numeric |  |
| `right_short_drop_rate` | numeric |  |
| `right_behind_los_accuracy_percent` | numeric |  |
| `blitz_positive_epa_percent` | numeric |  |
| `no_pressure_btt_rate` | numeric |  |
| `left_short_yards` | numeric |  |
| `deep_accuracy_percent` | numeric |  |
| `right_behind_los_avg_depth_of_target` | numeric |  |
| `center_behind_los_thrown_aways` | numeric |  |
| `center_behind_los_attempts_percent` | numeric |  |
| `no_screen_grades_offense_penalty` | numeric |  |
| `right_deep_first_downs` | numeric |  |
| `left_short_completions` | numeric |  |
| `deep_completion_percent` | numeric |  |
| `left_deep_positive_epa_percent` | numeric |  |
| `npa_passing_snaps` | numeric |  |
| `screen_drop_rate` | numeric |  |
| `screen_epa` | numeric |  |
| `no_pressure_drop_rate` | numeric |  |
| `no_blitz_turnover_worthy_plays` | numeric |  |
| `yards` | numeric | The number of receiving yards |
| `right_deep_sack_percent` | numeric |  |
| `behind_los_pressure_to_sack_rate` | numeric |  |
| `screen_grades_offense_penalty` | numeric |  |
| `right_deep_touchdowns` | numeric |  |
| `npa_spikes` | numeric |  |
| `pressure_positive_epa_percent` | numeric |  |
| `screen_hit_as_threw` | numeric |  |
| `left_behind_los_def_gen_pressures` | numeric |  |
| `left_behind_los_drops` | numeric |  |
| `deep_epa` | numeric |  |
| `left_deep_ypa` | numeric |  |
| `medium_touchdowns` | numeric |  |
| `no_screen_aimed_passes` | numeric |  |
| `screen_drops` | numeric |  |
| `left_medium_grades_pass` | numeric |  |
| `accuracy_percent` | numeric |  |
| `right_behind_los_spikes` | numeric |  |
| `screen_ypa` | numeric |  |
| `medium_grades_pass` | numeric |  |
| `blitz_first_downs` | numeric |  |
| `npa_twp_rate` | numeric |  |
| `behind_los_passing_snaps` | numeric |  |
| `left_short_first_downs` | numeric |  |
| `center_short_passing_snaps` | numeric |  |
| `center_deep_accuracy_percent` | numeric |  |
| `right_deep_positive_epa_percent` | numeric |  |
| `right_medium_sack_percent` | numeric |  |
| `right_deep_qb_rating` | numeric |  |
| `no_blitz_dropbacks_percent` | numeric |  |
| `right_short_completion_percent` | numeric |  |
| `screen_accuracy_percent` | numeric |  |
| `right_behind_los_interceptions` | numeric |  |
| `behind_los_yards` | numeric |  |
| `center_behind_los_grades_pass` | numeric |  |
| `left_deep_dropbacks` | numeric |  |
| `npa_hit_as_threw` | numeric |  |
| `center_behind_los_spikes` | numeric |  |
| `scrambles` | numeric |  |
| `right_behind_los_aimed_passes` | numeric |  |
| `left_short_interceptions` | numeric |  |
| `right_medium_interceptions` | numeric |  |
| `left_behind_los_yards` | numeric |  |
| `pa_accuracy_percent` | numeric |  |
| `center_deep_btt_rate` | numeric |  |
| `pressure_turnover_worthy_plays` | numeric |  |
| `medium_first_downs` | numeric |  |
| `no_pressure_epa` | numeric |  |
| `left_short_avg_depth_of_target` | numeric |  |
| `left_behind_los_ypa` | numeric |  |
| `short_attempts` | numeric |  |
| `right_medium_bats` | numeric |  |
| `no_blitz_avg_time_to_throw` | numeric |  |
| `left_behind_los_touchdowns` | numeric |  |
| `pa_def_gen_pressures` | numeric |  |
| `right_medium_dropbacks` | numeric |  |
| `interceptions` | numeric | The number of interceptions thrown. |
| `screen_avg_depth_of_target` | numeric |  |
| `pa_sacks` | numeric |  |
| `short_turnover_worthy_plays` | numeric |  |
| `screen_passing_snaps` | numeric |  |
| `right_deep_thrown_aways` | numeric |  |
| `drop_rate` | numeric |  |
| `right_behind_los_drop_rate` | numeric |  |
| `no_screen_grades_run` | numeric |  |
| `right_short_qb_rating` | numeric |  |
| `medium_sacks` | numeric |  |
| `center_deep_attempts_percent` | numeric |  |
| `left_short_dropbacks` | numeric |  |
| `behind_los_drops` | numeric |  |
| `short_sack_percent` | numeric |  |
| `no_screen_first_downs` | numeric |  |
| `no_blitz_positive_epa_percent` | numeric |  |
| `left_medium_spikes` | numeric |  |
| `left_medium_accuracy_percent` | numeric |  |
| `pressure_btt_rate` | numeric |  |
| `pa_ypa` | numeric |  |
| `behind_los_completions` | numeric |  |
| `npa_scrambles` | numeric |  |
| `no_pressure_aimed_passes` | numeric |  |
| `pressure_dropbacks_percent` | numeric |  |
| `grades_run` | numeric |  |
| `behind_los_sack_percent` | numeric |  |
| `left_deep_yards` | numeric |  |
| `left_short_aimed_passes` | numeric |  |
| `npa_pressure_to_sack_rate` | numeric |  |
| `center_behind_los_completions` | numeric |  |
| `pa_twp_rate` | numeric |  |
| `no_pressure_def_gen_pressures` | numeric |  |
| `left_short_thrown_aways` | numeric |  |
| `pressure_grades_offense` | numeric |  |
| `left_short_sack_percent` | numeric |  |
| `center_short_thrown_aways` | numeric |  |
| `center_short_interceptions` | numeric |  |
| `short_avg_depth_of_target` | numeric |  |
| `pressure_completion_percent` | numeric |  |
| `left_deep_touchdowns` | numeric |  |
| `deep_yards` | numeric |  |
| `center_behind_los_turnover_worthy_plays` | numeric |  |
| `medium_ypa` | numeric |  |
| `left_medium_epa` | numeric |  |
| `left_deep_avg_depth_of_target` | numeric |  |
| `deep_first_downs` | numeric |  |
| `pressure_avg_depth_of_target` | numeric |  |
| `short_drop_rate` | numeric |  |
| `left_medium_bats` | numeric |  |
| `blitz_epa` | numeric |  |
| `center_deep_spikes` | numeric |  |
| `center_short_hit_as_threw` | numeric |  |
| `left_medium_positive_epa_percent` | numeric |  |
| `center_deep_touchdowns` | numeric |  |
| `right_deep_ypa` | numeric |  |
| `right_short_big_time_throws` | numeric |  |
| `center_short_big_time_throws` | numeric |  |
| `short_positive_epa_percent` | numeric |  |
| `no_screen_twp_rate` | numeric |  |
| `left_behind_los_avg_time_to_throw` | numeric |  |
| `right_deep_passing_snaps` | numeric |  |
| `pressure_drops` | numeric |  |
| `right_short_twp_rate` | numeric |  |
| `center_medium_attempts` | numeric |  |
| `right_deep_avg_time_to_throw` | numeric |  |
| `no_blitz_def_gen_pressures` | numeric |  |
| `center_deep_def_gen_pressures` | numeric |  |
| `pressure_attempts` | numeric |  |
| `npa_grades_hands_fumble` | numeric |  |
| `behind_los_epa` | numeric |  |
| `right_medium_twp_rate` | numeric |  |
| `right_deep_aimed_passes` | numeric |  |
| `right_deep_scrambles` | numeric |  |
| `deep_big_time_throws` | numeric |  |
| `left_short_turnover_worthy_plays` | numeric |  |
| `qb_rating` | numeric |  |
| `center_short_touchdowns` | numeric |  |
| `right_medium_drops` | numeric |  |
| `left_deep_epa` | numeric |  |
| `pressure_big_time_throws` | numeric |  |
| `no_blitz_thrown_aways` | numeric |  |
| `short_ypa` | numeric |  |
| `medium_pressure_to_sack_rate` | numeric |  |
| `left_medium_thrown_aways` | numeric |  |
| `right_behind_los_avg_time_to_throw` | numeric |  |
| `completion_percent` | numeric |  |
| `blitz_drops` | numeric |  |
| `behind_los_btt_rate` | numeric |  |
| `screen_completions` | numeric |  |
| `screen_btt_rate` | numeric |  |
| `npa_grades_run` | numeric |  |
| `no_pressure_touchdowns` | numeric |  |
| `no_screen_interceptions` | numeric |  |
| `medium_avg_time_to_throw` | numeric |  |
| `center_deep_completion_percent` | numeric |  |
| `behind_los_avg_time_to_throw` | numeric |  |
| `right_medium_touchdowns` | numeric |  |
| `no_pressure_sacks` | numeric |  |
| `center_short_avg_time_to_throw` | numeric |  |
| `npa_grades_run_block` | numeric |  |
| `left_deep_aimed_passes` | numeric |  |
| `left_medium_yards` | numeric |  |
| `center_medium_touchdowns` | numeric |  |
| `blitz_sack_percent` | numeric |  |
| `no_screen_sacks` | numeric |  |
| `center_short_drop_rate` | numeric |  |
| `left_short_twp_rate` | numeric |  |
| `pressure_sack_percent` | numeric |  |
| `no_pressure_attempts` | numeric |  |
| `right_behind_los_attempts_percent` | numeric |  |
| `no_blitz_grades_offense` | numeric |  |
| `attempts` | numeric | The number of pass attempts as defined by the NFL. |
| `blitz_scrambles` | numeric |  |
| `right_deep_avg_depth_of_target` | numeric |  |
| `behind_los_sacks` | numeric |  |
| `center_deep_yards` | numeric |  |
| `short_dropbacks` | numeric |  |
| `no_screen_big_time_throws` | numeric |  |
| `left_deep_sack_percent` | numeric |  |
| `center_behind_los_avg_time_to_throw` | numeric |  |
| `blitz_dropbacks_percent` | numeric |  |
| `behind_los_avg_depth_of_target` | numeric |  |
| `left_behind_los_epa` | numeric |  |
| `medium_twp_rate` | numeric |  |
| `left_short_def_gen_pressures` | numeric |  |
| `npa_drop_rate` | numeric |  |
| `medium_turnover_worthy_plays` | numeric |  |
| `behind_los_twp_rate` | numeric |  |
| `left_behind_los_thrown_aways` | numeric |  |
| `left_short_bats` | numeric |  |
| `center_medium_drop_rate` | numeric |  |
| `npa_attempts` | numeric |  |
| `right_deep_bats` | numeric |  |
| `screen_qb_rating` | numeric |  |
| `medium_yards` | numeric |  |
| `center_deep_grades_pass` | numeric |  |
| `center_medium_passing_snaps` | numeric |  |
| `center_behind_los_first_downs` | numeric |  |
| `npa_grades_offense_penalty` | numeric |  |
| `no_pressure_qb_rating` | numeric |  |
| `center_medium_interceptions` | numeric |  |
| `pa_bats` | numeric |  |
| `pa_attempts` | numeric |  |
| `behind_los_grades_pass` | numeric |  |
| `no_blitz_passing_snaps` | numeric |  |
| `npa_def_gen_pressures` | numeric |  |
| `left_short_hit_as_threw` | numeric |  |
| `deep_qb_rating` | numeric |  |
| `no_blitz_aimed_passes` | numeric |  |
| `blitz_yards` | numeric |  |
| `no_screen_avg_time_to_throw` | numeric |  |
| `center_deep_bats` | numeric |  |
| `behind_los_ypa` | numeric |  |
| `pa_yards` | numeric |  |
| `right_short_interceptions` | numeric |  |
| `left_deep_interceptions` | numeric |  |
| `right_medium_sacks` | numeric |  |
| `npa_positive_epa_percent` | numeric |  |
| `right_behind_los_turnover_worthy_plays` | numeric |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `no_screen_scrambles` | numeric |  |
| `pa_turnover_worthy_plays` | numeric |  |
| `right_medium_spikes` | numeric |  |
| `behind_los_hit_as_threw` | numeric |  |
| `no_blitz_attempts` | numeric |  |
| `pressure_accuracy_percent` | numeric |  |
| `left_medium_pressure_to_sack_rate` | numeric |  |
| `medium_dropbacks` | numeric |  |
| `deep_hit_as_threw` | numeric |  |
| `no_blitz_hit_as_threw` | numeric |  |
| `right_medium_completion_percent` | numeric |  |
| `pressure_hit_as_threw` | numeric |  |
| `short_epa` | numeric |  |
| `deep_drop_rate` | numeric |  |
| `blitz_pressure_to_sack_rate` | numeric |  |
| `pa_aimed_passes` | numeric |  |
| `deep_scrambles` | numeric |  |
| `pa_grades_offense` | numeric |  |
| `screen_positive_epa_percent` | numeric |  |
| `left_deep_completion_percent` | numeric |  |
| `no_screen_dropbacks` | numeric |  |
| `right_short_epa` | numeric |  |
| `no_screen_dropbacks_percent` | numeric |  |
| `no_blitz_btt_rate` | numeric |  |
| `medium_attempts_percent` | numeric |  |
| `right_behind_los_completion_percent` | numeric |  |
| `left_medium_hit_as_threw` | numeric |  |
| `left_behind_los_pressure_to_sack_rate` | numeric |  |
| `right_short_def_gen_pressures` | numeric |  |
| `no_blitz_ypa` | numeric |  |
| `right_short_attempts_percent` | numeric |  |
| `center_short_aimed_passes` | numeric |  |
| `npa_sack_percent` | numeric |  |
| `short_hit_as_threw` | numeric |  |
| `blitz_grades_run` | numeric |  |
| `right_medium_turnover_worthy_plays` | numeric |  |
| `left_short_epa` | numeric |  |
| `center_behind_los_pressure_to_sack_rate` | numeric |  |
| `pa_positive_epa_percent` | numeric |  |
| `pressure_interceptions` | numeric |  |
| `right_behind_los_ypa` | numeric |  |
| `deep_interceptions` | numeric |  |
| `left_medium_twp_rate` | numeric |  |
| `blitz_grades_hands_fumble` | numeric |  |
| `passing_snaps` | numeric |  |
| `pa_grades_run_block` | numeric |  |
| `pressure_to_sack_rate` | numeric |  |
| `ypa` | numeric |  |
| `right_behind_los_big_time_throws` | numeric |  |
| `center_medium_sack_percent` | numeric |  |
| `drops` | numeric | Throws dropped |
| `center_deep_thrown_aways` | numeric |  |
| `short_thrown_aways` | numeric |  |
| `left_short_btt_rate` | numeric |  |
| `no_screen_turnover_worthy_plays` | numeric |  |
| `no_pressure_grades_offense_penalty` | numeric |  |
| `right_medium_accuracy_percent` | numeric |  |
| `position` | character | Primary position as reported by NFL.com |
| `short_def_gen_pressures` | numeric |  |
| `center_deep_dropbacks` | numeric |  |
| `center_medium_accuracy_percent` | numeric |  |
| `blitz_grades_offense` | numeric |  |
| `right_behind_los_yards` | numeric |  |
| `right_medium_attempts` | numeric |  |
| `grades_hands_fumble` | numeric |  |
| `left_medium_attempts` | numeric |  |
| `npa_epa` | numeric |  |
| `no_screen_avg_depth_of_target` | numeric |  |
| `medium_accuracy_percent` | numeric |  |
| `left_medium_drops` | numeric |  |
| `pa_grades_hands_fumble` | numeric |  |
| `no_screen_completion_percent` | numeric |  |
| `left_short_avg_time_to_throw` | numeric |  |
| `pa_hit_as_threw` | numeric |  |
| `medium_passing_snaps` | numeric |  |
| `center_short_epa` | numeric |  |
| `left_behind_los_passing_snaps` | numeric |  |
| `left_deep_pressure_to_sack_rate` | numeric |  |
| `avg_time_to_throw` | numeric | Average time elapsed from the time of snap to throw on every pass attempt for a passer (sacks excluded). |
| `pa_grades_offense_penalty` | numeric |  |
| `deep_bats` | numeric |  |
| `npa_dropbacks_percent` | numeric |  |
| `left_medium_passing_snaps` | numeric |  |
| `pressure_def_gen_pressures` | numeric |  |
| `left_medium_scrambles` | numeric |  |
| `npa_grades_pass` | numeric |  |
| `pressure_grades_pass` | numeric |  |
| `right_deep_completion_percent` | numeric |  |
| `big_time_throws` | numeric |  |
| `screen_grades_run` | numeric |  |
| `left_short_drops` | numeric |  |
| `screen_first_downs` | numeric |  |
| `npa_completion_percent` | numeric |  |
| `left_deep_accuracy_percent` | numeric |  |
| `medium_positive_epa_percent` | numeric |  |
| `no_blitz_qb_rating` | numeric |  |
| `blitz_hit_as_threw` | numeric |  |
| `behind_los_turnover_worthy_plays` | numeric |  |
| `right_medium_epa` | numeric |  |
| `right_deep_dropbacks` | numeric |  |
| `deep_ypa` | numeric |  |
| `center_medium_completions` | numeric |  |
| `left_medium_avg_time_to_throw` | numeric |  |
| `left_short_sacks` | numeric |  |
| `left_behind_los_spikes` | numeric |  |
| `no_screen_grades_run_block` | numeric |  |
| `pressure_ypa` | numeric |  |
| `left_deep_def_gen_pressures` | numeric |  |
| `no_screen_hit_as_threw` | numeric |  |
| `center_short_qb_rating` | numeric |  |
| `center_deep_interceptions` | numeric |  |
| `right_deep_completions` | numeric |  |
| `center_behind_los_sacks` | numeric |  |
| `screen_turnover_worthy_plays` | numeric |  |
| `deep_completions` | numeric |  |
| `left_medium_attempts_percent` | numeric |  |
| `pressure_avg_time_to_throw` | numeric |  |
| `short_yards` | numeric |  |
| `behind_los_qb_rating` | numeric |  |
| `right_short_sacks` | numeric |  |
| `right_behind_los_thrown_aways` | numeric |  |
| `center_short_spikes` | numeric |  |
| `center_behind_los_hit_as_threw` | numeric |  |
| `no_blitz_completions` | numeric |  |
| `center_deep_turnover_worthy_plays` | numeric |  |
| `left_medium_sack_percent` | numeric |  |
| `behind_los_completion_percent` | numeric |  |
| `left_deep_attempts_percent` | numeric |  |
| `no_pressure_sack_percent` | numeric |  |
| `center_deep_big_time_throws` | numeric |  |
| `npa_avg_depth_of_target` | numeric |  |
| `npa_dropbacks` | numeric |  |
| `player` | character | Player name |
| `left_medium_avg_depth_of_target` | numeric |  |
| `no_blitz_accuracy_percent` | numeric |  |
| `behind_los_thrown_aways` | numeric |  |
| `center_medium_def_gen_pressures` | numeric |  |
| `short_avg_time_to_throw` | numeric |  |
| `left_deep_twp_rate` | numeric |  |
| `center_behind_los_epa` | numeric |  |
| `left_behind_los_big_time_throws` | numeric |  |
| `pa_drops` | numeric |  |
| `no_pressure_pressure_to_sack_rate` | character |  |
| `right_medium_yards` | numeric |  |
| `pa_btt_rate` | numeric |  |
| `no_pressure_turnover_worthy_plays` | numeric |  |
| `pressure_first_downs` | numeric |  |
| `positive_epa_percent` | numeric |  |
| `right_medium_drop_rate` | numeric |  |
| `blitz_passing_snaps` | numeric |  |
| `center_medium_big_time_throws` | numeric |  |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `left_short_completion_percent` | numeric |  |
| `screen_big_time_throws` | numeric |  |
| `left_short_drop_rate` | numeric |  |
| `left_behind_los_scrambles` | numeric |  |
| `left_medium_btt_rate` | numeric |  |
| `screen_aimed_passes` | numeric |  |
| `right_deep_hit_as_threw` | numeric |  |
| `behind_los_bats` | numeric |  |
| `no_pressure_twp_rate` | numeric |  |
| `npa_touchdowns` | numeric |  |
| `no_blitz_sacks` | numeric |  |
| `short_first_downs` | numeric |  |
| `left_deep_attempts` | numeric |  |
| `right_behind_los_attempts` | numeric |  |
| `screen_attempts` | numeric |  |
| `screen_dropbacks` | numeric |  |
| `right_deep_sacks` | numeric |  |
| `left_behind_los_attempts_percent` | numeric |  |
| `behind_los_big_time_throws` | numeric |  |
| `center_deep_pressure_to_sack_rate` | numeric |  |
| `left_medium_big_time_throws` | numeric |  |
| `right_behind_los_bats` | numeric |  |
| `left_medium_def_gen_pressures` | numeric |  |
| `no_screen_yards` | numeric |  |
| `right_deep_turnover_worthy_plays` | numeric |  |
| `left_short_spikes` | numeric |  |
| `left_medium_aimed_passes` | numeric |  |
| `left_behind_los_twp_rate` | numeric |  |
| `short_grades_pass` | numeric |  |
| `right_short_drops` | numeric |  |
| `right_short_avg_time_to_throw` | numeric |  |
| `pa_scrambles` | numeric |  |
| `right_medium_thrown_aways` | numeric |  |
| `no_pressure_accuracy_percent` | numeric |  |
| `center_short_sack_percent` | numeric |  |
| `right_short_passing_snaps` | numeric |  |
| `center_short_def_gen_pressures` | numeric |  |
| `center_medium_positive_epa_percent` | numeric |  |
| `medium_drops` | numeric |  |
| `center_medium_aimed_passes` | numeric |  |
| `center_short_sacks` | numeric |  |
| `pa_completion_percent` | numeric |  |
| `deep_attempts_percent` | numeric |  |
| `center_behind_los_def_gen_pressures` | numeric |  |
| `left_short_passing_snaps` | numeric |  |
| `center_medium_grades_pass` | numeric |  |
| `center_deep_avg_time_to_throw` | numeric |  |
| `pa_avg_depth_of_target` | numeric |  |
| `pa_interceptions` | numeric |  |
| `no_screen_accuracy_percent` | numeric |  |
| `behind_los_positive_epa_percent` | numeric |  |
| `no_screen_spikes` | numeric |  |
| `center_short_completion_percent` | numeric |  |
| `pa_dropbacks` | numeric |  |
| `left_deep_hit_as_threw` | numeric |  |
| `center_short_grades_pass` | numeric |  |
| `left_short_attempts_percent` | numeric |  |
| `left_deep_scrambles` | numeric |  |
| `no_blitz_twp_rate` | numeric |  |
| `no_pressure_hit_as_threw` | numeric |  |
| `right_short_completions` | numeric |  |
| `center_behind_los_aimed_passes` | numeric |  |
| `right_short_scrambles` | numeric |  |
| `center_medium_qb_rating` | numeric |  |
| `no_pressure_grades_pass` | numeric |  |
| `npa_big_time_throws` | numeric |  |
| `deep_avg_time_to_throw` | numeric |  |
| `no_screen_sack_percent` | numeric |  |
| `avg_depth_of_target` | numeric |  |
| `no_pressure_positive_epa_percent` | numeric |  |
| `turnover_worthy_plays` | numeric |  |
| `epa` | numeric | Expected points added (EPA) by the posteam for the given play. |
| `pressure_spikes` | numeric |  |
| `pressure_grades_hands_fumble` | numeric |  |
| `left_deep_drops` | numeric |  |
| `right_behind_los_def_gen_pressures` | numeric |  |
| `pa_epa` | numeric |  |
| `pressure_qb_rating` | numeric |  |
| `center_deep_positive_epa_percent` | numeric |  |
| `center_deep_hit_as_threw` | numeric |  |
| `no_screen_attempts` | numeric |  |
| `pa_passing_snaps` | numeric |  |
| `aimed_passes` | numeric |  |
| `blitz_bats` | numeric |  |
| `pressure_touchdowns` | numeric |  |
| `npa_completions` | numeric |  |
| `short_spikes` | numeric |  |
| `pressure_thrown_aways` | numeric |  |
| `right_behind_los_completions` | numeric |  |
| `no_blitz_interceptions` | numeric |  |
| `center_behind_los_drop_rate` | numeric |  |
| `short_drops` | numeric |  |
| `deep_aimed_passes` | numeric |  |
| `right_medium_avg_depth_of_target` | numeric |  |
| `short_pressure_to_sack_rate` | numeric |  |
| `screen_touchdowns` | numeric |  |
| `center_deep_qb_rating` | numeric |  |
| `left_medium_completions` | numeric |  |
| `center_deep_sacks` | numeric |  |
| `left_deep_big_time_throws` | numeric |  |
| `blitz_dropbacks` | numeric |  |
| `center_medium_first_downs` | numeric |  |
| `center_medium_dropbacks` | numeric |  |
| `blitz_def_gen_pressures` | numeric |  |
| `npa_first_downs` | numeric |  |
| `no_screen_touchdowns` | numeric |  |
| `right_deep_def_gen_pressures` | numeric |  |
| `left_short_touchdowns` | numeric |  |
| `medium_drop_rate` | numeric |  |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `npa_yards` | numeric |  |
| `left_medium_ypa` | numeric |  |
| `right_medium_passing_snaps` | numeric |  |
| `no_screen_ypa` | numeric |  |
| `no_blitz_big_time_throws` | numeric |  |
| `center_short_scrambles` | numeric |  |
| `npa_turnover_worthy_plays` | numeric |  |
| `left_behind_los_turnover_worthy_plays` | numeric |  |
| `center_medium_yards` | numeric |  |
| `center_deep_epa` | numeric |  |
| `npa_interceptions` | numeric |  |
| `left_medium_interceptions` | numeric |  |
| `right_deep_interceptions` | numeric |  |
| `no_pressure_avg_depth_of_target` | numeric |  |
| `touchdowns` | numeric |  |
| `medium_completion_percent` | numeric |  |
| `right_behind_los_grades_pass` | numeric |  |
| `deep_positive_epa_percent` | numeric |  |
| `no_screen_pressure_to_sack_rate` | numeric |  |
| `behind_los_accuracy_percent` | numeric |  |
| `center_deep_scrambles` | numeric |  |
| `center_short_twp_rate` | numeric |  |
| `behind_los_touchdowns` | numeric |  |
| `left_medium_qb_rating` | numeric |  |
| `no_pressure_yards` | numeric |  |
| `screen_sacks` | numeric |  |
| `def_gen_pressures` | numeric |  |
| `right_medium_completions` | numeric |  |
| `right_behind_los_drops` | numeric |  |
| `left_behind_los_first_downs` | numeric |  |
| `blitz_twp_rate` | numeric |  |
| `short_qb_rating` | numeric |  |
| `blitz_accuracy_percent` | numeric |  |
| `center_medium_btt_rate` | numeric |  |
| `no_pressure_big_time_throws` | numeric |  |
| `left_deep_grades_pass` | numeric |  |
| `no_blitz_grades_pass_block` | numeric |  |
| `screen_grades_hands_drop` | numeric |  |
| `screen_grades_pass_block` | numeric |  |
| `no_screen_grades_pass_block` | numeric |  |
| `pressure_grades_pass_route` | numeric |  |
| `no_screen_grades_pass_route` | numeric |  |
| `pa_grades_hands_drop` | numeric |  |
| `blitz_grades_pass_block` | numeric |  |
| `screen_grades_pass_route` | numeric |  |
| `no_pressure_grades_pass_block` | numeric |  |
| `npa_grades_pass_block` | numeric |  |
| `pressure_grades_pass_block` | numeric |  |
| `pa_grades_pass_block` | numeric |  |
| `no_pressure_grades_hands_drop` | numeric |  |
| `npa_grades_pass_route` | numeric |  |
| `npa_grades_hands_drop` | numeric |  |
| `pa_grades_pass_route` | numeric |  |
| `blitz_grades_hands_drop` | numeric |  |
| `blitz_grades_pass_route` | numeric |  |
| `no_screen_grades_hands_drop` | numeric |  |
| `no_pressure_grades_pass_route` | numeric |  |
| `no_blitz_grades_pass_route` | numeric |  |
| `pressure_grades_hands_drop` | numeric |  |
| `no_blitz_grades_hands_drop` | numeric |  |
| `pa_grades_screen_block` | numeric |  |
| `blitz_grades_run_block` | numeric |  |
| `screen_grades_screen_block` | numeric |  |
| `no_pressure_grades_run_block` | numeric |  |
| `no_screen_grades_screen_block` | numeric |  |
| `no_blitz_grades_run_block` | numeric |  |
| `blitz_grades_screen_block` | numeric |  |
| `pressure_grades_screen_block` | numeric |  |
| `no_blitz_grades_screen_block` | numeric |  |
| `npa_grades_screen_block` | numeric |  |
| `pressure_grades_run_block` | numeric |  |
| `no_pressure_grades_screen_block` | numeric |  |
| `pa_grades_defense_penalty` | character |  |
| `screen_grades_run_defense` | character |  |
| `npa_grades_defense_penalty` | numeric |  |
| `pa_grades_run_defense` | character |  |
| `pa_grades_defense` | character |  |
| `npa_grades_defense` | numeric |  |
| `screen_grades_defense` | character |  |
| `screen_grades_defense_penalty` | character |  |
| `no_screen_grades_run_defense` | numeric |  |
| `no_screen_grades_defense` | numeric |  |
| `no_screen_grades_defense_penalty` | numeric |  |
| `npa_grades_run_defense` | numeric |  |

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
| `declined_penalties` | numeric |  |
| `draft_season` | numeric |  |
| `eligible_season` | numeric |  |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `gap_grades_run_block` | numeric |  |
| `gap_run_block_percent` | numeric |  |
| `gap_snap_counts_run_block` | numeric |  |
| `gap_snap_counts_run_block_percent` | numeric |  |
| `gap_snap_counts_run_play` | numeric |  |
| `grades_run_block` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `penalties` | numeric | Total number of penalties. |
| `player` | character | Player name |
| `player_game_count` | numeric |  |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `position` | character | Primary position as reported by NFL.com |
| `run_block_percent` | numeric |  |
| `snap_counts_run_block` | numeric |  |
| `snap_counts_run_play` | numeric |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `zone_grades_run_block` | numeric |  |
| `zone_run_block_percent` | numeric |  |
| `zone_snap_counts_run_block` | numeric |  |
| `zone_snap_counts_run_block_percent` | numeric |  |
| `zone_snap_counts_run_play` | numeric |  |

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
| `true_pass_set_non_spike_pass_block_percentage` | numeric |  |
| `true_pass_set_pressures_allowed` | numeric |  |
| `grades_pass_block` | numeric |  |
| `true_pass_set_pass_block_percent` | numeric |  |
| `pbe` | numeric |  |
| `non_spike_pass_block_percentage` | numeric |  |
| `draft_season` | numeric |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `hits_allowed` | numeric |  |
| `true_pass_set_non_spike_pass_block` | numeric |  |
| `player_game_count` | numeric |  |
| `eligible_season` | numeric |  |
| `hurries_allowed` | numeric |  |
| `true_pass_set_hurries_allowed` | numeric |  |
| `true_pass_set_snap_counts_pass_play` | numeric |  |
| `true_pass_set_hits_allowed` | numeric |  |
| `pressures_allowed` | numeric |  |
| `true_pass_set_pbe` | numeric |  |
| `snap_counts_pass_play` | numeric |  |
| `penalties` | numeric | Total number of penalties. |
| `sacks_allowed` | numeric | Opponent sacks. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric |  |
| `position` | character | Primary position as reported by NFL.com |
| `true_pass_set_grades_pass_block` | numeric |  |
| `non_spike_pass_block` | numeric |  |
| `true_pass_set_snap_counts_pass_block` | numeric |  |
| `player` | character | Player name |
| `true_pass_set_sacks_allowed` | numeric |  |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `snap_counts_pass_block` | numeric |  |
| `pass_block_percent` | numeric |  |
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
| `passing_snaps` | numeric |  |
| `pressure_to_sack_rate` | numeric |  |
| `ypa` | numeric |  |
| `drops` | numeric | Throws dropped |
| `position` | character | Primary position as reported by NFL.com |
| `grades_hands_fumble` | numeric |  |
| `avg_time_to_throw` | numeric | Average time elapsed from the time of snap to throw on every pass attempt for a passer (sacks excluded). |
| `big_time_throws` | numeric |  |
| `player` | character | Player name |
| `positive_epa_percent` | numeric |  |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `avg_depth_of_target` | numeric |  |
| `turnover_worthy_plays` | numeric |  |
| `epa` | numeric | Expected points added (EPA) by the posteam for the given play. |
| `aimed_passes` | numeric |  |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `touchdowns` | numeric |  |
| `def_gen_pressures` | numeric |  |

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
| `touchbacks` | numeric |  |
| `attempts_with_hangtime` | numeric |  |
| `draft_season` | numeric |  |
| `percent_returned` | numeric |  |
| `fair_catches` | numeric |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `player_game_count` | numeric |  |
| `eligible_season` | numeric |  |
| `average_net_yards` | numeric |  |
| `yards` | numeric | The number of receiving yards |
| `average_hangtime` | numeric |  |
| `total_net_yards` | numeric |  |
| `penalties` | numeric | Total number of penalties. |
| `attempts` | numeric | The number of pass attempts as defined by the NFL. |
| `inside_twenties` | numeric |  |
| `out_of_bounds` | numeric | 1 if play description contains ran ob, pushed ob, or sacked ob; 0 otherwise. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `average_yards_per_return` | numeric |  |
| `total_hangtime` | numeric |  |
| `declined_penalties` | numeric |  |
| `returns` | numeric |  |
| `position` | character | Primary position as reported by NFL.com |
| `long` | numeric |  |
| `blocks` | numeric | Total blocks. |
| `average_yards_per_attempt` | numeric |  |
| `player` | character | Player name |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_punter` | numeric |  |
| `return_yards` | numeric | Yards gained by the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `downeds` | numeric |  |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `snaps` | numeric |  |

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
| `left_behind_los_accuracy_percent` | numeric |  |
| `left_short_scrambles` | numeric |  |
| `center_short_first_downs` | numeric |  |
| `right_short_bats` | numeric |  |
| `right_behind_los_positive_epa_percent` | numeric |  |
| `left_behind_los_completions` | numeric |  |
| `left_medium_dropbacks` | numeric |  |
| `right_medium_grades_pass` | numeric |  |
| `right_medium_hit_as_threw` | numeric |  |
| `behind_los_attempts_percent` | numeric |  |
| `right_short_turnover_worthy_plays` | numeric |  |
| `right_medium_qb_rating` | numeric |  |
| `deep_twp_rate` | numeric |  |
| `center_medium_sacks` | numeric |  |
| `medium_interceptions` | numeric |  |
| `left_deep_completions` | numeric |  |
| `behind_los_spikes` | numeric |  |
| `medium_aimed_passes` | numeric |  |
| `center_behind_los_drops` | numeric |  |
| `center_behind_los_interceptions` | numeric |  |
| `behind_los_dropbacks` | numeric |  |
| `left_behind_los_interceptions` | numeric |  |
| `left_deep_first_downs` | numeric |  |
| `deep_passing_snaps` | numeric |  |
| `center_short_btt_rate` | numeric |  |
| `center_medium_thrown_aways` | numeric |  |
| `center_deep_avg_depth_of_target` | numeric |  |
| `center_short_drops` | numeric |  |
| `center_deep_first_downs` | numeric |  |
| `left_medium_completion_percent` | numeric |  |
| `center_deep_attempts` | numeric |  |
| `center_behind_los_attempts` | numeric |  |
| `right_short_positive_epa_percent` | numeric |  |
| `center_deep_aimed_passes` | numeric |  |
| `right_deep_grades_pass` | numeric |  |
| `right_medium_big_time_throws` | numeric |  |
| `deep_sack_percent` | numeric |  |
| `center_medium_pressure_to_sack_rate` | numeric |  |
| `deep_sacks` | numeric |  |
| `right_medium_ypa` | numeric |  |
| `left_deep_drop_rate` | numeric |  |
| `right_medium_attempts_percent` | numeric |  |
| `right_deep_btt_rate` | numeric |  |
| `center_short_attempts_percent` | numeric |  |
| `deep_drops` | numeric |  |
| `center_behind_los_big_time_throws` | numeric |  |
| `center_behind_los_touchdowns` | numeric |  |
| `right_behind_los_twp_rate` | numeric |  |
| `center_deep_completions` | numeric |  |
| `center_short_attempts` | numeric |  |
| `left_behind_los_sack_percent` | numeric |  |
| `center_short_pressure_to_sack_rate` | numeric |  |
| `deep_touchdowns` | numeric |  |
| `center_behind_los_accuracy_percent` | numeric |  |
| `center_deep_drop_rate` | numeric |  |
| `right_short_first_downs` | numeric |  |
| `right_behind_los_first_downs` | numeric |  |
| `short_interceptions` | numeric |  |
| `center_medium_scrambles` | numeric |  |
| `left_behind_los_sacks` | numeric |  |
| `left_short_ypa` | numeric |  |
| `left_short_qb_rating` | numeric |  |
| `right_behind_los_qb_rating` | numeric |  |
| `draft_season` | numeric |  |
| `right_behind_los_dropbacks` | numeric |  |
| `behind_los_def_gen_pressures` | numeric |  |
| `right_short_thrown_aways` | numeric |  |
| `deep_def_gen_pressures` | numeric |  |
| `right_short_btt_rate` | numeric |  |
| `center_behind_los_positive_epa_percent` | numeric |  |
| `left_behind_los_grades_pass` | numeric |  |
| `deep_grades_pass` | numeric |  |
| `center_short_turnover_worthy_plays` | numeric |  |
| `center_behind_los_scrambles` | numeric |  |
| `right_short_aimed_passes` | numeric |  |
| `center_short_dropbacks` | numeric |  |
| `medium_epa` | numeric |  |
| `right_short_ypa` | numeric |  |
| `center_medium_ypa` | numeric |  |
| `medium_attempts` | numeric |  |
| `right_deep_accuracy_percent` | numeric |  |
| `behind_los_scrambles` | numeric |  |
| `center_behind_los_completion_percent` | numeric |  |
| `left_behind_los_btt_rate` | numeric |  |
| `right_behind_los_btt_rate` | numeric |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `short_touchdowns` | numeric |  |
| `center_medium_drops` | numeric |  |
| `left_behind_los_aimed_passes` | numeric |  |
| `deep_attempts` | numeric |  |
| `right_behind_los_sack_percent` | numeric |  |
| `center_short_positive_epa_percent` | numeric |  |
| `deep_avg_depth_of_target` | numeric |  |
| `left_deep_btt_rate` | numeric |  |
| `medium_scrambles` | numeric |  |
| `center_behind_los_avg_depth_of_target` | numeric |  |
| `short_attempts_percent` | numeric |  |
| `right_behind_los_sacks` | numeric |  |
| `center_medium_avg_time_to_throw` | numeric |  |
| `left_short_pressure_to_sack_rate` | numeric |  |
| `center_deep_sack_percent` | numeric |  |
| `center_deep_ypa` | numeric |  |
| `left_behind_los_hit_as_threw` | numeric |  |
| `medium_big_time_throws` | numeric |  |
| `deep_thrown_aways` | numeric |  |
| `right_short_accuracy_percent` | numeric |  |
| `left_deep_turnover_worthy_plays` | numeric |  |
| `center_medium_bats` | numeric |  |
| `right_short_grades_pass` | numeric |  |
| `right_deep_spikes` | numeric |  |
| `left_deep_passing_snaps` | numeric |  |
| `center_medium_twp_rate` | numeric |  |
| `right_behind_los_passing_snaps` | numeric |  |
| `left_deep_qb_rating` | numeric |  |
| `right_deep_drop_rate` | numeric |  |
| `left_behind_los_drop_rate` | numeric |  |
| `left_medium_drop_rate` | numeric |  |
| `right_deep_attempts` | numeric |  |
| `left_deep_spikes` | numeric |  |
| `center_behind_los_dropbacks` | numeric |  |
| `right_deep_big_time_throws` | numeric |  |
| `medium_hit_as_threw` | numeric |  |
| `right_short_dropbacks` | numeric |  |
| `medium_def_gen_pressures` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `left_short_positive_epa_percent` | numeric |  |
| `left_short_attempts` | numeric |  |
| `center_deep_twp_rate` | numeric |  |
| `right_medium_avg_time_to_throw` | numeric |  |
| `left_behind_los_qb_rating` | numeric |  |
| `left_behind_los_dropbacks` | numeric |  |
| `deep_dropbacks` | numeric |  |
| `right_behind_los_scrambles` | numeric |  |
| `behind_los_interceptions` | numeric |  |
| `right_deep_drops` | numeric |  |
| `right_deep_yards` | numeric |  |
| `right_short_hit_as_threw` | numeric |  |
| `right_short_avg_depth_of_target` | numeric |  |
| `short_bats` | numeric |  |
| `right_deep_twp_rate` | numeric |  |
| `right_behind_los_pressure_to_sack_rate` | numeric |  |
| `deep_spikes` | numeric |  |
| `center_medium_spikes` | numeric |  |
| `behind_los_aimed_passes` | numeric |  |
| `right_behind_los_touchdowns` | numeric |  |
| `right_medium_pressure_to_sack_rate` | numeric |  |
| `medium_thrown_aways` | numeric |  |
| `short_sacks` | numeric |  |
| `center_behind_los_twp_rate` | numeric |  |
| `left_behind_los_attempts` | numeric |  |
| `short_aimed_passes` | numeric |  |
| `short_completions` | numeric |  |
| `right_short_pressure_to_sack_rate` | numeric |  |
| `medium_avg_depth_of_target` | numeric |  |
| `left_behind_los_positive_epa_percent` | numeric |  |
| `center_medium_attempts_percent` | numeric |  |
| `left_medium_touchdowns` | numeric |  |
| `center_short_bats` | numeric |  |
| `left_deep_avg_time_to_throw` | numeric |  |
| `center_behind_los_passing_snaps` | numeric |  |
| `center_short_yards` | numeric |  |
| `right_medium_btt_rate` | numeric |  |
| `right_medium_scrambles` | numeric |  |
| `medium_btt_rate` | numeric |  |
| `center_behind_los_btt_rate` | numeric |  |
| `center_short_avg_depth_of_target` | numeric |  |
| `left_medium_sacks` | numeric |  |
| `right_short_yards` | numeric |  |
| `left_medium_first_downs` | numeric |  |
| `right_medium_positive_epa_percent` | numeric |  |
| `left_short_big_time_throws` | numeric |  |
| `deep_turnover_worthy_plays` | numeric |  |
| `left_deep_bats` | numeric |  |
| `left_medium_turnover_worthy_plays` | numeric |  |
| `base_dropbacks` | numeric |  |
| `center_deep_drops` | numeric |  |
| `center_medium_avg_depth_of_target` | numeric |  |
| `center_behind_los_ypa` | numeric |  |
| `right_short_attempts` | numeric |  |
| `center_medium_completion_percent` | numeric |  |
| `right_short_sack_percent` | numeric |  |
| `left_behind_los_avg_depth_of_target` | numeric |  |
| `center_medium_epa` | numeric |  |
| `center_behind_los_sack_percent` | numeric |  |
| `medium_completions` | numeric |  |
| `medium_sack_percent` | numeric |  |
| `left_short_grades_pass` | numeric |  |
| `deep_btt_rate` | numeric |  |
| `left_short_accuracy_percent` | numeric |  |
| `short_twp_rate` | numeric |  |
| `short_passing_snaps` | numeric |  |
| `left_deep_sacks` | numeric |  |
| `short_scrambles` | numeric |  |
| `center_medium_turnover_worthy_plays` | numeric |  |
| `behind_los_drop_rate` | numeric |  |
| `right_behind_los_epa` | numeric |  |
| `right_medium_aimed_passes` | numeric |  |
| `short_accuracy_percent` | numeric |  |
| `player_game_count` | numeric |  |
| `short_btt_rate` | numeric |  |
| `right_short_touchdowns` | numeric |  |
| `center_short_completions` | numeric |  |
| `right_short_spikes` | numeric |  |
| `behind_los_first_downs` | numeric |  |
| `right_medium_def_gen_pressures` | numeric |  |
| `deep_pressure_to_sack_rate` | numeric |  |
| `medium_qb_rating` | numeric |  |
| `short_completion_percent` | numeric |  |
| `right_deep_attempts_percent` | numeric |  |
| `eligible_season` | numeric |  |
| `short_big_time_throws` | numeric |  |
| `behind_los_attempts` | numeric |  |
| `center_behind_los_yards` | numeric |  |
| `center_behind_los_bats` | numeric |  |
| `center_behind_los_qb_rating` | numeric |  |
| `right_deep_pressure_to_sack_rate` | numeric |  |
| `center_short_accuracy_percent` | numeric |  |
| `left_behind_los_bats` | numeric |  |
| `center_deep_passing_snaps` | numeric |  |
| `right_behind_los_hit_as_threw` | numeric |  |
| `medium_bats` | numeric |  |
| `right_medium_first_downs` | numeric |  |
| `medium_spikes` | numeric |  |
| `left_deep_thrown_aways` | numeric |  |
| `right_deep_epa` | numeric |  |
| `center_medium_hit_as_threw` | numeric |  |
| `left_behind_los_completion_percent` | numeric |  |
| `center_short_ypa` | numeric |  |
| `right_short_drop_rate` | numeric |  |
| `right_behind_los_accuracy_percent` | numeric |  |
| `left_short_yards` | numeric |  |
| `deep_accuracy_percent` | numeric |  |
| `right_behind_los_avg_depth_of_target` | numeric |  |
| `center_behind_los_thrown_aways` | numeric |  |
| `center_behind_los_attempts_percent` | numeric |  |
| `right_deep_first_downs` | numeric |  |
| `left_short_completions` | numeric |  |
| `deep_completion_percent` | numeric |  |
| `left_deep_positive_epa_percent` | numeric |  |
| `right_deep_sack_percent` | numeric |  |
| `behind_los_pressure_to_sack_rate` | numeric |  |
| `right_deep_touchdowns` | numeric |  |
| `left_behind_los_def_gen_pressures` | numeric |  |
| `left_behind_los_drops` | numeric |  |
| `deep_epa` | numeric |  |
| `left_deep_ypa` | numeric |  |
| `medium_touchdowns` | numeric |  |
| `left_medium_grades_pass` | numeric |  |
| `right_behind_los_spikes` | numeric |  |
| `medium_grades_pass` | numeric |  |
| `behind_los_passing_snaps` | numeric |  |
| `left_short_first_downs` | numeric |  |
| `center_short_passing_snaps` | numeric |  |
| `center_deep_accuracy_percent` | numeric |  |
| `right_deep_positive_epa_percent` | numeric |  |
| `right_medium_sack_percent` | numeric |  |
| `right_deep_qb_rating` | numeric |  |
| `right_short_completion_percent` | numeric |  |
| `right_behind_los_interceptions` | numeric |  |
| `behind_los_yards` | numeric |  |
| `center_behind_los_grades_pass` | numeric |  |
| `left_deep_dropbacks` | numeric |  |
| `center_behind_los_spikes` | numeric |  |
| `right_behind_los_aimed_passes` | numeric |  |
| `left_short_interceptions` | numeric |  |
| `right_medium_interceptions` | numeric |  |
| `left_behind_los_yards` | numeric |  |
| `center_deep_btt_rate` | numeric |  |
| `medium_first_downs` | numeric |  |
| `left_short_avg_depth_of_target` | numeric |  |
| `left_behind_los_ypa` | numeric |  |
| `short_attempts` | numeric |  |
| `right_medium_bats` | numeric |  |
| `left_behind_los_touchdowns` | numeric |  |
| `right_medium_dropbacks` | numeric |  |
| `short_turnover_worthy_plays` | numeric |  |
| `right_deep_thrown_aways` | numeric |  |
| `right_behind_los_drop_rate` | numeric |  |
| `right_short_qb_rating` | numeric |  |
| `medium_sacks` | numeric |  |
| `center_deep_attempts_percent` | numeric |  |
| `left_short_dropbacks` | numeric |  |
| `behind_los_drops` | numeric |  |
| `short_sack_percent` | numeric |  |
| `left_medium_spikes` | numeric |  |
| `left_medium_accuracy_percent` | numeric |  |
| `behind_los_completions` | numeric |  |
| `behind_los_sack_percent` | numeric |  |
| `left_deep_yards` | numeric |  |
| `left_short_aimed_passes` | numeric |  |
| `center_behind_los_completions` | numeric |  |
| `left_short_thrown_aways` | numeric |  |
| `left_short_sack_percent` | numeric |  |
| `center_short_thrown_aways` | numeric |  |
| `center_short_interceptions` | numeric |  |
| `short_avg_depth_of_target` | numeric |  |
| `left_deep_touchdowns` | numeric |  |
| `deep_yards` | numeric |  |
| `center_behind_los_turnover_worthy_plays` | numeric |  |
| `medium_ypa` | numeric |  |
| `left_medium_epa` | numeric |  |
| `left_deep_avg_depth_of_target` | numeric |  |
| `deep_first_downs` | numeric |  |
| `short_drop_rate` | numeric |  |
| `left_medium_bats` | numeric |  |
| `center_deep_spikes` | numeric |  |
| `center_short_hit_as_threw` | numeric |  |
| `left_medium_positive_epa_percent` | numeric |  |
| `center_deep_touchdowns` | numeric |  |
| `right_deep_ypa` | numeric |  |
| `right_short_big_time_throws` | numeric |  |
| `center_short_big_time_throws` | numeric |  |
| `short_positive_epa_percent` | numeric |  |
| `left_behind_los_avg_time_to_throw` | numeric |  |
| `right_deep_passing_snaps` | numeric |  |
| `right_short_twp_rate` | numeric |  |
| `center_medium_attempts` | numeric |  |
| `right_deep_avg_time_to_throw` | numeric |  |
| `center_deep_def_gen_pressures` | numeric |  |
| `behind_los_epa` | numeric |  |
| `right_medium_twp_rate` | numeric |  |
| `right_deep_aimed_passes` | numeric |  |
| `right_deep_scrambles` | numeric |  |
| `deep_big_time_throws` | numeric |  |
| `left_short_turnover_worthy_plays` | numeric |  |
| `center_short_touchdowns` | numeric |  |
| `right_medium_drops` | numeric |  |
| `left_deep_epa` | numeric |  |
| `short_ypa` | numeric |  |
| `medium_pressure_to_sack_rate` | numeric |  |
| `left_medium_thrown_aways` | numeric |  |
| `right_behind_los_avg_time_to_throw` | numeric |  |
| `behind_los_btt_rate` | numeric |  |
| `medium_avg_time_to_throw` | numeric |  |
| `center_deep_completion_percent` | numeric |  |
| `behind_los_avg_time_to_throw` | numeric |  |
| `right_medium_touchdowns` | numeric |  |
| `center_short_avg_time_to_throw` | numeric |  |
| `left_deep_aimed_passes` | numeric |  |
| `left_medium_yards` | numeric |  |
| `center_medium_touchdowns` | numeric |  |
| `center_short_drop_rate` | numeric |  |
| `left_short_twp_rate` | numeric |  |
| `penalties` | numeric | Total number of penalties. |
| `right_behind_los_attempts_percent` | numeric |  |
| `right_deep_avg_depth_of_target` | numeric |  |
| `behind_los_sacks` | numeric |  |
| `center_deep_yards` | numeric |  |
| `short_dropbacks` | numeric |  |
| `left_deep_sack_percent` | numeric |  |
| `center_behind_los_avg_time_to_throw` | numeric |  |
| `behind_los_avg_depth_of_target` | numeric |  |
| `left_behind_los_epa` | numeric |  |
| `medium_twp_rate` | numeric |  |
| `left_short_def_gen_pressures` | numeric |  |
| `medium_turnover_worthy_plays` | numeric |  |
| `behind_los_twp_rate` | numeric |  |
| `left_behind_los_thrown_aways` | numeric |  |
| `left_short_bats` | numeric |  |
| `center_medium_drop_rate` | numeric |  |
| `right_deep_bats` | numeric |  |
| `medium_yards` | numeric |  |
| `center_deep_grades_pass` | numeric |  |
| `center_medium_passing_snaps` | numeric |  |
| `center_behind_los_first_downs` | numeric |  |
| `center_medium_interceptions` | numeric |  |
| `behind_los_grades_pass` | numeric |  |
| `left_short_hit_as_threw` | numeric |  |
| `deep_qb_rating` | numeric |  |
| `center_deep_bats` | numeric |  |
| `behind_los_ypa` | numeric |  |
| `right_short_interceptions` | numeric |  |
| `left_deep_interceptions` | numeric |  |
| `right_medium_sacks` | numeric |  |
| `right_behind_los_turnover_worthy_plays` | numeric |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `right_medium_spikes` | numeric |  |
| `behind_los_hit_as_threw` | numeric |  |
| `left_medium_pressure_to_sack_rate` | numeric |  |
| `medium_dropbacks` | numeric |  |
| `deep_hit_as_threw` | numeric |  |
| `right_medium_completion_percent` | numeric |  |
| `short_epa` | numeric |  |
| `deep_drop_rate` | numeric |  |
| `declined_penalties` | numeric |  |
| `deep_scrambles` | numeric |  |
| `left_deep_completion_percent` | numeric |  |
| `right_short_epa` | numeric |  |
| `medium_attempts_percent` | numeric |  |
| `right_behind_los_completion_percent` | numeric |  |
| `left_medium_hit_as_threw` | numeric |  |
| `left_behind_los_pressure_to_sack_rate` | numeric |  |
| `right_short_def_gen_pressures` | numeric |  |
| `right_short_attempts_percent` | numeric |  |
| `center_short_aimed_passes` | numeric |  |
| `short_hit_as_threw` | numeric |  |
| `right_medium_turnover_worthy_plays` | numeric |  |
| `left_short_epa` | numeric |  |
| `center_behind_los_pressure_to_sack_rate` | numeric |  |
| `right_behind_los_ypa` | numeric |  |
| `deep_interceptions` | numeric |  |
| `left_medium_twp_rate` | numeric |  |
| `right_behind_los_big_time_throws` | numeric |  |
| `center_medium_sack_percent` | numeric |  |
| `center_deep_thrown_aways` | numeric |  |
| `short_thrown_aways` | numeric |  |
| `left_short_btt_rate` | numeric |  |
| `right_medium_accuracy_percent` | numeric |  |
| `position` | character | Primary position as reported by NFL.com |
| `short_def_gen_pressures` | numeric |  |
| `center_deep_dropbacks` | numeric |  |
| `center_medium_accuracy_percent` | numeric |  |
| `right_behind_los_yards` | numeric |  |
| `right_medium_attempts` | numeric |  |
| `left_medium_attempts` | numeric |  |
| `medium_accuracy_percent` | numeric |  |
| `left_medium_drops` | numeric |  |
| `left_short_avg_time_to_throw` | numeric |  |
| `medium_passing_snaps` | numeric |  |
| `center_short_epa` | numeric |  |
| `left_behind_los_passing_snaps` | numeric |  |
| `left_deep_pressure_to_sack_rate` | numeric |  |
| `deep_bats` | numeric |  |
| `left_medium_passing_snaps` | numeric |  |
| `left_medium_scrambles` | numeric |  |
| `right_deep_completion_percent` | numeric |  |
| `left_short_drops` | numeric |  |
| `left_deep_accuracy_percent` | numeric |  |
| `medium_positive_epa_percent` | numeric |  |
| `behind_los_turnover_worthy_plays` | numeric |  |
| `right_medium_epa` | numeric |  |
| `right_deep_dropbacks` | numeric |  |
| `deep_ypa` | numeric |  |
| `center_medium_completions` | numeric |  |
| `left_medium_avg_time_to_throw` | numeric |  |
| `left_short_sacks` | numeric |  |
| `left_behind_los_spikes` | numeric |  |
| `left_deep_def_gen_pressures` | numeric |  |
| `center_short_qb_rating` | numeric |  |
| `center_deep_interceptions` | numeric |  |
| `right_deep_completions` | numeric |  |
| `center_behind_los_sacks` | numeric |  |
| `deep_completions` | numeric |  |
| `left_medium_attempts_percent` | numeric |  |
| `short_yards` | numeric |  |
| `behind_los_qb_rating` | numeric |  |
| `right_short_sacks` | numeric |  |
| `right_behind_los_thrown_aways` | numeric |  |
| `base_attempts` | numeric |  |
| `center_short_spikes` | numeric |  |
| `center_behind_los_hit_as_threw` | numeric |  |
| `center_deep_turnover_worthy_plays` | numeric |  |
| `left_medium_sack_percent` | numeric |  |
| `behind_los_completion_percent` | numeric |  |
| `left_deep_attempts_percent` | numeric |  |
| `center_deep_big_time_throws` | numeric |  |
| `player` | character | Player name |
| `left_medium_avg_depth_of_target` | numeric |  |
| `behind_los_thrown_aways` | numeric |  |
| `center_medium_def_gen_pressures` | numeric |  |
| `short_avg_time_to_throw` | numeric |  |
| `left_deep_twp_rate` | numeric |  |
| `center_behind_los_epa` | numeric |  |
| `left_behind_los_big_time_throws` | numeric |  |
| `right_medium_yards` | numeric |  |
| `right_medium_drop_rate` | numeric |  |
| `center_medium_big_time_throws` | numeric |  |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `left_short_completion_percent` | numeric |  |
| `left_short_drop_rate` | numeric |  |
| `left_behind_los_scrambles` | numeric |  |
| `left_medium_btt_rate` | numeric |  |
| `right_deep_hit_as_threw` | numeric |  |
| `behind_los_bats` | numeric |  |
| `short_first_downs` | numeric |  |
| `left_deep_attempts` | numeric |  |
| `right_behind_los_attempts` | numeric |  |
| `right_deep_sacks` | numeric |  |
| `left_behind_los_attempts_percent` | numeric |  |
| `behind_los_big_time_throws` | numeric |  |
| `center_deep_pressure_to_sack_rate` | numeric |  |
| `left_medium_big_time_throws` | numeric |  |
| `right_behind_los_bats` | numeric |  |
| `left_medium_def_gen_pressures` | numeric |  |
| `right_deep_turnover_worthy_plays` | numeric |  |
| `left_short_spikes` | numeric |  |
| `left_medium_aimed_passes` | numeric |  |
| `left_behind_los_twp_rate` | numeric |  |
| `short_grades_pass` | numeric |  |
| `right_short_drops` | numeric |  |
| `right_short_avg_time_to_throw` | numeric |  |
| `right_medium_thrown_aways` | numeric |  |
| `center_short_sack_percent` | numeric |  |
| `right_short_passing_snaps` | numeric |  |
| `center_short_def_gen_pressures` | numeric |  |
| `center_medium_positive_epa_percent` | numeric |  |
| `medium_drops` | numeric |  |
| `center_medium_aimed_passes` | numeric |  |
| `center_short_sacks` | numeric |  |
| `deep_attempts_percent` | numeric |  |
| `center_behind_los_def_gen_pressures` | numeric |  |
| `left_short_passing_snaps` | numeric |  |
| `center_medium_grades_pass` | numeric |  |
| `center_deep_avg_time_to_throw` | numeric |  |
| `behind_los_positive_epa_percent` | numeric |  |
| `center_short_completion_percent` | numeric |  |
| `left_deep_hit_as_threw` | numeric |  |
| `center_short_grades_pass` | numeric |  |
| `left_short_attempts_percent` | numeric |  |
| `left_deep_scrambles` | numeric |  |
| `right_short_completions` | numeric |  |
| `center_behind_los_aimed_passes` | numeric |  |
| `right_short_scrambles` | numeric |  |
| `center_medium_qb_rating` | numeric |  |
| `deep_avg_time_to_throw` | numeric |  |
| `left_deep_drops` | numeric |  |
| `right_behind_los_def_gen_pressures` | numeric |  |
| `center_deep_positive_epa_percent` | numeric |  |
| `center_deep_hit_as_threw` | numeric |  |
| `short_spikes` | numeric |  |
| `right_behind_los_completions` | numeric |  |
| `center_behind_los_drop_rate` | numeric |  |
| `short_drops` | numeric |  |
| `deep_aimed_passes` | numeric |  |
| `right_medium_avg_depth_of_target` | numeric |  |
| `short_pressure_to_sack_rate` | numeric |  |
| `center_deep_qb_rating` | numeric |  |
| `left_medium_completions` | numeric |  |
| `center_deep_sacks` | numeric |  |
| `left_deep_big_time_throws` | numeric |  |
| `center_medium_first_downs` | numeric |  |
| `center_medium_dropbacks` | numeric |  |
| `right_deep_def_gen_pressures` | numeric |  |
| `left_short_touchdowns` | numeric |  |
| `medium_drop_rate` | numeric |  |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `left_medium_ypa` | numeric |  |
| `right_medium_passing_snaps` | numeric |  |
| `center_short_scrambles` | numeric |  |
| `left_behind_los_turnover_worthy_plays` | numeric |  |
| `center_medium_yards` | numeric |  |
| `center_deep_epa` | numeric |  |
| `left_medium_interceptions` | numeric |  |
| `right_deep_interceptions` | numeric |  |
| `medium_completion_percent` | numeric |  |
| `right_behind_los_grades_pass` | numeric |  |
| `deep_positive_epa_percent` | numeric |  |
| `behind_los_accuracy_percent` | numeric |  |
| `center_deep_scrambles` | numeric |  |
| `center_short_twp_rate` | numeric |  |
| `behind_los_touchdowns` | numeric |  |
| `left_medium_qb_rating` | numeric |  |
| `right_medium_completions` | numeric |  |
| `right_behind_los_drops` | numeric |  |
| `left_behind_los_first_downs` | numeric |  |
| `short_qb_rating` | numeric |  |
| `center_medium_btt_rate` | numeric |  |
| `left_deep_grades_pass` | numeric |  |

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
| `no_blitz_completion_percent` | numeric |  |
| `grades_offense` | numeric |  |
| `no_pressure_scrambles` | numeric |  |
| `blitz_touchdowns` | numeric |  |
| `pressure_yards` | numeric |  |
| `no_pressure_spikes` | numeric |  |
| `blitz_ypa` | numeric |  |
| `no_blitz_grades_run` | numeric |  |
| `blitz_qb_rating` | numeric |  |
| `no_pressure_thrown_aways` | numeric |  |
| `no_blitz_bats` | numeric |  |
| `no_blitz_drops` | numeric |  |
| `no_pressure_completion_percent` | numeric |  |
| `blitz_aimed_passes` | numeric |  |
| `pressure_grades_run` | numeric |  |
| `no_blitz_sack_percent` | numeric |  |
| `no_pressure_bats` | numeric |  |
| `pressure_completions` | numeric |  |
| `blitz_big_time_throws` | numeric |  |
| `no_blitz_drop_rate` | numeric |  |
| `blitz_spikes` | numeric |  |
| `no_pressure_completions` | numeric |  |
| `draft_season` | numeric |  |
| `no_pressure_passing_snaps` | numeric |  |
| `no_blitz_first_downs` | numeric |  |
| `blitz_avg_time_to_throw` | numeric |  |
| `no_pressure_grades_hands_fumble` | numeric |  |
| `pressure_aimed_passes` | numeric |  |
| `blitz_sacks` | numeric |  |
| `no_pressure_interceptions` | numeric |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `pressure_epa` | numeric |  |
| `blitz_completions` | numeric |  |
| `blitz_attempts` | numeric |  |
| `pressure_twp_rate` | numeric |  |
| `pressure_sacks` | numeric |  |
| `no_blitz_pressure_to_sack_rate` | numeric |  |
| `no_pressure_ypa` | numeric |  |
| `pressure_passing_snaps` | numeric |  |
| `grades_pass` | numeric |  |
| `pressure_bats` | numeric |  |
| `blitz_thrown_aways` | numeric |  |
| `no_pressure_drops` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `pressure_grades_offense_penalty` | numeric |  |
| `pressure_scrambles` | numeric |  |
| `blitz_drop_rate` | numeric |  |
| `pressure_grades_pass_route` | numeric |  |
| `blitz_completion_percent` | numeric |  |
| `no_pressure_first_downs` | numeric |  |
| `blitz_grades_offense_penalty` | numeric |  |
| `no_blitz_epa` | numeric |  |
| `blitz_interceptions` | numeric |  |
| `no_blitz_dropbacks` | numeric |  |
| `no_blitz_grades_pass` | numeric |  |
| `no_blitz_scrambles` | numeric |  |
| `pressure_drop_rate` | numeric |  |
| `no_blitz_yards` | numeric |  |
| `base_dropbacks` | numeric |  |
| `pressure_pressure_to_sack_rate` | numeric |  |
| `no_blitz_grades_hands_fumble` | numeric |  |
| `no_pressure_grades_offense` | numeric |  |
| `no_pressure_avg_time_to_throw` | numeric |  |
| `pressure_dropbacks` | numeric |  |
| `no_blitz_grades_offense_penalty` | numeric |  |
| `no_pressure_grades_run` | numeric |  |
| `player_game_count` | numeric |  |
| `blitz_turnover_worthy_plays` | numeric |  |
| `no_blitz_touchdowns` | numeric |  |
| `no_blitz_avg_depth_of_target` | numeric |  |
| `no_pressure_dropbacks_percent` | numeric |  |
| `blitz_grades_pass` | numeric |  |
| `eligible_season` | numeric |  |
| `blitz_avg_depth_of_target` | numeric |  |
| `no_blitz_spikes` | numeric |  |
| `no_pressure_dropbacks` | numeric |  |
| `blitz_btt_rate` | numeric |  |
| `blitz_positive_epa_percent` | numeric |  |
| `no_pressure_btt_rate` | numeric |  |
| `no_pressure_drop_rate` | numeric |  |
| `no_blitz_turnover_worthy_plays` | numeric |  |
| `pressure_positive_epa_percent` | numeric |  |
| `blitz_first_downs` | numeric |  |
| `no_blitz_dropbacks_percent` | numeric |  |
| `pressure_turnover_worthy_plays` | numeric |  |
| `no_pressure_epa` | numeric |  |
| `no_blitz_avg_time_to_throw` | numeric |  |
| `no_blitz_positive_epa_percent` | numeric |  |
| `pressure_btt_rate` | numeric |  |
| `no_pressure_aimed_passes` | numeric |  |
| `pressure_dropbacks_percent` | numeric |  |
| `grades_run` | numeric |  |
| `no_pressure_def_gen_pressures` | numeric |  |
| `pressure_grades_offense` | numeric |  |
| `pressure_completion_percent` | numeric |  |
| `pressure_avg_depth_of_target` | numeric |  |
| `blitz_epa` | numeric |  |
| `pressure_drops` | numeric |  |
| `no_blitz_def_gen_pressures` | numeric |  |
| `pressure_attempts` | numeric |  |
| `pressure_big_time_throws` | numeric |  |
| `no_blitz_thrown_aways` | numeric |  |
| `blitz_drops` | numeric |  |
| `no_pressure_touchdowns` | numeric |  |
| `no_pressure_sacks` | numeric |  |
| `blitz_sack_percent` | numeric |  |
| `pressure_sack_percent` | numeric |  |
| `penalties` | numeric | Total number of penalties. |
| `no_pressure_attempts` | numeric |  |
| `no_blitz_grades_offense` | numeric |  |
| `blitz_scrambles` | numeric |  |
| `blitz_dropbacks_percent` | numeric |  |
| `no_pressure_qb_rating` | numeric |  |
| `no_blitz_passing_snaps` | numeric |  |
| `no_blitz_aimed_passes` | numeric |  |
| `blitz_yards` | numeric |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `no_blitz_attempts` | numeric |  |
| `pressure_accuracy_percent` | numeric |  |
| `no_blitz_hit_as_threw` | numeric |  |
| `pressure_hit_as_threw` | numeric |  |
| `blitz_pressure_to_sack_rate` | numeric |  |
| `declined_penalties` | numeric |  |
| `no_blitz_btt_rate` | numeric |  |
| `no_blitz_ypa` | numeric |  |
| `blitz_grades_run` | numeric |  |
| `pressure_interceptions` | numeric |  |
| `blitz_grades_hands_fumble` | numeric |  |
| `no_pressure_grades_offense_penalty` | numeric |  |
| `position` | character | Primary position as reported by NFL.com |
| `blitz_grades_offense` | numeric |  |
| `grades_hands_fumble` | numeric |  |
| `pressure_def_gen_pressures` | numeric |  |
| `pressure_grades_pass` | numeric |  |
| `no_blitz_qb_rating` | numeric |  |
| `blitz_hit_as_threw` | numeric |  |
| `pressure_ypa` | numeric |  |
| `blitz_grades_pass_route` | numeric |  |
| `pressure_avg_time_to_throw` | numeric |  |
| `no_blitz_completions` | numeric |  |
| `no_pressure_grades_pass_route` | numeric |  |
| `no_pressure_sack_percent` | numeric |  |
| `player` | character | Player name |
| `no_blitz_accuracy_percent` | numeric |  |
| `no_pressure_pressure_to_sack_rate` | character |  |
| `no_pressure_turnover_worthy_plays` | numeric |  |
| `pressure_first_downs` | numeric |  |
| `blitz_passing_snaps` | numeric |  |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `no_pressure_twp_rate` | numeric |  |
| `no_blitz_sacks` | numeric |  |
| `no_blitz_grades_pass_route` | numeric |  |
| `no_pressure_accuracy_percent` | numeric |  |
| `no_blitz_twp_rate` | numeric |  |
| `no_pressure_hit_as_threw` | numeric |  |
| `no_pressure_grades_pass` | numeric |  |
| `no_pressure_positive_epa_percent` | numeric |  |
| `pressure_spikes` | numeric |  |
| `pressure_grades_hands_fumble` | numeric |  |
| `pressure_qb_rating` | numeric |  |
| `blitz_bats` | numeric |  |
| `pressure_touchdowns` | numeric |  |
| `pressure_thrown_aways` | numeric |  |
| `no_blitz_interceptions` | numeric |  |
| `blitz_dropbacks` | numeric |  |
| `blitz_def_gen_pressures` | numeric |  |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `no_blitz_big_time_throws` | numeric |  |
| `no_pressure_avg_depth_of_target` | numeric |  |
| `no_pressure_yards` | numeric |  |
| `blitz_twp_rate` | numeric |  |
| `blitz_accuracy_percent` | numeric |  |
| `no_pressure_big_time_throws` | numeric |  |
| `no_blitz_grades_pass_block` | numeric |  |
| `blitz_grades_pass_block` | numeric |  |
| `no_pressure_grades_pass_block` | numeric |  |
| `pressure_grades_pass_block` | numeric |  |
| `no_pressure_grades_hands_drop` | numeric |  |
| `blitz_grades_hands_drop` | numeric |  |
| `pressure_grades_hands_drop` | numeric |  |
| `no_blitz_grades_hands_drop` | numeric |  |
| `blitz_grades_run_block` | numeric |  |
| `no_pressure_grades_run_block` | numeric |  |
| `no_blitz_grades_run_block` | numeric |  |
| `blitz_grades_screen_block` | numeric |  |
| `pressure_grades_screen_block` | numeric |  |
| `no_blitz_grades_screen_block` | numeric |  |
| `pressure_grades_run_block` | numeric |  |
| `no_pressure_grades_screen_block` | numeric |  |
| `pressure_grades_coverage_defense` | numeric |  |
| `pressure_grades_defense` | numeric |  |
| `no_blitz_grades_defense` | numeric |  |
| `no_blitz_grades_defense_penalty` | numeric |  |
| `blitz_grades_coverage_defense` | numeric |  |
| `no_pressure_grades_defense` | numeric |  |
| `no_pressure_grades_defense_penalty` | numeric |  |
| `no_blitz_grades_coverage_defense` | numeric |  |
| `blitz_grades_defense` | numeric |  |
| `blitz_grades_defense_penalty` | numeric |  |
| `no_pressure_grades_coverage_defense` | numeric |  |
| `pressure_grades_defense_penalty` | numeric |  |
| `pressure_grades_pass_rush_defense` | numeric |  |
| `blitz_grades_pass_rush_defense` | numeric |  |
| `no_pressure_grades_tackle` | numeric |  |
| `blitz_grades_tackle` | numeric |  |
| `blitz_grades_overall_tackle` | numeric |  |
| `no_pressure_grades_pass_rush_defense` | numeric |  |
| `pressure_grades_overall_tackle` | numeric |  |
| `no_blitz_grades_overall_tackle` | numeric |  |
| `pressure_grades_tackle` | numeric |  |
| `no_blitz_grades_pass_rush_defense` | character |  |
| `no_pressure_grades_overall_tackle` | numeric |  |
| `no_blitz_grades_tackle` | numeric |  |

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
| `grades_pass_block` | numeric |  |
| `grades_offense` | numeric | PFF overall offense grade (0-100). |
| `yards_after_catch_per_reception` | numeric |  |
| `grades_pass_route` | numeric | PFF receiving/route grade (0-100). |
| `draft_season` | numeric |  |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `yprr` | numeric | Yards per route run. |
| `wide_snaps` | numeric |  |
| `fumbles` | numeric |  |
| `first_downs` | numeric | First downs earned by the team. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `inline_snaps` | numeric |  |
| `contested_targets` | numeric | Contested targets. |
| `player_game_count` | numeric | Games with at least one qualifying snap in the requested range. |
| `eligible_season` | numeric |  |
| `inline_rate` | numeric |  |
| `contested_catch_rate` | numeric |  |
| `yards` | numeric | Receiving yards. |
| `receptions` | numeric | Passes caught by the receiver. |
| `targeted_qb_rating` | numeric |  |
| `interceptions` | numeric | The number of interceptions thrown. |
| `caught_percent` | numeric |  |
| `drop_rate` | numeric |  |
| `grades_hands_drop` | numeric | PFF hands/drop grade (0-100). |
| `slot_rate` | numeric |  |
| `slot_snaps` | numeric |  |
| `penalties` | numeric | Total number of penalties. |
| `wide_rate` | numeric |  |
| `pass_block_rate` | numeric |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric |  |
| `route_rate` | numeric |  |
| `drops` | numeric | Dropped passes. |
| `position` | character | Primary position as reported by NFL.com |
| `grades_hands_fumble` | numeric |  |
| `longest` | numeric |  |
| `pass_blocks` | numeric |  |
| `routes` | numeric | Pass routes run by the receiver. |
| `pass_plays` | numeric |  |
| `yards_per_reception` | numeric |  |
| `player` | character | Player name |
| `positive_epa_percent` | numeric |  |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `contested_receptions` | numeric | Contested catches made. |
| `yards_after_catch` | numeric | Yards after the catch. |
| `avg_depth_of_target` | numeric |  |
| `epa` | numeric | Expected points added (EPA) by the posteam for the given play. |
| `avoided_tackles` | numeric |  |
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
| `declined_penalties` | numeric |  |
| `draft_season` | numeric |  |
| `eligible_season` | numeric |  |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_kick_return` | numeric |  |
| `grades_return` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `kickoff_attempts` | numeric |  |
| `kickoff_fair_catches` | numeric |  |
| `kickoff_long` | numeric |  |
| `kickoff_muffed_returns` | numeric |  |
| `kickoff_touchdowns` | numeric |  |
| `kickoff_yards` | numeric |  |
| `kickoff_ypa` | numeric |  |
| `penalties` | numeric | Total number of penalties. |
| `player` | character | Player name |
| `player_game_count` | numeric |  |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `position` | character | Primary position as reported by NFL.com |
| `punt_attempts` | numeric |  |
| `punt_fair_catches` | numeric |  |
| `punt_long` | numeric |  |
| `punt_muffed_returns` | numeric |  |
| `punt_touchdowns` | numeric |  |
| `punt_yards` | numeric |  |
| `punt_ypa` | numeric |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `total_attempts` | numeric |  |
| `grades_punt_return` | numeric |  |

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
| `directions` | list |  |
| `draft_season` | numeric |  |
| `eligible_season` | numeric |  |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `player` | character | Player name |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `position` | character | Primary position as reported by NFL.com |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `total_attempts` | numeric |  |

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
| `grades_pass_block` | numeric |  |
| `grades_offense` | numeric | PFF overall offense grade (0-100). |
| `yards_after_contact` | numeric | Yards after contact. |
| `explosive` | numeric |  |
| `grades_pass_route` | numeric |  |
| `draft_season` | numeric |  |
| `elu_rush_mtf` | numeric |  |
| `breakaway_attempts` | numeric |  |
| `designed_yards` | numeric |  |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `yprr` | numeric |  |
| `breakaway_percent` | numeric |  |
| `fumbles` | numeric | Fumbles by the ball carrier. |
| `first_downs` | numeric | Rushing first downs. |
| `elusive_rating` | numeric | PFF elusive rating. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `breakaway_yards` | numeric | Breakaway (long-run) yards. |
| `player_game_count` | numeric | Games with at least one qualifying snap in the requested range. |
| `eligible_season` | numeric |  |
| `total_touches` | numeric |  |
| `scramble_yards` | numeric |  |
| `yco_attempt` | numeric |  |
| `yards` | numeric | Total rushing yards gained. |
| `grades_run_block` | numeric |  |
| `receptions` | numeric | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `zone_attempts` | numeric |  |
| `scrambles` | numeric |  |
| `grades_run` | numeric | PFF rushing grade (0-100). |
| `penalties` | numeric | Total number of penalties. |
| `attempts` | numeric | Rushing attempts (carries) by the runner. |
| `elu_yco` | numeric |  |
| `elu_recv_mtf` | numeric |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric |  |
| `ypa` | numeric |  |
| `drops` | numeric | Throws dropped |
| `position` | character | Primary position as reported by NFL.com |
| `grades_hands_fumble` | numeric |  |
| `longest` | numeric |  |
| `routes` | numeric |  |
| `player` | character | Player name |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `rec_yards` | numeric | Career receiving yards |
| `gap_attempts` | numeric |  |
| `run_plays` | numeric |  |
| `avoided_tackles` | numeric | Missed tackles forced. |
| `grades_offense_penalty` | numeric |  |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `touchdowns` | numeric | Rushing touchdowns. |
| `grades_pass` | numeric |  |

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
| `coverage_snaps` | numeric |  |
| `coverage_snaps_per_reception` | numeric |  |
| `coverage_snaps_per_target` | numeric |  |
| `draft_season` | numeric |  |
| `eligible_season` | numeric |  |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `interceptions` | numeric | The number of interceptions thrown. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `player` | character | Player name |
| `player_game_count` | numeric |  |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `position` | character | Primary position as reported by NFL.com |
| `qb_rating_against` | numeric |  |
| `receptions` | numeric | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | numeric | The number of pass plays where the player was the targeted receiver. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `touchdowns` | numeric |  |
| `yards` | numeric | The number of receiving yards |
| `yards_after_catch` | numeric | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `yards_per_coverage_snap` | numeric |  |

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
| `hits_allowed` | numeric |  |
| `hurries_allowed` | numeric |  |
| `pass_snaps` | numeric |  |
| `pbe` | numeric |  |
| `player_game_count` | numeric |  |
| `pressures_allowed` | numeric |  |
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
| `lhs_sacks` | numeric |  |
| `rhs_hits` | numeric |  |
| `rhs_prp` | numeric |  |
| `draft_season` | numeric |  |
| `pass_snaps` | numeric |  |
| `lhs_hurries` | numeric |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `prp` | numeric |  |
| `tackles` | numeric | Team tackles. |
| `rhs_pressures` | numeric |  |
| `rhs_pass_rush_percent` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `lhs_pass_rush_percent` | numeric |  |
| `lhs_pass_rush_snaps` | numeric |  |
| `sacks` | numeric | The Number of times sacked. |
| `lhs_assists` | numeric |  |
| `player_game_count` | numeric |  |
| `eligible_season` | numeric |  |
| `lhs_pressures` | numeric |  |
| `pass_rush_snaps` | numeric |  |
| `hurries` | numeric |  |
| `rhs_pass_rush_snaps` | numeric |  |
| `lhs_prp` | numeric |  |
| `hits` | numeric | Hits. |
| `lhs_hits` | numeric |  |
| `rhs_tackles` | numeric |  |
| `lhs_stops` | numeric |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `stops` | numeric |  |
| `rhs_misses` | numeric |  |
| `position` | character | Primary position as reported by NFL.com |
| `pressures` | numeric |  |
| `misses` | numeric |  |
| `player` | character | Player name |
| `rhs_sacks` | numeric |  |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `pass_rush_percent` | numeric |  |
| `rhs_hurries` | numeric |  |
| `rhs_assists` | numeric |  |
| `rhs_stops` | numeric |  |
| `assists` | numeric | Total assists. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `lhs_tackles` | numeric |  |
| `lhs_misses` | numeric |  |

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
| `man_contested_catch_rate` | numeric |  |
| `man_touchdowns` | numeric |  |
| `zone_targets_percent` | numeric |  |
| `man_interceptions` | numeric |  |
| `zone_epa` | numeric |  |
| `man_avg_depth_of_target` | numeric |  |
| `zone_pass_blocks` | numeric |  |
| `zone_avoided_tackles` | numeric |  |
| `man_targeted_qb_rating` | numeric |  |
| `draft_season` | numeric |  |
| `zone_positive_epa_percent` | numeric |  |
| `man_targets_percent` | numeric |  |
| `man_yards_per_reception` | numeric |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `man_drop_rate` | numeric |  |
| `zone_grades_pass_route` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `zone_fumbles` | numeric |  |
| `man_pass_block_rate` | numeric |  |
| `zone_yards` | numeric |  |
| `man_yprr` | numeric |  |
| `player_game_count` | numeric |  |
| `eligible_season` | numeric |  |
| `zone_drops` | numeric |  |
| `zone_receptions` | numeric |  |
| `man_pass_plays` | numeric |  |
| `man_epa` | numeric |  |
| `zone_yards_per_reception` | numeric |  |
| `man_contested_targets` | numeric |  |
| `man_longest` | numeric |  |
| `zone_yards_after_catch` | numeric |  |
| `man_receptions` | numeric |  |
| `man_avoided_tackles` | numeric |  |
| `man_first_downs` | numeric |  |
| `base_targets` | numeric |  |
| `zone_contested_catch_rate` | numeric |  |
| `zone_targeted_qb_rating` | numeric |  |
| `man_routes` | numeric |  |
| `zone_grades_hands_drop` | numeric |  |
| `man_route_rate` | numeric |  |
| `zone_pass_block_rate` | numeric |  |
| `man_grades_pass_route` | numeric |  |
| `penalties` | numeric | Total number of penalties. |
| `zone_first_downs` | numeric |  |
| `zone_yprr` | numeric |  |
| `man_drops` | numeric |  |
| `zone_caught_percent` | numeric |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `man_fumbles` | numeric |  |
| `declined_penalties` | numeric |  |
| `man_yards_after_catch` | numeric |  |
| `man_yards` | numeric |  |
| `zone_pass_plays` | numeric |  |
| `position` | character | Primary position as reported by NFL.com |
| `man_targets` | numeric |  |
| `man_grades_hands_drop` | numeric |  |
| `man_pass_blocks` | numeric |  |
| `zone_touchdowns` | numeric |  |
| `zone_route_rate` | numeric |  |
| `zone_yards_after_catch_per_reception` | numeric |  |
| `zone_avg_depth_of_target` | numeric |  |
| `player` | character | Player name |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `zone_contested_targets` | numeric |  |
| `zone_contested_receptions` | numeric |  |
| `man_contested_receptions` | numeric |  |
| `man_yards_after_catch_per_reception` | numeric |  |
| `zone_targets` | numeric |  |
| `zone_longest` | numeric |  |
| `man_caught_percent` | numeric |  |
| `zone_drop_rate` | numeric |  |
| `zone_interceptions` | numeric |  |
| `man_positive_epa_percent` | numeric |  |
| `zone_routes` | numeric |  |
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
| `more_btt_rate` | numeric |  |
| `more_grades_run` | numeric |  |
| `more_ypa` | numeric |  |
| `more_passing_snaps` | numeric |  |
| `more_pressure_to_sack_rate` | numeric |  |
| `less_def_gen_pressures` | numeric |  |
| `dropbacks` | numeric |  |
| `less_sack_percent` | numeric |  |
| `less_first_downs` | numeric |  |
| `less_ypa` | numeric |  |
| `draft_season` | numeric |  |
| `more_grades_pass_route` | character |  |
| `less_pressure_to_sack_rate` | numeric |  |
| `less_aimed_passes` | numeric |  |
| `more_dropbacks_percent` | numeric |  |
| `less_positive_epa_percent` | numeric |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `avg_ttt_scrambles` | numeric |  |
| `more_yards` | numeric |  |
| `more_accuracy_percent` | numeric |  |
| `less_attempts` | numeric |  |
| `less_drop_rate` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `more_turnover_worthy_plays` | numeric |  |
| `more_interceptions` | numeric |  |
| `more_sack_percent` | numeric |  |
| `more_twp_rate` | numeric |  |
| `less_completions` | numeric |  |
| `more_thrown_aways` | numeric |  |
| `less_btt_rate` | numeric |  |
| `more_big_time_throws` | numeric |  |
| `less_qb_rating` | numeric |  |
| `less_hit_as_threw` | numeric |  |
| `more_attempts` | numeric |  |
| `player_game_count` | numeric |  |
| `less_spikes` | numeric |  |
| `eligible_season` | numeric |  |
| `less_dropbacks_percent` | numeric |  |
| `more_qb_rating` | numeric |  |
| `less_dropbacks` | numeric |  |
| `more_grades_hands_fumble` | numeric |  |
| `more_avg_depth_of_target` | numeric |  |
| `less_scrambles` | numeric |  |
| `more_positive_epa_percent` | numeric |  |
| `less_sacks` | numeric |  |
| `less_grades_pass` | numeric |  |
| `less_drops` | numeric |  |
| `more_sacks` | numeric |  |
| `more_first_downs` | numeric |  |
| `less_big_time_throws` | numeric |  |
| `avg_ttt_attempts` | numeric |  |
| `more_aimed_passes` | numeric |  |
| `less_accuracy_percent` | numeric |  |
| `more_scrambles` | numeric |  |
| `less_epa` | numeric |  |
| `more_spikes` | numeric |  |
| `less_grades_pass_route` | character |  |
| `less_grades_offense` | numeric |  |
| `avg_ttt_sacks` | numeric |  |
| `more_grades_offense_penalty` | numeric |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `less_bats` | numeric |  |
| `less_grades_run` | numeric |  |
| `position` | character | Primary position as reported by NFL.com |
| `less_touchdowns` | numeric |  |
| `less_yards` | numeric |  |
| `less_grades_run_block` | character |  |
| `less_grades_hands_fumble` | numeric |  |
| `more_hit_as_threw` | numeric |  |
| `more_epa` | numeric |  |
| `avg_time_to_throw` | numeric | Average time elapsed from the time of snap to throw on every pass attempt for a passer (sacks excluded). |
| `less_turnover_worthy_plays` | numeric |  |
| `less_completion_percent` | numeric |  |
| `more_drops` | numeric |  |
| `player` | character | Player name |
| `more_touchdowns` | numeric |  |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `more_drop_rate` | numeric |  |
| `less_twp_rate` | numeric |  |
| `more_grades_offense` | numeric |  |
| `less_thrown_aways` | numeric |  |
| `less_avg_depth_of_target` | numeric |  |
| `less_avg_time_to_throw` | numeric |  |
| `less_grades_offense_penalty` | numeric |  |
| `less_interceptions` | numeric |  |
| `more_def_gen_pressures` | numeric |  |
| `more_avg_time_to_throw` | numeric |  |
| `more_dropbacks` | numeric |  |
| `more_grades_pass` | numeric |  |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `less_passing_snaps` | numeric |  |
| `more_completions` | numeric |  |
| `more_grades_run_block` | character |  |
| `more_bats` | numeric |  |
| `more_completion_percent` | numeric |  |
| `more_grades_hands_drop` | character |  |
| `less_grades_hands_drop` | character |  |
| `less_grades_pass_block` | character |  |
| `more_grades_pass_block` | character |  |
| `less_grades_screen_block` | character |  |
| `more_grades_screen_block` | character |  |
| `less_grades_defense_penalty` | character |  |
| `less_grades_coverage_defense` | character |  |
| `more_grades_defense_penalty` | character |  |
| `more_grades_defense` | character |  |
| `more_grades_coverage_defense` | character |  |
| `less_grades_defense` | character |  |
| `less_grades_overall_tackle` | character |  |
| `more_grades_pass_rush_defense` | character |  |
| `more_grades_run_defense` | character |  |
| `more_grades_tackle` | character |  |
| `less_grades_pass_rush_defense` | character |  |
| `less_grades_tackle` | character |  |
| `more_grades_overall_tackle` | character |  |
| `less_grades_run_defense` | character |  |

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
| `screen_caught_percent` | numeric |  |
| `screen_targeted_qb_rating` | numeric |  |
| `slot_grades_pass_route` | numeric |  |
| `slot_avg_depth_of_target` | numeric |  |
| `slot_positive_epa_percent` | numeric |  |
| `screen_yprr` | numeric |  |
| `draft_season` | numeric |  |
| `slot_routes` | numeric |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `screen_grades_hands_drop` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `screen_contested_catch_rate` | numeric |  |
| `slot_yards_per_reception` | numeric |  |
| `screen_interceptions` | numeric |  |
| `slot_targets_percent` | numeric |  |
| `screen_longest` | numeric |  |
| `slot_avoided_tackles` | numeric |  |
| `slot_yards_after_catch` | numeric |  |
| `slot_grades_hands_drop` | numeric |  |
| `player_game_count` | numeric |  |
| `screen_contested_targets` | numeric |  |
| `eligible_season` | numeric |  |
| `screen_yards_after_catch_per_reception` | numeric |  |
| `screen_yards` | numeric |  |
| `slot_route_rate` | numeric |  |
| `screen_drop_rate` | numeric |  |
| `screen_epa` | numeric |  |
| `screen_grades_pass_route` | numeric |  |
| `screen_drops` | numeric |  |
| `screen_fumbles` | numeric |  |
| `slot_interceptions` | numeric |  |
| `screen_yards_after_catch` | numeric |  |
| `screen_avg_depth_of_target` | numeric |  |
| `screen_pass_block_rate` | numeric |  |
| `slot_pass_block_rate` | numeric |  |
| `slot_yprr` | numeric |  |
| `base_targets` | numeric |  |
| `slot_longest` | numeric |  |
| `slot_drops` | numeric |  |
| `screen_routes` | numeric |  |
| `slot_fumbles` | numeric |  |
| `penalties` | numeric | Total number of penalties. |
| `slot_contested_catch_rate` | numeric |  |
| `slot_pass_plays` | numeric |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric |  |
| `screen_positive_epa_percent` | numeric |  |
| `slot_first_downs` | numeric |  |
| `screen_route_rate` | numeric |  |
| `position` | character | Primary position as reported by NFL.com |
| `screen_pass_blocks` | numeric |  |
| `slot_targets` | numeric |  |
| `slot_pass_blocks` | numeric |  |
| `slot_receptions` | numeric |  |
| `screen_first_downs` | numeric |  |
| `slot_caught_percent` | numeric |  |
| `screen_avoided_tackles` | numeric |  |
| `player` | character | Player name |
| `slot_epa` | numeric |  |
| `slot_drop_rate` | numeric |  |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `slot_touchdowns` | numeric |  |
| `slot_yards_after_catch_per_reception` | numeric |  |
| `screen_receptions` | numeric |  |
| `slot_targeted_qb_rating` | numeric |  |
| `slot_contested_targets` | numeric |  |
| `screen_yards_per_reception` | numeric |  |
| `slot_contested_receptions` | numeric |  |
| `screen_pass_plays` | numeric |  |
| `screen_contested_receptions` | numeric |  |
| `screen_touchdowns` | numeric |  |
| `screen_targets` | numeric |  |
| `screen_targets_percent` | numeric |  |
| `slot_yards` | numeric |  |
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
| `declined_penalties` | numeric |  |
| `draft_season` | numeric |  |
| `eligible_season` | numeric |  |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_fgep_kicker` | numeric |  |
| `grades_kickoff_kicker` | numeric |  |
| `grades_misc_st` | numeric |  |
| `grades_special_teams_penalty` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `missed_tackles` | numeric |  |
| `penalties` | numeric | Total number of penalties. |
| `player` | character | Player name |
| `player_game_count` | numeric |  |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `position` | character | Primary position as reported by NFL.com |
| `snap_counts_field_goal` | numeric |  |
| `snap_counts_field_goal_blocking` | numeric |  |
| `snap_counts_kickoff` | numeric |  |
| `snap_counts_kickoff_return` | numeric |  |
| `snap_counts_punt_coverage` | numeric |  |
| `snap_counts_punt_return` | numeric |  |
| `tackles` | numeric | Team tackles. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `grades_fgep_defense` | numeric |  |
| `grades_fgep_offense` | numeric |  |
| `grades_long_snap` | numeric |  |
| `grades_punter` | numeric |  |
| `grades_kick_return` | numeric |  |
| `grades_punt_return` | numeric |  |

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
| `left_deep_route_rate` | numeric |  |
| `center_short_first_downs` | numeric |  |
| `center_short_routes` | numeric |  |
| `right_behind_los_positive_epa_percent` | numeric |  |
| `left_behind_los_yards_after_catch` | numeric |  |
| `center_behind_los_contested_targets` | numeric |  |
| `right_short_routes` | numeric |  |
| `right_deep_pass_block_rate` | numeric |  |
| `medium_interceptions` | numeric |  |
| `right_short_targets_percent` | numeric |  |
| `left_deep_contested_catch_rate` | numeric |  |
| `right_medium_grades_hands_drop` | numeric |  |
| `left_short_targeted_qb_rating` | numeric |  |
| `right_medium_contested_catch_rate` | numeric |  |
| `short_targets` | numeric |  |
| `deep_contested_catch_rate` | numeric |  |
| `center_behind_los_drops` | numeric |  |
| `center_behind_los_interceptions` | numeric |  |
| `left_behind_los_interceptions` | numeric |  |
| `left_deep_first_downs` | numeric |  |
| `left_behind_los_contested_targets` | numeric |  |
| `center_deep_avg_depth_of_target` | numeric |  |
| `center_short_drops` | numeric |  |
| `right_behind_los_targets` | numeric |  |
| `right_behind_los_yards_after_catch_per_reception` | numeric |  |
| `center_deep_first_downs` | numeric |  |
| `behind_los_yards_after_catch_per_reception` | numeric |  |
| `right_deep_grades_pass_route` | numeric |  |
| `left_short_fumbles` | numeric |  |
| `right_short_positive_epa_percent` | numeric |  |
| `center_deep_pass_block_rate` | numeric |  |
| `left_behind_los_avoided_tackles` | numeric |  |
| `center_short_longest` | numeric |  |
| `left_deep_drop_rate` | numeric |  |
| `center_short_contested_receptions` | numeric |  |
| `left_deep_routes` | numeric |  |
| `deep_drops` | numeric |  |
| `deep_contested_receptions` | numeric |  |
| `center_behind_los_touchdowns` | numeric |  |
| `center_medium_targeted_qb_rating` | numeric |  |
| `left_short_yards_per_reception` | numeric |  |
| `deep_touchdowns` | numeric |  |
| `left_short_yprr` | numeric |  |
| `center_deep_drop_rate` | numeric |  |
| `center_behind_los_pass_plays` | numeric |  |
| `right_short_first_downs` | numeric |  |
| `right_behind_los_first_downs` | numeric |  |
| `right_behind_los_grades_pass_route` | numeric |  |
| `short_interceptions` | numeric |  |
| `behind_los_routes` | numeric |  |
| `right_behind_los_contested_targets` | numeric |  |
| `left_medium_grades_pass_route` | numeric |  |
| `right_short_contested_catch_rate` | numeric |  |
| `draft_season` | numeric |  |
| `center_short_grades_hands_drop` | numeric |  |
| `deep_receptions` | numeric |  |
| `center_deep_pass_blocks` | numeric |  |
| `left_short_yards_after_catch_per_reception` | numeric |  |
| `center_medium_avoided_tackles` | numeric |  |
| `center_medium_grades_pass_route` | numeric |  |
| `center_behind_los_positive_epa_percent` | numeric |  |
| `medium_yards_after_catch_per_reception` | numeric |  |
| `medium_receptions` | numeric |  |
| `deep_pass_blocks` | numeric |  |
| `right_deep_yards_per_reception` | numeric |  |
| `center_medium_longest` | numeric |  |
| `center_deep_contested_catch_rate` | numeric |  |
| `medium_epa` | numeric |  |
| `left_short_route_rate` | numeric |  |
| `right_deep_grades_hands_drop` | numeric |  |
| `center_deep_targets` | numeric |  |
| `short_contested_receptions` | numeric |  |
| `center_deep_pass_plays` | numeric |  |
| `left_medium_routes` | numeric |  |
| `deep_yards_after_catch_per_reception` | numeric |  |
| `center_deep_contested_receptions` | numeric |  |
| `center_short_pass_blocks` | numeric |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `deep_pass_block_rate` | numeric |  |
| `short_touchdowns` | numeric |  |
| `center_medium_drops` | numeric |  |
| `left_short_pass_blocks` | numeric |  |
| `right_short_grades_pass_route` | numeric |  |
| `center_short_positive_epa_percent` | numeric |  |
| `deep_avg_depth_of_target` | numeric |  |
| `deep_grades_hands_drop` | numeric |  |
| `center_behind_los_avg_depth_of_target` | numeric |  |
| `left_deep_caught_percent` | numeric |  |
| `right_behind_los_contested_catch_rate` | numeric |  |
| `center_medium_route_rate` | numeric |  |
| `short_grades_pass_route` | numeric |  |
| `center_behind_los_contested_receptions` | numeric |  |
| `center_short_receptions` | numeric |  |
| `center_short_yprr` | numeric |  |
| `medium_pass_blocks` | numeric |  |
| `medium_yards_per_reception` | numeric |  |
| `center_deep_yprr` | numeric |  |
| `right_deep_drop_rate` | numeric |  |
| `center_short_targets` | numeric |  |
| `center_medium_pass_block_rate` | numeric |  |
| `left_behind_los_drop_rate` | numeric |  |
| `right_behind_los_targets_percent` | numeric |  |
| `short_contested_catch_rate` | numeric |  |
| `behind_los_fumbles` | numeric |  |
| `left_medium_drop_rate` | numeric |  |
| `left_medium_longest` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `medium_yprr` | numeric |  |
| `left_short_positive_epa_percent` | numeric |  |
| `center_behind_los_pass_block_rate` | numeric |  |
| `right_short_fumbles` | numeric |  |
| `right_deep_route_rate` | numeric |  |
| `right_short_pass_blocks` | numeric |  |
| `left_short_longest` | numeric |  |
| `medium_contested_targets` | numeric |  |
| `behind_los_interceptions` | numeric |  |
| `right_medium_targeted_qb_rating` | numeric |  |
| `right_deep_drops` | numeric |  |
| `right_short_route_rate` | numeric |  |
| `medium_grades_pass_route` | numeric |  |
| `right_deep_yards` | numeric |  |
| `right_medium_contested_targets` | numeric |  |
| `center_medium_targets` | numeric |  |
| `right_short_avg_depth_of_target` | numeric |  |
| `center_deep_longest` | numeric |  |
| `right_behind_los_yards_per_reception` | numeric |  |
| `left_medium_pass_block_rate` | numeric |  |
| `right_deep_yards_after_catch` | numeric |  |
| `right_medium_receptions` | numeric |  |
| `right_short_targeted_qb_rating` | numeric |  |
| `left_deep_targets_percent` | numeric |  |
| `behind_los_pass_blocks` | numeric |  |
| `right_behind_los_touchdowns` | numeric |  |
| `right_medium_targets` | numeric |  |
| `deep_routes` | numeric |  |
| `left_medium_yprr` | numeric |  |
| `deep_yards_per_reception` | numeric |  |
| `center_behind_los_avoided_tackles` | numeric |  |
| `left_deep_contested_targets` | numeric |  |
| `medium_avg_depth_of_target` | numeric |  |
| `short_route_rate` | numeric |  |
| `left_behind_los_positive_epa_percent` | numeric |  |
| `left_medium_touchdowns` | numeric |  |
| `deep_targets_percent` | numeric |  |
| `behind_los_targets` | numeric |  |
| `center_short_yards` | numeric |  |
| `center_medium_routes` | numeric |  |
| `center_behind_los_grades_pass_route` | numeric |  |
| `right_deep_yprr` | numeric |  |
| `center_deep_targeted_qb_rating` | numeric |  |
| `left_deep_targeted_qb_rating` | numeric |  |
| `deep_pass_plays` | numeric |  |
| `center_short_avg_depth_of_target` | numeric |  |
| `right_short_yards` | numeric |  |
| `left_medium_first_downs` | numeric |  |
| `right_medium_positive_epa_percent` | numeric |  |
| `left_medium_targeted_qb_rating` | numeric |  |
| `right_short_contested_receptions` | numeric |  |
| `right_deep_avoided_tackles` | numeric |  |
| `right_medium_fumbles` | numeric |  |
| `right_behind_los_fumbles` | numeric |  |
| `center_deep_drops` | numeric |  |
| `center_medium_avg_depth_of_target` | numeric |  |
| `right_behind_los_pass_block_rate` | numeric |  |
| `left_short_yards_after_catch` | numeric |  |
| `right_behind_los_pass_blocks` | numeric |  |
| `left_behind_los_avg_depth_of_target` | numeric |  |
| `center_medium_epa` | numeric |  |
| `left_medium_targets_percent` | numeric |  |
| `left_behind_los_grades_pass_route` | numeric |  |
| `center_behind_los_yards_per_reception` | numeric |  |
| `left_deep_targets` | numeric |  |
| `short_contested_targets` | numeric |  |
| `left_medium_contested_catch_rate` | numeric |  |
| `left_deep_yprr` | numeric |  |
| `right_medium_yards_after_catch_per_reception` | numeric |  |
| `right_medium_longest` | numeric |  |
| `behind_los_drop_rate` | numeric |  |
| `right_behind_los_epa` | numeric |  |
| `player_game_count` | numeric |  |
| `left_short_pass_plays` | numeric |  |
| `right_short_touchdowns` | numeric |  |
| `behind_los_first_downs` | numeric |  |
| `right_deep_receptions` | numeric |  |
| `left_medium_pass_plays` | numeric |  |
| `eligible_season` | numeric |  |
| `center_behind_los_yards` | numeric |  |
| `right_medium_pass_blocks` | numeric |  |
| `left_short_grades_pass_route` | numeric |  |
| `left_behind_los_targets_percent` | numeric |  |
| `behind_los_longest` | numeric |  |
| `right_medium_first_downs` | numeric |  |
| `center_deep_yards_after_catch_per_reception` | numeric |  |
| `deep_contested_targets` | numeric |  |
| `behind_los_avoided_tackles` | numeric |  |
| `left_behind_los_receptions` | numeric |  |
| `right_short_yards_after_catch` | numeric |  |
| `right_behind_los_yards_after_catch` | numeric |  |
| `right_short_pass_plays` | numeric |  |
| `right_deep_epa` | numeric |  |
| `left_short_caught_percent` | numeric |  |
| `center_behind_los_contested_catch_rate` | numeric |  |
| `right_short_yards_after_catch_per_reception` | numeric |  |
| `right_short_drop_rate` | numeric |  |
| `left_short_yards` | numeric |  |
| `right_behind_los_avg_depth_of_target` | numeric |  |
| `right_deep_first_downs` | numeric |  |
| `left_deep_positive_epa_percent` | numeric |  |
| `short_yards_after_catch` | numeric |  |
| `center_medium_yprr` | numeric |  |
| `short_pass_blocks` | numeric |  |
| `left_short_contested_receptions` | numeric |  |
| `short_fumbles` | numeric |  |
| `left_short_pass_block_rate` | numeric |  |
| `center_deep_routes` | numeric |  |
| `left_medium_avoided_tackles` | numeric |  |
| `left_behind_los_pass_plays` | numeric |  |
| `right_deep_touchdowns` | numeric |  |
| `center_medium_receptions` | numeric |  |
| `left_behind_los_drops` | numeric |  |
| `center_short_targets_percent` | numeric |  |
| `deep_epa` | numeric |  |
| `medium_touchdowns` | numeric |  |
| `left_medium_route_rate` | numeric |  |
| `left_behind_los_grades_hands_drop` | numeric |  |
| `left_deep_fumbles` | numeric |  |
| `medium_avoided_tackles` | numeric |  |
| `center_medium_contested_catch_rate` | numeric |  |
| `right_behind_los_routes` | numeric |  |
| `short_targets_percent` | numeric |  |
| `left_behind_los_yprr` | numeric |  |
| `medium_route_rate` | numeric |  |
| `left_short_first_downs` | numeric |  |
| `short_longest` | numeric |  |
| `right_behind_los_grades_hands_drop` | numeric |  |
| `behind_los_contested_targets` | numeric |  |
| `right_behind_los_avoided_tackles` | numeric |  |
| `right_deep_longest` | numeric |  |
| `right_deep_contested_targets` | numeric |  |
| `right_deep_positive_epa_percent` | numeric |  |
| `right_behind_los_pass_plays` | numeric |  |
| `medium_contested_receptions` | numeric |  |
| `short_targeted_qb_rating` | numeric |  |
| `right_behind_los_interceptions` | numeric |  |
| `behind_los_targets_percent` | numeric |  |
| `center_deep_yards_after_catch` | numeric |  |
| `behind_los_yards` | numeric |  |
| `right_deep_targets_percent` | numeric |  |
| `left_medium_fumbles` | numeric |  |
| `short_receptions` | numeric |  |
| `left_short_interceptions` | numeric |  |
| `right_medium_interceptions` | numeric |  |
| `left_behind_los_yards` | numeric |  |
| `medium_first_downs` | numeric |  |
| `left_short_avg_depth_of_target` | numeric |  |
| `right_medium_pass_block_rate` | numeric |  |
| `right_medium_targets_percent` | numeric |  |
| `left_behind_los_touchdowns` | numeric |  |
| `right_behind_los_drop_rate` | numeric |  |
| `right_deep_fumbles` | numeric |  |
| `left_deep_contested_receptions` | numeric |  |
| `behind_los_drops` | numeric |  |
| `short_yards_after_catch_per_reception` | numeric |  |
| `medium_pass_plays` | numeric |  |
| `behind_los_pass_plays` | numeric |  |
| `left_deep_yards` | numeric |  |
| `center_behind_los_longest` | numeric |  |
| `center_medium_grades_hands_drop` | numeric |  |
| `left_behind_los_longest` | numeric |  |
| `center_short_interceptions` | numeric |  |
| `short_avg_depth_of_target` | numeric |  |
| `deep_yprr` | numeric |  |
| `left_deep_touchdowns` | numeric |  |
| `base_targets` | numeric |  |
| `deep_yards` | numeric |  |
| `center_medium_yards_after_catch` | numeric |  |
| `left_medium_epa` | numeric |  |
| `left_deep_avg_depth_of_target` | numeric |  |
| `deep_first_downs` | numeric |  |
| `short_drop_rate` | numeric |  |
| `left_medium_positive_epa_percent` | numeric |  |
| `center_deep_touchdowns` | numeric |  |
| `right_behind_los_yprr` | numeric |  |
| `center_medium_pass_blocks` | numeric |  |
| `short_positive_epa_percent` | numeric |  |
| `medium_fumbles` | numeric |  |
| `center_medium_yards_after_catch_per_reception` | numeric |  |
| `left_short_routes` | numeric |  |
| `behind_los_route_rate` | numeric |  |
| `behind_los_epa` | numeric |  |
| `right_behind_los_contested_receptions` | numeric |  |
| `right_medium_yards_after_catch` | numeric |  |
| `center_short_touchdowns` | numeric |  |
| `right_medium_drops` | numeric |  |
| `left_behind_los_routes` | numeric |  |
| `left_deep_epa` | numeric |  |
| `left_short_targets` | numeric |  |
| `left_deep_pass_block_rate` | numeric |  |
| `right_deep_caught_percent` | numeric |  |
| `center_short_grades_pass_route` | numeric |  |
| `left_behind_los_fumbles` | numeric |  |
| `right_medium_touchdowns` | numeric |  |
| `center_short_fumbles` | numeric |  |
| `center_behind_los_routes` | numeric |  |
| `behind_los_receptions` | numeric |  |
| `medium_targets_percent` | numeric |  |
| `left_medium_yards` | numeric |  |
| `center_medium_touchdowns` | numeric |  |
| `medium_targeted_qb_rating` | numeric |  |
| `center_short_drop_rate` | numeric |  |
| `penalties` | numeric | Total number of penalties. |
| `short_pass_block_rate` | numeric |  |
| `right_deep_avg_depth_of_target` | numeric |  |
| `center_deep_yards` | numeric |  |
| `center_short_yards_after_catch_per_reception` | numeric |  |
| `right_short_targets` | numeric |  |
| `behind_los_avg_depth_of_target` | numeric |  |
| `left_behind_los_contested_catch_rate` | numeric |  |
| `center_short_targeted_qb_rating` | numeric |  |
| `left_behind_los_epa` | numeric |  |
| `deep_route_rate` | numeric |  |
| `short_yprr` | numeric |  |
| `deep_caught_percent` | numeric |  |
| `center_medium_drop_rate` | numeric |  |
| `center_behind_los_yards_after_catch` | numeric |  |
| `medium_yards` | numeric |  |
| `center_deep_avoided_tackles` | numeric |  |
| `left_short_contested_targets` | numeric |  |
| `center_behind_los_first_downs` | numeric |  |
| `center_medium_interceptions` | numeric |  |
| `center_deep_yards_per_reception` | numeric |  |
| `center_deep_contested_targets` | numeric |  |
| `left_short_targets_percent` | numeric |  |
| `short_grades_hands_drop` | numeric |  |
| `right_deep_yards_after_catch_per_reception` | numeric |  |
| `left_medium_contested_receptions` | numeric |  |
| `center_short_yards_per_reception` | numeric |  |
| `center_deep_grades_hands_drop` | numeric |  |
| `deep_longest` | numeric |  |
| `left_medium_yards_after_catch_per_reception` | numeric |  |
| `right_short_interceptions` | numeric |  |
| `center_short_caught_percent` | numeric |  |
| `left_deep_interceptions` | numeric |  |
| `short_yards_per_reception` | numeric |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `center_short_avoided_tackles` | numeric |  |
| `behind_los_yards_per_reception` | numeric |  |
| `short_epa` | numeric |  |
| `deep_drop_rate` | numeric |  |
| `left_deep_yards_per_reception` | numeric |  |
| `right_short_yprr` | numeric |  |
| `declined_penalties` | numeric |  |
| `medium_longest` | numeric |  |
| `left_short_contested_catch_rate` | numeric |  |
| `right_medium_routes` | numeric |  |
| `left_short_receptions` | numeric |  |
| `left_deep_yards_after_catch_per_reception` | numeric |  |
| `deep_targets` | numeric |  |
| `left_short_grades_hands_drop` | numeric |  |
| `right_short_epa` | numeric |  |
| `left_behind_los_contested_receptions` | numeric |  |
| `medium_targets` | numeric |  |
| `behind_los_targeted_qb_rating` | numeric |  |
| `center_short_contested_targets` | numeric |  |
| `center_short_route_rate` | numeric |  |
| `right_medium_route_rate` | numeric |  |
| `left_short_epa` | numeric |  |
| `left_deep_avoided_tackles` | numeric |  |
| `center_behind_los_pass_blocks` | numeric |  |
| `deep_interceptions` | numeric |  |
| `right_short_caught_percent` | numeric |  |
| `center_deep_caught_percent` | numeric |  |
| `center_medium_fumbles` | numeric |  |
| `behind_los_grades_hands_drop` | numeric |  |
| `left_behind_los_targets` | numeric |  |
| `right_medium_yprr` | numeric |  |
| `right_short_receptions` | numeric |  |
| `position` | character | Primary position as reported by NFL.com |
| `right_short_contested_targets` | numeric |  |
| `center_medium_yards_per_reception` | numeric |  |
| `right_behind_los_yards` | numeric |  |
| `center_behind_los_grades_hands_drop` | numeric |  |
| `right_behind_los_receptions` | numeric |  |
| `left_medium_caught_percent` | numeric |  |
| `left_medium_drops` | numeric |  |
| `short_avoided_tackles` | numeric |  |
| `center_short_epa` | numeric |  |
| `center_behind_los_targets` | numeric |  |
| `left_deep_pass_blocks` | numeric |  |
| `left_short_drops` | numeric |  |
| `medium_positive_epa_percent` | numeric |  |
| `center_deep_receptions` | numeric |  |
| `right_medium_epa` | numeric |  |
| `right_short_pass_block_rate` | numeric |  |
| `center_deep_route_rate` | numeric |  |
| `center_short_pass_plays` | numeric |  |
| `center_deep_interceptions` | numeric |  |
| `left_medium_yards_after_catch` | numeric |  |
| `left_behind_los_pass_blocks` | numeric |  |
| `right_behind_los_longest` | numeric |  |
| `short_yards` | numeric |  |
| `left_deep_receptions` | numeric |  |
| `deep_grades_pass_route` | numeric |  |
| `left_medium_targets` | numeric |  |
| `behind_los_grades_pass_route` | numeric |  |
| `right_deep_targeted_qb_rating` | numeric |  |
| `behind_los_pass_block_rate` | numeric |  |
| `left_medium_pass_blocks` | numeric |  |
| `player` | character | Player name |
| `left_medium_avg_depth_of_target` | numeric |  |
| `deep_avoided_tackles` | numeric |  |
| `center_behind_los_yprr` | numeric |  |
| `center_behind_los_epa` | numeric |  |
| `right_behind_los_route_rate` | numeric |  |
| `center_deep_fumbles` | numeric |  |
| `left_medium_grades_hands_drop` | numeric |  |
| `right_medium_yards` | numeric |  |
| `left_deep_yards_after_catch` | numeric |  |
| `left_deep_pass_plays` | numeric |  |
| `center_short_yards_after_catch` | numeric |  |
| `center_behind_los_route_rate` | numeric |  |
| `right_medium_drop_rate` | numeric |  |
| `medium_grades_hands_drop` | numeric |  |
| `right_deep_pass_blocks` | numeric |  |
| `medium_contested_catch_rate` | numeric |  |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `center_behind_los_caught_percent` | numeric |  |
| `right_behind_los_caught_percent` | numeric |  |
| `short_pass_plays` | numeric |  |
| `center_behind_los_targeted_qb_rating` | numeric |  |
| `deep_targeted_qb_rating` | numeric |  |
| `center_short_pass_block_rate` | numeric |  |
| `right_short_avoided_tackles` | numeric |  |
| `left_short_drop_rate` | numeric |  |
| `left_medium_yards_per_reception` | numeric |  |
| `left_medium_receptions` | numeric |  |
| `right_medium_pass_plays` | numeric |  |
| `center_medium_contested_targets` | numeric |  |
| `short_first_downs` | numeric |  |
| `center_medium_targets_percent` | numeric |  |
| `right_short_longest` | numeric |  |
| `left_deep_longest` | numeric |  |
| `right_short_drops` | numeric |  |
| `right_medium_contested_receptions` | numeric |  |
| `right_deep_contested_catch_rate` | numeric |  |
| `center_behind_los_receptions` | numeric |  |
| `right_medium_caught_percent` | numeric |  |
| `right_deep_targets` | numeric |  |
| `center_medium_positive_epa_percent` | numeric |  |
| `medium_drops` | numeric |  |
| `left_deep_grades_hands_drop` | numeric |  |
| `behind_los_positive_epa_percent` | numeric |  |
| `left_short_avoided_tackles` | numeric |  |
| `right_medium_grades_pass_route` | numeric |  |
| `right_short_grades_hands_drop` | numeric |  |
| `left_behind_los_yards_per_reception` | numeric |  |
| `medium_yards_after_catch` | numeric |  |
| `center_medium_pass_plays` | numeric |  |
| `left_behind_los_caught_percent` | numeric |  |
| `left_medium_contested_targets` | numeric |  |
| `center_medium_contested_receptions` | numeric |  |
| `behind_los_contested_catch_rate` | numeric |  |
| `behind_los_yards_after_catch` | numeric |  |
| `left_deep_drops` | numeric |  |
| `center_deep_positive_epa_percent` | numeric |  |
| `left_behind_los_yards_after_catch_per_reception` | numeric |  |
| `right_deep_routes` | numeric |  |
| `center_deep_grades_pass_route` | numeric |  |
| `deep_yards_after_catch` | numeric |  |
| `center_behind_los_drop_rate` | numeric |  |
| `short_drops` | numeric |  |
| `right_deep_contested_receptions` | numeric |  |
| `left_behind_los_targeted_qb_rating` | numeric |  |
| `right_medium_avg_depth_of_target` | numeric |  |
| `right_medium_avoided_tackles` | numeric |  |
| `center_medium_caught_percent` | numeric |  |
| `center_behind_los_fumbles` | numeric |  |
| `center_medium_first_downs` | numeric |  |
| `behind_los_caught_percent` | numeric |  |
| `short_routes` | numeric |  |
| `left_deep_grades_pass_route` | numeric |  |
| `medium_routes` | numeric |  |
| `medium_caught_percent` | numeric |  |
| `behind_los_contested_receptions` | numeric |  |
| `behind_los_yprr` | numeric |  |
| `left_short_touchdowns` | numeric |  |
| `medium_drop_rate` | numeric |  |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `left_behind_los_pass_block_rate` | numeric |  |
| `right_deep_pass_plays` | numeric |  |
| `short_caught_percent` | numeric |  |
| `right_medium_yards_per_reception` | numeric |  |
| `center_medium_yards` | numeric |  |
| `center_deep_epa` | numeric |  |
| `left_medium_interceptions` | numeric |  |
| `right_deep_interceptions` | numeric |  |
| `deep_positive_epa_percent` | numeric |  |
| `deep_fumbles` | numeric |  |
| `center_short_contested_catch_rate` | numeric |  |
| `center_behind_los_targets_percent` | numeric |  |
| `right_short_yards_per_reception` | numeric |  |
| `center_deep_targets_percent` | numeric |  |
| `center_behind_los_yards_after_catch_per_reception` | numeric |  |
| `behind_los_touchdowns` | numeric |  |
| `medium_pass_block_rate` | numeric |  |
| `left_behind_los_route_rate` | numeric |  |
| `right_behind_los_targeted_qb_rating` | numeric |  |
| `right_behind_los_drops` | numeric |  |
| `left_behind_los_first_downs` | numeric |  |

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
| `default_season` | numeric |  |
| `default_week` | numeric |  |
| `default_week_group` | character |  |
| `id` | numeric | ID of the player in the 'name' column. |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `seasons` | list | NBA seasons played. |
| `slug` | character | URL slug for the team. |
| `week_groups` | list |  |
| `weeks` | list |  |

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
| `heirarchy` | list |  |
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
| `grades_misc_st` | numeric |  |
| `grades_offense` | numeric | PFF team offense grade (0-100). |
| `grades_overall` | numeric |  |
| `grades_pass` | numeric |  |
| `grades_pass_block` | numeric |  |
| `grades_pass_route` | numeric |  |
| `grades_pass_rush_defense` | numeric |  |
| `grades_run` | numeric |  |
| `grades_run_block` | numeric |  |
| `grades_run_defense` | numeric |  |
| `grades_tackle` | numeric |  |
| `losses` | numeric | Losses against the spread in the split. |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `points_allowed` | numeric | Points for the opponent. |
| `points_scored` | numeric |  |
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
| `current_class` | character |  |
| `current_eligible_year` | numeric |  |
| `dob` | character | Player date of birth. |
| `draft` | list |  |
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
