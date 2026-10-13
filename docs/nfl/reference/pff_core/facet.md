# NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet: run–passing

> NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet: run–passing — function reference in sdv-py, the SportsDataverse Python package.

## pff_facet_run_defense_summary

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/run (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

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

### Returns {#pff_facet_run_defense_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `assists` | numeric | Assisted tackles credited to the player. |
| `avg_depth_of_tackle` | numeric | Average depth downfield, in yards, at which the player made his tackles on run plays. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `forced_fumbles` | numeric | Fumbles forced by the player. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `grades_coverage_defense` | numeric | PFF coverage grade, 0-100. |
| `grades_defense` | numeric | PFF overall defense grade, 0-100. |
| `grades_defense_penalty` | numeric | PFF defensive penalty grade, 0-100. |
| `grades_pass_rush_defense` | numeric | PFF pass-rush grade, 0-100. |
| `grades_run_defense` | numeric | PFF run-defense grade, 0-100. |
| `grades_tackle` | numeric | PFF tackling grade, 0-100. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `missed_tackle_rate` | numeric | Share of tackle attempts the player missed. |
| `missed_tackles` | numeric | Missed tackles. |
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `player` | character | Player's display name as PFF lists it. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `run_stop_opp` | numeric | Run-defense snaps PFF counts as run-stop opportunities. |
| `snap_counts_run` | numeric | Run-defense snaps played. |
| `stop_percent` | numeric | Percentage of run-stop opportunities converted into stops. |
| `stops` | numeric | Stops, PFF's tackles that constitute a failed play for the offense. |
| `tackles` | numeric | Tackles made by the player, as charted by PFF. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_facet_run_defense_summary-example}

```python
pff_facet_run_defense_summary()
```

_Last validated n/a._

## pff_facet_field_goal_summary

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /field_goal/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

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

### Returns {#pff_facet_field_goal_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `twenty_attempts` | numeric | Field goals attempted from 20-29 yards. |
| `pat_percent` | numeric | Extra-point percentage. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `forty_made` | numeric | Field goals made from 40-49 yards. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `fifty_percent` | numeric | Field-goal percentage from 50 or more yards. |
| `total_made` | numeric | Total field goals made across all distances. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
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
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `pat_made` | numeric | Extra points made. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `one_percent` | numeric | Field-goal percentage from 1-19 yards. |
| `total_percent` | numeric | Overall field-goal percentage. |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `twenty_percent` | numeric | Field-goal percentage from 20-29 yards. |
| `forty_percent` | numeric | Field-goal percentage from 40-49 yards. |
| `player` | character | Player's display name as PFF lists it. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `fifty_made` | numeric | Field goals made from 50 or more yards. |
| `thirty_made` | numeric | Field goals made from 30-39 yards. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_facet_field_goal_summary-example}

```python
pff_facet_field_goal_summary()
```

_Last validated n/a._

## pff_facet_coverage_summary

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

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

### Returns {#pff_facet_coverage_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `targets` | numeric | Passes thrown into the player's coverage (targets allowed). |
| `yards_per_coverage_snap` | numeric | Receiving yards allowed per coverage snap. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `missed_tackles` | numeric | Missed tackles. |
| `catch_rate` | numeric | Completion percentage allowed on targets into the player's coverage. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `tackles` | numeric | Tackles made by the player, as charted by PFF. |
| `coverage_percent` | numeric | Share of pass-play snaps spent in coverage. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `dropped_ints` | numeric | Interception chances PFF charted as dropped. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `grades_tackle` | numeric | PFF tackling grade, 0-100. |
| `yards` | numeric | Receiving yards allowed in the player's coverage. |
| `receptions` | numeric | Receptions allowed in the player's coverage. |
| `forced_incompletion_rate` | numeric | Share of targets into the player's coverage with a PFF-charted forced incompletion. |
| `grades_coverage_defense` | numeric | PFF coverage grade, 0-100. |
| `interceptions` | numeric | Interceptions made by the player in coverage. |
| `snap_counts_coverage` | numeric | Coverage snaps played. |
| `grades_run_defense` | numeric | PFF run-defense grade, 0-100. |
| `snap_counts_pass_play` | numeric | Pass-play snaps. |
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `forced_incompletes` | numeric | Incompletions forced by the player's coverage, per PFF charting. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `coverage_snaps_per_target` | numeric | Coverage snaps played per target into the player's coverage. |
| `stops` | numeric | Stops, PFF's tackles that constitute a failed play for the offense. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `longest` | numeric | Longest completion allowed, in yards. |
| `missed_tackle_rate` | numeric | Share of tackle attempts the player missed. |
| `grades_defense` | numeric | PFF overall defense grade, 0-100. |
| `yards_per_reception` | numeric | Average yards allowed per reception. |
| `grades_defense_penalty` | numeric | PFF defensive penalty grade, 0-100. |
| `player` | character | Player's display name as PFF lists it. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `yards_after_catch` | numeric | Yards after the catch allowed in the player's coverage. |
| `avg_depth_of_target` | numeric | Average depth of targets into the player's coverage, in yards downfield. |
| `pass_break_ups` | numeric | Passes broken up. |
| `qb_rating_against` | numeric | NFL passer rating allowed on targets into the player's coverage. |
| `coverage_snaps_per_reception` | numeric | Coverage snaps played per reception allowed. |
| `assists` | numeric | Assisted tackles credited to the player. |
| `grades_pass_rush_defense` | numeric | PFF pass-rush grade, 0-100. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `touchdowns` | numeric | Touchdowns allowed into the player's coverage. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_facet_coverage_summary-example}

```python
pff_facet_coverage_summary()
```

_Last validated n/a._

## pff_facet_kicking_summary

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /kickoff/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

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

### Returns {#pff_facet_kicking_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `attempts` | numeric | Kickoffs by the player. |
| `attempts_with_hangtime` | numeric | Kickoffs with a PFF-recorded hangtime. |
| `average_distance` | numeric | Average kickoff distance in yards. |
| `average_hangtime` | numeric | Average kickoff hangtime in seconds. |
| `average_starting_field_position` | numeric | Average opponent starting field position following the player's kickoffs. |
| `average_yards_per_return` | numeric | Average return yards allowed per kickoff returned. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `fair_catches` | numeric | Kickoffs fair-caught by the return team. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `grades_kickoff_kicker` | numeric | PFF kickoff kicking grade, 0-100. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `kicked_yards` | numeric | Total kickoff yards. |
| `kicks_returned` | numeric | Kickoffs returned by the opponent. |
| `onside_kicks` | numeric | Onside kicks attempted. |
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `percent_returned` | numeric | Percentage of the player's kickoffs that were returned. |
| `player` | character | Player's display name as PFF lists it. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `return_yards` | numeric | Return yards gained by the return team on the player's kickoffs. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `total_hangtime` | numeric | Total kickoff hangtime in seconds. |
| `touchbacks` | numeric | Kickoffs resulting in touchbacks. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_facet_kicking_summary-example}

```python
pff_facet_kicking_summary()
```

_Last validated n/a._

## pff_facet_blocking_summary

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

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

### Returns {#pff_facet_blocking_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `grades_pass_block` | numeric | PFF pass-blocking grade, 0-100. |
| `grades_offense` | numeric | PFF overall offense grade, 0-100. |
| `pbe` | numeric | PFF Pass Blocking Efficiency rating, pressures allowed per pass-blocking snap weighted toward sacks. |
| `non_spike_pass_block_percentage` | numeric | Share of non-spike pass-play snaps spent pass blocking. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `snap_counts_rg` | numeric | Snaps aligned at right guard. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
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
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `sacks_allowed` | numeric | Sacks allowed by the player in pass protection. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `snap_counts_te` | numeric | Snaps aligned at tight end. |
| `snap_counts_rt` | numeric | Snaps aligned at right tackle. |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `non_spike_pass_block` | numeric | Pass-blocking snaps excluding spike plays. |
| `snap_counts_lt` | numeric | Snaps aligned at left tackle. |
| `player` | character | Player's display name as PFF lists it. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `snap_counts_pass_block` | numeric | Pass-blocking snaps played. |
| `pass_block_percent` | numeric | Share of pass-play snaps spent pass blocking. |
| `snap_counts_lg` | numeric | Snaps aligned at left guard. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_facet_blocking_summary-example}

```python
pff_facet_blocking_summary()
```

_Last validated n/a._

## pff_facet_defense_summary

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

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

### Returns {#pff_facet_defense_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `targets` | numeric | Passes thrown into the player's coverage (targets allowed). |
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
| `yards` | numeric | Receiving yards allowed in the player's coverage. |
| `receptions` | numeric | Receptions allowed in the player's coverage. |
| `grades_coverage_defense` | numeric | PFF coverage grade (0-100). |
| `hurries` | numeric | Quarterback hurries recorded. |
| `interceptions` | numeric | Interceptions made in coverage. |
| `snap_counts_coverage` | numeric | Coverage snaps played. |
| `snap_counts_dl_over_t` | numeric | Defensive-line snaps aligned head-up over the offensive tackle. |
| `snap_counts_dl_a_gap` | numeric | Defensive-line snaps aligned in the A gap. |
| `fumble_recoveries` | numeric | Opponent fumbles recovered by the player. |
| `grades_run_defense` | numeric | PFF run-defense grade (0-100). |
| `snap_counts_corner` | numeric | Snaps aligned at outside cornerback. |
| `hits` | numeric | Quarterback hits recorded by the player. |
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `batted_passes` | numeric | Passes batted down at the line of scrimmage. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `stops` | numeric | Tackles that constitute an offensive failure ("stops"). |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `total_pressures` | numeric | Total quarterback pressures (sacks + hits + hurries). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `fumble_recovery_touchdowns` | numeric | Touchdowns scored on fumble recoveries. |
| `longest` | numeric | Longest completion allowed, in yards. |
| `snap_counts_slot` | numeric | Snaps aligned in the slot. |
| `missed_tackle_rate` | numeric | Share of tackle attempts the player missed. |
| `grades_defense` | numeric | PFF overall defense grade (0-100). |
| `yards_per_reception` | numeric | Average yards allowed per reception. |
| `grades_defense_penalty` | numeric | PFF defensive penalty grade, 0-100. |
| `safeties` | numeric | Safeties recorded by the player. |
| `player` | character | Player's display name as PFF lists it. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `snap_counts_defense` | numeric | Total defensive snaps played. |
| `yards_after_catch` | numeric | Yards after the catch allowed in the player's coverage. |
| `snap_counts_dl_b_gap` | numeric | Defensive-line snaps aligned in the B gap. |
| `pass_break_ups` | numeric | Passes broken up. |
| `qb_rating_against` | numeric | NFL passer rating allowed on targets into the player's coverage. |
| `snap_counts_run_defense` | numeric | Run-defense snaps played. |
| `tackles_for_loss` | numeric | Tackles for loss made by the player. |
| `assists` | numeric | Assisted tackles. |
| `grades_pass_rush_defense` | numeric | PFF pass-rush grade, 0-100. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `snap_counts_fs` | numeric | Snaps aligned at free safety. |
| `touchdowns` | numeric | Touchdowns allowed into the player's coverage. |
| `snap_counts_dl_outside_t` | numeric | Defensive-line snaps aligned outside the offensive tackle. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_facet_defense_summary-example}

```python
pff_facet_defense_summary()
```

_Last validated n/a._

## pff_facet_offense_summary

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

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

### Returns {#pff_facet_offense_summary-returns}

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
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `player` | character | Player's display name as PFF lists it. |
| `player_game_count` | numeric | Games with at least one qualifying snap in the requested range. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `snap_counts_pass` | numeric | Pass-play snaps spent as the passer, rather than blocking or running a route. |
| `snap_counts_pass_block` | numeric | Pass-blocking snaps played. |
| `snap_counts_pass_route` | numeric | Snaps spent running a pass route. |
| `snap_counts_run` | numeric | Run-play snaps spent as a runner, rather than run blocking. |
| `snap_counts_run_block` | numeric | Run-blocking snaps played. |
| `snap_counts_total` | numeric | Total offensive snaps played. |
| `snap_counts_total_pass` | numeric | Total pass-play snaps across passing, pass blocking, and route running. |
| `snap_counts_total_run` | numeric | Total run-play snaps across rushing and run blocking. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `grades_pass_block` | numeric | PFF pass-blocking grade (0-100). |
| `grades_pass_route` | numeric | PFF receiving/route grade (0-100). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_facet_offense_summary-example}

```python
pff_facet_offense_summary()
```

_Last validated n/a._

## pff_facet_passing_allowed_pressure

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/allowed_pressure (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

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

### Returns {#pff_facet_passing_allowed_pressure-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `te_percent` | numeric | Share of allowed pressures attributed to tight ends, expressed as a percentage. |
| `draft_season` | numeric | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `pressures_lt` | numeric | Number of allowed pressures PFF attributes to left tackle. |
| `pressures_rg` | numeric | Number of allowed pressures PFF attributes to right guard. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `lt_percent` | numeric | Share of allowed pressures attributed to left tackle, expressed as a percentage. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
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
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `sacks_allowed` | numeric | Sacks allowed on the quarterback's dropbacks, as attributed by PFF. |
| `ol_te_percent` | numeric | Share of allowed pressures attributed to the offensive line and tight ends combined, expressed as a percentage. |
| `allowed_pressure_dropbacks` | numeric | Number of dropbacks over which allowed pressures are attributed, from the PFF allowed-pressure facet. |
| `pressures_other` | numeric | Number of allowed pressures PFF attributes to other players outside the listed blocking positions. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | numeric | Number of declined penalties committed by the player. |
| `ce_percent` | numeric | Share of allowed pressures attributed to center, expressed as a percentage. |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `pressures_te` | numeric | Number of allowed pressures PFF attributes to tight ends. |
| `pressures_self` | numeric | Number of allowed pressures PFF attributes to the quarterback himself. |
| `lg_percent` | numeric | Share of allowed pressures attributed to left guard, expressed as a percentage. |
| `player` | character | Player's display name as PFF lists it. |
| `rt_percent` | numeric | Share of allowed pressures attributed to right tackle, expressed as a percentage. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `other_percent` | numeric | Share of allowed pressures attributed to other players outside the listed blocking positions, expressed as a percentage. |
| `pressures_off` | numeric | Number of allowed pressures PFF attributes to the offense without a specific blocker charged. |
| `rg_percent` | numeric | Share of allowed pressures attributed to right guard, expressed as a percentage. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_facet_passing_allowed_pressure-example}

```python
pff_facet_passing_allowed_pressure()
```

_Last validated n/a._

## pff_facet_pass_rush_summary

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/pass_rush (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

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

### Returns {#pff_facet_pass_rush_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `true_pass_set_total_pressures` | numeric | Total pressures generated (sacks, hits, and hurries) on PFF-designated true pass sets. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `true_pass_set_prp` | numeric | PFF Pass Rush Productivity rating, pressure generated per pass-rush snap weighted toward sacks on PFF-designated true pass sets. |
| `true_pass_set_hurries` | numeric | Quarterback hurries recorded on PFF-designated true pass sets. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `prp` | numeric | PFF Pass Rush Productivity rating, pressure generated per pass-rush snap weighted toward sacks. |
| `true_pass_set_sacks` | numeric | Sacks recorded on PFF-designated true pass sets. |
| `pass_rush_win_rate` | numeric | Percentage of pass-rush snaps with a PFF-charted pass-rush win. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `sacks` | numeric | Sacks recorded by the pass rusher. |
| `snap_counts_pass_rush` | numeric | Pass-rush snaps played. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `true_pass_set_snap_counts_pass_play` | numeric | Pass-play snaps on PFF-designated true pass sets. |
| `pass_rush_wins` | numeric | PFF-charted pass-rush wins. |
| `hurries` | numeric | Quarterback hurries recorded. |
| `pass_rush_opp` | numeric | Pass-rush snaps PFF counts as pressure opportunities. |
| `snap_counts_pass_play` | numeric | Pass-play snaps. |
| `hits` | numeric | Quarterback hits recorded by the pass rusher. |
| `true_pass_set_pass_rush_win_rate` | numeric | Percentage of pass-rush snaps with a PFF-charted pass-rush win on PFF-designated true pass sets. |
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `batted_passes` | numeric | Passes batted down at the line of scrimmage. |
| `true_pass_set_hits` | numeric | Quarterback hits recorded on PFF-designated true pass sets. |
| `true_pass_set_snap_counts_pass_rush` | numeric | Pass-rush snaps played on PFF-designated true pass sets. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `true_pass_set_pass_rush_wins` | numeric | PFF-charted pass-rush wins on PFF-designated true pass sets. |
| `total_pressures` | numeric | Total pressures generated (sacks, hits, and hurries). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `true_pass_set_grades_pass_rush_defense` | numeric | PFF pass-rush grade on PFF-designated true pass sets, 0-100. |
| `true_pass_set_batted_passes` | numeric | Passes batted down at the line of scrimmage on PFF-designated true pass sets. |
| `player` | character | Player's display name as PFF lists it. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `pass_rush_percent` | numeric | Share of pass-play snaps spent rushing the passer. |
| `true_pass_set_pass_rush_opp` | numeric | Pass-rush snaps PFF counts as pressure opportunities on PFF-designated true pass sets. |
| `true_pass_set_pass_rush_percent` | numeric | Share of pass-play snaps spent rushing the passer on PFF-designated true pass sets. |
| `grades_pass_rush_defense` | numeric | PFF pass-rush grade, 0-100. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_facet_pass_rush_summary-example}

```python
pff_facet_pass_rush_summary()
```

_Last validated n/a._

## pff_facet_passing_concept

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

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

### Returns {#pff_facet_passing_concept-returns}

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
| `screen_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on screen passes. |
| `screen_thrown_aways` | numeric | Number of intentional throwaways on screen passes. |
| `npa_sacks` | numeric | Number of sacks taken on non-play-action dropbacks. |
| `npa_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on non-play-action dropbacks, as charted by PFF. |
| `no_screen_completions` | numeric | Number of completed passes excluding screen passes. |
| `no_screen_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added excluding screen passes. |
| `screen_spikes` | numeric | Number of clock-stopping spikes on screen passes. |
| `pa_first_downs` | numeric | Number of passing first downs gained on play-action dropbacks. |
| `pa_big_time_throws` | numeric | Number of big-time throws on play-action dropbacks, per PFF's highest-value, highest-difficulty throw designation. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `pa_spikes` | numeric | Number of clock-stopping spikes on play-action dropbacks. |
| `pa_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on play-action dropbacks. |
| `screen_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on screen passes, expressed as a percentage. |
| `no_screen_epa` | numeric | Total expected points added (EPA) on the player's dropbacks excluding screen passes. |
| `npa_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on non-play-action dropbacks. |
| `screen_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on screen passes. |
| `screen_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) on screen passes. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
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
| `pa_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on play-action dropbacks. |
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
| `no_screen_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) excluding screen passes, as charted by PFF. |
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
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `no_screen_big_time_throws` | numeric | Number of big-time throws excluding screen passes, per PFF's highest-value, highest-difficulty throw designation. |
| `npa_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on non-play-action dropbacks. |
| `npa_attempts` | numeric | Number of pass attempts on non-play-action dropbacks. |
| `screen_qb_rating` | numeric | Traditional NFL passer rating on screen passes. |
| `npa_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on non-play-action dropbacks. |
| `pa_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on play-action dropbacks. |
| `pa_attempts` | numeric | Number of pass attempts on play-action dropbacks. |
| `npa_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on non-play-action dropbacks, as charted by PFF. |
| `no_screen_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, excluding screen passes. |
| `pa_yards` | numeric | Passing yards gained on play-action dropbacks. |
| `npa_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on non-play-action dropbacks. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `no_screen_scrambles` | numeric | Number of scrambles excluding screen passes. |
| `pa_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on play-action dropbacks, plays PFF charts as deserving of a turnover. |
| `pa_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on play-action dropbacks, as charted by PFF. |
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
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
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
| `player` | character | Player's display name as PFF lists it. |
| `pa_drops` | numeric | Number of catchable passes dropped by receivers on play-action dropbacks. |
| `pa_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on play-action dropbacks, per PFF charting. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `screen_big_time_throws` | numeric | Number of big-time throws on screen passes, per PFF's highest-value, highest-difficulty throw designation. |
| `screen_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on screen passes, as charted by PFF. |
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
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
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

### Example {#pff_facet_passing_concept-example}

```python
pff_facet_passing_concept()
```

_Last validated n/a._
