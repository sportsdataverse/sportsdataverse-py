# NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet: run–pass

> NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet: run–pass — function reference in sdv-py, the SportsDataverse Python package.

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
| `assists` | Float64 | Assisted tackles credited to the player. |
| `avg_depth_of_tackle` | Float64 | Average depth downfield, in yards, at which the player made his tackles on run plays. |
| `declined_penalties` | Float64 | Penalties committed by the player that were declined. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `forced_fumbles` | Float64 | Fumbles forced by the player. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `grades_coverage_defense` | Float64 | PFF coverage grade, 0-100. |
| `grades_defense` | Float64 | PFF overall defense grade, 0-100. |
| `grades_defense_penalty` | Float64 | PFF defensive penalty grade, 0-100. |
| `grades_pass_rush_defense` | Float64 | PFF pass-rush grade, 0-100. |
| `grades_run_defense` | Float64 | PFF run-defense grade, 0-100. |
| `grades_tackle` | Float64 | PFF tackling grade, 0-100. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `missed_tackle_rate` | Float64 | Share of tackle attempts the player missed. |
| `missed_tackles` | Float64 | Missed tackles. |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `player` | String | Player's display name as PFF lists it. |
| `player_game_count` | Float64 | Number of games the player appeared in over the covered span. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `run_stop_opp` | Float64 | Run-defense snaps PFF counts as run-stop opportunities. |
| `snap_counts_run` | Float64 | Run-defense snaps played. |
| `stop_percent` | Float64 | Percentage of run-stop opportunities converted into stops. |
| `stops` | Float64 | Stops, PFF's tackles that constitute a failed play for the offense. |
| `tackles` | Float64 | Tackles made by the player, as charted by PFF. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_run_defense_summary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_run_defense_summary-example}

```python
pff_facet_run_defense_summary()
```

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
| `twenty_attempts` | Float64 | Field goals attempted from 20-29 yards. |
| `pat_percent` | Float64 | Extra-point percentage. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `forty_made` | Float64 | Field goals made from 40-49 yards. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `fifty_percent` | Float64 | Field-goal percentage from 50 or more yards. |
| `total_made` | Float64 | Total field goals made across all distances. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `one_made` | Float64 | Field goals made from 1-19 yards. |
| `fifty_attempts` | Float64 | Field goals attempted from 50 or more yards. |
| `forty_attempts` | Float64 | Field goals attempted from 40-49 yards. |
| `player_game_count` | Float64 | Number of games the player appeared in over the covered span. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `thirty_percent` | Float64 | Field-goal percentage from 30-39 yards. |
| `total_attempts` | Float64 | Total field goals attempted across all distances. |
| `pat_attempts` | Float64 | Extra points attempted. |
| `twenty_made` | Float64 | Field goals made from 20-29 yards. |
| `one_attempts` | Float64 | Field goals attempted from 1-19 yards. |
| `grades_fgep_kicker` | Float64 | PFF field-goal and extra-point kicking grade, 0-100. |
| `thirty_attempts` | Float64 | Field goals attempted from 30-39 yards. |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `pat_made` | Float64 | Extra points made. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | Float64 | Penalties committed by the player that were declined. |
| `one_percent` | Float64 | Field-goal percentage from 1-19 yards. |
| `total_percent` | Float64 | Overall field-goal percentage. |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `twenty_percent` | Float64 | Field-goal percentage from 20-29 yards. |
| `forty_percent` | Float64 | Field-goal percentage from 40-49 yards. |
| `player` | String | Player's display name as PFF lists it. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `fifty_made` | Float64 | Field goals made from 50 or more yards. |
| `thirty_made` | Float64 | Field goals made from 30-39 yards. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_field_goal_summary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_field_goal_summary-example}

```python
pff_facet_field_goal_summary()
```

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
| `targets` | Float64 | Passes thrown into the player's coverage (targets allowed). |
| `yards_per_coverage_snap` | Float64 | Receiving yards allowed per coverage snap. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `missed_tackles` | Float64 | Missed tackles. |
| `catch_rate` | Float64 | Completion percentage allowed on targets into the player's coverage. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `tackles` | Float64 | Tackles made by the player, as charted by PFF. |
| `coverage_percent` | Float64 | Share of pass-play snaps spent in coverage. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `dropped_ints` | Float64 | Interception chances PFF charted as dropped. |
| `player_game_count` | Float64 | Number of games the player appeared in over the covered span. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `grades_tackle` | Float64 | PFF tackling grade, 0-100. |
| `yards` | Float64 | Receiving yards allowed in the player's coverage. |
| `receptions` | Float64 | Receptions allowed in the player's coverage. |
| `forced_incompletion_rate` | Float64 | Share of targets into the player's coverage with a PFF-charted forced incompletion. |
| `grades_coverage_defense` | Float64 | PFF coverage grade, 0-100. |
| `interceptions` | Float64 | Interceptions made by the player in coverage. |
| `snap_counts_coverage` | Float64 | Coverage snaps played. |
| `grades_run_defense` | Float64 | PFF run-defense grade, 0-100. |
| `snap_counts_pass_play` | Float64 | Pass-play snaps. |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `forced_incompletes` | Float64 | Incompletions forced by the player's coverage, per PFF charting. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `coverage_snaps_per_target` | Float64 | Coverage snaps played per target into the player's coverage. |
| `stops` | Float64 | Stops, PFF's tackles that constitute a failed play for the offense. |
| `declined_penalties` | Float64 | Penalties committed by the player that were declined. |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `longest` | Float64 | Longest completion allowed, in yards. |
| `missed_tackle_rate` | Float64 | Share of tackle attempts the player missed. |
| `grades_defense` | Float64 | PFF overall defense grade, 0-100. |
| `yards_per_reception` | Float64 | Average yards allowed per reception. |
| `grades_defense_penalty` | Float64 | PFF defensive penalty grade, 0-100. |
| `player` | String | Player's display name as PFF lists it. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `yards_after_catch` | Float64 | Yards after the catch allowed in the player's coverage. |
| `avg_depth_of_target` | Float64 | Average depth of targets into the player's coverage, in yards downfield. |
| `pass_break_ups` | Float64 | Passes broken up. |
| `qb_rating_against` | Float64 | NFL passer rating allowed on targets into the player's coverage. |
| `coverage_snaps_per_reception` | Float64 | Coverage snaps played per reception allowed. |
| `assists` | Float64 | Assisted tackles credited to the player. |
| `grades_pass_rush_defense` | Float64 | PFF pass-rush grade, 0-100. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `touchdowns` | Float64 | Touchdowns allowed into the player's coverage. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_coverage_summary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_coverage_summary-example}

```python
pff_facet_coverage_summary()
```

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
| `attempts` | Float64 | Kickoffs by the player. |
| `attempts_with_hangtime` | Float64 | Kickoffs with a PFF-recorded hangtime. |
| `average_distance` | Float64 | Average kickoff distance in yards. |
| `average_hangtime` | Float64 | Average kickoff hangtime in seconds. |
| `average_starting_field_position` | Float64 | Average opponent starting field position following the player's kickoffs. |
| `average_yards_per_return` | Float64 | Average return yards allowed per kickoff returned. |
| `declined_penalties` | Float64 | Penalties committed by the player that were declined. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `fair_catches` | Float64 | Kickoffs fair-caught by the return team. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `grades_kickoff_kicker` | Float64 | PFF kickoff kicking grade, 0-100. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `kicked_yards` | Float64 | Total kickoff yards. |
| `kicks_returned` | Float64 | Kickoffs returned by the opponent. |
| `onside_kicks` | Float64 | Onside kicks attempted. |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `percent_returned` | Float64 | Percentage of the player's kickoffs that were returned. |
| `player` | String | Player's display name as PFF lists it. |
| `player_game_count` | Float64 | Number of games the player appeared in over the covered span. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `return_yards` | Float64 | Return yards gained by the return team on the player's kickoffs. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `total_hangtime` | Float64 | Total kickoff hangtime in seconds. |
| `touchbacks` | Float64 | Kickoffs resulting in touchbacks. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_kicking_summary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_kicking_summary-example}

```python
pff_facet_kicking_summary()
```

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
| `grades_pass_block` | Float64 | PFF pass-blocking grade, 0-100. |
| `grades_offense` | Float64 | PFF overall offense grade, 0-100. |
| `pbe` | Float64 | PFF Pass Blocking Efficiency rating, pressures allowed per pass-blocking snap weighted toward sacks. |
| `non_spike_pass_block_percentage` | Float64 | Share of non-spike pass-play snaps spent pass blocking. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `snap_counts_rg` | Float64 | Snaps aligned at right guard. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `snap_counts_ce` | Float64 | Snaps aligned at center. |
| `hits_allowed` | Float64 | Quarterback hits allowed. |
| `block_percent` | Float64 | Share of offensive snaps spent blocking. |
| `snap_counts_offense` | Float64 | Offensive snaps played. |
| `player_game_count` | Float64 | Number of games the player appeared in over the covered span. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `snap_counts_block` | Float64 | Total blocking snaps played. |
| `hurries_allowed` | Float64 | Quarterback hurries allowed. |
| `grades_run_block` | Float64 | PFF run-blocking grade, 0-100. |
| `snap_counts_run_block` | Float64 | Run-blocking snaps played. |
| `pressures_allowed` | Float64 | Total pressures allowed (sacks, hits, and hurries). |
| `snap_counts_pass_play` | Float64 | Pass-play snaps. |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `sacks_allowed` | Float64 | Sacks allowed by the player in pass protection. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | Float64 | Penalties committed by the player that were declined. |
| `snap_counts_te` | Float64 | Snaps aligned at tight end. |
| `snap_counts_rt` | Float64 | Snaps aligned at right tackle. |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `non_spike_pass_block` | Float64 | Pass-blocking snaps excluding spike plays. |
| `snap_counts_lt` | Float64 | Snaps aligned at left tackle. |
| `player` | String | Player's display name as PFF lists it. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `snap_counts_pass_block` | Float64 | Pass-blocking snaps played. |
| `pass_block_percent` | Float64 | Share of pass-play snaps spent pass blocking. |
| `snap_counts_lg` | Float64 | Snaps aligned at left guard. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_blocking_summary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_blocking_summary-example}

```python
pff_facet_blocking_summary()
```

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
| `targets` | Float64 | Passes thrown into the player's coverage (targets allowed). |
| `interception_touchdowns` | Float64 | Touchdowns scored on interception returns. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `forced_fumbles` | Float64 | Forced fumbles. |
| `missed_tackles` | Float64 | Missed tackles. |
| `catch_rate` | Float64 | Completion percentage allowed on targets into the player's coverage. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `tackles` | Float64 | Total tackles made by the defender. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `snap_counts_offball` | Float64 | Snaps aligned as an off-ball linebacker. |
| `snap_counts_box` | Float64 | Snaps aligned in the box. |
| `sacks` | Float64 | Sacks credited. |
| `snap_counts_pass_rush` | Float64 | Pass-rush snaps played. |
| `player_game_count` | Float64 | Games with at least one qualifying snap in the requested range. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `snap_counts_dl` | Float64 | Snaps aligned on the defensive line. |
| `grades_tackle` | Float64 | PFF tackling grade, 0-100. |
| `yards` | Float64 | Receiving yards allowed in the player's coverage. |
| `receptions` | Float64 | Receptions allowed in the player's coverage. |
| `grades_coverage_defense` | Float64 | PFF coverage grade (0-100). |
| `hurries` | Float64 | Quarterback hurries recorded. |
| `interceptions` | Float64 | Interceptions made in coverage. |
| `snap_counts_coverage` | Float64 | Coverage snaps played. |
| `snap_counts_dl_over_t` | Float64 | Defensive-line snaps aligned head-up over the offensive tackle. |
| `snap_counts_dl_a_gap` | Float64 | Defensive-line snaps aligned in the A gap. |
| `fumble_recoveries` | Float64 | Opponent fumbles recovered by the player. |
| `grades_run_defense` | Float64 | PFF run-defense grade (0-100). |
| `snap_counts_corner` | Float64 | Snaps aligned at outside cornerback. |
| `hits` | Float64 | Quarterback hits recorded by the player. |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `batted_passes` | Float64 | Passes batted down at the line of scrimmage. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `stops` | Float64 | Tackles that constitute an offensive failure ("stops"). |
| `declined_penalties` | Float64 | Penalties committed by the player that were declined. |
| `total_pressures` | Float64 | Total quarterback pressures (sacks + hits + hurries). |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `fumble_recovery_touchdowns` | Float64 | Touchdowns scored on fumble recoveries. |
| `longest` | Float64 | Longest completion allowed, in yards. |
| `snap_counts_slot` | Float64 | Snaps aligned in the slot. |
| `missed_tackle_rate` | Float64 | Share of tackle attempts the player missed. |
| `grades_defense` | Float64 | PFF overall defense grade (0-100). |
| `yards_per_reception` | Float64 | Average yards allowed per reception. |
| `grades_defense_penalty` | Float64 | PFF defensive penalty grade, 0-100. |
| `safeties` | Float64 | Safeties recorded by the player. |
| `player` | String | Player's display name as PFF lists it. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `snap_counts_defense` | Float64 | Total defensive snaps played. |
| `yards_after_catch` | Float64 | Yards after the catch allowed in the player's coverage. |
| `snap_counts_dl_b_gap` | Float64 | Defensive-line snaps aligned in the B gap. |
| `pass_break_ups` | Float64 | Passes broken up. |
| `qb_rating_against` | Float64 | NFL passer rating allowed on targets into the player's coverage. |
| `snap_counts_run_defense` | Float64 | Run-defense snaps played. |
| `tackles_for_loss` | Float64 | Tackles for loss made by the player. |
| `assists` | Float64 | Assisted tackles. |
| `grades_pass_rush_defense` | Float64 | PFF pass-rush grade, 0-100. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `snap_counts_fs` | Float64 | Snaps aligned at free safety. |
| `touchdowns` | Float64 | Touchdowns allowed into the player's coverage. |
| `snap_counts_dl_outside_t` | Float64 | Defensive-line snaps aligned outside the offensive tackle. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_defense_summary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_defense_summary-example}

```python
pff_facet_defense_summary()
```

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
| `declined_penalties` | Float64 | Penalties committed by the player that were declined. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `grades_hands_fumble` | Float64 | PFF ball-security (hands/fumble) grade, 0-100. |
| `grades_offense` | Float64 | PFF overall offense grade (0-100). |
| `grades_offense_penalty` | Float64 | PFF offensive penalty grade, 0-100. |
| `grades_pass` | Float64 | PFF passing grade (0-100). |
| `grades_run` | Float64 | PFF rushing grade (0-100). |
| `grades_run_block` | Float64 | PFF run-blocking grade (0-100). |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `player` | String | Player's display name as PFF lists it. |
| `player_game_count` | Float64 | Games with at least one qualifying snap in the requested range. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `snap_counts_pass` | Float64 | Pass-play snaps spent as the passer, rather than blocking or running a route. |
| `snap_counts_pass_block` | Float64 | Pass-blocking snaps played. |
| `snap_counts_pass_route` | Float64 | Snaps spent running a pass route. |
| `snap_counts_run` | Float64 | Run-play snaps spent as a runner, rather than run blocking. |
| `snap_counts_run_block` | Float64 | Run-blocking snaps played. |
| `snap_counts_total` | Float64 | Total offensive snaps played. |
| `snap_counts_total_pass` | Float64 | Total pass-play snaps across passing, pass blocking, and route running. |
| `snap_counts_total_run` | Float64 | Total run-play snaps across rushing and run blocking. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `grades_pass_block` | Float64 | PFF pass-blocking grade (0-100). |
| `grades_pass_route` | Float64 | PFF receiving/route grade (0-100). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_offense_summary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_offense_summary-example}

```python
pff_facet_offense_summary()
```

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
| `te_percent` | Float64 | Share of allowed pressures attributed to tight ends, expressed as a percentage. |
| `draft_season` | Float64 | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `pressures_lt` | Float64 | Number of allowed pressures PFF attributes to left tackle. |
| `pressures_rg` | Float64 | Number of allowed pressures PFF attributes to right guard. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `lt_percent` | Float64 | Share of allowed pressures attributed to left tackle, expressed as a percentage. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `hits_allowed` | Float64 | Number of quarterback hits allowed on the player's dropbacks, as charted by PFF. |
| `pressures_lg` | Float64 | Number of allowed pressures PFF attributes to left guard. |
| `player_game_count` | Float64 | Number of games the player appeared in during the period covered. |
| `eligible_season` | Float64 | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `hurries_allowed` | Float64 | Number of hurries allowed on the player's dropbacks, as charted by PFF. |
| `self_percent` | Float64 | Share of allowed pressures attributed to the quarterback himself, expressed as a percentage. |
| `pressures_rt` | Float64 | Number of allowed pressures PFF attributes to right tackle. |
| `pressures_allowed` | Float64 | Total pressures allowed on the quarterback's dropbacks, as attributed by PFF. |
| `pressures_ol_te` | Float64 | Number of allowed pressures PFF attributes to the offensive line and tight ends combined. |
| `pressures_ce` | Float64 | Number of allowed pressures PFF attributes to center. |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `sacks_allowed` | Float64 | Sacks allowed on the quarterback's dropbacks, as attributed by PFF. |
| `ol_te_percent` | Float64 | Share of allowed pressures attributed to the offensive line and tight ends combined, expressed as a percentage. |
| `allowed_pressure_dropbacks` | Float64 | Number of dropbacks over which allowed pressures are attributed, from the PFF allowed-pressure facet. |
| `pressures_other` | Float64 | Number of allowed pressures PFF attributes to other players outside the listed blocking positions. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | Float64 | Number of declined penalties committed by the player. |
| `ce_percent` | Float64 | Share of allowed pressures attributed to center, expressed as a percentage. |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `pressures_te` | Float64 | Number of allowed pressures PFF attributes to tight ends. |
| `pressures_self` | Float64 | Number of allowed pressures PFF attributes to the quarterback himself. |
| `lg_percent` | Float64 | Share of allowed pressures attributed to left guard, expressed as a percentage. |
| `player` | String | Player's display name as PFF lists it. |
| `rt_percent` | Float64 | Share of allowed pressures attributed to right tackle, expressed as a percentage. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `other_percent` | Float64 | Share of allowed pressures attributed to other players outside the listed blocking positions, expressed as a percentage. |
| `pressures_off` | Float64 | Number of allowed pressures PFF attributes to the offense without a specific blocker charged. |
| `rg_percent` | Float64 | Share of allowed pressures attributed to right guard, expressed as a percentage. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_passing_allowed_pressure-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_passing_allowed_pressure-example}

```python
pff_facet_passing_allowed_pressure()
```

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
| `true_pass_set_total_pressures` | Float64 | Total pressures generated (sacks, hits, and hurries) on PFF-designated true pass sets. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `true_pass_set_prp` | Float64 | PFF Pass Rush Productivity rating, pressure generated per pass-rush snap weighted toward sacks on PFF-designated true pass sets. |
| `true_pass_set_hurries` | Float64 | Quarterback hurries recorded on PFF-designated true pass sets. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `prp` | Float64 | PFF Pass Rush Productivity rating, pressure generated per pass-rush snap weighted toward sacks. |
| `true_pass_set_sacks` | Float64 | Sacks recorded on PFF-designated true pass sets. |
| `pass_rush_win_rate` | Float64 | Percentage of pass-rush snaps with a PFF-charted pass-rush win. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `sacks` | Float64 | Sacks recorded by the pass rusher. |
| `snap_counts_pass_rush` | Float64 | Pass-rush snaps played. |
| `player_game_count` | Float64 | Number of games the player appeared in over the covered span. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `true_pass_set_snap_counts_pass_play` | Float64 | Pass-play snaps on PFF-designated true pass sets. |
| `pass_rush_wins` | Float64 | PFF-charted pass-rush wins. |
| `hurries` | Float64 | Quarterback hurries recorded. |
| `pass_rush_opp` | Float64 | Pass-rush snaps PFF counts as pressure opportunities. |
| `snap_counts_pass_play` | Float64 | Pass-play snaps. |
| `hits` | Float64 | Quarterback hits recorded by the pass rusher. |
| `true_pass_set_pass_rush_win_rate` | Float64 | Percentage of pass-rush snaps with a PFF-charted pass-rush win on PFF-designated true pass sets. |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `batted_passes` | Float64 | Passes batted down at the line of scrimmage. |
| `true_pass_set_hits` | Float64 | Quarterback hits recorded on PFF-designated true pass sets. |
| `true_pass_set_snap_counts_pass_rush` | Float64 | Pass-rush snaps played on PFF-designated true pass sets. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | Float64 | Penalties committed by the player that were declined. |
| `true_pass_set_pass_rush_wins` | Float64 | PFF-charted pass-rush wins on PFF-designated true pass sets. |
| `total_pressures` | Float64 | Total pressures generated (sacks, hits, and hurries). |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `true_pass_set_grades_pass_rush_defense` | Float64 | PFF pass-rush grade on PFF-designated true pass sets, 0-100. |
| `true_pass_set_batted_passes` | Float64 | Passes batted down at the line of scrimmage on PFF-designated true pass sets. |
| `player` | String | Player's display name as PFF lists it. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `pass_rush_percent` | Float64 | Share of pass-play snaps spent rushing the passer. |
| `true_pass_set_pass_rush_opp` | Float64 | Pass-rush snaps PFF counts as pressure opportunities on PFF-designated true pass sets. |
| `true_pass_set_pass_rush_percent` | Float64 | Share of pass-play snaps spent rushing the passer on PFF-designated true pass sets. |
| `grades_pass_rush_defense` | Float64 | PFF pass-rush grade, 0-100. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_pass_rush_summary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_pass_rush_summary-example}

```python
pff_facet_pass_rush_summary()
```
