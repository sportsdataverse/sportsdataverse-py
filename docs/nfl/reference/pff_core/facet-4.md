# NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet: run–punting

> NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet: run–punting — function reference in sdv-py, the SportsDataverse Python package.

## pff_facet_run_blocking

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/run_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

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

### Returns {#pff_facet_run_blocking-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `gap_grades_run_block` | numeric | PFF run-blocking grade on gap-scheme runs, 0-100. |
| `gap_run_block_percent` | numeric | Share of run-play snaps spent run blocking on gap-scheme runs. |
| `gap_snap_counts_run_block` | numeric | Run-blocking snaps played on gap-scheme runs. |
| `gap_snap_counts_run_block_percent` | numeric | Share of the player's run-blocking snaps on gap-scheme runs. |
| `gap_snap_counts_run_play` | numeric | Run-play snaps on gap-scheme runs. |
| `grades_run_block` | numeric | PFF run-blocking grade, 0-100. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `player` | character | Player's display name as PFF lists it. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `run_block_percent` | numeric | Share of run-play snaps spent run blocking. |
| `snap_counts_run_block` | numeric | Run-blocking snaps played. |
| `snap_counts_run_play` | numeric | Run-play snaps. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `zone_grades_run_block` | numeric | PFF run-blocking grade on zone-scheme runs, 0-100. |
| `zone_run_block_percent` | numeric | Share of run-play snaps spent run blocking on zone-scheme runs. |
| `zone_snap_counts_run_block` | numeric | Run-blocking snaps played on zone-scheme runs. |
| `zone_snap_counts_run_block_percent` | numeric | Share of the player's run-blocking snaps on zone-scheme runs. |
| `zone_snap_counts_run_play` | numeric | Run-play snaps on zone-scheme runs. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_facet_run_blocking-example}

```python
pff_facet_run_blocking()
```

_Last validated n/a._

## pff_facet_pass_blocking

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/pass_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

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

### Returns {#pff_facet_pass_blocking-returns}

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
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
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
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `sacks_allowed` | numeric | Sacks allowed by the player in pass protection. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `true_pass_set_grades_pass_block` | numeric | PFF pass-blocking grade on PFF-designated true pass sets, 0-100. |
| `non_spike_pass_block` | numeric | Pass-blocking snaps excluding spike plays. |
| `true_pass_set_snap_counts_pass_block` | numeric | Pass-blocking snaps played on PFF-designated true pass sets. |
| `player` | character | Player's display name as PFF lists it. |
| `true_pass_set_sacks_allowed` | numeric | Sacks allowed on PFF-designated true pass sets. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `snap_counts_pass_block` | numeric | Pass-blocking snaps played. |
| `pass_block_percent` | numeric | Share of pass-play snaps spent pass blocking. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_facet_pass_blocking-example}

```python
pff_facet_pass_blocking()
```

_Last validated n/a._

## pff_facet_passing_summary

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

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

### Returns {#pff_facet_passing_summary-returns}

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
| `drops` | numeric | Passes dropped by the passer's receivers. |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100). |
| `avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release. |
| `big_time_throws` | numeric | Number of big-time throws, per PFF's highest-value, highest-difficulty throw designation. |
| `player` | character | Player's display name as PFF lists it. |
| `positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `avg_depth_of_target` | numeric | Average depth of target in air yards. |
| `turnover_worthy_plays` | numeric | Number of turnover-worthy plays, plays PFF charts as deserving of a turnover. |
| `epa` | numeric | Expected points added per play on the passer's dropbacks, as computed by PFF (an average such as 0.14, not a total). |
| `aimed_passes` | numeric | Aimed passes: attempts excluding throwaways, spikes, batted passes and throws made while hit (the accuracy_percent denominator). |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `touchdowns` | numeric | Number of passing touchdowns thrown. |
| `def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks, as charted by PFF. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_facet_passing_summary-example}

```python
pff_facet_passing_summary()
```

_Last validated n/a._

## pff_facet_punting_summary

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /punting/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

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

### Returns {#pff_facet_punting_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `touchbacks` | numeric | Punts resulting in touchbacks. |
| `attempts_with_hangtime` | numeric | Punts with a PFF-recorded hangtime. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `percent_returned` | numeric | Percentage of the player's punts that were returned. |
| `fair_catches` | numeric | Punts fair-caught by the return team. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `average_net_yards` | numeric | Average net punting yards per attempt. |
| `yards` | numeric | Gross punt yards: the summed distance of the player's punts, before any return. |
| `average_hangtime` | numeric | Average punt hangtime in seconds. |
| `total_net_yards` | numeric | Total net punting yards. |
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `attempts` | numeric | Punts by the player. |
| `inside_twenties` | numeric | Punts downed inside the opponent 20-yard line. |
| `out_of_bounds` | numeric | Punts that went out of bounds. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `average_yards_per_return` | numeric | Average return yards allowed per punt returned. |
| `total_hangtime` | numeric | Total punt hangtime in seconds. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `returns` | numeric | Punts returned by the opponent. |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `long` | numeric | Longest punt in yards. |
| `blocks` | numeric | Punts that were blocked. |
| `average_yards_per_attempt` | numeric | Average gross punting yards per attempt. |
| `player` | character | Player's display name as PFF lists it. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `grades_punter` | numeric | PFF punting grade, 0-100. |
| `return_yards` | numeric | Return yards gained by the return team on the player's punts. |
| `downeds` | numeric | Punts downed by the coverage unit. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `snaps` | numeric | Punting snaps played. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_facet_punting_summary-example}

```python
pff_facet_punting_summary()
```

_Last validated n/a._
