---
title: "NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet: run–punting"
sidebar_label: "Facet: run–punting"
sidebar_position: 4
description: "NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet: run–punting — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet: run–punting

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
| `declined_penalties` | Float64 | Penalties committed by the player that were declined. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `gap_grades_run_block` | Float64 | PFF run-blocking grade on gap-scheme runs, 0-100. |
| `gap_run_block_percent` | Float64 | Share of run-play snaps spent run blocking on gap-scheme runs. |
| `gap_snap_counts_run_block` | Float64 | Run-blocking snaps played on gap-scheme runs. |
| `gap_snap_counts_run_block_percent` | Float64 | Share of the player's run-blocking snaps on gap-scheme runs. |
| `gap_snap_counts_run_play` | Float64 | Run-play snaps on gap-scheme runs. |
| `grades_run_block` | Float64 | PFF run-blocking grade, 0-100. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `player` | String | Player's display name as PFF lists it. |
| `player_game_count` | Float64 | Number of games the player appeared in over the covered span. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `run_block_percent` | Float64 | Share of run-play snaps spent run blocking. |
| `snap_counts_run_block` | Float64 | Run-blocking snaps played. |
| `snap_counts_run_play` | Float64 | Run-play snaps. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `zone_grades_run_block` | Float64 | PFF run-blocking grade on zone-scheme runs, 0-100. |
| `zone_run_block_percent` | Float64 | Share of run-play snaps spent run blocking on zone-scheme runs. |
| `zone_snap_counts_run_block` | Float64 | Run-blocking snaps played on zone-scheme runs. |
| `zone_snap_counts_run_block_percent` | Float64 | Share of the player's run-blocking snaps on zone-scheme runs. |
| `zone_snap_counts_run_play` | Float64 | Run-play snaps on zone-scheme runs. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_run_blocking-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_run_blocking-example}

```python
pff_facet_run_blocking()
```

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
| `true_pass_set_non_spike_pass_block_percentage` | Float64 | Share of non-spike pass-play snaps spent pass blocking on PFF-designated true pass sets. |
| `true_pass_set_pressures_allowed` | Float64 | Total pressures allowed (sacks, hits, and hurries) on PFF-designated true pass sets. |
| `grades_pass_block` | Float64 | PFF pass-blocking grade, 0-100. |
| `true_pass_set_pass_block_percent` | Float64 | Share of pass-play snaps spent pass blocking on PFF-designated true pass sets. |
| `pbe` | Float64 | PFF Pass Blocking Efficiency rating, pressures allowed per pass-blocking snap weighted toward sacks. |
| `non_spike_pass_block_percentage` | Float64 | Share of non-spike pass-play snaps spent pass blocking. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `hits_allowed` | Float64 | Quarterback hits allowed. |
| `true_pass_set_non_spike_pass_block` | Float64 | Pass-blocking snaps excluding spike plays on PFF-designated true pass sets. |
| `player_game_count` | Float64 | Number of games the player appeared in over the covered span. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `hurries_allowed` | Float64 | Quarterback hurries allowed. |
| `true_pass_set_hurries_allowed` | Float64 | Quarterback hurries allowed on PFF-designated true pass sets. |
| `true_pass_set_snap_counts_pass_play` | Float64 | Pass-play snaps on PFF-designated true pass sets. |
| `true_pass_set_hits_allowed` | Float64 | Quarterback hits allowed on PFF-designated true pass sets. |
| `pressures_allowed` | Float64 | Total pressures allowed (sacks, hits, and hurries). |
| `true_pass_set_pbe` | Float64 | PFF Pass Blocking Efficiency rating, pressures allowed per pass-blocking snap weighted toward sacks on PFF-designated true pass sets. |
| `snap_counts_pass_play` | Float64 | Pass-play snaps. |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `sacks_allowed` | Float64 | Sacks allowed by the player in pass protection. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | Float64 | Penalties committed by the player that were declined. |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `true_pass_set_grades_pass_block` | Float64 | PFF pass-blocking grade on PFF-designated true pass sets, 0-100. |
| `non_spike_pass_block` | Float64 | Pass-blocking snaps excluding spike plays. |
| `true_pass_set_snap_counts_pass_block` | Float64 | Pass-blocking snaps played on PFF-designated true pass sets. |
| `player` | String | Player's display name as PFF lists it. |
| `true_pass_set_sacks_allowed` | Float64 | Sacks allowed on PFF-designated true pass sets. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `snap_counts_pass_block` | Float64 | Pass-blocking snaps played. |
| `pass_block_percent` | Float64 | Share of pass-play snaps spent pass blocking. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_pass_blocking-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_pass_blocking-example}

```python
pff_facet_pass_blocking()
```

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
| `grades_offense` | Float64 | PFF overall offense grade (0-100). |
| `twp_rate` | Float64 | Turnover-worthy-play rate. |
| `btt_rate` | Float64 | Big-time-throw rate. |
| `spikes` | Float64 | Clock-stopping spike plays. |
| `dropbacks` | Float64 | Total quarterback dropbacks. |
| `thrown_aways` | Float64 | Passes intentionally thrown away. |
| `draft_season` | Float64 | Draft class (year) of the player. |
| `team_name` | String | Team name/abbreviation the player is credited to for the range. |
| `grades_pass` | Float64 | PFF passing grade (0-100). |
| `hit_as_threw` | Float64 | Plays where the quarterback was hit as he threw. |
| `first_downs` | Float64 | Passing first downs. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `sack_percent` | Float64 | Sack rate (sacks per dropback). |
| `bats` | Float64 | Passes batted at the line. |
| `sacks` | Float64 | Times the passer was sacked. |
| `player_game_count` | Float64 | Games with at least one qualifying dropback in the requested range. |
| `eligible_season` | Float64 | First eligible season for the player. |
| `completions` | Float64 | Completed passes by the passer. |
| `yards` | Float64 | Total passing yards gained. |
| `accuracy_percent` | Float64 | Charted accuracy percentage. |
| `scrambles` | Float64 | Scramble plays. |
| `interceptions` | Float64 | Interceptions thrown. |
| `drop_rate` | Float64 | Receiver drop rate on the quarterback's throws. |
| `grades_run` | Float64 | PFF rushing grade (0-100). |
| `qb_rating` | Float64 | NFL passer rating. |
| `completion_percent` | Float64 | Completion percentage. |
| `penalties` | Float64 | Penalties charged. |
| `attempts` | Float64 | Pass attempts thrown by the passer. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | Float64 | Declined penalties. |
| `passing_snaps` | Float64 | Number of passing snaps played. |
| `pressure_to_sack_rate` | Float64 | Percentage of pressured dropbacks that ended in a sack. |
| `ypa` | Float64 | Yards gained per pass attempt. |
| `drops` | Float64 | Passes dropped by the passer's receivers. |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `grades_hands_fumble` | Float64 | PFF hands (fumble) grade for the player, reflecting ball security (0-100). |
| `avg_time_to_throw` | Float64 | Average time to throw per dropback, in seconds from snap to release. |
| `big_time_throws` | Float64 | Number of big-time throws, per PFF's highest-value, highest-difficulty throw designation. |
| `player` | String | Player's display name as PFF lists it. |
| `positive_epa_percent` | Float64 | Percentage of dropbacks with positive expected points added. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `avg_depth_of_target` | Float64 | Average depth of target in air yards. |
| `turnover_worthy_plays` | Float64 | Number of turnover-worthy plays, plays PFF charts as deserving of a turnover. |
| `epa` | Float64 | Expected points added per play on the passer's dropbacks, as computed by PFF (an average such as 0.14, not a total). |
| `aimed_passes` | Float64 | Aimed passes: attempts excluding throwaways, spikes, batted passes and throws made while hit (the accuracy_percent denominator). |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `touchdowns` | Float64 | Number of passing touchdowns thrown. |
| `def_gen_pressures` | Float64 | Number of defense-generated pressures on the player's dropbacks, as charted by PFF. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_passing_summary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_passing_summary-example}

```python
pff_facet_passing_summary()
```

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
| `touchbacks` | Float64 | Punts resulting in touchbacks. |
| `attempts_with_hangtime` | Float64 | Punts with a PFF-recorded hangtime. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `percent_returned` | Float64 | Percentage of the player's punts that were returned. |
| `fair_catches` | Float64 | Punts fair-caught by the return team. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `player_game_count` | Float64 | Number of games the player appeared in over the covered span. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `average_net_yards` | Float64 | Average net punting yards per attempt. |
| `yards` | Float64 | Gross punt yards: the summed distance of the player's punts, before any return. |
| `average_hangtime` | Float64 | Average punt hangtime in seconds. |
| `total_net_yards` | Float64 | Total net punting yards. |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `attempts` | Float64 | Punts by the player. |
| `inside_twenties` | Float64 | Punts downed inside the opponent 20-yard line. |
| `out_of_bounds` | Float64 | Punts that went out of bounds. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `average_yards_per_return` | Float64 | Average return yards allowed per punt returned. |
| `total_hangtime` | Float64 | Total punt hangtime in seconds. |
| `declined_penalties` | Float64 | Penalties committed by the player that were declined. |
| `returns` | Float64 | Punts returned by the opponent. |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `long` | Float64 | Longest punt in yards. |
| `blocks` | Float64 | Punts that were blocked. |
| `average_yards_per_attempt` | Float64 | Average gross punting yards per attempt. |
| `player` | String | Player's display name as PFF lists it. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `grades_punter` | Float64 | PFF punting grade, 0-100. |
| `return_yards` | Float64 | Return yards gained by the return team on the player's punts. |
| `downeds` | Float64 | Punts downed by the coverage unit. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `snaps` | Float64 | Punting snaps played. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_punting_summary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_punting_summary-example}

```python
pff_facet_punting_summary()
```
