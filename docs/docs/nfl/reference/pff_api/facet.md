---
title: "NFL — PFF Developer API (api.pff.com, API key) — Facet: offense–passing"
sidebar_label: "Facet: offense–passing"
sidebar_position: 1
description: "NFL — PFF Developer API (api.pff.com, API key) — Facet: offense–passing — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — PFF Developer API (api.pff.com, API key) — Facet: offense–passing

## pff_api_facet_offense_summary

League-wide offense summary leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/offense/summary`

**Valid URL:** [https://api.pff.com/v1/facet/offense/summary?league=nfl&season=2022](https://api.pff.com/v1/facet/offense/summary?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_offense_summary-returns}

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

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_facet_offense_summary-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_offense_summary-example}

```python
pff_api_facet_offense_summary(league='nfl', season='2022')
```

## pff_api_facet_offense_blocking

League-wide blocking leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/offense/blocking`

**Valid URL:** [https://api.pff.com/v1/facet/offense/blocking?league=nfl&season=2022](https://api.pff.com/v1/facet/offense/blocking?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_offense_blocking-returns}

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

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_facet_offense_blocking-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_offense_blocking-example}

```python
pff_api_facet_offense_blocking(league='nfl', season='2022')
```

## pff_api_facet_offense_pass_blocking

League-wide pass-blocking leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/offense/pass_blocking`

**Valid URL:** [https://api.pff.com/v1/facet/offense/pass_blocking?league=nfl&season=2022](https://api.pff.com/v1/facet/offense/pass_blocking?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_offense_pass_blocking-returns}

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

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_facet_offense_pass_blocking-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_offense_pass_blocking-example}

```python
pff_api_facet_offense_pass_blocking(league='nfl', season='2022')
```

## pff_api_facet_offense_run_blocking

League-wide run-blocking leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/offense/run_blocking`

**Valid URL:** [https://api.pff.com/v1/facet/offense/run_blocking?league=nfl&season=2022](https://api.pff.com/v1/facet/offense/run_blocking?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_offense_run_blocking-returns}

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

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_facet_offense_run_blocking-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_offense_run_blocking-example}

```python
pff_api_facet_offense_run_blocking(league='nfl', season='2022')
```

## pff_api_facet_passing_allowed_pressure

League-wide pressure-allowed leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/passing/allowed_pressure`

**Valid URL:** [https://api.pff.com/v1/facet/passing/allowed_pressure?league=nfl&season=2022](https://api.pff.com/v1/facet/passing/allowed_pressure?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_passing_allowed_pressure-returns}

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

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_facet_passing_allowed_pressure-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_passing_allowed_pressure-example}

```python
pff_api_facet_passing_allowed_pressure(league='nfl', season='2022')
```

## pff_api_facet_passing_concept

League-wide passing-by-concept leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/passing/concept`

**Valid URL:** [https://api.pff.com/v1/facet/passing/concept?league=nfl&season=2022](https://api.pff.com/v1/facet/passing/concept?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_passing_concept-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `comp_pct_diff` | Float64 | Difference in completion percentage between play-action and non-play-action attempts (PA minus non-PA), from the PFF passing-concept split. |
| `pa_grades_pass` | Float64 | PFF passing grade (0-100) on play-action dropbacks. |
| `no_screen_grades_offense` | Float64 | PFF overall offense grade for the player (0-100) excluding screen passes. |
| `pa_qb_rating` | Float64 | Traditional NFL passer rating on play-action dropbacks. |
| `no_screen_qb_rating` | Float64 | Traditional NFL passer rating excluding screen passes. |
| `pa_completions` | Float64 | Number of completed passes on play-action dropbacks. |
| `pa_thrown_aways` | Float64 | Number of intentional throwaways on play-action dropbacks. |
| `pa_dropbacks_percent` | Float64 | Share of the player's total dropbacks that came on play-action dropbacks, expressed as a percentage. |
| `ypa_diff` | Float64 | Difference in yards per attempt between play-action and non-play-action attempts (PA minus non-PA), from the PFF passing-concept split. |
| `no_screen_drops` | Float64 | Number of catchable passes dropped by receivers excluding screen passes. |
| `screen_completion_percent` | Float64 | Percentage of pass attempts completed on screen passes. |
| `npa_bats` | Float64 | Number of pass attempts batted down at the line of scrimmage on non-play-action dropbacks. |
| `no_screen_thrown_aways` | Float64 | Number of intentional throwaways excluding screen passes. |
| `pa_grades_run` | Float64 | PFF rushing grade for the player (0-100) on play-action dropbacks. |
| `screen_pressure_to_sack_rate` | Float64 | Percentage of pressured dropbacks that ended in a sack on screen passes. |
| `pa_drop_rate` | Float64 | Percentage of catchable passes dropped by receivers on play-action dropbacks. |
| `screen_grades_offense` | Float64 | PFF overall offense grade for the player (0-100) on screen passes. |
| `dropbacks` | Float64 | Number of dropbacks. |
| `npa_thrown_aways` | Float64 | Number of intentional throwaways on non-play-action dropbacks. |
| `no_screen_def_gen_pressures` | Float64 | Number of defense-generated pressures on the player's dropbacks excluding screen passes, as charted by PFF. |
| `screen_grades_hands_fumble` | Float64 | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on screen passes. |
| `pa_touchdowns` | Float64 | Number of passing touchdowns thrown on play-action dropbacks. |
| `npa_ypa` | Float64 | Yards gained per pass attempt on non-play-action dropbacks. |
| `draft_season` | Float64 | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `screen_avg_time_to_throw` | Float64 | Average time to throw per dropback, in seconds from snap to release, on screen passes. |
| `screen_thrown_aways` | Float64 | Number of intentional throwaways on screen passes. |
| `npa_sacks` | Float64 | Number of sacks taken on non-play-action dropbacks. |
| `npa_aimed_passes` | Float64 | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on non-play-action dropbacks, as charted by PFF. |
| `no_screen_completions` | Float64 | Number of completed passes excluding screen passes. |
| `no_screen_positive_epa_percent` | Float64 | Percentage of dropbacks with positive expected points added excluding screen passes. |
| `screen_spikes` | Float64 | Number of clock-stopping spikes on screen passes. |
| `pa_first_downs` | Float64 | Number of passing first downs gained on play-action dropbacks. |
| `pa_big_time_throws` | Float64 | Number of big-time throws on play-action dropbacks, per PFF's highest-value, highest-difficulty throw designation. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `pa_spikes` | Float64 | Number of clock-stopping spikes on play-action dropbacks. |
| `pa_sack_percent` | Float64 | Percentage of dropbacks that ended in a sack on play-action dropbacks. |
| `screen_dropbacks_percent` | Float64 | Share of the player's total dropbacks that came on screen passes, expressed as a percentage. |
| `no_screen_epa` | Float64 | Total expected points added (EPA) on the player's dropbacks excluding screen passes. |
| `npa_avg_time_to_throw` | Float64 | Average time to throw per dropback, in seconds from snap to release, on non-play-action dropbacks. |
| `screen_bats` | Float64 | Number of pass attempts batted down at the line of scrimmage on screen passes. |
| `screen_grades_run_block` | Float64 | PFF run-blocking grade for the player (0-100) on screen passes. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `no_screen_bats` | Float64 | Number of pass attempts batted down at the line of scrimmage excluding screen passes. |
| `npa_grades_offense` | Float64 | PFF overall offense grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_grades_pass_route` | Float64 | PFF receiving (route) grade for the player (0-100) excluding screen passes. |
| `screen_def_gen_pressures` | Float64 | Number of defense-generated pressures on the player's dropbacks on screen passes, as charted by PFF. |
| `pa_pressure_to_sack_rate` | Float64 | Percentage of pressured dropbacks that ended in a sack on play-action dropbacks. |
| `screen_sack_percent` | Float64 | Percentage of dropbacks that ended in a sack on screen passes. |
| `no_screen_btt_rate` | Float64 | Big-time throws as a percentage of qualifying attempts excluding screen passes, per PFF charting. |
| `npa_btt_rate` | Float64 | Big-time throws as a percentage of qualifying attempts on non-play-action dropbacks, per PFF charting. |
| `screen_interceptions` | Float64 | Number of passes intercepted on screen passes. |
| `no_screen_passing_snaps` | Float64 | Number of passing snaps played excluding screen passes. |
| `no_screen_grades_hands_fumble` | Float64 | PFF hands (fumble) grade for the player, reflecting ball security (0-100) excluding screen passes. |
| `screen_scrambles` | Float64 | Number of scrambles on screen passes. |
| `screen_grades_pass` | Float64 | PFF passing grade (0-100) on screen passes. |
| `npa_qb_rating` | Float64 | Traditional NFL passer rating on non-play-action dropbacks. |
| `no_screen_grades_pass` | Float64 | PFF passing grade (0-100) excluding screen passes. |
| `pa_avg_time_to_throw` | Float64 | Average time to throw per dropback, in seconds from snap to release, on play-action dropbacks. |
| `screen_twp_rate` | Float64 | Turnover-worthy plays as a percentage of qualifying attempts on screen passes, per PFF charting. |
| `player_game_count` | Float64 | Number of games the player appeared in during the period covered. |
| `npa_accuracy_percent` | Float64 | Percentage of aimed passes charted as accurate by PFF on non-play-action dropbacks. |
| `eligible_season` | Float64 | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `npa_drops` | Float64 | Number of catchable passes dropped by receivers on non-play-action dropbacks. |
| `screen_yards` | Float64 | Passing yards gained on screen passes. |
| `no_screen_drop_rate` | Float64 | Percentage of catchable passes dropped by receivers excluding screen passes. |
| `no_screen_grades_offense_penalty` | Float64 | PFF offensive penalty grade for the player (0-100) excluding screen passes. |
| `npa_passing_snaps` | Float64 | Number of passing snaps played on non-play-action dropbacks. |
| `screen_drop_rate` | Float64 | Percentage of catchable passes dropped by receivers on screen passes. |
| `screen_epa` | Float64 | Total expected points added (EPA) on the player's dropbacks on screen passes. |
| `screen_grades_pass_route` | Float64 | PFF receiving (route) grade for the player (0-100) on screen passes. |
| `screen_grades_offense_penalty` | Float64 | PFF offensive penalty grade for the player (0-100) on screen passes. |
| `npa_spikes` | Float64 | Number of clock-stopping spikes on non-play-action dropbacks. |
| `screen_hit_as_threw` | Float64 | Number of attempts on which the passer was hit as he threw on screen passes, as charted by PFF. |
| `no_screen_aimed_passes` | Float64 | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) excluding screen passes, as charted by PFF. |
| `screen_drops` | Float64 | Number of catchable passes dropped by receivers on screen passes. |
| `screen_ypa` | Float64 | Yards gained per pass attempt on screen passes. |
| `npa_twp_rate` | Float64 | Turnover-worthy plays as a percentage of qualifying attempts on non-play-action dropbacks, per PFF charting. |
| `screen_accuracy_percent` | Float64 | Percentage of aimed passes charted as accurate by PFF on screen passes. |
| `npa_hit_as_threw` | Float64 | Number of attempts on which the passer was hit as he threw on non-play-action dropbacks, as charted by PFF. |
| `pa_accuracy_percent` | Float64 | Percentage of aimed passes charted as accurate by PFF on play-action dropbacks. |
| `pa_def_gen_pressures` | Float64 | Number of defense-generated pressures on the player's dropbacks on play-action dropbacks, as charted by PFF. |
| `screen_avg_depth_of_target` | Float64 | Average depth of target in air yards on screen passes. |
| `pa_sacks` | Float64 | Number of sacks taken on play-action dropbacks. |
| `screen_passing_snaps` | Float64 | Number of passing snaps played on screen passes. |
| `no_screen_grades_run` | Float64 | PFF rushing grade for the player (0-100) excluding screen passes. |
| `no_screen_first_downs` | Float64 | Number of passing first downs gained excluding screen passes. |
| `pa_ypa` | Float64 | Yards gained per pass attempt on play-action dropbacks. |
| `npa_scrambles` | Float64 | Number of scrambles on non-play-action dropbacks. |
| `npa_pressure_to_sack_rate` | Float64 | Percentage of pressured dropbacks that ended in a sack on non-play-action dropbacks. |
| `pa_twp_rate` | Float64 | Turnover-worthy plays as a percentage of qualifying attempts on play-action dropbacks, per PFF charting. |
| `no_screen_twp_rate` | Float64 | Turnover-worthy plays as a percentage of qualifying attempts excluding screen passes, per PFF charting. |
| `npa_grades_hands_fumble` | Float64 | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on non-play-action dropbacks. |
| `screen_completions` | Float64 | Number of completed passes on screen passes. |
| `screen_btt_rate` | Float64 | Big-time throws as a percentage of qualifying attempts on screen passes, per PFF charting. |
| `npa_grades_run` | Float64 | PFF rushing grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_interceptions` | Float64 | Number of passes intercepted excluding screen passes. |
| `npa_grades_run_block` | Float64 | PFF run-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_sacks` | Float64 | Number of sacks taken excluding screen passes. |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `no_screen_big_time_throws` | Float64 | Number of big-time throws excluding screen passes, per PFF's highest-value, highest-difficulty throw designation. |
| `npa_drop_rate` | Float64 | Percentage of catchable passes dropped by receivers on non-play-action dropbacks. |
| `npa_attempts` | Float64 | Number of pass attempts on non-play-action dropbacks. |
| `screen_qb_rating` | Float64 | Traditional NFL passer rating on screen passes. |
| `npa_grades_offense_penalty` | Float64 | PFF offensive penalty grade for the player (0-100) on non-play-action dropbacks. |
| `pa_bats` | Float64 | Number of pass attempts batted down at the line of scrimmage on play-action dropbacks. |
| `pa_attempts` | Float64 | Number of pass attempts on play-action dropbacks. |
| `npa_def_gen_pressures` | Float64 | Number of defense-generated pressures on the player's dropbacks on non-play-action dropbacks, as charted by PFF. |
| `no_screen_avg_time_to_throw` | Float64 | Average time to throw per dropback, in seconds from snap to release, excluding screen passes. |
| `pa_yards` | Float64 | Passing yards gained on play-action dropbacks. |
| `npa_positive_epa_percent` | Float64 | Percentage of dropbacks with positive expected points added on non-play-action dropbacks. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `no_screen_scrambles` | Float64 | Number of scrambles excluding screen passes. |
| `pa_turnover_worthy_plays` | Float64 | Number of turnover-worthy plays on play-action dropbacks, plays PFF charts as deserving of a turnover. |
| `pa_aimed_passes` | Float64 | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on play-action dropbacks, as charted by PFF. |
| `declined_penalties` | Float64 | Number of declined penalties committed by the player. |
| `pa_grades_offense` | Float64 | PFF overall offense grade for the player (0-100) on play-action dropbacks. |
| `screen_positive_epa_percent` | Float64 | Percentage of dropbacks with positive expected points added on screen passes. |
| `no_screen_dropbacks` | Float64 | Number of dropbacks excluding screen passes. |
| `no_screen_dropbacks_percent` | Float64 | Share of the player's total dropbacks that came excluding screen passes, expressed as a percentage. |
| `npa_grades_pass_route` | Float64 | PFF receiving (route) grade for the player (0-100) on non-play-action dropbacks. |
| `npa_sack_percent` | Float64 | Percentage of dropbacks that ended in a sack on non-play-action dropbacks. |
| `pa_positive_epa_percent` | Float64 | Percentage of dropbacks with positive expected points added on play-action dropbacks. |
| `pa_grades_run_block` | Float64 | PFF run-blocking grade for the player (0-100) on play-action dropbacks. |
| `no_screen_turnover_worthy_plays` | Float64 | Number of turnover-worthy plays excluding screen passes, plays PFF charts as deserving of a turnover. |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `pa_grades_pass_route` | Float64 | PFF receiving (route) grade for the player (0-100) on play-action dropbacks. |
| `npa_epa` | Float64 | Total expected points added (EPA) on the player's dropbacks on non-play-action dropbacks. |
| `no_screen_avg_depth_of_target` | Float64 | Average depth of target in air yards excluding screen passes. |
| `pa_grades_hands_fumble` | Float64 | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on play-action dropbacks. |
| `no_screen_completion_percent` | Float64 | Percentage of pass attempts completed excluding screen passes. |
| `pa_hit_as_threw` | Float64 | Number of attempts on which the passer was hit as he threw on play-action dropbacks, as charted by PFF. |
| `pa_grades_offense_penalty` | Float64 | PFF offensive penalty grade for the player (0-100) on play-action dropbacks. |
| `npa_dropbacks_percent` | Float64 | Share of the player's total dropbacks that came on non-play-action dropbacks, expressed as a percentage. |
| `npa_grades_pass` | Float64 | PFF passing grade (0-100) on non-play-action dropbacks. |
| `screen_grades_run` | Float64 | PFF rushing grade for the player (0-100) on screen passes. |
| `screen_first_downs` | Float64 | Number of passing first downs gained on screen passes. |
| `npa_completion_percent` | Float64 | Percentage of pass attempts completed on non-play-action dropbacks. |
| `no_screen_grades_run_block` | Float64 | PFF run-blocking grade for the player (0-100) excluding screen passes. |
| `no_screen_hit_as_threw` | Float64 | Number of attempts on which the passer was hit as he threw excluding screen passes, as charted by PFF. |
| `screen_turnover_worthy_plays` | Float64 | Number of turnover-worthy plays on screen passes, plays PFF charts as deserving of a turnover. |
| `npa_avg_depth_of_target` | Float64 | Average depth of target in air yards on non-play-action dropbacks. |
| `npa_dropbacks` | Float64 | Number of dropbacks on non-play-action dropbacks. |
| `player` | String | Player's display name as PFF lists it. |
| `pa_drops` | Float64 | Number of catchable passes dropped by receivers on play-action dropbacks. |
| `pa_btt_rate` | Float64 | Big-time throws as a percentage of qualifying attempts on play-action dropbacks, per PFF charting. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `screen_big_time_throws` | Float64 | Number of big-time throws on screen passes, per PFF's highest-value, highest-difficulty throw designation. |
| `screen_aimed_passes` | Float64 | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on screen passes, as charted by PFF. |
| `npa_touchdowns` | Float64 | Number of passing touchdowns thrown on non-play-action dropbacks. |
| `screen_attempts` | Float64 | Number of pass attempts on screen passes. |
| `screen_dropbacks` | Float64 | Number of dropbacks on screen passes. |
| `no_screen_yards` | Float64 | Passing yards gained excluding screen passes. |
| `pa_scrambles` | Float64 | Number of scrambles on play-action dropbacks. |
| `pa_completion_percent` | Float64 | Percentage of pass attempts completed on play-action dropbacks. |
| `pa_avg_depth_of_target` | Float64 | Average depth of target in air yards on play-action dropbacks. |
| `pa_interceptions` | Float64 | Number of passes intercepted on play-action dropbacks. |
| `no_screen_accuracy_percent` | Float64 | Percentage of aimed passes charted as accurate by PFF excluding screen passes. |
| `no_screen_spikes` | Float64 | Number of clock-stopping spikes excluding screen passes. |
| `pa_dropbacks` | Float64 | Number of dropbacks on play-action dropbacks. |
| `npa_big_time_throws` | Float64 | Number of big-time throws on non-play-action dropbacks, per PFF's highest-value, highest-difficulty throw designation. |
| `no_screen_sack_percent` | Float64 | Percentage of dropbacks that ended in a sack excluding screen passes. |
| `pa_epa` | Float64 | Total expected points added (EPA) on the player's dropbacks on play-action dropbacks. |
| `no_screen_attempts` | Float64 | Number of pass attempts excluding screen passes. |
| `pa_passing_snaps` | Float64 | Number of passing snaps played on play-action dropbacks. |
| `npa_completions` | Float64 | Number of completed passes on non-play-action dropbacks. |
| `screen_touchdowns` | Float64 | Number of passing touchdowns thrown on screen passes. |
| `npa_first_downs` | Float64 | Number of passing first downs gained on non-play-action dropbacks. |
| `no_screen_touchdowns` | Float64 | Number of passing touchdowns thrown excluding screen passes. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `npa_yards` | Float64 | Passing yards gained on non-play-action dropbacks. |
| `no_screen_ypa` | Float64 | Yards gained per pass attempt excluding screen passes. |
| `npa_turnover_worthy_plays` | Float64 | Number of turnover-worthy plays on non-play-action dropbacks, plays PFF charts as deserving of a turnover. |
| `npa_interceptions` | Float64 | Number of passes intercepted on non-play-action dropbacks. |
| `no_screen_pressure_to_sack_rate` | Float64 | Percentage of pressured dropbacks that ended in a sack excluding screen passes. |
| `screen_sacks` | Float64 | Number of sacks taken on screen passes. |
| `screen_grades_hands_drop` | Float64 | PFF hands (drop) grade for the player (0-100) on screen passes. |
| `screen_grades_pass_block` | Float64 | PFF pass-blocking grade for the player (0-100) on screen passes. |
| `no_screen_grades_pass_block` | Float64 | PFF pass-blocking grade for the player (0-100) excluding screen passes. |
| `pa_grades_hands_drop` | Float64 | PFF hands (drop) grade for the player (0-100) on play-action dropbacks. |
| `npa_grades_pass_block` | Float64 | PFF pass-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `pa_grades_pass_block` | Float64 | PFF pass-blocking grade for the player (0-100) on play-action dropbacks. |
| `npa_grades_hands_drop` | Float64 | PFF hands (drop) grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_grades_hands_drop` | Float64 | PFF hands (drop) grade for the player (0-100) excluding screen passes. |
| `pa_grades_screen_block` | Float64 | PFF screen-blocking grade for the player (0-100) on play-action dropbacks. |
| `screen_grades_screen_block` | Float64 | PFF screen-blocking grade for the player (0-100) on screen passes. |
| `no_screen_grades_screen_block` | Float64 | PFF screen-blocking grade for the player (0-100) excluding screen passes. |
| `npa_grades_screen_block` | Float64 | PFF screen-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `pa_grades_defense_penalty` | Float64 | PFF defensive penalty grade for the player (0-100) on play-action dropbacks. |
| `npa_grades_defense_penalty` | Float64 | PFF defensive penalty grade for the player (0-100) on non-play-action dropbacks. |
| `npa_grades_coverage_defense` | Float64 | PFF coverage grade for the player (0-100) on non-play-action dropbacks. |
| `pa_grades_defense` | Float64 | PFF overall defense grade for the player (0-100) on play-action dropbacks. |
| `screen_grades_coverage_defense` | Float64 | PFF coverage grade for the player (0-100) on screen passes. |
| `npa_grades_defense` | Float64 | PFF overall defense grade for the player (0-100) on non-play-action dropbacks. |
| `screen_grades_defense` | Float64 | PFF overall defense grade for the player (0-100) on screen passes. |
| `screen_grades_defense_penalty` | Float64 | PFF defensive penalty grade for the player (0-100) on screen passes. |
| `no_screen_grades_defense` | Float64 | PFF overall defense grade for the player (0-100) excluding screen passes. |
| `pa_grades_coverage_defense` | Float64 | PFF coverage grade for the player (0-100) on play-action dropbacks. |
| `no_screen_grades_defense_penalty` | Float64 | PFF defensive penalty grade for the player (0-100) excluding screen passes. |
| `no_screen_grades_coverage_defense` | Float64 | PFF coverage grade for the player (0-100) excluding screen passes. |
| `no_screen_grades_overall_tackle` | Float64 | PFF overall tackling grade for the player (0-100) excluding screen passes. |
| `pa_grades_overall_tackle` | Float64 | PFF overall tackling grade for the player (0-100) on play-action dropbacks. |
| `pa_grades_tackle` | Float64 | PFF tackling grade for the player (0-100) on play-action dropbacks. |
| `pa_grades_pass_rush_defense` | String | PFF pass-rush grade for the player (0-100) on play-action dropbacks. |
| `screen_grades_run_defense` | String | PFF run-defense grade for the player (0-100) on screen passes. |
| `pa_grades_run_defense` | String | PFF run-defense grade for the player (0-100) on play-action dropbacks. |
| `npa_grades_overall_tackle` | Float64 | PFF overall tackling grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_grades_pass_rush_defense` | Float64 | PFF pass-rush grade for the player (0-100) excluding screen passes. |
| `npa_grades_tackle` | Float64 | PFF tackling grade for the player (0-100) on non-play-action dropbacks. |
| `screen_grades_tackle` | String | PFF tackling grade for the player (0-100) on screen passes. |
| `no_screen_grades_tackle` | Float64 | PFF tackling grade for the player (0-100) excluding screen passes. |
| `npa_grades_pass_rush_defense` | Float64 | PFF pass-rush grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_grades_run_defense` | Float64 | PFF run-defense grade for the player (0-100) excluding screen passes. |
| `screen_grades_overall_tackle` | String | PFF overall tackling grade for the player (0-100) on screen passes. |
| `npa_grades_run_defense` | Float64 | PFF run-defense grade for the player (0-100) on non-play-action dropbacks. |
| `screen_grades_pass_rush_defense` | Float64 | PFF pass-rush grade for the player (0-100) on screen passes. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_facet_passing_concept-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_passing_concept-example}

```python
pff_api_facet_passing_concept(league='nfl', season='2022')
```
