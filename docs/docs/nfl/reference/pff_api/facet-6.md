---
title: "NFL — PFF Developer API (api.pff.com, API key) — Facet: receiving–defense"
sidebar_label: "Facet: receiving–defense"
sidebar_position: 6
description: "NFL — PFF Developer API (api.pff.com, API key) — Facet: receiving–defense — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — PFF Developer API (api.pff.com, API key) — Facet: receiving–defense

## pff_api_facet_receiving_scheme

League-wide receiving-by-scheme leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/receiving/scheme`

**Valid URL:** [https://api.pff.com/v1/facet/receiving/scheme?league=nfl&season=2022](https://api.pff.com/v1/facet/receiving/scheme?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_receiving_scheme-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `man_contested_catch_rate` | Float64 | Percentage of PFF-charted contested targets caught against man coverage. |
| `man_touchdowns` | Float64 | Receiving touchdowns scored against man coverage. |
| `zone_targets_percent` | Float64 | Share of the team's targets thrown to the player against zone coverage. |
| `man_interceptions` | Float64 | Interceptions thrown on passes targeting the player against man coverage. |
| `zone_epa` | Float64 | Total expected points added on targets to the player against zone coverage. |
| `man_avg_depth_of_target` | Float64 | Average depth of target in yards downfield against man coverage. |
| `zone_pass_blocks` | Float64 | Pass-play snaps spent pass blocking against zone coverage. |
| `zone_avoided_tackles` | Float64 | Tackles avoided after the catch against zone coverage. |
| `man_targeted_qb_rating` | Float64 | NFL passer rating on throws targeting the player against man coverage. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `zone_positive_epa_percent` | Float64 | Percentage of the receiver's plays with an EPA value (in practice their routes run) producing positive expected points added against zone coverage. |
| `man_targets_percent` | Float64 | Share of the team's targets thrown to the player against man coverage. |
| `man_yards_per_reception` | Float64 | Average yards per reception against man coverage. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `man_drop_rate` | Float64 | Share of catchable targets the player dropped against man coverage. |
| `zone_grades_pass_route` | Float64 | PFF route-running (receiving) grade against zone coverage, 0-100. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `zone_fumbles` | Float64 | Fumbles by the player after the catch against zone coverage. |
| `man_pass_block_rate` | Float64 | Share of pass-play snaps spent pass blocking against man coverage. |
| `zone_yards` | Float64 | Receiving yards gained against zone coverage. |
| `man_yprr` | Float64 | Yards per route run against man coverage. |
| `player_game_count` | Float64 | Number of games the player appeared in over the covered span. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `zone_drops` | Float64 | PFF-charted drops against zone coverage. |
| `zone_receptions` | Float64 | Receptions made against zone coverage. |
| `man_pass_plays` | Float64 | Pass-play snaps against man coverage. |
| `man_epa` | Float64 | Total expected points added on targets to the player against man coverage. |
| `zone_yards_per_reception` | Float64 | Average yards per reception against zone coverage. |
| `man_contested_targets` | Float64 | PFF-charted contested targets against man coverage. |
| `man_longest` | Float64 | Longest reception in yards against man coverage. |
| `zone_yards_after_catch` | Float64 | Yards gained after the catch against zone coverage. |
| `man_receptions` | Float64 | Receptions made against man coverage. |
| `man_avoided_tackles` | Float64 | Tackles avoided after the catch against man coverage. |
| `man_first_downs` | Float64 | Receptions that converted a first down against man coverage. |
| `base_targets` | Float64 | Total targets from the facet's unsplit base row, across all coverage schemes. |
| `zone_contested_catch_rate` | Float64 | Percentage of PFF-charted contested targets caught against zone coverage. |
| `zone_targeted_qb_rating` | Float64 | NFL passer rating on throws targeting the player against zone coverage. |
| `man_routes` | Float64 | Pass routes run by the player against man coverage. |
| `zone_grades_hands_drop` | Float64 | PFF hands/drop grade against zone coverage, 0-100. |
| `man_route_rate` | Float64 | Share of pass-play snaps on which the player ran a route against man coverage. |
| `zone_pass_block_rate` | Float64 | Share of pass-play snaps spent pass blocking against zone coverage. |
| `man_grades_pass_route` | Float64 | PFF route-running (receiving) grade against man coverage, 0-100. |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `zone_first_downs` | Float64 | Receptions that converted a first down against zone coverage. |
| `zone_yprr` | Float64 | Yards per route run against zone coverage. |
| `man_drops` | Float64 | PFF-charted drops against man coverage. |
| `zone_caught_percent` | Float64 | Percentage of targets caught against zone coverage. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `man_fumbles` | Float64 | Fumbles by the player after the catch against man coverage. |
| `declined_penalties` | Float64 | Penalties committed by the player that were declined. |
| `man_yards_after_catch` | Float64 | Yards gained after the catch against man coverage. |
| `man_yards` | Float64 | Receiving yards gained against man coverage. |
| `zone_pass_plays` | Float64 | Pass-play snaps against zone coverage. |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `man_targets` | Float64 | Pass targets to the player against man coverage. |
| `man_grades_hands_drop` | Float64 | PFF hands/drop grade against man coverage, 0-100. |
| `man_pass_blocks` | Float64 | Pass-play snaps spent pass blocking against man coverage. |
| `zone_touchdowns` | Float64 | Receiving touchdowns scored against zone coverage. |
| `zone_route_rate` | Float64 | Share of pass-play snaps on which the player ran a route against zone coverage. |
| `zone_yards_after_catch_per_reception` | Float64 | Average yards after the catch per reception against zone coverage. |
| `zone_avg_depth_of_target` | Float64 | Average depth of target in yards downfield against zone coverage. |
| `player` | String | Player's display name as PFF lists it. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `zone_contested_targets` | Float64 | PFF-charted contested targets against zone coverage. |
| `zone_contested_receptions` | Float64 | Catches made on PFF-charted contested targets against zone coverage. |
| `man_contested_receptions` | Float64 | Catches made on PFF-charted contested targets against man coverage. |
| `man_yards_after_catch_per_reception` | Float64 | Average yards after the catch per reception against man coverage. |
| `zone_targets` | Float64 | Pass targets to the player against zone coverage. |
| `zone_longest` | Float64 | Longest reception in yards against zone coverage. |
| `man_caught_percent` | Float64 | Percentage of targets caught against man coverage. |
| `zone_drop_rate` | Float64 | Share of catchable targets the player dropped against zone coverage. |
| `zone_interceptions` | Float64 | Interceptions thrown on passes targeting the player against zone coverage. |
| `man_positive_epa_percent` | Float64 | Percentage of the receiver's plays with an EPA value (in practice their routes run) producing positive expected points added against man coverage. |
| `zone_routes` | Float64 | Pass routes run by the player against zone coverage. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_facet_receiving_scheme-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_receiving_scheme-example}

```python
pff_api_facet_receiving_scheme(league='nfl', season='2022')
```

## pff_api_facet_receiving_summary

League-wide receiving summary leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/receiving/summary`

**Valid URL:** [https://api.pff.com/v1/facet/receiving/summary?league=nfl&season=2022](https://api.pff.com/v1/facet/receiving/summary?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_receiving_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `targets` | Float64 | Times targeted. |
| `grades_pass_block` | Float64 | PFF pass-blocking grade, 0-100. |
| `grades_offense` | Float64 | PFF overall offense grade (0-100). |
| `yards_after_catch_per_reception` | Float64 | Average yards after the catch per reception. |
| `grades_pass_route` | Float64 | PFF receiving/route grade (0-100). |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `yprr` | Float64 | Yards per route run. |
| `wide_snaps` | Float64 | Receiving snaps aligned out wide. |
| `fumbles` | Float64 | Fumbles by the player after the catch. |
| `first_downs` | Float64 | Receptions that converted a first down. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `inline_snaps` | Float64 | Receiving snaps aligned inline, tight to the formation. |
| `contested_targets` | Float64 | Contested targets. |
| `player_game_count` | Float64 | Games with at least one qualifying snap in the requested range. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `inline_rate` | Float64 | Share of receiving snaps aligned inline, tight to the formation. |
| `contested_catch_rate` | Float64 | Percentage of PFF-charted contested targets caught. |
| `yards` | Float64 | Receiving yards. |
| `receptions` | Float64 | Passes caught by the receiver. |
| `targeted_qb_rating` | Float64 | NFL passer rating on throws targeting the player. |
| `interceptions` | Float64 | Interceptions on passes targeting the player. |
| `caught_percent` | Float64 | Percentage of targets caught. |
| `drop_rate` | Float64 | Share of catchable targets the player dropped. |
| `grades_hands_drop` | Float64 | PFF hands/drop grade (0-100). |
| `slot_rate` | Float64 | Share of receiving snaps aligned in the slot. |
| `slot_snaps` | Float64 | Receiving snaps aligned in the slot. |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `wide_rate` | Float64 | Share of receiving snaps aligned out wide. |
| `pass_block_rate` | Float64 | Share of pass-play snaps spent pass blocking. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | Float64 | Penalties committed by the player that were declined. |
| `route_rate` | Float64 | Share of pass-play snaps on which the player ran a route. |
| `drops` | Float64 | Dropped passes. |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `grades_hands_fumble` | Float64 | PFF ball-security (hands/fumble) grade, 0-100. |
| `longest` | Float64 | Longest reception in yards. |
| `pass_blocks` | Float64 | Pass-play snaps spent pass blocking. |
| `routes` | Float64 | Pass routes run by the receiver. |
| `pass_plays` | Float64 | Pass-play snaps. |
| `yards_per_reception` | Float64 | Average yards per reception. |
| `player` | String | Player's display name as PFF lists it. |
| `positive_epa_percent` | Float64 | Percentage of the receiver's plays with an EPA value (in practice their routes run) producing positive expected points added. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `contested_receptions` | Float64 | Contested catches made. |
| `yards_after_catch` | Float64 | Yards after the catch. |
| `avg_depth_of_target` | Float64 | Average depth of target in yards downfield. |
| `epa` | Float64 | Expected points added per target to the player, as computed by PFF (an average such as -0.27, not a total). |
| `avoided_tackles` | Float64 | Tackles avoided after the catch. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `touchdowns` | Float64 | Receiving touchdowns. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_facet_receiving_summary-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_receiving_summary-example}

```python
pff_api_facet_receiving_summary(league='nfl', season='2022')
```

## pff_api_facet_rushing_direction

League-wide rushing-by-direction leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/rushing/direction`

**Valid URL:** [https://api.pff.com/v1/facet/rushing/direction?league=nfl&season=2022](https://api.pff.com/v1/facet/rushing/direction?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_rushing_direction-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `directions` | List | Nested per-direction rushing splits (attempts and results by run direction) as returned by the PFF API. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `player` | String | Player's display name as PFF lists it. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `total_attempts` | Float64 | Total rushing attempts across all run directions. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_facet_rushing_direction-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_rushing_direction-example}

```python
pff_api_facet_rushing_direction(league='nfl', season='2022')
```

## pff_api_facet_rushing_summary

League-wide rushing summary leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/rushing/summary`

**Valid URL:** [https://api.pff.com/v1/facet/rushing/summary?league=nfl&season=2022](https://api.pff.com/v1/facet/rushing/summary?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_rushing_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `targets` | Float64 | Passes thrown to the ball carrier (targets). |
| `grades_pass_block` | Float64 | PFF pass-blocking grade, 0-100. |
| `grades_offense` | Float64 | PFF overall offense grade (0-100). |
| `yards_after_contact` | Float64 | Yards after contact. |
| `explosive` | Float64 | Runs PFF designates as explosive. |
| `grades_pass_route` | Float64 | PFF route-running (receiving) grade, 0-100. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `elu_rush_mtf` | Float64 | Missed tackles forced as a rusher, an input to PFF's elusive rating. |
| `breakaway_attempts` | Float64 | Runs of 15 or more yards, PFF's breakaway designation. |
| `designed_yards` | Float64 | Rushing yards gained on designed runs, excluding scrambles. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `yprr` | Float64 | Yards per route run. |
| `breakaway_percent` | Float64 | Share of rushing yards gained on breakaway runs of 15 or more yards. |
| `fumbles` | Float64 | Fumbles by the ball carrier. |
| `first_downs` | Float64 | Rushing first downs. |
| `elusive_rating` | Float64 | PFF elusive rating. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `breakaway_yards` | Float64 | Breakaway (long-run) yards. |
| `player_game_count` | Float64 | Games with at least one qualifying snap in the requested range. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `total_touches` | Float64 | Combined carries and receptions. |
| `scramble_yards` | Float64 | Rushing yards gained on scrambles. |
| `yco_attempt` | Float64 | Average yards after contact per rushing attempt. |
| `yards` | Float64 | Total rushing yards gained. |
| `grades_run_block` | Float64 | PFF run-blocking grade, 0-100. |
| `receptions` | Float64 | Passes caught by the ball carrier. |
| `zone_attempts` | Float64 | Rushing attempts on zone-scheme runs. |
| `scrambles` | Float64 | Quarterback scrambles. |
| `grades_run` | Float64 | PFF rushing grade (0-100). |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `attempts` | Float64 | Rushing attempts (carries) by the runner. |
| `elu_yco` | Float64 | Yards-after-contact component used in PFF's elusive rating. |
| `elu_recv_mtf` | Float64 | Missed tackles forced as a receiver, an input to PFF's elusive rating. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | Float64 | Penalties committed by the player that were declined. |
| `ypa` | Float64 | Average yards per rushing attempt. |
| `drops` | Float64 | Passes dropped by the ball carrier. |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `grades_hands_fumble` | Float64 | PFF ball-security (hands/fumble) grade, 0-100. |
| `longest` | Float64 | Longest run in yards. |
| `routes` | Float64 | Pass routes run by the player. |
| `player` | String | Player's display name as PFF lists it. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `rec_yards` | Float64 | Receiving yards gained by the ball carrier (the rushing report also carries his receiving line). |
| `gap_attempts` | Float64 | Rushing attempts on gap-scheme runs. |
| `run_plays` | Float64 | Run-play snaps. |
| `avoided_tackles` | Float64 | Missed tackles forced. |
| `grades_offense_penalty` | Float64 | PFF offensive penalty grade, 0-100. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `touchdowns` | Float64 | Rushing touchdowns. |
| `grades_pass` | Float64 | PFF passing grade, 0-100. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_facet_rushing_summary-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_rushing_summary-example}

```python
pff_api_facet_rushing_summary(league='nfl', season='2022')
```

## pff_api_facet_defense_coverage

League-wide coverage leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/defense/coverage`

**Valid URL:** [https://api.pff.com/v1/facet/defense/coverage?league=nfl&season=2022](https://api.pff.com/v1/facet/defense/coverage?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_defense_coverage-returns}

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

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_facet_defense_coverage-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_defense_coverage-example}

```python
pff_api_facet_defense_coverage(league='nfl', season='2022')
```

## pff_api_facet_defense_coverage_scheme

League-wide coverage-by-scheme leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/defense/coverage_scheme`

**Valid URL:** [https://api.pff.com/v1/facet/defense/coverage_scheme?league=nfl&season=2022](https://api.pff.com/v1/facet/defense/coverage_scheme?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_defense_coverage_scheme-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `man_touchdowns` | Float64 | Touchdowns allowed into the player's coverage when in man coverage. |
| `man_interceptions` | Float64 | Interceptions made in coverage when in man coverage. |
| `zone_snap_counts_coverage_percent` | Float64 | Share of the player's coverage snaps played when in zone coverage. |
| `man_avg_depth_of_target` | Float64 | Average depth of targets into the player's coverage, in yards downfield when in man coverage. |
| `man_dropped_ints` | Float64 | Interception chances PFF charted as dropped when in man coverage. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `zone_coverage_snaps_per_target` | Float64 | Coverage snaps played per target into the player's coverage when in zone coverage. |
| `man_yards_per_reception` | Float64 | Average yards allowed per reception when in man coverage. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `man_snap_counts_coverage` | Float64 | Coverage snaps played when in man coverage. |
| `man_tackles` | Float64 | Tackles made when in man coverage. |
| `zone_stops` | Float64 | Stops, PFF's tackles that constitute a failed play for the offense when in zone coverage. |
| `zone_snap_counts_coverage` | Float64 | Coverage snaps played when in zone coverage. |
| `zone_yards` | Float64 | Receiving yards allowed when in zone coverage. |
| `man_coverage_snaps_per_target` | Float64 | Coverage snaps played per target into the player's coverage when in man coverage. |
| `zone_coverage_percent` | Float64 | Share of pass-play snaps spent in coverage when in zone coverage. |
| `player_game_count` | Float64 | Number of games the player appeared in over the covered span. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `zone_receptions` | Float64 | Receptions allowed into the player's coverage when in zone coverage. |
| `zone_forced_incompletion_rate` | Float64 | Share of targets into the player's coverage with a PFF-charted forced incompletion when in zone coverage. |
| `man_snap_counts_pass_play` | Float64 | Pass-play snaps when in man coverage. |
| `zone_yards_per_reception` | Float64 | Average yards allowed per reception when in zone coverage. |
| `man_longest` | Float64 | Longest completion allowed, in yards when in man coverage. |
| `man_assists` | Float64 | Assisted tackles when in man coverage. |
| `zone_yards_after_catch` | Float64 | Yards after the catch allowed when in zone coverage. |
| `man_stops` | Float64 | Stops, PFF's tackles that constitute a failed play for the offense when in man coverage. |
| `man_receptions` | Float64 | Receptions allowed into the player's coverage when in man coverage. |
| `zone_tackles` | Float64 | Tackles made when in zone coverage. |
| `man_coverage_percent` | Float64 | Share of pass-play snaps spent in coverage when in man coverage. |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `zone_yards_per_coverage_snap` | Float64 | Receiving yards allowed per coverage snap when in zone coverage. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `man_catch_rate` | Float64 | Completion percentage allowed on targets into the player's coverage when in man coverage. |
| `man_grades_coverage_defense` | Float64 | PFF coverage grade when in man coverage, 0-100. |
| `declined_penalties` | Float64 | Penalties committed by the player that were declined. |
| `man_yards_after_catch` | Float64 | Yards after the catch allowed when in man coverage. |
| `man_pass_break_ups` | Float64 | Passes broken up when in man coverage. |
| `man_yards` | Float64 | Receiving yards allowed when in man coverage. |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `man_targets` | Float64 | Targets into the player's coverage when in man coverage. |
| `man_missed_tackle_rate` | Float64 | Share of tackle attempts the player missed when in man coverage. |
| `zone_assists` | Float64 | Assisted tackles when in zone coverage. |
| `zone_snap_counts_pass_play` | Float64 | Pass-play snaps when in zone coverage. |
| `zone_missed_tackle_rate` | Float64 | Share of tackle attempts the player missed when in zone coverage. |
| `zone_touchdowns` | Float64 | Touchdowns allowed into the player's coverage when in zone coverage. |
| `zone_coverage_snaps_per_reception` | Float64 | Coverage snaps played per reception allowed when in zone coverage. |
| `man_coverage_snaps_per_reception` | Float64 | Coverage snaps played per reception allowed when in man coverage. |
| `zone_avg_depth_of_target` | Float64 | Average depth of targets into the player's coverage, in yards downfield when in zone coverage. |
| `man_snap_counts_coverage_percent` | Float64 | Share of the player's coverage snaps played when in man coverage. |
| `player` | String | Player's display name as PFF lists it. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `zone_forced_incompletes` | Float64 | Incompletions forced by the player's coverage, per PFF charting when in zone coverage. |
| `man_forced_incompletion_rate` | Float64 | Share of targets into the player's coverage with a PFF-charted forced incompletion when in man coverage. |
| `zone_targets` | Float64 | Targets into the player's coverage when in zone coverage. |
| `zone_longest` | Float64 | Longest completion allowed, in yards when in zone coverage. |
| `zone_dropped_ints` | Float64 | Interception chances PFF charted as dropped when in zone coverage. |
| `zone_interceptions` | Float64 | Interceptions made in coverage when in zone coverage. |
| `man_forced_incompletes` | Float64 | Incompletions forced by the player's coverage, per PFF charting when in man coverage. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `zone_missed_tackles` | Float64 | Missed tackles when in zone coverage. |
| `base_snap_counts_coverage` | Float64 | Coverage snaps from the facet's unsplit base row, covering all coverage schemes. |
| `man_missed_tackles` | Float64 | Missed tackles when in man coverage. |
| `zone_grades_coverage_defense` | Float64 | PFF coverage grade when in zone coverage, 0-100. |
| `man_qb_rating_against` | Float64 | NFL passer rating allowed on targets into the player's coverage when in man coverage. |
| `zone_pass_break_ups` | Float64 | Passes broken up when in zone coverage. |
| `zone_qb_rating_against` | Float64 | NFL passer rating allowed on targets into the player's coverage when in zone coverage. |
| `man_yards_per_coverage_snap` | Float64 | Receiving yards allowed per coverage snap when in man coverage. |
| `zone_catch_rate` | Float64 | Completion percentage allowed on targets into the player's coverage when in zone coverage. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_facet_defense_coverage_scheme-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_defense_coverage_scheme-example}

```python
pff_api_facet_defense_coverage_scheme(league='nfl', season='2022')
```

## pff_api_facet_defense_coverage_matchup

League-wide coverage matchup leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/defense/coverage_matchup`

**Valid URL:** [https://api.pff.com/v1/facet/defense/coverage_matchup?league=nfl&season=2022](https://api.pff.com/v1/facet/defense/coverage_matchup?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_defense_coverage_matchup-returns}

**`return_parsed=True`** (default) — a dict of `polars.DataFrame`s (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**defenders**

| col_name | type | description |
|---|---|---|
| `broken_up_passes` | Int64 | Passes broken up in the row's coverage matchups. |
| `drops` | Int64 | Dropped passes in the row's coverage matchups. |
| `first_downs` | Int64 | Receiving first downs (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `grades_coverage_defense` | Float64 | The player's PFF coverage grade (0-100). |
| `grades_defense` | Float64 | The player's PFF overall defense grade (0-100). |
| `grades_defense_penalty` | Float64 | The player's PFF defensive penalty grade (0-100). |
| `grades_overall` | Float64 | The player's PFF overall grade (0-100) over the requested filters. |
| `grades_overall_tackle` | Float64 | The player's PFF overall tackling grade (0-100). |
| `grades_pass_rush_defense` | Float64 | The player's PFF pass-rush grade (0-100). |
| `grades_run_defense` | Float64 | The player's PFF run-defense grade (0-100). |
| `grades_tackle` | Float64 | The player's PFF tackling grade (0-100). |
| `interceptions` | Int64 | Interceptions on throws in the row's coverage matchups. |
| `longest` | Int64 | Longest reception, in yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `player_game_count` | Int64 | Games the player appeared in over the requested filters. |
| `player_id` | Int64 | PFF player id: the covering defender (defenders) or the receiver (receivers, versus). |
| `receptions` | Int64 | Receptions (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `targets` | Int64 | Targets (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `touchdowns` | Int64 | Receiving touchdowns (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards` | Int64 | Receiving yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_after_catch` | Int64 | Yards after the catch (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_per_reception` | Float64 | Yards per reception (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `grades_offense` | Float64 | The player's PFF overall offense grade (0-100). |
| `grades_offense_penalty` | Float64 | The player's PFF offensive penalty grade (0-100). |
| `grades_run_block` | Float64 | The player's PFF run-blocking grade (0-100). |
| `grades_pass_block` | Float64 | The player's PFF pass-blocking grade (0-100). |
| `grades_hands_drop` | Float64 | The player's PFF hands (drop) grade (0-100). |
| `grades_hands_fumble` | Float64 | The player's PFF ball-security (fumble) grade (0-100). |
| `grades_pass_route` | Float64 | The player's PFF receiving (route-running) grade (0-100). |
| `grades_pass` | Float64 | The player's PFF passing grade (0-100). |
| `grades_run` | Float64 | The player's PFF rushing grade (0-100). |

**receivers**

| col_name | type | description |
|---|---|---|
| `broken_up_passes` | Int64 | Passes broken up in the row's coverage matchups. |
| `drops` | Int64 | Dropped passes in the row's coverage matchups. |
| `first_downs` | Int64 | Receiving first downs (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `grades_hands_drop` | Float64 | The player's PFF hands (drop) grade (0-100). |
| `grades_hands_fumble` | Float64 | The player's PFF ball-security (fumble) grade (0-100). |
| `grades_offense` | Float64 | The player's PFF overall offense grade (0-100). |
| `grades_offense_penalty` | Float64 | The player's PFF offensive penalty grade (0-100). |
| `grades_overall` | Float64 | The player's PFF overall grade (0-100) over the requested filters. |
| `grades_pass_route` | Float64 | The player's PFF receiving (route-running) grade (0-100). |
| `grades_run_block` | Float64 | The player's PFF run-blocking grade (0-100). |
| `grades_screen_block` | Float64 | The player's PFF screen-blocking grade (0-100). |
| `interceptions` | Int64 | Interceptions on throws in the row's coverage matchups. |
| `longest` | Int64 | Longest reception, in yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `player_game_count` | Int64 | Games the player appeared in over the requested filters. |
| `player_id` | Int64 | PFF player id: the covering defender (defenders) or the receiver (receivers, versus). |
| `receptions` | Int64 | Receptions (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `targets` | Int64 | Targets (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `touchdowns` | Int64 | Receiving touchdowns (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards` | Int64 | Receiving yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_after_catch` | Int64 | Yards after the catch (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_per_reception` | Float64 | Yards per reception (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `grades_pass` | Float64 | The player's PFF passing grade (0-100). |
| `grades_pass_block` | Float64 | The player's PFF pass-blocking grade (0-100). |
| `grades_run` | Float64 | The player's PFF rushing grade (0-100). |
| `grades_defense` | Float64 | The player's PFF overall defense grade (0-100). |
| `grades_defense_penalty` | Float64 | The player's PFF defensive penalty grade (0-100). |
| `grades_pass_rush_defense` | Float64 | The player's PFF pass-rush grade (0-100). |
| `grades_run_defense` | Float64 | The player's PFF run-defense grade (0-100). |
| `grades_coverage_defense` | Float64 | The player's PFF coverage grade (0-100). |
| `grades_overall_tackle` | Float64 | The player's PFF overall tackling grade (0-100). |
| `grades_tackle` | Float64 | The player's PFF tackling grade (0-100). |
| `grades_snap` | Float64 | PFF grade reported as grades_snap (0-100); PFF does not document it, and the capture shows it only in the ncaa receivers frame. |

**versus**

| col_name | type | description |
|---|---|---|
| `broken_up_passes` | Int64 | Passes broken up in the row's coverage matchups. |
| `coverage_player_id` | Int64 | PFF player id of the defender covering the receiver (versus only). |
| `drops` | Int64 | Dropped passes in the row's coverage matchups. |
| `first_downs` | Int64 | Receiving first downs (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `interceptions` | Int64 | Interceptions on throws in the row's coverage matchups. |
| `longest` | Int64 | Longest reception, in yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `player_id` | Int64 | PFF player id: the covering defender (defenders) or the receiver (receivers, versus). |
| `receptions` | Int64 | Receptions (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `targets` | Int64 | Targets (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `touchdowns` | Int64 | Receiving touchdowns (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards` | Int64 | Receiving yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_after_catch` | Int64 | Yards after the catch (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_per_reception` | Float64 | Yards per reception (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_facet_defense_coverage_matchup-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_defense_coverage_matchup-example}

```python
pff_api_facet_defense_coverage_matchup(league='nfl', season='2022')
```

## pff_api_facet_defense_pass_rush

League-wide pass-rush leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/defense/pass_rush`

**Valid URL:** [https://api.pff.com/v1/facet/defense/pass_rush?league=nfl&season=2022](https://api.pff.com/v1/facet/defense/pass_rush?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_defense_pass_rush-returns}

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

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_facet_defense_pass_rush-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_defense_pass_rush-example}

```python
pff_api_facet_defense_pass_rush(league='nfl', season='2022')
```

## pff_api_facet_defense_run

League-wide run-defense leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/defense/run`

**Valid URL:** [https://api.pff.com/v1/facet/defense/run?league=nfl&season=2022](https://api.pff.com/v1/facet/defense/run?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_defense_run-returns}

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

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_facet_defense_run-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_defense_run-example}

```python
pff_api_facet_defense_run(league='nfl', season='2022')
```

## pff_api_facet_defense_summary

League-wide defense summary leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/defense/summary`

**Valid URL:** [https://api.pff.com/v1/facet/defense/summary?league=nfl&season=2022](https://api.pff.com/v1/facet/defense/summary?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_defense_summary-returns}

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

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_facet_defense_summary-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_defense_summary-example}

```python
pff_api_facet_defense_summary(league='nfl', season='2022')
```
