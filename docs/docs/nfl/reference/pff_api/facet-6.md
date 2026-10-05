---
title: "NFL — PFF Developer API (api.pff.com, API key) — Facet: receiving–kickoff"
sidebar_label: "Facet: receiving–kickoff"
sidebar_position: 6
description: "NFL — PFF Developer API (api.pff.com, API key) — Facet: receiving–kickoff — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — PFF Developer API (api.pff.com, API key) — Facet: receiving–kickoff

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
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `man_drop_rate` | numeric | Share of catchable targets the player dropped against man coverage. |
| `zone_grades_pass_route` | numeric | PFF route-running (receiving) grade against zone coverage, 0-100. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
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
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `zone_first_downs` | numeric | Receptions that converted a first down against zone coverage. |
| `zone_yprr` | numeric | Yards per route run against zone coverage. |
| `man_drops` | numeric | PFF-charted drops against man coverage. |
| `zone_caught_percent` | numeric | Percentage of targets caught against zone coverage. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `man_fumbles` | numeric | Fumbles by the player after the catch against man coverage. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `man_yards_after_catch` | numeric | Yards gained after the catch against man coverage. |
| `man_yards` | numeric | Receiving yards gained against man coverage. |
| `zone_pass_plays` | numeric | Pass-play snaps against zone coverage. |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `man_targets` | numeric | Pass targets to the player against man coverage. |
| `man_grades_hands_drop` | numeric | PFF hands/drop grade against man coverage, 0-100. |
| `man_pass_blocks` | numeric | Pass-play snaps spent pass blocking against man coverage. |
| `zone_touchdowns` | numeric | Receiving touchdowns scored against zone coverage. |
| `zone_route_rate` | numeric | Share of pass-play snaps on which the player ran a route against zone coverage. |
| `zone_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception against zone coverage. |
| `zone_avg_depth_of_target` | numeric | Average depth of target in yards downfield against zone coverage. |
| `player` | character | Player's display name as PFF lists it. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
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
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_facet_receiving_scheme-example}

```python
pff_api_facet_receiving_scheme(league='nfl', season='2022')
```

_Last validated n/a._

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
| `first_downs` | numeric | Receptions that converted a first down. |
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
| `interceptions` | numeric | Interceptions on passes targeting the player. |
| `caught_percent` | numeric | Percentage of targets caught. |
| `drop_rate` | numeric | Share of catchable targets the player dropped. |
| `grades_hands_drop` | numeric | PFF hands/drop grade (0-100). |
| `slot_rate` | numeric | Share of receiving snaps aligned in the slot. |
| `slot_snaps` | numeric | Receiving snaps aligned in the slot. |
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `wide_rate` | numeric | Share of receiving snaps aligned out wide. |
| `pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `route_rate` | numeric | Share of pass-play snaps on which the player ran a route. |
| `drops` | numeric | Dropped passes. |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `grades_hands_fumble` | numeric | PFF ball-security (hands/fumble) grade, 0-100. |
| `longest` | numeric | Longest reception in yards. |
| `pass_blocks` | numeric | Pass-play snaps spent pass blocking. |
| `routes` | numeric | Pass routes run by the receiver. |
| `pass_plays` | numeric | Pass-play snaps. |
| `yards_per_reception` | numeric | Average yards per reception. |
| `player` | character | Player's display name as PFF lists it. |
| `positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `contested_receptions` | numeric | Contested catches made. |
| `yards_after_catch` | numeric | Yards after the catch. |
| `avg_depth_of_target` | numeric | Average depth of target in yards downfield. |
| `epa` | numeric | Expected points added per target to the player, as computed by PFF (an average such as -0.27, not a total). |
| `avoided_tackles` | numeric | Tackles avoided after the catch. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `touchdowns` | numeric | Receiving touchdowns. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_facet_receiving_summary-example}

```python
pff_api_facet_receiving_summary(league='nfl', season='2022')
```

_Last validated n/a._

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
| `directions` | list | Nested per-direction rushing splits (attempts and results by run direction) as returned by the PFF API. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `player` | character | Player's display name as PFF lists it. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `total_attempts` | numeric | Total rushing attempts across all run directions. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_facet_rushing_direction-example}

```python
pff_api_facet_rushing_direction(league='nfl', season='2022')
```

_Last validated n/a._

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
| `targets` | numeric | Passes thrown to the ball carrier (targets). |
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
| `receptions` | numeric | Passes caught by the ball carrier. |
| `zone_attempts` | numeric | Rushing attempts on zone-scheme runs. |
| `scrambles` | numeric | Quarterback scrambles. |
| `grades_run` | numeric | PFF rushing grade (0-100). |
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `attempts` | numeric | Rushing attempts (carries) by the runner. |
| `elu_yco` | numeric | Yards-after-contact component used in PFF's elusive rating. |
| `elu_recv_mtf` | numeric | Missed tackles forced as a receiver, an input to PFF's elusive rating. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `ypa` | numeric | Average yards per rushing attempt. |
| `drops` | numeric | Passes dropped by the ball carrier. |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `grades_hands_fumble` | numeric | PFF ball-security (hands/fumble) grade, 0-100. |
| `longest` | numeric | Longest run in yards. |
| `routes` | numeric | Pass routes run by the player. |
| `player` | character | Player's display name as PFF lists it. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `rec_yards` | numeric | Receiving yards gained by the ball carrier (the rushing report also carries his receiving line). |
| `gap_attempts` | numeric | Rushing attempts on gap-scheme runs. |
| `run_plays` | numeric | Run-play snaps. |
| `avoided_tackles` | numeric | Missed tackles forced. |
| `grades_offense_penalty` | numeric | PFF offensive penalty grade, 0-100. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `touchdowns` | numeric | Rushing touchdowns. |
| `grades_pass` | numeric | PFF passing grade, 0-100. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_facet_rushing_summary-example}

```python
pff_api_facet_rushing_summary(league='nfl', season='2022')
```

_Last validated n/a._

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

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_facet_defense_coverage-example}

```python
pff_api_facet_defense_coverage(league='nfl', season='2022')
```

_Last validated n/a._

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
| `man_touchdowns` | numeric | Touchdowns allowed into the player's coverage when in man coverage. |
| `man_interceptions` | numeric | Interceptions made in coverage when in man coverage. |
| `zone_snap_counts_coverage_percent` | numeric | Share of the player's coverage snaps played when in zone coverage. |
| `man_avg_depth_of_target` | numeric | Average depth of targets into the player's coverage, in yards downfield when in man coverage. |
| `man_dropped_ints` | numeric | Interception chances PFF charted as dropped when in man coverage. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `zone_coverage_snaps_per_target` | numeric | Coverage snaps played per target into the player's coverage when in zone coverage. |
| `man_yards_per_reception` | numeric | Average yards allowed per reception when in man coverage. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
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
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `zone_yards_per_coverage_snap` | numeric | Receiving yards allowed per coverage snap when in zone coverage. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `man_catch_rate` | numeric | Completion percentage allowed on targets into the player's coverage when in man coverage. |
| `man_grades_coverage_defense` | numeric | PFF coverage grade when in man coverage, 0-100. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `man_yards_after_catch` | numeric | Yards after the catch allowed when in man coverage. |
| `man_pass_break_ups` | numeric | Passes broken up when in man coverage. |
| `man_yards` | numeric | Receiving yards allowed when in man coverage. |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
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
| `player` | character | Player's display name as PFF lists it. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `zone_forced_incompletes` | numeric | Incompletions forced by the player's coverage, per PFF charting when in zone coverage. |
| `man_forced_incompletion_rate` | numeric | Share of targets into the player's coverage with a PFF-charted forced incompletion when in man coverage. |
| `zone_targets` | numeric | Targets into the player's coverage when in zone coverage. |
| `zone_longest` | numeric | Longest completion allowed, in yards when in zone coverage. |
| `zone_dropped_ints` | numeric | Interception chances PFF charted as dropped when in zone coverage. |
| `zone_interceptions` | numeric | Interceptions made in coverage when in zone coverage. |
| `man_forced_incompletes` | numeric | Incompletions forced by the player's coverage, per PFF charting when in man coverage. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `zone_missed_tackles` | numeric | Missed tackles when in zone coverage. |
| `base_snap_counts_coverage` | numeric | Coverage snaps from the facet's unsplit base row, covering all coverage schemes. |
| `man_missed_tackles` | numeric | Missed tackles when in man coverage. |
| `zone_grades_coverage_defense` | numeric | PFF coverage grade when in zone coverage, 0-100. |
| `man_qb_rating_against` | numeric | NFL passer rating allowed on targets into the player's coverage when in man coverage. |
| `zone_pass_break_ups` | numeric | Passes broken up when in zone coverage. |
| `zone_qb_rating_against` | numeric | NFL passer rating allowed on targets into the player's coverage when in zone coverage. |
| `man_yards_per_coverage_snap` | numeric | Receiving yards allowed per coverage snap when in man coverage. |
| `zone_catch_rate` | numeric | Completion percentage allowed on targets into the player's coverage when in zone coverage. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_facet_defense_coverage_scheme-example}

```python
pff_api_facet_defense_coverage_scheme(league='nfl', season='2022')
```

_Last validated n/a._

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
| `broken_up_passes` | integer | Passes broken up in the row's coverage matchups. |
| `drops` | integer | Dropped passes in the row's coverage matchups. |
| `first_downs` | integer | Receiving first downs (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `grades_coverage_defense` | numeric | The player's PFF coverage grade (0-100). |
| `grades_defense` | numeric | The player's PFF overall defense grade (0-100). |
| `grades_defense_penalty` | numeric | The player's PFF defensive penalty grade (0-100). |
| `grades_overall` | numeric | The player's PFF overall grade (0-100) over the requested filters. |
| `grades_overall_tackle` | numeric | The player's PFF overall tackling grade (0-100). |
| `grades_pass_rush_defense` | numeric | The player's PFF pass-rush grade (0-100). |
| `grades_run_defense` | numeric | The player's PFF run-defense grade (0-100). |
| `grades_tackle` | numeric | The player's PFF tackling grade (0-100). |
| `interceptions` | integer | Interceptions on throws in the row's coverage matchups. |
| `longest` | integer | Longest reception, in yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `player_game_count` | integer | Games the player appeared in over the requested filters. |
| `player_id` | integer | PFF player id: the covering defender (defenders) or the receiver (receivers, versus). |
| `receptions` | integer | Receptions (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `targets` | integer | Targets (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `touchdowns` | integer | Receiving touchdowns (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards` | integer | Receiving yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_after_catch` | integer | Yards after the catch (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_per_reception` | numeric | Yards per reception (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `grades_offense` | numeric | The player's PFF overall offense grade (0-100). |
| `grades_offense_penalty` | numeric | The player's PFF offensive penalty grade (0-100). |
| `grades_run_block` | numeric | The player's PFF run-blocking grade (0-100). |
| `grades_pass_block` | numeric | The player's PFF pass-blocking grade (0-100). |
| `grades_hands_drop` | numeric | The player's PFF hands (drop) grade (0-100). |
| `grades_hands_fumble` | numeric | The player's PFF ball-security (fumble) grade (0-100). |
| `grades_pass_route` | numeric | The player's PFF receiving (route-running) grade (0-100). |
| `grades_pass` | numeric | The player's PFF passing grade (0-100). |
| `grades_run` | numeric | The player's PFF rushing grade (0-100). |

**receivers**

| col_name | type | description |
|---|---|---|
| `broken_up_passes` | integer | Passes broken up in the row's coverage matchups. |
| `drops` | integer | Dropped passes in the row's coverage matchups. |
| `first_downs` | integer | Receiving first downs (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `grades_hands_drop` | numeric | The player's PFF hands (drop) grade (0-100). |
| `grades_hands_fumble` | numeric | The player's PFF ball-security (fumble) grade (0-100). |
| `grades_offense` | numeric | The player's PFF overall offense grade (0-100). |
| `grades_offense_penalty` | numeric | The player's PFF offensive penalty grade (0-100). |
| `grades_overall` | numeric | The player's PFF overall grade (0-100) over the requested filters. |
| `grades_pass_route` | numeric | The player's PFF receiving (route-running) grade (0-100). |
| `grades_run_block` | numeric | The player's PFF run-blocking grade (0-100). |
| `grades_screen_block` | numeric | The player's PFF screen-blocking grade (0-100). |
| `interceptions` | integer | Interceptions on throws in the row's coverage matchups. |
| `longest` | integer | Longest reception, in yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `player_game_count` | integer | Games the player appeared in over the requested filters. |
| `player_id` | integer | PFF player id: the covering defender (defenders) or the receiver (receivers, versus). |
| `receptions` | integer | Receptions (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `targets` | integer | Targets (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `touchdowns` | integer | Receiving touchdowns (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards` | integer | Receiving yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_after_catch` | integer | Yards after the catch (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_per_reception` | numeric | Yards per reception (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `grades_pass` | numeric | The player's PFF passing grade (0-100). |
| `grades_pass_block` | numeric | The player's PFF pass-blocking grade (0-100). |
| `grades_run` | numeric | The player's PFF rushing grade (0-100). |
| `grades_defense` | numeric | The player's PFF overall defense grade (0-100). |
| `grades_defense_penalty` | numeric | The player's PFF defensive penalty grade (0-100). |
| `grades_pass_rush_defense` | numeric | The player's PFF pass-rush grade (0-100). |
| `grades_run_defense` | numeric | The player's PFF run-defense grade (0-100). |
| `grades_coverage_defense` | numeric | The player's PFF coverage grade (0-100). |
| `grades_overall_tackle` | numeric | The player's PFF overall tackling grade (0-100). |
| `grades_tackle` | numeric | The player's PFF tackling grade (0-100). |
| `grades_snap` | numeric | PFF grade reported as grades_snap (0-100); PFF does not document it, and the capture shows it only in the ncaa receivers frame. |

**versus**

| col_name | type | description |
|---|---|---|
| `broken_up_passes` | integer | Passes broken up in the row's coverage matchups. |
| `coverage_player_id` | integer | PFF player id of the defender covering the receiver (versus only). |
| `drops` | integer | Dropped passes in the row's coverage matchups. |
| `first_downs` | integer | Receiving first downs (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `interceptions` | integer | Interceptions on throws in the row's coverage matchups. |
| `longest` | integer | Longest reception, in yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `player_id` | integer | PFF player id: the covering defender (defenders) or the receiver (receivers, versus). |
| `receptions` | integer | Receptions (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `targets` | integer | Targets (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `touchdowns` | integer | Receiving touchdowns (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards` | integer | Receiving yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_after_catch` | integer | Yards after the catch (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_per_reception` | numeric | Yards per reception (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_facet_defense_coverage_matchup-example}

```python
pff_api_facet_defense_coverage_matchup(league='nfl', season='2022')
```

_Last validated n/a._

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

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_facet_defense_pass_rush-example}

```python
pff_api_facet_defense_pass_rush(league='nfl', season='2022')
```

_Last validated n/a._

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

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_facet_defense_run-example}

```python
pff_api_facet_defense_run(league='nfl', season='2022')
```

_Last validated n/a._

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

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_facet_defense_summary-example}

```python
pff_api_facet_defense_summary(league='nfl', season='2022')
```

_Last validated n/a._

## pff_api_facet_field_goal_summary

League-wide field-goal kicking leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/field_goal/summary`

**Valid URL:** [https://api.pff.com/v1/facet/field_goal/summary?league=nfl&season=2022](https://api.pff.com/v1/facet/field_goal/summary?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_field_goal_summary-returns}

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

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_facet_field_goal_summary-example}

```python
pff_api_facet_field_goal_summary(league='nfl', season='2022')
```

_Last validated n/a._

## pff_api_facet_kickoff_summary

League-wide kickoff leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/kickoff/summary`

**Valid URL:** [https://api.pff.com/v1/facet/kickoff/summary?league=nfl&season=2022](https://api.pff.com/v1/facet/kickoff/summary?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_kickoff_summary-returns}

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

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_facet_kickoff_summary-example}

```python
pff_api_facet_kickoff_summary(league='nfl', season='2022')
```

_Last validated n/a._
