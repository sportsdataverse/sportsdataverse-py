---
title: "NFL — PFF Developer API (api.pff.com, API key) — Player: receiving–special"
sidebar_label: "Player: receiving–special"
sidebar_position: 12
description: "NFL — PFF Developer API (api.pff.com, API key) — Player: receiving–special — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — PFF Developer API (api.pff.com, API key) — Player: receiving–special

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
| `targets` | integer | Times targeted. |
| `grades_offense` | numeric | PFF overall offense grade (0-100). |
| `game_id` | integer | PFF game id of the game (integer join key). |
| `yards_after_catch_per_reception` | numeric | Average yards after the catch per reception. |
| `grades_pass_route` | numeric | PFF receiving/route grade (0-100). |
| `week` | integer | Week number of the game, as PFF numbers weeks. |
| `status` | character | "S" when the player started the game; PFF owns the value set. |
| `yprr` | numeric | Yards per route run. |
| `wide_snaps` | integer | Receiving snaps aligned out wide. |
| `fumbles` | integer | Fumbles by the player after the catch. |
| `first_downs` | integer | Receptions that converted a first down. |
| `jersey_number` | character | Jersey number the player wore in the game (string; zero-padded, e.g. "09"). |
| `inline_snaps` | integer | Receiving snaps aligned inline, tight to the formation. |
| `away_team_name` | character | Abbreviation of the away team (e.g. "LV"). |
| `contested_targets` | integer | Contested targets. |
| `inline_rate` | numeric | Share of receiving snaps aligned inline, tight to the formation. |
| `contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught. |
| `yards` | integer | Receiving yards. |
| `player_franchise_id` | integer | PFF franchise id of the team the player played for in the game. |
| `receptions` | integer | Passes caught by the receiver. |
| `targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player. |
| `interceptions` | integer | Interceptions on passes targeting the player. |
| `caught_percent` | numeric | Percentage of targets caught. |
| `positive_epa_plays` | integer | Plays with positive expected points added (positive_epa_percent = positive_epa_plays / plays_with_epa x 100). |
| `drop_rate` | numeric | Share of catchable targets the player dropped. |
| `grades_hands_drop` | numeric | PFF hands/drop grade (0-100). |
| `slot_rate` | numeric | Share of receiving snaps aligned in the slot. |
| `plays_with_epa` | integer | Plays with an expected-points-added value, the denominator of positive_epa_percent. |
| `slot_snaps` | integer | Receiving snaps aligned in the slot. |
| `penalties` | integer | Penalties charged to the player over the covered span. |
| `wide_rate` | numeric | Share of receiving snaps aligned out wide. |
| `pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking. |
| `declined_penalties` | integer | Penalties committed by the player that were declined. |
| `route_rate` | numeric | Share of pass-play snaps on which the player ran a route. |
| `drops` | integer | Dropped passes. |
| `position` | character | PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `grades_hands_fumble` | numeric | PFF ball-security (hands/fumble) grade, 0-100. |
| `longest` | integer | Longest reception in yards. |
| `pass_blocks` | integer | Pass-play snaps spent pass blocking. |
| `away_franchise_id` | integer | PFF franchise id of the away team. |
| `routes` | integer | Pass routes run by the receiver. |
| `pass_plays` | integer | Pass-play snaps. |
| `yards_per_reception` | numeric | Average yards per reception. |
| `targets_percent` | numeric | Targets per route run, as a percentage (targets / routes x 100). |
| `positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added. |
| `contested_receptions` | integer | Contested catches made. |
| `yards_after_catch` | integer | Yards after the catch. |
| `home_franchise_id` | integer | PFF franchise id of the home team. |
| `home_team_name` | character | Abbreviation of the home team (e.g. "NE"). |
| `avg_depth_of_target` | numeric | Average depth of target in yards downfield. |
| `epa` | numeric | Expected points added per target to the player, as computed by PFF (an average such as -0.27, not a total). |
| `avoided_tackles` | integer | Tackles avoided after the catch. |
| `player_id` | integer | PFF player id (integer join key) of the player the report is about. |
| `touchdowns` | integer | Receiving touchdowns. |
| `game_away_franchise_id` | integer | Repeats away_franchise_id from the row's nested game object: PFF franchise id of the away team. |
| `game_away_team_name` | character | Repeats away_team_name from the row's nested game object: Abbreviation of the away team (e.g. "LV"). |
| `game_game_id` | integer | Repeats game_id from the row's nested game object: PFF game id of the game (integer join key). |
| `game_home_franchise_id` | integer | Repeats home_franchise_id from the row's nested game object: PFF franchise id of the home team. |
| `game_home_team_name` | character | Repeats home_team_name from the row's nested game object: Abbreviation of the home team (e.g. "NE"). |
| `game_player_franchise_id` | integer | Repeats player_franchise_id from the row's nested game object: PFF franchise id of the team the player played for in the game. |
| `game_position` | character | Repeats position from the row's nested game object: PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `game_status` | character | Repeats status from the row's nested game object: "S" when the player started the game; PFF owns the value set. |
| `game_week` | integer | Repeats week from the row's nested game object: Week number of the game, as PFF numbers weeks. |
| `league_id` | integer | PFF league id (1 = NFL, 2 = NCAA), filled from the report's subject. |
| `season` | integer | Season (starting year) of the report, filled from the report's subject. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_player_receiving_summary-example}

```python
pff_api_player_receiving_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

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
| `targets` | integer | Passes thrown into the player's coverage (targets allowed). |
| `game_id` | integer | PFF game id of the game (integer join key). |
| `interception_touchdowns` | integer | Touchdowns scored on interception returns. |
| `week` | integer | Week number of the game, as PFF numbers weeks. |
| `forced_fumbles` | integer | Forced fumbles. |
| `missed_tackles` | integer | Missed tackles. |
| `status` | character | "S" when the player started the game; PFF owns the value set. |
| `catch_rate` | numeric | Completion percentage allowed on targets into the player's coverage. |
| `tackles` | integer | Total tackles made by the defender. |
| `jersey_number` | character | Jersey number the player wore in the game (string; zero-padded, e.g. "09"). |
| `away_team_name` | character | Abbreviation of the away team (e.g. "LV"). |
| `snap_counts_offball` | integer | Snaps aligned as an off-ball linebacker. |
| `snap_counts_box` | integer | Snaps aligned in the box. |
| `sacks` | integer | Sacks credited. |
| `snap_counts_pass_rush` | integer | Pass-rush snaps played. |
| `snap_counts_dl` | integer | Snaps aligned on the defensive line. |
| `grades_tackle` | numeric | PFF tackling grade, 0-100. |
| `yards` | integer | Receiving yards allowed in the player's coverage. |
| `player_franchise_id` | integer | PFF franchise id of the team the player played for in the game. |
| `receptions` | integer | Receptions allowed in the player's coverage. |
| `grades_coverage_defense` | numeric | PFF coverage grade (0-100). |
| `hurries` | integer | Quarterback hurries recorded. |
| `interceptions` | integer | Interceptions made in coverage. |
| `snap_counts_coverage` | integer | Coverage snaps played. |
| `snap_counts_dl_over_t` | integer | Defensive-line snaps aligned head-up over the offensive tackle. |
| `snap_counts_dl_a_gap` | integer | Defensive-line snaps aligned in the A gap. |
| `fumble_recoveries` | integer | Opponent fumbles recovered by the player. |
| `grades_run_defense` | numeric | PFF run-defense grade (0-100). |
| `snap_counts_corner` | integer | Snaps aligned at outside cornerback. |
| `hits` | integer | Quarterback hits recorded by the player. |
| `penalties` | integer | Penalties charged to the player over the covered span. |
| `batted_passes` | integer | Passes batted down at the line of scrimmage. |
| `stops` | integer | Tackles that constitute an offensive failure ("stops"). |
| `declined_penalties` | integer | Penalties committed by the player that were declined. |
| `total_pressures` | integer | Total quarterback pressures (sacks + hits + hurries). |
| `position` | character | PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `fumble_recovery_touchdowns` | integer | Touchdowns scored on fumble recoveries. |
| `longest` | integer | Longest completion allowed, in yards. |
| `away_franchise_id` | integer | PFF franchise id of the away team. |
| `snap_counts_slot` | integer | Snaps aligned in the slot. |
| `missed_tackle_rate` | numeric | Share of tackle attempts the player missed. |
| `grades_defense` | numeric | PFF overall defense grade (0-100). |
| `yards_per_reception` | numeric | Average yards allowed per reception. |
| `grades_defense_penalty` | numeric | PFF defensive penalty grade, 0-100. |
| `safeties` | integer | Safeties recorded by the player. |
| `snap_counts_defense` | integer | Total defensive snaps played. |
| `yards_after_catch` | integer | Yards after the catch allowed in the player's coverage. |
| `home_franchise_id` | integer | PFF franchise id of the home team. |
| `snap_counts_dl_b_gap` | integer | Defensive-line snaps aligned in the B gap. |
| `home_team_name` | character | Abbreviation of the home team (e.g. "NE"). |
| `pass_break_ups` | integer | Passes broken up. |
| `qb_rating_against` | numeric | NFL passer rating allowed on targets into the player's coverage. |
| `snap_counts_run_defense` | integer | Run-defense snaps played. |
| `tackles_for_loss` | integer | Tackles for loss made by the player. |
| `assists` | integer | Assisted tackles. |
| `grades_pass_rush_defense` | numeric | PFF pass-rush grade, 0-100. |
| `player_id` | integer | PFF player id (integer join key) of the player the report is about. |
| `snap_counts_fs` | integer | Snaps aligned at free safety. |
| `touchdowns` | integer | Touchdowns allowed into the player's coverage. |
| `snap_counts_dl_outside_t` | integer | Defensive-line snaps aligned outside the offensive tackle. |
| `game_away_franchise_id` | integer | Repeats away_franchise_id from the row's nested game object: PFF franchise id of the away team. |
| `game_away_team_name` | character | Repeats away_team_name from the row's nested game object: Abbreviation of the away team (e.g. "LV"). |
| `game_game_id` | integer | Repeats game_id from the row's nested game object: PFF game id of the game (integer join key). |
| `game_home_franchise_id` | integer | Repeats home_franchise_id from the row's nested game object: PFF franchise id of the home team. |
| `game_home_team_name` | character | Repeats home_team_name from the row's nested game object: Abbreviation of the home team (e.g. "NE"). |
| `game_player_franchise_id` | integer | Repeats player_franchise_id from the row's nested game object: PFF franchise id of the team the player played for in the game. |
| `game_position` | character | Repeats position from the row's nested game object: PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `game_status` | character | Repeats status from the row's nested game object: "S" when the player started the game; PFF owns the value set. |
| `game_week` | integer | Repeats week from the row's nested game object: Week number of the game, as PFF numbers weeks. |
| `league_id` | integer | PFF league id (1 = NFL, 2 = NCAA), filled from the report's subject. |
| `season` | integer | Season (starting year) of the report, filled from the report's subject. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_player_defense_summary-example}

```python
pff_api_player_defense_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

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
| `twenty_attempts` | integer | Field goals attempted from 20-29 yards. |
| `game_id` | integer | PFF game id of the game (integer join key). |
| `pat_percent` | numeric | Extra-point percentage. |
| `week` | integer | Week number of the game, as PFF numbers weeks. |
| `status` | character | "S" when the player started the game; PFF owns the value set. |
| `forty_made` | integer | Field goals made from 40-49 yards. |
| `fifty_percent` | numeric | Field-goal percentage from 50 or more yards. |
| `total_made` | integer | Total field goals made across all distances. |
| `jersey_number` | character | Jersey number the player wore in the game (string; zero-padded, e.g. "09"). |
| `away_team_name` | character | Abbreviation of the away team (e.g. "LV"). |
| `one_made` | integer | Field goals made from 1-19 yards. |
| `fifty_attempts` | integer | Field goals attempted from 50 or more yards. |
| `forty_attempts` | integer | Field goals attempted from 40-49 yards. |
| `thirty_percent` | numeric | Field-goal percentage from 30-39 yards. |
| `total_attempts` | integer | Total field goals attempted across all distances. |
| `pat_attempts` | integer | Extra points attempted. |
| `player_franchise_id` | integer | PFF franchise id of the team the player played for in the game. |
| `twenty_made` | integer | Field goals made from 20-29 yards. |
| `one_attempts` | integer | Field goals attempted from 1-19 yards. |
| `grades_fgep_kicker` | numeric | PFF field-goal and extra-point kicking grade, 0-100. |
| `thirty_attempts` | integer | Field goals attempted from 30-39 yards. |
| `pat_made` | integer | Extra points made. |
| `one_percent` | numeric | Field-goal percentage from 1-19 yards. |
| `total_percent` | numeric | Overall field-goal percentage. |
| `position` | character | PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `twenty_percent` | numeric | Field-goal percentage from 20-29 yards. |
| `away_franchise_id` | integer | PFF franchise id of the away team. |
| `forty_percent` | numeric | Field-goal percentage from 40-49 yards. |
| `fifty_made` | integer | Field goals made from 50 or more yards. |
| `thirty_made` | integer | Field goals made from 30-39 yards. |
| `home_franchise_id` | integer | PFF franchise id of the home team. |
| `home_team_name` | character | Abbreviation of the home team (e.g. "NE"). |
| `player_id` | integer | PFF player id (integer join key) of the player the report is about. |
| `game_away_franchise_id` | integer | Repeats away_franchise_id from the row's nested game object: PFF franchise id of the away team. |
| `game_away_team_name` | character | Repeats away_team_name from the row's nested game object: Abbreviation of the away team (e.g. "LV"). |
| `game_game_id` | integer | Repeats game_id from the row's nested game object: PFF game id of the game (integer join key). |
| `game_home_franchise_id` | integer | Repeats home_franchise_id from the row's nested game object: PFF franchise id of the home team. |
| `game_home_team_name` | character | Repeats home_team_name from the row's nested game object: Abbreviation of the home team (e.g. "NE"). |
| `game_player_franchise_id` | integer | Repeats player_franchise_id from the row's nested game object: PFF franchise id of the team the player played for in the game. |
| `game_position` | character | Repeats position from the row's nested game object: PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `game_status` | character | Repeats status from the row's nested game object: "S" when the player started the game; PFF owns the value set. |
| `game_week` | integer | Repeats week from the row's nested game object: Week number of the game, as PFF numbers weeks. |
| `league_id` | integer | PFF league id (1 = NFL, 2 = NCAA), filled from the report's subject. |
| `season` | integer | Season (starting year) of the report, filled from the report's subject. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_player_field_goal_summary-example}

```python
pff_api_player_field_goal_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

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
| `attempts` | integer | Kickoffs by the player. |
| `attempts_with_hangtime` | integer | Kickoffs with a PFF-recorded hangtime. |
| `average_distance` | numeric | Average kickoff distance in yards. |
| `average_hangtime` | numeric | Average kickoff hangtime in seconds. |
| `average_starting_field_position` | numeric | Average opponent starting field position following the player's kickoffs. |
| `average_yards_per_return` | numeric | Average return yards allowed per kickoff returned. |
| `away_franchise_id` | integer | PFF franchise id of the away team. |
| `away_team_name` | character | Abbreviation of the away team (e.g. "LV"). |
| `fair_catches` | integer | Kickoffs fair-caught by the return team. |
| `game_id` | integer | PFF game id of the game (integer join key). |
| `grades_kickoff_kicker` | numeric | PFF kickoff kicking grade, 0-100. |
| `home_franchise_id` | integer | PFF franchise id of the home team. |
| `home_team_name` | character | Abbreviation of the home team (e.g. "NE"). |
| `jersey_number` | character | Jersey number the player wore in the game (string; zero-padded, e.g. "09"). |
| `kicked_yards` | integer | Total kickoff yards. |
| `kicks_returned` | integer | Kickoffs returned by the opponent. |
| `onside_kicks` | integer | Onside kicks attempted. |
| `percent_returned` | numeric | Percentage of the player's kickoffs that were returned. |
| `player_franchise_id` | integer | PFF franchise id of the team the player played for in the game. |
| `player_id` | integer | PFF player id (integer join key) of the player the report is about. |
| `position` | character | PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `return_yards` | integer | Return yards gained by the return team on the player's kickoffs. |
| `status` | character | "S" when the player started the game; PFF owns the value set. |
| `total_hangtime` | numeric | Total kickoff hangtime in seconds. |
| `touchbacks` | integer | Kickoffs resulting in touchbacks. |
| `week` | integer | Week number of the game, as PFF numbers weeks. |
| `game_away_franchise_id` | integer | Repeats away_franchise_id from the row's nested game object: PFF franchise id of the away team. |
| `game_away_team_name` | character | Repeats away_team_name from the row's nested game object: Abbreviation of the away team (e.g. "LV"). |
| `game_game_id` | integer | Repeats game_id from the row's nested game object: PFF game id of the game (integer join key). |
| `game_home_franchise_id` | integer | Repeats home_franchise_id from the row's nested game object: PFF franchise id of the home team. |
| `game_home_team_name` | character | Repeats home_team_name from the row's nested game object: Abbreviation of the home team (e.g. "NE"). |
| `game_player_franchise_id` | integer | Repeats player_franchise_id from the row's nested game object: PFF franchise id of the team the player played for in the game. |
| `game_position` | character | Repeats position from the row's nested game object: PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `game_status` | character | Repeats status from the row's nested game object: "S" when the player started the game; PFF owns the value set. |
| `game_week` | integer | Repeats week from the row's nested game object: Week number of the game, as PFF numbers weeks. |
| `league_id` | integer | PFF league id (1 = NFL, 2 = NCAA), filled from the report's subject. |
| `season` | integer | Season (starting year) of the report, filled from the report's subject. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_player_kickoff_summary-example}

```python
pff_api_player_kickoff_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

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
| `game_id` | integer | PFF game id of the game (integer join key). |
| `touchbacks` | integer | Punts resulting in touchbacks. |
| `attempts_with_hangtime` | integer | Punts with a PFF-recorded hangtime. |
| `percent_returned` | numeric | Percentage of the player's punts that were returned. |
| `week` | integer | Week number of the game, as PFF numbers weeks. |
| `status` | character | "S" when the player started the game; PFF owns the value set. |
| `fair_catches` | integer | Punts fair-caught by the return team. |
| `jersey_number` | character | Jersey number the player wore in the game (string; zero-padded, e.g. "09"). |
| `away_team_name` | character | Abbreviation of the away team (e.g. "LV"). |
| `average_net_yards` | numeric | Average net punting yards per attempt. |
| `yards` | integer | Gross punt yards: the summed distance of the player's punts, before any return. |
| `player_franchise_id` | integer | PFF franchise id of the team the player played for in the game. |
| `average_hangtime` | numeric | Average punt hangtime in seconds. |
| `total_net_yards` | integer | Total net punting yards. |
| `attempts` | integer | Punts by the player. |
| `inside_twenties` | integer | Punts downed inside the opponent 20-yard line. |
| `out_of_bounds` | integer | Punts that went out of bounds. |
| `average_yards_per_return` | numeric | Average return yards allowed per punt returned. |
| `total_hangtime` | numeric | Total punt hangtime in seconds. |
| `returns` | integer | Punts returned by the opponent. |
| `position` | character | PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `long` | integer | Longest punt in yards. |
| `away_franchise_id` | integer | PFF franchise id of the away team. |
| `blocks` | integer | Punts that were blocked. |
| `average_yards_per_attempt` | numeric | Average gross punting yards per attempt. |
| `grades_punter` | numeric | PFF punting grade, 0-100. |
| `home_franchise_id` | integer | PFF franchise id of the home team. |
| `home_team_name` | character | Abbreviation of the home team (e.g. "NE"). |
| `return_yards` | integer | Return yards gained by the return team on the player's punts. |
| `downeds` | integer | Punts downed by the coverage unit. |
| `player_id` | integer | PFF player id (integer join key) of the player the report is about. |
| `snaps` | integer | Punting snaps played. |
| `game_away_franchise_id` | integer | Repeats away_franchise_id from the row's nested game object: PFF franchise id of the away team. |
| `game_away_team_name` | character | Repeats away_team_name from the row's nested game object: Abbreviation of the away team (e.g. "LV"). |
| `game_game_id` | integer | Repeats game_id from the row's nested game object: PFF game id of the game (integer join key). |
| `game_home_franchise_id` | integer | Repeats home_franchise_id from the row's nested game object: PFF franchise id of the home team. |
| `game_home_team_name` | character | Repeats home_team_name from the row's nested game object: Abbreviation of the home team (e.g. "NE"). |
| `game_player_franchise_id` | integer | Repeats player_franchise_id from the row's nested game object: PFF franchise id of the team the player played for in the game. |
| `game_position` | character | Repeats position from the row's nested game object: PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `game_status` | character | Repeats status from the row's nested game object: "S" when the player started the game; PFF owns the value set. |
| `game_week` | integer | Repeats week from the row's nested game object: Week number of the game, as PFF numbers weeks. |
| `league_id` | integer | PFF league id (1 = NFL, 2 = NCAA), filled from the report's subject. |
| `season` | integer | Season (starting year) of the report, filled from the report's subject. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_player_punting_summary-example}

```python
pff_api_player_punting_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

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
| `away_franchise_id` | integer | PFF franchise id of the away team. |
| `away_team_name` | character | Abbreviation of the away team (e.g. "LV"). |
| `game_id` | integer | PFF game id of the game (integer join key). |
| `grades_punt_return` | numeric | PFF punt-return grade, 0-100. |
| `grades_return` | numeric | PFF overall return grade, 0-100. |
| `home_franchise_id` | integer | PFF franchise id of the home team. |
| `home_team_name` | character | Abbreviation of the home team (e.g. "NE"). |
| `jersey_number` | character | Jersey number the player wore in the game (string; zero-padded, e.g. "09"). |
| `kickoff_attempts` | integer | Kickoff returns attempted. |
| `kickoff_count_of_yards` | integer | Kickoff returns counted toward the return-yardage figures (equal to kickoff_attempts in the captured rows). |
| `kickoff_fair_catches` | integer | Kickoffs fair-caught by the player. |
| `kickoff_long` | integer | Longest kickoff return in yards. |
| `kickoff_muffed_returns` | integer | Kickoff returns the player muffed. |
| `kickoff_touchdowns` | integer | Kickoff returns scoring a touchdown. |
| `kickoff_yards` | integer | Total kickoff-return yards. |
| `kickoff_ypa` | numeric | Average yards per kickoff return. |
| `player_franchise_id` | integer | PFF franchise id of the team the player played for in the game. |
| `player_id` | integer | PFF player id (integer join key) of the player the report is about. |
| `position` | character | PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `punt_attempts` | integer | Punt returns attempted. |
| `punt_count_of_yards` | integer | Punt returns counted toward the return-yardage figures (equal to punt_attempts in the captured rows). |
| `punt_fair_catches` | integer | Punts fair-caught by the player. |
| `punt_long` | integer | Longest punt return in yards. |
| `punt_muffed_returns` | integer | Punt returns the player muffed. |
| `punt_touchdowns` | integer | Punt returns scoring a touchdown. |
| `punt_yards` | integer | Total punt-return yards. |
| `punt_ypa` | numeric | Average yards per punt return. |
| `status` | character | "S" when the player started the game; PFF owns the value set. |
| `total_attempts` | integer | Total return attempts, kickoffs and punts combined. |
| `week` | integer | Week number of the game, as PFF numbers weeks. |
| `game_away_franchise_id` | integer | Repeats away_franchise_id from the row's nested game object: PFF franchise id of the away team. |
| `game_away_team_name` | character | Repeats away_team_name from the row's nested game object: Abbreviation of the away team (e.g. "LV"). |
| `game_game_id` | integer | Repeats game_id from the row's nested game object: PFF game id of the game (integer join key). |
| `game_home_franchise_id` | integer | Repeats home_franchise_id from the row's nested game object: PFF franchise id of the home team. |
| `game_home_team_name` | character | Repeats home_team_name from the row's nested game object: Abbreviation of the home team (e.g. "NE"). |
| `game_player_franchise_id` | integer | Repeats player_franchise_id from the row's nested game object: PFF franchise id of the team the player played for in the game. |
| `game_position` | character | Repeats position from the row's nested game object: PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `game_status` | character | Repeats status from the row's nested game object: "S" when the player started the game; PFF owns the value set. |
| `game_week` | integer | Repeats week from the row's nested game object: Week number of the game, as PFF numbers weeks. |
| `league_id` | integer | PFF league id (1 = NFL, 2 = NCAA), filled from the report's subject. |
| `season` | integer | Season (starting year) of the report, filled from the report's subject. |
| `grades_kick_return` | numeric | PFF kickoff-return grade, 0-100. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_player_return_summary-example}

```python
pff_api_player_return_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

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
| `assists` | integer | Assisted tackles credited to the player on special-teams plays. |
| `away_franchise_id` | integer | PFF franchise id of the away team. |
| `away_team_name` | character | Abbreviation of the away team (e.g. "LV"). |
| `declined_penalties` | integer | Penalties committed by the player that were declined. |
| `game_id` | integer | PFF game id of the game (integer join key). |
| `grades_fgep_defense` | numeric | PFF grade on field-goal and extra-point defense, 0-100. |
| `grades_misc_st` | numeric | PFF miscellaneous special-teams grade, 0-100. |
| `grades_special_teams_penalty` | numeric | PFF special-teams penalty grade, 0-100. |
| `home_franchise_id` | integer | PFF franchise id of the home team. |
| `home_team_name` | character | Abbreviation of the home team (e.g. "NE"). |
| `jersey_number` | character | Jersey number the player wore in the game (string; zero-padded, e.g. "09"). |
| `missed_tackles` | integer | Missed tackles on special-teams plays. |
| `penalties` | integer | Penalties charged to the player over the covered span. |
| `player_franchise_id` | integer | PFF franchise id of the team the player played for in the game. |
| `player_id` | integer | PFF player id (integer join key) of the player the report is about. |
| `position` | character | PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `pu_def_rush` | integer | PFF punt-defense rush count (PFF's pu_def_rush; PFF does not document it further). |
| `snap_counts_field_goal` | integer | Snaps on the field-goal and extra-point unit. |
| `snap_counts_field_goal_blocking` | integer | Snaps on the field-goal and extra-point block unit. |
| `snap_counts_kickoff` | integer | Snaps on the kickoff coverage unit. |
| `snap_counts_kickoff_return` | integer | Snaps on the kickoff return unit. |
| `snap_counts_punt_coverage` | integer | Snaps on the punt coverage unit. |
| `snap_counts_punt_return` | integer | Snaps on the punt return unit. |
| `status` | character | "S" when the player started the game; PFF owns the value set. |
| `tackles` | integer | Tackles made by the player on special-teams plays. |
| `week` | integer | Week number of the game, as PFF numbers weeks. |
| `game_away_franchise_id` | integer | Repeats away_franchise_id from the row's nested game object: PFF franchise id of the away team. |
| `game_away_team_name` | character | Repeats away_team_name from the row's nested game object: Abbreviation of the away team (e.g. "LV"). |
| `game_game_id` | integer | Repeats game_id from the row's nested game object: PFF game id of the game (integer join key). |
| `game_home_franchise_id` | integer | Repeats home_franchise_id from the row's nested game object: PFF franchise id of the home team. |
| `game_home_team_name` | character | Repeats home_team_name from the row's nested game object: Abbreviation of the home team (e.g. "NE"). |
| `game_player_franchise_id` | integer | Repeats player_franchise_id from the row's nested game object: PFF franchise id of the team the player played for in the game. |
| `game_position` | character | Repeats position from the row's nested game object: PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `game_status` | character | Repeats status from the row's nested game object: "S" when the player started the game; PFF owns the value set. |
| `game_week` | integer | Repeats week from the row's nested game object: Week number of the game, as PFF numbers weeks. |
| `league_id` | integer | PFF league id (1 = NFL, 2 = NCAA), filled from the report's subject. |
| `season` | integer | Season (starting year) of the report, filled from the report's subject. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_player_special_summary-example}

```python
pff_api_player_special_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._
