---
title: "NFL — PFF Developer API (api.pff.com, API key) — Facet: punting–special"
sidebar_label: "Facet: punting–special"
sidebar_position: 7
description: "NFL — PFF Developer API (api.pff.com, API key) — Facet: punting–special — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — PFF Developer API (api.pff.com, API key) — Facet: punting–special

## pff_api_facet_punting_summary

League-wide punting leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/punting/summary`

**Valid URL:** [https://api.pff.com/v1/facet/punting/summary?league=nfl&season=2022](https://api.pff.com/v1/facet/punting/summary?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_punting_summary-returns}

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

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_facet_punting_summary-example}

```python
pff_api_facet_punting_summary(league='nfl', season='2022')
```

_Last validated n/a._

## pff_api_facet_return_summary

League-wide return leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/return/summary`

**Valid URL:** [https://api.pff.com/v1/facet/return/summary?league=nfl&season=2022](https://api.pff.com/v1/facet/return/summary?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_return_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `grades_kick_return` | numeric | PFF kickoff-return grade, 0-100. |
| `grades_return` | numeric | PFF overall return grade, 0-100. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `kickoff_attempts` | numeric | Kickoff returns attempted. |
| `kickoff_fair_catches` | numeric | Kickoffs fair-caught by the player. |
| `kickoff_long` | numeric | Longest kickoff return in yards. |
| `kickoff_muffed_returns` | numeric | Kickoff returns the player muffed. |
| `kickoff_touchdowns` | numeric | Kickoff returns scoring a touchdown. |
| `kickoff_yards` | numeric | Total kickoff-return yards. |
| `kickoff_ypa` | numeric | Average yards per kickoff return. |
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `player` | character | Player's display name as PFF lists it. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `punt_attempts` | numeric | Punt returns attempted. |
| `punt_fair_catches` | numeric | Punts fair-caught by the player. |
| `punt_long` | numeric | Longest punt return in yards. |
| `punt_muffed_returns` | numeric | Punt returns the player muffed. |
| `punt_touchdowns` | numeric | Punt returns scoring a touchdown. |
| `punt_yards` | numeric | Total punt-return yards. |
| `punt_ypa` | numeric | Average yards per punt return. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `total_attempts` | numeric | Total return attempts, kickoffs and punts combined. |
| `grades_punt_return` | numeric | PFF punt-return grade, 0-100. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_facet_return_summary-example}

```python
pff_api_facet_return_summary(league='nfl', season='2022')
```

_Last validated n/a._

## pff_api_facet_special_summary

League-wide special-teams leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/special/summary`

**Valid URL:** [https://api.pff.com/v1/facet/special/summary?league=nfl&season=2022](https://api.pff.com/v1/facet/special/summary?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_special_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `assists` | numeric | Assisted tackles credited to the player on special-teams plays. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `grades_fgep_kicker` | numeric | PFF field-goal and extra-point kicking grade, 0-100. |
| `grades_kickoff_kicker` | numeric | PFF kickoff kicking grade, 0-100. |
| `grades_misc_st` | numeric | PFF miscellaneous special-teams grade, 0-100. |
| `grades_special_teams_penalty` | numeric | PFF special-teams penalty grade, 0-100. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `missed_tackles` | numeric | Missed tackles on special-teams plays. |
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `player` | character | Player's display name as PFF lists it. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `snap_counts_field_goal` | numeric | Snaps on the field-goal and extra-point unit. |
| `snap_counts_field_goal_blocking` | numeric | Snaps on the field-goal and extra-point block unit. |
| `snap_counts_kickoff` | numeric | Snaps on the kickoff coverage unit. |
| `snap_counts_kickoff_return` | numeric | Snaps on the kickoff return unit. |
| `snap_counts_punt_coverage` | numeric | Snaps on the punt coverage unit. |
| `snap_counts_punt_return` | numeric | Snaps on the punt return unit. |
| `tackles` | numeric | Tackles made by the player on special-teams plays. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `grades_fgep_defense` | numeric | PFF grade on field-goal and extra-point defense, 0-100. |
| `grades_fgep_offense` | numeric | PFF grade on the field-goal and extra-point protection unit, 0-100. |
| `grades_long_snap` | numeric | PFF long-snapping grade, 0-100. |
| `grades_punter` | numeric | PFF punting grade, 0-100. |
| `grades_kick_return` | numeric | PFF kickoff-return grade, 0-100. |
| `grades_punt_return` | numeric | PFF punt-return grade, 0-100. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_facet_special_summary-example}

```python
pff_api_facet_special_summary(league='nfl', season='2022')
```

_Last validated n/a._
