---
title: "NFL — PFF Developer API (api.pff.com, API key) — Facet: field–special"
sidebar_label: "Facet: field–special"
sidebar_position: 7
description: "NFL — PFF Developer API (api.pff.com, API key) — Facet: field–special — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — PFF Developer API (api.pff.com, API key) — Facet: field–special

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

### Raises {#pff_api_facet_field_goal_summary-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_field_goal_summary-example}

```python
pff_api_facet_field_goal_summary(league='nfl', season='2022')
```

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

### Raises {#pff_api_facet_kickoff_summary-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_kickoff_summary-example}

```python
pff_api_facet_kickoff_summary(league='nfl', season='2022')
```

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

### Raises {#pff_api_facet_punting_summary-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_punting_summary-example}

```python
pff_api_facet_punting_summary(league='nfl', season='2022')
```

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

### Raises {#pff_api_facet_return_summary-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_return_summary-example}

```python
pff_api_facet_return_summary(league='nfl', season='2022')
```

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

### Raises {#pff_api_facet_special_summary-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_special_summary-example}

```python
pff_api_facet_special_summary(league='nfl', season='2022')
```
