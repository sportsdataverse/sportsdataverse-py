# NFL — PFF Developer API (api.pff.com, API key) — Player: seasons–passing

> NFL — PFF Developer API (api.pff.com, API key) — Player: seasons–passing — function reference in sdv-py, the SportsDataverse Python package.

## pff_api_player_seasons

List the seasons a player has data for

**Endpoint URL:** `GET https://api.pff.com/v1/player/seasons`

**Valid URL:** [https://api.pff.com/v1/player/seasons?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/seasons?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |

### Returns {#pff_api_player_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `value` | Int64 | A season (starting year) PFF has data for the player in, newest first. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_player_seasons-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_player_seasons-example}

```python
pff_api_player_seasons(league='nfl', player_id=28022, season=2022)
```

## pff_api_player_snaps_summary

Snap counts for a player, broken out by position

**Endpoint URL:** `GET https://api.pff.com/v1/player/snaps/summary`

**Valid URL:** [https://api.pff.com/v1/player/snaps/summary?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/snaps/summary?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, as a four-digit year, taken as a positional argument. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |

### Returns {#pff_api_player_snaps_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (starting year) the snap totals cover. |
| `snap_counts_coverage` | Int64 | Coverage snaps played. |
| `snap_counts_defense` | Int64 | Total defensive snaps played. |
| `snap_counts_field_goal` | Int64 | Snaps on the field-goal and extra-point unit. |
| `snap_counts_field_goal_blocking` | Int64 | Snaps on the field-goal and extra-point block unit. |
| `snap_counts_field_goal_kicking` | Int64 | Field-goal and extra-point snaps spent kicking. |
| `snap_counts_kickoff_coverage` | Int64 | Snaps on the kickoff coverage unit. |
| `snap_counts_kickoff_kicking` | Int64 | Kickoff snaps spent kicking off. |
| `snap_counts_kickoff_return_blocking` | Int64 | Kickoff-return snaps spent blocking. |
| `snap_counts_kickoff_returning` | Int64 | Kickoff-return snaps spent as the returner. |
| `snap_counts_offense` | Int64 | Offensive snaps played. |
| `snap_counts_pass` | Int64 | Pass-play snaps spent as the passer, rather than blocking or running a route. |
| `snap_counts_pass_block` | Int64 | Pass-blocking snaps played. |
| `snap_counts_pass_route` | Int64 | Snaps spent running a pass route. |
| `snap_counts_pass_rush` | Int64 | Pass-rush snaps played. |
| `snap_counts_punt_coverage` | Int64 | Snaps on the punt coverage unit. |
| `snap_counts_punt_punting` | Int64 | Punt snaps spent punting. |
| `snap_counts_punt_return_blocking` | Int64 | Punt-return snaps spent blocking. |
| `snap_counts_punt_returning` | Int64 | Punt-return snaps spent as the returner. |
| `snap_counts_run` | Int64 | Run-play snaps spent as a runner, rather than run blocking. |
| `snap_counts_run_block` | Int64 | Run-blocking snaps played. |
| `snap_counts_run_defense` | Int64 | Run-defense snaps played. |
| `snap_counts_special_teams` | Int64 | Total special-teams snaps played. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_player_snaps_summary-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_player_snaps_summary-example}

```python
pff_api_player_snaps_summary(league='nfl', player_id=28022, season=2022)
```

## pff_api_player_position_pivot

Player snap counts pivoted by position

**Endpoint URL:** `GET https://api.pff.com/v1/player/position/pivot`

**Valid URL:** [https://api.pff.com/v1/player/position/pivot?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/position/pivot?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, as a four-digit year, taken as a positional argument. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |

### Returns {#pff_api_player_position_pivot-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group` | String | Display position group (e.g. "QB", "WR"); one row per group. |
| `group_order` | Int64 | Sort order of the group on PFF's page (0 = first). |
| `positions` | String | JSON-encoded list of the group's alignments, each with its per-week snap counts by snap type (PFF has no group rollup row: group totals are summed from these weeks). |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_player_position_pivot-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_player_position_pivot-example}

```python
pff_api_player_position_pivot(league='nfl', player_id=28022, season=2022)
```

## pff_api_player_offense_summary

Offense summary for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/offense/summary`

**Valid URL:** [https://api.pff.com/v1/player/offense/summary?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/offense/summary?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns {#pff_api_player_offense_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `away_franchise_id` | Int64 | PFF franchise id of the away team. |
| `away_team_name` | String | Abbreviation of the away team (e.g. "LV"). |
| `declined_penalties` | Int64 | Penalties committed by the player that were declined. |
| `game_id` | Int64 | PFF game id of the game (integer join key). |
| `grades_hands_fumble` | Float64 | PFF ball-security (hands/fumble) grade, 0-100. |
| `grades_offense` | Float64 | PFF overall offense grade (0-100). |
| `grades_offense_penalty` | Float64 | PFF offensive penalty grade, 0-100. |
| `grades_pass` | Float64 | PFF passing grade (0-100). |
| `grades_run` | Float64 | PFF rushing grade (0-100). |
| `grades_run_block` | Float64 | PFF run-blocking grade (0-100). |
| `home_franchise_id` | Int64 | PFF franchise id of the home team. |
| `home_team_name` | String | Abbreviation of the home team (e.g. "NE"). |
| `jersey_number` | String | Jersey number the player wore in the game (string; zero-padded, e.g. "09"). |
| `penalties` | Int64 | Penalties charged to the player over the covered span. |
| `player_franchise_id` | Int64 | PFF franchise id of the team the player played for in the game. |
| `player_id` | Int64 | PFF player id (integer join key) of the player the report is about. |
| `position` | String | PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `snap_counts_pass` | Int64 | Pass-play snaps spent as the passer, rather than blocking or running a route. |
| `snap_counts_pass_block` | Int64 | Pass-blocking snaps played. |
| `snap_counts_pass_route` | Int64 | Snaps spent running a pass route. |
| `snap_counts_run` | Int64 | Run-play snaps spent as a runner, rather than run blocking. |
| `snap_counts_run_block` | Int64 | Run-blocking snaps played. |
| `snap_counts_total` | Int64 | Total offensive snaps played. |
| `snap_counts_total_pass` | Int64 | Total pass-play snaps across passing, pass blocking, and route running. |
| `snap_counts_total_run` | Int64 | Total run-play snaps across rushing and run blocking. |
| `status` | String | "S" when the player started the game; PFF owns the value set. |
| `week` | Int64 | Week number of the game, as PFF numbers weeks. |
| `grades_pass_route` | Float64 | PFF receiving/route grade (0-100). |
| `game_away_franchise_id` | Int64 | Repeats away_franchise_id from the row's nested game object: PFF franchise id of the away team. |
| `game_away_team_name` | String | Repeats away_team_name from the row's nested game object: Abbreviation of the away team (e.g. "LV"). |
| `game_game_id` | Int64 | Repeats game_id from the row's nested game object: PFF game id of the game (integer join key). |
| `game_home_franchise_id` | Int64 | Repeats home_franchise_id from the row's nested game object: PFF franchise id of the home team. |
| `game_home_team_name` | String | Repeats home_team_name from the row's nested game object: Abbreviation of the home team (e.g. "NE"). |
| `game_player_franchise_id` | Int64 | Repeats player_franchise_id from the row's nested game object: PFF franchise id of the team the player played for in the game. |
| `game_position` | String | Repeats position from the row's nested game object: PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `game_status` | String | Repeats status from the row's nested game object: "S" when the player started the game; PFF owns the value set. |
| `game_week` | Int64 | Repeats week from the row's nested game object: Week number of the game, as PFF numbers weeks. |
| `league_id` | Int64 | PFF league id (1 = NFL, 2 = NCAA), filled from the report's subject. |
| `season` | Int64 | Season (starting year) of the report, filled from the report's subject. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_player_offense_summary-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_player_offense_summary-example}

```python
pff_api_player_offense_summary(league='nfl', player_id=28022, season=2022)
```

## pff_api_player_offense_blocking

Blocking report for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/offense/blocking`

**Valid URL:** [https://api.pff.com/v1/player/offense/blocking?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/offense/blocking?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns {#pff_api_player_offense_blocking-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `grades_pass_block` | Float64 | PFF pass-blocking grade, 0-100. |
| `grades_offense` | Float64 | PFF overall offense grade, 0-100. |
| `game_id` | Int64 | PFF game id of the game (integer join key). |
| `pbe` | Float64 | PFF Pass Blocking Efficiency rating, pressures allowed per pass-blocking snap weighted toward sacks. |
| `non_spike_pass_block_percentage` | Float64 | Share of non-spike pass-play snaps spent pass blocking. |
| `week` | Int64 | Week number of the game, as PFF numbers weeks. |
| `snap_counts_rg` | Int64 | Snaps aligned at right guard. |
| `status` | String | "S" when the player started the game; PFF owns the value set. |
| `jersey_number` | String | Jersey number the player wore in the game (string; zero-padded, e.g. "09"). |
| `snap_counts_ce` | Int64 | Snaps aligned at center. |
| `hits_allowed` | Int64 | Quarterback hits allowed. |
| `block_percent` | Float64 | Share of offensive snaps spent blocking. |
| `snap_counts_offense` | Int64 | Offensive snaps played. |
| `away_team_name` | String | Abbreviation of the away team (e.g. "LV"). |
| `snap_counts_block` | Int64 | Total blocking snaps played. |
| `hurries_allowed` | Int64 | Quarterback hurries allowed. |
| `player_franchise_id` | Int64 | PFF franchise id of the team the player played for in the game. |
| `grades_run_block` | Float64 | PFF run-blocking grade, 0-100. |
| `snap_counts_run_block` | Int64 | Run-blocking snaps played. |
| `pressures_allowed` | Int64 | Total pressures allowed (sacks, hits, and hurries). |
| `snap_counts_pass_play` | Int64 | Pass-play snaps. |
| `penalties` | Int64 | Penalties charged to the player over the covered span. |
| `sacks_allowed` | Int64 | Sacks allowed by the player in pass protection. |
| `declined_penalties` | Int64 | Penalties committed by the player that were declined. |
| `snap_counts_te` | Int64 | Snaps aligned at tight end. |
| `snap_counts_rt` | Int64 | Snaps aligned at right tackle. |
| `position` | String | PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `away_franchise_id` | Int64 | PFF franchise id of the away team. |
| `non_spike_pass_block` | Int64 | Pass-blocking snaps excluding spike plays. |
| `snap_counts_lt` | Int64 | Snaps aligned at left tackle. |
| `snap_counts_pass_block` | Int64 | Pass-blocking snaps played. |
| `pass_block_percent` | Float64 | Share of pass-play snaps spent pass blocking. |
| `home_franchise_id` | Int64 | PFF franchise id of the home team. |
| `home_team_name` | String | Abbreviation of the home team (e.g. "NE"). |
| `snap_counts_lg` | Int64 | Snaps aligned at left guard. |
| `non_spike_passing` | Int64 | Non-spike pass-play snaps, the denominator of non_spike_pass_block_percentage. |
| `player_id` | Int64 | PFF player id (integer join key) of the player the report is about. |
| `game_away_franchise_id` | Int64 | Repeats away_franchise_id from the row's nested game object: PFF franchise id of the away team. |
| `game_away_team_name` | String | Repeats away_team_name from the row's nested game object: Abbreviation of the away team (e.g. "LV"). |
| `game_game_id` | Int64 | Repeats game_id from the row's nested game object: PFF game id of the game (integer join key). |
| `game_home_franchise_id` | Int64 | Repeats home_franchise_id from the row's nested game object: PFF franchise id of the home team. |
| `game_home_team_name` | String | Repeats home_team_name from the row's nested game object: Abbreviation of the home team (e.g. "NE"). |
| `game_player_franchise_id` | Int64 | Repeats player_franchise_id from the row's nested game object: PFF franchise id of the team the player played for in the game. |
| `game_position` | String | Repeats position from the row's nested game object: PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `game_status` | String | Repeats status from the row's nested game object: "S" when the player started the game; PFF owns the value set. |
| `game_week` | Int64 | Repeats week from the row's nested game object: Week number of the game, as PFF numbers weeks. |
| `league_id` | Int64 | PFF league id (1 = NFL, 2 = NCAA), filled from the report's subject. |
| `season` | Int64 | Season (starting year) of the report, filled from the report's subject. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_player_offense_blocking-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_player_offense_blocking-example}

```python
pff_api_player_offense_blocking(league='nfl', player_id=28022, season=2022)
```

## pff_api_player_offense_pass_blocking

Pass-blocking report for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/offense/pass_blocking`

**Valid URL:** [https://api.pff.com/v1/player/offense/pass_blocking?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/offense/pass_blocking?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns {#pff_api_player_offense_pass_blocking-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `true_pass_set_non_spike_pass_block_percentage` | Float64 | Share of non-spike pass-play snaps spent pass blocking on PFF-designated true pass sets. |
| `true_pass_set_pressures_allowed` | Int64 | Total pressures allowed (sacks, hits, and hurries) on PFF-designated true pass sets. |
| `grades_pass_block` | Float64 | PFF pass-blocking grade, 0-100. |
| `true_pass_set_pass_block_percent` | Float64 | Share of pass-play snaps spent pass blocking on PFF-designated true pass sets. |
| `game_id` | Int64 | PFF game id of the game (integer join key). |
| `pbe` | Float64 | PFF Pass Blocking Efficiency rating, pressures allowed per pass-blocking snap weighted toward sacks. |
| `non_spike_pass_block_percentage` | Float64 | Share of non-spike pass-play snaps spent pass blocking. |
| `week` | Int64 | Week number of the game, as PFF numbers weeks. |
| `status` | String | "S" when the player started the game; PFF owns the value set. |
| `jersey_number` | String | Jersey number the player wore in the game (string; zero-padded, e.g. "09"). |
| `true_pass_set_non_spike_passing` | Int64 | Non-spike pass-play snaps on PFF-designated true pass sets, the denominator of true_pass_set_non_spike_pass_block_percentage. |
| `hits_allowed` | Int64 | Quarterback hits allowed. |
| `true_pass_set_non_spike_pass_block` | Int64 | Pass-blocking snaps excluding spike plays on PFF-designated true pass sets. |
| `away_team_name` | String | Abbreviation of the away team (e.g. "LV"). |
| `hurries_allowed` | Int64 | Quarterback hurries allowed. |
| `true_pass_set_hurries_allowed` | Int64 | Quarterback hurries allowed on PFF-designated true pass sets. |
| `player_franchise_id` | Int64 | PFF franchise id of the team the player played for in the game. |
| `true_pass_set_snap_counts_pass_play` | Int64 | Pass-play snaps on PFF-designated true pass sets. |
| `true_pass_set_hits_allowed` | Int64 | Quarterback hits allowed on PFF-designated true pass sets. |
| `pressures_allowed` | Int64 | Total pressures allowed (sacks, hits, and hurries). |
| `true_pass_set_pbe` | Float64 | PFF Pass Blocking Efficiency rating, pressures allowed per pass-blocking snap weighted toward sacks on PFF-designated true pass sets. |
| `snap_counts_pass_play` | Int64 | Pass-play snaps. |
| `penalties` | Int64 | Penalties charged to the player over the covered span. |
| `sacks_allowed` | Int64 | Sacks allowed by the player in pass protection. |
| `declined_penalties` | Int64 | Penalties committed by the player that were declined. |
| `position` | String | PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `away_franchise_id` | Int64 | PFF franchise id of the away team. |
| `non_spike_pass_block` | Int64 | Pass-blocking snaps excluding spike plays. |
| `true_pass_set_snap_counts_pass_block` | Int64 | Pass-blocking snaps played on PFF-designated true pass sets. |
| `true_pass_set_sacks_allowed` | Int64 | Sacks allowed on PFF-designated true pass sets. |
| `snap_counts_pass_block` | Int64 | Pass-blocking snaps played. |
| `pass_block_percent` | Float64 | Share of pass-play snaps spent pass blocking. |
| `home_franchise_id` | Int64 | PFF franchise id of the home team. |
| `home_team_name` | String | Abbreviation of the home team (e.g. "NE"). |
| `non_spike_passing` | Int64 | Non-spike pass-play snaps, the denominator of non_spike_pass_block_percentage. |
| `player_id` | Int64 | PFF player id (integer join key) of the player the report is about. |
| `game_away_franchise_id` | Int64 | Repeats away_franchise_id from the row's nested game object: PFF franchise id of the away team. |
| `game_away_team_name` | String | Repeats away_team_name from the row's nested game object: Abbreviation of the away team (e.g. "LV"). |
| `game_game_id` | Int64 | Repeats game_id from the row's nested game object: PFF game id of the game (integer join key). |
| `game_home_franchise_id` | Int64 | Repeats home_franchise_id from the row's nested game object: PFF franchise id of the home team. |
| `game_home_team_name` | String | Repeats home_team_name from the row's nested game object: Abbreviation of the home team (e.g. "NE"). |
| `game_player_franchise_id` | Int64 | Repeats player_franchise_id from the row's nested game object: PFF franchise id of the team the player played for in the game. |
| `game_position` | String | Repeats position from the row's nested game object: PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `game_status` | String | Repeats status from the row's nested game object: "S" when the player started the game; PFF owns the value set. |
| `game_week` | Int64 | Repeats week from the row's nested game object: Week number of the game, as PFF numbers weeks. |
| `league_id` | Int64 | PFF league id (1 = NFL, 2 = NCAA), filled from the report's subject. |
| `season` | Int64 | Season (starting year) of the report, filled from the report's subject. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_player_offense_pass_blocking-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_player_offense_pass_blocking-example}

```python
pff_api_player_offense_pass_blocking(league='nfl', player_id=28022, season=2022)
```

## pff_api_player_offense_run_blocking

Run-blocking report for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/offense/run_blocking`

**Valid URL:** [https://api.pff.com/v1/player/offense/run_blocking?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/offense/run_blocking?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns {#pff_api_player_offense_run_blocking-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `away_franchise_id` | Int64 | PFF franchise id of the away team. |
| `away_team_name` | String | Abbreviation of the away team (e.g. "LV"). |
| `declined_penalties` | Int64 | Penalties committed by the player that were declined. |
| `game_id` | Int64 | PFF game id of the game (integer join key). |
| `gap_run_block_percent` | Float64 | Share of run-play snaps spent run blocking on gap-scheme runs. |
| `gap_snap_counts_run_block` | Int64 | Run-blocking snaps played on gap-scheme runs. |
| `gap_snap_counts_run_block_percent` | Float64 | Share of the player's run-blocking snaps on gap-scheme runs. |
| `gap_snap_counts_run_play` | Int64 | Run-play snaps on gap-scheme runs. |
| `grades_run_block` | Float64 | PFF run-blocking grade, 0-100. |
| `home_franchise_id` | Int64 | PFF franchise id of the home team. |
| `home_team_name` | String | Abbreviation of the home team (e.g. "NE"). |
| `jersey_number` | String | Jersey number the player wore in the game (string; zero-padded, e.g. "09"). |
| `penalties` | Int64 | Penalties charged to the player over the covered span. |
| `player_franchise_id` | Int64 | PFF franchise id of the team the player played for in the game. |
| `player_id` | Int64 | PFF player id (integer join key) of the player the report is about. |
| `position` | String | PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `run_block_percent` | Float64 | Share of run-play snaps spent run blocking. |
| `snap_counts_run_block` | Int64 | Run-blocking snaps played. |
| `snap_counts_run_play` | Int64 | Run-play snaps. |
| `status` | String | "S" when the player started the game; PFF owns the value set. |
| `week` | Int64 | Week number of the game, as PFF numbers weeks. |
| `zone_run_block_percent` | Float64 | Share of run-play snaps spent run blocking on zone-scheme runs. |
| `zone_snap_counts_run_block` | Int64 | Run-blocking snaps played on zone-scheme runs. |
| `zone_snap_counts_run_block_percent` | Float64 | Share of the player's run-blocking snaps on zone-scheme runs. |
| `zone_snap_counts_run_play` | Int64 | Run-play snaps on zone-scheme runs. |
| `game_away_franchise_id` | Int64 | Repeats away_franchise_id from the row's nested game object: PFF franchise id of the away team. |
| `game_away_team_name` | String | Repeats away_team_name from the row's nested game object: Abbreviation of the away team (e.g. "LV"). |
| `game_game_id` | Int64 | Repeats game_id from the row's nested game object: PFF game id of the game (integer join key). |
| `game_home_franchise_id` | Int64 | Repeats home_franchise_id from the row's nested game object: PFF franchise id of the home team. |
| `game_home_team_name` | String | Repeats home_team_name from the row's nested game object: Abbreviation of the home team (e.g. "NE"). |
| `game_player_franchise_id` | Int64 | Repeats player_franchise_id from the row's nested game object: PFF franchise id of the team the player played for in the game. |
| `game_position` | String | Repeats position from the row's nested game object: PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `game_status` | String | Repeats status from the row's nested game object: "S" when the player started the game; PFF owns the value set. |
| `game_week` | Int64 | Repeats week from the row's nested game object: Week number of the game, as PFF numbers weeks. |
| `league_id` | Int64 | PFF league id (1 = NFL, 2 = NCAA), filled from the report's subject. |
| `season` | Int64 | Season (starting year) of the report, filled from the report's subject. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_player_offense_run_blocking-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_player_offense_run_blocking-example}

```python
pff_api_player_offense_run_blocking(league='nfl', player_id=28022, season=2022)
```

## pff_api_player_passing_summary

Passing summary for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/passing/summary`

**Valid URL:** [https://api.pff.com/v1/player/passing/summary?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/passing/summary?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns {#pff_api_player_passing_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `grades_offense` | Float64 | PFF overall offense grade (0-100). |
| `twp_rate` | Float64 | Turnover-worthy-play rate. |
| `game_id` | Int64 | PFF game id of the game (integer join key). |
| `btt_rate` | Float64 | Big-time-throw rate. |
| `spikes` | Int64 | Clock-stopping spike plays. |
| `dropbacks` | Int64 | Total quarterback dropbacks. |
| `all_attempts` | Int64 | All pass attempts on the player's passing snaps; attempts counts a subset of these. |
| `thrown_aways` | Int64 | Passes intentionally thrown away. |
| `week` | Int64 | Week number of the game, as PFF numbers weeks. |
| `status` | String | "S" when the player started the game; PFF owns the value set. |
| `all_dropbacks` | Int64 | All dropbacks, equal to passing_snaps; dropbacks counts a subset of these. |
| `grades_pass` | Float64 | PFF passing grade (0-100). |
| `ttt_total_time` | Float64 | Time to throw summed over the passer's dropbacks, in seconds (avg_time_to_throw = ttt_total_time / dropbacks). |
| `hit_as_threw` | Int64 | Plays where the quarterback was hit as he threw. |
| `first_downs` | Int64 | Passing first downs. |
| `jersey_number` | String | Jersey number the player wore in the game (string; zero-padded, e.g. "09"). |
| `sack_percent` | Float64 | Sack rate (sacks per dropback). |
| `bats` | Int64 | Passes batted at the line. |
| `away_team_name` | String | Abbreviation of the away team (e.g. "LV"). |
| `sacks` | Int64 | Times the passer was sacked. |
| `completions` | Int64 | Completed passes by the passer. |
| `yards` | Int64 | Total passing yards gained. |
| `player_franchise_id` | Int64 | PFF franchise id of the team the player played for in the game. |
| `accuracy_percent` | Float64 | Charted accuracy percentage. |
| `scrambles` | Int64 | Scramble plays. |
| `interceptions` | Int64 | Interceptions thrown. |
| `positive_epa_plays` | Int64 | Plays with positive expected points added (positive_epa_percent = positive_epa_plays / plays_with_epa x 100). |
| `drop_rate` | Float64 | Receiver drop rate on the quarterback's throws. |
| `grades_run` | Float64 | PFF rushing grade (0-100). |
| `qb_rating` | Float64 | NFL passer rating. |
| `completion_percent` | Float64 | Completion percentage. |
| `plays_with_epa` | Int64 | Plays with an expected-points-added value, the denominator of positive_epa_percent. |
| `penalties` | Int64 | Penalties charged. |
| `attempts` | Int64 | Pass attempts thrown by the passer. |
| `declined_penalties` | Int64 | Declined penalties. |
| `passing_snaps` | Int64 | Number of passing snaps played. |
| `pressure_to_sack_rate` | Float64 | Percentage of pressured dropbacks that ended in a sack. |
| `ypa` | Float64 | Yards gained per pass attempt. |
| `drops` | Int64 | Passes dropped by the passer's receivers. |
| `position` | String | PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `grades_hands_fumble` | Float64 | PFF hands (fumble) grade for the player, reflecting ball security (0-100). |
| `avg_time_to_throw` | Float64 | Average time to throw per dropback, in seconds from snap to release (ttt_total_time / dropbacks). |
| `away_franchise_id` | Int64 | PFF franchise id of the away team. |
| `big_time_throws` | Int64 | Number of big-time throws, per PFF's highest-value, highest-difficulty throw designation. |
| `positive_epa_percent` | Float64 | Percentage of dropbacks with positive expected points added. |
| `home_franchise_id` | Int64 | PFF franchise id of the home team. |
| `home_team_name` | String | Abbreviation of the home team (e.g. "NE"). |
| `avg_depth_of_target` | Float64 | Average depth of target in air yards. |
| `turnover_worthy_plays` | Int64 | Number of turnover-worthy plays, plays PFF charts as deserving of a turnover. |
| `epa` | Float64 | Expected points added per play on the passer's dropbacks, as computed by PFF (an average such as 0.14, not a total). |
| `aimed_passes` | Int64 | Aimed passes: attempts excluding throwaways, spikes, batted passes and throws made while hit (the accuracy_percent denominator). |
| `player_id` | Int64 | PFF player id (integer join key) of the player the report is about. |
| `touchdowns` | Int64 | Number of passing touchdowns thrown. |
| `def_gen_pressures` | Int64 | Number of defense-generated pressures on the player's dropbacks, as charted by PFF. |
| `game_away_franchise_id` | Int64 | Repeats away_franchise_id from the row's nested game object: PFF franchise id of the away team. |
| `game_away_team_name` | String | Repeats away_team_name from the row's nested game object: Abbreviation of the away team (e.g. "LV"). |
| `game_game_id` | Int64 | Repeats game_id from the row's nested game object: PFF game id of the game (integer join key). |
| `game_home_franchise_id` | Int64 | Repeats home_franchise_id from the row's nested game object: PFF franchise id of the home team. |
| `game_home_team_name` | String | Repeats home_team_name from the row's nested game object: Abbreviation of the home team (e.g. "NE"). |
| `game_player_franchise_id` | Int64 | Repeats player_franchise_id from the row's nested game object: PFF franchise id of the team the player played for in the game. |
| `game_position` | String | Repeats position from the row's nested game object: PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `game_status` | String | Repeats status from the row's nested game object: "S" when the player started the game; PFF owns the value set. |
| `game_week` | Int64 | Repeats week from the row's nested game object: Week number of the game, as PFF numbers weeks. |
| `league_id` | Int64 | PFF league id (1 = NFL, 2 = NCAA), filled from the report's subject. |
| `season` | Int64 | Season (starting year) of the report, filled from the report's subject. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_player_passing_summary-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_player_passing_summary-example}

```python
pff_api_player_passing_summary(league='nfl', player_id=28022, season=2022)
```

## pff_api_player_passing_concept

Passing by play concept for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/passing/concept`

**Valid URL:** [https://api.pff.com/v1/player/passing/concept?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/passing/concept?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, as a four-digit year, taken as a positional argument. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns {#pff_api_player_passing_concept-returns}

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

### Raises {#pff_api_player_passing_concept-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_player_passing_concept-example}

```python
pff_api_player_passing_concept(league='nfl', player_id=28022, season=2022)
```
