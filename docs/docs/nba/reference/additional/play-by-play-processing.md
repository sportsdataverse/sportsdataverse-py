---
title: "NBA — additional Python functions — Play-by-play processing"
sidebar_label: "Play-by-play processing"
sidebar_position: 9
description: "NBA — additional Python functions — Play-by-play processing — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Play-by-play processing

### build_athlete_identity_lookup {#build_athlete_identity_lookup}

`build_athlete_identity_lookup(rosters: 'dict[int | str, dict]') -> 'dict[str, dict[str, Any]]'`

R `build_athlete_identity_lookup`: athlete_id -> identity from team rosters.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `rosters` | `dict[int \| str, dict]` |  | Mapping of team_id -> that team's raw roster payload (`wbb/team_rosters/json/{season}/{team_id}.json`). NOTE: R walks `raw$athletes` directly here (no position-bucket unwrap, unlike the rosters dataset itself). |

**Returns**

athlete_id (str) -> identity fields for `helper_wbb_player_season_stats`.

### build_nba_player_identity_lookup {#build_nba_player_identity_lookup}

`build_nba_player_identity_lookup(player_box: 'pl.DataFrame') -> 'dict[str, dict[str, Any]]'`

R `build_identity_lookup(season)`: athlete_id -> identity from the

season's already-compiled `player_box` -- the authoritative "who played
in season Y" source (ESPN's team-roster endpoint is current-only and
cannot answer that for historical seasons).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_box` | `DataFrame` |  | The season's compiled player_box frame (e.g. `nba/player_box/parquet/player_box_{season}.parquet`, or whatever the season builder just wrote for this pass). Must carry `athlete_id`; other identity columns are best-effort. |

**Returns**

athlete_id (str) -> identity fields for `helper_nba_player_season_stats`. When an athlete appears in multiple rows (multiple games), the LAST row (by frame order) wins -- mirroring R's `!duplicated(athlete_id, fromLast = TRUE)`, which keeps an athlete's most recent team within the season.

### nba_pbp_disk {#nba_pbp_disk}

`nba_pbp_disk(game_id, path_to_json)`

Load a previously cached ESPN NBA summary JSON for a game from disk.

Reads `{path_to_json}/{game_id}.json`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | ESPN game / event identifier. |
| `path_to_json` | `str` |  | Directory containing the cached JSON file. |

**Returns**

Parsed JSON contents.

**Example**

```python
from sportsdataverse.nba import nba_pbp_disk
pbp = nba_pbp_disk(game_id=401585183, path_to_json="./cache")
print(list(pbp.keys()))
```

### nba_v3_to_v2_pbp {#nba_v3_to_v2_pbp}

`nba_v3_to_v2_pbp(pbp_v3: 'dict', box_v3: 'dict', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Convert a v3 `playbyplayv3` payload into the full v2-schema pbp frame.

Ports hoopR's `.v3_to_v2_format()` (`R/nba_stats_pbp.R` lines
210-810) to polars: the v3 feed (`stats.nba.com` `playbyplayv3`) is
reshaped into the older v2 schema that the committed hoopR-nba-stats-data
dataset carries and that `pbpstats`' `stats_nba` provider consumes.
This is a pure, network-free function -- both payloads must already be
fetched (e.g. via `nba_stats_playbyplayv3` / `nba_stats_boxscoretraditionalv3`).

Pipeline:

1. Build the per-`person_id` roster from `box_v3`
   (build_roster`) and recover `player2_id`/`player3_id`
   (assist/block/steal/sub-in/jump) from `pbp_v3` (
   extract_secondary_players`).
2. Drop the standalone block/steal rows consolidated into their parent
   Missed Shot / Turnover (is_dropped_block_steal`) -- the only
   row-count change versus the raw v3 action list.
3. Derive `event_type`/`event_action_type` from the module's lookup
   tables, split `description` by `location` into home/visitor/
   neutral, forward-fill the running score, and enrich `player2`/
   `player3` from the roster **by id** (see secondary_fields`
   for the deliberate divergence from hoopR's name-based re-resolution).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_v3` | `dict` |  | Raw `playbyplayv3` dict (`nba_stats_playbyplayv3` / `wnba_stats_playbyplayv3` payload shape); actions live at `pbp_v3["game"]["actions"]`. |
| `box_v3` | `dict` |  | Raw `boxscoretraditionalv3` dict, passed through to build_roster`. |
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame` instead of `polars.DataFrame`. |

**Returns**

Polars (or pandas) DataFrame with the full v2 schema (game/event identifiers, event/action type codes, home/visitor/neutral descriptions, forward-filled score + margin + leader, per-player columns for players 1-3, and the v3 passthrough columns). Empty or malformed input returns a zero-row frame with the same schema (never raises).

**Example**

```python
from sportsdataverse.nba.nba_v3_v2_adapter import nba_v3_to_v2_pbp
from sportsdataverse.nba.nba_stats import nba_stats_playbyplayv3, nba_stats_boxscoretraditionalv3

pbp_v3 = nba_stats_playbyplayv3(game_id="0022300001", return_parsed=False)
box_v3 = nba_stats_boxscoretraditionalv3(game_id="0022300001", return_parsed=False)
df = nba_v3_to_v2_pbp(pbp_v3, box_v3)
print(df.shape, df.columns)

# Pandas output

df_pd = nba_v3_to_v2_pbp(pbp_v3, box_v3, return_as_pandas=True)
print(type(df_pd))

# Pipeline next step (feed a pbpstats-style consumer)

df.filter(pl.col("event_type") == "1").select("player1_name", "player2_name")
```
