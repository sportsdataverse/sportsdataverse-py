---
title: "NBA — additional Python functions — Play-by-play processing"
sidebar_label: "Play-by-play processing"
sidebar_position: 10
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

### espn_nba_game_all {#espn_nba_game_all}

`espn_nba_game_all(game_id: 'int', return_as_pandas: 'bool' = False, **kwargs)`

espn_nba_game_all() - Pull one game's plays, team box and player box from one ESPN request.

The hoopR name for this request (`hoopR::espn_nba_game_all`): a single `nba/summary`
fetch, returned as the three frames `espn_nba_pbp`, `espn_nba_team_box` and
`espn_nba_player_box` would return one request each.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from `espn_nba_schedule()`. |
| `return_as_pandas` | `bool` | `False` | If True, the three frames are pandas instead of polars. |

**Returns**

hoopR's list names as keys -- `"Plays"` (the `load_nba_pbp` release frame), `"Team"` (the team box) and `"Player"` (the player box).

**Example**

```python
from sportsdataverse.nba import espn_nba_game_all
game = espn_nba_game_all(game_id=401585183)
print(game["Plays"].shape, game["Team"].shape, game["Player"].shape)

# Pipeline next step (one line)

game["Player"].sort("points", descending=True).select("athlete_display_name", "points").head()
```

### espn_nba_player_box {#espn_nba_player_box}

`espn_nba_player_box(game_id: 'int', return_as_pandas: 'bool' = False, **kwargs)`

espn_nba_player_box() - Pull one game's player box score from ESPN's `nba/summary` endpoint.

The hoopR name for this request (`hoopR::espn_nba_player_box`): one `summary` fetch run
through `~sportsdataverse.nba.helper_nba_player_box`, so the frame is the one the
`espn_nba_player_boxscores` release publishes (what
`~sportsdataverse.nba.load_nba_player_boxscore` reads) for that game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from `espn_nba_schedule()`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas frame instead of polars. |

**Returns**

One row per athlete (did-not-play athletes included, with null stats), away team first, with the release's player-box columns. A frame with no columns when ESPN has no box score for the game.

**Example**

```python
from sportsdataverse.nba import espn_nba_player_box
player_box = espn_nba_player_box(game_id=401585183)
print(player_box.shape)

# Pipeline next step (one line)

player_box.filter(pl.col("did_not_play") == False).select("athlete_display_name", "points")
```

### espn_nba_team_box {#espn_nba_team_box}

`espn_nba_team_box(game_id: 'int', return_as_pandas: 'bool' = False, **kwargs)`

espn_nba_team_box() - Pull one game's team box score from ESPN's `nba/summary` endpoint.

The hoopR name for this request (`hoopR::espn_nba_team_box`): one `summary` fetch run
through `~sportsdataverse.nba.helper_nba_team_box`, so the frame is the one the
`espn_nba_team_boxscores` release publishes (what
`~sportsdataverse.nba.load_nba_team_boxscore` reads) for that game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from `espn_nba_schedule()`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas frame instead of polars. |

**Returns**

Two rows, one per team, with the release's team-box columns. A frame with no columns when ESPN has no box score for the game.

**Example**

```python
from sportsdataverse.nba import espn_nba_team_box
team_box = espn_nba_team_box(game_id=401585183)
print(team_box.shape)

# Pipeline next step (one line)

team_box.select("team_display_name", "team_score", "team_winner")
```

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
   (`_build_roster`) and recover `player2_id`/`player3_id`
   (assist/block/steal/sub-in/jump) from `pbp_v3` (
   `_extract_secondary_players`).
2. Drop the standalone block/steal rows consolidated into their parent
   Missed Shot / Turnover (`_is_dropped_block_steal`) -- the only
   row-count change versus the raw v3 action list.
3. Derive `event_type`/`event_action_type` from the module's lookup
   tables, split `description` by `location` into home/visitor/
   neutral, forward-fill the running score, and enrich `player2`/
   `player3` from the roster **by id** (see `_secondary_fields`
   for the deliberate divergence from hoopR's name-based re-resolution).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_v3` | `dict` |  | Raw `playbyplayv3` dict (`nba_stats_playbyplayv3` / `wnba_stats_playbyplayv3` payload shape); actions live at `pbp_v3["game"]["actions"]`. |
| `box_v3` | `dict` |  | Raw `boxscoretraditionalv3` dict, passed through to `_build_roster`. |
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame` instead of `polars.DataFrame`. |

**Returns**

Polars (or pandas) DataFrame with the full v2 schema (game/event identifiers, event/action type codes, home/visitor/neutral descriptions, forward-filled score + margin + leader, per-player columns for players 1-3, and the v3 passthrough columns). Empty or malformed input returns a zero-row frame with the same schema (never raises). Malformed or empty input returns a zero-row frame with the documented schema instead of raising.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `event_num` | String | Sequential event number within the game (V2 PBP). |
| `event_type` | String | Event / play type code (V2 PBP). |
| `event_action_type` | String | Numeric event-action-type code (V2 PBP). |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `clock` | String | Game clock value. |
| `minute_game` | Float64 | Minute game. |
| `time_remaining` | Float64 | Time remaining. |
| `wc_time_string` | String | Wc time string. |
| `time_quarter` | String | Time quarter. |
| `minute_remaining_quarter` | Int64 | Minute remaining quarter. |
| `seconds_remaining_quarter` | Int64 | Seconds remaining quarter. |
| `action_type` | String | Action type label (e.g. 'Made Shot', 'Substitution'). |
| `sub_type` | String | Action sub-type label. |
| `home_description` | String | Home team's description. |
| `neutral_description` | String | Neutral description. |
| `visitor_description` | String | Visitor description. |
| `description` | String | Long-form description text. |
| `location` | String | Location. |
| `score` | String | Final score. |
| `away_score` | Int64 | Away team score at the time of the play. |
| `home_score` | Int64 | Home team score at the time of the play. |
| `score_margin` | String | Score margin. |
| `team_leading` | String | Team leading. |
| `person1type` | String | Person1type. |
| `player1_id` | String | V2 PBP primary player ID (e.g. shooter / fouler). |
| `player1_name` | String | V2 PBP primary player name. |
| `player1_team_id` | String | Team ID of player1. |
| `player1_team_city` | String | Player1 team city. |
| `player1_team_nickname` | String | Player1 team nickname. |
| `player1_team_abbreviation` | String | Player1 team abbreviation. |
| `person2type` | String | Person2type. |
| `player2_id` | String | V2 PBP secondary player ID (e.g. assister / fouled-by). |
| `player2_name` | String | V2 PBP secondary player name. |
| `player2_team_id` | String | Team ID of player2. |
| `player2_team_city` | String | Player2 team city. |
| `player2_team_nickname` | String | Player2 team nickname. |
| `player2_team_abbreviation` | String | Player2 team abbreviation. |
| `person3type` | String | Person3type. |
| `player3_id` | String | V2 PBP tertiary player ID (e.g. blocker). |
| `player3_name` | String | V2 PBP tertiary player name. |
| `player3_team_id` | String | Team ID of player3. |
| `player3_team_city` | String | Player3 team city. |
| `player3_team_nickname` | String | Player3 team nickname. |
| `player3_team_abbreviation` | String | Player3 team abbreviation. |
| `video_available_flag` | String | Video available flag. |
| `x_legacy` | Int64 | V2-format X coordinate (preserved for V3-to-V2 compatibility). |
| `y_legacy` | Int64 | V2-format Y coordinate (preserved for V3-to-V2 compatibility). |
| `shot_distance` | Int64 | Shot distance from the basket, in feet. |
| `shot_result` | String | Shot result ('Made' / 'Missed'). |
| `is_field_goal` | Int64 | 1 if the action was a field goal; 0 otherwise. |
| `points_total` | Int64 | Running total of points scored. |
| `shot_value` | Int64 | Point value of the shot (2 or 3). |
| `action_number` | Int64 | Sequential action number within a game (V3 PBP). |
| `team_id` | Int64 | Unique team identifier. |
| `team_tricode` | String | Three-letter team code (e.g. 'LAS' / 'NYL'). |
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `player_name` | String | Player name. |
| `score_home` | String | Score home. |
| `score_away` | String | Score away. |
| `action_id` | Int64 | Unique action identifier within a game (V3 PBP). |

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
