---
title: "NBA — additional Python functions — Analytics: team_pace–zone_value"
sidebar_label: "Analytics: team_pace–zone_value"
sidebar_position: 20
description: "NBA — additional Python functions — Analytics: team_pace–zone_value — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Analytics: team_pace–zone_value

### team_pace_projection {#team_pace_projection}

`team_pace_projection(home_team_id: 'str', away_team_id: 'str', ratings: 'pl.DataFrame', *, league_id: 'str' = '00') -> 'float'`

Expected possessions for a matchup (Phase-3 `expected_possessions`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_team_id` | `str` |  | Home team id (matched against `ratings['team_id']`). |
| `away_team_id` | `str` |  | Away team id. |
| `ratings` | `DataFrame` |  | One row per team with `team_id, adj_pace`. |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"`. |

**Returns**

Expected possessions for the game.

**Example**

```python
from sportsdataverse.nba.nba_player_props import team_pace_projection
poss = team_pace_projection("1", "2", ratings)
```

### team_play_context {#team_play_context}

`team_play_context(possessions: 'pl.DataFrame', *, league_non_transition_ppp: 'Optional[float]' = None, apply_ctg_filters: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Roll possessions up into CTG's team Play-Context table.

Reproduces the offensive half of CTG's `/stats/league/context` page.

Columns: `poss`, `points`, `pts_per_100`, `transition_poss`,
`transition_points`, `transition_freq`, `transition_pts_per_100`
(CTG's "Eff"), `non_transition_pts_per_100`, `transition_pts_added_per_100`
(CTG's "Pts+/Poss"), plus `halfcourt_*` twins and per-source transition
frequencies (`freq_off_steal` / `freq_off_live_rebound`).

**Pts+/Poss** is the subtle one. CTG: "CTG takes a team's points per
possession that starts with transition, and subtracts out **what an average
team does** in a possession that did not start with transition. ... We take the

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Frame from `add_play_context`. |
| `league_non_transition_ppp` | `Optional[float]` | `None` | League-average points per 100 possessions on non-transition-start possessions. Computed from the frame when omitted. |
| `apply_ctg_filters` | `bool` | `True` | Drop garbage-time, heave and non-counting possessions first (CTG's default view). Set `False` for the unfiltered totals. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per `offense_team_id`. Empty input returns a zero-row frame.

| col_name | type | description |
|---|---|---|
| `offense_team_id` | Int64 | Unique identifier for offense team. |
| `poss` | Int64 | Poss. |
| `points` | Int64 | Points scored. |
| `transition_poss` | Int64 |  |
| `transition_points` | Int64 |  |
| `halfcourt_poss` | Int64 |  |
| `pts_per_100` | Float64 |  |
| `transition_freq` | Float64 |  |
| `transition_pts_per_100` | Float64 |  |
| `non_transition_pts_per_100` | Float64 |  |
| `freq_off_steal` | Float64 |  |
| `freq_off_live_rebound` | Float64 |  |
| `transition_pts_added_per_100` | Float64 |  |
| `halfcourt_pts_per_100` | Float64 |  |

**Example**

```python
ctx = team_play_context(add_play_context(pbp))
print(ctx.select("offense_team_id", "transition_freq", "transition_pts_added_per_100"))

# Season-comparable Pts+/Poss

ctx = team_play_context(season_poss, league_non_transition_ppp=104.8)
```

### xpoints_baseline {#xpoints_baseline}

`xpoints_baseline(league_avgs: 'pl.DataFrame') -> 'pl.DataFrame'`

League-average FG% baseline table keyed by the three shot-zone columns.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league_avgs` | `DataFrame` |  | The `LeagueAverages` result set from `nba_stats_shotchartdetail` (`shot_zone_basic` / `shot_zone_area` / `shot_zone_range` / `fga` / `fgm` / `fg_pct`). |

**Returns**

One row per `(shot_zone_basic, shot_zone_area, shot_zone_range)`: `... base_fg_pct:Float64, is_three:Boolean` (`is_three` = the basic zone names a three). Empty input returns the zero-row schema.

| col_name | type | description |
|---|---|---|
| `shot_zone_basic` | String | Shot zone (e.g. 'Restricted Area', 'Mid-Range', 'Above the Break 3'). |
| `shot_zone_area` | String | Shot zone area ('Left Side', 'Right Side', 'Center'). |
| `shot_zone_range` | String | Shot zone range ('Less Than 8 ft.', '8-16 ft.', '16-24 ft.', etc.). |
| `base_fg_pct` | Float64 |  |
| `is_three` | Boolean |  |

**Example**

```python
from sportsdataverse.nba import nba_stats
from sportsdataverse.nba.nba_shot_value import xpoints_baseline
base = xpoints_baseline(league_avgs)
```

### zone_value_map {#zone_value_map}

`zone_value_map(scored_shots: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per-player per-zone value map: points and expected points per shot.

Collapses `shot_zone_basic` to a canonical zone via `ZONE_COLLAPSE`
(the two corner-3 zones merge) and aggregates realized vs expected points
per shot in each zone.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scored_shots` | `DataFrame` |  | `score_shot_xpoints` output (needs `player_id`, `shot_zone_basic`, `shot_made_flag`, `actual_points`, `xpoints`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per `(player_id, zone)`: `player_id:Int64, zone:Utf8, att:Int64, makes:Int64, pts:Float64, pps:Float64, xpps:Float64, pps_above_expected:Float64` (`pps` = points per shot, `xpps` = expected). Empty input returns the zero-row schema.

No returns table is published for this function: no capture: its input is stats.nba.com shot-chart detail (shot zones and types), which answers HTTP 403 to the datacenter IP the docs are built on and is not in the raw store.

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_shot_value import score_shot_xpoints, zone_value_map
zmap = zone_value_map(score_shot_xpoints(shots, league_avgs))

# Pipeline next step (one line)

zmap.filter(pl.col("zone") == "corner_3").sort("pps_above_expected", descending=True)
```
