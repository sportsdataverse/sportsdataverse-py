---
title: "WNBA — additional Python functions — Other"
sidebar_label: "Other"
sidebar_position: 5
description: "WNBA — additional Python functions — Other — function reference in sdv-py, the SportsDataverse Python package."
---
# WNBA — additional Python functions — Other

### load_wnba_stats_leaguedash {#load_wnba_stats_leaguedash}

`load_wnba_stats_leaguedash(family: 'str', seasons, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load one asset family of the `wnba_stats_leaguedash` release.

`wnba_stats_leaguedash` is a parameter cube: one asset per
(family, season) pair rather than one per season, so a family must be named.
The valid families are exported as `WNBA_STATS_LEAGUEDASH_FAMILIES` --
import that tuple to discover them rather than passing a bare string; an
unknown family raises `ValueError` listing every valid value. This is the
non-deprecated way to reach the cube; the four `load_wnba_stats_*` shims
below only reconstruct retired tags' stacked shapes from it.

Column sets are family-specific (a `lineups_*` frame keys on `group_id`,
a `player_*` frame on `player_id`), so this loader documents no fixed
returns table. `player_id` / `team_id` are `Int64` in every family and
season, so cross-family joins need no dtype reconciliation.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `family` | `str` |  | Asset family, e.g. `"player_stats_advanced"`. Must be one of `WNBA_STATS_LEAGUEDASH_FAMILIES`. |
| `seasons` | `int \| Iterable[int]` |  | Season, or iterable of seasons, to load. WNBA seasons are single calendar years. 1997 is the earliest season on the tag. A requested season the family does not publish is warned about and skipped, not an error. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe with one row per player / team / lineup per requested season for the requested family; an empty frame when no requested season is published.

**Example**

```python
from sportsdataverse.wnba import load_wnba_stats_leaguedash
adv = load_wnba_stats_leaguedash("player_stats_advanced", seasons=2025)
print(adv.shape)

# Discover the valid families

from sportsdataverse.wnba import WNBA_STATS_LEAGUEDASH_FAMILIES
print(WNBA_STATS_LEAGUEDASH_FAMILIES)

# Multi-season, pandas round-trip

team_pd = load_wnba_stats_leaguedash(
    "team_stats_base", seasons=range(2020, 2026), return_as_pandas=True
)

# Pipeline next step (best net rating in 2025)

import polars as pl
load_wnba_stats_leaguedash("team_stats_advanced", seasons=2025).sort(
    "net_rating", descending=True
).head()
```

### load_wnba_stats_lineups {#load_wnba_stats_lineups}

`load_wnba_stats_lineups(seasons, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load season-level WNBA 5-man lineup statistics (deprecated).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | an int or iterable of seasons. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per lineup-season-measure_type, stacked from the `wnba_stats_leaguedash` cube's `lineups_{base, advanced}` assets filtered to `group_quantity == 5` — matching the old `wnba_stats_lineups` tag's 5-man-only, Base+Advanced-only coverage. Call the cube's `lineups_*` assets directly (unfiltered) for 2/3/4-man lineups or the other 4 measure types.

**Example**

```python
from sportsdataverse.wnba import load_wnba_stats_lineups
df = load_wnba_stats_lineups(seasons=2026)
print(df.shape)
```

### load_wnba_stats_player_season_stats {#load_wnba_stats_player_season_stats}

`load_wnba_stats_player_season_stats(seasons, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load season-level WNBA player statistics (deprecated).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | an int or iterable of seasons. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per player-season-measure_type, stacked from the `wnba_stats_leaguedash` cube's `player_stats_*` assets (`Base`/`Advanced`/`Misc`/`Scoring`/`Usage`/`Defense` — matches the old `wnba_stats_player_season_stats` tag's coverage; player-level `Opponent`/`Four Factors` are empty upstream and were never populated by either version).

**Example**

```python
from sportsdataverse.wnba import load_wnba_stats_player_season_stats
df = load_wnba_stats_player_season_stats(seasons=2026)
print(df.shape)

# Pipeline next step (Advanced-only rows)

import polars as pl
adv = df.filter(pl.col("measure_type") == "Advanced")
```

### load_wnba_stats_standings {#load_wnba_stats_standings}

`load_wnba_stats_standings(seasons, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load season-level WNBA standings (deprecated).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | an int or iterable of seasons. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per team-season, read from the `wnba_stats_leaguedash` cube's `standings` asset -- the same underlying `leaguestandingsv3` endpoint/params as the old `wnba_stats_standings` tag, so this is close to a pure passthrough.

**Example**

```python
from sportsdataverse.wnba import load_wnba_stats_standings
df = load_wnba_stats_standings(seasons=2026)
print(df.shape)
```

### load_wnba_stats_team_season_stats {#load_wnba_stats_team_season_stats}

`load_wnba_stats_team_season_stats(seasons, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load season-level WNBA team statistics (deprecated).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | an int or iterable of seasons. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per team-season-measure_type, stacked from the `wnba_stats_leaguedash` cube's `team_stats_*` assets (`Base`/`Advanced`/`Misc`/`Scoring`/`Defense`/ `Opponent` — matches the old `wnba_stats_team_season_stats` tag's coverage; team-level `Usage`/`Four Factors` are empty upstream).

**Example**

```python
from sportsdataverse.wnba import load_wnba_stats_team_season_stats
df = load_wnba_stats_team_season_stats(seasons=2026)
print(df.shape)
```

### most_recent_wnba_season {#most_recent_wnba_season}

`most_recent_wnba_season()`

most_recent_wnba_season - return the most recent (likely-completed) WNBA season year.

Returns the current calendar year if it's May or later (the WNBA regular
season has tipped off), otherwise the previous calendar year.

**Returns**

Year (e.g. `2024`) suitable for passing as a `season` argument to schedule / loader functions.

**Example**

```python
from sportsdataverse.wnba import most_recent_wnba_season, espn_wnba_calendar
season = most_recent_wnba_season()
cal = espn_wnba_calendar(season=season)
print(season, cal.height)
```

### build_athlete_identity_lookup {#build_athlete_identity_lookup}

`build_athlete_identity_lookup(rosters: 'dict[int | str, dict]') -> 'dict[str, dict[str, Any]]'`

R `build_athlete_identity_lookup`: athlete_id -> identity from team rosters.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `rosters` | `dict[int \| str, dict]` |  | Mapping of team_id -> that team's raw roster payload (`wbb/team_rosters/json/{season}/{team_id}.json`). NOTE: R walks `raw$athletes` directly here (no position-bucket unwrap, unlike the rosters dataset itself). |

**Returns**

athlete_id (str) -> identity fields for `helper_wbb_player_season_stats`.

### build_wnba_season_wp {#build_wnba_season_wp}

`build_wnba_season_wp(season: 'int', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

A WNBA season's play-by-play with win-probability columns joined in.

Loads the season's play-by-play, schedule, and team boxscores, builds a
leakage-free weekly as-of pregame anchor per game from the WNBA ratings
engine (`league_id="10"`), scores every play through the bundled
in-game win-probability artifact, and returns the full `load_wnba_pbp`
frame with `pregame_home_prob` + `home_win_prob` appended -- the
enrich-in-place shape that overwrites the season's
`play_by_play_<season>.parquet` release asset.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season year (e.g. `2024`); bounded by `load_wnba_pbp` release availability. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The season's `load_wnba_pbp` frame (every column preserved) with the two WP columns `pregame_home_prob` + `home_win_prob` appended (both `Float64`), sorted by `game_id` then `game_play_number`.

**Example**

```python
from sportsdataverse.wnba import build_wnba_season_wp
wp = build_wnba_season_wp(2024)
wp.select("game_id", "game_play_number", "home_win_prob").head()

# Pandas output

wp_pd = build_wnba_season_wp(2024, return_as_pandas=True)
```

### espn_wnba_teams {#espn_wnba_teams}

`espn_wnba_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_wnba_teams - look up WNBA teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams for the requested league. This function caches by default, so if you want to refresh the data, use the command sportsdataverse.wnba.espn_wnba_teams.clear_cache().

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_alternate_color` | character | Team alternate color (hex without leading '#'). |
| `team_color` | character | Team primary color (hex without leading '#'). |
| `team_display_name` | character | Full team display name. |
| `team_id` | character | Unique team identifier. |
| `team_is_active` | logical | TRUE if the team is currently active. |
| `team_is_all_star` | logical | TRUE if the row represents an All-Star team. |
| `team_location` | character | Team city or location string. |
| `team_logos` | integer |  |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_nickname` | character | Team nickname. |
| `team_short_display_name` | character | Short team display name (e.g. 'Aces'). |
| `team_slug` | character | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_uid` | character | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |

**Example**

```python
from sportsdataverse.wnba import espn_wnba_teams
teams = espn_wnba_teams()
print(teams.shape)
teams.select(["team_id", "team_abbreviation", "team_display_name"]).head()

# Find Las Vegas Aces (team_id 17)

teams.filter(__import__("polars").col("team_id") == "17").to_dicts()

# Refresh the cache (the call is ``lru_cache``'d)

espn_wnba_teams.cache_clear()  # cached at function-level
teams_pd = espn_wnba_teams(return_as_pandas=True)
```

### make_prob_by_context {#make_prob_by_context}

`make_prob_by_context(ptshots: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'dict[str, Union[pl.DataFrame, pd.DataFrame]]'"`

Marginal FG% tables by defender distance and by shot clock.

The public API exposes defender-distance and shot-clock only as aggregate
bucket tables (`playerdashptshots`), not per-shot fields, so this
aggregates `Σfgm/Σfga` across players within each bucket.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ptshots` | `DataFrame` |  | The stacked `playerdashptshots` fixture — one frame with a `result_set` tag (`ClosestDefenderShooting` / `ShotClockShooting`) plus `bucket, fga, fgm`. |
| `return_as_pandas` | `bool` | `False` | Return pandas DataFrames instead of polars. |

**Returns**

`{"defender": frame, "shot_clock": frame}` each with rows per `bucket` (`bucket, fga, fgm, fg_pct`). Missing result sets return the zero-row schema.

**Example**

```python
from sportsdataverse.nba.nba_shot_value import make_prob_by_context
tables = make_prob_by_context(ptshots)
tables["defender"].sort("fg_pct")
```

### make_prob_joint {#make_prob_joint}

`make_prob_joint(defender: 'pl.DataFrame', shot_clock: 'pl.DataFrame', overall_fg_pct: 'float', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Independence-combined defender x shot-clock make probability.

Combines the two marginal FG% tables under a conditional-independence
assumption via odds multipliers: `odds(p) = p/(1-p)`;
`odds_joint = odds_overall * (odds_def/odds_overall) *
(odds_clock/odds_overall)`; `joint = odds_joint/(1+odds_joint)`. This
assumes defender distance and shot-clock effects are independent given the
league baseline — a simplification (a late clock correlates with tighter
defense), documented here so callers weigh it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `defender` | `DataFrame` |  | The `"defender"` marginal table from `make_prob_by_context` (`bucket, fg_pct`). |
| `shot_clock` | `DataFrame` |  | The `"shot_clock"` marginal table (`bucket, fg_pct`). |
| `overall_fg_pct` | `float` |  | The league overall FG% baseline. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per `(close_def_dist_range, shot_clock_range)`: `close_def_dist_range:Utf8, shot_clock_range:Utf8, joint_fg_pct:Float64`. Empty inputs return the zero-row schema.

**Example**

```python
from sportsdataverse.nba.nba_shot_value import make_prob_by_context, make_prob_joint
t = make_prob_by_context(ptshots)
joint = make_prob_joint(t["defender"], t["shot_clock"], 0.47)
```

### score_shot_xpoints {#score_shot_xpoints}

`score_shot_xpoints(shots: 'pl.DataFrame', league_avgs: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Score each shot with expected points from the league-average baseline.

Joins the per-shot frame to the zone baseline (falling back to the
within-`shot_zone_range` mean when a zone triple is unmatched) and adds
`shot_value` (3 for a `3PT` shot else 2), `xpoints = base_fg_pct *
shot_value`, and `actual_points = shot_made_flag * shot_value`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shots` | `DataFrame` |  | Per-shot `Shot_Chart_Detail` frame (needs `shot_type` + the three zone keys + `shot_made_flag`). |
| `league_avgs` | `DataFrame` |  | The `LeagueAverages` frame (see `xpoints_baseline`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The input `shots` plus `shot_value:Int64, base_fg_pct:Float64, xpoints:Float64, actual_points:Float64`. Empty input returns the augmented schema with zero rows.

**Example**

```python
from sportsdataverse.nba.nba_shot_value import score_shot_xpoints
scored = score_shot_xpoints(shots, league_avgs)

# Pipeline next step (one line)

scored.group_by("player_id").agg(pl.col("xpoints").sum())
```

### scoreboard_event_parsing {#scoreboard_event_parsing}

`scoreboard_event_parsing(event)`

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` |  |  |  |

### shooter_talent {#shooter_talent}

`shooter_talent(scored_shots: 'pl.DataFrame', *, league_id: 'str' = '00', min_attempts: 'int' = 50, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Regressed shooter true-talent: make%-above-expected, shrunk to the mean.

Aggregates `score_shot_xpoints` output per shooter and regresses the
raw over-expected rate toward zero by `n/(n+k)` (`k =
get_shrinkage_k(league_id)`, fitted split-half). **As-of leakage
boundary:** to score a shooter's talent for shots after date *D*, pass
only that shooter's shots before *D* -- this function does not enforce the
cut itself.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scored_shots` | `DataFrame` |  | `score_shot_xpoints` output (needs `player_id`, `shot_made_flag`, `base_fg_pct`, `xpoints`, `actual_points`). |
| `league_id` | `str` | `'00'` | `"00"` NBA, `"10"` WNBA, `"20"` G-League. |
| `min_attempts` | `int` | `50` | Drop shooters with fewer attempts (unstable estimate). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per `player_id`: `player_id:Int64, n_att:Int64, actual_makes:Int64, exp_makes:Float64, points_above_expected:Float64, raw_above_pct:Float64, talent_pct:Float64`. Empty input returns the zero-row schema.

**Example**

```python
from sportsdataverse.nba.nba_shot_value import score_shot_xpoints, shooter_talent
talent = shooter_talent(score_shot_xpoints(shots, league_avgs))

# Pipeline next step (one line)

talent.sort("talent_pct", descending=True).head(15)
```

### shot_selection_quality {#shot_selection_quality}

`shot_selection_quality(scored_shots: 'pl.DataFrame', *, min_attempts: 'int' = 50, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Player shot-selection quality: mean expected value vs the league mean.

`xev_per_shot` is a player's mean `xpoints` (the value of the LOOKS
they take, independent of makes); `selection_quality` is that minus the
league-wide mean `xpoints` over the same frame -- a rim-and-three diet
scores positive, a mid-range diet negative.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scored_shots` | `DataFrame` |  | `score_shot_xpoints` output (needs `player_id`, `xpoints`). |
| `min_attempts` | `int` | `50` | Drop players with fewer attempts. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per `player_id`: `player_id:Int64, n_att:Int64, xev_per_shot:Float64, league_xev_per_shot:Float64, selection_quality:Float64`. Empty input returns the zero-row schema.

**Example**

```python
from sportsdataverse.nba.nba_shot_value import score_shot_xpoints, shot_selection_quality
sel = shot_selection_quality(score_shot_xpoints(shots, league_avgs))

# Pipeline next step (one line)

sel.sort("selection_quality", descending=True).head(15)
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

**Example**

```python
from sportsdataverse.nba.nba_shot_value import score_shot_xpoints, zone_value_map
zmap = zone_value_map(score_shot_xpoints(shots, league_avgs))

# Pipeline next step (one line)

zmap.filter(pl.col("zone") == "corner_3").sort("pps_above_expected", descending=True)
```
