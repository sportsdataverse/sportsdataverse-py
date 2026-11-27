---
title: "NBA — additional Python functions — Analytics: shrink_clutch–zone_value"
sidebar_label: "Analytics: shrink_clutch–zone_value"
sidebar_position: 23
description: "NBA — additional Python functions — Analytics: shrink_clutch–zone_value — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Analytics: shrink_clutch–zone_value

### shrink_clutch {#shrink_clutch}

`shrink_clutch(delta: 'pl.DataFrame', *, league_id: 'str' = '00') -> 'pl.DataFrame'`

Empirical-Bayes / James-Stein shrinkage of `clutch_delta` toward zero.

Per-team sampling variance is `σ²_i = scale / clutch_poss` (small samples
shrink harder); the between-team signal variance `τ²` is the observed
variance of `clutch_delta` net of mean sampling variance; the shrink
factor `k_i = τ² / (τ² + σ²_i)` and `clutch_skill_shrunk = k_i · delta_i`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `delta` | `DataFrame` |  | Output of `clutch_delta` (needs `clutch_delta` + `clutch_poss`). |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"` (accepted for parity; the scale is currently league-shared). |

**Returns**

`delta` with an added `clutch_skill_shrunk` column. Empty input returns the input schema plus that column.

No returns table is published for this function: no capture: its clutch frame is built from stats.nba.com leaguedashteamclutch, which answers HTTP 403 to the datacenter IP the docs are built on.

**Example**

```python
from sportsdataverse.nba.nba_clutch import clutch_delta, shrink_clutch
skill = shrink_clutch(clutch_delta(clutch_frame, baseline_frame))
```

### spotrac_team_cap {#spotrac_team_cap}

`spotrac_team_cap(season: 'Optional[int]' = None, *, proxy: 'Any' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Team salary-cap allocations from Spotrac.

One row per team: cap allocations, cap space, active-player count and average
roster age for a season. No API key required.

The page carries a `<noscript>` fallback that looks like a JS challenge but
is not -- the cap table is in the static HTML. The team cell duplicates the
abbreviation (`"ORL ORL"`), so only the first token is kept, and every
`$`-formatted column is parsed to `Float64`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season in 4-digit ENDING-year form (`2024` = the 2023-24 season). Defaults to `~sportsdataverse.nba.nba_schedule.most_recent_nba_season`. |
| `proxy` | `Any` | `None` | Proxy configuration forwarded to `~sportsdataverse.dl_utils.download` (`requests` `proxies=` shape). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team. Columns follow Spotrac's table -- `rank`, `team`, `record`, `players_active`, `avg_age_team`, `total_cap_allocations`, `cap_space_all` -- plus `season`. An unreachable or table-less page yields a zero-row frame with that schema.

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank. |
| `team` | String | Team-side label or team identifier. |
| `record` | String | Overall win-loss record. |
| `players_active` | Int64 | Number of active players. |
| `avg_age_team` | Float64 | Average roster age. |
| `total_cap_allocations` | Float64 | Total cap allocations (USD). |
| `cap_space_all` | Float64 | Cap space / over-the-cap amount (USD). |
| `season` | Int64 | Season year. |

**Raises**

| Exception | Description |
|---|---|
| `UserWarning` | Emitted (not raised) when the page cannot be fetched or carries no table; the call still returns an empty frame. |

**Example**

```python
from sportsdataverse.nba import spotrac_team_cap

cap = spotrac_team_cap(season=2024)
print(cap.shape)

# As pandas

cap_pd = spotrac_team_cap(season=2024, return_as_pandas=True)

# Pipeline next step (most cap space)

cap.sort("cap_space_all", descending=True).head()
```

### starters_on_court_counts {#starters_on_court_counts}

`starters_on_court_counts(possessions: 'pl.DataFrame', starters: 'dict[int, list[int]]') -> 'dict[int, int]'`

Count, per possession, how many **starters** are on the floor across BOTH teams.

This supplies the second half of CTG's garbage-time rule — "there have to be
**two or fewer starters on the floor combined between the two teams**" — which
`flag_garbage_time` cannot evaluate on its own (the possession frame does
not carry who is on the floor).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Possession frame with the ten on-court columns `off_player_1..5` **and** `def_player_1..5` (from `~sportsdataverse.nba.nba_possessions.attach_possession_lineups`). |
| `starters` | `dict[int, list[int]]` |  | `{team_id: [player_id, ...]}` — e.g. from `~sportsdataverse.nba.nba_lineups._starters_from_boxscore_v3`. Player ids are matched across both teams' starting fives, so the offense/defense split of the lineup columns does not matter. |

**Returns**

`{possession_number: starters_on_floor}`, each value in `0..10`. An **empty** *starters* map yields all-zero counts, which would make CTG's `<= 2` clause vacuously true and flag every margin-qualifying possession. The counts are reported honestly rather than guessed — do not pass an empty map and then read the result as CTG-exact.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | When the `off_player_*` / `def_player_*` columns are absent. |

**Example**

```python
counts = starters_on_court_counts(poss, _starters_from_boxscore_v3(box))
print(max(counts.values()))  # 10 at the opening tip
```

### team_pace_projection {#team_pace_projection}

`team_pace_projection(home_team_id: 'str', away_team_id: 'str', ratings: 'pl.DataFrame', *, league_id: 'str' = '00') -> 'float'`

Expected possessions for a matchup (Phase-3 `expected_possessions`).

A team with no row in `ratings` (no game before the cut yet: opening night,
or an expansion team's first game) or with a null or non-finite `adj_pace`
is given the league's `avg_pace`, so the projection stays finite.

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

### two_for_one_summary {#two_for_one_summary}

`two_for_one_summary(flagged: 'pl.DataFrame', *, by: 'tuple[str, ...]' = ('offense_team_id',), exclude_garbage: 'bool' = True, late_margin_max: 'int' = 10, n_boot: 'int' = 1000, ci: 'float' = 0.9, seed: 'int' = 0) -> 'pl.DataFrame'`

How often teams go 2-for-1, how often it pays the extra possession, and clock-matched net points.

Descriptive, not causal: whether a team goes depends on score, lineup, opponent and
timeouts, so `net_diff` is not "the value of going 2-for-1".

Kept: possessions with a `two_for_one` value, minus garbage time when
`exclude_garbage` (needs `is_garbage_time` from `add_possession_flags`), and in
period 4 and overtime only when `|margin_at_change| <= late_margin_max` (late-game
fouling is not a 2-for-1 choice).

Per `by` group: `games`, `n_went` / `n_held` / `n_other`, `go_rate` (went over
went plus held, null with neither), `extra_rate_*` (share with `extra_possession`),
`extra_rate_1s_*` (the sensitivity that counts the ball back only with 1.0 s or more on
its change clock), mean `sequence_net` per choice (`net_*`), and `net_diff`: went
minus held, taken within 1-second `change_clock` bins (`(b - 1, b]`, the window's shape)
and averaged with each bin weighted by its went plus held count, so the time each choice
starts with does not drive it. `net_diff_lo` / `net_diff_hi` are the `ci` interval of
a bootstrap that resamples whole games (never rows) within the group; a replicate whose draw
leaves no bin holding both choices is dropped, so the interval is conditional on the
comparison existing. `"other"` stays out of the comparison.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `flagged` | `DataFrame` |  | `add_two_for_one_flags` output with every possession of each game (the 1-second sensitivity reads the later possessions). Possessions carry no season column: to group by season, add one and pass it in `by`. |
| `by` | `tuple[str, ...]` | `('offense_team_id',)` | Group columns; `()` for one league row. Default `("offense_team_id",)`. |
| `exclude_garbage` | `bool` | `True` | Drop `is_garbage_time` possessions. Default True. |
| `late_margin_max` | `int` | `10` | Period 4 and overtime keep `\|margin_at_change\|` up to this. Default 10. |
| `n_boot` | `int` | `1000` | Bootstrap replicates, at least 1. Default 1000. |
| `ci` | `float` | `0.9` | Interval coverage, strictly between 0 and 1. Default 0.90. |
| `seed` | `int` | `0` | Bootstrap seed. Default 0. |

**Returns**

One row per group: the `by` columns then `TWO_FOR_ONE_SUMMARY_SCHEMA`, sorted by `by`. `net_diff` is null with no bin holding both choices. Zero rows when no possession is kept, whatever `by` is.

| col_name | type | description |
|---|---|---|
| `offense_team_id` | Int64 | NBA Stats team id of the offense (the default by group). |
| `games` | Int64 | Games with at least one kept 2-for-1-window possession for the group. |
| `n_went` | Int64 | Kept window possessions that went 2-for-1. |
| `n_held` | Int64 | Kept window possessions that held. |
| `n_other` | Int64 | Kept window possessions that were neither (an early turnover or foul trip, or an end between 24 and the floor). |
| `go_rate` | Float64 | n_went over n_went plus n_held; null when the group has neither. |
| `extra_rate_went` | Float64 | Share of went possessions whose offense got the ball back after the opponent's possession. |
| `extra_rate_held` | Float64 | Share of held possessions whose offense got the ball back after the opponent's possession. |
| `extra_rate_1s_went` | Float64 | As extra_rate_went, counting the ball back only with 1.0 s or more on its change clock. |
| `extra_rate_1s_held` | Float64 | As extra_rate_held, counting the ball back only with 1.0 s or more on its change clock. |
| `net_went` | Float64 | Mean sequence_net (the offense's rest-of-period points minus the defense's) of went possessions. |
| `net_held` | Float64 | Mean sequence_net of held possessions. |
| `net_other` | Float64 | Mean sequence_net of other possessions (a sensitivity; not in the comparison). |
| `net_diff` | Float64 | Went minus held sequence_net within 1-second change_clock bins ((b - 1, b]), averaged with each bin weighted by its went plus held count. Descriptive, not the value of going 2-for-1. |
| `net_diff_lo` | Float64 | Lower end of net_diff's interval (90% by default) from a bootstrap that resamples whole games. |
| `net_diff_hi` | Float64 | Upper end of net_diff's interval (90% by default) from a bootstrap that resamples whole games. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If a needed column is missing, `n_boot` is below 1, or `ci` is not strictly between 0 and 1. |

**Example**

```python
from sportsdataverse.nba.nba_possession_flags import two_for_one_summary
# poss: add_possession_flags then add_two_for_one_flags, plus a season column
print(two_for_one_summary(poss, by=("season",)))

# Per team

two_for_one_summary(poss).sort("go_rate", descending=True).head()
```

### xpoints_baseline {#xpoints_baseline}

`xpoints_baseline(league_avgs: 'pl.DataFrame') -> 'pl.DataFrame'`

League-average FG% baseline table keyed by the three shot-zone columns.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league_avgs` | `DataFrame` |  | The `LeagueAverages` result set from `nba_stats_shotchartdetail` (`ShotChartLineupLeagueAverage` from `nba_stats_shotchartlineupdetail`, same columns: `shot_zone_basic` / `shot_zone_area` / `shot_zone_range` / `fga` / `fgm` / `fg_pct`). |

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
