---
title: "NBA — additional Python functions — Analytics: project_player–zone_value"
sidebar_label: "Analytics: project_player–zone_value"
sidebar_position: 16
description: "NBA — additional Python functions — Analytics: project_player–zone_value — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Analytics: project_player–zone_value

### project_player_line {#project_player_line}

`project_player_line(rate_row: 'dict[str, Any]', exp_minutes: 'float', pace_factor: 'float' = 1.0) -> 'dict[str, float]'`

Project a player's expected counting line from per-minute rates.

`exp_stat = rate_per_min * exp_minutes * pace_factor` -- counting stats
scale with both projected minutes and pace.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `rate_row` | `dict[str, Any]` |  | One row of `player_rates` (as a dict). |
| `exp_minutes` | `float` |  | Projected minutes for the game. |
| `pace_factor` | `float` | `1.0` | Pace multiplier (`exp_poss / avg_pace`); `1.0` for a league-average-pace matchup. |

**Returns**

`{"exp_pts", "exp_reb", "exp_ast", "exp_fg3m"}`.

**Example**

```python
from sportsdataverse.nba.nba_player_props import player_rates, project_player_line
r = player_rates(box_logs).row(0, named=True)
line = project_player_line(r, exp_minutes=32.0, pace_factor=1.02)
```

### prop_distribution {#prop_distribution}

`prop_distribution(exp_value: 'float', stat: 'str', *, league_id: 'str' = '00') -> 'tuple[str, dict[str, float]]'`

Distribution family + parameters for a projected stat mean.

Points -> Normal `(mu, sd)` with `sd = a + b*sqrt(mu)`; counts
(reb/ast/fg3m) -> Negative-Binomial `(r, p)` matching mean `mu` and
variance `dispersion*mu` (Poisson if dispersion <= 1).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_value` | `float` |  | Projected mean of the stat. |
| `stat` | `str` |  | One of `"pts"`, `"reb"`, `"ast"`, `"fg3m"`. |
| `league_id` | `str` | `'00'` | Accepted for parity (dispersion is currently league-shared). |

**Returns**

`(family, params)` where family is `"normal"`, `"nbinom"` or `"poisson"`.

**Example**

```python
from sportsdataverse.nba.nba_player_props import prop_distribution
fam, par = prop_distribution(24.0, "pts")
```

### ratings_as_of {#ratings_as_of}

`ratings_as_of(model: 'AnyModel', possessions: 'pl.DataFrame', asof: 'datetime.date') -> 'RatingsFit'`

Fit `model` on every possession dated on or before `asof` and return ratings.

This is the through-date primitive: possessions with `game_date > asof`
are excluded from the fit entirely (never merely down-weighted), which is
what makes the panel built from repeated calls to this function leakage-free
by construction — see `tests/nba/test_nba_ratings_panel.py::test_ratings_as_of_is_leakage_free_append_invariant`.
NOTE: the leakage property is proven by the append-invariance test TOGETHER
with the panel's per-date-parity test — neither alone covers
cross-checkpoint-window leaks; do not prune one without the other.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `AnyModel` |  | A harness model conforming to `nba_model_validation.AnyModel` (a `RapmModel`, `RatingsModel`, or `PriorModel`). |
| `possessions` | `DataFrame` |  | A possession+lineup frame that MUST carry a `game_date` (`pl.Date`) column (as emitted by `compile_nba_season`). |
| `asof` | `date` |  | The through-date checkpoint (inclusive: `game_date <= asof`). A pregame consumer of a game on day D passes `D - 1 day`; the pregame surfaces (`nba_player_props`, `nba_team_ratings`) take a strict `as_of_date` instead. |

**Returns**

`RatingsFit` with per-player offense/defense ratings (per-100-possession scale, same sign convention as `nba_rapm`: positive `d_ratings` means good defense). Empty dicts when no possessions fall on or before `asof` or when `possessions` is empty.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `possessions` (non-empty) does not carry a `game_date` column — this is a caller-contract violation, not a normal empty case, so it raises rather than silently returning empty ratings. |

**Example**

```python
import datetime
from sportsdataverse.nba.nba_model_validation import RidgeRapmModel
from sportsdataverse.nba.nba_ratings_panel import ratings_as_of

rf = ratings_as_of(RidgeRapmModel(), season_poss, datetime.date(2023, 12, 1))
print(rf.o_ratings[201939])   # per-100 offensive rating through Dec 1
```

### rotowire_injuries {#rotowire_injuries}

`rotowire_injuries(*, proxy: 'Any' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

The current NBA injury report from RotoWire.

One row per injured player: team, position, the injury, the designation
(Out / Doubtful / Questionable / GTD / Day-To-Day) and a link to the player's
RotoWire page. This is the live replacement for the defunct RotoWorld feed.

The rendered grid at `/basketball/news.php?view=injuries` builds itself
client-side, so this reads the JSON table endpoint the grid calls
(`/basketball/tables/injury-report.php?team=ALL&pos=ALL`) rather than
scraping the page. The projected return date is subscriber-gated and comes
back as `"Subscribers Only"`; it is returned as null for non-subscribers.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `proxy` | `Any` | `None` | Proxy configuration forwarded to `~sportsdataverse.dl_utils.download` (`requests` `proxies=` shape). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per injured player with `player_id`, `player`, `first_name`, `last_name`, `team`, `position`, `injury`, `status`, `return_date` and `url`. An unreachable endpoint or a non-list body yields a zero-row frame with that schema.

| col_name | type | description |
|---|---|---|
| `player_id` | String | Unique player identifier. |
| `player` | String | Player name. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `team` | String | Team-side label or team identifier. |
| `position` | String | Listed roster position (G, F, C, etc.). |
| `injury` | String | Injury (body part / description). |
| `status` | String | Status label. |
| `return_date` | String | Projected return date as RotoWire shows it; null for non-subscribers (the endpoint sends "Subscribers Only") and when the cell is blank. |
| `url` | String | RotoWire player page URL. |

**Raises**

| Exception | Description |
|---|---|
| `UserWarning` | Emitted (not raised) when the endpoint cannot be fetched or does not answer with a JSON array; the call still returns an empty frame. |

**Example**

```python
import polars as pl
from sportsdataverse.nba import rotowire_injuries

injuries = rotowire_injuries()
print(injuries.shape)

# As pandas

injuries_pd = rotowire_injuries(return_as_pandas=True)

# Pipeline next step (who is ruled out)

injuries.filter(pl.col("status") == "Out").select("player", "team", "injury")
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

No returns table is published for this function: no capture: its input is stats.nba.com shot-chart detail (shot zones and types), which answers HTTP 403 to the datacenter IP the docs are built on and is not in the raw store.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | A zone join key has a different dtype on the two frames. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_shot_value import score_shot_xpoints
scored = score_shot_xpoints(shots, league_avgs)

# Pipeline next step (one line)

scored.group_by("player_id").agg(pl.col("xpoints").sum())
```

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

No returns table is published for this function: no capture: its input is stats.nba.com shot-chart detail (shot zones and types), which answers HTTP 403 to the datacenter IP the docs are built on and is not in the raw store.

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

No returns table is published for this function: no capture: its input is stats.nba.com shot-chart detail (shot zones and types), which answers HTTP 403 to the datacenter IP the docs are built on and is not in the raw store.

**Example**

```python
from sportsdataverse.nba.nba_shot_value import score_shot_xpoints, shot_selection_quality
sel = shot_selection_quality(score_shot_xpoints(shots, league_avgs))

# Pipeline next step (one line)

sel.sort("selection_quality", descending=True).head(15)
```

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
