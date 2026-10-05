---
title: "NBA — additional Python functions — Other: make_prob–zone_value"
sidebar_label: "Other: make_prob–zone_value"
sidebar_position: 10
description: "NBA — additional Python functions — Other: make_prob–zone_value — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Other: make_prob–zone_value

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

### nbadraft_mock_draft {#nbadraft_mock_draft}

`nbadraft_mock_draft(year: 'Optional[int]' = None, *, proxy: 'Any' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

The current consensus mock draft from NBADraft.net.

One row per pick across both rounds. The page renders round 1 and round 2 as
the first two pick tables and then **repeats round 1 in a third table**, so
only the first two are taken -- concatenating all three double-counts round 1.
The `<noscript>` fallback is another false-positive JS challenge; the pick
tables are static. A traded pick's team cell carries `*`, which is stripped.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Optional[int]` | `None` | Draft year (e.g. `2025`). `None` (default) reads the site's current mock; a year uses the `/nba-mock-drafts/{year}/` path where NBADraft.net has one. |
| `proxy` | `Any` | `None` | Proxy configuration forwarded to `~sportsdataverse.dl_utils.download` (`requests` `proxies=` shape). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per pick with `round` (1 or 2), `pick`, `team`, `player`, `height`, `weight`, `position`, `school` and `class`. An unreachable or table-less page yields a zero-row frame with that schema.

| col_name | type | description |
|---|---|---|
| `round` | integer | Tournament / playoff round. |
| `pick` | integer | Pick number within the round. |
| `team` | character | Team-side label or team identifier. |
| `player` | character | Player name. |
| `height` | character | Player height (string e.g. '6-2' or inches). |
| `weight` | integer | Player weight in pounds. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `school` | character | Player school / pre-draft team. |
| `class` | character | College class / draft eligibility note. |

**Example**

```python
from sportsdataverse.nba import nbadraft_mock_draft

mock = nbadraft_mock_draft()
print(mock.shape)

# A specific draft year, as pandas

mock_pd = nbadraft_mock_draft(year=2025, return_as_pandas=True)

# Pipeline next step (lottery only)

mock.filter((pl.col("round") == 1) & (pl.col("pick") <= 14))
```

### normalize_player_name {#normalize_player_name}

`normalize_player_name(name: 'str') -> 'str'`

Fold a player display name to a join-safe key.

Lower-cases, strips diacritics (`"Jokić"` -> `"jokic"` -- the real
stats.nba.com feed spells Nikola Jokic's name with the Serbian `ć`,
while the DARKO/D&T CSVs use plain ASCII), drops periods/apostrophes/
hyphens, collapses internal whitespace, and strips a trailing
Jr./Sr./II/III/IV suffix. Two names normalize equal iff they refer to
the same join key under this scheme -- it is NOT guaranteed globally
unique (rare true duplicate full names are a known, accepted residual;
`external_validity`'s `coverage_pct` surfaces the effect rather
than hiding it).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | A raw display name, e.g. `"Nikola Jokić"` or `"A.J. Green"`. |

**Returns**

The normalized key, e.g. `"nikola jokic"`, `"aj green"`. Empty string in, empty string out (never raises).

**Example**

```python
from sportsdataverse.nba.nba_oracle_data import normalize_player_name
assert normalize_player_name("Nikola Jokić") == normalize_player_name("Nikola Jokic")
assert normalize_player_name("Gary Trent Jr.") == normalize_player_name("Gary Trent")
```

### predict_margin {#predict_margin}

`predict_margin(home_net: 'float', away_net: 'float', *, home_pace: 'float', away_pace: 'float', neutral: 'bool' = False, league_id: 'str' = '00') -> 'float'`

Expected home-minus-away margin from two adjusted net ratings.

The AdjNet difference (points/100 possessions) is scaled by the
matchup's `expected_possessions` before the home-court advantage
is added.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_net` | `float` |  | Home team's adjusted net rating (`adj_net_rtg`). |
| `away_net` | `float` |  | Away team's adjusted net rating. |
| `home_pace` | `float` |  | Home team's adjusted pace. |
| `away_pace` | `float` |  | Away team's adjusted pace. |
| `neutral` | `bool` | `False` | True for a neutral-site game (no home-court advantage). |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"` -- selects the fitted HFA. |

**Returns**

Expected margin in points (positive favors the home team).

**Example**

```python
from sportsdataverse.nba.nba_game_predict import predict_margin
predict_margin(10.0, -2.0, home_pace=100.0, away_pace=98.0, neutral=False)
```

### predict_total {#predict_total}

`predict_total(home_off: 'float', home_def: 'float', away_off: 'float', away_def: 'float', home_pace: 'float', away_pace: 'float', *, league_id: 'str' = '00') -> 'float'`

Expected total points from adjusted ratings and paces.

Expected possessions come from `expected_possessions`; each side's
expected points per 100 possessions blend its offense with the
opponent's defense (`0.5 * (off + opp_def)`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_off` | `float` |  | Home adjusted offensive rating (points/100 poss). |
| `home_def` | `float` |  | Home adjusted defensive rating. |
| `away_off` | `float` |  | Away adjusted offensive rating. |
| `away_def` | `float` |  | Away adjusted defensive rating. |
| `home_pace` | `float` |  | Home team's adjusted pace. |
| `away_pace` | `float` |  | Away team's adjusted pace. |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"` -- selects the pace anchor. |

**Returns**

Expected combined points scored by both teams.

**Example**

```python
from sportsdataverse.nba.nba_game_predict import predict_total
predict_total(118.0, 108.0, 110.0, 112.0, 100.0, 98.0)
```

### prob_over {#prob_over}

`prob_over(exp_value: 'float', line: 'float', stat: 'str', *, league_id: 'str' = '00') -> 'float'`

Probability a stat finishes strictly above `line`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_value` | `float` |  | Projected mean of the stat. |
| `line` | `float` |  | The prop line. |
| `stat` | `str` |  | One of `"pts"`, `"reb"`, `"ast"`, `"fg3m"`. |
| `league_id` | `str` | `'00'` | Accepted for parity. |

**Returns**

`P(stat > line)` in `[0, 1]`.

**Example**

```python
from sportsdataverse.nba.nba_player_props import prob_over
prob_over(24.0, 22.5, "pts")
```

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
| `asof` | `date` |  | The through-date checkpoint (inclusive). |

**Returns**

`RatingsFit` with per-player offense/defense ratings (per-100-possession scale, same sign convention as `nba_rapm`: positive `d_ratings` means good defense). Empty dicts when no possessions fall on or before `asof` or when `possessions` is empty.

**Example**

```python
import datetime
from sportsdataverse.nba.nba_model_validation import RidgeRapmModel
from sportsdataverse.nba.nba_ratings_panel import ratings_as_of

rf = ratings_as_of(RidgeRapmModel(), season_poss, datetime.date(2023, 12, 1))
print(rf.o_ratings[201939])   # per-100 offensive rating through Dec 1
```

### raw_game_efficiency {#raw_game_efficiency}

`raw_game_efficiency(schedule: 'pl.DataFrame', team_box: 'pl.DataFrame') -> 'pl.DataFrame'`

Per-team, per-game possessions + raw offensive/defensive rating.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `schedule` | `DataFrame` |  | Frame with `game_id, season, date, home_team_id, away_team_id, neutral_site` (ids cast to `Utf8` here). |
| `team_box` | `DataFrame` |  | Per-team box score with `game_id, team_id, field_goals_attempted, offensive_rebounds, turnovers, free_throws_attempted, team_score`. |

**Returns**

One row per (game_id, team_id): `game_id, season, date, team_id, opp_team_id, is_home, neutral_site, poss, off_rtg, def_rtg`. Empty input returns that schema with zero rows.

**Example**

```python
from sportsdataverse.nba.nba_loaders import load_nba_schedule, load_nba_team_boxscore
from sportsdataverse.nba.nba_team_ratings import raw_game_efficiency
eff = raw_game_efficiency(load_nba_schedule([2024]), load_nba_team_boxscore([2024]))
```

### render_report {#render_report}

`render_report(report: 'ValidationReport') -> 'str'`

Render a `ValidationReport` as a human-readable markdown validation card.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `report` | `ValidationReport` |  | A populated `ValidationReport` from `validate_model`. |

**Returns**

A multi-section markdown string with one `##` heading per oracle. Sections whose oracle result is `None` (either skipped or not applicable for a point-estimate model) are rendered as `- n/a`.

**Example**

```python
from sportsdataverse.nba.nba_model_validation import (
    RidgeRapmModel, validate_model, render_report,
)

rep = validate_model(RidgeRapmModel(), season_frames, model_name="plain_rapm")
md = render_report(rep)
print(md)

# Capture the markdown string for downstream use

with open("validation_card.md", "w") as f:
    f.write(render_report(rep))
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
| `player_id` | character | Unique player identifier. |
| `player` | character | Player name. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `team` | character | Team-side label or team identifier. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `injury` | character | Injury (body part / description). |
| `status` | character | Status label. |
| `return_date` | character | Projected return (`NA` unless a subscriber). |
| `url` | character | RotoWire player page URL. |

**Example**

```python
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

**Example**

```python
from sportsdataverse.nba.nba_shot_value import score_shot_xpoints
scored = score_shot_xpoints(shots, league_avgs)

# Pipeline next step (one line)

scored.group_by("player_id").agg(pl.col("xpoints").sum())
```

### scoreboard_event_parsing {#scoreboard_event_parsing}

`scoreboard_event_parsing(event)`

Internal helper that flattens an ESPN NBA scoreboard event dict into a

shape suitable for `pd.json_normalize`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` | `dict` |  | A single scoreboard `events[*]` entry from the ESPN NBA scoreboard API. |

**Returns**

The same event dict, mutated in place with `home`/`away` copies of the competitors and trimmed of unused link/odds keys.

**Example**

```python
from sportsdataverse.nba import espn_nba_schedule
sched = espn_nba_schedule(dates=20230102)
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
| `rank` | integer | Rank. |
| `team` | character | Team-side label or team identifier. |
| `record` | character | Overall win-loss record. |
| `players_active` | integer | Number of active players. |
| `avg_age_team` | double | Average roster age. |
| `total_cap_allocations` | double | Total cap allocations (USD). |
| `cap_space_all` | double | Cap space / over-the-cap amount (USD). |
| `season` | integer | Season year. |

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

**Example**

```python
ctx = team_play_context(add_play_context(pbp))
print(ctx.select("offense_team_id", "transition_freq", "transition_pts_added_per_100"))

# Season-comparable Pts+/Poss

ctx = team_play_context(season_poss, league_non_transition_ppp=104.8)
```

### train_spm {#train_spm}

`train_spm(box_features: 'pl.DataFrame', rapm_target: 'pl.DataFrame', *, feature_names: 'Optional[List[str]]' = None, alpha: 'float' = 100.0) -> 'SpmCoefficients'`

Ridge-fit box features onto `o_rapm` and `d_rapm` (two regressions).

The two models share the same feature matrix but separate target vectors,
producing independent offense and defense coefficient vectors.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_features` | `DataFrame` |  | Per-player per-100 features. Must contain `player_id` and every column in *feature_names*. |
| `rapm_target` | `DataFrame` |  | Per-player RAPM target frame with columns `player_id`, `o_rapm`, and `d_rapm`. Only the rows whose `player_id` appears in *box_features* are used (inner join). |
| `feature_names` | `Optional[List[str]]` | `None` | Ordered list of feature columns to regress on. Defaults to `SPM_FEATURES` (= STATS` from `nba_box_logs`). |
| `alpha` | `float` | `100.0` | Ridge regularization strength (`sklearn.linear_model.Ridge`). Lower values approach OLS; higher values shrink toward zero. |

**Returns**

`SpmCoefficients` with offense and defense coefficient vectors, intercepts, and the ordered `feature_names`.

**Example**

```python
from sportsdataverse.nba import train_spm
coef = train_spm(box_feats, rapm_ratings)

# With custom regularization

coef = train_spm(box_feats, rapm_ratings, alpha=50.0)
```

### validate_model {#validate_model}

`validate_model(model: 'AnyModel', season_frames: 'List[pl.DataFrame]', *, model_name: 'str' = 'model', oracles: 'Tuple[str, ...]' = ('retrodiction', 'reliability', 'cross_season', 'calibration'), seed: 'int' = 0, external_ratings: 'Optional[pl.DataFrame]' = None, external_oracle: 'Optional[pl.DataFrame]' = None, external_rating_col: 'str' = 'rating', external_oracle_col: 'str' = 'oracle_value', external_join: 'str' = 'id', walk_forward_horizon_days: 'int' = 14, walk_forward_min_games: 'int' = 15) -> 'ValidationReport'`

Run the selected oracles and assemble a `ValidationReport`.

`retrodiction`/`reliability`/`calibration` run on the pooled possessions
(all seasons concatenated); `cross_season` runs on the ordered per-season
frames. Any oracle not selected is left `None`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `AnyModel` |  | A fitted or unfitted RAPM-family estimator (`fit(X, y)` protocol). |
| `season_frames` | `List[DataFrame]` |  | Ordered list of per-season possession frames. All frames are concatenated into a single pooled frame for Oracles 1, 2, and 4. |
| `model_name` | `str` | `'model'` | Label written into the returned report and markdown card. |
| `oracles` | `Tuple[str, ...]` | `('retrodiction', 'reliability', 'cross_season', 'calibration')` | Tuple of oracle names to run. Omit a name to skip that oracle and leave its result field `None`. Accepts `"external"` and `"walk_forward"` in addition to the four original names; the default tuple is unchanged, so existing callers are unaffected. |
| `seed` | `int` | `0` | RNG seed forwarded to each oracle for determinism. |
| `external_ratings` | `Optional[DataFrame]` | `None` | The model's own ratings frame -- required when `"external"` is in `oracles`. |
| `external_oracle` | `Optional[DataFrame]` | `None` | A loaded oracle frame (from `nba_oracle_data`) -- required when `"external"` is in `oracles`. |
| `external_rating_col` | `str` | `'rating'` | Rating column name in `external_ratings`. |
| `external_oracle_col` | `str` | `'oracle_value'` | Value column name in `external_oracle`. |
| `external_join` | `str` | `'id'` | `"id"` or `"name"`, forwarded to `external_validity`. |
| `walk_forward_horizon_days` | `int` | `14` | Forwarded to `walk_forward`. |
| `walk_forward_min_games` | `int` | `15` | Forwarded to `walk_forward` as `min_games_before_first_checkpoint`. |

**Returns**

A `ValidationReport` whose fields are populated for every selected oracle and `None` for every skipped oracle.

**Example**

```python
from sportsdataverse.nba.nba_model_validation import (
    RidgeRapmModel, validate_model,
)

# season_frames is a list[pl.DataFrame] of possession stints
rep = validate_model(RidgeRapmModel(), season_frames, model_name="plain_rapm")
print(rep.retrodiction.game_margin_rmse)   # out-of-sample margin RMSE
print(rep.reliability.spearman_brown)      # split-half Spearman-Brown
print(rep.calibration)                     # None — RidgeRapmModel has no posterior

# Skip slow oracles when iterating quickly

rep = validate_model(
    RidgeRapmModel(), season_frames,
    oracles=("retrodiction", "reliability"),
)
print(rep.cross_season)   # None — not selected
```

### walk_forward {#walk_forward}

`walk_forward(model: 'AnyModel', possessions: 'pl.DataFrame', *, checkpoint_dates: 'Optional[List[datetime.date]]' = None, horizon_days: 'int' = 14, min_games_before_first_checkpoint: 'int' = 15) -> 'WalkForwardResult'`

Oracle 6: time-ordered "predict tomorrow" retrodiction.

For each checkpoint date D: fit on games with `game_date <= D`, predict
games with `D < game_date <= D + horizon_days`, aggregate to
per-(game, team) margins -- reusing fit_on` / design_with_ids`
/ `predict_points` / team_game_margins` (the same machinery
`retrodiction` uses). `carry_forward_rmse` reapplies the PREVIOUS
checkpoint's fit (no refit) to the current window. `random_fold_rmse` is
`retrodiction`'s pooled game-margin RMSE on the same possessions.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `AnyModel` |  | A harness model (`RapmModel`/`RatingsModel`/`PriorModel`). |
| `possessions` | `DataFrame` |  | A season possession+lineup frame with `game_date` (from `compile_nba_season`), `game_id`, `offense_team_id`, `points`, and the ten lineup columns. |
| `checkpoint_dates` | `Optional[List[date]]` | `None` | Explicit checkpoint grid; derived from `possessions` via `horizon_days`/`min_games_before_first_checkpoint` when `None` (default). |
| `horizon_days` | `int` | `14` | Days-ahead prediction window per checkpoint (default 14). |
| `min_games_before_first_checkpoint` | `int` | `15` | Distinct-game-date index of the first checkpoint when deriving the default grid (default 15, "~game 15 of the season"). |

**Returns**

`WalkForwardResult`. All metrics `nan` and counts `0` when `possessions` is empty, lacks a `game_date` column, or the derived/given grid produces zero non-degenerate checkpoints.

**Example**

```python
from sportsdataverse.nba.nba_model_validation import RidgeRapmModel, walk_forward
res = walk_forward(RidgeRapmModel(), season_possessions)
print(res.game_margin_rmse, res.carry_forward_rmse, res.random_fold_rmse)
```

### win_prob_from_margin {#win_prob_from_margin}

`win_prob_from_margin(exp_margin: 'float', *, league_id: 'str' = '00') -> 'float'`

Home win probability from an expected margin (normal-CDF closed form).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_margin` | `float` |  | Expected home-minus-away margin in points. |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"` -- selects the fitted margin sigma. |

**Returns**

Probability the home team wins, in `(0, 1)`.

**Example**

```python
from sportsdataverse.nba.nba_game_predict import win_prob_from_margin
win_prob_from_margin(5.0)
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

**Example**

```python
from sportsdataverse.nba.nba_shot_value import score_shot_xpoints, zone_value_map
zmap = zone_value_map(score_shot_xpoints(shots, league_avgs))

# Pipeline next step (one line)

zmap.filter(pl.col("zone") == "corner_3").sort("pps_above_expected", descending=True)
```
