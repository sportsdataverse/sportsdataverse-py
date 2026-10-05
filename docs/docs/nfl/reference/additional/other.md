---
title: "NFL — additional Python functions — Other"
sidebar_label: "Other"
sidebar_position: 14
description: "NFL — additional Python functions — Other — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Other

### NflConfig {#NflConfig}

`NflConfig(cache_mode: 'CacheMode' = 'memory', cache_dir: 'Optional[Path]' = None, cache_duration: 'int' = 86400, verbose: 'bool' = True, timeout: 'int' = 30, user_agent: 'str' = 'sportsdataverse-py-nfl') -> None`

Runtime configuration for sdv-py NFL loaders.

Fields mirror nflreadpy's `NflreadpyConfig` so users can swap engines
without changing call sites. The defaults are conservative: in-memory
caching with a 24-hour TTL, verbose progress bars on, 30-second
HTTP timeout.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `cache_mode` | `CacheMode` | `'memory'` |  |
| `cache_dir` | `Optional[Path]` | `None` |  |
| `cache_duration` | `int` | `86400` |  |
| `verbose` | `bool` | `True` |  |
| `timeout` | `int` | `30` |  |
| `user_agent` | `str` | `'sportsdataverse-py-nfl'` |  |

**Example**

```python
from sportsdataverse.nfl import get_config
cfg = get_config()  # NflConfig instance
cfg.cache_mode      # "memory"
cfg.cache_duration  # 86400 (24h)
cfg.timeout         # 30 (seconds)

# Construct a fresh instance directly (rarely needed -- prefer ``update_config``)

from sportsdataverse.nfl import NflConfig
cfg = NflConfig(cache_mode="off", timeout=10)
```

### adjust_pressure_pairs {#adjust_pressure_pairs}

`adjust_pressure_pairs(pairs: 'pl.DataFrame', *, max_iter: 'int' = 50, tol: 'float' = 0.0001) -> 'pl.DataFrame'`

Opponent-adjust matchup pressure rates via an additive fixed point.

Fits `rate(off, def) ~ mu + alpha_off + beta_def` per season by
alternating dropback-weighted residual means (league-mean-centered);
league-agnostic (no NFL constant inside).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pairs` | `DataFrame` |  | Output of `pressure_pairs` (or any frame with `season`, `off_team`, `def_team`, `dropbacks`, `pressures`). |
| `max_iter` | `int` | `50` | Fixed-point iteration cap. |
| `tol` | `float` | `0.0001` | Max-abs-change convergence tolerance. |

**Returns**

Per `(season, team)`: raw allowed/generated rates + counts and `adj_pressure_rate_allowed` (`mu + alpha`) / `adj_pressure_rate_generated` (`mu + beta`).

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the aggregate. |
| `team` | character | Team abbreviation. |
| `dropbacks_off` | integer | Offensive dropbacks (qb_dropback plays). |
| `pressures_allowed` | integer | Sacks plus QB hits allowed on the team's own dropbacks. |
| `pressure_rate_allowed` | double | pressures_allowed / dropbacks_off (raw). |
| `dropbacks_def` | integer | Opponent dropbacks faced on defense. |
| `pressures_generated` | integer | Sacks plus QB hits generated against opponent dropbacks. |
| `pressure_rate_generated` | double | pressures_generated / dropbacks_def (raw). |
| `adj_pressure_rate_allowed` | double | Opponent-adjusted allowed pressure rate (mu + team offense effect from the additive fixed point). |
| `adj_pressure_rate_generated` | double | Opponent-adjusted generated pressure rate (mu + team defense effect from the additive fixed point). |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_line_grades import (
    adjust_pressure_pairs, pressure_pairs,
)
adj = adjust_pressure_pairs(pressure_pairs(load_nfl_pbp([2023])))
print(adj.sort("adj_pressure_rate_generated", descending=True).head())
```

### cached_loader {#cached_loader}

`cached_loader(func: 'F') -> 'F'`

Decorator that adds caching to a `load_nfl_*` function.

Honors the active `NflConfig.cache_mode`:

- `memory`: dict-based per-process cache.
- `filesystem`: parquet-based cross-process cache under `cache_dir`.
- `off`: no caching, function runs every time.

The cache key is the hash of `(qualified_name, args, kwargs)` with
`return_as_pandas` excluded so memory / disk hits work regardless of
which return shape the caller asked for. The cache always stores the
polars frame internally and converts to pandas on read when requested.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `func` | `F` |  |  |

**Example**

```python
import polars as pl
from sportsdataverse.nfl.cache import cached_loader

@cached_loader
def load_my_thing(season: int, return_as_pandas: bool = False):
    # ... fetch parquet, build a polars frame ...
    return pl.DataFrame({"season": [season]})

df1 = load_my_thing(2024)            # network hit, populates cache
df2 = load_my_thing(2024)            # served from cache
df_pd = load_my_thing(2024, return_as_pandas=True)
# `return_as_pandas` is excluded from the cache key, so the
# polars hit is reused and converted to pandas on the way out.

# Switch caching modes at runtime

from sportsdataverse.nfl import clear_cache, update_config

update_config(cache_mode="filesystem")  # parquet-on-disk reuse
df3 = load_my_thing(2024)               # writes parquet under cache_dir
clear_cache()                           # wipe both memory + filesystem
update_config(cache_mode="off")         # bypass cache entirely
```

### clean_nfl_pbp {#clean_nfl_pbp}

`clean_nfl_pbp(df: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Canonicalize names/ids/teams on a play-by-play frame (nflfastR `clean_pbp` port).

See the module docstring for the full column set added, the
compute-if-absent scope note on `pass`/`rush`, and the lookaround ->
capture-group regex rewrites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | An nflverse-shape (or ESPN/native) play-by-play `polars.DataFrame`. Required columns: `desc`, `epa`, `game_id`, `play_id`, `season`, `posteam`. See the module docstring for the full optional-column-with-default list. |
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame`; otherwise a `polars.DataFrame` (default). |

**Returns**

The input frame with every §6 column added/overwritten (idempotent -- pre-existing values of those columns, except `pass`/`rush`, are dropped and recomputed). A zero-row input yields a zero-row frame carrying the full documented schema rather than raising.

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_clean import clean_nfl_pbp

pbp = load_nfl_pbp([2023])
cleaned = clean_nfl_pbp(pbp)
print(cleaned.select("name", "id", "fantasy").head())

# Pandas output

cleaned_pd = clean_nfl_pbp(pbp, return_as_pandas=True)

# Pipeline next step (one line)

import polars as pl
cleaned.filter(pl.col("play") == 1).group_by("passer").len()
```

### clear_cache {#clear_cache}

`clear_cache() -> 'None'`

Clear both memory and filesystem caches.

Memory: empties the in-process dict.
Filesystem: removes all entries under `config.cache_dir`. The
directory itself is preserved so subsequent writes succeed without
needing `mkdir`.

The `models/` subdirectory is **deliberately preserved** — it holds
download-on-demand model artifacts (e.g. the ~34 MB `xyac_model.ubj`)
that are expensive to re-fetch. Clearing the *data* cache should not force
a model re-download; delete `<cache_dir>/models/` by hand to drop those.

**Example**

```python
from sportsdataverse.nfl import clear_cache, load_nfl_pbp
clear_cache()
pbp = load_nfl_pbp(seasons=[2024])

# Pair with a cache-mode switch

from sportsdataverse.nfl import clear_cache, update_config
update_config(cache_mode="filesystem")
# ... lots of cached calls accumulate parquet files on disk ...
clear_cache()  # wipe disk + memory together
```

### compose_counting_projection {#compose_counting_projection}

`compose_counting_projection(rate_proj: 'pl.DataFrame', avail_proj: 'pl.DataFrame', *, rate_col: 'str' = 'proj_rate', volume_col: 'str' = 'proj_volume') -> 'pl.DataFrame'`

Compose skill and availability into a counting projection.

The **only** place skill (rate x volume) and availability meet:
`proj_counting = rate * volume * proj_availability`, joined on
`player_id` (dtype-asserted).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `rate_proj` | `pl.DataFrame` |  | Skill projection carrying `player_id` + `rate_col` + `volume_col`. |
| `avail_proj` | `pl.DataFrame` |  | Availability projection carrying `player_id` + `proj_availability`. |
| `rate_col` | `str` | `'proj_rate'` | Rate column name in `rate_proj`. |
| `volume_col` | `str` | `'proj_volume'` | Volume column name in `rate_proj`. |

**Returns**

`rate_proj` columns plus `proj_availability` and `proj_counting:Float64`.

| col_name | type | description |
|---|---|---|
| `player_id` | character | nflverse gsis player id (character join key; asserted Utf8 on both sides of the join). |
| `proj_rate` | double | Projected per-opportunity rate carried through from the skill projection (rate_col). |
| `proj_volume` | double | Projected opportunity volume carried through from the skill projection (volume_col). |
| `proj_availability` | double | Projected availability rate in [0, 1] from nfl_availability_projection. |
| `proj_counting` | double | Composed counting projection - proj_rate x proj_volume x proj_availability (the only place skill and availability meet). |

**Example**

```python
import polars as pl
from sportsdataverse.nfl.nfl_availability import compose_counting_projection
out = compose_counting_projection(rate_frame, avail_frame)
```

### efficiency_ratings {#efficiency_ratings}

`efficiency_ratings(plays: 'pl.DataFrame', *, config: 'RatingsConfig | None' = None) -> 'pl.DataFrame'`

One row per team: opponent-adjusted offense/defense EPA per play.

Filters `plays` to competitive non-special-teams scrimmage plays
(`special != 1`, `qb_kneel != 1`, `qb_spike != 1`,
`min_competitive_wp <= wp <= max_competitive_wp`, non-null
`epa`/`posteam`/`defteam`) and fits
`opponent_adjusted_ridge` on `epa`. Callers pass an already
as-of-date-filtered frame (the public `nfl_ratings` entry point does
the date filter) -- this function is pure.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | An `load_nfl_pbp`-schema frame carrying `game_id`, `posteam`, `defteam`, `home_team`, `epa`, `wp`, `special`, `qb_kneel`, `qb_spike`. |
| `config` | `RatingsConfig \| None` | `None` | Tuning knobs (`ridge_lambda` + the competitive-`wp` window); defaults to `RatingsConfig`. |

**Returns**

One row per `team_id` (Utf8) with `adj_off_epa` / `adj_def_epa` / `adj_net` (Float64, `adj_net = adj_off_epa - adj_def_epa`) and `games` (Int64). Zero-row, correctly-typed on empty/fully-filtered input.

**Example**

```python
from sportsdataverse.nfl.nfl_ratings import efficiency_ratings
ratings = efficiency_ratings(pbp)
ratings.sort("adj_net", descending=True).head()
```

### env_adjusted_make_prob {#env_adjusted_make_prob}

`env_adjusted_make_prob(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Add `base_make_prob` + environment-adjusted `exp_make_prob`.

`exp_make_prob = sigmoid(logit(base) + b_wind*wind + b_temp*(temp-baseline)
+ b_alt*altitude_kft)` with coefficients from
`sportsdataverse.nfl.nfl_scheme_constants.ENVIRONMENT_FG_COEF` and
altitude from `STADIUM_ALTITUDE[home_team]`.  Dome / closed-roof kicks
(and missing readings) are treated as neutral (wind 0, temp = baseline).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | FG-attempt rows with `yardline_100` / `roof` / `temp` / `wind` / `home_team` (+ `season` or `era0..era4` / `fg_roof`). |

**Returns**

The input plus `base_make_prob` and `exp_make_prob` (Float64).

| col_name | type | description |
|---|---|---|
| `base_make_prob` | double | Shipped fg_model make probability (with nfl4th long-kick clamps applied). |
| `exp_make_prob` | double | Environment-adjusted make probability (logit shift for long-kick clamp correction, wind, temperature and altitude). |

**Example**

```python
import polars as pl
from sportsdataverse.nfl.nfl_kicker_rating import env_adjusted_make_prob
fg = pl.read_parquet("tests/fixtures/nfl_scheme/fg_attempts_2019_2023.parquet")
out = env_adjusted_make_prob(fg)
print(out.select("base_make_prob", "exp_make_prob").describe())
```

### espn_nfl_teams {#espn_nfl_teams}

`espn_nfl_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nfl_teams - look up NFL teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams for the requested league. This function caches by default, so if you want to refresh the data, use the command sportsdataverse.nfl.espn_nfl_teams.clear_cache().

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | character | Team abbreviation. |
| `team_alternate_color` | character | Alternate team color. |
| `team_color` | character | Primary team color. |
| `team_display_name` | character | Full team display name. |
| `team_id` | character | ESPN team id. |
| `team_is_active` | logical | TRUE if the team is currently active. |
| `team_is_all_star` | logical | TRUE if the row represents an All-Star team. |
| `team_location` | character | Team location / school name. |
| `team_logos` | integer | Team logo metadata. |
| `team_name` | character | Team nickname. |
| `team_nickname` | character | Team nickname label. |
| `team_short_display_name` | character | Short team display name. |
| `team_slug` | character | Team slug for the stat row. |
| `team_uid` | character | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |

**Example**

```python
from sportsdataverse.nfl import espn_nfl_teams
teams = espn_nfl_teams()
teams.shape

# Pandas round-trip

teams_pd = espn_nfl_teams(return_as_pandas=True)
teams_pd[["team_abbreviation", "team_display_name"]].head()

# Force a refresh after upstream ESPN updates

espn_nfl_teams.cache_clear()  # underlying lru_cache
teams = espn_nfl_teams()
```

### fg_make_probability {#fg_make_probability}

`fg_make_probability(yardline_100: 'np.ndarray', fg_roof: 'np.ndarray', era: 'np.ndarray') -> 'Optional[np.ndarray]'`

Predict FG make probability from the bundled `fg_model` (public wrapper).

Thin supported alias over the private underscore-prefixed helper so downstream
consumers (e.g. the kicker-rating spine) reuse the shipped model through a
public import instead of a private reach.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `yardline_100` | `ndarray` |  | Kick spot (yards from the opponent end zone); the attempt distance is `yardline_100 + 18`. |
| `fg_roof` | `ndarray` |  | 1.0 when `roof == "outdoors"` else 0.0, per kick. |
| `era` | `ndarray` |  | `(n, 5)` one-hot era matrix (`era0`..`era4`, season cuts 2001/2005/2013/2017). |

**Returns**

Make probabilities (with nfl4th's long-kick clamps), or `None` when the bundled model is unavailable.

**Example**

```python
import numpy as np
from sportsdataverse.nfl.nfl_fourth_down import fg_make_probability
p = fg_make_probability(
    np.array([30.0]), np.array([1.0]),
    np.array([[0.0, 0.0, 0.0, 0.0, 1.0]]),
)
print(p)
```

### fit_nfl_field_position_ep {#fit_nfl_field_position_ep}

`fit_nfl_field_position_ep(pbp: 'pl.DataFrame', *, exclude_garbage: 'bool' = True) -> 'pl.DataFrame'`

Fit the NFL EP-by-starting-yardline curve from released `espn_nfl_pbp` plays.

Extracts one row per drive (starting yard line from the offense's own
goal, realized drive points) and fits the monotone curve with
`sportsdataverse.cfb.cfb_field_position.fit_field_position_ep` --
the same estimator and target the college curve uses. This is how the
bundled artifact was produced; re-run it on newer seasons to refresh it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | plays in the released `espn_nfl_pbp` shape (any number of seasons concatenated). Needs the drive fields (`drive.id`, `drive.result`, `drive.start.yardLine`), `homeTeamId`, `period` and `start.pos_team.id` / `start.def_pos_team.id`. |
| `exclude_garbage` | `bool` | `True` | drop drives that start in garbage time. |

**Returns**

`yardline_own: Int64 (1..99), ep: Float64` -- monotone non-decreasing. Empty input returns a zero-row frame.

**Example**

```python
import polars as pl
from sportsdataverse.nfl import fit_nfl_field_position_ep
pbp = pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")
curve = fit_nfl_field_position_ep(pbp)
curve.write_parquet("nfl_field_position_ep.parquet")
```

### get_2pt_probs {#get_2pt_probs}

`get_2pt_probs(pbp_df: "Union[pl.DataFrame, 'pd.DataFrame']") -> 'pd.DataFrame'`

The PAT-vs-2pt decision surface for post-touchdown states (CFB-shaped).

The NFL twin of `sportsdataverse.cfb.cfb_two_point.get_2pt_probs`. It
runs the same three-outcome enumeration `get_2pt_wp` uses, but returns
the decision columns instead of folding them into `wp_td`. The two option

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` | `Union[DataFrame, 'DataFrame']` |  | Post-touchdown states in nflverse column space (the same inputs `get_4th_down_probs` takes; `score_differential` is the scoring team's lead **after** the six points). Prepared frames are accepted as-is. |

**Returns**

A pandas frame with `go_index` plus `two_pt_wp`, `xp_wp`, `prob_2pt`, `two_pt_recommendation` (`"go_for_2"` iff `two_pt_wp > xp_wp` else `"kick_xp"`) and `two_pt_wp_diff` (`two_pt_wp - xp_wp`). All NaN / null when the models are unavailable.

**Example**

```python
from sportsdataverse.nfl.nfl_fourth_down import get_2pt_probs
out = get_2pt_probs(touchdown_states)
print(out[["two_pt_wp", "xp_wp", "two_pt_recommendation"]].head())
```

### get_2pt_wp {#get_2pt_wp}

`get_2pt_wp(pbp_df: "Union[pl.DataFrame, 'pd.DataFrame']") -> 'pd.DataFrame'`

Win probability of the PAT-vs-2pt choice after a touchdown (nfl4th `get_2pt_wp`).

For each row, scores the post-touchdown state under three scoring outcomes
(0 / 1 / 2 added points) from the kicking-off team's ensuing-drive WP, and
combines them with the 2-pt conversion probability (`two_pt_model`) and the
PAT make probability (the FG model at `yardline_100 = 15`) into `wp_td` —
the better of go-for-2 and kick-the-PAT.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` | `Union[DataFrame, 'DataFrame']` |  | Play-by-play frame (polars or pandas) of post-touchdown states, already carrying the prepared state columns (see module docstring). |

**Returns**

A pandas frame with `go_index`, `yardline_100` (always 0) and `wp_td`. `wp_td` is NaN when the WP / 2-pt models are unavailable.

**Example**

```python
from sportsdataverse.nfl.nfl_fourth_down import get_2pt_wp
out = get_2pt_wp(touchdown_states)
print(out[["go_index", "wp_td"]].head())
```

### get_4th_down_probs {#get_4th_down_probs}

`get_4th_down_probs(pbp_df: "Union[pl.DataFrame, 'pd.DataFrame']") -> 'pd.DataFrame'`

Full 4th-down decision surface (nfl4th `add_4th_probs`) + recommendation.

Runs `get_go_wp`, `get_fg_wp`, `get_punt_wp` on the
fourth-down rows and adds the combined option columns plus:

* `go_boost` -- nfl4th's headline number: `100 * (go_wp - max(fg_wp,
  punt_wp))` in percentage points (a NaN `punt_wp` is treated as 0).
* `fourth_down_recommendation` -- the max-WP choice among `{go, punt,
  field_goal}` (NaN options are excluded).
* `go_wp_diff` / `punt_wp_diff` / `fg_wp_diff` -- each option's WP minus
  the recommended option's WP (the recommended option's diff is 0, the others
  <= 0).  NaN where the option WP is NaN.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` | `Union[DataFrame, 'DataFrame']` |  | Play-by-play frame (polars or pandas) of fourth-down situations (the nflverse-shape output of `load_nfl_pbp`; see module docstring for required columns). |

**Returns**

A pandas copy of `pbp_df` with the decision columns added. Empty input returns the input plus empty decision columns.

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_fourth_down import get_4th_down_probs

pbp = load_nfl_pbp([2023])
fourth = pbp.filter((pl.col("down") == 4) & pl.col("yardline_100").is_not_null())
out = get_4th_down_probs(fourth)
print(out[["go_wp", "punt_wp", "fg_wp", "go_boost", "fourth_down_recommendation"]].head())
```

### get_config {#get_config}

`get_config() -> 'NflConfig'`

Return the live `NflConfig` singleton.

The same object is returned on every call; mutate via `update_config`
rather than reassigning fields directly so future hooks (e.g. logging
on config change) have a single choke point.

**Example**

```python
from sportsdataverse.nfl import get_config
cfg = get_config()
print(cfg.cache_mode, cfg.cache_duration, cfg.cache_dir)

# Pair with ``update_config`` to verify a change took effect

from sportsdataverse.nfl import update_config, get_config
update_config(cache_mode="off")
assert get_config().cache_mode == "off"
```

### get_fg_wp {#get_fg_wp}

`get_fg_wp(pbp_df: "Union[pl.DataFrame, 'pd.DataFrame']") -> 'pd.DataFrame'`

Expected win probability of attempting a field goal (nfl4th `get_fg_wp`).

The make probability comes from the self-trained `fg_model` (a
`binary:logistic` XGBoost re-train of the original mgcv GAM, features
`[yardline_100, fg_roof, fg_era]`), shrunk by 0.9 for kicks at/beyond
`yardline_100 = 38` and zeroed at/beyond `yardline_100 = 45`
(>= ~63-yard kicks).  The made-FG state (opponent receives a touchback
kickoff at the 25, kicking team +3) and the missed-FG state (opponent takes
over 8 yards back of the spot, capped at the 80) are each scored with win
probability; `fg_wp = make_prob * make_wp + (1 - make_prob) * miss_wp`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` | `Union[DataFrame, 'DataFrame']` |  | Play-by-play frame (polars or pandas) of fourth-down situations. |

**Returns**

A pandas copy of `pbp_df` plus `fg_make_prob`, `make_fg_wp`, `miss_fg_wp` and `fg_wp` (from the kicking team's perspective). All four are NaN when the FG model or WP model is unavailable.

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_fourth_down import get_fg_wp

pbp = load_nfl_pbp([2023])
fourth = pbp.filter((pl.col("down") == 4) & pl.col("yardline_100").is_not_null())
out = get_fg_wp(fourth)
print(out[["fg_make_prob", "fg_wp"]].head())
```

### get_go_wp {#get_go_wp}

`get_go_wp(pbp_df: "Union[pl.DataFrame, 'pd.DataFrame']") -> 'pd.DataFrame'`

Expected win probability of going for it on 4th down (nfl4th `get_go_wp`).

The fd_model 76-class yards-gained distribution is expanded per play; each
outcome's hypothetical post-play game state (turnover-on-downs flip, +6
touchdown with the PAT/2-pt branch routed through `get_2pt_wp`, 6-second
runoff, goal-to-go distance shrink) is scored with win probability and the
end-of-game kneel-out clamps are applied; the option value is the
prob-weighted WP.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` | `Union[DataFrame, 'DataFrame']` |  | Play-by-play frame (polars or pandas) of fourth-down situations carrying the prepared state columns (see module docstring). The frame is prepared internally if it lacks the derived columns. |

**Returns**

A pandas copy of `pbp_df` plus `go_wp` (prob-weighted WP of going for it), `first_down_prob` (P(conversion)), `wp_succeed` (mean WP over conversion outcomes) and `wp_fail` (mean WP over failure outcomes). All are NaN when the fourth-down / WP models are unavailable (`FD_MODEL_AVAILABLE` / `WP_MODEL_AVAILABLE`).

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_fourth_down import get_go_wp

pbp = load_nfl_pbp([2023])
fourth = pbp.filter((pl.col("down") == 4) & pl.col("yardline_100").is_not_null())
out = get_go_wp(fourth)
print(out[["go_wp", "first_down_prob"]].head())
```

### get_punt_wp {#get_punt_wp}

`get_punt_wp(pbp_df: "Union[pl.DataFrame, 'pd.DataFrame']") -> 'pd.DataFrame'`

Expected win probability of punting on 4th down (nfl4th `get_punt_wp`).

The punt landing distribution (`punt_data`: `yardline_after` / `pct` /
`muff` per `yardline_100`) is joined per play; possession is flipped to
the receiving team, with return-touchdown (`yardline_after == 100`) and muff
(`muff == 1`) recoveries flipping the ball back to the punting team; each
landing spot's ensuing-drive WP is scored and the option value is the
prob-weighted WP from the punting team's perspective.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` | `Union[DataFrame, 'DataFrame']` |  | Play-by-play frame (polars or pandas) of fourth-down situations. |

**Returns**

A pandas copy of `pbp_df` plus `punt_wp`. `punt_wp` is NaN where the punt distribution has no support for the play's `yardline_100` (inside the punting team's own 31, where the table is empty — matching the R reference's left-join NA behavior) or when the WP model is unavailable.

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_fourth_down import get_punt_wp

pbp = load_nfl_pbp([2023])
fourth = pbp.filter((pl.col("down") == 4) & pl.col("yardline_100").is_not_null())
out = get_punt_wp(fourth)
print(out[["punt_wp"]].head())
```

### opponent_adjusted_ridge {#opponent_adjusted_ridge}

`opponent_adjusted_ridge(plays: 'pl.DataFrame', *, off_col: 'str', def_col: 'str', home_col: 'str', resp_col: 'str', lam: 'float', penalize_home: 'bool' = False, hfa_col: 'str | None' = None) -> 'tuple[pl.DataFrame, float, float]'`

Ridge-regress `resp_col` on offense + defense team indicators + HFA.

League-agnostic (column names are arguments): builds the full
offense/defense-indicator + intercept + home design and solves the
ridge normal equations `beta = (X'X + lam*R)^-1 X'y`. Only team
coefficients are penalised; the intercept (and, unless
`penalize_home`, the home term) is free. Moved (T7.2) from
`sportsdataverse.nfl.nfl_ratings`. Callers: NFL ratings, and CFB
adjusted EPA (`cfb_adjusted_epa._fit_team_strengths`, via `hfa_col`);
`cfb_ratings` uses the different `dropped_level_ridge`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | One row per play. Rows with a null `off_col` / `def_col` / `resp_col` must be filtered by the caller. |
| `off_col` | `str` |  | Column naming the offense (possession) team. |
| `def_col` | `str` |  | Column naming the defense team. |
| `home_col` | `str` |  | Column naming the home team (HFA indicator is `off_col == home_col`). |
| `resp_col` | `str` |  | Numeric response column (e.g. `epa`). |
| `lam` | `float` |  | Ridge penalty applied to the team coefficients. |
| `penalize_home` | `bool` | `False` | Also penalise the home-field coefficient (default False). |
| `hfa_col` | `str \| None` | `None` | Numeric column used as the home regressor as-is (e.g. CFB's `+1` home offense / `0` neutral site / `-1` away), in place of the `off_col == home_col` indicator, which then goes unused. Must not contain nulls (raises `ValueError`). |

**Returns**

A `(frame, intercept, home_coef)` tuple: `frame` has one row per team (`team_id` Utf8, `off_coef` / `def_coef` Float64); `intercept` is the league baseline; `home_coef` the fitted HFA in response units. Zero-row frame + `(0.0, 0.0)` on empty input.

**Example**

```python
from sportsdataverse.nfl.nfl_ratings import opponent_adjusted_ridge
frame, intercept, hfa = opponent_adjusted_ridge(
    plays, off_col="posteam", def_col="defteam",
    home_col="home_team", resp_col="epa", lam=200.0,
)
frame.sort("off_coef", descending=True).head()
```

### playcall_features {#playcall_features}

`playcall_features(pbp: 'pl.DataFrame', participation: 'Optional[pl.DataFrame]' = None) -> 'pl.DataFrame'`

Build the play-call feature frame (one row per offensive run/pass play).

Filters to plays with `pass == 1` or `rush == 1`, derives the 5-class
`family` label (scramble > deep/short pass > inside/outside run), and
left-joins the optional participation frame for personnel counts.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | nflverse-format pbp with the pre-snap feature columns + `pass` / `rush` / `qb_scramble` / `pass_length` / `run_location` / `run_gap` and `xpass`. |
| `participation` | `Optional[DataFrame]` | `None` | Optional nflverse participation frame with `game_id` / `play_id` / `offense_personnel`. |

**Returns**

Keys + `PLAYCALL_FEATURE_ORDER` columns + `family` + `is_pass`. Personnel columns are null (`has_participation=0`) when no participation row matches.

| col_name | type | description |
|---|---|---|
| `game_id` | character | nflverse game identifier (Utf8 join key). |
| `play_id` | integer | nflverse play identifier within the game (Int64 join key). |
| `season` | integer | Season of the play. |
| `week` | integer | Week of the play. |
| `posteam` | character | Offense (possession) team abbreviation. |
| `down` | double | Down (1-4) at the snap. |
| `ydstogo` | double | Yards to go for a first down. |
| `yardline_100` | double | Yards from the opponent end zone at the snap. |
| `score_differential` | double | Offense score minus defense score at the snap. |
| `half_seconds_remaining` | double | Seconds remaining in the half. |
| `game_seconds_remaining` | double | Seconds remaining in the game. |
| `wp` | double | Start-of-play win probability for the offense. |
| `shotgun` | double | 1 when the offense lined up in shotgun. |
| `no_huddle` | double | 1 when the play was run without a huddle. |
| `xpass` | double | Shipped nflfastR-parity expected-dropback probability for the play. |
| `n_rb` | double | Running backs in the offensive personnel grouping (null without participation data). |
| `n_te` | double | Tight ends in the offensive personnel grouping (null without participation data). |
| `n_wr` | double | Wide receivers in the offensive personnel grouping (null without participation data). |
| `has_participation` | integer | 1 when a participation row matched the play, else 0. |
| `family` | character | 5-class play-call label (inside_run, outside_run, short_pass, deep_pass, scramble). |
| `is_pass` | integer | 1 when the play was a pass (including scrambles), else 0. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.ep_wp import calculate_xpass
from sportsdataverse.nfl.nfl_playcall import playcall_features
feat = playcall_features(calculate_xpass(load_nfl_pbp([2023])))
print(feat["family"].value_counts())
```

### player_usage_efficiency {#player_usage_efficiency}

`player_usage_efficiency(player_stats: 'pl.DataFrame', *, as_of_week: 'int', era: 'str' = 'modern') -> 'pl.DataFrame'`

Per-player as-of usage + efficiency with empirical-Bayes shrinkage.

Aggregates one season of week-level player stats over weeks strictly
before `as_of_week` (the leakage boundary), then shrinks every usage
(per-game attempts / carries / targets) and efficiency (yards + TDs per
opportunity) stat toward its position prior:
`(n * player_value + kappa * prior) / (n + kappa)` with `n` = games
played and `kappa` the stat family's fitted shrinkage.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_stats` | `DataFrame` |  | One season of `load_nfl_player_stats()` rows (columns `player_id`, `position`, `recent_team`, `week`, `attempts`, `passing_yards`, `passing_tds`, `carries`, `rushing_yards`, `rushing_tds`, `targets`, `receiving_yards`, `receiving_tds`). |
| `as_of_week` | `int` |  | Only weeks `< as_of_week` are used. |
| `era` | `str` | `'modern'` | Constants era key (supplies kappas + position priors). |

**Returns**

One row per `player_id` (Utf8) whose position has a prior table: `position` / `team_id` (Utf8, latest team), `games` (Int64), `exp_attempts` / `exp_carries` / `exp_targets` (Float64, shrunk per-game usage), `ypa` / `ypc` / `ypt` / `pass_td_rate` / `rush_td_rate` / `rec_td_rate` (Float64, shrunk per-opportunity efficiency). Zero-row, correctly-typed on empty input.

**Example**

```python
import polars as pl
import sportsdataverse.nfl as nfl
stats = nfl.load_nfl_player_stats().filter(pl.col("season") == 2023)
usage = nfl.player_usage_efficiency(stats, as_of_week=10)
usage.sort("exp_attempts", descending=True).head()
```

### predict_margin {#predict_margin}

`predict_margin(home_adj_net: 'float', away_adj_net: 'float', neutral: 'bool', *, era: 'str' = 'modern') -> 'float'`

Expected home scoring margin from two net ratings.

`points_per_net * (home_adj_net - away_adj_net)` plus the era HFA on
non-neutral fields.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_adj_net` | `float` |  | Home team's `adj_net` (EPA/play units). |
| `away_adj_net` | `float` |  | Away team's `adj_net`. |
| `neutral` | `bool` |  | True drops the home-field advantage. |
| `era` | `str` | `'modern'` | Constants era key (default `"modern"`). |

**Returns**

Expected home margin in points (positive = home favored).

**Example**

```python
from sportsdataverse.nfl.nfl_market import predict_margin
predict_margin(0.10, -0.05, False)
```

### predict_total {#predict_total}

`predict_total(home_adj_off: 'float', home_adj_def: 'float', away_adj_off: 'float', away_adj_def: 'float', *, era: 'str' = 'modern') -> 'float'`

Expected combined point total from the four efficiency components.

`avg_total + total_scale * (home_adj_off + away_adj_def + away_adj_off +
home_adj_def)`. The four ratings are **summed** because each side's
scoring rises with its own offense and with the opponent's EPA-*allowed*
(`adj_def` is lower = better defense) -- same semantics as the shipped
CFB analog. (The plan text wrote this with a minus; that sign flips a
good defense into raising the total, so the analog's sum is used.)

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_adj_off` | `float` |  | Home `adj_off_epa`. |
| `home_adj_def` | `float` |  | Home `adj_def_epa` (lower = better defense). |
| `away_adj_off` | `float` |  | Away `adj_off_epa`. |
| `away_adj_def` | `float` |  | Away `adj_def_epa`. |
| `era` | `str` | `'modern'` | Constants era key. |

**Returns**

Expected combined total in points.

**Example**

```python
from sportsdataverse.nfl.nfl_market import predict_total
predict_total(0.10, -0.02, 0.05, 0.01)
```

### pressure_pairs {#pressure_pairs}

`pressure_pairs(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Per (season, off_team, def_team) dropbacks + pressures (matchup grid).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the matchup aggregate. |
| `off_team` | character | Offense team abbreviation. |
| `def_team` | character | Defense team abbreviation. |
| `dropbacks` | integer | Offense dropbacks in the matchup. |
| `pressures` | integer | Sacks plus QB hits in the matchup. |

### reset_config {#reset_config}

`reset_config() -> 'NflConfig'`

Reset the active config to its env-var-derived defaults.

Convenience for tests / interactive sessions that want to undo a chain
of `update_config()` calls without restarting the interpreter.

**Example**

```python
from sportsdataverse.nfl import update_config, reset_config
update_config(cache_mode="off", timeout=5)
# ... do work ...
reset_config()  # back to env-derived defaults
```

### scoreboard_event_parsing {#scoreboard_event_parsing}

`scoreboard_event_parsing(event)`

Normalize one ESPN scoreboard `event` into a flatter shape.

Splits the competitors list into `home` / `away` siblings, hoists
notes / broadcast metadata onto the competition root, and drops the
fields the schedule helper does not need (`odds`, `leaders`,
`geoBroadcasts`, etc.). Used internally by
`espn_nfl_schedule`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` | `Dict` |  | A single `events[i]` dict from the ESPN scoreboard endpoint. |

**Returns**

The mutated event dict with normalized `home` / `away` / broadcast keys.

**Example**

```python
from sportsdataverse.dl_utils import download
from sportsdataverse.nfl.nfl_schedule import scoreboard_event_parsing
url = "http://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
payload = download(url=url).json()
for ev in payload.get("events", []):
    scoreboard_event_parsing(ev)
    ev["competitions"][0]["home"]["abbreviation"]
```

### shield_nfl_pbp {#shield_nfl_pbp}

`shield_nfl_pbp(game_detail: 'Optional[Dict[str, Any]]' = None, shield_game_id: 'Optional[str]' = None, *, enrich: 'bool' = True, context: 'Optional[Dict[str, Any]]' = None, game_id: 'Optional[str]' = None) -> 'pl.DataFrame'`

Build one NFL game's nflverse-shape play-by-play from Shield, at ANY game phase.

The live entry point: the same parser `build_pbp` runs on the archived
`nfl/raw` finals, plus the four things a game still being played needs — the
in-progress drive's possession, game-outcome columns held null until the feed says
FINAL, a next-snap row from `summary`, and provisional rows flagged (see
`sportsdataverse.nfl.shield_pbp.live`). Safe to poll: pass the payload you
already have via *game_detail* (no network), or a *shield_game_id* to fetch it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_detail` | `Optional[Dict[str, Any]]` | `None` | A Shield `experience/v2/gamedetails` payload (the raw body, or a `{"data": ...}` envelope). Takes precedence over *shield_game_id*, so tests and pollers that already hold a payload never touch the network. |
| `shield_game_id` | `Optional[str]` | `None` | Shield game uuid, fetched via `sportsdataverse.nfl.nfl_game_details_v2` with `include_drive_chart=True, return_parsed=False` when *game_detail* is None. |
| `enrich` | `bool` | `True` | Run `sportsdataverse.nfl.ep_wp.enrich_nfl_pbp` on the result (default True) for the `nfl_model_pbp` EP/EPA/WP/WPA/CP/CPOE columns. Pass False for the base frame only (no model loads). |
| `context` | `Optional[Dict[str, Any]]` | `None` | Game context `{"roof": ..., "spread_line": ..., "total_line": ...}` the Shield feed omits. Unset fields fall back to the nflverse schedule row for this game, then to `live.DEFAULT_CONTEXT` (`outdoors` / 2.5 / 55.5, the same default the ESPN processor uses). |
| `game_id` | `Optional[str]` | `None` | Override the nflverse game_id (computed from the payload when None). |

**Returns**

A polars DataFrame, one row per play (plus, while `summary.phase` is `INGAME`, one current-situation row), carrying the `nfl_model_pbp` columns — the `build_pbp` base frame, the EP/WP enrichment when *enrich* is True, and: | col_name | type | description | |----------|------|-------------| | `live_phase` | `str` | The payload's `summary.phase`: `PREGAME`, `INGAME`, `HALFTIME`, `FINAL` or `FINAL_OVERTIME`. | | `is_play` | `int` | `1` for a real play; `0` for the feed's `GAME_START` / `END_QUARTER` / `END_GAME` markers and the current-situation row. | | `provisional` | `int` | `1` when the feed has not closed the play (`playEndTime` null) and it is in the trailing run of such plays of a non-final game — its text, yardage and stats may still change. Always `0` on a final game. | `home_score` / `away_score` / `result` are null until the game is final. The current-situation row is not inert once *enrich* is True: it is the next state, so it also completes the **previous** play's lead-diff columns (`epa`, `qb_epa`, `wpa`, `vegas_wpa`, the `total_*` running sums). That play is usually still `provisional`, so those values can move on the next poll. A payload Shield has not populated a drive chart for (every scheduled game before kickoff) returns a zero-row frame carrying only the three live columns — check `df.is_empty()` before selecting anything else.

**Example**

```python
import polars as pl
from sportsdataverse.nfl import shield_nfl_pbp

df = shield_nfl_pbp(shield_game_id="a9a8944e-4feb-11f1-abca-2c54536568a9")
df.filter(pl.col("is_play") == 0).select("posteam", "down", "ydstogo", "wp")
```

### shield_to_espn_summary {#shield_to_espn_summary}

`shield_to_espn_summary(game_detail: 'Mapping[str, Any]', idmap_row: 'Mapping[str, Any]', *, parsed: 'Optional[pl.DataFrame]' = None, odds: 'Optional[Mapping[str, Any]]' = None, player_stats: 'Optional[Mapping[str, Any]]' = None, team_stats: 'Optional[Mapping[str, Any]]' = None) -> 'Tuple[Dict[str, Any], List[str]]'`

Project one Shield game (any phase) onto an ESPN-summary-shaped dict.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_detail` | `Mapping[str, Any]` |  | A Shield `experience/v2/gamedetails` payload (raw body or a `{"data": ...}` envelope) -- the same object `sportsdataverse.nfl.shield_pbp.build.shield_nfl_pbp` consumes. |
| `idmap_row` | `Mapping[str, Any]` |  | The game's pre-kickoff id-map row (`sportsdataverse.football.sources.idmap.GAME_SCHEMA`): `espn_event_id`, `home_espn_team_id` and `away_espn_team_id` are required; the optional `home_team` / `away_team` sub-dicts supply the era-correct `espn_abbr`. |
| `parsed` | `Optional[DataFrame]` | `None` | The frame `shield_nfl_pbp(game_detail, enrich=False)` already produced. Built here when None -- pass it to parse the payload once for both projections. |
| `odds` | `Optional[Mapping[str, Any]]` | `None` | `{gameSpread, overUnder, homeFavorite, gameSpreadAvailable}` (the stored closing line, `sportsdataverse.football.sources.idmap._odds_override_from_row`). Becomes the summary's one-provider `pickcenter`. |
| `player_stats` | `Optional[Mapping[str, Any]]` | `None` | A Shield `/football/v2/stats/live/player-statistics/{gameId}` body. Becomes `boxscore.players` in ESPN's exact shape (ten categories, athletes carrying ESPN ids from the players crosswalk). Omitted -> the box stays empty and no ESPN athlete id is attached to any play. |
| `team_stats` | `Optional[Mapping[str, Any]]` | `None` | A Shield `/football/v2/stats/live/team-statistics/{gameId}` body. Becomes `boxscore.teams` -- the authoritative countable team totals `NFLPlayProcess.create_box_score` prefers over its play-by-play derivation. |

**Returns**

`(summary, notes)`. | item | type | description | |---|---|---| | summary | dict | An ESPN-summary-shaped payload: `header` (season/week/competitions/competitors/status), `drives.previous` (+ `drives.current` while the game is live), `gameInfo`, `pickcenter`, `boxscore` (filled when `player_stats`/`team_stats` are given) and passthrough arrays. Feed it to `espn_nfl_pbp(summary=)`. | | notes | list[str] | Adapter-side degradations worth surfacing in provenance: a missing `summary.timeouts` block, a missing `summary.homeTeam`/`awayTeam` team id, a PAT with no touchdown to fold into, plays outside the drive chart, and (pre-2014) play ids that do not join ESPN's own. |

**Example**

```python
import json
from sportsdataverse.nfl import NFLPlayProcess, shield_to_espn_summary

# any Shield gamedetails body -- here the copy nfl-raw keeps
with open("nfl/raw/2025/2025_07_LA_JAX.json") as fh:
    game = json.load(fh)
row = {"espn_event_id": "401772635", "home_espn_team_id": "30", "away_espn_team_id": "14"}
summary, notes = shield_to_espn_summary(game, row)
proc = NFLPlayProcess(gameId=401772635, join_participants=False)
proc.espn_nfl_pbp(summary=summary)
result = proc.run_processing_pipeline()
```

### special_teams_ratings {#special_teams_ratings}

`special_teams_ratings(plays: 'pl.DataFrame', *, config: 'RatingsConfig | None' = None) -> 'pl.DataFrame'`

One row per team: opponent-adjusted special-teams EPA per play.

Reuses `opponent_adjusted_ridge` (no forked solver) restricted to
`special == 1` plays with `resp_col="epa"`; `adj_st_epa` is the
`off_coef` (the special-teams unit acting as "offense" on the play).
Teams appearing anywhere in `plays` but on no special-teams play get
the documented neutral fill `adj_st_epa = 0.0`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | An `load_nfl_pbp`-schema frame carrying `posteam`, `defteam`, `home_team`, `epa`, `special`. Not pre-filtered -- this function selects the ST plays itself. |
| `config` | `RatingsConfig \| None` | `None` | Tuning knobs (only `ridge_lambda` is consulted); defaults to `RatingsConfig`. |

**Returns**

One row per `team_id` (Utf8) with `adj_st_epa` (Float64). Zero-row, correctly-typed when `plays` is empty.

**Example**

```python
from sportsdataverse.nfl.nfl_ratings import special_teams_ratings
st = special_teams_ratings(pbp)
st.sort("adj_st_epa", descending=True).head()
```

### team_game_pace {#team_game_pace}

`team_game_pace(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Per team-game pace + pass-rate-over-expected.

`sec_per_play` is the per-drive elapsed `game_seconds_remaining`
divided by drive plays, averaged over the team's offensive drives
(kneels / spikes / no_plays excluded).  Neutral = `wp` in [0.2, 0.8]
and `half_seconds_remaining` > 120.  `proe` is the mean `pass_oe`
over dropbacks.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | nflverse-format pbp with `game_id` / `season` / `week` / `posteam` / `drive` / `play_type` / `qb_dropback` / `pass_oe` / `game_seconds_remaining` / `wp` / `half_seconds_remaining`. |

**Returns**

One row per `(game_id, season, week, posteam)` with `off_plays`, `sec_per_play`, `neutral_plays`, `neutral_sec_per_play`, `proe`. Empty input yields a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `game_id` | character | nflverse game identifier (Utf8 join key). |
| `season` | integer | Season of the game. |
| `week` | integer | Week of the game. |
| `posteam` | character | Offense team abbreviation. |
| `off_plays` | integer | Offensive plays in the game (kneels, spikes and no_plays excluded). |
| `sec_per_play` | double | Mean over the team's drives of elapsed game clock divided by drive plays. |
| `neutral_plays` | integer | Offensive plays in neutral situations (wp in [0.2, 0.8], over 2 minutes left in the half). |
| `neutral_sec_per_play` | double | sec_per_play computed on neutral-situation plays only. |
| `proe` | double | Mean pass_oe over the team's dropbacks in the game (percentage points). |
| `dropbacks` | integer | Dropbacks with a non-null pass_oe (the proe denominator). |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_gamescript import team_game_pace
pace = team_game_pace(load_nfl_pbp([2023]))
print(pace.sort("sec_per_play").head())
```

### team_name_fn {#team_name_fn}

`team_name_fn(expr: 'pl.Expr') -> 'pl.Expr'`

Fold historical/relocated team codes onto their current abbreviation.

Verbatim port of nflfastR's `team_name_fn` (a plain
`stringr::str_replace_all` over a 10-entry named vector). Operates as a
**substring** replace (not a full-value lookup) so it also fixes
embedded codes like `"SD 49" -> "LAC 49"` on yard-line columns. The
10 from-codes are disjoint from all of their to-values, so the order of
the 10 sequential replacements does not matter (verified in
`tests.nfl.test_nfl_clean`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `expr` | `Expr` |  | A `polars.Expr` over a Utf8 column (e.g. `pl.col("posteam")`). |

**Returns**

The same expression with every occurrence of the 10 historical codes replaced by their current-franchise code.

### team_pressure_rates {#team_pressure_rates}

`team_pressure_rates(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Per (season, team) raw pressure rates, both sides of the ball.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | nflverse-format pbp with `season` / `posteam` / `defteam` / `qb_dropback` / `sack` / `qb_hit`. |

**Returns**

Per `(season, team)`: `dropbacks_off`, `pressures_allowed`, `pressure_rate_allowed`, `dropbacks_def`, `pressures_generated`, `pressure_rate_generated`. Empty input yields a zero-row frame.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the aggregate. |
| `team` | character | Team abbreviation. |
| `dropbacks_off` | integer | Offensive dropbacks (qb_dropback plays). |
| `pressures_allowed` | integer | Sacks plus QB hits allowed on the team's own dropbacks. |
| `pressure_rate_allowed` | double | pressures_allowed / dropbacks_off. |
| `dropbacks_def` | integer | Opponent dropbacks faced on defense. |
| `pressures_generated` | integer | Sacks plus QB hits generated against opponent dropbacks. |
| `pressure_rate_generated` | double | pressures_generated / dropbacks_def. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_line_grades import team_pressure_rates
rates = team_pressure_rates(load_nfl_pbp([2023]))
print(rates.sort("pressure_rate_generated", descending=True).head())
```

### update_config {#update_config}

`update_config(**kwargs: 'object') -> 'NflConfig'`

Update the active config in place.

**Returns**

The (mutated) global config object, for chaining or inspection.

**Example**

```python
from sportsdataverse.nfl import update_config
update_config(cache_mode="filesystem", cache_duration=3600)

# Disable caching for development

update_config(cache_mode="off")

# Point cache at a custom directory

update_config(cache_dir="~/sdv-cache")
```

### win_prob_from_margin {#win_prob_from_margin}

`win_prob_from_margin(exp_margin: 'float', *, era: 'str' = 'modern') -> 'float'`

Home win probability from an expected margin (Gaussian margin model).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_margin` | `float` |  | Expected home margin in points. |
| `era` | `str` | `'modern'` | Constants era key (supplies `margin_sd`). |

**Returns**

`Phi(exp_margin / margin_sd)` in `[0, 1]`.

**Example**

```python
from sportsdataverse.nfl.nfl_market import win_prob_from_margin
win_prob_from_margin(3.0)
```
