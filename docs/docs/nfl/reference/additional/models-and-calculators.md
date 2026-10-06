---
title: "NFL — additional Python functions — Models and calculators: adjust_pressure–nfl_ratings"
sidebar_label: "Models and calculators: adjust_pressure–nfl_ratings"
sidebar_position: 11
description: "NFL — additional Python functions — Models and calculators: adjust_pressure–nfl_ratings — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Models and calculators: adjust_pressure–nfl_ratings

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

### calculate_completion_probability {#calculate_completion_probability}

`calculate_completion_probability(pbp_data: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Compute completion probability (CP) and CPOE for pass plays.

Mirrors nflfastR's `helper_add_cp_cpoe.R`.  Scores only intended pass
plays (where `air_yards` is not null); non-pass plays receive null in
the `cp` column.  When `complete_pass` is present,
`cpoe = 100 * (complete_pass - cp)` is also added — on nflfastR's
percentage-point scale (`add_cp` in `helper_add_cp_cpoe.R`).

Drops and recomputes any existing `cp` / `cpoe` columns.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_data` | `DataFrame` |  | nflverse-format play-by-play DataFrame. Required: `air_yards`, `season`, `ydstogo`, `down`, `posteam`, `home_team`. Optional: `roof`, `pass_location` (for `pass_middle`), `qb_hit`, `complete_pass` (to derive `cpoe`). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

DataFrame with the original columns plus `cp` (null for non-pass plays) and `cpoe` (null when `complete_pass` absent).

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.ep_wp import calculate_completion_probability

pbp = load_nfl_pbp([2023])
pbp_cp = calculate_completion_probability(pbp)
print(pbp_cp.select("cp", "cpoe").head())
```

### calculate_epa {#calculate_epa}

`calculate_epa(df: 'pl.DataFrame') -> 'pl.DataFrame'`

Derive expected points added (EPA) from pre-scored EP point estimates.

This is the **derivation half** of `NFLPlayProcess.__process_epa` lifted
into a shared, model-free function so the same nflfastR-faithful EPA logic
can be reused by the streaming `enrich_nfl_pbp` pipeline and by
process_epa` itself.  It performs **no** model inference — the caller
must already have scored the per-play EP point estimates.

Derivation rules (mirror nflfastR / the original process_epa`):

* Scoring overlays rewrite `EP_end` to the realized point value
  (offense TD `+7` / `+6.92` / 2pt variants, made FG `+3`,
  defensive scores, extra points, etc.) using the same `type.text` /
  `text` classification as process_epa`.
* Turnovers (`end_change_vec` / `downs_turnover`), kickoff turnovers
  and recovered onside kicks flip `EP_end` to the opponent's
  perspective (`EP_end * -1`).
* `lag_EP_end` is the previous play's `EP_end`; `EP_between` flips
  its sign on a prior-play possession change.
* Kickoffs use `EP_start_touchback` as `EP_start`.
* `EPA = EP_end - EP_start` normally; `-EP_start` on a non-scoring
  end-of-half play; `0` on a timeout; `EP_end - EP_start + EP_between`
  on a (non-kickoff, non-`Penalty`) penalty-in-text play.

**Every** `shift` is grouped `.over("game_id")` so a concatenated
multi-game frame never leaks EP across game boundaries — this differs from
process_epa` (which runs one game per instance and therefore needs no
grouping).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | Play-by-play DataFrame that already carries the EP point estimates under the ESPN-internal names `EP_start`, `EP_end` and `EP_start_touchback` (e.g. as produced by the EP-scoring half of process_epa`), plus the play-classification / flag columns: `game_id`, `type.text`, `text`, `change_of_pos_team`, `downs_turnover`, `kickoff_onside`, `scoring_play`, `end_of_half` and `penalty_in_text`. See EPA_REQUIRED_COLUMNS`. This function does **not** score EP itself — score it first via the EP feature pipeline (the `EP_*` triple is the ESPN-internal naming, distinct from `calculate_expected_points`'s lowercase `ep`). |

**Returns**

The input frame with the EPA derivation applied. `EP_start` is rewritten to `0.92` for scoring-attempt play types (`Extra Point Good`, `Extra Point Missed`, `Two-Point Conversion Good`, `Two-Point Conversion Missed`, `Two Point Pass`, `Two Point Rush`, `Blocked PAT`, `Defensive 2pt Conversion`) before any other overlays fire. `EP_start` / `EP_end` are then rewritten in place (overlays, sign flips, touchback), `EP_between`, `lag_EP_end` and `lag_change_of_pos_team` are added, `EPA` is added, and lowercase nflverse aliases `ep` (`= EP_end`), `epa` (`= EPA`), `ep_start` (`= EP_start`) and `ep_end` (`= EP_end`) are added for downstream contract parity.

**Example**

```python
# For most use cases, call the high-level entry point instead. ``enrich_nfl_pbp`` scores EP, derives EPA, and adds WP/WPA/CP/CPOE in one shot on any nflverse-shape frame

    from sportsdataverse.nfl import load_nfl_pbp
    from sportsdataverse.nfl.ep_wp import enrich_nfl_pbp

    pbp = load_nfl_pbp([2023])
    enriched = enrich_nfl_pbp(pbp)
    print(enriched.select("game_id", "ep", "epa").head())

``calculate_epa`` directly requires ESPN-internal columns
(``EP_start``, ``EP_end``, ``EP_start_touchback``, ``type.text``,
etc.) produced by ``NFLPlayProcess``.  It is called internally by
``NFLPlayProcess.__process_epa`` and by the ``enrich_nfl_pbp``
orchestrator — a naked ``calculate_epa(load_nfl_pbp([2023]))``
will raise ``KeyError`` because those columns are absent from a
nflverse frame.
```

### calculate_expected_points {#calculate_expected_points}

`calculate_expected_points(pbp_data: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Compute expected points for provided plays.

Mirrors nflfastR's `calculate_expected_points()`.  Drops and recomputes
any existing `ep` / `*_prob` columns so the output is always fresh.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_data` | `DataFrame` |  | Play-by-play DataFrame with nflverse columns. Required: `season`, `posteam`, `home_team`, `roof`, `half_seconds_remaining`, `yardline_100`, `down`, `ydstogo`, `posteam_timeouts_remaining`, `defteam_timeouts_remaining`. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

DataFrame with the original columns plus: `td_prob`, `opp_td_prob`, `fg_prob`, `opp_fg_prob`, `safety_prob`, `opp_safety_prob`, `no_score_prob`, and `ep` (expected points, clipped to [-10, 10]).

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.ep_wp import calculate_expected_points

pbp = load_nfl_pbp([2023])
pbp_ep = calculate_expected_points(pbp)
print(pbp_ep.select("ep").head())
```

### calculate_nfl_series_conversion_rates {#calculate_nfl_series_conversion_rates}

`calculate_nfl_series_conversion_rates(pbp: 'pl.DataFrame', *, weekly: 'bool' = False, return_as_pandas: 'bool' = False) -> "pl.DataFrame | 'pd.DataFrame'"`

Compute per-team offense + defense series conversion rates.

A faithful polars port of nflfastR's `calculate_series_conversion_rates`.
Series where `down` is null (kickoffs, PAT/2pt attempts, non-plays, no
`posteam`) and series ending in a `"QB kneel"` are excluded from the
series count before rates are computed, matching the R source.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame carrying `season`, `week`, `posteam`, `defteam`, `down`, `series`, `series_success`, and `series_result` (added by the `add_series_data` port). Rows must already be in play order within each series so the internal `first()`/`last()` series collapse is correct. |
| `weekly` | `bool` | `False` | If `True`, group on `(season, team, week)`; if `False` (default), group on `(season, team)` -- collapsing every week into one season-level rate. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; else polars. |

**Returns**

A polars (or pandas) DataFrame with one row per team (per week when `weekly=True`), `off_n`/`def_n` (series count) plus the `off_*`/`def_*` rate columns documented in reference Sec 11. A team with offensive series but zero defensive series in a group (or vice versa -- effectively never happens in real data) carries nulls in the missing side rather than being dropped (full outer join).

**Example**

```python
from sportsdataverse.nfl import calculate_nfl_series_conversion_rates
rates = calculate_nfl_series_conversion_rates(pbp)
rates.filter(pl.col("team") == "KC").select("off_scr", "def_scr")

# Weekly grain

weekly = calculate_nfl_series_conversion_rates(pbp, weekly=True)

# Pipeline next step (one line)

rates.sort("off_scr", descending=True).head()
```

### calculate_nfl_standings {#calculate_nfl_standings}

`calculate_nfl_standings(games: 'pl.DataFrame', *, teams: 'pl.DataFrame | None' = None, tiebreaker_depth: 'int' = 3, playoff_seeds: 'int | None' = None, return_as_pandas: 'bool' = False) -> "pl.DataFrame | 'pd.DataFrame'"`

Compute NFL division standings + conference playoff seeds.

A reduced port of the tiebreaker ladder nflfastR delegates to the external
`nflseedR` package (see the module docstring for the exact scope). Games
are doubled into one row per team per game, regular-season win/loss/tie
records are computed per team, and ties are broken win_pct -> head-to-head
-> division record -> conference record, to the depth configured by
`tiebreaker_depth`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | A `load_nfl_schedule`-shaped frame: `game_id`, `season`, `game_type`, `week`, `home_team`, `away_team`, `home_score`, `away_score`. Only `game_type == "REG"` rows with both scores present are used. |
| `teams` | `DataFrame \| None` | `None` | A `load_nfl_teams`-shaped frame (`team_abbr`, `team_conf`, `team_division`). When `None` (default), calls `sportsdataverse.nfl.load_nfl_teams`. Must cover every team abbreviation appearing in `games` -- a team absent from `teams` gets null `conf`/`division` and is silently pooled into the `(season, None)` division/conference group rather than raising. |
| `tiebreaker_depth` | `int` | `3` | `1` (win_pct only), `2` (adds head-to-head + division record), or `3` (default; adds conference record too). |
| `playoff_seeds` | `int \| None` | `None` | Number of teams per conference that receive a non-null `seed`. When `None` (default), uses the 2020 playoff -format cutover: `6` for seasons <= 2019, `7` for 2020+. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; else polars. |

**Returns**

A polars (or pandas) DataFrame with one row per (season, team): `conf`, `division`, `div_rank`, `seed` (null past `playoff_seeds`), `team`, `games`, `wins`, `losses`, `ties`, `win_pct` (ties count as 0.5 win), `div_pct`, `conf_pct`. Sorted by `(season, division, div_rank, seed)`.

**Example**

```python
from sportsdataverse.nfl import calculate_nfl_standings, load_nfl_schedule
games = load_nfl_schedule(seasons=[2023])
standings = calculate_nfl_standings(games)
standings.filter(standings["div_rank"] == 1)

# Injected teams frame (offline)

standings = calculate_nfl_standings(games, teams=my_teams_df)

# Pipeline next step (one line)

standings.sort(["conf", "seed"]).select("team", "seed", "win_pct")
```

### calculate_win_probability {#calculate_win_probability}

`calculate_win_probability(pbp_data: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Compute win probability for provided plays.

Mirrors nflfastR's `calculate_win_probability()`.  Uses the
spread-adjusted model (`wp_spread.ubj`) when `spread_line` is
non-null, and falls back to the naive model (`wp_naive.ubj`) for plays
with a missing spread line.  Drops and recomputes any existing `wp` /
`vegas_wp` columns.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_data` | `DataFrame` |  | Play-by-play DataFrame. Required: all EP columns plus `score_differential`, `game_seconds_remaining`, `spread_line`, `receive_2h_ko`. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

DataFrame with the original columns plus: `wp` (naive WP) and `vegas_wp` (spread-adjusted WP).

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.ep_wp import calculate_win_probability

pbp = load_nfl_pbp([2023])
pbp_wp = calculate_win_probability(pbp)
print(pbp_wp.select("wp", "vegas_wp").head())
```

### calculate_wpa {#calculate_wpa}

`calculate_wpa(df: 'pl.DataFrame') -> 'pl.DataFrame'`

Derive win probability added (WPA) from pre-scored WP point estimates.

This is the **derivation half** of `NFLPlayProcess.__process_wpa` lifted
into a shared, model-free function so the same nflfastR-faithful WPA logic
can be reused by the streaming `enrich_nfl_pbp` pipeline and by
process_wpa` itself.  It performs **no** model inference — the caller
must already have scored the per-play WP point estimates
(`wp_spread.ubj`) for the start / touchback / end feature views and
attached them as `wp_before` / `wp_touchback` / `wp_after`.  This
mirrors `calculate_epa`, which likewise consumes pre-scored EP point
estimates and leaves prediction to the orchestrator.

Derivation rules (mirror the original process_wpa`):

* **Leading overlay (do not drop):** on a kickoff (`type.text` in
  `kickoff_vec`) `wp_before` is replaced by `wp_touchback` — the
  win-probability scored from the touchback feature view — before any
  other column derives.  This is the WP analogue of the EPA `0.92`
  scoring-attempt overlay and must fire first.
* **Try rows:** a standalone try row (`Extra Point Good`, `Two Point
  Pass`, `Defensive 2pt Conversion`, ...) takes the `wp_after` of the
  touchdown before it (the last play that is not a clock stoppage) as its
  `wp_before` when the try is the touchdown's end team's, so the
  touchdown hands over to the try. A clock stoppage just before the try
  inherits too, restated for the team ESPN credits it to, and the try
  still hands over from the touchdown. The model cannot score the try's
  own start state (ESPN's down-0 placeholder). A return or defensive
  touchdown (a `scoringPlay` whose end team is the scorer, not its start
  team) hands over only a `wp_after` scored for the scorer, as
  `NFLPlayProcess` scores it.
* `def_wp_before = 1 - wp_before`; `home_wp_before` / `away_wp_before`
  are the posteam->home perspective columns (the offense's `wp_before`
  flows to home when the start possession team is the home team, otherwise
  to the defense `def_wp_before`).
* `wp_after` is rewritten by the end-of-half / end-of-game / OT two-path:
  timeouts hold `wp_before`; a completed final play resolves to `1.0` /
  `0.0` by the winner; end-of-half and `End Period` / `End of Half`
  lead plays take `lead_wp_before` (or `1 - lead_wp_before` on a
  possession change); a possession change otherwise flips the lead;
  everything else keeps the model `wp_after`.
* `def_wp_after = 1 - wp_after`; `home_wp_after` / `away_wp_after`
  use the **end** possession team for the perspective flip.
* `wpa = wp_after - wp_before`.

**Every** `shift` / forward reference is grouped `.over("game_id")` so a
concatenated multi-game frame never leaks WP across game boundaries — the
`lead_wp_before` / `lead_wp_before2` shifts and the end-of-game
`game_play_number == max()` lookup are all per-game.  This differs from
process_wpa` (which runs one game per instance and therefore needs no
grouping).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | Play-by-play DataFrame that already carries the WP point estimates `wp_before` (start feature view), `wp_touchback` (touchback feature view) and `wp_after` (end feature view), plus the play-classification / perspective columns `game_id`, `type.text`, `homeTeamId`, `start.pos_team.id`, `end.pos_team.id`, `start.pos_team_receives_2H_kickoff`, `change_of_pos_team`, `scoringPlay`, `kickoff_onside`, `end_of_half`, `status_type_completed`, `pos_score_diff_end`, `lead_play_type`, `lead_pos_team` and `game_play_number`. See WPA_REQUIRED_COLUMNS`. This function does **not** score WP itself — score it first via `calculate_win_probability` / the `wp_spread` feature pipeline. |

**Returns**

The input frame with the WPA derivation applied: `wp_before` rewritten by the kickoff-touchback overlay; `def_wp_before`, `home_wp_before`, `away_wp_before`, `lead_wp_before`, `lead_wp_before2`, the rewritten `wp_after`, `def_wp_after`, `home_wp_after`, `away_wp_after` and `wpa` added; plus first-class lowercase aliases `wp` (`= wp_before`), `def_wp` (`= def_wp_before`), `home_wp` (`= home_wp_before`) and `away_wp` (`= away_wp_before`) for downstream contract parity (the per-play offense win probability is the pre-snap `wp_before`, matching nflfastR's `wp` semantics).

**Example**

```python
# For most use cases, call the high-level entry point instead. ``enrich_nfl_pbp`` scores WP, derives WPA, and adds EP/EPA/CP/CPOE in one shot on any nflverse-shape frame

    from sportsdataverse.nfl import load_nfl_pbp
    from sportsdataverse.nfl.ep_wp import enrich_nfl_pbp

    pbp = load_nfl_pbp([2023])
    enriched = enrich_nfl_pbp(pbp)
    print(enriched.select("game_id", "wp", "def_wp", "home_wp", "away_wp", "wpa").head())

``calculate_wpa`` directly requires ESPN-internal columns
(``wp_before``, ``wp_touchback``, ``wp_after``, ``homeTeamId``,
``start.pos_team.id``, etc.) produced by ``NFLPlayProcess``.  It is
called internally by ``NFLPlayProcess.__process_wpa`` and by the
``enrich_nfl_pbp`` orchestrator — a naked
``calculate_wpa(load_nfl_pbp([2023]))`` will raise ``KeyError``
because those columns are absent from a nflverse frame.
```

### calculate_xpass {#calculate_xpass}

`calculate_xpass(pbp_data: 'pl.DataFrame', *, models_dir: 'Union[str, None]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Compute expected dropback probability (`xpass`) and `pass_oe`.

Faithful polars port of nflfastR's `add_xpass` /
`prepare_xpass_data` (`helper_add_xpass.R`).  Scores a single
`binary:logistic` XGBoost model (17 features, in `XPASS_FEATURES`
order) over the rows that satisfy nflfastR's `valid_play` filter:

- `season >= 2006` (before this the NFL did not mark scrambles), and
- `play_type in {"no_play", "pass", "run"}`, and
- none of `posteam` / `down` / `defteam_timeouts_remaining` /
  `posteam_timeouts_remaining` / `yardline_100` /
  `score_differential` is null.

The era2..4 + `outdoors` / `retractable` / `dome` dummies and the
`home` indicator are produced by make_cp_mutations` (the same
nflfastR `make_model_mutations` logic CP uses) rather than re-derived.
`wp` / `vegas_wp` are the start-of-play win-probability columns and
must already be present (run after the WP step / inside
`enrich_nfl_pbp`).

The booster ships with no embedded `feature_names`, so the DMatrix is
built with `XPASS_FEATURES` as the column order — feeding the
features in any other order silently yields wrong predictions.

Drops and recomputes any existing `xpass` / `pass_oe` columns.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_data` | `DataFrame` |  | nflverse-format play-by-play DataFrame. Required: `season`, `play_type`, `posteam`, `home_team`, `down`, `ydstogo`, `yardline_100`, `qtr`, `wp`, `vegas_wp`, `score_differential`, `half_seconds_remaining`, `posteam_timeouts_remaining`, `defteam_timeouts_remaining`. Optional: `roof` (for the roof dummies), `pass` / `rush` (the 0/1 dropback / rush indicators used by `pass_oe`). |
| `models_dir` | `Union[str, None]` | `None` | Optional directory to load `xpass_model.ubj` from instead of downloading / caching it (offline or custom model). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

DataFrame with the original columns plus `xpass` (predicted pass probability, null outside the `valid_play` filter; float64) and `pass_oe` (`100 * (pass - xpass)`, null when `xpass` is null and null when `rush == 0 & pass == 0`; float64).

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.ep_wp import enrich_nfl_pbp, calculate_xpass

pbp = enrich_nfl_pbp(load_nfl_pbp([2023]))  # gives wp / vegas_wp
pbp_xp = calculate_xpass(pbp)
print(pbp_xp.select("xpass", "pass_oe").head())

# Pipeline next step

pbp_xp.filter(pl.col("play_type") == "pass").select("posteam", "xpass", "pass_oe").head()
```

### calculate_xyac {#calculate_xyac}

`calculate_xyac(pbp_data: 'pl.DataFrame', *, models_dir: 'Optional[Union[str, Path]]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Compute expected yards after catch (xYAC) for intended pass plays.

Faithful polars port of nflfastR's `add_xyac`.  Unlike a per-statistic
regressor, xYAC is **one** `multi:softprob` model (`num_class=76`) that
predicts a distribution over YAC buckets (`yac = -5..70`); the five output
columns are *derived* from that distribution by re-scoring expected points on
every outcome.  `ep` is **not** required on the input — it is recomputed on
the outcome rows via `calculate_expected_points`.  The play's pre-snap
`ep` (`original_ep`) is the EPA baseline; `air_epa` is also part of the
baseline (`xyac_epa = Σ((ep − original_ep)·prob) − air_epa`).  `air_epa`
is **optional**: when present (the nflverse path) it is used verbatim so
parity is byte-for-byte preserved; when absent (the Shield-native / ESPN
path) it is computed from the already-scored `yac == 0` (catch-spot)
outcome — `air_epa = ep(yac == 0) − original_ep` — and, since it was
genuinely missing, surfaced as an extra `air_epa` output column.

Inference filter (nflfastR `valid_pass` ∧ `distance_to_goal != 0`):
`complete_pass == 1` OR `incomplete_pass == 1` OR `interception == 1`,
`air_yards` in `[-15, 70)`, non-null `receiver_player_name` and
`pass_location`, and `distance_to_goal != 0`.  Non-qualifying rows
receive null in all five columns.  Drops and recomputes any existing xYAC
output columns.

The xYAC model (`xyac_model.ubj`, ~34 MB) is **not** bundled in the
wheel: on first use it is downloaded from the `nfl_model_artifacts`
GitHub release and cached under `<cache_dir>/models/` (see
`sportsdataverse.nfl.get_config`).  Subsequent calls load it from the
cache; `clear_cache()` deliberately preserves the `models/` subdir so a
data-cache clear does not force a re-download.  Pass `models_dir=` to
point at a local directory containing `xyac_model.ubj` (offline / custom
model override).  If the model is genuinely unavailable (no cache + no
network) the underlying loader raises `FileNotFoundError`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_data` | `DataFrame` |  | nflverse-format play-by-play DataFrame. Required: `air_yards`, `season`, `half_seconds_remaining`, `yardline_100`, `ydstogo`, `down`, `posteam`, `home_team`, `roof`, `ep`, `posteam_timeouts_remaining`, `defteam_timeouts_remaining`, `complete_pass`, `incomplete_pass`, `interception`, `pass_location`, `receiver_player_name`. Optional: `air_epa` (used verbatim when present for byte-for-byte nflverse parity; computed from the `yac == 0` outcome and added as an output column when absent), `qb_hit`. |
| `models_dir` | `Optional[Union[str, Path]]` | `None` | Optional directory to load `xyac_model.ubj` from instead of downloading/caching it (offline use or a custom-trained model). When `None` (default) the model is resolved bundled → cache → downloaded-from-release. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

DataFrame with the original columns plus the five nflfastR xYAC columns (`Float64`, null on non-qualifying rows): `xyac_epa`, `xyac_mean_yardage`, `xyac_median_yardage`, `xyac_success`, `xyac_fd`. When the input lacked `air_epa` and at least one qualifying pass was scored, a computed `air_epa` column (catch-spot air EPA) is also added.

**Example**

```python
import polars as pl

from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.ep_wp import calculate_xyac

pbp = load_nfl_pbp([2023])
pbp = calculate_xyac(pbp)
print(pbp.select("xyac_epa", "xyac_mean_yardage").head())

# Pipeline next step (one line)

pbp.filter(pl.col("xyac_epa").is_not_null()).select("xyac_epa", "xyac_fd").head()
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

### load_nfl_fp_curve {#load_nfl_fp_curve}

`load_nfl_fp_curve() -> 'pl.DataFrame'`

Load the bundled NFL EP-by-yardline curve (no network).

**Returns**

`yardline_own: Int64 (1..99), ep: Float64`.

| col_name | type | description |
|---|---|---|
| `yardline_own` | integer | Starting yard line from the offense's own goal (1-99); one row per yard line of the bundled NFL EP-by-starting-yardline curve. |
| `ep` | double | Using the scoring event probabilities, the estimated expected points with respect to the possession team for the given play. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_fp_curve
curve = load_nfl_fp_curve()
curve.filter(curve["yardline_own"] == 30)
```

### nfl_compute_results {#nfl_compute_results}

`nfl_compute_results(teams: 'pl.DataFrame', games: 'pl.DataFrame', week_num: 'Union[str, int]', *, rng: 'Optional[np.random.Generator]' = None, elo: 'Optional[Mapping[str, float]]' = None, **kwargs: 'Any') -> 'Dict[str, pl.DataFrame]'`

Compute NFL game results for one week of a season simulation.

Faithful port of `nflseedR_compute_results` (simulations_utils.R
L183-290) — the 538-style dynamic ELO model initially coded by Lee
Sharpe and rewritten by Sebastian Carl: home/away ELO difference plus
rest (+25 per extra week), home field (+20), and a 1.2x postseason
multiplier produce a win probability and a point spread `estimate`
(`elo_diff / 25`); missing results for `week_num` are drawn from
`Normal(estimate, 13)` and rounded away from zero. ELO ratings are
updated from all of the week's results and carried to the next week
via the returned `teams` frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `teams` | `DataFrame` |  | Teams frame with `sim` and `team` columns. An `elo` column is added on first call (from `elo` or random `Normal(1500, 150)` initial ratings shared across sims) and must be carried between calls. |
| `games` | `DataFrame` |  | Games frame with `sim`, `week`, `game_type`, `location`, `home_team`/`away_team`, `home_rest`/ `away_rest`, and `result` columns. |
| `week_num` | `Union[str, int]` |  | The week to simulate. Only rows with `week == week_num` and a missing `result` are filled. |
| `rng` | `Optional[Generator]` | `None` | numpy random generator; a fresh one is created when `None`. |
| `elo` | `Optional[Mapping[str, float]]` | `None` | Optional mapping of team abbreviation to initial ELO rating. |

**Returns**

`{"teams": teams, "games": games}` with updated ELO ratings and filled results.

| col_name | type | description |
|---|---|---|
| `teams.sim` | integer | Simulated season identifier the team row belongs to, carried through from the input teams frame. |
| `teams.team` | character | Team abbreviation, carried through from the input teams frame. |
| `teams.conf` | character | Conference of the team (AFC or NFC), carried through from the input teams frame. |
| `teams.division` | character | Division of the team (e.g. "AFC East"), carried through from the input teams frame. |
| `teams.elo` | double | Dynamic ELO rating after applying the shifts from the simulated week's results; carried into the next week's call so ratings evolve over the simulated season. |
| `games.sim` | integer | Simulated season identifier the game row belongs to. |
| `games.game_type` | character | Game type of the row - REG for regular season or the playoff round (WC, DIV, CON, SB). |
| `games.week` | character | Week key used by the simulation engine - regular season week numbers as strings and postseason rounds as WC/DIV/CON/SB. |
| `games.away_team` | character | Team abbreviation of the away team. |
| `games.home_team` | character | Team abbreviation of the home team. |
| `games.away_rest` | integer | Days of rest for the away team before the game (feeds the ELO rest adjustment of 25 points per extra week). |
| `games.home_rest` | integer | Days of rest for the home team before the game. |
| `games.location` | character | Game site indicator - "Home" applies the +20 ELO home-field adjustment, "Neutral" (Super Bowl) does not. |
| `games.result` | integer | Home margin (home score minus away score). Rows of the simulated week that were missing are filled from Normal(estimate, 13) rounded away from zero; all other rows pass through unchanged. |

**Example**

```python
from sportsdataverse.nfl.nfl_simulations import nfl_compute_results
out = nfl_compute_results(teams, games, week_num="5")
teams, games = out["teams"], out["games"]
```

### nfl_draft_projection {#nfl_draft_projection}

`nfl_draft_projection(seasons: 'List[int]', target_class: 'int', *, lam: 'float' = 100.0, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Draft outcome projection for one draft class.

Trains the closed-form ridge (expected `car_av`) and the IRLS logistic
(`hit_prob` = P(`seasons_started >= 3`)) on **matured** classes
(`season <= target_class - 5`) and scores the `target_class`
prospects. Features: standardized combine measurables (+ imputation
flags), draft `round`/`pick`/`log(pick)`, position one-hots.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  | Draft classes to load (training classes beyond the maturity boundary are filtered out automatically). |
| `target_class` | `int` |  | The draft class to score. |
| `lam` | `float` | `100.0` | Ridge regularization strength. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. |

**Returns**

One row per `target_class` prospect: `gsis_id:Utf8, target_class:Int64, position:Utf8, pred_car_av:Float64, hit_prob:Float64, outcome_rank:Int64` (dense rank, best first). Empty training or prediction slice returns a zero-row frame.

| col_name | type | description |
|---|---|---|
| `gsis_id` | character | nflverse gsis player id of the drafted prospect (character join key). |
| `target_class` | integer | The draft class scored (training uses matured classes <= target_class - 5). |
| `position` | character | Draft position group of the prospect. |
| `pred_car_av` | double | Predicted career value - closed-form ridge on standardized combine measurables + round/pick/log(pick) + position one-hots; the label is nflverse w_av (PFR weighted career Approximate Value). |
| `hit_prob` | double | P(multi-year starter) - ridge-regularized IRLS logistic on the same features, hit := seasons_started >= 3. |
| `outcome_rank` | integer | Dense rank of pred_car_av within the class (best prospect = 1). |

**Example**

```python
from sportsdataverse.nfl.nfl_draft_model import nfl_draft_projection
proj = nfl_draft_projection(list(range(2000, 2020)), 2019)
proj.sort("outcome_rank").head()
```

### nfl_fantasy_projection {#nfl_fantasy_projection}

`nfl_fantasy_projection(seasons: 'List[int]', target_season: 'int', *, scoring: 'Union[Dict[str, float], str]' = 'ppr', calibrate: 'bool' = True, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Fantasy-points projection: deterministic scoring of the Marcel component

stats plus a fitted per-position linear calibration.

Scores `nfl_player_projection`'s projected component *counting* stats
(rate x projected games) under the scoring format, then applies the fitted
`fp_calibration` `(a, b)` from `POSITION_CONSTANTS`
(`calibrated = a + b * raw`). The FantasyPros consensus is used only as a
concurrent-validity oracle in the tests — never as an input.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  | History seasons to load. |
| `target_season` | `int` |  | The season being projected. |
| `scoring` | `Union[Dict[str, float], str]` | `'ppr'` | `"ppr"` / `"half"` / `"standard"` or a custom points-per-unit dict. |
| `calibrate` | `bool` | `True` | Apply the fitted per-position calibration. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. |

**Returns**

`player_id:Utf8, target_season:Int64, position_group:Utf8, proj_fantasy_points:Float64, proj_fantasy_points_per_game:Float64, position_rank:Int64`.

| col_name | type | description |
|---|---|---|
| `player_id` | character | nflverse gsis player id (character join key). |
| `target_season` | integer | The season being projected (features use strictly earlier seasons only). |
| `position_group` | character | nflverse offensive position group (QB/RB/WR/TE plus fringe groups). |
| `proj_fantasy_points` | double | Projected season fantasy points - the Marcel component rates x projected games scored under the scoring format, with the fitted per-position linear calibration applied by default. |
| `proj_fantasy_points_per_game` | double | Projected fantasy points per game (proj_fantasy_points / projected games). |
| `position_rank` | integer | Dense rank of proj_fantasy_points within the position group (best = 1). |

**Example**

```python
from sportsdataverse.nfl.nfl_projection import nfl_fantasy_projection
fp = nfl_fantasy_projection([2021, 2022, 2023], 2024)
fp.filter(pl.col("position_group") == "WR").head()

# Custom scoring

fp_std = nfl_fantasy_projection([2021, 2022, 2023], 2024, scoring="standard")
```

### nfl_kicker_rating {#nfl_kicker_rating}

`nfl_kicker_rating(seasons: 'Union[int, List[int]]', *, as_of: 'Optional[Tuple[int, int]]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Environment-adjusted kicker FG-over-expected ratings.

Loads pbp FG attempts for `seasons`, computes the environment-adjusted
expected make probability per kick, and aggregates to per
`(season, kicker)` FGOE (raw + EB-shrunk with the fitted `K_fg`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, List[int]]` |  | Season or list of seasons. |
| `as_of` | `Optional[Tuple[int, int]]` | `None` | Optional `(season, week)`; uses only kicks strictly before that point (the as-of leakage boundary for mid-season ratings). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

Per `(season, kicker_player_id)`: `kicker`, `team`, `fg_att`, `fg_made`, `exp_made`, `fgoe`, `fgoe_per_att`, `fgoe_shrunk`, `rating` (100 +/- 15 z of `fgoe_shrunk`). Empty seasons yield a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the rating. |
| `kicker_player_id` | character | nflverse kicker GSIS id (Utf8 join key). |
| `kicker` | character | Display name of the kicker (e.g. J.Tucker), from kicker_player_name. |
| `team` | character | Team of the kicker's most recent attempt in the window. |
| `fg_att` | integer | Field-goal attempts. |
| `fg_made` | integer | Field goals made. |
| `exp_made` | double | Sum of environment-adjusted make probabilities (expected makes). |
| `fgoe` | double | Field goals made over expected (fg_made - exp_made). |
| `fgoe_per_att` | double | FGOE per attempt. |
| `fgoe_shrunk` | double | Empirical-Bayes shrunk FGOE per attempt, fgoe_per_att * att / (att + K_fg). |
| `rating` | double | 100 +/- 15 z-score of fgoe_shrunk within the frame. |

**Example**

```python
from sportsdataverse.nfl.nfl_kicker_rating import nfl_kicker_rating
r = nfl_kicker_rating([2023])
print(r.head())

# Mid-season as-of rating

r = nfl_kicker_rating([2023], as_of=(2023, 10))
```

### nfl_line_grades {#nfl_line_grades}

`nfl_line_grades(seasons: 'Union[int, List[int]]', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Team-season OL pass-block + DL pass-rush grades (opponent-adjusted, EB-shrunk).

Loads pbp, builds the matchup pressure grid, opponent-adjusts it, grades
both units on a 0-100 board (`50 + 15*z*n/(n+K_pressure)`), and joins
PFR's independent team pressure measurement
(`load_nfl_pfr_advstats(stat_type="def", summary_level="season")`,
`prss` summed to team / pbp dropbacks faced) as `pfr_pressure_pct`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, List[int]]` |  | Season or list of seasons (PFR advstats coverage is 2018+). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

Per `(season, team)`: raw + adjusted pressure rates and dropback counts, `ol_pass_block_grade`, `dl_pass_rush_grade`, `pfr_pressure_pct`. Empty seasons yield a zero-row frame.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the grade. |
| `team` | character | Team abbreviation. |
| `dropbacks_off` | integer | Offensive dropbacks (qb_dropback plays). |
| `pressures_allowed` | integer | Sacks plus QB hits allowed on the team's own dropbacks. |
| `pressure_rate_allowed` | double | pressures_allowed / dropbacks_off (raw). |
| `dropbacks_def` | integer | Opponent dropbacks faced on defense. |
| `pressures_generated` | integer | Sacks plus QB hits generated against opponent dropbacks. |
| `pressure_rate_generated` | double | pressures_generated / dropbacks_def (raw). |
| `adj_pressure_rate_allowed` | double | Opponent-adjusted allowed pressure rate (additive fixed point, league-mean-centered). |
| `adj_pressure_rate_generated` | double | Opponent-adjusted generated pressure rate (additive fixed point, league-mean-centered). |
| `ol_pass_block_grade` | double | OL pass-block grade, 50 + 15 * z * n/(n + K_pressure) on the inverted adjusted allowed rate. |
| `dl_pass_rush_grade` | double | DL pass-rush grade, 50 + 15 * z * n/(n + K_pressure) on the adjusted generated rate. |
| `pfr_pressure_pct` | double | PFR team pressures (prss summed, traded 2TM/3TM rows excluded) divided by pbp dropbacks faced. |

**Example**

```python
from sportsdataverse.nfl.nfl_line_grades import nfl_line_grades
g = nfl_line_grades([2023])
print(g.sort("dl_pass_rush_grade", descending=True).head())
```

### nfl_player_projection {#nfl_player_projection}

`nfl_player_projection(seasons: 'List[int]', target_season: 'int', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Marcel-style next-season player projection with delta-method aging.

Loads weekly player stats + rosters, aggregates to season rates, and for
every player visible in seasons **strictly before** `target_season`
(the as-of-date leakage boundary) produces a recency-weighted rate blend
regressed toward the volume-weighted position mean by
`k / (k + reliability)`, scaled by the position aging-curve ratio
`aging_mult(proj_age) / aging_mult(current_age)`. The aging curve is fit
only on the same pre-target history.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  | History seasons to load (seasons `>= target_season` are discarded by the leakage split). |
| `target_season` | `int` |  | The season being projected. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. |

**Returns**

One row per projected player: `player_id:Utf8, target_season:Int64, position_group:Utf8, proj_age:Float64, proj_ppg:Float64, proj_volume:Float64, proj_games:Float64, aging_mult:Float64, reliability:Float64` plus `proj_<stat>_rate` component-rate columns. Empty history returns a zero-row frame.

| col_name | type | description |
|---|---|---|
| `player_id` | character | nflverse gsis player id (character join key). |
| `target_season` | integer | The season being projected (features use strictly earlier seasons only - the as-of-date leakage boundary). |
| `position_group` | character | nflverse offensive position group (QB/RB/WR/TE plus fringe groups). |
| `proj_age` | double | Projected age at the target season (age at last visible season + season gap). |
| `proj_ppg` | double | Projected PPR fantasy points per game - recency-weighted rate blend regressed toward the volume-weighted position mean by k/(k + reliability), scaled by the damped aging-curve ratio. |
| `proj_volume` | double | Projected position-specific opportunity volume (QB = pass attempts, RB = carries + targets, WR/TE = targets). |
| `proj_games` | double | Recency-weighted mean of historical games played. |
| `aging_mult` | double | Applied aging multiplier - the damped, clamped ratio aging_curve(proj_age) / aging_curve(current_age). |
| `reliability` | double | Recency-weighted volume sum - the shrinkage evidence weight. |
| `proj_completions_rate` | double | Projected per-game pass completions (Marcel blend x aging ratio). |
| `proj_attempts_rate` | double | Projected per-game pass attempts (Marcel blend x aging ratio). |
| `proj_passing_yards_rate` | double | Projected per-game passing yards (Marcel blend x aging ratio). |
| `proj_passing_tds_rate` | double | Projected per-game passing touchdowns (Marcel blend x aging ratio). |
| `proj_interceptions_rate` | double | Projected per-game interceptions thrown (Marcel blend x aging ratio). |
| `proj_carries_rate` | double | Projected per-game rush attempts (Marcel blend x aging ratio). |
| `proj_rushing_yards_rate` | double | Projected per-game rushing yards (Marcel blend x aging ratio). |
| `proj_rushing_tds_rate` | double | Projected per-game rushing touchdowns (Marcel blend x aging ratio). |
| `proj_receptions_rate` | double | Projected per-game receptions (Marcel blend x aging ratio). |
| `proj_targets_rate` | double | Projected per-game targets (Marcel blend x aging ratio). |
| `proj_receiving_yards_rate` | double | Projected per-game receiving yards (Marcel blend x aging ratio). |
| `proj_receiving_tds_rate` | double | Projected per-game receiving touchdowns (Marcel blend x aging ratio). |
| `proj_receiving_air_yards_rate` | double | Projected per-game receiving air yards (Marcel blend x aging ratio). |
| `proj_fumbles_lost_rate` | double | Projected per-game fumbles lost (Marcel blend x aging ratio). |

**Example**

```python
from sportsdataverse.nfl.nfl_projection import nfl_player_projection
proj = nfl_player_projection([2021, 2022, 2023], 2024)
proj.sort("proj_ppg", descending=True).head()

# Pandas round-trip

proj_pd = nfl_player_projection([2021, 2022, 2023], 2024, return_as_pandas=True)
```

### nfl_ratings {#nfl_ratings}

`nfl_ratings(seasons: 'int | list[int]', *, as_of_date: 'datetime.date | None' = None, config: 'RatingsConfig | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

One row per team: the native NFL ratings spine (off/def/ST EPA).

Public orchestrator over `efficiency_ratings` +
`special_teams_ratings`. Loads play-by-play + schedule via
`load_nfl_pbp` / `load_nfl_schedule`, joins each game's `gameday`
onto the plays, optionally applies the as-of-date leakage boundary
(only plays from games with `gameday < as_of_date` are used), then
fits both components and reshapes into one wide per-team table with
dense ranks and a net z-score.

The loaded pbp is down-selected to the ridge columns *before* any fit so
no market column (`spread_line` / `vegas_wp`) can leak into the
ratings (the binding non-market boundary).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A single season (e.g. `2023`) or a list of seasons pooled into one combined fit. |
| `as_of_date` | `date \| None` | `None` | When given, only plays from games strictly before this date are used (mirrors what was knowable heading into that date). `None` (default) uses the full season(s). |
| `config` | `RatingsConfig \| None` | `None` | Tuning knobs forwarded to both component fits; defaults to `RatingsConfig`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame. |

**Returns**

A DataFrame with one row per `team_id`: `season` (Int64 -- the single passed season, `null` for a pooled multi-season call), `team_id` (Utf8), `adj_off_epa` / `adj_def_epa` / `adj_st_epa` / `adj_net` (Float64; `adj_net` is offense minus defense -- special teams stays a separate column), `games` (Int64), `off_rank` / `def_rank` / `net_rank` (Int64; `def_rank` ascends -- fewer EPA allowed ranks better), `net_z` (Float64). Zero-row, correctly-typed when the seasons have no data or `as_of_date` filters out every play.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the ratings cover (null for a pooled multi-season fit). |
| `team_id` | character | nflverse team abbreviation (character join key, e.g. "KC"). |
| `adj_off_epa` | double | Opponent-adjusted offensive EPA per play (higher is better); competitive-play ridge fit. |
| `adj_def_epa` | double | Opponent-adjusted defensive EPA allowed per play (lower is better); competitive-play ridge fit. |
| `adj_st_epa` | double | Opponent-adjusted special-teams EPA per play (ridge on special==1 plays; 0.0 for teams with no special-teams plays in the window). |
| `adj_net` | double | Opponent-adjusted net efficiency (adj_off_epa minus adj_def_epa; special teams not folded in). |
| `games` | integer | Number of games the team played in the fitted window. |
| `off_rank` | integer | Dense rank on adj_off_epa descending (best offense = 1). |
| `def_rank` | integer | Dense rank on adj_def_epa ascending (fewer EPA allowed ranks better). |
| `net_rank` | integer | Dense rank on adj_net descending (best net rating = 1). |
| `net_z` | double | Z-score of adj_net across the 32 teams. |

**Example**

```python
from sportsdataverse.nfl import nfl_ratings
ratings = nfl_ratings(2023)
ratings.sort("net_rank").head()

# As-of-date leakage boundary

import datetime as dt
week6 = nfl_ratings(2023, as_of_date=dt.date(2023, 10, 12))
```
