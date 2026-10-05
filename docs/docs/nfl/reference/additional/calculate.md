---
title: "NFL — additional Python functions — Calculate"
sidebar_label: "Calculate"
sidebar_position: 2
description: "NFL — additional Python functions — Calculate — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Calculate

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
