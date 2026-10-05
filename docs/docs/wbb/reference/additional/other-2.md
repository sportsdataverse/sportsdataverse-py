---
title: "WBB — additional Python functions — Other: adjust_efficiency–find_missing"
sidebar_label: "Other: adjust_efficiency–find_missing"
sidebar_position: 10
description: "WBB — additional Python functions — Other: adjust_efficiency–find_missing — function reference in sdv-py, the SportsDataverse Python package."
---
# WBB — additional Python functions — Other: adjust_efficiency–find_missing

### adjust_efficiency {#adjust_efficiency}

`adjust_efficiency(game_eff: 'pl.DataFrame', *, league: 'str' = 'mens', max_iter: 'int' = 100, tol: 'float' = 0.0001) -> 'pl.DataFrame'`

Iterative opponent-adjusted efficiency -> AdjO / AdjD / AdjEM per team-season.

KenPom-style fixed point: initialise `adj_o = raw_o` / `adj_d = raw_d`,
then repeatedly recompute each team's rating from its games with the
opponent's *current* adjusted rating and a home-court adjustment removed,
until the largest change is below `tol`. Ratings are computed independently
per season (a team's opponent pool is within-season).

The per-game offensive update is
`off_eff - (adj_d_opp - avg) - loc_o` where `loc_o` is `+hfa/2` at
home, `-hfa/2` away, `0` neutral (defense is symmetric with the opposite
sign); `avg` is the league mean efficiency and `hfa` comes from
`~sportsdataverse.mbb.mbb_prediction_constants.get_constants`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_eff` | `DataFrame` |  | Output of `raw_game_efficiency`. |
| `league` | `str` | `'mens'` | `"mens"` / `"womens"` -- selects the HFA constant. |
| `max_iter` | `int` | `100` | Maximum fixed-point iterations. |
| `tol` | `float` | `0.0001` | Convergence tolerance on the largest rating change. |

**Returns**

One row per (season, team_id): `season, team_id, adj_o, adj_d, adj_em, raw_o, raw_d, games`. Empty input returns that schema with zero rows.

**Example**

```python
from sportsdataverse.mbb.mbb_team_ratings import adjust_efficiency, raw_game_efficiency
ratings = adjust_efficiency(raw_game_efficiency(sched, box))
```

### adjust_off_rating_stats {#adjust_off_rating_stats}

`adjust_off_rating_stats(pts_correction_factor: 'float', poss_correction_factor: 'float', mutable_o_rtg: 'ORtgDiagnostics', maybe_raw_o_rtg: 'float | None') -> 'tuple[float, float] | None'`

Apply a missing-possession correction factor to an `ORtgDiagnostics` dict in place.

Faithful port of `RatingUtils.adjustOffRatingStats` (`RatingUtils.ts:993-1033`).
Genuinely public upstream (called from `LineupTableUtils.ts` after a
lineup-level pts/poss reconciliation), so this port is public too.
Recomputes the productivity fields via `build_productivity`
(reused, not re-derived).

**Landmine 4** (see module docstring): the `o_adj = avgEff / defSos or
1` recomputation here is unguarded against `defSos == 0` -- same
reachability analysis as landmine 3 (only reachable if the diagnostics
dict's original `build_o_rtg` call used `avg_efficiency == 0`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pts_correction_factor` | `float` |  | Points correction factor (e.g. team pts / sum of player pts, capped to `[0.95, 1.05]` by callers). |
| `poss_correction_factor` | `float` |  | Possession correction factor, same shape. |
| `mutable_o_rtg` | `ORtgDiagnostics` |  | The `ORtgDiagnostics` dict to mutate in place (`oRtg`, `Usage`, `adjORtg`, `adjORtgPlus`, `Usage_Bonus`, `SoS_Bonus`, `adjPtsFactor`, `adjPossFactor`, and (conditionally) `Raw_Usage` are all updated). |
| `maybe_raw_o_rtg` | `float \| None` |  | The un-overridden raw `oRtg` value (`rawORtg`'s `.value`, or `None` when no override was in play), used to compute the raw-side return. |

**Returns**

`(new_raw_o_rtg, raw_adj_o_rtg_plus)` when both `mutable_o_rtg["Raw_Usage"]` and `maybe_raw_o_rtg` are not `None`; otherwise `None` (.isNil` semantics -- an explicit `0` does NOT count as nil).

**Example**

```python
from sportsdataverse.mbb.mbb_ratings import build_o_rtg, adjust_off_rating_stats

_, _, raw_o_rtg, _, o_diags = build_o_rtg(player, {}, {}, 100.0, True, False)
maybe_raw = raw_o_rtg["value"] if raw_o_rtg else None
adjust_off_rating_stats(1.1, 0.9, o_diags, maybe_raw)
print(o_diags["oRtg"], o_diags["adjORtgPlus"])
```

### adjust_tempo {#adjust_tempo}

`adjust_tempo(game_eff: 'pl.DataFrame', *, league: 'str' = 'mens', max_iter: 'int' = 100, tol: 'float' = 0.0001) -> 'pl.DataFrame'`

Opponent-adjusted tempo (possessions/40) per team-season.

Same fixed point as `adjust_efficiency`, applied to game possessions
under the additive model `poss = tempo_i + tempo_j - avg`: a team's tempo
is recovered by removing its opponents' current adjusted tempo. `avg` is
the league baseline tempo from
`~sportsdataverse.mbb.mbb_prediction_constants.get_constants`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_eff` | `DataFrame` |  | Output of `raw_game_efficiency`. |
| `league` | `str` | `'mens'` | `"mens"` / `"womens"` -- selects the tempo baseline. |
| `max_iter` | `int` | `100` | Maximum fixed-point iterations. |
| `tol` | `float` | `0.0001` | Convergence tolerance on the largest tempo change. |

**Returns**

One row per (season, team_id): `season, team_id, adj_tempo`. Empty input returns that schema with zero rows.

**Example**

```python
from sportsdataverse.mbb.mbb_team_ratings import adjust_tempo, raw_game_efficiency
tempo = adjust_tempo(raw_game_efficiency(sched, box))
```

### aggregate_player_seasons {#aggregate_player_seasons}

`aggregate_player_seasons(seasons: "'list[int]'", *, league: 'str' = 'mens') -> 'pl.DataFrame'`

Canonical per-player-season counting frame from the boxscore release.

Sums the per-game player boxscores into one row per (player_id, season,
team_id) with the counting columns `player_per100_features` expects.
Shot-location splits come from the shots release (2025+): free throws
(`MadeFreeThrow`) are excluded, layup/dunk/tip = rim, and jump shots
split three vs mid by `score_value` (the release's `type_text` carries
no three-point marker; `score_value` is populated on misses too). For
seasons without shots data, three-point attempts come from the box and
all remaining attempts fold into `fga_mid`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to aggregate. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |

**Returns**

One row per (player_id, season, team_id): `player_id:Utf8, season, team_id:Utf8, player, minutes` + the counting columns + `fga_rim, fga_mid, fga_three`. Empty input returns zero rows.

**Example**

```python
from sportsdataverse.mbb.mbb_player_value_constants import (
    aggregate_player_seasons, player_per100_features,
)
feats = player_per100_features(aggregate_player_seasons([2025]))
```

### alias_combos {#alias_combos}

`alias_combos(first: 'str', last: 'str', to_name: 'str') -> 'dict[str, str]'`

Pair each of `combos`' three name variants with a shared alias

target (`DataQualityIssues.alias_combos`, `DataQualityIssues.scala:351-356`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `first` | `str` |  | The player's first (mis-recorded) first name. |
| `last` | `str` |  | The player's (mis-recorded) last name. |
| `to_name` | `str` |  | The canonical `"Lastname, Firstname"` this player should resolve to. |

**Returns**

A dict mapping each of the three name variants to `to_name`.

### analyze_and_fix_clumps {#analyze_and_fix_clumps}

`analyze_and_fix_clumps(clump: 'BadLineupClump', box_lineup: 'LineupEvent', valid_player_codes: 'set[str]') -> 'tuple[list[LineupEvent], BadLineupClump]'`

Runs the full self-healing fixer pipeline over one bad-lineup clump

(`LineupErrorAnalysisUtils.analyze_and_fix_clumps`, `:556-610`).

The strict, order-dependent sequence (each stage threads
`(fixed_so_far + newly_fixed, still_to_fix)`):

1. `handle_common_sub_bug`,
2. `find_missing_subs`,
3. `add_missing_players`,
4. `find_missing_subs` **again** -- the Scala's own comment
   (`:587-588`) explains: "Try this again since add_missing_players can
   go too far". Step 3 back-fills onto every event and can push some past
   5 players; the second trim pass removes the over-add.

Finally every accumulated `fixed` lineup gets a fresh `lineup_id` via
`~sportsdataverse.mbb.mbb_ncaa_stints.build_lineup_id` (`:597-605`)
-- the fixers changed the on-floor `players`, so the id computed during
stint construction is stale. The Scala's `debug`-gated
`analyze_unfixed_clumps` call (`:593-596`) is dropped (see the module
docstring's "Debug-only" note); it only prints.

The Scala wraps the whole pipeline in `Some(clump).map { ... }
.getOrElse((Nil, clump))`, but `Some(_)` is never empty so the
`getOrElse` is dead -- omitted here.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clump` | `BadLineupClump` |  | The bad-lineup clump to repair. |
| `box_lineup` | `LineupEvent` |  | The team's box-score lineup event (roster + name context). |
| `valid_player_codes` | `set[str]` |  | Every player code on the box score / roster. |

**Returns**

`(fixed_lineups, still_to_fix)` -- every repaired lineup (with a recomputed `lineup_id`) and whatever clump the pipeline could not fix.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stint_validation import (
    analyze_and_fix_clumps,
)
fixed, still = analyze_and_fix_clumps(clump, box_lineup, valid_codes)
for lineup in fixed:
    print(lineup.lineup_id.value)
```

### apply_relative_positional_overrides {#apply_relative_positional_overrides}

`apply_relative_positional_overrides(results: 'list[dict[str, str]]', team_season: 'str', recurse_count: 'int' = 0) -> 'list[dict[str, str]]'`

Recursively re-shuffle an ordered lineup per `RELATIVE_POSITION_FIXES`.

Faithful port of the private `PositionUtils.applyRelativePositionalOverrides`
(`PositionUtils.ts:657-693`). Finds the first rule (in table order) whose
`key` slots all match the current `results` codes (a `None` key slot
matches anything), applies that rule's `rule` slots (`None` = leave
unchanged, `int` = 1-based back-reference into the *pre-rule* results,
`dict` = literal replacement) to produce a new ordering, then recurses on
the new ordering -- since one swap can expose a second rule to match (e.g.
the Maryland 2019/20 Morsell/Wiggins swap can cascade into the Lindo/Smith
swap). Recursion is bounded by `recurse_count < len(rules)` (ported
verbatim from the TS bound), so it always terminates even if two rules
somehow ping-ponged each other.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `results` | `list[dict[str, str]]` |  | The current 5-slot `{"code": ..., "id": ...}` ordering (PG/SG/SF/PF/C, index 0-4). |
| `team_season` | `str` |  | Key into `RELATIVE_POSITION_FIXES`. A team/season absent from the table (or the recursion exhausting that team/season's rule count) returns `results` unchanged. |
| `recurse_count` | `int` | `0` | Internal recursion depth counter -- callers should not pass this explicitly (mirrors the TS default parameter). |

**Returns**

The (possibly re-shuffled) 5-slot ordering.

**Example**

```python
from sportsdataverse.mbb.mbb_positions import apply_relative_positional_overrides
results = [
    {"code": "AnCowan", "id": "Cowan, Anthony"},
    {"code": "ErAyala", "id": "Ayala, Eric"},
    {"code": "DaMorsell", "id": "Morsell, Darryl"},
    {"code": "AaWiggins", "id": "Wiggins, Aaron"},
    {"code": "JaSmith", "id": "Smith, Jalen"},
]
apply_relative_positional_overrides(results, "Men_Maryland_2019/20")
```

### apply_weak_priors {#apply_weak_priors}

`apply_weak_priors(field: 'str', player_poss_pcts: 'list[float]', prior_info: 'RapmPriorInfo', debug_mode: 'bool' = False) -> 'Callable[[float, list[float]], list[float]]'`

Build a closure that nudges ridge-regressed RAPM back towards its weak prior.

Faithful port of `RapmUtils.applyWeakPriors` (`RapmUtils.ts:921-995`).
Ridge regression depresses estimates towards `0`; this "fills" the
team-total error (see `pick_ridge_regression`'s
`[IMPORTANT-EQUATION-01]` team-total reconciliation) back in using each
player's weak (KenPom-derived) prior as the fallback signal, capped so no
more than half the team-total error gets attributed via this path
(`max_multiplier = -0.5`) -- an alternate flat-translation path
(`use_alt_rating`) kicks in for `off_adj_ppp`/`def_adj_ppp` fields
when the capped path can't fully explain the error.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `field` | `str` |  | The prior key to read off each `prior_info["players_weak"]` entry, e.g. `"off_adj_ppp"`. |
| `player_poss_pcts` | `list[float]` |  | Per-player possession-share weights (index-aligned with `prior_info["players_weak"]`), e.g. `pick_ridge_regression`'s own `pct_by_player[off_or_def]`. |
| `prior_info` | `RapmPriorInfo` |  | A `RapmPriorInfo` (only `["players_weak"]` is read). |
| `debug_mode` | `bool` | `False` | Kept for TS signature parity -- upstream gates a `console.log` behind this flag (`RapmUtils.ts:979-984`), which this port deliberately does not reproduce: every production call site pins it `False` (`offDefDebugMode.off`/`.def` are hardcoded `False` constants inside `pickRidgeRegression`), so it is dead in every current caller and would only ever emit console noise, not test-observable behavior. |

**Returns**

A closure `(error, base_results) -> adjusted_results` -- call it with the team-total efficiency error and the pre-adjustment RAPM vector to get the weak-prior-nudged result.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import apply_weak_priors

nudge = apply_weak_priors("off_adj_ppp", pct_by_player, ctx["prior_info"])
adjusted = nudge(adj_eff_err_pre_prior, results_pre_prior)
```

### as_of_ratings_split {#as_of_ratings_split}

`as_of_ratings_split(results: 'pl.DataFrame', cutoff_date: 'datetime.date') -> 'pl.DataFrame'`

Filter a results frame to games strictly before a cutoff date (leakage boundary).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `results` | `DataFrame` |  | A `polars.DataFrame` with a `date` column. |
| `cutoff_date` | `date` |  | Games on or after this date are excluded. |

**Returns**

A `polars.DataFrame` containing only rows with `date < cutoff_date`.

**Example**

```python
import datetime as dt
from sportsdataverse._common.metrics import as_of_ratings_split
as_of_ratings_split(results, dt.date(2023, 9, 8))
```

### as_of_season_split {#as_of_season_split}

`as_of_season_split(df: 'pl.DataFrame', target_season: 'int') -> 'pl.DataFrame'`

Rows strictly before `target_season` -- the leakage boundary.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | Frame with an integer `season` column. |
| `target_season` | `int` |  | The season being predicted; its rows (and later) drop. |

**Returns**

The subset with `season < target_season`.

**Example**

```python
from sportsdataverse.mbb.mbb_player_value_constants import as_of_season_split
prior = as_of_season_split(df, 2026)
```

### assign_to_right_lineup {#assign_to_right_lineup}

`assign_to_right_lineup(state: 'PossState', team_stats: 'PossCalcFragment', opponent_stats: 'PossCalcFragment', clump: 'ConcurrentClump', prev_clump: 'ConcurrentClump') -> 'list[LineupEvent]'`

Assign a clump's possessions to the lineup(s) ending in it

(`PossessionUtils.assign_to_right_lineup`, `PossessionUtils.scala
:418-518`).

Applies the running `state` total (accumulated since the last lineup
boundary) to the *first* ending lineup only, then hands off to
`lineup_balancer` (this clump's own fragment, split across
candidates if there's more than one) and finally `lineup_fixer`
(the negative-possession clamp).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `state` | `PossState` |  | The running possession state since the last lineup boundary. |
| `team_stats` | `PossCalcFragment` |  | This clump's team-direction fragment. |
| `opponent_stats` | `PossCalcFragment` |  | This clump's opponent-direction fragment. |
| `clump` | `ConcurrentClump` |  | The merged clump ending one or more lineups. |
| `prev_clump` | `ConcurrentClump` |  | The previous merged clump. |

**Returns**

The lineup(s) ending in this clump, enriched with possession counts. Empty if `clump.lineups` is empty (see the module docstring's landmine-index note -- unreachable via `calculate_possessions_by_event`).

### attr_regex_filter {#attr_regex_filter}

`attr_regex_filter(tags: 'list[Tag]', attr: 'str', regex: 'str') -> 'list[Tag]'`

JSoup `[attr~=regex]`: candidates whose `attr` value matches

`regex`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `tags` | `list[Tag]` |  | Candidate tags to filter (typically the result of an earlier `.select()`/`.find_all()` call). |
| `attr` | `str` |  | The attribute name to test. |
| `regex` | `str` |  | The pattern the attribute value must `re.search`-match. |

**Returns**

The subset of `tags` that have `attr` set and whose value matches `regex`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_html import attr_regex_filter, parse_html
soup = parse_html('<div width="45%"></div><div width="10%"></div>')
attr_regex_filter(soup.find_all("div"), "width", r"^\[?4?5")
```

### bootstrap_ari {#bootstrap_ari}

`bootstrap_ari(fit_fn: "'Callable[[np.ndarray], tuple[np.ndarray, np.ndarray]]'", X: 'np.ndarray', n_boot: 'int' = 20, seed: 'int' = 0) -> 'float'`

Cluster stability: mean ARI between the full fit and bootstrap refits.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fit_fn` | `Callable[[ndarray], tuple[ndarray, ndarray]]` |  | `X -> (centers, labels)` (e.g. a seeded `kmeans_fit` partial). |
| `X` | `ndarray` |  | Feature matrix. |
| `n_boot` | `int` | `20` | Bootstrap resamples. |
| `seed` | `int` | `0` | RNG seed. |

**Returns**

Mean adjusted Rand index of the resample fits' assignments (of the FULL sample, via nearest refit center) vs the full-fit labels.

**Example**

```python
from functools import partial
score = bootstrap_ari(lambda Z: kmeans_fit(Z, 8, seed=0), Z, n_boot=20, seed=0)
```

### box_aware_compare {#box_aware_compare}

`box_aware_compare(candidate_in: 'str', box_name_in: 'str') -> 'MatchResult'`

Score how well a single play-by-play candidate name fits a single

box-score name (`NameFixer.box_aware_compare`, `:658-766`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `candidate_in` | `str` |  | The raw play-by-play name fragment. |
| `box_name_in` | `str` |  | One box-score player's full name (`"Surname, First [Middle]"` format). |

**Returns**

A `StrongSurnameMatch` / `WeakSurnameMatch` / `NoSurnameMatch`, per the surname- and whole-name-score thresholds.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_names import box_aware_compare
box_aware_compare("Tuitele, Peanut", "Tuitele, Peanut")
# StrongSurnameMatch(box_name='Tuitele, Peanut', score=100)
```

### brier_score {#brier_score}

`brier_score(y_true: 'np.ndarray', p_pred: 'np.ndarray') -> 'float'`

Mean squared error between predicted probabilities and binary outcomes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `y_true` | `ndarray` |  | Array of binary outcomes (0/1). |
| `p_pred` | `ndarray` |  | Array of predicted probabilities in [0, 1]. |

**Returns**

The Brier score (0.0 is a perfect forecast).

**Example**

```python
import numpy as np
from sportsdataverse._common.metrics import brier_score
brier_score(np.array([1, 0]), np.array([0.9, 0.1]))
```

### cached_path {#cached_path}

`cached_path(path: 'str', *, cache_dir: 'Optional[Path]' = None) -> 'Path'`

Return the on-disk cache file path for *path*, without touching it.

Layout: `{cache_dir}/stats.ncaa.org/{dirs...}/{last}.html`, where the
URL path's `/`-separated segments become nested directories and a
query string is folded into the final filename as {safe_query}.html`
(unsafe characters replaced with `). Two different query strings for
the same base path therefore always produce two distinct cache files.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  |  |
| `cache_dir` | `Optional[Path]` | `None` |  |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_fetch import cached_path
cached_path("contests/4690813/play_by_play")
# .../stats.ncaa.org/contests/4690813/play_by_play.html
cached_path("contests/4690813/box_score?period_no=2")
# .../stats.ncaa.org/contests/4690813/box_score__period_no=2.html
```

### calibration_table {#calibration_table}

`calibration_table(y_true: 'np.ndarray', p_pred: 'np.ndarray', n_bins: 'int' = 10) -> 'pl.DataFrame'`

Bucket predicted probabilities into bins and compare to actual outcome rates.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `y_true` | `ndarray` |  | Array of binary outcomes (0/1). |
| `p_pred` | `ndarray` |  | Array of predicted probabilities in [0, 1]. |
| `n_bins` | `int` | `10` | Number of equal-width probability bins. |

**Returns**

A `polars.DataFrame` with columns `bin_mid`, `mean_pred`, `mean_actual`, `n` (one row per non-empty bin).

**Example**

```python
import numpy as np
from sportsdataverse._common.metrics import calibration_table
calibration_table(np.array([1, 0, 1, 0]), np.array([0.9, 0.1, 0.8, 0.2]))
```

### categorize_bad_lineups {#categorize_bad_lineups}

`categorize_bad_lineups(lineup_events: 'list[LineupEvent]') -> 'dict[int, tuple[int, int]]'`

Aggregates bad lineup events for display, by clump-leader player count

(`LineupErrorAnalysisUtils.categorize_bad_lineups`, `:617-633`,
display-only -- the Scala doc comment says "can live without tests").

Re-clumps `lineup_events` (each paired with `next_good=None` --
`clump_bad_lineups`'s grouping predicate never inspects
`next_good`, so this re-clumping is faithful to the Scala's own
`lineup_events.map(e => (e, None))`), then groups the resulting clumps
by `len(clump.evs[0].players)` (the FIRST event's player count -- `5`
means a lineup with a bad *player*, not a bad *count*).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup_events` | `list[LineupEvent]` |  | The bad lineup events to categorize, in chronological order. |

**Returns**

Player count -> `(num_clumps, total_possessions)`, where `total_possessions` sums `team_stats.num_possessions` across every event in every clump in that group.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stint_validation import categorize_bad_lineups
categorize_bad_lineups([bad_ev])  # {5: (1, bad_ev.team_stats.num_possessions)}
```

### classify_point_value {#classify_point_value}

`classify_point_value(dist_ft: 'float', x: 'float', y: 'float', *, league: 'str', season: 'int') -> 'int'`

2 or 3 from basket-relative geometry (arc radius + corner band).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dist_ft` | `float` |  | Euclidean distance from the basket, feet. |
| `x` | `float` |  | Lateral offset from the basket, feet (baseline direction). |
| `y` | `float` |  | Distance up-court from the basket, feet. |
| `league` | `str` |  | `"mens"` or `"womens"`. |
| `season` | `int` |  | Season-ending year (selects the arc era). |

**Returns**

`3` at/beyond the arc or in the corner band, else `2`.

**Example**

```python
from sportsdataverse.mbb.mbb_shots_adapter import classify_point_value
classify_point_value(24.0, 0.0, 24.0, league="mens", season=2020)
```

### classify_zone_geometry {#classify_zone_geometry}

`classify_zone_geometry(dist_ft: 'float', x: 'float', y: 'float', *, league: 'str', season: 'int') -> 'str'`

Shot zone from geometry: `rim | paint | mid | corner3 | abovebreak3`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dist_ft` | `float` |  | Euclidean distance from the basket, feet. |
| `x` | `float` |  | Lateral offset from the basket, feet. |
| `y` | `float` |  | Distance up-court from the basket, feet. |
| `league` | `str` |  | `"mens"` or `"womens"`. |
| `season` | `int` |  | Season-ending year (selects the arc era). |

**Returns**

One of `rim`, `paint`, `mid`, `corner3`, `abovebreak3`.

**Example**

```python
from sportsdataverse.mbb.mbb_shots_adapter import classify_zone_geometry
classify_zone_geometry(2.0, 0.0, 2.0, league="mens", season=2020)
```

### classify_zone_type {#classify_zone_type}

`classify_zone_type(type_text: "'str | None'") -> "'str | None'"`

Collapse a source shot-type label to `rim | arc3 | jump`.

Note: the 2025+ ESPN shots release carries NO three-point marker in
`type_text` (vocabulary is JumpShot/LayUpShot/DunkShot/TipShot), so
`arc3` typically comes from geometry/score_value there; the branch
exists for sources that do label threes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `type_text` | `str \| None` |  | Source label (e.g. `"DunkShot"`); `None` passes through. |

**Returns**

`rim`, `arc3`, `jump`, or `None` for null input.

**Example**

```python
from sportsdataverse.mbb.mbb_shots_adapter import classify_zone_type
classify_zone_type("DunkShot")
```

### clump_bad_lineups {#clump_bad_lineups}

`clump_bad_lineups(lineup_events: 'list[tuple[LineupEvent, Optional[LineupEvent]]]') -> 'list[BadLineupClump]'`

Groups consecutive bad lineup events into `BadLineupClump`\ s

(`LineupErrorAnalysisUtils.clump_bad_lineups`, `:229-263`).

The Scala original is a bespoke `foldLeft` (NOT the generic
`Clumper` utility used elsewhere in the codebase) that prepends onto
two nested lists -- the per-clump `evs` and the top-level clump list
-- and reverses both at the end. This port walks the input once and
appends directly (to the current clump's `evs`, or a new clump to the
result list), which produces the identical chronological order as the
Scala's prepend-then-double-reverse without needing an explicit reverse
step: mirroring a "prepend to the front, reverse at the end" fold as a
plain "append to the back" loop is behavior-preserving precisely because
reversing a prepend-built list restores insertion order.

The current clump extends to cover the next `(lineup, next_good)` pair
iff ALL 5 conditions hold, compared against the clump's LAST-ADDED event
(`last`, not its first event) (`:242-249`):

1. `lineup.team == last.team`
2. `lineup.opponent == last.opponent`
3. `lineup.start_min == last.end_min` (no time gap)
4. `len(lineup.players) == len(last.players)`
5. `len(lineup.players_in) == len(lineup.players_out)` -- this checks
   the INCOMING lineup's own in/out balance, not a comparison against
   `last` (an unbalanced sub is a bad sign in isolation, per the
   Scala's own comment at `:247`).

`TeamSeasonId` (`lineup.team` / `.opponent`) is a plain (non-frozen)
dataclass, so `==` is a field-wise value comparison out of the box --
no `PlayerCodeId`-unhashability workaround is needed here, since this
predicate only compares team identities and player-list lengths, never a
set of `PlayerCodeId`.

Each time a clump is extended, `next_good` is REPLACED with the
incoming pair's own second element (`:251`) -- the final clump's
`next_good` is always the LAST-extended event's `next`, discarding
whatever `next_good` an earlier extension set.

Starting a new clump uses the incoming pair's own `next` too (`:234`,
`:253`) -- a fresh clump's `next_good` is never inherited from the
clump before it.

The Scala's third `foldLeft` case (`:255-259`, matching a head clump
whose `evs` is empty) is dead code in practice -- every
`BadLineupClump` this function ever constructs starts with exactly one
event and is only ever appended to, so `evs` can never be empty. Omitted
here with this comment in place of an unreachable branch.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup_events` | `list[tuple[LineupEvent, Optional[LineupEvent]]]` |  | `(lineup_event, next_good_or_None)` pairs, in chronological order. |

**Returns**

The clumps, in chronological order, each with `evs` in chronological order.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stint_validation import clump_bad_lineups
clumps = clump_bad_lineups([(bad_ev, good_ev)])
clumps[0].evs  # [bad_ev]
```

### code_from_box {#code_from_box}

`code_from_box(name: 'str', box_lineup: 'LineupEvent', team: 'Optional[TeamId]' = None) -> 'PlayerCodeId'`

Resolve a tidied player NAME to the box roster's own `PlayerCodeId`.

`~sportsdataverse.mbb.mbb_ncaa_stints.build_player_code` is a
faithful `ExtractorUtils.scala` port and keys a player as
`{first-two-letters}{Surname}`. When two teammates collide on that --
siblings, overwhelmingly --
`~sportsdataverse.mbb.mbb_ncaa_boxscore_parser.validate_box_score`
widens BOTH to full-name codes so the game is not thrown away.

Re-deriving a code from the tidied name after that point silently undoes
the widening: both Morris twins code back to `MaMorris`, one of them
wins the match, and the other DISAPPEARS from the lineup events. Kansas
2010 parsed 110 events with `MarcusMorris` present 18 times and
`MarkieffMorris` present ZERO times -- a game that looks healthy by
every count while a starter is missing.

So the roster is the authority. **Every PBP-side path that needs a code
for a name must call this, never** `build_player_code`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | The tidied player name, as produced by `tidy_player`. |
| `box_lineup` | `LineupEvent` |  | The team's box-score `~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent`, whose `players` carry the (possibly widened) codes. |
| `team` | `Optional[TeamId]` | `None` | Team context for the fallback `build_player_code` call, used only when `name` is not on the roster. |

**Returns**

The roster's `~sportsdataverse.mbb.mbb_ncaa_models.PlayerCodeId` when `name` is on it, else a freshly built one.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_names import code_from_box
code_from_box("Morris, Markieff", box_lineup, box_lineup.team.team)

# The distinction that matters

from sportsdataverse.mbb.mbb_ncaa_stints import build_player_code
build_player_code("Morris, Markieff", team).code  # "MaMorris" -- collides
code_from_box("Morris, Markieff", box_lineup, team).code  # "MarkieffMorris"
```

### combos {#combos}

`combos(first: 'str', last: 'str') -> 'list[str]'`

Generate the three name-string variants NCAA sources use for one

player (`DataQualityIssues.combos`, `DataQualityIssues.scala:330-337`).

The Scala signature takes a single `(String, String)` tuple, but every
call site (including the `fix_combos`/`alias_combos` helpers below
and the upstream `DataQualityIssuesTests` oracle) invokes it with two
positional arguments via Scala's tuple auto-conversion -- ported here as
a plain two-argument function since Python has no such conversion.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `first` | `str` |  | The player's first name. |
| `last` | `str` |  | The player's last name. |

**Returns**

`[f"{last}, {first}", f"{first} {last}", f"{last.upper()},{first.upper()}"]` -- new-box, new-PbP, and old-box/legacy-PbP formats respectively.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_data_quality import combos

combos("Makhi", "Mitchell")
# ['Mitchell, Makhi', 'Makhi Mitchell', 'MITCHELL,MAKHI']
```

### complete_weighted_avg {#complete_weighted_avg}

`complete_weighted_avg(mutable_acc: 'LineupStatSet', harmonic_weighting: 'bool' = False, regress_diffs: 'float' = 0.0) -> 'None'`

Finish a `weighted_avg` accumulator into true weighted averages.

Faithful port of `LineupUtils.completeWeightedAvg` (`LineupUtils.ts:752`).
Mutates `mutable_acc` in place and returns `None`, mirroring the
upstream `void` + mutable-arg contract. Recomputes the per-field weight
tables from `mutable_acc` itself (`getSimpleWeights(mutableAcc, 1,
regressDiffs)` -- note the `default_val=1`, unlike `weighted_avg`'s
`default_val=0`), then, unless `harmonic_weighting` is set, calls
recalculate_play_type_poss` to fix up the transition/scramble
possession fields that `weighted_avg` skipped. Finally divides every
non-ignored field's accumulated weighted sum by its matching weight
total (shot-type / `ppp_totals` / `orb_totals` / `fta_totals` /
`ast_totals` / generic FGA fallback); `total_*` and `SUM_FIELDS`
fields are left untouched (they are already true totals, not sums to be
averaged). `off_ftr` / `def_ftr` get a special non-`harmonic_weighting`
recompute straight from the accumulated `total_{off|def}_fta` rather
than dividing their own weighted sum.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `mutable_acc` | `LineupStatSet` |  | The `weighted_avg`-accumulated `LineupStatSet` to finish in place. Every field with a non-`total_`/`SUM_FIELDS` key is converted from a weighted sum to a weighted average. |
| `harmonic_weighting` | `bool` | `False` | When `True`, skips the recalculate_play_type_poss` fixup and uses a harmonic-style division for `off_ftr`/`def_ftr` instead of the totals-based recompute. Matches the upstream default (`False`) used by `calculate_aggregated_lineup_stats`. |
| `regress_diffs` | `float` | `0.0` | Forwarded to get_simple_weights` -- regression toward ~1000 possessions for on/off diff calculations. Defaults to `0.0` (no regression), matching `calculate_aggregated_lineup_stats`'s call site. |

**Returns**

None. `mutable_acc` is mutated in place.

**Example**

```python
from sportsdataverse.mbb.mbb_lineup_stats import weighted_avg, complete_weighted_avg

acc: dict = {}
for lineup in lineups:
    weighted_avg(acc, lineup)
complete_weighted_avg(acc)
print(acc["off_ppp"]["value"])  # now a true weighted average
```

### compute_league_averages_from_per_game {#compute_league_averages_from_per_game}

`compute_league_averages_from_per_game(teams: 'Sequence[TeamDetail]', fields: 'Sequence[str]' = ('efg', '3p', '2pmid', '2prim')) -> 'LeagueAverages'`

Possession-weighted league means per field (`computeLeagueAveragesFromPerGame`, `ts:189-221`).

For each field, the weighted mean of every team's per-game raw rate over
all their games; only games with a non-`None` raw and a positive weight
contribute. An empty accumulator yields `0`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `teams` | `Sequence[TeamDetail]` |  | All teams. |
| `fields` | `Sequence[str]` | `('efg', '3p', '2pmid', '2prim')` | The stat fields to average (default `STRENGTH_ADJUSTED_FIELDS`). |

**Returns**

`{field: {"league_off": float, "league_def": float}}`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import compute_league_averages_from_per_game

teams = [{"team_name": "A", "opponents": [{"off_3p_made": 5, "off_3p_attempts": 10}]}]
print(compute_league_averages_from_per_game(teams, ["3p"])["3p"]["league_off"])  # 0.5
```

### compute_opponent_strengths {#compute_opponent_strengths}

`compute_opponent_strengths(team: 'TeamDetail', team_by_name: 'dict[str, TeamDetail]', fields: 'Sequence[str]', adj_values: 'AdjValues') -> 'dict[str, SideValues]'`

Schedule-weighted opponent strength per field (`computeOpponentStrengths`, `ts:253-299`).

**Cross-named on purpose:** `avg_opp_def` is weighted by the *offensive*
game weights and reads each opponent's `def` adjustment; `avg_opp_off`
is weighted by *defensive* weights and reads the opponent's `off`. Each
opponent value is its current adjusted value, falling back to its raw
per-game value when no adjustment exists yet. Games whose opponent is not
in `team_by_name` (or whose off+def weights are both `<= 0`) are
skipped.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team` | `TeamDetail` |  | The team whose schedule is being summarized. |
| `team_by_name` | `dict[str, TeamDetail]` |  | `{team_name: team_detail}` for opponent lookup. |
| `fields` | `Sequence[str]` |  | The stat fields to compute. |
| `adj_values` | `AdjValues` |  | Current `{team_name: field: {"off","def"}}` adjustments. |

**Returns**

`{field: {"avg_opp_def": float, "avg_opp_off": float}}`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import compute_opponent_strengths

team = {"team_name": "A", "opponents": [{"oppo_name": "B", "off_3p_attempts": 10}]}
by_name = {"A": team, "B": {"team_name": "B"}}
adj = {"B": {"3p": {"off": 0.5, "def": 0.3}}}
print(compute_opponent_strengths(team, by_name, ["3p"], adj)["3p"]["avg_opp_def"])  # 0.3
```

### compute_possession_splits {#compute_possession_splits}

`compute_possession_splits(team: 'TeamDetail') -> 'PossessionSplits'`

Home/away/neutral possession totals for a team (`computePossessionSplits`, `ts:154-186`).

Each opponent game's `off_poss`/`def_poss` (missing -> 0) is bucketed
by `location_type` (missing or any non `"Home"`/`"Away"` value ->
the neutral bucket).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team` | `TeamDetail` |  | A `team_details` team dict. |

**Returns**

A `PossessionSplits`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import compute_possession_splits

team = {"opponents": [{"off_poss": 70, "def_poss": 68, "location_type": "Home"}]}
print(compute_possession_splits(team).home_off_poss)  # 70.0
```

### concurrent_event_handler {#concurrent_event_handler}

`concurrent_event_handler(clumps: 'Iterable[ConcurrentClump]') -> 'list[ConcurrentClump]'`

Batch a stream of singleton/boundary clumps into merged

concurrent-event clumps (`Concurrency.concurrent_event_handler` +
`StateUtils.foldLeft`'s clumping machinery, `PossessionUtils.scala
:71-111` -- see the module docstring for the full batching-predicate
breakdown and the post-game-break singleton port trap).

# ponytail: manual accumulate-and-flush loop replacing the generic
# Clumper/StateUtils.foldLeft abstraction -- this is the ONE clumper
# instantiation in the port, so a reusable abstraction buys nothing.
# Lift this back into a small clumper type if a second concurrent-event
# family needs the same batching later.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clumps` | `Iterable[ConcurrentClump]` |  | An ordered stream of `ConcurrentClump`\ s, each either a singleton raw event (`evs=[ev]`) or a lineup-boundary marker (`evs=[]`, `lineups=[lineup]`), e.g. from `lineup_as_raw_clumps`. |

**Returns**

The merged clumps, each an in-order concatenation of one batch's `evs`/`lineups`.

### convert_from_digits {#convert_from_digits}

`convert_from_digits(name: 'str', player_numbers: 'list[PlayerCodeId]') -> 'Optional[str]'`

Resolve a jersey-number-only name to its box-score player

(`LineupErrorAnalysisUtils.convert_from_digits`, `:166-175`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | The candidate name; only matches if every character is a digit (an empty string is vacuously all-digit, matching Scala's `forall` on an empty `String`). |
| `player_numbers` | `list[PlayerCodeId]` |  | Candidate `(code, id)` pairs -- typically `box_lineup.players_out`, since a number-only PbP mention almost always refers to a player who just left the game. |

**Returns**

The matching player's full name, or `None` if `name` isn't all-digit or no code matches.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_models import PlayerCodeId, PlayerId
from sportsdataverse.mbb.mbb_ncaa_names import convert_from_digits
codes = [PlayerCodeId(code="1000", id=PlayerId("name1"))]
convert_from_digits("1000", codes)  # "name1"
```

### convert_from_initials {#convert_from_initials}

`convert_from_initials(name: 'str', codes_to_names: 'dict[str, str]') -> 'Optional[str]'`

Resolve a 2-initial name (`"A B"` / `"B, A"`) to the single

box-score player whose code starts with those initials
(`LineupErrorAnalysisUtils.convert_from_initials`, `:147-164`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | The candidate initials string. |
| `codes_to_names` | `dict[str, str]` |  | Player code -> full name (e.g. `TidyPlayerContext.all_players_map`). |

**Returns**

The single matching full name, or `None` if `name` isn't an initials shorthand, or if zero or multiple codes match.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_names import convert_from_initials
convert_from_initials("A B", {"AoBo": "name1"})  # "name1"
```

### count_matching {#count_matching}

`count_matching(evs: 'Iterable[RawGameEvent]', side: 'DirFn', *parsers: 'Parser') -> 'int'`

Count events on one side matching any of the given parsers.

Ports the pervasive `clump.evs.collect { case side(ParseX(_)) => () }
.size` idiom (and its multi-arm `case side(ParseX(_)) => ();
case side(ParseY(_)) => ()` union form, when more than one parser is
passed -- e.g. the and-one free-throw count, which matches *either* a
made or a missed free throw on the same event).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `evs` | `Iterable[RawGameEvent]` |  | The events to scan. |
| `side` | `DirFn` |  | `~sportsdataverse.mbb.mbb_ncaa_models.PossessionEvent .attacking_team` or `.defending_team`, selecting which raw string (if any) to test per event. |

**Returns**

The count of matching events.

### create_lineup_data {#create_lineup_data}

`create_lineup_data(filename: 'str', in_html: 'str', box_lineup: 'LineupEvent', format_version: 'int') -> 'Union[tuple[list[LineupEvent], list[LineupEvent]], list[ParseError]]'`

Combines the different methods to build a set of lineup events

(`PlayByPlayParser.create_lineup_data`, `:153-217`) -- the
orchestrator that chains the ENTIRE Phase 5a-5d surface:

1. `parse_game_events` -- HTML -> reversed
   `~sportsdataverse.mbb.mbb_ncaa_stints.PlayByPlayEvent`\ s.
2. `~sportsdataverse.mbb.mbb_ncaa_stints.build_partial_lineup_list`
   -- events -> chronological lineup stints.
3. `~sportsdataverse.mbb.mbb_ncaa_lineup_enrich.fix_possible_score_swap_bug`
   -- undoes a rare NCAA score-transposition bug.
4. `~sportsdataverse.mbb.mbb_ncaa_lineup_enrich.enrich_lineup`
   (mapped over every stint) -- populates `pts`/`plus_minus`/stat
   trees.
5. `~sportsdataverse.mbb.mbb_ncaa_possessions.calculate_possessions`
   -- per-stint possession counts.
6. Zip each stint with its successor (`None` for the last), then
   `~sportsdataverse.mbb.mbb_ncaa_stint_validation.validate_lineup`
   partitions the `(stint, next)` pairs into good (empty error list)
   and bad.
7. `~sportsdataverse.mbb.mbb_ncaa_stint_validation.clump_bad_lineups`
   groups consecutive bad stints, then
   `~sportsdataverse.mbb.mbb_ncaa_stint_validation.analyze_and_fix_clumps`
   tries to self-heal each clump.
8. Concatenate: good stints + every clump's fixed stints -> `good`;
   every clump's still-unfixed stints -> `bad`, each stamped with
   `player_count_error=len(players)` as the VERY LAST step.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filename` | `str` |  | The source file name, used only for error reporting. |
| `in_html` | `str` |  | The raw play-by-play-page HTML. |
| `box_lineup` | `LineupEvent` |  | The team's validated box-score lineup (`~sportsdataverse.mbb.mbb_ncaa_boxscore_parser.get_box_lineup`'s result) -- supplies the full roster (for validation), the team/year (for parsing), and the trusted final score (for the swap-bug fix). |
| `format_version` | `int` |  | `0` for the legacy layout, `1` for the 2018+ layout. |

**Returns**

`(good_lineups, bad_lineups)` on success, or a `list[ParseError]` if `parse_game_events` failed.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup
from sportsdataverse.mbb.mbb_ncaa_models import TeamId
from sportsdataverse.mbb.mbb_ncaa_pbp_parser import create_lineup_data

with open("tests/fixtures/ncaa/test_lineup.html", encoding="utf-8") as f:
    box_html = f.read()
box_lineup = get_box_lineup("test_p1.html", box_html, TeamId("TeamA"), format_version=0)

with open("tests/fixtures/ncaa/test_play_by_play.html", encoding="utf-8") as f:
    pbp_html = f.read()
result = create_lineup_data("test.html", pbp_html, box_lineup, format_version=0)

# Pipeline next step (one line)

    good, bad = result
    sum(ev.duration_mins for ev in good + bad)
```

### create_player_events {#create_player_events}

`create_player_events(lineup_event_maybe_bad: 'LineupEvent', box_lineup: 'LineupEvent') -> 'list[PlayerEvent]'`

Split a lineup event into one :class:`~sportsdataverse.mbb

.mbb_ncaa_models.PlayerEvent` per player on the floor
(`create_player_events`, `LineupUtils.scala:1454-1529`).

First re-tidies `lineup_event_maybe_bad`'s `players`/`players_in`/
`players_out` against `box_lineup` (via player_tidier`),
dropping any player who doesn't actually resolve to a box-score player --
this recovers from "impossible" lineups. Then, for each surviving player
(in lineup-slot order, 0-4), builds their own `enrich_stats` call
with a per-player `player_filter_coder` + that player's slot index (the
only caller in this module that ever passes a non-default
`player_index`, wiring increment_player_3p_shot_info`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup_event_maybe_bad` | `LineupEvent` |  | The lineup event to split (its player lists may reference names not actually in `box_lineup`). |
| `box_lineup` | `LineupEvent` |  | The trusted box-score lineup for this game (name resolution + team-scoping context). |

**Returns**

One `~sportsdataverse.mbb.mbb_ncaa_models.PlayerEvent` per (tidied) player in `lineup_event_maybe_bad.players`, same order. Kept even if a player has zero matching raw events -- needed downstream for usage/possession math.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import create_player_events

player_events = create_player_events(lineup, box_lineup)
player_events[0].player_stats.fg_3p.made.total
```

### create_shot_event_data {#create_shot_event_data}

`create_shot_event_data(filename: 'str', in_html: 'str', box_lineup: 'LineupEvent') -> 'Union[list[ShotEvent], list[ParseError]]'`

Parses a game page's SVG shot map into a list of :class:`~sportsdataverse

.mbb.mbb_ncaa_models.ShotEvent` (`ShotEventParser.create_shot_event_data`,
`:175-259`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filename` | `str` |  | The source file name, used only for error reporting. |
| `in_html` | `str` |  | The raw game-page HTML (containing the SVG shot map, either baked in as `circle.shot` elements or built client-side via an `addShot(...)` JS call -- see `shot_js_to_html`). |
| `box_lineup` | `LineupEvent` |  | The team's box-score lineup event (supplies `team`/ `year`/`location_type` and the tidy-name lookup context). |

**Returns**

Every shot found, sorted chronologically and court-geometry enriched, or a `list[ParseError]` if the HTML couldn't be parsed, the team names couldn't be matched, no shot events were found (even after the JS fallback), or any one circle failed to parse (the first such failure's error(s) only -- Scala's `.sequence` over `List[Either[...]]` is fail-fast, not accumulating).

**Example**

```python
from pathlib import Path
from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup
from sportsdataverse.mbb.mbb_ncaa_models import TeamId
from sportsdataverse.mbb.mbb_ncaa_shot_parser import create_shot_event_data

box_html = Path("tests/fixtures/ncaa/test_lineup.html").read_text(encoding="utf-8")
box_lineup = get_box_lineup("test_p1.html", box_html, TeamId("TeamA"), format_version=1)
shots = create_shot_event_data("test_p1.html", box_html, box_lineup)
```

### display_name_to_roster_key {#display_name_to_roster_key}

`display_name_to_roster_key(name: 'Optional[str]') -> 'str'`

`"Ballisager Webb, Jermaine"` -> `"JERMAINE.BALLISAGER.WEBB"`.

Box-score and shot-chart pages render a player as `"Surname, First"`,
while `team_rosters` renders the same person as `FIRST.MIDDLE.LAST`
uppercase -- whitespace becomes dots, hyphens collapse, diacritics fold.
Joining the two needs one canonical direction, and this is it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `Optional[str]` |  | The display name (`"Surname, First"`), or `None`. |

**Returns**

The roster-style key, or `""` when the name cannot be split into at least a surname and a first name. An empty key never matches, which is the intended outcome -- an unresolved row beats a wrong join.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_names import display_name_to_roster_key

display_name_to_roster_key("Clark, Garry")            # "GARRY.CLARK"
display_name_to_roster_key("Wrightsell Jr., Latrell") # "LATRELL.WRIGHTSELL"
display_name_to_roster_key('"TJ" Madlock, Antonio')   # "ANTONIO.MADLOCK"
```

### duration_from_period {#duration_from_period}

`duration_from_period(period: 'int', is_women_game: 'bool') -> 'float'`

The game duration (minutes elapsed) once `period` has completed

(`ExtractorUtils.scala:286-287`: `start_time_from_period(period + 1,
...)`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `period` | `int` |  | The 1-indexed period number. |
| `is_women_game` | `bool` |  | Whether to use the women's or men's period schedule. |

**Returns**

The game-clock minute at the end of `period`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stints import duration_from_period
duration_from_period(2, is_women_game=False)  # 40.0 (end of men's regulation)
duration_from_period(4, is_women_game=True)  # 40.0 (end of women's regulation)
```

### enrich_and_reverse_game_events {#enrich_and_reverse_game_events}

`enrich_and_reverse_game_events(in_events: 'list[PlayByPlayEvent]') -> 'list[PlayByPlayEvent]'`

Inserts game-break events and turns descending per-row times into

ascending game-clock minutes, returning the whole list latest-to-earliest
(`PlayByPlayParser.enrich_and_reverse_game_events`, `:297-370`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `in_events` | `list[PlayByPlayEvent]` |  | The raw parsed events, earliest to latest, with each `.min` still a per-period DESCENDING clock reading. |

**Returns**

`in_events` with `~sportsdataverse.mbb.mbb_ncaa_stints .GameBreakEvent`\ s inserted at every period boundary, every `.min` converted to an ASCENDING whole-game reading, and a trailing (once reversed, LEADING) `~sportsdataverse.mbb .mbb_ncaa_stints.GameEndEvent` -- the whole list in LATEST-TO-EARLIEST order (the caller is expected to `reversed(...)` it back when chronological order is wanted, exactly like `get_sorted_pbp_events` does).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_models import Score
from sportsdataverse.mbb.mbb_ncaa_pbp_parser import enrich_and_reverse_game_events
from sportsdataverse.mbb.mbb_ncaa_stints import OtherTeamEvent

events = [OtherTeamEvent(18.0, Score(1, 1), "tipoff")]
reversed_enriched = enrich_and_reverse_game_events(events)
reversed_enriched[0].__class__.__name__  # 'GameEndEvent'
```

### enrich_lineup {#enrich_lineup}

`enrich_lineup(lineup: 'LineupEvent') -> 'LineupEvent'`

Populate `pts`/`plus_minus` from the score delta, then run the

full stat-tree enrichment (`enrich_lineup`, `LineupUtils.scala:29-46`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup` | `LineupEvent` |  | The lineup event to enrich (not mutated -- see the module docstring's "Scala idiom decisions"). |

**Returns**

A new `~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent` with `team_stats`/`opponent_stats` fully populated.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import enrich_lineup

enriched = enrich_lineup(lineup)
enriched.team_stats.pts
```

### enrich_shot_events_with_pbp {#enrich_shot_events_with_pbp}

`enrich_shot_events_with_pbp(sorted_shot_events: 'list[ShotEvent]', sorted_pbp_events: 'list[PlayByPlayEvent]', lineup_events: 'list[LineupEvent]', bad_lineup_events: 'list[LineupEvent]', box_lineup: 'LineupEvent') -> 'list[ShotEvent]'`

Enrich each shot with its play-by-play event + on-floor lineup

(`PlayByPlayUtils.enrich_shot_events_with_pbp`,
`PlayByPlayUtils.scala:28-278`).

Folds over the (time-sorted) shots, threading two iterators (play-by-play
and lineup) and a small amount of carry-over state. For each shot it:

1. gathers the play-by-play events at the shot's time (`find_pbp_clump`),
   keeping only the ones on the shot's side (team if `is_off`);
2. picks the matching shot event via a strict -> loose -> first-of-N
   cascade (`right_kind_of_shot` then `matching_player`);
3. locates the on-floor lineup (`find_lineup`), falling back to
   `bad_lineup_events` if the good lineups yield nothing (a bad-lineup
   match is used for `players` but its id is suppressed);
4. attributes an assist (a same-time non-self assist event) and transition
   flag (`"fastbreak"` in the event string), and fills in `lineup_id` /
   `players` / `pts` / `value` / `ast_by` / `is_ast` / `is_trans`
   -- exactly the fields Task 5e.5's parser left as placeholders.

Shots with no matching play-by-play clump, no matching shot event, or no
matching lineup are dropped (the Scala logs a `WARN` and discards; the
logging is dropped per the module note, the discard preserved).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sorted_shot_events` | `list[ShotEvent]` |  | Shots in ascending game-clock order. |
| `sorted_pbp_events` | `list[PlayByPlayEvent]` |  | The full play-by-play event stream, ascending. |
| `lineup_events` | `list[LineupEvent]` |  | The good (validation-passing) stint events. |
| `bad_lineup_events` | `list[LineupEvent]` |  | The validation-flagged stint events, used only as a last resort (their ids are never attributed). |
| `box_lineup` | `LineupEvent` |  | The roster lineup event (drives name resolution). |

**Returns**

The enriched, still-time-sorted list of shots (a subset of the input -- unmatchable shots are dropped).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_pbp_glue import enrich_shot_events_with_pbp
enriched = enrich_shot_events_with_pbp(
    shots, pbp, good_lineups, bad_lineups, box_lineup
)
```

### enrich_stats {#enrich_stats}

`enrich_stats(lineup: 'LineupEvent', event_parser: 'PossessionEvent', stats: 'LineupEventStats', player_filter_coder: 'Optional[PlayerFilterCoder]' = None, player_index: 'int' = -1) -> 'LineupEventStats'`

Fold a lineup's raw events into a counting-stat tree (``protected def

enrich_stats`, `LineupUtils.scala:115-162``). Reuses the Task 5a.3
concurrent-clump batching (`~sportsdataverse.mbb.mbb_ncaa_possessions
.lineup_as_raw_clumps` + `~sportsdataverse.mbb.mbb_ncaa_possessions
.concurrent_event_handler`) rather than duplicating it -- both were
already public/exported from Task 5a.3.

`stats` is deep-copied once up front (see the module docstring's
"Scala idiom decisions"), so this function never mutates the caller's
`stats` argument -- safe to call repeatedly against the same starting
literal (e.g. a shared "empty stats" fixture).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup` | `LineupEvent` |  | The lineup whose `raw_game_events` to fold over. |
| `event_parser` | `PossessionEvent` |  | Selects which side (team/opponent) is "attacking". |
| `stats` | `LineupEventStats` |  | The starting stat tree (not mutated -- see above). |
| `player_filter_coder` | `Optional[PlayerFilterCoder]` | `None` | Optional `name -> (is_this_player, code)` predicate/coder, for per-player scoping (Task 5c.4). |
| `player_index` | `int` | `-1` | Lineup-slot index for `~sportsdataverse.mbb .mbb_ncaa_models.PlayerShotInfo` tuples (Task 5c.4; `-1` for team-level calls, the only value exercised before then). |

**Returns**

A new `~sportsdataverse.mbb.mbb_ncaa_models.LineupEventStats` with every matching event folded in.

### ensure_ev_uniqueness {#ensure_ev_uniqueness}

`ensure_ev_uniqueness(clump: 'ConcurrentClump') -> 'ConcurrentClump'`

Nudge each event's `min` by a tiny per-index delta so truly

concurrent (identical-`min`) events within a clump don't collapse
under `==` (`ensure_ev_uniqueness`, `LineupUtils.scala:105-111`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clump` | `ConcurrentClump` |  | The clump whose events to nudge. |

**Returns**

A new `~sportsdataverse.mbb.mbb_ncaa_possessions.ConcurrentClump` with each event's `min` incremented by `1e-6 * index`.

### espn_shots_to_canonical {#espn_shots_to_canonical}

`espn_shots_to_canonical(espn: 'pl.DataFrame', *, league: 'str', season: 'int', scale: "'tuple[float, float, float] | None'" = None) -> 'pl.DataFrame'`

ESPN `load_mbb_shots` frame -> the canonical shot frame.

Field-goal attempts only (free throws and sentinel-coordinate rows are
dropped). `point_value` comes from `score_value` -- the release
populates it on misses too, and its `type_text` carries NO three-point
marker, so `arc3` is value-derived. Coordinates are re-based to the
fitted basket origin and scaled to feet.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `espn` | `DataFrame` |  | `load_mbb_shots`-shaped frame. |
| `league` | `str` |  | `"mens"` or `"womens"`. |
| `season` | `int` |  | Season-ending year. |
| `scale` | `tuple[float, float, float] \| None` | `None` | Optional pre-fitted `(origin_x, origin_y, feet_per_unit)`; fitted from `espn` when `None`. |

**Returns**

The canonical shot frame (`CANONICAL_SHOT_SCHEMA`); empty input returns the zero-row schema.

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_mbb_shots
from sportsdataverse.mbb.mbb_shots_adapter import espn_shots_to_canonical
df = espn_shots_to_canonical(load_mbb_shots([2025]), league="mens", season=2025)
```

### espn_wbb_teams {#espn_wbb_teams}

`espn_wbb_teams(groups=None, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_wbb_teams - look up the women's college basketball teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `groups` | `int` | `None` | Used to define different divisions. 50 is Division I, 51 is Division II/Division III. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams for the requested league. This function caches by default, so if you want to refresh the data, use the command sportsdataverse.wbb.espn_wbb_teams.clear_cache().

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
| `team_logos` | integer | Team logo metadata. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_nickname` | character | Team nickname. |
| `team_short_display_name` | character | Short team display name (e.g. 'Aces'). |
| `team_slug` | character | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_uid` | character | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |

**Example**

```python
from sportsdataverse.wbb import espn_wbb_teams
teams = espn_wbb_teams()
print(teams.shape)
print(teams.columns[:8])

# Walk every team-id (handy for batched scrapes)

team_ids = teams["team_id"].to_list()
print(len(team_ids), "D1 teams")

# Pandas round-trip + Division II/III

d2_d3 = espn_wbb_teams(groups=51, return_as_pandas=True)
d2_d3.head()
```

### extract_player_from_ev {#extract_player_from_ev}

`extract_player_from_ev(shot: 'ShotEvent', pbp_event: 'MiscGameEvent', tidy_ctx: 'TidyPlayerContext') -> 'Optional[PlayerCodeId]'`

Resolve the player named in `pbp_event` to a

`~sportsdataverse.mbb.mbb_ncaa_models.PlayerCodeId`
(`ShotEnrichmentUtils.extract_player_from_ev`,
`PlayByPlayUtils.scala:613-635`).

For a shot by the team under analysis (`shot.is_off`) the name is
tidied against the box score before coding (so a mis-spelled play-by-play
name resolves to the roster identity); for an opponent shot it is coded
verbatim with no team context.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shot` | `ShotEvent` |  | The shot being enriched (only `is_off` is read). |
| `pbp_event` | `MiscGameEvent` |  | The play-by-play event naming the player. |
| `tidy_ctx` | `TidyPlayerContext` |  | The name-resolution context for this game. |

**Returns**

The resolved `PlayerCodeId`, or `None` if the event string names no player (`~sportsdataverse.mbb.mbb_ncaa_events.parse_any_play` found nothing).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_pbp_glue import extract_player_from_ev
pc = extract_player_from_ev(shot, pbp_event, tidy_ctx)
```

### field_keys {#field_keys}

`field_keys(field: 'str') -> 'dict[str, str]'`

Off/def stat-key names for a field (`fieldKeys`, `ts:77-79`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `field` | `str` |  | A stat field (`"efg"` / `"3p"` / `"2pmid"` / `"2prim"`). |

**Returns**

`{"off": f"off_{field}", "def": f"def_{field}"}`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import field_keys

keys = field_keys("3p")
print(keys["off"], keys["def"])  # off_3p def_3p
```

### filter_matching_own {#filter_matching_own}

`filter_matching_own(tags: 'list[Tag]', regex: 'str') -> 'list[Tag]'`

JSoup `:matchesOwn(regex)` applied to an already-computed candidate

list, rather than a fresh `root.select(selector)` call (Task 5e.2
addition; see the module docstring's note on composing this with
`attr_regex_filter`).

Same own-text-only semantics as `select_matching_own` -- JSoup's
`Element.ownText()` walks only the element's direct `TextNode`
children, not text nested inside child elements.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `tags` | `list[Tag]` |  | Candidate tags to filter (typically the result of an earlier `.select()`/`attr_regex_filter` call). |
| `regex` | `str` |  | The pattern each candidate's own (whitespace-collapsed) text must `re.search`-match. |

**Returns**

The subset of `tags` whose own text contains a `regex` match, in the input list's order.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_html import attr_regex_filter, filter_matching_own, parse_html
soup = parse_html('<td style="font-size:36px">92</td><td style="color:red">x</td>')
candidates = attr_regex_filter(soup.find_all("td"), "style", r"font-size:36px")
filter_matching_own(candidates, r"[0-9]+")  # [<td style="font-size:36px">92</td>]
```

### find_lineup {#find_lineup}

`find_lineup(shot: 'ShotEvent', curr_pbp: 'Optional[MiscGameEvent]', curr_lineups: 'list[LineupEvent]', lineup_it: "'PeekableIterator[LineupEvent]'") -> 'tuple[Optional[LineupEvent], list[LineupEvent]]'`

Find the lineup (stint) event on the floor for `shot`

(`ShotEnrichmentUtils.find_lineup`, `PlayByPlayUtils.scala:352-517`).

A recursive state machine over three lists: `curr_lineup` (the current
candidate), `fallback_lineups` (time-matching lineups whose raw events
did not contain `curr_pbp` -- kept as fallbacks), and `stashed_lineups`
(lineups pulled from the iterator but not yet stepped into, available for
future shots). The branch cases (labelled 2.1-2.4 in the Scala):

* **2.1** -- no time-matching lineup left: return the fallbacks.
* **2.2** -- the next lineup starts *after* the shot: no match, stash it.
* **2.3** -- strictly inside a lineup with no prior fallbacks: take it.
* **2.4** -- shot is exactly at a lineup boundary (or we are already
  choosing among multiple fallbacks): take this lineup iff its raw game
  events contain `curr_pbp`'s event string (`curr_pbp is None` takes
  it unconditionally); otherwise keep it as a fallback and recurse.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shot` | `ShotEvent` |  | The shot to place (only `min` / `is_off` are read). |
| `curr_pbp` | `Optional[MiscGameEvent]` |  | The already-matched play-by-play event for this shot, used to disambiguate boundary lineups; `None` disables that check. |
| `curr_lineups` | `list[LineupEvent]` |  | Lineups pulled from the iterator on a previous call and still available (the current one first). |
| `lineup_it` | `PeekableIterator[LineupEvent]` |  | The shared lineup iterator (consumed in place). |

**Returns**

`(matched_lineup_or_None, lineups_to_retry_next_time)` -- the second element always includes the matched lineup (so out-of-order shots sharing it still resolve) plus any leftover stash.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_pbp_glue import (
    PeekableIterator,
    find_lineup,
)
matched, retry = find_lineup(shot, None, [lineup], PeekableIterator([]))
```

### find_missing_subs {#find_missing_subs}

`find_missing_subs(clump: 'BadLineupClump', box_lineup: 'LineupEvent', valid_player_codes: 'set[str]') -> 'tuple[list[LineupEvent], BadLineupClump]'`

Trims a clump whose lineups carry TOO MANY players by identifying the

"ghost" player(s) a missing sub-out left behind
(`LineupErrorAnalysisUtils.find_missing_subs`, `:406-514`).

Fires only when the clump's first event has `>= 6` on-floor players
(`:415-416`: `candidates.size < 6` is a no-op). `expected_size_diff`
(`:419`) is `first_event_player_count - 5` -- the number of ghosts the
trim should end up removing.

**Phase 1 -- shrink the candidate pool** (`:437-478`). Starting from the
first event's players, walk the clump chronologically. At each event a
candidate is *confirmed present* (and dropped from the pool) if it subs
out (`ev.players_out`, **skipped for the first event** -- `:445`,
literal port of `clump.evs.headOption.contains(ev)` as value equality
`ev == clump.evs[0]`; for a well-formed clump of distinct events this is
exactly `index == 0`) or is named in one of the event's team-side raw
plays (`parse_any_play` -> `~sportsdataverse.mbb.mbb_ncaa_names
.tidy_player` -> `~sportsdataverse.mbb.mbb_ncaa_stints
.build_player_code`, `:448-456`; unlike `validate_lineup` this
does NOT skip the literal `"team"` token -- ported verbatim).
`matching_index` is the **FIRST** event index at which the pool size
first equals `expected_size_diff` (`:475`); once set it freezes -- all
later events are skipped in phase 1 (`:439-441`).

**Accept gate** (`:479-480`): the final pool must be non-empty and no
larger than `expected_size_diff`. If `matching_index` never fired
(the pool jumped past `expected_size_diff` in a single step, or never
shrank to it), the gate still accepts iff the residual pool is a non-empty
subset of size `<= expected_size_diff` -- in which case phase 3 routes
**every** event through the "before match" branch (`index > None` is
always false). On failure the **original** clump is returned unchanged.

**Phase 3 -- rebuild the events** (`:482-503`, a `scanLeft` ported as
a manual accumulate loop that drops the seed). For events at/before
`matching_index` the ghost pool is simply removed from `players`
(`filterNot`). For events strictly **after** `matching_index`
(`index > matching_index` -- the matched event itself is "before")
`players` is rebuilt from the previous *tidied* event via
`~sportsdataverse.mbb.mbb_ncaa_stints.build_new_player_list` (the
`scanLeft` threads the previously-emitted event; its seed is `None`,
but the first event can never be an "after match" event, so the
`getOrElse(ev)` fallback is only ever a formality -- ported faithfully
all the same). The rebuilt events are partitioned by
`validate_lineup`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clump` | `BadLineupClump` |  | The bad-lineup clump to attempt to repair. |
| `box_lineup` | `LineupEvent` |  | The team's box-score lineup event (roster + name context). |
| `valid_player_codes` | `set[str]` |  | Every player code on the box score / roster. |

**Returns**

`(fixed_lineups, still_to_fix)` -- the now-valid rebuilt events and a clump of the still-invalid ones (carrying the input's `next_good`); or `([], clump)` on a no-op / rejected fix.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stint_validation import (
    find_missing_subs,
)
fixed, still = find_missing_subs(clump, box_lineup, valid_codes)
```
