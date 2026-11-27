# College football — additional Python functions — Models and calculators: cfb_adjusted–cfb_roster

> College football — additional Python functions — Models and calculators: cfb_adjusted–cfb_roster — function reference in sdv-py, the SportsDataverse Python package.

### cfb_adjusted_breakdowns {#cfb_adjusted_breakdowns}

`cfb_adjusted_breakdowns(plays: 'pl.DataFrame | pd.DataFrame', *, metrics: 'Sequence[str] | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Season opponent-adjusted team breakdowns, shrunk toward the league mean for display.

Each breakdown is a slice of the plays `cfb_adjusted_epa` scores (the pass and rush plays that are not
garbage time by the score-margin rule) and a response: EPA, the EPA success flag or the EPA explosive flag. The
slice goes through the same opponent fit as `cfb_adjusted_epa` with that response in place of EPA, then the
displayed value is shrunk toward the league mean by `n / (n + k)` (see the module docstring). Breakdowns whose
adjustment did not improve their reliability are published as the raw value with the same shrinkage and a null
`_adj`. In-sample and descriptive: the fit uses the whole season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame \| DataFrame` |  | A cfbfastR-schema play-by-play frame (polars or pandas) with the columns `cfb_adjusted_epa` reads, plus the ones each requested breakdown slices on: `EPA_success` (or `epa_success`), `EPA_explosive`, `start.down` (or `down`), `standard_down`, `passing_down`, `period`, `pass` and `rush`. One season at a time. |
| `metrics` | `Sequence[str] \| None` | `None` | Breakdown names to return, in that order (default: every published one). Each name covers both sides that are published for it. |
| `return_as_pandas` | `bool` | `False` | Return a pandas `DataFrame` instead of polars. |

**Returns**

One row per team rated in any requested breakdown: `team_id`, `pos_team`, then for each published breakdown and side (`off` = the team's offense, `def` = its defense) `<metric>_<side>_raw`, `_adj` (null when the breakdown is published raw), `_shrunk` (the display value), `_n` (plays) and `_rank` (on `_shrunk`; offense highest first, defense lowest first). A team needs two or more games with plays on both sides of the slice to be rated in it. No rows when every play is garbage time.

| col_name | type | description |
|---|---|---|
| `team_id` | String | Team ESPN id (character join key). |
| `pos_team` | String | Team name as ESPN spells it in the play-by-play. |
| `epa_all_off_raw` | Float64 | Raw EPA per play the offense gained on all downs (pass and rush plays outside garbage time). |
| `epa_all_off_adj` | Float64 | Opponent-adjusted EPA per play the offense gained on all downs (league mean plus the team's mean per-game residual against its opponents' fitted strength). |
| `epa_all_off_shrunk` | Float64 | Display value: the adjusted EPA per play the offense gained on all downs, shrunk toward the league mean by n / (n + 222.5). |
| `epa_all_off_n` | Int64 | Plays behind the offense values: its plays on all downs outside garbage time, pass and rush only (the n in the shrinkage). |
| `epa_all_off_rank` | Float64 | Rank of epa_all_off_shrunk (highest = 1, ties averaged). |
| `epa_all_def_raw` | Float64 | Raw EPA per play the defense allowed on all downs (pass and rush plays outside garbage time). |
| `epa_all_def_adj` | Float64 | Opponent-adjusted EPA per play the defense allowed on all downs (league mean plus the team's mean per-game residual against its opponents' fitted strength). |
| `epa_all_def_shrunk` | Float64 | Display value: the adjusted EPA per play the defense allowed on all downs, shrunk toward the league mean by n / (n + 244.2). |
| `epa_all_def_n` | Int64 | Plays behind the defense values: its plays on all downs outside garbage time, pass and rush only (the n in the shrinkage). |
| `epa_all_def_rank` | Float64 | Rank of epa_all_def_shrunk (lowest = 1, ties averaged). |
| `success_all_off_raw` | Float64 | Raw success rate (share of plays with positive EPA) the offense gained on all downs (pass and rush plays outside garbage time). |
| `success_all_off_adj` | Float64 | Opponent-adjusted success rate (share of plays with positive EPA) the offense gained on all downs (league mean plus the team's mean per-game residual against its opponents' fitted strength). |
| `success_all_off_shrunk` | Float64 | Display value: the adjusted success rate (share of plays with positive EPA) the offense gained on all downs, shrunk toward the league mean by n / (n + 199.4). |
| `success_all_off_n` | Int64 | Plays behind the offense values: its plays on all downs outside garbage time, pass and rush only (the n in the shrinkage). |
| `success_all_off_rank` | Float64 | Rank of success_all_off_shrunk (highest = 1, ties averaged). |
| `success_all_def_raw` | Float64 | Raw success rate (share of plays with positive EPA) the defense allowed on all downs (pass and rush plays outside garbage time). |
| `success_all_def_adj` | Float64 | Opponent-adjusted success rate (share of plays with positive EPA) the defense allowed on all downs (league mean plus the team's mean per-game residual against its opponents' fitted strength). |
| `success_all_def_shrunk` | Float64 | Display value: the adjusted success rate (share of plays with positive EPA) the defense allowed on all downs, shrunk toward the league mean by n / (n + 229.4). |
| `success_all_def_n` | Int64 | Plays behind the defense values: its plays on all downs outside garbage time, pass and rush only (the n in the shrinkage). |
| `success_all_def_rank` | Float64 | Rank of success_all_def_shrunk (lowest = 1, ties averaged). |
| `epa_early_down_off_raw` | Float64 | Raw EPA per play the offense gained on first and second downs (pass and rush plays outside garbage time). |
| `epa_early_down_off_adj` | Float64 | Opponent-adjusted EPA per play the offense gained on first and second downs (league mean plus the team's mean per-game residual against its opponents' fitted strength). |
| `epa_early_down_off_shrunk` | Float64 | Display value: the adjusted EPA per play the offense gained on first and second downs, shrunk toward the league mean by n / (n + 205.8). |
| `epa_early_down_off_n` | Int64 | Plays behind the offense values: its plays on first and second downs outside garbage time, pass and rush only (the n in the shrinkage). |
| `epa_early_down_off_rank` | Float64 | Rank of epa_early_down_off_shrunk (highest = 1, ties averaged). |
| `epa_early_down_def_raw` | Float64 | Raw EPA per play the defense allowed on first and second downs (pass and rush plays outside garbage time). |
| `epa_early_down_def_adj` | Float64 | Opponent-adjusted EPA per play the defense allowed on first and second downs (league mean plus the team's mean per-game residual against its opponents' fitted strength). |
| `epa_early_down_def_shrunk` | Float64 | Display value: the adjusted EPA per play the defense allowed on first and second downs, shrunk toward the league mean by n / (n + 233.3). |
| `epa_early_down_def_n` | Int64 | Plays behind the defense values: its plays on first and second downs outside garbage time, pass and rush only (the n in the shrinkage). |
| `epa_early_down_def_rank` | Float64 | Rank of epa_early_down_def_shrunk (lowest = 1, ties averaged). |
| `epa_standard_down_off_raw` | Float64 | Raw EPA per play the offense gained on standard downs (pass and rush plays outside garbage time). |
| `epa_standard_down_off_adj` | Float64 | Opponent-adjusted EPA per play the offense gained on standard downs (league mean plus the team's mean per-game residual against its opponents' fitted strength). |
| `epa_standard_down_off_shrunk` | Float64 | Display value: the adjusted EPA per play the offense gained on standard downs, shrunk toward the league mean by n / (n + 252.8). |
| `epa_standard_down_off_n` | Int64 | Plays behind the offense values: its plays on standard downs outside garbage time, pass and rush only (the n in the shrinkage). |
| `epa_standard_down_off_rank` | Float64 | Rank of epa_standard_down_off_shrunk (highest = 1, ties averaged). |
| `epa_standard_down_def_raw` | Float64 | Raw EPA per play the defense allowed on standard downs (pass and rush plays outside garbage time). |
| `epa_standard_down_def_adj` | Float64 | Opponent-adjusted EPA per play the defense allowed on standard downs (league mean plus the team's mean per-game residual against its opponents' fitted strength). |
| `epa_standard_down_def_shrunk` | Float64 | Display value: the adjusted EPA per play the defense allowed on standard downs, shrunk toward the league mean by n / (n + 233.7). |
| `epa_standard_down_def_n` | Int64 | Plays behind the defense values: its plays on standard downs outside garbage time, pass and rush only (the n in the shrinkage). |
| `epa_standard_down_def_rank` | Float64 | Rank of epa_standard_down_def_shrunk (lowest = 1, ties averaged). |
| `epa_first_half_off_raw` | Float64 | Raw EPA per play the offense gained on first-half plays (pass and rush plays outside garbage time). |
| `epa_first_half_off_adj` | Float64 | Always null: EPA per play on first-half plays for the offense is published raw: the opponent adjustment did not pass the pre-registered reliability rule. |
| `epa_first_half_off_shrunk` | Float64 | Display value: the raw EPA per play the offense gained on first-half plays, shrunk toward the league mean by n / (n + 216.5). |
| `epa_first_half_off_n` | Int64 | Plays behind the offense values: its first-half plays outside garbage time, pass and rush only (the n in the shrinkage). |
| `epa_first_half_off_rank` | Float64 | Rank of epa_first_half_off_shrunk (highest = 1, ties averaged). |
| `epa_first_half_def_raw` | Float64 | Raw EPA per play the defense allowed on first-half plays (pass and rush plays outside garbage time). |
| `epa_first_half_def_adj` | Float64 | Always null: EPA per play on first-half plays for the defense is published raw: the opponent adjustment did not pass the pre-registered reliability rule. |
| `epa_first_half_def_shrunk` | Float64 | Display value: the raw EPA per play the defense allowed on first-half plays, shrunk toward the league mean by n / (n + 280.3). |
| `epa_first_half_def_n` | Int64 | Plays behind the defense values: its first-half plays outside garbage time, pass and rush only (the n in the shrinkage). |
| `epa_first_half_def_rank` | Float64 | Rank of epa_first_half_def_shrunk (lowest = 1, ties averaged). |
| `epa_pass_off_raw` | Float64 | Raw EPA per play the offense gained on pass plays outside garbage time. |
| `epa_pass_off_adj` | Float64 | Always null: EPA per play on pass plays for the offense is published raw: the opponent adjustment did not pass the pre-registered reliability rule. |
| `epa_pass_off_shrunk` | Float64 | Display value: the raw EPA per play the offense gained on pass plays, shrunk toward the league mean by n / (n + 183.4). |
| `epa_pass_off_n` | Int64 | Plays behind the offense values: its pass plays outside garbage time (the n in the shrinkage). |
| `epa_pass_off_rank` | Float64 | Rank of epa_pass_off_shrunk (highest = 1, ties averaged). |
| `success_pass_off_raw` | Float64 | Raw success rate (share of plays with positive EPA) the offense gained on pass plays outside garbage time. |
| `success_pass_off_adj` | Float64 | Opponent-adjusted success rate (share of plays with positive EPA) the offense gained on pass plays (league mean plus the team's mean per-game residual against its opponents' fitted strength). |
| `success_pass_off_shrunk` | Float64 | Display value: the adjusted success rate (share of plays with positive EPA) the offense gained on pass plays, shrunk toward the league mean by n / (n + 133). |
| `success_pass_off_n` | Int64 | Plays behind the offense values: its pass plays outside garbage time (the n in the shrinkage). |
| `success_pass_off_rank` | Float64 | Rank of success_pass_off_shrunk (highest = 1, ties averaged). |
| `epa_rush_off_raw` | Float64 | Raw EPA per play the offense gained on rush plays outside garbage time. |
| `epa_rush_off_adj` | Float64 | Always null: EPA per play on rush plays for the offense is published raw: the opponent adjustment did not pass the pre-registered reliability rule. |
| `epa_rush_off_shrunk` | Float64 | Display value: the raw EPA per play the offense gained on rush plays, shrunk toward the league mean by n / (n + 293.1). |
| `epa_rush_off_n` | Int64 | Plays behind the offense values: its rush plays outside garbage time (the n in the shrinkage). |
| `epa_rush_off_rank` | Float64 | Rank of epa_rush_off_shrunk (highest = 1, ties averaged). |
| `epa_rush_def_raw` | Float64 | Raw EPA per play the defense allowed on rush plays outside garbage time. |
| `epa_rush_def_adj` | Float64 | Always null: EPA per play on rush plays for the defense is published raw: the opponent adjustment did not pass the pre-registered reliability rule. |
| `epa_rush_def_shrunk` | Float64 | Display value: the raw EPA per play the defense allowed on rush plays, shrunk toward the league mean by n / (n + 275.9). |
| `epa_rush_def_n` | Int64 | Plays behind the defense values: its rush plays outside garbage time (the n in the shrinkage). |
| `epa_rush_def_rank` | Float64 | Rank of epa_rush_def_shrunk (lowest = 1, ties averaged). |
| `success_rush_off_raw` | Float64 | Raw success rate (share of plays with positive EPA) the offense gained on rush plays outside garbage time. |
| `success_rush_off_adj` | Float64 | Opponent-adjusted success rate (share of plays with positive EPA) the offense gained on rush plays (league mean plus the team's mean per-game residual against its opponents' fitted strength). |
| `success_rush_off_shrunk` | Float64 | Display value: the adjusted success rate (share of plays with positive EPA) the offense gained on rush plays, shrunk toward the league mean by n / (n + 210). |
| `success_rush_off_n` | Int64 | Plays behind the offense values: its rush plays outside garbage time (the n in the shrinkage). |
| `success_rush_off_rank` | Float64 | Rank of success_rush_off_shrunk (highest = 1, ties averaged). |
| `success_rush_def_raw` | Float64 | Raw success rate (share of plays with positive EPA) the defense allowed on rush plays outside garbage time. |
| `success_rush_def_adj` | Float64 | Opponent-adjusted success rate (share of plays with positive EPA) the defense allowed on rush plays (league mean plus the team's mean per-game residual against its opponents' fitted strength). |
| `success_rush_def_shrunk` | Float64 | Display value: the adjusted success rate (share of plays with positive EPA) the defense allowed on rush plays, shrunk toward the league mean by n / (n + 169). |
| `success_rush_def_n` | Int64 | Plays behind the defense values: its rush plays outside garbage time (the n in the shrinkage). |
| `success_rush_def_rank` | Float64 | Rank of success_rush_def_shrunk (lowest = 1, ties averaged). |
| `epa_pass_def_raw` | Float64 | Raw EPA per play the defense allowed on pass plays outside garbage time. |
| `epa_pass_def_adj` | Float64 | Always null: EPA per play on pass plays for the defense is published raw: the opponent adjustment did not pass the pre-registered reliability rule. |
| `epa_pass_def_shrunk` | Float64 | Display value: the raw EPA per play the defense allowed on pass plays, shrunk toward the league mean by n / (n + 387.6). |
| `epa_pass_def_n` | Int64 | Plays behind the defense values: its pass plays outside garbage time (the n in the shrinkage). |
| `epa_pass_def_rank` | Float64 | Rank of epa_pass_def_shrunk (lowest = 1, ties averaged). |
| `success_pass_def_raw` | Float64 | Raw success rate (share of plays with positive EPA) the defense allowed on pass plays outside garbage time. |
| `success_pass_def_adj` | Float64 | Always null: success rate (share of plays with positive EPA) on pass plays for the defense is published raw: the opponent adjustment did not pass the pre-registered reliability rule. |
| `success_pass_def_shrunk` | Float64 | Display value: the raw success rate (share of plays with positive EPA) the defense allowed on pass plays, shrunk toward the league mean by n / (n + 335.2). |
| `success_pass_def_n` | Int64 | Plays behind the defense values: its pass plays outside garbage time (the n in the shrinkage). |
| `success_pass_def_rank` | Float64 | Rank of success_pass_def_shrunk (lowest = 1, ties averaged). |
| `epa_passing_down_off_raw` | Float64 | Raw EPA per play the offense gained on passing downs (pass and rush plays outside garbage time). |
| `epa_passing_down_off_adj` | Float64 | Always null: EPA per play on passing downs for the offense is published raw: the opponent adjustment did not pass the pre-registered reliability rule. |
| `epa_passing_down_off_shrunk` | Float64 | Display value: the raw EPA per play the offense gained on passing downs, shrunk toward the league mean by n / (n + 245.3). |
| `epa_passing_down_off_n` | Int64 | Plays behind the offense values: its plays on passing downs outside garbage time, pass and rush only (the n in the shrinkage). |
| `epa_passing_down_off_rank` | Float64 | Rank of epa_passing_down_off_shrunk (highest = 1, ties averaged). |
| `epa_q1_off_raw` | Float64 | Raw EPA per play the offense gained on first-quarter plays (pass and rush plays outside garbage time). |
| `epa_q1_off_adj` | Float64 | Always null: EPA per play on first-quarter plays for the offense is published raw: the opponent adjustment did not pass the pre-registered reliability rule. |
| `epa_q1_off_shrunk` | Float64 | Display value: the raw EPA per play the offense gained on first-quarter plays, shrunk toward the league mean by n / (n + 165.2). |
| `epa_q1_off_n` | Int64 | Plays behind the offense values: its first-quarter plays outside garbage time, pass and rush only (the n in the shrinkage). |
| `epa_q1_off_rank` | Float64 | Rank of epa_q1_off_shrunk (highest = 1, ties averaged). |
| `explosive_rate_off_raw` | Float64 | Raw explosive-play rate (pass EPA at least 2.4, rush EPA at least 1.8) the offense gained on all downs (pass and rush plays outside garbage time). |
| `explosive_rate_off_adj` | Float64 | Always null: explosive-play rate (pass EPA at least 2.4, rush EPA at least 1.8) on all downs for the offense is published raw: the opponent adjustment did not pass the pre-registered reliability rule. |
| `explosive_rate_off_shrunk` | Float64 | Display value: the raw explosive-play rate (pass EPA at least 2.4, rush EPA at least 1.8) the offense gained on all downs, shrunk toward the league mean by n / (n + 799.6). |
| `explosive_rate_off_n` | Int64 | Plays behind the offense values: its plays on all downs outside garbage time, pass and rush only (the n in the shrinkage). |
| `explosive_rate_off_rank` | Float64 | Rank of explosive_rate_off_shrunk (highest = 1, ties averaged). |
| `success_late_down_off_raw` | Float64 | Raw success rate (share of plays with positive EPA) the offense gained on third and fourth downs (pass and rush plays outside garbage time). |
| `success_late_down_off_adj` | Float64 | Always null: success rate (share of plays with positive EPA) on third and fourth downs is never opponent-adjusted; it is published raw with shrinkage. |
| `success_late_down_off_shrunk` | Float64 | Display value: the raw success rate (share of plays with positive EPA) the offense gained on third and fourth downs, shrunk toward the league mean by n / (n + 164.1). |
| `success_late_down_off_n` | Int64 | Plays behind the offense values: its plays on third and fourth downs outside garbage time, pass and rush only (the n in the shrinkage). |
| `success_late_down_off_rank` | Float64 | Rank of success_late_down_off_shrunk (highest = 1, ties averaged). |
| `success_late_down_def_raw` | Float64 | Raw success rate (share of plays with positive EPA) the defense allowed on third and fourth downs (pass and rush plays outside garbage time). |
| `success_late_down_def_adj` | Float64 | Always null: success rate (share of plays with positive EPA) on third and fourth downs is never opponent-adjusted; it is published raw with shrinkage. |
| `success_late_down_def_shrunk` | Float64 | Display value: the raw success rate (share of plays with positive EPA) the defense allowed on third and fourth downs, shrunk toward the league mean by n / (n + 184.9). |
| `success_late_down_def_n` | Int64 | Plays behind the defense values: its plays on third and fourth downs outside garbage time, pass and rush only (the n in the shrinkage). |
| `success_late_down_def_rank` | Float64 | Rank of success_late_down_def_shrunk (lowest = 1, ties averaged). |
| `epa_q2_off_raw` | Float64 | Raw EPA per play the offense gained on second-quarter plays (pass and rush plays outside garbage time). |
| `epa_q2_off_adj` | Float64 | Always null: EPA per play on second-quarter plays is never opponent-adjusted; it is published raw with shrinkage. |
| `epa_q2_off_shrunk` | Float64 | Display value: the raw EPA per play the offense gained on second-quarter plays, shrunk toward the league mean by n / (n + 237.9). |
| `epa_q2_off_n` | Int64 | Plays behind the offense values: its second-quarter plays outside garbage time, pass and rush only (the n in the shrinkage). |
| `epa_q2_off_rank` | Float64 | Rank of epa_q2_off_shrunk (highest = 1, ties averaged). |
| `epa_q2_def_raw` | Float64 | Raw EPA per play the defense allowed on second-quarter plays (pass and rush plays outside garbage time). |
| `epa_q2_def_adj` | Float64 | Always null: EPA per play on second-quarter plays is never opponent-adjusted; it is published raw with shrinkage. |
| `epa_q2_def_shrunk` | Float64 | Display value: the raw EPA per play the defense allowed on second-quarter plays, shrunk toward the league mean by n / (n + 287.8). |
| `epa_q2_def_n` | Int64 | Plays behind the defense values: its second-quarter plays outside garbage time, pass and rush only (the n in the shrinkage). |
| `epa_q2_def_rank` | Float64 | Rank of epa_q2_def_shrunk (lowest = 1, ties averaged). |
| `iso_ppp_def_raw` | Float64 | Raw EPA per play (isoPPP) the defense allowed on successful plays outside garbage time. |
| `iso_ppp_def_adj` | Float64 | Always null: EPA per play (isoPPP) on successful plays is never opponent-adjusted; it is published raw with shrinkage. |
| `iso_ppp_def_shrunk` | Float64 | Display value: the raw EPA per play (isoPPP) the defense allowed on successful plays, shrunk toward the league mean by n / (n + 299.2). |
| `iso_ppp_def_n` | Int64 | Plays behind the defense values: its successful plays outside garbage time (the n in the shrinkage). |
| `iso_ppp_def_rank` | Float64 | Rank of iso_ppp_def_shrunk (lowest = 1, ties averaged). |

**Raises**

| Exception | Description |
|---|---|
| `KeyError` | If `plays` is missing a required column. |
| `ValueError` | If a name in `metrics` is not a published breakdown. |

**Example**

```python
import polars as pl
import sportsdataverse.cfb as cfb
bd = cfb.cfb_adjusted_breakdowns(plays)
bd.sort("success_all_off_rank").select("pos_team", "success_all_off_shrunk", "success_all_off_n").head()

# Only some breakdowns

cfb.cfb_adjusted_breakdowns(plays, metrics=["success_all", "epa_early_down"])
```

### cfb_adjusted_epa {#cfb_adjusted_epa}

`cfb_adjusted_epa(plays: 'pl.DataFrame | pd.DataFrame', *, ridge_lambda: 'float | None' = None, method: "Literal['current', 'pre598']" = 'current', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Season opponent-adjusted per-team EPA from a season's play-by-play.

Fits one ridge of per-play `EPA` on offense-team, defense-team, and
home-field indicators (every team shrunk toward the league average by its own
play count) over the pass and rush plays that are not garbage time (the
score-margin rule, see the module docstring), nets each team's per-game raw
EPA on those same plays against the opponent's fitted strength, and averages
to a season figure. In-sample/descriptive (the
fit uses the whole season); for leak-free per-game values use
`cfb_adjusted_epa_by_game`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame \| DataFrame` |  | A cfbfastR-schema play-by-play frame (polars or pandas) with the columns listed in the module docstring. One season at a time. |
| `ridge_lambda` | `float \| None` | `None` | Ridge penalty. Under `method="current"` it is per play of a full team season: each team keeps `n / (n + ridge_lambda * 577)` of its own signal for its `n` fit plays and is shrunk toward the league average by the rest (a fixed ~43 plays at the default, ~6% of a full season; most of it on a handful of plays); must be > 0. Under `method="pre598"` it is passed unscaled to the old standardized ridge (the per-observation penalty; no 577 scaling, no positivity check). `None` (default) means 0.075 for `"current"` (the owner's choice, `_ADJ_EPA_LAMBDA`) and 0.035 for `"pre598"`. |
| `method` | `Literal['current', 'pre598']` | `'current'` | `"current"` (default) or `"pre598"`, the fit this function used before #598 (`0.1 <= wp_before <= 0.9` band, standardized ridge with the first team id as the reference level, lambda 0.035, own EPA on every play). pre598 reads `wp_before` instead of `period` / `pos_score_diff_start`. It exists for nfl-data's NFL team summaries, is not validated for NFL either, and is kept only for continuity until NFL is validated. |
| `return_as_pandas` | `bool` | `False` | Return a pandas `DataFrame` instead of polars. |

**Returns**

One row per team (>= 2 valid games): `team_id`, `pos_team`, `valid_games`, `adj_off_epa`, `adj_def_epa`, `off_strength_faced`, `def_strength_faced`, `net_adj_epa` and their `*_rank` columns.

No returns table is published for this function: no capture: it needs play-by-play joined with schedule fields (home, neutral_site, pos_team_id) that neither load_cfb_pbp nor load_cfb_pbp_r carries; only cfb_ratings builds that join, internally.

**Raises**

| Exception | Description |
|---|---|
| `KeyError` | If `plays` is missing a required column. |
| `ValueError` | If `method` is unknown, or (`"current"`) if `ridge_lambda` is not positive or every play is garbage time. |

**Example**

```python
import sportsdataverse.cfb as cfb
pbp = cfb.load_cfb_pbp(seasons=[2023])
cfb.cfb_adjusted_epa(pbp).sort("net_adj_epa_rank").head()

# NFL team summaries (the pre-#598 method; reads wp_before)

cfb.cfb_adjusted_epa(nfl_plays, method="pre598")
```

### cfb_adjusted_epa_by_game {#cfb_adjusted_epa_by_game}

`cfb_adjusted_epa_by_game(plays: 'pl.DataFrame | pd.DataFrame', *, ridge_lambda: 'float | None' = None, method: "Literal['current', 'pre598']" = 'current', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Walk-forward (point-in-time) opponent-adjusted EPA, one row per team-game.

For each week `w` the opponent-strength ridge is fit on non-garbage-time
plays from **weeks before `w` only**, then that week's games are adjusted with
those as-of strengths -- so the value uses no future information and is valid
as an in-season power-rating / model feature. Week 1 (no prior) yields null
adjustments; not-yet-seen opponents fall back to the league baseline (an
average team), and teams seen on few plays are shrunk most of the way there.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame \| DataFrame` |  | A cfbfastR-schema play-by-play frame (polars or pandas) with the module-docstring columns **plus** `week`. One season at a time. |
| `ridge_lambda` | `float \| None` | `None` | Ridge penalty, as in `cfb_adjusted_epa` (`None` means 0.075 for `"current"`, 0.035 for `"pre598"`). |
| `method` | `Literal['current', 'pre598']` | `'current'` | `"current"` (default) or `"pre598"`, the fit this function used before #598 (see `cfb_adjusted_epa`). pre598 also keeps the old week order: it sorts by `week` alone and does not read `seasonType`, so postseason games that restart at week 1 are fit with (and leak into) the regular season, exactly as before. It exists for nfl-data's NFL team summaries, is not validated for NFL either, and is kept only for continuity until NFL is validated. |
| `return_as_pandas` | `bool` | `False` | Return a pandas `DataFrame` instead of polars. |

**Returns**

One row per (game, team), sorted by `week` then `team_id`: `game_id`, `week`, `team_id`, `opponent_id`, `pos_team`, `raw_off_epa`, `adj_off_epa`, `raw_def_epa`, `adj_def_epa`, `off_strength_faced` (opponent offense), `def_strength_faced` (opponent defense), `net_adj_epa`. The `adj_*` / `net` columns are null for week 1 (and any week with no prior fit).

No returns table is published for this function: no capture: it needs play-by-play joined with schedule fields (home, neutral_site, pos_team_id) that neither load_cfb_pbp nor load_cfb_pbp_r carries; only cfb_ratings builds that join, internally.

**Raises**

| Exception | Description |
|---|---|
| `KeyError` | If `plays` is missing a required column (incl. `week`, and `seasonType` unless `method="pre598"`). |
| `ValueError` | If `method` is unknown, or (`"current"`) if `ridge_lambda` is not positive. (Every play garbage time gives an empty frame; a team-game with no non-garbage play has no row -- none of 38,620 in 2014-2026.) |

**Example**

```python
import sportsdataverse.cfb as cfb
pbp = cfb.load_cfb_pbp(seasons=[2023])
tg = cfb.cfb_adjusted_epa_by_game(pbp)
tg.filter(pl.col("week") >= 5).sort("net_adj_epa", descending=True).head()
```

### cfb_adjusted_epa_ledger {#cfb_adjusted_epa_ledger}

`cfb_adjusted_epa_ledger(plays: 'pl.DataFrame | pd.DataFrame', *, ridge_lambda: 'float | None' = None, influence: 'bool' = True, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Per-game decomposition of `cfb_adjusted_epa`'s season figure, one row per team-game.

The season `net_adj_epa` is a mean over games of `adj_off_epa` minus a mean over
games of `adj_def_epa`, and each game's adjusted value is its raw EPA plus a schedule
term (the opponent's fitted strengths). This frame lays that sum out: per team-game the
raw net EPA, the schedule term, the game's weight in each mean and its contribution. It
uses the same plays, fit and penalty as `cfb_adjusted_epa` (`method="current"`),
so for every team `contribution` sums to its season `net_adj_epa`, and
`raw_contribution + schedule_contribution == contribution` row by row. The fitted
home-field term is not netted out of a team's own EPA (nor is it in the season figure),
so the ledger records each game's `site` and has no home term.

With `influence=True` it also refits the season once per game without that game
(leave one game out) and reports how far the game moved each of its two teams' season
values (the refit moves every team; the ledger reports the two that played).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame \| DataFrame` |  | A cfbfastR-schema play-by-play frame (polars or pandas) with the columns `cfb_adjusted_epa` reads. One season at a time. `season`, `week` and `seasonType` are carried through when present. |
| `ridge_lambda` | `float \| None` | `None` | Ridge penalty, as in `cfb_adjusted_epa` (`None` means 0.075). |
| `influence` | `bool` | `True` | Add `loo_net_adj_epa` and `game_influence` (one ridge refit per game; seconds for a full FBS season). |
| `return_as_pandas` | `bool` | `False` | Return a pandas `DataFrame` instead of polars. |

**Returns**

One row per (game, team) for the teams `cfb_adjusted_epa` rates (two or more valid games), sorted by `team_id`, then `week` (when present), then `game_id`: `game_id`, the carried `season` / `week` / `season_type`, `team_id`, `pos_team`, `opponent_id`, `site` (`home` / `away` / `neutral`), `off_plays` / `def_plays` (the team's offensive / defensive plays in the fit), `raw_off_epa`, `raw_def_epa`, `raw_net_epa`, `off_strength_faced` / `def_strength_faced` (the opponent's fitted offense / defense, EPA/play), `schedule_adj` (`off_strength_faced - def_strength_faced`: what the schedule term adds to this game's raw net), `adj_off_epa`, `adj_def_epa`, `adj_net_epa`, `off_weight` / `def_weight` (the game's weight in each season mean: 1 / the games where that side has fit plays, so the two differ when a game has plays on one side only), `raw_contribution`, `schedule_contribution`, `contribution` (to the season value), `net_adj_epa` (the season value, repeated) and, with `influence`, `loo_net_adj_epa` (the season value without this game) and `game_influence` (`net_adj_epa - loo_net_adj_epa`; both null when the team would keep fewer than two games).

| col_name | type | description |
|---|---|---|
| `game_id` | String | Game id of the team-game (the play-by-play game_id). |
| `season` | Int64 | Season carried through from the plays when present. |
| `week` | Int64 | Week carried through from the plays when present. |
| `season_type` | Int64 | ESPN season type carried from the plays' seasonType (2 = regular season, 3 = postseason) when present. |
| `team_id` | String | Id of the rated team (the pos_team_id the fit uses). |
| `pos_team` | String | Name of the rated team. |
| `opponent_id` | String | Id of the opponent in this game. |
| `site` | String | Where the rated team played: home, away or neutral. |
| `off_plays` | Int64 | The team's offensive plays in this game that entered the fit. |
| `def_plays` | Int64 | The team's defensive plays in this game that entered the fit. |
| `raw_off_epa` | Float64 | Raw offensive EPA per play in this game. |
| `raw_def_epa` | Float64 | Raw EPA per play allowed in this game. |
| `raw_net_epa` | Float64 | raw_off_epa minus raw_def_epa for this game. |
| `off_strength_faced` | Float64 | The opponent's fitted offensive strength, EPA per play (what the defense faced). |
| `def_strength_faced` | Float64 | The opponent's fitted defensive strength, EPA per play (what the offense faced). |
| `schedule_adj` | Float64 | off_strength_faced minus def_strength_faced: what the schedule term adds to this game's raw net. |
| `adj_off_epa` | Float64 | Opponent-adjusted offensive EPA per play for this game. |
| `adj_def_epa` | Float64 | Opponent-adjusted EPA per play allowed for this game. |
| `adj_net_epa` | Float64 | adj_off_epa minus adj_def_epa for this game. |
| `off_weight` | Float64 | This game's weight in the season offensive mean: 1 / the games with offensive fit plays. |
| `def_weight` | Float64 | This game's weight in the season defensive mean: 1 / the games with defensive fit plays. |
| `raw_contribution` | Float64 | The raw part of this game's contribution to the season net_adj_epa. |
| `schedule_contribution` | Float64 | The schedule part of this game's contribution to the season net_adj_epa. |
| `contribution` | Float64 | This game's contribution to the season net_adj_epa (raw plus schedule); sums to it per team. |
| `net_adj_epa` | Float64 | The team's season net adjusted EPA per play, repeated on each of its games. |
| `loo_net_adj_epa` | Float64 | The team's season net_adj_epa refit without this game (with influence); null under two games or when a play's opponent id is null. |
| `game_influence` | Float64 | net_adj_epa minus loo_net_adj_epa: how far this game moved the team's season value. |

**Raises**

| Exception | Description |
|---|---|
| `KeyError` | If `plays` is missing a required column. |
| `ValueError` | If `ridge_lambda` is not positive or every play is garbage time. |

**Example**

```python
import polars as pl
import sportsdataverse.cfb as cfb
ledger = cfb.cfb_adjusted_epa_ledger(plays)
ledger.filter(pl.col("pos_team") == "LSU").select("opponent_id", "raw_net_epa", "schedule_adj")

# The games that moved a team the most

ledger.sort(pl.col("game_influence").abs(), descending=True).head()
```

### cfb_compute_results {#cfb_compute_results}

`cfb_compute_results(teams: 'pl.DataFrame', games: 'pl.DataFrame', week_num: 'int', *, rng: 'np.random.Generator | None' = None, elo: 'dict[str, float] | None' = None, home_field: 'float' = 59.4, margin_sd: 'float' = 16.7, **kwargs: 'Any') -> 'dict[str, pl.DataFrame]'`

Default results generator — nflseedR's dynamic ELO model for CFB.

Fills `result` for week `week_num` games that are still unplayed and
updates each team's ELO rating from that week's results (real results
included). The ELO machinery is nflseedR's `nflseedR_compute_results`
minus the NFL rest-day adjustment (CFB plays weekly — documented
simplification); the home-field bonus and the margin noise are fitted to
FBS results (`home_field`, `margin_sd`).

The defaults come from cfbseedR's `data-raw/cfb_elo_constants.R`, fitted
on 8,551 completed FBS-vs-FBS games from 2014-2025 (2020 left out):
`home_field = 25 * 2.376` points of within-season home edge (SE 0.168),
and `margin_sd` = 16.734 points, the spread of real margins around the
pregame-ELO estimate this function computes (SE 0.128). They replace
nflseedR's NFL values (20 ELO = 0.8 points, sd 13). cfbseedR uses the
same numbers; refit both together.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `teams` | `DataFrame` |  | Per-sim team table (`sim`, `team`, `conference`, optionally `elo` carried over from the previous week). |
| `games` | `DataFrame` |  | Per-sim games table (engine schema; see `sportsdataverse.cfb.cfb_standings`). |
| `week_num` | `int` |  | The week to fill. |
| `rng` | `Generator \| None` | `None` | numpy Generator (seeded by `cfb_simulations`). A fresh default generator is created when omitted. |
| `elo` | `dict[str, float] \| None` | `None` | Optional initial ratings `{team: elo}` applied to every sim. Teams missing from the dict start at 1500. When neither `elo` nor a `teams.elo` column exists, ratings initialize randomly at `N(1500, 150)` per (sim, team) — nflseedR behavior. |
| `home_field` | `float` | `59.4` | Home-field advantage in ELO points, added for games that are not neutral (default 59.4, i.e. 2.38 points of margin at 25 ELO per point). `20.0` restores the pre-fit NFL value. |
| `margin_sd` | `float` | `16.7` | Standard deviation, in points, of a simulated margin around its ELO estimate (default 16.7). `13.0` restores the NFL value. |

**Returns**

`{"teams": ..., "games": ...}` — updated frames, mirroring nflseedR's returned list.

| col_name | type | description |
|---|---|---|
| `sim` | Int64 | Simulation identifier the game row belongs to (1..n simulated seasons; ELO ratings never mix across simulations). |
| `week` | Int64 | Week of the season the game is played in; only games matching the requested week_num are filled. |
| `game_type` | String | Game classification in the seedr engine schema - REG (regular season), CONF_CHAMP (conference championship) or POST (postseason/CFP). |
| `home_team` | String | Team name of the home team in the simulated game (returned games frame). |
| `away_team` | String | Team name of the away team in the simulated game (returned games frame). |
| `result` | Float64 | Home-team margin of victory (home score minus away score) - real results are preserved and the target week's unplayed games are filled from the ELO model. |
| `neutral` | Int64 | Neutral-site flag (1 = neutral site, 0 = true home game; only non-neutral games receive the ELO home bump). |

**Example**

```python
from sportsdataverse.cfb.cfb_simulations import cfb_compute_results
out = cfb_compute_results(teams, games, 5, rng=rng)
teams, games = out["teams"], out["games"]

# Simulate with the NFL constants instead (``cfb_simulations`` passes only ``rng``, so bind overrides with ``functools.partial``)

import functools
from sportsdataverse.cfb.cfb_simulations import cfb_compute_results, cfb_simulations
nfl_elo = functools.partial(cfb_compute_results, home_field=20.0, margin_sd=13.0)
sims = cfb_simulations(games, teams, compute_results=nfl_elo, seed=1)
```

### cfb_draft_projection {#cfb_draft_projection}

`cfb_draft_projection(target_draft_year: 'int', *, division: 'str' = 'fbs', history_years: 'list[int] | None' = None, l2: 'float' = 1.0, return_as_pandas: 'bool' = False) -> 'dict[str, pl.DataFrame] | dict[str, pd.DataFrame]'`

Project NFL-draft probability per player + expected picks per team.

Fits an L2 logistic of `drafted` on `[recruit_stars, talent_points,
career_production_z, class_year]` over draft years strictly before the
target (the as-of boundary, enforced internally), then scores the target
year's eligible players.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `target_draft_year` | `int` |  | Draft year to project. |
| `division` | `str` | `'fbs'` | Division slug for constants lookups. |
| `history_years` | `list[int] \| None` | `None` | Training draft years (default: the five before target). |
| `l2` | `float` | `1.0` | Logistic L2 penalty. |
| `return_as_pandas` | `bool` | `False` | If True, both frames return as pandas. |

**Returns**

`{"players": ..., "teams": ...}` — players: `draft_year` (Int64), `team_id` / `player_id` / `player_name` (Utf8), `draft_prob` (Float64); teams: `draft_year`, `team_id`, `proj_draft_picks` (Float64, the sum of member draft probabilities). Zero-row (typed) frames when no data is available.

**Example**

```python
from sportsdataverse.cfb import cfb_draft_projection
out = cfb_draft_projection(2024)
out["teams"].sort("proj_draft_picks", descending=True).head(10)
```

### cfb_field_position {#cfb_field_position}

`cfb_field_position(seasons: 'Union[int, list[int]]', *, exclude_garbage: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Team-season field-position value: avg start, drive EP, margin, pts/drive.

Derives one row per drive from `load_cfb_pbp`, values each starting
yard line with the bundled EP curve, and aggregates per (season, team):
`avg_start_yardline` (yards from own goal, higher = better),
`fp_ep` (mean drive-start EP), `fp_margin` (own `fp_ep` minus the
mean drive-start EP of opponents' drives faced), and
`points_per_drive` (mean realized offensive points: TD=7, FG=3;
non-offensive negative results such as safeties and defensive return
TDs are floored to 0 before averaging).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | season or list of seasons (hosted pbp covers 2002-2021). |
| `exclude_garbage` | `bool` | `True` | drop drives that start in Connelly garbage time. |
| `return_as_pandas` | `bool` | `False` | return a pandas `DataFrame` instead of polars. |

**Returns**

One row per (season, team_id); zero-row frame with the documented schema on empty input.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season the field-position stats cover. |
| `team_id` | String | Team ESPN id (character join key). |
| `drives` | Int64 | Offensive drives counted (garbage-time drives excluded by default). |
| `avg_start_yardline` | Float64 | Mean drive-start yard line from the team's own goal (higher = better field position). |
| `fp_ep` | Float64 | Mean bundled expected points of the team's drive starts. |
| `fp_margin` | Float64 | Own fp_ep minus the mean drive-start EP of opponents' drives faced. |
| `points_per_drive` | Float64 | Mean realized offensive points per drive (TD=7, FG=3). |

**Example**

```python
from sportsdataverse.cfb import cfb_field_position
df = cfb_field_position([2021])
print(df.shape)

# Pipeline next step (one line)

df.sort("fp_margin", descending=True).head()
```

### cfb_hierarchical_ratings {#cfb_hierarchical_ratings}

`cfb_hierarchical_ratings(schedule: 'pl.DataFrame', season: 'int', t: 'int', *, first_season: 'int' = 2014, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

As-of hierarchical team strengths for slot `t` of `season`, in points.

Feed the result to `~sportsdataverse.cfb.cfb_game_predict.cfb_predict_games` with
`model="hierarchical"`. Expected home margin is `theta_home - theta_away + hfa` off
neutral sites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `schedule` | `DataFrame` |  | A `~sportsdataverse.cfb.load_cfb_schedule` frame covering `first_season` through `season`. It needs `game_id`, `season`, `week`, `season_type`, `fbs_participant`, `home_id`, `away_id`, `neutral_site`, `home_conference`, `away_conference`, `home_points` and `away_points`. Only completed games before the slot are read, so a whole season's schedule can be passed. |
| `season` | `int` |  | Season of the slot. |
| `t` | `int` |  | Slot to rate: the regular-season week, or `100 + week` for the postseason (bowls are postseason week 1, so `t=101`). |
| `first_season` | `int` | `2014` | Start of the carryover chain, seeded with zero strengths. The gated model starts at 2014; another start gives a different (ungated) model. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

One row per team in the corpus, with columns: | col_name | type | description | |---|---|---| | season | Int64 | Season of the slot. | | asof_t | Int64 | The slot rated; only games before it were used. | | team_id | String | ESPN team id (the `cfb_ratings` key dtype). | | theta | Float64 | Team strength in points, relative to the corpus average. | | hfa | Float64 | Home-field advantage in points (one value per fit). | | tau_team | Float64 | Fitted between-team spread of strength. | | sigma_obs | Float64 | Fitted per-game residual spread. |

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season of the slot rated. |
| `asof_t` | Int64 | The slot rated (regular-season week, or 100 + postseason week); only completed games before it were used. |
| `team_id` | String | ESPN team id (character, the cfb_ratings join key). |
| `theta` | Float64 | Team strength in points relative to the corpus average; expected home margin is theta_home - theta_away + hfa. |
| `hfa` | Float64 | Home-field advantage in points from this fit, applied off neutral sites (one value per fit). |
| `tau_team` | Float64 | Fitted between-team spread of strength in points. |
| `sigma_obs` | Float64 | Fitted per-game residual spread of the margin in points. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `schedule` has no completed `first_season` games, or `season` has no completed games before slot `t`. It never falls back to the closed form. |

**Example**

```python
from sportsdataverse.cfb import load_cfb_schedule, cfb_hierarchical_ratings
sched = load_cfb_schedule(seasons=list(range(2014, 2027)))
hier = cfb_hierarchical_ratings(sched, 2026, 13)
```

### cfb_kickoff_out_of_bounds_spot_expr {#cfb_kickoff_out_of_bounds_spot_expr}

`cfb_kickoff_out_of_bounds_spot_expr(season: polars.expr.expr.Expr) -> polars.expr.expr.Expr`

The receiving team's yardline after a kickoff out of bounds: the 40 through 2011, the 35 from 2012.

Reads `KICKOFF_OUT_OF_BOUNDS_SPOT_BY_SEASON` (the NCAA 2012 kickoff rule).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Expr` |  | Integer season expression (`pl.col("season")`). |

**Returns**

An integer expression of the out-of-bounds spot per row.

**Example**

```python
import polars as pl
from sportsdataverse.cfb import cfb_kickoff_out_of_bounds_spot_expr

df = pl.DataFrame({"season": [2011, 2012]})
print(df.select(cfb_kickoff_out_of_bounds_spot_expr(pl.col("season"))).to_series().to_list())  # [40, 35]
```

### cfb_predict_games {#cfb_predict_games}

`cfb_predict_games(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, era: 'str' = 'modern', model: "Literal['closed_form', 'hierarchical']" = 'closed_form', hierarchical: 'pl.DataFrame | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Predict a whole schedule of games from a ratings frame (vectorized).

Applies the three closed-form predictors across every row of `games` in
one pass. `ratings` is joined twice -- once on `home_team_id` and once on
`away_team_id` -- so each game carries both teams' `adj_net` / `adj_off_epa`
/ `adj_def_epa` / `off_pace`. The totals model's `game_pace` factor is
computed here as `home_off_pace * away_off_pace / league_avg_pace`, where the
league average is the mean `off_pace` of the passed ratings frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | Schedule frame with `game_id`, `home_team_id`, `away_team_id`, and `neutral_site` columns. The two team-id columns must share the dtype of `ratings["team_id"]` (asserted before the join). |
| `ratings` | `DataFrame` |  | A `cfb_ratings.cfb_ratings`-style frame with `team_id`, `adj_net`, `adj_off_epa`, `adj_def_epa`, and `off_pace`. |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS`. |
| `model` | `Literal['closed_form', 'hierarchical']` | `'closed_form'` | `"closed_form"` (the default and the served path) or `"hierarchical"` (opt-in). With `"hierarchical"`, `exp_margin` is `theta_home - theta_away + hfa` off neutral sites from `hierarchical`, and `home_win_prob` is `Phi(exp_margin / HIER_MARGIN_SD)`. `exp_total` stays closed-form, since the hierarchical model has no totals. |
| `hierarchical` | `DataFrame \| None` | `None` | The `~sportsdataverse.cfb.cfb_hierarchical.cfb_hierarchical_ratings` frame for the slot being predicted. Required when `model="hierarchical"`. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

One row per game with `game_id`, `home_team_id`, `away_team_id`, `neutral_site`, `exp_margin`, `home_win_prob`, `exp_total`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game identifier carried through from the input schedule. |
| `home_team_id` | String | Home team ESPN id (character; the ratings `team_id` join key). |
| `away_team_id` | String | Away team ESPN id (character; the ratings `team_id` join key). |
| `neutral_site` | Boolean | Whether the game is at a neutral site (home-field advantage is dropped when true). |
| `exp_margin` | Float64 | Expected home scoring margin in points (net_points_scale * net rating differential + the ridge-native home-field advantage on non-neutral fields). |
| `home_win_prob` | Float64 | Home win probability, Phi(exp_margin / margin_sd) under a Gaussian margin model. |
| `exp_total` | Float64 | Expected combined point total from the fitted efficiency + pace totals model. |

**Raises**

| Exception | Description |
|---|---|
| `AssertionError` | If either team-id join key's dtype disagrees with `ratings["team_id"]` (or `hierarchical["team_id"]`). |
| `ValueError` | If `model` is unknown, `model="hierarchical"` has no `hierarchical` frame, or a team in `games` has no hierarchical strength. It never falls back to the closed form. |

**Example**

```python
from sportsdataverse.cfb.cfb_game_predict import cfb_predict_games
from sportsdataverse.cfb import cfb_ratings
from sportsdataverse.cfb.cfb_schedule import cfb_schedule  # schedule loader
ratings = cfb_ratings(2023)
preds = cfb_predict_games(schedule_2023, ratings)

# Opt-in hierarchical margins (as of week 13, from completed games before it)

from sportsdataverse.cfb import load_cfb_schedule, cfb_hierarchical_ratings
hier = cfb_hierarchical_ratings(load_cfb_schedule(list(range(2014, 2027))), 2026, 13)
ratings_2026 = cfb_ratings(2026)  # still feeds exp_total
preds = cfb_predict_games(week13_games, ratings_2026, model="hierarchical", hierarchical=hier)
```

### cfb_ratings {#cfb_ratings}

`cfb_ratings(seasons: 'int | list[int]', *, as_of_date: 'datetime.date | None' = None, config: 'RatingsConfig | None' = None, fbs_only: 'bool' = True, drop_kneels: 'bool' = True, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

One row per team: the full CFB ratings spine (off/def/ST EPA + FEI).

Public orchestrator over `efficiency_ratings`,
`special_teams_ratings`, and `fei_ratings`. Loads play-by-play
+ schedule via `sportsdataverse.cfb.cfb_loaders.load_cfb_pbp` /
`sportsdataverse.cfb.cfb_loaders.load_cfb_schedule`, joins the
schedule's per-game date onto the plays, optionally applies the
as-of-date leakage boundary
(`sportsdataverse.cfb.cfb_prediction_constants.as_of_ratings_split`),
then fits all three component ratings on the (optionally filtered) plays
and reshapes them into one wide per-team table with dense ranks and a
net-rating z-score.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A single season (e.g. `2023`) or a list of seasons to pool into one combined fit. |
| `as_of_date` | `date \| None` | `None` | When given, the leakage boundary -- only plays from games with `date < as_of_date` are used to fit the ratings (mirrors what was knowable heading into that date). `None` (default) uses the full season(s), unfiltered. |
| `config` | `RatingsConfig \| None` | `None` | Ratings tuning knobs forwarded to all three component functions. Defaults to `RatingsConfig` when omitted. |
| `fbs_only` | `bool` | `True` | Keep only FBS-vs-FBS games (gameonpaper `cfb-team-summaries` parity) -- both of the schedule's `home_division` / `away_division` must be `"fbs"`. Default True. Skipped (all games kept) when the schedule lacks the division columns; pass False to rate FCS opponents as regular teams. |
| `drop_kneels` | `bool` | `True` | Strip kneel-downs before fitting (gameonpaper parity). Default True. Uses a pipeline `kneel_down` flag when present, otherwise the play-text regex (`kneel` / `takes a knee`) plus the end-of-half anonymized-TEAM-run clock heuristic; skipped when neither a flag nor a play-text column exists. Pass False to let kneels with non-null EPA flow into the fit. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; otherwise polars. |

**Returns**

A DataFrame with one row per `team_id`, columns in this order: `season` (Int64 -- the single passed season for the common single-season call; `null` for a pooled multi-season call, since no single season applies to every row), `team_id` (Utf8), `adj_off_epa`, `adj_def_epa` (Float64, from `efficiency_ratings`), `adj_st_epa` (Float64, from `special_teams_ratings`), `adj_net` (Float64 -- offense minus defense only; special teams is a separate column, not folded in), `fei_off`, `fei_def`, `fei_net` (Float64, from `fei_ratings`), `games` (Int64), `off_pace` (Float64 -- scrimmage plays per game, the tempo input the totals model uses), `off_rank` (Int64, dense rank on `adj_off_epa` descending), `def_rank` (Int64, dense rank on `adj_def_epa` **ascending** -- fewer EPA allowed ranks better), `net_rank` (Int64, dense rank on `adj_net` descending), `net_z` (Float64, z-score of `adj_net`), `fei_off_rank` (Int64, dense rank on `fei_off` descending), `fei_def_rank` (Int64, dense rank on `fei_def` **ascending** -- fewer drive EPA allowed ranks better), `fei_net_rank` (Int64, dense rank on `fei_net` descending). Zero-row (correctly-typed) when the requested season(s) have no published pbp/schedule asset, or when `as_of_date` filters out every play.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (4-digit year). |
| `team_id` | String | ESPN team id. |
| `adj_off_epa` | Float64 |  |
| `adj_def_epa` | Float64 |  |
| `adj_st_epa` | Float64 |  |
| `adj_net` | Float64 |  |
| `fei_off` | Float64 |  |
| `fei_def` | Float64 |  |
| `fei_net` | Float64 |  |
| `games` | Int64 | Number of games included in the ATS summary. |
| `off_pace` | Float64 |  |
| `off_rank` | Int64 |  |
| `def_rank` | Int64 |  |
| `net_rank` | Int64 |  |
| `net_z` | Float64 |  |
| `fei_off_rank` | Int64 |  |
| `fei_def_rank` | Int64 |  |
| `fei_net_rank` | Int64 |  |

**Raises**

| Exception | Description |
|---|---|
| `KeyError` | If the loaded plays frame is missing a required column. |
| `ImportError` | If `scikit-learn` is not installed. |

**Example**

```python
from sportsdataverse.cfb.cfb_ratings import cfb_ratings
ratings = cfb_ratings(2023)
ratings.sort("net_rank").head()

# As-of-date leakage boundary

import datetime as dt
week3 = cfb_ratings(2023, as_of_date=dt.date(2023, 9, 18))

# Pandas round-trip

ratings_pd = cfb_ratings(2023, return_as_pandas=True)
```

### cfb_ratings_compute_results {#cfb_ratings_compute_results}

`cfb_ratings_compute_results(teams: 'pl.DataFrame', games: 'pl.DataFrame', week_num: 'int', *, ratings: 'FrameLike', rating_type: 'str' = 'points', scale: 'float | dict[str, float] | None' = None, home_field: 'float | None' = None, margin_sd: 'float | dict[str, float] | None' = None, rng: 'np.random.Generator | None' = None, **kwargs: 'Any') -> 'dict[str, pl.DataFrame]'`

Results generator driven by fixed team ratings (points, adjusted EPA or raw EPA).

An alternative to `cfb_compute_results` (dynamic ELO), with cfbseedR's
`cfbseedR_ratings_compute_results()` semantics and defaults: each unplayed `week == week_num`
game gets `Normal(scale * (rating_home - rating_away) + home_field, margin_sd)` (no home field at
a neutral site), rounded away from zero; a postseason draw of exactly 0 is re-decided by
`Phi(expected / margin_sd)` with a 1-point margin. Ratings are fixed; `teams` passes through.
`scale` / `margin_sd` buckets use the smaller `games` of the two teams (unrated or null counts
skipped), then the bucket with the largest lower bound at or below it (bottom bucket below all;
top bucket when no count). Defaults: `"adj_epa"` (the scale of `cfb_ratings` `adj_net`)
reads the modern-era `slope_by_games` / `margin_sd` / `hfa_points` of
`sportsdataverse.cfb.cfb_prediction_constants.CFB_CONSTANTS`; `"points"` and `"raw_epa"`
are `_RATINGS_DEFAULTS`, cfbseedR's walk-forward fit (FBS-vs-FBS 2014-2025). In that test the ELO
formula on CollegeFootballData's pregame ELO had the smallest error (MAE 12.94 vs 13.10 for a
least-squares points rating); from about six games on the points rating is level with it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `teams` | `DataFrame` |  | Per-sim team table (`sim`, `team`, `conference`); returned unchanged. |
| `games` | `DataFrame` |  | Per-sim games table (engine schema; see `sportsdataverse.cfb.cfb_standings`). |
| `week_num` | `int` |  | The week to fill. |
| `ratings` | `FrameLike` |  | One row per team: `team`, `rating` (numeric, no nulls) and optionally `games` (non-negative games behind the rating; null = unknown). Polars or pandas. Teams missing from it rate 0 (league average). |
| `rating_type` | `str` | `'points'` | `"points"` (default), `"adj_epa"` or `"raw_epa"`; picks the defaults. |
| `scale` | `float \| dict[str, float] \| None` | `None` | Points of margin per rating unit: a number or `{"lo-hi": value}` buckets. `None` uses the default for `rating_type`. |
| `home_field` | `float \| None` | `None` | Home-field advantage in points for non-neutral games. `None` = default. |
| `margin_sd` | `float \| dict[str, float] \| None` | `None` | Margin SD in points (at least 0): a number or buckets. `None` = default. |
| `rng` | `Generator \| None` | `None` | numpy Generator (seeded by `cfb_simulations`). A fresh default generator is created when omitted. |

**Returns**

`{"teams": teams, "games": games}` with `result` (Float64 home margin) filled for this week's previously unplayed games.

| col_name | type | description |
|---|---|---|
| `sim` | Int64 | Simulation identifier the game row belongs to (1..n simulated seasons). |
| `week` | Int64 | Week of the season the game is played in; only games matching the requested week_num are filled. |
| `game_type` | String | Game classification in the seedr engine schema - REG (regular season), CONF_CHAMP (conference championship) or POST (postseason/CFP). |
| `home_team` | String | Team name of the home team in the simulated game (returned games frame). |
| `away_team` | String | Team name of the away team in the simulated game (returned games frame). |
| `result` | Float64 | Home-team margin of victory (home score minus away score) - real results are preserved and the target week's unplayed games are filled from the fixed team ratings. |
| `neutral` | Int64 | Neutral-site flag (1 = neutral site, 0 = true home game; only non-neutral games receive home_field). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | On an unknown `rating_type`, missing `team` / `rating` columns, a null, NaN or duplicated rating, a bad `games` count, malformed or NaN constants, or a negative `margin_sd`. |

**Example**

```python
import polars as pl
from sportsdataverse.cfb.cfb_simulations import cfb_ratings_compute_results
ratings = pl.DataFrame({"team": ["A", "B"], "rating": [7.0, -7.0], "games": [8, 8]})
out = cfb_ratings_compute_results(teams, games, 3, ratings=ratings, rating_type="points")

# Simulate a season with it (``cfb_simulations`` passes only ``rng``, so bind the ratings with ``functools.partial``)

import functools
from sportsdataverse.cfb.cfb_simulations import cfb_ratings_compute_results, cfb_simulations
cr = functools.partial(cfb_ratings_compute_results, ratings=ratings, rating_type="points")
sims = cfb_simulations(games, teams, compute_results=cr, seed=1)
```

### cfb_recruiting_projection {#cfb_recruiting_projection}

`cfb_recruiting_projection(target_season: 'int', *, division: 'str' = 'fbs', history_seasons: 'list[int] | None' = None, alpha: 'float' = 1.0, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Project team wins / scoring margin for a season from preseason roster features.

Fits a ridge regression of realized wins (and average scoring margin) on
`[talent_composite, blue_chip_ratio, off_returning, def_returning,
prior_wins]` over strictly-prior seasons, then predicts the target season
from its preseason-known features. The as-of boundary is enforced
internally: rows with `season >= target_season` never enter training even
if `history_seasons` includes them.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `target_season` | `int` |  | Season to project. |
| `division` | `str` | `'fbs'` | Division slug for constants lookups. |
| `history_seasons` | `list[int] \| None` | `None` | Seasons to draw training rows from (default: the six seasons before `target_season`). |
| `alpha` | `float` | `1.0` | Ridge L2 penalty. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

Per team: `season` (Int64, = target), `team_id` (Utf8 ESPN id), `pred_wins`, `pred_margin` (Float64), `pred_net_epa` (Float64, currently null -- the adjusted-EPA target's hosted pbp source 404s). Zero-row (typed) when no history is available.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Target season being projected (equals the requested target_season). |
| `team_id` | String | ESPN team id as a string (integer-origin). |
| `pred_wins` | Float64 | Ridge-projected season win total from preseason roster features. |
| `pred_margin` | Float64 | Ridge-projected average scoring margin per game. |
| `pred_net_epa` | Float64 | Reserved adjusted-EPA projection - currently null (the hosted pbp source 404s). |

**Example**

```python
from sportsdataverse.cfb import cfb_recruiting_projection
proj = cfb_recruiting_projection(2024)
proj.sort("pred_wins", descending=True).head(10)
```

### cfb_roster_talent {#cfb_roster_talent}

`cfb_roster_talent(seasons: 'int | list[int]', *, division: 'str' = 'fbs', composite_247: 'pl.DataFrame | None' = None, max_class_size: 'int' = 25, rank_decay: 'float' = 0.75, recruits: 'pl.DataFrame | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Team-talent composite per team-season (247 Team Talent Composite style).

Talent is the class-recency-weighted sum of per-recruit star points over the
trailing eligible recruiting classes (window = the length of the division's
`class_recency_weights`). When a 247 team-talent snapshot is supplied via
`composite_247`, its value overrides the derived composite for matched
team-seasons (the derived value remains the fallback).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | Target season or list of seasons to rate. |
| `division` | `str` | `'fbs'` | Division slug for `get_constants` (star points, weights). |
| `composite_247` | `DataFrame \| None` | `None` | Optional frame with `season` (Int64), `team_id` (Utf8), `talent_247` (Float64). Join-key dtypes are asserted. |
| `max_class_size` | `int` | `25` | Top-N recruits per class that count toward `talent_composite`, ranked by star points. Defaults to the FBS limit of 25 initial counters. Raise it only deliberately: an uncapped sum measures class VOLUME, which put Air Force 7th nationally on 200 signees at a 0.000 blue-chip ratio. Largely superseded by `rank_decay`; retained as a hard floor. |
| `rank_decay` | `float` | `0.75` | Diminishing-returns exponent on a recruit's rank within their class (see `_RANK_DECAY`). 0.0 restores the flat sum. The default 0.75 was selected by sweeping against Spearman with actual wins, not chosen by taste. |
| `recruits` | `DataFrame \| None` | `None` | Pre-loaded per-recruit frame (the `load_recruit_classes` contract). Supplying it SKIPS the 247 fetch entirely, which is what the cfbfastR-cfb-data producer does when compiling from the raw store: a class is immutable once signed, but the composite spans a 4-season window, so fetching live re-pulled the same frozen classes once per target season (~20 min per call). Callers passing this own the frame's completeness. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

Per `(season, team_id)`: `team` (Utf8), `talent_composite` (Float64), `talent_rank` (Int64 dense rank desc within season), `blue_chip_ratio` (Float64), `n_recruits` (Int64). Zero-row (typed) when no recruits load.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season the talent composite describes (trailing eligible classes aggregated). |
| `team_id` | String | 247Sports signed-institution team key as a string (integer-origin; joins to the recruit feed, not ESPN). |
| `team` | String | 247Sports full team name - the cross-source name-join key (the recruit-feed and talent-feed id spaces differ). |
| `talent_composite` | Float64 | Class-recency-weighted sum of per-recruit star points (247 Team Talent Composite style); the 247 snapshot value when composite_247 is supplied. |
| `talent_rank` | Int64 | Dense rank on talent_composite descending within season (best = 1). |
| `blue_chip_ratio` | Float64 | Share of the trailing four signing classes rated 4+ stars. |
| `n_recruits` | Int64 | Total signees across the trailing recruiting-class window. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `composite_247` is supplied without the documented schema. |

**Example**

```python
from sportsdataverse.cfb.cfb_roster_talent import cfb_roster_talent
tal = cfb_roster_talent(2023)
tal.sort("talent_rank").head(10)
```
