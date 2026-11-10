---
title: "Men's college basketball — additional Python functions — Analytics: ncaa_mbb–weighted_avg"
sidebar_label: "Analytics: ncaa_mbb–weighted_avg"
sidebar_position: 17
description: "Men's college basketball — additional Python functions — Analytics: ncaa_mbb–weighted_avg — function reference in sdv-py, the SportsDataverse Python package."
---
# Men's college basketball — additional Python functions — Analytics: ncaa_mbb–weighted_avg

### ncaa_mbb_on_off {#ncaa_mbb_on_off}

`ncaa_mbb_on_off(players: 'Union[str, Sequence[str]]', lineups: 'pl.DataFrame', *, included: 'Union[str, Sequence[str], None]' = None, excluded: 'Union[str, Sequence[str], None]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Team stats for every on/off combination of the given players.

Port of bigballR `on_off_generator` (`all_functions.R:2555-2749`,
`include_transition=F` path). For k players, all `2^k` on/off
assignments are enumerated in R's `expand.grid` order (first player
varies fastest; first row all-On, last all-Off); each combination sums
the lineups whose membership matches exactly, re-derives the full ratio
block (including `e_poss`), rounds to 3 decimals, and zeroes NA/Inf.
Combinations matching zero lineups produce an all-zero row.

Faithful R quirk (kept, flagged): when `included`/`excluded` is
passed, the base lineup set comes from the membership filter ONLY — the
inferred-team filter is skipped (`all_functions.R:2584-2589`), so an
included player on another team would leak that team's lineups in.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `players` | `Union[str, Sequence[str]]` |  | Player name(s) to split on (the `Status` axis). |
| `lineups` | `DataFrame` |  | Lineups frame from `ncaa_mbb_lineups`. |
| `included` | `Union[str, Sequence[str], None]` | `None` | Optional membership filter forwarded to `ncaa_mbb_player_lineups` (replaces the team filter). |
| `excluded` | `Union[str, Sequence[str], None]` | `None` | Optional membership filter forwarded to `ncaa_mbb_player_lineups` (replaces the team filter). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`pl.DataFrame` (or `pd.DataFrame`) with `2^k` rows — `status` (e.g. `"A.PLAYER On | B.PLAYER Off"`) + the 69 stat columns (`ON_OFF_COLUMNS`).

| col_name | type | description |
|---|---|---|
| `status` | String | Status label. |
| `mins` | Float64 |  |
| `o_mins` | Float64 |  |
| `d_mins` | Float64 |  |
| `o_poss` | Float64 |  |
| `d_poss` | Float64 |  |
| `ortg` | Float64 |  |
| `drtg` | Float64 |  |
| `netrtg` | Float64 |  |
| `pts` | Float64 | Points scored. |
| `d_pts` | Float64 |  |
| `fga` | Float64 | Field goal attempts. |
| `d_fga` | Float64 |  |
| `fgm` | Float64 | Field goals made. |
| `d_fgm` | Float64 |  |
| `tpa` | Float64 |  |
| `d_tpa` | Float64 |  |
| `tpm` | Float64 |  |
| `d_tpm` | Float64 |  |
| `fta` | Float64 | Free throw attempts. |
| `d_fta` | Float64 |  |
| `ftm` | Float64 | Free throws made. |
| `d_ftm` | Float64 |  |
| `rima` | Float64 |  |
| `d_rima` | Float64 |  |
| `rimm` | Float64 |  |
| `d_rimm` | Float64 |  |
| `orb` | Float64 |  |
| `d_orb` | Float64 |  |
| `drb` | Float64 |  |
| `d_drb` | Float64 |  |
| `blk` | Float64 | Blocks. |
| `d_blk` | Float64 |  |
| `to` | Float64 | To. |
| `d_to` | Float64 |  |
| `ast` | Float64 | Assists. |
| `d_ast` | Float64 |  |
| `e_poss` | Float64 |  |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `d_fg_pct` | Float64 |  |
| `tpp` | Float64 |  |
| `d_tpp` | Float64 |  |
| `ftp` | Float64 |  |
| `d_ftp` | Float64 |  |
| `efg_pct` | Float64 |  |
| `d_efg_pct` | Float64 |  |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `d_ts_pct` | Float64 |  |
| `rim_pct` | Float64 |  |
| `d_rim_pct` | Float64 |  |
| `mid_pct` | Float64 |  |
| `d_mid_pct` | Float64 |  |
| `tp_rate` | Float64 |  |
| `d_tp_rate` | Float64 |  |
| `rim_rate` | Float64 |  |
| `d_rim_rate` | Float64 |  |
| `mid_rate` | Float64 |  |
| `d_mid_rate` | Float64 |  |
| `ft_rate` | Float64 | Ft rate. |
| `d_ft_rate` | Float64 |  |
| `ast_rate` | Float64 |  |
| `d_ast_rate` | Float64 |  |
| `to_rate` | Float64 | To rate. |
| `d_to_rate` | Float64 |  |
| `blk_rate` | Float64 |  |
| `o_blk_rate` | Float64 |  |
| `orb_pct` | Float64 | Offensive rebound percentage. |
| `drb_pct` | Float64 | Defensive rebound percentage. |
| `time_per_poss` | Float64 |  |
| `d_time_per_poss` | Float64 |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If the players' team cannot be uniquely inferred from the lineups (R's `"ERROR- Player team not found"`). |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineups import ncaa_mbb_on_off
split = ncaa_mbb_on_off("KEATON.WAGLER", lineups)
print(split.shape)

# Two-player interaction

duo = ncaa_mbb_on_off(["A.PLAYER", "B.PLAYER"], lineups)

# Pipeline next step (one line)

split.select("status", "netrtg")
```

### ncaa_mbb_player_combos {#ncaa_mbb_player_combos}

`ncaa_mbb_player_combos(lineups: 'pl.DataFrame', *, n: 'int' = 2, min_mins: 'float' = 0, included: 'Union[str, Sequence[str], None]' = None, excluded: 'Union[str, Sequence[str], None]' = None, include_transition: 'bool' = False, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Team stats for every n-player combination on the court together.

Port of bigballR `get_player_combos` + `team_comb`
(`get_player_combos.R:19-38` + `:42-191`). Combos are enumerated per
team over the byte-sorted unique player pool (lexicographic
`gtools::combinations` order), filtered to combos whose lineups total
strictly more than `min_mins` minutes, then each combo's lineup rows
are summed and the ratio block re-derived. **Rounding is 2 decimals**
here (R rounds 3 in `get_lineups` / `on_off_generator`) and `e_poss`
is the SUM of the per-lineup estimates, not a recompute — both faithful.

R's `include_transition` switch is dead code (an exact-match guard at
`get_player_combos.R:28-30` always forces it back to `FALSE`); the
port implements the suffix-match intent but keeps the `False` default,
which is the only R-reachable behavior.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineups` | `DataFrame` |  | Lineups frame from `ncaa_mbb_lineups`. |
| `n` | `int` | `2` | Combination size, 1-5. |
| `min_mins` | `float` | `0` | Keep combos with total on-court minutes strictly greater than this (summed over the rounded per-lineup `mins`). |
| `included` | `Union[str, Sequence[str], None]` | `None` | Player name(s) that must be on the court in every lineup considered. |
| `excluded` | `Union[str, Sequence[str], None]` | `None` | Player name(s) that must be off the court in every lineup considered. |
| `include_transition` | `bool` | `False` | Re-derive the `_trans`/`_half` ratio surface (requires a transition lineups frame; forced `False` otherwise). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`pl.DataFrame` (or `pd.DataFrame`) with one row per combo: `team, p1..pn` + the stat surface of the input frame. Teams appear in byte-sorted order.

| col_name | type | description |
|---|---|---|
| `team` | String | Team-side label or team identifier. |
| `p1` | String |  |
| `p2` | String |  |
| `mins` | Float64 |  |
| `o_mins` | Float64 |  |
| `d_mins` | Float64 |  |
| `o_poss` | Float64 |  |
| `d_poss` | Float64 |  |
| `ortg` | Float64 |  |
| `drtg` | Float64 |  |
| `netrtg` | Float64 |  |
| `pts` | Float64 | Points scored. |
| `d_pts` | Float64 |  |
| `fga` | Float64 | Field goal attempts. |
| `d_fga` | Float64 |  |
| `fgm` | Float64 | Field goals made. |
| `d_fgm` | Float64 |  |
| `tpa` | Float64 |  |
| `d_tpa` | Float64 |  |
| `tpm` | Float64 |  |
| `d_tpm` | Float64 |  |
| `fta` | Float64 | Free throw attempts. |
| `d_fta` | Float64 |  |
| `ftm` | Float64 | Free throws made. |
| `d_ftm` | Float64 |  |
| `rima` | Float64 |  |
| `d_rima` | Float64 |  |
| `rimm` | Float64 |  |
| `d_rimm` | Float64 |  |
| `orb` | Float64 |  |
| `d_orb` | Float64 |  |
| `drb` | Float64 |  |
| `d_drb` | Float64 |  |
| `blk` | Float64 | Blocks. |
| `d_blk` | Float64 |  |
| `to` | Float64 | To. |
| `d_to` | Float64 |  |
| `ast` | Float64 | Assists. |
| `d_ast` | Float64 |  |
| `e_poss` | Float64 |  |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `d_fg_pct` | Float64 |  |
| `tpp` | Float64 |  |
| `d_tpp` | Float64 |  |
| `ftp` | Float64 |  |
| `d_ftp` | Float64 |  |
| `efg_pct` | Float64 |  |
| `d_efg_pct` | Float64 |  |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `d_ts_pct` | Float64 |  |
| `rim_pct` | Float64 |  |
| `d_rim_pct` | Float64 |  |
| `mid_pct` | Float64 |  |
| `d_mid_pct` | Float64 |  |
| `tp_rate` | Float64 |  |
| `d_tp_rate` | Float64 |  |
| `rim_rate` | Float64 |  |
| `d_rim_rate` | Float64 |  |
| `mid_rate` | Float64 |  |
| `d_mid_rate` | Float64 |  |
| `ft_rate` | Float64 | Ft rate. |
| `d_ft_rate` | Float64 |  |
| `ast_rate` | Float64 |  |
| `d_ast_rate` | Float64 |  |
| `to_rate` | Float64 | To rate. |
| `d_to_rate` | Float64 |  |
| `blk_rate` | Float64 |  |
| `o_blk_rate` | Float64 |  |
| `orb_pct` | Float64 | Offensive rebound percentage. |
| `drb_pct` | Float64 | Defensive rebound percentage. |
| `time_per_poss` | Float64 |  |
| `d_time_per_poss` | Float64 |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `n` is not an integer from 1 to 5. |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineups import ncaa_mbb_player_combos
duos = ncaa_mbb_player_combos(lineups, n=2, min_mins=5)
print(duos.shape)

# Anchored on one player

trios = ncaa_mbb_player_combos(lineups, n=3, included="KEATON.WAGLER")

# Pipeline next step (one line)

duos.sort("netrtg", descending=True).head()
```

### ncaa_mbb_player_lineups {#ncaa_mbb_player_lineups}

`ncaa_mbb_player_lineups(lineups: 'pl.DataFrame', *, included: 'Union[str, Sequence[str], None]' = None, excluded: 'Union[str, Sequence[str], None]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Filter a lineups frame by on-court player membership.

Port of bigballR `get_player_lineups` (`all_functions.R:2761-2792`):
keep rows where every `included` player is on the court (in `p1..p5`)
and no `excluded` player is. With both filters `None` the input is
returned unchanged (R's `Included = NA, Excluded = NA` passthrough).
Membership is tested by name against `p1..p5` (R tests the positional
first five columns); row order is preserved.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineups` | `DataFrame` |  | Lineups frame from `ncaa_mbb_lineups` (any frame with `p1..p5` works). |
| `included` | `Union[str, Sequence[str], None]` | `None` | Player name(s) that must ALL be on the court. |
| `excluded` | `Union[str, Sequence[str], None]` | `None` | Player name(s) that must NONE be on the court. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

Row-subset of `lineups`; schema unchanged.

| col_name | type | description |
|---|---|---|
| `p1` | String |  |
| `p2` | String |  |
| `p3` | String |  |
| `p4` | String |  |
| `p5` | String |  |
| `team` | String | Team-side label or team identifier. |
| `mins` | Float64 |  |
| `o_mins` | Float64 |  |
| `d_mins` | Float64 |  |
| `o_poss` | Float64 |  |
| `d_poss` | Float64 |  |
| `ortg` | Float64 |  |
| `drtg` | Float64 |  |
| `netrtg` | Float64 |  |
| `pts` | Float64 | Points scored. |
| `d_pts` | Float64 |  |
| `fga` | Float64 | Field goal attempts. |
| `d_fga` | Float64 |  |
| `fgm` | Float64 | Field goals made. |
| `d_fgm` | Float64 |  |
| `tpa` | Float64 |  |
| `d_tpa` | Float64 |  |
| `tpm` | Float64 |  |
| `d_tpm` | Float64 |  |
| `fta` | Float64 | Free throw attempts. |
| `d_fta` | Float64 |  |
| `ftm` | Float64 | Free throws made. |
| `d_ftm` | Float64 |  |
| `rima` | Float64 |  |
| `d_rima` | Float64 |  |
| `rimm` | Float64 |  |
| `d_rimm` | Float64 |  |
| `orb` | Float64 |  |
| `d_orb` | Float64 |  |
| `drb` | Float64 |  |
| `d_drb` | Float64 |  |
| `blk` | Float64 | Blocks. |
| `d_blk` | Float64 |  |
| `to` | Float64 | To. |
| `d_to` | Float64 |  |
| `ast` | Float64 | Assists. |
| `d_ast` | Float64 |  |
| `e_poss` | Float64 |  |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `d_fg_pct` | Float64 |  |
| `tpp` | Float64 |  |
| `d_tpp` | Float64 |  |
| `ftp` | Float64 |  |
| `d_ftp` | Float64 |  |
| `efg_pct` | Float64 |  |
| `d_efg_pct` | Float64 |  |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `d_ts_pct` | Float64 |  |
| `rim_pct` | Float64 |  |
| `d_rim_pct` | Float64 |  |
| `mid_pct` | Float64 |  |
| `d_mid_pct` | Float64 |  |
| `tp_rate` | Float64 |  |
| `d_tp_rate` | Float64 |  |
| `rim_rate` | Float64 |  |
| `d_rim_rate` | Float64 |  |
| `mid_rate` | Float64 |  |
| `d_mid_rate` | Float64 |  |
| `ft_rate` | Float64 | Ft rate. |
| `d_ft_rate` | Float64 |  |
| `ast_rate` | Float64 |  |
| `d_ast_rate` | Float64 |  |
| `to_rate` | Float64 | To rate. |
| `d_to_rate` | Float64 |  |
| `blk_rate` | Float64 |  |
| `o_blk_rate` | Float64 |  |
| `orb_pct` | Float64 | Offensive rebound percentage. |
| `drb_pct` | Float64 | Defensive rebound percentage. |
| `time_per_poss` | Float64 |  |
| `d_time_per_poss` | Float64 |  |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineups import ncaa_mbb_player_lineups
on = ncaa_mbb_player_lineups(lineups, included="KEATON.WAGLER")
print(on.shape)

# Included + excluded combination

df = ncaa_mbb_player_lineups(lineups, included=["A.PLAYER"], excluded=["B.PLAYER"])

# Pipeline next step (one line)

on.select(pl.col("mins").sum())
```

### ncaa_possession_rate_rows {#ncaa_possession_rate_rows}

`ncaa_possession_rate_rows(possessions: 'pl.DataFrame', schedule: 'pl.DataFrame', *, league: 'str' = 'mens') -> 'pl.DataFrame'`

Map `ncaa_{mbb,wbb}_possessions` rows onto the team-season-rates possession frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | `load_ncaa_mbb_possessions` / `load_ncaa_wbb_possessions` rows. |
| `schedule` | `DataFrame` |  | `load_mbb_schedule` / `load_wbb_schedule` rows (`game_id`, `neutral_site`), joined on `espn_game_id`. |
| `league` | `str` | `'mens'` | `"mens"` (two 20-minute halves) or `"womens"` (10-minute quarters from 2016, halves before). |

**Returns**

One row per possession in `~sportsdataverse._common.team_season_rates.RATE_ROWS_SCHEMA`, with null four-factor counts.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends. |
| `game_id` | String | The stats.ncaa.org contest id. |
| `period` | Int64 | Period of the possession; overtimes follow the last regulation period. |
| `possession_number` | Int64 | Order of the possession in the game. |
| `late` | Boolean | True in the 2nd half (men; women before 2016), the 4th quarter (women from 2016) or an overtime: the periods the clutch window can fall in. |
| `start_clock` | Float64 | Seconds left in the period when the possession starts: its length plus the lengths of the later possessions in the period. |
| `end_clock` | Float64 | Seconds left in the period when the possession ends. |
| `offense_team_id` | Int64 | ESPN id of the team with the ball; null for a non-D1 team. |
| `defense_team_id` | Int64 | ESPN id of the team on defense; null for a non-D1 team. |
| `offense_is_home` | Boolean | True when the offense is the (nominal) home team; it signs the running score for the clutch margin. |
| `neutral_site` | Boolean | True for an ESPN neutral-site game (False when ESPN does not carry the game); such a game is in neither the home nor the away split. |
| `points` | Int64 | Points the offense scored on the possession. |
| `fga` | Float64 | Always null: the NCAA possessions carry no fga counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `fgm` | Float64 | Always null: the NCAA possessions carry no fgm counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `fg3m` | Float64 | Always null: the NCAA possessions carry no fg3m counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `fta` | Float64 | Always null: the NCAA possessions carry no fta counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `oreb` | Float64 | Always null: the NCAA possessions carry no oreb counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `dreb` | Float64 | Always null: the NCAA possessions carry no dreb counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `tov` | Float64 | Always null: the NCAA possessions carry no tov counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `ff_poss` | Float64 | Always null: the turnover rate's denominator comes from the team box. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_mbb_schedule, load_ncaa_mbb_possessions
from sportsdataverse.mbb.mbb_team_season_rates import ncaa_possession_rate_rows
rows = ncaa_possession_rate_rows(load_ncaa_mbb_possessions(2025), load_mbb_schedule(2025))
```

### ncaa_score_rows {#ncaa_score_rows}

`ncaa_score_rows(pbp: 'pl.DataFrame', schedule: 'pl.DataFrame', *, league: 'str' = 'mens') -> 'pl.DataFrame'`

Map the stats.ncaa.org play-by-play onto the game-flow score rows.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | `load_ncaa_mbb_pbp` / `load_ncaa_wbb_pbp` rows, in release (game) order (`contest_id`, `season`, `period`, `game_seconds`, `home_score`, `away_score`, `event_team_espn_team_id`, `home_espn_team_id`, `away_espn_team_id`, `espn_game_id`). |
| `schedule` | `DataFrame` |  | `load_mbb_schedule` / `load_wbb_schedule` rows (`game_id`, `neutral_site`), joined on `espn_game_id`; a contest it lacks is not neutral. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |

**Returns**

Rows in `~sportsdataverse._common.game_flow.SCORE_ROWS_SCHEMA`, without `home_wp`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `game_id` | String | stats.ncaa.org contest id. |
| `order` | Int64 | The play's position in its game (the pbp order_index, or the release row order for college). |
| `home_team_id` | Int64 | The home team's id (stats.nba.com, or the ESPN team id for college; null for a non-D1 host). |
| `away_team_id` | Int64 | The away team's id (stats.nba.com, or the ESPN team id for college; null for a non-D1 visitor). |
| `neutral_site` | Boolean | True for a neutral-site game (no home-court prior); always False in the NBA / WNBA stats schedule, which carries no venue. |
| `elapsed` | Float64 | Game-clock seconds run when the play happens, counted from the tip. |
| `sec_left` | Float64 | The win-probability clock: seconds left in regulation, or in the overtime period. |
| `game_seconds` | Float64 | Game length in seconds: regulation plus the overtimes played. |
| `event_team_id` | Int64 | The play's team (the model's ball side); 0 or null for a team-less play. |
| `home_score` | Int64 | Home running score after the play. |
| `away_score` | Int64 | Away running score after the play. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_mbb_schedule, load_ncaa_mbb_pbp
from sportsdataverse.mbb.mbb_game_flow import ncaa_score_rows
rows = ncaa_score_rows(load_ncaa_mbb_pbp(2026), load_mbb_schedule(2026))
```

### ncaa_situation_rows {#ncaa_situation_rows}

`ncaa_situation_rows(possessions: 'pl.DataFrame', schedule: 'pl.DataFrame', *, league: 'str' = 'mens') -> 'pl.DataFrame'`

Map `ncaa_{mbb,wbb}_possessions` rows onto the situational-splits possession frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | `load_ncaa_mbb_possessions` / `load_ncaa_wbb_possessions` rows. |
| `schedule` | `DataFrame` |  | `load_mbb_schedule` / `load_wbb_schedule` rows (`game_id`, `neutral_site`). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |

**Returns**

One row per possession in `~sportsdataverse._common.situational_splits.situation_row_schema` (String player ids, null `is_second_chance`).

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends. |
| `game_id` | String | The stats.ncaa.org contest id. |
| `period` | Int64 | Period of the possession; overtimes follow the last regulation period. |
| `possession_number` | Int64 | Order of the possession in the game. |
| `late` | Boolean | True in the 2nd half (men; women before 2016), the 4th quarter (women from 2016) or an overtime: the periods the clutch window can fall in. |
| `start_clock` | Float64 | Seconds left in the period when the possession starts: its length plus the lengths of the later possessions in the period. |
| `end_clock` | Float64 | Seconds left in the period when the possession ends. |
| `offense_team_id` | Int64 | ESPN id of the team with the ball; null for a non-D1 team. |
| `defense_team_id` | Int64 | ESPN id of the team on defense; null for a non-D1 team. |
| `offense_is_home` | Boolean | True when the offense is the (nominal) home team; it signs the running score for the clutch margin. |
| `neutral_site` | Boolean | True for an ESPN neutral-site game (False when ESPN does not carry the game); such a game is in neither the home nor the away split. |
| `points` | Int64 | Points the offense scored on the possession. |
| `fga` | Float64 | Always null: the NCAA possessions carry no fga counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `fgm` | Float64 | Always null: the NCAA possessions carry no fgm counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `fg3m` | Float64 | Always null: the NCAA possessions carry no fg3m counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `fta` | Float64 | Always null: the NCAA possessions carry no fta counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `oreb` | Float64 | Always null: the NCAA possessions carry no oreb counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `dreb` | Float64 | Always null: the NCAA possessions carry no dreb counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `tov` | Float64 | Always null: the NCAA possessions carry no tov counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `ff_poss` | Float64 | Always null: the turnover rate's denominator comes from the team box. |
| `reg_periods` | Int64 | Regulation periods of the game: 4 quarters, or 2 halves. |
| `is_transition` | Boolean | The possession is transition (BB-D7's flag for the pros, the release's own for college). |
| `is_garbage_time` | Boolean | The possession is in garbage time (bigballR's rule; BB-D7 for the pros, the release's flag for college). |
| `is_second_chance` | Boolean | The possession had an offensive rebound; null in college (the NCAA possessions carry no rebound counts). |
| `off_1` | String | Offense player 1 on the floor as the possession starts. |
| `off_2` | String | Offense player 2 on the floor as the possession starts. |
| `off_3` | String | Offense player 3 on the floor as the possession starts. |
| `off_4` | String | Offense player 4 on the floor as the possession starts. |
| `off_5` | String | Offense player 5 on the floor as the possession starts. |
| `def_1` | String | Defense player 1 on the floor as the possession starts. |
| `def_2` | String | Defense player 2 on the floor as the possession starts. |
| `def_3` | String | Defense player 3 on the floor as the possession starts. |
| `def_4` | String | Defense player 4 on the floor as the possession starts. |
| `def_5` | String | Defense player 5 on the floor as the possession starts. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_mbb_schedule, load_ncaa_mbb_possessions
from sportsdataverse.mbb.mbb_situational import ncaa_situation_rows
rows = ncaa_situation_rows(load_ncaa_mbb_possessions(2025), load_mbb_schedule(2025))
```

### ncaa_stint_rows {#ncaa_stint_rows}

`ncaa_stint_rows(pbp: 'pl.DataFrame', possessions: 'pl.DataFrame', *, league: 'str' = 'mens') -> 'pl.DataFrame'`

Map the stats.ncaa.org pbp and possessions onto the stint-row frame.

One row per event in game order, on the event's lineup: its clock, its points (a free throw's
on the row of the game's last foul) and the possessions that start at it. A possession starts
at the first event of its `poss_num`; one whose `poss_team` is neither side is dropped.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | `load_ncaa_mbb_pbp` / `load_ncaa_wbb_pbp` rows, in release (game) order, with `period`, `game_seconds` and `poss_num`. |
| `possessions` | `DataFrame` |  | `load_ncaa_mbb_possessions` / `load_ncaa_wbb_possessions` rows (`contest_id`, `poss_num`, `poss_team`, `home`, `away`). |
| `league` | `str` | `'mens'` | `"mens"` (20-minute halves) or `"womens"` (10-minute quarters from 2016, halves before); overtimes are 5 minutes. |

**Returns**

Rows in `sportsdataverse._common.game_stints.stint_row_schema` with String player ids: `period`, `clock` (seconds left in the period after the event) and `order` (the event's row in the release).

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season of the action. |
| `game_id` | String | stats.ncaa.org contest id. |
| `home_team_id` | Int64 | Home ESPN team id (null for a non-D1 team). |
| `away_team_id` | Int64 | Away ESPN team id (null for a non-D1 team). |
| `home_1` | String | Home player 1 on the floor. |
| `home_2` | String | Home player 2 on the floor. |
| `home_3` | String | Home player 3 on the floor. |
| `home_4` | String | Home player 4 on the floor. |
| `home_5` | String | Home player 5 on the floor. |
| `away_1` | String | Away player 1 on the floor. |
| `away_2` | String | Away player 2 on the floor. |
| `away_3` | String | Away player 3 on the floor. |
| `away_4` | String | Away player 4 on the floor. |
| `away_5` | String | Away player 5 on the floor. |
| `seconds` | Float64 | Game clock that ran into this action (from the previous action of the period). |
| `home_pts` | Int64 | Points the home team scored on this action; a free throw's points sit on the row of the game's last foul. |
| `away_pts` | Int64 | Points the away team scored on this action; a free throw's points sit on the row of the game's last foul. |
| `home_poss` | Int64 | Possessions of the home team that start at this action. |
| `away_poss` | Int64 | Possessions of the away team that start at this action. |
| `period` | Int64 | Period of the action. |
| `clock` | Float64 | Period clock (seconds left) after the event; 0 on the period's last event, which takes the period's tail. |
| `order` | Int64 | The event's row in the pbp release (its place in the game). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_pbp, load_ncaa_mbb_possessions
from sportsdataverse.mbb.mbb_game_stints import ncaa_stint_rows
rows = ncaa_stint_rows(load_ncaa_mbb_pbp(2025), load_ncaa_mbb_possessions(2025))
```

### ncaa_team_box_count_rows {#ncaa_team_box_count_rows}

`ncaa_team_box_count_rows(team_box: 'pl.DataFrame') -> 'pl.DataFrame'`

Map `ncaa_{mbb,wbb}_team_box` rows onto the four-factor count frame.

Each row is one team's offense in one game: its FGA, FGM, 3PM, FTA, offensive rebounds and
turnovers, the opponent's defensive rebounds (`d_drb`) and its own possessions (`o_poss`).
`offense_is_home` is the box's own home designation (`team == home`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_box` | `DataFrame` |  | `load_ncaa_mbb_team_box` / `load_ncaa_wbb_team_box` rows. |

**Returns**

One row per (game, team) in `~sportsdataverse._common.team_season_rates.COUNT_ROWS_SCHEMA`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `game_id` | String | The stats.ncaa.org contest id. |
| `offense_team_id` | Int64 | ESPN id of the team whose offense the row counts; null for a non-D1 team. |
| `defense_team_id` | Int64 | ESPN id of its opponent; null for a non-D1 team. |
| `offense_is_home` | Boolean | True when the team is the game's (nominal) home team in the NCAA team box; a neutral-site game is in neither split. |
| `fga` | Float64 | Field-goal attempts in the game. |
| `fgm` | Float64 | Field goals made in the game. |
| `fg3m` | Float64 | Three-pointers made in the game. |
| `fta` | Float64 | Free-throw attempts in the game. |
| `oreb` | Float64 | Offensive rebounds in the game. |
| `dreb` | Float64 | The opponent's defensive rebounds in the game (the other half of the offensive-rebound chances). |
| `tov` | Float64 | Turnovers in the game. |
| `ff_poss` | Float64 | The team box's offensive possessions in the game, the turnover-rate denominator. |

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_team_box
from sportsdataverse.mbb.mbb_team_season_rates import ncaa_team_box_count_rows
counts = ncaa_team_box_count_rows(load_ncaa_mbb_team_box(2025))
```

### ncaa_team_coaches {#ncaa_team_coaches}

`ncaa_team_coaches(root: 'str | Path', seasons: 'int | list[int]', *, league: 'str' = 'mens') -> 'pl.DataFrame'`

Every team-season's head coach from a raw-repo tree of stats.ncaa.org roster pages.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `root` | `str \| Path` |  | The `ncaa-mbb-hoops-raw` / `ncaa-wbb-hoops-raw` checkout. |
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |

**Returns**

One row per page with a coach card, in `NCAA_COACHES_SCHEMA`; `team_id` is the ESPN team id from the season's `teams/json` crosswalk (null when it has none). A season with no crosswalk file is skipped with a warning.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `ncaa_team_id` | Int64 | stats.ncaa.org team-season id (the roster page's file name, teams/{id}). |
| `team_id` | Int64 | ESPN team id of the school, from the season's teams/json crosswalk; null when the crosswalk has none. |
| `team` | String | School name as stats.ncaa.org's crosswalk gives it. |
| `coach_id` | Int64 | NCAA person id of the head coach (stats.ncaa.org /people/{id}). |
| `coach` | String | Head coach's name on the page's Coach card. |
| `career_seasons` | Int64 | The coach's seasons as a head coach through this one, every school and division (the card's Seasons). |
| `career_wins` | Int64 | The coach's career wins through this season (the card's Record; the NCAA drops vacated wins). |
| `career_losses` | Int64 | The coach's career losses through this season (the card's Record). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

### order_lineup {#order_lineup}

`order_lineup(player_codes_and_ids: 'list[dict[str, str]]', players_by_id: 'dict[str, dict[str, Any]]', team_season: 'str') -> 'list[dict[str, str]]'`

Order a 5-man lineup `X1_X2_X3_X4_X5` into PG/SG/SF/PF/C slot order.

Faithful port of `PositionUtils.orderLineup` (`PositionUtils.ts:696-761`).
Greedily fits each player (in input order) to their best-scoring slot via
`_fit_player` (dominated by `pos_class_to_score` on the
player's `posClass`, tie-broken by their raw `posConfidences`),
evicting and recursively re-fitting any player displaced along the way,
then applies `apply_relative_positional_overrides` (keyed on
`team_season`) as a final hand-tuned correction pass.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_codes_and_ids` | `list[dict[str, str]]` |  | The lineup membership, each a `{"code": ..., "id": ...}` dict. Order does not affect the final result (the slot-fitting algorithm is order-invariant by construction -- displaced players are always re-fit). |
| `players_by_id` | `dict[str, dict[str, Any]]` |  | Per-player positional info keyed by `id`, each a `{"posConfidences": [pg, sg, sf, pf, c], "posClass": "..."}` dict (the tradPosList-ordered raw confidence scores plus the classifier's `ID_TO_POSITION`-keyed class label). |
| `team_season` | `str` |  | Key into `RELATIVE_POSITION_FIXES` for the final override pass. |

**Returns**

A 5-element list of `{"code": ..., "id": ...}` dicts in PG/SG/SF/PF/C order.

**Example**

```python
::

from sportsdataverse.mbb.mbb_positions import order_lineup
players_by_id = {
    "Cowan, Anthony": {"posConfidences": [60, 40, 10, 0, 0], "posClass": "s-PG"},
    "Ayala, Eric": {"posConfidences": [40, 60, 10, 0, 0], "posClass": "CG"},
}
order_lineup(
    [{"code": "AnCowan", "id": "Cowan, Anthony"},
     {"code": "ErAyala", "id": "Ayala, Eric"}],
    players_by_id, "",
)
```

### pos_class_to_score {#pos_class_to_score}

`pos_class_to_score(pos_class: 'str') -> 'int'`

Ordinal "positional weight" for a position class, PG=1000..C=8000.

Faithful port of `PositionUtils.posClassToScore` (`PositionUtils.ts:629-654`,
a literal `switch`). Unmapped classes default to `4000` (the TS
default-case comment notes "won't happen").

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pos_class` | `str` |  | A position-class code (e.g. `"PG"`, `"WF"`, `"C"`). |

**Returns**

The class's ordinal score.

**Example**

```python
::

from sportsdataverse.mbb.mbb_positions import pos_class_to_score
pos_class_to_score("WF")
```

### project_bracket {#project_bracket}

`project_bracket(resume: 'pl.DataFrame', auto_bids: 'set[str]', *, league: 'str' = 'mens', field_size: 'int' = 68) -> 'pl.DataFrame'`

Select and seed a tournament field from a per-team résumé frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `resume` | `DataFrame` |  | One row per (season, team_id) with `adj_em_z, sos, wab, quad1_w` (the ratings + strength-of-schedule outputs joined). |
| `auto_bids` | `set[str]` |  | `team_id` set of conference auto-bid teams (see `~sportsdataverse.mbb.mbb_college_season.conference_auto_bids`); always in the field. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (kept for shim parity; the blend is league-agnostic). |
| `field_size` | `int` | `68` | Tournament field size (68). |

**Returns**

One row per input team: `season, team_id, resume_score, projected_seed` (1-16, null outside the field; in a 68-team field the last four at-large teams and the four worst auto-bids share First Four slots, so every line has four slots), `at_large_prob` (logistic in `resume_score` centred on the selection cutoff -- every selected at-large clears 0.5), `auto_bid`, `at_large_rank` (1 = the best résumé without an auto-bid; null for auto-bids), `bid` (exactly `field_size` true).

No returns table is published for this function: no capture: it needs a resume frame (strength of schedule joined with the ratings' adj_em_z) that no package function returns.

**Example**

```python
from sportsdataverse.mbb.mbb_bracketology import project_bracket
field = project_bracket(resume, auto_bids)
```

### regress_shot_quality {#regress_shot_quality}

`regress_shot_quality(stat: 'float', pos: 'int', feat: 'str', player: 'dict[str, Any]') -> 'float'`

Shrink a small-sample shot-quality stat toward its positional average.

Faithful port of `PositionUtils.regressShotQuality`
(`PositionUtils.ts:216-258`). Only the three relative shot-quality
features (`calc_three_relative` / `calc_rim_relative` /
`calc_mid_relative`) are regressed; any other `feat` passes `stat`
through unchanged. A player is regressed toward the positional average
whenever the relevant shot volume is below `max(0.25 * total_fga, 15)`
(i.e. under 25% of their attempts come from that zone, floored at 15
attempts). A `center` (`pos == 4`) who took 0-2 threes and made none is
left at `0` to avoid widespread changes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stat` | `float` |  | The raw (unregressed) feature value. |
| `pos` | `int` |  | Position index (`0=pg` ... `4=c`). |
| `feat` | `str` |  | Feature field name (only the three relative shot-quality keys trigger regression; anything else is a passthrough). |
| `player` | `dict[str, Any]` |  | The player stat dict; reads `total_off_fga` and the per-feature volume field (`total_off_{3p,2pmid,2prim}_attempts`), each shaped `{"value": N}`. |

**Returns**

The regressed feature value (or `stat` unchanged when the feature is not regressed, volume is sufficient, or the center-3s carve-out fires).

**Raises**

| Exception | Description |
|---|---|
| `KeyError` | If `player` lacks `total_off_fga` (faithful to the TS, which throws on the same missing field). |

**Example**

```python
from sportsdataverse.mbb.mbb_positions import regress_shot_quality
player = {"total_off_fga": {"value": 25},
          "total_off_3p_attempts": {"value": 1}}
regress_shot_quality(-15.5, 2, "misc_feature", player)

# Low-volume shrink toward the positional average

regress_shot_quality(100, 3, "calc_rim_relative",
    {"total_off_fga": {"value": 25},
     "total_off_2prim_attempts": {"value": 8}})
```

### strength_of_schedule {#strength_of_schedule}

`strength_of_schedule(results: 'pl.DataFrame', ratings: 'pl.DataFrame', *, league: 'str' = 'mens') -> 'pl.DataFrame'`

Per-team SoS + Quad 1-4 record + WAB from completed games and ratings.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `results` | `DataFrame` |  | Completed games with `game_id, season, home_team_id, away_team_id, home_score, away_score, neutral_site`. |
| `ratings` | `DataFrame` |  | One row per team with `season, team_id, adj_em, rank` (the `mbb_team_ratings` output). Team-id dtype must match `results`. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (quad thresholds, HFA, bubble EM). |

**Returns**

One row per (season, team_id): `season, team_id, sos, sos_rank, wab, quad1_w .. quad4_l, quality_wins`. `sos` is the mean opponent `adj_em` (rank 1 = hardest schedule); quads follow the NET venue-adjusted opponent-rank thresholds; `quality_wins` is Quad-1 + Quad-2 wins; `wab` is actual wins minus a bubble-quality team's expected wins against the same schedule. Empty input returns the schema with zero rows.

No returns table is published for this function: no capture: it needs a schedule with home_team_id / away_team_id columns, and no package function returns one (load_mbb_schedule ships home_id / away_id).

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If the team-id dtypes of `results` and `ratings` disagree. |

**Example**

```python
from sportsdataverse.mbb.mbb_strength_of_schedule import strength_of_schedule
resume = strength_of_schedule(results, ratings)
```

### test_positional_aware_filter {#test_positional_aware_filter}

`test_positional_aware_filter(sorted_to_test: 'list[dict[str, str]]', pve_frags: 'list[dict[str, Any]]', nve_frags: 'list[dict[str, Any]]') -> 'bool'`

Check a positional-aware filter (from `build_positional_aware_filter`)

against a sorted (`order_lineup`-ordered) lineup array.

Faithful port of `PositionUtils.testPositionalAwareFilter`
(`PositionUtils.ts:831-858`). A fragment matches if any of its
position-restricted slots (or, when `pos` is empty, any slot at all)
has a `code`/`id` containing the fragment's filter text
(case-insensitive substring match). Every positive fragment must match
(vacuously true if there are none); no negative fragment may match
(vacuously true if there are none).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sorted_to_test` | `list[dict[str, str]]` |  | The ordered lineup, each a `{"id": ..., "code": ...}` dict (as returned by `order_lineup`). |
| `pve_frags` | `list[dict[str, Any]]` |  | Positive-filter fragments (must ALL match). |
| `nve_frags` | `list[dict[str, Any]]` |  | Negative-filter fragments (NONE may match). |

**Returns**

Whether the lineup satisfies both the positive and negative filters.

**Example**

```python
::

from sportsdataverse.mbb.mbb_positions import test_positional_aware_filter
lineup = [{"code": "AnCowan", "id": "Cowan, Anthony"}]
test_positional_aware_filter(lineup, [{"filter": "cowan", "pos": []}], [])
```

### using_roster_pos {#using_roster_pos}

`using_roster_pos(pos_class: 'str', roster_pos: 'str | None') -> 'tuple[str, str | None]'`

Reconcile a stats-derived position class against roster metadata.

Faithful port of `PositionUtils.usingRosterPos` (`PositionUtils.ts:583-626`).
When the classifier landed on an "unsure" bucket (`"G?"`/`"F/C?"`),
roster info narrows it (a roster `"C"` always wins outright); otherwise
an obviously-wrong stats classification is compromised toward the
roster-implied side, gated by `pos_class_to_score` thresholds.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pos_class` | `str` |  | The stats-derived position class. |
| `roster_pos` | `str \| None` |  | The roster-reported position (`"G"`/`"F"`/`"C"`), or `None`/`""` when unknown. `if (rosterPos)` (ts:587) is a plain JS truthiness check on a string -- `""` and `None` behave identically (both mean "no correction"), so `if not roster_pos` is the faithful Python mirror, not an `is None` landmine. |

**Returns**

A `(position, info)` tuple. `info` is `None` when no correction/explanation applies (matches the TS `undefined`), else a human-readable note on why the position was adjusted.

**Example**

```python
from sportsdataverse.mbb.mbb_positions import using_roster_pos
using_roster_pos("G?", "C")
```

### weighted_avg {#weighted_avg}

`weighted_avg(mutable_acc: 'LineupStatSet', obj: 'LineupStatSet') -> 'None'`

Merge `obj` into `mutable_acc` with possession weighting.

Faithful port of `LineupUtils.weightedAvg` (`LineupUtils.ts:645`).
Mutates `mutable_acc` in place (matching the upstream mutable-state
contract) and returns `None`. Each call accumulates a **weighted
sum**, not a weighted average -- the companion `completeWeightedAvg`
(upstream `LineupUtils.ts:752`, not yet ported) divides by the
accumulated weight totals to finish the average. The per-field weight
used at each merge step is derived from `obj`'s *own* totals (e.g.
that single lineup's `total_off_fga`), not from any running total on
`mutable_acc` -- callers accumulating many lineups must call
`weighted_avg` once per lineup so every lineup contributes its own
weight.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `mutable_acc` | `LineupStatSet` |  | The running accumulator (`LineupStatSet`). Mutated in place; fields absent from the accumulator are initialized to `{"value": 0.0}` (plus `old_value` / `override` when `obj`'s field carries a luck-adjustment `override` marker) before `obj`'s contribution is added. |
| `obj` | `LineupStatSet` |  | The per-lineup `LineupStatSet` document to merge in. |

**Returns**

None. `mutable_acc` is mutated in place.

**Raises**

| Exception | Description |
|---|---|
| `NotImplementedError` | If `obj` carries a `game_info` key -- that merge branch is deliberately deferred (see module docstring). |

**Example**

```python
from sportsdataverse.mbb.mbb_lineup_stats import weighted_avg

acc: dict = {}
weighted_avg(acc, lineup_a)
weighted_avg(acc, lineup_b)
print(acc["off_poss"]["value"])  # plain sum (SUM_FIELDS)

# Two-lineup possession-weighted merge

acc = {}
for lineup in three_lineups:
    weighted_avg(acc, lineup)
# acc now holds weighted SUMS; complete_weighted_avg (not yet
# ported) is required to turn these into rate-stat averages.
```
