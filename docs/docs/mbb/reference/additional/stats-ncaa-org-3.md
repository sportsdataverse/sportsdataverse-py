---
title: "MBB — additional Python functions — stats.ncaa.org: ncaa_mbb–td_at"
sidebar_label: "stats.ncaa.org: ncaa_mbb–td_at"
sidebar_position: 5
description: "MBB — additional Python functions — stats.ncaa.org: ncaa_mbb–td_at — function reference in sdv-py, the SportsDataverse Python package."
---
# MBB — additional Python functions — stats.ncaa.org: ncaa_mbb–td_at

### ncaa_mbb_join_pbp_shots {#ncaa_mbb_join_pbp_shots}

`ncaa_mbb_join_pbp_shots(pbp: 'pl.DataFrame', shots: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, Any]'"`

Attach shot-chart coordinates to play-by-play rows (bigballR

`join_pbp_shots`, `get_shot_locations.R:93-135`).

FG attempts (`shot_value` 2/3) are matched to chart shots on
`(game_id, game_seconds, event_result == shot_result, shot_no)` where
`shot_no` is the within-second same-result sequence number on BOTH
sides — free throws and non-shot rows are deliberately excluded from
matching (the chart plots FGs only) and pass through NA-filled. Row
count and per-game row order are preserved; the explicit `shot_dist`
carry-through is fork-skew fix #11 (wbigballR's unexported copy drops
it). Works identically for the WBB extension — feed it quarter-model
pbp + shots frames.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | The 35-column snake_case pbp contract frame (see `~sportsdataverse.mbb.mbb_ncaa_game_pbp.PBP_SCHEMA`). |
| `shots` | `DataFrame` |  | A `SHOTS_SCHEMA` frame (from `parse_ncaa_bb_shots` / `ncaa_mbb_shot_locations`) covering exactly the same game ids. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The input pbp frame + `team`, `player`, `x`, `y`, `shot_dist` (null on non-FG rows and unmatched FG rows), sorted by (game_id, original per-game row order) exactly as R's `arrange(row, .by_group = TRUE)`.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `home` | character | Home. |
| `away` | character | Away record. |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `clock` | character | Game clock value. |
| `game_time` | character | Game start time. |
| `game_seconds` | integer |  |
| `home_score` | integer | Home team score at the time of the play. |
| `away_score` | integer | Away team score at the time of the play. |
| `event_team` | character |  |
| `event_description` | character |  |
| `player_1` | character |  |
| `player_2` | character |  |
| `event_type` | character | Event / play type code (V2 PBP). |
| `event_result` | character |  |
| `shot_value` | integer | Point value of the shot (2 or 3). |
| `event_length` | integer |  |
| `poss_num` | integer |  |
| `poss_team` | character |  |
| `poss_length` | integer |  |
| `is_transition` | logical |  |
| `home_1` | character |  |
| `home_2` | character |  |
| `home_3` | character |  |
| `home_4` | character |  |
| `home_5` | character |  |
| `away_1` | character |  |
| `away_2` | character |  |
| `away_3` | character |  |
| `away_4` | character |  |
| `away_5` | character |  |
| `status` | character | Status label. |
| `is_garbage_time` | logical |  |
| `sub_deviate` | integer |  |
| `team` | character | Team-side label or team identifier. |
| `player` | character | Player name. |
| `x` | double | X. |
| `y` | double | Y. |
| `shot_dist` | double |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If the pbp and shots game-id sets differ (R `stop("PBP and Shot Locations do not match.")`, `:97-99`), or if a join-key dtype disagrees between the two frames. |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_game_pbp import ncaa_mbb_play_by_play
from sportsdataverse.mbb.mbb_ncaa_shots import (
    ncaa_mbb_join_pbp_shots,
    ncaa_mbb_shot_locations,
)
pbp = ncaa_mbb_play_by_play(["6470186"])
shots = ncaa_mbb_shot_locations(["6470186"])
joined = ncaa_mbb_join_pbp_shots(pbp, shots)

# Pipeline next step (one line)

joined.filter(pl.col("x").is_not_null()).head()
```

### ncaa_mbb_player_stats {#ncaa_mbb_player_stats}

`ncaa_mbb_player_stats(pbp: 'pl.DataFrame', *, multi_games: 'bool' = False, simple: 'bool' = False, fix_tip_in: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Aggregate bigballR-contract play-by-play into per-player box stats.

Port of bigballR `get_player_stats` (`all_functions.R:2810-3177`) +
its `get_mins` helper (`:3240-3263`). Counting stats are summarised
per (game, team, player), assists counted from `player_2`, minutes and
offensive possessions derived from the ten on-court columns, and rates
(FG%, TS%, eFG%, rim/mid splits, ...) computed from the counters and
rounded to 3 decimals with R's `round` semantics. With
`multi_games=True` the per-game rows are summed per (player, team),
every rate is recomputed from the summed counters (never averaged), and
`GP`/`GS` are appended.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame in the sdv-py 35-column snake_case bigballR contract. May span multiple games. |
| `multi_games` | `bool` | `False` | When True, aggregate across games per (player, team) — the season-stat surface. When False (default, R parity), treat each game separately and keep the game id columns. |
| `simple` | `bool` | `False` | When True, return the reduced 33-column (multi) / 35-column (per-game) surface without the transition / assisted / putback / block-location splits. |
| `fix_tip_in` | `bool` | `True` | When True (default), rim and putback stats count the scrape engine's real `"Tip In"` vocabulary. When False, reproduce R's literal `"Tip-In"` test (`all_functions.R:2827`) — tip-ins silently excluded — for oracle parity. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`pl.DataFrame` (or `pd.DataFrame`): one row per player+team (+game when `multi_games=False`). Columns follow `PLAYER_STATS_COLUMNS` / `PLAYER_STATS_SIMPLE_COLUMNS` / `PLAYER_GAME_STATS_COLUMNS` / `PLAYER_GAME_STATS_SIMPLE_COLUMNS`. Rows sorted by the group keys (byte order, matching dplyr's C-locale group order). Empty input yields an empty frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `home` | character | Home. |
| `away` | character | Away record. |
| `team` | character | Team-side label or team identifier. |
| `player` | character | Player name. |
| `mins` | double |  |
| `o_poss` | double |  |
| `pts` | double | Points scored. |
| `orb` | double |  |
| `drb` | double |  |
| `ast` | double | Assists. |
| `stl` | double | Steals. |
| `blk` | double | Blocks. |
| `tov` | double | Turnovers. |
| `pf` | double | Personal fouls. |
| `ts_pct` | double | True shooting percentage (0-1). |
| `efg_pct` | double |  |
| `fgm` | double | Field goals made. |
| `fga` | double | Field goal attempts. |
| `fg_pct` | double | Field goal percentage (0-1). |
| `tpm` | double |  |
| `tpa` | double |  |
| `tp_pct` | double |  |
| `ftm` | double | Free throws made. |
| `fta` | double | Free throw attempts. |
| `ft_pct` | double | Free throw percentage (0-1). |
| `rimm` | double |  |
| `rima` | double |  |
| `rim_pct` | double |  |
| `midm` | double |  |
| `mida` | double |  |
| `mid_pct` | double |  |
| `pbackm` | double |  |
| `pbacka` | double |  |
| `pback_pct` | double |  |
| `blk_rim` | double |  |
| `blk_mid` | double |  |
| `blk_three` | double |  |
| `pct_fga_trans` | double |  |
| `pct_tpa_trans` | double |  |
| `pct_rima_trans` | double |  |
| `pct_fgm_trans` | double |  |
| `pct_tpm_trans` | double |  |
| `pct_rimm_trans` | double |  |
| `pct_fgm_ast` | double |  |
| `pct_tpm_ast` | double |  |
| `pct_rimm_ast` | double |  |
| `pts_trans` | double |  |
| `orb_trans` | double |  |
| `drb_trans` | double |  |
| `ast_trans` | double |  |
| `stl_trans` | double |  |
| `blk_trans` | double |  |
| `tov_trans` | double |  |
| `ts_pct_trans` | double |  |
| `efg_pct_trans` | double |  |
| `fgm_trans` | double |  |
| `fga_trans` | double |  |
| `fg_pct_trans` | double |  |
| `tpm_trans` | double |  |
| `tpa_trans` | double |  |
| `tp_pct_trans` | double |  |
| `ftm_trans` | double |  |
| `fta_trans` | double |  |
| `ft_pct_trans` | double |  |
| `rimm_trans` | double |  |
| `rima_trans` | double |  |
| `rim_pct_trans` | double |  |
| `midm_trans` | double |  |
| `mida_trans` | double |  |
| `mid_pct_trans` | double |  |
| `pts_half` | double |  |
| `orb_half` | double |  |
| `drb_half` | double |  |
| `ast_half` | double |  |
| `stl_half` | double |  |
| `blk_half` | double |  |
| `tov_half` | double |  |
| `ts_pct_half` | double |  |
| `efg_pct_half` | double |  |
| `fgm_half` | double |  |
| `fga_half` | double |  |
| `fg_pct_half` | double |  |
| `tpm_half` | double |  |
| `tpa_half` | double |  |
| `tp_pct_half` | double |  |
| `ftm_half` | double |  |
| `fta_half` | double |  |
| `ft_pct_half` | double |  |
| `rimm_half` | double |  |
| `rima_half` | double |  |
| `rim_pct_half` | double |  |
| `midm_half` | double |  |
| `mida_half` | double |  |
| `mid_pct_half` | double |  |
| `pts_ast` | double |  |
| `fgm_ast` | double |  |
| `tpm_ast` | double |  |
| `rimm_ast` | double |  |
| `midm_ast` | double |  |
| `pts_unast` | double |  |
| `efg_pct_unast` | double |  |
| `fgm_unast` | double |  |
| `fga_unast` | double |  |
| `fg_pct_unast` | double |  |
| `tpm_unast` | double |  |
| `tpa_unast` | double |  |
| `tp_pct_unast` | double |  |
| `rimm_unast` | double |  |
| `rima_unast` | double |  |
| `rim_pct_unast` | double |  |
| `midm_unast` | double |  |
| `mida_unast` | double |  |
| `mid_pct_unast` | double |  |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stats_agg import ncaa_mbb_player_stats
season = ncaa_mbb_player_stats(pbp, multi_games=True)
print(season.shape)

# Reduced surface, pandas out

df_pd = ncaa_mbb_player_stats(pbp, multi_games=True, simple=True, return_as_pandas=True)

# Pipeline next step (one line)

season.filter(pl.col("mins") > 50).sort("pts", descending=True).head()
```

### ncaa_mbb_possessions {#ncaa_mbb_possessions}

`ncaa_mbb_possessions(pbp: 'pl.DataFrame', *, simple: 'bool' = False, fix_cross_game_leak: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Aggregate bigballR-contract play-by-play into one row per possession.

Port of bigballR `get_possessions` (`all_functions.R:3686-3745`).
Groups by the possession keys stamped upstream by the scrape engine
(`poss_num`, `poss_team`, the ten on-court lineup columns, plus game
identity), drops possessions with any missing on-court player, and — in
the full variant — sorts each row's home/away lineup alphabetically so a
given lineup always occupies the same columns.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame in the sdv-py 35-column snake_case bigballR contract (`parse_ncaa_bb_game_pbp` output). May span multiple games; rows must be in scrape order. |
| `simple` | `bool` | `False` | When True, return only the 17-column possession/points frame (`all_functions.R:3687-3694`) with lineups in on-court order. When False (default), return the full 28-column frame with per-possession context columns and alpha-sorted lineups. |
| `fix_cross_game_leak` | `bool` | `True` | When True (default, and the CORRECT behavior), window the `start_event_type` lag with `.over("game_id")` so a game's first possession has a null start event instead of inheriting the PREVIOUS game's last event. When False, reproduce R's ungrouped `dplyr::lag` (`all_functions.R:3698`) and its cross-game leak. Parity tests pass False. Ignored when `simple=True` (that variant emits no `start_event_type`). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`pl.DataFrame` (or `pd.DataFrame`) with one row per possession — 28 columns per `POSSESSION_SEG_SCHEMA` (full) or 17 per `POSSESSIONS_SIMPLE_SCHEMA` (simple). Empty input yields an empty frame carrying the documented schema.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `home` | character | Home. |
| `away` | character | Away record. |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `poss_num` | integer |  |
| `poss_team` | character |  |
| `home_1` | character |  |
| `home_2` | character |  |
| `home_3` | character |  |
| `home_4` | character |  |
| `home_5` | character |  |
| `away_1` | character |  |
| `away_2` | character |  |
| `away_3` | character |  |
| `away_4` | character |  |
| `away_5` | character |  |
| `home_score` | integer | Home team score at the time of the play. |
| `away_score` | integer | Away team score at the time of the play. |
| `pts` | integer | Points scored. |
| `is_assisted` | integer |  |
| `is_transition` | integer |  |
| `is_garbage_time` | integer |  |
| `start_event_type` | character |  |
| `first_shot_time` | integer |  |
| `first_shot_type` | character |  |
| `last_event_time` | integer |  |
| `last_event_type` | character |  |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_possession_seg import ncaa_mbb_possessions
poss = ncaa_mbb_possessions(pbp)
print(poss.shape)

# Simple points-per-possession variant

poss_pd = ncaa_mbb_possessions(pbp, simple=True, return_as_pandas=True)

# Pipeline next step (one line)

poss.group_by("poss_team").agg(pl.col("pts").mean())
```

### ncaa_mbb_shot_locations {#ncaa_mbb_shot_locations}

`ncaa_mbb_shot_locations(game_ids: "'Sequence[object]'", *, fetcher: 'Optional[_SupportsFetchGameBox]' = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, Any]'"`

Scrape MBB shot locations for one or more games (bigballR

`get_shot_locations`, `get_shot_locations.R:3-89`).

Fetches each game's `stats.ncaa.org/contests/{id}/box_score` page and
parses the embedded shot-chart JS through `parse_ncaa_bb_shots`.
NA ids are dropped up front (R `:5`); per-game "shots found" messages
go to the module logger (R `message`, `:69-70`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_ids` | `Sequence[object]` |  | NCAA contest ids; `None`/NaN entries are dropped. |
| `fetcher` | `Optional[_SupportsFetchGameBox]` | `None` | Optional injected fetcher exposing `fetch_game_box` (for tests/offline use). Defaults to a fresh `NcaaFetcher.with_browser()` context per call. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

All games' shots row-bound (zero-row `SHOTS_SCHEMA` frame when no ids survive or no charts are found).

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `clock` | character | Game clock value. |
| `game_seconds` | integer |  |
| `team` | character | Team-side label or team identifier. |
| `player` | character | Player name. |
| `shot_result` | character | Shot result ('Made' / 'Missed'). |
| `x` | double | X. |
| `y` | double | Y. |
| `shot_dist` | double |  |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_shots import ncaa_mbb_shot_locations
df = ncaa_mbb_shot_locations(["6470186", "6479639"])
print(df.shape)

# Offline with an injected fetcher

df = ncaa_mbb_shot_locations(["6470186"], fetcher=my_fetcher)

# Pipeline next step (one line)

df.group_by("team").agg(pl.col("shot_dist").mean()).head()
```

### ncaa_mbb_team_stats {#ncaa_mbb_team_stats}

`ncaa_mbb_team_stats(pbp: 'pl.DataFrame', *, include_transition: 'bool' = False, fix_tip_in: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Aggregate bigballR-contract play-by-play into per-team game stats.

Port of bigballR `get_team_stats` (`all_functions.R:2530-2538`): the
ten on-court columns are blanked so every row shares one "lineup", then
`get_lineups` (`ncaa_mbb_lineups`) runs per game and the lineup
key columns are dropped — yielding two rows (one per team) per game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame in the sdv-py 35-column snake_case bigballR contract. May span multiple games. |
| `include_transition` | `bool` | `False` | When True, append the `_trans`/`_half` split surface plus `o_trans_pct`/`d_trans_pct`. |
| `fix_tip_in` | `bool` | `True` | When True (default), rim stats count the scrape engine's real `"Tip In"` vocabulary; `False` reproduces R's literal `"Tip-In"` bug for oracle parity. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`pl.DataFrame` (or `pd.DataFrame`) with one row per team per game — `TEAM_STATS_COLUMNS` (73) or `TEAM_STATS_TRANSITION_COLUMNS` with `include_transition=True`. Games ordered by the Utf8 `game_id` byte sort (R's do() sorts a numeric ID — identical for equal-width ids), teams within a game byte-sorted. Empty input yields an empty frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `home` | character | Home. |
| `away` | character | Away record. |
| `team` | character | Team-side label or team identifier. |
| `mins` | double |  |
| `o_mins` | double |  |
| `d_mins` | double |  |
| `o_poss` | double |  |
| `d_poss` | double |  |
| `ortg` | double |  |
| `drtg` | double |  |
| `netrtg` | double |  |
| `pts` | double | Points scored. |
| `d_pts` | double |  |
| `fga` | double | Field goal attempts. |
| `d_fga` | double |  |
| `fgm` | double | Field goals made. |
| `d_fgm` | double |  |
| `tpa` | double |  |
| `d_tpa` | double |  |
| `tpm` | double |  |
| `d_tpm` | double |  |
| `fta` | double | Free throw attempts. |
| `d_fta` | double |  |
| `ftm` | double | Free throws made. |
| `d_ftm` | double |  |
| `rima` | double |  |
| `d_rima` | double |  |
| `rimm` | double |  |
| `d_rimm` | double |  |
| `orb` | double |  |
| `d_orb` | double |  |
| `drb` | double |  |
| `d_drb` | double |  |
| `blk` | double | Blocks. |
| `d_blk` | double |  |
| `to` | double | To. |
| `d_to` | double |  |
| `ast` | double | Assists. |
| `d_ast` | double |  |
| `e_poss` | double |  |
| `fg_pct` | double | Field goal percentage (0-1). |
| `d_fg_pct` | double |  |
| `tpp` | double |  |
| `d_tpp` | double |  |
| `ftp` | double |  |
| `d_ftp` | double |  |
| `efg_pct` | double |  |
| `d_efg_pct` | double |  |
| `ts_pct` | double | True shooting percentage (0-1). |
| `d_ts_pct` | double |  |
| `rim_pct` | double |  |
| `d_rim_pct` | double |  |
| `mid_pct` | double |  |
| `d_mid_pct` | double |  |
| `tp_rate` | double |  |
| `d_tp_rate` | double |  |
| `rim_rate` | double |  |
| `d_rim_rate` | double |  |
| `mid_rate` | double |  |
| `d_mid_rate` | double |  |
| `ft_rate` | double | Ft rate. |
| `d_ft_rate` | double |  |
| `ast_rate` | double |  |
| `d_ast_rate` | double |  |
| `to_rate` | double | To rate. |
| `d_to_rate` | double |  |
| `blk_rate` | double |  |
| `o_blk_rate` | double |  |
| `orb_pct` | double | Offensive rebound percentage. |
| `drb_pct` | double | Defensive rebound percentage. |
| `time_per_poss` | double |  |
| `d_time_per_poss` | double |  |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stats_agg import ncaa_mbb_team_stats
teams = ncaa_mbb_team_stats(pbp)
print(teams.shape)

# Transition splits, pandas out

df_pd = ncaa_mbb_team_stats(pbp, include_transition=True, return_as_pandas=True)

# Pipeline next step (one line)

teams.sort("netrtg", descending=True).head()
```

### normalize_player_key {#normalize_player_key}

`normalize_player_key(expr: 'pl.Expr') -> 'pl.Expr'`

Normalize a player name-code so possessions and rosters agree.

Three normalizations, each fixing an observed divergence between the two
sources:

* **Trailing dots** -- `possessions` writes `ANTONIA.BATES.` where
  `team_rosters` writes `ANTONIA.BATES`. Interior dots are the
  `FIRST.LAST` separator and are load-bearing, so only trailing ones go.
* **Diacritics** -- `ANAELLE.DUTAT` on the floor vs `ANAËLLE.DUTAT` on
  the roster. NFKD-decompose and drop combining marks.
* **Case** -- belt and braces; both sources are upper today.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `expr` | `Expr` |  | A `Utf8` expression holding the raw name-code. |

**Returns**

The normalized key expression.

**Example**

```python
import polars as pl
from sportsdataverse.mbb import normalize_player_key

pl.DataFrame({"code": ["anaëlle.dutat."]}).select(normalize_player_key(pl.col("code")))
# ANAELLE.DUTAT
```

### observed_pairs {#observed_pairs}

`observed_pairs(possessions: 'pl.DataFrame') -> 'pl.DataFrame'`

Distinct `(team, player)` pairs across the ten on-floor slots.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Published `ncaa_{lg}_possessions` frame. |

**Returns**

Unique `team`/`player` frame, the input `expand_xwalk_aliases` expects.

| col_name | type | description |
|---|---|---|
| `team` | character | Team-side label or team identifier. |
| `player` | character | Player name. |

**Example**

```python
::

from sportsdataverse.mbb.mbb_ncaa_rapm_input import observed_pairs

pairs = observed_pairs(possessions)
```

### phase1_shot_event_enrichment {#phase1_shot_event_enrichment}

`phase1_shot_event_enrichment(sorted_very_raw_events: 'list[tuple[int, ShotEvent]]', second_half_override: 'Optional[set[int]]' = None) -> 'list[ShotEvent]'`

The court-geometry enrichment pass: ascending time, coordinate

transform + geo synthesis, and the self-correcting side-flip re-run
(`ShotEventParser.phase1_shot_event_enrichment`, `:415-528`).

For each shot: compute the ascending game time, decide (from
`is_team_shooting_left_to_start` + which half the period falls in)
whether the shot's side needs flipping, run `transform_shot_location`
to get both the believed-correct and alternative (mirrored) locations,
keep whichever is closer to the basket (a >1.2x distance advantage for
the "alternative" wins, or ANY shot taken with <0.1 min left on the
clock always keeps the original -- a half-court heave near the buzzer
is plausible, so the tie-break favors trusting the raw geometry there),
then synthesize a lat/lon.

After all shots are processed, if any period had >=6 shots AND more than
75% of them came back implausibly long-distance (>50ft), the whole pass
re-runs ONCE with those periods' orientation flipped (the self-correcting
part) -- `second_half_override` is `None` on the initial call and a
non-`None` set on the one allowed retry, preventing infinite recursion.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sorted_very_raw_events` | `list[tuple[int, ShotEvent]]` |  | The chronologically-sorted (period, shot) pairs from `parse_shot_html`, pre-geometry-transform. |
| `second_half_override` | `Optional[set[int]]` | `None` | The set of periods whose `second_half_switch` orientation should be inverted (the self-correction re-run's input); `None` on the first call. |

**Returns**

The fully court-geometry-enriched shots, in the same order as `sorted_very_raw_events`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_shot_parser import phase1_shot_event_enrichment
shots = phase1_shot_event_enrichment([(1, very_raw_shot)])
```

### playwright_transport {#playwright_transport}

`playwright_transport(*, headless_new: 'bool' = True, challenge_wait_ms: 'int' = 8000, nav_timeout_ms: 'int' = 45000, user_agent: 'Optional[str]' = None, solve_attempts: 'int' = 3, relaunch_backoff: 'float' = 2.0) -> "'_PlaywrightTransport'"`

Build the **suggested** stats.ncaa.org game-detail scraping transport.

Drives a real Chromium via Playwright in Chrome's new-headless mode
(`--headless=new`) to clear the Akamai `bm-verify` challenge that
`curl_cffi` cannot, then serves raw server HTML for the 5a-5e parsers.
Playwright is a **lazy optional import** (not a hard dependency); a clear
`ImportError` fires on first use if it is missing.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `headless_new` | `bool` | `True` | Use `--headless=new` (real-GPU render, no window) -- the default and the proven-working mode. `False` runs old headless (`headless_shell`), which Akamai flags -- avoid. |
| `challenge_wait_ms` | `int` | `8000` | Milliseconds to let the bm-verify sensor run after the first navigation. |
| `nav_timeout_ms` | `int` | `45000` | Per-navigation timeout. |
| `user_agent` | `Optional[str]` | `None` | Override the Chrome UA string. |
| `solve_attempts` | `int` | `3` |  |
| `relaunch_backoff` | `float` | `2.0` |  |

**Returns**

A stateful, callable `FetchTransport` reusing one browser for the session. Close it when done (it is a context manager, has `close()`, and registers an `atexit` safety net).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher
with NcaaFetcher.with_browser() as fetcher:
    pbp = fetcher.fetch_game_pbp("1613299")               # raw PBP HTML
    box = fetcher.fetch_game_individual_stats("1613299")  # raw box HTML
# -> feed to get_box_lineup / create_lineup_data (mbb_ncaa_*_parser)
```

### possession_deciles {#possession_deciles}

`possession_deciles(players: 'pl.DataFrame', *, n_bins: 'int' = 10) -> 'pl.DataFrame'`

Equal-count possession bins: median of every SE column, mean of every coverage flag.

One table serves both SE validations: on `solve_rapm_league`
output the median posterior SE must fall with playing time (Spearman
strongly negative; the top deciles flatten at a collinearity floor --
heavy-minute starters share the floor with the same four teammates); on
`split_half_se_check` output the sampling coverage should sit
near 0.95 in every bin. Uses `poss` when present, else
`off_poss + def_poss`. Bins are rank-based (always `n_bins`
equal-count bins), so heavy ties at low possessions cannot collapse a
decile.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `players` | `DataFrame` |  | Output of `solve_rapm_league` or the `per_player` frame of `split_half_se_check`. |
| `n_bins` | `int` | `10` | Number of equal-count bins (10 = deciles). |

**Returns**

One row per bin, sorted: `decile` (0 = fewest possessions), `n`, `poss_min`, `poss_max`, `median_<se col>` ... and `coverage_<c>[_sampling]` ... (only for the columns present).

| col_name | type | description |
|---|---|---|
| `decile` | integer | Possession-exposure bin, 0 = the players with the fewest possessions. |
| `n` | integer | Number of players in the bin. |
| `poss_min` | integer | Fewest possessions of any player in the bin. |
| `poss_max` | integer | Most possessions of any player in the bin. |
| `median_orapm_se` | double | Median orapm_se of the players in the bin. |
| `median_drapm_se` | double | Median drapm_se of the players in the bin. |
| `median_rapm_net_se` | double | Median rapm_net_se of the players in the bin. |
| `median_orapm_se_sampling` | double | Median orapm_se_sampling of the players in the bin. |
| `median_drapm_se_sampling` | double | Median drapm_se_sampling of the players in the bin. |
| `median_rapm_net_se_sampling` | double | Median rapm_net_se_sampling of the players in the bin. |

**Example**

```python
d = possession_deciles(players)
assert d["median_rapm_net_se"][0] > d["median_rapm_net_se"][-1]
```

### remove_diacritics {#remove_diacritics}

`remove_diacritics(fragment: 'str') -> 'str'`

Strip diacritical marks, e.g. `"Juhász"` -> `"Juhasz"`

(`ExtractorUtils.scala:38-43`: NFD normalization then removal of the
combining-diacritical-marks block).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fragment` | `str` |  | Any string (a full player name or a name fragment). |

**Returns**

The string with combining marks removed.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stints import remove_diacritics
print(remove_diacritics("Dorka Juhász"))  # "Dorka Juhasz"
```

### remove_html_encoding {#remove_html_encoding}

`remove_html_encoding(html_str: 'str') -> 'str'`

Undo a handful of literal HTML entity escapes (``ExtractorUtils

.remove_html_encoding`, `ExtractorUtils.scala:25-33`). **Scope
addition, Task 5e.5** -- the first consumer is
`mbb_ncaa_shot_parser.parse_shot_html` (the `player` name / shooting
team name extracted from an SVG shot's `<title>` text).

In practice bs4/lxml already decode standard HTML entities (`&#39;`,
`&quot;`, `&amp;``) while parsing text nodes, so this is usually a
no-op by the time it runs on already-parsed text -- ported anyway for
exact behavioral parity with any double-escaped input the upstream
Scala guards against (JSoup has the same auto-decoding behavior, so the
Scala original is equally a defensive no-op in the common case).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `html_str` | `str` |  | Any string, typically already-parsed element text. |

**Returns**

`html_str` with `&#39;`/`&quot;`/`&amp;` replaced by their literal characters, only if `"&"` appears at all (short-circuit matching the Scala's `if (html_str.indexOf("&") >= 0)` guard).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stints import remove_html_encoding
remove_html_encoding("De&#39;Shayne")  # "De'Shayne"
remove_html_encoding("Plain Name")  # "Plain Name" (unchanged)
```

### reorder_and_reverse {#reorder_and_reverse}

`reorder_and_reverse(reversed_partial_events: 'Iterable[PlayByPlayEvent]') -> 'list[PlayByPlayEvent]'`

Orders same-minute play-by-play events so subs never enclose the plays

they logically precede/follow (`ExtractorUtils.scala:435-599`).

Groups consecutive events sharing the same `min` into a block (the
input arrives in descending/reverse-chronological order, so blocks are
discovered and internally accumulated in reverse too), then -- for any
block containing a sub -- reorders it via `inner_sort`: events
referencing a subbed-OUT player (or scoring no higher than the sub) land
in a pre-sub group, the subs themselves come next (in ascending-score
order), and events referencing a subbed-IN player (or scoring higher
than the sub) land in a trailing post-sub group. Free-throw attempts
sharing the sub's inferred "direction" (team vs. opponent, inferred from
the nearest preceding shot/FT/foul) are pulled into the pre-sub group
unless the shooter is one of the players being subbed in. Blocks with no
sub are returned unchanged apart from the initial score-based sort.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `reversed_partial_events` | `Iterable[PlayByPlayEvent]` |  | Events for one lineup event, in reverse-chronological (descending-time) order -- the natural order encountered walking play-by-play text bottom-up. |

**Returns**

The same events, forward-chronological (ascending time), with each same-minute block internally reordered so no sub encloses a play it logically shouldn't.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_models import Score
from sportsdataverse.mbb.mbb_ncaa_stints import (
    OtherTeamEvent,
    SubInEvent,
    reorder_and_reverse,
)
events = [
    SubInEvent(0.4, Score(0, 0), "player1"),
    OtherTeamEvent(0.4, Score(0, 0), "rebound"),
]
reorder_and_reverse(events)
# [OtherTeamEvent(...), SubInEvent(...)]
```

### reset_config {#reset_config}

`reset_config() -> 'NcaaFetchConfig'`

Reset the active config to its env-var-derived defaults.

**Returns**

The live singleton, now holding the env-var-derived defaults again.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_fetch import update_config, reset_config
update_config(timeout=5)
reset_config()
```

### resolve_possessions {#resolve_possessions}

`resolve_possessions(possessions: 'pl.DataFrame', xwalk: 'pl.DataFrame', *, non_di: "Literal['drop', 'pool']" = 'drop') -> 'pl.DataFrame'`

Attach a `player_id` to each of the ten on-floor slots.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Published `ncaa_{lg}_possessions` frame, carrying `home`/`away` and `home_1..5`/`away_1..5`. |
| `xwalk` | `DataFrame` |  | Output of `build_player_xwalk`. |
| `non_di` | `Literal['drop', 'pool']` | `'drop'` | How to treat possessions involving a team with no roster. `"drop"` (default, conservative) discards them; `"pool"` rewrites the team to `NON_DI_TEAM` so the D-I side keeps its minutes. |

**Returns**

`possessions` plus ten `{slot}_id` columns. `TEAM` slots and unresolved players carry a null id -- an explicit "not a rated player" that the design-matrix build must skip, never impute.

| col_name | type | description |
|---|---|---|
| `game_date` | character | Game date (YYYY-MM-DD). |
| `home` | character | Home. |
| `away` | character | Away record. |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `poss_num` | integer | Sequential possession number within the game. |
| `poss_team` | character | Name of the team in possession. |
| `home_1` | character | Name of the home team's on-floor player in lineup slot 1 for the possession, from the substitution walk-forward. |
| `home_2` | character | Name of the home team's on-floor player in lineup slot 2 for the possession, from the substitution walk-forward. |
| `home_3` | character | Name of the home team's on-floor player in lineup slot 3 for the possession, from the substitution walk-forward. |
| `home_4` | character | Name of the home team's on-floor player in lineup slot 4 for the possession, from the substitution walk-forward. |
| `home_5` | character | Name of the home team's on-floor player in lineup slot 5 for the possession, from the substitution walk-forward. |
| `away_1` | character | Name of the away team's on-floor player in lineup slot 1 for the possession, from the substitution walk-forward. |
| `away_2` | character | Name of the away team's on-floor player in lineup slot 2 for the possession, from the substitution walk-forward. |
| `away_3` | character | Name of the away team's on-floor player in lineup slot 3 for the possession, from the substitution walk-forward. |
| `away_4` | character | Name of the away team's on-floor player in lineup slot 4 for the possession, from the substitution walk-forward. |
| `away_5` | character | Name of the away team's on-floor player in lineup slot 5 for the possession, from the substitution walk-forward. |
| `home_score` | integer | Home team score at the time of the play. |
| `away_score` | integer | Away team score at the time of the play. |
| `pts` | integer | Points scored. |
| `is_assisted` | integer | Count of the possession's events that carry an assisting second player (0, 1, rarely 2) -- a count, not a flag, kept from the R source for parity. |
| `is_transition` | integer | 1 for transition possessions, else 0. |
| `is_garbage_time` | integer | 1 for possessions in garbage time under the score-margin and clock rule, else 0. |
| `start_event_type` | character | Event type that opened the possession (e.g., a defensive rebound or a made-basket inbound). |
| `first_shot_time` | integer | Seconds into the possession (poss_length) at its first field-goal attempt; null when the possession had no shot. |
| `first_shot_type` | character | stats.ncaa.org event name of the possession's first field-goal attempt ("Layup", "Three Point Jumper", "Two Point Jumper", "Hook", "Dunk", "Tip In"); null when the possession had no shot. |
| `last_event_time` | integer | Seconds into the possession (poss_length) at its final event. |
| `last_event_type` | character | Event type that ended the possession (e.g., a made shot, turnover, or defensive rebound). |
| `contest_id` | character | stats.ncaa.org contest (game) identifier. |
| `home_ncaa_team_id` | character | stats.ncaa.org team identifier for the home team. |
| `home_espn_team_id` | character | ESPN home team id (NA for bart-only rows). |
| `away_ncaa_team_id` | character | stats.ncaa.org team identifier for the away team. |
| `away_espn_team_id` | character | ESPN away team id (NA for bart-only rows). |
| `poss_team_ncaa_team_id` | character | stats.ncaa.org team identifier of the team in possession. |
| `poss_team_espn_team_id` | character | ESPN team identifier of the team in possession, via the NCAA-to-ESPN crosswalk. |
| `home_1_player_id` | character | stats.ncaa.org player identifier for the home slot-1 on-floor player. |
| `home_1_clean_name` | character | Normalized (diacritics- and punctuation-cleaned) name of the home slot-1 on-floor player. |
| `home_2_player_id` | character | stats.ncaa.org player identifier for the home slot-2 on-floor player. |
| `home_2_clean_name` | character | Normalized (diacritics- and punctuation-cleaned) name of the home slot-2 on-floor player. |
| `home_3_player_id` | character | stats.ncaa.org player identifier for the home slot-3 on-floor player. |
| `home_3_clean_name` | character | Normalized (diacritics- and punctuation-cleaned) name of the home slot-3 on-floor player. |
| `home_4_player_id` | character | stats.ncaa.org player identifier for the home slot-4 on-floor player. |
| `home_4_clean_name` | character | Normalized (diacritics- and punctuation-cleaned) name of the home slot-4 on-floor player. |
| `home_5_player_id` | character | stats.ncaa.org player identifier for the home slot-5 on-floor player. |
| `home_5_clean_name` | character | Normalized (diacritics- and punctuation-cleaned) name of the home slot-5 on-floor player. |
| `away_1_player_id` | character | stats.ncaa.org player identifier for the away slot-1 on-floor player. |
| `away_1_clean_name` | character | Normalized (diacritics- and punctuation-cleaned) name of the away slot-1 on-floor player. |
| `away_2_player_id` | character | stats.ncaa.org player identifier for the away slot-2 on-floor player. |
| `away_2_clean_name` | character | Normalized (diacritics- and punctuation-cleaned) name of the away slot-2 on-floor player. |
| `away_3_player_id` | character | stats.ncaa.org player identifier for the away slot-3 on-floor player. |
| `away_3_clean_name` | character | Normalized (diacritics- and punctuation-cleaned) name of the away slot-3 on-floor player. |
| `away_4_player_id` | character | stats.ncaa.org player identifier for the away slot-4 on-floor player. |
| `away_4_clean_name` | character | Normalized (diacritics- and punctuation-cleaned) name of the away slot-4 on-floor player. |
| `away_5_player_id` | character | stats.ncaa.org player identifier for the away slot-5 on-floor player. |
| `away_5_clean_name` | character | Normalized (diacritics- and punctuation-cleaned) name of the away slot-5 on-floor player. |
| `espn_game_id` | character | ESPN game id (NA for bart-only rows). |
| `season` | integer | Season year. |
| `home_1_id` | character | Resolved player_id of the home slot-1 player; null for a TEAM slot or an unresolved name. |
| `home_2_id` | character | Resolved player_id of the home slot-2 player; null for a TEAM slot or an unresolved name. |
| `home_3_id` | character | Resolved player_id of the home slot-3 player; null for a TEAM slot or an unresolved name. |
| `home_4_id` | character | Resolved player_id of the home slot-4 player; null for a TEAM slot or an unresolved name. |
| `home_5_id` | character | Resolved player_id of the home slot-5 player; null for a TEAM slot or an unresolved name. |
| `away_1_id` | character | Resolved player_id of the away slot-1 player; null for a TEAM slot or an unresolved name. |
| `away_2_id` | character | Resolved player_id of the away slot-2 player; null for a TEAM slot or an unresolved name. |
| `away_3_id` | character | Resolved player_id of the away slot-3 player; null for a TEAM slot or an unresolved name. |
| `away_4_id` | character | Resolved player_id of the away slot-4 player; null for a TEAM slot or an unresolved name. |
| `away_5_id` | character | Resolved player_id of the away slot-5 player; null for a TEAM slot or an unresolved name. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `non_di` is not `"drop"` or `"pool"`. |

### right_kind_of_shot {#right_kind_of_shot}

`right_kind_of_shot(shot: 'ShotEvent', pbp_event: 'MiscGameEvent', strict: 'bool') -> 'bool'`

Whether `pbp_event`'s shot type is compatible with `shot`'s

distance and make/miss (`ShotEnrichmentUtils.right_kind_of_shot`,
`PlayByPlayUtils.scala:659-679`).

The distance-in-the-data is approximate, so exact 2-vs-3 discrimination is
impossible; this only rules out the *obvious* mismatches (a clearly-short
shot matched to a 3, or vice versa) and always requires make/miss
agreement.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shot` | `ShotEvent` |  | The shot being enriched (`pts`/`dist` read). |
| `pbp_event` | `MiscGameEvent` |  | The candidate play-by-play event. |
| `strict` | `bool` |  | If `True`, also apply the distance gate; if `False`, only the make/miss agreement is required. |

**Returns**

`True` if the event could plausibly be this shot.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_pbp_glue import right_kind_of_shot
right_kind_of_shot(shot, pbp_event, strict=True)
```

### run_iterative_adjustment_with_hca {#run_iterative_adjustment_with_hca}

`run_iterative_adjustment_with_hca(teams: 'Sequence[TeamDetail]', team_by_name: 'dict[str, TeamDetail]', fields: 'Sequence[str]', league_averages: 'LeagueAverages', poss_splits: 'dict[str, PossessionSplits]', *, max_iterations: 'int' = 100, tolerance: 'float' = 1e-06) -> 'IterationResult'`

KenPom-style SoS + HCA fixed-point solver (`runIterativeAdjustmentWithHCA`, `ts:306-527`).

Each iteration (Jacobi -- all teams read the *previous* iteration's
adjustments, then commit together):

1. Per team/field, adjust every game
   `adj_game = raw_game * (league / (opp_adj +/- hca))` and take the
   weighted mean; a field with no valid games keeps its current value.
2. Re-estimate per-field HCA from home/away possession-imbalance residuals
   `hca = sum((raw - pred) * |imbalance|) / sum(|imbalance|)` over teams
   with `|imbalance| >= IMBALANCE_MIN`.

Stops when the max per-team/field change drops below `tolerance` or after
`max_iterations` sweeps (the HCA re-estimate still runs on the final
sweep). The cross-guard on the per-game branch, the asymmetric residual
prediction, and the cross-named opponent strengths are all preserved -- see
the module docstring's landmine list.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `teams` | `Sequence[TeamDetail]` |  | The teams to solve over. |
| `team_by_name` | `dict[str, TeamDetail]` |  | `{team_name: team_detail}` for opponent lookup. |
| `fields` | `Sequence[str]` |  | The stat fields to solve. |
| `league_averages` | `LeagueAverages` |  | Output of `compute_league_averages_from_per_game`. |
| `poss_splits` | `dict[str, PossessionSplits]` |  | `{team_name:` `PossessionSplits` `}`. |
| `max_iterations` | `int` | `100` | Iteration cap (default `MAX_ITERATIONS`; pin to `1` to inspect a single sweep). |
| `tolerance` | `float` | `1e-06` | Convergence tolerance (default `TOLERANCE`). |

**Returns**

An `IterationResult` (`adj_values`, `hca_per_field`).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import (
    STRENGTH_ADJUSTED_FIELDS,
    compute_league_averages_from_per_game,
    compute_possession_splits,
    run_iterative_adjustment_with_hca,
)

by_name = {t["team_name"]: t for t in teams}
league = compute_league_averages_from_per_game(teams)
splits = {t["team_name"]: compute_possession_splits(t) for t in teams}
result = run_iterative_adjustment_with_hca(
    teams, by_name, STRENGTH_ADJUSTED_FIELDS, league, splits,
)
print(result.hca_per_field["3p"]["hca_off"])
```

### same_school {#same_school}

`same_school(a: 'str', b: 'str') -> 'bool'`

Whether two team-name spellings denote the same school.

Exact match, or both names inside one `team_name_equivalents` class.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `a` | `str` |  | One team-name spelling. |
| `b` | `str` |  | The other team-name spelling. |

**Returns**

`True` if the two names refer to the same school.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_data_quality import same_school
same_school("New Orleans", "LSU New Orleans")  # True
same_school("Miami (FL)", "Miami (OH)")        # False
```

### season_slice {#season_slice}

`season_slice(players: 'pl.DataFrame', stints: 'pl.DataFrame') -> 'pl.DataFrame'`

Cut a POOLED fit's players back to ONE season's participants and exposure.

A fit pooled over seasons `t-2 .. t` rates everyone who played in any of
them, and its `off_poss` / `def_poss` are three-season sums. A per-season
published asset must be neither: this inner-joins the players to the
exposure of `stints` (the TARGET season alone), which in one step drops
everyone who did not play that season and restores the season's own
possession counts. Every other column -- coefficients and standard errors,
which are properties of the pooled fit -- is carried through untouched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `players` | `DataFrame` |  | First element of a `solve_rapm_league` return. |
| `stints` | `DataFrame` |  | The TARGET season's stints only (`aggregate_stints` output), NOT the pooled design. |

**Returns**

`players` restricted to the season's participants, with `off_poss` / `def_poss` recomputed from `stints`.

| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `orapm` | double | Offensive regularized adjusted plus-minus: points contributed per 100 possessions on offense, adjusted for the other 9 players on the floor. |
| `drapm` | double | Defensive regularized adjusted plus-minus: points prevented per 100 possessions on defense (higher is better defense), adjusted for the other 9 players on the floor. |
| `rapm_net` | double | Net RAPM (orapm plus drapm): overall point contribution per 100 possessions. Verified against live data: rapm_net == orapm + drapm exactly. |
| `off_poss` | integer | Offensive possessions the player was on court for; the regression weight behind orapm. |
| `def_poss` | integer | Defensive possessions the player was on court for; the regression weight behind drapm. |
| `orapm_se` | double | Posterior standard error of orapm (per 100 possessions) from the ridge fit. |
| `drapm_se` | double | Posterior standard error of drapm (per 100 possessions) from the ridge fit. |
| `rapm_net_se` | double | Posterior standard error of rapm_net (per 100 possessions) from the ridge fit. |
| `orapm_se_sampling` | double | Frequentist (sandwich) standard error of the shrunk orapm estimate -- its refit-to-refit repeatability, not an interval for the true effect (see orapm_se); near 0 for a player the ridge holds at zero. |
| `drapm_se_sampling` | double | Frequentist (sandwich) standard error of the shrunk drapm estimate -- its refit-to-refit repeatability, not an interval for the true effect (see drapm_se); near 0 for a player the ridge holds at zero. |
| `rapm_net_se_sampling` | double | Frequentist (sandwich) standard error of the shrunk rapm_net estimate -- its refit-to-refit repeatability, not an interval for the true effect (see rapm_net_se); near 0 for a player the ridge holds at zero. |

**Example**

```python
pooled = stack_seasons({2023: s23, 2024: s24}, 2024, 0.5)
players, info = solve_rapm_league(pooled)
season_2024 = season_slice(players, s24)
```

### select_contains {#select_contains}

`select_contains(root: 'Tag', selector: 'str', text: 'str') -> 'list[Tag]'`

JSoup `root.select(sel + ":contains(text)")`: candidates whose full

text (own + every descendant's) case-insensitively CONTAINS `text` as
a plain substring -- **not** a regex (Task 5e.2 addition; see the module
docstring's "Critical divergence" note).

JSoup's `:contains()` is documented case-insensitive substring
containment; soupsieve's `:-soup-contains()` (the non-deprecated
spelling of its `:contains()`) is case-SENSITIVE, with no
case-insensitive variant of its own. Reproducing JSoup's actual
semantics therefore needs this helper rather than `:-soup-contains()`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `root` | `Tag` |  | The element to search within. |
| `selector` | `str` |  | A plain (soupsieve-legal) CSS selector for the structural part of the match (everything before `:contains`). |
| `text` | `str` |  | The plain substring each candidate's collapsed text must case-insensitively contain. |

**Returns**

Every `selector` match whose `jsoup_text` case-insensitively contains `text`, in document order.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_html import parse_html, select_contains
soup = parse_html("<td>game date:</td><td>Location:</td>")
select_contains(soup, "td", "Game Date:")  # [<td>game date:</td>]
```

### select_matching {#select_matching}

`select_matching(root: 'Tag', selector: 'str', regex: 'str') -> 'list[Tag]'`

JSoup `root.select(sel + ":matches(regex)")`: candidates whose full

text (own + every descendant's) matches `regex`.

Soupsieve has no `:matches()` pseudo-class equivalent, so this runs the
plain structural `selector` first, then filters by `re.search`
over each candidate's `jsoup_text` (own text plus descendants',
matching JSoup's `:matches()` semantics -- as opposed to
`select_matching_own`'s own-text-only `:matchesOwn()`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `root` | `Tag` |  | The element to search within. |
| `selector` | `str` |  | A plain (soupsieve-legal) CSS selector. |
| `regex` | `str` |  | The pattern each candidate's collapsed text must `re.search`-match. |

**Returns**

Every `selector` match whose `jsoup_text` contains a `regex` match, in document order.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_html import parse_html, select_matching
soup = parse_html("<div><p>Home Team</p><p>Away Team</p></div>")
select_matching(soup, "p", r"^Home")  # [<p>Home Team</p>]
```

### select_matching_own {#select_matching_own}

`select_matching_own(root: 'Tag', selector: 'str', regex: 'str') -> 'list[Tag]'`

JSoup `root.select(sel + ":matchesOwn(regex)")`: candidates whose

OWN text only (excluding descendant elements' text) matches `regex`.

JSoup's `Element.ownText()` walks only the element's direct
`TextNode` children, not text nested inside child elements -- the
same distinction bs4 draws between a tag's direct
`bs4.NavigableString` children and its full `.get_text()`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `root` | `Tag` |  | The element to search within. |
| `selector` | `str` |  | A plain (soupsieve-legal) CSS selector. |
| `regex` | `str` |  | The pattern each candidate's own (whitespace-collapsed) text must `re.search`-match. |

**Returns**

Every `selector` match whose own text contains a `regex` match, in document order.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_html import parse_html, select_matching_own
soup = parse_html('<div class="card-header">Coach <b>Info</b></div>')
select_matching_own(soup, "div.card-header", r"^Coach")
# [<div class="card-header">Coach <b>Info</b></div>]
```

### shot_js_to_html {#shot_js_to_html}

`shot_js_to_html(js: 'str') -> 'list[Tag]'`

Converts client-side `addShot(...)` JS calls into parseable

`circle.shot` HTML, for pages where the shot map is built on the fly
rather than baked into the initial HTML (`ShotEventParser
.shot_js_to_html`, `:266-283`). See the module docstring's "Scala
idiom decision" note -- the Scala's `builders`/`browser` parameters
are dropped here since the Scala body never actually uses them.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `js` | `str` |  | The concatenated `<script>` text containing one or more `addShot(x, y, ..., 'title', ...)` calls, one per line. |

**Returns**

The `circle.shot` elements reconstructed from every matching line (non-matching lines, e.g. the `addShot` function definition line itself, are silently skipped).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_shot_parser import shot_js_to_html
js = "addShot(27.0, 77.0, 392, false, 1, 'title text', 'class', false);"
circles = shot_js_to_html(js)
```

### shot_value {#shot_value}

`shot_value(event_str: 'str') -> 'int'`

Classify a play-by-play event string as a 3, a 2, or an assist

(`ShotEnrichmentUtils.shot_value`, `PlayByPlayUtils.scala:534-542`).

Ported as an ordered first-match cascade, exactly mirroring the Scala
`match` arm order (assist is tested first, so an assist string never
falls through to a shot classifier).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event_str` | `str` |  | The raw play-by-play event string. |

**Returns**

`0` for an assist, `3` for any 3-pointer (made or missed), `2` for any 2-pointer (made or missed), or `-1` for anything else (rebounds, turnovers, unparseable, ...).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_pbp_glue import shot_value
shot_value("18:28:00,0-0,Eric Ayala, 3pt jumpshot made")   # 3
shot_value("18:28:00,0-0,Kyle Guy, assist")                # 0
shot_value("04:28:0,52-59,Team, rebound deadballdeadball")  # -1
```

### sides_from_box {#sides_from_box}

`sides_from_box(box_lineup: "'LineupEvent'") -> "'tuple[Optional[str], Optional[str]]'"`

`(home_team, away_team)` implied by an already-resolved box lineup.

Lets the play-by-play and shot parsers reuse the pairing `get_box_lineup`
established, so a page that names only ONE team (a non-D-I opponent has no
header on the box page) does not fail again further down the chain.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_lineup` | `LineupEvent` |  | A box lineup already resolved by `~sportsdataverse.mbb.mbb_ncaa_boxscore_parser.get_box_lineup`. |

**Returns**

`(home_team, away_team)`, or `(None, None)` for a NEUTRAL-site game: there `location_type` comes from the date rather than title order, so it carries no ordering information and guessing would pick the wrong roster table.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup
from sportsdataverse.mbb.mbb_ncaa_models import TeamId
from sportsdataverse.mbb.mbb_ncaa_stints import sides_from_box

box = get_box_lineup("individual_stats.html", stats_html, TeamId("Kansas"), format_version=1)
home, away = sides_from_box(box)

# Feeding the hints back into a later parse

from sportsdataverse.mbb.mbb_ncaa_stints import parse_team_name
parse_team_name(["Kansas"], TeamId("Kansas"), box.team.year, home, away)
```

### solve_rapm_league {#solve_rapm_league}

`solve_rapm_league(stints: 'pl.DataFrame', *, ridge_lambda: 'float' = 1000.0, prior_mean: "'pl.DataFrame | None'" = None, compute_se: 'bool' = True, return_as_pandas: 'bool' = False) -> "'tuple[pl.DataFrame | pd.DataFrame, dict[str, float]]'"`

Weighted sparse joint O/D ridge over matchup stints.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stints` | `DataFrame` |  | Output of `aggregate_stints`. Two OPTIONAL columns are honoured when present, both no-ops when absent: `fit_weight` (Float64) multiplies the row's possession weight — the decay weight of a stacked multi-season design, where older seasons enter the same fit at `decay ** (t - s)`; and `y_offset` (Float64) is subtracted from the row's per-100 rate before centering, which is how a pooled design removes the season-to-season scoring-level drift that would otherwise credit a player for the era he played in. `off_poss` / `def_poss` always report real possessions, never weighted ones. |
| `ridge_lambda` | `float` | `1000.0` | L2 penalty on the possession-weighted normal equations. Because rows are weighted by possessions, the same `lambda` is exactly equivalent to a per-possession ridge with that penalty. |
| `prior_mean` | `DataFrame \| None` | `None` | Optional frame with `player_id` (Utf8), `orapm_prior` and `drapm_prior`: the ridge then shrinks each player toward THAT point instead of zero (`beta ~ N(b0, sigma^2 / lambda)`), which is the box-score-plus-minus-prior form of RAPM. Solved by re-centering — `delta = beta - b0` against the offset target `y - X b0` — so `lambda` keeps its meaning and the posterior covariance `sigma^2 (X'WX + lambda I)^-1` (hence every `*_se` column) is unchanged. Players absent from the frame keep a zero prior; a frame that overlaps NO rated player raises rather than silently degrading to the flat ridge. |
| `compute_se` | `bool` | `True` | Also return the posterior standard errors (module docstring, "Uncertainty"). Costs one dense Cholesky inverse of the `(2P+1)`-square penalised Gram matrix. One such matrix is ~0.8 GB for a D-I season (P ~ 5,000, dim ~ 10,001); the symmetrise step holds two of them at once, so the peak is ~1.6 GB and a few seconds. Switch off for pooled designs past ~20k columns. |
| `return_as_pandas` | `bool` | `False` | Return the players frame as pandas. |

**Returns**

`(players, info)`. `players` has one row per rated player: `player_id`, `orapm`, `drapm` (positive = good defense), `rapm_net = orapm + drapm` (all per 100 possessions), `off_poss`, `def_poss`, the posterior standard errors `orapm_se`, `drapm_se`, `rapm_net_se` and the sampling (sandwich) standard errors `*_se_sampling` (both net SEs include the O/D covariance; all six are null when `compute_se=False`; see the module docstring for which to publish). `info` carries `intercept` (possession-weighted league mean pts/100), `hca` (half the home-minus-away offensive gap), `ridge_lambda`, `n_stints`, `n_poss`, `prior_mean_mad` (mean |b0| over the player block — 0.0 without a prior, so a prior that silently failed to attach is visible in the output rather than only in the intent), the lsqr diagnostics and, with SEs, `sigma2` (weighted residual variance on the per-100 scale = `1e4 x` the per-possession points variance, ~1.3e4 in D-I), `df_eff` (trace of the ridge hat matrix), `hca_se` and `solve_max_abs_dev` (max |lsqr - exact| coefficient deviation -- the exact solve comes free with the factorisation and doubles as the solver-tolerance check).

**Raises**

| Exception | Description |
|---|---|
| `RuntimeError` | Lsqr stopped without converging, or the penalised Gram matrix is not positive definite (`ridge_lambda` must be > 0 for the SEs). |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_rapm_league import (
    aggregate_stints, solve_rapm_league,
)

players, info = solve_rapm_league(aggregate_stints(resolved))
players.sort("rapm_net", descending=True).head()

# A 95% interval per player

players.with_columns(
    (pl.col("rapm_net") - 2 * pl.col("rapm_net_se")).alias("lo95"),
    (pl.col("rapm_net") + 2 * pl.col("rapm_net_se")).alias("hi95"),
)
```

### split_half_se_check {#split_half_se_check}

`split_half_se_check(resolved: 'pl.DataFrame', *, ridge_lambda: 'float' = 1000.0, refit: "'Any | None'" = None) -> "'tuple[pl.DataFrame, dict[str, float]]'"`

Odd-vs-even-game refit: are the standard errors on the right scale?

Splits `resolved` by the parity of `contest_id` (deterministic and
roster-neutral -- the halves differ by sampling noise, not by the
mid-season development or transfer drift a date split would add), fits
`solve_rapm_league` on each half, and asks per player rated in both
whether the two estimates agree to within `2 * sqrt(se_A^2 + se_B^2)`.

Two readings, both reported. Under the SAMPLING SE this is the textbook
calibration test -- ~95% coverage means `sigma2` and the inverse are
right. Under the POSTERIOR SE (the published one) coverage is expected
ABOVE nominal (~100% on real seasons): a credible interval for the truth
is wider than the estimator's repeatability, so its coverage here is a
one-sided guard (a bug that shrinks the SEs drops it), not a two-sided
calibration.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `resolved` | `DataFrame` |  | Output of `resolve_possessions` WITH its `contest_id` column (integer-like). |
| `ridge_lambda` | `float` | `1000.0` | Passed to both half fits. |
| `refit` | `Any \| None` | `None` | Optional `(half_resolved, half_index) -> (players, info)` replacing the default single-season flat-ridge half fit. Pass it when the PUBLISHED estimator is not that fit -- a pooled or prior-shrunk producer must check the standard errors it actually publishes, and it must split every pooled season by the same parity, or the two halves share possessions and the coverage is inflated by construction. |

**Returns**

`(per_player, summary)`. `per_player` has one row per player rated in both halves: `player_id`, `poss` (both halves' exposure) and, for each of `orapm` / `drapm` / `rapm_net`, `<c>_a`, `<c>_b`, `<c>_z` / `<c>_covered` (posterior SE units, |z| <= 2) and `<c>_z_sampling` / `<c>_covered_sampling` (sampling SE units). `summary` carries `n_players`, `n_games_a`, `n_games_b` and, per `<c>`, `coverage_<c>`, `coverage_sampling_<c>`, `z_sd_<c>` (a posterior/sampling ratio proxy) and `z_sd_sampling_<c>` (~1 when calibrated). Feed `per_player` to `possession_deciles` for the coverage by playing time.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `contest_id` is missing or not integer-like. |

**Example**

```python
per_player, summary = split_half_se_check(resolved)
print(summary["coverage_sampling_rapm_net"])   # ~0.95
possession_deciles(per_player)
```

### stack_seasons {#stack_seasons}

`stack_seasons(per_season: "'dict[int, pl.DataFrame]'", target: 'int', decay: 'float') -> 'pl.DataFrame'`

Pool several seasons' stints into ONE decayed-weight design for `target`.

Each season `s` contributes its stints with `fit_weight = decay ** (target
- s)` and `y_offset` equal to its own per-100 scoring level minus the
target season's, so a pooled fit can neither credit a player for his era's
pace nor weight a three-year-old possession like last night's. The stint
frames must be keyed by a CROSS-SEASON person id (see
`mbb_ncaa_rapm_input.build_person_keys`) or the pool rates the same human
as several different players.

**Leakage boundary.** A season strictly after `target` is refused, not
down-weighted: the estimate published for season `t` must be computable
from what was known at the end of season `t`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `per_season` | `dict[int, DataFrame]` |  | `{season: stints}`, each the output of `aggregate_stints`. Seasons `> target` raise. |
| `target` | `int` |  | The season the pooled fit is FOR. |
| `decay` | `float` |  | Per-season weight multiplier in `(0, 1]`. `1.0` pools unweighted; the producers use 0.5 (mbb) / 0.75 (wbb). |

**Returns**

One vertically concatenated stint frame carrying the extra `fit_weight` / `y_offset` columns `solve_rapm_league` reads.

| col_name | type | description |
|---|---|---|
| `off_ids` | list | Sorted player ids of the five offensive players in the stint (a list), from the resolved slot ids. |
| `def_ids` | list | Sorted player ids of the five defensive players in the stint (a list). |
| `off_team` | character | Team on offense during the stint. |
| `def_team` | character | Team on defense during the stint. |
| `is_home_offense` | logical | True when the home team is on offense, the home-court term of the RAPM design matrix. |
| `n_poss` | integer | Possessions the five-on-five unit played together in this offensive direction. |
| `pts` | integer | Points scored. |
| `fit_weight` | double | Season weight in the pooled fit: decay ** (target - season). |
| `y_offset` | double | The row's season per-100 scoring level minus the target season's, so the pool does not credit era pace. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | A season is after `target`, a season's stints carry no possessions, `decay` is outside `(0, 1]`, or `target` itself is absent from `per_season`. |

**Example**

```python
pooled = stack_seasons({2022: s22, 2023: s23, 2024: s24}, 2024, 0.5)
players, info = solve_rapm_league(pooled)
```

### start_time_from_period {#start_time_from_period}

`start_time_from_period(period: 'int', is_women_game: 'bool') -> 'float'`

The game-clock time (minutes elapsed) a period starts at

(`ExtractorUtils.scala:272-281`).

Women's games play four 10-minute quarters then 5-minute overtimes; men's
games play two 20-minute halves then 5-minute overtimes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `period` | `int` |  | The 1-indexed period number (1/2 = halves for men, 1-4 = quarters for women, 5+ = overtimes for both). |
| `is_women_game` | `bool` |  | Whether to use the women's (quarters) or men's (halves) period schedule. |

**Returns**

The game-clock minute the period begins at.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stints import start_time_from_period
start_time_from_period(2, is_women_game=False)  # 20.0 (men's 2nd half)
start_time_from_period(1, is_women_game=True)  # 0.0 (women's 1st quarter)
start_time_from_period(6, is_women_game=False)  # 45.0 (men's 2nd OT)
```

### stint_exposure {#stint_exposure}

`stint_exposure(stints: 'pl.DataFrame') -> 'pl.DataFrame'`

`player_id` -> `off_poss` / `def_poss` from a stint frame's REAL possessions.

`fit_weight` is deliberately ignored: exposure is a count of possessions
PLAYED, never a count of possessions the fit chose to believe. Callers that
need a possession total (an SPM prior's exposure shrink, a possession bin)
use this so the count means the same thing everywhere.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stints` | `DataFrame` |  | `aggregate_stints` output (one season, or a pooled design). |

**Returns**

One row per player: `player_id`, `off_poss`, `def_poss` (Int64).

| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `off_poss` | integer | Offensive possessions the player was on court for; the regression weight behind orapm. |
| `def_poss` | integer | Defensive possessions the player was on court for; the regression weight behind drapm. |

**Example**

```python
stint_exposure(stints).sort("off_poss", descending=True).head()
```

### sum_event_stats {#sum_event_stats}

`sum_event_stats(lhs: 'LineupEventStats', rhs: 'LineupEventStats') -> 'LineupEventStats'`

Field-wise add two :class:`~sportsdataverse.mbb.mbb_ncaa_models

.LineupEventStats` (`protected def sum_event_stats`, `LineupUtils.scala
:1534-1622`, debug-only -- the Scala's own docstring says "just used for
debug"). The Scala builds this via `shapeless.Generic` field-zipping;
this port is an explicit field-by-field call since Python has no
equivalent generic-programming machinery.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lhs` | `LineupEventStats` |  | The left-hand stat tree. |
| `rhs` | `LineupEventStats` |  | The right-hand stat tree. |

**Returns**

A new `~sportsdataverse.mbb.mbb_ncaa_models.LineupEventStats` with every field summed (see the module's private `_sum_*` helpers for the `Optional`/nested-field summing rules).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import sum_event_stats
from sportsdataverse.mbb.mbb_ncaa_models import LineupEventStats

sum_event_stats(LineupEventStats.empty(), LineupEventStats.empty()).num_events
```

### sum_shot_infos {#sum_shot_infos}

`sum_shot_infos(shot_infos: 'list[PlayerShotInfo]') -> 'Optional[PlayerShotInfo]'`

Field-wise sum a list of :class:`~sportsdataverse.mbb.mbb_ncaa_models

.PlayerShotInfo`\ s (`sum_shot_infos`, `LineupUtils.scala:1625-1655`,
debug-only).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shot_infos` | `list[PlayerShotInfo]` |  | The list to combine, in order. |

**Returns**

`None` if `shot_infos` is empty; the single element if there's exactly one; otherwise a left-fold of pairwise field-wise sums (`reduceOption`).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import sum_shot_infos
from sportsdataverse.mbb.mbb_ncaa_models import PlayerShotInfo

sum_shot_infos([PlayerShotInfo(ast_3pm=(1, 0, 0, 0, 0)), PlayerShotInfo(ast_3pm=(0, 1, 0, 0, 0))])
```

### td_at {#td_at}

`td_at(row: 'Tag', n: 'int') -> 'Optional[Tag]'`

JSoup `row >?> element("td:eq(n)")`: the `n`-th `<td>` child.

Soupsieve has no `:eq()` positional pseudo-class (unlike JSoup), so
this is a plain 0-indexed lookup into `row.find_all("td")`, guarded
against an out-of-range index (JSoup's `>?>` returns `None` rather
than raising when the selector matches nothing).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `row` | `Tag` |  | The row (or other container) element to search. |
| `n` | `int` |  | The 0-indexed `<td>` position. |

**Returns**

The `n`-th `<td>` descendant, or `None` if `row` has fewer than `n + 1` of them.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_html import parse_html, td_at
soup = parse_html("<tr><td>A</td><td>B</td></tr>")
row = soup.find("tr")
td_at(row, 1).get_text()  # "B"
td_at(row, 5)  # None
```
