---
title: "NBA — additional Python functions — Highlights"
sidebar_label: "Highlights"
sidebar_position: 1
description: "NBA — additional Python functions — Highlights — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Highlights

### bref_players_stats {#bref_players_stats}

`bref_players_stats(season: 'Optional[int]' = None, table: 'str' = 'per_game', league: 'str' = 'nba', *, return_as_pandas: 'bool' = False, proxy: 'Any' = None, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame'`

Player season statistics for an entire league season.

Port of hoopR's `bref_players_stats()` (NBA) and wehoop's
`bref_wnba_player_stats()` (WNBA). One row per player, with columns named
by Basketball-Reference `data-stat` keys.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season in 4-digit ending-year format (`2024` = 2023-24). The WNBA season is a plain calendar year. Defaults to the current season. |
| `table` | `str` | `'per_game'` | Which stat table. NBA accepts `per_game` (default), `totals`, `advanced`, `per_minute` (per 36) and `per_poss` (per 100 possessions); WNBA accepts `per_game`, `totals` and `advanced`. |
| `league` | `str` | `'nba'` | `"nba"` (default) or `"wnba"`. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `proxy` | `Any` | `None` | Proxy configuration in the `requests` `proxies=` shape. |

**Returns**

One row per player: `ranker`, `player`, `age`, `team`, `pos`, `g`, `gs` plus the box columns scaled to `table` (the `advanced` table adds `per`, `ts_pct`, `usg_pct`, `ws`, `bpm`, `vorp` …), and echoed `season` / `table` / `league` columns. A zero-row frame when the page carries no player table.

| col_name | type | description |
|---|---|---|
| `ranker` | Float64 | Row rank. |
| `player` | String | Player name. |
| `age` | Float64 | Player age (in years). |
| `team` | String | Team-side label or team identifier. |
| `pos` | String | Position. |
| `g` | Float64 | Games played. |
| `gs` | Float64 | Games started. |
| `mp_per_g` | Float64 | Minutes (per_game table) / `mp` total (totals table). |
| `fg_per_g` | Float64 | Field goals made per game over the season (Float64, one decimal, e.g. 10.8); null where the page leaves the cell blank. |
| `fga_per_g` | Float64 | Field goal attempts per game over the season (Float64, one decimal, e.g. 22.8); null where the page leaves the cell blank. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_per_g` | Float64 | Three-point field goals made per game over the season (Float64, one decimal, e.g. 4.0); null where the page leaves the cell blank. |
| `fg3a_per_g` | Float64 | Three-point field goal attempts per game over the season (Float64, one decimal, e.g. 10.8); null where the page leaves the cell blank. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `fg2_per_g` | Float64 | Two-point field goals made per game over the season (Float64, one decimal, e.g. 6.9); null where the page leaves the cell blank. |
| `fg2a_per_g` | Float64 | Two-point field goal attempts per game over the season (Float64, one decimal, e.g. 11.9); null where the page leaves the cell blank. |
| `fg2_pct` | Float64 | Two-point field goal percentage as a 0-1 proportion (e.g. 0.575); null where the page leaves the cell blank. |
| `efg_pct` | Float64 | Effective field goal percentage as a 0-1 proportion (e.g. 0.563), crediting made threes at 1.5 field goals: (FG + 0.5 * 3P) / FGA; null where the page leaves the cell blank. |
| `ft_per_g` | Float64 | Free throws made per game over the season (Float64, one decimal, e.g. 7.9); null where the page leaves the cell blank. |
| `fta_per_g` | Float64 | Free throw attempts per game over the season (Float64, one decimal, e.g. 10.1); null where the page leaves the cell blank. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `orb_per_g` | Float64 | Offensive rebounds per game over the season (Float64, one decimal, e.g. 0.6); null where the page leaves the cell blank. |
| `drb_per_g` | Float64 | Defensive rebounds per game over the season (Float64, one decimal, e.g. 7.1); null where the page leaves the cell blank. |
| `trb_per_g` | Float64 | Total rebounds per game over the season (Float64, one decimal, e.g. 7.7); null where the page leaves the cell blank. |
| `ast_per_g` | Float64 | Assists per game over the season (Float64, one decimal, e.g. 8.3); null where the page leaves the cell blank. |
| `stl_per_g` | Float64 | Steals per game over the season (Float64, one decimal, e.g. 1.6); null where the page leaves the cell blank. |
| `blk_per_g` | Float64 | Blocks per game over the season (Float64, one decimal, e.g. 0.8); null where the page leaves the cell blank. |
| `tov_per_g` | Float64 | Turnovers per game over the season (Float64, one decimal, e.g. 4.0); null where the page leaves the cell blank. |
| `pf_per_g` | Float64 | Personal fouls per game over the season (Float64, one decimal, e.g. 2.4); null where the page leaves the cell blank. |
| `pts_per_g` | Float64 | Points (scaled to the chosen `table`). |
| `awards` | String | Comma-separated Basketball-Reference award codes for the season (e.g. 'MVP-4,CPOY-8,AS,NBA1'): voting finishes as CODE-place, AS for All-Star, NBA1/NBA2/NBA3 for the All-NBA team; empty string when the player has none. |
| `season` | Int64 | Season year. |
| `table` | String | Stat table requested through the table argument, echoed on every row ('per_game' in sampled data). |
| `league` | String | League slug. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | When `league` is not `"nba"`/`"wnba"`, or `table` is not one of that league's stat tables. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.bref import bref_players_stats

df = bref_players_stats(season=2024)
print(df.shape)

# Advanced metrics, and the WNBA page

adv = bref_players_stats(season=2024, table="advanced")
wnba = bref_players_stats(season=2024, league="wnba")

# Pipeline next step (one line)

adv.filter(pl.col("vorp") > 3.0).sort("vorp", descending=True).head()
```

### bref_standings {#bref_standings}

`bref_standings(season: 'Optional[int]' = None, league: 'str' = 'nba', *, return_as_pandas: 'bool' = False, proxy: 'Any' = None, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame'`

Conference standings for a season, both conferences stacked.

Port of hoopR's `bref_standings()` (NBA) and wehoop's
`bref_wnba_standings()` (WNBA).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season in 4-digit ending-year format. Defaults to the current season. |
| `league` | `str` | `'nba'` | `"nba"` (default) or `"wnba"`. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `proxy` | `Any` | `None` | Proxy configuration in the `requests` `proxies=` shape. |

**Returns**

One row per team: `conference` (`"E"` / `"W"` for **both** leagues -- wehoop emits `"Eastern"`/`"Western"`), `team` (playoff `*` marker stripped), `playoffs` (bool, from that marker), `wins`, `losses`, `win_loss_pct`, `gb`, `pts_per_g`, `opp_pts_per_g`, `srs`, plus echoed `season` / `league`. Zero rows when neither conference table is present.

| col_name | type | description |
|---|---|---|
| `team` | String | Team-side label or team identifier. |
| `wins` | Float64 | Total wins. |
| `losses` | Float64 | Total losses. |
| `win_loss_pct` | Float64 | Win-loss percentage. |
| `gb` | String | Games behind the conference leader. |
| `pts_per_g` | Float64 | Points (scaled to the chosen `table`). |
| `opp_pts_per_g` | Float64 | Opponent points per game. |
| `srs` | Float64 | Simple Rating System (point margin + SOS). |
| `conference` | String | Conference name. |
| `playoffs` | Boolean | `TRUE` if the team made the playoffs (`*` marker). |
| `season` | Int64 | Season year. |
| `league` | String | League slug. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | When `league` is not `"nba"`/`"wnba"`. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.bref import bref_standings

df = bref_standings(season=2024)
print(df.shape)

# The WNBA page

wnba = bref_standings(season=2024, league="wnba")

# Pipeline next step (one line)

df.filter(pl.col("playoffs") == True).sort("srs", descending=True).head()
```

### bref_teams_stats {#bref_teams_stats}

`bref_teams_stats(season: 'Optional[int]' = None, table: 'str' = 'per_game', league: 'str' = 'nba', *, return_as_pandas: 'bool' = False, proxy: 'Any' = None, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame'`

Team season statistics from the league season page.

Port of hoopR's `bref_teams_stats()` (NBA) and wehoop's
`bref_wnba_team_stats()` (WNBA). Every team table lives on the one season
page and all but the first are comment-hidden, which is why the table `id`
selection in `_bref_table` matters here.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season in 4-digit ending-year format (`2024` = 2023-24). Defaults to the current season. |
| `table` | `str` | `'per_game'` | NBA accepts `per_game` (default), `totals`, `per_poss`, `advanced` and `opponent` (opponent per-game); WNBA accepts the same set minus `opponent`. |
| `league` | `str` | `'nba'` | `"nba"` (default) or `"wnba"`. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `proxy` | `Any` | `None` | Proxy configuration in the `requests` `proxies=` shape. |

**Returns**

One row per team: `ranker`, `team`, `g`, `mp` and the box categories scaled to `table`, plus echoed `season` / `table` / `league`. The WNBA path drops the `League Average` footer row, as wehoop does. A zero-row frame when the table id is absent.

| col_name | type | description |
|---|---|---|
| `ranker` | Float64 | Row rank. |
| `team` | String | Team-side label or team identifier. |
| `g` | Float64 | Games played. |
| `mp` | Float64 | Minutes played. |
| `fg` | Float64 | Field goals made by the team, scaled to the requested table (per game in the sampled per_game table, e.g. 43.5). |
| `fga` | Float64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3` | Float64 | Three-point field goals made by the team, scaled to the requested table (per game in the sampled per_game table, e.g. 14.2). |
| `fg3a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `fg2` | Float64 | Two-point field goals made by the team, scaled to the requested table (per game in the sampled per_game table, e.g. 29.4). |
| `fg2a` | Float64 | Two-point field goal attempts by the team, scaled to the requested table (per game in the sampled per_game table, e.g. 52.0). |
| `fg2_pct` | Float64 | Team two-point field goal percentage as a 0-1 proportion (e.g. 0.565). |
| `ft` | Float64 | Free throws made by the team, scaled to the requested table (per game in the sampled per_game table, e.g. 20.8). |
| `fta` | Float64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `orb` | Float64 | Offensive rebounds by the team, scaled to the requested table (per game in the sampled per_game table, e.g. 9.8). |
| `drb` | Float64 | Defensive rebounds by the team, scaled to the requested table (per game in the sampled per_game table, e.g. 34.2). |
| `trb` | Float64 | Career total rebounds. |
| `ast` | Float64 | Assists. |
| `stl` | Float64 | Steals. |
| `blk` | Float64 | Blocks. |
| `tov` | Float64 | Turnovers. |
| `pf` | Float64 | Personal fouls. |
| `pts` | Float64 | Points scored. |
| `season` | Int64 | Season year. |
| `table` | String | Stat table requested through the table argument, echoed on every row ('per_game' in sampled data). |
| `league` | String | League slug. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | When `league` or `table` is not one of the supported values for that league. |

**Example**

```python
from sportsdataverse.nba.bref import bref_teams_stats

df = bref_teams_stats(season=2024)
print(df.shape)

# Opponent per-game, and pandas output

opp = bref_teams_stats(season=2024, table="opponent")
df_pd = bref_teams_stats(season=2024, return_as_pandas=True)

# Pipeline next step (one line)

df.sort("pts_per_g", descending=True).head()
```

### compile_nba_season {#compile_nba_season}

`compile_nba_season(season: 'int', season_type: 'str' = 'Regular Season', *, resume: 'bool' = True, cache_dir: 'Optional[str]' = None, delay_s: 'float' = 0.6, lineup_source: 'str' = 'auto', proxy_provider: 'Optional[Callable[[], Optional[str]]]' = None, raw_store_dir: 'RawStoreDir' = None, raw_store_readonly: 'Optional[bool]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Compile a full season's possession stint matrix (cached + resumable + throttled).

Discovers game ids, dedupes, then per game loads the cached parquet if present
(`resume`), else fetches via `_fetch_possessions`, caches it, and sleeps
`delay_s` (throttle; only on live fetches). A game that errors or returns no
possessions is logged and skipped (best-effort — a per-game failure never
raises; see `Raises` for the game_date integrity error). The assembled
frame is tagged with a `season` column.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season END year (e.g. 2024 for 2023-24). |
| `season_type` | `str` | `'Regular Season'` | `"Regular Season"` (default) or `"Playoffs"`. |
| `resume` | `bool` | `True` | Reuse per-game cached parquet when present. |
| `cache_dir` | `Optional[str]` | `None` | Cache root; defaults to `SDV_PY_NBA_CACHE_DIR` or `~/.sdv_py_nba_cache/possessions`. |
| `delay_s` | `float` | `0.6` | Seconds to sleep after each live fetch (rate-limit throttle). |
| `lineup_source` | `str` | `'auto'` | Which on-court lineup producer to use — `"auto"` (default; tries rotation then falls back to pbp), `"rotation"` (gamerotation endpoint only), or `"pbp"` (pbp-derived, no gamerotation fetch — useful when the gamerotation endpoint is throttled or unavailable). |
| `proxy_provider` | `Optional[Callable[[], Optional[str]]]` | `None` | Optional zero-arg callable returning a proxy URL (or `None`). **Called once for game discovery, then once per game** (`N + 1` calls for an `N`-game season), so a rotating pool spreads a season's fetches across many exit IPs rather than hammering `stats.nba.com` from one address. `stats.nba.com` rejects or hangs on datacenter/cloud IPs, so an unattended host (CI, a droplet) MUST supply one — a proxied request is judged on the proxy's exit IP, which is what makes such a host viable at all. Note discovery is proxied too: an unproxied index call returns no rows there, compiling the season to zero games without an error. Any `() -> str \| None` works; a round-robin pool's `.next` matches the signature directly:: compile_nba_season(2024, proxy_provider=round_robin.next) |
| `raw_store_dir` | `RawStoreDir` | `None` | Explicit raw JSON store root forwarded to every per-game fetch — a single path, or a per-endpoint mapping (`"*"` default key) so payload families can live in independent trees. `None` -> env vars (per-endpoint `SDV_PY_NBA_RAW_JSON_DIR_{ENDPOINT}`, then the generic `SDV_PY_NBA_RAW_JSON_DIR`); `""` force-disables. Same spirit as `cache_dir`'s arg-over-env precedence. |
| `raw_store_readonly` | `Optional[bool]` | `None` | If `True`, per-game fetches are fully offline — the store is the only source, and a game with no capture raises `~sportsdataverse.errors.RawStoreMissError`, which this loop's per-game handler logs and skips (so an uncaptured game is visibly skipped instead of silently completed from the live API). `None` defers to `SDV_PY_NBA_RAW_JSON_READONLY`. |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |

**Returns**

The season possession frame (+ `season` and `game_date` cols). Empty typed frame if no games.

No returns table is published for this function: no capture: it reads stats.nba.com (HTTP 403 to the datacenter IP the docs are built on), and compiling a season from the raw store takes far longer than the capture allows.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If any compiled possession's `game_id` is missing (or has a null) `game_date` in the season index — surfaced as an explicit error rather than silently emitting null dates. |

**Example**

```python
from sportsdataverse.nba.nba_season_compile import compile_nba_season

poss = compile_nba_season(2024)
print(poss.shape)          # (n_possessions, n_cols)
print(poss["season"][0])   # 2024

# Resume a partially completed run and return as pandas

poss_pd = compile_nba_season(2024, resume=True, return_as_pandas=True)
print(type(poss_pd))       # <class 'pandas.core.frame.DataFrame'>

# Compile Playoffs with a custom cache directory

poss = compile_nba_season(
    2024,
    season_type="Playoffs",
    cache_dir="/tmp/nba_cache",
)
```

### espn_nba_player_stats {#espn_nba_player_stats}

`espn_nba_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull an NBA athlete's ESPN **season** stat line as one wide row.

See `sportsdataverse.wbb.espn_wbb_player_stats` for full
documentation of the wide return shape, the `{category}_{stat}` stat
columns, the athlete / team metadata blocks, and the `season_type` /
`total` parameters. For the richer multi-category web-v3 payload use
`sportsdataverse.nba.espn_nba_player_stats_v3`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `athlete_id` | `int` |  | ESPN NBA athlete identifier (e.g. `1966` for LeBron James). |
| `season` | `int` |  | Season year, used in the core-v2 path. |
| `season_type` | `str` | `'regular'` | `"regular"` (type 2) or `"postseason"` (type 3). |
| `total` | `bool` | `False` | Forward-compat totals passthrough. |
| `raw` | `bool` | `False` | If True, returns the raw core-v2 statistics JSON dict. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; else polars. |

**Returns**

A single-row wide DataFrame (polars by default). When `raw=True` returns the raw statistics JSON `dict`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season year. |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `total` | Boolean | Total. |
| `athlete_id` | Int64 | Unique athlete identifier (ESPN). |
| `athlete_uid` | String | ESPN athlete UID (universal identifier). |
| `athlete_guid` | String | ESPN athlete GUID. |
| `athlete_type` | String | Athlete type / class. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `full_name` | String | Player's full name. |
| `display_name` | String | Display name. |
| `short_name` | String | Short display name. |
| `weight` | Float64 | Player weight in pounds. |
| `display_weight` | String | Player weight in display format (e.g. '180 lbs'). |
| `height` | Float64 | Player height (string e.g. '6-2' or inches). |
| `display_height` | String | Player height in display format (e.g. '6-2'). |
| `age` | Int64 | Player age (in years). |
| `date_of_birth` | String | Date of birth (YYYY-MM-DD). |
| `jersey` | String | Jersey number worn by the player. |
| `slug` | String | URL-safe identifier. |
| `active` | Boolean | TRUE if the row represents an active record (player / team / season). |
| `position_id` | Int64 | Unique position identifier. |
| `position_name` | String | Listed roster position ('Guard', 'Forward', 'Center'). |
| `position_display_name` | String | Position display name. |
| `position_abbreviation` | String | Position abbreviation ('G' / 'F' / 'C'). |
| `college_name` | String | College / pre-draft team. |
| `status_id` | Int64 | Status identifier. |
| `status_name` | String | Status label. |
| `defensive_blocks` | Float64 | Short for blocked shot, number of times when a defensive player legally deflects a field goal attempt from an offensive player. |
| `defensive_defensive_rebounds` | Float64 | The number of times when the defense obtains the possession of the ball after a missed shot by the offense. |
| `defensive_steals` | Float64 | The number of times a defensive player forced a turnover by intercepting or deflecting a pass or a dribble of an offensive player. |
| `defensive_def_rebound_rate` | Float64 | The percentage of missed shots that a team rebounds defensively. Rebound Rate = (Defensive Rebounds x Team Minutes) divided by (Player Minutes x (Team Defensive Rebounds + Opponent Defensive Rebounds)). |
| `defensive_avg_defensive_rebounds` | Float64 | The average defensive rebounds per game. |
| `defensive_avg_blocks` | Float64 | The average blocks per game. |
| `defensive_avg_steals` | Float64 | The average steals per game. |
| `defensive_avg48_defensive_rebounds` | Float64 | Player's average defensive rebounds per 48 minutes played. |
| `defensive_avg48_blocks` | Float64 | Player's average blocked shots per 48 minutes played. |
| `defensive_avg48_steals` | Float64 | Player's average steals per 48 minutes played. |
| `defensive_drpm` | Float64 | Defensive Real Plus-Minus. |
| `general_disqualifications` | Float64 | The number of times a player reached the foul limit. |
| `general_flagrant_fouls` | Float64 | The number of fouls that the officials thought were unnecessary or excessive. |
| `general_fouls` | Float64 | The number of times a player had illegal contact with the opponent. |
| `general_per` | Float64 | A numerical value for each of a player's accomplishments per-minute and is pace-adjusted for the team they play on. The league average in PER to 15.00 every season. |
| `general_rebound_rate` | Float64 | The percentage of missed shots that a team rebounds. Rebound Rate = (Rebounds x Team Minutes) divided by (Player Minutes x (Team Rebounds + Opponent Rebounds)). |
| `general_ejections` | Float64 | The number of times a player or coach is removed from the game as a result of a serious offense. |
| `general_technical_fouls` | Float64 | The number of times an player or coach was called for a technical foul (unsportsmanlike conduct or violations). |
| `general_rebounds` | Float64 | The total number of rebounds (offensive and defensive). |
| `general_vorp` | Float64 | Value Over Replacement Player. |
| `general_warp` | Float64 | Wins Above Replacement Player. |
| `general_rpm` | Float64 | Real Plus-Minus. |
| `general_minutes` | Float64 | The total number of minutes played. |
| `general_avg_minutes` | Float64 | The average number of minutes per game. |
| `general_nba_rating` | Float64 | General nba rating. |
| `general_plus_minus` | Float64 | A player's estimated on-court impact on team performance measured in point differential per 100 possessions. |
| `general_avg_rebounds` | Float64 | The average rebounds per game. |
| `general_avg_fouls` | Float64 | The average fouls committed per game. |
| `general_avg_flagrant_fouls` | Float64 | The average number of flagrant fouls per game. |
| `general_avg_technical_fouls` | Float64 | The average number of technical fouls per game. |
| `general_avg_ejections` | Float64 | The average ejections per game. |
| `general_avg_disqualifications` | Float64 | The average number of disqualifications per game. |
| `general_assist_turnover_ratio` | Float64 | The average number of assists a player or team records per turnover. |
| `general_steal_foul_ratio` | Float64 | The average number of steals a player or team records per foul committed. |
| `general_block_foul_ratio` | Float64 | The average number of blocks a player or record per foul committed. |
| `general_avg_team_rebounds` | Float64 | The average number of rebounds for a team per game. |
| `general_total_rebounds` | Float64 | The total number of rebounds for a team or player. |
| `general_total_technical_fouls` | Float64 | The total number of technical fouls for a team or player. |
| `general_team_assist_turnover_ratio` | Float64 | The number of assists per turnover for a team. |
| `general_steal_turnover_ratio` | Float64 | The number of steals per turnover. |
| `general_avg48_rebounds` | Float64 | Player's average total rebounds (offensive + defensive) per 48 minutes played. |
| `general_avg48_fouls` | Float64 | Player's average personal fouls committed per 48 minutes played. |
| `general_avg48_flagrant_fouls` | Float64 | Player's average flagrant fouls assessed per 48 minutes played. |
| `general_avg48_technical_fouls` | Float64 | Player's average technical fouls assessed per 48 minutes played. |
| `general_avg48_ejections` | Float64 | Player's average ejections per 48 minutes played. |
| `general_avg48_disqualifications` | Float64 | Player's average disqualifications (fouling out) per 48 minutes played. |
| `general_r40` | Float64 | Rebounds Per 40 Minutes. |
| `general_games_played` | Float64 | Games Played. |
| `general_games_started` | Float64 | The number of games started by an athlete. |
| `general_double_double` | Float64 | The number of times double digit values were accumulated in 2 of the following categories: points, rebounds, assists, steals, and blocked shots. |
| `general_triple_double` | Float64 | The number of times double digit values were accumulated in 3 of the following categories: points, rebounds, assists, steals, and blocked shots. |
| `offensive_assists` | Float64 | The number of times a player who passes the ball to a teammate in a way that leads to a score by field goal, meaning that he or she was "assisting" in the basket. There is some judgment involved in deciding whether a passer should be credited with an assist. |
| `offensive_effective_fg_pct` | Float64 | Offensive effective field goals percentage (0-1 decimal). |
| `offensive_field_goals` | Float64 | Field Goal makes and attempts. |
| `offensive_field_goals_attempted` | Float64 | The number of times a 2pt field goal was attempted. |
| `offensive_field_goals_made` | Float64 | The number of times a 2pt field goal was made. |
| `offensive_field_goal_pct` | Float64 | The ratio of field goals made to field goals attempted: FGM / FGA. |
| `offensive_free_throws` | Float64 | Free Throw makes and attempts. |
| `offensive_free_throw_pct` | Float64 | The ratio of free throws made to free throws attempted: FTM / FTA. |
| `offensive_free_throws_attempted` | Float64 | The number of times a free throw was attempted. |
| `offensive_free_throws_made` | Float64 | The number of times a free throw was made. |
| `offensive_offensive_rebounds` | Float64 | The number of times when the offense obtains the possession of the ball after a missed shot. |
| `offensive_points` | Float64 | The number of points scored. |
| `offensive_turnovers` | Float64 | The number of times a player loses possession to the other team. |
| `offensive_three_point_pct` | Float64 | The ratio of 3pt field goals made to 3pt field goals attempted: 3PM / 3PA. |
| `offensive_three_point_field_goals_attempted` | Float64 | The number of times a 3pt field goal was attempted. |
| `offensive_three_point_field_goals_made` | Float64 | The number of times a 3pt field goal was made. |
| `offensive_true_shooting_pct` | Float64 | What a team's shooting percentage would be if we accounted for free throws and 3-pointers. True Shooting Percentage = (Total points x 50) divided by ((FGA + (FTA x 0.44)). |
| `offensive_total_turnovers` | Float64 | The number of turnovers plus team turnovers for the team. |
| `offensive_assist_ratio` | Float64 | The percentage of a team's possessions that ends in an assist. Assist Ratio = (Assists x 100) divided by ((FGA + (FTA x 0.44) + Assists + Turnovers). |
| `offensive_points_in_paint` | Float64 | The amount of points scored in the area known as "the Paint"(the rectangle between the foul line and the baseline). |
| `offensive_off_rebound_rate` | Float64 | The percentage of missed shots that a team rebounds offensively. Offensive Rebound Rate = (Offensive Rebounds x Team Minutes) divided by (Player Minutes x (Team Offensive Rebounds + Opponent Defensive Rebounds)). |
| `offensive_turnover_ratio` | Float64 | The percentage of a team's possessions that end in a turnover. Turnover Ratio = (Turnover x 100) divided by ((FGA + (FTA x 0.44) + Assists + Turnovers). |
| `offensive_brick_index` | Float64 | How many points a player costs his team with his shooting compared with the league average on a per-40-minute basis. ((52.8 - TS%) x (FGA + (FTA x 0.44))) / (Min/40) . |
| `offensive_usage_rate` | Float64 | the number of possessions a player uses per 40 minutes. Usage Rate = ((FGA + (FT Att. x 0.44) + (Ast x 0.33) + TO) x 40 x League Pace) divided by (Minutes x Team Pace). |
| `offensive_avg_field_goals_made` | Float64 | The average field goals made per game. |
| `offensive_avg_field_goals_attempted` | Float64 | The average field goals attempted per game. |
| `offensive_avg_three_point_field_goals_made` | Float64 | The average three point field goals made per game. |
| `offensive_avg_three_point_field_goals_attempted` | Float64 | The average three point field goals attempted per game. |
| `offensive_avg_free_throws_made` | Float64 | The average free throw shots made per game. |
| `offensive_avg_free_throws_attempted` | Float64 | The average free throw shots attempted per game. |
| `offensive_avg_points` | Float64 | The average number of points scored per game. |
| `offensive_avg_offensive_rebounds` | Float64 | The average offensive rebounds per game. |
| `offensive_avg_assists` | Float64 | The average assists per game. |
| `offensive_avg_turnovers` | Float64 | The average turnovers committed per game. |
| `offensive_offensive_rebound_pct` | Float64 | The percentage of the number of times they obtain the possession of the ball after a missed shot. |
| `offensive_estimated_possessions` | Float64 | An estimation of the number of possessions for a team or player. |
| `offensive_avg_estimated_possessions` | Float64 | The average number of estimated possessions per game for a team or player. |
| `offensive_points_per_estimated_possessions` | Float64 | The number of points per estimated possession for a team or player. |
| `offensive_avg_team_turnovers` | Float64 | The average number of turnovers for a team per game. |
| `offensive_avg_total_turnovers` | Float64 | The average number of total turnovers for a team per game. |
| `offensive_three_point_field_goal_pct` | Float64 | The ratio of 3pt field goals made to 3pt field goals attempted: 3PM / 3PA. |
| `offensive_two_point_field_goals_made` | Float64 | The number of 2-point field goals made for a team or player. |
| `offensive_two_point_field_goals_attempted` | Float64 | The number of 2-point field goals attempted for a team or player. |
| `offensive_avg_two_point_field_goals_made` | Float64 | The number of 2-point field goals made per game for a team or player. |
| `offensive_avg_two_point_field_goals_attempted` | Float64 | The number of 2-point field goals attempted per game for a team or player. |
| `offensive_two_point_field_goal_pct` | Float64 | The percentage of 2-points fields goals made by a team or player. |
| `offensive_shooting_efficiency` | Float64 | The efficiency with which a team or player shoots the basketball. |
| `offensive_scoring_efficiency` | Float64 | The efficiency with which a team or player scores the basketball. |
| `offensive_avg48_field_goals_made` | Float64 | Player's average field goals made per 48 minutes played. |
| `offensive_avg48_field_goals_attempted` | Float64 | Player's average field goal attempts per 48 minutes played. |
| `offensive_avg48_three_point_field_goals_made` | Float64 | Player's average three-point field goals made per 48 minutes played. |
| `offensive_avg48_three_point_field_goals_attempted` | Float64 | Player's average three-point field goal attempts per 48 minutes played. |
| `offensive_avg48_free_throws_made` | Float64 | Player's average free throws made per 48 minutes played. |
| `offensive_avg48_free_throws_attempted` | Float64 | Player's average free throw attempts per 48 minutes played. |
| `offensive_avg48_points` | Float64 | Player's average points scored per 48 minutes played. |
| `offensive_avg48_offensive_rebounds` | Float64 | Player's average offensive rebounds per 48 minutes played. |
| `offensive_avg48_assists` | Float64 | Player's average assists per 48 minutes played. |
| `offensive_avg48_turnovers` | Float64 | Player's average turnovers committed per 48 minutes played. |
| `offensive_p40` | Float64 | Points Per 40 Minutes. |
| `offensive_a40` | Float64 | Assists Per 40 Minutes. |
| `offensive_orpm` | Float64 | Offensive Real Plus-Minus. |
| `team_id` | Int64 | Unique team identifier. |
| `team_uid` | String | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_guid` | String | ESPN team GUID. |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_location` | String | Team city or location string. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | String | Full team display name. |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `team_alternate_color` | String | Team alternate color (hex without leading '#'). |
| `team_is_active` | Boolean | TRUE if the team is currently active. |
| `team_logo_href` | String |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `season_type` is not `"regular"`/`"postseason"`. |
| `sportsdataverse.errors.NoDataError` | ESPN returned 404. |
| `sportsdataverse.errors.AssetFetchError` | The statistics, athlete or team fetch failed (403, 429, 5xx, a dead connection); only a 404 on the athlete or team node leaves those columns null. |

**Example**

```python
from sportsdataverse.nba import espn_nba_player_stats
df = espn_nba_player_stats(athlete_id=1966, season=2023)
df.select(["full_name", "team_display_name", "offensive_points"])
```

### espn_nba_schedule {#espn_nba_schedule}

`espn_nba_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nba_schedule - look up the NBA schedule for a given date from ESPN

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dates` | `int` | `None` | Used to define different seasons. 2002 is the earliest available season. |
| `season_type` | `int` | `None` | season type, 1 for pre-season, 2 for regular season, 3 for post-season, 4 for all-star, 5 for off-season |
| `limit` | `int` | `500` | number of records to return, default: 500. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing schedule dates for the requested season. A day with no games returns a zero-row frame carrying the documented columns.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Id. |
| `uid` | String | ESPN UID string. |
| `date` | String | Date in YYYY-MM-DD format. |
| `attendance` | Int64 | Reported attendance. |
| `time_valid` | Boolean | Time valid. |
| `neutral_site` | Boolean | Neutral site. |
| `conference_competition` | Boolean | Conference competition. |
| `play_by_play_available` | Boolean |  |
| `recent` | Boolean | Recent. |
| `start_date` | String | Start date (YYYY-MM-DD). |
| `broadcast` | String | Broadcast information string. |
| `highlights` | Int64 |  |
| `notes_type` | String | Notes type. |
| `notes_headline` | String | Notes headline. |
| `broadcast_market` | String | Broadcast market label (e.g. 'national', 'home'). |
| `broadcast_name` | String | Broadcast name. |
| `type_id` | Int64 | Type identifier (numeric). |
| `type_abbreviation` | String | Type abbreviation. |
| `venue_id` | Int64 | Unique venue identifier. |
| `venue_full_name` | String | Venue full name. |
| `venue_address_city` | String | Venue address city. |
| `venue_address_state` | String | Venue address state / region. |
| `venue_indoor` | Boolean | TRUE if the venue is indoors. |
| `status_clock` | Float64 | Status clock. |
| `status_display_clock` | String | Status display clock. |
| `status_period` | Int64 | Status period. |
| `status_type_id` | Int64 | Unique identifier for status type. |
| `status_type_name` | String | Status type name. |
| `status_type_state` | String | Status type state. |
| `status_type_completed` | Boolean | Status type completed. |
| `status_type_description` | String | Status type description. |
| `status_type_detail` | String | Status type detail. |
| `status_type_short_detail` | String | Status type short detail. |
| `format_regulation_periods` | Int64 | Format regulation periods. |
| `home_id` | Int64 | Unique identifier for home. |
| `home_uid` | String | Home team's uid. |
| `home_location` | String | Home team's location. |
| `home_name` | String | Home name. |
| `home_abbreviation` | String | Home team's abbreviation. |
| `home_display_name` | String | Home display name. |
| `home_short_display_name` | String | Home short display name. |
| `home_color` | String | Color code (hex) for home. |
| `home_alternate_color` | String | Color code (hex) for home alternate. |
| `home_is_active` | Boolean | Home team's is active. |
| `home_venue_id` | Int64 | Unique identifier for home venue. |
| `home_logo` | String | Home team logo URL. |
| `home_score` | String | Home team score at the time of the play. |
| `home_winner` | Boolean | Home team's winner. |
| `home_linescores` | List | Period-by-period point totals for the home team, stored as a list of integer scores. |
| `home_records` | String | Win-loss record strings for the home team across relevant splits (e.g., overall, home/away, conference). |
| `away_id` | Int64 | Unique identifier for away. |
| `away_uid` | String | Away team's uid. |
| `away_location` | String | Away team's location. |
| `away_name` | String | Away name. |
| `away_abbreviation` | String | Away team's abbreviation. |
| `away_display_name` | String | Away display name. |
| `away_short_display_name` | String | Away short display name. |
| `away_color` | String | Color code (hex) for away. |
| `away_alternate_color` | String | Color code (hex) for away alternate. |
| `away_is_active` | Boolean | Away team's is active. |
| `away_venue_id` | Int64 | Unique identifier for away venue. |
| `away_logo` | String | Away team logo URL. |
| `away_score` | String | Away team score at the time of the play. |
| `away_winner` | Boolean | Away team's winner. |
| `away_linescores` | List | Period-by-period point totals for the away team, stored as a list of integer scores. |
| `away_records` | String | Win-loss record strings for the away team across relevant splits (e.g., overall, home/away, conference). |
| `game_id` | Int64 | Unique game identifier. |
| `season` | Int64 | Season year. |
| `season_type` | Int64 | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `home_logo_dark` | String |  |
| `away_logo_dark` | String |  |

**Example**

```python
from sportsdataverse.nba import espn_nba_schedule
slate = espn_nba_schedule()
print(slate.shape)

# Pull a specific date

jan2 = espn_nba_schedule(dates=20230102, season_type=2)

# Pipeline next step (extract finals only)

import polars as pl
finals = espn_nba_schedule(dates=20230102).filter(
    pl.col("status_type_completed") == True
)
```

### espn_nba_teams {#espn_nba_teams}

`espn_nba_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nba_teams - look up NBA teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe of the NBA teams, one row per team, with the columns in the returns table. A payload without teams returns a zero-row frame with those columns. The result is not cached: every call fetches (through the package's opt-in TTL response cache, `sportsdataverse.cache`, when it is on).

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_alternate_color` | String | Team alternate color (hex without leading '#'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `team_display_name` | String | Full team display name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_is_active` | Boolean | TRUE if the team is currently active. |
| `team_is_all_star` | Boolean | TRUE if the row represents an All-Star team. |
| `team_location` | String | Team city or location string. |
| `team_logos` | Int64 |  |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_nickname` | String | Team nickname. |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_uid` | String | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | ESPN answered 404 for the teams resource. |
| `AssetFetchError` | The fetch failed (403, 429, 5xx, a dropped connection); never returned as an empty frame. |

**Example**

```python
from sportsdataverse.nba import espn_nba_teams
teams = espn_nba_teams()
print(teams.shape)

# Pandas round-trip

teams_pd = espn_nba_teams(return_as_pandas=True)
teams_pd.head()

# Pipeline next step (build a team_id to abbreviation map)

teams = espn_nba_teams()
abbr_map = dict(zip(teams["team_id"], teams["team_abbreviation"]))
```

### most_recent_nba_season {#most_recent_nba_season}

`most_recent_nba_season()`

Return the most recent NBA season year based on today's date.

The NBA season crosses calendar years -- a season started in October of
year Y is reported as season Y+1. If today is in October or later, this
returns next calendar year; otherwise it returns the current calendar year.

**Returns**

The most recent NBA season year (e.g. 2024 for the 2023-24 season).

**Example**

```python
from sportsdataverse.nba import most_recent_nba_season
year = most_recent_nba_season()
print(year)

# Combine with the loaders for a "current season" pull

from sportsdataverse.nba import load_nba_schedule, most_recent_nba_season
sched = load_nba_schedule(seasons=[most_recent_nba_season()])
```
