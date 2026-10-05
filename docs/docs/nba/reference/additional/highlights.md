---
title: "NBA — additional Python functions — Highlights"
sidebar_label: "Highlights"
sidebar_position: 3
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
| `ranker` | double | Row rank. |
| `player` | character | Player name. |
| `age` | double | Player age (in years). |
| `team` | character | Team-side label or team identifier. |
| `pos` | character | Position. |
| `g` | double | Games played. |
| `gs` | double | Games started. |
| `mp_per_g` | double | Minutes (per_game table) / `mp` total (totals table). |
| `fg_per_g` | double | Field goals made per game over the season (Float64, one decimal, e.g. 10.8); null where the page leaves the cell blank. |
| `fga_per_g` | double | Field goal attempts per game over the season (Float64, one decimal, e.g. 22.8); null where the page leaves the cell blank. |
| `fg_pct` | double | Field goal percentage (0-1). |
| `fg3_per_g` | double | Three-point field goals made per game over the season (Float64, one decimal, e.g. 4.0); null where the page leaves the cell blank. |
| `fg3a_per_g` | double | Three-point field goal attempts per game over the season (Float64, one decimal, e.g. 10.8); null where the page leaves the cell blank. |
| `fg3_pct` | double | Three-point field goal percentage (0-1). |
| `fg2_per_g` | double | Two-point field goals made per game over the season (Float64, one decimal, e.g. 6.9); null where the page leaves the cell blank. |
| `fg2a_per_g` | double | Two-point field goal attempts per game over the season (Float64, one decimal, e.g. 11.9); null where the page leaves the cell blank. |
| `fg2_pct` | double | Two-point field goal percentage as a 0-1 proportion (e.g. 0.575); null where the page leaves the cell blank. |
| `efg_pct` | double | Effective field goal percentage as a 0-1 proportion (e.g. 0.563), crediting made threes at 1.5 field goals: (FG + 0.5 * 3P) / FGA; null where the page leaves the cell blank. |
| `ft_per_g` | double | Free throws made per game over the season (Float64, one decimal, e.g. 7.9); null where the page leaves the cell blank. |
| `fta_per_g` | double | Free throw attempts per game over the season (Float64, one decimal, e.g. 10.1); null where the page leaves the cell blank. |
| `ft_pct` | double | Free throw percentage (0-1). |
| `orb_per_g` | double | Offensive rebounds per game over the season (Float64, one decimal, e.g. 0.6); null where the page leaves the cell blank. |
| `drb_per_g` | double | Defensive rebounds per game over the season (Float64, one decimal, e.g. 7.1); null where the page leaves the cell blank. |
| `trb_per_g` | double | Total rebounds per game over the season (Float64, one decimal, e.g. 7.7); null where the page leaves the cell blank. |
| `ast_per_g` | double | Assists per game over the season (Float64, one decimal, e.g. 8.3); null where the page leaves the cell blank. |
| `stl_per_g` | double | Steals per game over the season (Float64, one decimal, e.g. 1.6); null where the page leaves the cell blank. |
| `blk_per_g` | double | Blocks per game over the season (Float64, one decimal, e.g. 0.8); null where the page leaves the cell blank. |
| `tov_per_g` | double | Turnovers per game over the season (Float64, one decimal, e.g. 4.0); null where the page leaves the cell blank. |
| `pf_per_g` | double | Personal fouls per game over the season (Float64, one decimal, e.g. 2.4); null where the page leaves the cell blank. |
| `pts_per_g` | double | Points (scaled to the chosen `table`). |
| `awards` | character | Comma-separated Basketball-Reference award codes for the season (e.g. 'MVP-4,CPOY-8,AS,NBA1'): voting finishes as CODE-place, AS for All-Star, NBA1/NBA2/NBA3 for the All-NBA team; empty string when the player has none. |
| `season` | integer | Season year. |
| `table` | character | Stat table requested through the table argument, echoed on every row ('per_game' in sampled data). |
| `league` | character | League slug. |

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
| `team` | character | Team-side label or team identifier. |
| `wins` | double | Total wins. |
| `losses` | double | Total losses. |
| `win_loss_pct` | double | Win-loss percentage. |
| `gb` | character | Games behind the conference leader. |
| `pts_per_g` | double | Points (scaled to the chosen `table`). |
| `opp_pts_per_g` | double | Opponent points per game. |
| `srs` | double | Simple Rating System (point margin + SOS). |
| `conference` | character | Conference name. |
| `playoffs` | logical | `TRUE` if the team made the playoffs (`*` marker). |
| `season` | integer | Season year. |
| `league` | character | League slug. |

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
selection in bref_table` matters here.

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
| `ranker` | double | Row rank. |
| `team` | character | Team-side label or team identifier. |
| `g` | double | Games played. |
| `mp` | double | Minutes played. |
| `fg` | double | Field goals made by the team, scaled to the requested table (per game in the sampled per_game table, e.g. 43.5). |
| `fga` | double | Field goal attempts. |
| `fg_pct` | double | Field goal percentage (0-1). |
| `fg3` | double | Three-point field goals made by the team, scaled to the requested table (per game in the sampled per_game table, e.g. 14.2). |
| `fg3a` | double | Three-point field goal attempts. |
| `fg3_pct` | double | Three-point field goal percentage (0-1). |
| `fg2` | double | Two-point field goals made by the team, scaled to the requested table (per game in the sampled per_game table, e.g. 29.4). |
| `fg2a` | double | Two-point field goal attempts by the team, scaled to the requested table (per game in the sampled per_game table, e.g. 52.0). |
| `fg2_pct` | double | Team two-point field goal percentage as a 0-1 proportion (e.g. 0.565). |
| `ft` | double | Free throws made by the team, scaled to the requested table (per game in the sampled per_game table, e.g. 20.8). |
| `fta` | double | Free throw attempts. |
| `ft_pct` | double | Free throw percentage (0-1). |
| `orb` | double | Offensive rebounds by the team, scaled to the requested table (per game in the sampled per_game table, e.g. 9.8). |
| `drb` | double | Defensive rebounds by the team, scaled to the requested table (per game in the sampled per_game table, e.g. 34.2). |
| `trb` | double | Career total rebounds. |
| `ast` | double | Assists. |
| `stl` | double | Steals. |
| `blk` | double | Blocks. |
| `tov` | double | Turnovers. |
| `pf` | double | Personal fouls. |
| `pts` | double | Points scored. |
| `season` | integer | Season year. |
| `table` | character | Stat table requested through the table argument, echoed on every row ('per_game' in sampled data). |
| `league` | character | League slug. |

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
(`resume`), else fetches via fetch_possessions`, caches it, and sleeps
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
| `season` | integer | Season year. |
| `season_type` | character | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `total` | logical | Total. |
| `athlete_id` | integer | Unique athlete identifier (ESPN). |
| `athlete_uid` | character | ESPN athlete UID (universal identifier). |
| `athlete_guid` | character | ESPN athlete GUID. |
| `athlete_type` | character | Athlete type / class. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `full_name` | character | Player's full name. |
| `display_name` | character | Display name. |
| `short_name` | character | Short display name. |
| `weight` | double | Player weight in pounds. |
| `display_weight` | character | Player weight in display format (e.g. '180 lbs'). |
| `height` | double | Player height (string e.g. '6-2' or inches). |
| `display_height` | character | Player height in display format (e.g. '6-2'). |
| `age` | integer | Player age (in years). |
| `date_of_birth` | character | Date of birth (YYYY-MM-DD). |
| `jersey` | character | Jersey number worn by the player. |
| `slug` | character | URL-safe identifier. |
| `active` | logical | TRUE if the row represents an active record (player / team / season). |
| `position_id` | integer | Unique position identifier. |
| `position_name` | character | Listed roster position ('Guard', 'Forward', 'Center'). |
| `position_display_name` | character | Position display name. |
| `position_abbreviation` | character | Position abbreviation ('G' / 'F' / 'C'). |
| `college_name` | character | College / pre-draft team. |
| `status_id` | integer | Status identifier. |
| `status_name` | character | Status label. |
| `defensive_blocks` | double | Short for blocked shot, number of times when a defensive player legally deflects a field goal attempt from an offensive player. |
| `defensive_defensive_rebounds` | double | The number of times when the defense obtains the possession of the ball after a missed shot by the offense. |
| `defensive_steals` | double | The number of times a defensive player forced a turnover by intercepting or deflecting a pass or a dribble of an offensive player. |
| `defensive_def_rebound_rate` | double | The percentage of missed shots that a team rebounds defensively. Rebound Rate = (Defensive Rebounds x Team Minutes) divided by (Player Minutes x (Team Defensive Rebounds + Opponent Defensive Rebounds)). |
| `defensive_avg_defensive_rebounds` | double | The average defensive rebounds per game. |
| `defensive_avg_blocks` | double | The average blocks per game. |
| `defensive_avg_steals` | double | The average steals per game. |
| `defensive_avg48_defensive_rebounds` | double | Player's average defensive rebounds per 48 minutes played. |
| `defensive_avg48_blocks` | double | Player's average blocked shots per 48 minutes played. |
| `defensive_avg48_steals` | double | Player's average steals per 48 minutes played. |
| `defensive_drpm` | double | Defensive Real Plus-Minus. |
| `general_disqualifications` | double | The number of times a player reached the foul limit. |
| `general_flagrant_fouls` | double | The number of fouls that the officials thought were unnecessary or excessive. |
| `general_fouls` | double | The number of times a player had illegal contact with the opponent. |
| `general_per` | double | A numerical value for each of a player's accomplishments per-minute and is pace-adjusted for the team they play on. The league average in PER to 15.00 every season. |
| `general_rebound_rate` | double | The percentage of missed shots that a team rebounds. Rebound Rate = (Rebounds x Team Minutes) divided by (Player Minutes x (Team Rebounds + Opponent Rebounds)). |
| `general_ejections` | double | The number of times a player or coach is removed from the game as a result of a serious offense. |
| `general_technical_fouls` | double | The number of times an player or coach was called for a technical foul (unsportsmanlike conduct or violations). |
| `general_rebounds` | double | The total number of rebounds (offensive and defensive). |
| `general_vorp` | double | Value Over Replacement Player. |
| `general_warp` | double | Wins Above Replacement Player. |
| `general_rpm` | double | Real Plus-Minus. |
| `general_minutes` | double | The total number of minutes played. |
| `general_avg_minutes` | double | The average number of minutes per game. |
| `general_nba_rating` | double | General nba rating. |
| `general_plus_minus` | double | A player's estimated on-court impact on team performance measured in point differential per 100 possessions. |
| `general_avg_rebounds` | double | The average rebounds per game. |
| `general_avg_fouls` | double | The average fouls committed per game. |
| `general_avg_flagrant_fouls` | double | The average number of flagrant fouls per game. |
| `general_avg_technical_fouls` | double | The average number of technical fouls per game. |
| `general_avg_ejections` | double | The average ejections per game. |
| `general_avg_disqualifications` | double | The average number of disqualifications per game. |
| `general_assist_turnover_ratio` | double | The average number of assists a player or team records per turnover. |
| `general_steal_foul_ratio` | double | The average number of steals a player or team records per foul committed. |
| `general_block_foul_ratio` | double | The average number of blocks a player or record per foul committed. |
| `general_avg_team_rebounds` | double | The average number of rebounds for a team per game. |
| `general_total_rebounds` | double | The total number of rebounds for a team or player. |
| `general_total_technical_fouls` | double | The total number of technical fouls for a team or player. |
| `general_team_assist_turnover_ratio` | double | The number of assists per turnover for a team. |
| `general_steal_turnover_ratio` | double | The number of steals per turnover. |
| `general_avg48_rebounds` | double | Player's average total rebounds (offensive + defensive) per 48 minutes played. |
| `general_avg48_fouls` | double | Player's average personal fouls committed per 48 minutes played. |
| `general_avg48_flagrant_fouls` | double | Player's average flagrant fouls assessed per 48 minutes played. |
| `general_avg48_technical_fouls` | double | Player's average technical fouls assessed per 48 minutes played. |
| `general_avg48_ejections` | double | Player's average ejections per 48 minutes played. |
| `general_avg48_disqualifications` | double | Player's average disqualifications (fouling out) per 48 minutes played. |
| `general_r40` | double | Rebounds Per 40 Minutes. |
| `general_games_played` | double | Games Played. |
| `general_games_started` | double | The number of games started by an athlete. |
| `general_double_double` | double | The number of times double digit values were accumulated in 2 of the following categories: points, rebounds, assists, steals, and blocked shots. |
| `general_triple_double` | double | The number of times double digit values were accumulated in 3 of the following categories: points, rebounds, assists, steals, and blocked shots. |
| `offensive_assists` | double | The number of times a player who passes the ball to a teammate in a way that leads to a score by field goal, meaning that he or she was "assisting" in the basket. There is some judgment involved in deciding whether a passer should be credited with an assist. |
| `offensive_effective_fg_pct` | double | Offensive effective field goals percentage (0-1 decimal). |
| `offensive_field_goals` | double | Field Goal makes and attempts. |
| `offensive_field_goals_attempted` | double | The number of times a 2pt field goal was attempted. |
| `offensive_field_goals_made` | double | The number of times a 2pt field goal was made. |
| `offensive_field_goal_pct` | double | The ratio of field goals made to field goals attempted: FGM / FGA. |
| `offensive_free_throws` | double | Free Throw makes and attempts. |
| `offensive_free_throw_pct` | double | The ratio of free throws made to free throws attempted: FTM / FTA. |
| `offensive_free_throws_attempted` | double | The number of times a free throw was attempted. |
| `offensive_free_throws_made` | double | The number of times a free throw was made. |
| `offensive_offensive_rebounds` | double | The number of times when the offense obtains the possession of the ball after a missed shot. |
| `offensive_points` | double | The number of points scored. |
| `offensive_turnovers` | double | The number of times a player loses possession to the other team. |
| `offensive_three_point_pct` | double | The ratio of 3pt field goals made to 3pt field goals attempted: 3PM / 3PA. |
| `offensive_three_point_field_goals_attempted` | double | The number of times a 3pt field goal was attempted. |
| `offensive_three_point_field_goals_made` | double | The number of times a 3pt field goal was made. |
| `offensive_true_shooting_pct` | double | What a team's shooting percentage would be if we accounted for free throws and 3-pointers. True Shooting Percentage = (Total points x 50) divided by ((FGA + (FTA x 0.44)). |
| `offensive_total_turnovers` | double | The number of turnovers plus team turnovers for the team. |
| `offensive_assist_ratio` | double | The percentage of a team's possessions that ends in an assist. Assist Ratio = (Assists x 100) divided by ((FGA + (FTA x 0.44) + Assists + Turnovers). |
| `offensive_points_in_paint` | double | The amount of points scored in the area known as "the Paint"(the rectangle between the foul line and the baseline). |
| `offensive_off_rebound_rate` | double | The percentage of missed shots that a team rebounds offensively. Offensive Rebound Rate = (Offensive Rebounds x Team Minutes) divided by (Player Minutes x (Team Offensive Rebounds + Opponent Defensive Rebounds)). |
| `offensive_turnover_ratio` | double | The percentage of a team's possessions that end in a turnover. Turnover Ratio = (Turnover x 100) divided by ((FGA + (FTA x 0.44) + Assists + Turnovers). |
| `offensive_brick_index` | double | How many points a player costs his team with his shooting compared with the league average on a per-40-minute basis. ((52.8 - TS%) x (FGA + (FTA x 0.44))) / (Min/40) . |
| `offensive_usage_rate` | double | the number of possessions a player uses per 40 minutes. Usage Rate = ((FGA + (FT Att. x 0.44) + (Ast x 0.33) + TO) x 40 x League Pace) divided by (Minutes x Team Pace). |
| `offensive_avg_field_goals_made` | double | The average field goals made per game. |
| `offensive_avg_field_goals_attempted` | double | The average field goals attempted per game. |
| `offensive_avg_three_point_field_goals_made` | double | The average three point field goals made per game. |
| `offensive_avg_three_point_field_goals_attempted` | double | The average three point field goals attempted per game. |
| `offensive_avg_free_throws_made` | double | The average free throw shots made per game. |
| `offensive_avg_free_throws_attempted` | double | The average free throw shots attempted per game. |
| `offensive_avg_points` | double | The average number of points scored per game. |
| `offensive_avg_offensive_rebounds` | double | The average offensive rebounds per game. |
| `offensive_avg_assists` | double | The average assists per game. |
| `offensive_avg_turnovers` | double | The average turnovers committed per game. |
| `offensive_offensive_rebound_pct` | double | The percentage of the number of times they obtain the possession of the ball after a missed shot. |
| `offensive_estimated_possessions` | double | An estimation of the number of possessions for a team or player. |
| `offensive_avg_estimated_possessions` | double | The average number of estimated possessions per game for a team or player. |
| `offensive_points_per_estimated_possessions` | double | The number of points per estimated possession for a team or player. |
| `offensive_avg_team_turnovers` | double | The average number of turnovers for a team per game. |
| `offensive_avg_total_turnovers` | double | The average number of total turnovers for a team per game. |
| `offensive_three_point_field_goal_pct` | double | The ratio of 3pt field goals made to 3pt field goals attempted: 3PM / 3PA. |
| `offensive_two_point_field_goals_made` | double | The number of 2-point field goals made for a team or player. |
| `offensive_two_point_field_goals_attempted` | double | The number of 2-point field goals attempted for a team or player. |
| `offensive_avg_two_point_field_goals_made` | double | The number of 2-point field goals made per game for a team or player. |
| `offensive_avg_two_point_field_goals_attempted` | double | The number of 2-point field goals attempted per game for a team or player. |
| `offensive_two_point_field_goal_pct` | double | The percentage of 2-points fields goals made by a team or player. |
| `offensive_shooting_efficiency` | double | The efficiency with which a team or player shoots the basketball. |
| `offensive_scoring_efficiency` | double | The efficiency with which a team or player scores the basketball. |
| `offensive_avg48_field_goals_made` | double | Player's average field goals made per 48 minutes played. |
| `offensive_avg48_field_goals_attempted` | double | Player's average field goal attempts per 48 minutes played. |
| `offensive_avg48_three_point_field_goals_made` | double | Player's average three-point field goals made per 48 minutes played. |
| `offensive_avg48_three_point_field_goals_attempted` | double | Player's average three-point field goal attempts per 48 minutes played. |
| `offensive_avg48_free_throws_made` | double | Player's average free throws made per 48 minutes played. |
| `offensive_avg48_free_throws_attempted` | double | Player's average free throw attempts per 48 minutes played. |
| `offensive_avg48_points` | double | Player's average points scored per 48 minutes played. |
| `offensive_avg48_offensive_rebounds` | double | Player's average offensive rebounds per 48 minutes played. |
| `offensive_avg48_assists` | double | Player's average assists per 48 minutes played. |
| `offensive_avg48_turnovers` | double | Player's average turnovers committed per 48 minutes played. |
| `offensive_p40` | double | Points Per 40 Minutes. |
| `offensive_a40` | double | Assists Per 40 Minutes. |
| `offensive_orpm` | double | Offensive Real Plus-Minus. |
| `team_id` | integer | Unique team identifier. |
| `team_uid` | character | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_guid` | character | ESPN team GUID. |
| `team_slug` | character | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_location` | character | Team city or location string. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | character | Full team display name. |
| `team_short_display_name` | character | Short team display name (e.g. 'Aces'). |
| `team_color` | character | Team primary color (hex without leading '#'). |
| `team_alternate_color` | character | Team alternate color (hex without leading '#'). |
| `team_is_active` | logical | TRUE if the team is currently active. |
| `team_logo_href` | character | Default team logo URL. |

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

Polars dataframe containing schedule dates for the requested season. Returns None if no games

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `uid` | character | ESPN UID string. |
| `date` | character | Date in YYYY-MM-DD format. |
| `attendance` | integer | Reported attendance. |
| `time_valid` | logical | Time valid. |
| `neutral_site` | logical | Neutral site. |
| `conference_competition` | logical | Conference competition. |
| `play_by_play_available` | logical | Whether play-by-play data is available. |
| `recent` | logical | Recent. |
| `start_date` | character | Start date (YYYY-MM-DD). |
| `broadcast` | character | Broadcast information string. |
| `highlights` | integer | Game highlight urls. |
| `notes_type` | character | Notes type. |
| `notes_headline` | character | Notes headline. |
| `broadcast_market` | character | Broadcast market label (e.g. 'national', 'home'). |
| `broadcast_name` | character | Broadcast name. |
| `type_id` | character | Type identifier (numeric). |
| `type_abbreviation` | character | Type abbreviation. |
| `venue_id` | character | Unique venue identifier. |
| `venue_full_name` | character | Venue full name. |
| `venue_address_city` | character | Venue address city. |
| `venue_address_state` | character | Venue address state / region. |
| `venue_indoor` | logical | TRUE if the venue is indoors. |
| `status_clock` | double | Status clock. |
| `status_display_clock` | character | Status display clock. |
| `status_period` | integer | Status period. |
| `status_type_id` | character | Unique identifier for status type. |
| `status_type_name` | character | Status type name. |
| `status_type_state` | character | Status type state. |
| `status_type_completed` | logical | Status type completed. |
| `status_type_description` | character | Status type description. |
| `status_type_detail` | character | Status type detail. |
| `status_type_short_detail` | character | Status type short detail. |
| `format_regulation_periods` | integer | Format regulation periods. |
| `home_id` | character | Unique identifier for home. |
| `home_uid` | character | Home team's uid. |
| `home_location` | character | Home team's location. |
| `home_name` | character | Home name. |
| `home_abbreviation` | character | Home team's abbreviation. |
| `home_display_name` | character | Home display name. |
| `home_short_display_name` | character | Home short display name. |
| `home_color` | character | Color code (hex) for home. |
| `home_alternate_color` | character | Color code (hex) for home alternate. |
| `home_is_active` | logical | Home team's is active. |
| `home_venue_id` | character | Unique identifier for home venue. |
| `home_logo` | character | Home team logo URL. |
| `home_score` | character | Home team score at the time of the play. |
| `home_winner` | logical | Home team's winner. |
| `home_linescores` | list | Period-by-period point totals for the home team, stored as a list of integer scores. |
| `home_records` | character | Win-loss record strings for the home team across relevant splits (e.g., overall, home/away, conference). |
| `away_id` | character | Unique identifier for away. |
| `away_uid` | character | Away team's uid. |
| `away_location` | character | Away team's location. |
| `away_name` | character | Away name. |
| `away_abbreviation` | character | Away team's abbreviation. |
| `away_display_name` | character | Away display name. |
| `away_short_display_name` | character | Away short display name. |
| `away_color` | character | Color code (hex) for away. |
| `away_alternate_color` | character | Color code (hex) for away alternate. |
| `away_is_active` | logical | Away team's is active. |
| `away_venue_id` | character | Unique identifier for away venue. |
| `away_logo` | character | Away team logo URL. |
| `away_score` | character | Away team score at the time of the play. |
| `away_winner` | logical | Away team's winner. |
| `away_linescores` | list | Period-by-period point totals for the away team, stored as a list of integer scores. |
| `away_records` | character | Win-loss record strings for the away team across relevant splits (e.g., overall, home/away, conference). |
| `game_id` | integer | Unique game identifier. |
| `season` | integer | Season year. |
| `season_type` | integer | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |

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

Polars dataframe containing teams for the requested league. This function caches by default, so if you want to refresh the data, use the command sportsdataverse.nba.espn_nba_teams.clear_cache().

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
