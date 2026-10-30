# NBA — additional Python functions — RealGM

> NBA — additional Python functions — RealGM — function reference in sdv-py, the SportsDataverse Python package.

### realgm_close_browser {#realgm_close_browser}

`realgm_close_browser() -> 'None'`

Close the cached headless browser, if one is open.

The browser is otherwise kept for `SDV_PY_REALGM_TTL` idle seconds and released at
interpreter exit. Call this to free it early -- after a batch pull, or before a long
stretch of work that will not touch RealGM.

**Example**

```python
from sportsdataverse.nba.realgm import realgm_players, realgm_close_browser

players = realgm_players()
realgm_close_browser()
```

### realgm_coaches {#realgm_coaches}

`realgm_coaches(*, fetcher: 'Optional[Fetcher]' = None, proxy: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Current NBA head coaches.

Port of hoopR's `realgm_coaches()` (staff-role id `20`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fetcher` | `Optional[Fetcher]` | `None` | Callable `(path, proxy) -> html`; defaults to the headless-browser fetch. |
| `proxy` | `Optional[str]` | `None` | Proxy URL for the browser launch (env fallback `SDV_PY_REALGM_PROXY`). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per coach -- `staff`, `team`, `start_season`, `years_in_role`, `birth_date`, `nationality`. Zero rows when the page carried no data table.

| col_name | type | description |
|---|---|---|
| `staff` | String | Coach name. |
| `team` | String | Team-side label or team identifier. |
| `start_season` | String | Season the coach started with the team. |
| `years_in_role` | Int64 | Seasons in the role. |
| `birth_date` | String | Date of birth (YYYY-MM-DD). |
| `nationality` | String | Player nationality. |

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | When Playwright is missing and no `fetcher` was supplied. |
| `AssetFetchError` | The page was still Cloudflare's challenge page (it never cleared). It is a failed fetch, never a zero-row answer. |

**Example**

```python
from sportsdataverse.nba.realgm import realgm_coaches

coaches = realgm_coaches()
print(coaches.shape)
```

### realgm_draft {#realgm_draft}

`realgm_draft(year: 'Optional[int]' = None, *, fetcher: 'Optional[Fetcher]' = None, proxy: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Results of one past NBA draft.

Port of hoopR's `realgm_draft()`. Every table carrying `player` / `pos` / `ht`
is stacked: the pick tables plus RealGM's listed undrafted players. `round` is
derived from the overall pick number (`> 30` -> round 2) as in the R original, and is
null for a table with no `pick` column (the undrafted list).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Optional[int]` | `None` | Draft year (the calendar year the draft was held). Defaults to the most recently completed draft. |
| `fetcher` | `Optional[Fetcher]` | `None` | Callable `(path, proxy) -> html`; defaults to the headless-browser fetch. |
| `proxy` | `Optional[str]` | `None` | Proxy URL for the browser launch (env fallback `SDV_PY_REALGM_PROXY`). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per selection -- `pick`, `player`, `team`, `draft_trades`, `pos`, `ht`, `wt`, `age`, `yos`, `pre_draft_team`, `class`, `nationality`, plus `round` and `draft_year`. Zero rows when no draft table was found.

| col_name | type | description |
|---|---|---|
| `pick` | Int64 | Pick number within the round. |
| `player` | String | Player name. |
| `team` | String | Team-side label or team identifier. |
| `draft_trades` | String | Draft-night trade note, if any. |
| `pos` | String | Position. |
| `ht` | String | Listed height. |
| `wt` | Int64 | Listed weight (lbs). |
| `age` | Int64 | Player age (in years). |
| `yos` | Int64 | Years of service. |
| `pre_draft_team` | String | Pre-draft team / school / club. |
| `class` | String | College class / draft eligibility note. |
| `nationality` | String | Player nationality. |
| `round` | Int64 | Tournament / playoff round. |
| `draft_year` | Int64 | Draft year (4-digit). |

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | When Playwright is missing and no `fetcher` was supplied. |
| `AssetFetchError` | The page was still Cloudflare's challenge page (it never cleared). It is a failed fetch, never a zero-row answer. |

**Example**

```python
from sportsdataverse.nba.realgm import realgm_draft

draft = realgm_draft(year=2020)
print(draft.shape)

# Pipeline next step

draft.filter(pl.col("round") == 1).head()
```

### realgm_draft_prospects {#realgm_draft_prospects}

`realgm_draft_prospects(*, fetcher: 'Optional[Fetcher]' = None, proxy: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Current NBA draft-prospect statistics.

Port of hoopR's `realgm_draft_prospects()`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fetcher` | `Optional[Fetcher]` | `None` | Callable `(path, proxy) -> html`; defaults to the headless-browser fetch. |
| `proxy` | `Optional[str]` | `None` | Proxy URL for the browser launch (env fallback `SDV_PY_REALGM_PROXY`). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per prospect -- `player`, `team` (school / club), `gp`, `mpg`, `ppg`, shooting splits, `rpg`, `apg`, `spg`, `bpg`. Zero rows when the page carried no data table.

No returns table is published for this function: no capture: the RealGM page it reads now carries no table, so it returns an empty frame.

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | When Playwright is missing and no `fetcher` was supplied. |
| `AssetFetchError` | The page was still Cloudflare's challenge page (it never cleared). It is a failed fetch, never a zero-row answer. |

**Example**

```python
from sportsdataverse.nba.realgm import realgm_draft_prospects

prospects = realgm_draft_prospects()
print(prospects.shape)
```

### realgm_early_entry {#realgm_early_entry}

`realgm_early_entry(*, fetcher: 'Optional[Fetcher]' = None, proxy: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

The current NBA draft early-entrant and withdrawal list.

Port of hoopR's `realgm_early_entry()`: RealGM's college and international
entrant/withdrawal tables stacked into one frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fetcher` | `Optional[Fetcher]` | `None` | Callable `(path, proxy) -> html`; defaults to the headless-browser fetch. |
| `proxy` | `Optional[str]` | `None` | Proxy URL for the browser launch (env fallback `SDV_PY_REALGM_PROXY`). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per candidate -- `player`, `pos`, `ht`, `wt`, `birth_date`, `college` / `pre_draft_team`, `class`, `draft_status`, `yos`, `nationality`. Zero rows when no early-entry table was found.

| col_name | type | description |
|---|---|---|
| `player` | String | Player name. |
| `pos` | String | Position. |
| `ht` | String | Listed height. |
| `wt` | Float64 | Listed weight (lbs). |
| `birth_date` | String | Date of birth (YYYY-MM-DD). |
| `college` | String | College. |
| `class` | String | College class / draft eligibility note. |
| `draft_status` | String | Draft pick / undrafted status. |
| `yos` | Int64 | Years of service. |
| `nationality` | String | Player nationality. |
| `pre_draft_team` | String | Pre-draft team / school / club. |

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | When Playwright is missing and no `fetcher` was supplied. |
| `AssetFetchError` | The page was still Cloudflare's challenge page (it never cleared). It is a failed fetch, never a zero-row answer. |

**Example**

```python
from sportsdataverse.nba.realgm import realgm_early_entry

entrants = realgm_early_entry()
print(entrants.shape)
```

### realgm_future_free_agents {#realgm_future_free_agents}

`realgm_future_free_agents(*, fetcher: 'Optional[Fetcher]' = None, proxy: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

RealGM's projected future NBA free-agent classes, with each player's agent.

Port of hoopR's `realgm_future_free_agents()`. The `agent` column is the
distinctive one -- no first-party feed publishes it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fetcher` | `Optional[Fetcher]` | `None` | Callable `(path, proxy) -> html`; defaults to the headless-browser fetch. |
| `proxy` | `Optional[str]` | `None` | Proxy URL for the browser launch (env fallback `SDV_PY_REALGM_PROXY`). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per upcoming free agent -- `player`, `pos`, `team`, `season`, `age`, `yos`, `veteran_fa_status`, `gp`, `pts`, `reb`, `ast`, `per`, `agent`. Zero rows when the page carried no data table.

| col_name | type | description |
|---|---|---|
| `player` | String | Player name. |
| `pos` | String | Position. |
| `team` | String | Team-side label or team identifier. |
| `season` | String | Season year. |
| `age` | Int64 | Player age (in years). |
| `yos` | Int64 | Years of service. |
| `veteran_fa_status` | String | Bird / Non-Bird / veteran FA status. |
| `gp` | Int64 | Games played. |
| `pts` | Float64 | Points scored. |
| `reb` | Float64 | Rebounds per game. |
| `ast` | Float64 | Assists. |
| `per` | Float64 | Player Efficiency Rating. |
| `agent` | String | Listed player agent. |

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | When Playwright is missing and no `fetcher` was supplied. |
| `AssetFetchError` | The page was still Cloudflare's challenge page (it never cleared). It is a failed fetch, never a zero-row answer. |

**Example**

```python
from sportsdataverse.nba.realgm import realgm_future_free_agents

fas = realgm_future_free_agents()
print(fas.shape)

# Pipeline next step

fas.group_by("agent").agg(pl.len().alias("clients")).sort("clients", descending=True)
```

### realgm_gms {#realgm_gms}

`realgm_gms(*, fetcher: 'Optional[Fetcher]' = None, proxy: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Current NBA general managers.

Port of hoopR's `realgm_gms()` (staff-role id `16`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fetcher` | `Optional[Fetcher]` | `None` | Callable `(path, proxy) -> html`; defaults to the headless-browser fetch. |
| `proxy` | `Optional[str]` | `None` | Proxy URL for the browser launch (env fallback `SDV_PY_REALGM_PROXY`). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per general manager -- `staff`, `team`, `start_season`, `years_in_role`, `birth_date`, `nationality`. Zero rows when the page carried no data table.

| col_name | type | description |
|---|---|---|
| `staff` | String | Coach name. |
| `team` | String | Team-side label or team identifier. |
| `start_season` | String | Season the coach started with the team. |
| `years_in_role` | Int64 | Seasons in the role. |
| `birth_date` | String | Date of birth (YYYY-MM-DD). |
| `nationality` | String | Player nationality. |

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | When Playwright is missing and no `fetcher` was supplied. |
| `AssetFetchError` | The page was still Cloudflare's challenge page (it never cleared). It is a failed fetch, never a zero-row answer. |

**Example**

```python
from sportsdataverse.nba.realgm import realgm_gms

gms = realgm_gms()
print(gms.shape)
```

### realgm_individual_games {#realgm_individual_games}

`realgm_individual_games(*, fetcher: 'Optional[Fetcher]' = None, proxy: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

The all-time best individual NBA games leaderboard.

Port of hoopR's `realgm_individual_games()`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fetcher` | `Optional[Fetcher]` | `None` | Callable `(path, proxy) -> html`; defaults to the headless-browser fetch. |
| `proxy` | `Optional[str]` | `None` | Proxy URL for the browser launch (env fallback `SDV_PY_REALGM_PROXY`). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per player-game -- `player`, `date`, `team`, `min`, `pts`, `fgm`, `fga`, `reb`, `ast`, `stl`, `blk`, ... Zero rows when the page carried no data table.

No returns table is published for this function: no capture: the RealGM page it reads now carries no table, so it returns an empty frame.

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | When Playwright is missing and no `fetcher` was supplied. |
| `AssetFetchError` | The page was still Cloudflare's challenge page (it never cleared). It is a failed fetch, never a zero-row answer. |

**Example**

```python
from sportsdataverse.nba.realgm import realgm_individual_games

best = realgm_individual_games()
print(best.shape)
```

### realgm_individual_seasons {#realgm_individual_seasons}

`realgm_individual_seasons(*, fetcher: 'Optional[Fetcher]' = None, proxy: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

The all-time best individual NBA seasons leaderboard.

Port of hoopR's `realgm_individual_seasons()`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fetcher` | `Optional[Fetcher]` | `None` | Callable `(path, proxy) -> html`; defaults to the headless-browser fetch. |
| `proxy` | `Optional[str]` | `None` | Proxy URL for the browser launch (env fallback `SDV_PY_REALGM_PROXY`). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per player-season -- `player`, `season`, `team`, `gp`, `min`, `pts`, shooting splits, `reb`, `ast`, ... Zero rows when the page carried no data table.

| col_name | type | description |
|---|---|---|
| `number` | Float64 | Number. |
| `player` | String | Player name. |
| `season` | Int64 | Season year. |
| `team` | String | Team-side label or team identifier. |
| `gp` | Int64 | Games played. |
| `min` | Float64 | Minutes played. |
| `pts` | Float64 | Points scored. |
| `fgm` | Float64 | Field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `3_pm` | String |  |
| `3_pa` | String |  |
| `3_p_pct` | String |  |
| `ftm` | Float64 | Free throws made. |
| `fta` | Float64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `orb` | String |  |
| `drb` | String |  |
| `reb` | Float64 | Rebounds per game. |
| `ast` | Float64 | Assists. |
| `stl` | String | Steals. |
| `blk` | String | Blocks. |
| `tov` | String | Turnovers. |
| `pf` | Float64 | Personal fouls. |

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | When Playwright is missing and no `fetcher` was supplied. |
| `AssetFetchError` | The page was still Cloudflare's challenge page (it never cleared). It is a failed fetch, never a zero-row answer. |

**Example**

```python
from sportsdataverse.nba.realgm import realgm_individual_seasons

best = realgm_individual_seasons()
print(best.shape)
```

### realgm_player_stats {#realgm_player_stats}

`realgm_player_stats(season: 'Optional[int]' = None, stat_type: 'str' = 'Averages', season_type: 'str' = 'Regular_Season', *, fetcher: 'Optional[Fetcher]' = None, proxy: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Season player-statistics leaderboard for one stat family and season segment.

Port of hoopR's `realgm_player_stats()`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season by **ending** year (`2026` = 2025-26). Defaults to `sportsdataverse.nba.nba_schedule.most_recent_nba_season`. |
| `stat_type` | `str` | `'Averages'` | One of `"Averages"`, `"Totals"`, `"Per_48"`, `"Per_40"`, `"Per_36"`, `"Per_Minute"`, `"Advanced_Stats"`, `"Misc_Stats"`. |
| `season_type` | `str` | `'Regular_Season'` | One of `"Regular_Season"`, `"Playoffs"`, `"Preseason"`, `"Summer_League"`. |
| `fetcher` | `Optional[Fetcher]` | `None` | Callable `(path, proxy) -> html`; defaults to the headless-browser fetch. |
| `proxy` | `Optional[str]` | `None` | Proxy URL for the browser launch (env fallback `SDV_PY_REALGM_PROXY`). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per qualified player, columns varying by `stat_type` (for `"Averages"`: `player`, `team`, `gp`, `mpg`, `ppg`, `rpg`, `apg`, ...), plus the echoed `season` / `stat_type` / `season_type`. Zero rows when the page carried no data table.

| col_name | type | description |
|---|---|---|
| `number` | Float64 | Number. |
| `player` | String | Player name. |
| `team` | String | Team-side label or team identifier. |
| `gp` | Int64 | Games played. |
| `mpg` | Float64 | Minutes per game. |
| `ppg` | Float64 | Points per game. |
| `fgm` | Float64 | Field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `3_pm` | Float64 |  |
| `3_pa` | Float64 |  |
| `3_p_pct` | Float64 |  |
| `ftm` | Float64 | Free throws made. |
| `fta` | Float64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `orb` | Float64 |  |
| `drb` | Float64 |  |
| `rpg` | Float64 | Rebounds per game. |
| `apg` | Float64 | Assists per game. |
| `spg` | Float64 | Steals per game. |
| `bpg` | Float64 | Blocks per game. |
| `tov` | Float64 | Turnovers. |
| `pf` | Float64 | Personal fouls. |
| `season` | Int64 | Season year. |
| `stat_type` | String | Stat type code (e.g. "win", "loss"). |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | When Playwright is missing and no `fetcher` was supplied. |
| `AssetFetchError` | The page was still Cloudflare's challenge page (it never cleared). It is a failed fetch, never a zero-row answer. |

**Example**

```python
from sportsdataverse.nba.realgm import realgm_player_stats

stats = realgm_player_stats(season=2025, stat_type="Averages")
print(stats.shape)

# Pipeline next step

stats.sort("ppg", descending=True).head(10)
```

### realgm_players {#realgm_players}

`realgm_players(*, fetcher: 'Optional[Fetcher]' = None, proxy: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

The active NBA player index from RealGM.

Port of hoopR's `realgm_players()`. RealGM's roster of active players, including the
pre-draft / international club detail the site is uniquely good for (Jokic ->
"KK Mega Bemax (Serbia)").

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fetcher` | `Optional[Fetcher]` | `None` | Callable `(path, proxy) -> html`. Defaults to the headless-browser fetch; inject one to run offline. |
| `proxy` | `Optional[str]` | `None` | Proxy URL for the browser launch. Falls back to `SDV_PY_REALGM_PROXY` then `SDV_PY_PROXY`. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per active player -- `number`, `player`, `pos`, `ht`, `wt`, `age`, `current_team`, `yos`, `pre_draft_team`, `draft_status`, `nationality` (transcribed from hoopR; unverified against live HTML). A zero-row frame when the page carried no data table.

| col_name | type | description |
|---|---|---|
| `number` | Float64 | Number. |
| `player` | String | Player name. |
| `pos` | String | Position. |
| `ht` | String | Listed height. |
| `wt` | Int64 | Listed weight (lbs). |
| `age` | Int64 | Player age (in years). |
| `current_team` | String | Current NBA team. |
| `yos` | Int64 | Years of service. |
| `pre_draft_team` | String | Pre-draft team / school / club. |
| `draft_status` | String | Draft pick / undrafted status. |
| `nationality` | String | Player nationality. |

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | When Playwright is missing and no `fetcher` was supplied. |
| `AssetFetchError` | The page was still Cloudflare's challenge page (it never cleared). It is a failed fetch, never a zero-row answer. |

**Example**

```python
from sportsdataverse.nba.realgm import realgm_players

players = realgm_players()
print(players.shape)

# Offline / testing -- inject a transport, no browser needed

players = realgm_players(fetcher=lambda path, proxy: "<html>...</html>")

# Pipeline next step

players.filter(pl.col("nationality") != "United States").head()
```

### realgm_players_abroad {#realgm_players_abroad}

`realgm_players_abroad(*, fetcher: 'Optional[Fetcher]' = None, proxy: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

NBA-affiliated players currently playing overseas.

Port of hoopR's `realgm_players_abroad()`. Draft picks, two-way and free-agent
players on international rosters -- a view no first-party NBA/ESPN endpoint provides.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fetcher` | `Optional[Fetcher]` | `None` | Callable `(path, proxy) -> html`; defaults to the headless-browser fetch. |
| `proxy` | `Optional[str]` | `None` | Proxy URL for the browser launch (env fallback `SDV_PY_REALGM_PROXY`). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per player -- `player`, `pos`, `ht`, `wt`, `nba_status`, `team_s`, `gp`, `mpg`, `ppg`, `rpg`, `apg`. Zero rows when the page carried no data table.

| col_name | type | description |
|---|---|---|
| `player` | String | Player name. |
| `pos` | String | Position. |
| `ht` | String | Listed height. |
| `wt` | Int64 | Listed weight (lbs). |
| `nba_status` | String | NBA contract / rights status. |
| `team_s` | String | Current overseas team(s) and NBA affiliation. |
| `gp` | Int64 | Games played. |
| `mpg` | Float64 | Minutes per game. |
| `ppg` | Float64 | Points per game. |
| `rpg` | Float64 | Rebounds per game. |
| `apg` | Float64 | Assists per game. |

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | When Playwright is missing and no `fetcher` was supplied. |
| `AssetFetchError` | The page was still Cloudflare's challenge page (it never cleared). It is a failed fetch, never a zero-row answer. |

**Example**

```python
from sportsdataverse.nba.realgm import realgm_players_abroad

abroad = realgm_players_abroad()
print(abroad.shape)
```

### realgm_rookie_scale {#realgm_rookie_scale}

`realgm_rookie_scale(*, fetcher: 'Optional[Fetcher]' = None, proxy: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

The current NBA rookie-scale salary table.

Port of hoopR's `realgm_rookie_scale()`. Dollar figures are the formatted strings
RealGM publishes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fetcher` | `Optional[Fetcher]` | `None` | Callable `(path, proxy) -> html`; defaults to the headless-browser fetch. |
| `proxy` | `Optional[str]` | `None` | Proxy URL for the browser launch (env fallback `SDV_PY_REALGM_PROXY`). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per first-round pick -- `pick`, the four contract-year amounts, the 4th-year option increase and the qualifying-offer increase. Zero rows when the page carried no data table.

| col_name | type | description |
|---|---|---|
| `pick` | Int64 | Pick number within the round. |
| `1st_year_salary` | String |  |
| `2nd_year_salary` | String |  |
| `3rd_year_option_salary` | String |  |
| `4th_year_option_percentage_increased_over_3rd_year_salary` | String |  |
| `qualifying_offer_percentage_increase_over_4th_year_salary` | String |  |

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | When Playwright is missing and no `fetcher` was supplied. |
| `AssetFetchError` | The page was still Cloudflare's challenge page (it never cleared). It is a failed fetch, never a zero-row answer. |

**Example**

```python
from sportsdataverse.nba.realgm import realgm_rookie_scale

scale = realgm_rookie_scale()
print(scale.shape)
```

### realgm_salary_cap {#realgm_salary_cap}

`realgm_salary_cap(*, fetcher: 'Optional[Fetcher]' = None, proxy: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

NBA salary-cap history and projections.

Port of hoopR's `realgm_salary_cap()`. Dollar figures come back as the formatted
strings RealGM publishes (`"$140,588,000"`) -- strip non-numeric characters to get
numerics.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fetcher` | `Optional[Fetcher]` | `None` | Callable `(path, proxy) -> html`; defaults to the headless-browser fetch. |
| `proxy` | `Optional[str]` | `None` | Proxy URL for the browser launch (env fallback `SDV_PY_REALGM_PROXY`). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per season -- `season`, `salary_cap`, `luxury_tax`, `x1st_apron`, `x2nd_apron`, `bae`, `non_taxpayer_mle`, `taxpayer_mle`, `team_room_mle`. Zero rows when the page carried no data table.

| col_name | type | description |
|---|---|---|
| `season` | String | Season year. |
| `salary_cap` | String | Salary cap. |
| `luxury_tax` | String | Luxury-tax threshold. |
| `1st_apron` | String |  |
| `2nd_apron` | String |  |
| `bae` | String | Bi-annual exception. |
| `non_taxpayer_mle` | String | Non-taxpayer mid-level exception. |
| `taxpayer_mle` | String | Taxpayer mid-level exception. |
| `team_room_mle` | String | Room mid-level exception. |

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | When Playwright is missing and no `fetcher` was supplied. |
| `AssetFetchError` | The page was still Cloudflare's challenge page (it never cleared). It is a failed fetch, never a zero-row answer. |

**Example**

```python
from sportsdataverse.nba.realgm import realgm_salary_cap

caps = realgm_salary_cap()
print(caps.shape)

# Pipeline next step -- parse the dollar strings

caps.with_columns(pl.col("salary_cap").str.replace_all(r"[^0-9.]", "").cast(pl.Float64))
```

### realgm_standings {#realgm_standings}

`realgm_standings(*, fetcher: 'Optional[Fetcher]' = None, proxy: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Current NBA standings, both conferences stacked.

Port of hoopR's `realgm_standings()`. The Eastern and Western conference tables are
row-bound and labelled by a `conference` column, assigned **by table order** (first
qualifying table -> Eastern) exactly as the R original does -- so a RealGM layout
change that reorders the two tables would mislabel them.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fetcher` | `Optional[Fetcher]` | `None` | Callable `(path, proxy) -> html`; defaults to the headless-browser fetch. |
| `proxy` | `Optional[str]` | `None` | Proxy URL for the browser launch (env fallback `SDV_PY_REALGM_PROXY`). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per team -- `number`, `team`, `w`, `l`, `pct`, `gb`, `l10`, `strk`, `ppg`, `oppg`, `diff`, `home`, `away` plus `conference` (`"Eastern"` / `"Western"`). Zero rows when no standings table was found.

| col_name | type | description |
|---|---|---|
| `number` | Int64 | Number. |
| `team` | String | Team-side label or team identifier. |
| `w` | Int64 | Wins. |
| `l` | Int64 | Losses. |
| `pct` | Float64 | Win percentage. |
| `gb` | Int64 | Games behind the conference leader. |
| `l10` | String | Last-ten record. |
| `strk` | Int64 | Current streak. |
| `ppg` | Int64 | Points per game. |
| `oppg` | Int64 | Opponent points per game. |
| `diff` | Int64 | Scoring margin. |
| `home` | String | Home. |
| `away` | String | Away record. |
| `div` | String |  |
| `conf` | String | character. |
| `rem` | Int64 |  |
| `rowp` | Float64 |  |
| `conference` | String | Conference name. |

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | When Playwright is missing and no `fetcher` was supplied. |
| `AssetFetchError` | The page was still Cloudflare's challenge page (it never cleared). It is a failed fetch, never a zero-row answer. |

**Example**

```python
from sportsdataverse.nba.realgm import realgm_standings

standings = realgm_standings()
print(standings.shape)

# Pipeline next step

standings.filter(pl.col("conference") == "Eastern").head()
```

### realgm_team_stats {#realgm_team_stats}

`realgm_team_stats(season: 'Optional[int]' = None, stat_type: 'str' = 'Averages', season_type: 'str' = 'Regular_Season', *, fetcher: 'Optional[Fetcher]' = None, proxy: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Season team statistics for one stat family and season segment.

Port of hoopR's `realgm_team_stats()`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season by **ending** year (`2026` = 2025-26). Defaults to `sportsdataverse.nba.nba_schedule.most_recent_nba_season`. |
| `stat_type` | `str` | `'Averages'` | One of `"Averages"`, `"Totals"`, `"Advanced_Stats"`, `"Misc_Stats"`. |
| `season_type` | `str` | `'Regular_Season'` | One of `"Regular_Season"`, `"Playoffs"`, `"Preseason"`, `"Summer_League"`. |
| `fetcher` | `Optional[Fetcher]` | `None` | Callable `(path, proxy) -> html`; defaults to the headless-browser fetch. |
| `proxy` | `Optional[str]` | `None` | Proxy URL for the browser launch (env fallback `SDV_PY_REALGM_PROXY`). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per team (`team`, `gp`, `mpg`, `ppg`, `rpg`, `apg`, ... for `"Averages"`) plus the echoed `season` / `stat_type` / `season_type`. Zero rows when the page carried no data table.

| col_name | type | description |
|---|---|---|
| `number` | Float64 | Number. |
| `team` | String | Team-side label or team identifier. |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `e_fg_pct` | Float64 | E field goals percentage (0-1 decimal). |
| `total_s_pct` | Float64 |  |
| `orb_pct` | Float64 | Offensive rebound percentage. |
| `drb_pct` | Float64 | Defensive rebound percentage. |
| `trb_pct` | Float64 |  |
| `ast_pct` | Float64 | Assist percentage. |
| `tov_pct` | Float64 |  |
| `stl_pct` | Float64 | Steals percentage (0-1 decimal). |
| `blk_pct` | Float64 | Blocks percentage (0-1 decimal). |
| `pps` | Float64 |  |
| `fic40` | Float64 |  |
| `o_rtg` | Float64 | O rtg. |
| `d_rtg` | Float64 |  |
| `e_diff` | Float64 |  |
| `poss` | Float64 | Poss. |
| `pace` | Float64 | Possessions per 48 minutes. |
| `season` | Int64 | Season year. |
| `stat_type` | String | Stat type code (e.g. "win", "loss"). |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | When Playwright is missing and no `fetcher` was supplied. |
| `AssetFetchError` | The page was still Cloudflare's challenge page (it never cleared). It is a failed fetch, never a zero-row answer. |

**Example**

```python
from sportsdataverse.nba.realgm import realgm_team_stats

teams = realgm_team_stats(season=2025, stat_type="Advanced_Stats")
print(teams.shape)
```

### realgm_teams {#realgm_teams}

`realgm_teams(*, fetcher: 'Optional[Fetcher]' = None, proxy: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

The NBA team index with division and conference.

Port of hoopR's `realgm_teams()`. RealGM renders one small table per division, headed
by e.g. "Atlantic Division"; the division name comes from that first header and the
conference from a static division -> conference map.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fetcher` | `Optional[Fetcher]` | `None` | Callable `(path, proxy) -> html`; defaults to the headless-browser fetch. |
| `proxy` | `Optional[str]` | `None` | Proxy URL for the browser launch (env fallback `SDV_PY_REALGM_PROXY`). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per team -- `team`, `division`, `conference`. Zero rows (with that schema) when no division table was recognised.

| col_name | type | description |
|---|---|---|
| `team` | String | Team-side label or team identifier. |
| `division` | String | Team division. |
| `conference` | String | Conference name. |

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | When Playwright is missing and no `fetcher` was supplied. |
| `AssetFetchError` | The page was still Cloudflare's challenge page (it never cleared). It is a failed fetch, never a zero-row answer. |

**Example**

```python
from sportsdataverse.nba.realgm import realgm_teams

teams = realgm_teams()
print(teams.shape)
```

### realgm_transactions {#realgm_transactions}

`realgm_transactions(*, fetcher: 'Optional[Fetcher]' = None, proxy: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

The NBA league transactions log.

Port of hoopR's `realgm_transactions()` -- the one non-tabular RealGM page. RealGM
publishes transactions as a dated narrative list (`h3` date heading + `ul li`
items), so this parses the DOM rather than a `<table>`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fetcher` | `Optional[Fetcher]` | `None` | Callable `(path, proxy) -> html`; defaults to the headless-browser fetch. |
| `proxy` | `Optional[str]` | `None` | Proxy URL for the browser launch (env fallback `SDV_PY_REALGM_PROXY`). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per transaction -- `date` (`polars.Date`) and `transaction` (text). Zero rows (with that schema) when no dated block parsed.

| col_name | type | description |
|---|---|---|
| `date` | String | Date in YYYY-MM-DD format. |
| `transaction` | String | Transaction description. |

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | When Playwright is missing and no `fetcher` was supplied. |
| `AssetFetchError` | The page was still Cloudflare's challenge page (it never cleared). It is a failed fetch, never a zero-row answer. |

**Example**

```python
from sportsdataverse.nba.realgm import realgm_transactions

log = realgm_transactions()
print(log.shape)

# Pipeline next step

log.filter(pl.col("transaction").str.contains("(?i)two-way")).head()
```
