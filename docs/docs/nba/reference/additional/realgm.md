---
title: "NBA — additional Python functions — RealGM"
sidebar_label: "RealGM"
sidebar_position: 7
description: "NBA — additional Python functions — RealGM — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — RealGM

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
| `pick` | integer | Pick number within the round. |
| `player` | character | Player name. |
| `team` | character | Team-side label or team identifier. |
| `draft_trades` | character | Draft-night trade note, if any. |
| `pos` | character | Position. |
| `ht` | character | Listed height. |
| `wt` | integer | Listed weight (lbs). |
| `age` | integer | Player age (in years). |
| `yos` | integer | Years of service. |
| `pre_draft_team` | character | Pre-draft team / school / club. |
| `class` | character | College class / draft eligibility note. |
| `nationality` | character | Player nationality. |
| `round` | integer | Tournament / playoff round. |
| `draft_year` | integer | Draft year (4-digit). |

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
| `team` | character | Team-side label or team identifier. |
| `division` | character | Team division. |
| `conference` | character | Conference name. |

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
| `date` | character | Date in YYYY-MM-DD format. |
| `transaction` | character | Transaction description. |

**Example**

```python
from sportsdataverse.nba.realgm import realgm_transactions

log = realgm_transactions()
print(log.shape)

# Pipeline next step

log.filter(pl.col("transaction").str.contains("(?i)two-way")).head()
```
