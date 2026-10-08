---
title: "NBA — additional Python functions — Basketball-Reference"
sidebar_label: "Basketball-Reference"
sidebar_position: 6
description: "NBA — additional Python functions — Basketball-Reference — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Basketball-Reference

### bref_awards {#bref_awards}

`bref_awards(season: 'Optional[int]' = None, *, return_as_pandas: 'bool' = False, proxy: 'Any' = None, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame'`

End-of-season award voting, all awards stacked into one frame.

Port of hoopR's `bref_awards()`. NBA only -- wehoop wraps no WNBA awards
page, so none is guessed at here.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season in 4-digit ending-year format. Defaults to the current NBA season. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `proxy` | `Any` | `None` | Proxy configuration in the `requests` `proxies=` shape. |

**Returns**

One row per candidate per award: `award` (`mvp`, `roy`, `dpoy`, `smoy`, `mip`, `clutch_poy`, `coy`), `rank`, `player`, `age`, `team`, `votes_first`, `points_won`, `points_max`, `award_share`, plus `season`. Zero rows when the page carries no voting table (award voting predates 1956 for none of them).

| col_name | type | description |
|---|---|---|
| `rank` | character | Rank. |
| `player` | character | Player name. |
| `age` | double | Player age (in years). |
| `team` | character | Team-side label or team identifier. |
| `votes_first` | double | First-place votes. |
| `points_won` | double | Voting points won. |
| `points_max` | double | Maximum possible voting points. |
| `award_share` | double | Share of the maximum voting points. |
| `award` | character | Award slug (`mvp`, `roy`, `dpoy`, `smoy`, `mip`, `clutch_poy`, `coy`). |
| `season` | integer | Season year. |

**Example**

```python
from sportsdataverse.nba.bref import bref_awards

df = bref_awards(season=2024)
print(df.shape)

# Pandas output

df_pd = bref_awards(season=2024, return_as_pandas=True)

# Pipeline next step (one line)

df.filter(pl.col("award") == "mvp").sort("award_share", descending=True).head()
```

### bref_draft {#bref_draft}

`bref_draft(season: 'Optional[int]' = None, *, return_as_pandas: 'bool' = False, proxy: 'Any' = None, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame'`

NBA draft results with each pick's career totals and advanced metrics.

Port of hoopR's `bref_draft()`. NBA only.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Draft year (e.g. `2024`). Defaults to the current NBA season. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `proxy` | `Any` | `None` | Proxy configuration in the `requests` `proxies=` shape. |

**Returns**

One row per pick: `pick_overall`, `round`, `team`, `player`, `college_name`, `seasons`, `g`, `mp`, `pts`, `trb`, `ast`, `fg_pct` …, `ws`, `ws_per_48`, `bpm`, `vorp`, plus `season`. Zero rows when the draft page is absent.

| col_name | type | description |
|---|---|---|
| `ranker` | double | Row rank. |
| `pick_overall` | double | Overall draft pick number. |
| `team` | character | Team-side label or team identifier. |
| `player` | character | Player name. |
| `college_name` | character | College / pre-draft team. |
| `seasons` | character | NBA seasons played. |
| `g` | character | Games played. |
| `mp` | character | Minutes played. |
| `pts` | character | Points scored. |
| `trb` | character | Career total rebounds. |
| `ast` | character | Assists. |
| `fg_pct` | character | Field goal percentage (0-1). |
| `fg3_pct` | character | Three-point field goal percentage (0-1). |
| `ft_pct` | character | Free throw percentage (0-1). |
| `mp_per_g` | character | Minutes (per_game table) / `mp` total (totals table). |
| `pts_per_g` | character | Points (scaled to the chosen `table`). |
| `trb_per_g` | character | Career total rebounds per game for the drafted player, from Basketball-Reference's draft table. Every value is an empty string in the sampled 2026 draft, whose picks had no NBA stats yet, so the column stays text there. |
| `ast_per_g` | character | Career assists per game for the drafted player, from Basketball-Reference's draft table. Every value is an empty string in the sampled 2026 draft, whose picks had no NBA stats yet, so the column stays text there. |
| `ws` | character | Career win shares. |
| `ws_per_48` | character | Career win shares per 48 minutes for the drafted player, from Basketball-Reference's draft table. Every value is an empty string in the sampled 2026 draft, whose picks had no NBA stats yet, so the column stays text there. |
| `bpm` | character | Career box plus/minus. |
| `vorp` | character | Career value over replacement player. |
| `season` | integer | Season year. |

**Example**

```python
from sportsdataverse.nba.bref import bref_draft

df = bref_draft(season=2024)
print(df.shape)

# Pandas output

df_pd = bref_draft(season=2003, return_as_pandas=True)

# Pipeline next step (one line)

df.sort("vorp", descending=True).head()
```

### bref_injuries {#bref_injuries}

`bref_injuries(*, return_as_pandas: 'bool' = False, proxy: 'Any' = None, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame'`

The current NBA injury report.

Port of hoopR's `bref_injuries()`. This is the live report -- there is no
season argument and no history. hoopR uses it in place of RotoWorld, which
NBC retired.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `proxy` | `Any` | `None` | Proxy configuration in the `requests` `proxies=` shape. |

**Returns**

One row per injured player: `player`, `team_name`, `date_update` and `note` (status plus description). Zero rows when no one is listed or the page is unreachable.

| col_name | type | description |
|---|---|---|
| `player` | character | Player name. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `date_update` | character | Date the status was last updated. |
| `note` | character | Injury status and description. |

**Example**

```python
from sportsdataverse.nba.bref import bref_injuries

df = bref_injuries()
print(df.shape)

# Pandas output

df_pd = bref_injuries(return_as_pandas=True)

# Pipeline next step (one line)

df.filter(pl.col("note").str.contains("(?i)out")).head()
```

### bref_player_bios {#bref_player_bios}

`bref_player_bios(letter: 'str' = 'a', *, return_as_pandas: 'bool' = False, proxy: 'Any' = None, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame'`

The player index for one last-name initial -- bios plus the id slugs.

Port of hoopR's `bref_player_bios()`. NBA only. This doubles as the
Basketball-Reference **player dictionary**: `player_id` is the slug that
`bref_player_game_log` takes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `letter` | `str` | `'a'` | Single letter `a`-`z` (last-name initial). Only the first character is used, case-insensitively. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `proxy` | `Any` | `None` | Proxy configuration in the `requests` `proxies=` shape. |

**Returns**

One row per player: `player`, `player_id` (e.g. `jamesle01`), `year_min`, `year_max`, `pos`, `height`, `weight`, `birth_date`, `colleges`, plus the echoed `letter`. `player_id` is omitted when the number of player links on the page does not match the number of rows (the same guard the R wrapper applies).

| col_name | type | description |
|---|---|---|
| `player` | character | Player name. |
| `player_id` | character | Unique player identifier. |
| `year_min` | double | First season played. |
| `year_max` | double | Last season played. |
| `pos` | character | Position. |
| `height` | character | Player height (string e.g. '6-2' or inches). |
| `weight` | double | Player weight in pounds. |
| `birth_date` | character | Date of birth (YYYY-MM-DD). |
| `colleges` | character | College(s). |
| `letter` | character | Last-name initial (echoes the `letter` argument). |

**Example**

```python
from sportsdataverse.nba.bref import bref_player_bios

df = bref_player_bios(letter="j")
print(df.shape)

# Build the id dictionary for the whole alphabet

import string
ids = [bref_player_bios(ch) for ch in string.ascii_lowercase]

# Pipeline next step (one line)

df.filter(pl.col("year_max") >= 2024).select(["player", "player_id"]).head()
```

### bref_player_game_log {#bref_player_game_log}

`bref_player_game_log(player_id: 'str', season: 'Optional[int]' = None, *, return_as_pandas: 'bool' = False, proxy: 'Any' = None, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame'`

A player's regular-season game-by-game log.

Port of hoopR's `bref_player_game_log()`. NBA only. The playoff log on the
same page (`player_game_log_post`) is not wrapped, matching the R surface.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_id` | `str` |  | Basketball-Reference player id slug -- the id in the player's URL, e.g. `jokicni01` from `/players/j/jokicni01.html`. Use `bref_player_bios` as the id dictionary. |
| `season` | `Optional[int]` | `None` | Season in 4-digit ending-year format. Defaults to the current NBA season. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `proxy` | `Any` | `None` | Proxy configuration in the `requests` `proxies=` shape. |

**Returns**

One row per regular-season game: `ranker`, `player_game_num_career`, `date`, `team`, `location` (`@` for away), `opp`, `result`, `is_starter`, `mp`, the full shooting / box columns, `game_score`, `plus_minus`, plus echoed `player_id` / `season`. Month-separator and no-date rows are dropped. Zero rows when the player did not play that season.

| col_name | type | description |
|---|---|---|
| `ranker` | double | Row rank. |
| `player_game_num_career` | double | Career game number. |
| `team_game_num_season` | double |  |
| `date` | character | Date in YYYY-MM-DD format. |
| `team` | character | Team-side label or team identifier. |
| `location` | character | Location. |
| `opp` | character | Opponent abbreviation. |
| `result` | character | Result. |
| `is_starter` | character | 1 if the player started. |
| `mp` | character | Minutes played. |
| `fg` | double |  |
| `fga` | double | Field goal attempts. |
| `fg_pct` | double | Field goal percentage (0-1). |
| `fg3` | double |  |
| `fg3a` | double | Three-point field goal attempts. |
| `fg3_pct` | double | Three-point field goal percentage (0-1). |
| `fg2` | double |  |
| `fg2a` | double |  |
| `fg2_pct` | double |  |
| `efg_pct` | double |  |
| `ft` | double |  |
| `fta` | double | Free throw attempts. |
| `ft_pct` | double | Free throw percentage (0-1). |
| `orb` | double |  |
| `drb` | double |  |
| `trb` | double | Career total rebounds. |
| `ast` | double | Assists. |
| `stl` | double | Steals. |
| `blk` | double | Blocks. |
| `tov` | double | Turnovers. |
| `pf` | double | Personal fouls. |
| `pts` | double | Points scored. |
| `game_score` | double | Bart Torvik single-game quality score. |
| `plus_minus` | double | Plus/minus point differential while on court. |
| `player_id` | character | Unique player identifier. |
| `season` | integer | Season year. |

**Example**

```python
from sportsdataverse.nba.bref import bref_player_game_log

df = bref_player_game_log(player_id="jokicni01", season=2024)
print(df.shape)

# Pandas output

df_pd = bref_player_game_log("jamesle01", 2024, return_as_pandas=True)

# Pipeline next step (one line)

df.select(["date", "opp", "pts", "trb", "ast"]).head()
```

### bref_team_roster {#bref_team_roster}

`bref_team_roster(team: 'str', season: 'Optional[int]' = None, *, return_as_pandas: 'bool' = False, proxy: 'Any' = None, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame'`

A team's roster for one season.

Port of hoopR's `bref_team_roster()`. NBA only.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team` | `str` |  | Basketball-Reference team abbreviation (`BOS`, `LAL`, `GSW`). Historical franchises use their era code (`NJN`, `SEA`). |
| `season` | `Optional[int]` | `None` | Season in 4-digit ending-year format. Defaults to the current NBA season. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `proxy` | `Any` | `None` | Proxy configuration in the `requests` `proxies=` shape. |

**Returns**

One row per rostered player: `number`, `player`, `pos`, `height`, `weight`, `birth_date`, `flag`, `years_experience`, `college`, plus echoed `team` / `season`. Zero rows when the team/season combination has no page.

| col_name | type | description |
|---|---|---|
| `number` | double | Number. |
| `player` | character | Player name. |
| `pos` | character | Position. |
| `height` | character | Player height (string e.g. '6-2' or inches). |
| `weight` | double | Player weight in pounds. |
| `birth_date` | character | Date of birth (YYYY-MM-DD). |
| `flag` | character |  |
| `years_experience` | character | Years of NBA experience (`R` for rookies). |
| `college` | character | College. |
| `team` | character | Team-side label or team identifier. |
| `season` | integer | Season year. |

**Example**

```python
from sportsdataverse.nba.bref import bref_team_roster

df = bref_team_roster(team="BOS", season=2024)
print(df.shape)

# A historical franchise code

sonics = bref_team_roster(team="SEA", season=1996)

# Pipeline next step (one line)

df.select(["player", "pos", "height", "college"]).head()
```
