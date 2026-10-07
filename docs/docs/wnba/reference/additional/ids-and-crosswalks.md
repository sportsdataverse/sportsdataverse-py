---
title: "WNBA — additional Python functions — IDs and crosswalks"
sidebar_label: "IDs and crosswalks"
sidebar_position: 9
description: "WNBA — additional Python functions — IDs and crosswalks — function reference in sdv-py, the SportsDataverse Python package."
---
# WNBA — additional Python functions — IDs and crosswalks

### wnba_player_crosswalk {#wnba_player_crosswalk}

`wnba_player_crosswalk(season: 'Optional[int]' = None, min_confidence: 'float' = 0.92, *, return_as_pandas: 'bool' = False, strict: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the WNBA cross-source player crosswalk (ESPN / WNBA Stats / Fox).

One row per ESPN athlete per team. `match_method` / `match_confidence`
describe the **Stats API** match (normalized exact name, then
Jaro-Winkler with jersey and DOB tiebreaks); Fox contributes
`fox_athlete_id` only.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year (e.g. `2026`). Defaults to the most recent WNBA season. |
| `min_confidence` | `float` | `0.92` | Jaro-Winkler floor for fuzzy matches (R default 0.92). |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |
| `strict` | `bool` | `False` | Raise on the first failed per-team ESPN or Fox roster fetch (a 404 is still skipped) instead of skipping isolated failures. Default `False` matches the R producers; a provider whose every item failed raises either way. An item the host *answered* -- including a 404 -- counts as answered. |

**Returns**

`pl.DataFrame` (or pandas), one row per ESPN athlete, 21 columns.

**Example**

```python
from sportsdataverse.wnba import wnba_player_crosswalk
df = wnba_player_crosswalk(season=2026)
print(df["match_method"].value_counts())

# Tighten the fuzzy floor

strict = wnba_player_crosswalk(season=2026, min_confidence=0.97)

# Pipeline next step (one line)

df.filter(pl.col("match_method") == "fuzzy_jw").head()
```

### wnba_schedule_crosswalk {#wnba_schedule_crosswalk}

`wnba_schedule_crosswalk(season: 'Optional[int]' = None, *, stats_games: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, strict: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the WNBA cross-source schedule crosswalk (ESPN / WNBA Stats).

One row per game, joined on `(game_date, home_espn_team_id,
away_espn_team_id)` after both sides reduce to the Eastern-Time date. The
Stats CDN serves the current season only, so the live builder is
effectively current-season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year (e.g. `2026`). Defaults to the most recent WNBA season. |
| `stats_games` | `Optional[DataFrame]` | `None` | Pre-fetched Stats schedule frame; `None` fetches live. |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |
| `strict` | `bool` | `False` | Raise on the first failed per-date ESPN scoreboard fetch (a 404 is still skipped) instead of skipping isolated failures. Default `False` matches the R producers; a provider whose every item failed raises either way. An item the host *answered* -- including a 404 -- counts as answered. |

**Returns**

`pl.DataFrame` (or pandas) with `SCHEDULE_COLUMNS`.

**Example**

```python
from sportsdataverse.wnba import wnba_schedule_crosswalk
df = wnba_schedule_crosswalk(season=2026)
print(df["match_method"].value_counts())

# Pipeline next step (one line)

df.filter(pl.col("match_method") == "both").select("espn_game_id", "wnba_game_id").head()
```

### wnba_team_crosswalk {#wnba_team_crosswalk}

`wnba_team_crosswalk(season: 'Optional[int]' = None, *, stats: 'Optional[pl.DataFrame]' = None, fox: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the WNBA cross-source team crosswalk (ESPN / WNBA Stats / Fox).

One row per ESPN team, keyed on `espn_team_id`. The Stats side is
derived from the season schedule's home/away team fields (as in wehoop)
and joined on the normalized `city + name`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year (e.g. `2026`). Defaults to the most recent WNBA season. |
| `stats` | `Optional[DataFrame]` | `None` | Pre-fetched Stats team directory. `None` derives it from the Stats schedule. |
| `fox` | `Optional[DataFrame]` | `None` | Pre-fetched `fox_wnba_teams()` frame. `None` fetches live. |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |

**Returns**

`pl.DataFrame` (or pandas), one row per ESPN team, with `TEAM_COLUMNS`.

**Example**

```python
from sportsdataverse.wnba import wnba_team_crosswalk
df = wnba_team_crosswalk(season=2026)
print(df.shape)

# Offline with pre-fetched provider frames

df = wnba_team_crosswalk(season=2026, stats=my_stats, fox=my_fox)

# Pipeline next step (one line)

df.select("espn_team_id", "wnba_team_id", "match_method").head()
```
