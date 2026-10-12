---
title: "WNBA — additional Python functions — IDs and crosswalks"
sidebar_label: "IDs and crosswalks"
sidebar_position: 9
description: "WNBA — additional Python functions — IDs and crosswalks — function reference in sdv-py, the SportsDataverse Python package."
---
# WNBA — additional Python functions — IDs and crosswalks

### wnba_crosswalks_from_archive {#wnba_crosswalks_from_archive}

`wnba_crosswalks_from_archive(season: 'int', espn_schedule: 'pl.DataFrame', espn_game_rosters: 'pl.DataFrame', stats_dir: 'Union[str, Path]', min_confidence: 'float' = 0.92) -> 'Dict[str, pl.DataFrame]'`

Build the WNBA team, schedule and player crosswalks for a past season, offline.

The live builders read current-season endpoints, so they cannot produce
history. This one reads stored data only: the season's ESPN schedule and
ESPN game rosters (`espn_wnba_schedules` / `espn_wnba_game_rosters`
parquet, 2002+) and the local stats.wnba.com JSON archive
(`wehoop-wnba-stats-raw/wnba_stats/json`: `boxscoretraditionalv3`, with
`boxscoresummaryv2` / `leaguegamelog` for the game date).

Teams pair by game-date co-occurrence (`match_method` `game_dates`),
games join on (US-Eastern date, home ESPN team, away ESPN team), and
players match inside each matched (game, team) on normalized name and
jersey, then vote across the season into one row per ESPN athlete per
team (`match_confidence` = the share of matched games that agree).
Fox and Yahoo columns stay null: Yahoo WNBA team ids differ from ESPN's.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season (calendar year). |
| `espn_schedule` | `DataFrame` |  | That season's ESPN schedule parquet. |
| `espn_game_rosters` | `DataFrame` |  | That season's ESPN game rosters parquet. |
| `stats_dir` | `Union[str, Path]` |  | The stats archive's `json` root. |
| `min_confidence` | `float` | `0.92` | Jaro-Winkler floor for a fuzzy player match. |

**Returns**

`{"team": ..., "schedule": ..., "player": ...}`, with the same columns as `wnba_team_crosswalk`, `wnba_schedule_crosswalk` and `wnba_player_crosswalk`.

**Example**

```python
import polars as pl
from sportsdataverse.wnba import wnba_crosswalks_from_archive

out = wnba_crosswalks_from_archive(
    2015,
    pl.read_parquet("wnba/schedules/parquet/wnba_schedule_2015.parquet"),
    pl.read_parquet("wnba/game_rosters/parquet/game_rosters_2015.parquet"),
    "../wehoop-wnba-stats-raw/wnba_stats/json",
)
print(out["player"]["match_method"].value_counts())

# Pipeline next step (one line)

out["schedule"].filter(pl.col("match_method") != "both").head()
```

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

No returns table is published for this function: no capture: it reads stats.wnba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

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

No returns table is published for this function: no capture: it reads stats.wnba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

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

No returns table is published for this function: no capture: it reads stats.wnba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

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
