---
title: "WNBA — additional Python functions — IDs and crosswalks"
sidebar_label: "IDs and crosswalks"
sidebar_position: 9
description: "WNBA — additional Python functions — IDs and crosswalks — function reference in sdv-py, the SportsDataverse Python package."
---
# WNBA — additional Python functions — IDs and crosswalks

### wnba_crosswalks_from_archive {#wnba_crosswalks_from_archive}

`wnba_crosswalks_from_archive(season: 'int', espn_schedule: 'pl.DataFrame', espn_game_rosters: 'pl.DataFrame', stats_dir: 'Union[str, Path]', min_confidence: 'float' = 0.92, *, fox: 'bool' = False, fox_games: 'bool' = False, yahoo: 'bool' = False, alt_root: 'Optional[Union[str, Path]]' = None) -> 'Dict[str, pl.DataFrame]'`

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
All-Star and other exhibition sides are left out of the team and player
crosswalks; their games stay in the schedule. Yahoo columns stay null
unless `yahoo=True` (Yahoo WNBA team ids differ from ESPN's), and Fox
columns stay null unless `fox=True`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season (calendar year). |
| `espn_schedule` | `DataFrame` |  | That season's ESPN schedule parquet. |
| `espn_game_rosters` | `DataFrame` |  | That season's ESPN game rosters parquet. |
| `stats_dir` | `Union[str, Path]` |  | The stats archive's `json` root. |
| `min_confidence` | `float` | `0.92` | Jaro-Winkler floor for a fuzzy player match. |
| `fox` | `bool` | `False` | Fetch the Fox legs live (the only network call). `fox_team_id` / `fox_team_name` join on the normalized team name, so they fill for any season whose franchise names match today's Fox names. The Fox player columns fill only for the current season (Fox rosters are today's) and stay null otherwise. A failed Fox fetch logs a warning and leaves the columns null; it never fails the build. |
| `fox_games` | `bool` | `False` | Fill `fox_game_id` and the Fox home/away team ids from Fox's scoreboard, one Fox call per game date, matched on US-Eastern date and teams (either home/away order), for any season Fox serves. Fox teams resolve through `fox_team_id` (with `fox`) or the season's ESPN team names. A failed fetch logs a warning and leaves the columns null. |
| `yahoo` | `bool` | `False` | Fill `yahoo_team_id` / `yahoo_team_abbreviation` / `yahoo_team_name` from Yahoo's team listing (one call), joined on team name in tiers, so a relocated or renamed franchise stays null; and `yahoo_game_id` = `wnba.g.{YYYYMMDD ET}{home Yahoo id, 3 digits}`, kept only where Yahoo lists that id for the season (one call per week). A failed fetch logs a warning and leaves the columns null; it never fails the build. |
| `alt_root` | `Optional[Union[str, Path]]` | `None` | The league's `*-alt-data` checkout (e.g. `wehoop-wnba-alt-data`). When set, the Fox and Yahoo player columns are filled offline from its parsed history (`{provider}/game_rosters` and `{provider}/games` parquet for `season`): provider games join ESPN's on (US-Eastern date, team pair) and players match inside each game and team, voted across the season. Only null ids are filled; a provider with no parsed season is skipped. |

**Returns**

`{"team": ..., "schedule": ..., "player": ...}`, with the same columns as `wnba_team_crosswalk`, `wnba_schedule_crosswalk` and `wnba_player_crosswalk`.

**Raises**

| Exception | Description |
|---|---|
| `FileNotFoundError` | `stats_dir` holds no box scores for `season`. |

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

**Raises**

| Exception | Description |
|---|---|
| `CrosswalkSourceError` | A Stats team-directory or `commonteamroster` call for any team failed or was refused (stats.wnba.com answers a throttle or block with an empty body), so `wnba_*` would otherwise be silently null. Throttles are retried only when `SDV_PY_NBA_STATS_RETRIES` is set (default `0`, backoff `SDV_PY_NBA_STATS_BACKOFF`); without it one transient refusal aborts the whole build. |
| `CrosswalkSourceError` | The per-team ESPN or Fox rosters failed every per-item fetch and answered none -- the signature of an unreachable or rate-limited host -- or, with `strict`, any one fetch failed. Isolated failures are skipped and logged as a warning. |

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

`wnba_schedule_crosswalk(season: 'Optional[int]' = None, *, stats_games: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, strict: 'bool' = False, fox: 'bool' = False, yahoo: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the WNBA cross-source schedule crosswalk (ESPN / WNBA Stats / Fox / Yahoo).

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
| `fox` | `bool` | `False` | Fill `fox_game_id` / `fox_home_team_id` / `fox_away_team_id` from Fox's scoreboard, one Fox call per game date. A Fox game matches the row with the same US-Eastern date and the same teams (in either home/away order); the Fox home/away ids are Fox's own. A failed Fox fetch logs a warning and leaves the columns null. |
| `yahoo` | `bool` | `False` | Fill `yahoo_game_id` = `wnba.g.{YYYYMMDD ET}{home Yahoo id, 3 digits}`, checked against the ids Yahoo lists for the season's dates so a neutral-site game takes Yahoo's home side and a game Yahoo does not list stays null. The Yahoo team ids come from `wnba_team_crosswalk` with `yahoo=True`. A failed Yahoo fetch logs a warning; without the team listing the ids stay null, without the id list the unverified home-side id is kept. |

**Returns**

`pl.DataFrame` (or pandas) with `SCHEDULE_COLUMNS`.

No returns table is published for this function: no capture: it reads stats.wnba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Raises**

| Exception | Description |
|---|---|
| `CrosswalkSourceError` | The per-date ESPN scoreboard failed every per-item fetch and answered none -- the signature of an unreachable or rate-limited host -- or, with `strict`, any one fetch failed. Isolated failures are skipped and logged as a warning. |

**Example**

```python
from sportsdataverse.wnba import wnba_schedule_crosswalk
df = wnba_schedule_crosswalk(season=2026)
print(df["match_method"].value_counts())

# Pipeline next step (one line)

df.filter(pl.col("match_method") == "both").select("espn_game_id", "wnba_game_id").head()
```

### wnba_team_crosswalk {#wnba_team_crosswalk}

`wnba_team_crosswalk(season: 'Optional[int]' = None, *, stats: 'Optional[pl.DataFrame]' = None, fox: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, yahoo: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the WNBA cross-source team crosswalk (ESPN / WNBA Stats / Fox / Yahoo).

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
| `yahoo` | `bool` | `False` | Fill `yahoo_team_id` / `yahoo_team_abbreviation` / `yahoo_team_name` from Yahoo's team listing (one call), joined on the normalized name. A failed Yahoo fetch logs a warning and leaves them null; Yahoo teams no ESPN team took are logged. |

**Returns**

`pl.DataFrame` (or pandas), one row per ESPN team, with `TEAM_COLUMNS`.

No returns table is published for this function: no capture: it reads stats.wnba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Raises**

| Exception | Description |
|---|---|
| `CrosswalkSourceError` | A source that was not passed in pre-fetched could not be produced (Fox or the Stats schedule). Building on a missing source would emit a well-formed crosswalk whose `fox_*` / `wnba_*` columns are silently all-null, so it fails here instead. |

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
