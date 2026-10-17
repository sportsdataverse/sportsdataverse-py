---
title: "NBA — additional Python functions — IDs and crosswalks"
sidebar_label: "IDs and crosswalks"
sidebar_position: 15
description: "NBA — additional Python functions — IDs and crosswalks — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — IDs and crosswalks

### nba_crosswalks_from_archive {#nba_crosswalks_from_archive}

`nba_crosswalks_from_archive(season: 'int', espn_schedule: 'pl.DataFrame', espn_game_rosters: 'pl.DataFrame', stats_dir: 'Union[str, Path]', min_confidence: 'float' = 0.92, *, fox: 'bool' = False, fox_games: 'bool' = False, alt_root: 'Optional[Union[str, Path]]' = None) -> 'Dict[str, pl.DataFrame]'`

Build the NBA team, schedule and player crosswalks for a past season, offline.

The live builders read current-season endpoints, so they cannot produce
history. This one reads stored data only: the season's ESPN schedule and
ESPN game rosters (`espn_nba_schedules` / `espn_nba_game_rosters`
parquet, 2002+) and the local stats.nba.com JSON archive
(`hoopR-nba-stats-raw/nba_stats/json`: `boxscoretraditionalv3`, with
`boxscoresummaryv2` / `leaguegamelog` for the game date).

* **Teams** pair by game-date co-occurrence (`match_method`
  `game_dates`; `match_confidence` is the share of the ESPN team's
  home/away dates the Stats team shares), so relocated franchises resolve
  without a name table.
* **Games** join on (US-Eastern date, home ESPN team, away ESPN team), as
  `nba_schedule_crosswalk` does (`both` / `espn_only` /
  `stats_only`).
* **Players** match inside each matched (game, team) on normalized name,
  tie-broken by jersey, then vote across the season into one row per ESPN
  athlete per team. `match_confidence` is the share of that athlete's
  matched games that agree on the chosen `nba_player_id`.

All-Star and other exhibition sides are left out of the team and player
crosswalks (they never play a franchise); their games stay in the
schedule. Yahoo team and game ids are computed from 2014 on; Yahoo player
columns stay null unless `alt_root` is set, and Fox columns stay null
unless `fox=True` or `alt_root` is set.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season, ending year (`2025` = 2024-25). |
| `espn_schedule` | `DataFrame` |  | That season's ESPN schedule parquet. |
| `espn_game_rosters` | `DataFrame` |  | That season's ESPN game rosters parquet. |
| `stats_dir` | `Union[str, Path]` |  | The stats archive's `json` root. |
| `min_confidence` | `float` | `0.92` | Jaro-Winkler floor for a fuzzy player match. |
| `fox` | `bool` | `False` | Fetch the Fox legs live (the only network call). `fox_team_id` / `fox_team_name` join on the normalized team name, so they fill for any season whose franchise names match today's Fox names. The Fox player columns fill only for the current season (Fox rosters are today's) and stay null otherwise. A failed Fox fetch logs a warning and leaves the columns null; it never fails the build. |
| `fox_games` | `bool` | `False` | Fill `fox_game_id` and the Fox home/away team ids from Fox's scoreboard, one Fox call per game date, matched on US-Eastern date and teams (either home/away order), for any season Fox serves. Fox teams resolve through `fox_team_id` (with `fox`) or the season's ESPN team names. A failed fetch logs a warning and leaves the columns null. |
| `alt_root` | `Optional[Union[str, Path]]` | `None` | The league's `*-alt-data` checkout (e.g. `hoopR-nba-alt-data`). When set, the Fox and Yahoo player columns are filled offline from its parsed history (`{provider}/game_rosters` and `{provider}/games` parquet for `season`): provider games join ESPN's on (US-Eastern date, team pair) and players match inside each game and team, voted across the season. Only null ids are filled; a provider with no parsed season is skipped. |

**Returns**

`{"team": ..., "schedule": ..., "player": ...}`, with the same columns as `nba_team_crosswalk`, `nba_schedule_crosswalk` and `nba_player_crosswalk`.

**Example**

```python
import polars as pl
from sportsdataverse.nba import nba_crosswalks_from_archive

out = nba_crosswalks_from_archive(
    2015,
    pl.read_parquet("nba/schedules/parquet/nba_schedule_2015.parquet"),
    pl.read_parquet("nba/game_rosters/parquet/game_rosters_2015.parquet"),
    "../hoopR-nba-stats-raw/nba_stats/json",
)
print(out["schedule"]["match_method"].value_counts())

# Pipeline next step (one line)

out["player"].filter(pl.col("match_method") == "unmatched").head()
```

### nba_player_crosswalk {#nba_player_crosswalk}

`nba_player_crosswalk(season: 'Optional[int]' = None, min_confidence: 'float' = 0.92, *, return_as_pandas: 'bool' = False, strict: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the NBA cross-source player crosswalk (ESPN / NBA Stats / Fox).

One row per ESPN athlete per team. `match_method` / `match_confidence`
describe the **Stats API** match (normalized exact name, then
Jaro-Winkler with jersey and DOB tiebreaks); Fox contributes
`fox_athlete_id` only.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year per hoopR convention. Defaults to the most recent NBA season. |
| `min_confidence` | `float` | `0.92` | Jaro-Winkler floor for fuzzy matches (R default 0.92). |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |
| `strict` | `bool` | `False` | Raise on the first failed per-team ESPN or Fox roster fetch (a 404 is still skipped) instead of skipping isolated failures. Default `False` matches the R producers; a provider whose every item failed raises either way. An item the host *answered* -- including a 404 -- counts as answered. |

**Returns**

`pl.DataFrame` (or pandas), one row per ESPN athlete, 21 columns.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season year. |
| `espn_team_id` | integer | ESPN team id (canonical key). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `player_name` | character | Player name. |
| `espn_athlete_id` | character | ESPN athlete id. |
| `espn_full_name` | character | ESPN full name. |
| `espn_jersey` | character | ESPN jersey number. |
| `espn_position` | character | ESPN position abbreviation. |
| `nba_player_id` | character | NBA Stats API (stats.nba.com) player id as a string, matched to the ESPN athlete within the same team by normalized exact name, then Jaro-Winkler fuzzy name match (min_confidence, default 0.92) with jersey and birth-date tiebreaks; null when the athlete had no Stats match. |
| `nba_player_name` | character | Player name from the NBA Stats commonteamroster row matched to the ESPN athlete; null when the athlete had no Stats match. |
| `nba_jersey_num` | character | Jersey number as a string from the NBA Stats commonteamroster row matched to the ESPN athlete; null when the athlete had no Stats match. |
| `nba_position` | character | Position from the NBA Stats commonteamroster row matched to the ESPN athlete; null when the athlete had no Stats match. |
| `fox_athlete_id` | character | Fox athlete id (NA if unmatched). |
| `fox_player` | character | Fox player name (NA if unmatched). |
| `fox_jersey` | character | Fox jersey number (NA if unmatched). |
| `fox_position_group` | character | Fox position group label (NA if unmatched). |
| `yahoo_player_id` | character | Yahoo player id (NA placeholder). |
| `yahoo_player_name` | character | Yahoo player name (NA placeholder). |
| `match_method` | character | Combination of matched sources, e.g. "fox+bart" / "fox_only" / "bart_only" / "espn_only". |
| `match_confidence` | double | Jaro-Winkler score or 1 for exact (NA if none). |
| `match_keys` | character | NA (reserved for future use). |

**Example**

```python
from sportsdataverse.nba import nba_player_crosswalk
df = nba_player_crosswalk(season=2026)
print(df["match_method"].value_counts())

# Tighten the fuzzy floor

strict = nba_player_crosswalk(season=2026, min_confidence=0.97)

# Pipeline next step (one line)

df.filter(pl.col("match_method") == "fuzzy_jw").head()
```

### nba_schedule_crosswalk {#nba_schedule_crosswalk}

`nba_schedule_crosswalk(season: 'Optional[int]' = None, *, stats_games: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, strict: 'bool' = False, fox: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the NBA cross-source schedule crosswalk (ESPN / NBA Stats / Fox).

One row per game. Both sides reduce to the Eastern-Time game date before
joining on `(game_date, home_espn_team_id, away_espn_team_id)`. The
Stats CDN serves the current season only, so the live builder is
effectively current-season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year per hoopR convention. Defaults to the most recent NBA season. |
| `stats_games` | `Optional[DataFrame]` | `None` | Pre-fetched Stats schedule frame; `None` fetches live. |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |
| `strict` | `bool` | `False` | Raise on the first failed per-date ESPN scoreboard fetch (a 404 is still skipped) instead of skipping isolated failures. Default `False` matches the R producers; a provider whose every item failed raises either way. An item the host *answered* -- including a 404 -- counts as answered. |
| `fox` | `bool` | `False` | Fill `fox_game_id` / `fox_home_team_id` / `fox_away_team_id` from Fox's scoreboard, one Fox call per game date. A Fox game matches the row with the same US-Eastern date and the same teams (in either home/away order); the Fox home/away ids are Fox's own. A failed Fox fetch logs a warning and leaves the columns null. |

**Returns**

`pl.DataFrame` (or pandas) with `SCHEDULE_COLUMNS`.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_schedule_crosswalk
df = nba_schedule_crosswalk(season=2026)
print(df["match_method"].value_counts())

# Pipeline next step (one line)

df.filter(pl.col("match_method") == "both").select("espn_game_id", "nba_game_id").head()
```

### nba_team_crosswalk {#nba_team_crosswalk}

`nba_team_crosswalk(season: 'Optional[int]' = None, *, stats: 'Optional[pl.DataFrame]' = None, fox: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the NBA cross-source team crosswalk (ESPN / NBA Stats / Fox).

One row per ESPN team, keyed on `espn_team_id`. ESPN and Stats team
endpoints are current-season snapshots, so `season` is a stamp;
historical relocations are not back-modelled.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year per hoopR convention (`2026` = 2025-26). Defaults to the most recent NBA season. |
| `stats` | `Optional[DataFrame]` | `None` | Pre-fetched Stats team directory (`espn_team_id` + `nba_team_*`). `None` derives it from `nba_stats_leaguestandingsv3` joined to ESPN on the normalized team nickname. |
| `fox` | `Optional[DataFrame]` | `None` | Pre-fetched Fox directory. `None` fetches live. |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |

**Returns**

`pl.DataFrame` (or pandas), one row per ESPN team, with `TEAM_COLUMNS`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season year. |
| `espn_team_id` | integer | ESPN team id (canonical key). |
| `espn_abbreviation` | character | ESPN abbreviation. |
| `espn_display_name` | character | ESPN display name (school + mascot). |
| `espn_short_name` | character | ESPN short name. |
| `espn_location` | character | ESPN school/location only. |
| `espn_mascot` | character | ESPN mascot/nickname. |
| `nba_team_id` | character | NBA Stats API (stats.nba.com) team id as a string, attached to the ESPN team row on espn_team_id after the Stats team nickname is matched to ESPN's short_name; null when no Stats team matched the ESPN team. |
| `nba_team_abbreviation` | character | NBA Stats team tricode, taken from nba_stats_leaguegamelog's team_abbreviation because leaguestandingsv3 publishes none; null when no Stats team matched the ESPN team. |
| `nba_team_name` | character | Full NBA Stats team name built as team_city plus team_name (city then nickname) from nba_stats_leaguestandingsv3; null when no Stats team matched the ESPN team. |
| `nba_team_city` | character | Team city (team_city) from nba_stats_leaguestandingsv3; null when no Stats team matched the ESPN team. |
| `nba_team_slug` | character | URL slug for the team (team_slug) from nba_stats_leaguestandingsv3; null when no Stats team matched the ESPN team. |
| `nba_conference` | character | Team's conference as NBA Stats labels it (conference) from nba_stats_leaguestandingsv3; null when no Stats team matched the ESPN team. |
| `nba_division` | character | Team's division as NBA Stats labels it (division) from nba_stats_leaguestandingsv3; null when no Stats team matched the ESPN team. |
| `fox_team_id` | character | Fox Bifrost team id (NA if unmatched). |
| `fox_team_name` | character | Fox team name (NA if unmatched). |
| `yahoo_team_id` | character | Yahoo Sports team id, nba.t.<espn_team_id> (Yahoo's NBA ids equal ESPN's), from the 2014 season on; null for earlier seasons. |
| `yahoo_team_abbreviation` | character | Yahoo abbreviation (NA placeholder). |
| `yahoo_team_name` | character | Yahoo team name (NA placeholder). |
| `match_method` | character | Combination of matched sources, e.g. "fox+bart" / "fox_only" / "bart_only" / "espn_only". |
| `match_confidence` | double | Jaro-Winkler score or 1 for exact (NA if none). |

**Example**

```python
from sportsdataverse.nba import nba_team_crosswalk
df = nba_team_crosswalk(season=2026)
print(df.shape)

# Offline with a pre-fetched Stats frame

df = nba_team_crosswalk(season=2026, stats=my_stats, fox=my_fox)

# Pipeline next step (one line)

df.select("espn_team_id", "nba_team_id", "match_method").head()
```
