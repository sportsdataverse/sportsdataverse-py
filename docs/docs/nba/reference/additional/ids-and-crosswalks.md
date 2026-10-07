---
title: "NBA — additional Python functions — IDs and crosswalks"
sidebar_label: "IDs and crosswalks"
sidebar_position: 15
description: "NBA — additional Python functions — IDs and crosswalks — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — IDs and crosswalks

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

`nba_schedule_crosswalk(season: 'Optional[int]' = None, *, stats_games: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, strict: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the NBA cross-source schedule crosswalk (ESPN / NBA Stats).

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

**Returns**

`pl.DataFrame` (or pandas) with `SCHEDULE_COLUMNS`.

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
| `yahoo_team_id` | character | Yahoo team id (NA placeholder). |
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
