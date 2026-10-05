---
title: "NFL — additional Python functions — Scrape"
sidebar_label: "Scrape"
sidebar_position: 12
description: "NFL — additional Python functions — Scrape — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Scrape

### scrape_ngs_season {#scrape_ngs_season}

`scrape_ngs_season(stat_type: 'str', season: 'int', *, include_season_totals: 'bool' = True, return_as_pandas: 'bool' = False)`

Scrape a full season of NGS statboard data, shaped like the nflverse parquet.

Port of nflverse ngs-data's R `save_ngs_type`: loop the regular-season weeks
(`1..max_reg` where `max_reg = 18` for `season >= 2021` else `17`) plus
the playoff weeks (`max_reg+1 .. max_reg+5`, fetched with
`season_type="POST"`), stack them diagonally, and -- when
`include_season_totals` -- prepend the season-aggregate rows (NGS `week=0`,
a `REG` call with no `week` param) tagged `week=0`. Duplicate rows (NGS
returned dupes for some 2022 weeks) are de-duplicated on
`(season, week, player_gsis_id)`.

Output columns match the published nflverse NGS parquet read by
`sportsdataverse.nfl.load_nfl_nextgen_stats` (snake_case, `team_abbr`
resolved). It will not be byte-identical -- nflverse post-processes (column
pruning / ordering) -- but the core metric columns and the
player/team/week keys align.

NGS statboard rows are **player-week aggregates**, NOT per-play rows.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stat_type` | `str` |  | one of `"passing"`, `"rushing"`, `"receiving"`. |
| `season` | `int` |  | season year (NGS coverage starts in 2016). |
| `include_season_totals` | `bool` | `True` | also fetch the season-aggregate (`week=0`) rows. Defaults to `True` (matches ngs-data, whose week loop starts at 0). |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame` stacking every week (and, by default, the season totals) for the requested `stat_type` and `season`. An EMPTY frame carrying the documented key schema if nothing is returned.

| col_name | type | description |
|---|---|---|
| `aggressiveness` | double | Percentage of pass attempts thrown into tight windows (defender within one yard of the receiver at completion or incompletion), from NFL Next Gen Stats. Passing only. |
| `attempts` | integer | Pass attempts (including incompletions) recorded for the passer during the slice. Passing only. |
| `avg_air_distance` | double | Average air distance in yards on all pass attempts, measuring how far the ball travels through the air regardless of direction. Passing only. |
| `avg_air_yards_differential` | double | Average difference between intended air yards and completed air yards, measuring accuracy relative to target depth. Passing only. |
| `avg_air_yards_to_sticks` | double | Average air yards relative to the first-down marker on pass attempts (positive = beyond the sticks). Passing only. |
| `avg_completed_air_yards` | double | Average air yards on completed passes only, measuring depth of actual completions. Passing only. |
| `avg_intended_air_yards` | double | Average depth of target on all pass attempts, regardless of completion. Passing/receiving. |
| `avg_time_to_throw` | double | Average time in seconds from snap to release for the passer, as tracked by NFL Next Gen Stats. Passing only. |
| `completion_percentage` | double | Actual completion percentage for the passer during the slice. Passing only. |
| `completion_percentage_above_expectation` | double | Completion percentage above the model-expected completion rate (CPOE/CPAE). Passing only. |
| `completions` | integer | Completed passes for the passer during the slice. Passing only. |
| `expected_completion_percentage` | double | Model-expected completion percentage based on target depth, separation, and coverage, from NFL Next Gen Stats. Passing only. |
| `games_played` | integer | Number of games played by the player during the slice covered by the row. |
| `interceptions` | integer | Interceptions thrown by the passer during the slice. Passing only. |
| `max_air_distance` | double | Maximum air distance in yards recorded on any single pass attempt during the slice. Passing only. |
| `max_completed_air_distance` | double | Maximum air distance in yards recorded on any single completed pass during the slice. Passing only. |
| `pass_touchdowns` | integer | Passing touchdowns thrown by the passer during the slice. Passing only. |
| `pass_yards` | integer | Total passing yards accumulated by the passer during the slice. Passing only. |
| `passer_rating` | double | NFL passer rating (0–158.3 scale) for the passer during the slice. Passing only. |
| `player_name` | character | Display name of the player as returned at the top-level statboard row. |
| `season` | integer | NFL season year for the row. |
| `season_type` | character | Season segment for the row ('REG' for regular-season weeks and the week-0 aggregate, 'POST' for playoff weeks). |
| `week` | integer | NFL Next Gen Stats week tag; 0 is the season aggregate, 1..max_reg are regular-season weeks, and higher values are continuous playoff weeks. |
| `position` | character | Player position as returned at the top-level statboard row (e.g., 'QB', 'WR', 'RB'). |
| `team_id` | character | NFL Next Gen Stats team identifier for the player's team on this row; the key joined to resolve team_abbr. |
| `player_season` | integer | NFL season year recorded in the nested player record. |
| `player_season_type` | character | Season segment recorded in the nested player record. |
| `player_week` | integer | Week recorded in the nested player record (the loop week tag overrides this on the top-level week column). |
| `player_jersey_number` | integer | Jersey number worn by the player. |
| `player_last_name` | character | Last name of the player. |
| `player_football_name` | character | Football name used by the player, which may differ from the legal first name. |
| `player_first_name` | character | First name of the player. |
| `player_position` | character | Player position from the nested player record. |
| `player_gsis_it_id` | integer | NFL GSIS internal tracking integer identifier for the player. |
| `player_gsis_id` | character | NFL GSIS (Game Statistics and Information System) identifier, the primary nflverse player key. |
| `player_esb_id` | character | Elias Sports Bureau (ESB) identifier for the player. |
| `player_display_name` | character | Full display name of the player as used in NFL Next Gen Stats records. |
| `player_short_name` | character | Shortened display name for the player (e.g., 'P.Mahomes'). |
| `player_uniform_number` | character | Uniform number worn by the player, stored as a string to preserve leading zeros if applicable. |
| `player_status` | character | Current roster status of the player (e.g., 'ACT' for active, 'IR' for injured reserve). |
| `player_current_team_id` | character | NFL Next Gen Stats team identifier for the team the player is currently rostered on. |
| `player_smart_id` | character | NFL Next Gen Stats smart (UUID-style) identifier for the player. |
| `player_headshot` | character | URL template for the player's headshot image. |
| `player_position_group` | character | Broad position group for the player (e.g., 'QB', 'WR') in the NGS player record. |
| `player_ngs_position` | character | Player's position as classified by the NFL Next Gen Stats system. |
| `player_ngs_position_group` | character | Broader position group the player belongs to as classified by the NFL Next Gen Stats system. |
| `team_abbr` | character | Team abbreviation resolved from team_id via the NGS team directory (relocated franchise abbreviations dropped to keep the mapping one-to-one). |

**Example**

```python
from sportsdataverse.nfl import scrape_ngs_season
pas = scrape_ngs_season("passing", 2023)
pas.select(["season", "week", "player_display_name", "team_abbr"]).head()

# Regular-season weeks only (skip the week-0 totals)

wk = scrape_ngs_season("receiving", 2023, include_season_totals=False)

# Column-compatible with the published parquet

from sportsdataverse.nfl import load_nfl_nextgen_stats
published = load_nfl_nextgen_stats(seasons=[2023], stat_type="passing")
shared = set(pas.columns) & set(published.columns)
```

### scrape_ngs_week {#scrape_ngs_week}

`scrape_ngs_week(stat_type: 'str', season: 'int', week: 'int', season_type: 'str' = 'REG', *, return_as_pandas: 'bool' = False)`

Scrape one (season, week) NGS statboard slice, shaped like the nflverse parquet.

Port of nflverse ngs-data's R `load_week_ngs`: fetch a single statboard
slice via `nfl_ngs_statboard`, resolve `team_abbr` from the team
directory, snake-case every column, and tag the row with the loop `week`.
`week=0` is the season-aggregate row (a `season_type="REG"` call with no
`week` query param); weeks `1..max_reg` are regular-season, and the
playoff weeks (`max_reg+1` upward) are fetched with `season_type="POST"`.

NGS statboard rows are **player-week aggregates** (`avg_intended_air_yards`,
`completion_percentage_above_expectation`, `avg_time_to_throw`, ...), NOT
per-play rows -- this is a season-stats source, not a per-play air-yards /
completion-probability source.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stat_type` | `str` |  | one of `"passing"`, `"rushing"`, `"receiving"`. |
| `season` | `int` |  | season year (NGS coverage starts in 2016). |
| `week` | `int` |  | NGS week. `0` -> season aggregate; `1..max_reg` -> REG; higher -> POST. The supplied value is what tags the returned rows. |
| `season_type` | `str` | `'REG'` | `"REG"` or `"POST"`; the caller (or `scrape_ngs_season`) selects this per week. Defaults to `"REG"`. |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame` of player-week NGS rows with snake-cased columns + a resolved `team_abbr`. An EMPTY frame carrying the documented key schema (not an exception) when the API yields no stats.

| col_name | type | description |
|---|---|---|
| `aggressiveness` | double | Percentage of pass attempts thrown into tight windows (defender within one yard of the receiver at completion or incompletion), from NFL Next Gen Stats. Passing only. |
| `attempts` | integer | Pass attempts (including incompletions) recorded for the passer during the week. Passing only. |
| `avg_air_distance` | double | Average air distance in yards on all pass attempts, measuring how far the ball travels through the air regardless of direction. Passing only. |
| `avg_air_yards_differential` | double | Average difference between intended air yards and completed air yards, measuring accuracy relative to target depth. Passing only. |
| `avg_air_yards_to_sticks` | double | Average air yards relative to the first-down marker on pass attempts (positive = beyond the sticks). Passing only. |
| `avg_completed_air_yards` | double | Average air yards on completed passes only, measuring depth of actual completions. Passing only. |
| `avg_intended_air_yards` | double | Average depth of target on all pass attempts, regardless of completion. Passing/receiving. |
| `avg_time_to_throw` | double | Average time in seconds from snap to release for the passer, as tracked by NFL Next Gen Stats. Passing only. |
| `completion_percentage` | double | Actual completion percentage for the passer during the week. Passing only. |
| `completion_percentage_above_expectation` | double | Completion percentage above the model-expected completion rate (CPOE/CPAE). Passing only. |
| `completions` | integer | Completed passes for the passer during the week. Passing only. |
| `expected_completion_percentage` | double | Model-expected completion percentage based on target depth, separation, and coverage, from NFL Next Gen Stats. Passing only. |
| `games_played` | integer | Number of games played by the player during the slice covered by the row. |
| `interceptions` | integer | Interceptions thrown by the passer during the week. Passing only. |
| `max_air_distance` | double | Maximum air distance in yards recorded on any single pass attempt during the week. Passing only. |
| `max_completed_air_distance` | double | Maximum air distance in yards recorded on any single completed pass during the week. Passing only. |
| `pass_touchdowns` | integer | Passing touchdowns thrown by the passer during the week. Passing only. |
| `pass_yards` | integer | Total passing yards accumulated by the passer during the week. Passing only. |
| `passer_rating` | double | NFL passer rating (0–158.3 scale) for the passer during the week. Passing only. |
| `player_name` | character | Display name of the player as returned at the top-level statboard row. |
| `season` | integer | NFL season year for the row. |
| `season_type` | character | Season segment for the row ('REG' or 'POST') as supplied by the caller. |
| `week` | integer | NFL Next Gen Stats week tag supplied by the caller; 0 is the season aggregate, 1..max_reg are regular-season weeks, and higher values are continuous playoff weeks. |
| `position` | character | Player position as returned at the top-level statboard row (e.g., 'QB', 'WR', 'RB'). |
| `team_id` | character | NFL Next Gen Stats team identifier for the player's team on this row; the key joined to resolve team_abbr. |
| `player_season` | integer | NFL season year recorded in the nested player record. |
| `player_season_type` | character | Season segment recorded in the nested player record. |
| `player_week` | integer | Week recorded in the nested player record (the loop week tag overrides this on the top-level week column). |
| `player_jersey_number` | integer | Jersey number worn by the player. |
| `player_last_name` | character | Last name of the player. |
| `player_football_name` | character | Football name used by the player, which may differ from the legal first name. |
| `player_first_name` | character | First name of the player. |
| `player_position` | character | Player position from the nested player record. |
| `player_gsis_it_id` | integer | NFL GSIS internal tracking integer identifier for the player. |
| `player_gsis_id` | character | NFL GSIS (Game Statistics and Information System) identifier, the primary nflverse player key. |
| `player_esb_id` | character | Elias Sports Bureau (ESB) identifier for the player. |
| `player_display_name` | character | Full display name of the player as used in NFL Next Gen Stats records. |
| `player_short_name` | character | Shortened display name for the player (e.g., 'P.Mahomes'). |
| `player_uniform_number` | character | Uniform number worn by the player, stored as a string to preserve leading zeros if applicable. |
| `player_status` | character | Current roster status of the player (e.g., 'ACT' for active, 'IR' for injured reserve). |
| `player_current_team_id` | character | NFL Next Gen Stats team identifier for the team the player is currently rostered on. |
| `player_smart_id` | character | NFL Next Gen Stats smart (UUID-style) identifier for the player. |
| `player_headshot` | character | URL template for the player's headshot image. |
| `player_position_group` | character | Broad position group for the player (e.g., 'QB', 'WR') in the NGS player record. |
| `player_ngs_position` | character | Player's position as classified by the NFL Next Gen Stats system. |
| `player_ngs_position_group` | character | Broader position group the player belongs to as classified by the NFL Next Gen Stats system. |
| `team_abbr` | character | Team abbreviation resolved from team_id via the NGS team directory (relocated franchise abbreviations dropped to keep the mapping one-to-one). |

**Example**

```python
from sportsdataverse.nfl import scrape_ngs_week
wk1 = scrape_ngs_week("passing", 2023, week=1)
wk1.select(["season", "week", "player_display_name", "team_abbr"]).head()

# Season-aggregate row (week 0)

tot = scrape_ngs_week("rushing", 2023, week=0)
```
