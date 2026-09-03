---
title: MBB — KenPom (kenpom.com, subscription)
sidebar_label: KenPom (kenpom.com, subscription)
description: "MBB — KenPom (kenpom.com, subscription) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 11
---
# MBB — KenPom (kenpom.com, subscription)

`sportsdataverse.mbb` — 30 endpoints.

## `kenpom_ratings`

GET /index.php - Pomeroy season ratings (AdjEM/AdjO/AdjD/AdjT plus SOS, one row per team). Port of hoopR kp_pomeroy_ratings().

**Endpoint URL:** `GET https://kenpom.com/index.php`

**Valid URL:** [https://kenpom.com/index.php?y=2025](https://kenpom.com/index.php?y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year (2025 = the 2024-25 season). Data begins at 2002. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_ratings(year=2025)
```

_Last validated n/a._

## `kenpom_efficiency`

GET /summary.php - efficiency and tempo summary (adjusted and raw O/D/T, average possession length). Port of hoopR kp_efficiency().

**Endpoint URL:** `GET https://kenpom.com/summary.php`

**Valid URL:** [https://kenpom.com/summary.php?y=2025](https://kenpom.com/summary.php?y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. Columns are narrower before 2010. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_efficiency(year=2025)
```

_Last validated n/a._

## `kenpom_four_factors`

GET /stats.php - four-factors rankings (eFG%, TO%, OR%, FTRate on offense and defense). Port of hoopR kp_fourfactors().

**Endpoint URL:** `GET https://kenpom.com/stats.php`

**Valid URL:** [https://kenpom.com/stats.php?y=2025](https://kenpom.com/stats.php?y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_four_factors(year=2025)
```

_Last validated n/a._

## `kenpom_point_distribution`

GET /pointdist.php - share of points scored from 2s, 3s and free throws, offense and defense. Port of hoopR kp_pointdist().

**Endpoint URL:** `GET https://kenpom.com/pointdist.php`

**Valid URL:** [https://kenpom.com/pointdist.php?y=2025](https://kenpom.com/pointdist.php?y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_point_distribution(year=2025)
```

_Last validated n/a._

## `kenpom_height`

GET /height.php - team height, effective height, experience, bench minutes and continuity. Port of hoopR kp_height().

**Endpoint URL:** `GET https://kenpom.com/height.php`

**Valid URL:** [https://kenpom.com/height.php?y=2025](https://kenpom.com/height.php?y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. Columns are narrower before 2008. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_height(year=2025)
```

_Last validated n/a._

## `kenpom_foul_trouble`

GET /foul_trouble.php - team foul-trouble splits (minutes and efficiency with starters in foul trouble). Port of hoopR kp_foul_trouble().

**Endpoint URL:** `GET https://kenpom.com/foul_trouble.php`

**Valid URL:** [https://kenpom.com/foul_trouble.php?y=2025](https://kenpom.com/foul_trouble.php?y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_foul_trouble(year=2025)
```

_Last validated n/a._

## `kenpom_team_stats`

GET /teamstats.php - team shooting and style splits; side='o' for offense, 'd' for defense. Port of hoopR kp_teamstats().

**Endpoint URL:** `GET https://kenpom.com/teamstats.php`

**Valid URL:** [https://kenpom.com/teamstats.php?y=2025&od=o](https://kenpom.com/teamstats.php?y=2025&od=o)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |
| `od` | `side` |  |  | `Y` | Side of the ball: 'o' (offense, hoopR's default) or 'd' (defense). |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_team_stats(year=2025, side='o')
```

_Last validated n/a._

## `kenpom_player_stats`

GET /playerstats.php - national player leaderboard for one metric. Port of hoopR kp_playerstats().

**Endpoint URL:** `GET https://kenpom.com/playerstats.php`

**Valid URL:** [https://kenpom.com/playerstats.php?y=2025&s=eFG](https://kenpom.com/playerstats.php?y=2025&s=eFG)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. Data begins at 2004. |
| `s` | `metric` |  | `Y` |  | Metric slug as KenPom spells it on the wire - one of ORtg, PctMin, eFG, PctPoss, PctShots, ORPct, DRPct, TORate, ARate, PctBlocks, FTRate, PctStls, TS, FCper40, FDper40, FG2Pct, FG3Pct, FTPct. (hoopR's kp_playerstats() takes the display labels - ORtg, Min, eFG, Poss, Shots, OR, DR, TO, ARate, Blk, FTRate, Stl, TS, FC40, FD40, 2P, 3P, FT - and maps them to these.) |
| `f` | `conf` |  |  | `Y` | Conference filter (KenPom abbreviation, e.g. 'ACC', 'B10'); omit for all of Division I. |
| `c` | `conf_only` |  |  | `Y` | Conference-games-only toggle: 'c' restricts the leaderboard to conference play. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_player_stats(year=2025, metric='eFG')
```

_Last validated n/a._

## `kenpom_kpoy`

GET /kpoy.php - KenPom Player of the Year standings and the game-MVP table. Port of hoopR kp_kpoy().

**Endpoint URL:** `GET https://kenpom.com/kpoy.php`

**Valid URL:** [https://kenpom.com/kpoy.php?y=2025](https://kenpom.com/kpoy.php?y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_kpoy(year=2025)
```

_Last validated n/a._

## `kenpom_team`

GET /team.php - a team's full season page. Returns EVERY table on it, so one call covers hoopR's kp_team_schedule(), kp_team_players(), kp_team_depth_chart() and kp_team_lineups(), which each fetch this same page separately.

**Endpoint URL:** `GET https://kenpom.com/team.php`

**Valid URL:** [https://kenpom.com/team.php?team=Duke&y=2025](https://kenpom.com/team.php?team=Duke&y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | KenPom team name, spelled as the site does (e.g. 'Duke', 'Michigan St.'). |
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. Lineup tables begin at 2011. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_team(team='Duke', year=2025)
```

_Last validated n/a._

## `kenpom_team_players_expanded`

GET /player-expanded.php - a team's expanded per-player table plus the minutes matrix. Covers hoopR's kp_team_player_stats() and kp_minutes_matrix() in one fetch.

**Endpoint URL:** `GET https://kenpom.com/player-expanded.php`

**Valid URL:** [https://kenpom.com/player-expanded.php?team=Duke&y=2025](https://kenpom.com/player-expanded.php?team=Duke&y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | KenPom team name, spelled as the site does. |
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. Starts ('S') are available from 2014. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_team_players_expanded(team='Duke', year=2025)
```

_Last validated n/a._

## `kenpom_game_plan`

GET /gameplan.php - a team's game-plan page (per-game four factors and personnel splits). Port of hoopR kp_gameplan().

**Endpoint URL:** `GET https://kenpom.com/gameplan.php`

**Valid URL:** [https://kenpom.com/gameplan.php?team=Duke&y=2025](https://kenpom.com/gameplan.php?team=Duke&y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | KenPom team name, spelled as the site does. |
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_game_plan(team='Duke', year=2025)
```

_Last validated n/a._

## `kenpom_opponent_tracker`

GET /opptracker.php - opponent tracker; side='o' for offense, 'd' for defense. Port of hoopR kp_opptracker().

**Endpoint URL:** `GET https://kenpom.com/opptracker.php`

**Valid URL:** [https://kenpom.com/opptracker.php?team=Duke&y=2025&t=o](https://kenpom.com/opptracker.php?team=Duke&y=2025&t=o)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | KenPom team name, spelled as the site does. |
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. Columns are narrower before 2010. |
| `t` | `side` |  |  | `Y` | Side of the ball: 'o' (offense) or 'd' (defense). |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_opponent_tracker(team='Duke', year=2025, side='o')
```

_Last validated n/a._

## `kenpom_player_career`

GET /player.php - one player's career page (season-by-season stats and game log). Port of hoopR kp_player_career().

**Endpoint URL:** `GET https://kenpom.com/player.php`

**Valid URL:** [https://kenpom.com/player.php?p=51234](https://kenpom.com/player.php?p=51234)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `p` | `player_id` |  | `Y` |  | KenPom player id - the `p=` value on a player-page URL. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_player_career(player_id=51234)
```

_Last validated n/a._

## `kenpom_box`

GET /box.php - box-score detail for one game (per-team four factors, player lines, scoring runs). Port of hoopR kp_box().

**Endpoint URL:** `GET https://kenpom.com/box.php`

**Valid URL:** [https://kenpom.com/box.php?g=20250401&y=2025](https://kenpom.com/box.php?g=20250401&y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `g` | `game_id` |  | `Y` |  | KenPom game id - the `g=` value on a FanMatch game link. |
| `y` | `year` |  | `Y` |  | Season (4-digit ENDING year) the game belongs to. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_box(game_id=20250401, year=2025)
```

_Last validated n/a._

## `kenpom_win_probability`

GET /winprob.php - in-game win-probability table for one game. Port of hoopR kp_winprob().

**Endpoint URL:** `GET https://kenpom.com/winprob.php`

**Valid URL:** [https://kenpom.com/winprob.php?g=20250401&y=2025](https://kenpom.com/winprob.php?g=20250401&y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `g` | `game_id` |  | `Y` |  | KenPom game id. |
| `y` | `year` |  | `Y` |  | Season (4-digit ENDING year) the game belongs to. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_win_probability(game_id=20250401, year=2025)
```

_Last validated n/a._

## `kenpom_fan_match`

GET /fanmatch.php - the FanMatch slate for one date (predictions, thrill score, results). Port of hoopR kp_fanmatch().

**Endpoint URL:** `GET https://kenpom.com/fanmatch.php`

**Valid URL:** [https://kenpom.com/fanmatch.php?d=2025-02-01](https://kenpom.com/fanmatch.php?d=2025-02-01)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `d` | `date` |  | `Y` |  | Slate date as YYYY-MM-DD. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_fan_match(date='2025-02-01')
```

_Last validated n/a._

## `kenpom_team_history`

GET /history.php?t= - a program's season-by-season history. Port of hoopR kp_team_history().

**Endpoint URL:** `GET https://kenpom.com/history.php`

**Valid URL:** [https://kenpom.com/history.php?t=Duke](https://kenpom.com/history.php?t=Duke)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `t` | `team` |  | `Y` |  | KenPom team name, spelled as the site does. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_team_history(team='Duke')
```

_Last validated n/a._

## `kenpom_coach_history`

GET /history.php?c= - a coach's season-by-season history. Port of hoopR kp_coach_history().

**Endpoint URL:** `GET https://kenpom.com/history.php`

**Valid URL:** [https://kenpom.com/history.php?c=Jon+Scheyer](https://kenpom.com/history.php?c=Jon+Scheyer)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `c` | `coach` |  | `Y` |  | Coach name as KenPom spells it (e.g. 'Jon Scheyer'). |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_coach_history(coach='Jon Scheyer')
```

_Last validated n/a._

## `kenpom_program_ratings`

GET /programs.php - program-level ratings across the full KenPom era. Port of hoopR kp_program_ratings().

**Endpoint URL:** `GET https://kenpom.com/programs.php`

**Valid URL:** [https://kenpom.com/programs.php](https://kenpom.com/programs.php)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_program_ratings()
```

_Last validated n/a._

## `kenpom_archive_ratings`

GET /archive.php - the Pomeroy ratings as they stood on a past date. Port of hoopR kp_pomeroy_archive_ratings().

**Endpoint URL:** `GET https://kenpom.com/archive.php`

**Valid URL:** [https://kenpom.com/archive.php?d=2025-02-01](https://kenpom.com/archive.php?d=2025-02-01)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `d` | `date` |  | `Y` |  | Snapshot date as YYYY-MM-DD. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_archive_ratings(date='2025-02-01')
```

_Last validated n/a._

## `kenpom_conference`

GET /conf.php - one conference's season page (standings, efficiency, per-team splits). Port of hoopR kp_conf().

**Endpoint URL:** `GET https://kenpom.com/conf.php`

**Valid URL:** [https://kenpom.com/conf.php?c=ACC&y=2025](https://kenpom.com/conf.php?c=ACC&y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `c` | `conf` |  | `Y` |  | KenPom conference abbreviation (e.g. 'ACC', 'B10', 'SEC'). |
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_conference(conf='ACC', year=2025)
```

_Last validated n/a._

## `kenpom_conference_stats`

GET /confstats.php - league-wide conference comparison for one season. Port of hoopR kp_confstats().

**Endpoint URL:** `GET https://kenpom.com/confstats.php`

**Valid URL:** [https://kenpom.com/confstats.php?y=2025](https://kenpom.com/confstats.php?y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_conference_stats(year=2025)
```

_Last validated n/a._

## `kenpom_conference_history`

GET /confhistory.php - one conference's season-by-season history. Port of hoopR kp_confhistory().

**Endpoint URL:** `GET https://kenpom.com/confhistory.php`

**Valid URL:** [https://kenpom.com/confhistory.php?c=ACC](https://kenpom.com/confhistory.php?c=ACC)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `c` | `conf` |  | `Y` |  | KenPom conference abbreviation. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_conference_history(conf='ACC')
```

_Last validated n/a._

## `kenpom_trends`

GET /trends.php - national Division I trends by season (tempo, efficiency, shooting, fouls). Port of hoopR kp_trends().

**Endpoint URL:** `GET https://kenpom.com/trends.php`

**Valid URL:** [https://kenpom.com/trends.php](https://kenpom.com/trends.php)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_trends()
```

_Last validated n/a._

## `kenpom_home_court_advantage`

GET /hca.php - per-team home-court advantage estimates. Port of hoopR kp_hca().

**Endpoint URL:** `GET https://kenpom.com/hca.php`

**Valid URL:** [https://kenpom.com/hca.php](https://kenpom.com/hca.php)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_home_court_advantage()
```

_Last validated n/a._

## `kenpom_arenas`

GET /arenas.php - arena reference (name, capacity, average attendance) by team. Port of hoopR kp_arenas().

**Endpoint URL:** `GET https://kenpom.com/arenas.php`

**Valid URL:** [https://kenpom.com/arenas.php?y=2025](https://kenpom.com/arenas.php?y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_arenas(year=2025)
```

_Last validated n/a._

## `kenpom_officials`

GET /officials.php - referee ratings for one season. Port of hoopR kp_officials().

**Endpoint URL:** `GET https://kenpom.com/officials.php`

**Valid URL:** [https://kenpom.com/officials.php?y=2025](https://kenpom.com/officials.php?y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_officials(year=2025)
```

_Last validated n/a._

## `kenpom_referee`

GET /referee.php - one referee's game log and splits for a season. Port of hoopR kp_referee().

**Endpoint URL:** `GET https://kenpom.com/referee.php`

**Valid URL:** [https://kenpom.com/referee.php?r=Ron+Groover&y=2025](https://kenpom.com/referee.php?r=Ron+Groover&y=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `r` | `referee` |  | `Y` |  | Referee name as KenPom spells it (take it from the officials table). |
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_referee(referee='Ron Groover', year=2025)
```

_Last validated n/a._

## `kenpom_game_attributes`

GET /game_attrs.php - season game leaderboards by attribute (thrill score, comebacks, upsets, ...). Port of hoopR kp_game_attrs().

**Endpoint URL:** `GET https://kenpom.com/game_attrs.php`

**Valid URL:** [https://kenpom.com/game_attrs.php?y=2025&s=ThrillScore](https://kenpom.com/game_attrs.php?y=2025&s=ThrillScore)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `y` | `year` |  | `Y` |  | Season as a 4-digit ENDING year. |
| `s` | `attribute` |  |  | `Y` | Attribute slug, e.g. ThrillScore, Comeback, FanMatch, Upsets, Busts, MinutesPlayed, PossessionLength, LeadChanges. |

### Returns

**`return_parsed=True`** (default) — A `dict` of polars DataFrames, one per HTML table on the page, keyed by the table's HTML id (a KenPom page often carries several -- `team.php` alone holds the schedule, roster, depth chart and lineup tables); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw page HTML (`str`).

### Example

```python
kenpom_game_attributes(year=2025, attribute='ThrillScore')
```

_Last validated n/a._
