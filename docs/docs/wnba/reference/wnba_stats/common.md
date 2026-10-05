---
title: "WNBA — WNBA Stats API (stats.wnba.com) — Common"
sidebar_label: "Common"
sidebar_position: 3
description: "WNBA — WNBA Stats API (stats.wnba.com) — Common — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WNBA — WNBA Stats API (stats.wnba.com) — Common

## wnba_stats_commonallplayers

GET /stats/commonallplayers

**Endpoint URL:** `GET https://stats.wnba.com/stats/commonallplayers`

**Valid URL:** [https://stats.wnba.com/stats/commonallplayers?IsOnlyCurrentSeason=0&LeagueID=10](https://stats.wnba.com/stats/commonallplayers?IsOnlyCurrentSeason=0&LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `IsOnlyCurrentSeason` | `is_only_current_season` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#wnba_stats_commonallplayers-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `display_last_comma_first` | character | Player name formatted as "Last, First". |
| `display_first_last` | character | Player name formatted as "First Last". |
| `rosterstatus` | integer | Roster status flag (1 = currently on a roster, 0 = not). |
| `from_year` | character | First season. |
| `to_year` | character | Most recent season. |
| `playercode` | character | URL-style player code slug used by the league's legacy stats pages. |
| `player_slug` | character | URL-safe player identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_code` | character | Internal team code. |
| `team_slug` | character | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `is_nba_assigned` | integer | Flag indicating whether the player is currently on an NBA roster assignment (two-way and G League assignment tracking). |
| `nba_assigned_team_id` | integer | Team identifier of the NBA team the player is assigned to, when on assignment. |
| `games_played_flag` | character | Y/N flag for whether the player has appeared in a league game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_commonallplayers-example}

```python
wnba_stats_commonallplayers(league_id='10')
```

_Last validated n/a._

## wnba_stats_commonplayerinfo

GET /stats/commonplayerinfo

**Endpoint URL:** `GET https://stats.wnba.com/stats/commonplayerinfo`

**Valid URL:** [https://stats.wnba.com/stats/commonplayerinfo?LeagueID=10&PlayerID=1628932](https://stats.wnba.com/stats/commonplayerinfo?LeagueID=10&PlayerID=1628932)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |

### Returns {#wnba_stats_commonplayerinfo-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`CommonPlayerInfo`, `PlayerHeadlineStats`, `AvailableSeasons`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**CommonPlayerInfo**

| col_name | type | description |
|---|---|---|
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `display_first_last` | character | NBA or WNBA Stats value for display first last in the commonplayerinfo result set. |
| `display_last_comma_first` | character | NBA or WNBA Stats value for display last comma first in the commonplayerinfo result set. |
| `display_fi_last` | character | NBA or WNBA Stats value for display fi last in the commonplayerinfo result set. |
| `player_slug` | character | URL-safe player identifier. |
| `birthdate` | character | Date of birth. |
| `school` | character | Player's school / college (when distinct from 'college'). |
| `country` | character | Country (full name or code). |
| `last_affiliation` | character | NBA or WNBA Stats value for last affiliation in the commonplayerinfo result set. |
| `height` | character | Player height (string e.g. '6-2' or inches). |
| `weight` | character | Player weight in pounds. |
| `season_exp` | integer | NBA or WNBA Stats value for season exp in the commonplayerinfo result set. |
| `jersey` | character | Jersey number worn by the player. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `rosterstatus` | character | NBA or WNBA Stats value for rosterstatus in the commonplayerinfo result set. |
| `games_played_current_season_flag` | character | Flag indicating games played current season flag for the requested NBA or WNBA Stats context. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_code` | character | Internal team code. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `playercode` | character | NBA or WNBA Stats value for playercode in the commonplayerinfo result set. |
| `from_year` | integer | First season. |
| `to_year` | integer | Most recent season. |
| `dleague_flag` | character | Flag indicating dleague flag for the requested NBA or WNBA Stats context. |
| `nba_flag` | character | Flag indicating NBA flag for the requested NBA or WNBA Stats context. |
| `games_played_flag` | character | Flag indicating games played flag for the requested NBA or WNBA Stats context. |
| `draft_year` | character | Draft year (4-digit). |
| `draft_round` | character | Round of the draft selection. |
| `draft_number` | character | The number pick that was used to select a given player. |
| `greatest_75_flag` | character | Flag indicating greatest 75 flag for the requested NBA or WNBA Stats context. |

**PlayerHeadlineStats**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `time_frame` | character |  |
| `pts` | numeric | Points scored. |
| `ast` | numeric | Assists. |
| `reb` | numeric | Total rebounds. |
| `pie` | integer | Player Impact Estimate (0-1). |

**AvailableSeasons**

| col_name | type | description |
|---|---|---|
| `season_id` | character | Unique season identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_commonplayerinfo-example}

```python
wnba_stats_commonplayerinfo(league_id='10')
```

_Last validated n/a._

## wnba_stats_commonplayoffseries

GET /stats/commonplayoffseries

**Endpoint URL:** `GET https://stats.wnba.com/stats/commonplayoffseries`

**Valid URL:** [https://stats.wnba.com/stats/commonplayoffseries?LeagueID=10&SeriesID=](https://stats.wnba.com/stats/commonplayoffseries?LeagueID=10&SeriesID=)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeriesID` | `series_id_nullable` |  |  | `Y` |  |

### Returns {#wnba_stats_commonplayoffseries-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `home_team_id` | integer | Unique identifier for the home team. |
| `visitor_team_id` | integer | Unique identifier for visitor team. |
| `series_id` | character | Series identifier (e.g. 'W_1'). |
| `game_num` | integer | NBA or WNBA Stats value for game number in the commonplayoffseries result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_commonplayoffseries-example}

```python
wnba_stats_commonplayoffseries(league_id='10')
```

_Last validated n/a._

## wnba_stats_commonteamroster

GET /stats/commonteamroster

**Endpoint URL:** `GET https://stats.wnba.com/stats/commonteamroster`

**Valid URL:** [https://stats.wnba.com/stats/commonteamroster?LeagueID=10&TeamID=1611661317](https://stats.wnba.com/stats/commonteamroster?LeagueID=10&TeamID=1611661317)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#wnba_stats_commonteamroster-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`CommonTeamRoster`, `Coaches`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**CommonTeamRoster**

| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `season` | character | Season identifier (4-digit year or 'YYYY-YY' string). |
| `league_id` | character | League identifier from the stats API ("00" = NBA, "10" = WNBA, "20" = G League). |
| `player` | character | Player name. |
| `nickname` | character | Team or athlete nickname. |
| `player_slug` | character | URL-safe player identifier. |
| `num` | character | Inning number. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `height` | character | Player height (string e.g. '6-2' or inches). |
| `weight` | character | Player weight in pounds. |
| `birth_date` | character | Date of birth (YYYY-MM-DD). |
| `age` | numeric | Player age (in years). |
| `exp` | character | Exp. |
| `school` | character | Player's school / college (when distinct from 'college'). |
| `player_id` | integer | Unique player identifier. |
| `how_acquired` | character | How the team acquired the player (e.g. draft, trade, free agency). |

**Coaches**

| col_name | type | description |
|---|---|---|
| `coach_id` | integer | Unique identifier for coach. |
| `team_id` | integer | Unique team identifier. |
| `season` | character | Season identifier (4-digit year or 'YYYY-YY' string). |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `coach_name` | character |  |
| `is_assistant` | integer |  |
| `coach_type` | character |  |
| `sort_sequence` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_commonteamroster-example}

```python
wnba_stats_commonteamroster(league_id='10')
```

_Last validated n/a._

## wnba_stats_commonteamyears

GET /stats/commonteamyears

**Endpoint URL:** `GET https://stats.wnba.com/stats/commonteamyears`

**Valid URL:** [https://stats.wnba.com/stats/commonteamyears?LeagueID=10](https://stats.wnba.com/stats/commonteamyears?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns {#wnba_stats_commonteamyears-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `min_year` | character | Minimum year queried (echoes `min_year`). |
| `max_year` | character | Maximum year queried (echoes `max_year`). |
| `abbreviation` | character | Short abbreviation. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_commonteamyears-example}

```python
wnba_stats_commonteamyears(league_id='10')
```

_Last validated n/a._
