---
title: "NBA — NBA Stats API (stats.nba.com) — Common"
sidebar_label: "Common"
sidebar_position: 2
description: "NBA — NBA Stats API (stats.nba.com) — Common — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA — NBA Stats API (stats.nba.com) — Common

## nba_stats_commonallplayers

GET /stats/commonallplayers

**Endpoint URL:** `GET https://stats.nba.com/stats/commonallplayers`

**Valid URL:** [https://stats.nba.com/stats/commonallplayers?LeagueID=00](https://stats.nba.com/stats/commonallplayers?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `IsOnlyCurrentSeason` | `is_only_current_season` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#nba_stats_commonallplayers-returns}

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
| `games_played_flag` | character | Y/N flag for whether the player has appeared in a league game. |
| `otherleague_experience_ch` | character | Code for the player's experience in another league (e.g. G League), as reported by the stats API. |
| `is_nba_assigned` | integer | Flag indicating whether the player is currently on an NBA roster assignment (two-way and G League assignment tracking). |
| `nba_assigned_team_id` | integer | Team identifier of the NBA team the player is assigned to, when on assignment. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_commonallplayers-example}

```python
nba_stats_commonallplayers(league_id='00')
```

_Last validated n/a._

## nba_stats_commonplayerinfo

GET /stats/commonplayerinfo

**Endpoint URL:** `GET https://stats.nba.com/stats/commonplayerinfo`

**Valid URL:** [https://stats.nba.com/stats/commonplayerinfo?LeagueID=00](https://stats.nba.com/stats/commonplayerinfo?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |

### Returns {#nba_stats_commonplayerinfo-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
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
| `school` | character | Player school / pre-draft team. |
| `country` | character | Venue country. |
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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_commonplayerinfo-example}

```python
nba_stats_commonplayerinfo(league_id='00')
```

_Last validated n/a._

## nba_stats_commonplayoffseries

GET /stats/commonplayoffseries

**Endpoint URL:** `GET https://stats.nba.com/stats/commonplayoffseries`

**Valid URL:** [https://stats.nba.com/stats/commonplayoffseries?LeagueID=00](https://stats.nba.com/stats/commonplayoffseries?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeriesID` | `series_id_nullable` |  |  | `Y` |  |

### Returns {#nba_stats_commonplayoffseries-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `home_team_id` | integer | Unique identifier for the home team. |
| `visitor_team_id` | integer | Unique identifier for visitor team. |
| `series_id` | character | Series identifier (e.g. 'W_1'). |
| `game_num` | integer | NBA or WNBA Stats value for game number in the commonplayoffseries result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_commonplayoffseries-example}

```python
nba_stats_commonplayoffseries(league_id='00')
```

_Last validated n/a._

## nba_stats_commonteamroster

GET /stats/commonteamroster

**Endpoint URL:** `GET https://stats.nba.com/stats/commonteamroster`

**Valid URL:** [https://stats.nba.com/stats/commonteamroster?LeagueID=00](https://stats.nba.com/stats/commonteamroster?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#nba_stats_commonteamroster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `teamid` | integer | Teamid. |
| `season` | character | Season year. |
| `leagueid` | character | League identifier from the stats API ("00" = NBA, "10" = WNBA, "20" = G League). |
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
| `school` | character | Player school / pre-draft team. |
| `player_id` | integer | Unique player identifier. |
| `how_acquired` | character | How the team acquired the player (e.g. draft, trade, free agency). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_commonteamroster-example}

```python
nba_stats_commonteamroster(league_id='00')
```

_Last validated n/a._

## nba_stats_commonteamyears

GET /stats/commonteamyears

**Endpoint URL:** `GET https://stats.nba.com/stats/commonteamyears`

**Valid URL:** [https://stats.nba.com/stats/commonteamyears?LeagueID=00](https://stats.nba.com/stats/commonteamyears?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns {#nba_stats_commonteamyears-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `min_year` | character | Minimum year queried (echoes `min_year`). |
| `max_year` | character | Maximum year queried (echoes `max_year`). |
| `abbreviation` | character | Short abbreviation. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_commonteamyears-example}

```python
nba_stats_commonteamyears(league_id='00')
```

_Last validated n/a._
