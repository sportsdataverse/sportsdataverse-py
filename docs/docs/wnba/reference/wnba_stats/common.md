---
title: "WNBA — WNBA Stats API (stats.wnba.com) — Common"
sidebar_label: "Common"
sidebar_position: 3
description: "WNBA — WNBA Stats API (stats.wnba.com) — Common — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WNBA — WNBA Stats API (stats.wnba.com) — Common

## wnba_stats_commonallplayers

Common all players.

**Endpoint URL:** `GET https://stats.wnba.com/stats/commonallplayers`

**Valid URL:** [https://stats.wnba.com/stats/commonallplayers?IsOnlyCurrentSeason=0&LeagueID=10&Season=2024](https://stats.wnba.com/stats/commonallplayers?IsOnlyCurrentSeason=0&LeagueID=10&Season=2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `IsOnlyCurrentSeason` | `is_only_current_season` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A WNBA season tips off in mid-May and becomes the default in June (``2025`` through May 2026, ``2026`` from June 2026); a draft (``drafthistory``, mid-April) in May. With season type ``Playoffs`` (or ``commonplayoffseries``) the WNBA rolls over in October, after its mid-September playoffs start; with ``All Star`` in August, after the July game. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |

### Returns {#wnba_stats_commonallplayers-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `display_last_comma_first` | String | Player name formatted as "Last, First". |
| `display_first_last` | String | Player name formatted as "First Last". |
| `rosterstatus` | Int64 | Roster status flag (1 = currently on a roster, 0 = not). |
| `from_year` | String | First season. |
| `to_year` | String | Most recent season. |
| `playercode` | String | URL-style player code slug used by the league's legacy stats pages. |
| `player_slug` | String | URL-safe player identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_code` | String | Lowercase slug of the player's team used in stats.nba.com URLs (e.g. 'lakers'). |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `is_nba_assigned` | Int64 | Flag indicating whether the player is currently on an NBA roster assignment (two-way and G League assignment tracking). |
| `nba_assigned_team_id` | Int64 | Team identifier of the NBA team the player is assigned to, when on assignment. |
| `games_played_flag` | String | Y/N flag for whether the player has appeared in a league game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_commonallplayers-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_commonallplayers-example}

```python
wnba_stats_commonallplayers(league_id='10', season='2024')
```

## wnba_stats_commonplayerinfo

Common player info.

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
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `display_first_last` | String | NBA or WNBA Stats value for display first last in the commonplayerinfo result set. |
| `display_last_comma_first` | String | NBA or WNBA Stats value for display last comma first in the commonplayerinfo result set. |
| `display_fi_last` | String | NBA or WNBA Stats value for display fi last in the commonplayerinfo result set. |
| `player_slug` | String | URL-safe player identifier. |
| `birthdate` | String | Player's date of birth as an ISO timestamp string (e.g. '1984-12-30T00:00:00'). |
| `school` | String | Player's school / college (when distinct from 'college'). |
| `country` | String | Country (full name or code). |
| `last_affiliation` | String | NBA or WNBA Stats value for last affiliation in the commonplayerinfo result set. |
| `height` | String | Player height (string e.g. '6-2' or inches). |
| `weight` | String | Player weight in pounds. |
| `season_exp` | Int64 | NBA or WNBA Stats value for season exp in the commonplayerinfo result set. |
| `jersey` | String | Jersey number worn by the player. |
| `position` | String | Listed roster position (G, F, C, etc.). |
| `rosterstatus` | String | NBA or WNBA Stats value for rosterstatus in the commonplayerinfo result set. |
| `games_played_current_season_flag` | String | Flag indicating games played current season flag for the requested NBA or WNBA Stats context. |
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_code` | String | Lowercase slug of the player's current team used in stats.nba.com URLs (e.g. 'lakers'). |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `playercode` | String | NBA or WNBA Stats value for playercode in the commonplayerinfo result set. |
| `from_year` | Int64 | First season. |
| `to_year` | Int64 | Most recent season. |
| `dleague_flag` | String | Flag indicating dleague flag for the requested NBA or WNBA Stats context. |
| `nba_flag` | String | Flag indicating NBA flag for the requested NBA or WNBA Stats context. |
| `games_played_flag` | String | Flag indicating games played flag for the requested NBA or WNBA Stats context. |
| `draft_year` | String | Draft year (4-digit). |
| `draft_round` | String | Round of the draft selection. |
| `draft_number` | String | Overall pick the player was selected with, as a string; 'Undrafted' when never drafted. |
| `greatest_75_flag` | String | Flag indicating greatest 75 flag for the requested NBA or WNBA Stats context. |

**PlayerHeadlineStats**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `time_frame` | String |  |
| `pts` | Float64 | Points scored. |
| `ast` | Float64 | Assists. |
| `reb` | Float64 | Total rebounds. |
| `pie` | Int64 | Player Impact Estimate (0-1). |

**AvailableSeasons**

| col_name | type | description |
|---|---|---|
| `season_id` | String | Unique season identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_commonplayerinfo-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_commonplayerinfo-example}

```python
wnba_stats_commonplayerinfo(league_id='10')
```

## wnba_stats_commonplayoffseries

Common playoff series.

**Endpoint URL:** `GET https://stats.wnba.com/stats/commonplayoffseries`

**Valid URL:** [https://stats.wnba.com/stats/commonplayoffseries?LeagueID=10&Season=2024&SeriesID=](https://stats.wnba.com/stats/commonplayoffseries?LeagueID=10&Season=2024&SeriesID=)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A WNBA season tips off in mid-May and becomes the default in June (``2025`` through May 2026, ``2026`` from June 2026); a draft (``drafthistory``, mid-April) in May. With season type ``Playoffs`` (or ``commonplayoffseries``) the WNBA rolls over in October, after its mid-September playoffs start; with ``All Star`` in August, after the July game. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `SeriesID` | `series_id_nullable` |  |  | `Y` |  |

### Returns {#wnba_stats_commonplayoffseries-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `visitor_team_id` | Int64 | Unique identifier for visitor team. |
| `series_id` | String | Playoff series id, the game id stem without the game sequence digit (e.g. '004230010' for the 2023-24 playoffs' first series). |
| `game_num` | Int64 | NBA or WNBA Stats value for game number in the commonplayoffseries result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_commonplayoffseries-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_commonplayoffseries-example}

```python
wnba_stats_commonplayoffseries(league_id='10', season='2024')
```

## wnba_stats_commonteamroster

Common team roster.

**Endpoint URL:** `GET https://stats.wnba.com/stats/commonteamroster`

**Valid URL:** [https://stats.wnba.com/stats/commonteamroster?LeagueID=10&Season=2024&TeamID=1611661317](https://stats.wnba.com/stats/commonteamroster?LeagueID=10&Season=2024&TeamID=1611661317)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A WNBA season tips off in mid-May and becomes the default in June (``2025`` through May 2026, ``2026`` from June 2026); a draft (``drafthistory``, mid-April) in May. With season type ``Playoffs`` (or ``commonplayoffseries``) the WNBA rolls over in October, after its mid-September playoffs start; with ``All Star`` in August, after the July game. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#wnba_stats_commonteamroster-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`CommonTeamRoster`, `Coaches`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**CommonTeamRoster**

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Unique team identifier. |
| `season` | String | Season identifier (4-digit year or 'YYYY-YY' string). |
| `league_id` | String | League identifier from the stats API ("00" = NBA, "10" = WNBA, "20" = G League). |
| `player` | String | Player name. |
| `nickname` | String | Team or athlete nickname. |
| `player_slug` | String | URL-safe player identifier. |
| `num` | String | Jersey number as printed on the roster (string; may be blank or carry a suffix). |
| `position` | String | Listed roster position (G, F, C, etc.). |
| `height` | String | Player height (string e.g. '6-2' or inches). |
| `weight` | String | Player weight in pounds. |
| `birth_date` | String | Date of birth (YYYY-MM-DD). |
| `age` | Float64 | Player age (in years). |
| `exp` | String | Exp. |
| `school` | String | Player's school / college (when distinct from 'college'). |
| `player_id` | Int64 | Unique player identifier. |
| `how_acquired` | String | How the team acquired the player (e.g. draft, trade, free agency). |

**Coaches**

| col_name | type | description |
|---|---|---|
| `coach_id` | Int64 | Unique identifier for coach. |
| `team_id` | Int64 | Unique team identifier. |
| `season` | String | Season identifier (4-digit year or 'YYYY-YY' string). |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `coach_name` | String |  |
| `is_assistant` | Int64 |  |
| `coach_type` | String |  |
| `sort_sequence` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_commonteamroster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_commonteamroster-example}

```python
wnba_stats_commonteamroster(league_id='10', season='2024')
```

## wnba_stats_commonteamyears

Common team years.

**Endpoint URL:** `GET https://stats.wnba.com/stats/commonteamyears`

**Valid URL:** [https://stats.wnba.com/stats/commonteamyears?LeagueID=10](https://stats.wnba.com/stats/commonteamyears?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns {#wnba_stats_commonteamyears-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | String | League identifier ('10' = WNBA). |
| `team_id` | Int64 | Unique team identifier. |
| `min_year` | String | Minimum year queried (echoes `min_year`). |
| `max_year` | String | Maximum year queried (echoes `max_year`). |
| `abbreviation` | String | Short abbreviation. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_commonteamyears-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_commonteamyears-example}

```python
wnba_stats_commonteamyears(league_id='10')
```
