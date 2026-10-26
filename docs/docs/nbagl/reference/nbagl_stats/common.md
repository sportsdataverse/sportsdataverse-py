---
title: "NBAGL — G League Stats API (stats.nba.com, LeagueID=20) — Common"
sidebar_label: "Common"
sidebar_position: 3
description: "NBAGL — G League Stats API (stats.nba.com, LeagueID=20) — Common — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBAGL — G League Stats API (stats.nba.com, LeagueID=20) — Common

## nbagl_stats_commonallplayers

GET /stats/commonallplayers

**Endpoint URL:** `GET https://stats.nba.com/stats/commonallplayers`

**Valid URL:** [https://stats.nba.com/stats/commonallplayers?IsOnlyCurrentSeason=0&LeagueID=20&Season=2024-25](https://stats.nba.com/stats/commonallplayers?IsOnlyCurrentSeason=0&LeagueID=20&Season=2024-25)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `IsOnlyCurrentSeason` | `is_only_current_season` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |

### Returns {#nbagl_stats_commonallplayers-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `person_id` | integer |  |
| `display_last_comma_first` | character | Player name formatted as "Last, First". |
| `display_first_last` | character | Player name formatted as "First Last". |
| `rosterstatus` | integer | Roster status flag (1 = currently on a roster, 0 = not). |
| `from_year` | character |  |
| `to_year` | character |  |
| `playercode` | character | URL-style player code slug used by the league's legacy stats pages. |
| `player_slug` | character |  |
| `team_id` | integer |  |
| `team_city` | character |  |
| `team_name` | character |  |
| `team_abbreviation` | character |  |
| `team_code` | character | Lowercase slug of the player's team used in stats.nba.com URLs (e.g. 'lakers'). |
| `team_slug` | character |  |
| `is_nba_assigned` | integer | Flag indicating whether the player is currently on an NBA roster assignment (two-way and G League assignment tracking). |
| `nba_assigned_team_id` | integer | Team identifier of the NBA team the player is assigned to, when on assignment. |
| `games_played_flag` | character | Y/N flag for whether the player has appeared in a league game. |
| `otherleague_experience_ch` | character | Code for the player's experience in another league (e.g. G League), as reported by the stats API. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_commonallplayers-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_commonallplayers-example}

```python
nbagl_stats_commonallplayers(league_id='20', season='2024-25')
```

## nbagl_stats_commonplayerinfo

GET /stats/commonplayerinfo

**Endpoint URL:** `GET https://stats.nba.com/stats/commonplayerinfo`

**Valid URL:** [https://stats.nba.com/stats/commonplayerinfo?LeagueID=20&PlayerID=1642443](https://stats.nba.com/stats/commonplayerinfo?LeagueID=20&PlayerID=1642443)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_commonplayerinfo-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`CommonPlayerInfo`, `PlayerHeadlineStats`, `AvailableSeasons`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**CommonPlayerInfo**

| col_name | type | description |
|---|---|---|
| `person_id` | integer |  |
| `first_name` | character |  |
| `last_name` | character |  |
| `display_first_last` | character | NBA or WNBA Stats value for display first last in the commonplayerinfo result set. |
| `display_last_comma_first` | character | NBA or WNBA Stats value for display last comma first in the commonplayerinfo result set. |
| `display_fi_last` | character | NBA or WNBA Stats value for display fi last in the commonplayerinfo result set. |
| `player_slug` | character |  |
| `birthdate` | character | Player's date of birth as an ISO timestamp string (e.g. '1984-12-30T00:00:00'). |
| `school` | character |  |
| `country` | character |  |
| `last_affiliation` | character | NBA or WNBA Stats value for last affiliation in the commonplayerinfo result set. |
| `height` | character |  |
| `weight` | character |  |
| `season_exp` | integer | NBA or WNBA Stats value for season exp in the commonplayerinfo result set. |
| `jersey` | character |  |
| `position` | character |  |
| `rosterstatus` | character | NBA or WNBA Stats value for rosterstatus in the commonplayerinfo result set. |
| `games_played_current_season_flag` | character | Flag indicating games played current season flag for the requested NBA or WNBA Stats context. |
| `team_id` | integer |  |
| `team_name` | character |  |
| `team_abbreviation` | character |  |
| `team_code` | character | Lowercase slug of the player's current team used in stats.nba.com URLs (e.g. 'lakers'). |
| `team_city` | character |  |
| `playercode` | character | NBA or WNBA Stats value for playercode in the commonplayerinfo result set. |
| `from_year` | integer |  |
| `to_year` | integer |  |
| `dleague_flag` | character | Flag indicating dleague flag for the requested NBA or WNBA Stats context. |
| `nba_flag` | character | Flag indicating NBA flag for the requested NBA or WNBA Stats context. |
| `games_played_flag` | character | Flag indicating games played flag for the requested NBA or WNBA Stats context. |
| `draft_year` | character |  |
| `draft_round` | character |  |
| `draft_number` | character | Overall pick the player was selected with, as a string; 'Undrafted' when never drafted. |
| `greatest_75_flag` | character | Flag indicating greatest 75 flag for the requested NBA or WNBA Stats context. |

**PlayerHeadlineStats**

| col_name | type | description |
|---|---|---|
| `player_id` | integer |  |
| `player_name` | character |  |
| `time_frame` | character |  |
| `pts` | numeric |  |
| `ast` | numeric |  |
| `reb` | numeric |  |
| `pie` | integer |  |

**AvailableSeasons**

| col_name | type | description |
|---|---|---|
| `season_id` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_commonplayerinfo-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_commonplayerinfo-example}

```python
nbagl_stats_commonplayerinfo(league_id='20')
```

## nbagl_stats_commonplayoffseries

GET /stats/commonplayoffseries

**Endpoint URL:** `GET https://stats.nba.com/stats/commonplayoffseries`

**Valid URL:** [https://stats.nba.com/stats/commonplayoffseries?LeagueID=20&Season=2024-25&SeriesID=](https://stats.nba.com/stats/commonplayoffseries?LeagueID=20&Season=2024-25&SeriesID=)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeriesID` | `series_id_nullable` |  |  | `Y` |  |

### Returns {#nbagl_stats_commonplayoffseries-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `home_team_id` | integer |  |
| `visitor_team_id` | integer |  |
| `series_id` | character | Playoff series id, the game id stem without the game sequence digit (e.g. '004230010' for the 2023-24 playoffs' first series). |
| `game_num` | integer | NBA or WNBA Stats value for game number in the commonplayoffseries result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_commonplayoffseries-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_commonplayoffseries-example}

```python
nbagl_stats_commonplayoffseries(league_id='20', season='2024-25')
```

## nbagl_stats_commonteamroster

GET /stats/commonteamroster

**Endpoint URL:** `GET https://stats.nba.com/stats/commonteamroster`

**Valid URL:** [https://stats.nba.com/stats/commonteamroster?LeagueID=20&Season=2024-25&TeamID=1612709914](https://stats.nba.com/stats/commonteamroster?LeagueID=20&Season=2024-25&TeamID=1612709914)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_commonteamroster-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`CommonTeamRoster`, `Coaches`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**CommonTeamRoster**

| col_name | type | description |
|---|---|---|
| `team_id` | integer |  |
| `season` | character |  |
| `league_id` | character | League identifier from the stats API ("00" = NBA, "10" = WNBA, "20" = G League). |
| `player` | character |  |
| `nickname` | character |  |
| `player_slug` | character |  |
| `num` | character | Jersey number as printed on the roster (string; may be blank or carry a suffix). |
| `position` | character |  |
| `height` | character |  |
| `weight` | character |  |
| `birth_date` | character |  |
| `age` | numeric |  |
| `exp` | character |  |
| `school` | character |  |
| `player_id` | integer |  |
| `how_acquired` | character | How the team acquired the player (e.g. draft, trade, free agency). |

**Coaches**

| col_name | type | description |
|---|---|---|
| `coach_id` | integer |  |
| `team_id` | integer |  |
| `season` | character |  |
| `first_name` | character |  |
| `last_name` | character |  |
| `coach_name` | character |  |
| `is_assistant` | integer |  |
| `coach_type` | character |  |
| `sort_sequence` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_commonteamroster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_commonteamroster-example}

```python
nbagl_stats_commonteamroster(league_id='20', season='2024-25')
```

## nbagl_stats_commonteamyears

GET /stats/commonteamyears

**Endpoint URL:** `GET https://stats.nba.com/stats/commonteamyears`

**Valid URL:** [https://stats.nba.com/stats/commonteamyears?LeagueID=20](https://stats.nba.com/stats/commonteamyears?LeagueID=20)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_commonteamyears-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | character |  |
| `team_id` | integer |  |
| `min_year` | character |  |
| `max_year` | character |  |
| `abbreviation` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_commonteamyears-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_commonteamyears-example}

```python
nbagl_stats_commonteamyears(league_id='20')
```
