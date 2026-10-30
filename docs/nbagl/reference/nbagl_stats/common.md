# NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Common

> NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Common — function reference in sdv-py, the SportsDataverse Python package.

## nbagl_stats_commonallplayers

Common all players.

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
| `person_id` | Int64 |  |
| `display_last_comma_first` | String | Player name formatted as "Last, First". |
| `display_first_last` | String | Player name formatted as "First Last". |
| `rosterstatus` | Int64 | Roster status flag (1 = currently on a roster, 0 = not). |
| `from_year` | String |  |
| `to_year` | String |  |
| `playercode` | String | URL-style player code slug used by the league's legacy stats pages. |
| `player_slug` | String |  |
| `team_id` | Int64 |  |
| `team_city` | String |  |
| `team_name` | String |  |
| `team_abbreviation` | String |  |
| `team_code` | String | Lowercase slug of the player's team used in stats.nba.com URLs (e.g. 'lakers'). |
| `team_slug` | String |  |
| `is_nba_assigned` | Int64 | Flag indicating whether the player is currently on an NBA roster assignment (two-way and G League assignment tracking). |
| `nba_assigned_team_id` | Int64 | Team identifier of the NBA team the player is assigned to, when on assignment. |
| `games_played_flag` | String | Y/N flag for whether the player has appeared in a league game. |
| `otherleague_experience_ch` | String | Code for the player's experience in another league (e.g. G League), as reported by the stats API. |

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

Common player info.

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
| `person_id` | Int64 |  |
| `first_name` | String |  |
| `last_name` | String |  |
| `display_first_last` | String | NBA or WNBA Stats value for display first last in the commonplayerinfo result set. |
| `display_last_comma_first` | String | NBA or WNBA Stats value for display last comma first in the commonplayerinfo result set. |
| `display_fi_last` | String | NBA or WNBA Stats value for display fi last in the commonplayerinfo result set. |
| `player_slug` | String |  |
| `birthdate` | String | Player's date of birth as an ISO timestamp string (e.g. '1984-12-30T00:00:00'). |
| `school` | String |  |
| `country` | String |  |
| `last_affiliation` | String | NBA or WNBA Stats value for last affiliation in the commonplayerinfo result set. |
| `height` | String |  |
| `weight` | String |  |
| `season_exp` | Int64 | NBA or WNBA Stats value for season exp in the commonplayerinfo result set. |
| `jersey` | String |  |
| `position` | String |  |
| `rosterstatus` | String | NBA or WNBA Stats value for rosterstatus in the commonplayerinfo result set. |
| `games_played_current_season_flag` | String | Flag indicating games played current season flag for the requested NBA or WNBA Stats context. |
| `team_id` | Int64 |  |
| `team_name` | String |  |
| `team_abbreviation` | String |  |
| `team_code` | String | Lowercase slug of the player's current team used in stats.nba.com URLs (e.g. 'lakers'). |
| `team_city` | String |  |
| `playercode` | String | NBA or WNBA Stats value for playercode in the commonplayerinfo result set. |
| `from_year` | Int64 |  |
| `to_year` | Int64 |  |
| `dleague_flag` | String | Flag indicating dleague flag for the requested NBA or WNBA Stats context. |
| `nba_flag` | String | Flag indicating NBA flag for the requested NBA or WNBA Stats context. |
| `games_played_flag` | String | Flag indicating games played flag for the requested NBA or WNBA Stats context. |
| `draft_year` | String |  |
| `draft_round` | String |  |
| `draft_number` | String | Overall pick the player was selected with, as a string; 'Undrafted' when never drafted. |
| `greatest_75_flag` | String | Flag indicating greatest 75 flag for the requested NBA or WNBA Stats context. |

**PlayerHeadlineStats**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `time_frame` | String |  |
| `pts` | Float64 |  |
| `ast` | Float64 |  |
| `reb` | Float64 |  |
| `pie` | Int64 |  |

**AvailableSeasons**

| col_name | type | description |
|---|---|---|
| `season_id` | String |  |

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

Common playoff series.

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
| `game_id` | String |  |
| `home_team_id` | Int64 |  |
| `visitor_team_id` | Int64 |  |
| `series_id` | String | Playoff series id, the game id stem without the game sequence digit (e.g. '004230010' for the 2023-24 playoffs' first series). |
| `game_num` | Int64 | NBA or WNBA Stats value for game number in the commonplayoffseries result set. |

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

Common team roster.

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
| `team_id` | Int64 |  |
| `season` | String |  |
| `league_id` | String | League identifier from the stats API ("00" = NBA, "10" = WNBA, "20" = G League). |
| `player` | String |  |
| `nickname` | String |  |
| `player_slug` | String |  |
| `num` | String | Jersey number as printed on the roster (string; may be blank or carry a suffix). |
| `position` | String |  |
| `height` | String |  |
| `weight` | String |  |
| `birth_date` | String |  |
| `age` | Float64 |  |
| `exp` | String |  |
| `school` | String |  |
| `player_id` | Int64 |  |
| `how_acquired` | String | How the team acquired the player (e.g. draft, trade, free agency). |

**Coaches**

| col_name | type | description |
|---|---|---|
| `coach_id` | Int64 |  |
| `team_id` | Int64 |  |
| `season` | String |  |
| `first_name` | String |  |
| `last_name` | String |  |
| `coach_name` | String |  |
| `is_assistant` | Int64 |  |
| `coach_type` | String |  |
| `sort_sequence` | Int64 |  |

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

Common team years.

**Endpoint URL:** `GET https://stats.nba.com/stats/commonteamyears`

**Valid URL:** [https://stats.nba.com/stats/commonteamyears?LeagueID=20](https://stats.nba.com/stats/commonteamyears?LeagueID=20)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_commonteamyears-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | String |  |
| `team_id` | Int64 |  |
| `min_year` | String |  |
| `max_year` | String |  |
| `abbreviation` | String |  |

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
