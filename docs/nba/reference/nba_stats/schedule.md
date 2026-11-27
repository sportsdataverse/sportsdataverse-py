# NBA — NBA Stats API (stats.nba.com) — Schedule

> NBA — NBA Stats API (stats.nba.com) — Schedule — function reference in sdv-py, the SportsDataverse Python package.

## nba_stats_scheduleleaguev2

Schedule league v2.

**Endpoint URL:** `GET https://stats.nba.com/stats/scheduleleaguev2`

**Valid URL:** [https://stats.nba.com/stats/scheduleleaguev2?LeagueID=00&Season=2024-25](https://stats.nba.com/stats/scheduleleaguev2?LeagueID=00&Season=2024-25)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. An NBA season tips off in late October and becomes the default in November (``2025-26`` through October 2026, ``2026-27`` from November 2026); a G League season (regular season from late December) in January; a Summer League (played in July, which stats.nba.com labels ``2026-27`` in 2026) in August; a draft combine (May) in June; a draft (``drafthistory``, a year; late June) in July. With season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) the NBA and the G League roll over in May, after their playoffs start; with ``All Star`` the NBA rolls over in March, after the February game (the G League has no All-Star rows and keeps its own rule). A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |

### Returns {#nba_stats_scheduleleaguev2-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `arena_city` | String | City hosting the game's arena. |
| `arena_name` | String | Name of the arena hosting the game. |
| `arena_state` | String | State or province of the game's arena (blank for international sites). |
| `away_team_city` | String | City of the away team. |
| `away_team_id` | Int64 | stats.nba.com / stats.wnba.com team id of the away team. |
| `away_team_losses` | Int64 | Away team's losses entering the game. |
| `away_team_name` | String | Away team nickname (e.g. Fever). |
| `away_team_score` | Int64 | Away team's final score, 0 before the game is played. |
| `away_team_seed` | Int64 | Away team's playoff seed, 0 outside the postseason. |
| `away_team_slug` | String | URL slug of the away team. |
| `away_team_time` | String | Scheduled tip-off in the away team's local time zone. |
| `away_team_tricode` | String | Three-letter abbreviation of the away team. |
| `away_team_wins` | Int64 | Away team's wins entering the game. |
| `branch_link` | String | Deep link for the game, blank when not published. |
| `day` | String | Three-letter day of week of the game date. |
| `game_code` | String | Provider game code, `YYYYMMDD/AWYHOM`. |
| `game_date` | String | Game date as served by the schedule feed (`MM/DD/YYYY HH:MM:SS`). |
| `game_date_est` | String | Game date at midnight Eastern, ISO-8601. |
| `game_date_time_est` | String | Scheduled tip-off in Eastern time, ISO-8601. |
| `game_date_time_utc` | String | Scheduled tip-off in UTC, ISO-8601. The timestamp to reduce to a calendar date. |
| `game_date_utc` | String | Game date at midnight UTC, ISO-8601. |
| `game_id` | String | Unique stats.nba.com / stats.wnba.com game id; its 3rd character encodes the season type. |
| `game_label` | String | Human-readable round or event label (e.g. Preseason, Conf. Finals). |
| `game_sequence` | Int64 | Ordinal of the game within its date. |
| `game_status` | Int64 | Game status code: 1 scheduled, 2 in progress, 3 final. |
| `game_status_text` | String | Human-readable game status (e.g. Final, 7:00 pm ET). |
| `game_sub_label` | String | Secondary event label (e.g. NBA Abu Dhabi Game). |
| `game_subtype` | String | Game subtype tag (e.g. Global Games), blank for standard games. |
| `game_time_est` | String | Scheduled tip-off time of day, Eastern. |
| `game_time_utc` | String | Scheduled tip-off time of day, UTC. |
| `home_team_city` | String | City of the home team. |
| `home_team_id` | Int64 | stats.nba.com / stats.wnba.com team id of the home team. |
| `home_team_losses` | Int64 | Home team's losses entering the game. |
| `home_team_name` | String | Home team nickname (e.g. Liberty). |
| `home_team_score` | Int64 | Home team's final score, 0 before the game is played. |
| `home_team_seed` | Int64 | Home team's playoff seed, 0 outside the postseason. |
| `home_team_slug` | String | URL slug of the home team. |
| `home_team_time` | String | Scheduled tip-off in the home team's local time zone. |
| `home_team_tricode` | String | Three-letter abbreviation of the home team. |
| `home_team_wins` | Int64 | Home team's wins entering the game. |
| `if_necessary` | String | Whether the game is a conditional series game that may not be played. |
| `is_neutral` | Boolean | Whether the game is played at a neutral site. |
| `league_id` | String | League id of the schedule: '00' NBA, '10' WNBA, '20' G-League. |
| `month_num` | Int64 | Calendar month number of the game date. |
| `postponed_status` | String | Postponement status; 'N' when the game is on as scheduled. |
| `season` | String | Season the schedule covers, as published by the feed ('2025-26' for the NBA, '2026' for the WNBA). |
| `season_type_description` | String | Season type label derived from season_type_id: Pre-Season, Regular Season, All-Star, Playoffs, Play-In Game. |
| `season_type_id` | String | Season type digit, the 3rd character of game_id. |
| `series_game_number` | String | Game number within a playoff series, blank outside a series. |
| `series_text` | String | Series context line (e.g. series tied 1-1), blank when not applicable. |
| `week_name` | String | Name of the schedule week, blank outside the regular season. |
| `week_number` | Int64 | Schedule week number, 0 outside the regular season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_scheduleleaguev2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_scheduleleaguev2-example}

```python
nba_stats_scheduleleaguev2(league_id='00', season='2024-25')
```

## nba_stats_scheduleleaguev2int

Schedule league v2 int.

**Endpoint URL:** `GET https://stats.nba.com/stats/scheduleleaguev2int`

**Valid URL:** [https://stats.nba.com/stats/scheduleleaguev2int?LeagueID=00&Season=2024-25](https://stats.nba.com/stats/scheduleleaguev2int?LeagueID=00&Season=2024-25)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. An NBA season tips off in late October and becomes the default in November (``2025-26`` through October 2026, ``2026-27`` from November 2026); a G League season (regular season from late December) in January; a Summer League (played in July, which stats.nba.com labels ``2026-27`` in 2026) in August; a draft combine (May) in June; a draft (``drafthistory``, a year; late June) in July. With season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) the NBA and the G League roll over in May, after their playoffs start; with ``All Star`` the NBA rolls over in March, after the February game (the G League has no All-Star rows and keeps its own rule). A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |

### Returns {#nba_stats_scheduleleaguev2int-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `arena_city` | String | City hosting the game's arena. |
| `arena_name` | String | Name of the arena hosting the game. |
| `arena_state` | String | State or province of the game's arena (blank for international sites). |
| `away_team_city` | String | City of the away team. |
| `away_team_id` | Int64 | stats.nba.com / stats.wnba.com team id of the away team. |
| `away_team_losses` | Int64 | Away team's losses entering the game. |
| `away_team_name` | String | Away team nickname (e.g. Fever). |
| `away_team_score` | Int64 | Away team's final score, 0 before the game is played. |
| `away_team_seed` | Int64 | Away team's playoff seed, 0 outside the postseason. |
| `away_team_slug` | String | URL slug of the away team. |
| `away_team_time` | String | Scheduled tip-off in the away team's local time zone. |
| `away_team_tricode` | String | Three-letter abbreviation of the away team. |
| `away_team_wins` | Int64 | Away team's wins entering the game. |
| `branch_link` | String | Deep link for the game, blank when not published. |
| `day` | String | Three-letter day of week of the game date. |
| `game_code` | String | Provider game code, `YYYYMMDD/AWYHOM`. |
| `game_date` | String | Game date as served by the schedule feed (`MM/DD/YYYY HH:MM:SS`). |
| `game_date_est` | String | Game date at midnight Eastern, ISO-8601. |
| `game_date_time_est` | String | Scheduled tip-off in Eastern time, ISO-8601. |
| `game_date_time_utc` | String | Scheduled tip-off in UTC, ISO-8601. The timestamp to reduce to a calendar date. |
| `game_date_utc` | String | Game date at midnight UTC, ISO-8601. |
| `game_id` | String | Unique stats.nba.com / stats.wnba.com game id; its 3rd character encodes the season type. |
| `game_label` | String | Human-readable round or event label (e.g. Preseason, Conf. Finals). |
| `game_sequence` | Int64 | Ordinal of the game within its date. |
| `game_status` | Int64 | Game status code: 1 scheduled, 2 in progress, 3 final. |
| `game_status_text` | String | Human-readable game status (e.g. Final, 7:00 pm ET). |
| `game_sub_label` | String | Secondary event label (e.g. NBA Abu Dhabi Game). |
| `game_subtype` | String | Game subtype tag (e.g. Global Games), blank for standard games. |
| `game_time_est` | String | Scheduled tip-off time of day, Eastern. |
| `game_time_utc` | String | Scheduled tip-off time of day, UTC. |
| `home_team_city` | String | City of the home team. |
| `home_team_id` | Int64 | stats.nba.com / stats.wnba.com team id of the home team. |
| `home_team_losses` | Int64 | Home team's losses entering the game. |
| `home_team_name` | String | Home team nickname (e.g. Liberty). |
| `home_team_score` | Int64 | Home team's final score, 0 before the game is played. |
| `home_team_seed` | Int64 | Home team's playoff seed, 0 outside the postseason. |
| `home_team_slug` | String | URL slug of the home team. |
| `home_team_time` | String | Scheduled tip-off in the home team's local time zone. |
| `home_team_tricode` | String | Three-letter abbreviation of the home team. |
| `home_team_wins` | Int64 | Home team's wins entering the game. |
| `if_necessary` | Boolean | Whether the game is a conditional series game that may not be played. |
| `is_neutral` | Boolean | Whether the game is played at a neutral site. |
| `league_id` | String | League id of the schedule: '00' NBA, '10' WNBA, '20' G-League. |
| `month_num` | Int64 | Calendar month number of the game date. |
| `postponed_status` | String | Postponement status; 'N' when the game is on as scheduled. |
| `season` | String | Season the schedule covers, as published by the feed ('2025-26' for the NBA, '2026' for the WNBA). |
| `season_type_description` | String | Season type label derived from season_type_id: Pre-Season, Regular Season, All-Star, Playoffs, Play-In Game. |
| `season_type_id` | String | Season type digit, the 3rd character of game_id. |
| `series_game_number` | String | Game number within a playoff series, blank outside a series. |
| `series_text` | String | Series context line (e.g. series tied 1-1), blank when not applicable. |
| `week_name` | String | Name of the schedule week, blank outside the regular season. |
| `week_number` | Int64 | Schedule week number, 0 outside the regular season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_scheduleleaguev2int-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_scheduleleaguev2int-example}

```python
nba_stats_scheduleleaguev2int(league_id='00', season='2024-25')
```
