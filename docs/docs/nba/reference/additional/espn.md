---
title: "NBA — additional Python functions — ESPN"
sidebar_label: "ESPN"
sidebar_position: 2
description: "NBA — additional Python functions — ESPN — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — ESPN

### espn_nba_calendar {#espn_nba_calendar}

`espn_nba_calendar(season=None, ondays=None, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nba_calendar - look up the NBA calendar for a given season from ESPN

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `None` | Used to define different seasons. 2002 is the earliest available season. |
| `ondays` | `boolean` | `None` | Used to return dates for calendar ondays |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing calendar dates for the requested season.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season year. |
| `datetime` | String | The calendar day as ESPN sends it, an ISO 8601 UTC timestamp string. |
| `date` | String | Date in YYYY-MM-DD format. |
| `year` | String | 4-digit year. |
| `month` | String | Month of the calendar day as a two-digit string. |
| `day` | String | Day number within the month. |
| `dateURL` | String | The calendar day as YYYYMMDD, the dates= value of that day's scoreboard request. |
| `url` | String | RotoWire player page URL. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `season` is less than 2002. |

**Example**

```python
from sportsdataverse.nba import espn_nba_calendar
cal = espn_nba_calendar(season=2023)
print(cal.shape)

# Use ondays to get every scheduled date for the season

ondays = espn_nba_calendar(season=2023, ondays=True)

# Pipeline next step (loop the URLs to scrape day-by-day)

cal = espn_nba_calendar(season=2023, ondays=True)
urls = cal["url"].to_list()  # feed each into espn_nba_schedule
```

### espn_nba_game_rosters {#espn_nba_game_rosters}

`espn_nba_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame | pd.DataFrame | dict'`

espn_nba_game_rosters() - Pull the game by id.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from espn_nba_schedule(). |
| `raw` | `bool` | `False` | If True, returns the Core v2 competitors payload (dict) as ESPN sent it; its roster, team and athlete `$ref` links are not followed. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe of game roster data with columns: 'athlete_id', 'athlete_uid', 'athlete_guid', 'athlete_type', 'first_name', 'last_name', 'full_name', 'athlete_display_name', 'short_name', 'weight', 'display_weight', 'height', 'display_height', 'age', 'date_of_birth', 'slug', 'jersey', 'linked', 'active', 'alternate_ids_sdr', 'birth_place_city', 'birth_place_state', 'birth_place_country', 'headshot_href', 'headshot_alt', 'experience_years', 'experience_display_value', 'experience_abbreviation', 'status_id', 'status_name', 'status_type', 'status_abbreviation', 'hand_type', 'hand_abbreviation', 'hand_display_value', 'draft_display_text', 'draft_round', 'draft_year', 'draft_selection', 'player_id', 'starter', 'valid', 'did_not_play', 'display_name', 'ejected', 'athlete_href', 'position_href', 'statistics_href', 'team_id', 'team_guid', 'team_uid', 'team_slug', 'team_location', 'team_name', 'team_abbreviation', 'team_display_name', 'team_short_display_name', 'team_color', 'team_alternate_color', 'is_active', 'is_all_star', 'logo_href', 'logo_dark_href', 'game_id' plus the athlete's `position_id`, `position_name`, `position_display_name`, `position_abbreviation` and `position_leaf`. With `raw=True`, the competitors payload (dict).

| col_name | type | description |
|---|---|---|
| `athlete_id` | Int64 | Unique athlete identifier (ESPN). |
| `athlete_uid` | String | ESPN athlete UID (universal identifier). |
| `athlete_guid` | String | ESPN athlete GUID. |
| `athlete_type` | String | Athlete type / class. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `full_name` | String | Player's full name. |
| `athlete_display_name` | String | Athlete display name (full). |
| `short_name` | String | Short display name. |
| `weight` | Float64 | Player weight in pounds. |
| `display_weight` | String | Player weight in display format (e.g. '180 lbs'). |
| `height` | Float64 | Player height (string e.g. '6-2' or inches). |
| `display_height` | String | Player height in display format (e.g. '6-2'). |
| `age` | Int64 | Player age (in years). |
| `date_of_birth` | String | Date of birth (YYYY-MM-DD). |
| `debut_year` | Int64 | Year of professional debut. |
| `citizenship` | String | Citizenship. |
| `slug` | String | URL-safe identifier. |
| `jersey` | String | Jersey number worn by the player. |
| `linked` | Boolean | TRUE if the record is linked to a related entity. |
| `active` | Boolean | TRUE if the row represents an active record (player / team / season). |
| `alternate_ids_sdr` | String |  |
| `birth_place_city` | String | Birth place city. |
| `birth_place_country` | String | Birth place country. |
| `headshot_href` | String | Headshot image URL. |
| `headshot_alt` | String | Alternative-text label for the headshot. |
| `contracts_href` | String |  |
| `experience_years` | Int64 | Experience years. |
| `contract_href` | String |  |
| `contract_bird_status` | Int64 | Contract bird status. |
| `contract_base_year_compensation_active` | Boolean | Contract base year compensation active. |
| `contract_poison_pill_provision_active` | Boolean |  |
| `contract_incoming_trade_value` | Int64 | Contract incoming trade value. |
| `contract_outgoing_trade_value` | Int64 | Contract outgoing trade value. |
| `contract_minimum_salary_exception` | Boolean | Contract minimum salary exception. |
| `contract_option_type` | Int64 | Contract option type. |
| `contract_salary` | Int64 | Contract salary. |
| `contract_salary_remaining` | Int64 | Contract salary remaining. |
| `contract_years_remaining` | Int64 | Contract years remaining. |
| `contract_season_href` | String |  |
| `contract_team_href` | String |  |
| `contract_trade_kicker_active` | Boolean | Contract trade kicker active. |
| `contract_trade_kicker_percentage` | Float64 | Contract trade kicker percentage (0-1 decimal). |
| `contract_trade_kicker_value` | Int64 | Contract trade kicker value. |
| `contract_trade_kicker_trade_value` | Int64 | Contract trade kicker trade value. |
| `contract_trade_restriction` | Boolean | Contract trade restriction. |
| `contract_unsigned_foreign_pick` | Boolean | Contract unsigned foreign pick. |
| `contract_active` | Boolean | Contract active. |
| `draft_display_text` | String | Draft display text. |
| `draft_round` | Int64 | Round of the draft selection. |
| `draft_year` | Int64 | Draft year (4-digit). |
| `draft_selection` | Int64 | Draft selection. |
| `draft_team_href` | String |  |
| `draft_pick_href` | String |  |
| `status_id` | Int64 | Status identifier. |
| `status_name` | String | Status label. |
| `status_type` | String | Status type. |
| `status_abbreviation` | String | Status abbreviation. |
| `position_id` | Int64 | ESPN's numeric identifier of the athlete's listed position (the Core v2 position resource id). |
| `position_name` | String | Name of the athlete's listed position from ESPN's position resource, e.g. Guard or Forward. |
| `position_display_name` | String | Display form of the athlete's listed position as ESPN renders it; usually equal to position_name. |
| `position_abbreviation` | String | Short code of the athlete's listed position (e.g. G, F, RW, OL), from ESPN's position resource. |
| `position_leaf` | Boolean | Whether the position is a leaf of ESPN's position tree; False marks a grouping, not a specific role. |
| `birth_place_state` | String | Birth place state. |
| `college_athlete_href` | String |  |
| `hand_type` | String | Hand type. |
| `hand_abbreviation` | String | Hand abbreviation. |
| `hand_display_value` | String | Hand display value. |
| `starter` | Boolean | TRUE if the player was in the starting lineup; FALSE otherwise. |
| `jersey_right` | String |  |
| `valid` | Boolean | Valid. |
| `did_not_play` | Boolean | TRUE if the player did not appear in the game. |
| `display_name` | String | Display name. |
| `reason` | String | Reason. |
| `ejected` | Boolean | TRUE if the player was ejected from the game. |
| `athlete_href` | String |  |
| `position_href` | String |  |
| `statistics_href` | String |  |
| `team_id` | Int64 | Unique team identifier. |
| `order` | Int64 | Display order within the result set. |
| `home_away` | String | Game venue label ('home' or 'away'). |
| `winner` | Boolean | Winner. |
| `team_guid` | String | ESPN team GUID. |
| `team_uid` | String | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_location` | String | Team city or location string. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | String | Full team display name. |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `team_alternate_color` | String | Team alternate color (hex without leading '#'). |
| `is_active` | Boolean | Is active. |
| `is_all_star` | Boolean | Is all star. |
| `logo_href` | String | Team or league logo URL. |
| `logo_dark_href` | String | Logo URL for dark backgrounds. |
| `game_id` | Int64 | Unique game identifier. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | ESPN has no competitors, roster or athlete resource for the game (404), or every rostered athlete's `$ref` answered 404. A single 404 athlete is left out with a `UserWarning` and the rest of the roster is returned. |
| `AssetFetchError` | A fetch failed (403, 429, 5xx, a dropped connection); never treated as a missing athlete. |

**Example**

```python
from sportsdataverse.nba import espn_nba_game_rosters
rosters = espn_nba_game_rosters(game_id=401585183)
print(rosters.shape)

# Pandas round-trip

rosters_pd = espn_nba_game_rosters(game_id=401585183, return_as_pandas=True)
rosters_pd.head()

# Pipeline next step (filter to game starters)

import polars as pl
starters = espn_nba_game_rosters(game_id=401585183).filter(
    pl.col("starter") == True
)
```

### espn_nba_pbp {#espn_nba_pbp}

`espn_nba_pbp(game_id: 'int', raw: 'bool' = False, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs)`

espn_nba_pbp() - Pull one game's play-by-play from ESPN's `nba/summary` endpoint.

By default the plays come back as the frame the `espn_nba_pbp` release publishes (what
`~sportsdataverse.nba.load_nba_pbp` reads): snake_case columns, Int32 team / athlete /
game ids, the Int64 play `id`, `game_date` / `game_date_time`, the hoopR shot
coordinates, `athlete_name_{1,2,3}`, and `home_score` / `away_score` rebuilt from the
scoring plays when that reproduces the final (`score_source` says which).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from nba_schedule(). |
| `raw` | `bool` | `False` | If True, returns the raw json from the API endpoint (wins over `return_parsed`). |
| `return_parsed` | `bool` | `True` | If True (default), returns the release play-by-play frame. If False, returns the game dictionary (the shape before 0.2.0, which `helper_nba_play_by_play` / `helper_nba_team_box` / `helper_nba_player_box` and `game_page_frames` read). |
| `return_as_pandas` | `bool` | `False` | If True, returns the frame as pandas instead of polars. |

**Returns**

One row per play, columns and dtypes as the release (`load_nba_pbp`) for that season. Empty (no columns) when ESPN has no play-by-play for the game (`playByPlaySource == "none"`) or 10 or fewer plays, as hoopR returns NULL. With `return_parsed=False`, a dictionary of game data with keys - "gameId", "plays", "winprobability", "boxscore", "header", "broadcasts", "videos", "playByPlaySource", "standings", "leaders", "seasonseries", "timeouts", "pickcenter", "againstTheSpread", "odds", "predictor", "espnWP", "gameInfo", "season".

**Example**

```python
from sportsdataverse.nba import espn_nba_pbp
plays = espn_nba_pbp(game_id=401585183)
print(plays.shape)

# Pipeline next step (made threes)

import polars as pl
plays.filter(pl.col("scoring_play") == True, pl.col("score_value") == 3).height

# The game dictionary (box score, header, pickcenter, ...)

pbp = espn_nba_pbp(game_id=401585183, return_parsed=False)
print(list(pbp.keys()))

# Pull only the raw ESPN summary payload (skip cleaning)

raw_pbp = espn_nba_pbp(game_id=401585183, raw=True)
```

### scoreboard_event_parsing {#scoreboard_event_parsing}

`scoreboard_event_parsing(event)`

Internal helper that flattens an ESPN NBA scoreboard event dict into a

shape suitable for `pd.json_normalize`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` | `dict` |  | A single scoreboard `events[*]` entry from the ESPN NBA scoreboard API. |

**Returns**

The same event dict, mutated in place with `home`/`away` copies of the competitors and trimmed of unused link/odds keys.

**Example**

```python
from sportsdataverse.nba import espn_nba_schedule
sched = espn_nba_schedule(dates=20230102)
```
