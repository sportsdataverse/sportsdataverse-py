# Men's college basketball — additional Python functions — ESPN

> Men's college basketball — additional Python functions — ESPN — function reference in sdv-py, the SportsDataverse Python package.

### espn_mbb_calendar {#espn_mbb_calendar}

`espn_mbb_calendar(season=None, ondays=None, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_mbb_calendar - look up the men's college basketball calendar for a given season

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `None` | Used to define different seasons. 2002 is the earliest available season; `None` means the current season. |
| `ondays` | `boolean` | `None` | Used to return dates for calendar ondays |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing calendar dates for the requested season.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season year. |
| `datetime` | String | The calendar day as ESPN sends it, an ISO 8601 UTC timestamp string (e.g. 2024-11-04T08:00Z). |
| `date` | String | Date in YYYY-MM-DD format. |
| `year` | String | 4-digit year. |
| `month` | String | Month of the calendar day as a two-digit string, e.g. 11. |
| `day` | String | Day number within the month. |
| `dateURL` | String | The calendar day as YYYYMMDD, the dates= value of that day's scoreboard request. |
| `url` | String | RotoWire player page URL. |

**Raises**

| Exception | Description |
|---|---|
| `SeasonNotFoundError` | If `season` is less than 2002. |

**Example**

```python
from sportsdataverse.mbb import espn_mbb_calendar
cal = espn_mbb_calendar(season=2024)
cal.head()

# On-days only (dates with games on the schedule)

ondays = espn_mbb_calendar(season=2024, ondays=True)
ondays.head()

# Pandas round-trip

cal_pd = espn_mbb_calendar(season=2024, return_as_pandas=True)
cal_pd.head()
```

### espn_mbb_game_all {#espn_mbb_game_all}

`espn_mbb_game_all(game_id: 'int', return_as_pandas: 'bool' = False, **kwargs)`

espn_mbb_game_all() - Pull one game's plays, team box and player box from one ESPN request.

The hoopR name for this request (`hoopR::espn_mbb_game_all`): a single `mens-college-basketball/summary`
fetch, returned as the three frames `espn_mbb_pbp`, `espn_mbb_team_box` and
`espn_mbb_player_box` would return one request each.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from `espn_mbb_schedule()`. |
| `return_as_pandas` | `bool` | `False` | If True, the three frames are pandas instead of polars. |

**Returns**

hoopR's list names as keys -- `"Plays"` (the `load_mbb_pbp` release frame), `"Team"` (the team box) and `"Player"` (the player box).

**Example**

```python
from sportsdataverse.mbb import espn_mbb_game_all
game = espn_mbb_game_all(game_id=401638645)
print(game["Plays"].shape, game["Team"].shape, game["Player"].shape)

# Pipeline next step (one line)

game["Player"].sort("points", descending=True, nulls_last=True).select("athlete_display_name", "points").head()
```

### espn_mbb_player_box {#espn_mbb_player_box}

`espn_mbb_player_box(game_id: 'int', return_as_pandas: 'bool' = False, **kwargs)`

espn_mbb_player_box() - Pull one game's player box score from ESPN's `mens-college-basketball/summary` endpoint.

The hoopR name for this request (`hoopR::espn_mbb_player_box`): one `summary` fetch run
through `~sportsdataverse.mbb.helper_mbb_player_box`, so the frame is the one the
`espn_mens_college_basketball_player_boxscores` release publishes (what
`~sportsdataverse.mbb.load_mbb_player_boxscore` reads) for that game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from `espn_mbb_schedule()`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas frame instead of polars. |

**Returns**

One row per athlete (did-not-play athletes included, with null stats), away team first, with the release's player-box columns. A frame with no columns when ESPN has no box score for the game.

**Example**

```python
from sportsdataverse.mbb import espn_mbb_player_box
player_box = espn_mbb_player_box(game_id=401638645)
print(player_box.shape)

# Pipeline next step (one line)

player_box.filter(pl.col("did_not_play") == False).select("athlete_display_name", "points")
```

### espn_mbb_team_box {#espn_mbb_team_box}

`espn_mbb_team_box(game_id: 'int', return_as_pandas: 'bool' = False, **kwargs)`

espn_mbb_team_box() - Pull one game's team box score from ESPN's `mens-college-basketball/summary` endpoint.

The hoopR name for this request (`hoopR::espn_mbb_team_box`): one `summary` fetch run
through `~sportsdataverse.mbb.helper_mbb_team_box`, so the frame is the one the
`espn_mens_college_basketball_team_boxscores` release publishes (what
`~sportsdataverse.mbb.load_mbb_team_boxscore` reads) for that game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from `espn_mbb_schedule()`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas frame instead of polars. |

**Returns**

Two rows, one per team, with the release's team-box columns. A frame with no columns when ESPN has no box score for the game.

**Example**

```python
from sportsdataverse.mbb import espn_mbb_team_box
team_box = espn_mbb_team_box(game_id=401638645)
print(team_box.shape)

# Pipeline next step (one line)

team_box.select("team_display_name", "team_score", "team_winner")
```

### scoreboard_event_parsing {#scoreboard_event_parsing}

`scoreboard_event_parsing(event)`

Flatten one ESPN scoreboard event for the schedule frame, in place.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` |  |  | One element of the scoreboard payload's `events` list. |

**Returns**

The same event, modified: `competitions[0]` gains `home` / `away` team dicts (with `score`, `winner`, `currentRank`, `linescores`, `records`), `notes_type` / `notes_headline` and `broadcast_market` / `broadcast_name`, and loses `competitors`, `broadcasts`, `notes`, `odds`, `leaders` and the other nested blocks the schedule frame does not use.
