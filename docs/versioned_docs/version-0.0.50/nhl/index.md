<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [sportsdataverse.nhl package](#sportsdataversenhl-package)
  - [Submodules](#submodules)
  - [sportsdataverse.nhl.nhl_api module](#sportsdataversenhlnhl_api-module)
    - [sportsdataverse.nhl.nhl_api.nhl_api_pbp(game_id: int, \*\*kwargs) → Dict](#sportsdataversenhlnhl_apinhl_api_pbpgame_id-int-%5C%5Ckwargs-%E2%86%92-dict)
    - [Example](#example)
    - [sportsdataverse.nhl.nhl_api.nhl_api_schedule(start_date: str, end_date: str, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversenhlnhl_apinhl_api_schedulestart_date-str-end_date-str-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-1)
  - [sportsdataverse.nhl.nhl_game_rosters module](#sportsdataversenhlnhl_game_rosters-module)
    - [sportsdataverse.nhl.nhl_game_rosters.espn_nhl_game_rosters(game_id: int, raw=False, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversenhlnhl_game_rostersespn_nhl_game_rostersgame_id-int-rawfalse-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-2)
    - [sportsdataverse.nhl.nhl_game_rosters.helper_nhl_athlete_items(teams_rosters, \*\*kwargs)](#sportsdataversenhlnhl_game_rostershelper_nhl_athlete_itemsteams_rosters-%5C%5Ckwargs)
    - [sportsdataverse.nhl.nhl_game_rosters.helper_nhl_game_items(summary)](#sportsdataversenhlnhl_game_rostershelper_nhl_game_itemssummary)
    - [sportsdataverse.nhl.nhl_game_rosters.helper_nhl_roster_items(items, summary_url, \*\*kwargs)](#sportsdataversenhlnhl_game_rostershelper_nhl_roster_itemsitems-summary_url-%5C%5Ckwargs)
    - [sportsdataverse.nhl.nhl_game_rosters.helper_nhl_team_items(items, \*\*kwargs)](#sportsdataversenhlnhl_game_rostershelper_nhl_team_itemsitems-%5C%5Ckwargs)
  - [sportsdataverse.nhl.nhl_loaders module](#sportsdataversenhlnhl_loaders-module)
    - [sportsdataverse.nhl.nhl_loaders.load_nhl_pbp(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversenhlnhl_loadersload_nhl_pbpseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-3)
    - [sportsdataverse.nhl.nhl_loaders.load_nhl_player_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversenhlnhl_loadersload_nhl_player_boxscoreseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-4)
    - [sportsdataverse.nhl.nhl_loaders.load_nhl_schedule(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversenhlnhl_loadersload_nhl_scheduleseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-5)
    - [sportsdataverse.nhl.nhl_loaders.load_nhl_team_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversenhlnhl_loadersload_nhl_team_boxscoreseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-6)
    - [sportsdataverse.nhl.nhl_loaders.nhl_teams(return_as_pandas=False) → DataFrame](#sportsdataversenhlnhl_loadersnhl_teamsreturn_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-7)
  - [sportsdataverse.nhl.nhl_pbp module](#sportsdataversenhlnhl_pbp-module)
    - [sportsdataverse.nhl.nhl_pbp.espn_nhl_pbp(game_id: int, raw=False, \*\*kwargs) → Dict](#sportsdataversenhlnhl_pbpespn_nhl_pbpgame_id-int-rawfalse-%5C%5Ckwargs-%E2%86%92-dict)
    - [Example](#example-8)
    - [sportsdataverse.nhl.nhl_pbp.helper_nhl_game_data(pbp_txt, init)](#sportsdataversenhlnhl_pbphelper_nhl_game_datapbp_txt-init)
    - [sportsdataverse.nhl.nhl_pbp.helper_nhl_pbp(game_id, pbp_txt)](#sportsdataversenhlnhl_pbphelper_nhl_pbpgame_id-pbp_txt)
    - [sportsdataverse.nhl.nhl_pbp.helper_nhl_pbp_features(game_id, pbp_txt, init)](#sportsdataversenhlnhl_pbphelper_nhl_pbp_featuresgame_id-pbp_txt-init)
    - [sportsdataverse.nhl.nhl_pbp.helper_nhl_pickcenter(pbp_txt)](#sportsdataversenhlnhl_pbphelper_nhl_pickcenterpbp_txt)
    - [sportsdataverse.nhl.nhl_pbp.nhl_pbp_disk(game_id, path_to_json)](#sportsdataversenhlnhl_pbpnhl_pbp_diskgame_id-path_to_json)
  - [sportsdataverse.nhl.nhl_schedule module](#sportsdataversenhlnhl_schedule-module)
    - [sportsdataverse.nhl.nhl_schedule.espn_nhl_calendar(season=None, ondays=None, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversenhlnhl_scheduleespn_nhl_calendarseasonnone-ondaysnone-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-9)
    - [sportsdataverse.nhl.nhl_schedule.espn_nhl_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversenhlnhl_scheduleespn_nhl_scheduledatesnone-season_typenone-limit500-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-10)
    - [sportsdataverse.nhl.nhl_schedule.most_recent_nhl_season()](#sportsdataversenhlnhl_schedulemost_recent_nhl_season)
    - [Example](#example-11)
    - [sportsdataverse.nhl.nhl_schedule.scoreboard_event_parsing(event)](#sportsdataversenhlnhl_schedulescoreboard_event_parsingevent)
    - [sportsdataverse.nhl.nhl_schedule.year_to_season(year)](#sportsdataversenhlnhl_scheduleyear_to_seasonyear)
    - [Example](#example-12)
  - [sportsdataverse.nhl.nhl_teams module](#sportsdataversenhlnhl_teams-module)
    - [sportsdataverse.nhl.nhl_teams.espn_nhl_teams(return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversenhlnhl_teamsespn_nhl_teamsreturn_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-13)
  - [Module contents](#module-contents)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# sportsdataverse.nhl package

## Submodules

## sportsdataverse.nhl.nhl_api module

### sportsdataverse.nhl.nhl_api.nhl_api_pbp(game_id: int, \*\*kwargs) → Dict

nhl_api_pbp() - Pull the game by id. Data from API endpoints - nhl/playbyplay, nhl/summary

* **Parameters:**
  **game_id** (*int*) – Unique game_id, can be obtained from nhl_schedule().
* **Returns:**
  Dictionary of game data with keys - “gameId”, “plays”, “boxscore”, “header”, “broadcasts”,
  : ”videos”, “playByPlaySource”, “standings”, “leaders”, “seasonseries”, “pickcenter”, “againstTheSpread”,
    “odds”, “onIce”, “gameInfo”, “season”
* **Return type:**
  Dict

### Example

Pull a single game’s metadata via the legacy NHL Stats API endpoint:

```default
from sportsdataverse.nhl import nhl_api_pbp
game = nhl_api_pbp(game_id=2021020079)
sorted(game.keys())  # ['datetime', 'game', 'gameId', 'gameLink', 'players', 'status', 'teams', 'venues']
print(game["gameId"], game["status"]["abstractGameState"])
```

Inspect the home / away team summary blocks:

```default
game["teams"]["home"]["name"], game["teams"]["away"]["name"]
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

### sportsdataverse.nhl.nhl_api.nhl_api_schedule(start_date: str, end_date: str, return_as_pandas=False, \*\*kwargs) → DataFrame

nhl_api_schedule() - Pull the schedule by start and end date. Data from API endpoints - nhl/schedule

* **Parameters:**
  * **start_date** (*str*) – Start date to pull the NHL API schedule.
  * **end_date** (*str*) – End date to pull the NHL API schedule.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the schedule for the requested seasons.
* **Return type:**
  pl.DataFrame

### Example

Pull a one-week schedule slice:

```default
from sportsdataverse.nhl import nhl_api_schedule
sched = nhl_api_schedule(start_date="2021-10-23", end_date="2021-10-28")
print(sched.shape)
sched.select(["gamePk", "gameDate", "teams.home.team.name", "teams.away.team.name"]).head()
```

Pandas round-trip:

```default
sched_pd = nhl_api_schedule(
    start_date="2021-10-23", end_date="2021-10-28", return_as_pandas=True
)
sched_pd[["gamePk", "gameDate", "status.detailedState"]].head()
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

## sportsdataverse.nhl.nhl_game_rosters module

### sportsdataverse.nhl.nhl_game_rosters.espn_nhl_game_rosters(game_id: int, raw=False, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_nhl_game_rosters() - Pull the game by id.

* **Parameters:**
  * **game_id** (*int*) – Unique game_id, can be obtained from espn_nhl_schedule().
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe of game roster data with columns:
  ‘athlete_id’, ‘athlete_uid’, ‘athlete_guid’, ‘athlete_type’,
  ‘first_name’, ‘last_name’, ‘full_name’, ‘athlete_display_name’,
  ‘short_name’, ‘weight’, ‘display_weight’, ‘height’, ‘display_height’,
  ‘age’, ‘date_of_birth’, ‘slug’, ‘jersey’, ‘linked’, ‘active’,
  ‘alternate_ids_sdr’, ‘birth_place_city’, ‘birth_place_state’,
  ‘birth_place_country’, ‘headshot_href’, ‘headshot_alt’,
  ‘experience_years’, ‘experience_display_value’,
  ‘experience_abbreviation’, ‘status_id’, ‘status_name’, ‘status_type’,
  ‘status_abbreviation’, ‘hand_type’, ‘hand_abbreviation’,
  ‘hand_display_value’, ‘draft_display_text’, ‘draft_round’, ‘draft_year’,
  ‘draft_selection’, ‘player_id’, ‘starter’, ‘valid’, ‘did_not_play’,
  ‘display_name’, ‘ejected’, ‘athlete_href’, ‘position_href’,
  ‘statistics_href’, ‘team_id’, ‘team_guid’, ‘team_uid’, ‘team_slug’,
  ‘team_location’, ‘team_name’, ‘team_abbreviation’,
  ‘team_display_name’, ‘team_short_display_name’, ‘team_color’,
  ‘team_alternate_color’, ‘is_active’, ‘is_all_star’,
  ‘logo_href’, ‘logo_dark_href’, ‘game_id’
* **Return type:**
  pl.DataFrame

### Example

Pull both teams’ rosters for a single game (Stanley Cup Final 2023):

```default
from sportsdataverse.nhl import espn_nhl_game_rosters
rosters = espn_nhl_game_rosters(game_id=401559395)
print(rosters.shape)
rosters.select(["athlete_display_name", "jersey", "team_abbreviation", "starter"]).head(10)
```

Just the starters:

```default
import polars as pl
rosters.filter(pl.col("starter") == True).select(["athlete_display_name", "team_abbreviation"])
```

Pandas round-trip:

```default
rosters_pd = espn_nhl_game_rosters(game_id=401559395, return_as_pandas=True)
rosters_pd[["athlete_display_name", "team_abbreviation", "did_not_play"]].head()
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

### sportsdataverse.nhl.nhl_game_rosters.helper_nhl_athlete_items(teams_rosters, \*\*kwargs)

### sportsdataverse.nhl.nhl_game_rosters.helper_nhl_game_items(summary)

### sportsdataverse.nhl.nhl_game_rosters.helper_nhl_roster_items(items, summary_url, \*\*kwargs)

### sportsdataverse.nhl.nhl_game_rosters.helper_nhl_team_items(items, \*\*kwargs)

## sportsdataverse.nhl.nhl_loaders module

### sportsdataverse.nhl.nhl_loaders.load_nhl_pbp(seasons: List[int], return_as_pandas=False) → DataFrame

Load NHL play by play data going back to 2011

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2011 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the play-by-plays available for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2011.

### Example

Pull a single season’s play-by-play parquet:

```default
from sportsdataverse.nhl import load_nhl_pbp
pbp = load_nhl_pbp(seasons=2023)
print(pbp.shape)
```

Pull a range of seasons:

```default
pbp = load_nhl_pbp(seasons=range(2018, 2024))
pbp.group_by("season").len().sort("season")
```

Filter to goal events and round-trip to pandas:

```default
import polars as pl
goals = pbp.filter(pl.col("type_text") == "Goal")
goals_pd = goals.to_pandas()
goals_pd[["season", "period", "time", "text"]].head()
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

### sportsdataverse.nhl.nhl_loaders.load_nhl_player_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame

Load NHL player boxscore data

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2011 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the
  player boxscores available for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2011.

### Example

Pull player box scores for a single season:

```default
from sportsdataverse.nhl import load_nhl_player_boxscore
pb = load_nhl_player_boxscore(seasons=2023)
print(pb.shape)
```

Top 10 single-game point performers:

```default
import polars as pl
pb.with_columns(points=pl.col("goals") + pl.col("assists")).sort(
    "points", descending=True
).select(["game_id", "athlete_display_name", "goals", "assists", "points"]).head(10)
```

Pandas round-trip across multiple seasons:

```default
pb_pd = load_nhl_player_boxscore(seasons=range(2020, 2024), return_as_pandas=True)
pb_pd.groupby("season")[["goals", "assists"]].sum()
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

### sportsdataverse.nhl.nhl_loaders.load_nhl_schedule(seasons: List[int], return_as_pandas=False) → DataFrame

Load NHL schedule data

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2002 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the schedule for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2002.

### Example

Pull a single season’s schedule:

```default
from sportsdataverse.nhl import load_nhl_schedule
sched = load_nhl_schedule(seasons=2023)
print(sched.shape)
```

Pull a range of seasons and count by status:

```default
sched = load_nhl_schedule(seasons=range(2018, 2024))
sched.group_by(["season", "status_type_description"]).len().sort(["season", "len"])
```

Pandas round-trip with a single season:

```default
sched_pd = load_nhl_schedule(seasons=[2023], return_as_pandas=True)
sched_pd[["game_id", "home_name", "away_name", "game_date"]].head()
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

### sportsdataverse.nhl.nhl_loaders.load_nhl_team_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame

Load NHL team boxscore data

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2011 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the
  team boxscores available for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2011.

### Example

Pull team box scores for a single season:

```default
from sportsdataverse.nhl import load_nhl_team_boxscore
tb = load_nhl_team_boxscore(seasons=2023)
print(tb.shape)
```

Pull a range of seasons:

```default
tb = load_nhl_team_boxscore(seasons=range(2018, 2024))
tb.group_by("season").len().sort("season")
```

Tampa Bay Lightning (team_id 14) game-by-game scoring:

```default
import polars as pl
tb.filter(pl.col("team_id") == 14).select(["game_id", "team_score", "opponent_team_score"]).head()
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

### sportsdataverse.nhl.nhl_loaders.nhl_teams(return_as_pandas=False) → DataFrame

Load NHL team ID information and logos

* **Parameters:**
  **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing teams available for the requested seasons.
* **Return type:**
  pl.DataFrame

### Example

Pull the static teams + logos table:

```default
from sportsdataverse.nhl import nhl_teams
teams = nhl_teams()
print(teams.shape)
teams.head()
```

Pandas round-trip — convenient for joining against your own roster table:

```default
teams_pd = nhl_teams(return_as_pandas=True)
list(teams_pd.columns)[:10]
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

## sportsdataverse.nhl.nhl_pbp module

### sportsdataverse.nhl.nhl_pbp.espn_nhl_pbp(game_id: int, raw=False, \*\*kwargs) → Dict

espn_nhl_pbp() - Pull the game by id. Data from API endpoints - nhl/playbyplay, nhl/summary

* **Parameters:**
  **game_id** (*int*) – Unique game_id, can be obtained from nhl_schedule().
* **Returns:**
  Dictionary of game data with keys - “gameId”, “plays”, “boxscore”, “header”, “broadcasts”,
  : ”videos”, “playByPlaySource”, “standings”, “leaders”, “seasonseries”, “pickcenter”, “againstTheSpread”,
    “odds”, “onIce”, “gameInfo”, “season”
* **Return type:**
  Dict

### Example

Pull a single game’s parsed feed (Stanley Cup Finals 2023 game):

```default
from sportsdataverse.nhl import espn_nhl_pbp
game = espn_nhl_pbp(game_id=401559395)
list(game.keys())  # 'gameId', 'plays', 'boxscore', ...
```

Inspect parsed plays and a quick filter on goal events:

```default
import polars as pl
plays = pl.DataFrame(game["plays"])
print(plays.shape)
goals = plays.filter(pl.col("type.text") == "Goal")
goals.select(["period", "time", "text"]).head()
```

Pull the unparsed payload for custom downstream parsing:

```default
raw = espn_nhl_pbp(game_id=401559395, raw=True)
sorted(raw.keys())[:5]
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

### sportsdataverse.nhl.nhl_pbp.helper_nhl_game_data(pbp_txt, init)

### sportsdataverse.nhl.nhl_pbp.helper_nhl_pbp(game_id, pbp_txt)

### sportsdataverse.nhl.nhl_pbp.helper_nhl_pbp_features(game_id, pbp_txt, init)

### sportsdataverse.nhl.nhl_pbp.helper_nhl_pickcenter(pbp_txt)

### sportsdataverse.nhl.nhl_pbp.nhl_pbp_disk(game_id, path_to_json)

## sportsdataverse.nhl.nhl_schedule module

### sportsdataverse.nhl.nhl_schedule.espn_nhl_calendar(season=None, ondays=None, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_nhl_calendar - look up the NHL calendar for a given season

* **Parameters:**
  * **season** (*int*) – Used to define different seasons. 2002 is the earliest available season.
  * **ondays** (*boolean*) – Used to return dates for calendar ondays
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing calendar dates for the requested season.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2002.

### Example

Calendar dates for a season:

```default
from sportsdataverse.nhl import espn_nhl_calendar
cal = espn_nhl_calendar(season=2023)
print(cal.shape)
cal.head()
```

Just the on-days (game-played dates), useful for batch loops:

```default
ondays = espn_nhl_calendar(season=2023, ondays=True)
for url in ondays["url"].head(3).to_list():
    print(url)
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

### sportsdataverse.nhl.nhl_schedule.espn_nhl_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_nhl_schedule - look up the NHL schedule for a given date

* **Parameters:**
  * **dates** (*int*) – Used to define different seasons. 2002 is the earliest available season.
  * **season_type** (*int*) – season type, 1 for pre-season, 2 for regular season, 3 for post-season, 4 for all-star, 5 for off-season
  * **limit** (*int*) – number of records to return, default: 500.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing schedule dates for the requested season. Returns None if no games
* **Return type:**
  pl.DataFrame

### Example

Pull a single date’s slate (YYYYMMDD):

```default
from sportsdataverse.nhl import espn_nhl_schedule
sched = espn_nhl_schedule(dates=20230613)  # 2023 Stanley Cup Final game date
print(sched.shape)
sched.select(["game_id", "home_name", "away_name", "status_type_description"]).head()
```

Pull a regular-season slate from a season-year:

```default
reg = espn_nhl_schedule(dates=2023, season_type=2, limit=500)
reg.group_by("status_type_description").len().sort("len", descending=True)
```

Pandas round-trip for one date:

```default
espn_nhl_schedule(dates=20230613, return_as_pandas=True).head()
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

### sportsdataverse.nhl.nhl_schedule.most_recent_nhl_season()

most_recent_nhl_season - return the season year for “today”.

NHL seasons are labeled by the year they end in. October flips the
label to next calendar year (the new season just started), otherwise
the current calendar year is returned.

* **Returns:**
  A season year suitable for season-aware loaders / schedule helpers.
* **Return type:**
  int

### Example

Use as a default season for downstream calls:

```default
from sportsdataverse.nhl import most_recent_nhl_season, espn_nhl_calendar
season = most_recent_nhl_season()
cal = espn_nhl_calendar(season=season)
print(season, cal.height)
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

### sportsdataverse.nhl.nhl_schedule.scoreboard_event_parsing(event)

### sportsdataverse.nhl.nhl_schedule.year_to_season(year)

year_to_season - format a starting year as the canonical `YYYY-YY` season string.

NHL season strings (used by `statsapi` / `api-web.nhle.com`) are of the form
`"2023-24"`. This helper converts a starting year (`2023`) into that string.

* **Parameters:**
  **year** – Starting calendar year of the season (e.g. `2023`).
* **Returns:**
  Season string formatted as `"YYYY-YY"`.
* **Return type:**
  str

### Example

Convert a starting year:

```default
from sportsdataverse.nhl import year_to_season
year_to_season(2023)  # '2023-24'
year_to_season(2009)  # '2009-10'
year_to_season(1999)  # '1999-00'
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

## sportsdataverse.nhl.nhl_teams module

### sportsdataverse.nhl.nhl_teams.espn_nhl_teams(return_as_pandas=False, \*\*kwargs) → DataFrame

espn_nhl_teams - look up NHL teams

* **Parameters:**
  **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing teams for the requested league.
  This function caches by default, so if you want to refresh the data, use the command
  sportsdataverse.nhl.espn_nhl_teams.clear_cache().
* **Return type:**
  pl.DataFrame

### Example

Pull the full NHL team directory:

```default
from sportsdataverse.nhl import espn_nhl_teams
teams = espn_nhl_teams()
print(teams.shape)
teams.select(["team_id", "team_abbreviation", "team_display_name"]).head()
```

Find Tampa Bay Lightning (team_id 14):

```default
import polars as pl
teams.filter(pl.col("team_id") == "14").to_dicts()
```

Refresh the cache (the call is `lru_cache`’d) and round-trip to pandas:

```default
espn_nhl_teams.cache_clear()
teams_pd = espn_nhl_teams(return_as_pandas=True)
teams_pd[["team_id", "team_abbreviation", "team_display_name"]].head()
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

## Module contents
