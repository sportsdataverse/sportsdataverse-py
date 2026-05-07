<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [sportsdataverse.mbb package](#sportsdataversembb-package)
  - [Submodules](#submodules)
  - [sportsdataverse.mbb.mbb_game_rosters module](#sportsdataversembbmbb_game_rosters-module)
    - [sportsdataverse.mbb.mbb_game_rosters.espn_mbb_game_rosters(game_id: int, raw=False, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversembbmbb_game_rostersespn_mbb_game_rostersgame_id-int-rawfalse-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example)
    - [sportsdataverse.mbb.mbb_game_rosters.helper_mbb_athlete_items(teams_rosters, \*\*kwargs)](#sportsdataversembbmbb_game_rostershelper_mbb_athlete_itemsteams_rosters-%5C%5Ckwargs)
    - [sportsdataverse.mbb.mbb_game_rosters.helper_mbb_game_items(summary)](#sportsdataversembbmbb_game_rostershelper_mbb_game_itemssummary)
    - [sportsdataverse.mbb.mbb_game_rosters.helper_mbb_roster_items(items, summary_url, \*\*kwargs)](#sportsdataversembbmbb_game_rostershelper_mbb_roster_itemsitems-summary_url-%5C%5Ckwargs)
    - [sportsdataverse.mbb.mbb_game_rosters.helper_mbb_team_items(items, \*\*kwargs)](#sportsdataversembbmbb_game_rostershelper_mbb_team_itemsitems-%5C%5Ckwargs)
  - [sportsdataverse.mbb.mbb_loaders module](#sportsdataversembbmbb_loaders-module)
    - [sportsdataverse.mbb.mbb_loaders.load_mbb_pbp(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversembbmbb_loadersload_mbb_pbpseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-1)
    - [sportsdataverse.mbb.mbb_loaders.load_mbb_player_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversembbmbb_loadersload_mbb_player_boxscoreseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-2)
    - [sportsdataverse.mbb.mbb_loaders.load_mbb_schedule(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversembbmbb_loadersload_mbb_scheduleseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-3)
    - [sportsdataverse.mbb.mbb_loaders.load_mbb_team_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversembbmbb_loadersload_mbb_team_boxscoreseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-4)
  - [sportsdataverse.mbb.mbb_pbp module](#sportsdataversembbmbb_pbp-module)
    - [sportsdataverse.mbb.mbb_pbp.espn_mbb_pbp(game_id: int, raw=False, \*\*kwargs) → Dict](#sportsdataversembbmbb_pbpespn_mbb_pbpgame_id-int-rawfalse-%5C%5Ckwargs-%E2%86%92-dict)
    - [Example](#example-5)
    - [sportsdataverse.mbb.mbb_pbp.helper_mbb_game_data(pbp_txt, init)](#sportsdataversembbmbb_pbphelper_mbb_game_datapbp_txt-init)
    - [sportsdataverse.mbb.mbb_pbp.helper_mbb_pbp(game_id, pbp_txt)](#sportsdataversembbmbb_pbphelper_mbb_pbpgame_id-pbp_txt)
    - [sportsdataverse.mbb.mbb_pbp.helper_mbb_pbp_features(game_id, pbp_txt, init)](#sportsdataversembbmbb_pbphelper_mbb_pbp_featuresgame_id-pbp_txt-init)
    - [sportsdataverse.mbb.mbb_pbp.helper_mbb_pickcenter(pbp_txt)](#sportsdataversembbmbb_pbphelper_mbb_pickcenterpbp_txt)
    - [sportsdataverse.mbb.mbb_pbp.mbb_pbp_disk(game_id, path_to_json)](#sportsdataversembbmbb_pbpmbb_pbp_diskgame_id-path_to_json)
  - [sportsdataverse.mbb.mbb_schedule module](#sportsdataversembbmbb_schedule-module)
    - [sportsdataverse.mbb.mbb_schedule.espn_mbb_calendar(season=None, ondays=None, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversembbmbb_scheduleespn_mbb_calendarseasonnone-ondaysnone-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-6)
    - [sportsdataverse.mbb.mbb_schedule.espn_mbb_schedule(dates=None, groups=50, season_type=None, limit=500, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversembbmbb_scheduleespn_mbb_scheduledatesnone-groups50-season_typenone-limit500-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-7)
    - [sportsdataverse.mbb.mbb_schedule.most_recent_mbb_season()](#sportsdataversembbmbb_schedulemost_recent_mbb_season)
    - [Example](#example-8)
    - [sportsdataverse.mbb.mbb_schedule.scoreboard_event_parsing(event)](#sportsdataversembbmbb_schedulescoreboard_event_parsingevent)
  - [sportsdataverse.mbb.mbb_teams module](#sportsdataversembbmbb_teams-module)
    - [sportsdataverse.mbb.mbb_teams.espn_mbb_teams(groups=None, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversembbmbb_teamsespn_mbb_teamsgroupsnone-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-9)
  - [Module contents](#module-contents)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# sportsdataverse.mbb package

## Submodules

## sportsdataverse.mbb.mbb_game_rosters module

### sportsdataverse.mbb.mbb_game_rosters.espn_mbb_game_rosters(game_id: int, raw=False, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_mbb_game_rosters() - Pull the game by id.

* **Parameters:**
  * **game_id** (*int*) – Unique game_id, can be obtained from mbb_schedule().
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
  ‘team_location’, ‘team_name’, ‘team_nickname’, ‘team_abbreviation’,
  ‘team_display_name’, ‘team_short_display_name’, ‘team_color’,
  ‘team_alternate_color’, ‘is_active’, ‘is_all_star’,
  ‘team_alternate_ids_sdr’, ‘logo_href’, ‘logo_dark_href’, ‘game_id’
* **Return type:**
  pl.DataFrame

### Example

Quick start (2024 NCAA M championship game):

```default
from sportsdataverse.mbb import espn_mbb_game_rosters
roster = espn_mbb_game_rosters(game_id=401638637)
print(roster.shape)
```

Identify starters:

```default
import polars as pl
starters = roster.filter(pl.col("starter") == True).select(
    ["full_name", "jersey", "team_display_name"]
)
```

Pandas round-trip:

```default
roster_pd = espn_mbb_game_rosters(game_id=401638637, return_as_pandas=True)
roster_pd.head()
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

### sportsdataverse.mbb.mbb_game_rosters.helper_mbb_athlete_items(teams_rosters, \*\*kwargs)

### sportsdataverse.mbb.mbb_game_rosters.helper_mbb_game_items(summary)

### sportsdataverse.mbb.mbb_game_rosters.helper_mbb_roster_items(items, summary_url, \*\*kwargs)

### sportsdataverse.mbb.mbb_game_rosters.helper_mbb_team_items(items, \*\*kwargs)

## sportsdataverse.mbb.mbb_loaders module

### sportsdataverse.mbb.mbb_loaders.load_mbb_pbp(seasons: List[int], return_as_pandas=False) → DataFrame

Load men’s college basketball play by play data going back to 2002

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2002 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the
  play-by-plays available for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2002.

### Example

Single season:

```default
from sportsdataverse.mbb import load_mbb_pbp
pbp = load_mbb_pbp(seasons=[2024])
print(pbp.shape)
```

Range of seasons:

```default
pbp_multi = load_mbb_pbp(seasons=range(2022, 2025))
print(pbp_multi["season"].unique().sort())
```

Pandas round-trip:

```default
pbp_pd = load_mbb_pbp(seasons=[2024], return_as_pandas=True)
pbp_pd.head()
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

### sportsdataverse.mbb.mbb_loaders.load_mbb_player_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame

Load men’s college basketball player boxscore data

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2002 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the
  player boxscores available for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2002.

### Example

Single season:

```default
from sportsdataverse.mbb import load_mbb_player_boxscore
pb = load_mbb_player_boxscore(seasons=[2024])
print(pb.shape)
```

Range of seasons + top scorers:

```default
import polars as pl
pb_multi = load_mbb_player_boxscore(seasons=range(2022, 2025))
top = (
    pb_multi
    .group_by("athlete_display_name")
    .agg(pl.col("points").sum().alias("total_points"))
    .sort("total_points", descending=True)
    .head(10)
)
```

Pandas round-trip:

```default
pb_pd = load_mbb_player_boxscore(seasons=[2024], return_as_pandas=True)
pb_pd.head()
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

### sportsdataverse.mbb.mbb_loaders.load_mbb_schedule(seasons: List[int], return_as_pandas=False) → DataFrame

Load men’s college basketball schedule data

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2002 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the
  schedule for  the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2002.

### Example

Single season:

```default
from sportsdataverse.mbb import load_mbb_schedule
sched = load_mbb_schedule(seasons=[2024])
print(sched.shape)
```

Range of seasons:

```default
sched_multi = load_mbb_schedule(seasons=range(2022, 2025))
print(sched_multi["season"].unique().sort())
```

Pandas round-trip:

```default
sched_pd = load_mbb_schedule(seasons=[2024], return_as_pandas=True)
sched_pd.head()
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

### sportsdataverse.mbb.mbb_loaders.load_mbb_team_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame

Load men’s college basketball team boxscore data

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2002 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the
  team boxscores available for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2002.

### Example

Single season:

```default
from sportsdataverse.mbb import load_mbb_team_boxscore
tb = load_mbb_team_boxscore(seasons=[2024])
print(tb.shape)
```

Range of seasons + filter to a specific team (Duke `team_id=150`):

```default
import polars as pl
tb_multi = load_mbb_team_boxscore(seasons=range(2022, 2025))
duke = tb_multi.filter(pl.col("team_id") == 150)
```

Pandas round-trip:

```default
tb_pd = load_mbb_team_boxscore(seasons=[2024], return_as_pandas=True)
tb_pd.head()
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

## sportsdataverse.mbb.mbb_pbp module

### sportsdataverse.mbb.mbb_pbp.espn_mbb_pbp(game_id: int, raw=False, \*\*kwargs) → Dict

espn_mbb_pbp() - Pull the game by id. Data from API endpoints: mens-college-basketball/playbyplay, mens-college-basketball/summary

* **Parameters:**
  * **game_id** (*int*) – Unique game_id, can be obtained from mbb_schedule().
  * **raw** (*bool*) – If True, returns the raw json from the API endpoint. If False, returns a cleaned dictionary of datasets.
* **Returns:**
  Dictionary of game data with keys: “gameId”, “plays”, “winprobability”, “boxscore”, “header”, “broadcasts”,
  “videos”, “playByPlaySource”, “standings”, “leaders”, “timeouts”, “pickcenter”, “againstTheSpread”, “odds”, “predictor”,
  “espnWP”, “gameInfo”, “season”
* **Return type:**
  Dict

### Example

Quick start (2024 NCAA Division I men’s championship game):

```default
from sportsdataverse.mbb import espn_mbb_pbp
game = espn_mbb_pbp(game_id=401638637)
print(game["gameId"])
print(len(game["plays"]))
```

Filter shooting plays for a basic shot chart:

```default
import polars as pl
plays = pl.DataFrame(game["plays"])
shots = plays.filter(pl.col("shooting_play") == True)
shots.select(
    [
        "period_number",
        "clock_display_value",
        "team_id",
        "coordinate_x",
        "coordinate_y",
        "score_value",
        "text",
    ]
).head()
```

Convert to pandas:

```default
import pandas as pd
plays_pd = pd.DataFrame(game["plays"])
plays_pd[plays_pd["shooting_play"] == True].head()
```

Raw payload (skip the cleaning pipeline) for debugging:

```default
raw = espn_mbb_pbp(game_id=401638637, raw=True)
sorted(raw.keys())
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) - R sister package; mirrors this surface for men’s basketball
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

### sportsdataverse.mbb.mbb_pbp.helper_mbb_game_data(pbp_txt, init)

### sportsdataverse.mbb.mbb_pbp.helper_mbb_pbp(game_id, pbp_txt)

### sportsdataverse.mbb.mbb_pbp.helper_mbb_pbp_features(game_id, pbp_txt, init)

### sportsdataverse.mbb.mbb_pbp.helper_mbb_pickcenter(pbp_txt)

### sportsdataverse.mbb.mbb_pbp.mbb_pbp_disk(game_id, path_to_json)

## sportsdataverse.mbb.mbb_schedule module

### sportsdataverse.mbb.mbb_schedule.espn_mbb_calendar(season=None, ondays=None, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_mbb_calendar - look up the men’s college basketball calendar for a given season

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

Calendar dates for a single season:

```default
from sportsdataverse.mbb import espn_mbb_calendar
cal = espn_mbb_calendar(season=2024)
cal.head()
```

On-days only (dates with games on the schedule):

```default
ondays = espn_mbb_calendar(season=2024, ondays=True)
ondays.head()
```

Pandas round-trip:

```default
cal_pd = espn_mbb_calendar(season=2024, return_as_pandas=True)
cal_pd.head()
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

### sportsdataverse.mbb.mbb_schedule.espn_mbb_schedule(dates=None, groups=50, season_type=None, limit=500, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_mbb_schedule - look up the men’s college basketball scheduler for a given season

* **Parameters:**
  * **dates** (*int*) – Used to define different seasons. 2002 is the earliest available season.
  * **groups** (*int*) – Used to define different divisions. 50 is Division I, 51 is Division II/Division III.
  * **season_type** (*int*) – 2 for regular season, 3 for post-season, 4 for off-season.
  * **limit** (*int*) – number of records to return, default: 500.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing schedule dates for the requested season. Returns None if no games
* **Return type:**
  pl.DataFrame

### Example

Single date (April 8, 2024 - 2024 NCAA M championship day):

```default
from sportsdataverse.mbb import espn_mbb_schedule
day = espn_mbb_schedule(dates=20240408)
print(day.shape)
```

Season-level pull (2024 season):

```default
season = espn_mbb_schedule(dates=2024, limit=1500)
print(season.shape)
```

Filter to a specific team (Duke `team_id=150`):

```default
import polars as pl
duke = season.filter(
    (pl.col("home_id") == "150") | (pl.col("away_id") == "150")
)
```

Pandas round-trip:

```default
season_pd = espn_mbb_schedule(dates=2024, return_as_pandas=True)
season_pd.head()
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

### sportsdataverse.mbb.mbb_schedule.most_recent_mbb_season()

Return the most recent men’s college basketball season year.

The men’s college basketball season spans early November through early
April; for any month October-December the “current season” is the
following calendar year (e.g. October 2025 returns `2026`).

* **Returns:**
  The most recent / current season year.
* **Return type:**
  int

### Example

Use as a default season argument:

```default
from sportsdataverse.mbb import most_recent_mbb_season, espn_mbb_schedule
season = most_recent_mbb_season()
sched = espn_mbb_schedule(dates=season)
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football

### sportsdataverse.mbb.mbb_schedule.scoreboard_event_parsing(event)

## sportsdataverse.mbb.mbb_teams module

### sportsdataverse.mbb.mbb_teams.espn_mbb_teams(groups=None, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_mbb_teams - look up the men’s college basketball teams

* **Parameters:**
  * **groups** (*int*) – Used to define different divisions. 50 is Division I, 51 is Division II/Division III.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing teams for the requested league.
  This function caches by default, so if you want to refresh the data, use the command
  sportsdataverse.mbb.espn_mbb_teams.clear_cache().
* **Return type:**
  pl.DataFrame

### Example

Default groups (D1):

```default
from sportsdataverse.mbb import espn_mbb_teams
teams = espn_mbb_teams()
print(teams.shape)
print(teams.columns[:8])
```

Walk every team-id (handy for batched scrapes):

```default
team_ids = teams["team_id"].to_list()
print(len(team_ids), "D1 teams")
```

Pandas round-trip + Division II/III:

```default
d2_d3 = espn_mbb_teams(groups=51, return_as_pandas=True)
d2_d3.head()
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

## Module contents
