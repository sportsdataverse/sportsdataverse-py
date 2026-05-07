<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [sportsdataverse.nba package](#sportsdataversenba-package)
  - [Submodules](#submodules)
  - [sportsdataverse.nba.nba_game_rosters module](#sportsdataversenbanba_game_rosters-module)
    - [sportsdataverse.nba.nba_game_rosters.espn_nba_game_rosters(game_id: int, raw=False, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversenbanba_game_rostersespn_nba_game_rostersgame_id-int-rawfalse-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example)
    - [sportsdataverse.nba.nba_game_rosters.helper_nba_athlete_items(teams_rosters, \*\*kwargs)](#sportsdataversenbanba_game_rostershelper_nba_athlete_itemsteams_rosters-%5C%5Ckwargs)
    - [Example](#example-1)
    - [sportsdataverse.nba.nba_game_rosters.helper_nba_game_items(summary)](#sportsdataversenbanba_game_rostershelper_nba_game_itemssummary)
    - [Example](#example-2)
    - [sportsdataverse.nba.nba_game_rosters.helper_nba_roster_items(items, summary_url, \*\*kwargs)](#sportsdataversenbanba_game_rostershelper_nba_roster_itemsitems-summary_url-%5C%5Ckwargs)
    - [Example](#example-3)
    - [sportsdataverse.nba.nba_game_rosters.helper_nba_team_items(items, \*\*kwargs)](#sportsdataversenbanba_game_rostershelper_nba_team_itemsitems-%5C%5Ckwargs)
    - [Example](#example-4)
  - [sportsdataverse.nba.nba_loaders module](#sportsdataversenbanba_loaders-module)
    - [sportsdataverse.nba.nba_loaders.load_nba_pbp(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversenbanba_loadersload_nba_pbpseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-5)
    - [sportsdataverse.nba.nba_loaders.load_nba_player_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversenbanba_loadersload_nba_player_boxscoreseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-6)
    - [sportsdataverse.nba.nba_loaders.load_nba_schedule(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversenbanba_loadersload_nba_scheduleseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-7)
    - [sportsdataverse.nba.nba_loaders.load_nba_team_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversenbanba_loadersload_nba_team_boxscoreseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-8)
  - [sportsdataverse.nba.nba_pbp module](#sportsdataversenbanba_pbp-module)
    - [sportsdataverse.nba.nba_pbp.espn_nba_pbp(game_id: int, raw=False, \*\*kwargs) → Dict](#sportsdataversenbanba_pbpespn_nba_pbpgame_id-int-rawfalse-%5C%5Ckwargs-%E2%86%92-dict)
    - [Example](#example-9)
    - [sportsdataverse.nba.nba_pbp.helper_nba_game_data(pbp_txt, init)](#sportsdataversenbanba_pbphelper_nba_game_datapbp_txt-init)
    - [Example](#example-10)
    - [sportsdataverse.nba.nba_pbp.helper_nba_pbp(game_id, pbp_txt)](#sportsdataversenbanba_pbphelper_nba_pbpgame_id-pbp_txt)
    - [Example](#example-11)
    - [sportsdataverse.nba.nba_pbp.helper_nba_pbp_features(game_id, pbp_txt, init)](#sportsdataversenbanba_pbphelper_nba_pbp_featuresgame_id-pbp_txt-init)
    - [Example](#example-12)
    - [sportsdataverse.nba.nba_pbp.helper_nba_pickcenter(pbp_txt)](#sportsdataversenbanba_pbphelper_nba_pickcenterpbp_txt)
    - [Example](#example-13)
    - [sportsdataverse.nba.nba_pbp.nba_pbp_disk(game_id, path_to_json)](#sportsdataversenbanba_pbpnba_pbp_diskgame_id-path_to_json)
    - [Example](#example-14)
  - [sportsdataverse.nba.nba_schedule module](#sportsdataversenbanba_schedule-module)
    - [sportsdataverse.nba.nba_schedule.espn_nba_calendar(season=None, ondays=None, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversenbanba_scheduleespn_nba_calendarseasonnone-ondaysnone-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-15)
    - [sportsdataverse.nba.nba_schedule.espn_nba_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversenbanba_scheduleespn_nba_scheduledatesnone-season_typenone-limit500-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-16)
    - [sportsdataverse.nba.nba_schedule.most_recent_nba_season()](#sportsdataversenbanba_schedulemost_recent_nba_season)
    - [Example](#example-17)
    - [sportsdataverse.nba.nba_schedule.scoreboard_event_parsing(event)](#sportsdataversenbanba_schedulescoreboard_event_parsingevent)
    - [Example](#example-18)
    - [sportsdataverse.nba.nba_schedule.year_to_season(year)](#sportsdataversenbanba_scheduleyear_to_seasonyear)
    - [Example](#example-19)
  - [sportsdataverse.nba.nba_teams module](#sportsdataversenbanba_teams-module)
    - [sportsdataverse.nba.nba_teams.espn_nba_teams(return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversenbanba_teamsespn_nba_teamsreturn_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-20)
  - [Module contents](#module-contents)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# sportsdataverse.nba package

## Submodules

## sportsdataverse.nba.nba_game_rosters module

### sportsdataverse.nba.nba_game_rosters.espn_nba_game_rosters(game_id: int, raw=False, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_nba_game_rosters() - Pull the game by id.

* **Parameters:**
  * **game_id** (*int*) – Unique game_id, can be obtained from espn_nba_schedule().
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

Quick start:

```default
from sportsdataverse.nba import espn_nba_game_rosters
rosters = espn_nba_game_rosters(game_id=401585183)
print(rosters.shape)
```

Pandas round-trip:

```default
rosters_pd = espn_nba_game_rosters(game_id=401585183, return_as_pandas=True)
rosters_pd.head()
```

Pipeline next step (filter to game starters):

```default
import polars as pl
starters = espn_nba_game_rosters(game_id=401585183).filter(
    pl.col("starter") == True
)
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) – R sister package for NBA rosters
  * [wehoop](https://wehoop.sportsdataverse.org) – women’s basketball parallel
  * [nba_api](https://github.com/swar/nba_api) – Python alternative to the NBA Stats API

### sportsdataverse.nba.nba_game_rosters.helper_nba_athlete_items(teams_rosters, \*\*kwargs)

Internal helper that resolves each athlete `$ref` in a team-rosters frame
to the canonical athlete detail row.

* **Parameters:**
  * **teams_rosters** (*pl.DataFrame*) – Output of [`helper_nba_roster_items()`](#sportsdataverse.nba.nba_game_rosters.helper_nba_roster_items)
    (must contain an `athlete_href` column).
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download()`.
* **Returns:**
  One row per resolved athlete.
* **Return type:**
  pl.DataFrame

### Example

Used internally by [`espn_nba_game_rosters()`](#sportsdataverse.nba.nba_game_rosters.espn_nba_game_rosters):

```default
from sportsdataverse.nba import espn_nba_game_rosters
rosters = espn_nba_game_rosters(game_id=401585183)
```

### sportsdataverse.nba.nba_game_rosters.helper_nba_game_items(summary)

Internal helper that flattens the ESPN `competitions/competitors`
summary payload into a polars DataFrame keyed by `team_id`.

* **Parameters:**
  **summary** (*dict*) – Parsed JSON from the ESPN competitors summary endpoint.
* **Returns:**
  Polars dataframe with one row per competitor team in the game.
* **Return type:**
  pl.DataFrame

### Example

Used internally by [`espn_nba_game_rosters()`](#sportsdataverse.nba.nba_game_rosters.espn_nba_game_rosters):

```default
from sportsdataverse.nba import espn_nba_game_rosters
rosters = espn_nba_game_rosters(game_id=401585183)
```

### sportsdataverse.nba.nba_game_rosters.helper_nba_roster_items(items, summary_url, \*\*kwargs)

Internal helper that fetches the roster entries for every team in a game.

* **Parameters:**
  * **items** (*pl.DataFrame*) – Output of [`helper_nba_game_items()`](#sportsdataverse.nba.nba_game_rosters.helper_nba_game_items).
  * **summary_url** (*str*) – Base ESPN summary URL used to derive each team’s
    roster endpoint.
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download()`.
* **Returns:**
  One row per game-roster entry across both teams.
* **Return type:**
  pl.DataFrame

### Example

Used internally by [`espn_nba_game_rosters()`](#sportsdataverse.nba.nba_game_rosters.espn_nba_game_rosters):

```default
from sportsdataverse.nba import espn_nba_game_rosters
rosters = espn_nba_game_rosters(game_id=401585183)
```

### sportsdataverse.nba.nba_game_rosters.helper_nba_team_items(items, \*\*kwargs)

Internal helper that fetches team detail rows for every team referenced in
the competitors summary and returns them as a flat polars DataFrame.

* **Parameters:**
  * **items** (*pl.DataFrame*) – Output of [`helper_nba_game_items()`](#sportsdataverse.nba.nba_game_rosters.helper_nba_game_items).
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download()`.
* **Returns:**
  Team detail rows with logo URLs flattened out.
* **Return type:**
  pl.DataFrame

### Example

Used internally by [`espn_nba_game_rosters()`](#sportsdataverse.nba.nba_game_rosters.espn_nba_game_rosters):

```default
from sportsdataverse.nba import espn_nba_game_rosters
rosters = espn_nba_game_rosters(game_id=401585183)
```

## sportsdataverse.nba.nba_loaders module

### sportsdataverse.nba.nba_loaders.load_nba_pbp(seasons: List[int], return_as_pandas=False) → DataFrame

Load NBA play by play data going back to 2002

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

Quick start:

```default
from sportsdataverse.nba import load_nba_pbp
pbp = load_nba_pbp(seasons=[2023])
print(pbp.shape)
```

Multi-season pull as pandas:

```default
pbp_pd = load_nba_pbp(seasons=range(2020, 2024), return_as_pandas=True)
pbp_pd.head()
```

Pipeline next step (filter to made 3-pointers):

```default
import polars as pl
threes = load_nba_pbp(seasons=[2023]).filter(
    pl.col("type_text") == "3PT Field Goal"
)
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) – R sister package for NBA data
  * [wehoop](https://wehoop.sportsdataverse.org) – women’s basketball parallel
  * [nba_api](https://github.com/swar/nba_api) – Python alternative to the NBA Stats API

### sportsdataverse.nba.nba_loaders.load_nba_player_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame

Load NBA player boxscore data

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

Quick start:

```default
from sportsdataverse.nba import load_nba_player_boxscore
box = load_nba_player_boxscore(seasons=[2023])
print(box.shape)
```

Pandas round-trip:

```default
box_pd = load_nba_player_boxscore(seasons=[2023], return_as_pandas=True)
box_pd.head()
```

Pipeline next step (top season scorers):

```default
import polars as pl
top = (
    load_nba_player_boxscore(seasons=[2023])
    .group_by("athlete_display_name")
    .agg(pl.col("points").sum())
    .sort("points", descending=True)
    .head(10)
)
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) – R sister package for NBA data
  * [wehoop](https://wehoop.sportsdataverse.org) – women’s basketball parallel
  * [nba_api](https://github.com/swar/nba_api) – Python alternative to the NBA Stats API

### sportsdataverse.nba.nba_loaders.load_nba_schedule(seasons: List[int], return_as_pandas=False) → DataFrame

Load NBA schedule data

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

Quick start:

```default
from sportsdataverse.nba import load_nba_schedule
sched = load_nba_schedule(seasons=[2023])
print(sched.shape)
```

Pandas round-trip:

```default
sched_pd = load_nba_schedule(seasons=range(2020, 2024), return_as_pandas=True)
sched_pd.head()
```

Pipeline next step (filter to playoff games):

```default
import polars as pl
playoffs = load_nba_schedule(seasons=[2023]).filter(pl.col("season_type") == 3)
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) – R sister package for NBA data
  * [nba_api](https://github.com/swar/nba_api) – Python alternative to the NBA Stats API

### sportsdataverse.nba.nba_loaders.load_nba_team_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame

Load NBA team boxscore data

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

Quick start:

```default
from sportsdataverse.nba import load_nba_team_boxscore
box = load_nba_team_boxscore(seasons=[2023])
print(box.shape)
```

Pandas round-trip:

```default
box_pd = load_nba_team_boxscore(seasons=[2023], return_as_pandas=True)
box_pd.head()
```

Pipeline next step (compute average team OFF rating):

```default
import polars as pl
avg = (
    load_nba_team_boxscore(seasons=[2023])
    .group_by("team_display_name")
    .agg(pl.col("offensive_rating").mean())
)
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) – R sister package for NBA data
  * [nba_api](https://github.com/swar/nba_api) – Python alternative to the NBA Stats API

## sportsdataverse.nba.nba_pbp module

### sportsdataverse.nba.nba_pbp.espn_nba_pbp(game_id: int, raw=False, \*\*kwargs) → Dict

espn_nba_pbp() - Pull the game by id - Data from API endpoints - nba/playbyplay, nba/summary

* **Parameters:**
  * **game_id** (*int*) – Unique game_id, can be obtained from nba_schedule().
  * **raw** (*bool*) – If True, returns the raw json from the API endpoint. If False, returns a cleaned dictionary of datasets.
* **Returns:**
  Dictionary of game data with keys - “gameId”, “plays”, “winprobability”, “boxscore”, “header”, “broadcasts”,
  : ”videos”, “playByPlaySource”, “standings”, “leaders”, “seasonseries”, “timeouts”, “pickcenter”, “againstTheSpread”,
    “odds”, “predictor”, “espnWP”, “gameInfo”, “season”
* **Return type:**
  Dict

### Example

Quick start:

```default
from sportsdataverse.nba import espn_nba_pbp
pbp = espn_nba_pbp(game_id=401585183)
print(list(pbp.keys()))
```

Pull only the raw ESPN summary payload (skip cleaning):

```default
raw_pbp = espn_nba_pbp(game_id=401585183, raw=True)
```

Pipeline next step (load plays into a polars DataFrame):

```default
import polars as pl
pbp = espn_nba_pbp(game_id=401585183)
plays_df = pl.from_dicts(pbp["plays"])
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) – R sister package for NBA PBP
  * [wehoop](https://wehoop.sportsdataverse.org) – women’s basketball parallel
  * [nba_api](https://github.com/swar/nba_api) – Python alternative to the NBA Stats API

### sportsdataverse.nba.nba_pbp.helper_nba_game_data(pbp_txt, init)

Internal helper that lifts home/away team identification fields from the
ESPN summary payload onto the cleaned `pbp_txt` and `init` dictionaries.

* **Parameters:**
  * **pbp_txt** (*dict*) – ESPN summary payload.
  * **init** (*dict*) – Pickcenter-derived spread / favorite / over-under metadata.
* **Returns:**
  `(pbp_txt, init)` with team-id, mascot, location,
  abbreviation, and alt-name fields populated for both sides.
* **Return type:**
  tuple[dict, dict]

### Example

Used internally by [`espn_nba_pbp()`](#sportsdataverse.nba.nba_pbp.espn_nba_pbp):

```default
from sportsdataverse.nba import espn_nba_pbp
pbp = espn_nba_pbp(game_id=401585183)
```

### sportsdataverse.nba.nba_pbp.helper_nba_pbp(game_id, pbp_txt)

Internal helper that runs the ESPN summary payload through pickcenter,
game-data, and feature pipelines and returns the cleaned dictionary
consumed by [`espn_nba_pbp()`](#sportsdataverse.nba.nba_pbp.espn_nba_pbp).

* **Parameters:**
  * **game_id** (*int*) – ESPN game / event identifier.
  * **pbp_txt** (*dict*) – Trimmed ESPN summary payload (already filtered to
    the keys [`espn_nba_pbp()`](#sportsdataverse.nba.nba_pbp.espn_nba_pbp) keeps).
* **Returns:**
  Cleaned game payload with cleaned plays, boxscore, broadcasts,
  odds, etc.
* **Return type:**
  dict

### Example

Used internally by [`espn_nba_pbp()`](#sportsdataverse.nba.nba_pbp.espn_nba_pbp):

```default
from sportsdataverse.nba import espn_nba_pbp
pbp = espn_nba_pbp(game_id=401585183)
```

### sportsdataverse.nba.nba_pbp.helper_nba_pbp_features(game_id, pbp_txt, init)

Internal helper that builds the polars play-by-play frame and timeout
metadata from the ESPN summary payload.

Adds clock decomposition (minutes/seconds), per-quarter and per-half
seconds-remaining columns, half/quarter lag-lead helpers, and a per-game
timeout map keyed by team id and half.

* **Parameters:**
  * **game_id** (*int*) – ESPN game / event identifier.
  * **pbp_txt** (*dict*) – ESPN summary payload (with `plays` already lifted).
  * **init** (*dict*) – Output of [`helper_nba_pickcenter()`](#sportsdataverse.nba.nba_pbp.helper_nba_pickcenter) plus team-id
    metadata from [`helper_nba_game_data()`](#sportsdataverse.nba.nba_pbp.helper_nba_game_data).
* **Returns:**
  `pbp_txt` mutated with `plays` (a polars DataFrame) and
  `timeouts` populated.
* **Return type:**
  dict

### Example

Used internally by [`espn_nba_pbp()`](#sportsdataverse.nba.nba_pbp.espn_nba_pbp):

```default
from sportsdataverse.nba import espn_nba_pbp
pbp = espn_nba_pbp(game_id=401585183)
```

### sportsdataverse.nba.nba_pbp.helper_nba_pickcenter(pbp_txt)

Internal helper that extracts spread / over-under / home-favorite info
from the ESPN `pickcenter` array.

Falls back to sensible defaults (spread 2.5, OU 215.5, home favorite True,
spread unavailable) when no pickcenter data is present.

* **Parameters:**
  **pbp_txt** (*dict*) – ESPN summary payload.
* **Returns:**
  `{"gameSpread", "overUnder", "homeFavorite", "gameSpreadAvailable"}`.
* **Return type:**
  dict

### Example

Used internally by [`espn_nba_pbp()`](#sportsdataverse.nba.nba_pbp.espn_nba_pbp):

```default
from sportsdataverse.nba import espn_nba_pbp
pbp = espn_nba_pbp(game_id=401585183)
```

### sportsdataverse.nba.nba_pbp.nba_pbp_disk(game_id, path_to_json)

Load a previously cached ESPN NBA summary JSON for a game from disk.

Reads `{path_to_json}/{game_id}.json`.

* **Parameters:**
  * **game_id** (*int*) – ESPN game / event identifier.
  * **path_to_json** (*str*) – Directory containing the cached JSON file.
* **Returns:**
  Parsed JSON contents.
* **Return type:**
  dict

### Example

Quick start:

```default
from sportsdataverse.nba import nba_pbp_disk
pbp = nba_pbp_disk(game_id=401585183, path_to_json="./cache")
print(list(pbp.keys()))
```

## sportsdataverse.nba.nba_schedule module

### sportsdataverse.nba.nba_schedule.espn_nba_calendar(season=None, ondays=None, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_nba_calendar - look up the NBA calendar for a given season from ESPN

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

Quick start:

```default
from sportsdataverse.nba import espn_nba_calendar
cal = espn_nba_calendar(season=2023)
print(cal.shape)
```

Use ondays to get every scheduled date for the season:

```default
ondays = espn_nba_calendar(season=2023, ondays=True)
```

Pipeline next step (loop the URLs to scrape day-by-day):

```default
cal = espn_nba_calendar(season=2023, ondays=True)
urls = cal["url"].to_list()  # feed each into espn_nba_schedule
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) – R sister package for NBA data

### sportsdataverse.nba.nba_schedule.espn_nba_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_nba_schedule - look up the NBA schedule for a given date from ESPN

* **Parameters:**
  * **dates** (*int*) – Used to define different seasons. 2002 is the earliest available season.
  * **season_type** (*int*) – season type, 1 for pre-season, 2 for regular season, 3 for post-season,
  * **all-star** (*4 for*)
  * **off-season** (*5 for*)
  * **limit** (*int*) – number of records to return, default: 500.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing schedule dates for the requested season. Returns None if no games
* **Return type:**
  pl.DataFrame

### Example

Quick start (today’s slate):

```default
from sportsdataverse.nba import espn_nba_schedule
slate = espn_nba_schedule()
print(slate.shape)
```

Pull a specific date:

```default
jan2 = espn_nba_schedule(dates=20230102, season_type=2)
```

Pipeline next step (extract finals only):

```default
import polars as pl
finals = espn_nba_schedule(dates=20230102).filter(
    pl.col("status_type_completed") == True
)
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) – R sister package for NBA data
  * [nba_api](https://github.com/swar/nba_api) – Python alternative to the NBA Stats API

### sportsdataverse.nba.nba_schedule.most_recent_nba_season()

Return the most recent NBA season year based on today’s date.

The NBA season crosses calendar years – a season started in October of
year Y is reported as season Y+1. If today is in October or later, this
returns next calendar year; otherwise it returns the current calendar year.

* **Returns:**
  The most recent NBA season year (e.g. 2024 for the 2023-24 season).
* **Return type:**
  int

### Example

Quick start:

```default
from sportsdataverse.nba import most_recent_nba_season
year = most_recent_nba_season()
print(year)
```

Combine with the loaders for a “current season” pull:

```default
from sportsdataverse.nba import load_nba_schedule, most_recent_nba_season
sched = load_nba_schedule(seasons=[most_recent_nba_season()])
```

### sportsdataverse.nba.nba_schedule.scoreboard_event_parsing(event)

Internal helper that flattens an ESPN NBA scoreboard event dict into a
shape suitable for `pd.json_normalize`.

* **Parameters:**
  **event** (*dict*) – A single scoreboard `events[*]` entry from the ESPN
  NBA scoreboard API.
* **Returns:**
  The same event dict, mutated in place with `home`/`away`
  copies of the competitors and trimmed of unused link/odds keys.
* **Return type:**
  dict

### Example

Used internally by [`espn_nba_schedule()`](#sportsdataverse.nba.nba_schedule.espn_nba_schedule):

```default
from sportsdataverse.nba import espn_nba_schedule
sched = espn_nba_schedule(dates=20230102)
```

### sportsdataverse.nba.nba_schedule.year_to_season(year)

Convert a season-end year (e.g. 2024) to the NBA’s hyphenated label
(e.g. `"2023-24"`).

Handles century rollover (1999 -> `"1999-00"`) and zero-pads the
second half of the label.

* **Parameters:**
  **year** (*int*) – The starting calendar year of the season (e.g. 2023 for
  the 2023-24 season).
* **Returns:**
  NBA-style season label.
* **Return type:**
  str

### Example

Quick start:

```default
from sportsdataverse.nba import year_to_season
label = year_to_season(2023)
print(label)  # "2023-24"
```

Century rollover:

```default
print(year_to_season(1999))  # "1999-00"
```

## sportsdataverse.nba.nba_teams module

### sportsdataverse.nba.nba_teams.espn_nba_teams(return_as_pandas=False, \*\*kwargs) → DataFrame

espn_nba_teams - look up NBA teams

* **Parameters:**
  **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing teams for the requested league.
  This function caches by default, so if you want to refresh the data, use the command
  sportsdataverse.nba.espn_nba_teams.clear_cache().
* **Return type:**
  pl.DataFrame

### Example

Quick start:

```default
from sportsdataverse.nba import espn_nba_teams
teams = espn_nba_teams()
print(teams.shape)
```

Pandas round-trip:

```default
teams_pd = espn_nba_teams(return_as_pandas=True)
teams_pd.head()
```

Pipeline next step (build a team_id to abbreviation map):

```default
teams = espn_nba_teams()
abbr_map = dict(zip(teams["team_id"], teams["team_abbreviation"]))
```

See Also:
: * [hoopR](https://hoopR.sportsdataverse.org) – R sister package for NBA team data
  * [nba_api](https://github.com/swar/nba_api) – Python alternative to the NBA Stats API

## Module contents
