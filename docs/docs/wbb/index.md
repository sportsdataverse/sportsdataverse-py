<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [sportsdataverse.wbb package](#sportsdataversewbb-package)
  - [Submodules](#submodules)
  - [sportsdataverse.wbb.wbb_event_officials module](#sportsdataversewbbwbb_event_officials-module)
    - [sportsdataverse.wbb.wbb_event_officials.espn_wbb_event_officials(game_id: int, season: int | None = None, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]](#sportsdataversewbbwbb_event_officialsespn_wbb_event_officialsgame_id-int-season-int--none--none--raw-literaltrue-return_as_pandas-bool--false-%5C%5Ckwargs-any-%E2%86%92-dictstr-any)
    - [sportsdataverse.wbb.wbb_event_officials.espn_wbb_event_officials(game_id: int, season: int | None = None, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → DataFrame](#sportsdataversewbbwbb_event_officialsespn_wbb_event_officialsgame_id-int-season-int--none--none--raw-literalfalse--false-return_as_pandas-literaltrue-%5C%5Ckwargs-any-%E2%86%92-dataframe)
    - [sportsdataverse.wbb.wbb_event_officials.espn_wbb_event_officials(game_id: int, season: int | None = None, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → DataFrame](#sportsdataversewbbwbb_event_officialsespn_wbb_event_officialsgame_id-int-season-int--none--none--raw-literalfalse--false-return_as_pandas-literalfalse--false-%5C%5Ckwargs-any-%E2%86%92-dataframe)
    - [Example](#example)
  - [sportsdataverse.wbb.wbb_game_rosters module](#sportsdataversewbbwbb_game_rosters-module)
    - [sportsdataverse.wbb.wbb_game_rosters.espn_wbb_game_rosters(game_id: int, raw=False, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversewbbwbb_game_rostersespn_wbb_game_rostersgame_id-int-rawfalse-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-1)
    - [sportsdataverse.wbb.wbb_game_rosters.helper_wbb_athlete_items(teams_rosters, \*\*kwargs)](#sportsdataversewbbwbb_game_rostershelper_wbb_athlete_itemsteams_rosters-%5C%5Ckwargs)
    - [sportsdataverse.wbb.wbb_game_rosters.helper_wbb_game_items(summary)](#sportsdataversewbbwbb_game_rostershelper_wbb_game_itemssummary)
    - [sportsdataverse.wbb.wbb_game_rosters.helper_wbb_roster_items(items, summary_url, \*\*kwargs)](#sportsdataversewbbwbb_game_rostershelper_wbb_roster_itemsitems-summary_url-%5C%5Ckwargs)
    - [sportsdataverse.wbb.wbb_game_rosters.helper_wbb_team_items(items, \*\*kwargs)](#sportsdataversewbbwbb_game_rostershelper_wbb_team_itemsitems-%5C%5Ckwargs)
  - [sportsdataverse.wbb.wbb_loaders module](#sportsdataversewbbwbb_loaders-module)
    - [sportsdataverse.wbb.wbb_loaders.load_wbb_pbp(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversewbbwbb_loadersload_wbb_pbpseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-2)
    - [sportsdataverse.wbb.wbb_loaders.load_wbb_player_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversewbbwbb_loadersload_wbb_player_boxscoreseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-3)
    - [sportsdataverse.wbb.wbb_loaders.load_wbb_schedule(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversewbbwbb_loadersload_wbb_scheduleseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-4)
    - [sportsdataverse.wbb.wbb_loaders.load_wbb_team_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversewbbwbb_loadersload_wbb_team_boxscoreseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-5)
  - [sportsdataverse.wbb.wbb_pbp module](#sportsdataversewbbwbb_pbp-module)
    - [sportsdataverse.wbb.wbb_pbp.espn_wbb_pbp(game_id: int, raw=False, \*\*kwargs) → Dict](#sportsdataversewbbwbb_pbpespn_wbb_pbpgame_id-int-rawfalse-%5C%5Ckwargs-%E2%86%92-dict)
    - [Example](#example-6)
    - [sportsdataverse.wbb.wbb_pbp.helper_wbb_game_data(pbp_txt, init)](#sportsdataversewbbwbb_pbphelper_wbb_game_datapbp_txt-init)
    - [sportsdataverse.wbb.wbb_pbp.helper_wbb_pbp(game_id, pbp_txt)](#sportsdataversewbbwbb_pbphelper_wbb_pbpgame_id-pbp_txt)
    - [sportsdataverse.wbb.wbb_pbp.helper_wbb_pbp_features(game_id, pbp_txt, init)](#sportsdataversewbbwbb_pbphelper_wbb_pbp_featuresgame_id-pbp_txt-init)
    - [sportsdataverse.wbb.wbb_pbp.helper_wbb_pickcenter(pbp_txt)](#sportsdataversewbbwbb_pbphelper_wbb_pickcenterpbp_txt)
    - [sportsdataverse.wbb.wbb_pbp.wbb_pbp_disk(game_id, path_to_json)](#sportsdataversewbbwbb_pbpwbb_pbp_diskgame_id-path_to_json)
  - [sportsdataverse.wbb.wbb_player_stats module](#sportsdataversewbbwbb_player_stats-module)
    - [sportsdataverse.wbb.wbb_player_stats.espn_wbb_player_stats(athlete_id: int, season: int, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]](#sportsdataversewbbwbb_player_statsespn_wbb_player_statsathlete_id-int-season-int--raw-literaltrue-return_as_pandas-bool--false-%5C%5Ckwargs-any-%E2%86%92-dictstr-any)
    - [sportsdataverse.wbb.wbb_player_stats.espn_wbb_player_stats(athlete_id: int, season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → dict[str, DataFrame]](#sportsdataversewbbwbb_player_statsespn_wbb_player_statsathlete_id-int-season-int--raw-literalfalse--false-return_as_pandas-literaltrue-%5C%5Ckwargs-any-%E2%86%92-dictstr-dataframe)
    - [sportsdataverse.wbb.wbb_player_stats.espn_wbb_player_stats(athlete_id: int, season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → dict[str, DataFrame]](#sportsdataversewbbwbb_player_statsespn_wbb_player_statsathlete_id-int-season-int--raw-literalfalse--false-return_as_pandas-literalfalse--false-%5C%5Ckwargs-any-%E2%86%92-dictstr-dataframe)
    - [Example](#example-7)
  - [sportsdataverse.wbb.wbb_schedule module](#sportsdataversewbbwbb_schedule-module)
    - [sportsdataverse.wbb.wbb_schedule.espn_wbb_calendar(season=None, ondays=None, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversewbbwbb_scheduleespn_wbb_calendarseasonnone-ondaysnone-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-8)
    - [sportsdataverse.wbb.wbb_schedule.espn_wbb_schedule(dates=None, groups=50, season_type=None, limit=500, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversewbbwbb_scheduleespn_wbb_scheduledatesnone-groups50-season_typenone-limit500-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-9)
    - [sportsdataverse.wbb.wbb_schedule.most_recent_wbb_season()](#sportsdataversewbbwbb_schedulemost_recent_wbb_season)
    - [Example](#example-10)
    - [sportsdataverse.wbb.wbb_schedule.scoreboard_event_parsing(event)](#sportsdataversewbbwbb_schedulescoreboard_event_parsingevent)
  - [sportsdataverse.wbb.wbb_standings module](#sportsdataversewbbwbb_standings-module)
    - [sportsdataverse.wbb.wbb_standings.espn_wbb_standings(season: int, group: int = 50, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]](#sportsdataversewbbwbb_standingsespn_wbb_standingsseason-int-group-int--50--raw-literaltrue-return_as_pandas-bool--false-%5C%5Ckwargs-any-%E2%86%92-dictstr-any)
    - [sportsdataverse.wbb.wbb_standings.espn_wbb_standings(season: int, group: int = 50, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → DataFrame](#sportsdataversewbbwbb_standingsespn_wbb_standingsseason-int-group-int--50--raw-literalfalse--false-return_as_pandas-literaltrue-%5C%5Ckwargs-any-%E2%86%92-dataframe)
    - [sportsdataverse.wbb.wbb_standings.espn_wbb_standings(season: int, group: int = 50, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → DataFrame](#sportsdataversewbbwbb_standingsespn_wbb_standingsseason-int-group-int--50--raw-literalfalse--false-return_as_pandas-literalfalse--false-%5C%5Ckwargs-any-%E2%86%92-dataframe)
    - [Example](#example-11)
  - [sportsdataverse.wbb.wbb_team_roster module](#sportsdataversewbbwbb_team_roster-module)
    - [sportsdataverse.wbb.wbb_team_roster.espn_wbb_team_roster(team_id: int, season: int | None = None, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]](#sportsdataversewbbwbb_team_rosterespn_wbb_team_rosterteam_id-int-season-int--none--none--raw-literaltrue-return_as_pandas-bool--false-%5C%5Ckwargs-any-%E2%86%92-dictstr-any)
    - [sportsdataverse.wbb.wbb_team_roster.espn_wbb_team_roster(team_id: int, season: int | None = None, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → DataFrame](#sportsdataversewbbwbb_team_rosterespn_wbb_team_rosterteam_id-int-season-int--none--none--raw-literalfalse--false-return_as_pandas-literaltrue-%5C%5Ckwargs-any-%E2%86%92-dataframe)
    - [sportsdataverse.wbb.wbb_team_roster.espn_wbb_team_roster(team_id: int, season: int | None = None, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → DataFrame](#sportsdataversewbbwbb_team_rosterespn_wbb_team_rosterteam_id-int-season-int--none--none--raw-literalfalse--false-return_as_pandas-literalfalse--false-%5C%5Ckwargs-any-%E2%86%92-dataframe)
    - [Example](#example-12)
  - [sportsdataverse.wbb.wbb_team_stats module](#sportsdataversewbbwbb_team_stats-module)
    - [sportsdataverse.wbb.wbb_team_stats.espn_wbb_team_stats(team_id: int, season: int, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]](#sportsdataversewbbwbb_team_statsespn_wbb_team_statsteam_id-int-season-int--raw-literaltrue-return_as_pandas-bool--false-%5C%5Ckwargs-any-%E2%86%92-dictstr-any)
    - [sportsdataverse.wbb.wbb_team_stats.espn_wbb_team_stats(team_id: int, season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → dict[str, DataFrame]](#sportsdataversewbbwbb_team_statsespn_wbb_team_statsteam_id-int-season-int--raw-literalfalse--false-return_as_pandas-literaltrue-%5C%5Ckwargs-any-%E2%86%92-dictstr-dataframe)
    - [sportsdataverse.wbb.wbb_team_stats.espn_wbb_team_stats(team_id: int, season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → dict[str, DataFrame]](#sportsdataversewbbwbb_team_statsespn_wbb_team_statsteam_id-int-season-int--raw-literalfalse--false-return_as_pandas-literalfalse--false-%5C%5Ckwargs-any-%E2%86%92-dictstr-dataframe)
    - [Example](#example-13)
  - [sportsdataverse.wbb.wbb_teams module](#sportsdataversewbbwbb_teams-module)
    - [sportsdataverse.wbb.wbb_teams.espn_wbb_teams(groups=None, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversewbbwbb_teamsespn_wbb_teamsgroupsnone-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-14)
  - [Module contents](#module-contents)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# sportsdataverse.wbb package

## Submodules

## sportsdataverse.wbb.wbb_event_officials module

ESPN women’s-college-basketball game officials scraper.

Single ESPN endpoint:
: sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/events/{event_id}/competitions/{event_id}/officials

Returns one row per official assigned to a game (referee, umpires, etc.). The
`items[]` array carries each official’s identity (`id`, `fullName`,
`firstName`, `lastName`, `displayName`) and a nested `position`
sub-object with the assignment role. ESPN’s site-v2 `summary?event={id}`
endpoint surfaces the same officials list under `gameInfo.officials[]` but
without the official’s `id`, so this wrapper prefers the core-api path that
the wehoop R helper uses too.

The `wbb` and `wnba` public wrappers share a single internal helper
(`_espn_basketball_event_officials`) parameterized by league slug, mirroring
the `team_roster` / `player_stats` shim pattern.

### sportsdataverse.wbb.wbb_event_officials.espn_wbb_event_officials(game_id: int, season: int | None = None, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]

### sportsdataverse.wbb.wbb_event_officials.espn_wbb_event_officials(game_id: int, season: int | None = None, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → DataFrame

### sportsdataverse.wbb.wbb_event_officials.espn_wbb_event_officials(game_id: int, season: int | None = None, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → DataFrame

Pull the officials assigned to a women’s-college-basketball game.

* **Parameters:**
  * **game_id** – ESPN event identifier (e.g. `401637613` for the 2024
    NCAA Division I women’s championship game).
  * **season** – Season year. Recorded as the `season` column on the output;
    does NOT alter the request URL because ESPN’s officials endpoint
    keys on event ID alone.
  * **raw** – If True, returns the parsed JSON dict before any flattening.
  * **return_as_pandas** – If True, returns a pandas DataFrame; otherwise polars.
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download`.
* **Returns:**
  `game_id`, `season`, `official_id`, `first_name`,
  `last_name`, `full_name`, `display_name`, `position_id`,
  `position_name`, `position_display_name`, `order`.

  When ESPN ships no officials for the game (often for unscheduled or
  future events), an empty frame with the documented schema is
  returned so callers see a stable column set.

  If `raw=True`, returns the raw response dict.
* **Return type:**
  Polars (or pandas) DataFrame with one row per official
* **Raises:**
  * **sportsdataverse.errors.NoESPNDataError** – ESPN returned 404.
  * **requests.exceptions.RequestException** – Other network failures after retries.

### Example

Quick start (2024 NCAA W championship game):

```default
from sportsdataverse.wbb import espn_wbb_event_officials
officials = espn_wbb_event_officials(game_id=401587902, season=2024)
print(officials.shape)
officials.select(["full_name", "position_display_name", "order"]).head()
```

Pandas round-trip:

```default
officials_pd = espn_wbb_event_officials(
    game_id=401587902, season=2024, return_as_pandas=True
)
officials_pd.head()
```

Raw payload (skip the cleaning pipeline):

```default
raw = espn_wbb_event_officials(
    game_id=401587902, season=2024, raw=True
)
sorted(raw.keys())
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

## sportsdataverse.wbb.wbb_game_rosters module

### sportsdataverse.wbb.wbb_game_rosters.espn_wbb_game_rosters(game_id: int, raw=False, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_wbb_game_rosters() - Pull the game by id.

* **Parameters:**
  * **game_id** (*int*) – Unique game_id, can be obtained from wbb_schedule().
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

Quick start (2024 NCAA W championship game):

```default
from sportsdataverse.wbb import espn_wbb_game_rosters
roster = espn_wbb_game_rosters(game_id=401587902)
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
roster_pd = espn_wbb_game_rosters(game_id=401587902, return_as_pandas=True)
roster_pd.head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

### sportsdataverse.wbb.wbb_game_rosters.helper_wbb_athlete_items(teams_rosters, \*\*kwargs)

### sportsdataverse.wbb.wbb_game_rosters.helper_wbb_game_items(summary)

### sportsdataverse.wbb.wbb_game_rosters.helper_wbb_roster_items(items, summary_url, \*\*kwargs)

### sportsdataverse.wbb.wbb_game_rosters.helper_wbb_team_items(items, \*\*kwargs)

## sportsdataverse.wbb.wbb_loaders module

### sportsdataverse.wbb.wbb_loaders.load_wbb_pbp(seasons: List[int], return_as_pandas=False) → DataFrame

Load women’s college basketball play by play data going back to 2002

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
from sportsdataverse.wbb import load_wbb_pbp
pbp = load_wbb_pbp(seasons=[2024])
print(pbp.shape)
```

Range of seasons:

```default
pbp_multi = load_wbb_pbp(seasons=range(2022, 2025))
print(pbp_multi["season"].unique().sort())
```

Pandas round-trip:

```default
pbp_pd = load_wbb_pbp(seasons=[2024], return_as_pandas=True)
pbp_pd.head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

### sportsdataverse.wbb.wbb_loaders.load_wbb_player_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame

Load women’s college basketball player boxscore data

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
from sportsdataverse.wbb import load_wbb_player_boxscore
pb = load_wbb_player_boxscore(seasons=[2024])
print(pb.shape)
```

Range of seasons + top scorers:

```default
import polars as pl
pb_multi = load_wbb_player_boxscore(seasons=range(2022, 2025))
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
pb_pd = load_wbb_player_boxscore(seasons=[2024], return_as_pandas=True)
pb_pd.head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

### sportsdataverse.wbb.wbb_loaders.load_wbb_schedule(seasons: List[int], return_as_pandas=False) → DataFrame

Load women’s college basketball schedule data

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
from sportsdataverse.wbb import load_wbb_schedule
sched = load_wbb_schedule(seasons=[2024])
print(sched.shape)
```

Range of seasons:

```default
sched_multi = load_wbb_schedule(seasons=range(2022, 2025))
print(sched_multi["season"].unique().sort())
```

Pandas round-trip:

```default
sched_pd = load_wbb_schedule(seasons=[2024], return_as_pandas=True)
sched_pd.head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

### sportsdataverse.wbb.wbb_loaders.load_wbb_team_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame

Load women’s college basketball team boxscore data

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
from sportsdataverse.wbb import load_wbb_team_boxscore
tb = load_wbb_team_boxscore(seasons=[2024])
print(tb.shape)
```

Range of seasons + filter to a specific team:

```default
import polars as pl
tb_multi = load_wbb_team_boxscore(seasons=range(2022, 2025))
uconn = tb_multi.filter(pl.col("team_id") == 41)  # team_id 41 = UConn
```

Pandas round-trip:

```default
tb_pd = load_wbb_team_boxscore(seasons=[2024], return_as_pandas=True)
tb_pd.head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

## sportsdataverse.wbb.wbb_pbp module

### sportsdataverse.wbb.wbb_pbp.espn_wbb_pbp(game_id: int, raw=False, \*\*kwargs) → Dict

espn_wbb_pbp() - Pull the game by id. Data from API endpoints - womens-college-basketball/playbyplay,
womens-college-basketball/summary

* **Parameters:**
  * **game_id** (*int*) – Unique game_id, can be obtained from wbb_schedule().
  * **raw** (*bool*) – If True, returns the raw json from the API endpoint. If False, returns a cleaned dictionary of datasets.
* **Returns:**
  Dictionary of game data with keys - “gameId”, “plays”, “winprobability”, “boxscore”, “header”,
  “broadcasts”, “videos”, “playByPlaySource”, “standings”, “leaders”, “timeouts”, “pickcenter”,
  “againstTheSpread”, “odds”, “predictor”,”espnWP”, “gameInfo”, “season”
* **Return type:**
  Dict

### Example

Quick start (2024 NCAA Division I women’s championship game):

```default
from sportsdataverse.wbb import espn_wbb_pbp
game = espn_wbb_pbp(game_id=401587902)
print(game["gameId"])
print(len(game["plays"]))
```

Convert plays to a DataFrame and filter shooting plays:

```default
import polars as pl
plays = pl.DataFrame(game["plays"])
shots = plays.filter(pl.col("scoring_play") | pl.col("shooting_play"))
shots.select(["period_number", "clock_display_value", "team_id", "coordinate_x", "coordinate_y", "score_value", "text"]).head()
```

Convert to pandas for downstream analysis:

```default
import pandas as pd
shots_pd = pd.DataFrame(game["plays"])
shots_pd[shots_pd["shooting_play"] == True].head()
```

Raw payload (skip the cleaning pipeline) for debugging:

```default
raw = espn_wbb_pbp(game_id=401587902, raw=True)
sorted(raw.keys())
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) - R sister package; mirrors this surface for women’s basketball
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

### sportsdataverse.wbb.wbb_pbp.helper_wbb_game_data(pbp_txt, init)

### sportsdataverse.wbb.wbb_pbp.helper_wbb_pbp(game_id, pbp_txt)

### sportsdataverse.wbb.wbb_pbp.helper_wbb_pbp_features(game_id, pbp_txt, init)

### sportsdataverse.wbb.wbb_pbp.helper_wbb_pickcenter(pbp_txt)

### sportsdataverse.wbb.wbb_pbp.wbb_pbp_disk(game_id, path_to_json)

## sportsdataverse.wbb.wbb_player_stats module

ESPN women’s-college-basketball athlete season stats scraper.

Single ESPN endpoint:
: site.web.api.espn.com/apis/common/v3/sports/basketball/womens-college-basketball/athletes/{athlete_id}/stats?season={year}

Unlike the team-roster endpoint, this one returns *multi-table* data — ESPN
ships an array of stat categories (currently three: season averages, season
totals, miscellaneous totals) and the wrapper returns one polars DataFrame
per category, keyed by a canonical category name.

The canonical category keys (`"Averages"`, `"Totals"`, `"Misc"`) are
always present in the return dict, even when ESPN omits one (the missing
slot is filled with an empty frame carrying the documented schema). Any
category whose ESPN `displayName` / `name` does not map onto one of
those three is collected under an additional `"Other"` key — that key is
only added when there is at least one un-mapped category, so callers
shouldn’t unconditionally index into it.

The canonical-key set was chosen to match ESPN’s 2025-current shape
(`averages` / `totals` / `miscellaneous`), not the legacy
`General` / `Offensive` / `Defensive` / `Rebounding` / `Shooting`
naming the original ESPN schema used. If ESPN reverts or expands the
category set, the new names will surface under `"Other"` until the
mapping table here is updated.

### sportsdataverse.wbb.wbb_player_stats.espn_wbb_player_stats(athlete_id: int, season: int, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]

### sportsdataverse.wbb.wbb_player_stats.espn_wbb_player_stats(athlete_id: int, season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → dict[str, DataFrame]

### sportsdataverse.wbb.wbb_player_stats.espn_wbb_player_stats(athlete_id: int, season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → dict[str, DataFrame]

Pull ESPN season stats for a women’s-college-basketball athlete.

* **Parameters:**
  * **athlete_id** – ESPN athlete identifier (e.g. `4433985` for Kylie
    Feuerbach).
  * **season** – Season year, forwarded to ESPN as `?season=YYYY`.
  * **raw** – If True, returns the parsed JSON dict before any flattening.
  * **return_as_pandas** – If True, returns a dict of pandas DataFrames;
    otherwise polars.
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download`.
* **Returns:**
  Dict with one DataFrame per stat category. The canonical keys
  `"Averages"`, `"Totals"`, `"Misc"` are ALWAYS present;
  missing categories come back as empty frames carrying the
  documented schema. Any ESPN-shipped category whose name does not
  match one of the three canonical keys is collected under an
  additional `"Other"` key (only added if non-empty).

  Per-category column set (one row per stat):
  * `stat_name` (Utf8)
  * `display_value` (Utf8)
  * `value` (Float64)
  * `description` (Utf8)
  * `category` (Utf8, constant per frame)
  * `athlete_id` (Int64, constant)
  * `season` (Int32, constant)

  If `raw=True`, returns the raw response dict.
* **Raises:**
  * **sportsdataverse.errors.NoESPNDataError** – ESPN returned 404.
  * **requests.exceptions.RequestException** – Other network failures after
        retries.

### Example

Quick start - canonical `Averages` / `Totals` / `Misc` keys:

```default
from sportsdataverse.wbb import espn_wbb_player_stats
frames = espn_wbb_player_stats(athlete_id=4433985, season=2025)
print(sorted(frames.keys()))
```

Index into a specific table:

```default
averages = frames["Averages"]
print(averages.shape)
averages.select(["stat_name", "display_value", "value"]).head()
```

Iterate over canonical categories:

```default
for cat in ("Averages", "Totals", "Misc"):
    print(cat, frames[cat].shape)
```

`Other` fallback bucket (only present when ESPN ships a category
that does not map onto one of the three canonical keys):

```default
if "Other" in frames:
    frames["Other"].select(["category", "stat_name", "value"])
```

Pandas round-trip:

```default
frames_pd = espn_wbb_player_stats(
    athlete_id=4433985, season=2025, return_as_pandas=True
)
frames_pd["Averages"].head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

## sportsdataverse.wbb.wbb_schedule module

### sportsdataverse.wbb.wbb_schedule.espn_wbb_calendar(season=None, ondays=None, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_wbb_calendar - look up the women’s college basketball calendar for a given season

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
from sportsdataverse.wbb import espn_wbb_calendar
cal = espn_wbb_calendar(season=2024)
cal.head()
```

On-days only (dates with games on the schedule):

```default
ondays = espn_wbb_calendar(season=2024, ondays=True)
ondays.head()
```

Pandas round-trip:

```default
cal_pd = espn_wbb_calendar(season=2024, return_as_pandas=True)
cal_pd.head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

### sportsdataverse.wbb.wbb_schedule.espn_wbb_schedule(dates=None, groups=50, season_type=None, limit=500, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_wbb_schedule - look up the women’s college basketball schedule for a given season

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

Single date (April 7, 2024 - 2024 NCAA W championship day):

```default
from sportsdataverse.wbb import espn_wbb_schedule
day = espn_wbb_schedule(dates=20240407)
print(day.shape)
```

Season-level pull (2024 season):

```default
season = espn_wbb_schedule(dates=2024, limit=1500)
print(season.shape)
```

Filter to a specific team (UConn `team_id=2509`):

```default
import polars as pl
uconn = season.filter(
    (pl.col("home_id") == "2509") | (pl.col("away_id") == "2509")
)
```

Pandas round-trip:

```default
season_pd = espn_wbb_schedule(dates=2024, return_as_pandas=True)
season_pd.head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

### sportsdataverse.wbb.wbb_schedule.most_recent_wbb_season()

Return the most recent women’s college basketball season year.

The women’s college basketball season spans late October through early
April; for any month October-December the “current season” is the
following calendar year (e.g. October 2025 returns `2026`).

* **Returns:**
  The most recent / current season year.
* **Return type:**
  int

### Example

Use as a default season argument:

```default
from sportsdataverse.wbb import most_recent_wbb_season, espn_wbb_schedule
season = most_recent_wbb_season()
sched = espn_wbb_schedule(dates=season)
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football

### sportsdataverse.wbb.wbb_schedule.scoreboard_event_parsing(event)

## sportsdataverse.wbb.wbb_standings module

ESPN women’s-college-basketball standings scraper.

Single ESPN endpoint:
: site.api.espn.com/apis/v2/sports/basketball/womens-college-basketball/standings?season={year}&group={group}

ESPN ships standings as a tree: the top-level payload has `children[]`
(one entry per conference under the requested group; `group=50` is NCAA
Division I women), each carrying a `standings.entries[]` array. Each
entry pairs a `team` block with a `stats[]` array of stat objects
(`avgPointsAgainst`, `wins`, `losses`, `streak`, etc.). The
wrapper flattens that tree to a single polars DataFrame, one row per
team, with the stat values surfaced as named columns.

### sportsdataverse.wbb.wbb_standings.espn_wbb_standings(season: int, group: int = 50, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]

### sportsdataverse.wbb.wbb_standings.espn_wbb_standings(season: int, group: int = 50, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → DataFrame

### sportsdataverse.wbb.wbb_standings.espn_wbb_standings(season: int, group: int = 50, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → DataFrame

Pull ESPN women’s-college-basketball standings for a season.

* **Parameters:**
  * **season** – Season year, forwarded to ESPN as `?season=YYYY`.
  * **group** – ESPN `group` filter. `50` is NCAA Division I women’s
    basketball (the default); `51` is Division II/III.
  * **raw** – If True, returns the parsed JSON dict before any flattening.
  * **return_as_pandas** – If True, returns a pandas DataFrame; otherwise
    polars.
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download`.
* **Returns:**
  Polars (or pandas) DataFrame with one row per team. Documented
  columns include `team_id`, `team_uid`, `team_slug`,
  `team_location`, `team_name`, `team_abbreviation`,
  `team_display_name`, `team_short_display_name`,
  `team_color`, `conference_id`, `wins`, `losses`,
  `win_percent`, `games_back`, `streak`, `points_for`,
  `points_against`, `point_differential`, `home_wins`,
  `home_losses`, `road_wins`, `road_losses`,
  `division_wins`, `division_losses`, `season`.

  If `raw=True`, returns the raw response dict.
* **Raises:**
  * **sportsdataverse.errors.NoESPNDataError** – ESPN returned 404.
  * **requests.exceptions.RequestException** – Other network failures after
        retries.

### Example

Quick start (Division I women’s standings, 2024 season):

```default
from sportsdataverse.wbb import espn_wbb_standings
standings = espn_wbb_standings(season=2024, group=50)
print(standings.shape)
standings.select(
    ["team_display_name", "wins", "losses", "win_percent"]
).head(10)
```

Top teams by win percentage:

```default
import polars as pl
top10 = standings.sort("win_percent", descending=True).head(10)
```

Pandas round-trip + Division II/III:

```default
d2_d3 = espn_wbb_standings(
    season=2024, group=51, return_as_pandas=True
)
d2_d3.head()
```

Raw payload (skip the cleaning pipeline):

```default
raw = espn_wbb_standings(season=2024, group=50, raw=True)
sorted(raw.keys())
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

## sportsdataverse.wbb.wbb_team_roster module

ESPN women’s-college-basketball team-level season roster scraper.

Single ESPN endpoint:
: site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/{team_id}/roster

Returns one row per athlete on the team’s CURRENT roster. ESPN’s roster
endpoint ignores `?season=YYYY`; the `season` argument is recorded on the
output frame as a column for downstream join purposes but does NOT alter the
request URL.

### sportsdataverse.wbb.wbb_team_roster.espn_wbb_team_roster(team_id: int, season: int | None = None, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]

### sportsdataverse.wbb.wbb_team_roster.espn_wbb_team_roster(team_id: int, season: int | None = None, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → DataFrame

### sportsdataverse.wbb.wbb_team_roster.espn_wbb_team_roster(team_id: int, season: int | None = None, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → DataFrame

Pull the current ESPN team roster for a women’s-college-basketball team.

* **Parameters:**
  * **team_id** – ESPN team identifier (e.g. `2509` for UConn).
  * **season** – Season year. Recorded as the `season` column on the output;
    does NOT alter the request URL because ESPN’s
    `/teams/{id}/roster` endpoint ignores `?season=YYYY`.
  * **raw** – If True, returns the parsed JSON dict before any flattening.
  * **return_as_pandas** – If True, returns a pandas DataFrame; otherwise polars.
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download`.
* **Returns:**
  `athlete_id`, `athlete_uid`, `first_name`, `last_name`,
  `full_name`, `display_name`, `short_name`, `jersey`,
  `position_id`, `position_name`, `position_abbreviation`,
  `height`, `display_height`, `weight`, `display_weight`,
  `age`, `date_of_birth`, `birth_city`, `birth_state`,
  `headshot_href`, `link_web`, `status_name`, `team_id`,
  `season`.

  If `raw=True`, returns the raw response dict.
* **Return type:**
  Polars (or pandas) DataFrame with one row per athlete
* **Raises:**
  * **sportsdataverse.errors.NoESPNDataError** – ESPN returned 404.
  * **requests.exceptions.RequestException** – Other network failures after retries.

### Example

Quick start (UConn `team_id=2509`):

```default
from sportsdataverse.wbb import espn_wbb_team_roster
roster = espn_wbb_team_roster(team_id=2509, season=2025)
print(roster.shape)
roster.select(["full_name", "jersey", "position_abbreviation"]).head()
```

Pandas round-trip:

```default
roster_pd = espn_wbb_team_roster(team_id=2509, season=2025, return_as_pandas=True)
roster_pd.head()
```

Pipeline next step - join with team metadata:

```default
from sportsdataverse.wbb import espn_wbb_teams
teams = espn_wbb_teams()
roster.join(
    teams.select(["team_id", "team_display_name"]),
    on="team_id",
    how="left",
)
```

Raw payload (skip the cleaning pipeline):

```default
raw = espn_wbb_team_roster(team_id=2509, season=2025, raw=True)
sorted(raw.keys())
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

## sportsdataverse.wbb.wbb_team_stats module

ESPN women’s-college-basketball team season-stats scraper.

Single ESPN endpoint:
: site.web.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/{team_id}/statistics?season={year}

ESPN ships team season stats as a multi-table payload — the `categories`
array under `results.stats.categories` carries one bucket per stat family
(currently `General`, `Offensive`, `Defensive`). The wrapper returns
one polars DataFrame per category, keyed by a canonical category name.

The canonical category keys (`"Averages"`, `"Totals"`, `"Misc"`) are
always present in the return dict, even when ESPN omits one (the missing
slot is filled with an empty frame carrying the documented schema). Any
category whose ESPN `displayName` / `name` does not map onto one of
those three is collected under an additional `"Other"` key — that key is
only added when there is at least one un-mapped category, so callers
shouldn’t unconditionally index into it.

ESPN’s current team-stats response uses `General` / `Offensive` /
`Defensive` rather than the player-stats triad of `Averages` /
`Totals` / `Misc`, so the default lookup table maps both shapes onto
the canonical keys. `General` rolls up to `Averages` (the
games-played-style aggregates ESPN ships there are per-game numbers),
`Offensive` rolls up to `Totals`, and `Defensive` rolls up to
`Misc`. If ESPN reverts or expands the category set, the new names will
surface under `"Other"` until the mapping table here is updated.

### sportsdataverse.wbb.wbb_team_stats.espn_wbb_team_stats(team_id: int, season: int, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]

### sportsdataverse.wbb.wbb_team_stats.espn_wbb_team_stats(team_id: int, season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → dict[str, DataFrame]

### sportsdataverse.wbb.wbb_team_stats.espn_wbb_team_stats(team_id: int, season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → dict[str, DataFrame]

Pull ESPN team season stats for a women’s-college-basketball team.

* **Parameters:**
  * **team_id** – ESPN team identifier (e.g. `2509` for UConn).
  * **season** – Season year, forwarded to ESPN as `?season=YYYY`.
  * **raw** – If True, returns the parsed JSON dict before any flattening.
  * **return_as_pandas** – If True, returns a dict of pandas DataFrames;
    otherwise polars.
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download`.
* **Returns:**
  Dict with one DataFrame per stat category. The canonical keys
  `"Averages"`, `"Totals"`, `"Misc"` are ALWAYS present;
  missing categories come back as empty frames carrying the
  documented schema. Any ESPN-shipped category whose name does not
  match one of the three canonical keys is collected under an
  additional `"Other"` key (only added if non-empty).

  Per-category column set (one row per stat):
  * `stat_name` (Utf8)
  * `abbreviation` (Utf8)
  * `display_value` (Utf8)
  * `value` (Float64)
  * `description` (Utf8)
  * `category` (Utf8, constant per frame)
  * `team_id` (Int64, constant)
  * `season` (Int32, constant)

  If `raw=True`, returns the raw response dict.
* **Raises:**
  * **sportsdataverse.errors.NoESPNDataError** – ESPN returned 404.
  * **requests.exceptions.RequestException** – Other network failures after
        retries.

### Example

Quick start (UConn `team_id=2509`):

```default
from sportsdataverse.wbb import espn_wbb_team_stats
frames = espn_wbb_team_stats(team_id=2509, season=2025)
print(sorted(frames.keys()))
```

Index into a specific table:

```default
averages = frames["Averages"]
print(averages.shape)
averages.select(["stat_name", "display_value", "value"]).head()
```

Iterate the canonical categories:

```default
for cat in ("Averages", "Totals", "Misc"):
    print(cat, frames[cat].shape)
```

`Other` fallback bucket (only present when ESPN ships a category
that does not map onto one of the three canonical keys):

```default
if "Other" in frames:
    frames["Other"].select(["category", "stat_name", "value"])
```

Pandas round-trip:

```default
frames_pd = espn_wbb_team_stats(
    team_id=2579, season=2025, return_as_pandas=True
)  # team_id 2579 = South Carolina
frames_pd["Averages"].head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

## sportsdataverse.wbb.wbb_teams module

### sportsdataverse.wbb.wbb_teams.espn_wbb_teams(groups=None, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_wbb_teams - look up the women’s college basketball teams

* **Parameters:**
  * **groups** (*int*) – Used to define different divisions. 50 is Division I, 51 is Division II/Division III.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing teams for the requested league.
  This function caches by default, so if you want to refresh the data, use the command
  sportsdataverse.wbb.espn_wbb_teams.clear_cache().
* **Return type:**
  pl.DataFrame

### Example

Default groups (D1 = `50`):

```default
from sportsdataverse.wbb import espn_wbb_teams
teams = espn_wbb_teams()
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
d2_d3 = espn_wbb_teams(groups=51, return_as_pandas=True)
d2_d3.head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) - R sister package
  * [cfbfastR](https://cfbfastR.sportsdataverse.org) - companion R package for college football
  * [ESPN](https://www.espn.com) - data origin

## Module contents
