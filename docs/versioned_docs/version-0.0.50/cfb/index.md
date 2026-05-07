<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [sportsdataverse.cfb package](#sportsdataversecfb-package)
  - [Submodules](#submodules)
  - [sportsdataverse.cfb.cfb_game_rosters module](#sportsdataversecfbcfb_game_rosters-module)
    - [sportsdataverse.cfb.cfb_game_rosters.espn_cfb_game_rosters(game_id: int, raw=False, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversecfbcfb_game_rostersespn_cfb_game_rostersgame_id-int-rawfalse-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example)
    - [sportsdataverse.cfb.cfb_game_rosters.helper_cfb_athlete_items(teams_rosters, \*\*kwargs)](#sportsdataversecfbcfb_game_rostershelper_cfb_athlete_itemsteams_rosters-%5C%5Ckwargs)
    - [Example](#example-1)
    - [sportsdataverse.cfb.cfb_game_rosters.helper_cfb_game_items(summary)](#sportsdataversecfbcfb_game_rostershelper_cfb_game_itemssummary)
    - [Example](#example-2)
    - [sportsdataverse.cfb.cfb_game_rosters.helper_cfb_roster_items(items, summary_url, \*\*kwargs)](#sportsdataversecfbcfb_game_rostershelper_cfb_roster_itemsitems-summary_url-%5C%5Ckwargs)
    - [Example](#example-3)
    - [sportsdataverse.cfb.cfb_game_rosters.helper_cfb_team_items(items, \*\*kwargs)](#sportsdataversecfbcfb_game_rostershelper_cfb_team_itemsitems-%5C%5Ckwargs)
    - [Example](#example-4)
  - [sportsdataverse.cfb.cfb_loaders module](#sportsdataversecfbcfb_loaders-module)
    - [sportsdataverse.cfb.cfb_loaders.get_cfb_teams(return_as_pandas=False) → DataFrame](#sportsdataversecfbcfb_loadersget_cfb_teamsreturn_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-5)
    - [sportsdataverse.cfb.cfb_loaders.load_cfb_betting_lines(return_as_pandas=False) → DataFrame](#sportsdataversecfbcfb_loadersload_cfb_betting_linesreturn_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-6)
    - [sportsdataverse.cfb.cfb_loaders.load_cfb_pbp(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversecfbcfb_loadersload_cfb_pbpseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-7)
    - [sportsdataverse.cfb.cfb_loaders.load_cfb_rosters(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversecfbcfb_loadersload_cfb_rostersseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-8)
    - [sportsdataverse.cfb.cfb_loaders.load_cfb_schedule(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversecfbcfb_loadersload_cfb_scheduleseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-9)
    - [sportsdataverse.cfb.cfb_loaders.load_cfb_team_info(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversecfbcfb_loadersload_cfb_team_infoseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-10)
  - [sportsdataverse.cfb.cfb_pbp module](#sportsdataversecfbcfb_pbp-module)
    - [*class* sportsdataverse.cfb.cfb_pbp.CFBPlayProcess(gameId=0, raw=False, path_to_json='/', return_keys=None, \*\*kwargs)](#class-sportsdataversecfbcfb_pbpcfbplayprocessgameid0-rawfalse-path_to_json-return_keysnone-%5C%5Ckwargs)
      - [\_\_helper_\_espn_cfb_odds_information_\_()](#%5C_%5C_helper_%5C_espn_cfb_odds_information_%5C_)
      - [\_\_init_\_(gameId=0, raw=False, path_to_json='/', return_keys=None, \*\*kwargs)](#%5C_%5C_init_%5C_gameid0-rawfalse-path_to_json-return_keysnone-%5C%5Ckwargs)
      - [cfb_pbp_disk()](#cfb_pbp_disk)
    - [Example](#example-11)
      - [cfb_pbp_json(\*\*kwargs)](#cfb_pbp_json%5C%5Ckwargs)
    - [Example](#example-12)
      - [corrupt_pbp_check()](#corrupt_pbp_check)
    - [Example](#example-13)
      - [create_box_score(play_df)](#create_box_scoreplay_df)
    - [Example](#example-14)
      - [espn_cfb_pbp(\*\*kwargs)](#espn_cfb_pbp%5C%5Ckwargs)
    - [Example](#example-15)
      - [gameId *= 0*](#gameid--0)
      - [path_to_json *= '/'*](#path_to_json--)
      - [ran_cleaning_pipeline *= False*](#ran_cleaning_pipeline--false)
      - [ran_pipeline *= False*](#ran_pipeline--false)
      - [raw *= False*](#raw--false)
      - [return_keys *= None*](#return_keys--none)
      - [run_cleaning_pipeline()](#run_cleaning_pipeline)
    - [Example](#example-16)
      - [run_processing_pipeline()](#run_processing_pipeline)
    - [Example](#example-17)
  - [sportsdataverse.cfb.cfb_play_participants module](#sportsdataversecfbcfb_play_participants-module)
    - [sportsdataverse.cfb.cfb_play_participants.espn_cfb_play_participants(game_id: int, *, raw: Literal[True], return_as_pandas: bool = False, resolve_missing: bool = True, resolve_missing_max: int = 50, \*\*kwargs: Any) → dict[str, Any]](#sportsdataversecfbcfb_play_participantsespn_cfb_play_participantsgame_id-int--raw-literaltrue-return_as_pandas-bool--false-resolve_missing-bool--true-resolve_missing_max-int--50-%5C%5Ckwargs-any-%E2%86%92-dictstr-any)
    - [sportsdataverse.cfb.cfb_play_participants.espn_cfb_play_participants(game_id: int, *, raw: Literal[False] = False, return_as_pandas: Literal[True], resolve_missing: bool = True, resolve_missing_max: int = 50, \*\*kwargs: Any) → DataFrame](#sportsdataversecfbcfb_play_participantsespn_cfb_play_participantsgame_id-int--raw-literalfalse--false-return_as_pandas-literaltrue-resolve_missing-bool--true-resolve_missing_max-int--50-%5C%5Ckwargs-any-%E2%86%92-dataframe)
    - [sportsdataverse.cfb.cfb_play_participants.espn_cfb_play_participants(game_id: int, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, resolve_missing: bool = True, resolve_missing_max: int = 50, \*\*kwargs: Any) → DataFrame](#sportsdataversecfbcfb_play_participantsespn_cfb_play_participantsgame_id-int--raw-literalfalse--false-return_as_pandas-literalfalse--false-resolve_missing-bool--true-resolve_missing_max-int--50-%5C%5Ckwargs-any-%E2%86%92-dataframe)
    - [Example](#example-18)
  - [sportsdataverse.cfb.cfb_schedule module](#sportsdataversecfbcfb_schedule-module)
    - [sportsdataverse.cfb.cfb_schedule.espn_cfb_calendar(season=None, groups=None, ondays=None, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversecfbcfb_scheduleespn_cfb_calendarseasonnone-groupsnone-ondaysnone-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-19)
    - [sportsdataverse.cfb.cfb_schedule.espn_cfb_schedule(dates=None, week=None, season_type=None, groups=None, limit=500, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversecfbcfb_scheduleespn_cfb_scheduledatesnone-weeknone-season_typenone-groupsnone-limit500-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-20)
    - [sportsdataverse.cfb.cfb_schedule.most_recent_cfb_season()](#sportsdataversecfbcfb_schedulemost_recent_cfb_season)
    - [Example](#example-21)
    - [sportsdataverse.cfb.cfb_schedule.scoreboard_event_parsing(event)](#sportsdataversecfbcfb_schedulescoreboard_event_parsingevent)
    - [Example](#example-22)
  - [sportsdataverse.cfb.cfb_teams module](#sportsdataversecfbcfb_teams-module)
    - [sportsdataverse.cfb.cfb_teams.espn_cfb_teams(groups=None, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversecfbcfb_teamsespn_cfb_teamsgroupsnone-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-23)
  - [sportsdataverse.cfb.model_vars module](#sportsdataversecfbmodel_vars-module)
  - [Module contents](#module-contents)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# sportsdataverse.cfb package

## Submodules

## sportsdataverse.cfb.cfb_game_rosters module

### sportsdataverse.cfb.cfb_game_rosters.espn_cfb_game_rosters(game_id: int, raw=False, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_cfb_game_rosters() - Pull the game by id.

* **Parameters:**
  * **game_id** (*int*) – Unique game_id, can be obtained from espn_cfb_schedule().
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

Quick start:

```default
from sportsdataverse.cfb import espn_cfb_game_rosters
rosters = espn_cfb_game_rosters(game_id=401628334)
print(rosters.shape)
```

Pandas round-trip:

```default
rosters_pd = espn_cfb_game_rosters(game_id=401628334, return_as_pandas=True)
rosters_pd.head()
```

Pipeline next step (filter to game starters):

```default
import polars as pl
starters = espn_cfb_game_rosters(game_id=401628334).filter(
    pl.col("starter") == True
)
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB rosters
  * [recruitR](https://github.com/sportsdataverse/recruitR) – recruiting data companion

### sportsdataverse.cfb.cfb_game_rosters.helper_cfb_athlete_items(teams_rosters, \*\*kwargs)

Internal helper that resolves each athlete `$ref` in a team-rosters frame
to the canonical athlete detail row.

* **Parameters:**
  * **teams_rosters** (*pl.DataFrame*) – Output of [`helper_cfb_roster_items()`](#sportsdataverse.cfb.cfb_game_rosters.helper_cfb_roster_items)
    (must contain an `athlete_href` column).
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download()`.
* **Returns:**
  One row per resolved athlete.
* **Return type:**
  pl.DataFrame

### Example

Used internally by [`espn_cfb_game_rosters()`](#sportsdataverse.cfb.cfb_game_rosters.espn_cfb_game_rosters):

```default
from sportsdataverse.cfb import espn_cfb_game_rosters
rosters = espn_cfb_game_rosters(game_id=401628334)
```

### sportsdataverse.cfb.cfb_game_rosters.helper_cfb_game_items(summary)

Internal helper that flattens the ESPN `competitions/competitors` summary
payload into a polars DataFrame keyed by `team_id`.

* **Parameters:**
  **summary** (*dict*) – Parsed JSON from the ESPN competitors summary endpoint.
* **Returns:**
  Polars dataframe with one row per competitor team in the game.
* **Return type:**
  pl.DataFrame

### Example

Used internally by [`espn_cfb_game_rosters()`](#sportsdataverse.cfb.cfb_game_rosters.espn_cfb_game_rosters):

```default
from sportsdataverse.cfb import espn_cfb_game_rosters
rosters = espn_cfb_game_rosters(game_id=401628334)
```

### sportsdataverse.cfb.cfb_game_rosters.helper_cfb_roster_items(items, summary_url, \*\*kwargs)

Internal helper that fetches the roster entries for every team in a game.

* **Parameters:**
  * **items** (*pl.DataFrame*) – Output of [`helper_cfb_game_items()`](#sportsdataverse.cfb.cfb_game_rosters.helper_cfb_game_items).
  * **summary_url** (*str*) – Base ESPN summary URL used to derive each team’s
    roster endpoint.
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download()`.
* **Returns:**
  One row per game-roster entry across both teams.
* **Return type:**
  pl.DataFrame

### Example

Used internally by [`espn_cfb_game_rosters()`](#sportsdataverse.cfb.cfb_game_rosters.espn_cfb_game_rosters):

```default
from sportsdataverse.cfb import espn_cfb_game_rosters
rosters = espn_cfb_game_rosters(game_id=401628334)
```

### sportsdataverse.cfb.cfb_game_rosters.helper_cfb_team_items(items, \*\*kwargs)

Internal helper that fetches team detail rows for every team referenced in
the competitors summary and returns them as a flat polars DataFrame.

* **Parameters:**
  * **items** (*pl.DataFrame*) – Output of [`helper_cfb_game_items()`](#sportsdataverse.cfb.cfb_game_rosters.helper_cfb_game_items).
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download()`.
* **Returns:**
  Team detail rows with logo URLs flattened out.
* **Return type:**
  pl.DataFrame

### Example

Used internally by [`espn_cfb_game_rosters()`](#sportsdataverse.cfb.cfb_game_rosters.espn_cfb_game_rosters):

```default
from sportsdataverse.cfb import espn_cfb_game_rosters
rosters = espn_cfb_game_rosters(game_id=401628334)
```

## sportsdataverse.cfb.cfb_loaders module

### sportsdataverse.cfb.cfb_loaders.get_cfb_teams(return_as_pandas=False) → DataFrame

Load college football team ID information and logos

* **Parameters:**
  **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing teams available.
* **Return type:**
  pl.DataFrame

### Example

Quick start:

```default
from sportsdataverse.cfb import get_cfb_teams
teams = get_cfb_teams()
print(teams.shape)
```

Pandas round-trip:

```default
teams_pd = get_cfb_teams(return_as_pandas=True)
teams_pd.head()
```

Pipeline next step (build a team_id to logo URL map):

```default
teams = get_cfb_teams()
logo_map = dict(zip(teams["team_id"], teams["logo"]))
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB team metadata

### sportsdataverse.cfb.cfb_loaders.load_cfb_betting_lines(return_as_pandas=False) → DataFrame

Load college football betting lines information

* **Parameters:**
  **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing betting lines available for the available seasons.
* **Return type:**
  pl.DataFrame

### Example

Quick start:

```default
from sportsdataverse.cfb import load_cfb_betting_lines
lines = load_cfb_betting_lines()
print(lines.shape)
```

Pandas round-trip:

```default
lines_pd = load_cfb_betting_lines(return_as_pandas=True)
lines_pd.head()
```

Pipeline next step (filter to one provider in 2023):

```default
import polars as pl
consensus_2023 = load_cfb_betting_lines().filter(
    (pl.col("season") == 2023) & (pl.col("provider") == "consensus")
)
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB betting lines
  * [nflverse](https://nflverse.nflverse.com) – companion data ecosystem for the NFL

### sportsdataverse.cfb.cfb_loaders.load_cfb_pbp(seasons: List[int], return_as_pandas=False) → DataFrame

Load college football play by play data going back to 2003

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2003 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the play-by-plays available for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2003.

### Example

Quick start:

```default
from sportsdataverse.cfb import load_cfb_pbp
pbp = load_cfb_pbp(seasons=[2023])
print(pbp.shape)
```

Multi-season pull as pandas:

```default
pbp_pd = load_cfb_pbp(seasons=range(2020, 2024), return_as_pandas=True)
pbp_pd.head()
```

Pipeline next step (filter to rushing plays):

```default
import polars as pl
rushes = load_cfb_pbp(seasons=[2023]).filter(pl.col("rush") == True)
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB PBP
  * [nflverse](https://nflverse.nflverse.com) – companion data ecosystem for the NFL

### sportsdataverse.cfb.cfb_loaders.load_cfb_rosters(seasons: List[int], return_as_pandas=False) → DataFrame

Load roster data

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2014 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing rosters available for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2014.

### Example

Quick start:

```default
from sportsdataverse.cfb import load_cfb_rosters
rosters = load_cfb_rosters(seasons=[2023])
print(rosters.shape)
```

Pandas round-trip:

```default
rosters_pd = load_cfb_rosters(seasons=[2023], return_as_pandas=True)
rosters_pd.head()
```

Pipeline next step (count quarterbacks per team):

```default
import polars as pl
qbs = (
    load_cfb_rosters(seasons=[2023])
    .filter(pl.col("position").eq("QB"))
    .group_by("team")
    .len()
)
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB rosters
  * [recruitR](https://github.com/sportsdataverse/recruitR) – recruiting data companion

### sportsdataverse.cfb.cfb_loaders.load_cfb_schedule(seasons: List[int], return_as_pandas=False) → DataFrame

Load college football schedule data

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

Quick start:

```default
from sportsdataverse.cfb import load_cfb_schedule
sched = load_cfb_schedule(seasons=[2023])
print(sched.shape)
```

Multi-season pull as pandas:

```default
sched_pd = load_cfb_schedule(seasons=range(2020, 2024), return_as_pandas=True)
sched_pd.head()
```

Pipeline next step (extract bowl games):

```default
import polars as pl
bowls = load_cfb_schedule(seasons=[2023]).filter(pl.col("season_type") == 3)
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB schedules
  * [nflverse](https://nflverse.nflverse.com) – companion data ecosystem for the NFL

### sportsdataverse.cfb.cfb_loaders.load_cfb_team_info(seasons: List[int], return_as_pandas=False) → DataFrame

Load college football team info

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2002 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the team info available for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2002.

### Example

Quick start:

```default
from sportsdataverse.cfb import load_cfb_team_info
teams = load_cfb_team_info(seasons=[2023])
print(teams.shape)
```

Pandas round-trip:

```default
teams_pd = load_cfb_team_info(seasons=[2023], return_as_pandas=True)
teams_pd.head()
```

Pipeline next step (join team info onto schedule):

```default
from sportsdataverse.cfb import load_cfb_schedule
sched = load_cfb_schedule(seasons=[2023])
teams = load_cfb_team_info(seasons=[2023])
enriched = sched.join(teams, left_on="home_id", right_on="team_id", how="left")
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB team data
  * [nflverse](https://nflverse.nflverse.com) – companion data ecosystem for the NFL

## sportsdataverse.cfb.cfb_pbp module

### *class* sportsdataverse.cfb.cfb_pbp.CFBPlayProcess(gameId=0, raw=False, path_to_json='/', return_keys=None, \*\*kwargs)

Bases: `object`

#### \_\_helper_\_espn_cfb_odds_information_\_()

Fetch pre-game spread/total from ESPN’s modern core odds endpoint.

Returns `(gameSpread, overUnder, homeFavorite, gameSpreadAvailable)`.
ESPN emptied the legacy `pickcenter` array on the summary endpoint
for 2024+ college games; this helper restores the data path for those
games via the `sports.core.api.espn.com` v2 odds collection. Falls
back to defaults `(2.5, 55.5, True, False)` when the endpoint
returns no items, errors out, or the JSON cannot be decoded —
preserving the legacy caller-visible behavior on those failure
paths.

#### \_\_init_\_(gameId=0, raw=False, path_to_json='/', return_keys=None, \*\*kwargs)

#### cfb_pbp_disk()

Load a previously cached ESPN summary JSON for this game from disk.

Reads `{path_to_json}/{gameId}.json` where `path_to_json` was passed
to the [`CFBPlayProcess`](#sportsdataverse.cfb.cfb_pbp.CFBPlayProcess) constructor.

* **Returns:**
  Parsed JSON contents, also stored on `self.json`.
* **Return type:**
  dict

### Example

Quick start:

```default
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334, path_to_json="./cache")
pbp = game.cfb_pbp_disk()
print(list(pbp.keys()))
```

#### cfb_pbp_json(\*\*kwargs)

Return the JSON payload currently attached to this [`CFBPlayProcess`](#sportsdataverse.cfb.cfb_pbp.CFBPlayProcess)
instance.

* **Returns:**
  The cached JSON payload (`self.json`).
* **Return type:**
  dict

### Example

Quick start:

```default
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
cached = game.cfb_pbp_json()
```

#### corrupt_pbp_check()

Heuristic check for corrupt or incomplete play-by-play.

Flags games with zero plays, fewer than 50 plays for a completed game,
or more than 500 plays for a completed game – all of which historically
indicate ESPN delivered a malformed PBP payload that should not be
processed downstream.

* **Returns:**
  True if PBP looks corrupt and the processing pipeline should
  be skipped, False otherwise.
* **Return type:**
  bool

### Example

Quick start:

```default
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
if not game.corrupt_pbp_check():
    game.run_processing_pipeline()
```

#### create_box_score(play_df)

Build a per-team and per-player advanced box score from a processed
plays frame.

Triggers [`run_processing_pipeline()`](#sportsdataverse.cfb.cfb_pbp.CFBPlayProcess.run_processing_pipeline) first if it hasn’t already run,
so the input `play_df` is expected to be the post-pipeline plays frame.

* **Parameters:**
  **play_df** (*pl.DataFrame*) – The plays frame produced by
  [`run_processing_pipeline()`](#sportsdataverse.cfb.cfb_pbp.CFBPlayProcess.run_processing_pipeline) (with EPA, WPA and play-type
  flags already populated).
* **Returns:**
  Box-score sections keyed by `"passing"`, `"rushing"`,
  `"receiving"`, `"defensive"`, `"turnover"`, and `"drives"`.
* **Return type:**
  dict

### Example

Quick start:

```default
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
processed = game.run_processing_pipeline()
box = game.create_box_score(game.plays_json)
print(list(box.keys()))
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package

#### espn_cfb_pbp(\*\*kwargs)

espn_cfb_pbp() - Pull the game by id. Data from API endpoints: college-football/playbyplay,
college-football/summary

* **Parameters:**
  * **game_id** (*int*) – Unique game_id, can be obtained from cfb_schedule().
  * **raw** (*bool*) – If True, returns the raw json from the API endpoint. If False, returns a
  * **datasets.** (*cleaned dictionary of*)
* **Returns:**
  Dictionary of game data with keys - “gameId”, “plays”, “boxscore”, “header”, “broadcasts”,
  : ”videos”, “playByPlaySource”, “standings”, “leaders”, “timeouts”, “homeTeamSpread”, “overUnder”,
    “pickcenter”, “againstTheSpread”, “odds”, “predictor”, “winprobability”, “espnWP”,
    “gameInfo”, “season”
* **Return type:**
  Dict

### Example

Quick start:

```default
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
pbp = game.espn_cfb_pbp()
print(list(pbp.keys()))
```

Pull only the raw ESPN summary payload (skip cleaning):

```default
raw_pbp = CFBPlayProcess(gameId=401628334, raw=True).espn_cfb_pbp()
```

Pipeline next step (run the full processing pipeline for advanced features):

```default
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
processed = game.run_processing_pipeline()  # adds EPA, WPA, box score
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB PBP
  * [nflverse](https://nflverse.nflverse.com) – companion data ecosystem for the NFL

#### gameId *= 0*

#### path_to_json *= '/'*

#### ran_cleaning_pipeline *= False*

#### ran_pipeline *= False*

#### raw *= False*

#### return_keys *= None*

#### run_cleaning_pipeline()

Run the lighter cleaning pipeline (no EPA/WPA/QBR/box-score).

Same per-play feature engineering as [`run_processing_pipeline()`](#sportsdataverse.cfb.cfb_pbp.CFBPlayProcess.run_processing_pipeline)
through `__add_spread_time`, but stops short of the modeling steps.
Use this when you only need cleaned plays and don’t need expected
points or win probability columns.

* **Returns:**
  Cleaned game payload (no `advBoxScore` key).
* **Return type:**
  dict

### Example

Quick start:

```default
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
cleaned = game.run_cleaning_pipeline()
print(len(cleaned["plays"]))
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB PBP

#### run_processing_pipeline()

Run the full play-by-play processing pipeline.

Applies every scoring/feature step in order: down detection, play type
flags, rush/pass flags, team score variables, new play types, penalty
setup, play category flags, yardage cols, player cols, after cols,
spread time, EPA, WPA, drive data, and QBR. Also produces an advanced
box score and stores it under `advBoxScore` on the returned dict.

Idempotent – subsequent calls return the cached `self.json`.

* **Returns:**
  The fully-processed game payload. If the constructor was
  given `return_keys`, only those keys are returned.
* **Return type:**
  dict

### Example

Quick start:

```default
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
processed = game.run_processing_pipeline()
print(processed["advBoxScore"].keys())
```

Pipeline next step (return only selected keys):

```default
game = CFBPlayProcess(gameId=401628334, return_keys=["plays", "advBoxScore"])
game.espn_cfb_pbp()
trimmed = game.run_processing_pipeline()
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB PBP

## sportsdataverse.cfb.cfb_play_participants module

ESPN college-football play-participants scraper.

Single ESPN endpoint:
: sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{game_id}/competitions/{game_id}/plays?limit=1000

ESPN’s per-play `participants[]` array is the authoritative source for which
athletes were involved in each play (passer, rusher, receiver, tackler, etc.).
This wrapper pulls the full play-list for a game, extracts the participants,
resolves each `$ref` URL into an `athlete_id` / `position_id`, attaches
the per-athlete display name from a sibling roster lookup, and pivots the
result so each play has one row keyed by `play_id` with the participant
display name and id materialized as `{type}_player_name` /
`{type}_player_id` columns (e.g. `passer_player_name`).

Designed to replace the regex-based player-name extraction the
`cfb_pbp.CFBPlayProcess.__add_player_cols` method previously did against
the freeform `text` column. Coverage was probed back to season 2014 (the
earliest season with reliable ESPN CFB PBP coverage) and is solid for every
sampled era — see the project diff doc for the probe table.

Caveats:

* `$ref` URLs are parsed for the athlete/position id (the trailing numeric
  segment). The full `$ref` URL is also retained so the optional
  `resolve_missing` pass can fetch any athlete the sidecar omitted.
* Display names come primarily from the `cdn.espn.com/.../playbyplay`
  sidecar (the same one the legacy class uses). The sidecar is one round
  trip for the whole roster, but it is built from the box-score side and
  occasionally omits athletes who appear only in the participants payload
  (split sacks where the second sacker isn’t on the leaders list, returners
  on lateral plays, etc.). When `resolve_missing=True` (the default),
  athletes still missing a name after the sidecar pass are fetched
  one-by-one from their canonical `$ref` URL and the names backfilled
  before the pivot. The fan-out is capped per game (default 50) so a
  pathological game can’t run away.
* Pagination: the endpoint historically caps at one page of 1000 plays per
  game. We follow the `pageCount` cursor defensively in case ESPN ever
  changes that.

### sportsdataverse.cfb.cfb_play_participants.espn_cfb_play_participants(game_id: int, *, raw: Literal[True], return_as_pandas: bool = False, resolve_missing: bool = True, resolve_missing_max: int = 50, \*\*kwargs: Any) → dict[str, Any]

### sportsdataverse.cfb.cfb_play_participants.espn_cfb_play_participants(game_id: int, *, raw: Literal[False] = False, return_as_pandas: Literal[True], resolve_missing: bool = True, resolve_missing_max: int = 50, \*\*kwargs: Any) → DataFrame

### sportsdataverse.cfb.cfb_play_participants.espn_cfb_play_participants(game_id: int, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, resolve_missing: bool = True, resolve_missing_max: int = 50, \*\*kwargs: Any) → DataFrame

Pull ESPN per-play participants for a college-football game.

* **Parameters:**
  * **game_id** – ESPN game / event identifier.
  * **raw** – If True, returns the raw list of play-items dicts (after
    following pagination) before any flattening.
  * **return_as_pandas** – If True, returns a pandas DataFrame; otherwise polars.
  * **resolve_missing** – If True (default), athletes that the
    `cdn.espn.com` sidecar omits are fetched one-by-one from
    their canonical ESPN `$ref` URL so the resulting frame has
    populated `*_player_name` / `*_player_names` columns
    wherever an `*_player_id` is non-null. Setting this to
    False skips the extra HTTP fan-out and reproduces the
    pre-enhancement behavior — rows may then ship with
    `*_player_id` populated but `*_player_name` null on the
    handful of athletes the sidecar misses (most visible on
    split sacks, multi-lateral returns, and older games).
  * **resolve_missing_max** – Hard cap on the number of per-athlete
    `$ref` requests issued for a single game. Defaults to 50,
    which comfortably covers every probed game (typical max is
    ≤8 unique missing athletes). If breached, a warning is
    logged and the remaining missing athletes are left with
    null names. Ignored when `resolve_missing=False`.
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download`.
* **Returns:**
  Polars (or pandas) DataFrame, one row per play. Columns include
  `game_id`, `play_id`, and TWO column families for every
  participant `type` ESPN ships for the game (typical types:
  `passer`, `rusher`, `receiver`, `tackler`, `sacked_by`,
  `forced_by`, `pass_defender`, `kicker`, `punter`,
  `returner`, `recoverer`, `scorer`, `pat_scorer`,
  `penalized`, `assisted_by`):
  * **Scalar** — `{type}_player_id` / `{type}_player_name`: the
    first occurrence of that participant type on the play. Backwards
    compatible with the legacy regex-extractor shape.
  * **List** — `{type}_player_ids` / `{type}_player_names`:
    `List(Utf8)` columns containing **every** occurrence of that
    participant type on the play, in the order ESPN shipped them.
    Plays with no participant of a given type carry an empty list
    `[]` (not null) for downstream consumption simplicity. This
    family preserves multi-entry participant types (split sacks
    where ESPN ships two `sackedBy` entries, multi-tacklers,
    etc.) that the scalar family collapses to first-only.

  If `raw=True`, returns the parsed JSON list of play dicts.
* **Raises:**
  * **sportsdataverse.errors.NoESPNDataError** – ESPN returned 404.
  * **requests.exceptions.RequestException** – Other network failures after retries.

### Example

Quick start:

```default
from sportsdataverse.cfb import espn_cfb_play_participants
participants = espn_cfb_play_participants(game_id=401628334)
print(participants.shape)
```

Skip the per-athlete fan-out for speed:

```default
participants_fast = espn_cfb_play_participants(
    game_id=401628334,
    resolve_missing=False,
)
```

Pipeline next step (join onto play-by-play frame):

```default
from sportsdataverse.cfb import CFBPlayProcess
pbp = CFBPlayProcess(gameId=401628334).espn_cfb_pbp()
plays = pbp["plays"]
joined = plays.join(participants, how="left", left_on="id", right_on="play_id")
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB PBP
  * [nflverse](https://nflverse.nflverse.com) – companion data ecosystem for the NFL

## sportsdataverse.cfb.cfb_schedule module

### sportsdataverse.cfb.cfb_schedule.espn_cfb_calendar(season=None, groups=None, ondays=None, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_cfb_calendar - look up the men’s college football calendar for a given season

* **Parameters:**
  * **season** (*int*) – Used to define different seasons. 2002 is the earliest available season.
  * **groups** (*int*) – Used to define different divisions. 80 is FBS, 81 is FCS.
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
from sportsdataverse.cfb import espn_cfb_calendar
cal = espn_cfb_calendar(season=2023)
print(cal.shape)
```

Use ondays to get every scheduled date for the season:

```default
ondays = espn_cfb_calendar(season=2023, ondays=True)
```

Pipeline next step (loop the URLs to scrape day-by-day):

```default
cal = espn_cfb_calendar(season=2023, ondays=True)
urls = cal["url"].to_list()  # feed each into espn_cfb_schedule
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB schedules

### sportsdataverse.cfb.cfb_schedule.espn_cfb_schedule(dates=None, week=None, season_type=None, groups=None, limit=500, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_cfb_schedule - look up the college football schedule for a given season

* **Parameters:**
  * **dates** (*int*) – Used to define different seasons. 2002 is the earliest available season.
  * **week** (*int*) – Week of the schedule.
  * **groups** (*int*) – Used to define different divisions. 80 is FBS, 81 is FCS.
  * **season_type** (*int*) – 2 for regular season, 3 for post-season, 4 for off-season.
  * **limit** (*int*) – number of records to return, default: 500.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing schedule dates for the requested season. Returns None if no games
* **Return type:**
  pl.DataFrame

### Example

Quick start (today’s slate):

```default
from sportsdataverse.cfb import espn_cfb_schedule
slate = espn_cfb_schedule()
print(slate.shape if slate is not None else "no games")
```

Pull a specific week of FBS games:

```default
week5 = espn_cfb_schedule(dates=2023, week=5, season_type=2)
```

Pipeline next step (extract finals only):

```default
import polars as pl
finals = espn_cfb_schedule(dates=2023, week=5).filter(
    pl.col("status_type_completed") == True
)
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB schedules
  * [nflverse](https://nflverse.nflverse.com) – companion data ecosystem for the NFL

### sportsdataverse.cfb.cfb_schedule.most_recent_cfb_season()

Return the most recent college football season year based on today’s date.

The college football season starts in mid-August. If today is on or after
August 15 (or any day in September or later), this returns the current
calendar year. Otherwise, it returns the previous calendar year.

* **Returns:**
  The most recent CFB season year.
* **Return type:**
  int

### Example

Quick start:

```default
from sportsdataverse.cfb import most_recent_cfb_season
year = most_recent_cfb_season()
print(year)
```

Combine with the loaders for a “current season” pull:

```default
from sportsdataverse.cfb import load_cfb_schedule, most_recent_cfb_season
sched = load_cfb_schedule(seasons=[most_recent_cfb_season()])
```

### sportsdataverse.cfb.cfb_schedule.scoreboard_event_parsing(event)

Internal helper that flattens an ESPN scoreboard event dict into a shape
suitable for `pd.json_normalize`.

* **Parameters:**
  **event** (*dict*) – A single scoreboard `events[*]` entry from the ESPN
  college-football scoreboard API.
* **Returns:**
  The same event dict, mutated in place with `home`/`away`
  copies of the competitors and trimmed of unused link/odds keys.
* **Return type:**
  dict

### Example

Used internally by [`espn_cfb_schedule()`](#sportsdataverse.cfb.cfb_schedule.espn_cfb_schedule):

```default
from sportsdataverse.cfb import espn_cfb_schedule
sched = espn_cfb_schedule(dates=2023, week=5)
```

## sportsdataverse.cfb.cfb_teams module

### sportsdataverse.cfb.cfb_teams.espn_cfb_teams(groups=None, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_cfb_teams - look up the college football teams

* **Parameters:**
  * **groups** (*int*) – Used to define different divisions. 80 is FBS, 81 is FCS.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing schedule dates for the requested season.
  This function caches by default, so if you want to refresh the data, use the command
  sportsdataverse.cfb.espn_cfb_teams.clear_cache().
* **Return type:**
  pl.DataFrame

### Example

Quick start (FBS only by default):

```default
from sportsdataverse.cfb import espn_cfb_teams
teams = espn_cfb_teams()
print(teams.shape)
```

Pull FCS teams (group 81):

```default
fcs = espn_cfb_teams(groups=81, return_as_pandas=True)
fcs.head()
```

Pipeline next step (build an abbreviation lookup):

```default
teams = espn_cfb_teams()
abbr_map = dict(zip(teams["team_id"], teams["team_abbreviation"]))
```

See Also:
: * [cfbfastR](https://cfbfastR.sportsdataverse.org) – R sister package for CFB team data
  * [recruitR](https://github.com/sportsdataverse/recruitR) – recruiting data companion

## sportsdataverse.cfb.model_vars module

## Module contents
