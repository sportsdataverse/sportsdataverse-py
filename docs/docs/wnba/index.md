<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [sportsdataverse.wnba package](#sportsdataversewnba-package)
  - [Submodules](#submodules)
  - [sportsdataverse.wnba.wnba_draft module](#sportsdataversewnbawnba_draft-module)
    - [sportsdataverse.wnba.wnba_draft.espn_wnba_draft(season: int, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]](#sportsdataversewnbawnba_draftespn_wnba_draftseason-int--raw-literaltrue-return_as_pandas-bool--false-%5C%5Ckwargs-any-%E2%86%92-dictstr-any)
    - [sportsdataverse.wnba.wnba_draft.espn_wnba_draft(season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → DataFrame](#sportsdataversewnbawnba_draftespn_wnba_draftseason-int--raw-literalfalse--false-return_as_pandas-literaltrue-%5C%5Ckwargs-any-%E2%86%92-dataframe)
    - [sportsdataverse.wnba.wnba_draft.espn_wnba_draft(season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → DataFrame](#sportsdataversewnbawnba_draftespn_wnba_draftseason-int--raw-literalfalse--false-return_as_pandas-literalfalse--false-%5C%5Ckwargs-any-%E2%86%92-dataframe)
    - [Example](#example)
  - [sportsdataverse.wnba.wnba_event_officials module](#sportsdataversewnbawnba_event_officials-module)
    - [sportsdataverse.wnba.wnba_event_officials.espn_wnba_event_officials(game_id: int, season: int | None = None, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]](#sportsdataversewnbawnba_event_officialsespn_wnba_event_officialsgame_id-int-season-int--none--none--raw-literaltrue-return_as_pandas-bool--false-%5C%5Ckwargs-any-%E2%86%92-dictstr-any)
    - [sportsdataverse.wnba.wnba_event_officials.espn_wnba_event_officials(game_id: int, season: int | None = None, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → DataFrame](#sportsdataversewnbawnba_event_officialsespn_wnba_event_officialsgame_id-int-season-int--none--none--raw-literalfalse--false-return_as_pandas-literaltrue-%5C%5Ckwargs-any-%E2%86%92-dataframe)
    - [sportsdataverse.wnba.wnba_event_officials.espn_wnba_event_officials(game_id: int, season: int | None = None, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → DataFrame](#sportsdataversewnbawnba_event_officialsespn_wnba_event_officialsgame_id-int-season-int--none--none--raw-literalfalse--false-return_as_pandas-literalfalse--false-%5C%5Ckwargs-any-%E2%86%92-dataframe)
    - [Example](#example-1)
  - [sportsdataverse.wnba.wnba_game_rosters module](#sportsdataversewnbawnba_game_rosters-module)
    - [sportsdataverse.wnba.wnba_game_rosters.espn_wnba_game_rosters(game_id: int, raw=False, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversewnbawnba_game_rostersespn_wnba_game_rostersgame_id-int-rawfalse-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-2)
    - [sportsdataverse.wnba.wnba_game_rosters.helper_wnba_athlete_items(teams_rosters, \*\*kwargs)](#sportsdataversewnbawnba_game_rostershelper_wnba_athlete_itemsteams_rosters-%5C%5Ckwargs)
    - [sportsdataverse.wnba.wnba_game_rosters.helper_wnba_game_items(summary)](#sportsdataversewnbawnba_game_rostershelper_wnba_game_itemssummary)
    - [sportsdataverse.wnba.wnba_game_rosters.helper_wnba_roster_items(items, summary_url, \*\*kwargs)](#sportsdataversewnbawnba_game_rostershelper_wnba_roster_itemsitems-summary_url-%5C%5Ckwargs)
    - [sportsdataverse.wnba.wnba_game_rosters.helper_wnba_team_items(items, \*\*kwargs)](#sportsdataversewnbawnba_game_rostershelper_wnba_team_itemsitems-%5C%5Ckwargs)
  - [sportsdataverse.wnba.wnba_loaders module](#sportsdataversewnbawnba_loaders-module)
    - [sportsdataverse.wnba.wnba_loaders.load_wnba_pbp(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversewnbawnba_loadersload_wnba_pbpseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-3)
    - [sportsdataverse.wnba.wnba_loaders.load_wnba_player_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversewnbawnba_loadersload_wnba_player_boxscoreseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-4)
    - [sportsdataverse.wnba.wnba_loaders.load_wnba_schedule(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversewnbawnba_loadersload_wnba_scheduleseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-5)
    - [sportsdataverse.wnba.wnba_loaders.load_wnba_team_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversewnbawnba_loadersload_wnba_team_boxscoreseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-6)
  - [sportsdataverse.wnba.wnba_pbp module](#sportsdataversewnbawnba_pbp-module)
    - [sportsdataverse.wnba.wnba_pbp.espn_wnba_pbp(game_id: int, raw=False, \*\*kwargs) → Dict](#sportsdataversewnbawnba_pbpespn_wnba_pbpgame_id-int-rawfalse-%5C%5Ckwargs-%E2%86%92-dict)
    - [Example](#example-7)
    - [sportsdataverse.wnba.wnba_pbp.helper_wnba_game_data(pbp_txt, init)](#sportsdataversewnbawnba_pbphelper_wnba_game_datapbp_txt-init)
    - [sportsdataverse.wnba.wnba_pbp.helper_wnba_pbp(game_id, pbp_txt)](#sportsdataversewnbawnba_pbphelper_wnba_pbpgame_id-pbp_txt)
    - [sportsdataverse.wnba.wnba_pbp.helper_wnba_pbp_features(game_id, pbp_txt, init)](#sportsdataversewnbawnba_pbphelper_wnba_pbp_featuresgame_id-pbp_txt-init)
    - [sportsdataverse.wnba.wnba_pbp.helper_wnba_pickcenter(pbp_txt)](#sportsdataversewnbawnba_pbphelper_wnba_pickcenterpbp_txt)
    - [sportsdataverse.wnba.wnba_pbp.wnba_pbp_disk(game_id, path_to_json)](#sportsdataversewnbawnba_pbpwnba_pbp_diskgame_id-path_to_json)
  - [sportsdataverse.wnba.wnba_player_stats module](#sportsdataversewnbawnba_player_stats-module)
    - [sportsdataverse.wnba.wnba_player_stats.espn_wnba_player_stats(athlete_id: int, season: int, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]](#sportsdataversewnbawnba_player_statsespn_wnba_player_statsathlete_id-int-season-int--raw-literaltrue-return_as_pandas-bool--false-%5C%5Ckwargs-any-%E2%86%92-dictstr-any)
    - [sportsdataverse.wnba.wnba_player_stats.espn_wnba_player_stats(athlete_id: int, season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → dict[str, DataFrame]](#sportsdataversewnbawnba_player_statsespn_wnba_player_statsathlete_id-int-season-int--raw-literalfalse--false-return_as_pandas-literaltrue-%5C%5Ckwargs-any-%E2%86%92-dictstr-dataframe)
    - [sportsdataverse.wnba.wnba_player_stats.espn_wnba_player_stats(athlete_id: int, season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → dict[str, DataFrame]](#sportsdataversewnbawnba_player_statsespn_wnba_player_statsathlete_id-int-season-int--raw-literalfalse--false-return_as_pandas-literalfalse--false-%5C%5Ckwargs-any-%E2%86%92-dictstr-dataframe)
    - [Example](#example-8)
  - [sportsdataverse.wnba.wnba_schedule module](#sportsdataversewnbawnba_schedule-module)
    - [sportsdataverse.wnba.wnba_schedule.espn_wnba_calendar(season=None, ondays=None, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversewnbawnba_scheduleespn_wnba_calendarseasonnone-ondaysnone-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-9)
    - [sportsdataverse.wnba.wnba_schedule.espn_wnba_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversewnbawnba_scheduleespn_wnba_scheduledatesnone-season_typenone-limit500-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-10)
    - [sportsdataverse.wnba.wnba_schedule.most_recent_wnba_season()](#sportsdataversewnbawnba_schedulemost_recent_wnba_season)
    - [Example](#example-11)
    - [sportsdataverse.wnba.wnba_schedule.scoreboard_event_parsing(event)](#sportsdataversewnbawnba_schedulescoreboard_event_parsingevent)
  - [sportsdataverse.wnba.wnba_standings module](#sportsdataversewnbawnba_standings-module)
    - [sportsdataverse.wnba.wnba_standings.espn_wnba_standings(season: int, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]](#sportsdataversewnbawnba_standingsespn_wnba_standingsseason-int--raw-literaltrue-return_as_pandas-bool--false-%5C%5Ckwargs-any-%E2%86%92-dictstr-any)
    - [sportsdataverse.wnba.wnba_standings.espn_wnba_standings(season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → DataFrame](#sportsdataversewnbawnba_standingsespn_wnba_standingsseason-int--raw-literalfalse--false-return_as_pandas-literaltrue-%5C%5Ckwargs-any-%E2%86%92-dataframe)
    - [sportsdataverse.wnba.wnba_standings.espn_wnba_standings(season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → DataFrame](#sportsdataversewnbawnba_standingsespn_wnba_standingsseason-int--raw-literalfalse--false-return_as_pandas-literalfalse--false-%5C%5Ckwargs-any-%E2%86%92-dataframe)
    - [Example](#example-12)
  - [sportsdataverse.wnba.wnba_team_roster module](#sportsdataversewnbawnba_team_roster-module)
    - [sportsdataverse.wnba.wnba_team_roster.espn_wnba_team_roster(team_id: int, season: int | None = None, *, raw: bool = False, return_as_pandas: bool = False, \*\*kwargs: Any) → DataFrame | DataFrame | dict[str, Any]](#sportsdataversewnbawnba_team_rosterespn_wnba_team_rosterteam_id-int-season-int--none--none--raw-bool--false-return_as_pandas-bool--false-%5C%5Ckwargs-any-%E2%86%92-dataframe--dataframe--dictstr-any)
    - [Example](#example-13)
  - [sportsdataverse.wnba.wnba_team_stats module](#sportsdataversewnbawnba_team_stats-module)
    - [sportsdataverse.wnba.wnba_team_stats.espn_wnba_team_stats(team_id: int, season: int, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]](#sportsdataversewnbawnba_team_statsespn_wnba_team_statsteam_id-int-season-int--raw-literaltrue-return_as_pandas-bool--false-%5C%5Ckwargs-any-%E2%86%92-dictstr-any)
    - [sportsdataverse.wnba.wnba_team_stats.espn_wnba_team_stats(team_id: int, season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → dict[str, DataFrame]](#sportsdataversewnbawnba_team_statsespn_wnba_team_statsteam_id-int-season-int--raw-literalfalse--false-return_as_pandas-literaltrue-%5C%5Ckwargs-any-%E2%86%92-dictstr-dataframe)
    - [sportsdataverse.wnba.wnba_team_stats.espn_wnba_team_stats(team_id: int, season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → dict[str, DataFrame]](#sportsdataversewnbawnba_team_statsespn_wnba_team_statsteam_id-int-season-int--raw-literalfalse--false-return_as_pandas-literalfalse--false-%5C%5Ckwargs-any-%E2%86%92-dictstr-dataframe)
    - [Example](#example-14)
  - [sportsdataverse.wnba.wnba_teams module](#sportsdataversewnbawnba_teams-module)
    - [sportsdataverse.wnba.wnba_teams.espn_wnba_teams(return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversewnbawnba_teamsespn_wnba_teamsreturn_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-15)
  - [Module contents](#module-contents)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# sportsdataverse.wnba package

## Submodules

## sportsdataverse.wnba.wnba_draft module

ESPN WNBA draft picks scraper.

Single ESPN endpoint:
: site.web.api.espn.com/apis/site/v2/sports/basketball/wnba/draft?season={year}

ESPN ships the modern draft response with each pick inlined under
`picks[]`, carrying the rich athlete metadata (display name, height,
position id, college team, headshot, ESPN profile link) the older
`sports.core.api.espn.com` `/draft/rounds` endpoint required a separate
`$ref` resolution to fetch. This wrapper flattens that `picks[]` array
to a single polars DataFrame, one row per pick.

Fields ESPN does not inline on the draft response (e.g. `firstName` /
`lastName`, `weight`, `age`, birth city / state, full position name,
school id) come back as `None`; resolve them via
`espn_wnba_athlete_info` (or the matching wehoop R wrapper) using the
returned `athlete_id`.

### sportsdataverse.wnba.wnba_draft.espn_wnba_draft(season: int, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]

### sportsdataverse.wnba.wnba_draft.espn_wnba_draft(season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → DataFrame

### sportsdataverse.wnba.wnba_draft.espn_wnba_draft(season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → DataFrame

Pull ESPN WNBA draft picks for a season.

* **Parameters:**
  * **season** – Season year (e.g. `2024` for the 2024 WNBA Draft).
    Forwarded to ESPN as `?season=YYYY`.
  * **raw** – If True, returns the parsed JSON dict before any flattening.
  * **return_as_pandas** – If True, returns a pandas DataFrame; otherwise
    polars.
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download`.
* **Returns:**
  Polars (or pandas) DataFrame with one row per draft pick.
  Documented columns: `season`, `round_number`, `pick_number`,
  `overall_pick`, `team_id`, `team_abbreviation`,
  `team_display_name`, `athlete_id`, `athlete_first_name`,
  `athlete_last_name`, `athlete_full_name`,
  `athlete_display_name`, `athlete_position_id`,
  `athlete_position_name`, `athlete_position_abbreviation`,
  `athlete_height`, `athlete_weight`, `athlete_age`,
  `athlete_birth_city`, `athlete_birth_state`, `headshot_href`,
  `school_id`, `school_name`, `school_abbreviation`,
  `link_web`.

  Fields ESPN does not inline on the draft response (e.g.
  first / last name, weight, age, birth location, school id) come
  back as `None`; resolve them via the athlete-info endpoint
  using the returned `athlete_id`.

  If `raw=True`, returns the raw response dict.
* **Raises:**
  * [**sportsdataverse.errors.NoESPNDataError**](sportsdataverse.md#sportsdataverse.errors.NoESPNDataError) – ESPN returned 404.
  * **requests.exceptions.RequestException** – Other network failures after
        retries.

### Example

Pull a single draft year — one row per pick:

```default
from sportsdataverse.wnba import espn_wnba_draft
draft = espn_wnba_draft(season=2024)
print(draft.shape)
draft.select(
    ["overall_pick", "round_number", "team_abbreviation", "athlete_display_name", "school_name"]
).head(12)
```

First-round picks only:

```default
import polars as pl
draft.filter(pl.col("round_number") == 1).head()
```

Pandas round-trip — convenient for joining against your own roster table:

```default
draft_pd = espn_wnba_draft(season=2024, return_as_pandas=True)
draft_pd[["overall_pick", "athlete_display_name", "school_name"]].head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

## sportsdataverse.wnba.wnba_event_officials module

ESPN WNBA game officials scraper.

Mirror of `sportsdataverse.wbb.espn_wbb_event_officials()` for the WNBA
league slug. The actual fetch + parse logic lives in
`sportsdataverse.wbb.wbb_event_officials._espn_basketball_event_officials`
to keep the wbb / wnba pair DRY.

### sportsdataverse.wnba.wnba_event_officials.espn_wnba_event_officials(game_id: int, season: int | None = None, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]

### sportsdataverse.wnba.wnba_event_officials.espn_wnba_event_officials(game_id: int, season: int | None = None, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → DataFrame

### sportsdataverse.wnba.wnba_event_officials.espn_wnba_event_officials(game_id: int, season: int | None = None, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → DataFrame

Pull the officials assigned to a WNBA game.

See `sportsdataverse.wbb.espn_wbb_event_officials()` for full
documentation of the column set, the empty-frame fallback when ESPN
ships no officials, and the `raw` / `return_as_pandas` flag
semantics.

* **Parameters:**
  * **game_id** – ESPN WNBA event identifier (e.g. `401620238` for Game 1
    of the 2024 WNBA Finals).
  * **season** – Season year (recorded as output column only).
  * **raw** – If True, returns the parsed JSON dict before any flattening.
  * **return_as_pandas** – If True, returns a pandas DataFrame; otherwise polars.
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download`.
* **Returns:**
  Polars (or pandas) DataFrame with the same columns documented in
  `sportsdataverse.wbb.espn_wbb_event_officials()`. If
  `raw=True`, returns the raw response dict.
* **Raises:**
  * [**sportsdataverse.errors.NoESPNDataError**](sportsdataverse.md#sportsdataverse.errors.NoESPNDataError) – ESPN returned 404.
  * **requests.exceptions.RequestException** – Other network failures after retries.

### Example

Pull officials for the 2024 WNBA Finals Game 1:

```default
from sportsdataverse.wnba import espn_wnba_event_officials
refs = espn_wnba_event_officials(game_id=401620238, season=2024)
print(refs.shape)
refs.select(["full_name", "position_name", "order"]).head()
```

Pandas round-trip:

```default
refs_pd = espn_wnba_event_officials(
    game_id=401620238, season=2024, return_as_pandas=True
)
refs_pd[["full_name", "position_name"]].head()
```

Inspect the raw ESPN payload (e.g. for fields not flattened):

```default
payload = espn_wnba_event_officials(game_id=401620238, season=2024, raw=True)
list(payload.keys())[:8]
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

## sportsdataverse.wnba.wnba_game_rosters module

### sportsdataverse.wnba.wnba_game_rosters.espn_wnba_game_rosters(game_id: int, raw=False, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_wnba_game_rosters() - Pull the game by id.

* **Parameters:**
  * **game_id** (*int*) – Unique game_id, can be obtained from espn_wnba_schedule().
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars Data frame of game roster data with columns:
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

Pull both teams’ rosters for a single game:

```default
from sportsdataverse.wnba import espn_wnba_game_rosters
rosters = espn_wnba_game_rosters(game_id=401620238)  # 2024 WNBA Finals Game 1
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
rosters_pd = espn_wnba_game_rosters(game_id=401620238, return_as_pandas=True)
rosters_pd[["athlete_display_name", "team_abbreviation", "did_not_play"]].head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

### sportsdataverse.wnba.wnba_game_rosters.helper_wnba_athlete_items(teams_rosters, \*\*kwargs)

### sportsdataverse.wnba.wnba_game_rosters.helper_wnba_game_items(summary)

### sportsdataverse.wnba.wnba_game_rosters.helper_wnba_roster_items(items, summary_url, \*\*kwargs)

### sportsdataverse.wnba.wnba_game_rosters.helper_wnba_team_items(items, \*\*kwargs)

## sportsdataverse.wnba.wnba_loaders module

### sportsdataverse.wnba.wnba_loaders.load_wnba_pbp(seasons: List[int], return_as_pandas=False) → DataFrame

Load WNBA play by play data going back to 2002

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

Pull a single season’s play-by-play parquet:

```default
from sportsdataverse.wnba import load_wnba_pbp
pbp = load_wnba_pbp(seasons=2024)
print(pbp.shape)
```

Pull a range of seasons (closed-open like Python `range`):

```default
pbp = load_wnba_pbp(seasons=range(2020, 2025))
pbp.group_by("season").len().sort("season")
```

Pandas round-trip and a quick filter on play type:

```default
pbp_pd = load_wnba_pbp(seasons=[2024], return_as_pandas=True)
pbp_pd[pbp_pd["type_text"] == "JumpShot"].head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

### sportsdataverse.wnba.wnba_loaders.load_wnba_player_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame

Load WNBA player boxscore data

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

Pull player box scores for a single season:

```default
from sportsdataverse.wnba import load_wnba_player_boxscore
pb = load_wnba_player_boxscore(seasons=2024)
print(pb.shape)
```

A’ja Wilson (athlete_id 3149391) game-by-game scoring:

```default
import polars as pl
wilson = pb.filter(pl.col("athlete_id") == 3149391)
wilson.select(["game_id", "minutes", "points", "rebounds", "assists"]).head()
```

Pandas round-trip across multiple seasons:

```default
pb_pd = load_wnba_player_boxscore(seasons=range(2022, 2025), return_as_pandas=True)
pb_pd.groupby("season")["points"].mean()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

### sportsdataverse.wnba.wnba_loaders.load_wnba_schedule(seasons: List[int], return_as_pandas=False) → DataFrame

Load WNBA schedule data

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

Pull a single season’s schedule:

```default
from sportsdataverse.wnba import load_wnba_schedule
sched = load_wnba_schedule(seasons=2024)
print(sched.shape)
```

Pull a range of seasons and count by status:

```default
sched = load_wnba_schedule(seasons=range(2020, 2025))
sched.group_by(["season", "status_type_description"]).len().sort(["season", "len"])
```

Pandas round-trip with a single season:

```default
sched_pd = load_wnba_schedule(seasons=[2024], return_as_pandas=True)
sched_pd[["game_id", "home_name", "away_name", "game_date"]].head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

### sportsdataverse.wnba.wnba_loaders.load_wnba_team_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame

Load WNBA team boxscore data

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

Pull team box scores for a single season:

```default
from sportsdataverse.wnba import load_wnba_team_boxscore
tb = load_wnba_team_boxscore(seasons=2024)
print(tb.shape)
```

Pull a range of seasons:

```default
tb = load_wnba_team_boxscore(seasons=range(2020, 2025))
tb.group_by("season").len().sort("season")
```

Aces (team_id 17) game-by-game scoring:

```default
import polars as pl
tb.filter(pl.col("team_id") == 17).select(["game_id", "team_score", "opponent_team_score"]).head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

## sportsdataverse.wnba.wnba_pbp module

### sportsdataverse.wnba.wnba_pbp.espn_wnba_pbp(game_id: int, raw=False, \*\*kwargs) → Dict

espn_wnba_pbp() - Pull the game by id. Data from API endpoints - wnba/playbyplay, wnba/summary

* **Parameters:**
  **game_id** (*int*) – Unique game_id, can be obtained from wnba_schedule().
* **Returns:**
  Dictionary of game data with keys - “gameId”, “plays”, “winprobability”, “boxscore”, “header”,
  : ”broadcasts”, “videos”, “playByPlaySource”, “standings”, “leaders”, “seasonseries”, “timeouts”,
    “pickcenter”, “againstTheSpread”, “odds”, “predictor”, “espnWP”, “gameInfo”, “season”
* **Return type:**
  Dict

### Example

Pull a single game’s play-by-play feed:

```default
from sportsdataverse.wnba import espn_wnba_pbp
game = espn_wnba_pbp(game_id=401620238)  # 2024 WNBA Finals Game 1
list(game.keys())  # ['gameId', 'plays', 'winprobability', ...]
```

Inspect the parsed plays and a header summary:

```default
import polars as pl
plays = pl.DataFrame(game["plays"])
print(plays.shape)
print(plays.select(["period", "time", "type.text", "text"]).head(5))
```

Fetch the unparsed payload for custom downstream parsing:

```default
raw = espn_wnba_pbp(game_id=401620238, raw=True)
sorted(raw.keys())[:5]  # raw ESPN summary keys, no flattening
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

### sportsdataverse.wnba.wnba_pbp.helper_wnba_game_data(pbp_txt, init)

### sportsdataverse.wnba.wnba_pbp.helper_wnba_pbp(game_id, pbp_txt)

### sportsdataverse.wnba.wnba_pbp.helper_wnba_pbp_features(game_id, pbp_txt, init)

### sportsdataverse.wnba.wnba_pbp.helper_wnba_pickcenter(pbp_txt)

### sportsdataverse.wnba.wnba_pbp.wnba_pbp_disk(game_id, path_to_json)

## sportsdataverse.wnba.wnba_player_stats module

ESPN WNBA athlete season stats scraper.

Mirror of `sportsdataverse.wbb.espn_wbb_player_stats()` for the WNBA
league slug. The actual fetch + parse logic lives in
`sportsdataverse.wbb.wbb_player_stats._espn_basketball_player_stats` to
keep the wbb / wnba pair DRY.

### sportsdataverse.wnba.wnba_player_stats.espn_wnba_player_stats(athlete_id: int, season: int, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]

### sportsdataverse.wnba.wnba_player_stats.espn_wnba_player_stats(athlete_id: int, season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → dict[str, DataFrame]

### sportsdataverse.wnba.wnba_player_stats.espn_wnba_player_stats(athlete_id: int, season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → dict[str, DataFrame]

Pull ESPN season stats for a WNBA athlete.

See `sportsdataverse.wbb.espn_wbb_player_stats()` for full
documentation of the return shape, the canonical three category keys
(`"Averages"`, `"Totals"`, `"Misc"`), the per-category column
set, and the `"Other"` fallback bucket.

* **Parameters:**
  * **athlete_id** – ESPN WNBA athlete identifier (e.g. `3149391` for A’ja
    Wilson).
  * **season** – Season year, forwarded to ESPN as `?season=YYYY`.
  * **raw** – If True, returns the parsed JSON dict before any flattening.
  * **return_as_pandas** – If True, returns a dict of pandas DataFrames;
    otherwise polars.
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download`.
* **Returns:**
  Dict with one DataFrame per stat category — see
  `sportsdataverse.wbb.espn_wbb_player_stats()` for the full
  column / key documentation. If `raw=True`, returns the raw
  response dict.
* **Raises:**
  * [**sportsdataverse.errors.NoESPNDataError**](sportsdataverse.md#sportsdataverse.errors.NoESPNDataError) – ESPN returned 404.
  * **requests.exceptions.RequestException** – Other network failures after
        retries.

### Example

Pull A’ja Wilson’s 2024 season stats and inspect the canonical category keys:

```default
from sportsdataverse.wnba import espn_wnba_player_stats
frames = espn_wnba_player_stats(athlete_id=3149391, season=2024)
sorted(frames.keys())  # at minimum: 'Averages', 'Totals', 'Misc'
frames["Averages"].head()
```

Combine the per-game `Averages` and full-season `Totals`:

```default
avgs = frames["Averages"]
totals = frames["Totals"]
print(avgs.shape, totals.shape)
avgs.select(["points_per_game", "rebounds_per_game", "assists_per_game"]).head()
```

Pandas round-trip — returns a dict of DataFrames keyed by category:

```default
frames_pd = espn_wnba_player_stats(
    athlete_id=3149391, season=2024, return_as_pandas=True
)
frames_pd["Misc"].head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

## sportsdataverse.wnba.wnba_schedule module

### sportsdataverse.wnba.wnba_schedule.espn_wnba_calendar(season=None, ondays=None, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_wnba_calendar - look up the WNBA calendar for a given season

* **Parameters:**
  * **season** (*int*) – Used to define different seasons. 2002 is the earliest available season.
  * **ondays** (*boolean*) – Used to return dates for calendar ondays
* **Returns:**
  Polars dataframe containing calendar dates for the requested season.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2002.

### Example

Calendar entries for a season:

```default
from sportsdataverse.wnba import espn_wnba_calendar
cal = espn_wnba_calendar(season=2024)
print(cal.shape)
cal.head()
```

Just the on-days (game-played dates), useful for batch loops:

```default
ondays = espn_wnba_calendar(season=2024, ondays=True)
for url in ondays["url"].head(3).to_list():
    print(url)
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

### sportsdataverse.wnba.wnba_schedule.espn_wnba_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_wnba_schedule - look up the WNBA schedule for a given season

* **Parameters:**
  * **dates** (*int*) – Used to define different seasons. 2002 is the earliest available season.
  * **season_type** (*int*) – 2 for regular season, 3 for post-season, 4 for off-season.
  * **limit** (*int*) – number of records to return, default: 500.
* **Returns:**
  Polars dataframe containing schedule dates for the requested season. Returns None if no games
* **Return type:**
  pl.DataFrame

### Example

Pull a single date’s slate (YYYYMMDD):

```default
from sportsdataverse.wnba import espn_wnba_schedule
sched = espn_wnba_schedule(dates=20241011)  # 2024 WNBA Finals Game 1
print(sched.shape)
sched.select(["game_id", "home_name", "away_name", "status_type_description"]).head()
```

Pull a full regular season’s worth of games:

```default
reg = espn_wnba_schedule(dates=2024, season_type=2, limit=500)
reg.group_by("status_type_description").len().sort("len", descending=True)
```

Pandas round-trip for a single date:

```default
espn_wnba_schedule(dates=20241011, return_as_pandas=True).head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

### sportsdataverse.wnba.wnba_schedule.most_recent_wnba_season()

most_recent_wnba_season - return the most recent (likely-completed) WNBA season year.

Returns the current calendar year if it’s May or later (the WNBA regular
season has tipped off), otherwise the previous calendar year.

* **Returns:**
  Year (e.g. `2024`) suitable for passing as a `season` argument
  to schedule / loader functions.
* **Return type:**
  int

### Example

Use as a default for season-aware loaders:

```default
from sportsdataverse.wnba import most_recent_wnba_season, espn_wnba_calendar
season = most_recent_wnba_season()
cal = espn_wnba_calendar(season=season)
print(season, cal.height)
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

### sportsdataverse.wnba.wnba_schedule.scoreboard_event_parsing(event)

## sportsdataverse.wnba.wnba_standings module

ESPN WNBA standings scraper.

Mirror of `sportsdataverse.wbb.espn_wbb_standings()` for the WNBA
league slug. The actual fetch + parse logic lives in
`sportsdataverse.wbb.wbb_standings._espn_basketball_standings` to keep
the wbb / wnba pair DRY.

Unlike the WBB endpoint, the WNBA standings call doesn’t take a `group`
filter — the league has a single division, so the helper is invoked with
`group=None`.

### sportsdataverse.wnba.wnba_standings.espn_wnba_standings(season: int, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]

### sportsdataverse.wnba.wnba_standings.espn_wnba_standings(season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → DataFrame

### sportsdataverse.wnba.wnba_standings.espn_wnba_standings(season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → DataFrame

Pull ESPN WNBA standings for a season.

See `sportsdataverse.wbb.espn_wbb_standings()` for full
documentation of the column set. The WNBA endpoint does not take a
`group` filter.

* **Parameters:**
  * **season** – Season year, forwarded to ESPN as `?season=YYYY`.
  * **raw** – If True, returns the parsed JSON dict before any flattening.
  * **return_as_pandas** – If True, returns a pandas DataFrame; otherwise
    polars.
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download`.
* **Returns:**
  Polars (or pandas) DataFrame with one row per team — see
  `sportsdataverse.wbb.espn_wbb_standings()` for the full
  column list. If `raw=True`, returns the raw response dict.
* **Raises:**
  * [**sportsdataverse.errors.NoESPNDataError**](sportsdataverse.md#sportsdataverse.errors.NoESPNDataError) – ESPN returned 404.
  * **requests.exceptions.RequestException** – Other network failures after
        retries.

### Example

Pull WNBA standings for a season:

```default
from sportsdataverse.wnba import espn_wnba_standings
standings = espn_wnba_standings(season=2024)
print(standings.shape)
standings.head()
```

Sort by win percentage:

```default
import polars as pl
standings.sort("win_percent", descending=True).select(
    ["team_display_name", "wins", "losses", "win_percent"]
).head(8)
```

Pandas round-trip:

```default
standings_pd = espn_wnba_standings(season=2024, return_as_pandas=True)
standings_pd[["team_display_name", "wins", "losses"]].head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

## sportsdataverse.wnba.wnba_team_roster module

ESPN WNBA team-level season roster scraper.

Mirror of `sportsdataverse.wbb.espn_wbb_team_roster()` for the WNBA
league slug. The actual fetch + parse logic lives in
`sportsdataverse.wbb.wbb_team_roster._espn_basketball_team_roster` to keep
the wbb / wnba pair DRY.

### sportsdataverse.wnba.wnba_team_roster.espn_wnba_team_roster(team_id: int, season: int | None = None, *, raw: bool = False, return_as_pandas: bool = False, \*\*kwargs: Any) → DataFrame | DataFrame | dict[str, Any]

Pull the current ESPN team roster for a WNBA team.

See `sportsdataverse.wbb.espn_wbb_team_roster()` for full documentation
of the column set. ESPN’s `/teams/{id}/roster` endpoint ignores
`?season=YYYY`; the `season` argument is recorded as an output column
only and does not alter the request URL.

* **Parameters:**
  * **team_id** – ESPN WNBA team identifier (e.g. `3` for Dallas Wings).
  * **season** – Season year (recorded as output column only).
  * **raw** – If True, returns the parsed JSON dict before any flattening.
  * **return_as_pandas** – If True, returns a pandas DataFrame; otherwise polars.
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download`.
* **Returns:**
  Polars (or pandas) DataFrame with the same columns documented in
  `sportsdataverse.wbb.espn_wbb_team_roster()`. If `raw=True`,
  returns the raw response dict.
* **Raises:**
  * [**sportsdataverse.errors.NoESPNDataError**](sportsdataverse.md#sportsdataverse.errors.NoESPNDataError) – ESPN returned 404.
  * **requests.exceptions.RequestException** – Other network failures after retries.

### Example

Las Vegas Aces (team_id 17) current roster:

```default
from sportsdataverse.wnba import espn_wnba_team_roster
roster = espn_wnba_team_roster(team_id=17, season=2024)
print(roster.shape)
roster.select(["athlete_id", "full_name", "jersey", "position_abbreviation"]).head()
```

Pandas round-trip — useful for one-off notebook work:

```default
roster_pd = espn_wnba_team_roster(team_id=17, season=2024, return_as_pandas=True)
roster_pd[["full_name", "jersey", "position_abbreviation", "height"]].head()
```

Inspect the raw ESPN payload:

```default
payload = espn_wnba_team_roster(team_id=17, season=2024, raw=True)
list(payload.keys())[:8]
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

## sportsdataverse.wnba.wnba_team_stats module

ESPN WNBA team season-stats scraper.

Mirror of `sportsdataverse.wbb.espn_wbb_team_stats()` for the WNBA
league slug. The actual fetch + parse logic lives in
`sportsdataverse.wbb.wbb_team_stats._espn_basketball_team_stats` to keep
the wbb / wnba pair DRY.

### sportsdataverse.wnba.wnba_team_stats.espn_wnba_team_stats(team_id: int, season: int, *, raw: Literal[True], return_as_pandas: bool = False, \*\*kwargs: Any) → dict[str, Any]

### sportsdataverse.wnba.wnba_team_stats.espn_wnba_team_stats(team_id: int, season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[True], \*\*kwargs: Any) → dict[str, DataFrame]

### sportsdataverse.wnba.wnba_team_stats.espn_wnba_team_stats(team_id: int, season: int, *, raw: Literal[False] = False, return_as_pandas: Literal[False] = False, \*\*kwargs: Any) → dict[str, DataFrame]

Pull ESPN team season stats for a WNBA team.

See `sportsdataverse.wbb.espn_wbb_team_stats()` for full
documentation of the return shape, the canonical three category keys
(`"Averages"`, `"Totals"`, `"Misc"`), the per-category column
set, and the `"Other"` fallback bucket.

* **Parameters:**
  * **team_id** – ESPN WNBA team identifier (e.g. `17` for the Las Vegas
    Aces).
  * **season** – Season year, forwarded to ESPN as `?season=YYYY`.
  * **raw** – If True, returns the parsed JSON dict before any flattening.
  * **return_as_pandas** – If True, returns a dict of pandas DataFrames;
    otherwise polars.
  * **\*\*kwargs** – Forwarded to `sportsdataverse.dl_utils.download`.
* **Returns:**
  Dict with one DataFrame per stat category — see
  `sportsdataverse.wbb.espn_wbb_team_stats()` for the full
  column / key documentation. If `raw=True`, returns the raw
  response dict.
* **Raises:**
  * [**sportsdataverse.errors.NoESPNDataError**](sportsdataverse.md#sportsdataverse.errors.NoESPNDataError) – ESPN returned 404.
  * **requests.exceptions.RequestException** – Other network failures after
        retries.

### Example

Las Vegas Aces’ 2024 team stats — keyed by category:

```default
from sportsdataverse.wnba import espn_wnba_team_stats
frames = espn_wnba_team_stats(team_id=17, season=2024)
sorted(frames.keys())  # 'Averages', 'Totals', 'Misc' (plus optional 'Other')
frames["Averages"].head()
```

Compare per-game and totals at a glance:

```default
avgs = frames["Averages"]
totals = frames["Totals"]
print(avgs.shape, totals.shape)
avgs.select(["games_played", "points_per_game", "rebounds_per_game"])
```

Pandas round-trip:

```default
frames_pd = espn_wnba_team_stats(team_id=17, season=2024, return_as_pandas=True)
frames_pd["Misc"].head()
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

## sportsdataverse.wnba.wnba_teams module

### sportsdataverse.wnba.wnba_teams.espn_wnba_teams(return_as_pandas=False, \*\*kwargs) → DataFrame

espn_wnba_teams - look up WNBA teams

* **Parameters:**
  **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing teams for the requested league.
  This function caches by default, so if you want to refresh the data, use the command
  sportsdataverse.wnba.espn_wnba_teams.clear_cache().
* **Return type:**
  pl.DataFrame

### Example

Pull the full WNBA team directory:

```default
from sportsdataverse.wnba import espn_wnba_teams
teams = espn_wnba_teams()
print(teams.shape)
teams.select(["team_id", "team_abbreviation", "team_display_name"]).head()
```

Find Las Vegas Aces (team_id 17):

```default
teams.filter(__import__("polars").col("team_id") == "17").to_dicts()
```

Refresh the cache (the call is `lru_cache`’d):

```default
espn_wnba_teams.cache_clear()  # cached at function-level
teams_pd = espn_wnba_teams(return_as_pandas=True)
```

See Also:
: * [wehoop](https://wehoop.sportsdataverse.org) — R sister package; mirrors this surface
  * [nba_api](https://github.com/swar/nba_api) — alternative Python source for NBA/WNBA stats endpoints
  * [hoopR](https://hoopR.sportsdataverse.org) — companion R package for men’s basketball

## Module contents
