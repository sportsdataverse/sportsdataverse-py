<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [sportsdataverse.nfl package](#sportsdataversenfl-package)
  - [Submodules](#submodules)
  - [sportsdataverse.nfl.cache module](#sportsdataversenflcache-module)
    - [sportsdataverse.nfl.cache.cached_loader(func: F) → F](#sportsdataversenflcachecached_loaderfunc-f-%E2%86%92-f)
    - [Example](#example)
    - [sportsdataverse.nfl.cache.clear_cache() → None](#sportsdataversenflcacheclear_cache-%E2%86%92-none)
    - [Example](#example-1)
  - [sportsdataverse.nfl.config module](#sportsdataversenflconfig-module)
    - [*class* sportsdataverse.nfl.config.NflConfig(cache_mode: Literal['memory', 'filesystem', 'off'] = 'memory', cache_dir: Path | None = None, cache_duration: int = 86400, verbose: bool = True, timeout: int = 30, user_agent: str = 'sportsdataverse-py-nfl')](#class-sportsdataversenflconfignflconfigcache_mode-literalmemory-filesystem-off--memory-cache_dir-path--none--none-cache_duration-int--86400-verbose-bool--true-timeout-int--30-user_agent-str--sportsdataverse-py-nfl)
    - [Example](#example-2)
      - [\_\_init_\_(cache_mode: Literal['memory', 'filesystem', 'off'] = 'memory', cache_dir: Path | None = None, cache_duration: int = 86400, verbose: bool = True, timeout: int = 30, user_agent: str = 'sportsdataverse-py-nfl') → None](#%5C_%5C_init_%5C_cache_mode-literalmemory-filesystem-off--memory-cache_dir-path--none--none-cache_duration-int--86400-verbose-bool--true-timeout-int--30-user_agent-str--sportsdataverse-py-nfl-%E2%86%92-none)
      - [cache_dir *: Path | None* *= None*](#cache_dir--path--none--none)
      - [cache_duration *: int* *= 86400*](#cache_duration--int--86400)
      - [cache_mode *: Literal['memory', 'filesystem', 'off']* *= 'memory'*](#cache_mode--literalmemory-filesystem-off--memory)
      - [timeout *: int* *= 30*](#timeout--int--30)
      - [user_agent *: str* *= 'sportsdataverse-py-nfl'*](#user_agent--str--sportsdataverse-py-nfl)
      - [verbose *: bool* *= True*](#verbose--bool--true)
    - [sportsdataverse.nfl.config.get_config() → NflConfig](#sportsdataversenflconfigget_config-%E2%86%92-nflconfig)
    - [Example](#example-3)
    - [sportsdataverse.nfl.config.reset_config() → NflConfig](#sportsdataversenflconfigreset_config-%E2%86%92-nflconfig)
    - [Example](#example-4)
    - [sportsdataverse.nfl.config.update_config(\*\*kwargs: object) → NflConfig](#sportsdataversenflconfigupdate_config%5C%5Ckwargs-object-%E2%86%92-nflconfig)
    - [Example](#example-5)
  - [sportsdataverse.nfl.datasets module](#sportsdataversenfldatasets-module)
    - [Example](#example-6)
      - [SEE ALSO](#see-also)
  - [sportsdataverse.nfl.model_vars module](#sportsdataversenflmodel_vars-module)
  - [sportsdataverse.nfl.nfl_game_rosters module](#sportsdataversenflnfl_game_rosters-module)
    - [sportsdataverse.nfl.nfl_game_rosters.espn_nfl_game_rosters(game_id: int, raw=False, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversenflnfl_game_rostersespn_nfl_game_rostersgame_id-int-rawfalse-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-7)
    - [sportsdataverse.nfl.nfl_game_rosters.helper_nfl_athlete_items(teams_rosters, \*\*kwargs)](#sportsdataversenflnfl_game_rostershelper_nfl_athlete_itemsteams_rosters-%5C%5Ckwargs)
    - [Example](#example-8)
    - [sportsdataverse.nfl.nfl_game_rosters.helper_nfl_game_items(summary)](#sportsdataversenflnfl_game_rostershelper_nfl_game_itemssummary)
    - [Example](#example-9)
    - [sportsdataverse.nfl.nfl_game_rosters.helper_nfl_roster_items(items, summary_url, \*\*kwargs)](#sportsdataversenflnfl_game_rostershelper_nfl_roster_itemsitems-summary_url-%5C%5Ckwargs)
    - [Example](#example-10)
    - [sportsdataverse.nfl.nfl_game_rosters.helper_nfl_team_items(items, \*\*kwargs)](#sportsdataversenflnfl_game_rostershelper_nfl_team_itemsitems-%5C%5Ckwargs)
    - [Example](#example-11)
  - [sportsdataverse.nfl.nfl_games module](#sportsdataversenflnfl_games-module)
    - [sportsdataverse.nfl.nfl_games.nfl_game_details(game_id=None, headers=None, raw=False) → Dict](#sportsdataversenflnfl_gamesnfl_game_detailsgame_idnone-headersnone-rawfalse-%E2%86%92-dict)
    - [Example](#example-12)
    - [sportsdataverse.nfl.nfl_games.nfl_game_schedule(season=2021, season_type='REG', week=1, headers=None, raw=False) → Dict](#sportsdataversenflnfl_gamesnfl_game_scheduleseason2021-season_typereg-week1-headersnone-rawfalse-%E2%86%92-dict)
    - [Example](#example-13)
    - [sportsdataverse.nfl.nfl_games.nfl_headers_gen()](#sportsdataversenflnfl_gamesnfl_headers_gen)
    - [Example](#example-14)
    - [sportsdataverse.nfl.nfl_games.nfl_token_gen()](#sportsdataversenflnfl_gamesnfl_token_gen)
    - [Example](#example-15)
  - [sportsdataverse.nfl.nfl_loaders module](#sportsdataversenflnfl_loaders-module)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_combine(return_as_pandas=False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_combinereturn_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-16)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_contracts(return_as_pandas=False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_contractsreturn_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-17)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_depth_charts(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_depth_chartsseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-18)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_draft_picks(return_as_pandas=False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_draft_picksreturn_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-19)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_ff_opportunity(seasons: List[int], stat_type: str = 'weekly', model_version: str = 'latest', return_as_pandas=False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_ff_opportunityseasons-listint-stat_type-str--weekly-model_version-str--latest-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-20)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_ff_playerids(return_as_pandas=False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_ff_playeridsreturn_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-21)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_ff_rankings(type: str = 'draft', kind: str = None, return_as_pandas=False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_ff_rankingstype-str--draft-kind-str--none-return_as_pandasfalse-%E2%86%92-dataframe)
      - [NOTE](#note)
    - [Example](#example-22)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_ftn_charting(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_ftn_chartingseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-23)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_injuries(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_injuriesseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-24)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_nextgen_stats(seasons: List[int], stat_type: str = 'passing', return_as_pandas: bool = False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_nextgen_statsseasons-listint-stat_type-str--passing-return_as_pandas-bool--false-%E2%86%92-dataframe)
    - [Example](#example-25)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_ngs_passing(seasons: List[int] = None, return_as_pandas: bool = False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_ngs_passingseasons-listint--none-return_as_pandas-bool--false-%E2%86%92-dataframe)
    - [Example](#example-26)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_ngs_receiving(seasons: List[int] = None, return_as_pandas: bool = False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_ngs_receivingseasons-listint--none-return_as_pandas-bool--false-%E2%86%92-dataframe)
    - [Example](#example-27)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_ngs_rushing(seasons: List[int] = None, return_as_pandas: bool = False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_ngs_rushingseasons-listint--none-return_as_pandas-bool--false-%E2%86%92-dataframe)
    - [Example](#example-28)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_officials(return_as_pandas=False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_officialsreturn_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-29)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_pbp(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_pbpseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-30)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_pbp_participation(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_pbp_participationseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-31)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_advstats(seasons: List[int], stat_type: str = 'pass', summary_level: str = 'week', return_as_pandas: bool = False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_pfr_advstatsseasons-listint-stat_type-str--pass-summary_level-str--week-return_as_pandas-bool--false-%E2%86%92-dataframe)
    - [Example](#example-32)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_def(return_as_pandas: bool = False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_pfr_defreturn_as_pandas-bool--false-%E2%86%92-dataframe)
    - [Example](#example-33)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_pass(return_as_pandas: bool = False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_pfr_passreturn_as_pandas-bool--false-%E2%86%92-dataframe)
    - [Example](#example-34)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_rec(return_as_pandas: bool = False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_pfr_recreturn_as_pandas-bool--false-%E2%86%92-dataframe)
    - [Example](#example-35)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_rush(return_as_pandas: bool = False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_pfr_rushreturn_as_pandas-bool--false-%E2%86%92-dataframe)
    - [Example](#example-36)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_weekly_def(seasons: List[int], return_as_pandas: bool = False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_pfr_weekly_defseasons-listint-return_as_pandas-bool--false-%E2%86%92-dataframe)
    - [Example](#example-37)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_weekly_pass(seasons: List[int], return_as_pandas: bool = False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_pfr_weekly_passseasons-listint-return_as_pandas-bool--false-%E2%86%92-dataframe)
    - [Example](#example-38)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_weekly_rec(seasons: List[int], return_as_pandas: bool = False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_pfr_weekly_recseasons-listint-return_as_pandas-bool--false-%E2%86%92-dataframe)
    - [Example](#example-39)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_weekly_rush(seasons: List[int], return_as_pandas: bool = False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_pfr_weekly_rushseasons-listint-return_as_pandas-bool--false-%E2%86%92-dataframe)
    - [Example](#example-40)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_player_stats(kicking=False, return_as_pandas=False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_player_statskickingfalse-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-41)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_players(return_as_pandas=False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_playersreturn_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-42)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_rosters(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_rostersseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-43)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_schedule(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_scheduleseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-44)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_snap_counts(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_snap_countsseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-45)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_team_stats(seasons: List[int], summary_level: str = 'week', return_as_pandas=False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_team_statsseasons-listint-summary_level-str--week-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-46)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_teams(return_as_pandas=False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_teamsreturn_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-47)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_trades(return_as_pandas=False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_tradesreturn_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-48)
    - [sportsdataverse.nfl.nfl_loaders.load_nfl_weekly_rosters(seasons: List[int], return_as_pandas=False) → DataFrame](#sportsdataversenflnfl_loadersload_nfl_weekly_rostersseasons-listint-return_as_pandasfalse-%E2%86%92-dataframe)
    - [Example](#example-49)
  - [sportsdataverse.nfl.nfl_pbp module](#sportsdataversenflnfl_pbp-module)
    - [*class* sportsdataverse.nfl.nfl_pbp.NFLPlayProcess(gameId=0, raw=False, path_to_json='/', return_keys=None, \*\*kwargs)](#class-sportsdataversenflnfl_pbpnflplayprocessgameid0-rawfalse-path_to_json-return_keysnone-%5C%5Ckwargs)
    - [Example](#example-50)
      - [\_\_helper_\_espn_nfl_odds_information_\_()](#%5C_%5C_helper_%5C_espn_nfl_odds_information_%5C_)
      - [\_\_init_\_(gameId=0, raw=False, path_to_json='/', return_keys=None, \*\*kwargs)](#%5C_%5C_init_%5C_gameid0-rawfalse-path_to_json-return_keysnone-%5C%5Ckwargs)
      - [corrupt_pbp_check()](#corrupt_pbp_check)
    - [Example](#example-51)
      - [create_box_score(play_df)](#create_box_scoreplay_df)
    - [Example](#example-52)
      - [espn_nfl_pbp(\*\*kwargs)](#espn_nfl_pbp%5C%5Ckwargs)
    - [Example](#example-53)
      - [gameId *= 0*](#gameid--0)
      - [nfl_pbp_disk()](#nfl_pbp_disk)
    - [Example](#example-54)
      - [nfl_pbp_json(\*\*kwargs)](#nfl_pbp_json%5C%5Ckwargs)
    - [Example](#example-55)
      - [path_to_json *= '/'*](#path_to_json--)
      - [ran_cleaning_pipeline *= False*](#ran_cleaning_pipeline--false)
      - [ran_pipeline *= False*](#ran_pipeline--false)
      - [raw *= False*](#raw--false)
      - [return_keys *= None*](#return_keys--none)
      - [run_cleaning_pipeline()](#run_cleaning_pipeline)
    - [Example](#example-56)
      - [run_processing_pipeline()](#run_processing_pipeline)
    - [Example](#example-57)
  - [sportsdataverse.nfl.nfl_schedule module](#sportsdataversenflnfl_schedule-module)
    - [sportsdataverse.nfl.nfl_schedule.espn_nfl_calendar(season=None, ondays=None, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversenflnfl_scheduleespn_nfl_calendarseasonnone-ondaysnone-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-58)
    - [sportsdataverse.nfl.nfl_schedule.espn_nfl_schedule(dates=None, week=None, season_type=None, groups=None, limit=500, return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversenflnfl_scheduleespn_nfl_scheduledatesnone-weeknone-season_typenone-groupsnone-limit500-return_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-59)
    - [sportsdataverse.nfl.nfl_schedule.get_current_week()](#sportsdataversenflnfl_scheduleget_current_week)
    - [Example](#example-60)
    - [sportsdataverse.nfl.nfl_schedule.most_recent_nfl_season()](#sportsdataversenflnfl_schedulemost_recent_nfl_season)
    - [Example](#example-61)
    - [sportsdataverse.nfl.nfl_schedule.scoreboard_event_parsing(event)](#sportsdataversenflnfl_schedulescoreboard_event_parsingevent)
    - [Example](#example-62)
  - [sportsdataverse.nfl.nfl_teams module](#sportsdataversenflnfl_teams-module)
    - [sportsdataverse.nfl.nfl_teams.espn_nfl_teams(return_as_pandas=False, \*\*kwargs) → DataFrame](#sportsdataversenflnfl_teamsespn_nfl_teamsreturn_as_pandasfalse-%5C%5Ckwargs-%E2%86%92-dataframe)
    - [Example](#example-63)
  - [sportsdataverse.nfl.utils_date module](#sportsdataversenflutils_date-module)
    - [sportsdataverse.nfl.utils_date.get_current_nfl_season(roster: bool = False) → int](#sportsdataversenflutils_dateget_current_nfl_seasonroster-bool--false-%E2%86%92-int)
    - [Example](#example-64)
    - [sportsdataverse.nfl.utils_date.get_current_nfl_week(use_date: bool = True, roster: bool = False) → int](#sportsdataversenflutils_dateget_current_nfl_weekuse_date-bool--true-roster-bool--false-%E2%86%92-int)
    - [Example](#example-65)
    - [sportsdataverse.nfl.utils_date.most_recent_nfl_season(roster: bool = False) → int](#sportsdataversenflutils_datemost_recent_nfl_seasonroster-bool--false-%E2%86%92-int)
    - [Example](#example-66)
  - [Module contents](#module-contents)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# sportsdataverse.nfl package

## Submodules

## sportsdataverse.nfl.cache module

Cache backend for sdv-py NFL loaders.

Three modes:

- `memory`: per-process dict, never invalidated within a process unless
  `clear_cache()` is called or the duration TTL expires.
- `filesystem`: persisted to disk under `config.cache_dir` as parquet
  files. Cross-process / cross-session reuse.
- `off`: no caching; every call hits the network.

The cache is wired into loaders via the `@cached_loader` decorator. The
decorator is intentionally minimal so it can be lifted to a cross-sport
`sportsdataverse.cache` module later without churn — no NFL-specific
assumptions live in here.

Cache key is derived from `(qualified_name, args, sorted_kwargs)` and
hashed with sha256. The key deliberately excludes `return_as_pandas` so
a memory or disk hit serves both polars and pandas callers from a single
stored polars frame.

### sportsdataverse.nfl.cache.cached_loader(func: F) → F

Decorator that adds caching to a `load_nfl_*` function.

Honors the active `NflConfig.cache_mode`:

- `memory`: dict-based per-process cache.
- `filesystem`: parquet-based cross-process cache under `cache_dir`.
- `off`: no caching, function runs every time.

The cache key is the hash of `(qualified_name, args, kwargs)` with
`return_as_pandas` excluded so memory / disk hits work regardless of
which return shape the caller asked for. The cache always stores the
polars frame internally and converts to pandas on read when requested.

### Example

Decorate a custom loader:

```default
import polars as pl
from sportsdataverse.nfl.cache import cached_loader

@cached_loader
def load_my_thing(season: int, return_as_pandas: bool = False):
    # ... fetch parquet, build a polars frame ...
    return pl.DataFrame({"season": [season]})

df1 = load_my_thing(2024)            # network hit, populates cache
df2 = load_my_thing(2024)            # served from cache
df_pd = load_my_thing(2024, return_as_pandas=True)
# `return_as_pandas` is excluded from the cache key, so the
# polars hit is reused and converted to pandas on the way out.
```

Switch caching modes at runtime:

```default
from sportsdataverse.nfl import clear_cache, update_config

update_config(cache_mode="filesystem")  # parquet-on-disk reuse
df3 = load_my_thing(2024)               # writes parquet under cache_dir
clear_cache()                           # wipe both memory + filesystem
update_config(cache_mode="off")         # bypass cache entirely
```

See Also:
: * `functools.lru_cache()` – standard-library alternative.
    `cached_loader` exists separately to add a TTL
    (`cache_duration`), a filesystem persistence mode, and a
    polars/pandas return-type round-trip on top of the same
    `(qualname, args, sorted_kwargs)` key shape. See
    [https://docs.python.org/3/library/functools.html#functools.lru_cache](https://docs.python.org/3/library/functools.html#functools.lru_cache).

### sportsdataverse.nfl.cache.clear_cache() → None

Clear both memory and filesystem caches.

Memory: empties the in-process dict.
Filesystem: removes all entries under `config.cache_dir`. The
directory itself is preserved so subsequent writes succeed without
needing `mkdir`.

### Example

Force a fresh fetch after upstream changes:

```default
from sportsdataverse.nfl import clear_cache, load_nfl_pbp
clear_cache()
pbp = load_nfl_pbp(seasons=[2024])
```

Pair with a cache-mode switch:

```default
from sportsdataverse.nfl import clear_cache, update_config
update_config(cache_mode="filesystem")
# ... lots of cached calls accumulate parquet files on disk ...
clear_cache()  # wipe disk + memory together
```

See Also:
: * `sportsdataverse.nfl.update_config()` – toggle cache mode/duration.
  * [`sportsdataverse.nfl.cache.cached_loader()`](#sportsdataverse.nfl.cache.cached_loader) – decorator that reads/writes the cache.

## sportsdataverse.nfl.config module

sdv-py NFL configuration.

Mirrors nflreadpy’s config surface so users coming from nflreadpy have a
near-drop-in replacement. Env-var prefix is `SDV_PY_NFL_*` (NFL-scoped
for now; future lift to `SDV_PY_*` cross-sport is intentional and will
be additive — old vars will keep working).

Environment variables (all optional):

- `SDV_PY_NFL_CACHE`        — cache mode (`memory`, `filesystem`, `off`)
- `SDV_PY_NFL_CACHE_DIR`    — directory for filesystem cache
- `SDV_PY_NFL_CACHE_DURATION` — cache duration in seconds (int)
- `SDV_PY_NFL_VERBOSE`      — verbose output (1/0, true/false, yes/no)
- `SDV_PY_NFL_TIMEOUT`      — HTTP timeout in seconds (int)
- `SDV_PY_NFL_USER_AGENT`   — custom user-agent string

Programmatic example:

```default
from sportsdataverse.nfl import update_config, get_config
update_config(cache_mode="filesystem", cache_duration=3600)
config = get_config()
```

### *class* sportsdataverse.nfl.config.NflConfig(cache_mode: Literal['memory', 'filesystem', 'off'] = 'memory', cache_dir: Path | None = None, cache_duration: int = 86400, verbose: bool = True, timeout: int = 30, user_agent: str = 'sportsdataverse-py-nfl')

Bases: `object`

Runtime configuration for sdv-py NFL loaders.

Fields mirror nflreadpy’s `NflreadpyConfig` so users can swap engines
without changing call sites. The defaults are conservative: in-memory
caching with a 24-hour TTL, verbose progress bars on, 30-second
HTTP timeout.

### Example

Inspect defaults via `get_config()`:

```default
from sportsdataverse.nfl import get_config
cfg = get_config()  # NflConfig instance
cfg.cache_mode      # "memory"
cfg.cache_duration  # 86400 (24h)
cfg.timeout         # 30 (seconds)
```

Construct a fresh instance directly (rarely needed – prefer
`update_config`):

```default
from sportsdataverse.nfl import NflConfig
cfg = NflConfig(cache_mode="off", timeout=10)
```

#### \_\_init_\_(cache_mode: Literal['memory', 'filesystem', 'off'] = 'memory', cache_dir: Path | None = None, cache_duration: int = 86400, verbose: bool = True, timeout: int = 30, user_agent: str = 'sportsdataverse-py-nfl') → None

#### cache_dir *: Path | None* *= None*

#### cache_duration *: int* *= 86400*

#### cache_mode *: Literal['memory', 'filesystem', 'off']* *= 'memory'*

#### timeout *: int* *= 30*

#### user_agent *: str* *= 'sportsdataverse-py-nfl'*

#### verbose *: bool* *= True*

### sportsdataverse.nfl.config.get_config() → [NflConfig](#sportsdataverse.nfl.config.NflConfig)

Return the live `NflConfig` singleton.

The same object is returned on every call; mutate via `update_config`
rather than reassigning fields directly so future hooks (e.g. logging
on config change) have a single choke point.

### Example

Inspect the active config:

```default
from sportsdataverse.nfl import get_config
cfg = get_config()
print(cfg.cache_mode, cfg.cache_duration, cfg.cache_dir)
```

Pair with `update_config` to verify a change took effect:

```default
from sportsdataverse.nfl import update_config, get_config
update_config(cache_mode="off")
assert get_config().cache_mode == "off"
```

### sportsdataverse.nfl.config.reset_config() → [NflConfig](#sportsdataverse.nfl.config.NflConfig)

Reset the active config to its env-var-derived defaults.

Convenience for tests / interactive sessions that want to undo a chain
of `update_config()` calls without restarting the interpreter.

### Example

Restore defaults after a session of tweaks:

```default
from sportsdataverse.nfl import update_config, reset_config
update_config(cache_mode="off", timeout=5)
# ... do work ...
reset_config()  # back to env-derived defaults
```

### sportsdataverse.nfl.config.update_config(\*\*kwargs: object) → [NflConfig](#sportsdataverse.nfl.config.NflConfig)

Update the active config in place.

Pass keyword arguments matching `NflConfig` fields:

```default
update_config(cache_mode="filesystem", cache_duration=3600)
```

String values for `cache_dir` are coerced to `pathlib.Path` and
`~` is expanded for convenience.

* **Parameters:**
  **\*\*kwargs** – Field name → new value pairs.
* **Returns:**
  The (mutated) global config object, for chaining or inspection.
* **Raises:**
  **ValueError** – If a passed key does not correspond to an
      `NflConfig` field.

### Example

Switch to filesystem caching with a 1-hour TTL:

```default
from sportsdataverse.nfl import update_config
update_config(cache_mode="filesystem", cache_duration=3600)
```

Disable caching for development:

```default
update_config(cache_mode="off")
```

Point cache at a custom directory:

```default
update_config(cache_dir="~/sdv-cache")
```

See Also:
: * `sportsdataverse.nfl.reset_config()` – undo a chain of updates.
  * `sportsdataverse.nfl.clear_cache()` – wipe cached entries.

## sportsdataverse.nfl.datasets module

sdv-py NFL static datasets — nflreadpy parity exports.

Three module-level dicts shipped at import time:

- `team_abbr_mapping`: every historical team abbreviation -> current
  abbreviation (relocations FOLDED). Use when you want to canonicalize
  historical data to today’s franchise names — e.g. `"OAK" -> "LV"`,
  `"SD" -> "LAC"`, `"STL" -> "LA"`.
- `team_abbr_mapping_norelocate`: every historical team abbreviation ->
  itself, with typos / variant codes resolved. Use when you want to PRESERVE
  the historical franchise identity — `"OAK"` stays `"OAK"` rather than
  becoming `"LV"`, but variant casings / spellings still resolve.
- `player_name_mapping`: common name variants -> canonical name. Used to
  cross-reference players across sources where naming conventions differ.

The data is bundled inline (rather than read from a separate JSON file at
import time) so the dicts ship cleanly inside any wheel / sdist without
relying on the project’s `package_data` declaration. Total payload is
~12 KB across the three dicts, which is small enough that the inline form
costs nothing in import time and removes one class of packaging surprise.

Source of truth: nflreadpy’s `data/` parquet files
([https://github.com/nflverse/nflreadpy/tree/main/src/nflreadpy/data](https://github.com/nflverse/nflreadpy/tree/main/src/nflreadpy/data)).
The R-side equivalent is the `nflreadr` package, which exposes the same
mappings as polars DataFrames with two columns `name` / `value`.

To refresh from upstream:

> 1. Download the three parquets:
>    [https://raw.githubusercontent.com/nflverse/nflreadpy/main/src/nflreadpy/data/team_abbr_mapping.parquet](https://raw.githubusercontent.com/nflverse/nflreadpy/main/src/nflreadpy/data/team_abbr_mapping.parquet)
>    [https://raw.githubusercontent.com/nflverse/nflreadpy/main/src/nflreadpy/data/team_abbr_mapping_norelocate.parquet](https://raw.githubusercontent.com/nflverse/nflreadpy/main/src/nflreadpy/data/team_abbr_mapping_norelocate.parquet)
>    [https://raw.githubusercontent.com/nflverse/nflreadpy/main/src/nflreadpy/data/player_name_mapping.parquet](https://raw.githubusercontent.com/nflverse/nflreadpy/main/src/nflreadpy/data/player_name_mapping.parquet)
> 2. For each, `polars.read_parquet(...).unique(subset=['name'],
>    keep='first', maintain_order=True)` then build a
>    `dict(zip(name, value))`. The `unique(keep='first')` step
>    deduplicates the small number of full-team-name keys that appear
>    twice in the upstream `team_abbr_mapping_norelocate` parquet
>    (e.g. `"RAMS"` -> `"STL"` and `"RAMS"` -> `"LA"`); keeping
>    the first occurrence preserves the “norelocate” semantics by
>    picking the historical (older) abbreviation.
> 3. Replace the three literals below with the new dicts (sorted keys
>    for diff-friendliness).

The dicts are loaded eagerly at import time so they behave like
nflreadpy’s module-level exports.

### Example

Canonicalize a relocated franchise:

```default
from sportsdataverse.nfl import team_abbr_mapping
team_abbr_mapping["OAK"]  # -> "LV"
team_abbr_mapping["SD"]   # -> "LAC"
team_abbr_mapping["STL"]  # -> "LA"
```

Preserve historical identity (no relocation fold):

```default
from sportsdataverse.nfl import team_abbr_mapping_norelocate
team_abbr_mapping_norelocate["OAK"]  # -> "OAK"
```

Resolve a player-name variant:

```default
from sportsdataverse.nfl import player_name_mapping
player_name_mapping["Pat Mahomes"]  # -> canonical "Patrick Mahomes"
```

Use defensively in a polars pipeline:

```default
import polars as pl
from sportsdataverse.nfl import load_nfl_pbp, team_abbr_mapping

pbp = (
    load_nfl_pbp(seasons=[2024])
    .with_columns(
        home_team=pl.col("home_team").map_elements(
            lambda t: team_abbr_mapping.get(t, t), return_dtype=pl.Utf8
        )
    )
)
```

#### SEE ALSO
* [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)
* [nflreadpy](https://github.com/nflverse/nflreadpy) – direct nflverse Python bindings (mirrors these dicts)

## sportsdataverse.nfl.model_vars module

## sportsdataverse.nfl.nfl_game_rosters module

### sportsdataverse.nfl.nfl_game_rosters.espn_nfl_game_rosters(game_id: int, raw=False, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_nfl_game_rosters() - Pull the game by id.

* **Parameters:**
  * **game_id** (*int*) – Unique game_id, can be obtained from espn_nfl_schedule().
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
from sportsdataverse.nfl import espn_nfl_game_rosters
rosters = espn_nfl_game_rosters(game_id=401220403)
rosters.shape
```

Pandas round-trip with home/away split:

```default
rosters_pd = espn_nfl_game_rosters(game_id=401220403, return_as_pandas=True)
home = rosters_pd[rosters_pd["home_away"] == "home"]
away = rosters_pd[rosters_pd["home_away"] == "away"]
```

### sportsdataverse.nfl.nfl_game_rosters.helper_nfl_athlete_items(teams_rosters, \*\*kwargs)

Internal helper – expand each `athlete_href` from the team
rosters into a tidy per-player DataFrame (names, dob, headshot, etc.).

Used by [`espn_nfl_game_rosters()`](#sportsdataverse.nfl.nfl_game_rosters.espn_nfl_game_rosters).

### Example

Wired into the public flow (preferred entry point):

```default
from sportsdataverse.nfl import espn_nfl_game_rosters
rosters = espn_nfl_game_rosters(game_id=401220403)
```

### sportsdataverse.nfl.nfl_game_rosters.helper_nfl_game_items(summary)

Internal helper – normalize the `items` array from the ESPN
`/competitors` endpoint into a tidy team-level DataFrame.

Used by [`espn_nfl_game_rosters()`](#sportsdataverse.nfl.nfl_game_rosters.espn_nfl_game_rosters). Exposed for advanced callers
who already have the JSON payload and want to skip the network call.

### Example

Reuse against a cached payload:

```default
from sportsdataverse.dl_utils import download
from sportsdataverse.nfl.nfl_game_rosters import helper_nfl_game_items
url = (
    "https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/"
    "events/401220403/competitions/401220403/competitors"
)
items = helper_nfl_game_items(download(url).json())
items.columns
```

### sportsdataverse.nfl.nfl_game_rosters.helper_nfl_roster_items(items, summary_url, \*\*kwargs)

Internal helper – pull each team’s roster from ESPN and return a
combined per-game roster DataFrame.

Used by [`espn_nfl_game_rosters()`](#sportsdataverse.nfl.nfl_game_rosters.espn_nfl_game_rosters).

### Example

Wired into the public flow (preferred entry point):

```default
from sportsdataverse.nfl import espn_nfl_game_rosters
rosters = espn_nfl_game_rosters(game_id=401220403)
```

### sportsdataverse.nfl.nfl_game_rosters.helper_nfl_team_items(items, \*\*kwargs)

Internal helper – fan out from `items.team_href` to the per-team
ESPN payloads and return a tidy team-detail DataFrame (logos,
colors, slugs).

Used by [`espn_nfl_game_rosters()`](#sportsdataverse.nfl.nfl_game_rosters.espn_nfl_game_rosters).

### Example

Pair with [`helper_nfl_game_items()`](#sportsdataverse.nfl.nfl_game_rosters.helper_nfl_game_items) for an offline-style flow:

```default
from sportsdataverse.nfl.nfl_game_rosters import (
    helper_nfl_game_items, helper_nfl_team_items,
)
# items = helper_nfl_game_items(...)
# teams = helper_nfl_team_items(items)
```

## sportsdataverse.nfl.nfl_games module

### sportsdataverse.nfl.nfl_games.nfl_game_details(game_id=None, headers=None, raw=False) → Dict

nfl_game_details() – pull full `api.nfl.com` game details by game id.

* **Parameters:**
  * **game_id** (*str*) – UUID-style game id from `api.nfl.com` (e.g. `'7ae87c4c-d24c-11ec-b23d-d15a91047884'`).
  * **headers** (*Dict* *[**str* *,* *str* *]*  *|* *None*) – Pre-built header dict (skip the auth roundtrip).
    Defaults to a fresh `nfl_headers_gen()` call.
  * **raw** (*bool*) – If True, return the ESPN payload untouched. If False (default),
    normalize keys to the expected schema (filling missing keys with
    empty dicts/lists).
* **Returns:**
  Dictionary of game details (drives, plays, scoring summaries,
  timeouts, weather, attendance, etc.).
* **Return type:**
  Dict

### Example

Quick start:

```default
from sportsdataverse.nfl.nfl_games import nfl_game_details
details = nfl_game_details(game_id="7ae87c4c-d24c-11ec-b23d-d15a91047884")
sorted(details.keys())[:5]
```

Reuse headers across many calls (avoids re-minting tokens):

```default
from sportsdataverse.nfl.nfl_games import nfl_game_details, nfl_headers_gen
hdrs = nfl_headers_gen()
details = nfl_game_details(
    game_id="7ae87c4c-d24c-11ec-b23d-d15a91047884", headers=hdrs
)
```

Raw passthrough:

```default
raw = nfl_game_details(
    game_id="7ae87c4c-d24c-11ec-b23d-d15a91047884", raw=True
)
```

### sportsdataverse.nfl.nfl_games.nfl_game_schedule(season=2021, season_type='REG', week=1, headers=None, raw=False) → Dict

nfl_game_schedule() – list `api.nfl.com` games for a season/week slice.

* **Parameters:**
  * **season** (*int*) – season year (e.g. `2024`).
  * **season_type** (*str*) – season type. One of `"REG"` or `"POST"`.
  * **week** (*int*) – week number (1-18 regular season, 1-4 post-season).
  * **headers** (*Dict* *[**str* *,* *str* *]*  *|* *None*) – Pre-built header dict.
    Defaults to a fresh `nfl_headers_gen()` call.
  * **raw** (*bool*) – Currently ignored – the function always returns the
    raw NFL.com summary payload.
* **Returns:**
  Dictionary with the games list under `"games"` plus
  pagination metadata.
* **Return type:**
  Dict

### Example

Week 1 of the 2024 regular season:

```default
from sportsdataverse.nfl.nfl_games import nfl_game_schedule
week_one = nfl_game_schedule(season=2024, season_type="REG", week=1)
```

Wild Card weekend (post-season):

```default
wild_card = nfl_game_schedule(season=2023, season_type="POST", week=1)
```

Reuse headers across many calls:

```default
from sportsdataverse.nfl.nfl_games import nfl_game_schedule, nfl_headers_gen
hdrs = nfl_headers_gen()
for week in range(1, 19):
    summary = nfl_game_schedule(
        season=2024, season_type="REG", week=week, headers=hdrs,
    )
```

### sportsdataverse.nfl.nfl_games.nfl_headers_gen()

Build the full request-header dict expected by `api.nfl.com`.

Mints a fresh bearer token via [`nfl_token_gen()`](#sportsdataverse.nfl.nfl_games.nfl_token_gen) and combines it
with the browser-style headers (`Origin`, `Referer`, `User-Agent`,
`Sec-Fetch-*`, etc.) the NFL.com web app sends on every request.

* **Returns:**
  Header dict ready to drop into `requests.get`.
* **Return type:**
  Dict[str, str]

### Example

Reuse one header set across many calls:

```default
from sportsdataverse.nfl.nfl_games import (
    nfl_headers_gen, nfl_game_schedule,
)
hdrs = nfl_headers_gen()
week_one = nfl_game_schedule(season=2024, season_type="REG", week=1, headers=hdrs)
week_two = nfl_game_schedule(season=2024, season_type="REG", week=2, headers=hdrs)
```

### sportsdataverse.nfl.nfl_games.nfl_token_gen()

Mint a fresh `api.nfl.com` access token via the public reroute endpoint.

Wraps the unauthenticated `client_credentials` grant the NFL.com web
app uses. The returned bearer token is what `nfl_headers_gen()` puts
on the `Authorization` header.

* **Returns:**
  The access token string.
* **Return type:**
  str

### Example

Mint a token and inspect its prefix:

```default
from sportsdataverse.nfl.nfl_games import nfl_token_gen
token = nfl_token_gen()
assert isinstance(token, str)
```

Pair with a downstream call (`nfl_headers_gen` does this for you):

```default
import requests
token = nfl_token_gen()
headers = {"Authorization": f"Bearer {token}"}
```

## sportsdataverse.nfl.nfl_loaders module

### sportsdataverse.nfl.nfl_loaders.load_nfl_combine(return_as_pandas=False) → DataFrame

Load NFL Combine information

* **Parameters:**
  **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing NFL combine data available.
* **Return type:**
  pl.DataFrame

### Example

Quick start:

```default
from sportsdataverse.nfl import load_nfl_combine
combine = load_nfl_combine()
combine.shape
```

Filter by draft year and position:

```default
import polars as pl
qbs_2024 = (
    load_nfl_combine()
    .filter((pl.col("season") == 2024) & (pl.col("pos") == "QB"))
)
```

See Also:
: * [Pro Football Reference](https://www.pro-football-reference.com) – upstream combine source
  * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)

### sportsdataverse.nfl.nfl_loaders.load_nfl_contracts(return_as_pandas=False) → DataFrame

Load NFL Historical contracts information

* **Parameters:**
  **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing historical contracts available.
* **Return type:**
  pl.DataFrame

### Example

Quick start:

```default
from sportsdataverse.nfl import load_nfl_contracts
contracts = load_nfl_contracts()
contracts.shape
```

Pandas round-trip with sort by APY:

```default
contracts_pd = load_nfl_contracts(return_as_pandas=True)
contracts_pd.sort_values("apy", ascending=False).head()
```

See Also:
: * [Over The Cap](https://overthecap.com) – upstream contracts source
  * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)

### sportsdataverse.nfl.nfl_loaders.load_nfl_depth_charts(seasons: List[int], return_as_pandas=False) → DataFrame

Load NFL Depth Chart data for selected seasons

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2001 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing depth chart data available for the requested seasons.
* **Return type:**
  pl.DataFrame

### Example

Single season:

```default
from sportsdataverse.nfl import load_nfl_depth_charts
depth = load_nfl_depth_charts(seasons=[2024])
```

Multi-season range:

```default
depth = load_nfl_depth_charts(seasons=range(2020, 2025))
```

See Also:
: * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)
  * [nflreadpy](https://github.com/nflverse/nflreadpy) – direct nflverse Python bindings

### sportsdataverse.nfl.nfl_loaders.load_nfl_draft_picks(return_as_pandas=False) → DataFrame

Load NFL Draft picks information

* **Parameters:**
  **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing NFL Draft picks data available.
* **Return type:**
  pl.DataFrame

### Example

Quick start:

```default
from sportsdataverse.nfl import load_nfl_draft_picks
picks = load_nfl_draft_picks()
picks.shape
```

Filter to a single year and round:

```default
import polars as pl
r1_2024 = (
    load_nfl_draft_picks()
    .filter((pl.col("season") == 2024) & (pl.col("round") == 1))
)
```

See Also:
: * [Pro Football Reference](https://www.pro-football-reference.com) – upstream draft source
  * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)

### sportsdataverse.nfl.nfl_loaders.load_nfl_ff_opportunity(seasons: List[int], stat_type: str = 'weekly', model_version: str = 'latest', return_as_pandas=False) → DataFrame

Load NFL fantasy football opportunity data from ffverse/ffopportunity

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2006 is the earliest available season.
  * **stat_type** (*str*) – One of “weekly”, “pbp_pass”, “pbp_rush”. Defaults to “weekly”.
  * **model_version** (*str*) – One of “latest”, “v1.0.0”. Defaults to “latest”.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing fantasy football opportunity data
  : for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2006, or if stat_type / model_version
      are not allowed values.

### Example

Weekly opportunity stats (default):

```default
from sportsdataverse.nfl import load_nfl_ff_opportunity
weekly = load_nfl_ff_opportunity(seasons=[2024])
```

Pass play-by-play opportunity stats:

```default
pbp_pass = load_nfl_ff_opportunity(seasons=[2024], stat_type="pbp_pass")
```

Rush play-by-play opportunity stats with pinned model version:

```default
pbp_rush = load_nfl_ff_opportunity(
    seasons=[2024], stat_type="pbp_rush", model_version="v1.0.0"
)
```

See Also:
: * [ffopportunity](https://github.com/ffverse/ffopportunity) – upstream opportunity model
  * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)

### sportsdataverse.nfl.nfl_loaders.load_nfl_ff_playerids(return_as_pandas=False) → DataFrame

Load fantasy football player IDs from DynastyProcess.com

* **Parameters:**
  **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing fantasy football player ID mappings across platforms.
* **Return type:**
  pl.DataFrame

### Example

Quick start:

```default
from sportsdataverse.nfl import load_nfl_ff_playerids
ids = load_nfl_ff_playerids()
ids.shape
```

Filter to active QBs:

```default
import polars as pl
qbs = (
    load_nfl_ff_playerids()
    .filter((pl.col("position") == "QB") & (pl.col("status") == "ACT"))
)
```

See Also:
: * [DynastyProcess](https://github.com/dynastyprocess) – upstream ID-mapping project
  * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)

### sportsdataverse.nfl.nfl_loaders.load_nfl_ff_rankings(type: str = 'draft', kind: str = None, return_as_pandas=False) → DataFrame

Load fantasy football rankings and projections

* **Parameters:**
  * **type** (*str*) – Type of rankings to load. One of `"draft"` (current draft
    rankings), `"week"` (weekly rankings), or `"all"` (full historical
    rankings). Defaults to `"draft"`. Kept for nflreadpy parity since
    its parameter is also called `type`; the forward-going preferred
    name is `kind`.
  * **kind** (*str*) – Preferred parameter name. Same semantics and allowed values
    as `type`. If both are supplied, `kind` wins. If neither is
    supplied, defaults to `"draft"` via `type`.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False,
    returns a polars dataframe.
* **Returns:**
  Polars dataframe containing fantasy football rankings data.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If the resolved value is not one of the allowed values.

#### NOTE
Available as the alias `sportsdataverse.nfl.load_ff_rankings` for
nflreadpy parity.

### Example

Preferred `kind=` parameter:

```default
from sportsdataverse.nfl import load_nfl_ff_rankings
draft = load_nfl_ff_rankings(kind="draft")
```

Weekly rankings:

```default
weekly = load_nfl_ff_rankings(kind="week")
```

Full historical rankings (parquet):

```default
history = load_nfl_ff_rankings(kind="all")
```

nflreadpy-parity `type=` parameter (still supported):

```default
draft = load_nfl_ff_rankings(type="draft")
```

See Also:
: * [DynastyProcess](https://github.com/dynastyprocess) – upstream rankings source
  * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)

### sportsdataverse.nfl.nfl_loaders.load_nfl_ftn_charting(seasons: List[int], return_as_pandas=False) → DataFrame

Load NFL FTN charting data going back to 2022

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2022 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing FTN charting data available for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2022.

### Example

Single season:

```default
from sportsdataverse.nfl import load_nfl_ftn_charting
charting = load_nfl_ftn_charting(seasons=[2024])
```

Multi-season range:

```default
charting = load_nfl_ftn_charting(seasons=range(2022, 2025))
```

Filter to plays with motion:

```default
import polars as pl
motion_plays = (
    load_nfl_ftn_charting(seasons=[2024])
    .filter(pl.col("is_motion") == 1)
)
```

See Also:
: * [FTN Network](https://ftnfantasy.com) – upstream charting source
  * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)

### sportsdataverse.nfl.nfl_loaders.load_nfl_injuries(seasons: List[int], return_as_pandas=False) → DataFrame

Load NFL injuries data for selected seasons

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2009 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing injuries data available for the requested seasons.
* **Return type:**
  pl.DataFrame

### Example

Single season:

```default
from sportsdataverse.nfl import load_nfl_injuries
injuries = load_nfl_injuries(seasons=[2024])
```

Multi-season range with team filter:

```default
import polars as pl
sf_injuries = (
    load_nfl_injuries(seasons=range(2020, 2025))
    .filter(pl.col("team") == "SF")
)
```

See Also:
: * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)
  * [nflreadpy](https://github.com/nflverse/nflreadpy) – direct nflverse Python bindings

### sportsdataverse.nfl.nfl_loaders.load_nfl_nextgen_stats(seasons: List[int], stat_type: str = 'passing', return_as_pandas: bool = False) → DataFrame

Load NFL NextGen Stats data going back to 2016.

Unified loader that consolidates the per-stat-type NextGen Stats
accessors. Mirrors the API surface of nflreadpy’s
`load_nextgen_stats` so downstream code can swap engines without
changing call sites.

* **Parameters:**
  * **seasons** (*list* *[**int* *]*) – Seasons to filter to. The upstream parquet
    covers a single combined file per stat type — `seasons` is
    applied as a post-filter on the `season` column.
  * **stat_type** (*str*) – One of `"passing"`, `"rushing"`,
    `"receiving"`. Defaults to `"passing"`.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe.
    If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing NextGen Stats data
  : for the requested `stat_type` and `seasons`.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If `stat_type` is not one of the allowed values.

### Example

Passing NextGen stats (default):

```default
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs_pass = load_nfl_nextgen_stats(seasons=[2024], stat_type="passing")
```

Rushing NextGen stats:

```default
ngs_rush = load_nfl_nextgen_stats(seasons=[2024], stat_type="rushing")
```

Receiving NextGen stats with a follow-up filter:

```default
import polars as pl
ngs_rec = (
    load_nfl_nextgen_stats(seasons=[2024], stat_type="receiving")
    .filter(pl.col("week") > 0)
)
```

Pandas round-trip:

```default
ngs_pd = load_nfl_nextgen_stats(
    seasons=[2024], stat_type="passing", return_as_pandas=True
)
```

See Also:
: * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)
  * [nflreadpy](https://github.com/nflverse/nflreadpy) – direct nflverse Python bindings

### sportsdataverse.nfl.nfl_loaders.load_nfl_ngs_passing(seasons: List[int] = None, return_as_pandas: bool = False) → DataFrame

Deprecated alias for `load_nfl_nextgen_stats(stat_type='passing')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_nextgen_stats` function.

### Example

Migrate to the unified entry point:

```default
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs = load_nfl_nextgen_stats(seasons=[2024], stat_type="passing")
```

### sportsdataverse.nfl.nfl_loaders.load_nfl_ngs_receiving(seasons: List[int] = None, return_as_pandas: bool = False) → DataFrame

Deprecated alias for `load_nfl_nextgen_stats(stat_type='receiving')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_nextgen_stats` function.

### Example

Migrate to the unified entry point:

```default
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs = load_nfl_nextgen_stats(seasons=[2024], stat_type="receiving")
```

### sportsdataverse.nfl.nfl_loaders.load_nfl_ngs_rushing(seasons: List[int] = None, return_as_pandas: bool = False) → DataFrame

Deprecated alias for `load_nfl_nextgen_stats(stat_type='rushing')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_nextgen_stats` function.

### Example

Migrate to the unified entry point:

```default
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs = load_nfl_nextgen_stats(seasons=[2024], stat_type="rushing")
```

### sportsdataverse.nfl.nfl_loaders.load_nfl_officials(return_as_pandas=False) → DataFrame

Load NFL Officials information

* **Parameters:**
  **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing officials available.
* **Return type:**
  pl.DataFrame

### Example

Quick start:

```default
from sportsdataverse.nfl import load_nfl_officials
officials = load_nfl_officials()
officials.shape
```

Pandas round-trip:

```default
officials_pd = load_nfl_officials(return_as_pandas=True)
officials_pd.head()
```

See Also:
: * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)
  * [nflreadpy](https://github.com/nflverse/nflreadpy) – direct nflverse Python bindings

### sportsdataverse.nfl.nfl_loaders.load_nfl_pbp(seasons: List[int], return_as_pandas=False) → DataFrame

Load NFL play by play data going back to 1999

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 1999 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the play-by-plays available for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 1999.

### Example

Quick start:

```default
from sportsdataverse.nfl import load_nfl_pbp
pbp = load_nfl_pbp(seasons=[2024])
print(pbp.shape)
```

Multi-season range:

```default
pbp = load_nfl_pbp(seasons=range(2020, 2025))
```

With cache off (development workflow):

```default
from sportsdataverse.nfl import load_nfl_pbp, update_config
update_config(cache_mode="off")
pbp = load_nfl_pbp(seasons=[2024])
```

Pandas round-trip:

```default
pbp_pd = load_nfl_pbp(seasons=[2024], return_as_pandas=True)
pbp_pd.head()
```

See Also:
: * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)
  * [nflreadpy](https://github.com/nflverse/nflreadpy) – direct nflverse Python bindings
  * [nflfastR](https://www.nflfastr.com) – R sister package for NFL PBP

### sportsdataverse.nfl.nfl_loaders.load_nfl_pbp_participation(seasons: List[int], return_as_pandas=False) → DataFrame

Load NFL play-by-play participation data for selected seasons

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2016 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing play-by-play participation data available for the requested seasons.
* **Return type:**
  pl.DataFrame

### Example

Single season:

```default
from sportsdataverse.nfl import load_nfl_pbp_participation
participation = load_nfl_pbp_participation(seasons=[2022])
```

Multi-season range:

```default
participation = load_nfl_pbp_participation(seasons=range(2018, 2023))
```

See Also:
: * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)
  * [nflreadpy](https://github.com/nflverse/nflreadpy) – direct nflverse Python bindings

### sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_advstats(seasons: List[int], stat_type: str = 'pass', summary_level: str = 'week', return_as_pandas: bool = False) → DataFrame

Load Pro-Football Reference advanced statistics going back to 2018.

Unified loader that consolidates the per-stat-type / per-summary-level
PFR advstats accessors. Mirrors the API surface of nflreadpy’s
`load_pfr_advstats` so downstream code can swap engines without
changing call sites.

* **Parameters:**
  * **seasons** (*list* *[**int* *]*) – Seasons to load. For `summary_level='week'`
    this drives the per-season parquet fan-out; for
    `summary_level='season'` it post-filters the combined
    parquet by the `season` column.
  * **stat_type** (*str*) – One of `"pass"`, `"rush"`, `"rec"`,
    `"def"`. Defaults to `"pass"`.
  * **summary_level** (*str*) – One of `"week"` or `"season"`. Defaults
    to `"week"`.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe.
    If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing PFR advanced stats
  : data for the requested `stat_type`, `summary_level`,
    and `seasons`.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If `stat_type` or `summary_level` are not allowed
      values, or if any season is less than 2018.

### Example

Weekly passing advanced stats (per-game splits):

```default
from sportsdataverse.nfl import load_nfl_pfr_advstats
pass_week = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="pass", summary_level="week"
)
```

Season-level rushing summaries (one row per player per season):

```default
rush_season = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rush", summary_level="season"
)
```

Defensive stats with a follow-up filter:

```default
import polars as pl
def_week = (
    load_nfl_pfr_advstats(seasons=[2024], stat_type="def", summary_level="week")
    .filter(pl.col("week") <= 8)
)
```

Pandas round-trip:

```default
rec_pd = load_nfl_pfr_advstats(
    seasons=[2024],
    stat_type="rec",
    summary_level="season",
    return_as_pandas=True,
)
```

See Also:
: * [Pro Football Reference](https://www.pro-football-reference.com) – upstream source for advanced stats
  * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)
  * [nflreadpy](https://github.com/nflverse/nflreadpy) – direct nflverse Python bindings

### sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_def(return_as_pandas: bool = False) → DataFrame

Deprecated alias for `load_nfl_pfr_advstats(stat_type='def', summary_level='season')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

### Example

Migrate to the unified entry point:

```default
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="def", summary_level="season"
)
```

### sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_pass(return_as_pandas: bool = False) → DataFrame

Deprecated alias for `load_nfl_pfr_advstats(stat_type='pass', summary_level='season')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

### Example

Migrate to the unified entry point:

```default
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="pass", summary_level="season"
)
```

### sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_rec(return_as_pandas: bool = False) → DataFrame

Deprecated alias for `load_nfl_pfr_advstats(stat_type='rec', summary_level='season')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

### Example

Migrate to the unified entry point:

```default
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rec", summary_level="season"
)
```

### sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_rush(return_as_pandas: bool = False) → DataFrame

Deprecated alias for `load_nfl_pfr_advstats(stat_type='rush', summary_level='season')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

### Example

Migrate to the unified entry point:

```default
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rush", summary_level="season"
)
```

### sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_weekly_def(seasons: List[int], return_as_pandas: bool = False) → DataFrame

Deprecated alias for `load_nfl_pfr_advstats(stat_type='def', summary_level='week')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

### Example

Migrate to the unified entry point:

```default
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="def", summary_level="week"
)
```

### sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_weekly_pass(seasons: List[int], return_as_pandas: bool = False) → DataFrame

Deprecated alias for `load_nfl_pfr_advstats(stat_type='pass', summary_level='week')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

### Example

Migrate to the unified entry point:

```default
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="pass", summary_level="week"
)
```

### sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_weekly_rec(seasons: List[int], return_as_pandas: bool = False) → DataFrame

Deprecated alias for `load_nfl_pfr_advstats(stat_type='rec', summary_level='week')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

### Example

Migrate to the unified entry point:

```default
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rec", summary_level="week"
)
```

### sportsdataverse.nfl.nfl_loaders.load_nfl_pfr_weekly_rush(seasons: List[int], return_as_pandas: bool = False) → DataFrame

Deprecated alias for `load_nfl_pfr_advstats(stat_type='rush', summary_level='week')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

### Example

Migrate to the unified entry point:

```default
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rush", summary_level="week"
)
```

### sportsdataverse.nfl.nfl_loaders.load_nfl_player_stats(kicking=False, return_as_pandas=False) → DataFrame

Load NFL player stats data

* **Parameters:**
  * **kicking** (*bool*) – If True, load kicking stats. If False, load all other stats.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing player stats.
* **Return type:**
  pl.DataFrame

### Example

Quick start (offense / defense / special teams):

```default
from sportsdataverse.nfl import load_nfl_player_stats
stats = load_nfl_player_stats()
stats.shape
```

Kicking-only stats:

```default
kicking = load_nfl_player_stats(kicking=True)
```

Filter to a single season after load:

```default
import polars as pl
stats_2024 = load_nfl_player_stats().filter(pl.col("season") == 2024)
```

See Also:
: * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)
  * [nflreadpy](https://github.com/nflverse/nflreadpy) – direct nflverse Python bindings

### sportsdataverse.nfl.nfl_loaders.load_nfl_players(return_as_pandas=False) → DataFrame

Load NFL Player ID information

* **Parameters:**
  **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing players available.
* **Return type:**
  pl.DataFrame

### Example

Quick start:

```default
from sportsdataverse.nfl import load_nfl_players
players = load_nfl_players()
players.shape
```

Pandas round-trip:

```default
players_pd = load_nfl_players(return_as_pandas=True)
players_pd.head()
```

See Also:
: * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)
  * [nflreadpy](https://github.com/nflverse/nflreadpy) – direct nflverse Python bindings

### sportsdataverse.nfl.nfl_loaders.load_nfl_rosters(seasons: List[int], return_as_pandas=False) → DataFrame

Load NFL roster data for all seasons

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 1920 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing rosters available for the requested seasons.
* **Return type:**
  pl.DataFrame

### Example

Single season:

```default
from sportsdataverse.nfl import load_nfl_rosters
rosters = load_nfl_rosters(seasons=[2024])
```

Multi-season range:

```default
rosters = load_nfl_rosters(seasons=range(2020, 2025))
```

Filter to a single team:

```default
import polars as pl
kc = load_nfl_rosters(seasons=[2024]).filter(pl.col("team") == "KC")
```

See Also:
: * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)
  * [nflreadpy](https://github.com/nflverse/nflreadpy) – direct nflverse Python bindings

### sportsdataverse.nfl.nfl_loaders.load_nfl_schedule(seasons: List[int], return_as_pandas=False) → DataFrame

Load NFL schedule data

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 1999 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the schedule for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 1999.

### Example

Single season:

```default
from sportsdataverse.nfl import load_nfl_schedule
schedule = load_nfl_schedule(seasons=[2024])
schedule.shape
```

Multi-season range:

```default
schedule = load_nfl_schedule(seasons=range(2020, 2025))
```

Filter to a single week:

```default
import polars as pl
week_one = load_nfl_schedule(seasons=[2024]).filter(pl.col("week") == 1)
```

Pandas round-trip:

```default
schedule_pd = load_nfl_schedule(seasons=[2024], return_as_pandas=True)
schedule_pd[["game_id", "home_team", "away_team", "week"]].head()
```

See Also:
: * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)
  * [nflreadpy](https://github.com/nflverse/nflreadpy) – direct nflverse Python bindings

### sportsdataverse.nfl.nfl_loaders.load_nfl_snap_counts(seasons: List[int], return_as_pandas=False) → DataFrame

Load NFL snap counts data for selected seasons

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2012 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing snap counts available for the requested seasons.
* **Return type:**
  pl.DataFrame

### Example

Single season:

```default
from sportsdataverse.nfl import load_nfl_snap_counts
snaps = load_nfl_snap_counts(seasons=[2024])
```

Multi-season range with offense-only filter:

```default
import polars as pl
offense = (
    load_nfl_snap_counts(seasons=range(2022, 2025))
    .filter(pl.col("offense_snaps") > 0)
)
```

See Also:
: * [Pro Football Reference](https://www.pro-football-reference.com) – upstream snap-count source
  * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)

### sportsdataverse.nfl.nfl_loaders.load_nfl_team_stats(seasons: List[int], summary_level: str = 'week', return_as_pandas=False) → DataFrame

Load NFL team stats data going back to 1999

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 1999 is the earliest available season.
  * **summary_level** (*str*) – Aggregation level. One of “week”, “reg”, “post”, “reg+post”. Defaults to “week”.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing team stats available for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 1999, or if summary_level is not one of the
      allowed values.

### Example

Weekly team stats (default):

```default
from sportsdataverse.nfl import load_nfl_team_stats
weekly = load_nfl_team_stats(seasons=[2024])
```

Regular-season-only team stats:

```default
reg = load_nfl_team_stats(seasons=[2024], summary_level="reg")
```

Combined regular + post-season at season grain:

```default
combined = load_nfl_team_stats(seasons=[2023, 2024], summary_level="reg+post")
```

See Also:
: * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)
  * [nflreadpy](https://github.com/nflverse/nflreadpy) – direct nflverse Python bindings

### sportsdataverse.nfl.nfl_loaders.load_nfl_teams(return_as_pandas=False) → DataFrame

Load NFL team ID information and logos

* **Parameters:**
  **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing teams available.
* **Return type:**
  pl.DataFrame

### Example

Quick start:

```default
from sportsdataverse.nfl import load_nfl_teams
teams = load_nfl_teams()
teams.shape
```

Pandas round-trip:

```default
teams_pd = load_nfl_teams(return_as_pandas=True)
teams_pd[["team_abbr", "team_name", "team_conf", "team_division"]].head()
```

See Also:
: * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)
  * [nflreadpy](https://github.com/nflverse/nflreadpy) – direct nflverse Python bindings

### sportsdataverse.nfl.nfl_loaders.load_nfl_trades(return_as_pandas=False) → DataFrame

Load NFL trades data

* **Parameters:**
  **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing NFL trade information.
* **Return type:**
  pl.DataFrame

### Example

Quick start:

```default
from sportsdataverse.nfl import load_nfl_trades
trades = load_nfl_trades()
trades.shape
```

Filter to a single season:

```default
import polars as pl
trades_2024 = load_nfl_trades().filter(pl.col("season") == 2024)
```

See Also:
: * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)
  * [nflreadpy](https://github.com/nflverse/nflreadpy) – direct nflverse Python bindings

### sportsdataverse.nfl.nfl_loaders.load_nfl_weekly_rosters(seasons: List[int], return_as_pandas=False) → DataFrame

Load NFL weekly roster data for selected seasons

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2002 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing weekly rosters available for the requested seasons.
* **Return type:**
  pl.DataFrame

### Example

Single season:

```default
from sportsdataverse.nfl import load_nfl_weekly_rosters
weekly = load_nfl_weekly_rosters(seasons=[2024])
```

Multi-season range with a follow-up week filter:

```default
import polars as pl
wk1 = (
    load_nfl_weekly_rosters(seasons=range(2022, 2025))
    .filter(pl.col("week") == 1)
)
```

See Also:
: * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)
  * [nflreadpy](https://github.com/nflverse/nflreadpy) – direct nflverse Python bindings

## sportsdataverse.nfl.nfl_pbp module

### *class* sportsdataverse.nfl.nfl_pbp.NFLPlayProcess(gameId=0, raw=False, path_to_json='/', return_keys=None, \*\*kwargs)

Bases: `object`

Process ESPN NFL play-by-play feeds into a tidy game-level dictionary.

Wraps the ESPN `summary` endpoint (or a local JSON dump) and pipes the
result through a chain of feature-engineering steps – down/distance,
play-type flags, EPA, WPA, QBR, drive aggregation, and an advanced
box score. Use `run_processing_pipeline()` for the full feature set
or `run_cleaning_pipeline()` for a lighter clean.

* **Parameters:**
  * **gameId** (*int*) – ESPN `event` id (e.g. `401671801`).
  * **raw** (*bool*) – If `True`, `espn_nfl_pbp()` returns the ESPN payload
    untouched. If `False` (default), it normalizes keys.
  * **path_to_json** (*str*) – Directory containing `{gameId}.json` for the
    `nfl_pbp_disk()` flow (offline replay).
  * **return_keys** (*list* *[**str* *]*  *|* *None*) – If supplied, `run_processing_pipeline`
    returns only the listed keys from the result dict.

### Example

End-to-end pipeline against the live ESPN endpoint:

```default
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
result = proc.run_processing_pipeline()
len(result["plays"])
```

Offline replay from a JSON dump:

```default
proc = NFLPlayProcess(gameId=401671801, path_to_json="./pbp_dump")
proc.nfl_pbp_disk()
cleaned = proc.run_cleaning_pipeline()
```

Subset the return payload:

```default
proc = NFLPlayProcess(gameId=401671801, return_keys=["plays", "boxscore"])
proc.espn_nfl_pbp()
slim = proc.run_processing_pipeline()
sorted(slim.keys())  # ['boxscore', 'plays']
```

See Also:
: * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)
  * [nflfastR](https://www.nflfastr.com) – R sister package for NFL PBP

#### \_\_helper_\_espn_nfl_odds_information_\_()

Fetch pre-game spread/total from ESPN’s modern core odds endpoint.

Returns `(gameSpread, overUnder, homeFavorite, gameSpreadAvailable)`.
Mirrors the CFB equivalent — the legacy `pickcenter` array on the
summary endpoint trends toward empty for recent games, so this
restores the data path via the `sports.core.api.espn.com` v2 odds
collection. Falls back to defaults `(2.5, 55.5, True, False)` on
empty / error / decode failure to preserve pre-existing
caller-visible behavior.

#### \_\_init_\_(gameId=0, raw=False, path_to_json='/', return_keys=None, \*\*kwargs)

#### corrupt_pbp_check()

Detect ESPN payloads that look corrupt or partial.

Returns `True` when one of three guard conditions trips:

* No plays at all.
* Fewer than 50 plays for a game ESPN reports as completed.
* More than 500 plays for a game ESPN reports as completed.

`run_processing_pipeline()` and `run_cleaning_pipeline()` use
this to skip feature engineering on obviously broken payloads.

* **Returns:**
  `True` if the payload looks corrupt; `False` otherwise.
* **Return type:**
  bool

### Example

Verify before running expensive feature engineering:

```default
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
if not proc.corrupt_pbp_check():
    result = proc.run_processing_pipeline()
```

#### create_box_score(play_df)

Build the advanced box score (passer / rusher / receiver / team / situational / defensive / turnover / drives)
from a feature-engineered plays DataFrame.

This is normally called by `run_processing_pipeline()` – it
auto-runs the pipeline first if it hasn’t been triggered yet, so
callers can also invoke it directly on a freshly-instantiated
processor.

* **Parameters:**
  **play_df** (*pl.DataFrame*) – The plays frame produced after the full
  feature-engineering chain (downs, play-type flags, EPA,
  WPA, drive aggregation).
* **Returns:**
  Box score keyed by `"pass"`, `"rush"`,
  `"receiver"`, `"team"`, `"situational"`, `"defensive"`,
  `"turnover"`, `"drives"` – each value a list of dicts
  ready to be serialized.
* **Return type:**
  Dict[str, list]

### Example

Run the pipeline and pull out the box score:

```default
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
result = proc.run_processing_pipeline()
box = result["advBoxScore"]
sorted(box.keys())
```

#### espn_nfl_pbp(\*\*kwargs)

espn_nfl_pbp() - Pull the game by id. Data from API endpoints: nfl/playbyplay, nfl/summary

* **Parameters:**
  **game_id** (*int*) – Unique game_id, can be obtained from nfl_schedule().
* **Returns:**
  Dictionary of game data with keys - “gameId”, “plays”, “boxscore”, “header”, “broadcasts”,
  : ”videos”, “playByPlaySource”, “standings”, “leaders”, “timeouts”, “homeTeamSpread”, “overUnder”,
    “pickcenter”, “againstTheSpread”, “odds”, “predictor”, “winprobability”, “espnWP”,
    “gameInfo”, “season”
* **Return type:**
  Dict

### Example

Standard normalized payload:

```default
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401220403)
payload = proc.espn_nfl_pbp()
sorted(payload.keys())[:5]
```

Raw ESPN passthrough (no key normalization):

```default
proc_raw = NFLPlayProcess(gameId=401220403, raw=True)
espn_dump = proc_raw.espn_nfl_pbp()
```

Chain into the full processing pipeline:

```default
proc = NFLPlayProcess(gameId=401220403)
proc.espn_nfl_pbp()
result = proc.run_processing_pipeline()
```

#### gameId *= 0*

#### nfl_pbp_disk()

Load a previously-saved ESPN payload from `{path_to_json}/{gameId}.json`.

Use this to replay an old game offline without hitting the ESPN
endpoint – handy for snapshot-driven tests and reproducible
feature engineering.

* **Returns:**
  The parsed JSON content; also stored on `self.json`.
* **Return type:**
  Dict

### Example

Replay a dump on disk:

```default
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401220403, path_to_json="./pbp_dump")
proc.nfl_pbp_disk()
result = proc.run_processing_pipeline()
```

#### nfl_pbp_json(\*\*kwargs)

Set `self.json` to the imported `json` module reference (legacy stub).

Retained for API compatibility. Prefer `espn_nfl_pbp()` (live)
or `nfl_pbp_disk()` (offline) to populate `self.json` with an
actual ESPN payload.

* **Returns:**
  The Python `json` module reference (mirrors legacy behavior).
* **Return type:**
  module

### Example

Stub usage (rarely needed – prefer the live or disk loaders):

```default
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401220403)
proc.nfl_pbp_json()  # populates `self.json` with the json module
```

#### path_to_json *= '/'*

#### ran_cleaning_pipeline *= False*

#### ran_pipeline *= False*

#### raw *= False*

#### return_keys *= None*

#### run_cleaning_pipeline()

Run the lighter cleaning pipeline against `self.json`.

Identical to `run_processing_pipeline()` up through the
`__add_spread_time` step but stops short of EPA / WPA / QBR /
drive aggregation and the advanced box score. Use this when you
want clean play structure without the modeled features.

* **Returns:**
  The cleaned game dict (or the subset specified by
  `return_keys` at construction).
* **Return type:**
  Dict

### Example

Lighter run – drop the modeled features:

```default
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
cleaned = proc.run_cleaning_pipeline()
"plays" in cleaned and "advBoxScore" not in cleaned
```

#### run_processing_pipeline()

Run the full feature-engineering pipeline against `self.json`.

Pipes the plays frame through the chain of helpers: downs,
play-type flags, rush/pass flags, team-score variables, new play
types, penalties, play-category flags, yardage cols, player cols,
post-play cols, spread time, EPA, WPA, drive data, and QBR –
followed by the advanced box score build.

* **Returns:**
  The full processed game dict (or the subset
  specified by `return_keys` at construction). Returns the
  partial result when `corrupt_pbp_check()` short-circuits.
* **Return type:**
  Dict | None

### Example

Standard end-to-end run:

```default
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
result = proc.run_processing_pipeline()
len(result["plays"]), len(result["drives"])
```

Subset returned keys for downstream serialization:

```default
proc = NFLPlayProcess(
    gameId=401671801,
    return_keys=["plays", "advBoxScore", "winprobability"],
)
proc.espn_nfl_pbp()
slim = proc.run_processing_pipeline()
sorted(slim.keys())
```

## sportsdataverse.nfl.nfl_schedule module

### sportsdataverse.nfl.nfl_schedule.espn_nfl_calendar(season=None, ondays=None, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_nfl_calendar - look up the NFL calendar for a given season

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

Fetch the calendar for a single season:

```default
from sportsdataverse.nfl import espn_nfl_calendar
cal = espn_nfl_calendar(season=2024)
```

Fetch the ondays-style flattened schedule URLs:

```default
on_days = espn_nfl_calendar(season=2024, ondays=True)
```

Pandas round-trip:

```default
cal_pd = espn_nfl_calendar(season=2024, return_as_pandas=True)
```

### sportsdataverse.nfl.nfl_schedule.espn_nfl_schedule(dates=None, week=None, season_type=None, groups=None, limit=500, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_nfl_schedule - look up the NFL schedule for a given season

* **Parameters:**
  * **dates** (*int*) – Used to define different seasons. 2002 is the earliest available season.
  * **week** (*int*) – Week of the schedule.
  * **season_type** (*int*) – 2 for regular season, 3 for post-season, 4 for off-season.
  * **limit** (*int*) – number of records to return, default: 500.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing schedule dates for the requested season. Returns None if no games
* **Return type:**
  pl.DataFrame

### Example

Single date (YYYYMMDD):

```default
from sportsdataverse.nfl import espn_nfl_schedule
sched = espn_nfl_schedule(dates=20240908)
```

Specific week of regular season (`season_type=2`):

```default
wk1 = espn_nfl_schedule(dates=2024, week=1, season_type=2)
```

Pandas round-trip:

```default
sched_pd = espn_nfl_schedule(dates=20240908, return_as_pandas=True)
```

### sportsdataverse.nfl.nfl_schedule.get_current_week()

Return the current NFL week (1-22) using a calendar-driven heuristic.

Counts whole weeks since the first Thursday of September of the
current season (Labor Day Monday + 3 days) and clamps to `[1, 22]`.

* **Returns:**
  Current week number, capped at 22.
* **Return type:**
  int

### Example

Pair with `most_recent_nfl_season` for a season+week label:

```default
from sportsdataverse.nfl.nfl_schedule import (
    get_current_week, most_recent_nfl_season,
)
season, week = most_recent_nfl_season(), get_current_week()
label = f"NFL {season} -- Week {week}"
```

See Also:
: * `sportsdataverse.nfl.get_current_nfl_week()` – the
    unified `utils_date` variant; supports a schedule-driven
    `use_date=False` mode and a `roster=True` flag.

### sportsdataverse.nfl.nfl_schedule.most_recent_nfl_season()

Return the most recent NFL season year using a coarse September cutoff.

Simpler counterpart to `sportsdataverse.nfl.get_current_nfl_season()` –
flips on September 1 rather than the Thursday after Labor Day. Use the
`utils_date` version when you need calendar precision; this one is
fine for “what’s the current season” UI labels.

* **Returns:**
  Current season year.
* **Return type:**
  int

### Example

Quick label for a UI banner:

```default
from sportsdataverse.nfl.nfl_schedule import most_recent_nfl_season
label = f"NFL {most_recent_nfl_season()} season"
```

### sportsdataverse.nfl.nfl_schedule.scoreboard_event_parsing(event)

Normalize one ESPN scoreboard `event` into a flatter shape.

Splits the competitors list into `home` / `away` siblings, hoists
notes / broadcast metadata onto the competition root, and drops the
fields the schedule helper does not need (`odds`, `leaders`,
`geoBroadcasts`, etc.). Used internally by
[`espn_nfl_schedule()`](#sportsdataverse.nfl.nfl_schedule.espn_nfl_schedule).

* **Parameters:**
  **event** (*Dict*) – A single `events[i]` dict from the ESPN
  scoreboard endpoint.
* **Returns:**
  The mutated event dict with normalized `home` / `away` /
  broadcast keys.
* **Return type:**
  Dict

### Example

Wire it into a custom schedule pull:

```default
from sportsdataverse.dl_utils import download
from sportsdataverse.nfl.nfl_schedule import scoreboard_event_parsing
url = "http://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
payload = download(url=url).json()
for ev in payload.get("events", []):
    scoreboard_event_parsing(ev)
    ev["competitions"][0]["home"]["abbreviation"]
```

## sportsdataverse.nfl.nfl_teams module

### sportsdataverse.nfl.nfl_teams.espn_nfl_teams(return_as_pandas=False, \*\*kwargs) → DataFrame

espn_nfl_teams - look up NFL teams

* **Parameters:**
  **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing teams for the requested league.
  This function caches by default, so if you want to refresh the data, use the command
  sportsdataverse.nfl.espn_nfl_teams.clear_cache().
* **Return type:**
  pl.DataFrame

### Example

Quick start:

```default
from sportsdataverse.nfl import espn_nfl_teams
teams = espn_nfl_teams()
teams.shape
```

Pandas round-trip:

```default
teams_pd = espn_nfl_teams(return_as_pandas=True)
teams_pd[["team_abbreviation", "team_display_name"]].head()
```

Force a refresh after upstream ESPN updates:

```default
espn_nfl_teams.cache_clear()  # underlying lru_cache
teams = espn_nfl_teams()
```

## sportsdataverse.nfl.utils_date module

Date utility helpers for the NFL module.

Ported from nflreadpy’s utils_date.py (get_current_season,
get_current_week) so sdv-py users get the same conventions for
“what season are we in?” and “what week are we in?”.

NFL season convention (matching nflreadpy):

* A new season “starts” on the Thursday following Labor Day. Before that
  date, the current season is the previous calendar year (e.g. early Aug
  2026 is still season 2025).
* Roster year flips on March 15 instead.
* Week is 1-22 (1-18 regular season, 19-22 playoffs).

### sportsdataverse.nfl.utils_date.get_current_nfl_season(roster: bool = False) → int

Return the current NFL season year.

* **Parameters:**
  **roster** – If True, use roster-year logic (current calendar year on/after
  March 15, otherwise previous year). If False, use season logic
  (current calendar year on/after the Thursday following Labor Day,
  otherwise previous year).
* **Returns:**
  The current season (or roster) year.
* **Return type:**
  int
* **Raises:**
  **TypeError** – If roster is not a bool.

### Example

Default season-year semantics:

```default
from sportsdataverse.nfl import get_current_nfl_season
season = get_current_nfl_season()
print(season)
```

Roster-year semantics (March 15 cutover):

```default
roster_year = get_current_nfl_season(roster=True)
```

Pair with a loader to fetch only the active season:

```default
from sportsdataverse.nfl import load_nfl_schedule
schedule = load_nfl_schedule(seasons=[get_current_nfl_season()])
```

See Also:
: * [nflreadpy](https://github.com/nflverse/nflreadpy) – mirrors this convention
  * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)

### sportsdataverse.nfl.utils_date.get_current_nfl_week(use_date: bool = True, roster: bool = False) → int

Return the current NFL week (1-22).

* **Parameters:**
  * **use_date** – If True (default), compute the week purely from the calendar
    (number of weeks since the first Thursday of September of the
    current season). If False, hit the live schedule via
    load_nfl_schedule() and return the week of the next unplayed
    game (matches nflreadpy’s use_date=False path).
  * **roster** – Forwarded to get_current_nfl_season() for season inference.
* **Returns:**
  The current week, capped at 22.
* **Return type:**
  int
* **Raises:**
  **TypeError** – If use_date or roster is not a bool.

### Example

Calendar-driven week (default, no network):

```default
from sportsdataverse.nfl import get_current_nfl_week
week = get_current_nfl_week()
```

Schedule-driven week (hits the live schedule parquet):

```default
week_live = get_current_nfl_week(use_date=False)
```

Roster-year season inference:

```default
week_roster = get_current_nfl_week(roster=True)
```

Pair with a PBP fetch to grab only the most recent season+week:

```default
import polars as pl
from sportsdataverse.nfl import (
    get_current_nfl_season, get_current_nfl_week, load_nfl_pbp,
)
current_pbp = (
    load_nfl_pbp(seasons=[get_current_nfl_season()])
    .filter(pl.col("week") == get_current_nfl_week())
)
```

See Also:
: * [nflreadpy](https://github.com/nflverse/nflreadpy) – mirrors this convention
  * [nflverse](https://nflverse.nflverse.com) – full data ecosystem (R + Python)

### sportsdataverse.nfl.utils_date.most_recent_nfl_season(roster: bool = False) → int

Alias for get_current_nfl_season() mirroring nflreadr’s
most_recent_season().

### Example

Bare alias call (matches the R-side `most_recent_season()`):

```default
from sportsdataverse.nfl.utils_date import most_recent_nfl_season
season = most_recent_nfl_season()
```

Roster-year flavor:

```default
roster_year = most_recent_nfl_season(roster=True)
```

## Module contents
