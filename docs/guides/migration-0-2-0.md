# Migrating to 0.2.0

> Every breaking change in sportsdataverse-py 0.2.0, with before and after code and how to migrate.

0.2.0 is the release after 0.1.4 (0.1.5 was never published; its changes ship in 0.2.0); it ships first as the
`0.2.0b1` pre-release (`pip install --pre sportsdataverse`). It carries sixteen breaking changes, each recorded
as a `changelog.d/*.breaking.md` fragment; the changes are on `main` already. This
page takes them one at a time: what changed, who is affected, a before/after
snippet and how to migrate. The fragments are the source of record, and the
release's `CHANGELOG.md` section is built from them.

Most code is affected by at most two of them: the [Int64 ids](#ids-are-int64-at-the-parser-boundary)
and the [polars 2 floor](#polars-2-is-required). Read those first.

In the snippets, `# 0.1.x` marks code that shows the old behaviour (it is not meant
to be run against 0.2.0) and `# 0.2.0` marks the replacement.

## Ids are Int64 at the parser boundary

**What changed.** The ESPN, NHL and HockeyTech parsers emit every `id` / `*_id`
column as `Int64`. Those columns used to be String, Int32, or Float64 where a
missing value passed through pandas. This covers the generated `espn_<lg>_<short>`
wrappers, the hand-written `espn_<lg>_schedule` / `espn_<lg>_teams` /
`espn_<lg>_game_rosters`, the ESPN injury and depth-chart snapshots, the ESPN
content parser, the NCAA ESPN crosswalks, the NFL processor (`*_player_id`,
`pos_team` and its lags and leads), the NHL API parsers, `nhl_player_props` /
`pwhl_player_props`, and every HockeyTech frame (PWHL and the 19 other leagues).
With `return_as_pandas=True` the same columns are pandas' nullable `Int64` (or
`int64[pyarrow]` for the Arrow-backed frames). A few ids are text by design and stay
String: ESPN's `now_id`, NHL stats-rest `formatted_season_id`, and NFL.com gsis ids
such as `"00-0034796"`. An id column holding a value that is not an integer keeps
its dtype and warns `SchemaDriftWarning`.

**Not yet covered:** the soccer leagues' `scoreboard`, `standings`, `summary`,
`teams_site` and `team_roster` wrappers and the cricket leagues' `scoreboard`,
`standings` and `summary` wrappers keep String ids in this release.

**Who is affected.** Anyone who compares an id to a string, joins a parser frame to
a String id, or stores these frames.

```python
# 0.1.x
props.filter(pl.col("player_id") == "8478402")
```

```python
# 0.2.0
import polars as pl
from sportsdataverse.nba import espn_nba_scoreboard

games = espn_nba_scoreboard(dates="20240317")
print(games.schema["game_id"])                      # Int64
games.filter(pl.col("home_id") == 19)               # compare to an int, not "19"
```

**How to migrate.**

- Compare ids to ints. If you need text, cast back with
  `pl.col("team_id").cast(pl.String)` (pandas: `.astype("string")`); an `Int64` id
  stringifies as `"123"`, never `"123.0"`.
- **Release loaders keep their published dtypes.** Several release files still
  carry String ids: `load_pwhl_schedules` (`game_id`, `home_team_id`, `away_team_id`),
  `load_pwhl_pbp` (`team_id`, `player_team_id`, `home_team_id`, `away_team_id`,
  `game_season_id`), the PWHL box scores' `player_id`,
  `load_{mbb,nba,wbb,wnba}_rosters` (`athlete_id`, `position_id`, `status_id`) and
  `load_{mbb,nba,wnba}_game_rosters` (`game_id`). Joining one of those to live parser
  output raises `SchemaError`, so cast first:
  `release.with_columns(pl.col("game_id").cast(pl.Int64))`. An Int32 id joins an
  Int64 one as it is. [IDs and joins](ids-and-joins.md) has a worked example.
- **`build_nfl_season` cache:** `PIPELINE_VERSION` is 2, so per-game frames cached by
  an earlier version are rebuilt rather than mixed into a season with the new Int64
  ids. Nothing to do.
- **Producers that store or republish these frames** should move the published ids
  to Int64 now, not back to String, and rewrite the stored history in one pass:
  - Deploy order: update (or pin) every producer that writes these frames first,
    then upgrade.
  - Stored history: cast the stored ids once (`pl.col(c).cast(pl.Int64)`) or rebuild
    every season before the first post-upgrade run. Otherwise `pd.concat` /
    `to_parquet` of the master fails on a mixed column, and
    `pl.concat(how="diagonal_relaxed")` silently turns the ids back into String.
  - RDS: `pyreadr.write_rds` cannot write a nullable `Int64` with a missing value.
    For that write only, cast the id columns to pandas' nullable `Int32` when every
    id fits in 32 bits (`df[id_cols] = df[id_cols].astype("Int32")`), otherwise to
    `float64`. The parquet stays Int64.
  - R packages: releases rebuilt from these parsers carry integer ids where hoopR,
    wehoop and fastRhockey document some of them as character. Republish every
    season in the same pass and update the R documentation.

## polars 2 is required

**What changed.** The runtime pin is `polars>=2.0,<3`. polars 1.x is no longer
supported, and the package may use 2.x-only API.

**Who is affected.** Environments that pin polars below 2.

```bash
# 0.1.x
pip install "sportsdataverse" "polars<2"
```

```bash
# 0.2.0
pip install -U "sportsdataverse" "polars>=2,<3"
```

**How to migrate.** Upgrade polars, then check your own code against the polars 2
upgrade guide. The changes that bite most often: `explode()` turns an empty list
into zero rows (pass `empty_as_null=` explicitly), casting a String column with
`.cast(pl.Date)` raises (use `str.to_date()`), `is_in` raises on an Int column
against a float list, and `pl.read_parquet` on a GitHub release URL fails with
HTTP 501 (pass `use_pyarrow=True`).

## Basketball pbp coordinates use the hoopR / wehoop frame

**What changed.** In `espn_{nba,wnba,mbb,wbb}_pbp` plays, `coordinate.x` /
`coordinate.y` keep their names but change meaning and dtype. They were ESPN's raw
feet (hoop at (25, 0)); they are now the full-court frame the hoopR / wehoop files
use (`x = y - 41.75`, `y = x - 25`, both negated on home-team plays), always
Float64. ESPN's values move to `coordinate.x.raw` / `coordinate.y.raw`
(`coordinate_x_raw` / `coordinate_y_raw` once snake-cased). On the NBA / WNBA free
throws that hoopR / wehoop's "Free Throw" match finds, the raw pair holds the pinned
(25, 13.75), as in the published files. The release converters
(`helper_<lg>_play_by_play`) rebuild from the raw pair, so a processed payload
converts once.

**Who is affected.** Shot charts and anything that reads `coordinate.x` /
`coordinate.y` as ESPN feet.

```python
# 0.1.x: ESPN feet
shots = plays.filter(pl.col("shootingPlay") == True).select("coordinate.x", "coordinate.y")
```

```python
# 0.2.0: ESPN feet are the .raw pair; filter out the missing-location sentinel
import polars as pl
from sportsdataverse.nba import espn_nba_pbp, nba_game_page

plays = pl.from_dicts(espn_nba_pbp(game_id=401585607)["plays"], infer_schema_length=None)
shots = plays.filter(
    (pl.col("shootingPlay") == True) & (pl.col("coordinate.x.raw") > nba_game_page.COORD_SENTINEL)
).select("coordinate.x.raw", "coordinate.y.raw", "coordinate.x", "coordinate.y")
```

**How to migrate.** Read the `.raw` pair where you wanted ESPN feet, or use the new
columns directly if you were converting to the hoopR frame yourself. ESPN's
missing-location sentinel reads about ±2.1e8 after the transform; filter on
`coordinate_x_raw > nba_game_page.COORD_SENTINEL` (the snake-cased name in the
processed frames).

## ESPN leaders return one row per athlete

**What changed.** `espn_<league>_leaders()` returns one row per athlete and stat
category (#1018). It used to return a single envelope row whose `pagination_*`,
`league_*` and `current_season_*` columns described the request, with the
athletes packed into one cell. Leagues without a leaderboard return zero rows.

**Who is affected.** Callers that read the envelope columns, or that unpacked the
`athletes` / `categories` cells themselves.

```python
# 0.2.0
from sportsdataverse.nba import espn_nba_leaders

leaders = espn_nba_leaders()                     # one row per athlete
raw = espn_nba_leaders(return_parsed=False)      # pagination and per-stat ranks
```

**How to migrate.** Drop the unpacking step. Read page counts and ESPN's
per-stat ranks from `return_parsed=False`.

## ESPN game rosters honour `raw=True`

**What changed.** `espn_<lg>_game_rosters(raw=True)` (mbb, wbb, nba, wnba, nfl, nhl,
cfb) returns the Core v2 competitors payload, a dict. `raw=` used to be accepted
and ignored, so it returned the parsed frame.

**Who is affected.** Callers that passed `raw=True` and used the result as a frame.

```python
# 0.1.x: raw=True returned the frame anyway
roster = espn_nba_game_rosters(game_id=401585607, raw=True)
```

```python
# 0.2.0
from sportsdataverse.nba import espn_nba_game_rosters

roster = espn_nba_game_rosters(game_id=401585607)               # the parsed frame
payload = espn_nba_game_rosters(game_id=401585607, raw=True)    # the raw dict
```

**How to migrate.** Drop `raw=True` where you want the frame.

## ESPN schedules filter by season type

**What changed.** The six schedule copies that sent `seasonType` (NFL, NHL, WNBA,
NBA, MBB, WBB) now send `seasontype`. ESPN ignored the camelCase key, so
`season_type=` with `dates=YYYY&week=N` never filtered; it does now.

**Who is affected.** Callers passing `season_type=` to those schedules. The same
call returns different rows: only that season type.

```python
# 0.2.0: season_type=2 now really returns the regular season only
from sportsdataverse.nfl import espn_nfl_schedule

week1 = espn_nfl_schedule(dates=2024, week=1, season_type=2)
```

**How to migrate.** If you relied on the unfiltered rows, drop `season_type=`.

## Fox pbp splits the team from the player

**What changed.** In `fox_nfl_pbp` / `fox_cfb_pbp`, `play_team` is the club code of
the drive group the play is filed under (for example `"NYG"`). It used to hold the
name of the player Fox credits the play to, which moves to the new `play_player`
column.

**Who is affected.** Anyone reading `play_team` from the Fox pbp.

```python
# 0.1.x: play_team held a player name
pbp.select("play_team")
```

```python
# 0.2.0
from sportsdataverse.nfl import fox_nfl_pbp

pbp = fox_nfl_pbp("11052")               # a Fox Bifrost event id
pbp.select("play_team", "play_player")   # club code ("BUF"), credited player
```

**How to migrate.** Rename your `play_team` reads to `play_player` where you meant
the player.

## Helpers report failed fetches

**What changed.** Two helpers no longer hide a failure:

- `nfl_players_crosswalk()` raises `AssetFetchError` when the nflverse players master
  cannot be fetched. It used to return a zero-row frame, which nulled every id in a
  join. An absent asset still gives zero rows, and a failed DynastyProcess load now
  warns (the yahoo / cbs ids are null).
- `nfl.update_config(cache_mode=...)` raises `ValueError` for a mode other than
  `"memory"`, `"filesystem"` or `"off"`. An unknown mode used to switch caching off
  silently.

**Who is affected.** Pipelines that called either and relied on the silent fallback.

```python
# 0.1.x: "disk" switched the cache off without a word
update_config(cache_mode="disk")
```

```python
# 0.2.0
from sportsdataverse.errors import AssetFetchError
from sportsdataverse.nfl import nfl_players_crosswalk, update_config

update_config(cache_mode="filesystem")   # "memory", "filesystem" or "off"; anything else raises ValueError
try:
    xwalk = nfl_players_crosswalk()
except AssetFetchError:
    xwalk = None                          # the fetch failed: retry later, do not join on nulls
```

**How to migrate.** Catch `AssetFetchError` where you can retry, and fix any cache
mode string that was not one of the three.

## stats.ncaa.org failures raise the shared vocabulary

**What changed.** The stats.ncaa.org fetch layer raises `NoDataError` on a real 404
after one request, and `AssetFetchError` (naming which) for a banned or exhausted
proxy pool, an uncleared bm-verify challenge or the Terms gate. Both used to be
`RuntimeError`. A missing proxy configuration is still `RuntimeError`.

**Who is affected.** Code that caught `RuntimeError` around stats.ncaa.org calls.

```python
# 0.1.x (not run here: stats.ncaa.org bans cloud IPs)
try:
    pbp = ncaa_mbb_play_by_play([5254137])
except RuntimeError:
    ...
```

```python
# 0.2.0 (not run here: stats.ncaa.org bans cloud IPs)
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.mbb import ncaa_mbb_play_by_play

try:
    pbp = ncaa_mbb_play_by_play([5254137])
except NoDataError:
    pbp = None          # the game page does not exist
except AssetFetchError:
    raise               # banned, challenged or out of proxies: the answer is unknown
```

**How to migrate.** Catch `AssetFetchError` (and `NoDataError` where a missing page
is expected) instead of `RuntimeError`.

## NCAA basketball scrapes raise instead of dropping games

**What changed.** `ncaa_{mbb,wbb}_play_by_play` and `ncaa_{mbb,wbb}_box_scores`
raise `AssetFetchError` when a fetch fails (a ban, an exhausted pool, an uncleared
challenge) instead of dropping that game, which made a ban read as a smaller season.
`errors="skip"` keeps going and adds a `scrape_status` column with one row per
dropped game saying why.

**Who is affected.** Season scrapes that expected a partial frame on failure.

```python
# 0.2.0 (not run here: stats.ncaa.org bans cloud IPs)
import polars as pl
from sportsdataverse.mbb import ncaa_mbb_box_scores

box = ncaa_mbb_box_scores([5254137, 5254138], errors="skip")
dropped = box.filter(pl.col("scrape_status").is_not_null())
```

**How to migrate.** Pass `errors="skip"` and read `scrape_status` if you want the
old keep-going behaviour, with the drops now visible.

## NFL and CFB processed plays keep their documented Float64

**What changed.** Processed NFL and CFB plays keep the documented Float64 for the
EP/WP family, `first_down_prob`, `prob_2pt` and `drive_start`; they were Float32
when a model scored. No value changes. NFL `teamParticipants` has one struct field
order, and `create_box_score(plays_frame)` still reproduces `advBoxScore` exactly.

**Who is affected.** Code that asserted Float32 on those columns or concatenated
them with frames written by an earlier version.

```python
# 0.2.0
import polars as pl
from sportsdataverse.nfl import build_nfl_season

plays = build_nfl_season(game_ids=[401671889])
print(plays.schema["home_wp_before"])       # Float64
```

**How to migrate.** Cast stored Float32 history to Float64 before concatenating it
with new frames.

## The NFL QBR model checks its feature order

**What changed.** `sportsdataverse.nfl.model_vars.qbr_vars` is in the QBR trainer's
order (`qbr_epa`, `pass_epa`, `rush_epa`, `sack_epa`, `pen_epa`, `spread`), and
`nfl/models/qbr_model.ubj` is re-saved with those feature names (same trees). A frame
in the old column order raises instead of scoring permuted inputs.

**Who is affected.** Code that scores the bundled QBR model directly.

```python
# 0.2.0
from sportsdataverse.nfl.model_vars import qbr_vars

print(qbr_vars)   # ['qbr_epa', 'pass_epa', 'rush_epa', 'sack_epa', 'pen_epa', 'spread']
```

**How to migrate.** Select features with `qbr_vars` (`frame.select(qbr_vars)`)
rather than a hand-written column list.

## The current NFL season follows nflreadr's rule

**What changed.** The current season flips on the Wednesday after Labor Day, and
week 1 starts on the Thursday after it (nflreadr's rules). Earlier code read the week
one high in 2015-16, 2020-22 and 2026-27. `most_recent_nfl_season` and
`get_current_week` are now that one rule, so from September 1 to that Wednesday they
give the previous season (week 22), not the new one.

**Who is affected.** Jobs that run in early September and default to the current
season.

```python
# 0.2.0
from sportsdataverse.nfl import get_current_week, most_recent_nfl_season

print(most_recent_nfl_season(), get_current_week())
```

**How to migrate.** Pass the season explicitly in jobs that run between September 1
and the Wednesday after Labor Day.

## Provider jerseys and padded codes stay String

**What changed.** Columns whose values are codes, not numbers, keep their text:

- Basketball-Reference: `bref_team_roster()` returns `number` as String (was
  Float64), so Robert Parish's `"00"` is no longer `0.0`.
- Bart Torvik: `torvik_player_stats()` / `parse_torvik_player_stats()` return
  `number` as String (was Int64), so the `"00"` jerseys no longer merge into the `0`
  rows.
- 247Sports site pages: `primary_color`, `jersey` and `postal_code` are String (were
  Int64 when every value was digits); a (0, 0) `latitude` / `longitude` pair, 247's
  no-location sentinel, is null.
- KenPom: the jersey column is String on every page and renamed to `jersey`
  (`column_0` on the team pages, `column_0_rk` on the box pages).

**Who is affected.** Code that does arithmetic on jerseys or reads the old KenPom
column names.

```python
# 0.1.x
kenpom_team.select("column_0")
```

```python
# 0.2.0 (not run here: the KenPom pages need a subscription login)
kenpom_team.select("jersey")
```

**How to migrate.** Treat jerseys as text, and rename the KenPom column reads.

## PWHL player box returns skaters and goalies

**What changed.** `pwhl_player_box(game_id)` returns
`{"skaters": DataFrame, "goalies": DataFrame}`, as fastRhockey's `pwhl_player_box`
does. It used to return a single DataFrame, which was always empty.

**Who is affected.** Every caller of `pwhl_player_box`.

```python
# 0.1.x
box = pwhl_player_box(game_id)          # one (empty) frame
```

```python
# 0.2.0
from sportsdataverse.pwhl import pwhl_player_box, pwhl_schedule

game_id = int(pwhl_schedule(season=2025)["game_id"][0])
box = pwhl_player_box(game_id)
skaters, goalies = box["skaters"], box["goalies"]
```

**How to migrate.** Index the result by `"skaters"` or `"goalies"`.

## Spring football EP/WP refuses the CFL

**What changed.** `build_spring_football_pbp(..., league="cfl")` and
`enrich_spring_football_pbp(..., league="cfl")` raise `ValueError`. They used to
score CFL plays with the NFL EP/WP boosters, whose support is `yardline_100` 1-99
while a CFL field is 110 yards with three downs. No CFL pipeline ships.
`get_sf_constants("cfl")` still returns the descriptive CFL rule constants.

**Who is affected.** Anyone who built CFL EP/WP through these functions. Those
numbers were not meaningful.

```python
# 0.2.0
from sportsdataverse.football.spring_football_constants import get_sf_constants

cfl = get_sf_constants("cfl")    # the rule constants still work
```

**How to migrate.** Drop CFL from spring-football EP/WP runs. The UFL and XFL are
unchanged.

## Deprecations are removed in 0.4.0

Nothing deprecated in 0.2.0 breaks yet. Each name below still works, warns
`DeprecationWarning` naming its replacement, and is removed in 0.4.0, two minor releases on:

- the old package paths moved under `sportsdataverse.basketball`, `sportsdataverse.providers` and
  `sportsdataverse.analytics`;
- the 12 old `*_espn_ext` names of the wrappers renamed to `*_site`;
- 29 unread `sportsdataverse.config` release URLs;
- `dl_utils.ESPNHTTP` / `ESPNResponse`, the `df.janitor` case methods and
  `decorators.record_mem_usage` / `record_time_usage`;
- `espn_<lg>_teams.cache_clear()` and `sportsdataverse.scrape.ncaa.league_config`;
- `pwhl_team_ratings(league=...)`;
- the `load_nba_stats_*_v3` and four `load_wnba_stats_*` loader shims.

**How to migrate.** Run your code with `python -W error::DeprecationWarning` (or
`pytest -W error::DeprecationWarning`) and follow each message.
