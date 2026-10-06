---
title: "NBA — additional Python functions — Other"
sidebar_label: "Other"
sidebar_position: 10
description: "NBA — additional Python functions — Other — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Other

### espn_nba_game_rosters {#espn_nba_game_rosters}

`espn_nba_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nba_game_rosters() - Pull the game by id.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from espn_nba_schedule(). |
| `raw` |  | `False` |  |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe of game roster data with columns: 'athlete_id', 'athlete_uid', 'athlete_guid', 'athlete_type', 'first_name', 'last_name', 'full_name', 'athlete_display_name', 'short_name', 'weight', 'display_weight', 'height', 'display_height', 'age', 'date_of_birth', 'slug', 'jersey', 'linked', 'active', 'alternate_ids_sdr', 'birth_place_city', 'birth_place_state', 'birth_place_country', 'headshot_href', 'headshot_alt', 'experience_years', 'experience_display_value', 'experience_abbreviation', 'status_id', 'status_name', 'status_type', 'status_abbreviation', 'hand_type', 'hand_abbreviation', 'hand_display_value', 'draft_display_text', 'draft_round', 'draft_year', 'draft_selection', 'player_id', 'starter', 'valid', 'did_not_play', 'display_name', 'ejected', 'athlete_href', 'position_href', 'statistics_href', 'team_id', 'team_guid', 'team_uid', 'team_slug', 'team_location', 'team_name', 'team_abbreviation', 'team_display_name', 'team_short_display_name', 'team_color', 'team_alternate_color', 'is_active', 'is_all_star', 'logo_href', 'logo_dark_href', 'game_id'

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

### load_nba_stats_leaguedash {#load_nba_stats_leaguedash}

`load_nba_stats_leaguedash(family: 'str', seasons: 'int | Iterable[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Load one asset family of the `nba_stats_leaguedash` release.

`nba_stats_leaguedash` is a parameter cube: one asset per
(family, season) pair rather than one per season, so a family must be named.
The valid families are exported as
`NBA_STATS_LEAGUEDASH_FAMILIES` -- import that tuple to discover them
rather than passing a bare string; an unknown family raises `ValueError`
listing every valid value.

Column sets are family-specific (a `lineups_*` frame keys on `group_id`,
a `player_*` frame on `player_id`), so this loader documents no fixed
returns table. `player_id` / `team_id` are `Int64` in every family and
season, so cross-family joins need no dtype reconciliation.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `family` | `str` |  | Asset family, e.g. `"player_stats_advanced"`. Must be one of `NBA_STATS_LEAGUEDASH_FAMILIES`. |
| `seasons` | `int \| Iterable[int]` |  | Season, or iterable of seasons, to load. Seasons are END years (`2024` = the 2023-24 NBA season). 1996 is the earliest season on the tag; per-family coverage starts later (`lineups_*` 2008, most `player_tracking_*` 2014). A requested season the family does not publish is warned about and skipped, not an error. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe with one row per player / team / lineup per requested season for the requested family; an empty frame when no requested season is published.

**Example**

```python
from sportsdataverse.nba import load_nba_stats_leaguedash
adv = load_nba_stats_leaguedash("player_stats_advanced", seasons=2024)
print(adv.shape)

# Discover the valid families

from sportsdataverse.nba import NBA_STATS_LEAGUEDASH_FAMILIES
print([f for f in NBA_STATS_LEAGUEDASH_FAMILIES if f.startswith("player_tracking_")])

# Multi-season, pandas round-trip

drives_pd = load_nba_stats_leaguedash(
    "player_tracking_drives", seasons=range(2020, 2025), return_as_pandas=True
)

# Pipeline next step (top usage rates in 2024)

import polars as pl
usage = load_nba_stats_leaguedash("player_stats_usage", seasons=2024)
usage.sort("usg_pct", descending=True).head()
```

### load_darko_dpm {#load_darko_dpm}

`load_darko_dpm(path: 'str') -> 'pl.DataFrame'`

Parse a DARKO DPM leaderboard CSV (e.g. `2026-darko-dpm-leaderboard.csv`).

Name-keyed only (no shared player id with the model zoo) -- this is the
family `~sportsdataverse.nba.nba_model_validation.external_validity`
joins with `join="name"`. Handles two real-file quirks: a leading UTF-8
BOM (read with `encoding="utf-8-sig"`, which strips it) and
sign-prefixed integer columns (`"+7"`, not `"7"`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a DARKO DPM leaderboard CSV. |

**Returns**

Frame with schema `DARKO_DPM_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
from sportsdataverse.nba.nba_oracle_data import load_darko_dpm
oracle = load_darko_dpm(f"{oracle_dir}/2026-darko-dpm-leaderboard.csv")
print(oracle.sort("dpm", descending=True).head())
```

### load_dunks_threes_stats {#load_dunks_threes_stats}

`load_dunks_threes_stats(path: 'str') -> 'pl.DataFrame'`

Parse a Dunks & Threes counting-stats CSV (e.g. `2025_Dunks_&_Threes_Stats.csv`).

Only `ewins` (estimated wins) is kept -- the WAR-layer oracle target
the spec pairs with LEBRON's `WAR` column. WP4's `nba_war` doesn't
exist yet, so this loader is built and tested standalone (see the
plan's "WP4/WP2 dependency notes").

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a D&T counting-stats CSV. |

**Returns**

Frame with schema `DT_STATS_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
from sportsdataverse.nba.nba_oracle_data import load_dunks_threes_stats
oracle = load_dunks_threes_stats(f"{oracle_dir}/2025_Dunks_&_Threes_Stats.csv")
```

### load_epm {#load_epm}

`load_epm(path: 'str') -> 'pl.DataFrame'`

Parse a Dunks & Threes EPM CSV (`{season}_EPM_data.csv`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a D&T EPM CSV. |

**Returns**

Frame with schema `EPM_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
from sportsdataverse.nba.nba_oracle_data import load_epm
oracle = load_epm(f"{oracle_dir}/2025_EPM_data.csv")
```

### load_lebron_daily {#load_lebron_daily}

`load_lebron_daily(path: 'str') -> 'pl.DataFrame'`

Parse a LEBRON daily-snapshot CSV (e.g. `lebron_daily_2026-07-02.csv`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a LEBRON daily-snapshot CSV. |

**Returns**

Frame with schema `LEBRON_DAILY_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
import glob
from sportsdataverse.nba.nba_oracle_data import load_lebron_daily
latest = sorted(glob.glob(f"{oracle_dir}/lebron_daily_*.csv"))[-1]
oracle = load_lebron_daily(latest)
```

### load_lebron_season {#load_lebron_season}

`load_lebron_season(path: 'str') -> 'pl.DataFrame'`

Parse a LEBRON season-file CSV (e.g. `lebron-data-2026.csv`).

`seasons` is passed through as a raw string -- per-season files carry a
single year (`"2026"`); the combined all-years file carries a
multi-year window (`"2010-2013"`). Both parse with this one function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a LEBRON season CSV. |

**Returns**

Frame with schema `LEBRON_SEASON_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
from sportsdataverse.nba.nba_oracle_data import load_lebron_season
oracle = load_lebron_season(f"{oracle_dir}/lebron-data-2026.csv")
```

### load_rapm_ryan_davis {#load_rapm_ryan_davis}

`load_rapm_ryan_davis(path: 'str') -> 'pl.DataFrame'`

Parse a Ryan Davis published RAPM CSV (single-season or multi-year window).

Serves BOTH real files -- `rapm_ryan_davis.csv` (`season` like
`"2009-10"`) and `rapm_multi_ryan_davis.csv` (`season` like
`"2011-16"`, a multi-year decay window) -- since they share an
identical header. Only the combined (not per-side Off`/Def`)
rating columns are kept, matching the model zoo's combined-rating
convention (`nba_rapm`'s `rapm` column, not separate offense/defense).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | Filesystem path to a Ryan Davis RAPM CSV. |

**Returns**

Frame with schema `RAPM_ORACLE_SCHEMA`. Zero rows (with that schema) when the file has a header but no data rows.

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_oracle_data import load_rapm_ryan_davis
oracle = load_rapm_ryan_davis(f"{oracle_dir}/rapm_ryan_davis.csv")
season = oracle.filter(pl.col("season") == "2022-23")
```

### normalize_player_name {#normalize_player_name}

`normalize_player_name(name: 'str') -> 'str'`

Fold a player display name to a join-safe key.

Lower-cases, strips diacritics (`"Jokić"` -> `"jokic"` -- the real
stats.nba.com feed spells Nikola Jokic's name with the Serbian `ć`,
while the DARKO/D&T CSVs use plain ASCII), drops periods/apostrophes/
hyphens, collapses internal whitespace, and strips a trailing
Jr./Sr./II/III/IV suffix. Two names normalize equal iff they refer to
the same join key under this scheme -- it is NOT guaranteed globally
unique (rare true duplicate full names are a known, accepted residual;
`external_validity`'s `coverage_pct` surfaces the effect rather
than hiding it).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | A raw display name, e.g. `"Nikola Jokić"` or `"A.J. Green"`. |

**Returns**

The normalized key, e.g. `"nikola jokic"`, `"aj green"`. Empty string in, empty string out (never raises).

**Example**

```python
from sportsdataverse.nba.nba_oracle_data import normalize_player_name
assert normalize_player_name("Nikola Jokić") == normalize_player_name("Nikola Jokic")
assert normalize_player_name("Gary Trent Jr.") == normalize_player_name("Gary Trent")
```

### build_athlete_identity_lookup {#build_athlete_identity_lookup}

`build_athlete_identity_lookup(rosters: 'dict[int | str, dict]') -> 'dict[str, dict[str, Any]]'`

R `build_athlete_identity_lookup`: athlete_id -> identity from team rosters.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `rosters` | `dict[int \| str, dict]` |  | Mapping of team_id -> that team's raw roster payload (`wbb/team_rosters/json/{season}/{team_id}.json`). NOTE: R walks `raw$athletes` directly here (no position-bucket unwrap, unlike the rosters dataset itself). |

**Returns**

athlete_id (str) -> identity fields for `helper_wbb_player_season_stats`.

### build_nba_player_identity_lookup {#build_nba_player_identity_lookup}

`build_nba_player_identity_lookup(player_box: 'pl.DataFrame') -> 'dict[str, dict[str, Any]]'`

R `build_identity_lookup(season)`: athlete_id -> identity from the

season's already-compiled `player_box` -- the authoritative "who played
in season Y" source (ESPN's team-roster endpoint is current-only and
cannot answer that for historical seasons).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_box` | `DataFrame` |  | The season's compiled player_box frame (e.g. `nba/player_box/parquet/player_box_{season}.parquet`, or whatever the season builder just wrote for this pass). Must carry `athlete_id`; other identity columns are best-effort. |

**Returns**

athlete_id (str) -> identity fields for `helper_nba_player_season_stats`. When an athlete appears in multiple rows (multiple games), the LAST row (by frame order) wins -- mirroring R's `!duplicated(athlete_id, fromLast = TRUE)`, which keeps an athlete's most recent team within the season.

### build_play_context_shots {#build_play_context_shots}

`build_play_context_shots(possessions: 'pl.DataFrame', enhanced_pbp: 'pl.DataFrame', *, putback_seconds: 'float' = 2.0) -> 'pl.DataFrame'`

Build the per-shot frame carrying CTG's play context.

CTG assigns context **per play**, not per possession: one possession can
contain a transition miss, a halfcourt reset and a putback. This frame is the
play-level view — one row per field-goal attempt.

* `is_putback` — pbpstats `field_goal.py:112-144`: an **unassisted 2-point**
  attempt whose preceding event is a **real offensive rebound by the same
  player**, within `putback_seconds`. A three is never a putback.
* `is_second_chance_shot` — the shot follows an offensive rebound earlier in
  the same possession.
* `shot_context` — `transition` / `putback` / `halfcourt`. **Transition
  wins over putback**, reproducing CTG exactly: "if a team comes down in
  transition and misses a shot but gets a putback, that putback is classified
  as part of the overall transition event."

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Frame from `add_transition` (needs `is_transition`). |
| `enhanced_pbp` | `DataFrame` |  | The enhanced PBP frame the possessions were built from. |
| `putback_seconds` | `float` | `2.0` | Rebound-to-shot window. Default 2.0 (pbpstats). |

**Returns**

Polars DataFrame with schema `PLAY_CONTEXT_SHOTS_SCHEMA` — one row per field-goal attempt. Empty input returns the zero-row schema.

**Example**

```python
shots = build_play_context_shots(poss, pbp)
print(shots.group_by("shot_context").len())
print(shots.filter(pl.col("is_putback") == True).height)
```

### build_possession_shooting {#build_possession_shooting}

`build_possession_shooting(enhanced_pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Build the per-shooter companion frame from an enhanced play-by-play DataFrame.

Companion to `build_possessions`: instead of one team-level row per
possession, emits one row per distinct shooter (`player_id`) per
possession, with their own `fg2a/fg2m/fg3a/fg3m/fta/ftm` counts. Shares
the same possession-group traversal as `build_possessions` via
assemble` — the two frames are always built from a single
consistent pass over the play-by-play. Consumed by WP2's luck-adjusted
shooting response.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enhanced_pbp` | `DataFrame` |  | Polars DataFrame with schema `ENHANCED_PBP_SCHEMA` (from `~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`). An empty or malformed frame returns a zero-row frame with `POSSESSION_SHOOTING_SCHEMA` — never raises. |

**Returns**

Polars DataFrame with schema `POSSESSION_SHOOTING_SCHEMA`. One row per `(possession_number, player_id)` pair. Events with `person_id == 0` are skipped (unattributable to a shooter — they still count toward `build_possessions`' team-level totals). Per-possession sums of the six shooting columns match the corresponding `build_possessions` columns exactly.

**Example**

```python
import json, pathlib
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_possessions import build_possession_shooting

payload = json.loads(pathlib.Path("playbyplayv3.json").read_text())
pbp = enhanced_pbp_from_payload(payload)
sh = build_possession_shooting(pbp)
print(sh.shape, sh.schema["player_id"])

# Per-player shooting totals

import polars as pl
totals = sh.group_by("player_id").agg(
    pl.col("fg3m").sum(), pl.col("ftm").sum()
)
print(totals.head())
```

### espn_nba_pbp {#espn_nba_pbp}

`espn_nba_pbp(game_id: 'int', raw=False, **kwargs) -> 'Dict'`

espn_nba_pbp() - Pull the game by id - Data from API endpoints - `nba/playbyplay`, `nba/summary`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from nba_schedule(). |
| `raw` | `bool` | `False` | If True, returns the raw json from the API endpoint. If False, returns a cleaned dictionary of datasets. |

**Returns**

Dictionary of game data with keys - "gameId", "plays", "winprobability", "boxscore", "header", "broadcasts", "videos", "playByPlaySource", "standings", "leaders", "seasonseries", "timeouts", "pickcenter", "againstTheSpread", "odds", "predictor", "espnWP", "gameInfo", "season"

**Example**

```python
from sportsdataverse.nba import espn_nba_pbp
pbp = espn_nba_pbp(game_id=401585183)
print(list(pbp.keys()))

# Pull only the raw ESPN summary payload (skip cleaning)

raw_pbp = espn_nba_pbp(game_id=401585183, raw=True)

# Pipeline next step (load plays into a polars DataFrame)

import polars as pl
pbp = espn_nba_pbp(game_id=401585183)
plays_df = pl.from_dicts(pbp["plays"])
```

### nba_pbp_disk {#nba_pbp_disk}

`nba_pbp_disk(game_id, path_to_json)`

Load a previously cached ESPN NBA summary JSON for a game from disk.

Reads `{path_to_json}/{game_id}.json`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | ESPN game / event identifier. |
| `path_to_json` | `str` |  | Directory containing the cached JSON file. |

**Returns**

Parsed JSON contents.

**Example**

```python
from sportsdataverse.nba import nba_pbp_disk
pbp = nba_pbp_disk(game_id=401585183, path_to_json="./cache")
print(list(pbp.keys()))
```

### nba_v3_to_v2_pbp {#nba_v3_to_v2_pbp}

`nba_v3_to_v2_pbp(pbp_v3: 'dict', box_v3: 'dict', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Convert a v3 `playbyplayv3` payload into the full v2-schema pbp frame.

Ports hoopR's `.v3_to_v2_format()` (`R/nba_stats_pbp.R` lines
210-810) to polars: the v3 feed (`stats.nba.com` `playbyplayv3`) is
reshaped into the older v2 schema that the committed hoopR-nba-stats-data
dataset carries and that `pbpstats`' `stats_nba` provider consumes.
This is a pure, network-free function -- both payloads must already be
fetched (e.g. via `nba_stats_playbyplayv3` / `nba_stats_boxscoretraditionalv3`).

Pipeline:

1. Build the per-`person_id` roster from `box_v3`
   (build_roster`) and recover `player2_id`/`player3_id`
   (assist/block/steal/sub-in/jump) from `pbp_v3` (
   extract_secondary_players`).
2. Drop the standalone block/steal rows consolidated into their parent
   Missed Shot / Turnover (is_dropped_block_steal`) -- the only
   row-count change versus the raw v3 action list.
3. Derive `event_type`/`event_action_type` from the module's lookup
   tables, split `description` by `location` into home/visitor/
   neutral, forward-fill the running score, and enrich `player2`/
   `player3` from the roster **by id** (see secondary_fields`
   for the deliberate divergence from hoopR's name-based re-resolution).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_v3` | `dict` |  | Raw `playbyplayv3` dict (`nba_stats_playbyplayv3` / `wnba_stats_playbyplayv3` payload shape); actions live at `pbp_v3["game"]["actions"]`. |
| `box_v3` | `dict` |  | Raw `boxscoretraditionalv3` dict, passed through to build_roster`. |
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame` instead of `polars.DataFrame`. |

**Returns**

Polars (or pandas) DataFrame with the full v2 schema (game/event identifiers, event/action type codes, home/visitor/neutral descriptions, forward-filled score + margin + leader, per-player columns for players 1-3, and the v3 passthrough columns). Empty or malformed input returns a zero-row frame with the same schema (never raises).

**Example**

```python
from sportsdataverse.nba.nba_v3_v2_adapter import nba_v3_to_v2_pbp
from sportsdataverse.nba.nba_stats import nba_stats_playbyplayv3, nba_stats_boxscoretraditionalv3

pbp_v3 = nba_stats_playbyplayv3(game_id="0022300001", return_parsed=False)
box_v3 = nba_stats_boxscoretraditionalv3(game_id="0022300001", return_parsed=False)
df = nba_v3_to_v2_pbp(pbp_v3, box_v3)
print(df.shape, df.columns)

# Pandas output

df_pd = nba_v3_to_v2_pbp(pbp_v3, box_v3, return_as_pandas=True)
print(type(df_pd))

# Pipeline next step (feed a pbpstats-style consumer)

df.filter(pl.col("event_type") == "1").select("player1_name", "player2_name")
```

### year_to_season {#year_to_season}

`year_to_season(year)`

Convert a season START year (e.g. 2023) to the NBA's hyphenated label

(e.g. `"2023-24"`).

Callers working in the end-year convention pass `end_year - 1` (e.g.
`year_to_season(most_recent_nba_season() - 1)`).

Handles century rollover (1999 -> `"1999-00"`) and zero-pads the
second half of the label.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `int` |  | The starting calendar year of the season (e.g. 2023 for the 2023-24 season). |

**Returns**

NBA-style season label.

**Example**

```python
from sportsdataverse.nba import year_to_season
label = year_to_season(2023)
print(label)  # "2023-24"

# Century rollover

print(year_to_season(1999))  # "1999-00"
```

### nba_player_crosswalk {#nba_player_crosswalk}

`nba_player_crosswalk(season: 'Optional[int]' = None, min_confidence: 'float' = 0.92, *, return_as_pandas: 'bool' = False, strict: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the NBA cross-source player crosswalk (ESPN / NBA Stats / Fox).

One row per ESPN athlete per team. `match_method` / `match_confidence`
describe the **Stats API** match (normalized exact name, then
Jaro-Winkler with jersey and DOB tiebreaks); Fox contributes
`fox_athlete_id` only.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year per hoopR convention. Defaults to the most recent NBA season. |
| `min_confidence` | `float` | `0.92` | Jaro-Winkler floor for fuzzy matches (R default 0.92). |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |
| `strict` | `bool` | `False` | Raise on the first failed per-team ESPN or Fox roster fetch (a 404 is still skipped) instead of skipping isolated failures. Default `False` matches the R producers; a provider whose every item failed raises either way. An item the host *answered* -- including a 404 -- counts as answered. |

**Returns**

`pl.DataFrame` (or pandas), one row per ESPN athlete, 21 columns.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season year. |
| `espn_team_id` | integer | ESPN team id (canonical key). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `player_name` | character | Player name. |
| `espn_athlete_id` | character | ESPN athlete id. |
| `espn_full_name` | character | ESPN full name. |
| `espn_jersey` | character | ESPN jersey number. |
| `espn_position` | character | ESPN position abbreviation. |
| `nba_player_id` | character | NBA Stats API (stats.nba.com) player id as a string, matched to the ESPN athlete within the same team by normalized exact name, then Jaro-Winkler fuzzy name match (min_confidence, default 0.92) with jersey and birth-date tiebreaks; null when the athlete had no Stats match. |
| `nba_player_name` | character | Player name from the NBA Stats commonteamroster row matched to the ESPN athlete; null when the athlete had no Stats match. |
| `nba_jersey_num` | character | Jersey number as a string from the NBA Stats commonteamroster row matched to the ESPN athlete; null when the athlete had no Stats match. |
| `nba_position` | character | Position from the NBA Stats commonteamroster row matched to the ESPN athlete; null when the athlete had no Stats match. |
| `fox_athlete_id` | character | Fox athlete id (NA if unmatched). |
| `fox_player` | character | Fox player name (NA if unmatched). |
| `fox_jersey` | character | Fox jersey number (NA if unmatched). |
| `fox_position_group` | character | Fox position group label (NA if unmatched). |
| `yahoo_player_id` | character | Yahoo player id (NA placeholder). |
| `yahoo_player_name` | character | Yahoo player name (NA placeholder). |
| `match_method` | character | Combination of matched sources, e.g. "fox+bart" / "fox_only" / "bart_only" / "espn_only". |
| `match_confidence` | double | Jaro-Winkler score or 1 for exact (NA if none). |
| `match_keys` | character | NA (reserved for future use). |

**Example**

```python
from sportsdataverse.nba import nba_player_crosswalk
df = nba_player_crosswalk(season=2026)
print(df["match_method"].value_counts())

# Tighten the fuzzy floor

strict = nba_player_crosswalk(season=2026, min_confidence=0.97)

# Pipeline next step (one line)

df.filter(pl.col("match_method") == "fuzzy_jw").head()
```

### nba_schedule_crosswalk {#nba_schedule_crosswalk}

`nba_schedule_crosswalk(season: 'Optional[int]' = None, *, stats_games: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, strict: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the NBA cross-source schedule crosswalk (ESPN / NBA Stats).

One row per game. Both sides reduce to the Eastern-Time game date before
joining on `(game_date, home_espn_team_id, away_espn_team_id)`. The
Stats CDN serves the current season only, so the live builder is
effectively current-season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year per hoopR convention. Defaults to the most recent NBA season. |
| `stats_games` | `Optional[DataFrame]` | `None` | Pre-fetched Stats schedule frame; `None` fetches live. |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |
| `strict` | `bool` | `False` | Raise on the first failed per-date ESPN scoreboard fetch (a 404 is still skipped) instead of skipping isolated failures. Default `False` matches the R producers; a provider whose every item failed raises either way. An item the host *answered* -- including a 404 -- counts as answered. |

**Returns**

`pl.DataFrame` (or pandas) with `SCHEDULE_COLUMNS`.

**Example**

```python
from sportsdataverse.nba import nba_schedule_crosswalk
df = nba_schedule_crosswalk(season=2026)
print(df["match_method"].value_counts())

# Pipeline next step (one line)

df.filter(pl.col("match_method") == "both").select("espn_game_id", "nba_game_id").head()
```

### nba_team_crosswalk {#nba_team_crosswalk}

`nba_team_crosswalk(season: 'Optional[int]' = None, *, stats: 'Optional[pl.DataFrame]' = None, fox: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the NBA cross-source team crosswalk (ESPN / NBA Stats / Fox).

One row per ESPN team, keyed on `espn_team_id`. ESPN and Stats team
endpoints are current-season snapshots, so `season` is a stamp;
historical relocations are not back-modelled.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year per hoopR convention (`2026` = 2025-26). Defaults to the most recent NBA season. |
| `stats` | `Optional[DataFrame]` | `None` | Pre-fetched Stats team directory (`espn_team_id` + `nba_team_*`). `None` derives it from `nba_stats_leaguestandingsv3` joined to ESPN on the normalized team nickname. |
| `fox` | `Optional[DataFrame]` | `None` | Pre-fetched Fox directory. `None` fetches live. |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |

**Returns**

`pl.DataFrame` (or pandas), one row per ESPN team, with `TEAM_COLUMNS`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season year. |
| `espn_team_id` | integer | ESPN team id (canonical key). |
| `espn_abbreviation` | character | ESPN abbreviation. |
| `espn_display_name` | character | ESPN display name (school + mascot). |
| `espn_short_name` | character | ESPN short name. |
| `espn_location` | character | ESPN school/location only. |
| `espn_mascot` | character | ESPN mascot/nickname. |
| `nba_team_id` | character | NBA Stats API (stats.nba.com) team id as a string, attached to the ESPN team row on espn_team_id after the Stats team nickname is matched to ESPN's short_name; null when no Stats team matched the ESPN team. |
| `nba_team_abbreviation` | character | NBA Stats team tricode, taken from nba_stats_leaguegamelog's team_abbreviation because leaguestandingsv3 publishes none; null when no Stats team matched the ESPN team. |
| `nba_team_name` | character | Full NBA Stats team name built as team_city plus team_name (city then nickname) from nba_stats_leaguestandingsv3; null when no Stats team matched the ESPN team. |
| `nba_team_city` | character | Team city (team_city) from nba_stats_leaguestandingsv3; null when no Stats team matched the ESPN team. |
| `nba_team_slug` | character | URL slug for the team (team_slug) from nba_stats_leaguestandingsv3; null when no Stats team matched the ESPN team. |
| `nba_conference` | character | Team's conference as NBA Stats labels it (conference) from nba_stats_leaguestandingsv3; null when no Stats team matched the ESPN team. |
| `nba_division` | character | Team's division as NBA Stats labels it (division) from nba_stats_leaguestandingsv3; null when no Stats team matched the ESPN team. |
| `fox_team_id` | character | Fox Bifrost team id (NA if unmatched). |
| `fox_team_name` | character | Fox team name (NA if unmatched). |
| `yahoo_team_id` | character | Yahoo team id (NA placeholder). |
| `yahoo_team_abbreviation` | character | Yahoo abbreviation (NA placeholder). |
| `yahoo_team_name` | character | Yahoo team name (NA placeholder). |
| `match_method` | character | Combination of matched sources, e.g. "fox+bart" / "fox_only" / "bart_only" / "espn_only". |
| `match_confidence` | double | Jaro-Winkler score or 1 for exact (NA if none). |

**Example**

```python
from sportsdataverse.nba import nba_team_crosswalk
df = nba_team_crosswalk(season=2026)
print(df.shape)

# Offline with a pre-fetched Stats frame

df = nba_team_crosswalk(season=2026, stats=my_stats, fox=my_fox)

# Pipeline next step (one line)

df.select("espn_team_id", "nba_team_id", "match_method").head()
```
