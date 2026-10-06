---
title: "WNBA — additional Python functions — Play-by-play processing"
sidebar_label: "Play-by-play processing"
sidebar_position: 4
description: "WNBA — additional Python functions — Play-by-play processing — function reference in sdv-py, the SportsDataverse Python package."
---
# WNBA — additional Python functions — Play-by-play processing

### build_athlete_identity_lookup {#build_athlete_identity_lookup}

`build_athlete_identity_lookup(rosters: 'dict[int | str, dict]') -> 'dict[str, dict[str, Any]]'`

R `build_athlete_identity_lookup`: athlete_id -> identity from team rosters.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `rosters` | `dict[int \| str, dict]` |  | Mapping of team_id -> that team's raw roster payload (`wbb/team_rosters/json/{season}/{team_id}.json`). NOTE: R walks `raw$athletes` directly here (no position-bucket unwrap, unlike the rosters dataset itself). |

**Returns**

athlete_id (str) -> identity fields for `helper_wbb_player_season_stats`.

### build_wnba_season_wp {#build_wnba_season_wp}

`build_wnba_season_wp(season: 'int', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

A WNBA season's play-by-play with win-probability columns joined in.

Loads the season's play-by-play, schedule, and team boxscores, builds a
leakage-free weekly as-of pregame anchor per game from the WNBA ratings
engine (`league_id="10"`), scores every play through the bundled
in-game win-probability artifact, and returns the full `load_wnba_pbp`
frame with `pregame_home_prob` + `home_win_prob` appended -- the
enrich-in-place shape that overwrites the season's
`play_by_play_<season>.parquet` release asset.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season year (e.g. `2024`); bounded by `load_wnba_pbp` release availability. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The season's `load_wnba_pbp` frame (every column preserved) with the two WP columns `pregame_home_prob` + `home_win_prob` appended (both `Float64`), sorted by `game_id` then `game_play_number`.

**Example**

```python
from sportsdataverse.wnba import build_wnba_season_wp
wp = build_wnba_season_wp(2024)
wp.select("game_id", "game_play_number", "home_win_prob").head()

# Pandas output

wp_pd = build_wnba_season_wp(2024, return_as_pandas=True)
```

### espn_wnba_pbp {#espn_wnba_pbp}

`espn_wnba_pbp(game_id: 'int', raw=False, **kwargs) -> 'Dict'`

espn_wnba_pbp() - Pull the game by id. Data from API endpoints - `wnba/playbyplay`, `wnba/summary`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from wnba_schedule(). |
| `raw` |  | `False` |  |

**Returns**

Dictionary of game data with keys - "gameId", "plays", "winprobability", "boxscore", "header", "broadcasts", "videos", "playByPlaySource", "standings", "leaders", "seasonseries", "timeouts", "pickcenter", "againstTheSpread", "odds", "predictor", "espnWP", "gameInfo", "season"

**Example**

```python
from sportsdataverse.wnba import espn_wnba_pbp
game = espn_wnba_pbp(game_id=401620238)  # 2024 WNBA Finals Game 1
list(game.keys())  # ['gameId', 'plays', 'winprobability', ...]

# Inspect the parsed plays and a header summary

import polars as pl
plays = pl.DataFrame(game["plays"])
print(plays.shape)
print(plays.select(["period", "time", "type.text", "text"]).head(5))

# Fetch the unparsed payload for custom downstream parsing

raw = espn_wnba_pbp(game_id=401620238, raw=True)
sorted(raw.keys())[:5]  # raw ESPN summary keys, no flattening
```

### wnba_enhanced_pbp {#wnba_enhanced_pbp}

`wnba_enhanced_pbp(game_id: 'str', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Return a normalised enhanced play-by-play frame for a WNBA game.

Fetches the raw `playbyplayv3` payload from `stats.wnba.com` via
`~sportsdataverse.wnba.wnba_stats.wnba_stats_playbyplayv3` then
delegates all transformation to the league-agnostic
`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`
core with `league_id="10"`.  Never raises on malformed or empty
payloads — returns a zero-row frame instead.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` |  | WNBA game identifier string (e.g. `"1022400001"`). |
| `return_as_pandas` | `bool` | `False` | If `True`, convert the result to a `pandas.DataFrame` before returning. |

**Returns**

Polars (or pandas) DataFrame with schema `sportsdataverse.nba.nba_enhanced_pbp.ENHANCED_PBP_SCHEMA`. Key columns include `game_id` (Utf8), `action_number` (Int64), `period` (Int64), `seconds_remaining` (Float64), `team_id` (Int64), `person_id` (Int64), `is_substitution` (Boolean), and one Boolean flag per event type.

**Example**

```python
from sportsdataverse.wnba.wnba_engine import wnba_enhanced_pbp
df = wnba_enhanced_pbp("1022400001")
print(df.shape)

# Pandas output

df_pd = wnba_enhanced_pbp("1022400001", return_as_pandas=True)
print(type(df_pd))

# Filter substitution events

subs = df.filter(df["is_substitution"] == True)  # noqa: E712
print(subs.select(["period", "seconds_remaining", "person_id"]))
```

### wnba_on_court {#wnba_on_court}

`wnba_on_court(game_id: 'str', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Return the rotation-keyed on-court player frame for a WNBA game.

Makes three network calls (play-by-play v3, game rotation,
box-score traditional v3), infers on-court rosters from the rotation
stints via
`~sportsdataverse.nba.nba_lineups.players_on_court_from_rotation`,
and returns one row per PBP action with ten Int64 player-ID columns
(`home_player_1..5` / `away_player_1..5`).  All transformation is
performed by the shared `nba/` core with `league_id="10"` forwarded
to the rotation endpoint.  Never raises on malformed payloads.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` |  | WNBA game identifier string (e.g. `"1022400001"`). |
| `return_as_pandas` | `bool` | `False` | If `True`, convert the result to a `pandas.DataFrame` before returning. |

**Returns**

Polars (or pandas) DataFrame with one row per PBP action and columns `home_player_1` … `home_player_5`, `away_player_1` … `away_player_5` (all Int64), plus the `action_number` join key.

**Example**

```python
from sportsdataverse.wnba.wnba_engine import wnba_on_court
oc = wnba_on_court("1022400001")
print(oc.select(["action_number", "home_player_1"]).head())

# Pandas output

oc_pd = wnba_on_court("1022400001", return_as_pandas=True)
print(type(oc_pd))

# Join on enhanced PBP

from sportsdataverse.wnba.wnba_engine import wnba_enhanced_pbp
enh = wnba_enhanced_pbp("1022400001")
joined = enh.join(oc, on="action_number", how="left")
```

### wnba_pbp_disk {#wnba_pbp_disk}

`wnba_pbp_disk(game_id, path_to_json)`

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  |  |
| `path_to_json` |  |  |  |

### wnba_play_context {#wnba_play_context}

`wnba_play_context(game_id: 'str', *, transition_seconds: 'float' = 6.0, transition_variant: 'str' = 'hoop_math', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Return a WNBA game's possessions with the full CTG play-context surface.

WNBA sibling of
`~sportsdataverse.nba.nba_play_context.nba_play_context` — the
Cleaning the Glass recreation (possession start-type taxonomy + the
halfcourt / transition / putback contexts + CTG's garbage-time and heave
filters). One network call (`playbyplayv3` on `stats.wnba.com`); every
transformation is done by the league-agnostic
`~sportsdataverse.nba.nba_play_context.add_play_context` core, so
there is **no WNBA-specific classification logic** to drift.

Two caveats worth stating plainly:

* **CTG is NBA-only.** There is no published WNBA play-context table to
  calibrate against, so `transition_seconds` inherits the NBA's fitted
  6.0 s default. The WNBA fixtures land inside the NBA's transition-frequency
  gate at that value (`tests/wnba/test_wnba_play_context_shim.py`), which
  is a sanity check on the shared engine — not evidence that 6.0 s is the
  *right* WNBA cutoff. Re-fit it if a WNBA oracle ever appears.
* Shot-zone boundaries are league-agnostic (feet from the rim), and the
  corner-three test uses the same legacy coordinates, which the WNBA feed
  also ships.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` |  | WNBA game identifier (e.g. `"1022400001"`). |
| `transition_seconds` | `float` | `6.0` | Transition initial-play cutoff, in seconds. |
| `transition_variant` | `str` | `'hoop_math'` | See `~sportsdataverse.nba.nba_play_context.add_transition`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The possession frame (`POSSESSIONS_SCHEMA`) plus `~sportsdataverse.nba.nba_play_context.PLAY_CONTEXT_POSSESSIONS_SCHEMA`. Empty or malformed payloads return a zero-row frame — never raises on payload content.

**Example**

```python
from sportsdataverse.wnba.wnba_engine import wnba_play_context
poss = wnba_play_context("1022400001")
print(poss["possession_start_type_ctg"].value_counts())

# Transition rate (CTG's default filtered view)

import polars as pl
clean = poss.filter(
    (pl.col("is_garbage_time") == False)  # noqa: E712
    & (pl.col("is_heave_possession") == False)  # noqa: E712
)
print(clean["is_transition"].mean())
```

### wnba_possessions {#wnba_possessions}

`wnba_possessions(game_id: 'str', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Return the possession-level lineup stint matrix for a WNBA game.

Builds possessions from the enhanced PBP via
`~sportsdataverse.nba.nba_possessions.build_possessions`, resolves
on-court rosters via
`~sportsdataverse.nba.nba_lineups.players_on_court_from_rotation`,
then attaches the 5v5 lineups via
`~sportsdataverse.nba.nba_possessions.attach_possession_lineups`.
All transformation is performed by the shared `nba/` cores — no WNBA-
specific logic.  Never raises on malformed payloads.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` |  | WNBA game identifier string (e.g. `"1022400001"`). |
| `return_as_pandas` | `bool` | `False` | If `True`, convert the result to a `pandas.DataFrame` before returning. |

**Returns**

Polars (or pandas) DataFrame with schema combining `POSSESSIONS_SCHEMA` and ten lineup columns: `off_player_1` … `off_player_5`, `def_player_1` … `def_player_5` (all Int64). One row per possession. Empty or malformed inputs return a zero-row frame.

**Example**

```python
from sportsdataverse.wnba.wnba_engine import wnba_possessions
poss = wnba_possessions("1022400001")
print(poss.shape)

# Pandas output

poss_pd = wnba_possessions("1022400001", return_as_pandas=True)
print(type(poss_pd))

# Total points check

total = int(poss["points"].sum())
print(f"Total points scored: {total}")
```

### wnba_rapm_from_games {#wnba_rapm_from_games}

`wnba_rapm_from_games(game_ids: 'Sequence[str]', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Compute per-player RAPM estimates over a sequence of WNBA games.

Iterates *game_ids*, builds the possession-level stint matrix for each
via `wnba_possessions`, concatenates the results, and fits a
ridge-regression RAPM model via
`~sportsdataverse.nba.nba_rapm.nba_rapm`.  Games whose possession
frame is empty (e.g. a malformed payload) are silently skipped.  Returns
a zero-row frame when no valid possessions are found.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_ids` | `Sequence[str]` |  | Sequence of WNBA game identifier strings. |
| `return_as_pandas` | `bool` | `False` | If `True`, convert the result to a `pandas.DataFrame` before returning. |

**Returns**

Polars (or pandas) DataFrame with one row per player and columns `player_id` (Int64), `o_rapm` (Float64), `d_rapm` (Float64), `rapm` (Float64), `off_poss` (Int64), `def_poss` (Int64).

**Example**

```python
from sportsdataverse.wnba.wnba_engine import wnba_rapm_from_games
rapm = wnba_rapm_from_games(["1022400001", "1022400003"])
print(rapm.sort("rapm", descending=True).head())

# Pandas output

rapm_pd = wnba_rapm_from_games(["1022400001"], return_as_pandas=True)
print(type(rapm_pd))

# Multi-season aggregation

import polars as pl
game_ids = pl.read_parquet("wnba_schedule.parquet")["game_id"].to_list()
rapm = wnba_rapm_from_games(game_ids)
print(rapm.sort("rapm", descending=True).head(10))
```
