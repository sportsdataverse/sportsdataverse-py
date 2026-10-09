# Package — additional Python functions — Play-by-play processing

> Package — additional Python functions — Play-by-play processing — function reference in sdv-py, the SportsDataverse Python package.

### nbagl_enhanced_pbp {#nbagl_enhanced_pbp}

`nbagl_enhanced_pbp(game_id: 'str', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Return a normalised enhanced play-by-play frame for a G-League game.

Fetches the raw `playbyplayv3` payload from `stats.nba.com` via
`~sportsdataverse.nba.nba_stats.nba_stats_playbyplayv3` then
delegates all transformation to the league-agnostic
`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`
core with `league_id="20"`.  Never raises on malformed or empty
payloads — returns a zero-row frame instead.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` |  | G-League game identifier string (e.g. `"2022400003"`). |
| `return_as_pandas` | `bool` | `False` | If `True`, convert the result to a `pandas.DataFrame` before returning. |

**Returns**

Polars (or pandas) DataFrame with schema `sportsdataverse.nba.nba_enhanced_pbp.ENHANCED_PBP_SCHEMA`. Key columns include `game_id` (Utf8), `action_number` (Int64), `period` (Int64), `seconds_remaining` (Float64), `team_id` (Int64), `person_id` (Int64), `is_substitution` (Boolean), and one Boolean flag per event type.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nbagl.nbagl_engine import nbagl_enhanced_pbp
df = nbagl_enhanced_pbp("2022400003")
print(df.shape)

# Pandas output

df_pd = nbagl_enhanced_pbp("2022400003", return_as_pandas=True)
print(type(df_pd))

# Filter substitution events

subs = df.filter(df["is_substitution"] == True)  # noqa: E712
print(subs.select(["period", "seconds_remaining", "person_id"]))
```

### nbagl_on_court {#nbagl_on_court}

`nbagl_on_court(game_id: 'str', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Return the rotation-keyed on-court player frame for a G-League game.

Makes three network calls (play-by-play v3, game rotation,
box-score traditional v3), infers on-court rosters from the rotation
stints via
`~sportsdataverse.nba.nba_lineups.players_on_court_from_rotation`,
and returns one row per PBP action with ten Int64 player-ID columns
(`home_player_1..5` / `away_player_1..5`).  All transformation is
performed by the shared `nba/` core with `league_id="20"` forwarded
to the rotation endpoint.  Never raises on malformed payloads.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` |  | G-League game identifier string (e.g. `"2022400003"`). |
| `return_as_pandas` | `bool` | `False` | If `True`, convert the result to a `pandas.DataFrame` before returning. |

**Returns**

Polars (or pandas) DataFrame with one row per PBP action and columns `home_player_1` … `home_player_5`, `away_player_1` … `away_player_5` (all Int64), plus the `action_number` join key.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nbagl.nbagl_engine import nbagl_on_court
oc = nbagl_on_court("2022400003")
print(oc.select(["action_number", "home_player_1"]).head())

# Pandas output

oc_pd = nbagl_on_court("2022400003", return_as_pandas=True)
print(type(oc_pd))

# Join on enhanced PBP

from sportsdataverse.nbagl.nbagl_engine import nbagl_enhanced_pbp
enh = nbagl_enhanced_pbp("2022400003")
joined = enh.join(oc, on="action_number", how="left")
```

### nbagl_possessions {#nbagl_possessions}

`nbagl_possessions(game_id: 'str', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Return the possession-level lineup stint matrix for a G-League game.

Builds possessions from the enhanced PBP via
`~sportsdataverse.nba.nba_possessions.build_possessions`, resolves
on-court rosters via
`~sportsdataverse.nba.nba_lineups.players_on_court_from_rotation`,
then attaches the 5v5 lineups via
`~sportsdataverse.nba.nba_possessions.attach_possession_lineups`.
All transformation is performed by the shared `nba/` cores — no
G-League-specific logic.  Never raises on malformed payloads.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` |  | G-League game identifier string (e.g. `"2022400003"`). |
| `return_as_pandas` | `bool` | `False` | If `True`, convert the result to a `pandas.DataFrame` before returning. |

**Returns**

Polars (or pandas) DataFrame with schema combining `POSSESSIONS_SCHEMA` and ten lineup columns: `off_player_1` … `off_player_5`, `def_player_1` … `def_player_5` (all Int64). One row per possession. Empty or malformed inputs return a zero-row frame.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nbagl.nbagl_engine import nbagl_possessions
poss = nbagl_possessions("2022400003")
print(poss.shape)

# Pandas output

poss_pd = nbagl_possessions("2022400003", return_as_pandas=True)
print(type(poss_pd))

# Total points check

total = int(poss["points"].sum())
print(f"Total points scored: {total}")
```

### nbagl_rapm_from_games {#nbagl_rapm_from_games}

`nbagl_rapm_from_games(game_ids: 'Sequence[str]', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Compute per-player RAPM estimates over a sequence of G-League games.

Iterates *game_ids*, builds the possession-level stint matrix for each
via `nbagl_possessions`, concatenates the results, and fits a
ridge-regression RAPM model via
`~sportsdataverse.nba.nba_rapm.nba_rapm`.  Games whose possession
frame is empty (e.g. a malformed payload) are silently skipped.  Returns
a zero-row frame when no valid possessions are found.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_ids` | `Sequence[str]` |  | Sequence of G-League game identifier strings. |
| `return_as_pandas` | `bool` | `False` | If `True`, convert the result to a `pandas.DataFrame` before returning. |

**Returns**

Polars (or pandas) DataFrame with one row per player and columns `player_id` (Int64), `o_rapm` (Float64), `d_rapm` (Float64), `rapm` (Float64), `off_poss` (Int64), `def_poss` (Int64).

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nbagl.nbagl_engine import nbagl_rapm_from_games
rapm = nbagl_rapm_from_games(["2022400003", "2022400009"])
print(rapm.sort("rapm", descending=True).head())

# Pandas output

rapm_pd = nbagl_rapm_from_games(["2022400003"], return_as_pandas=True)
print(type(rapm_pd))

# Multi-season aggregation

import polars as pl
game_ids = pl.read_parquet("nbagl_schedule.parquet")["game_id"].to_list()
rapm = nbagl_rapm_from_games(game_ids)
print(rapm.sort("rapm", descending=True).head(10))
```
