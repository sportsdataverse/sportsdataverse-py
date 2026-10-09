# NBA — additional Python functions — Analytics: nba_shot–zone_value

> NBA — additional Python functions — Analytics: nba_shot–zone_value — function reference in sdv-py, the SportsDataverse Python package.

### nba_shot_value {#nba_shot_value}

`nba_shot_value(player_ids: "'list[int]'", season: 'str', *, league_id: 'str' = '00', include_context: 'bool' = False, return_as_pandas: 'bool' = False) -> "'dict[str, Union[pl.DataFrame, pd.DataFrame]]'"`

One-call shot-value spine: fetch, score, and run all five models.

Fetches each player's `shotchartdetail`, scores per-shot expected points
from the free `LeagueAverages` zone table, and returns the scored shots
plus shooter talent, selection quality, and zone-value maps (and the
defender/shot-clock context tables when `include_context=True`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_ids` | `list[int]` |  | Player ids to fetch. |
| `season` | `str` |  | Season string, e.g. `"2022-23"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA, `"10"` WNBA, `"20"` G-League. |
| `include_context` | `bool` | `False` | Also fetch + return the `playerdashptshots` defender/shot-clock context tables, once per player and team his fetched shots came from. |
| `return_as_pandas` | `bool` | `False` | Return pandas frames instead of polars. |

**Returns**

`{"shots", "talent", "selection", "zones"}` (plus `"context"` when requested). An empty fetch returns a dict of zero-row frames.

**Example**

```python
from sportsdataverse.nba import nba_shot_value
out = nba_shot_value([201939], "2022-23")
out["talent"].head()
```

### nba_shot_value_lineups {#nba_shot_value_lineups}

`nba_shot_value_lineups(group_id: 'str', season: 'str', *, team_id: 'int', league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Scored per-shot frame for one 5-man lineup (`shotchartlineupdetail`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `group_id` | `str` |  | The 5-man lineup group id (dash-joined player ids); kept `Utf8`. |
| `season` | `str` |  | Season string, e.g. `"2022-23"`. |
| `team_id` | `int` |  | The lineup's team id. |
| `league_id` | `str` | `'00'` | `"00"` NBA, `"10"` WNBA, `"20"` G-League. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The lineup's shots scored by `score_shot_xpoints` (with `xpoints`). Empty fetch returns the augmented zero-row schema.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_shot_value_lineups
df = nba_shot_value_lineups("201939-202691-...", "2022-23", team_id=1610612744)
```

### nba_team_clutch {#nba_team_clutch}

`nba_team_clutch(season: 'int', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Opponent-agnostic clutch skill (shrunk clutch net-rating delta) per team.

Loads the season's clutch net rating (`nba_stats_leaguedashteamclutch`)
and full-game net baseline (`nba_stats_leaguedashteamstats`), computes
`clutch_delta`, and applies `shrink_clutch`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | End year of the season (e.g. `2024` for 2023-24). |
| `league_id` | `str` | `'00'` | `"00"` NBA / `"10"` WNBA / `"20"` G-League. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per team: `season, team_id, clutch_net_rating, adj_net_rtg, clutch_delta, clutch_skill_shrunk, clutch_poss`. Empty input returns that schema with zero rows.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba.nba_clutch import nba_team_clutch
skill = nba_team_clutch(2024)
skill.sort("clutch_skill_shrunk", descending=True).head()
```

### nba_tracking_drive_value {#nba_tracking_drive_value}

`nba_tracking_drive_value(seasons: "'int | str | list'", *, league_id: 'str' = '00', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Drive value over expected + rim-pressure, per player-season.

Fetches the `Drives` `leaguedashptstats` measure and computes
`drive_pts_oe = drive_pts - drives * bucket_pts_per_drive`. `rim_pressure`
is the z-score of `drive_fta / drives` within the player's role bucket
(a proxy for foul-drawing pressure independent of scoring efficiency).
`drive_ast`/`drive_tov` are passed through unchanged.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to the fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute the baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, gp:Int64, min:Float64, drives:Float64, drive_pts:Float64, drive_baseline_rate:Float64, drive_expected:Float64, drive_pts_oe:Float64, drive_pts_oe_per_36:Float64, drive_fta:Float64, rim_pressure:Float64, drive_ast:Float64, drive_tov:Float64, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_tracking_drive_value
df = nba_tracking_drive_value(2024)
print(df.sort("drive_pts_oe", descending=True).head())
```

### nba_tracking_pass_value {#nba_tracking_pass_value}

`nba_tracking_pass_value(seasons: "'int | str | list'", *, league_id: 'str' = '00', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, fetch_potential_assists: 'bool' = False, max_players: 'int' = 0, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None, _pass_get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Expected-assists / passer value: `ast_oe` per player-season.

Fetches the `Passing` `leaguedashptstats` measure (one call) and computes
`ast_oe = ast - passes * bucket_assist_rate`. When
`fetch_potential_assists=True`, also fetches `nba_stats_playerdashptpass`
for the top-`max_players` passers (capped, optional -- never a hard
dependency) and recomputes the residual against the richer
`potential_assists` denominator for that subset; `max_players=0`
(default) makes exactly one request total. `ast_pts_created` is passed
through directly from the Passing measure (it is already computed there;
not re-derived).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to the fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute the baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `fetch_potential_assists` | `bool` | `False` | Enrich the top passers with `playerdashptpass` potential-assist counts. |
| `max_players` | `int` | `0` | Cap on per-player enrichment fetches; `0` disables enrichment regardless of `fetch_potential_assists`. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`. |
| `_pass_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_playerdashptpass`. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, gp:Int64, min:Float64, ast:Float64, passes:Float64, ast_baseline_rate:Float64, ast_expected:Float64, ast_oe:Float64, ast_oe_per_36:Float64, ast_pts_created:Float64, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_tracking_pass_value
df = nba_tracking_pass_value(2024)
print(df.sort("ast_oe", descending=True).head())

# With potential-assist enrichment for the top 50 passers

df = nba_tracking_pass_value(2024, fetch_potential_assists=True, max_players=50)
```

### nba_tracking_reb_oe {#nba_tracking_reb_oe}

`nba_tracking_reb_oe(seasons: "'int | str | list'", *, league_id: 'str' = '00', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Rebounding-over-expected: `reb_oe` plus OREB/DREB splits, per player-season.

Fetches the `Rebounding` `leaguedashptstats` measure, attaches a
`guard`/`wing`/`big` role bucket, and computes
`reb_oe = reb - reb_chances * bucket_rate` (contest-difficulty-adjusted
when the endpoint carries separate contested/uncontested CHANCE columns;
the live `stats.nba.com` payload currently does not, so this degrades
gracefully to the plain rate -- see the fixtures README for the finding).
OREB/DREB residuals are computed identically against their own chance
columns. Baselines are recomputed from the same season slice on every
call -- there is no fitted constant or bundled artifact.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season (`int` ending-year or `"YYYY-YY"` string) or a list of seasons to concatenate. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to the fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute the baseline within `guard`/`wing`/`big` buckets (default). `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame (see attach_role_bucket`); mostly for injecting a fixture in tests. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats` returning the raw payload dict directly -- offline testing hook. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, gp:Int64, min:Float64, reb:Float64, reb_chances:Float64, reb_baseline_rate:Float64, reb_expected:Float64, reb_oe:Float64, reb_oe_per_36:Float64, oreb_oe:Float64, dreb_oe:Float64, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_tracking_reb_oe
df = nba_tracking_reb_oe(2024)
print(df.sort("reb_oe", descending=True).head())

# League-wide baseline (no position split)

df_all = nba_tracking_reb_oe(2024, by_position=False)

# Pandas output

df_pd = nba_tracking_reb_oe(2024, return_as_pandas=True)
```

### nba_tracking_rim_protect_value {#nba_tracking_rim_protect_value}

`nba_tracking_rim_protect_value(seasons: "'int | str | list'", *, league_id: 'str' = '00', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, source: 'str' = 'leaguedash', max_players: 'int' = 0, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None, _defend_get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Rim-protection / shot-defend points-saved over expected, per player-season.

Fetches the `Defense` `leaguedashptstats` measure -- which on the live
`stats.nba.com` payload exposes only rim-band defended shooting
(`def_rim_fgm`/`def_rim_fga`/`def_rim_fg_pct`, no separate overall
figure -- see the fixtures README) -- and computes
`rim_protect_pts_saved = (normal_fg_pct - d_fg_pct) * d_fga * 2` where
`normal_fg_pct` is the bucket-mean defended rate (there is no
shooters'-own-average column on this endpoint, so the bucket mean is the
baseline; this is the same attempts-weighted construction as every other
model, just sign-flipped so a defender who holds shooters BELOW the
bucket mean gets a positive points-saved value).

`source="shotdefend"` swaps in the `Less-Than-6-Ft` band from
`nba_stats_playerdashptshotdefend` for the top-`max_players` defenders
by attempt volume (capped, optional -- never a hard dependency);
`max_players=0` (default) uses the leaguedash figures for everyone.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to the fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute the baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `source` | `str` | `'leaguedash'` | `"leaguedash"` (default) or `"shotdefend"`. |
| `max_players` | `int` | `0` | Cap on per-player `shotdefend` enrichment fetches; ignored unless `source="shotdefend"`. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`. |
| `_defend_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_playerdashptshotdefend`. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, gp:Int64, min:Float64, d_fga:Float64, d_fgm:Float64, d_fg_pct:Float64, normal_fg_pct:Float64, rim_protect_pts_saved:Float64, rim_protect_pts_saved_per_36:Float64, source:Utf8, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_tracking_rim_protect_value
df = nba_tracking_rim_protect_value(2024)
print(df.sort("rim_protect_pts_saved", descending=True).head())
```

### nba_tracking_shot_diet_value {#nba_tracking_shot_diet_value}

`nba_tracking_shot_diet_value(seasons: "'int | str | list'", *, league_id: 'str' = '00', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Catch-&-shoot vs pull-up points-over-expected, per player-season.

Fetches `CatchShoot` and `PullUpShot` (two calls), scores each with the
shared engine, joins on `player_id` (dtype-asserted `Utf8` both sides
first), and computes `shot_diet_delta = (cs_pts_oe / cs_fga) -
(pu_pts_oe / pu_fga)` (null-safe on zero attempts) -- positive means the
player's efficiency edge comes from catch-&-shoot, negative from
off-the-dribble.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to each fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute each measure's baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`, dispatched by the `pt_measure_type` kwarg for each of the two calls. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, cs_fga:Float64, cs_pts:Float64, cs_pts_oe:Float64, pu_fga:Float64, pu_pts:Float64, pu_pts_oe:Float64, shot_diet_delta:Float64, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_tracking_shot_diet_value
df = nba_tracking_shot_diet_value(2024)
print(df.sort("cs_pts_oe", descending=True).head())
```

### nba_tracking_touch_value {#nba_tracking_touch_value}

`nba_tracking_touch_value(seasons: "'int | str | list'", *, league_id: 'str' = '00', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Touch / possession-time value over expected, per player-season.

Fetches the `Possessions` `leaguedashptstats` measure and computes
`pts_per_touch_oe = pts - touches * bucket_pts_per_touch`.
`time_of_poss_eff` is the z-score of `pts / time_of_poss` within the
player's role bucket -- scoring economy per second of possession,
independent of touch volume.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to the fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute the baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, gp:Int64, min:Float64, touches:Float64, pts:Float64, touch_baseline_rate:Float64, touch_expected:Float64, pts_per_touch_oe:Float64, time_of_poss:Float64, time_of_poss_eff:Float64, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_tracking_touch_value
df = nba_tracking_touch_value(2024)
print(df.sort("pts_per_touch_oe", descending=True).head())
```

### nbadraft_mock_draft {#nbadraft_mock_draft}

`nbadraft_mock_draft(year: 'Optional[int]' = None, *, proxy: 'Any' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

The current consensus mock draft from NBADraft.net.

One row per pick across both rounds. The page renders round 1 and round 2 as
the first two pick tables and then **repeats round 1 in a third table**, so
only the first two are taken -- concatenating all three double-counts round 1.
The `<noscript>` fallback is another false-positive JS challenge; the pick
tables are static. A traded pick's team cell carries `*`, which is stripped.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Optional[int]` | `None` | Draft year (e.g. `2025`). `None` (default) reads the site's current mock; a year uses the `/nba-mock-drafts/{year}/` path where NBADraft.net has one. |
| `proxy` | `Any` | `None` | Proxy configuration forwarded to `~sportsdataverse.dl_utils.download` (`requests` `proxies=` shape). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per pick with `round` (1 or 2), `pick`, `team`, `player`, `height`, `weight`, `position`, `school` and `class`. An unreachable or table-less page yields a zero-row frame with that schema.

| col_name | type | description |
|---|---|---|
| `round` | integer | Tournament / playoff round. |
| `pick` | integer | Pick number within the round. |
| `team` | character | Team-side label or team identifier. |
| `player` | character | Player name. |
| `height` | character | Player height (string e.g. '6-2' or inches). |
| `weight` | integer | Player weight in pounds. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `school` | character | Player school / pre-draft team. |
| `class` | character | College class / draft eligibility note. |

**Example**

```python
from sportsdataverse.nba import nbadraft_mock_draft

mock = nbadraft_mock_draft()
print(mock.shape)

# A specific draft year, as pandas

mock_pd = nbadraft_mock_draft(year=2025, return_as_pandas=True)

# Pipeline next step (lottery only)

mock.filter((pl.col("round") == 1) & (pl.col("pick") <= 14))
```

### player_play_context {#player_play_context}

`player_play_context(possessions: 'pl.DataFrame', *, league_non_transition_ppp: 'Optional[float]' = None, apply_ctg_filters: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Per-player offensive On/Off Play-Context table (CTG's On/Off page, offense half).

For each player: their team's offensive play-context **with them on the floor**
(`on_*`), **without them** (`off_*`), and the on-minus-off difference
(`diff_*`) — which is the number CTG actually displays.

The OFF side is derived by **subtraction** (team total minus on-court), not by a
second scan. That is deliberate: it makes the partition exact by construction —
`on_poss + off_poss == team_poss` and the same for points — so a leak (a
double-counted possession, a dropped lineup slot) is impossible to hide. The
test suite asserts that identity directly.

Like CTG's on/off, this is a **raw** split: no luck adjustment, no opponent
adjustment, no minutes threshold. It is a descriptive difference, not a causal
estimate — for that, use the RAPM surface
(`~sportsdataverse.nba.nba_rapm.nba_rapm`).

Requires `off_player_1..5` from
`~sportsdataverse.nba.nba_possessions.attach_possession_lineups`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Frame from `add_play_context` **with lineups attached**. |
| `league_non_transition_ppp` | `Optional[float]` | `None` | Pts+/Poss baseline; see `team_play_context`. One baseline is shared across the on and off sides so the diffs are comparable. |
| `apply_ctg_filters` | `bool` | `True` | Drop garbage-time / heave / non-counting possessions first. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (player, team) with `PLAYER_PLAY_CONTEXT_SCHEMA`. Empty input returns a zero-row frame with that schema.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `offense_team_id` | integer | Unique identifier for offense team. |
| `on_poss` | integer |  |
| `off_poss` | integer |  |
| `on_points` | integer |  |
| `off_points` | integer |  |
| `on_pts_per_100` | double |  |
| `off_pts_per_100` | double |  |
| `diff_pts_per_100` | double |  |
| `on_transition_freq` | double |  |
| `off_transition_freq` | double |  |
| `diff_transition_freq` | double |  |
| `on_transition_pts_per_100` | double |  |
| `off_transition_pts_per_100` | double |  |
| `diff_transition_pts_per_100` | double |  |
| `on_halfcourt_pts_per_100` | double |  |
| `off_halfcourt_pts_per_100` | double |  |
| `diff_halfcourt_pts_per_100` | double |  |
| `on_transition_pts_added_per_100` | double |  |
| `off_transition_pts_added_per_100` | double |  |

**Example**

```python
poss = attach_possession_lineups(add_play_context(enh), oncourt, enh, home_team_id=home)
onoff = player_play_context(poss)
print(onoff.sort("diff_pts_per_100", descending=True).head())

# Who makes their team run?

print(onoff.sort("diff_transition_freq", descending=True).head())
```

### player_rates {#player_rates}

`player_rates(box_logs: 'pl.DataFrame') -> 'pl.DataFrame'`

Per-player per-minute rate stats from box logs.

Rows with null minutes (DNPs) are dropped. Rate = total stat / total
minutes across the player's games; `minutes_pg` is the mean minutes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_logs` | `DataFrame` |  | Per-player-per-game frame with `player_id, team_id, minutes, pts, reb, ast, fg3m`. |

**Returns**

One row per player: `player_id, team_id, games, minutes_pg, pts_per_min, reb_per_min, ast_per_min, fg3m_per_min`. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `team_id` | character | Unique team identifier. |
| `games` | integer | Games played. |
| `minutes_pg` | double |  |
| `pts_per_min` | double |  |
| `reb_per_min` | double |  |
| `ast_per_min` | double |  |
| `fg3m_per_min` | double |  |

**Example**

```python
from sportsdataverse.nba.nba_player_props import player_rates
rates = player_rates(box_logs)
```

### players_on_court_from_pbp {#players_on_court_from_pbp}

`players_on_court_from_pbp(enhanced_pbp: 'pl.DataFrame', raw_box: 'dict', *, home_team_id: 'int', away_team_id: 'int') -> 'pl.DataFrame'`

Reconstruct the 5-on-5 on-court lineup from pbp subs + boxscore starters.

Pure function (no network). A gamerotation-free alternative to
`players_on_court_from_rotation` returning the identical
`LINEUPS_SCHEMA` frame (one row per action, slots sorted ascending or
`None`). See the module design for the algorithm.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enhanced_pbp` | `DataFrame` |  | Output of `enhanced_pbp_from_payload`. Must carry `game_id`, `action_number`, `order_index`, `period`, `team_id`, `person_id`, `description`, `is_substitution`. |
| `raw_box` | `dict` |  | Raw `boxscoretraditionalv3` dict (starters + name map). |
| `home_team_id` | `int` |  | Home team id (from `boxscore_home_away`). |
| `away_team_id` | `int` |  | Away team id (from `boxscore_home_away`). |

**Returns**

`polars.DataFrame` conforming to `LINEUPS_SCHEMA`. Empty input returns a zero-row frame (never raises).

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `action_number` | integer | Sequential action number within a game (V3 PBP). |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `home_player_1` | integer |  |
| `home_player_2` | integer |  |
| `home_player_3` | integer |  |
| `home_player_4` | integer |  |
| `home_player_5` | integer |  |
| `away_player_1` | integer |  |
| `away_player_2` | integer |  |
| `away_player_3` | integer |  |
| `away_player_4` | integer |  |
| `away_player_5` | integer |  |

**Example**

```python
import json, pathlib
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_lineups import (
    boxscore_home_away, players_on_court_from_pbp,
)
box = json.loads(pathlib.Path("boxscoretraditionalv3.json").read_text())
pbp = json.loads(pathlib.Path("playbyplayv3.json").read_text())
enh = enhanced_pbp_from_payload(pbp)
home, away = boxscore_home_away(box)
oc = players_on_court_from_pbp(enh, box, home_team_id=home, away_team_id=away)
print(oc.shape)
```

### players_on_court_from_quarter_boxscores {#players_on_court_from_quarter_boxscores}

`players_on_court_from_quarter_boxscores(enhanced_pbp: 'pl.DataFrame', period_boxscores: 'Dict[int, dict]', raw_box: 'Optional[dict]' = None, *, home_team_id: 'int', away_team_id: 'int') -> 'pl.DataFrame'`

Reconstruct the 5-on-5 on-court lineup, seeding each period exactly where possible.

A structural sibling of `players_on_court_from_pbp` — same
`LINEUPS_SCHEMA` output, same sub-batching walk / ffill-bfill / ascending
sort tail — whose only difference is *how each period is seeded*: when
that period's range-boxscore (period_box_oncourt`) narrows to
exactly 5 on-court candidates for a team, the period is seeded EXACTLY
from it; otherwise it falls back to the same gamerotation-free
first-appearance inference `players_on_court_from_pbp` uses
(period_starters`, carrying the prior period's ending lineup as
the silent-starter fallback). See period_box_oncourt` for the
narrowing recipe (empirically re-derived against pbpstats'
`StartOfPeriod._get_starters_from_boxscore_request` — see that
function's docstring for the concrete evidence behind its zero-sentinel
polarity).

Substitution name resolution merges up to three sources via
merge_name_maps`: name_map_from_period_boxes` (the union
of every period's range-box roster), name_map_from_pbp_actors`
(every row's own actor identity — covers bench players who never touch a
period boundary but do record at least one action), and — when the
caller supplies it — boxscore_name_map` over the full-game
`raw_box` payload, the SAME full-roster source
`players_on_court_from_pbp` uses. That third source is what fixes
the one residual name-resolution gap the first two cannot cover: a
player who is subbed in and then records **zero** further pbp actions for
the rest of the game (so never appears in name_map_from_pbp_actors`)
and never happens to be on court at an exact period-opening tick (so
never appears in name_map_from_period_boxes`) is still present in the
full-game boxscore roster — which lists every player on both teams
regardless of playing time — and therefore still resolvable. Passing
`raw_box` is optional (`None` preserves the pre-existing two-source
behavior) but strongly recommended: without it this producer's per-game
agreement with the gamerotation oracle can regress well below
`players_on_court_from_pbp`'s own floor on a fixture with a
late, stat-less bench appearance (see
`tests/nba/test_nba_lineups.py::test_quarter_box_agreement_floors`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enhanced_pbp` | `DataFrame` |  | Output of `~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`. Must carry `game_id`, `action_number`, `order_index`, `period`, `team_id`, `person_id`, `player_name`, `player_name_i`, `description`, `is_substitution`. |
| `period_boxscores` | `Dict[int, dict]` |  | `{period: raw_boxscoretraditionalv3_range_payload}` — one entry per period, captured at that period's period_start_range` window. A missing period key falls back to pbp seeding for that period only (never raises). |
| `raw_box` | `Optional[dict]` | `None` | Optional raw full-game `boxscoretraditionalv3` payload (the same one `players_on_court_from_pbp` and `boxscore_home_away` consume) — supplies the full-roster name map described above. `None` (default) falls back to resolving names from `period_boxscores` + pbp actors only. |
| `home_team_id` | `int` |  | Home team id (from `boxscore_home_away`). |
| `away_team_id` | `int` |  | Away team id (from `boxscore_home_away`). |

**Returns**

`polars.DataFrame` conforming to `LINEUPS_SCHEMA`. Empty `enhanced_pbp` returns a zero-row frame (never raises).

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `action_number` | integer | Sequential action number within a game (V3 PBP). |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `home_player_1` | integer |  |
| `home_player_2` | integer |  |
| `home_player_3` | integer |  |
| `home_player_4` | integer |  |
| `home_player_5` | integer |  |
| `away_player_1` | integer |  |
| `away_player_2` | integer |  |
| `away_player_3` | integer |  |
| `away_player_4` | integer |  |
| `away_player_5` | integer |  |

**Example**

```python
import json, pathlib
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_lineups import (
    boxscore_home_away, players_on_court_from_quarter_boxscores,
)
box = json.loads(pathlib.Path("boxscoretraditionalv3.json").read_text())
pbp = json.loads(pathlib.Path("playbyplayv3.json").read_text())
periods = json.loads(pathlib.Path("boxv3_periods.json").read_text())
period_boxscores = {int(k): v for k, v in periods.items()}
enh = enhanced_pbp_from_payload(pbp)
home, away = boxscore_home_away(box)
oc = players_on_court_from_quarter_boxscores(
    enh, period_boxscores, box, home_team_id=home, away_team_id=away
)
print(oc.shape)
```

### players_on_court_from_rotation {#players_on_court_from_rotation}

`players_on_court_from_rotation(enhanced_pbp: 'pl.DataFrame', rotation: 'dict[str, list[dict]]', *, home_team_id: 'int', away_team_id: 'int') -> 'pl.DataFrame'`

Reconstruct the 5-on-5 on-court lineup via the rotation (gamerotation) algorithm.

Pure function — no network calls.  Port of hoopR's `.players_on_court_v3()`
(R/nba_stats_pbp.R lines 857-1041).

The rotation dict may use either `"HomeTeam"`/`"AwayTeam"` or
`"homeTeam"`/`"awayTeam"` as keys — both are accepted.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enhanced_pbp` | `DataFrame` |  | Output of `~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`. Must contain `game_id`, `action_number`, `period`, `seconds_remaining`, `is_substitution`, and `team_id`. |
| `rotation` | `dict[str, list[dict]]` |  | Parsed rotation dict, typically from `parse_rotation_resultsets`. Each team's list contains stint dicts with numeric `PERSON_ID`, `IN_TIME_REAL`, `OUT_TIME_REAL`. |
| `home_team_id` | `int` |  | Integer team ID of the home team. |
| `away_team_id` | `int` |  | Integer team ID of the away team. |

**Returns**

`polars.DataFrame` conforming to `LINEUPS_SCHEMA` with one row per action in *enhanced_pbp* (same row count, same ordering). Never raises — empty/malformed rotation returns a zero-row frame.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `action_number` | integer | Sequential action number within a game (V3 PBP). |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `home_player_1` | integer |  |
| `home_player_2` | integer |  |
| `home_player_3` | integer |  |
| `home_player_4` | integer |  |
| `home_player_5` | integer |  |
| `away_player_1` | integer |  |
| `away_player_2` | integer |  |
| `away_player_3` | integer |  |
| `away_player_4` | integer |  |
| `away_player_5` | integer |  |

**Example**

```python
import json, pathlib
import polars as pl
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_lineups import (
    boxscore_home_away, parse_rotation_resultsets,
    players_on_court_from_rotation,
)
box = json.loads(pathlib.Path("boxscoretraditionalv3.json").read_text())
pbp = json.loads(pathlib.Path("playbyplayv3.json").read_text())
rot = json.loads(pathlib.Path("gamerotation.json").read_text())
enh = enhanced_pbp_from_payload(pbp)
home, away = boxscore_home_away(box)
rotation = parse_rotation_resultsets(rot)
df = players_on_court_from_rotation(
    enh, rotation, home_team_id=home, away_team_id=away
)
print(df.shape)
```

### prob_over {#prob_over}

`prob_over(exp_value: 'float', line: 'float', stat: 'str', *, league_id: 'str' = '00') -> 'float'`

Probability a stat finishes strictly above `line`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_value` | `float` |  | Projected mean of the stat. |
| `line` | `float` |  | The prop line. |
| `stat` | `str` |  | One of `"pts"`, `"reb"`, `"ast"`, `"fg3m"`. |
| `league_id` | `str` | `'00'` | Accepted for parity. |

**Returns**

`P(stat > line)` in `[0, 1]`.

**Example**

```python
from sportsdataverse.nba.nba_player_props import prob_over
prob_over(24.0, 22.5, "pts")
```

### project_player_line {#project_player_line}

`project_player_line(rate_row: 'dict[str, Any]', exp_minutes: 'float', pace_factor: 'float' = 1.0) -> 'dict[str, float]'`

Project a player's expected counting line from per-minute rates.

`exp_stat = rate_per_min * exp_minutes * pace_factor` -- counting stats
scale with both projected minutes and pace.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `rate_row` | `dict[str, Any]` |  | One row of `player_rates` (as a dict). |
| `exp_minutes` | `float` |  | Projected minutes for the game. |
| `pace_factor` | `float` | `1.0` | Pace multiplier (`exp_poss / avg_pace`); `1.0` for a league-average-pace matchup. |

**Returns**

`{"exp_pts", "exp_reb", "exp_ast", "exp_fg3m"}`.

**Example**

```python
from sportsdataverse.nba.nba_player_props import player_rates, project_player_line
r = player_rates(box_logs).row(0, named=True)
line = project_player_line(r, exp_minutes=32.0, pace_factor=1.02)
```

### prop_distribution {#prop_distribution}

`prop_distribution(exp_value: 'float', stat: 'str', *, league_id: 'str' = '00') -> 'tuple[str, dict[str, float]]'`

Distribution family + parameters for a projected stat mean.

Points -> Normal `(mu, sd)` with `sd = a + b*sqrt(mu)`; counts
(reb/ast/fg3m) -> Negative-Binomial `(r, p)` matching mean `mu` and
variance `dispersion*mu` (Poisson if dispersion <= 1).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_value` | `float` |  | Projected mean of the stat. |
| `stat` | `str` |  | One of `"pts"`, `"reb"`, `"ast"`, `"fg3m"`. |
| `league_id` | `str` | `'00'` | Accepted for parity (dispersion is currently league-shared). |

**Returns**

`(family, params)` where family is `"normal"`, `"nbinom"` or `"poisson"`.

**Example**

```python
from sportsdataverse.nba.nba_player_props import prop_distribution
fam, par = prop_distribution(24.0, "pts")
```

### ratings_as_of {#ratings_as_of}

`ratings_as_of(model: 'AnyModel', possessions: 'pl.DataFrame', asof: 'datetime.date') -> 'RatingsFit'`

Fit `model` on every possession dated on or before `asof` and return ratings.

This is the through-date primitive: possessions with `game_date > asof`
are excluded from the fit entirely (never merely down-weighted), which is
what makes the panel built from repeated calls to this function leakage-free
by construction — see `tests/nba/test_nba_ratings_panel.py::test_ratings_as_of_is_leakage_free_append_invariant`.
NOTE: the leakage property is proven by the append-invariance test TOGETHER
with the panel's per-date-parity test — neither alone covers
cross-checkpoint-window leaks; do not prune one without the other.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `AnyModel` |  | A harness model conforming to `nba_model_validation.AnyModel` (a `RapmModel`, `RatingsModel`, or `PriorModel`). |
| `possessions` | `DataFrame` |  | A possession+lineup frame that MUST carry a `game_date` (`pl.Date`) column (as emitted by `compile_nba_season`). |
| `asof` | `date` |  | The through-date checkpoint (inclusive). |

**Returns**

`RatingsFit` with per-player offense/defense ratings (per-100-possession scale, same sign convention as `nba_rapm`: positive `d_ratings` means good defense). Empty dicts when no possessions fall on or before `asof` or when `possessions` is empty.

**Example**

```python
import datetime
from sportsdataverse.nba.nba_model_validation import RidgeRapmModel
from sportsdataverse.nba.nba_ratings_panel import ratings_as_of

rf = ratings_as_of(RidgeRapmModel(), season_poss, datetime.date(2023, 12, 1))
print(rf.o_ratings[201939])   # per-100 offensive rating through Dec 1
```

### rotowire_injuries {#rotowire_injuries}

`rotowire_injuries(*, proxy: 'Any' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

The current NBA injury report from RotoWire.

One row per injured player: team, position, the injury, the designation
(Out / Doubtful / Questionable / GTD / Day-To-Day) and a link to the player's
RotoWire page. This is the live replacement for the defunct RotoWorld feed.

The rendered grid at `/basketball/news.php?view=injuries` builds itself
client-side, so this reads the JSON table endpoint the grid calls
(`/basketball/tables/injury-report.php?team=ALL&pos=ALL`) rather than
scraping the page. The projected return date is subscriber-gated and comes
back as `"Subscribers Only"`; it is returned as null for non-subscribers.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `proxy` | `Any` | `None` | Proxy configuration forwarded to `~sportsdataverse.dl_utils.download` (`requests` `proxies=` shape). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per injured player with `player_id`, `player`, `first_name`, `last_name`, `team`, `position`, `injury`, `status`, `return_date` and `url`. An unreachable endpoint or a non-list body yields a zero-row frame with that schema.

| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `player` | character | Player name. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `team` | character | Team-side label or team identifier. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `injury` | character | Injury (body part / description). |
| `status` | character | Status label. |
| `return_date` | character | Projected return (`NA` unless a subscriber). |
| `url` | character | RotoWire player page URL. |

**Example**

```python
from sportsdataverse.nba import rotowire_injuries

injuries = rotowire_injuries()
print(injuries.shape)

# As pandas

injuries_pd = rotowire_injuries(return_as_pandas=True)

# Pipeline next step (who is ruled out)

injuries.filter(pl.col("status") == "Out").select("player", "team", "injury")
```

### score_shot_xpoints {#score_shot_xpoints}

`score_shot_xpoints(shots: 'pl.DataFrame', league_avgs: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Score each shot with expected points from the league-average baseline.

Joins the per-shot frame to the zone baseline (falling back to the
within-`shot_zone_range` mean when a zone triple is unmatched) and adds
`shot_value` (3 for a `3PT` shot else 2), `xpoints = base_fg_pct *
shot_value`, and `actual_points = shot_made_flag * shot_value`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shots` | `DataFrame` |  | Per-shot `Shot_Chart_Detail` frame (needs `shot_type` + the three zone keys + `shot_made_flag`). |
| `league_avgs` | `DataFrame` |  | The `LeagueAverages` frame (see `xpoints_baseline`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The input `shots` plus `shot_value:Int64, base_fg_pct:Float64, xpoints:Float64, actual_points:Float64`. Empty input returns the augmented schema with zero rows.

No returns table is published for this function: no capture: its input is stats.nba.com shot-chart detail (shot zones and types), which answers HTTP 403 to the datacenter IP the docs are built on and is not in the raw store.

**Example**

```python
from sportsdataverse.nba.nba_shot_value import score_shot_xpoints
scored = score_shot_xpoints(shots, league_avgs)

# Pipeline next step (one line)

scored.group_by("player_id").agg(pl.col("xpoints").sum())
```

### shooter_talent {#shooter_talent}

`shooter_talent(scored_shots: 'pl.DataFrame', *, league_id: 'str' = '00', min_attempts: 'int' = 50, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Regressed shooter true-talent: make%-above-expected, shrunk to the mean.

Aggregates `score_shot_xpoints` output per shooter and regresses the
raw over-expected rate toward zero by `n/(n+k)` (`k =
get_shrinkage_k(league_id)`, fitted split-half). **As-of leakage
boundary:** to score a shooter's talent for shots after date *D*, pass
only that shooter's shots before *D* -- this function does not enforce the
cut itself.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scored_shots` | `DataFrame` |  | `score_shot_xpoints` output (needs `player_id`, `shot_made_flag`, `base_fg_pct`, `xpoints`, `actual_points`). |
| `league_id` | `str` | `'00'` | `"00"` NBA, `"10"` WNBA, `"20"` G-League. |
| `min_attempts` | `int` | `50` | Drop shooters with fewer attempts (unstable estimate). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per `player_id`: `player_id:Int64, n_att:Int64, actual_makes:Int64, exp_makes:Float64, points_above_expected:Float64, raw_above_pct:Float64, talent_pct:Float64`. Empty input returns the zero-row schema.

No returns table is published for this function: no capture: its input is stats.nba.com shot-chart detail (shot zones and types), which answers HTTP 403 to the datacenter IP the docs are built on and is not in the raw store.

**Example**

```python
from sportsdataverse.nba.nba_shot_value import score_shot_xpoints, shooter_talent
talent = shooter_talent(score_shot_xpoints(shots, league_avgs))

# Pipeline next step (one line)

talent.sort("talent_pct", descending=True).head(15)
```

### shot_selection_quality {#shot_selection_quality}

`shot_selection_quality(scored_shots: 'pl.DataFrame', *, min_attempts: 'int' = 50, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Player shot-selection quality: mean expected value vs the league mean.

`xev_per_shot` is a player's mean `xpoints` (the value of the LOOKS
they take, independent of makes); `selection_quality` is that minus the
league-wide mean `xpoints` over the same frame -- a rim-and-three diet
scores positive, a mid-range diet negative.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scored_shots` | `DataFrame` |  | `score_shot_xpoints` output (needs `player_id`, `xpoints`). |
| `min_attempts` | `int` | `50` | Drop players with fewer attempts. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per `player_id`: `player_id:Int64, n_att:Int64, xev_per_shot:Float64, league_xev_per_shot:Float64, selection_quality:Float64`. Empty input returns the zero-row schema.

No returns table is published for this function: no capture: its input is stats.nba.com shot-chart detail (shot zones and types), which answers HTTP 403 to the datacenter IP the docs are built on and is not in the raw store.

**Example**

```python
from sportsdataverse.nba.nba_shot_value import score_shot_xpoints, shot_selection_quality
sel = shot_selection_quality(score_shot_xpoints(shots, league_avgs))

# Pipeline next step (one line)

sel.sort("selection_quality", descending=True).head(15)
```

### shrink_clutch {#shrink_clutch}

`shrink_clutch(delta: 'pl.DataFrame', *, league_id: 'str' = '00') -> 'pl.DataFrame'`

Empirical-Bayes / James-Stein shrinkage of `clutch_delta` toward zero.

Per-team sampling variance is `σ²_i = scale / clutch_poss` (small samples
shrink harder); the between-team signal variance `τ²` is the observed
variance of `clutch_delta` net of mean sampling variance; the shrink
factor `k_i = τ² / (τ² + σ²_i)` and `clutch_skill_shrunk = k_i · delta_i`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `delta` | `DataFrame` |  | Output of `clutch_delta` (needs `clutch_delta` + `clutch_poss`). |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"` (accepted for parity; the scale is currently league-shared). |

**Returns**

`delta` with an added `clutch_skill_shrunk` column. Empty input returns the input schema plus that column.

No returns table is published for this function: no capture: its clutch frame is built from stats.nba.com leaguedashteamclutch, which answers HTTP 403 to the datacenter IP the docs are built on.

**Example**

```python
from sportsdataverse.nba.nba_clutch import clutch_delta, shrink_clutch
skill = shrink_clutch(clutch_delta(clutch_frame, baseline_frame))
```

### spotrac_team_cap {#spotrac_team_cap}

`spotrac_team_cap(season: 'Optional[int]' = None, *, proxy: 'Any' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Team salary-cap allocations from Spotrac.

One row per team: cap allocations, cap space, active-player count and average
roster age for a season. No API key required.

The page carries a `<noscript>` fallback that looks like a JS challenge but
is not -- the cap table is in the static HTML. The team cell duplicates the
abbreviation (`"ORL ORL"`), so only the first token is kept, and every
`$`-formatted column is parsed to `Float64`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season in 4-digit ENDING-year form (`2024` = the 2023-24 season). Defaults to `~sportsdataverse.nba.nba_schedule.most_recent_nba_season`. |
| `proxy` | `Any` | `None` | Proxy configuration forwarded to `~sportsdataverse.dl_utils.download` (`requests` `proxies=` shape). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team. Columns follow Spotrac's table -- `rank`, `team`, `record`, `players_active`, `avg_age_team`, `total_cap_allocations`, `cap_space_all` -- plus `season`. An unreachable or table-less page yields a zero-row frame with that schema.

| col_name | type | description |
|---|---|---|
| `rank` | integer | Rank. |
| `team` | character | Team-side label or team identifier. |
| `record` | character | Overall win-loss record. |
| `players_active` | integer | Number of active players. |
| `avg_age_team` | double | Average roster age. |
| `total_cap_allocations` | double | Total cap allocations (USD). |
| `cap_space_all` | double | Cap space / over-the-cap amount (USD). |
| `season` | integer | Season year. |

**Example**

```python
from sportsdataverse.nba import spotrac_team_cap

cap = spotrac_team_cap(season=2024)
print(cap.shape)

# As pandas

cap_pd = spotrac_team_cap(season=2024, return_as_pandas=True)

# Pipeline next step (most cap space)

cap.sort("cap_space_all", descending=True).head()
```

### starters_on_court_counts {#starters_on_court_counts}

`starters_on_court_counts(possessions: 'pl.DataFrame', starters: 'dict[int, list[int]]') -> 'dict[int, int]'`

Count, per possession, how many **starters** are on the floor across BOTH teams.

This supplies the second half of CTG's garbage-time rule — "there have to be
**two or fewer starters on the floor combined between the two teams**" — which
`flag_garbage_time` cannot evaluate on its own (the possession frame does
not carry who is on the floor).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Possession frame with the ten on-court columns `off_player_1..5` **and** `def_player_1..5` (from `~sportsdataverse.nba.nba_possessions.attach_possession_lineups`). |
| `starters` | `dict[int, list[int]]` |  | `{team_id: [player_id, ...]}` — e.g. from `~sportsdataverse.nba.nba_lineups._starters_from_boxscore_v3`. Player ids are matched across both teams' starting fives, so the offense/defense split of the lineup columns does not matter. |

**Returns**

`{possession_number: starters_on_floor}`, each value in `0..10`. An **empty** *starters* map yields all-zero counts, which would make CTG's `<= 2` clause vacuously true and flag every margin-qualifying possession. The counts are reported honestly rather than guessed — do not pass an empty map and then read the result as CTG-exact.

**Example**

```python
counts = starters_on_court_counts(poss, _starters_from_boxscore_v3(box))
print(max(counts.values()))  # 10 at the opening tip
```

### team_pace_projection {#team_pace_projection}

`team_pace_projection(home_team_id: 'str', away_team_id: 'str', ratings: 'pl.DataFrame', *, league_id: 'str' = '00') -> 'float'`

Expected possessions for a matchup (Phase-3 `expected_possessions`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_team_id` | `str` |  | Home team id (matched against `ratings['team_id']`). |
| `away_team_id` | `str` |  | Away team id. |
| `ratings` | `DataFrame` |  | One row per team with `team_id, adj_pace`. |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"`. |

**Returns**

Expected possessions for the game.

**Example**

```python
from sportsdataverse.nba.nba_player_props import team_pace_projection
poss = team_pace_projection("1", "2", ratings)
```

### team_play_context {#team_play_context}

`team_play_context(possessions: 'pl.DataFrame', *, league_non_transition_ppp: 'Optional[float]' = None, apply_ctg_filters: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Roll possessions up into CTG's team Play-Context table.

Reproduces the offensive half of CTG's `/stats/league/context` page.

Columns: `poss`, `points`, `pts_per_100`, `transition_poss`,
`transition_points`, `transition_freq`, `transition_pts_per_100`
(CTG's "Eff"), `non_transition_pts_per_100`, `transition_pts_added_per_100`
(CTG's "Pts+/Poss"), plus `halfcourt_*` twins and per-source transition
frequencies (`freq_off_steal` / `freq_off_live_rebound`).

**Pts+/Poss** is the subtle one. CTG: "CTG takes a team's points per
possession that starts with transition, and subtracts out **what an average
team does** in a possession that did not start with transition. ... We take the

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Frame from `add_play_context`. |
| `league_non_transition_ppp` | `Optional[float]` | `None` | League-average points per 100 possessions on non-transition-start possessions. Computed from the frame when omitted. |
| `apply_ctg_filters` | `bool` | `True` | Drop garbage-time, heave and non-counting possessions first (CTG's default view). Set `False` for the unfiltered totals. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per `offense_team_id`. Empty input returns a zero-row frame.

| col_name | type | description |
|---|---|---|
| `offense_team_id` | integer | Unique identifier for offense team. |
| `poss` | integer | Poss. |
| `points` | integer | Points scored. |
| `transition_poss` | integer |  |
| `transition_points` | integer |  |
| `halfcourt_poss` | integer |  |
| `pts_per_100` | double |  |
| `transition_freq` | double |  |
| `transition_pts_per_100` | double |  |
| `non_transition_pts_per_100` | double |  |
| `freq_off_steal` | double |  |
| `freq_off_live_rebound` | double |  |
| `transition_pts_added_per_100` | double |  |
| `halfcourt_pts_per_100` | double |  |

**Example**

```python
ctx = team_play_context(add_play_context(pbp))
print(ctx.select("offense_team_id", "transition_freq", "transition_pts_added_per_100"))

# Season-comparable Pts+/Poss

ctx = team_play_context(season_poss, league_non_transition_ppp=104.8)
```

### xpoints_baseline {#xpoints_baseline}

`xpoints_baseline(league_avgs: 'pl.DataFrame') -> 'pl.DataFrame'`

League-average FG% baseline table keyed by the three shot-zone columns.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league_avgs` | `DataFrame` |  | The `LeagueAverages` result set from `nba_stats_shotchartdetail` (`shot_zone_basic` / `shot_zone_area` / `shot_zone_range` / `fga` / `fgm` / `fg_pct`). |

**Returns**

One row per `(shot_zone_basic, shot_zone_area, shot_zone_range)`: `... base_fg_pct:Float64, is_three:Boolean` (`is_three` = the basic zone names a three). Empty input returns the zero-row schema.

| col_name | type | description |
|---|---|---|
| `shot_zone_basic` | character | Shot zone (e.g. 'Restricted Area', 'Mid-Range', 'Above the Break 3'). |
| `shot_zone_area` | character | Shot zone area ('Left Side', 'Right Side', 'Center'). |
| `shot_zone_range` | character | Shot zone range ('Less Than 8 ft.', '8-16 ft.', '16-24 ft.', etc.). |
| `base_fg_pct` | double |  |
| `is_three` | logical |  |

**Example**

```python
from sportsdataverse.nba import nba_stats
from sportsdataverse.nba.nba_shot_value import xpoints_baseline
base = xpoints_baseline(league_avgs)
```

### zone_value_map {#zone_value_map}

`zone_value_map(scored_shots: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per-player per-zone value map: points and expected points per shot.

Collapses `shot_zone_basic` to a canonical zone via `ZONE_COLLAPSE`
(the two corner-3 zones merge) and aggregates realized vs expected points
per shot in each zone.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scored_shots` | `DataFrame` |  | `score_shot_xpoints` output (needs `player_id`, `shot_zone_basic`, `shot_made_flag`, `actual_points`, `xpoints`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per `(player_id, zone)`: `player_id:Int64, zone:Utf8, att:Int64, makes:Int64, pts:Float64, pps:Float64, xpps:Float64, pps_above_expected:Float64` (`pps` = points per shot, `xpps` = expected). Empty input returns the zero-row schema.

No returns table is published for this function: no capture: its input is stats.nba.com shot-chart detail (shot zones and types), which answers HTTP 403 to the datacenter IP the docs are built on and is not in the raw store.

**Example**

```python
from sportsdataverse.nba.nba_shot_value import score_shot_xpoints, zone_value_map
zmap = zone_value_map(score_shot_xpoints(shots, league_avgs))

# Pipeline next step (one line)

zmap.filter(pl.col("zone") == "corner_3").sort("pps_above_expected", descending=True)
```
