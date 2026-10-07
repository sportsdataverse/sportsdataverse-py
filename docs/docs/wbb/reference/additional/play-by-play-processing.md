---
title: "WBB — additional Python functions — Play-by-play processing"
sidebar_label: "Play-by-play processing"
sidebar_position: 7
description: "WBB — additional Python functions — Play-by-play processing — function reference in sdv-py, the SportsDataverse Python package."
---
# WBB — additional Python functions — Play-by-play processing

### build_athlete_identity_lookup {#build_athlete_identity_lookup}

`build_athlete_identity_lookup(rosters: 'dict[int | str, dict]') -> 'dict[str, dict[str, Any]]'`

R `build_athlete_identity_lookup`: athlete_id -> identity from team rosters.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `rosters` | `dict[int \| str, dict]` |  | Mapping of team_id -> that team's raw roster payload (`wbb/team_rosters/json/{season}/{team_id}.json`). NOTE: R walks `raw$athletes` directly here (no position-bucket unwrap, unlike the rosters dataset itself). |

**Returns**

athlete_id (str) -> identity fields for `helper_wbb_player_season_stats`.

### classify_point_value {#classify_point_value}

`classify_point_value(dist_ft: 'float', x: 'float', y: 'float', *, league: 'str', season: 'int') -> 'int'`

2 or 3 from basket-relative geometry (arc radius + corner band).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dist_ft` | `float` |  | Euclidean distance from the basket, feet. |
| `x` | `float` |  | Lateral offset from the basket, feet (baseline direction). |
| `y` | `float` |  | Distance up-court from the basket, feet. |
| `league` | `str` |  | `"mens"` or `"womens"`. |
| `season` | `int` |  | Season-ending year (selects the arc era). |

**Returns**

`3` at/beyond the arc or in the corner band, else `2`.

**Example**

```python
from sportsdataverse.mbb.mbb_shots_adapter import classify_point_value
classify_point_value(24.0, 0.0, 24.0, league="mens", season=2020)
```

### classify_zone_geometry {#classify_zone_geometry}

`classify_zone_geometry(dist_ft: 'float', x: 'float', y: 'float', *, league: 'str', season: 'int') -> 'str'`

Shot zone from geometry: `rim | paint | mid | corner3 | abovebreak3`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dist_ft` | `float` |  | Euclidean distance from the basket, feet. |
| `x` | `float` |  | Lateral offset from the basket, feet. |
| `y` | `float` |  | Distance up-court from the basket, feet. |
| `league` | `str` |  | `"mens"` or `"womens"`. |
| `season` | `int` |  | Season-ending year (selects the arc era). |

**Returns**

One of `rim`, `paint`, `mid`, `corner3`, `abovebreak3`.

**Example**

```python
from sportsdataverse.mbb.mbb_shots_adapter import classify_zone_geometry
classify_zone_geometry(2.0, 0.0, 2.0, league="mens", season=2020)
```

### classify_zone_type {#classify_zone_type}

`classify_zone_type(type_text: "'str | None'") -> "'str | None'"`

Collapse a source shot-type label to `rim | arc3 | jump`.

Note: the 2025+ ESPN shots release carries NO three-point marker in
`type_text` (vocabulary is JumpShot/LayUpShot/DunkShot/TipShot), so
`arc3` typically comes from geometry/score_value there; the branch
exists for sources that do label threes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `type_text` | `str \| None` |  | Source label (e.g. `"DunkShot"`); `None` passes through. |

**Returns**

`rim`, `arc3`, `jump`, or `None` for null input.

**Example**

```python
from sportsdataverse.mbb.mbb_shots_adapter import classify_zone_type
classify_zone_type("DunkShot")
```

### espn_shots_to_canonical {#espn_shots_to_canonical}

`espn_shots_to_canonical(espn: 'pl.DataFrame', *, league: 'str', season: 'int', scale: "'tuple[float, float, float] | None'" = None) -> 'pl.DataFrame'`

ESPN `load_mbb_shots` frame -> the canonical shot frame.

Field-goal attempts only (free throws and sentinel-coordinate rows are
dropped). `point_value` comes from `score_value` -- the release
populates it on misses too, and its `type_text` carries NO three-point
marker, so `arc3` is value-derived. Coordinates are re-based to the
fitted basket origin and scaled to feet.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `espn` | `DataFrame` |  | `load_mbb_shots`-shaped frame. |
| `league` | `str` |  | `"mens"` or `"womens"`. |
| `season` | `int` |  | Season-ending year. |
| `scale` | `tuple[float, float, float] \| None` | `None` | Optional pre-fitted `(origin_x, origin_y, feet_per_unit)`; fitted from `espn` when `None`. |

**Returns**

The canonical shot frame (`CANONICAL_SHOT_SCHEMA`); empty input returns the zero-row schema.

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_mbb_shots
from sportsdataverse.mbb.mbb_shots_adapter import espn_shots_to_canonical
df = espn_shots_to_canonical(load_mbb_shots([2025]), league="mens", season=2025)
```

### fit_espn_court_scale {#fit_espn_court_scale}

`fit_espn_court_scale(espn: 'pl.DataFrame', *, league: 'str', season: 'int') -> "'tuple[float, float, float]'"`

Fit the ESPN raw-coordinate court scale: `(origin_x, origin_y, feet_per_unit)`.

The release's `coordinate_{x,y}_raw` grid is basket-anchored half-court
(width 0-50, rim cluster near `(25, 2)`). Origin = median raw
coordinates of made rim-type shots; `feet_per_unit` = arc radius /
median unit-distance of made threes from that origin -- fitted, not
guessed, so a units change in the release shows up as a scale shift.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `espn` | `DataFrame` |  | `load_mbb_shots`-shaped frame. |
| `league` | `str` |  | `"mens"` or `"womens"`. |
| `season` | `int` |  | Season-ending year (selects the arc radius). |

**Returns**

`(origin_x, origin_y, feet_per_unit)`; documented fallbacks `(25.0, 2.0, 1.0)` when either calibration subset is empty.

**Example**

```python
from sportsdataverse.mbb.mbb_shots_adapter import fit_espn_court_scale
scale = fit_espn_court_scale(espn, league="mens", season=2025)
```

### ncaa_wbb_game_pbp {#ncaa_wbb_game_pbp}

`ncaa_wbb_game_pbp(game_id: 'object', *, fetcher: 'Optional[_SupportsFetchGamePbp]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Scrape one WBB game's play-by-play (wbigballR `scrape_game`, quarters fixed).

Same engine as `sportsdataverse.mbb.mbb_ncaa_game_pbp.ncaa_mbb_game_pbp`
with `period_model=(4, 600, 300)` bound (see the module docstring for why
this deliberately diverges from wbigballR's halves math).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `object` |  | NCAA contest id (e.g. `"5722355"`). |
| `fetcher` | `Optional[_SupportsFetchGamePbp]` | `None` | Optional injected fetcher exposing `fetch_game_pbp` (for tests/offline use). Defaults to a fresh `NcaaFetcher.with_browser()` context per call. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The 35-column play-by-play frame (zero rows when the game is not found).

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_game_pbp import ncaa_wbb_game_pbp
df = ncaa_wbb_game_pbp("5722355")
print(df.shape)
```

### ncaa_wbb_play_by_play {#ncaa_wbb_play_by_play}

`ncaa_wbb_play_by_play(game_ids: 'Sequence[object]', *, fetcher: 'Optional[_SupportsFetchGamePbp]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Scrape many WBB games' play-by-play (wbigballR `get_play_by_play`, quarters fixed).

Same driver as `sportsdataverse.mbb.mbb_ncaa_game_pbp.ncaa_mbb_play_by_play`
(drop missing ids, shared fetcher session, one retry per empty scrape) with
the WBB quarter model `(4, 600, 300)` bound.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_ids` | `Sequence[object]` |  | NCAA contest ids; `None`/NaN entries are dropped. |
| `fetcher` | `Optional[_SupportsFetchGamePbp]` | `None` | Optional injected fetcher exposing `fetch_game_pbp`. Defaults to one shared `NcaaFetcher.with_browser()` context. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Row-bound play-by-play for every game that scraped successfully (zero-row contract frame when none did).

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_game_pbp import ncaa_wbb_play_by_play
df = ncaa_wbb_play_by_play(["5722355", "5732292"])
print(df.shape)
```

### shot_events_to_frame {#shot_events_to_frame}

`shot_events_to_frame(events: 'list[ShotEvent]', *, season: 'int', league: 'str' = 'mens') -> 'pl.DataFrame'`

Flatten NCAA HTML `ShotEvent` objects to the canonical frame.

The NCAA SVG shot maps carry location + made/miss but no shot-type label
(`shot_type = "unknown"`); `point_value`/`shot_zone` come from the
geometry classifiers. The parser-phase `pts` field is the MADE flag
(1/0), not the point value.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `events` | `list[ShotEvent]` |  | Parsed shot events (`create_shot_event_data` output). |
| `season` | `int` |  | Season-ending year the events belong to. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |

**Returns**

The canonical shot frame (`CANONICAL_SHOT_SCHEMA`); empty input returns the zero-row schema.

**Example**

```python
from sportsdataverse.mbb.mbb_shots_adapter import shot_events_to_frame
df = shot_events_to_frame(events, season=2025)
```

### wbb_pbp_disk {#wbb_pbp_disk}

`wbb_pbp_disk(game_id, path_to_json)`

Read a saved ESPN WBB play-by-play payload from disk.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  | The ESPN game id; the file read is `{game_id}.json`. |
| `path_to_json` |  |  | The directory holding the saved payloads. |

**Returns**

The payload exactly as saved (the raw ESPN summary JSON), ready for `helper_wbb_pbp`.
