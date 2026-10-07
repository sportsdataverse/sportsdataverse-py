---
title: "MBB — additional Python functions — Play-by-play processing"
sidebar_label: "Play-by-play processing"
sidebar_position: 8
description: "MBB — additional Python functions — Play-by-play processing — function reference in sdv-py, the SportsDataverse Python package."
---
# MBB — additional Python functions — Play-by-play processing

### build_athlete_identity_lookup {#build_athlete_identity_lookup}

`build_athlete_identity_lookup(rosters: 'dict[int | str, dict]') -> 'dict[str, dict[str, Any]]'`

R `build_athlete_identity_lookup`: athlete_id -> identity from team rosters.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `rosters` | `dict[int \| str, dict]` |  | Mapping of team_id -> that team's raw roster payload (`wbb/team_rosters/json/{season}/{team_id}.json`). NOTE: R walks `raw$athletes` directly here (no position-bucket unwrap, unlike the rosters dataset itself). |

**Returns**

athlete_id (str) -> identity fields for `helper_wbb_player_season_stats`.

### build_mbb_player_identity_lookup {#build_mbb_player_identity_lookup}

`build_mbb_player_identity_lookup(player_box: 'pl.DataFrame') -> 'dict[str, dict[str, Any]]'`

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

### mbb_pbp_disk {#mbb_pbp_disk}

`mbb_pbp_disk(game_id, path_to_json)`

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  |  |
| `path_to_json` |  |  |  |

### mbb_shot_data {#mbb_shot_data}

`mbb_shot_data(seasons: "'int | list[int]'", *, source: 'str' = 'espn', league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

Season(s) of shots in the canonical frame (the spine's data entry point).

`source="espn"` loads the sportsdataverse-data shots release
(`load_mbb_shots` / `load_wbb_shots`) and canonicalizes it. The NCAA
HTML path is per-game, not per-season -- parse with
`create_shot_event_data` and flatten via `shot_events_to_frame`
instead (`source="ncaa"` raises with that pointer).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season (e.g. `2025`) or list of seasons. |
| `source` | `str` | `'espn'` | `"espn"` (the only batch source). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The canonical shot frame; seasons the release doesn't cover are skipped, and no coverage at all returns the zero-row schema.

**Example**

```python
from sportsdataverse.mbb import mbb_shot_data
shots = mbb_shot_data(2025)

# Pipeline next step (one line)

shots.group_by("shot_zone").agg(pl.col("made").mean()).sort("shot_zone")
```

### ncaa_mbb_game_pbp {#ncaa_mbb_game_pbp}

`ncaa_mbb_game_pbp(game_id: 'object', *, fetcher: 'Optional[_SupportsFetchGamePbp]' = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, Any]'"`

Scrape one MBB game's play-by-play (bigballR `scrape_game`).

Fetches `stats.ncaa.org/contests/{game_id}/play_by_play` and parses it
through `parse_ncaa_bb_game_pbp` with the MBB period model
`(2, 1200, 300)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `object` |  | NCAA contest id (e.g. `"6470186"`). |
| `fetcher` | `Optional[_SupportsFetchGamePbp]` | `None` | Optional injected fetcher exposing `fetch_game_pbp` (for tests/offline use). Defaults to a fresh `NcaaFetcher.with_browser()` context per call. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The 35-column play-by-play frame (zero rows when the game is not found).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_game_pbp import ncaa_mbb_game_pbp
df = ncaa_mbb_game_pbp("6470186")
print(df.shape)

# Offline with an injected fetcher

df = ncaa_mbb_game_pbp("6470186", fetcher=my_fetcher)

# Pipeline next step (one line)

df.filter(pl.col("event_type") == "Three Point Jumper").head()
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
