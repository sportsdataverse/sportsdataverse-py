# CBS — CBS Sports NAPI (api.cbssports.com/napi) — Player

> CBS — CBS Sports NAPI (api.cbssports.com/napi) — Player — function reference in sdv-py, the SportsDataverse Python package.

## cbs_player

Get a player resource

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/1751796](https://api.cbssports.com/napi/resource/player/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `dateFormat` | `date_format` |  |  | `Y` | Optional for any date field.  Options here: http://momentjs.com/docs/#/displaying/format/ |
| `year` | `year` |  |  | `Y` | Optional year in YYYY format (for Transactions only) |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: playerTeamAssociations, injuries, transactions, depthCharts, metaData, playerStats, standings, rankings, playerOutlook, draftInfo, combineData, positionRankings, gameStats, encyclopedia, golferResults, playerGolfMetadata, playerFutures, golferMarkets, recruitTeamAssociations, coachTeamAssociations, recruitRankings, coachRankings. |

### Returns {#cbs_player-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_player-example}

```python
cbs_player(player_id=1751796)
```

_Last validated n/a._

## cbs_player_combine_data

Get draft related info for a particular player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/combineData/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/combineData/1751796](https://api.cbssports.com/napi/resource/player/combineData/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |

### Returns {#cbs_player_combine_data-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_player_combine_data-example}

```python
cbs_player_combine_data(player_id=1751796)
```

_Last validated n/a._

## cbs_player_depth_charts

Get depth charts for a particular player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/depthCharts/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/depthCharts/1751796](https://api.cbssports.com/napi/resource/player/depthCharts/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `position` | `position` |  |  | `Y` | A csv of positions to filter with |
| `pitchPos` | `pitch_pos` |  |  | `Y` | A csv of pitch positions to filter with (baseball only) |

### Returns {#cbs_player_depth_charts-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_player_depth_charts-example}

```python
cbs_player_depth_charts(player_id=1751796)
```

_Last validated n/a._

## cbs_player_draft_info

Get draft info for a particular player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/draftInfo/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/draftInfo/1751796](https://api.cbssports.com/napi/resource/player/draftInfo/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `seasonYear` | `season_year` |  |  | `Y` | View option, filter by seasonYear |
| `seasonType` | `season_type` |  |  | `Y` | View option, filter by seasonType Allowed: regular, pre, post. |
| `seasonId` | `season_id` |  |  | `Y` | View option, filter by seasonId |

### Returns {#cbs_player_draft_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_player_draft_info-example}

```python
cbs_player_draft_info(player_id=1751796)
```

_Last validated n/a._

## cbs_player_encyclopedia

Get encyclopedia resource for a particular player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/encyclopedia/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/encyclopedia/1751796](https://api.cbssports.com/napi/resource/player/encyclopedia/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `seasonYear` | `season_year` |  |  | `Y` | View option.  Filter by seasonType. |
| `seasonId` | `season_id` |  |  | `Y` | View option.  Filter by seasonId. |

### Returns {#cbs_player_encyclopedia-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_player_encyclopedia-example}

```python
cbs_player_encyclopedia(player_id=1751796)
```

_Last validated n/a._

## cbs_player_futures

Get futures for a player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/futures/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/futures/1751796](https://api.cbssports.com/napi/resource/player/futures/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |

### Returns {#cbs_player_futures-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_player_futures-example}

```python
cbs_player_futures(player_id=1751796)
```

_Last validated n/a._

## cbs_player_game_stats

Get game stats for a particular player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/gameStats/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/gameStats/1751796](https://api.cbssports.com/napi/resource/player/gameStats/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `gameId` | `game_id` |  |  | `Y` | View option, filter by gameId |
| `seasonYear` | `season_year` |  |  | `Y` | Season Year in YYYY format |
| `seasonType` | `season_type` |  |  | `Y` | Csv list of pre, regular, or post Allowed: pre, regular, post. |

### Returns {#cbs_player_game_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_player_game_stats-example}

```python
cbs_player_game_stats(player_id=1751796)
```

_Last validated n/a._

## cbs_player_hockey_meta

Get the hockey meta data resource for a particular player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/hockey/meta/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/hockey/meta/1751796](https://api.cbssports.com/napi/resource/player/hockey/meta/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |

### Returns {#cbs_player_hockey_meta-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_player_hockey_meta-example}

```python
cbs_player_hockey_meta(player_id=1751796)
```

_Last validated n/a._

## cbs_player_injuries

Get injuries for a particular player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/injuries/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/injuries/1751796](https://api.cbssports.com/napi/resource/player/injuries/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `dateFormat` | `date_format` |  |  | `Y` | Optional.  Options here: http://momentjs.com/docs/#/displaying/format/ |

### Returns {#cbs_player_injuries-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_player_injuries-example}

```python
cbs_player_injuries(player_id=1751796)
```

_Last validated n/a._

## cbs_player_meta_baseball

Get meta data associated to a particular baseball player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/meta/baseball/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/meta/baseball/1751796](https://api.cbssports.com/napi/resource/player/meta/baseball/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |

### Returns {#cbs_player_meta_baseball-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_player_meta_baseball-example}

```python
cbs_player_meta_baseball(player_id=1751796)
```

_Last validated n/a._

## cbs_player_meta_golf

Get a metadata resource for a particular golf player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/meta/golf/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/meta/golf/1751796](https://api.cbssports.com/napi/resource/player/meta/golf/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |

### Returns {#cbs_player_meta_golf-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_player_meta_golf-example}

```python
cbs_player_meta_golf(player_id=1751796)
```

_Last validated n/a._

## cbs_player_outlook

Get outlook for a player (context is now)

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/outlook/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/outlook/1751796](https://api.cbssports.com/napi/resource/player/outlook/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `dateFormat` | `date_format` |  |  | `Y` | Optional format for dateCreated field. Available options here: http://momentjs.com/docs/#/displaying/format/ |

### Returns {#cbs_player_outlook-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_player_outlook-example}

```python
cbs_player_outlook(player_id=1751796)
```

_Last validated n/a._

## cbs_player_position_rankings

Get position rankings for a player

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/positionRankings/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/positionRankings/1751796](https://api.cbssports.com/napi/resource/player/positionRankings/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `position` | `position` |  |  | `Y` | Filter by position |

### Returns {#cbs_player_position_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_player_position_rankings-example}

```python
cbs_player_position_rankings(player_id=1751796)
```

_Last validated n/a._

## cbs_player_rankings

Get all rankings for a player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/rankings/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/rankings/1751796](https://api.cbssports.com/napi/resource/player/rankings/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `seasonYear` | `season_year` |  |  | `Y` | View option.  Filter by seasonYear. |
| `seasonType` | `season_type` |  |  | `Y` | View option.  Filter by seasonType. Allowed: regular, pre, post. |
| `seasonId` | `season_id` |  |  | `Y` | View option.  Filter by seasonId. |
| `isCurrent` | `is_current` |  |  | `Y` | View option.  Only show stats for seasons where isCurrent is true. Allowed: 1. |
| `categories` | `categories` |  |  | `Y` | View option.  Only return the specified rankings categories. |

### Returns {#cbs_player_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_player_rankings-example}

```python
cbs_player_rankings(player_id=1751796)
```

_Last validated n/a._

## cbs_player_recruit_associations

Get recruit associations for a particular player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/recruitAssociations/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/recruitAssociations/1751796](https://api.cbssports.com/napi/resource/player/recruitAssociations/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve. Defaults to none. Allowed: team. |

### Returns {#cbs_player_recruit_associations-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_player_recruit_associations-example}

```python
cbs_player_recruit_associations(player_id=1751796)
```

_Last validated n/a._

## cbs_player_standings

Get standings for a particular player

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/standings/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/standings/1751796](https://api.cbssports.com/napi/resource/player/standings/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `seasonYear` | `season_year` |  |  | `Y` | View option.  Filter by seasonYear. |
| `seasonType` | `season_type` |  |  | `Y` | View option.  Filter by seasonType. Allowed: regular, pre, post. |
| `seasonId` | `season_id` |  |  | `Y` | View option.  Filter by seasonId. |
| `isCurrent` | `is_current` |  |  | `Y` | View option.  Only show standings for seasons where isCurrent is true. Allowed: 1. |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: league. |

### Returns {#cbs_player_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_player_standings-example}

```python
cbs_player_standings(player_id=1751796)
```

_Last validated n/a._

## cbs_player_stats

Get all statistics for a player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/stats/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/stats/1751796](https://api.cbssports.com/napi/resource/player/stats/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `seasonYear` | `season_year` |  |  | `Y` | View option.  Filter by seasonYear. |
| `seasonType` | `season_type` |  |  | `Y` | View option.  Filter by seasonType. Allowed: regular, pre, post. |
| `seasonId` | `season_id` |  |  | `Y` | View option.  Filter by seasonId. |
| `isCurrent` | `is_current` |  |  | `Y` | View option.  Only show stats for seasons where isCurrent is true. Allowed: 1. |
| `teamId` | `team_id` |  |  | `Y` | View option.  Filter by teamId. |
| `teamAbbr` | `team_abbr` |  |  | `Y` | View option.  Filter by a specific team abbreviation. |
| `isTotal` | `is_total` |  |  | `Y` | View option.  Filter only the isTotal record for players who played for multiple teams. Allowed: 1. |

### Returns {#cbs_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_player_stats-example}

```python
cbs_player_stats(player_id=1751796)
```

_Last validated n/a._

## cbs_player_team_associations

Get team associations for a particular player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/teamAssociations/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/teamAssociations/1751796](https://api.cbssports.com/napi/resource/player/teamAssociations/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `assocType` | `assoc_type` |  |  | `Y` | Filter associations by assoc type Allowed: C, H, S, F. |
| `rosterStatus` | `roster_status` |  |  | `Y` | Filter associations by roster status Allowed: ACT, NWT, MIN, MNR, RET, DEV, CUT, DIS, DL, IR, UFA, UDF, EXE, TRA, SUS, PUP, FA, RFA, KIA, INA. |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve. Defaults to none. Allowed: team. |

### Returns {#cbs_player_team_associations-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_player_team_associations-example}

```python
cbs_player_team_associations(player_id=1751796)
```

_Last validated n/a._

## cbs_player_transactions

Get transactions resource for a particular player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/transactions/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/transactions/1751796](https://api.cbssports.com/napi/resource/player/transactions/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `dateFormat` | `date_format` |  |  | `Y` | Optional.  Options here: http://momentjs.com/docs/#/displaying/format/ |
| `seasonYear` | `season_year` |  |  | `Y` | View option.  Filter by seasonYear. |
| `seasonType` | `season_type` |  |  | `Y` | View option.  Filter by seasonType. Allowed: regular, pre, post. |
| `seasonId` | `season_id` |  |  | `Y` | View option.  Filter by seasonId. |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve. Defaults to none. Allowed: targetTeam, currentTeam, fromTeam. |

### Returns {#cbs_player_transactions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_player_transactions-example}

```python
cbs_player_transactions(player_id=1751796)
```

_Last validated n/a._
