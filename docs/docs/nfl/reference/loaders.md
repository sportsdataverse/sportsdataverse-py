---
title: NFL dataset loaders
sidebar_label: Loaders
sidebar_position: 1
---
# NFL dataset loaders

```mermaid
flowchart LR
  raw["scrape / raw"] --> enrich["enrich"] --> rel["release asset"] --> load["load_*()"]
```

## Automation status

| Dataset | Release tag | Pipeline |
|---|---|---|
| `load_nfl_pbp` | [pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/pbp) | — |
| `load_nfl_rosters` | [rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/rosters) | — |
| `load_nfl_weekly_rosters` | [weekly_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/weekly_rosters) | — |
| `load_nfl_depth_charts` | [depth_charts](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/depth_charts) | — |
| `load_nfl_injuries` | [injuries](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/injuries) | — |
| `load_nfl_snap_counts` | [snap_counts](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/snap_counts) | — |
| `load_nfl_pbp_participation` | [pbp_participation](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/pbp_participation) | — |
| `load_nfl_ftn_charting` | [ftn_charting](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ftn_charting) | — |

## `load_nfl_pbp`

Release: [pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/pbp) · asset `https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{season}.parquet`
```python
load_nfl_pbp(seasons=2024)
```

## `load_nfl_rosters`

Release: [rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/rosters) · asset `https://github.com/nflverse/nflverse-data/releases/download/rosters/roster_{season}.parquet`
```python
load_nfl_rosters(seasons=2024)
```

## `load_nfl_weekly_rosters`

Release: [weekly_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/weekly_rosters) · asset `https://github.com/nflverse/nflverse-data/releases/download/weekly_rosters/roster_weekly_{season}.parquet`
```python
load_nfl_weekly_rosters(seasons=2024)
```

## `load_nfl_depth_charts`

Release: [depth_charts](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/depth_charts) · asset `https://github.com/nflverse/nflverse-data/releases/download/depth_charts/depth_charts_{season}.parquet`
```python
load_nfl_depth_charts(seasons=2024)
```

## `load_nfl_injuries`

Release: [injuries](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/injuries) · asset `https://github.com/nflverse/nflverse-data/releases/download/injuries/injuries_{season}.parquet`
```python
load_nfl_injuries(seasons=2024)
```

## `load_nfl_snap_counts`

Release: [snap_counts](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/snap_counts) · asset `https://github.com/nflverse/nflverse-data/releases/download/snap_counts/snap_counts_{season}.parquet`
```python
load_nfl_snap_counts(seasons=2024)
```

## `load_nfl_pbp_participation`

Release: [pbp_participation](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/pbp_participation) · asset `https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/pbp_participation_{season}.parquet`
```python
load_nfl_pbp_participation(seasons=2024)
```

## `load_nfl_ftn_charting`

Release: [ftn_charting](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ftn_charting) · asset `https://github.com/nflverse/nflverse-data/releases/download/ftn_charting/ftn_charting_{season}.parquet`
```python
load_nfl_ftn_charting(seasons=2024)
```
