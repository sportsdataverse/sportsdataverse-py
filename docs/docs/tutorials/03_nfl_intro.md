---
title: NFL tutorial
sidebar_label: NFL
sidebar_position: 6
---

# NFL intro — sportsdataverse-py

The NFL submodule mirrors [nflreadpy](https://github.com/nflverse/nflreadpy) so existing nflverse code can swap engines with minimal changes — its `load_*` functions read the same [nflverse](https://nflverse.nflverse.com) parquet releases.

R companions: [nflfastR](https://www.nflfastr.com) / [nflreadr](https://nflreadr.nflverse.com). Part of the [SportsDataverse](https://py.sportsdataverse.org/docs/ecosystem).

## Setup

```sh
pip install sportsdataverse
```


```python
import polars as pl
import sportsdataverse.nfl as nfl
```

## Caching layer

sdv-py NFL caches loader results to keep repeat calls fast. For reproducibility in a notebook, turn caching off; clean up afterwards.


```python
nfl.update_config(cache_mode='off')
nfl.get_config()
```




    NflConfig(cache_mode='off', cache_dir=WindowsPath('C:/Users/saiem/.cache/sportsdataverse/nfl'), cache_duration=86400, verbose=True, timeout=30, user_agent='sportsdataverse-py-nfl')



## nflreadpy parity surface — `load_pbp`, `load_schedules`


```python
schedules = nfl.load_schedules([2024])
schedules.shape
```




    (285, 46)




```python
(schedules
    .select(['season', 'week', 'gameday', 'home_team', 'away_team', 'home_score', 'away_score'])
    .head())
```




    shape: (5, 7)
    ┌────────┬──────┬────────────┬───────────┬───────────┬────────────┬────────────┐
    │ season ┆ week ┆ gameday    ┆ home_team ┆ away_team ┆ home_score ┆ away_score │
    │ ---    ┆ ---  ┆ ---        ┆ ---       ┆ ---       ┆ ---        ┆ ---        │
    │ i32    ┆ i32  ┆ str        ┆ str       ┆ str       ┆ i32        ┆ i32        │
    ╞════════╪══════╪════════════╪═══════════╪═══════════╪════════════╪════════════╡
    │ 2024   ┆ 1    ┆ 2024-09-05 ┆ KC        ┆ BAL       ┆ 27         ┆ 20         │
    │ 2024   ┆ 1    ┆ 2024-09-06 ┆ PHI       ┆ GB        ┆ 34         ┆ 29         │
    │ 2024   ┆ 1    ┆ 2024-09-08 ┆ ATL       ┆ PIT       ┆ 10         ┆ 18         │
    │ 2024   ┆ 1    ┆ 2024-09-08 ┆ BUF       ┆ ARI       ┆ 34         ┆ 28         │
    │ 2024   ┆ 1    ┆ 2024-09-08 ┆ CHI       ┆ TEN       ┆ 24         ┆ 17         │
    └────────┴──────┴────────────┴───────────┴───────────┴────────────┴────────────┘




```python
pbp = nfl.load_pbp([2024])
pbp.shape
```




    (49492, 372)




```python
(pbp
    .select(['game_id', 'play_id', 'qtr', 'down', 'ydstogo', 'desc'])
    .head())
```




    shape: (5, 6)
    ┌─────────────────┬─────────┬─────┬──────┬─────────┬─────────────────────────────────┐
    │ game_id         ┆ play_id ┆ qtr ┆ down ┆ ydstogo ┆ desc                            │
    │ ---             ┆ ---     ┆ --- ┆ ---  ┆ ---     ┆ ---                             │
    │ str             ┆ f64     ┆ f64 ┆ f64  ┆ f64     ┆ str                             │
    ╞═════════════════╪═════════╪═════╪══════╪═════════╪═════════════════════════════════╡
    │ 2024_01_ARI_BUF ┆ 1.0     ┆ 1.0 ┆ null ┆ 0.0     ┆ GAME                            │
    │ 2024_01_ARI_BUF ┆ 40.0    ┆ 1.0 ┆ null ┆ 0.0     ┆ 2-T.Bass kicks 65 yards from B… │
    │ 2024_01_ARI_BUF ┆ 61.0    ┆ 1.0 ┆ 1.0  ┆ 10.0    ┆ (15:00) 6-J.Conner up the midd… │
    │ 2024_01_ARI_BUF ┆ 83.0    ┆ 1.0 ┆ 2.0  ┆ 7.0     ┆ (14:27) 1-K.Murray pass short … │
    │ 2024_01_ARI_BUF ┆ 108.0   ┆ 1.0 ┆ 1.0  ┆ 10.0    ┆ (13:43) (Shotgun) 1-K.Murray p… │
    └─────────────────┴─────────┴─────┴──────┴─────────┴─────────────────────────────────┘



## NextGen Stats


```python
ngs = nfl.load_nextgen_stats([2024], stat_type='passing')
ngs.shape
```




    (614, 29)




```python
(ngs
    .select(['season', 'player_display_name', 'team_abbr', 'attempts', 'pass_yards', 'completion_percentage_above_expectation'])
    .head())
```




    shape: (5, 6)
    ┌────────┬─────────────────────┬───────────┬──────────┬────────────┬───────────────────────────────┐
    │ season ┆ player_display_name ┆ team_abbr ┆ attempts ┆ pass_yards ┆ completion_percentage_above_e │
    │ ---    ┆ ---                 ┆ ---       ┆ ---      ┆ ---        ┆ x…                            │
    │ i32    ┆ str                 ┆ str       ┆ i32      ┆ i32        ┆ ---                           │
    │        ┆                     ┆           ┆          ┆            ┆ f64                           │
    ╞════════╪═════════════════════╪═══════════╪══════════╪════════════╪═══════════════════════════════╡
    │ 2024   ┆ Justin Herbert      ┆ LAC       ┆ 504      ┆ 3870       ┆ 2.391567                      │
    │ 2024   ┆ Caleb Williams      ┆ CHI       ┆ 562      ┆ 3541       ┆ -1.126352                     │
    │ 2024   ┆ Jalen Hurts         ┆ PHI       ┆ 361      ┆ 2903       ┆ 6.305457                      │
    │ 2024   ┆ Sam Darnold         ┆ MIN       ┆ 545      ┆ 4319       ┆ 2.799064                      │
    │ 2024   ┆ Kirk Cousins        ┆ ATL       ┆ 453      ┆ 3508       ┆ 3.442009                      │
    └────────┴─────────────────────┴───────────┴──────────┴────────────┴───────────────────────────────┘



## Current-season helpers


```python
nfl.get_current_season(), nfl.get_current_week()
```




    (2025, 22)



## Schedule via ESPN (live, scoreboard-style)


```python
espn_sched = nfl.espn_nfl_schedule(dates=20240908)
espn_sched.select(['id', 'home_display_name', 'away_display_name', 'home_score', 'away_score']).head()
```




    shape: (5, 5)
    ┌───────────┬────────────────────┬──────────────────────┬────────────┬────────────┐
    │ id        ┆ home_display_name  ┆ away_display_name    ┆ home_score ┆ away_score │
    │ ---       ┆ ---                ┆ ---                  ┆ ---        ┆ ---        │
    │ str       ┆ str                ┆ str                  ┆ str        ┆ str        │
    ╞═══════════╪════════════════════╪══════════════════════╪════════════╪════════════╡
    │ 401671744 ┆ Atlanta Falcons    ┆ Pittsburgh Steelers  ┆ 10         ┆ 18         │
    │ 401671617 ┆ Buffalo Bills      ┆ Arizona Cardinals    ┆ 34         ┆ 28         │
    │ 401671719 ┆ Chicago Bears      ┆ Tennessee Titans     ┆ 24         ┆ 17         │
    │ 401671628 ┆ Cincinnati Bengals ┆ New England Patriots ┆ 10         ┆ 16         │
    │ 401671861 ┆ Indianapolis Colts ┆ Houston Texans       ┆ 27         ┆ 29         │
    └───────────┴────────────────────┴──────────────────────┴────────────┴────────────┘



## Pipeline example: total points per team in 2024

Sum home and away scores for every team across the season.


```python
home = schedules.select(['home_team', 'home_score']).rename({'home_team': 'team', 'home_score': 'pts'})
away = schedules.select(['away_team', 'away_score']).rename({'away_team': 'team', 'away_score': 'pts'})
totals = (
    pl.concat([home, away])
    .drop_nulls('pts')
    .group_by('team')
    .agg(pl.col('pts').sum().alias('points_for'),
         pl.len().alias('games'))
    .sort('points_for', descending=True)
)
totals.head(10)
```




    shape: (10, 3)
    ┌──────┬────────────┬───────┐
    │ team ┆ points_for ┆ games │
    │ ---  ┆ ---        ┆ ---   │
    │ str  ┆ i32        ┆ u32   │
    ╞══════╪════════════╪═══════╡
    │ BUF  ┆ 612        ┆ 20    │
    │ PHI  ┆ 608        ┆ 21    │
    │ DET  ┆ 595        ┆ 18    │
    │ WAS  ┆ 576        ┆ 20    │
    │ BAL  ┆ 571        ┆ 19    │
    │ TB   ┆ 522        ┆ 18    │
    │ CIN  ┆ 472        ┆ 17    │
    │ GB   ┆ 470        ┆ 18    │
    │ KC   ┆ 462        ┆ 20    │
    │ MIN  ┆ 441        ┆ 18    │
    └──────┴────────────┴───────┘



## Clean up the cache


```python
nfl.clear_cache()
```

## Cross-references

- nflverse: <https://nflverse.nflverse.com>
- nflreadpy (Python): <https://github.com/nflverse/nflreadpy>
- nflfastR (R): <https://www.nflfastr.com>
- Plotting: matplotlib, plotnine

## Where to go next

- API docs: `docs/docs/nfl/index.md`
- Next notebook: `04_nba_intro.ipynb`
