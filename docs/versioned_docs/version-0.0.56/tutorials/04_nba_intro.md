---
title: NBA tutorial
sidebar_label: NBA
sidebar_position: 2
---

# NBA intro — sportsdataverse-py

ESPN-backed NBA data: play-by-play, schedule, teams, game rosters. Wrappers follow the `espn_nba_*` pattern; pre-built datasets load via `load_nba_*`.

R companion: [hoopR](https://hoopR.sportsdataverse.org). Python neighbor: [nba_api](https://github.com/swar/nba_api) (NBA Stats endpoints). Part of the [SportsDataverse](https://py.sportsdataverse.org/docs/ecosystem).

## Setup

```sh
pip install sportsdataverse
```


```python
import polars as pl
import sportsdataverse as sdv
```

## Teams


```python
teams = sdv.nba.espn_nba_teams()
teams.shape
```




    (30, 14)




```python
teams.select(['team_id', 'team_location', 'team_name', 'team_abbreviation']).head()
```




    shape: (5, 4)
    ┌─────────┬───────────────┬───────────┬───────────────────┐
    │ team_id ┆ team_location ┆ team_name ┆ team_abbreviation │
    │ ---     ┆ ---           ┆ ---       ┆ ---               │
    │ str     ┆ str           ┆ str       ┆ str               │
    ╞═════════╪═══════════════╪═══════════╪═══════════════════╡
    │ 1       ┆ Atlanta       ┆ Hawks     ┆ ATL               │
    │ 2       ┆ Boston        ┆ Celtics   ┆ BOS               │
    │ 17      ┆ Brooklyn      ┆ Nets      ┆ BKN               │
    │ 30      ┆ Charlotte     ┆ Hornets   ┆ CHA               │
    │ 4       ┆ Chicago       ┆ Bulls     ┆ CHI               │
    └─────────┴───────────────┴───────────┴───────────────────┘



## Schedule (ESPN scoreboard, single date)


```python
schedule = sdv.nba.espn_nba_schedule(dates=20240606)  # 2024 NBA Finals, Game 1
schedule.select(['id', 'home_display_name', 'away_display_name', 'home_score', 'away_score']).head()
```




    shape: (1, 5)
    ┌───────────┬───────────────────┬───────────────────┬────────────┬────────────┐
    │ id        ┆ home_display_name ┆ away_display_name ┆ home_score ┆ away_score │
    │ ---       ┆ ---               ┆ ---               ┆ ---        ┆ ---        │
    │ str       ┆ str               ┆ str               ┆ str        ┆ str        │
    ╞═══════════╪═══════════════════╪═══════════════════╪════════════╪════════════╡
    │ 401656359 ┆ Boston Celtics    ┆ Dallas Mavericks  ┆ 107        ┆ 89         │
    └───────────┴───────────────────┴───────────────────┴────────────┴────────────┘



## Multi-season schedule via the parquet loader


```python
schedule_2024 = sdv.nba.load_nba_schedule(seasons=[2024])
schedule_2024.shape
```




    (1322, 77)



## Play-by-play

`espn_nba_pbp(game_id=...)` returns a dict with the full game payload.


```python
pbp = sdv.nba.espn_nba_pbp(game_id=401585660)
list(pbp.keys())[:8]
```




    ['gameId',
     'plays',
     'winprobability',
     'boxscore',
     'header',
     'format',
     'broadcasts',
     'videos']




```python
plays = pl.DataFrame(pbp['plays'], infer_schema_length=None)
plays.select(['period.number', 'clock.displayValue', 'text', 'scoringPlay']).head()
```




    shape: (5, 4)
    ┌───────────────┬────────────────────┬─────────────────────────────────┬─────────────┐
    │ period.number ┆ clock.displayValue ┆ text                            ┆ scoringPlay │
    │ ---           ┆ ---                ┆ ---                             ┆ ---         │
    │ i64           ┆ str                ┆ str                             ┆ bool        │
    ╞═══════════════╪════════════════════╪═════════════════════════════════╪═════════════╡
    │ 1             ┆ 12:00              ┆ Myles Turner vs. Anthony Davis… ┆ false       │
    │ 1             ┆ 11:42              ┆ Aaron Nesmith makes 26-foot th… ┆ true        │
    │ 1             ┆ 11:17              ┆ Austin Reaves misses driving l… ┆ false       │
    │ 1             ┆ 11:14              ┆ Austin Reaves offensive reboun… ┆ false       │
    │ 1             ┆ 11:12              ┆ Austin Reaves misses 14-foot t… ┆ false       │
    └───────────────┴────────────────────┴─────────────────────────────────┴─────────────┘



## Game rosters


```python
rosters = sdv.nba.espn_nba_game_rosters(game_id=401585660)
rosters.select(['athlete_id', 'athlete_display_name', 'team_abbreviation', 'starter']).head()
```




    shape: (5, 4)
    ┌────────────┬──────────────────────┬───────────────────┬─────────┐
    │ athlete_id ┆ athlete_display_name ┆ team_abbreviation ┆ starter │
    │ ---        ┆ ---                  ┆ ---               ┆ ---     │
    │ i64        ┆ str                  ┆ str               ┆ bool    │
    ╞════════════╪══════════════════════╪═══════════════════╪═════════╡
    │ 1966       ┆ LeBron James         ┆ LAL               ┆ true    │
    │ 6583       ┆ Anthony Davis        ┆ LAL               ┆ true    │
    │ 4066648    ┆ Rui Hachimura        ┆ LAL               ┆ true    │
    │ 2580782    ┆ Spencer Dinwiddie    ┆ LAL               ┆ true    │
    │ 4066457    ┆ Austin Reaves        ┆ LAL               ┆ true    │
    └────────────┴──────────────────────┴───────────────────┴─────────┘



## Shot-distance distribution (no chart)

Bucket every shot by distance and count attempts. The user can render the result with their preferred plotting library.


```python
shots = (
    plays
    .filter(pl.col('shootingPlay') == True)
    .with_columns(
        pl.when(pl.col('coordinate.x').is_null()).then(None)
          .otherwise(
              ((pl.col('coordinate.x').cast(pl.Float64, strict=False) ** 2 +
                pl.col('coordinate.y').cast(pl.Float64, strict=False) ** 2) ** 0.5).round(0)
          )
          .alias('shot_distance')
    )
)
shots.select(['period.number', 'text', 'coordinate.x', 'coordinate.y', 'shot_distance']).head()
```




    shape: (5, 5)
    ┌───────────────┬─────────────────────────────────┬──────────────┬──────────────┬───────────────┐
    │ period.number ┆ text                            ┆ coordinate.x ┆ coordinate.y ┆ shot_distance │
    │ ---           ┆ ---                             ┆ ---          ┆ ---          ┆ ---           │
    │ i64           ┆ str                             ┆ i64          ┆ i64          ┆ f64           │
    ╞═══════════════╪═════════════════════════════════╪══════════════╪══════════════╪═══════════════╡
    │ 1             ┆ Aaron Nesmith makes 26-foot th… ┆ 10           ┆ 22           ┆ 24.0          │
    │ 1             ┆ Austin Reaves misses driving l… ┆ 26           ┆ 9            ┆ 28.0          │
    │ 1             ┆ Austin Reaves misses 14-foot t… ┆ 10           ┆ 2            ┆ 10.0          │
    │ 1             ┆ Rui Hachimura blocks Pascal Si… ┆ 16           ┆ 2            ┆ 16.0          │
    │ 1             ┆ Spencer Dinwiddie makes 2-foot… ┆ 25           ┆ 3            ┆ 25.0          │
    └───────────────┴─────────────────────────────────┴──────────────┴──────────────┴───────────────┘




```python
(shots
    .with_columns(pl.col('shot_distance').cut([5, 10, 15, 20, 25], labels=['0-5','6-10','11-15','16-20','21-25','25+']).alias('bucket'))
    .group_by('bucket')
    .agg(pl.len().alias('attempts'),
         pl.col('scoringPlay').sum().alias('makes'))
    .sort('bucket'))
```




    shape: (6, 3)
    ┌────────┬──────────┬───────┐
    │ bucket ┆ attempts ┆ makes │
    │ ---    ┆ ---      ┆ ---   │
    │ enum   ┆ u32      ┆ u32   │
    ╞════════╪══════════╪═══════╡
    │ 0-5    ┆ 10       ┆ 5     │
    │ 6-10   ┆ 2        ┆ 1     │
    │ 11-15  ┆ 1        ┆ 1     │
    │ 16-20  ┆ 11       ┆ 3     │
    │ 21-25  ┆ 82       ┆ 49    │
    │ 25+    ┆ 149      ┆ 97    │
    └────────┴──────────┴───────┘



## Pipeline example: highest-scoring games of a date range

Pull a multi-day window via `dates=` (range form `YYYYMMDD-YYYYMMDD`).


```python
window = sdv.nba.espn_nba_schedule(dates='20240606-20240617')  # 2024 Finals window
(window
    .with_columns((pl.col('home_score').cast(pl.Int64, strict=False) + pl.col('away_score').cast(pl.Int64, strict=False)).alias('total'))
    .sort('total', descending=True)
    .select(['date', 'home_display_name', 'away_display_name', 'home_score', 'away_score', 'total'])
    .head())
```




    shape: (5, 6)
    ┌───────────────────┬───────────────────┬───────────────────┬────────────┬────────────┬───────┐
    │ date              ┆ home_display_name ┆ away_display_name ┆ home_score ┆ away_score ┆ total │
    │ ---               ┆ ---               ┆ ---               ┆ ---        ┆ ---        ┆ ---   │
    │ str               ┆ str               ┆ str               ┆ str        ┆ str        ┆ i64   │
    ╞═══════════════════╪═══════════════════╪═══════════════════╪════════════╪════════════╪═══════╡
    │ 2024-06-15T00:30Z ┆ Dallas Mavericks  ┆ Boston Celtics    ┆ 122        ┆ 84         ┆ 206   │
    │ 2024-06-13T00:30Z ┆ Dallas Mavericks  ┆ Boston Celtics    ┆ 99         ┆ 106        ┆ 205   │
    │ 2024-06-10T00:00Z ┆ Boston Celtics    ┆ Dallas Mavericks  ┆ 105        ┆ 98         ┆ 203   │
    │ 2024-06-07T00:30Z ┆ Boston Celtics    ┆ Dallas Mavericks  ┆ 107        ┆ 89         ┆ 196   │
    │ 2024-06-18T00:30Z ┆ Boston Celtics    ┆ Dallas Mavericks  ┆ 106        ┆ 88         ┆ 194   │
    └───────────────────┴───────────────────┴───────────────────┴────────────┴────────────┴───────┘



## Cross-references

- R companion: [hoopR](https://hoopR.sportsdataverse.org)
- Data source: ESPN NBA API
- Stats-API alternative (Python): [nba_api](https://github.com/swar/nba_api)
- Plotting: matplotlib, plotnine

## Where to go next

- API docs: `docs/docs/nba/index.md`
- Next notebook: `05_wbb_intro.ipynb`
