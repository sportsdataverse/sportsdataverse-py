---
title: MBB tutorial
sidebar_label: MBB
sidebar_position: 4
---

# MBB intro — sportsdataverse-py

ESPN-backed NCAA men's basketball: play-by-play, schedule, teams, game rosters. The `espn_mbb_*` surface mirrors the NBA wrappers — same shape, different league.

R companion: [hoopR](https://hoopR.sportsdataverse.org) (men's basketball: NBA + NCAA). Part of the [SportsDataverse](https://py.sportsdataverse.org/docs/ecosystem).

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
teams = sdv.mbb.espn_mbb_teams()
teams.shape
```




    (362, 14)




```python
teams.select(['team_id', 'team_location', 'team_name', 'team_abbreviation']).head()
```




    shape: (5, 4)
    ┌─────────┬───────────────────┬──────────────┬───────────────────┐
    │ team_id ┆ team_location     ┆ team_name    ┆ team_abbreviation │
    │ ---     ┆ ---               ┆ ---          ┆ ---               │
    │ str     ┆ str               ┆ str          ┆ str               │
    ╞═════════╪═══════════════════╪══════════════╪═══════════════════╡
    │ 2000    ┆ Abilene Christian ┆ Wildcats     ┆ ACU               │
    │ 2005    ┆ Air Force         ┆ Falcons      ┆ AF                │
    │ 2006    ┆ Akron             ┆ Zips         ┆ AKR               │
    │ 2010    ┆ Alabama A&M       ┆ Bulldogs     ┆ AAMU              │
    │ 333     ┆ Alabama           ┆ Crimson Tide ┆ ALA               │
    └─────────┴───────────────────┴──────────────┴───────────────────┘



## Schedule


```python
schedule = sdv.mbb.espn_mbb_schedule(dates=20240408)  # 2024 national championship day
schedule.select(['id', 'home_display_name', 'away_display_name', 'home_score', 'away_score']).head()
```




    shape: (1, 5)
    ┌───────────┬───────────────────┬─────────────────────┬────────────┬────────────┐
    │ id        ┆ home_display_name ┆ away_display_name   ┆ home_score ┆ away_score │
    │ ---       ┆ ---               ┆ ---                 ┆ ---        ┆ ---        │
    │ str       ┆ str               ┆ str                 ┆ str        ┆ str        │
    ╞═══════════╪═══════════════════╪═════════════════════╪════════════╪════════════╡
    │ 401638645 ┆ UConn Huskies     ┆ Purdue Boilermakers ┆ 75         ┆ 60         │
    └───────────┴───────────────────┴─────────────────────┴────────────┴────────────┘



## Multi-season parquet loader


```python
schedule_2024 = sdv.mbb.load_mbb_schedule(seasons=[2024])
schedule_2024.shape
```




    (6249, 84)



## Play-by-play — 2024 men's national championship


```python
pbp = sdv.mbb.espn_mbb_pbp(game_id=401638636)
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
    │ 1             ┆ 20:00              ┆ Jump Ball won by Tennessee      ┆ false       │
    │ 1             ┆ 19:50              ┆ Jahmai Mashack missed Three Po… ┆ false       │
    │ 1             ┆ 19:50              ┆ Ryan Kalkbrenner Defensive Reb… ┆ false       │
    │ 1             ┆ 19:27              ┆ Ryan Kalkbrenner made Layup. A… ┆ true        │
    │ 1             ┆ 19:10              ┆ Zakai Zeigler missed Three Poi… ┆ false       │
    └───────────────┴────────────────────┴─────────────────────────────────┴─────────────┘



## Game rosters


```python
rosters = sdv.mbb.espn_mbb_game_rosters(game_id=401638636)
rosters.select(['athlete_id', 'athlete_display_name', 'team_abbreviation', 'starter']).head()
```




    shape: (5, 4)
    ┌────────────┬──────────────────────┬───────────────────┬─────────┐
    │ athlete_id ┆ athlete_display_name ┆ team_abbreviation ┆ starter │
    │ ---        ┆ ---                  ┆ ---               ┆ ---     │
    │ i64        ┆ str                  ┆ str               ┆ bool    │
    ╞════════════╪══════════════════════╪═══════════════════╪═════════╡
    │ 4684682    ┆ Jonas Aidoo          ┆ TENN              ┆ true    │
    │ 4897943    ┆ Dalton Knecht        ┆ TENN              ┆ true    │
    │ 4883573    ┆ Zakai Zeigler        ┆ TENN              ┆ true    │
    │ 4683934    ┆ Jahmai Mashack       ┆ TENN              ┆ true    │
    │ 4431685    ┆ Josiah-Jordan James  ┆ TENN              ┆ true    │
    └────────────┴──────────────────────┴───────────────────┴─────────┘



## Polars summary — teams by active status

The ESPN teams frame doesn't carry a conference column, so here's a simple grouped
count as a polars warm-up.


```python
(teams
    .group_by('team_is_active')
    .agg(pl.len().alias('teams'))
    .sort('teams', descending=True))
```




    shape: (1, 2)
    ┌────────────────┬───────┐
    │ team_is_active ┆ teams │
    │ ---            ┆ ---   │
    │ bool           ┆ u32   │
    ╞════════════════╪═══════╡
    │ true           ┆ 362   │
    └────────────────┴───────┘



## Pipeline example: highest-scoring tournament games

Pull a date window covering March Madness 2024 and rank by total points.


```python
march = sdv.mbb.espn_mbb_schedule(dates='20240321-20240408')  # March Madness 2024 window
(march
    .with_columns((pl.col('home_score').cast(pl.Int64, strict=False) + pl.col('away_score').cast(pl.Int64, strict=False)).alias('total'))
    .sort('total', descending=True)
    .select(['date', 'home_display_name', 'away_display_name', 'home_score', 'away_score', 'total'])
    .head(10))
```




    shape: (10, 6)
    ┌───────────────────┬──────────────────────┬─────────────────────┬────────────┬────────────┬───────┐
    │ date              ┆ home_display_name    ┆ away_display_name   ┆ home_score ┆ away_score ┆ total │
    │ ---               ┆ ---                  ┆ ---                 ┆ ---        ┆ ---        ┆ ---   │
    │ str               ┆ str                  ┆ str                 ┆ str        ┆ str        ┆ i64   │
    ╞═══════════════════╪══════════════════════╪═════════════════════╪════════════╪════════════╪═══════╡
    │ 2024-03-23T00:03Z ┆ Alabama Crimson Tide ┆ Charleston Cougars  ┆ 109        ┆ 96         ┆ 205   │
    │ 2024-03-22T20:38Z ┆ Florida Gators       ┆ Colorado Buffaloes  ┆ 100        ┆ 102        ┆ 202   │
    │ 2024-03-25T00:40Z ┆ Houston Cougars      ┆ Texas A&M Aggies    ┆ 100        ┆ 95         ┆ 195   │
    │ 2024-04-02T23:00Z ┆ Indiana State        ┆ Utah Utes           ┆ 100        ┆ 90         ┆ 190   │
    │                   ┆ Sycamores            ┆                     ┆            ┆            ┆       │
    │ 2024-03-22T02:20Z ┆ Kansas Jayhawks      ┆ Samford Bulldogs    ┆ 93         ┆ 89         ┆ 182   │
    │ 2024-03-22T23:00Z ┆ Nebraska Cornhuskers ┆ Texas A&M Aggies    ┆ 83         ┆ 98         ┆ 181   │
    │ 2024-03-29T01:54Z ┆ North Carolina Tar   ┆ Alabama Crimson     ┆ 87         ┆ 89         ┆ 176   │
    │                   ┆ Heels                ┆ Tide                ┆            ┆            ┆       │
    │ 2024-03-24T18:47Z ┆ Purdue Boilermakers  ┆ Utah State Aggies   ┆ 106        ┆ 67         ┆ 173   │
    │ 2024-03-25T01:00Z ┆ Utah Utes            ┆ Iowa Hawkeyes       ┆ 91         ┆ 82         ┆ 173   │
    │ 2024-03-31T00:49Z ┆ Alabama Crimson Tide ┆ Clemson Tigers      ┆ 89         ┆ 82         ┆ 171   │
    └───────────────────┴──────────────────────┴─────────────────────┴────────────┴────────────┴───────┘



## Cross-references

- R companion: [hoopR](https://hoopR.sportsdataverse.org)
- Data source: ESPN MBB API
- Plotting: matplotlib, plotnine

## Where to go next

- API docs: `docs/docs/mbb/index.md`
- Next notebook: `07_nhl_intro.ipynb`
