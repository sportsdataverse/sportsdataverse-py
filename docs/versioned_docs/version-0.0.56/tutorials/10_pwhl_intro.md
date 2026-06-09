---
title: PWHL tutorial
sidebar_label: PWHL
sidebar_position: 10
---

# PWHL intro — sportsdataverse-py

The Professional Women's Hockey League (PWHL) launched its inaugural season in 2024 with six teams: Boston, Minnesota, Montreal, New York, Ottawa, and Toronto. In `sportsdataverse-py` the PWHL is a **loader-only league** — there are no live API wrappers, just `load_pwhl_*` functions that read pre-built parquet releases (schedules, boxscores, play-by-play, scoring/penalty summaries, rosters, and more) and return polars frames.

R companion: [fastRhockey](https://fastRhockey.sportsdataverse.org) (NHL + PWHL). Part of the [SportsDataverse](https://py.sportsdataverse.org/docs/ecosystem).

## Setup

```sh
pip install sportsdataverse
```


```python
import polars as pl
import sportsdataverse.pwhl as pwhl
```

## Loaders

Every PWHL accessor is a `load_pwhl_*` function. Most take `seasons=[...]` (the inaugural season is `2024`) and all accept `return_as_pandas=True` if you prefer pandas over polars.


```python
[fn for fn in dir(pwhl) if fn.startswith('load_pwhl_')]
```




    ['load_pwhl_game_info',
     'load_pwhl_game_rosters',
     'load_pwhl_games',
     'load_pwhl_goalie_box',
     'load_pwhl_goalie_boxscores',
     'load_pwhl_officials',
     'load_pwhl_pbp',
     'load_pwhl_penalty_summary',
     'load_pwhl_player_box',
     'load_pwhl_player_boxscores',
     'load_pwhl_rosters',
     'load_pwhl_schedule',
     'load_pwhl_schedules',
     'load_pwhl_scoring_summary',
     'load_pwhl_shootout',
     'load_pwhl_shots_by_period',
     'load_pwhl_skater_box',
     'load_pwhl_skater_boxscores',
     'load_pwhl_team_box',
     'load_pwhl_team_boxscores',
     'load_pwhl_three_stars']



## Schedule

`load_pwhl_schedule(seasons=[2024])` returns one row per game with the result and a set of URL/flag columns pointing at the per-game feeds. Note `home_score`/`away_score` are **strings** — cast before arithmetic.


```python
schedule = pwhl.load_pwhl_schedule(seasons=[2024])
schedule.shape
```




    (85, 29)




```python
schedule.select([
    'game_id', 'game_date', 'home_team', 'away_team',
    'home_score', 'away_score', 'winner', 'game_type'
]).head()
```




    shape: (5, 8)
    ┌─────────┬─────────────┬───────────┬───────────┬────────────┬────────────┬───────────┬───────────┐
    │ game_id ┆ game_date   ┆ home_team ┆ away_team ┆ home_score ┆ away_score ┆ winner    ┆ game_type │
    │ ---     ┆ ---         ┆ ---       ┆ ---       ┆ ---        ┆ ---        ┆ ---       ┆ ---       │
    │ str     ┆ str         ┆ str       ┆ str       ┆ str        ┆ str        ┆ str       ┆ str       │
    ╞═════════╪═════════════╪═══════════╪═══════════╪════════════╪════════════╪═══════════╪═══════════╡
    │ 84      ┆ Wed, May 8  ┆ Toronto   ┆ Minnesota ┆ 4          ┆ 0          ┆ Toronto   ┆ playoffs  │
    │ 98      ┆ Wed, May 29 ┆ Boston    ┆ Minnesota ┆ 0          ┆ 3          ┆ Minnesota ┆ playoffs  │
    │ 90      ┆ Wed, May 15 ┆ Minnesota ┆ Toronto   ┆ 1          ┆ 0          ┆ Minnesota ┆ playoffs  │
    │ 63      ┆ Wed, May 1  ┆ Toronto   ┆ Minnesota ┆ 4          ┆ 1          ┆ Toronto   ┆ regular   │
    │ 45      ┆ Wed, Mar 6  ┆ Toronto   ┆ Boston    ┆ 3          ┆ 1          ┆ Toronto   ┆ regular   │
    └─────────┴─────────────┴───────────┴───────────┴────────────┴────────────┴───────────┴───────────┘



## Standings

There is no dedicated standings loader, but the schedule's `winner` column makes a regular-season win count a one-liner.


```python
(schedule
    .filter(pl.col('game_type') == 'regular')
    .group_by('winner')
    .agg(pl.len().alias('wins'))
    .sort('wins', descending=True))
```




    shape: (6, 2)
    ┌───────────┬──────┐
    │ winner    ┆ wins │
    │ ---       ┆ ---  │
    │ str       ┆ u32  │
    ╞═══════════╪══════╡
    │ Toronto   ┆ 17   │
    │ Montreal  ┆ 13   │
    │ Boston    ┆ 12   │
    │ Minnesota ┆ 12   │
    │ Ottawa    ┆ 9    │
    │ New York  ┆ 9    │
    └───────────┴──────┘



## Rosters

`load_pwhl_rosters(seasons=[2024])` gives one row per player per team, split into skaters and goalies via `player_type`.


```python
rosters = pwhl.load_pwhl_rosters(seasons=[2024])
rosters.select([
    'team', 'team_abbr', 'player_type', 'first_name', 'last_name',
    'jersey_number', 'position'
]).head()
```




    shape: (5, 7)
    ┌──────────────┬───────────┬─────────────┬────────────┬───────────┬───────────────┬──────────┐
    │ team         ┆ team_abbr ┆ player_type ┆ first_name ┆ last_name ┆ jersey_number ┆ position │
    │ ---          ┆ ---       ┆ ---         ┆ ---        ┆ ---       ┆ ---           ┆ ---      │
    │ str          ┆ str       ┆ str         ┆ str        ┆ str       ┆ i32           ┆ str      │
    ╞══════════════╪═══════════╪═════════════╪════════════╪═══════════╪═══════════════╪══════════╡
    │ PWHL Toronto ┆ TOR       ┆ skater      ┆ Jocelyne   ┆ Larocque  ┆ 3             ┆ LD       │
    │ PWHL Toronto ┆ TOR       ┆ skater      ┆ Lauriane   ┆ Rougeau   ┆ 5             ┆ LD       │
    │ PWHL Toronto ┆ TOR       ┆ skater      ┆ Kali       ┆ Flanagan  ┆ 6             ┆ RD       │
    │ PWHL Toronto ┆ TOR       ┆ skater      ┆ Olivia     ┆ Knowles   ┆ 7             ┆ RD       │
    │ PWHL Toronto ┆ TOR       ┆ skater      ┆ Alexa      ┆ Vasko     ┆ 10            ┆ C        │
    └──────────────┴───────────┴─────────────┴────────────┴───────────┴───────────────┴──────────┘



## Boxscores

Boxscores come in four flavours: `team_box`, `skater_box`, `goalie_box`, and a combined `player_box`. Each is one row per team/player per game.


```python
team_box = pwhl.load_pwhl_team_box(seasons=[2024])
team_box.select([
    'game_id', 'team', 'team_side', 'goals', 'shots',
    'pp_goals', 'pp_opportunities', 'faceoff_win_pct'
]).head()
```




    shape: (5, 8)
    ┌─────────┬───────────────┬───────────┬───────┬───────┬──────────┬────────────────┬────────────────┐
    │ game_id ┆ team          ┆ team_side ┆ goals ┆ shots ┆ pp_goals ┆ pp_opportuniti ┆ faceoff_win_pc │
    │ ---     ┆ ---           ┆ ---       ┆ ---   ┆ ---   ┆ ---      ┆ es             ┆ t              │
    │ i32     ┆ str           ┆ str       ┆ i32   ┆ i32   ┆ i32      ┆ ---            ┆ ---            │
    │         ┆               ┆           ┆       ┆       ┆          ┆ i32            ┆ f64            │
    ╞═════════╪═══════════════╪═══════════╪═══════╪═══════╪══════════╪════════════════╪════════════════╡
    │ 2       ┆ PWHL Toronto  ┆ home      ┆ 0     ┆ 29    ┆ 0        ┆ 4              ┆ 40.7           │
    │ 2       ┆ PWHL New York ┆ away      ┆ 4     ┆ 28    ┆ 0        ┆ 2              ┆ 59.3           │
    │ 3       ┆ PWHL Ottawa   ┆ home      ┆ 2     ┆ 28    ┆ 1        ┆ 6              ┆ 32.0           │
    │ 3       ┆ PWHL Montreal ┆ away      ┆ 3     ┆ 24    ┆ 0        ┆ 2              ┆ 68.0           │
    │ 4       ┆ PWHL Boston   ┆ home      ┆ 2     ┆ 35    ┆ 1        ┆ 4              ┆ 65.4           │
    └─────────┴───────────────┴───────────┴───────┴───────┴──────────┴────────────────┴────────────────┘




```python
skater_box = pwhl.load_pwhl_skater_box(seasons=[2024])
skater_box.select([
    'game_id', 'first_name', 'last_name', 'position',
    'goals', 'assists', 'points', 'shots', 'plus_minus', 'time_on_ice'
]).head()
```




    shape: (5, 10)
    ┌─────────┬────────────┬───────────┬──────────┬───┬────────┬───────┬────────────┬─────────────┐
    │ game_id ┆ first_name ┆ last_name ┆ position ┆ … ┆ points ┆ shots ┆ plus_minus ┆ time_on_ice │
    │ ---     ┆ ---        ┆ ---       ┆ ---      ┆   ┆ ---    ┆ ---   ┆ ---        ┆ ---         │
    │ i32     ┆ str        ┆ str       ┆ str      ┆   ┆ i32    ┆ i32   ┆ i32        ┆ f64         │
    ╞═════════╪════════════╪═══════════╪══════════╪═══╪════════╪═══════╪════════════╪═════════════╡
    │ 2       ┆ Jocelyne   ┆ Larocque  ┆ LD       ┆ … ┆ 0      ┆ 2     ┆ -2         ┆ 26.7        │
    │ 2       ┆ Lauriane   ┆ Rougeau   ┆ LD       ┆ … ┆ 0      ┆ 0     ┆ 0          ┆ 12.1        │
    │ 2       ┆ Kali       ┆ Flanagan  ┆ RD       ┆ … ┆ 0      ┆ 1     ┆ -1         ┆ 21.6        │
    │ 2       ┆ Olivia     ┆ Knowles   ┆ RD       ┆ … ┆ 0      ┆ 0     ┆ 0          ┆ 9.7         │
    │ 2       ┆ Alexa      ┆ Vasko     ┆ C        ┆ … ┆ 0      ┆ 3     ┆ 0          ┆ 10.5        │
    └─────────┴────────────┴───────────┴──────────┴───┴────────┴───────┴────────────┴─────────────┘




```python
goalie_box = pwhl.load_pwhl_goalie_box(seasons=[2024])
goalie_box.select([
    'game_id', 'first_name', 'last_name',
    'saves', 'shots_against', 'goals_against', 'time_on_ice'
]).head()
```




    shape: (5, 7)
    ┌─────────┬────────────┬────────────┬───────┬───────────────┬───────────────┬─────────────┐
    │ game_id ┆ first_name ┆ last_name  ┆ saves ┆ shots_against ┆ goals_against ┆ time_on_ice │
    │ ---     ┆ ---        ┆ ---        ┆ ---   ┆ ---           ┆ ---           ┆ ---         │
    │ i32     ┆ str        ┆ str        ┆ i32   ┆ i32           ┆ i32           ┆ f64         │
    ╞═════════╪════════════╪════════════╪═══════╪═══════════════╪═══════════════╪═════════════╡
    │ 2       ┆ Erica      ┆ Howe       ┆ 0     ┆ 0             ┆ 0             ┆ null        │
    │ 2       ┆ Kristen    ┆ Campbell   ┆ 24    ┆ 28            ┆ 4             ┆ 60.0        │
    │ 2       ┆ Corinne    ┆ Schroeder  ┆ 29    ┆ 29            ┆ 0             ┆ 60.0        │
    │ 2       ┆ Abbey      ┆ Levy       ┆ 0     ┆ 0             ┆ 0             ┆ null        │
    │ 3       ┆ Sandra     ┆ Abstreiter ┆ 0     ┆ 0             ┆ 0             ┆ null        │
    └─────────┴────────────┴────────────┴───────┴───────────────┴───────────────┴─────────────┘



## Play-by-play

`load_pwhl_pbp(seasons=[2024])` returns a wide (95-column) event log. The `event` column tags each row as `faceoff`, `shot`, `goal`, or `penalty`, and there are multiple coordinate systems (`x_coord`/`y_coord` plus rink-normalized variants).


```python
pbp = pwhl.load_pwhl_pbp(seasons=[2024])
pbp.shape
```




    (10456, 95)




```python
pbp.select([
    'game_id', 'period_of_game', 'clock', 'event',
    'player_name_first', 'player_name_last', 'x_coord', 'y_coord'
]).head()
```




    shape: (5, 8)
    ┌─────────┬────────────────┬───────┬─────────┬────────────────┬────────────────┬─────────┬─────────┐
    │ game_id ┆ period_of_game ┆ clock ┆ event   ┆ player_name_fi ┆ player_name_la ┆ x_coord ┆ y_coord │
    │ ---     ┆ ---            ┆ ---   ┆ ---     ┆ rst            ┆ st             ┆ ---     ┆ ---     │
    │ i32     ┆ str            ┆ str   ┆ str     ┆ ---            ┆ ---            ┆ f64     ┆ f64     │
    │         ┆                ┆       ┆         ┆ str            ┆ str            ┆         ┆         │
    ╞═════════╪════════════════╪═══════╪═════════╪════════════════╪════════════════╪═════════╪═════════╡
    │ 2       ┆ 1              ┆ 20:00 ┆ faceoff ┆ Blayre         ┆ Turnbull       ┆ 0.0     ┆ 0.0     │
    │ 2       ┆ 1              ┆ 18:31 ┆ shot    ┆ Ella           ┆ Shelton        ┆ 49.3333 ┆ 24.65   │
    │ 2       ┆ 1              ┆ 16:30 ┆ shot    ┆ Jessie         ┆ Eldridge       ┆ 83.0    ┆ -9.0667 │
    │ 2       ┆ 1              ┆ 16:24 ┆ shot    ┆ Olivia         ┆ Zafuto         ┆ 29.0    ┆ -21.25  │
    │ 2       ┆ 1              ┆ 15:30 ┆ faceoff ┆ Emma           ┆ Maltais        ┆ 52.3333 ┆ 28.6167 │
    └─────────┴────────────────┴───────┴─────────┴────────────────┴────────────────┴─────────┴─────────┘




```python
(pbp
    .group_by('event')
    .agg(pl.len().alias('events'))
    .sort('events', descending=True))
```




    shape: (4, 2)
    ┌─────────┬────────┐
    │ event   ┆ events │
    │ ---     ┆ ---    │
    │ str     ┆ u32    │
    ╞═════════╪════════╡
    │ shot    ┆ 4922   │
    │ faceoff ┆ 4631   │
    │ penalty ┆ 518    │
    │ goal    ┆ 385    │
    └─────────┴────────┘



## Scoring & penalty summaries

`load_pwhl_scoring_summary` is a tidy goal log (scorer + up to two assists, plus situation flags like power play / short handed). `load_pwhl_three_stars` carries the post-game three-star selections.


```python
scoring = pwhl.load_pwhl_scoring_summary(seasons=[2024])
scoring.select([
    'game_id', 'period', 'time', 'team_abbr',
    'scorer_first', 'scorer_last', 'is_power_play', 'is_game_winning'
]).head()
```




    shape: (5, 8)
    ┌─────────┬────────┬───────┬───────────┬──────────────┬─────────────┬───────────────┬──────────────┐
    │ game_id ┆ period ┆ time  ┆ team_abbr ┆ scorer_first ┆ scorer_last ┆ is_power_play ┆ is_game_winn │
    │ ---     ┆ ---    ┆ ---   ┆ ---       ┆ ---          ┆ ---         ┆ ---           ┆ ing          │
    │ i32     ┆ str    ┆ str   ┆ str       ┆ str          ┆ str         ┆ i32           ┆ ---          │
    │         ┆        ┆       ┆           ┆              ┆             ┆               ┆ i32          │
    ╞═════════╪════════╪═══════╪═══════════╪══════════════╪═════════════╪═══════════════╪══════════════╡
    │ 2       ┆ 1st    ┆ 10:43 ┆ NY        ┆ Ella         ┆ Shelton     ┆ 0             ┆ 1            │
    │ 2       ┆ 3rd    ┆ 2:53  ┆ NY        ┆ Alex         ┆ Carpenter   ┆ 0             ┆ 0            │
    │ 2       ┆ 3rd    ┆ 4:57  ┆ NY        ┆ Jill         ┆ Saulnier    ┆ 0             ┆ 0            │
    │ 2       ┆ 3rd    ┆ 7:42  ┆ NY        ┆ Kayla        ┆ Vespa       ┆ 0             ┆ 0            │
    │ 3       ┆ 2nd    ┆ 16:24 ┆ OTT       ┆ Hayley       ┆ Scamurra    ┆ 1             ┆ 0            │
    └─────────┴────────┴───────┴───────────┴──────────────┴─────────────┴───────────────┴──────────────┘



## Pipeline example: season scoring leaders

Aggregate the skater boxscore across all games to build a points leaderboard — the inaugural-season top of the table.


```python
(skater_box
    .group_by(['player_id', 'first_name', 'last_name'])
    .agg(
        pl.col('goals').sum().alias('goals'),
        pl.col('assists').sum().alias('assists'),
        pl.col('points').sum().alias('points'),
    )
    .sort('points', descending=True)
    .select(['first_name', 'last_name', 'goals', 'assists', 'points'])
    .head(10))
```




    shape: (10, 5)
    ┌──────────────┬─────────────────┬───────┬─────────┬────────┐
    │ first_name   ┆ last_name       ┆ goals ┆ assists ┆ points │
    │ ---          ┆ ---             ┆ ---   ┆ ---     ┆ ---    │
    │ str          ┆ str             ┆ i32   ┆ i32     ┆ i32    │
    ╞══════════════╪═════════════════╪═══════╪═════════╪════════╡
    │ Natalie      ┆ Spooner         ┆ 21    ┆ 8       ┆ 29     │
    │ Marie-Philip ┆ Poulin          ┆ 11    ┆ 14      ┆ 25     │
    │ Sarah        ┆ Nurse           ┆ 11    ┆ 13      ┆ 24     │
    │ Alex         ┆ Carpenter       ┆ 8     ┆ 15      ┆ 23     │
    │ Emma         ┆ Maltais         ┆ 5     ┆ 16      ┆ 21     │
    │ Ella         ┆ Shelton         ┆ 7     ┆ 14      ┆ 21     │
    │ Taylor       ┆ Heise           ┆ 9     ┆ 12      ┆ 21     │
    │ Grace        ┆ Zumwinkle       ┆ 12    ┆ 8       ┆ 20     │
    │ Kendall      ┆ Coyne Schofield ┆ 7     ┆ 13      ┆ 20     │
    │ Erin         ┆ Ambrose         ┆ 4     ┆ 16      ┆ 20     │
    └──────────────┴─────────────────┴───────┴─────────┴────────┘



## Cross-references

- R companion: [fastRhockey](https://fastRhockey.sportsdataverse.org) (NHL + PWHL)
- Data source: PWHL HockeyTech feeds, packaged as parquet in the sportsdataverse-data releases
- Plotting: matplotlib, plotnine (rink plots from the `x_coord`/`y_coord` PBP columns)

## Where to go next

- API docs: `docs/docs/pwhl/index.md`
- For the men's game and the modern NHL APIs, see `07_nhl_intro.ipynb`.
