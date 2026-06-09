---
title: CFB tutorial
sidebar_label: CFB
sidebar_position: 7
---

# CFB intro — sportsdataverse-py

ESPN-backed college football: play-by-play, schedule, teams, and per-play participant resolution. Wrappers follow the `espn_cfb_*` pattern; pre-built datasets load via `load_cfb_*`.

R companion: [cfbfastR](https://cfbfastR.sportsdataverse.org) — the same verbs in R. Part of the [SportsDataverse](https://py.sportsdataverse.org/docs/ecosystem).

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
teams = sdv.cfb.espn_cfb_teams()
teams.shape
```




    (755, 14)




```python
teams.select(['team_id', 'team_location', 'team_name', 'team_abbreviation']).head()
```




    shape: (5, 4)
    ┌─────────┬───────────────────┬───────────┬───────────────────┐
    │ team_id ┆ team_location     ┆ team_name ┆ team_abbreviation │
    │ ---     ┆ ---               ┆ ---       ┆ ---               │
    │ str     ┆ str               ┆ str       ┆ str               │
    ╞═════════╪═══════════════════╪═══════════╪═══════════════════╡
    │ 2000    ┆ Abilene Christian ┆ Wildcats  ┆ ACU               │
    │ 2001    ┆ Adams State       ┆ Grizzlies ┆ ADSU              │
    │ 2003    ┆ Adrian            ┆ Bulldogs  ┆ ADR               │
    │ 2005    ┆ Air Force         ┆ Falcons   ┆ AF                │
    │ 2006    ┆ Akron             ┆ Zips      ┆ AKR               │
    └─────────┴───────────────────┴───────────┴───────────────────┘



## Schedule


```python
schedule = sdv.cfb.espn_cfb_schedule(dates=20240831)  # opening-Saturday slate
schedule.shape
```




    (68, 77)




```python
(schedule
 .select(['id', 'date', 'home_display_name', 'away_display_name', 'home_score', 'away_score'])
 .head())
```




    shape: (5, 6)
    ┌───────────┬───────────────────┬────────────────────┬───────────────────┬────────────┬────────────┐
    │ id        ┆ date              ┆ home_display_name  ┆ away_display_name ┆ home_score ┆ away_score │
    │ ---       ┆ ---               ┆ ---                ┆ ---               ┆ ---        ┆ ---        │
    │ str       ┆ str               ┆ str                ┆ str               ┆ str        ┆ str        │
    ╞═══════════╪═══════════════════╪════════════════════╪═══════════════════╪════════════╪════════════╡
    │ 401628323 ┆ 2024-08-31T16:00Z ┆ Georgia Bulldogs   ┆ Clemson Tigers    ┆ 34         ┆ 3          │
    │ 401628455 ┆ 2024-08-31T19:30Z ┆ Ohio State         ┆ Akron Zips        ┆ 52         ┆ 6          │
    │           ┆                   ┆ Buckeyes           ┆                   ┆            ┆            │
    │ 401628456 ┆ 2024-08-31T23:30Z ┆ Oregon Ducks       ┆ Idaho Vandals     ┆ 24         ┆ 14         │
    │ 401628331 ┆ 2024-08-31T19:30Z ┆ Texas Longhorns    ┆ Colorado State    ┆ 52         ┆ 0          │
    │           ┆                   ┆                    ┆ Rams              ┆            ┆            │
    │ 401628319 ┆ 2024-08-31T23:00Z ┆ Alabama Crimson    ┆ Western Kentucky  ┆ 63         ┆ 0          │
    │           ┆                   ┆ Tide               ┆ Hilltoppers       ┆            ┆            │
    └───────────┴───────────────────┴────────────────────┴───────────────────┴────────────┴────────────┘



## Play-by-play

`CFBPlayProcess(gameId=...)` drives the full ESPN college-football PBP pipeline: call `.espn_cfb_pbp()` to fetch the raw game summary, then `.run_processing_pipeline()` returns a dict whose `plays` key is the processed play list (EPA/WPA, down & distance, play types) alongside an `advBoxScore` and game/team metadata.


```python
from sportsdataverse.cfb import CFBPlayProcess

game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()  # fetch the raw ESPN game summary
processed = game.run_processing_pipeline()  # full PBP feature pipeline (EPA/WPA + adv box score)
list(processed.keys())[:8]
```




    ['gameId',
     'plays',
     'season',
     'week',
     'gameInfo',
     'teamInfo',
     'playByPlaySource',
     'drives']




```python
plays = pl.DataFrame(processed['plays'], infer_schema_length=None)
plays.select(['period', 'clock.displayValue', 'pos_team', 'down', 'distance', 'text', 'scoring_play', 'EPA']).head()
```




    shape: (5, 8)
    ┌────────┬────────────────┬──────────┬──────┬──────────┬────────────────┬──────────────┬───────────┐
    │ period ┆ clock.displayV ┆ pos_team ┆ down ┆ distance ┆ text           ┆ scoring_play ┆ EPA       │
    │ ---    ┆ alue           ┆ ---      ┆ ---  ┆ ---      ┆ ---            ┆ ---          ┆ ---       │
    │ i64    ┆ ---            ┆ i64      ┆ i64  ┆ i64      ┆ str            ┆ bool         ┆ f64       │
    │        ┆ str            ┆          ┆      ┆          ┆                ┆              ┆           │
    ╞════════╪════════════════╪══════════╪══════╪══════════╪════════════════╪══════════════╪═══════════╡
    │ 1      ┆ 15:00          ┆ 99       ┆ 1    ┆ 10       ┆ Michael Lantz  ┆ false        ┆ -1.309487 │
    │        ┆                ┆          ┆      ┆          ┆ kickoff for 58 ┆              ┆           │
    │        ┆                ┆          ┆      ┆          ┆ y…             ┆              ┆           │
    │ 1      ┆ 14:55          ┆ 99       ┆ 1    ┆ 10       ┆ Garrett        ┆ false        ┆ 1.130336  │
    │        ┆                ┆          ┆      ┆          ┆ Nussmeier pass ┆              ┆           │
    │        ┆                ┆          ┆      ┆          ┆ complet…       ┆              ┆           │
    │ 1      ┆ 14:24          ┆ 99       ┆ 1    ┆ 10       ┆ Garrett        ┆ false        ┆ 0.963541  │
    │        ┆                ┆          ┆      ┆          ┆ Nussmeier pass ┆              ┆           │
    │        ┆                ┆          ┆      ┆          ┆ complet…       ┆              ┆           │
    │ 1      ┆ 14:03          ┆ 99       ┆ 1    ┆ 10       ┆ Josh Williams  ┆ false        ┆ -0.674958 │
    │        ┆                ┆          ┆      ┆          ┆ run for 2 yds  ┆              ┆           │
    │        ┆                ┆          ┆      ┆          ┆ to…            ┆              ┆           │
    │ 1      ┆ 13:29          ┆ 99       ┆ 2    ┆ 8        ┆ Garrett        ┆ false        ┆ 0.250361  │
    │        ┆                ┆          ┆      ┆          ┆ Nussmeier pass ┆              ┆           │
    │        ┆                ┆          ┆      ┆          ┆ complet…       ┆              ┆           │
    └────────┴────────────────┴──────────┴──────┴──────────┴────────────────┴──────────────┴───────────┘



## Play participants (the `resolve_missing` flag)

`espn_cfb_play_participants` returns a per-play long frame of athletes who participated in each play. By default it falls back to the canonical ESPN `$ref` URL when the sidecar omits an athlete; set `resolve_missing=False` to skip that fan-out.


```python
participants = sdv.cfb.espn_cfb_play_participants(
    game_id=401628334,
    resolve_missing=True,
    resolve_missing_max=20,
)
participants.shape
```




    (158, 54)




```python
participants.head()
```




    shape: (5, 54)
    ┌───────────┬───────────┬───────────┬───────────┬───┬───────────┬───────────┬───────────┬──────────┐
    │ game_id   ┆ play_id   ┆ kicker_pl ┆ returner_ ┆ … ┆ forced_by ┆ sacked_by ┆ tackler_p ┆ pass_def │
    │ ---       ┆ ---       ┆ ayer_name ┆ player_na ┆   ┆ _player_i ┆ _player_i ┆ layer_ids ┆ ender_pl │
    │ i64       ┆ i64       ┆ ---       ┆ me        ┆   ┆ ds        ┆ ds        ┆ ---       ┆ ayer_ids │
    │           ┆           ┆ str       ┆ ---       ┆   ┆ ---       ┆ ---       ┆ list[str] ┆ ---      │
    │           ┆           ┆           ┆ str       ┆   ┆ list[str] ┆ list[str] ┆           ┆ list[str │
    │           ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆ ]        │
    ╞═══════════╪═══════════╪═══════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪══════════╡
    │ 401628334 ┆ 401628334 ┆ Michael   ┆ Zavion    ┆ … ┆ []        ┆ []        ┆ []        ┆ []       │
    │           ┆ 101849902 ┆ Lantz     ┆ Thomas    ┆   ┆           ┆           ┆           ┆          │
    │ 401628334 ┆ 401628334 ┆ null      ┆ null      ┆ … ┆ []        ┆ []        ┆ []        ┆ []       │
    │           ┆ 101854401 ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
    │ 401628334 ┆ 401628334 ┆ null      ┆ null      ┆ … ┆ []        ┆ []        ┆ []        ┆ []       │
    │           ┆ 101857501 ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
    │ 401628334 ┆ 401628334 ┆ null      ┆ null      ┆ … ┆ []        ┆ []        ┆ []        ┆ []       │
    │           ┆ 101859601 ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
    │ 401628334 ┆ 401628334 ┆ null      ┆ null      ┆ … ┆ []        ┆ []        ┆ []        ┆ []       │
    │           ┆ 101867001 ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
    └───────────┴───────────┴───────────┴───────────┴───┴───────────┴───────────┴───────────┴──────────┘



## Multi-season schedule via the loader


```python
schedule_2023 = sdv.cfb.load_cfb_schedule(seasons=[2023])
schedule_2023.shape
```




    (3734, 31)



## Pipeline example: highest-scoring games on the slate

Combine the schedule frame with simple polars expressions. ESPN returns scores as
strings, so cast before arithmetic.


```python
(schedule
    .with_columns([
        pl.col('home_score').cast(pl.Int64, strict=False),
        pl.col('away_score').cast(pl.Int64, strict=False),
    ])
    .with_columns((pl.col('home_score') + pl.col('away_score')).alias('total_points'))
    .sort('total_points', descending=True)
    .select(['date', 'home_display_name', 'away_display_name', 'home_score', 'away_score', 'total_points'])
    .head(10))
```




    shape: (10, 6)
    ┌───────────────────┬──────────────────┬──────────────────┬────────────┬────────────┬──────────────┐
    │ date              ┆ home_display_nam ┆ away_display_nam ┆ home_score ┆ away_score ┆ total_points │
    │ ---               ┆ e                ┆ e                ┆ ---        ┆ ---        ┆ ---          │
    │ str               ┆ ---              ┆ ---              ┆ i64        ┆ i64        ┆ i64          │
    │                   ┆ str              ┆ str              ┆            ┆            ┆              │
    ╞═══════════════════╪══════════════════╪══════════════════╪════════════╪════════════╪══════════════╡
    │ 2024-08-31T23:30Z ┆ Texas Tech Red   ┆ Abilene          ┆ 52         ┆ 51         ┆ 103          │
    │                   ┆ Raiders          ┆ Christian        ┆            ┆            ┆              │
    │                   ┆                  ┆ Wildcats         ┆            ┆            ┆              │
    │ 2024-08-31T20:00Z ┆ Georgia Southern ┆ Boise State      ┆ 45         ┆ 56         ┆ 101          │
    │                   ┆ Eagles           ┆ Broncos          ┆            ┆            ┆              │
    │ 2024-09-01T02:30Z ┆ Arizona Wildcats ┆ New Mexico Lobos ┆ 61         ┆ 39         ┆ 100          │
    │ 2024-08-31T19:00Z ┆ Washington State ┆ Portland State   ┆ 70         ┆ 30         ┆ 100          │
    │                   ┆ Cougars          ┆ Vikings          ┆            ┆            ┆              │
    │ 2024-08-31T21:00Z ┆ South Alabama    ┆ North Texas Mean ┆ 38         ┆ 52         ┆ 90           │
    │                   ┆ Jaguars          ┆ Green            ┆            ┆            ┆              │
    │ 2024-08-31T16:00Z ┆ Pittsburgh       ┆ Kent State       ┆ 55         ┆ 24         ┆ 79           │
    │                   ┆ Panthers         ┆ Golden Flashes   ┆            ┆            ┆              │
    │ 2024-08-31T23:00Z ┆ Ole Miss Rebels  ┆ Furman Paladins  ┆ 76         ┆ 0          ┆ 76           │
    │ 2024-08-31T23:30Z ┆ Auburn Tigers    ┆ Alabama A&M      ┆ 73         ┆ 3          ┆ 76           │
    │                   ┆                  ┆ Bulldogs         ┆            ┆            ┆              │
    │ 2024-08-31T16:45Z ┆ Tennessee        ┆ Chattanooga Mocs ┆ 69         ┆ 3          ┆ 72           │
    │                   ┆ Volunteers       ┆                  ┆            ┆            ┆              │
    │ 2024-08-31T16:00Z ┆ Navy Midshipmen  ┆ Bucknell Bison   ┆ 49         ┆ 21         ┆ 70           │
    └───────────────────┴──────────────────┴──────────────────┴────────────┴────────────┴──────────────┘



## Cross-references

- R companion: [cfbfastR](https://cfbfastR.sportsdataverse.org)
- Data source: [ESPN CFB API](https://www.espn.com/college-football/)
- Plotting: [matplotlib](https://matplotlib.org), [plotnine](https://plotnine.org), or [polars-native plotting](https://docs.pola.rs)

## Where to go next

- API docs: `docs/docs/cfb/index.md`
- Next notebook: `03_nfl_intro.ipynb`
